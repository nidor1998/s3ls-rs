pub mod client_builder;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use anyhow::Result;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::RequestPayer;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::config::ClientConfig;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ListEntry, S3Object};

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

/// Whether to use ListObjectsV2 or ListObjectVersions.
#[derive(Clone, Copy)]
enum ListingMode {
    Objects,
    Versions,
}

/// A single page of listing results from S3.
struct ListPage {
    objects: Vec<ListEntry>,
    sub_prefixes: Vec<String>,
    is_truncated: bool,
    continuation_token: Option<String>,
    key_marker: Option<String>,
    version_id_marker: Option<String>,
}

/// S3-backed implementation of [`StorageTrait`].
#[derive(Clone)]
pub struct S3Storage {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    client: Client,
    cancellation_token: PipelineCancellationToken,
    request_payer: Option<RequestPayer>,
    max_parallel_listings: u16,
    max_parallel_listing_max_depth: u16,
    allow_parallel_listings_in_express_one_zone: bool,
    listing_worker_semaphore: Arc<tokio::sync::Semaphore>,
}

impl S3Storage {
    /// Create a new `S3Storage`.
    ///
    /// When `recursive` is `false`, a delimiter of `"/"` is used so that S3
    /// returns common prefixes (virtual directories). When `recursive` is
    /// `true`, no delimiter is set and all objects under the prefix are
    /// returned.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        client_config: &ClientConfig,
        bucket: String,
        prefix: Option<String>,
        recursive: bool,
        cancellation_token: PipelineCancellationToken,
        request_payer: Option<RequestPayer>,
        max_parallel_listings: u16,
        max_parallel_listing_max_depth: u16,
        allow_parallel_listings_in_express_one_zone: bool,
    ) -> Self {
        let client = client_config.create_client().await;
        let delimiter = if recursive {
            None
        } else {
            Some("/".to_string())
        };

        let semaphore_size = max_parallel_listings.max(1) as usize;

        Self {
            bucket,
            prefix,
            delimiter,
            client,
            cancellation_token,
            request_payer,
            max_parallel_listings,
            max_parallel_listing_max_depth,
            allow_parallel_listings_in_express_one_zone,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(semaphore_size)),
        }
    }

    /// Returns true if the bucket is an Express One Zone bucket.
    fn is_express_onezone_storage(&self) -> bool {
        self.bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
    }

    /// Send a batch of entries to the channel, returning `true` if sending
    /// should stop (cancellation or receiver dropped).
    async fn send_listed_entries(
        &self,
        entries: Vec<ListEntry>,
        sender: &Sender<ListEntry>,
    ) -> Result<bool> {
        for entry in entries {
            if self.cancellation_token.is_cancelled() {
                return Ok(true);
            }
            if sender.send(entry).await.is_err() {
                return Ok(true); // receiver dropped
            }
        }
        Ok(false)
    }

    /// Decide whether to use parallel or sequential listing, then dispatch.
    async fn list_dispatch(
        &self,
        mode: ListingMode,
        sender: &Sender<ListEntry>,
        max_keys: i32,
    ) -> Result<()> {
        let use_parallel = self.max_parallel_listings > 1
            && self.delimiter.is_none() // recursive mode
            && (!self.is_express_onezone_storage()
                || self.allow_parallel_listings_in_express_one_zone);

        if use_parallel {
            debug!(
                bucket = %self.bucket,
                max_parallel = self.max_parallel_listings,
                max_depth = self.max_parallel_listing_max_depth,
                "Using parallel listing"
            );
            let permit = self
                .listing_worker_semaphore
                .clone()
                .acquire_owned()
                .await
                .expect("listing semaphore closed unexpectedly");
            self.list_with_parallel(mode, sender, max_keys, self.prefix.clone(), 0, permit)
                .await
        } else {
            debug!(bucket = %self.bucket, "Using sequential listing");
            self.list_sequential(mode, sender, max_keys, self.prefix.clone(), self.delimiter.clone())
                .await
        }
    }

    /// Sequential listing: paginate through all results for the given prefix/delimiter.
    async fn list_sequential(
        &self,
        mode: ListingMode,
        sender: &Sender<ListEntry>,
        max_keys: i32,
        prefix: Option<String>,
        delimiter: Option<String>,
    ) -> Result<()> {
        let mut continuation_token: Option<String> = None;
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            if self.cancellation_token.is_cancelled() {
                debug!("list_sequential cancelled");
                break;
            }

            let page = self
                .fetch_list_page(
                    mode,
                    max_keys,
                    prefix.as_deref(),
                    delimiter.as_deref(),
                    continuation_token.as_deref(),
                    key_marker.as_deref(),
                    version_id_marker.as_deref(),
                )
                .await?;

            // Send objects
            if self.send_listed_entries(page.objects, sender).await? {
                return Ok(());
            }

            // Send common prefixes
            let prefix_entries: Vec<ListEntry> = page
                .sub_prefixes
                .iter()
                .map(|p| ListEntry::CommonPrefix(p.clone()))
                .collect();
            if self.send_listed_entries(prefix_entries, sender).await? {
                return Ok(());
            }

            // Check for more pages
            if page.is_truncated {
                match mode {
                    ListingMode::Objects => {
                        continuation_token = page.continuation_token;
                        if continuation_token.is_none() {
                            warn!("truncated response but no continuation token");
                            break;
                        }
                    }
                    ListingMode::Versions => {
                        key_marker = page.key_marker;
                        version_id_marker = page.version_id_marker;
                        if key_marker.is_none() {
                            warn!("truncated response but no next key marker");
                            break;
                        }
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Parallel listing using recursive prefix discovery with JoinSet.
    fn list_with_parallel<'a>(
        &'a self,
        mode: ListingMode,
        sender: &'a Sender<ListEntry>,
        max_keys: i32,
        prefix: Option<String>,
        depth: u16,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Beyond max depth: switch to sequential with no delimiter
            if depth > self.max_parallel_listing_max_depth {
                drop(permit);
                return self
                    .list_sequential(mode, sender, max_keys, prefix, None)
                    .await;
            }

            let mut current_permit = Some(permit);

            // Paginate at this level with "/" delimiter to discover sub-prefixes
            let mut continuation_token: Option<String> = None;
            let mut key_marker: Option<String> = None;
            let mut version_id_marker: Option<String> = None;
            let mut all_sub_prefixes: Vec<String> = Vec::new();

            loop {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }

                let page = self
                    .fetch_list_page(
                        mode,
                        max_keys,
                        prefix.as_deref(),
                        Some("/"),
                        continuation_token.as_deref(),
                        key_marker.as_deref(),
                        version_id_marker.as_deref(),
                    )
                    .await?;

                // Send objects at this level
                if self.send_listed_entries(page.objects, sender).await? {
                    return Ok(());
                }

                // Collect sub-prefixes
                all_sub_prefixes.extend(page.sub_prefixes);

                if page.is_truncated {
                    match mode {
                        ListingMode::Objects => {
                            continuation_token = page.continuation_token;
                            if continuation_token.is_none() {
                                warn!("truncated response but no continuation token");
                                break;
                            }
                        }
                        ListingMode::Versions => {
                            key_marker = page.key_marker;
                            version_id_marker = page.version_id_marker;
                            if key_marker.is_none() {
                                warn!("truncated response but no next key marker");
                                break;
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            // Release permit before spawning sub-tasks
            drop(current_permit.take());

            // Spawn sub-tasks for each sub-prefix
            if !all_sub_prefixes.is_empty() {
                let mut join_set = JoinSet::new();

                for sub_prefix in all_sub_prefixes {
                    let storage = self.clone();
                    let sender = sender.clone();
                    let next_depth = depth + 1;
                    let sem = self.listing_worker_semaphore.clone();

                    join_set.spawn(async move {
                        let sub_permit = sem
                            .acquire_owned()
                            .await
                            .expect("listing semaphore closed unexpectedly");
                        storage
                            .list_with_parallel(
                                mode,
                                &sender,
                                max_keys,
                                Some(sub_prefix),
                                next_depth,
                                sub_permit,
                            )
                            .await
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            self.cancellation_token.cancel();
                            return Err(e);
                        }
                        Err(join_err) => {
                            self.cancellation_token.cancel();
                            return Err(anyhow::anyhow!(
                                "Listing sub-task panicked: {}",
                                join_err
                            ));
                        }
                    }
                }
            }

            Ok(())
        })
    }

    /// Fetch a single page of listing results from S3.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_list_page(
        &self,
        mode: ListingMode,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> Result<ListPage> {
        match mode {
            ListingMode::Objects => {
                self.fetch_list_objects_page(max_keys, prefix, delimiter, continuation_token)
                    .await
            }
            ListingMode::Versions => {
                self.fetch_list_versions_page(
                    max_keys,
                    prefix,
                    delimiter,
                    key_marker,
                    version_id_marker,
                )
                .await
            }
        }
    }

    async fn fetch_list_objects_page(
        &self,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
    ) -> Result<ListPage> {
        let mut req = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .max_keys(max_keys);

        if let Some(prefix) = prefix {
            req = req.prefix(prefix);
        }
        if let Some(delimiter) = delimiter {
            req = req.delimiter(delimiter);
        }
        if let Some(token) = continuation_token {
            req = req.continuation_token(token);
        }
        if let Some(ref payer) = self.request_payer {
            req = req.request_payer(payer.clone());
        }

        let response = req.send().await.map_err(|e| {
            let (code, msg) = extract_sdk_error_details(&e);
            tracing::error!(
                bucket = %self.bucket,
                prefix = ?prefix,
                s3_error_code = %code,
                s3_error_message = %msg,
                "S3 ListObjectsV2 API call failed"
            );
            anyhow::anyhow!(e).context(format!(
                "S3 ListObjectsV2 failed for s3://{}/{}",
                self.bucket,
                prefix.unwrap_or("")
            ))
        })?;

        let objects: Vec<ListEntry> = response
            .contents()
            .iter()
            .filter_map(convert_object)
            .collect();

        let sub_prefixes: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix().map(|p| p.to_string()))
            .collect();

        Ok(ListPage {
            objects,
            sub_prefixes,
            is_truncated: response.is_truncated() == Some(true),
            continuation_token: response
                .next_continuation_token()
                .map(|s| s.to_string()),
            key_marker: None,
            version_id_marker: None,
        })
    }

    async fn fetch_list_versions_page(
        &self,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> Result<ListPage> {
        let mut req = self
            .client
            .list_object_versions()
            .bucket(&self.bucket)
            .max_keys(max_keys);

        if let Some(prefix) = prefix {
            req = req.prefix(prefix);
        }
        if let Some(delimiter) = delimiter {
            req = req.delimiter(delimiter);
        }
        if let Some(marker) = key_marker {
            req = req.key_marker(marker);
        }
        if let Some(marker) = version_id_marker {
            req = req.version_id_marker(marker);
        }
        if let Some(ref payer) = self.request_payer {
            req = req.request_payer(payer.clone());
        }

        let response = req.send().await.map_err(|e| {
            let (code, msg) = extract_sdk_error_details(&e);
            tracing::error!(
                bucket = %self.bucket,
                prefix = ?prefix,
                s3_error_code = %code,
                s3_error_message = %msg,
                "S3 ListObjectVersions API call failed"
            );
            anyhow::anyhow!(e).context(format!(
                "S3 ListObjectVersions failed for s3://{}/{}",
                self.bucket,
                prefix.unwrap_or("")
            ))
        })?;

        let mut objects: Vec<ListEntry> = Vec::new();

        for version in response.versions() {
            if let Some(entry) = convert_object_version(version) {
                objects.push(entry);
            }
        }

        for marker in response.delete_markers() {
            if let Some(entry) = convert_delete_marker(marker) {
                objects.push(entry);
            }
        }

        let sub_prefixes: Vec<String> = response
            .common_prefixes()
            .iter()
            .filter_map(|cp| cp.prefix().map(|p| p.to_string()))
            .collect();

        Ok(ListPage {
            objects,
            sub_prefixes,
            is_truncated: response.is_truncated() == Some(true),
            continuation_token: None,
            key_marker: response.next_key_marker().map(|s| s.to_string()),
            version_id_marker: response
                .next_version_id_marker()
                .map(|s| s.to_string()),
        })
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()> {
        self.list_dispatch(ListingMode::Objects, sender, max_keys)
            .await
    }

    async fn list_object_versions(
        &self,
        sender: &Sender<ListEntry>,
        max_keys: i32,
    ) -> Result<()> {
        self.list_dispatch(ListingMode::Versions, sender, max_keys)
            .await
    }
}

/// Extract error code and message from an AWS SDK error.
fn extract_sdk_error_details<E: std::fmt::Display + ProvideErrorMetadata>(
    e: &SdkError<E>,
) -> (String, String) {
    if let Some(service_err) = e.as_service_error() {
        (
            service_err.code().unwrap_or("unknown").to_string(),
            service_err.message().unwrap_or("no message").to_string(),
        )
    } else {
        ("N/A".to_string(), e.to_string())
    }
}

/// Convert an AWS SDK `Object` into a [`ListEntry`].
fn convert_object(object: &aws_sdk_s3::types::Object) -> Option<ListEntry> {
    let key = object.key()?.to_string();
    let size = object.size().unwrap_or(0).max(0) as u64;
    let last_modified = aws_datetime_to_chrono(object.last_modified())?;
    let e_tag = object.e_tag().unwrap_or_default().to_string();
    let storage_class = object.storage_class().map(|sc| sc.as_str().to_string());
    let checksum_algorithm = object
        .checksum_algorithm()
        .first()
        .map(|a| a.as_str().to_string());
    let checksum_type = object
        .checksum_type()
        .map(|ct| ct.as_str().to_string());

    Some(ListEntry::Object(S3Object::NotVersioning {
        key,
        size,
        last_modified,
        e_tag,
        storage_class,
        checksum_algorithm,
        checksum_type,
    }))
}

/// Convert an AWS SDK `ObjectVersion` into a [`ListEntry`].
fn convert_object_version(version: &aws_sdk_s3::types::ObjectVersion) -> Option<ListEntry> {
    let key = version.key()?.to_string();
    let version_id = version.version_id().unwrap_or("null").to_string();
    let size = version.size().unwrap_or(0).max(0) as u64;
    let last_modified = aws_datetime_to_chrono(version.last_modified())?;
    let e_tag = version.e_tag().unwrap_or_default().to_string();
    let is_latest = version.is_latest().unwrap_or(false);
    let storage_class = version.storage_class().map(|sc| sc.as_str().to_string());
    let checksum_algorithm = version
        .checksum_algorithm()
        .first()
        .map(|a| a.as_str().to_string());
    let checksum_type = version
        .checksum_type()
        .map(|ct| ct.as_str().to_string());

    Some(ListEntry::Object(S3Object::Versioning {
        key,
        version_id,
        size,
        last_modified,
        e_tag,
        is_latest,
        storage_class,
        checksum_algorithm,
        checksum_type,
    }))
}

/// Convert an AWS SDK `DeleteMarkerEntry` into a [`ListEntry`].
fn convert_delete_marker(marker: &aws_sdk_s3::types::DeleteMarkerEntry) -> Option<ListEntry> {
    let key = marker.key()?.to_string();
    let version_id = marker.version_id().unwrap_or("null").to_string();
    let last_modified = aws_datetime_to_chrono(marker.last_modified())?;
    let is_latest = marker.is_latest().unwrap_or(false);

    Some(ListEntry::DeleteMarker {
        key,
        version_id,
        last_modified,
        is_latest,
    })
}

/// Convert an AWS SDK `DateTime` to a `chrono::DateTime<Utc>`.
fn aws_datetime_to_chrono(dt: Option<&aws_smithy_types::DateTime>) -> Option<DateTime<Utc>> {
    let dt = dt?;
    let epoch_secs = dt.secs();
    let subsec_nanos = dt.subsec_nanos();
    DateTime::from_timestamp(epoch_secs, subsec_nanos)
}
