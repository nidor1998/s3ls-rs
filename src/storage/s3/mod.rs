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
use tracing::debug;

use crate::config::ClientConfig;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ListEntry, S3Object};

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

/// Whether to use ListObjectsV2 or ListObjectVersions.
#[derive(Clone, Copy)]
pub(crate) enum ListingMode {
    Objects,
    Versions,
}

/// A single page of listing results from S3.
pub(crate) struct ListPage {
    pub objects: Vec<ListEntry>,
    pub sub_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub continuation_token: Option<String>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<String>,
}

/// Trait abstracting the page-fetching call so the listing algorithm can be tested
/// without a real S3 client.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub(crate) trait PageFetcher: Send + Sync {
    async fn fetch_page(
        &self,
        mode: ListingMode,
        max_keys: i32,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
    ) -> Result<ListPage>;
}

// ---------------------------------------------------------------------------
// S3PageFetcher — real AWS SDK implementation of PageFetcher
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct S3PageFetcher {
    client: Client,
    bucket: String,
    request_payer: Option<RequestPayer>,
    fetch_owner: bool,
}

impl S3PageFetcher {
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
        if self.fetch_owner {
            req = req.optional_object_attributes(aws_sdk_s3::types::OptionalObjectAttributes::RestoreStatus);
            req = req.fetch_owner(true);
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
impl PageFetcher for S3PageFetcher {
    async fn fetch_page(
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
}

// ---------------------------------------------------------------------------
// ListingEngine — the listing algorithm, parameterized over any PageFetcher
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ListingEngine<F: PageFetcher + Clone> {
    fetcher: F,
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    cancellation_token: PipelineCancellationToken,
    max_parallel_listings: u16,
    max_parallel_listing_max_depth: u16,
    allow_parallel_listings_in_express_one_zone: bool,
    listing_worker_semaphore: Arc<tokio::sync::Semaphore>,
    max_depth: Option<u16>,
}

impl<F: PageFetcher + Clone + 'static> ListingEngine<F> {
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
    pub(crate) async fn list_dispatch(
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
                .fetcher
                .fetch_page(
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
                            anyhow::bail!(
                                "S3 returned truncated response but no continuation token for s3://{}/{}",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
                        }
                    }
                    ListingMode::Versions => {
                        key_marker = page.key_marker;
                        version_id_marker = page.version_id_marker;
                        if key_marker.is_none() {
                            anyhow::bail!(
                                "S3 returned truncated response but no next key marker for s3://{}/{}",
                                self.bucket,
                                self.prefix.as_deref().unwrap_or("")
                            );
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

            // Content depth limit reached: don't fetch anything beyond max_depth
            if let Some(max_depth) = self.max_depth {
                if depth > max_depth {
                    return Ok(());
                }
            }

            // Beyond max parallel depth: switch to sequential with no delimiter
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
                    .fetcher
                    .fetch_page(
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
                                anyhow::bail!(
                                    "S3 returned truncated response but no continuation token for s3://{}/{}",
                                    self.bucket,
                                    self.prefix.as_deref().unwrap_or("")
                                );
                            }
                        }
                        ListingMode::Versions => {
                            key_marker = page.key_marker;
                            version_id_marker = page.version_id_marker;
                            if key_marker.is_none() {
                                anyhow::bail!(
                                    "S3 returned truncated response but no next key marker for s3://{}/{}",
                                    self.bucket,
                                    self.prefix.as_deref().unwrap_or("")
                                );
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            // Release permit before spawning sub-tasks
            drop(current_permit.take());

            // At max_depth: don't recurse into sub-prefixes
            if let Some(max_depth) = self.max_depth {
                if depth >= max_depth {
                    return Ok(());
                }
            }

            // Spawn sub-tasks for each sub-prefix
            if !all_sub_prefixes.is_empty() {
                let mut join_set = JoinSet::new();

                for sub_prefix in all_sub_prefixes {
                    let engine = self.clone();
                    let sender = sender.clone();
                    let next_depth = depth + 1;
                    let sem = self.listing_worker_semaphore.clone();

                    join_set.spawn(async move {
                        let sub_permit = sem
                            .acquire_owned()
                            .await
                            .expect("listing semaphore closed unexpectedly");
                        engine
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
}

// ---------------------------------------------------------------------------
// S3Storage — public interface wrapping ListingEngine<S3PageFetcher>
// ---------------------------------------------------------------------------

/// S3-backed implementation of [`StorageTrait`].
#[derive(Clone)]
pub struct S3Storage {
    engine: ListingEngine<S3PageFetcher>,
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
        max_depth: Option<u16>,
        allow_parallel_listings_in_express_one_zone: bool,
        fetch_owner: bool,
    ) -> Self {
        let client = client_config.create_client().await;
        let delimiter = if recursive {
            None
        } else {
            Some("/".to_string())
        };

        let semaphore_size = max_parallel_listings.max(1) as usize;

        let fetcher = S3PageFetcher {
            client,
            bucket: bucket.clone(),
            request_payer,
            fetch_owner,
        };

        let engine = ListingEngine {
            fetcher,
            bucket,
            prefix,
            delimiter,
            cancellation_token,
            max_parallel_listings,
            max_parallel_listing_max_depth,
            allow_parallel_listings_in_express_one_zone,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(semaphore_size)),
            max_depth,
        };

        Self { engine }
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()> {
        self.engine
            .list_dispatch(ListingMode::Objects, sender, max_keys)
            .await
    }

    async fn list_object_versions(
        &self,
        sender: &Sender<ListEntry>,
        max_keys: i32,
    ) -> Result<()> {
        self.engine
            .list_dispatch(ListingMode::Versions, sender, max_keys)
            .await
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

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
    let owner_display_name = object.owner().and_then(|o| o.display_name()).map(|s| s.to_string());
    let owner_id = object.owner().and_then(|o| o.id()).map(|s| s.to_string());
    let is_restore_in_progress = object
        .restore_status()
        .and_then(|rs| rs.is_restore_in_progress());
    let restore_expiry_date = object
        .restore_status()
        .and_then(|rs| rs.restore_expiry_date())
        .and_then(|dt| aws_datetime_to_chrono(Some(dt)))
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));

    Some(ListEntry::Object(S3Object::NotVersioning {
        key,
        size,
        last_modified,
        e_tag,
        storage_class,
        checksum_algorithm,
        checksum_type,
        owner_display_name,
        owner_id,
        is_restore_in_progress,
        restore_expiry_date,
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
    let owner_display_name = version.owner().and_then(|o| o.display_name()).map(|s| s.to_string());
    let owner_id = version.owner().and_then(|o| o.id()).map(|s| s.to_string());
    // ObjectVersion does not have restore_status in the AWS SDK
    let is_restore_in_progress = None;
    let restore_expiry_date = None;

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
        owner_display_name,
        owner_id,
        is_restore_in_progress,
        restore_expiry_date,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::token::create_pipeline_cancellation_token;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // MockPageFetcher
    // -----------------------------------------------------------------------

    type PageMap = HashMap<(Option<String>, Option<String>), Vec<ListPage>>;

    /// A mock page fetcher that returns pre-configured pages keyed by
    /// (prefix, delimiter) so parallel listing can get different results
    /// for different sub-prefixes.
    #[derive(Clone)]
    struct MockPageFetcher {
        /// Key: (prefix, delimiter) -> queue of pages to return in order.
        pages: Arc<Mutex<PageMap>>,
    }

    impl MockPageFetcher {
        /// Create a mock that returns the given pages for the default prefix.
        fn from_pages(prefix: Option<&str>, delimiter: Option<&str>, pages: Vec<ListPage>) -> Self {
            let mut map = HashMap::new();
            map.insert(
                (prefix.map(|s| s.to_string()), delimiter.map(|s| s.to_string())),
                pages,
            );
            Self {
                pages: Arc::new(Mutex::new(map)),
            }
        }

        /// Create a mock with a full map of (prefix, delimiter) -> pages.
        fn from_map(map: PageMap) -> Self {
            Self {
                pages: Arc::new(Mutex::new(map)),
            }
        }
    }

    #[async_trait]
    impl PageFetcher for MockPageFetcher {
        async fn fetch_page(
            &self,
            _mode: ListingMode,
            _max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            _continuation_token: Option<&str>,
            _key_marker: Option<&str>,
            _version_id_marker: Option<&str>,
        ) -> Result<ListPage> {
            let key = (
                prefix.map(|s| s.to_string()),
                delimiter.map(|s| s.to_string()),
            );
            let mut map = self.pages.lock().unwrap();
            if let Some(queue) = map.get_mut(&key)
                && !queue.is_empty()
            {
                return Ok(queue.remove(0));
            }
            // Default: empty non-truncated page
            Ok(ListPage {
                objects: vec![],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            })
        }
    }

    /// A mock that always returns an error (used for error-propagation tests).
    #[derive(Clone)]
    struct ErrorPageFetcher {
        /// Which prefix triggers the error. None means all prefixes error.
        error_prefix: Option<String>,
        /// Fallback fetcher for non-error prefixes.
        fallback: MockPageFetcher,
    }

    #[async_trait]
    impl PageFetcher for ErrorPageFetcher {
        async fn fetch_page(
            &self,
            mode: ListingMode,
            max_keys: i32,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            key_marker: Option<&str>,
            version_id_marker: Option<&str>,
        ) -> Result<ListPage> {
            if let Some(ref err_prefix) = self.error_prefix {
                if prefix == Some(err_prefix.as_str()) {
                    anyhow::bail!("simulated S3 error for prefix {}", err_prefix);
                }
            } else {
                anyhow::bail!("simulated S3 error");
            }
            self.fallback
                .fetch_page(mode, max_keys, prefix, delimiter, continuation_token, key_marker, version_id_marker)
                .await
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_entry(key: &str) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(),
            size: 100,
            last_modified: chrono::Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: None,
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
        })
    }

    fn make_engine<F: PageFetcher + Clone + 'static>(
        fetcher: F,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_parallel: u16,
        max_depth: u16,
        allow_express: bool,
    ) -> ListingEngine<F> {
        let token = create_pipeline_cancellation_token();
        make_engine_with_token(fetcher, bucket, prefix, delimiter, max_parallel, max_depth, allow_express, token)
    }

    #[allow(clippy::too_many_arguments)]
    fn make_engine_with_token<F: PageFetcher + Clone + 'static>(
        fetcher: F,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_parallel: u16,
        max_depth: u16,
        allow_express: bool,
        token: PipelineCancellationToken,
    ) -> ListingEngine<F> {
        ListingEngine {
            fetcher,
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            delimiter: delimiter.map(|s| s.to_string()),
            cancellation_token: token,
            max_parallel_listings: max_parallel,
            max_parallel_listing_max_depth: max_depth,
            allow_parallel_listings_in_express_one_zone: allow_express,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(
                max_parallel.max(1) as usize,
            )),
            max_depth: None,
        }
    }

    async fn collect_entries(
        engine: &ListingEngine<impl PageFetcher + Clone + 'static>,
        mode: ListingMode,
        max_keys: i32,
    ) -> Result<Vec<ListEntry>> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let result = engine.list_dispatch(mode, &tx, max_keys).await;
        drop(tx);
        let mut entries = Vec::new();
        while let Some(e) = rx.recv().await {
            entries.push(e);
        }
        result.map(|()| entries)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    // 1. dispatch_uses_sequential_when_non_recursive
    #[tokio::test]
    async fn dispatch_uses_sequential_when_non_recursive() {
        // delimiter is set -> sequential, even with max_parallel > 1
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            Some("/"),
            vec![ListPage {
                objects: vec![make_entry("prefix/file.txt")],
                sub_prefixes: vec!["prefix/sub/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), Some("/"), 4, 3, false);

        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();
        // Should get the object + the common prefix
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key(), "prefix/file.txt");
        assert_eq!(entries[1].key(), "prefix/sub/");
    }

    // 2. dispatch_uses_sequential_when_max_parallel_is_1
    #[tokio::test]
    async fn dispatch_uses_sequential_when_max_parallel_is_1() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None, // no delimiter in sequential call when recursive
            vec![ListPage {
                objects: vec![make_entry("prefix/a.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        // recursive (no delimiter) + max_parallel=1 => sequential
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/a.txt");
    }

    // 3. dispatch_uses_parallel_when_recursive_and_multiple_workers
    #[tokio::test]
    async fn dispatch_uses_parallel_when_recursive_and_multiple_workers() {
        // Parallel listing first fetches with delimiter "/" at the top level,
        // then recurses into sub-prefixes.
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top-level: returns sub-prefixes a/ and b/, plus one object at root
        map.insert(
            (Some("prefix/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/root.txt")],
                sub_prefixes: vec!["prefix/a/".to_string(), "prefix/b/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix a/
        map.insert(
            (Some("prefix/a/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/a/1.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix b/
        map.insert(
            (Some("prefix/b/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/b/2.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        // recursive (no delimiter) + max_parallel=4 => parallel
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 4, 3, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["prefix/a/1.txt", "prefix/b/2.txt", "prefix/root.txt"]);
    }

    // 4. dispatch_uses_sequential_for_express_one_zone
    #[tokio::test]
    async fn dispatch_uses_sequential_for_express_one_zone() {
        // Bucket ends with --x-s3, allow_express=false => sequential
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![make_entry("prefix/express.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        // Note: sequential with recursive (no delimiter in engine) will call with delimiter=None
        let engine = make_engine(
            fetcher,
            "my-bucket--x-s3",
            Some("prefix/"),
            None,
            4,
            3,
            false,
        );
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/express.txt");
    }

    // 5. dispatch_uses_parallel_for_express_one_zone_when_allowed
    #[tokio::test]
    async fn dispatch_uses_parallel_for_express_one_zone_when_allowed() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();
        map.insert(
            (Some("prefix/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("prefix/express.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let fetcher = MockPageFetcher::from_map(map);
        // Express bucket + allow=true + recursive + parallel>1 => parallel
        let engine = make_engine(
            fetcher,
            "my-bucket--x-s3",
            Some("prefix/"),
            None,
            4,
            3,
            true,
        );
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key(), "prefix/express.txt");
    }

    // 6. sequential_paginates_through_multiple_pages
    #[tokio::test]
    async fn sequential_paginates_through_multiple_pages() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![
                ListPage {
                    objects: vec![make_entry("prefix/1.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("token1".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![make_entry("prefix/2.txt")],
                    sub_prefixes: vec![],
                    is_truncated: true,
                    continuation_token: Some("token2".to_string()),
                    key_marker: None,
                    version_id_marker: None,
                },
                ListPage {
                    objects: vec![make_entry("prefix/3.txt")],
                    sub_prefixes: vec![],
                    is_truncated: false,
                    continuation_token: None,
                    key_marker: None,
                    version_id_marker: None,
                },
            ],
        );
        // max_parallel=1 => sequential
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();
        let keys: Vec<&str> = entries.iter().map(|e| e.key()).collect();
        assert_eq!(keys, vec!["prefix/1.txt", "prefix/2.txt", "prefix/3.txt"]);
    }

    // 7. sequential_stops_on_cancellation
    #[tokio::test]
    async fn sequential_stops_on_cancellation() {
        // Cancel the token before listing starts => no pages should be fetched at all
        let token = create_pipeline_cancellation_token();
        token.cancel();

        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![
                ListPage {
                    objects: vec![make_entry("prefix/1.txt")],
                    sub_prefixes: vec![],
                    is_truncated: false,
                    continuation_token: None,
                    key_marker: None,
                    version_id_marker: None,
                },
            ],
        );

        let engine = make_engine_with_token(
            fetcher, "bucket", Some("prefix/"), None, 1, 3, false, token,
        );

        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();
        // Should have received no entries because token was already cancelled
        assert!(entries.is_empty());
    }

    // 8. sequential_errors_on_truncated_without_token
    #[tokio::test]
    async fn sequential_errors_on_truncated_without_token() {
        let fetcher = MockPageFetcher::from_pages(
            Some("prefix/"),
            None,
            vec![ListPage {
                objects: vec![make_entry("prefix/1.txt")],
                sub_prefixes: vec![],
                is_truncated: true,
                continuation_token: None, // missing!
                key_marker: None,
                version_id_marker: None,
            }],
        );
        let engine = make_engine(fetcher, "bucket", Some("prefix/"), None, 1, 3, false);
        let result = collect_entries(&engine, ListingMode::Objects, 1000).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("truncated") && err_msg.contains("continuation token"),
            "unexpected error: {}",
            err_msg
        );
    }

    // 9. parallel_discovers_sub_prefixes_and_lists_them
    #[tokio::test]
    async fn parallel_discovers_sub_prefixes_and_lists_them() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level with delimiter
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/a/".to_string(), "p/b/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix a/ with delimiter
        map.insert(
            (Some("p/a/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a/file1.txt"), make_entry("p/a/file2.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Sub-prefix b/ with delimiter
        map.insert(
            (Some("p/b/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/b/file3.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 5, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["p/a/file1.txt", "p/a/file2.txt", "p/b/file3.txt"]);
    }

    // 10. parallel_falls_back_to_sequential_beyond_max_depth
    #[tokio::test]
    async fn parallel_falls_back_to_sequential_beyond_max_depth() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level (depth 0) with delimiter
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/deep/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // depth 1 with delimiter
        map.insert(
            (Some("p/deep/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/deep/deeper/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // depth 2 > max_depth(1) => falls back to sequential with NO delimiter
        // So the call will be (prefix="p/deep/deeper/", delimiter=None)
        map.insert(
            (Some("p/deep/deeper/".to_string()), None),
            vec![ListPage {
                objects: vec![
                    make_entry("p/deep/deeper/file.txt"),
                    make_entry("p/deep/deeper/sub/file2.txt"),
                ],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        // max_depth = 1, so depth 2 (> 1) falls back to sequential
        let engine = make_engine(fetcher, "bucket", Some("p/"), None, 4, 1, false);
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["p/deep/deeper/file.txt", "p/deep/deeper/sub/file2.txt"]
        );
    }

    // 11. parallel_cancels_on_sub_task_error
    #[tokio::test]
    async fn parallel_cancels_on_sub_task_error() {
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level discovers two sub-prefixes
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![],
                sub_prefixes: vec!["p/ok/".to_string(), "p/fail/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // p/ok/ succeeds
        map.insert(
            (Some("p/ok/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/ok/file.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fallback = MockPageFetcher::from_map(map);
        let fetcher = ErrorPageFetcher {
            error_prefix: Some("p/fail/".to_string()),
            fallback,
        };

        let token = create_pipeline_cancellation_token();
        let token_check = token.clone();
        let engine = make_engine_with_token(
            fetcher, "bucket", Some("p/"), None, 4, 5, false, token,
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let result = engine.list_dispatch(ListingMode::Objects, &tx, 1000).await;
        drop(tx);

        // Drain channel
        let mut entries = Vec::new();
        while let Some(e) = rx.recv().await {
            entries.push(e);
        }

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("simulated S3 error"),
            "unexpected error: {}",
            err_msg
        );
        // Token should have been cancelled
        assert!(token_check.is_cancelled());
    }

    // Helper: make_engine_with_max_depth — allows setting content max_depth
    #[allow(clippy::too_many_arguments)]
    fn make_engine_with_max_depth<F: PageFetcher + Clone + 'static>(
        fetcher: F,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        max_parallel: u16,
        max_depth: u16,
        allow_express: bool,
        content_max_depth: Option<u16>,
    ) -> ListingEngine<F> {
        let token = create_pipeline_cancellation_token();
        ListingEngine {
            fetcher,
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            delimiter: delimiter.map(|s| s.to_string()),
            cancellation_token: token,
            max_parallel_listings: max_parallel,
            max_parallel_listing_max_depth: max_depth,
            allow_parallel_listings_in_express_one_zone: allow_express,
            listing_worker_semaphore: Arc::new(tokio::sync::Semaphore::new(
                max_parallel.max(1) as usize,
            )),
            max_depth: content_max_depth,
        }
    }

    // 12. parallel_respects_max_depth
    #[tokio::test]
    async fn parallel_respects_max_depth() {
        // Structure: p/ -> p/a/ -> p/a/deep/ -> p/a/deep/file.txt
        // With max_depth=1, only objects directly in p/ and p/a/ should appear.
        // p/a/deep/ should NOT be recursed into.
        let mut map: HashMap<(Option<String>, Option<String>), Vec<ListPage>> = HashMap::new();

        // Top level (depth 0): discovers sub-prefix a/
        map.insert(
            (Some("p/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/root.txt")],
                sub_prefixes: vec!["p/a/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Depth 1 (a/): discovers sub-prefix deep/, has direct objects
        map.insert(
            (Some("p/a/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a/file.txt")],
                sub_prefixes: vec!["p/a/deep/".to_string()],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        // Depth 2 (deep/) — should NOT be reached with max_depth=1
        map.insert(
            (Some("p/a/deep/".to_string()), Some("/".to_string())),
            vec![ListPage {
                objects: vec![make_entry("p/a/deep/should_not_appear.txt")],
                sub_prefixes: vec![],
                is_truncated: false,
                continuation_token: None,
                key_marker: None,
                version_id_marker: None,
            }],
        );

        let fetcher = MockPageFetcher::from_map(map);
        // max_depth content limit = 1 (only 1 level of sub-prefixes)
        let engine = make_engine_with_max_depth(
            fetcher, "bucket", Some("p/"), None, 4, 5, false, Some(1),
        );
        let entries = collect_entries(&engine, ListingMode::Objects, 1000).await.unwrap();

        let mut keys: Vec<String> = entries.iter().map(|e| e.key().to_string()).collect();
        keys.sort();
        assert_eq!(keys, vec!["p/a/file.txt", "p/root.txt"]);
    }
}
