pub mod client_builder;

use async_trait::async_trait;
use anyhow::Result;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::RequestPayer;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

use crate::config::ClientConfig;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ListEntry, S3Object};

/// S3-backed implementation of [`StorageTrait`].
pub struct S3Storage {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    client: Client,
    cancellation_token: PipelineCancellationToken,
    request_payer: Option<RequestPayer>,
}

impl S3Storage {
    /// Create a new `S3Storage`.
    ///
    /// When `recursive` is `false`, a delimiter of `"/"` is used so that S3
    /// returns common prefixes (virtual directories). When `recursive` is
    /// `true`, no delimiter is set and all objects under the prefix are
    /// returned.
    pub async fn new(
        client_config: &ClientConfig,
        bucket: String,
        prefix: Option<String>,
        recursive: bool,
        cancellation_token: PipelineCancellationToken,
        request_payer: Option<RequestPayer>,
    ) -> Self {
        let client = client_config.create_client().await;
        let delimiter = if recursive {
            None
        } else {
            Some("/".to_string())
        };

        Self {
            bucket,
            prefix,
            delimiter,
            client,
            cancellation_token,
            request_payer,
        }
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()> {
        let mut continuation_token: Option<String> = None;

        loop {
            if self.cancellation_token.is_cancelled() {
                debug!("list_objects cancelled");
                break;
            }

            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .max_keys(max_keys);

            if let Some(ref prefix) = self.prefix {
                req = req.prefix(prefix);
            }
            if let Some(ref delimiter) = self.delimiter {
                req = req.delimiter(delimiter);
            }
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }
            if let Some(ref payer) = self.request_payer {
                req = req.request_payer(payer.clone());
            }

            let response = req.send().await.map_err(|e| {
                tracing::error!(bucket = %self.bucket, error = %e, "S3 list_objects failed");
                e
            })?;

            // Send objects
            for object in response.contents() {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }
                if let Some(entry) = convert_object(object) {
                    sender.send(entry).await.ok();
                }
            }

            // Send common prefixes
            for cp in response.common_prefixes() {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }
                if let Some(p) = cp.prefix() {
                    sender
                        .send(ListEntry::CommonPrefix(p.to_string()))
                        .await
                        .ok();
                }
            }

            // Check for more pages
            if response.is_truncated() == Some(true) {
                continuation_token = response.next_continuation_token().map(|s| s.to_string());
                if continuation_token.is_none() {
                    warn!("truncated response but no continuation token");
                    break;
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn list_object_versions(
        &self,
        sender: &Sender<ListEntry>,
        max_keys: i32,
    ) -> Result<()> {
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            if self.cancellation_token.is_cancelled() {
                debug!("list_object_versions cancelled");
                break;
            }

            let mut req = self
                .client
                .list_object_versions()
                .bucket(&self.bucket)
                .max_keys(max_keys);

            if let Some(ref prefix) = self.prefix {
                req = req.prefix(prefix);
            }
            if let Some(ref delimiter) = self.delimiter {
                req = req.delimiter(delimiter);
            }
            if let Some(ref marker) = key_marker {
                req = req.key_marker(marker);
            }
            if let Some(ref marker) = version_id_marker {
                req = req.version_id_marker(marker);
            }
            if let Some(ref payer) = self.request_payer {
                req = req.request_payer(payer.clone());
            }

            let response = req.send().await.map_err(|e| {
                tracing::error!(bucket = %self.bucket, error = %e, "S3 list_object_versions failed");
                e
            })?;

            // Send object versions
            for version in response.versions() {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }
                if let Some(entry) = convert_object_version(version) {
                    sender.send(entry).await.ok();
                }
            }

            // Send delete markers
            for marker in response.delete_markers() {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }
                if let Some(entry) = convert_delete_marker(marker) {
                    sender.send(entry).await.ok();
                }
            }

            // Send common prefixes
            for cp in response.common_prefixes() {
                if self.cancellation_token.is_cancelled() {
                    return Ok(());
                }
                if let Some(p) = cp.prefix() {
                    sender
                        .send(ListEntry::CommonPrefix(p.to_string()))
                        .await
                        .ok();
                }
            }

            // Check for more pages
            if response.is_truncated() == Some(true) {
                key_marker = response.next_key_marker().map(|s| s.to_string());
                version_id_marker = response.next_version_id_marker().map(|s| s.to_string());
                if key_marker.is_none() {
                    warn!("truncated response but no next key marker");
                    break;
                }
            } else {
                break;
            }
        }

        Ok(())
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
