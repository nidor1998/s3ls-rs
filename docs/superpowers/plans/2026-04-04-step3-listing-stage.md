# Step 3: Listing Stage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the lister stage that queries S3 and emits `ListEntry` items through a bounded `tokio::sync::mpsc` channel, following s3rm-rs patterns where pagination is internal to `S3Storage` and entries are sent directly to a channel sender.

**Architecture:** `StorageTrait` methods accept a `&Sender<ListEntry>` and handle all pagination internally (no continuation tokens exposed to callers). `S3Storage` stores bucket, prefix, client, cancellation_token, and request_payer. The `ObjectLister` is a thin wrapper that delegates to `StorageTrait`. The pipeline creates the channel, builds `S3Storage`, spawns `ObjectLister`, and drains the receiver. `ClientConfig` methods (`create_client`, `load_sdk_config`, etc.) live as `impl ClientConfig` methods, not freestanding functions.

**Tech Stack:** Rust 2024, tokio (mpsc channel), aws-sdk-s3, async-trait

**Depends on:** Steps 1 and 2 must be complete (CLIArgs, Config with nested types, types, pipeline scaffold with cancellation token)

**Reference:** s3rm-rs `src/storage/`, `src/lister.rs`

**Note for Step 6:** Add `uuid` to `[dev-dependencies]` in `Cargo.toml` when integration tests are introduced.

---

### Task 1: Create StorageTrait and MockStorage

**Files:**
- Create: `src/storage/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test for MockStorage**

```rust
// src/storage/mod.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};
    use chrono::Utc;

    #[tokio::test]
    async fn mock_storage_list_objects_sends_entries() {
        let entries = vec![
            ListEntry::Object(S3Object::NotVersioning {
                key: "file1.txt".to_string(),
                size: 100,
                last_modified: Utc::now(),
                e_tag: "\"e1\"".to_string(),
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: None,
                checksum_type: None,
            }),
            ListEntry::Object(S3Object::NotVersioning {
                key: "file2.txt".to_string(),
                size: 200,
                last_modified: Utc::now(),
                e_tag: "\"e2\"".to_string(),
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: None,
                checksum_type: None,
            }),
        ];
        let storage = MockStorage::new(entries);
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        storage.list_objects(&tx, 1000).await.unwrap();
        drop(tx);

        let mut received = vec![];
        while let Some(entry) = rx.recv().await {
            received.push(entry);
        }
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "file2.txt");
    }

    #[tokio::test]
    async fn mock_storage_list_object_versions_sends_entries() {
        let entries = vec![
            ListEntry::Object(S3Object::Versioning {
                key: "file1.txt".to_string(),
                version_id: "v1".to_string(),
                size: 100,
                last_modified: Utc::now(),
                e_tag: "\"e1\"".to_string(),
                is_latest: true,
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: None,
                checksum_type: None,
            }),
            ListEntry::DeleteMarker {
                key: "file1.txt".to_string(),
                version_id: "v0".to_string(),
                last_modified: Utc::now(),
                is_latest: false,
            },
        ];
        let storage = MockStorage::new(entries);
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        storage.list_object_versions(&tx, 1000).await.unwrap();
        drop(tx);

        let mut received = vec![];
        while let Some(entry) = rx.recv().await {
            received.push(entry);
        }
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "file1.txt");
    }
}
```

- [ ] **Step 2: Implement StorageTrait and MockStorage**

```rust
// src/storage/mod.rs

pub mod s3;

use async_trait::async_trait;
use crate::types::ListEntry;
use anyhow::Result;
use tokio::sync::mpsc::Sender;

/// Trait for S3 listing operations.
///
/// Pagination is handled internally by implementations. Entries are sent
/// directly through the provided channel sender.
#[async_trait]
pub trait StorageTrait: Send + Sync {
    /// List objects (non-versioned), sending entries through the sender.
    /// Handles pagination internally.
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()>;

    /// List object versions, sending entries through the sender.
    /// Handles pagination internally.
    async fn list_object_versions(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()>;
}

/// Mock storage for unit tests.
pub struct MockStorage {
    entries: Vec<ListEntry>,
}

impl MockStorage {
    pub fn new(entries: Vec<ListEntry>) -> Self {
        Self { entries }
    }
}

#[async_trait]
impl StorageTrait for MockStorage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, _max_keys: i32) -> Result<()> {
        for entry in &self.entries {
            sender.send(entry.clone()).await.ok();
        }
        Ok(())
    }

    async fn list_object_versions(&self, sender: &Sender<ListEntry>, _max_keys: i32) -> Result<()> {
        for entry in &self.entries {
            sender.send(entry.clone()).await.ok();
        }
        Ok(())
    }
}

// tests module here (from Step 1)
```

- [ ] **Step 3: Register storage module in `src/lib.rs`**

Add `pub mod storage;` to `src/lib.rs`.

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test --lib storage 2>&1 | tail -10`

- [ ] **Step 5: Commit**

```bash
git add src/storage/mod.rs src/lib.rs
git commit -m "feat(step3): add StorageTrait and MockStorage with channel-based listing"
```

---

### Task 2: Create S3Storage with client_builder methods on ClientConfig

**Files:**
- Create: `src/storage/s3/mod.rs`
- Create: `src/storage/s3/client_builder.rs`

- [ ] **Step 1: Write `src/storage/s3/client_builder.rs` — methods on `ClientConfig`**

All client-building logic lives as `impl ClientConfig` methods, following s3rm-rs:

```rust
// src/storage/s3/client_builder.rs

use crate::config::{ClientConfig, S3Credentials};
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, ConfigLoader};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use aws_types::SdkConfig;

impl ClientConfig {
    /// Create an S3 client from this configuration.
    pub async fn create_client(&self) -> Client {
        let sdk_config = self.load_sdk_config().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        // Force path style
        if self.force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        // Accelerate
        if self.accelerate {
            s3_config_builder = s3_config_builder.accelerate(true);
        }

        // Timeout config
        if let Some(timeout_config) = self.build_timeout_config() {
            s3_config_builder = s3_config_builder.timeout_config(timeout_config);
        }

        Client::from_conf(s3_config_builder.build())
    }

    async fn load_sdk_config(&self) -> SdkConfig {
        let mut config_loader = aws_config::defaults(BehaviorVersion::latest());

        // Region
        config_loader = config_loader.region(self.build_region_provider());

        // Credentials
        config_loader = self.load_config_credential(config_loader);

        // Endpoint URL — set on the AWS config loader, not the S3 config builder
        if let Some(ref endpoint_url) = self.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        // Retry config
        config_loader = config_loader.retry_config(self.build_retry_config());

        // Stalled stream protection — applied on the config loader, not S3 config builder
        if self.disable_stalled_stream_protection {
            config_loader = config_loader.stalled_stream_protection(
                aws_config::stalled_stream_protection::StalledStreamProtectionConfig::disabled(),
            );
        }

        config_loader.load().await
    }

    fn load_config_credential(&self, config_loader: ConfigLoader) -> ConfigLoader {
        match &self.credential {
            S3Credentials::Credentials { access_keys } => {
                let credentials = aws_sdk_s3::config::Credentials::new(
                    &access_keys.access_key,
                    &access_keys.secret_access_key,
                    access_keys.session_token.clone(),
                    None,
                    "s3ls-rs",
                );
                config_loader.credentials_provider(credentials)
            }
            S3Credentials::Profile(profile) => {
                let mut env_files_builder =
                    aws_config::profile::profile_file::EnvConfigFiles::builder();
                if let Some(ref config_file) = self.aws_config_file {
                    env_files_builder = env_files_builder.with_file(
                        aws_config::profile::profile_file::EnvConfigFileKind::Config,
                        config_file,
                    );
                }
                if let Some(ref creds_file) = self.aws_shared_credentials_file {
                    env_files_builder = env_files_builder.with_file(
                        aws_config::profile::profile_file::EnvConfigFileKind::Credentials,
                        creds_file,
                    );
                }
                let provider = aws_config::profile::ProfileFileCredentialsProvider::builder()
                    .profile_name(profile)
                    .profile_files(env_files_builder.build())
                    .build();
                config_loader.credentials_provider(provider)
            }
            S3Credentials::FromEnvironment => {
                // Use default SDK credential chain (env vars, instance metadata, etc.)
                config_loader
            }
        }
    }

    fn build_region_provider(&self) -> Box<dyn aws_config::meta::region::ProvideRegion> {
        let mut builder = aws_config::profile::ProfileFileRegionProvider::builder();

        if let crate::config::S3Credentials::Profile(ref profile_name) = self.credential {
            if let Some(ref aws_config_file) = self.aws_config_file {
                let profile_files = aws_runtime::env_config::file::EnvConfigFiles::builder()
                    .with_file(aws_runtime::env_config::file::EnvConfigFileKind::Config, aws_config_file)
                    .build();
                builder = builder.profile_files(profile_files);
            }
            builder = builder.profile_name(profile_name);
        }

        let provider_region = if matches!(&self.credential, crate::config::S3Credentials::FromEnvironment) {
            aws_config::meta::region::RegionProviderChain::first_try(self.region.clone().map(aws_types::region::Region::new))
                .or_default_provider()
        } else {
            aws_config::meta::region::RegionProviderChain::first_try(self.region.clone().map(aws_types::region::Region::new))
                .or_else(builder.build())
        };

        Box::new(provider_region)
    }

    fn build_retry_config(&self) -> RetryConfig {
        RetryConfig::standard()
            .with_max_attempts(self.retry_config.aws_max_attempts)
            .with_initial_backoff(std::time::Duration::from_millis(
                self.retry_config.initial_backoff_milliseconds,
            ))
    }

    fn build_timeout_config(&self) -> Option<TimeoutConfig> {
        let tc = &self.cli_timeout_config;

        // Return None if all timeout values are None (no custom timeouts configured)
        if tc.operation_timeout_milliseconds.is_none()
            && tc.operation_attempt_timeout_milliseconds.is_none()
            && tc.connect_timeout_milliseconds.is_none()
            && tc.read_timeout_milliseconds.is_none()
        {
            return None;
        }

        let mut builder = TimeoutConfig::builder();

        if let Some(ms) = tc.operation_timeout_milliseconds {
            builder = builder.operation_timeout(std::time::Duration::from_millis(ms));
        }
        if let Some(ms) = tc.operation_attempt_timeout_milliseconds {
            builder = builder.operation_attempt_timeout(std::time::Duration::from_millis(ms));
        }
        if let Some(ms) = tc.connect_timeout_milliseconds {
            builder = builder.connect_timeout(std::time::Duration::from_millis(ms));
        }
        if let Some(ms) = tc.read_timeout_milliseconds {
            builder = builder.read_timeout(std::time::Duration::from_millis(ms));
        }

        Some(builder.build())
    }
}
```

- [ ] **Step 2: Write `src/storage/s3/mod.rs` — S3Storage struct**

S3Storage stores bucket, prefix, client, cancellation_token, and request_payer. Pagination is fully internal.

```rust
// src/storage/s3/mod.rs

pub mod client_builder;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use anyhow::Result;
use tokio::sync::mpsc::Sender;

use crate::config::ClientConfig;
use crate::storage::StorageTrait;
use crate::types::{ListEntry, S3Object};
use crate::types::token::PipelineCancellationToken;

pub struct S3Storage {
    bucket: String,
    prefix: Option<String>,
    delimiter: Option<String>,
    client: Client,
    cancellation_token: PipelineCancellationToken,
    request_payer: bool,
}

impl S3Storage {
    pub async fn new(
        client_config: &ClientConfig,
        bucket: &str,
        prefix: Option<&str>,
        recursive: bool,
        cancellation_token: PipelineCancellationToken,
        request_payer: bool,
    ) -> Result<Self> {
        let client = client_config.create_client().await;
        Ok(Self {
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            delimiter: if recursive { None } else { Some("/".to_string()) },
            client,
            cancellation_token,
            request_payer,
        })
    }

    fn request_payer_str(&self) -> Option<aws_sdk_s3::types::RequestPayer> {
        if self.request_payer {
            Some(aws_sdk_s3::types::RequestPayer::Requester)
        } else {
            None
        }
    }

    fn convert_object(object: &aws_sdk_s3::types::Object) -> Option<ListEntry> {
        let key = object.key()?.to_string();
        let size = object.size().unwrap_or(0).max(0) as u64;
        let last_modified = object
            .last_modified()
            .and_then(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
            })
            .unwrap_or_else(chrono::Utc::now);
        let e_tag = object.e_tag().unwrap_or("").to_string();
        let storage_class = object.storage_class().map(|sc| sc.as_str().to_string());
        let checksum_algorithm = object
            .checksum_algorithm()
            .first()
            .map(|ca| ca.as_str().to_string());
        let checksum_type = object.checksum_type().map(|ct| ct.as_str().to_string());

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

    fn convert_object_version(version: &aws_sdk_s3::types::ObjectVersion) -> Option<ListEntry> {
        let key = version.key()?.to_string();
        let version_id = version.version_id().unwrap_or("null").to_string();
        let size = version.size().unwrap_or(0).max(0) as u64;
        let last_modified = version
            .last_modified()
            .and_then(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
            })
            .unwrap_or_else(chrono::Utc::now);
        let e_tag = version.e_tag().unwrap_or("").to_string();
        let is_latest = version.is_latest();
        let storage_class = version.storage_class().map(|sc| sc.as_str().to_string());
        let checksum_algorithm = version
            .checksum_algorithm()
            .first()
            .map(|ca| ca.as_str().to_string());
        let checksum_type = version.checksum_type().map(|ct| ct.as_str().to_string());

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

    fn convert_delete_marker(marker: &aws_sdk_s3::types::DeleteMarkerEntry) -> Option<ListEntry> {
        let key = marker.key()?.to_string();
        let version_id = marker.version_id().unwrap_or("null").to_string();
        let last_modified = marker
            .last_modified()
            .and_then(|dt| {
                chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
            })
            .unwrap_or_else(chrono::Utc::now);
        let is_latest = marker.is_latest();

        Some(ListEntry::DeleteMarker {
            key,
            version_id,
            last_modified,
            is_latest,
        })
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()> {
        let mut continuation_token: Option<String> = None;

        loop {
            if self.cancellation_token.is_cancelled() {
                tracing::info!("list_objects cancelled");
                break;
            }

            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .max_keys(max_keys);

            if let Some(ref prefix) = self.prefix {
                request = request.prefix(prefix);
            }
            if let Some(ref token) = continuation_token {
                request = request.continuation_token(token);
            }
            if let Some(ref d) = self.delimiter {
                request = request.delimiter(d);
            }
            if let Some(payer) = self.request_payer_str() {
                request = request.request_payer(payer);
            }

            let response = request.send().await?;

            // Send objects
            if let Some(objects) = response.contents() {
                for object in objects {
                    if let Some(entry) = Self::convert_object(object) {
                        if sender.send(entry).await.is_err() {
                            // Receiver dropped, stop listing
                            return Ok(());
                        }
                    }
                }
            }

            // Send common prefixes (for non-recursive listing with delimiter)
            if let Some(prefixes) = response.common_prefixes() {
                for cp in prefixes {
                    if let Some(prefix) = cp.prefix() {
                        if sender
                            .send(ListEntry::CommonPrefix(prefix.to_string()))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
            }

            // Check for more pages
            match response.next_continuation_token() {
                Some(token) => continuation_token = Some(token.to_string()),
                None => break,
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
                tracing::info!("list_object_versions cancelled");
                break;
            }

            let mut request = self
                .client
                .list_object_versions()
                .bucket(&self.bucket)
                .max_keys(max_keys);

            if let Some(ref prefix) = self.prefix {
                request = request.prefix(prefix);
            }
            if let Some(ref marker) = key_marker {
                request = request.key_marker(marker);
            }
            if let Some(ref marker) = version_id_marker {
                request = request.version_id_marker(marker);
            }
            if let Some(ref d) = self.delimiter {
                request = request.delimiter(d);
            }
            if let Some(payer) = self.request_payer_str() {
                request = request.request_payer(payer);
            }

            let response = request.send().await?;

            // Send object versions
            if let Some(versions) = response.versions() {
                for version in versions {
                    if let Some(entry) = Self::convert_object_version(version) {
                        if sender.send(entry).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }

            // Send delete markers
            if let Some(markers) = response.delete_markers() {
                for marker in markers {
                    if let Some(entry) = Self::convert_delete_marker(marker) {
                        if sender.send(entry).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }

            // Send common prefixes
            if let Some(prefixes) = response.common_prefixes() {
                for cp in prefixes {
                    if let Some(prefix) = cp.prefix() {
                        if sender
                            .send(ListEntry::CommonPrefix(prefix.to_string()))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
            }

            // Check for more pages
            if response.is_truncated() {
                key_marker = response.next_key_marker().map(|s| s.to_string());
                version_id_marker = response.next_version_id_marker().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(())
    }
}
```

- [ ] **Step 3: Verify compilation**

Run: `cargo check 2>&1 | tail -10`
Expected: compiles (no runtime test without real S3)

- [ ] **Step 4: Commit**

```bash
git add src/storage/
git commit -m "feat(step3): add S3Storage with client_builder methods on ClientConfig"
```

---

### Task 3: Implement ObjectLister

**Files:**
- Create: `src/lister.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/lister.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MockStorage;
    use crate::types::{ListEntry, S3Object};
    use chrono::Utc;

    #[tokio::test]
    async fn lister_sends_objects_to_channel() {
        let entries = vec![
            ListEntry::Object(S3Object::NotVersioning {
                key: "a.txt".to_string(),
                size: 10,
                last_modified: Utc::now(),
                e_tag: "\"e\"".to_string(),
                storage_class: None,
                checksum_algorithm: None,
                checksum_type: None,
            }),
            ListEntry::Object(S3Object::NotVersioning {
                key: "b.txt".to_string(),
                size: 20,
                last_modified: Utc::now(),
                e_tag: "\"f\"".to_string(),
                storage_class: None,
                checksum_algorithm: None,
                checksum_type: None,
            }),
        ];
        let storage: Arc<dyn StorageTrait> = Arc::new(MockStorage::new(entries));
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let lister = ObjectLister {
            storage,
            sender: tx,
            all_versions: false,
            max_keys: 1000,
        };
        lister.list_target().await.unwrap();

        // Sender dropped when lister is consumed, so rx.recv() will return None after all entries
        let mut received = vec![];
        while let Some(entry) = rx.recv().await {
            received.push(entry);
        }
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].key(), "a.txt");
        assert_eq!(received[1].key(), "b.txt");
    }

    #[tokio::test]
    async fn lister_uses_versions_when_all_versions_set() {
        let entries = vec![
            ListEntry::Object(S3Object::Versioning {
                key: "a.txt".to_string(),
                version_id: "v1".to_string(),
                size: 10,
                last_modified: Utc::now(),
                e_tag: "\"e\"".to_string(),
                is_latest: true,
                storage_class: None,
                checksum_algorithm: None,
                checksum_type: None,
            }),
        ];
        let storage: Arc<dyn StorageTrait> = Arc::new(MockStorage::new(entries));
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        let lister = ObjectLister {
            storage,
            sender: tx,
            all_versions: true,
            max_keys: 1000,
        };
        lister.list_target().await.unwrap();

        let mut received = vec![];
        while let Some(entry) = rx.recv().await {
            received.push(entry);
        }
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].key(), "a.txt");
    }
}
```

- [ ] **Step 2: Implement ObjectLister**

The ObjectLister is a thin wrapper that delegates to StorageTrait, following s3rm-rs:

```rust
// src/lister.rs

use crate::storage::StorageTrait;
use crate::types::ListEntry;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct ObjectLister {
    pub storage: Arc<dyn StorageTrait>,
    pub sender: mpsc::Sender<ListEntry>,
    pub all_versions: bool,
    pub max_keys: i32,
}

impl ObjectLister {
    pub async fn list_target(self) -> Result<()> {
        if self.all_versions {
            self.storage
                .list_object_versions(&self.sender, self.max_keys)
                .await
        } else {
            self.storage
                .list_objects(&self.sender, self.max_keys)
                .await
        }
    }
}

// tests module here (from Step 1)
```

- [ ] **Step 3: Register lister module in `src/lib.rs`**

Add `pub mod lister;` to `src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib lister 2>&1 | tail -10`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/lister.rs src/lib.rs
git commit -m "feat(step3): add ObjectLister as thin wrapper delegating to StorageTrait"
```

---

### Task 4: Wire lister into pipeline with temporary aggregate

**Files:**
- Modify: `src/pipeline.rs`

- [ ] **Step 1: Update pipeline to create channel, spawn lister, drain and print**

Update `ListingPipeline::run()` following s3rm-rs pipeline wiring pattern:

```rust
// src/pipeline.rs

use crate::config::Config;
use crate::lister::ObjectLister;
use crate::storage::StorageTrait;
use crate::storage::s3::S3Storage;
use crate::types::token::PipelineCancellationToken;
use anyhow::{bail, Result};
use std::sync::Arc;

pub struct ListingPipeline {
    config: Config,
    cancellation_token: PipelineCancellationToken,
    #[cfg(test)]
    storage_override: Option<Arc<dyn StorageTrait>>,
}

impl ListingPipeline {
    pub fn new(config: Config, cancellation_token: PipelineCancellationToken) -> Self {
        Self {
            config,
            cancellation_token,
            #[cfg(test)]
            storage_override: None,
        }
    }

    #[cfg(test)]
    fn with_storage(
        config: Config,
        cancellation_token: PipelineCancellationToken,
        storage: Arc<dyn StorageTrait>,
    ) -> Self {
        Self {
            config,
            cancellation_token,
            storage_override: Some(storage),
        }
    }

    pub async fn run(self) -> Result<()> {
        tracing::info!(
            target = %self.config.target,
            recursive = self.config.recursive,
            "Starting listing pipeline"
        );

        if self.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Create bounded channel
        let (tx, mut rx) =
            tokio::sync::mpsc::channel(self.config.object_listing_queue_size as usize);

        // Build storage
        let storage: Arc<dyn StorageTrait> = {
            #[cfg(test)]
            {
                if let Some(s) = self.storage_override {
                    s
                } else {
                    self.build_s3_storage().await?
                }
            }
            #[cfg(not(test))]
            {
                self.build_s3_storage().await?
            }
        };

        // Spawn lister
        let lister = ObjectLister {
            storage,
            sender: tx,
            all_versions: self.config.all_versions,
            max_keys: self.config.max_keys,
        };

        let lister_handle = tokio::spawn(async move { lister.list_target().await });

        // Temporary drain: print each entry key to stdout
        // (will be replaced by formatter/aggregate in Step 5)
        while let Some(entry) = rx.recv().await {
            if self.cancellation_token.is_cancelled() {
                break;
            }
            println!("{}", entry.key());
        }

        lister_handle.await??;
        Ok(())
    }

    async fn build_s3_storage(&self) -> Result<Arc<dyn StorageTrait>> {
        let client_config = self
            .config
            .target_client_config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No client configuration provided"))?;

        let storage = S3Storage::new(
            client_config,
            &self.config.target.bucket,
            self.config.target.prefix.as_deref(),
            self.config.recursive,
            self.cancellation_token.clone(),
            client_config.request_payer,
        )
        .await?;

        Ok(Arc::new(storage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MockStorage;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::types::{ListEntry, S3Object};
    use chrono::Utc;

    #[tokio::test]
    async fn pipeline_runs_and_returns_success() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        let storage: Arc<dyn StorageTrait> = Arc::new(MockStorage::new(vec![]));
        let pipeline = ListingPipeline::with_storage(config, token, storage);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pipeline_respects_cancellation() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let storage: Arc<dyn StorageTrait> = Arc::new(MockStorage::new(vec![]));
        let pipeline = ListingPipeline::with_storage(config, token, storage);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pipeline_lists_mock_objects() {
        let entries = vec![
            ListEntry::Object(S3Object::NotVersioning {
                key: "file1.txt".to_string(),
                size: 100,
                last_modified: Utc::now(),
                e_tag: "\"e1\"".to_string(),
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: None,
                checksum_type: None,
            }),
            ListEntry::Object(S3Object::NotVersioning {
                key: "file2.txt".to_string(),
                size: 200,
                last_modified: Utc::now(),
                e_tag: "\"e2\"".to_string(),
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: None,
                checksum_type: None,
            }),
        ];

        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        let storage: Arc<dyn StorageTrait> = Arc::new(MockStorage::new(entries));
        let pipeline = ListingPipeline::with_storage(config, token, storage);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }
}
```

- [ ] **Step 2: Run all tests**

Run: `cargo test 2>&1 | tail -15`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add src/pipeline.rs
git commit -m "feat(step3): wire lister into pipeline with temporary stdout drain, step 3 complete"
```
