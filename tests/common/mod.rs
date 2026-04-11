//! Shared E2E test infrastructure for s3ls-rs.
//!
//! Provides `TestHelper` for bucket management, object operations, and
//! pipeline/binary execution against real AWS S3. All helpers use the
//! `s3ls-e2e-test` AWS profile.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Delete,
    ObjectIdentifier, VersioningConfiguration,
};
use uuid::Uuid;

/// AWS profile used for all E2E tests.
const AWS_PROFILE: &str = "s3ls-e2e-test";

/// Default region for E2E tests (used when the profile doesn't set one).
const DEFAULT_REGION: &str = "us-east-1";

/// Shared test helper for E2E tests.
///
/// Wraps an AWS S3 `Client` built with the `s3ls-e2e-test` profile and provides
/// convenience methods for bucket management, object operations, and pipeline execution.
pub struct TestHelper {
    client: Client,
    region: String,
}

impl TestHelper {
    /// Create a new TestHelper with an S3 client configured via the e2e test profile.
    pub async fn new() -> Arc<Self> {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .profile_name(AWS_PROFILE)
            .load()
            .await;

        let region = sdk_config
            .region()
            .map(|r| r.to_string())
            .unwrap_or_else(|| DEFAULT_REGION.to_string());

        let client = Client::new(&sdk_config);

        Arc::new(Self { client, region })
    }

    /// Return the AWS region this helper is configured for.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Return a reference to the underlying S3 client for advanced operations.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Generate a unique bucket name for test isolation.
    ///
    /// Returns a name like `s3ls-e2e-{uuid}` which is guaranteed unique
    /// across parallel test runs. 9 + 36 = 45 chars, under S3's 63-char limit.
    pub fn generate_bucket_name(&self) -> String {
        format!("s3ls-e2e-{}", Uuid::new_v4())
    }
}

/// Guard that cleans up a test bucket when explicitly consumed.
///
/// Call [`BucketGuard::cleanup`] at the end of each test to delete all objects
/// and the bucket itself. If the test panics before `cleanup` is reached, the
/// bucket is intentionally left behind — this avoids the `block_on`-in-`Drop`
/// footgun that can cause double-panic aborts when the Tokio runtime is
/// shutting down.
pub struct BucketGuard {
    helper: Arc<TestHelper>,
    bucket: String,
}

impl BucketGuard {
    /// Delete all objects and the bucket. Call this at the end of each test.
    pub async fn cleanup(self) {
        self.helper.delete_bucket_cascade(&self.bucket).await;
    }
}

impl TestHelper {
    /// Create a guard for test bucket cleanup.
    pub fn bucket_guard(self: &Arc<Self>, bucket: &str) -> BucketGuard {
        BucketGuard {
            helper: Arc::clone(self),
            bucket: bucket.to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // Bucket management
    // -----------------------------------------------------------------------

    /// Create a standard (non-versioned) S3 bucket.
    pub async fn create_bucket(&self, bucket: &str) {
        let mut builder = self.client.create_bucket().bucket(bucket);

        // us-east-1 must NOT specify a location constraint
        if self.region != "us-east-1" {
            let constraint = BucketLocationConstraint::from(self.region.as_str());
            let config = CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build();
            builder = builder.create_bucket_configuration(config);
        }

        builder
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to create bucket {bucket}: {e}"));
    }

    /// Create a versioned S3 bucket (create bucket + enable versioning).
    pub async fn create_versioned_bucket(&self, bucket: &str) {
        self.create_bucket(bucket).await;

        let versioning_config = VersioningConfiguration::builder()
            .status(BucketVersioningStatus::Enabled)
            .build();

        self.client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(versioning_config)
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to enable versioning on {bucket}: {e}"));
    }

    /// Delete all objects (including versions and delete markers) and then delete the bucket.
    ///
    /// Best-effort: errors are swallowed so cleanup never panics during teardown
    /// (which would mask the real test failure).
    pub async fn delete_bucket_cascade(&self, bucket: &str) {
        self.delete_all_versions(bucket).await;
        self.delete_all_objects(bucket).await;
        let _ = self.client.delete_bucket().bucket(bucket).send().await;
    }

    /// Delete all object versions and delete markers from a bucket.
    async fn delete_all_versions(&self, bucket: &str) {
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            let mut req = self.client.list_object_versions().bucket(bucket);
            if let Some(ref km) = key_marker {
                req = req.key_marker(km);
            }
            if let Some(ref vim) = version_id_marker {
                req = req.version_id_marker(vim);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(_) => return, // Bucket may not exist or no permission
            };

            let mut objects_to_delete: Vec<ObjectIdentifier> = Vec::new();

            for v in resp.versions() {
                if let (Some(key), Some(vid)) = (v.key(), v.version_id()) {
                    objects_to_delete.push(
                        ObjectIdentifier::builder()
                            .key(key)
                            .version_id(vid)
                            .build()
                            .unwrap(),
                    );
                }
            }

            for m in resp.delete_markers() {
                if let (Some(key), Some(vid)) = (m.key(), m.version_id()) {
                    objects_to_delete.push(
                        ObjectIdentifier::builder()
                            .key(key)
                            .version_id(vid)
                            .build()
                            .unwrap(),
                    );
                }
            }

            for chunk in objects_to_delete.chunks(1000) {
                let delete = Delete::builder()
                    .set_objects(Some(chunk.to_vec()))
                    .quiet(true)
                    .build()
                    .unwrap();
                let _ = self
                    .client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(delete)
                    .send()
                    .await;
            }

            // Treat missing `is_truncated` as "not truncated" — safer to stop than loop.
            if !resp.is_truncated().unwrap_or(false) {
                break;
            }

            let next_km = resp.next_key_marker().map(|s| s.to_string());
            let next_vim = resp.next_version_id_marker().map(|s| s.to_string());

            // Defensive: if truncated but no forward-progress markers, break to
            // avoid an infinite loop on malformed S3-compatible responses.
            if next_km.is_none() && next_vim.is_none() {
                break;
            }

            key_marker = next_km;
            version_id_marker = next_vim;
        }
    }

    /// Delete all non-versioned objects from a bucket.
    async fn delete_all_objects(&self, bucket: &str) {
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket);
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(_) => return,
            };

            let contents = resp.contents();
            if contents.is_empty() {
                break;
            }

            let objects: Vec<ObjectIdentifier> = contents
                .iter()
                .filter_map(|obj| {
                    obj.key()
                        .map(|k| ObjectIdentifier::builder().key(k).build().unwrap())
                })
                .collect();

            if !objects.is_empty() {
                let delete = Delete::builder()
                    .set_objects(Some(objects))
                    .quiet(true)
                    .build()
                    .unwrap();
                let _ = self
                    .client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(delete)
                    .send()
                    .await;
            }

            // Treat missing `is_truncated` as "not truncated" — safer to stop than loop.
            if !resp.is_truncated().unwrap_or(false) {
                break;
            }

            let next_token = resp.next_continuation_token().map(|s| s.to_string());

            // Defensive: if truncated but no continuation token, break to avoid
            // an infinite loop on malformed S3-compatible responses.
            if next_token.is_none() {
                break;
            }

            continuation_token = next_token;
        }
    }
}

impl TestHelper {
    // -----------------------------------------------------------------------
    // Object operations
    // -----------------------------------------------------------------------

    /// Upload an object with the given body bytes.
    pub async fn put_object(&self, bucket: &str, key: &str, body: Vec<u8>) {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put object {key} in {bucket}: {e}"));
    }

    /// Upload an object with an explicit Content-Type.
    pub async fn put_object_with_content_type(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .content_type(content_type)
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to put object {key} with content-type {content_type}: {e}")
            });
    }

    /// Upload an object with user-defined metadata.
    pub async fn put_object_with_metadata(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        metadata: HashMap<String, String>,
    ) {
        let mut builder = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into());

        for (k, v) in &metadata {
            builder = builder.metadata(k, v);
        }

        builder
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put object {key} with metadata: {e}"));
    }

    /// Upload an object with S3 tags.
    ///
    /// NOTE: The caller is responsible for ensuring tag keys and values are
    /// URL-safe. This helper does not URL-encode — a key or value containing
    /// `=`, `&`, `+`, `%`, or non-ASCII characters will confuse S3's tagging
    /// parser. Test fixtures should stick to alphanumerics.
    pub async fn put_object_with_tags(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        tags: HashMap<String, String>,
    ) {
        let tag_string: String = tags
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .tagging(&tag_string)
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put object {key} with tags: {e}"));
    }

    /// Upload an object with content-type, metadata, AND tags all at once.
    ///
    /// NOTE: Same URL-safety caveat as `put_object_with_tags` — tag keys and
    /// values must not contain `=`, `&`, `+`, `%`, or non-ASCII characters.
    pub async fn put_object_full(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
        metadata: HashMap<String, String>,
        tags: HashMap<String, String>,
    ) {
        let tag_string: String = tags
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        let mut builder = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .content_type(content_type)
            .tagging(&tag_string);

        for (k, v) in &metadata {
            builder = builder.metadata(k, v);
        }

        builder
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put object {key} with full properties: {e}"));
    }

    /// Upload multiple objects in parallel (up to 16 concurrent uploads).
    pub async fn put_objects_parallel(&self, bucket: &str, objects: Vec<(String, Vec<u8>)>) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(16));
        let mut set = tokio::task::JoinSet::new();
        let client = self.client.clone();
        let bucket = bucket.to_string();

        for (key, body) in objects {
            let client = client.clone();
            let bucket = bucket.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            set.spawn(async move {
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(body.into())
                    .send()
                    .await
                    .unwrap_or_else(|e| panic!("Failed to put object {key} in {bucket}: {e}"));
                drop(permit);
            });
        }

        while let Some(result) = set.join_next().await {
            result.expect("Upload task panicked");
        }
    }

    /// List remaining object keys under the given prefix.
    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .unwrap_or_else(|e| panic!("Failed to list objects in {bucket}/{prefix}: {e}"));

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        keys
    }

    /// List object versions in a bucket. Returns (key, version_id) pairs.
    /// Delete markers are returned with a "[delete-marker]" prefix on the key.
    pub async fn list_object_versions(&self, bucket: &str) -> Vec<(String, String)> {
        let mut result: Vec<(String, String)> = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            let mut req = self.client.list_object_versions().bucket(bucket);
            if let Some(ref km) = key_marker {
                req = req.key_marker(km);
            }
            if let Some(ref vim) = version_id_marker {
                req = req.version_id_marker(vim);
            }

            let resp = req
                .send()
                .await
                .unwrap_or_else(|e| panic!("Failed to list object versions in {bucket}: {e}"));

            for v in resp.versions() {
                if let (Some(key), Some(vid)) = (v.key(), v.version_id()) {
                    result.push((key.to_string(), vid.to_string()));
                }
            }

            for m in resp.delete_markers() {
                if let (Some(key), Some(vid)) = (m.key(), m.version_id()) {
                    result.push((format!("[delete-marker]{key}"), vid.to_string()));
                }
            }

            if resp.is_truncated() == Some(true) {
                key_marker = resp.next_key_marker().map(|s| s.to_string());
                version_id_marker = resp.next_version_id_marker().map(|s| s.to_string());
            } else {
                break;
            }
        }

        result
    }

    /// Count objects remaining under the given prefix.
    pub async fn count_objects(&self, bucket: &str, prefix: &str) -> usize {
        self.list_objects(bucket, prefix).await.len()
    }
}

// ---------------------------------------------------------------------------
// Step 1: S3lsOutput and run_s3ls
// ---------------------------------------------------------------------------

/// Captured output of a single `s3ls` binary invocation.
///
/// Stdout and stderr are pre-decoded from UTF-8 (lossy) so tests can use
/// them as `&str` without repeating the decode boilerplate.
pub struct S3lsOutput {
    pub stdout: String,
    pub stderr: String,
    pub status: std::process::ExitStatus,
}

impl TestHelper {
    /// Run the s3ls binary with the given args and return captured output.
    ///
    /// Auto-appends `--target-profile s3ls-e2e-test` unless the args already
    /// contain `--target-profile` or `--target-access-key`. This shadows any
    /// inherited `AWS_PROFILE` env var on the calling shell, which is the
    /// safer default for E2E tests.
    ///
    /// Synchronous by design: the framework does no other work while waiting
    /// for a single subprocess, and a blocking spawn is simpler than
    /// `tokio::process::Command`.
    pub fn run_s3ls(args: &[&str]) -> S3lsOutput {
        let has_profile = args.iter().any(|a| a.starts_with("--target-profile"));
        let has_access_key = args.iter().any(|a| a.starts_with("--target-access-key"));

        let mut full_args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        if !has_profile && !has_access_key {
            full_args.push("--target-profile".to_string());
            full_args.push(AWS_PROFILE.to_string());
        }

        let output = std::process::Command::new(env!("CARGO_BIN_EXE_s3ls"))
            .args(&full_args)
            .output()
            .expect("Failed to spawn s3ls binary");

        S3lsOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            status: output.status,
        }
    }
}

// ---------------------------------------------------------------------------
// Step 2: build_config and run_pipeline
// ---------------------------------------------------------------------------

use s3ls_rs::{Config, ListingPipeline, create_pipeline_cancellation_token};

impl TestHelper {
    // -----------------------------------------------------------------------
    // Programmatic pipeline helpers
    // -----------------------------------------------------------------------

    /// Build a `Config` from CLI-style args.
    ///
    /// Automatically prepends the binary name ("s3ls") and appends
    /// `--target-profile s3ls-e2e-test` unless the args already contain
    /// `--target-profile` or `--target-access-key`. Panics on build failure.
    pub fn build_config(args: Vec<&str>) -> Config {
        let mut full_args: Vec<String> = vec!["s3ls".to_string()];
        full_args.extend(args.iter().map(|s| s.to_string()));

        let has_profile = full_args.iter().any(|a| a.starts_with("--target-profile"));
        let has_access_key = full_args
            .iter()
            .any(|a| a.starts_with("--target-access-key"));
        if !has_profile && !has_access_key {
            full_args.push("--target-profile".to_string());
            full_args.push(AWS_PROFILE.to_string());
        }

        s3ls_rs::build_config_from_args(full_args)
            .unwrap_or_else(|e| panic!("Failed to build config from args: {e}"))
    }

    /// Construct a `ListingPipeline` and run it to completion.
    ///
    /// Returns `ListingPipeline::run`'s `anyhow::Result<()>`. Intended for
    /// tests that assert on pipeline behavior (error paths, cancellation,
    /// credential loading) rather than rendered output — rendered output is
    /// asserted via the binary path (`run_s3ls`).
    pub async fn run_pipeline(config: Config) -> anyhow::Result<()> {
        let token = create_pipeline_cancellation_token();
        let pipeline = ListingPipeline::new(config, token);
        pipeline.run().await
    }
}

// ---------------------------------------------------------------------------
// Step 3: Timeout infrastructure and assert_key_order
// ---------------------------------------------------------------------------

/// Default hard timeout for E2E tests (60 seconds).
///
/// Generous for single listing operations but prevents indefinite hangs on
/// network stalls or cancellation bugs. Tune here if the suite grows.
pub const E2E_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Wraps an async E2E test body with a hard timeout.
///
/// Usage:
/// ```ignore
/// #[tokio::test]
/// async fn e2e_my_test() {
///     e2e_timeout!(async {
///         // test body here
///     });
/// }
/// ```
#[macro_export]
macro_rules! e2e_timeout {
    ($body:expr) => {
        tokio::time::timeout(common::E2E_TIMEOUT, $body)
            .await
            .expect("E2E test timed out")
    };
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

/// Assert that the given keys appear in `stdout` in the specified order.
///
/// Panics with a descriptive message (including the full stdout) if any key
/// is missing or out of order. Uses byte-offset comparison via `str::find`;
/// works for any text output where each expected key appears at most once.
pub fn assert_key_order(stdout: &str, expected_order: &[&str]) {
    let positions: Vec<(usize, &str)> = expected_order
        .iter()
        .map(|key| {
            let pos = stdout
                .find(key)
                .unwrap_or_else(|| panic!("key {key:?} not found in stdout:\n{stdout}"));
            (pos, *key)
        })
        .collect();

    for window in positions.windows(2) {
        let (pos_a, key_a) = window[0];
        let (pos_b, key_b) = window[1];
        assert!(
            pos_a < pos_b,
            "expected {key_a:?} before {key_b:?}; got positions {pos_a} vs {pos_b} in stdout:\n{stdout}"
        );
    }
}
