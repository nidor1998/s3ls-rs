//! Shared E2E test infrastructure for s3ls-rs.
//!
//! Provides `TestHelper` for bucket management, object operations, and
//! pipeline/binary execution against real AWS S3. All helpers use the
//! `s3ls-e2e-test` AWS profile.

#![allow(dead_code)]

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

            if resp.is_truncated() == Some(true) {
                key_marker = resp.next_key_marker().map(|s| s.to_string());
                version_id_marker = resp.next_version_id_marker().map(|s| s.to_string());
            } else {
                break;
            }
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

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
    }
}
