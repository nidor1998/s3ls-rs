# Step 6: E2E Test Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the e2e test framework for s3ls-rs in `tests/common/mod.rs`, modeled on s3rm-rs, plus two smoke tests in `tests/e2e_listing.rs` that exercise every framework seam.

**Architecture:** Gated via `#![cfg(e2e_test)]` (cfg flag already pre-registered in `Cargo.toml`). Single `TestHelper` struct wraps an `aws-sdk-s3` client configured via the `s3ls-e2e-test` AWS profile. Per-test bucket isolation (`s3ls-e2e-{uuid}`) with explicit `BucketGuard::cleanup().await` (no `Drop`, to avoid the `block_on`-in-`Drop` footgun). Binary runner `run_s3ls` is primary for output assertions; programmatic `run_pipeline` is secondary for pipeline-behavior tests. 60-second hard timeout per test via `e2e_timeout!` macro.

**Tech Stack:** Rust 2024, tokio, aws-config, aws-sdk-s3 (production deps, reused by tests), uuid v4 (new dev-dep), `std::process::Command` for binary invocation.

**Depends on:** Steps 1–5 (complete s3ls tool with `ListingPipeline`, `Config`, `build_config_from_args` public API).

**Spec:** `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md`

**Supersedes:** Task 1 (framework sketch) of `docs/superpowers/plans/2026-04-04-step6-e2e-tests.md`. The per-feature test tasks in that old plan (filters, output, versions) are out of scope here and will be re-planned per feature area after this framework lands.

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- Auto-memory feedback: **logging via `tracing` / `tracing-subscriber`**, not `log` directly. (Not relevant to this plan — no new logging code is added — but noted.)

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/common/mod.rs` | `TestHelper`, `BucketGuard`, `S3lsOutput`, `E2E_TIMEOUT`, `e2e_timeout!` macro, `assert_key_order`. Single shared framework module. |
| `tests/e2e_listing.rs` | Two smoke tests (`e2e_binary_smoke`, `e2e_programmatic_smoke`). Gated with `#![cfg(e2e_test)]`. |
| `tests/README.md` | How to run, prerequisites, manual cleanup, CI note. |

**Modified files:**

| Path | Change |
|---|---|
| `Cargo.toml` | Add `uuid = { version = "1", features = ["v4"] }` to `[dev-dependencies]`. |

**No changes to `src/`.** The framework uses the existing public API as-is.

---

## Important notes for the executor

**AWS SDK is a production dep.** `aws-config` and `aws-sdk-s3` are already in `[dependencies]` — do NOT re-declare them in `[dev-dependencies]`. The only new dev-dep is `uuid`.

**`tests/common/` is a directory module.** Rust's integration-test harness compiles every top-level `.rs` file under `tests/` as its own binary. Using a plain `tests/common.rs` would create a binary with zero tests and emit a warning. The `tests/common/mod.rs` directory layout is the idiomatic workaround — Cargo ignores subdirectories, so `common` becomes a shared module that `e2e_*.rs` files import via `mod common;`.

**Do NOT add `#![cfg(e2e_test)]` at the top of `tests/common/mod.rs`.** The module is only compiled when a gated test file imports it via `mod common;`, so the gating is already transitive. Adding the attribute to `mod.rs` creates confusing "unused import" warnings in the framework under non-gated builds.

**`#![allow(dead_code)]` at the top of `tests/common/mod.rs` is required.** The framework exposes helpers (versioned-bucket creation, metadata/tag uploads, parallel uploads, list helpers) that the two smoke tests don't exercise. Without the allow, clippy complains. s3rm-rs uses the same pattern.

**Verification commands used throughout this plan:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — must pass with no e2e tests compiled in. |
| `cargo build --tests` | Non-gated test build — ensures no dead code warnings leak from a plain non-cfg build. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build — compiles the framework and smoke tests. Does NOT hit S3. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint — catches framework bugs. |
| `cargo clippy --all-features -- -D warnings` | Non-gated lint — unchanged surface must stay clean. |
| `cargo fmt --check` | Formatting — must pass. |
| `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing -- --nocapture` | **Manual only.** Final verification against real S3 with a configured `s3ls-e2e-test` profile. Does NOT run in CI. Only Task 7 mentions this. |

---

### Task 1: Add `uuid` dev-dependency

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add the dependency line**

In `Cargo.toml`, under `[dev-dependencies]`, add a `uuid` entry. The section currently looks like:

```toml
[dev-dependencies]
proptest = "1.11"
once_cell = "1.21.4"
nix = { version = "0.31.2", features = ["process", "signal"] }
rusty-fork = "0.3.1"
```

Add this line immediately after `rusty-fork`:

```toml
uuid = { version = "1", features = ["v4"] }
```

- [ ] **Step 2: Verify the lockfile and build**

Run: `cargo build --tests`
Expected: Successful build, `Cargo.lock` updated with a new `uuid` entry. No warnings.

- [ ] **Step 3: Verify non-gated build still clean**

Run: `cargo test`
Expected: All existing tests pass. No e2e tests yet.

- [ ] **Step 4: Commit**

```bash
cargo fmt
cargo clippy --all-features -- -D warnings
git add Cargo.toml Cargo.lock
git commit -m "$(cat <<'EOF'
chore(step6): add uuid dev-dependency for e2e test framework

Prep for tests/common/mod.rs (e2e test framework). uuid v4 is
used to generate unique per-test bucket names of the form
s3ls-e2e-{uuid}.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Scaffold framework — `TestHelper` core, `BucketGuard`, bucket lifecycle

**Files:**
- Create: `tests/common/mod.rs`
- Create: `tests/e2e_listing.rs`

This task lays down the framework module with the bucket-lifecycle portion only. A trivial sanity test in `e2e_listing.rs` exercises `TestHelper::new()` and `generate_bucket_name()` to prove compilation. Full-S3 smoke tests land in Task 5.

- [ ] **Step 1: Create `tests/common/mod.rs` with imports and `TestHelper` struct**

Create `tests/common/mod.rs` with the following content. Write the whole file in this step; later steps will append new sections.

```rust
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
```

- [ ] **Step 2: Append `BucketGuard` and bucket-lifecycle methods**

Append the following to `tests/common/mod.rs`:

```rust
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
```

- [ ] **Step 3: Create `tests/e2e_listing.rs` with a sanity test**

Create `tests/e2e_listing.rs` with this content. The sanity test compiles under `--cfg e2e_test`, constructs a `TestHelper` (no S3 network calls), and verifies the bucket name format. It will be REPLACED with the real smoke tests in Task 5.

```rust
#![cfg(e2e_test)]

mod common;

use common::TestHelper;

#[tokio::test]
async fn e2e_sanity() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    assert!(bucket.starts_with("s3ls-e2e-"));
    assert_eq!(bucket.len(), 9 + 36); // "s3ls-e2e-" + UUID v4
}
```

- [ ] **Step 4: Verify non-gated build stays clean**

Run: `cargo test`
Expected: All existing tests pass. No e2e tests compiled (the files are `#![cfg(e2e_test)]`).

- [ ] **Step 5: Verify gated build compiles**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: Successful build. Both `tests/common/mod.rs` and `tests/e2e_listing.rs` compile.

- [ ] **Step 6: Verify gated clippy is clean**

Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: No warnings, no errors.

- [ ] **Step 7: Commit**

```bash
cargo fmt
cargo clippy --all-features -- -D warnings
git add tests/
git commit -m "$(cat <<'EOF'
test(step6): scaffold e2e framework with TestHelper and bucket lifecycle

Adds tests/common/mod.rs with TestHelper (new, region, client,
generate_bucket_name), BucketGuard (explicit cleanup, no Drop), and
bucket-lifecycle methods (create_bucket, create_versioned_bucket,
delete_bucket_cascade with cascade version/marker/object deletion).

Adds tests/e2e_listing.rs with a sanity test exercising TestHelper
construction. The real smoke tests will replace it in a later commit.

All gated under #![cfg(e2e_test)]; non-gated cargo test unchanged.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add object operations to `TestHelper`

**Files:**
- Modify: `tests/common/mod.rs`

Ports the object-operation helpers from s3rm-rs. None of them are called by the sanity test yet; `#![allow(dead_code)]` at the top of `mod.rs` suppresses warnings.

- [ ] **Step 1: Append object operations**

Append a new `impl TestHelper` block to `tests/common/mod.rs`. This block lives after the bucket-lifecycle block added in Task 2.

```rust
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
```

- [ ] **Step 2: Verify gated build still compiles**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: Successful build. Dead-code warnings suppressed by `#![allow(dead_code)]`.

- [ ] **Step 3: Verify gated clippy is clean**

Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: No warnings.

- [ ] **Step 4: Verify non-gated build stays clean**

Run: `cargo test`
Expected: All existing tests pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt
cargo clippy --all-features -- -D warnings
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(step6): add object-operation helpers to e2e TestHelper

Ports put_object variants (plain, content-type, metadata, tags, full),
put_objects_parallel (16-way concurrent via semaphore + JoinSet), and
listing helpers (list_objects, list_object_versions, count_objects)
from s3rm-rs. Unused by current smoke tests but needed by later
feature-area e2e tests; allowed by #![allow(dead_code)].

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add runners, timeout infrastructure, and `assert_key_order`

**Files:**
- Modify: `tests/common/mod.rs`

Adds the binary runner (`run_s3ls`), programmatic runner (`build_config`, `run_pipeline`), `S3lsOutput`, timeout infrastructure (`E2E_TIMEOUT`, `e2e_timeout!` macro), and the `assert_key_order` helper.

- [ ] **Step 1: Add `S3lsOutput` and `run_s3ls`**

Append the following to `tests/common/mod.rs`:

```rust
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
```

- [ ] **Step 2: Add `build_config` and `run_pipeline`**

Append the following to `tests/common/mod.rs`. Note that `build_config_from_args` is in the public API of `s3ls_rs` and returns `Result<Config, String>`; the panic message formats the `String` directly.

```rust
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
```

- [ ] **Step 3: Add timeout infrastructure and `assert_key_order`**

Append the following to `tests/common/mod.rs`:

```rust
// ---------------------------------------------------------------------------
// Timeout infrastructure
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
```

- [ ] **Step 4: Verify gated build still compiles**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: Successful build.

- [ ] **Step 5: Verify gated clippy is clean**

Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: No warnings.

- [ ] **Step 6: Verify non-gated build stays clean**

Run: `cargo test`
Expected: All existing tests pass.

- [ ] **Step 7: Commit**

```bash
cargo fmt
cargo clippy --all-features -- -D warnings
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(step6): add e2e runners, timeout macro, and assert_key_order

- S3lsOutput + run_s3ls: binary invocation via CARGO_BIN_EXE_s3ls, auto
  profile injection, pre-decoded UTF-8 stdout/stderr.
- build_config + run_pipeline: programmatic path using public API
  (Config, ListingPipeline, create_pipeline_cancellation_token).
- E2E_TIMEOUT const + e2e_timeout! macro for 60s hard per-test timeouts.
- assert_key_order helper for sort-order assertions with self-diagnosing
  panic messages.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Replace sanity test with the two real smoke tests

**Files:**
- Modify: `tests/e2e_listing.rs`

Replaces the sanity test from Task 2 with the two real smoke tests that exercise the full framework against real S3. The binary smoke test uses `assert_key_order` to verify s3ls's default key-sort is stable; the programmatic smoke test exercises the `build_config` / `ListingPipeline::new` / `run` path.

- [ ] **Step 1: Replace the file content**

Replace the entire contents of `tests/e2e_listing.rs` with:

```rust
#![cfg(e2e_test)]

mod common;

use common::*;
use s3ls_rs::create_pipeline_cancellation_token;

/// Binary-path smoke test.
///
/// Uploads three objects in reverse alphabetical order and runs
/// `s3ls --recursive`, asserting via `assert_key_order` that s3ls's default
/// key-sort produces alphabetical output. This double-purposes as framework
/// plumbing verification (TestHelper, bucket lifecycle, run_s3ls, S3lsOutput,
/// assert_key_order, e2e_timeout!, BucketGuard) and as a regression check
/// against s3ls's sort stability.
#[tokio::test]
async fn e2e_binary_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload in REVERSE alphabetical order — default key sort must still
        // produce a, b, c.
        helper.put_object(&bucket, "c.txt", b"ccc".to_vec()).await;
        helper.put_object(&bucket, "b.txt", b"bb".to_vec()).await;
        helper.put_object(&bucket, "a.txt", b"a".to_vec()).await;

        let target = format!("s3://{bucket}/");
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive"]);

        assert!(
            output.status.success(),
            "s3ls exited non-zero: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            output.stdout,
            output.stderr
        );

        assert_key_order(&output.stdout, &["a.txt", "b.txt", "c.txt"]);
    });

    _guard.cleanup().await;
}

/// Programmatic-path smoke test.
///
/// Builds a `Config` via `TestHelper::build_config`, constructs a
/// `ListingPipeline`, and runs it. Asserts only that the pipeline returned
/// `Ok(())` — rendered output is the binary path's concern. This catches
/// API-drift bugs at the `s3ls_rs` public-API surface (`Config`,
/// `ListingPipeline::new`, `ListingPipeline::run`, cancellation token).
#[tokio::test]
async fn e2e_programmatic_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object(&bucket, "file.txt", b"hello".to_vec())
            .await;

        let target = format!("s3://{bucket}/");
        let config = TestHelper::build_config(vec![target.as_str(), "--recursive"]);
        let token = create_pipeline_cancellation_token();
        let pipeline = s3ls_rs::ListingPipeline::new(config, token);

        pipeline.run().await.expect("pipeline run failed");
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify gated build compiles**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: Successful build.

- [ ] **Step 3: Verify gated clippy is clean**

Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: No warnings.

Important: the smoke tests themselves are NOT run at this step. Running them requires a configured `s3ls-e2e-test` AWS profile and hits real S3. That verification happens manually in Task 7.

- [ ] **Step 4: Verify non-gated build stays clean**

Run: `cargo test`
Expected: All existing tests pass. e2e_listing is not compiled under this build.

- [ ] **Step 5: Commit**

```bash
cargo fmt
cargo clippy --all-features -- -D warnings
git add tests/e2e_listing.rs
git commit -m "$(cat <<'EOF'
test(step6): add binary and programmatic smoke tests for e2e framework

- e2e_binary_smoke: uploads 3 objects in reverse alphabetical order,
  runs s3ls --recursive, asserts default key-sort via assert_key_order.
  Exercises TestHelper::new, bucket_guard, create_bucket, put_object,
  run_s3ls, S3lsOutput, assert_key_order, e2e_timeout!, cleanup.
- e2e_programmatic_smoke: builds Config via TestHelper::build_config,
  constructs ListingPipeline, runs it, asserts Ok(()). Exercises the
  s3ls_rs public API surface.

Both tests are gated with #![cfg(e2e_test)] and require the
s3ls-e2e-test AWS profile for manual invocation.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Write `tests/README.md`

**Files:**
- Create: `tests/README.md`

Documents prerequisites, how to run e2e tests, cost caveats, and manual cleanup for leaked buckets.

- [ ] **Step 1: Create the README**

Create `tests/README.md` with the following content:

````markdown
# E2E Tests

End-to-end tests for s3ls-rs. Gated behind `--cfg e2e_test` so they only run
when explicitly requested and never interfere with `cargo test`.

## Prerequisites

### 1. AWS profile

```bash
aws configure --profile s3ls-e2e-test
```

The framework loads credentials from the `s3ls-e2e-test` profile and applies
`--target-profile s3ls-e2e-test` to every s3ls invocation (both binary and
programmatic paths). The region from the profile is used to create test
buckets.

### 2. IAM permissions

The profile's principal needs the following S3 permissions:

- `s3:CreateBucket`
- `s3:DeleteBucket`
- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`
- `s3:DeleteObject`
- `s3:ListBucketVersions`
- `s3:PutBucketVersioning`
- `s3:PutBucketPolicy` *(forward-compatibility for future error-path tests)*
- `s3:DeleteBucketPolicy` *(forward-compatibility)*

No bucket pre-creation is required — the framework creates a fresh bucket of
the form `s3ls-e2e-{uuid}` per test and cleans it up at the end.

## Running

Run all e2e tests:

```bash
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

Run one file:

```bash
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing -- --nocapture
```

Run one test:

```bash
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing e2e_binary_smoke -- --nocapture
```

`--nocapture` is recommended so pipeline output and debug prints surface
immediately on failure.

## Costs and caveats

- Tests hit real AWS S3 and create real buckets. Expect small charges
  (bucket ops, short-lived objects).
- AWS eventual consistency can cause occasional flakes. Retry once; if it
  fails again, investigate.
- Tests run against whatever region is configured in the `s3ls-e2e-test`
  profile — pick a region you control and can be billed from.

## Cleaning leaked buckets

Each test uses an explicit `BucketGuard::cleanup().await` instead of a `Drop`
impl. This is intentional: a `Drop` impl that calls `block_on` during test
panic unwinding can deadlock or double-panic, losing the original failure
message. The trade is that if a test panics before reaching `cleanup()`, its
bucket is leaked.

To clean leaked buckets:

```bash
# List any leaked e2e buckets
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text

# For each leaked bucket:
aws s3 rb s3://s3ls-e2e-<uuid> --force --profile s3ls-e2e-test
```

## CI

E2E tests are **not** run in CI. The existing GitHub Actions workflows run
`cargo test` without the `--cfg e2e_test` flag, so these tests stay invisible
to CI. Wiring them in is tracked separately and requires decisions about
secrets, cost budget, flake retries, and which events trigger the suite.
````

- [ ] **Step 2: Verify the file renders**

Run: `cat tests/README.md | head -20`
Expected: Markdown content displayed correctly with no stray characters.

- [ ] **Step 3: Commit**

```bash
git add tests/README.md
git commit -m "$(cat <<'EOF'
docs(step6): add tests/README.md for e2e test suite

Documents prerequisites (AWS profile, IAM permissions), how to run
(RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*'), cost caveats,
manual cleanup of leaked buckets (with rationale for explicit cleanup
over Drop), and the CI non-integration note.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Final verification

**Files:** (none modified)

Sanity-checks the full verification matrix and documents the manual S3 run.

- [ ] **Step 1: Non-gated build must stay clean**

Run: `cargo test 2>&1 | tail -20`
Expected: All existing unit tests pass. No e2e tests compiled.

- [ ] **Step 2: Non-gated clippy**

Run: `cargo clippy --all-features -- -D warnings 2>&1 | tail -5`
Expected: No warnings.

- [ ] **Step 3: Gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests 2>&1 | tail -10`
Expected: Successful build. Both `tests/common/mod.rs` and `tests/e2e_listing.rs` compile.

- [ ] **Step 4: Gated clippy**

Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -10`
Expected: No warnings.

- [ ] **Step 5: Formatting**

Run: `cargo fmt --check`
Expected: No diff.

- [ ] **Step 6: Manual S3 verification — DO THIS ONLY IF YOU HAVE AN `s3ls-e2e-test` PROFILE**

This step actually runs the smoke tests against real AWS S3. Skip it if you don't have credentials configured; the automated build/lint checks in steps 1–5 cover compilation and style.

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing -- --nocapture`
Expected: Both `e2e_binary_smoke` and `e2e_programmatic_smoke` pass. Test runtime is typically 3–10 seconds per test depending on S3 latency.

After a successful run, verify cleanup worked:

Run: `aws s3api list-buckets --profile s3ls-e2e-test --query 'Buckets[?starts_with(Name, \`s3ls-e2e-\`)].Name' --output text`
Expected: Empty output (no leaked buckets).

If buckets leak because a test panicked, clean them per the instructions in `tests/README.md`.

- [ ] **Step 7: Confirm step 6-framework is complete**

At this point:
- `tests/common/mod.rs` exists with the full `TestHelper` framework (~450 lines).
- `tests/e2e_listing.rs` has two smoke tests.
- `tests/README.md` documents running and cleanup.
- `Cargo.toml` has `uuid` as a dev-dependency.
- Non-gated and gated builds are both clean (clippy + fmt + build).
- Manual S3 verification (step 6) is optional but recommended.

No commit in this step — the working tree should already be clean from Task 6.

Run: `git status`
Expected: "nothing to commit, working tree clean".

---

## What this plan does NOT do

Listed here so a future reader doesn't mistake scope:

- Does **not** write e2e tests for filters, output formats, sort, or versioning. Those are separate brainstorms per feature area.
- Does **not** wire e2e tests into CI. That's a separate decision about secrets and cost budget.
- Does **not** add Express One Zone / directory-bucket helpers to `TestHelper`. Add them when the first Express-One-Zone test appears.
- Does **not** refactor `ListingPipeline` to accept an injected writer for in-process output capture. Rendered output is asserted via the binary path only.
- Does **not** modify the existing `docs/superpowers/plans/2026-04-04-step6-e2e-tests.md` plan document. The framework section of that plan is superseded by this plan but the per-feature test tasks remain for future re-planning.
