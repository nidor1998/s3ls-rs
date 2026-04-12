# Step 6: E2E Test Framework — Design

**Date:** 2026-04-11
**Status:** Implemented. See `tests/common/mod.rs` and `tests/e2e_listing.rs` for final state.
**Supersedes:** The framework sketch in `docs/superpowers/plans/2026-04-04-step6-e2e-tests.md` (Task 1). The per-feature test tasks in that plan (filters, output, versions) are out of scope here and will be re-planned per feature area after this framework lands.

---

## Goal

Build the end-to-end test framework for s3ls-rs, modeled on s3rm-rs's `tests/common/mod.rs`, plus one smoke test file (`tests/e2e_listing.rs`) with two tests that exercise every framework seam. After this step:

- `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing` runs against real S3 and passes (with an `s3ls-e2e-test` AWS profile configured).
- `cargo test` (without the cfg flag) still passes with no e2e tests compiled in.
- `cargo clippy --all-features` and `cargo fmt --check` stay clean.
- All subsequent e2e test work can be done by writing new `tests/e2e_*.rs` files that `use common::*;` — no further framework changes required for the common cases.

Actual feature-area test coverage (listing variants, filters, sort, output formats, versioning, error paths) is explicitly out of scope for this step and will be brainstormed per feature area.

## Non-goals

- Writing the full e2e test matrix listed in the 2026-04-04 plan (filters, output, versions). Only two smoke tests land here.
- CI integration. The existing GitHub Actions workflows run `cargo test` without the cfg flag, so e2e tests stay invisible to CI. Wiring them in requires separate decisions about secrets, cost budget, flake retries, and event triggers.
- Express One Zone / directory-bucket support. s3ls-rs has no Express-One-Zone-specific code path to test; adding the helper now would be dead code.
- Production code changes. The framework uses the existing public API of `s3ls_rs` (`Config`, `ListingPipeline`, `build_config_from_args`, `create_pipeline_cancellation_token`) as-is. No refactoring of pipeline output plumbing for testability.
- Refactoring the existing 2026-04-04 plan document. The framework section of that plan is superseded; the per-feature tasks are left in place to be re-planned later.

---

## Architecture

### File layout

```
tests/
  common/
    mod.rs           # TestHelper, BucketGuard, S3lsOutput, e2e_timeout!, E2E_TIMEOUT, assert_key_order
  e2e_listing.rs     # Two smoke tests (binary path + programmatic path)
  README.md          # How to run, prerequisites, manual cleanup, CI note
```

`tests/common/` is a **directory module**, not a plain `tests/common.rs`. Rust's integration-test harness compiles every file directly under `tests/` as its own binary crate; a `tests/common.rs` would become a binary with zero tests and emit a warning. Cargo skips subdirectories, so `tests/common/mod.rs` becomes a shared module that `e2e_*.rs` files import via `mod common;`. This is the idiomatic pattern and what s3rm-rs uses.

### Cfg gating

- `Cargo.toml` already contains `[lints.rust] unexpected_cfgs = { level = "warn", check-cfg = ['cfg(e2e_test)'] }`. No Cargo changes are required for this step.
- Every e2e test file starts with `#![cfg(e2e_test)]`. Under normal `cargo test` these compile to empty binaries (no warnings). Under `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*'` they compile normally.
- `tests/common/mod.rs` is **not** itself gated. It is imported via `mod common;` from files that are already gated, so it only compiles when a gated file compiles. Adding `#![cfg(e2e_test)]` at the top of `mod.rs` would cause "unused import" warnings in the framework under non-gated builds.

### Dependency additions

Add to `[dev-dependencies]`:

```toml
uuid = { version = "1", features = ["v4"] }
```

`aws-config`, `aws-sdk-s3`, `aws-smithy-types`, `tokio`, and `anyhow` are already production dependencies and are reused by the framework without being re-declared in dev-deps.

---

## `TestHelper` API

Single struct in `tests/common/mod.rs`, constructed as `Arc<TestHelper>`. All fields are private; access is via methods.

### Construction and identity

```rust
pub struct TestHelper {
    client: aws_sdk_s3::Client,
    region: String,
}

impl TestHelper {
    /// Load the `s3ls-e2e-test` AWS profile and return a shared helper.
    pub async fn new() -> Arc<Self>;

    /// The AWS region resolved from the profile (used for bucket creation).
    pub fn region(&self) -> &str;

    /// Escape hatch for tests that need an S3 operation the helper does not expose.
    pub fn client(&self) -> &aws_sdk_s3::Client;

    /// Return a unique bucket name of the form `s3ls-e2e-{uuid-v4}`.
    pub fn generate_bucket_name(&self) -> String;
}
```

Bucket names: `s3ls-e2e-` (9 chars) + UUID v4 (36 chars) = 45 chars, comfortably under the 63-char S3 limit; all lowercase; no underscores.

### Bucket lifecycle

```rust
impl TestHelper {
    /// Create a standard (non-versioned) bucket in the helper's region.
    /// us-east-1 must not specify a LocationConstraint; other regions must.
    pub async fn create_bucket(&self, bucket: &str);

    /// Create a bucket and enable versioning on it.
    pub async fn create_versioned_bucket(&self, bucket: &str);

    /// Return a cleanup guard for the given bucket (see BucketGuard below).
    pub fn bucket_guard(self: &Arc<Self>, bucket: &str) -> BucketGuard;

    /// Delete everything in the bucket (versions → delete markers → objects)
    /// and then the bucket itself. Errors are swallowed — cleanup is best-effort.
    pub async fn delete_bucket_cascade(&self, bucket: &str);
}
```

### `BucketGuard` pattern

```rust
pub struct BucketGuard {
    helper: Arc<TestHelper>,
    bucket: String,
}

impl BucketGuard {
    /// Delete all objects and the bucket. Call at the end of every test.
    pub async fn cleanup(self) {
        self.helper.delete_bucket_cascade(&self.bucket).await;
    }
}
```

**Explicit `.cleanup().await` — no `Drop` impl.** A `Drop` impl that calls `block_on(delete_bucket_cascade(...))` deadlocks or double-panics during test-failure unwinding: the Tokio runtime is already tearing down, and `block_on` inside `Drop` during runtime shutdown is the canonical Rust footgun. s3rm-rs discovered this and moved to explicit cleanup; this framework inherits the decision.

**Consequence:** if a test panics before reaching `.cleanup().await`, the bucket is intentionally leaked. This is the correct trade — a leaked bucket is a noisy manual-cleanup problem, but a double-panic abort during teardown loses the original failure message entirely, which is much worse for debugging. `tests/README.md` documents how to clean leaked buckets.

**Canonical test structure:**

```rust
#[tokio::test]
async fn e2e_some_test() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file1.txt", b"hello".to_vec()).await;

        let output = TestHelper::run_s3ls(&[&format!("s3://{bucket}/")]);
        assert!(output.status.success());
        assert!(output.stdout.contains("file1.txt"));
    });

    _guard.cleanup().await;
}
```

The underscore prefix on `_guard` signals "held for lifetime, not read" but the binding is still used on the `cleanup()` line. Matches the s3rm-rs idiom.

### `delete_bucket_cascade` semantics

1. **`delete_all_versions(bucket)`** — paginated `list_object_versions`, collect versions + delete markers, batch-delete in chunks of 1000 with `quiet(true)`. Handles versioned buckets.
2. **`delete_all_objects(bucket)`** — paginated `list_objects_v2`, batch-delete in chunks of 1000. Handles any non-versioned objects missed by step 1 (e.g., objects uploaded to a bucket before versioning was enabled).
3. **`client.delete_bucket(bucket)`** — best effort; errors ignored.

All three steps swallow errors (via `let _ = ...` / `unwrap_or_else(return)`). Cleanup must never panic during teardown: a panic here would mask the real test failure, and a leaked bucket surfaces via the manual-cleanup workflow anyway.

### Object operations

Ported verbatim from s3rm-rs (the production code already handles content-type, metadata, and tag filters, so future s3ls tests will need these helpers):

```rust
impl TestHelper {
    pub async fn put_object(&self, bucket: &str, key: &str, body: Vec<u8>);
    pub async fn put_object_with_content_type(&self, bucket: &str, key: &str, body: Vec<u8>, content_type: &str);
    pub async fn put_object_with_metadata(&self, bucket: &str, key: &str, body: Vec<u8>, metadata: HashMap<String, String>);
    pub async fn put_object_with_tags(&self, bucket: &str, key: &str, body: Vec<u8>, tags: HashMap<String, String>);
    pub async fn put_object_full(&self, bucket: &str, key: &str, body: Vec<u8>, content_type: &str, metadata: HashMap<String, String>, tags: HashMap<String, String>);

    /// Upload up to 16 objects in parallel via a `JoinSet` + semaphore.
    pub async fn put_objects_parallel(&self, bucket: &str, objects: Vec<(String, Vec<u8>)>);

    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> Vec<String>;
    pub async fn list_object_versions(&self, bucket: &str) -> Vec<(String, String)>;
    pub async fn count_objects(&self, bucket: &str, prefix: &str) -> usize;
}
```

None of these are exercised by the smoke tests beyond `put_object`, but they land together because dropping and re-adding them later is churn. `put_objects_parallel` uses `tokio::sync::Semaphore::new(16)` and `tokio::task::JoinSet`.

### Binary runner

```rust
pub struct S3lsOutput {
    pub stdout: String,          // UTF-8, lossy
    pub stderr: String,          // UTF-8, lossy
    pub status: std::process::ExitStatus,
}

impl TestHelper {
    /// Run the s3ls binary with the given args. Auto-appends
    /// `--target-profile s3ls-e2e-test` unless the args already contain
    /// `--target-profile` or `--target-access-key`.
    pub fn run_s3ls(args: &[&str]) -> S3lsOutput;
}
```

Implementation notes:

- Locates the binary via `env!("CARGO_BIN_EXE_s3ls")` — Cargo guarantees this is set for integration tests of a crate with a `[[bin]]` target named `s3ls`.
- Uses blocking `std::process::Command::output()`. Synchronous I/O inside a `#[tokio::test]` is fine: the framework does no other work while waiting for one subprocess, and a blocking spawn is simpler than `tokio::process::Command`.
- **Associated function, not method** (`TestHelper::run_s3ls(&[...])`, not `helper.run_s3ls(...)`). It needs no state and test bodies are cleaner without a receiver.
- UTF-8 decoding is done once inside `run_s3ls` via `String::from_utf8_lossy(...).into_owned()`. Test bodies never write `String::from_utf8_lossy(&output.stdout)`.

### Programmatic runner

```rust
impl TestHelper {
    /// Build a `Config` from CLI-style args. Auto-injects
    /// `--target-profile s3ls-e2e-test` unless the args already specify
    /// `--target-profile` or `--target-access-key`. Panics on build failure.
    pub fn build_config(args: Vec<&str>) -> Config;

    /// Construct a `ListingPipeline` from the config and run it to completion.
    /// Returns the pipeline's `anyhow::Result<()>` (the public signature of
    /// `ListingPipeline::run`). Intended for tests that assert on pipeline
    /// behavior (error paths, cancellation, credential loading) rather than
    /// rendered output — rendered output is asserted via the binary path.
    pub async fn run_pipeline(config: Config) -> anyhow::Result<()>;
}
```

`run_pipeline` creates a cancellation token via `create_pipeline_cancellation_token()`, constructs `ListingPipeline::new(config, token)`, calls `.run().await`, and propagates the result. It does **not** capture rendered stdout — that would require refactoring `ListingPipeline`'s output plumbing to accept an injected writer, which is an explicit non-goal. Tests that need output assertions use the binary path.

### Auto-profile injection

Both `run_s3ls` and `build_config` append `--target-profile s3ls-e2e-test` to the arg list unless the args already contain a flag starting with `--target-profile` or `--target-access-key`. This escape hatch lets tests override credentials when needed (e.g., a test that asserts on invalid-credential error paths).

The binary path matters: `Command::new` inherits the current environment, so a developer with `AWS_PROFILE=production` set could otherwise run e2e tests against production. Auto-injecting on the CLI side shadows any inherited env var (s3ls CLI args take precedence over env vars), which is the safer default.

### Timeout infrastructure

```rust
pub const E2E_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

#[macro_export]
macro_rules! e2e_timeout {
    ($body:expr) => {
        tokio::time::timeout(common::E2E_TIMEOUT, $body)
            .await
            .expect("E2E test timed out")
    };
}
```

Every test body is wrapped in `e2e_timeout!(async { ... });`. 60 seconds is generous for single listing operations but a known-good upper bound for eventual stress tests (large-result listings, versioned buckets with many delete markers). The constant is tunable in one place.

### Sort-order helper

```rust
/// Assert that the given keys appear in `stdout` in the specified order.
/// Panics with a descriptive message (including the full stdout) if any key
/// is missing or out of order. Uses byte-offset comparison via `str::find`;
/// works for any text output where each expected key appears at most once.
pub fn assert_key_order(stdout: &str, expected_order: &[&str]);
```

Free function, not a method — no state needed. Typical call:

```rust
common::assert_key_order(&output.stdout, &["a.txt", "b.txt", "c.txt"]);
```

Internally: finds each key's byte position, panics with `"key {key:?} not found in stdout:\n{stdout}"` if absent, then walks adjacent pairs and panics with `"expected {a:?} before {b:?}; got positions {pa} vs {pb} in stdout:\n{stdout}"` if out of order. The full stdout is included in every panic message so failures are self-diagnosing.

This is the one helper extracted up-front despite the "grow helpers when duplication appears" rule, because it is explicitly requested and small (~15 lines).

---

## Smoke tests — `tests/e2e_listing.rs`

Two tests, both gated with `#![cfg(e2e_test)]` at the file top. Both tests live in the same file so Cargo builds one integration-test binary, not two (each file under `tests/` compiles as a separate binary and embeds its own copy of the `common` module).

### Test 1 — binary path, with sort-order assertion

Uploads three objects in reverse alphabetical order (`c.txt`, `b.txt`, `a.txt`), runs `s3ls --recursive`, and asserts via `assert_key_order` that the output is in alphabetical order. Double-purposes as framework plumbing verification and a regression check against s3ls's default key-sort stability.

```rust
#![cfg(e2e_test)]

mod common;
use common::*;

#[tokio::test]
async fn e2e_binary_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        helper.put_object(&bucket, "c.txt", b"ccc".to_vec()).await;
        helper.put_object(&bucket, "b.txt", b"bb".to_vec()).await;
        helper.put_object(&bucket, "a.txt", b"a".to_vec()).await;

        let output = TestHelper::run_s3ls(&[
            &format!("s3://{bucket}/"),
            "--recursive",
        ]);

        assert!(
            output.status.success(),
            "s3ls exited non-zero: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status, output.stdout, output.stderr
        );

        assert_key_order(&output.stdout, &["a.txt", "b.txt", "c.txt"]);
    });

    _guard.cleanup().await;
}
```

**Seams exercised:** `TestHelper::new`, `generate_bucket_name`, `bucket_guard`, `create_bucket`, `put_object`, `run_s3ls`, `S3lsOutput`, `assert_key_order`, `e2e_timeout!`, `BucketGuard::cleanup`.

### Test 2 — programmatic path

Builds a `Config`, constructs `ListingPipeline`, runs it, asserts `Ok(())`. Does not assert on rendered output.

```rust
use s3ls_rs::create_pipeline_cancellation_token;

#[tokio::test]
async fn e2e_programmatic_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", b"hello".to_vec()).await;

        let target = format!("s3://{bucket}/");
        let config = TestHelper::build_config(vec![&target, "--recursive"]);
        let token = create_pipeline_cancellation_token();
        let pipeline = s3ls_rs::ListingPipeline::new(config, token);

        pipeline.run().await.expect("pipeline run failed");
    });

    _guard.cleanup().await;
}
```

**Seams exercised beyond Test 1:** `TestHelper::build_config` (auto-profile injection for the programmatic path), `ListingPipeline::new`, `ListingPipeline::run`, `create_pipeline_cancellation_token`. This catches the "does the framework's API assumptions match the current public API" class of bug, which is the most likely first-integration failure.

---

## `tests/README.md`

Outline and required content:

1. **Prerequisites** — `aws configure --profile s3ls-e2e-test`; IAM permissions list: `s3:CreateBucket`, `s3:DeleteBucket`, `s3:PutObject`, `s3:GetObject`, `s3:ListBucket`, `s3:DeleteObject`, `s3:ListBucketVersions`, `s3:PutBucketVersioning`, `s3:PutBucketPolicy`, `s3:DeleteBucketPolicy`. (The policy permissions are included for forward-compatibility with future tests that assert on access-denied error paths, even though the smoke test doesn't use them.)
2. **Running tests** — the three `RUSTFLAGS='--cfg e2e_test' cargo test ...` invocations (all files, one file, one test).
3. **Costs & caveats** — tests hit real AWS S3, create real buckets, incur real (small) charges; AWS eventual consistency may cause occasional flakes that resolve on retry.
4. **Cleaning leaked buckets** — `aws s3api list-buckets --profile s3ls-e2e-test --query 'Buckets[?starts_with(Name, \`s3ls-e2e-\`)].Name' --output text` followed by `aws s3 rb s3://s3ls-e2e-<uuid> --force --profile s3ls-e2e-test`. Explains the panic-before-cleanup rationale so future-readers don't think leaks are a bug.
5. **CI note** — "E2E tests are not run in CI yet; manual invocation only. CI integration is tracked separately and requires decisions about secrets, cost budget, and flake retries."

---

## Verification criteria

Step 6-framework is complete when all of these hold:

1. `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_listing -- --nocapture` runs against a configured `s3ls-e2e-test` profile and both smoke tests pass.
2. `cargo test` (no cfg flag) passes with no e2e tests compiled in and no warnings.
3. `cargo clippy --all-features` passes with no warnings.
4. `cargo fmt --check` passes.
5. `cargo build` builds clean.
6. `tests/README.md` exists and covers the five outline items above.
7. After a test run, `aws s3api list-buckets --profile s3ls-e2e-test` shows no `s3ls-e2e-*` buckets (assuming no tests panicked).

Per auto-memory: run `cargo fmt` and `cargo clippy --all-features` before any commit.
