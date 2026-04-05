# Step 6: E2E Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create end-to-end tests that hit real S3 (or MinIO) to verify the complete s3ls tool works correctly, gated by the `E2E_TEST` environment variable.

**Architecture:** E2E tests use `cargo test` with `#[cfg(e2e_test)]` gating. Each test creates a unique S3 prefix, uploads test data, runs the s3ls binary, verifies output, and cleans up. Tests are organized by feature area.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (for test setup/teardown), assert_cmd or Command

**Depends on:** Steps 1-5 (complete s3ls tool)

**Note:** This step is human-directed. The human instructs Claude Code to generate E2E tests one by one. This plan provides the structure and test specifications.

---

### Task 1: E2E test infrastructure

**Files:**
- Create: `tests/common/mod.rs`
- Create: `tests/e2e_listing.rs`

- [ ] **Step 1: Write test helper module**

```rust
// tests/common/mod.rs

use aws_sdk_s3::Client;
use std::process::Command;

pub const E2E_BUCKET: &str = "s3ls-e2e-test"; // Set via E2E_TEST_BUCKET env var

pub async fn get_test_client() -> Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    Client::new(&config)
}

pub fn get_bucket() -> String {
    std::env::var("E2E_TEST_BUCKET").unwrap_or_else(|_| E2E_BUCKET.to_string())
}

pub async fn upload_test_object(client: &Client, bucket: &str, key: &str, body: &[u8]) {
    client.put_object()
        .bucket(bucket)
        .key(key)
        .body(body.to_vec().into())
        .send()
        .await
        .expect("Failed to upload test object");
}

pub async fn delete_test_objects(client: &Client, bucket: &str, prefix: &str) {
    let resp = client.list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .expect("Failed to list test objects");

    for obj in resp.contents() {
        if let Some(key) = obj.key() {
            client.delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .ok();
        }
    }
}

pub fn run_s3ls(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_s3ls"))
        .args(args)
        .output()
        .expect("Failed to execute s3ls")
}

pub fn unique_prefix() -> String {
    format!("e2e-test/{}/", uuid::Uuid::new_v4())
}
```

- [ ] **Step 2: Commit**

```bash
git add tests/
git commit -m "feat(step6): add E2E test infrastructure"
```

---

### Task 2: Basic listing E2E tests

**Files:**
- Modify: `tests/e2e_listing.rs`

- [ ] **Step 1: Write basic listing tests**

Pipeline is created with a cancellation token:

```rust
#![cfg(e2e_test)]

mod common;

use common::*;

#[tokio::test]
async fn e2e_non_recursive_listing() {
    let client = get_test_client().await;
    let bucket = get_bucket();
    let prefix = unique_prefix();

    // Setup: upload objects at different depths
    upload_test_object(&client, &bucket, &format!("{prefix}file1.txt"), b"hello").await;
    upload_test_object(&client, &bucket, &format!("{prefix}file2.txt"), b"world").await;
    upload_test_object(&client, &bucket, &format!("{prefix}subdir/file3.txt"), b"deep").await;

    // Run: non-recursive listing (default)
    let output = run_s3ls(&[&format!("s3://{bucket}/{prefix}")]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify: should show file1, file2, and PRE subdir/
    assert!(stdout.contains("file1.txt"));
    assert!(stdout.contains("file2.txt"));
    assert!(stdout.contains("PRE"));
    assert!(stdout.contains("subdir/"));
    assert!(!stdout.contains("file3.txt")); // nested, not shown in non-recursive

    // Cleanup
    delete_test_objects(&client, &bucket, &prefix).await;
}

#[tokio::test]
async fn e2e_recursive_listing() {
    let client = get_test_client().await;
    let bucket = get_bucket();
    let prefix = unique_prefix();

    upload_test_object(&client, &bucket, &format!("{prefix}a.txt"), b"a").await;
    upload_test_object(&client, &bucket, &format!("{prefix}dir/b.txt"), b"b").await;

    let output = run_s3ls(&[&format!("s3://{bucket}/{prefix}"), "--recursive"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("a.txt"));
    assert!(stdout.contains("dir/b.txt"));
    assert!(!stdout.contains("PRE")); // no PRE in recursive mode

    delete_test_objects(&client, &bucket, &prefix).await;
}
```

For programmatic tests (not using the binary), Config and pipeline are used like:

```rust
use s3ls_rs::{Config, ListingPipeline, create_pipeline_cancellation_token};

let config = Config::for_target("my-bucket", "prefix/");
let token = create_pipeline_cancellation_token();
let pipeline = ListingPipeline::new(config, token);
pipeline.run().await.unwrap();
```

Or from args:

```rust
use s3ls_rs::build_config_from_args;

let config = build_config_from_args(vec!["s3ls", "s3://bucket/prefix/"]).unwrap();
let token = create_pipeline_cancellation_token();
let pipeline = ListingPipeline::new(config, token);
```

- [ ] **Step 2: Run E2E tests (requires real S3)**

Run: `E2E_TEST=1 cargo test --test e2e_listing -- --nocapture 2>&1`
Expected: Tests pass if S3 credentials and bucket are configured

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test(step6): add basic listing E2E tests"
```

---

### Task 3: Filter E2E tests

**Files:**
- Create: `tests/e2e_filters.rs`

Tests to add:
- `e2e_include_regex_filter` - upload mixed files, verify regex include works
- `e2e_exclude_regex_filter` - verify regex exclude works
- `e2e_size_filter` - upload files of different sizes, verify size filtering
- `e2e_storage_class_filter` - upload with different storage classes, verify filtering

---

### Task 4: Sort and output format E2E tests

**Files:**
- Create: `tests/e2e_output.rs`

Tests to add:
- `e2e_sort_by_size` - verify output is sorted by size
- `e2e_sort_by_date_reverse` - verify reverse date sort
- `e2e_sort_by_key_default` - verify default sort is by key (config.sort = SortField::Key)
- `e2e_human_readable_sizes` - verify KiB/MiB formatting (config.display_config.human)
- `e2e_ndjson_output` - verify valid NDJSON output (config.display_config.json), parse each line
- `e2e_summary_line` - verify summary shows correct totals (config.display_config.summary)
- `e2e_show_extra_columns` - verify ETag and storage class columns appear (config.display_config.show_etag, config.display_config.show_storage_class)

---

### Task 5: All-versions E2E tests

**Files:**
- Create: `tests/e2e_versions.rs`

Tests to add (requires versioned bucket):
- `e2e_all_versions_listing` - upload, overwrite, verify multiple versions shown; verify default sort by key with secondary sort by mtime
- `e2e_delete_markers` - delete an object, verify delete marker appears
- `e2e_all_versions_with_json` - verify NDJSON includes version_id and is_latest (config.display_config.json + config.all_versions)

---

### Task 6: Final E2E verification

- [ ] **Step 1: Run full E2E suite**

Run: `E2E_TEST=1 cargo test --test 'e2e_*' -- --nocapture 2>&1`
Expected: All E2E tests pass

- [ ] **Step 2: Run coverage report**

Run: `cargo llvm-cov --all-targets 2>&1 | tail -5`
Expected: ~99% coverage target

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test(step6): complete E2E test suite, step 6 complete"
```
