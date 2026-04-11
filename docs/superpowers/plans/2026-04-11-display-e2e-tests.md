# Display E2E Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 17 end-to-end tests for s3ls display functionality (every `--show-*` flag, `--header`, `--summarize`, `--human-readable`, `--show-relative-path`, plus 2 bucket listing display tests) in a new `tests/e2e_display.rs` file, using 6 new helpers in `tests/common/mod.rs`.

**Architecture:** Gated with `#![cfg(e2e_test)]`. One new file holds all 17 tests. Per-flag tests do 3–4 `run_s3ls` invocations against a single bucket (text-on, text-off, JSON optionally with/without flag). Row-type tests exercise `CommonPrefix` (PRE) and `DeleteMarker` (DELETE) row padding with multiple `--show-*` flags enabled at once. Summarize tests assert the text and JSON summary line shapes. Bucket listing tests scope assertions to a known test-bucket name.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (production dep), `serde_json` (production dep), existing framework helpers from step 6 + step 7 + versioning plan.

**Depends on:** Step 6 framework, step 7 filter suite, versioning suite. The new file is a sibling of `tests/e2e_filters.rs`, `tests/e2e_filters_versioned.rs`, and `tests/e2e_listing.rs`.

**Spec:** `docs/superpowers/specs/2026-04-11-display-e2e-tests-design.md`

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- Auto-memory feedback: **logging via `tracing` / `tracing-subscriber`**, not `log` directly. (Not relevant — no logging code — but noted.)

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/e2e_display.rs` | 17 `#[tokio::test]` functions covering display functionality. Gated with `#![cfg(e2e_test)]`. |

**Modified files:**

| Path | Change |
|---|---|
| `tests/common/mod.rs` | Add 6 helpers: `TestHelper::put_object_with_checksum_algorithm`, `parse_tsv_line`, `assert_header_columns`, `assert_all_data_rows_have_columns`, `assert_summary_present_text`, `assert_summary_present_json`. Add `ChecksumAlgorithm` to the `aws_sdk_s3::types` import. |

**No changes to `src/`.** No `Cargo.toml` changes — `serde_json`, `chrono`, `aws-sdk-s3`, `tokio` are existing production dependencies.

---

## Important notes for the executor

**Framework reuse.** Every test uses the established pattern:

```rust
let helper = TestHelper::new().await;
let bucket = helper.generate_bucket_name();
let _guard = helper.bucket_guard(&bucket);

e2e_timeout!(async {
    helper.create_bucket(&bucket).await;  // or create_versioned_bucket
    // ... fixture + run_s3ls + assertions ...
});

_guard.cleanup().await;  // OUTSIDE e2e_timeout!
```

**Column order reference** (from `src/aggregate.rs:444-475` — `format_header`):

```
DATE, SIZE,
STORAGE_CLASS?, ETAG?, CHECKSUM_ALGORITHM?, CHECKSUM_TYPE?,
VERSION_ID (if --all-versions)?,
IS_LATEST (if --show-is-latest)?,
OWNER_DISPLAY_NAME?, OWNER_ID? (if --show-owner — 2 cols),
IS_RESTORE_IN_PROGRESS?, RESTORE_EXPIRY_DATE? (if --show-restore-status — 2 cols),
KEY
```

**JSON field-dependency map:**
- `ETag`, `Size`, `Key`, `LastModified`: always present for regular objects.
- `StorageClass`: present only when S3 returned a non-None class (may be absent for default STANDARD objects — do NOT assert presence/absence for this field in display tests, it's noise).
- `ChecksumAlgorithm`: present when non-empty (requires upload with explicit `ChecksumAlgorithm`).
- `ChecksumType`: present when the object has a checksum.
- `VersionId` / `IsLatest`: present under `--all-versions`.
- `Owner`: present only when `fetch_owner=true` (i.e. `--show-owner` set under non-versioned listing; ALWAYS present under `--all-versions`).
- `RestoreStatus`: present only when the object has a populated `is_restore_in_progress` field (Glacier-restored objects); for regular objects, absent even with `--show-restore-status`.

**Do NOT use `--hide-delete-markers` in display tests.** It's out of scope and belongs to the versioning suite.

**Running against real S3 is NOT part of this plan.** All verification in D-Tasks 1-11 is compile + clippy + fmt only. D-Task 12 is manual real-S3 verification.

**Verification commands used throughout:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — new file compiles to empty. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build — compiles framework + display tests. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint. |
| `cargo clippy --all-features -- -D warnings` | Non-gated lint. |
| `cargo fmt --check` | Formatting. |

---

### D-Task 1: Add 6 helpers to `tests/common/mod.rs`

Pure framework plumbing. No new test file yet.

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add `ChecksumAlgorithm` to the `aws_sdk_s3::types` import**

Find the existing import block in `tests/common/mod.rs` (which already includes `StorageClass` from Step 7 Task 1):

```rust
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Delete,
    ObjectIdentifier, StorageClass, VersioningConfiguration,
};
```

Replace it with:

```rust
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, ChecksumAlgorithm,
    CreateBucketConfiguration, Delete, ObjectIdentifier, StorageClass, VersioningConfiguration,
};
```

- [ ] **Step 2: Add `put_object_with_checksum_algorithm` method**

Find the `create_delete_marker` method inside the `impl TestHelper` "Object operations" block (added by the versioning plan V-Task 1). Immediately after `create_delete_marker` and before `put_object_full`, add:

```rust
    /// Upload an object with an explicit S3 ChecksumAlgorithm.
    ///
    /// Used by display tests that exercise `--show-checksum-algorithm` /
    /// `--show-checksum-type` — the default PUT does not populate a
    /// checksum field, so tests that want non-empty checksum columns
    /// must use this helper.
    ///
    /// Accepts the algorithm as a string ("CRC32", "CRC32C", "SHA1",
    /// "SHA256", "CRC64NVME"). Converts via `ChecksumAlgorithm::from(&str)`
    /// — same pattern as `put_object_with_storage_class`.
    pub async fn put_object_with_checksum_algorithm(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        algorithm: &str,
    ) {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .checksum_algorithm(ChecksumAlgorithm::from(algorithm))
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to put object {key} with checksum-algorithm {algorithm}: {e}")
            });
    }
```

- [ ] **Step 3: Add `parse_tsv_line` free function**

At the bottom of `tests/common/mod.rs`, after the existing `assert_json_version_shapes_eq` helper, append:

```rust
/// Split a tab-delimited line into its columns. Helper for display tests
/// that need to assert on specific column indices.
pub fn parse_tsv_line(line: &str) -> Vec<&str> {
    line.split('\t').collect()
}
```

- [ ] **Step 4: Add `assert_header_columns` free function**

Append:

```rust
/// Assert the first line of `stdout` (from `s3ls --header ...` in text
/// mode) is a tab-delimited header row with exactly the expected column
/// names in order. Panics with the label on mismatch.
///
/// Panics if:
/// - `stdout` has no non-empty lines,
/// - the first non-empty line's columns don't match `expected` exactly.
pub fn assert_header_columns(stdout: &str, expected: &[&str], label: &str) {
    let header_line = stdout
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_else(|| panic!("[{label}] stdout is empty"));
    let actual: Vec<&str> = parse_tsv_line(header_line);
    if actual != expected {
        panic!(
            "[{label}] header column mismatch\n  expected: {expected:?}\n  actual:   {actual:?}\n  header line: {header_line}"
        );
    }
}
```

- [ ] **Step 5: Add `assert_all_data_rows_have_columns` free function**

Append:

```rust
/// Assert that every non-empty line of `stdout` has exactly
/// `expected_count` tab-separated columns.
///
/// Lines identified as the summary (starting with `"Total:\t"`) are
/// EXCLUDED from the count check, since the summary has a different
/// column count than data rows.
///
/// The header row (if `--header` was used) has the same column count as
/// data rows, so it naturally passes the same check and is NOT excluded.
pub fn assert_all_data_rows_have_columns(
    stdout: &str,
    expected_count: usize,
    label: &str,
) {
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if line.starts_with("Total:\t") {
            continue;
        }
        let cols: Vec<&str> = parse_tsv_line(line);
        if cols.len() != expected_count {
            panic!(
                "[{label}] row column count mismatch\n  expected: {expected_count}\n  actual:   {}\n  line: {line}",
                cols.len()
            );
        }
    }
}
```

- [ ] **Step 6: Add `assert_summary_present_text` free function**

Append:

```rust
/// Assert that `stdout` contains a text-mode summary line starting with
/// `"Total:\t"` and return it. The caller can then do further substring
/// assertions on its contents (e.g. contains the expected object count).
pub fn assert_summary_present_text(stdout: &str, label: &str) -> String {
    stdout
        .lines()
        .find(|l| l.starts_with("Total:\t"))
        .unwrap_or_else(|| {
            panic!("[{label}] no 'Total:' summary line found in stdout:\n{stdout}")
        })
        .to_string()
}
```

- [ ] **Step 7: Add `assert_summary_present_json` free function**

Append:

```rust
/// Assert that `stdout` contains a JSON summary line (an NDJSON line that
/// parses to an object with a top-level `"Summary"` key) and return the
/// parsed `serde_json::Value`. The caller can then do further field
/// assertions on its contents (e.g. `v["Summary"]["TotalObjects"]`).
pub fn assert_summary_present_json(stdout: &str, label: &str) -> serde_json::Value {
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
            && v.get("Summary").is_some()
        {
            return v;
        }
    }
    panic!("[{label}] no JSON 'Summary' line found in stdout:\n{stdout}");
}
```

- [ ] **Step 8: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass, no warnings.

- [ ] **Step 9: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 10: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 11: Commit**

```bash
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e helpers for display assertions

Adds six helpers to the e2e framework:
- TestHelper::put_object_with_checksum_algorithm: upload with an
  explicit ChecksumAlgorithm so display tests for
  --show-checksum-algorithm / --show-checksum-type have non-empty
  cells to assert against.
- parse_tsv_line: split tab-delimited lines into columns.
- assert_header_columns: assert the first line of stdout is a
  tab-delimited header with exactly the expected column names.
- assert_all_data_rows_have_columns: assert every non-empty line
  (except the summary) has the expected column count.
- assert_summary_present_text: locate and return the 'Total:' line.
- assert_summary_present_json: locate and parse the JSON summary line.

Also adds ChecksumAlgorithm to the aws_sdk_s3::types import group.

No production code changes. No Cargo.toml changes.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 2: Scaffold `tests/e2e_display.rs` with `show_storage_class` and `show_etag` tests

Creates the new file with the preamble and the two simplest per-flag tests.

**Files:**
- Create: `tests/e2e_display.rs`

- [ ] **Step 1: Create `tests/e2e_display.rs` with preamble and Test 1**

Create the file with:

```rust
#![cfg(e2e_test)]

//! Display end-to-end tests.
//!
//! Covers s3ls's text-format and JSON-format output rendering, including
//! every `--show-*` flag, `--header`, `--summarize`, `--human-readable`,
//! `--show-relative-path`, the CommonPrefix (PRE) and DeleteMarker
//! (DELETE) row types, and bucket listing display flags.
//!
//! Per-flag tests do 3 `run_s3ls` invocations against a single bucket
//! (text with flag on, text with flag off, JSON). Flags that gate an
//! S3 API-level fetch (`--show-owner`, `--show-restore-status`) do 4
//! invocations to observe the JSON field's presence/absence.
//!
//! Design: `docs/superpowers/specs/2026-04-11-display-e2e-tests-design.md`

mod common;

use common::*;

/// `--show-storage-class` adds a STORAGE_CLASS column between SIZE and KEY.
/// JSON output's `StorageClass` field is driven by whether S3 returned a
/// non-None class, not by this flag, so the JSON sub-assertion here only
/// checks that the output parses cleanly with mandatory fields.
#[tokio::test]
async fn e2e_display_show_storage_class() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-storage-class",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "STORAGE_CLASS", "KEY"],
            "show-storage-class: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-storage-class: text on row count",
        );
        assert!(
            output.stdout.contains("file.txt"),
            "show-storage-class: key 'file.txt' missing from text output"
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-storage-class: text off header",
        );

        // Sub-assertion 3: JSON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(v.get("Key").is_some(), "show-storage-class: Key missing from JSON");
        assert!(v.get("Size").is_some(), "show-storage-class: Size missing from JSON");
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_show_etag` test**

Append to the same file:

```rust

/// `--show-etag` adds an ETAG column between SIZE and KEY. The JSON output
/// always includes the `ETag` field for regular objects regardless of the
/// flag, so the JSON sub-assertion verifies the field is present.
#[tokio::test]
async fn e2e_display_show_etag() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-etag",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "ETAG", "KEY"],
            "show-etag: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-etag: text on row count",
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-etag: text off header",
        );

        // Sub-assertion 3: JSON — ETag always present for regular objects
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(v.get("ETag").is_some(), "show-etag: ETag missing from JSON");
        assert!(v.get("Key").is_some(), "show-etag: Key missing from JSON");
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes. New file compiles to empty under non-gated builds.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for --show-storage-class and --show-etag

Two per-flag display tests. Each uses a single-object fixture and
runs s3ls three times: text with the flag on (verifies column
addition), text with the flag off (verifies no column), and JSON
(verifies mandatory fields are present). Uses the new
assert_header_columns and assert_all_data_rows_have_columns helpers.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 3: Add `show_checksum_algorithm` and `show_checksum_type` tests

Both tests share the same fixture (one object uploaded with an explicit CRC32 checksum). The `put_object_with_checksum_algorithm` helper from D-Task 1 is used for both.

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_show_checksum_algorithm` test**

Append:

```rust

/// `--show-checksum-algorithm` adds a CHECKSUM_ALGORITHM column. The test
/// uploads with an explicit CRC32 checksum so the column has a non-empty
/// value to assert. JSON output's `ChecksumAlgorithm` field is emitted
/// whenever the checksum_algorithm Vec is non-empty, so the JSON
/// sub-assertion verifies field presence and value.
#[tokio::test]
async fn e2e_display_show_checksum_algorithm() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object_with_checksum_algorithm(&bucket, "file.txt", vec![0u8; 100], "CRC32")
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-checksum-algorithm",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "CHECKSUM_ALGORITHM", "KEY"],
            "show-checksum-algorithm: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-checksum-algorithm: text on row count",
        );
        // Find the data row and check column index 2 (CHECKSUM_ALGORITHM)
        // contains "CRC32".
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            cols[2].contains("CRC32"),
            "show-checksum-algorithm: CHECKSUM_ALGORITHM column did not contain CRC32, got {:?}",
            cols[2]
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-checksum-algorithm: text off header",
        );

        // Sub-assertion 3: JSON — ChecksumAlgorithm field is emitted when non-empty
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        let algos = v
            .get("ChecksumAlgorithm")
            .and_then(|a| a.as_array())
            .expect("show-checksum-algorithm: ChecksumAlgorithm missing or not an array in JSON");
        assert!(
            algos.iter().any(|a| a.as_str() == Some("CRC32")),
            "show-checksum-algorithm: ChecksumAlgorithm array did not contain CRC32, got {algos:?}"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_show_checksum_type` test**

Append:

```rust

/// `--show-checksum-type` adds a CHECKSUM_TYPE column. Same fixture
/// strategy as show_checksum_algorithm — upload with an explicit CRC32
/// checksum so S3 populates the ChecksumType field automatically.
#[tokio::test]
async fn e2e_display_show_checksum_type() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object_with_checksum_algorithm(&bucket, "file.txt", vec![0u8; 100], "CRC32")
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-checksum-type",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "CHECKSUM_TYPE", "KEY"],
            "show-checksum-type: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-checksum-type: text on row count",
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-checksum-type: text off header",
        );

        // Sub-assertion 3: JSON — ChecksumType field is emitted when set
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(
            v.get("ChecksumType").is_some(),
            "show-checksum-type: ChecksumType missing from JSON"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for --show-checksum-algorithm and --show-checksum-type

Two tests covering checksum display flags. Both upload via
put_object_with_checksum_algorithm with CRC32 so the columns and
JSON fields have populated values to assert against. The
checksum-algorithm test verifies the CRC32 value appears in both
the text column and the JSON array.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 4: Add `show_is_latest` test

The `--show-is-latest` flag requires `--all-versions`, so this test creates a versioned bucket with two versions of the same key.

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_show_is_latest` test**

Append:

```rust

/// `--show-is-latest` adds an IS_LATEST column (requires `--all-versions`).
/// Two versions of the same key guarantee at least one LATEST row and
/// one NOT_LATEST row. The JSON sub-assertion verifies `IsLatest` is
/// present under `--all-versions` regardless of the text-mode flag.
#[tokio::test]
async fn e2e_display_show_is_latest() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of doc.txt — v1 becomes NOT_LATEST, v2 becomes LATEST.
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
            "--show-is-latest",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "VERSION_ID", "IS_LATEST", "KEY"],
            "show-is-latest: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            5,
            "show-is-latest: text on row count",
        );
        assert!(
            output.stdout.contains("LATEST"),
            "show-is-latest: 'LATEST' token missing from text output"
        );
        assert!(
            output.stdout.contains("NOT_LATEST"),
            "show-is-latest: 'NOT_LATEST' token missing from text output"
        );

        // Sub-assertion 2: text with flag OFF (still with --all-versions)
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "VERSION_ID", "KEY"],
            "show-is-latest: text off header",
        );

        // Sub-assertion 3: JSON — IsLatest present under --all-versions
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(
            v.get("VersionId").is_some(),
            "show-is-latest: VersionId missing from JSON"
        );
        assert!(
            v.get("IsLatest").is_some(),
            "show-is-latest: IsLatest missing from JSON"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 3: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 4: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e test for --show-is-latest under --all-versions

Creates a versioned bucket with 2 versions of doc.txt so the text
output has both LATEST and NOT_LATEST rows. The flag-off
sub-assertion uses --all-versions without --show-is-latest to
verify the VERSION_ID column is still present but IS_LATEST is not.
JSON sub-assertion confirms VersionId and IsLatest fields are
present under --all-versions regardless of the text-mode flag.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 5: Add `show_owner` and `show_restore_status` tests (4 sub-assertions each)

Both tests have an asymmetric 4-sub-assertion shape because the flags gate S3-level data fetching. `show_owner` uses non-versioned listing (where the flag actually gates the fetch); `show_restore_status` verifies that the JSON field stays absent for non-Glacier-restored objects even with the flag set.

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_show_owner` test**

Append:

```rust

/// `--show-owner` adds 2 columns (OWNER_DISPLAY_NAME, OWNER_ID). Under
/// non-versioned listing, this flag is the only way to populate owner
/// data — S3's ListObjectsV2 only returns owner when `fetch_owner=true`,
/// which `src/pipeline.rs:177` wires to `display_config.show_owner`.
///
/// This test uses non-versioned listing specifically so the JSON "Owner"
/// field absence/presence tracks the flag. Under --all-versions, S3
/// always returns owner regardless of the flag (see
/// src/storage/s3/mod.rs:174), so the JSON assertion would be
/// non-discriminating.
#[tokio::test]
async fn e2e_display_show_owner() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-owner",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "OWNER_DISPLAY_NAME", "OWNER_ID", "KEY"],
            "show-owner: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            5,
            "show-owner: text on row count",
        );
        // Verify OWNER_ID cell (index 3) is non-empty for the data row.
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            !cols[3].is_empty(),
            "show-owner: OWNER_ID column is empty, expected non-empty owner ID"
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-owner: text off header",
        );

        // Sub-assertion 3: JSON without --show-owner — Owner field absent
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(
            v.get("Owner").is_none(),
            "show-owner: Owner field present in JSON without --show-owner, got {:?}",
            v.get("Owner")
        );

        // Sub-assertion 4: JSON with --show-owner — Owner field present
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-owner",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        let owner = v
            .get("Owner")
            .expect("show-owner: Owner field missing from JSON with --show-owner");
        assert!(
            owner.get("ID").and_then(|id| id.as_str()).is_some_and(|s| !s.is_empty()),
            "show-owner: Owner.ID is empty or missing, got {owner:?}"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_show_restore_status` test**

Append:

```rust

/// `--show-restore-status` adds 2 columns (IS_RESTORE_IN_PROGRESS,
/// RESTORE_EXPIRY_DATE). For non-restored STANDARD objects, S3 doesn't
/// populate the restore fields even when `OptionalObjectAttributes=
/// RestoreStatus` is set, so the text cells are empty and the JSON
/// `RestoreStatus` field is absent in BOTH the flag-on and flag-off
/// JSON runs. This is a "flag is accepted, s3ls runs successfully,
/// field is correctly absent for non-Glacier objects" test rather
/// than a "flag populates the field" test — triggering a real Glacier
/// restore inside an e2e test would require Glacier-class storage
/// (90+ day billing) and lifecycle rules, which are out of scope.
#[tokio::test]
async fn e2e_display_show_restore_status() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "IS_RESTORE_IN_PROGRESS",
                "RESTORE_EXPIRY_DATE",
                "KEY",
            ],
            "show-restore-status: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            5,
            "show-restore-status: text on row count",
        );
        // Verify both restore cells (indices 2 and 3) are empty for a
        // STANDARD object.
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            cols[2].is_empty(),
            "show-restore-status: IS_RESTORE_IN_PROGRESS should be empty for non-restored object, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "show-restore-status: RESTORE_EXPIRY_DATE should be empty for non-restored object, got {:?}",
            cols[3]
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-restore-status: text off header",
        );

        // Sub-assertion 3: JSON without --show-restore-status — RestoreStatus absent
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(
            v.get("RestoreStatus").is_none(),
            "show-restore-status: RestoreStatus unexpectedly present in JSON without flag, got {:?}",
            v.get("RestoreStatus")
        );

        // Sub-assertion 4: JSON with --show-restore-status — still absent
        // for a STANDARD (non-restored) object. The flag is accepted,
        // s3ls runs successfully, and the field stays correctly absent.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert!(
            v.get("RestoreStatus").is_none(),
            "show-restore-status: RestoreStatus should be absent for non-Glacier object even with flag, got {:?}",
            v.get("RestoreStatus")
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for --show-owner and --show-restore-status

Two tests with 4 sub-assertions each because both flags gate an
S3 API-level data fetch. show-owner verifies the JSON Owner field
is absent without the flag and present with it (under non-versioned
listing, where the flag actually gates fetch_owner). show-restore-
status verifies the JSON RestoreStatus field stays absent for a
STANDARD object even with the flag set, because S3 only populates
restore data for Glacier-restored objects.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 6: Add `show_relative_path` and `show_relative_path_prefixed` tests

Two related tests. The first covers the flag against a bucket root (baseline; no observable change). The second covers the flag against a prefixed target (where the flag actually changes the key display).

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_show_relative_path` test**

Append:

```rust

/// `--show-relative-path` baseline test. At bucket root (no prefix),
/// the flag has no observable effect — keys are the same whether
/// rendered relative or not. This test exercises the flag's existence
/// and ensures it doesn't crash at bucket root. The prefixed-target
/// case is covered by `e2e_display_show_relative_path_prefixed`.
#[tokio::test]
async fn e2e_display_show_relative_path() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON — baseline, 3-column header
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-relative-path: text on header (no column added)",
        );
        assert!(
            output.stdout.contains("file.txt"),
            "show-relative-path: key missing from text output"
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-relative-path: text off header",
        );
        assert!(output.stdout.contains("file.txt"));

        // Sub-assertion 3: JSON with flag — Key field is "file.txt"
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert_eq!(
            v.get("Key").and_then(|k| k.as_str()),
            Some("file.txt"),
            "show-relative-path: Key field should be 'file.txt' at bucket root"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_show_relative_path_prefixed` test**

Append:

```rust

/// `--show-relative-path` against a prefixed target. The key
/// `data/foo.txt` is uploaded, but the target is `s3://bucket/data/`,
/// so the flag should render the key as `foo.txt` (relative to the
/// prefix) rather than `data/foo.txt` (full key). Verified in both
/// text and JSON modes since `format_key_display` at
/// `src/aggregate.rs:394, 528` applies to both.
#[tokio::test]
async fn e2e_display_show_relative_path_prefixed() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "data/foo.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/data/");

        // Sub-assertion 1: text — KEY column is "foo.txt" (not "data/foo.txt")
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert_eq!(
            cols.last().copied(),
            Some("foo.txt"),
            "show-relative-path-prefixed: KEY column should be 'foo.txt' relative to data/, got {:?}",
            cols.last()
        );

        // Sub-assertion 2: JSON — Key field is "foo.txt"
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line)
            .expect("JSON line failed to parse");
        assert_eq!(
            v.get("Key").and_then(|k| k.as_str()),
            Some("foo.txt"),
            "show-relative-path-prefixed: Key should be 'foo.txt' relative to data/"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for --show-relative-path (baseline and prefixed)

Two tests for --show-relative-path. The baseline test verifies
the flag is accepted at bucket root where it has no observable
effect (key "file.txt" stays "file.txt"). The prefixed test
verifies the flag renders "data/foo.txt" as "foo.txt" when the
target is s3://bucket/data/, in both text and JSON modes.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 7: Add `all_show_flags_combined` test

The combo test. Every object `--show-*` flag enabled at once (except `--show-is-latest` which requires `--all-versions` and `--show-relative-path` which doesn't add a column), `--header` on, one object with populated checksum data.

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_all_show_flags_combined` test**

Append:

```rust

/// Every object `--show-*` flag enabled at once. Verifies the full
/// 11-column header order and that every row has 11 columns. The
/// fixture uses put_object_with_checksum_algorithm so CHECKSUM_ALGORITHM
/// and CHECKSUM_TYPE cells are populated.
///
/// Does NOT include --all-versions or --show-is-latest (which would
/// need a versioned bucket) — the combo is specifically about the
/// column layout of the maximal non-versioned case.
#[tokio::test]
async fn e2e_display_all_show_flags_combined() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object_with_checksum_algorithm(&bucket, "file.txt", vec![0u8; 100], "CRC32")
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-storage-class",
            "--show-etag",
            "--show-checksum-algorithm",
            "--show-checksum-type",
            "--show-owner",
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "STORAGE_CLASS",
                "ETAG",
                "CHECKSUM_ALGORITHM",
                "CHECKSUM_TYPE",
                "OWNER_DISPLAY_NAME",
                "OWNER_ID",
                "IS_RESTORE_IN_PROGRESS",
                "RESTORE_EXPIRY_DATE",
                "KEY",
            ],
            "combo: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            11,
            "combo: row count",
        );

        // Spot-check: data row cells for CHECKSUM_ALGORITHM (index 4)
        // contain "CRC32" and OWNER_ID (index 7) is non-empty.
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            cols[4].contains("CRC32"),
            "combo: CHECKSUM_ALGORITHM should contain CRC32, got {:?}",
            cols[4]
        );
        assert!(
            !cols[7].is_empty(),
            "combo: OWNER_ID should be non-empty, got {:?}",
            cols[7]
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 3: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 4: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e test for all --show-* flags combined

Enables every non-versioning object --show-* flag at once plus
--header, and asserts the full 11-column header order and row
count. Catches column-order regressions that single-flag tests
might miss. Uses put_object_with_checksum_algorithm so
CHECKSUM_ALGORITHM and CHECKSUM_TYPE cells have values.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 8: Add `common_prefix_row` and `delete_marker_row` tests

Two tests that exercise the special column padding for `CommonPrefix` (PRE) and `DeleteMarker` (DELETE) row types. Each enables several `--show-*` flags at once to verify optional columns are empty for non-object rows.

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_common_prefix_row` test**

Append:

```rust

/// Verifies that `CommonPrefix` rows (rendered as "PRE" in text mode)
/// correctly pad optional columns with empty cells. Uses `--max-depth 1`
/// on a fixture where some keys are at depth 2, so s3ls emits PRE
/// entries at the depth-1 boundary.
#[tokio::test]
async fn e2e_display_common_prefix_row() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // One top-level object, one deep object (collapses to `logs/` PRE
        // under --max-depth 1).
        helper.put_object(&bucket, "top.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "logs/2025/a.log", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "1",
            "--header",
            "--show-etag",
            "--show-storage-class",
            "--show-owner",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Header: DATE, SIZE, STORAGE_CLASS, ETAG, OWNER_DISPLAY_NAME, OWNER_ID, KEY = 7 cols
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "STORAGE_CLASS",
                "ETAG",
                "OWNER_DISPLAY_NAME",
                "OWNER_ID",
                "KEY",
            ],
            "common-prefix-row: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            7,
            "common-prefix-row: row count",
        );

        // Find the PRE row (SIZE column contains "PRE") and verify optional
        // columns are empty.
        let pre_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "PRE"
            })
            .expect("common-prefix-row: no PRE row found");
        let cols = parse_tsv_line(pre_row);
        assert!(
            cols[0].is_empty(),
            "common-prefix-row: PRE row DATE should be empty, got {:?}",
            cols[0]
        );
        assert_eq!(cols[1], "PRE");
        assert!(
            cols[2].is_empty(),
            "common-prefix-row: PRE row STORAGE_CLASS should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "common-prefix-row: PRE row ETAG should be empty, got {:?}",
            cols[3]
        );
        assert!(
            cols[4].is_empty(),
            "common-prefix-row: PRE row OWNER_DISPLAY_NAME should be empty"
        );
        assert!(
            cols[5].is_empty(),
            "common-prefix-row: PRE row OWNER_ID should be empty"
        );
        assert_eq!(
            cols[6], "logs/",
            "common-prefix-row: PRE row KEY should be 'logs/', got {:?}",
            cols[6]
        );

        // Find the object row (SIZE column is numeric, not "PRE") and
        // verify some optional cells are populated.
        let obj_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] != "PRE" && cols[1] != "SIZE"
            })
            .expect("common-prefix-row: no object row found");
        let cols = parse_tsv_line(obj_row);
        assert_eq!(cols[6], "top.txt");
        assert!(!cols[5].is_empty(), "object row OWNER_ID should be populated");
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_delete_marker_row` test**

Append:

```rust

/// Verifies that `DeleteMarker` rows (rendered as "DELETE" in text mode)
/// correctly pad optional columns with empty cells for non-version
/// columns, populate VERSION_ID and KEY, and populate Owner (since
/// ListObjectVersions always returns owner per src/storage/s3/mod.rs:174).
#[tokio::test]
async fn e2e_display_delete_marker_row() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
            "--show-etag",
            "--show-storage-class",
            "--show-owner",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Header: DATE, SIZE, STORAGE_CLASS, ETAG, VERSION_ID, OWNER_DISPLAY_NAME, OWNER_ID, KEY = 8 cols
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "STORAGE_CLASS",
                "ETAG",
                "VERSION_ID",
                "OWNER_DISPLAY_NAME",
                "OWNER_ID",
                "KEY",
            ],
            "delete-marker-row: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            8,
            "delete-marker-row: row count",
        );

        // Find the DELETE row and verify optional columns.
        let dm_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "DELETE"
            })
            .expect("delete-marker-row: no DELETE row found");
        let cols = parse_tsv_line(dm_row);
        assert!(
            !cols[0].is_empty(),
            "delete-marker-row: DELETE row DATE should be populated, got {:?}",
            cols[0]
        );
        assert_eq!(cols[1], "DELETE");
        assert!(
            cols[2].is_empty(),
            "delete-marker-row: DELETE row STORAGE_CLASS should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "delete-marker-row: DELETE row ETAG should be empty, got {:?}",
            cols[3]
        );
        assert!(
            !cols[4].is_empty(),
            "delete-marker-row: DELETE row VERSION_ID should be populated, got {:?}",
            cols[4]
        );
        assert_eq!(cols[7], "doc.txt");
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for PRE and DELETE row padding

Two tests verifying CommonPrefix (PRE) and DeleteMarker (DELETE)
rows correctly pad optional columns with empty cells when multiple
--show-* flags are enabled. PRE rows have empty DATE, STORAGE_CLASS,
ETAG, OWNER_*; DELETE rows have populated DATE and VERSION_ID with
empty STORAGE_CLASS/ETAG (and populated OWNER_* because
ListObjectVersions always returns owner).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 9: Add `summarize_objects` and `summarize_versioned` tests

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_summarize_objects` test**

Append:

```rust

/// Verifies `--summarize` appends a summary line in text mode (with and
/// without `--human-readable`) and a JSON summary object in JSON mode.
/// Fixture is 3 objects × 1000 bytes each = 3000 bytes total.
#[tokio::test]
async fn e2e_display_summarize_objects() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "a.bin", vec![0u8; 1000]).await;
        helper.put_object(&bucket, "b.bin", vec![0u8; 1000]).await;
        helper.put_object(&bucket, "c.bin", vec![0u8; 1000]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text, no human-readable
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--summarize",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(&output.stdout, "summarize: text no-human");
        assert!(
            summary.contains("\t3\tobjects"),
            "summarize text: expected 3 objects in summary, got {summary:?}"
        );
        assert!(
            summary.contains("\t3000\tbytes"),
            "summarize text: expected 3000 bytes, got {summary:?}"
        );

        // Sub-assertion 2: text, human-readable
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--summarize",
            "--human-readable",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(&output.stdout, "summarize: text human");
        assert!(
            summary.contains("\t3\tobjects"),
            "summarize human: expected 3 objects in summary, got {summary:?}"
        );
        // Human-readable form should NOT contain "3000\tbytes" (that's
        // the non-human form).
        assert!(
            !summary.contains("\t3000\tbytes"),
            "summarize human: summary should not have '3000\\tbytes', got {summary:?}"
        );
        // And should contain some unit other than "bytes" (KiB, KB, etc.
        // depending on byte-unit formatting).
        assert!(
            summary.contains("KiB") || summary.contains("KB"),
            "summarize human: expected KiB/KB unit in summary, got {summary:?}"
        );

        // Sub-assertion 3: JSON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--summarize",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let v = assert_summary_present_json(&output.stdout, "summarize: json");
        let summary_obj = v
            .get("Summary")
            .expect("summarize json: missing Summary object");
        assert_eq!(
            summary_obj
                .get("TotalObjects")
                .and_then(|n| n.as_u64()),
            Some(3),
            "summarize json: TotalObjects should be 3"
        );
        assert_eq!(
            summary_obj
                .get("TotalSize")
                .and_then(|n| n.as_u64()),
            Some(3000),
            "summarize json: TotalSize should be 3000"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_summarize_versioned` test**

Append:

```rust

/// Verifies `--summarize --all-versions` appends the delete-markers
/// count to the summary line. Fixture is a versioned bucket with 2
/// versions of doc.txt (100 + 200 bytes) and 1 delete marker.
#[tokio::test]
async fn e2e_display_summarize_versioned() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--summarize",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(
            &output.stdout,
            "summarize versioned",
        );
        // 2 live object versions (100 + 200 = 300 bytes)
        assert!(
            summary.contains("\t2\tobjects"),
            "summarize versioned: expected 2 objects, got {summary:?}"
        );
        assert!(
            summary.contains("\t300\tbytes"),
            "summarize versioned: expected 300 bytes, got {summary:?}"
        );
        // 1 delete marker
        assert!(
            summary.contains("\t1\tdelete markers"),
            "summarize versioned: expected 1 delete markers, got {summary:?}"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for --summarize (objects and versioned)

Two tests for --summarize. summarize_objects uploads 3 × 1000-byte
objects and verifies the text summary (with and without
--human-readable) and the JSON Summary object have the correct
TotalObjects and TotalSize values. summarize_versioned verifies
that --all-versions adds the delete-markers count to the summary.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 10: Add `human_readable` test

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_human_readable` test**

Append:

```rust

/// Verifies `--human-readable` renders object row sizes in human form.
/// Fixture is a 2048-byte object so the expected rendering is exactly
/// "2 KiB" or "2.00 KiB" (2048 / 1024 = 2, binary units).
#[tokio::test]
async fn e2e_display_human_readable() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 2048]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--human-readable",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert!(
            output.stdout.contains("file.txt"),
            "human-readable: key missing from output"
        );
        // 2048 bytes = 2 KiB exactly. byte-unit may render as "2 KiB" or
        // "2.00 KiB" depending on precision — accept either.
        assert!(
            output.stdout.contains("2 KiB") || output.stdout.contains("2.00 KiB"),
            "human-readable: expected '2 KiB' or '2.00 KiB' in output, got:\n{}",
            output.stdout
        );
        // And verify the non-human form is NOT there (no "2048" as a size).
        assert!(
            !output.stdout.contains("\t2048\t"),
            "human-readable: unexpected '\\t2048\\t' in output (should be rendered as KiB):\n{}",
            output.stdout
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 3: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 4: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e test for --human-readable on object row sizes

Uploads a 2048-byte object and runs s3ls --human-readable. Asserts
the output contains "2 KiB" or "2.00 KiB" (either precision the
byte-unit crate might render) and does NOT contain the raw "2048"
as a size cell. Complements the summarize_objects test which
already covers --human-readable on the summary line.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 11: Add bucket listing display tests (`show_bucket_arn`, `show_owner`)

Both tests use `s3ls` with no target argument (bucket listing mode). They scope assertions to the test bucket's unique name, since the account may have other buckets.

**Files:**
- Modify: `tests/e2e_display.rs`

- [ ] **Step 1: Append `e2e_display_bucket_listing_show_bucket_arn` test**

Append:

```rust

/// Bucket listing `--show-bucket-arn` — adds a BUCKET_ARN column in text
/// mode and a `BucketArn` field in JSON mode. Assertions are scoped to
/// the test bucket's unique name because the account may have other
/// buckets.
#[tokio::test]
async fn e2e_display_bucket_listing_show_bucket_arn() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sub-assertion 1: text with flag ON — header contains BUCKET_ARN
        let output = TestHelper::run_s3ls(&[
            "--header",
            "--show-bucket-arn",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing text on: empty stdout");
        assert!(
            header_line.contains("BUCKET_ARN"),
            "bucket listing show-bucket-arn text on: header missing BUCKET_ARN, got {header_line:?}"
        );
        // Find our bucket's row and verify the ARN cell is non-empty.
        let bucket_row = output
            .stdout
            .lines()
            .find(|l| l.contains(&bucket))
            .unwrap_or_else(|| {
                panic!("bucket listing text on: test bucket {bucket} not found in output")
            });
        let cols = parse_tsv_line(bucket_row);
        // Header is DATE\tREGION\tBUCKET\tBUCKET_ARN[\tOWNER...], so
        // BUCKET_ARN is column index 3.
        assert!(
            cols.len() >= 4 && !cols[3].is_empty(),
            "bucket listing show-bucket-arn: expected non-empty BUCKET_ARN cell, got row {bucket_row:?}"
        );

        // Sub-assertion 2: text with flag OFF — header does NOT contain BUCKET_ARN
        let output = TestHelper::run_s3ls(&["--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing text off: empty stdout");
        assert!(
            !header_line.contains("BUCKET_ARN"),
            "bucket listing show-bucket-arn text off: header unexpectedly contains BUCKET_ARN, got {header_line:?}"
        );

        // Sub-assertion 3: JSON with flag ON — BucketArn field present
        let output = TestHelper::run_s3ls(&["--json", "--show-bucket-arn"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let bucket_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket)
                    })
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!("bucket listing json on: test bucket {bucket} not found in JSON output")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("BucketArn").is_some(),
            "bucket listing show-bucket-arn json on: BucketArn field missing, got {v:?}"
        );

        // Sub-assertion 4: JSON without flag — BucketArn field absent
        let output = TestHelper::run_s3ls(&["--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let bucket_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket)
                    })
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!("bucket listing json off: test bucket {bucket} not found in JSON output")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("BucketArn").is_none(),
            "bucket listing show-bucket-arn json off: BucketArn should be absent, got {:?}",
            v.get("BucketArn")
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_display_bucket_listing_show_owner` test**

Append:

```rust

/// Bucket listing `--show-owner` — adds OWNER_DISPLAY_NAME and OWNER_ID
/// columns in text mode and an `Owner` object in JSON mode.
#[tokio::test]
async fn e2e_display_bucket_listing_show_owner() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sub-assertion 1: text with flag ON — header contains OWNER_ID
        let output = TestHelper::run_s3ls(&["--header", "--show-owner"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing show-owner text on: empty stdout");
        assert!(
            header_line.contains("OWNER_ID"),
            "bucket listing show-owner text on: header missing OWNER_ID, got {header_line:?}"
        );

        // Sub-assertion 2: text with flag OFF — header does NOT contain OWNER_ID
        let output = TestHelper::run_s3ls(&["--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing show-owner text off: empty stdout");
        assert!(
            !header_line.contains("OWNER_ID"),
            "bucket listing show-owner text off: header unexpectedly contains OWNER_ID, got {header_line:?}"
        );

        // Sub-assertion 3: JSON with flag ON — Owner field present for our bucket
        let output = TestHelper::run_s3ls(&["--json", "--show-owner"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let bucket_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket)
                    })
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!("bucket listing show-owner json on: test bucket {bucket} not found")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("Owner").is_some(),
            "bucket listing show-owner json on: Owner field missing, got {v:?}"
        );

        // Sub-assertion 4: JSON without flag — Owner field absent
        let output = TestHelper::run_s3ls(&["--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let bucket_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket)
                    })
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| {
                panic!("bucket listing show-owner json off: test bucket {bucket} not found")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("Owner").is_none(),
            "bucket listing show-owner json off: Owner should be absent, got {:?}",
            v.get("Owner")
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. This completes the 17-test file.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_display.rs
git commit -m "$(cat <<'EOF'
test(display): add e2e tests for bucket listing display flags

Two tests covering bucket listing display: --show-bucket-arn and
--show-owner. Each has 4 sub-assertions (text on/off, JSON on/off).
Assertions are scoped to the test bucket's unique name because
the account may have other buckets from concurrent test runs or
ambient setup.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### D-Task 12: Manual verification against real S3

Manual-only verification. Does **not** run in CI. Requires a configured `s3ls-e2e-test` AWS profile.

**Files:** (none modified)

- [ ] **Step 1: Confirm AWS profile**

Run: `aws configure list --profile s3ls-e2e-test`
Expected: shows a configured profile with region and credentials.

- [ ] **Step 2: Run the display suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_display -- --nocapture`
Expected: all 17 tests pass. Runtime: ~2-4 minutes depending on region latency.

- [ ] **Step 3: Run the full e2e suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture`
Expected: `e2e_listing`, `e2e_filters`, `e2e_filters_versioned`, and `e2e_display` all pass.

- [ ] **Step 4: Confirm non-gated `cargo test` stays clean**

Run: `cargo test`
Expected: all existing unit + bin tests pass.

- [ ] **Step 5: Check for leaked buckets**

Run:
```bash
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text
```
Expected: empty output. If non-empty, follow `tests/README.md` for manual cleanup (versioned buckets need non-current versions + delete markers removed before `DeleteBucket`).

- [ ] **Step 6: No commit for this task**

Verification only.

---

## Notes for the executor

- **Each task produces one commit** except D-Task 12 (verification only). Expect 11 commits on the branch.
- **The file `tests/e2e_display.rs` grows monotonically** — every task after D-Task 2 appends. Do not reorder tests within the file.
- **`rustfmt` may reflow argument slices and expected-column arrays** — that's fine, expected, and does not change semantics.
- **Bucket listing tests (D-Task 11)** scope their assertions to the test bucket's unique name. If the account has many other buckets, that's fine — the test only looks at its own bucket's row.
- **`--show-restore-status`'s JSON assertion is intentionally a negative assertion** (field stays absent). See the spec for why triggering a real Glacier restore in a test isn't practical.
- **Do NOT add `#[ignore]` to any test.** Flakiness should be diagnosed via `--nocapture` and fixed at the root.
- **The only test that sleeps is ... none of them.** Display tests don't require time pivots.
