# Versioned-Bucket Filter E2E Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add end-to-end test coverage for filter behaviors that are specific to versioned S3 buckets — 7 tests in a new `tests/e2e_filters_versioned.rs` file, using 2 new helpers in `tests/common/mod.rs`.

**Architecture:** Gated with `#![cfg(e2e_test)]` (cfg pre-registered in `Cargo.toml`). One new file holds all 7 tests; each test creates a fresh versioned bucket via `create_versioned_bucket`, builds a minimal inline fixture, runs `s3ls --all-versions --json` with a single filter flag, and asserts via a new multiset-based helper `assert_json_version_shapes_eq`. Delete markers are created via a new `create_delete_marker` helper that calls `DeleteObject` without a VersionId on a versioned bucket.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (production deps), `serde_json` (production dep), `chrono` for RFC3339 formatting, `std::collections::HashMap` for multiset comparison.

**Depends on:** Step 6 (e2e framework) and Step 7 (filter e2e tests). The `create_versioned_bucket` helper already exists at `tests/common/mod.rs:124`; the `delete_bucket_cascade` teardown already handles versioned buckets correctly.

**Spec:** `docs/superpowers/specs/2026-04-11-versioned-filter-e2e-tests-design.md`

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- Auto-memory feedback: **logging via `tracing` / `tracing-subscriber`**, not `log` directly. (Not relevant — no logging code — but noted.)

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/e2e_filters_versioned.rs` | 7 `#[tokio::test]` functions exercising versioning-specific filter behaviors. Gated with `#![cfg(e2e_test)]`. |

**Modified files:**

| Path | Change |
|---|---|
| `tests/common/mod.rs` | Add `TestHelper::create_delete_marker` method and `assert_json_version_shapes_eq` free function. |

**No changes to `src/`.** No `Cargo.toml` changes — `serde_json`, `chrono`, `aws-sdk-s3`, `tokio` are all existing production dependencies.

---

## Important notes for the executor

**Framework reuse.** Every test uses the step 6 lifecycle pattern:

```rust
let helper = TestHelper::new().await;
let bucket = helper.generate_bucket_name();
let _guard = helper.bucket_guard(&bucket);

e2e_timeout!(async {
    helper.create_versioned_bucket(&bucket).await;  // note: versioned variant
    // ... fixture setup ...
    // ... run_s3ls + assertion ...
});

_guard.cleanup().await;  // OUTSIDE e2e_timeout!
```

Use `create_versioned_bucket`, NOT `create_bucket`. The versioned variant already exists at `tests/common/mod.rs:124`.

**Multi-version uploads.** To create multiple versions of the same key, call `put_object(&bucket, "same_key", body)` multiple times. On a versioned bucket, each call produces a new version. `put_objects_parallel` can also be used but sequential `put_object` calls are clearer when order-of-upload matters for mtime pivots.

**Delete marker creation.** The new `create_delete_marker` helper (Task 1) calls `DeleteObject` without a `VersionId`. On a versioned bucket, S3 interprets this as "add a delete marker" rather than "permanently delete the object". Do NOT try to create a delete marker by other means.

**Assertion semantics.** The new `assert_json_version_shapes_eq` helper compares a **multiset** of `(Key, is_delete_marker)` tuples — not a set. A key with 3 versions shows up 3 times in the expected slice. This distinguishes "2 versions of `doc.txt` + 1 DM" from "1 version of `doc.txt` + 1 DM".

**`--hide-delete-markers` precedence** is applied BEFORE the filter chain (`src/lister.rs:48`). Tests that observe "DM passes filter" MUST NOT pass `--hide-delete-markers`, or they'll see the hide flag's effect rather than the filter's. Only Test 6 uses this flag, and it does so intentionally.

**`--all-versions --json` field shapes:**
- Regular versioned object: `{"Key": ..., "VersionId": ..., "Size": ..., "LastModified": ..., ...}` — no `DeleteMarker` field
- Delete marker: `{"Key": ..., "VersionId": ..., "LastModified": ..., "DeleteMarker": true, ...}`

`assert_json_version_shapes_eq` defaults missing `DeleteMarker` to `false` per this contract.

**`cargo fmt` and `cargo clippy` before every commit.** Auto-memory feedback applies.

**Running the tests against real S3 is NOT part of this plan.** All verification in Tasks 1-6 is compile + clippy + fmt only. Task 7 is manual real-S3 verification.

**Verification commands used throughout:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — must pass with the new e2e file compiling to an empty binary. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build — compiles framework + versioned-filter tests. Does NOT hit S3. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint. |
| `cargo clippy --all-features -- -D warnings` | Non-gated lint. |
| `cargo fmt --check` | Formatting. |
| `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters_versioned -- --nocapture` | **Task 7 only.** Final verification against real S3. |

---

### Task 1: Add `create_delete_marker` and `assert_json_version_shapes_eq` to `tests/common/mod.rs`

Pure framework plumbing. No new test file yet.

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add `create_delete_marker` method**

Find the `put_object_with_storage_class` method (added in Step 7 Task 1) inside the `impl TestHelper` block that starts with `// Object operations`. Immediately after `put_object_with_storage_class`, add:

```rust
    /// Create a delete marker on a versioned bucket by calling DeleteObject
    /// without a VersionId. On a versioned bucket, S3 interprets this as
    /// "add a delete marker" (the object appears deleted to non-versioned
    /// readers, but all prior versions remain listable via
    /// ListObjectVersions).
    ///
    /// Requires: the bucket must have versioning ENABLED (create it via
    /// `create_versioned_bucket`). On a non-versioned bucket this call
    /// would permanently delete the object.
    pub async fn create_delete_marker(&self, bucket: &str, key: &str) {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to create delete marker for {key} in {bucket}: {e}")
            });
    }
```

- [ ] **Step 2: Add `assert_json_version_shapes_eq` free function**

At the very bottom of `tests/common/mod.rs`, after the existing `assert_json_keys_or_prefixes_eq` function, append:

```rust
/// Parse NDJSON stdout from `s3ls --all-versions --json` and assert the
/// multiset of `(Key, is_delete_marker)` tuples equals `expected`.
///
/// Unlike `assert_json_keys_eq` (which compares a set of `Key` strings),
/// this helper:
/// 1. Extracts both `Key` and the `DeleteMarker` boolean field from each
///    JSON line (missing `DeleteMarker` field defaults to `false`).
/// 2. Uses multiset comparison: 3 rows of `("doc.txt", false)` is
///    distinguishable from 2 rows of `("doc.txt", false)`.
///
/// Panics if:
/// - any non-empty line fails to parse as JSON,
/// - any JSON line is missing the `Key` field,
/// - the multiset of `(Key, is_delete_marker)` tuples does not equal
///   `expected` (reports missing and extra counts separately).
pub fn assert_json_version_shapes_eq(
    stdout: &str,
    expected: &[(&str, bool)],
    label: &str,
) {
    use std::collections::HashMap;

    let mut actual: HashMap<(String, bool), usize> = HashMap::new();
    for line in stdout.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
            panic!("[{label}] failed to parse JSON line: {line}\nerror: {e}")
        });
        let key = v
            .get("Key")
            .and_then(|k| k.as_str())
            .unwrap_or_else(|| panic!("[{label}] JSON line missing `Key`: {line}"))
            .to_string();
        let is_delete_marker = v
            .get("DeleteMarker")
            .and_then(|d| d.as_bool())
            .unwrap_or(false);
        *actual.entry((key, is_delete_marker)).or_insert(0) += 1;
    }

    let mut expected_counts: HashMap<(String, bool), usize> = HashMap::new();
    for (key, is_dm) in expected {
        *expected_counts
            .entry((key.to_string(), *is_dm))
            .or_insert(0) += 1;
    }

    if actual != expected_counts {
        let mut missing: Vec<((String, bool), usize)> = expected_counts
            .iter()
            .filter_map(|(k, c)| {
                let a = actual.get(k).copied().unwrap_or(0);
                if a < *c {
                    Some((k.clone(), c - a))
                } else {
                    None
                }
            })
            .collect();
        missing.sort();
        let mut extra: Vec<((String, bool), usize)> = actual
            .iter()
            .filter_map(|(k, c)| {
                let e = expected_counts.get(k).copied().unwrap_or(0);
                if *c > e {
                    Some((k.clone(), c - e))
                } else {
                    None
                }
            })
            .collect();
        extra.sort();
        panic!(
            "[{label}] version shape multiset mismatch\n  missing: {missing:?}\n  extra:   {extra:?}\n  stdout:\n{stdout}"
        );
    }
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass, no warnings. New helpers are in `tests/common/mod.rs` which has `#![allow(dead_code)]`, so unused helpers won't trigger warnings.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. Existing `tests/e2e_listing.rs` and `tests/e2e_filters.rs` still compile.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(versioning): add e2e helpers for delete markers and version shape assertions

Adds two helpers to the e2e framework:
- TestHelper::create_delete_marker: call DeleteObject without a
  VersionId on a versioned bucket to add a delete marker (rather than
  permanently delete the object).
- assert_json_version_shapes_eq: parse --all-versions --json NDJSON
  and compare the multiset of (Key, is_delete_marker) tuples against
  an expected slice.

No production code changes. No Cargo.toml changes.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Scaffold `tests/e2e_filters_versioned.rs` with the two regex tests

Creates the new file with the preamble and Tests 1 and 2 (both regex-filter tests, similar structure).

**Files:**
- Create: `tests/e2e_filters_versioned.rs`

- [ ] **Step 1: Create `tests/e2e_filters_versioned.rs` with preamble and Test 1**

Create the file with:

```rust
#![cfg(e2e_test)]

//! Versioned-bucket filter end-to-end tests.
//!
//! Covers filter behaviors that are specific to versioned S3 buckets —
//! interactions that `tests/e2e_filters.rs` explicitly defers:
//!
//! 1. Regex filters apply to delete-marker keys.
//! 2. Size and storage-class filters let delete markers pass through
//!    unconditionally.
//! 3. Mtime filters evaluate delete markers by their own timestamps.
//! 4. `--hide-delete-markers` strips delete markers regardless of filters.
//! 5. Filters evaluate each version of a key independently.
//!
//! Each test creates a fresh versioned bucket via `create_versioned_bucket`,
//! builds a minimal inline fixture, runs `s3ls --all-versions --json` with
//! a single filter flag, and asserts the resulting NDJSON via
//! `assert_json_version_shapes_eq` (a multiset of `(Key, is_delete_marker)`
//! tuples).
//!
//! Design: `docs/superpowers/specs/2026-04-11-versioned-filter-e2e-tests-design.md`

mod common;

use common::*;

/// Proves `--filter-include-regex` is applied to delete-marker keys: a
/// delete marker whose key matches the regex is kept; a delete marker
/// whose key doesn't match is dropped. Two versions of `keep.csv` plus
/// two delete markers exercise both multi-version handling and the
/// regex-against-DM-key contract simultaneously.
#[tokio::test]
async fn e2e_versioned_include_regex_drops_delete_marker() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of keep.csv
        helper.put_object(&bucket, "keep.csv", vec![0u8; 100]).await;
        helper.put_object(&bucket, "keep.csv", vec![0u8; 200]).await;

        // One version of drop.txt
        helper.put_object(&bucket, "drop.txt", vec![0u8; 100]).await;

        // Delete markers on both keys
        helper.create_delete_marker(&bucket, "drop.txt").await;
        helper.create_delete_marker(&bucket, "keep.csv").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 2 keep.csv object rows + 1 keep.csv DM row.
        // drop.txt v1 fails the regex; drop.txt DM fails the regex.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[
                ("keep.csv", false),
                ("keep.csv", false),
                ("keep.csv", true),
            ],
            "versioned include-regex: DM filtered by key",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append Test 2 (exclude-regex)**

Append to the same file:

```rust

/// Proves `--filter-exclude-regex` is applied to delete-marker keys: a
/// delete marker whose key matches the exclude regex is dropped.
/// Inverse of `e2e_versioned_include_regex_drops_delete_marker`.
#[tokio::test]
async fn e2e_versioned_exclude_regex_drops_delete_marker() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of keep.bin
        helper.put_object(&bucket, "keep.bin", vec![0u8; 100]).await;
        helper.put_object(&bucket, "keep.bin", vec![0u8; 200]).await;

        // One version of skip_me.bin
        helper.put_object(&bucket, "skip_me.bin", vec![0u8; 100]).await;

        // Delete markers on both keys
        helper.create_delete_marker(&bucket, "skip_me.bin").await;
        helper.create_delete_marker(&bucket, "keep.bin").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-exclude-regex",
            "^skip_",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 2 keep.bin object rows + 1 keep.bin DM row.
        // skip_me.bin v1 fails the exclude regex; skip_me.bin DM fails
        // the exclude regex.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[
                ("keep.bin", false),
                ("keep.bin", false),
                ("keep.bin", true),
            ],
            "versioned exclude-regex: DM filtered by key",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass. New file compiles to empty under non-gated builds.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build with both new tests type-checked.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_filters_versioned.rs
git commit -m "$(cat <<'EOF'
test(versioning): add e2e tests for regex filters applied to delete-marker keys

Two tests covering --filter-include-regex and --filter-exclude-regex
against versioned buckets. Each fixture has one key with 2 object
versions plus a delete marker, and a second key whose object version
and delete marker are both filtered out. Uses the multiset assertion
helper to verify exact version counts per (Key, is_delete_marker) shape.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add Tests 3 and 5 (size passthrough and storage-class passthrough)

Two tests that exercise the "delete markers always pass through" contract for size and storage-class filters. Both are simple (no sleep, no runtime computation).

**Files:**
- Modify: `tests/e2e_filters_versioned.rs`

- [ ] **Step 1: Append Test 3 (`e2e_versioned_size_filter_passes_delete_markers`)**

Append to the file:

```rust

/// Locks in "delete markers always pass size filters" — verified against
/// `src/filters/smaller_size.rs:25` and `larger_size.rs:25`, both of which
/// unconditionally return `Ok(true)` for `ListEntry::DeleteMarker` before
/// any size comparison.
///
/// Does NOT use `--hide-delete-markers` because the test's entire point is
/// to observe a delete marker surviving the filter; the hide flag would
/// strip the DM before the filter chain runs (`src/lister.rs:48` applies
/// it before `filter_chain.matches` at line 51).
#[tokio::test]
async fn e2e_versioned_size_filter_passes_delete_markers() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of big.bin, both passing --filter-larger-size 1000.
        helper.put_object(&bucket, "big.bin", vec![0u8; 5000]).await;
        helper.put_object(&bucket, "big.bin", vec![0u8; 7000]).await;

        // One version of small.bin (100 bytes, fails size filter).
        helper.put_object(&bucket, "small.bin", vec![0u8; 100]).await;

        // DM on small.bin — has no size, must pass the filter anyway.
        helper.create_delete_marker(&bucket, "small.bin").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-larger-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 2 big.bin versions + 1 small.bin DM.
        // small.bin v1 (100 bytes) fails --filter-larger-size 1000.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[
                ("big.bin", false),
                ("big.bin", false),
                ("small.bin", true),
            ],
            "versioned size filter: DM passes through",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append Test 5 (`e2e_versioned_storage_class_passes_delete_markers`)**

Append to the same file:

```rust

/// Locks in "delete markers always pass storage-class filter" — verified
/// against `src/filters/storage_class.rs:47`
/// (`ListEntry::DeleteMarker { .. } => Ok(true)`).
#[tokio::test]
async fn e2e_versioned_storage_class_passes_delete_markers() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // ia.bin: explicit STANDARD_IA class (fails --storage-class STANDARD).
        helper
            .put_object_with_storage_class(&bucket, "ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;
        // DM on ia.bin — has no storage class, must pass the filter anyway.
        helper.create_delete_marker(&bucket, "ia.bin").await;

        // std.bin: default STANDARD class (S3 reports as None, filter treats
        // as STANDARD per src/filters/storage_class.rs:33).
        helper.put_object(&bucket, "std.bin", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 1 std.bin object row + 1 ia.bin DM row.
        // ia.bin v1 (STANDARD_IA) fails the filter.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("std.bin", false), ("ia.bin", true)],
            "versioned storage-class: DM passes through",
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
git add tests/e2e_filters_versioned.rs
git commit -m "$(cat <<'EOF'
test(versioning): add e2e tests for delete-marker passthrough in size and storage-class filters

Two tests locking in that ListEntry::DeleteMarker unconditionally
passes size and storage-class filters (verified against
src/filters/smaller_size.rs:25, larger_size.rs:25, and
storage_class.rs:47). Each test has a delete marker that must
survive the filter despite the filtered object version failing it.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add Test 4 (`e2e_versioned_mtime_filter_applies_to_delete_markers`)

The most complex test in the suite: two-batch upload with a 1.5s sleep, runtime pivot computation, and a three-part proof (DMs subject to mtime; DM mtime is its own creation time, not the object's; original version of a key still fails mtime even if a later DM on the same key passes).

**Files:**
- Modify: `tests/e2e_filters_versioned.rs`

- [ ] **Step 1: Append Test 4**

Append to the file:

```rust

/// Locks in "mtime filters DO apply to delete-marker timestamps" —
/// verified against `src/filters/mtime_before.rs:27` and
/// `mtime_after.rs:27`, which both use `entry.last_modified()` uniformly
/// for both objects and delete markers.
///
/// Two-batch fixture with a 1.5s sleep between batches to guarantee a
/// second-level time pivot:
///   Batch 1: put_object("old.bin", ...) — v1 of old.bin, BEFORE pivot
///   sleep 1.5s
///   Batch 2: put_object("new.bin", ...) — v1 of new.bin, AFTER pivot
///           create_delete_marker("old.bin") — DM on old.bin, AFTER pivot
///
/// Expected under `--filter-mtime-after <pivot>`:
///   - new.bin v1 passes (batch 2)
///   - old.bin v1 fails (batch 1, before pivot)
///   - old.bin DM passes (created in batch 2, after pivot — DM mtime is
///     its own creation time, not the original object's)
#[tokio::test]
async fn e2e_versioned_mtime_filter_applies_to_delete_markers() {
    use chrono::{DateTime, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Batch 1: old.bin v1 (BEFORE pivot)
        helper.put_object(&bucket, "old.bin", vec![0u8; 100]).await;

        sleep(Duration::from_millis(1500)).await;

        // Batch 2: new.bin v1 + DM on old.bin (BOTH AFTER pivot)
        helper.put_object(&bucket, "new.bin", vec![0u8; 100]).await;
        helper.create_delete_marker(&bucket, "old.bin").await;

        // Read back all rows via list_object_versions. This returns
        // objects AND delete markers with their LastModified timestamps.
        // Compute t_pivot = min(batch 2 last_modified) and sanity-check
        // it is strictly after old.bin v1's LastModified.
        let resp = helper
            .client()
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_object_versions failed");

        let mut old_lm: Option<DateTime<Utc>> = None;
        let mut batch2_min: Option<DateTime<Utc>> = None;

        // Regular object versions
        for v in resp.versions() {
            let key = v.key().expect("version missing key");
            let lm = v.last_modified().expect("version missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            if key == "old.bin" {
                // old.bin v1 — batch 1
                old_lm = Some(dt);
            } else {
                // new.bin v1 — batch 2
                batch2_min = Some(match batch2_min {
                    None => dt,
                    Some(cur) => cur.min(dt),
                });
            }
        }

        // Delete markers (old.bin DM — batch 2)
        for m in resp.delete_markers() {
            let lm = m.last_modified().expect("DM missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            batch2_min = Some(match batch2_min {
                None => dt,
                Some(cur) => cur.min(dt),
            });
        }

        let old_lm = old_lm.expect("old.bin v1 not found in listing");
        let t_pivot = batch2_min.expect("batch 2 rows not found in listing");

        assert!(
            t_pivot > old_lm,
            "t_pivot ({t_pivot}) must be strictly after old.bin v1 ({old_lm}) \
             — the 1.5s sleep should have guaranteed this. Clock skew > 1.5s?"
        );

        let mtime_after = t_pivot.to_rfc3339();
        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-mtime-after",
            mtime_after.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: new.bin v1 + old.bin DM. old.bin v1 fails mtime-after.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("new.bin", false), ("old.bin", true)],
            "versioned mtime-after: DM filtered by own timestamp",
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
git add tests/e2e_filters_versioned.rs
git commit -m "$(cat <<'EOF'
test(versioning): add e2e test for mtime filter against delete marker timestamps

Locks in that mtime filters evaluate delete marker timestamps (not
the original object's mtime), and that a DM's mtime is its own
creation time. Two-batch fixture with a 1.5s sleep: old.bin v1 in
batch 1, new.bin v1 + old.bin DM in batch 2. Under
--filter-mtime-after <t_pivot>, new.bin v1 and old.bin DM survive
but old.bin v1 fails.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Add Test 6 (`e2e_versioned_hide_delete_markers`)

Two-part test: run `s3ls` twice against the same bucket — once with `--hide-delete-markers` and once without — to prove the flag makes a difference.

**Files:**
- Modify: `tests/e2e_filters_versioned.rs`

- [ ] **Step 1: Append Test 6**

Append to the file:

```rust

/// Locks in `--hide-delete-markers` behavior. Runs s3ls twice against
/// the same bucket: once WITH the flag (expect 2 rows) and once
/// WITHOUT (expect 3 rows). The difference of exactly one delete
/// marker row proves the flag strips DMs as documented.
///
/// `--hide-delete-markers` is applied at `src/lister.rs:48`, BEFORE
/// the filter chain runs at line 51. This test doesn't combine the
/// flag with any filter; it asserts the flag's effect in isolation.
#[tokio::test]
async fn e2e_versioned_hide_delete_markers() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of doc.txt plus a delete marker as the "latest".
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        // Run 1: with --hide-delete-markers. Expect 2 object rows, no DM.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--hide-delete-markers",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("doc.txt", false), ("doc.txt", false)],
            "hide-delete-markers: DM stripped",
        );

        // Run 2: without --hide-delete-markers. Expect 2 object rows + 1 DM.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_version_shapes_eq(
            &output.stdout,
            &[
                ("doc.txt", false),
                ("doc.txt", false),
                ("doc.txt", true),
            ],
            "hide-delete-markers: baseline includes DM",
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
git add tests/e2e_filters_versioned.rs
git commit -m "$(cat <<'EOF'
test(versioning): add e2e test for --hide-delete-markers

Runs s3ls twice against a bucket with 2 versions of doc.txt plus a
delete marker: once with --hide-delete-markers (expect 2 rows, no
DM) and once without (expect 3 rows including the DM). The exact
one-row difference proves the flag strips delete markers as
documented.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Add Test 7 (`e2e_versioned_size_filter_per_version`)

Final test: proves filters evaluate each version's own metadata independently. Same key with 3 different sizes across versions; only the middle version survives.

**Files:**
- Modify: `tests/e2e_filters_versioned.rs`

- [ ] **Step 1: Append Test 7**

Append to the file:

```rust

/// Locks in "size filters evaluate each version's own size" — the same
/// key appears with 3 different sizes across versions, and only the
/// middle version (v2, 5000 bytes) survives `--filter-larger-size 1000`.
///
/// This is the one test in the suite where the same key appears multiple
/// times in the fixture but NOT all versions survive. It proves filters
/// see each version's metadata independently rather than treating all
/// versions of a key as a unit.
#[tokio::test]
async fn e2e_versioned_size_filter_per_version() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Three versions of growing.bin: small, large, small again.
        helper.put_object(&bucket, "growing.bin", vec![0u8; 100]).await;
        helper.put_object(&bucket, "growing.bin", vec![0u8; 5000]).await;
        helper.put_object(&bucket, "growing.bin", vec![0u8; 200]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-larger-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: ONLY v2 (5000 bytes) survives. v1 (100) and v3 (200)
        // fail the size filter on their own sizes.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("growing.bin", false)],
            "versioned size filter: only v2 survives per-version check",
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
Expected: successful build. This completes the 7-test file.

- [ ] **Step 4: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_filters_versioned.rs
git commit -m "$(cat <<'EOF'
test(versioning): add e2e test for per-version size filter evaluation

Uploads 3 versions of growing.bin at sizes 100, 5000, 200. Under
--filter-larger-size 1000, only v2 (5000 bytes) survives — v1 and
v3 fail on their own sizes. Proves filters evaluate each version's
metadata independently rather than treating versions of a key as
a unit.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Manual verification against real S3

Manual-only verification. Does **not** run in CI. Requires a configured `s3ls-e2e-test` AWS profile.

**Files:** (none modified)

- [ ] **Step 1: Confirm AWS profile exists**

Run: `aws configure list --profile s3ls-e2e-test`
Expected: shows a configured profile with region and credentials source.

- [ ] **Step 2: Run the versioning suite against real S3**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters_versioned -- --nocapture`
Expected: all 7 tests pass. Each creates a fresh `s3ls-e2e-{uuid}` versioned bucket, uploads a small fixture, runs s3ls with the filter under test, asserts via `assert_json_version_shapes_eq`, and cleans up. Expected runtime: one to two minutes depending on region latency. Watch for any test that hits the `e2e_timeout!` 60-second hard limit.

- [ ] **Step 3: Run the full e2e suite to confirm no cross-suite regressions**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture`
Expected: `e2e_listing` (step 6), `e2e_filters` (step 7), and `e2e_filters_versioned` (this step) all pass.

- [ ] **Step 4: Confirm non-gated `cargo test` still clean**

Run: `cargo test`
Expected: all existing unit tests pass.

- [ ] **Step 5: Check for any leaked buckets**

Run:
```bash
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text
```
Expected: empty output. If non-empty, follow `tests/README.md` "Versioned buckets" section for manual cleanup (since versioned buckets need non-current version + delete marker removal before `DeleteBucket`).

- [ ] **Step 6: No commit for this task**

Verification only. If any of the steps failed, fix the underlying issue in the test that failed and re-run before proceeding.

---

## Notes for the executor

- **Each task produces one commit** except Task 7. Expect 6 commits on the branch.
- **The file `tests/e2e_filters_versioned.rs` grows monotonically** — every task after Task 2 appends. Do not reorder tests within the file.
- **`rustfmt` may reflow argument slices** — that's fine, expected, and does not change semantics.
- **If a test fails in Task 7**, the fix is almost always in the test code (wrong expected shape, mis-typed fixture, sleep too short for a slow network). Only edit `tests/common/mod.rs` if a helper itself is broken.
- **Do NOT add `#[ignore]` to any test** — flakiness should be diagnosed via `--nocapture` output and fixed at the root.
- **Task 4 is the only task that sleeps.** The 1.5s sleep is unavoidable — S3 `LastModified` is second-precision and the test needs a time gap between batch 1 and batch 2.
