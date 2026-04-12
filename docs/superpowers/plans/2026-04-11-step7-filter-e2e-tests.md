# Step 7: Filter E2E Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add end-to-end test coverage for every filter flag in s3ls, plus their combinations, plus two orthogonal-flag interaction smoke tests — all in a single `tests/e2e_filters.rs` file, reusing the step 6 framework.

**Architecture:** Gated via `#![cfg(e2e_test)]` (cfg flag pre-registered in `Cargo.toml`). One `tests/e2e_filters.rs` file holds 14 test functions in three categories: per-filter (7), combinations (5), orthogonal-flag smokes (2). Per-filter tests use a shared-fixture-within-a-test pattern (one bucket per test, multiple `run_s3ls` invocations with labeled sub-assertions). Combination/smoke tests use one-test-per-scenario. Three new helpers (`assert_json_keys_eq`, `assert_json_keys_or_prefixes_eq`, `put_object_with_storage_class`) land in `tests/common/mod.rs`. Assertions compare sets of `Key` fields parsed from `s3ls --json` NDJSON output.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (production deps, reused by tests), `serde_json` (production dep, reused by tests), `chrono` for RFC3339 formatting, `std::collections::HashSet` for set equality.

**Depends on:** Step 6 (e2e framework in `tests/common/mod.rs` + `tests/e2e_listing.rs`). All framework plumbing — `TestHelper`, `BucketGuard`, `run_s3ls`, `e2e_timeout!` — is reused without modification.

**Spec:** `docs/superpowers/specs/2026-04-11-step7-filter-e2e-tests-design.md`

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- Auto-memory feedback: **logging via `tracing` / `tracing-subscriber`**, not `log` directly. (Not relevant — the plan adds no logging code — but noted.)

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/e2e_filters.rs` | 14 `#[tokio::test]` functions exercising every filter flag, their combinations, and two orthogonal-flag interaction smoke tests. Gated with `#![cfg(e2e_test)]`. |

**Modified files:**

| Path | Change |
|---|---|
| `tests/common/mod.rs` | Add three helpers: `assert_json_keys_eq`, `assert_json_keys_or_prefixes_eq`, `put_object_with_storage_class`. Add `StorageClass` to the `aws_sdk_s3::types` import. |

**No changes to `src/`.** No `Cargo.toml` changes — `serde_json`, `chrono`, `aws-sdk-s3`, and `tokio` are all existing production dependencies, available to test code automatically.

---

## Important notes for the executor

**Framework reuse.** Every test in this plan uses the step 6 pattern verbatim:

```rust
let helper = TestHelper::new().await;
let bucket = helper.generate_bucket_name();
let _guard = helper.bucket_guard(&bucket);

e2e_timeout!(async {
    helper.create_bucket(&bucket).await;
    // ... fixture setup ...
    // ... run_s3ls calls + assertions ...
});

_guard.cleanup().await;
```

Do **NOT** put `_guard.cleanup().await` inside `e2e_timeout!` — it must run after the timed block so cleanup still happens on slow-but-eventually-succeeding tests. A panic inside the timed block will skip cleanup (intentional — see step 6 spec for why).

**Regex arguments to `run_s3ls`.** The `run_s3ls` helper takes `&[&str]`, so regex patterns should be passed as Rust string literals. Use raw strings (`r"\.csv$"`) to avoid double-backslashing. The shell is NOT involved — `std::process::Command::args` passes each slice element as a single argv entry, so the regex is literal and does not need shell quoting.

**`--json` + `--recursive` for every filter test.** The plan always passes `--json` (for parseable assertion) and `--recursive` (so the listing covers every object). Non-recursive listing is a separate concern, tested in a later step.

**Tie-handling for mtime tests.** S3 `LastModified` is second-precision. Parallel uploads in the same batch can share a timestamp. The mtime tests compute expected sets at runtime from observed timestamps (not hardcoded) and skip the "middle pivot" sub-assertion with a `println!` note when all 4 timestamps collide into one. See Task 5 for the exact code.

**Sleep policy.** Only two tests sleep: `e2e_filter_combo_all_seven` and `e2e_filter_pair_mtime_and_storage_class`, both for 1500 ms between batches to guarantee a time gap for their pivot. Per-filter tests do NOT sleep.

**`cargo fmt` and `cargo clippy` before every commit.** Auto-memory feedback applies. Every commit step below includes the fmt+clippy commands.

**Verification commands used throughout:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — must pass with no new e2e tests compiled in. |
| `cargo build --tests` | Non-gated test build — ensures no warnings leak. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build — compiles framework + filter tests. Does NOT hit S3. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint. |
| `cargo clippy --all-features -- -D warnings` | Non-gated lint. |
| `cargo fmt --check` | Formatting. |
| `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters -- --nocapture` | **Manual only.** Task 11 final verification against real S3. Does NOT run in CI. |

---

### Task 1: Add three helpers to `tests/common/mod.rs`

Adds the assertion helpers and the storage-class PutObject helper. This is a pure framework change — no new test file yet.

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add `StorageClass` to the `aws_sdk_s3::types` import**

Find the existing import block in `tests/common/mod.rs`:

```rust
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Delete,
    ObjectIdentifier, VersioningConfiguration,
};
```

Replace it with:

```rust
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, CreateBucketConfiguration, Delete,
    ObjectIdentifier, StorageClass, VersioningConfiguration,
};
```

- [ ] **Step 2: Add `put_object_with_storage_class` method**

Find the `put_object_with_tags` method inside the `impl TestHelper` block that starts with `// Object operations`. Immediately after `put_object_with_tags` (and before `put_object_full`), add:

```rust
    /// Upload an object with an explicit S3 StorageClass.
    ///
    /// Pass the storage class as the string form (e.g. `"STANDARD_IA"`,
    /// `"ONEZONE_IA"`, `"REDUCED_REDUNDANCY"`, `"INTELLIGENT_TIERING"`).
    /// The helper converts it via `StorageClass::from(&str)`, which is
    /// the same path `src/config/args/value_parser/storage_class.rs` uses.
    ///
    /// Used by filter e2e tests that need objects in multiple storage
    /// classes to exercise `--storage-class` filtering.
    pub async fn put_object_with_storage_class(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        storage_class: &str,
    ) {
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into())
            .storage_class(StorageClass::from(storage_class))
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to put object {key} with storage-class {storage_class}: {e}")
            });
    }
```

- [ ] **Step 3: Add `assert_json_keys_eq` helper**

At the very bottom of `tests/common/mod.rs`, after the `assert_key_order` function, append:

```rust
/// Parse NDJSON stdout from `s3ls --json` and assert the set of `Key`
/// fields equals `expected`. `label` is included in panic messages so
/// tests with multiple sub-assertions can identify which sub-case failed.
///
/// Panics if:
/// - any non-empty line fails to parse as JSON,
/// - any JSON line is missing the `Key` field (use
///   `assert_json_keys_or_prefixes_eq` if the output includes
///   `{"Prefix": ...}` entries — only happens with `--max-depth`),
/// - the actual set of keys does not equal `expected`.
///
/// Set-equality (not list-equality): catches both missing AND extra keys,
/// order-independent, so filter tests don't accidentally depend on sort.
pub fn assert_json_keys_eq(stdout: &str, expected: &[&str], label: &str) {
    use std::collections::HashSet;

    let actual: HashSet<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("[{label}] failed to parse JSON line: {line}\nerror: {e}")
            });
            v.get("Key")
                .and_then(|k| k.as_str())
                .unwrap_or_else(|| panic!("[{label}] JSON line missing `Key`: {line}"))
                .to_string()
        })
        .collect();

    let expected_set: HashSet<String> = expected.iter().map(|s| s.to_string()).collect();

    if actual != expected_set {
        let mut missing: Vec<&String> = expected_set.difference(&actual).collect();
        let mut extra: Vec<&String> = actual.difference(&expected_set).collect();
        missing.sort();
        extra.sort();
        panic!(
            "[{label}] key set mismatch\n  missing: {missing:?}\n  extra:   {extra:?}\n  stdout:\n{stdout}"
        );
    }
}
```

- [ ] **Step 4: Add `assert_json_keys_or_prefixes_eq` helper**

Append immediately after `assert_json_keys_eq`:

```rust
/// Like `assert_json_keys_eq`, but accepts JSON lines that have EITHER
/// a `Key` field (for object entries) OR a `Prefix` field (for
/// `CommonPrefix` entries under `--max-depth`). The actual set is the
/// union of both kinds, and `expected` is compared against that union.
///
/// Used only by `e2e_filter_max_depth_common_prefix_passthrough`, which
/// verifies that `--filter-include-regex` + `--max-depth` emits both
/// matching objects (`{"Key": "readme.csv", ...}`) AND common prefixes
/// (`{"Prefix": "logs/"}`) — the latter passes through every filter
/// unconditionally per `src/filters/mod.rs:37`.
pub fn assert_json_keys_or_prefixes_eq(stdout: &str, expected: &[&str], label: &str) {
    use std::collections::HashSet;

    let actual: HashSet<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("[{label}] failed to parse JSON line: {line}\nerror: {e}")
            });
            if let Some(k) = v.get("Key").and_then(|k| k.as_str()) {
                k.to_string()
            } else if let Some(p) = v.get("Prefix").and_then(|p| p.as_str()) {
                p.to_string()
            } else {
                panic!("[{label}] JSON line has neither `Key` nor `Prefix`: {line}");
            }
        })
        .collect();

    let expected_set: HashSet<String> = expected.iter().map(|s| s.to_string()).collect();

    if actual != expected_set {
        let mut missing: Vec<&String> = expected_set.difference(&actual).collect();
        let mut extra: Vec<&String> = actual.difference(&expected_set).collect();
        missing.sort();
        extra.sort();
        panic!(
            "[{label}] key/prefix set mismatch\n  missing: {missing:?}\n  extra:   {extra:?}\n  stdout:\n{stdout}"
        );
    }
}
```

- [ ] **Step 5: Verify non-gated build still passes**

Run: `cargo test`
Expected: all existing tests pass, no warnings. The new helpers are in `tests/common/mod.rs` which has `#![allow(dead_code)]`, so unused helpers won't trigger warnings.

- [ ] **Step 6: Verify gated build compiles**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. The existing `tests/e2e_listing.rs` still compiles against the new framework module. No new tests yet.

- [ ] **Step 7: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e test helpers for filter assertions

Adds three helpers to the e2e framework:
- assert_json_keys_eq: parse --json NDJSON and assert the set of Key
  fields equals an expected set (used by all filter tests).
- assert_json_keys_or_prefixes_eq: accepts both Key and Prefix entries
  (used by the --max-depth smoke test).
- put_object_with_storage_class: upload an object with an explicit S3
  storage class (used by the --storage-class filter test).

No production code changes. No Cargo.toml changes — serde_json is
already a direct production dependency.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Scaffold `tests/e2e_filters.rs` with the two regex-filter tests

Creates the new test file, gated with `#![cfg(e2e_test)]`, and lands the two regex tests (`e2e_filter_include_regex` and `e2e_filter_exclude_regex`) because they share a fixture.

**Files:**
- Create: `tests/e2e_filters.rs`

- [ ] **Step 1: Create `tests/e2e_filters.rs` with preamble and include-regex test**

Create the file with the following content:

```rust
#![cfg(e2e_test)]

//! Filter end-to-end tests.
//!
//! Covers every filter flag (`--filter-include-regex`,
//! `--filter-exclude-regex`, `--filter-smaller-size`,
//! `--filter-larger-size`, `--filter-mtime-before`,
//! `--filter-mtime-after`, `--storage-class`), their AND-composition,
//! and two orthogonal-flag interaction smoke tests (`--max-depth`
//! common-prefix passthrough, `--no-sort` streaming).
//!
//! Per-filter tests use a shared-fixture-within-a-test pattern: one
//! bucket per test, one fixture upload, multiple `run_s3ls` invocations
//! with labeled sub-assertions. This minimizes AWS round-trips while
//! keeping failure messages actionable via the `label` argument to
//! `assert_json_keys_eq`.
//!
//! Design: `docs/superpowers/specs/2026-04-11-step7-filter-e2e-tests-design.md`

mod common;

use common::*;

/// `--filter-include-regex`: include only keys matching a regex.
///
/// Fixture is 5 keys spanning two file types (csv, non-csv) so that
/// one small fixture supports match, no-match, anchor, and wildcard
/// sub-assertions.
#[tokio::test]
async fn e2e_filter_include_regex() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("report.csv".to_string(), b"a".to_vec()),
            ("data.csv".to_string(), b"a".to_vec()),
            ("summary.txt".to_string(), b"a".to_vec()),
            ("archive.tar.gz".to_string(), b"a".to_vec()),
            ("notes.md".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match `\.csv$`
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
        ]);
        assert!(
            output.status.success(),
            "s3ls failed: {}",
            output.stderr
        );
        assert_json_keys_eq(
            &output.stdout,
            &["report.csv", "data.csv"],
            "include-regex: match \\.csv$",
        );

        // Sub-assertion 2: no match `\.xlsx$`
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.xlsx$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "include-regex: no match \\.xlsx$");

        // Sub-assertion 3: anchor `^data`
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            "^data",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &["data.csv"], "include-regex: anchor ^data");

        // Sub-assertion 4: wildcard `.*` passes everything
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            ".*",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["report.csv", "data.csv", "summary.txt", "archive.tar.gz", "notes.md"],
            "include-regex: .* passes all",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_filter_exclude_regex` test to the same file**

Append the following to the end of `tests/e2e_filters.rs`:

```rust

/// `--filter-exclude-regex`: exclude keys matching a regex.
///
/// Fixture is identical to `e2e_filter_include_regex` — exclude-regex
/// is the logical inverse of include-regex over the same object set.
#[tokio::test]
async fn e2e_filter_exclude_regex() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("report.csv".to_string(), b"a".to_vec()),
            ("data.csv".to_string(), b"a".to_vec()),
            ("summary.txt".to_string(), b"a".to_vec()),
            ("archive.tar.gz".to_string(), b"a".to_vec()),
            ("notes.md".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match `\.csv$` — excludes 2, keeps 3
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["summary.txt", "archive.tar.gz", "notes.md"],
            "exclude-regex: match \\.csv$",
        );

        // Sub-assertion 2: no match `\.xlsx$` — keeps all 5
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\.xlsx$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["report.csv", "data.csv", "summary.txt", "archive.tar.gz", "notes.md"],
            "exclude-regex: no match \\.xlsx$",
        );

        // Sub-assertion 3: wildcard `.*` excludes everything
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            ".*",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "exclude-regex: .* excludes all");
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build stays clean**

Run: `cargo test`
Expected: all existing tests pass. `tests/e2e_filters.rs` is cfg-gated, so it compiles to an empty binary under non-gated builds.

- [ ] **Step 4: Verify gated build compiles**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. Both `e2e_filter_include_regex` and `e2e_filter_exclude_regex` are type-checked.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e tests for --filter-include-regex / --filter-exclude-regex

Two test functions covering the regex filter flags. Each uses a
5-key shared fixture (two csv files plus three non-csv) to exercise
match, no-match, anchoring, and wildcard sub-assertions with labeled
panic messages.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add `e2e_filter_smaller_size` and `e2e_filter_larger_size`

Two size-filter tests sharing a 4-key fixture (tiny/small/medium/large). Locks in strict `<` (smaller-size) vs inclusive `>=` (larger-size) semantics verified against source.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_smaller_size` to the file**

Append:

```rust

/// `--filter-smaller-size`: include only objects with `size < threshold`
/// (strict less-than, verified against `src/filters/smaller_size.rs:29`).
#[tokio::test]
async fn e2e_filter_smaller_size() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sizes chosen so 1000, 5000, and 1024 (1KiB) each bisect the set.
        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("small.bin".to_string(), vec![0u8; 1000]),
            ("medium.bin".to_string(), vec![0u8; 10_000]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match 5000 — tiny (10) and small (1000) pass
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "5000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["tiny.bin", "small.bin"],
            "smaller-size: match 5000",
        );

        // Sub-assertion 2: no match 1 — zero objects are smaller than 1 byte
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "1",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "smaller-size: no match 1");

        // Sub-assertion 3: strict-< boundary at 1000 — small.bin (exactly 1000)
        // is NOT strictly smaller than 1000, so only tiny.bin passes.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["tiny.bin"],
            "smaller-size: boundary 1000 strict",
        );

        // Sub-assertion 4: 1KiB = 1024 parses correctly. small.bin at 1000 is
        // strictly smaller than 1024, so both tiny and small pass.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "1KiB",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["tiny.bin", "small.bin"],
            "smaller-size: 1KiB parses",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_filter_larger_size` to the file**

Append:

```rust

/// `--filter-larger-size`: include only objects with `size >= threshold`
/// (inclusive `>=`, verified against `src/filters/larger_size.rs:29`).
#[tokio::test]
async fn e2e_filter_larger_size() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("small.bin".to_string(), vec![0u8; 1000]),
            ("medium.bin".to_string(), vec![0u8; 10_000]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match 5000 — medium (10_000) and large (100_000) pass
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "5000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["medium.bin", "large.bin"],
            "larger-size: match 5000",
        );

        // Sub-assertion 2: no match 1_000_000 — no object is that large
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "1000000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "larger-size: no match 1000000");

        // Sub-assertion 3: inclusive >= boundary at 10_000. medium.bin at
        // exactly 10_000 passes because the filter is inclusive.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "10000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["medium.bin", "large.bin"],
            "larger-size: boundary 10000 inclusive",
        );

        // Sub-assertion 4: 10KiB = 10240. medium.bin at 10_000 is less than
        // 10_240, so medium FAILS; only large (100_000) passes.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "10KiB",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["large.bin"],
            "larger-size: 10KiB parses",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e tests for --filter-smaller-size / --filter-larger-size

Two tests covering the size filters. Each uses a 4-key fixture
(tiny 10B, small 1000B, medium 10000B, large 100000B) and asserts
strict-< semantics for smaller-size vs inclusive->= for larger-size,
including the byte-unit suffix parsing (1KiB, 10KiB).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add `e2e_filter_mtime_before` and `e2e_filter_mtime_after`

These two tests share a helper that reads object LastModified values back from S3 and computes pivots + expected sets at runtime. Includes tie-handling for the fast-region case where all parallel uploads land in the same S3-second.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_mtime_before` to the file**

Append:

```rust

/// `--filter-mtime-before`: include only objects with `last_modified < pivot`
/// (strict `<`, verified against `src/filters/mtime_before.rs:27`).
///
/// Because S3 `LastModified` is second-precision, this test reads back
/// the actual timestamps from S3 after upload and computes both the
/// pivot and expected sets at runtime. If all 4 objects land in the
/// same S3-second, the "middle pivot" sub-assertion is skipped with a
/// logged note.
#[tokio::test]
async fn e2e_filter_mtime_before() {
    use chrono::{DateTime, Utc};
    use std::collections::BTreeSet;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = (1..=4)
            .map(|i| (format!("obj{i}"), vec![0u8; 100]))
            .collect();
        helper.put_objects_parallel(&bucket, fixture).await;

        // Read back actual LastModified values from S3.
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let pairs: Vec<(String, DateTime<Utc>)> = resp
            .contents()
            .iter()
            .map(|obj| {
                let key = obj.key().expect("object missing key").to_string();
                let lm = obj
                    .last_modified()
                    .expect("object missing last_modified");
                // aws_smithy_types::DateTime -> chrono::DateTime<Utc>
                let secs = lm.secs();
                let nanos = lm.subsec_nanos();
                let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
                    .expect("invalid timestamp from S3");
                (key, dt)
            })
            .collect();
        assert_eq!(pairs.len(), 4, "expected 4 uploaded objects, got {}", pairs.len());

        let distinct_times: BTreeSet<DateTime<Utc>> =
            pairs.iter().map(|(_, t)| *t).collect();
        let distinct_times: Vec<DateTime<Utc>> = distinct_times.into_iter().collect();

        let target = format!("s3://{bucket}/");

        // Helper: compute expected set for `last_modified < pivot`
        let expected_before = |pivot: DateTime<Utc>| -> Vec<String> {
            let mut out: Vec<String> = pairs
                .iter()
                .filter(|(_, t)| *t < pivot)
                .map(|(k, _)| k.clone())
                .collect();
            out.sort();
            out
        };

        // Sub-assertion 1: middle pivot (skipped if all four in one second)
        if distinct_times.len() >= 2 {
            let pivot = distinct_times[distinct_times.len() / 2];
            let expected = expected_before(pivot);
            let expected_refs: Vec<&str> = expected.iter().map(|s| s.as_str()).collect();
            let pivot_str = pivot.to_rfc3339();
            let output = TestHelper::run_s3ls(&[
                target.as_str(),
                "--recursive",
                "--json",
                "--filter-mtime-before",
                pivot_str.as_str(),
            ]);
            assert!(output.status.success(), "s3ls failed: {}", output.stderr);
            assert_json_keys_eq(
                &output.stdout,
                &expected_refs,
                "mtime-before: match (middle pivot)",
            );
        } else {
            println!(
                "mtime-before: skipped middle-pivot case — all 4 uploads share one S3-second"
            );
        }

        // Sub-assertion 2: earliest pivot. Strict-< against the minimum
        // observed time means NO object can pass.
        let earliest = distinct_times[0];
        let pivot_str = earliest.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-before",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[],
            "mtime-before: no match (earliest pivot)",
        );

        // Sub-assertion 3: boundary = max observed time. All objects strictly
        // earlier than the max are expected. If all 4 share one time, this
        // yields the empty set.
        let max_time = *distinct_times.last().unwrap();
        let expected = expected_before(max_time);
        let expected_refs: Vec<&str> = expected.iter().map(|s| s.as_str()).collect();
        let pivot_str = max_time.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-before",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &expected_refs,
            "mtime-before: boundary (max pivot)",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_filter_mtime_after` to the file**

Append:

```rust

/// `--filter-mtime-after`: include only objects with `last_modified >= pivot`
/// (inclusive `>=`, verified against `src/filters/mtime_after.rs:27`).
///
/// Tie-handling: same pattern as `e2e_filter_mtime_before` — the
/// "middle pivot" case is skipped if all 4 uploads collide into one
/// S3-second.
#[tokio::test]
async fn e2e_filter_mtime_after() {
    use chrono::{DateTime, Duration, Utc};
    use std::collections::BTreeSet;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = (1..=4)
            .map(|i| (format!("obj{i}"), vec![0u8; 100]))
            .collect();
        helper.put_objects_parallel(&bucket, fixture).await;

        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let pairs: Vec<(String, DateTime<Utc>)> = resp
            .contents()
            .iter()
            .map(|obj| {
                let key = obj.key().expect("object missing key").to_string();
                let lm = obj
                    .last_modified()
                    .expect("object missing last_modified");
                let secs = lm.secs();
                let nanos = lm.subsec_nanos();
                let dt = DateTime::<Utc>::from_timestamp(secs, nanos)
                    .expect("invalid timestamp from S3");
                (key, dt)
            })
            .collect();
        assert_eq!(pairs.len(), 4, "expected 4 uploaded objects, got {}", pairs.len());

        let distinct_times: BTreeSet<DateTime<Utc>> =
            pairs.iter().map(|(_, t)| *t).collect();
        let distinct_times: Vec<DateTime<Utc>> = distinct_times.into_iter().collect();

        let target = format!("s3://{bucket}/");

        let expected_after = |pivot: DateTime<Utc>| -> Vec<String> {
            let mut out: Vec<String> = pairs
                .iter()
                .filter(|(_, t)| *t >= pivot)
                .map(|(k, _)| k.clone())
                .collect();
            out.sort();
            out
        };

        // Sub-assertion 1: middle pivot (skipped if all in one second)
        if distinct_times.len() >= 2 {
            let pivot = distinct_times[distinct_times.len() / 2];
            let expected = expected_after(pivot);
            let expected_refs: Vec<&str> = expected.iter().map(|s| s.as_str()).collect();
            let pivot_str = pivot.to_rfc3339();
            let output = TestHelper::run_s3ls(&[
                target.as_str(),
                "--recursive",
                "--json",
                "--filter-mtime-after",
                pivot_str.as_str(),
            ]);
            assert!(output.status.success(), "s3ls failed: {}", output.stderr);
            assert_json_keys_eq(
                &output.stdout,
                &expected_refs,
                "mtime-after: match (middle pivot)",
            );
        } else {
            println!(
                "mtime-after: skipped middle-pivot case — all 4 uploads share one S3-second"
            );
        }

        // Sub-assertion 2: pivot 1 second beyond the max observed time —
        // inclusive `>=` cannot match anything.
        let after_max = *distinct_times.last().unwrap() + Duration::seconds(1);
        let pivot_str = after_max.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[],
            "mtime-after: no match (after max)",
        );

        // Sub-assertion 3: pivot = earliest observed time — inclusive `>=`
        // at the minimum always matches every object.
        let earliest = distinct_times[0];
        let expected = expected_after(earliest);
        let expected_refs: Vec<&str> = expected.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            expected.len(),
            4,
            "sanity: earliest pivot must match all 4 objects, got {}",
            expected.len()
        );
        let pivot_str = earliest.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &expected_refs,
            "mtime-after: boundary (earliest pivot inclusive)",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e tests for --filter-mtime-before / --filter-mtime-after

Two tests covering the mtime filters. Both upload 4 objects in
parallel, read back actual LastModified values from S3, and compute
pivots + expected sets at runtime (not hardcoded). The "middle pivot"
sub-assertion is skipped with a println when all 4 uploads collide
into a single S3-second, which keeps the suite stable in fast regions.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Add `e2e_filter_storage_class`

Uses the `put_object_with_storage_class` helper added in Task 1.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_storage_class` to the file**

Append:

```rust

/// `--storage-class`: include only objects in listed storage classes.
///
/// S3 omits the `StorageClass` field for STANDARD objects (returning
/// `None`), and `src/filters/storage_class.rs:33` treats `None` as
/// `"STANDARD"` — so `--storage-class STANDARD` still matches objects
/// uploaded with the default class. This test locks that in.
#[tokio::test]
async fn e2e_filter_storage_class() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // std.bin uses the default (no explicit class), so S3 records
        // StorageClass=None → filter treats as STANDARD.
        helper.put_object(&bucket, "std.bin", vec![0u8; 100]).await;
        helper
            .put_object_with_storage_class(&bucket, "rrs.bin", vec![0u8; 100], "REDUCED_REDUNDANCY")
            .await;
        helper
            .put_object_with_storage_class(&bucket, "ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;
        helper
            .put_object_with_storage_class(&bucket, "oz.bin", vec![0u8; 100], "ONEZONE_IA")
            .await;
        helper
            .put_object_with_storage_class(
                &bucket,
                "it.bin",
                vec![0u8; 100],
                "INTELLIGENT_TIERING",
            )
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: single class match
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "STANDARD_IA",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["ia.bin"],
            "storage-class: single STANDARD_IA",
        );

        // Sub-assertion 2: multiple classes (comma-separated)
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "STANDARD_IA,ONEZONE_IA",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["ia.bin", "oz.bin"],
            "storage-class: multiple",
        );

        // Sub-assertion 3: no object in GLACIER — empty result
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "GLACIER",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "storage-class: no match GLACIER");

        // Sub-assertion 4: STANDARD matches the None-StorageClass object
        // (std.bin). REDUCED_REDUNDANCY is NOT STANDARD, and the other three
        // are explicitly different classes.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["std.bin"],
            "storage-class: STANDARD matches None",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e test for --storage-class filter

Covers single-class match, comma-separated multi-class match, no-match
(GLACIER with no GLACIER objects), and the STANDARD / None-in-API
equivalence path. Uses the put_object_with_storage_class helper added
in Task 1 to create objects in REDUCED_REDUNDANCY, STANDARD_IA,
ONEZONE_IA, and INTELLIGENT_TIERING classes.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Add `e2e_filter_combo_all_seven`

The "all filters at once" test. Uses a 1.5-second sleep between the batch-1 and batch-2 uploads to guarantee a time pivot exists. Exactly one object (`target.csv`) must survive all seven filters.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_combo_all_seven` to the file**

Append:

```rust

/// All seven filters at once. Proves AND-composition across every filter
/// flag simultaneously. Exactly one object (`target.csv`) is designed
/// to survive the full filter chain.
///
/// Fixture strategy: two-batch upload with a 1.5s sleep between batches
/// so that `t_pivot = min(batch_2.last_modified)` is strictly greater
/// than `old.csv.last_modified`. S3 LastModified is second-precision,
/// so the 1.5s sleep is enough to push the next upload into the
/// following second even with clock skew.
#[tokio::test]
async fn e2e_filter_combo_all_seven() {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // --- Batch 1: the one object that will fail mtime-after. ---
        helper.put_object(&bucket, "old.csv", vec![0u8; 5000]).await;

        // Guarantee a 1-second gap between the two batches.
        sleep(Duration::from_millis(1500)).await;

        // --- Batch 2: five objects, four of which each fail exactly one filter. ---
        let batch2: Vec<(String, Vec<u8>)> = vec![
            ("target.csv".to_string(), vec![0u8; 5000]),     // survivor
            ("target.txt".to_string(), vec![0u8; 5000]),     // fails include-regex
            ("excluded.csv".to_string(), vec![0u8; 5000]),   // fails exclude-regex
            ("small.csv".to_string(), vec![0u8; 100]),       // fails larger-size
        ];
        helper.put_objects_parallel(&bucket, batch2).await;

        // ia.csv needs a distinct storage class, so it goes through the
        // single-object storage-class helper.
        helper
            .put_object_with_storage_class(&bucket, "ia.csv", vec![0u8; 5000], "STANDARD_IA")
            .await;

        // --- Read back LastModified for all 6 objects. ---
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let mut old_lm: Option<DateTime<Utc>> = None;
        let mut batch2_min: Option<DateTime<Utc>> = None;
        for obj in resp.contents() {
            let key = obj.key().expect("object missing key");
            let lm = obj.last_modified().expect("object missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            if key == "old.csv" {
                old_lm = Some(dt);
            } else {
                batch2_min = Some(match batch2_min {
                    None => dt,
                    Some(cur) => cur.min(dt),
                });
            }
        }
        let old_lm = old_lm.expect("old.csv not found in listing");
        let t_pivot = batch2_min.expect("batch 2 objects not found in listing");

        assert!(
            t_pivot > old_lm,
            "t_pivot ({t_pivot}) must be strictly after old.csv last-modified ({old_lm}) \
             — the 1.5s sleep should have guaranteed this. Clock skew > 1.5s?"
        );

        let mtime_after = t_pivot.to_rfc3339();
        let mtime_before = (t_pivot + ChronoDuration::hours(1)).to_rfc3339();

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
            "--filter-exclude-regex",
            "^excluded",
            "--filter-mtime-after",
            mtime_after.as_str(),
            "--filter-mtime-before",
            mtime_before.as_str(),
            "--filter-larger-size",
            "1000",
            "--filter-smaller-size",
            "10000",
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["target.csv"],
            "combo all seven: exactly target.csv survives",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e test combining all seven filters at once

Uploads 6 objects in two batches (separated by a 1.5s sleep to
guarantee a second-level time pivot), runs s3ls with every filter
flag set, and asserts exactly one object (target.csv) survives the
full AND-composition.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Add the two no-sleep pair tests (`regex_and_size`, `include_and_exclude_regex`)

Two simple combination tests that don't need time pivots. Grouped into one task because each is small.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_pair_regex_and_size`**

Append:

```rust

/// Regex × size composition: `.csv AND >= 1000 bytes`.
///
/// Fixture bisects cleanly: csv vs txt × small vs big, yielding exactly
/// one survivor.
#[tokio::test]
async fn e2e_filter_pair_regex_and_size() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("a.csv".to_string(), vec![0u8; 100]),
            ("b.csv".to_string(), vec![0u8; 2000]),
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 2000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
            "--filter-larger-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["b.csv"],
            "pair regex+size: b.csv only",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_filter_pair_include_and_exclude_regex`**

Append:

```rust

/// Include-regex × exclude-regex composition: `.csv AND NOT _tmp`.
///
/// Proves the two regex filters compose correctly — exclude is applied
/// to the survivors of include, not to the original set.
#[tokio::test]
async fn e2e_filter_pair_include_and_exclude_regex() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("report.csv".to_string(), b"a".to_vec()),
            ("report_tmp.csv".to_string(), b"a".to_vec()),
            ("data.csv".to_string(), b"a".to_vec()),
            ("notes.txt".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
            "--filter-exclude-regex",
            "_tmp",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["report.csv", "data.csv"],
            "pair include+exclude: .csv minus _tmp",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e pair tests for regex+size and include+exclude-regex

Two simple combination tests proving AND-composition between
(include-regex, larger-size) and (include-regex, exclude-regex).
Neither test needs a time pivot, so neither sleeps.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Add the two remaining pair tests (`mtime_and_storage_class`, `exclude_and_size_range`)

One of these needs a 1.5s sleep for the mtime pivot; the other does not.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_pair_mtime_and_storage_class`**

Append:

```rust

/// Mtime × storage-class composition: `mtime-after pivot AND STANDARD`.
///
/// Two-batch upload with a 1.5s sleep between batches. Each batch
/// contains one STANDARD and one STANDARD_IA object. The only survivor
/// is the batch-2 STANDARD object.
#[tokio::test]
async fn e2e_filter_pair_mtime_and_storage_class() {
    use chrono::{DateTime, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Batch 1
        helper.put_object(&bucket, "old_std.bin", vec![0u8; 100]).await;
        helper
            .put_object_with_storage_class(&bucket, "old_ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;

        sleep(Duration::from_millis(1500)).await;

        // Batch 2
        helper.put_object(&bucket, "new_std.bin", vec![0u8; 100]).await;
        helper
            .put_object_with_storage_class(&bucket, "new_ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;

        // Read back LastModified for all 4 objects.
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let mut old_max: Option<DateTime<Utc>> = None;
        let mut new_min: Option<DateTime<Utc>> = None;
        for obj in resp.contents() {
            let key = obj.key().expect("object missing key");
            let lm = obj.last_modified().expect("object missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            if key.starts_with("old_") {
                old_max = Some(match old_max {
                    None => dt,
                    Some(cur) => cur.max(dt),
                });
            } else {
                new_min = Some(match new_min {
                    None => dt,
                    Some(cur) => cur.min(dt),
                });
            }
        }
        let old_max = old_max.expect("batch 1 objects not found");
        let new_min = new_min.expect("batch 2 objects not found");

        assert!(
            new_min > old_max,
            "new_min ({new_min}) must be strictly after old_max ({old_max}) \
             — 1.5s sleep should have guaranteed this. Clock skew > 1.5s?"
        );

        let mtime_after = new_min.to_rfc3339();
        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            mtime_after.as_str(),
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["new_std.bin"],
            "pair mtime+storage-class: new_std.bin only",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_filter_pair_exclude_and_size_range`**

Append:

```rust

/// Exclude × size-range composition: `NOT .tmp AND >= 1000 AND < 4000`.
///
/// Fixture is designed so exactly one object (`keep_mid.bin`) satisfies
/// all three constraints at once.
#[tokio::test]
async fn e2e_filter_pair_exclude_and_size_range() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("keep_small.bin".to_string(), vec![0u8; 500]),
            ("keep_big.bin".to_string(), vec![0u8; 5000]),
            ("keep_mid.bin".to_string(), vec![0u8; 2000]),
            ("skip_mid.tmp".to_string(), vec![0u8; 2000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\.tmp$",
            "--filter-larger-size",
            "1000",
            "--filter-smaller-size",
            "4000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["keep_mid.bin"],
            "pair exclude+size-range: keep_mid.bin only",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e pair tests for mtime+storage-class and exclude+size-range

Two more combination tests. mtime+storage-class uses a 1.5s sleep
between batches for the time pivot; exclude+size-range is fixture-only
with no sleep.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Add orthogonal-flag smoke test `e2e_filter_max_depth_common_prefix_passthrough`

Locks in the `CommonPrefix` passthrough behavior from `src/filters/mod.rs:37`. This test uses `assert_json_keys_or_prefixes_eq` because `--max-depth` emits `{"Prefix": "logs/"}` entries alongside object entries.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_max_depth_common_prefix_passthrough`**

Append:

```rust

/// Locks in `CommonPrefix` passthrough under `--filter-include-regex`
/// + `--max-depth`.
///
/// `FilterChain::matches` at `src/filters/mod.rs:37` short-circuits
/// `CommonPrefix` entries to always pass every filter. Without this
/// short-circuit, `--filter-include-regex '\.csv$'` would drop
/// `{"Prefix": "logs/"}` (the prefix doesn't match `\.csv$`), which
/// would break depth-limited recursion. This test hits that exact
/// interaction with real S3 listing + `--max-depth 1`.
///
/// The expected output includes both a `{"Key": "readme.csv", ...}`
/// object entry and a `{"Prefix": "logs/"}` common-prefix entry, so
/// this is the one test that uses `assert_json_keys_or_prefixes_eq`.
/// JSON shape confirmed against `src/aggregate.rs:514`.
#[tokio::test]
async fn e2e_filter_max_depth_common_prefix_passthrough() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("logs/2025/a.log".to_string(), b"a".to_vec()),
            ("logs/2025/b.log".to_string(), b"a".to_vec()),
            ("logs/2026/a.log".to_string(), b"a".to_vec()),
            ("readme.csv".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "1",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected:
        // - readme.csv matches the regex → {"Key": "readme.csv", ...}
        // - logs/ is a CommonPrefix at depth 1 → {"Prefix": "logs/"}
        //   passes through the filter because CommonPrefix is exempt.
        assert_json_keys_or_prefixes_eq(
            &output.stdout,
            &["readme.csv", "logs/"],
            "max-depth: CommonPrefix passthrough under include-regex",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

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
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e smoke for CommonPrefix passthrough under --max-depth

Locks in the FilterChain short-circuit at src/filters/mod.rs:37 —
CommonPrefix entries always pass every filter. Without this the
combination of --filter-include-regex and --max-depth would drop
PRE entries, breaking depth-limited recursion. Uses the
assert_json_keys_or_prefixes_eq helper because the expected output
mixes {"Key": ...} and {"Prefix": ...} entries.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Add orthogonal-flag smoke test `e2e_filter_no_sort_streaming`

Locks in that `--no-sort` (streaming mode) still applies filters correctly.

**Files:**
- Modify: `tests/e2e_filters.rs`

- [ ] **Step 1: Append `e2e_filter_no_sort_streaming`**

Append:

```rust

/// Locks in that `--no-sort` still applies filters.
///
/// The streaming path bypasses the sort buffer. This test confirms
/// that the filter chain still runs — a future refactor that moved
/// filtering into the post-sort step would regress this.
///
/// Asserted as a set (order-independent) because `--no-sort` emits
/// results in arrival order, which is non-deterministic across runs.
#[tokio::test]
async fn e2e_filter_no_sort_streaming() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // a1..a6 with sizes 1000..6000 (step 1000).
        let fixture: Vec<(String, Vec<u8>)> = (1..=6)
            .map(|i| (format!("a{i}.bin"), vec![0u8; (i * 1000) as usize]))
            .collect();
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // --filter-larger-size 3000 → a3 (3000) through a6 (6000) pass.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--no-sort",
            "--json",
            "--filter-larger-size",
            "3000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["a3.bin", "a4.bin", "a5.bin", "a6.bin"],
            "no-sort streaming: larger-size 3000",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass.

- [ ] **Step 3: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. This completes the 14-test file.

- [ ] **Step 4: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_filters.rs
git commit -m "$(cat <<'EOF'
test(step7): add e2e smoke for --no-sort + filter composition

Locks in that streaming mode (--no-sort) still runs the filter
chain. Fixture is 6 objects with sizes 1000..6000; larger-size 3000
must still keep the largest 4 even though results arrive unsorted.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Final verification against real S3

Manual-only verification. Does **not** run in CI. Requires a configured `s3ls-e2e-test` AWS profile per `tests/README.md`.

**Files:** (none modified)

- [ ] **Step 1: Confirm AWS profile exists**

Run: `aws configure list --profile s3ls-e2e-test`
Expected: shows a configured profile with a region and credentials source. If missing, follow `tests/README.md` to set it up before proceeding.

- [ ] **Step 2: Run the filter suite against real S3**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters -- --nocapture`
Expected: all 14 tests pass. Each creates a fresh `s3ls-e2e-{uuid}` bucket, uploads a small fixture, runs s3ls with the filter under test, asserts, and cleans up. Expected runtime: on the order of a minute or two depending on region latency and parallel test scheduling. Watch for any test that hits the `e2e_timeout!` 60-second hard limit.

- [ ] **Step 3: Run the full e2e suite to confirm no cross-suite regressions**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture`
Expected: both `e2e_listing` (step 6) and `e2e_filters` (step 7) pass.

- [ ] **Step 4: Confirm non-gated `cargo test` still clean**

Run: `cargo test`
Expected: all existing unit tests pass. Neither e2e file compiles under non-gated builds.

- [ ] **Step 5: Check for any leaked buckets (sanity)**

Run:
```bash
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text
```
Expected: empty output. If non-empty, a test panicked before `_guard.cleanup().await` could run — follow the cleanup instructions in `tests/README.md`.

- [ ] **Step 6: No commit for this task**

Verification only. If any of the steps failed, fix the underlying issue (likely in the test that failed) and re-run before proceeding.

---

## Notes for the executor

- **Each task produces one commit** except Task 11 (verification only, no commit). Expect 10 commits on the branch.
- **The file grows monotonically.** Every task after Task 2 appends to `tests/e2e_filters.rs`. If you want to reorder tests within the file, do so only in a follow-up — the plan expects the append-only sequence for diff clarity.
- **`rustfmt` may reflow the `run_s3ls` argument slices** into one-per-line arrays. That's fine and expected; the commit diff will show the reflow only on the first `cargo fmt` run.
- **If a test fails in Task 11**, the fix is almost always in the test code (wrong expected set, mis-sized fixture, sleep too short for a slow network). Only edit the framework (`tests/common/mod.rs`) if the helper itself is broken.
- **Do NOT add `#[ignore]` to any test in this file** even if one is flaky. Flakiness should be diagnosed (usually via `--nocapture` output) and fixed at the root. The step 6 design explicitly declines `#[ignore]` as a flake-hiding tool.
