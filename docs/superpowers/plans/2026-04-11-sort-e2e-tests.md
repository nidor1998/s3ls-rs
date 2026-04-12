# Sort E2E Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 11 end-to-end tests for s3ls sort functionality (every sort field in both directions, multi-column tiebreak, `--no-sort`, versioning secondary-sort, and bucket listing sort) in a new `tests/e2e_sort.rs` file, using 1 new helper in `tests/common/mod.rs`. All assertions use `--json` output.

**Architecture:** Gated with `#![cfg(e2e_test)]`. One new file holds all 11 tests. Most tests run `s3ls` once and assert the exact sequence of `Key` fields via a new `assert_json_keys_order_eq` helper. Date-dependent tests use `sleep(1500ms)` between sequential uploads to guarantee distinct S3-second timestamps. Bucket listing tests create 2 test buckets with deterministic alphabetical prefixes (`s3ls-e2e-a-*`, `s3ls-e2e-z-*`) and assert relative position.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (production dep), `serde_json` (production dep), `uuid` (existing dev-dep), existing framework helpers from step 6 + step 7.

**Depends on:** Step 6 framework. The new file is a sibling of `tests/e2e_filters.rs`, `tests/e2e_filters_versioned.rs`, `tests/e2e_display.rs`, and `tests/e2e_listing.rs`.

**Spec:** `docs/superpowers/specs/2026-04-11-sort-e2e-tests-design.md`

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- Auto-memory feedback: **logging via `tracing` / `tracing-subscriber`**, not `log` directly. (Not relevant — no logging code — but noted.)

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/e2e_sort.rs` | 11 `#[tokio::test]` functions covering sort functionality. Gated with `#![cfg(e2e_test)]`. |

**Modified files:**

| Path | Change |
|---|---|
| `tests/common/mod.rs` | Add `assert_json_keys_order_eq` free function. |

**No changes to `src/`.** No `Cargo.toml` changes — `serde_json`, `tokio`, `uuid` are all existing dependencies.

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

**Sort CLI args reference:**
- `--sort key` / `--sort size` / `--sort date` / `--sort bucket`
- `--sort size,key` (comma-separated multi-column, `value_delimiter = ','` at `src/config/args/mod.rs:223`)
- `--reverse` (flips the final comparator)
- `--no-sort` (`conflicts_with_all = ["sort", "reverse"]` — cannot be combined with those flags)
- Default sort when `--sort` not given: `key` (objects) / `bucket` (bucket listing)
- Versioning auto-append: `--all-versions --sort key` → auto-appends `date` as secondary sort

**JSON-only assertions.** Every test uses `--json`. The new `assert_json_keys_order_eq` helper is the primary assertion mechanism — it verifies the exact ordered sequence of `Key` fields in NDJSON output. Test 8 (`--no-sort`) uses the existing `assert_json_keys_eq` for set equality.

**Date sort tests require sleeps.** Tests 5, 6, and 9 upload objects sequentially with `tokio::time::sleep(Duration::from_millis(1500))` between each to guarantee distinct S3-second `LastModified` timestamps. This matches the 1500ms precedent from step 7 (`e2e_filter_combo_all_seven`) and the versioning plan.

**Bucket listing tests (10-11) create 2 buckets each.** Both buckets get guards and both guards are cleaned up. The bucket names use `s3ls-e2e-a-{uuid}` and `s3ls-e2e-z-{uuid}` to guarantee deterministic alphabetical ordering. The test only asserts RELATIVE position of these two in the output (the account may have other buckets between them).

**Running against real S3 is NOT part of this plan.** All verification in S-Tasks 1-7 is compile + clippy + fmt only. S-Task 8 is manual real-S3 verification.

**Verification commands used throughout:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — new file compiles to empty. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build — compiles framework + sort tests. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint. |
| `cargo clippy --all-features -- -D warnings` | Non-gated lint. |
| `cargo fmt --check` | Formatting. |

---

### S-Task 1: Add `assert_json_keys_order_eq` helper to `tests/common/mod.rs`

Pure framework plumbing. No new test file yet.

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add `assert_json_keys_order_eq` free function**

At the bottom of `tests/common/mod.rs`, after the existing assertion helpers (the last one is currently `assert_summary_present_json` from the display plan, or `assert_json_version_shapes_eq` / display helpers — whichever is last), append:

```rust
/// Parse NDJSON stdout from `s3ls --json` and assert the sequence of
/// `Key` fields (in order) equals `expected`. Unlike
/// `assert_json_keys_eq` which does set comparison, this helper
/// verifies exact ordering — the primary assertion for sort tests.
///
/// Duplicates in `expected` are handled naturally: a key that appears
/// twice in the expected slice must also appear twice in the output,
/// in the same positions. This makes the helper suitable for
/// versioned-listing tests where the same key appears multiple times.
///
/// Lines that parse as JSON but have no `Key` field (e.g.,
/// `{"Prefix": ...}` or `{"Summary": ...}`) are skipped — the ordering
/// check applies only to object/delete-marker rows.
///
/// Panics if:
/// - any non-empty line fails to parse as JSON,
/// - the resulting sequence of `Key` values does not equal `expected`.
pub fn assert_json_keys_order_eq(stdout: &str, expected: &[&str], label: &str) {
    let actual: Vec<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let v: serde_json::Value = serde_json::from_str(line).unwrap_or_else(|e| {
                panic!("[{label}] failed to parse JSON line: {line}\nerror: {e}")
            });
            v.get("Key").and_then(|k| k.as_str()).map(|s| s.to_string())
        })
        .collect();

    let expected_owned: Vec<String> = expected.iter().map(|s| s.to_string()).collect();

    if actual != expected_owned {
        panic!(
            "[{label}] key order mismatch\n  expected: {expected_owned:?}\n  actual:   {actual:?}\n  stdout:\n{stdout}"
        );
    }
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass, no warnings.

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
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(sort): add assert_json_keys_order_eq helper for sequence assertions

Adds a helper that parses NDJSON from s3ls --json and asserts the
sequence of Key fields matches an expected slice. Unlike
assert_json_keys_eq (set comparison), this verifies exact ordering
— the primary assertion for sort tests. Handles duplicate keys for
versioned-listing tests.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### S-Task 2: Scaffold `tests/e2e_sort.rs` with key sort tests (Tests 1 and 2)

Creates the new file with the preamble and the two key-sort tests.

**Files:**
- Create: `tests/e2e_sort.rs`

- [ ] **Step 1: Create `tests/e2e_sort.rs` with preamble and Test 1**

Create the file with:

```rust
#![cfg(e2e_test)]

//! Sort end-to-end tests.
//!
//! Covers s3ls sort functionality in JSON mode: every sort field
//! (`key`, `size`, `date` for objects; `bucket` for bucket listings),
//! both directions (`--reverse`), multi-column with tiebreak,
//! `--no-sort` streaming, and the `--all-versions` auto-appended
//! secondary date sort.
//!
//! All assertions use `--json` output and `assert_json_keys_order_eq`
//! (sequence comparison) or `assert_json_keys_eq` (set comparison for
//! `--no-sort`).
//!
//! Design: `docs/superpowers/specs/2026-04-11-sort-e2e-tests-design.md`

mod common;

use common::*;

/// Default and explicit `--sort key`: objects sorted alphabetically by key.
///
/// Fixture keys are non-alphabetical (`c, a, b`) so the sort is
/// observable. Two sub-assertions verify both explicit `--sort key` and
/// the implicit default produce the same ascending-key order.
#[tokio::test]
async fn e2e_sort_key_asc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("c.txt".to_string(), vec![0u8; 100]),
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 100]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: explicit --sort key
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "key",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "sort key asc: explicit --sort key",
        );

        // Sub-assertion 2: no --sort (default is key ascending)
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "sort key asc: default (no --sort)",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_sort_key_desc` test**

Append:

```rust

/// `--sort key --reverse`: objects sorted in reverse alphabetical order.
#[tokio::test]
async fn e2e_sort_key_desc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("c.txt".to_string(), vec![0u8; 100]),
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 100]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "key",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["c.txt", "b.txt", "a.txt"],
            "sort key desc",
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
git add tests/e2e_sort.rs
git commit -m "$(cat <<'EOF'
test(sort): add e2e tests for --sort key ascending and descending

Two tests covering key sort. key_asc verifies both explicit
--sort key and the default (no --sort) produce alphabetical order.
key_desc verifies --reverse produces reverse alphabetical order.
Fixture keys are non-alphabetical (c, a, b) so the sort effect
is observable.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### S-Task 3: Add size sort tests (Tests 3 and 4)

**Files:**
- Modify: `tests/e2e_sort.rs`

- [ ] **Step 1: Append `e2e_sort_size_asc` test**

Append:

```rust

/// `--sort size`: objects sorted by size ascending. Fixture keys are
/// non-alphabetical and sizes are distinct so sort-by-size produces
/// a different order than sort-by-key.
#[tokio::test]
async fn e2e_sort_size_asc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("medium.bin".to_string(), vec![0u8; 5000]),
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
            ("small.bin".to_string(), vec![0u8; 1000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "size",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["tiny.bin", "small.bin", "medium.bin", "large.bin"],
            "sort size asc",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_sort_size_desc` test**

Append:

```rust

/// `--sort size --reverse`: objects sorted by size descending.
#[tokio::test]
async fn e2e_sort_size_desc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("medium.bin".to_string(), vec![0u8; 5000]),
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
            ("small.bin".to_string(), vec![0u8; 1000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "size",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["large.bin", "medium.bin", "small.bin", "tiny.bin"],
            "sort size desc",
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
git add tests/e2e_sort.rs
git commit -m "$(cat <<'EOF'
test(sort): add e2e tests for --sort size ascending and descending

Two tests covering size sort. Fixture has 4 objects with distinct
sizes (10, 1000, 5000, 100000) and non-alphabetical keys so
sort-by-size produces a clearly different order than sort-by-key.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### S-Task 4: Add date sort tests (Tests 5 and 6)

These tests use sequential uploads with `sleep(1500ms)` between each to guarantee distinct S3-second timestamps. Upload order is non-alphabetical.

**Files:**
- Modify: `tests/e2e_sort.rs`

- [ ] **Step 1: Append `e2e_sort_date_asc` test**

Append:

```rust

/// `--sort date`: objects sorted by LastModified ascending (oldest first).
///
/// Fixture uploads objects sequentially with 1.5s sleeps between each
/// to guarantee distinct S3-second timestamps. Upload order `c, a, b`
/// is deliberately non-alphabetical so `--sort date` produces a
/// different order than the default key sort.
#[tokio::test]
async fn e2e_sort_date_asc() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload in non-alphabetical order: c, a, b.
        // Sleeps guarantee distinct LastModified seconds.
        helper.put_object(&bucket, "c.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "a.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "b.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "date",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Oldest first = upload order: c, a, b.
        assert_json_keys_order_eq(
            &output.stdout,
            &["c.txt", "a.txt", "b.txt"],
            "sort date asc",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_sort_date_desc` test**

Append:

```rust

/// `--sort date --reverse`: objects sorted by LastModified descending
/// (newest first).
#[tokio::test]
async fn e2e_sort_date_desc() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        helper.put_object(&bucket, "c.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "a.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "b.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "date",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Newest first = reverse upload order: b, a, c.
        assert_json_keys_order_eq(
            &output.stdout,
            &["b.txt", "a.txt", "c.txt"],
            "sort date desc",
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
git add tests/e2e_sort.rs
git commit -m "$(cat <<'EOF'
test(sort): add e2e tests for --sort date ascending and descending

Two tests covering date sort. Fixture uploads 3 objects sequentially
with 1.5s sleeps between each (c, a, b) to guarantee distinct
S3-second timestamps. --sort date produces upload order (oldest
first); --sort date --reverse produces reverse upload order.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### S-Task 5: Add multi-column tiebreak, no-sort, and versioning tests (Tests 7, 8, 9)

Three conceptually distinct tests grouped in one task because each is independent.

**Files:**
- Modify: `tests/e2e_sort.rs`

- [ ] **Step 1: Append `e2e_sort_size_key_tiebreak` test**

Append:

```rust

/// `--sort size,key`: multi-column sort where two objects tie on size
/// (5000 bytes each) and the secondary key sort disambiguates.
///
/// `a.csv` must appear before `b.csv` in the result even though `b.csv`
/// was uploaded first — both have size 5000, so the primary sort ties
/// them, and the secondary `key` sort produces alphabetical order.
#[tokio::test]
async fn e2e_sort_size_key_tiebreak() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("z.txt".to_string(), vec![0u8; 100]),
            ("b.csv".to_string(), vec![0u8; 5000]),
            ("a.csv".to_string(), vec![0u8; 5000]),
            ("m.txt".to_string(), vec![0u8; 10000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "size,key",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["z.txt", "a.csv", "b.csv", "m.txt"],
            "sort size,key tiebreak",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_sort_no_sort` test**

Append:

```rust

/// `--no-sort`: results stream in arbitrary order. This test asserts
/// only set equality (all expected keys are present), NOT ordering.
///
/// `--no-sort` has `conflicts_with_all = ["sort", "reverse"]` at
/// `src/config/args/mod.rs:234`, so it cannot be combined with
/// `--sort` or `--reverse` — clap rejects it at parse time.
///
/// Order is intentionally not asserted per commit 3e6c4fb
/// ("clarify --no-sort produces arbitrary order").
#[tokio::test]
async fn e2e_sort_no_sort() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 100]),
            ("c.txt".to_string(), vec![0u8; 100]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--no-sort",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Set equality only — order is intentionally not asserted.
        assert_json_keys_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "no-sort: set equality",
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 3: Append `e2e_sort_versioned_secondary_date` test**

Append:

```rust

/// `--all-versions --sort key`: auto-appends `date` as secondary sort
/// (per `src/config/args/mod.rs:759-761`). Two keys with 2 versions
/// each, uploaded sequentially with 1.5s sleeps. The result must show
/// key ascending (apple before banana) and within each key, date
/// ascending (v1 before v2).
#[tokio::test]
async fn e2e_sort_versioned_secondary_date() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // apple v1 → apple v2 → banana v1 → banana v2
        // Each upload separated by 1.5s to guarantee distinct LastModified.
        helper.put_object(&bucket, "apple.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "apple.txt", vec![0u8; 200]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "banana.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "banana.txt", vec![0u8; 200]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--sort",
            "key",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Assertion 1: key sequence is apple, apple, banana, banana.
        assert_json_keys_order_eq(
            &output.stdout,
            &["apple.txt", "apple.txt", "banana.txt", "banana.txt"],
            "versioned secondary date: key order",
        );

        // Assertion 2: within each Key group, LastModified is
        // non-decreasing. This proves the auto-appended `date`
        // secondary sort is actually applied.
        let rows: Vec<(String, String)> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|line| {
                let v: serde_json::Value = serde_json::from_str(line).ok()?;
                let key = v.get("Key")?.as_str()?.to_string();
                let lm = v.get("LastModified")?.as_str()?.to_string();
                Some((key, lm))
            })
            .collect();

        let mut prev_key: Option<&str> = None;
        let mut prev_lm: Option<&str> = None;
        for (k, lm) in &rows {
            if Some(k.as_str()) == prev_key {
                assert!(
                    lm.as_str() >= prev_lm.unwrap(),
                    "versioned secondary sort: within key {k:?}, LastModified not monotonic: {:?} -> {lm:?}",
                    prev_lm.unwrap()
                );
            }
            prev_key = Some(k.as_str());
            prev_lm = Some(lm.as_str());
        }
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 4: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 5: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build.

- [ ] **Step 6: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 7: Commit**

```bash
git add tests/e2e_sort.rs
git commit -m "$(cat <<'EOF'
test(sort): add e2e tests for multi-column tiebreak, --no-sort, and versioning secondary sort

Three tests: size,key tiebreak (proves secondary key sort
disambiguates when sizes tie), --no-sort (set-equality only,
order intentionally not asserted), and --all-versions --sort key
(proves the auto-appended date secondary sort produces chronological
version ordering within each key group).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### S-Task 6: Add bucket listing sort tests (Tests 10 and 11)

Both tests create 2 test buckets with deterministic alphabetical prefixes and assert their relative positions.

**Files:**
- Modify: `tests/e2e_sort.rs`

- [ ] **Step 1: Append `e2e_sort_bucket_listing_asc` test**

Append:

```rust

/// Bucket listing `--sort bucket`: two test buckets with deterministic
/// name prefixes (`s3ls-e2e-a-*` and `s3ls-e2e-z-*`) are created, and
/// the test asserts the `a-` bucket appears before the `z-` bucket in
/// the listing. Assertions are scoped to these two test buckets because
/// the account may have other buckets.
#[tokio::test]
async fn e2e_sort_bucket_listing_asc() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&["--json", "--sort", "bucket"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Find positions of our two test buckets in the NDJSON output.
        let mut pos_a: Option<usize> = None;
        let mut pos_z: Option<usize> = None;
        for (i, line) in output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
        {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(name) = v.get("Name").and_then(|n| n.as_str()) {
                    if name == bucket_a {
                        pos_a = Some(i);
                    } else if name == bucket_z {
                        pos_z = Some(i);
                    }
                }
            }
        }

        let pos_a = pos_a.unwrap_or_else(|| {
            panic!("bucket listing asc: test bucket {bucket_a} not found in output")
        });
        let pos_z = pos_z.unwrap_or_else(|| {
            panic!("bucket listing asc: test bucket {bucket_z} not found in output")
        });

        assert!(
            pos_a < pos_z,
            "bucket listing asc: expected {bucket_a} (pos {pos_a}) before {bucket_z} (pos {pos_z})"
        );
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}
```

- [ ] **Step 2: Append `e2e_sort_bucket_listing_desc` test**

Append:

```rust

/// Bucket listing `--sort bucket --reverse`: the `z-` test bucket must
/// appear before the `a-` test bucket.
#[tokio::test]
async fn e2e_sort_bucket_listing_desc() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&[
            "--json",
            "--sort",
            "bucket",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        let mut pos_a: Option<usize> = None;
        let mut pos_z: Option<usize> = None;
        for (i, line) in output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
        {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(name) = v.get("Name").and_then(|n| n.as_str()) {
                    if name == bucket_a {
                        pos_a = Some(i);
                    } else if name == bucket_z {
                        pos_z = Some(i);
                    }
                }
            }
        }

        let pos_a = pos_a.unwrap_or_else(|| {
            panic!("bucket listing desc: test bucket {bucket_a} not found in output")
        });
        let pos_z = pos_z.unwrap_or_else(|| {
            panic!("bucket listing desc: test bucket {bucket_z} not found in output")
        });

        assert!(
            pos_z < pos_a,
            "bucket listing desc: expected {bucket_z} (pos {pos_z}) before {bucket_a} (pos {pos_a})"
        );
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}
```

- [ ] **Step 3: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 4: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. This completes the 11-test file.

- [ ] **Step 5: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_sort.rs
git commit -m "$(cat <<'EOF'
test(sort): add e2e tests for bucket listing sort ascending and descending

Two tests covering bucket listing sort. Each creates two test
buckets with deterministic alphabetical prefixes (s3ls-e2e-a-*
and s3ls-e2e-z-*) and asserts their relative position in the
output. --sort bucket puts a- before z-; --sort bucket --reverse
flips the order.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### S-Task 7: Final non-gated verification

Quick sanity check that the full non-gated `cargo test` still passes with the new file compiling to empty.

**Files:** (none modified)

- [ ] **Step 1: Run full non-gated test suite**

Run: `cargo test`
Expected: all existing tests pass. `tests/e2e_sort.rs` compiles to an empty binary with 0 tests.

- [ ] **Step 2: Run full gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: all e2e test files compile cleanly.

- [ ] **Step 3: Count tests**

Run: `grep -c '#\[tokio::test\]' tests/e2e_sort.rs`
Expected: `11`

- [ ] **Step 4: No commit for this step**

Verification only.

---

### S-Task 8: Manual verification against real S3

Manual-only verification. Does **not** run in CI. Requires a configured `s3ls-e2e-test` AWS profile.

**Files:** (none modified)

- [ ] **Step 1: Confirm AWS profile**

Run: `aws configure list --profile s3ls-e2e-test`
Expected: configured profile with region and credentials.

- [ ] **Step 2: Run the sort suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_sort -- --nocapture`
Expected: all 11 tests pass. Date-sort and versioning tests take ~3-5 seconds each due to sleeps; size/key/no-sort/bucket tests complete quickly.

- [ ] **Step 3: Run the full e2e suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture`
Expected: `e2e_listing`, `e2e_filters`, `e2e_filters_versioned`, `e2e_display`, and `e2e_sort` all pass.

- [ ] **Step 4: Confirm non-gated `cargo test` still clean**

Run: `cargo test`
Expected: all existing unit + bin tests pass.

- [ ] **Step 5: Check for leaked buckets**

Run:
```bash
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text
```
Expected: empty output. Tests 10-11 create 2 buckets each; if they panicked before cleanup, those buckets may remain. Follow `tests/README.md` for manual cleanup.

- [ ] **Step 6: No commit for this task**

Verification only.

---

## Notes for the executor

- **Each task produces one commit** except S-Tasks 7 and 8 (verification only). Expect 6 commits on the branch.
- **The file `tests/e2e_sort.rs` grows monotonically** — every task after S-Task 2 appends.
- **`rustfmt` may reflow argument slices** — expected, does not change semantics.
- **Tests 5, 6, 9 are the only tests that sleep.** Tests 1-4, 7, 8 use parallel uploads and run quickly.
- **Tests 10-11 each create 2 buckets and hold 2 guards.** Both guards must be cleaned up. Cleanup calls are `_guard_a.cleanup().await; _guard_z.cleanup().await;` — sequential, both outside `e2e_timeout!`.
- **Test 8 (`--no-sort`) uses `assert_json_keys_eq` (set equality from step 7), NOT `assert_json_keys_order_eq`.** This is intentional.
- **Test 9 has TWO assertions:** key-order via the helper, plus an inline monotonicity check on LastModified. Both must pass.
- **Do NOT add `#[ignore]` to any test.** Flakiness should be diagnosed via `--nocapture`.
- **The `uuid::Uuid` import in Tests 10-11 is function-local** (`use uuid::Uuid;` inside the test body). `uuid` is already a dev-dependency from step 6.
