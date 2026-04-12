# Large-Scale Listing Completeness E2E Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 1 end-to-end test that uploads ~16,000 objects with a realistic 6-7 level hierarchy and verifies s3ls enumerates every object correctly under 5 different listing configurations (full recursive, prefix-scoped, max-depth, and two max-parallel-listing-max-depth values).

**Architecture:** One new `tests/e2e_large_listing.rs` file with a single `#[tokio::test]` function. The test generates 16,082 keys programmatically, uploads them via a new `put_objects_parallel_n` helper (256 concurrent uploads), then runs s3ls 5 times against the same bucket and asserts set-equality or structural correctness for each run. Uses a 300-second timeout instead of the standard 60-second `e2e_timeout!`.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (production dep), `serde_json` (production dep), `std::collections::HashSet` for set comparison.

**Depends on:** Step 6 framework (`TestHelper`, `BucketGuard`, `run_s3ls`).

**Spec:** `docs/superpowers/specs/2026-04-11-large-listing-e2e-tests-design.md`

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- This test costs ~$0.16 per run (16K PUTs + 16K DELETEs). Keep that in mind.
- The test uses a **300-second timeout** (not the standard 60s `e2e_timeout!`).

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/e2e_large_listing.rs` | 1 `#[tokio::test]` function with 5 sub-assertions. Gated with `#![cfg(e2e_test)]`. |

**Modified files:**

| Path | Change |
|---|---|
| `tests/common/mod.rs` | Add `put_objects_parallel_n` method, refactor `put_objects_parallel` to delegate. |

**No changes to `src/`.** No `Cargo.toml` changes.

---

## Important notes for the executor

**Upload concurrency.** The new `put_objects_parallel_n` helper takes a `max_concurrency` parameter. The test uses 256 concurrent uploads to keep the 16K-object upload under ~15 seconds. The existing `put_objects_parallel` is refactored to delegate to `put_objects_parallel_n` with concurrency=16 — all existing tests continue to work unchanged.

**Timeout.** The test uses `tokio::time::timeout(Duration::from_secs(300), async { ... }).await.expect("test timed out")` directly instead of `e2e_timeout!`. The standard 60s timeout is too short for this test.

**No `--sort`.** All s3ls runs use `--no-sort` to avoid buffering 16K objects in memory. The test is about enumeration completeness, not sort correctness.

**JSON only.** All assertions parse `--json` NDJSON output.

**Inline set comparison.** The test defines a local `assert_key_set_eq` function that reports only the first 10 missing/extra keys on failure (not the full 16K-line stdout). This is NOT added to `tests/common/mod.rs` because it's specific to the large-listing test's error reporting needs.

**Verification commands:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — new file compiles to empty. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint. |
| `cargo fmt --check` | Formatting. |

---

### LL-Task 1: Add `put_objects_parallel_n` to `tests/common/mod.rs`

Refactors the existing `put_objects_parallel` to delegate to a new `put_objects_parallel_n` method with configurable concurrency.

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Replace `put_objects_parallel` with `put_objects_parallel_n` + delegating wrapper**

Find the existing `put_objects_parallel` method at `tests/common/mod.rs:547-574`. Replace the entire method with:

```rust
    /// Upload multiple objects in parallel (up to 16 concurrent uploads).
    pub async fn put_objects_parallel(&self, bucket: &str, objects: Vec<(String, Vec<u8>)>) {
        self.put_objects_parallel_n(bucket, objects, 16).await;
    }

    /// Upload multiple objects in parallel with configurable concurrency.
    ///
    /// `max_concurrency` controls the semaphore limit. Use 16 for small
    /// fixtures (the default via `put_objects_parallel`). Use 256+ for
    /// large-scale tests (e.g., 10K+ objects) to keep upload time
    /// reasonable.
    pub async fn put_objects_parallel_n(
        &self,
        bucket: &str,
        objects: Vec<(String, Vec<u8>)>,
        max_concurrency: usize,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrency));
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
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: all existing tests pass (they all use `put_objects_parallel` which now delegates to `put_objects_parallel_n` with concurrency=16 — identical behavior).

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
test(large-listing): add put_objects_parallel_n with configurable concurrency

Refactors put_objects_parallel to delegate to a new
put_objects_parallel_n method that accepts a max_concurrency
parameter. The existing put_objects_parallel now calls
put_objects_parallel_n with concurrency=16 — all existing tests
work unchanged. The large-listing test will use concurrency=256
to upload ~16K objects in ~15 seconds.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### LL-Task 2: Create `tests/e2e_large_listing.rs` with the 5-sub-assertion test

The single test function that uploads 16,082 objects and runs 5 listing configurations.

**Files:**
- Create: `tests/e2e_large_listing.rs`

- [ ] **Step 1: Create the file**

Create `tests/e2e_large_listing.rs` with the following complete content:

```rust
#![cfg(e2e_test)]

//! Large-scale listing completeness test.
//!
//! Uploads ~16,000 objects with a realistic 6-7 level hierarchy (data
//! lake partitions at depth 6, application logs at depth 7) and
//! verifies s3ls enumerates every object correctly under 5 different
//! listing configurations: full recursive, prefix-scoped, max-depth 3,
//! and two max-parallel-listing-max-depth values (1 and 4).
//!
//! This is the only e2e test that exercises the parallel listing engine
//! at realistic scale. All other tests use tiny fixtures (3-10 objects).
//!
//! Uses a 300-second timeout (not the standard 60s e2e_timeout!).
//!
//! Design: `docs/superpowers/specs/2026-04-11-large-listing-e2e-tests-design.md`

mod common;

use common::*;
use std::collections::HashSet;

/// Generate the expected set of 16,082 keys for the large-listing fixture.
///
/// Hierarchy:
/// - `config.json` (depth 1)
/// - `data/manifest.json` (depth 2)
/// - `data/tenant-{01..05}/{2024,2025}/{01..12}/{01..25}/part-{001..005}.parquet` (depth 6)
/// - `logs/app/{2024,2025}/{01..12}/{01..15}/server-{01..03}/app.log` (depth 7)
fn generate_expected_keys() -> Vec<String> {
    let mut keys: Vec<String> = Vec::with_capacity(16_082);

    // Depth 1: config file
    keys.push("config.json".to_string());

    // Depth 2: data manifest
    keys.push("data/manifest.json".to_string());

    // Depth 6: data partitions
    // 5 tenants × 2 years × 12 months × 25 days × 5 files = 15,000
    for tenant in 1..=5 {
        for year in [2024, 2025] {
            for month in 1..=12 {
                for day in 1..=25 {
                    for part in 1..=5 {
                        keys.push(format!(
                            "data/tenant-{tenant:02}/{year}/{month:02}/{day:02}/part-{part:03}.parquet"
                        ));
                    }
                }
            }
        }
    }

    // Depth 7: application logs
    // 2 years × 12 months × 15 days × 3 servers × 1 file = 1,080
    for year in [2024, 2025] {
        for month in 1..=12 {
            for day in 1..=15 {
                for server in 1..=3 {
                    keys.push(format!(
                        "logs/app/{year}/{month:02}/{day:02}/server-{server:02}/app.log"
                    ));
                }
            }
        }
    }

    assert_eq!(keys.len(), 16_082, "key generation bug: expected 16,082 keys");
    keys
}

/// Parse NDJSON stdout and collect all `Key` fields into a HashSet.
fn collect_keys_from_json(stdout: &str) -> HashSet<String> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()?
                .get("Key")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect()
}

/// Parse NDJSON stdout and collect all `Prefix` fields into a HashSet.
fn collect_prefixes_from_json(stdout: &str) -> HashSet<String> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()?
                .get("Prefix")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect()
}

/// Assert that `actual` key set equals `expected` key set. On mismatch,
/// reports counts and the first 10 missing/extra keys (not the full
/// 16K-line stdout).
fn assert_key_set_eq(actual: &HashSet<String>, expected: &HashSet<String>, label: &str) {
    if actual == expected {
        return;
    }

    let missing_count = expected.difference(actual).count();
    let extra_count = actual.difference(expected).count();
    let mut missing: Vec<&String> = expected.difference(actual).collect();
    let mut extra: Vec<&String> = actual.difference(expected).collect();
    missing.sort();
    extra.sort();
    missing.truncate(10);
    extra.truncate(10);

    panic!(
        "[{label}] key set mismatch\n  \
         expected count: {}\n  \
         actual count:   {}\n  \
         missing ({missing_count} total, first 10): {missing:?}\n  \
         extra ({extra_count} total, first 10): {extra:?}",
        expected.len(),
        actual.len(),
    );
}

/// Large-scale listing completeness test.
///
/// Uploads 16,082 objects with a 6-7 level hierarchy, then runs s3ls
/// 5 times under different configurations and asserts enumeration
/// completeness for each.
///
/// Uses a 300-second timeout (not the standard 60s e2e_timeout!).
/// Upload uses 256 concurrent PUTs via `put_objects_parallel_n`.
#[tokio::test]
async fn e2e_large_listing_completeness() {
    use std::time::Duration;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    let result = tokio::time::timeout(Duration::from_secs(300), async {
        helper.create_bucket(&bucket).await;

        // --- Generate and upload fixture ---
        let expected_keys = generate_expected_keys();
        let expected_set: HashSet<String> = expected_keys.iter().cloned().collect();

        println!("Uploading {} objects with 256 concurrent PUTs...", expected_keys.len());
        let objects: Vec<(String, Vec<u8>)> = expected_keys
            .iter()
            .map(|k| (k.clone(), b"x".to_vec()))
            .collect();
        helper
            .put_objects_parallel_n(&bucket, objects, 256)
            .await;
        println!("Upload complete.");

        let target = format!("s3://{bucket}/");

        // --- Sub-assertion 1: Full recursive listing from root ---
        println!("Sub-assertion 1: full recursive listing...");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--no-sort",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual = collect_keys_from_json(&output.stdout);
        assert_key_set_eq(&actual, &expected_set, "full recursive listing");
        println!("  OK: {} keys match.", actual.len());

        // --- Sub-assertion 2: Prefix-scoped listing ---
        println!("Sub-assertion 2: prefix-scoped listing (data/tenant-03/2025/)...");
        let prefix_target = format!("s3://{bucket}/data/tenant-03/2025/");
        let output = TestHelper::run_s3ls(&[
            prefix_target.as_str(),
            "--recursive",
            "--json",
            "--no-sort",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual_prefix = collect_keys_from_json(&output.stdout);
        let expected_prefix: HashSet<String> = expected_set
            .iter()
            .filter(|k| k.starts_with("data/tenant-03/2025/"))
            .cloned()
            .collect();
        assert_eq!(
            expected_prefix.len(),
            1500,
            "sanity: expected 12 months × 25 days × 5 files = 1500"
        );
        assert_key_set_eq(
            &actual_prefix,
            &expected_prefix,
            "prefix-scoped listing (data/tenant-03/2025/)",
        );
        println!("  OK: {} keys match.", actual_prefix.len());

        // --- Sub-assertion 3: Depth-limited listing (max-depth 3) ---
        println!("Sub-assertion 3: max-depth 3...");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "3",
            "--json",
            "--no-sort",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        let depth3_keys = collect_keys_from_json(&output.stdout);
        let depth3_prefixes = collect_prefixes_from_json(&output.stdout);

        // Objects at depth ≤ 3: config.json (depth 1), data/manifest.json (depth 2)
        let expected_depth3_keys: HashSet<String> =
            ["config.json", "data/manifest.json"]
                .iter()
                .map(|s| s.to_string())
                .collect();
        assert_key_set_eq(
            &depth3_keys,
            &expected_depth3_keys,
            "max-depth 3: objects at depth ≤ 3",
        );

        // Prefix entries at depth 3 boundary: data/tenant-{01..05}/ (5)
        // + logs/app/2024/, logs/app/2025/ (2) = 7.
        let expected_depth3_prefixes: HashSet<String> = [
            "data/tenant-01/",
            "data/tenant-02/",
            "data/tenant-03/",
            "data/tenant-04/",
            "data/tenant-05/",
            "logs/app/2024/",
            "logs/app/2025/",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            depth3_prefixes, expected_depth3_prefixes,
            "max-depth 3: prefix entries mismatch\n  expected: {expected_depth3_prefixes:?}\n  actual: {depth3_prefixes:?}"
        );
        println!(
            "  OK: {} objects + {} prefixes.",
            depth3_keys.len(),
            depth3_prefixes.len()
        );

        // --- Sub-assertion 4: max-parallel-listing-max-depth 1 ---
        println!("Sub-assertion 4: max-parallel-listing-max-depth 1...");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--no-sort",
            "--max-parallel-listing-max-depth",
            "1",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual = collect_keys_from_json(&output.stdout);
        assert_key_set_eq(
            &actual,
            &expected_set,
            "max-parallel-listing-max-depth 1",
        );
        println!("  OK: {} keys match.", actual.len());

        // --- Sub-assertion 5: max-parallel-listing-max-depth 4 ---
        println!("Sub-assertion 5: max-parallel-listing-max-depth 4...");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--no-sort",
            "--max-parallel-listing-max-depth",
            "4",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual = collect_keys_from_json(&output.stdout);
        assert_key_set_eq(
            &actual,
            &expected_set,
            "max-parallel-listing-max-depth 4",
        );
        println!("  OK: {} keys match.", actual.len());

        println!("All 5 sub-assertions passed for {} objects.", expected_set.len());
    })
    .await;

    // Cleanup runs regardless of timeout.
    _guard.cleanup().await;

    // Propagate timeout error after cleanup.
    result.expect("large-listing test timed out after 300 seconds");
}
```

- [ ] **Step 2: Verify non-gated build**

Run: `cargo test`
Expected: passes. New file compiles to empty.

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
git add tests/e2e_large_listing.rs
git commit -m "$(cat <<'EOF'
test(large-listing): add 16K-object listing completeness test

One test that uploads 16,082 objects with a realistic 6-7 level
hierarchy (data lake partitions at depth 6, application logs at
depth 7) and verifies s3ls enumerates every object correctly under
5 configurations: full recursive, prefix-scoped, max-depth 3,
max-parallel-listing-max-depth 1, and max-parallel-listing-max-depth
4. Uses 256 concurrent PUTs for upload and a 300-second timeout.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### LL-Task 3: Manual verification against real S3

Manual-only.

- [ ] **Step 1: Run the large-listing test**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_large_listing -- --nocapture`
Expected: 1 test passes with progress output showing upload + 5 sub-assertions. Runtime ~2-3 minutes.

- [ ] **Step 2: Run full e2e suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture`
Expected: all 58 tests pass (57 prior + 1 new).

- [ ] **Step 3: Check for leaked buckets**

Run:
```bash
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text
```

- [ ] **Step 4: No commit**

---

## Notes for the executor

- **2 implementation tasks + 1 verification task.** Expect 2 commits.
- **LL-Task 1 is a pure refactor** — existing `put_objects_parallel` behavior is preserved via delegation. If any existing test breaks, the refactor is wrong.
- **LL-Task 2 is the entire test in one file.** The test has module-level helper functions (`generate_expected_keys`, `collect_keys_from_json`, `collect_prefixes_from_json`, `assert_key_set_eq`) that are private to the test file — NOT added to `tests/common/mod.rs`.
- **The test uses `tokio::time::timeout` directly** with a 300-second limit. The `_guard.cleanup().await` call is OUTSIDE the timeout (after `result.expect(...)` — actually, looking at the code, cleanup runs before the `expect`. This ensures cleanup happens even on timeout. The `result.expect(...)` then propagates the timeout panic after cleanup.
- **`--no-sort` is critical.** Without it, s3ls would buffer all 16K objects in memory for sorting, which is slow and unnecessary for a completeness test.
- **The `println!` progress lines** appear only with `--nocapture`. They help track which sub-assertion is running during the ~2-3 minute test.
- **Cost: ~$0.16 per run.** Be mindful of repeated runs during debugging.
