# Bucket Listing E2E Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 6 end-to-end tests for s3ls bucket listing functionality (default JSON shape, `--bucket-name-prefix` filtering, combined display flags, `--no-sort`, and `--list-express-one-zone-buckets`) in a new `tests/e2e_bucket_listing.rs` file, using 2 new helpers in `tests/common/mod.rs`. All assertions use `--json` output.

**Architecture:** Gated with `#![cfg(e2e_test)]`. One new file holds all 6 tests. Each test creates 1-3 buckets (general-purpose or directory), runs `s3ls --json` with specific bucket-listing flags (no target URL), and asserts on the NDJSON output scoped to known test bucket names. Test 6 (`--list-express-one-zone-buckets`) skips gracefully in regions without a mapped Express One Zone availability zone.

**Tech Stack:** Rust 2024, tokio, aws-sdk-s3 (production dep), `serde_json` (production dep), `uuid` (existing dev-dep), existing framework helpers.

**Depends on:** Step 6 framework. The new file is a sibling of the other `tests/e2e_*.rs` files.

**Spec:** `docs/superpowers/specs/2026-04-11-bucket-listing-e2e-tests-design.md`

**Important:**
- Auto-memory feedback: **always run `cargo fmt` and `cargo clippy --all-features` before `git commit`.**
- Bucket listing tests use **no target URL** in `run_s3ls` args — that's what triggers bucket listing mode.
- All assertions scope to the test bucket's unique name (the account may have other buckets).

---

## File Structure

**New files:**

| Path | Responsibility |
|---|---|
| `tests/e2e_bucket_listing.rs` | 6 `#[tokio::test]` functions covering bucket listing. Gated with `#![cfg(e2e_test)]`. |

**Modified files:**

| Path | Change |
|---|---|
| `tests/common/mod.rs` | Add `TestHelper::create_directory_bucket` method and `express_one_zone_az_for_region` free function. |

**No changes to `src/`.** No `Cargo.toml` changes.

---

## Important notes for the executor

**Bucket listing mode.** When `run_s3ls` is called without a target URL (e.g., `&["--json"]`), s3ls enters bucket listing mode. The `run_s3ls` helper auto-appends `--target-profile s3ls-e2e-test` but does NOT inject a target URL, so bucket listing triggers correctly.

**JSON output for bucket listing** (from `src/bucket_lister.rs:77-117`):
```json
{
  "Name": "bucket-name",
  "CreationDate": "2026-01-01T00:00:00+00:00",
  "BucketRegion": "us-east-1",
  "BucketArn": "arn:aws:s3:::bucket-name",   // only with --show-bucket-arn
  "Owner": { "DisplayName": "...", "ID": "..." }  // only with --show-owner
}
```

**Scoping assertions to test buckets.** The account may have many buckets. Every assertion finds the test bucket's NDJSON line by parsing each line and matching `v.get("Name").as_str() == bucket_name`, then asserts on that specific line. Never assert on total line count or absolute position.

**Finding a bucket in NDJSON output — reusable inline pattern:**
```rust
let bucket_line = output.stdout.lines()
    .filter(|l| !l.trim().is_empty())
    .find(|l| {
        serde_json::from_str::<serde_json::Value>(l)
            .ok()
            .and_then(|v| v.get("Name").and_then(|n| n.as_str()).map(|s| s == bucket))
            .unwrap_or(false)
    });
```

**Directory bucket naming convention:** Express One Zone directory buckets must have names ending with `--{az_id}--x-s3` (e.g., `s3ls-e2e-express-abc123--use1-az4--x-s3`). The `create_directory_bucket` helper enforces this at the S3 API level.

**Verification commands:**

| Command | Purpose |
|---|---|
| `cargo test` | Non-gated build — new file compiles to empty. |
| `RUSTFLAGS='--cfg e2e_test' cargo build --tests` | Gated build. |
| `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings` | Gated lint. |
| `cargo clippy --all-features -- -D warnings` | Non-gated lint. |
| `cargo fmt --check` | Formatting. |

---

### BL-Task 1: Add `create_directory_bucket` and `express_one_zone_az_for_region` to `tests/common/mod.rs`

Pure framework plumbing. No new test file yet.

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add `create_directory_bucket` method**

Find the `create_versioned_bucket` method in the `impl TestHelper` "Bucket management" block. Immediately after `create_versioned_bucket`, add:

```rust
    /// Create an Express One Zone (directory) bucket.
    ///
    /// `bucket` must end with `--{az_id}--x-s3` (e.g.,
    /// `s3ls-e2e-express-abc123--use1-az4--x-s3`).
    /// `az_id` must be a valid availability zone ID that supports
    /// Express One Zone (e.g., `"use1-az4"`).
    ///
    /// Used by `e2e_bucket_listing_express_one_zone`. The helper uses
    /// `BucketType::Directory` + `DataRedundancy::SingleAvailabilityZone` +
    /// `LocationType::AvailabilityZone`.
    pub async fn create_directory_bucket(&self, bucket: &str, az_id: &str) {
        use aws_sdk_s3::types::{
            BucketInfo, BucketType, DataRedundancy, LocationInfo, LocationType,
        };

        let location = LocationInfo::builder()
            .r#type(LocationType::AvailabilityZone)
            .name(az_id)
            .build();

        let bucket_info = BucketInfo::builder()
            .data_redundancy(DataRedundancy::SingleAvailabilityZone)
            .r#type(BucketType::Directory)
            .build();

        let config = CreateBucketConfiguration::builder()
            .location(location)
            .bucket(bucket_info)
            .build();

        self.client
            .create_bucket()
            .bucket(bucket)
            .create_bucket_configuration(config)
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to create directory bucket {bucket}: {e}")
            });
    }
```

- [ ] **Step 2: Add `express_one_zone_az_for_region` free function**

At the bottom of `tests/common/mod.rs`, after the existing helpers, append:

```rust
/// Map a region to a known Express One Zone availability zone ID.
/// Returns `None` for regions where Express One Zone is not mapped.
/// Tests that depend on this can skip gracefully with a `println!`
/// note when the region is unmapped.
pub fn express_one_zone_az_for_region(region: &str) -> Option<&'static str> {
    match region {
        "us-east-1" => Some("use1-az4"),
        "us-east-2" => Some("use2-az1"),
        "us-west-2" => Some("usw2-az1"),
        "ap-northeast-1" => Some("apne1-az4"),
        "ap-southeast-1" => Some("apse1-az2"),
        "eu-west-1" => Some("euw1-az1"),
        "eu-north-1" => Some("eun1-az1"),
        _ => None,
    }
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
git add tests/common/mod.rs
git commit -m "$(cat <<'EOF'
test(bucket-listing): add helpers for directory buckets and AZ lookup

Adds two helpers to the e2e framework:
- TestHelper::create_directory_bucket: create an Express One Zone
  (directory) bucket with BucketType::Directory and
  DataRedundancy::SingleAvailabilityZone.
- express_one_zone_az_for_region: map a region to a known Express
  One Zone availability zone ID, returning None for unmapped regions
  so tests can skip gracefully.

No production code changes. No Cargo.toml changes.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### BL-Task 2: Scaffold `tests/e2e_bucket_listing.rs` with default shape and prefix filter tests (Tests 1, 2, 3)

Creates the new file with the first 3 tests.

**Files:**
- Create: `tests/e2e_bucket_listing.rs`

- [ ] **Step 1: Create file with preamble and Test 1**

Create the file:

```rust
#![cfg(e2e_test)]

//! Bucket listing end-to-end tests.
//!
//! Covers s3ls bucket listing (no target URL) in JSON mode: default
//! JSON shape, `--bucket-name-prefix` filtering, combined display
//! flags, `--no-sort`, and `--list-express-one-zone-buckets`.
//!
//! All assertions scope to test bucket names because the AWS account
//! may have other buckets.
//!
//! Design: `docs/superpowers/specs/2026-04-11-bucket-listing-e2e-tests-design.md`

mod common;

use common::*;

/// Default bucket listing JSON shape: verify mandatory fields are present
/// and optional fields are absent when no display flags are set.
#[tokio::test]
async fn e2e_bucket_listing_default_json_shape() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&["--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Find our test bucket in the NDJSON output.
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
                panic!("default shape: test bucket {bucket} not found in output")
            });

        let v: serde_json::Value =
            serde_json::from_str(bucket_line).expect("failed to parse bucket line");

        // Mandatory fields
        assert_eq!(
            v.get("Name").and_then(|n| n.as_str()),
            Some(bucket.as_str()),
            "default shape: Name mismatch"
        );
        assert!(
            v.get("CreationDate")
                .and_then(|d| d.as_str())
                .is_some_and(|s| !s.is_empty()),
            "default shape: CreationDate missing or empty, got {v:?}"
        );
        assert!(
            v.get("BucketRegion")
                .and_then(|r| r.as_str())
                .is_some_and(|s| !s.is_empty()),
            "default shape: BucketRegion missing or empty, got {v:?}"
        );

        // Optional fields must be absent (no display flags set)
        assert!(
            v.get("BucketArn").is_none(),
            "default shape: BucketArn should be absent without --show-bucket-arn, got {v:?}"
        );
        assert!(
            v.get("Owner").is_none(),
            "default shape: Owner should be absent without --show-owner, got {v:?}"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append Test 2 (`e2e_bucket_listing_prefix_filter`)**

Append:

```rust

/// `--bucket-name-prefix` filtering: only buckets whose name starts
/// with the prefix appear in the output. A non-matching bucket is
/// excluded.
#[tokio::test]
async fn e2e_bucket_listing_prefix_filter() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let id = Uuid::new_v4();
    let bucket_match = format!("s3ls-e2e-pfx-match-{id}");
    let bucket_other = format!("s3ls-e2e-pfx-other-{id}");
    let _guard_match = helper.bucket_guard(&bucket_match);
    let _guard_other = helper.bucket_guard(&bucket_other);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_match).await;
        helper.create_bucket(&bucket_other).await;

        let output = TestHelper::run_s3ls(&[
            "--json",
            "--bucket-name-prefix",
            "s3ls-e2e-pfx-match-",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Matching bucket must appear.
        let found_match = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket_match)
                    })
                    .unwrap_or(false)
            });
        assert!(
            found_match,
            "prefix filter: matching bucket {bucket_match} not found in output"
        );

        // Non-matching bucket must NOT appear.
        let found_other = output.stdout.contains(&bucket_other);
        assert!(
            !found_other,
            "prefix filter: non-matching bucket {bucket_other} unexpectedly found in output"
        );
    });

    _guard_match.cleanup().await;
    _guard_other.cleanup().await;
}
```

- [ ] **Step 3: Append Test 3 (`e2e_bucket_listing_prefix_no_match`)**

Append:

```rust

/// `--bucket-name-prefix` with a prefix that matches nothing: s3ls
/// exits successfully with no bucket entries in the output.
#[tokio::test]
async fn e2e_bucket_listing_prefix_no_match() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    // Create a bucket just for guard lifecycle (its name is irrelevant).
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Use a UUID-suffixed prefix that is guaranteed to match no bucket.
        let no_match_prefix = format!("s3ls-e2e-nonexistent-{}", Uuid::new_v4());

        let output = TestHelper::run_s3ls(&[
            "--json",
            "--bucket-name-prefix",
            no_match_prefix.as_str(),
        ]);
        assert!(
            output.status.success(),
            "prefix no-match: s3ls should exit 0 even with no matches, got: {}",
            output.stderr
        );

        // No NDJSON line should have a Name field.
        let has_name = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| v.get("Name").map(|_| true))
                    .unwrap_or(false)
            });
        assert!(
            !has_name,
            "prefix no-match: expected no bucket entries in output, but found at least one"
        );
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
git add tests/e2e_bucket_listing.rs
git commit -m "$(cat <<'EOF'
test(bucket-listing): add e2e tests for default JSON shape and --bucket-name-prefix

Three tests: default_json_shape verifies mandatory fields (Name,
CreationDate, BucketRegion) are present and optional fields
(BucketArn, Owner) are absent without display flags. prefix_filter
creates two buckets and verifies only the matching one appears.
prefix_no_match verifies s3ls exits 0 with no output when the
prefix matches nothing.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### BL-Task 3: Add combined flags, no-sort, and Express One Zone tests (Tests 4, 5, 6)

**Files:**
- Modify: `tests/e2e_bucket_listing.rs`

- [ ] **Step 1: Append Test 4 (`e2e_bucket_listing_combined_flags`)**

Append:

```rust

/// `--show-bucket-arn --show-owner` together: verify ALL fields are
/// present. The display suite tests each flag individually; this test
/// verifies they compose correctly without interference.
#[tokio::test]
async fn e2e_bucket_listing_combined_flags() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&[
            "--json",
            "--show-bucket-arn",
            "--show-owner",
        ]);
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
                panic!("combined flags: test bucket {bucket} not found in output")
            });

        let v: serde_json::Value =
            serde_json::from_str(bucket_line).expect("failed to parse bucket line");

        // Mandatory fields
        assert!(
            v.get("Name").is_some(),
            "combined flags: Name missing"
        );
        assert!(
            v.get("CreationDate").is_some(),
            "combined flags: CreationDate missing"
        );
        assert!(
            v.get("BucketRegion").is_some(),
            "combined flags: BucketRegion missing"
        );

        // Both optional fields must be present
        assert!(
            v.get("BucketArn")
                .and_then(|a| a.as_str())
                .is_some_and(|s| !s.is_empty()),
            "combined flags: BucketArn missing or empty, got {v:?}"
        );
        let owner = v
            .get("Owner")
            .expect("combined flags: Owner missing");
        assert!(
            owner
                .get("ID")
                .and_then(|id| id.as_str())
                .is_some_and(|s| !s.is_empty()),
            "combined flags: Owner.ID missing or empty, got {owner:?}"
        );
    });

    _guard.cleanup().await;
}
```

- [ ] **Step 2: Append Test 5 (`e2e_bucket_listing_no_sort`)**

Append:

```rust

/// `--no-sort`: both test buckets appear in the output (set check,
/// order not asserted).
#[tokio::test]
async fn e2e_bucket_listing_no_sort() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&["--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Both buckets must appear somewhere in the output.
        assert!(
            output.stdout.contains(&bucket_a),
            "no-sort: bucket {bucket_a} not found in output"
        );
        assert!(
            output.stdout.contains(&bucket_z),
            "no-sort: bucket {bucket_z} not found in output"
        );
        // Order is intentionally NOT asserted.
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}
```

- [ ] **Step 3: Append Test 6 (`e2e_bucket_listing_express_one_zone`)**

Append:

```rust

/// `--list-express-one-zone-buckets`: creates a directory bucket and a
/// regular bucket, then lists only Express One Zone buckets. The
/// directory bucket must appear; the regular bucket must not.
///
/// Skips gracefully in regions where Express One Zone is not mapped
/// (prints a note and returns without assertions).
#[tokio::test]
async fn e2e_bucket_listing_express_one_zone() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;

    // Look up AZ for this region. Skip if unmapped.
    let az_id = match express_one_zone_az_for_region(helper.region()) {
        Some(az) => az,
        None => {
            println!(
                "skipped: no Express One Zone AZ mapped for region {:?}",
                helper.region()
            );
            return;
        }
    };

    let id = Uuid::new_v4();
    let bucket_express = format!("s3ls-e2e-express-{id}--{az_id}--x-s3");
    let bucket_regular = format!("s3ls-e2e-regular-{id}");
    let _guard_express = helper.bucket_guard(&bucket_express);
    let _guard_regular = helper.bucket_guard(&bucket_regular);

    e2e_timeout!(async {
        helper.create_directory_bucket(&bucket_express, az_id).await;
        helper.create_bucket(&bucket_regular).await;

        let output = TestHelper::run_s3ls(&[
            "--json",
            "--list-express-one-zone-buckets",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Directory bucket must appear.
        let found_express = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket_express)
                    })
                    .unwrap_or(false)
            });
        assert!(
            found_express,
            "express one zone: directory bucket {bucket_express} not found in output"
        );

        // Regular bucket must NOT appear.
        let found_regular = output.stdout.contains(&bucket_regular);
        assert!(
            !found_regular,
            "express one zone: regular bucket {bucket_regular} unexpectedly found in output"
        );
    });

    _guard_express.cleanup().await;
    _guard_regular.cleanup().await;
}
```

- [ ] **Step 4: Verify non-gated build**

Run: `cargo test`
Expected: passes.

- [ ] **Step 5: Verify gated build**

Run: `RUSTFLAGS='--cfg e2e_test' cargo build --tests`
Expected: successful build. This completes the 6-test file.

- [ ] **Step 6: fmt + clippy**

Run: `cargo fmt`
Run: `cargo clippy --all-features -- -D warnings`
Run: `RUSTFLAGS='--cfg e2e_test' cargo clippy --all-targets --all-features -- -D warnings`
Expected: all clean.

- [ ] **Step 7: Commit**

```bash
git add tests/e2e_bucket_listing.rs
git commit -m "$(cat <<'EOF'
test(bucket-listing): add e2e tests for combined flags, --no-sort, and Express One Zone

Three tests: combined_flags verifies --show-bucket-arn +
--show-owner compose correctly with all fields present. no_sort
verifies both test buckets appear (set check, order not asserted).
express_one_zone creates a directory bucket and a regular bucket,
lists with --list-express-one-zone-buckets, and verifies only the
directory bucket appears. Skips gracefully in unmapped regions.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### BL-Task 4: Manual verification against real S3

Manual-only. Does **not** run in CI.

**Files:** (none modified)

- [ ] **Step 1: Confirm AWS profile**

Run: `aws configure list --profile s3ls-e2e-test`

- [ ] **Step 2: Run the bucket listing suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_bucket_listing -- --nocapture`
Expected: 5 or 6 tests pass (Test 6 may skip with a println note if the region has no Express One Zone AZ mapping).

- [ ] **Step 3: Run the full e2e suite**

Run: `RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture`
Expected: all suites pass.

- [ ] **Step 4: Confirm non-gated `cargo test` still clean**

Run: `cargo test`

- [ ] **Step 5: Check for leaked buckets**

Run:
```bash
aws s3api list-buckets --profile s3ls-e2e-test \
  --query 'Buckets[?starts_with(Name, `s3ls-e2e-`)].Name' \
  --output text
```
Expected: empty. If non-empty, follow `tests/README.md` for cleanup. Note: directory buckets (`--x-s3` suffix) can be deleted via `aws s3api delete-bucket --bucket <name>` directly (they don't hold objects in these tests).

- [ ] **Step 6: No commit**

Verification only.

---

## Notes for the executor

- **Each task produces one commit** except BL-Task 4 (verification only). Expect 3 commits.
- **The file grows monotonically** — BL-Task 3 appends to the file created by BL-Task 2.
- **Tests 2, 5, and 6 each create 2+ buckets and hold 2+ guards.** All guards are cleaned up in order, OUTSIDE `e2e_timeout!`.
- **Test 3 uses a UUID-suffixed prefix** for the no-match case, guaranteeing no false collisions.
- **Test 6 returns early (not panics) when the AZ is unmapped** — `println!` + `return` before any `e2e_timeout!` block or bucket creation. This means `_guard_express` and `_guard_regular` are never initialized in the skip path. The test structure must account for this: declare guards INSIDE the `e2e_timeout!` block OR use `Option<BucketGuard>` with conditional cleanup. The simplest approach (used in Step 3 above): declare guards before `e2e_timeout!`, return early before entering the block. Since the guards are created with `bucket_guard` before any bucket exists, cleanup on an uncreated bucket is a no-op (the `delete_bucket_cascade` calls return early on non-existent buckets).
- **Do NOT add `#[ignore]` to Test 6.** The graceful skip is handled by the `express_one_zone_az_for_region` + `return` pattern, not by `#[ignore]`.
- **`uuid::Uuid` imports are function-local** in tests that need custom bucket names (`use uuid::Uuid;` inside the test body).
