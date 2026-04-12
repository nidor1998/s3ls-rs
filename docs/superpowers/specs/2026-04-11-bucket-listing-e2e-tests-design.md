# Bucket Listing E2E Tests — Design

**Date:** 2026-04-11
**Status:** Design (pending implementation plan)
**Builds on:**
- `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md` (e2e framework)
- `docs/superpowers/specs/2026-04-11-display-e2e-tests-design.md` (display tests — includes 2 bucket listing display tests)
- `docs/superpowers/specs/2026-04-11-sort-e2e-tests-design.md` (sort tests — includes 2 bucket listing sort tests)

---

## Goal

Add end-to-end test coverage for s3ls bucket listing functionality —
the behaviors exercised when s3ls is invoked without a target S3 URL.
Fills the gaps left by the display suite (which tests individual
`--show-*` flags) and the sort suite (which tests `--sort bucket`
ordering) by covering the baseline JSON shape, `--bucket-name-prefix`
filtering, combined display flags, `--no-sort`, and
`--list-express-one-zone-buckets`.

All assertions use `--json` output. Runs under
`RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_bucket_listing`.

After this step:

- `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_bucket_listing` runs
  against real S3 and passes.
- `cargo test` (without the cfg flag) still passes.
- `cargo clippy --all-features` and `cargo fmt --check` stay clean.
- Every bucket listing option has explicit coverage (except text-mode,
  which the user scoped out).

## Non-goals

- **Text-mode assertions.** User scoped to JSON-only.
- **Re-testing individual `--show-*` flag on/off.** Already covered by
  `e2e_display_bucket_listing_show_bucket_arn` and
  `e2e_display_bucket_listing_show_owner` in the display suite.
- **Re-testing `--sort bucket` ordering.** Already covered by
  `e2e_sort_bucket_listing_asc` and `e2e_sort_bucket_listing_desc` in
  the sort suite.
- **`--sort date` / `--sort region` for buckets.** `--sort date` requires
  controlled creation timestamps (sleeps + 2 buckets), which is already
  tested for object listing. `--sort region` requires multi-region bucket
  creation, which is impractical.
- **Production code changes.**
- **Error-path tests** (invalid arguments rejected at parse time).

---

## Architecture

### File layout

New file:

```
tests/e2e_bucket_listing.rs    # 6 test functions, cfg-gated
```

Modified file:

```
tests/common/mod.rs            # 2 additions: create_directory_bucket, express_one_zone_az_for_region
```

### Cfg gating

`tests/e2e_bucket_listing.rs` starts with `#![cfg(e2e_test)]`. Matches
the pattern of all other e2e test files.

### Framework reuse

Reused without modification:
- `TestHelper::new()`, `generate_bucket_name()`, `create_bucket()`,
  `bucket_guard()`
- `TestHelper::run_s3ls(&args)` + `S3lsOutput`
- `e2e_timeout!()` macro, `E2E_TIMEOUT` constant
- `BucketGuard::cleanup()`

### New additions to `tests/common/mod.rs`

#### 1. `TestHelper::create_directory_bucket`

```rust
/// Create an Express One Zone (directory) bucket.
///
/// Requires:
/// - `bucket` must end with `--{az_id}--x-s3` (e.g.,
///   `s3ls-e2e-express-abc123--use1-az4--x-s3`).
/// - `az_id` must be a valid availability zone ID that supports
///   Express One Zone (e.g., `"use1-az4"`).
/// - The test account must have permission to create directory buckets.
///
/// Used only by `e2e_bucket_listing_express_one_zone`. The helper uses
/// `BucketType::Directory` + `DataRedundancy::SingleAvailabilityZone` +
/// `LocationType::AvailabilityZone`.
pub async fn create_directory_bucket(
    &self,
    bucket: &str,
    az_id: &str,
);
```

Placement: inside the `impl TestHelper` "Bucket management" block, after
`create_versioned_bucket`.

#### 2. `express_one_zone_az_for_region`

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

Placement: free function at the bottom of `tests/common/mod.rs`.

### Bucket listing internals (verified against source)

Source: `src/bucket_lister.rs:17-149`.

**Entry point:** `list_buckets(config)` at line 17.

**Two code paths:**
- `config.list_express_one_zone_buckets == true` → calls
  `list_directory_buckets(client)` (uses `ListDirectoryBuckets` API).
  Note: this API does NOT support server-side prefix filtering, so
  `--bucket-name-prefix` is applied client-side via
  `buckets.retain(|e| e.name.starts_with(prefix))` (line 30).
- `config.list_express_one_zone_buckets == false` → calls
  `list_general_buckets(client, prefix)` (uses `ListBuckets` API with
  server-side `.prefix(prefix)` at line 158).

**JSON output per bucket** (lines 77-117):
```json
{
  "Name": "bucket-name",
  "CreationDate": "2026-01-01T00:00:00+00:00",   // if available
  "BucketRegion": "us-east-1",                     // if available
  "BucketArn": "arn:aws:s3:::bucket-name",         // if --show-bucket-arn
  "Owner": { "DisplayName": "...", "ID": "..." }   // if --show-owner
}
```

**Sorting** (lines 38-51): uses `config.sort` fields. Skipped when
`config.no_sort` is true.

**Key behavioral contracts being tested:**
1. Mandatory fields (`Name`, `CreationDate`, `BucketRegion`) always present
   for general-purpose buckets.
2. Optional fields (`BucketArn`, `Owner`) absent when flags are off, present
   when flags are on.
3. `--bucket-name-prefix` filters the listing (server-side for general
   buckets, client-side for directory buckets).
4. `--list-express-one-zone-buckets` switches to the directory-bucket API
   and excludes general-purpose buckets.
5. `--no-sort` returns all buckets in arbitrary order.

---

## Test catalog

Six test functions. Each creates 1-3 buckets (general-purpose or
directory), runs `s3ls --json` with specific flags, and asserts on the
JSON output. All assertions scope to the test bucket(s) by name.

### Test 1: `e2e_bucket_listing_default_json_shape`

**Fixture:** 1 general-purpose test bucket.

**Run:** `s3ls --json` (no target, no display flags)

**Assertion:** find test bucket by `Name` in NDJSON output, verify:
- `Name` equals test bucket name
- `CreationDate` present, non-empty string
- `BucketRegion` present, non-empty string
- `BucketArn` absent (no `--show-bucket-arn`)
- `Owner` absent (no `--show-owner`)

### Test 2: `e2e_bucket_listing_prefix_filter`

**Fixture:** 2 test buckets:
- `s3ls-e2e-pfx-match-{uuid}`
- `s3ls-e2e-pfx-other-{uuid}`

**Run:** `s3ls --json --bucket-name-prefix s3ls-e2e-pfx-match-`

**Assertion:**
- Matching bucket (`pfx-match-`) found by `Name` in output.
- Non-matching bucket (`pfx-other-`) NOT found by searching stdout for
  its name.

### Test 3: `e2e_bucket_listing_prefix_no_match`

**Fixture:** 1 test bucket (for guard lifecycle; its name is irrelevant).

**Run:** `s3ls --json --bucket-name-prefix s3ls-e2e-nonexistent-{uuid}`
(the UUID suffix guarantees no real bucket matches)

**Assertion:**
- s3ls exits successfully (exit code 0).
- No NDJSON lines with a `Name` field in stdout. (There may be zero lines,
  or empty lines — the test asserts NO parseable JSON object has `Name`.)

### Test 4: `e2e_bucket_listing_combined_flags`

**Fixture:** 1 general-purpose test bucket.

**Run:** `s3ls --json --show-bucket-arn --show-owner`

**Assertion:** find test bucket by `Name`, verify:
- `Name`, `CreationDate`, `BucketRegion` present
- `BucketArn` present and non-empty
- `Owner` present with non-empty `ID` subfield

### Test 5: `e2e_bucket_listing_no_sort`

**Fixture:** 2 test buckets (`s3ls-e2e-a-{uuid}`, `s3ls-e2e-z-{uuid}`).

**Run:** `s3ls --json --no-sort`

**Assertion:** both bucket names appear somewhere in stdout (search for
each name). Order is NOT asserted.

### Test 6: `e2e_bucket_listing_express_one_zone`

**Fixture:**
1. Look up AZ via `express_one_zone_az_for_region(helper.region())`. If
   `None`, print `"skipped: no Express One Zone AZ mapped for region
   {region}"` and return early (no bucket creation, no assertion, no
   failure).
2. Create directory bucket: name = `s3ls-e2e-express-{uuid}--{az}--x-s3`,
   via `create_directory_bucket`.
3. Create regular bucket: `s3ls-e2e-regular-{uuid}`.

**Run:** `s3ls --json --list-express-one-zone-buckets`

**Assertion:**
- Directory bucket found by `Name` in output.
- Regular bucket NOT found by searching stdout for its name.

**Cleanup:** both guards cleaned up. Directory bucket cleanup uses the
existing `delete_bucket_cascade` which calls `delete_all_versions` +
`delete_all_objects` + `delete_bucket`. For an empty directory bucket,
`delete_all_versions` and `delete_all_objects` return empty results and
`delete_bucket` succeeds — no special handling needed.

---

## Cost and runtime

- **6 tests × 1-3 buckets each** ≈ 10 bucket creates + 10 bucket deletes.
- **No `PutObject` calls** — these are bucket-listing tests, not object
  listing tests.
- **No sleeps** — no time-dependent behavior.
- **~6 `run_s3ls` invocations.**
- **Test 6 skips gracefully** in regions without Express One Zone AZ
  mapping, so it adds zero AWS cost in those regions.

At current S3 pricing: **well under $0.01 per run.** Runtime bounded by
`E2E_TIMEOUT = 60s` per test.

## Execution

```bash
# Just the bucket listing suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_bucket_listing -- --nocapture

# All e2e tests
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

---

## Summary of new code

- **New file:** `tests/e2e_bucket_listing.rs` (~300-400 LOC, 6 tests,
  cfg-gated).
- **Modified file:** `tests/common/mod.rs`:
  - Add `TestHelper::create_directory_bucket(bucket, az_id)` method.
  - Add `express_one_zone_az_for_region(region) -> Option<&'static str>`
    free function.
- **No production code changes.**
- **No `Cargo.toml` changes.**
- **No changes to existing test files.**
