# Versioned-Bucket Filter E2E Tests — Design

**Date:** 2026-04-11
**Status:** Implemented (9 tests, up from 7 in design). Key deviations: mtime pivot simplified to `old_lm + 1s`; added pagination test (parallel + sequential with `--max-keys 3`); added `--show-restore-status` check. See `tests/e2e_filters_versioned.rs` for final state.
**Builds on:**
- `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md` (e2e framework)
- `docs/superpowers/specs/2026-04-11-step7-filter-e2e-tests-design.md` (non-versioning filter tests, including `assert_json_keys_eq` pattern)

---

## Goal

Add end-to-end test coverage for filter behaviors that are **specific to
versioned S3 buckets** — the interactions that `tests/e2e_filters.rs`
explicitly deferred as a "non-goal" in the Step 7 spec. Runs under
`RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters_versioned` against
real AWS S3 using the `s3ls-e2e-test` profile.

After this step:

- `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters_versioned` runs
  against real S3 and passes.
- `cargo test` (without the cfg flag) still passes — the new file compiles
  to an empty binary under non-gated builds.
- `cargo clippy --all-features` and `cargo fmt --check` stay clean.
- Every versioning-specific filter behavior is locked in by an end-to-end
  test that would catch a regression at the CLI boundary.

## Non-goals

- **Full filter-matrix coverage under `--all-versions`.** Step 7 already
  locks in filter semantics for regular (non-versioned) object listing.
  Retesting every filter under `--all-versions` would mostly duplicate
  coverage — a bug in a filter's size/regex/mtime predicate would fail both
  suites identically. This spec only covers behaviors that *differ* under
  versioning.
- **Non-current version storage-class coverage.** S3 allows different storage
  classes for different versions of the same key, but setting this via the
  CLI requires multiple PUT calls with different `--storage-class` flags
  *and* it's a rarely-used pattern in practice (users typically set a
  lifecycle rule instead). Deferred.
- **`--show-is-latest` display tests.** `--show-is-latest` is a column
  toggle, not a filter. Display-formatting tests are a separate feature
  area.
- **Combination / multi-filter AND tests under `--all-versions`.** The 7
  basic tests here plus the Step 7 combination tests cover the primary
  contracts. A full cross-product would triple the test count for
  diminishing returns.
- **Error-path tests.** Same rationale as Step 7: invalid argument
  rejection is unit-tested at the `build_config_from_args` layer.
- **Production code changes.** This suite uses the existing public API
  of `s3ls_rs` as-is.

---

## Architecture

### File layout

New file:

```
tests/e2e_filters_versioned.rs    # 7 test functions, cfg-gated
```

Modified file:

```
tests/common/mod.rs               # 2 new helpers
```

### Cfg gating

`tests/e2e_filters_versioned.rs` starts with `#![cfg(e2e_test)]`, matching
Step 6 and Step 7 patterns. Under normal `cargo test` it compiles to an
empty binary; only `RUSTFLAGS='--cfg e2e_test'` compiles and runs the test
bodies. `tests/common/mod.rs` stays un-gated.

### Framework reuse

All of the following are reused without modification:

- `TestHelper::new()`, `generate_bucket_name()`, `create_versioned_bucket()`
  (already exists at `tests/common/mod.rs:124`)
- `TestHelper::put_object`, `put_object_with_storage_class` (Step 1 helper)
- `BucketGuard::cleanup()` — already handles versioned buckets via
  `delete_all_versions` + `delete_all_objects`
- `TestHelper::run_s3ls(&args)` + `S3lsOutput`
- `e2e_timeout!()` macro, `E2E_TIMEOUT` constant

The existing `assert_json_keys_eq` and `assert_json_keys_or_prefixes_eq`
helpers are **not** used by this suite — they compare sets of keys, which
collapses multiple versions of the same key into a single entry. This suite
uses a new multiset-based helper instead (see below).

### New helpers in `tests/common/mod.rs`

Two additions:

#### 1. `TestHelper::create_delete_marker`

```rust
/// Create a delete marker on a versioned bucket by calling DeleteObject
/// without a VersionId. On a versioned bucket, S3 interprets this as
/// "add a delete marker" — the object appears deleted to non-versioned
/// readers, but all prior versions remain listable via ListObjectVersions.
///
/// Requires: the bucket must have versioning ENABLED. Call this only on
/// buckets created via `create_versioned_bucket`. On a non-versioned
/// bucket this call would permanently delete the object.
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

Placement: inside the existing `impl TestHelper` "Object operations" block,
after `put_object_with_storage_class` (Step 7 Task 1 helper).

#### 2. `assert_json_version_shapes_eq`

Free function at the bottom of `tests/common/mod.rs`, after the existing
`assert_json_keys_or_prefixes_eq`.

```rust
/// Parse NDJSON stdout from `s3ls --all-versions --json` and assert the
/// multiset of `(Key, is_delete_marker)` tuples equals `expected`.
///
/// Unlike `assert_json_keys_eq` (which compares a set of `Key` strings),
/// this helper:
/// 1. Extracts both `Key` and the `DeleteMarker` boolean field from each
///    JSON line (missing `DeleteMarker` field defaults to `false`).
/// 2. Uses multiset comparison: 3 rows of `("doc.txt", false)` + 1 row of
///    `("doc.txt", true)` is distinguishable from 2 rows of `("doc.txt",
///    false)` + 1 row of `("doc.txt", true)`.
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
);
```

**Why multiset and not set:** a key with 3 versions appears as 3 NDJSON
lines with the same `Key` field. A set would collapse them into one entry
and silently lose coverage of "how many versions survived the filter." The
multiset lets tests assert exact version counts per `(key, is_delete_marker)`
shape without needing to know `VersionId` strings.

**Why "missing DeleteMarker → false":** `src/aggregate.rs:625` emits
`DeleteMarker: true` only on delete-marker rows. Regular versioned object
rows have no `DeleteMarker` field. Defaulting to `false` on missing matches
the production output shape exactly.

### Filter semantics under versioning (verified against source)

These were resolved up-front so the spec can state exact expectations:

| Filter | Delete-marker handling | Source |
|---|---|---|
| `--filter-include-regex R` | filters DM by **key** (DM key matches or not) | `src/filters/include_regex.rs` |
| `--filter-exclude-regex R` | filters DM by **key** (inverse of include) | `src/filters/exclude_regex.rs` |
| `--filter-smaller-size N` | DM always passes (unconditional `Ok(true)` early return) | `src/filters/smaller_size.rs:25` |
| `--filter-larger-size N` | DM always passes (unconditional `Ok(true)` early return) | `src/filters/larger_size.rs:25` |
| `--filter-mtime-before T` | filters DM by its own `last_modified` (DMs have real timestamps) | `src/filters/mtime_before.rs:27` |
| `--filter-mtime-after T` | filters DM by its own `last_modified` | `src/filters/mtime_after.rs:27` |
| `--storage-class LIST` | DM always passes (unconditional match arm) | `src/filters/storage_class.rs:47` |

### `--hide-delete-markers` precedence (verified against source)

`src/lister.rs:48` applies `--hide-delete-markers` **before** the filter
chain at `src/lister.rs:51`. Order:

```rust
while let Some(entry) = list_rx.recv().await {
    if self.hide_delete_markers && entry.is_delete_marker() {
        continue;
    }
    match self.filter_chain.matches(&entry) {
        Ok(true) => { ... }
        ...
    }
}
```

Implication: when `--hide-delete-markers` is set, delete markers are
dropped before any filter runs. Tests that want to observe "delete markers
pass the filter" must NOT pass `--hide-delete-markers`.

### `--json` field shapes (verified against source)

- Regular versioned object: `{"Key": ..., "VersionId": ..., "IsLatest": ..., "Size": ..., "LastModified": ..., ...}` — no `DeleteMarker` field.
  (`src/aggregate.rs` — the `ListEntry::Object(S3Object::Versioning)` branch.)
- Delete marker: `{"Key": ..., "VersionId": ..., "IsLatest": ..., "LastModified": ..., "DeleteMarker": true, ...}` —
  exactly one `"DeleteMarker": true` field per DM row.
  (`src/aggregate.rs:625`.)

---

## Test organization

Seven test functions, each using the standard step-6 framework pattern:

```rust
#[tokio::test]
async fn e2e_versioned_<name>() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        // ... inline fixture ...
        // ... run_s3ls ...
        // ... assert_json_version_shapes_eq ...
    });

    _guard.cleanup().await;
}
```

Unlike the per-filter tests in Step 7, each of these tests makes ONE
`run_s3ls` invocation (or TWO, for Test 6 which asserts both with and
without `--hide-delete-markers`). No shared-fixture-within-a-test pattern —
each test has a single narrow scope.

---

## Test catalog

All fixtures are minimal and per-test (not shared across tests). Each test
creates a fresh versioned bucket.

### Test 1: `e2e_versioned_include_regex_drops_delete_marker`

Proves `--filter-include-regex` is applied to delete-marker keys — a DM
whose key matches the regex is kept, a DM whose key doesn't match is dropped.

**Fixture:**
- `put_object("keep.csv", 100 bytes)` → v1 of `keep.csv`
- `put_object("keep.csv", 200 bytes)` → v2 of `keep.csv`
- `put_object("drop.txt", 100 bytes)` → v1 of `drop.txt`
- `create_delete_marker("drop.txt")` → DM on `drop.txt`
- `create_delete_marker("keep.csv")` → DM on `keep.csv`

**Run:**
```
s3ls --recursive --all-versions --json --filter-include-regex '\.csv$'
```

**Expected (`assert_json_version_shapes_eq`, 3 rows):**
- `("keep.csv", false)` — v1
- `("keep.csv", false)` — v2
- `("keep.csv", true)` — the DM (passes because its key matches the regex)

**Non-matches:** `drop.txt` v1 fails the regex (not `.csv`); `drop.txt` DM fails the regex (key is `drop.txt`).

### Test 2: `e2e_versioned_exclude_regex_drops_delete_marker`

Proves `--filter-exclude-regex` is applied to delete-marker keys — a DM
whose key matches the exclude regex is dropped.

**Fixture:**
- `put_object("keep.bin", 100 bytes)` → v1
- `put_object("keep.bin", 200 bytes)` → v2
- `put_object("skip_me.bin", 100 bytes)` → v1
- `create_delete_marker("skip_me.bin")` → DM on `skip_me.bin`
- `create_delete_marker("keep.bin")` → DM on `keep.bin`

**Run:**
```
s3ls --recursive --all-versions --json --filter-exclude-regex '^skip_'
```

**Expected (3 rows):**
- `("keep.bin", false)` — v1
- `("keep.bin", false)` — v2
- `("keep.bin", true)` — DM (exclude regex doesn't match `keep.bin`, so it's kept)

**Non-matches:** `skip_me.bin` v1 and `skip_me.bin` DM both fail the exclude regex.

### Test 3: `e2e_versioned_size_filter_passes_delete_markers`

Locks in "delete markers always pass size filters" — verified against
`src/filters/smaller_size.rs:25` and `larger_size.rs:25`, both of which
unconditionally return `Ok(true)` for `ListEntry::DeleteMarker` before any
size comparison.

**Fixture:**
- `put_object("big.bin", 5000 bytes)` → v1
- `put_object("big.bin", 7000 bytes)` → v2
- `put_object("small.bin", 100 bytes)` → v1 (fails size)
- `create_delete_marker("small.bin")` → DM on `small.bin` (has no size)

**Run:**
```
s3ls --recursive --all-versions --json --filter-larger-size 1000
```

**Expected (3 rows):**
- `("big.bin", false)` — v1, 5000 ≥ 1000
- `("big.bin", false)` — v2, 7000 ≥ 1000
- `("small.bin", true)` — DM passes through despite `--filter-larger-size 1000`

**Non-matches:** `small.bin` v1 (100 < 1000).

**Does NOT use `--hide-delete-markers`** because the test's entire point
is to observe a delete marker surviving the filter — the hide flag would
strip the DM before the filter runs.

### Test 4: `e2e_versioned_mtime_filter_applies_to_delete_markers`

Locks in "mtime filters DO apply to delete-marker timestamps" — DMs have
a real `LastModified`, and `src/filters/mtime_before.rs:27` and
`mtime_after.rs:27` use `entry.last_modified()` uniformly for both objects
and DMs.

**Fixture (two-batch with sleep, same pattern as Step 7 `combo_all_seven`):**

1. `put_object("old.bin", 100 bytes)` → v1 of `old.bin`
2. `tokio::time::sleep(Duration::from_millis(1500)).await` — guaranteed 1-second gap
3. `put_object("new.bin", 100 bytes)` → v1 of `new.bin`
4. `create_delete_marker("old.bin")` → DM on `old.bin`, timestamped after the pivot

Read back all 3 rows via `list_object_versions`, compute
`t_pivot = min(batch_2_LastModified)` where batch 2 includes `new.bin` v1
and the `old.bin` DM. Sanity-assert `t_pivot > old.bin_v1.LastModified`.

**Run:**
```
s3ls --recursive --all-versions --json --filter-mtime-after <t_pivot_rfc3339>
```

**Expected (2 rows):**
- `("new.bin", false)` — batch 2, passes mtime-after
- `("old.bin", true)` — DM is in batch 2 (created after sleep), passes mtime-after based on the DM's own timestamp (NOT the original object's)

**Non-matches:** `old.bin` v1 (batch 1, before pivot).

This test proves three things at once:
1. Delete markers are subject to mtime filters (not blanket passthrough).
2. A DM's mtime is its own creation time, not the original object's.
3. The original version of a key still fails mtime-after even if a later DM on the same key passes.

### Test 5: `e2e_versioned_storage_class_passes_delete_markers`

Locks in "delete markers always pass storage-class filter" — verified
against `src/filters/storage_class.rs:47` (`ListEntry::DeleteMarker => Ok(true)`).

**Fixture:**
- `put_object_with_storage_class("ia.bin", 100 bytes, "STANDARD_IA")` → v1
- `create_delete_marker("ia.bin")` → DM on `ia.bin`
- `put_object("std.bin", 100 bytes)` → v1 (STANDARD default, reported as None by S3)

**Run:**
```
s3ls --recursive --all-versions --json --storage-class STANDARD
```

**Expected (2 rows):**
- `("std.bin", false)` — STANDARD (None → STANDARD per Step 7 Test 5 semantics)
- `("ia.bin", true)` — DM passes through

**Non-matches:** `ia.bin` v1 (STANDARD_IA, fails filter).

### Test 6: `e2e_versioned_hide_delete_markers`

Locks in `--hide-delete-markers` behavior. This test runs `s3ls` TWICE
against the same bucket to prove the flag makes a difference (guards
against a regression where the flag is silently ignored).

**Fixture:**
- `put_object("doc.txt", 100 bytes)` → v1
- `put_object("doc.txt", 200 bytes)` → v2
- `create_delete_marker("doc.txt")` → DM as the latest "version"

**Run 1 (with flag):**
```
s3ls --recursive --all-versions --hide-delete-markers --json
```

**Expected 1 (2 rows):**
- `("doc.txt", false)` — v1
- `("doc.txt", false)` — v2

**Run 2 (without flag, same bucket):**
```
s3ls --recursive --all-versions --json
```

**Expected 2 (3 rows):**
- `("doc.txt", false)` — v1
- `("doc.txt", false)` — v2
- `("doc.txt", true)` — DM

The "with flag" result having exactly one fewer row (the DM) than the
"without flag" result proves the flag strips DMs as documented.

### Test 7: `e2e_versioned_size_filter_per_version`

Locks in "size filters evaluate each version's own size" — the same key
with 3 different sizes across versions, only the middle version passes a
size filter.

**Fixture:**
- `put_object("growing.bin", 100 bytes)` → v1, small
- `put_object("growing.bin", 5000 bytes)` → v2, large
- `put_object("growing.bin", 200 bytes)` → v3, small again

**Run:**
```
s3ls --recursive --all-versions --json --filter-larger-size 1000
```

**Expected (1 row):**
- `("growing.bin", false)` — ONLY v2 (5000 bytes ≥ 1000) survives

**Non-matches:** v1 (100 < 1000) and v3 (200 < 1000) fail the size filter
on their own sizes.

This is the one test where the same key appears multiple times in the
fixture but NOT all versions survive. It proves filters evaluate each
version's metadata independently rather than treating all versions of a
key as a unit.

---

## Cost and runtime

- **7 tests × 1 versioned bucket each** = 7 bucket creates + 7 bucket deletes per run.
- **~18 `PutObject` calls** across all fixtures.
- **~6 `DeleteObject` (delete marker creation)** calls across tests that need DMs.
- **~8 `run_s3ls` subprocess invocations** (most tests do 1; Test 6 does 2).
- **Cleanup** uses existing `delete_bucket_cascade` → `delete_all_versions` → `delete_all_objects` — already handles versioned buckets.

At current S3 pricing: **well under $0.01 per run**. Runtime bounded by
`E2E_TIMEOUT = 60s` per test.

## Execution

```bash
# Run just the versioning suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters_versioned -- --nocapture

# Run a single test
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters_versioned e2e_versioned_include_regex_drops_delete_marker -- --nocapture

# Run all e2e tests (listing + filters + filters_versioned)
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

Each test owns a uniquely-named bucket (`s3ls-e2e-{uuid}`), so the default
parallel test runner is safe.

---

## Summary of new code

- **New file:** `tests/e2e_filters_versioned.rs` (~400 LOC, 7 tests, cfg-gated).
- **Modified file:** `tests/common/mod.rs`:
  - Add `TestHelper::create_delete_marker(bucket, key)` method.
  - Add `assert_json_version_shapes_eq(stdout, expected, label)` free function.
- **No production code changes.**
- **No `Cargo.toml` changes** — all dependencies reused from production.
- **No changes to existing tests** — `tests/e2e_filters.rs` and `tests/e2e_listing.rs` untouched.
