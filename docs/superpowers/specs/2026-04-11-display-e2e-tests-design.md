# Display E2E Tests — Design

**Date:** 2026-04-11
**Status:** Design (pending implementation plan)
**Builds on:**
- `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md` (e2e framework)
- `docs/superpowers/specs/2026-04-11-step7-filter-e2e-tests-design.md` (non-versioning filter tests)
- `docs/superpowers/specs/2026-04-11-versioned-filter-e2e-tests-design.md` (versioning filter tests)

---

## Goal

Add end-to-end test coverage for s3ls display functionality — the rendering
of listing output in both text and JSON formats, with and without the
various display flags. Every `--show-*` flag, `--header`, `--summarize`,
`--human-readable`, and `--show-relative-path` gets explicit coverage for
both output formats. Runs under `RUSTFLAGS='--cfg e2e_test' cargo test
--test e2e_display` against real AWS S3 using the `s3ls-e2e-test` profile.

After this step:

- `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_display` runs against
  real S3 and passes.
- `cargo test` (without the cfg flag) still passes — the new file compiles
  to an empty binary.
- `cargo clippy --all-features` and `cargo fmt --check` stay clean.
- Every display flag has positive (flag on) AND negative (flag off) coverage
  in at least one of the output formats where the flag meaningfully changes
  output.
- Row-type-specific formatting (PRE for `CommonPrefix`, DELETE for
  `DeleteMarker`) is locked in by dedicated tests that exercise multiple
  `--show-*` flags simultaneously.

## Non-goals

- **Exhaustive `--show-*` flag combinations** beyond the single combo test.
  Combinatorial explosion with diminishing returns.
- **`--json` + `--header` combinations.** These are mutually exclusive at
  config-parse time; no runtime behavior to test.
- **`--show-is-latest` without `--all-versions`.** Validated at config-parse
  time via clap's `requires` attribute.
- **Error-path tests.** Same rationale as earlier steps: invalid argument
  handling is unit-tested in `src/config/`.
- **Express One Zone directory bucket listing.** No ambient test bucket; a
  dedicated IAM setup would be required.
- **Production code changes.** Uses the existing public API of `s3ls_rs`
  as-is.

---

## Architecture

### File layout

New file:

```
tests/e2e_display.rs     # 17 test functions, cfg-gated
```

Modified file:

```
tests/common/mod.rs      # 6 new helpers
```

### Cfg gating

`tests/e2e_display.rs` starts with `#![cfg(e2e_test)]`. Under normal
`cargo test` it compiles to an empty binary; only
`RUSTFLAGS='--cfg e2e_test'` compiles and runs the test bodies. Matches the
pattern of `tests/e2e_filters.rs`, `tests/e2e_filters_versioned.rs`, and
`tests/e2e_listing.rs`.

### Framework reuse

Reused without modification:
- `TestHelper::new()`, `generate_bucket_name()`, `create_bucket()`,
  `create_versioned_bucket()`, `bucket_guard()`
- `TestHelper::put_object`, `put_object_with_storage_class`,
  `put_objects_parallel`, `create_delete_marker`
- `TestHelper::run_s3ls(&args)` + `S3lsOutput`
- `e2e_timeout!()` macro, `E2E_TIMEOUT` constant
- `BucketGuard::cleanup()`

### New helpers in `tests/common/mod.rs`

Six additions. The first goes inside the "Object operations" `impl
TestHelper` block; the others are free functions at the bottom of the file
after the existing assertion helpers.

#### 1. `TestHelper::put_object_with_checksum_algorithm`

```rust
/// Upload an object with an explicit S3 ChecksumAlgorithm. Used by display
/// tests that exercise `--show-checksum-algorithm` / `--show-checksum-type`
/// — the default PUT does not populate a checksum field, so tests that want
/// non-empty checksum columns must use this helper.
///
/// Accepts the algorithm as a string ("CRC32", "CRC32C", "SHA1", "SHA256",
/// "CRC64NVME"). Converts via `ChecksumAlgorithm::from(&str)` — same
/// pattern as `put_object_with_storage_class`.
pub async fn put_object_with_checksum_algorithm(
    &self,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    algorithm: &str,
);
```

Placement: inside the existing `impl TestHelper` "Object operations" block,
immediately after `create_delete_marker` (added by the versioning spec).

#### 2. `parse_tsv_line`

```rust
/// Split a tab-delimited line into its columns. Helper for display tests
/// that need to assert on specific column indices.
pub fn parse_tsv_line(line: &str) -> Vec<&str> {
    line.split('\t').collect()
}
```

Free function at the bottom of `tests/common/mod.rs`.

#### 3. `assert_header_columns`

```rust
/// Assert the first line of `stdout` (from `s3ls --header ...` in text
/// mode) is a tab-delimited header row with exactly the expected column
/// names in order. Panics with the label on mismatch.
///
/// Panics if:
/// - `stdout` has no lines (empty output),
/// - the first line's columns don't match `expected` exactly.
pub fn assert_header_columns(stdout: &str, expected: &[&str], label: &str);
```

#### 4. `assert_all_data_rows_have_columns`

```rust
/// Assert that every non-empty line of `stdout` has exactly
/// `expected_count` tab-separated columns. Catches missing-column or
/// extra-column regressions across every row at once.
///
/// Lines identified as the summary (starting with "Total:\t") are
/// EXCLUDED from the count check, since the summary has a different
/// column count than data rows.
///
/// The header row (if `--header` was used) has the same column count as
/// data rows, so it naturally passes the same check.
pub fn assert_all_data_rows_have_columns(
    stdout: &str,
    expected_count: usize,
    label: &str,
);
```

#### 5. `assert_summary_present_text`

```rust
/// Assert that `stdout` contains a text-mode summary line starting with
/// "Total:\t" and return it. The caller can then do further substring
/// assertions on its contents (e.g. contains the expected object count).
pub fn assert_summary_present_text(stdout: &str, label: &str) -> String;
```

#### 6. `assert_summary_present_json`

```rust
/// Assert that `stdout` contains a JSON summary line (an NDJSON line that
/// parses to an object with a top-level "Summary" key) and return the
/// parsed `serde_json::Value`. The caller can then do further field
/// assertions on its contents.
pub fn assert_summary_present_json(stdout: &str, label: &str) -> serde_json::Value;
```

**Why no `serde_json` dep change:** already a direct production dependency
(`Cargo.toml:19`), available to test code.

### Display semantics verified against source

Verified against `src/aggregate.rs:315-476` (`format_entry`, `format_header`)
and `src/aggregate.rs:514-642` (`format_entry_json`), plus the pipeline
wiring at `src/pipeline.rs:177-178` and the S3 request layer at
`src/storage/s3/mod.rs:68-179`.

#### Text mode column order

Optional columns are inserted in this exact order:

```
DATE                         always
SIZE                         always (or "PRE" / "DELETE")
STORAGE_CLASS                if --show-storage-class
ETAG                         if --show-etag
CHECKSUM_ALGORITHM           if --show-checksum-algorithm
CHECKSUM_TYPE                if --show-checksum-type
VERSION_ID                   if --all-versions
IS_LATEST                    if --show-is-latest (requires --all-versions)
OWNER_DISPLAY_NAME           if --show-owner (col 1/2)
OWNER_ID                     if --show-owner (col 2/2)
IS_RESTORE_IN_PROGRESS       if --show-restore-status (col 1/2)
RESTORE_EXPIRY_DATE          if --show-restore-status (col 2/2)
KEY                          always
```

#### Row-type handling

- **Object rows:** populate every column from the object metadata.
- **CommonPrefix rows** (`src/aggregate.rs:319-356`): DATE empty, SIZE =
  `"PRE"`, every optional column empty, KEY = the prefix string.
- **DeleteMarker rows** (`src/aggregate.rs:396-438`): DATE populated,
  SIZE = `"DELETE"`, STORAGE_CLASS/ETAG/CHECKSUM* empty, VERSION_ID
  populated, IS_LATEST optional, Owner populated if fetched,
  IS_RESTORE_IN_PROGRESS/RESTORE_EXPIRY_DATE empty (DMs have no restore
  status), KEY populated.

#### JSON mode field contracts

- **Every field is always emitted when the underlying data is populated.**
  `--show-*` flags don't gate JSON field emission — they gate WHETHER s3ls
  fetches the data from S3 in the first place.
- **`--show-owner` under non-versioned listing** (`--recursive`) sets
  `fetch_owner=true` on `ListObjectsV2`. Without the flag, S3 doesn't
  return owner data, so the JSON `Owner` field is absent.
  (`src/storage/s3/mod.rs:98-100`, `src/pipeline.rs:177`)
- **`--show-owner` under `--all-versions`** has no effect because
  `ListObjectVersions` always returns owner. The JSON `Owner` field is
  always present under `--all-versions` regardless of the flag.
  (`src/storage/s3/mod.rs:174` — inline comment in source)
- **`--show-restore-status`** sets `OptionalObjectAttributes=RestoreStatus`
  on both `ListObjectsV2` and `ListObjectVersions`. Without the flag, S3
  doesn't return restore-status data, so the JSON `RestoreStatus` field is
  absent. (`src/storage/s3/mod.rs:101-105`, `src/storage/s3/mod.rs:176-179`,
  `src/pipeline.rs:178`)
- **All other `--show-*` flags** (`--show-etag`, `--show-storage-class`,
  `--show-checksum-algorithm`, `--show-checksum-type`, `--show-is-latest`,
  `--show-relative-path`) do NOT affect whether the JSON field is emitted.
  The underlying data is fetched regardless. The JSON test sub-assertions
  for these flags verify that the relevant field is present in JSON
  (a baseline "JSON keeps emitting this") rather than "flag on vs off
  changes the field's presence".
- **`--show-relative-path`** DOES affect both text and JSON key/prefix
  rendering via `format_key_display` (`src/aggregate.rs:394, 528, 614,
  520`).

#### Summary line format

- **Text mode:** `"Total:\t{count}\tobjects\t{size_num}\t{size_unit}"`, with
  `\t{dm_count}\tdelete markers` appended if `--all-versions`.
  (`src/aggregate.rs:711-719`)
- **Text mode + `--human-readable`:** size_num/size_unit are the split form
  (`"1.95"`, `"KiB"`). Non-human mode uses numeric bytes and `"bytes"` as
  unit. (`src/aggregate.rs:706-710`)
- **JSON mode:** a final NDJSON line `{"Summary":{"TotalObjects":N,
  "TotalSize":M,"TotalDeleteMarkers":K}}`. The `TotalDeleteMarkers` field
  is present only under `--all-versions`. (`src/aggregate.rs:689-704`)

#### Bucket listing display

Source: `src/bucket_lister.rs:53-148`.

- **Text mode columns:** `DATE\tREGION\tBUCKET\t[BUCKET_ARN]\t[OWNER_DISPLAY_NAME\tOWNER_ID]`
- **JSON mode fields:** `{"Name", "CreationDate", "BucketRegion",
  "BucketArn"?, "Owner"?}` (per bucket, one NDJSON line each).
- **`--header`** supported (same gating as object listing).
- **`--summarize` NOT supported** — `format_summary` is never called from
  `bucket_lister.rs`.
- **`--show-bucket-arn`** — sourced at request time and attached to the
  listing entry (`entry.bucket_arn`). Without the flag, `entry.bucket_arn`
  is None and neither the text BUCKET_ARN column nor the JSON `BucketArn`
  field is emitted.
- **`--show-owner`** — similar wiring; without the flag, neither the text
  OWNER columns nor the JSON `Owner` object is emitted.

---

## Test organization

Three distinct shapes depending on test category.

### Shape A: per-flag tests (Tests 1-8 and 16-17)

Each test creates one bucket, uploads a minimal fixture, and makes 3
`run_s3ls` invocations against the same bucket:

1. **Text sub-assertion, flag ON** — `--header --show-<flag>` verifies the
   header row contains the expected column names in order, and every data
   row has the expected column count.
2. **Text sub-assertion, flag OFF** — `--header` only; verifies the header
   row does NOT contain the flag-specific columns (column count drops).
3. **JSON sub-assertion** — `--json` verifies the relevant field's
   presence or absence. For most flags, the field is always present in
   JSON when data is populated (so this is a smoke assertion). For
   `--show-owner` and `--show-restore-status`, the JSON assertion is
   meaningful because the underlying data is fetched only when the flag
   is set — these two tests have **4 sub-assertions** (3 text + 1 extra
   JSON showing field absence without the flag AND presence with it; the
   "with flag" run replaces the third sub-assertion above).

Standard scaffolding:

```rust
#[tokio::test]
async fn e2e_display_show_<name>() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // ... fixture (1-2 objects) ...

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(), "--recursive", "--header", "--show-<name>",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(&output.stdout, &[...], "<label>: text on");
        assert_all_data_rows_have_columns(&output.stdout, N, "<label>: text on");

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[
            target.as_str(), "--recursive", "--header",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(&output.stdout, &["DATE", "SIZE", "KEY"], "<label>: text off");

        // Sub-assertion 3: JSON
        let output = TestHelper::run_s3ls(&[
            target.as_str(), "--recursive", "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let line = output.stdout.lines().next().expect("empty JSON");
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(v.get("<field>").is_some(), "<label>: JSON field missing");
    });

    _guard.cleanup().await;
}
```

### Shape B: row-type tests (Tests 10, 11) and combo test (Test 9)

One `run_s3ls` invocation each, with multiple `--show-*` flags at once:

- **Test 9 (`all_show_flags_combined`):** asserts the full 11-column header
  + every object row has 11 columns.
- **Test 10 (`common_prefix_row`):** fixture has both a deep object (to
  produce a PRE at depth boundary) and a top-level object, run with
  `--max-depth 1 --header --show-etag --show-storage-class --show-owner`,
  assert PRE rows have the same column count as object rows with correct
  cells empty.
- **Test 11 (`delete_marker_row`):** versioned bucket with one object + one
  DM, run with `--all-versions --header --show-etag --show-storage-class
  --show-owner`, assert DELETE rows have the correct shape.

### Shape C: summarize / human-readable / relative-path / bucket tests

- **Test 12 (`summarize_objects`):** 3 sub-assertions — text, text+human,
  json. Verifies summary content.
- **Test 13 (`summarize_versioned`):** 1 sub-assertion — text with
  `--all-versions` verifying delete-marker count in summary.
- **Test 14 (`human_readable`):** 1 sub-assertion — object row's SIZE column
  contains a human-readable token.
- **Test 15 (`show_relative_path_prefixed`):** 2 sub-assertions — text +
  json, both showing the key rendered relative to a `s3://bucket/prefix/`
  target.
- **Test 16 (`bucket_listing_show_bucket_arn`):** 3 sub-assertions — text
  flag on, text flag off, json flag on/off (using field presence check).
- **Test 17 (`bucket_listing_show_owner`):** same shape as 16.

---

## Test catalog

All fixtures are minimal and per-test. Each test creates a fresh bucket
except where noted.

### Test 1: `e2e_display_show_storage_class`

**Fixture:** 1 object `file.txt` (100 B, default STANDARD class via plain `put_object`).

**Sub-assertion 1 (text on):** `--recursive --header --show-storage-class`
- Header: `["DATE", "SIZE", "STORAGE_CLASS", "KEY"]`
- Every row has 4 columns
- `stdout.contains("file.txt")`

**Sub-assertion 2 (text off):** `--recursive --header`
- Header: `["DATE", "SIZE", "KEY"]` (3 columns, no STORAGE_CLASS)

**Sub-assertion 3 (JSON):** `--recursive --json`
- First NDJSON line parses
- JSON value has `"Key"` and `"Size"` fields present
- `"StorageClass"` may or may not be present depending on S3 (None → absent
  per `aggregate.rs:556-560`). This test does NOT assert `StorageClass`
  presence in JSON — it's noise because the field is dependent on S3's
  response, not the flag. Instead, this test asserts only that the JSON
  parses cleanly and has the mandatory fields.

### Test 2: `e2e_display_show_etag`

**Fixture:** 1 object `file.txt` (100 B).

**Sub-assertion 1 (text on):** `--recursive --header --show-etag`
- Header: `["DATE", "SIZE", "ETAG", "KEY"]`
- Every row has 4 columns

**Sub-assertion 2 (text off):** `--recursive --header`
- Header: `["DATE", "SIZE", "KEY"]`

**Sub-assertion 3 (JSON):** `--recursive --json`
- JSON has `"ETag"` field present (always present for regular objects).

### Test 3: `e2e_display_show_checksum_algorithm`

**Fixture:** 1 object `file.txt` (100 B) uploaded via
`put_object_with_checksum_algorithm(&bucket, "file.txt", body, "CRC32")`
so S3 records the checksum.

**Sub-assertion 1 (text on):** `--recursive --header --show-checksum-algorithm`
- Header: `["DATE", "SIZE", "CHECKSUM_ALGORITHM", "KEY"]`
- CHECKSUM_ALGORITHM column (index 2) in the data row contains `"CRC32"`.

**Sub-assertion 2 (text off):** `--recursive --header`
- Header: `["DATE", "SIZE", "KEY"]`

**Sub-assertion 3 (JSON):** `--recursive --json`
- JSON has `"ChecksumAlgorithm"` field (always emitted when non-empty per
  `aggregate.rs:538-548`), and the array contains `"CRC32"`.

### Test 4: `e2e_display_show_checksum_type`

**Fixture:** same as Test 3 — the checksum type field is populated
automatically by S3 when a checksum algorithm is specified on the upload.

**Sub-assertion 1 (text on):** `--recursive --header --show-checksum-type`
- Header: `["DATE", "SIZE", "CHECKSUM_TYPE", "KEY"]`

**Sub-assertion 2 (text off):** `--recursive --header`
- Header: `["DATE", "SIZE", "KEY"]`

**Sub-assertion 3 (JSON):** `--recursive --json`
- JSON has `"ChecksumType"` field present.

### Test 5: `e2e_display_show_is_latest`

**Fixture:** versioned bucket, 2 versions of `file.txt` (different sizes).

**Sub-assertion 1 (text on):** `--recursive --all-versions --header --show-is-latest`
- Header: `["DATE", "SIZE", "VERSION_ID", "IS_LATEST", "KEY"]`
- Every row has 5 columns
- Some row's IS_LATEST column (index 3) contains `"LATEST"`; some contains `"NOT_LATEST"`.

**Sub-assertion 2 (text off):** `--recursive --all-versions --header`
- Header: `["DATE", "SIZE", "VERSION_ID", "KEY"]` (4 columns, no IS_LATEST)

**Sub-assertion 3 (JSON):** `--recursive --all-versions --json`
- First NDJSON line has `"VersionId"` AND `"IsLatest"` fields.

### Test 6: `e2e_display_show_owner` (4 sub-assertions)

**Fixture:** 1 object `file.txt` (100 B). NOT versioned — `--show-owner`
only has observable effect under non-versioned listing.

**Sub-assertion 1 (text on):** `--recursive --header --show-owner`
- Header: `["DATE", "SIZE", "OWNER_DISPLAY_NAME", "OWNER_ID", "KEY"]`
- OWNER_ID column (index 3) is non-empty for the object row.

**Sub-assertion 2 (text off):** `--recursive --header`
- Header: `["DATE", "SIZE", "KEY"]`

**Sub-assertion 3 (JSON off):** `--recursive --json`
- `"Owner"` field is ABSENT from the JSON (fetch_owner was not set, S3
  didn't return owner data).

**Sub-assertion 4 (JSON on):** `--recursive --json --show-owner`
- `"Owner"` field is PRESENT in the JSON with a non-empty `"ID"` subfield.

### Test 7: `e2e_display_show_restore_status` (4 sub-assertions)

**Fixture:** 1 object `file.txt` (100 B).

**Sub-assertion 1 (text on):** `--recursive --header --show-restore-status`
- Header: `["DATE", "SIZE", "IS_RESTORE_IN_PROGRESS", "RESTORE_EXPIRY_DATE", "KEY"]`
- Both restore columns (indices 2 and 3) are empty strings for the object
  row (no restore ever triggered).

**Sub-assertion 2 (text off):** `--recursive --header`
- Header: `["DATE", "SIZE", "KEY"]`

**Sub-assertion 3 (JSON off):** `--recursive --json`
- `"RestoreStatus"` field is ABSENT from the JSON.

**Sub-assertion 4 (JSON on):** `--recursive --json --show-restore-status`
- `"RestoreStatus"` field is still absent because the object is not a
  Glacier-class object and there's nothing to restore. This is NOT a bug —
  `src/aggregate.rs:584` only emits `RestoreStatus` if
  `is_restore_in_progress.is_some()`, and S3 won't return that for a
  STANDARD object even when `OptionalObjectAttributes=RestoreStatus` is
  set. The test asserts: the flag is accepted, s3ls runs successfully,
  and `"RestoreStatus"` is absent. This is a "flag doesn't crash" assertion
  rather than a "flag populates the field" one.

**Why this asymmetry with `--show-owner`:** Triggering a real Glacier
restore inside an e2e test would require either a Glacier object (90+ day
minimum billing — out of scope) or a convoluted lifecycle rule. The
honest test is "flag works, JSON field remains correctly absent for
non-restored objects".

### Test 8: `e2e_display_show_relative_path`

**Fixture:** 1 object `file.txt` at the BUCKET ROOT (no prefix). This test
exercises the flag's existence and baseline behavior; Test 15 covers the
prefixed-target case where the relative path actually differs from the
full key.

**Sub-assertion 1 (text on):** `--recursive --header --show-relative-path`
- Header: `["DATE", "SIZE", "KEY"]` (3 columns — the flag doesn't add a
  column)
- KEY column contains `"file.txt"` (same as non-relative since the target
  has no prefix)

**Sub-assertion 2 (text off):** `--recursive --header`
- Same header and KEY content — at bucket root, the flag is a no-op.

**Sub-assertion 3 (JSON):** `--recursive --json --show-relative-path`
- JSON `"Key"` field is `"file.txt"`.

### Test 9: `e2e_display_all_show_flags_combined`

**Fixture:** 1 object `file.txt` (100 B) uploaded with
`put_object_with_checksum_algorithm(..., "CRC32")` so checksum fields are
populated.

**One run (text):**
```
--recursive --header --show-storage-class --show-etag \
  --show-checksum-algorithm --show-checksum-type \
  --show-owner --show-restore-status
```

**Assertion:** the header is EXACTLY (in this order, no VERSION_ID because
not `--all-versions`):
```
["DATE", "SIZE", "STORAGE_CLASS", "ETAG", "CHECKSUM_ALGORITHM",
 "CHECKSUM_TYPE", "OWNER_DISPLAY_NAME", "OWNER_ID",
 "IS_RESTORE_IN_PROGRESS", "RESTORE_EXPIRY_DATE", "KEY"]
```

Every data row has 11 columns. The object row's cells for
`OWNER_ID`, `ETAG`, and `CHECKSUM_ALGORITHM` are non-empty.

### Test 10: `e2e_display_common_prefix_row`

**Fixture:** 2 objects — `top.txt` at bucket root, `logs/2025/a.log` at
depth 2.

**One run (text):**
```
--recursive --max-depth 1 --header --show-etag --show-storage-class --show-owner
```

**Assertion:**
- Header: `["DATE", "SIZE", "STORAGE_CLASS", "ETAG", "OWNER_DISPLAY_NAME", "OWNER_ID", "KEY"]` (7 columns)
- Every data row has 7 columns
- Some row has `SIZE = "PRE"` and `KEY = "logs/"` (the common prefix at the depth-1 boundary), with STORAGE_CLASS / ETAG / OWNER_* cells empty
- Another row has non-empty `SIZE` (the `top.txt` file) and non-empty optional cells

### Test 11: `e2e_display_delete_marker_row`

**Fixture:** versioned bucket, 1 object `file.txt` (100 B), 1 delete marker
on `file.txt`.

**One run (text):**
```
--recursive --all-versions --header --show-etag --show-storage-class --show-owner
```

**Assertion:**
- Header: `["DATE", "SIZE", "STORAGE_CLASS", "ETAG", "VERSION_ID", "OWNER_DISPLAY_NAME", "OWNER_ID", "KEY"]` (8 columns — `--all-versions` adds VERSION_ID)
- Every data row has 8 columns
- Some row has `SIZE = "DELETE"` with STORAGE_CLASS / ETAG cells empty,
  VERSION_ID populated, OWNER_* populated (ListObjectVersions always
  returns owner per source)
- Another row has non-empty SIZE (the object version)

### Test 12: `e2e_display_summarize_objects`

**Fixture:** 3 objects, each 1000 bytes. Total size = 3000 bytes.

**Sub-assertion 1 (text, no human):** `--recursive --summarize`
- `assert_summary_present_text` returns a line starting with `"Total:"`
- Line contains `"3"` (count), `"3000"` (size), `"bytes"` (unit), and
  `"objects"` (label)

**Sub-assertion 2 (text, human-readable):** `--recursive --summarize --human-readable`
- Summary line contains `"3"` and some non-byte unit (verify it does NOT
  contain `"3000"` as the size literal AND does NOT contain `" bytes"` —
  human mode renders `"2.93 KiB"` or similar). The exact format depends on
  `byte-unit` crate behavior; the test asserts the structure, not the
  decimal precision.

**Sub-assertion 3 (JSON):** `--recursive --summarize --json`
- `assert_summary_present_json` returns a value with
  `.Summary.TotalObjects == 3` and `.Summary.TotalSize == 3000`

### Test 13: `e2e_display_summarize_versioned`

**Fixture:** versioned bucket, 2 versions of `doc.txt` (100 B, 200 B), 1
delete marker.

**One run:** `--recursive --all-versions --summarize`

**Assertion:**
- Summary line starts with `"Total:"`
- Line contains `"2"` (object count — 2 live object versions), `"300"`
  (total size = 100 + 200), and `"delete markers"` (label), and `"1"`
  appearing after `"delete markers"` … actually the format is `"Total:\t2\tobjects\t300\tbytes\t1\tdelete markers"`.
- Simpler assertion: the returned summary line contains `"delete markers"`
  and a `"1"` preceding that substring. Specific byte-level match is
  brittle; we assert the key tokens are present.

### Test 14: `e2e_display_human_readable`

**Fixture:** 1 object `file.txt` at exactly 2000 bytes.

**One run (text):** `--recursive --human-readable`

**Assertion:**
- The output contains `"file.txt"`
- The output contains `"KiB"` as a substring (because `byte-unit`'s
  human-readable form for 2000 bytes is `"1.95 KiB"` using decimal base
  — actually the test uses 2048 bytes to guarantee an integer `"2 KiB"`
  match. See revision note below.)

**Revision:** change the fixture to **2048 bytes** (not 2000) so the
human-readable rendering is `"2 KiB"` exactly. The test then asserts:
```
output.stdout.contains("2 KiB") || output.stdout.contains("2.00 KiB")
```
This tolerates either precision format the `byte-unit` crate might emit.

### Test 15: `e2e_display_show_relative_path_prefixed`

**Fixture:** 1 object at key `data/foo.txt`. Target is `s3://bucket/data/`.

**Sub-assertion 1 (text):** `--recursive --header --show-relative-path s3://bucket/data/`
- Last column of the data row is `"foo.txt"` (NOT `"data/foo.txt"`)

**Sub-assertion 2 (JSON):** `--recursive --json --show-relative-path s3://bucket/data/`
- The NDJSON line's `"Key"` field is `"foo.txt"`

### Test 16: `e2e_display_bucket_listing_show_bucket_arn`

**Fixture:** one test bucket created (any name — the listing is account-global).

**Sub-assertion 1 (text on):** `s3ls --header --show-bucket-arn` (no target argument → bucket listing mode)
- Header contains `"BUCKET_ARN"` as a column (not asserting exact full
  header — account may have a lot of buckets with owner columns etc., we
  just check the BUCKET_ARN string appears in the header line)
- The created test bucket's row has a non-empty ARN cell

**Sub-assertion 2 (text off):** `s3ls --header` (bucket listing, no arn flag)
- Header does NOT contain `"BUCKET_ARN"`

**Sub-assertion 3 (JSON on):** `s3ls --json --show-bucket-arn`
- Finding the NDJSON line whose `"Name"` matches the created test bucket,
  verify `"BucketArn"` field is present and non-empty.

**Sub-assertion 4 (JSON off):** `s3ls --json`
- The created test bucket's NDJSON line does NOT have a `"BucketArn"` field.

### Test 17: `e2e_display_bucket_listing_show_owner`

Same structure as Test 16 but for `--show-owner` / `Owner` field. Account
owner is deterministic enough to assert "owner object is present / non-empty"
without asserting the specific `"ID"` value.

**Caveat for tests 16 and 17:** the created test bucket's name is known
(via `generate_bucket_name`), so the tests filter the listing output for
that specific bucket name rather than asserting on total row counts. The
AWS account may have other buckets — that's fine as long as the assertion
is scoped to the test bucket.

---

## Cost and runtime

- **17 tests × 1 bucket each** = 17 bucket creates + 17 bucket deletes per
  run (a handful are versioned buckets).
- **~30-40 `PutObject` calls total** across all fixtures.
- **~2-4 `create_delete_marker`** calls (Tests 11, 13).
- **~45-50 `run_s3ls` subprocess invocations** across all tests. Breakdown:
  - Tests 1-5, 8 (6 tests × 3 sub-assertions) = 18
  - Tests 6, 7 (2 tests × 4 sub-assertions) = 8
  - Test 9 (1 run) = 1
  - Tests 10, 11 (2 runs) = 2
  - Test 12 (3 sub-assertions) = 3
  - Test 13 (1 run) = 1
  - Test 14 (1 run) = 1
  - Test 15 (2 sub-assertions) = 2
  - Tests 16, 17 (2 tests × 4 sub-assertions) = 8
  - **Total = ~44 invocations**
- **Bucket listing tests (16, 17)** don't upload objects — they rely on the
  bucket-creation side effect of `bucket_guard`/`create_bucket` to
  guarantee at least one bucket exists in the account.

At current S3 pricing: **well under $0.02 per run**. Runtime bounded by
`E2E_TIMEOUT = 60s` per test.

## Execution

```bash
# Just the display suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_display -- --nocapture

# All e2e tests (listing + filters + filters_versioned + display)
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

Each test owns a uniquely-named bucket (`s3ls-e2e-{uuid}`), so the default
parallel test runner is safe. Bucket listing tests (16, 17) list the
entire account but scope their assertions to their own test bucket's name.

---

## Summary of new code

- **New file:** `tests/e2e_display.rs` (~700-900 LOC, 17 tests, cfg-gated).
- **Modified file:** `tests/common/mod.rs`:
  - Add `TestHelper::put_object_with_checksum_algorithm(bucket, key, body, algorithm)` method.
  - Add `parse_tsv_line(line) -> Vec<&str>` free function.
  - Add `assert_header_columns(stdout, expected, label)` free function.
  - Add `assert_all_data_rows_have_columns(stdout, expected_count, label)` free function.
  - Add `assert_summary_present_text(stdout, label) -> String` free function.
  - Add `assert_summary_present_json(stdout, label) -> serde_json::Value` free function.
- **No production code changes.**
- **No `Cargo.toml` changes** — all dependencies reused from production.
- **No changes to existing tests** — `tests/e2e_listing.rs`,
  `tests/e2e_filters.rs`, and `tests/e2e_filters_versioned.rs` untouched.
