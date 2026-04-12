# Step 7: Filter E2E Tests — Design

**Date:** 2026-04-11
**Status:** Implemented. Key deviations from design: mtime tests rewritten to use sequential uploads with 1.5s sleeps instead of parallel uploads with BTreeSet/conditional skip. See `tests/e2e_filters.rs` for final state.
**Builds on:** `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md`
(the e2e framework from step 6 is reused without modification beyond one
added assertion helper).

---

## Goal

Add end-to-end test coverage for every filter flag in s3ls, plus their
combinations, plus two orthogonal-flag interaction smoke tests. Runs under
`RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters` against real AWS
S3, using the `s3ls-e2e-test` profile established in step 6.

After this step:

- `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters` runs against real
  S3 and passes.
- `cargo test` (without the cfg flag) still passes with no e2e tests compiled
  in.
- `cargo clippy --all-features` and `cargo fmt --check` stay clean.
- Every filter flag has positive, negative, and boundary coverage.
- All seven filters are verified to compose correctly via one
  "all-seven-filters" test, plus four targeted pair tests.
- `CommonPrefix` passthrough under `--max-depth` and filter application under
  `--no-sort` are locked in by smoke tests.

## Non-goals

- **`--all-versions` interaction.** Filter + version listing has its own
  nuances (delete markers have no size / no storage class) that deserve a
  separate design pass. Deferred to a later step.
- **Error-path tests.** Invalid regex, malformed time, invalid size suffix,
  unknown storage class — these fail at `Config` build time via clap and
  `build_config_from_args`, and belong in `src/config/` unit tests, not in an
  e2e suite that spins up real S3 buckets.
- **Full `--max-depth` / `--no-sort` matrix.** Only two smoke tests land here;
  the full listing/streaming matrix is a separate e2e suite.
- **CI integration.** The existing GitHub Actions workflows run `cargo test`
  without the cfg flag, so e2e tests stay invisible to CI.
- **Production code changes.** The suite uses the existing public API of
  `s3ls_rs` as-is. The only source change is one added helper function in
  `tests/common/mod.rs`.
- **GLACIER / DEEP_ARCHIVE storage classes.** They have 90-180 day minimum
  storage billing that makes them unsafe for CI-style test objects.

---

## Architecture

### File layout

New file:

```
tests/e2e_filters.rs     # ~14 test functions, cfg-gated
```

Modified file:

```
tests/common/mod.rs      # one added helper: assert_json_keys_eq
```

### Cfg gating

`tests/e2e_filters.rs` starts with `#![cfg(e2e_test)]`, matching the step 6
pattern. Under normal `cargo test` it compiles to an empty binary; under
`RUSTFLAGS='--cfg e2e_test'` it compiles and runs normally. `tests/common/mod.rs`
is NOT gated (it is imported from gated files and only compiles when they do).

### Framework reuse

All of the following are reused without modification:

- `TestHelper::new()`, `generate_bucket_name()`, `create_bucket()`,
  `put_objects_parallel()`, `bucket_guard()`, `list_objects()`
- `BucketGuard::cleanup()`
- `TestHelper::run_s3ls(&args)` + `S3lsOutput`
- `e2e_timeout!()` macro, `E2E_TIMEOUT` constant

The existing `assert_key_order` helper stays in place for sort tests but is
not used here — filter tests assert on **sets**, not order.

### New helper: `assert_json_keys_eq`

Added to `tests/common/mod.rs`:

```rust
/// Parse NDJSON stdout from `s3ls --json` and assert the set of `Key` fields
/// equals `expected`. `label` is included in the panic message so multi-
/// assertion tests can identify which sub-case failed.
///
/// Panics if:
/// - any non-empty line fails to parse as JSON,
/// - any JSON line is missing the `Key` field (callers that expect
///   `CommonPrefix` entries should use `assert_json_keys_or_prefixes_eq`),
/// - the resulting set of keys does not equal `expected`.
pub fn assert_json_keys_eq(stdout: &str, expected: &[&str], label: &str);
```

A second helper, `assert_json_keys_or_prefixes_eq`, accepts JSON lines with
either a `"Key"` or `"Prefix"` field. This is used by the `--max-depth` smoke
test, where `s3ls --json` emits `{"Prefix": "logs/"}` for `CommonPrefix`
entries and `{"Key": "readme.csv", ...}` for objects (verified against
`src/aggregate.rs:514`).

**Dependency note:** `serde_json` is already a direct production dependency
(`Cargo.toml:19`). Test code in the `tests/` directory has access to all
crate dependencies — no `[dev-dependencies]` change is required.

### Filter semantics (verified against source)

These were resolved up-front so the spec can state exact boundary expectations
rather than hedging:

| Filter | Semantics | Source |
|---|---|---|
| `--filter-smaller-size N` | strict `size < N` | `src/filters/smaller_size.rs:29` |
| `--filter-larger-size N` | inclusive `size >= N` | `src/filters/larger_size.rs:29` |
| `--filter-mtime-before T` | strict `last_modified < T` | `src/filters/mtime_before.rs:27` |
| `--filter-mtime-after T` | inclusive `last_modified >= T` | `src/filters/mtime_after.rs:27` |
| `--storage-class LIST` | exact string match, with `None` treated as `"STANDARD"` | `src/filters/storage_class.rs:33` |
| `--filter-include-regex R` | `fancy-regex::Regex::is_match(key)` (no implicit anchoring) | `src/filters/include_regex.rs` |
| `--filter-exclude-regex R` | inverse of include | `src/filters/exclude_regex.rs` |

`CommonPrefix` entries always pass through the `FilterChain`
(`src/filters/mod.rs:37`) — this is what the max-depth smoke test locks in.

Delete markers also pass through size and mtime filters, but delete markers
are only produced under `--all-versions`, which is out of scope for this step.

---

## Test organization

Fourteen test functions, grouped into three categories:

1. **Per-filter tests (7)** — one function per filter, each using a shared
   fixture with three to four sub-assertions (match, no-match, boundary).
2. **Combination tests (5)** — one "all seven filters" test plus four pair
   tests. Each has its own bucket and fixture.
3. **Orthogonal-flag smoke tests (2)** — one for `--max-depth` + include-regex
   (locks `CommonPrefix` passthrough), one for `--no-sort` + larger-size
   (locks filter application under streaming mode).

The per-filter category uses **shared-fixture-within-a-test**: one
`#[tokio::test]` function creates one bucket, uploads one fixture, and makes
multiple `run_s3ls` invocations with different args. This minimizes bucket
create/delete round-trips (one per filter rather than one per case) while
still catching match/no-match/boundary regressions separately via labeled
assertions.

The combination and smoke categories use **one test per scenario** because
each scenario needs a different fixture anyway — there is no fixture to
share across combination tests.

### Standard test shape (per-filter)

```rust
#[tokio::test]
async fn e2e_filter_<name>() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload fixture via put_objects_parallel or individual put_object calls.
        // ...

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match case
        let output = TestHelper::run_s3ls(&[
            target.as_str(), "--recursive", "--json",
            "--filter-<name>", "<match-pattern>",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &["expected_key_1", "expected_key_2"], "<name>: match");

        // Sub-assertion 2: no-match case
        // ...

        // Sub-assertion 3: boundary case
        // ...
    });

    _guard.cleanup().await;
}
```

The same boilerplate (helper, bucket, guard, e2e_timeout, cleanup) applies to
combination and smoke tests.

---

## Test catalog

### Per-filter tests (7 functions)

#### `e2e_filter_include_regex`

**Fixture:** `report.csv`, `data.csv`, `summary.txt`, `archive.tar.gz`, `notes.md`

| Label | Args | Expected keys |
|---|---|---|
| `include-regex: match \.csv$` | `--filter-include-regex '\.csv$'` | `{report.csv, data.csv}` |
| `include-regex: no match \.xlsx$` | `--filter-include-regex '\.xlsx$'` | `{}` |
| `include-regex: anchor ^data` | `--filter-include-regex '^data'` | `{data.csv}` |
| `include-regex: .* passes all` | `--filter-include-regex '.*'` | all 5 keys |

#### `e2e_filter_exclude_regex`

**Fixture:** same as include-regex.

| Label | Args | Expected keys |
|---|---|---|
| `exclude-regex: match \.csv$` | `--filter-exclude-regex '\.csv$'` | `{summary.txt, archive.tar.gz, notes.md}` |
| `exclude-regex: no match \.xlsx$` | `--filter-exclude-regex '\.xlsx$'` | all 5 keys |
| `exclude-regex: .* excludes all` | `--filter-exclude-regex '.*'` | `{}` |

#### `e2e_filter_smaller_size`

**Fixture:** `tiny.bin` (10 B), `small.bin` (1000 B), `medium.bin` (10_000 B), `large.bin` (100_000 B).

Since `smaller-size` is strict `<`:

| Label | Args | Expected keys |
|---|---|---|
| `smaller-size: match 5000` | `--filter-smaller-size 5000` | `{tiny.bin, small.bin}` |
| `smaller-size: no match 1` | `--filter-smaller-size 1` | `{}` |
| `smaller-size: boundary 1000 strict` | `--filter-smaller-size 1000` | `{tiny.bin}` (strict `<`, `small.bin` at 1000 fails) |
| `smaller-size: 1KiB parses` | `--filter-smaller-size 1KiB` | `{tiny.bin, small.bin}` (1KiB = 1024, `small.bin` at 1000 < 1024 passes) |

#### `e2e_filter_larger_size`

**Fixture:** same as smaller-size.

Since `larger-size` is inclusive `>=`:

| Label | Args | Expected keys |
|---|---|---|
| `larger-size: match 5000` | `--filter-larger-size 5000` | `{medium.bin, large.bin}` |
| `larger-size: no match 1000000` | `--filter-larger-size 1000000` | `{}` |
| `larger-size: boundary 10000 inclusive` | `--filter-larger-size 10000` | `{medium.bin, large.bin}` (medium.bin at exactly 10000 passes) |
| `larger-size: 10KiB parses` | `--filter-larger-size 10KiB` | `{large.bin}` (10KiB = 10240, medium.bin at 10000 < 10240 fails) |

#### `e2e_filter_mtime_before`

**Fixture:** 4 objects uploaded in one batch via `put_objects_parallel`:
`obj1`, `obj2`, `obj3`, `obj4` (each 100 B).

**Observed-time capture:** after upload, call
`helper.client().list_objects_v2().bucket(&bucket).send()` and read each
object's `LastModified`. The test builds a sorted list of the **distinct**
timestamps observed. S3 `LastModified` is second-precision, so parallel
uploads can share a timestamp — the distinct-times list has between 1 and 4
entries.

**Pivot selection and expected sets are computed at runtime from the
observed distinct times**, not hardcoded. For each sub-assertion the test
picks a pivot from the distinct-times list (or derives one) and computes the
expected set by filtering the observed `(key, last_modified)` pairs through
the same predicate the filter uses (`last_modified < pivot` for mtime-before,
`last_modified >= pivot` for mtime-after). This keeps the assertions exact
regardless of tie structure.

Since `mtime-before` is strict `<`:

| Label | Pivot strategy | Expected set |
|---|---|---|
| `mtime-before: match (middle pivot)` | `distinct_times[len/2]` — if there are ≥2 distinct times, use the upper-half start; otherwise **skip this case** with a logged note | `{key : last_modified < pivot}` computed at runtime |
| `mtime-before: no match (earliest pivot)` | `distinct_times[0]` (the smallest observed time) | `{}` (strict `<` against the minimum can never match) |
| `mtime-before: boundary (max pivot)` | `distinct_times[len-1]` (the largest observed time) | `{key : last_modified < distinct_times[len-1]}` (all objects strictly earlier; if all 4 objects share one timestamp, this is `{}`) |

**Tie handling:** if all four uploads collide into the same second
(`distinct_times.len() == 1`), the "middle pivot" sub-assertion is
**skipped** with a `println!("skipped due to LastModified collision")`. The
"earliest" and "boundary" cases still run — they are well-defined even with
one distinct time (both yield `{}`). In regions with non-trivial latency the
full assertion set runs; this fallback exists so the suite doesn't become
flaky in fast regions.

#### `e2e_filter_mtime_after`

**Fixture:** same upload pattern as `mtime_before`.

Since `mtime-after` is inclusive `>=`, expected sets are again computed at
runtime from observed distinct times:

| Label | Pivot strategy | Expected set |
|---|---|---|
| `mtime-after: match (middle pivot)` | `distinct_times[len/2]` (skipped if `len == 1`) | `{key : last_modified >= pivot}` |
| `mtime-after: no match (after max)` | `distinct_times[len-1] + 1s` | `{}` (strict-greater against max + 1s can never match) |
| `mtime-after: boundary (earliest pivot inclusive)` | `distinct_times[0]` | all 4 objects (inclusive `>=` at the minimum always matches everyone) |

Same tie-handling fallback as `mtime_before`: the "middle pivot" case is
skipped if there is only one distinct observed timestamp.

#### `e2e_filter_storage_class`

**Fixture:** 5 objects uploaded via individual `put_object` calls with
explicit `StorageClass` set on each:

- `std.bin` — STANDARD (default, no `StorageClass` in put)
- `rrs.bin` — REDUCED_REDUNDANCY
- `ia.bin` — STANDARD_IA
- `oz.bin` — ONEZONE_IA
- `it.bin` — INTELLIGENT_TIERING

The framework's existing `put_object` helper does not accept a storage class
argument. **This test requires adding a new helper**
`put_object_with_storage_class(&self, bucket, key, body, storage_class: &str)`
to `tests/common/mod.rs`, which builds the `PutObject` request with
`.storage_class(StorageClass::from_str(...))`. This is additive; no other
test depends on it.

| Label | Args | Expected keys |
|---|---|---|
| `storage-class: single STANDARD_IA` | `--storage-class STANDARD_IA` | `{ia.bin}` |
| `storage-class: multiple` | `--storage-class STANDARD_IA,ONEZONE_IA` | `{ia.bin, oz.bin}` |
| `storage-class: no match GLACIER` | `--storage-class GLACIER` | `{}` |
| `storage-class: STANDARD matches None` | `--storage-class STANDARD` | `{std.bin}` (S3 omits StorageClass for STANDARD objects; filter treats None as STANDARD per `storage_class.rs:33`) |

### Combination tests (5 functions)

#### `e2e_filter_combo_all_seven`

The "all filters at once" test. Proves AND-composition works across every
filter flag simultaneously. Exactly one object must survive all seven
filters.

**Fixture strategy:** two-batch upload to get a time pivot.

**Batch 1 (uploaded first):**
- `old.csv` (5000 B, STANDARD) — will fail `mtime-after`

**Batch 2 (uploaded after reading back batch 1's LastModified):**
- `target.csv` (5000 B, STANDARD) — **the one survivor**
- `target.txt` (5000 B, STANDARD) — fails `include-regex '\.csv$'`
- `excluded.csv` (5000 B, STANDARD) — fails `exclude-regex '^excluded'`
- `small.csv` (100 B, STANDARD) — fails `larger-size 1000`
- `ia.csv` (5000 B, STANDARD_IA) — fails `storage-class STANDARD`

**Setup sequence:**

1. Upload `old.csv`.
2. `tokio::time::sleep(Duration::from_millis(1500)).await`. S3
   `LastModified` is second-precision, so we need a guaranteed gap between
   batch 1 and batch 2. 1.5s is enough to push the next upload into the
   following second even with clock skew and request-latency variance.
   Sleeps appear only in tests that need a time pivot (this one and
   `e2e_filter_pair_mtime_and_storage_class`); none of the per-filter tests
   sleep.
3. Upload the five batch-2 objects via `put_objects_parallel` (plus a
   `put_object_with_storage_class` call for `ia.csv`).
4. `list_objects_v2` to read every object's `LastModified`. Compute
   `t_pivot = min(batch_2_LastModified)`.
5. Assert `t_pivot > old_csv.LastModified` — this MUST hold after the 1.5s
   sleep. A failure here means something went very wrong (clock skew > 1.5s)
   and the test should panic with a clear message.
6. Use `t_pivot` as the `--filter-mtime-after` pivot (RFC3339 format) and
   `t_pivot + 1h` as the `--filter-mtime-before` pivot. Target.csv was
   uploaded in batch 2, so its `LastModified >= t_pivot` and
   `< t_pivot + 1h` (the fixture upload completes in seconds, well under an
   hour).

**Run:**
```
s3ls --recursive --json \
  --filter-include-regex '\.csv$' \
  --filter-exclude-regex '^excluded' \
  --filter-mtime-after <t_pivot-rfc3339> \
  --filter-mtime-before <t_pivot+1h-rfc3339> \
  --filter-larger-size 1000 \
  --filter-smaller-size 10000 \
  --storage-class STANDARD \
  s3://<bucket>/
```

**Expected keys:** `{target.csv}`.

#### `e2e_filter_pair_regex_and_size`

**Fixture:** `a.csv` (100 B), `b.csv` (2000 B), `a.txt` (100 B), `b.txt` (2000 B)

| Args | Expected keys |
|---|---|
| `--filter-include-regex '\.csv$' --filter-larger-size 1000` | `{b.csv}` |

#### `e2e_filter_pair_mtime_and_storage_class`

**Fixture:** two-batch upload with a 1.5s sleep between batches (same pattern
as `e2e_filter_combo_all_seven`).

- Batch 1: `old_std.bin` (STANDARD), `old_ia.bin` (STANDARD_IA) — both 100 B.
- `tokio::time::sleep(Duration::from_millis(1500)).await`
- Batch 2: `new_std.bin` (STANDARD), `new_ia.bin` (STANDARD_IA).
- `list_objects_v2` to read every object's `LastModified`.
- `t_new_min = min(batch_2_LastModified)`.
- `t_old_max = max(batch_1_LastModified)`.
- Assert `t_new_min > t_old_max` (must hold after the 1.5s sleep; panic if not).

| Args | Expected keys |
|---|---|
| `--filter-mtime-after <t_new_min-rfc3339> --storage-class STANDARD` | `{new_std.bin}` (inclusive `>=` picks up batch 2 at or after `t_new_min`; `new_ia.bin` is filtered out by storage-class; `old_std.bin` is filtered out by mtime-after) |

#### `e2e_filter_pair_exclude_and_size_range`

**Fixture:** `keep_small.bin` (500 B), `keep_big.bin` (5000 B),
`keep_mid.bin` (2000 B), `skip_mid.tmp` (2000 B)

| Args | Expected keys |
|---|---|
| `--filter-exclude-regex '\.tmp$' --filter-larger-size 1000 --filter-smaller-size 4000` | `{keep_mid.bin}` |

#### `e2e_filter_pair_include_and_exclude_regex`

**Fixture:** `report.csv`, `report_tmp.csv`, `data.csv`, `notes.txt`

| Args | Expected keys |
|---|---|
| `--filter-include-regex '\.csv$' --filter-exclude-regex '_tmp'` | `{report.csv, data.csv}` |

### Orthogonal-flag smoke tests (2 functions)

#### `e2e_filter_max_depth_common_prefix_passthrough`

**Why this test exists:** `FilterChain::matches` at `src/filters/mod.rs:37`
short-circuits `CommonPrefix` entries to always return `true`. A future
refactor could easily break this passthrough (e.g., by making include-regex
apply to `CommonPrefix` strings). Nothing in the unit-test suite catches a
change in the interaction with real S3 listing + `--max-depth`. This test
locks it in.

**Fixture:** `logs/2025/a.log`, `logs/2025/b.log`, `logs/2026/a.log`,
`readme.csv`

**Run:**
```
s3ls --recursive --max-depth 1 --json --filter-include-regex '\.csv$' s3://<bucket>/
```

**Expected JSON lines (as a set, parsed from NDJSON):**
- `{"Key": "readme.csv", ...}` — matches regex
- `{"Prefix": "logs/"}` — passes filter because `CommonPrefix` is exempt

The test uses `assert_json_keys_or_prefixes_eq` to accept both shapes.

**Confirmed against source:** `src/aggregate.rs:514` (`format_entry_json`)
emits `{"Prefix": ...}` for `CommonPrefix` and `{"Key": ..., ...}` for
`Object`. No ambiguity.

#### `e2e_filter_no_sort_streaming`

**Why this test exists:** `--no-sort` bypasses the sort buffer and streams
results. Filter application should still work in streaming mode. This test
confirms the streaming path doesn't accidentally skip the filter chain.

**Fixture:** 6 objects, `a1.bin` (1000 B) through `a6.bin` (6000 B).

**Run:**
```
s3ls --recursive --no-sort --json --filter-larger-size 3000 s3://<bucket>/
```

**Expected keys (set-equal, order-independent because of `--no-sort`):**
`{a3.bin, a4.bin, a5.bin, a6.bin}`

---

## Cost and runtime

- **14 tests × 1 bucket each** = 14 bucket creates + 14 bucket deletes per run
  (well under the 100-bucket account default).
- **~60 `PutObject` calls** total across all fixtures.
- **~30 `run_s3ls` subprocess invocations** across all tests.
- **~30 `list_objects_v2` reads** for mtime-filter timestamp capture.
- **Teardown** uses the existing `delete_all_objects` batch helper.

At current S3 pricing: **well under $0.01 per full suite run**. Individual
filter tests cost a fraction of a cent.

Wall-clock runtime is not estimated — it depends on region latency and
test-binary scheduling — but is bounded by `E2E_TIMEOUT = 60s` per test.

## Execution

```bash
# Run just the filter suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters -- --nocapture

# Run a single filter test
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_filters e2e_filter_include_regex -- --nocapture

# Run all e2e tests (listing + filters)
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

Each test owns a uniquely-named bucket, so the default parallel test runner
is safe.

---

## Summary of new code

- **New file:** `tests/e2e_filters.rs` (~14 tests, ~500-700 LOC).
- **Modified file:** `tests/common/mod.rs`:
  - Add `assert_json_keys_eq(stdout, expected, label)`
  - Add `assert_json_keys_or_prefixes_eq(stdout, expected, label)`
  - Add `put_object_with_storage_class(bucket, key, body, storage_class)`
- **No production code changes.**
- **No Cargo.toml changes.** `serde_json` is already a direct production
  dependency.
- **No docs changes** beyond this spec (and its eventual implementation plan).
