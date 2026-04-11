# Sort E2E Tests — Design

**Date:** 2026-04-11
**Status:** Design (pending implementation plan)
**Builds on:**
- `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md` (e2e framework)
- `docs/superpowers/specs/2026-04-11-step7-filter-e2e-tests-design.md` (non-versioning filter tests)
- `docs/superpowers/specs/2026-04-11-versioned-filter-e2e-tests-design.md` (versioning filter tests)
- `docs/superpowers/specs/2026-04-11-display-e2e-tests-design.md` (display tests)

---

## Goal

Add end-to-end test coverage for s3ls sort functionality — every sort
field (`key`, `size`, `date` for object listings; `bucket` for bucket
listings), both directions (`--reverse`), multi-column (comma-separated),
`--no-sort` streaming, and the `--all-versions` auto-appended secondary
sort. All assertions use `--json` output. Runs under
`RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_sort` against real AWS S3.

After this step:

- `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_sort` runs against real
  S3 and passes.
- `cargo test` (without the cfg flag) still passes — the new file compiles
  to an empty binary.
- `cargo clippy --all-features` and `cargo fmt --check` stay clean.
- Every sort field has ascending + descending coverage.
- Multi-column sort is verified by a deliberate tiebreak test.
- `--no-sort` is verified for set equality (order is intentionally not
  asserted per commit `3e6c4fb` — "clarify --no-sort produces arbitrary
  order").
- The `--all-versions --sort key` auto-append of `date` as secondary sort
  (`src/config/args/mod.rs:759-761`) is locked in by a dedicated test.
- Bucket listing sort is verified with a 2-bucket fixture that makes
  relative ordering deterministic regardless of ambient bucket state.

## Non-goals

- **Text-mode sort output.** The user explicitly scoped the suite to
  JSON-only. Text-mode sort is implicitly covered by existing smoke tests
  (`e2e_binary_smoke` in `tests/e2e_listing.rs` already uses
  `assert_key_order` against text output for 3 keys).
- **`--sort region`.** `SortField::Region => std::cmp::Ordering::Equal` in
  `src/aggregate.rs:500` makes this a no-op comparator. Testing a no-op
  provides no contract value.
- **`--sort date` for bucket listings.** Bucket creation dates are
  ambient and uncontrollable in a general-purpose test account; asserting
  date order across arbitrary existing buckets would be flaky. The object
  listing date tests already exercise the date comparator.
- **`--parallel-sort-threshold` behavior.** This is a performance tuning
  knob, not a correctness contract.
- **Error-path tests.** Invalid sort fields are rejected at config-parse
  time by clap's `ValueEnum` derive (`src/config/args/mod.rs:48`). Unit
  tests cover the parse-time validation.
- **Production code changes.** The suite uses the existing public API as-is.

---

## Architecture

### File layout

New file:

```
tests/e2e_sort.rs      # 11 test functions, cfg-gated
```

Modified file:

```
tests/common/mod.rs    # 1 new helper: assert_json_keys_order_eq
```

### Cfg gating

`tests/e2e_sort.rs` starts with `#![cfg(e2e_test)]`. Matches the pattern
of `tests/e2e_filters.rs`, `tests/e2e_filters_versioned.rs`,
`tests/e2e_display.rs`, and `tests/e2e_listing.rs`.

### Framework reuse

All of the following are reused without modification:
- `TestHelper::new()`, `generate_bucket_name()`, `create_bucket()`,
  `create_versioned_bucket()`, `bucket_guard()`
- `TestHelper::put_object`, `put_objects_parallel`
- `TestHelper::run_s3ls(&args)` + `S3lsOutput`
- `e2e_timeout!()` macro, `E2E_TIMEOUT` constant
- `BucketGuard::cleanup()`
- `assert_json_keys_eq` (used by the `--no-sort` test for set equality)

### New helper: `assert_json_keys_order_eq`

Added to `tests/common/mod.rs` after the existing `assert_json_version_shapes_eq`
helper (which is the last assertion helper currently in the file).

```rust
/// Parse NDJSON stdout from `s3ls --json` and assert the sequence of
/// `Key` fields (in order) equals `expected`. Unlike
/// `assert_json_keys_eq` which does set comparison, this helper
/// verifies exact ordering — the primary assertion for sort tests.
///
/// Duplicates in `expected` are handled naturally: a key that appears
/// twice in the expected slice must also appear twice in the output,
/// in the same positions. This is what makes the helper suitable for
/// versioned-listing tests where the same key appears multiple times.
///
/// Lines that parse as JSON but have no `Key` field (e.g., `CommonPrefix`
/// entries which emit `{"Prefix": ...}`, or summary lines which emit
/// `{"Summary": ...}`) are SKIPPED — the ordering check applies only to
/// object/delete-marker rows.
///
/// Panics if:
/// - any non-empty line fails to parse as JSON,
/// - the resulting sequence of `Key` values does not equal `expected`.
pub fn assert_json_keys_order_eq(
    stdout: &str,
    expected: &[&str],
    label: &str,
) {
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

**Why sequence (not multiset) comparison:** sort tests assert exact order.
A regression where the sort is applied but reversed, or where the primary
and secondary fields are swapped, produces a different sequence with the
same multiset.

**Why `filter_map` instead of `expect`:** skipping JSON lines without a
`Key` field (summaries, common prefixes) is intentional. Sort tests use
`--recursive` without `--max-depth`, so no CommonPrefix lines are expected
in practice, but the filter is defensive.

### Sort contract (verified against source)

| Aspect | Source | Contract |
|---|---|---|
| `--sort` value_delimiter | `src/config/args/mod.rs:223` | `value_delimiter = ','` — `--sort size,key` is one argument |
| Default sort | `src/config/args/mod.rs:714-720` | Object listing = `key`; bucket listing = `bucket` |
| Max sort fields | `src/config/args/mod.rs:751` | 2 |
| `--reverse` semantics | `src/aggregate.rs:502-504` | Flips the FINAL comparator (after all sort fields applied) |
| `--no-sort` | `src/config/args/mod.rs:234` | `conflicts_with_all = ["sort", "reverse"]` — clap rejects combining these |
| Versioning secondary | `src/config/args/mod.rs:759-761` | When `--all-versions` is set AND user specified exactly 1 sort field AND that field is not `date`, `date` is appended as secondary |
| `sort_entries` direction | `src/aggregate.rs:487-512` | Iterates sort fields, composes comparisons via `then_with`, reverses at the end if `reverse` is true |

---

## Test catalog

Eleven test functions. Each creates its own bucket (two buckets for Tests
10-11), uploads a minimal fixture, runs `s3ls` once or twice, and asserts
via `assert_json_keys_order_eq` (or `assert_json_keys_eq` for Test 8).

### Test 1: `e2e_sort_key_asc`

**Fixture** (3 objects, parallel upload, same size):
```rust
[("c.txt", 100 B), ("a.txt", 100 B), ("b.txt", 100 B)]
```

**Sub-assertion 1 (explicit `--sort key`):**
`--recursive --json --sort key` → `["a.txt", "b.txt", "c.txt"]`

**Sub-assertion 2 (default, no `--sort`):**
`--recursive --json` → same expected. Proves the default is key
ascending.

### Test 2: `e2e_sort_key_desc`

Same fixture as Test 1.

**Run:** `--recursive --json --sort key --reverse`
**Expected:** `["c.txt", "b.txt", "a.txt"]`

### Test 3: `e2e_sort_size_asc`

**Fixture** (4 objects, parallel upload, distinct sizes, non-alphabetical
keys so sort-by-size is distinguishable from sort-by-key):
```rust
[
    ("medium.bin", 5000 B),
    ("tiny.bin", 10 B),
    ("large.bin", 100000 B),
    ("small.bin", 1000 B),
]
```

**Run:** `--recursive --json --sort size`
**Expected:** `["tiny.bin", "small.bin", "medium.bin", "large.bin"]`

### Test 4: `e2e_sort_size_desc`

Same fixture.

**Run:** `--recursive --json --sort size --reverse`
**Expected:** `["large.bin", "medium.bin", "small.bin", "tiny.bin"]`

### Test 5: `e2e_sort_date_asc`

**Fixture** (3 objects, SEQUENTIAL upload with `sleep(1500ms)` between
each so LastModified values are in distinct S3-seconds). Upload-time
order is deliberately non-alphabetical so sort-by-date differs from
sort-by-key:

```rust
put_object("c.txt", vec![0u8; 100]).await;
sleep(Duration::from_millis(1500)).await;
put_object("a.txt", vec![0u8; 100]).await;
sleep(Duration::from_millis(1500)).await;
put_object("b.txt", vec![0u8; 100]).await;
```

**Run 1:** `--recursive --json --sort date`
**Expected:** `["c.txt", "a.txt", "b.txt"]` (oldest first = upload order)

**Run 2 (defensive):** inline check that `actual != ["a.txt", "b.txt", "c.txt"]`
— proves sort-by-date is actually running, not accidentally falling
through to the default key sort. Only useful if the result happens to
match both expected sequences (impossible with this fixture, but
defensive).

Actually: Run 2 is unnecessary because Run 1 already asserts the exact
sequence `[c, a, b]`, and `[a, b, c]` is not equal to `[c, a, b]`. The
defensive check would be redundant. **Dropping Run 2.**

### Test 6: `e2e_sort_date_desc`

Same sequential fixture as Test 5 (fresh bucket — each test creates its
own).

**Run:** `--recursive --json --sort date --reverse`
**Expected:** `["b.txt", "a.txt", "c.txt"]` (newest first)

### Test 7: `e2e_sort_size_key_tiebreak`

**Fixture** (4 objects, parallel upload, 2 objects at the same size to
exercise the secondary sort):
```rust
[
    ("z.txt", 100 B),
    ("b.csv", 5000 B),
    ("a.csv", 5000 B),
    ("m.txt", 10000 B),
]
```

**Run:** `--recursive --json --sort size,key`
**Expected:** `["z.txt", "a.csv", "b.csv", "m.txt"]`

**Critical assertion:** `a.csv` appears before `b.csv` in the output even
though `b.csv` was uploaded first. Both have size 5000, so the primary
sort ties them; the secondary `key` sort disambiguates in alphabetical
order. This proves the comma-delimited multi-column sort is actually
parsing both fields and the `sort_entries` function at `src/aggregate.rs:492-504`
is applying them in order with the correct `then_with` chaining.

### Test 8: `e2e_sort_no_sort`

**Fixture** (3 objects, parallel upload, identical sizes):
```rust
[("a.txt", 100 B), ("b.txt", 100 B), ("c.txt", 100 B)]
```

**Run:** `--recursive --json --no-sort`

**Assertion:** `assert_json_keys_eq` (set equality, NOT
`assert_json_keys_order_eq`) against `&["a.txt", "b.txt", "c.txt"]`.

**Doc comment:** explicitly notes that order is intentionally not asserted
per commit `3e6c4fb` ("clarify `--no-sort` produces arbitrary order"). The
test is a smoke test for "does s3ls accept `--no-sort` and return all
expected results" rather than a contract test for ordering.

**Why not combine with `--sort` or `--reverse`:** `--no-sort` has
`conflicts_with_all = ["sort", "reverse"]` at `src/config/args/mod.rs:234`.
Clap would reject the combination at parse time. The test uses
`--no-sort` alone.

### Test 9: `e2e_sort_versioned_secondary_date`

**Fixture** (versioned bucket, 2 keys × 2 versions each, SEQUENTIAL
uploads with sleeps to guarantee distinct LastModified):

```rust
create_versioned_bucket(&bucket).await;
put_object(&bucket, "apple.txt", vec![0u8; 100]).await;   // apple v1
sleep(Duration::from_millis(1500)).await;
put_object(&bucket, "apple.txt", vec![0u8; 200]).await;   // apple v2
sleep(Duration::from_millis(1500)).await;
put_object(&bucket, "banana.txt", vec![0u8; 100]).await;  // banana v1
sleep(Duration::from_millis(1500)).await;
put_object(&bucket, "banana.txt", vec![0u8; 200]).await;  // banana v2
```

**Run:** `--recursive --all-versions --json --sort key`

(No explicit `date` — `src/config/args/mod.rs:759-761` auto-appends it as
the secondary sort when `--all-versions` is set and the user specified
only 1 sort field and it's not `date`.)

**Assertion 1 (key sequence):**
`assert_json_keys_order_eq` against `&["apple.txt", "apple.txt", "banana.txt", "banana.txt"]`

**Assertion 2 (within-key LastModified monotonicity):** inline check.
Parse the NDJSON lines, extract `(Key, LastModified)` pairs, iterate and
assert that within each same-Key run, LastModified is non-decreasing.
RFC3339 timestamps compare lexicographically because the format is
fixed-width and sortable.

```rust
// After asserting key order, verify LastModified is non-decreasing
// within each Key group. This proves the auto-appended `date`
// secondary sort is actually applied, not just the primary `key` sort.
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
```

### Test 10: `e2e_sort_bucket_listing_asc`

**Fixture:** create TWO test buckets with deterministic sort-order names:

```rust
let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
let _guard_a = helper.bucket_guard(&bucket_a);
let _guard_z = helper.bucket_guard(&bucket_z);
helper.create_bucket(&bucket_a).await;
helper.create_bucket(&bucket_z).await;
```

Both bucket names start with `s3ls-e2e-` (so the cleanup pattern in
`tests/README.md` still finds them if the test leaks), but the `a-` and
`z-` prefix chars guarantee a deterministic alphabetical relationship
between the two.

**`uuid::Uuid` is already a dev dependency** (added by step 6 Task 1 —
verified in `Cargo.toml:59`). No new dependency.

**Run:** `--json --sort bucket` (no target argument → bucket listing mode)

**Assertion:** parse NDJSON lines, find positions of `bucket_a` and
`bucket_z` in the output by matching `v.get("Name").as_str() == bucket`,
assert `position(bucket_a) < position(bucket_z)`. The AWS account may
have other buckets between them; the test only asserts the RELATIVE order.

**Cleanup:** both `_guard_a.cleanup().await` and `_guard_z.cleanup().await`
at the end, in order.

### Test 11: `e2e_sort_bucket_listing_desc`

Same 2-bucket fixture strategy as Test 10. Fresh UUIDs so the test runs
in parallel safely with Test 10.

**Run:** `--json --sort bucket --reverse`

**Assertion:** assert `position(bucket_z) < position(bucket_a)`.

---

## Cost and runtime

- **11 tests × ~1.3 buckets average** ≈ 14 bucket creates + 14 bucket deletes per run.
- **~25-30 `PutObject` calls** total (most fixtures are 3-4 objects).
- **~13 `run_s3ls` subprocess invocations** (Test 1 makes 2; Test 9 makes 1; others make 1).
- **Sleeps:**
  - Test 5: 2 × 1500 ms = 3 s
  - Test 6: 2 × 1500 ms = 3 s
  - Test 9: 3 × 1500 ms = 4.5 s
  - **Total cumulative sleep:** ~10.5 s across the suite if run serially;
    less if parallel (Rust test harness runs integration tests in parallel
    by default, so date/version tests overlap in wall-clock time).

At current S3 pricing: **well under $0.01 per run.** Runtime bounded by
`E2E_TIMEOUT = 60 s` per test.

## Execution

```bash
# Just the sort suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_sort -- --nocapture

# A single test
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_sort e2e_sort_size_asc -- --nocapture

# All e2e tests (listing + filters + filters_versioned + display + sort)
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

Each test owns its own bucket(s), so the default parallel test runner is
safe. Tests 10-11 each create 2 buckets with fresh UUIDs, so even parallel
runs of those two tests produce 4 unique buckets with no collisions.

---

## Summary of new code

- **New file:** `tests/e2e_sort.rs` (~500-700 LOC, 11 tests, cfg-gated).
- **Modified file:** `tests/common/mod.rs`:
  - Add `assert_json_keys_order_eq(stdout, expected, label)` free function.
- **No production code changes.**
- **No `Cargo.toml` changes.** `uuid` is already a dev dependency
  (added by step 6 Task 1).
- **No changes to existing tests** — `tests/e2e_listing.rs`,
  `tests/e2e_filters.rs`, `tests/e2e_filters_versioned.rs`, and
  `tests/e2e_display.rs` are all untouched.
