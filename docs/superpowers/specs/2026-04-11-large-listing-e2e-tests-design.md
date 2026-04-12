# Large-Scale Listing Completeness E2E Test — Design

**Date:** 2026-04-11
**Status:** Design (pending implementation plan)
**Builds on:**
- `docs/superpowers/specs/2026-04-11-step6-e2e-framework-design.md` (e2e framework)

---

## Goal

One end-to-end test that uploads ~16,000 objects with a realistic 6-7
level hierarchy into a single bucket, then verifies s3ls enumerates
every object correctly under 5 different listing configurations:
full recursive listing, prefix-scoped listing, depth-limited listing,
and two different `--max-parallel-listing-max-depth` values.

This test targets the core parallel-listing engine's correctness: does
it enumerate every object exactly once across complex prefix-tree
structures with varying parallelism depths? All other e2e tests use
tiny fixtures (3-10 objects) — this test is the only one that exercises
the engine at realistic scale.

Runs under `RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_large_listing`.

## Non-goals

- **Sorting.** All runs use `--no-sort` to avoid buffering 16K objects
  in memory. Sort correctness is covered by the sort e2e suite.
- **Display items / column formatting.** Covered by the display suite.
- **Filters.** Covered by the filter suite.
- **Versioned listing.** The parallel engine works identically for
  `ListObjectVersions` — no need to duplicate the 16K-object test.
- **Performance measurement.** The test asserts correctness, not speed.

---

## Architecture

### File layout

New file:

```
tests/e2e_large_listing.rs    # 1 test function, cfg-gated
```

Modified file:

```
tests/common/mod.rs           # add put_objects_parallel_n, refactor put_objects_parallel
```

### Hierarchy

Mimics a real-world data lake (date-partitioned, multi-tenant) plus an
application log archive:

```
config.json                                                          depth 1
data/manifest.json                                                   depth 2
data/tenant-{01..05}/{2024,2025}/{01..12}/{01..25}/part-{001..005}.parquet   depth 6
logs/app/{2024,2025}/{01..12}/{01..15}/server-{01..03}/app.log              depth 7
```

| Subtree | Formula | Count |
|---|---|---|
| Shallow objects | `config.json` + `data/manifest.json` | 2 |
| Data partition | 5 tenants × 2 years × 12 months × 25 days × 5 files | 15,000 |
| Logs | 2 years × 12 months × 15 days × 3 servers × 1 file | 1,080 |
| **Total** | | **16,082** |

All objects are 1-byte payloads (`b"x"`) — the test is about
enumeration completeness, not content.

### Key generation

The fixture is generated programmatically via nested loops:

```rust
let mut keys: Vec<String> = Vec::with_capacity(16_082);

// Depth 1: config file
keys.push("config.json".to_string());

// Depth 2: data manifest
keys.push("data/manifest.json".to_string());

// Depth 6: data partitions
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

assert_eq!(keys.len(), 16_082);
```

### Upload strategy

The existing `put_objects_parallel` helper is hardcoded at 16 concurrent
uploads. At that concurrency, 16K objects would take ~15 minutes — well
beyond the test timeout.

**New helper:** `put_objects_parallel_n(&self, bucket, objects, max_concurrency)`
with configurable concurrency. The test uses 256 concurrent uploads:
16,082 / 256 ≈ 63 rounds × ~200ms ≈ 13 seconds for the upload phase.

The existing `put_objects_parallel` is refactored to delegate:
```rust
pub async fn put_objects_parallel(&self, bucket: &str, objects: Vec<(String, Vec<u8>)>) {
    self.put_objects_parallel_n(bucket, objects, 16).await;
}
```

All existing tests that call `put_objects_parallel` continue to work
unchanged.

### Timeout

The test uses `tokio::time::timeout(Duration::from_secs(300), ...)`
directly instead of the `e2e_timeout!` macro (which is 60 seconds). A
16K-object upload + 5 s3ls runs + cleanup needs ~2-3 minutes.

### Verification strategy

All 5 sub-assertions use `--json` output. The test parses every NDJSON
line, extracts `Key` fields (for object rows) and `Prefix` fields (for
CommonPrefix rows under `--max-depth`), and compares against expected
sets computed from the same key-generation loops.

**No new assertion helpers needed.** The test uses:
- `HashSet<String>` for set-equality on full listings (sub-assertions
  1, 2, 4, 5)
- Inline checks for `--max-depth` (sub-assertion 3)

The set comparison is done inline rather than via `assert_json_keys_eq`
because the error message for a 16K-element diff needs to be truncated
(showing only the first few missing/extra keys), not dumping the full
stdout.

---

## Sub-assertions (5 runs, one bucket)

### Sub-assertion 1: Full recursive listing from root

**Run:** `s3ls --recursive --json --no-sort s3://bucket/`

**Assertion:** parse all NDJSON lines, collect `Key` fields into a
`HashSet<String>`. Compare against the expected set of 16,082 keys
(generated by the same loop that built the fixture). The sets must be
equal.

**What this proves:** the parallel listing engine enumerates every
object in a complex 7-level hierarchy at the default
`--max-parallel-listing-max-depth 2` without dropping or duplicating
any keys.

### Sub-assertion 2: Prefix-scoped listing

**Run:** `s3ls --recursive --json --no-sort s3://bucket/data/tenant-03/2025/`

**Assertion:** the returned key set equals the expected subset of keys
starting with `data/tenant-03/2025/`. Expected count:
12 months × 25 days × 5 files = 1,500 keys.

**What this proves:** prefix-scoped listing enumerates correctly within
a subtree without leaking keys from sibling subtrees.

### Sub-assertion 3: Depth-limited listing

**Run:** `s3ls --recursive --max-depth 3 --json --no-sort s3://bucket/`

**Assertion:**
- Objects at depth ≤ 3: `config.json` (depth 1) and
  `data/manifest.json` (depth 2) appear as `Key` entries. No other
  objects exist at depth ≤ 3, so exactly 2 `Key` entries.
- CommonPrefix entries at the depth boundary: `data/tenant-{01..05}/`
  (5 entries at depth 3) and `logs/app/2024/`, `logs/app/2025/` (2
  entries at depth 3). Total: 7 `Prefix` entries.
- Total output lines (excluding empty): 2 + 7 = 9.

**What this proves:** `--max-depth` correctly truncates the listing at
the specified depth, emitting PRE entries at the boundary without
descending further.

### Sub-assertion 4: Shallow parallelism depth

**Run:** `s3ls --recursive --json --no-sort --max-parallel-listing-max-depth 1 s3://bucket/`

**Assertion:** same 16,082-key set as sub-assertion 1.

**What this proves:** reducing `--max-parallel-listing-max-depth` to 1
(the engine discovers prefixes at only 1 level before listing
sequentially within each) still produces a complete listing.

### Sub-assertion 5: Deep parallelism depth

**Run:** `s3ls --recursive --json --no-sort --max-parallel-listing-max-depth 4 s3://bucket/`

**Assertion:** same 16,082-key set as sub-assertion 1.

**What this proves:** increasing `--max-parallel-listing-max-depth` to
4 (deeper prefix discovery) still produces a complete listing without
duplicating or dropping keys.

---

## Inline set-comparison (not using `assert_json_keys_eq`)

The existing `assert_json_keys_eq` helper dumps the full stdout on
failure — unusable for 16K objects. The test implements its own inline
comparison that:

1. Collects `Key` fields from NDJSON into a `HashSet<String>`.
2. Compares against the expected `HashSet<String>`.
3. On mismatch, reports:
   - Count of missing keys (expected but not in output).
   - Count of extra keys (in output but not expected).
   - First 10 missing keys (for debugging).
   - First 10 extra keys.
   - Does NOT dump the full stdout.

```rust
fn assert_key_set_eq(
    stdout: &str,
    expected: &HashSet<String>,
    label: &str,
) {
    let actual: HashSet<String> = stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()?
                .get("Key")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect();

    if actual == *expected {
        return;
    }

    let missing: Vec<&String> = expected.difference(&actual).take(10).collect();
    let extra: Vec<&String> = actual.difference(expected).take(10).collect();
    panic!(
        "[{label}] key set mismatch\n  \
         expected count: {}\n  \
         actual count:   {}\n  \
         missing ({} total, first 10): {missing:?}\n  \
         extra ({} total, first 10): {extra:?}",
        expected.len(),
        actual.len(),
        expected.difference(&actual).count(),
        actual.difference(expected).count(),
    );
}
```

This function is defined locally inside the test (not in
`tests/common/mod.rs`) because it's specific to this test's scale.

---

## Cost and runtime

- **16,082 PutObject** calls (1-byte payload each) — ~$0.08 at $5/million.
- **5 listing runs** — each makes ~16 `ListObjectsV2` pages (1000 keys
  per page). ~$0.0004 per run × 5 = negligible.
- **16,082 DeleteObject** calls for cleanup — ~$0.08.
- **Total per run:** ~$0.16.
- **Runtime:** ~2-3 minutes (upload ~15s at 256 concurrency, 5 listing
  runs ~20-30s each, cleanup ~15s).

## Execution

```bash
# Just this test
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_large_listing -- --nocapture

# All e2e tests
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*' -- --nocapture
```

---

## Summary of new code

- **New file:** `tests/e2e_large_listing.rs` (~200-250 LOC, 1 test
  function with 5 sub-assertions, cfg-gated).
- **Modified file:** `tests/common/mod.rs`:
  - Add `TestHelper::put_objects_parallel_n(bucket, objects, max_concurrency)`
    method.
  - Refactor existing `put_objects_parallel` to delegate to
    `put_objects_parallel_n` with concurrency=16.
- **No production code changes.**
- **No `Cargo.toml` changes.**
- **No changes to existing test files** — all other e2e tests continue
  to use `put_objects_parallel` unchanged.
