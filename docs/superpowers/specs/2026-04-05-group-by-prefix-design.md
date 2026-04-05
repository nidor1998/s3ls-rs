# Group-by-Prefix Design Specification

## Overview

Add `--group-by-prefix` with `--depth N` to s3ls-rs — an alternative output mode that replaces per-object listing with prefix-level aggregation. Analogous to `du` for S3: shows count, size statistics, and date range per "directory" at a configurable depth.

## CLI Interface

### New Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--group-by-prefix` | bool | false | Activate grouped output mode (no per-object lines) |
| `--depth N` | u32 | 1 | Prefix depth to group at. Requires `--group-by-prefix`. |

### Examples

```bash
# Group by top-level prefix
s3ls --group-by-prefix s3://bucket/

# Group 2 levels deep
s3ls --group-by-prefix --depth 2 s3://bucket/

# Combine with filters — only group .parquet files
s3ls --group-by-prefix --filter-include-regex '\.parquet$' s3://bucket/data/

# JSON output
s3ls --group-by-prefix --json s3://bucket/

# Human-readable sizes
s3ls --group-by-prefix --human s3://bucket/
```

### Flag Compatibility

**Incompatible flags** (rejected at parse time with a clear error):

- `--sort` / `--reverse` — grouping produces its own ordering (alphabetical by prefix)
- `--show-etag`, `--show-storage-class`, `--show-checksum-algorithm`, `--show-checksum-type`, `--show-owner`, `--show-restore-status`, `--show-is-latest`, `--show-fullpath` — per-object columns don't apply to groups
- `--all-versions` — delete markers / version semantics don't map to group stats

**Compatible flags:**

- All filters (`--filter-include-regex`, `--filter-exclude-regex`, `--filter-mtime-before`, `--filter-mtime-after`, `--filter-smaller-size`, `--filter-larger-size`, `--storage-class`) — applied before grouping
- `--human` — affects all size columns (TOTAL_SIZE, MIN_SIZE, MAX_SIZE, AVG_SIZE)
- `--json` — NDJSON output per group
- `--header` — column headers
- `--summary` — grand total line after grouped output
- `--recursive` — implied by `--group-by-prefix` (grouping non-recursive listing is meaningless)

### Validation Rules

- `--depth` without `--group-by-prefix` is an error
- `--group-by-prefix` automatically sets `--recursive` to true

## Depth Semantics & Prefix Extraction

### How Depth Works

Given an object key, split by `/` and take the first N segments:

```
Key: data/2024/01/file.csv
Depth 1 → data/
Depth 2 → data/2024/
Depth 3 → data/2024/01/
```

### Target Prefix Interaction

Depth is relative to the target prefix. Running `s3ls --group-by-prefix --depth 1 s3://bucket/data/` with key `data/2024/01/file.csv` extracts prefix `2024/` (relative to `data/`), not `data/`.

### Edge Cases

| Case | Behavior |
|------|----------|
| Root-level objects (no `/` in key, e.g. `README.md`) | Grouped under `(root)` |
| Trailing slashes (keys like `data/`) | Treated as normal objects; prefix at depth 1 is `data/` |
| Depth exceeds key segments (key `data/file.csv` at depth 3) | Uses full key path up to last `/`: `data/` |
| CommonPrefix entries | Skipped — they have no size or date |

### Sort Order

Grouped rows are sorted alphabetically by prefix. The `(root)` group appears last (after all prefixed groups).

## Output Format

### Text Output (default)

Tab-delimited, consistent with existing output style:

```
PREFIX	COUNT	TOTAL_SIZE	MIN_SIZE	MAX_SIZE	AVG_SIZE	OLDEST	NEWEST
data/2024/	45000	1319413953	102	52428800	29320	2024-01-01T00:00:00Z	2024-12-31T23:59:00Z
data/2025/	82000	2638827906	256	104857600	32180	2025-01-01T00:00:00Z	2025-03-15T12:00:00Z
logs/	73001	483183820	64	1048576	6619	2024-06-01T00:00:00Z	2025-04-01T08:00:00Z
(root)	3	4521	1024	2048	1507	2024-01-15T00:00:00Z	2025-02-01T00:00:00Z
```

- `--header` adds the header row; without it, data rows only
- `--human` affects TOTAL_SIZE, MIN_SIZE, MAX_SIZE, AVG_SIZE
- AVG_SIZE is integer (floor division), displayed same as other sizes

### JSON Output (`--json`)

NDJSON, one line per group:

```json
{"prefix":"data/2024/","count":45000,"total_size":1319413953,"min_size":102,"max_size":52428800,"avg_size":29320,"oldest":"2024-01-01T00:00:00Z","newest":"2024-12-31T23:59:00Z"}
{"prefix":"data/2025/","count":82000,"total_size":2638827906,"min_size":256,"max_size":104857600,"avg_size":32180,"oldest":"2025-01-01T00:00:00Z","newest":"2025-03-15T12:00:00Z"}
```

### Summary (`--summary`)

Text:
```
Total: 200004 objects, 4441430200 bytes, 4 groups
```

JSON:
```json
{"summary":{"total_objects":200004,"total_size":4441430200,"total_groups":4}}
```

Adds group count to the existing summary format.

## Implementation

### Approach

Post-pipeline transformation (Approach C): collect all entries through the existing lister → filter → channel pipeline as today. After collection, branch on `group_by_prefix` config to either output per-object lines (existing behavior) or transform into grouped output.

### Data Structures

New struct in `aggregate.rs`:

```rust
pub struct PrefixGroup {
    pub prefix: String,
    pub count: u64,
    pub total_size: u64,
    pub min_size: u64,
    pub max_size: u64,
    pub oldest: chrono::DateTime<chrono::Utc>,
    pub newest: chrono::DateTime<chrono::Utc>,
}
```

### New Functions in `aggregate.rs`

- `group_by_prefix(entries: &[ListEntry], depth: usize, target_prefix: Option<&str>) -> Vec<PrefixGroup>` — iterates entries, extracts prefix at depth (relative to target prefix), accumulates stats into a `HashMap<String, PrefixGroup>`, then sorts alphabetically with `(root)` last. Skips `CommonPrefix` entries.

- `format_group_entry(group: &PrefixGroup, human: bool) -> String` — tab-delimited text row

- `format_group_header() -> String` — the header row

- `format_group_entry_json(group: &PrefixGroup) -> String` — NDJSON row

### Changes to `pipeline.rs`

After the existing collect step, add a branch:

```rust
if self.config.group_by_prefix {
    let groups = group_by_prefix(&entries, self.config.depth, ...);
    // format and write groups instead of entries
} else {
    // existing per-object output (unchanged)
}
```

### Changes to `config/`

- Add `group_by_prefix: bool` and `depth: u32` to `Config`
- Add `--group-by-prefix` and `--depth` CLI args to `CLIArgs`
- Add validation: reject incompatible flag combinations with clap conflicts
- `--group-by-prefix` forces `recursive = true`

## Testing

### Unit Tests (`aggregate.rs`)

- `group_by_prefix_depth_1` — groups entries by first path segment, verifies count/size/dates
- `group_by_prefix_depth_2` — verifies two-level grouping
- `group_by_prefix_root_objects` — keys without `/` land in `(root)` group
- `group_by_prefix_depth_exceeds_segments` — key `data/file.csv` at depth 3 → `data/`
- `group_by_prefix_relative_to_target` — respects target prefix stripping
- `group_by_prefix_skips_common_prefix` — `CommonPrefix` entries excluded from stats
- `group_by_prefix_sorted_alpha_root_last` — alphabetical order, `(root)` at end
- `format_group_entry_text` — correct tab-delimited columns
- `format_group_entry_human` — human-readable sizes in all size columns
- `format_group_entry_json` — valid JSON with all fields
- `format_group_header` — correct column names

### Unit Tests (`config/args/`)

- `group_by_prefix_implies_recursive` — verify recursive is set
- `group_by_prefix_rejects_sort` — error when combined with `--sort`
- `group_by_prefix_rejects_show_flags` — error when combined with `--show-etag` etc.
- `group_by_prefix_rejects_all_versions` — error when combined with `--all-versions`
- `group_by_prefix_default_depth` — depth defaults to 1
- `depth_without_group_by_prefix_errors` — `--depth` alone is invalid

### Integration Test (`pipeline.rs`)

- `pipeline_group_by_prefix` — MockStorage with entries at various depths, verify grouped output
