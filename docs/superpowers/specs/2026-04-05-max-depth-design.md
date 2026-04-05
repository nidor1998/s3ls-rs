# `--max-depth` Option Design Specification

## Overview

Add a `--max-depth <N>` CLI option that limits the depth of recursive object listing. When specified, the listing engine stops recursing beyond the given depth, so objects deeper than `N` levels below the listing prefix are never fetched from S3.

## Requirements

- `--max-depth` accepts a `u16` value representing the maximum number of directory levels to descend below the listing prefix.
- The option has no default value. When absent, recursive listing is unbounded (current behavior).
- The option requires `--recursive`; using it without `--recursive` produces a clap validation error.

## Depth Counting

Depth is defined as the number of `/` separators between the listing prefix and the object key.

Given a listing at `s3://bucket/data/`:

| Object key               | Depth |
|--------------------------|-------|
| `data/file.txt`          | 1     |
| `data/a/file.txt`        | 2     (one `/` below `data/`)  |
| `data/a/b/file.txt`      | 3     |
| `data/a/b/c/file.txt`    | 4     |

With `--max-depth 2`, only objects at depth 1 and 2 are returned. Objects at depth 3+ are never fetched.

## Behavior

- Objects beyond `max-depth` are **excluded** — they are not fetched from S3 and do not appear in output.
- No collapsed prefix entries are shown at the boundary (unlike non-recursive mode).

## Implementation

### CLI Layer

Add to `CLIArgs` in `src/config/args/mod.rs`:

```rust
#[arg(long, requires = "recursive", help_heading = "General",
      env = "MAX_DEPTH")]
pub max_depth: Option<u16>,
```

### Config Layer

Add to `Config` in `src/config/mod.rs`:

```rust
pub max_depth: Option<u16>,
```

Wire through in `build_config_from_args()` / `TryFrom<CLIArgs>`.

### Storage Layer (S3-level enforcement)

Pass `max_depth: Option<u16>` into `S3Storage::new()` and store it in `ListingEngine`.

#### Parallel listing (`list_with_parallel`)

The engine already tracks `current_depth`. The change:

- When `current_depth == max_depth`: list objects at this prefix with `delimiter="/"` to fetch only direct objects at this level. Do **not** recurse into discovered sub-prefixes.
- When `current_depth < max_depth`: continue normal parallel/recursive behavior.
- When `max_depth` is `None`: current unbounded behavior (no change).

#### Interaction with `max_parallel_listing_max_depth`

Both constraints apply independently:

- `max_parallel_listing_max_depth` controls when parallel discovery switches to sequential listing. It is a performance tuning parameter.
- `max_depth` is a hard content cutoff — no objects beyond this depth are fetched regardless of listing strategy.

When both are set, the parallel engine respects whichever limit is hit first for prefix discovery. But even after switching to sequential mode (due to `max_parallel_listing_max_depth`), the sequential lister must still enforce `max_depth` by counting depth of returned keys and skipping those that exceed the limit.

#### Sequential listing (`list_sequential`)

When `max_depth` is set and the listing is sequential (either because parallel is disabled or because `max_parallel_listing_max_depth` was exceeded):

- Count the depth of each returned object key relative to the listing prefix.
- Skip objects whose depth exceeds `max_depth`.
- This is a fallback — the parallel path avoids fetching these objects entirely.

#### `list_dispatch`

Pass `max_depth` through to the listing methods. No logic change in dispatch itself.

### Pipeline / Lister

No changes needed. Depth enforcement happens entirely in the storage layer before objects enter the channel.

## Option Interactions

| Combined with               | Behavior                                                     |
|-----------------------------|--------------------------------------------------------------|
| No `--recursive`            | Clap validation error                                        |
| `--group-by-prefix`         | Compatible — depth limit applies before grouping             |
| Filters (`--filter-*`)      | Compatible — depth enforced at S3 level, filters applied after |
| `--all-versions`            | Compatible — depth applies to key paths                      |
| `--sort`                    | Compatible — sorting happens after listing                   |
| `--json`                    | Compatible — output format is independent                    |
| `--summary`                 | Compatible — summary reflects only fetched objects           |
| `--max-parallel-listings`   | Compatible — independent performance tuning                  |

## Testing

- **CLI validation**: `--max-depth` without `--recursive` produces an error.
- **CLI parsing**: `--max-depth 3 --recursive` parses correctly.
- **Depth counting**: Unit tests for depth calculation relative to prefix.
- **Parallel listing**: Verify recursion stops at max-depth and objects beyond are not fetched.
- **Sequential fallback**: Verify depth filtering works when sequential listing is used.
