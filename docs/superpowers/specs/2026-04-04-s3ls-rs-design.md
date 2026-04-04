# s3ls-rs Design Specification

## Overview

s3ls-rs is an ultra-fast S3 object listing tool that reuses over 95% of the source code from [nidor1998/s3rm-rs](https://github.com/nidor1998/s3rm-rs). Its key feature is ultra-fast object listing (approximately 100,000 objects per second) utilizing s3rm-rs's parallel listing functionality.

This is a code reuse (copy-and-modify) approach, not a fork. The two codebases are independent going forward.

### Library-First

All core functionality resides in the library crate (`src/lib.rs`). The CLI binary (`src/bin/s3ls/main.rs`) is a thin wrapper that parses arguments, builds a `Config`, and runs a `ListingPipeline`. All CLI features are available as a Rust library for programmatic use.

## Architecture

### 2-Stage Pipeline

```
┌──────────────────────────────┐    channel     ┌───────────┐
│  Lister                      │──────────────→│ Aggregate  │
│                              │  ListEntry    │            │
│  list object                 │  bounded      │ collect    │
│  → apply filter chain        │  (200,000)    │ sort       │
│  → if passes: send to channel│               │ format     │
│  → if not: discard           │               │ output     │
└──────────────────────────────┘               └───────────┘
```

Filters are applied inline within the lister task as synchronous function calls, avoiding channel communication overhead between separate filter stages. This differs from s3rm-rs which uses SPSC channels between filter stages.

### Pipeline Orchestration

```
1. execute_pipeline():
   a. Create Lister stage with FilterChain (spawn async task)
   b. Create Aggregate stage (spawn async task)
   c. Wait for Aggregate to complete
2. Return exit code (and statistics if --summary)
```

No event/callback infrastructure. The pipeline just runs and returns.

## Project Structure

```
s3ls-rs/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # Public API exports
│   ├── bin/
│   │   └── s3ls/
│   │       └── main.rs           # CLI entry point
│   ├── config/
│   │   ├── mod.rs                # Config struct, build_config_from_args()
│   │   └── args/
│   │       ├── mod.rs            # CLIArgs (Clap) — adapted from s3rm-rs
│   │       ├── tests.rs          # Argument parsing tests
│   │       └── value_parser/     # Custom Clap value parsers (reused)
│   ├── types/
│   │   ├── mod.rs                # S3Object, S3Target, ListEntry, ListingStatistics
│   │   └── error.rs              # ListingError enum
│   ├── pipeline.rs               # ListingPipeline — 2-stage orchestrator
│   ├── lister.rs                 # ObjectLister with inline filtering (reused + adapted)
│   ├── filters/                  # Filter implementations (reused)
│   │   ├── mod.rs                # FilterChain, ObjectFilter trait
│   │   ├── include_regex.rs
│   │   ├── exclude_regex.rs
│   │   ├── mtime_before.rs
│   │   ├── mtime_after.rs
│   │   ├── smaller_size.rs
│   │   ├── larger_size.rs
│   │   └── storage_class.rs      # NEW — filter by storage class
│   ├── aggregate.rs              # NEW — collect, sort, format, output
│   ├── storage/
│   │   ├── mod.rs                # StorageTrait interface (reused)
│   │   └── s3/
│   │       ├── mod.rs            # S3Storage + parallel listing (reused)
│   │       └── client_builder.rs # AWS client setup (reused)
│   └── tests/                    # Unit test modules
├── tests/
│   ├── common/                   # Test utilities
│   ├── unit_*.rs                 # Unit tests with mocks
│   └── e2e_*.rs                  # E2E tests (gated by E2E_TEST env flag)
```

**Removed from s3rm-rs:** `deleter.rs`, `terminator.rs`, `safety/`, `lua/`, `property_tests/`, `callback/`, `stage.rs` (SPSC filter wiring)

**New:** `aggregate.rs`, `filters/storage_class.rs`

## CLI Arguments

```
Usage: s3ls [OPTIONS] [TARGET]

Arguments:
  [TARGET] s3://<BUCKET_NAME>[/prefix] [env: TARGET=]

Options:
  -v, --verbose...    Increase logging verbosity
  -q, --quiet...      Decrease logging verbosity
  -h, --help          Print help (see more with '--help')
  -V, --version       Print version
```

### General

| Flag | Description | Env | Default |
|------|-------------|-----|---------|
| `--all-versions` | All versions including delete markers | `LIST_ALL_VERSIONS` | false |
| `--recursive` | List all objects recursively | `RECURSIVE` | false |

### Filtering

| Flag | Description | Env |
|------|-------------|-----|
| `--filter-include-regex <REGEX>` | List only objects whose key matches | `FILTER_INCLUDE_REGEX` |
| `--filter-exclude-regex <REGEX>` | Skip objects whose key matches | `FILTER_EXCLUDE_REGEX` |
| `--filter-mtime-before <TIME>` | Objects modified before this time | `FILTER_MTIME_BEFORE` |
| `--filter-mtime-after <TIME>` | Objects modified at or after this time | `FILTER_MTIME_AFTER` |
| `--filter-smaller-size <SIZE>` | Objects smaller than this size | `FILTER_SMALLER_SIZE` |
| `--filter-larger-size <SIZE>` | Objects larger than or equal to this size | `FILTER_LARGER_SIZE` |
| `--storage-class <LIST>` | Comma-separated storage classes to include | `STORAGE_CLASS` |

### Sort

| Flag | Description | Default |
|------|-------------|---------|
| `--sort <FIELD>` | Sort by: `key`, `size`, or `date` | `key` |
| `--reverse` | Reverse sort order | false |

The default sort field is `key` (lexicographic). When `--all-versions` is enabled, a secondary sort by `last_modified` (chronological) is applied after the primary sort field to guarantee deterministic ordering across parallel executions.

### Display

| Flag | Description |
|------|-------------|
| `--summary` | Append summary line (total count, total size) |
| `--human` | Human-readable sizes (e.g., `1.2KiB`) |
| `--show-fullpath` | Show full key instead of relative to prefix |
| `--show-etag` | Show ETag column |
| `--show-storage-class` | Show storage class column |
| `--show-checksum-algorithm` | Show checksum algorithm column |
| `--show-checksum-type` | Show checksum type column |
| `--json` | Output as NDJSON (one JSON object per line) |

### Tracing/Logging

| Flag | Description | Env |
|------|-------------|-----|
| `--json-tracing` | Structured JSON logs | `JSON_TRACING` |
| `--aws-sdk-tracing` | Include AWS SDK traces | `AWS_SDK_TRACING` |
| `--span-events-tracing` | Include span open/close events | `SPAN_EVENTS_TRACING` |
| `--disable-color-tracing` | Disable colored log output | `DISABLE_COLOR_TRACING` |

### AWS Configuration

| Flag | Description | Env |
|------|-------------|-----|
| `--aws-config-file <PATH>` | AWS config file path | `AWS_CONFIG_FILE` |
| `--aws-shared-credentials-file <PATH>` | AWS credentials file path | `AWS_SHARED_CREDENTIALS_FILE` |
| `--target-profile <PROFILE>` | AWS CLI profile | `TARGET_PROFILE` |
| `--target-access-key <KEY>` | Access key | `TARGET_ACCESS_KEY` |
| `--target-secret-access-key <KEY>` | Secret access key | `TARGET_SECRET_ACCESS_KEY` |
| `--target-session-token <TOKEN>` | Session token | `TARGET_SESSION_TOKEN` |
| `--target-region <REGION>` | AWS region | `TARGET_REGION` |
| `--target-endpoint-url <URL>` | Custom S3-compatible endpoint | `TARGET_ENDPOINT_URL` |
| `--target-force-path-style` | Path-style access | `TARGET_FORCE_PATH_STYLE` |
| `--target-accelerate` | S3 Transfer Acceleration | `TARGET_ACCELERATE` |
| `--target-request-payer` | Requester-pays | `TARGET_REQUEST_PAYER` |
| `--disable-stalled-stream-protection` | Disable stalled stream protection | `DISABLE_STALLED_STREAM_PROTECTION` |

### Performance

| Flag | Description | Env | Default |
|------|-------------|-----|---------|
| `--max-parallel-listings <N>` | Concurrent listing operations | `MAX_PARALLEL_LISTINGS` | 16 |
| `--max-parallel-listing-max-depth <N>` | Max depth for parallel listing | `MAX_PARALLEL_LISTING_MAX_DEPTH` | 2 |
| `--object-listing-queue-size <N>` | Internal queue size | `OBJECT_LISTING_QUEUE_SIZE` | 200,000 |
| `--allow-parallel-listings-in-express-one-zone` | Allow parallel in Express One Zone | `ALLOW_PARALLEL_LISTINGS_IN_EXPRESS_ONE_ZONE` | false |

### Retry

| Flag | Description | Env | Default |
|------|-------------|-----|---------|
| `--aws-max-attempts <N>` | Max retry attempts | `AWS_MAX_ATTEMPTS` | 10 |
| `--initial-backoff-milliseconds <N>` | Initial retry backoff (ms) | `INITIAL_BACKOFF_MILLISECONDS` | 100 |

### Timeout

| Flag | Description | Env |
|------|-------------|-----|
| `--operation-timeout-milliseconds <N>` | Overall operation timeout | `OPERATION_TIMEOUT_MILLISECONDS` |
| `--operation-attempt-timeout-milliseconds <N>` | Per-attempt timeout | `OPERATION_ATTEMPT_TIMEOUT_MILLISECONDS` |
| `--connect-timeout-milliseconds <N>` | Connection timeout | `CONNECT_TIMEOUT_MILLISECONDS` |
| `--read-timeout-milliseconds <N>` | Read timeout | `READ_TIMEOUT_MILLISECONDS` |

### Advanced

| Flag | Description | Env | Default |
|------|-------------|-----|---------|
| `--max-keys <N>` | Max objects per list request | `MAX_KEYS` | 1000 |
| `--auto-complete-shell <SHELL>` | Generate shell completions | `AUTO_COMPLETE_SHELL` | - |

## Core Types

### ListEntry

```rust
pub enum ListEntry {
    Object(S3Object),
    CommonPrefix(String),       // PRE entries in non-recursive mode
    DeleteMarker {              // --all-versions only
        key: String,
        version_id: String,
        last_modified: DateTime<Utc>,
        is_latest: bool,
    },
}
```

### S3Object (reused from s3rm-rs)

```rust
pub enum S3Object {
    NotVersioning {
        key: String,
        size: u64,
        last_modified: DateTime<Utc>,
        e_tag: String,
        storage_class: Option<String>,
        checksum_algorithm: Option<String>,
        checksum_type: Option<String>,
    },
    Versioning {
        key: String,
        version_id: String,
        size: u64,
        last_modified: DateTime<Utc>,
        e_tag: String,
        is_latest: bool,
        storage_class: Option<String>,
        checksum_algorithm: Option<String>,
        checksum_type: Option<String>,
    },
}
```

### SortField

```rust
#[derive(Clone, Debug, ValueEnum)]
pub enum SortField {
    Key,
    Size,
    Date,
}
```

### ListingStatistics (only computed when --summary)

```rust
pub struct ListingStatistics {
    pub total_objects: u64,
    pub total_size: u64,
    pub total_versions: u64,       // when --all-versions
    pub total_delete_markers: u64, // when --all-versions
}
```

## Filter System

### ObjectFilter Trait

```rust
pub trait ObjectFilter: Send + Sync {
    fn matches(&self, entry: &ListEntry) -> bool;
}
```

### FilterChain

```rust
pub struct FilterChain {
    filters: Vec<Box<dyn ObjectFilter>>,
}

impl FilterChain {
    pub fn matches(&self, entry: &ListEntry) -> bool {
        // CommonPrefix entries always pass through
        // For Object/DeleteMarker: all filters must pass (AND logic)
        self.filters.iter().all(|f| f.matches(entry))
    }
}
```

### Available Filters

| Filter | Reuse | Description |
|--------|-------|-------------|
| `IncludeRegexFilter` | From s3rm-rs | Key must match regex |
| `ExcludeRegexFilter` | From s3rm-rs | Key must NOT match regex |
| `MtimeBeforeFilter` | From s3rm-rs | Modified before timestamp |
| `MtimeAfterFilter` | From s3rm-rs | Modified at or after timestamp |
| `SmallerSizeFilter` | From s3rm-rs | Size < threshold |
| `LargerSizeFilter` | From s3rm-rs | Size >= threshold |
| `StorageClassFilter` | New | Storage class in comma-separated list |

Filters are applied inline within the lister. CommonPrefix entries bypass all filters.

## Lister Behavior

| Mode | Delimiter | Parallel Listing | Emits |
|------|-----------|-----------------|-------|
| Non-recursive (default) | `/` | No | `Object` + `CommonPrefix` |
| Recursive (`--recursive`) | None | Yes | `Object` only |
| Recursive + `--all-versions` | None | Yes (via `list_object_versions`) | `Object` + `DeleteMarker` |
| Non-recursive + `--all-versions` | `/` | No | `Object` + `DeleteMarker` + `CommonPrefix` |

Non-recursive mode uses the S3 API's delimiter feature directly. Parallel listing only applies to recursive mode.

## Aggregate Stage

### Flow

1. Drain channel into `Vec<ListEntry>`
2. Sort by the `--sort` field (default: `key`):
   - Key: lexicographic
   - Size: numeric (CommonPrefix sorts as size 0)
   - Date: chronological (CommonPrefix sorts last)
   - When `--all-versions`: secondary sort by `last_modified` (chronological) to ensure deterministic output across parallel executions
3. If `--reverse`: reverse the Vec
4. Format and write each entry to `BufWriter<Stdout>`
5. If `--summary`: append summary line

### Output Formats

**Default (text):**
```
                           PRE logs/
2024-01-15T10:30:00Z       1234 readme.txt
2024-01-15T11:00:00Z    5678901 data.csv
```

**With `--human`:**
```
2024-01-15T10:30:00Z    1.2KiB readme.txt
2024-01-15T11:00:00Z    5.4MiB data.csv
```

**With `--show-etag --show-storage-class`:**
```
2024-01-15T10:30:00Z       1234 STANDARD "abc123" readme.txt
```

**With `--all-versions` (versioned objects and delete markers):**
```
2024-01-15T10:30:00Z       1234 abc123-version-id readme.txt
2024-01-16T09:00:00Z          0 def456-version-id (delete marker) readme.txt
```

**With `--json` (NDJSON):**
```json
{"key":"readme.txt","size":1234,"last_modified":"2024-01-15T10:30:00Z"}
{"key":"data.csv","size":5678901,"last_modified":"2024-01-15T11:00:00Z"}
```
- JSON includes all available fields regardless of display flags
- `--summary` appends: `{"summary":{"total_objects":2,"total_size":5680135}}`

**Summary line (text):**
```
Total: 2 objects, 5.4MiB
```
Summary always uses human-readable sizes.

## Config Struct

```rust
pub struct Config {
    // Target
    pub target: S3Target,
    pub storage: Box<dyn StorageTrait>,

    // Listing mode
    pub recursive: bool,
    pub all_versions: bool,

    // Filtering
    pub filter_include_regex: Option<String>,
    pub filter_exclude_regex: Option<String>,
    pub filter_mtime_before: Option<DateTime<Utc>>,
    pub filter_mtime_after: Option<DateTime<Utc>>,
    pub filter_smaller_size: Option<u64>,
    pub filter_larger_size: Option<u64>,
    pub storage_class: Option<Vec<String>>,

    // Sort
    pub sort: Option<SortField>,
    pub reverse: bool,

    // Display
    pub summary: bool,
    pub human: bool,
    pub show_fullpath: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub json: bool,

    // Performance
    pub max_parallel_listings: u16,
    pub max_parallel_listing_max_depth: u16,
    pub object_listing_queue_size: u32,
    pub allow_parallel_listings_in_express_one_zone: bool,

    // AWS/Retry/Timeout (reused from s3rm-rs)
    pub aws_max_attempts: u32,
    pub initial_backoff_milliseconds: u64,
    pub operation_timeout_milliseconds: Option<u64>,
    pub operation_attempt_timeout_milliseconds: Option<u64>,
    pub connect_timeout_milliseconds: Option<u64>,
    pub read_timeout_milliseconds: Option<u64>,
    pub max_keys: i32,

    // Tracing
    pub tracing_config: TracingConfig,
}

pub struct TracingConfig {
    pub json_tracing: bool,
    pub aws_sdk_tracing: bool,
    pub span_events_tracing: bool,
    pub disable_color_tracing: bool,
}
```

## Testing Strategy

### Mock Layer

```rust
pub struct MockStorage {
    objects: Vec<ListEntry>,
}
impl StorageTrait for MockStorage {
    // Returns objects from the Vec, simulating S3 responses
}
```

### Unit Tests (with mocks)

| Area | Reuse from s3rm-rs |
|------|--------------------|
| Argument parsing | ~90% |
| Config building | ~80% |
| Each filter | ~95% |
| Filter chain composition | ~90% |
| S3Target parsing | 100% |
| Aggregate sorting | New |
| Aggregate formatting (text, NDJSON, extra columns) | New |
| Aggregate summary | New |
| Non-recursive listing (CommonPrefix) | New |
| ListEntry type | Adapted |

### E2E Tests (gated by `E2E_TEST` env flag)

- Basic listing (recursive, non-recursive)
- Parallel listing throughput
- All-versions with delete markers
- Filters with real S3 data
- Sort and output formatting
- Storage class filtering

### Coverage Target: ~99%

## Implementation Staging

### Step 1: Argument Design (Clap)
- Copy and adapt `config/args/` from s3rm-rs
- Strip deletion args, add s3ls args (sort, display, storage_class filter)
- Reuse value parsers (mtime, size)
- Unit tests: port and adapt argument parsing tests (all flag combinations, env vars, defaults, validation)
- Deliverable: `s3ls --help` works, all arg tests pass

### Step 2: Empty Pipeline
- Scaffold `ListingPipeline` with `run()`
- Wire up `Config` and `build_config_from_args()`
- Create `main.rs` entry point (tracing init, config load, pipeline run)
- Copy and adapt `StorageTrait`, `S3Storage`, `client_builder`
- Unit tests: Config building from args, S3Target parsing, MockStorage setup
- Deliverable: pipeline runs, returns success, does nothing, all tests pass

### Step 3: Listing Stage
- Copy and adapt `lister.rs` from s3rm-rs
- Emit `ListEntry` (including `CommonPrefix` in non-recursive mode)
- Parallel listing for recursive mode, delimiter listing for non-recursive
- Temporary aggregate: drain and print keys
- Unit tests: ListEntry type, lister with MockStorage (recursive, non-recursive, all-versions, CommonPrefix emission)
- Deliverable: list real bucket objects, all tests pass

### Step 4: Filtering Stage
- Copy and adapt `filters/` from s3rm-rs
- Adapt to `ListEntry` (CommonPrefix passes through)
- Add `StorageClassFilter`
- Inline filters into lister
- Unit tests: each filter individually, FilterChain composition, CommonPrefix passthrough, StorageClassFilter
- Deliverable: filtered listing works, all tests pass

### Step 5: Aggregate Stage
- Implement collect, sort, format, output
- All display options: text, human, NDJSON, extra columns
- Summary line (when `--summary`)
- Non-recursive `PRE` formatting
- Unit tests: sort by key/size/date, reverse, all formatting combinations (text, human, NDJSON, extra columns), summary output, PRE formatting
- Deliverable: complete s3ls tool, all tests pass

### Step 6: E2E Tests (human-directed)
- Human instructs Claude Code to generate E2E tests one by one
- Tests hit real S3 or MinIO, gated by `E2E_TEST` env flag
- Scope: basic listing, recursive/non-recursive, parallel listing, all-versions, filters, sort/output, storage class filter
- Deliverable: E2E test suite, ~99% coverage
