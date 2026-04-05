# s3ls-rs Design Specification

## Overview

s3ls-rs is a fast S3 object listing tool that reuses over 95% of the source code from [nidor1998/s3rm-rs](https://github.com/nidor1998/s3rm-rs). Its key feature is fast object listing (approximately 100,000 objects per second) utilizing s3rm-rs's parallel listing functionality.

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
1. ListingPipeline::new(config, cancellation_token)
2. pipeline.run():
   a. Check cancellation token — return early if already cancelled
   b. Create Lister stage with FilterChain (spawn async task)
   c. Create Aggregate stage (spawn async task)
   d. Wait for Aggregate to complete
3. Return exit code (and statistics if --summarize)
```

The pipeline accepts a `PipelineCancellationToken` (a `tokio_util::sync::CancellationToken` type alias) that enables graceful shutdown. The Ctrl+C handler in the binary cancels this token to stop the pipeline.

## Project Structure

```
s3ls-rs/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # Public API exports
│   ├── bin/
│   │   └── s3ls/
│   │       ├── main.rs           # CLI entry point
│   │       ├── ctrl_c_handler/
│   │       │   └── mod.rs        # Ctrl+C signal handler (adapted from s3sync)
│   │       └── tracing_init.rs   # Tracing subscriber init (adapted from s3sync)
│   ├── config/
│   │   ├── mod.rs                # Config, FilterConfig, DisplayConfig, ClientConfig, etc.
│   │   └── args/
│   │       ├── mod.rs            # CLIArgs (Clap) — adapted from s3rm-rs
│   │       ├── tests.rs          # Argument parsing tests
│   │       └── value_parser/     # Custom Clap value parsers (reused)
│   ├── types/
│   │   ├── mod.rs                # S3Object, S3Target, ListEntry, ListingStatistics
│   │   ├── error.rs              # S3lsError enum + helper functions
│   │   └── token.rs              # PipelineCancellationToken type alias
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
│   ├── bucket_lister.rs          # NEW — list buckets (when no target specified)
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

**New:** `aggregate.rs`, `bucket_lister.rs`, `filters/storage_class.rs`, `config/args/value_parser/storage_class.rs`, `types/token.rs`, `bin/s3ls/ctrl_c_handler/mod.rs`, `bin/s3ls/tracing_init.rs`

## CLI Arguments

```
Usage: s3ls [OPTIONS] [TARGET]

Arguments:
  [TARGET] s3://<BUCKET_NAME>[/prefix] (omit to list buckets) [env: TARGET=]

Options:
  -v, --verbose...    Increase logging verbosity
  -q, --quiet...      Decrease logging verbosity
  -h, --help          Print help (see more with '--help')
  -V, --version       Print version
```

When no `[TARGET]` is specified, s3ls enters **bucket listing mode** — lists all buckets with creation date and name.

### General

| Flag | Description | Env | Default |
|------|-------------|-----|---------|
| `--recursive` | List all objects recursively | `RECURSIVE` | false |
| `--all-versions` | All versions including delete markers | `LIST_ALL_VERSIONS` | false |
| `--hide-delete-marker` | Hide delete markers from version listing (requires `--all-versions`) | - | false |
| `--max-depth <N>` | Maximum depth for recursive listing, minimum 1 (requires `--recursive`) | `MAX_DEPTH` | - |
| `--bucket-name-prefix <PREFIX>` | Filter buckets by name prefix (bucket listing mode) | - | - |
| `--list-express-one-zone-buckets` | List only Express One Zone directory buckets (bucket listing mode) | - | false |

### Filtering

| Flag | Description | Env |
|------|-------------|-----|
| `--filter-include-regex <REGEX>` | List only objects whose key matches | `FILTER_INCLUDE_REGEX` |
| `--filter-exclude-regex <REGEX>` | Skip objects whose key matches | `FILTER_EXCLUDE_REGEX` |
| `--filter-mtime-before <TIME>` | Objects modified before this time | `FILTER_MTIME_BEFORE` |
| `--filter-mtime-after <TIME>` | Objects modified at or after this time | `FILTER_MTIME_AFTER` |
| `--filter-smaller-size <SIZE>` | Objects smaller than this size | `FILTER_SMALLER_SIZE` |
| `--filter-larger-size <SIZE>` | Objects larger than or equal to this size | `FILTER_LARGER_SIZE` |
| `--storage-class <LIST>` | Comma-separated storage classes to include (validated against AWS SDK) | `STORAGE_CLASS` |

### Sort

| Flag | Description | Default |
|------|-------------|---------|
| `--sort <FIELD>[,<FIELD>]` | Sort by up to 2 comma-separated fields: `key`, `size`, `date`, `bucket` | `key` |
| `--reverse` | Reverse sort order | false |
| `--no-sort` | Disable sorting and stream results directly (reduces memory usage; conflicts with `--sort`, `--reverse`) | false |

Sort accepts up to 2 comma-separated fields (e.g. `--sort date,key`). No duplicate fields allowed. The user's specification is the complete sort order — no hidden tie-breakers.

When `--all-versions` is enabled and only one sort field is specified, `date` is automatically appended as a secondary sort so versions of the same key appear in chronological order. The `bucket` sort field is intended for bucket listing mode but also works in object listings (behaves same as `key`).

When `--no-sort` is set, entries are streamed directly to stdout as they arrive from the lister, bypassing the collect-sort-output aggregate path. This eliminates memory buffering and is useful for large listings where order doesn't matter. Summary statistics (`--summarize`) are computed incrementally in this mode.

### Display

| Flag | Description |
|------|-------------|
| `--summarize` | Append summary line (total count, total size) |
| `--human-readable` | Human-readable sizes (e.g., `1.2KiB`); also affects `--summarize` |
| `--show-relative-path` | Show key relative to prefix instead of full path (default: full path) |
| `--show-etag` | Show ETag column |
| `--show-storage-class` | Show storage class column |
| `--show-checksum-algorithm` | Show checksum algorithm column |
| `--show-checksum-type` | Show checksum type column |
| `--show-is-latest` | Show LATEST/NOT_LATEST column (requires `--all-versions`) |
| `--show-owner` | Show owner DisplayName and ID columns; enables `fetch-owner` on S3 API |
| `--show-restore-status` | Show restore status column; enables `OptionalObjectAttributes=RestoreStatus` on S3 API |
| `--show-bucket-arn` | Show bucket ARN column (bucket listing mode) |
| `--header` | Add a header row with column names (text mode only) |
| `--json` | Output as NDJSON (one JSON object per line); includes all available fields |

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
| `--max-parallel-listings <N>` | Concurrent listing operations | `MAX_PARALLEL_LISTINGS` | 32 |
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
| `--max-keys <N>` | Max objects per list request (1..=1000) | `MAX_KEYS` | 1000 |
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
        owner_display_name: Option<String>,
        owner_id: Option<String>,
        restore_status: Option<String>,
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
        owner_display_name: Option<String>,
        owner_id: Option<String>,
        restore_status: Option<String>,
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
    Bucket,
}
```

### ListingStatistics (only computed when --summarize)

```rust
pub struct ListingStatistics {
    pub total_objects: u64,
    pub total_size: u64,
    pub total_versions: u64,       // when --all-versions
    pub total_delete_markers: u64, // when --all-versions
}
```

### S3lsError

```rust
#[derive(Error, Debug)]
pub enum S3lsError {
    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Listing error: {0}")]
    ListingError(String),

    #[error("Pipeline cancelled")]
    Cancelled,
}
```

Helper functions:

```rust
/// Check if an error represents a user-initiated cancellation (Ctrl+C).
pub fn is_cancelled_error(err: &anyhow::Error) -> bool;

/// Map an error to an appropriate process exit code.
/// Cancelled -> 0, InvalidConfig/InvalidUri -> 2, ListingError -> 1, other -> 1.
pub fn exit_code_from_error(err: &anyhow::Error) -> i32;
```

### PipelineCancellationToken

```rust
/// Type alias for tokio_util::sync::CancellationToken.
pub type PipelineCancellationToken = tokio_util::sync::CancellationToken;

/// Create a new PipelineCancellationToken.
pub fn create_pipeline_cancellation_token() -> PipelineCancellationToken;
```

## Config Struct

The configuration uses nested structs to organize related settings, following the s3rm-rs pattern.

```rust
pub struct Config {
    // Target
    pub target: S3Target,

    // Listing mode
    pub recursive: bool,
    pub all_versions: bool,
    pub hide_delete_marker: bool,
    pub max_depth: Option<u16>,
    pub bucket_name_prefix: Option<String>,
    pub list_express_one_zone_bucket: bool,

    // Filtering (nested)
    pub filter_config: FilterConfig,

    // Sort — always set, defaults to vec![Key]
    pub sort: Vec<SortField>,
    pub reverse: bool,
    pub no_sort: bool,

    // Display (nested)
    pub display_config: DisplayConfig,

    // Performance
    pub max_parallel_listings: u16,
    pub max_parallel_listing_max_depth: u16,
    pub object_listing_queue_size: u32,
    pub allow_parallel_listings_in_express_one_zone: bool,

    // AWS Client (nested, None when using default credential chain)
    pub target_client_config: Option<ClientConfig>,

    // Advanced
    pub max_keys: i32,
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,

    // Tracing (None when no tracing flags are set)
    pub tracing_config: Option<TracingConfig>,
}

pub struct FilterConfig {
    pub include_regex: Option<String>,
    pub exclude_regex: Option<String>,
    pub mtime_before: Option<DateTime<Utc>>,
    pub mtime_after: Option<DateTime<Utc>>,
    pub smaller_size: Option<u64>,
    pub larger_size: Option<u64>,
    pub storage_class: Option<Vec<String>>,
}

pub struct DisplayConfig {
    pub summary: bool,
    pub human: bool,
    pub show_relative_path: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub show_is_latest: bool,
    pub show_owner: bool,         // enables fetch-owner on ListObjectsV2; also works in bucket listing
    pub show_restore_status: bool, // enables OptionalObjectAttributes=RestoreStatus on ListObjectsV2
    pub show_bucket_arn: bool,    // bucket ARN column in bucket listing mode
    pub header: bool,
    pub json: bool,
}

pub struct ClientConfig {
    pub aws_config_file: Option<PathBuf>,
    pub aws_shared_credentials_file: Option<PathBuf>,
    pub credential: S3Credentials,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub accelerate: bool,
    pub request_payer: bool,
    pub retry_config: RetryConfig,
    pub cli_timeout_config: CLITimeoutConfig,
    pub disable_stalled_stream_protection: bool,
}

pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
}

// Custom Debug impl redacts secrets in log output.
// S3Credentials::Credentials shows AccessKeys with redacted fields.

/// AWS access key pair with secure zeroization (zeroize crate).
/// secret_access_key and session_token are securely cleared from memory on drop.
pub struct AccessKeys {
    pub access_key: String,
    pub secret_access_key: String,   // Debug: "** redacted **"
    pub session_token: Option<String>, // Debug: "** redacted **" when Some
}

pub struct RetryConfig {
    pub aws_max_attempts: u32,
    pub initial_backoff_milliseconds: u64,
}

pub struct CLITimeoutConfig {
    pub operation_timeout_milliseconds: Option<u64>,
    pub operation_attempt_timeout_milliseconds: Option<u64>,
    pub connect_timeout_milliseconds: Option<u64>,
    pub read_timeout_milliseconds: Option<u64>,
}

pub struct TracingConfig {
    pub tracing_level: log::Level,
    pub json_tracing: bool,
    pub aws_sdk_tracing: bool,
    pub span_events_tracing: bool,
    pub disable_color_tracing: bool,
}
```

## Binary Structure (main.rs)

The CLI binary (`src/bin/s3ls/main.rs`) is organized into three modules:

- **`ctrl_c_handler`** (adapted from s3sync) — spawns an async task that listens for Ctrl+C signals and cancels the pipeline token
- **`tracing_init`** (adapted from s3sync) — initializes the tracing subscriber

### Entry Point Flow

```
main():
  1. load_config_exit_if_err()
     — Parse CLIArgs, build Config, exit with clap error on failure
  2. If auto_complete_shell is set:
     — Generate shell completions via clap_complete::generate() and return
  3. start_tracing_if_necessary(&config)
     — Init tracing subscriber if config has tracing_config
  4. run(config).await
```

### run() Function

```
run(config):
  1. If target is empty (bucket listing mode):
     — Call bucket_lister::list_buckets(&config)
     — Handle BrokenPipe silently, exit on other errors
     — Return
  2. Create cancellation token
  3. Spawn Ctrl+C handler with token clone
  4. Record start time
  5. Create ListingPipeline::new(config, cancellation_token)
  6. pipeline.run().await
  7. On Ok: log completion with duration
  8. On Err:
     — If BrokenPipe (e.g. piped to head/tail): return Ok silently
     — If is_cancelled_error: log "cancelled by user", return Ok
     — Otherwise: exit_code_from_error, log error, std::process::exit(code)
```

### Tracing Initialization

The tracing subscriber is configured with:
- **Format:** compact, stderr writer (via `PipeSafeWriter` that silently ignores BrokenPipe errors)
- **Crate-specific filters:** `s3ls_rs={level},s3ls={level}`
- **AWS SDK tracing:** additionally includes `aws_smithy_runtime`, `aws_config`, `aws_sigv4` filters
- **RUST_LOG support:** falls back to `RUST_LOG` env var when aws_sdk_tracing is not enabled
- **Span events:** NEW + CLOSE when `--span-events-tracing` is set
- **Terminal detection:** ANSI colors enabled only when stderr is a terminal (and `--disable-color-tracing` is not set)
- **JSON mode:** uses `.json()` format when `--json-tracing` is set

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
| Recursive + `--max-depth N` | None/`/` | Yes | `Object` + `CommonPrefix` at boundary |

Non-recursive mode uses the S3 API's delimiter feature directly. Parallel listing only applies to recursive mode.

When `--max-depth N` is set with `--recursive`, the listing engine limits recursion depth. At the boundary depth, sub-prefixes are emitted as `CommonPrefix` ("PRE") entries instead of being recursed into, mimicking non-recursive listing at that level. In sequential fallback mode, objects beyond the depth limit are replaced with synthetic deduplicated `CommonPrefix` entries.

## Aggregate Stage

### Flow

1. Drain channel into `Vec<ListEntry>`
2. Sort by `--sort` fields (default: `[key]`), chaining comparisons with `.then_with()`:
   - Key/Bucket: lexicographic
   - Size: numeric (CommonPrefix sorts as size 0)
   - Date: chronological (CommonPrefix sorts last)
   - When `--all-versions` with single sort field: `date` auto-appended as secondary
3. If `--reverse`: reverse the combined comparison
4. If `--header` and not `--json`: write header row
5. Format and write each entry to `BufWriter<Stdout>` (tab-delimited)
6. If `--summarize`: write blank line then summary line

### Output Formats

All text output is **tab-delimited** for consistent parsing with `awk -F'\t'`, `cut`, etc.

**Default (text):**
```
	PRE	logs/
2024-01-15T10:30:00Z	1234	readme.txt
2024-01-15T11:00:00Z	5678901	data.csv
```

**With `--header`:**
```
DATE	SIZE	KEY
2024-01-15T10:30:00Z	1234	readme.txt
```

**With `--show-etag --show-storage-class`:**
```
2024-01-15T10:30:00Z	1234	STANDARD	abc123	readme.txt
```
PRE rows show empty strings for optional columns.

**With `--all-versions` (versioned objects and delete markers):**
```
2024-01-15T10:30:00Z	1234	abc123-version-id	readme.txt
2024-01-16T09:00:00Z	DELETE	def456-version-id	readme.txt
```
Delete markers show `DELETE` in the size column instead of `0`.

**With `--show-is-latest --all-versions`:**
```
2024-01-15T10:30:00Z	1234	abc123-version-id	LATEST	readme.txt
2024-01-15T09:00:00Z	1000	def456-version-id	NOT_LATEST	readme.txt
```
Column values are padded to equal width (10 chars).

**With `--json` (NDJSON):**
```json
{"Key":"readme.txt","LastModified":"2024-01-15T10:30:00+00:00","ETag":"\"abc123\"","Size":1234,"StorageClass":"STANDARD"}
{"Key":"data.csv","LastModified":"2024-01-15T11:00:00+00:00","ETag":"\"def456\"","Size":5678901,"StorageClass":"STANDARD"}
```
- JSON uses PascalCase keys matching the S3 API (Key, Size, LastModified, ETag, StorageClass, etc.)
- JSON always includes all available fields regardless of `--show-*` flags
- ETag includes double quotes as returned by the S3 API
- `ChecksumAlgorithm` is an array (e.g., `["CRC64NVME"]`)
- `Owner` is a nested object: `{"DisplayName":"...","ID":"..."}`
- `RestoreStatus` is a nested object: `{"IsRestoreInProgress":true,"RestoreExpiryDate":"..."}`
- `--summarize` appends: `{"summary":{"total_objects":2,"total_size":5680135}}`
- CommonPrefix entries: `{"Prefix":"logs/"}`
- Delete markers: `{"Key":"...","VersionId":"...","IsLatest":true,"LastModified":"..."}`

**With `--show-owner`:**
```
2024-01-15T10:30:00Z	1234	John	abc123def456	readme.txt
```
Enables `fetch_owner(true)` on `ListObjectsV2`. `ListObjectVersions` returns owner by default.

**With `--show-restore-status`:**
```
2024-01-15T10:30:00Z	1234	in_progress=true,expiry=2026-04-10T00:00:00Z	readme.txt
```
Enables `OptionalObjectAttributes=RestoreStatus` on `ListObjectsV2`.

**Summary line (text):**
```

Total: 2 objects, 5.4MiB
```
A blank line separates data from summary. With `--human-readable`, sizes are human-readable; without, raw bytes: `Total: 2 objects, 5680135 bytes`.

**Bucket listing mode (no target):**
```
DATE	REGION	BUCKET
2026-01-15T10:30:00Z	us-east-1	my-bucket
2026-03-28T11:55:00Z	ap-northeast-1	data.cpp17.org
```
Columns: DATE, REGION, BUCKET. Region is returned by S3 when using paginated `ListBuckets` (`max-buckets`). Supports `--header`, `--json`, `--sort`, `--reverse`, `--show-owner`, `--show-bucket-arn`, `--bucket-name-prefix`.

With `--show-owner`, adds OWNER_DISPLAY_NAME and OWNER_ID columns. With `--show-bucket-arn`, adds BUCKET_ARN column. With `--bucket-name-prefix`, filters buckets by name prefix (server-side for general buckets via S3 API, client-side for directory buckets).

**Bucket JSON output** uses S3 API field names: `Name`, `CreationDate`, `BucketRegion`, `BucketArn`, `Owner: {DisplayName, ID}`.

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
- Summary line (when `--summarize`)
- Non-recursive `PRE` formatting
- Unit tests: sort by key/size/date, reverse, all formatting combinations (text, human, NDJSON, extra columns), summary output, PRE formatting
- Deliverable: complete s3ls tool, all tests pass

### Step 6: E2E Tests (human-directed)
- Human instructs Claude Code to generate E2E tests one by one
- Tests hit real S3 or MinIO, gated by `E2E_TEST` env flag
- Scope: basic listing, recursive/non-recursive, parallel listing, all-versions, filters, sort/output, storage class filter
- Deliverable: E2E test suite, ~99% coverage
