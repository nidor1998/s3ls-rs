# Step 2: Empty Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold the ListingPipeline, Config struct (with nested types), main.rs entry point, and cancellation token so the binary compiles and runs (doing nothing).

**Architecture:** Build `Config` struct with **nested sub-structs** (`FilterConfig`, `DisplayConfig`, `ClientConfig`, `TracingConfig`) matching the design spec. Wire `build_config_from_args()` from CLIArgs to Config using helper methods (`build_filter_config`, `build_client_config`, `build_tracing_config`). Scaffold `ListingPipeline::new(config, cancellation_token)` as a no-op. The `main.rs` binary parses args, initializes tracing via `tracing_init` module, spawns a Ctrl+C handler, builds config, and runs the pipeline.

**Tech Stack:** Rust 2024, tokio (async runtime), aws-sdk-s3, aws-config, tracing, tracing-subscriber, async-trait, tokio-util (CancellationToken), zeroize

**Depends on:** Step 1 (CLIArgs and parse_from_args must be complete)

**Reference:** s3rm-rs source at https://github.com/nidor1998/s3rm-rs

**Status:** COMPLETED

---

### Task 1: Add Step 2 dependencies to Cargo.toml

**Files:**
- Modify: `Cargo.toml`

- [x] **Step 1: Add async runtime, AWS SDK, tracing, and security dependencies**

Actual `Cargo.toml` additions:

```toml
# Async runtime
async-trait = "0.1.89"
tokio = { version = "1.50.0", features = ["full"] }
tokio-util = "0.7.18"

# AWS SDK
aws-config = { version = "1.8.15", features = ["behavior-version-latest"] }
aws-runtime = "1.7.2"
aws-sdk-s3 = "1.127.0"
aws-smithy-runtime-api = "1.11.6"
aws-smithy-types = "1.4.7"
aws-types = "1.3.14"

# Security
zeroize = "1.8.2"
zeroize_derive = "1.4.3"

# Logging
tracing = "0.1.44"
tracing-subscriber = { version = "0.3.23", features = ["env-filter", "json", "local-time"] }
```

Dev dependencies added:
```toml
[dev-dependencies]
proptest = "1.11"
once_cell = "1.21.4"
nix = { version = "0.31.2", features = ["process", "signal"] }
rusty-fork = "0.3.1"
```

- [x] **Step 2: Verify dependencies resolve**
- [x] **Step 3: Commit**

---

### Task 2: Create types module (S3Object, S3Target, ListEntry, error, token)

**Files:**
- Create: `src/types/mod.rs`
- Create: `src/types/error.rs`
- Create: `src/types/token.rs`
- Modify: `src/lib.rs`

- [x] **Step 1: Write S3Target, ListEntry, S3Object, ListingStatistics**

Types module includes S3Target with `parse()` and `Display`, S3Object (NotVersioning/Versioning enum), ListEntry (Object/CommonPrefix/DeleteMarker), and ListingStatistics.

- [x] **Step 2: Write `src/types/error.rs` with S3lsError**

Includes `exit_code_from_error` and `is_cancelled_error` helper functions.

- [x] **Step 3: Write `src/types/token.rs` with PipelineCancellationToken**

```rust
/// A cancellation token used to signal pipeline shutdown.
pub type PipelineCancellationToken = tokio_util::sync::CancellationToken;

pub fn create_pipeline_cancellation_token() -> PipelineCancellationToken {
    tokio_util::sync::CancellationToken::new()
}
```

- [x] **Step 4: Update `src/lib.rs`**

```rust
pub mod config;
pub mod pipeline;
pub mod types;

pub use pipeline::ListingPipeline;
pub use config::args::{build_config_from_args, parse_from_args, CLIArgs, SortField};
pub use config::Config;
pub use types::{ListEntry, ListingStatistics, S3Object, S3Target};
pub use types::error::{exit_code_from_error, is_cancelled_error, S3lsError};
pub use types::token::{create_pipeline_cancellation_token, PipelineCancellationToken};
```

- [x] **Step 5: Verify tests pass**
- [x] **Step 6: Commit**

---

### Task 3: Create Config struct with nested types and build_config_from_args

**Files:**
- Modify: `src/config/mod.rs`
- Modify: `src/config/args/mod.rs`
- Modify: `src/lib.rs`

- [x] **Step 1: Write `src/config/mod.rs` with Config and nested structs**

The actual Config uses **nested structs** instead of flat fields:

```rust
#[derive(Debug, Clone)]
pub struct Config {
    pub target: S3Target,
    pub recursive: bool,
    pub all_versions: bool,
    pub filter_config: FilterConfig,       // nested struct
    pub sort: SortField,                   // NOT Option - always set, default Key
    pub reverse: bool,
    pub display_config: DisplayConfig,     // nested struct
    pub max_parallel_listings: u16,
    pub max_parallel_listing_max_depth: u16,
    pub object_listing_queue_size: u32,
    pub allow_parallel_listings_in_express_one_zone: bool,
    pub target_client_config: Option<ClientConfig>,  // nested struct, Optional
    pub max_keys: i32,
    pub auto_complete_shell: Option<Shell>,
    pub tracing_config: Option<TracingConfig>,
}

#[derive(Debug, Clone, Default)]
pub struct FilterConfig {
    pub include_regex: Option<String>,
    pub exclude_regex: Option<String>,
    pub mtime_before: Option<DateTime<Utc>>,
    pub mtime_after: Option<DateTime<Utc>>,
    pub smaller_size: Option<u64>,
    pub larger_size: Option<u64>,
    pub storage_class: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct DisplayConfig {
    pub summary: bool,
    pub human: bool,
    pub show_fullpath: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub json: bool,
}

#[derive(Debug, Clone)]
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
```

Also includes:
- `S3Credentials` enum (Profile/Credentials/FromEnvironment)
- `AccessKeys` with `zeroize_derive::Zeroize` and `ZeroizeOnDrop` for secure memory clearing
- `RetryConfig` and `CLITimeoutConfig` nested structs
- `TracingConfig` struct
- `Config::default()` and `Config::for_target(bucket, prefix)` helper

- [x] **Step 2: Add TryFrom<CLIArgs> for Config using helper methods**

The `TryFrom` implementation uses three builder helpers on `CLIArgs`:

```rust
impl CLIArgs {
    fn parse_target(&self) -> Result<S3Target, String> { ... }
    fn build_filter_config(&self) -> Result<FilterConfig, String> { ... }
    fn build_client_config(&self) -> Option<ClientConfig> { ... }
    fn build_tracing_config(&self) -> Option<TracingConfig> { ... }
}

impl TryFrom<CLIArgs> for Config {
    type Error = String;
    fn try_from(args: CLIArgs) -> Result<Self, Self::Error> {
        let target = args.parse_target()?;
        let filter_config = args.build_filter_config()?;
        let target_client_config = args.build_client_config();
        let tracing_config = args.build_tracing_config();

        Ok(Config {
            target,
            recursive: args.recursive,
            all_versions: args.all_versions,
            filter_config,
            sort: args.sort,                // SortField directly, not Option
            reverse: args.reverse,
            display_config: DisplayConfig {
                summary: args.summary,
                human: args.human,
                show_fullpath: args.show_fullpath,
                show_etag: args.show_etag,
                show_storage_class: args.show_storage_class,
                show_checksum_algorithm: args.show_checksum_algorithm,
                show_checksum_type: args.show_checksum_type,
                json: args.json,
            },
            max_parallel_listings: args.max_parallel_listings,
            // ... etc
            target_client_config,
            max_keys: args.max_keys,
            auto_complete_shell: args.auto_complete_shell,
            tracing_config,
        })
    }
}
```

- [x] **Step 3: Add `build_config_from_args` public function**

```rust
pub fn build_config_from_args<I, T>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli_args = CLIArgs::try_parse_from(args).map_err(|e| e.to_string())?;
    Config::try_from(cli_args)
}
```

- [x] **Step 4: Run all tests**
- [x] **Step 5: Commit**

---

### Task 4: Create ListingPipeline scaffold with cancellation token

**Files:**
- Create: `src/pipeline.rs`
- Modify: `src/lib.rs`

- [x] **Step 1: Implement the pipeline scaffold**

The pipeline takes a `PipelineCancellationToken` (which is `tokio_util::sync::CancellationToken`):

```rust
use crate::config::Config;
use crate::types::token::PipelineCancellationToken;
use anyhow::Result;

pub struct ListingPipeline {
    config: Config,
    cancellation_token: PipelineCancellationToken,
}

impl ListingPipeline {
    pub fn new(config: Config, cancellation_token: PipelineCancellationToken) -> Self {
        Self { config, cancellation_token }
    }

    pub async fn run(self) -> Result<()> {
        tracing::info!(
            target = %self.config.target,
            recursive = self.config.recursive,
            "Starting listing pipeline"
        );

        if self.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Step 3+ will implement stages
        Ok(())
    }
}
```

- [x] **Step 2: Add tests including cancellation test**

```rust
#[tokio::test]
async fn pipeline_runs_and_returns_success() {
    let config = Config::try_from(
        crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap()
    ).unwrap();
    let token = create_pipeline_cancellation_token();
    let pipeline = ListingPipeline::new(config, token);
    let result = pipeline.run().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn pipeline_respects_cancellation() {
    let config = Config::try_from(
        crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap()
    ).unwrap();
    let token = create_pipeline_cancellation_token();
    token.cancel();
    let pipeline = ListingPipeline::new(config, token);
    let result = pipeline.run().await;
    assert!(result.is_ok());
}
```

- [x] **Step 3: Run tests**
- [x] **Step 4: Commit**

---

### Task 5: Create main.rs entry point with tracing_init and ctrl_c_handler

**Files:**
- Create: `src/bin/s3ls/main.rs`
- Create: `src/bin/s3ls/tracing_init.rs`
- Create: `src/bin/s3ls/ctrl_c_handler/mod.rs`

- [x] **Step 1: Write `src/bin/s3ls/main.rs`**

The actual main.rs uses a modular structure with separate `tracing_init` and `ctrl_c_handler` modules:

```rust
use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use tracing::{debug, error, trace};

use s3ls_rs::config::Config;
use s3ls_rs::{
    create_pipeline_cancellation_token, exit_code_from_error, is_cancelled_error, CLIArgs,
    ListingPipeline,
};

mod ctrl_c_handler;
mod tracing_init;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config_exit_if_err();

    if let Some(shell) = config.auto_complete_shell {
        generate(shell, &mut CLIArgs::command(), "s3ls", &mut std::io::stdout());
        return Ok(());
    }

    start_tracing_if_necessary(&config);
    trace!("config = {:?}", config);
    run(config).await
}

fn load_config_exit_if_err() -> Config {
    match Config::try_from(CLIArgs::parse()) {
        Ok(config) => config,
        Err(error_message) => {
            clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
        }
    }
}

async fn run(config: Config) -> Result<()> {
    let cancellation_token = create_pipeline_cancellation_token();
    ctrl_c_handler::spawn_ctrl_c_handler(cancellation_token.clone());

    let start_time = tokio::time::Instant::now();
    debug!("listing pipeline start.");

    let pipeline = ListingPipeline::new(config, cancellation_token);

    match pipeline.run().await {
        Ok(()) => {
            let duration_sec = format!("{:.3}", start_time.elapsed().as_secs_f32());
            debug!(duration_sec = duration_sec, "s3ls has been completed.");
            Ok(())
        }
        Err(e) => {
            if is_cancelled_error(&e) {
                debug!("listing cancelled by user.");
                return Ok(());
            }
            let code = exit_code_from_error(&e);
            error!("{}", e);
            std::process::exit(code);
        }
    }
}
```

- [x] **Step 2: Write `src/bin/s3ls/tracing_init.rs`**

Uses tracing-subscriber (not log directly), following the s3rm-rs pattern.

- [x] **Step 3: Write `src/bin/s3ls/ctrl_c_handler/mod.rs`**

Spawns a tokio task that listens for Ctrl+C and cancels the pipeline token.

- [x] **Step 4: Verify the binary compiles and runs**
- [x] **Step 5: Commit**
