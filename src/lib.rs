//! This crate is intended to be used as a binary crate (`s3ls`) and is not
//! intended for use as a library in any way. The public items below exist
//! only to support the binary and integration tests; no API stability is
//! provided and external consumers should not depend on them.

pub mod aggregate;
pub mod bucket_lister;
pub mod config;
pub mod display;
pub mod display_writer;
pub mod filters;
pub mod lister;
pub mod pipeline;
pub mod storage;
pub mod types;

// Core pipeline
pub use pipeline::ListingPipeline;

pub use config::args::{CLIArgs, SortField, build_config_from_args, parse_from_args};
// Configuration
pub use config::Config;

// Object types
pub use types::{
    AccessKeys, ClientConfigLocation, ListEntry, ListingStatistics, S3Credentials, S3Object,
    S3Target,
};

// Error types
pub use types::error::{S3lsError, exit_code_from_error, is_cancelled_error};

// Cancellation token
pub use types::token::{PipelineCancellationToken, create_pipeline_cancellation_token};
