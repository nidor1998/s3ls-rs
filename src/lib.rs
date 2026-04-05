pub mod aggregate;
pub mod bucket_lister;
pub mod config;
pub mod filters;
pub mod lister;
pub mod pipeline;
pub mod storage;
pub mod types;

// Core pipeline
pub use pipeline::ListingPipeline;

// Configuration
pub use config::Config;
pub use config::args::{CLIArgs, SortField, build_config_from_args, parse_from_args};

// Object types
pub use types::{
    AccessKeys, ClientConfigLocation, ListEntry, ListingStatistics, S3Credentials, S3Object,
    S3Target,
};

// Error types
pub use types::error::{S3lsError, exit_code_from_error, is_cancelled_error};

// Cancellation token
pub use types::token::{PipelineCancellationToken, create_pipeline_cancellation_token};
