pub mod args;

use crate::config::args::SortField;
use crate::types::S3Target;
use chrono::{DateTime, Utc};
use std::path::PathBuf;

pub struct Config {
    // Target
    pub target: S3Target,

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
    pub sort: SortField,
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

    // AWS Credentials
    pub aws_config_file: Option<PathBuf>,
    pub aws_shared_credentials_file: Option<PathBuf>,
    pub target_profile: Option<String>,
    pub target_access_key: Option<String>,
    pub target_secret_access_key: Option<String>,
    pub target_session_token: Option<String>,
    pub target_region: Option<String>,
    pub target_endpoint_url: Option<String>,
    pub target_force_path_style: bool,
    pub target_accelerate: bool,
    pub target_request_payer: bool,
    pub disable_stalled_stream_protection: bool,

    // Retry
    pub aws_max_attempts: u32,
    pub initial_backoff_milliseconds: u64,

    // Timeout
    pub operation_timeout_milliseconds: Option<u64>,
    pub operation_attempt_timeout_milliseconds: Option<u64>,
    pub connect_timeout_milliseconds: Option<u64>,
    pub read_timeout_milliseconds: Option<u64>,

    // Advanced
    pub max_keys: i32,

    // Tracing
    pub tracing_config: Option<TracingConfig>,
}

#[derive(Debug, Clone, Copy)]
pub struct TracingConfig {
    pub tracing_level: log::Level,
    pub json_tracing: bool,
    pub aws_sdk_tracing: bool,
    pub span_events_tracing: bool,
    pub disable_color_tracing: bool,
}
