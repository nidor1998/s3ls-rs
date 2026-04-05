pub mod args;

use crate::config::args::SortField;
use crate::types::{ClientConfigLocation, S3Credentials, S3Target};
use chrono::{DateTime, Utc};
use fancy_regex::Regex;

/// Main configuration for the s3ls-rs listing pipeline.
#[derive(Debug, Clone)]
pub struct Config {
    // Target
    pub target: S3Target,

    // Listing mode
    pub recursive: bool,
    pub all_versions: bool,

    // Filtering
    pub filter_config: FilterConfig,

    // Sort
    pub sort: Vec<SortField>,
    pub reverse: bool,

    // Display
    pub display_config: DisplayConfig,

    // Performance
    pub max_parallel_listings: u16,
    pub max_parallel_listing_max_depth: u16,
    pub object_listing_queue_size: u32,
    pub allow_parallel_listings_in_express_one_zone: bool,

    // AWS Client
    pub target_client_config: Option<ClientConfig>,

    // Advanced
    pub max_keys: i32,
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,

    // Tracing
    pub tracing_config: Option<TracingConfig>,
}

impl Config {
    /// Create a Config with sensible defaults for the given S3 bucket and prefix.
    pub fn for_target(bucket: &str, prefix: &str) -> Self {
        Config {
            target: S3Target {
                bucket: bucket.to_string(),
                prefix: if prefix.is_empty() {
                    None
                } else {
                    Some(prefix.to_string())
                },
            },
            ..Config::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            target: S3Target {
                bucket: String::new(),
                prefix: None,
            },
            recursive: false,
            all_versions: false,
            filter_config: FilterConfig::default(),
            sort: vec![SortField::Key],
            reverse: false,
            display_config: DisplayConfig::default(),
            max_parallel_listings: 32,
            max_parallel_listing_max_depth: 2,
            object_listing_queue_size: 200_000,
            allow_parallel_listings_in_express_one_zone: false,
            target_client_config: None,
            max_keys: 1000,
            auto_complete_shell: None,
            tracing_config: None,
        }
    }
}

/// Filter configuration for object selection.
#[derive(Debug, Clone, Default)]
pub struct FilterConfig {
    pub include_regex: Option<Regex>,
    pub exclude_regex: Option<Regex>,
    pub mtime_before: Option<DateTime<Utc>>,
    pub mtime_after: Option<DateTime<Utc>>,
    pub smaller_size: Option<u64>,
    pub larger_size: Option<u64>,
    pub storage_class: Option<Vec<String>>,
}

/// Display and output format configuration.
#[derive(Debug, Clone, Default)]
pub struct DisplayConfig {
    pub summary: bool,
    pub human: bool,
    pub show_fullpath: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub show_is_latest: bool,
    pub json: bool,
}

/// AWS S3 client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub client_config_location: ClientConfigLocation,
    pub credential: S3Credentials,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub accelerate: bool,
    pub request_payer: Option<aws_sdk_s3::types::RequestPayer>,
    pub request_checksum_calculation: aws_smithy_types::checksum_config::RequestChecksumCalculation,
    pub retry_config: RetryConfig,
    pub cli_timeout_config: CLITimeoutConfig,
    pub disable_stalled_stream_protection: bool,
}

/// Retry configuration for AWS SDK operations.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub aws_max_attempts: u32,
    pub initial_backoff_milliseconds: u64,
}

/// Timeout configuration for AWS SDK operations.
#[derive(Debug, Clone)]
pub struct CLITimeoutConfig {
    pub operation_timeout_milliseconds: Option<u64>,
    pub operation_attempt_timeout_milliseconds: Option<u64>,
    pub connect_timeout_milliseconds: Option<u64>,
    pub read_timeout_milliseconds: Option<u64>,
}

/// Tracing (logging) configuration.
#[derive(Debug, Clone, Copy)]
pub struct TracingConfig {
    pub tracing_level: log::Level,
    pub json_tracing: bool,
    pub aws_sdk_tracing: bool,
    pub span_events_tracing: bool,
    pub disable_color_tracing: bool,
}
