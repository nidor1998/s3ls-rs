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
    pub hide_delete_markers: bool,
    pub show_objects_only: bool,
    pub max_depth: Option<u16>,
    pub bucket_name_prefix: Option<String>,
    pub list_express_one_zone_buckets: bool,

    // Filtering
    pub filter_config: FilterConfig,

    // Sort
    pub sort: Vec<SortField>,
    pub reverse: bool,
    pub no_sort: bool,

    // Display
    pub display_config: DisplayConfig,

    // Performance
    pub max_parallel_listings: u16,
    pub max_parallel_listing_max_depth: u16,
    pub object_listing_queue_size: u32,
    pub allow_parallel_listings_in_express_one_zone: bool,
    pub rate_limit_api: Option<u32>,
    pub parallel_sort_threshold: u32,

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
            hide_delete_markers: false,
            show_objects_only: false,
            max_depth: None,
            bucket_name_prefix: None,
            list_express_one_zone_buckets: false,
            filter_config: FilterConfig::default(),
            sort: vec![SortField::Key],
            reverse: false,
            no_sort: false,
            display_config: DisplayConfig::default(),
            max_parallel_listings: 64,
            max_parallel_listing_max_depth: 2,
            object_listing_queue_size: 200_000,
            allow_parallel_listings_in_express_one_zone: false,
            rate_limit_api: None,
            parallel_sort_threshold: 1_000_000,
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
    pub show_relative_path: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub show_is_latest: bool,
    pub show_owner: bool,
    pub show_restore_status: bool,
    pub show_bucket_arn: bool,
    pub show_local_time: bool,
    pub header: bool,
    pub json: bool,
    /// Emit raw S3 key/prefix bytes without escaping control characters.
    /// Defaults to false — text-mode output replaces control chars with
    /// `\xNN` escapes to prevent injection of fake rows or terminal
    /// escape sequences by maliciously-named objects.
    pub raw_output: bool,
    /// Emit tab-separated text instead of the default whitespace-aligned
    /// columns (see `Args::tsv`).
    pub tsv: bool,
    /// Display only the key (or bucket name), one per line, with no
    /// other columns (see `Args::one_line`).
    pub one_line: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_values() {
        let config = Config::default();

        // Target
        assert!(config.target.bucket.is_empty());
        assert!(config.target.prefix.is_none());

        // Listing mode
        assert!(!config.recursive);
        assert!(!config.all_versions);
        assert!(!config.hide_delete_markers);
        assert!(!config.show_objects_only);
        assert!(config.max_depth.is_none());
        assert!(config.bucket_name_prefix.is_none());
        assert!(!config.list_express_one_zone_buckets);

        // Sort
        assert_eq!(config.sort, vec![SortField::Key]);
        assert!(!config.reverse);
        assert!(!config.no_sort);

        // Display
        assert!(!config.display_config.summary);
        assert!(!config.display_config.human);
        assert!(!config.display_config.show_relative_path);
        assert!(!config.display_config.show_etag);
        assert!(!config.display_config.show_storage_class);
        assert!(!config.display_config.show_checksum_algorithm);
        assert!(!config.display_config.show_checksum_type);
        assert!(!config.display_config.show_is_latest);
        assert!(!config.display_config.show_owner);
        assert!(!config.display_config.show_restore_status);
        assert!(!config.display_config.show_bucket_arn);
        assert!(!config.display_config.show_local_time);
        assert!(!config.display_config.header);
        assert!(!config.display_config.json);
        assert!(!config.display_config.raw_output);
        assert!(!config.display_config.tsv);
        assert!(!config.display_config.one_line);

        // Performance
        assert_eq!(config.max_parallel_listings, 64);
        assert_eq!(config.max_parallel_listing_max_depth, 2);
        assert_eq!(config.object_listing_queue_size, 200_000);
        assert!(!config.allow_parallel_listings_in_express_one_zone);
        assert!(config.rate_limit_api.is_none());
        assert_eq!(config.parallel_sort_threshold, 1_000_000);

        // AWS Client
        assert!(config.target_client_config.is_none());

        // Advanced
        assert_eq!(config.max_keys, 1000);
        assert!(config.auto_complete_shell.is_none());

        // Tracing
        assert!(config.tracing_config.is_none());

        // Filter
        assert!(config.filter_config.include_regex.is_none());
        assert!(config.filter_config.exclude_regex.is_none());
        assert!(config.filter_config.mtime_before.is_none());
        assert!(config.filter_config.mtime_after.is_none());
        assert!(config.filter_config.smaller_size.is_none());
        assert!(config.filter_config.larger_size.is_none());
        assert!(config.filter_config.storage_class.is_none());
    }

    #[test]
    fn config_for_target_sets_bucket_and_prefix() {
        let config = Config::for_target("my-bucket", "data/");
        assert_eq!(config.target.bucket, "my-bucket");
        assert_eq!(config.target.prefix.as_deref(), Some("data/"));

        // Empty prefix → None
        let config = Config::for_target("my-bucket", "");
        assert_eq!(config.target.bucket, "my-bucket");
        assert!(config.target.prefix.is_none());
    }
}
