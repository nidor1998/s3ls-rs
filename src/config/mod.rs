pub mod args;

use crate::config::args::SortField;
use crate::types::S3Target;
use chrono::{DateTime, Utc};
use std::path::PathBuf;

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
    pub sort: SortField,
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
            sort: SortField::Key,
            reverse: false,
            display_config: DisplayConfig::default(),
            max_parallel_listings: 16,
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
    pub include_regex: Option<String>,
    pub exclude_regex: Option<String>,
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
    pub json: bool,
}

/// AWS S3 client configuration.
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

/// S3 credential types.
#[derive(Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
}

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Credentials::Profile(p) => f.debug_tuple("Profile").field(p).finish(),
            S3Credentials::Credentials { access_keys } => f
                .debug_struct("Credentials")
                .field("access_keys", access_keys)
                .finish(),
            S3Credentials::FromEnvironment => write!(f, "FromEnvironment"),
        }
    }
}

/// AWS access key pair with secure zeroization.
///
/// The secret_access_key and session_token are securely cleared from memory
/// when this struct is dropped, using the zeroize crate.
#[derive(Clone, zeroize_derive::Zeroize, zeroize_derive::ZeroizeOnDrop)]
pub struct AccessKeys {
    pub access_key: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for AccessKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let session_token = self
            .session_token
            .as_ref()
            .map_or("None", |_| "** redacted **");
        f.debug_struct("AccessKeys")
            .field("access_key", &self.access_key)
            .field("secret_access_key", &"** redacted **")
            .field("session_token", &session_token)
            .finish()
    }
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
