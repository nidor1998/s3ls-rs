use chrono::{DateTime, Utc};
use clap::builder::NonEmptyStringValueParser;
use clap::{Parser, ValueEnum};
use clap_verbosity_flag::{Verbosity, WarnLevel};
use std::ffi::OsString;
use std::path::PathBuf;

mod value_parser;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Default constants
// ---------------------------------------------------------------------------

const DEFAULT_MAX_PARALLEL_LISTINGS: u16 = 16;
const DEFAULT_PARALLEL_LISTING_MAX_DEPTH: u16 = 2;
const DEFAULT_OBJECT_LISTING_QUEUE_SIZE: u32 = 200000;
const DEFAULT_AWS_MAX_ATTEMPTS: u32 = 10;
const DEFAULT_INITIAL_BACKOFF_MILLISECONDS: u64 = 100;
const DEFAULT_MAX_KEYS: i32 = 1000;

const ERROR_MESSAGE_INVALID_TARGET: &str = "target must be an S3 path (e.g. s3://bucket/prefix)";

fn check_s3_target(s: &str) -> Result<String, String> {
    if s.starts_with("s3://") && s.len() > 5 {
        Ok(s.to_string())
    } else {
        Err(ERROR_MESSAGE_INVALID_TARGET.to_string())
    }
}

fn parse_human_bytes(s: &str) -> Result<u64, String> {
    value_parser::human_bytes::parse_human_bytes(s)
}

/// Field to sort listing results by.
#[derive(Clone, Debug, PartialEq, ValueEnum)]
pub enum SortField {
    Key,
    Size,
    Date,
}

impl std::fmt::Display for SortField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortField::Key => write!(f, "key"),
            SortField::Size => write!(f, "size"),
            SortField::Date => write!(f, "date"),
        }
    }
}

/// s3ls - Ultra-fast S3 object listing tool.
///
/// List objects in S3 buckets with filtering, sorting, and multiple output formats.
///
/// Example:
///   s3ls s3://my-bucket/logs/
///   s3ls s3://my-bucket/ --recursive --human --summary
///   s3ls s3://my-bucket/data/ --sort size --reverse --json
#[derive(Parser, Clone, Debug)]
#[command(name = "s3ls", about, long_about = None, version)]
pub struct CLIArgs {
    /// S3 target path: s3://<BUCKET_NAME>[/prefix]
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>[/prefix]",
        value_parser = check_s3_target,
        default_value_if("auto_complete_shell", clap::builder::ArgPredicate::IsPresent, "s3://ignored"),
        required = false,
    )]
    pub target: String,

    // -----------------------------------------------------------------------
    // General options
    // -----------------------------------------------------------------------
    /// List all objects recursively (enables parallel listing)
    #[arg(short = 'r', long, env, default_value_t = false, help_heading = "General")]
    pub recursive: bool,

    /// List all versions including delete markers
    #[arg(long, env = "LIST_ALL_VERSIONS", default_value_t = false, help_heading = "General")]
    pub all_versions: bool,

    // -----------------------------------------------------------------------
    // Filtering options
    // -----------------------------------------------------------------------
    /// List only objects whose key matches this regex
    #[arg(long, env, value_parser = value_parser::regex::parse_regex, help_heading = "Filtering")]
    pub filter_include_regex: Option<String>,

    /// Skip objects whose key matches this regex
    #[arg(long, env, value_parser = value_parser::regex::parse_regex, help_heading = "Filtering")]
    pub filter_exclude_regex: Option<String>,

    /// List only objects modified before this time
    #[arg(
        long,
        env,
        help_heading = "Filtering",
        long_help = "List only objects modified before the given time (RFC 3339 format).\nExample: 2023-02-19T12:00:00Z"
    )]
    pub filter_mtime_before: Option<DateTime<Utc>>,

    /// List only objects modified at or after this time
    #[arg(
        long,
        env,
        help_heading = "Filtering",
        long_help = "List only objects modified at or after the given time (RFC 3339 format).\nExample: 2023-02-19T12:00:00Z"
    )]
    pub filter_mtime_after: Option<DateTime<Utc>>,

    /// List only objects smaller than this size
    #[arg(
        long,
        env,
        value_parser = value_parser::human_bytes::check_human_bytes,
        help_heading = "Filtering",
        long_help = "List only objects smaller than the given size.\nSupported suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB"
    )]
    pub filter_smaller_size: Option<String>,

    /// List only objects larger than or equal to this size
    #[arg(
        long,
        env,
        value_parser = value_parser::human_bytes::check_human_bytes,
        help_heading = "Filtering",
        long_help = "List only objects larger than or equal to the given size.\nSupported suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB"
    )]
    pub filter_larger_size: Option<String>,

    /// Comma-separated list of storage classes to include
    #[arg(
        long,
        env,
        value_delimiter = ',',
        help_heading = "Filtering",
        long_help = "List only objects whose storage class is in the given list.\nMultiple classes can be separated by commas.\n\nExample: --storage-class STANDARD,GLACIER,DEEP_ARCHIVE"
    )]
    pub storage_class: Option<Vec<String>>,

    // -----------------------------------------------------------------------
    // Sort options
    // -----------------------------------------------------------------------
    /// Sort results by field: key, size, or date
    #[arg(long, value_enum, default_value_t = SortField::Key, help_heading = "Sort")]
    pub sort: SortField,

    /// Reverse the sort order
    #[arg(long, default_value_t = false, help_heading = "Sort")]
    pub reverse: bool,

    // -----------------------------------------------------------------------
    // Display options
    // -----------------------------------------------------------------------
    /// Append summary line (total count, total size)
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub summary: bool,

    /// Human-readable sizes (e.g. 1.2KiB)
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub human: bool,

    /// Show full key instead of relative to prefix
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_fullpath: bool,

    /// Show ETag column
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_etag: bool,

    /// Show storage class column
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_storage_class: bool,

    /// Show checksum algorithm column
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_checksum_algorithm: bool,

    /// Show checksum type column
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_checksum_type: bool,

    /// Output as NDJSON (one JSON object per line)
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub json: bool,

    // -----------------------------------------------------------------------
    // Tracing/Logging options (reused from s3rm-rs)
    // -----------------------------------------------------------------------
    /// Verbosity level (-q quiet, default normal, -v, -vv, -vvv)
    #[command(flatten)]
    pub verbosity: Verbosity<WarnLevel>,

    /// Output structured logs in JSON format
    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub json_tracing: bool,

    /// Include AWS SDK internal traces in log output
    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub aws_sdk_tracing: bool,

    /// Include span open/close events in log output
    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub span_events_tracing: bool,

    /// Disable colored output in logs
    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub disable_color_tracing: bool,

    // -----------------------------------------------------------------------
    // AWS configuration (reused from s3rm-rs)
    // -----------------------------------------------------------------------
    /// Path to the AWS config file
    #[arg(long, env, help_heading = "AWS Configuration")]
    pub aws_config_file: Option<PathBuf>,

    /// Path to the AWS shared credentials file
    #[arg(long, env, help_heading = "AWS Configuration")]
    pub aws_shared_credentials_file: Option<PathBuf>,

    /// Target AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["target_access_key", "target_secret_access_key", "target_session_token"], value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_profile: Option<String>,

    /// Target access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_secret_access_key", value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_access_key: Option<String>,

    /// Target secret access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_secret_access_key: Option<String>,

    /// Target session token
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_session_token: Option<String>,

    /// AWS region for the target
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_region: Option<String>,

    /// Custom S3-compatible endpoint URL (e.g. MinIO, Wasabi)
    #[arg(long, env, value_parser = value_parser::url::check_scheme, help_heading = "AWS Configuration")]
    pub target_endpoint_url: Option<String>,

    /// Use path-style access (required by some S3-compatible services)
    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub target_force_path_style: bool,

    /// Enable S3 Transfer Acceleration
    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub target_accelerate: bool,

    /// Enable requester-pays for the target bucket
    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub target_request_payer: bool,

    /// Disable stalled stream protection
    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub disable_stalled_stream_protection: bool,

    // -----------------------------------------------------------------------
    // Performance options (reused from s3rm-rs)
    // -----------------------------------------------------------------------
    /// Number of concurrent listing operations (1-65535)
    #[arg(long, env, default_value_t = DEFAULT_MAX_PARALLEL_LISTINGS, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    pub max_parallel_listings: u16,

    /// Maximum depth for parallel listing operations
    #[arg(long, env, default_value_t = DEFAULT_PARALLEL_LISTING_MAX_DEPTH, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    pub max_parallel_listing_max_depth: u16,

    /// Internal queue size for object listing
    #[arg(long, env, default_value_t = DEFAULT_OBJECT_LISTING_QUEUE_SIZE, value_parser = clap::value_parser!(u32).range(1..), help_heading = "Performance")]
    pub object_listing_queue_size: u32,

    /// Allow parallel listings in Express One Zone storage
    #[arg(long, env, default_value_t = false, help_heading = "Performance")]
    pub allow_parallel_listings_in_express_one_zone: bool,

    // -----------------------------------------------------------------------
    // Retry options (reused from s3rm-rs)
    // -----------------------------------------------------------------------
    /// Maximum retry attempts for AWS SDK operations
    #[arg(long, env, default_value_t = DEFAULT_AWS_MAX_ATTEMPTS, help_heading = "Retry Options")]
    pub aws_max_attempts: u32,

    /// Initial backoff in milliseconds for retries
    #[arg(long, env, default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS, help_heading = "Retry Options")]
    pub initial_backoff_milliseconds: u64,

    // -----------------------------------------------------------------------
    // Timeout options (reused from s3rm-rs)
    // -----------------------------------------------------------------------
    /// Overall operation timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub operation_timeout_milliseconds: Option<u64>,

    /// Per-attempt operation timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub operation_attempt_timeout_milliseconds: Option<u64>,

    /// Connection timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub connect_timeout_milliseconds: Option<u64>,

    /// Read timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub read_timeout_milliseconds: Option<u64>,

    // -----------------------------------------------------------------------
    // Advanced options
    // -----------------------------------------------------------------------
    /// Maximum number of objects returned in a single list object request (1-1000)
    #[arg(long, env, default_value_t = DEFAULT_MAX_KEYS, value_parser = clap::value_parser!(i32).range(1..=1000), help_heading = "Advanced")]
    pub max_keys: i32,

    /// Generate shell completions for the given shell
    #[arg(long, env, help_heading = "Advanced")]
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,
}

/// Parse command-line arguments into a `CLIArgs` struct.
pub fn parse_from_args<I, T>(args: I) -> Result<CLIArgs, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    CLIArgs::try_parse_from(args)
}

/// Parse arguments and build a Config in one step.
pub fn build_config_from_args<I, T>(args: I) -> Result<crate::config::Config, String>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli_args = CLIArgs::try_parse_from(args).map_err(|e| e.to_string())?;
    crate::config::Config::try_from(cli_args)
}

impl CLIArgs {
    fn parse_target(&self) -> Result<crate::types::S3Target, String> {
        crate::types::S3Target::parse(&self.target).map_err(|e| e.to_string())
    }

    fn build_tracing_config(&self) -> Option<crate::config::TracingConfig> {
        self.verbosity
            .log_level()
            .map(|log_level| crate::config::TracingConfig {
                tracing_level: log_level,
                json_tracing: self.json_tracing,
                aws_sdk_tracing: self.aws_sdk_tracing,
                span_events_tracing: self.span_events_tracing,
                disable_color_tracing: self.disable_color_tracing,
            })
    }
}

impl TryFrom<CLIArgs> for crate::config::Config {
    type Error = String;

    fn try_from(args: CLIArgs) -> Result<Self, Self::Error> {
        let target = args.parse_target()?;
        let tracing_config = args.build_tracing_config();

        let filter_larger_size = args
            .filter_larger_size
            .as_deref()
            .map(parse_human_bytes)
            .transpose()
            .map_err(|e| format!("Invalid filter-larger-size: {e}"))?;
        let filter_smaller_size = args
            .filter_smaller_size
            .as_deref()
            .map(parse_human_bytes)
            .transpose()
            .map_err(|e| format!("Invalid filter-smaller-size: {e}"))?;

        Ok(crate::config::Config {
            target,
            recursive: args.recursive,
            all_versions: args.all_versions,
            filter_include_regex: args.filter_include_regex,
            filter_exclude_regex: args.filter_exclude_regex,
            filter_mtime_before: args.filter_mtime_before,
            filter_mtime_after: args.filter_mtime_after,
            filter_smaller_size,
            filter_larger_size,
            storage_class: args.storage_class,
            sort: args.sort,
            reverse: args.reverse,
            summary: args.summary,
            human: args.human,
            show_fullpath: args.show_fullpath,
            show_etag: args.show_etag,
            show_storage_class: args.show_storage_class,
            show_checksum_algorithm: args.show_checksum_algorithm,
            show_checksum_type: args.show_checksum_type,
            json: args.json,
            max_parallel_listings: args.max_parallel_listings,
            max_parallel_listing_max_depth: args.max_parallel_listing_max_depth,
            object_listing_queue_size: args.object_listing_queue_size,
            allow_parallel_listings_in_express_one_zone: args
                .allow_parallel_listings_in_express_one_zone,
            aws_config_file: args.aws_config_file,
            aws_shared_credentials_file: args.aws_shared_credentials_file,
            target_profile: args.target_profile,
            target_access_key: args.target_access_key,
            target_secret_access_key: args.target_secret_access_key,
            target_session_token: args.target_session_token,
            target_region: args.target_region,
            target_endpoint_url: args.target_endpoint_url,
            target_force_path_style: args.target_force_path_style,
            target_accelerate: args.target_accelerate,
            target_request_payer: args.target_request_payer,
            disable_stalled_stream_protection: args.disable_stalled_stream_protection,
            aws_max_attempts: args.aws_max_attempts,
            initial_backoff_milliseconds: args.initial_backoff_milliseconds,
            operation_timeout_milliseconds: args.operation_timeout_milliseconds,
            operation_attempt_timeout_milliseconds: args.operation_attempt_timeout_milliseconds,
            connect_timeout_milliseconds: args.connect_timeout_milliseconds,
            read_timeout_milliseconds: args.read_timeout_milliseconds,
            max_keys: args.max_keys,
            tracing_config,
        })
    }
}
