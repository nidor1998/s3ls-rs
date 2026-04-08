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

const DEFAULT_MAX_PARALLEL_LISTINGS: u16 = 32;
const DEFAULT_PARALLEL_LISTING_MAX_DEPTH: u16 = 2;
const DEFAULT_OBJECT_LISTING_QUEUE_SIZE: u32 = 200000;
const DEFAULT_AWS_MAX_ATTEMPTS: u32 = 10;
const DEFAULT_INITIAL_BACKOFF_MILLISECONDS: u64 = 100;
const DEFAULT_MAX_KEYS: i32 = 1000;

const ERROR_MESSAGE_INVALID_TARGET: &str = "target must be an S3 path (e.g. s3://bucket or s3://bucket/prefix)";

fn check_s3_target(s: &str) -> Result<String, String> {
    if s.is_empty() || (s.starts_with("s3://") && s.len() > 5) {
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
    Bucket,
    Region,
}

impl std::fmt::Display for SortField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortField::Key => write!(f, "key"),
            SortField::Size => write!(f, "size"),
            SortField::Date => write!(f, "date"),
            SortField::Bucket => write!(f, "bucket"),
            SortField::Region => write!(f, "region"),
        }
    }
}

/// s3ls - Fast S3 object listing tool.
///
/// List objects in S3 buckets with filtering, sorting, and multiple output formats.
///
/// Example:
///   s3ls s3://my-bucket/logs/
///   s3ls s3://my-bucket/ --recursive --human-readable --summarize
///   s3ls s3://my-bucket/data/ --sort size --reverse --json
#[derive(Parser, Clone, Debug)]
#[command(name = "s3ls", about, long_about = None, version)]
pub struct CLIArgs {
    /// S3 target path: s3://<BUCKET_NAME>[/prefix] (omit to list buckets)
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>[/prefix]",
        value_parser = check_s3_target,
        default_value_if("auto_complete_shell", clap::builder::ArgPredicate::IsPresent, "s3://ignored"),
        required = false,
        default_value = "",
    )]
    pub target: String,

    // -----------------------------------------------------------------------
    // General options
    // -----------------------------------------------------------------------
    /// List all objects recursively (enables parallel listing)
    #[arg(
        short = 'r',
        long,
        env,
        default_value_t = false,
        help_heading = "General"
    )]
    pub recursive: bool,

    /// List all versions including delete markers
    #[arg(
        long,
        env = "LIST_ALL_VERSIONS",
        default_value_t = false,
        help_heading = "General"
    )]
    pub all_versions: bool,

    /// Hide delete markers from version listing (requires --all-versions)
    #[arg(
        long,
        default_value_t = false,
        requires = "all_versions",
        help_heading = "General"
    )]
    pub hide_delete_marker: bool,

    /// Maximum depth for recursive listing (requires --recursive)
    #[arg(long, requires = "recursive", env = "MAX_DEPTH", value_parser = clap::value_parser!(u16).range(1..), help_heading = "General")]
    pub max_depth: Option<u16>,

    /// Filter buckets by name prefix (when listing buckets)
    #[arg(long, help_heading = "General")]
    pub bucket_name_prefix: Option<String>,

    /// List only Express One Zone directory buckets (when listing buckets)
    #[arg(long, default_value_t = false, help_heading = "General")]
    pub list_express_one_zone_buckets: bool,

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
        value_parser = value_parser::storage_class::parse_storage_class,
        help_heading = "Filtering",
        long_help = "List only objects whose storage class is in the given list.\nMultiple classes can be separated by commas.\n\nExample: --storage-class STANDARD,GLACIER,DEEP_ARCHIVE"
    )]
    pub storage_class: Option<Vec<String>>,

    // -----------------------------------------------------------------------
    // Sort options
    // -----------------------------------------------------------------------
    /// Sort results by field(s): key, size, date (comma-separated, max 2)
    #[arg(
        long,
        default_value = "key",
        value_delimiter = ',',
        ignore_case = true,
        help_heading = "Sort"
    )]
    pub sort: Vec<SortField>,

    /// Reverse the sort order
    #[arg(long, default_value_t = false, help_heading = "Sort")]
    pub reverse: bool,

    /// Disable sorting and stream results directly (reduces memory usage)
    #[arg(long, default_value_t = false, conflicts_with_all = ["sort", "reverse"], help_heading = "Sort")]
    pub no_sort: bool,

    // -----------------------------------------------------------------------
    // Display options
    // -----------------------------------------------------------------------
    /// Append summary line (total count, total size)
    #[arg(long = "summarize", default_value_t = false, help_heading = "Display")]
    pub summary: bool,

    /// Human-readable sizes (e.g. 1.2KiB)
    #[arg(
        long = "human-readable",
        default_value_t = false,
        help_heading = "Display"
    )]
    pub human: bool,

    /// Show key relative to prefix instead of full path
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_relative_path: bool,

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

    /// Show is_latest column (requires --all-versions)
    #[arg(
        long,
        default_value_t = false,
        requires = "all_versions",
        help_heading = "Display"
    )]
    pub show_is_latest: bool,

    /// Show owner DisplayName and ID columns
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_owner: bool,

    /// Show restore status column
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_restore_status: bool,

    /// Show bucket ARN column (when listing buckets)
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_bucket_arn: bool,

    /// Add a header row to each column
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub header: bool,

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
    let cli_args = parse_from_args(args).map_err(|e| e.to_string())?;
    crate::config::Config::try_from(cli_args)
}

impl CLIArgs {
    fn parse_target(&self) -> Result<crate::types::S3Target, String> {
        if self.target.is_empty() {
            // No target — bucket listing mode
            Ok(crate::types::S3Target {
                bucket: String::new(),
                prefix: None,
            })
        } else {
            crate::types::S3Target::parse(&self.target).map_err(|e| e.to_string())
        }
    }

    fn build_filter_config(&self) -> Result<crate::config::FilterConfig, String> {
        let compile_regex = |pattern: &Option<String>| -> Option<fancy_regex::Regex> {
            pattern.as_ref().map(|p| {
                fancy_regex::Regex::new(p).expect("regex was already validated by value_parser")
            })
        };

        let larger_size = self
            .filter_larger_size
            .as_deref()
            .map(parse_human_bytes)
            .transpose()
            .map_err(|e| format!("Invalid filter-larger-size: {e}"))?;
        let smaller_size = self
            .filter_smaller_size
            .as_deref()
            .map(parse_human_bytes)
            .transpose()
            .map_err(|e| format!("Invalid filter-smaller-size: {e}"))?;

        Ok(crate::config::FilterConfig {
            include_regex: compile_regex(&self.filter_include_regex),
            exclude_regex: compile_regex(&self.filter_exclude_regex),
            mtime_before: self.filter_mtime_before,
            mtime_after: self.filter_mtime_after,
            smaller_size,
            larger_size,
            storage_class: self.storage_class.clone(),
        })
    }

    fn build_client_config(&self) -> Option<crate::config::ClientConfig> {
        let credential = if let Some(ref profile) = self.target_profile {
            crate::types::S3Credentials::Profile(profile.clone())
        } else if let Some(ref access_key) = self.target_access_key {
            crate::types::S3Credentials::Credentials {
                access_keys: crate::types::AccessKeys {
                    access_key: access_key.clone(),
                    secret_access_key: self.target_secret_access_key.clone().unwrap_or_default(),
                    session_token: self.target_session_token.clone(),
                },
            }
        } else {
            crate::types::S3Credentials::FromEnvironment
        };

        Some(crate::config::ClientConfig {
            client_config_location: crate::types::ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential,
            region: self.target_region.clone(),
            endpoint_url: self.target_endpoint_url.clone(),
            force_path_style: self.target_force_path_style,
            accelerate: self.target_accelerate,
            request_payer: if self.target_request_payer {
                Some(aws_sdk_s3::types::RequestPayer::Requester)
            } else {
                None
            },
            request_checksum_calculation:
                aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: self.aws_max_attempts,
                initial_backoff_milliseconds: self.initial_backoff_milliseconds,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: self.operation_timeout_milliseconds,
                operation_attempt_timeout_milliseconds: self.operation_attempt_timeout_milliseconds,
                connect_timeout_milliseconds: self.connect_timeout_milliseconds,
                read_timeout_milliseconds: self.read_timeout_milliseconds,
            },
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
        })
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
        if args.sort.len() > 2 {
            return Err("at most 2 sort fields allowed".to_string());
        }
        for i in 0..args.sort.len() {
            for j in (i + 1)..args.sort.len() {
                if args.sort[i] == args.sort[j] {
                    return Err(format!("duplicate sort field '{}'", args.sort[i]));
                }
            }
        }

        let target = args.parse_target()?;
        let filter_config = args.build_filter_config()?;
        let target_client_config = args.build_client_config();
        let tracing_config = args.build_tracing_config();

        // In bucket listing mode, replace the default sort field (Key) with Bucket.
        let mut sort = args.sort;
        if target.bucket.is_empty() && sort == vec![crate::config::args::SortField::Key] {
            sort = vec![crate::config::args::SortField::Bucket];
        }

        // When --all-versions is set and the user specified only one sort field,
        // append Date as a secondary sort so versions of the same key appear in
        // chronological order.
        if args.all_versions
            && sort.len() == 1
            && !sort.contains(&crate::config::args::SortField::Date)
        {
            sort.push(crate::config::args::SortField::Date);
        }

        Ok(crate::config::Config {
            target,
            recursive: args.recursive,
            all_versions: args.all_versions,
            hide_delete_marker: args.hide_delete_marker,
            max_depth: args.max_depth,
            bucket_name_prefix: args.bucket_name_prefix,
            list_express_one_zone_buckets: args.list_express_one_zone_buckets,
            filter_config,
            sort,
            reverse: args.reverse,
            no_sort: args.no_sort,
            display_config: crate::config::DisplayConfig {
                summary: args.summary,
                human: args.human,
                show_relative_path: args.show_relative_path,
                show_etag: args.show_etag,
                show_storage_class: args.show_storage_class,
                show_checksum_algorithm: args.show_checksum_algorithm,
                show_checksum_type: args.show_checksum_type,
                show_is_latest: args.show_is_latest,
                show_owner: args.show_owner,
                show_restore_status: args.show_restore_status,
                show_bucket_arn: args.show_bucket_arn,
                header: args.header,
                json: args.json,
            },
            max_parallel_listings: args.max_parallel_listings,
            max_parallel_listing_max_depth: args.max_parallel_listing_max_depth,
            object_listing_queue_size: args.object_listing_queue_size,
            allow_parallel_listings_in_express_one_zone: args
                .allow_parallel_listings_in_express_one_zone,
            target_client_config,
            max_keys: args.max_keys,
            auto_complete_shell: args.auto_complete_shell,
            tracing_config,
        })
    }
}
