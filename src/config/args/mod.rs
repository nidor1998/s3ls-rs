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
#[derive(Clone, Debug, ValueEnum)]
pub enum SortField {
    Key,
    Size,
    Date,
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

    // -- General --
    #[arg(short = 'r', long, env, default_value_t = false, help_heading = "General")]
    pub recursive: bool,

    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub all_versions: bool,

    // -- Filtering --
    #[arg(long, env, value_parser = value_parser::regex::parse_regex, help_heading = "Filtering")]
    pub filter_include_regex: Option<String>,

    #[arg(long, env, value_parser = value_parser::regex::parse_regex, help_heading = "Filtering")]
    pub filter_exclude_regex: Option<String>,

    #[arg(long, env, help_heading = "Filtering",
        long_help = "List only objects modified before the given time (RFC 3339 format).\nExample: 2023-02-19T12:00:00Z")]
    pub filter_mtime_before: Option<DateTime<Utc>>,

    #[arg(long, env, help_heading = "Filtering",
        long_help = "List only objects modified at or after the given time (RFC 3339 format).\nExample: 2023-02-19T12:00:00Z")]
    pub filter_mtime_after: Option<DateTime<Utc>>,

    #[arg(long, env, value_parser = value_parser::human_bytes::check_human_bytes, help_heading = "Filtering",
        long_help = "List only objects smaller than the given size.\nSupported suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB")]
    pub filter_smaller_size: Option<String>,

    #[arg(long, env, value_parser = value_parser::human_bytes::check_human_bytes, help_heading = "Filtering",
        long_help = "List only objects larger than or equal to the given size.\nSupported suffixes: KB, KiB, MB, MiB, GB, GiB, TB, TiB")]
    pub filter_larger_size: Option<String>,

    /// Comma-separated list of storage classes to include
    #[arg(long, env, value_delimiter = ',', help_heading = "Filtering")]
    pub storage_class: Option<Vec<String>>,

    // -- Sort --
    #[arg(long, value_enum, help_heading = "Sort")]
    pub sort: Option<SortField>,

    #[arg(long, default_value_t = false, help_heading = "Sort")]
    pub reverse: bool,

    // -- Display --
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub summary: bool,

    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub human: bool,

    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_fullpath: bool,

    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_etag: bool,

    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_storage_class: bool,

    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_checksum_algorithm: bool,

    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub show_checksum_type: bool,

    /// Output as NDJSON (one JSON object per line)
    #[arg(long, default_value_t = false, help_heading = "Display")]
    pub json: bool,

    // -- Tracing/Logging --
    #[command(flatten)]
    pub verbosity: Verbosity<WarnLevel>,

    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub json_tracing: bool,

    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub aws_sdk_tracing: bool,

    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub span_events_tracing: bool,

    #[arg(long, env, default_value_t = false, help_heading = "Tracing/Logging")]
    pub disable_color_tracing: bool,

    // -- AWS Configuration --
    #[arg(long, env, help_heading = "AWS Configuration")]
    pub aws_config_file: Option<PathBuf>,

    #[arg(long, env, help_heading = "AWS Configuration")]
    pub aws_shared_credentials_file: Option<PathBuf>,

    #[arg(long, env, conflicts_with_all = ["target_access_key", "target_secret_access_key", "target_session_token"], value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_profile: Option<String>,

    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_secret_access_key", value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_access_key: Option<String>,

    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_secret_access_key: Option<String>,

    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_session_token: Option<String>,

    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_region: Option<String>,

    #[arg(long, env, value_parser = value_parser::url::check_scheme, help_heading = "AWS Configuration")]
    pub target_endpoint_url: Option<String>,

    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub target_force_path_style: bool,

    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub target_accelerate: bool,

    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub target_request_payer: bool,

    #[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
    pub disable_stalled_stream_protection: bool,

    // -- Performance --
    #[arg(long, env, default_value_t = DEFAULT_MAX_PARALLEL_LISTINGS, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    pub max_parallel_listings: u16,

    #[arg(long, env, default_value_t = DEFAULT_PARALLEL_LISTING_MAX_DEPTH, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    pub max_parallel_listing_max_depth: u16,

    #[arg(long, env, default_value_t = DEFAULT_OBJECT_LISTING_QUEUE_SIZE, value_parser = clap::value_parser!(u32).range(1..), help_heading = "Performance")]
    pub object_listing_queue_size: u32,

    #[arg(long, env, default_value_t = false, help_heading = "Performance")]
    pub allow_parallel_listings_in_express_one_zone: bool,

    // -- Retry --
    #[arg(long, env, default_value_t = DEFAULT_AWS_MAX_ATTEMPTS, help_heading = "Retry Options")]
    pub aws_max_attempts: u32,

    #[arg(long, env, default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS, help_heading = "Retry Options")]
    pub initial_backoff_milliseconds: u64,

    // -- Timeout --
    #[arg(long, env, help_heading = "Timeout Options")]
    pub operation_timeout_milliseconds: Option<u64>,

    #[arg(long, env, help_heading = "Timeout Options")]
    pub operation_attempt_timeout_milliseconds: Option<u64>,

    #[arg(long, env, help_heading = "Timeout Options")]
    pub connect_timeout_milliseconds: Option<u64>,

    #[arg(long, env, help_heading = "Timeout Options")]
    pub read_timeout_milliseconds: Option<u64>,

    // -- Advanced --
    #[arg(long, env, default_value_t = DEFAULT_MAX_KEYS, value_parser = clap::value_parser!(i32).range(1..=32767), help_heading = "Advanced")]
    pub max_keys: i32,

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
