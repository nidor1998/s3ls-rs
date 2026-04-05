use super::*;
use proptest::prelude::*;

// ===========================================================================
// Helper
// ===========================================================================

fn args(extra: &[&str]) -> Vec<String> {
    let mut v = vec!["s3ls".to_string()];
    v.extend(extra.iter().map(|s| s.to_string()));
    v
}

// ===========================================================================
// 1. Basic / minimal parsing
// ===========================================================================

#[test]
fn parse_minimal_args() {
    let cli = parse_from_args(args(&["s3://bucket"])).unwrap();
    assert_eq!(cli.target, "s3://bucket");
}

// ===========================================================================
// 2. General options
// ===========================================================================

#[test]
fn recursive_long() {
    let cli = parse_from_args(args(&["s3://bucket", "--recursive"])).unwrap();
    assert!(cli.recursive);
}

#[test]
fn recursive_short() {
    let cli = parse_from_args(args(&["s3://bucket", "-r"])).unwrap();
    assert!(cli.recursive);
}

#[test]
fn all_versions() {
    let cli = parse_from_args(args(&["s3://bucket", "--all-versions"])).unwrap();
    assert!(cli.all_versions);
}

// ===========================================================================
// 3. Filtering
// ===========================================================================

#[test]
fn filter_include_regex() {
    let cli = parse_from_args(args(&["s3://bucket", "--filter-include-regex", r"\.csv$"])).unwrap();
    assert_eq!(cli.filter_include_regex.as_deref(), Some(r"\.csv$"));
}

#[test]
fn filter_exclude_regex() {
    let cli = parse_from_args(args(&["s3://bucket", "--filter-exclude-regex", r"^temp/"])).unwrap();
    assert_eq!(cli.filter_exclude_regex.as_deref(), Some(r"^temp/"));
}

#[test]
fn filter_mtime_before() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--filter-mtime-before",
        "2024-01-15T00:00:00Z",
    ]))
    .unwrap();
    assert!(cli.filter_mtime_before.is_some());
}

#[test]
fn filter_mtime_after() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--filter-mtime-after",
        "2023-06-01T12:00:00Z",
    ]))
    .unwrap();
    assert!(cli.filter_mtime_after.is_some());
}

#[test]
fn filter_smaller_size() {
    let cli = parse_from_args(args(&["s3://bucket", "--filter-smaller-size", "10MiB"])).unwrap();
    assert_eq!(cli.filter_smaller_size.as_deref(), Some("10MiB"));
}

#[test]
fn filter_larger_size() {
    let cli = parse_from_args(args(&["s3://bucket", "--filter-larger-size", "1GiB"])).unwrap();
    assert_eq!(cli.filter_larger_size.as_deref(), Some("1GiB"));
}

#[test]
fn storage_class_single() {
    let cli = parse_from_args(args(&["s3://bucket", "--storage-class", "GLACIER"])).unwrap();
    assert_eq!(cli.storage_class, Some(vec!["GLACIER".to_string()]));
}

#[test]
fn storage_class_multiple_comma_separated() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--storage-class", "STANDARD,GLACIER"])).unwrap();
    assert_eq!(
        cli.storage_class,
        Some(vec!["STANDARD".to_string(), "GLACIER".to_string()])
    );
}

#[test]
fn all_filters_combined() {
    let cli = parse_from_args(args(&[
        "s3://bucket/prefix",
        "--filter-include-regex",
        r"\.log$",
        "--filter-exclude-regex",
        r"^archive/",
        "--filter-mtime-before",
        "2024-06-01T00:00:00Z",
        "--filter-mtime-after",
        "2024-01-01T00:00:00Z",
        "--filter-smaller-size",
        "100MiB",
        "--filter-larger-size",
        "1KiB",
        "--storage-class",
        "STANDARD,INTELLIGENT_TIERING",
    ]))
    .unwrap();
    assert!(cli.filter_include_regex.is_some());
    assert!(cli.filter_exclude_regex.is_some());
    assert!(cli.filter_mtime_before.is_some());
    assert!(cli.filter_mtime_after.is_some());
    assert_eq!(cli.filter_smaller_size.as_deref(), Some("100MiB"));
    assert_eq!(cli.filter_larger_size.as_deref(), Some("1KiB"));
    assert_eq!(
        cli.storage_class,
        Some(vec![
            "STANDARD".to_string(),
            "INTELLIGENT_TIERING".to_string()
        ])
    );
}

#[test]
fn invalid_regex_rejected() {
    let result = parse_from_args(args(&["s3://bucket", "--filter-include-regex", "(unclosed"]));
    assert!(result.is_err());
}

// ===========================================================================
// 4. Sort
// ===========================================================================

#[test]
fn sort_single_key() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "key"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Key]);
}

#[test]
fn sort_single_size() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "size"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Size]);
}

#[test]
fn sort_single_date() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "date"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Date]);
}

#[test]
fn sort_two_fields_date_key() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "date,key"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Date, SortField::Key]);
}

#[test]
fn sort_two_fields_size_date() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "size,date"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Size, SortField::Date]);
}

#[test]
fn sort_rejects_three_fields() {
    let result = parse_from_args(args(&["s3://bucket", "--sort", "key,size,date"]));
    assert!(result.is_err());
}

#[test]
fn sort_rejects_duplicate_fields() {
    let result = parse_from_args(args(&["s3://bucket", "--sort", "date,date"]));
    assert!(result.is_err());
}

#[test]
fn sort_invalid_value() {
    let result = parse_from_args(args(&["s3://bucket", "--sort", "name"]));
    assert!(result.is_err());
}

#[test]
fn sort_case_insensitive() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "Date,KEY"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Date, SortField::Key]);
}

#[test]
fn reverse_flag() {
    let cli = parse_from_args(args(&["s3://bucket", "--reverse"])).unwrap();
    assert!(cli.reverse);
}

#[test]
fn sort_and_reverse_combo() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "size,key", "--reverse"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Size, SortField::Key]);
    assert!(cli.reverse);
}

// ===========================================================================
// 5. Display
// ===========================================================================

#[test]
fn display_summary() {
    let cli = parse_from_args(args(&["s3://bucket", "--summary"])).unwrap();
    assert!(cli.summary);
}

#[test]
fn display_human() {
    let cli = parse_from_args(args(&["s3://bucket", "--human"])).unwrap();
    assert!(cli.human);
}

#[test]
fn display_show_fullpath() {
    let cli = parse_from_args(args(&["s3://bucket", "--show-fullpath"])).unwrap();
    assert!(cli.show_fullpath);
}

#[test]
fn display_show_etag() {
    let cli = parse_from_args(args(&["s3://bucket", "--show-etag"])).unwrap();
    assert!(cli.show_etag);
}

#[test]
fn display_show_storage_class() {
    let cli = parse_from_args(args(&["s3://bucket", "--show-storage-class"])).unwrap();
    assert!(cli.show_storage_class);
}

#[test]
fn display_show_checksum_algorithm() {
    let cli = parse_from_args(args(&["s3://bucket", "--show-checksum-algorithm"])).unwrap();
    assert!(cli.show_checksum_algorithm);
}

#[test]
fn display_show_checksum_type() {
    let cli = parse_from_args(args(&["s3://bucket", "--show-checksum-type"])).unwrap();
    assert!(cli.show_checksum_type);
}

#[test]
fn display_json() {
    let cli = parse_from_args(args(&["s3://bucket", "--json"])).unwrap();
    assert!(cli.json);
}

#[test]
fn all_display_options_combined() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--summary",
        "--human",
        "--show-fullpath",
        "--show-etag",
        "--show-storage-class",
        "--show-checksum-algorithm",
        "--show-checksum-type",
        "--json",
    ]))
    .unwrap();
    assert!(cli.summary);
    assert!(cli.human);
    assert!(cli.show_fullpath);
    assert!(cli.show_etag);
    assert!(cli.show_storage_class);
    assert!(cli.show_checksum_algorithm);
    assert!(cli.show_checksum_type);
    assert!(cli.json);
}

// ===========================================================================
// 6. Tracing
// ===========================================================================

#[test]
fn tracing_json_tracing() {
    let cli = parse_from_args(args(&["s3://bucket", "--json-tracing"])).unwrap();
    assert!(cli.json_tracing);
}

#[test]
fn tracing_aws_sdk_tracing() {
    let cli = parse_from_args(args(&["s3://bucket", "--aws-sdk-tracing"])).unwrap();
    assert!(cli.aws_sdk_tracing);
}

#[test]
fn tracing_span_events_tracing() {
    let cli = parse_from_args(args(&["s3://bucket", "--span-events-tracing"])).unwrap();
    assert!(cli.span_events_tracing);
}

#[test]
fn tracing_disable_color_tracing() {
    let cli = parse_from_args(args(&["s3://bucket", "--disable-color-tracing"])).unwrap();
    assert!(cli.disable_color_tracing);
}

// ===========================================================================
// 7. AWS Configuration
// ===========================================================================

#[test]
fn aws_profile_region_endpoint_path_style() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--target-profile",
        "prod",
        "--target-region",
        "us-west-2",
        "--target-endpoint-url",
        "https://custom.endpoint.example.com",
        "--target-force-path-style",
    ]))
    .unwrap();
    assert_eq!(cli.target_profile.as_deref(), Some("prod"));
    assert_eq!(cli.target_region.as_deref(), Some("us-west-2"));
    assert_eq!(
        cli.target_endpoint_url.as_deref(),
        Some("https://custom.endpoint.example.com")
    );
    assert!(cli.target_force_path_style);
}

#[test]
fn aws_access_keys_with_session_token() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--target-access-key",
        "AKIAIOSFODNN7EXAMPLE",
        "--target-secret-access-key",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "--target-session-token",
        "FwoGZXIvYXdzEBYaDExampleSessionToken",
    ]))
    .unwrap();
    assert_eq!(
        cli.target_access_key.as_deref(),
        Some("AKIAIOSFODNN7EXAMPLE")
    );
    assert_eq!(
        cli.target_secret_access_key.as_deref(),
        Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    );
    assert_eq!(
        cli.target_session_token.as_deref(),
        Some("FwoGZXIvYXdzEBYaDExampleSessionToken")
    );
}

#[test]
fn aws_profile_conflicts_with_access_key() {
    let result = parse_from_args(args(&[
        "s3://bucket",
        "--target-profile",
        "prod",
        "--target-access-key",
        "AKIAIOSFODNN7EXAMPLE",
        "--target-secret-access-key",
        "secret",
    ]));
    assert!(result.is_err());
}

#[test]
fn aws_access_key_requires_secret_key() {
    let result = parse_from_args(args(&[
        "s3://bucket",
        "--target-access-key",
        "AKIAIOSFODNN7EXAMPLE",
    ]));
    assert!(result.is_err());
}

#[test]
fn aws_accelerate() {
    let cli = parse_from_args(args(&["s3://bucket", "--target-accelerate"])).unwrap();
    assert!(cli.target_accelerate);
}

#[test]
fn aws_request_payer() {
    let cli = parse_from_args(args(&["s3://bucket", "--target-request-payer"])).unwrap();
    assert!(cli.target_request_payer);
}

#[test]
fn aws_disable_stalled_stream_protection() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--disable-stalled-stream-protection"])).unwrap();
    assert!(cli.disable_stalled_stream_protection);
}

// ===========================================================================
// 8. Performance
// ===========================================================================

#[test]
fn perf_max_parallel_listings() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--max-parallel-listings", "32"])).unwrap();
    assert_eq!(cli.max_parallel_listings, 32);
}

#[test]
fn perf_max_parallel_listing_max_depth() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--max-parallel-listing-max-depth", "5"])).unwrap();
    assert_eq!(cli.max_parallel_listing_max_depth, 5);
}

#[test]
fn perf_object_listing_queue_size() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--object-listing-queue-size", "500000"])).unwrap();
    assert_eq!(cli.object_listing_queue_size, 500000);
}

#[test]
fn perf_allow_parallel_listings_in_express_one_zone() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--allow-parallel-listings-in-express-one-zone",
    ]))
    .unwrap();
    assert!(cli.allow_parallel_listings_in_express_one_zone);
}

#[test]
fn perf_reject_zero_max_parallel_listings() {
    let result = parse_from_args(args(&["s3://bucket", "--max-parallel-listings", "0"]));
    assert!(result.is_err());
}

#[test]
fn perf_reject_zero_max_parallel_listing_max_depth() {
    let result = parse_from_args(args(&["s3://bucket", "--max-parallel-listing-max-depth", "0"]));
    assert!(result.is_err());
}

#[test]
fn perf_reject_zero_object_listing_queue_size() {
    let result = parse_from_args(args(&["s3://bucket", "--object-listing-queue-size", "0"]));
    assert!(result.is_err());
}

// ===========================================================================
// 9. Retry
// ===========================================================================

#[test]
fn retry_aws_max_attempts() {
    let cli = parse_from_args(args(&["s3://bucket", "--aws-max-attempts", "5"])).unwrap();
    assert_eq!(cli.aws_max_attempts, 5);
}

#[test]
fn retry_initial_backoff_milliseconds() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--initial-backoff-milliseconds", "250"])).unwrap();
    assert_eq!(cli.initial_backoff_milliseconds, 250);
}

// ===========================================================================
// 10. Timeout
// ===========================================================================

#[test]
fn timeout_operation() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--operation-timeout-milliseconds", "30000"]))
            .unwrap();
    assert_eq!(cli.operation_timeout_milliseconds, Some(30000));
}

#[test]
fn timeout_operation_attempt() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--operation-attempt-timeout-milliseconds",
        "5000",
    ]))
    .unwrap();
    assert_eq!(cli.operation_attempt_timeout_milliseconds, Some(5000));
}

#[test]
fn timeout_connect() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--connect-timeout-milliseconds", "3000"])).unwrap();
    assert_eq!(cli.connect_timeout_milliseconds, Some(3000));
}

#[test]
fn timeout_read() {
    let cli =
        parse_from_args(args(&["s3://bucket", "--read-timeout-milliseconds", "10000"])).unwrap();
    assert_eq!(cli.read_timeout_milliseconds, Some(10000));
}

// ===========================================================================
// 11. Advanced
// ===========================================================================

#[test]
fn advanced_max_keys() {
    let cli = parse_from_args(args(&["s3://bucket", "--max-keys", "500"])).unwrap();
    assert_eq!(cli.max_keys, 500);
}

#[test]
fn advanced_reject_max_keys_zero() {
    let result = parse_from_args(args(&["s3://bucket", "--max-keys", "0"]));
    assert!(result.is_err());
}

#[test]
fn advanced_reject_max_keys_above_range() {
    let result = parse_from_args(args(&["s3://bucket", "--max-keys", "1001"]));
    assert!(result.is_err());
}

// ===========================================================================
// 12. Target validation
// ===========================================================================

#[test]
fn target_invalid_no_s3_prefix() {
    let result = parse_from_args(args(&["mybucket/prefix"]));
    assert!(result.is_err());
}

#[test]
fn target_missing_enters_bucket_listing_mode() {
    let cli = parse_from_args(args(&[])).unwrap();
    assert_eq!(cli.target, "");
}

#[test]
fn target_bucket_only() {
    let cli = parse_from_args(args(&["s3://mybucket"])).unwrap();
    assert_eq!(cli.target, "s3://mybucket");
}

#[test]
fn target_bucket_with_trailing_slash() {
    let cli = parse_from_args(args(&["s3://mybucket/"])).unwrap();
    assert_eq!(cli.target, "s3://mybucket/");
}

// ===========================================================================
// 13. Defaults
// ===========================================================================

#[test]
fn verify_all_defaults() {
    let cli = parse_from_args(args(&["s3://bucket"])).unwrap();

    // Target
    assert_eq!(cli.target, "s3://bucket");

    // General
    assert!(!cli.recursive);
    assert!(!cli.all_versions);

    // Filtering
    assert!(cli.filter_include_regex.is_none());
    assert!(cli.filter_exclude_regex.is_none());
    assert!(cli.filter_mtime_before.is_none());
    assert!(cli.filter_mtime_after.is_none());
    assert!(cli.filter_smaller_size.is_none());
    assert!(cli.filter_larger_size.is_none());
    assert!(cli.storage_class.is_none());

    // Sort
    assert_eq!(cli.sort, vec![SortField::Key]);
    assert!(!cli.reverse);

    // Display
    assert!(!cli.summary);
    assert!(!cli.human);
    assert!(!cli.show_fullpath);
    assert!(!cli.show_etag);
    assert!(!cli.show_storage_class);
    assert!(!cli.show_checksum_algorithm);
    assert!(!cli.show_checksum_type);
    assert!(!cli.json);

    // Tracing
    assert!(!cli.json_tracing);
    assert!(!cli.aws_sdk_tracing);
    assert!(!cli.span_events_tracing);
    assert!(!cli.disable_color_tracing);

    // AWS Configuration
    assert!(cli.aws_config_file.is_none());
    assert!(cli.aws_shared_credentials_file.is_none());
    assert!(cli.target_profile.is_none());
    assert!(cli.target_access_key.is_none());
    assert!(cli.target_secret_access_key.is_none());
    assert!(cli.target_session_token.is_none());
    assert!(cli.target_region.is_none());
    assert!(cli.target_endpoint_url.is_none());
    assert!(!cli.target_force_path_style);
    assert!(!cli.target_accelerate);
    assert!(!cli.target_request_payer);
    assert!(!cli.disable_stalled_stream_protection);

    // Performance
    assert_eq!(cli.max_parallel_listings, 32);
    assert_eq!(cli.max_parallel_listing_max_depth, 2);
    assert_eq!(cli.object_listing_queue_size, 200000);
    assert!(!cli.allow_parallel_listings_in_express_one_zone);

    // Retry
    assert_eq!(cli.aws_max_attempts, 10);
    assert_eq!(cli.initial_backoff_milliseconds, 100);

    // Timeout
    assert!(cli.operation_timeout_milliseconds.is_none());
    assert!(cli.operation_attempt_timeout_milliseconds.is_none());
    assert!(cli.connect_timeout_milliseconds.is_none());
    assert!(cli.read_timeout_milliseconds.is_none());

    // Advanced
    assert_eq!(cli.max_keys, 1000);
    assert!(cli.auto_complete_shell.is_none());
}

// ===========================================================================
// 14. Property tests (proptest)
// ===========================================================================

proptest! {
    #[test]
    fn proptest_reject_invalid_targets(target in "[a-z]{1,20}") {
        // Strings that do not start with s3:// and have length > 5 after prefix should fail
        let result = parse_from_args(args(&[&target]));
        prop_assert!(result.is_err());
    }

    #[test]
    fn proptest_accept_valid_targets(bucket in "[a-z][a-z0-9\\-]{2,20}") {
        let target = format!("s3://{bucket}");
        let result = parse_from_args(args(&[&target]));
        prop_assert!(result.is_ok());
    }

    #[test]
    fn proptest_flag_aliases_recursive(use_short in proptest::bool::ANY) {
        let flag = if use_short { "-r" } else { "--recursive" };
        let cli = parse_from_args(args(&["s3://bucket", flag])).unwrap();
        prop_assert!(cli.recursive);
    }
}

// ===========================================================================
// 15. Human bytes (parse_human_bytes from parent module)
// ===========================================================================

#[test]
fn human_bytes_mib() {
    assert_eq!(parse_human_bytes("1MiB").unwrap(), 1024 * 1024);
}

#[test]
fn human_bytes_gib() {
    assert_eq!(parse_human_bytes("1GiB").unwrap(), 1024 * 1024 * 1024);
}

#[test]
fn human_bytes_kib() {
    assert_eq!(parse_human_bytes("1KiB").unwrap(), 1024);
}

#[test]
fn human_bytes_plain() {
    assert_eq!(parse_human_bytes("4096").unwrap(), 4096);
}

#[test]
fn human_bytes_invalid() {
    assert!(parse_human_bytes("notanumber").is_err());
}

#[test]
fn human_bytes_zero() {
    assert_eq!(parse_human_bytes("0").unwrap(), 0);
}

#[test]
fn human_bytes_tib() {
    assert_eq!(
        parse_human_bytes("1TiB").unwrap(),
        1024u64 * 1024 * 1024 * 1024
    );
}

#[test]
fn human_bytes_large_value_8eib() {
    // 8 EiB = 8 * 2^60 = 9223372036854775808 which fits in u64
    let result = parse_human_bytes("8EiB");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 8u64 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024);
}

// ===========================================================================
// 16. Full combination
// ===========================================================================

#[test]
fn full_combination_many_flags() {
    let cli = parse_from_args(args(&[
        "s3://my-bucket/logs/2024/",
        "--recursive",
        "--all-versions",
        "--filter-include-regex",
        r"\.json$",
        "--filter-smaller-size",
        "50MiB",
        "--filter-larger-size",
        "1KiB",
        "--storage-class",
        "STANDARD,GLACIER",
        "--sort",
        "date",
        "--reverse",
        "--summary",
        "--human",
        "--show-fullpath",
        "--show-etag",
        "--json",
        "--target-region",
        "eu-west-1",
        "--max-parallel-listings",
        "64",
        "--max-keys",
        "500",
        "--aws-max-attempts",
        "3",
        "--operation-timeout-milliseconds",
        "60000",
    ]))
    .unwrap();

    assert_eq!(cli.target, "s3://my-bucket/logs/2024/");
    assert!(cli.recursive);
    assert!(cli.all_versions);
    assert_eq!(cli.filter_include_regex.as_deref(), Some(r"\.json$"));
    assert_eq!(cli.filter_smaller_size.as_deref(), Some("50MiB"));
    assert_eq!(cli.filter_larger_size.as_deref(), Some("1KiB"));
    assert_eq!(
        cli.storage_class,
        Some(vec!["STANDARD".to_string(), "GLACIER".to_string()])
    );
    assert_eq!(cli.sort, vec![SortField::Date]);
    assert!(cli.reverse);
    assert!(cli.summary);
    assert!(cli.human);
    assert!(cli.show_fullpath);
    assert!(cli.show_etag);
    assert!(cli.json);
    assert_eq!(cli.target_region.as_deref(), Some("eu-west-1"));
    assert_eq!(cli.max_parallel_listings, 64);
    assert_eq!(cli.max_keys, 500);
    assert_eq!(cli.aws_max_attempts, 3);
    assert_eq!(cli.operation_timeout_milliseconds, Some(60000));
}

// ===========================================================================
// 17. Help / version
// ===========================================================================

#[test]
fn help_does_not_panic() {
    let result = parse_from_args(args(&["--help"]));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
}

#[test]
fn version_does_not_panic() {
    let result = parse_from_args(args(&["--version"]));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
}

// ===========================================================================
// Config building tests
// ===========================================================================

#[test]
fn config_from_minimal_args() {
    let config = build_config_from_args(vec!["s3ls", "s3://my-bucket/prefix/"]).unwrap();
    assert_eq!(config.target.bucket, "my-bucket");
    assert_eq!(config.target.prefix.as_deref(), Some("prefix/"));
    assert!(!config.recursive);
    assert!(!config.all_versions);
}

#[test]
fn config_from_full_args() {
    let config = build_config_from_args(vec![
        "s3ls",
        "s3://bucket/logs/",
        "--recursive",
        "--all-versions",
        "--filter-include-regex",
        r".*\.log$",
        "--filter-larger-size",
        "1GiB",
        "--storage-class",
        "STANDARD,GLACIER",
        "--sort",
        "date",
        "--reverse",
        "--human",
        "--summary",
        "--json",
    ])
    .unwrap();
    assert!(config.recursive);
    assert!(config.all_versions);
    assert!(config.filter_config.include_regex.is_some());
    assert_eq!(config.filter_config.larger_size, Some(1024 * 1024 * 1024));
    assert_eq!(
        config.filter_config.storage_class.unwrap(),
        vec!["STANDARD", "GLACIER"]
    );
    assert_eq!(config.sort, vec![SortField::Date]);
    assert!(config.reverse);
    assert!(config.display_config.human);
    assert!(config.display_config.summary);
    assert!(config.display_config.json);
}

#[test]
fn config_filter_size_values_are_u64() {
    let config = build_config_from_args(vec![
        "s3ls",
        "s3://bucket/",
        "--filter-larger-size",
        "1GiB",
        "--filter-smaller-size",
        "2GiB",
    ])
    .unwrap();
    assert_eq!(config.filter_config.larger_size, Some(1024 * 1024 * 1024));
    assert_eq!(
        config.filter_config.smaller_size,
        Some(2 * 1024 * 1024 * 1024)
    );
}

#[test]
fn build_config_no_target_creates_empty_bucket() {
    let config = build_config_from_args(vec!["s3ls"]).unwrap();
    assert!(config.target.bucket.is_empty());
    assert!(config.target.prefix.is_none());
}

#[test]
fn config_tracing_config_none_when_silent() {
    let config = build_config_from_args(vec!["s3ls", "s3://bucket/", "-qq"]).unwrap();
    assert!(config.tracing_config.is_none());
}

#[test]
fn config_tracing_config_info_with_verbose() {
    let config = build_config_from_args(vec!["s3ls", "s3://bucket/", "-v"]).unwrap();
    assert!(config.tracing_config.is_some());
    assert_eq!(
        config.tracing_config.unwrap().tracing_level,
        clap_verbosity_flag::log::Level::Info
    );
}
