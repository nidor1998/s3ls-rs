#![cfg(e2e_test)]

mod common;

use common::*;
use s3ls_rs::create_pipeline_cancellation_token;

/// Binary-path smoke test.
///
/// Uploads three objects in reverse alphabetical order and runs
/// `s3ls --recursive`, asserting via `assert_key_order` that s3ls's default
/// key-sort produces alphabetical output. This double-purposes as framework
/// plumbing verification (TestHelper, bucket lifecycle, run_s3ls, S3lsOutput,
/// assert_key_order, e2e_timeout!, BucketGuard) and as a regression check
/// against s3ls's sort stability.
#[tokio::test]
async fn e2e_binary_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload in REVERSE alphabetical order — default key sort must still
        // produce a, b, c.
        helper.put_object(&bucket, "c.txt", b"ccc".to_vec()).await;
        helper.put_object(&bucket, "b.txt", b"bb".to_vec()).await;
        helper.put_object(&bucket, "a.txt", b"a".to_vec()).await;

        let target = format!("s3://{bucket}/");
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive"]);

        assert!(
            output.status.success(),
            "s3ls exited non-zero: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            output.stdout,
            output.stderr
        );

        assert_key_order(&output.stdout, &["a.txt", "b.txt", "c.txt"]);
    });

    _guard.cleanup().await;
}

/// Programmatic-path smoke test.
///
/// Builds a `Config` via `TestHelper::build_config`, constructs a
/// `ListingPipeline`, and runs it. Asserts only that the pipeline returned
/// `Ok(())` — rendered output is the binary path's concern. This catches
/// API-drift bugs at the `s3ls_rs` public-API surface (`Config`,
/// `ListingPipeline::new`, `ListingPipeline::run`, cancellation token).
#[tokio::test]
async fn e2e_programmatic_smoke() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object(&bucket, "file.txt", b"hello".to_vec())
            .await;

        let target = format!("s3://{bucket}/");
        let config = TestHelper::build_config(vec![target.as_str(), "--recursive"]);
        let token = create_pipeline_cancellation_token();
        let pipeline = s3ls_rs::ListingPipeline::new(config, token);

        pipeline.run().await.expect("pipeline run failed");
    });

    _guard.cleanup().await;
}

/// Object listing inside an Express One Zone (directory) bucket with
/// prefix scoping. Uploads objects under `data/` and `logs/` prefixes,
/// then lists with `s3://express-bucket/data/` to verify only the
/// matching prefix's objects are returned.
///
/// Skips gracefully when:
/// - The region has no mapped Express One Zone AZ.
/// - S3 rejects the directory bucket creation.
#[tokio::test]
async fn e2e_listing_express_one_zone_with_prefix() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;

    let az_id = match express_one_zone_az_for_region(helper.region()) {
        Some(az) => az,
        None => {
            println!(
                "skipped: no Express One Zone AZ mapped for region {:?}",
                helper.region()
            );
            return;
        }
    };

    let id = Uuid::new_v4();
    let bucket = format!("s3ls-e2e-{id}--{az_id}--x-s3");
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        if let Err(e) = helper.try_create_directory_bucket(&bucket, az_id).await {
            println!("skipped: {e}");
            return;
        }

        // Upload objects under two distinct prefixes.
        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("data/a.txt".to_string(), b"x".to_vec()),
            ("data/b.txt".to_string(), b"x".to_vec()),
            ("data/sub/c.txt".to_string(), b"x".to_vec()),
            ("logs/app.log".to_string(), b"x".to_vec()),
            ("logs/error.log".to_string(), b"x".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        // Sub-assertion 1: full recursive listing — all 5 objects.
        let target_full = format!("s3://{bucket}/");
        let output =
            TestHelper::run_s3ls(&[target_full.as_str(), "--recursive", "--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[
                "data/a.txt",
                "data/b.txt",
                "data/sub/c.txt",
                "logs/app.log",
                "logs/error.log",
            ],
            "express listing: full recursive",
        );

        // Sub-assertion 2: prefix-scoped listing — only data/ objects.
        let target_prefix = format!("s3://{bucket}/data/");
        let output =
            TestHelper::run_s3ls(&[target_prefix.as_str(), "--recursive", "--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["data/a.txt", "data/b.txt", "data/sub/c.txt"],
            "express listing: data/ prefix only",
        );

        // Logs must NOT appear in the prefix-scoped listing.
        assert!(
            !output.stdout.contains("logs/"),
            "express listing: logs/ should not appear in data/ listing"
        );
    });

    _guard.cleanup().await;
}

/// Verifies that basic object listing works when all timeout and retry
/// options are explicitly specified on the command line.
///
/// This test passes every timeout/retry/stalled-stream flag with
/// reasonable production-like values — proving the flags are accepted,
/// wired through to the AWS SDK client, and don't break normal listing
/// operations.
///
/// Flags exercised:
///   --aws-max-attempts 3
///   --initial-backoff-milliseconds 500
///   --operation-timeout-milliseconds 30000
///   --operation-attempt-timeout-milliseconds 10000
///   --connect-timeout-milliseconds 5000
///   --read-timeout-milliseconds 5000
///   --disable-stalled-stream-protection
#[tokio::test]
async fn e2e_listing_with_explicit_timeouts_and_retries() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        helper.put_object(&bucket, "a.txt", b"aaa".to_vec()).await;
        helper.put_object(&bucket, "b.txt", b"bb".to_vec()).await;
        helper.put_object(&bucket, "c.txt", b"c".to_vec()).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            // Retry options
            "--aws-max-attempts",
            "3",
            "--initial-backoff-milliseconds",
            "500",
            // Timeout options
            "--operation-timeout-milliseconds",
            "30000",
            "--operation-attempt-timeout-milliseconds",
            "10000",
            "--connect-timeout-milliseconds",
            "5000",
            "--read-timeout-milliseconds",
            "5000",
            // Stalled stream protection
            "--disable-stalled-stream-protection",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_json_keys_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "listing with explicit timeouts and retries",
        );
    });

    _guard.cleanup().await;
}

/// Exit code 1 when object listing fails (nonexistent bucket).
///
/// s3ls should return a non-zero exit code when it cannot list objects
/// — e.g., because the bucket doesn't exist. This test uses a
/// UUID-suffixed bucket name that is never created, so the S3 API
/// call fails with NoSuchBucket.
#[tokio::test]
async fn e2e_listing_error_returns_exit_code_1() {
    use uuid::Uuid;

    e2e_timeout!(async {
        // Bucket is never created — guaranteed to not exist.
        let nonexistent = format!("s3ls-e2e-noexist-{}", Uuid::new_v4());
        let target = format!("s3://{nonexistent}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(
            !output.status.success(),
            "listing nonexistent bucket should fail, but s3ls exited 0.\nstdout: {}\nstderr: {}",
            output.stdout,
            output.stderr
        );

        // Verify exit code is specifically 1 (not some other non-zero).
        let code = output.status.code().expect("process terminated by signal");
        assert_eq!(
            code, 1,
            "expected exit code 1 for listing error, got {code}"
        );
    });
}

/// Exit code 1 when bucket listing fails (invalid credentials).
///
/// s3ls should return exit code 1 when bucket listing fails — e.g.,
/// because the credentials are invalid. This test passes a bogus
/// access key to trigger an authentication failure.
#[tokio::test]
async fn e2e_bucket_listing_error_returns_exit_code_1() {
    e2e_timeout!(async {
        let output = TestHelper::run_s3ls(&[
            "--json",
            "--target-access-key",
            "AKIAINVALIDEXAMPLE00",
            "--target-secret-access-key",
            "wJalrXUtnFEMI/K7MDENG/INVALIDEXAMPLEKEY00",
        ]);
        assert!(
            !output.status.success(),
            "bucket listing with invalid credentials should fail, but s3ls exited 0.\nstderr: {}",
            output.stderr
        );

        let code = output.status.code().expect("process terminated by signal");
        assert_eq!(
            code, 1,
            "expected exit code 1 for bucket listing error, got {code}"
        );
    });
}

/// Exit code 0 when listing an empty bucket (no objects to display).
///
/// An empty bucket is not an error — s3ls should exit 0 with no
/// output. This test creates a bucket, uploads nothing, and verifies
/// s3ls exits cleanly.
#[tokio::test]
async fn e2e_listing_empty_bucket_returns_exit_code_0() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(
            output.status.success(),
            "listing empty bucket should succeed (exit 0), but s3ls failed.\nstderr: {}",
            output.stderr
        );

        let code = output.status.code().expect("process terminated by signal");
        assert_eq!(
            code, 0,
            "expected exit code 0 for empty bucket listing, got {code}"
        );

        // No Key entries in the output.
        let has_keys = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| v.get("Key").map(|_| true))
                    .unwrap_or(false)
            });
        assert!(
            !has_keys,
            "empty bucket should produce no Key entries in JSON output"
        );
    });

    _guard.cleanup().await;
}

/// Exit code 0 when listing a non-empty bucket with a prefix that
/// matches nothing (no objects to display, but the bucket exists).
#[tokio::test]
async fn e2e_listing_no_matching_prefix_returns_exit_code_0() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object(&bucket, "data/file.txt", b"x".to_vec())
            .await;

        // List with a prefix that matches nothing.
        let target = format!("s3://{bucket}/nonexistent-{}/", Uuid::new_v4());

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(
            output.status.success(),
            "listing with no matching prefix should succeed (exit 0), but s3ls failed.\nstderr: {}",
            output.stderr
        );

        let code = output.status.code().expect("process terminated by signal");
        assert_eq!(
            code, 0,
            "expected exit code 0 for no-matching-prefix listing, got {code}"
        );
    });

    _guard.cleanup().await;
}

/// Exit code 1 when directory bucket listing fails (invalid credentials).
///
/// `--list-express-one-zone-buckets` uses the `ListDirectoryBuckets`
/// API. With bogus credentials, the API call fails and s3ls should
/// return exit code 1.
#[tokio::test]
async fn e2e_directory_bucket_listing_error_returns_exit_code_1() {
    e2e_timeout!(async {
        let output = TestHelper::run_s3ls(&[
            "--json",
            "--list-express-one-zone-buckets",
            "--target-access-key",
            "AKIAINVALIDEXAMPLE00",
            "--target-secret-access-key",
            "wJalrXUtnFEMI/K7MDENG/INVALIDEXAMPLEKEY00",
        ]);
        assert!(
            !output.status.success(),
            "directory bucket listing with invalid credentials should fail, but s3ls exited 0.\nstderr: {}",
            output.stderr
        );

        let code = output.status.code().expect("process terminated by signal");
        assert_eq!(
            code, 1,
            "expected exit code 1 for directory bucket listing error, got {code}"
        );
    });
}
