#![cfg(e2e_test)]

//! Bucket listing end-to-end tests.
//!
//! Covers s3ls bucket listing (no target URL) in JSON mode: default
//! JSON shape, `--bucket-name-prefix` filtering, combined display
//! flags, `--no-sort`, and `--list-express-one-zone-buckets`.
//!
//! All assertions scope to test bucket names because the AWS account
//! may have other buckets.
//!
//! Design: `docs/superpowers/specs/2026-04-11-bucket-listing-e2e-tests-design.md`

mod common;

use common::*;

/// Default bucket listing JSON shape: verify mandatory fields are present
/// and optional fields are absent when no display flags are set.
#[tokio::test]
async fn e2e_bucket_listing_default_json_shape() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&["--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Find our test bucket in the NDJSON output.
        let bucket_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| v.get("Name").and_then(|n| n.as_str()).map(|s| s == bucket))
                    .unwrap_or(false)
            })
            .unwrap_or_else(|| panic!("default shape: test bucket {bucket} not found in output"));

        let v: serde_json::Value =
            serde_json::from_str(bucket_line).expect("failed to parse bucket line");

        // Mandatory fields
        assert_eq!(
            v.get("Name").and_then(|n| n.as_str()),
            Some(bucket.as_str()),
            "default shape: Name mismatch"
        );
        assert!(
            v.get("CreationDate")
                .and_then(|d| d.as_str())
                .is_some_and(|s| !s.is_empty()),
            "default shape: CreationDate missing or empty, got {v:?}"
        );
        assert!(
            v.get("BucketRegion")
                .and_then(|r| r.as_str())
                .is_some_and(|s| !s.is_empty()),
            "default shape: BucketRegion missing or empty, got {v:?}"
        );

        // Optional fields must be absent (no display flags set)
        assert!(
            v.get("BucketArn").is_none(),
            "default shape: BucketArn should be absent without --show-bucket-arn, got {v:?}"
        );
        assert!(
            v.get("Owner").is_none(),
            "default shape: Owner should be absent without --show-owner, got {v:?}"
        );
    });

    _guard.cleanup().await;
}

/// `--bucket-name-prefix` filtering: only buckets whose name starts
/// with the prefix appear in the output. A non-matching bucket is
/// excluded.
#[tokio::test]
async fn e2e_bucket_listing_prefix_filter() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let id = Uuid::new_v4();
    let bucket_match = format!("s3ls-e2e-pfx-match-{id}");
    let bucket_other = format!("s3ls-e2e-pfx-other-{id}");
    let _guard_match = helper.bucket_guard(&bucket_match);
    let _guard_other = helper.bucket_guard(&bucket_other);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_match).await;
        helper.create_bucket(&bucket_other).await;

        let output =
            TestHelper::run_s3ls(&["--json", "--bucket-name-prefix", "s3ls-e2e-pfx-match-"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Matching bucket must appear.
        let found_match = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket_match)
                    })
                    .unwrap_or(false)
            });
        assert!(
            found_match,
            "prefix filter: matching bucket {bucket_match} not found in output"
        );

        // Non-matching bucket must NOT appear.
        let found_other = output.stdout.contains(&bucket_other);
        assert!(
            !found_other,
            "prefix filter: non-matching bucket {bucket_other} unexpectedly found in output"
        );
    });

    _guard_match.cleanup().await;
    _guard_other.cleanup().await;
}

/// `--bucket-name-prefix` with a prefix that matches nothing: s3ls
/// exits successfully with no bucket entries in the output.
#[tokio::test]
async fn e2e_bucket_listing_prefix_no_match() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    // Create a bucket just for guard lifecycle (its name is irrelevant).
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Use a UUID-suffixed prefix that is guaranteed to match no bucket.
        let no_match_prefix = format!("s3ls-e2e-nonexistent-{}", Uuid::new_v4());

        let output =
            TestHelper::run_s3ls(&["--json", "--bucket-name-prefix", no_match_prefix.as_str()]);
        assert!(
            output.status.success(),
            "prefix no-match: s3ls should exit 0 even with no matches, got: {}",
            output.stderr
        );

        // No NDJSON line should have a Name field.
        let has_name = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| v.get("Name").map(|_| true))
                    .unwrap_or(false)
            });
        assert!(
            !has_name,
            "prefix no-match: expected no bucket entries in output, but found at least one"
        );
    });

    _guard.cleanup().await;
}
