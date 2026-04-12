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

/// `--show-bucket-arn --show-owner` together: verify ALL fields are
/// present. The display suite tests each flag individually; this test
/// verifies they compose correctly without interference.
#[tokio::test]
async fn e2e_bucket_listing_combined_flags() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&["--json", "--show-bucket-arn", "--show-owner"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

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
            .unwrap_or_else(|| panic!("combined flags: test bucket {bucket} not found in output"));

        let v: serde_json::Value =
            serde_json::from_str(bucket_line).expect("failed to parse bucket line");

        // Mandatory fields
        assert!(v.get("Name").is_some(), "combined flags: Name missing");
        assert!(
            v.get("CreationDate").is_some(),
            "combined flags: CreationDate missing"
        );
        assert!(
            v.get("BucketRegion").is_some(),
            "combined flags: BucketRegion missing"
        );

        // Both optional fields must be present
        assert!(
            v.get("BucketArn")
                .and_then(|a| a.as_str())
                .is_some_and(|s| !s.is_empty()),
            "combined flags: BucketArn missing or empty, got {v:?}"
        );
        let owner = v.get("Owner").expect("combined flags: Owner missing");
        assert!(
            owner
                .get("ID")
                .and_then(|id| id.as_str())
                .is_some_and(|s| !s.is_empty()),
            "combined flags: Owner.ID missing or empty, got {owner:?}"
        );
    });

    _guard.cleanup().await;
}

/// `--no-sort`: both test buckets appear in the output (set check,
/// order not asserted).
#[tokio::test]
async fn e2e_bucket_listing_no_sort() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&["--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Both buckets must appear somewhere in the output.
        assert!(
            output.stdout.contains(&bucket_a),
            "no-sort: bucket {bucket_a} not found in output"
        );
        assert!(
            output.stdout.contains(&bucket_z),
            "no-sort: bucket {bucket_z} not found in output"
        );
        // Order is intentionally NOT asserted.
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}

/// `--list-express-one-zone-buckets`: creates a directory bucket and a
/// regular bucket, then lists only Express One Zone buckets. The
/// directory bucket must appear; the regular bucket must not.
///
/// Skips gracefully in regions where Express One Zone is not mapped
/// (prints a note and returns without assertions).
#[tokio::test]
async fn e2e_bucket_listing_express_one_zone() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;

    // Look up AZ for this region. Skip if unmapped.
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
    let bucket_express = format!("s3ls-e2e-express-{id}--{az_id}--x-s3");
    let bucket_regular = format!("s3ls-e2e-regular-{id}");
    let _guard_express = helper.bucket_guard(&bucket_express);
    let _guard_regular = helper.bucket_guard(&bucket_regular);

    e2e_timeout!(async {
        helper.create_directory_bucket(&bucket_express, az_id).await;
        helper.create_bucket(&bucket_regular).await;

        let output = TestHelper::run_s3ls(&["--json", "--list-express-one-zone-buckets"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Directory bucket must appear.
        let found_express = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .any(|l| {
                serde_json::from_str::<serde_json::Value>(l)
                    .ok()
                    .and_then(|v| {
                        v.get("Name")
                            .and_then(|n| n.as_str())
                            .map(|s| s == bucket_express)
                    })
                    .unwrap_or(false)
            });
        assert!(
            found_express,
            "express one zone: directory bucket {bucket_express} not found in output"
        );

        // Regular bucket must NOT appear.
        let found_regular = output.stdout.contains(&bucket_regular);
        assert!(
            !found_regular,
            "express one zone: regular bucket {bucket_regular} unexpectedly found in output"
        );
    });

    _guard_express.cleanup().await;
    _guard_regular.cleanup().await;
}
