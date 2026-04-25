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
/// Skips gracefully when:
/// - The region has no mapped Express One Zone AZ
///   (`express_one_zone_az_for_region` returns `None`).
/// - S3 rejects the directory bucket creation (wrong AZ, unsupported
///   region, missing permissions, etc.) — `try_create_directory_bucket`
///   returns `Err`.
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
    // Directory bucket name must be ≤63 chars. "s3ls-e2e-" (9) + uuid (36)
    // + "--" + az_id (up to 9) + "--x-s3" (6) = up to 63.
    let bucket_express = format!("s3ls-e2e-{id}--{az_id}--x-s3");
    let bucket_regular = format!("s3ls-e2e-regular-{id}");
    let _guard_express = helper.bucket_guard(&bucket_express);
    let _guard_regular = helper.bucket_guard(&bucket_regular);

    e2e_timeout!(async {
        // Try to create the directory bucket. Skip if S3 rejects it
        // (wrong AZ, unsupported region, missing permissions, etc.).
        if let Err(e) = helper
            .try_create_directory_bucket(&bucket_express, az_id)
            .await
        {
            println!("skipped: {e}");
            return;
        }

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

/// `--list-express-one-zone-buckets --bucket-name-prefix`: filters
/// the directory bucket listing by name prefix. Creates two directory
/// buckets with distinct name prefixes and verifies only the matching
/// one appears in the output.
///
/// Note: the `ListDirectoryBuckets` API does not support server-side
/// prefix filtering — `src/bucket_lister.rs:29-31` applies it
/// client-side via `buckets.retain(|e| e.name.starts_with(prefix))`.
/// This test exercises that client-side filtering path.
///
/// Skips gracefully when the region doesn't support Express One Zone
/// or bucket creation fails.
#[tokio::test]
async fn e2e_bucket_listing_express_one_zone_with_prefix() {
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
    // Two directory buckets with distinct name prefixes.
    // Both must be ≤63 chars: "s3ls-e2e-{tag}-{short_id}--{az}--x-s3"
    let short_id = &id.to_string()[..8];
    let bucket_match = format!("s3ls-e2e-m-{short_id}--{az_id}--x-s3");
    let bucket_other = format!("s3ls-e2e-o-{short_id}--{az_id}--x-s3");
    let _guard_match = helper.bucket_guard(&bucket_match);
    let _guard_other = helper.bucket_guard(&bucket_other);

    e2e_timeout!(async {
        if let Err(e) = helper
            .try_create_directory_bucket(&bucket_match, az_id)
            .await
        {
            println!("skipped: {e}");
            return;
        }
        if let Err(e) = helper
            .try_create_directory_bucket(&bucket_other, az_id)
            .await
        {
            println!("skipped: {e}");
            return;
        }

        // Filter directory buckets by the "match" prefix.
        let prefix = format!("s3ls-e2e-m-{short_id}");
        let output = TestHelper::run_s3ls(&[
            "--json",
            "--list-express-one-zone-buckets",
            "--bucket-name-prefix",
            prefix.as_str(),
        ]);
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
            "express prefix filter: matching bucket {bucket_match} not found in output"
        );

        // Non-matching bucket must NOT appear.
        assert!(
            !output.stdout.contains(&bucket_other),
            "express prefix filter: non-matching bucket {bucket_other} unexpectedly found in output"
        );
    });

    _guard_match.cleanup().await;
    _guard_other.cleanup().await;
}

/// `--header --show-bucket-arn --show-owner` combined (default aligned format).
///
/// Verifies that every column in the aligned bucket listing lands at the
/// byte offset computed from the width constants in
/// `s3ls_rs::display::aligned`.  The column order is:
///   DATE, REGION, BUCKET, BUCKET_ARN, OWNER_DISPLAY_NAME, OWNER_ID
/// where OWNER_ID is the rightmost (unpadded) column — analogous to KEY
/// in object listings.
///
/// Uses `--bucket-name-prefix` to scope the output to the single test
/// bucket so assertions are deterministic even when the account has many
/// buckets.
#[tokio::test]
async fn e2e_aligned_bucket_listing_all_columns() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Scope to just our test bucket via --bucket-name-prefix.
        let output = TestHelper::run_s3ls_no_default_format(&[
            "--header",
            "--show-bucket-arn",
            "--show-owner",
            "--bucket-name-prefix",
            bucket.as_str(),
        ]);
        assert!(
            output.status.success(),
            "aligned bucket all-columns: s3ls failed: {}",
            output.stderr
        );

        // Compute the prefix length up to (but not including) the OWNER_ID
        // column.  OWNER_ID is the last/rightmost column and is emitted
        // unpadded — so we assert only that what comes before it is exactly
        // the right length.
        use s3ls_rs::display::aligned::{
            SEP, W_BUCKET_ARN, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE, W_OWNER_DISPLAY_NAME,
        };
        let sep = SEP.len();
        // Prefix = DATE + SEP + REGION + SEP + BUCKET + SEP + BUCKET_ARN + SEP
        //        + OWNER_DISPLAY_NAME + SEP
        let prefix_before_owner_id = W_DATE
            + sep
            + W_BUCKET_REGION
            + sep
            + W_BUCKET_NAME
            + sep
            + W_BUCKET_ARN
            + sep
            + W_OWNER_DISPLAY_NAME
            + sep;

        // Collect all non-empty lines.
        let all_lines: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();

        // Must have at least a header and one data line.
        assert!(
            all_lines.len() >= 2,
            "aligned bucket all-columns: expected at least 2 lines (header + data), got {}:\n{}",
            all_lines.len(),
            output.stdout
        );

        // No row should contain a tab — aligned mode uses spaces only.
        for line in &all_lines {
            assert!(
                !line.contains('\t'),
                "aligned bucket all-columns: row contains tab (aligned mode must not use tabs): {line:?}"
            );
        }

        // Header line: starts with "DATE", and at the expected offset the
        // OWNER_ID label begins.
        let header = all_lines[0];
        assert!(
            header.starts_with("DATE"),
            "aligned bucket all-columns: header should start with 'DATE', got: {header:?}"
        );
        assert!(
            header.len() >= prefix_before_owner_id,
            "aligned bucket all-columns: header shorter than prefix ({prefix_before_owner_id}): {header:?}"
        );
        // The text at the OWNER_DISPLAY_NAME offset in the header is "OWNER_ID".
        let owner_id_label = &header[prefix_before_owner_id..];
        assert!(
            owner_id_label.starts_with("OWNER_ID"),
            "aligned bucket all-columns: expected 'OWNER_ID' at offset {prefix_before_owner_id} in header, got: {owner_id_label:?}"
        );

        // Data rows: the bucket name we created must appear, and the prefix
        // length before OWNER_ID must equal prefix_before_owner_id.
        let data_rows: Vec<&str> = all_lines
            .iter()
            .copied()
            .filter(|l| !l.starts_with("DATE"))
            .collect();
        assert!(
            !data_rows.is_empty(),
            "aligned bucket all-columns: no data rows found in output:\n{}",
            output.stdout
        );
        for row in &data_rows {
            assert!(
                row.len() >= prefix_before_owner_id,
                "aligned bucket all-columns: data row shorter than prefix ({prefix_before_owner_id}): {row:?}"
            );
        }

        // The test bucket must appear somewhere in the output.
        assert!(
            output.stdout.contains(bucket.as_str()),
            "aligned bucket all-columns: test bucket {bucket} not found in output"
        );
    });

    _guard.cleanup().await;
}

/// `-1` bucket listing: verifies that the one-line formatter emits
/// exactly the bucket name per line (no tabs), and that `--header`
/// prepends exactly `BUCKET`.
///
/// Uses `--bucket-name-prefix {bucket}` to scope the output to just the
/// test bucket so assertions are deterministic even when the account has
/// many buckets.
#[tokio::test]
async fn e2e_one_line_bucket_listing() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sub-assertion 1: -1 without --header
        let output = TestHelper::run_s3ls(&["-1", "--bucket-name-prefix", bucket.as_str()]);
        assert!(
            output.status.success(),
            "one-line bucket -1: s3ls failed: {}",
            output.stderr
        );
        // No tabs anywhere.
        assert!(
            !output.stdout.contains('\t'),
            "one-line bucket -1: output contains tab character"
        );
        // The bucket name must appear as a line.
        assert!(
            output.stdout.contains(bucket.as_str()),
            "one-line bucket -1: bucket {bucket} missing from output"
        );
        // Every non-empty line should be a bucket name (no date/region columns).
        let lines: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        assert!(!lines.is_empty(), "one-line bucket -1: no output lines");
        // No line contains a space (bucket names are alphanumeric-and-dash only,
        // and one-line mode must not add padding or extra columns).
        for line in &lines {
            assert!(
                !line.contains(' '),
                "one-line bucket -1: line contains space (unexpected columns or padding): {line:?}"
            );
        }

        // Sub-assertion 2: --header -1 prepends exactly "BUCKET"
        let output2 =
            TestHelper::run_s3ls(&["--header", "-1", "--bucket-name-prefix", bucket.as_str()]);
        assert!(
            output2.status.success(),
            "one-line bucket --header: s3ls failed: {}",
            output2.stderr
        );
        assert!(
            !output2.stdout.contains('\t'),
            "one-line bucket --header: output contains tab character"
        );
        let all_lines2: Vec<&str> = output2
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        // First non-blank line must be exactly "BUCKET".
        assert_eq!(
            all_lines2.first().copied().unwrap_or(""),
            "BUCKET",
            "one-line bucket --header: first non-blank line should be 'BUCKET', got: {:?}",
            all_lines2.first()
        );
        // Subsequent lines must include the test bucket name.
        let data_lines2: Vec<&str> = all_lines2.iter().copied().skip(1).collect();
        assert!(
            data_lines2.contains(&bucket.as_str()),
            "one-line bucket --header: bucket {bucket} not found after header"
        );
    });

    _guard.cleanup().await;
}

/// `--list-express-one-zone-buckets` (default aligned format): verifies
/// that aligned mode works correctly for directory bucket listings.
///
/// Verifies:
/// - Exit 0.
/// - At least one data row (the directory bucket created for this test).
/// - No tabs in any row (aligned mode uses spaces).
/// - Every data row is at least `W_DATE + SEP.len() + W_BUCKET_REGION + SEP.len()`
///   characters long, confirming the date+region prefix is padded.
///
/// Skips gracefully when:
/// - The region has no mapped Express One Zone AZ.
/// - S3 rejects the directory bucket creation (wrong AZ, unsupported
///   region, missing permissions, etc.).
#[tokio::test]
async fn e2e_aligned_bucket_listing_express_one_zone() {
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
    let short_id = &id.to_string()[..8];
    // Directory bucket name: "s3ls-e2e-al-{short_id}--{az_id}--x-s3"
    // "s3ls-e2e-al-" (12) + 8 + "--" (2) + az_id (≤9) + "--x-s3" (6) = ≤37 chars.
    let bucket_express = format!("s3ls-e2e-al-{short_id}--{az_id}--x-s3");
    let _guard_express = helper.bucket_guard(&bucket_express);

    e2e_timeout!(async {
        // Try to create the directory bucket. Skip if S3 rejects it.
        if let Err(e) = helper
            .try_create_directory_bucket(&bucket_express, az_id)
            .await
        {
            println!("skipped: {e}");
            return;
        }

        // Use the short_id prefix so only this test's bucket is included.
        let prefix = format!("s3ls-e2e-al-{short_id}");
        let output = TestHelper::run_s3ls_no_default_format(&[
            "--list-express-one-zone-buckets",
            "--bucket-name-prefix",
            prefix.as_str(),
        ]);
        assert!(
            output.status.success(),
            "aligned express one zone: s3ls failed: {}",
            output.stderr
        );

        // Collect all non-empty data rows (no header was requested).
        let data_rows: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();

        assert!(
            !data_rows.is_empty(),
            "aligned express one zone: expected at least one data row, got none:\n{}",
            output.stdout
        );

        // No row should contain a tab — aligned mode uses spaces only.
        for row in &data_rows {
            assert!(
                !row.contains('\t'),
                "aligned express one zone: row contains tab (aligned mode must not use tabs): {row:?}"
            );
        }

        // Every data row must be at least W_DATE + SEP + W_BUCKET_REGION + SEP long.
        use s3ls_rs::display::aligned::{SEP, W_BUCKET_REGION, W_DATE};
        let min_prefix_len = W_DATE + SEP.len() + W_BUCKET_REGION + SEP.len();
        for row in &data_rows {
            assert!(
                row.len() >= min_prefix_len,
                "aligned express one zone: row shorter than expected date+region prefix ({min_prefix_len}): {row:?}"
            );
        }

        // The directory bucket we created must appear somewhere in the output.
        assert!(
            output.stdout.contains(bucket_express.as_str()),
            "aligned express one zone: directory bucket {bucket_express} not found in output"
        );
    });

    _guard_express.cleanup().await;
}
