#![cfg(e2e_test)]

//! Display end-to-end tests.
//!
//! Covers s3ls's text-format and JSON-format output rendering, including
//! every `--show-*` flag, `--header`, `--summarize`, `--human-readable`,
//! `--show-relative-path`, the CommonPrefix (PRE) and DeleteMarker
//! (DELETE) row types, and bucket listing display flags.
//!
//! Per-flag tests do 3 `run_s3ls` invocations against a single bucket
//! (text with flag on, text with flag off, JSON). Flags that gate an
//! S3 API-level fetch (`--show-owner`, `--show-restore-status`) do 4
//! invocations to observe the JSON field's presence/absence.
//!
//! Design: `docs/superpowers/specs/2026-04-11-display-e2e-tests-design.md`

mod common;

use common::*;

/// `--show-storage-class` adds a STORAGE_CLASS column between SIZE and KEY.
/// JSON output's `StorageClass` field is driven by whether S3 returned a
/// non-None class, not by this flag, so the JSON sub-assertion here only
/// checks that the output parses cleanly with mandatory fields.
#[tokio::test]
async fn e2e_display_show_storage_class() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-storage-class",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "STORAGE_CLASS", "KEY"],
            "show-storage-class: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-storage-class: text on row count",
        );
        assert!(
            output.stdout.contains("file.txt"),
            "show-storage-class: key 'file.txt' missing from text output"
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-storage-class: text off header",
        );

        // Sub-assertion 3: JSON
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert!(
            v.get("Key").is_some(),
            "show-storage-class: Key missing from JSON"
        );
        assert!(
            v.get("Size").is_some(),
            "show-storage-class: Size missing from JSON"
        );
    });

    _guard.cleanup().await;
}

/// `--show-etag` adds an ETAG column between SIZE and KEY. The JSON output
/// always includes the `ETag` field for regular objects regardless of the
/// flag, so the JSON sub-assertion verifies the field is present.
#[tokio::test]
async fn e2e_display_show_etag() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header", "--show-etag"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "ETAG", "KEY"],
            "show-etag: text on header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 4, "show-etag: text on row count");

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-etag: text off header",
        );

        // Sub-assertion 3: JSON — ETag always present for regular objects
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert!(v.get("ETag").is_some(), "show-etag: ETag missing from JSON");
        assert!(v.get("Key").is_some(), "show-etag: Key missing from JSON");
    });

    _guard.cleanup().await;
}

/// `--show-checksum-algorithm` adds a CHECKSUM_ALGORITHM column. The test
/// uploads with an explicit CRC32 checksum so the column has a non-empty
/// value to assert. JSON output's `ChecksumAlgorithm` field is emitted
/// whenever the checksum_algorithm Vec is non-empty, so the JSON
/// sub-assertion verifies field presence and value.
#[tokio::test]
async fn e2e_display_show_checksum_algorithm() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object_with_checksum_algorithm(&bucket, "file.txt", vec![0u8; 100], "CRC32")
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-checksum-algorithm",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "CHECKSUM_ALGORITHM", "KEY"],
            "show-checksum-algorithm: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-checksum-algorithm: text on row count",
        );
        // Find the data row and check column index 2 (CHECKSUM_ALGORITHM)
        // contains "CRC32".
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            cols[2].contains("CRC32"),
            "show-checksum-algorithm: CHECKSUM_ALGORITHM column did not contain CRC32, got {:?}",
            cols[2]
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-checksum-algorithm: text off header",
        );

        // Sub-assertion 3: JSON — ChecksumAlgorithm field is emitted when non-empty
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        let algos = v
            .get("ChecksumAlgorithm")
            .and_then(|a| a.as_array())
            .expect("show-checksum-algorithm: ChecksumAlgorithm missing or not an array in JSON");
        assert!(
            algos.iter().any(|a| a.as_str() == Some("CRC32")),
            "show-checksum-algorithm: ChecksumAlgorithm array did not contain CRC32, got {algos:?}"
        );
    });

    _guard.cleanup().await;
}

/// `--show-checksum-type` adds a CHECKSUM_TYPE column. Same fixture
/// strategy as show_checksum_algorithm — upload with an explicit CRC32
/// checksum so S3 populates the ChecksumType field automatically.
#[tokio::test]
async fn e2e_display_show_checksum_type() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object_with_checksum_algorithm(&bucket, "file.txt", vec![0u8; 100], "CRC32")
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-checksum-type",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "CHECKSUM_TYPE", "KEY"],
            "show-checksum-type: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            4,
            "show-checksum-type: text on row count",
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-checksum-type: text off header",
        );

        // Sub-assertion 3: JSON — ChecksumType field is emitted when set
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert!(
            v.get("ChecksumType").is_some(),
            "show-checksum-type: ChecksumType missing from JSON"
        );
    });

    _guard.cleanup().await;
}
