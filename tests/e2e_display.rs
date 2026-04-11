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

/// `--show-is-latest` adds an IS_LATEST column (requires `--all-versions`).
/// Two versions of the same key guarantee at least one LATEST row and
/// one NOT_LATEST row. The JSON sub-assertion verifies `IsLatest` is
/// present under `--all-versions` regardless of the text-mode flag.
#[tokio::test]
async fn e2e_display_show_is_latest() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of doc.txt — v1 becomes NOT_LATEST, v2 becomes LATEST.
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
            "--show-is-latest",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "VERSION_ID", "IS_LATEST", "KEY"],
            "show-is-latest: text on header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 5, "show-is-latest: text on row count");
        assert!(
            output.stdout.contains("LATEST"),
            "show-is-latest: 'LATEST' token missing from text output"
        );
        assert!(
            output.stdout.contains("NOT_LATEST"),
            "show-is-latest: 'NOT_LATEST' token missing from text output"
        );

        // Sub-assertion 2: text with flag OFF (still with --all-versions)
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--all-versions", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "VERSION_ID", "KEY"],
            "show-is-latest: text off header",
        );

        // Sub-assertion 3: JSON — IsLatest present under --all-versions
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--all-versions", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert!(
            v.get("VersionId").is_some(),
            "show-is-latest: VersionId missing from JSON"
        );
        assert!(
            v.get("IsLatest").is_some(),
            "show-is-latest: IsLatest missing from JSON"
        );
    });

    _guard.cleanup().await;
}

/// `--show-owner` adds 2 columns (OWNER_DISPLAY_NAME, OWNER_ID). Under
/// non-versioned listing, this flag is the only way to populate owner
/// data — S3's ListObjectsV2 only returns owner when `fetch_owner=true`,
/// which `src/pipeline.rs:177` wires to `display_config.show_owner`.
///
/// This test uses non-versioned listing specifically so the JSON "Owner"
/// field absence/presence tracks the flag. Under --all-versions, S3
/// always returns owner regardless of the flag (see
/// src/storage/s3/mod.rs:174), so the JSON assertion would be
/// non-discriminating.
#[tokio::test]
async fn e2e_display_show_owner() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header", "--show-owner"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "OWNER_DISPLAY_NAME", "OWNER_ID", "KEY"],
            "show-owner: text on header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 5, "show-owner: text on row count");
        // Verify OWNER_ID cell (index 3) is non-empty for the data row.
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            !cols[3].is_empty(),
            "show-owner: OWNER_ID column is empty, expected non-empty owner ID"
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-owner: text off header",
        );

        // Sub-assertion 3: JSON without --show-owner — Owner field absent
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
            v.get("Owner").is_none(),
            "show-owner: Owner field present in JSON without --show-owner, got {:?}",
            v.get("Owner")
        );

        // Sub-assertion 4: JSON with --show-owner — Owner field present
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json", "--show-owner"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        let owner = v
            .get("Owner")
            .expect("show-owner: Owner field missing from JSON with --show-owner");
        assert!(
            owner
                .get("ID")
                .and_then(|id| id.as_str())
                .is_some_and(|s| !s.is_empty()),
            "show-owner: Owner.ID is empty or missing, got {owner:?}"
        );
    });

    _guard.cleanup().await;
}

/// `--show-restore-status` adds 2 columns (IS_RESTORE_IN_PROGRESS,
/// RESTORE_EXPIRY_DATE). For non-restored STANDARD objects, S3 doesn't
/// populate the restore fields even when `OptionalObjectAttributes=
/// RestoreStatus` is set, so the text cells are empty and the JSON
/// `RestoreStatus` field is absent in BOTH the flag-on and flag-off
/// JSON runs. This is a "flag is accepted, s3ls runs successfully,
/// field is correctly absent for non-Glacier objects" test rather
/// than a "flag populates the field" test — triggering a real Glacier
/// restore inside an e2e test would require Glacier-class storage
/// (90+ day billing) and lifecycle rules, which are out of scope.
#[tokio::test]
async fn e2e_display_show_restore_status() {
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
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "IS_RESTORE_IN_PROGRESS",
                "RESTORE_EXPIRY_DATE",
                "KEY",
            ],
            "show-restore-status: text on header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            5,
            "show-restore-status: text on row count",
        );
        // Verify both restore cells (indices 2 and 3) are empty for a
        // STANDARD object.
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            cols[2].is_empty(),
            "show-restore-status: IS_RESTORE_IN_PROGRESS should be empty for non-restored object, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "show-restore-status: RESTORE_EXPIRY_DATE should be empty for non-restored object, got {:?}",
            cols[3]
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-restore-status: text off header",
        );

        // Sub-assertion 3: JSON without --show-restore-status — RestoreStatus absent
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
            v.get("RestoreStatus").is_none(),
            "show-restore-status: RestoreStatus unexpectedly present in JSON without flag, got {:?}",
            v.get("RestoreStatus")
        );

        // Sub-assertion 4: JSON with --show-restore-status — still absent
        // for a STANDARD (non-restored) object. The flag is accepted,
        // s3ls runs successfully, and the field stays correctly absent.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert!(
            v.get("RestoreStatus").is_none(),
            "show-restore-status: RestoreStatus should be absent for non-Glacier object even with flag, got {:?}",
            v.get("RestoreStatus")
        );
    });

    _guard.cleanup().await;
}
