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

/// `--show-checksum-algorithm --show-checksum-type` on a CommonPrefix (PRE)
/// row: columns are present in the header but empty for PRE rows.
///
/// Covers `src/aggregate.rs:338-342` (CommonPrefix branch for
/// show_checksum_algorithm and show_checksum_type).
#[tokio::test]
async fn e2e_display_common_prefix_with_checksum_flags() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "top.txt", vec![0u8; 100]).await;
        helper
            .put_object(&bucket, "logs/deep.log", vec![0u8; 100])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "1",
            "--header",
            "--show-checksum-algorithm",
            "--show-checksum-type",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "CHECKSUM_ALGORITHM", "CHECKSUM_TYPE", "KEY"],
            "common-prefix checksum flags: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            5,
            "common-prefix checksum flags: row count",
        );

        // PRE row: checksum columns should be empty
        let pre_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "PRE"
            })
            .expect("common-prefix checksum flags: no PRE row found");
        let cols = parse_tsv_line(pre_row);
        assert!(
            cols[2].is_empty(),
            "common-prefix checksum flags: CHECKSUM_ALGORITHM should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "common-prefix checksum flags: CHECKSUM_TYPE should be empty, got {:?}",
            cols[3]
        );
    });

    _guard.cleanup().await;
}

/// `--show-restore-status` on a CommonPrefix (PRE) row: columns are present
/// in the header but empty for PRE rows.
///
/// Covers `src/aggregate.rs:358-359` (CommonPrefix branch for
/// show_restore_status).
#[tokio::test]
async fn e2e_display_common_prefix_with_restore_status() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "top.txt", vec![0u8; 100]).await;
        helper
            .put_object(&bucket, "logs/deep.log", vec![0u8; 100])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "1",
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
            "common-prefix restore-status: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            5,
            "common-prefix restore-status: row count",
        );

        // PRE row: restore-status columns should be empty
        let pre_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "PRE"
            })
            .expect("common-prefix restore-status: no PRE row found");
        let cols = parse_tsv_line(pre_row);
        assert!(
            cols[2].is_empty(),
            "common-prefix restore-status: IS_RESTORE_IN_PROGRESS should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "common-prefix restore-status: RESTORE_EXPIRY_DATE should be empty, got {:?}",
            cols[3]
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

/// `--show-relative-path` baseline test. At bucket root (no prefix),
/// the flag has no observable effect — keys are the same whether
/// rendered relative or not. This test exercises the flag's existence
/// and ensures it doesn't crash at bucket root. The prefixed-target
/// case is covered by `e2e_display_show_relative_path_prefixed`.
#[tokio::test]
async fn e2e_display_show_relative_path() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text with flag ON — baseline, 3-column header
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-relative-path: text on header (no column added)",
        );
        assert!(
            output.stdout.contains("file.txt"),
            "show-relative-path: key missing from text output"
        );

        // Sub-assertion 2: text with flag OFF
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &["DATE", "SIZE", "KEY"],
            "show-relative-path: text off header",
        );
        assert!(output.stdout.contains("file.txt"));

        // Sub-assertion 3: JSON with flag — Key field is "file.txt"
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert_eq!(
            v.get("Key").and_then(|k| k.as_str()),
            Some("file.txt"),
            "show-relative-path: Key field should be 'file.txt' at bucket root"
        );
    });

    _guard.cleanup().await;
}

/// `--show-relative-path` against a prefixed target. The key
/// `data/foo.txt` is uploaded, but the target is `s3://bucket/data/`,
/// so the flag should render the key as `foo.txt` (relative to the
/// prefix) rather than `data/foo.txt` (full key). Verified in both
/// text and JSON modes since `format_key_display` at
/// `src/aggregate.rs:394, 528` applies to both.
#[tokio::test]
async fn e2e_display_show_relative_path_prefixed() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object(&bucket, "data/foo.txt", vec![0u8; 100])
            .await;

        let target = format!("s3://{bucket}/data/");

        // Sub-assertion 1: text — KEY column is "foo.txt" (not "data/foo.txt")
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert_eq!(
            cols.last().copied(),
            Some("foo.txt"),
            "show-relative-path-prefixed: KEY column should be 'foo.txt' relative to data/, got {:?}",
            cols.last()
        );

        // Sub-assertion 2: JSON — Key field is "foo.txt"
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-relative-path",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("empty JSON output");
        let v: serde_json::Value =
            serde_json::from_str(first_line).expect("JSON line failed to parse");
        assert_eq!(
            v.get("Key").and_then(|k| k.as_str()),
            Some("foo.txt"),
            "show-relative-path-prefixed: Key should be 'foo.txt' relative to data/"
        );
    });

    _guard.cleanup().await;
}

/// Every object `--show-*` flag enabled at once. Verifies the full
/// 11-column header order and that every row has 11 columns. The
/// fixture uses put_object_with_checksum_algorithm so CHECKSUM_ALGORITHM
/// and CHECKSUM_TYPE cells are populated.
///
/// Does NOT include --all-versions or --show-is-latest (which would
/// need a versioned bucket) — the combo is specifically about the
/// column layout of the maximal non-versioned case.
#[tokio::test]
async fn e2e_display_all_show_flags_combined() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object_with_checksum_algorithm(&bucket, "file.txt", vec![0u8; 100], "CRC32")
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-storage-class",
            "--show-etag",
            "--show-checksum-algorithm",
            "--show-checksum-type",
            "--show-owner",
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "STORAGE_CLASS",
                "ETAG",
                "CHECKSUM_ALGORITHM",
                "CHECKSUM_TYPE",
                "OWNER_DISPLAY_NAME",
                "OWNER_ID",
                "IS_RESTORE_IN_PROGRESS",
                "RESTORE_EXPIRY_DATE",
                "KEY",
            ],
            "combo: header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 11, "combo: row count");

        // Spot-check: data row cells for CHECKSUM_ALGORITHM (index 4)
        // contain "CRC32" and OWNER_ID (index 7) is non-empty.
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row found");
        let cols = parse_tsv_line(data_line);
        assert!(
            cols[4].contains("CRC32"),
            "combo: CHECKSUM_ALGORITHM should contain CRC32, got {:?}",
            cols[4]
        );
        assert!(
            !cols[7].is_empty(),
            "combo: OWNER_ID should be non-empty, got {:?}",
            cols[7]
        );
    });

    _guard.cleanup().await;
}

/// Verifies that `CommonPrefix` rows (rendered as "PRE" in text mode)
/// correctly pad optional columns with empty cells. Uses `--max-depth 1`
/// on a fixture where some keys are at depth 2, so s3ls emits PRE
/// entries at the depth-1 boundary.
#[tokio::test]
async fn e2e_display_common_prefix_row() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // One top-level object, one deep object (collapses to `logs/` PRE
        // under --max-depth 1).
        helper.put_object(&bucket, "top.txt", vec![0u8; 100]).await;
        helper
            .put_object(&bucket, "logs/2025/a.log", vec![0u8; 100])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "1",
            "--header",
            "--show-etag",
            "--show-storage-class",
            "--show-owner",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Header: DATE, SIZE, STORAGE_CLASS, ETAG, OWNER_DISPLAY_NAME, OWNER_ID, KEY = 7 cols
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "STORAGE_CLASS",
                "ETAG",
                "OWNER_DISPLAY_NAME",
                "OWNER_ID",
                "KEY",
            ],
            "common-prefix-row: header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 7, "common-prefix-row: row count");

        // Find the PRE row (SIZE column contains "PRE") and verify optional
        // columns are empty.
        let pre_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "PRE"
            })
            .expect("common-prefix-row: no PRE row found");
        let cols = parse_tsv_line(pre_row);
        assert!(
            cols[0].is_empty(),
            "common-prefix-row: PRE row DATE should be empty, got {:?}",
            cols[0]
        );
        assert_eq!(cols[1], "PRE");
        assert!(
            cols[2].is_empty(),
            "common-prefix-row: PRE row STORAGE_CLASS should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "common-prefix-row: PRE row ETAG should be empty, got {:?}",
            cols[3]
        );
        assert!(
            cols[4].is_empty(),
            "common-prefix-row: PRE row OWNER_DISPLAY_NAME should be empty"
        );
        assert!(
            cols[5].is_empty(),
            "common-prefix-row: PRE row OWNER_ID should be empty"
        );
        assert_eq!(
            cols[6], "logs/",
            "common-prefix-row: PRE row KEY should be 'logs/', got {:?}",
            cols[6]
        );

        // Find the object row (SIZE column is numeric, not "PRE") and
        // verify some optional cells are populated.
        let obj_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] != "PRE" && cols[1] != "SIZE"
            })
            .expect("common-prefix-row: no object row found");
        let cols = parse_tsv_line(obj_row);
        assert_eq!(cols[6], "top.txt");
        assert!(
            !cols[5].is_empty(),
            "object row OWNER_ID should be populated"
        );
    });

    _guard.cleanup().await;
}

/// Verifies that `DeleteMarker` rows (rendered as "DELETE" in text mode)
/// correctly pad optional columns with empty cells for non-version
/// columns, populate VERSION_ID and KEY, and populate Owner (since
/// ListObjectVersions always returns owner per src/storage/s3/mod.rs:174).
#[tokio::test]
async fn e2e_display_delete_marker_row() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
            "--show-etag",
            "--show-storage-class",
            "--show-owner",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Header: DATE, SIZE, STORAGE_CLASS, ETAG, VERSION_ID, OWNER_DISPLAY_NAME, OWNER_ID, KEY = 8 cols
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "STORAGE_CLASS",
                "ETAG",
                "VERSION_ID",
                "OWNER_DISPLAY_NAME",
                "OWNER_ID",
                "KEY",
            ],
            "delete-marker-row: header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 8, "delete-marker-row: row count");

        // Find the DELETE row and verify optional columns.
        let dm_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "DELETE"
            })
            .expect("delete-marker-row: no DELETE row found");
        let cols = parse_tsv_line(dm_row);
        assert!(
            !cols[0].is_empty(),
            "delete-marker-row: DELETE row DATE should be populated, got {:?}",
            cols[0]
        );
        assert_eq!(cols[1], "DELETE");
        assert!(
            cols[2].is_empty(),
            "delete-marker-row: DELETE row STORAGE_CLASS should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "delete-marker-row: DELETE row ETAG should be empty, got {:?}",
            cols[3]
        );
        assert!(
            !cols[4].is_empty(),
            "delete-marker-row: DELETE row VERSION_ID should be populated, got {:?}",
            cols[4]
        );
        assert_eq!(cols[7], "doc.txt");
    });

    _guard.cleanup().await;
}

/// Verifies that DeleteMarker text rows correctly pad
/// `--show-checksum-algorithm --show-checksum-type` columns as empty.
///
/// Covers `src/aggregate.rs:419-423` (DeleteMarker branch for
/// show_checksum_algorithm and show_checksum_type).
#[tokio::test]
async fn e2e_display_delete_marker_with_checksum_flags() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
            "--show-checksum-algorithm",
            "--show-checksum-type",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "CHECKSUM_ALGORITHM",
                "CHECKSUM_TYPE",
                "VERSION_ID",
                "KEY",
            ],
            "delete-marker checksum flags: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            6,
            "delete-marker checksum flags: row count",
        );

        // Find the DELETE row and verify checksum columns are empty.
        let dm_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "DELETE"
            })
            .expect("delete-marker checksum flags: no DELETE row found");
        let cols = parse_tsv_line(dm_row);
        assert!(
            cols[2].is_empty(),
            "delete-marker checksum flags: CHECKSUM_ALGORITHM should be empty, got {:?}",
            cols[2]
        );
        assert!(
            cols[3].is_empty(),
            "delete-marker checksum flags: CHECKSUM_TYPE should be empty, got {:?}",
            cols[3]
        );
        assert_eq!(cols[5], "doc.txt");
    });

    _guard.cleanup().await;
}

/// Verifies that DeleteMarker text rows with `--show-is-latest` show
/// `LATEST` or `NOT_LATEST` correctly. Two versions of doc.txt + 1
/// delete marker (which is the latest). This also exercises the
/// NOT_LATEST path for both an older object version and verifies a DM
/// can be LATEST.
///
/// Covers `src/aggregate.rs:427-430` (DeleteMarker branch for
/// show_is_latest, both LATEST and NOT_LATEST).
#[tokio::test]
async fn e2e_display_delete_marker_with_is_latest() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

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
            "delete-marker is-latest: header",
        );
        assert_all_data_rows_have_columns(&output.stdout, 5, "delete-marker is-latest: row count");

        // Find the DELETE row: its IS_LATEST should be "LATEST" since
        // it was created after the two object versions.
        let dm_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "DELETE"
            })
            .expect("delete-marker is-latest: no DELETE row found");
        let cols = parse_tsv_line(dm_row);
        assert_eq!(
            cols[3], "LATEST",
            "delete-marker is-latest: DM should be LATEST, got {:?}",
            cols[3]
        );

        // Verify NOT_LATEST appears for object versions (they are no longer latest)
        let not_latest_count = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 4 && cols[3] == "NOT_LATEST"
            })
            .count();
        assert_eq!(
            not_latest_count, 2,
            "delete-marker is-latest: expected 2 NOT_LATEST rows (both object versions), got {not_latest_count}"
        );
    });

    _guard.cleanup().await;
}

/// Verifies that DeleteMarker text rows pad `--show-restore-status`
/// columns as empty.
///
/// Covers `src/aggregate.rs:439-442` (DeleteMarker branch for
/// show_restore_status — empty columns since DMs have no restore status).
#[tokio::test]
async fn e2e_display_delete_marker_with_restore_status() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--header",
            "--show-restore-status",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_header_columns(
            &output.stdout,
            &[
                "DATE",
                "SIZE",
                "VERSION_ID",
                "IS_RESTORE_IN_PROGRESS",
                "RESTORE_EXPIRY_DATE",
                "KEY",
            ],
            "delete-marker restore-status: header",
        );
        assert_all_data_rows_have_columns(
            &output.stdout,
            6,
            "delete-marker restore-status: row count",
        );

        // Find the DELETE row and verify restore-status columns are empty.
        let dm_row = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .find(|l| {
                let cols = parse_tsv_line(l);
                cols.len() >= 2 && cols[1] == "DELETE"
            })
            .expect("delete-marker restore-status: no DELETE row found");
        let cols = parse_tsv_line(dm_row);
        assert!(
            cols[3].is_empty(),
            "delete-marker restore-status: IS_RESTORE_IN_PROGRESS should be empty, got {:?}",
            cols[3]
        );
        assert!(
            cols[4].is_empty(),
            "delete-marker restore-status: RESTORE_EXPIRY_DATE should be empty, got {:?}",
            cols[4]
        );
        assert_eq!(cols[5], "doc.txt");
    });

    _guard.cleanup().await;
}

/// Verifies `--summarize` appends a summary line in text mode (with and
/// without `--human-readable`) and a JSON summary object in JSON mode.
/// Fixture is 3 objects × 1000 bytes each = 3000 bytes total.
#[tokio::test]
async fn e2e_display_summarize_objects() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "a.bin", vec![0u8; 1000]).await;
        helper.put_object(&bucket, "b.bin", vec![0u8; 1000]).await;
        helper.put_object(&bucket, "c.bin", vec![0u8; 1000]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text, no human-readable
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--summarize"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(&output.stdout, "summarize: text no-human");
        assert!(
            summary.contains("\t3\tobjects"),
            "summarize text: expected 3 objects in summary, got {summary:?}"
        );
        assert!(
            summary.contains("\t3000\tbytes"),
            "summarize text: expected 3000 bytes, got {summary:?}"
        );

        // Sub-assertion 2: text, human-readable
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--summarize",
            "--human-readable",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(&output.stdout, "summarize: text human");
        assert!(
            summary.contains("\t3\tobjects"),
            "summarize human: expected 3 objects in summary, got {summary:?}"
        );
        // Human-readable form should NOT contain "3000\tbytes" (that's
        // the non-human form).
        assert!(
            !summary.contains("\t3000\tbytes"),
            "summarize human: summary should not have '3000\\tbytes', got {summary:?}"
        );
        // And should contain some unit other than "bytes" (KiB, KB, etc.
        // depending on byte-unit formatting).
        assert!(
            summary.contains("KiB") || summary.contains("KB"),
            "summarize human: expected KiB/KB unit in summary, got {summary:?}"
        );

        // Sub-assertion 3: JSON
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--summarize", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let v = assert_summary_present_json(&output.stdout, "summarize: json");
        let summary_obj = v
            .get("Summary")
            .expect("summarize json: missing Summary object");
        assert_eq!(
            summary_obj.get("TotalObjects").and_then(|n| n.as_u64()),
            Some(3),
            "summarize json: TotalObjects should be 3"
        );
        assert_eq!(
            summary_obj.get("TotalSize").and_then(|n| n.as_u64()),
            Some(3000),
            "summarize json: TotalSize should be 3000"
        );
    });

    _guard.cleanup().await;
}

/// Verifies `--summarize --all-versions` appends the delete-markers
/// count to the summary line. Fixture is a versioned bucket with 2
/// versions of doc.txt (100 + 200 bytes) and 1 delete marker.
#[tokio::test]
async fn e2e_display_summarize_versioned() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--summarize",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(&output.stdout, "summarize versioned");
        // 2 live object versions (100 + 200 = 300 bytes)
        assert!(
            summary.contains("\t2\tobjects"),
            "summarize versioned: expected 2 objects, got {summary:?}"
        );
        assert!(
            summary.contains("\t300\tbytes"),
            "summarize versioned: expected 300 bytes, got {summary:?}"
        );
        // 1 delete marker
        assert!(
            summary.contains("\t1\tdelete markers"),
            "summarize versioned: expected 1 delete markers, got {summary:?}"
        );
    });

    _guard.cleanup().await;
}

/// Verifies `--summarize --human-readable` with a total size below 1024
/// bytes renders as raw bytes in the summary line (the `format_size_split`
/// path where `size < 1024` returns `(size, "bytes")`).
///
/// Covers `src/aggregate.rs:264-265` (format_size_split size < 1024 branch).
#[tokio::test]
async fn e2e_display_summarize_human_small_total() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // 3 objects × 100 bytes = 300 bytes total (< 1024)
        helper.put_object(&bucket, "a.bin", vec![0u8; 100]).await;
        helper.put_object(&bucket, "b.bin", vec![0u8; 100]).await;
        helper.put_object(&bucket, "c.bin", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--summarize",
            "--human-readable",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary = assert_summary_present_text(&output.stdout, "summarize human small total");
        assert!(
            summary.contains("\t3\tobjects"),
            "summarize human small: expected 3 objects, got {summary:?}"
        );
        // 300 bytes < 1024 — format_size_split returns "300" + "bytes" even
        // in human-readable mode.
        assert!(
            summary.contains("\t300\tbytes"),
            "summarize human small: expected '300\\tbytes' (< 1024 stays as bytes), got {summary:?}"
        );
    });

    _guard.cleanup().await;
}

/// Verifies `--human-readable` renders object row sizes in human form.
/// Fixture is a 2048-byte object so the expected rendering is "2.0KiB"
/// (2048 / 1024 = 2.0, binary units, no space between number and unit
/// per `src/aggregate.rs:284-285` which strips the space via replacen).
#[tokio::test]
async fn e2e_display_human_readable() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper
            .put_object(&bucket, "file.txt", vec![0u8; 2048])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--human-readable"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert!(
            output.stdout.contains("file.txt"),
            "human-readable: key missing from output"
        );
        // 2048 bytes = 2.0 KiB. format_size at src/aggregate.rs:284-285
        // uses `{adjusted:.1}` and strips the space, yielding "2.0KiB".
        // Accept "2.00KiB" defensively in case byte-unit's precision
        // changes in a future version.
        assert!(
            output.stdout.contains("2.0KiB") || output.stdout.contains("2.00KiB"),
            "human-readable: expected '2.0KiB' in output, got:\n{}",
            output.stdout
        );
        // And verify the non-human form is NOT there (no "2048" as a size).
        assert!(
            !output.stdout.contains("\t2048\t"),
            "human-readable: unexpected '\\t2048\\t' in output (should be rendered as KiB):\n{}",
            output.stdout
        );
    });

    _guard.cleanup().await;
}

/// Bucket listing `--show-bucket-arn` — adds a BUCKET_ARN column in text
/// mode and a `BucketArn` field in JSON mode. Assertions are scoped to
/// the test bucket's unique name because the account may have other
/// buckets.
#[tokio::test]
async fn e2e_display_bucket_listing_show_bucket_arn() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sub-assertion 1: text with flag ON — header contains BUCKET_ARN
        let output = TestHelper::run_s3ls(&["--header", "--show-bucket-arn"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing text on: empty stdout");
        assert!(
            header_line.contains("BUCKET_ARN"),
            "bucket listing show-bucket-arn text on: header missing BUCKET_ARN, got {header_line:?}"
        );
        // Find our bucket's row and verify the ARN cell is non-empty.
        let bucket_row = output
            .stdout
            .lines()
            .find(|l| l.contains(&bucket))
            .unwrap_or_else(|| {
                panic!("bucket listing text on: test bucket {bucket} not found in output")
            });
        let cols = parse_tsv_line(bucket_row);
        // Header is DATE\tREGION\tBUCKET\tBUCKET_ARN[\tOWNER...], so
        // BUCKET_ARN is column index 3.
        assert!(
            cols.len() >= 4 && !cols[3].is_empty(),
            "bucket listing show-bucket-arn: expected non-empty BUCKET_ARN cell, got row {bucket_row:?}"
        );

        // Sub-assertion 2: text with flag OFF — header does NOT contain BUCKET_ARN
        let output = TestHelper::run_s3ls(&["--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing text off: empty stdout");
        assert!(
            !header_line.contains("BUCKET_ARN"),
            "bucket listing show-bucket-arn text off: header unexpectedly contains BUCKET_ARN, got {header_line:?}"
        );

        // Sub-assertion 3: JSON with flag ON — BucketArn field present
        let output = TestHelper::run_s3ls(&["--json", "--show-bucket-arn"]);
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
            .unwrap_or_else(|| {
                panic!("bucket listing json on: test bucket {bucket} not found in JSON output")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("BucketArn").is_some(),
            "bucket listing show-bucket-arn json on: BucketArn field missing, got {v:?}"
        );

        // Sub-assertion 4: JSON without flag — BucketArn field absent
        let output = TestHelper::run_s3ls(&["--json"]);
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
            .unwrap_or_else(|| {
                panic!("bucket listing json off: test bucket {bucket} not found in JSON output")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("BucketArn").is_none(),
            "bucket listing show-bucket-arn json off: BucketArn should be absent, got {:?}",
            v.get("BucketArn")
        );
    });

    _guard.cleanup().await;
}

/// Bucket listing `--show-owner` — adds OWNER_DISPLAY_NAME and OWNER_ID
/// columns in text mode and an `Owner` object in JSON mode.
#[tokio::test]
async fn e2e_display_bucket_listing_show_owner() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sub-assertion 1: text with flag ON — header contains OWNER_ID
        let output = TestHelper::run_s3ls(&["--header", "--show-owner"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing show-owner text on: empty stdout");
        assert!(
            header_line.contains("OWNER_ID"),
            "bucket listing show-owner text on: header missing OWNER_ID, got {header_line:?}"
        );

        // Sub-assertion 2: text with flag OFF — header does NOT contain OWNER_ID
        let output = TestHelper::run_s3ls(&["--header"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let header_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("bucket listing show-owner text off: empty stdout");
        assert!(
            !header_line.contains("OWNER_ID"),
            "bucket listing show-owner text off: header unexpectedly contains OWNER_ID, got {header_line:?}"
        );

        // Sub-assertion 3: JSON with flag ON — Owner field present for our bucket
        let output = TestHelper::run_s3ls(&["--json", "--show-owner"]);
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
            .unwrap_or_else(|| {
                panic!("bucket listing show-owner json on: test bucket {bucket} not found")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("Owner").is_some(),
            "bucket listing show-owner json on: Owner field missing, got {v:?}"
        );

        // Sub-assertion 4: JSON without flag — Owner field absent
        let output = TestHelper::run_s3ls(&["--json"]);
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
            .unwrap_or_else(|| {
                panic!("bucket listing show-owner json off: test bucket {bucket} not found")
            });
        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        assert!(
            v.get("Owner").is_none(),
            "bucket listing show-owner json off: Owner should be absent, got {:?}",
            v.get("Owner")
        );
    });

    _guard.cleanup().await;
}

/// `--show-objects-only` hides CommonPrefix (PRE) rows from both text and
/// JSON output. By default (without the flag), prefixes appear normally.
#[tokio::test]
async fn e2e_display_show_objects_only() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // Create objects under a sub-prefix so a non-recursive listing
        // produces both a CommonPrefix ("subdir/") and a top-level object.
        helper.put_object(&bucket, "top.txt", vec![0u8; 50]).await;
        helper
            .put_object(&bucket, "subdir/nested.txt", vec![0u8; 50])
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text WITHOUT --show-objects-only shows PRE
        let output = TestHelper::run_s3ls(&[target.as_str()]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert!(
            output.stdout.contains("PRE"),
            "show-objects-only off: PRE should be present in text output"
        );
        assert!(
            output.stdout.contains("top.txt"),
            "show-objects-only off: top.txt should be present"
        );

        // Sub-assertion 2: text WITH --show-objects-only hides PRE
        let output = TestHelper::run_s3ls(&[target.as_str(), "--show-objects-only"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert!(
            !output.stdout.contains("PRE"),
            "show-objects-only on: PRE should NOT be in text output, got:\n{}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("subdir/"),
            "show-objects-only on: subdir/ prefix should NOT appear, got:\n{}",
            output.stdout
        );
        assert!(
            output.stdout.contains("top.txt"),
            "show-objects-only on: top.txt should still be present"
        );

        // Sub-assertion 3: JSON WITHOUT --show-objects-only shows Prefix
        let output = TestHelper::run_s3ls(&[target.as_str(), "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert!(
            output.stdout.contains("\"Prefix\""),
            "show-objects-only off json: Prefix field should be present"
        );

        // Sub-assertion 4: JSON WITH --show-objects-only hides Prefix
        let output = TestHelper::run_s3ls(&[target.as_str(), "--json", "--show-objects-only"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert!(
            !output.stdout.contains("\"Prefix\""),
            "show-objects-only on json: Prefix field should NOT appear, got:\n{}",
            output.stdout
        );
        assert!(
            output.stdout.contains("top.txt"),
            "show-objects-only on json: top.txt should still be present"
        );
    });

    _guard.cleanup().await;
}

/// `--show-local-time` displays timestamps with a numeric UTC offset instead
/// of the trailing "Z" in both text and JSON output. Without the flag,
/// timestamps use UTC with "Z" suffix.
#[tokio::test]
async fn e2e_display_show_local_time() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "file.txt", vec![0u8; 50]).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text WITHOUT --show-local-time uses UTC (Z suffix)
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_data_line = output
            .stdout
            .lines()
            .find(|l| l.contains("file.txt"))
            .expect("file.txt not in output");
        let date_field = first_data_line.split('\t').next().unwrap();
        assert!(
            date_field.ends_with('Z'),
            "default text should end with Z, got: {date_field}"
        );

        // Sub-assertion 2: text WITH --show-local-time uses numeric offset
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--show-local-time"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_data_line = output
            .stdout
            .lines()
            .find(|l| l.contains("file.txt"))
            .expect("file.txt not in output");
        let date_field = first_data_line.split('\t').next().unwrap();
        assert!(
            !date_field.ends_with('Z'),
            "local time text should NOT end with Z, got: {date_field}"
        );
        // Should contain a numeric offset like +00:00 or +09:00
        assert!(
            date_field.contains('+') || date_field.contains("-0") || date_field.contains("-1"),
            "local time should have a numeric offset, got: {date_field}"
        );

        // Sub-assertion 3: JSON WITHOUT --show-local-time
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let json_line = output
            .stdout
            .lines()
            .find(|l| l.contains("file.txt"))
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(json_line).unwrap();
        let last_modified = v["LastModified"].as_str().unwrap();
        assert!(
            last_modified.ends_with('Z') || last_modified.contains("+00:00"),
            "default JSON should be UTC, got: {last_modified}"
        );

        // Sub-assertion 4: JSON WITH --show-local-time
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--show-local-time",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let json_line = output
            .stdout
            .lines()
            .find(|l| l.contains("file.txt"))
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(json_line).unwrap();
        let last_modified = v["LastModified"].as_str().unwrap();
        assert!(
            !last_modified.ends_with('Z'),
            "local time JSON should NOT end with Z, got: {last_modified}"
        );
    });

    _guard.cleanup().await;
}

/// Bucket listing `--show-owner` JSON: verify `Owner.DisplayName` is
/// emitted in the JSON when the S3 account returns it.
///
/// Covers `src/bucket_lister.rs:105-109` (Owner.DisplayName insertion)
/// and `src/bucket_lister.rs:114-115` (Owner object insertion into map).
#[tokio::test]
async fn e2e_display_bucket_listing_show_owner_json_owner_fields() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&["--json", "--show-owner"]);
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
            .unwrap_or_else(|| {
                panic!("bucket owner json: test bucket {bucket} not found in output")
            });

        let v: serde_json::Value = serde_json::from_str(bucket_line).unwrap();
        let owner = v.get("Owner");

        // The S3 ListBuckets API always returns at least an Owner.ID for the
        // account. Owner.DisplayName may or may not be present depending on
        // account configuration. Verify the Owner object exists and has at
        // least one field.
        assert!(
            owner.is_some(),
            "bucket owner json: Owner should be present with --show-owner, got {v:?}"
        );
        let owner = owner.unwrap();
        let has_id = owner.get("ID").and_then(|v| v.as_str()).is_some();
        let has_name = owner.get("DisplayName").and_then(|v| v.as_str()).is_some();
        assert!(
            has_id || has_name,
            "bucket owner json: Owner should have at least ID or DisplayName, got {owner:?}"
        );
    });

    _guard.cleanup().await;
}

/// Bucket listing text mode with `--raw-output`: verifies that the
/// text-mode escape function chooses the raw (no-escape) path.
///
/// Covers `src/bucket_lister.rs:124-125` (raw_output branch of the
/// escape closure).
#[tokio::test]
async fn e2e_display_bucket_listing_text_raw_output() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&["--raw-output"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // The test bucket must appear in the output. The --raw-output flag
        // disables control-char escaping (bucket names are all clean ASCII
        // anyway, so the output is identical to the default — but the code
        // path through the raw_output branch is exercised).
        assert!(
            output.stdout.contains(&bucket),
            "bucket listing raw-output: test bucket {bucket} not found in output"
        );
    });

    _guard.cleanup().await;
}

/// `--aligned` produces fixed-width space-separated columns so that KEY
/// starts at the same character position on every object row.
///
/// The default column layout (DATE, SIZE, KEY) gives a prefix of
/// `W_DATE + SEP + W_SIZE + SEP` = 25 + 2 + 20 + 2 = 49 characters
/// before the KEY value on every data row. This test verifies that
/// prefix length is identical across all rows.
#[tokio::test]
async fn e2e_aligned_object_listing() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // Three objects with different key lengths and different sizes so
        // the SIZE column needs right-padding on all but the widest.
        helper.put_object(&bucket, "a.txt", vec![0u8; 1]).await;
        helper
            .put_object(&bucket, "longer-name.txt", vec![0u8; 1000])
            .await;
        helper
            .put_object(&bucket, "dir/nested.txt", vec![0u8; 123456])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--aligned"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Collect non-empty, non-summary data rows.
        let data_rows: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| {
                !l.trim().is_empty() && !l.starts_with("\nTotal:") && !l.starts_with("Total:")
            })
            .collect();

        assert!(
            !data_rows.is_empty(),
            "aligned object listing: no data rows in output:\n{}",
            output.stdout
        );

        // In aligned mode every row is space-separated, not tab-separated.
        // Verify no tabs appear in any data row.
        for row in &data_rows {
            assert!(
                !row.contains('\t'),
                "aligned object listing: row contains tab (aligned mode must use spaces): {row:?}"
            );
        }

        // The KEY value begins at byte offset
        //   prefix_len = W_DATE + SEP + W_SIZE + SEP
        // and the two SEP slots sit at [W_DATE..W_DATE+SEP.len()] and
        // [W_DATE+SEP.len()+W_SIZE..prefix_len]. Derive all positions
        // from the module constants so the assertions track any future
        // width adjustment automatically.
        use s3ls_rs::display::aligned::{SEP, W_DATE, W_SIZE};
        let sep_len = SEP.len();
        let date_sep_start = W_DATE;
        let date_sep_end = date_sep_start + sep_len;
        let size_sep_start = date_sep_end + W_SIZE;
        let size_sep_end = size_sep_start + sep_len;
        let prefix_len = size_sep_end;
        for row in &data_rows {
            assert!(
                row.len() >= prefix_len,
                "aligned object listing: row shorter than expected prefix of {prefix_len}: {row:?}"
            );
            let row_prefix = &row[..prefix_len];
            assert!(
                !row_prefix.contains('\t'),
                "aligned object listing: tab found in column prefix area: {row:?}"
            );
            assert_eq!(
                &row[date_sep_start..date_sep_end],
                SEP,
                "aligned object listing: SEP missing after DATE column in row: {row:?}"
            );
            assert_eq!(
                &row[size_sep_start..size_sep_end],
                SEP,
                "aligned object listing: SEP missing after SIZE column in row: {row:?}"
            );
        }

        // Every row's KEY value (the part after the fixed prefix) must be
        // one of the uploaded keys.
        let expected_keys = ["a.txt", "longer-name.txt", "dir/nested.txt"];
        for row in &data_rows {
            let key_part = &row[prefix_len..];
            assert!(
                expected_keys.iter().any(|k| key_part == *k),
                "aligned object listing: unexpected KEY value {key_part:?} in row: {row:?}"
            );
        }

        // All three keys must appear.
        for key in &expected_keys {
            assert!(
                output.stdout.contains(key),
                "aligned object listing: key {key:?} missing from output"
            );
        }
    });

    _guard.cleanup().await;
}

/// `--recursive --aligned --no-sort` exits successfully, emits at least
/// one row, and every row still follows the fixed-width aligned layout
/// (alignment must not depend on pre-sorting or buffering).
#[tokio::test]
async fn e2e_aligned_with_no_sort() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "alpha.txt", vec![0u8; 10]).await;
        helper
            .put_object(&bucket, "beta/gamma.txt", vec![0u8; 20])
            .await;
        helper.put_object(&bucket, "zeta.txt", vec![0u8; 30]).await;

        let target = format!("s3://{bucket}/");

        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--aligned", "--no-sort"]);
        assert!(
            output.status.success(),
            "aligned --no-sort: s3ls failed: {}",
            output.stderr
        );

        let data_rows: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| {
                !l.trim().is_empty() && !l.starts_with("\nTotal:") && !l.starts_with("Total:")
            })
            .collect();

        assert!(
            !data_rows.is_empty(),
            "aligned --no-sort: no data rows emitted:\n{}",
            output.stdout
        );

        // Every row must be space-separated (no tabs) and must have the
        // aligned prefix structure: W_DATE + SEP + W_SIZE + SEP.
        use s3ls_rs::display::aligned::{SEP, W_DATE, W_SIZE};
        let sep_len = SEP.len();
        let date_sep_start = W_DATE;
        let date_sep_end = date_sep_start + sep_len;
        let size_sep_start = date_sep_end + W_SIZE;
        let size_sep_end = size_sep_start + sep_len;
        let prefix_len = size_sep_end;
        for row in &data_rows {
            assert!(
                !row.contains('\t'),
                "aligned --no-sort: row contains tab: {row:?}"
            );
            assert!(
                row.len() >= prefix_len,
                "aligned --no-sort: row shorter than fixed prefix ({prefix_len}): {row:?}"
            );
            assert_eq!(
                &row[date_sep_start..date_sep_end],
                SEP,
                "aligned --no-sort: SEP missing after DATE in row: {row:?}"
            );
            assert_eq!(
                &row[size_sep_start..size_sep_end],
                SEP,
                "aligned --no-sort: SEP missing after SIZE in row: {row:?}"
            );
        }
    });

    _guard.cleanup().await;
}

/// `--recursive --aligned --human-readable --summarize` exits
/// successfully, data rows follow the human-readable aligned layout
/// (W_SIZE_HUMAN=9 instead of W_SIZE=20), and the summary line uses
/// spaces (not tabs) as separators.
#[tokio::test]
async fn e2e_aligned_with_human_and_summary() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        // Three objects × 1000 bytes each = 3000 bytes total.
        helper.put_object(&bucket, "x.bin", vec![0u8; 1000]).await;
        helper.put_object(&bucket, "y.bin", vec![0u8; 1000]).await;
        helper.put_object(&bucket, "z.bin", vec![0u8; 1000]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--aligned",
            "--human-readable",
            "--summarize",
        ]);
        assert!(
            output.status.success(),
            "aligned human+summary: s3ls failed: {}",
            output.stderr
        );

        // Data rows: W_DATE + SEP + W_SIZE_HUMAN + SEP before the KEY value.
        use s3ls_rs::display::aligned::{SEP, W_DATE, W_SIZE_HUMAN};
        let sep_len = SEP.len();
        let date_sep_start = W_DATE;
        let date_sep_end = date_sep_start + sep_len;
        let size_sep_start = date_sep_end + W_SIZE_HUMAN;
        let size_sep_end = size_sep_start + sep_len;
        let prefix_len = size_sep_end;
        let data_rows: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| {
                !l.trim().is_empty() && !l.starts_with("\nTotal:") && !l.starts_with("Total:")
            })
            .collect();

        assert!(
            !data_rows.is_empty(),
            "aligned human+summary: no data rows:\n{}",
            output.stdout
        );

        for row in &data_rows {
            assert!(
                !row.contains('\t'),
                "aligned human+summary: row contains tab: {row:?}"
            );
            assert!(
                row.len() >= prefix_len,
                "aligned human+summary: row shorter than prefix ({prefix_len}): {row:?}"
            );
            assert_eq!(
                &row[date_sep_start..date_sep_end],
                SEP,
                "aligned human+summary: SEP missing after DATE in row: {row:?}"
            );
            assert_eq!(
                &row[size_sep_start..size_sep_end],
                SEP,
                "aligned human+summary: SEP missing after SIZE in row: {row:?}"
            );
        }

        // Summary line: format is "\nTotal: {count} objects {size} {unit}"
        // In aligned mode the separator is a single space, not a tab.
        let summary_line = output
            .stdout
            .lines()
            .find(|l| l.starts_with("Total:"))
            .unwrap_or_else(|| {
                panic!(
                    "aligned human+summary: no 'Total:' summary line in output:\n{}",
                    output.stdout
                )
            });
        assert!(
            !summary_line.contains('\t'),
            "aligned human+summary: summary line contains tab (should use spaces): {summary_line:?}"
        );
        assert!(
            summary_line.contains("3") && summary_line.contains("objects"),
            "aligned human+summary: expected '3 objects' in summary, got: {summary_line:?}"
        );
    });

    _guard.cleanup().await;
}

/// `--all-versions --aligned --header` with every `--show-*` flag enabled.
///
/// Verifies that when all 12 non-KEY columns are present the byte offsets
/// computed from the width constants in `s3ls_rs::display::aligned` match
/// the actual character positions in the header and data rows.  This
/// catches any future column-width change or column-order change that
/// would silently break alignment.
///
/// A versioned bucket is required so that `--show-is-latest` (which
/// requires `--all-versions`) has real version metadata to render.
#[tokio::test]
async fn e2e_aligned_all_columns() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        // Versioned bucket so VERSION_ID / IS_LATEST columns carry real data.
        helper.create_versioned_bucket(&bucket).await;
        helper
            .put_object(&bucket, "test.txt", b"hello world".to_vec())
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--all-versions",
            "--aligned",
            "--header",
            "--show-storage-class",
            "--show-etag",
            "--show-checksum-algorithm",
            "--show-checksum-type",
            "--show-is-latest",
            "--show-owner",
            "--show-restore-status",
        ]);
        assert!(
            output.status.success(),
            "aligned all-columns: s3ls failed: {}",
            output.stderr
        );

        // Compute the expected prefix length from the width constants —
        // the same arithmetic the unit test `format_text_aligned_with_all_optional_columns`
        // uses in src/display/aligned_formatter.rs.
        use s3ls_rs::display::aligned::{
            SEP, W_CHECKSUM_ALGORITHM, W_CHECKSUM_TYPE, W_DATE, W_ETAG, W_IS_LATEST,
            W_IS_RESTORE_IN_PROGRESS, W_OWNER_DISPLAY_NAME, W_OWNER_ID, W_RESTORE_EXPIRY_DATE,
            W_SIZE, W_STORAGE_CLASS, W_VERSION_ID,
        };
        let sep = SEP.len();
        // 12 non-KEY columns, each followed by SEP; KEY itself is unpadded at the end.
        let expected_prefix_len = W_DATE
            + sep
            + W_SIZE
            + sep
            + W_STORAGE_CLASS
            + sep
            + W_ETAG
            + sep
            + W_CHECKSUM_ALGORITHM
            + sep
            + W_CHECKSUM_TYPE
            + sep
            + W_VERSION_ID
            + sep
            + W_IS_LATEST
            + sep
            + W_OWNER_DISPLAY_NAME
            + sep
            + W_OWNER_ID
            + sep
            + W_IS_RESTORE_IN_PROGRESS
            + sep
            + W_RESTORE_EXPIRY_DATE
            + sep;

        // Collect all non-empty lines.
        let all_lines: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();

        // Must have at least a header line and one data line.
        assert!(
            all_lines.len() >= 2,
            "aligned all-columns: expected at least 2 lines (header + data), got {}:\n{}",
            all_lines.len(),
            output.stdout
        );

        // Header line: starts with "DATE", ends with "KEY", correct total length.
        let header = all_lines[0];
        assert!(
            header.starts_with("DATE"),
            "aligned all-columns: header should start with 'DATE', got: {header:?}"
        );
        let expected_header_len = expected_prefix_len + "KEY".len();
        assert_eq!(
            header.len(),
            expected_header_len,
            "aligned all-columns: header length mismatch (expected {expected_header_len}, got {}): {header:?}",
            header.len()
        );
        assert!(
            header.ends_with("KEY"),
            "aligned all-columns: header should end with 'KEY', got: {header:?}"
        );

        // No row should contain a tab — aligned mode uses spaces only.
        for line in &all_lines {
            assert!(
                !line.contains('\t'),
                "aligned all-columns: row contains tab (aligned mode must not use tabs): {line:?}"
            );
        }

        // Data rows (all lines except the header): KEY starts at expected_prefix_len.
        let data_rows: Vec<&str> = all_lines
            .iter()
            .copied()
            .filter(|l| !l.starts_with("DATE"))
            .filter(|l| !l.starts_with("Total:"))
            .collect();
        assert!(
            !data_rows.is_empty(),
            "aligned all-columns: no data rows found in output:\n{}",
            output.stdout
        );
        for row in &data_rows {
            assert!(
                row.len() >= expected_prefix_len,
                "aligned all-columns: data row shorter than prefix ({expected_prefix_len}): {row:?}"
            );
            let key_part = &row[expected_prefix_len..];
            assert_eq!(
                key_part, "test.txt",
                "aligned all-columns: KEY at offset {expected_prefix_len} should be 'test.txt', got {key_part:?} in row: {row:?}"
            );
        }
    });

    _guard.cleanup().await;
}

/// `-1` / `--one` object-listing: verifies that the one-line formatter
/// emits exactly one key per line (no tabs, no extra columns), that the
/// long form `--one` is equivalent to the short form `-1`, that
/// `--header` prepends exactly `KEY`, that non-recursive listing includes
/// common-prefix lines for `dir/`, and that `--show-objects-only`
/// suppresses those common-prefix lines.
#[tokio::test]
async fn e2e_one_line_object_listing() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        helper.put_object(&bucket, "a.txt", vec![0u8; 10]).await;
        helper.put_object(&bucket, "dir/b.txt", vec![0u8; 10]).await;
        helper.put_object(&bucket, "dir/c.txt", vec![0u8; 10]).await;

        let target = format!("s3://{bucket}/");
        let expected_keys = ["a.txt", "dir/b.txt", "dir/c.txt"];

        // Sub-assertion 1: -1 short form, recursive, no header
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "-1"]);
        assert!(
            output.status.success(),
            "one-line -1: s3ls failed: {}",
            output.stderr
        );
        // No tabs anywhere.
        assert!(
            !output.stdout.contains('\t'),
            "one-line -1: output contains tab character"
        );
        // All three keys present.
        for key in &expected_keys {
            assert!(
                output.stdout.contains(key),
                "one-line -1: key {key:?} missing from output"
            );
        }
        // Exactly one non-empty line per key (no header line by default).
        let data_lines: Vec<&str> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        assert_eq!(
            data_lines.len(),
            3,
            "one-line -1: expected 3 lines, got {}: {:?}",
            data_lines.len(),
            data_lines
        );
        // No header: no line is exactly "KEY".
        assert!(
            !data_lines.iter().any(|l| *l == "KEY"),
            "one-line -1: unexpected KEY header line without --header"
        );

        // Sub-assertion 2: --one long form produces same output
        let output2 = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--one"]);
        assert!(
            output2.status.success(),
            "one-line --one: s3ls failed: {}",
            output2.stderr
        );
        assert!(
            !output2.stdout.contains('\t'),
            "one-line --one: output contains tab character"
        );
        for key in &expected_keys {
            assert!(
                output2.stdout.contains(key),
                "one-line --one: key {key:?} missing from output"
            );
        }
        let data_lines2: Vec<&str> = output2
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        assert_eq!(
            data_lines2.len(),
            3,
            "one-line --one: expected 3 lines, got {}: {:?}",
            data_lines2.len(),
            data_lines2
        );

        // Sub-assertion 3: --header prepends exactly "KEY"
        let output3 = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--header", "-1"]);
        assert!(
            output3.status.success(),
            "one-line --header: s3ls failed: {}",
            output3.stderr
        );
        assert!(
            !output3.stdout.contains('\t'),
            "one-line --header: output contains tab character"
        );
        let all_lines3: Vec<&str> = output3
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        // First non-blank line must be exactly "KEY".
        assert_eq!(
            all_lines3.first().copied().unwrap_or(""),
            "KEY",
            "one-line --header: first non-blank line should be 'KEY', got: {:?}",
            all_lines3.first()
        );
        // Remaining lines are the three keys.
        let header_data: Vec<&str> = all_lines3.iter().copied().skip(1).collect();
        assert_eq!(
            header_data.len(),
            3,
            "one-line --header: expected 3 data lines after header, got {}: {:?}",
            header_data.len(),
            header_data
        );
        for key in &expected_keys {
            assert!(
                header_data.contains(key),
                "one-line --header: key {key:?} missing from data lines"
            );
        }

        // Sub-assertion 4: non-recursive listing includes "dir/" as a common-prefix line
        let output4 = TestHelper::run_s3ls(&[target.as_str(), "-1"]);
        assert!(
            output4.status.success(),
            "one-line non-recursive: s3ls failed: {}",
            output4.stderr
        );
        assert!(
            output4.stdout.contains("dir/"),
            "one-line non-recursive: expected 'dir/' common-prefix line in output"
        );

        // Sub-assertion 5: --show-objects-only suppresses the "dir/" common-prefix line
        let output5 = TestHelper::run_s3ls(&[target.as_str(), "-1", "--show-objects-only"]);
        assert!(
            output5.status.success(),
            "one-line --show-objects-only: s3ls failed: {}",
            output5.stderr
        );
        assert!(
            !output5.stdout.contains("dir/"),
            "one-line --show-objects-only: 'dir/' common-prefix should be suppressed"
        );
        // Only "a.txt" at the top level; dir/b.txt and dir/c.txt are under a prefix
        // and are not returned by the non-recursive listing.
        assert!(
            output5.stdout.contains("a.txt"),
            "one-line --show-objects-only: key 'a.txt' missing from output"
        );
    });

    _guard.cleanup().await;
}
