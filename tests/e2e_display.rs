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
