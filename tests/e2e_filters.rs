#![cfg(e2e_test)]

//! Filter end-to-end tests.
//!
//! Covers every filter flag (`--filter-include-regex`,
//! `--filter-exclude-regex`, `--filter-smaller-size`,
//! `--filter-larger-size`, `--filter-mtime-before`,
//! `--filter-mtime-after`, `--storage-class`), their AND-composition,
//! and two orthogonal-flag interaction smoke tests (`--max-depth`
//! common-prefix passthrough, `--no-sort` streaming).
//!
//! Per-filter tests use a shared-fixture-within-a-test pattern: one
//! bucket per test, one fixture upload, multiple `run_s3ls` invocations
//! with labeled sub-assertions. This minimizes AWS round-trips while
//! keeping failure messages actionable via the `label` argument to
//! `assert_json_keys_eq`.
//!
//! Design: `docs/superpowers/specs/2026-04-11-step7-filter-e2e-tests-design.md`

mod common;

use common::*;

/// `--filter-include-regex`: include only keys matching a regex.
///
/// Fixture is 5 keys spanning two file types (csv, non-csv) so that
/// one small fixture supports match, no-match, anchor, and wildcard
/// sub-assertions.
#[tokio::test]
async fn e2e_filter_include_regex() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("report.csv".to_string(), b"a".to_vec()),
            ("data.csv".to_string(), b"a".to_vec()),
            ("summary.txt".to_string(), b"a".to_vec()),
            ("archive.tar.gz".to_string(), b"a".to_vec()),
            ("notes.md".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match `\.csv$`
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["report.csv", "data.csv"],
            "include-regex: match \\.csv$",
        );

        // Sub-assertion 2: no match `\.xlsx$`
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.xlsx$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "include-regex: no match \\.xlsx$");

        // Sub-assertion 3: anchor `^data`
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            "^data",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &["data.csv"], "include-regex: anchor ^data");

        // Sub-assertion 4: wildcard `.*` passes everything
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            ".*",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[
                "report.csv",
                "data.csv",
                "summary.txt",
                "archive.tar.gz",
                "notes.md",
            ],
            "include-regex: .* passes all",
        );
    });

    _guard.cleanup().await;
}

/// `--filter-exclude-regex`: exclude keys matching a regex.
///
/// Fixture is identical to `e2e_filter_include_regex` — exclude-regex
/// is the logical inverse of include-regex over the same object set.
#[tokio::test]
async fn e2e_filter_exclude_regex() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("report.csv".to_string(), b"a".to_vec()),
            ("data.csv".to_string(), b"a".to_vec()),
            ("summary.txt".to_string(), b"a".to_vec()),
            ("archive.tar.gz".to_string(), b"a".to_vec()),
            ("notes.md".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match `\.csv$` — excludes 2, keeps 3
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["summary.txt", "archive.tar.gz", "notes.md"],
            "exclude-regex: match \\.csv$",
        );

        // Sub-assertion 2: no match `\.xlsx$` — keeps all 5
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\.xlsx$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[
                "report.csv",
                "data.csv",
                "summary.txt",
                "archive.tar.gz",
                "notes.md",
            ],
            "exclude-regex: no match \\.xlsx$",
        );

        // Sub-assertion 3: wildcard `.*` excludes everything
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            ".*",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "exclude-regex: .* excludes all");
    });

    _guard.cleanup().await;
}

/// `--filter-smaller-size`: include only objects with `size < threshold`
/// (strict less-than, verified against `src/filters/smaller_size.rs:29`).
#[tokio::test]
async fn e2e_filter_smaller_size() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sizes chosen so 1000, 5000, and 1024 (1KiB) each bisect the set.
        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("small.bin".to_string(), vec![0u8; 1000]),
            ("medium.bin".to_string(), vec![0u8; 10_000]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match 5000 — tiny (10) and small (1000) pass
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "5000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["tiny.bin", "small.bin"],
            "smaller-size: match 5000",
        );

        // Sub-assertion 2: no match 1 — zero objects are smaller than 1 byte
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "1",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "smaller-size: no match 1");

        // Sub-assertion 3: strict-< boundary at 1000 — small.bin (exactly 1000)
        // is NOT strictly smaller than 1000, so only tiny.bin passes.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["tiny.bin"],
            "smaller-size: boundary 1000 strict",
        );

        // Sub-assertion 4: 1KiB = 1024 parses correctly. small.bin at 1000 is
        // strictly smaller than 1024, so both tiny and small pass.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-smaller-size",
            "1KiB",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["tiny.bin", "small.bin"],
            "smaller-size: 1KiB parses",
        );
    });

    _guard.cleanup().await;
}

/// `--filter-larger-size`: include only objects with `size >= threshold`
/// (inclusive `>=`, verified against `src/filters/larger_size.rs:29`).
#[tokio::test]
async fn e2e_filter_larger_size() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("small.bin".to_string(), vec![0u8; 1000]),
            ("medium.bin".to_string(), vec![0u8; 10_000]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: match 5000 — medium (10_000) and large (100_000) pass
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "5000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["medium.bin", "large.bin"],
            "larger-size: match 5000",
        );

        // Sub-assertion 2: no match 1_000_000 — no object is that large
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "1000000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "larger-size: no match 1000000");

        // Sub-assertion 3: inclusive >= boundary at 10_000. medium.bin at
        // exactly 10_000 passes because the filter is inclusive.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "10000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["medium.bin", "large.bin"],
            "larger-size: boundary 10000 inclusive",
        );

        // Sub-assertion 4: 10KiB = 10240. medium.bin at 10_000 is less than
        // 10_240, so medium FAILS; only large (100_000) passes.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-larger-size",
            "10KiB",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &["large.bin"], "larger-size: 10KiB parses");
    });

    _guard.cleanup().await;
}

/// `--filter-mtime-before`: include only objects with `last_modified < pivot`
/// (strict `<`, verified against `src/filters/mtime_before.rs:27`).
///
/// Uploads 3 objects sequentially with 1.5s sleeps between each to
/// guarantee distinct S3-second timestamps: old.txt (t1), mid.txt (t2),
/// new.txt (t3). Reads back actual timestamps from S3 and uses them as
/// deterministic pivots for each sub-assertion.
#[tokio::test]
async fn e2e_filter_mtime_before() {
    use chrono::{DateTime, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sequential uploads with sleeps guarantee distinct LastModified.
        helper.put_object(&bucket, "old.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "mid.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "new.txt", vec![0u8; 100]).await;

        // Read back actual LastModified values from S3.
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let mut timestamps: Vec<(String, DateTime<Utc>)> = resp
            .contents()
            .iter()
            .map(|obj| {
                let key = obj.key().expect("object missing key").to_string();
                let lm = obj.last_modified().expect("object missing last_modified");
                let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                    .expect("invalid timestamp from S3");
                (key, dt)
            })
            .collect();
        timestamps.sort_by_key(|(_, dt)| *dt);
        assert_eq!(timestamps.len(), 3);

        let t1 = timestamps[0].1; // old.txt
        let t2 = timestamps[1].1; // mid.txt
        let t3 = timestamps[2].1; // new.txt

        // Sanity: sleeps guarantee strict ordering.
        assert!(
            t1 < t2 && t2 < t3,
            "timestamps not strictly ordered: t1={t1}, t2={t2}, t3={t3}"
        );

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: pivot = t2. strict-< means old.txt (t1 < t2) passes.
        let pivot_str = t2.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-before",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["old.txt"],
            "mtime-before: middle pivot t2",
        );

        // Sub-assertion 2: pivot = t1. strict-< against the earliest means
        // nothing passes.
        let pivot_str = t1.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-before",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[],
            "mtime-before: no match (earliest pivot)",
        );

        // Sub-assertion 3: pivot = t3. Everything strictly before t3 passes.
        let pivot_str = t3.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-before",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["old.txt", "mid.txt"],
            "mtime-before: boundary (max pivot t3)",
        );
    });

    _guard.cleanup().await;
}

/// `--filter-mtime-after`: include only objects with `last_modified >= pivot`
/// (inclusive `>=`, verified against `src/filters/mtime_after.rs:27`).
///
/// Same sequential-upload pattern as `e2e_filter_mtime_before`: 3 objects
/// with 1.5s sleeps guarantee distinct S3-second timestamps.
#[tokio::test]
async fn e2e_filter_mtime_after() {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Sequential uploads with sleeps guarantee distinct LastModified.
        helper.put_object(&bucket, "old.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "mid.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "new.txt", vec![0u8; 100]).await;

        // Read back actual LastModified values from S3.
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let mut timestamps: Vec<(String, DateTime<Utc>)> = resp
            .contents()
            .iter()
            .map(|obj| {
                let key = obj.key().expect("object missing key").to_string();
                let lm = obj.last_modified().expect("object missing last_modified");
                let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                    .expect("invalid timestamp from S3");
                (key, dt)
            })
            .collect();
        timestamps.sort_by_key(|(_, dt)| *dt);
        assert_eq!(timestamps.len(), 3);

        let t1 = timestamps[0].1; // old.txt
        let t2 = timestamps[1].1; // mid.txt
        let t3 = timestamps[2].1; // new.txt

        assert!(
            t1 < t2 && t2 < t3,
            "timestamps not strictly ordered: t1={t1}, t2={t2}, t3={t3}"
        );

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: pivot = t2. inclusive-≥ means mid.txt and new.txt pass.
        let pivot_str = t2.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["mid.txt", "new.txt"],
            "mtime-after: middle pivot t2",
        );

        // Sub-assertion 2: pivot = t3 + 1s. Nothing at or after that.
        let after_max = t3 + ChronoDuration::seconds(1);
        let pivot_str = after_max.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "mtime-after: no match (after max)");

        // Sub-assertion 3: pivot = t1. inclusive-≥ at the earliest means
        // all 3 objects pass.
        let pivot_str = t1.to_rfc3339();
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            pivot_str.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["old.txt", "mid.txt", "new.txt"],
            "mtime-after: boundary (earliest pivot inclusive)",
        );
    });

    _guard.cleanup().await;
}

/// `--storage-class`: include only objects in listed storage classes.
///
/// S3 omits the `StorageClass` field for STANDARD objects (returning
/// `None`), and `src/filters/storage_class.rs:33` treats `None` as
/// `"STANDARD"` — so `--storage-class STANDARD` still matches objects
/// uploaded with the default class. This test locks that in.
#[tokio::test]
async fn e2e_filter_storage_class() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // std.bin uses the default (no explicit class), so S3 records
        // StorageClass=None → filter treats as STANDARD.
        helper.put_object(&bucket, "std.bin", vec![0u8; 100]).await;
        helper
            .put_object_with_storage_class(&bucket, "rrs.bin", vec![0u8; 100], "REDUCED_REDUNDANCY")
            .await;
        helper
            .put_object_with_storage_class(&bucket, "ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;
        helper
            .put_object_with_storage_class(&bucket, "oz.bin", vec![0u8; 100], "ONEZONE_IA")
            .await;
        helper
            .put_object_with_storage_class(&bucket, "it.bin", vec![0u8; 100], "INTELLIGENT_TIERING")
            .await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: single class match
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "STANDARD_IA",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["ia.bin"],
            "storage-class: single STANDARD_IA",
        );

        // Sub-assertion 2: multiple classes (comma-separated)
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "STANDARD_IA,ONEZONE_IA",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["ia.bin", "oz.bin"],
            "storage-class: multiple",
        );

        // Sub-assertion 3: no object in GLACIER — empty result
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "GLACIER",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &[], "storage-class: no match GLACIER");

        // Sub-assertion 4: STANDARD matches the None-StorageClass object
        // (std.bin). REDUCED_REDUNDANCY is NOT STANDARD, and the other three
        // are explicitly different classes.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["std.bin"],
            "storage-class: STANDARD matches None",
        );
    });

    _guard.cleanup().await;
}

/// All seven filters at once. Proves AND-composition across every filter
/// flag simultaneously. Exactly one object (`target.csv`) is designed
/// to survive the full filter chain.
///
/// Fixture strategy: two-batch upload with a 1.5s sleep between batches
/// so that `t_pivot = min(batch_2.last_modified)` is strictly greater
/// than `old.csv.last_modified`. S3 LastModified is second-precision,
/// so the 1.5s sleep is enough to push the next upload into the
/// following second even with clock skew.
#[tokio::test]
async fn e2e_filter_combo_all_seven() {
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // --- Batch 1: the one object that will fail mtime-after. ---
        helper.put_object(&bucket, "old.csv", vec![0u8; 5000]).await;

        // Guarantee a 1-second gap between the two batches.
        sleep(Duration::from_millis(1500)).await;

        // --- Batch 2: five objects, four of which each fail exactly one filter. ---
        let batch2: Vec<(String, Vec<u8>)> = vec![
            ("target.csv".to_string(), vec![0u8; 5000]),   // survivor
            ("target.txt".to_string(), vec![0u8; 5000]),   // fails include-regex
            ("excluded.csv".to_string(), vec![0u8; 5000]), // fails exclude-regex
            ("small.csv".to_string(), vec![0u8; 100]),     // fails larger-size
        ];
        helper.put_objects_parallel(&bucket, batch2).await;

        // ia.csv needs a distinct storage class, so it goes through the
        // single-object storage-class helper.
        helper
            .put_object_with_storage_class(&bucket, "ia.csv", vec![0u8; 5000], "STANDARD_IA")
            .await;

        // --- Read back LastModified for all 6 objects. ---
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let mut old_lm: Option<DateTime<Utc>> = None;
        let mut batch2_min: Option<DateTime<Utc>> = None;
        for obj in resp.contents() {
            let key = obj.key().expect("object missing key");
            let lm = obj.last_modified().expect("object missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            if key == "old.csv" {
                old_lm = Some(dt);
            } else {
                batch2_min = Some(match batch2_min {
                    None => dt,
                    Some(cur) => cur.min(dt),
                });
            }
        }
        let old_lm = old_lm.expect("old.csv not found in listing");
        let t_pivot = batch2_min.expect("batch 2 objects not found in listing");

        assert!(
            t_pivot > old_lm,
            "t_pivot ({t_pivot}) must be strictly after old.csv last-modified ({old_lm}) \
             — the 1.5s sleep should have guaranteed this. Clock skew > 1.5s?"
        );

        let mtime_after = t_pivot.to_rfc3339();
        let mtime_before = (t_pivot + ChronoDuration::hours(1)).to_rfc3339();

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
            "--filter-exclude-regex",
            "^excluded",
            "--filter-mtime-after",
            mtime_after.as_str(),
            "--filter-mtime-before",
            mtime_before.as_str(),
            "--filter-larger-size",
            "1000",
            "--filter-smaller-size",
            "10000",
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["target.csv"],
            "combo all seven: exactly target.csv survives",
        );
    });

    _guard.cleanup().await;
}

/// Regex × size composition: `.csv AND >= 1000 bytes`.
///
/// Fixture bisects cleanly: csv vs txt × small vs big, yielding exactly
/// one survivor.
#[tokio::test]
async fn e2e_filter_pair_regex_and_size() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("a.csv".to_string(), vec![0u8; 100]),
            ("b.csv".to_string(), vec![0u8; 2000]),
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 2000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
            "--filter-larger-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(&output.stdout, &["b.csv"], "pair regex+size: b.csv only");
    });

    _guard.cleanup().await;
}

/// Include-regex × exclude-regex composition: `.csv AND NOT _tmp`.
///
/// Proves the two regex filters compose correctly — exclude is applied
/// to the survivors of include, not to the original set.
#[tokio::test]
async fn e2e_filter_pair_include_and_exclude_regex() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("report.csv".to_string(), b"a".to_vec()),
            ("report_tmp.csv".to_string(), b"a".to_vec()),
            ("data.csv".to_string(), b"a".to_vec()),
            ("notes.txt".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
            "--filter-exclude-regex",
            "_tmp",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["report.csv", "data.csv"],
            "pair include+exclude: .csv minus _tmp",
        );
    });

    _guard.cleanup().await;
}

/// Mtime × storage-class composition: `mtime-after pivot AND STANDARD`.
///
/// Two-batch upload with a 1.5s sleep between batches. Each batch
/// contains one STANDARD and one STANDARD_IA object. The only survivor
/// is the batch-2 STANDARD object.
#[tokio::test]
async fn e2e_filter_pair_mtime_and_storage_class() {
    use chrono::{DateTime, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Batch 1
        helper
            .put_object(&bucket, "old_std.bin", vec![0u8; 100])
            .await;
        helper
            .put_object_with_storage_class(&bucket, "old_ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;

        sleep(Duration::from_millis(1500)).await;

        // Batch 2
        helper
            .put_object(&bucket, "new_std.bin", vec![0u8; 100])
            .await;
        helper
            .put_object_with_storage_class(&bucket, "new_ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;

        // Read back LastModified for all 4 objects.
        let resp = helper
            .client()
            .list_objects_v2()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_objects_v2 failed");

        let mut old_max: Option<DateTime<Utc>> = None;
        let mut new_min: Option<DateTime<Utc>> = None;
        for obj in resp.contents() {
            let key = obj.key().expect("object missing key");
            let lm = obj.last_modified().expect("object missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            if key.starts_with("old_") {
                old_max = Some(match old_max {
                    None => dt,
                    Some(cur) => cur.max(dt),
                });
            } else {
                new_min = Some(match new_min {
                    None => dt,
                    Some(cur) => cur.min(dt),
                });
            }
        }
        let old_max = old_max.expect("batch 1 objects not found");
        let new_min = new_min.expect("batch 2 objects not found");

        assert!(
            new_min > old_max,
            "new_min ({new_min}) must be strictly after old_max ({old_max}) \
             — 1.5s sleep should have guaranteed this. Clock skew > 1.5s?"
        );

        let mtime_after = new_min.to_rfc3339();
        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-mtime-after",
            mtime_after.as_str(),
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["new_std.bin"],
            "pair mtime+storage-class: new_std.bin only",
        );
    });

    _guard.cleanup().await;
}

/// Exclude × size-range composition: `NOT .tmp AND >= 1000 AND < 4000`.
///
/// Fixture is designed so exactly one object (`keep_mid.bin`) satisfies
/// all three constraints at once.
#[tokio::test]
async fn e2e_filter_pair_exclude_and_size_range() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("keep_small.bin".to_string(), vec![0u8; 500]),
            ("keep_big.bin".to_string(), vec![0u8; 5000]),
            ("keep_mid.bin".to_string(), vec![0u8; 2000]),
            ("skip_mid.tmp".to_string(), vec![0u8; 2000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\.tmp$",
            "--filter-larger-size",
            "1000",
            "--filter-smaller-size",
            "4000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["keep_mid.bin"],
            "pair exclude+size-range: keep_mid.bin only",
        );
    });

    _guard.cleanup().await;
}

/// Locks in `CommonPrefix` passthrough under `--filter-include-regex`
/// + `--max-depth`.
///
/// `FilterChain::matches` at `src/filters/mod.rs:37` short-circuits
/// `CommonPrefix` entries to always pass every filter. Without this
/// short-circuit, `--filter-include-regex '\.csv$'` would drop
/// `{"Prefix": "logs/"}` (the prefix doesn't match `\.csv$`), which
/// would break depth-limited recursion. This test hits that exact
/// interaction with real S3 listing + `--max-depth 1`.
///
/// The expected output includes both a `{"Key": "readme.csv", ...}`
/// object entry and a `{"Prefix": "logs/"}` common-prefix entry, so
/// this is the one test that uses `assert_json_keys_or_prefixes_eq`.
/// JSON shape confirmed against `src/aggregate.rs:514`.
#[tokio::test]
async fn e2e_filter_max_depth_common_prefix_passthrough() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("logs/2025/a.log".to_string(), b"a".to_vec()),
            ("logs/2025/b.log".to_string(), b"a".to_vec()),
            ("logs/2026/a.log".to_string(), b"a".to_vec()),
            ("readme.csv".to_string(), b"a".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "1",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected:
        // - readme.csv matches the regex → {"Key": "readme.csv", ...}
        // - logs/ is a CommonPrefix at depth 1 → {"Prefix": "logs/"}
        //   passes through the filter because CommonPrefix is exempt.
        assert_json_keys_or_prefixes_eq(
            &output.stdout,
            &["readme.csv", "logs/"],
            "max-depth: CommonPrefix passthrough under include-regex",
        );
    });

    _guard.cleanup().await;
}

/// Locks in that `--no-sort` still applies filters.
///
/// The streaming path bypasses the sort buffer. This test confirms
/// that the filter chain still runs — a future refactor that moved
/// filtering into the post-sort step would regress this.
///
/// Asserted as a set (order-independent) because `--no-sort` emits
/// results in arrival order, which is non-deterministic across runs.
#[tokio::test]
async fn e2e_filter_no_sort_streaming() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // a1..a6 with sizes 1000..6000 (step 1000).
        let fixture: Vec<(String, Vec<u8>)> = (1..=6)
            .map(|i| (format!("a{i}.bin"), vec![0u8; (i * 1000) as usize]))
            .collect();
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // --filter-larger-size 3000 → a3 (3000) through a6 (6000) pass.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--no-sort",
            "--json",
            "--filter-larger-size",
            "3000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["a3.bin", "a4.bin", "a5.bin", "a6.bin"],
            "no-sort streaming: larger-size 3000",
        );
    });

    _guard.cleanup().await;
}
