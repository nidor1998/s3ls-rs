#![cfg(e2e_test)]

//! Versioned-bucket filter end-to-end tests.
//!
//! Covers filter behaviors that are specific to versioned S3 buckets —
//! interactions that `tests/e2e_filters.rs` explicitly defers:
//!
//! 1. Regex filters apply to delete-marker keys.
//! 2. Size and storage-class filters let delete markers pass through
//!    unconditionally.
//! 3. Mtime filters evaluate delete markers by their own timestamps.
//! 4. `--hide-delete-markers` strips delete markers regardless of filters.
//! 5. Filters evaluate each version of a key independently.
//!
//! Each test creates a fresh versioned bucket via `create_versioned_bucket`,
//! builds a minimal inline fixture, runs `s3ls --all-versions --json` with
//! a single filter flag, and asserts the resulting NDJSON via
//! `assert_json_version_shapes_eq` (a multiset of `(Key, is_delete_marker)`
//! tuples).
//!
//! Design: `docs/superpowers/specs/2026-04-11-versioned-filter-e2e-tests-design.md`

mod common;

use common::*;

/// Proves `--filter-include-regex` is applied to delete-marker keys: a
/// delete marker whose key matches the regex is kept; a delete marker
/// whose key doesn't match is dropped. Two versions of `keep.csv` plus
/// two delete markers exercise both multi-version handling and the
/// regex-against-DM-key contract simultaneously.
#[tokio::test]
async fn e2e_versioned_include_regex_drops_delete_marker() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of keep.csv
        helper.put_object(&bucket, "keep.csv", vec![0u8; 100]).await;
        helper.put_object(&bucket, "keep.csv", vec![0u8; 200]).await;

        // One version of drop.txt
        helper.put_object(&bucket, "drop.txt", vec![0u8; 100]).await;

        // Delete markers on both keys
        helper.create_delete_marker(&bucket, "drop.txt").await;
        helper.create_delete_marker(&bucket, "keep.csv").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-include-regex",
            r"\.csv$",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 2 keep.csv object rows + 1 keep.csv DM row.
        // drop.txt v1 fails the regex; drop.txt DM fails the regex.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("keep.csv", false), ("keep.csv", false), ("keep.csv", true)],
            "versioned include-regex: DM filtered by key",
        );
    });

    _guard.cleanup().await;
}

/// Proves `--filter-exclude-regex` is applied to delete-marker keys: a
/// delete marker whose key matches the exclude regex is dropped.
/// Inverse of `e2e_versioned_include_regex_drops_delete_marker`.
#[tokio::test]
async fn e2e_versioned_exclude_regex_drops_delete_marker() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of keep.bin
        helper.put_object(&bucket, "keep.bin", vec![0u8; 100]).await;
        helper.put_object(&bucket, "keep.bin", vec![0u8; 200]).await;

        // One version of skip_me.bin
        helper
            .put_object(&bucket, "skip_me.bin", vec![0u8; 100])
            .await;

        // Delete markers on both keys
        helper.create_delete_marker(&bucket, "skip_me.bin").await;
        helper.create_delete_marker(&bucket, "keep.bin").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-exclude-regex",
            "^skip_",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 2 keep.bin object rows + 1 keep.bin DM row.
        // skip_me.bin v1 fails the exclude regex; skip_me.bin DM fails
        // the exclude regex.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("keep.bin", false), ("keep.bin", false), ("keep.bin", true)],
            "versioned exclude-regex: DM filtered by key",
        );
    });

    _guard.cleanup().await;
}

/// Locks in "delete markers always pass size filters" — verified against
/// `src/filters/smaller_size.rs:25` and `larger_size.rs:25`, both of which
/// unconditionally return `Ok(true)` for `ListEntry::DeleteMarker` before
/// any size comparison.
///
/// Does NOT use `--hide-delete-markers` because the test's entire point is
/// to observe a delete marker surviving the filter; the hide flag would
/// strip the DM before the filter chain runs (`src/lister.rs:48` applies
/// it before `filter_chain.matches` at line 51).
#[tokio::test]
async fn e2e_versioned_size_filter_passes_delete_markers() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of big.bin, both passing --filter-larger-size 1000.
        helper.put_object(&bucket, "big.bin", vec![0u8; 5000]).await;
        helper.put_object(&bucket, "big.bin", vec![0u8; 7000]).await;

        // One version of small.bin (100 bytes, fails size filter).
        helper
            .put_object(&bucket, "small.bin", vec![0u8; 100])
            .await;

        // DM on small.bin — has no size, must pass the filter anyway.
        helper.create_delete_marker(&bucket, "small.bin").await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-larger-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 2 big.bin versions + 1 small.bin DM.
        // small.bin v1 (100 bytes) fails --filter-larger-size 1000.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("big.bin", false), ("big.bin", false), ("small.bin", true)],
            "versioned size filter: DM passes through",
        );
    });

    _guard.cleanup().await;
}

/// Locks in "delete markers always pass storage-class filter" — verified
/// against `src/filters/storage_class.rs:47`
/// (`ListEntry::DeleteMarker { .. } => Ok(true)`).
#[tokio::test]
async fn e2e_versioned_storage_class_passes_delete_markers() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // ia.bin: explicit STANDARD_IA class (fails --storage-class STANDARD).
        helper
            .put_object_with_storage_class(&bucket, "ia.bin", vec![0u8; 100], "STANDARD_IA")
            .await;
        // DM on ia.bin — has no storage class, must pass the filter anyway.
        helper.create_delete_marker(&bucket, "ia.bin").await;

        // std.bin: default STANDARD class (S3 reports as None, filter treats
        // as STANDARD per src/filters/storage_class.rs:33).
        helper.put_object(&bucket, "std.bin", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--storage-class",
            "STANDARD",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: 1 std.bin object row + 1 ia.bin DM row.
        // ia.bin v1 (STANDARD_IA) fails the filter.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("std.bin", false), ("ia.bin", true)],
            "versioned storage-class: DM passes through",
        );
    });

    _guard.cleanup().await;
}

/// Locks in "mtime filters DO apply to delete-marker timestamps" —
/// verified against `src/filters/mtime_before.rs:27` and
/// `mtime_after.rs:27`, which both use `entry.last_modified()` uniformly
/// for both objects and delete markers.
///
/// Two-batch fixture with a 1.5s sleep between batches to guarantee a
/// second-level time pivot:
///   Batch 1: put_object("old.bin", ...) — v1 of old.bin, BEFORE pivot
///   sleep 1.5s
///   Batch 2: put_object("new.bin", ...) — v1 of new.bin, AFTER pivot
///           create_delete_marker("old.bin") — DM on old.bin, AFTER pivot
///
/// Expected under `--filter-mtime-after <pivot>`:
///   - new.bin v1 passes (batch 2)
///   - old.bin v1 fails (batch 1, before pivot)
///   - old.bin DM passes (created in batch 2, after pivot — DM mtime is
///     its own creation time, not the original object's)
#[tokio::test]
async fn e2e_versioned_mtime_filter_applies_to_delete_markers() {
    use chrono::{DateTime, Utc};
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Batch 1: old.bin v1 (BEFORE pivot)
        helper.put_object(&bucket, "old.bin", vec![0u8; 100]).await;

        sleep(Duration::from_millis(1500)).await;

        // Batch 2: new.bin v1 + DM on old.bin (BOTH AFTER pivot)
        helper.put_object(&bucket, "new.bin", vec![0u8; 100]).await;
        helper.create_delete_marker(&bucket, "old.bin").await;

        // Read back all rows via list_object_versions. This returns
        // objects AND delete markers with their LastModified timestamps.
        // Compute t_pivot = min(batch 2 last_modified) and sanity-check
        // it is strictly after old.bin v1's LastModified.
        let resp = helper
            .client()
            .list_object_versions()
            .bucket(&bucket)
            .send()
            .await
            .expect("list_object_versions failed");

        let mut old_lm: Option<DateTime<Utc>> = None;
        let mut batch2_min: Option<DateTime<Utc>> = None;

        // Regular object versions
        for v in resp.versions() {
            let key = v.key().expect("version missing key");
            let lm = v.last_modified().expect("version missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            if key == "old.bin" {
                // old.bin v1 — batch 1
                old_lm = Some(dt);
            } else {
                // new.bin v1 — batch 2
                batch2_min = Some(match batch2_min {
                    None => dt,
                    Some(cur) => cur.min(dt),
                });
            }
        }

        // Delete markers (old.bin DM — batch 2)
        for m in resp.delete_markers() {
            let lm = m.last_modified().expect("DM missing last_modified");
            let dt = DateTime::<Utc>::from_timestamp(lm.secs(), lm.subsec_nanos())
                .expect("invalid timestamp from S3");
            batch2_min = Some(match batch2_min {
                None => dt,
                Some(cur) => cur.min(dt),
            });
        }

        let old_lm = old_lm.expect("old.bin v1 not found in listing");
        let t_pivot = batch2_min.expect("batch 2 rows not found in listing");

        assert!(
            t_pivot > old_lm,
            "t_pivot ({t_pivot}) must be strictly after old.bin v1 ({old_lm}) \
             — the 1.5s sleep should have guaranteed this. Clock skew > 1.5s?"
        );

        let mtime_after = t_pivot.to_rfc3339();
        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-mtime-after",
            mtime_after.as_str(),
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: new.bin v1 + old.bin DM. old.bin v1 fails mtime-after.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("new.bin", false), ("old.bin", true)],
            "versioned mtime-after: DM filtered by own timestamp",
        );
    });

    _guard.cleanup().await;
}

/// Locks in `--hide-delete-markers` behavior. Runs s3ls twice against
/// the same bucket: once WITH the flag (expect 2 rows) and once
/// WITHOUT (expect 3 rows). The difference of exactly one delete
/// marker row proves the flag strips DMs as documented.
///
/// `--hide-delete-markers` is applied at `src/lister.rs:48`, BEFORE
/// the filter chain runs at line 51. This test doesn't combine the
/// flag with any filter; it asserts the flag's effect in isolation.
#[tokio::test]
async fn e2e_versioned_hide_delete_markers() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of doc.txt plus a delete marker as the "latest".
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        // Run 1: with --hide-delete-markers. Expect 2 object rows, no DM.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--hide-delete-markers",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("doc.txt", false), ("doc.txt", false)],
            "hide-delete-markers: DM stripped",
        );

        // Run 2: without --hide-delete-markers. Expect 2 object rows + 1 DM.
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--all-versions", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("doc.txt", false), ("doc.txt", false), ("doc.txt", true)],
            "hide-delete-markers: baseline includes DM",
        );
    });

    _guard.cleanup().await;
}

/// Locks in "size filters evaluate each version's own size" — the same
/// key appears with 3 different sizes across versions, and only the
/// middle version (v2, 5000 bytes) survives `--filter-larger-size 1000`.
///
/// This is the one test in the suite where the same key appears multiple
/// times in the fixture but NOT all versions survive. It proves filters
/// see each version's metadata independently rather than treating all
/// versions of a key as a unit.
#[tokio::test]
async fn e2e_versioned_size_filter_per_version() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Three versions of growing.bin: small, large, small again.
        helper
            .put_object(&bucket, "growing.bin", vec![0u8; 100])
            .await;
        helper
            .put_object(&bucket, "growing.bin", vec![0u8; 5000])
            .await;
        helper
            .put_object(&bucket, "growing.bin", vec![0u8; 200])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--filter-larger-size",
            "1000",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Expected: ONLY v2 (5000 bytes) survives. v1 (100) and v3 (200)
        // fail the size filter on their own sizes.
        assert_json_version_shapes_eq(
            &output.stdout,
            &[("growing.bin", false)],
            "versioned size filter: only v2 survives per-version check",
        );
    });

    _guard.cleanup().await;
}
