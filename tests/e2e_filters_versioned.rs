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
