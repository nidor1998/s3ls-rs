#![cfg(e2e_test)]

//! Sort end-to-end tests.
//!
//! Covers s3ls sort functionality in JSON mode: every sort field
//! (`key`, `size`, `date` for objects; `bucket` for bucket listings),
//! both directions (`--reverse`), multi-column with tiebreak,
//! `--no-sort` streaming, and the `--all-versions` auto-appended
//! secondary date sort.
//!
//! All assertions use `--json` output and `assert_json_keys_order_eq`
//! (sequence comparison) or `assert_json_keys_eq` (set comparison for
//! `--no-sort`).
//!
//! Design: `docs/superpowers/specs/2026-04-11-sort-e2e-tests-design.md`

mod common;

use common::*;

/// Default and explicit `--sort key`: objects sorted alphabetically by key.
///
/// Fixture keys are non-alphabetical (`c, a, b`) so the sort is
/// observable. Two sub-assertions verify both explicit `--sort key` and
/// the implicit default produce the same ascending-key order.
#[tokio::test]
async fn e2e_sort_key_asc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("c.txt".to_string(), vec![0u8; 100]),
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 100]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: explicit --sort key
        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json", "--sort", "key"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "sort key asc: explicit --sort key",
        );

        // Sub-assertion 2: no --sort (default is key ascending)
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "sort key asc: default (no --sort)",
        );
    });

    _guard.cleanup().await;
}

/// `--sort key --reverse`: objects sorted in reverse alphabetical order.
#[tokio::test]
async fn e2e_sort_key_desc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("c.txt".to_string(), vec![0u8; 100]),
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 100]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "key",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["c.txt", "b.txt", "a.txt"],
            "sort key desc",
        );
    });

    _guard.cleanup().await;
}
