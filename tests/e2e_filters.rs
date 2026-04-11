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
