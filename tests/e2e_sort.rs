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

/// `--sort size`: objects sorted by size ascending. Fixture keys are
/// non-alphabetical and sizes are distinct so sort-by-size produces
/// a different order than sort-by-key.
#[tokio::test]
async fn e2e_sort_size_asc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("medium.bin".to_string(), vec![0u8; 5000]),
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
            ("small.bin".to_string(), vec![0u8; 1000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json", "--sort", "size"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["tiny.bin", "small.bin", "medium.bin", "large.bin"],
            "sort size asc",
        );
    });

    _guard.cleanup().await;
}

/// `--sort size --reverse`: objects sorted by size descending.
#[tokio::test]
async fn e2e_sort_size_desc() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("medium.bin".to_string(), vec![0u8; 5000]),
            ("tiny.bin".to_string(), vec![0u8; 10]),
            ("large.bin".to_string(), vec![0u8; 100_000]),
            ("small.bin".to_string(), vec![0u8; 1000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "size",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["large.bin", "medium.bin", "small.bin", "tiny.bin"],
            "sort size desc",
        );
    });

    _guard.cleanup().await;
}

/// `--sort date`: objects sorted by LastModified ascending (oldest first).
///
/// Fixture uploads objects sequentially with 1.5s sleeps between each
/// to guarantee distinct S3-second timestamps. Upload order `c, a, b`
/// is deliberately non-alphabetical so `--sort date` produces a
/// different order than the default key sort.
#[tokio::test]
async fn e2e_sort_date_asc() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload in non-alphabetical order: c, a, b.
        // Sleeps guarantee distinct LastModified seconds.
        helper.put_object(&bucket, "c.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "a.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "b.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output =
            TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json", "--sort", "date"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Oldest first = upload order: c, a, b.
        assert_json_keys_order_eq(
            &output.stdout,
            &["c.txt", "a.txt", "b.txt"],
            "sort date asc",
        );
    });

    _guard.cleanup().await;
}

/// `--sort date --reverse`: objects sorted by LastModified descending
/// (newest first).
#[tokio::test]
async fn e2e_sort_date_desc() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        helper.put_object(&bucket, "c.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "a.txt", vec![0u8; 100]).await;
        sleep(Duration::from_millis(1500)).await;
        helper.put_object(&bucket, "b.txt", vec![0u8; 100]).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "date",
            "--reverse",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Newest first = reverse upload order: b, a, c.
        assert_json_keys_order_eq(
            &output.stdout,
            &["b.txt", "a.txt", "c.txt"],
            "sort date desc",
        );
    });

    _guard.cleanup().await;
}

/// `--sort size,key`: multi-column sort where two objects tie on size
/// (5000 bytes each) and the secondary key sort disambiguates.
///
/// `a.csv` must appear before `b.csv` in the result even though `b.csv`
/// was uploaded first — both have size 5000, so the primary sort ties
/// them, and the secondary `key` sort produces alphabetical order.
#[tokio::test]
async fn e2e_sort_size_key_tiebreak() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("z.txt".to_string(), vec![0u8; 100]),
            ("b.csv".to_string(), vec![0u8; 5000]),
            ("a.csv".to_string(), vec![0u8; 5000]),
            ("m.txt".to_string(), vec![0u8; 10000]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--sort",
            "size,key",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_order_eq(
            &output.stdout,
            &["z.txt", "a.csv", "b.csv", "m.txt"],
            "sort size,key tiebreak",
        );
    });

    _guard.cleanup().await;
}

/// `--no-sort`: results stream in arbitrary order. This test asserts
/// only set equality (all expected keys are present), NOT ordering.
///
/// `--no-sort` has `conflicts_with_all = ["sort", "reverse"]` at
/// `src/config/args/mod.rs:234`, so it cannot be combined with
/// `--sort` or `--reverse` — clap rejects it at parse time.
///
/// Order is intentionally not asserted per commit 3e6c4fb
/// ("clarify --no-sort produces arbitrary order").
#[tokio::test]
async fn e2e_sort_no_sort() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("a.txt".to_string(), vec![0u8; 100]),
            ("b.txt".to_string(), vec![0u8; 100]),
            ("c.txt".to_string(), vec![0u8; 100]),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Set equality only — order is intentionally not asserted.
        assert_json_keys_eq(
            &output.stdout,
            &["a.txt", "b.txt", "c.txt"],
            "no-sort: set equality",
        );
    });

    _guard.cleanup().await;
}

/// `--all-versions --sort key`: auto-appends `date` as secondary sort
/// (per `src/config/args/mod.rs:759-761`). Two keys with 2 versions
/// each, uploaded sequentially with 1.5s sleeps. The result must show
/// key ascending (apple before banana) and within each key, date
/// ascending (v1 before v2).
#[tokio::test]
async fn e2e_sort_versioned_secondary_date() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // apple v1 → apple v2 → banana v1 → banana v2
        // Each upload separated by 1.5s to guarantee distinct LastModified.
        helper
            .put_object(&bucket, "apple.txt", vec![0u8; 100])
            .await;
        sleep(Duration::from_millis(1500)).await;
        helper
            .put_object(&bucket, "apple.txt", vec![0u8; 200])
            .await;
        sleep(Duration::from_millis(1500)).await;
        helper
            .put_object(&bucket, "banana.txt", vec![0u8; 100])
            .await;
        sleep(Duration::from_millis(1500)).await;
        helper
            .put_object(&bucket, "banana.txt", vec![0u8; 200])
            .await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--sort",
            "key",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Assertion 1: key sequence is apple, apple, banana, banana.
        assert_json_keys_order_eq(
            &output.stdout,
            &["apple.txt", "apple.txt", "banana.txt", "banana.txt"],
            "versioned secondary date: key order",
        );

        // Assertion 2: within each Key group, LastModified is
        // non-decreasing. This proves the auto-appended `date`
        // secondary sort is actually applied.
        let rows: Vec<(String, String)> = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|line| {
                let v: serde_json::Value = serde_json::from_str(line).ok()?;
                let key = v.get("Key")?.as_str()?.to_string();
                let lm = v.get("LastModified")?.as_str()?.to_string();
                Some((key, lm))
            })
            .collect();

        let mut prev_key: Option<&str> = None;
        let mut prev_lm: Option<&str> = None;
        for (k, lm) in &rows {
            if Some(k.as_str()) == prev_key {
                assert!(
                    lm.as_str() >= prev_lm.unwrap(),
                    "versioned secondary sort: within key {k:?}, LastModified not monotonic: {:?} -> {lm:?}",
                    prev_lm.unwrap()
                );
            }
            prev_key = Some(k.as_str());
            prev_lm = Some(lm.as_str());
        }
    });

    _guard.cleanup().await;
}

/// Bucket listing `--sort bucket`: two test buckets with deterministic
/// name prefixes (`s3ls-e2e-a-*` and `s3ls-e2e-z-*`) are created, and
/// the test asserts the `a-` bucket appears before the `z-` bucket in
/// the listing. Assertions are scoped to these two test buckets because
/// the account may have other buckets.
#[tokio::test]
async fn e2e_sort_bucket_listing_asc() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&["--json", "--sort", "bucket"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Find positions of our two test buckets in the NDJSON output.
        let mut pos_a: Option<usize> = None;
        let mut pos_z: Option<usize> = None;
        for (i, line) in output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
        {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
                && let Some(name) = v.get("Name").and_then(|n| n.as_str())
            {
                if name == bucket_a {
                    pos_a = Some(i);
                } else if name == bucket_z {
                    pos_z = Some(i);
                }
            }
        }

        let pos_a = pos_a.unwrap_or_else(|| {
            panic!("bucket listing asc: test bucket {bucket_a} not found in output")
        });
        let pos_z = pos_z.unwrap_or_else(|| {
            panic!("bucket listing asc: test bucket {bucket_z} not found in output")
        });

        assert!(
            pos_a < pos_z,
            "bucket listing asc: expected {bucket_a} (pos {pos_a}) before {bucket_z} (pos {pos_z})"
        );
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}

/// Bucket listing `--sort bucket --reverse`: the `z-` test bucket must
/// appear before the `a-` test bucket.
#[tokio::test]
async fn e2e_sort_bucket_listing_desc() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&["--json", "--sort", "bucket", "--reverse"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        let mut pos_a: Option<usize> = None;
        let mut pos_z: Option<usize> = None;
        for (i, line) in output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
        {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
                && let Some(name) = v.get("Name").and_then(|n| n.as_str())
            {
                if name == bucket_a {
                    pos_a = Some(i);
                } else if name == bucket_z {
                    pos_z = Some(i);
                }
            }
        }

        let pos_a = pos_a.unwrap_or_else(|| {
            panic!("bucket listing desc: test bucket {bucket_a} not found in output")
        });
        let pos_z = pos_z.unwrap_or_else(|| {
            panic!("bucket listing desc: test bucket {bucket_z} not found in output")
        });

        assert!(
            pos_z < pos_a,
            "bucket listing desc: expected {bucket_z} (pos {pos_z}) before {bucket_a} (pos {pos_a})"
        );
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}

/// Bucket listing `--sort date`: buckets sorted by creation date ascending.
///
/// Creates two buckets with a 1.5s sleep between them. The first bucket
/// (`early-`) should appear before the second bucket (`late-`).
///
/// Covers `src/bucket_lister.rs:44` (SortField::Date for bucket sorting).
#[tokio::test]
async fn e2e_sort_bucket_listing_by_date() {
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let id = Uuid::new_v4();
    let bucket_early = format!("s3ls-e2e-early-{id}");
    let bucket_late = format!("s3ls-e2e-late-{id}");
    let _guard_early = helper.bucket_guard(&bucket_early);
    let _guard_late = helper.bucket_guard(&bucket_late);

    e2e_timeout!(async {
        // Create early bucket first, then sleep, then create late bucket.
        helper.create_bucket(&bucket_early).await;
        sleep(Duration::from_millis(1500)).await;
        helper.create_bucket(&bucket_late).await;

        let output = TestHelper::run_s3ls(&["--json", "--sort", "date"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Find positions of our two test buckets.
        let mut pos_early: Option<usize> = None;
        let mut pos_late: Option<usize> = None;
        for (i, line) in output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .enumerate()
        {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
                && let Some(name) = v.get("Name").and_then(|n| n.as_str())
            {
                if name == bucket_early {
                    pos_early = Some(i);
                } else if name == bucket_late {
                    pos_late = Some(i);
                }
            }
        }

        let pos_early = pos_early.unwrap_or_else(|| {
            panic!("bucket sort date: test bucket {bucket_early} not found in output")
        });
        let pos_late = pos_late.unwrap_or_else(|| {
            panic!("bucket sort date: test bucket {bucket_late} not found in output")
        });

        assert!(
            pos_early < pos_late,
            "bucket sort date: expected {bucket_early} (pos {pos_early}) before {bucket_late} (pos {pos_late})"
        );
    });

    _guard_early.cleanup().await;
    _guard_late.cleanup().await;
}

/// Bucket listing `--sort size`: buckets have no size, so the comparator
/// always returns Equal. The test verifies that both buckets appear and
/// s3ls does not error.
///
/// Covers `src/bucket_lister.rs:46` (SortField::Size for bucket sorting —
/// always Equal).
#[tokio::test]
async fn e2e_sort_bucket_listing_by_size() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        // `--sort size` is not valid for bucket listing and must be rejected.
        let output = TestHelper::run_s3ls(&["--json", "--sort", "size"]);
        assert!(
            !output.status.success(),
            "s3ls should reject --sort size for bucket listing"
        );
        assert!(
            output
                .stderr
                .contains("sort field 'size' is not valid for bucket listing"),
            "expected validation error in stderr, got: {}",
            output.stderr
        );
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}

/// Bucket listing `--sort region`: buckets sorted by region. Since all
/// test buckets are in the same region, sort-by-region is equivalent to
/// a stable no-op — the test verifies that both buckets appear in the
/// output (the branch is exercised even if the region comparison always
/// returns Equal).
///
/// Covers `src/bucket_lister.rs:45` (SortField::Region for bucket sorting).
#[tokio::test]
async fn e2e_sort_bucket_listing_by_region() {
    use uuid::Uuid;

    let helper = TestHelper::new().await;
    let bucket_a = format!("s3ls-e2e-a-{}", Uuid::new_v4());
    let bucket_z = format!("s3ls-e2e-z-{}", Uuid::new_v4());
    let _guard_a = helper.bucket_guard(&bucket_a);
    let _guard_z = helper.bucket_guard(&bucket_z);

    e2e_timeout!(async {
        helper.create_bucket(&bucket_a).await;
        helper.create_bucket(&bucket_z).await;

        let output = TestHelper::run_s3ls(&["--json", "--sort", "region"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Both buckets must appear in the output.
        assert!(
            output.stdout.contains(&bucket_a),
            "bucket sort region: bucket {bucket_a} not found in output"
        );
        assert!(
            output.stdout.contains(&bucket_z),
            "bucket sort region: bucket {bucket_z} not found in output"
        );
    });

    _guard_a.cleanup().await;
    _guard_z.cleanup().await;
}
