#![cfg(e2e_test)]

//! Large-scale listing completeness test.
//!
//! Uploads ~16,000 objects with a realistic 6-7 level hierarchy (data
//! lake partitions at depth 6, application logs at depth 7) and
//! verifies s3ls enumerates every object correctly under 4 different
//! listing configurations: full recursive, prefix-scoped, max-depth 3,
//! and max-parallel-listing-max-depth 1.
//!
//! This is the only e2e test that exercises the parallel listing engine
//! at realistic scale. All other tests use tiny fixtures (3-10 objects).
//!
//! Uses a 300-second timeout (not the standard 60s e2e_timeout!).
//!
//! Design: `docs/superpowers/specs/2026-04-11-large-listing-e2e-tests-design.md`

mod common;

use common::*;
use std::collections::HashSet;

/// Generate the expected set of 16,082 keys for the large-listing fixture.
///
/// Hierarchy:
/// - `config.json` (depth 1)
/// - `data/manifest.json` (depth 2)
/// - `data/tenant-{01..05}/{2024,2025}/{01..12}/{01..25}/part-{001..005}.parquet` (depth 6)
/// - `logs/app/{2024,2025}/{01..12}/{01..15}/server-{01..03}/app.log` (depth 7)
fn generate_expected_keys() -> Vec<String> {
    let mut keys: Vec<String> = Vec::with_capacity(16_082);

    // Depth 1: config file
    keys.push("config.json".to_string());

    // Depth 2: data manifest
    keys.push("data/manifest.json".to_string());

    // Depth 6: data partitions
    // 5 tenants × 2 years × 12 months × 25 days × 5 files = 15,000
    for tenant in 1..=5 {
        for year in [2024, 2025] {
            for month in 1..=12 {
                for day in 1..=25 {
                    for part in 1..=5 {
                        keys.push(format!(
                            "data/tenant-{tenant:02}/{year}/{month:02}/{day:02}/part-{part:03}.parquet"
                        ));
                    }
                }
            }
        }
    }

    // Depth 7: application logs
    // 2 years × 12 months × 15 days × 3 servers × 1 file = 1,080
    for year in [2024, 2025] {
        for month in 1..=12 {
            for day in 1..=15 {
                for server in 1..=3 {
                    keys.push(format!(
                        "logs/app/{year}/{month:02}/{day:02}/server-{server:02}/app.log"
                    ));
                }
            }
        }
    }

    assert_eq!(
        keys.len(),
        16_082,
        "key generation bug: expected 16,082 keys"
    );
    keys
}

/// Parse NDJSON stdout and collect all `Key` fields into a HashSet.
fn collect_keys_from_json(stdout: &str) -> HashSet<String> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()?
                .get("Key")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect()
}

/// Parse NDJSON stdout and collect all `Prefix` fields into a HashSet.
fn collect_prefixes_from_json(stdout: &str) -> HashSet<String> {
    stdout
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()?
                .get("Prefix")?
                .as_str()
                .map(|s| s.to_string())
        })
        .collect()
}

/// Assert that `actual` key set equals `expected` key set. On mismatch,
/// reports counts and the first 10 missing/extra keys (not the full
/// 16K-line stdout).
fn assert_key_set_eq(actual: &HashSet<String>, expected: &HashSet<String>, label: &str) {
    if actual == expected {
        return;
    }

    let missing_count = expected.difference(actual).count();
    let extra_count = actual.difference(expected).count();
    let mut missing: Vec<&String> = expected.difference(actual).collect();
    let mut extra: Vec<&String> = actual.difference(expected).collect();
    missing.sort();
    extra.sort();
    missing.truncate(10);
    extra.truncate(10);

    panic!(
        "[{label}] key set mismatch\n  \
         expected count: {}\n  \
         actual count:   {}\n  \
         missing ({missing_count} total, first 10): {missing:?}\n  \
         extra ({extra_count} total, first 10): {extra:?}",
        expected.len(),
        actual.len(),
    );
}

/// Large-scale listing completeness test.
///
/// Uploads 16,082 objects with a 6-7 level hierarchy, then runs s3ls
/// 5 times under different configurations and asserts enumeration
/// completeness for each.
///
/// Uses a 300-second timeout (not the standard 60s e2e_timeout!).
/// Upload uses 256 concurrent PUTs via `put_objects_parallel_n`.
#[tokio::test]
async fn e2e_large_listing_completeness() {
    use std::time::Duration;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    let result = tokio::time::timeout(Duration::from_secs(300), async {
        helper.create_bucket(&bucket).await;

        // --- Generate and upload fixture ---
        let expected_keys = generate_expected_keys();
        let expected_set: HashSet<String> = expected_keys.iter().cloned().collect();

        println!(
            "Uploading {} objects with 64 concurrent PUTs...",
            expected_keys.len()
        );
        let objects: Vec<(String, Vec<u8>)> = expected_keys
            .iter()
            .map(|k| (k.clone(), b"x".to_vec()))
            .collect();
        helper.put_objects_parallel_n(&bucket, objects, 64).await;
        println!("Upload complete.");

        let target = format!("s3://{bucket}/");

        // --- Sub-assertion 1: Full recursive listing from root ---
        println!("Sub-assertion 1: full recursive listing...");
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual = collect_keys_from_json(&output.stdout);
        assert_key_set_eq(&actual, &expected_set, "full recursive listing");
        println!("  OK: {} keys match.", actual.len());

        // --- Sub-assertion 2: Prefix-scoped listing ---
        println!("Sub-assertion 2: prefix-scoped listing (data/tenant-03/2025/)...");
        let prefix_target = format!("s3://{bucket}/data/tenant-03/2025/");
        let output =
            TestHelper::run_s3ls(&[prefix_target.as_str(), "--recursive", "--json", "--no-sort"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual_prefix = collect_keys_from_json(&output.stdout);
        let expected_prefix: HashSet<String> = expected_set
            .iter()
            .filter(|k| k.starts_with("data/tenant-03/2025/"))
            .cloned()
            .collect();
        assert_eq!(
            expected_prefix.len(),
            1500,
            "sanity: expected 12 months × 25 days × 5 files = 1500"
        );
        assert_key_set_eq(
            &actual_prefix,
            &expected_prefix,
            "prefix-scoped listing (data/tenant-03/2025/)",
        );
        println!("  OK: {} keys match.", actual_prefix.len());

        // --- Sub-assertion 3: Depth-limited listing (max-depth 3) ---
        println!("Sub-assertion 3: max-depth 3...");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--max-depth",
            "3",
            "--json",
            "--no-sort",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        let depth3_keys = collect_keys_from_json(&output.stdout);
        let depth3_prefixes = collect_prefixes_from_json(&output.stdout);

        // Objects at depth ≤ 3: config.json (depth 1), data/manifest.json (depth 2)
        let expected_depth3_keys: HashSet<String> = ["config.json", "data/manifest.json"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_key_set_eq(
            &depth3_keys,
            &expected_depth3_keys,
            "max-depth 3: objects at depth <= 3",
        );

        // Prefix entries at the depth-3 boundary:
        // data/tenant-{01..05}/{2024,2025}/ = 5 × 2 = 10 entries
        // + logs/app/{2024,2025}/ = 2 entries
        // Total: 12 prefixes.
        let mut expected_depth3_prefixes: HashSet<String> = HashSet::new();
        for tenant in 1..=5 {
            for year in [2024, 2025] {
                expected_depth3_prefixes.insert(format!("data/tenant-{tenant:02}/{year}/"));
            }
        }
        expected_depth3_prefixes.insert("logs/app/2024/".to_string());
        expected_depth3_prefixes.insert("logs/app/2025/".to_string());
        assert_eq!(
            depth3_prefixes, expected_depth3_prefixes,
            "max-depth 3: prefix entries mismatch\n  \
             expected: {expected_depth3_prefixes:?}\n  \
             actual: {depth3_prefixes:?}"
        );
        println!(
            "  OK: {} objects + {} prefixes.",
            depth3_keys.len(),
            depth3_prefixes.len()
        );

        // --- Sub-assertion 4: max-parallel-listing-max-depth 1 ---
        println!("Sub-assertion 4: max-parallel-listing-max-depth 1...");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--no-sort",
            "--max-parallel-listing-max-depth",
            "1",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let actual = collect_keys_from_json(&output.stdout);
        assert_key_set_eq(&actual, &expected_set, "max-parallel-listing-max-depth 1");
        println!("  OK: {} keys match.", actual.len());

        println!(
            "All 4 sub-assertions passed for {} objects.",
            expected_set.len()
        );
    })
    .await;

    // Cleanup runs regardless of timeout.
    _guard.cleanup().await;

    // Propagate timeout error after cleanup.
    result.expect("large-listing test timed out after 300 seconds");
}
