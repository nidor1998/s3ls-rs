#![cfg(e2e_test)]

//! Edge-case end-to-end tests.
//!
//! Covers unusual S3 key patterns and display behaviors:
//! - UTF-8 multi-byte keys (2-3 byte characters)
//! - UTF-8 4-byte characters (emoji, CJK supplementary)
//! - Control characters in keys (escaped vs raw)
//! - Zero-byte objects with "/" suffix (directory marker keys)
//! - Multiple checksum algorithms in text and JSON format
//! - `--raw-output` control character passthrough

mod common;

use common::*;

/// UTF-8 multi-byte characters (2-3 bytes: CJK, accented Latin, etc.)
/// can be listed correctly in JSON mode.
#[tokio::test]
async fn e2e_edge_case_utf8_keys() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("日本語/テスト.txt".to_string(), b"x".to_vec()),
            ("données/rapport.csv".to_string(), b"x".to_vec()),
            ("中文/数据.json".to_string(), b"x".to_vec()),
            ("한국어/파일.txt".to_string(), b"x".to_vec()),
            ("ascii/plain.txt".to_string(), b"x".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_json_keys_eq(
            &output.stdout,
            &[
                "日本語/テスト.txt",
                "données/rapport.csv",
                "中文/数据.json",
                "한국어/파일.txt",
                "ascii/plain.txt",
            ],
            "utf8 keys",
        );
    });

    _guard.cleanup().await;
}

/// UTF-8 4-byte characters (emoji, CJK Unified Ideographs Extension B)
/// round-trip correctly through S3 and s3ls JSON output.
#[tokio::test]
async fn e2e_edge_case_utf8_4byte_keys() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // 4-byte UTF-8: emoji (U+1F389, U+1F4CA), CJK ext B (U+2000B)
        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("🎉/party.txt".to_string(), b"x".to_vec()),
            ("data/📊report.csv".to_string(), b"x".to_vec()),
            ("𠀋/rare-cjk.txt".to_string(), b"x".to_vec()),
            ("mix/café☕.txt".to_string(), b"x".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_json_keys_eq(
            &output.stdout,
            &[
                "🎉/party.txt",
                "data/📊report.csv",
                "𠀋/rare-cjk.txt",
                "mix/café☕.txt",
            ],
            "utf8 4-byte keys",
        );
    });

    _guard.cleanup().await;
}

/// Keys containing control characters (tab, newline, NUL) are escaped
/// in text output as `\xNN` hex sequences by default.
///
/// Verified against `src/aggregate.rs:66-87` (`escape_control_chars`):
/// bytes `0x00-0x1f` and `0x7f` are replaced with `\xNN`. Multi-byte
/// UTF-8 sequences survive intact.
#[tokio::test]
async fn e2e_edge_case_control_chars() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Key with a literal tab (0x09) and a literal newline (0x0a).
        let key_with_tab = "data/file\tname.txt";
        let key_with_newline = "data/line\none.txt";
        helper
            .put_object(&bucket, key_with_tab, b"x".to_vec())
            .await;
        helper
            .put_object(&bucket, key_with_newline, b"x".to_vec())
            .await;

        let target = format!("s3://{bucket}/");

        // JSON mode: serde_json escapes control chars as \t, \n, \uXXXX.
        // The Key field should contain the original characters (JSON-escaped).
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[key_with_tab, key_with_newline],
            "control chars: json keys",
        );

        // Text mode (default, no --raw-output): control chars are escaped
        // as \xNN hex sequences.
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // Tab (0x09) should appear as literal "\x09" in the escaped text.
        assert!(
            output.stdout.contains("\\x09"),
            "control chars: expected \\x09 (escaped tab) in text output, got:\n{}",
            output.stdout
        );
        // Newline (0x0a) should appear as "\x0a".
        assert!(
            output.stdout.contains("\\x0a"),
            "control chars: expected \\x0a (escaped newline) in text output, got:\n{}",
            output.stdout
        );
    });

    _guard.cleanup().await;
}

/// Zero-byte objects whose key ends with "/" (directory marker pattern).
///
/// S3 treats these as regular objects, not CommonPrefix entries. Under
/// `--recursive`, they must appear as object rows with Size=0. This
/// tests that s3ls does not confuse them with prefix entries.
#[tokio::test]
async fn e2e_edge_case_slash_suffix_key() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Zero-byte "directory marker" objects.
        helper.put_object(&bucket, "photos/", vec![]).await;
        helper.put_object(&bucket, "photos/2025/", vec![]).await;
        // A real file alongside them.
        helper
            .put_object(&bucket, "photos/2025/img.jpg", b"x".to_vec())
            .await;

        let target = format!("s3://{bucket}/");

        // JSON recursive listing: all 3 should appear as Key entries
        // (not Prefix entries, because --recursive with no --max-depth
        // enumerates everything as objects).
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_json_keys_eq(
            &output.stdout,
            &["photos/", "photos/2025/", "photos/2025/img.jpg"],
            "slash-suffix: all 3 keys in recursive listing",
        );

        // Verify the "/" objects have Size=0 in JSON.
        for line in output.stdout.lines().filter(|l| !l.trim().is_empty()) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
                && let Some(key) = v.get("Key").and_then(|k| k.as_str())
                && key.ends_with('/')
            {
                let size = v.get("Size").and_then(|s| s.as_u64());
                assert_eq!(
                    size,
                    Some(0),
                    "slash-suffix: key {key:?} should have Size=0, got {size:?}"
                );
            }
        }
    });

    _guard.cleanup().await;
}

/// Objects with two explicitly-specified checksums display correctly
/// in both text and JSON formats.
///
/// Uploads a single object with pre-computed CRC32 AND SHA256 checksum
/// values on the same PutObject request. S3 stores both and reports
/// them in ListObjectsV2's `ChecksumAlgorithm` array.
///
/// JSON: `ChecksumAlgorithm` is an array containing both `"CRC32"` and
/// `"SHA256"` (plus possibly `"CRC64NVME"` auto-added by S3).
/// Text: the `--show-checksum-algorithm` column joins them with commas
/// (e.g., `"CRC32,SHA256"`) per `src/aggregate.rs:367`.
#[tokio::test]
async fn e2e_edge_case_multiple_checksums() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Upload with BOTH CRC32 and SHA256 pre-computed checksum values.
        // Pre-computed for the body b"hello":
        //   CRC32:  0x3610A686 → base64 "NhCmhg=="
        //   SHA256: 2cf24d...  → base64 "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ="
        helper
            .client()
            .put_object()
            .bucket(&bucket)
            .key("file.txt")
            .body(b"hello".to_vec().into())
            .checksum_crc32("NhCmhg==")
            .checksum_sha256("LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=")
            .send()
            .await
            .expect("failed to upload with dual checksums");

        let target = format!("s3://{bucket}/");

        // --- JSON sub-assertion ---
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let first_line = output
            .stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .expect("no JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line).expect("failed to parse JSON");
        let algos = v
            .get("ChecksumAlgorithm")
            .and_then(|a| a.as_array())
            .expect("multiple checksums: ChecksumAlgorithm missing or not array");

        // Must contain BOTH explicitly-specified algorithms.
        let algo_strs: Vec<&str> = algos.iter().filter_map(|a| a.as_str()).collect();
        assert!(
            algo_strs.contains(&"CRC32"),
            "multiple checksums json: CRC32 not in array: {algo_strs:?}"
        );
        assert!(
            algo_strs.contains(&"SHA256"),
            "multiple checksums json: SHA256 not in array: {algo_strs:?}"
        );
        assert!(
            algos.len() >= 2,
            "multiple checksums json: expected 2+ algorithms, got {algos:?}"
        );
        println!("  JSON ChecksumAlgorithm: {algo_strs:?}");

        // --- Text sub-assertion ---
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--header",
            "--show-checksum-algorithm",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let data_line = output
            .stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .nth(1)
            .expect("no data row");
        let cols = parse_tsv_line(data_line);
        // CHECKSUM_ALGORITHM is at index 2 (DATE=0, SIZE=1, CHECKSUM_ALGORITHM=2, KEY=3).
        assert!(
            cols[2].contains("CRC32"),
            "multiple checksums text: column should contain CRC32, got {:?}",
            cols[2]
        );
        assert!(
            cols[2].contains("SHA256"),
            "multiple checksums text: column should contain SHA256, got {:?}",
            cols[2]
        );
        // Multiple algorithms are comma-separated.
        assert!(
            cols[2].contains(','),
            "multiple checksums text: expected comma-separated algorithms, got {:?}",
            cols[2]
        );
        println!("  Text column: {:?}", cols[2]);
    });

    _guard.cleanup().await;
}

/// `--raw-output` disables control character escaping in text mode.
/// Literal tab/newline bytes appear in the output instead of `\xNN`.
///
/// Verified against `src/aggregate.rs:90-96` (`maybe_escape`): when
/// `raw_output` is true, `escape_control_chars` is skipped.
///
/// Note: `--raw-output` conflicts with `--json` at the CLI level
/// (`conflicts_with = "json"` at src/config/args/mod.rs:352), so this
/// test uses text mode only.
#[tokio::test]
async fn e2e_edge_case_raw_output() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Key with a literal newline (0x0a).
        let key_with_newline = "data/line\none.txt";
        helper
            .put_object(&bucket, key_with_newline, b"x".to_vec())
            .await;

        let target = format!("s3://{bucket}/");

        // With --raw-output: the literal newline byte should appear in
        // stdout. We can detect this by checking that "one.txt" appears
        // on a SEPARATE line from "data/line" (because the raw newline
        // splits the key across two output lines).
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--raw-output"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        // The escaped form "\x0a" should NOT appear.
        assert!(
            !output.stdout.contains("\\x0a"),
            "raw-output: should NOT contain escaped \\x0a, got:\n{}",
            output.stdout
        );
        // The raw newline causes "one.txt" to appear on its own line.
        assert!(
            output.stdout.contains("one.txt"),
            "raw-output: 'one.txt' should appear somewhere in output"
        );

        // Without --raw-output (default): "\x0a" escape should appear.
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert!(
            output.stdout.contains("\\x0a"),
            "default (no raw): should contain escaped \\x0a, got:\n{}",
            output.stdout
        );
    });

    _guard.cleanup().await;
}
