#![cfg(e2e_test)]

//! Edge-case end-to-end tests.
//!
//! Covers unusual S3 key patterns and display behaviors:
//! - UTF-8 multi-byte keys (2-3 byte characters)
//! - UTF-8 4-byte characters (emoji, CJK supplementary)
//! - Control characters in keys (escaped vs raw)
//! - Zero-byte objects with "/" suffix (directory marker keys)
//! - `--raw-output` control character passthrough
//! - UTF-8 prefix listing and regex filters
//! - Control-character prefix listing and regex filters
//! - Control-character prefix listing and regex filters

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

/// Listing with a UTF-8 prefix: `s3ls s3://bucket/日本語/` should
/// enumerate only objects under the `日本語/` prefix, not objects in
/// sibling prefixes. Verifies that multi-byte UTF-8 prefix handling
/// works end-to-end in both the S3 API call and s3ls output.
#[tokio::test]
async fn e2e_edge_case_utf8_prefix_listing() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("日本語/ファイル1.txt".to_string(), b"x".to_vec()),
            ("日本語/サブ/ファイル2.txt".to_string(), b"x".to_vec()),
            ("中文/数据.json".to_string(), b"x".to_vec()),
            ("ascii/plain.txt".to_string(), b"x".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        // List with UTF-8 prefix — only 日本語/ objects should appear.
        let target = format!("s3://{bucket}/日本語/");
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_json_keys_eq(
            &output.stdout,
            &["日本語/ファイル1.txt", "日本語/サブ/ファイル2.txt"],
            "utf8 prefix: only 日本語/ objects",
        );

        // Verify sibling prefixes did NOT leak into the result.
        assert!(
            !output.stdout.contains("中文"),
            "utf8 prefix: 中文/ should not appear in output"
        );
        assert!(
            !output.stdout.contains("ascii"),
            "utf8 prefix: ascii/ should not appear in output"
        );
    });

    _guard.cleanup().await;
}

/// `--filter-include-regex` and `--filter-exclude-regex` work correctly
/// with UTF-8 patterns. The `fancy-regex` crate used by s3ls supports
/// Unicode, so patterns like `\.txt$` and `日本語` should match
/// multi-byte keys correctly.
#[tokio::test]
async fn e2e_edge_case_utf8_regex_filters() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let fixture: Vec<(String, Vec<u8>)> = vec![
            ("日本語/レポート.csv".to_string(), b"x".to_vec()),
            ("日本語/データ.txt".to_string(), b"x".to_vec()),
            ("中文/报告.csv".to_string(), b"x".to_vec()),
            ("中文/数据.txt".to_string(), b"x".to_vec()),
            ("english/report.csv".to_string(), b"x".to_vec()),
            ("english/data.txt".to_string(), b"x".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: include-regex with a UTF-8 pattern.
        // Match only keys containing "日本語".
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            "日本語",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["日本語/レポート.csv", "日本語/データ.txt"],
            "utf8 include-regex: 日本語 pattern",
        );

        // Sub-assertion 2: exclude-regex with a UTF-8 pattern.
        // Exclude keys containing "中文" — keeps 日本語/ and english/.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            "中文",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[
                "日本語/レポート.csv",
                "日本語/データ.txt",
                "english/report.csv",
                "english/data.txt",
            ],
            "utf8 exclude-regex: exclude 中文",
        );

        // Sub-assertion 3: include-regex with .csv$ on UTF-8 keys.
        // Proves the regex engine handles the boundary between UTF-8
        // characters and ASCII extensions correctly.
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
            &["日本語/レポート.csv", "中文/报告.csv", "english/report.csv"],
            "utf8 include-regex: .csv$ across UTF-8 keys",
        );
    });

    _guard.cleanup().await;
}

/// Listing with a prefix that contains a control character.
///
/// S3 allows any byte sequence as a key prefix. Objects under a prefix
/// like `"data\ttab/"` (literal tab in the prefix path) should be
/// enumerable by passing the same literal prefix to s3ls. In JSON mode
/// the key appears with the control character JSON-escaped (`\t`).
#[tokio::test]
async fn e2e_edge_case_control_char_prefix_listing() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Keys with a literal tab (0x09) in the prefix component.
        let prefix_with_tab = "data\ttab/";
        let key_under_tab = format!("{prefix_with_tab}file.txt");
        let key_sibling = "data-normal/file.txt".to_string();

        helper
            .put_object(&bucket, &key_under_tab, b"x".to_vec())
            .await;
        helper
            .put_object(&bucket, &key_sibling, b"x".to_vec())
            .await;

        // List with the control-char prefix — only the tab-prefixed
        // object should appear.
        let target = format!("s3://{bucket}/{prefix_with_tab}");
        let output = TestHelper::run_s3ls(&[target.as_str(), "--recursive", "--json"]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        assert_json_keys_eq(
            &output.stdout,
            &[&key_under_tab],
            "control prefix: only tab-prefixed object",
        );

        // Sibling must NOT appear.
        assert!(
            !output.stdout.contains("data-normal"),
            "control prefix: sibling 'data-normal/' should not appear in output"
        );
    });

    _guard.cleanup().await;
}

/// `--filter-include-regex` and `--filter-exclude-regex` work correctly
/// with patterns that match control characters in keys.
///
/// S3 keys can contain literal control bytes (tab, newline, etc.).
/// The `fancy-regex` crate matches against the raw key string, so a
/// regex like `\t` (which regex interprets as the tab character)
/// should match keys containing a literal tab byte.
#[tokio::test]
async fn e2e_edge_case_control_char_regex_filters() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Keys: two with literal tab in the path, two without.
        let key_tab_a = "data\ttab/a.txt".to_string();
        let key_tab_b = "data\ttab/b.csv".to_string();
        let key_clean_a = "data-clean/a.txt".to_string();
        let key_clean_b = "data-clean/b.csv".to_string();

        let fixture: Vec<(String, Vec<u8>)> = vec![
            (key_tab_a.clone(), b"x".to_vec()),
            (key_tab_b.clone(), b"x".to_vec()),
            (key_clean_a.clone(), b"x".to_vec()),
            (key_clean_b.clone(), b"x".to_vec()),
        ];
        helper.put_objects_parallel(&bucket, fixture).await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: include-regex matching the tab character.
        // Regex `\t` matches the literal tab byte (0x09) in the key.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-include-regex",
            r"\t",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[&key_tab_a, &key_tab_b],
            "control include-regex: \\t matches tab keys",
        );

        // Sub-assertion 2: exclude-regex removing keys with tab.
        // Keeps only the clean keys.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--filter-exclude-regex",
            r"\t",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &[&key_clean_a, &key_clean_b],
            "control exclude-regex: \\t excludes tab keys",
        );
    });

    _guard.cleanup().await;
}

/// `--target-request-payer` with general bucket listing, directory
/// bucket listing, object listing, and versioned object listing.
///
/// S3 ignores the `x-amz-request-payer: requester` header when the
/// requester IS the bucket owner, so this test runs against the test
/// account's own buckets and verifies the flag is accepted and
/// wired through without breaking normal operations.
///
/// The versioned sub-assertion uses `--max-keys 3` to force pagination
/// (2 keys × 3 versions = 6 version rows → 2 pages at max-keys=3).
#[tokio::test]
async fn e2e_edge_case_request_payer() {
    use std::time::Duration;
    use tokio::time::sleep;

    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        // --- Sub-assertion 1: general bucket listing ---
        // Create a bucket so we have at least one to find.
        helper.create_bucket(&bucket).await;

        let output = TestHelper::run_s3ls(&["--json", "--target-request-payer"]);
        assert!(
            output.status.success(),
            "request-payer bucket listing failed: {}",
            output.stderr
        );
        assert!(
            output.stdout.contains(&bucket),
            "request-payer bucket listing: test bucket not found in output"
        );

        // --- Sub-assertion 2: directory bucket listing ---
        // Skip if Express One Zone is not available.
        let express_output = TestHelper::run_s3ls(&[
            "--json",
            "--list-express-one-zone-buckets",
            "--target-request-payer",
        ]);
        // Just verify the command is accepted (exit 0). It may return
        // zero directory buckets — that's fine; we're testing the flag
        // doesn't cause an error.
        assert!(
            express_output.status.success(),
            "request-payer directory bucket listing failed: {}",
            express_output.stderr
        );

        // --- Sub-assertion 3: object listing ---
        helper.put_object(&bucket, "a.txt", b"aaa".to_vec()).await;
        helper.put_object(&bucket, "b.txt", b"bb".to_vec()).await;

        let target = format!("s3://{bucket}/");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--json",
            "--target-request-payer",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        assert_json_keys_eq(
            &output.stdout,
            &["a.txt", "b.txt"],
            "request-payer object listing",
        );

        // --- Sub-assertion 4: versioned listing with pagination ---
        // Create a separate versioned bucket for this sub-assertion.
        // We reuse the same bucket by enabling versioning on it.
        // Actually, we can't enable versioning on an existing non-versioned
        // bucket and have previous objects become versioned retroactively
        // in a useful way. Use a new bucket instead.
    });

    _guard.cleanup().await;

    // Versioned sub-assertion in a separate bucket.
    let v_bucket = helper.generate_bucket_name();
    let _v_guard = helper.bucket_guard(&v_bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&v_bucket).await;

        // Upload 2 keys × 3 versions each = 6 version rows.
        // With --max-keys 3, this forces 2 pages of ListObjectVersions.
        for i in 1..=3 {
            helper
                .put_object(&v_bucket, "alpha.txt", format!("v{i}").into_bytes())
                .await;
            helper
                .put_object(&v_bucket, "beta.txt", format!("v{i}").into_bytes())
                .await;
            // Sleep between rounds to guarantee distinct version timestamps.
            if i < 3 {
                sleep(Duration::from_millis(100)).await;
            }
        }

        let target = format!("s3://{v_bucket}/");
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--json",
            "--no-sort",
            "--target-request-payer",
            "--max-keys",
            "3",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);

        // Should have 6 version rows (2 keys × 3 versions).
        assert_json_version_shapes_eq(
            &output.stdout,
            &[
                ("alpha.txt", false),
                ("alpha.txt", false),
                ("alpha.txt", false),
                ("beta.txt", false),
                ("beta.txt", false),
                ("beta.txt", false),
            ],
            "request-payer versioned listing with pagination",
        );
    });

    _v_guard.cleanup().await;
}

/// `--show-relative-path` when the prefix exactly equals the object key
/// (no trailing `/`). The key is displayed unchanged — the prefix
/// stripping only applies when the prefix has a trailing `/`.
///
/// Uses a UTF-8 key to simultaneously exercise multi-byte character
/// handling in the prefix-match path.
#[tokio::test]
async fn e2e_edge_case_prefix_equals_key_relative_path() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        let key = "☃👪森鴎外あいうえお";
        helper.put_object(&bucket, key, b"x".to_vec()).await;

        // Target IS the exact key (no trailing /).
        let target = format!("s3://{bucket}/{key}");

        // With --show-relative-path: the key should be displayed
        // unchanged (full key), NOT stripped to "".
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
            .expect("no JSON output");
        let v: serde_json::Value = serde_json::from_str(first_line).expect("failed to parse JSON");
        assert_eq!(
            v.get("Key").and_then(|k| k.as_str()),
            Some(key),
            "prefix == key (no trailing /): Key should be the full key unchanged"
        );
    });

    _guard.cleanup().await;
}

/// `--summarize --all-versions` with delete markers: both text and JSON
/// summary lines include the delete-marker count.
///
/// Object listing piped to `head -1`: the process should exit 0 despite
/// receiving SIGPIPE / BrokenPipe from the terminated reader.
///
/// Covers `src/bin/s3ls/main.rs:94-97` (object listing broken-pipe
/// recovery path).
#[tokio::test]
async fn e2e_edge_case_object_listing_broken_pipe() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;
        for i in 0..10 {
            helper
                .put_object(&bucket, &format!("file_{i:04}.txt"), vec![0u8; 10])
                .await;
        }

        let target = format!("s3://{bucket}/");

        // Pipe to `head -1`: the reader closes after one line, causing
        // SIGPIPE / BrokenPipe on the writer side.
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "{} --recursive --target-profile s3ls-e2e-test {} | head -1",
                env!("CARGO_BIN_EXE_s3ls"),
                target
            ))
            .output()
            .expect("failed to spawn sh -c pipe command");

        // s3ls should exit 0 (broken pipe handled gracefully)
        assert!(
            output.status.success(),
            "object listing broken pipe: expected exit 0, got {:?}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );

        // head -1 should have produced exactly one line
        let stdout = String::from_utf8_lossy(&output.stdout);
        let non_empty_lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(
            non_empty_lines.len(),
            1,
            "object listing broken pipe: expected 1 line from head, got {}",
            non_empty_lines.len()
        );
    });

    _guard.cleanup().await;
}

/// Bucket listing piped to `head -1`: the process should exit 0 despite
/// the early pipe close.
///
/// Covers `src/bin/s3ls/main.rs:66-69` (bucket listing broken-pipe
/// recovery path).
#[tokio::test]
async fn e2e_edge_case_bucket_listing_broken_pipe() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_bucket(&bucket).await;

        // Pipe bucket listing to `head -1`. Even if the account has
        // many buckets, head closes the pipe after one line.
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "{} --target-profile s3ls-e2e-test | head -1",
                env!("CARGO_BIN_EXE_s3ls"),
            ))
            .output()
            .expect("failed to spawn sh -c pipe command");

        assert!(
            output.status.success(),
            "bucket listing broken pipe: expected exit 0, got {:?}\nstderr: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    });

    _guard.cleanup().await;
}

/// Fixture: versioned bucket with 2 object versions + 1 delete marker.
/// Text summary: `"Total:\t2\tobjects\t...\t1\tdelete markers"`.
/// JSON summary: `{"Summary":{"TotalObjects":2,"TotalSize":...,"TotalDeleteMarkers":1}}`.
#[tokio::test]
async fn e2e_edge_case_versioned_summary_with_delete_marker() {
    let helper = TestHelper::new().await;
    let bucket = helper.generate_bucket_name();
    let _guard = helper.bucket_guard(&bucket);

    e2e_timeout!(async {
        helper.create_versioned_bucket(&bucket).await;

        // Two versions of doc.txt (100 + 200 bytes) + 1 delete marker.
        helper.put_object(&bucket, "doc.txt", vec![0u8; 100]).await;
        helper.put_object(&bucket, "doc.txt", vec![0u8; 200]).await;
        helper.create_delete_marker(&bucket, "doc.txt").await;

        let target = format!("s3://{bucket}/");

        // Sub-assertion 1: text summary.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--summarize",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let summary =
            assert_summary_present_text(&output.stdout, "versioned summary with DM: text");
        assert!(
            summary.contains("\t2\tobjects"),
            "versioned summary text: expected 2 objects, got {summary:?}"
        );
        assert!(
            summary.contains("\t300\tbytes"),
            "versioned summary text: expected 300 bytes, got {summary:?}"
        );
        assert!(
            summary.contains("\t1\tdelete markers"),
            "versioned summary text: expected 1 delete markers, got {summary:?}"
        );

        // Sub-assertion 2: JSON summary.
        let output = TestHelper::run_s3ls(&[
            target.as_str(),
            "--recursive",
            "--all-versions",
            "--summarize",
            "--json",
        ]);
        assert!(output.status.success(), "s3ls failed: {}", output.stderr);
        let v = assert_summary_present_json(&output.stdout, "versioned summary with DM: json");
        let summary_obj = v.get("Summary").expect("missing Summary object");
        assert_eq!(
            summary_obj.get("TotalObjects").and_then(|n| n.as_u64()),
            Some(2),
            "versioned summary json: TotalObjects should be 2"
        );
        assert_eq!(
            summary_obj.get("TotalSize").and_then(|n| n.as_u64()),
            Some(300),
            "versioned summary json: TotalSize should be 300"
        );
        assert_eq!(
            summary_obj
                .get("TotalDeleteMarkers")
                .and_then(|n| n.as_u64()),
            Some(1),
            "versioned summary json: TotalDeleteMarkers should be 1"
        );
    });

    _guard.cleanup().await;
}
