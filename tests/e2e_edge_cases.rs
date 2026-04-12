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
