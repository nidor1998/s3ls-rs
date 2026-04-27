pub mod aligned;
pub mod aligned_formatter;
pub mod columns;
pub mod json;
pub mod one_line_formatter;
pub mod tsv;

use byte_unit::Byte;
use std::borrow::Cow;

use crate::types::{ListEntry, ListingStatistics};

pub trait EntryFormatter: Send {
    fn format_entry(&self, entry: &ListEntry) -> String;
    fn format_header(&self) -> Option<String>;
    fn format_summary(&self, stats: &ListingStatistics) -> String;
}

#[derive(Default)]
pub struct FormatOptions {
    pub human: bool,
    pub show_relative_path: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub show_is_latest: bool,
    pub show_owner: bool,
    pub show_restore_status: bool,
    pub all_versions: bool,
    pub prefix: Option<String>,
    /// When false (the default), control characters and ANSI escape
    /// sequences in S3-returned strings (keys, prefixes, owner fields)
    /// are replaced with `\xNN` hex escapes in text-mode output. This
    /// prevents a maliciously-named S3 object from forging phantom rows
    /// in tab-delimited output or injecting terminal escape sequences.
    /// Users who want byte-exact keys can pass `--raw-output` or use
    /// `--json` (JSON output always preserves the original bytes since
    /// serde_json escapes them safely).
    pub raw_output: bool,
    pub show_local_time: bool,
}

impl FormatOptions {
    pub fn from_display_config(
        display_config: &crate::config::DisplayConfig,
        prefix: Option<String>,
        all_versions: bool,
    ) -> Self {
        FormatOptions {
            human: display_config.human,
            show_relative_path: display_config.show_relative_path,
            show_etag: display_config.show_etag,
            show_storage_class: display_config.show_storage_class,
            show_checksum_algorithm: display_config.show_checksum_algorithm,
            show_checksum_type: display_config.show_checksum_type,
            show_is_latest: display_config.show_is_latest,
            show_owner: display_config.show_owner,
            show_restore_status: display_config.show_restore_status,
            all_versions,
            prefix,
            raw_output: display_config.raw_output,
            show_local_time: display_config.show_local_time,
        }
    }
}

/// Escape control characters and ESC in a string for safe text-mode
/// output. Replaces `\x00-\x1f` and `\x7f` with `\xNN` hex escape
/// notation. Returns `Cow::Borrowed` when no escaping is needed (the
/// common case) so most strings incur zero allocations.
///
/// This is applied to any user-visible string that originated from S3
/// (object keys, common prefixes, owner names, bucket names) so that a
/// maliciously-named object cannot inject newlines, tabs, or ANSI
/// escape sequences into the operator's terminal or break downstream
/// tab-delimited parsing.
pub(crate) fn escape_control_chars(s: &str) -> Cow<'_, str> {
    // Fast path: any control byte in the UTF-8 encoding of `s` is
    // itself a control character, because UTF-8 continuation bytes are
    // all `>= 0x80`. So a byte scan is sufficient to detect "no
    // escaping needed" and lets us return `Cow::Borrowed` zero-alloc.
    if !s.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Cow::Borrowed(s);
    }
    // Slow path: iterate by `char` so multi-byte UTF-8 sequences
    // survive intact. Iterating by byte and pushing each `b as char`
    // would silently corrupt non-ASCII characters.
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        let cp = ch as u32;
        if cp < 0x20 || cp == 0x7f {
            out.push_str(&format!("\\x{cp:02x}"));
        } else {
            out.push(ch);
        }
    }
    Cow::Owned(out)
}

/// Apply `escape_control_chars` only when `raw_output` is false.
pub(crate) fn maybe_escape<'a>(s: &'a str, opts: &FormatOptions) -> Cow<'a, str> {
    if opts.raw_output {
        Cow::Borrowed(s)
    } else {
        escape_control_chars(s)
    }
}

/// Split a size into (number, unit) for tab-delimited summary output.
/// Mirrors `format_size(size, true)` but returns the two parts separately.
pub(crate) fn format_size_split(size: u64) -> (String, String) {
    if size < 1024 {
        (size.to_string(), "bytes".to_string())
    } else {
        let byte = Byte::from_u64(size);
        let adjusted = byte.get_appropriate_unit(byte_unit::UnitType::Binary);
        // byte-unit formats as "5.4 MiB"; split on the space
        let s = format!("{adjusted:.1}");
        match s.split_once(' ') {
            Some((num, unit)) => (num.to_string(), unit.to_string()),
            None => (s, String::new()),
        }
    }
}

pub(crate) fn format_size(size: u64, human: bool) -> String {
    if human {
        let byte = Byte::from_u64(size);
        let adjusted = byte.get_appropriate_unit(byte_unit::UnitType::Binary);
        if size < 1024 {
            format!("{size}")
        } else {
            // byte-unit formats as "5.4 MiB" but spec requires "5.4MiB" (no space)
            let s = format!("{adjusted:.1}");
            s.replacen(' ', "", 1)
        }
    } else {
        size.to_string()
    }
}

pub(crate) fn format_key_display(entry_key: &str, opts: &FormatOptions) -> String {
    if !opts.show_relative_path {
        return entry_key.to_string();
    }
    if let Some(ref prefix) = opts.prefix {
        let after_prefix = entry_key.strip_prefix(prefix.as_str()).unwrap_or(entry_key);
        // Strip exactly one boundary '/' only when the user-provided prefix
        // didn't already end in one. trim_start_matches('/') would collapse
        // legitimate multi-slash residues — keys like "logs//report.csv"
        // relative to "logs/" must stay as "/report.csv", since S3 keys
        // are opaque strings and the second slash is part of the key.
        let stripped = if !prefix.ends_with('/') {
            after_prefix.strip_prefix('/').unwrap_or(after_prefix)
        } else {
            after_prefix
        };
        if stripped.is_empty() {
            entry_key.to_string()
        } else {
            stripped.to_string()
        }
    } else {
        entry_key.to_string()
    }
}

pub(crate) fn format_rfc3339(dt: &chrono::DateTime<chrono::Utc>, local: bool) -> String {
    if local {
        let local_dt: chrono::DateTime<chrono::Local> = dt.with_timezone(&chrono::Local);
        local_dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
    } else {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    }
}

/// Incrementally accumulate statistics for a single entry (streaming mode).
pub fn accumulate_statistics(entry: &ListEntry, stats: &mut ListingStatistics) {
    match entry {
        ListEntry::Object(obj) => {
            stats.total_objects += 1;
            stats.total_size += obj.size;
        }
        ListEntry::CommonPrefix(_) => {}
        ListEntry::DeleteMarker { .. } => {
            stats.total_delete_markers += 1;
        }
    }
}

pub fn compute_statistics(entries: &[ListEntry]) -> ListingStatistics {
    let mut total_objects: u64 = 0;
    let mut total_size: u64 = 0;
    let mut total_delete_markers: u64 = 0;

    for entry in entries {
        match entry {
            ListEntry::Object(obj) => {
                total_objects += 1;
                total_size += obj.size;
            }
            ListEntry::CommonPrefix(_) => {}
            ListEntry::DeleteMarker { .. } => {
                total_delete_markers += 1;
            }
        }
    }

    ListingStatistics {
        total_objects,
        total_size,
        total_delete_markers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};
    use chrono::TimeZone;

    fn make_entry_dated(key: &str, size: u64, year: i32, month: u32) -> ListEntry {
        ListEntry::Object(S3Object {
            key: key.to_string(),
            size,
            last_modified: chrono::Utc
                .with_ymd_and_hms(year, month, 1, 0, 0, 0)
                .unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        })
    }

    // ========================================================================
    // Control-char escaping in text output (injection defense)
    // ========================================================================

    #[test]
    fn escape_control_chars_passes_plain_ascii() {
        let s = "hello/world.txt";
        assert!(matches!(escape_control_chars(s), Cow::Borrowed(_)));
        assert_eq!(escape_control_chars(s).as_ref(), "hello/world.txt");
    }

    #[test]
    fn escape_control_chars_replaces_newline_and_tab() {
        assert_eq!(escape_control_chars("a\nb").as_ref(), "a\\x0ab");
        assert_eq!(escape_control_chars("a\tb").as_ref(), "a\\x09b");
        assert_eq!(escape_control_chars("a\rb").as_ref(), "a\\x0db");
    }

    #[test]
    fn escape_control_chars_replaces_ansi_escape() {
        assert_eq!(escape_control_chars("\x1b[2Jevil").as_ref(), "\\x1b[2Jevil");
    }

    #[test]
    fn escape_control_chars_replaces_del() {
        assert_eq!(escape_control_chars("a\x7fb").as_ref(), "a\\x7fb");
    }

    #[test]
    fn escape_control_chars_leaves_printable_utf8_alone() {
        let s = "héllo-日本語.txt";
        assert_eq!(escape_control_chars(s).as_ref(), s);
    }

    #[test]
    fn escape_control_chars_preserves_utf8_around_control_chars() {
        assert_eq!(escape_control_chars("日\n本").as_ref(), "日\\x0a本");
        assert_eq!(escape_control_chars("résumé\t").as_ref(), "résumé\\x09");
        assert_eq!(escape_control_chars("🦀\x1bend").as_ref(), "🦀\\x1bend");
    }

    #[test]
    fn format_size_split_below_1024() {
        use super::format_size_split;
        let (num, unit) = format_size_split(512);
        assert_eq!(num, "512");
        assert_eq!(unit, "bytes");
    }

    #[test]
    fn format_size_split_zero() {
        use super::format_size_split;
        let (num, unit) = format_size_split(0);
        assert_eq!(num, "0");
        assert_eq!(unit, "bytes");
    }

    #[test]
    fn format_size_split_above_1024() {
        use super::format_size_split;
        let (num, unit) = format_size_split(1_048_576); // 1 MiB
        assert_eq!(num, "1.0");
        assert_eq!(unit, "MiB");
    }

    #[test]
    fn format_size_split_exactly_1024() {
        use super::format_size_split;
        let (num, unit) = format_size_split(1024);
        assert_eq!(num, "1.0");
        assert_eq!(unit, "KiB");
    }

    // ========================================================================
    // format_key_display: --show-relative-path output
    // ========================================================================

    fn rel_opts(prefix: Option<&str>) -> FormatOptions {
        FormatOptions {
            show_relative_path: true,
            prefix: prefix.map(|s| s.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn format_key_display_returns_original_when_relative_path_disabled() {
        let opts = FormatOptions {
            show_relative_path: false,
            prefix: Some("logs/".to_string()),
            ..Default::default()
        };
        assert_eq!(format_key_display("logs/a.txt", &opts), "logs/a.txt");
    }

    #[test]
    fn format_key_display_returns_original_when_no_prefix_set() {
        let opts = rel_opts(None);
        assert_eq!(format_key_display("a/b.txt", &opts), "a/b.txt");
    }

    #[test]
    fn format_key_display_strips_prefix_with_trailing_slash() {
        let opts = rel_opts(Some("logs/"));
        assert_eq!(format_key_display("logs/a.txt", &opts), "a.txt");
    }

    #[test]
    fn format_key_display_strips_boundary_slash_when_prefix_lacks_trailing_slash() {
        let opts = rel_opts(Some("logs"));
        assert_eq!(format_key_display("logs/a.txt", &opts), "a.txt");
    }

    #[test]
    fn format_key_display_preserves_extra_leading_slash_when_prefix_has_trailing_slash() {
        // Regression for trim_start_matches('/'): used to collapse "/report.csv"
        // (the legitimate residue of "logs//report.csv" against "logs/") down
        // to "report.csv", losing key information.
        let opts = rel_opts(Some("logs/"));
        assert_eq!(format_key_display("logs//report.csv", &opts), "/report.csv");
    }

    #[test]
    fn format_key_display_preserves_extra_leading_slash_when_prefix_lacks_trailing_slash() {
        // prefix "logs", key "logs//report.csv": strip "logs" → "//report.csv",
        // then swallow exactly one boundary '/' → "/report.csv".
        let opts = rel_opts(Some("logs"));
        assert_eq!(format_key_display("logs//report.csv", &opts), "/report.csv");
    }

    #[test]
    fn format_key_display_falls_back_to_full_key_when_residue_is_empty() {
        let opts = rel_opts(Some("logs/"));
        assert_eq!(format_key_display("logs/", &opts), "logs/");
    }

    #[test]
    fn format_key_display_falls_back_when_residue_is_only_boundary_slash() {
        let opts = rel_opts(Some("logs"));
        assert_eq!(format_key_display("logs/", &opts), "logs/");
    }

    #[test]
    fn format_key_display_returns_original_when_key_does_not_start_with_prefix() {
        let opts = rel_opts(Some("logs/"));
        assert_eq!(format_key_display("other/a.txt", &opts), "other/a.txt");
    }

    #[test]
    fn compute_statistics_counts_correctly() {
        let entries = vec![
            make_entry_dated("a.txt", 100, 2024, 1),
            make_entry_dated("b.txt", 200, 2024, 2),
            ListEntry::CommonPrefix("logs/".to_string()),
            ListEntry::DeleteMarker {
                key: "c.txt".to_string(),
                version_info: crate::types::VersionInfo {
                    version_id: "v1".to_string(),
                    is_latest: true,
                },
                last_modified: chrono::Utc::now(),
                owner_display_name: None,
                owner_id: None,
            },
        ];
        let stats = compute_statistics(&entries);
        assert_eq!(stats.total_objects, 2);
        assert_eq!(stats.total_size, 300);
        assert_eq!(stats.total_delete_markers, 1);
    }
}
