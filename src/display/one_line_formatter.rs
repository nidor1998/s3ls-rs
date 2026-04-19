//! `EntryFormatter` that emits just the key (or prefix) per line.
//!
//! Selected by the pipeline when `-1` is set. Produces output
//! reminiscent of `ls -1` — one key per line, no columns, no header.
//! All `--show-*` flags are ignored. Common prefixes are emitted
//! alongside objects by default; combine with `--show-objects-only`
//! to suppress them.

use crate::display::{
    EntryFormatter, FormatOptions, format_key_display, format_size_split, maybe_escape,
};
use crate::types::{ListEntry, ListingStatistics};

pub struct OneLineFormatter {
    opts: FormatOptions,
}

impl OneLineFormatter {
    pub fn new(opts: FormatOptions) -> Self {
        Self { opts }
    }
}

impl EntryFormatter for OneLineFormatter {
    fn format_entry(&self, entry: &ListEntry) -> String {
        let displayed = format_key_display(entry.key(), &self.opts);
        maybe_escape(&displayed, &self.opts).into_owned()
    }

    fn format_header(&self) -> Option<String> {
        None
    }

    fn format_summary(&self, stats: &ListingStatistics) -> String {
        let (size_num, size_unit) = if self.opts.human {
            format_size_split(stats.total_size)
        } else {
            (stats.total_size.to_string(), "bytes".to_string())
        };
        let mut line = format!(
            "\nTotal: {} objects {} {}",
            stats.total_objects, size_num, size_unit
        );
        if self.opts.all_versions {
            line.push_str(&format!(" {} delete markers", stats.total_delete_markers));
        }
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{S3Object, VersionInfo};
    use chrono::TimeZone;

    fn make_object(key: &str) -> ListEntry {
        ListEntry::Object(S3Object {
            key: key.to_string(),
            size: 100,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
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

    #[test]
    fn one_line_object_emits_only_key() {
        let fmt = OneLineFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&make_object("data/readme.txt"));
        assert_eq!(line, "data/readme.txt");
    }

    #[test]
    fn one_line_common_prefix_emits_prefix() {
        let fmt = OneLineFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&ListEntry::CommonPrefix("logs/".to_string()));
        assert_eq!(line, "logs/");
    }

    #[test]
    fn one_line_delete_marker_emits_key() {
        let fmt = OneLineFormatter::new(FormatOptions::default());
        let entry = ListEntry::DeleteMarker {
            key: "deleted.txt".to_string(),
            version_info: VersionInfo {
                version_id: "v1".to_string(),
                is_latest: true,
            },
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            owner_display_name: None,
            owner_id: None,
        };
        let line = fmt.format_entry(&entry);
        assert_eq!(line, "deleted.txt");
    }

    #[test]
    fn one_line_ignores_show_options() {
        // All --show-* options should have no effect on output.
        let fmt = OneLineFormatter::new(FormatOptions {
            show_etag: true,
            show_storage_class: true,
            show_checksum_algorithm: true,
            show_checksum_type: true,
            show_is_latest: true,
            show_owner: true,
            show_restore_status: true,
            all_versions: true,
            human: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&make_object("file.txt"));
        assert_eq!(line, "file.txt");
    }

    #[test]
    fn one_line_header_is_none() {
        let fmt = OneLineFormatter::new(FormatOptions::default());
        assert!(fmt.format_header().is_none());
    }

    #[test]
    fn one_line_escapes_control_chars() {
        let fmt = OneLineFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&make_object("evil\nkey"));
        assert!(!line.contains('\n'));
        assert_eq!(line, "evil\\x0akey");
    }

    #[test]
    fn one_line_raw_output_preserves_bytes() {
        let fmt = OneLineFormatter::new(FormatOptions {
            raw_output: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&make_object("evil\nkey"));
        assert_eq!(line, "evil\nkey");
    }

    #[test]
    fn one_line_respects_show_relative_path() {
        let fmt = OneLineFormatter::new(FormatOptions {
            show_relative_path: true,
            prefix: Some("data/2024/".to_string()),
            ..Default::default()
        });
        let line = fmt.format_entry(&make_object("data/2024/report.csv"));
        assert_eq!(line, "report.csv");
    }

    #[test]
    fn one_line_summary_uses_plain_spaces() {
        let stats = crate::types::ListingStatistics {
            total_objects: 42,
            total_size: 100,
            total_delete_markers: 0,
        };
        let fmt = OneLineFormatter::new(FormatOptions::default());
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal: 42 objects 100 bytes");
    }

    #[test]
    fn one_line_summary_with_human_and_versions() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 5_678_901,
            total_delete_markers: 3,
        };
        let fmt = OneLineFormatter::new(FormatOptions {
            human: true,
            all_versions: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal: 10 objects 5.4 MiB 3 delete markers");
    }
}
