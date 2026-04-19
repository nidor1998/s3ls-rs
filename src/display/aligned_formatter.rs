//! `EntryFormatter` that produces human-readable, space-padded rows.
//!
//! Selected by the pipeline when `--aligned` is set. Non-KEY columns
//! are padded to fixed widths defined in `crate::display::aligned`,
//! columns are joined by the two-space `SEP`, and the KEY column is
//! emitted unpadded at the end.

use crate::display::aligned::render_cols;
use crate::display::columns::{build_entry_cols, build_header_cols};
use crate::display::{EntryFormatter, FormatOptions, format_size_split};
use crate::types::{ListEntry, ListingStatistics};

pub struct AlignedFormatter {
    opts: FormatOptions,
}

impl AlignedFormatter {
    pub fn new(opts: FormatOptions) -> Self {
        Self { opts }
    }
}

impl EntryFormatter for AlignedFormatter {
    fn format_entry(&self, entry: &ListEntry) -> String {
        let (specs, key) = build_entry_cols(entry, &self.opts);
        render_cols(&specs, &key)
    }

    fn format_header(&self) -> Option<String> {
        let specs = build_header_cols(&self.opts);
        Some(render_cols(&specs, "KEY"))
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
    use crate::types::{ListEntry, S3Object, VersionInfo};
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

    // ===========================================================================
    // Object row layout
    // ===========================================================================

    #[test]
    fn format_text_aligned_basic_object() {
        use crate::display::aligned::{SEP, W_DATE, W_SIZE};
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        let date = "2024-01-01T00:00:00Z";
        let date_padded = format!("{date}{}", " ".repeat(W_DATE - date.chars().count()));
        let size_padded = format!("{}1234", " ".repeat(W_SIZE - 4));
        let expected = format!("{date_padded}{SEP}{size_padded}{SEP}readme.txt");
        assert_eq!(line, expected);
    }

    #[test]
    fn format_text_aligned_right_aligns_size_number() {
        use crate::display::aligned::{SEP, W_SIZE};
        let entry = make_entry_dated("f.txt", 42, 2024, 1);
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        let value = "42";
        let slot = format!("{}{value}{SEP}", " ".repeat(W_SIZE - value.chars().count()));
        assert!(line.contains(&slot), "got: {line:?}");
    }

    #[test]
    fn format_text_aligned_pre_marker_right_aligned() {
        use crate::display::aligned::{SEP, W_SIZE};
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        let value = "PRE";
        let slot = format!("{}{value}{SEP}", " ".repeat(W_SIZE - value.chars().count()));
        assert!(line.contains(&slot), "got: {line:?}");
        assert!(line.ends_with("logs/"));
    }

    #[test]
    fn format_text_aligned_delete_marker_right_aligned() {
        use crate::display::aligned::{SEP, W_SIZE};
        let entry = ListEntry::DeleteMarker {
            key: "k.txt".to_string(),
            version_info: VersionInfo {
                version_id: "v1".to_string(),
                is_latest: false,
            },
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            owner_display_name: None,
            owner_id: None,
        };
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        let value = "DELETE";
        let slot = format!("{}{value}{SEP}", " ".repeat(W_SIZE - value.chars().count()));
        assert!(line.contains(&slot), "got: {line:?}");
        assert!(line.ends_with("k.txt"));
    }

    #[test]
    fn format_text_aligned_overflow_preserves_value() {
        // An OwnerDisplayName longer than W_OWNER_DISPLAY_NAME (64)
        // must not be truncated.
        let big_name = "a".repeat(80);
        let entry = ListEntry::Object(S3Object {
            key: "f.txt".to_string(),
            size: 1,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: Some(big_name.clone()),
            owner_id: Some("z".to_string()),
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        });
        let fmt = AlignedFormatter::new(FormatOptions {
            show_owner: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains(&big_name), "got: {line:?}");
        assert!(line.ends_with("f.txt"));
    }

    #[test]
    fn format_text_aligned_escapes_before_padding() {
        let entry = ListEntry::Object(S3Object {
            key: "evil\nkey".to_string(),
            size: 1,
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
        });
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(!line.contains('\n'));
        assert!(line.ends_with("evil\\x0akey"));
    }

    // ===========================================================================
    // Header row layout
    // ===========================================================================

    #[test]
    fn format_text_aligned_header_padded() {
        use crate::display::aligned::{SEP, W_DATE, W_SIZE};
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let header = fmt.format_header().unwrap();
        let date_label = format!("DATE{}", " ".repeat(W_DATE - "DATE".len()));
        let size_label = format!("SIZE{}", " ".repeat(W_SIZE - "SIZE".len()));
        let expected = format!("{date_label}{SEP}{size_label}{SEP}KEY");
        assert_eq!(header, expected);
    }

    #[test]
    fn format_text_aligned_with_all_optional_columns() {
        use crate::display::aligned::{
            W_CHECKSUM_ALGORITHM, W_CHECKSUM_TYPE, W_DATE, W_ETAG, W_IS_LATEST,
            W_IS_RESTORE_IN_PROGRESS, W_OWNER_DISPLAY_NAME, W_OWNER_ID, W_RESTORE_EXPIRY_DATE,
            W_SIZE, W_STORAGE_CLASS, W_VERSION_ID,
        };
        let entry = ListEntry::Object(S3Object {
            key: "f.txt".to_string(),
            size: 10,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            e_tag: "\"abc\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec!["CRC32".to_string()],
            checksum_type: Some("FULL_OBJECT".to_string()),
            owner_display_name: Some("alice".to_string()),
            owner_id: Some("id-a".to_string()),
            is_restore_in_progress: Some(true),
            restore_expiry_date: Some("2024-02-01T00:00:00Z".to_string()),
            version_info: Some(VersionInfo {
                version_id: "v1".to_string(),
                is_latest: true,
            }),
        });
        let fmt = AlignedFormatter::new(FormatOptions {
            all_versions: true,
            show_storage_class: true,
            show_etag: true,
            show_checksum_algorithm: true,
            show_checksum_type: true,
            show_is_latest: true,
            show_owner: true,
            show_restore_status: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        // 12 columns + SEP per column + KEY at the end. The total
        // character count equals the sum of all widths + 2-char SEP
        // per column + the KEY length.
        let expected_prefix_len = W_DATE
            + 2
            + W_SIZE
            + 2
            + W_STORAGE_CLASS
            + 2
            + W_ETAG
            + 2
            + W_CHECKSUM_ALGORITHM
            + 2
            + W_CHECKSUM_TYPE
            + 2
            + W_VERSION_ID
            + 2
            + W_IS_LATEST
            + 2
            + W_OWNER_DISPLAY_NAME
            + 2
            + W_OWNER_ID
            + 2
            + W_IS_RESTORE_IN_PROGRESS
            + 2
            + W_RESTORE_EXPIRY_DATE
            + 2;
        assert!(line.ends_with("f.txt"));
        assert_eq!(
            line.chars().count(),
            expected_prefix_len + "f.txt".chars().count()
        );
    }

    // ===========================================================================
    // Summary line
    // ===========================================================================

    #[test]
    fn format_summary_aligned_uses_spaces() {
        let stats = crate::types::ListingStatistics {
            total_objects: 42,
            total_size: 5678901,
            total_delete_markers: 0,
        };
        let fmt = AlignedFormatter::new(FormatOptions {
            human: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal: 42 objects 5.4 MiB");
    }

    #[test]
    fn format_summary_aligned_non_human() {
        let stats = crate::types::ListingStatistics {
            total_objects: 3,
            total_size: 100,
            total_delete_markers: 0,
        };
        let fmt = AlignedFormatter::new(FormatOptions::default());
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal: 3 objects 100 bytes");
    }

    #[test]
    fn format_summary_aligned_with_versions() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 3,
        };
        let fmt = AlignedFormatter::new(FormatOptions {
            all_versions: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal: 10 objects 1024 bytes 3 delete markers");
    }
}
