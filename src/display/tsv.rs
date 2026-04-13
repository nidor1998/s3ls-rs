use crate::display::{
    EntryFormatter, FormatOptions, format_key_display, format_rfc3339, format_size,
    format_size_split, maybe_escape,
};
use crate::types::{ListEntry, ListingStatistics};

pub struct TsvFormatter {
    opts: FormatOptions,
}

impl TsvFormatter {
    pub fn new(opts: FormatOptions) -> Self {
        Self { opts }
    }
}

impl EntryFormatter for TsvFormatter {
    fn format_entry(&self, entry: &ListEntry) -> String {
        let opts = &self.opts;
        let mut cols: Vec<String> = Vec::new();

        match entry {
            ListEntry::CommonPrefix(_) => {
                // date
                cols.push(String::new());
                // size
                cols.push("PRE".to_string());
                // optional columns
                if opts.show_storage_class {
                    cols.push(String::new());
                }
                if opts.show_etag {
                    cols.push(String::new());
                }
                if opts.show_checksum_algorithm {
                    cols.push(String::new());
                }
                if opts.show_checksum_type {
                    cols.push(String::new());
                }
                // In --all-versions mode, Object and DeleteMarker rows include a
                // version_id column (and is_latest if enabled). CommonPrefix has
                // neither, so emit placeholders to keep columns aligned.
                if opts.all_versions {
                    cols.push(String::new());
                    if opts.show_is_latest {
                        cols.push(String::new());
                    }
                }
                if opts.show_owner {
                    cols.push(String::new());
                    cols.push(String::new());
                }
                if opts.show_restore_status {
                    cols.push(String::new());
                    cols.push(String::new());
                }
                // key (escape control chars in text mode to avoid injection)
                cols.push(maybe_escape(&format_key_display(entry.key(), opts), opts).into_owned());
            }
            ListEntry::Object(obj) => {
                cols.push(format_rfc3339(&obj.last_modified, opts.show_local_time));
                cols.push(format_size(obj.size, opts.human));
                if opts.show_storage_class {
                    cols.push(
                        obj.storage_class
                            .as_deref()
                            .unwrap_or("STANDARD")
                            .to_string(),
                    );
                }
                if opts.show_etag {
                    cols.push(obj.e_tag.trim_matches('"').to_string());
                }
                if opts.show_checksum_algorithm {
                    cols.push(obj.checksum_algorithm.join(","));
                }
                if opts.show_checksum_type {
                    cols.push(obj.checksum_type.as_deref().unwrap_or("").to_string());
                }
                if let Some(vid) = obj.version_id() {
                    cols.push(vid.to_string());
                }
                if opts.show_is_latest && obj.version_id().is_some() {
                    cols.push(if obj.is_latest() {
                        "LATEST".to_string()
                    } else {
                        "NOT_LATEST".to_string()
                    });
                }
                if opts.show_owner {
                    cols.push(
                        maybe_escape(obj.owner_display_name.as_deref().unwrap_or(""), opts)
                            .into_owned(),
                    );
                    cols.push(
                        maybe_escape(obj.owner_id.as_deref().unwrap_or(""), opts).into_owned(),
                    );
                }
                if opts.show_restore_status {
                    cols.push(
                        obj.is_restore_in_progress
                            .map(|b| b.to_string())
                            .unwrap_or_default(),
                    );
                    cols.push(obj.restore_expiry_date.as_deref().unwrap_or("").to_string());
                }
                cols.push(maybe_escape(&format_key_display(entry.key(), opts), opts).into_owned());
            }
            ListEntry::DeleteMarker {
                key,
                version_info,
                last_modified,
                owner_display_name,
                owner_id,
            } => {
                cols.push(format_rfc3339(last_modified, opts.show_local_time));
                cols.push("DELETE".to_string());
                if opts.show_storage_class {
                    cols.push(String::new());
                }
                if opts.show_etag {
                    cols.push(String::new());
                }
                if opts.show_checksum_algorithm {
                    cols.push(String::new());
                }
                if opts.show_checksum_type {
                    cols.push(String::new());
                }
                cols.push(version_info.version_id.clone());
                if opts.show_is_latest {
                    cols.push(if version_info.is_latest {
                        "LATEST".to_string()
                    } else {
                        "NOT_LATEST".to_string()
                    });
                }
                if opts.show_owner {
                    cols.push(
                        maybe_escape(owner_display_name.as_deref().unwrap_or(""), opts)
                            .into_owned(),
                    );
                    cols.push(maybe_escape(owner_id.as_deref().unwrap_or(""), opts).into_owned());
                }
                if opts.show_restore_status {
                    // Delete markers have no restore status — leave empty.
                    cols.push(String::new());
                    cols.push(String::new());
                }
                cols.push(maybe_escape(&format_key_display(key, opts), opts).into_owned());
            }
        }

        cols.join("\t")
    }

    fn format_header(&self) -> Option<String> {
        let opts = &self.opts;
        let mut cols: Vec<&str> = Vec::new();
        cols.push("DATE");
        cols.push("SIZE");
        if opts.show_storage_class {
            cols.push("STORAGE_CLASS");
        }
        if opts.show_etag {
            cols.push("ETAG");
        }
        if opts.show_checksum_algorithm {
            cols.push("CHECKSUM_ALGORITHM");
        }
        if opts.show_checksum_type {
            cols.push("CHECKSUM_TYPE");
        }
        if opts.all_versions {
            cols.push("VERSION_ID");
        }
        if opts.show_is_latest {
            cols.push("IS_LATEST");
        }
        if opts.show_owner {
            cols.push("OWNER_DISPLAY_NAME");
            cols.push("OWNER_ID");
        }
        if opts.show_restore_status {
            cols.push("IS_RESTORE_IN_PROGRESS");
            cols.push("RESTORE_EXPIRY_DATE");
        }
        cols.push("KEY");
        Some(cols.join("\t"))
    }

    fn format_summary(&self, stats: &ListingStatistics) -> String {
        let (size_num, size_unit) = if self.opts.human {
            format_size_split(stats.total_size)
        } else {
            (stats.total_size.to_string(), "bytes".to_string())
        };
        let mut line = format!(
            "\nTotal:\t{}\tobjects\t{}\t{}",
            stats.total_objects, size_num, size_unit
        );
        if self.opts.all_versions {
            line.push_str(&format!("\t{}\tdelete markers", stats.total_delete_markers));
        }
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{S3Object, VersionInfo};
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

    fn make_entry_with_checksums(key: &str, checksums: Vec<&str>) -> ListEntry {
        ListEntry::Object(S3Object {
            key: key.to_string(),
            size: 100,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: checksums.iter().map(|s| s.to_string()).collect(),
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        })
    }

    #[test]
    fn format_text_basic_object() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(line.contains("2024-01-01T00:00:00Z"));
        assert!(line.contains("1234"));
        assert!(line.ends_with("readme.txt"));
    }

    #[test]
    fn format_text_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(line.contains("PRE"));
        assert!(line.ends_with("logs/"));
    }

    #[test]
    fn format_text_human_size() {
        let entry = make_entry_dated("data.csv", 5678901, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions {
            human: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains("5.4MiB"));
        assert!(line.ends_with("data.csv"));
    }

    #[test]
    fn format_text_extra_columns_before_key() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions {
            show_etag: true,
            show_storage_class: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields.len(), 5);
        assert!(fields[0].contains("2024-01-01"));
        assert_eq!(fields[1], "1234");
        assert_eq!(fields[2], "STANDARD");
        assert_eq!(fields[3], "e");
        assert_eq!(fields[4], "readme.txt");
    }

    #[test]
    fn format_text_versioned_object() {
        let entry = ListEntry::Object(S3Object {
            key: "readme.txt".to_string(),
            size: 1234,
            last_modified: chrono::Utc
                .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
                .unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: Some(VersionInfo {
                version_id: "abc123-version-id".to_string(),
                is_latest: true,
            }),
        });
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        let size_pos = line.find("1234").unwrap();
        let vid_pos = line.find("abc123-version-id").unwrap();
        let key_pos = line.rfind("readme.txt").unwrap();
        assert!(size_pos < vid_pos, "size before version_id");
        assert!(vid_pos < key_pos, "version_id before key");
    }

    #[test]
    fn format_text_common_prefix_aligns_with_versioned_object() {
        let obj = ListEntry::Object(S3Object {
            key: "logs/file.txt".to_string(),
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
            version_info: Some(VersionInfo {
                version_id: "v1".to_string(),
                is_latest: true,
            }),
        });
        let prefix = ListEntry::CommonPrefix("logs/".to_string());

        let fmt = TsvFormatter::new(FormatOptions {
            all_versions: true,
            show_is_latest: true,
            ..Default::default()
        });
        let obj_line = fmt.format_entry(&obj);
        let prefix_line = fmt.format_entry(&prefix);
        let obj_cols: Vec<&str> = obj_line.split('\t').collect();
        let prefix_cols: Vec<&str> = prefix_line.split('\t').collect();
        assert_eq!(
            obj_cols.len(),
            prefix_cols.len(),
            "column count mismatch between Object ({:?}) and CommonPrefix ({:?})",
            obj_cols,
            prefix_cols
        );
    }

    #[test]
    fn format_text_common_prefix_no_version_placeholder_without_all_versions() {
        let obj = make_entry_dated("file.txt", 100, 2024, 1);
        let prefix = ListEntry::CommonPrefix("logs/".to_string());

        let fmt = TsvFormatter::new(FormatOptions::default());
        let obj_line = fmt.format_entry(&obj);
        let prefix_line = fmt.format_entry(&prefix);
        let obj_cols: Vec<&str> = obj_line.split('\t').collect();
        let prefix_cols: Vec<&str> = prefix_line.split('\t').collect();
        assert_eq!(obj_cols.len(), prefix_cols.len());
    }

    #[test]
    fn format_text_delete_marker() {
        let entry = ListEntry::DeleteMarker {
            key: "readme.txt".to_string(),
            version_info: VersionInfo {
                version_id: "def456-version-id".to_string(),
                is_latest: false,
            },
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 16, 9, 0, 0).unwrap(),
            owner_display_name: None,
            owner_id: None,
        };
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(line.contains("2024-01-16T09:00:00Z"));
        assert!(line.contains("DELETE"));
        assert!(!line.contains("(delete marker)"));
        let delete_pos = line.find("DELETE").unwrap();
        let vid_pos = line.find("def456-version-id").unwrap();
        let key_pos = line.rfind("readme.txt").unwrap();
        assert!(delete_pos < vid_pos, "DELETE before version_id");
        assert!(vid_pos < key_pos, "version_id before key");
    }

    #[test]
    fn format_text_delete_marker_emits_owner_when_show_owner() {
        let entry = ListEntry::DeleteMarker {
            key: "readme.txt".to_string(),
            version_info: VersionInfo {
                version_id: "v1".to_string(),
                is_latest: false,
            },
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            owner_display_name: Some("alice".to_string()),
            owner_id: Some("id-123".to_string()),
        };
        let fmt = TsvFormatter::new(FormatOptions {
            all_versions: true,
            show_owner: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(
            line.contains("alice"),
            "expected owner display name in output, got: {line:?}"
        );
        assert!(
            line.contains("id-123"),
            "expected owner id in output, got: {line:?}"
        );
    }

    #[test]
    fn format_text_escapes_malicious_key_by_default() {
        let evil_key = "innocent.txt\n2024-01-01T00:00:00Z\t0\tphantom.txt";
        let entry = ListEntry::Object(S3Object {
            key: evil_key.to_string(),
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
        });
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(
            !line.contains('\n'),
            "newline should have been escaped, got: {line:?}"
        );
        assert!(line.contains("\\x0a"));
    }

    #[test]
    fn format_text_preserves_malicious_key_when_raw_output() {
        let evil_key = "evil\nkey";
        let entry = ListEntry::Object(S3Object {
            key: evil_key.to_string(),
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
        });
        let fmt = TsvFormatter::new(FormatOptions {
            raw_output: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains("evil\nkey"));
    }

    #[test]
    fn format_text_escapes_owner_fields() {
        let entry = ListEntry::Object(S3Object {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: Some("alice\x1b[31m".to_string()),
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        });
        let fmt = TsvFormatter::new(FormatOptions {
            show_owner: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(!line.contains('\x1b'));
        assert!(line.contains("\\x1b"));
    }

    #[test]
    fn format_text_escapes_delete_marker_owner() {
        let entry = ListEntry::DeleteMarker {
            key: "file.txt".to_string(),
            version_info: VersionInfo {
                version_id: "v1".to_string(),
                is_latest: false,
            },
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            owner_display_name: Some("alice\nbob".to_string()),
            owner_id: Some("id\tnext".to_string()),
        };
        let fmt = TsvFormatter::new(FormatOptions {
            all_versions: true,
            show_owner: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(!line.contains('\n'));
        assert!(line.contains("alice\\x0abob"));
        assert!(line.contains("id\\x09next"));
    }

    #[test]
    fn format_text_escapes_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs\n/evil/".to_string());
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(!line.contains('\n'));
        assert!(line.contains("logs\\x0a/evil/"));
    }

    #[test]
    fn format_text_local_time() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions {
            show_local_time: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        let date_field = line.split('\t').next().unwrap();
        assert!(
            !date_field.ends_with('Z'),
            "local time should not end with Z, got: {date_field}"
        );
        assert!(
            date_field.contains("2024-01-01"),
            "should still contain the date, got: {date_field}"
        );
    }

    #[test]
    fn format_text_utc_time_default() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        assert!(
            line.contains("2024-01-01T00:00:00Z"),
            "default should be UTC with Z suffix"
        );
    }

    #[test]
    fn format_text_strips_prefix_with_relative_path() {
        let entry = make_entry_dated("logs/2024/data.csv", 100, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/2024/".to_string()),
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.ends_with("data.csv"));
        assert!(!line.contains("logs/2024/"));
    }

    #[test]
    fn format_text_default_shows_fullpath() {
        let entry = make_entry_dated("logs/2024/data.csv", 100, 2024, 1);
        let fmt = TsvFormatter::new(FormatOptions {
            show_relative_path: false,
            prefix: Some("logs/2024/".to_string()),
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains("logs/2024/data.csv"));
    }

    #[test]
    fn format_text_common_prefix_strips_prefix_with_relative_path() {
        let entry = ListEntry::CommonPrefix("logs/2024/".to_string());
        let fmt = TsvFormatter::new(FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/".to_string()),
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains("PRE"));
        assert!(line.ends_with("2024/"));
        assert!(!line.contains("logs/2024/"));
    }

    #[test]
    fn format_text_multiple_checksum_algorithms() {
        let entry = make_entry_with_checksums("file.txt", vec!["CRC32", "SHA256"]);
        let fmt = TsvFormatter::new(FormatOptions {
            show_checksum_algorithm: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[2], "CRC32,SHA256");
    }

    #[test]
    fn format_text_single_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec!["SHA256"]);
        let fmt = TsvFormatter::new(FormatOptions {
            show_checksum_algorithm: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields[2], "SHA256");
    }

    #[test]
    fn format_text_no_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec![]);
        let fmt = TsvFormatter::new(FormatOptions {
            show_checksum_algorithm: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields[2], "");
    }

    #[test]
    fn format_summary_text() {
        let stats = crate::types::ListingStatistics {
            total_objects: 42,
            total_size: 5678901,
            total_delete_markers: 0,
        };
        let fmt = TsvFormatter::new(FormatOptions {
            human: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal:\t42\tobjects\t5.4\tMiB");
    }

    #[test]
    fn format_summary_text_non_human() {
        let stats = crate::types::ListingStatistics {
            total_objects: 200002,
            total_size: 9578216,
            total_delete_markers: 0,
        };
        let fmt = TsvFormatter::new(FormatOptions {
            human: false,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert_eq!(summary, "\nTotal:\t200002\tobjects\t9578216\tbytes");
    }

    #[test]
    fn format_text_common_prefix_with_all_optional_columns() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let fmt = TsvFormatter::new(FormatOptions {
            show_storage_class: true,
            show_etag: true,
            show_checksum_algorithm: true,
            show_checksum_type: true,
            all_versions: true,
            show_is_latest: true,
            show_owner: true,
            show_restore_status: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        let fields: Vec<&str> = line.split('\t').collect();
        // date, PRE, storage_class, etag, checksum_algo, checksum_type,
        // version_id, is_latest, owner_name, owner_id, restore_in_progress,
        // restore_expiry, key
        assert_eq!(fields.len(), 13);
        assert_eq!(fields[0], ""); // date
        assert_eq!(fields[1], "PRE"); // size
        // Optional columns should all be empty
        for i in 2..12 {
            assert_eq!(fields[i], "", "field {i} should be empty");
        }
        assert_eq!(fields[12], "logs/"); // key
    }

    fn make_delete_marker() -> ListEntry {
        ListEntry::DeleteMarker {
            key: "deleted.txt".to_string(),
            version_info: VersionInfo {
                version_id: "ver-123".to_string(),
                is_latest: false,
            },
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap(),
            owner_display_name: Some("alice".to_string()),
            owner_id: Some("id-alice".to_string()),
        }
    }

    #[test]
    fn format_text_delete_marker_basic() {
        let entry = make_delete_marker();
        let fmt = TsvFormatter::new(FormatOptions::default());
        let line = fmt.format_entry(&entry);
        let fields: Vec<&str> = line.split('\t').collect();
        // date, DELETE, version_id, key
        assert_eq!(fields[1], "DELETE");
        assert_eq!(fields[2], "ver-123");
        assert!(fields.last().unwrap().contains("deleted.txt"));
    }

    #[test]
    fn format_text_delete_marker_with_all_optional_columns() {
        let entry = make_delete_marker();
        let fmt = TsvFormatter::new(FormatOptions {
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
        let fields: Vec<&str> = line.split('\t').collect();
        // date, DELETE, storage_class(empty), etag(empty), checksum_algo(empty),
        // checksum_type(empty), version_id, is_latest, owner_name, owner_id,
        // restore_in_progress(empty), restore_expiry(empty), key
        assert_eq!(fields[1], "DELETE");
        assert_eq!(fields[2], ""); // storage_class
        assert_eq!(fields[3], ""); // etag
        assert_eq!(fields[4], ""); // checksum_algorithm
        assert_eq!(fields[5], ""); // checksum_type
        assert_eq!(fields[6], "ver-123"); // version_id
        assert_eq!(fields[7], "NOT_LATEST"); // is_latest
        assert_eq!(fields[8], "alice"); // owner_display_name
        assert_eq!(fields[9], "id-alice"); // owner_id
        assert_eq!(fields[10], ""); // restore_in_progress
        assert_eq!(fields[11], ""); // restore_expiry
        assert_eq!(fields[12], "deleted.txt"); // key
    }

    #[test]
    fn format_text_delete_marker_is_latest() {
        let entry = ListEntry::DeleteMarker {
            key: "k".to_string(),
            version_info: VersionInfo {
                version_id: "v".to_string(),
                is_latest: true,
            },
            last_modified: chrono::Utc::now(),
            owner_display_name: None,
            owner_id: None,
        };
        let fmt = TsvFormatter::new(FormatOptions {
            show_is_latest: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains("LATEST"));
        assert!(!line.contains("NOT_LATEST"));
    }

    #[test]
    fn format_text_object_with_restore_status() {
        let entry = ListEntry::Object(S3Object {
            key: "k.dat".to_string(),
            size: 100,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("GLACIER".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: Some(true),
            restore_expiry_date: Some("2024-02-01T00:00:00Z".to_string()),
            version_info: None,
        });
        let fmt = TsvFormatter::new(FormatOptions {
            show_restore_status: true,
            ..Default::default()
        });
        let line = fmt.format_entry(&entry);
        assert!(line.contains("true"));
        assert!(line.contains("2024-02-01T00:00:00Z"));
    }

    #[test]
    fn format_summary_text_with_human_size() {
        let stats = crate::types::ListingStatistics {
            total_objects: 5,
            total_size: 1_048_576, // 1 MiB
            total_delete_markers: 0,
        };
        let fmt = TsvFormatter::new(FormatOptions {
            human: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert!(summary.starts_with("\nTotal:"));
        assert!(summary.contains("5"));
        assert!(summary.contains("MiB"));
    }

    #[test]
    fn format_summary_text_with_delete_markers() {
        let stats = crate::types::ListingStatistics {
            total_objects: 3,
            total_size: 100,
            total_delete_markers: 2,
        };
        let fmt = TsvFormatter::new(FormatOptions {
            human: false,
            all_versions: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert!(summary.contains("2"));
        assert!(summary.contains("delete markers"));
    }

    #[test]
    fn format_summary_with_versions() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 3,
        };
        let fmt = TsvFormatter::new(FormatOptions {
            human: false,
            all_versions: true,
            ..Default::default()
        });
        let summary = fmt.format_summary(&stats);
        assert!(summary.contains("\t3\tdelete markers"));
    }
}
