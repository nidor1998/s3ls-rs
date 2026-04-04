use crate::config::args::SortField;
use crate::types::ListEntry;
use byte_unit::Byte;

#[derive(Default)]
pub struct FormatOptions {
    pub human: bool,
    pub show_fullpath: bool,
    pub show_etag: bool,
    pub show_storage_class: bool,
    pub show_checksum_algorithm: bool,
    pub show_checksum_type: bool,
    pub prefix: Option<String>,
}

impl FormatOptions {
    pub fn from_display_config(
        display_config: &crate::config::DisplayConfig,
        prefix: Option<String>,
    ) -> Self {
        FormatOptions {
            human: display_config.human,
            show_fullpath: display_config.show_fullpath,
            show_etag: display_config.show_etag,
            show_storage_class: display_config.show_storage_class,
            show_checksum_algorithm: display_config.show_checksum_algorithm,
            show_checksum_type: display_config.show_checksum_type,
            prefix,
        }
    }
}

fn format_size(size: u64, human: bool) -> String {
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

fn format_key_display(entry_key: &str, opts: &FormatOptions) -> String {
    if opts.show_fullpath {
        entry_key.to_string()
    } else if let Some(ref prefix) = opts.prefix {
        entry_key
            .strip_prefix(prefix.as_str())
            .unwrap_or(entry_key)
            .to_string()
    } else {
        entry_key.to_string()
    }
}

fn format_rfc3339(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub fn format_entry(entry: &ListEntry, opts: &FormatOptions) -> String {
    match entry {
        ListEntry::CommonPrefix(_) => {
            let key_display = format_key_display(entry.key(), opts);
            format!("{:>30} PRE {}", "", key_display)
        }
        ListEntry::Object(obj) => {
            let date = format_rfc3339(obj.last_modified());
            let size = format_size(obj.size(), opts.human);
            let key_display = format_key_display(entry.key(), opts);

            // Build middle columns: extra columns then version_id, all before key
            let mut middle = String::new();
            if opts.show_storage_class {
                middle.push_str(&format!(
                    " {}",
                    obj.storage_class().unwrap_or("STANDARD")
                ));
            }
            if opts.show_etag {
                middle.push_str(&format!(" {}", obj.e_tag()));
            }
            if opts.show_checksum_algorithm
                && let Some(algo) = obj.checksum_algorithm()
            {
                middle.push_str(&format!(" {algo}"));
            }
            if opts.show_checksum_type
                && let Some(ctype) = obj.checksum_type()
            {
                middle.push_str(&format!(" {ctype}"));
            }
            if let Some(version_id) = obj.version_id() {
                middle.push_str(&format!(" {version_id}"));
            }

            format!("{date} {size:>10}{middle} {key_display}")
        }
        ListEntry::DeleteMarker {
            key,
            version_id,
            last_modified,
            ..
        } => {
            let date = format_rfc3339(last_modified);
            let key_display = format_key_display(key, opts);
            format!("{date} {:>10} {version_id} (delete marker) {key_display}", "0")
        }
    }
}

pub fn sort_entries(
    entries: &mut [ListEntry],
    field: &SortField,
    reverse: bool,
    all_versions: bool,
) {
    entries.sort_by(|a, b| {
        let cmp = match field {
            SortField::Key => {
                let primary = a.key().cmp(b.key());
                if all_versions && primary == std::cmp::Ordering::Equal {
                    let a_time = a.last_modified();
                    let b_time = b.last_modified();
                    match (a_time, b_time) {
                        (Some(at), Some(bt)) => at.cmp(bt),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                } else {
                    primary
                }
            }
            SortField::Size => a.size().cmp(&b.size()),
            SortField::Date => {
                let a_time = a.last_modified();
                let b_time = b.last_modified();
                match (a_time, b_time) {
                    (Some(at), Some(bt)) => at.cmp(bt),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            }
        };
        if reverse { cmp.reverse() } else { cmp }
    });
}

pub fn format_entry_json(entry: &ListEntry) -> String {
    match entry {
        ListEntry::CommonPrefix(prefix) => {
            let mut map = serde_json::Map::new();
            map.insert("common_prefix".to_string(), serde_json::Value::String(prefix.clone()));
            serde_json::to_string(&map).unwrap()
        }
        ListEntry::Object(obj) => {
            let mut map = serde_json::Map::new();
            map.insert("key".to_string(), serde_json::Value::String(obj.key().to_string()));
            map.insert("size".to_string(), serde_json::json!(obj.size()));
            map.insert(
                "last_modified".to_string(),
                serde_json::Value::String(obj.last_modified().to_rfc3339()),
            );
            map.insert("e_tag".to_string(), serde_json::Value::String(obj.e_tag().to_string()));
            if let Some(sc) = obj.storage_class() {
                map.insert("storage_class".to_string(), serde_json::Value::String(sc.to_string()));
            }
            if let Some(algo) = obj.checksum_algorithm() {
                map.insert(
                    "checksum_algorithm".to_string(),
                    serde_json::Value::String(algo.to_string()),
                );
            }
            if let Some(ctype) = obj.checksum_type() {
                map.insert(
                    "checksum_type".to_string(),
                    serde_json::Value::String(ctype.to_string()),
                );
            }
            if let Some(vid) = obj.version_id() {
                map.insert("version_id".to_string(), serde_json::Value::String(vid.to_string()));
                map.insert("is_latest".to_string(), serde_json::json!(obj.is_latest()));
            }
            serde_json::to_string(&map).unwrap()
        }
        ListEntry::DeleteMarker {
            key,
            version_id,
            last_modified,
            is_latest,
        } => {
            let mut map = serde_json::Map::new();
            map.insert("key".to_string(), serde_json::Value::String(key.clone()));
            map.insert("delete_marker".to_string(), serde_json::json!(true));
            map.insert("version_id".to_string(), serde_json::Value::String(version_id.clone()));
            map.insert(
                "last_modified".to_string(),
                serde_json::Value::String(last_modified.to_rfc3339()),
            );
            map.insert("is_latest".to_string(), serde_json::json!(*is_latest));
            serde_json::to_string(&map).unwrap()
        }
    }
}

pub fn compute_statistics(entries: &[ListEntry]) -> crate::types::ListingStatistics {
    let mut total_objects: u64 = 0;
    let mut total_size: u64 = 0;
    let mut total_versions: u64 = 0;
    let mut total_delete_markers: u64 = 0;

    for entry in entries {
        match entry {
            ListEntry::Object(obj) => {
                total_objects += 1;
                total_size += obj.size();
                if obj.version_id().is_some() {
                    total_versions += 1;
                }
            }
            ListEntry::CommonPrefix(_) => {}
            ListEntry::DeleteMarker { .. } => {
                total_delete_markers += 1;
            }
        }
    }

    crate::types::ListingStatistics {
        total_objects,
        total_size,
        total_versions,
        total_delete_markers,
    }
}

pub fn format_summary(
    stats: &crate::types::ListingStatistics,
    json: bool,
    all_versions: bool,
) -> String {
    if json {
        let mut map = serde_json::Map::new();
        let mut summary = serde_json::Map::new();
        summary.insert("total_objects".to_string(), serde_json::json!(stats.total_objects));
        summary.insert("total_size".to_string(), serde_json::json!(stats.total_size));
        if all_versions {
            summary.insert("total_versions".to_string(), serde_json::json!(stats.total_versions));
            summary.insert(
                "total_delete_markers".to_string(),
                serde_json::json!(stats.total_delete_markers),
            );
        }
        map.insert("summary".to_string(), serde_json::Value::Object(summary));
        serde_json::to_string(&map).unwrap()
    } else {
        let size_str = format_size(stats.total_size, true);
        let mut line = format!("Total: {} objects, {}", stats.total_objects, size_str);
        if all_versions {
            line.push_str(&format!(
                ", {} versions, {} delete markers",
                stats.total_versions, stats.total_delete_markers
            ));
        }
        line
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::SortField;
    use crate::types::{ListEntry, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64, year: i32, month: u32) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(),
            size,
            last_modified: chrono::Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: None,
            checksum_type: None,
        })
    }

    #[test]
    fn sort_by_key() {
        let mut entries = vec![
            make_entry("c.txt", 100, 2024, 1),
            make_entry("a.txt", 200, 2024, 2),
            make_entry("b.txt", 300, 2024, 3),
        ];
        sort_entries(&mut entries, &SortField::Key, false, false);
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[1].key(), "b.txt");
        assert_eq!(entries[2].key(), "c.txt");
    }

    #[test]
    fn sort_by_key_with_all_versions_secondary_mtime() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("a.txt", 200, 2024, 1),
            make_entry("b.txt", 300, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Key, false, true);
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[0].size(), 200); // Jan entry first
        assert_eq!(entries[1].key(), "a.txt");
        assert_eq!(entries[1].size(), 100); // Mar entry second
        assert_eq!(entries[2].key(), "b.txt");
    }

    #[test]
    fn sort_by_size() {
        let mut entries = vec![
            make_entry("a.txt", 300, 2024, 1),
            make_entry("b.txt", 100, 2024, 2),
            make_entry("c.txt", 200, 2024, 3),
        ];
        sort_entries(&mut entries, &SortField::Size, false, false);
        assert_eq!(entries[0].size(), 100);
        assert_eq!(entries[1].size(), 200);
        assert_eq!(entries[2].size(), 300);
    }

    #[test]
    fn sort_by_date() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("b.txt", 100, 2024, 1),
            make_entry("c.txt", 100, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Date, false, false);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "c.txt");
        assert_eq!(entries[2].key(), "a.txt");
    }

    #[test]
    fn sort_reverse() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            make_entry("b.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Key, true, false);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
    }

    #[test]
    fn format_text_basic_object() {
        let entry = make_entry("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions::default();
        let line = format_entry(&entry, &opts);
        // Spec: 2024-01-01T00:00:00Z       1234 readme.txt
        assert!(line.contains("2024-01-01T00:00:00Z"));
        assert!(line.contains("1234"));
        assert!(line.ends_with("readme.txt"));
    }

    #[test]
    fn format_text_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let opts = FormatOptions::default();
        let line = format_entry(&entry, &opts);
        assert!(line.contains("PRE"));
        assert!(line.ends_with("logs/"));
    }

    #[test]
    fn format_text_human_size() {
        let entry = make_entry("data.csv", 5678901, 2024, 1);
        let opts = FormatOptions { human: true, ..Default::default() };
        let line = format_entry(&entry, &opts);
        // Spec: 2024-01-01T00:00:00Z    5.4MiB data.csv
        assert!(line.contains("5.4MiB"));
        assert!(line.ends_with("data.csv"));
    }

    #[test]
    fn format_text_extra_columns_before_key() {
        // Spec: 2024-01-15T10:30:00Z       1234 STANDARD "abc123" readme.txt
        let entry = make_entry("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions {
            show_etag: true,
            show_storage_class: true,
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        // storage_class and etag appear between size and key
        let size_pos = line.find("1234").unwrap();
        let class_pos = line.find("STANDARD").unwrap();
        let etag_pos = line.find("\"e\"").unwrap();
        let key_pos = line.find("readme.txt").unwrap();
        assert!(size_pos < class_pos, "size before class");
        assert!(class_pos < etag_pos, "class before etag");
        assert!(etag_pos < key_pos, "etag before key");
    }

    #[test]
    fn format_text_versioned_object() {
        // Spec: 2024-01-15T10:30:00Z       1234 abc123-version-id readme.txt
        let entry = ListEntry::Object(S3Object::Versioning {
            key: "readme.txt".to_string(),
            version_id: "abc123-version-id".to_string(),
            size: 1234,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 15, 10, 30, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            is_latest: true,
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: None,
            checksum_type: None,
        });
        let opts = FormatOptions::default();
        let line = format_entry(&entry, &opts);
        // version_id appears between size and key
        let size_pos = line.find("1234").unwrap();
        let vid_pos = line.find("abc123-version-id").unwrap();
        let key_pos = line.rfind("readme.txt").unwrap();
        assert!(size_pos < vid_pos, "size before version_id");
        assert!(vid_pos < key_pos, "version_id before key");
    }

    #[test]
    fn format_text_delete_marker() {
        // Spec: 2024-01-16T09:00:00Z          0 def456-version-id (delete marker) readme.txt
        let entry = ListEntry::DeleteMarker {
            key: "readme.txt".to_string(),
            version_id: "def456-version-id".to_string(),
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 16, 9, 0, 0).unwrap(),
            is_latest: false,
        };
        let opts = FormatOptions::default();
        let line = format_entry(&entry, &opts);
        assert!(line.contains("2024-01-16T09:00:00Z"));
        let zero_pos = line.find('0').unwrap();
        let vid_pos = line.find("def456-version-id").unwrap();
        let marker_pos = line.find("(delete marker)").unwrap();
        let key_pos = line.rfind("readme.txt").unwrap();
        assert!(zero_pos < vid_pos, "0 before version_id");
        assert!(vid_pos < marker_pos, "version_id before (delete marker)");
        assert!(marker_pos < key_pos, "(delete marker) before key");
    }

    #[test]
    fn sort_common_prefix_by_size_sorts_as_zero() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            ListEntry::CommonPrefix("logs/".to_string()),
        ];
        sort_entries(&mut entries, &SortField::Size, false, false);
        assert_eq!(entries[0].key(), "logs/");
        assert_eq!(entries[1].key(), "a.txt");
    }

    #[test]
    fn format_ndjson_object() {
        let entry = make_entry("readme.txt", 1234, 2024, 1);
        let json = format_entry_json(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["key"], "readme.txt");
        assert_eq!(parsed["size"], 1234);
    }

    #[test]
    fn format_ndjson_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let json = format_entry_json(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["common_prefix"], "logs/");
    }

    #[test]
    fn format_ndjson_delete_marker() {
        let entry = ListEntry::DeleteMarker {
            key: "deleted.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
        };
        let json = format_entry_json(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["key"], "deleted.txt");
        assert_eq!(parsed["delete_marker"], true);
    }

    #[test]
    fn format_summary_text() {
        let stats = crate::types::ListingStatistics {
            total_objects: 42,
            total_size: 5678901,
            total_versions: 0,
            total_delete_markers: 0,
        };
        let summary = format_summary(&stats, false, false);
        assert!(summary.contains("42 objects"));
        assert!(summary.contains("5.4MiB"));
    }

    #[test]
    fn format_summary_json() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_versions: 0,
            total_delete_markers: 0,
        };
        let summary = format_summary(&stats, true, false);
        let parsed: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(parsed["summary"]["total_objects"], 10);
        assert_eq!(parsed["summary"]["total_size"], 1024);
    }

    #[test]
    fn format_summary_with_versions() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_versions: 15,
            total_delete_markers: 3,
        };
        let summary = format_summary(&stats, false, true);
        assert!(summary.contains("15 versions"));
        assert!(summary.contains("3 delete markers"));
    }

    #[test]
    fn format_text_strips_prefix_by_default() {
        let entry = make_entry("logs/2024/data.csv", 100, 2024, 1);
        let opts = FormatOptions {
            prefix: Some("logs/2024/".to_string()),
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        assert!(line.ends_with("data.csv"));
        assert!(!line.contains("logs/2024/"));
    }

    #[test]
    fn format_text_show_fullpath_keeps_full_key() {
        let entry = make_entry("logs/2024/data.csv", 100, 2024, 1);
        let opts = FormatOptions {
            show_fullpath: true,
            prefix: Some("logs/2024/".to_string()),
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        assert!(line.contains("logs/2024/data.csv"));
    }

    #[test]
    fn format_text_common_prefix_strips_prefix() {
        let entry = ListEntry::CommonPrefix("logs/2024/".to_string());
        let opts = FormatOptions {
            prefix: Some("logs/".to_string()),
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        assert!(line.contains("PRE"));
        assert!(line.ends_with("2024/"));
        assert!(!line.contains("logs/2024/"));
    }

    #[test]
    fn compute_statistics_counts_correctly() {
        let entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            make_entry("b.txt", 200, 2024, 2),
            ListEntry::CommonPrefix("logs/".to_string()),
            ListEntry::DeleteMarker {
                key: "c.txt".to_string(),
                version_id: "v1".to_string(),
                last_modified: chrono::Utc::now(),
                is_latest: true,
            },
        ];
        let stats = compute_statistics(&entries);
        assert_eq!(stats.total_objects, 2);
        assert_eq!(stats.total_size, 300);
        assert_eq!(stats.total_delete_markers, 1);
    }
}
