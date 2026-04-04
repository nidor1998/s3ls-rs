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
    pub fn from_display_config(display_config: &crate::config::DisplayConfig) -> Self {
        FormatOptions {
            human: display_config.human,
            show_fullpath: display_config.show_fullpath,
            show_etag: display_config.show_etag,
            show_storage_class: display_config.show_storage_class,
            show_checksum_algorithm: display_config.show_checksum_algorithm,
            show_checksum_type: display_config.show_checksum_type,
            prefix: None,
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
            format!("{adjusted:.1}")
        }
    } else {
        size.to_string()
    }
}

fn format_key<'a>(entry: &'a ListEntry, opts: &FormatOptions) -> &'a str {
    if opts.show_fullpath {
        entry.key()
    } else if let Some(ref prefix) = opts.prefix {
        entry.key().strip_prefix(prefix.as_str()).unwrap_or(entry.key())
    } else {
        entry.key()
    }
}

pub fn format_entry(entry: &ListEntry, bucket: Option<&str>, opts: &FormatOptions) -> String {
    match entry {
        ListEntry::CommonPrefix(_) => {
            let key = format_key(entry, opts);
            let key_display = if let Some(bucket) = bucket {
                if opts.show_fullpath {
                    format!("s3://{bucket}/{key}")
                } else {
                    key.to_string()
                }
            } else {
                key.to_string()
            };
            format!("{:>30} PRE {}", "", key_display)
        }
        ListEntry::Object(obj) => {
            let date = obj.last_modified().format("%Y-%m-%d %H:%M:%S");
            let size = format_size(obj.size(), opts.human);
            let key = format_key(entry, opts);
            let key_display = if let Some(bucket) = bucket {
                if opts.show_fullpath {
                    format!("s3://{bucket}/{key}")
                } else {
                    key.to_string()
                }
            } else {
                key.to_string()
            };

            let mut line = format!("{date} {size:>10} {key_display}");

            if let Some(version_id) = obj.version_id() {
                let latest = if obj.is_latest() { " (latest)" } else { "" };
                line.push_str(&format!("  [version: {version_id}{latest}]"));
            }

            if opts.show_etag {
                line.push_str(&format!("  etag:{}", obj.e_tag()));
            }
            if opts.show_storage_class {
                line.push_str(&format!(
                    "  class:{}",
                    obj.storage_class().unwrap_or("STANDARD")
                ));
            }
            if opts.show_checksum_algorithm {
                if let Some(algo) = obj.checksum_algorithm() {
                    line.push_str(&format!("  checksum_algo:{algo}"));
                }
            }
            if opts.show_checksum_type {
                if let Some(ctype) = obj.checksum_type() {
                    line.push_str(&format!("  checksum_type:{ctype}"));
                }
            }

            line
        }
        ListEntry::DeleteMarker {
            key,
            version_id,
            last_modified,
            is_latest,
        } => {
            let date = last_modified.format("%Y-%m-%d %H:%M:%S");
            let key = if opts.show_fullpath {
                if let Some(bucket) = bucket {
                    format!("s3://{bucket}/{key}")
                } else {
                    key.clone()
                }
            } else if let Some(ref prefix) = opts.prefix {
                key.strip_prefix(prefix.as_str())
                    .unwrap_or(key)
                    .to_string()
            } else {
                key.clone()
            };
            let latest = if *is_latest { " (latest)" } else { "" };
            format!("{date} DELETE_MARKER {key}  [version: {version_id}{latest}]")
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
        let line = format_entry(&entry, None, &opts);
        assert!(line.contains("2024-01-01"));
        assert!(line.contains("1234"));
        assert!(line.contains("readme.txt"));
    }

    #[test]
    fn format_text_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let opts = FormatOptions::default();
        let line = format_entry(&entry, None, &opts);
        assert!(line.contains("PRE"));
        assert!(line.contains("logs/"));
    }

    #[test]
    fn format_text_human_size() {
        let entry = make_entry("data.csv", 5678901, 2024, 1);
        let opts = FormatOptions { human: true, ..Default::default() };
        let line = format_entry(&entry, None, &opts);
        assert!(line.contains("5.4 MiB"));
    }

    #[test]
    fn format_text_with_etag() {
        let entry = make_entry("file.txt", 100, 2024, 1);
        let opts = FormatOptions { show_etag: true, ..Default::default() };
        let line = format_entry(&entry, None, &opts);
        assert!(line.contains("\"e\""));
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
}
