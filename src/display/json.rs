use crate::display::{EntryFormatter, FormatOptions, format_key_display, format_rfc3339};
use crate::types::{ListEntry, ListingStatistics};

pub struct JsonFormatter {
    opts: FormatOptions,
}

impl JsonFormatter {
    pub fn new(opts: FormatOptions) -> Self {
        Self { opts }
    }
}

impl EntryFormatter for JsonFormatter {
    fn format_entry(&self, entry: &ListEntry) -> String {
        let opts = &self.opts;
        match entry {
            ListEntry::CommonPrefix(prefix) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "Prefix".to_string(),
                    serde_json::Value::String(format_key_display(prefix, opts)),
                );
                serde_json::to_string(&map).unwrap()
            }
            ListEntry::Object(obj) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "Key".to_string(),
                    serde_json::Value::String(format_key_display(obj.key(), opts)),
                );
                map.insert(
                    "LastModified".to_string(),
                    serde_json::Value::String(format_rfc3339(
                        obj.last_modified(),
                        opts.show_local_time,
                    )),
                );
                map.insert(
                    "ETag".to_string(),
                    serde_json::Value::String(obj.e_tag().to_string()),
                );
                if !obj.checksum_algorithm().is_empty() {
                    map.insert(
                        "ChecksumAlgorithm".to_string(),
                        serde_json::Value::Array(
                            obj.checksum_algorithm()
                                .iter()
                                .map(|a| serde_json::Value::String(a.clone()))
                                .collect(),
                        ),
                    );
                }
                if let Some(ctype) = obj.checksum_type() {
                    map.insert(
                        "ChecksumType".to_string(),
                        serde_json::Value::String(ctype.to_string()),
                    );
                }
                map.insert("Size".to_string(), serde_json::json!(obj.size()));
                if let Some(sc) = obj.storage_class() {
                    map.insert(
                        "StorageClass".to_string(),
                        serde_json::Value::String(sc.to_string()),
                    );
                }
                if let Some(vid) = obj.version_id() {
                    map.insert(
                        "VersionId".to_string(),
                        serde_json::Value::String(vid.to_string()),
                    );
                    map.insert("IsLatest".to_string(), serde_json::json!(obj.is_latest()));
                }
                let owner_id = obj.owner_id();
                let owner_name = obj.owner_display_name();
                if owner_id.is_some() || owner_name.is_some() {
                    let mut owner = serde_json::Map::new();
                    if let Some(name) = owner_name {
                        owner.insert(
                            "DisplayName".to_string(),
                            serde_json::Value::String(name.to_string()),
                        );
                    }
                    if let Some(id) = owner_id {
                        owner.insert("ID".to_string(), serde_json::Value::String(id.to_string()));
                    }
                    map.insert("Owner".to_string(), serde_json::Value::Object(owner));
                }
                if let Some(in_progress) = obj.is_restore_in_progress() {
                    let mut restore = serde_json::Map::new();
                    restore.insert(
                        "IsRestoreInProgress".to_string(),
                        serde_json::json!(in_progress),
                    );
                    if let Some(expiry) = obj.restore_expiry_date() {
                        restore.insert(
                            "RestoreExpiryDate".to_string(),
                            serde_json::Value::String(expiry.to_string()),
                        );
                    }
                    map.insert(
                        "RestoreStatus".to_string(),
                        serde_json::Value::Object(restore),
                    );
                }
                serde_json::to_string(&map).unwrap()
            }
            ListEntry::DeleteMarker {
                key,
                version_id,
                last_modified,
                is_latest,
                owner_display_name,
                owner_id,
            } => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "Key".to_string(),
                    serde_json::Value::String(format_key_display(key, opts)),
                );
                map.insert(
                    "VersionId".to_string(),
                    serde_json::Value::String(version_id.clone()),
                );
                map.insert("IsLatest".to_string(), serde_json::json!(*is_latest));
                map.insert(
                    "LastModified".to_string(),
                    serde_json::Value::String(format_rfc3339(last_modified, opts.show_local_time)),
                );
                map.insert("DeleteMarker".to_string(), serde_json::json!(true));
                if owner_id.is_some() || owner_display_name.is_some() {
                    let mut owner = serde_json::Map::new();
                    if let Some(name) = owner_display_name {
                        owner.insert(
                            "DisplayName".to_string(),
                            serde_json::Value::String(name.clone()),
                        );
                    }
                    if let Some(id) = owner_id {
                        owner.insert("ID".to_string(), serde_json::Value::String(id.clone()));
                    }
                    map.insert("Owner".to_string(), serde_json::Value::Object(owner));
                }
                serde_json::to_string(&map).unwrap()
            }
        }
    }

    fn format_header(&self) -> Option<String> {
        None
    }

    fn format_summary(&self, stats: &ListingStatistics) -> String {
        let mut map = serde_json::Map::new();
        let mut summary = serde_json::Map::new();
        summary.insert(
            "TotalObjects".to_string(),
            serde_json::json!(stats.total_objects),
        );
        summary.insert("TotalSize".to_string(), serde_json::json!(stats.total_size));
        if self.opts.all_versions {
            summary.insert(
                "TotalDeleteMarkers".to_string(),
                serde_json::json!(stats.total_delete_markers),
            );
        }
        map.insert("Summary".to_string(), serde_json::Value::Object(summary));
        serde_json::to_string(&map).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::FormatOptions;
    use crate::types::{ListEntry, S3Object};
    use chrono::TimeZone;

    fn make_entry_dated(key: &str, size: u64, year: i32, month: u32) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
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
        })
    }

    fn make_entry_with_checksums(key: &str, checksums: Vec<&str>) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
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
        })
    }

    #[test]
    fn format_ndjson_object() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions::default();
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "readme.txt");
        assert_eq!(parsed["Size"], 1234);
        assert!(parsed["ETag"].is_string());
        assert!(parsed["LastModified"].is_string());
    }

    #[test]
    fn format_ndjson_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        let opts = FormatOptions::default();
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Prefix"], "logs/");
    }

    #[test]
    fn format_ndjson_delete_marker() {
        let entry = ListEntry::DeleteMarker {
            key: "deleted.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
            owner_display_name: None,
            owner_id: None,
        };
        let opts = FormatOptions::default();
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "deleted.txt");
        assert_eq!(parsed["VersionId"], "v1");
        assert_eq!(parsed["IsLatest"], true);
        assert_eq!(parsed["DeleteMarker"], true);
    }

    #[test]
    fn format_json_preserves_control_chars() {
        let entry = ListEntry::Object(S3Object::NotVersioning {
            key: "evil\nkey".to_string(),
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
        });
        let opts = FormatOptions::default();
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "evil\nkey");
        assert!(!json.contains('\n'));
    }

    #[test]
    fn format_json_local_time() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions {
            show_local_time: true,
            ..Default::default()
        };
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let last_modified = parsed["LastModified"].as_str().unwrap();
        assert!(
            !last_modified.ends_with('Z'),
            "local time JSON should not end with Z, got: {last_modified}"
        );
    }

    #[test]
    fn format_json_utc_time_default() {
        let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions::default();
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let last_modified = parsed["LastModified"].as_str().unwrap();
        assert!(
            last_modified.ends_with('Z') || last_modified.contains("+00:00"),
            "default JSON should be UTC, got: {last_modified}"
        );
    }

    #[test]
    fn format_ndjson_object_relative_path() {
        let entry = make_entry_dated("logs/2024/readme.txt", 1234, 2024, 1);
        let opts = FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/2024/".to_string()),
            ..FormatOptions::default()
        };
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "readme.txt");
    }

    #[test]
    fn format_ndjson_common_prefix_relative_path() {
        let entry = ListEntry::CommonPrefix("logs/2024/subdir/".to_string());
        let opts = FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/2024/".to_string()),
            ..FormatOptions::default()
        };
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Prefix"], "subdir/");
    }

    #[test]
    fn format_ndjson_delete_marker_relative_path() {
        let entry = ListEntry::DeleteMarker {
            key: "logs/2024/deleted.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
            owner_display_name: None,
            owner_id: None,
        };
        let opts = FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/2024/".to_string()),
            ..FormatOptions::default()
        };
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "deleted.txt");
    }

    #[test]
    fn format_ndjson_delete_marker_with_owner() {
        let entry = ListEntry::DeleteMarker {
            key: "deleted.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
            owner_display_name: Some("alice".to_string()),
            owner_id: Some("id123".to_string()),
        };
        let opts = FormatOptions::default();
        let json = JsonFormatter::new(opts).format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Owner"]["DisplayName"], "alice");
        assert_eq!(parsed["Owner"]["ID"], "id123");
        assert_eq!(parsed["DeleteMarker"], true);
    }

    #[test]
    fn format_summary_json() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 0,
        };
        let summary = JsonFormatter::new(FormatOptions::default()).format_summary(&stats);
        let parsed: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(parsed["Summary"]["TotalObjects"], 10);
        assert_eq!(parsed["Summary"]["TotalSize"], 1024);
    }

    #[test]
    fn format_json_multiple_checksum_algorithms() {
        let entry = make_entry_with_checksums("file.txt", vec!["CRC32", "SHA256"]);
        let opts = FormatOptions::default();
        let json_str = JsonFormatter::new(opts).format_entry(&entry);
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let algos = val["ChecksumAlgorithm"].as_array().unwrap();
        assert_eq!(algos.len(), 2);
        assert_eq!(algos[0], "CRC32");
        assert_eq!(algos[1], "SHA256");
    }

    #[test]
    fn format_json_single_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec!["CRC64NVME"]);
        let opts = FormatOptions::default();
        let json_str = JsonFormatter::new(opts).format_entry(&entry);
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let algos = val["ChecksumAlgorithm"].as_array().unwrap();
        assert_eq!(algos.len(), 1);
        assert_eq!(algos[0], "CRC64NVME");
    }

    #[test]
    fn format_json_no_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec![]);
        let opts = FormatOptions::default();
        let json_str = JsonFormatter::new(opts).format_entry(&entry);
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert!(val.get("ChecksumAlgorithm").is_none());
    }
}
