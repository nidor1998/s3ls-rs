use crate::config::args::SortField;
use crate::types::ListEntry;
use anyhow::Result;
use byte_unit::Byte;
use std::io::Write;
use tokio::sync::mpsc;

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
        }
    }
}

pub struct AggregatorConfig {
    pub use_json: bool,
    pub no_sort: bool,
    pub sort_fields: Vec<SortField>,
    pub reverse: bool,
    pub summary: bool,
    pub human: bool,
    pub all_versions: bool,
    pub cancellation_token: crate::types::token::PipelineCancellationToken,
}

pub struct Aggregator<W: Write + Send + 'static> {
    rx: mpsc::Receiver<ListEntry>,
    writer: W,
    opts: FormatOptions,
    config: AggregatorConfig,
}

impl<W: Write + Send + 'static> Aggregator<W> {
    pub fn new(
        rx: mpsc::Receiver<ListEntry>,
        writer: W,
        opts: FormatOptions,
        config: AggregatorConfig,
    ) -> Self {
        Self {
            rx,
            writer,
            opts,
            config,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        if self.config.no_sort {
            self.run_streaming().await
        } else {
            self.run_aggregate().await
        }
    }

    /// Print the column header line (call before `run` if needed).
    pub fn write_header(&mut self) -> Result<()> {
        writeln!(self.writer, "{}", format_header(&self.opts))?;
        Ok(())
    }

    async fn run_streaming(&mut self) -> Result<()> {
        let mut stats = crate::types::ListingStatistics {
            total_objects: 0,
            total_size: 0,
            total_delete_markers: 0,
        };

        while let Some(entry) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                break;
            }
            if self.config.summary {
                accumulate_statistics(&entry, &mut stats);
            }
            let line = if self.config.use_json {
                format_entry_json(&entry, &self.opts)
            } else {
                format_entry(&entry, &self.opts)
            };
            writeln!(self.writer, "{line}")?;
        }

        if self.config.summary {
            self.write_summary(&stats)?;
        }

        self.writer.flush()?;
        Ok(())
    }

    async fn run_aggregate(&mut self) -> Result<()> {
        let mut entries = Vec::new();
        while let Some(entry) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                break;
            }
            entries.push(entry);
        }

        sort_entries(&mut entries, &self.config.sort_fields, self.config.reverse);

        for entry in &entries {
            let line = if self.config.use_json {
                format_entry_json(entry, &self.opts)
            } else {
                format_entry(entry, &self.opts)
            };
            writeln!(self.writer, "{line}")?;
        }

        if self.config.summary {
            let stats = compute_statistics(&entries);
            self.write_summary(&stats)?;
        }

        self.writer.flush()?;
        Ok(())
    }

    fn write_summary(&mut self, stats: &crate::types::ListingStatistics) -> Result<()> {
        let summary = format_summary(
            stats,
            self.config.use_json,
            self.config.human,
            self.config.all_versions,
        );
        if !self.config.use_json {
            writeln!(self.writer)?;
        }
        writeln!(self.writer, "{summary}")?;
        Ok(())
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
    if !opts.show_relative_path {
        return entry_key.to_string();
    }
    if let Some(ref prefix) = opts.prefix {
        let stripped = entry_key
            .strip_prefix(prefix.as_str())
            .unwrap_or(entry_key)
            .trim_start_matches('/');
        if stripped.is_empty() {
            entry_key.to_string()
        } else {
            stripped.to_string()
        }
    } else {
        entry_key.to_string()
    }
}

fn format_rfc3339(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub fn format_entry(entry: &ListEntry, opts: &FormatOptions) -> String {
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
            // version_id is only present in versioned entries; PRE never has one,
            // but we still need the placeholder when other versioned rows exist.
            // However, we can't know here whether the listing has versions, so
            // we skip — version_id is always the last optional column before key
            // and only appears on versioned objects/delete markers.
            if opts.show_is_latest {
                cols.push(String::new());
            }
            if opts.show_owner {
                cols.push(String::new());
                cols.push(String::new());
            }
            if opts.show_restore_status {
                cols.push(String::new());
                cols.push(String::new());
            }
            // key
            cols.push(format_key_display(entry.key(), opts));
        }
        ListEntry::Object(obj) => {
            cols.push(format_rfc3339(obj.last_modified()));
            cols.push(format_size(obj.size(), opts.human));
            if opts.show_storage_class {
                cols.push(obj.storage_class().unwrap_or("STANDARD").to_string());
            }
            if opts.show_etag {
                cols.push(obj.e_tag().trim_matches('"').to_string());
            }
            if opts.show_checksum_algorithm {
                cols.push(obj.checksum_algorithm().join(","));
            }
            if opts.show_checksum_type {
                cols.push(obj.checksum_type().unwrap_or("").to_string());
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
                cols.push(obj.owner_display_name().unwrap_or("").to_string());
                cols.push(obj.owner_id().unwrap_or("").to_string());
            }
            if opts.show_restore_status {
                cols.push(
                    obj.is_restore_in_progress()
                        .map(|b| b.to_string())
                        .unwrap_or_default(),
                );
                cols.push(obj.restore_expiry_date().unwrap_or("").to_string());
            }
            cols.push(format_key_display(entry.key(), opts));
        }
        ListEntry::DeleteMarker {
            key,
            version_id,
            last_modified,
            is_latest,
            ..
        } => {
            cols.push(format_rfc3339(last_modified));
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
            cols.push(version_id.clone());
            if opts.show_is_latest {
                cols.push(if *is_latest {
                    "LATEST".to_string()
                } else {
                    "NOT_LATEST".to_string()
                });
            }
            if opts.show_owner {
                cols.push(String::new());
                cols.push(String::new());
            }
            if opts.show_restore_status {
                cols.push(String::new());
                cols.push(String::new());
            }
            cols.push(format_key_display(key, opts));
        }
    }

    cols.join("\t")
}

pub fn format_header(opts: &FormatOptions) -> String {
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
    cols.join("\t")
}

fn cmp_mtime(a: &ListEntry, b: &ListEntry) -> std::cmp::Ordering {
    match (a.last_modified(), b.last_modified()) {
        (Some(at), Some(bt)) => at.cmp(bt),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub fn sort_entries(entries: &mut [ListEntry], fields: &[SortField], reverse: bool) {
    entries.sort_by(|a, b| {
        let mut cmp = std::cmp::Ordering::Equal;
        for field in fields {
            cmp = cmp.then_with(|| match field {
                SortField::Key | SortField::Bucket => a.key().cmp(b.key()),
                SortField::Size => a.size().cmp(&b.size()),
                SortField::Date => cmp_mtime(a, b),
                SortField::Region => std::cmp::Ordering::Equal,
            });
        }
        if reverse { cmp.reverse() } else { cmp }
    });
}

pub fn format_entry_json(entry: &ListEntry, opts: &FormatOptions) -> String {
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
                serde_json::Value::String(obj.last_modified().to_rfc3339()),
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
                serde_json::Value::String(last_modified.to_rfc3339()),
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

/// Incrementally accumulate statistics for a single entry (streaming mode).
pub fn accumulate_statistics(entry: &ListEntry, stats: &mut crate::types::ListingStatistics) {
    match entry {
        ListEntry::Object(obj) => {
            stats.total_objects += 1;
            stats.total_size += obj.size();
        }
        ListEntry::CommonPrefix(_) => {}
        ListEntry::DeleteMarker { .. } => {
            stats.total_delete_markers += 1;
        }
    }
}

pub fn compute_statistics(entries: &[ListEntry]) -> crate::types::ListingStatistics {
    let mut total_objects: u64 = 0;
    let mut total_size: u64 = 0;
    let mut total_delete_markers: u64 = 0;

    for entry in entries {
        match entry {
            ListEntry::Object(obj) => {
                total_objects += 1;
                total_size += obj.size();
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
        total_delete_markers,
    }
}

pub fn format_summary(
    stats: &crate::types::ListingStatistics,
    json: bool,
    human: bool,
    all_versions: bool,
) -> String {
    if json {
        let mut map = serde_json::Map::new();
        let mut summary = serde_json::Map::new();
        summary.insert(
            "total_objects".to_string(),
            serde_json::json!(stats.total_objects),
        );
        summary.insert(
            "total_size".to_string(),
            serde_json::json!(stats.total_size),
        );
        if all_versions {
            summary.insert(
                "total_delete_markers".to_string(),
                serde_json::json!(stats.total_delete_markers),
            );
        }
        map.insert("summary".to_string(), serde_json::Value::Object(summary));
        serde_json::to_string(&map).unwrap()
    } else {
        let size_str = if human {
            format_size(stats.total_size, true)
        } else {
            format!("{} bytes", stats.total_size)
        };
        let mut line = format!("Total: {} objects, {}", stats.total_objects, size_str);
        if all_versions {
            line.push_str(&format!(", {} delete markers", stats.total_delete_markers));
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

    #[test]
    fn sort_by_key() {
        let mut entries = vec![
            make_entry("c.txt", 100, 2024, 1),
            make_entry("a.txt", 200, 2024, 2),
            make_entry("b.txt", 300, 2024, 3),
        ];
        sort_entries(&mut entries, &[SortField::Key], false);
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[1].key(), "b.txt");
        assert_eq!(entries[2].key(), "c.txt");
    }

    #[test]
    fn sort_by_size() {
        let mut entries = vec![
            make_entry("a.txt", 300, 2024, 1),
            make_entry("b.txt", 100, 2024, 2),
            make_entry("c.txt", 200, 2024, 3),
        ];
        sort_entries(&mut entries, &[SortField::Size], false);
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
        sort_entries(&mut entries, &[SortField::Date], false);
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
        sort_entries(&mut entries, &[SortField::Key], true);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
    }

    #[test]
    fn sort_two_fields_date_then_key() {
        let mut entries = vec![
            make_entry("c.txt", 300, 2024, 1),
            make_entry("a.txt", 100, 2024, 1),
            make_entry("b.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &[SortField::Date, SortField::Key], false);
        // Same date (Jan): tiebreak by key -> a < c
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[1].key(), "c.txt");
        // Feb entry last
        assert_eq!(entries[2].key(), "b.txt");
    }

    #[test]
    fn sort_two_fields_size_then_date() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("b.txt", 100, 2024, 1),
            make_entry("c.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &[SortField::Size, SortField::Date], false);
        // Same size (100): tiebreak by date -> Jan < Mar
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
        // Larger size last
        assert_eq!(entries[2].key(), "c.txt");
    }

    #[test]
    fn sort_single_field_no_tiebreaker() {
        // Two entries with same key -- order is stable but no secondary sort
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("a.txt", 200, 2024, 1),
        ];
        sort_entries(&mut entries, &[SortField::Key], false);
        // Both have same key, no tiebreaker -- stable sort preserves input order
        assert_eq!(entries[0].size(), 100);
        assert_eq!(entries[1].size(), 200);
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
        let opts = FormatOptions {
            human: true,
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        // Spec: 2024-01-01T00:00:00Z    5.4MiB data.csv
        assert!(line.contains("5.4MiB"));
        assert!(line.ends_with("data.csv"));
    }

    #[test]
    fn format_text_extra_columns_before_key() {
        let entry = make_entry("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions {
            show_etag: true,
            show_storage_class: true,
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        // Tab-delimited: date \t size \t storage_class \t etag \t key
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields.len(), 5);
        assert!(fields[0].contains("2024-01-01"));
        assert_eq!(fields[1], "1234");
        assert_eq!(fields[2], "STANDARD");
        assert_eq!(fields[3], "e"); // quotes stripped
        assert_eq!(fields[4], "readme.txt");
    }

    #[test]
    fn format_text_versioned_object() {
        // Spec: 2024-01-15T10:30:00Z       1234 abc123-version-id readme.txt
        let entry = ListEntry::Object(S3Object::Versioning {
            key: "readme.txt".to_string(),
            version_id: "abc123-version-id".to_string(),
            size: 1234,
            last_modified: chrono::Utc
                .with_ymd_and_hms(2024, 1, 15, 10, 30, 0)
                .unwrap(),
            e_tag: "\"e\"".to_string(),
            is_latest: true,
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
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
        // Spec: 2024-01-16T09:00:00Z     DELETE def456-version-id readme.txt
        let entry = ListEntry::DeleteMarker {
            key: "readme.txt".to_string(),
            version_id: "def456-version-id".to_string(),
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 16, 9, 0, 0).unwrap(),
            is_latest: false,
            owner_display_name: None,
            owner_id: None,
        };
        let opts = FormatOptions::default();
        let line = format_entry(&entry, &opts);
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
    fn sort_common_prefix_by_size_sorts_as_zero() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            ListEntry::CommonPrefix("logs/".to_string()),
        ];
        sort_entries(&mut entries, &[SortField::Size], false);
        assert_eq!(entries[0].key(), "logs/");
        assert_eq!(entries[1].key(), "a.txt");
    }

    #[test]
    fn format_ndjson_object() {
        let entry = make_entry("readme.txt", 1234, 2024, 1);
        let opts = FormatOptions::default();
        let json = format_entry_json(&entry, &opts);
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
        let json = format_entry_json(&entry, &opts);
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
        let json = format_entry_json(&entry, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "deleted.txt");
        assert_eq!(parsed["VersionId"], "v1");
        assert_eq!(parsed["IsLatest"], true);
        assert_eq!(parsed["DeleteMarker"], true);
    }

    #[test]
    fn format_ndjson_object_relative_path() {
        let entry = make_entry("logs/2024/readme.txt", 1234, 2024, 1);
        let opts = FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/2024/".to_string()),
            ..FormatOptions::default()
        };
        let json = format_entry_json(&entry, &opts);
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
        let json = format_entry_json(&entry, &opts);
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
        let json = format_entry_json(&entry, &opts);
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
        let json = format_entry_json(&entry, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Owner"]["DisplayName"], "alice");
        assert_eq!(parsed["Owner"]["ID"], "id123");
        assert_eq!(parsed["DeleteMarker"], true);
    }

    #[test]
    fn format_summary_text() {
        let stats = crate::types::ListingStatistics {
            total_objects: 42,
            total_size: 5678901,
            total_delete_markers: 0,
        };
        let summary = format_summary(&stats, false, true, false);
        assert!(summary.contains("42 objects"));
        assert!(summary.contains("5.4MiB"));
    }

    #[test]
    fn format_summary_json() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 0,
        };
        let summary = format_summary(&stats, true, false, false);
        let parsed: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(parsed["summary"]["total_objects"], 10);
        assert_eq!(parsed["summary"]["total_size"], 1024);
    }

    #[test]
    fn format_summary_with_versions() {
        let stats = crate::types::ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 3,
        };
        let summary = format_summary(&stats, false, false, true);
        assert!(summary.contains("3 delete markers"));
    }

    #[test]
    fn format_text_strips_prefix_with_relative_path() {
        let entry = make_entry("logs/2024/data.csv", 100, 2024, 1);
        let opts = FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/2024/".to_string()),
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        assert!(line.ends_with("data.csv"));
        assert!(!line.contains("logs/2024/"));
    }

    #[test]
    fn format_text_default_shows_fullpath() {
        let entry = make_entry("logs/2024/data.csv", 100, 2024, 1);
        let opts = FormatOptions {
            show_relative_path: false,
            prefix: Some("logs/2024/".to_string()),
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        assert!(line.contains("logs/2024/data.csv"));
    }

    #[test]
    fn format_text_common_prefix_strips_prefix_with_relative_path() {
        let entry = ListEntry::CommonPrefix("logs/2024/".to_string());
        let opts = FormatOptions {
            show_relative_path: true,
            prefix: Some("logs/".to_string()),
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        assert!(line.contains("PRE"));
        assert!(line.ends_with("2024/"));
        assert!(!line.contains("logs/2024/"));
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
    fn format_text_multiple_checksum_algorithms() {
        let entry = make_entry_with_checksums("file.txt", vec!["CRC32", "SHA256"]);
        let opts = FormatOptions {
            show_checksum_algorithm: true,
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        let fields: Vec<&str> = line.split('\t').collect();
        // date \t size \t checksum_algorithm \t key
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[2], "CRC32,SHA256");
    }

    #[test]
    fn format_text_single_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec!["SHA256"]);
        let opts = FormatOptions {
            show_checksum_algorithm: true,
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields[2], "SHA256");
    }

    #[test]
    fn format_text_no_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec![]);
        let opts = FormatOptions {
            show_checksum_algorithm: true,
            ..Default::default()
        };
        let line = format_entry(&entry, &opts);
        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields[2], "");
    }

    #[test]
    fn format_json_multiple_checksum_algorithms() {
        let entry = make_entry_with_checksums("file.txt", vec!["CRC32", "SHA256"]);
        let opts = FormatOptions::default();
        let json_str = format_entry_json(&entry, &opts);
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
        let json_str = format_entry_json(&entry, &opts);
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let algos = val["ChecksumAlgorithm"].as_array().unwrap();
        assert_eq!(algos.len(), 1);
        assert_eq!(algos[0], "CRC64NVME");
    }

    #[test]
    fn format_json_no_checksum_algorithm() {
        let entry = make_entry_with_checksums("file.txt", vec![]);
        let opts = FormatOptions::default();
        let json_str = format_entry_json(&entry, &opts);
        let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert!(val.get("ChecksumAlgorithm").is_none());
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
