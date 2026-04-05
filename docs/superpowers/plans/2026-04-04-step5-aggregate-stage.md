# Step 5: Aggregate Stage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the aggregate stage that collects entries from the channel, sorts them, formats output (text, human, NDJSON, extra columns), and writes to stdout with optional summary.

**Architecture:** The aggregate stage drains the channel into a `Vec<ListEntry>`, sorts by the configured field (default `Key`, with secondary sort by mtime when `--all-versions`), then formats and writes each entry to `BufWriter<Stdout>`. Formatting supports text (default), human-readable sizes, NDJSON, extra columns (ETag, storage class, checksums), and version/delete-marker display. Summary line is appended when `--summary` is set.

**Tech Stack:** Rust 2024, serde + serde_json (for NDJSON), chrono

**Depends on:** Steps 1-4 (all prior modules)

---

### Task 1: Add serde dependencies to Cargo.toml

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add serde and serde_json**

```toml
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check 2>&1 | tail -5`

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "feat(step5): add serde and serde_json for NDJSON output"
```

---

### Task 2: Implement sorting

**Files:**
- Create: `src/aggregate.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing tests for sorting**

Note: `config.sort` is `SortField` (not `Option<SortField>`) -- always present, default `Key`.
When `config.all_versions` is true, secondary sort by mtime is applied.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64, year: i32, month: u32) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(), size,
            last_modified: chrono::Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: None, checksum_type: None,
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
        // When all_versions is true and sort is Key, secondary sort by mtime
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),  // same key, later mtime
            make_entry("a.txt", 200, 2024, 1),  // same key, earlier mtime
            make_entry("b.txt", 300, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Key, false, true);
        assert_eq!(entries[0].key(), "a.txt");
        // Secondary sort: earlier mtime first
        assert_eq!(entries[0].size(), 200); // the Jan entry
        assert_eq!(entries[1].key(), "a.txt");
        assert_eq!(entries[1].size(), 100); // the Mar entry
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
        assert_eq!(entries[0].key(), "b.txt"); // Jan
        assert_eq!(entries[1].key(), "c.txt"); // Feb
        assert_eq!(entries[2].key(), "a.txt"); // Mar
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
    fn sort_common_prefix_by_size_sorts_as_zero() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            ListEntry::CommonPrefix("logs/".to_string()),
        ];
        sort_entries(&mut entries, &SortField::Size, false, false);
        assert_eq!(entries[0].key(), "logs/"); // size 0
        assert_eq!(entries[1].key(), "a.txt"); // size 100
    }
}
```

- [ ] **Step 2: Implement sort_entries function**

The sort function takes `all_versions: bool` to enable secondary mtime sort:

```rust
use crate::config::args::SortField;
use crate::types::ListEntry;

pub fn sort_entries(entries: &mut [ListEntry], field: &SortField, reverse: bool, all_versions: bool) {
    entries.sort_by(|a, b| {
        let cmp = match field {
            SortField::Key => {
                let primary = a.key().cmp(b.key());
                if all_versions && primary == std::cmp::Ordering::Equal {
                    // Secondary sort by mtime when all_versions is enabled
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
```

- [ ] **Step 3: Run tests, verify pass**
- [ ] **Step 4: Commit**

---

### Task 3: Implement text formatting

**Files:**
- Modify: `src/aggregate.rs`

- [ ] **Step 1: Write tests for text formatting**

FormatOptions references `config.display_config` fields:

```rust
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
    assert!(line.contains("5.4MiB"));
}

#[test]
fn format_text_with_etag() {
    let entry = make_entry("file.txt", 100, 2024, 1);
    let opts = FormatOptions { show_etag: true, ..Default::default() };
    let line = format_entry(&entry, None, &opts);
    assert!(line.contains("\"e\""));
}
```

- [ ] **Step 2: Implement FormatOptions and format_entry**

FormatOptions maps to `config.display_config` fields:

```rust
/// Built from config.display_config fields:
/// - human -> config.display_config.human
/// - show_fullpath -> config.display_config.show_fullpath
/// - show_etag -> config.display_config.show_etag
/// - show_storage_class -> config.display_config.show_storage_class
/// - show_checksum_algorithm -> config.display_config.show_checksum_algorithm
/// - show_checksum_type -> config.display_config.show_checksum_type
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
    /// Build from DisplayConfig
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
```

- [ ] **Step 3: Run tests, commit**

---

### Task 4: Implement NDJSON formatting

**Files:**
- Modify: `src/aggregate.rs`

- [ ] **Step 1: Write NDJSON tests**

JSON output is controlled by `config.display_config.json`:

```rust
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
        key: "deleted.txt".to_string(), version_id: "v1".to_string(),
        last_modified: chrono::Utc::now(), is_latest: true,
    };
    let json = format_entry_json(&entry);
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["key"], "deleted.txt");
    assert_eq!(parsed["delete_marker"], true);
}
```

- [ ] **Step 2: Implement format_entry_json**
- [ ] **Step 3: Run tests, commit**

---

### Task 5: Implement summary line

**Files:**
- Modify: `src/aggregate.rs`

- [ ] **Step 1: Write tests**

Summary is controlled by `config.display_config.summary`.
Human-readable sizes in summary use `config.display_config.human`.
JSON summary uses `config.display_config.json`.

```rust
#[test]
fn format_summary_text() {
    let stats = ListingStatistics {
        total_objects: 42, total_size: 5678901,
        total_versions: 0, total_delete_markers: 0,
    };
    let summary = format_summary(&stats, false, false);
    assert!(summary.contains("42 objects"));
    assert!(summary.contains("5.4MiB")); // Summary always uses human sizes
}

#[test]
fn format_summary_json() {
    let stats = ListingStatistics {
        total_objects: 10, total_size: 1024,
        total_versions: 0, total_delete_markers: 0,
    };
    let summary = format_summary(&stats, true, false);
    let parsed: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(parsed["summary"]["total_objects"], 10);
    assert_eq!(parsed["summary"]["total_size"], 1024);
}

#[test]
fn format_summary_with_versions() {
    let stats = ListingStatistics {
        total_objects: 10, total_size: 1024,
        total_versions: 15, total_delete_markers: 3,
    };
    let summary = format_summary(&stats, false, true);
    assert!(summary.contains("15 versions"));
    assert!(summary.contains("3 delete markers"));
}
```

- [ ] **Step 2: Implement format_summary and compute_statistics**
- [ ] **Step 3: Run tests, commit**

---

### Task 6: Wire aggregate into pipeline

**Files:**
- Modify: `src/pipeline.rs`

- [ ] **Step 1: Replace temporary drain with full aggregate**

Update `ListingPipeline::run()`:
1. Spawn lister (already done)
2. Drain channel into `Vec<ListEntry>`
3. Sort using `config.sort` (always present, default Key) with `config.all_versions` for secondary mtime sort:
   ```rust
   sort_entries(&mut entries, &self.config.sort, self.config.reverse, self.config.all_versions);
   ```
4. Open `BufWriter<Stdout>`
5. For each entry:
   - If `config.display_config.json`: call `format_entry_json()`
   - Else: call `format_entry()` with options from `config.display_config`
6. If `config.display_config.summary`: compute stats and append summary line
   - Use `config.display_config.json` for JSON summary format
   - Use `config.all_versions` for version/delete-marker counts
7. Flush and return

- [ ] **Step 2: Write integration test**

```rust
#[tokio::test]
async fn pipeline_sorts_and_formats_output() {
    // Use MockStorage with 3 entries
    // Configure sort by size, human output via config.display_config
    // Capture stdout, verify ordering and formatting
}
```

- [ ] **Step 3: Run full test suite**

Run: `cargo test 2>&1 | tail -20`
Expected: All tests pass

- [ ] **Step 4: Run clippy**

Run: `cargo clippy --all-targets 2>&1 | tail -20`
Expected: No warnings

- [ ] **Step 5: Commit**

```bash
git add src/aggregate.rs src/pipeline.rs src/lib.rs Cargo.toml
git commit -m "feat(step5): implement aggregate stage with sort, text/NDJSON formatting, and summary, step 5 complete"
```
