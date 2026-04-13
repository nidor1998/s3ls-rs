# EntryFormatter Polymorphism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `if use_json` branching with an `EntryFormatter` trait and two implementations (`TextFormatter`, `JsonFormatter`), splitting `display.rs` into `display/mod.rs`, `display/text.rs`, and `display/json.rs`.

**Architecture:** A strategy pattern where `DisplayWriter` holds a `Box<dyn EntryFormatter>`. The trait defines `format_entry`, `format_header`, and `format_summary`. `TextFormatter` and `JsonFormatter` each own a `FormatOptions` and implement the trait. The pipeline constructs the appropriate formatter at startup based on `--json`. `DisplayWriterConfig` drops `use_json`, `human`, and `all_versions` since those are now formatter concerns.

**Tech Stack:** Rust, serde_json (JSON formatting), byte-unit (size formatting), chrono (timestamps)

---

## File Structure

| File | Responsibility |
|------|----------------|
| `src/display/mod.rs` | `EntryFormatter` trait, `FormatOptions`, shared helpers (`escape_control_chars`, `maybe_escape`, `format_size`, `format_size_split`, `format_key_display`, `format_rfc3339`), `accumulate_statistics`, `compute_statistics` |
| `src/display/text.rs` | `TextFormatter` struct, `impl EntryFormatter` for text: `format_entry`, `format_header`, `format_summary` |
| `src/display/json.rs` | `JsonFormatter` struct, `impl EntryFormatter` for JSON: `format_entry_json`, `format_summary_json` |
| `src/display_writer.rs` | `DisplayWriter` uses `Box<dyn EntryFormatter>`, `DisplayWriterConfig` simplified (no `use_json`/`human`/`all_versions`) |
| `src/pipeline.rs` | Constructs `TextFormatter` or `JsonFormatter` based on `--json`, passes to `DisplayWriter` |

---

### Task 1: Create `display/mod.rs` with `EntryFormatter` trait and shared helpers

Convert `src/display.rs` (flat file) into `src/display/mod.rs` (directory module) and add the `EntryFormatter` trait. Move only the shared helpers and their tests; leave `format_entry`, `format_entry_json`, `format_header`, and `format_summary` in place temporarily (they move in Tasks 2–3).

**Files:**
- Rename: `src/display.rs` → `src/display/mod.rs`
- Modify: `src/display/mod.rs` (add trait)

- [ ] **Step 1: Convert flat file to directory module**

```bash
mkdir src/display
git mv src/display.rs src/display/mod.rs
```

- [ ] **Step 2: Verify the rename compiles**

Run: `cargo test --lib`
Expected: All tests PASS (module path `crate::display` unchanged)

- [ ] **Step 3: Add the `EntryFormatter` trait**

Add this to the top of `src/display/mod.rs`, after the existing imports:

```rust
use crate::types::{ListEntry, ListingStatistics};

pub trait EntryFormatter: Send {
    /// Format a single entry as one output line.
    fn format_entry(&self, entry: &ListEntry) -> String;

    /// Return the header line, or `None` if this format has no header (e.g. JSON).
    fn format_header(&self) -> Option<String>;

    /// Format the summary. Text format includes a leading `\n` separator.
    fn format_summary(&self, stats: &ListingStatistics) -> String;
}
```

Note: the `ListEntry` and `ListingStatistics` imports already exist — just add the trait definition.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib`
Expected: All PASS

- [ ] **Step 5: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/display/ src/display.rs
git commit -m "refactor: convert display.rs to display/ module directory, add EntryFormatter trait"
```

---

### Task 2: Create `TextFormatter` in `display/text.rs`

Extract text-specific formatting into a `TextFormatter` struct that implements `EntryFormatter`.

**Files:**
- Create: `src/display/text.rs`
- Modify: `src/display/mod.rs` (add `pub mod text;`, remove moved functions)

- [ ] **Step 1: Write the failing test**

Create `src/display/text.rs` with a test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::FormatOptions;
    use crate::types::{ListEntry, ListingStatistics, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(),
            size,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
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
    fn text_formatter_format_entry_basic() {
        let fmt = TextFormatter::new(FormatOptions::default());
        let entry = make_entry("readme.txt", 1234);
        let line = fmt.format_entry(&entry);
        assert!(line.contains("2024-01-01T00:00:00Z"));
        assert!(line.contains("1234"));
        assert!(line.ends_with("readme.txt"));
    }

    #[test]
    fn text_formatter_format_header() {
        let fmt = TextFormatter::new(FormatOptions::default());
        let header = fmt.format_header();
        assert!(header.is_some());
        let h = header.unwrap();
        assert!(h.starts_with("DATE\t"));
        assert!(h.ends_with("KEY"));
    }

    #[test]
    fn text_formatter_format_summary() {
        let fmt = TextFormatter::new(FormatOptions {
            human: true,
            ..Default::default()
        });
        let stats = ListingStatistics {
            total_objects: 42,
            total_size: 5678901,
            total_delete_markers: 0,
        };
        let summary = fmt.format_summary(&stats);
        assert!(summary.starts_with('\n'));
        assert!(summary.contains("Total:"));
        assert!(summary.contains("5.4"));
    }
}
```

- [ ] **Step 2: Add `pub mod text;` to `src/display/mod.rs`**

Add at the top of `src/display/mod.rs`:

```rust
pub mod text;
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib display::text::tests`
Expected: FAIL — `TextFormatter` does not exist

- [ ] **Step 4: Implement `TextFormatter`**

Add to the top of `src/display/text.rs`:

```rust
use crate::display::{
    EntryFormatter, FormatOptions, format_key_display, format_rfc3339, format_size,
    format_size_split, maybe_escape,
};
use crate::types::{ListEntry, ListingStatistics};

pub struct TextFormatter {
    opts: FormatOptions,
}

impl TextFormatter {
    pub fn new(opts: FormatOptions) -> Self {
        Self { opts }
    }
}

impl EntryFormatter for TextFormatter {
    fn format_entry(&self, entry: &ListEntry) -> String {
        // Move the body of the existing `format_entry` function here,
        // replacing `opts` parameter references with `&self.opts`
        let mut cols: Vec<String> = Vec::new();
        // ... (full body of existing format_entry from display/mod.rs)
        // Use self.opts instead of opts parameter
        todo!()
    }

    fn format_header(&self) -> Option<String> {
        // Move the body of the existing `format_header` function here
        let mut cols: Vec<&str> = Vec::new();
        // ... (full body of existing format_header from display/mod.rs)
        // Use self.opts instead of opts parameter
        Some(todo!())
    }

    fn format_summary(&self, stats: &ListingStatistics) -> String {
        // Text summary with leading \n separator
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
```

The `format_entry` and `format_header` methods contain the **exact same logic** as the existing `format_entry(entry, opts)` and `format_header(opts)` functions in `display/mod.rs`, but using `&self.opts` instead of the `opts` parameter.

Make the shared helpers in `display/mod.rs` that `TextFormatter` needs `pub(crate)` visible:
- `format_key_display` → `pub(crate)`
- `format_rfc3339` → `pub(crate)`
- `format_size` → `pub(crate)`
- `format_size_split` → `pub(crate)`
- `maybe_escape` → `pub(crate)`

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib display::text::tests`
Expected: All PASS

- [ ] **Step 6: Move all text formatting tests from `display/mod.rs` to `display/text.rs`**

Move these tests from `display/mod.rs::tests` to `display/text.rs::tests`, updating them to use `TextFormatter` (i.e., construct a `TextFormatter::new(opts)` and call `fmt.format_entry(&entry)` instead of `format_entry(&entry, &opts)`):

- `format_text_basic_object`
- `format_text_common_prefix`
- `format_text_human_size`
- `format_text_extra_columns_before_key`
- `format_text_versioned_object`
- `format_text_common_prefix_aligns_with_versioned_object`
- `format_text_common_prefix_no_version_placeholder_without_all_versions`
- `format_text_delete_marker`
- `format_text_delete_marker_emits_owner_when_show_owner`
- `format_text_escapes_malicious_key_by_default`
- `format_text_preserves_malicious_key_when_raw_output`
- `format_text_escapes_owner_fields`
- `format_text_escapes_delete_marker_owner`
- `format_text_escapes_common_prefix`
- `format_text_local_time`
- `format_text_utc_time_default`
- `format_text_strips_prefix_with_relative_path`
- `format_text_default_shows_fullpath`
- `format_text_common_prefix_strips_prefix_with_relative_path`
- `format_text_multiple_checksum_algorithms`
- `format_text_single_checksum_algorithm`
- `format_text_no_checksum_algorithm`
- `format_summary_text`
- `format_summary_text_non_human`
- `format_summary_with_versions`
- `make_entry_with_checksums` helper (also needed in text tests)

- [ ] **Step 7: Remove the old `format_entry` and `format_header` functions from `display/mod.rs`**

Remove:
- `pub fn format_entry(entry: &ListEntry, opts: &FormatOptions) -> String`
- `pub fn format_header(opts: &FormatOptions) -> String`
- The text branch of `format_summary` (remove the whole function for now — JSON branch moves in Task 3)

- [ ] **Step 8: Update imports in files that called the removed functions**

**`src/display_writer.rs`**: Remove `format_entry`, `format_header`, `format_summary` from the import. These will be replaced by trait calls in Task 4.

Temporarily, to keep compilation working, you can keep the old functions as thin wrappers that delegate to the new `TextFormatter` (these get removed in Task 4):

```rust
// Temporary compatibility wrappers — removed in Task 4
pub fn format_entry(entry: &ListEntry, opts: &FormatOptions) -> String {
    text::TextFormatter::new_ref(opts).format_entry(entry)
}
```

Actually, it's simpler to just keep `display_writer.rs` importing the `TextFormatter` directly for now and using it inline. But since Task 4 changes the DisplayWriter anyway, the cleanest approach is: **keep the old functions in `display/mod.rs` for now as thin wrappers**, then remove them in Task 4.

- [ ] **Step 9: Run all tests**

Run: `cargo test`
Expected: All PASS

- [ ] **Step 10: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 11: Commit**

```bash
git add src/display/
git commit -m "refactor: extract TextFormatter implementing EntryFormatter trait"
```

---

### Task 3: Create `JsonFormatter` in `display/json.rs`

Extract JSON-specific formatting into a `JsonFormatter` struct that implements `EntryFormatter`.

**Files:**
- Create: `src/display/json.rs`
- Modify: `src/display/mod.rs` (add `pub mod json;`, remove moved functions)

- [ ] **Step 1: Write the failing test**

Create `src/display/json.rs` with a test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::FormatOptions;
    use crate::types::{ListEntry, ListingStatistics, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(),
            size,
            last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
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
    fn json_formatter_format_entry_basic() {
        let fmt = JsonFormatter::new(FormatOptions::default());
        let entry = make_entry("readme.txt", 1234);
        let json = fmt.format_entry(&entry);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["Key"], "readme.txt");
        assert_eq!(parsed["Size"], 1234);
    }

    #[test]
    fn json_formatter_format_header_returns_none() {
        let fmt = JsonFormatter::new(FormatOptions::default());
        assert!(fmt.format_header().is_none());
    }

    #[test]
    fn json_formatter_format_summary() {
        let fmt = JsonFormatter::new(FormatOptions::default());
        let stats = ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 0,
        };
        let summary = fmt.format_summary(&stats);
        assert!(!summary.starts_with('\n'));
        let parsed: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(parsed["Summary"]["TotalObjects"], 10);
        assert_eq!(parsed["Summary"]["TotalSize"], 1024);
    }
}
```

- [ ] **Step 2: Add `pub mod json;` to `src/display/mod.rs`**

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test --lib display::json::tests`
Expected: FAIL — `JsonFormatter` does not exist

- [ ] **Step 4: Implement `JsonFormatter`**

Add to the top of `src/display/json.rs`:

```rust
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
        // Move the body of the existing `format_entry_json` function here,
        // replacing `opts` parameter references with `&self.opts`
        // ... (full body of existing format_entry_json from display/mod.rs)
        todo!()
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
```

The `format_entry` method contains the **exact same logic** as the existing `format_entry_json(entry, opts)` function, using `&self.opts` instead of the `opts` parameter.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --lib display::json::tests`
Expected: All PASS

- [ ] **Step 6: Move all JSON formatting tests from `display/mod.rs` to `display/json.rs`**

Move these tests, updating to use `JsonFormatter`:

- `format_ndjson_object`
- `format_ndjson_common_prefix`
- `format_ndjson_delete_marker`
- `format_json_preserves_control_chars`
- `format_json_local_time`
- `format_json_utc_time_default`
- `format_ndjson_object_relative_path`
- `format_ndjson_common_prefix_relative_path`
- `format_ndjson_delete_marker_relative_path`
- `format_ndjson_delete_marker_with_owner`
- `format_summary_json`
- `format_json_multiple_checksum_algorithms`
- `format_json_single_checksum_algorithm`
- `format_json_no_checksum_algorithm`

- [ ] **Step 7: Remove old `format_entry_json` and `format_summary` from `display/mod.rs`**

Remove:
- `pub fn format_entry_json(entry: &ListEntry, opts: &FormatOptions) -> String`
- `pub fn format_summary(...)` (if any wrapper remains from Task 2)

If there are still temporary wrapper functions from Task 2, remove those too — they're no longer needed after this task.

- [ ] **Step 8: Run all tests**

Run: `cargo test`
Expected: All PASS (display_writer.rs tests may fail if they still call removed functions — see note below)

**Note:** If `display_writer.rs` still imports the removed functions, it won't compile. For now, update the imports in `display_writer.rs` to use `TextFormatter`/`JsonFormatter` directly in its existing branching logic. Task 4 replaces this with the trait-based approach, but compilation must work here:

```rust
use crate::display::text::TextFormatter;
use crate::display::json::JsonFormatter;
use crate::display::EntryFormatter;
```

And in `run()`:
```rust
DisplayMessage::Entry(entry) => {
    let line = if self.config.use_json {
        JsonFormatter::new(/* ... */).format_entry(&entry)
    } else {
        TextFormatter::new(/* ... */).format_entry(&entry)
    };
    // ...
}
```

Actually, this is wasteful (creating formatters per entry). The simpler bridge: keep `display_writer.rs` unchanged for now by keeping the old functions as wrappers in `display/mod.rs` that delegate to the formatters. Remove them in Task 4.

```rust
// Temporary wrappers — removed in Task 4 when DisplayWriter uses Box<dyn EntryFormatter>
pub fn format_entry(entry: &ListEntry, opts: &FormatOptions) -> String {
    text::TextFormatter::new_borrowed(opts).format_entry_borrowed(entry)
}
```

Wait — this doesn't work because `TextFormatter::new` takes ownership of `FormatOptions`. The cleanest bridge: keep the old standalone functions as wrappers that construct a temporary formatter, cloning opts. But `FormatOptions` doesn't derive `Clone`.

**Simplest approach:** Add `#[derive(Clone)]` to `FormatOptions`, then the wrappers work:

```rust
pub fn format_entry(entry: &ListEntry, opts: &FormatOptions) -> String {
    text::TextFormatter::new(opts.clone()).format_entry(entry)
}
pub fn format_entry_json(entry: &ListEntry, opts: &FormatOptions) -> String {
    json::JsonFormatter::new(opts.clone()).format_entry(entry)
}
pub fn format_header(opts: &FormatOptions) -> String {
    text::TextFormatter::new(opts.clone()).format_header().unwrap()
}
pub fn format_summary(stats: &ListingStatistics, json: bool, human: bool, all_versions: bool) -> String {
    if json {
        let opts = FormatOptions { all_versions, ..Default::default() };
        json::JsonFormatter::new(opts).format_summary(stats)
    } else {
        let opts = FormatOptions { human, all_versions, ..Default::default() };
        text::TextFormatter::new(opts).format_summary(stats)
    }
}
```

These wrappers keep `display_writer.rs` and `bucket_lister.rs` compiling until Task 4.

- [ ] **Step 9: Run all tests**

Run: `cargo test`
Expected: All PASS

- [ ] **Step 10: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 11: Commit**

```bash
git add src/display/
git commit -m "refactor: extract JsonFormatter implementing EntryFormatter trait"
```

---

### Task 4: Update `DisplayWriter` to use `Box<dyn EntryFormatter>`

Replace branching in `DisplayWriter` with the strategy pattern.

**Files:**
- Modify: `src/display_writer.rs`
- Modify: `src/pipeline.rs`
- Modify: `src/display/mod.rs` (remove temporary wrapper functions)

- [ ] **Step 1: Write the failing test — DisplayWriter with TextFormatter**

Update the existing test `formatter_writes_entries` in `display_writer.rs` to use `Box<dyn EntryFormatter>`:

```rust
use crate::display::text::TextFormatter;
use crate::display::{EntryFormatter, FormatOptions};

#[tokio::test]
async fn display_writer_writes_text_entries() {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = DisplayWriterConfig {
        header: false,
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(TextFormatter::new(FormatOptions::default()));
    let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

    tx.send(DisplayMessage::Entry(Box::new(make_entry("hello.txt", 42))))
        .await
        .unwrap();
    drop(tx);

    display_writer.run().await.unwrap();
    let output = buf.as_string();
    assert!(output.contains("hello.txt"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib display_writer::tests::display_writer_writes_text_entries`
Expected: FAIL — `DisplayWriter::new` signature mismatch

- [ ] **Step 3: Update `DisplayWriter` struct and impl**

Replace `display_writer.rs` content (above `#[cfg(test)]`):

```rust
use std::io::Write;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::display::EntryFormatter;
use crate::types::ListingStatistics;
use crate::types::token::PipelineCancellationToken;

pub enum DisplayMessage {
    Entry(Box<crate::types::ListEntry>),
    Summary(ListingStatistics),
}

pub struct DisplayWriterConfig {
    pub header: bool,
    pub cancellation_token: PipelineCancellationToken,
}

pub struct DisplayWriter<W: Write + Send + 'static> {
    rx: mpsc::Receiver<DisplayMessage>,
    writer: W,
    formatter: Box<dyn EntryFormatter>,
    config: DisplayWriterConfig,
}

impl<W: Write + Send + 'static> DisplayWriter<W> {
    pub fn new(
        rx: mpsc::Receiver<DisplayMessage>,
        writer: W,
        formatter: Box<dyn EntryFormatter>,
        config: DisplayWriterConfig,
    ) -> Self {
        Self {
            rx,
            writer,
            formatter,
            config,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        if self.config.header {
            if let Some(header) = self.formatter.format_header() {
                writeln!(self.writer, "{header}")?;
            }
        }

        while let Some(msg) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                self.writer.flush()?;
                return Ok(());
            }
            match msg {
                DisplayMessage::Entry(entry) => {
                    writeln!(self.writer, "{}", self.formatter.format_entry(&entry))?;
                }
                DisplayMessage::Summary(stats) => {
                    writeln!(self.writer, "{}", self.formatter.format_summary(&stats))?;
                }
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}
```

Note: no `if use_json` anywhere. No `FormatOptions` field. No `write_header()` method. The `run()` loop is completely format-agnostic.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib display_writer::tests::display_writer_writes_text_entries`
Expected: PASS

- [ ] **Step 5: Update all `DisplayWriter` tests**

Rewrite the existing tests to construct the appropriate formatter:

```rust
use crate::display::json::JsonFormatter;
use crate::display::text::TextFormatter;
use crate::display::{EntryFormatter, FormatOptions};

// ... existing SharedBuf and make_entry helpers stay the same ...

#[tokio::test]
async fn display_writer_writes_text_entries() {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = DisplayWriterConfig {
        header: false,
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(TextFormatter::new(FormatOptions::default()));
    let dw = DisplayWriter::new(rx, buf.clone(), formatter, config);

    tx.send(DisplayMessage::Entry(Box::new(make_entry("hello.txt", 42))))
        .await
        .unwrap();
    drop(tx);
    dw.run().await.unwrap();
    assert!(buf.as_string().contains("hello.txt"));
}

#[tokio::test]
async fn display_writer_writes_header_when_configured() {
    let (_tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = DisplayWriterConfig {
        header: true,
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(TextFormatter::new(FormatOptions::default()));
    let dw = DisplayWriter::new(rx, buf.clone(), formatter, config);

    drop(_tx);
    dw.run().await.unwrap();
    assert!(buf.as_string().starts_with("DATE\t"));
}

#[tokio::test]
async fn display_writer_json_skips_header() {
    let (_tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = DisplayWriterConfig {
        header: true,  // header requested, but JSON has no header
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(JsonFormatter::new(FormatOptions::default()));
    let dw = DisplayWriter::new(rx, buf.clone(), formatter, config);

    drop(_tx);
    dw.run().await.unwrap();
    assert!(buf.as_string().is_empty());
}

#[tokio::test]
async fn display_writer_writes_summary() {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = DisplayWriterConfig {
        header: false,
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(TextFormatter::new(FormatOptions::default()));
    let dw = DisplayWriter::new(rx, buf.clone(), formatter, config);

    tx.send(DisplayMessage::Entry(Box::new(make_entry("a.txt", 100))))
        .await
        .unwrap();
    tx.send(DisplayMessage::Summary(ListingStatistics {
        total_objects: 1,
        total_size: 100,
        total_delete_markers: 0,
    }))
    .await
    .unwrap();
    drop(tx);
    dw.run().await.unwrap();
    assert!(buf.as_string().contains("Total:"));
}

#[tokio::test]
async fn display_writer_writes_json() {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = DisplayWriterConfig {
        header: false,
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(JsonFormatter::new(FormatOptions::default()));
    let dw = DisplayWriter::new(rx, buf.clone(), formatter, config);

    tx.send(DisplayMessage::Entry(Box::new(make_entry("test.json", 50))))
        .await
        .unwrap();
    drop(tx);
    dw.run().await.unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(buf.as_string().trim()).unwrap();
    assert_eq!(parsed["Key"], "test.json");
}

#[tokio::test]
async fn display_writer_skips_output_on_cancellation() {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    token.cancel();
    let config = DisplayWriterConfig {
        header: false,
        cancellation_token: token,
    };
    let formatter: Box<dyn EntryFormatter> =
        Box::new(TextFormatter::new(FormatOptions::default()));
    let dw = DisplayWriter::new(rx, buf.clone(), formatter, config);

    tx.send(DisplayMessage::Entry(Box::new(make_entry("x.txt", 1))))
        .await
        .unwrap();
    drop(tx);
    dw.run().await.unwrap();
    assert!(buf.as_string().is_empty());
}
```

- [ ] **Step 6: Update `pipeline.rs` — construct formatter in `spawn_display_writer`**

```rust
use crate::display::text::TextFormatter;
use crate::display::json::JsonFormatter;
use crate::display::{EntryFormatter, FormatOptions};
use crate::display_writer::{DisplayWriter, DisplayWriterConfig, DisplayMessage};

// ... in spawn_display_writer:
fn spawn_display_writer(
    &self,
    rx: tokio::sync::mpsc::Receiver<DisplayMessage>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let opts = FormatOptions::from_display_config(
        &self.config.display_config,
        self.config.target.prefix.clone(),
        self.config.all_versions,
    );
    let writer: Box<dyn std::io::Write + Send> =
        Box::new(std::io::BufWriter::new(std::io::stdout()));

    let formatter: Box<dyn EntryFormatter> = if self.config.display_config.json {
        Box::new(JsonFormatter::new(opts))
    } else {
        Box::new(TextFormatter::new(opts))
    };

    let display_writer_config = DisplayWriterConfig {
        header: self.config.display_config.header,
        cancellation_token: self.cancellation_token.clone(),
    };
    let display_writer = DisplayWriter::new(rx, writer, formatter, display_writer_config);

    Ok(tokio::spawn(async move { display_writer.run().await }))
}
```

Note: `header` is no longer gated by `!use_json` — the `JsonFormatter::format_header()` returns `None`, so the header is naturally skipped for JSON.

- [ ] **Step 7: Remove temporary wrapper functions from `display/mod.rs`**

Remove these functions that were kept as compatibility bridges:
- `format_entry`
- `format_entry_json`
- `format_header`
- `format_summary`

Also remove their imports from any file that was using them. After this step, all formatting goes through the `EntryFormatter` trait.

Update `src/aggregate.rs` if it imported `format_entry` etc. — it shouldn't need them anymore (it only needs `accumulate_statistics` and `compute_statistics` which stay in `display/mod.rs`).

- [ ] **Step 8: Run all tests**

Run: `cargo test`
Expected: All PASS

- [ ] **Step 9: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 10: Commit**

```bash
git add src/display_writer.rs src/pipeline.rs src/display/
git commit -m "refactor: DisplayWriter uses Box<dyn EntryFormatter> strategy pattern, eliminating if-json branching"
```

---

### Task 5: Clean up and verify

Final pass: remove dead code, verify line counts, run full suite.

**Files:**
- Modify: `src/display/mod.rs` (remove any dead code)
- Modify: `src/lib.rs` (if needed)

- [ ] **Step 1: Check for dead code**

Run: `cargo clippy --all-features 2>&1 | grep -E "unused|dead_code"`
Expected: No warnings. Fix any that appear.

- [ ] **Step 2: Remove `#[derive(Clone)]` from `FormatOptions` if no longer needed**

Check if anything still clones `FormatOptions`. If the temporary wrappers are gone (removed in Task 4 Step 7), `Clone` may no longer be needed. Remove it if unused.

- [ ] **Step 3: Run the full test suite**

Run: `cargo test`
Expected: All PASS

- [ ] **Step 4: Run fmt**

Run: `cargo fmt --check`
Expected: No changes needed

- [ ] **Step 5: Verify line counts**

Run: `wc -l src/display/mod.rs src/display/text.rs src/display/json.rs src/display_writer.rs`

Expected approximate sizes:
- `display/mod.rs`: ~250 lines (shared helpers + trait + utility tests)
- `display/text.rs`: ~450 lines (text formatting + tests)
- `display/json.rs`: ~350 lines (JSON formatting + tests)
- `display_writer.rs`: ~200 lines (pipeline stage + tests)

- [ ] **Step 6: Commit any cleanup**

```bash
git add -u
git commit -m "refactor: clean up dead code after EntryFormatter polymorphism"
```

---

## Summary of final architecture

```
pipeline.rs constructs:
  if --json → JsonFormatter
  else      → TextFormatter

Lister ──[mpsc<ListEntry>]──> Aggregator ──[mpsc<DisplayMessage>]──> DisplayWriter ──> stdout
                                (sort)                                (Box<dyn EntryFormatter>)
```

- **`display/mod.rs`**: `EntryFormatter` trait, `FormatOptions`, shared helpers, statistics
- **`display/text.rs`**: `TextFormatter` — tab-delimited text output
- **`display/json.rs`**: `JsonFormatter` — NDJSON output
- **`display_writer.rs`**: `DisplayWriter` — format-agnostic pipeline stage
- **`pipeline.rs`**: Constructs the right formatter, passes `Box<dyn EntryFormatter>` to `DisplayWriter`
