# Extract Formatter from Aggregator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `aggregate.rs` (1,864 lines) into two pipeline stages: the Aggregator (collect + sort) and a new Formatter (format + write to output), connected by an mpsc channel.

**Architecture:** The current two-stage pipeline (Lister → Aggregator) becomes three stages: Lister → Aggregator → Formatter. The Aggregator collects entries, sorts them (or passes them through in streaming mode), computes statistics, and sends `FormatterMessage` values over a channel. The Formatter receives these messages and handles all text/JSON formatting, header output, and summary writing. This mirrors the existing Lister→Aggregator channel pattern.

**Tech Stack:** Rust, tokio mpsc channels, rayon (existing sorting), serde_json (existing JSON output), byte-unit (existing size formatting)

---

### Task 1: Create `FormatterMessage` enum and `FormatterConfig`

**Files:**
- Create: `src/formatter.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test**

In `src/formatter.rs`, add a test that constructs `FormatterMessage` variants to verify the enum exists and can carry entries and statistics:

```rust
#[cfg(test)]
mod tests {
    use super::*;
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
    fn formatter_message_entry_variant() {
        let msg = FormatterMessage::Entry(make_entry("a.txt", 100));
        assert!(matches!(msg, FormatterMessage::Entry(_)));
    }

    #[test]
    fn formatter_message_summary_variant() {
        let stats = ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 0,
        };
        let msg = FormatterMessage::Summary(stats);
        assert!(matches!(msg, FormatterMessage::Summary(_)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib formatter::tests -- --nocapture`
Expected: FAIL — `formatter` module does not exist

- [ ] **Step 3: Write minimal implementation**

Create `src/formatter.rs` with the enum, config struct, and the module declaration:

```rust
use crate::types::{ListEntry, ListingStatistics};
use crate::types::token::PipelineCancellationToken;

pub enum FormatterMessage {
    Entry(ListEntry),
    Summary(ListingStatistics),
}

pub struct FormatterConfig {
    pub use_json: bool,
    pub human: bool,
    pub all_versions: bool,
    pub header: bool,
    pub cancellation_token: PipelineCancellationToken,
}
```

Add to `src/lib.rs`:
```rust
pub mod formatter;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib formatter::tests -- --nocapture`
Expected: PASS

- [ ] **Step 5: Run clippy**

Run: `cargo clippy --all-features`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/formatter.rs src/lib.rs
git commit -m "refactor: add FormatterMessage enum and FormatterConfig struct"
```

---

### Task 2: Move formatting utility functions to `formatter.rs`

Move these pure functions from `aggregate.rs` to `formatter.rs`:
- `escape_control_chars` (and keep it `pub(crate)` re-exported or move callers)
- `maybe_escape`
- `format_size`
- `format_size_split`
- `format_key_display`
- `format_rfc3339`
- `format_entry`
- `format_entry_json`
- `format_header`
- `format_summary`
- `accumulate_statistics`
- `compute_statistics`
- `FormatOptions` struct (including `from_display_config`)

**Files:**
- Modify: `src/formatter.rs`
- Modify: `src/aggregate.rs`

- [ ] **Step 1: Move `FormatOptions` and all formatting functions**

Cut these items from `src/aggregate.rs` and paste into `src/formatter.rs`:

1. `FormatOptions` struct and its `impl` block (lines 9–56)
2. `escape_control_chars` function (lines 68–89)
3. `maybe_escape` function (lines 92–98)
4. `format_size_split` function (lines 263–276)
5. `format_size` function (lines 278–292)
6. `format_key_display` function (lines 294–311)
7. `format_rfc3339` function (lines 313–320)
8. `format_entry` function (lines 322–449)
9. `format_header` function (lines 451–483)
10. `format_entry_json` function (lines 521–652)
11. `accumulate_statistics` function (lines 655–666)
12. `compute_statistics` function (lines 668–691)
13. `format_summary` function (lines 693–730)

Add the necessary imports to the top of `src/formatter.rs`:

```rust
use crate::config::args::SortField;
use crate::types::{ListEntry, ListingStatistics};
use crate::types::token::PipelineCancellationToken;
use byte_unit::Byte;
use std::borrow::Cow;
```

Make `escape_control_chars` and `maybe_escape` `pub(crate)` in `formatter.rs`.

- [ ] **Step 2: Update `aggregate.rs` to import from `formatter`**

In `aggregate.rs`, remove the moved functions and add imports:

```rust
use crate::formatter::{
    FormatOptions, accumulate_statistics, compute_statistics, escape_control_chars,
    format_entry, format_entry_json, format_header, format_summary,
};
```

The `Aggregator` struct and its `impl` block, `AggregatorConfig`, `sort_entries`, and `cmp_mtime` remain in `aggregate.rs`.

- [ ] **Step 3: Update `pipeline.rs` to import `FormatOptions` from `formatter`**

Change:
```rust
use crate::aggregate::{Aggregator, AggregatorConfig, FormatOptions};
```
to:
```rust
use crate::aggregate::{Aggregator, AggregatorConfig};
use crate::formatter::FormatOptions;
```

- [ ] **Step 4: Run all tests**

Run: `cargo test --lib -- --nocapture`
Expected: All existing tests PASS. The formatting tests that were in `aggregate.rs::tests` need to move too (next step).

- [ ] **Step 5: Move formatting tests to `formatter.rs`**

Move all tests from `aggregate.rs::tests` that test formatting/statistics functions to `formatter.rs::tests`. These are the tests that call `format_entry`, `format_entry_json`, `format_header`, `format_summary`, `escape_control_chars`, `compute_statistics`, etc.

The following tests stay in `aggregate.rs::tests` (they test sorting):
- `sort_by_key`
- `sort_by_size`
- `sort_by_date`
- `sort_reverse`
- `sort_two_fields_date_then_key`
- `sort_two_fields_size_then_date`
- `parallel_sort_by_key_produces_same_order_as_sequential`
- `parallel_sort_by_size_reverse`
- `parallel_sort_multi_field`
- `parallel_sort_threshold_boundary`
- `sort_single_field_no_tiebreaker`
- `sort_common_prefix_by_size_sorts_as_zero`

The following tests stay in `aggregate.rs::tests` (they test the Aggregator run logic with cancellation):
- `run_aggregate_skips_output_when_cancelled_mid_stream`
- `run_aggregate_skips_output_when_cancelled_after_close`
- `run_aggregate_emits_output_when_not_cancelled`
- `run_streaming_skips_summary_on_cancellation`
- `run_streaming_emits_summary_when_not_cancelled`
- `compute_statistics_counts_correctly`

Also move the `SharedBuf` helper struct and `default_aggregator_config` helper — keep them in `aggregate.rs` since the cancellation tests need them. The `make_entry` helper is needed in both files — define it in each test module independently (it's a 15-line test helper, no abstraction needed).

All other tests (formatting, escaping, JSON, summary) move to `formatter.rs::tests`.

- [ ] **Step 6: Run all tests**

Run: `cargo test -- --nocapture`
Expected: All tests PASS

- [ ] **Step 7: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 8: Commit**

```bash
git add src/aggregate.rs src/formatter.rs src/pipeline.rs
git commit -m "refactor: move formatting functions and tests from aggregate to formatter module"
```

---

### Task 3: Implement the `Formatter` struct

The `Formatter` receives `FormatterMessage` values from a channel and writes formatted output.

**Files:**
- Modify: `src/formatter.rs`

- [ ] **Step 1: Write the failing test — Formatter formats entries to output**

Add to `formatter.rs::tests`:

```rust
use tokio::sync::mpsc;

#[tokio::test]
async fn formatter_writes_entries() {
    let (tx, rx) = mpsc::channel(10);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = FormatterConfig {
        use_json: false,
        human: false,
        all_versions: false,
        header: false,
        cancellation_token: token,
    };
    let opts = FormatOptions::default();
    let formatter = Formatter::new(rx, buf.clone(), opts, config);

    tx.send(FormatterMessage::Entry(make_entry("a.txt", 100)))
        .await
        .unwrap();
    drop(tx);

    formatter.run().await.unwrap();

    let out = buf.as_string();
    assert!(out.contains("a.txt"), "expected a.txt in output, got: {out:?}");
}
```

Also add the `SharedBuf` helper struct in `formatter.rs::tests` (same implementation as in `aggregate.rs::tests`):

```rust
#[derive(Clone)]
struct SharedBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl SharedBuf {
    fn new() -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())))
    }
    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib formatter::tests::formatter_writes_entries -- --nocapture`
Expected: FAIL — `Formatter` struct doesn't exist yet

- [ ] **Step 3: Write minimal implementation**

Add to `src/formatter.rs`:

```rust
use std::io::Write;
use tokio::sync::mpsc;
use anyhow::Result;

pub struct Formatter<W: Write + Send + 'static> {
    rx: mpsc::Receiver<FormatterMessage>,
    writer: W,
    opts: FormatOptions,
    config: FormatterConfig,
}

impl<W: Write + Send + 'static> Formatter<W> {
    pub fn new(
        rx: mpsc::Receiver<FormatterMessage>,
        writer: W,
        opts: FormatOptions,
        config: FormatterConfig,
    ) -> Self {
        Self {
            rx,
            writer,
            opts,
            config,
        }
    }

    pub fn write_header(&mut self) -> Result<()> {
        writeln!(self.writer, "{}", format_header(&self.opts))?;
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                self.writer.flush()?;
                return Ok(());
            }
            match msg {
                FormatterMessage::Entry(entry) => {
                    let line = if self.config.use_json {
                        format_entry_json(&entry, &self.opts)
                    } else {
                        format_entry(&entry, &self.opts)
                    };
                    writeln!(self.writer, "{line}")?;
                }
                FormatterMessage::Summary(stats) => {
                    let summary = format_summary(
                        &stats,
                        self.config.use_json,
                        self.config.human,
                        self.config.all_versions,
                    );
                    if !self.config.use_json {
                        writeln!(self.writer)?;
                    }
                    writeln!(self.writer, "{summary}")?;
                }
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib formatter::tests::formatter_writes_entries -- --nocapture`
Expected: PASS

- [ ] **Step 5: Write additional tests — header, summary, JSON, cancellation**

Add these tests to `formatter.rs::tests`:

```rust
#[tokio::test]
async fn formatter_writes_header_when_configured() {
    let (tx, rx) = mpsc::channel(10);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = FormatterConfig {
        use_json: false,
        human: false,
        all_versions: false,
        header: true,
        cancellation_token: token,
    };
    let opts = FormatOptions::default();
    let mut formatter = Formatter::new(rx, buf.clone(), opts, config);
    formatter.write_header().unwrap();

    tx.send(FormatterMessage::Entry(make_entry("a.txt", 100)))
        .await
        .unwrap();
    drop(tx);

    formatter.run().await.unwrap();
    let out = buf.as_string();
    assert!(out.starts_with("DATE\t"), "expected header, got: {out:?}");
    assert!(out.contains("a.txt"));
}

#[tokio::test]
async fn formatter_writes_summary() {
    let (tx, rx) = mpsc::channel(10);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = FormatterConfig {
        use_json: false,
        human: false,
        all_versions: false,
        header: false,
        cancellation_token: token,
    };
    let opts = FormatOptions::default();
    let formatter = Formatter::new(rx, buf.clone(), opts, config);

    tx.send(FormatterMessage::Entry(make_entry("a.txt", 100)))
        .await
        .unwrap();
    let stats = ListingStatistics {
        total_objects: 1,
        total_size: 100,
        total_delete_markers: 0,
    };
    tx.send(FormatterMessage::Summary(stats)).await.unwrap();
    drop(tx);

    formatter.run().await.unwrap();
    let out = buf.as_string();
    assert!(out.contains("a.txt"));
    assert!(out.contains("Total:"));
}

#[tokio::test]
async fn formatter_writes_json() {
    let (tx, rx) = mpsc::channel(10);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = FormatterConfig {
        use_json: true,
        human: false,
        all_versions: false,
        header: false,
        cancellation_token: token,
    };
    let opts = FormatOptions::default();
    let formatter = Formatter::new(rx, buf.clone(), opts, config);

    tx.send(FormatterMessage::Entry(make_entry("a.txt", 100)))
        .await
        .unwrap();
    drop(tx);

    formatter.run().await.unwrap();
    let out = buf.as_string();
    let parsed: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(parsed["Key"], "a.txt");
}

#[tokio::test]
async fn formatter_skips_output_on_cancellation() {
    let (tx, rx) = mpsc::channel(10);
    let buf = SharedBuf::new();
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = FormatterConfig {
        use_json: false,
        human: false,
        all_versions: false,
        header: false,
        cancellation_token: token.clone(),
    };
    let opts = FormatOptions::default();
    let formatter = Formatter::new(rx, buf.clone(), opts, config);

    token.cancel();
    tx.send(FormatterMessage::Entry(make_entry("a.txt", 100)))
        .await
        .unwrap();
    drop(tx);

    formatter.run().await.unwrap();
    assert_eq!(buf.as_string(), "", "expected empty output on cancellation");
}
```

- [ ] **Step 6: Run all formatter tests**

Run: `cargo test --lib formatter::tests -- --nocapture`
Expected: All PASS

- [ ] **Step 7: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 8: Commit**

```bash
git add src/formatter.rs
git commit -m "refactor: implement Formatter struct with channel-based message processing"
```

---

### Task 4: Refactor Aggregator to send to Formatter via channel

Change the Aggregator so it no longer writes to a `Write` sink. Instead, it sends `FormatterMessage` values to a channel. The Formatter (spawned separately) handles all output.

**Files:**
- Modify: `src/aggregate.rs`
- Modify: `src/formatter.rs` (import only)

- [ ] **Step 1: Write the failing test — Aggregator sends sorted entries to channel**

Add to `aggregate.rs::tests`:

```rust
use crate::formatter::FormatterMessage;

#[tokio::test]
async fn aggregator_sends_sorted_entries_to_channel() {
    let (entry_tx, entry_rx) = mpsc::channel(10);
    let (fmt_tx, mut fmt_rx) = mpsc::channel(10);
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = AggregatorConfig {
        no_sort: false,
        sort_fields: vec![SortField::Key],
        reverse: false,
        summary: false,
        parallel_sort_threshold: usize::MAX,
        cancellation_token: token,
    };
    let aggregator = Aggregator::new(entry_rx, fmt_tx, config);

    entry_tx.send(make_entry("c.txt", 300, 2024, 1)).await.unwrap();
    entry_tx.send(make_entry("a.txt", 100, 2024, 2)).await.unwrap();
    entry_tx.send(make_entry("b.txt", 200, 2024, 3)).await.unwrap();
    drop(entry_tx);

    aggregator.run().await.unwrap();

    // Entries should arrive sorted by key
    let msg1 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg1, FormatterMessage::Entry(e) if e.key() == "a.txt"));
    let msg2 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg2, FormatterMessage::Entry(e) if e.key() == "b.txt"));
    let msg3 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg3, FormatterMessage::Entry(e) if e.key() == "c.txt"));
    // Channel should be closed
    assert!(fmt_rx.recv().await.is_none());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib aggregate::tests::aggregator_sends_sorted_entries_to_channel -- --nocapture`
Expected: FAIL — `Aggregator::new` signature doesn't match

- [ ] **Step 3: Rewrite `Aggregator` to use output channel instead of `Write`**

Replace the `Aggregator` struct and impl in `src/aggregate.rs`:

```rust
use crate::config::args::SortField;
use crate::formatter::FormatterMessage;
use crate::types::ListEntry;
use anyhow::Result;
use tokio::sync::mpsc;
use tracing::debug;

pub struct AggregatorConfig {
    pub no_sort: bool,
    pub sort_fields: Vec<SortField>,
    pub reverse: bool,
    pub summary: bool,
    pub parallel_sort_threshold: usize,
    pub cancellation_token: crate::types::token::PipelineCancellationToken,
}

pub struct Aggregator {
    rx: mpsc::Receiver<ListEntry>,
    tx: mpsc::Sender<FormatterMessage>,
    config: AggregatorConfig,
}

impl Aggregator {
    pub fn new(
        rx: mpsc::Receiver<ListEntry>,
        tx: mpsc::Sender<FormatterMessage>,
        config: AggregatorConfig,
    ) -> Self {
        Self { rx, tx, config }
    }

    pub async fn run(mut self) -> Result<()> {
        if self.config.no_sort {
            self.run_streaming().await
        } else {
            self.run_aggregate().await
        }
    }

    async fn run_streaming(&mut self) -> Result<()> {
        let mut stats = crate::types::ListingStatistics {
            total_objects: 0,
            total_size: 0,
            total_delete_markers: 0,
        };

        while let Some(entry) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                return Ok(());
            }
            if self.config.summary {
                crate::formatter::accumulate_statistics(&entry, &mut stats);
            }
            if self.tx.send(FormatterMessage::Entry(entry)).await.is_err() {
                return Ok(());
            }
        }

        if self.config.cancellation_token.is_cancelled() {
            return Ok(());
        }

        if self.config.summary {
            let _ = self.tx.send(FormatterMessage::Summary(stats)).await;
        }

        Ok(())
    }

    async fn run_aggregate(&mut self) -> Result<()> {
        let mut entries = Vec::new();
        while let Some(entry) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                return Ok(());
            }
            entries.push(entry);
        }

        if self.config.cancellation_token.is_cancelled() {
            return Ok(());
        }

        debug!(
            entry_count = entries.len(),
            parallel_sort_threshold = self.config.parallel_sort_threshold,
            "sort_entries started"
        );
        let sort_started = std::time::Instant::now();
        sort_entries(
            &mut entries,
            &self.config.sort_fields,
            self.config.reverse,
            self.config.parallel_sort_threshold,
        );
        debug!(
            entry_count = entries.len(),
            elapsed_ms = sort_started.elapsed().as_millis() as u64,
            "sort_entries finished"
        );

        let stats = if self.config.summary {
            Some(crate::formatter::compute_statistics(&entries))
        } else {
            None
        };

        for entry in entries {
            if self.config.cancellation_token.is_cancelled() {
                return Ok(());
            }
            if self.tx.send(FormatterMessage::Entry(entry)).await.is_err() {
                return Ok(());
            }
        }

        if let Some(stats) = stats {
            let _ = self.tx.send(FormatterMessage::Summary(stats)).await;
        }

        Ok(())
    }
}
```

Note the key changes:
- No more `W: Write` generic — the Aggregator sends `FormatterMessage` to a channel
- No more `FormatOptions` or `writer` — formatting is the Formatter's job
- `use_json`, `human`, `all_versions` removed from `AggregatorConfig` — those are formatter concerns
- `write_header()` removed — that's the Formatter's responsibility
- Statistics computed here, sent as `FormatterMessage::Summary`

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib aggregate::tests::aggregator_sends_sorted_entries_to_channel -- --nocapture`
Expected: PASS

- [ ] **Step 5: Write additional aggregator tests**

Add to `aggregate.rs::tests`:

```rust
#[tokio::test]
async fn aggregator_streams_entries_in_order_when_no_sort() {
    let (entry_tx, entry_rx) = mpsc::channel(10);
    let (fmt_tx, mut fmt_rx) = mpsc::channel(10);
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = AggregatorConfig {
        no_sort: true,
        sort_fields: vec![],
        reverse: false,
        summary: false,
        parallel_sort_threshold: usize::MAX,
        cancellation_token: token,
    };
    let aggregator = Aggregator::new(entry_rx, fmt_tx, config);

    // Send entries in c, a, b order
    entry_tx.send(make_entry("c.txt", 300, 2024, 1)).await.unwrap();
    entry_tx.send(make_entry("a.txt", 100, 2024, 2)).await.unwrap();
    entry_tx.send(make_entry("b.txt", 200, 2024, 3)).await.unwrap();
    drop(entry_tx);

    aggregator.run().await.unwrap();

    // Entries should arrive in original order (no sorting)
    let msg1 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg1, FormatterMessage::Entry(e) if e.key() == "c.txt"));
    let msg2 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg2, FormatterMessage::Entry(e) if e.key() == "a.txt"));
    let msg3 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg3, FormatterMessage::Entry(e) if e.key() == "b.txt"));
    assert!(fmt_rx.recv().await.is_none());
}

#[tokio::test]
async fn aggregator_sends_summary_when_enabled() {
    let (entry_tx, entry_rx) = mpsc::channel(10);
    let (fmt_tx, mut fmt_rx) = mpsc::channel(10);
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = AggregatorConfig {
        no_sort: false,
        sort_fields: vec![SortField::Key],
        reverse: false,
        summary: true,
        parallel_sort_threshold: usize::MAX,
        cancellation_token: token,
    };
    let aggregator = Aggregator::new(entry_rx, fmt_tx, config);

    entry_tx.send(make_entry("a.txt", 100, 2024, 1)).await.unwrap();
    entry_tx.send(make_entry("b.txt", 200, 2024, 2)).await.unwrap();
    drop(entry_tx);

    aggregator.run().await.unwrap();

    // Two entries then summary
    let msg1 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg1, FormatterMessage::Entry(_)));
    let msg2 = fmt_rx.recv().await.unwrap();
    assert!(matches!(&msg2, FormatterMessage::Entry(_)));
    let msg3 = fmt_rx.recv().await.unwrap();
    match msg3 {
        FormatterMessage::Summary(stats) => {
            assert_eq!(stats.total_objects, 2);
            assert_eq!(stats.total_size, 300);
        }
        _ => panic!("expected Summary message"),
    }
    assert!(fmt_rx.recv().await.is_none());
}

#[tokio::test]
async fn aggregator_skips_all_on_cancellation() {
    let (entry_tx, entry_rx) = mpsc::channel(10);
    let (fmt_tx, mut fmt_rx) = mpsc::channel(10);
    let token = crate::types::token::create_pipeline_cancellation_token();
    let config = AggregatorConfig {
        no_sort: false,
        sort_fields: vec![SortField::Key],
        reverse: false,
        summary: true,
        parallel_sort_threshold: usize::MAX,
        cancellation_token: token.clone(),
    };
    let aggregator = Aggregator::new(entry_rx, fmt_tx, config);

    entry_tx.send(make_entry("a.txt", 100, 2024, 1)).await.unwrap();
    token.cancel();
    drop(entry_tx);

    aggregator.run().await.unwrap();

    // Nothing should have been sent
    assert!(fmt_rx.recv().await.is_none());
}
```

- [ ] **Step 6: Update existing aggregator cancellation tests**

The old cancellation tests (`run_aggregate_skips_output_when_cancelled_mid_stream`, etc.) used `SharedBuf` and checked written output. Replace them with channel-based assertions matching the new architecture. Remove the `SharedBuf` struct from `aggregate.rs::tests`, and the `default_aggregator_config` helper — replace with the new config shape (no `use_json`, `human`, `all_versions` fields).

- [ ] **Step 7: Run all tests**

Run: `cargo test -- --nocapture`
Expected: All PASS

- [ ] **Step 8: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 9: Commit**

```bash
git add src/aggregate.rs src/formatter.rs
git commit -m "refactor: change Aggregator to send FormatterMessage via channel instead of writing output directly"
```

---

### Task 5: Wire up the three-stage pipeline

Update `pipeline.rs` to spawn three tasks: Lister → Aggregator → Formatter, connected by two channels.

**Files:**
- Modify: `src/pipeline.rs`

- [ ] **Step 1: Write the failing test — three-stage pipeline produces output**

The existing pipeline tests in `pipeline.rs` (`pipeline_runs_and_returns_success`, `pipeline_lists_mock_objects`, etc.) should keep working after the wiring change. No new test needed — the existing tests are the regression tests.

Run: `cargo test --lib pipeline::tests -- --nocapture`
Expected: FAIL — `spawn_aggregator` signature is wrong because `Aggregator::new` changed

- [ ] **Step 2: Rewrite `spawn_aggregator` and add `spawn_formatter`**

Update `src/pipeline.rs`:

```rust
use crate::aggregate::{Aggregator, AggregatorConfig};
use crate::formatter::{Formatter, FormatterConfig, FormatterMessage, FormatOptions};
use crate::config::Config;
use crate::filters::build_filter_chain;
use crate::lister::ObjectLister;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use anyhow::Result;
use std::sync::Arc;
```

Replace the `run` method's aggregator spawning section. The new flow:

```rust
pub async fn run(self) -> Result<()> {
    tracing::debug!(
        target = %self.config.target,
        recursive = self.config.recursive,
        "Starting listing pipeline"
    );

    if self.cancellation_token.is_cancelled() {
        return Ok(());
    }

    let queue_size = self.config.object_listing_queue_size as usize;
    let (lister_tx, lister_rx) = tokio::sync::mpsc::channel(queue_size);
    let (fmt_tx, fmt_rx) = tokio::sync::mpsc::channel(queue_size);

    let storage = self.build_storage().await?;

    let lister_handle = self.spawn_lister(Arc::clone(&storage), lister_tx, queue_size)?;
    let aggregator_handle = self.spawn_aggregator(lister_rx, fmt_tx)?;
    let formatter_handle = self.spawn_formatter(fmt_rx)?;

    // Wait for formatter to finish first (it's the terminal stage).
    let formatter_err = match formatter_handle.await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => {
            self.cancellation_token.cancel();
            Some(e)
        }
        Err(join_err) => {
            self.cancellation_token.cancel();
            Some(anyhow::anyhow!("Formatter task panicked: {}", join_err))
        }
    };

    // Wait for aggregator.
    let aggregator_err = match aggregator_handle.await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => {
            self.cancellation_token.cancel();
            Some(e)
        }
        Err(join_err) => {
            self.cancellation_token.cancel();
            Some(anyhow::anyhow!("Aggregator task panicked: {}", join_err))
        }
    };

    // Wait for lister.
    let lister_result = match lister_handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(join_err) => Err(anyhow::anyhow!("Lister+filter task panicked: {}", join_err)),
    };

    // Surface errors in order: formatter > aggregator > lister
    if let Some(e) = formatter_err {
        return Err(e);
    }
    if let Some(e) = aggregator_err {
        return Err(e);
    }
    lister_result?;

    tracing::debug!(
        api_calls = storage.api_call_count(),
        "Listing pipeline completed"
    );
    Ok(())
}
```

Update `spawn_aggregator`:

```rust
fn spawn_aggregator(
    &self,
    rx: tokio::sync::mpsc::Receiver<crate::types::ListEntry>,
    tx: tokio::sync::mpsc::Sender<FormatterMessage>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let aggregator_config = AggregatorConfig {
        no_sort: self.config.no_sort,
        sort_fields: self.config.sort.clone(),
        reverse: self.config.reverse,
        summary: self.config.display_config.summary,
        parallel_sort_threshold: self.config.parallel_sort_threshold as usize,
        cancellation_token: self.cancellation_token.clone(),
    };
    let aggregator = Aggregator::new(rx, tx, aggregator_config);

    Ok(tokio::spawn(async move { aggregator.run().await }))
}
```

Add `spawn_formatter`:

```rust
fn spawn_formatter(
    &self,
    rx: tokio::sync::mpsc::Receiver<FormatterMessage>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    let opts = FormatOptions::from_display_config(
        &self.config.display_config,
        self.config.target.prefix.clone(),
        self.config.all_versions,
    );
    let use_json = self.config.display_config.json;
    let writer: Box<dyn std::io::Write + Send> =
        Box::new(std::io::BufWriter::new(std::io::stdout()));

    let formatter_config = FormatterConfig {
        use_json,
        human: self.config.display_config.human,
        all_versions: self.config.all_versions,
        header: !use_json && self.config.display_config.header,
        cancellation_token: self.cancellation_token.clone(),
    };
    let mut formatter = Formatter::new(rx, writer, opts, formatter_config);

    if formatter_config.header {
        formatter.write_header()?;
    }

    Ok(tokio::spawn(async move { formatter.run().await }))
}
```

Note: `FormatterConfig` needs `Copy` or `Clone` for the `header` field check after constructing. Alternatively, move the header write into `Formatter::run()` itself gated on `self.config.header`. That's cleaner — do that instead:

In `Formatter::run()`, at the top before the message loop:
```rust
pub async fn run(mut self) -> Result<()> {
    if self.config.header {
        writeln!(self.writer, "{}", format_header(&self.opts))?;
    }
    // ... existing message loop
}
```

Then `spawn_formatter` simplifies — no need to call `write_header()` externally, and `FormatterConfig` doesn't need `Clone`. Remove the `write_header()` public method or keep it for tests.

- [ ] **Step 3: Run all pipeline tests**

Run: `cargo test --lib pipeline::tests -- --nocapture`
Expected: All PASS

- [ ] **Step 4: Run the full test suite**

Run: `cargo test -- --nocapture`
Expected: All PASS

- [ ] **Step 5: Run clippy and fmt**

Run: `cargo fmt && cargo clippy --all-features`
Expected: No errors

- [ ] **Step 6: Commit**

```bash
git add src/pipeline.rs src/aggregate.rs src/formatter.rs
git commit -m "refactor: wire three-stage pipeline Lister -> Aggregator -> Formatter"
```

---

### Task 6: Clean up and verify

Final pass: remove dead code, verify no regressions.

**Files:**
- Modify: `src/aggregate.rs` (if any dead imports remain)
- Modify: `src/lib.rs` (if any re-exports changed)

- [ ] **Step 1: Check for dead code**

Run: `cargo clippy --all-features 2>&1 | grep -E "unused|dead_code"`
Expected: No warnings. Fix any that appear.

- [ ] **Step 2: Run the full test suite including integration tests**

Run: `cargo test`
Expected: All PASS

- [ ] **Step 3: Run fmt**

Run: `cargo fmt --check`
Expected: No changes needed

- [ ] **Step 4: Verify line counts**

Run: `wc -l src/aggregate.rs src/formatter.rs`

Expected: `aggregate.rs` should be significantly smaller (sorting + channel logic only, roughly ~200-300 lines including tests). `formatter.rs` should contain the formatting code (~800-1000 lines including tests).

- [ ] **Step 5: Commit any cleanup**

```bash
git add -u
git commit -m "refactor: clean up dead code after aggregator/formatter split"
```

---

## Summary of final architecture

```
[Lister + Filter Chain] ──[mpsc<ListEntry>]──> Aggregator ──[mpsc<DisplayMessage>]──> DisplayWriter ──> stdout
                                                (collect+sort)                       (Box<dyn EntryFormatter>)
```

- **`aggregate.rs`**: `Aggregator`, `AggregatorConfig`, `sort_entries`, `cmp_mtime`, sorting tests
- **`display/mod.rs`**: `EntryFormatter` trait, `FormatOptions`, shared helpers, statistics
- **`display/tsv.rs`**: `TsvFormatter` — tab-delimited output
- **`display/json.rs`**: `JsonFormatter` — NDJSON output
- **`display_writer.rs`**: `DisplayWriter`, `DisplayWriterConfig`, `DisplayMessage`, pipeline stage tests
- **`pipeline.rs`**: Three `tokio::spawn` tasks, two channels, error precedence: DisplayWriter > Aggregator > Lister
