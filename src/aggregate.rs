use crate::config::args::SortField;
use crate::display::{accumulate_statistics, compute_statistics};
use crate::display_writer::DisplayMessage;
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
    tx: mpsc::Sender<DisplayMessage>,
    config: AggregatorConfig,
}

impl Aggregator {
    pub fn new(
        rx: mpsc::Receiver<ListEntry>,
        tx: mpsc::Sender<DisplayMessage>,
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
                accumulate_statistics(&entry, &mut stats);
            }
            if self
                .tx
                .send(DisplayMessage::Entry(Box::new(entry)))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        if self.config.cancellation_token.is_cancelled() {
            return Ok(());
        }

        if self.config.summary {
            let _ = self.tx.send(DisplayMessage::Summary(stats)).await;
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
            Some(compute_statistics(&entries))
        } else {
            None
        };

        for entry in entries {
            if self.config.cancellation_token.is_cancelled() {
                return Ok(());
            }
            if self
                .tx
                .send(DisplayMessage::Entry(Box::new(entry)))
                .await
                .is_err()
            {
                return Ok(());
            }
        }

        if let Some(stats) = stats {
            let _ = self.tx.send(DisplayMessage::Summary(stats)).await;
        }

        Ok(())
    }
}

fn cmp_mtime(a: &ListEntry, b: &ListEntry) -> std::cmp::Ordering {
    match (a.last_modified(), b.last_modified()) {
        (Some(at), Some(bt)) => at.cmp(bt),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub fn sort_entries(
    entries: &mut [ListEntry],
    fields: &[SortField],
    reverse: bool,
    parallel_sort_threshold: usize,
) {
    let cmp_fn = |a: &ListEntry, b: &ListEntry| {
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
    };

    if entries.len() >= parallel_sort_threshold {
        use rayon::slice::ParallelSliceMut;
        entries.par_sort_by(cmp_fn);
    } else {
        entries.sort_by(cmp_fn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::SortField;
    use crate::display_writer::DisplayMessage;
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
        sort_entries(&mut entries, &[SortField::Key], false, usize::MAX);
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
        sort_entries(&mut entries, &[SortField::Size], false, usize::MAX);
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
        sort_entries(&mut entries, &[SortField::Date], false, usize::MAX);
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
        sort_entries(&mut entries, &[SortField::Key], true, usize::MAX);
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
        sort_entries(
            &mut entries,
            &[SortField::Date, SortField::Key],
            false,
            usize::MAX,
        );
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
        sort_entries(
            &mut entries,
            &[SortField::Size, SortField::Date],
            false,
            usize::MAX,
        );
        // Same size (100): tiebreak by date -> Jan < Mar
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
        // Larger size last
        assert_eq!(entries[2].key(), "c.txt");
    }

    // ========================================================================
    // Parallel sort (rayon) tests — use a low threshold so the parallel path
    // is actually exercised.
    // ========================================================================

    #[test]
    fn parallel_sort_by_key_produces_same_order_as_sequential() {
        // 200 entries with keys in pseudo-random order
        let mut seq_entries: Vec<ListEntry> = (0..200)
            .map(|i| make_entry(&format!("file_{:05}.txt", (i * 37) % 200), 100, 2024, 1))
            .collect();
        let mut par_entries = seq_entries.clone();

        // Sequential (threshold = MAX never triggers parallel)
        sort_entries(&mut seq_entries, &[SortField::Key], false, usize::MAX);
        // Parallel (threshold = 0 always triggers parallel)
        sort_entries(&mut par_entries, &[SortField::Key], false, 0);

        let seq_keys: Vec<&str> = seq_entries.iter().map(|e| e.key()).collect();
        let par_keys: Vec<&str> = par_entries.iter().map(|e| e.key()).collect();
        assert_eq!(seq_keys, par_keys);

        // And verify actually sorted
        let mut expected = par_keys.clone();
        expected.sort();
        assert_eq!(par_keys, expected);
    }

    #[test]
    fn parallel_sort_by_size_reverse() {
        let mut entries: Vec<ListEntry> = (0..500)
            .map(|i| make_entry(&format!("f{i}.txt"), ((i * 11) % 500) as u64, 2024, 1))
            .collect();

        // threshold = 100 triggers parallel path for 500 entries
        sort_entries(&mut entries, &[SortField::Size], true, 100);

        // Verify sorted descending by size
        for window in entries.windows(2) {
            assert!(
                window[0].size() >= window[1].size(),
                "not sorted descending: {} then {}",
                window[0].size(),
                window[1].size()
            );
        }
    }

    #[test]
    fn parallel_sort_multi_field() {
        // Mix of sizes and dates; sort by Size then Date
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("b.txt", 100, 2024, 1),
            make_entry("c.txt", 200, 2024, 2),
            make_entry("d.txt", 100, 2024, 2),
            make_entry("e.txt", 200, 2024, 1),
        ];
        sort_entries(
            &mut entries,
            &[SortField::Size, SortField::Date],
            false,
            0, // force parallel
        );
        // Size 100: dates Jan, Feb, Mar → b, d, a
        // Size 200: dates Jan, Feb → e, c
        let keys: Vec<&str> = entries.iter().map(|e| e.key()).collect();
        assert_eq!(keys, vec!["b.txt", "d.txt", "a.txt", "e.txt", "c.txt"]);
    }

    #[test]
    fn parallel_sort_threshold_boundary() {
        // Exactly at threshold should use parallel; below should use sequential.
        // Both must produce identical results.
        let mut at_threshold: Vec<ListEntry> = (0..10)
            .map(|i| make_entry(&format!("k{}", 9 - i), 100, 2024, 1))
            .collect();
        let mut below_threshold = at_threshold.clone();

        sort_entries(&mut at_threshold, &[SortField::Key], false, 10);
        sort_entries(&mut below_threshold, &[SortField::Key], false, 11);

        let at_keys: Vec<&str> = at_threshold.iter().map(|e| e.key()).collect();
        let below_keys: Vec<&str> = below_threshold.iter().map(|e| e.key()).collect();
        assert_eq!(at_keys, below_keys);
        assert_eq!(
            at_keys,
            vec!["k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"]
        );
    }

    #[test]
    fn sort_single_field_no_tiebreaker() {
        // Two entries with same key -- order is stable but no secondary sort
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("a.txt", 200, 2024, 1),
        ];
        sort_entries(&mut entries, &[SortField::Key], false, usize::MAX);
        // Both have same key, no tiebreaker -- stable sort preserves input order
        assert_eq!(entries[0].size(), 100);
        assert_eq!(entries[1].size(), 200);
    }

    #[test]
    fn sort_common_prefix_by_size_sorts_as_zero() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            ListEntry::CommonPrefix("logs/".to_string()),
        ];
        sort_entries(&mut entries, &[SortField::Size], false, usize::MAX);
        assert_eq!(entries[0].key(), "logs/");
        assert_eq!(entries[1].key(), "a.txt");
    }

    // ========================================================================
    // Channel-based aggregator tests
    // ========================================================================

    #[tokio::test]
    async fn aggregator_sends_sorted_entries_to_channel() {
        let (entry_tx, entry_rx) = mpsc::channel(10);
        let (display_tx, mut display_rx) = mpsc::channel(10);
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = AggregatorConfig {
            no_sort: false,
            sort_fields: vec![SortField::Key],
            reverse: false,
            summary: false,
            parallel_sort_threshold: usize::MAX,
            cancellation_token: token,
        };
        let aggregator = Aggregator::new(entry_rx, display_tx, config);

        entry_tx
            .send(make_entry("c.txt", 300, 2024, 1))
            .await
            .unwrap();
        entry_tx
            .send(make_entry("a.txt", 100, 2024, 2))
            .await
            .unwrap();
        entry_tx
            .send(make_entry("b.txt", 200, 2024, 3))
            .await
            .unwrap();
        drop(entry_tx);

        aggregator.run().await.unwrap();

        let msg1 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg1, DisplayMessage::Entry(e) if e.key() == "a.txt"));
        let msg2 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg2, DisplayMessage::Entry(e) if e.key() == "b.txt"));
        let msg3 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg3, DisplayMessage::Entry(e) if e.key() == "c.txt"));
        assert!(display_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn aggregator_streams_entries_in_order_when_no_sort() {
        let (entry_tx, entry_rx) = mpsc::channel(10);
        let (display_tx, mut display_rx) = mpsc::channel(10);
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = AggregatorConfig {
            no_sort: true,
            sort_fields: vec![],
            reverse: false,
            summary: false,
            parallel_sort_threshold: usize::MAX,
            cancellation_token: token,
        };
        let aggregator = Aggregator::new(entry_rx, display_tx, config);

        entry_tx
            .send(make_entry("c.txt", 300, 2024, 1))
            .await
            .unwrap();
        entry_tx
            .send(make_entry("a.txt", 100, 2024, 2))
            .await
            .unwrap();
        entry_tx
            .send(make_entry("b.txt", 200, 2024, 3))
            .await
            .unwrap();
        drop(entry_tx);

        aggregator.run().await.unwrap();

        let msg1 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg1, DisplayMessage::Entry(e) if e.key() == "c.txt"));
        let msg2 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg2, DisplayMessage::Entry(e) if e.key() == "a.txt"));
        let msg3 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg3, DisplayMessage::Entry(e) if e.key() == "b.txt"));
        assert!(display_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn aggregator_sends_summary_when_enabled() {
        let (entry_tx, entry_rx) = mpsc::channel(10);
        let (display_tx, mut display_rx) = mpsc::channel(10);
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = AggregatorConfig {
            no_sort: false,
            sort_fields: vec![SortField::Key],
            reverse: false,
            summary: true,
            parallel_sort_threshold: usize::MAX,
            cancellation_token: token,
        };
        let aggregator = Aggregator::new(entry_rx, display_tx, config);

        entry_tx
            .send(make_entry("a.txt", 100, 2024, 1))
            .await
            .unwrap();
        entry_tx
            .send(make_entry("b.txt", 200, 2024, 2))
            .await
            .unwrap();
        drop(entry_tx);

        aggregator.run().await.unwrap();

        let msg1 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg1, DisplayMessage::Entry(_)));
        let msg2 = display_rx.recv().await.unwrap();
        assert!(matches!(&msg2, DisplayMessage::Entry(_)));
        let msg3 = display_rx.recv().await.unwrap();
        match msg3 {
            DisplayMessage::Summary(stats) => {
                assert_eq!(stats.total_objects, 2);
                assert_eq!(stats.total_size, 300);
            }
            _ => panic!("expected Summary message"),
        }
        assert!(display_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn aggregator_skips_all_on_cancellation() {
        let (entry_tx, entry_rx) = mpsc::channel(10);
        let (display_tx, mut display_rx) = mpsc::channel(10);
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = AggregatorConfig {
            no_sort: false,
            sort_fields: vec![SortField::Key],
            reverse: false,
            summary: true,
            parallel_sort_threshold: usize::MAX,
            cancellation_token: token.clone(),
        };
        let aggregator = Aggregator::new(entry_rx, display_tx, config);

        entry_tx
            .send(make_entry("a.txt", 100, 2024, 1))
            .await
            .unwrap();
        token.cancel();
        drop(entry_tx);

        aggregator.run().await.unwrap();

        assert!(display_rx.recv().await.is_none());
    }
}
