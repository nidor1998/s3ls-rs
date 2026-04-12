use crate::filters::FilterChain;
use crate::storage::StorageTrait;
use crate::types::ListEntry;
use crate::types::token::PipelineCancellationToken;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

pub struct ObjectLister {
    pub storage: Arc<dyn StorageTrait>,
    pub sender: mpsc::Sender<ListEntry>,
    pub all_versions: bool,
    pub max_keys: i32,
    pub queue_size: usize,
    pub cancellation_token: PipelineCancellationToken,
    pub hide_delete_markers: bool,
    pub show_objects_only: bool,
    pub filter_chain: FilterChain,
}

impl ObjectLister {
    pub async fn list_target(self) -> Result<()> {
        debug!("list target objects has started.");

        let (list_tx, mut list_rx) = mpsc::channel(self.queue_size);

        let storage = self.storage.clone();
        let all_versions = self.all_versions;
        let max_keys = self.max_keys;

        let inner_handle = tokio::spawn(async move {
            if all_versions {
                storage.list_object_versions(&list_tx, max_keys).await
            } else {
                storage.list_objects(&list_tx, max_keys).await
            }
        });

        // Filter inline and forward to aggregate channel. Rather than
        // returning early on a filter error, record it and break out of the
        // loop so the normal cleanup path runs (dropping list_rx, then
        // joining the storage task).
        let mut forward_err: Option<anyhow::Error> = None;
        while let Some(entry) = list_rx.recv().await {
            if self.cancellation_token.is_cancelled() {
                break;
            }
            if self.hide_delete_markers && entry.is_delete_marker() {
                continue;
            }
            if self.show_objects_only && matches!(entry, ListEntry::CommonPrefix(_)) {
                continue;
            }
            match self.filter_chain.matches(&entry) {
                Ok(true) => {
                    if self.sender.send(entry).await.is_err() {
                        break;
                    }
                }
                Ok(false) => continue,
                Err(e) => {
                    self.cancellation_token.cancel();
                    forward_err = Some(e);
                    break;
                }
            }
        }

        // Drop the intermediate receiver before joining the storage task.
        // Otherwise a storage task blocked on `list_tx.send(...)` (because
        // the bounded queue is full and nobody is draining it) would prevent
        // `inner_handle.await` from ever resolving, deadlocking the lister.
        drop(list_rx);

        let inner_result = match inner_handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.cancellation_token.cancel();
                Err(e)
            }
            Err(join_err) => {
                self.cancellation_token.cancel();
                Err(anyhow::anyhow!("Lister task panicked: {}", join_err))
            }
        };

        // A filter error takes precedence over the storage task's result,
        // since it caused the cancellation.
        if let Some(e) = forward_err {
            return Err(e);
        }
        inner_result?;

        debug!("list target objects has been completed.");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MockStorage;
    use crate::types::{ListEntry, S3Object};
    use chrono::Utc;

    use crate::filters::FilterChain;
    use crate::types::token::create_pipeline_cancellation_token;

    fn sample_entries() -> Vec<ListEntry> {
        vec![
            ListEntry::Object(S3Object::NotVersioning {
                key: "file1.txt".to_string(),
                size: 100,
                last_modified: Utc::now(),
                e_tag: "\"abc\"".to_string(),
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: vec![],
                checksum_type: None,
                owner_display_name: None,
                owner_id: None,
                is_restore_in_progress: None,
                restore_expiry_date: None,
            }),
            ListEntry::CommonPrefix("logs/".to_string()),
        ]
    }

    #[tokio::test]
    async fn lister_sends_objects_to_channel() {
        let entries = sample_entries();
        let mock = Arc::new(MockStorage::new(entries.clone()));
        let (tx, mut rx) = mpsc::channel(10);

        let lister = ObjectLister {
            storage: mock,
            sender: tx,
            all_versions: false,
            max_keys: 1000,
            queue_size: 10,
            cancellation_token: create_pipeline_cancellation_token(),
            hide_delete_markers: false,
            show_objects_only: false,
            filter_chain: FilterChain::new(vec![]),
        };

        lister.list_target().await.unwrap();

        let mut received = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            received.push(entry);
        }

        assert_eq!(received.len(), 2);
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "logs/");
    }

    #[tokio::test]
    async fn lister_does_not_deadlock_when_cancelled_with_full_queue() {
        // Regression test for the deadlock where an early break in the
        // forward loop left list_rx alive, causing the spawned storage task
        // to block forever on a full-queue send().
        //
        // Setup:
        //   - queue_size = 1 (tiny intermediate channel)
        //   - MockStorage has many entries to try to send
        //   - Token is pre-cancelled, so the forward loop will break after
        //     receiving the first entry
        //   - Without the fix, list_rx would stay alive and the storage task
        //     would block forever on the second send(). We wrap the whole
        //     run in a timeout so a regression fails rather than hangs.

        // Generate 100 entries — plenty to overflow a queue of size 1.
        let entries: Vec<ListEntry> = (0..100)
            .map(|i| {
                ListEntry::Object(S3Object::NotVersioning {
                    key: format!("file_{i}.txt"),
                    size: 100,
                    last_modified: Utc::now(),
                    e_tag: "\"e\"".to_string(),
                    storage_class: Some("STANDARD".to_string()),
                    checksum_algorithm: vec![],
                    checksum_type: None,
                    owner_display_name: None,
                    owner_id: None,
                    is_restore_in_progress: None,
                    restore_expiry_date: None,
                })
            })
            .collect();

        let mock = Arc::new(MockStorage::new(entries));
        let (tx, _rx) = mpsc::channel(10);

        let token = create_pipeline_cancellation_token();
        token.cancel(); // pre-cancel so the forward loop breaks immediately

        let lister = ObjectLister {
            storage: mock,
            sender: tx,
            all_versions: false,
            max_keys: 1000,
            queue_size: 1, // tiny — storage will fill it fast
            cancellation_token: token,
            hide_delete_markers: false,
            show_objects_only: false,
            filter_chain: FilterChain::new(vec![]),
        };

        // Should complete quickly. If the deadlock regresses, this times
        // out and the test fails with an unambiguous signal.
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(2), lister.list_target()).await;
        assert!(
            result.is_ok(),
            "list_target deadlocked — list_rx was not dropped before joining storage task"
        );
        result.unwrap().unwrap();
    }

    #[tokio::test]
    async fn lister_uses_versions_when_all_versions_set() {
        let entries = sample_entries();
        let mock = Arc::new(MockStorage::new(entries.clone()));
        let (tx, mut rx) = mpsc::channel(10);

        let lister = ObjectLister {
            storage: mock,
            sender: tx,
            all_versions: true,
            max_keys: 1000,
            queue_size: 10,
            cancellation_token: create_pipeline_cancellation_token(),
            hide_delete_markers: false,
            show_objects_only: false,
            filter_chain: FilterChain::new(vec![]),
        };

        lister.list_target().await.unwrap();

        let mut received = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            received.push(entry);
        }

        // MockStorage sends the same entries for both methods,
        // but this verifies the all_versions path is taken
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "logs/");
    }

    #[tokio::test]
    async fn show_objects_only_filters_common_prefixes() {
        let entries = sample_entries(); // contains file1.txt + CommonPrefix("logs/")
        let mock = Arc::new(MockStorage::new(entries));
        let (tx, mut rx) = mpsc::channel(10);

        let lister = ObjectLister {
            storage: mock,
            sender: tx,
            all_versions: false,
            max_keys: 1000,
            queue_size: 10,
            cancellation_token: create_pipeline_cancellation_token(),
            hide_delete_markers: false,
            show_objects_only: true,
            filter_chain: FilterChain::new(vec![]),
        };

        lister.list_target().await.unwrap();

        let mut received = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            received.push(entry);
        }

        assert_eq!(received.len(), 1, "CommonPrefix should be filtered out");
        assert_eq!(received[0].key(), "file1.txt");
    }

    #[tokio::test]
    async fn show_objects_only_false_keeps_common_prefixes() {
        let entries = sample_entries();
        let mock = Arc::new(MockStorage::new(entries));
        let (tx, mut rx) = mpsc::channel(10);

        let lister = ObjectLister {
            storage: mock,
            sender: tx,
            all_versions: false,
            max_keys: 1000,
            queue_size: 10,
            cancellation_token: create_pipeline_cancellation_token(),
            hide_delete_markers: false,
            show_objects_only: false,
            filter_chain: FilterChain::new(vec![]),
        };

        lister.list_target().await.unwrap();

        let mut received = Vec::new();
        while let Ok(entry) = rx.try_recv() {
            received.push(entry);
        }

        assert_eq!(received.len(), 2, "Both entries should pass through");
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "logs/");
    }
}
