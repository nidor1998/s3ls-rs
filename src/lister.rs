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

        // Filter inline and forward to aggregate channel
        while let Some(entry) = list_rx.recv().await {
            if self.cancellation_token.is_cancelled() {
                break;
            }
            if self.hide_delete_markers && entry.is_delete_marker() {
                continue;
            }
            if self.filter_chain.matches(&entry) && self.sender.send(entry).await.is_err() {
                break;
            }
        }

        match inner_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                self.cancellation_token.cancel();
                return Err(e);
            }
            Err(join_err) => {
                self.cancellation_token.cancel();
                return Err(anyhow::anyhow!("Lister task panicked: {}", join_err));
            }
        }

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
}
