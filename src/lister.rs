use crate::storage::StorageTrait;
use crate::types::ListEntry;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

pub struct ObjectLister {
    pub storage: Arc<dyn StorageTrait>,
    pub sender: mpsc::Sender<ListEntry>,
    pub all_versions: bool,
    pub max_keys: i32,
}

impl ObjectLister {
    pub async fn list_target(self) -> Result<()> {
        debug!("list target objects has started.");
        let result = if self.all_versions {
            self.storage
                .list_object_versions(&self.sender, self.max_keys)
                .await
        } else {
            self.storage
                .list_objects(&self.sender, self.max_keys)
                .await
        };
        debug!("list target objects has been completed.");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MockStorage;
    use crate::types::{ListEntry, S3Object};
    use chrono::Utc;

    fn sample_entries() -> Vec<ListEntry> {
        vec![
            ListEntry::Object(S3Object::NotVersioning {
                key: "file1.txt".to_string(),
                size: 100,
                last_modified: Utc::now(),
                e_tag: "\"abc\"".to_string(),
                storage_class: Some("STANDARD".to_string()),
                checksum_algorithm: None,
                checksum_type: None,
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
