pub mod s3;

use async_trait::async_trait;
use crate::types::ListEntry;
use anyhow::Result;
use tokio::sync::mpsc::Sender;

#[async_trait]
pub trait StorageTrait: Send + Sync {
    async fn list_objects(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()>;
    async fn list_object_versions(&self, sender: &Sender<ListEntry>, max_keys: i32) -> Result<()>;
}

#[cfg(test)]
pub(crate) struct MockStorage {
    entries: Vec<ListEntry>,
}

#[cfg(test)]
impl MockStorage {
    pub(crate) fn new(entries: Vec<ListEntry>) -> Self {
        Self { entries }
    }
}

#[cfg(test)]
#[async_trait]
impl StorageTrait for MockStorage {
    async fn list_objects(&self, sender: &Sender<ListEntry>, _max_keys: i32) -> Result<()> {
        for entry in &self.entries {
            sender.send(entry.clone()).await.ok();
        }
        Ok(())
    }

    async fn list_object_versions(&self, sender: &Sender<ListEntry>, _max_keys: i32) -> Result<()> {
        for entry in &self.entries {
            sender.send(entry.clone()).await.ok();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            ListEntry::DeleteMarker {
                key: "deleted.txt".to_string(),
                version_id: "v1".to_string(),
                last_modified: Utc::now(),
                is_latest: true,
            },
        ]
    }

    #[tokio::test]
    async fn mock_storage_list_objects_sends_all_entries() {
        let entries = sample_entries();
        let mock = MockStorage::new(entries.clone());
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        mock.list_objects(&tx, 1000).await.unwrap();
        drop(tx);

        let mut received = Vec::new();
        while let Some(entry) = rx.recv().await {
            received.push(entry);
        }

        assert_eq!(received.len(), 3);
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "logs/");
        assert_eq!(received[2].key(), "deleted.txt");
    }

    #[tokio::test]
    async fn mock_storage_list_object_versions_sends_all_entries() {
        let entries = sample_entries();
        let mock = MockStorage::new(entries.clone());
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        mock.list_object_versions(&tx, 1000).await.unwrap();
        drop(tx);

        let mut received = Vec::new();
        while let Some(entry) = rx.recv().await {
            received.push(entry);
        }

        assert_eq!(received.len(), 3);
        assert_eq!(received[0].key(), "file1.txt");
        assert_eq!(received[1].key(), "logs/");
        assert_eq!(received[2].key(), "deleted.txt");
    }

    #[tokio::test]
    async fn mock_storage_empty_entries() {
        let mock = MockStorage::new(vec![]);
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);

        mock.list_objects(&tx, 1000).await.unwrap();
        drop(tx);

        assert!(rx.recv().await.is_none());
    }
}
