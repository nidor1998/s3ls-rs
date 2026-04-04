use crate::config::Config;
use crate::lister::ObjectLister;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use anyhow::Result;
use std::sync::Arc;

pub struct ListingPipeline {
    config: Config,
    cancellation_token: PipelineCancellationToken,
    #[cfg(test)]
    storage_override: Option<Arc<dyn StorageTrait>>,
}

impl ListingPipeline {
    pub fn new(config: Config, cancellation_token: PipelineCancellationToken) -> Self {
        Self {
            config,
            cancellation_token,
            #[cfg(test)]
            storage_override: None,
        }
    }

    #[cfg(test)]
    pub fn with_storage(
        config: Config,
        cancellation_token: PipelineCancellationToken,
        storage: Arc<dyn StorageTrait>,
    ) -> Self {
        Self {
            config,
            cancellation_token,
            storage_override: Some(storage),
        }
    }

    pub async fn run(self) -> Result<()> {
        tracing::info!(
            target = %self.config.target,
            recursive = self.config.recursive,
            "Starting listing pipeline"
        );

        if self.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let queue_size = self.config.object_listing_queue_size as usize;
        let (tx, mut rx) = tokio::sync::mpsc::channel(queue_size);

        let storage = self.build_storage().await;

        let lister = ObjectLister {
            storage,
            sender: tx,
            all_versions: self.config.all_versions,
            max_keys: self.config.max_keys,
        };

        let lister_handle = tokio::spawn(async move { lister.list_target().await });

        let cancellation_token = self.cancellation_token.clone();
        while let Some(entry) = rx.recv().await {
            if cancellation_token.is_cancelled() {
                break;
            }
            // Temporary: print key to stdout (replaced in Step 5)
            println!("{}", entry.key());
        }

        match lister_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                cancellation_token.cancel();
                return Err(e);
            }
            Err(join_err) => {
                cancellation_token.cancel();
                return Err(anyhow::anyhow!("Lister task panicked: {}", join_err));
            }
        }

        Ok(())
    }

    async fn build_storage(&self) -> Arc<dyn StorageTrait> {
        #[cfg(test)]
        if let Some(ref storage) = self.storage_override {
            return Arc::clone(storage);
        }

        self.build_s3_storage().await
    }

    async fn build_s3_storage(&self) -> Arc<dyn StorageTrait> {
        let client_config = self
            .config
            .target_client_config
            .as_ref()
            .expect("target_client_config is required for S3 operations");

        let storage = crate::storage::s3::S3Storage::new(
            client_config,
            self.config.target.bucket.clone(),
            self.config.target.prefix.clone(),
            self.config.recursive,
            self.cancellation_token.clone(),
            client_config.request_payer.clone(),
            self.config.max_parallel_listings,
            self.config.max_parallel_listing_max_depth,
            self.config.allow_parallel_listings_in_express_one_zone,
        )
        .await;

        Arc::new(storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MockStorage;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::types::{ListEntry, S3Object};
    use chrono::Utc;

    fn mock_entries() -> Vec<ListEntry> {
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
    async fn pipeline_runs_and_returns_success() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        let storage = Arc::new(MockStorage::new(vec![]));
        let pipeline = ListingPipeline::with_storage(config, token, storage);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pipeline_respects_cancellation() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let storage = Arc::new(MockStorage::new(vec![]));
        let pipeline = ListingPipeline::with_storage(config, token, storage);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pipeline_lists_mock_objects() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        let storage = Arc::new(MockStorage::new(mock_entries()));
        let pipeline = ListingPipeline::with_storage(config, token, storage);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }
}
