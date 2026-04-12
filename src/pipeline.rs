use crate::aggregate::{Aggregator, AggregatorConfig, FormatOptions};
use crate::config::Config;
use crate::filters::build_filter_chain;
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
        tracing::debug!(
            target = %self.config.target,
            recursive = self.config.recursive,
            "Starting listing pipeline"
        );

        if self.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let queue_size = self.config.object_listing_queue_size as usize;
        let (tx, rx) = tokio::sync::mpsc::channel(queue_size);

        let storage = self.build_storage().await?;

        let lister_handle = self.spawn_lister(storage, tx, queue_size)?;
        let aggregator_handle = self.spawn_aggregator(rx)?;

        // Wait for aggregator to finish (completes when rx closes).
        // On aggregator error, cancel the token and wait for the lister to
        // wind down before returning — otherwise the lister keeps issuing
        // S3 API calls until it notices the dropped aggregate receiver.
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

        // Wait for lister to finish and propagate errors
        let lister_result = match lister_handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(join_err) => Err(anyhow::anyhow!("Lister+filter task panicked: {}", join_err)),
        };

        // The aggregator error (if any) is the original cause; surface it
        // in preference to the lister's error, which is usually a downstream
        // symptom (cancellation or receiver dropped).
        if let Some(e) = aggregator_err {
            return Err(e);
        }
        lister_result?;

        tracing::debug!("Listing pipeline completed");
        Ok(())
    }

    fn spawn_lister(
        &self,
        storage: Arc<dyn StorageTrait>,
        tx: tokio::sync::mpsc::Sender<crate::types::ListEntry>,
        queue_size: usize,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let filter_chain =
            build_filter_chain(&self.config.filter_config).map_err(|e| anyhow::anyhow!(e))?;

        let lister = ObjectLister {
            storage,
            sender: tx,
            all_versions: self.config.all_versions,
            max_keys: self.config.max_keys,
            queue_size,
            cancellation_token: self.cancellation_token.clone(),
            hide_delete_markers: self.config.hide_delete_markers,
            show_objects_only: self.config.show_objects_only,
            filter_chain,
        };

        Ok(tokio::spawn(async move { lister.list_target().await }))
    }

    fn spawn_aggregator(
        &self,
        rx: tokio::sync::mpsc::Receiver<crate::types::ListEntry>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let opts = FormatOptions::from_display_config(
            &self.config.display_config,
            self.config.target.prefix.clone(),
            self.config.all_versions,
        );
        let use_json = self.config.display_config.json;
        let writer: Box<dyn std::io::Write + Send> =
            Box::new(std::io::BufWriter::new(std::io::stdout()));

        let aggregator_config = AggregatorConfig {
            use_json,
            no_sort: self.config.no_sort,
            sort_fields: self.config.sort.clone(),
            reverse: self.config.reverse,
            summary: self.config.display_config.summary,
            human: self.config.display_config.human,
            all_versions: self.config.all_versions,
            parallel_sort_threshold: self.config.parallel_sort_threshold as usize,
            cancellation_token: self.cancellation_token.clone(),
        };
        let mut aggregator = Aggregator::new(rx, writer, opts, aggregator_config);

        if !use_json && self.config.display_config.header {
            aggregator.write_header()?;
        }

        Ok(tokio::spawn(async move { aggregator.run().await }))
    }

    async fn build_storage(&self) -> Result<Arc<dyn StorageTrait>> {
        #[cfg(test)]
        if let Some(ref storage) = self.storage_override {
            return Ok(Arc::clone(storage));
        }

        self.build_s3_storage().await
    }

    async fn build_s3_storage(&self) -> Result<Arc<dyn StorageTrait>> {
        let client_config = self
            .config
            .target_client_config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No client configuration provided"))?;

        let storage = crate::storage::s3::S3Storage::new(
            client_config,
            self.config.target.bucket.clone(),
            self.config.target.prefix.clone(),
            self.config.recursive,
            self.cancellation_token.clone(),
            client_config.request_payer.clone(),
            self.config.max_parallel_listings,
            self.config.max_parallel_listing_max_depth,
            self.config.max_depth,
            self.config.allow_parallel_listings_in_express_one_zone,
            self.config.display_config.show_owner,
            self.config.display_config.show_restore_status,
            self.config.rate_limit_objects,
        )
        .await;

        Ok(Arc::new(storage))
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
