use crate::aggregate::{
    compute_statistics, format_entry, format_entry_json, format_summary, sort_entries,
    FormatOptions,
};
use crate::config::Config;
use crate::filters::build_filter_chain;
use crate::lister::ObjectLister;
use crate::storage::StorageTrait;
use crate::types::token::PipelineCancellationToken;
use anyhow::Result;
use std::io::Write;
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

        let storage = self.build_storage().await?;

        let filter_chain = build_filter_chain(&self.config.filter_config)
            .map_err(|e| anyhow::anyhow!(e))?;

        // Lister task: list objects, apply filter chain inline, send filtered
        // entries to the aggregate channel. Matches spec: "Lister → apply
        // filter chain → if passes: send to channel → if not: discard".
        let cancellation_token = self.cancellation_token.clone();
        let all_versions = self.config.all_versions;
        let max_keys = self.config.max_keys;

        let lister_handle = tokio::spawn(async move {
            let (list_tx, mut list_rx) = tokio::sync::mpsc::channel(queue_size);

            let lister = ObjectLister {
                storage,
                sender: list_tx,
                all_versions,
                max_keys,
            };

            let inner_handle = tokio::spawn(async move { lister.list_target().await });

            // Filter inline and forward to aggregate channel
            while let Some(entry) = list_rx.recv().await {
                if cancellation_token.is_cancelled() {
                    break;
                }
                if filter_chain.matches(&entry)
                    && tx.send(entry).await.is_err()
                {
                    break;
                }
            }

            match inner_handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    cancellation_token.cancel();
                    Err(e)
                }
                Err(join_err) => {
                    cancellation_token.cancel();
                    Err(anyhow::anyhow!("Lister task panicked: {}", join_err))
                }
            }
        });

        // Aggregate: collect all filtered entries
        let mut entries = Vec::new();
        while let Some(entry) = rx.recv().await {
            entries.push(entry);
        }

        match lister_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(join_err) => {
                return Err(anyhow::anyhow!("Lister+filter task panicked: {}", join_err));
            }
        }

        // Sort
        sort_entries(
            &mut entries,
            &self.config.sort,
            self.config.reverse,
        );

        // Format and write output
        let stdout = std::io::stdout();
        let mut writer = std::io::BufWriter::new(stdout.lock());

        let opts = FormatOptions::from_display_config(
            &self.config.display_config,
            self.config.target.prefix.clone(),
        );
        let use_json = self.config.display_config.json;

        for entry in &entries {
            let line = if use_json {
                format_entry_json(entry)
            } else {
                format_entry(entry, &opts)
            };
            writeln!(writer, "{line}")?;
        }

        // Summary
        if self.config.display_config.summary {
            let stats = compute_statistics(&entries);
            let summary = format_summary(&stats, use_json, self.config.all_versions);
            writeln!(writer, "{summary}")?;
        }

        writer.flush()?;

        Ok(())
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
            self.config.allow_parallel_listings_in_express_one_zone,
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
