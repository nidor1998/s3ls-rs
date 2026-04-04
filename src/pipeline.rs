use crate::config::Config;
use anyhow::Result;

pub struct ListingPipeline {
    config: Config,
}

impl ListingPipeline {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Result<()> {
        tracing::info!(
            target = %self.config.target,
            recursive = self.config.recursive,
            "Starting listing pipeline"
        );

        // Step 3+ will implement:
        // 1. Create lister stage with filter chain
        // 2. Create aggregate stage
        // 3. Wait for completion

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pipeline_runs_and_returns_success() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let pipeline = ListingPipeline::new(config);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }
}
