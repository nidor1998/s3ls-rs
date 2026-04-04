use crate::config::Config;
use crate::types::token::PipelineCancellationToken;
use anyhow::Result;

pub struct ListingPipeline {
    config: Config,
    cancellation_token: PipelineCancellationToken,
}

impl ListingPipeline {
    pub fn new(config: Config, cancellation_token: PipelineCancellationToken) -> Self {
        Self {
            config,
            cancellation_token,
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
    use crate::types::token::create_pipeline_cancellation_token;

    #[tokio::test]
    async fn pipeline_runs_and_returns_success() {
        let config =
            Config::try_from(crate::parse_from_args(vec!["s3ls", "s3://test-bucket/"]).unwrap())
                .unwrap();
        let token = create_pipeline_cancellation_token();
        let pipeline = ListingPipeline::new(config, token);
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
        let pipeline = ListingPipeline::new(config, token);
        let result = pipeline.run().await;
        assert!(result.is_ok());
    }
}
