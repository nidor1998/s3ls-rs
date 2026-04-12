/// A cancellation token used to signal pipeline shutdown.
///
/// This is a type alias for [`tokio_util::sync::CancellationToken`]. Pass the
/// token to [`ListingPipeline::new`](crate::ListingPipeline::new) and call
/// [`cancel()`](tokio_util::sync::CancellationToken::cancel) on it to request
/// graceful shutdown of a running pipeline (e.g., in a Ctrl+C handler).
pub type PipelineCancellationToken = tokio_util::sync::CancellationToken;

/// Create a new [`PipelineCancellationToken`].
pub fn create_pipeline_cancellation_token() -> PipelineCancellationToken {
    tokio_util::sync::CancellationToken::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_cancellation_token() {
        let token = create_pipeline_cancellation_token();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancel_token() {
        let token = create_pipeline_cancellation_token();
        token.cancel();
        assert!(token.is_cancelled());
    }
}
