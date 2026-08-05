// Ctrl+C signal handler adapted from s3sync's `bin/s3sync/cli/ctrl_c_handler/mod.rs`.
//
// Uses tokio::select! to wait for either pipeline cancellation or Ctrl+C signal.

use std::sync::atomic::{AtomicBool, Ordering};

use s3ls_rs::PipelineCancellationToken;
use tokio::task::JoinHandle;
use tokio::{select, signal};
use tracing::{debug, error};

static CTRL_C_RECEIVED: AtomicBool = AtomicBool::new(false);

/// Whether a Ctrl+C (SIGINT) signal has been received by the handler.
///
/// The flag is stored before the cancellation token is cancelled, so any
/// code that observes the pipeline stopping due to cancellation is
/// guaranteed to see `true` here when Ctrl+C was the cause.
pub fn is_ctrl_c_received() -> bool {
    CTRL_C_RECEIVED.load(Ordering::SeqCst)
}

pub fn spawn_ctrl_c_handler(cancellation_token: PipelineCancellationToken) -> JoinHandle<()> {
    tokio::spawn(async move {
        select! {
            _ = cancellation_token.cancelled() => {
                debug!("cancellation_token canceled.")
            }
            result = signal::ctrl_c() => {
                match result {
                    Ok(()) => {
                        debug!("ctrl-c received, shutting down.");
                        CTRL_C_RECEIVED.store(true, Ordering::SeqCst);
                        cancellation_token.cancel();
                    }
                    Err(e) => {
                        error!("failed to listen for ctrl-c signal: {e}");
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use once_cell::sync::Lazy;
    use s3ls_rs::create_pipeline_cancellation_token;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    static SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| Arc::new(Semaphore::new(1)));

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn ctrl_c_handler_handles_sigint() {
        const WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START: u64 = 100;

        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let cancellation_token = create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START,
        ))
        .await;

        kill_sigint_to_self();

        join_handle.await.unwrap();

        assert!(cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn ctrl_c_handler_handles_cancellation_token() {
        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();

        let cancellation_token = create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        cancellation_token.cancel();

        join_handle.await.unwrap();

        assert!(cancellation_token.is_cancelled());
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn ctrl_c_received_flag_is_set_on_sigint() {
        const WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START: u64 = 100;

        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();
        reset_ctrl_c_received();

        let cancellation_token = create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START,
        ))
        .await;

        assert!(!is_ctrl_c_received());

        kill_sigint_to_self();

        join_handle.await.unwrap();

        assert!(is_ctrl_c_received());
        assert!(cancellation_token.is_cancelled());
    }

    /// The flag must already be visible when the token cancellation becomes
    /// observable — main.rs relies on this ordering to decide the exit code
    /// after the pipeline stops.
    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn ctrl_c_received_flag_is_visible_once_token_is_cancelled() {
        const WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START: u64 = 100;

        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();
        reset_ctrl_c_received();

        let cancellation_token = create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        tokio::time::sleep(std::time::Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_CTRL_C_HANDLER_START,
        ))
        .await;

        kill_sigint_to_self();

        cancellation_token.cancelled().await;
        assert!(is_ctrl_c_received());

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn ctrl_c_received_flag_is_not_set_on_token_cancellation() {
        init_dummy_tracing_subscriber();

        let _semaphore = SEMAPHORE.clone().acquire_owned().await.unwrap();
        reset_ctrl_c_received();

        let cancellation_token = create_pipeline_cancellation_token();

        let join_handle = spawn_ctrl_c_handler(cancellation_token.clone());
        cancellation_token.cancel();

        join_handle.await.unwrap();

        assert!(!is_ctrl_c_received());
    }

    /// The flag is process-global; tests that assert on it reset it first
    /// (while holding SEMAPHORE) so they don't observe a SIGINT from an
    /// earlier test.
    fn reset_ctrl_c_received() {
        CTRL_C_RECEIVED.store(false, Ordering::SeqCst);
    }

    #[cfg(target_family = "unix")]
    fn kill_sigint_to_self() {
        nix::sys::signal::kill(nix::unistd::Pid::this(), nix::sys::signal::Signal::SIGINT).unwrap();
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
