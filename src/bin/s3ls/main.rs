use std::io::Write;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use tracing::{debug, error, trace};

use s3ls_rs::bucket_lister;
use s3ls_rs::config::Config;
use s3ls_rs::{
    CLIArgs, ListingPipeline, create_pipeline_cancellation_token, exit_code_from_error,
    is_cancelled_error,
};

mod ctrl_c_handler;
mod tracing_init;

/// Conventional exit code for termination by Ctrl+C: 128 + SIGINT(2), the
/// shell encoding that lets scripts distinguish user interruption from
/// real failures.
const SIGINT_EXIT_CODE: i32 = 130;

/// s3ls - Fast S3 object listing tool.
///
/// This binary is a thin wrapper over the s3ls-rs library.
/// All core functionality is implemented in the library crate.
#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config_exit_if_err();

    if let Some(shell) = config.auto_complete_shell {
        return print_completion_script(shell);
    }

    start_tracing_if_necessary(&config);

    trace!("config = {:?}", config);

    run(config).await
}

/// clap_complete's `generate` panics on any stdout write error, including
/// the BrokenPipe raised when the reader of a pipe exits without consuming
/// the script (`s3ls --auto-complete-shell bash | head -1`). Render the
/// script into memory (infallible), then write it ourselves: a closed pipe
/// is the normal end of a pipeline and exits 0, any other write failure
/// (e.g. disk full on a redirect) is a real error.
fn print_completion_script(shell: clap_complete::shells::Shell) -> Result<()> {
    let mut script = Vec::new();
    generate(shell, &mut CLIArgs::command(), "s3ls", &mut script);

    let mut stdout = std::io::stdout().lock();
    match stdout.write_all(&script).and_then(|()| stdout.flush()) {
        Err(e) if e.kind() != std::io::ErrorKind::BrokenPipe => {
            Err(anyhow::anyhow!("failed to write completion script: {e}"))
        }
        _ => Ok(()),
    }
}

fn load_config_exit_if_err() -> Config {
    match Config::try_from(CLIArgs::parse()) {
        Ok(config) => config,
        Err(error_message) => {
            // clap prints raw error messages verbatim, so terminate the line
            // ourselves to keep the shell prompt off the message line.
            clap::Error::raw(
                clap::error::ErrorKind::ValueValidation,
                format!("{error_message}\n"),
            )
            .exit();
        }
    }
}

fn start_tracing_if_necessary(config: &Config) -> bool {
    if let Some(tracing_config) = config.tracing_config.as_ref() {
        tracing_init::init_tracing(tracing_config);
        true
    } else {
        false
    }
}

async fn run(config: Config) -> Result<()> {
    // Bucket listing mode: no target specified
    if config.target.bucket.is_empty() {
        return match bucket_lister::list_buckets(&config).await {
            Ok(()) => Ok(()),
            Err(e) => {
                if let Some(io_err) = e.downcast_ref::<std::io::Error>()
                    && io_err.kind() == std::io::ErrorKind::BrokenPipe
                {
                    return Ok(());
                }
                error!("{}", e);
                std::process::exit(1);
            }
        };
    }

    let cancellation_token = create_pipeline_cancellation_token();

    ctrl_c_handler::spawn_ctrl_c_handler(cancellation_token.clone());

    let start_time = tokio::time::Instant::now();
    debug!("listing pipeline start.");

    let pipeline = ListingPipeline::new(config, cancellation_token);

    let result = pipeline.run().await;

    // Ctrl+C takes precedence over whatever the pipeline returned: once
    // SIGINT is received the run is "interrupted", even if the shutdown
    // also surfaced an error.
    if ctrl_c_handler::is_ctrl_c_received() {
        let duration_sec = format!("{:.3}", start_time.elapsed().as_secs_f32());
        debug!(duration_sec = duration_sec, "listing cancelled by user.");
        std::process::exit(SIGINT_EXIT_CODE);
    }

    match result {
        Ok(()) => {
            let duration_sec = format!("{:.3}", start_time.elapsed().as_secs_f32());
            debug!(duration_sec = duration_sec, "s3ls has been completed.");
            Ok(())
        }
        Err(e) => {
            // Broken pipe is expected when piped to head/tail — exit silently.
            if let Some(io_err) = e.downcast_ref::<std::io::Error>()
                && io_err.kind() == std::io::ErrorKind::BrokenPipe
            {
                return Ok(());
            }
            let duration_sec = format!("{:.3}", start_time.elapsed().as_secs_f32());
            if is_cancelled_error(&e) {
                debug!("listing cancelled by user.");
                return Ok(());
            }
            let code = exit_code_from_error(&e);
            error!(duration_sec = duration_sec, "s3ls failed.");
            error!("{}", e);
            std::process::exit(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sigint_exit_code_is_130() {
        assert_eq!(SIGINT_EXIT_CODE, 130);
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn sigint_exit_code_follows_128_plus_signal_number_convention() {
        assert_eq!(
            SIGINT_EXIT_CODE,
            128 + nix::sys::signal::Signal::SIGINT as i32
        );
    }
}
