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

/// s3ls - Fast S3 object listing tool.
///
/// This binary is a thin wrapper over the s3ls-rs library.
/// All core functionality is implemented in the library crate.
#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config_exit_if_err();

    if let Some(shell) = config.auto_complete_shell {
        generate(
            shell,
            &mut CLIArgs::command(),
            "s3ls",
            &mut std::io::stdout(),
        );

        return Ok(());
    }

    start_tracing_if_necessary(&config);

    trace!("config = {:?}", config);

    run(config).await
}

fn load_config_exit_if_err() -> Config {
    match Config::try_from(CLIArgs::parse()) {
        Ok(config) => config,
        Err(error_message) => {
            clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
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

    match pipeline.run().await {
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
