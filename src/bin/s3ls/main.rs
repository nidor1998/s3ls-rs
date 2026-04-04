use s3ls_rs::types::error::S3lsError;
use s3ls_rs::{build_config_from_args, ListingPipeline};
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match build_config_from_args(std::env::args_os()) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    // Initialize tracing
    if let Some(ref tracing_config) = config.tracing_config {
        init_tracing(tracing_config);
    }

    let pipeline = ListingPipeline::new(config);
    match pipeline.run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if let Some(s3ls_err) = e.downcast_ref::<S3lsError>() {
                eprintln!("{s3ls_err}");
                ExitCode::from(s3ls_err.exit_code() as u8)
            } else {
                eprintln!("Error: {e}");
                ExitCode::from(1)
            }
        }
    }
}

fn init_tracing(config: &s3ls_rs::config::TracingConfig) {
    use tracing_subscriber::fmt;
    use tracing_subscriber::EnvFilter;

    let level = match config.tracing_level {
        log::Level::Error => "error",
        log::Level::Warn => "warn",
        log::Level::Info => "info",
        log::Level::Debug => "debug",
        log::Level::Trace => "trace",
    };

    let filter = if config.aws_sdk_tracing {
        EnvFilter::new(level)
    } else {
        EnvFilter::new(format!(
            "{level},aws_config=off,aws_sdk=off,aws_smithy=off"
        ))
    };

    let builder = fmt().with_env_filter(filter);

    if config.json_tracing {
        builder.json().init();
    } else if config.disable_color_tracing {
        builder.with_ansi(false).init();
    } else {
        builder.init();
    }
}
