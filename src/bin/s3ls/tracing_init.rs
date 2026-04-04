// Tracing infrastructure adapted from s3sync.
// Initializes the tracing subscriber for the CLI binary.

use std::env;
use std::io::IsTerminal;

use tracing_subscriber::fmt::format::FmtSpan;

use s3ls_rs::config::TracingConfig;

const EVENT_FILTER_ENV_VAR: &str = "RUST_LOG";

pub fn init_tracing(config: &TracingConfig) {
    let fmt_span = if config.span_events_tracing {
        FmtSpan::NEW | FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    let subscriber_builder = tracing_subscriber::fmt()
        .with_writer(std::io::stdout)
        .compact()
        .with_target(false)
        .with_ansi(!config.disable_color_tracing && std::io::stdout().is_terminal())
        .with_span_events(fmt_span);

    let mut show_target = true;
    let tracing_level = config.tracing_level;
    let event_filter = if config.aws_sdk_tracing {
        format!(
            "s3ls_rs={tracing_level},s3ls={tracing_level},aws_smithy_runtime={tracing_level},aws_config={tracing_level},aws_sigv4={tracing_level}"
        )
    } else if let Ok(env_filter) = env::var(EVENT_FILTER_ENV_VAR) {
        env_filter
    } else {
        show_target = false;
        format!("s3ls_rs={tracing_level},s3ls={tracing_level}")
    };

    let subscriber_builder = subscriber_builder
        .with_env_filter(event_filter)
        .with_target(show_target);
    if config.json_tracing {
        subscriber_builder.json().init();
    } else {
        subscriber_builder.init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_fork::rusty_fork_test;

    rusty_fork_test! {
        #[test]
        fn init_json_tracing() {
            init_tracing(&TracingConfig {
                tracing_level: log::Level::Info,
                json_tracing: true,
                aws_sdk_tracing: false,
                span_events_tracing: false,
                disable_color_tracing: false,
            });
        }

        #[test]
        fn init_aws_sdk_tracing() {
            init_tracing(&TracingConfig {
                tracing_level: log::Level::Info,
                json_tracing: false,
                aws_sdk_tracing: true,
                span_events_tracing: false,
                disable_color_tracing: false,
            });
        }

        #[test]
        fn init_normal_tracing() {
            unsafe { env::remove_var(EVENT_FILTER_ENV_VAR) };
            init_tracing(&TracingConfig {
                tracing_level: log::Level::Info,
                json_tracing: false,
                aws_sdk_tracing: false,
                span_events_tracing: false,
                disable_color_tracing: false,
            });
        }

        #[test]
        fn init_span_events_tracing() {
            init_tracing(&TracingConfig {
                tracing_level: log::Level::Info,
                json_tracing: false,
                aws_sdk_tracing: true,
                span_events_tracing: true,
                disable_color_tracing: false,
            });
        }

        #[test]
        fn init_disable_color_tracing() {
            init_tracing(&TracingConfig {
                tracing_level: log::Level::Info,
                json_tracing: false,
                aws_sdk_tracing: false,
                span_events_tracing: false,
                disable_color_tracing: true,
            });
        }

        #[test]
        fn init_with_env() {
            unsafe { env::set_var(EVENT_FILTER_ENV_VAR, "trace") };
            init_tracing(&TracingConfig {
                tracing_level: log::Level::Info,
                json_tracing: false,
                aws_sdk_tracing: false,
                span_events_tracing: false,
                disable_color_tracing: true,
            });
        }
    }
}
