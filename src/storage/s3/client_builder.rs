use crate::config::ClientConfig;
use crate::types::S3Credentials;
use aws_config::meta::region::RegionProviderChain;
use aws_config::{ConfigLoader, SdkConfig};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use std::time::Duration;
use tracing::debug;

impl ClientConfig {
    /// Create a fully-configured S3 [`Client`] from this configuration.
    pub async fn create_client(&self) -> Client {
        let sdk_config = self.load_sdk_config().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(self.force_path_style)
            .accelerate(self.accelerate)
            .request_checksum_calculation(self.request_checksum_calculation.clone());

        if let Some(timeout_config) = self.build_timeout_config() {
            s3_config_builder = s3_config_builder.timeout_config(timeout_config);
        }

        Client::from_conf(s3_config_builder.build())
    }

    /// Load the AWS SDK config with region, credentials, retry, endpoint, and
    /// stalled-stream protection applied.
    async fn load_sdk_config(&self) -> SdkConfig {
        let region_provider = self.build_region_provider();
        let retry_config = self.build_retry_config();

        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_provider)
            .retry_config(retry_config)
            .stalled_stream_protection(
                aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
                    .grace_period(Duration::from_secs(5))
                    .build(),
            );

        if self.disable_stalled_stream_protection {
            config_loader = config_loader.stalled_stream_protection(
                aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
            );
        }

        if let Some(ref endpoint_url) = self.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        // Apply AWS config file locations
        // SAFETY: These env vars are set early during client init, before any
        // multi-threaded AWS SDK config loading occurs.
        if let Some(ref config_file) = self.client_config_location.aws_config_file {
            unsafe { std::env::set_var("AWS_CONFIG_FILE", config_file) };
            debug!(?config_file, "set AWS_CONFIG_FILE");
        }
        if let Some(ref creds_file) = self.client_config_location.aws_shared_credentials_file {
            unsafe { std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", creds_file) };
            debug!(?creds_file, "set AWS_SHARED_CREDENTIALS_FILE");
        }

        config_loader = self.load_config_credential(config_loader);

        config_loader.load().await
    }

    /// Apply credential configuration to the config loader based on the
    /// [`S3Credentials`] variant.
    fn load_config_credential(&self, config_loader: ConfigLoader) -> ConfigLoader {
        match &self.credential {
            S3Credentials::Profile(profile_name) => {
                debug!(%profile_name, "using profile credentials");
                config_loader.credentials_provider(
                    aws_config::profile::ProfileFileCredentialsProvider::builder()
                        .profile_name(profile_name)
                        .build(),
                )
            }
            S3Credentials::Credentials { access_keys } => {
                debug!(access_key = %access_keys.access_key, "using explicit credentials");
                let creds = aws_sdk_s3::config::Credentials::new(
                    &access_keys.access_key,
                    &access_keys.secret_access_key,
                    access_keys.session_token.clone(),
                    None, // expiry
                    "s3ls-rs-cli",
                );
                config_loader.credentials_provider(creds)
            }
            S3Credentials::FromEnvironment => {
                debug!("using environment credentials");
                config_loader
            }
        }
    }

    /// Build a region provider chain: explicit region -> profile -> default.
    fn build_region_provider(&self) -> Box<dyn aws_config::meta::region::ProvideRegion> {
        let chain = match &self.region {
            Some(region) => {
                debug!(%region, "using explicit region");
                RegionProviderChain::first_try(Region::new(region.clone()))
                    .or_default_provider()
            }
            None => {
                debug!("using default region provider chain");
                RegionProviderChain::default_provider()
            }
        };
        Box::new(chain)
    }

    /// Build retry configuration from CLI settings.
    fn build_retry_config(&self) -> RetryConfig {
        RetryConfig::standard()
            .with_max_attempts(self.retry_config.aws_max_attempts)
            .with_initial_backoff(Duration::from_millis(
                self.retry_config.initial_backoff_milliseconds,
            ))
    }

    /// Build timeout configuration from CLI settings, returning `None` if no
    /// timeouts are configured.
    fn build_timeout_config(&self) -> Option<TimeoutConfig> {
        let cli = &self.cli_timeout_config;

        if cli.operation_timeout_milliseconds.is_none()
            && cli.operation_attempt_timeout_milliseconds.is_none()
            && cli.connect_timeout_milliseconds.is_none()
            && cli.read_timeout_milliseconds.is_none()
        {
            return None;
        }

        let mut builder = TimeoutConfig::builder();

        if let Some(ms) = cli.operation_timeout_milliseconds {
            builder = builder.operation_timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = cli.operation_attempt_timeout_milliseconds {
            builder = builder.operation_attempt_timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = cli.connect_timeout_milliseconds {
            builder = builder.connect_timeout(Duration::from_millis(ms));
        }
        if let Some(ms) = cli.read_timeout_milliseconds {
            builder = builder.read_timeout(Duration::from_millis(ms));
        }

        Some(builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CLITimeoutConfig, ClientConfigLocation, RetryConfig as CliRetryConfig};

    fn default_client_config() -> ClientConfig {
        ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: None,
            },
            credential: S3Credentials::FromEnvironment,
            region: None,
            endpoint_url: None,
            force_path_style: false,
            accelerate: false,
            request_payer: None,
            request_checksum_calculation:
                aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired,
            retry_config: CliRetryConfig {
                aws_max_attempts: 3,
                initial_backoff_milliseconds: 500,
            },
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
        }
    }

    #[test]
    fn build_retry_config_uses_settings() {
        let mut config = default_client_config();
        config.retry_config.aws_max_attempts = 5;
        config.retry_config.initial_backoff_milliseconds = 1000;

        let retry = config.build_retry_config();
        assert_eq!(retry.max_attempts(), 5);
        assert_eq!(retry.initial_backoff(), Duration::from_millis(1000));
    }

    #[test]
    fn build_timeout_config_none_when_all_empty() {
        let config = default_client_config();
        assert!(config.build_timeout_config().is_none());
    }

    #[test]
    fn build_timeout_config_some_when_set() {
        let mut config = default_client_config();
        config.cli_timeout_config.operation_timeout_milliseconds = Some(5000);
        config.cli_timeout_config.connect_timeout_milliseconds = Some(2000);

        let timeout = config.build_timeout_config();
        assert!(timeout.is_some());
    }

    #[test]
    fn build_region_provider_explicit() {
        let mut config = default_client_config();
        config.region = Some("us-west-2".to_string());
        // Just ensure it doesn't panic
        let _provider = config.build_region_provider();
    }

    #[test]
    fn build_region_provider_default() {
        let config = default_client_config();
        let _provider = config.build_region_provider();
    }
}
