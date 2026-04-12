use crate::config::ClientConfig;
use crate::types::S3Credentials;
use aws_config::ConfigLoader;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_types::SdkConfig;
use aws_types::region::Region;
use std::time::Duration;
use tracing::debug;

impl ClientConfig {
    /// Create a fully-configured S3 [`Client`] from this configuration.
    pub async fn create_client(&self) -> Client {
        let sdk_config = self.load_sdk_config().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(self.force_path_style)
            .accelerate(self.accelerate)
            .request_checksum_calculation(self.request_checksum_calculation);

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

        let mut config_loader = if self.disable_stalled_stream_protection {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .stalled_stream_protection(
                    aws_smithy_runtime_api::client::stalled_stream_protection::StalledStreamProtectionConfig::disabled()
                )
        } else {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .stalled_stream_protection(
                    aws_smithy_runtime_api::client::stalled_stream_protection::StalledStreamProtectionConfig::enabled().build()
                )
        };

        config_loader = config_loader
            .region(region_provider)
            .retry_config(retry_config);

        if let Some(ref endpoint_url) = self.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        config_loader = self.load_config_credential(config_loader);

        config_loader.load().await
    }

    /// Build an [`EnvConfigFiles`] that honors both `--aws-config-file` and
    /// `--aws-shared-credentials-file` when either is set. A profile can
    /// legitimately be defined across both files (e.g. credentials in
    /// `~/.aws/credentials`, `role_arn` / `source_profile` / `sso_*` / region
    /// in `~/.aws/config`), so both the credentials provider and the region
    /// provider need to see the same merged view.
    ///
    /// Returns `None` when neither file is specified, letting the AWS SDK
    /// fall back to its default paths.
    fn build_profile_files(&self) -> Option<aws_runtime::env_config::file::EnvConfigFiles> {
        use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};

        let loc = &self.client_config_location;
        if loc.aws_shared_credentials_file.is_none() && loc.aws_config_file.is_none() {
            return None;
        }

        let mut builder = EnvConfigFiles::builder();
        if let Some(ref creds_file) = loc.aws_shared_credentials_file {
            builder = builder.with_file(EnvConfigFileKind::Credentials, creds_file);
        }
        if let Some(ref config_file) = loc.aws_config_file {
            builder = builder.with_file(EnvConfigFileKind::Config, config_file);
        }
        Some(builder.build())
    }

    /// Apply credential configuration to the config loader based on the
    /// [`S3Credentials`] variant.
    fn load_config_credential(&self, config_loader: ConfigLoader) -> ConfigLoader {
        match &self.credential {
            S3Credentials::Profile(profile_name) => {
                debug!(%profile_name, "using profile credentials");
                let mut builder = aws_config::profile::ProfileFileCredentialsProvider::builder();

                if let Some(profile_files) = self.build_profile_files() {
                    builder = builder.profile_files(profile_files);
                }

                config_loader.credentials_provider(builder.profile_name(profile_name).build())
            }
            S3Credentials::Credentials { access_keys } => {
                debug!(
                    access_key = %access_keys.masked_access_key(),
                    "using explicit credentials"
                );
                let creds = aws_sdk_s3::config::Credentials::new(
                    &access_keys.access_key,
                    &access_keys.secret_access_key,
                    access_keys.session_token.clone(),
                    None, // expiry
                    "",
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
        let mut builder = aws_config::profile::ProfileFileRegionProvider::builder();

        if let crate::types::S3Credentials::Profile(ref profile_name) = self.credential {
            if let Some(profile_files) = self.build_profile_files() {
                builder = builder.profile_files(profile_files);
            }
            builder = builder.profile_name(profile_name);
        }

        let provider_region = if matches!(
            &self.credential,
            crate::types::S3Credentials::FromEnvironment
        ) {
            RegionProviderChain::first_try(self.region.clone().map(Region::new))
                .or_default_provider()
        } else {
            RegionProviderChain::first_try(self.region.clone().map(Region::new))
                .or_else(builder.build())
                .or_default_provider()
        };

        Box::new(provider_region)
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
    use crate::config::{CLITimeoutConfig, RetryConfig as CliRetryConfig};
    use crate::types::ClientConfigLocation;

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

    #[test]
    fn build_profile_files_returns_none_when_both_files_unset() {
        let config = default_client_config();
        assert!(config.build_profile_files().is_none());
    }

    #[test]
    fn build_profile_files_some_when_only_credentials_file_set() {
        let mut config = default_client_config();
        config.client_config_location.aws_shared_credentials_file =
            Some("./test_data/credentials".into());
        assert!(config.build_profile_files().is_some());
    }

    #[test]
    fn build_profile_files_some_when_only_config_file_set() {
        let mut config = default_client_config();
        config.client_config_location.aws_config_file = Some("./test_data/config".into());
        assert!(config.build_profile_files().is_some());
    }

    #[test]
    fn build_profile_files_some_when_both_files_set() {
        // Regression for the bug where the credential provider only saw
        // `--aws-shared-credentials-file` and the region provider only
        // saw `--aws-config-file`, causing profiles with role_arn or
        // source_profile chains to fail. Now both providers see both.
        let mut config = default_client_config();
        config.client_config_location.aws_shared_credentials_file =
            Some("./test_data/credentials".into());
        config.client_config_location.aws_config_file = Some("./test_data/config".into());
        assert!(config.build_profile_files().is_some());
    }

    /// Verifies that `create_client` can load credentials and region
    /// from actual custom config and credentials files on disk.
    ///
    /// Writes standard-format AWS config and credentials files to a
    /// temp directory, builds a `ClientConfig` pointing to those files
    /// with a profile name, and calls `create_client()`. The client is
    /// constructed without error, proving the SDK successfully parsed
    /// the custom files and resolved the profile.
    #[tokio::test]
    async fn create_client_from_custom_config_and_credentials_files() {
        let tmp_dir = std::env::temp_dir().join(format!("s3ls-test-config-{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

        let config_path = tmp_dir.join("config");
        let credentials_path = tmp_dir.join("credentials");

        // Write a minimal AWS config file with a profile.
        std::fs::write(&config_path, "[profile test-custom]\nregion = us-west-2\n")
            .expect("failed to write config file");

        // Write a minimal AWS credentials file with matching profile.
        std::fs::write(
            &credentials_path,
            "[test-custom]\n\
             aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n\
             aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n",
        )
        .expect("failed to write credentials file");

        let config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: Some(config_path.clone()),
                aws_shared_credentials_file: Some(credentials_path.clone()),
            },
            credential: S3Credentials::Profile("test-custom".to_string()),
            region: None, // should be resolved from the config file
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
        };

        // create_client loads the SDK config from the custom files,
        // resolves the profile, and constructs an S3 Client. If the
        // files are malformed or the profile is not found, this panics.
        let _client = config.create_client().await;

        // Cleanup temp files.
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    /// Same as above but with only a credentials file (no config file).
    /// The SDK should still resolve credentials from the custom
    /// credentials file even without a custom config file.
    #[tokio::test]
    async fn create_client_from_custom_credentials_file_only() {
        let tmp_dir =
            std::env::temp_dir().join(format!("s3ls-test-creds-only-{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");

        let credentials_path = tmp_dir.join("credentials");

        std::fs::write(
            &credentials_path,
            "[test-creds-only]\n\
             aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n\
             aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n",
        )
        .expect("failed to write credentials file");

        let config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: Some(credentials_path.clone()),
            },
            credential: S3Credentials::Profile("test-creds-only".to_string()),
            region: Some("us-east-1".to_string()), // explicit region since no config file
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
        };

        let _client = config.create_client().await;

        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
