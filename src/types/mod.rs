pub mod error;
pub mod token;

use chrono::{DateTime, Utc};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// S3Credentials / AccessKeys
// ---------------------------------------------------------------------------

/// S3 credential types.
#[derive(Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials {
        access_keys: AccessKeys,
    },
    FromEnvironment,
    /// Disable request signing entirely and do not attempt to load
    /// credentials from any source. Used for public (anonymous) S3
    /// buckets and similar read-only public endpoints.
    NoSign,
}

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Credentials::Profile(p) => f.debug_tuple("Profile").field(p).finish(),
            S3Credentials::Credentials { access_keys } => f
                .debug_struct("Credentials")
                .field("access_keys", access_keys)
                .finish(),
            S3Credentials::FromEnvironment => write!(f, "FromEnvironment"),
            S3Credentials::NoSign => write!(f, "NoSign"),
        }
    }
}

/// AWS access key pair with secure zeroization.
///
/// The secret_access_key and session_token are securely cleared from memory
/// when this struct is dropped, using the zeroize crate.
#[derive(Clone, zeroize_derive::Zeroize, zeroize_derive::ZeroizeOnDrop)]
pub struct AccessKeys {
    pub access_key: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl AccessKeys {
    /// Return a masked form of the access key ID safe to log.
    ///
    /// For a 20-character AWS access key ID (e.g. `AKIAIOSFODNN7EXAMPLE`),
    /// returns `AKIA************MPLE` — the first 4 characters (which
    /// identify the credential type: `AKIA` for long-term IAM user keys,
    /// `ASIA` for STS temporary credentials, `AROA` for role credentials,
    /// etc.) and the last 4 characters, with the middle replaced by
    /// asterisks. Keys shorter than 8 characters are fully redacted to
    /// avoid accidentally revealing short secrets.
    pub fn masked_access_key(&self) -> String {
        mask_access_key(&self.access_key)
    }
}

fn mask_access_key(key: &str) -> String {
    let len = key.chars().count();
    if len < 8 {
        return "** redacted **".to_string();
    }
    let prefix: String = key.chars().take(4).collect();
    let suffix: String = key.chars().skip(len - 4).collect();
    let masked_middle = "*".repeat(len - 8);
    format!("{prefix}{masked_middle}{suffix}")
}

impl std::fmt::Debug for AccessKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let session_token = self
            .session_token
            .as_ref()
            .map_or("None", |_| "** redacted **");
        f.debug_struct("AccessKeys")
            .field("access_key", &self.masked_access_key())
            .field("secret_access_key", &"** redacted **")
            .field("session_token", &session_token)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ClientConfigLocation
// ---------------------------------------------------------------------------

/// AWS configuration file locations.
#[derive(Debug, Clone)]
pub struct ClientConfigLocation {
    pub aws_config_file: Option<PathBuf>,
    pub aws_shared_credentials_file: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// S3Target
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct S3Target {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl S3Target {
    pub fn parse(s3_uri: &str) -> Result<Self, error::S3lsError> {
        if !s3_uri.starts_with("s3://") {
            return Err(error::S3lsError::InvalidUri(format!(
                "Target URI must start with 's3://': {s3_uri}"
            )));
        }

        let without_scheme = &s3_uri[5..];

        if without_scheme.is_empty() {
            return Err(error::S3lsError::InvalidUri(format!(
                "Bucket name cannot be empty: {s3_uri}"
            )));
        }

        let (bucket, prefix) = match without_scheme.find('/') {
            Some(idx) => {
                let bucket = &without_scheme[..idx];
                let prefix = &without_scheme[idx + 1..];
                (
                    bucket.to_string(),
                    if prefix.is_empty() {
                        None
                    } else {
                        Some(prefix.to_string())
                    },
                )
            }
            None => (without_scheme.to_string(), None),
        };

        if bucket.is_empty() {
            return Err(error::S3lsError::InvalidUri(format!(
                "Bucket name cannot be empty: {s3_uri}"
            )));
        }

        Ok(S3Target { bucket, prefix })
    }
}

impl Display for S3Target {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.prefix {
            Some(prefix) => write!(f, "s3://{}/{}", self.bucket, prefix),
            None => write!(f, "s3://{}", self.bucket),
        }
    }
}

// ---------------------------------------------------------------------------
// ListEntry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ListEntry {
    Object(S3Object),
    CommonPrefix(String),
    DeleteMarker {
        key: String,
        version_info: VersionInfo,
        last_modified: DateTime<Utc>,
        owner_display_name: Option<String>,
        owner_id: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// VersionInfo
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version_id: String,
    pub is_latest: bool,
}

// ---------------------------------------------------------------------------
// S3Object
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub e_tag: String,
    pub storage_class: Option<String>,
    pub checksum_algorithm: Vec<String>,
    pub checksum_type: Option<String>,
    pub owner_display_name: Option<String>,
    pub owner_id: Option<String>,
    pub is_restore_in_progress: Option<bool>,
    pub restore_expiry_date: Option<String>,
    pub version_info: Option<VersionInfo>,
}

impl S3Object {
    pub fn version_id(&self) -> Option<&str> {
        self.version_info.as_ref().map(|v| v.version_id.as_str())
    }

    pub fn is_latest(&self) -> bool {
        self.version_info.as_ref().is_none_or(|v| v.is_latest)
    }
}

impl ListEntry {
    pub fn key(&self) -> &str {
        match self {
            Self::Object(obj) => &obj.key,
            Self::CommonPrefix(prefix) => prefix,
            Self::DeleteMarker { key, .. } => key,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::Object(obj) => obj.size,
            Self::CommonPrefix(_) => 0,
            Self::DeleteMarker { .. } => 0,
        }
    }

    pub fn last_modified(&self) -> Option<&DateTime<Utc>> {
        match self {
            Self::Object(obj) => Some(&obj.last_modified),
            Self::CommonPrefix(_) => None,
            Self::DeleteMarker { last_modified, .. } => Some(last_modified),
        }
    }

    pub fn version_id(&self) -> Option<&str> {
        match self {
            Self::Object(obj) => obj.version_id(),
            Self::DeleteMarker { version_info, .. } => Some(&version_info.version_id),
            Self::CommonPrefix(_) => None,
        }
    }

    pub fn is_delete_marker(&self) -> bool {
        matches!(self, Self::DeleteMarker { .. })
    }
}

// ---------------------------------------------------------------------------
// ListingStatistics
// ---------------------------------------------------------------------------

pub struct ListingStatistics {
    pub total_objects: u64,
    pub total_size: u64,
    pub total_delete_markers: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masked_access_key_preserves_prefix_and_suffix() {
        // Typical 20-char AWS access key ID
        assert_eq!(
            mask_access_key("AKIAIOSFODNN7EXAMPLE"),
            "AKIA************MPLE"
        );
    }

    #[test]
    fn masked_access_key_preserves_sts_prefix() {
        // STS temporary credentials start with ASIA
        assert_eq!(
            mask_access_key("ASIAIOSFODNN7EXAMPLE"),
            "ASIA************MPLE"
        );
    }

    #[test]
    fn masked_access_key_fully_redacts_short_values() {
        assert_eq!(mask_access_key(""), "** redacted **");
        assert_eq!(mask_access_key("AKIA"), "** redacted **");
        assert_eq!(mask_access_key("AKIA123"), "** redacted **");
    }

    #[test]
    fn masked_access_key_handles_exact_minimum_length() {
        // 8 characters: 4 prefix + 0 middle + 4 suffix, no asterisks
        assert_eq!(mask_access_key("ABCD1234"), "ABCD1234");
    }

    #[test]
    fn access_keys_debug_masks_access_key() {
        let keys = AccessKeys {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "supersecret".to_string(),
            session_token: Some("XYZZY-secret-token".to_string()),
        };
        let rendered = format!("{keys:?}");
        // Masked access key appears, raw form does not
        assert!(rendered.contains("AKIA************MPLE"));
        assert!(!rendered.contains("AKIAIOSFODNN7EXAMPLE"));
        // Secret is still fully redacted
        assert!(rendered.contains("** redacted **"));
        assert!(!rendered.contains("supersecret"));
        assert!(!rendered.contains("XYZZY-secret-token"));
    }

    #[test]
    fn s3_target_parse_bucket_only() {
        let target = S3Target::parse("s3://my-bucket").unwrap();
        assert_eq!(target.bucket, "my-bucket");
        assert!(target.prefix.is_none());
    }

    #[test]
    fn s3_target_parse_bucket_with_trailing_slash() {
        let target = S3Target::parse("s3://my-bucket/").unwrap();
        assert_eq!(target.bucket, "my-bucket");
        assert!(target.prefix.is_none());
    }

    #[test]
    fn s3_target_parse_bucket_with_prefix() {
        let target = S3Target::parse("s3://my-bucket/logs/2023/").unwrap();
        assert_eq!(target.bucket, "my-bucket");
        assert_eq!(target.prefix.as_deref(), Some("logs/2023/"));
    }

    #[test]
    fn s3_target_parse_invalid_no_scheme() {
        let result = S3Target::parse("my-bucket/prefix");
        assert!(result.is_err());
    }

    #[test]
    fn s3_target_parse_invalid_empty_bucket() {
        let result = S3Target::parse("s3://");
        assert!(result.is_err());
    }

    #[test]
    fn s3_target_parse_invalid_empty_bucket_with_slash() {
        let result = S3Target::parse("s3:///prefix");
        assert!(result.is_err());
    }

    #[test]
    fn s3_target_display_bucket_only() {
        let target = S3Target {
            bucket: "my-bucket".to_string(),
            prefix: None,
        };
        assert_eq!(target.to_string(), "s3://my-bucket");
    }

    #[test]
    fn s3_target_display_with_prefix() {
        let target = S3Target {
            bucket: "my-bucket".to_string(),
            prefix: Some("logs/2023/".to_string()),
        };
        assert_eq!(target.to_string(), "s3://my-bucket/logs/2023/");
    }

    #[test]
    fn s3_target_roundtrip() {
        let uri = "s3://my-bucket/some/prefix/";
        let target = S3Target::parse(uri).unwrap();
        assert_eq!(target.to_string(), uri);
    }

    #[test]
    fn s3_object_not_versioning_getters() {
        let obj = S3Object {
            key: "test/key.txt".to_string(),
            size: 1024,
            last_modified: Utc::now(),
            e_tag: "\"abc123\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        };
        assert_eq!(obj.key, "test/key.txt");
        assert_eq!(obj.size, 1024);
        assert_eq!(obj.e_tag, "\"abc123\"");
        assert_eq!(obj.storage_class.as_deref(), Some("STANDARD"));
        assert!(obj.version_id().is_none());
        assert!(obj.is_latest());
    }

    #[test]
    fn s3_object_versioning_getters() {
        let obj = S3Object {
            key: "test/key.txt".to_string(),
            size: 2048,
            last_modified: Utc::now(),
            e_tag: "\"def456\"".to_string(),
            storage_class: Some("GLACIER".to_string()),
            checksum_algorithm: vec!["SHA256".to_string()],
            checksum_type: Some("FULL_OBJECT".to_string()),
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: Some(VersionInfo {
                version_id: "v1".to_string(),
                is_latest: false,
            }),
        };
        assert_eq!(obj.key, "test/key.txt");
        assert_eq!(obj.size, 2048);
        assert_eq!(obj.version_id(), Some("v1"));
        assert!(!obj.is_latest());
        assert_eq!(obj.storage_class.as_deref(), Some("GLACIER"));
        assert_eq!(obj.checksum_algorithm.as_slice(), &["SHA256"]);
        assert_eq!(obj.checksum_type.as_deref(), Some("FULL_OBJECT"));
    }

    #[test]
    fn list_entry_object_key_and_size() {
        let entry = ListEntry::Object(S3Object {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        });
        assert_eq!(entry.key(), "file.txt");
        assert_eq!(entry.size(), 100);
        assert!(entry.last_modified().is_some());
    }

    #[test]
    fn list_entry_common_prefix() {
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        assert_eq!(entry.key(), "logs/");
        assert_eq!(entry.size(), 0);
        assert!(entry.last_modified().is_none());
    }

    #[test]
    fn list_entry_delete_marker() {
        let entry = ListEntry::DeleteMarker {
            key: "deleted.txt".to_string(),
            version_info: VersionInfo {
                version_id: "dm-v1".to_string(),
                is_latest: true,
            },
            last_modified: Utc::now(),
            owner_display_name: None,
            owner_id: None,
        };
        assert_eq!(entry.key(), "deleted.txt");
        assert_eq!(entry.size(), 0);
        assert!(entry.last_modified().is_some());
    }

    #[test]
    fn s3_credentials_from_environment_debug() {
        let cred = S3Credentials::FromEnvironment;
        let rendered = format!("{cred:?}");
        assert_eq!(rendered, "FromEnvironment");
    }

    #[test]
    fn s3_credentials_profile_debug() {
        let cred = S3Credentials::Profile("my-profile".to_string());
        let rendered = format!("{cred:?}");
        assert!(rendered.contains("my-profile"));
    }

    #[test]
    fn s3_credentials_no_sign_debug() {
        let cred = S3Credentials::NoSign;
        let rendered = format!("{cred:?}");
        assert_eq!(rendered, "NoSign");
    }

    #[test]
    fn s3_credentials_credentials_debug() {
        let cred = S3Credentials::Credentials {
            access_keys: AccessKeys {
                access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            },
        };
        let rendered = format!("{cred:?}");
        assert!(rendered.contains("Credentials"));
        assert!(rendered.contains("AKIA"));
    }

    #[test]
    fn restore_expiry_date_not_versioning() {
        let obj = S3Object {
            key: "k".to_string(),
            size: 0,
            last_modified: Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: Some(false),
            restore_expiry_date: Some("2024-12-31T00:00:00Z".to_string()),
            version_info: None,
        };
        assert_eq!(
            obj.restore_expiry_date.as_deref(),
            Some("2024-12-31T00:00:00Z")
        );
        assert_eq!(obj.is_restore_in_progress, Some(false));
    }

    #[test]
    fn restore_expiry_date_versioning() {
        let obj = S3Object {
            key: "k".to_string(),
            size: 0,
            last_modified: Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: Some(true),
            restore_expiry_date: Some("2025-01-15T00:00:00Z".to_string()),
            version_info: Some(VersionInfo {
                version_id: "v1".to_string(),
                is_latest: true,
            }),
        };
        assert_eq!(
            obj.restore_expiry_date.as_deref(),
            Some("2025-01-15T00:00:00Z")
        );
        assert_eq!(obj.is_restore_in_progress, Some(true));
    }

    #[test]
    fn list_entry_version_id_for_all_variants() {
        let obj_entry = ListEntry::Object(S3Object {
            key: "a".to_string(),
            size: 0,
            last_modified: Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        });
        assert!(obj_entry.version_id().is_none());

        let dm_entry = ListEntry::DeleteMarker {
            key: "b".to_string(),
            version_info: VersionInfo {
                version_id: "dm-v1".to_string(),
                is_latest: true,
            },
            last_modified: Utc::now(),
            owner_display_name: None,
            owner_id: None,
        };
        assert_eq!(dm_entry.version_id(), Some("dm-v1"));

        let cp_entry = ListEntry::CommonPrefix("prefix/".to_string());
        assert!(cp_entry.version_id().is_none());
    }

    #[test]
    fn list_entry_is_delete_marker() {
        let dm = ListEntry::DeleteMarker {
            key: "k".to_string(),
            version_info: VersionInfo {
                version_id: "v".to_string(),
                is_latest: false,
            },
            last_modified: Utc::now(),
            owner_display_name: None,
            owner_id: None,
        };
        assert!(dm.is_delete_marker());

        let obj = ListEntry::Object(S3Object {
            key: "k".to_string(),
            size: 0,
            last_modified: Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
            version_info: None,
        });
        assert!(!obj.is_delete_marker());

        let cp = ListEntry::CommonPrefix("p/".to_string());
        assert!(!cp.is_delete_marker());
    }
}
