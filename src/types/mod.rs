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
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
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

impl std::fmt::Debug for AccessKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let session_token = self
            .session_token
            .as_ref()
            .map_or("None", |_| "** redacted **");
        f.debug_struct("AccessKeys")
            .field("access_key", &self.access_key)
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
        version_id: String,
        last_modified: DateTime<Utc>,
        is_latest: bool,
    },
}

// ---------------------------------------------------------------------------
// S3Object
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum S3Object {
    NotVersioning {
        key: String,
        size: u64,
        last_modified: DateTime<Utc>,
        e_tag: String,
        storage_class: Option<String>,
        checksum_algorithm: Option<String>,
        checksum_type: Option<String>,
    },
    Versioning {
        key: String,
        version_id: String,
        size: u64,
        last_modified: DateTime<Utc>,
        e_tag: String,
        is_latest: bool,
        storage_class: Option<String>,
        checksum_algorithm: Option<String>,
        checksum_type: Option<String>,
    },
}

impl S3Object {
    pub fn key(&self) -> &str {
        match self {
            Self::NotVersioning { key, .. } => key,
            Self::Versioning { key, .. } => key,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::NotVersioning { size, .. } => *size,
            Self::Versioning { size, .. } => *size,
        }
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        match self {
            Self::NotVersioning { last_modified, .. } => last_modified,
            Self::Versioning { last_modified, .. } => last_modified,
        }
    }

    pub fn e_tag(&self) -> &str {
        match self {
            Self::NotVersioning { e_tag, .. } => e_tag,
            Self::Versioning { e_tag, .. } => e_tag,
        }
    }

    pub fn storage_class(&self) -> Option<&str> {
        match self {
            Self::NotVersioning { storage_class, .. } => storage_class.as_deref(),
            Self::Versioning { storage_class, .. } => storage_class.as_deref(),
        }
    }

    pub fn checksum_algorithm(&self) -> Option<&str> {
        match self {
            Self::NotVersioning { checksum_algorithm, .. } => checksum_algorithm.as_deref(),
            Self::Versioning { checksum_algorithm, .. } => checksum_algorithm.as_deref(),
        }
    }

    pub fn checksum_type(&self) -> Option<&str> {
        match self {
            Self::NotVersioning { checksum_type, .. } => checksum_type.as_deref(),
            Self::Versioning { checksum_type, .. } => checksum_type.as_deref(),
        }
    }

    pub fn version_id(&self) -> Option<&str> {
        match self {
            Self::NotVersioning { .. } => None,
            Self::Versioning { version_id, .. } => Some(version_id),
        }
    }

    pub fn is_latest(&self) -> bool {
        match self {
            Self::NotVersioning { .. } => true,
            Self::Versioning { is_latest, .. } => *is_latest,
        }
    }
}

impl ListEntry {
    pub fn key(&self) -> &str {
        match self {
            Self::Object(obj) => obj.key(),
            Self::CommonPrefix(prefix) => prefix,
            Self::DeleteMarker { key, .. } => key,
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            Self::Object(obj) => obj.size(),
            Self::CommonPrefix(_) => 0,
            Self::DeleteMarker { .. } => 0,
        }
    }

    pub fn last_modified(&self) -> Option<&DateTime<Utc>> {
        match self {
            Self::Object(obj) => Some(obj.last_modified()),
            Self::CommonPrefix(_) => None,
            Self::DeleteMarker { last_modified, .. } => Some(last_modified),
        }
    }
}

// ---------------------------------------------------------------------------
// ListingStatistics
// ---------------------------------------------------------------------------

pub struct ListingStatistics {
    pub total_objects: u64,
    pub total_size: u64,
    pub total_versions: u64,
    pub total_delete_markers: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
        let obj = S3Object::NotVersioning {
            key: "test/key.txt".to_string(),
            size: 1024,
            last_modified: Utc::now(),
            e_tag: "\"abc123\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: None,
            checksum_type: None,
        };
        assert_eq!(obj.key(), "test/key.txt");
        assert_eq!(obj.size(), 1024);
        assert_eq!(obj.e_tag(), "\"abc123\"");
        assert_eq!(obj.storage_class(), Some("STANDARD"));
        assert!(obj.version_id().is_none());
        assert!(obj.is_latest());
    }

    #[test]
    fn s3_object_versioning_getters() {
        let obj = S3Object::Versioning {
            key: "test/key.txt".to_string(),
            version_id: "v1".to_string(),
            size: 2048,
            last_modified: Utc::now(),
            e_tag: "\"def456\"".to_string(),
            is_latest: false,
            storage_class: Some("GLACIER".to_string()),
            checksum_algorithm: Some("SHA256".to_string()),
            checksum_type: Some("FULL_OBJECT".to_string()),
        };
        assert_eq!(obj.key(), "test/key.txt");
        assert_eq!(obj.size(), 2048);
        assert_eq!(obj.version_id(), Some("v1"));
        assert!(!obj.is_latest());
        assert_eq!(obj.storage_class(), Some("GLACIER"));
        assert_eq!(obj.checksum_algorithm(), Some("SHA256"));
        assert_eq!(obj.checksum_type(), Some("FULL_OBJECT"));
    }

    #[test]
    fn list_entry_object_key_and_size() {
        let entry = ListEntry::Object(S3Object::NotVersioning {
            key: "file.txt".to_string(),
            size: 100,
            last_modified: Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: None,
            checksum_type: None,
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
            version_id: "dm-v1".to_string(),
            last_modified: Utc::now(),
            is_latest: true,
        };
        assert_eq!(entry.key(), "deleted.txt");
        assert_eq!(entry.size(), 0);
        assert!(entry.last_modified().is_some());
    }
}
