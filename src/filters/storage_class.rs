//! Storage class filter.
//!
//! New filter for s3ls-rs (no s3rm-rs equivalent).
//! Passes entries whose storage class matches one of the configured classes.
//! Delete markers and CommonPrefix entries always pass.
//!
//! **S3 API behavior:** The S3 ListObjectsV2 and ListObjectVersions APIs omit
//! the `StorageClass` field (returning `None`) for objects stored in the
//! STANDARD class. This filter treats `None` as `"STANDARD"` so that
//! `--storage-class STANDARD` correctly matches those objects.

use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use tracing::debug;

const FILTER_NAME: &str = "StorageClassFilter";

pub struct StorageClassFilter {
    classes: Vec<String>,
}

impl StorageClassFilter {
    pub fn new(classes: Vec<String>) -> Self {
        Self { classes }
    }
}

impl ObjectFilter for StorageClassFilter {
    fn matches(&self, entry: &ListEntry) -> anyhow::Result<bool> {
        match entry {
            ListEntry::Object(obj) => {
                // S3 API omits StorageClass for STANDARD objects (returns None).
                let sc = obj.storage_class().unwrap_or("STANDARD");
                let matched = self.classes.iter().any(|c| c == sc);
                if !matched {
                    debug!(
                        name = FILTER_NAME,
                        key = entry.key(),
                        delete_marker = entry.is_delete_marker(),
                        version_id = entry.version_id(),
                        storage_class = sc,
                        "entry filtered."
                    );
                }
                Ok(matched)
            }
            ListEntry::DeleteMarker { .. } => Ok(true),
            ListEntry::CommonPrefix(_) => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::ObjectFilter;
    use crate::types::{ListEntry, S3Object};

    fn make_entry_with_class(class: Option<&str>) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(),
            size: 100,
            last_modified: chrono::Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: class.map(|s| s.to_string()),
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
        })
    }

    #[test]
    fn matches_listed_class() {
        let filter = StorageClassFilter::new(vec!["STANDARD".to_string(), "GLACIER".to_string()]);
        assert!(
            filter
                .matches(&make_entry_with_class(Some("STANDARD")))
                .unwrap()
        );
        assert!(
            filter
                .matches(&make_entry_with_class(Some("GLACIER")))
                .unwrap()
        );
        assert!(
            !filter
                .matches(&make_entry_with_class(Some("DEEP_ARCHIVE")))
                .unwrap()
        );
    }

    #[test]
    fn none_class_treated_as_standard() {
        // S3 API omits StorageClass for STANDARD objects
        let filter = StorageClassFilter::new(vec!["STANDARD".to_string()]);
        assert!(filter.matches(&make_entry_with_class(None)).unwrap());
    }

    #[test]
    fn none_class_does_not_match_non_standard() {
        let filter = StorageClassFilter::new(vec!["GLACIER".to_string()]);
        assert!(!filter.matches(&make_entry_with_class(None)).unwrap());
    }

    #[test]
    fn delete_marker_passes_through() {
        let filter = StorageClassFilter::new(vec!["STANDARD".to_string()]);
        let entry = ListEntry::DeleteMarker {
            key: "test.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
            owner_display_name: None,
            owner_id: None,
        };
        assert!(filter.matches(&entry).unwrap());
    }

    #[test]
    fn common_prefix_passes_through() {
        let filter = StorageClassFilter::new(vec!["GLACIER".to_string()]);
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        assert!(filter.matches(&entry).unwrap());
    }
}
