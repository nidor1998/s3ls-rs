//! Storage class filter.
//!
//! New filter for s3ls-rs (no s3rm-rs equivalent).
//! Passes entries whose storage class matches one of the configured classes.
//! Delete markers and CommonPrefix entries always pass.

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
    fn matches(&self, entry: &ListEntry) -> bool {
        match entry {
            ListEntry::Object(obj) => match obj.storage_class() {
                Some(sc) => {
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
                    matched
                }
                None => {
                    debug!(
                        name = FILTER_NAME,
                        key = entry.key(),
                        delete_marker = entry.is_delete_marker(),
                        version_id = entry.version_id(),
                        "entry has no storage class, filtered."
                    );
                    false
                }
            },
            ListEntry::DeleteMarker { .. } => true,
            ListEntry::CommonPrefix(_) => true,
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
            checksum_algorithm: None,
            checksum_type: None,
        })
    }

    #[test]
    fn matches_listed_class() {
        let filter =
            StorageClassFilter::new(vec!["STANDARD".to_string(), "GLACIER".to_string()]);
        assert!(filter.matches(&make_entry_with_class(Some("STANDARD"))));
        assert!(filter.matches(&make_entry_with_class(Some("GLACIER"))));
        assert!(!filter.matches(&make_entry_with_class(Some("DEEP_ARCHIVE"))));
    }

    #[test]
    fn no_class_does_not_match() {
        let filter = StorageClassFilter::new(vec!["STANDARD".to_string()]);
        assert!(!filter.matches(&make_entry_with_class(None)));
    }

    #[test]
    fn delete_marker_passes_through() {
        let filter = StorageClassFilter::new(vec!["STANDARD".to_string()]);
        let entry = ListEntry::DeleteMarker {
            key: "test.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
        };
        assert!(filter.matches(&entry));
    }
}
