//! Larger-size filter.
//!
//! Reused from s3rm-rs's `filters/larger_size.rs`.
//! Passes entries whose size is greater than or equal to the configured threshold.
//! Delete markers always pass (they have no meaningful size).

use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use tracing::debug;

const FILTER_NAME: &str = "LargerSizeFilter";

pub struct LargerSizeFilter {
    threshold: u64,
}

impl LargerSizeFilter {
    pub fn new(threshold: u64) -> Self {
        Self { threshold }
    }
}

impl ObjectFilter for LargerSizeFilter {
    fn matches(&self, entry: &ListEntry) -> anyhow::Result<bool> {
        if matches!(entry, ListEntry::DeleteMarker { .. }) {
            return Ok(true);
        }
        let size = entry.size();
        if size < self.threshold {
            debug!(
                name = FILTER_NAME,
                key = entry.key(),
                content_length = size,
                delete_marker = entry.is_delete_marker(),
                version_id = entry.version_id(),
                config_size = self.threshold,
                "entry filtered."
            );
            return Ok(false);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::ObjectFilter;
    use crate::types::{ListEntry, S3Object};

    fn make_entry_with_size(size: u64) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(),
            size,
            last_modified: chrono::Utc::now(),
            e_tag: "\"e\"".to_string(),
            storage_class: None,
            checksum_algorithm: vec![],
            checksum_type: None,
            owner_display_name: None,
            owner_id: None,
            is_restore_in_progress: None,
            restore_expiry_date: None,
        })
    }

    #[test]
    fn matches_larger_entries() {
        let filter = LargerSizeFilter::new(100);
        assert!(!filter.matches(&make_entry_with_size(50)).unwrap());
        assert!(filter.matches(&make_entry_with_size(100)).unwrap());
        assert!(filter.matches(&make_entry_with_size(200)).unwrap());
    }

    #[test]
    fn delete_marker_passes_through() {
        let filter = LargerSizeFilter::new(100);
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
    fn zero_size_is_larger_or_equal_to_zero_threshold() {
        let filter = LargerSizeFilter::new(0);
        assert!(filter.matches(&make_entry_with_size(0)).unwrap());
    }

    #[test]
    fn zero_size_not_larger_than_one() {
        let filter = LargerSizeFilter::new(1);
        assert!(!filter.matches(&make_entry_with_size(0)).unwrap());
    }

    #[test]
    fn size_one_below_threshold() {
        let filter = LargerSizeFilter::new(100);
        assert!(!filter.matches(&make_entry_with_size(99)).unwrap());
    }

    #[test]
    fn size_one_above_threshold() {
        let filter = LargerSizeFilter::new(100);
        assert!(filter.matches(&make_entry_with_size(101)).unwrap());
    }

    #[test]
    fn threshold_at_u64_max() {
        let filter = LargerSizeFilter::new(u64::MAX);
        // u64::MAX - 1 < u64::MAX, so NOT larger or equal
        assert!(!filter.matches(&make_entry_with_size(u64::MAX - 1)).unwrap());
    }

    #[test]
    fn size_at_u64_max_equals_threshold() {
        let filter = LargerSizeFilter::new(u64::MAX);
        // Equal, so passes
        assert!(filter.matches(&make_entry_with_size(u64::MAX)).unwrap());
    }
}
