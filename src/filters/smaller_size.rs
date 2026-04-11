//! Smaller-size filter.
//!
//! Reused from s3rm-rs's `filters/smaller_size.rs`.
//! Passes entries whose size is strictly less than the configured threshold.
//! Delete markers always pass (they have no meaningful size).

use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use tracing::debug;

const FILTER_NAME: &str = "SmallerSizeFilter";

pub struct SmallerSizeFilter {
    threshold: u64,
}

impl SmallerSizeFilter {
    pub fn new(threshold: u64) -> Self {
        Self { threshold }
    }
}

impl ObjectFilter for SmallerSizeFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        if matches!(entry, ListEntry::DeleteMarker { .. }) {
            return true;
        }
        let size = entry.size();
        if size >= self.threshold {
            debug!(
                name = FILTER_NAME,
                key = entry.key(),
                content_length = size,
                delete_marker = entry.is_delete_marker(),
                version_id = entry.version_id(),
                config_size = self.threshold,
                "entry filtered."
            );
            return false;
        }
        true
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
    fn matches_smaller_entries() {
        let filter = SmallerSizeFilter::new(100);
        assert!(filter.matches(&make_entry_with_size(50)));
        assert!(!filter.matches(&make_entry_with_size(100)));
        assert!(!filter.matches(&make_entry_with_size(200)));
    }

    #[test]
    fn delete_marker_passes_through() {
        let filter = SmallerSizeFilter::new(100);
        let entry = ListEntry::DeleteMarker {
            key: "test.txt".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
            owner_display_name: None,
            owner_id: None,
        };
        assert!(filter.matches(&entry));
    }

    #[test]
    fn zero_size_smaller_than_threshold() {
        let filter = SmallerSizeFilter::new(1);
        assert!(filter.matches(&make_entry_with_size(0)));
    }

    #[test]
    fn zero_size_not_smaller_than_zero_threshold() {
        let filter = SmallerSizeFilter::new(0);
        // 0 >= 0, so NOT smaller
        assert!(!filter.matches(&make_entry_with_size(0)));
    }

    #[test]
    fn size_one_below_threshold() {
        let filter = SmallerSizeFilter::new(100);
        assert!(filter.matches(&make_entry_with_size(99)));
    }

    #[test]
    fn size_one_above_threshold() {
        let filter = SmallerSizeFilter::new(100);
        assert!(!filter.matches(&make_entry_with_size(101)));
    }

    #[test]
    fn threshold_at_u64_max() {
        let filter = SmallerSizeFilter::new(u64::MAX);
        // Any finite size < u64::MAX
        assert!(filter.matches(&make_entry_with_size(u64::MAX - 1)));
    }

    #[test]
    fn size_at_u64_max_equals_threshold() {
        let filter = SmallerSizeFilter::new(u64::MAX);
        // Equal, so NOT smaller
        assert!(!filter.matches(&make_entry_with_size(u64::MAX)));
    }
}
