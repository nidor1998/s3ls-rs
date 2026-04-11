//! Modified-time "after" filter.
//!
//! Reused from s3rm-rs's `filters/mtime_after.rs`.
//! Passes entries whose last_modified time is at or after the configured threshold.

use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use chrono::{DateTime, Utc};
use tracing::debug;

const FILTER_NAME: &str = "MtimeAfterFilter";

pub struct MtimeAfterFilter {
    after: DateTime<Utc>,
}

impl MtimeAfterFilter {
    pub fn new(after: DateTime<Utc>) -> Self {
        Self { after }
    }
}

impl ObjectFilter for MtimeAfterFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        match entry.last_modified() {
            Some(lm) => {
                if *lm < self.after {
                    debug!(
                        name = FILTER_NAME,
                        key = entry.key(),
                        delete_marker = entry.is_delete_marker(),
                        version_id = entry.version_id(),
                        last_modified = %lm.to_rfc3339(),
                        config_time = %self.after.to_rfc3339(),
                        "entry filtered."
                    );
                    return false;
                }
                true
            }
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::ObjectFilter;
    use crate::types::{ListEntry, S3Object};
    use chrono::{Duration, Utc};

    fn make_entry_at(time: DateTime<Utc>) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(),
            size: 100,
            last_modified: time,
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
    fn matches_newer_entries() {
        let now = Utc::now();
        let filter = MtimeAfterFilter::new(now);
        assert!(filter.matches(&make_entry_at(now + Duration::hours(1))));
        assert!(!filter.matches(&make_entry_at(now - Duration::hours(1))));
    }

    #[test]
    fn exact_time_matches() {
        let now = Utc::now();
        let filter = MtimeAfterFilter::new(now);
        assert!(filter.matches(&make_entry_at(now)));
    }
}
