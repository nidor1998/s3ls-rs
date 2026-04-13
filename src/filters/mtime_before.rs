//! Modified-time "before" filter.
//!
//! Reused from s3rm-rs's `filters/mtime_before.rs`.
//! Passes entries whose last_modified time is strictly before the configured threshold.

use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use chrono::{DateTime, Utc};
use tracing::debug;

const FILTER_NAME: &str = "MtimeBeforeFilter";

pub struct MtimeBeforeFilter {
    before: DateTime<Utc>,
}

impl MtimeBeforeFilter {
    pub fn new(before: DateTime<Utc>) -> Self {
        Self { before }
    }
}

impl ObjectFilter for MtimeBeforeFilter {
    fn matches(&self, entry: &ListEntry) -> anyhow::Result<bool> {
        match entry.last_modified() {
            Some(lm) => {
                if self.before <= *lm {
                    debug!(
                        name = FILTER_NAME,
                        key = entry.key(),
                        delete_marker = entry.is_delete_marker(),
                        version_id = entry.version_id(),
                        last_modified = %lm.to_rfc3339(),
                        config_time = %self.before.to_rfc3339(),
                        "entry filtered."
                    );
                    return Ok(false);
                }
                Ok(true)
            }
            None => Ok(true),
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
        ListEntry::Object(S3Object {
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
            version_info: None,
        })
    }

    #[test]
    fn matches_older_entries() {
        let now = Utc::now();
        let filter = MtimeBeforeFilter::new(now);
        assert!(
            filter
                .matches(&make_entry_at(now - Duration::hours(1)))
                .unwrap()
        );
        assert!(
            !filter
                .matches(&make_entry_at(now + Duration::hours(1)))
                .unwrap()
        );
    }

    #[test]
    fn exact_time_does_not_match() {
        let now = Utc::now();
        let filter = MtimeBeforeFilter::new(now);
        assert!(!filter.matches(&make_entry_at(now)).unwrap());
    }

    #[test]
    fn common_prefix_passes() {
        let filter = MtimeBeforeFilter::new(Utc::now());
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        assert!(filter.matches(&entry).unwrap());
    }
}
