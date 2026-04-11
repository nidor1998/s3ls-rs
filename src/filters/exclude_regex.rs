//! Exclude regex filter.
//!
//! Reused from s3rm-rs's `filters/exclude_regex.rs`.
//! Passes entries whose key does NOT match the configured exclude regex pattern.

use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use fancy_regex::Regex;
use tracing::{debug, warn};

const FILTER_NAME: &str = "ExcludeRegexFilter";

pub struct ExcludeRegexFilter {
    regex: Regex,
}

impl ExcludeRegexFilter {
    pub fn new(pattern: &str) -> Result<Self, String> {
        let regex = Regex::new(pattern).map_err(|e| e.to_string())?;
        Ok(Self { regex })
    }

    pub fn from_regex(regex: Regex) -> Self {
        Self { regex }
    }
}

impl ObjectFilter for ExcludeRegexFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        let match_result = match self.regex.is_match(entry.key()) {
            Ok(matched) => matched,
            Err(e) => {
                warn!(
                    name = FILTER_NAME,
                    key = entry.key(),
                    error = %e,
                    "regex match failed, keeping entry to be safe."
                );
                return true;
            }
        };

        if match_result {
            debug!(
                name = FILTER_NAME,
                key = entry.key(),
                delete_marker = entry.is_delete_marker(),
                version_id = entry.version_id(),
                exclude_regex = self.regex.as_str(),
                "entry filtered."
            );
        }

        !match_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::ObjectFilter;
    use crate::types::{ListEntry, S3Object};

    fn make_entry(key: &str) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(),
            size: 100,
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
    fn excludes_matching_key() {
        let filter = ExcludeRegexFilter::new(r".*\.log$").unwrap();
        assert!(!filter.matches(&make_entry("app.log")));
        assert!(filter.matches(&make_entry("app.txt")));
    }

    #[test]
    fn excludes_delete_marker_by_key() {
        let filter = ExcludeRegexFilter::new(r".*\.log$").unwrap();
        let entry = ListEntry::DeleteMarker {
            key: "app.log".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
            owner_display_name: None,
            owner_id: None,
        };
        assert!(!filter.matches(&entry));
    }

    #[test]
    fn regex_error_keeps_entry() {
        use fancy_regex::RegexBuilder;

        // Backreference forces fancy_regex VM. With backtrack_limit(1),
        // matching against a non-matching string triggers RuntimeError.
        let regex = RegexBuilder::new(r"(a+)\1b")
            .backtrack_limit(1)
            .build()
            .unwrap();

        // Verify this actually produces an error (not Ok)
        assert!(regex.is_match("aaaaaaaaaaaaaaac").is_err());

        // On error, exclude filter keeps entry (returns true) to be safe
        let filter = ExcludeRegexFilter::from_regex(regex);
        assert!(filter.matches(&make_entry("aaaaaaaaaaaaaaac")));
    }
}
