//! Filter system for the listing pipeline.
//!
//! Reused from s3rm-rs's `filters/` module with architectural adaptation:
//! - s3rm-rs uses channel-based pipeline stages (`ObjectFilterBase` with async `filter()`)
//! - s3ls-rs uses synchronous `FilterChain` with inline `matches()` calls
//!
//! Each filter implements `ObjectFilter::matches(&self, entry: &ListEntry) -> bool`.
//! `FilterChain` applies AND logic across all filters. `CommonPrefix` entries
//! always pass through all filters.

use crate::config::FilterConfig;
use crate::types::ListEntry;

pub mod exclude_regex;
pub mod include_regex;
pub mod larger_size;
pub mod mtime_after;
pub mod mtime_before;
pub mod smaller_size;
pub mod storage_class;

pub trait ObjectFilter: Send + Sync {
    fn matches(&self, entry: &ListEntry) -> anyhow::Result<bool>;
}

pub struct FilterChain {
    filters: Vec<Box<dyn ObjectFilter>>,
}

impl FilterChain {
    pub fn new(filters: Vec<Box<dyn ObjectFilter>>) -> Self {
        Self { filters }
    }

    pub fn matches(&self, entry: &ListEntry) -> anyhow::Result<bool> {
        // CommonPrefix entries always pass through
        if matches!(entry, ListEntry::CommonPrefix(_)) {
            return Ok(true);
        }
        for filter in &self.filters {
            if !filter.matches(entry)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }
}

pub fn build_filter_chain(filter_config: &FilterConfig) -> Result<FilterChain, String> {
    let mut filters: Vec<Box<dyn ObjectFilter>> = Vec::new();

    if let Some(ref regex) = filter_config.include_regex {
        filters.push(Box::new(include_regex::IncludeRegexFilter::from_regex(
            regex.clone(),
        )));
    }
    if let Some(ref regex) = filter_config.exclude_regex {
        filters.push(Box::new(exclude_regex::ExcludeRegexFilter::from_regex(
            regex.clone(),
        )));
    }
    if let Some(before) = filter_config.mtime_before {
        filters.push(Box::new(mtime_before::MtimeBeforeFilter::new(before)));
    }
    if let Some(after) = filter_config.mtime_after {
        filters.push(Box::new(mtime_after::MtimeAfterFilter::new(after)));
    }
    if let Some(size) = filter_config.smaller_size {
        filters.push(Box::new(smaller_size::SmallerSizeFilter::new(size)));
    }
    if let Some(size) = filter_config.larger_size {
        filters.push(Box::new(larger_size::LargerSizeFilter::new(size)));
    }
    if let Some(ref classes) = filter_config.storage_class {
        filters.push(Box::new(storage_class::StorageClassFilter::new(
            classes.clone(),
        )));
    }

    Ok(FilterChain::new(filters))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};

    #[test]
    fn empty_filter_chain_passes_all() {
        let chain = FilterChain::new(vec![]);
        let entry = ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(),
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
        });
        assert!(chain.matches(&entry).unwrap());
    }

    #[test]
    fn common_prefix_always_passes() {
        let chain = FilterChain::new(vec![Box::new(RejectAllFilter)]);
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        assert!(chain.matches(&entry).unwrap());
    }

    #[test]
    fn filter_error_propagates_through_chain() {
        let chain = FilterChain::new(vec![Box::new(ErrorFilter)]);
        let entry = ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(),
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
        });
        assert!(chain.matches(&entry).is_err());
    }

    struct RejectAllFilter;
    impl ObjectFilter for RejectAllFilter {
        fn matches(&self, _entry: &ListEntry) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    struct ErrorFilter;
    impl ObjectFilter for ErrorFilter {
        fn matches(&self, _entry: &ListEntry) -> anyhow::Result<bool> {
            Err(anyhow::anyhow!("simulated filter error"))
        }
    }
}
