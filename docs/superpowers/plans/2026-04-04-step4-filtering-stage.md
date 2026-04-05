# Step 4: Filtering Stage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the filter system (ObjectFilter trait, FilterChain, 7 individual filters) and inline them into the lister so only matching entries are sent to the channel.

**Architecture:** Filters are applied inline within the lister as synchronous function calls (no separate channel stages). `FilterChain` holds a `Vec<Box<dyn ObjectFilter>>` and applies AND logic. `CommonPrefix` entries always pass through all filters. Each filter implements `ObjectFilter::matches(&self, entry: &ListEntry) -> bool`.

**Tech Stack:** Rust 2024, fancy-regex, chrono

**Depends on:** Steps 1-3 (CLIArgs, Config with nested types, types, lister)

**Reference:** s3rm-rs `src/filters/`

---

### Task 1: Create ObjectFilter trait and FilterChain

**Files:**
- Create: `src/filters/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Write the failing test for FilterChain**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};

    #[test]
    fn empty_filter_chain_passes_all() {
        let chain = FilterChain::new(vec![]);
        let entry = ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(), size: 100,
            last_modified: chrono::Utc::now(), e_tag: "\"e\"".to_string(),
            storage_class: None, checksum_algorithm: None, checksum_type: None,
        });
        assert!(chain.matches(&entry));
    }

    #[test]
    fn common_prefix_always_passes() {
        // Even with a filter that rejects everything
        let chain = FilterChain::new(vec![
            Box::new(RejectAllFilter),
        ]);
        let entry = ListEntry::CommonPrefix("logs/".to_string());
        assert!(chain.matches(&entry));
    }
}

struct RejectAllFilter;
impl ObjectFilter for RejectAllFilter {
    fn matches(&self, _entry: &ListEntry) -> bool { false }
}
```

- [ ] **Step 2: Implement ObjectFilter and FilterChain**

```rust
use crate::types::ListEntry;

pub mod include_regex;
pub mod exclude_regex;
pub mod mtime_before;
pub mod mtime_after;
pub mod smaller_size;
pub mod larger_size;
pub mod storage_class;

pub trait ObjectFilter: Send + Sync {
    fn matches(&self, entry: &ListEntry) -> bool;
}

pub struct FilterChain {
    filters: Vec<Box<dyn ObjectFilter>>,
}

impl FilterChain {
    pub fn new(filters: Vec<Box<dyn ObjectFilter>>) -> Self {
        Self { filters }
    }

    pub fn matches(&self, entry: &ListEntry) -> bool {
        // CommonPrefix entries always pass through
        if matches!(entry, ListEntry::CommonPrefix(_)) {
            return true;
        }
        self.filters.iter().all(|f| f.matches(entry))
    }

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }
}
```

- [ ] **Step 3: Run test, verify pass**

Run: `cargo test --lib filters 2>&1 | tail -10`

- [ ] **Step 4: Commit**

```bash
git add src/filters/
git commit -m "feat(step4): add ObjectFilter trait and FilterChain with CommonPrefix passthrough"
```

---

### Task 2: Implement IncludeRegexFilter

**Files:**
- Create: `src/filters/include_regex.rs`

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};
    use crate::filters::ObjectFilter;

    fn make_entry(key: &str) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(), size: 100,
            last_modified: chrono::Utc::now(), e_tag: "\"e\"".to_string(),
            storage_class: None, checksum_algorithm: None, checksum_type: None,
        })
    }

    #[test]
    fn matches_matching_key() {
        let filter = IncludeRegexFilter::new(r".*\.log$").unwrap();
        assert!(filter.matches(&make_entry("app.log")));
        assert!(!filter.matches(&make_entry("app.txt")));
    }

    #[test]
    fn passes_delete_marker_by_key() {
        let filter = IncludeRegexFilter::new(r".*\.log$").unwrap();
        let entry = ListEntry::DeleteMarker {
            key: "app.log".to_string(),
            version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(),
            is_latest: true,
        };
        assert!(filter.matches(&entry));
    }
}
```

- [ ] **Step 2: Implement**

```rust
use crate::filters::ObjectFilter;
use crate::types::ListEntry;
use fancy_regex::Regex;

pub struct IncludeRegexFilter {
    regex: Regex,
}

impl IncludeRegexFilter {
    pub fn new(pattern: &str) -> Result<Self, String> {
        let regex = Regex::new(pattern).map_err(|e| e.to_string())?;
        Ok(Self { regex })
    }
}

impl ObjectFilter for IncludeRegexFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        self.regex.is_match(entry.key()).unwrap_or(false)
    }
}
```

- [ ] **Step 3: Run tests, commit**

---

### Task 3: Implement ExcludeRegexFilter

**Files:**
- Create: `src/filters/exclude_regex.rs`

Same pattern as IncludeRegexFilter but inverted: `!self.regex.is_match(entry.key()).unwrap_or(true)`

---

### Task 4: Implement MtimeBeforeFilter and MtimeAfterFilter

**Files:**
- Create: `src/filters/mtime_before.rs`
- Create: `src/filters/mtime_after.rs`

```rust
// mtime_before.rs
pub struct MtimeBeforeFilter {
    before: DateTime<Utc>,
}

impl ObjectFilter for MtimeBeforeFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        match entry.last_modified() {
            Some(lm) => *lm < self.before,
            None => true, // CommonPrefix has no timestamp
        }
    }
}
```

```rust
// mtime_after.rs
pub struct MtimeAfterFilter {
    after: DateTime<Utc>,
}

impl ObjectFilter for MtimeAfterFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        match entry.last_modified() {
            Some(lm) => *lm >= self.after,
            None => true,
        }
    }
}
```

---

### Task 5: Implement SmallerSizeFilter and LargerSizeFilter

**Files:**
- Create: `src/filters/smaller_size.rs`
- Create: `src/filters/larger_size.rs`

```rust
// smaller_size.rs
pub struct SmallerSizeFilter { threshold: u64 }

impl ObjectFilter for SmallerSizeFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        if matches!(entry, ListEntry::DeleteMarker { .. }) {
            return true;
        }
        entry.size() < self.threshold
    }
}
```

```rust
// larger_size.rs
pub struct LargerSizeFilter { threshold: u64 }

impl ObjectFilter for LargerSizeFilter {
    fn matches(&self, entry: &ListEntry) -> bool {
        if matches!(entry, ListEntry::DeleteMarker { .. }) {
            return true;
        }
        entry.size() >= self.threshold
    }
}
```

---

### Task 6: Implement StorageClassFilter

**Files:**
- Create: `src/filters/storage_class.rs`

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, S3Object};
    use crate::filters::ObjectFilter;

    fn make_entry_with_class(class: Option<&str>) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: "test.txt".to_string(), size: 100,
            last_modified: chrono::Utc::now(), e_tag: "\"e\"".to_string(),
            storage_class: class.map(|s| s.to_string()),
            checksum_algorithm: None, checksum_type: None,
        })
    }

    #[test]
    fn matches_listed_class() {
        let filter = StorageClassFilter::new(vec!["STANDARD".to_string(), "GLACIER".to_string()]);
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
            key: "test.txt".to_string(), version_id: "v1".to_string(),
            last_modified: chrono::Utc::now(), is_latest: true,
        };
        // Delete markers have no storage class, should pass through
        assert!(filter.matches(&entry));
    }
}
```

- [ ] **Step 2: Implement**

```rust
use crate::filters::ObjectFilter;
use crate::types::ListEntry;

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
            ListEntry::Object(obj) => {
                match obj.storage_class() {
                    Some(sc) => self.classes.iter().any(|c| c == sc),
                    None => false,
                }
            }
            ListEntry::DeleteMarker { .. } => true,
            ListEntry::CommonPrefix(_) => true, // handled by FilterChain
        }
    }
}
```

---

### Task 7: Build FilterChain from FilterConfig and wire into lister

**Files:**
- Modify: `src/filters/mod.rs` (add `build_filter_chain`)
- Modify: `src/lister.rs` (apply filter before sending)
- Modify: `src/pipeline.rs` (build chain, pass to lister)

- [ ] **Step 1: Add `build_filter_chain` function**

Takes `&FilterConfig` (the nested struct), NOT `&Config`:

```rust
use crate::config::FilterConfig;

pub fn build_filter_chain(filter_config: &FilterConfig) -> Result<FilterChain, String> {
    let mut filters: Vec<Box<dyn ObjectFilter>> = Vec::new();

    if let Some(ref pattern) = filter_config.include_regex {
        filters.push(Box::new(include_regex::IncludeRegexFilter::new(pattern)?));
    }
    if let Some(ref pattern) = filter_config.exclude_regex {
        filters.push(Box::new(exclude_regex::ExcludeRegexFilter::new(pattern)?));
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
        filters.push(Box::new(storage_class::StorageClassFilter::new(classes.clone())));
    }

    Ok(FilterChain::new(filters))
}
```

- [ ] **Step 2: Update lister to accept FilterChain**

Add `filter_chain: Arc<FilterChain>` field to `ObjectLister`. Before sending each entry, check `filter_chain.matches(&entry)`.

- [ ] **Step 3: Update pipeline to build filter chain from config**

In `ListingPipeline::run()`:
```rust
let filter_chain = build_filter_chain(&self.config.filter_config)
    .map_err(|e| anyhow::anyhow!(e))?;
```

- [ ] **Step 4: Run all tests**

Run: `cargo test 2>&1 | tail -15`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add src/filters/ src/lister.rs src/pipeline.rs
git commit -m "feat(step4): implement 7 filters, FilterChain, and inline filtering in lister, step 4 complete"
```
