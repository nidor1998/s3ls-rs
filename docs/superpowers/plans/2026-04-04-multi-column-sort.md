# Multi-Column Sort Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow `--sort date,key` (up to 2 comma-separated fields) with no automatic tie-breakers.

**Architecture:** Custom value parser splits comma-separated string into `Vec<SortField>` (max 2, no duplicates). `sort_entries` chains comparisons in field order. The `all_versions` tie-breaker logic is removed entirely.

**Tech Stack:** Rust, clap 4.6, existing `SortField` enum

---

### Task 1: Add custom value parser and update CLIArgs

**Files:**
- Modify: `src/config/args/mod.rs`

- [ ] **Step 1: Add the `parse_sort_fields` function**

Add this function after the existing `parse_human_bytes` function (after line 36):

```rust
fn parse_sort_fields(s: &str) -> Result<Vec<SortField>, String> {
    let tokens: Vec<&str> = s.split(',').collect();
    if tokens.len() > 2 {
        return Err("at most 2 sort fields allowed".to_string());
    }
    let mut fields = Vec::with_capacity(tokens.len());
    for token in &tokens {
        let field = match token.trim().to_lowercase().as_str() {
            "key" => SortField::Key,
            "size" => SortField::Size,
            "date" => SortField::Date,
            other => {
                return Err(format!(
                    "invalid sort field '{other}'; expected one of: key, size, date"
                ));
            }
        };
        if fields.contains(&field) {
            return Err(format!("duplicate sort field '{}'", token.trim().to_lowercase()));
        }
        fields.push(field);
    }
    Ok(fields)
}
```

- [ ] **Step 2: Change `CLIArgs.sort` from `SortField` to `Vec<SortField>`**

Replace the existing `--sort` arg definition (lines 150-152):

```rust
    /// Sort results by field(s): key, size, date (comma-separated, max 2)
    #[arg(long, default_value = "key", value_parser = parse_sort_fields, help_heading = "Sort")]
    pub sort: Vec<SortField>,
```

- [ ] **Step 3: Update `TryFrom<CLIArgs> for Config` to pass `sort` as Vec**

In the `TryFrom` impl (line 456), change:

```rust
            sort: args.sort,
```

This line stays the same — no code change needed since both sides are now `Vec<SortField>`.

- [ ] **Step 4: Verify it compiles (expect test failures, that's OK)**

Run: `cargo check 2>&1 | head -30`

Expected: Compilation errors in `config/mod.rs` (type mismatch on `sort` field) and `aggregate.rs` (signature mismatch). These are fixed in Tasks 2 and 3.

---

### Task 2: Update Config struct

**Files:**
- Modify: `src/config/mod.rs`

- [ ] **Step 1: Change `Config.sort` from `SortField` to `Vec<SortField>`**

Replace line 22:

```rust
    pub sort: Vec<SortField>,
```

- [ ] **Step 2: Update `Default` impl**

Replace line 72:

```rust
            sort: vec![SortField::Key],
```

- [ ] **Step 3: Verify config module compiles**

Run: `cargo check 2>&1 | head -30`

Expected: Still errors in `aggregate.rs` and `pipeline.rs` (fixed in Tasks 3-4). No errors in `config/`.

---

### Task 3: Update sort_entries in aggregate.rs

**Files:**
- Modify: `src/aggregate.rs`

- [ ] **Step 1: Change `sort_entries` signature and implementation**

Replace the entire `sort_entries` function (lines 126-166):

```rust
pub fn sort_entries(
    entries: &mut [ListEntry],
    fields: &[SortField],
    reverse: bool,
) {
    entries.sort_by(|a, b| {
        let mut cmp = std::cmp::Ordering::Equal;
        for field in fields {
            cmp = cmp.then_with(|| match field {
                SortField::Key => a.key().cmp(b.key()),
                SortField::Size => a.size().cmp(&b.size()),
                SortField::Date => cmp_mtime(a, b),
            });
        }
        if reverse { cmp.reverse() } else { cmp }
    });
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | head -30`

Expected: Errors only in `pipeline.rs` (extra `all_versions` arg) and test code. Fixed in Tasks 4-5.

---

### Task 4: Update pipeline call site

**Files:**
- Modify: `src/pipeline.rs`

- [ ] **Step 1: Update the `sort_entries` call**

Replace lines 122-127:

```rust
        sort_entries(
            &mut entries,
            &self.config.sort,
            self.config.reverse,
        );
```

- [ ] **Step 2: Verify the whole project compiles (ignoring test failures)**

Run: `cargo check 2>&1 | head -30`

Expected: No errors in production code. Test compilation errors remain (fixed in Task 5).

---

### Task 5: Update all tests

**Files:**
- Modify: `src/config/args/tests.rs`
- Modify: `src/aggregate.rs` (test module)

- [ ] **Step 1: Update existing sort tests in `args/tests.rs`**

Replace the sort test section (lines 157-192):

```rust
#[test]
fn sort_single_key() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "key"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Key]);
}

#[test]
fn sort_single_size() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "size"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Size]);
}

#[test]
fn sort_single_date() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "date"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Date]);
}

#[test]
fn sort_two_fields_date_key() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "date,key"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Date, SortField::Key]);
}

#[test]
fn sort_two_fields_size_date() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "size,date"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Size, SortField::Date]);
}

#[test]
fn sort_rejects_three_fields() {
    let result = parse_from_args(args(&["s3://bucket", "--sort", "key,size,date"]));
    assert!(result.is_err());
}

#[test]
fn sort_rejects_duplicate_fields() {
    let result = parse_from_args(args(&["s3://bucket", "--sort", "date,date"]));
    assert!(result.is_err());
}

#[test]
fn sort_invalid_value() {
    let result = parse_from_args(args(&["s3://bucket", "--sort", "name"]));
    assert!(result.is_err());
}

#[test]
fn sort_case_insensitive() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "Date,KEY"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Date, SortField::Key]);
}

#[test]
fn reverse_flag() {
    let cli = parse_from_args(args(&["s3://bucket", "--reverse"])).unwrap();
    assert!(cli.reverse);
}

#[test]
fn sort_and_reverse_combo() {
    let cli = parse_from_args(args(&["s3://bucket", "--sort", "size,key", "--reverse"])).unwrap();
    assert_eq!(cli.sort, vec![SortField::Size, SortField::Key]);
    assert!(cli.reverse);
}
```

- [ ] **Step 2: Update the defaults test in `args/tests.rs`**

Replace line 575:

```rust
    assert_eq!(cli.sort, vec![SortField::Key]);
```

- [ ] **Step 3: Update the full_combination test in `args/tests.rs`**

Replace line 755:

```rust
    assert_eq!(cli.sort, vec![SortField::Date]);
```

- [ ] **Step 4: Update the config_from_full_args test in `args/tests.rs`**

Replace line 831:

```rust
    assert_eq!(config.sort, vec![SortField::Date]);
```

- [ ] **Step 5: Update aggregate sort tests**

Replace the sort test block in `src/aggregate.rs` tests (lines 306-401):

```rust
    #[test]
    fn sort_by_key() {
        let mut entries = vec![
            make_entry("c.txt", 100, 2024, 1),
            make_entry("a.txt", 200, 2024, 2),
            make_entry("b.txt", 300, 2024, 3),
        ];
        sort_entries(&mut entries, &[SortField::Key], false);
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[1].key(), "b.txt");
        assert_eq!(entries[2].key(), "c.txt");
    }

    #[test]
    fn sort_by_size() {
        let mut entries = vec![
            make_entry("a.txt", 300, 2024, 1),
            make_entry("b.txt", 100, 2024, 2),
            make_entry("c.txt", 200, 2024, 3),
        ];
        sort_entries(&mut entries, &[SortField::Size], false);
        assert_eq!(entries[0].size(), 100);
        assert_eq!(entries[1].size(), 200);
        assert_eq!(entries[2].size(), 300);
    }

    #[test]
    fn sort_by_date() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("b.txt", 100, 2024, 1),
            make_entry("c.txt", 100, 2024, 2),
        ];
        sort_entries(&mut entries, &[SortField::Date], false);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "c.txt");
        assert_eq!(entries[2].key(), "a.txt");
    }

    #[test]
    fn sort_reverse() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            make_entry("b.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &[SortField::Key], true);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
    }

    #[test]
    fn sort_two_fields_date_then_key() {
        let mut entries = vec![
            make_entry("c.txt", 300, 2024, 1),
            make_entry("a.txt", 100, 2024, 1),
            make_entry("b.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &[SortField::Date, SortField::Key], false);
        // Same date (Jan): tiebreak by key → a < c
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[1].key(), "c.txt");
        // Feb entry last
        assert_eq!(entries[2].key(), "b.txt");
    }

    #[test]
    fn sort_two_fields_size_then_date() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("b.txt", 100, 2024, 1),
            make_entry("c.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &[SortField::Size, SortField::Date], false);
        // Same size (100): tiebreak by date → Jan < Mar
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
        // Larger size last
        assert_eq!(entries[2].key(), "c.txt");
    }

    #[test]
    fn sort_single_field_no_tiebreaker() {
        // Two entries with same key — order is stable but no secondary sort
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("a.txt", 200, 2024, 1),
        ];
        sort_entries(&mut entries, &[SortField::Key], false);
        // Both have same key, no tiebreaker — stable sort preserves input order
        assert_eq!(entries[0].size(), 100);
        assert_eq!(entries[1].size(), 200);
    }
```

- [ ] **Step 6: Update the `sort_common_prefix_by_size_sorts_as_zero` test**

Replace the test (lines 498-507):

```rust
    #[test]
    fn sort_common_prefix_by_size_sorts_as_zero() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            ListEntry::CommonPrefix("logs/".to_string()),
        ];
        sort_entries(&mut entries, &[SortField::Size], false);
        assert_eq!(entries[0].key(), "logs/");
        assert_eq!(entries[1].key(), "a.txt");
    }
```

- [ ] **Step 7: Run all tests**

Run: `cargo test 2>&1`

Expected: All tests pass.

- [ ] **Step 8: Run clippy**

Run: `cargo clippy -- -D warnings 2>&1`

Expected: No warnings.

---

### Task 6: Commit

- [ ] **Step 1: Commit all changes**

```bash
git add src/config/args/mod.rs src/config/mod.rs src/aggregate.rs src/pipeline.rs src/config/args/tests.rs
git commit -m "feat: support multi-column sort (--sort date,key)"
```
