# Step 1: Argument Design (Clap) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define all CLI arguments for s3ls using Clap, adapted from s3rm-rs, with unit tests for every flag combination.

**Architecture:** Copy s3rm-rs's `config/args/` module, strip deletion-specific args (dry_run, force, batch_size, worker_size, keep_latest_only, if_match, Lua, content-type/metadata/tag filters), add s3ls-specific args (recursive, all_versions, sort, reverse, summary, human, show-fullpath, show-etag, show-storage-class, show-checksum-algorithm, show-checksum-type, json, storage-class filter). Reuse value parsers (regex, human_bytes, url) verbatim.

**Tech Stack:** Rust 2024 edition, clap 4.6 (derive), clap-verbosity-flag 3.0, clap_complete 4.6, fancy-regex, byte-unit, chrono, url

**Reference:** s3rm-rs source at https://github.com/nidor1998/s3rm-rs (commit 68f45467)

**Status:** COMPLETED

---

### Task 1: Update Cargo.toml with Step 1 dependencies

**Files:**
- Modify: `Cargo.toml`

- [x] **Step 1: Add dependencies needed for argument parsing**

```toml
[package]
name = "s3ls-rs"
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"
description = "Fast S3 object listing tool"
repository = "https://github.com/nidor1998/s3ls-rs"

[dependencies]
# Error handling
anyhow = "1.0.102"
thiserror = "2.0.18"

# CLI
clap = { version = "4.6.0", features = ["derive", "env", "cargo", "string"] }
clap_complete = "4.6.0"
clap-verbosity-flag = "3.0.4"

# Date/time
chrono = "0.4.44"

# Regex
fancy-regex = "0.17.0"

# Byte unit parsing
byte-unit = "5.2.0"

# URL validation
url = "2.5.8"

# Logging
log = "0.4.29"

[dev-dependencies]
proptest = "1.11"

[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = ['cfg(e2e_test)'] }
```

- [x] **Step 2: Verify dependencies resolve**

- [x] **Step 3: Commit**

---

### Task 2: Create value_parser modules (reused verbatim from s3rm-rs)

**Files:**
- Create: `src/config/args/value_parser/mod.rs`
- Create: `src/config/args/value_parser/regex.rs`
- Create: `src/config/args/value_parser/human_bytes.rs`
- Create: `src/config/args/value_parser/url.rs`

- [x] **Step 1: Create directory structure**

- [x] **Step 2: Write `src/config/args/value_parser/mod.rs`**

```rust
pub mod human_bytes;
pub mod regex;
pub mod url;
```

- [x] **Step 3: Write `src/config/args/value_parser/regex.rs`**

Copied verbatim from s3rm-rs:

```rust
use fancy_regex::Regex;

const INVALID_REGEX: &str = "Invalid regular expression";

pub fn parse_regex(regex: &str) -> Result<String, String> {
    if Regex::new(regex).is_err() {
        return Err(INVALID_REGEX.to_string());
    }

    Ok(regex.to_string())
}
```

- [x] **Step 4: Write `src/config/args/value_parser/human_bytes.rs`**

Copied verbatim from s3rm-rs (with tests).

- [x] **Step 5: Write `src/config/args/value_parser/url.rs`**

Copied verbatim from s3rm-rs (with tests).

- [x] **Step 6: Verify value parser modules compile**

- [x] **Step 7: Commit**

---

### Task 3: Create CLIArgs struct with all s3ls flags

**Files:**
- Create: `src/config/args/mod.rs`
- Create: `src/config/mod.rs`
- Modify: `src/lib.rs`

- [x] **Step 1: Write `src/config/args/mod.rs`**

This is the core CLIArgs definition adapted from s3rm-rs. Key differences from the original plan:

1. **`sort` defaults to `SortField::Key`** (not `Option<SortField>`):
   ```rust
   #[arg(long, value_enum, default_value_t = SortField::Key, help_heading = "Sort")]
   pub sort: SortField,
   ```

2. **`max_keys` range is `1..=1000`** (not `1..=32767`):
   ```rust
   #[arg(long, env, default_value_t = DEFAULT_MAX_KEYS, value_parser = clap::value_parser!(i32).range(1..=1000), help_heading = "Advanced")]
   pub max_keys: i32,
   ```

3. **`storage_class` has `long_help`**:
   ```rust
   #[arg(
       long, env, value_delimiter = ',', help_heading = "Filtering",
       long_help = "List only objects whose storage class is in the given list.\nMultiple classes can be separated by commas.\n\nExample: --storage-class STANDARD,GLACIER,DEEP_ARCHIVE"
   )]
   pub storage_class: Option<Vec<String>>,
   ```

4. **`all_versions` uses `env = "LIST_ALL_VERSIONS"`** (not default `env`):
   ```rust
   #[arg(long, env = "LIST_ALL_VERSIONS", default_value_t = false, help_heading = "General")]
   pub all_versions: bool,
   ```

5. **Boolean defaults use literal `false`** instead of named constants.

6. **`SortField` derives `PartialEq`** and has a `Display` impl.

Full actual struct: see `src/config/args/mod.rs`.

- [x] **Step 2: Write `src/config/mod.rs`**

Minimal module declaration (Config struct comes in Step 2):

```rust
pub mod args;
```

- [x] **Step 3: Update `src/lib.rs` to wire up the config module**

- [x] **Step 4: Verify compilation**

- [x] **Step 5: Verify `--help` output**

- [x] **Step 6: Commit**

---

### Task 4: Create an empty tests module placeholder

**Files:**
- Create: `src/config/args/tests.rs`

- [x] **Step 1: Write the failing test for minimal arg parsing**
- [x] **Step 2: Run the test to verify it passes**
- [x] **Step 3: Commit**

---

### Task 5: Add comprehensive argument parsing tests - General & Filtering

**Files:**
- Modify: `src/config/args/tests.rs`

- [x] **Step 1: Add general and filtering argument tests**
- [x] **Step 2: Run the tests**
- [x] **Step 3: Commit**

---

### Task 6: Add Sort, Display, and Output option tests

**Files:**
- Modify: `src/config/args/tests.rs`

Note: In the actual implementation, sort tests use `SortField::Key` (not `Some(SortField::Key)`) since sort is no longer optional. The `parse_defaults` test checks `cli.sort == SortField::Key` instead of `cli.sort.is_none()`.

- [x] **Step 1: Add sort, display, and output tests**
- [x] **Step 2: Run the tests**
- [x] **Step 3: Commit**

---

### Task 7: Add AWS, Performance, Retry, Timeout, and Advanced option tests

**Files:**
- Modify: `src/config/args/tests.rs`

Note: `parse_rejects_max_keys_too_large` uses 1001 (not 32768) since max_keys range is `1..=1000`.

- [x] **Step 1: Add remaining option tests**
- [x] **Step 2: Run all tests**
- [x] **Step 3: Commit**

---

### Task 8: Add property-based tests

**Files:**
- Modify: `src/config/args/tests.rs`

- [x] **Step 1: Add proptest tests for input validation**
- [x] **Step 2: Run all tests**
- [x] **Step 3: Commit**

---

### Task 9: Final verification

**Files:** (none modified)

- [x] **Step 1: Run full test suite**
- [x] **Step 2: Run clippy**
- [x] **Step 3: Verify `--help` works via test**
- [x] **Step 4: Commit**
