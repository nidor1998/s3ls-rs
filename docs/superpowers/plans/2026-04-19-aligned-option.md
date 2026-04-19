# `--aligned` Option Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--aligned` CLI flag that emits the default TSV output with whitespace-padded, fixed-width columns for human-readable terminal viewing, independently of `--human-readable`.

**Architecture:** Every non-KEY column has a bounded max width derived from the S3 API or known enums, and KEY is always rightmost. A new `src/display/aligned.rs` module exposes the width constants, a tiny `Align` + `ColumnSpec` model, and a `render_cols` helper. `TsvFormatter` (object listing) and `bucket_lister` (bucket listing) branch on `FormatOptions.aligned` to pick the tab-joined path or the padded-space path. No buffering is introduced; streaming works unchanged with `--no-sort`.

**Tech Stack:** Rust 2024 edition, `clap` with derive macros for CLI, `tokio` async runtime, existing `EntryFormatter` trait. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-04-19-aligned-option-design.md`.

**Pre-flight checklist (every commit):**

- `cargo fmt` passes
- `cargo clippy --all-features` passes
- Use SSH signature (`git commit -S -m ...`)

---

## Task 1: New `src/display/aligned.rs` module

**Files:**
- Create: `src/display/aligned.rs`
- Modify: `src/display/mod.rs` (add `pub mod aligned;`)

- [ ] **Step 1.1: Write the failing tests**

Create `src/display/aligned.rs` with only the `#[cfg(test)]` block present (no implementation yet) so the tests fail at compile time:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pad_left_shorter_pads_with_trailing_spaces() {
        assert_eq!(pad("abc", 6, Align::Left), "abc   ");
    }

    #[test]
    fn pad_right_shorter_pads_with_leading_spaces() {
        assert_eq!(pad("12", 5, Align::Right), "   12");
    }

    #[test]
    fn pad_exact_length_unchanged() {
        assert_eq!(pad("hello", 5, Align::Left), "hello");
        assert_eq!(pad("hello", 5, Align::Right), "hello");
    }

    #[test]
    fn pad_longer_than_width_returned_as_is() {
        assert_eq!(pad("overflow", 3, Align::Left), "overflow");
        assert_eq!(pad("overflow", 3, Align::Right), "overflow");
    }

    #[test]
    fn pad_counts_chars_not_bytes() {
        // "日本" is 2 chars, 6 bytes in UTF-8.
        assert_eq!(pad("日本", 4, Align::Left), "日本  ");
    }

    #[test]
    fn render_cols_joins_with_two_spaces_and_appends_key_unpadded() {
        let cols = vec![
            ColumnSpec { value: "2024-01-01T00:00:00Z".to_string(), width: 25, align: Align::Left },
            ColumnSpec { value: "1234".to_string(), width: 20, align: Align::Right },
        ];
        let out = render_cols(&cols, "myobj.txt");
        // DATE padded left → 25 chars, SEP, SIZE padded right → 20 chars, SEP, key unpadded.
        assert_eq!(
            out,
            "2024-01-01T00:00:00Z       \
             \u{20}\u{20}                1234\u{20}\u{20}myobj.txt"
        );
    }

    #[test]
    fn render_cols_empty_columns_just_emits_key() {
        let cols: Vec<ColumnSpec> = Vec::new();
        assert_eq!(render_cols(&cols, "k"), "k");
    }
}
```

Note: the long assertion in `render_cols_joins_with_two_spaces_and_appends_key_unpadded` is deliberately explicit. Width 25 means "2024-01-01T00:00:00Z" (20 chars) + 5 trailing spaces. Then `"  "` separator. Then "1234" (4 chars) right-aligned in width 20 → 16 leading spaces + "1234". Then `"  "` separator. Then "myobj.txt".

- [ ] **Step 1.2: Run tests to verify they fail**

```
cargo test --lib display::aligned:: 2>&1 | tail -40
```

Expected: compile errors (`pad`, `Align`, `ColumnSpec`, `render_cols` undefined).

- [ ] **Step 1.3: Implement the module**

Replace `src/display/aligned.rs` with:

```rust
//! Fixed-width column layout used when the user passes `--aligned`.
//!
//! Every non-KEY column in s3ls output has a bounded maximum width
//! derived from the S3 API contract or a known enum, and KEY is always
//! rightmost. This module centralizes those widths and exposes the
//! small helpers used by `display::tsv::TsvFormatter` and
//! `bucket_lister` to emit padded, space-separated rows without
//! buffering.

// ---- Object listing column widths -----------------------------------

pub const W_DATE: usize = 25;
pub const W_SIZE: usize = 20;
pub const W_SIZE_HUMAN: usize = 9;
pub const W_STORAGE_CLASS: usize = 19;
pub const W_ETAG: usize = 35;
pub const W_CHECKSUM_ALGORITHM: usize = 34;
pub const W_CHECKSUM_TYPE: usize = 11;
pub const W_VERSION_ID: usize = 32;
pub const W_IS_LATEST: usize = 10;
pub const W_OWNER_DISPLAY_NAME: usize = 64;
pub const W_OWNER_ID: usize = 64;
pub const W_IS_RESTORE_IN_PROGRESS: usize = 5;
pub const W_RESTORE_EXPIRY_DATE: usize = 25;

// ---- Bucket listing column widths -----------------------------------

pub const W_BUCKET_REGION: usize = 20;
pub const W_BUCKET_NAME: usize = 63;
pub const W_BUCKET_ARN: usize = 100;

// ---- Separator -------------------------------------------------------

pub const SEP: &str = "  ";

// ---- Alignment model -------------------------------------------------

#[derive(Clone, Copy)]
pub enum Align {
    Left,
    Right,
}

pub struct ColumnSpec {
    pub value: String,
    pub width: usize,
    pub align: Align,
}

/// Pad `value` to `width` visible characters. Counts `chars` (not
/// bytes), so multi-byte UTF-8 sequences are counted as 1 char each.
/// Values longer than `width` are returned unchanged — no truncation.
pub fn pad(value: &str, width: usize, align: Align) -> String {
    let len = value.chars().count();
    if len >= width {
        return value.to_string();
    }
    let padding = width - len;
    match align {
        Align::Left => {
            let mut s = String::with_capacity(value.len() + padding);
            s.push_str(value);
            for _ in 0..padding {
                s.push(' ');
            }
            s
        }
        Align::Right => {
            let mut s = String::with_capacity(value.len() + padding);
            for _ in 0..padding {
                s.push(' ');
            }
            s.push_str(value);
            s
        }
    }
}

/// Render the given columns joined by `SEP`, then append `last_key`
/// with no trailing padding. If `cols` is empty, returns `last_key`.
pub fn render_cols(cols: &[ColumnSpec], last_key: &str) -> String {
    if cols.is_empty() {
        return last_key.to_string();
    }
    let mut out = String::new();
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            out.push_str(SEP);
        }
        out.push_str(&pad(&c.value, c.width, c.align));
    }
    out.push_str(SEP);
    out.push_str(last_key);
    out
}

#[cfg(test)]
mod tests {
    // (same test block as in Step 1.1)
    use super::*;

    #[test]
    fn pad_left_shorter_pads_with_trailing_spaces() {
        assert_eq!(pad("abc", 6, Align::Left), "abc   ");
    }

    #[test]
    fn pad_right_shorter_pads_with_leading_spaces() {
        assert_eq!(pad("12", 5, Align::Right), "   12");
    }

    #[test]
    fn pad_exact_length_unchanged() {
        assert_eq!(pad("hello", 5, Align::Left), "hello");
        assert_eq!(pad("hello", 5, Align::Right), "hello");
    }

    #[test]
    fn pad_longer_than_width_returned_as_is() {
        assert_eq!(pad("overflow", 3, Align::Left), "overflow");
        assert_eq!(pad("overflow", 3, Align::Right), "overflow");
    }

    #[test]
    fn pad_counts_chars_not_bytes() {
        assert_eq!(pad("日本", 4, Align::Left), "日本  ");
    }

    #[test]
    fn render_cols_joins_with_two_spaces_and_appends_key_unpadded() {
        let cols = vec![
            ColumnSpec {
                value: "2024-01-01T00:00:00Z".to_string(),
                width: 25,
                align: Align::Left,
            },
            ColumnSpec {
                value: "1234".to_string(),
                width: 20,
                align: Align::Right,
            },
        ];
        let out = render_cols(&cols, "myobj.txt");
        // "2024-01-01T00:00:00Z" is 20 chars -> pad Left to 25: add 5 spaces
        let expected = format!(
            "2024-01-01T00:00:00Z{}{}{}{}{}",
            " ".repeat(5),
            SEP,
            " ".repeat(16),
            "1234",
            format!("{SEP}myobj.txt"),
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn render_cols_empty_columns_just_emits_key() {
        let cols: Vec<ColumnSpec> = Vec::new();
        assert_eq!(render_cols(&cols, "k"), "k");
    }
}
```

Then register the module by adding this line near the top of `src/display/mod.rs` (next to `pub mod json;` and `pub mod tsv;`):

```rust
pub mod aligned;
```

- [ ] **Step 1.4: Run tests to verify they pass**

```
cargo test --lib display::aligned:: 2>&1 | tail -20
```

Expected: all 7 tests pass. Also run:

```
cargo fmt
cargo clippy --all-features 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 1.5: Commit**

```
git add src/display/aligned.rs src/display/mod.rs
git commit -S -m "feat(display): add aligned layout module with width constants and pad/render helpers"
```

---

## Task 2: CLI flag + config plumbing

**Files:**
- Modify: `src/config/args/mod.rs` (add `aligned` field, map to `DisplayConfig.aligned`)
- Modify: `src/config/mod.rs` (add `aligned: bool` to `DisplayConfig` + default-value test)
- Modify: `src/display/mod.rs` (add `aligned: bool` to `FormatOptions` + populate in `from_display_config`)
- Modify: `src/config/args/tests.rs` (add parse + conflict tests)

- [ ] **Step 2.1: Write the failing tests**

Append to `src/config/args/tests.rs`:

```rust
// ===========================================================================
// --aligned
// ===========================================================================

#[test]
fn aligned_default_false() {
    let cli = parse_from_args(args(&["s3://bucket"])).unwrap();
    assert!(!cli.aligned);
}

#[test]
fn aligned_long_flag() {
    let cli = parse_from_args(args(&["s3://bucket", "--aligned"])).unwrap();
    assert!(cli.aligned);
}

#[test]
fn aligned_conflicts_with_json() {
    let result = parse_from_args(args(&["s3://bucket", "--aligned", "--json"]));
    assert!(result.is_err());
}

#[test]
fn aligned_composes_with_no_sort() {
    let cli = parse_from_args(args(&[
        "s3://bucket",
        "--recursive",
        "--aligned",
        "--no-sort",
    ]))
    .unwrap();
    assert!(cli.aligned);
    assert!(cli.no_sort);
}

#[test]
fn aligned_composes_with_human_readable() {
    let cli = parse_from_args(args(&["s3://bucket", "--aligned", "--human-readable"])).unwrap();
    assert!(cli.aligned);
    assert!(cli.human);
}
```

Also append to the existing `config_default_values` test in `src/config/mod.rs` (inside the `#[cfg(test)] mod tests { ... }` block, just before the closing `}` of that function):

```rust
assert!(!config.display_config.aligned);
```

- [ ] **Step 2.2: Run tests to verify they fail**

```
cargo test 2>&1 | tail -20
```

Expected: multiple failures — `cli.aligned` and `config.display_config.aligned` do not exist yet.

- [ ] **Step 2.3: Add `aligned` to `Args`**

Open `src/config/args/mod.rs`. After the `raw_output` field (around line 363, inside the struct definition that has the `#[arg(long, env, ... help_heading = "Display")]` clauses), insert:

```rust
    /// Display output with columns aligned using whitespace padding.
    ///
    /// By default, s3ls emits tab-separated text (TSV). TSV is
    /// machine-friendly but columns don't line up visually in a
    /// terminal because tabs align to tab stops, not to content.
    /// `--aligned` pads each non-KEY column to a fixed width and
    /// uses two spaces as the column separator, producing output
    /// that's easy for a human to scan.
    ///
    /// Independent of `--human-readable`:
    ///   - `--human-readable` makes individual values human-friendly
    ///     (e.g., `1.2KiB` rather than raw bytes).
    ///   - `--aligned` makes the layout human-friendly (columns line
    ///     up on screen).
    /// The two can be combined.
    #[arg(
        long,
        env = "ALIGNED",
        default_value_t = false,
        conflicts_with = "json",
        help_heading = "Display"
    )]
    pub aligned: bool,
```

- [ ] **Step 2.4: Add `aligned` to `DisplayConfig`**

Open `src/config/mod.rs`. In the `DisplayConfig` struct (around lines 117-137), add the new field after `raw_output`:

```rust
    /// Display output with columns aligned using whitespace padding
    /// (see `Args::aligned`).
    pub aligned: bool,
```

(No change needed to `Default` — it's `#[derive(Default)]`, so `bool` defaults to `false`.)

- [ ] **Step 2.5: Add `aligned` to `FormatOptions` and wire up the mapping**

Open `src/display/mod.rs`. In `FormatOptions` (around lines 15-38), add after `show_local_time`:

```rust
    pub aligned: bool,
```

In `FormatOptions::from_display_config` (around lines 41-62), add inside the struct initializer:

```rust
            aligned: display_config.aligned,
```

Open `src/config/args/mod.rs`. In the `Args → Config` conversion (around lines 791-807, inside `display_config: crate::config::DisplayConfig { ... }`), add after `raw_output: args.raw_output,`:

```rust
                aligned: args.aligned,
```

- [ ] **Step 2.6: Run tests to verify they pass**

```
cargo test 2>&1 | tail -20
```

Expected: all previously failing tests now pass. No other test should regress.

```
cargo fmt
cargo clippy --all-features 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 2.7: Commit**

```
git add src/config/args/mod.rs src/config/args/tests.rs src/config/mod.rs src/display/mod.rs
git commit -S -m "feat(config): add --aligned CLI flag and plumb through DisplayConfig / FormatOptions"
```

---

## Task 3: Aligned branch in `TsvFormatter::format_entry`

**Files:**
- Modify: `src/display/tsv.rs`

- [ ] **Step 3.1: Write the failing tests**

Append to the `#[cfg(test)] mod tests { ... }` block in `src/display/tsv.rs`:

```rust
// ===========================================================================
// --aligned: object row layout
// ===========================================================================

#[test]
fn format_text_aligned_basic_object() {
    use crate::display::aligned::{SEP, W_DATE, W_SIZE};
    let entry = make_entry_dated("readme.txt", 1234, 2024, 1);
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    // Expect: <date padded L to 25>SEP<size padded R to 20>SEP<key>
    let date = "2024-01-01T00:00:00Z";
    let date_padded = format!("{date}{}", " ".repeat(W_DATE - date.chars().count()));
    let size_padded = format!("{}1234", " ".repeat(W_SIZE - 4));
    let expected = format!("{date_padded}{SEP}{size_padded}{SEP}readme.txt");
    assert_eq!(line, expected);
}

#[test]
fn format_text_aligned_right_aligns_size_number() {
    let entry = make_entry_dated("f.txt", 42, 2024, 1);
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    // The "42" should have many spaces to its left (right-aligned in a 20-wide column).
    // Locate "42" and check everything between the preceding separator and "42" is spaces.
    let size_end = line.find("42").unwrap() + 2;
    // Grab the SIZE column: it's between the first SEP after DATE and the SEP after SIZE.
    // Simpler assertion: the sequence "  42  " (size + trailing sep) appears.
    assert!(line.contains("                  42  "), "got: {line:?}");
}

#[test]
fn format_text_aligned_pre_marker_right_aligned() {
    let entry = crate::types::ListEntry::CommonPrefix("logs/".to_string());
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    // PRE is right-aligned in a 20-wide SIZE column: 17 spaces + "PRE".
    assert!(line.contains("                 PRE  "), "got: {line:?}");
    assert!(line.ends_with("logs/"));
}

#[test]
fn format_text_aligned_delete_marker_right_aligned() {
    let entry = crate::types::ListEntry::DeleteMarker {
        key: "k.txt".to_string(),
        version_info: crate::types::VersionInfo {
            version_id: "v1".to_string(),
            is_latest: false,
        },
        last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        owner_display_name: None,
        owner_id: None,
    };
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    // DELETE is right-aligned in a 20-wide SIZE column: 14 spaces + "DELETE".
    assert!(line.contains("              DELETE  "), "got: {line:?}");
    assert!(line.ends_with("k.txt"));
}

#[test]
fn format_text_aligned_overflow_preserves_value() {
    // An OwnerDisplayName longer than W_OWNER_DISPLAY_NAME (64) should not be truncated.
    let big_name = "a".repeat(80);
    let entry = crate::types::ListEntry::Object(crate::types::S3Object {
        key: "f.txt".to_string(),
        size: 1,
        last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        e_tag: "\"e\"".to_string(),
        storage_class: Some("STANDARD".to_string()),
        checksum_algorithm: vec![],
        checksum_type: None,
        owner_display_name: Some(big_name.clone()),
        owner_id: Some("z".to_string()),
        is_restore_in_progress: None,
        restore_expiry_date: None,
        version_info: None,
    });
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        show_owner: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    assert!(line.contains(&big_name), "got: {line:?}");
    assert!(line.ends_with("f.txt"));
}

#[test]
fn format_text_aligned_escapes_before_padding() {
    let entry = crate::types::ListEntry::Object(crate::types::S3Object {
        key: "evil\nkey".to_string(),
        size: 1,
        last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        e_tag: "\"e\"".to_string(),
        storage_class: Some("STANDARD".to_string()),
        checksum_algorithm: vec![],
        checksum_type: None,
        owner_display_name: None,
        owner_id: None,
        is_restore_in_progress: None,
        restore_expiry_date: None,
        version_info: None,
    });
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    assert!(!line.contains('\n'));
    assert!(line.ends_with("evil\\x0akey"));
}

#[test]
fn format_text_aligned_with_all_optional_columns() {
    use crate::display::aligned::{
        W_CHECKSUM_ALGORITHM, W_CHECKSUM_TYPE, W_DATE, W_ETAG, W_IS_LATEST,
        W_IS_RESTORE_IN_PROGRESS, W_OWNER_DISPLAY_NAME, W_OWNER_ID, W_RESTORE_EXPIRY_DATE,
        W_SIZE, W_STORAGE_CLASS, W_VERSION_ID,
    };
    let entry = crate::types::ListEntry::Object(crate::types::S3Object {
        key: "f.txt".to_string(),
        size: 10,
        last_modified: chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        e_tag: "\"abc\"".to_string(),
        storage_class: Some("STANDARD".to_string()),
        checksum_algorithm: vec!["CRC32".to_string()],
        checksum_type: Some("FULL_OBJECT".to_string()),
        owner_display_name: Some("alice".to_string()),
        owner_id: Some("id-a".to_string()),
        is_restore_in_progress: Some(true),
        restore_expiry_date: Some("2024-02-01T00:00:00Z".to_string()),
        version_info: Some(crate::types::VersionInfo {
            version_id: "v1".to_string(),
            is_latest: true,
        }),
    });
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        all_versions: true,
        show_storage_class: true,
        show_etag: true,
        show_checksum_algorithm: true,
        show_checksum_type: true,
        show_is_latest: true,
        show_owner: true,
        show_restore_status: true,
        ..Default::default()
    });
    let line = fmt.format_entry(&entry);
    // Verify each column is padded to its expected width by checking
    // the total length of everything before the key.
    let expected_prefix_len = W_DATE
        + 2
        + W_SIZE
        + 2
        + W_STORAGE_CLASS
        + 2
        + W_ETAG
        + 2
        + W_CHECKSUM_ALGORITHM
        + 2
        + W_CHECKSUM_TYPE
        + 2
        + W_VERSION_ID
        + 2
        + W_IS_LATEST
        + 2
        + W_OWNER_DISPLAY_NAME
        + 2
        + W_OWNER_ID
        + 2
        + W_IS_RESTORE_IN_PROGRESS
        + 2
        + W_RESTORE_EXPIRY_DATE
        + 2; // trailing SEP before key
    assert!(line.ends_with("f.txt"));
    assert_eq!(
        line.chars().count(),
        expected_prefix_len + "f.txt".chars().count()
    );
}
```

- [ ] **Step 3.2: Run tests to verify they fail**

```
cargo test --lib format_text_aligned 2>&1 | tail -40
```

Expected: all 7 new tests fail (aligned branch not implemented yet — `format_entry` ignores `opts.aligned`).

- [ ] **Step 3.3: Implement the aligned branch in `format_entry`**

Open `src/display/tsv.rs`. At the top of the file, extend the imports:

```rust
use crate::display::aligned::{
    Align, ColumnSpec, SEP, W_CHECKSUM_ALGORITHM, W_CHECKSUM_TYPE, W_DATE, W_ETAG,
    W_IS_LATEST, W_IS_RESTORE_IN_PROGRESS, W_OWNER_DISPLAY_NAME, W_OWNER_ID,
    W_RESTORE_EXPIRY_DATE, W_SIZE, W_SIZE_HUMAN, W_STORAGE_CLASS, W_VERSION_ID,
    render_cols,
};
```

Replace the entire body of `fn format_entry` with a helper-driven version. The essential change is: we build a `Vec<ColumnSpec>` with `(value, width, align)` per non-KEY column, then at the end choose between `cols.join("\t")` (TSV) and `render_cols(&specs, &key)` (aligned).

Since the current `format_entry` builds `cols: Vec<String>` in three match arms and then joins, the simplest change is to keep the existing `Vec<String>` code and in parallel build a `Vec<ColumnSpec>` that excludes the KEY column. Easier: keep a single list and attach metadata.

Concretely, rewrite `format_entry` as:

```rust
fn format_entry(&self, entry: &ListEntry) -> String {
    let opts = &self.opts;
    let mut specs: Vec<ColumnSpec> = Vec::new();
    let key_col: String;

    let size_width = if opts.human { W_SIZE_HUMAN } else { W_SIZE };

    match entry {
        ListEntry::CommonPrefix(_) => {
            // DATE (empty)
            specs.push(ColumnSpec {
                value: String::new(),
                width: W_DATE,
                align: Align::Left,
            });
            // SIZE = "PRE"
            specs.push(ColumnSpec {
                value: "PRE".to_string(),
                width: size_width,
                align: Align::Right,
            });
            if opts.show_storage_class {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_STORAGE_CLASS,
                    align: Align::Left,
                });
            }
            if opts.show_etag {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_ETAG,
                    align: Align::Left,
                });
            }
            if opts.show_checksum_algorithm {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_CHECKSUM_ALGORITHM,
                    align: Align::Left,
                });
            }
            if opts.show_checksum_type {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_CHECKSUM_TYPE,
                    align: Align::Left,
                });
            }
            if opts.all_versions {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_VERSION_ID,
                    align: Align::Left,
                });
                if opts.show_is_latest {
                    specs.push(ColumnSpec {
                        value: String::new(),
                        width: W_IS_LATEST,
                        align: Align::Left,
                    });
                }
            }
            if opts.show_owner {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_OWNER_DISPLAY_NAME,
                    align: Align::Left,
                });
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_OWNER_ID,
                    align: Align::Left,
                });
            }
            if opts.show_restore_status {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_IS_RESTORE_IN_PROGRESS,
                    align: Align::Left,
                });
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_RESTORE_EXPIRY_DATE,
                    align: Align::Left,
                });
            }
            key_col = maybe_escape(&format_key_display(entry.key(), opts), opts).into_owned();
        }
        ListEntry::Object(obj) => {
            specs.push(ColumnSpec {
                value: format_rfc3339(&obj.last_modified, opts.show_local_time),
                width: W_DATE,
                align: Align::Left,
            });
            specs.push(ColumnSpec {
                value: format_size(obj.size, opts.human),
                width: size_width,
                align: Align::Right,
            });
            if opts.show_storage_class {
                specs.push(ColumnSpec {
                    value: obj
                        .storage_class
                        .as_deref()
                        .unwrap_or("STANDARD")
                        .to_string(),
                    width: W_STORAGE_CLASS,
                    align: Align::Left,
                });
            }
            if opts.show_etag {
                specs.push(ColumnSpec {
                    value: obj.e_tag.trim_matches('"').to_string(),
                    width: W_ETAG,
                    align: Align::Left,
                });
            }
            if opts.show_checksum_algorithm {
                specs.push(ColumnSpec {
                    value: obj.checksum_algorithm.join(","),
                    width: W_CHECKSUM_ALGORITHM,
                    align: Align::Left,
                });
            }
            if opts.show_checksum_type {
                specs.push(ColumnSpec {
                    value: obj.checksum_type.as_deref().unwrap_or("").to_string(),
                    width: W_CHECKSUM_TYPE,
                    align: Align::Left,
                });
            }
            if let Some(vid) = obj.version_id() {
                specs.push(ColumnSpec {
                    value: vid.to_string(),
                    width: W_VERSION_ID,
                    align: Align::Left,
                });
            }
            if opts.show_is_latest && obj.version_id().is_some() {
                specs.push(ColumnSpec {
                    value: if obj.is_latest() {
                        "LATEST".to_string()
                    } else {
                        "NOT_LATEST".to_string()
                    },
                    width: W_IS_LATEST,
                    align: Align::Left,
                });
            }
            if opts.show_owner {
                specs.push(ColumnSpec {
                    value: maybe_escape(obj.owner_display_name.as_deref().unwrap_or(""), opts)
                        .into_owned(),
                    width: W_OWNER_DISPLAY_NAME,
                    align: Align::Left,
                });
                specs.push(ColumnSpec {
                    value: maybe_escape(obj.owner_id.as_deref().unwrap_or(""), opts).into_owned(),
                    width: W_OWNER_ID,
                    align: Align::Left,
                });
            }
            if opts.show_restore_status {
                specs.push(ColumnSpec {
                    value: obj
                        .is_restore_in_progress
                        .map(|b| b.to_string())
                        .unwrap_or_default(),
                    width: W_IS_RESTORE_IN_PROGRESS,
                    align: Align::Right,
                });
                specs.push(ColumnSpec {
                    value: obj.restore_expiry_date.as_deref().unwrap_or("").to_string(),
                    width: W_RESTORE_EXPIRY_DATE,
                    align: Align::Left,
                });
            }
            key_col = maybe_escape(&format_key_display(entry.key(), opts), opts).into_owned();
        }
        ListEntry::DeleteMarker {
            key,
            version_info,
            last_modified,
            owner_display_name,
            owner_id,
        } => {
            specs.push(ColumnSpec {
                value: format_rfc3339(last_modified, opts.show_local_time),
                width: W_DATE,
                align: Align::Left,
            });
            specs.push(ColumnSpec {
                value: "DELETE".to_string(),
                width: size_width,
                align: Align::Right,
            });
            if opts.show_storage_class {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_STORAGE_CLASS,
                    align: Align::Left,
                });
            }
            if opts.show_etag {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_ETAG,
                    align: Align::Left,
                });
            }
            if opts.show_checksum_algorithm {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_CHECKSUM_ALGORITHM,
                    align: Align::Left,
                });
            }
            if opts.show_checksum_type {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_CHECKSUM_TYPE,
                    align: Align::Left,
                });
            }
            specs.push(ColumnSpec {
                value: version_info.version_id.clone(),
                width: W_VERSION_ID,
                align: Align::Left,
            });
            if opts.show_is_latest {
                specs.push(ColumnSpec {
                    value: if version_info.is_latest {
                        "LATEST".to_string()
                    } else {
                        "NOT_LATEST".to_string()
                    },
                    width: W_IS_LATEST,
                    align: Align::Left,
                });
            }
            if opts.show_owner {
                specs.push(ColumnSpec {
                    value: maybe_escape(owner_display_name.as_deref().unwrap_or(""), opts)
                        .into_owned(),
                    width: W_OWNER_DISPLAY_NAME,
                    align: Align::Left,
                });
                specs.push(ColumnSpec {
                    value: maybe_escape(owner_id.as_deref().unwrap_or(""), opts).into_owned(),
                    width: W_OWNER_ID,
                    align: Align::Left,
                });
            }
            if opts.show_restore_status {
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_IS_RESTORE_IN_PROGRESS,
                    align: Align::Right,
                });
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_RESTORE_EXPIRY_DATE,
                    align: Align::Left,
                });
            }
            key_col = maybe_escape(&format_key_display(key, opts), opts).into_owned();
        }
    }

    if opts.aligned {
        render_cols(&specs, &key_col)
    } else {
        let mut parts: Vec<&str> = specs.iter().map(|c| c.value.as_str()).collect();
        parts.push(&key_col);
        parts.join("\t")
    }
}
```

Notes:
- Imports `Align`, `ColumnSpec`, `render_cols`, `SEP`, and every `W_*` constant from the new `aligned` module.
- Keeps the existing placeholder-column logic for `CommonPrefix` and `DeleteMarker` so column counts remain aligned with `Object` rows (preserves existing test `format_text_common_prefix_aligns_with_versioned_object`).
- Chooses `render_cols(&specs, &key_col)` when aligned, otherwise builds a tab-joined string from the same specs + key.

- [ ] **Step 3.4: Run the full test suite to verify both old and new pass**

```
cargo test 2>&1 | tail -30
```

Expected: all tests in `src/display/tsv.rs` pass — the original TSV tests (which never set `aligned`) should be unchanged, and the 7 new aligned tests pass.

```
cargo fmt
cargo clippy --all-features 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 3.5: Commit**

```
git add src/display/tsv.rs
git commit -S -m "feat(display): aligned branch for TsvFormatter::format_entry (object rows)"
```

---

## Task 4: Aligned branch in `TsvFormatter::format_header`

**Files:**
- Modify: `src/display/tsv.rs`

- [ ] **Step 4.1: Write the failing test**

Append to the `#[cfg(test)] mod tests { ... }` block in `src/display/tsv.rs`:

```rust
#[test]
fn format_text_aligned_header_padded() {
    use crate::display::aligned::{SEP, W_DATE, W_SIZE};
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let header = fmt.format_header().unwrap();
    let date_label = format!("DATE{}", " ".repeat(W_DATE - "DATE".len()));
    let size_label = format!("SIZE{}", " ".repeat(W_SIZE - "SIZE".len()));
    let expected = format!("{date_label}{SEP}{size_label}{SEP}KEY");
    assert_eq!(header, expected);
}
```

- [ ] **Step 4.2: Run the test to verify it fails**

```
cargo test --lib format_text_aligned_header_padded 2>&1 | tail -20
```

Expected: FAIL — header is still tab-joined regardless of `aligned`.

- [ ] **Step 4.3: Implement aligned header**

In `src/display/tsv.rs`, replace the body of `fn format_header` with:

```rust
fn format_header(&self) -> Option<String> {
    let opts = &self.opts;
    let size_width = if opts.human { W_SIZE_HUMAN } else { W_SIZE };

    // Collect header columns parallel to format_entry's column list.
    let mut specs: Vec<ColumnSpec> = Vec::new();
    specs.push(ColumnSpec {
        value: "DATE".to_string(),
        width: W_DATE,
        align: Align::Left,
    });
    specs.push(ColumnSpec {
        value: "SIZE".to_string(),
        width: size_width,
        align: Align::Left,
    });
    if opts.show_storage_class {
        specs.push(ColumnSpec {
            value: "STORAGE_CLASS".to_string(),
            width: W_STORAGE_CLASS,
            align: Align::Left,
        });
    }
    if opts.show_etag {
        specs.push(ColumnSpec {
            value: "ETAG".to_string(),
            width: W_ETAG,
            align: Align::Left,
        });
    }
    if opts.show_checksum_algorithm {
        specs.push(ColumnSpec {
            value: "CHECKSUM_ALGORITHM".to_string(),
            width: W_CHECKSUM_ALGORITHM,
            align: Align::Left,
        });
    }
    if opts.show_checksum_type {
        specs.push(ColumnSpec {
            value: "CHECKSUM_TYPE".to_string(),
            width: W_CHECKSUM_TYPE,
            align: Align::Left,
        });
    }
    if opts.all_versions {
        specs.push(ColumnSpec {
            value: "VERSION_ID".to_string(),
            width: W_VERSION_ID,
            align: Align::Left,
        });
    }
    if opts.show_is_latest {
        specs.push(ColumnSpec {
            value: "IS_LATEST".to_string(),
            width: W_IS_LATEST,
            align: Align::Left,
        });
    }
    if opts.show_owner {
        specs.push(ColumnSpec {
            value: "OWNER_DISPLAY_NAME".to_string(),
            width: W_OWNER_DISPLAY_NAME,
            align: Align::Left,
        });
        specs.push(ColumnSpec {
            value: "OWNER_ID".to_string(),
            width: W_OWNER_ID,
            align: Align::Left,
        });
    }
    if opts.show_restore_status {
        specs.push(ColumnSpec {
            value: "IS_RESTORE_IN_PROGRESS".to_string(),
            width: W_IS_RESTORE_IN_PROGRESS,
            align: Align::Left,
        });
        specs.push(ColumnSpec {
            value: "RESTORE_EXPIRY_DATE".to_string(),
            width: W_RESTORE_EXPIRY_DATE,
            align: Align::Left,
        });
    }

    if opts.aligned {
        Some(render_cols(&specs, "KEY"))
    } else {
        let mut parts: Vec<&str> = specs.iter().map(|c| c.value.as_str()).collect();
        parts.push("KEY");
        Some(parts.join("\t"))
    }
}
```

Note: header labels are always `Align::Left`, even for the SIZE column, since the label `"SIZE"` is text, not a number. This is intentional — a right-aligned `SIZE` label would place `SIZE` in the last 4 chars of a 20-wide column, far from the "S" in the expected reading position.

Note: header `IS_RESTORE_IN_PROGRESS` is width 22, longer than W_IS_RESTORE_IN_PROGRESS (5). The label exceeds the column width, so `pad()` returns it unchanged and subsequent header columns shift — acceptable since the label's overflow is bounded (22 chars) and the header is one-time output. (If this becomes visually problematic, a future iteration can bump `W_IS_RESTORE_IN_PROGRESS` to 22. Out of scope here — spec said 5.)

- [ ] **Step 4.4: Run tests to verify all pass**

```
cargo test 2>&1 | tail -30
```

Expected: all header tests pass (new aligned header and pre-existing `formatter_writes_header_when_configured`).

```
cargo fmt
cargo clippy --all-features 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 4.5: Commit**

```
git add src/display/tsv.rs
git commit -S -m "feat(display): aligned branch for TsvFormatter::format_header"
```

---

## Task 5: Aligned branch in `TsvFormatter::format_summary`

**Files:**
- Modify: `src/display/tsv.rs`

- [ ] **Step 5.1: Write the failing tests**

Append to the `#[cfg(test)] mod tests { ... }` block in `src/display/tsv.rs`:

```rust
#[test]
fn format_summary_aligned_uses_spaces() {
    let stats = crate::types::ListingStatistics {
        total_objects: 42,
        total_size: 5678901,
        total_delete_markers: 0,
    };
    let fmt = TsvFormatter::new(FormatOptions {
        human: true,
        aligned: true,
        ..Default::default()
    });
    let summary = fmt.format_summary(&stats);
    assert_eq!(summary, "\nTotal: 42 objects 5.4 MiB");
}

#[test]
fn format_summary_aligned_non_human() {
    let stats = crate::types::ListingStatistics {
        total_objects: 3,
        total_size: 100,
        total_delete_markers: 0,
    };
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        ..Default::default()
    });
    let summary = fmt.format_summary(&stats);
    assert_eq!(summary, "\nTotal: 3 objects 100 bytes");
}

#[test]
fn format_summary_aligned_with_versions() {
    let stats = crate::types::ListingStatistics {
        total_objects: 10,
        total_size: 1024,
        total_delete_markers: 3,
    };
    let fmt = TsvFormatter::new(FormatOptions {
        aligned: true,
        all_versions: true,
        ..Default::default()
    });
    let summary = fmt.format_summary(&stats);
    assert_eq!(summary, "\nTotal: 10 objects 1024 bytes 3 delete markers");
}
```

- [ ] **Step 5.2: Run tests to verify they fail**

```
cargo test --lib format_summary_aligned 2>&1 | tail -20
```

Expected: 3 failures — summary currently uses tabs regardless of `aligned`.

- [ ] **Step 5.3: Implement aligned summary**

In `src/display/tsv.rs`, replace the body of `fn format_summary` with:

```rust
fn format_summary(&self, stats: &ListingStatistics) -> String {
    let (size_num, size_unit) = if self.opts.human {
        format_size_split(stats.total_size)
    } else {
        (stats.total_size.to_string(), "bytes".to_string())
    };
    let sep = if self.opts.aligned { " " } else { "\t" };
    let mut line = format!(
        "\nTotal:{sep}{}{sep}objects{sep}{}{sep}{}",
        stats.total_objects, size_num, size_unit
    );
    if self.opts.all_versions {
        line.push_str(&format!("{sep}{}{sep}delete markers", stats.total_delete_markers));
    }
    line
}
```

- [ ] **Step 5.4: Run tests to verify they pass**

```
cargo test 2>&1 | tail -30
```

Expected: all summary tests pass (new aligned ones, plus the pre-existing `format_summary_text`, `format_summary_text_non_human`, etc.).

```
cargo fmt
cargo clippy --all-features 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 5.5: Commit**

```
git add src/display/tsv.rs
git commit -S -m "feat(display): aligned branch for TsvFormatter::format_summary"
```

---

## Task 6: Aligned output in `bucket_lister`

**Files:**
- Modify: `src/bucket_lister.rs`

- [ ] **Step 6.1: Write the failing tests**

Append to `src/bucket_lister.rs` (at the end of the file, either in an existing `#[cfg(test)] mod tests` block or add one if none exists):

```rust
#[cfg(test)]
mod aligned_tests {
    use super::*;

    fn entry() -> BucketEntry {
        BucketEntry {
            name: "mybucket".to_string(),
            region: Some("us-east-1".to_string()),
            creation_date: Some(
                chrono::Utc
                    .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
                    .unwrap(),
            ),
            bucket_arn: Some("arn:aws:s3:::mybucket".to_string()),
            owner_display_name: Some("alice".to_string()),
            owner_id: Some("id-alice".to_string()),
        }
    }

    fn opts(aligned: bool, show_arn: bool, show_owner: bool) -> BucketFormatOpts {
        BucketFormatOpts {
            aligned,
            show_bucket_arn: show_arn,
            show_owner,
            raw_output: false,
        }
    }

    #[test]
    fn bucket_tsv_unchanged_when_not_aligned() {
        let line = format_bucket_entry(&entry(), &opts(false, false, false));
        assert_eq!(line, "2024-01-01T00:00:00Z\tus-east-1\tmybucket");
    }

    #[test]
    fn bucket_aligned_default_bucket_is_last_unpadded() {
        use crate::display::aligned::{SEP, W_BUCKET_REGION, W_DATE};
        let line = format_bucket_entry(&entry(), &opts(true, false, false));
        let date = "2024-01-01T00:00:00Z";
        let region = "us-east-1";
        let expected = format!(
            "{date}{}{SEP}{region}{}{SEP}mybucket",
            " ".repeat(W_DATE - date.chars().count()),
            " ".repeat(W_BUCKET_REGION - region.chars().count()),
        );
        assert_eq!(line, expected);
    }

    #[test]
    fn bucket_aligned_with_show_bucket_arn_puts_arn_last() {
        use crate::display::aligned::{SEP, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE};
        let line = format_bucket_entry(&entry(), &opts(true, true, false));
        let date = "2024-01-01T00:00:00Z";
        let region = "us-east-1";
        let bucket = "mybucket";
        let arn = "arn:aws:s3:::mybucket";
        let expected = format!(
            "{date}{}{SEP}{region}{}{SEP}{bucket}{}{SEP}{arn}",
            " ".repeat(W_DATE - date.chars().count()),
            " ".repeat(W_BUCKET_REGION - region.chars().count()),
            " ".repeat(W_BUCKET_NAME - bucket.chars().count()),
        );
        assert_eq!(line, expected);
    }

    #[test]
    fn bucket_aligned_with_show_owner_puts_owner_id_last() {
        use crate::display::aligned::{
            SEP, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE, W_OWNER_DISPLAY_NAME,
        };
        let line = format_bucket_entry(&entry(), &opts(true, false, true));
        let date = "2024-01-01T00:00:00Z";
        let region = "us-east-1";
        let bucket = "mybucket";
        let owner_name = "alice";
        let expected = format!(
            "{date}{}{SEP}{region}{}{SEP}{bucket}{}{SEP}{owner_name}{}{SEP}id-alice",
            " ".repeat(W_DATE - date.chars().count()),
            " ".repeat(W_BUCKET_REGION - region.chars().count()),
            " ".repeat(W_BUCKET_NAME - bucket.chars().count()),
            " ".repeat(W_OWNER_DISPLAY_NAME - owner_name.chars().count()),
        );
        assert_eq!(line, expected);
    }

    #[test]
    fn bucket_aligned_header_default() {
        use crate::display::aligned::{SEP, W_BUCKET_REGION, W_DATE};
        let h = format_bucket_header(&opts(true, false, false));
        let expected = format!(
            "DATE{}{SEP}REGION{}{SEP}BUCKET",
            " ".repeat(W_DATE - "DATE".len()),
            " ".repeat(W_BUCKET_REGION - "REGION".len()),
        );
        assert_eq!(h, expected);
    }
}
```

Also adjust the `use` imports at the top of `bucket_lister.rs` to include `chrono::TimeZone` (needed by the test module). If `chrono` is not already imported there, the test module is already using `super::*` so adding `use chrono::TimeZone;` *inside* the `mod aligned_tests` block after `use super::*;` is the cleanest placement.

- [ ] **Step 6.2: Run tests to verify they fail**

```
cargo test --lib bucket_lister::aligned_tests 2>&1 | tail -30
```

Expected: compile errors — `format_bucket_entry`, `format_bucket_header`, `BucketFormatOpts` don't exist yet.

- [ ] **Step 6.3: Extract helpers and implement aligned branch**

Open `src/bucket_lister.rs`. Above `pub async fn list_buckets(...)`, add:

```rust
pub(crate) struct BucketFormatOpts {
    pub aligned: bool,
    pub show_bucket_arn: bool,
    pub show_owner: bool,
    pub raw_output: bool,
}

fn bucket_escape(s: &str, raw_output: bool) -> String {
    if raw_output {
        s.to_string()
    } else {
        crate::display::escape_control_chars(s).into_owned()
    }
}

pub(crate) fn format_bucket_entry(entry: &BucketEntry, opts: &BucketFormatOpts) -> String {
    use crate::display::aligned::{
        Align, ColumnSpec, W_BUCKET_ARN, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE,
        W_OWNER_DISPLAY_NAME, W_OWNER_ID, render_cols,
    };

    let date = entry
        .creation_date
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default();
    let region = entry.region.as_deref().unwrap_or("").to_string();
    let bucket = bucket_escape(&entry.name, opts.raw_output);

    // Build the full column sequence in display order (excluding the
    // final column). The "last column" is popped off and emitted
    // unpadded by render_cols / by the non-aligned joiner below.
    let mut specs: Vec<ColumnSpec> = Vec::new();
    specs.push(ColumnSpec {
        value: date,
        width: W_DATE,
        align: Align::Left,
    });
    specs.push(ColumnSpec {
        value: region,
        width: W_BUCKET_REGION,
        align: Align::Left,
    });
    specs.push(ColumnSpec {
        value: bucket,
        width: W_BUCKET_NAME,
        align: Align::Left,
    });
    if opts.show_bucket_arn {
        specs.push(ColumnSpec {
            value: entry.bucket_arn.as_deref().unwrap_or("").to_string(),
            width: W_BUCKET_ARN,
            align: Align::Left,
        });
    }
    if opts.show_owner {
        specs.push(ColumnSpec {
            value: bucket_escape(
                entry.owner_display_name.as_deref().unwrap_or(""),
                opts.raw_output,
            ),
            width: W_OWNER_DISPLAY_NAME,
            align: Align::Left,
        });
        specs.push(ColumnSpec {
            value: bucket_escape(entry.owner_id.as_deref().unwrap_or(""), opts.raw_output),
            width: W_OWNER_ID,
            align: Align::Left,
        });
    }

    // Pop the last column — it is emitted unpadded.
    let last = specs.pop().expect("at least DATE+REGION+BUCKET are present");

    if opts.aligned {
        render_cols(&specs, &last.value)
    } else {
        let mut parts: Vec<&str> = specs.iter().map(|c| c.value.as_str()).collect();
        parts.push(&last.value);
        parts.join("\t")
    }
}

pub(crate) fn format_bucket_header(opts: &BucketFormatOpts) -> String {
    use crate::display::aligned::{
        Align, ColumnSpec, W_BUCKET_ARN, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE,
        W_OWNER_DISPLAY_NAME, W_OWNER_ID, render_cols,
    };

    let mut specs: Vec<ColumnSpec> = Vec::new();
    specs.push(ColumnSpec {
        value: "DATE".to_string(),
        width: W_DATE,
        align: Align::Left,
    });
    specs.push(ColumnSpec {
        value: "REGION".to_string(),
        width: W_BUCKET_REGION,
        align: Align::Left,
    });
    specs.push(ColumnSpec {
        value: "BUCKET".to_string(),
        width: W_BUCKET_NAME,
        align: Align::Left,
    });
    if opts.show_bucket_arn {
        specs.push(ColumnSpec {
            value: "BUCKET_ARN".to_string(),
            width: W_BUCKET_ARN,
            align: Align::Left,
        });
    }
    if opts.show_owner {
        specs.push(ColumnSpec {
            value: "OWNER_DISPLAY_NAME".to_string(),
            width: W_OWNER_DISPLAY_NAME,
            align: Align::Left,
        });
        specs.push(ColumnSpec {
            value: "OWNER_ID".to_string(),
            width: W_OWNER_ID,
            align: Align::Left,
        });
    }
    let last = specs.pop().expect("at least DATE+REGION+BUCKET");

    if opts.aligned {
        render_cols(&specs, &last.value)
    } else {
        let mut parts: Vec<&str> = specs.iter().map(|c| c.value.as_str()).collect();
        parts.push(&last.value);
        parts.join("\t")
    }
}
```

Then rewrite the body of the existing `for entry in &entries { ... }` loop in `list_buckets` to use these helpers. Replace the non-JSON branch (currently lines 118–144 in the existing file):

```rust
        for entry in &entries {
            if config.display_config.json {
                // ... existing JSON code unchanged ...
            } else {
                let bopts = BucketFormatOpts {
                    aligned: config.display_config.aligned,
                    show_bucket_arn,
                    show_owner,
                    raw_output: config.display_config.raw_output,
                };
                writeln!(writer, "{}", format_bucket_entry(entry, &bopts))?;
            }
        }
```

And replace the header emission (currently lines 59–68) with:

```rust
    if config.display_config.header && !config.display_config.json {
        let bopts = BucketFormatOpts {
            aligned: config.display_config.aligned,
            show_bucket_arn,
            show_owner,
            raw_output: config.display_config.raw_output,
        };
        writeln!(writer, "{}", format_bucket_header(&bopts))?;
    }
```

- [ ] **Step 6.4: Run tests to verify they pass**

```
cargo test 2>&1 | tail -40
```

Expected: all 5 new `aligned_tests` pass; no pre-existing test regressions.

```
cargo fmt
cargo clippy --all-features 2>&1 | tail -20
```

Expected: no warnings.

- [ ] **Step 6.5: Commit**

```
git add src/bucket_lister.rs
git commit -S -m "feat(bucket-lister): aligned output support with extracted format helpers"
```

---

## Task 7: E2E tests for `--aligned`

**Files:**
- Modify: `tests/e2e_display.rs` (append new test functions gated by `#[cfg(e2e_test)]`)

E2E tests require `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_display` and live AWS credentials, so we add them but do not gate pre-commit validation on them. The goal is providing executable verification for humans running the full E2E suite.

- [ ] **Step 7.1: Inspect the existing e2e_display test style**

Before writing new tests, read the first ~150 lines of `tests/e2e_display.rs` to match its conventions — how the binary is invoked, how output is asserted, which test fixtures are already set up.

```
head -n 180 tests/e2e_display.rs
```

- [ ] **Step 7.2: Append new tests to `tests/e2e_display.rs`**

Following the style observed in Step 7.1, append three test functions. Each should:

1. Use whatever harness the existing tests use (likely a helper that invokes the s3ls binary and captures stdout).
2. Assert the specific property described.

Pseudocode (adapt to the actual harness):

```rust
#[cfg(e2e_test)]
#[tokio::test]
async fn e2e_aligned_object_listing() {
    // List a known prefix with --aligned. Parse each non-blank line's
    // first N columns (dropping the key column at the end), assert
    // that each of those column regions has the expected byte length
    // (W_DATE + 2, W_SIZE + 2, ...).
    //
    // Harness pattern: run_s3ls(&["--recursive", "--aligned", "s3://...whatever..."])
}

#[cfg(e2e_test)]
#[tokio::test]
async fn e2e_aligned_with_no_sort() {
    // Run --aligned --no-sort against a small real prefix. Verify:
    // 1. The process completes successfully.
    // 2. Each line has the expected column structure (same as above).
    // This is primarily a smoke test of the architectural claim that
    // --aligned does not require sort buffering.
}

#[cfg(e2e_test)]
#[tokio::test]
async fn e2e_aligned_with_human_and_summary() {
    // Run --aligned --human-readable --summarize.
    // 1. All object rows match the aligned layout (W_DATE + W_SIZE_HUMAN).
    // 2. The last line starts with "\nTotal: " (space-separated summary).
}
```

If the actual harness in `tests/e2e_display.rs` does not match this shape, follow whatever pattern already exists — the point is just to exercise the three flag combinations.

- [ ] **Step 7.3: Run the e2e suite locally (optional; requires AWS credentials)**

```
RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_display -- e2e_aligned 2>&1 | tail -30
```

Expected: all 3 tests pass (if AWS credentials are set up).

For environments without AWS credentials, verify at minimum that the tests compile:

```
RUSTFLAGS="--cfg e2e_test" cargo check --tests 2>&1 | tail -20
```

Expected: no compile errors.

- [ ] **Step 7.4: Commit**

```
git add tests/e2e_display.rs
git commit -S -m "test(e2e): add --aligned coverage to e2e_display"
```

---

## Task 8: Documentation updates

**Files:**
- Modify: `README.md`

- [ ] **Step 8.1: Update the "Display options" section**

Open `README.md`. In the "Display options" section (around lines 417-430 in the current version), add a new subsection **after** the existing examples:

```markdown
### Aligned output

```bash
# Default TSV — machine-friendly, tabs between columns
$ s3ls --recursive s3://my-bucket/data/
2024-01-15T10:30:00Z	1234	data/readme.txt
2024-06-01T08:00:00Z	5678	data/2024/report.csv

# Aligned — columns padded with spaces so the output scans well in a terminal
$ s3ls --recursive --aligned s3://my-bucket/data/
2024-01-15T10:30:00Z                        1234  data/readme.txt
2024-06-01T08:00:00Z                        5678  data/2024/report.csv

# Combined with --human-readable
$ s3ls --recursive --aligned --human-readable s3://my-bucket/data/
2024-01-15T10:30:00Z          1.2KiB  data/readme.txt
2024-06-01T08:00:00Z          5.5KiB  data/2024/report.csv
```

`--aligned` pads each non-KEY column to a fixed width so rows line up
on screen. It is independent of `--human-readable`:

- `--human-readable` makes individual **values** human-friendly.
- `--aligned` makes the **layout** human-friendly.

The default tab-separated output is kept for `cut`, `awk`, and other
Unix tools. `--aligned` composes with `--no-sort`, `--header`,
`--summarize`, and every `--show-*` flag. It conflicts with `--json`
(NDJSON is not columnar).
```

- [ ] **Step 8.2: Update the "All command line options" block**

The embedded `-h` output in `README.md` (around lines 828-961) lists every option. Either:

1. Regenerate the block after the feature is implemented:
   ```
   cargo build --release
   ./target/release/s3ls -h
   ```
   and paste the output into the fenced `text` block, or
2. Manually insert the `--aligned` line under the `Display:` section in that block, matching the style of `--raw-output`.

Option 1 is more reliable — the output should include `--aligned` automatically after Task 2 landed.

- [ ] **Step 8.3: Update the "Readable by both machines and humans" section**

In `README.md` (around lines 181-190 in the current version), add a final short paragraph to the "Tab-separated text" subsection:

```markdown
For interactive terminal use, add `--aligned` to pad each column to a
fixed width with spaces. This is a pure layout option and is
independent of `--human-readable` (which formats individual values).
```

- [ ] **Step 8.4: Commit**

```
git add README.md
git commit -S -m "docs: document --aligned option in README"
```

---

## Final verification

- [ ] **Full test run**

```
cargo test 2>&1 | tail -10
```

Expected: all tests pass, zero failures.

- [ ] **Lint**

```
cargo fmt -- --check
cargo clippy --all-features -- -D warnings
```

Expected: both succeed with no output.

- [ ] **Manual smoke test (optional)**

```
cargo build --release
./target/release/s3ls -h | grep -A1 aligned
```

Expected: the `--aligned` flag appears in the Display section of the help output.

- [ ] **Log summary commit for the feature (optional)**

If you prefer a single logical commit for the whole feature in lieu of the per-task commits, squash on merge. Otherwise the per-task commits stand as-is.
