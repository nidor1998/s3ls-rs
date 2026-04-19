# `--aligned` option — design

## Summary

Add a new `--aligned` flag that formats TSV output with columns padded to
fixed widths using ASCII spaces, producing a human-readable layout while
the default `\t`-separated output remains unchanged and machine-friendly.

`--aligned` is independent of `--human-readable`:

- `--human-readable` makes individual **values** human-friendly (sizes as
  `1.2KiB` rather than raw byte counts).
- `--aligned` makes the **layout** human-friendly (columns line up on
  screen rather than relying on tab stops).

The two can be combined freely.

## Motivation

The default tab-separated output is well-suited for piping into `cut`,
`awk`, `jq`, and other line-oriented tools. In a terminal, however,
tabs align to the terminal's tab stops, not to column content, so rows
with varying field lengths don't line up visually. `--aligned` fixes
this for interactive terminal use without compromising the machine
default.

## Architectural decision: zero buffering

Alignment is usually implemented by buffering all rows, measuring the
widest value per column, then emitting padded rows. In s3ls-rs this
buffering is unnecessary because:

1. Every non-KEY column has a bounded maximum width derived from the S3
   API contract or a known enum (see "Column widths" below).
2. The KEY column is always emitted last, so it needs no trailing
   padding — long keys just extend past the last aligned column.

Therefore `--aligned` pads each non-KEY column to a pre-determined
fixed width and streams rows one-by-one, preserving s3ls-rs's existing
pipeline behavior. In particular, `--aligned` composes cleanly with
`--no-sort` (streaming, constant memory) with no special handling.

## CLI surface

New flag on `Args` (in `src/config/args/mod.rs`):

```rust
/// Display output with columns aligned using whitespace padding.
#[arg(
    long,
    env = "ALIGNED",
    default_value_t = false,
    conflicts_with = "json",
    help_heading = "Display"
)]
pub aligned: bool,
```

### Conflicts

Only `--json` (NDJSON output is not columnar). `--aligned` composes
with `--no-sort`, `--header`, `--human-readable`, `--summarize`,
`--raw-output`, `--show-*`, `--show-relative-path`, `--show-local-time`,
and `--show-bucket-arn`.

### Interaction with `--raw-output`

Allowed. If raw bytes include control characters (newlines, tabs,
ANSI escapes), they are not re-escaped, so column widths are disrupted
for any row containing such a byte. This is a deliberate user tradeoff
that `--raw-output` already implies; we document it rather than forbid
the combination.

## Config plumbing

- `DisplayConfig.aligned: bool` (default `false`) in `src/config/mod.rs`.
- `FormatOptions.aligned: bool` in `src/display/mod.rs`, wired up by
  `FormatOptions::from_display_config`.
- `args.aligned` → `config.display_config.aligned` in the `Args → Config`
  conversion in `src/config/args/mod.rs`.

## Column widths & alignment rules

### Object listing

| Column                   | Width | Align | Source of bound                                     |
| ------------------------ | ----: | :---: | --------------------------------------------------- |
| DATE                     |    25 |   L   | RFC 3339 UTC (20) or local with offset (25)         |
| SIZE (digits)            |    20 | **R** | `u64::MAX` = 20 digits; also holds `PRE` / `DELETE` |
| SIZE (human)             |     9 | **R** | `1023.9EiB` worst case                              |
| STORAGE_CLASS            |    19 |   L   | `INTELLIGENT_TIERING`                               |
| ETAG                     |    35 |   L   | 32 hex + optional `-NN` multipart suffix            |
| CHECKSUM_ALGORITHM       |    34 |   L   | `CRC32,CRC32C,SHA1,SHA256,CRC64NVME`                |
| CHECKSUM_TYPE            |    11 |   L   | `FULL_OBJECT`                                       |
| VERSION_ID               |    32 |   L   | Typical S3 VersionId                                |
| IS_LATEST                |    10 |   L   | `NOT_LATEST`                                        |
| OWNER_DISPLAY_NAME       |    64 |   L   | Conservative                                        |
| OWNER_ID                 |    64 |   L   | Canonical user ID (fixed 64 hex)                    |
| IS_RESTORE_IN_PROGRESS   |     5 | **R** | `false`                                             |
| RESTORE_EXPIRY_DATE      |    25 |   L   | RFC 3339 with offset                                |
| KEY                      |     — |   L   | Rightmost, no trailing padding                      |

### Bucket listing

| Column             | Width | Align | Source                                                        |
| ------------------ | ----: | :---: | ------------------------------------------------------------- |
| DATE               |    25 |   L   | Same as above                                                 |
| REGION             |    20 |   L   | e.g., `ap-northeast-1` + margin                               |
| BUCKET             |    63 |   L   | S3 bucket name maximum                                        |
| BUCKET_ARN         |   100 |   L   | `arn:aws:s3:::<63-char-name>` + margin                        |
| OWNER_DISPLAY_NAME |    64 |   L   | Conservative                                                  |
| OWNER_ID           |    64 |   L   | Rightmost when present — no trailing padding                  |

The rightmost column actually emitted depends on the `--show-*` flags.
Whichever column is last in the row is emitted unpadded.

### Separator

Exactly two ASCII spaces between columns.

### Width measurement

`str::chars().count()`. No `unicode-width` crate dependency. CJK or
combining characters in OWNER_DISPLAY_NAME may occupy more visual
columns than `chars().count()` reports, causing minor misalignment on
those rows. Documented as a known limitation.

### Overflow policy

If a value exceeds its column width (e.g., an unusually long VersionId,
or an OwnerDisplayName with CJK characters that render wider than the
char count suggests), the value is emitted as-is without truncation.
Subsequent columns on that row shift right, but no data is hidden.
This mirrors `ls -l` behavior for long filenames.

## Implementation

### New module `src/display/aligned.rs`

Single location for all width constants plus helper functions:

```rust
// Widths for object listing columns
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

// Widths for bucket listing columns
pub const W_BUCKET_REGION: usize = 20;
pub const W_BUCKET_NAME: usize = 63;
pub const W_BUCKET_ARN: usize = 100;

pub const SEP: &str = "  ";

pub enum Align { Left, Right }

pub struct ColumnSpec {
    pub value: String,
    pub width: usize,
    pub align: Align,
}

pub fn pad(value: &str, width: usize, align: Align) -> String;
pub fn render_cols(cols: &[ColumnSpec], last_key: &str) -> String;
```

`render_cols` joins the non-KEY columns with `SEP` (after padding) and
appends `last_key` unpadded.

### `TsvFormatter` changes (`src/display/tsv.rs`)

The current `format_entry` builds a `Vec<String>` and joins with `\t`.
Replace the terminal `cols.join("\t")` with a branch:

- `if !opts.aligned`: current behavior, unchanged.
- `if opts.aligned`: build a `Vec<ColumnSpec>` describing each non-KEY
  column's width and alignment, call `render_cols(&specs, &key_str)`.

Same split for `format_header` — labels padded left to their column
widths, trailing `KEY` label unpadded.

`format_summary` becomes:

- `if !opts.aligned`: current tab-separated form, unchanged.
- `if opts.aligned`: join the same parts with single spaces:
  - `\nTotal: 42 objects 5.4 MiB`
  - `\nTotal: 42 objects 100 bytes 3 delete markers` (when
    `all_versions`)

Control character escaping happens before padding, so `\x0a` takes 4
visible characters and the column width accounts for that.

### `bucket_lister.rs` changes

`list_buckets()` currently formats lines inline. Extract:

```rust
fn format_bucket_entry(
    entry: &BucketEntry,
    opts: &BucketFormatOpts,
    aligned: bool,
) -> String;

fn format_bucket_header(opts: &BucketFormatOpts, aligned: bool) -> String;
```

Both helpers use `render_cols` from `src/display/aligned.rs` so the
alignment logic stays in one place. The last column (BUCKET, BUCKET_ARN,
or OWNER_ID depending on flags) is emitted unpadded.

JSON output path is unchanged (conflicts with `--aligned`). No summary
for bucket listing.

## Tests

### Unit tests (in `src/display/tsv.rs` and `src/bucket_lister.rs`)

1. `format_text_aligned_basic_object` — object row, aligned: columns
   have expected widths; key is unpadded; exactly two spaces between
   columns.
2. `format_text_aligned_right_aligns_size` — `12` padded with 18 leading
   spaces in a 20-wide SIZE column.
3. `format_text_aligned_pre_marker_right_aligned` — `PRE` and `DELETE`
   sentinels right-aligned.
4. `format_text_aligned_overflow_preserves_value` — value longer than
   its width is emitted as-is; subsequent columns shift; no data lost.
5. `format_text_aligned_header_padded` — header labels left-padded,
   trailing `KEY` unpadded.
6. `format_summary_aligned_uses_spaces` — summary uses single-space
   separators, not tabs.
7. `format_text_aligned_with_all_optional_columns` — every optional
   column has correct width and alignment.
8. `format_text_aligned_escapes_before_padding` — `\n` → `\x0a` and
   padding computed on the escaped string.
9. `bucket_aligned_basic` — bucket row padded; correct rightmost column
   left unpadded depending on `--show-*` flags.
10. `aligned_conflicts_with_json` — arg parsing rejects
    `--aligned --json` with a clear error.

### E2E tests (in `tests/e2e_display.rs`, gated by `#[cfg(e2e_test)]`)

1. `e2e_aligned_object_listing` — list a small prefix with `--aligned`;
   each non-last column position is consistent across rows.
2. `e2e_aligned_with_no_sort` — `--aligned --no-sort` streams aligned
   output without buffering (verifies the architectural property).
3. `e2e_aligned_with_human_and_summary` — `--aligned --human-readable
   --summarize` composes correctly.

## README updates

- New "Aligned output" subsection under "Display options" with a
  before/after example, and an example combined with
  `--human-readable`.
- Add `--aligned` to the "All command line options" block.
- Add a note under "Readable by both machines and humans" clarifying
  that TSV is the machine-friendly default and `--aligned` is the
  human-readable layout option, independent of `--human-readable`.

## Acceptance criteria

1. `s3ls --recursive --aligned s3://bucket/` produces a table where
   every non-KEY column has a consistent column position across rows
   (modulo overflow and CJK edge cases).
2. `--aligned --no-sort` works without any buffering (memory use equals
   `--no-sort` alone).
3. `--aligned --json` errors at arg parsing with a conflict message.
4. `--aligned --human-readable`, `--aligned --header`,
   `--aligned --summarize`, and every `--show-*` combination compose
   correctly.
5. `cargo fmt` and `cargo clippy --all-features` pass with zero
   warnings.
