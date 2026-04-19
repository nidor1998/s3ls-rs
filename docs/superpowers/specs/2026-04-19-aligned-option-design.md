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
- `args.aligned` → `config.display_config.aligned` in the `Args → Config`
  conversion in `src/config/args/mod.rs`.
- The pipeline reads `config.display_config.aligned` to pick between
  `TsvFormatter` and `AlignedFormatter`; `FormatOptions` itself does not
  carry an `aligned` field (the dispatch happens once, not per row).

## Column widths & alignment rules

### Object listing

| Column                   | Width | Align | Source of bound                                                     |
| ------------------------ | ----: | :---: | ------------------------------------------------------------------- |
| DATE                     |    25 |   L   | RFC 3339 UTC (20) or local with offset (25)                         |
| SIZE (digits)            |    14 | **R** | S3's 50 TiB single-object max (54,975,581,388,800 bytes = 14 digits); also holds `PRE` / `DELETE` |
| SIZE (human)             |     9 | **R** | `1023.9EiB` worst case                                              |
| STORAGE_CLASS            |    19 |   L   | `INTELLIGENT_TIERING`                                               |
| ETAG                     |    38 |   L   | 32 hex + `-` + up-to-5-digit part count (S3 allows up to 10,000 parts) |
| CHECKSUM_ALGORITHM       |    34 |   L   | `CRC32,CRC32C,SHA1,SHA256,CRC64NVME`                                |
| CHECKSUM_TYPE            |    13 |   L   | Sized to the header label (`FULL_OBJECT` is 11, but `CHECKSUM_TYPE` is 13) |
| VERSION_ID               |    32 |   L   | Typical S3 VersionId                                                |
| IS_LATEST                |    10 |   L   | `NOT_LATEST`                                                        |
| OWNER_DISPLAY_NAME       |    64 |   L   | Conservative                                                        |
| OWNER_ID                 |    64 |   L   | Canonical user ID (fixed 64 hex)                                    |
| IS_RESTORE_IN_PROGRESS   |    22 | **R** | Sized to the header label (`true`/`false` is 5, but `IS_RESTORE_IN_PROGRESS` is 22) |
| RESTORE_EXPIRY_DATE      |    25 |   L   | RFC 3339 with offset                                                |
| KEY                      |     — |   L   | Rightmost, no trailing padding                                      |

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

Four modules in `src/display/` collaborate:

1. **`aligned.rs`** — pure data + helpers. Width constants, `Align`
   enum, `ColumnSpec` struct, `pad`, `render_cols`. No opinion about
   which rows to render or when.
2. **`columns.rs`** — `build_entry_cols(entry, opts) -> (Vec<ColumnSpec>, String)`
   and `build_header_cols(opts) -> Vec<ColumnSpec>`. This is where the
   per-`ListEntry`-variant column shapes live. Both the TSV and
   aligned formatters share this builder.
3. **`tsv.rs`** — `TsvFormatter` implements `EntryFormatter`. Each
   method is a thin wrapper: call the builder, tab-join values, push
   the KEY.
4. **`aligned_formatter.rs`** — `AlignedFormatter` implements
   `EntryFormatter`. Each method calls the builder and then
   `render_cols` to produce padded, two-space-separated output.

The pipeline selects which formatter to instantiate based on
`config.display_config.aligned`; neither formatter branches per row.

### Width constants (`src/display/aligned.rs`)

```rust
// Object listing columns
pub const W_DATE: usize = 25;
pub const W_SIZE: usize = 14;                  // S3 max object = 50 TiB = 14 digits
pub const W_SIZE_HUMAN: usize = 9;
pub const W_STORAGE_CLASS: usize = 19;
pub const W_ETAG: usize = 38;                  // 32 hex + '-' + up-to-5-digit parts
pub const W_CHECKSUM_ALGORITHM: usize = 34;
pub const W_CHECKSUM_TYPE: usize = 13;         // sized to header label
pub const W_VERSION_ID: usize = 32;
pub const W_IS_LATEST: usize = 10;
pub const W_OWNER_DISPLAY_NAME: usize = 64;
pub const W_OWNER_ID: usize = 64;
pub const W_IS_RESTORE_IN_PROGRESS: usize = 22; // sized to header label
pub const W_RESTORE_EXPIRY_DATE: usize = 25;

// Bucket listing columns
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

### Formatter methods

Each formatter has a ~5-line `format_entry`, `format_header`, and
`format_summary`. Example — `AlignedFormatter`:

```rust
fn format_entry(&self, entry: &ListEntry) -> String {
    let (specs, key) = build_entry_cols(entry, &self.opts);
    render_cols(&specs, &key)
}

fn format_header(&self) -> Option<String> {
    let specs = build_header_cols(&self.opts);
    Some(render_cols(&specs, "KEY"))
}
```

`TsvFormatter` uses the same builders and replaces `render_cols` with
`specs.iter().map(|c| c.value.as_str()).collect::<Vec<_>>() +
push(&key); parts.join("\t")`.

**Summary lines.** `TsvFormatter::format_summary` joins with `\t`
(existing behavior preserved). `AlignedFormatter::format_summary`
joins with single spaces:

- `\nTotal: 42 objects 5.4 MiB`
- `\nTotal: 10 objects 1024 bytes 3 delete markers` (when `all_versions`)

Control character escaping happens before padding, so `\x0a` takes 4
visible characters and the column width accounts for that.

### `bucket_lister.rs`

`list_buckets()` used to format lines inline. Extracted into:

```rust
struct BucketFormatOpts {
    aligned: bool,
    show_bucket_arn: bool,
    show_owner: bool,
    raw_output: bool,
}

fn format_bucket_entry(entry: &BucketEntry, opts: &BucketFormatOpts) -> String;
fn format_bucket_header(opts: &BucketFormatOpts) -> String;
```

Both helpers build a `Vec<ColumnSpec>`, pop the last column (whichever
is rightmost based on the `--show-*` flags), and either call
`render_cols` or tab-join based on `opts.aligned`. The rightmost column
is emitted unpadded — same contract as the KEY column for object
listings.

JSON output path is unchanged (conflicts with `--aligned`). No summary
for bucket listing.

### Pipeline dispatch (`src/pipeline.rs`)

```rust
let formatter: Box<dyn EntryFormatter> = if config.display_config.json {
    Box::new(JsonFormatter::new(opts))
} else if config.display_config.aligned {
    Box::new(AlignedFormatter::new(opts))
} else {
    Box::new(TsvFormatter::new(opts))
};
```

## Tests

### Unit tests

- In `src/display/aligned.rs`: `pad` (left/right, exact-length,
  overflow, char-count-not-bytes), `render_cols` (empty, two-column,
  trailing SEP before key).
- In `src/display/aligned_formatter.rs`: object/prefix/delete-marker
  rows aligned correctly, right-aligned SIZE / PRE / DELETE, overflow
  preserves value, escape-before-pad, header padding, summary uses
  spaces, full column count via `format_text_aligned_with_all_optional_columns`.
- In `src/display/tsv.rs`: unchanged pre-existing TSV behavior
  (confirms the non-aligned path is byte-identical to the original
  implementation).
- In `src/bucket_lister.rs`: the four `(show_bucket_arn, show_owner)`
  combinations for aligned bucket rows + header; `bucket_tsv_unchanged_when_not_aligned`
  guards the non-aligned path.
- In `src/config/args/tests.rs`: `aligned_conflicts_with_json`,
  default-false, composition with `--no-sort` and `--human-readable`.

### E2E tests (gated by `#[cfg(e2e_test)]`)

In `tests/e2e_display.rs`:

1. `e2e_aligned_object_listing` — basic aligned object listing; SEP
   positions derived from `W_DATE + SEP.len() + W_SIZE + SEP.len()`.
2. `e2e_aligned_with_no_sort` — `--aligned --no-sort` streams aligned
   output without buffering.
3. `e2e_aligned_with_human_and_summary` — composition with
   `--human-readable --summarize`.
4. `e2e_aligned_all_columns` — every `--show-*` flag + `--header
   --all-versions`; asserts the total char count matches `sum(W_*) +
   12*SEP + 3` (12 non-KEY columns + `KEY`).

In `tests/e2e_bucket_listing.rs`:

5. `e2e_aligned_bucket_listing_all_columns` — aligned bucket listing
   with every `--show-*` flag + `--header`; asserts `OWNER_ID` sits at
   byte offset `W_DATE + SEP + W_BUCKET_REGION + SEP + W_BUCKET_NAME +
   SEP + W_BUCKET_ARN + SEP + W_OWNER_DISPLAY_NAME + SEP` as the
   rightmost unpadded column.

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

## Revisions since the initial spec

As the feature landed, a handful of specifics diverged from the
original plan. This section captures the current-truth values — the
rest of the doc has already been updated inline; this list is just
the "what changed and why" for reviewers reading the git history.

- **`W_SIZE` 20 → 14.** The original 20 was sized for `u64::MAX`
  digits. S3 caps a single object at 50 TiB = 14 digits, so 14 fits
  every well-formed size and saves 6 chars of horizontal space on
  every data row.
- **`W_ETAG` 35 → 38.** Original assumed 32 hex + `-NN` multipart
  suffix. S3 actually allows up to 10,000 parts, so the suffix is up
  to `-10000` (6 chars) → 38 total.
- **`W_CHECKSUM_TYPE` 11 → 13.** Original sized for the longest data
  value (`FULL_OBJECT`). Header label `CHECKSUM_TYPE` is 13 chars and
  would otherwise overflow the column on the header row, shifting
  every column to its right.
- **`W_IS_RESTORE_IN_PROGRESS` 5 → 22.** Originally left at the data-
  value max (`false`) with the 22-char header label accepted as
  overflow. Later widened to 22 so the header fits flush.
- **Formatters split.** The initial spec put the aligned branch
  inside `TsvFormatter::format_entry/_header/_summary`. The split
  into a dedicated `AlignedFormatter` (plus the shared `columns.rs`
  builders) kept each formatter short and removed per-row branching.
  `FormatOptions.aligned` was dropped — dispatch happens once in the
  pipeline, not on every row.
- **Bucket-listing helper signatures.** Original
  `format_bucket_entry(entry, opts, aligned)` was collapsed to
  `format_bucket_entry(entry, &BucketFormatOpts)` since `opts`
  already carries the `aligned` flag and three other row-invariant
  bools — keeping them together makes the call site one line.
- **E2E test coverage expanded.** Two additional all-columns tests
  (`e2e_aligned_all_columns` for objects, `e2e_aligned_bucket_listing_all_columns`
  for buckets) were added beyond the three tests in the initial
  spec, to catch regressions where a new column is introduced but
  the layout math wasn't updated.
