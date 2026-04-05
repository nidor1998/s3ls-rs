# Multi-Column Sort Design

## Summary

Allow users to specify up to two sort columns via `--sort`, comma-separated (e.g. `--sort date,key`). The user's specification is the complete sort order — no automatic tie-breakers are applied.

## CLI

The `--sort` argument changes from a single `ValueEnum` to a `String` with a custom value parser.

**Syntax:** `--sort <field>[,<field>]`

**Valid fields:** `key`, `size`, `date` (case-insensitive)

**Validation rules:**
- At most 2 fields
- No duplicate fields (e.g. `date,date` is rejected)
- Each token must be a valid field name

**Default:** `key`

**Examples:**
```
s3ls s3://bucket/                      # sort by key (default)
s3ls s3://bucket/ --sort size          # sort by size
s3ls s3://bucket/ --sort date,key      # sort by date, then key
s3ls s3://bucket/ --sort size,date     # sort by size, then date
```

**Error messages:**
- Unknown field: `invalid sort field 'name'; expected one of: key, size, date`
- Too many fields: `at most 2 sort fields allowed`
- Duplicate: `duplicate sort field 'date'`

## Config

`Config.sort` changes from `SortField` to `Vec<SortField>`.

## Sort Logic (`aggregate.rs`)

`sort_entries` signature becomes:

```rust
pub fn sort_entries(
    entries: &mut [ListEntry],
    fields: &[SortField],
    reverse: bool,
)
```

- The `all_versions: bool` parameter is removed.
- Comparison chains the user's fields in order using `.then_with(|| ...)`.
- `--reverse` applies to the final combined comparison (same as today).

## Pipeline

`pipeline.rs` updates the call site to pass `&self.config.sort` (a `Vec<SortField>`) and drops the `all_versions` argument.

## Behavioral Change

Previously, with `--all-versions`, automatic secondary tie-breakers were applied:
- `--sort key` → key, mtime
- `--sort size` → size, key, mtime
- `--sort date` → mtime, key

After this change, a single `--sort key` means sort by key only — no hidden tie-breakers. Users who want deterministic multi-field sorting specify it explicitly: `--sort key,date`.

## Files Changed

| File | Change |
|------|--------|
| `src/config/args/mod.rs` | Custom value parser, `CLIArgs.sort` becomes `Vec<SortField>` |
| `src/config/args/tests.rs` | New parsing tests, update existing sort tests |
| `src/config/mod.rs` | `Config.sort` becomes `Vec<SortField>` |
| `src/aggregate.rs` | `sort_entries` takes `&[SortField]`, remove `all_versions` param, chain fields |
| `src/pipeline.rs` | Update call site |
| `src/lib.rs` | No change expected (re-exports `SortField` which stays the same) |
