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
// S3 caps a single object at 50 TiB, which is 54_975_581_388_800 bytes
// — 14 digits. 14 therefore fits every well-formed object size.
pub const W_SIZE: usize = 14;
pub const W_SIZE_HUMAN: usize = 9;
pub const W_STORAGE_CLASS: usize = 19;
// 32 hex chars for the MD5-style digest, plus a `-<part-count>`
// suffix for multipart uploads. S3 allows up to 10,000 parts, so the
// suffix can be 6 chars (`-10000`): 32 + 6 = 38.
pub const W_ETAG: usize = 38;
pub const W_CHECKSUM_ALGORITHM: usize = 34;
// The longest data value is `FULL_OBJECT` (11 chars), but the header
// label `CHECKSUM_TYPE` is 13 chars. Widen to 13 so the header fits
// without overflowing and shifting columns to its right.
pub const W_CHECKSUM_TYPE: usize = 13;
pub const W_VERSION_ID: usize = 32;
pub const W_IS_LATEST: usize = 10;
pub const W_OWNER_DISPLAY_NAME: usize = 64;
pub const W_OWNER_ID: usize = 64;
// Sized to the header label `IS_RESTORE_IN_PROGRESS` (22 chars) so the
// header fits without overflowing. The longest data value (`false`,
// 5 chars) pads out with trailing spaces in each row.
pub const W_IS_RESTORE_IN_PROGRESS: usize = 22;
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
