//! Column-layout builders shared by `TsvFormatter` and `AlignedFormatter`.
//!
//! Both formatters emit the same column sequence for a given entry
//! and header; they differ only in how they render the resulting
//! `Vec<ColumnSpec>` (tab-join vs. padded space-join). Centralizing
//! the column construction here keeps both formatters short and
//! ensures any future column change updates in one place.

use crate::display::aligned::{
    Align, ColumnSpec, W_CHECKSUM_ALGORITHM, W_CHECKSUM_TYPE, W_DATE, W_ETAG, W_IS_LATEST,
    W_IS_RESTORE_IN_PROGRESS, W_OWNER_DISPLAY_NAME, W_OWNER_ID, W_RESTORE_EXPIRY_DATE, W_SIZE,
    W_SIZE_HUMAN, W_STORAGE_CLASS, W_VERSION_ID,
};
use crate::display::{
    FormatOptions, format_key_display, format_rfc3339, format_size, maybe_escape,
};
use crate::types::ListEntry;

/// Build the column specs for a single list entry. Returns the specs
/// (all non-KEY columns) and the display-ready, already-escaped key
/// string — always the rightmost column.
pub(crate) fn build_entry_cols(
    entry: &ListEntry,
    opts: &FormatOptions,
) -> (Vec<ColumnSpec>, String) {
    let mut specs: Vec<ColumnSpec> = Vec::new();
    let size_width = if opts.human { W_SIZE_HUMAN } else { W_SIZE };

    let key_col: String = match entry {
        ListEntry::CommonPrefix(_) => {
            specs.push(ColumnSpec {
                value: String::new(),
                width: W_DATE,
                align: Align::Left,
            });
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
            // In --all-versions mode, Object and DeleteMarker rows
            // include a version_id column (and is_latest if enabled).
            // CommonPrefix has neither, so emit placeholders to keep
            // columns aligned.
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
                    align: Align::Right,
                });
                specs.push(ColumnSpec {
                    value: String::new(),
                    width: W_RESTORE_EXPIRY_DATE,
                    align: Align::Left,
                });
            }
            maybe_escape(&format_key_display(entry.key(), opts), opts).into_owned()
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
            maybe_escape(&format_key_display(entry.key(), opts), opts).into_owned()
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
                // Delete markers have no restore status — leave empty.
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
            maybe_escape(&format_key_display(key, opts), opts).into_owned()
        }
    };

    (specs, key_col)
}

/// Build the non-KEY header label column specs. Each formatter appends
/// its own `"KEY"` label as the trailing column.
pub(crate) fn build_header_cols(opts: &FormatOptions) -> Vec<ColumnSpec> {
    let size_width = if opts.human { W_SIZE_HUMAN } else { W_SIZE };
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
    specs
}
