use crate::config::Config;
use crate::config::args::SortField;
use anyhow::Result;
use aws_sdk_s3::Client;
use aws_smithy_types::error::display::DisplayErrorContext;
use chrono::{DateTime, Utc};
use std::io::Write;

struct BucketEntry {
    name: String,
    creation_date: Option<DateTime<Utc>>,
    region: Option<String>,
    bucket_arn: Option<String>,
    owner_display_name: Option<String>,
    owner_id: Option<String>,
}

pub(crate) struct BucketFormatOpts {
    pub tsv: bool,
    pub one_line: bool,
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

fn format_bucket_entry(entry: &BucketEntry, opts: &BucketFormatOpts) -> String {
    if opts.one_line {
        return bucket_escape(&entry.name, opts.raw_output);
    }
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

    let mut specs: Vec<ColumnSpec> = vec![
        ColumnSpec {
            value: date,
            width: W_DATE,
            align: Align::Left,
        },
        ColumnSpec {
            value: region,
            width: W_BUCKET_REGION,
            align: Align::Left,
        },
        ColumnSpec {
            value: bucket,
            width: W_BUCKET_NAME,
            align: Align::Left,
        },
    ];
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

    // Whichever column is last is emitted unpadded (no trailing width padding).
    let last = specs
        .pop()
        .expect("at least DATE+REGION+BUCKET are always present");

    if opts.tsv {
        let mut parts: Vec<&str> = specs.iter().map(|c| c.value.as_str()).collect();
        parts.push(&last.value);
        parts.join("\t")
    } else {
        render_cols(&specs, &last.value)
    }
}

fn format_bucket_header(opts: &BucketFormatOpts) -> String {
    if opts.one_line {
        return "BUCKET".to_string();
    }
    use crate::display::aligned::{
        Align, ColumnSpec, W_BUCKET_ARN, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE,
        W_OWNER_DISPLAY_NAME, W_OWNER_ID, render_cols,
    };

    let mut specs: Vec<ColumnSpec> = vec![
        ColumnSpec {
            value: "DATE".to_string(),
            width: W_DATE,
            align: Align::Left,
        },
        ColumnSpec {
            value: "REGION".to_string(),
            width: W_BUCKET_REGION,
            align: Align::Left,
        },
        ColumnSpec {
            value: "BUCKET".to_string(),
            width: W_BUCKET_NAME,
            align: Align::Left,
        },
    ];
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

    let last = specs
        .pop()
        .expect("at least DATE+REGION+BUCKET are always present");

    if opts.tsv {
        let mut parts: Vec<&str> = specs.iter().map(|c| c.value.as_str()).collect();
        parts.push(&last.value);
        parts.join("\t")
    } else {
        render_cols(&specs, &last.value)
    }
}

pub async fn list_buckets(config: &Config) -> Result<()> {
    let client_config = config
        .target_client_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No client configuration provided"))?;

    let client = client_config.create_client().await;

    let mut entries = if config.list_express_one_zone_buckets {
        let mut buckets = list_directory_buckets(&client).await?;
        // ListDirectoryBuckets API does not support prefix filtering,
        // so filter client-side.
        if let Some(ref prefix) = config.bucket_name_prefix {
            buckets.retain(|e| e.name.starts_with(prefix.as_str()));
        }
        buckets
    } else {
        list_general_buckets(&client, config.bucket_name_prefix.as_deref()).await?
    };

    // Sort (skipped when --no-sort is set)
    if !config.no_sort {
        entries.sort_by(|a, b| {
            let mut cmp = std::cmp::Ordering::Equal;
            for field in &config.sort {
                cmp = cmp.then_with(|| match field {
                    SortField::Bucket | SortField::Key => a.name.cmp(&b.name),
                    SortField::Date => a.creation_date.cmp(&b.creation_date),
                    SortField::Region => a.region.cmp(&b.region),
                    SortField::Size => std::cmp::Ordering::Equal,
                });
            }
            if config.reverse { cmp.reverse() } else { cmp }
        });
    }

    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    let show_owner = config.display_config.show_owner;
    let show_bucket_arn = config.display_config.show_bucket_arn;

    // Options for text-mode header + per-entry formatting. Constructed
    // once because these flags are row-invariant.
    let bopts = BucketFormatOpts {
        tsv: config.display_config.tsv,
        one_line: config.display_config.one_line,
        show_bucket_arn,
        show_owner,
        raw_output: config.display_config.raw_output,
    };

    if config.display_config.header && !config.display_config.json {
        writeln!(writer, "{}", format_bucket_header(&bopts))?;
    }

    for entry in &entries {
        if config.display_config.json {
            let mut map = serde_json::Map::new();
            map.insert(
                "Name".to_string(),
                serde_json::Value::String(entry.name.clone()),
            );
            if let Some(d) = entry.creation_date {
                map.insert(
                    "CreationDate".to_string(),
                    serde_json::Value::String(d.to_rfc3339()),
                );
            }
            if let Some(ref r) = entry.region {
                map.insert(
                    "BucketRegion".to_string(),
                    serde_json::Value::String(r.clone()),
                );
            }
            if show_bucket_arn && let Some(ref arn) = entry.bucket_arn {
                map.insert(
                    "BucketArn".to_string(),
                    serde_json::Value::String(arn.clone()),
                );
            }
            if show_owner {
                let owner_id = entry.owner_id.as_ref();
                let owner_name = entry.owner_display_name.as_ref();
                if owner_id.is_some() || owner_name.is_some() {
                    let mut owner = serde_json::Map::new();
                    if let Some(name) = owner_name {
                        owner.insert(
                            "DisplayName".to_string(),
                            serde_json::Value::String(name.clone()),
                        );
                    }
                    if let Some(id) = owner_id {
                        owner.insert("ID".to_string(), serde_json::Value::String(id.clone()));
                    }
                    map.insert("Owner".to_string(), serde_json::Value::Object(owner));
                }
            }
            writeln!(writer, "{}", serde_json::to_string(&map).unwrap())?;
        } else {
            // Escape control chars in S3-returned strings to prevent
            // injection of fake rows or terminal escape sequences via
            // maliciously-named buckets / owners. JSON output is handled
            // safely by serde_json above.
            writeln!(writer, "{}", format_bucket_entry(entry, &bopts))?;
        }
    }

    writer.flush()?;
    Ok(())
}

async fn list_general_buckets(client: &Client, prefix: Option<&str>) -> Result<Vec<BucketEntry>> {
    let mut entries = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_buckets().max_buckets(1000);
        if let Some(prefix) = prefix {
            req = req.prefix(prefix);
        }
        if let Some(ref token) = continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list buckets: {}", DisplayErrorContext(&e)))?;

        let owner_display_name = resp
            .owner()
            .and_then(|o| o.display_name())
            .map(|s| s.to_string());
        let owner_id = resp.owner().and_then(|o| o.id()).map(|s| s.to_string());

        for b in resp.buckets() {
            entries.push(BucketEntry {
                name: b.name().unwrap_or_default().to_string(),
                creation_date: b.creation_date().and_then(aws_datetime_to_chrono),
                region: b.bucket_region().map(|r| r.to_string()),
                bucket_arn: b.bucket_arn().map(|s| s.to_string()),
                owner_display_name: owner_display_name.clone(),
                owner_id: owner_id.clone(),
            });
        }

        let next_token = resp
            .continuation_token()
            .filter(|t| !t.is_empty())
            .map(|s| s.to_string());
        match next_token {
            None => break,
            Some(ref t) if Some(t) == continuation_token.as_ref() => {
                anyhow::bail!(
                    "ListBuckets returned the same continuation token twice; \
                     refusing to loop. This is likely a bug in the S3-compatible endpoint."
                );
            }
            Some(t) => continuation_token = Some(t),
        }
    }

    Ok(entries)
}

async fn list_directory_buckets(client: &Client) -> Result<Vec<BucketEntry>> {
    let mut entries = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_directory_buckets();
        if let Some(ref token) = continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to list directory buckets: {}",
                DisplayErrorContext(&e)
            )
        })?;

        for b in resp.buckets() {
            entries.push(BucketEntry {
                name: b.name().unwrap_or_default().to_string(),
                creation_date: b.creation_date().and_then(aws_datetime_to_chrono),
                region: b.bucket_region().map(|r| r.to_string()),
                bucket_arn: b.bucket_arn().map(|s| s.to_string()),
                owner_display_name: None,
                owner_id: None,
            });
        }

        // An empty token must terminate (some S3-compatible endpoints emit
        // Some("") at end-of-pagination instead of omitting the field).
        // Detect a token equal to the one we just sent, which would otherwise
        // loop forever against a buggy endpoint.
        let next_token = resp
            .continuation_token()
            .filter(|t| !t.is_empty())
            .map(|s| s.to_string());
        match next_token {
            None => break,
            Some(ref t) if Some(t) == continuation_token.as_ref() => {
                anyhow::bail!(
                    "ListDirectoryBuckets returned the same continuation token twice; \
                     refusing to loop. This is likely a bug in the S3-compatible endpoint."
                );
            }
            Some(t) => continuation_token = Some(t),
        }
    }

    Ok(entries)
}

fn aws_datetime_to_chrono(dt: &aws_smithy_types::DateTime) -> Option<DateTime<Utc>> {
    let epoch_secs = dt.secs();
    chrono::DateTime::from_timestamp(epoch_secs, dt.subsec_nanos())
}

#[cfg(test)]
mod aligned_tests {
    use super::*;
    use chrono::TimeZone;

    fn entry() -> BucketEntry {
        BucketEntry {
            name: "mybucket".to_string(),
            region: Some("us-east-1".to_string()),
            creation_date: Some(chrono::Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()),
            bucket_arn: Some("arn:aws:s3:::mybucket".to_string()),
            owner_display_name: Some("alice".to_string()),
            owner_id: Some("id-alice".to_string()),
        }
    }

    fn opts(tsv: bool, show_arn: bool, show_owner: bool) -> BucketFormatOpts {
        BucketFormatOpts {
            tsv,
            one_line: false,
            show_bucket_arn: show_arn,
            show_owner,
            raw_output: false,
        }
    }

    #[test]
    fn bucket_one_line_emits_only_name() {
        let opts = BucketFormatOpts {
            tsv: false,
            one_line: true,
            show_bucket_arn: true,
            show_owner: true,
            raw_output: false,
        };
        let line = format_bucket_entry(&entry(), &opts);
        assert_eq!(line, "mybucket");
    }

    #[test]
    fn bucket_one_line_ignores_show_flags_even_when_tsv() {
        // one_line takes precedence over the column format in the formatter.
        let opts = BucketFormatOpts {
            tsv: true,
            one_line: true,
            show_bucket_arn: true,
            show_owner: true,
            raw_output: false,
        };
        let line = format_bucket_entry(&entry(), &opts);
        assert_eq!(line, "mybucket");
    }

    #[test]
    fn bucket_one_line_header_is_bucket_label() {
        let opts = BucketFormatOpts {
            tsv: false,
            one_line: true,
            show_bucket_arn: true,
            show_owner: true,
            raw_output: false,
        };
        assert_eq!(format_bucket_header(&opts), "BUCKET");
    }

    #[test]
    fn bucket_tsv_when_tsv_true() {
        let line = format_bucket_entry(&entry(), &opts(true, false, false));
        assert_eq!(line, "2024-01-01T00:00:00Z\tus-east-1\tmybucket");
    }

    #[test]
    fn bucket_aligned_default_bucket_is_last_unpadded() {
        use crate::display::aligned::{SEP, W_BUCKET_REGION, W_DATE};
        let line = format_bucket_entry(&entry(), &opts(false, false, false));
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
        let line = format_bucket_entry(&entry(), &opts(false, true, false));
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
        let line = format_bucket_entry(&entry(), &opts(false, false, true));
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
    fn bucket_aligned_with_arn_and_owner_owner_id_is_last() {
        use crate::display::aligned::{
            SEP, W_BUCKET_ARN, W_BUCKET_NAME, W_BUCKET_REGION, W_DATE, W_OWNER_DISPLAY_NAME,
        };
        let line = format_bucket_entry(&entry(), &opts(false, true, true));
        let date = "2024-01-01T00:00:00Z";
        let region = "us-east-1";
        let bucket = "mybucket";
        let arn = "arn:aws:s3:::mybucket";
        let owner_name = "alice";
        let expected = format!(
            "{date}{}{SEP}{region}{}{SEP}{bucket}{}{SEP}{arn}{}{SEP}{owner_name}{}{SEP}id-alice",
            " ".repeat(W_DATE - date.chars().count()),
            " ".repeat(W_BUCKET_REGION - region.chars().count()),
            " ".repeat(W_BUCKET_NAME - bucket.chars().count()),
            " ".repeat(W_BUCKET_ARN - arn.chars().count()),
            " ".repeat(W_OWNER_DISPLAY_NAME - owner_name.chars().count()),
        );
        assert_eq!(line, expected);
    }

    #[test]
    fn bucket_aligned_header_default() {
        use crate::display::aligned::{SEP, W_BUCKET_REGION, W_DATE};
        let h = format_bucket_header(&opts(false, false, false));
        let expected = format!(
            "DATE{}{SEP}REGION{}{SEP}BUCKET",
            " ".repeat(W_DATE - "DATE".len()),
            " ".repeat(W_BUCKET_REGION - "REGION".len()),
        );
        assert_eq!(h, expected);
    }
}
