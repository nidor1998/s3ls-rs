use crate::config::Config;
use crate::config::args::SortField;
use anyhow::Result;
use aws_sdk_s3::Client;
use aws_smithy_types::error::display::DisplayErrorContext;
use std::io::Write;
use time::OffsetDateTime;

/// Converts a bucket's `CreationDate`, warning when the value is outside the
/// supported range (years -9999 to 9999). The bucket is still listed, without
/// a creation date.
fn bucket_creation_date(
    name: Option<&str>,
    creation_date: Option<&aws_smithy_types::DateTime>,
) -> Option<OffsetDateTime> {
    let dt = creation_date?;
    let converted = crate::types::datetime::from_aws_datetime(dt);
    if converted.is_none() {
        tracing::warn!(
            bucket = name.unwrap_or_default(),
            creation_date_secs = dt.secs(),
            "CreationDate is outside the supported range (years -9999 to 9999); showing the bucket without it"
        );
    }
    converted
}

struct BucketEntry {
    name: String,
    creation_date: Option<OffsetDateTime>,
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
    pub show_local_time: bool,
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
        .as_ref()
        .map(|d| crate::display::format_rfc3339(d, opts.show_local_time))
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

    let mut entries =
        fetch_bucket_entries(config, &client, client_config.endpoint_url.is_none()).await?;

    sort_bucket_entries(&mut entries, config);

    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());
    write_bucket_entries(&mut writer, &entries, config)
}

/// Fetch the buckets selected by `config`: directory buckets (filtered
/// client-side by prefix) or general purpose buckets.
async fn fetch_bucket_entries(
    config: &Config,
    client: &Client,
    use_max_buckets: bool,
) -> Result<Vec<BucketEntry>> {
    if config.list_express_one_zone_buckets {
        let mut buckets = list_directory_buckets(client).await?;
        // ListDirectoryBuckets API does not support prefix filtering,
        // so filter client-side.
        if let Some(ref prefix) = config.bucket_name_prefix {
            buckets.retain(|e| e.name.starts_with(prefix.as_str()));
        }
        Ok(buckets)
    } else {
        list_general_buckets(
            client,
            config.bucket_name_prefix.as_deref(),
            use_max_buckets,
        )
        .await
    }
}

fn sort_bucket_entries(entries: &mut [BucketEntry], config: &Config) {
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
}

fn write_bucket_entries<W: Write>(
    writer: &mut W,
    entries: &[BucketEntry],
    config: &Config,
) -> Result<()> {
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
        show_local_time: config.display_config.show_local_time,
    };

    if config.display_config.header && !config.display_config.json {
        writeln!(writer, "{}", format_bucket_header(&bopts))?;
    }

    for entry in entries {
        if config.display_config.json {
            let mut map = serde_json::Map::new();
            map.insert(
                "Name".to_string(),
                serde_json::Value::String(entry.name.clone()),
            );
            if let Some(ref d) = entry.creation_date {
                map.insert(
                    "CreationDate".to_string(),
                    serde_json::Value::String(crate::display::format_rfc3339(
                        d,
                        bopts.show_local_time,
                    )),
                );
            }
            if let Some(ref r) = entry.region {
                map.insert(
                    "BucketRegion".to_string(),
                    serde_json::Value::String(r.clone()),
                );
            }
            if let Some(ref arn) = entry.bucket_arn {
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

async fn list_general_buckets(
    client: &Client,
    prefix: Option<&str>,
    use_max_buckets: bool,
) -> Result<Vec<BucketEntry>> {
    let mut entries = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = client.list_buckets();
        // AWS S3 only populates BucketArn/BucketRegion in the response when
        // MaxBuckets or Prefix is set on the request. Skip MaxBuckets when a
        // custom endpoint is configured, since some S3-compatible storage
        // providers do not support this parameter.
        if use_max_buckets {
            req = req.max_buckets(1000);
        }
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
                creation_date: bucket_creation_date(b.name(), b.creation_date()),
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
                creation_date: bucket_creation_date(b.name(), b.creation_date()),
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

#[cfg(test)]
mod aligned_tests {
    use super::*;

    #[test]
    fn bucket_creation_date_out_of_range_is_none() {
        use aws_smithy_types::DateTime as SmithyDateTime;
        let too_late = SmithyDateTime::from_secs(253_402_300_800);
        assert!(bucket_creation_date(Some("b"), Some(&too_late)).is_none());
        let ok = SmithyDateTime::from_secs(1_700_000_000);
        assert!(bucket_creation_date(Some("b"), Some(&ok)).is_some());
        assert!(bucket_creation_date(Some("b"), None).is_none());
    }

    fn entry() -> BucketEntry {
        BucketEntry {
            name: "mybucket".to_string(),
            region: Some("us-east-1".to_string()),
            creation_date: Some(crate::types::datetime::utc_ymd_hms(2024, 1, 1, 0, 0, 0)),
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
            show_local_time: false,
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
            show_local_time: false,
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
            show_local_time: false,
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
            show_local_time: false,
        };
        assert_eq!(format_bucket_header(&opts), "BUCKET");
    }

    #[test]
    fn bucket_date_uses_utc_z_when_show_local_time_false() {
        // Default: UTC with the trailing 'Z' — matches the object-listing convention.
        let opts = opts(true, false, false);
        let line = format_bucket_entry(&entry(), &opts);
        assert!(
            line.starts_with("2024-01-01T00:00:00Z\t"),
            "expected UTC 'Z' suffix, got: {line}"
        );
    }

    #[test]
    fn bucket_date_uses_local_offset_when_show_local_time_true() {
        // format_rfc3339 with local=true always emits an explicit numeric
        // offset (use_z=false), so 'Z' must never appear here. The string
        // must still re-parse to the original UTC instant regardless of the
        // host's timezone.
        let mut bopts = opts(true, false, false);
        bopts.show_local_time = true;
        let line = format_bucket_entry(&entry(), &bopts);
        let date_field = line.split('\t').next().unwrap();

        assert!(
            !date_field.ends_with('Z'),
            "expected explicit offset, got UTC 'Z': {date_field}"
        );

        let parsed = crate::types::datetime::parse_rfc3339_utc(date_field)
            .unwrap_or_else(|e| panic!("failed to parse {date_field}: {e}"));
        assert_eq!(parsed, entry().creation_date.unwrap());
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

#[cfg(test)]
mod listing_tests {
    use super::*;
    use crate::test_util::ReplayHttpClient;

    fn bucket_listing_config() -> Config {
        Config::try_from(crate::parse_from_args(vec!["s3ls"]).unwrap()).unwrap()
    }

    fn named(name: &str) -> BucketEntry {
        BucketEntry {
            name: name.to_string(),
            creation_date: Some(crate::types::datetime::utc_ymd_hms(2024, 1, 1, 0, 0, 0)),
            region: Some("us-east-1".to_string()),
            bucket_arn: Some(format!("arn:aws:s3:::{name}")),
            owner_display_name: None,
            owner_id: None,
        }
    }

    fn names(entries: &[BucketEntry]) -> Vec<&str> {
        entries.iter().map(|e| e.name.as_str()).collect()
    }

    fn general_page(buckets: &[&str], continuation_token: Option<&str>) -> String {
        let buckets: String = buckets
            .iter()
            .map(|name| {
                format!(
                    "<Bucket><Name>{name}</Name>\
                     <CreationDate>2024-01-01T00:00:00.000Z</CreationDate>\
                     <BucketRegion>us-east-1</BucketRegion>\
                     <BucketArn>arn:aws:s3:::{name}</BucketArn></Bucket>"
                )
            })
            .collect();
        let token = continuation_token
            .map(|t| format!("<ContinuationToken>{t}</ContinuationToken>"))
            .unwrap_or_default();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Owner><ID>owner-id</ID><DisplayName>alice</DisplayName></Owner>\
             <Buckets>{buckets}</Buckets>{token}</ListAllMyBucketsResult>"
        )
    }

    fn directory_page(buckets: &[&str], continuation_token: Option<&str>) -> String {
        let buckets: String = buckets
            .iter()
            .map(|name| {
                format!(
                    "<Bucket><Name>{name}</Name>\
                     <CreationDate>2024-01-01T00:00:00.000Z</CreationDate></Bucket>"
                )
            })
            .collect();
        let token = continuation_token
            .map(|t| format!("<ContinuationToken>{t}</ContinuationToken>"))
            .unwrap_or_default();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListAllMyDirectoryBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Buckets>{buckets}</Buckets>{token}</ListAllMyDirectoryBucketsResult>"
        )
    }

    // -----------------------------------------------------------------------
    // list_buckets
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_buckets_errors_without_client_config() {
        let mut config = bucket_listing_config();
        config.target_client_config = None;
        let err = list_buckets(&config).await.unwrap_err();
        assert!(
            err.to_string().contains("No client configuration"),
            "got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // list_general_buckets
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn general_buckets_follow_continuation_token() {
        let page1 = general_page(&["bucket-a"], Some("tok1"));
        let page2 = general_page(&["bucket-b"], None);
        let http = ReplayHttpClient::new(vec![(200, &page1), (200, &page2)]);

        let entries = list_general_buckets(&http.s3_client(), Some("bucket-"), true)
            .await
            .unwrap();

        assert_eq!(names(&entries), vec!["bucket-a", "bucket-b"]);
        assert_eq!(entries[0].owner_display_name.as_deref(), Some("alice"));
        assert_eq!(entries[0].owner_id.as_deref(), Some("owner-id"));
        assert_eq!(entries[0].region.as_deref(), Some("us-east-1"));
        assert_eq!(
            entries[0].bucket_arn.as_deref(),
            Some("arn:aws:s3:::bucket-a")
        );
        assert!(entries[0].creation_date.is_some());

        let uris = http.request_uris();
        assert_eq!(uris.len(), 2);
        assert!(uris[0].contains("max-buckets=1000"), "got: {}", uris[0]);
        assert!(uris[0].contains("prefix=bucket-"), "got: {}", uris[0]);
        assert!(
            !uris[0].contains("continuation-token"),
            "first request must not carry a token: {}",
            uris[0]
        );
        assert!(
            uris[1].contains("continuation-token=tok1"),
            "second request must carry the token: {}",
            uris[1]
        );
    }

    #[tokio::test]
    async fn general_buckets_omit_max_buckets_and_prefix_when_not_requested() {
        let page = general_page(&["bucket-a"], None);
        let http = ReplayHttpClient::new(vec![(200, &page)]);

        let entries = list_general_buckets(&http.s3_client(), None, false)
            .await
            .unwrap();

        assert_eq!(names(&entries), vec!["bucket-a"]);
        let uris = http.request_uris();
        assert_eq!(uris.len(), 1);
        assert!(!uris[0].contains("max-buckets"), "got: {}", uris[0]);
        assert!(!uris[0].contains("prefix="), "got: {}", uris[0]);
    }

    #[tokio::test]
    async fn general_buckets_stop_on_empty_continuation_token() {
        // Some S3-compatible endpoints emit an empty token at the end of
        // pagination instead of omitting the field.
        let page = general_page(&["bucket-a"], Some(""));
        let http = ReplayHttpClient::new(vec![(200, &page)]);

        let entries = list_general_buckets(&http.s3_client(), None, true)
            .await
            .unwrap();

        assert_eq!(names(&entries), vec!["bucket-a"]);
        assert_eq!(http.request_uris().len(), 1);
    }

    #[tokio::test]
    async fn general_buckets_bail_on_repeated_continuation_token() {
        let page1 = general_page(&["bucket-a"], Some("tok1"));
        let page2 = general_page(&["bucket-b"], Some("tok1"));
        let http = ReplayHttpClient::new(vec![(200, &page1), (200, &page2)]);

        let err = list_general_buckets(&http.s3_client(), None, true)
            .await
            .err()
            .unwrap();

        assert!(
            err.to_string()
                .contains("ListBuckets returned the same continuation token twice"),
            "got: {err}"
        );
        assert_eq!(http.request_uris().len(), 2, "must not loop further");
    }

    #[tokio::test]
    async fn general_buckets_report_api_failure() {
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                    <Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
        let http = ReplayHttpClient::new(vec![(403, body)]);

        let err = list_general_buckets(&http.s3_client(), None, true)
            .await
            .err()
            .unwrap();

        assert!(
            err.to_string().contains("Failed to list buckets"),
            "got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // list_directory_buckets
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn directory_buckets_follow_continuation_token() {
        let page1 = directory_page(&["a--use1-az4--x-s3"], Some("tok1"));
        let page2 = directory_page(&["b--use1-az4--x-s3"], None);
        let http = ReplayHttpClient::new(vec![(200, &page1), (200, &page2)]);

        let entries = list_directory_buckets(&http.s3_client()).await.unwrap();

        assert_eq!(
            names(&entries),
            vec!["a--use1-az4--x-s3", "b--use1-az4--x-s3"]
        );
        assert!(entries.iter().all(|e| e.owner_id.is_none()));
        assert!(entries.iter().all(|e| e.owner_display_name.is_none()));

        let uris = http.request_uris();
        assert_eq!(uris.len(), 2);
        assert!(!uris[0].contains("continuation-token"), "got: {}", uris[0]);
        assert!(
            uris[1].contains("continuation-token=tok1"),
            "got: {}",
            uris[1]
        );
    }

    #[tokio::test]
    async fn directory_buckets_stop_on_empty_continuation_token() {
        let page = directory_page(&["a--use1-az4--x-s3"], Some(""));
        let http = ReplayHttpClient::new(vec![(200, &page)]);

        let entries = list_directory_buckets(&http.s3_client()).await.unwrap();

        assert_eq!(names(&entries), vec!["a--use1-az4--x-s3"]);
        assert_eq!(http.request_uris().len(), 1);
    }

    #[tokio::test]
    async fn directory_buckets_bail_on_repeated_continuation_token() {
        let page1 = directory_page(&["a--use1-az4--x-s3"], Some("tok1"));
        let page2 = directory_page(&["b--use1-az4--x-s3"], Some("tok1"));
        let http = ReplayHttpClient::new(vec![(200, &page1), (200, &page2)]);

        let err = list_directory_buckets(&http.s3_client())
            .await
            .err()
            .unwrap();

        assert!(
            err.to_string()
                .contains("ListDirectoryBuckets returned the same continuation token twice"),
            "got: {err}"
        );
        assert_eq!(http.request_uris().len(), 2, "must not loop further");
    }

    #[tokio::test]
    async fn directory_buckets_report_api_failure() {
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                    <Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
        let http = ReplayHttpClient::new(vec![(403, body)]);

        let err = list_directory_buckets(&http.s3_client())
            .await
            .err()
            .unwrap();

        assert!(
            err.to_string().contains("Failed to list directory buckets"),
            "got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // fetch_bucket_entries
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn fetch_uses_general_buckets_by_default() {
        let page = general_page(&["bucket-a"], None);
        let http = ReplayHttpClient::new(vec![(200, &page)]);
        let mut config = bucket_listing_config();
        config.bucket_name_prefix = Some("bucket-".to_string());

        let entries = fetch_bucket_entries(&config, &http.s3_client(), false)
            .await
            .unwrap();

        assert_eq!(names(&entries), vec!["bucket-a"]);
        let uris = http.request_uris();
        assert!(uris[0].contains("prefix=bucket-"), "got: {}", uris[0]);
        assert!(!uris[0].contains("max-buckets"), "got: {}", uris[0]);
    }

    #[tokio::test]
    async fn fetch_filters_directory_buckets_by_prefix_client_side() {
        let page = directory_page(&["keep--use1-az4--x-s3", "drop--use1-az4--x-s3"], None);
        let http = ReplayHttpClient::new(vec![(200, &page)]);
        let mut config = bucket_listing_config();
        config.list_express_one_zone_buckets = true;
        config.bucket_name_prefix = Some("keep".to_string());

        let entries = fetch_bucket_entries(&config, &http.s3_client(), true)
            .await
            .unwrap();

        assert_eq!(names(&entries), vec!["keep--use1-az4--x-s3"]);
    }

    #[tokio::test]
    async fn fetch_returns_all_directory_buckets_without_prefix() {
        let page = directory_page(&["a--use1-az4--x-s3", "b--use1-az4--x-s3"], None);
        let http = ReplayHttpClient::new(vec![(200, &page)]);
        let mut config = bucket_listing_config();
        config.list_express_one_zone_buckets = true;
        config.bucket_name_prefix = None;

        let entries = fetch_bucket_entries(&config, &http.s3_client(), true)
            .await
            .unwrap();

        assert_eq!(entries.len(), 2);
    }

    // -----------------------------------------------------------------------
    // sort_bucket_entries
    // -----------------------------------------------------------------------

    #[test]
    fn sort_by_bucket_name_and_reverse() {
        let mut config = bucket_listing_config();
        config.no_sort = false;
        config.sort = vec![SortField::Bucket];
        config.reverse = false;

        let mut entries = vec![named("b"), named("c"), named("a")];
        sort_bucket_entries(&mut entries, &config);
        assert_eq!(names(&entries), vec!["a", "b", "c"]);

        config.reverse = true;
        sort_bucket_entries(&mut entries, &config);
        assert_eq!(names(&entries), vec!["c", "b", "a"]);
    }

    #[test]
    fn sort_by_region_then_date() {
        let mut config = bucket_listing_config();
        config.no_sort = false;
        config.sort = vec![SortField::Region, SortField::Date];
        config.reverse = false;

        let mut old_west = named("old-west");
        old_west.region = Some("us-west-2".to_string());
        old_west.creation_date = Some(crate::types::datetime::utc_ymd_hms(2020, 1, 1, 0, 0, 0));
        let mut new_west = named("new-west");
        new_west.region = Some("us-west-2".to_string());
        let east = named("east");

        let mut entries = vec![new_west, old_west, east];
        sort_bucket_entries(&mut entries, &config);
        assert_eq!(names(&entries), vec!["east", "old-west", "new-west"]);
    }

    #[test]
    fn sort_by_size_keeps_listing_order() {
        // Buckets have no size: the field compares equal, so the (stable)
        // sort leaves the listing order untouched.
        let mut config = bucket_listing_config();
        config.no_sort = false;
        config.sort = vec![SortField::Size];
        config.reverse = false;

        let mut entries = vec![named("b"), named("c"), named("a")];
        sort_bucket_entries(&mut entries, &config);
        assert_eq!(names(&entries), vec!["b", "c", "a"]);
    }

    #[test]
    fn no_sort_keeps_listing_order() {
        let mut config = bucket_listing_config();
        config.no_sort = true;
        config.sort = vec![SortField::Bucket];

        let mut entries = vec![named("b"), named("c"), named("a")];
        sort_bucket_entries(&mut entries, &config);
        assert_eq!(names(&entries), vec!["b", "c", "a"]);
    }

    // -----------------------------------------------------------------------
    // write_bucket_entries
    // -----------------------------------------------------------------------

    fn write_to_string(entries: &[BucketEntry], config: &Config) -> String {
        let mut out = Vec::new();
        write_bucket_entries(&mut out, entries, config).unwrap();
        String::from_utf8(out).unwrap()
    }

    fn json_lines(output: &str) -> Vec<serde_json::Value> {
        output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn json_includes_owner_display_name_and_id() {
        let mut config = bucket_listing_config();
        config.display_config.json = true;
        config.display_config.show_owner = true;

        let mut entry = named("mybucket");
        entry.owner_display_name = Some("alice".to_string());
        entry.owner_id = Some("id-alice".to_string());

        let parsed = json_lines(&write_to_string(&[entry], &config));
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["Name"], "mybucket");
        assert_eq!(parsed[0]["BucketRegion"], "us-east-1");
        assert_eq!(parsed[0]["BucketArn"], "arn:aws:s3:::mybucket");
        assert_eq!(parsed[0]["CreationDate"], "2024-01-01T00:00:00Z");
        assert_eq!(parsed[0]["Owner"]["DisplayName"], "alice");
        assert_eq!(parsed[0]["Owner"]["ID"], "id-alice");
    }

    #[test]
    fn json_owner_with_only_one_of_display_name_or_id() {
        let mut config = bucket_listing_config();
        config.display_config.json = true;
        config.display_config.show_owner = true;

        let mut name_only = named("name-only");
        name_only.owner_display_name = Some("alice".to_string());
        let mut id_only = named("id-only");
        id_only.owner_id = Some("id-alice".to_string());

        let parsed = json_lines(&write_to_string(&[name_only, id_only], &config));
        assert_eq!(parsed[0]["Owner"]["DisplayName"], "alice");
        assert!(parsed[0]["Owner"].get("ID").is_none());
        assert_eq!(parsed[1]["Owner"]["ID"], "id-alice");
        assert!(parsed[1]["Owner"].get("DisplayName").is_none());
    }

    #[test]
    fn json_omits_absent_fields_and_owner() {
        let mut config = bucket_listing_config();
        config.display_config.json = true;
        config.display_config.show_owner = true;

        let bare = BucketEntry {
            name: "bare".to_string(),
            creation_date: None,
            region: None,
            bucket_arn: None,
            owner_display_name: None,
            owner_id: None,
        };

        let parsed = json_lines(&write_to_string(&[bare], &config));
        let object = parsed[0].as_object().unwrap();
        assert_eq!(object.len(), 1, "only Name is present: {object:?}");
        assert_eq!(parsed[0]["Name"], "bare");
    }

    #[test]
    fn json_hides_owner_unless_requested_and_ignores_header() {
        let mut config = bucket_listing_config();
        config.display_config.json = true;
        config.display_config.show_owner = false;
        config.display_config.header = true;

        let mut entry = named("mybucket");
        entry.owner_display_name = Some("alice".to_string());
        entry.owner_id = Some("id-alice".to_string());

        let parsed = json_lines(&write_to_string(&[entry], &config));
        assert_eq!(parsed.len(), 1, "JSON output has no header line");
        assert!(parsed[0].get("Owner").is_none());
    }

    #[test]
    fn text_output_writes_header_then_rows() {
        let mut config = bucket_listing_config();
        config.display_config.json = false;
        config.display_config.tsv = true;
        config.display_config.one_line = false;
        config.display_config.header = true;
        config.display_config.show_owner = false;
        config.display_config.show_bucket_arn = false;
        config.display_config.show_local_time = false;

        let output = write_to_string(&[named("mybucket")], &config);
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec![
                "DATE\tREGION\tBUCKET",
                "2024-01-01T00:00:00Z\tus-east-1\tmybucket"
            ]
        );
    }

    #[test]
    fn text_output_without_header() {
        let mut config = bucket_listing_config();
        config.display_config.json = false;
        config.display_config.tsv = false;
        config.display_config.one_line = true;
        config.display_config.header = false;

        let output = write_to_string(&[named("a"), named("b")], &config);
        assert_eq!(output, "a\nb\n");
    }
}
