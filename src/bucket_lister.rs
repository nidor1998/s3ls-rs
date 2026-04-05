use crate::config::args::SortField;
use crate::config::Config;
use anyhow::Result;
use aws_sdk_s3::Client;
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

pub async fn list_buckets(config: &Config) -> Result<()> {
    let client_config = config
        .target_client_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No client configuration provided"))?;

    let client = client_config.create_client().await;

    let mut entries = if config.list_express_one_zone_buckets {
        list_directory_buckets(&client).await?
    } else {
        list_general_buckets(&client, config.bucket_name_prefix.as_deref()).await?
    };

    // Sort
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

    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    let show_owner = config.display_config.show_owner;
    let show_bucket_arn = config.display_config.show_bucket_arn;

    if config.display_config.header && !config.display_config.json {
        let mut header = "DATE\tREGION\tBUCKET".to_string();
        if show_bucket_arn {
            header.push_str("\tBUCKET_ARN");
        }
        if show_owner {
            header.push_str("\tOWNER_DISPLAY_NAME\tOWNER_ID");
        }
        writeln!(writer, "{header}")?;
    }

    for entry in &entries {
        let date = entry
            .creation_date
            .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_default();
        let region = entry.region.as_deref().unwrap_or("");
        if config.display_config.json {
            let mut map = serde_json::Map::new();
            map.insert("Name".to_string(), serde_json::Value::String(entry.name.clone()));
            if let Some(d) = entry.creation_date {
                map.insert("CreationDate".to_string(), serde_json::Value::String(d.to_rfc3339()));
            }
            if let Some(ref r) = entry.region {
                map.insert("BucketRegion".to_string(), serde_json::Value::String(r.clone()));
            }
            if show_bucket_arn
                && let Some(ref arn) = entry.bucket_arn
            {
                map.insert("BucketArn".to_string(), serde_json::Value::String(arn.clone()));
            }
            if show_owner {
                let owner_id = entry.owner_id.as_ref();
                let owner_name = entry.owner_display_name.as_ref();
                if owner_id.is_some() || owner_name.is_some() {
                    let mut owner = serde_json::Map::new();
                    if let Some(name) = owner_name {
                        owner.insert("DisplayName".to_string(), serde_json::Value::String(name.clone()));
                    }
                    if let Some(id) = owner_id {
                        owner.insert("ID".to_string(), serde_json::Value::String(id.clone()));
                    }
                    map.insert("Owner".to_string(), serde_json::Value::Object(owner));
                }
            }
            writeln!(writer, "{}", serde_json::to_string(&map).unwrap())?;
        } else {
            let mut line = format!("{date}\t{region}\t{}", entry.name);
            if show_bucket_arn {
                line.push_str(&format!("\t{}", entry.bucket_arn.as_deref().unwrap_or("")));
            }
            if show_owner {
                line.push_str(&format!(
                    "\t{}\t{}",
                    entry.owner_display_name.as_deref().unwrap_or(""),
                    entry.owner_id.as_deref().unwrap_or("")
                ));
            }
            writeln!(writer, "{line}")?;
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
            .map_err(|e| anyhow::anyhow!("Failed to list buckets: {e}"))?;

        let owner_display_name = resp.owner().and_then(|o| o.display_name()).map(|s| s.to_string());
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

        match resp.continuation_token() {
            Some(token) if !token.is_empty() => {
                continuation_token = Some(token.to_string());
            }
            _ => break,
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

        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list directory buckets: {e}"))?;

        for b in resp.buckets() {
            entries.push(BucketEntry {
                name: b.name().unwrap_or_default().to_string(),
                creation_date: b.creation_date().and_then(aws_datetime_to_chrono),
                region: b.bucket_region().map(|r| r.to_string()),
                bucket_arn: None,
                owner_display_name: None,
                owner_id: None,
            });
        }

        if resp.continuation_token().is_some() {
            continuation_token = resp.continuation_token().map(|s| s.to_string());
        } else {
            break;
        }
    }

    Ok(entries)
}

fn aws_datetime_to_chrono(dt: &aws_smithy_types::DateTime) -> Option<DateTime<Utc>> {
    let epoch_secs = dt.secs();
    chrono::DateTime::from_timestamp(epoch_secs, dt.subsec_nanos())
}
