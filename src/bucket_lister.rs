use crate::config::args::SortField;
use crate::config::Config;
use anyhow::Result;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};
use std::io::Write;

struct BucketEntry {
    name: String,
    creation_date: Option<DateTime<Utc>>,
}

pub async fn list_buckets(config: &Config) -> Result<()> {
    let client_config = config
        .target_client_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No client configuration provided"))?;

    let client = client_config.create_client().await;

    let mut entries = if config.list_express_one_zone_bucket {
        list_directory_buckets(&client).await?
    } else {
        list_general_buckets(&client).await?
    };

    // Sort
    entries.sort_by(|a, b| {
        let mut cmp = std::cmp::Ordering::Equal;
        for field in &config.sort {
            cmp = cmp.then_with(|| match field {
                SortField::Bucket | SortField::Key => a.name.cmp(&b.name),
                SortField::Date => a.creation_date.cmp(&b.creation_date),
                SortField::Size => std::cmp::Ordering::Equal,
            });
        }
        if config.reverse { cmp.reverse() } else { cmp }
    });

    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    if config.display_config.header && !config.display_config.json {
        writeln!(writer, "DATE\tBUCKET")?;
    }

    for entry in &entries {
        let date = entry
            .creation_date
            .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .unwrap_or_default();
        if config.display_config.json {
            let mut map = serde_json::Map::new();
            map.insert("bucket".to_string(), serde_json::Value::String(entry.name.clone()));
            if let Some(d) = entry.creation_date {
                map.insert("creation_date".to_string(), serde_json::Value::String(d.to_rfc3339()));
            }
            writeln!(writer, "{}", serde_json::to_string(&map).unwrap())?;
        } else {
            writeln!(writer, "{date}\t{}", entry.name)?;
        }
    }

    writer.flush()?;
    Ok(())
}

async fn list_general_buckets(client: &Client) -> Result<Vec<BucketEntry>> {
    let resp = client
        .list_buckets()
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list buckets: {e}"))?;

    let entries = resp
        .buckets()
        .iter()
        .map(|b| BucketEntry {
            name: b.name().unwrap_or_default().to_string(),
            creation_date: b.creation_date().and_then(aws_datetime_to_chrono),
        })
        .collect();

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
