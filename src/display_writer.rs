use std::io::Write;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::display::EntryFormatter;
use crate::types::ListingStatistics;
use crate::types::token::PipelineCancellationToken;

pub enum DisplayMessage {
    Entry(Box<crate::types::ListEntry>),
    Summary(ListingStatistics),
}

pub struct DisplayWriterConfig {
    pub header: bool,
    pub cancellation_token: PipelineCancellationToken,
}

pub struct DisplayWriter<W: Write + Send + 'static> {
    rx: mpsc::Receiver<DisplayMessage>,
    writer: W,
    formatter: Box<dyn EntryFormatter>,
    config: DisplayWriterConfig,
}

impl<W: Write + Send + 'static> DisplayWriter<W> {
    pub fn new(
        rx: mpsc::Receiver<DisplayMessage>,
        writer: W,
        formatter: Box<dyn EntryFormatter>,
        config: DisplayWriterConfig,
    ) -> Self {
        Self {
            rx,
            writer,
            formatter,
            config,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        if self.config.header
            && let Some(header) = self.formatter.format_header()
        {
            writeln!(self.writer, "{header}")?;
        }

        while let Some(msg) = self.rx.recv().await {
            if self.config.cancellation_token.is_cancelled() {
                self.writer.flush()?;
                return Ok(());
            }
            match msg {
                DisplayMessage::Entry(entry) => {
                    writeln!(self.writer, "{}", self.formatter.format_entry(&entry))?;
                }
                DisplayMessage::Summary(stats) => {
                    writeln!(self.writer, "{}", self.formatter.format_summary(&stats))?;
                }
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::FormatOptions;
    use crate::display::json::JsonFormatter;
    use crate::display::tsv::TsvFormatter;
    use crate::types::{ListEntry, ListingStatistics, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64) -> ListEntry {
        ListEntry::Object(S3Object {
            key: key.to_string(),
            size,
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
        })
    }

    #[derive(Clone)]
    struct SharedBuf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl SharedBuf {
        fn new() -> Self {
            Self(std::sync::Arc::new(std::sync::Mutex::new(Vec::new())))
        }
        fn as_string(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn formatter_writes_entries() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let buf = SharedBuf::new();
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = DisplayWriterConfig {
            header: false,
            cancellation_token: token,
        };
        let formatter: Box<dyn EntryFormatter> =
            Box::new(TsvFormatter::new(FormatOptions::default()));
        let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

        tx.send(DisplayMessage::Entry(Box::new(make_entry("hello.txt", 42))))
            .await
            .unwrap();
        drop(tx);

        display_writer.run().await.unwrap();
        let output = buf.as_string();
        assert!(
            output.contains("hello.txt"),
            "expected key in output, got: {output:?}"
        );
    }

    #[tokio::test]
    async fn formatter_writes_header_when_configured() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let buf = SharedBuf::new();
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = DisplayWriterConfig {
            header: true,
            cancellation_token: token,
        };
        let formatter: Box<dyn EntryFormatter> =
            Box::new(TsvFormatter::new(FormatOptions::default()));
        let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

        drop(tx);
        display_writer.run().await.unwrap();
        let output = buf.as_string();
        assert!(
            output.starts_with("DATE\t"),
            "expected header starting with DATE, got: {output:?}"
        );
    }

    #[tokio::test]
    async fn formatter_writes_summary() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let buf = SharedBuf::new();
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = DisplayWriterConfig {
            header: false,
            cancellation_token: token,
        };
        let formatter: Box<dyn EntryFormatter> =
            Box::new(TsvFormatter::new(FormatOptions::default()));
        let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

        tx.send(DisplayMessage::Entry(Box::new(make_entry("a.txt", 100))))
            .await
            .unwrap();
        tx.send(DisplayMessage::Summary(ListingStatistics {
            total_objects: 1,
            total_size: 100,
            total_delete_markers: 0,
        }))
        .await
        .unwrap();
        drop(tx);

        display_writer.run().await.unwrap();
        let output = buf.as_string();
        assert!(
            output.contains("Total:"),
            "expected summary in output, got: {output:?}"
        );
    }

    #[tokio::test]
    async fn formatter_writes_json() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let buf = SharedBuf::new();
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = DisplayWriterConfig {
            header: false,
            cancellation_token: token,
        };
        let formatter: Box<dyn EntryFormatter> =
            Box::new(JsonFormatter::new(FormatOptions::default()));
        let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

        tx.send(DisplayMessage::Entry(Box::new(make_entry("test.json", 50))))
            .await
            .unwrap();
        drop(tx);

        display_writer.run().await.unwrap();
        let output = buf.as_string();
        let parsed: serde_json::Value =
            serde_json::from_str(output.trim()).expect("output should be valid JSON");
        assert_eq!(parsed["Key"], "test.json");
    }

    #[tokio::test]
    async fn formatter_skips_output_on_cancellation() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let buf = SharedBuf::new();
        let token = crate::types::token::create_pipeline_cancellation_token();
        token.cancel();
        let config = DisplayWriterConfig {
            header: false,
            cancellation_token: token,
        };
        let formatter: Box<dyn EntryFormatter> =
            Box::new(TsvFormatter::new(FormatOptions::default()));
        let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

        tx.send(DisplayMessage::Entry(Box::new(make_entry(
            "should_not_appear.txt",
            1,
        ))))
        .await
        .unwrap();
        drop(tx);

        display_writer.run().await.unwrap();
        let output = buf.as_string();
        assert!(
            output.is_empty(),
            "expected empty output on cancellation, got: {output:?}"
        );
    }

    #[tokio::test]
    async fn display_writer_json_skips_header() {
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let buf = SharedBuf::new();
        let token = crate::types::token::create_pipeline_cancellation_token();
        let config = DisplayWriterConfig {
            header: true,
            cancellation_token: token,
        };
        let formatter: Box<dyn EntryFormatter> =
            Box::new(JsonFormatter::new(FormatOptions::default()));
        let display_writer = DisplayWriter::new(rx, buf.clone(), formatter, config);

        drop(tx);
        display_writer.run().await.unwrap();
        let output = buf.as_string();
        assert!(
            output.is_empty(),
            "expected empty output when JsonFormatter skips header, got: {output:?}"
        );
    }
}
