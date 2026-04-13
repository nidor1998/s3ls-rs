use crate::types::token::PipelineCancellationToken;
use crate::types::{ListEntry, ListingStatistics};

pub enum FormatterMessage {
    Entry(Box<ListEntry>),
    Summary(ListingStatistics),
}

pub struct FormatterConfig {
    pub use_json: bool,
    pub human: bool,
    pub all_versions: bool,
    pub header: bool,
    pub cancellation_token: PipelineCancellationToken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ListEntry, ListingStatistics, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
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
        })
    }

    #[test]
    fn formatter_message_entry_variant() {
        let msg = FormatterMessage::Entry(Box::new(make_entry("a.txt", 100)));
        assert!(matches!(msg, FormatterMessage::Entry(_)));
    }

    #[test]
    fn formatter_message_summary_variant() {
        let stats = ListingStatistics {
            total_objects: 10,
            total_size: 1024,
            total_delete_markers: 0,
        };
        let msg = FormatterMessage::Summary(stats);
        assert!(matches!(msg, FormatterMessage::Summary(_)));
    }
}
