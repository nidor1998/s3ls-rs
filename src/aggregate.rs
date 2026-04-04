use crate::config::args::SortField;
use crate::types::ListEntry;

pub fn sort_entries(
    entries: &mut [ListEntry],
    field: &SortField,
    reverse: bool,
    all_versions: bool,
) {
    entries.sort_by(|a, b| {
        let cmp = match field {
            SortField::Key => {
                let primary = a.key().cmp(b.key());
                if all_versions && primary == std::cmp::Ordering::Equal {
                    let a_time = a.last_modified();
                    let b_time = b.last_modified();
                    match (a_time, b_time) {
                        (Some(at), Some(bt)) => at.cmp(bt),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                } else {
                    primary
                }
            }
            SortField::Size => a.size().cmp(&b.size()),
            SortField::Date => {
                let a_time = a.last_modified();
                let b_time = b.last_modified();
                match (a_time, b_time) {
                    (Some(at), Some(bt)) => at.cmp(bt),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            }
        };
        if reverse { cmp.reverse() } else { cmp }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::SortField;
    use crate::types::{ListEntry, S3Object};
    use chrono::TimeZone;

    fn make_entry(key: &str, size: u64, year: i32, month: u32) -> ListEntry {
        ListEntry::Object(S3Object::NotVersioning {
            key: key.to_string(),
            size,
            last_modified: chrono::Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap(),
            e_tag: "\"e\"".to_string(),
            storage_class: Some("STANDARD".to_string()),
            checksum_algorithm: None,
            checksum_type: None,
        })
    }

    #[test]
    fn sort_by_key() {
        let mut entries = vec![
            make_entry("c.txt", 100, 2024, 1),
            make_entry("a.txt", 200, 2024, 2),
            make_entry("b.txt", 300, 2024, 3),
        ];
        sort_entries(&mut entries, &SortField::Key, false, false);
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[1].key(), "b.txt");
        assert_eq!(entries[2].key(), "c.txt");
    }

    #[test]
    fn sort_by_key_with_all_versions_secondary_mtime() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("a.txt", 200, 2024, 1),
            make_entry("b.txt", 300, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Key, false, true);
        assert_eq!(entries[0].key(), "a.txt");
        assert_eq!(entries[0].size(), 200); // Jan entry first
        assert_eq!(entries[1].key(), "a.txt");
        assert_eq!(entries[1].size(), 100); // Mar entry second
        assert_eq!(entries[2].key(), "b.txt");
    }

    #[test]
    fn sort_by_size() {
        let mut entries = vec![
            make_entry("a.txt", 300, 2024, 1),
            make_entry("b.txt", 100, 2024, 2),
            make_entry("c.txt", 200, 2024, 3),
        ];
        sort_entries(&mut entries, &SortField::Size, false, false);
        assert_eq!(entries[0].size(), 100);
        assert_eq!(entries[1].size(), 200);
        assert_eq!(entries[2].size(), 300);
    }

    #[test]
    fn sort_by_date() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 3),
            make_entry("b.txt", 100, 2024, 1),
            make_entry("c.txt", 100, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Date, false, false);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "c.txt");
        assert_eq!(entries[2].key(), "a.txt");
    }

    #[test]
    fn sort_reverse() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            make_entry("b.txt", 200, 2024, 2),
        ];
        sort_entries(&mut entries, &SortField::Key, true, false);
        assert_eq!(entries[0].key(), "b.txt");
        assert_eq!(entries[1].key(), "a.txt");
    }

    #[test]
    fn sort_common_prefix_by_size_sorts_as_zero() {
        let mut entries = vec![
            make_entry("a.txt", 100, 2024, 1),
            ListEntry::CommonPrefix("logs/".to_string()),
        ];
        sort_entries(&mut entries, &SortField::Size, false, false);
        assert_eq!(entries[0].key(), "logs/");
        assert_eq!(entries[1].key(), "a.txt");
    }
}
