//! Date-time helpers built on the `time` crate.
//!
//! Adapted from s3sync's / s3rm-rs's `types/datetime.rs`. The string formats
//! produced here intentionally match what `chrono` produced before the
//! migration, so listing output and log output stay unchanged.

use std::fmt::Write;

use time::format_description::well_known::Rfc3339;
use time::{OffsetDateTime, UtcOffset};

/// Parses an RFC 3339 date-time (any offset) and normalizes it to UTC.
///
/// The date/time separator must be `T`, `t`, or a space (as RFC 3339 allows);
/// `time` itself accepts any byte there.
///
/// Returns an error (rather than panicking) if the value is out of range after
/// conversion to UTC.
pub fn parse_rfc3339_utc(value: &str) -> Result<OffsetDateTime, String> {
    if !matches!(value.as_bytes().get(10), Some(b'T' | b't' | b' ')) {
        return Err(format!(
            "invalid RFC 3339 date-time (expected 'T' between date and time): {value}"
        ));
    }
    let dt = OffsetDateTime::parse(value, &Rfc3339).map_err(|e| e.to_string())?;
    dt.checked_to_offset(UtcOffset::UTC)
        .ok_or_else(|| format!("date-time out of range after conversion to UTC: {value}"))
}

/// Converts an AWS SDK `DateTime` to an [`OffsetDateTime`] in UTC.
///
/// Returns `None` if the value is outside the range of [`OffsetDateTime`]
/// (years -9999 to 9999).
pub fn from_aws_datetime(dt: &aws_smithy_types::DateTime) -> Option<OffsetDateTime> {
    let nanos = i128::from(dt.secs()) * 1_000_000_000 + i128::from(dt.subsec_nanos());
    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

/// Formats a timestamp for listing output, with whole-second precision.
///
/// - `local == false`: UTC with a `Z` suffix, e.g. `2024-01-01T00:00:00Z`.
/// - `local == true`: the system's local time with an explicit numeric
///   offset (never `Z`), e.g. `2024-01-01T09:00:00+09:00`. The offset is
///   looked up for that instant, so daylight saving time is honored. If the
///   local offset cannot be determined, UTC is used (`+00:00`).
pub fn format_rfc3339_secs(dt: &OffsetDateTime, local: bool) -> String {
    let utc = dt.checked_to_offset(UtcOffset::UTC).unwrap_or(*dt);
    if local {
        let offset = UtcOffset::local_offset_at(utc).unwrap_or(UtcOffset::UTC);
        let local_dt = utc.checked_to_offset(offset).unwrap_or(utc);
        write_rfc3339(&local_dt, false, false)
    } else {
        write_rfc3339(&utc, false, true)
    }
}

/// RFC 3339 formatting helper for [`OffsetDateTime`].
///
/// Negative years are written with an explicit sign and at least four digits
/// (e.g. `-0001`), matching `chrono`. Such output is an extension of RFC 3339,
/// which only allows four-digit non-negative years.
///
/// Values are converted to UTC. If that conversion would go out of range, the
/// value is formatted in its own offset instead of panicking.
pub trait Rfc3339Ext {
    /// Formats as RFC 3339 in UTC with `+00:00` and 0, 3, 6, or 9 fractional digits.
    /// e.g. `2023-01-20T17:56:37.552+00:00`
    fn to_rfc3339(&self) -> String;
}

impl Rfc3339Ext for OffsetDateTime {
    fn to_rfc3339(&self) -> String {
        let dt = self.checked_to_offset(UtcOffset::UTC).unwrap_or(*self);
        write_rfc3339(&dt, true, false)
    }
}

/// Writes `dt` as RFC 3339 in its own offset.
///
/// `fraction`: append 0, 3, 6, or 9 fractional digits (as `chrono`'s
/// `to_rfc3339`); otherwise the sub-second part is truncated.
/// `use_z`: write a zero offset as `Z` instead of `+00:00`.
fn write_rfc3339(dt: &OffsetDateTime, fraction: bool, use_z: bool) -> String {
    let year = dt.year();
    let mut s = String::with_capacity(35);
    if (0..=9999).contains(&year) {
        let _ = write!(s, "{year:04}");
    } else {
        let _ = write!(s, "{year:+05}");
    }
    let _ = write!(
        s,
        "-{:02}-{:02}T{:02}:{:02}:{:02}",
        u8::from(dt.month()),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );

    let nanos = dt.nanosecond();
    if fraction && nanos != 0 {
        if nanos.is_multiple_of(1_000_000) {
            let _ = write!(s, ".{:03}", nanos / 1_000_000);
        } else if nanos.is_multiple_of(1_000) {
            let _ = write!(s, ".{:06}", nanos / 1_000);
        } else {
            let _ = write!(s, ".{nanos:09}");
        }
    }

    let offset = dt.offset();
    if use_z && offset.is_utc() {
        s.push('Z');
    } else {
        let (hours, minutes, _) = offset.as_hms();
        let sign = if offset.is_negative() { '-' } else { '+' };
        let _ = write!(
            s,
            "{sign}{:02}:{:02}",
            hours.unsigned_abs(),
            minutes.unsigned_abs()
        );
    }
    s
}

/// Builds a UTC date-time from its parts. Panics on invalid input (tests only).
#[cfg(test)]
pub(crate) fn utc_ymd_hms(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> OffsetDateTime {
    let month = time::Month::try_from(u8::try_from(month).unwrap()).unwrap();
    time::Date::from_calendar_date(year, month, u8::try_from(day).unwrap())
        .unwrap()
        .with_hms(
            u8::try_from(hour).unwrap(),
            u8::try_from(minute).unwrap(),
            u8::try_from(second).unwrap(),
        )
        .unwrap()
        .assume_utc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_utc() {
        let dt = parse_rfc3339_utc("2023-01-20T17:56:37.552Z").unwrap();
        assert_eq!(dt.offset(), UtcOffset::UTC);
        assert_eq!(dt.to_rfc3339(), "2023-01-20T17:56:37.552+00:00");
    }

    #[test]
    fn parse_with_offset_normalizes_to_utc() {
        let dt = parse_rfc3339_utc("2023-04-20T21:40:11.111+09:00").unwrap();
        assert_eq!(dt.offset(), UtcOffset::UTC);
        assert_eq!(dt.to_rfc3339(), "2023-04-20T12:40:11.111+00:00");
    }

    #[test]
    fn parse_space_and_lowercase_separator() {
        assert_eq!(
            parse_rfc3339_utc("2023-02-19 12:00:00Z")
                .unwrap()
                .to_rfc3339(),
            "2023-02-19T12:00:00+00:00"
        );
        assert!(parse_rfc3339_utc("2023-02-19t12:00:00Z").is_ok());
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_rfc3339_utc("").is_err());
        assert!(parse_rfc3339_utc("2023-02-19").is_err());
        assert!(parse_rfc3339_utc("not a date").is_err());
        assert!(parse_rfc3339_utc("2023-02-19T12:00:00").is_err());
        assert!(parse_rfc3339_utc("2023-02-19X12:00:00Z").is_err());
        assert!(parse_rfc3339_utc("2023-02-19_12:00:00Z").is_err());
    }

    #[test]
    fn parse_max_date_with_negative_offset_does_not_panic() {
        // Out of range once converted to UTC (year 10000).
        assert!(parse_rfc3339_utc("9999-12-31T23:59:59-01:00").is_err());
    }

    #[test]
    fn parse_leap_second() {
        // `time` accepts a leap second only at the end of a UTC month and
        // clamps it to the last representable instant of that minute.
        assert_eq!(
            parse_rfc3339_utc("2016-12-31T23:59:60Z")
                .unwrap()
                .to_rfc3339(),
            "2016-12-31T23:59:59.999999999+00:00"
        );
        assert!(parse_rfc3339_utc("2016-12-31T12:00:60Z").is_err());
    }

    #[test]
    fn to_rfc3339_fraction_digits() {
        let base = OffsetDateTime::from_unix_timestamp(0).unwrap();
        assert_eq!(base.to_rfc3339(), "1970-01-01T00:00:00+00:00");
        assert_eq!(
            base.replace_millisecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.005+00:00"
        );
        assert_eq!(
            base.replace_microsecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.000005+00:00"
        );
        assert_eq!(
            base.replace_nanosecond(5).unwrap().to_rfc3339(),
            "1970-01-01T00:00:00.000000005+00:00"
        );
    }

    #[test]
    fn negative_year() {
        let dt = parse_rfc3339_utc("0000-01-01T00:00:00Z").unwrap() - time::Duration::days(1);
        assert_eq!(dt.to_rfc3339(), "-0001-12-31T00:00:00+00:00");
        assert_eq!(format_rfc3339_secs(&dt, false), "-0001-12-31T00:00:00Z");
    }

    #[test]
    fn to_rfc3339_out_of_range_utc_does_not_panic() {
        let max =
            time::PrimitiveDateTime::MAX.assume_offset(UtcOffset::from_hms(-1, -30, 0).unwrap());
        assert!(max.checked_to_offset(UtcOffset::UTC).is_none());
        assert_eq!(max.to_rfc3339(), "9999-12-31T23:59:59.999999999-01:30");
        // Must not panic in either mode.
        let _ = format_rfc3339_secs(&max, false);
        let _ = format_rfc3339_secs(&max, true);
    }

    #[test]
    fn format_secs_utc_uses_z_and_truncates_fraction() {
        let dt = parse_rfc3339_utc("2024-01-01T09:00:00.999+09:00").unwrap();
        assert_eq!(format_rfc3339_secs(&dt, false), "2024-01-01T00:00:00Z");
    }

    #[test]
    fn format_secs_local_has_numeric_offset_and_same_instant() {
        let dt = utc_ymd_hms(2024, 7, 1, 12, 34, 56);
        let local = format_rfc3339_secs(&dt, true);
        assert!(!local.ends_with('Z'), "got: {local}");
        assert_eq!(parse_rfc3339_utc(&local).unwrap(), dt);
    }

    #[test]
    fn format_secs_local_near_max_does_not_panic() {
        let dt = time::PrimitiveDateTime::MAX.assume_utc();
        let local = format_rfc3339_secs(&dt, true);
        assert!(local.starts_with("9999-12-31T") || local.starts_with("10000"));
    }

    #[test]
    fn http_date_rejects_year_10000() {
        // Guards against enabling `time`'s `large-dates` feature: Cargo feature
        // unification would turn it on inside aws-smithy-types too, and its
        // HTTP-date formatter relies on years >= 10000 being rejected. With the
        // feature on, this formats a malformed date (release) or panics (debug).
        use aws_smithy_types::date_time::Format;
        assert!(
            aws_smithy_types::DateTime::from_secs(253_402_300_800)
                .fmt(Format::HttpDate)
                .is_err()
        );
    }

    #[test]
    fn from_aws_datetime_range() {
        let dt = aws_smithy_types::DateTime::from_millis(1_674_237_397_552);
        assert_eq!(
            from_aws_datetime(&dt).unwrap().to_rfc3339(),
            "2023-01-20T17:56:37.552+00:00"
        );
        let dt = aws_smithy_types::DateTime::from_millis(-1);
        assert_eq!(
            from_aws_datetime(&dt).unwrap().to_rfc3339(),
            "1969-12-31T23:59:59.999+00:00"
        );
        // 10000-01-01T00:00:00Z is beyond `OffsetDateTime`'s range.
        let dt = aws_smithy_types::DateTime::from_secs(253_402_300_800);
        assert!(from_aws_datetime(&dt).is_none());
        let dt = aws_smithy_types::DateTime::from_secs(i64::MAX);
        assert!(from_aws_datetime(&dt).is_none());
    }

    #[test]
    fn utc_ymd_hms_builds_utc() {
        assert_eq!(
            utc_ymd_hms(2024, 1, 16, 9, 0, 0).to_rfc3339(),
            "2024-01-16T09:00:00+00:00"
        );
    }
}
