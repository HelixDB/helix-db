//! The profile's one temporal type: a UTC instant with millisecond precision,
//! held by [`super::Value::DateTime`] as epoch milliseconds, the same
//! representation stored datetime properties use. Text is read in one shared
//! grammar by Cypher's `datetime(text)`, SQL `TIMESTAMP '…'` literals and
//! `CAST(… AS TIMESTAMP)`, and written in the RFC 3339 form native query
//! responses use for stored datetimes.
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, SecondsFormat, Utc};

/// Upper bound on [`format_datetime`]'s output, `+262142-12-31T23:59:59.999Z`.
pub const MAX_DATETIME_TEXT_BYTES: usize = 27;

/// UTC epoch milliseconds of datetime text, or `None` when the text is not one
/// of these forms:
///
/// - RFC 3339 with a `Z` or `±hh:mm` offset, normalized to UTC;
/// - a date and time without an offset, read as UTC;
/// - a date alone, read as UTC midnight.
///
/// The date and time may be separated by `T` or a space, seconds may carry a
/// fraction, and digits past milliseconds are truncated. No other whitespace
/// is allowed.
///
/// ```
/// use helix_planner::relational::parse_datetime;
/// assert_eq!(parse_datetime("1970-01-01T00:00:01.5Z"), Some(1_500));
/// assert_eq!(parse_datetime("1970-01-01T02:00:00+02:00"), Some(0));
/// assert_eq!(parse_datetime("1970-01-01 00:00:00.0019"), Some(1));
/// assert_eq!(parse_datetime("1970-01-02"), Some(86_400_000));
/// assert_eq!(parse_datetime("1970-01-01T00:00:00+0100"), None);
/// assert_eq!(parse_datetime("yesterday"), None);
/// ```
pub fn parse_datetime(text: &str) -> Option<i64> {
    // RFC 3339 allows a space between date and time; chrono's naive formats
    // need one spelling, so a space separator reads as `T`.
    let spaced = text.len() > 10 && text.as_bytes()[10] == b' ';
    let normalized = spaced.then(|| format!("{}T{}", &text[..10], &text[11..]));
    let text = normalized.as_deref().unwrap_or(text);
    // chrono skips whitespace before numeric fields; this grammar has none.
    if !text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || b"-+:.TtZz".contains(&byte))
    {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|instant| instant.timestamp_millis())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|instant| instant.and_utc().timestamp_millis())
        })
        .or_else(|_| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map(|date| date.and_time(NaiveTime::MIN).and_utc().timestamp_millis())
        })
        .ok()
}

/// RFC 3339 UTC text with milliseconds, byte for byte the encoding of a
/// stored datetime in native query responses, or `None` past chrono's range
/// of years, which no RFC 3339 text can express.
///
/// ```
/// use helix_planner::relational::format_datetime;
/// assert_eq!(format_datetime(0).as_deref(), Some("1970-01-01T00:00:00.000Z"));
/// assert_eq!(format_datetime(-1).as_deref(), Some("1969-12-31T23:59:59.999Z"));
/// assert_eq!(format_datetime(i64::MAX), None);
/// ```
pub fn format_datetime(millis: i64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|instant| instant.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_accepted_form_reads_the_same_instant() {
        let instant = 1_700_000_000_123;
        for text in [
            "2023-11-14T22:13:20.123Z",
            "2023-11-14t22:13:20.123z",
            "2023-11-14 22:13:20.123Z",
            "2023-11-15T00:13:20.123+02:00",
            "2023-11-14T21:13:20.123-01:00",
            "2023-11-14T22:13:20.123",
            "2023-11-14 22:13:20.123",
            "2023-11-14T22:13:20.123999999Z",
        ] {
            assert_eq!(parse_datetime(text), Some(instant), "{text}");
        }
        assert_eq!(parse_datetime("2023-11-14T22:13:20"), Some(instant - 123));
        assert_eq!(parse_datetime("2023-11-14"), Some(instant - 80_000_123));
    }

    #[test]
    fn text_outside_the_grammar_is_rejected() {
        for text in [
            "",
            " ",
            "2023-11-14T22:13",
            "2023-11-14T22:13:20+0200",
            "2023-11-14T22:13:20+02:00[Europe/Paris]",
            "2023-11-14T25:00:00Z",
            "2023-02-30",
            "2023-11-14Tx",
            " 2023-11-14",
            "2023-11-14 ",
            "2023-11-14  22:13:20",
            "20231114",
            "1700000000123",
            "-999999999-01-01",
            "2023-11-14é",
            "2023-11-14\u{e9}22:13:20",
            "2023-11-14T 22:13:20",
            "2023- 11-14",
            "2023-11-14T22:13:20 Z",
            "\t2023-11-14",
        ] {
            assert_eq!(parse_datetime(text), None, "{text:?}");
        }
    }

    #[test]
    fn formatting_round_trips_every_renderable_instant() {
        for millis in [
            0,
            -1,
            1,
            1_700_000_000_123,
            -62_135_596_800_000,
            253_402_300_799_999,
        ] {
            let text = format_datetime(millis).unwrap();
            assert!(text.len() <= MAX_DATETIME_TEXT_BYTES, "{text}");
            assert_eq!(parse_datetime(&text), Some(millis), "{text}");
        }
        let earliest = chrono::DateTime::<Utc>::MIN_UTC.timestamp_millis();
        let latest = chrono::DateTime::<Utc>::MAX_UTC.timestamp_millis();
        for millis in [earliest, latest] {
            let text = format_datetime(millis).unwrap();
            assert!(text.len() <= MAX_DATETIME_TEXT_BYTES, "{text}");
        }
        for millis in [earliest - 1, latest + 1, i64::MIN, i64::MAX] {
            assert_eq!(format_datetime(millis), None, "{millis}");
        }
    }
}
