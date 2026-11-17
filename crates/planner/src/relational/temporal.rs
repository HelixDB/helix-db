//! The profile's one temporal type: a UTC instant with millisecond precision,
//! held by [`super::Value::DateTime`] as epoch milliseconds, the same
//! representation stored datetime properties use. Text is read in one shared
//! grammar by Cypher's `datetime(text)`, SQL `TIMESTAMP '…'` literals and
//! `CAST(… AS TIMESTAMP)`, and written in the RFC 3339 form native query
//! responses use for stored datetimes.
use chrono::{NaiveDate, NaiveTime, SecondsFormat, Utc};

/// Upper bound on [`format_datetime`]'s output, `+262142-12-31T23:59:59.999Z`.
pub const MAX_DATETIME_TEXT_BYTES: usize = 27;

/// Why [`parse_datetime`] read no instant from some text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatetimeTextError {
    /// The text is not one of the grammar's forms, such as an ISO 8601 week
    /// date, a zone name, an offset without a colon, a year outside
    /// `0000`–`9999`, or a field that is not zero-padded.
    OutsideGrammar,
    /// The text has the grammar's form but names no instant, such as
    /// February 30, hour 24, a leap second, or an offset of 24 hours.
    NoSuchInstant,
}

/// UTC epoch milliseconds of datetime text in one of these forms, each field
/// zero-padded to its full width:
///
/// - RFC 3339, `YYYY-MM-DDTHH:MM:SS[.f…]` with a `Z` or `±HH:MM` offset,
///   normalized to UTC;
/// - the same date and time without an offset, read as UTC;
/// - a date alone, `YYYY-MM-DD`, read as UTC midnight.
///
/// The date and time may be separated by `T`, `t` or one space, `Z` may be
/// `z`, and digits past milliseconds are truncated. Years are `0000`–`9999`,
/// the range RFC 3339 allows, so [`format_datetime`] renders every instant
/// this reads as text this reads back to the same instant.
///
/// ```
/// use helix_planner::relational::{parse_datetime, DatetimeTextError};
/// assert_eq!(parse_datetime("1970-01-01T00:00:01.5Z"), Ok(1_500));
/// assert_eq!(parse_datetime("1970-01-01T02:00:00+02:00"), Ok(0));
/// assert_eq!(parse_datetime("1970-01-01 00:00:00.0019"), Ok(1));
/// assert_eq!(parse_datetime("1970-01-02"), Ok(86_400_000));
/// assert_eq!(
///     parse_datetime("1970-01-01T00:00:00+0100"),
///     Err(DatetimeTextError::OutsideGrammar)
/// );
/// assert_eq!(parse_datetime("1970-1-1"), Err(DatetimeTextError::OutsideGrammar));
/// assert_eq!(parse_datetime("1970-02-30"), Err(DatetimeTextError::NoSuchInstant));
/// ```
pub fn parse_datetime(text: &str) -> Result<i64, DatetimeTextError> {
    use DatetimeTextError as Error;
    let bytes = text.as_bytes();
    // Whether `template` matches the bytes at `from`: `d` is an ASCII digit
    // and `T` the date-time separator.
    let fits = |template: &[u8], from: usize| {
        bytes.get(from..from + template.len()).is_some_and(|slice| {
            slice
                .iter()
                .zip(template)
                .all(|(&byte, &shape)| match shape {
                    b'd' => byte.is_ascii_digit(),
                    b'T' => matches!(byte, b'T' | b't' | b' '),
                    _ => byte == shape,
                })
        })
    };
    // Only ever called on bytes `fits` matched as digits.
    let digit = |byte: &u8| u32::from(byte - b'0');
    let number = |range: std::ops::Range<usize>| {
        bytes[range]
            .iter()
            .fold(0, |value, byte| value * 10 + digit(byte))
    };
    if !fits(b"dddd-dd-dd", 0) {
        return Err(Error::OutsideGrammar);
    }
    // At most 9999, so the year fits an `i32`.
    let date = NaiveDate::from_ymd_opt(number(0..4) as i32, number(5..7), number(8..10));
    if bytes.len() == 10 {
        let date = date.ok_or(Error::NoSuchInstant)?;
        return Ok(date.and_time(NaiveTime::MIN).and_utc().timestamp_millis());
    }
    if !fits(b"Tdd:dd:dd", 10) {
        return Err(Error::OutsideGrammar);
    }
    // The digits after a fraction's `.`, which needs at least one, and the
    // offset after them.
    let (fraction, offset) = match bytes[19..].strip_prefix(b".") {
        None => (&[][..], &bytes[19..]),
        Some(rest) => match rest.iter().take_while(|byte| byte.is_ascii_digit()).count() {
            0 => return Err(Error::OutsideGrammar),
            digits => rest.split_at(digits),
        },
    };
    let offset_seconds = match offset {
        [] | [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2]
            if [h1, h2, m1, m2].iter().all(|byte| byte.is_ascii_digit()) =>
        {
            let (hours, minutes) = (digit(h1) * 10 + digit(h2), digit(m1) * 10 + digit(m2));
            if hours > 23 || minutes > 59 {
                return Err(Error::NoSuchInstant);
            }
            let seconds = i64::from(hours * 3600 + minutes * 60);
            if *sign == b'-' {
                -seconds
            } else {
                seconds
            }
        }
        _ => return Err(Error::OutsideGrammar),
    };
    // The first three fraction digits, as milliseconds.
    let millis = (0..3).fold(0, |millis, at| {
        millis * 10 + fraction.get(at).map_or(0, digit)
    });
    // Chrono spells a leap second as second 59 with 1000 ms or more, so a
    // second of 60 has no time here.
    let time =
        NaiveTime::from_hms_milli_opt(number(11..13), number(14..16), number(17..19), millis);
    let (Some(date), Some(time)) = (date, time) else {
        return Err(Error::NoSuchInstant);
    };
    Ok(date.and_time(time).and_utc().timestamp_millis() - offset_seconds * 1000)
}

/// RFC 3339 UTC text with milliseconds, byte for byte the encoding of a
/// stored datetime in native query responses, or `None` past chrono's range
/// of years. Years outside `0000`–`9999` are written as native responses
/// write them, signed and with extra digits; that text is not RFC 3339 and
/// [`parse_datetime`] does not read it.
///
/// ```
/// use helix_planner::relational::{format_datetime, parse_datetime};
/// assert_eq!(format_datetime(0).as_deref(), Some("1970-01-01T00:00:00.000Z"));
/// assert_eq!(format_datetime(-1).as_deref(), Some("1969-12-31T23:59:59.999Z"));
/// let year_10000 = format_datetime(253_402_300_800_000).unwrap();
/// assert_eq!(year_10000, "+10000-01-01T00:00:00.000Z");
/// assert!(parse_datetime(&year_10000).is_err());
/// assert_eq!(format_datetime(i64::MAX), None);
/// ```
pub fn format_datetime(millis: i64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(millis).map(|instant| {
        // The formatter reserves more than it writes; callers admit the text.
        let mut text = instant.to_rfc3339_opts(SecondsFormat::Millis, true);
        text.shrink_to_fit();
        text
    })
}

/// A UTC component of a datetime that property access reads, such as
/// `dt.year`, named as openCypher names it. Weeks are ISO 8601 weeks, which
/// start on Monday, and `dayOfWeek` counts Monday as 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatetimeField {
    Year,
    Quarter,
    Month,
    Week,
    WeekYear,
    Day,
    OrdinalDay,
    DayOfWeek,
    Hour,
    Minute,
    Second,
    Millisecond,
    EpochSeconds,
    EpochMillis,
}

impl DatetimeField {
    /// The field a property key names, or `None` for any other key, which a
    /// datetime does not have.
    ///
    /// ```
    /// use helix_planner::relational::DatetimeField;
    /// assert_eq!(DatetimeField::from_key("epochMillis"), Some(DatetimeField::EpochMillis));
    /// assert_eq!(DatetimeField::from_key("Year"), None);
    /// ```
    pub fn from_key(key: &str) -> Option<Self> {
        Some(match key {
            "year" => Self::Year,
            "quarter" => Self::Quarter,
            "month" => Self::Month,
            "week" => Self::Week,
            "weekYear" => Self::WeekYear,
            "day" => Self::Day,
            "ordinalDay" => Self::OrdinalDay,
            "dayOfWeek" => Self::DayOfWeek,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "millisecond" => Self::Millisecond,
            "epochSeconds" => Self::EpochSeconds,
            "epochMillis" => Self::EpochMillis,
            _ => return None,
        })
    }

    /// The field of the instant `millis` epoch milliseconds name. Epoch
    /// fields exist for every instant; the others are `None` past chrono's
    /// range of years, where [`format_datetime`] is `None` too.
    ///
    /// ```
    /// use helix_planner::relational::DatetimeField as F;
    /// // 1969-12-31T23:59:59.999Z, a Wednesday.
    /// assert_eq!(F::Year.of(-1), Some(1969));
    /// assert_eq!(F::DayOfWeek.of(-1), Some(3));
    /// assert_eq!(F::Millisecond.of(-1), Some(999));
    /// assert_eq!(F::EpochSeconds.of(-1), Some(-1));
    /// assert_eq!(F::Year.of(i64::MAX), None);
    /// assert_eq!(F::EpochMillis.of(i64::MAX), Some(i64::MAX));
    /// ```
    pub fn of(self, millis: i64) -> Option<i64> {
        use chrono::{Datelike, Timelike};
        let instant = chrono::DateTime::<Utc>::from_timestamp_millis(millis);
        Some(match self {
            Self::EpochSeconds => millis.div_euclid(1000),
            Self::EpochMillis => millis,
            Self::Year => i64::from(instant?.year()),
            Self::Quarter => i64::from(instant?.month0() / 3 + 1),
            Self::Month => i64::from(instant?.month()),
            Self::Week => i64::from(instant?.iso_week().week()),
            Self::WeekYear => i64::from(instant?.iso_week().year()),
            Self::Day => i64::from(instant?.day()),
            Self::OrdinalDay => i64::from(instant?.ordinal()),
            Self::DayOfWeek => i64::from(instant?.weekday().number_from_monday()),
            Self::Hour => i64::from(instant?.hour()),
            Self::Minute => i64::from(instant?.minute()),
            Self::Second => i64::from(instant?.second()),
            Self::Millisecond => i64::from(instant?.timestamp_subsec_millis()),
        })
    }
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
            "2023-11-14T22:13:20.123+00:00",
            "2023-11-14T22:13:20.123-00:00",
        ] {
            assert_eq!(parse_datetime(text), Ok(instant), "{text}");
        }
        assert_eq!(parse_datetime("2023-11-14T22:13:20"), Ok(instant - 123));
        assert_eq!(parse_datetime("2023-11-14T22:13:20.1"), Ok(instant - 23));
        assert_eq!(parse_datetime("2023-11-14T22:13:20.12Z"), Ok(instant - 3));
        assert_eq!(parse_datetime("2023-11-14"), Ok(instant - 80_000_123));
        assert_eq!(
            parse_datetime("0000-01-01T00:00:00Z"),
            Ok(-62_167_219_200_000)
        );
        assert_eq!(
            parse_datetime("9999-12-31T23:59:59.999Z"),
            Ok(253_402_300_799_999)
        );
        assert_eq!(
            parse_datetime("2023-11-14T23:59:00+23:59"),
            Ok(1_699_920_000_000)
        );
    }

    #[test]
    fn text_outside_the_grammar_is_rejected() {
        for text in [
            "",
            " ",
            "2023-11-14T22:13",
            "2023-11-14T22:13:20+0200",
            "2023-11-14T22:13:20+02",
            "2023-11-14T22:13:20+02:00:00",
            "2023-11-14T22:13:20+02:00[Europe/Paris]",
            "2023-11-14T22:13:20.Z",
            "2023-11-14T22:13:20.",
            "2023-11-14T22:13:20,5Z",
            "2023-11-14T22:13:20ZZ",
            "2023-11-14Tx",
            " 2023-11-14",
            "2023-11-14 ",
            "2023-11-14T",
            "2023-11-14  22:13:20",
            "20231114",
            "1700000000123",
            "-999999999-01-01",
            "-0001-01-01",
            "+10000-01-01T00:00:00.000Z",
            "+2023-11-14",
            "10000-01-01",
            "2023-1-5",
            "2023-01-5",
            "2023-11-14T1:02:03",
            "2023-11-14T01:2:03",
            "2023-11-14T01:02:3",
            "1-01-01",
            "2015-W30-2",
            "2015-202",
            "2023-11-14é",
            "2023-11-14\u{e9}22:13:20",
            "２０２３-11-14",
            "2023-11-14T 22:13:20",
            "2023- 11-14",
            "2023-11-14T22:13:20 Z",
            "2023-11-14T22:13:20 +02:00",
            "\t2023-11-14",
            "2023/11/14",
        ] {
            assert_eq!(
                parse_datetime(text),
                Err(DatetimeTextError::OutsideGrammar),
                "{text:?}"
            );
        }
    }

    #[test]
    fn text_in_the_grammar_naming_no_instant_is_rejected() {
        for text in [
            "2023-02-29",
            "2023-02-30",
            "2023-13-01",
            "2023-00-10",
            "2023-11-00",
            "2023-11-14T24:00:00Z",
            "2023-11-14T25:00:00Z",
            "2023-11-14T22:60:00Z",
            "2016-12-31T23:59:60Z",
            "2016-12-31T23:59:60.5",
            "2023-11-14T22:13:20+24:00",
            "2023-11-14T22:13:20-02:60",
        ] {
            assert_eq!(
                parse_datetime(text),
                Err(DatetimeTextError::NoSuchInstant),
                "{text:?}"
            );
        }
        assert_eq!(parse_datetime("2024-02-29"), Ok(1_709_164_800_000));
    }

    #[test]
    fn formatting_round_trips_every_readable_instant() {
        let first = parse_datetime("0000-01-01T00:00:00.000Z").unwrap();
        let last = parse_datetime("9999-12-31T23:59:59.999Z").unwrap();
        for millis in [
            first,
            first + 1,
            -1,
            0,
            1,
            1_700_000_000_123,
            -62_135_596_800_000,
            last - 1,
            last,
        ] {
            let text = format_datetime(millis).unwrap();
            assert!(text.capacity() <= MAX_DATETIME_TEXT_BYTES, "{text}");
            assert_eq!(parse_datetime(&text), Ok(millis), "{text}");
        }
        // Instants outside the readable years still render, as native
        // responses do, in text the grammar does not read.
        let earliest = chrono::DateTime::<Utc>::MIN_UTC.timestamp_millis();
        let latest = chrono::DateTime::<Utc>::MAX_UTC.timestamp_millis();
        for millis in [first - 1, last + 1, earliest, latest] {
            let text = format_datetime(millis).unwrap();
            assert!(text.capacity() <= MAX_DATETIME_TEXT_BYTES, "{text}");
            assert_eq!(
                parse_datetime(&text),
                Err(DatetimeTextError::OutsideGrammar),
                "{text}"
            );
        }
        for millis in [earliest - 1, latest + 1, i64::MIN, i64::MAX] {
            assert_eq!(format_datetime(millis), None, "{millis}");
        }
    }

    #[test]
    fn fields_read_utc_components_and_iso_weeks() {
        use DatetimeField as F;
        // Monday 2024-12-30T13:45:06.789Z, in ISO week 1 of 2025.
        let monday = 1_735_566_306_789;
        // Friday 2021-01-01T00:00:00.000Z, in ISO week 53 of 2020.
        let friday = 1_609_459_200_000;
        for (key, field, at_monday, at_friday) in [
            ("year", F::Year, 2024, 2021),
            ("quarter", F::Quarter, 4, 1),
            ("month", F::Month, 12, 1),
            ("week", F::Week, 1, 53),
            ("weekYear", F::WeekYear, 2025, 2020),
            ("day", F::Day, 30, 1),
            ("ordinalDay", F::OrdinalDay, 365, 1),
            ("dayOfWeek", F::DayOfWeek, 1, 5),
            ("hour", F::Hour, 13, 0),
            ("minute", F::Minute, 45, 0),
            ("second", F::Second, 6, 0),
            ("millisecond", F::Millisecond, 789, 0),
            (
                "epochSeconds",
                F::EpochSeconds,
                1_735_566_306,
                1_609_459_200,
            ),
            ("epochMillis", F::EpochMillis, monday, friday),
        ] {
            assert_eq!(F::from_key(key), Some(field), "{key}");
            assert_eq!(field.of(monday), Some(at_monday), "{key}");
            assert_eq!(field.of(friday), Some(at_friday), "{key}");
            // Past chrono's years only the epoch fields exist.
            let epoch = matches!(field, F::EpochSeconds | F::EpochMillis);
            assert_eq!(field.of(i64::MIN).is_some(), epoch, "{key}");
        }
        // Quarters start in January, April, July and October.
        for (date, quarter) in [
            ("2024-03-31", 1),
            ("2024-04-01", 2),
            ("2024-07-01", 3),
            ("2024-10-01", 4),
        ] {
            let millis = parse_datetime(date).unwrap();
            assert_eq!(F::Quarter.of(millis), Some(quarter), "{date}");
        }
        // Epoch seconds round toward the past, as the instant does.
        assert_eq!(F::EpochSeconds.of(-1_001), Some(-2));
        assert_eq!(
            F::EpochSeconds.of(i64::MIN),
            Some(i64::MIN.div_euclid(1000))
        );
        for key in [
            "",
            "Year",
            "years",
            "epochmillis",
            "nanosecond",
            "timezone",
            "offset",
        ] {
            assert_eq!(F::from_key(key), None, "{key}");
        }
    }
}
