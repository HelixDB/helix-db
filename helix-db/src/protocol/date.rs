//! Date type for dates.
//!
//! This is a wrapper around a chrono DateTime<Utc>.
//!
//! It is used to deserialize a string date or numeric timestamp into a chrono DateTime<Utc>.

use core::fmt;
use std::ops::Deref;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{de::Visitor, Deserializer, Serialize};
use sonic_rs::Deserialize;

use super::value::Value;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
pub struct Date(DateTime<Utc>);
impl Date {
    pub fn inner(&self) -> &DateTime<Utc> {
        &self.0
    }

    /// Converts the Date to an RFC3339 string.
    pub fn to_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }

    /// Creates a new Date from a Value.
    pub fn new(date: &Value) -> Result<Self, DateError> {
        match date {
            Value::String(date) => {
                let date = match date.parse::<DateTime<Utc>>() {
                    Ok(date) => date.with_timezone(&Utc),
                    Err(e) => match date.parse::<NaiveDate>() {
                        Ok(date) => match date.and_hms_opt(0, 0, 0) {
                            Some(date) => date.and_utc(),
                            None => {
                                return Err(DateError::ParseError(e.to_string()));
                            }
                        },
                        Err(e) => {
                            return Err(DateError::ParseError(e.to_string()));
                        }
                    },
                };
                Ok(Date(date))
            }
            Value::I64(date) => {
                let date = match DateTime::from_timestamp(*date, 0) {
                    Some(date) => date,
                    None => {
                        return Err(DateError::ParseError(
                            "Date must be a valid date".to_string(),
                        ))
                    }
                };
                Ok(Date(date))
            }
            Value::U64(date) => {
                let date = match DateTime::from_timestamp(*date as i64, 0) {
                    Some(date) => date,
                    None => {
                        return Err(DateError::ParseError(
                            "Date must be a valid date".to_string(),
                        ))
                    }
                };
                Ok(Date(date))
            }
            _ => Err(DateError::ParseError(
                "Date must be a valid date".to_string(),
            )),
        }
    }
}

struct DateVisitor;

impl<'de> Visitor<'de> for DateVisitor {
    type Value = Date;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid Date")
    }

    /// Visits a string and parses it into a chrono DateTime<Utc>.
    ///
    /// TODO: check if this is correct -> is the same as implementation above so should be fine. 
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let date = match v.parse::<DateTime<Utc>>() {
            Ok(date) => date.with_timezone(&Utc),
            Err(e) => match v.parse::<NaiveDate>() {
                Ok(date) => match date.and_hms_opt(0, 0, 0) {
                    Some(date) => date.and_utc(),
                    None => {
                        return Err(E::custom(e.to_string()));
                    }
                },
                Err(e) => {
                    return Err(E::custom(e.to_string()));
                }
            },
        };
        Ok(Date(date))
    }

    /// Visits a i64 and parses it into a chrono DateTime<Utc>.
    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Date(match DateTime::from_timestamp(v, 0) {
            Some(date) => date,
            None => return Err(E::custom("Date must be a valid date".to_string())),
        }))
    }

    /// Visits a u64 and parses it into a chrono DateTime<Utc>.
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Date(match DateTime::from_timestamp(v as i64, 0) {
            Some(date) => date,
            None => return Err(E::custom("Date must be a valid date".to_string())),
        }))
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DateVisitor)
    }
}

impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_rfc3339())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DateError {
    ParseError(String),
}

impl fmt::Display for DateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateError::ParseError(error) => write!(f, "{error}"),
        }
    }
}

impl Deref for Date {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
