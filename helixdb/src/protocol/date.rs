use core::fmt;
use std::{fmt::Display, ops::Deref};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    ser::Error,
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};

use super::value::Value;

pub enum ValidDate {
    Date(chrono::DateTime<chrono::Utc>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
pub struct Date(DateTime<Utc>);
impl Date {
    pub fn inner(&self) -> &DateTime<Utc> {
        &self.0
    }

    pub fn to_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }

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

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let date = match v.parse::<DateTime<Utc>>() {
            Ok(date) => date,
            Err(e) => return Err(E::custom(e.to_string())),
        };
        Ok(Date(date))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Date(match DateTime::from_timestamp(v, 0) {
            Some(date) => date,
            None => return Err(E::custom("Date must be a valid date".to_string())),
        }))
    }

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

pub enum DateError {
    ParseError(String),
}

impl fmt::Display for DateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateError::ParseError(error) => write!(f, "{}", error),
        }
    }
}

impl Deref for Date {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
