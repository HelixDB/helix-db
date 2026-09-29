//! Typed order-preserving secondary range values.
//!
//! Supported values share one deterministic domain order:
//! `numeric < datetime < string`. Descending rows complement the complete
//! domain/value payload, exactly reversing that order.

use std::ops::Bound;

use bytes::{BufMut, Bytes};

use super::canonical_number::{self, CanonicalNumber};
use super::property_value::PropertyValue;
use crate::encoding::error::EncodingError;
use crate::encoding::indexes::range::RangeIndexDirection;

pub(crate) const MAX_RANGE_ENCODED_LEN: usize = 1024 * 1024 - 32;

const NUMERIC_DOMAIN: u8 = 0x01;
const DATETIME_DOMAIN: u8 = 0x02;
const STRING_DOMAIN: u8 = 0x03;

const NEGATIVE_INFINITY_CLASS: u8 = 0x01;
const NEGATIVE_FINITE_CLASS: u8 = 0x02;
const ZERO_CLASS: u8 = 0x03;
const POSITIVE_FINITE_CLASS: u8 = 0x04;
const POSITIVE_INFINITY_CLASS: u8 = 0x05;

const EXPONENT_BIAS: i32 = 1 << 15;
const MIN_FINITE_FLOOR_LOG2: i32 = -1074;
const MAX_FINITE_FLOOR_LOG2: i32 = 1023;
const STRING_ESCAPE: u8 = 0xFF;
const STRING_TERMINATOR: u8 = 0x00;

/// A complete physical value payload, excluding the entity-ID tie-breaker.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalRangeValue {
    direction: RangeIndexDirection,
    encoded: Bytes,
}

impl CanonicalRangeValue {
    pub(crate) fn try_from_encoded(
        direction: RangeIndexDirection,
        encoded: Bytes,
    ) -> Result<Self, EncodingError> {
        validate_encoded(direction, &encoded)?;
        Ok(Self { direction, encoded })
    }

    pub(crate) const fn direction(&self) -> RangeIndexDirection {
        self.direction
    }

    pub(crate) fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    /// Constructs the complete physical range-key suffix with its entity-ID
    /// tie-breaker.
    pub(crate) fn entity_key_suffix(&self, entity_id: u64) -> Bytes {
        let mut bytes = Vec::with_capacity(self.encoded.len() + core::mem::size_of::<u64>());
        bytes.put_slice(&self.encoded);
        bytes.put_u64(entity_id);
        Bytes::from(bytes)
    }

    /// Constructs the physical bounds for this value's complete typed domain.
    pub(crate) fn domain_key_bounds(&self) -> (Bound<Bytes>, Bound<Bytes>) {
        let domain = self.encoded[0];
        (
            Bound::Included(Bytes::copy_from_slice(&[domain])),
            Bound::Excluded(Bytes::copy_from_slice(&[domain
                .checked_add(1)
                .expect("canonical range domain tags always have a successor")])),
        )
    }
}

/// Closed maintenance/query projection for every property-value variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RangeValueProjection {
    Indexed(CanonicalRangeValue),
    Unsupported(&'static str),
    NaN,
    Oversized { encoded_len: usize, maximum: usize },
}

pub(crate) fn project_range_value(
    value: &PropertyValue,
    direction: RangeIndexDirection,
) -> RangeValueProjection {
    let mut encoded = Vec::new();
    match value {
        PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_) => {
            let Some(number) = canonical_number::from_property(value) else {
                return RangeValueProjection::NaN;
            };
            encoded.put_u8(NUMERIC_DOMAIN);
            put_ordered_number(&mut encoded, number);
        }
        PropertyValue::DateTime(value) => {
            encoded.put_u8(DATETIME_DOMAIN);
            encoded.put_u64((*value as u64) ^ (1_u64 << 63));
        }
        PropertyValue::String(value) => {
            encoded.put_u8(STRING_DOMAIN);
            for byte in value.as_bytes() {
                if *byte == STRING_TERMINATOR {
                    encoded.put_u8(STRING_TERMINATOR);
                    encoded.put_u8(STRING_ESCAPE);
                } else {
                    encoded.put_u8(*byte);
                }
            }
            encoded.put_u8(STRING_TERMINATOR);
            encoded.put_u8(STRING_TERMINATOR);
        }
        PropertyValue::Null => return RangeValueProjection::Unsupported("Null"),
        PropertyValue::Bool(_) => return RangeValueProjection::Unsupported("Bool"),
        PropertyValue::Bytes(_) => return RangeValueProjection::Unsupported("Bytes"),
        PropertyValue::I64Array(_) => return RangeValueProjection::Unsupported("I64Array"),
        PropertyValue::F64Array(_) => return RangeValueProjection::Unsupported("F64Array"),
        PropertyValue::F32Array(_) => return RangeValueProjection::Unsupported("F32Array"),
        PropertyValue::StringArray(_) => return RangeValueProjection::Unsupported("StringArray"),
        PropertyValue::Array(_) => return RangeValueProjection::Unsupported("Array"),
        PropertyValue::Object(_) => return RangeValueProjection::Unsupported("Object"),
    }
    if encoded.len() > MAX_RANGE_ENCODED_LEN {
        return RangeValueProjection::Oversized {
            encoded_len: encoded.len(),
            maximum: MAX_RANGE_ENCODED_LEN,
        };
    }
    if direction == RangeIndexDirection::Desc {
        encoded.iter_mut().for_each(|byte| *byte = !*byte);
    }
    RangeValueProjection::Indexed(CanonicalRangeValue {
        direction,
        encoded: Bytes::from(encoded),
    })
}

fn put_ordered_number(encoded: &mut Vec<u8>, number: CanonicalNumber) {
    match number {
        CanonicalNumber::NegativeInfinity => encoded.put_u8(NEGATIVE_INFINITY_CLASS),
        CanonicalNumber::NegativeFinite(finite) => {
            encoded.put_u8(NEGATIVE_FINITE_CLASS);
            encoded.put_u16(!biased_exponent(finite.floor_log2()));
            encoded.put_u64(!finite.normalized_significand());
        }
        CanonicalNumber::Zero => encoded.put_u8(ZERO_CLASS),
        CanonicalNumber::PositiveFinite(finite) => {
            encoded.put_u8(POSITIVE_FINITE_CLASS);
            encoded.put_u16(biased_exponent(finite.floor_log2()));
            encoded.put_u64(finite.normalized_significand());
        }
        CanonicalNumber::PositiveInfinity => encoded.put_u8(POSITIVE_INFINITY_CLASS),
    }
}

fn biased_exponent(exponent: i16) -> u16 {
    u16::try_from(i32::from(exponent) + EXPONENT_BIAS)
        .expect("canonical binary exponent fits biased u16")
}

fn validate_encoded(direction: RangeIndexDirection, encoded: &[u8]) -> Result<(), EncodingError> {
    if encoded.is_empty() {
        return Err(EncodingError::InvalidKey(
            "canonical range value is empty".to_string(),
        ));
    }
    if encoded.len() > MAX_RANGE_ENCODED_LEN {
        return Err(EncodingError::InvalidKey(format!(
            "canonical range value exceeds {MAX_RANGE_ENCODED_LEN} bytes"
        )));
    }
    let logical = |offset: usize| decode_byte(direction, encoded[offset]);
    match logical(0) {
        NUMERIC_DOMAIN => {
            if encoded.len() < 2 {
                return Err(EncodingError::BufferTooShort {
                    expected: 2,
                    actual: encoded.len(),
                });
            }
            let class = logical(1);
            match class {
                NEGATIVE_INFINITY_CLASS | ZERO_CLASS | POSITIVE_INFINITY_CLASS => {
                    require_len(encoded, 2)
                }
                NEGATIVE_FINITE_CLASS | POSITIVE_FINITE_CLASS => {
                    require_len(encoded, 12)?;
                    let mut exponent_bytes = [0_u8; core::mem::size_of::<u16>()];
                    for (target, source) in exponent_bytes
                        .iter_mut()
                        .zip(encoded[2..2 + core::mem::size_of::<u16>()].iter())
                    {
                        *target = decode_byte(direction, *source);
                    }
                    let mut biased = u16::from_be_bytes(exponent_bytes);
                    if class == NEGATIVE_FINITE_CLASS {
                        biased = !biased;
                    }
                    let exponent = i32::from(biased) - EXPONENT_BIAS;
                    if !(MIN_FINITE_FLOOR_LOG2..=MAX_FINITE_FLOOR_LOG2).contains(&exponent) {
                        return Err(EncodingError::InvalidKey(
                            "canonical range exponent is outside supported numeric values"
                                .to_string(),
                        ));
                    }
                    let mut normalized_bytes = [0_u8; core::mem::size_of::<u64>()];
                    for (target, source) in normalized_bytes
                        .iter_mut()
                        .zip(encoded[4..4 + core::mem::size_of::<u64>()].iter())
                    {
                        *target = decode_byte(direction, *source);
                    }
                    let mut normalized = u64::from_be_bytes(normalized_bytes);
                    if class == NEGATIVE_FINITE_CLASS {
                        normalized = !normalized;
                    }
                    if normalized.leading_zeros() != 0 {
                        return Err(EncodingError::InvalidKey(
                            "canonical range significand is not normalized".to_string(),
                        ));
                    }
                    Ok(())
                }
                class => Err(EncodingError::InvalidKey(format!(
                    "unknown canonical numeric class {class:#04x}"
                ))),
            }
        }
        DATETIME_DOMAIN => require_len(encoded, 1 + core::mem::size_of::<i64>()),
        STRING_DOMAIN => validate_string(direction, encoded),
        domain => Err(EncodingError::InvalidKey(format!(
            "unknown canonical range domain {domain:#04x}"
        ))),
    }
}

fn validate_string(direction: RangeIndexDirection, encoded: &[u8]) -> Result<(), EncodingError> {
    let mut decoded = Vec::new();
    let mut offset = 1;
    loop {
        if offset >= encoded.len() {
            return Err(EncodingError::InvalidKey(
                "canonical range string has no terminator".to_string(),
            ));
        }
        let byte = decode_byte(direction, encoded[offset]);
        offset += 1;
        if byte != STRING_TERMINATOR {
            decoded.push(byte);
            continue;
        }
        if offset >= encoded.len() {
            return Err(EncodingError::InvalidKey(
                "canonical range string has a truncated escape".to_string(),
            ));
        }
        let escaped = decode_byte(direction, encoded[offset]);
        offset += 1;
        match escaped {
            STRING_TERMINATOR if offset == encoded.len() => break,
            STRING_ESCAPE => decoded.push(STRING_TERMINATOR),
            STRING_TERMINATOR => {
                return Err(EncodingError::InvalidKey(
                    "canonical range string terminator has trailing bytes".to_string(),
                ));
            }
            value => {
                return Err(EncodingError::InvalidKey(format!(
                    "invalid canonical range string escape {value:#04x}"
                )));
            }
        }
    }
    std::str::from_utf8(&decoded)?;
    Ok(())
}

fn require_len(encoded: &[u8], expected: usize) -> Result<(), EncodingError> {
    if encoded.len() == expected {
        Ok(())
    } else {
        Err(EncodingError::InvalidKey(format!(
            "canonical range value has length {}, expected {expected}",
            encoded.len()
        )))
    }
}

const fn decode_byte(direction: RangeIndexDirection, byte: u8) -> u8 {
    match direction {
        RangeIndexDirection::Asc => byte,
        RangeIndexDirection::Desc => !byte,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed(value: &PropertyValue, direction: RangeIndexDirection) -> CanonicalRangeValue {
        let RangeValueProjection::Indexed(value) = project_range_value(value, direction) else {
            panic!("fixture must be range indexable");
        };
        value
    }

    #[test]
    fn ascending_encoding_preserves_exact_semantic_numeric_order() {
        let ordered = [
            PropertyValue::F64(f64::NEG_INFINITY),
            PropertyValue::I64(i64::MIN),
            PropertyValue::F64(-1.5),
            PropertyValue::F64(-f64::from_bits(1)),
            PropertyValue::F64(-0.0),
            PropertyValue::F64(f64::from_bits(1)),
            PropertyValue::F64(1.5),
            PropertyValue::I64(i64::MAX),
            PropertyValue::F64(f64::INFINITY),
        ];
        let ascending = ordered
            .iter()
            .map(|value| indexed(value, RangeIndexDirection::Asc))
            .collect::<Vec<_>>();
        assert!(ascending
            .windows(2)
            .all(|pair| pair[0].encoded < pair[1].encoded));
        let descending = ordered
            .iter()
            .map(|value| indexed(value, RangeIndexDirection::Desc))
            .collect::<Vec<_>>();
        assert!(descending
            .windows(2)
            .all(|pair| pair[0].encoded > pair[1].encoded));
    }

    #[test]
    fn exact_cross_numeric_values_share_one_range_payload() {
        assert_eq!(
            indexed(&PropertyValue::I64(42), RangeIndexDirection::Asc),
            indexed(&PropertyValue::F64(42.0), RangeIndexDirection::Asc)
        );
        assert_eq!(
            indexed(&PropertyValue::F64(-0.0), RangeIndexDirection::Asc),
            indexed(&PropertyValue::F32(0.0), RangeIndexDirection::Asc)
        );
        assert_ne!(
            indexed(
                &PropertyValue::I64(9_007_199_254_740_993),
                RangeIndexDirection::Asc
            ),
            indexed(
                &PropertyValue::F64(9_007_199_254_740_992.0),
                RangeIndexDirection::Asc
            )
        );
    }

    #[test]
    fn physical_key_suffix_and_domain_bounds_are_encoding_owned() {
        let value = indexed(
            &PropertyValue::String("a\0b".to_string()),
            RangeIndexDirection::Asc,
        );
        let suffix = value.entity_key_suffix(0x0102_0304_0506_0708);
        assert_eq!(
            &suffix[suffix.len() - core::mem::size_of::<u64>()..],
            &0x0102_0304_0506_0708_u64.to_be_bytes()
        );
        assert_eq!(
            value.domain_key_bounds(),
            (
                Bound::Included(Bytes::from_static(&[STRING_DOMAIN])),
                Bound::Excluded(Bytes::from_static(&[STRING_DOMAIN + 1])),
            )
        );

        let descending = indexed(
            &PropertyValue::String("a\0b".to_string()),
            RangeIndexDirection::Desc,
        );
        assert_eq!(
            descending.domain_key_bounds(),
            (
                Bound::Included(Bytes::from_static(&[!STRING_DOMAIN])),
                Bound::Excluded(Bytes::from_static(&[!STRING_DOMAIN + 1])),
            )
        );
    }

    #[test]
    fn domains_and_prefix_strings_have_one_total_order() {
        let values = [
            PropertyValue::I64(1),
            PropertyValue::DateTime(i64::MIN),
            PropertyValue::String(String::new()),
            PropertyValue::String("\0".to_string()),
            PropertyValue::String("a".to_string()),
            PropertyValue::String("a\0".to_string()),
            PropertyValue::String("aa".to_string()),
            PropertyValue::String("aaa".to_string()),
        ];
        let ascending = values
            .iter()
            .map(|value| indexed(value, RangeIndexDirection::Asc))
            .collect::<Vec<_>>();
        assert!(ascending
            .windows(2)
            .all(|pair| pair[0].encoded < pair[1].encoded));
        let descending = values
            .iter()
            .map(|value| indexed(value, RangeIndexDirection::Desc))
            .collect::<Vec<_>>();
        assert!(descending
            .windows(2)
            .all(|pair| pair[0].encoded > pair[1].encoded));
    }

    #[test]
    fn unsupported_nan_and_malformed_values_fail_closed() {
        assert_eq!(
            project_range_value(&PropertyValue::F64(f64::NAN), RangeIndexDirection::Asc),
            RangeValueProjection::NaN
        );
        assert_eq!(
            project_range_value(&PropertyValue::Bool(true), RangeIndexDirection::Asc),
            RangeValueProjection::Unsupported("Bool")
        );
        assert!(CanonicalRangeValue::try_from_encoded(
            RangeIndexDirection::Asc,
            Bytes::from_static(&[STRING_DOMAIN, b'a'])
        )
        .is_err());
    }

    #[test]
    fn every_unsupported_and_oversized_property_shape_is_classified_exactly() {
        let unsupported = [
            (PropertyValue::Null, "Null"),
            (PropertyValue::Bool(false), "Bool"),
            (PropertyValue::Bytes(vec![1]), "Bytes"),
            (PropertyValue::I64Array(vec![1]), "I64Array"),
            (PropertyValue::F64Array(vec![1.0]), "F64Array"),
            (PropertyValue::F32Array(vec![1.0]), "F32Array"),
            (
                PropertyValue::StringArray(vec!["value".to_string()]),
                "StringArray",
            ),
            (PropertyValue::Array(vec![PropertyValue::I64(1)]), "Array"),
            (
                PropertyValue::Object(std::collections::BTreeMap::from([(
                    "key".to_string(),
                    PropertyValue::I64(1),
                )])),
                "Object",
            ),
        ];
        for (value, expected_type) in unsupported {
            assert_eq!(
                project_range_value(&value, RangeIndexDirection::Asc),
                RangeValueProjection::Unsupported(expected_type)
            );
        }

        let oversized = PropertyValue::String("x".repeat(MAX_RANGE_ENCODED_LEN));
        assert_eq!(
            project_range_value(&oversized, RangeIndexDirection::Asc),
            RangeValueProjection::Oversized {
                encoded_len: MAX_RANGE_ENCODED_LEN + 3,
                maximum: MAX_RANGE_ENCODED_LEN,
            }
        );
    }

    #[test]
    fn encoded_range_validation_rejects_every_malformed_numeric_and_string_shape() {
        let malformed = [
            Vec::new(),
            vec![NUMERIC_DOMAIN],
            vec![NUMERIC_DOMAIN, NEGATIVE_INFINITY_CLASS, 0],
            vec![NUMERIC_DOMAIN, ZERO_CLASS, 0],
            vec![NUMERIC_DOMAIN, POSITIVE_INFINITY_CLASS, 0],
            vec![NUMERIC_DOMAIN, 0xff],
            vec![DATETIME_DOMAIN],
            vec![0xfe],
            vec![STRING_DOMAIN, b'a'],
            vec![STRING_DOMAIN, STRING_TERMINATOR],
            vec![STRING_DOMAIN, STRING_TERMINATOR, STRING_TERMINATOR, b'x'],
            vec![STRING_DOMAIN, STRING_TERMINATOR, 0x01],
            vec![STRING_DOMAIN, 0xff, STRING_TERMINATOR, STRING_TERMINATOR],
        ];
        for encoded in malformed {
            assert!(CanonicalRangeValue::try_from_encoded(
                RangeIndexDirection::Asc,
                Bytes::from(encoded),
            )
            .is_err());
        }

        let mut invalid_exponent = vec![0_u8; 12];
        invalid_exponent[0] = NUMERIC_DOMAIN;
        invalid_exponent[1] = POSITIVE_FINITE_CLASS;
        invalid_exponent[4] = 0x80;
        assert!(CanonicalRangeValue::try_from_encoded(
            RangeIndexDirection::Asc,
            Bytes::from(invalid_exponent),
        )
        .is_err());

        let mut unnormalized_significand = vec![0_u8; 12];
        unnormalized_significand[0] = NUMERIC_DOMAIN;
        unnormalized_significand[1] = POSITIVE_FINITE_CLASS;
        unnormalized_significand[2..2 + core::mem::size_of::<u16>()]
            .copy_from_slice(&(EXPONENT_BIAS as u16).to_be_bytes());
        assert!(CanonicalRangeValue::try_from_encoded(
            RangeIndexDirection::Asc,
            Bytes::from(unnormalized_significand),
        )
        .is_err());

        assert!(CanonicalRangeValue::try_from_encoded(
            RangeIndexDirection::Asc,
            Bytes::from(vec![0; MAX_RANGE_ENCODED_LEN + 1]),
        )
        .is_err());
        assert!(CanonicalRangeValue::try_from_encoded(
            RangeIndexDirection::Desc,
            Bytes::from_static(&[!0xfe]),
        )
        .is_err());

        for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
            for property in [
                PropertyValue::F64(-1.0),
                PropertyValue::F64(1.0),
                PropertyValue::DateTime(-1),
                PropertyValue::String("valid\0utf8".to_string()),
            ] {
                let projected = indexed(&property, direction);
                let decoded = CanonicalRangeValue::try_from_encoded(
                    direction,
                    Bytes::copy_from_slice(projected.encoded()),
                )
                .unwrap();
                assert_eq!(decoded.direction(), direction);
                assert_eq!(decoded, projected);
            }
        }
    }
}
