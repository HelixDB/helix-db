//! Canonical, type-preserving equality-index values.
//!
//! The digest is only a scan accelerator. Exact identity is always the
//! length-delimited canonical byte string.

use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};

use super::canonical_number::{self, CanonicalNumber};
use super::property_value::PropertyValue;
use crate::encoding::error::EncodingError;

pub(crate) const EQUALITY_DIGEST_LEN: usize = core::mem::size_of::<u64>();
pub(crate) const MAX_EQUALITY_CANONICAL_LEN: usize = 1024 * 1024 - 64;

const BOOL_TAG: u8 = 0x01;
const NUMBER_TAG: u8 = 0x02;
const DATETIME_TAG: u8 = 0x03;
const STRING_TAG: u8 = 0x04;
const BYTES_TAG: u8 = 0x05;
const I64_ARRAY_TAG: u8 = 0x06;
const F64_ARRAY_TAG: u8 = 0x07;
const F32_ARRAY_TAG: u8 = 0x08;
const STRING_ARRAY_TAG: u8 = 0x09;

const NEGATIVE_INFINITY_TAG: u8 = 0x01;
const NEGATIVE_FINITE_TAG: u8 = 0x02;
const ZERO_TAG: u8 = 0x03;
const POSITIVE_FINITE_TAG: u8 = 0x04;
const POSITIVE_INFINITY_TAG: u8 = 0x05;

/// Exact equality bytes and their bounded scan digest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalEqualityValue {
    digest: [u8; EQUALITY_DIGEST_LEN],
    canonical: Bytes,
}

impl CanonicalEqualityValue {
    fn new(canonical: Vec<u8>) -> Self {
        let digest = equality_digest(&canonical);
        Self {
            digest,
            canonical: Bytes::from(canonical),
        }
    }

    /// Reconstructs a persisted value while validating its canonical frame.
    pub(crate) fn try_from_parts(
        digest: [u8; EQUALITY_DIGEST_LEN],
        canonical: Bytes,
    ) -> Result<Self, EncodingError> {
        validate_canonical(&canonical)?;
        if digest != equality_digest(&canonical) {
            return Err(EncodingError::CanonicalEqualityDigestMismatch);
        }
        Ok(Self { digest, canonical })
    }

    pub(crate) const fn digest(&self) -> &[u8; EQUALITY_DIGEST_LEN] {
        &self.digest
    }

    pub(crate) fn canonical(&self) -> &[u8] {
        &self.canonical
    }

    /// Constructs an in-memory digest-collision fixture that must not be persisted.
    #[cfg(test)]
    pub(crate) fn with_test_digest_unchecked(
        value: &PropertyValue,
        digest: [u8; EQUALITY_DIGEST_LEN],
    ) -> Self {
        let EqualityValueProjection::Indexed(mut canonical) = project_equality_value(value) else {
            panic!("forced digest requires an indexed equality value");
        };
        canonical.digest = digest;
        canonical
    }
}

/// Closed maintenance/lookup projection for every property-value variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EqualityValueProjection {
    Indexed(CanonicalEqualityValue),
    AuthoritativeNull,
    NonReflexive,
    Unsupported(&'static str),
    Oversized { encoded_len: usize, maximum: usize },
}

pub(crate) fn project_equality_value(value: &PropertyValue) -> EqualityValueProjection {
    let mut canonical = Vec::new();
    let projection = match value {
        PropertyValue::Null => return EqualityValueProjection::AuthoritativeNull,
        PropertyValue::Bool(value) => {
            canonical.put_u8(BOOL_TAG);
            canonical.put_u8(u8::from(*value));
            Some(())
        }
        PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_) => {
            let Some(number) = canonical_number::from_property(value) else {
                return EqualityValueProjection::NonReflexive;
            };
            canonical.put_u8(NUMBER_TAG);
            put_number(&mut canonical, number);
            Some(())
        }
        PropertyValue::DateTime(value) => {
            canonical.put_u8(DATETIME_TAG);
            canonical.put_i64(*value);
            Some(())
        }
        PropertyValue::String(value) => {
            canonical.put_u8(STRING_TAG);
            put_length_delimited(&mut canonical, value.as_bytes())
        }
        PropertyValue::Bytes(value) => {
            canonical.put_u8(BYTES_TAG);
            put_length_delimited(&mut canonical, value)
        }
        PropertyValue::I64Array(values) => {
            canonical.put_u8(I64_ARRAY_TAG);
            put_count(&mut canonical, values.len()).map(|()| {
                values.iter().for_each(|value| canonical.put_i64(*value));
            })
        }
        PropertyValue::F64Array(values) => {
            canonical.put_u8(F64_ARRAY_TAG);
            let Some(()) = put_count(&mut canonical, values.len()) else {
                return oversized_projection(values.len());
            };
            for value in values {
                let Some(value) = CanonicalNumber::from_f64(*value) else {
                    return EqualityValueProjection::NonReflexive;
                };
                put_number(&mut canonical, value);
            }
            Some(())
        }
        PropertyValue::F32Array(values) => {
            canonical.put_u8(F32_ARRAY_TAG);
            let Some(()) = put_count(&mut canonical, values.len()) else {
                return oversized_projection(values.len());
            };
            for value in values {
                let Some(value) = CanonicalNumber::from_f32(*value) else {
                    return EqualityValueProjection::NonReflexive;
                };
                put_number(&mut canonical, value);
            }
            Some(())
        }
        PropertyValue::StringArray(values) => {
            canonical.put_u8(STRING_ARRAY_TAG);
            let Some(()) = put_count(&mut canonical, values.len()) else {
                return oversized_projection(values.len());
            };
            for value in values {
                if put_length_delimited(&mut canonical, value.as_bytes()).is_none() {
                    return oversized_projection(value.len());
                }
            }
            Some(())
        }
        PropertyValue::Array(_) => return EqualityValueProjection::Unsupported("Array"),
        PropertyValue::Object(_) => return EqualityValueProjection::Unsupported("Object"),
    };
    if projection.is_none() || canonical.len() > MAX_EQUALITY_CANONICAL_LEN {
        return oversized_projection(canonical.len());
    }
    EqualityValueProjection::Indexed(CanonicalEqualityValue::new(canonical))
}

fn oversized_projection(encoded_len: usize) -> EqualityValueProjection {
    EqualityValueProjection::Oversized {
        encoded_len,
        maximum: MAX_EQUALITY_CANONICAL_LEN,
    }
}

fn put_count(bytes: &mut Vec<u8>, count: usize) -> Option<()> {
    bytes.put_u32(u32::try_from(count).ok()?);
    Some(())
}

fn put_length_delimited(bytes: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    bytes.put_u32(u32::try_from(value.len()).ok()?);
    bytes.put_slice(value);
    Some(())
}

fn put_number(bytes: &mut Vec<u8>, value: CanonicalNumber) {
    match value {
        CanonicalNumber::NegativeInfinity => bytes.put_u8(NEGATIVE_INFINITY_TAG),
        CanonicalNumber::NegativeFinite(value) => {
            bytes.put_u8(NEGATIVE_FINITE_TAG);
            bytes.put_i16(value.exponent());
            bytes.put_u64(value.odd_significand());
        }
        CanonicalNumber::Zero => bytes.put_u8(ZERO_TAG),
        CanonicalNumber::PositiveFinite(value) => {
            bytes.put_u8(POSITIVE_FINITE_TAG);
            bytes.put_i16(value.exponent());
            bytes.put_u64(value.odd_significand());
        }
        CanonicalNumber::PositiveInfinity => bytes.put_u8(POSITIVE_INFINITY_TAG),
    }
}

fn equality_digest(canonical: &[u8]) -> [u8; EQUALITY_DIGEST_LEN] {
    let hash = Sha256::digest(canonical);
    hash[..EQUALITY_DIGEST_LEN]
        .try_into()
        .expect("SHA-256 contains an eight-byte digest prefix")
}

fn validate_canonical(canonical: &[u8]) -> Result<(), EncodingError> {
    if canonical.len() > MAX_EQUALITY_CANONICAL_LEN {
        return Err(EncodingError::InvalidKey(format!(
            "canonical equality value exceeds {MAX_EQUALITY_CANONICAL_LEN} bytes"
        )));
    }
    let mut decoder = CanonicalDecoder::new(canonical);
    match decoder.take_u8()? {
        BOOL_TAG => match decoder.take_u8()? {
            0x00 | 0x01 => {}
            value => {
                return Err(EncodingError::InvalidKey(format!(
                    "noncanonical equality boolean {value:#04x}"
                )));
            }
        },
        NUMBER_TAG => decoder.take_number()?,
        DATETIME_TAG => {
            decoder.take_raw(core::mem::size_of::<i64>())?;
        }
        STRING_TAG => {
            std::str::from_utf8(decoder.take_length_delimited()?)?;
        }
        BYTES_TAG => {
            decoder.take_length_delimited()?;
        }
        I64_ARRAY_TAG => {
            let count = decoder.take_u32()? as usize;
            decoder.take_raw(count.checked_mul(core::mem::size_of::<i64>()).ok_or_else(
                || EncodingError::InvalidKey("equality i64 array length overflowed".to_string()),
            )?)?;
        }
        F64_ARRAY_TAG | F32_ARRAY_TAG => {
            let count = decoder.take_u32()? as usize;
            for _ in 0..count {
                decoder.take_number()?;
            }
        }
        STRING_ARRAY_TAG => {
            let count = decoder.take_u32()? as usize;
            for _ in 0..count {
                std::str::from_utf8(decoder.take_length_delimited()?)?;
            }
        }
        tag => {
            return Err(EncodingError::InvalidKey(format!(
                "unknown canonical equality tag {tag:#04x}"
            )));
        }
    }
    decoder.finish()
}

struct CanonicalDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> CanonicalDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take_u8(&mut self) -> Result<u8, EncodingError> {
        Ok(self.take_raw(core::mem::size_of::<u8>())?[0])
    }

    fn take_u32(&mut self) -> Result<u32, EncodingError> {
        Ok(u32::from_be_bytes(
            self.take_raw(core::mem::size_of::<u32>())?
                .try_into()
                .expect("canonical u32 slice has exact length"),
        ))
    }

    fn take_raw(&mut self, len: usize) -> Result<&'a [u8], EncodingError> {
        if self.remaining.len() < len {
            return Err(EncodingError::BufferTooShort {
                expected: len,
                actual: self.remaining.len(),
            });
        }
        let value = &self.remaining[..len];
        self.remaining = &self.remaining[len..];
        Ok(value)
    }

    fn take_length_delimited(&mut self) -> Result<&'a [u8], EncodingError> {
        let len = self.take_u32()? as usize;
        self.take_raw(len)
    }

    fn take_number(&mut self) -> Result<(), EncodingError> {
        match self.take_u8()? {
            NEGATIVE_INFINITY_TAG | ZERO_TAG | POSITIVE_INFINITY_TAG => Ok(()),
            NEGATIVE_FINITE_TAG | POSITIVE_FINITE_TAG => {
                self.take_raw(core::mem::size_of::<i16>())?;
                let significand = u64::from_be_bytes(
                    self.take_raw(core::mem::size_of::<u64>())?
                        .try_into()
                        .expect("canonical significand slice has exact length"),
                );
                if significand == 0 || significand.is_multiple_of(2) {
                    return Err(EncodingError::InvalidKey(
                        "canonical finite significand must be nonzero and odd".to_string(),
                    ));
                }
                Ok(())
            }
            tag => Err(EncodingError::InvalidKey(format!(
                "unknown canonical number tag {tag:#04x}"
            ))),
        }
    }

    fn finish(self) -> Result<(), EncodingError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(EncodingError::InvalidKey(format!(
                "canonical equality value has {} trailing bytes",
                self.remaining.len()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed(value: &PropertyValue) -> CanonicalEqualityValue {
        let EqualityValueProjection::Indexed(value) = project_equality_value(value) else {
            panic!("fixture must produce indexed equality bytes");
        };
        value
    }

    #[test]
    fn canonical_equality_is_typed_exact_and_collision_safe() {
        assert_ne!(
            indexed(&PropertyValue::Bool(true)),
            indexed(&PropertyValue::String("true".to_string()))
        );
        assert_ne!(
            indexed(&PropertyValue::I64(42)),
            indexed(&PropertyValue::String("42".to_string()))
        );
        assert_ne!(
            indexed(&PropertyValue::Bytes(vec![1, 2])),
            indexed(&PropertyValue::String("[1, 2]".to_string()))
        );
        assert_ne!(
            indexed(&PropertyValue::I64Array(vec![1, 2])),
            indexed(&PropertyValue::I64Array(vec![3, 4]))
        );
    }

    #[test]
    fn canonical_numbers_unify_exact_cross_variants_without_rounding() {
        assert_eq!(
            indexed(&PropertyValue::I64(9_007_199_254_740_992)),
            indexed(&PropertyValue::F64(9_007_199_254_740_992.0))
        );
        assert_ne!(
            indexed(&PropertyValue::I64(9_007_199_254_740_993)),
            indexed(&PropertyValue::F64(9_007_199_254_740_992.0))
        );
        assert_eq!(
            indexed(&PropertyValue::F64(-0.0)),
            indexed(&PropertyValue::F32(0.0))
        );
    }

    #[test]
    fn null_nan_and_unsupported_values_have_closed_projections() {
        assert_eq!(
            project_equality_value(&PropertyValue::Null),
            EqualityValueProjection::AuthoritativeNull
        );
        assert_eq!(
            project_equality_value(&PropertyValue::F64(f64::NAN)),
            EqualityValueProjection::NonReflexive
        );
        assert_eq!(
            project_equality_value(&PropertyValue::F64Array(vec![f64::NAN])),
            EqualityValueProjection::NonReflexive
        );
        assert_eq!(
            project_equality_value(&PropertyValue::Array(Vec::new())),
            EqualityValueProjection::Unsupported("Array")
        );
    }

    #[test]
    fn persisted_canonical_frames_fail_closed() {
        let value = indexed(&PropertyValue::String("value".to_string()));
        assert_eq!(
            CanonicalEqualityValue::try_from_parts(
                *value.digest(),
                Bytes::copy_from_slice(value.canonical())
            )
            .unwrap(),
            value
        );
        assert!(CanonicalEqualityValue::try_from_parts(
            [0; EQUALITY_DIGEST_LEN],
            Bytes::from_static(&[STRING_TAG, 0, 0, 0, 2, b'x'])
        )
        .is_err());
    }

    #[test]
    fn persisted_canonical_digest_mismatches_fail_closed() {
        let value = indexed(&PropertyValue::String("value".to_string()));
        let mut mismatched_digest = *value.digest();
        mismatched_digest[0] ^= 0xFF;

        assert!(matches!(
            CanonicalEqualityValue::try_from_parts(
                mismatched_digest,
                Bytes::copy_from_slice(value.canonical())
            ),
            Err(EncodingError::CanonicalEqualityDigestMismatch)
        ));
    }

    #[test]
    fn digest_collisions_retain_distinct_exact_canonical_identity() {
        let digest = [0xA5; EQUALITY_DIGEST_LEN];
        let first = CanonicalEqualityValue::with_test_digest_unchecked(
            &PropertyValue::String("first".to_string()),
            digest,
        );
        let second = CanonicalEqualityValue::with_test_digest_unchecked(
            &PropertyValue::String("second".to_string()),
            digest,
        );

        assert_eq!(first.digest(), second.digest());
        assert_ne!(first.canonical(), second.canonical());
        assert_ne!(first, second);
    }

    #[test]
    fn equality_projection_covers_every_supported_scalar_array_and_bound() {
        let indexed_values = [
            PropertyValue::Bool(false),
            PropertyValue::I64(i64::MIN),
            PropertyValue::F64(f64::NEG_INFINITY),
            PropertyValue::F64(f64::INFINITY),
            PropertyValue::F32(-1.5),
            PropertyValue::DateTime(i64::MAX),
            PropertyValue::String("value".to_string()),
            PropertyValue::Bytes(vec![0, 1, 2]),
            PropertyValue::I64Array(vec![i64::MIN, i64::MAX]),
            PropertyValue::F64Array(vec![f64::NEG_INFINITY, -1.5, 0.0, f64::INFINITY]),
            PropertyValue::F32Array(vec![f32::NEG_INFINITY, -1.5, 0.0, f32::INFINITY]),
            PropertyValue::StringArray(vec!["left".to_string(), "right".to_string()]),
        ];
        for value in indexed_values {
            let EqualityValueProjection::Indexed(projected) = project_equality_value(&value) else {
                panic!("supported equality value must be indexed: {value:?}");
            };
            assert_eq!(
                CanonicalEqualityValue::try_from_parts(
                    *projected.digest(),
                    Bytes::copy_from_slice(projected.canonical()),
                )
                .expect("projected canonical frame validates"),
                projected
            );
        }

        assert_eq!(
            project_equality_value(&PropertyValue::F32(f64::NAN)),
            EqualityValueProjection::NonReflexive
        );
        assert_eq!(
            project_equality_value(&PropertyValue::F32Array(vec![f32::NAN])),
            EqualityValueProjection::NonReflexive
        );
        assert_eq!(
            project_equality_value(&PropertyValue::Object(Default::default())),
            EqualityValueProjection::Unsupported("Object")
        );

        let EqualityValueProjection::Oversized {
            encoded_len,
            maximum,
        } = project_equality_value(&PropertyValue::Bytes(vec![0; MAX_EQUALITY_CANONICAL_LEN]))
        else {
            panic!("oversized canonical byte value must be rejected");
        };
        assert!(encoded_len > maximum);
        assert_eq!(maximum, MAX_EQUALITY_CANONICAL_LEN);
    }

    #[test]
    fn persisted_canonical_decoder_rejects_every_malformed_frame_family() {
        let malformed = [
            Vec::new(),
            vec![BOOL_TAG],
            vec![BOOL_TAG, 0x02],
            vec![NUMBER_TAG],
            vec![NUMBER_TAG, 0xFF],
            vec![NUMBER_TAG, NEGATIVE_FINITE_TAG, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            vec![NUMBER_TAG, POSITIVE_FINITE_TAG, 0, 0, 0, 0, 0, 0, 0, 0, 2],
            vec![DATETIME_TAG, 0],
            vec![STRING_TAG, 0, 0, 0, 1, 0xFF],
            vec![BYTES_TAG, 0, 0, 0, 2, 1],
            vec![I64_ARRAY_TAG, 0, 0, 0, 1, 0],
            vec![F64_ARRAY_TAG, 0, 0, 0, 1],
            vec![F32_ARRAY_TAG, 0, 0, 0, 1, 0xFE],
            vec![STRING_ARRAY_TAG, 0, 0, 0, 1, 0, 0, 0, 1, 0xFF],
            vec![0xFF],
            vec![BOOL_TAG, 0, 0],
        ];
        for canonical in malformed {
            let digest = equality_digest(&canonical);
            assert!(
                CanonicalEqualityValue::try_from_parts(digest, Bytes::from(canonical)).is_err()
            );
        }

        let oversized = Bytes::from(vec![0; MAX_EQUALITY_CANONICAL_LEN.saturating_add(1)]);
        assert!(
            CanonicalEqualityValue::try_from_parts([0; EQUALITY_DIGEST_LEN], oversized).is_err()
        );
    }
}
