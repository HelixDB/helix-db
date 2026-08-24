//! Typed current-format graph-property encoding and decoding.
//!
//! This module owns the rkyv boundary for property rows and for deterministic
//! canonical property identities used by index lifecycle partition mappings
//! and collision checks.
//! Callers must not reconstruct either representation outside this module.

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use rkyv::{
    util::AlignedVec,
    with::{AsVec, With},
};

use super::{property::Property, property_value::PropertyValue};
use crate::encoding::error::EncodingError;

pub(crate) fn datetime_millis_to_rfc3339(millis: i64) -> Option<String> {
    chrono::DateTime::<Utc>::from_timestamp_millis(millis)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

const SIGNED_I64_SORT_MASK: u64 = 0x8000_0000_0000_0000;
pub(crate) fn sortable_i64_index_string(value: i64) -> String {
    format!("{:020}", (value as u64) ^ SIGNED_I64_SORT_MASK)
}

pub(super) const PROPERTY_ALIGNMENT: usize = core::mem::align_of::<rkyv::Archived<Vec<Property>>>();
#[inline]
fn align_for_rkyv(data: &[u8]) -> AlignedVec<PROPERTY_ALIGNMENT> {
    if data.is_empty() {
        return AlignedVec::<PROPERTY_ALIGNMENT>::new();
    }

    let mut aligned = AlignedVec::<PROPERTY_ALIGNMENT>::new();
    aligned.extend_from_slice(data);
    aligned
}

#[inline]
pub(crate) fn encode_properties(properties: &[Property]) -> Bytes {
    if properties.is_empty() {
        return Bytes::new();
    }

    let properties = With::<&[Property], AsVec>::cast(&properties);

    let bytes =
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(properties, Vec::<u8>::new())
            .expect("Property serialization should not fail");

    Bytes::from(bytes)
}

/// Encodes one property value as the canonical type-preserving index identity.
///
/// V2 tenant-partition mappings persist these bytes inside their typed work
/// values and hash the same bytes into mapping keys. Callers must not reproduce
/// this rkyv boundary or substitute display/JSON formatting.
#[inline]
pub(crate) fn encode_index_partition_value(value: &PropertyValue) -> Bytes {
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .expect("PropertyValue identity serialization should not fail");
    Bytes::copy_from_slice(&encoded)
}

/// Decodes property-row bytes into owned properties.
#[inline]
pub fn decode_properties(data: &[u8]) -> Result<Vec<Property>, EncodingError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let owned = align_for_rkyv(data);
    let archived = rkyv::access::<rkyv::Archived<Vec<Property>>, rkyv::rancor::Error>(&owned)
        .map_err(|e| EncodingError::Rkyv(e.to_string()))?;

    rkyv::deserialize::<Vec<Property>, rkyv::rancor::Error>(archived)
        .map_err(|e| EncodingError::Rkyv(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::encoding::property::{property::Property, property_value::PropertyValue};

    fn all_variant_properties() -> Vec<Property> {
        let mut object = BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::Bool(true));

        vec![
            Property::new("null", PropertyValue::Null),
            Property::bool("bool", true),
            Property::i64("i64", -7),
            Property::datetime_millis("datetime", 0),
            Property::f64("f64", 1.5),
            Property::new("f32", PropertyValue::F32(2.5)),
            Property::string("string", "value"),
            Property::bytes("bytes", vec![1, 2, 3]),
            Property::i64_array("i64_array", vec![1, 2]),
            Property::f64_array("f64_array", vec![1.0, 2.0]),
            Property::new("f32_array", PropertyValue::F32Array(vec![1.0, 2.0])),
            Property::string_array("string_array", vec!["a".to_string(), "b".to_string()]),
            Property::new("array", PropertyValue::Array(vec![PropertyValue::I64(1)])),
            Property::new("object", PropertyValue::Object(object)),
        ]
    }

    #[test]
    fn index_partition_value_is_deterministic_and_type_preserving() {
        let integer = PropertyValue::I64(7);
        assert_eq!(
            encode_index_partition_value(&integer),
            encode_index_partition_value(&integer)
        );
        assert_ne!(
            encode_index_partition_value(&integer),
            encode_index_partition_value(&PropertyValue::F64(7.0))
        );
    }

    #[test]
    fn empty_properties_encode_as_empty_bytes() {
        assert!(encode_properties(&[]).is_empty());
        assert_eq!(decode_properties(&[]).unwrap(), Vec::<Property>::new());
    }

    #[test]
    fn rkyv_alignment_helper_preserves_empty_and_non_empty_bytes() {
        assert!(align_for_rkyv(&[]).is_empty());

        let aligned = align_for_rkyv(b"abc");
        assert_eq!(aligned.as_slice(), b"abc");
    }

    #[test]
    fn properties_round_trip_all_variants() {
        let properties = all_variant_properties();
        let encoded = encode_properties(&properties);
        let decoded = decode_properties(&encoded).unwrap();

        assert_eq!(decoded, properties);
    }

    #[test]
    fn invalid_property_bytes_return_rkyv_error() {
        assert!(matches!(
            decode_properties(&[1, 2, 3]),
            Err(EncodingError::Rkyv(_))
        ));
    }

    #[test]
    fn datetime_and_sortable_i64_helpers_cover_boundaries() {
        assert_eq!(
            datetime_millis_to_rfc3339(0).as_deref(),
            Some("1970-01-01T00:00:00.000Z")
        );
        assert_eq!(datetime_millis_to_rfc3339(i64::MAX), None);

        let ordered = [i64::MIN, -1, 0, 1, i64::MAX]
            .into_iter()
            .map(sortable_i64_index_string)
            .collect::<Vec<_>>();
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
