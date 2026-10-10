//! Edge ordered secondary-index key codecs.
//!
//! Ascending values escape embedded zeroes and end with a fixed terminator.
//! Descending values additionally invert every byte. Both directions need
//! the terminator: without it, a value that is a proper prefix of another
//! value in the same index can compare incorrectly once the comparison
//! reaches the trailing edge id, since nothing marks where the value ends
//! and the id begins. V2 generation
//! entries reuse this value encoding through [`decode_range_value`], keeping
//! ordering and validation identical across both physical namespaces.

use crate::encoding::{
    error::EncodingError,
    indexes::{
        EdgeDirection, IndexPrefix, PropertyHash, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
        PROPERTY_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, ID_LEN, PREFIX_LEN},
    v2::keys::codec::read_u64,
    EdgeId, NodeId,
};
use bytes::BufMut;
use std::borrow::Cow;

use super::node::RangeIndexDirection;
#[cfg(test)]
use super::node::RangeIndexKey;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeRangeIndexDirection {
    /// Store range keys in ascending value order.
    #[default]
    Asc = 0x03,
    /// Store range keys in descending value order.
    Desc = 0x06,
}

impl EdgeRangeIndexDirection {
    #[cfg(test)]
    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            EdgeRangeIndexDirection::Asc => 0x03,
            EdgeRangeIndexDirection::Desc => 0x06,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_u8(u: u8) -> Result<Self, EncodingError> {
        match u {
            0x03 => Ok(EdgeRangeIndexDirection::Asc),
            0x06 => Ok(EdgeRangeIndexDirection::Desc),
            _ => Err(EncodingError::InvalidIndexKey(
                "expected edge range index direction".to_string(),
            )),
        }
    }
}

/// Edge range index: source+prop+value+edgeId -> presence
///
/// ```text
/// Out asc:  [0x03][0x03][0x00][source:8][prop_hash:4][asc_value:var][edge_id:8]
/// In asc:   [0x03][0x03][0x01][target:8][prop_hash:4][asc_value:var][edge_id:8]
/// Out desc: [0x03][0x06][0x00][source:8][prop_hash:4][desc_value:var][edge_id:8]
/// In desc:  [0x03][0x06][0x01][target:8][prop_hash:4][desc_value:var][edge_id:8]
/// Value: empty/presence
/// ```
///
/// Note BOTH direction use this key variant but reside in different indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeRangeIndexKey<'a> {
    pub(in crate::encoding::v2::keys::indexes) edge_direction: EdgeDirection,
    pub(in crate::encoding::v2::keys::indexes) range_direction: EdgeRangeIndexDirection,
    source: NodeId,
    property_hash: PropertyHash,
    pub(in crate::encoding::v2::keys::indexes) value: Cow<'a, str>,
    edge_id: EdgeId,
}

impl<'a> EdgeRangeIndexKey<'a> {
    pub fn new(
        edge_direction: EdgeDirection,
        range_direction: EdgeRangeIndexDirection,
        source: NodeId,
        property_hash: PropertyHash,
        value: Cow<'a, str>,
        edge_id: EdgeId,
    ) -> Self {
        Self {
            edge_direction,
            range_direction,
            source,
            property_hash,
            value,
            edge_id,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix(&self) -> IndexPrefix {
        IndexPrefix::EdgeRange(self.range_direction, self.edge_direction)
    }

    pub(crate) fn value(&self) -> &str {
        self.value.as_ref()
    }

    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    pub(crate) const fn edge_id(&self) -> EdgeId {
        self.edge_id
    }

    pub(crate) const fn range_direction(&self) -> EdgeRangeIndexDirection {
        self.range_direction
    }

    /// Parse the edge range key from a slice.
    ///
    /// key is `[0x03][0x03][0x00][source:8][prop_hash:4][asc_value:var][edge_id:8]`
    ///
    /// starts from the edge-range index prefix because the first byte is handled by `Key` parsing.
    ///
    /// e.g. `[0x03][0x00][source:8][prop_hash:4][asc_value:var][edge_id:8]` is what is parsed from the slice
    pub fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        // Checks AT LEAST the expected length
        // Variable length value means `value` and `node_id` access should be checked
        let expected = PREFIX_LEN
            + INDEX_PREFIX_LEN
            + size_of::<EdgeDirection>()
            + NODE_ID_MAX_LEN
            + PROPERTY_HASH_MAX_LEN
            + ID_LEN;
        if slice.len() < expected {
            return Err(EncodingError::BufferTooShort {
                expected,
                actual: slice.len(),
            });
        }

        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if !matches!(key_prefix, KeyPrefix::PropertyIndex) {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        let (edge_direction, range_direction) = match IndexPrefix::from_slice(slice)? {
            IndexPrefix::EdgeRange(range_direction, edge_direction) => {
                (edge_direction, range_direction)
            }
            IndexPrefix::Equality
            | IndexPrefix::Range(_)
            | IndexPrefix::EdgeEquality
            | IndexPrefix::EdgeLabel
            | IndexPrefix::EdgeLabelNeighbor(_)
            | IndexPrefix::GlobalEdgeEquality
            | IndexPrefix::GlobalEdgeRange(_) => {
                return Err(EncodingError::InvalidIndexKey(
                    "expected edge range index key".to_string(),
                ));
            }
        };

        let source = read_u64(
            slice,
            PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
        )?;

        let property_hash =
            slice[PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() + NODE_ID_MAX_LEN
                ..PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN]
                .try_into()
                .expect("property hash slice is 4 bytes");

        let value_bytes = slice
            .get(
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN..slice.len() - ID_LEN,
            )
            .ok_or(EncodingError::BufferTooShort {
                expected: PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN
                    + ID_LEN,
                actual: slice.len(),
            })?;
        let value = match range_direction {
            EdgeRangeIndexDirection::Asc => {
                if !value_bytes.ends_with(&[0x00, 0x00]) {
                    return Err(EncodingError::InvalidIndexKey(
                        "ascending range value missing terminator".to_string(),
                    ));
                }

                let mut decoded = Vec::with_capacity(value_bytes.len().saturating_sub(2));
                let mut index = 0;
                let value_body_len = value_bytes.len() - 2;
                while index < value_body_len {
                    let byte = value_bytes[index];
                    if byte == 0x00 {
                        if value_bytes.get(index + 1) != Some(&0xFF) {
                            return Err(EncodingError::InvalidIndexKey(
                                "invalid ascending range value escape".to_string(),
                            ));
                        }
                        decoded.push(0x00);
                        index += 2;
                    } else {
                        decoded.push(byte);
                        index += 1;
                    }
                }

                Cow::Owned(std::str::from_utf8(&decoded)?.to_owned())
            }
            EdgeRangeIndexDirection::Desc => {
                if !value_bytes.ends_with(&[0xFF, 0xFE]) {
                    return Err(EncodingError::InvalidIndexKey(
                        "descending range value missing terminator".to_string(),
                    ));
                }

                let mut decoded = Vec::with_capacity(value_bytes.len().saturating_sub(2));
                let mut index = 0;
                let value_body_len = value_bytes.len() - 2;
                while index < value_body_len {
                    let byte = value_bytes[index];
                    if byte == 0xFF {
                        if value_bytes.get(index + 1) != Some(&0x00) {
                            return Err(EncodingError::InvalidIndexKey(
                                "invalid descending range value escape".to_string(),
                            ));
                        }
                        decoded.push(0x00);
                        index += 2;
                    } else {
                        decoded.push(!byte);
                        index += 1;
                    }
                }

                Cow::Owned(std::str::from_utf8(&decoded)?.to_owned())
            }
        };

        // prefix + edge-range index prefix + source + property hash + value |> edge id
        let edge_id = read_u64(slice, slice.len() - ID_LEN)?;
        Ok(Self {
            edge_direction,
            range_direction,
            source,
            property_hash,
            value,
            edge_id,
        })
    }

    /// Encode the edge range key into a buffer.
    ///
    /// For ascending direction, the value is encoded as follows:
    /// - Each byte is written as-is
    /// - If a byte is 0x00, it is escaped with 0x00 0xFF
    /// - The final bytes are 0x00 0x00 to indicate the end of the value.
    ///
    /// For descending direction, the value is encoded as follows:
    /// - Each byte is inverted (0x00 -> 0xFF, 0xFF -> 0x00)
    /// - If a byte is 0x00, it is escaped with 0xFF 0x00
    /// - The final bytes are 0xFF 0xFE to indicate the end of the value.
    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::EdgeRange(self.range_direction, self.edge_direction).as_slice());
        buf.put_u64(self.source);
        buf.put_slice(&self.property_hash);
        match self.range_direction {
            EdgeRangeIndexDirection::Asc => {
                for byte in self.value.as_bytes().iter() {
                    buf.put_u8(*byte);
                    if *byte == 0x00 {
                        buf.put_u8(0xFF);
                    }
                }
                buf.put_u8(0x00);
                buf.put_u8(0x00);
            }
            EdgeRangeIndexDirection::Desc => {
                for byte in self.value.as_bytes().iter() {
                    buf.put_u8(!byte);
                    if *byte == 0x00 {
                        buf.put_u8(!0xFF);
                    }
                }
                buf.put_u8(!0x00);
                buf.put_u8(!0x01);
            }
        }
        buf.put_u64(self.edge_id);
    }
}

impl From<&EdgeRangeIndexKey<'_>> for KeyPrefix {
    fn from(_: &EdgeRangeIndexKey<'_>) -> KeyPrefix {
        EdgeRangeIndexKey::key_prefix()
    }
}

impl From<&EdgeRangeIndexKey<'_>> for IndexPrefix {
    fn from(key: &EdgeRangeIndexKey<'_>) -> IndexPrefix {
        key.index_prefix()
    }
}

/// Global edge range index: property+value+edgeId -> presence.
///
/// ```text
/// Asc:  [0x03][0x09][prop_hash:4][asc_value:var][edge_id:8]
/// Desc: [0x03][0x0a][prop_hash:4][desc_value:var][edge_id:8]
/// Value: empty/presence
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GlobalEdgeRangeIndexKey<'a> {
    pub(in crate::encoding::v2::keys::indexes) range_direction: RangeIndexDirection,
    property_hash: PropertyHash,
    pub(in crate::encoding::v2::keys::indexes) value: Cow<'a, str>,
    edge_id: EdgeId,
}

impl<'a> GlobalEdgeRangeIndexKey<'a> {
    pub(crate) fn new(
        range_direction: RangeIndexDirection,
        property_hash: PropertyHash,
        value: Cow<'a, str>,
        edge_id: EdgeId,
    ) -> Self {
        Self {
            range_direction,
            property_hash,
            value,
            edge_id,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix(&self) -> IndexPrefix {
        IndexPrefix::GlobalEdgeRange(self.range_direction)
    }

    pub(crate) fn value(&self) -> &str {
        self.value.as_ref()
    }

    pub(crate) const fn edge_id(&self) -> EdgeId {
        self.edge_id
    }

    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    pub(crate) const fn direction(&self) -> RangeIndexDirection {
        self.range_direction
    }

    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + ID_LEN;
        if slice.len() < expected {
            return Err(EncodingError::BufferTooShort {
                expected,
                actual: slice.len(),
            });
        }

        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if key_prefix != Self::key_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        let range_direction = match IndexPrefix::from_slice(slice)? {
            IndexPrefix::GlobalEdgeRange(direction) => direction,
            IndexPrefix::Equality
            | IndexPrefix::Range(_)
            | IndexPrefix::EdgeEquality
            | IndexPrefix::EdgeLabel
            | IndexPrefix::EdgeLabelNeighbor(_)
            | IndexPrefix::EdgeRange(..)
            | IndexPrefix::GlobalEdgeEquality => {
                return Err(EncodingError::InvalidIndexKey(
                    "expected global edge range index key".to_string(),
                ));
            }
        };

        let property_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
            .try_into()
            .expect("property hash slice is 4 bytes");
        let value_bytes = slice
            .get(PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN..slice.len() - ID_LEN)
            .ok_or(EncodingError::BufferTooShort {
                expected: PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + ID_LEN,
                actual: slice.len(),
            })?;
        let value = match range_direction {
            RangeIndexDirection::Asc => {
                if !value_bytes.ends_with(&[0x00, 0x00]) {
                    return Err(EncodingError::InvalidIndexKey(
                        "ascending global edge range value missing terminator".to_string(),
                    ));
                }

                let mut decoded = Vec::with_capacity(value_bytes.len().saturating_sub(2));
                let mut index = 0;
                let value_body_len = value_bytes.len() - 2;
                while index < value_body_len {
                    let byte = value_bytes[index];
                    if byte == 0x00 {
                        if value_bytes.get(index + 1) != Some(&0xFF) {
                            return Err(EncodingError::InvalidIndexKey(
                                "invalid ascending global edge range value escape".to_string(),
                            ));
                        }
                        decoded.push(0x00);
                        index += 2;
                    } else {
                        decoded.push(byte);
                        index += 1;
                    }
                }

                Cow::Owned(std::str::from_utf8(&decoded)?.to_owned())
            }
            RangeIndexDirection::Desc => {
                if !value_bytes.ends_with(&[0xFF, 0xFE]) {
                    return Err(EncodingError::InvalidIndexKey(
                        "descending global edge range value missing terminator".to_string(),
                    ));
                }

                let mut decoded = Vec::with_capacity(value_bytes.len().saturating_sub(2));
                let mut index = 0;
                let value_body_len = value_bytes.len() - 2;
                while index < value_body_len {
                    let byte = value_bytes[index];
                    if byte == 0xFF {
                        if value_bytes.get(index + 1) != Some(&0x00) {
                            return Err(EncodingError::InvalidIndexKey(
                                "invalid descending global edge range value escape".to_string(),
                            ));
                        }
                        decoded.push(0x00);
                        index += 2;
                    } else {
                        decoded.push(!byte);
                        index += 1;
                    }
                }

                Cow::Owned(std::str::from_utf8(&decoded)?.to_owned())
            }
        };
        let edge_id = read_u64(slice, slice.len() - ID_LEN)?;

        Ok(Self::new(range_direction, property_hash, value, edge_id))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(self.index_prefix().as_slice());
        buf.put_slice(&self.property_hash);
        match self.range_direction {
            RangeIndexDirection::Asc => {
                for byte in self.value.as_bytes().iter() {
                    buf.put_u8(*byte);
                    if *byte == 0x00 {
                        buf.put_u8(0xFF);
                    }
                }
                buf.put_u8(0x00);
                buf.put_u8(0x00);
            }
            RangeIndexDirection::Desc => {
                for byte in self.value.as_bytes().iter() {
                    buf.put_u8(!byte);
                    if *byte == 0x00 {
                        buf.put_u8(!0xFF);
                    }
                }
                buf.put_u8(!0x00);
                buf.put_u8(!0x01);
            }
        }
        buf.put_u64(self.edge_id);
    }
}

impl From<&GlobalEdgeRangeIndexKey<'_>> for KeyPrefix {
    fn from(_: &GlobalEdgeRangeIndexKey<'_>) -> KeyPrefix {
        GlobalEdgeRangeIndexKey::key_prefix()
    }
}

impl From<&GlobalEdgeRangeIndexKey<'_>> for IndexPrefix {
    fn from(key: &GlobalEdgeRangeIndexKey<'_>) -> IndexPrefix {
        key.index_prefix()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{indexes::PropertyIndexKey, keys::DataKeyKind};

    const PROP: PropertyHash = [1, 2, 3, 4];

    #[test]
    fn range_index_asc_layout_round_trips() {
        let key = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Asc,
            PROP,
            Cow::Borrowed("az"),
            0x0102_0304_0506_0708,
        )));
        let encoded = key.clone().to_bytes();

        let mut expected = vec![0x03, 0x01];
        expected.extend_from_slice(&PROP);
        expected.extend_from_slice(b"az");
        expected.extend_from_slice(&[0x00, 0x00]);
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        assert_eq!(encoded.as_ref(), expected.as_slice());

        let parsed = DataKeyKind::parse_from_slice(&encoded).unwrap();
        assert_eq!(parsed, key);

        let DataKeyKind::PropertyIndex(PropertyIndexKey::Range(parsed)) = parsed else {
            panic!("expected range index key");
        };
        assert_eq!(parsed.node_id(), 0x0102_0304_0506_0708);
        assert_eq!(parsed.property_hash(), &PROP);
    }

    #[test]
    fn range_index_desc_layout_decodes_original_value() {
        let key = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Desc,
            PROP,
            Cow::Borrowed("az"),
            0x0102_0304_0506_0708,
        )));
        let encoded = key.clone().to_bytes();

        let mut expected = vec![0x03, 0x05];
        expected.extend_from_slice(&PROP);
        expected.extend_from_slice(&[0x9E, 0x85, 0xFF, 0xFE]);
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        assert_eq!(encoded.as_ref(), expected.as_slice());

        let DataKeyKind::PropertyIndex(PropertyIndexKey::Range(parsed)) =
            DataKeyKind::parse_from_slice(&encoded).unwrap()
        else {
            panic!("expected range index key");
        };
        assert_eq!(parsed.value.as_ref(), "az");
        assert_eq!(parsed.direction(), RangeIndexDirection::Desc);
    }

    #[test]
    fn edge_range_layouts_encode_range_then_edge_direction() {
        let out_desc =
            DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(EdgeRangeIndexKey::new(
                EdgeDirection::Out,
                EdgeRangeIndexDirection::Desc,
                7,
                PROP,
                Cow::Borrowed("az"),
                11,
            )))
            .to_bytes();
        let in_asc =
            DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(EdgeRangeIndexKey::new(
                EdgeDirection::In,
                EdgeRangeIndexDirection::Asc,
                7,
                PROP,
                Cow::Borrowed("az"),
                11,
            )))
            .to_bytes();

        let mut expected_out_desc = vec![0x03, 0x06, 0x00];
        expected_out_desc.extend_from_slice(&7u64.to_be_bytes());
        expected_out_desc.extend_from_slice(&PROP);
        expected_out_desc.extend_from_slice(&[0x9E, 0x85, 0xFF, 0xFE]);
        expected_out_desc.extend_from_slice(&11u64.to_be_bytes());

        let mut expected_in_asc = vec![0x03, 0x03, 0x01];
        expected_in_asc.extend_from_slice(&7u64.to_be_bytes());
        expected_in_asc.extend_from_slice(&PROP);
        expected_in_asc.extend_from_slice(b"az");
        expected_in_asc.extend_from_slice(&[0x00, 0x00]);
        expected_in_asc.extend_from_slice(&11u64.to_be_bytes());

        assert_eq!(out_desc.as_ref(), expected_out_desc.as_slice());
        assert_eq!(in_asc.as_ref(), expected_in_asc.as_slice());
        assert_eq!(
            DataKeyKind::parse_from_slice(&out_desc).unwrap(),
            DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(EdgeRangeIndexKey::new(
                EdgeDirection::Out,
                EdgeRangeIndexDirection::Desc,
                7,
                PROP,
                Cow::Borrowed("az"),
                11,
            )))
        );
        let DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(parsed)) =
            DataKeyKind::parse_from_slice(&out_desc).unwrap()
        else {
            panic!("expected edge range key");
        };
        assert_eq!(parsed.edge_id(), 11);
        assert_eq!(parsed.property_hash(), &PROP);
        assert_eq!(parsed.range_direction(), EdgeRangeIndexDirection::Desc);
        assert_eq!(
            DataKeyKind::parse_from_slice(&in_asc).unwrap(),
            DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(EdgeRangeIndexKey::new(
                EdgeDirection::In,
                EdgeRangeIndexDirection::Asc,
                7,
                PROP,
                Cow::Borrowed("az"),
                11,
            )))
        );
    }

    #[test]
    fn range_direction_helpers_cover_all_wire_values() {
        assert_eq!(RangeIndexDirection::Asc.as_u8(), 0x01);
        assert_eq!(RangeIndexDirection::Desc.as_u8(), 0x05);
        assert_eq!(
            RangeIndexDirection::from_slice(&[0x01]).unwrap(),
            RangeIndexDirection::Asc
        );
        assert_eq!(
            RangeIndexDirection::from_slice(&[0x05]).unwrap(),
            RangeIndexDirection::Desc
        );
        assert!(matches!(
            RangeIndexDirection::from_slice(&[0x02]),
            Err(EncodingError::InvalidIndexKey(_))
        ));
        assert_eq!(EdgeRangeIndexDirection::Asc.as_u8(), 0x03);
        assert_eq!(EdgeRangeIndexDirection::Desc.as_u8(), 0x06);
        assert_eq!(
            EdgeRangeIndexDirection::from_u8(0x03).unwrap(),
            EdgeRangeIndexDirection::Asc
        );
        assert_eq!(
            EdgeRangeIndexDirection::from_u8(0x06).unwrap(),
            EdgeRangeIndexDirection::Desc
        );
        assert!(matches!(
            EdgeRangeIndexDirection::from_u8(0x04),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }

    #[test]
    fn range_key_prefix_contracts_cover_all_shapes() {
        let range = RangeIndexKey::new(RangeIndexDirection::Desc, PROP, Cow::Borrowed("a"), 1);
        assert_eq!(RangeIndexKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(
            range.index_prefix(),
            IndexPrefix::Range(RangeIndexDirection::Desc)
        );
        assert_eq!(KeyPrefix::from(&range), KeyPrefix::PropertyIndex);
        assert_eq!(
            IndexPrefix::from(&range),
            IndexPrefix::Range(RangeIndexDirection::Desc)
        );

        let edge = EdgeRangeIndexKey::new(
            EdgeDirection::In,
            EdgeRangeIndexDirection::Asc,
            2,
            PROP,
            Cow::Borrowed("b"),
            3,
        );
        assert_eq!(EdgeRangeIndexKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(
            edge.index_prefix(),
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Asc, EdgeDirection::In)
        );
        assert_eq!(KeyPrefix::from(&edge), KeyPrefix::PropertyIndex);
        assert_eq!(
            IndexPrefix::from(&edge),
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Asc, EdgeDirection::In)
        );

        let global =
            GlobalEdgeRangeIndexKey::new(RangeIndexDirection::Asc, PROP, Cow::Borrowed("c"), 4);
        assert_eq!(
            GlobalEdgeRangeIndexKey::key_prefix(),
            KeyPrefix::PropertyIndex
        );
        assert_eq!(
            global.index_prefix(),
            IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Asc)
        );
        assert_eq!(KeyPrefix::from(&global), KeyPrefix::PropertyIndex);
        assert_eq!(
            IndexPrefix::from(&global),
            IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Asc)
        );
    }

    #[test]
    fn range_keys_sort_by_direction() {
        let asc_a = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Asc,
            PROP,
            Cow::Borrowed("a"),
            1,
        )))
        .to_bytes();
        let asc_b = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Asc,
            PROP,
            Cow::Borrowed("b"),
            2,
        )))
        .to_bytes();
        let desc_a = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Desc,
            PROP,
            Cow::Borrowed("a"),
            1,
        )))
        .to_bytes();
        let desc_b = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Desc,
            PROP,
            Cow::Borrowed("b"),
            2,
        )))
        .to_bytes();

        assert!(asc_a < asc_b);
        assert!(desc_b < desc_a);
    }

    #[test]
    fn ascending_range_index_key_round_trips_embedded_null() {
        let key = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Asc,
            PROP,
            Cow::Borrowed("a\0z"),
            1,
        )));
        let encoded = key.clone().to_bytes();

        let DataKeyKind::PropertyIndex(PropertyIndexKey::Range(parsed)) =
            DataKeyKind::parse_from_slice(&encoded).unwrap()
        else {
            panic!("expected range index key");
        };
        assert_eq!(parsed.value(), "a\0z");
        assert_eq!(parsed.direction(), RangeIndexDirection::Asc);
    }

    #[test]
    fn ascending_range_index_keys_sort_correctly_when_one_value_is_a_prefix_of_another() {
        // regression test: a proper-prefix value must always sort before the longer
        // value it prefixes, regardless of what bytes the trailing node id happens
        // to contribute. before the ascending encoding gained a terminator, this
        // could invert depending on the id's leading byte or an embedded null in
        // the longer value.
        let shorter = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Asc,
            PROP,
            Cow::Borrowed("a"),
            5,
        )))
        .to_bytes();
        let longer_with_embedded_null = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(
            RangeIndexKey::new(RangeIndexDirection::Asc, PROP, Cow::Borrowed("a\0"), 1),
        ))
        .to_bytes();

        assert!(shorter < longer_with_embedded_null);
    }

    #[test]
    fn global_edge_range_index_key_has_exact_layout_and_round_trips() {
        let asc = DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeRange(
            GlobalEdgeRangeIndexKey::new(
                RangeIndexDirection::Asc,
                PROP,
                Cow::Borrowed("value"),
                0x0102_0304_0506_0708,
            ),
        ));
        let encoded = asc.to_bytes();
        let mut expected = vec![0x03, 0x09];
        expected.extend_from_slice(&PROP);
        expected.extend_from_slice(b"value");
        expected.extend_from_slice(&[0x00, 0x00]);
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        assert_eq!(encoded.as_ref(), expected.as_slice());
        assert_eq!(DataKeyKind::parse_from_slice(&encoded).unwrap(), asc);

        let desc = DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeRange(
            GlobalEdgeRangeIndexKey::new(RangeIndexDirection::Desc, PROP, Cow::Borrowed("a\0z"), 2),
        ));
        let encoded = desc.to_bytes();
        let DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeRange(parsed)) =
            DataKeyKind::parse_from_slice(&encoded).unwrap()
        else {
            panic!("expected global edge range key");
        };
        assert_eq!(parsed.value.as_ref(), "a\0z");
        assert_eq!(parsed.edge_id(), 2);
        assert_eq!(parsed.property_hash(), &PROP);
        assert_eq!(parsed.direction(), RangeIndexDirection::Desc);
    }

    #[test]
    fn range_parsers_reject_malformed_buffers() {
        assert!(matches!(
            RangeIndexKey::parse_from_slice(&[0x01, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut missing_desc_terminator = vec![0x03, 0x05];
        missing_desc_terminator.extend_from_slice(&PROP);
        missing_desc_terminator.extend_from_slice(b"not-desc-encoded");
        missing_desc_terminator.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            RangeIndexKey::parse_from_slice(&missing_desc_terminator),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&[0x03, 0x03, 0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));
    }

    #[test]
    fn range_parser_rejects_wrong_prefixes_and_malformed_values() {
        let mut wrong_key_prefix = vec![0x02, 0x01];
        wrong_key_prefix.extend_from_slice(&PROP);
        wrong_key_prefix.extend_from_slice(b"value");
        wrong_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            RangeIndexKey::parse_from_slice(&wrong_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_index_kind = vec![0x03, 0x00];
        wrong_index_kind.extend_from_slice(&PROP);
        wrong_index_kind.extend_from_slice(b"value");
        wrong_index_kind.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            RangeIndexKey::parse_from_slice(&wrong_index_kind),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut invalid_utf8 = vec![0x03, 0x01];
        invalid_utf8.extend_from_slice(&PROP);
        invalid_utf8.push(0xFF);
        invalid_utf8.extend_from_slice(&[0x00, 0x00]);
        invalid_utf8.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            RangeIndexKey::parse_from_slice(&invalid_utf8),
            Err(EncodingError::InvalidUtf8(_))
        ));

        let mut invalid_desc_escape = vec![0x03, 0x05];
        invalid_desc_escape.extend_from_slice(&PROP);
        invalid_desc_escape.extend_from_slice(&[0xFF, 0x01, 0xFF, 0xFE]);
        invalid_desc_escape.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            RangeIndexKey::parse_from_slice(&invalid_desc_escape),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let key = DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Desc,
            PROP,
            Cow::Borrowed("a\0z"),
            1,
        )));
        let encoded = key.to_bytes();
        let DataKeyKind::PropertyIndex(PropertyIndexKey::Range(parsed)) =
            DataKeyKind::parse_from_slice(&encoded).unwrap()
        else {
            panic!("expected range key");
        };
        assert_eq!(parsed.value.as_ref(), "a\0z");
    }

    #[test]
    fn edge_range_parser_rejects_wrong_prefixes_and_malformed_values() {
        let mut wrong_key_prefix = vec![0x02, 0x03, EdgeDirection::Out.as_u8()];
        wrong_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_key_prefix.extend_from_slice(&PROP);
        wrong_key_prefix.extend_from_slice(b"value");
        wrong_key_prefix.extend_from_slice(&2u64.to_be_bytes());
        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&wrong_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_index_kind = vec![0x03, 0x01, EdgeDirection::Out.as_u8()];
        wrong_index_kind.extend_from_slice(&1u64.to_be_bytes());
        wrong_index_kind.extend_from_slice(&PROP);
        wrong_index_kind.extend_from_slice(b"value");
        wrong_index_kind.extend_from_slice(&2u64.to_be_bytes());
        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&wrong_index_kind),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut invalid_edge_direction = vec![0x03, 0x03, 0x02];
        invalid_edge_direction.extend_from_slice(&1u64.to_be_bytes());
        invalid_edge_direction.extend_from_slice(&PROP);
        invalid_edge_direction.extend_from_slice(b"value");
        invalid_edge_direction.extend_from_slice(&2u64.to_be_bytes());
        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&invalid_edge_direction),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));

        let mut invalid_utf8 = vec![0x03, 0x03, EdgeDirection::Out.as_u8()];
        invalid_utf8.extend_from_slice(&1u64.to_be_bytes());
        invalid_utf8.extend_from_slice(&PROP);
        invalid_utf8.push(0xFF);
        invalid_utf8.extend_from_slice(&[0x00, 0x00]);
        invalid_utf8.extend_from_slice(&2u64.to_be_bytes());
        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&invalid_utf8),
            Err(EncodingError::InvalidUtf8(_))
        ));

        let mut missing_desc_terminator = vec![0x03, 0x06, EdgeDirection::Out.as_u8()];
        missing_desc_terminator.extend_from_slice(&1u64.to_be_bytes());
        missing_desc_terminator.extend_from_slice(&PROP);
        missing_desc_terminator.extend_from_slice(b"not-desc");
        missing_desc_terminator.extend_from_slice(&2u64.to_be_bytes());
        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&missing_desc_terminator),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut invalid_desc_escape = vec![0x03, 0x06, EdgeDirection::Out.as_u8()];
        invalid_desc_escape.extend_from_slice(&1u64.to_be_bytes());
        invalid_desc_escape.extend_from_slice(&PROP);
        invalid_desc_escape.extend_from_slice(&[0xFF, 0x01, 0xFF, 0xFE]);
        invalid_desc_escape.extend_from_slice(&2u64.to_be_bytes());
        assert!(matches!(
            EdgeRangeIndexKey::parse_from_slice(&invalid_desc_escape),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let key = DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(EdgeRangeIndexKey::new(
            EdgeDirection::In,
            EdgeRangeIndexDirection::Desc,
            1,
            PROP,
            Cow::Borrowed("a\0z"),
            2,
        )));
        let encoded = key.to_bytes();
        let DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(parsed)) =
            DataKeyKind::parse_from_slice(&encoded).unwrap()
        else {
            panic!("expected edge range key");
        };
        assert_eq!(parsed.value.as_ref(), "a\0z");
        assert_eq!(parsed.edge_direction, EdgeDirection::In);
    }

    #[test]
    fn global_edge_range_parser_rejects_wrong_prefixes_and_malformed_values() {
        assert!(matches!(
            GlobalEdgeRangeIndexKey::parse_from_slice(&[0x03, 0x09, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut wrong_key_prefix = vec![0x02, 0x09];
        wrong_key_prefix.extend_from_slice(&PROP);
        wrong_key_prefix.extend_from_slice(b"value");
        wrong_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            GlobalEdgeRangeIndexKey::parse_from_slice(&wrong_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_index_kind = vec![0x03, 0x01];
        wrong_index_kind.extend_from_slice(&PROP);
        wrong_index_kind.extend_from_slice(b"value");
        wrong_index_kind.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            GlobalEdgeRangeIndexKey::parse_from_slice(&wrong_index_kind),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut invalid_utf8 = vec![0x03, 0x09];
        invalid_utf8.extend_from_slice(&PROP);
        invalid_utf8.push(0xFF);
        invalid_utf8.extend_from_slice(&[0x00, 0x00]);
        invalid_utf8.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            GlobalEdgeRangeIndexKey::parse_from_slice(&invalid_utf8),
            Err(EncodingError::InvalidUtf8(_))
        ));

        let mut missing_desc_terminator = vec![0x03, 0x0a];
        missing_desc_terminator.extend_from_slice(&PROP);
        missing_desc_terminator.extend_from_slice(b"not-desc");
        missing_desc_terminator.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            GlobalEdgeRangeIndexKey::parse_from_slice(&missing_desc_terminator),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut invalid_desc_escape = vec![0x03, 0x0a];
        invalid_desc_escape.extend_from_slice(&PROP);
        invalid_desc_escape.extend_from_slice(&[0xFF, 0x01, 0xFF, 0xFE]);
        invalid_desc_escape.extend_from_slice(&1u64.to_be_bytes());
        assert!(matches!(
            GlobalEdgeRangeIndexKey::parse_from_slice(&invalid_desc_escape),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }
}
