//! V1 typed secondary-index key codecs and shared parsing entry points.

pub(crate) mod equality;
pub(crate) mod label;
pub(crate) mod range;
pub(crate) mod scan_prefixes;

use crate::encoding::{
    error::EncodingError,
    indexes::{
        equality::{EdgeEqualityIndexKey, EqualityIndexKey, GlobalEdgeEqualityIndexKey},
        label::{EdgeLabelKey, EdgeLabelNeighborKey},
        range::{
            EdgeRangeIndexDirection, EdgeRangeIndexKey, GlobalEdgeRangeIndexKey,
            RangeIndexDirection, RangeIndexKey,
        },
    },
    keys::{KeyPrefix, PREFIX_LEN},
    EdgeId, NodeId,
};
use bytes::BufMut;

pub type PropertyHash = [u8; 4];
pub type ValueHash = [u8; 8];

const INDEX_PREFIX_LEN: usize = core::mem::size_of::<u8>();
pub(crate) const NODE_ID_MAX_LEN: usize = core::mem::size_of::<NodeId>();
pub(crate) const PROPERTY_HASH_MAX_LEN: usize = core::mem::size_of::<PropertyHash>();
pub(crate) const VALUE_HASH_MAX_LEN: usize = core::mem::size_of::<ValueHash>();

#[inline]
pub(crate) fn hash_property_name(name: &str) -> [u8; 4] {
    use std::hash::{Hash, Hasher};
    let mut hasher = siphasher::sip::SipHasher13::new_with_keys(0, 0);
    name.hash(&mut hasher);
    let hash = hasher.finish();
    (hash as u32).to_be_bytes()
}

/// Hash a property value to an 8-byte value
#[inline]
pub(crate) fn hash_property_value(value: &str) -> [u8; 8] {
    use std::hash::{Hash, Hasher};
    let mut hasher = siphasher::sip::SipHasher13::new_with_keys(0, 0);
    value.hash(&mut hasher);
    hasher.finish().to_be_bytes()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum EdgeDirection {
    Out = 0x00,
    In = 0x01,
}

impl EdgeDirection {
    pub(crate) fn from_u8(u: u8) -> Result<Self, EncodingError> {
        match u {
            0x00 => Ok(EdgeDirection::Out),
            0x01 => Ok(EdgeDirection::In),
            _ => Err(EncodingError::InvalidEdgeIndexDirection(u)),
        }
    }

    #[cfg(test)]
    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            EdgeDirection::Out => 0x00,
            EdgeDirection::In => 0x01,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IndexPrefix {
    Equality,
    Range(RangeIndexDirection),
    EdgeEquality,
    EdgeLabel,
    EdgeLabelNeighbor(EdgeDirection),
    EdgeRange(EdgeRangeIndexDirection, EdgeDirection),
    GlobalEdgeEquality,
    GlobalEdgeRange(RangeIndexDirection),
}

impl IndexPrefix {
    pub const fn as_slice(&self) -> &[u8] {
        match self {
            IndexPrefix::Equality => &[0x00],
            IndexPrefix::Range(direction) => match direction {
                RangeIndexDirection::Asc => &[0x01],
                RangeIndexDirection::Desc => &[0x05],
            },
            IndexPrefix::EdgeEquality => &[0x02],
            IndexPrefix::EdgeLabel => &[0x04],
            IndexPrefix::EdgeLabelNeighbor(direction) => match direction {
                EdgeDirection::Out => &[0x10, 0x00],
                EdgeDirection::In => &[0x10, 0x01],
            },
            IndexPrefix::GlobalEdgeEquality => &[0x08],
            IndexPrefix::GlobalEdgeRange(direction) => match direction {
                RangeIndexDirection::Asc => &[0x09],
                RangeIndexDirection::Desc => &[0x0a],
            },
            IndexPrefix::EdgeRange(range_direction, edge_direction) => match range_direction {
                EdgeRangeIndexDirection::Asc => match edge_direction {
                    EdgeDirection::Out => {
                        &[EdgeRangeIndexDirection::Asc as u8, EdgeDirection::Out as u8]
                    }
                    EdgeDirection::In => {
                        &[EdgeRangeIndexDirection::Asc as u8, EdgeDirection::In as u8]
                    }
                },
                EdgeRangeIndexDirection::Desc => match edge_direction {
                    EdgeDirection::Out => &[
                        EdgeRangeIndexDirection::Desc as u8,
                        EdgeDirection::Out as u8,
                    ],
                    EdgeDirection::In => {
                        &[EdgeRangeIndexDirection::Desc as u8, EdgeDirection::In as u8]
                    }
                },
            },
        }
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        if slice.len() < PREFIX_LEN + INDEX_PREFIX_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN + INDEX_PREFIX_LEN,
                actual: slice.len(),
            });
        }

        if KeyPrefix::from_u8(slice[0])? != KeyPrefix::PropertyIndex {
            return Err(EncodingError::InvalidKey(format!(
                "expected PropertyIndex key prefix ({:#04x}), got {:#04x}",
                KeyPrefix::PropertyIndex.as_u8(),
                slice[0]
            )));
        }

        match slice[PREFIX_LEN] {
            0x00 => Ok(IndexPrefix::Equality),
            0x01 => Ok(IndexPrefix::Range(RangeIndexDirection::Asc)),
            0x05 => Ok(IndexPrefix::Range(RangeIndexDirection::Desc)),
            0x02 => Ok(IndexPrefix::EdgeEquality),
            0x04 => Ok(IndexPrefix::EdgeLabel),
            0x08 => Ok(IndexPrefix::GlobalEdgeEquality),
            0x09 => Ok(IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Asc)),
            0x0a => Ok(IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Desc)),
            0x10 => {
                if slice.len() < PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() {
                    return Err(EncodingError::BufferTooShort {
                        expected: PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
                        actual: slice.len(),
                    });
                }

                let edge_direction = EdgeDirection::from_u8(slice[PREFIX_LEN + INDEX_PREFIX_LEN])?;
                Ok(IndexPrefix::EdgeLabelNeighbor(edge_direction))
            }
            0x03 | 0x06 => {
                if slice.len() < PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() {
                    return Err(EncodingError::BufferTooShort {
                        expected: PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
                        actual: slice.len(),
                    });
                }

                let edge_direction = EdgeDirection::from_u8(slice[PREFIX_LEN + INDEX_PREFIX_LEN])?;
                match slice[PREFIX_LEN] {
                    0x03 => Ok(IndexPrefix::EdgeRange(
                        EdgeRangeIndexDirection::Asc,
                        edge_direction,
                    )),
                    0x06 => Ok(IndexPrefix::EdgeRange(
                        EdgeRangeIndexDirection::Desc,
                        edge_direction,
                    )),
                    _ => unreachable!("edge range index prefix was checked above"),
                }
            }
            invalid => Err(EncodingError::InvalidIndexPrefix(invalid)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IndexKey<'a> {
    /// Equality index: property+value -> set of NodeIds
    /// ```text
    /// Key: [0x03][0x00][prop_hash:4][value_hash:8]
    /// Value: RoaringTreemap<NodeId>
    /// ```
    Equality(EqualityIndexKey),

    /// Edge equality index: source+prop+value -> set of EdgeIds
    /// ```text
    /// Out: [0x03][0x02][0x00][source:8][prop_hash:4][value_hash:8]
    /// In:  [0x03][0x02][0x01][target:8][prop_hash:4][value_hash:8]
    /// Value: RoaringTreemap<EdgeId>
    /// ```
    EdgeEquality(EdgeEqualityIndexKey),

    /// Edge label index: label -> set of EdgeIds
    /// ```text
    /// Key: [0x03][0x04][label_hash:8]
    /// Value: RoaringTreemap<EdgeId>
    /// ```
    EdgeLabel(EdgeLabelKey),

    /// Edge-label neighbor index: endpoint+label -> set of opposite NodeIds
    /// ```text
    /// Out: [0x03][0x10][0x00][source:8][label_hash:8]
    /// In:  [0x03][0x10][0x01][target:8][label_hash:8]
    /// Value: RoaringTreemap<NodeId>
    /// ```
    EdgeLabelNeighbor(EdgeLabelNeighborKey),

    /// Global edge equality index: property+value -> set of EdgeIds
    /// ```text
    /// Key: [0x03][0x08][prop_hash:4][value_hash:8]
    /// Value: RoaringTreemap<EdgeId>
    /// ```
    GlobalEdgeEquality(GlobalEdgeEqualityIndexKey),

    /// Range index: property+value+nodeId -> presence
    /// ```text
    /// Asc:  [0x03][0x01][prop_hash:4][value:var][node_id:8]
    /// Desc: [0x03][0x05][prop_hash:4][desc_value:var][node_id:8]
    /// Value: empty/presence
    /// ```
    /// Note BOTH direction use this key variant but reside in different indexes.
    Range(RangeIndexKey<'a>),

    /// Edge range index: source+prop+value+edgeId -> presence
    /// ```text
    /// Out asc:  [0x03][0x03][0x00][source:8][prop_hash:4][value:var][edge_id:8]
    /// In asc:   [0x03][0x03][0x01][target:8][prop_hash:4][value:var][edge_id:8]
    /// Out desc: [0x03][0x06][0x00][source:8][prop_hash:4][desc_value:var][edge_id:8]
    /// In desc:  [0x03][0x06][0x01][target:8][prop_hash:4][desc_value:var][edge_id:8]
    /// Value: empty/presence
    /// ```
    /// Note BOTH direction use this key variant but reside in different indexes.
    EdgeRange(EdgeRangeIndexKey<'a>),

    /// Global edge range index: property+value+edgeId -> presence
    /// ```text
    /// Asc:  [0x03][0x09][prop_hash:4][value:var][edge_id:8]
    /// Desc: [0x03][0x0a][prop_hash:4][desc_value:var][edge_id:8]
    /// Value: empty/presence
    /// ```
    GlobalEdgeRange(GlobalEdgeRangeIndexKey<'a>),
}

impl<'a> IndexKey<'a> {
    #[cfg(test)]
    const fn prefix(&self) -> IndexPrefix {
        match self {
            IndexKey::Equality(_) => IndexPrefix::Equality,
            IndexKey::Range(RangeIndexKey {
                range_direction, ..
            }) => IndexPrefix::Range(*range_direction),
            IndexKey::EdgeEquality(_) => IndexPrefix::EdgeEquality,
            IndexKey::EdgeLabel(_) => IndexPrefix::EdgeLabel,
            IndexKey::EdgeLabelNeighbor(key) => key.index_prefix_for_key(),
            IndexKey::GlobalEdgeEquality(_) => IndexPrefix::GlobalEdgeEquality,
            IndexKey::EdgeRange(EdgeRangeIndexKey {
                edge_direction,
                range_direction,
                ..
            }) => IndexPrefix::EdgeRange(*range_direction, *edge_direction),
            IndexKey::GlobalEdgeRange(GlobalEdgeRangeIndexKey {
                range_direction, ..
            }) => IndexPrefix::GlobalEdgeRange(*range_direction),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn key_prefix(&self) -> KeyPrefix {
        match self {
            IndexKey::Equality(_) => EqualityIndexKey::key_prefix(),
            IndexKey::Range(_) => RangeIndexKey::key_prefix(),
            IndexKey::EdgeEquality(_) => EdgeEqualityIndexKey::key_prefix(),
            IndexKey::EdgeLabel(_) => EdgeLabelKey::key_prefix(),
            IndexKey::EdgeLabelNeighbor(_) => EdgeLabelNeighborKey::key_prefix(),
            IndexKey::EdgeRange(_) => EdgeRangeIndexKey::key_prefix(),
            IndexKey::GlobalEdgeEquality(_) => GlobalEdgeEqualityIndexKey::key_prefix(),
            IndexKey::GlobalEdgeRange(_) => GlobalEdgeRangeIndexKey::key_prefix(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn index_prefix(&self) -> IndexPrefix {
        match self {
            IndexKey::Equality(_) => EqualityIndexKey::index_prefix(),
            IndexKey::Range(key) => key.index_prefix(),
            IndexKey::EdgeEquality(_) => EdgeEqualityIndexKey::index_prefix(),
            IndexKey::EdgeLabel(_) => EdgeLabelKey::index_prefix(),
            IndexKey::EdgeLabelNeighbor(key) => key.index_prefix_for_key(),
            IndexKey::EdgeRange(key) => key.index_prefix(),
            IndexKey::GlobalEdgeEquality(_) => GlobalEdgeEqualityIndexKey::index_prefix(),
            IndexKey::GlobalEdgeRange(key) => key.index_prefix(),
        }
    }

    pub fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        let prefix = IndexPrefix::from_slice(slice)?;
        match prefix {
            IndexPrefix::Equality => Ok(IndexKey::Equality(EqualityIndexKey::parse_from_slice(
                slice,
            )?)),
            IndexPrefix::Range(_) => Ok(IndexKey::Range(RangeIndexKey::parse_from_slice(slice)?)),
            IndexPrefix::EdgeEquality => Ok(IndexKey::EdgeEquality(
                EdgeEqualityIndexKey::parse_from_slice(slice)?,
            )),
            IndexPrefix::EdgeLabel => {
                Ok(IndexKey::EdgeLabel(EdgeLabelKey::parse_from_slice(slice)?))
            }
            IndexPrefix::EdgeLabelNeighbor(_) => Ok(IndexKey::EdgeLabelNeighbor(
                EdgeLabelNeighborKey::parse_from_slice(slice)?,
            )),
            IndexPrefix::EdgeRange(..) => Ok(IndexKey::EdgeRange(
                EdgeRangeIndexKey::parse_from_slice(slice)?,
            )),
            IndexPrefix::GlobalEdgeEquality => Ok(IndexKey::GlobalEdgeEquality(
                GlobalEdgeEqualityIndexKey::parse_from_slice(slice)?,
            )),
            IndexPrefix::GlobalEdgeRange(_) => Ok(IndexKey::GlobalEdgeRange(
                GlobalEdgeRangeIndexKey::parse_from_slice(slice)?,
            )),
        }
    }

    /// Length of the persisted index key in bytes, including the outer property-index key prefix.
    ///
    /// e.g. `[0x03][0x00][prop_hash:4][value_hash:8]` counts as 14 bytes.
    #[inline]
    pub fn encoded_len(&self) -> usize {
        match self {
            IndexKey::Equality(_) => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN
            }
            IndexKey::Range(RangeIndexKey {
                value,
                range_direction,
                ..
            }) => {
                let value_len = match range_direction {
                    RangeIndexDirection::Asc => value.len(),
                    RangeIndexDirection::Desc => {
                        value
                            .as_bytes()
                            .iter()
                            .map(|byte| if *byte == 0x00 { 2 } else { 1 })
                            .sum::<usize>()
                            + 2
                    }
                };
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + PROPERTY_HASH_MAX_LEN
                    + value_len
                    + size_of::<NodeId>()
            }
            IndexKey::EdgeEquality(_) => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeRangeIndexDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN
                    + VALUE_HASH_MAX_LEN
            }
            IndexKey::EdgeLabel(_) => PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN,
            IndexKey::EdgeLabelNeighbor(_) => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + VALUE_HASH_MAX_LEN
            }
            IndexKey::GlobalEdgeEquality(_) => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN
            }
            IndexKey::EdgeRange(EdgeRangeIndexKey {
                value,
                range_direction,
                ..
            }) => {
                let value_len = match range_direction {
                    EdgeRangeIndexDirection::Asc => value.len(),
                    EdgeRangeIndexDirection::Desc => {
                        value
                            .as_bytes()
                            .iter()
                            .map(|byte| if *byte == 0x00 { 2 } else { 1 })
                            .sum::<usize>()
                            + 2
                    }
                };
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeRangeIndexDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN
                    + value_len
                    + size_of::<EdgeId>()
            }
            IndexKey::GlobalEdgeRange(GlobalEdgeRangeIndexKey {
                value,
                range_direction,
                ..
            }) => {
                let value_len = match range_direction {
                    RangeIndexDirection::Asc => value.len(),
                    RangeIndexDirection::Desc => {
                        value
                            .as_bytes()
                            .iter()
                            .map(|byte| if *byte == 0x00 { 2 } else { 1 })
                            .sum::<usize>()
                            + 2
                    }
                };
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + PROPERTY_HASH_MAX_LEN
                    + value_len
                    + size_of::<EdgeId>()
            }
        }
    }
    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        match self {
            IndexKey::Equality(key) => key.encode_into(buf),
            IndexKey::Range(key) => key.encode_into(buf),
            IndexKey::EdgeEquality(key) => key.encode_into(buf),
            IndexKey::EdgeLabel(key) => key.encode_into(buf),
            IndexKey::EdgeLabelNeighbor(key) => key.encode_into(buf),
            IndexKey::EdgeRange(key) => key.encode_into(buf),
            IndexKey::GlobalEdgeEquality(key) => key.encode_into(buf),
            IndexKey::GlobalEdgeRange(key) => key.encode_into(buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::indexes::equality::{
        self as equality_index, EdgeEqualityIndexKey, EqualityIndexKey, GlobalEdgeEqualityIndexKey,
    };
    use crate::encoding::indexes::label::{EdgeLabelKey, EdgeLabelNeighborKey};
    use crate::encoding::indexes::range::{
        EdgeRangeIndexKey, GlobalEdgeRangeIndexKey, RangeIndexKey,
    };
    use std::borrow::Cow;

    #[test]
    fn hash_functions_are_deterministic() {
        assert_eq!(hash_property_name("name"), hash_property_name("name"));
        assert_eq!(hash_property_value("value"), hash_property_value("value"));
        assert_ne!(hash_property_name("name"), hash_property_name("other"));
        assert_ne!(hash_property_value("value"), hash_property_value("other"));
    }

    #[test]
    fn index_prefix_wire_mappings_are_stable() {
        assert_eq!(IndexPrefix::Equality.as_slice(), &[0x00]);
        assert_eq!(
            IndexPrefix::Range(RangeIndexDirection::Asc).as_slice(),
            &[0x01]
        );
        assert_eq!(
            IndexPrefix::Range(RangeIndexDirection::Desc).as_slice(),
            &[0x05]
        );
        assert_eq!(IndexPrefix::EdgeEquality.as_slice(), &[0x02]);
        assert_eq!(IndexPrefix::EdgeLabel.as_slice(), &[0x04]);
        assert_eq!(
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::Out).as_slice(),
            &[0x10, 0x00]
        );
        assert_eq!(
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::In).as_slice(),
            &[0x10, 0x01]
        );
        assert_eq!(
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Asc, EdgeDirection::Out).as_slice(),
            &[0x03, 0x00]
        );
        assert_eq!(
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Asc, EdgeDirection::In).as_slice(),
            &[0x03, 0x01]
        );
        assert_eq!(
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Desc, EdgeDirection::Out).as_slice(),
            &[0x06, 0x00]
        );
        assert_eq!(
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Desc, EdgeDirection::In).as_slice(),
            &[0x06, 0x01]
        );
    }

    #[test]
    fn index_prefix_parse_handles_short_and_invalid_inputs() {
        assert!(matches!(
            IndexPrefix::from_slice(&[]),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 0
            })
        ));
        assert!(matches!(
            IndexPrefix::from_slice(&[0x03]),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 1
            })
        ));
        assert!(matches!(
            IndexPrefix::from_slice(&[0x03, 0x03]),
            Err(EncodingError::BufferTooShort {
                expected: 3,
                actual: 2
            })
        ));
        assert!(matches!(
            IndexPrefix::from_slice(&[0x03, 0x10]),
            Err(EncodingError::BufferTooShort {
                expected: 3,
                actual: 2
            })
        ));
        assert!(matches!(
            IndexPrefix::from_slice(&[0x03, 0x03, 0x02]),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));
        assert!(matches!(
            IndexPrefix::from_slice(&[0x03, 0xFF]),
            Err(EncodingError::InvalidIndexPrefix(0xFF))
        ));
        assert!(matches!(
            IndexPrefix::from_slice(&[0x00, 0x00]),
            Err(EncodingError::InvalidKey(_))
        ));
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x00]).unwrap(),
            IndexPrefix::Equality
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x01]).unwrap(),
            IndexPrefix::Range(RangeIndexDirection::Asc)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x05]).unwrap(),
            IndexPrefix::Range(RangeIndexDirection::Desc)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x02]).unwrap(),
            IndexPrefix::EdgeEquality
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x04]).unwrap(),
            IndexPrefix::EdgeLabel
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x08]).unwrap(),
            IndexPrefix::GlobalEdgeEquality
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x09]).unwrap(),
            IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Asc)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x0a]).unwrap(),
            IndexPrefix::GlobalEdgeRange(RangeIndexDirection::Desc)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x10, 0x00]).unwrap(),
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::Out)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x10, 0x01]).unwrap(),
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::In)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x03, 0x00]).unwrap(),
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Asc, EdgeDirection::Out)
        );
        assert_eq!(
            IndexPrefix::from_slice(&[0x03, 0x06, 0x01]).unwrap(),
            IndexPrefix::EdgeRange(EdgeRangeIndexDirection::Desc, EdgeDirection::In)
        );
    }

    #[test]
    fn edge_direction_wire_mappings_are_stable() {
        assert_eq!(EdgeDirection::Out.as_u8(), 0x00);
        assert_eq!(EdgeDirection::In.as_u8(), 0x01);
        assert_eq!(EdgeDirection::from_u8(0x00).unwrap(), EdgeDirection::Out);
        assert_eq!(EdgeDirection::from_u8(0x01).unwrap(), EdgeDirection::In);
        assert!(matches!(
            EdgeDirection::from_u8(0x02),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));
    }

    #[test]
    fn index_key_contracts_cover_dispatch_len_and_encoding() {
        let equality = IndexKey::Equality(EqualityIndexKey::new([1, 2, 3, 4], [5; 8]));
        let range = IndexKey::Range(RangeIndexKey::new(
            RangeIndexDirection::Desc,
            [2, 3, 4, 5],
            Cow::Borrowed("a\0z"),
            9,
        ));
        let edge_equality = IndexKey::EdgeEquality(EdgeEqualityIndexKey::new(
            equality_index::EdgeDirection::Out,
            7,
            [3, 4, 5, 6],
            [8; 8],
        ));
        let edge_label = IndexKey::EdgeLabel(EdgeLabelKey::new([9; 8]));
        let edge_label_neighbor =
            IndexKey::EdgeLabelNeighbor(EdgeLabelNeighborKey::new(EdgeDirection::In, 17, [10; 8]));
        let global_edge_equality = IndexKey::GlobalEdgeEquality(GlobalEdgeEqualityIndexKey::new(
            [11, 12, 13, 14],
            [15; 8],
        ));
        let edge_range = IndexKey::EdgeRange(EdgeRangeIndexKey::new(
            EdgeDirection::Out,
            EdgeRangeIndexDirection::Desc,
            11,
            [4, 5, 6, 7],
            Cow::Borrowed("x\0y"),
            13,
        ));
        let global_edge_range = IndexKey::GlobalEdgeRange(GlobalEdgeRangeIndexKey::new(
            RangeIndexDirection::Desc,
            [12, 13, 14, 15],
            Cow::Borrowed("g\0h"),
            19,
        ));
        assert!(matches!(
            IndexKey::parse_from_slice(&[KeyPrefix::PropertyIndex.as_u8(), 0x00]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        for key in [
            equality,
            range,
            edge_equality,
            edge_label,
            edge_label_neighbor,
            global_edge_equality,
            edge_range,
            global_edge_range,
        ] {
            let prefix = key.prefix();
            assert_eq!(key.key_prefix(), KeyPrefix::PropertyIndex);
            assert_eq!(key.index_prefix(), prefix);
            let mut encoded = Vec::with_capacity(key.encoded_len());
            key.encode_into(&mut encoded);
            assert_eq!(IndexPrefix::from_slice(&encoded).unwrap(), prefix);
            assert_eq!(IndexKey::parse_from_slice(&encoded).unwrap(), key);
            assert_eq!(encoded.len(), key.encoded_len());
        }
    }
}
