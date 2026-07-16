//! V1 equality-index key codecs and byte-compatible parsing contracts.

use crate::encoding::{
    error::EncodingError,
    indexes::{
        IndexPrefix, PropertyHash, ValueHash, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
        PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    v1::read_u64,
    NodeId,
};
use bytes::BufMut;

/// Equality index: property+value -> set of NodeIds
///
/// ```text
/// Key: [0x03][0x00][prop_hash:4][value_hash:8]
/// Value: RoaringTreemap<NodeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EqualityIndexKey {
    pub(crate) property_hash: PropertyHash,
    pub(crate) value_hash: ValueHash,
}

impl EqualityIndexKey {
    pub fn new(property_hash: PropertyHash, value_hash: ValueHash) -> Self {
        Self {
            property_hash,
            value_hash,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::Equality
    }

    /// Returns the exact scoped-property hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    /// Returns the exact indexed-value hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn value_hash(&self) -> &ValueHash {
        &self.value_hash
    }

    /// Parse the equality key from a slice.
    ///
    /// key is `[0x03][0x00][prop_hash:4][value_hash:8]`
    pub fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidIndexKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        // |> key prefix
        // safe to do unwrap because we checked the length above
        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if !matches!(key_prefix, KeyPrefix::PropertyIndex) {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        // key prefix |> index prefix
        let index_prefix = IndexPrefix::from_slice(slice)?;
        if !matches!(index_prefix, IndexPrefix::Equality) {
            return Err(EncodingError::Custom(format!(
                "expected Equality index prefix, got {:?}",
                index_prefix
            )));
        }

        // key prefix + index prefix |> property hash
        let property_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
            .try_into()
            .expect("property hash slice is 4 bytes");
        // key prefix + index prefix + property hash |> value
        let value_hash = slice[PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN
            ..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN]
            .try_into()
            .expect("value hash slice is 8 bytes");

        Ok(Self::new(property_hash, value_hash))
    }

    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_slice(&self.property_hash);
        buf.put_slice(&self.value_hash);
    }
}

impl From<&EqualityIndexKey> for KeyPrefix {
    fn from(_: &EqualityIndexKey) -> KeyPrefix {
        EqualityIndexKey::key_prefix()
    }
}

impl From<&EqualityIndexKey> for IndexPrefix {
    fn from(_: &EqualityIndexKey) -> IndexPrefix {
        EqualityIndexKey::index_prefix()
    }
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

    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            EdgeDirection::Out => 0x00,
            EdgeDirection::In => 0x01,
        }
    }
}

/// Edge equality index: source+prop+value -> set of EdgeIds
/// ```text
/// Out: [0x03][0x02][0x00][source:8][prop_hash:4][value_hash:8]
/// In:  [0x03][0x02][0x01][target:8][prop_hash:4][value_hash:8]
/// Value: RoaringTreemap<EdgeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeEqualityIndexKey {
    direction: EdgeDirection,
    pub(super) source: NodeId,
    property_hash: PropertyHash,
    pub(super) value_hash: ValueHash,
}

impl EdgeEqualityIndexKey {
    pub fn new(
        direction: EdgeDirection,
        source: NodeId,
        property_hash: PropertyHash,
        value_hash: ValueHash,
    ) -> Self {
        Self {
            direction,
            source,
            property_hash,
            value_hash,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::EdgeEquality
    }

    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    /// Returns whether the endpoint embedded in this row is outgoing or incoming.
    #[cfg(test)]
    pub(crate) const fn direction(&self) -> EdgeDirection {
        self.direction
    }

    /// Returns the source or target endpoint embedded in this local row.
    #[cfg(test)]
    pub(crate) const fn endpoint(&self) -> NodeId {
        self.source
    }

    /// Returns the exact indexed-value hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn value_hash(&self) -> &ValueHash {
        &self.value_hash
    }

    /// Parse the edge equality key from a slice.
    ///
    /// key is `[0x03][0x02][0x00][source:8][prop_hash:4][value_hash:8]`
    pub fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN
            + INDEX_PREFIX_LEN
            + size_of::<EdgeDirection>()
            + NODE_ID_MAX_LEN
            + PROPERTY_HASH_MAX_LEN
            + VALUE_HASH_MAX_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidIndexKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        // |> key prefix
        // safe to do unwrap because we checked the length above
        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if key_prefix != EdgeEqualityIndexKey::key_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        // key prefix |> index prefix
        let index_prefix = IndexPrefix::from_slice(slice)?;
        if index_prefix != EdgeEqualityIndexKey::index_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected EdgeEquality index prefix, got {:?}",
                index_prefix
            )));
        }

        // key prefix + index prefix |> direction
        // safe to do unwrap because we checked the length above
        let direction = EdgeDirection::from_u8(*slice.get(PREFIX_LEN + INDEX_PREFIX_LEN).unwrap())?;

        // key prefix + index prefix + direction |> source
        let source = read_u64(
            slice,
            PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
        )?;

        // prefix + index prefix + direction + source |> property hash

        let property_hash =
            slice[PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() + NODE_ID_MAX_LEN
                ..PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN]
                .try_into()
                .expect("property hash slice is 4 bytes");

        // prefix + index prefix + direction + source + property hash |> value
        let value_hash = slice[PREFIX_LEN
            + INDEX_PREFIX_LEN
            + size_of::<EdgeDirection>()
            + NODE_ID_MAX_LEN
            + PROPERTY_HASH_MAX_LEN
            ..PREFIX_LEN
                + INDEX_PREFIX_LEN
                + size_of::<EdgeDirection>()
                + NODE_ID_MAX_LEN
                + PROPERTY_HASH_MAX_LEN
                + VALUE_HASH_MAX_LEN]
            .try_into()
            .expect("value hash slice is 8 bytes");
        Ok(Self::new(direction, source, property_hash, value_hash))
    }

    /// Encode the edge equality key into a buffer.
    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_u8(self.direction.as_u8());
        buf.put_u64(self.source);
        buf.put_slice(&self.property_hash);
        buf.put_slice(&self.value_hash);
    }
}

impl From<&EdgeEqualityIndexKey> for KeyPrefix {
    fn from(_: &EdgeEqualityIndexKey) -> KeyPrefix {
        EdgeEqualityIndexKey::key_prefix()
    }
}

impl From<&EdgeEqualityIndexKey> for IndexPrefix {
    fn from(_: &EdgeEqualityIndexKey) -> IndexPrefix {
        EdgeEqualityIndexKey::index_prefix()
    }
}

/// Global edge equality index: property+value -> set of EdgeIds.
///
/// ```text
/// Key: [0x03][0x08][prop_hash:4][value_hash:8]
/// Value: RoaringTreemap<EdgeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GlobalEdgeEqualityIndexKey {
    property_hash: PropertyHash,
    value_hash: ValueHash,
}

impl GlobalEdgeEqualityIndexKey {
    pub(crate) fn new(property_hash: PropertyHash, value_hash: ValueHash) -> Self {
        Self {
            property_hash,
            value_hash,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::GlobalEdgeEquality
    }

    /// Returns the exact scoped-property hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    /// Returns the exact indexed-value hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn value_hash(&self) -> &ValueHash {
        &self.value_hash
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidIndexKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if key_prefix != Self::key_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        let index_prefix = IndexPrefix::from_slice(slice)?;
        if index_prefix != Self::index_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected GlobalEdgeEquality index prefix, got {:?}",
                index_prefix
            )));
        }

        let property_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
            .try_into()
            .expect("property hash slice is 4 bytes");
        let value_hash = slice[PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN
            ..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN]
            .try_into()
            .expect("value hash slice is 8 bytes");

        Ok(Self::new(property_hash, value_hash))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_slice(&self.property_hash);
        buf.put_slice(&self.value_hash);
    }
}

impl From<&GlobalEdgeEqualityIndexKey> for KeyPrefix {
    fn from(_: &GlobalEdgeEqualityIndexKey) -> KeyPrefix {
        GlobalEdgeEqualityIndexKey::key_prefix()
    }
}

impl From<&GlobalEdgeEqualityIndexKey> for IndexPrefix {
    fn from(_: &GlobalEdgeEqualityIndexKey) -> IndexPrefix {
        GlobalEdgeEqualityIndexKey::index_prefix()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::indexes::IndexKey;

    #[test]
    fn equality_index_key_has_exact_layout_and_round_trips() {
        let key = IndexKey::Equality(EqualityIndexKey::new(
            [1, 2, 3, 4],
            [5, 6, 7, 8, 9, 10, 11, 12],
        ));
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);

        assert_eq!(
            encoded.as_slice(),
            &[0x03, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            IndexKey::parse_from_slice(&encoded).unwrap(),
            IndexKey::Equality(EqualityIndexKey::new(
                [1, 2, 3, 4],
                [5, 6, 7, 8, 9, 10, 11, 12],
            ))
        );
        let IndexKey::Equality(parsed) = IndexKey::parse_from_slice(&encoded).unwrap() else {
            panic!("expected equality key");
        };
        assert_eq!(parsed.property_hash(), &[1, 2, 3, 4]);
        assert_eq!(parsed.value_hash(), &[5, 6, 7, 8, 9, 10, 11, 12]);
    }

    #[test]
    fn edge_equality_index_key_has_exact_layout_and_round_trips() {
        let key = IndexKey::EdgeEquality(EdgeEqualityIndexKey::new(
            EdgeDirection::In,
            0x0102_0304_0506_0708,
            [1, 2, 3, 4],
            [5, 6, 7, 8, 9, 10, 11, 12],
        ));
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);

        let mut expected = vec![0x03, 0x02, 0x01];
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_be_bytes());
        expected.extend_from_slice(&[1, 2, 3, 4]);
        expected.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);

        assert_eq!(encoded, expected);
        assert_eq!(IndexKey::parse_from_slice(&encoded).unwrap(), key);

        let IndexKey::EdgeEquality(parsed) = IndexKey::parse_from_slice(&encoded).unwrap() else {
            panic!("expected edge equality key");
        };
        assert_eq!(parsed.property_hash(), &[1, 2, 3, 4]);
        assert_eq!(parsed.direction(), EdgeDirection::In);
        assert_eq!(parsed.endpoint(), 0x0102_0304_0506_0708);
        assert_eq!(parsed.value_hash(), &[5, 6, 7, 8, 9, 10, 11, 12]);
    }

    #[test]
    fn global_edge_equality_index_key_has_exact_layout_and_round_trips() {
        let key = IndexKey::GlobalEdgeEquality(GlobalEdgeEqualityIndexKey::new(
            [1, 2, 3, 4],
            [5, 6, 7, 8, 9, 10, 11, 12],
        ));
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);

        assert_eq!(
            encoded.as_slice(),
            &[0x03, 0x08, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(IndexKey::parse_from_slice(&encoded).unwrap(), key);
        let IndexKey::GlobalEdgeEquality(parsed) = IndexKey::parse_from_slice(&encoded).unwrap()
        else {
            panic!("expected global edge equality key");
        };
        assert_eq!(parsed.property_hash(), &[1, 2, 3, 4]);
        assert_eq!(parsed.value_hash(), &[5, 6, 7, 8, 9, 10, 11, 12]);
    }

    #[test]
    fn equality_key_prefix_contracts_cover_all_shapes() {
        let equality = EqualityIndexKey::new([1, 2, 3, 4], [5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(EqualityIndexKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(EqualityIndexKey::index_prefix(), IndexPrefix::Equality);
        assert_eq!(KeyPrefix::from(&equality), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&equality), IndexPrefix::Equality);

        let edge = EdgeEqualityIndexKey::new(
            EdgeDirection::Out,
            42,
            [2, 3, 4, 5],
            [6, 7, 8, 9, 10, 11, 12, 13],
        );
        assert_eq!(EdgeEqualityIndexKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(
            EdgeEqualityIndexKey::index_prefix(),
            IndexPrefix::EdgeEquality
        );
        assert_eq!(KeyPrefix::from(&edge), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&edge), IndexPrefix::EdgeEquality);

        let global = GlobalEdgeEqualityIndexKey::new([3, 4, 5, 6], [7, 8, 9, 10, 11, 12, 13, 14]);
        assert_eq!(
            GlobalEdgeEqualityIndexKey::key_prefix(),
            KeyPrefix::PropertyIndex
        );
        assert_eq!(
            GlobalEdgeEqualityIndexKey::index_prefix(),
            IndexPrefix::GlobalEdgeEquality
        );
        assert_eq!(KeyPrefix::from(&global), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&global), IndexPrefix::GlobalEdgeEquality);
    }

    #[test]
    fn equality_parsers_reject_short_and_trailing_inputs() {
        assert!(matches!(
            EqualityIndexKey::parse_from_slice(&[0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut equality = vec![0x03, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        equality.push(0);
        assert!(matches!(
            EqualityIndexKey::parse_from_slice(&equality),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        assert!(matches!(
            EdgeEqualityIndexKey::parse_from_slice(&[0x02, 0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        assert!(matches!(
            GlobalEdgeEqualityIndexKey::parse_from_slice(&[0x03, 0x08, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut global = vec![0x03, 0x08, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        global.push(0);
        assert!(matches!(
            GlobalEdgeEqualityIndexKey::parse_from_slice(&global),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }

    #[test]
    fn equality_parsers_reject_wrong_prefixes_and_index_kinds() {
        let mut wrong_key_prefix = vec![0x02, 0x00];
        wrong_key_prefix.extend_from_slice(&[1, 2, 3, 4]);
        wrong_key_prefix.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EqualityIndexKey::parse_from_slice(&wrong_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_index_prefix = vec![0x03, 0x01];
        wrong_index_prefix.extend_from_slice(&[1, 2, 3, 4]);
        wrong_index_prefix.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EqualityIndexKey::parse_from_slice(&wrong_index_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut edge_trailing = vec![0x03, 0x02, EdgeDirection::Out.as_u8()];
        edge_trailing.extend_from_slice(&1u64.to_be_bytes());
        edge_trailing.extend_from_slice(&[1, 2, 3, 4]);
        edge_trailing.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        edge_trailing.push(0);
        assert!(matches!(
            EdgeEqualityIndexKey::parse_from_slice(&edge_trailing),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut edge_wrong_key_prefix = vec![0x02, 0x02, EdgeDirection::Out.as_u8()];
        edge_wrong_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        edge_wrong_key_prefix.extend_from_slice(&[1, 2, 3, 4]);
        edge_wrong_key_prefix.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EdgeEqualityIndexKey::parse_from_slice(&edge_wrong_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut edge_wrong_index_prefix = vec![0x03, 0x00, EdgeDirection::Out.as_u8()];
        edge_wrong_index_prefix.extend_from_slice(&1u64.to_be_bytes());
        edge_wrong_index_prefix.extend_from_slice(&[1, 2, 3, 4]);
        edge_wrong_index_prefix.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EdgeEqualityIndexKey::parse_from_slice(&edge_wrong_index_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut invalid_direction = vec![0x03, 0x02, 0x02];
        invalid_direction.extend_from_slice(&1u64.to_be_bytes());
        invalid_direction.extend_from_slice(&[1, 2, 3, 4]);
        invalid_direction.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EdgeEqualityIndexKey::parse_from_slice(&invalid_direction),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));

        let mut global_wrong_key_prefix = vec![0x02, 0x08];
        global_wrong_key_prefix.extend_from_slice(&[1, 2, 3, 4]);
        global_wrong_key_prefix.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            GlobalEdgeEqualityIndexKey::parse_from_slice(&global_wrong_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut global_wrong_index_prefix = vec![0x03, 0x00];
        global_wrong_index_prefix.extend_from_slice(&[1, 2, 3, 4]);
        global_wrong_index_prefix.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            GlobalEdgeEqualityIndexKey::parse_from_slice(&global_wrong_index_prefix),
            Err(EncodingError::Custom(_))
        ));
    }
}
