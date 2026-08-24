use crate::encoding::{
    error::EncodingError,
    indexes::{
        EdgeDirection, IndexPrefix, ValueHash, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
        VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    v2::keys::codec::read_u64,
    NodeId,
};
use bytes::{BufMut, Bytes};

/// Edge label index: label -> set of EdgeIds.
///
/// ```text
/// Key: [0x03][0x04][label_hash:8]
/// Value: RoaringTreemap<EdgeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeLabelKey {
    label_hash: ValueHash,
}

impl EdgeLabelKey {
    pub(crate) const fn new(label_hash: ValueHash) -> Self {
        Self { label_hash }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::EdgeLabel
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN;
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

        let key_prefix = KeyPrefix::from_u8(slice[0])?;
        if key_prefix != Self::key_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        let index_prefix = IndexPrefix::from_slice(slice)?;
        if index_prefix != Self::index_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected EdgeLabel index prefix, got {:?}",
                index_prefix
            )));
        }

        let label_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN]
            .try_into()
            .expect("label hash slice is 8 bytes");

        Ok(Self::new(label_hash))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_slice(&self.label_hash);
    }
}

impl From<&EdgeLabelKey> for KeyPrefix {
    fn from(_: &EdgeLabelKey) -> KeyPrefix {
        EdgeLabelKey::key_prefix()
    }
}

impl From<&EdgeLabelKey> for IndexPrefix {
    fn from(_: &EdgeLabelKey) -> IndexPrefix {
        EdgeLabelKey::index_prefix()
    }
}

/// Edge-label neighbor index: endpoint+label -> set of opposite NodeIds.
///
/// ```text
/// Out: [0x03][0x10][0x00][source:8][label_hash:8]
/// In:  [0x03][0x10][0x01][target:8][label_hash:8]
/// Value: RoaringTreemap<NodeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeLabelNeighborKey {
    direction: EdgeDirection,
    node_id: NodeId,
    label_hash: ValueHash,
}

impl EdgeLabelNeighborKey {
    pub(crate) const fn new(
        direction: EdgeDirection,
        node_id: NodeId,
        label_hash: ValueHash,
    ) -> Self {
        Self {
            direction,
            node_id,
            label_hash,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix(direction: EdgeDirection) -> IndexPrefix {
        IndexPrefix::EdgeLabelNeighbor(direction)
    }

    #[inline]
    pub(crate) const fn index_prefix_for_key(&self) -> IndexPrefix {
        Self::index_prefix(self.direction)
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN
            + INDEX_PREFIX_LEN
            + size_of::<EdgeDirection>()
            + size_of::<NodeId>()
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

        let key_prefix = KeyPrefix::from_u8(slice[0])?;
        if key_prefix != Self::key_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        let index_prefix = IndexPrefix::from_slice(slice)?;
        let IndexPrefix::EdgeLabelNeighbor(direction) = index_prefix else {
            return Err(EncodingError::Custom(format!(
                "expected EdgeLabelNeighbor index prefix, got {:?}",
                index_prefix
            )));
        };

        let node_id = read_u64(
            slice,
            PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>(),
        )?;
        let label_hash =
            slice[PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() + size_of::<NodeId>()
                ..PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + size_of::<EdgeDirection>()
                    + size_of::<NodeId>()
                    + VALUE_HASH_MAX_LEN]
                .try_into()
                .expect("label hash slice is 8 bytes");

        Ok(Self::new(direction, node_id, label_hash))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_u64(self.node_id);
        buf.put_slice(&self.label_hash);
    }
}

impl From<&EdgeLabelNeighborKey> for KeyPrefix {
    fn from(_: &EdgeLabelNeighborKey) -> KeyPrefix {
        EdgeLabelNeighborKey::key_prefix()
    }
}

impl From<&EdgeLabelNeighborKey> for IndexPrefix {
    fn from(key: &EdgeLabelNeighborKey) -> IndexPrefix {
        key.index_prefix_for_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::indexes::PropertyIndexKey;

    const LABEL_HASH: ValueHash = [5, 6, 7, 8, 9, 10, 11, 12];

    #[test]
    fn edge_label_key_has_exact_layout_and_round_trips() {
        let key = PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(LABEL_HASH));
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);

        assert_eq!(encoded.as_slice(), &[0x03, 0x04, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(PropertyIndexKey::parse_from_slice(&encoded).unwrap(), key);
    }

    #[test]
    fn edge_label_neighbor_key_has_exact_layout_and_round_trips() {
        let node_id = 0x0102_0304_0506_0708u64;
        let key = PropertyIndexKey::EdgeLabelNeighbor(EdgeLabelNeighborKey::new(
            EdgeDirection::Out,
            node_id,
            LABEL_HASH,
        ));
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);

        let mut expected = vec![0x03, 0x10, 0x00];
        expected.extend_from_slice(&node_id.to_be_bytes());
        expected.extend_from_slice(&LABEL_HASH);

        assert_eq!(encoded, expected);
        assert_eq!(PropertyIndexKey::parse_from_slice(&encoded).unwrap(), key);
    }

    #[test]
    fn edge_label_key_prefix_contracts_cover_all_shapes() {
        let label = EdgeLabelKey::new(LABEL_HASH);
        assert_eq!(EdgeLabelKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(EdgeLabelKey::index_prefix(), IndexPrefix::EdgeLabel);
        assert_eq!(KeyPrefix::from(&label), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&label), IndexPrefix::EdgeLabel);

        let neighbor = EdgeLabelNeighborKey::new(EdgeDirection::In, 99, LABEL_HASH);
        assert_eq!(EdgeLabelNeighborKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(
            EdgeLabelNeighborKey::index_prefix(EdgeDirection::In),
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::In)
        );
        assert_eq!(
            neighbor.index_prefix_for_key(),
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::In)
        );
        assert_eq!(KeyPrefix::from(&neighbor), KeyPrefix::PropertyIndex);
        assert_eq!(
            IndexPrefix::from(&neighbor),
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::In)
        );
    }

    #[test]
    fn edge_label_neighbor_rejects_invalid_direction() {
        let mut key = vec![0x03, 0x10, 0x02];
        key.extend_from_slice(&1u64.to_be_bytes());
        key.extend_from_slice(&LABEL_HASH);

        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&key),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));
    }

    #[test]
    fn edge_label_parsers_reject_short_and_trailing_inputs() {
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&[0x03, 0x04, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut key = vec![0x03, 0x04];
        key.extend_from_slice(&LABEL_HASH);
        key.push(0);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&key),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&[0x03, 0x10, 0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut neighbor = vec![0x03, 0x10, 0x00];
        neighbor.extend_from_slice(&1u64.to_be_bytes());
        neighbor.extend_from_slice(&LABEL_HASH);
        neighbor.push(0);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&neighbor),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }

    #[test]
    fn edge_label_parsers_reject_wrong_prefixes_and_index_kinds() {
        let mut wrong_label_key_prefix = vec![0x02, 0x04];
        wrong_label_key_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&wrong_label_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_label_index_prefix = vec![0x03, 0x00];
        wrong_label_index_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&wrong_label_index_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_neighbor_key_prefix = vec![0x02, 0x10, 0x00];
        wrong_neighbor_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_neighbor_key_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&wrong_neighbor_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_neighbor_index_prefix = vec![0x03, 0x04, 0x00];
        wrong_neighbor_index_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_neighbor_index_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&wrong_neighbor_index_prefix),
            Err(EncodingError::Custom(_))
        ));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum EdgeLabelScanPrefix {
    Index,
    Label { label_hash: ValueHash },
}

#[allow(dead_code)]
impl EdgeLabelScanPrefix {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::EdgeLabel.as_slice());

        let EdgeLabelScanPrefix::Label { label_hash } = self else {
            return;
        };
        buf.put_slice(label_hash);
    }

    const fn encoded_len(&self) -> usize {
        match self {
            EdgeLabelScanPrefix::Index => PREFIX_LEN + INDEX_PREFIX_LEN,
            EdgeLabelScanPrefix::Label { .. } => PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum EdgeLabelNeighborScanPrefix {
    Index,
    Direction {
        direction: EdgeDirection,
    },
    Endpoint {
        direction: EdgeDirection,
        node_id: NodeId,
    },
    Label {
        direction: EdgeDirection,
        node_id: NodeId,
        label_hash: ValueHash,
    },
}

#[allow(dead_code)]
impl EdgeLabelNeighborScanPrefix {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        match self {
            EdgeLabelNeighborScanPrefix::Index => {
                buf.put_u8(0x10);
            }
            EdgeLabelNeighborScanPrefix::Direction { direction } => {
                buf.put_slice(IndexPrefix::EdgeLabelNeighbor(*direction).as_slice());
            }
            EdgeLabelNeighborScanPrefix::Endpoint { direction, node_id } => {
                buf.put_slice(IndexPrefix::EdgeLabelNeighbor(*direction).as_slice());
                buf.put_u64(*node_id);
            }
            EdgeLabelNeighborScanPrefix::Label {
                direction,
                node_id,
                label_hash,
            } => {
                buf.put_slice(IndexPrefix::EdgeLabelNeighbor(*direction).as_slice());
                buf.put_u64(*node_id);
                buf.put_slice(label_hash);
            }
        }
    }

    const fn encoded_len(&self) -> usize {
        match self {
            EdgeLabelNeighborScanPrefix::Index => PREFIX_LEN + INDEX_PREFIX_LEN,
            EdgeLabelNeighborScanPrefix::Direction { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + core::mem::size_of::<EdgeDirection>()
            }
            EdgeLabelNeighborScanPrefix::Endpoint { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
            }
            EdgeLabelNeighborScanPrefix::Label { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + VALUE_HASH_MAX_LEN
            }
        }
    }
}
