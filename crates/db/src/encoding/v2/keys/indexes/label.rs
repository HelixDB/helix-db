use crate::encoding::{
    error::EncodingError,
    indexes::{
        canonical_label::{CanonicalLabel, LABEL_DIGEST_LEN},
        EdgeDirection, IndexPrefix, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    v2::keys::codec::read_u64,
    NodeId,
};
use bytes::{BufMut, Bytes};

/// Builtin node-label index: label -> set of NodeIds.
///
/// ```text
/// Key: [0x03][0x07][digest:8][u32 len][utf8]
/// Value: RoaringTreemap<NodeId>
/// ```
///
/// The digest is a scan accelerator. Exact identity is the canonical UTF-8 label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeLabelKey {
    label: CanonicalLabel,
}

impl NodeLabelKey {
    pub(crate) fn from_label(label: &str) -> Result<Self, EncodingError> {
        Ok(Self {
            label: CanonicalLabel::from_label(label)?,
        })
    }

    pub(crate) fn from_canonical(label: CanonicalLabel) -> Self {
        Self { label }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::NodeLabel
    }

    pub(crate) fn encoded_len(&self) -> usize {
        PREFIX_LEN + INDEX_PREFIX_LEN + self.label.encoded_len()
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let header = PREFIX_LEN + INDEX_PREFIX_LEN;
        if slice.len() < header {
            return Err(EncodingError::BufferTooShort {
                expected: header,
                actual: slice.len(),
            });
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
                "expected NodeLabel index prefix, got {:?}",
                index_prefix
            )));
        }

        Ok(Self {
            label: CanonicalLabel::parse_from_slice(&slice[header..])?,
        })
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        self.label.encode_into(buf);
    }
}

impl From<&NodeLabelKey> for KeyPrefix {
    fn from(_: &NodeLabelKey) -> KeyPrefix {
        NodeLabelKey::key_prefix()
    }
}

impl From<&NodeLabelKey> for IndexPrefix {
    fn from(_: &NodeLabelKey) -> IndexPrefix {
        NodeLabelKey::index_prefix()
    }
}

/// Edge label index: label -> set of EdgeIds.
///
/// ```text
/// Key: [0x03][0x04][digest:8][u32 len][utf8]
/// Value: RoaringTreemap<EdgeId>
/// ```
///
/// The digest is a scan accelerator. Exact identity is the canonical UTF-8 label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeLabelKey {
    label: CanonicalLabel,
}

impl EdgeLabelKey {
    pub(crate) fn from_label(label: &str) -> Result<Self, EncodingError> {
        Ok(Self {
            label: CanonicalLabel::from_label(label)?,
        })
    }

    pub(crate) fn from_canonical(label: CanonicalLabel) -> Self {
        Self { label }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::EdgeLabel
    }

    pub(crate) fn encoded_len(&self) -> usize {
        PREFIX_LEN + INDEX_PREFIX_LEN + self.label.encoded_len()
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let header = PREFIX_LEN + INDEX_PREFIX_LEN;
        if slice.len() < header {
            return Err(EncodingError::BufferTooShort {
                expected: header,
                actual: slice.len(),
            });
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

        Ok(Self {
            label: CanonicalLabel::parse_from_slice(&slice[header..])?,
        })
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        self.label.encode_into(buf);
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
/// Out: [0x03][0x10][0x00][source:8][digest:8][u32 len][utf8]
/// In:  [0x03][0x10][0x01][target:8][digest:8][u32 len][utf8]
/// Value: RoaringTreemap<NodeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeLabelNeighborKey {
    direction: EdgeDirection,
    node_id: NodeId,
    label: CanonicalLabel,
}

impl EdgeLabelNeighborKey {
    pub(crate) fn from_label(
        direction: EdgeDirection,
        node_id: NodeId,
        label: &str,
    ) -> Result<Self, EncodingError> {
        Ok(Self {
            direction,
            node_id,
            label: CanonicalLabel::from_label(label)?,
        })
    }

    pub(crate) fn from_canonical(
        direction: EdgeDirection,
        node_id: NodeId,
        label: CanonicalLabel,
    ) -> Self {
        Self {
            direction,
            node_id,
            label,
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

    pub(crate) fn encoded_len(&self) -> usize {
        PREFIX_LEN
            + INDEX_PREFIX_LEN
            + size_of::<EdgeDirection>()
            + NODE_ID_MAX_LEN
            + self.label.encoded_len()
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let header =
            PREFIX_LEN + INDEX_PREFIX_LEN + size_of::<EdgeDirection>() + size_of::<NodeId>();
        if slice.len() < header {
            return Err(EncodingError::BufferTooShort {
                expected: header,
                actual: slice.len(),
            });
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
        Ok(Self {
            direction,
            node_id,
            label: CanonicalLabel::parse_from_slice(&slice[header..])?,
        })
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_u64(self.node_id);
        self.label.encode_into(buf);
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
    use crate::encoding::indexes::canonical_label::LABEL_DIGEST_LEN;
    use crate::encoding::indexes::PropertyIndexKey;

    fn encode_key(key: &PropertyIndexKey<'_>) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);
        encoded
    }

    fn expected_label_frame(label: &str) -> Vec<u8> {
        let canonical = CanonicalLabel::from_label(label).unwrap();
        let mut frame = Vec::with_capacity(canonical.encoded_len());
        canonical.encode_into(&mut frame);
        frame
    }

    #[test]
    fn node_and_edge_label_keys_have_canonical_layout_and_round_trip() {
        let node = PropertyIndexKey::NodeLabel(NodeLabelKey::from_label("User").unwrap());
        let mut expected_node = vec![0x03, 0x07];
        expected_node.extend_from_slice(&expected_label_frame("User"));
        let encoded_node = encode_key(&node);
        assert_eq!(encoded_node, expected_node);
        assert_eq!(
            PropertyIndexKey::parse_from_slice(&encoded_node).unwrap(),
            node
        );

        let edge = PropertyIndexKey::EdgeLabel(EdgeLabelKey::from_label("FOLLOWS").unwrap());
        let mut expected_edge = vec![0x03, 0x04];
        expected_edge.extend_from_slice(&expected_label_frame("FOLLOWS"));
        let encoded_edge = encode_key(&edge);
        assert_eq!(encoded_edge, expected_edge);
        assert_eq!(
            PropertyIndexKey::parse_from_slice(&encoded_edge).unwrap(),
            edge
        );
    }

    #[test]
    fn edge_label_neighbor_key_has_canonical_layout_and_round_trips() {
        let node_id = 0x0102_0304_0506_0708u64;
        let key = PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::from_label(EdgeDirection::Out, node_id, "FOLLOWS").unwrap(),
        );
        let mut encoded = encode_key(&key);

        let mut expected = vec![0x03, 0x10, 0x00];
        expected.extend_from_slice(&node_id.to_be_bytes());
        expected.extend_from_slice(&expected_label_frame("FOLLOWS"));

        assert_eq!(encoded, expected);
        assert_eq!(PropertyIndexKey::parse_from_slice(&encoded).unwrap(), key);
        encoded.push(0);
        assert!(PropertyIndexKey::parse_from_slice(&encoded).is_err());
    }

    #[test]
    fn digest_colliding_labels_encode_distinct_keys() {
        let digest = [0xA5; LABEL_DIGEST_LEN];
        let first = CanonicalLabel::with_test_digest_unchecked("alpha", digest);
        let second = CanonicalLabel::with_test_digest_unchecked("beta", digest);
        let first_key = EdgeLabelKey::from_canonical(first.clone());
        let second_key = EdgeLabelKey::from_canonical(second.clone());
        let mut first_bytes = Vec::new();
        first_key.encode_into(&mut first_bytes);
        let mut second_bytes = Vec::new();
        second_key.encode_into(&mut second_bytes);
        assert_ne!(first_bytes, second_bytes);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&first_bytes),
            Err(EncodingError::CanonicalLabelDigestMismatch)
        ));
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&second_bytes),
            Err(EncodingError::CanonicalLabelDigestMismatch)
        ));

        let first_node = NodeLabelKey::from_canonical(first);
        let second_node = NodeLabelKey::from_canonical(second);
        let mut first_node_bytes = Vec::new();
        first_node.encode_into(&mut first_node_bytes);
        let mut second_node_bytes = Vec::new();
        second_node.encode_into(&mut second_node_bytes);
        assert_ne!(first_node_bytes, second_node_bytes);
    }

    #[test]
    fn edge_label_key_prefix_contracts_cover_all_shapes() {
        let label = EdgeLabelKey::from_label("FOLLOWS").unwrap();
        assert_eq!(EdgeLabelKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(EdgeLabelKey::index_prefix(), IndexPrefix::EdgeLabel);
        assert_eq!(KeyPrefix::from(&label), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&label), IndexPrefix::EdgeLabel);

        let node = NodeLabelKey::from_label("User").unwrap();
        assert_eq!(NodeLabelKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(NodeLabelKey::index_prefix(), IndexPrefix::NodeLabel);
        assert_eq!(KeyPrefix::from(&node), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&node), IndexPrefix::NodeLabel);

        let neighbor = EdgeLabelNeighborKey::from_label(EdgeDirection::In, 99, "FOLLOWS").unwrap();
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
        key.extend_from_slice(&expected_label_frame("FOLLOWS"));

        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&key),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));
    }

    #[test]
    fn hash_only_label_keys_fail_closed() {
        let mut old_edge = vec![0x03, 0x04];
        old_edge.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&old_edge),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut old_neighbor = vec![0x03, 0x10, 0x00];
        old_neighbor.extend_from_slice(&1u64.to_be_bytes());
        old_neighbor.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&old_neighbor),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut old_node = vec![0x03, 0x07];
        old_node.extend_from_slice(&[5, 6, 7, 8, 9, 10, 11, 12]);
        assert!(matches!(
            NodeLabelKey::parse_from_slice(&old_node),
            Err(EncodingError::BufferTooShort { .. })
        ));
    }

    #[test]
    fn edge_label_parsers_reject_short_and_trailing_inputs() {
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&[0x03, 0x04, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut key = Vec::new();
        EdgeLabelKey::from_label("FOLLOWS")
            .unwrap()
            .encode_into(&mut key);
        key.push(0);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&key),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&[0x03, 0x10, 0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut neighbor = Vec::new();
        EdgeLabelNeighborKey::from_label(EdgeDirection::Out, 1, "FOLLOWS")
            .unwrap()
            .encode_into(&mut neighbor);
        neighbor.push(0);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&neighbor),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }

    #[test]
    fn persisted_label_digest_mismatch_fails_closed() {
        let mut key = Vec::new();
        EdgeLabelKey::from_label("FOLLOWS")
            .unwrap()
            .encode_into(&mut key);
        key[PREFIX_LEN + INDEX_PREFIX_LEN] ^= 0xFF;
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&key),
            Err(EncodingError::CanonicalLabelDigestMismatch)
        ));
    }

    #[test]
    fn edge_label_parsers_reject_wrong_prefixes_and_index_kinds() {
        let frame = expected_label_frame("FOLLOWS");
        let mut wrong_label_key_prefix = vec![0x02, 0x04];
        wrong_label_key_prefix.extend_from_slice(&frame);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&wrong_label_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_label_index_prefix = vec![0x03, 0x00];
        wrong_label_index_prefix.extend_from_slice(&frame);
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&wrong_label_index_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_neighbor_key_prefix = vec![0x02, 0x10, 0x00];
        wrong_neighbor_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_neighbor_key_prefix.extend_from_slice(&frame);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&wrong_neighbor_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_neighbor_index_prefix = vec![0x03, 0x04, 0x00];
        wrong_neighbor_index_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_neighbor_index_prefix.extend_from_slice(&frame);
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&wrong_neighbor_index_prefix),
            Err(EncodingError::Custom(_))
        ));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum NodeLabelScanPrefix {
    Index,
}

#[allow(dead_code)]
impl NodeLabelScanPrefix {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(PREFIX_LEN + INDEX_PREFIX_LEN);
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::NodeLabel.as_slice());
        Bytes::from(buf)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum EdgeLabelScanPrefix {
    Index,
    Digest { digest: [u8; LABEL_DIGEST_LEN] },
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

        let EdgeLabelScanPrefix::Digest { digest } = self else {
            return;
        };
        buf.put_slice(digest);
    }

    fn encoded_len(&self) -> usize {
        match self {
            EdgeLabelScanPrefix::Index => PREFIX_LEN + INDEX_PREFIX_LEN,
            EdgeLabelScanPrefix::Digest { .. } => PREFIX_LEN + INDEX_PREFIX_LEN + LABEL_DIGEST_LEN,
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
    Digest {
        direction: EdgeDirection,
        node_id: NodeId,
        digest: [u8; LABEL_DIGEST_LEN],
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
            EdgeLabelNeighborScanPrefix::Digest {
                direction,
                node_id,
                digest,
            } => {
                buf.put_slice(IndexPrefix::EdgeLabelNeighbor(*direction).as_slice());
                buf.put_u64(*node_id);
                buf.put_slice(digest);
            }
        }
    }

    fn encoded_len(&self) -> usize {
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
            EdgeLabelNeighborScanPrefix::Digest { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + LABEL_DIGEST_LEN
            }
        }
    }
}
