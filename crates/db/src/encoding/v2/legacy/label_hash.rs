//! Retired hash-keyed label index rows.
//!
//! These rows file a label under the 8-byte SipHash-1-3 of its name, so two
//! labels with one hash share a bitmap. Current rows key the exact label (see
//! [`crate::encoding::indexes::label`]). Only the graph-format V1 rewrite still
//! stages these keys, earlier in the same writer open as the startup label
//! identity migration, which then moves every row to its exact key. Readers
//! consult them only while a store's marker predates the migration's
//! publication. Each key is built from the label string, never from a bare hash.

use crate::encoding::{
    error::EncodingError,
    indexes::{
        equality::{scans::EqualityScanPrefix, EqualityIndexKey},
        hash_property_name, hash_property_value,
        prefix::LEGACY_EDGE_LABEL_NEIGHBOR_INDEX_PREFIX,
        EdgeDirection, IndexPrefix, ValueHash, INDEX_PREFIX_LEN, VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    v2::keys::codec::read_u64,
    NodeId,
};
use bytes::{BufMut, Bytes};

/// Property name under which legacy node-label rows join the equality keyspace.
const LABEL_PROPERTY: &str = "$label";

/// Legacy node label rows: built-in equality rows of the `$label` property.
///
/// ```text
/// Key: [0x03][0x00][hash32("$label"):4][label_hash:8]
/// Value: RoaringTreemap<NodeId>
/// ```
pub(crate) struct LegacyNodeLabelKey;

impl LegacyNodeLabelKey {
    /// The equality row a legacy store files `label`'s nodes under.
    pub(crate) fn for_label(label: &str) -> EqualityIndexKey {
        EqualityIndexKey::new(
            hash_property_name(LABEL_PROPERTY),
            hash_property_value(label),
        )
    }

    /// Logical prefix of every legacy node label row in one scope.
    pub(crate) fn scan_prefix() -> Bytes {
        EqualityScanPrefix::Property {
            property_hash: hash_property_name(LABEL_PROPERTY),
        }
        .to_bytes()
    }
}

/// Edge label index: label -> set of EdgeIds.
///
/// ```text
/// Key: [0x03][0x04][label_hash:8]
/// Value: RoaringTreemap<EdgeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyEdgeLabelKey {
    label_hash: ValueHash,
}

impl LegacyEdgeLabelKey {
    const fn new(label_hash: ValueHash) -> Self {
        Self { label_hash }
    }

    /// The legacy row a store files `label`'s edges under.
    pub(crate) fn for_label(label: &str) -> Self {
        Self::new(hash_property_value(label))
    }

    pub(crate) const fn label_hash(&self) -> &ValueHash {
        &self.label_hash
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::LegacyEdgeLabel
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
                "expected legacy EdgeLabel index prefix, got {:?}",
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

impl From<&LegacyEdgeLabelKey> for KeyPrefix {
    fn from(_: &LegacyEdgeLabelKey) -> KeyPrefix {
        LegacyEdgeLabelKey::key_prefix()
    }
}

impl From<&LegacyEdgeLabelKey> for IndexPrefix {
    fn from(_: &LegacyEdgeLabelKey) -> IndexPrefix {
        LegacyEdgeLabelKey::index_prefix()
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
pub(crate) struct LegacyEdgeLabelNeighborKey {
    direction: EdgeDirection,
    node_id: NodeId,
    label_hash: ValueHash,
}

impl LegacyEdgeLabelNeighborKey {
    const fn new(direction: EdgeDirection, node_id: NodeId, label_hash: ValueHash) -> Self {
        Self {
            direction,
            node_id,
            label_hash,
        }
    }

    /// The legacy row a store files `node`'s `label` neighbors under.
    pub(crate) fn for_label(direction: EdgeDirection, node_id: NodeId, label: &str) -> Self {
        Self::new(direction, node_id, hash_property_value(label))
    }

    pub(crate) const fn direction(&self) -> EdgeDirection {
        self.direction
    }

    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn label_hash(&self) -> &ValueHash {
        &self.label_hash
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix(direction: EdgeDirection) -> IndexPrefix {
        IndexPrefix::LegacyEdgeLabelNeighbor(direction)
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
        let IndexPrefix::LegacyEdgeLabelNeighbor(direction) = index_prefix else {
            return Err(EncodingError::Custom(format!(
                "expected legacy EdgeLabelNeighbor index prefix, got {:?}",
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

impl From<&LegacyEdgeLabelNeighborKey> for KeyPrefix {
    fn from(_: &LegacyEdgeLabelNeighborKey) -> KeyPrefix {
        LegacyEdgeLabelNeighborKey::key_prefix()
    }
}

impl From<&LegacyEdgeLabelNeighborKey> for IndexPrefix {
    fn from(key: &LegacyEdgeLabelNeighborKey) -> IndexPrefix {
        key.index_prefix_for_key()
    }
}

/// Logical prefix of every legacy global edge label row in one scope.
pub(crate) fn legacy_edge_label_scan_prefix() -> Bytes {
    let mut buf = Vec::with_capacity(PREFIX_LEN + INDEX_PREFIX_LEN);
    buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
    buf.put_slice(IndexPrefix::LegacyEdgeLabel.as_slice());
    Bytes::from(buf)
}

/// Logical prefix of every legacy edge-label neighbor row, both directions, in one scope.
pub(crate) fn legacy_edge_label_neighbor_scan_prefix() -> Bytes {
    let mut buf = Vec::with_capacity(PREFIX_LEN + INDEX_PREFIX_LEN);
    buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
    buf.put_u8(LEGACY_EDGE_LABEL_NEIGHBOR_INDEX_PREFIX);
    Bytes::from(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::indexes::PropertyIndexKey;

    const LABEL_HASH: ValueHash = [5, 6, 7, 8, 9, 10, 11, 12];

    #[test]
    fn edge_label_key_has_exact_layout_and_round_trips() {
        let key = PropertyIndexKey::LegacyEdgeLabel(LegacyEdgeLabelKey::new(LABEL_HASH));
        let mut encoded = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut encoded);

        assert_eq!(encoded.as_slice(), &[0x03, 0x04, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(PropertyIndexKey::parse_from_slice(&encoded).unwrap(), key);
    }

    #[test]
    fn edge_label_neighbor_key_has_exact_layout_and_round_trips() {
        let node_id = 0x0102_0304_0506_0708u64;
        let key = PropertyIndexKey::LegacyEdgeLabelNeighbor(LegacyEdgeLabelNeighborKey::new(
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
        let label = LegacyEdgeLabelKey::new(LABEL_HASH);
        assert_eq!(LegacyEdgeLabelKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(
            LegacyEdgeLabelKey::index_prefix(),
            IndexPrefix::LegacyEdgeLabel
        );
        assert_eq!(KeyPrefix::from(&label), KeyPrefix::PropertyIndex);
        assert_eq!(IndexPrefix::from(&label), IndexPrefix::LegacyEdgeLabel);

        let neighbor = LegacyEdgeLabelNeighborKey::new(EdgeDirection::In, 99, LABEL_HASH);
        assert_eq!(
            LegacyEdgeLabelNeighborKey::key_prefix(),
            KeyPrefix::PropertyIndex
        );
        assert_eq!(
            LegacyEdgeLabelNeighborKey::index_prefix(EdgeDirection::In),
            IndexPrefix::LegacyEdgeLabelNeighbor(EdgeDirection::In)
        );
        assert_eq!(
            neighbor.index_prefix_for_key(),
            IndexPrefix::LegacyEdgeLabelNeighbor(EdgeDirection::In)
        );
        assert_eq!(KeyPrefix::from(&neighbor), KeyPrefix::PropertyIndex);
        assert_eq!(
            IndexPrefix::from(&neighbor),
            IndexPrefix::LegacyEdgeLabelNeighbor(EdgeDirection::In)
        );
    }

    #[test]
    fn scan_prefixes_cover_both_legacy_label_keyspaces() {
        assert_eq!(legacy_edge_label_scan_prefix().as_ref(), &[0x03, 0x04]);
        assert_eq!(
            legacy_edge_label_neighbor_scan_prefix().as_ref(),
            &[0x03, 0x10]
        );
        let mut node_labels = vec![0x03, 0x00];
        node_labels.extend_from_slice(&hash_property_name(LABEL_PROPERTY));
        assert_eq!(
            LegacyNodeLabelKey::scan_prefix().as_ref(),
            node_labels.as_slice()
        );
    }

    #[test]
    fn edge_label_neighbor_rejects_invalid_direction() {
        let mut key = vec![0x03, 0x10, 0x02];
        key.extend_from_slice(&1u64.to_be_bytes());
        key.extend_from_slice(&LABEL_HASH);

        assert!(matches!(
            LegacyEdgeLabelNeighborKey::parse_from_slice(&key),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));
    }

    #[test]
    fn edge_label_parsers_reject_short_and_trailing_inputs() {
        assert!(matches!(
            LegacyEdgeLabelKey::parse_from_slice(&[0x03, 0x04, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut key = vec![0x03, 0x04];
        key.extend_from_slice(&LABEL_HASH);
        key.push(0);
        assert!(matches!(
            LegacyEdgeLabelKey::parse_from_slice(&key),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        assert!(matches!(
            LegacyEdgeLabelNeighborKey::parse_from_slice(&[0x03, 0x10, 0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut neighbor = vec![0x03, 0x10, 0x00];
        neighbor.extend_from_slice(&1u64.to_be_bytes());
        neighbor.extend_from_slice(&LABEL_HASH);
        neighbor.push(0);
        assert!(matches!(
            LegacyEdgeLabelNeighborKey::parse_from_slice(&neighbor),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }

    #[test]
    fn edge_label_parsers_reject_wrong_prefixes_and_index_kinds() {
        let mut wrong_label_key_prefix = vec![0x02, 0x04];
        wrong_label_key_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            LegacyEdgeLabelKey::parse_from_slice(&wrong_label_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_label_index_prefix = vec![0x03, 0x00];
        wrong_label_index_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            LegacyEdgeLabelKey::parse_from_slice(&wrong_label_index_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_neighbor_key_prefix = vec![0x02, 0x10, 0x00];
        wrong_neighbor_key_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_neighbor_key_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            LegacyEdgeLabelNeighborKey::parse_from_slice(&wrong_neighbor_key_prefix),
            Err(EncodingError::Custom(_))
        ));

        let mut wrong_neighbor_index_prefix = vec![0x03, 0x04, 0x00];
        wrong_neighbor_index_prefix.extend_from_slice(&1u64.to_be_bytes());
        wrong_neighbor_index_prefix.extend_from_slice(&LABEL_HASH);
        assert!(matches!(
            LegacyEdgeLabelNeighborKey::parse_from_slice(&wrong_neighbor_index_prefix),
            Err(EncodingError::Custom(_))
        ));
    }
}
