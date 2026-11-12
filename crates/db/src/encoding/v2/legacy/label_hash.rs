//! Retired hash-keyed label index rows.
//!
//! These rows file a label under the 8-byte SipHash-1-3 of its name, so two
//! labels with one hash share a bitmap. Current rows key the exact label (see
//! [`crate::encoding::indexes::label`]). Only this codec builds or parses
//! them: their index bytes are no live [`crate::encoding::indexes::IndexPrefix`]
//! kind. Only the graph-format V1 rewrite still stages these keys, earlier in
//! the same writer open as the startup label identity migration, which then
//! copies every row to its exact key. Readers consult them only while a
//! store's marker predates the migration's publication. Each key is built from
//! the label string, never from a bare hash.

use crate::encoding::{
    error::EncodingError,
    indexes::{
        equality::{scans::EqualityScanPrefix, EqualityIndexKey},
        hash_property_name, hash_property_value, EdgeDirection, ValueHash, INDEX_PREFIX_LEN,
        VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    v2::keys::codec::read_u64,
    NodeId,
};
use bytes::{BufMut, Bytes};

/// Property name under which legacy node-label rows join the equality keyspace.
const LABEL_PROPERTY: &str = "$label";
/// Index byte of retired global edge label rows.
const EDGE_LABEL_INDEX_PREFIX: u8 = 0x04;
/// Index byte of retired edge-label neighbor rows; a direction byte follows.
const EDGE_LABEL_NEIGHBOR_INDEX_PREFIX: u8 = 0x10;

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
}

/// Checks that `slice` is exactly `expected` bytes of a property index row
/// whose index byte is `index_prefix`.
fn check_frame(slice: &[u8], expected: usize, index_prefix: u8) -> Result<(), EncodingError> {
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
    if key_prefix != KeyPrefix::PropertyIndex {
        return Err(EncodingError::Custom(format!(
            "expected PropertyIndex key prefix, got {key_prefix:?}"
        )));
    }
    if slice[PREFIX_LEN] != index_prefix {
        return Err(EncodingError::Custom(format!(
            "expected retired label index byte {index_prefix:#04x}, got {:#04x}",
            slice[PREFIX_LEN]
        )));
    }
    Ok(())
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
    const ENCODED_LEN: usize = PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN;

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

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        check_frame(slice, Self::ENCODED_LEN, EDGE_LABEL_INDEX_PREFIX)?;
        let label_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN]
            .try_into()
            .expect("label hash slice is 8 bytes");
        Ok(Self::new(label_hash))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_u8(EDGE_LABEL_INDEX_PREFIX);
        buf.put_slice(&self.label_hash);
    }

    /// The logical key, without any scope envelope.
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);
        self.encode_into(&mut buf);
        Bytes::from(buf)
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
    const ENCODED_LEN: usize = PREFIX_LEN
        + INDEX_PREFIX_LEN
        + size_of::<EdgeDirection>()
        + size_of::<NodeId>()
        + VALUE_HASH_MAX_LEN;

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

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        check_frame(slice, Self::ENCODED_LEN, EDGE_LABEL_NEIGHBOR_INDEX_PREFIX)?;
        let direction = EdgeDirection::from_u8(slice[PREFIX_LEN + INDEX_PREFIX_LEN])?;
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
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_u8(EDGE_LABEL_NEIGHBOR_INDEX_PREFIX);
        buf.put_u8(self.direction as u8);
        buf.put_u64(self.node_id);
        buf.put_slice(&self.label_hash);
    }

    /// The logical key, without any scope envelope.
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }
}

/// Logical prefixes of the retired label keyspaces in one scope, ordered as
/// their keys are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LegacyLabelScanPrefix {
    /// Every legacy node label row: the `$label` equality rows.
    NodeLabels,
    /// Every legacy global edge label row.
    EdgeLabels,
    /// Every legacy edge-label neighbor row, both directions.
    EdgeLabelNeighbors,
}

impl LegacyLabelScanPrefix {
    /// Every family, in key order.
    pub(crate) const ALL: [Self; 3] =
        [Self::NodeLabels, Self::EdgeLabels, Self::EdgeLabelNeighbors];

    pub(crate) fn to_bytes(self) -> Bytes {
        match self {
            Self::NodeLabels => EqualityScanPrefix::Property {
                property_hash: hash_property_name(LABEL_PROPERTY),
            }
            .to_bytes(),
            Self::EdgeLabels => {
                Bytes::copy_from_slice(&[KeyPrefix::PropertyIndex.as_u8(), EDGE_LABEL_INDEX_PREFIX])
            }
            Self::EdgeLabelNeighbors => Bytes::copy_from_slice(&[
                KeyPrefix::PropertyIndex.as_u8(),
                EDGE_LABEL_NEIGHBOR_INDEX_PREFIX,
            ]),
        }
    }

    /// The family of the logical key `logical`, if it is a retired label row.
    pub(crate) fn of(logical: &[u8]) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|family| logical.starts_with(&family.to_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::indexes::IndexPrefix;

    const LABEL_HASH: ValueHash = [5, 6, 7, 8, 9, 10, 11, 12];

    #[test]
    fn edge_label_key_has_exact_layout_and_round_trips() {
        let key = LegacyEdgeLabelKey::new(LABEL_HASH);
        let encoded = key.to_bytes();

        assert_eq!(encoded.as_ref(), &[0x03, 0x04, 5, 6, 7, 8, 9, 10, 11, 12]);
        assert_eq!(LegacyEdgeLabelKey::parse_from_slice(&encoded).unwrap(), key);
        assert_eq!(
            LegacyEdgeLabelKey::for_label("FOLLOWS").label_hash(),
            &hash_property_value("FOLLOWS")
        );
    }

    #[test]
    fn edge_label_neighbor_key_has_exact_layout_and_round_trips() {
        let node_id = 0x0102_0304_0506_0708u64;
        let key = LegacyEdgeLabelNeighborKey::new(EdgeDirection::Out, node_id, LABEL_HASH);
        let encoded = key.to_bytes();

        let mut expected = vec![0x03, 0x10, 0x00];
        expected.extend_from_slice(&node_id.to_be_bytes());
        expected.extend_from_slice(&LABEL_HASH);

        assert_eq!(encoded.as_ref(), expected.as_slice());
        let parsed = LegacyEdgeLabelNeighborKey::parse_from_slice(&encoded).unwrap();
        assert_eq!(parsed, key);
        assert_eq!(
            (parsed.direction(), parsed.node_id(), parsed.label_hash()),
            (EdgeDirection::Out, node_id, &LABEL_HASH)
        );
    }

    #[test]
    fn scan_prefixes_cover_every_legacy_label_keyspace_in_key_order() {
        let mut node_labels = vec![0x03, 0x00];
        node_labels.extend_from_slice(&hash_property_name(LABEL_PROPERTY));
        assert_eq!(
            LegacyLabelScanPrefix::NodeLabels.to_bytes().as_ref(),
            node_labels.as_slice()
        );
        assert_eq!(
            LegacyLabelScanPrefix::EdgeLabels.to_bytes().as_ref(),
            &[0x03, 0x04]
        );
        assert_eq!(
            LegacyLabelScanPrefix::EdgeLabelNeighbors
                .to_bytes()
                .as_ref(),
            &[0x03, 0x10]
        );
        assert!(LegacyLabelScanPrefix::ALL
            .windows(2)
            .all(|pair| pair[0].to_bytes() < pair[1].to_bytes()));

        let node = crate::encoding::v2::keys::DataKeyKind::PropertyIndex(
            crate::encoding::indexes::PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label(
                "Person",
            )),
        );
        let mut node_key = Vec::new();
        node.encode_into(&mut node_key);
        for (key, family) in [
            (node_key, LegacyLabelScanPrefix::NodeLabels),
            (
                LegacyEdgeLabelKey::new(LABEL_HASH).to_bytes().to_vec(),
                LegacyLabelScanPrefix::EdgeLabels,
            ),
            (
                LegacyEdgeLabelNeighborKey::new(EdgeDirection::In, 1, LABEL_HASH)
                    .to_bytes()
                    .to_vec(),
                LegacyLabelScanPrefix::EdgeLabelNeighbors,
            ),
        ] {
            assert_eq!(LegacyLabelScanPrefix::of(&key), Some(family));
        }
        assert_eq!(LegacyLabelScanPrefix::of(&[0x03, 0x11, 0, 0]), None);
        assert_eq!(LegacyLabelScanPrefix::of(&[0x03]), None);
    }

    #[test]
    fn retired_index_bytes_are_no_live_index_kind() {
        for retired in [
            LegacyEdgeLabelKey::new(LABEL_HASH).to_bytes(),
            LegacyEdgeLabelNeighborKey::new(EdgeDirection::Out, 1, LABEL_HASH).to_bytes(),
        ] {
            assert!(matches!(
                IndexPrefix::from_slice(&retired),
                Err(EncodingError::InvalidIndexPrefix(_))
            ));
        }
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
