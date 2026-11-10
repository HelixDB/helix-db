//! Exact label identities and the label index keys that carry them.
//!
//! Every label index row is keyed by the label itself: its big-endian `u16`
//! byte length followed by its UTF-8 bytes. Identity is the exact label, so two
//! labels are always two keys, and the length frame keeps every label key
//! prefix-free. Building or parsing a key validates the label's length and
//! UTF-8 but neither hashes nor allocates.
//!
//! ```text
//! Node label:          [0x03][0x11][len:2][label]               -> RoaringTreemap<NodeId>
//! Edge label:          [0x03][0x12][len:2][label]               -> RoaringTreemap<EdgeId>
//! Edge label neighbor: [0x03][0x13][dir:1][node:8][len:2][label] -> RoaringTreemap<NodeId>
//! ```
//!
//! Retired hash-keyed rows are decoded only by the legacy label-hash codec.

use std::sync::Arc;

use bytes::{BufMut, Bytes};

use crate::encoding::{
    error::EncodingError,
    indexes::{EdgeDirection, IndexPrefix, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN},
    keys::{scope::TENANT_ENVELOPE_LEN, KeyPrefix, PREFIX_LEN},
    v2::keys::{codec::read_u64, STORAGE_KEY_MAX_LEN},
    NodeId,
};

/// Bytes of the big-endian label length that leads every persisted label.
pub(crate) const LABEL_LEN_PREFIX_LEN: usize = core::mem::size_of::<u16>();
/// Longest label any label index key can hold.
///
/// The longest label key, an edge-label neighbor row in a tenant scope, must
/// fit the storage engine's physical key bound.
pub(crate) const MAX_LABEL_LEN: usize = STORAGE_KEY_MAX_LEN
    - TENANT_ENVELOPE_LEN
    - PREFIX_LEN
    - INDEX_PREFIX_LEN
    - core::mem::size_of::<EdgeDirection>()
    - NODE_ID_MAX_LEN
    - LABEL_LEN_PREFIX_LEN;

const _: () = assert!(
    MAX_LABEL_LEN <= u16::MAX as usize,
    "every keyable label length fits the u16 length frame"
);

/// One exact label that fits every label index key.
///
/// Only [`LabelIdentity::new`] and [`LabelIdentity::decode`] construct it, so
/// the label is always at most [`MAX_LABEL_LEN`] bytes. It borrows the label,
/// so a per-node key costs neither a hash nor an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LabelIdentity<'a> {
    label: &'a str,
}

impl<'a> LabelIdentity<'a> {
    /// Identity of `label`, or [`EncodingError::LabelTooLong`] for a label no
    /// label index key can hold.
    pub(crate) fn new(label: &'a str) -> Result<Self, EncodingError> {
        if label.len() > MAX_LABEL_LEN {
            return Err(EncodingError::LabelTooLong {
                len: label.len(),
                maximum: MAX_LABEL_LEN,
            });
        }
        Ok(Self { label })
    }

    pub(crate) const fn label(self) -> &'a str {
        self.label
    }

    pub(crate) const fn encoded_len(self) -> usize {
        LABEL_LEN_PREFIX_LEN + self.label.len()
    }

    pub(crate) fn encode_into<B: BufMut>(self, buf: &mut B) {
        buf.put_u16(u16::try_from(self.label.len()).expect("labels fit the u16 length frame"));
        buf.put_slice(self.label.as_bytes());
    }

    /// Decodes the length-framed label that fills `slice` exactly.
    pub(crate) fn decode(slice: &'a [u8]) -> Result<Self, EncodingError> {
        let Some(len) = slice.get(..LABEL_LEN_PREFIX_LEN) else {
            return Err(EncodingError::BufferTooShort {
                expected: LABEL_LEN_PREFIX_LEN,
                actual: slice.len(),
            });
        };
        let len = usize::from(u16::from_be_bytes(
            len.try_into().expect("label length slice is 2 bytes"),
        ));
        match slice.len().cmp(&(LABEL_LEN_PREFIX_LEN + len)) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: LABEL_LEN_PREFIX_LEN + len,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidIndexKey(format!(
                    "label expected {} bytes, got {}",
                    LABEL_LEN_PREFIX_LEN + len,
                    slice.len()
                )));
            }
        }
        Self::new(std::str::from_utf8(
            &slice[LABEL_LEN_PREFIX_LEN..LABEL_LEN_PREFIX_LEN + len],
        )?)
    }
}

/// An owned [`LabelIdentity`], reference counted so every row of one label
/// shares a single allocation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SharedLabel(Arc<str>);

impl SharedLabel {
    pub(crate) fn new(identity: LabelIdentity<'_>) -> Self {
        Self(Arc::from(identity.label))
    }

    pub(crate) fn identity(&self) -> LabelIdentity<'_> {
        LabelIdentity { label: &self.0 }
    }

    /// Heap bytes [`Self::new`] allocates for `identity`: one `Arc<str>` with
    /// its two reference counts.
    pub(crate) const fn allocation_bytes(identity: LabelIdentity<'_>) -> usize {
        2 * core::mem::size_of::<usize>() + identity.label.len()
    }
}

/// Lets a set of shared labels be searched by a borrowed label; both orders
/// compare the label string.
impl std::borrow::Borrow<str> for SharedLabel {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Node label index: label -> set of NodeIds.
///
/// ```text
/// Key: [0x03][0x11][len:2][label]
/// Value: RoaringTreemap<NodeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeLabelKey<'a> {
    identity: LabelIdentity<'a>,
}

impl<'a> NodeLabelKey<'a> {
    pub(crate) const fn new(identity: LabelIdentity<'a>) -> Self {
        Self { identity }
    }

    #[cfg(test)]
    pub(crate) const fn identity(&self) -> LabelIdentity<'a> {
        self.identity
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::NodeLabel
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + INDEX_PREFIX_LEN + self.identity.encoded_len()
    }

    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        // `from_slice` checks the key prefix and that the header bytes exist.
        let index_prefix = IndexPrefix::from_slice(slice)?;
        if index_prefix != Self::index_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected NodeLabel index prefix, got {index_prefix:?}"
            )));
        }
        Ok(Self::new(LabelIdentity::decode(
            &slice[PREFIX_LEN + INDEX_PREFIX_LEN..],
        )?))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(Self::key_prefix().as_u8());
        buf.put_slice(Self::index_prefix().as_slice());
        self.identity.encode_into(buf);
    }
}

/// Global edge label index: label -> set of EdgeIds.
///
/// ```text
/// Key: [0x03][0x12][len:2][label]
/// Value: RoaringTreemap<EdgeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeLabelKey<'a> {
    identity: LabelIdentity<'a>,
}

impl<'a> EdgeLabelKey<'a> {
    pub(crate) const fn new(identity: LabelIdentity<'a>) -> Self {
        Self { identity }
    }

    #[cfg(test)]
    pub(crate) const fn identity(&self) -> LabelIdentity<'a> {
        self.identity
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::EdgeLabel
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + INDEX_PREFIX_LEN + self.identity.encoded_len()
    }

    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        // `from_slice` checks the key prefix and that the header bytes exist.
        let index_prefix = IndexPrefix::from_slice(slice)?;
        if index_prefix != Self::index_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected EdgeLabel index prefix, got {index_prefix:?}"
            )));
        }
        Ok(Self::new(LabelIdentity::decode(
            &slice[PREFIX_LEN + INDEX_PREFIX_LEN..],
        )?))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(Self::key_prefix().as_u8());
        buf.put_slice(Self::index_prefix().as_slice());
        self.identity.encode_into(buf);
    }
}

/// Edge-label neighbor index: endpoint+label -> set of opposite NodeIds.
///
/// ```text
/// Out: [0x03][0x13][0x00][source:8][len:2][label]
/// In:  [0x03][0x13][0x01][target:8][len:2][label]
/// Value: RoaringTreemap<NodeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeLabelNeighborKey<'a> {
    direction: EdgeDirection,
    node_id: NodeId,
    identity: LabelIdentity<'a>,
}

impl<'a> EdgeLabelNeighborKey<'a> {
    pub(crate) const fn new(
        direction: EdgeDirection,
        node_id: NodeId,
        identity: LabelIdentity<'a>,
    ) -> Self {
        Self {
            direction,
            node_id,
            identity,
        }
    }

    #[cfg(test)]
    pub(crate) const fn direction(&self) -> EdgeDirection {
        self.direction
    }

    #[cfg(test)]
    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[cfg(test)]
    pub(crate) const fn identity(&self) -> LabelIdentity<'a> {
        self.identity
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

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN
            + INDEX_PREFIX_LEN
            + core::mem::size_of::<EdgeDirection>()
            + NODE_ID_MAX_LEN
            + self.identity.encoded_len()
    }

    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        // `from_slice` checks the key prefix and that the direction byte exists.
        let index_prefix = IndexPrefix::from_slice(slice)?;
        let IndexPrefix::EdgeLabelNeighbor(direction) = index_prefix else {
            return Err(EncodingError::Custom(format!(
                "expected EdgeLabelNeighbor index prefix, got {index_prefix:?}"
            )));
        };
        // `read_u64` checks that the node ID, and so the label offset, exists.
        let node_id = read_u64(
            slice,
            PREFIX_LEN + INDEX_PREFIX_LEN + core::mem::size_of::<EdgeDirection>(),
        )?;
        let identity = LabelIdentity::decode(
            &slice[PREFIX_LEN
                + INDEX_PREFIX_LEN
                + core::mem::size_of::<EdgeDirection>()
                + NODE_ID_MAX_LEN..],
        )?;
        Ok(Self::new(direction, node_id, identity))
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(Self::key_prefix().as_u8());
        buf.put_slice(self.index_prefix_for_key().as_slice());
        buf.put_u64(self.node_id);
        self.identity.encode_into(buf);
    }
}

/// Logical prefixes of the exact label index keyspaces in one scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelScanPrefix {
    /// Every node label row.
    NodeLabels,
    /// Every global edge label row.
    EdgeLabels,
    /// Every edge-label neighbor row, both directions.
    EdgeLabelNeighbors,
}

impl LabelScanPrefix {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(PREFIX_LEN + INDEX_PREFIX_LEN);
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        match self {
            Self::NodeLabels => buf.put_slice(IndexPrefix::NodeLabel.as_slice()),
            Self::EdgeLabels => buf.put_slice(IndexPrefix::EdgeLabel.as_slice()),
            Self::EdgeLabelNeighbors => {
                buf.put_u8(crate::encoding::indexes::prefix::EDGE_LABEL_NEIGHBOR_INDEX_PREFIX)
            }
        }
        Bytes::from(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::indexes::PropertyIndexKey;

    fn identity(label: &str) -> LabelIdentity<'_> {
        LabelIdentity::new(label).expect("fixture label fits a key")
    }

    fn encoded(key: &PropertyIndexKey<'_>) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(key.encoded_len());
        key.encode_into(&mut bytes);
        assert_eq!(bytes.len(), key.encoded_len());
        bytes
    }

    #[test]
    fn identity_frames_length_and_utf8_label() {
        let identity = identity("Person");
        let mut bytes = Vec::new();
        identity.encode_into(&mut bytes);

        let mut expected = 6u16.to_be_bytes().to_vec();
        expected.extend_from_slice(b"Person");
        assert_eq!(bytes, expected);
        assert_eq!(bytes.len(), identity.encoded_len());
        assert_eq!(identity.label(), "Person");
        assert_eq!(LabelIdentity::decode(&bytes).unwrap(), identity);
    }

    #[test]
    fn distinct_labels_are_distinct_keys() {
        let key = |label| {
            encoded(&PropertyIndexKey::NodeLabel(NodeLabelKey::new(identity(
                label,
            ))))
        };
        assert_ne!(key("Person"), key("person"));
        // The length frame keeps one label's key from prefixing another's.
        assert!(!key("PersonX").starts_with(&key("Person")));
        assert_eq!(key(""), vec![0x03, 0x11, 0x00, 0x00]);
    }

    #[test]
    fn shared_labels_view_and_compare_as_their_label() {
        let shared = SharedLabel::new(identity("Person"));
        assert_eq!(shared.identity(), identity("Person"));
        assert_eq!(std::borrow::Borrow::<str>::borrow(&shared), "Person");
        assert!(SharedLabel::new(identity("A")) < SharedLabel::new(identity("B")));
        assert_eq!(
            SharedLabel::allocation_bytes(identity("Person")),
            2 * core::mem::size_of::<usize>() + 6
        );
        let set = std::collections::BTreeSet::from([shared.clone()]);
        assert_eq!(set.get("Person"), Some(&shared));
    }

    #[test]
    fn identity_decode_rejects_short_trailing_and_non_utf8_frames() {
        assert!(matches!(
            LabelIdentity::decode(&[0; LABEL_LEN_PREFIX_LEN - 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut bytes = Vec::new();
        identity("Person").encode_into(&mut bytes);
        assert!(matches!(
            LabelIdentity::decode(&bytes[..bytes.len() - 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            LabelIdentity::decode(&trailing),
            Err(EncodingError::InvalidIndexKey(_))
        ));

        let mut non_utf8 = 1u16.to_be_bytes().to_vec();
        non_utf8.push(0xFF);
        assert!(matches!(
            LabelIdentity::decode(&non_utf8),
            Err(EncodingError::InvalidUtf8(_))
        ));
    }

    #[test]
    fn identity_rejects_labels_longer_than_any_label_key_holds() {
        let longest = "a".repeat(MAX_LABEL_LEN);
        assert_eq!(
            identity(&longest).encoded_len(),
            LABEL_LEN_PREFIX_LEN + MAX_LABEL_LEN
        );
        let too_long = "a".repeat(MAX_LABEL_LEN + 1);
        assert!(matches!(
            LabelIdentity::new(&too_long),
            Err(EncodingError::LabelTooLong { len, maximum })
                if len == MAX_LABEL_LEN + 1 && maximum == MAX_LABEL_LEN
        ));
        // A persisted frame longer than the bound fails closed too.
        let mut frame = u16::try_from(MAX_LABEL_LEN + 1)
            .unwrap()
            .to_be_bytes()
            .to_vec();
        frame.extend_from_slice(too_long.as_bytes());
        assert!(matches!(
            LabelIdentity::decode(&frame),
            Err(EncodingError::LabelTooLong { .. })
        ));

        let key = crate::encoding::v2::keys::DataKey::Data {
            scope: crate::encoding::keys::scope::DataScope::Tenant(
                crate::encoding::keys::scope::TenantId::from_u128(u128::MAX),
            ),
            kind: crate::encoding::v2::keys::DataKeyKind::PropertyIndex(
                PropertyIndexKey::EdgeLabelNeighbor(EdgeLabelNeighborKey::new(
                    EdgeDirection::In,
                    NodeId::MAX,
                    identity(&longest),
                )),
            ),
        };
        assert_eq!(key.to_bytes().len(), STORAGE_KEY_MAX_LEN);
    }

    #[test]
    fn node_label_key_has_exact_layout_and_round_trips() {
        let key = PropertyIndexKey::NodeLabel(NodeLabelKey::new(identity("Person")));
        let bytes = encoded(&key);

        let mut expected = vec![0x03, 0x11];
        identity("Person").encode_into(&mut expected);
        assert_eq!(bytes, expected);
        assert_eq!(PropertyIndexKey::parse_from_slice(&bytes).unwrap(), key);
        assert_eq!(
            NodeLabelKey::parse_from_slice(&bytes)
                .unwrap()
                .identity()
                .label(),
            "Person"
        );
    }

    #[test]
    fn edge_label_key_has_exact_layout_and_round_trips() {
        let key = PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(identity("FOLLOWS")));
        let bytes = encoded(&key);

        let mut expected = vec![0x03, 0x12];
        identity("FOLLOWS").encode_into(&mut expected);
        assert_eq!(bytes, expected);
        assert_eq!(PropertyIndexKey::parse_from_slice(&bytes).unwrap(), key);
        assert_eq!(
            EdgeLabelKey::parse_from_slice(&bytes)
                .unwrap()
                .identity()
                .label(),
            "FOLLOWS"
        );
    }

    #[test]
    fn edge_label_neighbor_key_has_exact_layout_and_round_trips() {
        let node_id = 0x0102_0304_0506_0708u64;
        for (direction, byte) in [(EdgeDirection::Out, 0x00), (EdgeDirection::In, 0x01)] {
            let key = PropertyIndexKey::EdgeLabelNeighbor(EdgeLabelNeighborKey::new(
                direction,
                node_id,
                identity("FOLLOWS"),
            ));
            let bytes = encoded(&key);

            let mut expected = vec![0x03, 0x13, byte];
            expected.extend_from_slice(&node_id.to_be_bytes());
            identity("FOLLOWS").encode_into(&mut expected);
            assert_eq!(bytes, expected);
            assert_eq!(PropertyIndexKey::parse_from_slice(&bytes).unwrap(), key);
            let parsed = EdgeLabelNeighborKey::parse_from_slice(&bytes).unwrap();
            assert_eq!(parsed.direction(), direction);
            assert_eq!(parsed.node_id(), node_id);
            assert_eq!(parsed.identity().label(), "FOLLOWS");
            assert_eq!(
                parsed.index_prefix_for_key(),
                IndexPrefix::EdgeLabelNeighbor(direction)
            );
        }
    }

    #[test]
    fn label_key_prefix_contracts_cover_all_shapes() {
        assert_eq!(NodeLabelKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(NodeLabelKey::index_prefix(), IndexPrefix::NodeLabel);
        assert_eq!(EdgeLabelKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(EdgeLabelKey::index_prefix(), IndexPrefix::EdgeLabel);
        assert_eq!(EdgeLabelNeighborKey::key_prefix(), KeyPrefix::PropertyIndex);
        assert_eq!(
            EdgeLabelNeighborKey::index_prefix(EdgeDirection::In),
            IndexPrefix::EdgeLabelNeighbor(EdgeDirection::In)
        );
        assert_eq!(
            LabelScanPrefix::NodeLabels.to_bytes().as_ref(),
            &[0x03, 0x11]
        );
        assert_eq!(
            LabelScanPrefix::EdgeLabels.to_bytes().as_ref(),
            &[0x03, 0x12]
        );
        assert_eq!(
            LabelScanPrefix::EdgeLabelNeighbors.to_bytes().as_ref(),
            &[0x03, 0x13]
        );
    }

    #[test]
    fn label_key_parsers_reject_wrong_prefixes_short_keys_and_bad_frames() {
        let node = encoded(&PropertyIndexKey::NodeLabel(NodeLabelKey::new(identity(
            "Person",
        ))));
        let edge = encoded(&PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(identity(
            "Person",
        ))));
        let neighbor = encoded(&PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeDirection::Out, 7, identity("Person")),
        ));

        assert!(matches!(
            NodeLabelKey::parse_from_slice(&edge),
            Err(EncodingError::Custom(_))
        ));
        assert!(matches!(
            EdgeLabelKey::parse_from_slice(&node),
            Err(EncodingError::Custom(_))
        ));
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&edge),
            Err(EncodingError::Custom(_))
        ));
        let mut wrong_key_prefix = node.clone();
        wrong_key_prefix[0] = KeyPrefix::NodeProperty.as_u8();
        assert!(matches!(
            NodeLabelKey::parse_from_slice(&wrong_key_prefix),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            NodeLabelKey::parse_from_slice(&[0x03]),
            Err(EncodingError::BufferTooShort { .. })
        ));
        assert!(matches!(
            NodeLabelKey::parse_from_slice(&node[..PREFIX_LEN + INDEX_PREFIX_LEN + 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&neighbor[..PREFIX_LEN + INDEX_PREFIX_LEN + 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(
                &neighbor[..PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EdgeDirection>()
                    + NODE_ID_MAX_LEN]
            ),
            Err(EncodingError::BufferTooShort { .. })
        ));
        let mut bad_direction = neighbor.clone();
        bad_direction[PREFIX_LEN + INDEX_PREFIX_LEN] = 0x02;
        assert!(matches!(
            EdgeLabelNeighborKey::parse_from_slice(&bad_direction),
            Err(EncodingError::InvalidEdgeIndexDirection(0x02))
        ));
        let mut trailing = neighbor;
        trailing.push(0);
        assert!(matches!(
            PropertyIndexKey::parse_from_slice(&trailing),
            Err(EncodingError::InvalidIndexKey(_))
        ));
    }
}
