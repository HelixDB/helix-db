#![allow(dead_code)]

use crate::encoding::{
    indexes::{
        equality::EdgeDirection as EqualityEdgeDirection,
        range::{EdgeRangeIndexDirection, RangeIndexDirection},
        EdgeDirection as RangeEdgeDirection, IndexPrefix, PropertyHash, ValueHash,
        INDEX_PREFIX_LEN, NODE_ID_MAX_LEN, PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    NodeId,
};
use bytes::{BufMut, Bytes};

pub(crate) fn exclusive_prefix_end_bound(prefix: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(prefix.len() + 1);
    buf.put_slice(prefix);
    buf.put_u8(0xFF);
    Bytes::from(buf)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EqualityScanPrefix {
    Index,
    Property {
        property_hash: PropertyHash,
    },
    PropertyValue {
        property_hash: PropertyHash,
        value_hash: ValueHash,
    },
}

impl EqualityScanPrefix {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::Equality.as_slice());

        match self {
            EqualityScanPrefix::Index => {}
            EqualityScanPrefix::Property { property_hash }
            | EqualityScanPrefix::PropertyValue { property_hash, .. } => {
                buf.put_slice(property_hash);
            }
        }

        let EqualityScanPrefix::PropertyValue { value_hash, .. } = self else {
            return;
        };
        buf.put_slice(value_hash);
    }

    const fn encoded_len(&self) -> usize {
        match self {
            EqualityScanPrefix::Index => PREFIX_LEN + INDEX_PREFIX_LEN,
            EqualityScanPrefix::Property { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN
            }
            EqualityScanPrefix::PropertyValue { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeEqualityScanPrefix {
    Index,
    Direction {
        direction: EqualityEdgeDirection,
    },
    Source {
        direction: EqualityEdgeDirection,
        source: NodeId,
    },
    Property {
        direction: EqualityEdgeDirection,
        source: NodeId,
        property_hash: PropertyHash,
    },
    PropertyValue {
        direction: EqualityEdgeDirection,
        source: NodeId,
        property_hash: PropertyHash,
        value_hash: ValueHash,
    },
}

impl EdgeEqualityScanPrefix {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::EdgeEquality.as_slice());

        match self {
            EdgeEqualityScanPrefix::Index => {}
            EdgeEqualityScanPrefix::Direction { direction } => {
                buf.put_u8(direction.as_u8());
            }
            EdgeEqualityScanPrefix::Source { direction, source } => {
                buf.put_u8(direction.as_u8());
                buf.put_u64(*source);
            }
            EdgeEqualityScanPrefix::Property {
                direction,
                source,
                property_hash,
            } => {
                buf.put_u8(direction.as_u8());
                buf.put_u64(*source);
                buf.put_slice(property_hash);
            }
            EdgeEqualityScanPrefix::PropertyValue {
                direction,
                source,
                property_hash,
                value_hash,
            } => {
                buf.put_u8(direction.as_u8());
                buf.put_u64(*source);
                buf.put_slice(property_hash);
                buf.put_slice(value_hash);
            }
        }
    }

    const fn encoded_len(&self) -> usize {
        match self {
            EdgeEqualityScanPrefix::Index => PREFIX_LEN + INDEX_PREFIX_LEN,
            EdgeEqualityScanPrefix::Direction { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + core::mem::size_of::<EqualityEdgeDirection>()
            }
            EdgeEqualityScanPrefix::Source { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EqualityEdgeDirection>()
                    + NODE_ID_MAX_LEN
            }
            EdgeEqualityScanPrefix::Property { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EqualityEdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN
            }
            EdgeEqualityScanPrefix::PropertyValue { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<EqualityEdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN
                    + VALUE_HASH_MAX_LEN
            }
        }
    }
}

/// Typed prefixes for the current global edge-equality row family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GlobalEdgeEqualityScanPrefix {
    /// Every global edge-equality row.
    Index,
    /// Rows belonging to one exact scoped-property hash.
    Property { property_hash: PropertyHash },
    /// The one row belonging to an exact scoped property and value hash.
    PropertyValue {
        property_hash: PropertyHash,
        value_hash: ValueHash,
    },
}

impl GlobalEdgeEqualityScanPrefix {
    /// Encodes a prefix using the unchanged global edge-equality key layout.
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    /// Appends the exact prefix segments selected by this closed shape.
    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::GlobalEdgeEquality.as_slice());

        match self {
            Self::Index => {}
            Self::Property { property_hash } | Self::PropertyValue { property_hash, .. } => {
                buf.put_slice(property_hash)
            }
        }
        let Self::PropertyValue { value_hash, .. } = self else {
            return;
        };
        buf.put_slice(value_hash);
    }

    /// Returns the exact number of bytes selected by this prefix shape.
    const fn encoded_len(&self) -> usize {
        match self {
            Self::Index => PREFIX_LEN + INDEX_PREFIX_LEN,
            Self::Property { .. } => PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN,
            Self::PropertyValue { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeLabelScanPrefix {
    Index,
    Label { label_hash: ValueHash },
}

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
pub(crate) enum EdgeLabelNeighborScanPrefix {
    Index,
    Direction {
        direction: RangeEdgeDirection,
    },
    Endpoint {
        direction: RangeEdgeDirection,
        node_id: NodeId,
    },
    Label {
        direction: RangeEdgeDirection,
        node_id: NodeId,
        label_hash: ValueHash,
    },
}

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
                PREFIX_LEN + INDEX_PREFIX_LEN + core::mem::size_of::<RangeEdgeDirection>()
            }
            EdgeLabelNeighborScanPrefix::Endpoint { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<RangeEdgeDirection>()
                    + NODE_ID_MAX_LEN
            }
            EdgeLabelNeighborScanPrefix::Label { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<RangeEdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + VALUE_HASH_MAX_LEN
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeScanPrefix<'a> {
    Direction {
        direction: RangeIndexDirection,
    },
    Property {
        direction: RangeIndexDirection,
        property_hash: PropertyHash,
    },
    PropertyValue(RangeScanValuePrefix<'a>),
}

impl<'a> RangeScanPrefix<'a> {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn exclusive_end_bound(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len() + 1);
        self.encode_into(&mut buf);
        buf.put_u8(0xFF);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        match self {
            RangeScanPrefix::Direction { direction } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::Range(*direction).as_slice());
            }
            RangeScanPrefix::Property {
                direction,
                property_hash,
            } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::Range(*direction).as_slice());
                buf.put_slice(property_hash);
            }
            RangeScanPrefix::PropertyValue(prefix) => prefix.encode_into(buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            RangeScanPrefix::Direction { .. } => PREFIX_LEN + INDEX_PREFIX_LEN,
            RangeScanPrefix::Property { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN
            }
            RangeScanPrefix::PropertyValue(prefix) => prefix.encoded_len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeScanValuePrefix<'a> {
    direction: RangeIndexDirection,
    property_hash: PropertyHash,
    value: &'a str,
}

impl<'a> RangeScanValuePrefix<'a> {
    pub(crate) const fn new(
        direction: RangeIndexDirection,
        property_hash: PropertyHash,
        value: &'a str,
    ) -> Self {
        Self {
            direction,
            property_hash,
            value,
        }
    }

    pub(crate) fn to_bytes(self) -> Bytes {
        RangeScanPrefix::PropertyValue(self).to_bytes()
    }

    pub(crate) fn exclusive_end_bound(&self) -> Bytes {
        RangeScanPrefix::PropertyValue(*self).exclusive_end_bound()
    }

    pub(crate) fn inclusive_end_bound(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len() + NODE_ID_MAX_LEN + 1);
        self.encode_into(&mut buf);
        buf.put_u64(u64::MAX);
        buf.put_u8(0);
        Bytes::from(buf)
    }

    fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::Range(self.direction).as_slice());
        buf.put_slice(&self.property_hash);
        put_ordered_value(
            buf,
            matches!(self.direction, RangeIndexDirection::Desc),
            self.value,
        );
    }

    fn encoded_len(&self) -> usize {
        PREFIX_LEN
            + INDEX_PREFIX_LEN
            + PROPERTY_HASH_MAX_LEN
            + ordered_value_len(
                matches!(self.direction, RangeIndexDirection::Desc),
                self.value,
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GlobalEdgeRangeScanPrefix<'a> {
    Direction {
        direction: RangeIndexDirection,
    },
    Property {
        direction: RangeIndexDirection,
        property_hash: PropertyHash,
    },
    PropertyValue(GlobalEdgeRangeScanValuePrefix<'a>),
}

impl<'a> GlobalEdgeRangeScanPrefix<'a> {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn exclusive_end_bound(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len() + 1);
        self.encode_into(&mut buf);
        buf.put_u8(0xFF);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        match self {
            GlobalEdgeRangeScanPrefix::Direction { direction } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::GlobalEdgeRange(*direction).as_slice());
            }
            GlobalEdgeRangeScanPrefix::Property {
                direction,
                property_hash,
            } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::GlobalEdgeRange(*direction).as_slice());
                buf.put_slice(property_hash);
            }
            GlobalEdgeRangeScanPrefix::PropertyValue(prefix) => prefix.encode_into(buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            GlobalEdgeRangeScanPrefix::Direction { .. } => PREFIX_LEN + INDEX_PREFIX_LEN,
            GlobalEdgeRangeScanPrefix::Property { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN
            }
            GlobalEdgeRangeScanPrefix::PropertyValue(prefix) => prefix.encoded_len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GlobalEdgeRangeScanValuePrefix<'a> {
    direction: RangeIndexDirection,
    property_hash: PropertyHash,
    value: &'a str,
}

impl<'a> GlobalEdgeRangeScanValuePrefix<'a> {
    pub(crate) const fn new(
        direction: RangeIndexDirection,
        property_hash: PropertyHash,
        value: &'a str,
    ) -> Self {
        Self {
            direction,
            property_hash,
            value,
        }
    }

    pub(crate) fn to_bytes(self) -> Bytes {
        GlobalEdgeRangeScanPrefix::PropertyValue(self).to_bytes()
    }

    pub(crate) fn inclusive_end_bound(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len() + NODE_ID_MAX_LEN + 1);
        self.encode_into(&mut buf);
        buf.put_u64(u64::MAX);
        buf.put_u8(0);
        Bytes::from(buf)
    }

    fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::GlobalEdgeRange(self.direction).as_slice());
        buf.put_slice(&self.property_hash);
        put_ordered_value(
            buf,
            matches!(self.direction, RangeIndexDirection::Desc),
            self.value,
        );
    }

    fn encoded_len(&self) -> usize {
        PREFIX_LEN
            + INDEX_PREFIX_LEN
            + PROPERTY_HASH_MAX_LEN
            + ordered_value_len(
                matches!(self.direction, RangeIndexDirection::Desc),
                self.value,
            )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EdgeRangeScanPrefix<'a> {
    Direction {
        edge_direction: RangeEdgeDirection,
        range_direction: EdgeRangeIndexDirection,
    },
    Endpoint {
        edge_direction: RangeEdgeDirection,
        range_direction: EdgeRangeIndexDirection,
        endpoint: NodeId,
    },
    Property {
        edge_direction: RangeEdgeDirection,
        range_direction: EdgeRangeIndexDirection,
        endpoint: NodeId,
        property_hash: PropertyHash,
    },
    PropertyValue(EdgeRangeScanValuePrefix<'a>),
}

impl<'a> EdgeRangeScanPrefix<'a> {
    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn exclusive_end_bound(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len() + 1);
        self.encode_into(&mut buf);
        buf.put_u8(0xFF);
        Bytes::from(buf)
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        match self {
            EdgeRangeScanPrefix::Direction {
                edge_direction,
                range_direction,
            } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::EdgeRange(*range_direction, *edge_direction).as_slice());
            }
            EdgeRangeScanPrefix::Endpoint {
                edge_direction,
                range_direction,
                endpoint,
            } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::EdgeRange(*range_direction, *edge_direction).as_slice());
                buf.put_u64(*endpoint);
            }
            EdgeRangeScanPrefix::Property {
                edge_direction,
                range_direction,
                endpoint,
                property_hash,
            } => {
                buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
                buf.put_slice(IndexPrefix::EdgeRange(*range_direction, *edge_direction).as_slice());
                buf.put_u64(*endpoint);
                buf.put_slice(property_hash);
            }
            EdgeRangeScanPrefix::PropertyValue(prefix) => prefix.encode_into(buf),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            EdgeRangeScanPrefix::Direction { .. } => {
                PREFIX_LEN + INDEX_PREFIX_LEN + core::mem::size_of::<RangeEdgeDirection>()
            }
            EdgeRangeScanPrefix::Endpoint { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<RangeEdgeDirection>()
                    + NODE_ID_MAX_LEN
            }
            EdgeRangeScanPrefix::Property { .. } => {
                PREFIX_LEN
                    + INDEX_PREFIX_LEN
                    + core::mem::size_of::<RangeEdgeDirection>()
                    + NODE_ID_MAX_LEN
                    + PROPERTY_HASH_MAX_LEN
            }
            EdgeRangeScanPrefix::PropertyValue(prefix) => prefix.encoded_len(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgeRangeScanValuePrefix<'a> {
    edge_direction: RangeEdgeDirection,
    range_direction: EdgeRangeIndexDirection,
    endpoint: NodeId,
    property_hash: PropertyHash,
    value: &'a str,
}

impl<'a> EdgeRangeScanValuePrefix<'a> {
    pub(crate) const fn new(
        edge_direction: RangeEdgeDirection,
        range_direction: EdgeRangeIndexDirection,
        endpoint: NodeId,
        property_hash: PropertyHash,
        value: &'a str,
    ) -> Self {
        Self {
            edge_direction,
            range_direction,
            endpoint,
            property_hash,
            value,
        }
    }

    pub(crate) fn to_bytes(self) -> Bytes {
        EdgeRangeScanPrefix::PropertyValue(self).to_bytes()
    }

    pub(crate) fn exclusive_end_bound(&self) -> Bytes {
        EdgeRangeScanPrefix::PropertyValue(*self).exclusive_end_bound()
    }

    pub(crate) fn inclusive_end_bound(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len() + NODE_ID_MAX_LEN + 1);
        self.encode_into(&mut buf);
        buf.put_u64(u64::MAX);
        buf.put_u8(0);
        Bytes::from(buf)
    }

    fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_slice(IndexPrefix::EdgeRange(self.range_direction, self.edge_direction).as_slice());
        buf.put_u64(self.endpoint);
        buf.put_slice(&self.property_hash);
        put_ordered_value(
            buf,
            matches!(self.range_direction, EdgeRangeIndexDirection::Desc),
            self.value,
        );
    }

    fn encoded_len(&self) -> usize {
        PREFIX_LEN
            + INDEX_PREFIX_LEN
            + core::mem::size_of::<RangeEdgeDirection>()
            + NODE_ID_MAX_LEN
            + PROPERTY_HASH_MAX_LEN
            + ordered_value_len(
                matches!(self.range_direction, EdgeRangeIndexDirection::Desc),
                self.value,
            )
    }
}

fn put_ordered_value<B: BufMut>(buf: &mut B, descending: bool, value: &str) {
    match descending {
        false => buf.put_slice(value.as_bytes()),
        true => {
            for byte in value.as_bytes() {
                buf.put_u8(!byte);
                if *byte == 0x00 {
                    buf.put_u8(!0xFF);
                }
            }
            buf.put_u8(!0x00);
            buf.put_u8(!0x01);
        }
    }
}

fn ordered_value_len(descending: bool, value: &str) -> usize {
    match descending {
        false => value.len(),
        true => {
            value
                .as_bytes()
                .iter()
                .map(|byte| if *byte == 0x00 { 2 } else { 1 })
                .sum::<usize>()
                + 2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROP: PropertyHash = [1, 2, 3, 4];
    const VALUE: ValueHash = [5, 6, 7, 8, 9, 10, 11, 12];

    #[test]
    fn equality_prefixes_encode_only_valid_segments() {
        assert_eq!(EqualityScanPrefix::Index.to_bytes().as_ref(), &[0x03, 0x00]);
        assert_eq!(
            EqualityScanPrefix::Property {
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x00, 1, 2, 3, 4]
        );
        assert_eq!(
            EqualityScanPrefix::PropertyValue {
                property_hash: PROP,
                value_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }

    #[test]
    fn exclusive_prefix_end_bound_appends_single_max_byte() {
        let prefix = Bytes::from_static(&[0x03, 0x01, 0xAA]);
        assert_eq!(
            exclusive_prefix_end_bound(&prefix).as_ref(),
            &[0x03, 0x01, 0xAA, 0xFF]
        );
    }

    #[test]
    fn edge_equality_prefixes_encode_source_before_optional_segments() {
        let source = 0x0102_0304_0506_0708u64;
        assert_eq!(
            EdgeEqualityScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x02]
        );
        assert_eq!(
            EdgeEqualityScanPrefix::Direction {
                direction: EqualityEdgeDirection::Out,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x02, 0x00]
        );

        let mut expected = vec![0x03, 0x02, EqualityEdgeDirection::Out.as_u8()];
        expected.extend_from_slice(&source.to_be_bytes());
        assert_eq!(
            EdgeEqualityScanPrefix::Source {
                direction: EqualityEdgeDirection::Out,
                source,
            }
            .to_bytes()
            .as_ref(),
            expected.as_slice()
        );

        expected.extend_from_slice(&PROP);
        assert_eq!(
            EdgeEqualityScanPrefix::Property {
                direction: EqualityEdgeDirection::Out,
                source,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            expected.as_slice()
        );

        expected.extend_from_slice(&VALUE);
        assert_eq!(
            EdgeEqualityScanPrefix::PropertyValue {
                direction: EqualityEdgeDirection::Out,
                source,
                property_hash: PROP,
                value_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            expected.as_slice()
        );
    }

    #[test]
    fn global_edge_equality_prefixes_encode_only_valid_segments() {
        assert_eq!(
            GlobalEdgeEqualityScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x08]
        );
        assert_eq!(
            GlobalEdgeEqualityScanPrefix::Property {
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x08, 1, 2, 3, 4]
        );
        assert_eq!(
            GlobalEdgeEqualityScanPrefix::PropertyValue {
                property_hash: PROP,
                value_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x08, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }

    #[test]
    fn edge_label_prefixes_encode_label_hash_layouts() {
        let node_id = 0x0102_0304_0506_0708u64;
        assert_eq!(
            EdgeLabelScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x04]
        );
        assert_eq!(
            EdgeLabelScanPrefix::Label { label_hash: VALUE }
                .to_bytes()
                .as_ref(),
            &[0x03, 0x04, 5, 6, 7, 8, 9, 10, 11, 12]
        );
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Index.to_bytes().as_ref(),
            &[0x03, 0x10]
        );
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Direction {
                direction: RangeEdgeDirection::Out,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x10, 0x00]
        );

        let mut endpoint = vec![0x03, 0x10, 0x01];
        endpoint.extend_from_slice(&node_id.to_be_bytes());
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Endpoint {
                direction: RangeEdgeDirection::In,
                node_id,
            }
            .to_bytes()
            .as_ref(),
            endpoint.as_slice()
        );

        endpoint.extend_from_slice(&VALUE);
        assert_eq!(
            EdgeLabelNeighborScanPrefix::Label {
                direction: RangeEdgeDirection::In,
                node_id,
                label_hash: VALUE,
            }
            .to_bytes()
            .as_ref(),
            endpoint.as_slice()
        );
    }

    #[test]
    fn range_prefixes_and_bounds_encode_existing_layout() {
        assert_eq!(
            RangeScanPrefix::Direction {
                direction: RangeIndexDirection::Asc,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x01]
        );
        assert_eq!(
            RangeScanPrefix::Property {
                direction: RangeIndexDirection::Desc,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x05, 1, 2, 3, 4]
        );

        let value_prefix = RangeScanValuePrefix::new(RangeIndexDirection::Desc, PROP, "a\0");
        assert_eq!(
            value_prefix.to_bytes().as_ref(),
            &[0x03, 0x05, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE]
        );
        assert_eq!(
            value_prefix.exclusive_end_bound().as_ref(),
            &[0x03, 0x05, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE, 0xFF,]
        );

        let inclusive_end =
            RangeScanValuePrefix::new(RangeIndexDirection::Asc, PROP, "a").inclusive_end_bound();
        let mut expected = vec![0x03, 0x01, 1, 2, 3, 4, b'a'];
        expected.extend_from_slice(&u64::MAX.to_be_bytes());
        expected.push(0);
        assert_eq!(inclusive_end.as_ref(), expected.as_slice());
    }

    #[test]
    fn global_edge_range_prefixes_and_bounds_encode_existing_layout() {
        assert_eq!(
            GlobalEdgeRangeScanPrefix::Direction {
                direction: RangeIndexDirection::Asc,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x09]
        );
        assert_eq!(
            GlobalEdgeRangeScanPrefix::Property {
                direction: RangeIndexDirection::Desc,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x0a, 1, 2, 3, 4]
        );

        let value_prefix =
            GlobalEdgeRangeScanValuePrefix::new(RangeIndexDirection::Desc, PROP, "a\0");
        assert_eq!(
            value_prefix.to_bytes().as_ref(),
            &[0x03, 0x0a, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE]
        );
        assert_eq!(
            GlobalEdgeRangeScanPrefix::PropertyValue(value_prefix)
                .exclusive_end_bound()
                .as_ref(),
            &[0x03, 0x0a, 1, 2, 3, 4, 0x9E, 0xFF, 0x00, 0xFF, 0xFE, 0xFF,]
        );

        let inclusive_end =
            GlobalEdgeRangeScanValuePrefix::new(RangeIndexDirection::Asc, PROP, "a")
                .inclusive_end_bound();
        let mut expected = vec![0x03, 0x09, 1, 2, 3, 4, b'a'];
        expected.extend_from_slice(&u64::MAX.to_be_bytes());
        expected.push(0);
        assert_eq!(inclusive_end.as_ref(), expected.as_slice());
    }

    #[test]
    fn edge_range_prefixes_include_endpoint_before_property() {
        let endpoint = 0x0102_0304_0506_0708u64;
        assert_eq!(
            EdgeRangeScanPrefix::Direction {
                edge_direction: RangeEdgeDirection::Out,
                range_direction: EdgeRangeIndexDirection::Asc,
            }
            .to_bytes()
            .as_ref(),
            &[0x03, 0x03, 0x00]
        );

        let mut endpoint_expected = vec![0x03, 0x03, 0x00];
        endpoint_expected.extend_from_slice(&endpoint.to_be_bytes());
        assert_eq!(
            EdgeRangeScanPrefix::Endpoint {
                edge_direction: RangeEdgeDirection::Out,
                range_direction: EdgeRangeIndexDirection::Asc,
                endpoint,
            }
            .to_bytes()
            .as_ref(),
            endpoint_expected.as_slice()
        );

        let mut property_expected = vec![0x03, 0x06, 0x00];
        property_expected.extend_from_slice(&endpoint.to_be_bytes());
        property_expected.extend_from_slice(&PROP);
        assert_eq!(
            EdgeRangeScanPrefix::Property {
                edge_direction: RangeEdgeDirection::Out,
                range_direction: EdgeRangeIndexDirection::Desc,
                endpoint,
                property_hash: PROP,
            }
            .to_bytes()
            .as_ref(),
            property_expected.as_slice()
        );

        let mut value_expected = vec![0x03, 0x03, 0x01];
        value_expected.extend_from_slice(&endpoint.to_be_bytes());
        value_expected.extend_from_slice(&PROP);
        value_expected.push(b'a');
        let value_prefix = EdgeRangeScanValuePrefix::new(
            RangeEdgeDirection::In,
            EdgeRangeIndexDirection::Asc,
            endpoint,
            PROP,
            "a",
        );
        assert_eq!(value_prefix.to_bytes().as_ref(), value_expected.as_slice());
        value_expected.push(0xFF);
        assert_eq!(
            value_prefix.exclusive_end_bound().as_ref(),
            value_expected.as_slice()
        );

        let mut desc_value_expected = vec![0x03, 0x06, 0x00];
        desc_value_expected.extend_from_slice(&endpoint.to_be_bytes());
        desc_value_expected.extend_from_slice(&PROP);
        desc_value_expected.extend_from_slice(&[0x9E, 0xFF, 0x00, 0xFF, 0xFE]);
        assert_eq!(
            EdgeRangeScanValuePrefix::new(
                RangeEdgeDirection::Out,
                EdgeRangeIndexDirection::Desc,
                endpoint,
                PROP,
                "a\0",
            )
            .to_bytes()
            .as_ref(),
            desc_value_expected.as_slice()
        );
    }
}
