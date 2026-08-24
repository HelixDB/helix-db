//! Range-index scan-prefix codecs.

#![allow(dead_code)]

use crate::encoding::{
    indexes::{
        range::{EdgeRangeIndexDirection, RangeIndexDirection},
        EdgeDirection as RangeEdgeDirection, IndexPrefix, PropertyHash, INDEX_PREFIX_LEN,
        NODE_ID_MAX_LEN, PROPERTY_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    NodeId,
};
use bytes::{BufMut, Bytes};

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
