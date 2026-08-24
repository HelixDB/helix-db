//! Equality-index scan-prefix codecs.

#![allow(dead_code)]

use crate::encoding::{
    indexes::{
        equality::EdgeDirection as EqualityEdgeDirection, IndexPrefix, PropertyHash, ValueHash,
        INDEX_PREFIX_LEN, NODE_ID_MAX_LEN, PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
    NodeId,
};
use bytes::{BufMut, Bytes};

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
