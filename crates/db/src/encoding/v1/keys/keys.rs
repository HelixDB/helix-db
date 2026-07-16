//! V1 graph data keys with typed construction and parsing boundaries.

use bytes::BufMut;

use crate::encoding::{error::EncodingError, v1::read_u64, EdgeId, NodeId};

use super::{KeyPrefix, ID_LEN, PREFIX_LEN};

/// Adjacency list storage key.
///
/// ```text
/// [0x00][node_id:8]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdjacencyKey {
    node_id: NodeId,
}

impl AdjacencyKey {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::Adjacency
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected Adjacency key prefix ({:#04x}), got {:#04x}",
                KeyPrefix::Adjacency.as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(read_u64(slice, PREFIX_LEN)?))
    }

    #[cfg(test)]
    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_u64(self.node_id);
    }
}

impl From<&AdjacencyKey> for KeyPrefix {
    fn from(_: &AdjacencyKey) -> KeyPrefix {
        AdjacencyKey::key_prefix()
    }
}

/// Edge endpoint storage key.
///
/// ```text
/// [0x04][edge_id:8]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgeEndpointsKey {
    edge_id: EdgeId,
}

impl EdgeEndpointsKey {
    pub(crate) fn new(edge_id: EdgeId) -> Self {
        Self { edge_id }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::EdgeEndpoints
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected EdgeEndpoints key prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(read_u64(slice, PREFIX_LEN)?))
    }

    pub(crate) const fn edge_id(&self) -> EdgeId {
        self.edge_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_u64(self.edge_id);
    }
}

impl From<&EdgeEndpointsKey> for KeyPrefix {
    fn from(_: &EdgeEndpointsKey) -> KeyPrefix {
        EdgeEndpointsKey::key_prefix()
    }
}

/// Edge pair index key for multigraph support.
///
/// ```text
/// [0x05][from:8][to:8]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgePairIndexKey {
    from: NodeId,
    to: NodeId,
}

impl EdgePairIndexKey {
    pub(crate) fn new(from: NodeId, to: NodeId) -> Self {
        Self { from, to }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::EdgePairIndex
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN * 2;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected EdgePairIndex key prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(
            read_u64(slice, PREFIX_LEN)?,
            read_u64(slice, PREFIX_LEN + ID_LEN)?,
        ))
    }

    #[cfg(test)]
    pub(crate) const fn from(&self) -> NodeId {
        self.from
    }

    #[cfg(test)]
    pub(crate) const fn to(&self) -> NodeId {
        self.to
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN * 2
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_u64(self.from);
        buf.put_u64(self.to);
    }
}

impl From<&EdgePairIndexKey> for KeyPrefix {
    fn from(_: &EdgePairIndexKey) -> KeyPrefix {
        EdgePairIndexKey::key_prefix()
    }
}

/// Multigraph edge property key.
///
/// ```text
/// [0x01][edge_id:8]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgePropertyByIdKey {
    edge_id: EdgeId,
}

impl EdgePropertyByIdKey {
    pub(crate) fn new(edge_id: EdgeId) -> Self {
        Self { edge_id }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::EdgePropertyById
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected EdgePropertyById key prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(read_u64(slice, PREFIX_LEN)?))
    }

    pub(crate) const fn edge_id(&self) -> EdgeId {
        self.edge_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_u64(self.edge_id);
    }
}

impl From<&EdgePropertyByIdKey> for KeyPrefix {
    fn from(_: &EdgePropertyByIdKey) -> KeyPrefix {
        EdgePropertyByIdKey::key_prefix()
    }
}

/// Legacy/simple edge property key.
///
/// ```text
/// [0x01][from:8][to:8]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgePropertyPairKey {
    from: NodeId,
    to: NodeId,
}

impl EdgePropertyPairKey {
    pub(crate) fn new(from: NodeId, to: NodeId) -> Self {
        Self { from, to }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::EdgePropertyPair
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN * 2;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected EdgePropertyPair key prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(
            read_u64(slice, PREFIX_LEN)?,
            read_u64(slice, PREFIX_LEN + ID_LEN)?,
        ))
    }

    #[cfg(test)]
    pub(crate) const fn from(&self) -> NodeId {
        self.from
    }

    #[cfg(test)]
    pub(crate) const fn to(&self) -> NodeId {
        self.to
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN * 2
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_u64(self.from);
        buf.put_u64(self.to);
    }
}

impl From<&EdgePropertyPairKey> for KeyPrefix {
    fn from(_: &EdgePropertyPairKey) -> KeyPrefix {
        EdgePropertyPairKey::key_prefix()
    }
}

/// Node property storage key.
///
/// ```text
/// [0x02][node_id:8]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodePropertyKey {
    node_id: NodeId,
}

impl NodePropertyKey {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::NodeProperty
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != KeyPrefix::NodeProperty.as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected NodeProperty key prefix ({:#04x}), got {:#04x}",
                KeyPrefix::NodeProperty.as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(read_u64(slice, PREFIX_LEN)?))
    }

    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_u64(self.node_id);
    }
}

impl From<&NodePropertyKey> for KeyPrefix {
    fn from(_: &NodePropertyKey) -> Self {
        NodePropertyKey::key_prefix()
    }
}
