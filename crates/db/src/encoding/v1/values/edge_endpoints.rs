//! Byte-compatible values for current edge-endpoint rows.
//!
//! An edge-endpoint row stores its source and target node identifiers as two
//! consecutive big-endian `u64` values. [`EdgeEndpointsValue`] owns that
//! deployed layout so mutation, lookup, DDL, and lifecycle verification do not
//! assemble or parse it independently. Decoding intentionally accepts trailing
//! bytes because the previous readers required only the first two identifiers.

use bytes::{BufMut, Bytes};

use crate::encoding::{error::EncodingError, NodeId};

const NODE_ID_LEN: usize = core::mem::size_of::<NodeId>();
const EDGE_ENDPOINTS_VALUE_LEN: usize = NODE_ID_LEN + NODE_ID_LEN;

/// The source and target stored by one current edge-endpoint row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EdgeEndpointsValue {
    source: NodeId,
    target: NodeId,
}

impl EdgeEndpointsValue {
    /// Constructs the value written for an edge from `source` to `target`.
    pub(crate) const fn new(source: NodeId, target: NodeId) -> Self {
        Self { source, target }
    }

    /// Returns the edge's outgoing/source endpoint.
    pub(crate) const fn source(self) -> NodeId {
        self.source
    }

    /// Returns the edge's incoming/target endpoint.
    pub(crate) const fn target(self) -> NodeId {
        self.target
    }

    /// Encodes the unchanged `[source:8][target:8]` deployed value.
    pub(crate) fn encode(self) -> Bytes {
        let mut bytes = Vec::with_capacity(EDGE_ENDPOINTS_VALUE_LEN);
        bytes.put_u64(self.source);
        bytes.put_u64(self.target);
        Bytes::from(bytes)
    }

    /// Decodes the first source and target fields from a current row.
    ///
    /// Trailing bytes remain accepted to preserve the leniency of existing
    /// readers; rows shorter than the two required identifiers fail closed.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        if bytes.len() < EDGE_ENDPOINTS_VALUE_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: EDGE_ENDPOINTS_VALUE_LEN,
                actual: bytes.len(),
            });
        }
        let source = NodeId::from_be_bytes(
            bytes[..NODE_ID_LEN]
                .try_into()
                .expect("source endpoint slice is one node identifier"),
        );
        let target = NodeId::from_be_bytes(
            bytes[NODE_ID_LEN..NODE_ID_LEN + NODE_ID_LEN]
                .try_into()
                .expect("target endpoint slice is one node identifier"),
        );
        Ok(Self::new(source, target))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_current_bytes_round_trip() {
        let value = EdgeEndpointsValue::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718);
        let encoded = value.encode();
        assert_eq!(
            encoded.as_ref(),
            &[1, 2, 3, 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23, 24,]
        );
        assert_eq!(EdgeEndpointsValue::decode(&encoded).unwrap(), value);
        assert_eq!(value.source(), 0x0102_0304_0506_0708);
        assert_eq!(value.target(), 0x1112_1314_1516_1718);
    }

    #[test]
    fn decode_preserves_trailing_byte_leniency_and_rejects_short_rows() {
        let mut encoded = EdgeEndpointsValue::new(1, 2).encode().to_vec();
        encoded.extend_from_slice(b"ignored");
        assert_eq!(
            EdgeEndpointsValue::decode(&encoded).unwrap(),
            EdgeEndpointsValue::new(1, 2)
        );
        assert!(matches!(
            EdgeEndpointsValue::decode(&encoded[..EDGE_ENDPOINTS_VALUE_LEN - 1]),
            Err(EncodingError::BufferTooShort {
                expected: EDGE_ENDPOINTS_VALUE_LEN,
                actual: 15,
            })
        ));
    }
}
