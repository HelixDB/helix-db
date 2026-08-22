//! Vector reverse-edge keys and scan prefixes.

use super::*;

/// `[0xF1][index_id:8][kind=reverse_edge][target_node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorReverseEdgePrefixKey {
    index_id: u64,
    target_node_id: NodeId,
}

impl VectorReverseEdgePrefixKey {
    pub(crate) const fn new(index_id: u64, target_node_id: NodeId) -> Self {
        Self {
            index_id,
            target_node_id,
        }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_NODE_KEY_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_NODE_KEY_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_NODE_KEY_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_REVERSE_EDGE {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector reverse-edge key kind ({KEY_KIND_REVERSE_EDGE:#04x}), got {:#04x}",
                slice[VECTOR_KIND_OFFSET]
            )));
        }

        Ok(Self::new(
            u64::from_be_bytes(
                slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                    .try_into()
                    .expect("index id slice is 8 bytes"),
            ),
            u64::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET..VECTOR_PAYLOAD_OFFSET + NODE_ID_LEN]
                    .try_into()
                    .expect("target node id slice is 8 bytes"),
            ),
        ))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    #[cfg(test)]
    pub(crate) const fn target_node_id(&self) -> NodeId {
        self.target_node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_NODE_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_REVERSE_EDGE);
        buf.put_u64(self.target_node_id);
    }
}

/// `[0xF1][index_id:8][kind=reverse_edge][target_node_id:8][layer:2][source_node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorReverseEdgeKey {
    index_id: u64,
    target_node_id: NodeId,
    layer: u16,
    source_node_id: NodeId,
}

impl VectorReverseEdgeKey {
    pub(crate) const fn new(
        index_id: u64,
        target_node_id: NodeId,
        layer: u16,
        source_node_id: NodeId,
    ) -> Self {
        Self {
            index_id,
            target_node_id,
            layer,
            source_node_id,
        }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_REVERSE_EDGE_KEY_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_REVERSE_EDGE_KEY_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_REVERSE_EDGE_KEY_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_REVERSE_EDGE {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector reverse-edge key kind ({KEY_KIND_REVERSE_EDGE:#04x}), got {:#04x}",
                slice[VECTOR_KIND_OFFSET]
            )));
        }

        Ok(Self::new(
            u64::from_be_bytes(
                slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                    .try_into()
                    .expect("index id slice is 8 bytes"),
            ),
            u64::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET..VECTOR_PAYLOAD_OFFSET + NODE_ID_LEN]
                    .try_into()
                    .expect("target node id slice is 8 bytes"),
            ),
            u16::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET + NODE_ID_LEN
                    ..VECTOR_PAYLOAD_OFFSET + NODE_ID_LEN + LAYER_LEN]
                    .try_into()
                    .expect("layer slice is 2 bytes"),
            ),
            u64::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET + NODE_ID_LEN + LAYER_LEN
                    ..VECTOR_PAYLOAD_OFFSET + NODE_ID_LEN + LAYER_LEN + NODE_ID_LEN]
                    .try_into()
                    .expect("source node id slice is 8 bytes"),
            ),
        ))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn target_node_id(&self) -> NodeId {
        self.target_node_id
    }

    pub(crate) const fn layer(&self) -> u16 {
        self.layer
    }

    pub(crate) const fn source_node_id(&self) -> NodeId {
        self.source_node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_REVERSE_EDGE_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_REVERSE_EDGE);
        buf.put_u64(self.target_node_id);
        buf.put_u16(self.layer);
        buf.put_u64(self.source_node_id);
    }
}
