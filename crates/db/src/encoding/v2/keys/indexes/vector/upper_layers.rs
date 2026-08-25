//! Vector upper-layer neighbor and vector keys.

use super::*;

/// `[0xF0][index_id:8][kind=upper][layer:2][node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorUpperNeighborsKey {
    index_id: u64,
    layer: u16,
    node_id: NodeId,
}

impl VectorUpperNeighborsKey {
    pub(crate) const fn new(index_id: u64, layer: u16, node_id: NodeId) -> Self {
        Self {
            index_id,
            layer,
            node_id,
        }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_LAYER_NODE_KEY_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_LAYER_NODE_KEY_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_LAYER_NODE_KEY_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_HOT_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_UPPER_NEIGHBORS {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector upper-neighbors key kind ({KEY_KIND_UPPER_NEIGHBORS:#04x}), got {:#04x}",
                slice[VECTOR_KIND_OFFSET]
            )));
        }

        Ok(Self::new(
            u64::from_be_bytes(
                slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                    .try_into()
                    .expect("index id slice is 8 bytes"),
            ),
            u16::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET..VECTOR_PAYLOAD_OFFSET + LAYER_LEN]
                    .try_into()
                    .expect("layer slice is 2 bytes"),
            ),
            u64::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET + LAYER_LEN
                    ..VECTOR_PAYLOAD_OFFSET + LAYER_LEN + NODE_ID_LEN]
                    .try_into()
                    .expect("node id slice is 8 bytes"),
            ),
        ))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn layer(&self) -> u16 {
        self.layer
    }

    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_LAYER_NODE_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_HOT_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_UPPER_NEIGHBORS);
        buf.put_u16(self.layer);
        buf.put_u64(self.node_id);
    }
}

/// `[0xF0][index_id:8][kind=upper_vec][node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorUpperVectorKey {
    index_id: u64,
    node_id: NodeId,
}

impl VectorUpperVectorKey {
    pub(crate) const fn new(index_id: u64, node_id: NodeId) -> Self {
        Self { index_id, node_id }
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

        if slice[0] != VECTOR_HOT_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_UPPER_VECTOR {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector upper-vector key kind ({KEY_KIND_UPPER_VECTOR:#04x}), got {:#04x}",
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
                    .expect("node id slice is 8 bytes"),
            ),
        ))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_NODE_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_HOT_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_UPPER_VECTOR);
        buf.put_u64(self.node_id);
    }
}
