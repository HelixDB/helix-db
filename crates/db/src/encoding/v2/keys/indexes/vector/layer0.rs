//! Vector layer-0 neighbor keys.

use super::*;

/// `[0xF0][index_id:8][kind=l0_vec_ks][node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorLayer0NeighborsKey {
    index_id: u64,
    node_id: NodeId,
}

impl VectorLayer0NeighborsKey {
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
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_LAYER0_VEC_KS {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector layer-0 neighbors key kind ({KEY_KIND_LAYER0_VEC_KS:#04x}), got {:#04x}",
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

    #[cfg(test)]
    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_NODE_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_HOT_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_LAYER0_VEC_KS);
        buf.put_u64(self.node_id);
    }
}
