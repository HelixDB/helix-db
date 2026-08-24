//! Vector entry-candidate keys and scan prefixes.

use super::*;

/// `[0xF1][index_id:8][kind=cand_sorted]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorEntryCandidatePrefixKey {
    index_id: u64,
}

impl VectorEntryCandidatePrefixKey {
    pub(crate) const fn new(index_id: u64) -> Self {
        Self { index_id }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_KIND_PREFIX_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_KIND_PREFIX_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_KIND_PREFIX_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_ENTRY_CAND_SORTED {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector entry candidate key kind ({KEY_KIND_ENTRY_CAND_SORTED:#04x}), got {:#04x}",
                slice[VECTOR_KIND_OFFSET]
            )));
        }

        Ok(Self::new(u64::from_be_bytes(
            slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                .try_into()
                .expect("index id slice is 8 bytes"),
        )))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_KIND_PREFIX_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_ENTRY_CAND_SORTED);
    }
}

/// `[0xF1][index_id:8][kind=cand_sorted][inv_layer:2][node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorEntryCandidateKey {
    index_id: u64,
    layer: u16,
    node_id: NodeId,
}

impl VectorEntryCandidateKey {
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

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_ENTRY_CAND_SORTED {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector entry candidate key kind ({KEY_KIND_ENTRY_CAND_SORTED:#04x}), got {:#04x}",
                slice[VECTOR_KIND_OFFSET]
            )));
        }

        let inverted_layer = u16::from_be_bytes(
            slice[VECTOR_PAYLOAD_OFFSET..VECTOR_PAYLOAD_OFFSET + LAYER_LEN]
                .try_into()
                .expect("layer slice is 2 bytes"),
        );

        Ok(Self::new(
            u64::from_be_bytes(
                slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                    .try_into()
                    .expect("index id slice is 8 bytes"),
            ),
            u16::MAX - inverted_layer,
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
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_ENTRY_CAND_SORTED);
        buf.put_u16(u16::MAX - self.layer);
        buf.put_u64(self.node_id);
    }
}

/// `[0xF1][index_id:8][kind=cand_node][node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorEntryCandidateNodeKey {
    index_id: u64,
    node_id: NodeId,
}

impl VectorEntryCandidateNodeKey {
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

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_ENTRY_CAND_NODE {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector entry candidate node key kind ({KEY_KIND_ENTRY_CAND_NODE:#04x}), got {:#04x}",
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
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_ENTRY_CAND_NODE);
        buf.put_u64(self.node_id);
    }
}
