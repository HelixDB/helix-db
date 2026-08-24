//! Vector item keys and item scan prefixes.

use super::*;

/// `[0xF1][index_id:8][kind=vec]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorItemPrefixKey {
    index_id: u64,
}

impl VectorItemPrefixKey {
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
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_VECTOR {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector item key kind ({KEY_KIND_VECTOR:#04x}), got {:#04x}",
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
        buf.put_u8(KEY_KIND_VECTOR);
    }
}

/// `[0xF1][index_id:8][kind=vec][order_code:8][node_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorItemKey {
    index_id: u64,
    order_code: u64,
    node_id: NodeId,
}

impl VectorItemKey {
    pub(crate) const fn new(index_id: u64, order_code: u64, node_id: NodeId) -> Self {
        Self {
            index_id,
            order_code,
            node_id,
        }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_ORDERED_KEY_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_ORDERED_KEY_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_ORDERED_KEY_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[VECTOR_KIND_OFFSET] != KEY_KIND_VECTOR {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector item key kind ({KEY_KIND_VECTOR:#04x}), got {:#04x}",
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
                slice[VECTOR_PAYLOAD_OFFSET..VECTOR_PAYLOAD_OFFSET + ORDER_CODE_LEN]
                    .try_into()
                    .expect("order code slice is 8 bytes"),
            ),
            u64::from_be_bytes(
                slice[VECTOR_PAYLOAD_OFFSET + ORDER_CODE_LEN
                    ..VECTOR_PAYLOAD_OFFSET + ORDER_CODE_LEN + NODE_ID_LEN]
                    .try_into()
                    .expect("node id slice is 8 bytes"),
            ),
        ))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn order_code(&self) -> u64 {
        self.order_code
    }

    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_ORDERED_KEY_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
        buf.put_u8(KEY_KIND_VECTOR);
        buf.put_u64(self.order_code);
        buf.put_u64(self.node_id);
    }
}
