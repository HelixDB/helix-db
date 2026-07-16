//! Current-format HNSW neighbor-list values outside layer 0.
//!
//! Upper-layer rows store a big-endian `u32` count followed by big-endian node
//! IDs. The retained flat codec stores only consecutive big-endian node IDs.
//! Both decoders require exact lengths so trailing corruption cannot be treated
//! as another valid list. Layer-0's tagged compatibility codecs remain in the
//! parent vector-values module.

use bytes::Bytes;

use crate::encoding::{error::EncodingError, NodeId};

const UPPER_NEIGHBOR_COUNT_LEN: usize = core::mem::size_of::<u32>();
const NODE_ID_LEN: usize = core::mem::size_of::<NodeId>();

/// Encodes a flat sequence of big-endian node IDs in its deployed format.
///
/// ```
/// use db::encoding::v1::values::vectors::neighbors::{
///     decode_flat_neighbors, encode_flat_neighbors,
/// };
///
/// let bytes = encode_flat_neighbors(&[1, 2]);
/// assert_eq!(decode_flat_neighbors(&bytes).unwrap(), vec![1, 2]);
/// ```
#[must_use]
pub fn encode_flat_neighbors(neighbors: &[NodeId]) -> Bytes {
    let capacity = neighbors
        .len()
        .checked_mul(NODE_ID_LEN)
        .expect("flat vector neighbor value length exceeds usize");
    let mut bytes = Vec::with_capacity(capacity);
    for &node_id in neighbors {
        bytes.extend_from_slice(&node_id.to_be_bytes());
    }
    Bytes::from(bytes)
}

/// Decodes an exact flat sequence of big-endian node IDs.
pub fn decode_flat_neighbors(data: &[u8]) -> Result<Vec<NodeId>, EncodingError> {
    if !data.len().is_multiple_of(NODE_ID_LEN) {
        return Err(EncodingError::Custom(format!(
            "invalid flat vector neighbor length: expected a multiple of {NODE_ID_LEN}, got {}",
            data.len()
        )));
    }

    Ok(data
        .chunks_exact(NODE_ID_LEN)
        .map(|chunk| {
            NodeId::from_be_bytes(
                chunk
                    .try_into()
                    .expect("flat vector neighbor node id slice is 8 bytes"),
            )
        })
        .collect())
}

/// Encodes upper-layer neighbors as `[count:4 BE][node_id:8 BE]...`.
pub(crate) fn encode_upper_neighbors(neighbors: &[NodeId]) -> Result<Bytes, EncodingError> {
    let count = u32::try_from(neighbors.len()).map_err(|_| {
        EncodingError::Custom("upper-layer vector neighbor count exceeds u32".to_string())
    })?;
    let payload_len = neighbors.len().checked_mul(NODE_ID_LEN).ok_or_else(|| {
        EncodingError::Custom("upper-layer vector neighbor value length overflow".to_string())
    })?;
    let capacity = UPPER_NEIGHBOR_COUNT_LEN
        .checked_add(payload_len)
        .ok_or_else(|| {
            EncodingError::Custom("upper-layer vector neighbor value length overflow".to_string())
        })?;

    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(&count.to_be_bytes());
    for &node_id in neighbors {
        bytes.extend_from_slice(&node_id.to_be_bytes());
    }
    Ok(Bytes::from(bytes))
}

/// Decodes an exact upper-layer neighbor row.
pub(crate) fn decode_upper_neighbors(data: &[u8]) -> Result<Vec<NodeId>, EncodingError> {
    if data.len() < UPPER_NEIGHBOR_COUNT_LEN {
        return Err(EncodingError::BufferTooShort {
            expected: UPPER_NEIGHBOR_COUNT_LEN,
            actual: data.len(),
        });
    }
    let count = u32::from_be_bytes(
        data[0..UPPER_NEIGHBOR_COUNT_LEN]
            .try_into()
            .expect("upper-layer vector neighbor count slice is 4 bytes"),
    ) as usize;
    let payload_len = count.checked_mul(NODE_ID_LEN).ok_or_else(|| {
        EncodingError::Custom("upper-layer vector neighbor value length overflow".to_string())
    })?;
    let expected_len = UPPER_NEIGHBOR_COUNT_LEN
        .checked_add(payload_len)
        .ok_or_else(|| {
            EncodingError::Custom("upper-layer vector neighbor value length overflow".to_string())
        })?;
    if data.len() != expected_len {
        return Err(EncodingError::BufferTooShort {
            expected: expected_len,
            actual: data.len(),
        });
    }

    Ok(data[UPPER_NEIGHBOR_COUNT_LEN..]
        .chunks_exact(NODE_ID_LEN)
        .map(|chunk| {
            NodeId::from_be_bytes(
                chunk
                    .try_into()
                    .expect("upper-layer vector neighbor node id slice is 8 bytes"),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_neighbor_bytes_are_frozen() {
        let encoded = encode_flat_neighbors(&[1, NodeId::MAX]);
        let mut expected = Vec::new();
        expected.extend_from_slice(&1_u64.to_be_bytes());
        expected.extend_from_slice(&NodeId::MAX.to_be_bytes());
        assert_eq!(encoded.as_ref(), expected.as_slice());
        assert_eq!(
            decode_flat_neighbors(&encoded).unwrap(),
            vec![1, NodeId::MAX]
        );
    }

    #[test]
    fn upper_neighbor_bytes_are_frozen() {
        let encoded = encode_upper_neighbors(&[1, 2]).unwrap();
        let mut expected = 2_u32.to_be_bytes().to_vec();
        expected.extend_from_slice(&1_u64.to_be_bytes());
        expected.extend_from_slice(&2_u64.to_be_bytes());
        assert_eq!(encoded.as_ref(), expected.as_slice());
        assert_eq!(decode_upper_neighbors(&encoded).unwrap(), vec![1, 2]);
    }

    #[test]
    fn neighbor_decoders_reject_malformed_and_trailing_bytes() {
        assert!(decode_flat_neighbors(&[0]).is_err());
        assert!(decode_upper_neighbors(&[0, 0, 0]).is_err());

        let mut truncated = encode_upper_neighbors(&[1]).unwrap().to_vec();
        truncated.pop();
        assert!(decode_upper_neighbors(&truncated).is_err());

        let mut trailing = encode_upper_neighbors(&[1]).unwrap().to_vec();
        trailing.push(0);
        assert!(decode_upper_neighbors(&trailing).is_err());
    }
}
