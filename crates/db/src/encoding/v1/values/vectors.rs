//! Current vector physical-row value codecs.
//!
//! Every function in this module family preserves an already deployed byte
//! layout. The submodules separate coarse physical value families so search,
//! lifecycle, and cache code consume one typed construction/parsing boundary.
//! Additive generation descriptors live beside these codecs and bind their
//! otherwise versionless bytes before runtime use.

use std::borrow::Cow;

use bytes::Bytes;

use super::{
    checked_len_with_element_count, ensure_exact_len, ensure_min_len, take_u32_be, take_u64_le,
    take_u8, ENCODING_TYPE_LEN, U32_LEN, U64_LEN,
};
use crate::encoding::{error::EncodingError, NodeId};

pub mod entry;
pub(crate) mod item;
pub(crate) mod markers;
pub(crate) mod metadata;
pub mod neighbors;
pub(crate) mod simhash;

/// Encoding type: compact packed layer-0 neighbor list.
///
/// Value must remain > 0x04 to avoid edge-delta marker overlap.
pub const ENCODING_TYPE_LAYER0_NEIGHBORS: u8 = 0x12;

/// Encoding type: compact layer-0 record with optional co-located SimHash.
///
/// Value must remain > 0x04 to avoid edge-delta marker overlap.
pub const ENCODING_TYPE_LAYER0_RECORD: u8 = 0x13;

const LAYER0_FLAG_SIMHASH_PRESENT: u8 = 0x01;

const LAYER0_FLAGS_LEN: usize = core::mem::size_of::<u8>();
const LAYER0_COUNT_LEN: usize = U32_LEN;
const NODE_ID_LEN: usize = core::mem::size_of::<NodeId>();
const SIMHASH_LEN: usize = U64_LEN;

const LAYER0_NEIGHBORS_HEADER_LEN: usize = ENCODING_TYPE_LEN + LAYER0_COUNT_LEN;
const LAYER0_RECORD_HEADER_LEN: usize = ENCODING_TYPE_LEN + LAYER0_FLAGS_LEN + LAYER0_COUNT_LEN;

#[inline]
fn encoded_len(header_len: usize, count: usize, extra_len: usize) -> usize {
    header_len
        .checked_add(extra_len)
        .and_then(|len| len.checked_add(count.checked_mul(NODE_ID_LEN)?))
        .expect("layer-0 encoded neighbor payload length overflow")
}

#[inline]
fn count_prefix(count: usize) -> [u8; LAYER0_COUNT_LEN] {
    let count = u32::try_from(count).expect("layer-0 neighbor count exceeds u32 count prefix");
    count.to_be_bytes()
}

#[inline]
fn append_node_ids(buf: &mut Vec<u8>, neighbors: &[NodeId]) {
    for &node_id in neighbors {
        buf.extend_from_slice(&node_id.to_be_bytes());
    }
}

#[inline]
fn canonical_neighbors(neighbors: &[NodeId]) -> Cow<'_, [NodeId]> {
    if neighbors.windows(2).all(|pair| pair[0] < pair[1]) {
        return Cow::Borrowed(neighbors);
    }

    let mut canonical = neighbors.to_vec();
    canonical.sort_unstable();
    canonical.dedup();
    Cow::Owned(canonical)
}

#[inline]
fn decode_node_ids(data: &[u8], count: usize) -> Vec<NodeId> {
    debug_assert_eq!(data.len(), count * NODE_ID_LEN);

    let mut neighbors = Vec::with_capacity(count);
    for chunk in data.chunks_exact(NODE_ID_LEN) {
        let bytes = chunk.try_into().expect("node id chunk is 8 bytes");
        neighbors.push(NodeId::from_be_bytes(bytes));
    }
    neighbors
}

/// Encode a layer-0 neighbor snapshot to compact bytes.
///
/// Format: [encoding_type:1][count:4 BE][node_id_1:8 BE]...
///
/// The payload is canonicalized to sorted unique node IDs to preserve deterministic
/// iteration behavior currently provided by Roaring-based snapshots.
#[inline]
pub fn encode_layer0_neighbors(neighbors: &[NodeId]) -> Bytes {
    let canonical = canonical_neighbors(neighbors);
    let count = count_prefix(canonical.len());
    let capacity = encoded_len(LAYER0_NEIGHBORS_HEADER_LEN, canonical.len(), 0);

    let mut buf = Vec::with_capacity(capacity);
    buf.push(ENCODING_TYPE_LAYER0_NEIGHBORS);
    buf.extend_from_slice(&count);
    append_node_ids(&mut buf, &canonical);
    debug_assert_eq!(buf.len(), capacity);

    Bytes::from(buf)
}

/// Encode a layer-0 record with neighbors and optional SimHash.
///
/// Format: [encoding_type:1][flags:1][count:4 BE][simhash?:8 LE][node_id_1:8 BE]...
#[inline]
pub fn encode_layer0_record(neighbors: &[NodeId], simhash_bits: Option<u64>) -> Bytes {
    let canonical = canonical_neighbors(neighbors);
    let count = count_prefix(canonical.len());
    let simhash_len = simhash_bits.map_or(0, |_| SIMHASH_LEN);
    let capacity = encoded_len(LAYER0_RECORD_HEADER_LEN, canonical.len(), simhash_len);

    let mut buf = Vec::with_capacity(capacity);
    buf.push(ENCODING_TYPE_LAYER0_RECORD);
    buf.push(if simhash_bits.is_some() {
        LAYER0_FLAG_SIMHASH_PRESENT
    } else {
        0
    });
    buf.extend_from_slice(&count);
    if let Some(bits) = simhash_bits {
        buf.extend_from_slice(&bits.to_le_bytes());
    }
    append_node_ids(&mut buf, &canonical);
    debug_assert_eq!(buf.len(), capacity);

    Bytes::from(buf)
}

#[inline]
fn decode_layer0_neighbors_v1(data: &[u8]) -> Result<Vec<NodeId>, EncodingError> {
    ensure_min_len(data, LAYER0_NEIGHBORS_HEADER_LEN)?;

    let mut offset = ENCODING_TYPE_LEN;
    let count = take_u32_be(data, &mut offset)?;
    let expected = checked_len_with_element_count(
        offset,
        count,
        NODE_ID_LEN,
        "Layer-0 neighbor payload length overflow",
    )?;
    ensure_exact_len(data, expected)?;

    Ok(decode_node_ids(&data[offset..], count))
}

#[inline]
fn decode_layer0_record_v2(data: &[u8]) -> Result<(Vec<NodeId>, Option<u64>), EncodingError> {
    ensure_min_len(data, LAYER0_RECORD_HEADER_LEN)?;

    let mut offset = ENCODING_TYPE_LEN;
    let flags = take_u8(data, &mut offset)?;
    if flags & !LAYER0_FLAG_SIMHASH_PRESENT != 0 {
        return Err(EncodingError::Custom(format!(
            "Invalid layer-0 record flags: {flags:#04x}"
        )));
    }

    let count = take_u32_be(data, &mut offset)?;
    let simhash_bits = if flags & LAYER0_FLAG_SIMHASH_PRESENT != 0 {
        Some(take_u64_le(data, &mut offset)?)
    } else {
        None
    };

    let expected = checked_len_with_element_count(
        offset,
        count,
        NODE_ID_LEN,
        "Layer-0 record payload length overflow",
    )?;
    ensure_exact_len(data, expected)?;

    Ok((decode_node_ids(&data[offset..], count), simhash_bits))
}

/// Decode a compact layer-0 record and return neighbors + optional SimHash bits.
#[inline]
pub fn decode_layer0_neighbors_and_simhash(
    data: &[u8],
) -> Result<(Vec<NodeId>, Option<u64>), EncodingError> {
    if data.is_empty() {
        return Ok((Vec::new(), None));
    }

    match data[0] {
        ENCODING_TYPE_LAYER0_NEIGHBORS => {
            let neighbors = decode_layer0_neighbors_v1(data)?;
            Ok((neighbors, None))
        }
        ENCODING_TYPE_LAYER0_RECORD => decode_layer0_record_v2(data),
        other => Err(EncodingError::InvalidEncodingType(other)),
    }
}

/// Decode a compact layer-0 neighbor snapshot.
#[inline]
pub fn decode_layer0_neighbors(data: &[u8]) -> Result<Vec<NodeId>, EncodingError> {
    let (neighbors, _) = decode_layer0_neighbors_and_simhash(data)?;
    Ok(neighbors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    #[test]
    fn encode_layer0_neighbors_zero_neighbors_has_exact_layout() {
        let encoded = encode_layer0_neighbors(&[]);

        assert_eq!(
            encoded.as_ref(),
            &[ENCODING_TYPE_LAYER0_NEIGHBORS, 0, 0, 0, 0]
        );
        assert_eq!(
            decode_layer0_neighbors(&encoded).unwrap(),
            Vec::<NodeId>::new()
        );
    }

    #[test]
    fn encode_layer0_neighbors_preserves_wire_layout_and_canonicalizes() {
        let encoded = encode_layer0_neighbors(&[7, 3, 7]);
        let mut expected = vec![ENCODING_TYPE_LAYER0_NEIGHBORS, 0, 0, 0, 2];
        expected.extend_from_slice(&3u64.to_be_bytes());
        expected.extend_from_slice(&7u64.to_be_bytes());

        assert_eq!(encoded.as_ref(), expected.as_slice());
    }

    #[test]
    fn encode_layer0_record_preserves_wire_layout_with_simhash() {
        let encoded = encode_layer0_record(&[9, 1, 9], Some(0x0102_0304_0506_0708));
        let mut expected = vec![
            ENCODING_TYPE_LAYER0_RECORD,
            LAYER0_FLAG_SIMHASH_PRESENT,
            0,
            0,
            0,
            2,
        ];
        expected.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.extend_from_slice(&9u64.to_be_bytes());

        assert_eq!(encoded.as_ref(), expected.as_slice());
    }

    #[test]
    fn encode_layer0_record_preserves_wire_layout_without_simhash() {
        let encoded = encode_layer0_record(&[1, 9], None);
        let mut expected = vec![ENCODING_TYPE_LAYER0_RECORD, 0, 0, 0, 0, 2];
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.extend_from_slice(&9u64.to_be_bytes());

        assert_eq!(encoded.as_ref(), expected.as_slice());
        assert_eq!(
            decode_layer0_neighbors_and_simhash(&encoded).unwrap(),
            (vec![1, 9], None)
        );
    }

    #[test]
    fn canonical_neighbors_borrows_already_sorted_unique_input() {
        let neighbors = [1, 2, 3];
        let canonical = canonical_neighbors(&neighbors);
        assert!(matches!(canonical, Cow::Borrowed(_)));
        assert_eq!(canonical.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn vector_private_helpers_cover_owned_and_decode_paths() {
        let canonical = canonical_neighbors(&[3, 1, 3, 2]);
        assert!(matches!(canonical, Cow::Owned(_)));
        assert_eq!(canonical.as_ref(), &[1, 2, 3]);

        let mut encoded_ids = Vec::new();
        append_node_ids(&mut encoded_ids, &[9, 1]);
        assert_eq!(decode_node_ids(&encoded_ids, 2), vec![9, 1]);
        assert_eq!(encoded_len(LAYER0_NEIGHBORS_HEADER_LEN, 2, 0), 21);
        assert_eq!(count_prefix(2), [0, 0, 0, 2]);
    }

    #[test]
    fn encode_layer0_neighbors_handles_min_and_max_node_ids() {
        let encoded = encode_layer0_neighbors(&[NodeId::MAX, NodeId::MIN]);

        assert_eq!(
            decode_layer0_neighbors(&encoded).unwrap(),
            vec![NodeId::MIN, NodeId::MAX]
        );
    }

    #[test]
    fn decode_layer0_neighbors_accepts_empty_compatibility_value() {
        assert_eq!(decode_layer0_neighbors(&[]).unwrap(), Vec::<NodeId>::new());
        assert_eq!(
            decode_layer0_neighbors_and_simhash(&[]).unwrap(),
            (Vec::new(), None)
        );
    }

    #[test]
    fn decode_layer0_neighbors_decodes_legacy_snapshot_without_simhash() {
        let encoded = encode_layer0_neighbors(&[4, 2, 2]);

        assert_eq!(
            decode_layer0_neighbors_and_simhash(&encoded).unwrap(),
            (vec![2, 4], None)
        );
    }

    #[test]
    fn decode_layer0_record_decodes_optional_simhash() {
        let encoded = encode_layer0_record(&[2], Some(0xAABB_CCDD_EEFF_0011));

        assert_eq!(
            decode_layer0_neighbors_and_simhash(&encoded).unwrap(),
            (vec![2], Some(0xAABB_CCDD_EEFF_0011))
        );
    }

    #[test]
    fn decode_layer0_record_without_simhash() {
        let encoded = encode_layer0_record(&[2], None);

        assert_eq!(
            decode_layer0_neighbors_and_simhash(&encoded).unwrap(),
            (vec![2], None)
        );
    }

    #[test]
    fn decode_layer0_neighbors_rejects_invalid_encoding_type() {
        assert!(matches!(
            decode_layer0_neighbors(&[0xFF]),
            Err(EncodingError::InvalidEncodingType(0xFF))
        ));
    }

    #[test]
    fn decode_layer0_record_rejects_unknown_flags() {
        let err =
            decode_layer0_neighbors_and_simhash(&[ENCODING_TYPE_LAYER0_RECORD, 0x80, 0, 0, 0, 0])
                .unwrap_err();

        assert!(
            matches!(err, EncodingError::Custom(message) if message.contains("Invalid layer-0 record flags"))
        );
    }

    #[test]
    fn decode_layer0_neighbors_reports_short_header() {
        assert!(matches!(
            decode_layer0_neighbors(&[ENCODING_TYPE_LAYER0_NEIGHBORS]),
            Err(EncodingError::BufferTooShort {
                expected: LAYER0_NEIGHBORS_HEADER_LEN,
                actual: 1
            })
        ));
    }

    #[test]
    fn decode_layer0_record_reports_missing_simhash_bytes() {
        assert!(matches!(
            decode_layer0_neighbors_and_simhash(&[
                ENCODING_TYPE_LAYER0_RECORD,
                LAYER0_FLAG_SIMHASH_PRESENT,
                0,
                0,
                0,
                0,
            ]),
            Err(EncodingError::BufferTooShort {
                expected: 14,
                actual: 6
            })
        ));
    }

    #[test]
    fn decode_layer0_neighbors_rejects_trailing_bytes() {
        let mut encoded = encode_layer0_neighbors(&[1]).to_vec();
        encoded.push(0);

        assert!(matches!(
            decode_layer0_neighbors(&encoded),
            Err(EncodingError::BufferTooShort {
                expected: 13,
                actual: 14
            })
        ));
    }

    #[test]
    fn decode_layer0_record_rejects_trailing_bytes_without_simhash() {
        let mut encoded = encode_layer0_record(&[1], None).to_vec();
        encoded.push(0);

        assert!(matches!(
            decode_layer0_neighbors_and_simhash(&encoded),
            Err(EncodingError::BufferTooShort {
                expected: 14,
                actual: 15
            })
        ));
    }

    #[test]
    fn decode_layer0_neighbors_rejects_count_payload_mismatch() {
        let mut encoded = vec![ENCODING_TYPE_LAYER0_NEIGHBORS, 0, 0, 0, 2];
        encoded.extend_from_slice(&1u64.to_be_bytes());

        assert!(matches!(
            decode_layer0_neighbors(&encoded),
            Err(EncodingError::BufferTooShort {
                expected: 21,
                actual: 13
            })
        ));
    }

    #[test]
    #[should_panic(expected = "layer-0 neighbor count exceeds u32 count prefix")]
    fn count_prefix_panics_when_count_exceeds_u32() {
        let _ = count_prefix(u32::MAX as usize + 1);
    }

    #[test]
    #[should_panic(expected = "layer-0 encoded neighbor payload length overflow")]
    fn encoded_len_panics_on_overflow() {
        let _ = encoded_len(usize::MAX, 1, 1);
    }

    proptest! {
        #[test]
        fn encode_layer0_neighbors_canonicalizes_arbitrary_neighbors(neighbors in proptest::collection::vec(any::<NodeId>(), 0..64)) {
            let expected = neighbors.iter().copied().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();

            prop_assert_eq!(decode_layer0_neighbors(&encode_layer0_neighbors(&neighbors)).unwrap(), expected);
        }
    }
}
