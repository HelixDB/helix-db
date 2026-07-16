use bytes::Bytes;
use roaring::RoaringTreemap;

use super::{take_slice, take_u32_le, take_u8, ENCODING_TYPE_LEN, U32_LEN};
use crate::encoding::{error::EncodingError, NodeId};

const BITMAP_LEN_PREFIX_LEN: usize = U32_LEN;

/// Encoding type: No compression
///
/// Note: Value must be > 0x04 to avoid collision with edge delta op codes (0x00-0x04)
pub const ENCODING_TYPE_NONE: u8 = 0x10;

/// Encoding type: Elias-Fano with Partitioning (WIP)
///
/// Note: Value must be > 0x04 to avoid collision with edge delta op codes (0x00-0x04)
pub const ENCODING_TYPE_EFP: u8 = 0x11;

// RoaringTreemap's portable format starts with a u64 shard count. An empty
// treemap is therefore exactly that zero count and no following bitmaps.
const EMPTY_ROARING_TREEMAP_LEN: usize = core::mem::size_of::<u64>();
const EMPTY_EDGES_ENCODED_LEN: usize = ENCODING_TYPE_LEN
    + BITMAP_LEN_PREFIX_LEN
    + EMPTY_ROARING_TREEMAP_LEN
    + BITMAP_LEN_PREFIX_LEN
    + EMPTY_ROARING_TREEMAP_LEN;

const fn empty_edges_bytes_array() -> [u8; EMPTY_EDGES_ENCODED_LEN] {
    let empty_len = (EMPTY_ROARING_TREEMAP_LEN as u32).to_le_bytes();
    let in_len_offset = ENCODING_TYPE_LEN + BITMAP_LEN_PREFIX_LEN + EMPTY_ROARING_TREEMAP_LEN;
    let mut bytes = [0; EMPTY_EDGES_ENCODED_LEN];

    bytes[0] = ENCODING_TYPE_NONE;
    bytes[1] = empty_len[0];
    bytes[2] = empty_len[1];
    bytes[3] = empty_len[2];
    bytes[4] = empty_len[3];
    bytes[in_len_offset] = empty_len[0];
    bytes[in_len_offset + 1] = empty_len[1];
    bytes[in_len_offset + 2] = empty_len[2];
    bytes[in_len_offset + 3] = empty_len[3];

    bytes
}

static EMPTY_EDGES_BYTES: [u8; EMPTY_EDGES_ENCODED_LEN] = empty_edges_bytes_array();

/// Eager edge updates: read-modify-write adjacency list
/// Best for low-degree vertices where read cost is small
pub const EDGE_UPDATE_EAGER: u8 = 0x0;

/// Lazy edge updates: append deltas, defer compaction
/// Best for high-degree vertices to avoid read amplification
pub const EDGE_UPDATE_LAZY: u8 = 0x1;

/// Adaptive policy: use Morris counter to choose eager/lazy per-node
pub const EDGE_UPDATE_ADAPTIVE: u8 = 0x2;

#[inline]
fn bitmap_len_prefix(size: usize, direction: &str) -> u32 {
    u32::try_from(size)
        .unwrap_or_else(|_| panic!("{direction} edge bitmap exceeds u32 length prefix"))
}

/// Encoded representation of an empty adjacency list.
#[inline]
pub fn empty_edges_bytes() -> Bytes {
    Bytes::from_static(&EMPTY_EDGES_BYTES)
}

/// Adjacency list for a node using RoaringTreemap for compression
///
/// Stores both outgoing and incoming edges using compressed bitmaps.
/// RoaringTreemap provides excellent compression for sequential IDs
/// and O(log n) membership testing.
#[derive(Debug, Clone, Default)]
pub struct Edges {
    /// Outgoing edges (this node -> neighbor)
    pub(crate) nxts_out: RoaringTreemap,
    /// Incoming edges (neighbor -> this node)
    pub(crate) nxts_in: RoaringTreemap,
}

impl Edges {
    /// Create an empty adjacency list
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an outgoing edge
    #[inline]
    pub fn add_out(&mut self, neighbor: NodeId) {
        self.nxts_out.insert(neighbor);
    }

    /// Add an incoming edge
    #[inline]
    pub fn add_in(&mut self, neighbor: NodeId) {
        self.nxts_in.insert(neighbor);
    }

    /// Remove an outgoing edge. Returns true if it existed.
    #[inline]
    pub fn remove_out(&mut self, neighbor: NodeId) -> bool {
        self.nxts_out.remove(neighbor)
    }

    /// Remove an incoming edge. Returns true if it existed.
    #[inline]
    pub fn remove_in(&mut self, neighbor: NodeId) -> bool {
        self.nxts_in.remove(neighbor)
    }

    /// Check if an outgoing edge exists
    #[inline]
    pub fn contains_out(&self, neighbor: NodeId) -> bool {
        self.nxts_out.contains(neighbor)
    }

    /// Check if an incoming edge exists
    #[inline]
    pub fn contains_in(&self, neighbor: NodeId) -> bool {
        self.nxts_in.contains(neighbor)
    }

    /// Number of outgoing edges
    #[inline]
    pub fn num_edges_out(&self) -> usize {
        self.nxts_out.len() as usize
    }

    /// Number of incoming edges
    #[inline]
    pub fn num_edges_in(&self) -> usize {
        self.nxts_in.len() as usize
    }

    /// Total number of edges (out + in)
    #[inline]
    pub fn num_edges(&self) -> usize {
        self.num_edges_out() + self.num_edges_in()
    }

    /// Iterate over outgoing edges in sorted order
    #[inline]
    pub fn iter_out(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nxts_out.iter()
    }

    /// Iterate over incoming edges in sorted order
    #[inline]
    pub fn iter_in(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.nxts_in.iter()
    }

    /// Get the nth outgoing edge (0-indexed)
    #[inline]
    pub fn get_out_nth(&self, n: usize) -> Option<NodeId> {
        self.nxts_out.select(n as u64)
    }

    /// Get the nth incoming edge (0-indexed)
    #[inline]
    pub fn get_in_nth(&self, n: usize) -> Option<NodeId> {
        self.nxts_in.select(n as u64)
    }

    /// Merge edges from another Edges instance
    pub fn merge(&mut self, other: &Edges) {
        self.nxts_out |= &other.nxts_out;
        self.nxts_in |= &other.nxts_in;
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nxts_out.is_empty() && self.nxts_in.is_empty()
    }
}

/// Encode an `Edges` struct to bytes
///
/// Format: [encoding_type:1][out_len:4 LE][out_bitmap][in_len:4 LE][in_bitmap]
#[inline]
pub fn encode_edges(edges: &Edges) -> Bytes {
    let out_size = bitmap_len_prefix(edges.nxts_out.serialized_size(), "outgoing");
    let in_size = bitmap_len_prefix(edges.nxts_in.serialized_size(), "incoming");

    // 1 byte encoding type + 4 bytes out_len + out_bitmap + 4 bytes in_len + in_bitmap
    let total_size = ENCODING_TYPE_LEN
        + BITMAP_LEN_PREFIX_LEN
        + out_size as usize
        + BITMAP_LEN_PREFIX_LEN
        + in_size as usize;
    let mut buf = Vec::with_capacity(total_size);

    // Encoding type
    buf.push(ENCODING_TYPE_NONE);

    // Out edges
    buf.extend_from_slice(&out_size.to_le_bytes());
    edges
        .nxts_out
        .serialize_into(&mut buf)
        .expect("serialize to vec cannot fail");

    // In edges
    buf.extend_from_slice(&in_size.to_le_bytes());
    edges
        .nxts_in
        .serialize_into(&mut buf)
        .expect("serialize to vec cannot fail");

    debug_assert_eq!(buf.len(), total_size);

    Bytes::from(buf)
}

/// Decode bytes to an `Edges` struct
///
/// Returns the decoded edges. Returns an error if the buffer is malformed.
#[inline]
pub fn decode_edges(data: &[u8]) -> Result<Edges, EncodingError> {
    if data.is_empty() {
        return Ok(Edges::new());
    }

    let mut offset = 0;
    let encoding_type = take_u8(data, &mut offset)?;
    if encoding_type != ENCODING_TYPE_NONE {
        return Err(EncodingError::InvalidEncodingType(encoding_type));
    }

    let out_len = take_u32_le(data, &mut offset)?;
    let out_bytes = take_slice(data, &mut offset, out_len)?;
    let nxts_out = RoaringTreemap::deserialize_from(out_bytes)?;

    let in_len = take_u32_le(data, &mut offset)?;
    let in_bytes = take_slice(data, &mut offset, in_len)?;
    let nxts_in = RoaringTreemap::deserialize_from(in_bytes)?;

    // Older decoder behavior ignored trailing bytes; preserve that leniency for
    // on-disk compatibility.

    Ok(Edges { nxts_out, nxts_in })
}

/// Decode only the outgoing edges to an `Edges` struct
///
/// Returns the decoded edges. Returns an error if the buffer is malformed.
#[inline]
pub fn decode_out_edges(data: &[u8]) -> Result<Edges, EncodingError> {
    if data.is_empty() {
        return Ok(Edges::new());
    }

    let mut offset = 0;
    let encoding_type = take_u8(data, &mut offset)?;
    if encoding_type != ENCODING_TYPE_NONE {
        return Err(EncodingError::InvalidEncodingType(encoding_type));
    }

    let out_len = take_u32_le(data, &mut offset)?;
    let out_bytes = take_slice(data, &mut offset, out_len)?;
    let nxts_out = RoaringTreemap::deserialize_from(out_bytes)?;

    Ok(Edges {
        nxts_out,
        nxts_in: RoaringTreemap::new(),
    })
}

/// Decode only the incoming edges to an `Edges` struct
///
/// Uses the length header of the outgoing edges to skip to the incoming edges.
/// Returns the decoded edges. Returns an error if the buffer is malformed.
#[inline]
pub fn decode_in_edges(data: &[u8]) -> Result<Edges, EncodingError> {
    if data.is_empty() {
        return Ok(Edges::new());
    }

    let mut offset = 0;
    let encoding_type = take_u8(data, &mut offset)?;
    if encoding_type != ENCODING_TYPE_NONE {
        return Err(EncodingError::InvalidEncodingType(encoding_type));
    }

    // Skip the outgoing edges
    let out_len = take_u32_le(data, &mut offset)?;
    offset += out_len;

    let in_len = take_u32_le(data, &mut offset)?;
    let in_bytes = take_slice(data, &mut offset, in_len)?;
    let nxts_in = RoaringTreemap::deserialize_from(in_bytes)?;

    Ok(Edges {
        nxts_out: RoaringTreemap::new(),
        nxts_in,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_edges_bytes_matches_encoder() {
        let generated = empty_edges_bytes_array();
        assert_eq!(generated.as_slice(), empty_edges_bytes().as_ref());
        assert_eq!(generated.as_slice(), encode_edges(&Edges::new()).as_ref());
        assert_eq!(empty_edges_bytes().len(), EMPTY_EDGES_ENCODED_LEN);
    }

    #[test]
    fn edge_encoding_and_update_policy_tags_are_stable() {
        assert_eq!(ENCODING_TYPE_NONE, 0x10);
        assert_eq!(ENCODING_TYPE_EFP, 0x11);
        assert_eq!(EDGE_UPDATE_EAGER, 0x00);
        assert_eq!(EDGE_UPDATE_LAZY, 0x01);
        assert_eq!(EDGE_UPDATE_ADAPTIVE, 0x02);
    }

    #[test]
    fn decode_edges_round_trips_edges() {
        let mut edges = Edges::new();
        edges.add_out(7);
        edges.add_out(3);
        edges.add_in(11);

        let decoded = decode_edges(&encode_edges(&edges)).unwrap();
        assert_eq!(decoded.iter_out().collect::<Vec<_>>(), vec![3, 7]);
        assert_eq!(decoded.iter_in().collect::<Vec<_>>(), vec![11]);
    }

    #[test]
    fn decode_edges_reports_short_fixed_fields_as_buffer_too_short() {
        let err = decode_edges(&[ENCODING_TYPE_NONE, 8]).unwrap_err();
        assert!(matches!(
            err,
            EncodingError::BufferTooShort {
                expected: 5,
                actual: 2
            }
        ));
    }

    #[test]
    fn edges_api_updates_counts_and_membership() {
        let mut edges = Edges::new();
        assert!(edges.is_empty());

        edges.add_out(3);
        edges.add_out(7);
        edges.add_in(11);
        assert!(edges.contains_out(3));
        assert!(edges.contains_in(11));
        assert_eq!(edges.num_edges_out(), 2);
        assert_eq!(edges.num_edges_in(), 1);
        assert_eq!(edges.num_edges(), 3);
        assert_eq!(edges.get_out_nth(1), Some(7));
        assert_eq!(edges.get_in_nth(0), Some(11));

        assert!(edges.remove_out(3));
        assert!(!edges.remove_out(3));
        assert!(!edges.contains_out(3));
        assert!(edges.remove_in(11));
        assert!(!edges.remove_in(11));
        assert!(!edges.contains_in(11));
    }

    #[test]
    fn edges_merge_unions_both_directions() {
        let mut left = Edges::new();
        left.add_out(1);
        let mut right = Edges::new();
        right.add_out(2);
        right.add_in(3);

        left.merge(&right);

        assert_eq!(left.iter_out().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(left.iter_in().collect::<Vec<_>>(), vec![3]);
    }

    #[test]
    fn directional_decoders_only_decode_requested_side() {
        let mut edges = Edges::new();
        edges.add_out(1);
        edges.add_in(2);
        let encoded = encode_edges(&edges);

        let out = decode_out_edges(&encoded).unwrap();
        let incoming = decode_in_edges(&encoded).unwrap();

        assert_eq!(out.iter_out().collect::<Vec<_>>(), vec![1]);
        assert!(out.iter_in().collect::<Vec<_>>().is_empty());
        assert!(incoming.iter_out().collect::<Vec<_>>().is_empty());
        assert_eq!(incoming.iter_in().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn decoders_reject_invalid_encoding_type() {
        assert!(matches!(
            decode_edges(&[0xFF]),
            Err(EncodingError::InvalidEncodingType(0xFF))
        ));
        assert!(matches!(
            decode_out_edges(&[0xFF]),
            Err(EncodingError::InvalidEncodingType(0xFF))
        ));
        assert!(matches!(
            decode_in_edges(&[0xFF]),
            Err(EncodingError::InvalidEncodingType(0xFF))
        ));
    }

    #[test]
    fn decoders_accept_empty_compatibility_values() {
        assert!(decode_edges(&[]).unwrap().is_empty());
        assert!(decode_out_edges(&[]).unwrap().is_empty());
        assert!(decode_in_edges(&[]).unwrap().is_empty());
    }

    #[test]
    fn full_decoder_preserves_trailing_byte_leniency() {
        let mut edges = Edges::new();
        edges.add_out(1);
        let mut encoded = encode_edges(&edges).to_vec();
        encoded.extend_from_slice(&[0xAA, 0xBB]);

        assert_eq!(
            decode_edges(&encoded)
                .unwrap()
                .iter_out()
                .collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[test]
    fn directional_decoders_report_malformed_lengths() {
        let malformed_out = [ENCODING_TYPE_NONE, 4, 0, 0, 0, 1];
        assert!(decode_out_edges(&malformed_out).is_err());

        assert!(matches!(
            decode_edges(&malformed_out),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut skip_past_incoming_header = vec![ENCODING_TYPE_NONE];
        skip_past_incoming_header.extend_from_slice(&100u32.to_le_bytes());
        assert!(matches!(
            decode_in_edges(&skip_past_incoming_header),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut short_incoming_bitmap = vec![ENCODING_TYPE_NONE];
        short_incoming_bitmap.extend_from_slice(&0u32.to_le_bytes());
        short_incoming_bitmap.extend_from_slice(&4u32.to_le_bytes());
        short_incoming_bitmap.push(1);
        assert!(matches!(
            decode_in_edges(&short_incoming_bitmap),
            Err(EncodingError::BufferTooShort { .. })
        ));
    }

    #[test]
    #[should_panic(expected = "outgoing edge bitmap exceeds u32 length prefix")]
    fn bitmap_len_prefix_panics_when_size_exceeds_u32() {
        let _ = bitmap_len_prefix(u32::MAX as usize + 1, "outgoing");
    }
}
