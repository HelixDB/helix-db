//! Merge operators for Helix graph database
//!
//! Provides merge operators for:
//! - Edge deltas (add/remove edges without read-modify-write)
//! - Counter updates (node/edge counts)
//!
//! The `HelixMergeOperator` dispatches to the appropriate operator based on key prefix.

use bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError};

use super::encoding::{
    decode_edges, decode_layer0_neighbors_and_simhash, encode_edges, encode_layer0_record,
    ENCODING_TYPE_LAYER0_NEIGHBORS,
};
use super::key_space::KeySpace;
use super::types::Edges;

// =============================================================================
// Edge Delta Format
// =============================================================================

/// Edge delta operation types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDeltaOp {
    /// Add to outgoing edges
    AddOut = 0x00,
    /// Remove from outgoing edges
    RemoveOut = 0x01,
    /// Add to incoming edges
    AddIn = 0x02,
    /// Remove from incoming edges
    RemoveIn = 0x03,
    /// Reset outgoing edges to empty set
    ResetOut = 0x04,
}

impl EdgeDeltaOp {
    /// Create from byte value
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x00 => Some(Self::AddOut),
            0x01 => Some(Self::RemoveOut),
            0x02 => Some(Self::AddIn),
            0x03 => Some(Self::RemoveIn),
            0x04 => Some(Self::ResetOut),
            _ => None,
        }
    }
}

/// Encode an edge delta operation
///
/// Format: [op_type:1][node_id:8 BE]
#[inline]
pub fn encode_edge_delta(op: EdgeDeltaOp, node_id: u64) -> Bytes {
    if op == EdgeDeltaOp::ResetOut {
        return Bytes::from_static(&[EdgeDeltaOp::ResetOut as u8]);
    }

    let mut buf = Vec::with_capacity(9);
    buf.push(op as u8);
    buf.extend_from_slice(&node_id.to_be_bytes());
    Bytes::from(buf)
}

/// Encode a reset operation for outgoing neighbors.
#[inline]
pub fn encode_edge_reset_out() -> Bytes {
    Bytes::from_static(&[EdgeDeltaOp::ResetOut as u8])
}

/// Decode an edge delta operation
///
/// Returns (operation, node_id) or None if invalid
#[inline]
pub fn decode_edge_delta(data: &[u8]) -> Option<(EdgeDeltaOp, u64)> {
    if data.is_empty() {
        return None;
    }
    let op = EdgeDeltaOp::from_byte(data[0])?;
    if op == EdgeDeltaOp::ResetOut {
        return Some((op, 0));
    }
    if data.len() < 9 {
        return None;
    }
    let node_id = u64::from_be_bytes(data[1..9].try_into().unwrap());
    Some((op, node_id))
}

/// Check if data is an edge delta (vs a full snapshot)
///
/// Edge deltas start with 0x00-0x04, while snapshots use encoding markers > 0x04.
#[inline]
pub fn is_edge_delta(data: &[u8]) -> bool {
    !data.is_empty() && data[0] <= 0x04
}

const LAYER0_SIMHASH_SET: u8 = 0x05;
const LAYER0_SIMHASH_CLEAR: u8 = 0x06;

/// Encode layer-0 SimHash set operand.
///
/// Format: [0x05][simhash_bits:8 LE]
#[inline]
pub fn encode_layer0_set_simhash(simhash_bits: u64) -> Bytes {
    let mut buf = Vec::with_capacity(9);
    buf.push(LAYER0_SIMHASH_SET);
    buf.extend_from_slice(&simhash_bits.to_le_bytes());
    Bytes::from(buf)
}

/// Encode layer-0 SimHash clear operand.
///
/// Format: [0x06]
#[inline]
pub fn encode_layer0_clear_simhash() -> Bytes {
    Bytes::from_static(&[LAYER0_SIMHASH_CLEAR])
}

#[inline]
fn decode_layer0_simhash_operand(data: &[u8]) -> Option<Option<u64>> {
    if data.is_empty() {
        return None;
    }

    match data[0] {
        LAYER0_SIMHASH_SET if data.len() == 9 => {
            let bits = u64::from_le_bytes(data[1..9].try_into().unwrap());
            Some(Some(bits))
        }
        LAYER0_SIMHASH_CLEAR if data.len() == 1 => Some(None),
        _ => None,
    }
}

// =============================================================================
// Edge Merge Operator
// =============================================================================

/// Merge operator for edge adjacency lists
///
/// Supports delta operations (add/remove single edges) that are merged
/// during compaction, avoiding read-modify-write for high-degree vertices.
///
/// # Delta Format
/// - `[0x00][node_id:8]` - Add outgoing edge
/// - `[0x01][node_id:8]` - Remove outgoing edge
/// - `[0x02][node_id:8]` - Add incoming edge
/// - `[0x03][node_id:8]` - Remove incoming edge
/// - `[0x04]` - Reset outgoing edges
///
/// # Snapshot Format
/// Full `Edges` struct encoded with `encode_edges()`
#[derive(Debug, Clone, Default)]
pub struct EdgeMergeOperator;

impl EdgeMergeOperator {
    /// Create a new edge merge operator
    pub fn new() -> Self {
        Self
    }

    /// Apply a delta operation to an Edges struct
    fn apply_delta(edges: &mut Edges, op: EdgeDeltaOp, node_id: u64) {
        match op {
            EdgeDeltaOp::AddOut => {
                edges.nxts_out.insert(node_id);
            }
            EdgeDeltaOp::RemoveOut => {
                edges.nxts_out.remove(node_id);
            }
            EdgeDeltaOp::AddIn => {
                edges.nxts_in.insert(node_id);
            }
            EdgeDeltaOp::RemoveIn => {
                edges.nxts_in.remove(node_id);
            }
            EdgeDeltaOp::ResetOut => {
                edges.nxts_out.clear();
            }
        }
    }
}

impl MergeOperator for EdgeMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        // Parse existing value (snapshot or empty)
        let mut edges = match existing_value {
            Some(data) => {
                if is_edge_delta(&data) {
                    // Existing is also a delta - apply to empty and continue
                    let mut e = Edges::new();
                    if let Some((op, id)) = decode_edge_delta(&data) {
                        Self::apply_delta(&mut e, op, id);
                    }
                    e
                } else {
                    // Existing is a snapshot
                    decode_edges(&data).unwrap_or_default()
                }
            }
            None => Edges::new(),
        };

        // Apply the operand
        if is_edge_delta(&operand) {
            if let Some((op, id)) = decode_edge_delta(&operand) {
                Self::apply_delta(&mut edges, op, id);
            }
        } else {
            // Operand is a snapshot - union with existing
            if let Ok(other) = decode_edges(&operand) {
                edges.nxts_out |= &other.nxts_out;
                edges.nxts_in |= &other.nxts_in;
            }
        }

        Ok(encode_edges(&edges))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        // Parse existing value
        let mut edges = match existing_value {
            Some(data) => {
                if is_edge_delta(&data) {
                    let mut e = Edges::new();
                    if let Some((op, id)) = decode_edge_delta(&data) {
                        Self::apply_delta(&mut e, op, id);
                    }
                    e
                } else {
                    decode_edges(&data).unwrap_or_default()
                }
            }
            None => Edges::new(),
        };

        // Apply operands in reverse order (newest-to-oldest -> oldest-to-newest)
        for operand in operands.iter().rev() {
            if is_edge_delta(operand) {
                if let Some((op, id)) = decode_edge_delta(operand) {
                    Self::apply_delta(&mut edges, op, id);
                }
            } else {
                // Snapshot - union
                if let Ok(other) = decode_edges(operand) {
                    edges.nxts_out |= &other.nxts_out;
                    edges.nxts_in |= &other.nxts_in;
                }
            }
        }

        Ok(encode_edges(&edges))
    }
}

// =============================================================================
// Layer-0 Neighbor Merge Operator
// =============================================================================

/// Merge operator for HNSW layer-0 compact neighbor snapshots.
///
/// Snapshot payloads track outgoing adjacency with optional co-located SimHash bits.
#[derive(Debug, Clone, Default)]
pub struct Layer0NeighborMergeOperator;

#[derive(Debug, Clone, Default)]
struct Layer0State {
    neighbors: Vec<u64>,
    simhash_bits: Option<u64>,
}

impl Layer0NeighborMergeOperator {
    /// Create a new layer-0 neighbor merge operator.
    pub fn new() -> Self {
        Self
    }

    #[inline]
    fn add_neighbor(state: &mut Layer0State, node_id: u64) {
        if let Err(pos) = state.neighbors.binary_search(&node_id) {
            state.neighbors.insert(pos, node_id);
        }
    }

    #[inline]
    fn remove_neighbor(state: &mut Layer0State, node_id: u64) {
        if let Ok(pos) = state.neighbors.binary_search(&node_id) {
            state.neighbors.remove(pos);
        }
    }

    #[inline]
    fn apply_delta(state: &mut Layer0State, op: EdgeDeltaOp, node_id: u64) {
        match op {
            EdgeDeltaOp::AddOut => Self::add_neighbor(state, node_id),
            EdgeDeltaOp::RemoveOut => Self::remove_neighbor(state, node_id),
            EdgeDeltaOp::ResetOut => state.neighbors.clear(),
            EdgeDeltaOp::AddIn | EdgeDeltaOp::RemoveIn => {
                // Layer-0 compact payloads do not track inbound adjacency.
            }
        }
    }

    #[inline]
    fn decode_snapshot(bytes: &[u8]) -> Layer0State {
        let (neighbors, simhash_bits) = decode_layer0_neighbors_and_simhash(bytes)
            .expect("layer-0 merge requires compact layer-0 snapshot payload");
        Layer0State {
            neighbors,
            simhash_bits,
        }
    }

    #[inline]
    fn apply_operand(state: &mut Layer0State, operand: &[u8]) {
        if let Some(simhash_bits) = decode_layer0_simhash_operand(operand) {
            state.simhash_bits = simhash_bits;
            return;
        }

        if is_edge_delta(operand) {
            if let Some((op, id)) = decode_edge_delta(operand) {
                Self::apply_delta(state, op, id);
            }
            return;
        }

        // Snapshot operands are authoritative materialized neighbor sets.
        // Replace the accumulator to avoid re-introducing removed neighbors when
        // partial-merge snapshots are merged onto older base snapshots.
        let snapshot = Self::decode_snapshot(operand);
        state.neighbors = snapshot.neighbors;

        // Legacy layer-0 snapshots only contain neighbors, so preserve existing SimHash.
        if !operand.is_empty() && operand[0] != ENCODING_TYPE_LAYER0_NEIGHBORS {
            state.simhash_bits = snapshot.simhash_bits;
        }
    }
}

impl MergeOperator for Layer0NeighborMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut state = Layer0State::default();

        if let Some(data) = existing_value {
            Self::apply_operand(&mut state, &data);
        }

        Self::apply_operand(&mut state, &operand);

        Ok(encode_layer0_record(&state.neighbors, state.simhash_bits))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut state = Layer0State::default();

        if let Some(data) = existing_value {
            Self::apply_operand(&mut state, &data);
        }

        for operand in operands.iter().rev() {
            Self::apply_operand(&mut state, operand);
        }

        Ok(encode_layer0_record(&state.neighbors, state.simhash_bits))
    }
}

// =============================================================================
// Counter Merge Operator
// =============================================================================

/// A merge operator that sums signed 64-bit integers.
///
/// Used for global node/edge counts where we write deltas (+1, -1, etc.)
/// and they get summed during compaction/read.
///
/// # Wire Format
/// Values are stored as 8-byte signed big-endian integers.
#[derive(Debug, Clone, Default)]
pub struct CounterMergeOperator;

impl CounterMergeOperator {
    /// Create a new counter merge operator
    pub fn new() -> Self {
        Self
    }

    /// Decode a counter value from bytes
    #[inline]
    fn decode(data: &[u8]) -> i64 {
        if data.len() >= 8 {
            i64::from_be_bytes(data[..8].try_into().unwrap())
        } else {
            0
        }
    }

    /// Encode a counter value to bytes
    #[inline]
    fn encode(value: i64) -> Bytes {
        Bytes::copy_from_slice(&value.to_be_bytes())
    }
}

impl MergeOperator for CounterMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let existing = existing_value.map(|v| Self::decode(&v)).unwrap_or(0);
        let delta = Self::decode(&operand);
        // Clamp to 0 - counts can't go negative
        let new_value = (existing + delta).max(0);
        Ok(Self::encode(new_value))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut total = existing_value.map(|v| Self::decode(&v)).unwrap_or(0);

        // Order doesn't matter for addition
        for operand in operands {
            let delta = Self::decode(operand);
            total += delta;
        }

        // Clamp to 0 - counts can't go negative
        Ok(Self::encode(total.max(0)))
    }
}

/// Encode a counter delta value for use with merge operations
#[allow(dead_code)]
#[inline]
pub fn encode_counter_delta(delta: i64) -> Bytes {
    Bytes::copy_from_slice(&delta.to_be_bytes())
}

// =============================================================================
// Combined Helix Merge Operator
// =============================================================================

/// Combined merge operator for Helix that dispatches based on key prefix.
///
/// - Graph adjacency keys (0x00): Use EdgeMergeOperator
/// - HNSW layer-0 keys ([0x03][0x03]..[kind=0x03]): Use Layer0NeighborMergeOperator
/// - Metadata keys (0xFF): Use CounterMergeOperator
/// - Other keys: Pass through (last-writer-wins)
#[derive(Debug, Clone, Default)]
pub struct HelixMergeOperator {
    edge_op: EdgeMergeOperator,
    layer0_op: Layer0NeighborMergeOperator,
    counter_op: CounterMergeOperator,
}

impl HelixMergeOperator {
    /// Create a new Helix merge operator
    pub fn new() -> Self {
        Self {
            edge_op: EdgeMergeOperator::new(),
            layer0_op: Layer0NeighborMergeOperator::new(),
            counter_op: CounterMergeOperator::new(),
        }
    }

    /// HNSW layer-0 key format (vector-hot keyspace):
    /// [0xF0][index_id:8][kind:l0_vec_ks=0x16][node_id:8]
    #[inline]
    fn is_hnsw_layer0_key(key: &[u8]) -> bool {
        key.len() == 18 && key[0] == 0xF0 && key[9] == 0x16
    }

    /// Determine which operator to use based on key prefix
    fn key_type(key: &[u8]) -> KeyType {
        if key.is_empty() {
            return KeyType::Other;
        }
        if Self::is_hnsw_layer0_key(key) {
            return KeyType::Layer0;
        }

        match key[0] {
            x if x == KeySpace::Adjacency.prefix() => KeyType::Edge,
            x if x == KeySpace::Metadata.prefix() => KeyType::Counter,
            _ => KeyType::Other,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum KeyType {
    Edge,
    Layer0,
    Counter,
    Other,
}

impl MergeOperator for HelixMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        match Self::key_type(key) {
            KeyType::Edge => self.edge_op.merge(key, existing_value, operand),
            KeyType::Layer0 => self.layer0_op.merge(key, existing_value, operand),
            KeyType::Counter => self.counter_op.merge(key, existing_value, operand),
            KeyType::Other => {
                // Last-writer-wins for unknown key types
                Ok(operand)
            }
        }
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        match Self::key_type(key) {
            KeyType::Edge => self.edge_op.merge_batch(key, existing_value, operands),
            KeyType::Layer0 => self.layer0_op.merge_batch(key, existing_value, operands),
            KeyType::Counter => self.counter_op.merge_batch(key, existing_value, operands),
            KeyType::Other => {
                // Last-writer-wins: take the newest (first in the slice since newest-to-oldest)
                operands
                    .first()
                    .cloned()
                    .or(existing_value)
                    .ok_or(MergeOperatorError::EmptyBatch)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{decode_layer0_neighbors, encode_layer0_neighbors};
    use slatedb::object_store::memory::InMemory;
    use std::sync::Arc;

    // =========================================================================
    // Edge Delta Tests
    // =========================================================================

    #[test]
    fn test_encode_decode_edge_delta() {
        let encoded = encode_edge_delta(EdgeDeltaOp::AddOut, 12345);
        assert_eq!(encoded.len(), 9);
        assert_eq!(encoded[0], 0x00);

        let (op, id) = decode_edge_delta(&encoded).unwrap();
        assert_eq!(op, EdgeDeltaOp::AddOut);
        assert_eq!(id, 12345);
    }

    #[test]
    fn test_edge_delta_ops() {
        for (op, expected_byte) in [
            (EdgeDeltaOp::AddOut, 0x00),
            (EdgeDeltaOp::RemoveOut, 0x01),
            (EdgeDeltaOp::AddIn, 0x02),
            (EdgeDeltaOp::RemoveIn, 0x03),
            (EdgeDeltaOp::ResetOut, 0x04),
        ] {
            let encoded = encode_edge_delta(op, 42);
            assert_eq!(encoded[0], expected_byte);
            let (decoded_op, _) = decode_edge_delta(&encoded).unwrap();
            assert_eq!(decoded_op, op);
        }
    }

    #[test]
    fn test_is_edge_delta() {
        assert!(is_edge_delta(&[0x00, 0, 0, 0, 0, 0, 0, 0, 1]));
        assert!(is_edge_delta(&[0x03, 0, 0, 0, 0, 0, 0, 0, 1]));
        assert!(is_edge_delta(&[0x04]));
        assert!(!is_edge_delta(&[0x10, 0, 0, 0])); // Snapshot prefix (> 0x04)
        assert!(!is_edge_delta(&[]));
    }

    // =========================================================================
    // Edge Merge Operator Tests
    // =========================================================================

    #[test]
    fn test_edge_merge_add_out() {
        let op = EdgeMergeOperator::new();
        let key = Bytes::from(vec![0x00, 0, 0, 0, 0, 0, 0, 0, 1]); // Adjacency key

        // Add edge to node 5
        let delta = encode_edge_delta(EdgeDeltaOp::AddOut, 5);
        let result = op.merge(&key, None, delta).unwrap();

        let edges = decode_edges(&result).unwrap();
        assert!(edges.contains_out(5));
        assert_eq!(edges.num_edges_out(), 1);
    }

    #[test]
    fn test_edge_merge_add_remove() {
        let op = EdgeMergeOperator::new();
        let key = Bytes::from(vec![0x00, 0, 0, 0, 0, 0, 0, 0, 1]);

        // Add edge to node 5
        let delta1 = encode_edge_delta(EdgeDeltaOp::AddOut, 5);
        let result1 = op.merge(&key, None, delta1).unwrap();

        // Remove edge to node 5
        let delta2 = encode_edge_delta(EdgeDeltaOp::RemoveOut, 5);
        let result2 = op.merge(&key, Some(result1), delta2).unwrap();

        let edges = decode_edges(&result2).unwrap();
        assert!(!edges.contains_out(5));
        assert_eq!(edges.num_edges_out(), 0);
    }

    #[test]
    fn test_edge_merge_reset_out() {
        let op = EdgeMergeOperator::new();
        let key = Bytes::from(vec![0x00, 0, 0, 0, 0, 0, 0, 0, 1]);

        let add = encode_edge_delta(EdgeDeltaOp::AddOut, 5);
        let with_edge = op.merge(&key, None, add).unwrap();

        let reset = encode_edge_reset_out();
        let result = op.merge(&key, Some(with_edge), reset).unwrap();

        let edges = decode_edges(&result).unwrap();
        assert_eq!(edges.num_edges_out(), 0);
        assert!(!edges.contains_out(5));
    }

    #[test]
    fn test_edge_merge_batch() {
        let op = EdgeMergeOperator::new();
        let key = Bytes::from(vec![0x00, 0, 0, 0, 0, 0, 0, 0, 1]);

        // Operands in newest-to-oldest order (as SlateDB provides)
        let operands = vec![
            encode_edge_delta(EdgeDeltaOp::AddOut, 7),    // newest
            encode_edge_delta(EdgeDeltaOp::RemoveOut, 5), // middle
            encode_edge_delta(EdgeDeltaOp::AddOut, 5),    // oldest
        ];

        let result = op.merge_batch(&key, None, &operands).unwrap();
        let edges = decode_edges(&result).unwrap();

        // Applied oldest-to-newest: add 5, remove 5, add 7 -> {7}
        assert!(!edges.contains_out(5));
        assert!(edges.contains_out(7));
        assert_eq!(edges.num_edges_out(), 1);
    }

    #[test]
    fn test_edge_merge_add_remove_add() {
        let op = EdgeMergeOperator::new();
        let key = Bytes::from(vec![0x00, 0, 0, 0, 0, 0, 0, 0, 1]);

        // Operands: add 5, remove 5, add 5 (newest-to-oldest order)
        let operands = vec![
            encode_edge_delta(EdgeDeltaOp::AddOut, 5), // newest: add 5
            encode_edge_delta(EdgeDeltaOp::RemoveOut, 5), // middle: remove 5
            encode_edge_delta(EdgeDeltaOp::AddOut, 5), // oldest: add 5
        ];

        let result = op.merge_batch(&key, None, &operands).unwrap();
        let edges = decode_edges(&result).unwrap();

        // Applied oldest-to-newest: add 5, remove 5, add 5 -> {5}
        assert!(edges.contains_out(5));
        assert_eq!(edges.num_edges_out(), 1);
    }

    #[tokio::test]
    async fn test_slate_db_edge_merge_batch_boundary_preserves_remove() {
        let object_store = Arc::new(InMemory::new());
        let db = slatedb::Db::builder("/tmp/test_edge_merge_batch_boundary", object_store)
            .with_merge_operator(Arc::new(HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
        let key = Bytes::from(vec![KeySpace::Adjacency.prefix(), 0, 0, 0, 0, 0, 0, 0, 1]);

        let mut base = Edges::new();
        base.add_out(5);
        db.put(&key, encode_edges(&base)).await.unwrap();

        // Cross SlateDB's 100-operand merge batch boundary before reaching the base value.
        for neighbor in 1000..1099 {
            db.merge(&key, encode_edge_delta(EdgeDeltaOp::AddOut, neighbor))
                .await
                .unwrap();
        }
        db.merge(&key, encode_edge_delta(EdgeDeltaOp::RemoveOut, 5))
            .await
            .unwrap();

        let value = db.get(&key).await.unwrap().unwrap();
        let edges = decode_edges(&value).unwrap();
        assert!(!edges.contains_out(5));
        assert!(edges.contains_out(1000));
        assert!(edges.contains_out(1098));
    }

    // =========================================================================
    // Layer-0 Merge Operator Tests
    // =========================================================================

    #[test]
    fn test_layer0_merge_add_remove_reset_out() {
        let op = Layer0NeighborMergeOperator::new();
        let key = Bytes::from_static(b"l0");

        let base = encode_layer0_neighbors(&[9, 3]);
        let with_add = op
            .merge(&key, Some(base), encode_edge_delta(EdgeDeltaOp::AddOut, 5))
            .unwrap();
        let with_remove = op
            .merge(
                &key,
                Some(with_add),
                encode_edge_delta(EdgeDeltaOp::RemoveOut, 9),
            )
            .unwrap();
        let with_reset = op
            .merge(&key, Some(with_remove), encode_edge_reset_out())
            .unwrap();

        let decoded = decode_layer0_neighbors(&with_reset).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_layer0_merge_batch_ordering() {
        let op = Layer0NeighborMergeOperator::new();
        let key = Bytes::from_static(b"l0");

        let operands = vec![
            encode_edge_delta(EdgeDeltaOp::AddOut, 7),    // newest
            encode_edge_delta(EdgeDeltaOp::RemoveOut, 5), // middle
            encode_edge_delta(EdgeDeltaOp::AddOut, 5),    // oldest
        ];

        let result = op.merge_batch(&key, None, &operands).unwrap();
        let decoded = decode_layer0_neighbors(&result).unwrap();
        assert_eq!(decoded, vec![7]);
    }

    #[test]
    fn test_layer0_merge_batch_repeated_add_remove_same_node() {
        let op = Layer0NeighborMergeOperator::new();
        let key = Bytes::from_static(b"l0");
        let existing = encode_layer0_neighbors(&[5, 9]);

        // Newest-to-oldest operands: add(5), remove(5) => final includes 5.
        let add_wins = vec![
            encode_edge_delta(EdgeDeltaOp::AddOut, 5),
            encode_edge_delta(EdgeDeltaOp::RemoveOut, 5),
        ];
        let result_add_wins = op
            .merge_batch(&key, Some(existing.clone()), &add_wins)
            .unwrap();
        let decoded_add_wins = decode_layer0_neighbors(&result_add_wins).unwrap();
        assert_eq!(decoded_add_wins, vec![5, 9]);

        // Newest-to-oldest operands: remove(5), add(5) => final excludes 5.
        let remove_wins = vec![
            encode_edge_delta(EdgeDeltaOp::RemoveOut, 5),
            encode_edge_delta(EdgeDeltaOp::AddOut, 5),
        ];
        let result_remove_wins = op.merge_batch(&key, Some(existing), &remove_wins).unwrap();
        let decoded_remove_wins = decode_layer0_neighbors(&result_remove_wins).unwrap();
        assert_eq!(decoded_remove_wins, vec![9]);
    }

    #[tokio::test]
    async fn test_slate_db_layer0_merge_batch_boundary_preserves_reset() {
        let object_store = Arc::new(InMemory::new());
        let db = slatedb::Db::builder("/tmp/test_layer0_merge_batch_boundary", object_store)
            .with_merge_operator(Arc::new(HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
        let mut key = vec![0; 18];
        key[0] = 0xF0;
        key[9] = 0x16;
        key[17] = 1;
        let key = Bytes::from(key);

        db.put(&key, encode_layer0_neighbors(&[5, 9]))
            .await
            .unwrap();

        // Cross SlateDB's 100-operand merge batch boundary before applying a reset.
        for neighbor in 1000..1099 {
            db.merge(&key, encode_edge_delta(EdgeDeltaOp::AddOut, neighbor))
                .await
                .unwrap();
        }
        db.merge(&key, encode_edge_reset_out()).await.unwrap();
        db.merge(&key, encode_edge_delta(EdgeDeltaOp::AddOut, 7))
            .await
            .unwrap();

        let value = db.get(&key).await.unwrap().unwrap();
        let neighbors = decode_layer0_neighbors(&value).unwrap();
        assert_eq!(neighbors, vec![7]);
    }

    #[test]
    fn test_layer0_snapshot_replaces_existing_neighbors() {
        let op = Layer0NeighborMergeOperator::new();
        let key = Bytes::from_static(b"l0");

        let existing = encode_layer0_neighbors(&[1, 2, 3]);
        let snapshot = encode_layer0_neighbors(&[2, 4]);

        let result = op.merge(&key, Some(existing), snapshot).unwrap();
        let decoded = decode_layer0_neighbors(&result).unwrap();
        assert_eq!(decoded, vec![2, 4]);
    }

    #[test]
    fn test_layer0_simhash_operand_sets_and_preserves_through_neighbor_deltas() {
        let op = Layer0NeighborMergeOperator::new();
        let key = Bytes::from_static(b"l0");

        let with_hash = op
            .merge(&key, None, encode_layer0_set_simhash(0xAABBCCDDEEFF0011))
            .unwrap();
        let with_neighbor = op
            .merge(
                &key,
                Some(with_hash),
                encode_edge_delta(EdgeDeltaOp::AddOut, 42),
            )
            .unwrap();

        let (neighbors, simhash_bits) =
            decode_layer0_neighbors_and_simhash(&with_neighbor).unwrap();
        assert_eq!(neighbors, vec![42]);
        assert_eq!(simhash_bits, Some(0xAABBCCDDEEFF0011));
    }

    #[test]
    fn test_layer0_simhash_clear_operand() {
        let op = Layer0NeighborMergeOperator::new();
        let key = Bytes::from_static(b"l0");

        let base = encode_layer0_record(&[7], Some(0x0102));
        let cleared = op
            .merge(&key, Some(base), encode_layer0_clear_simhash())
            .unwrap();

        let (neighbors, simhash_bits) = decode_layer0_neighbors_and_simhash(&cleared).unwrap();
        assert_eq!(neighbors, vec![7]);
        assert_eq!(simhash_bits, None);
    }

    // =========================================================================
    // Counter Merge Operator Tests
    // =========================================================================

    #[test]
    fn test_counter_merge_increment() {
        let op = CounterMergeOperator::new();
        let key = Bytes::from(vec![0xFF, b'c', b'n', b't']);

        // Start from nothing, add 5
        let result = op.merge(&key, None, encode_counter_delta(5)).unwrap();
        assert_eq!(CounterMergeOperator::decode(&result), 5);

        // Add 3 more
        let result = op
            .merge(&key, Some(result), encode_counter_delta(3))
            .unwrap();
        assert_eq!(CounterMergeOperator::decode(&result), 8);
    }

    #[test]
    fn test_counter_merge_decrement() {
        let op = CounterMergeOperator::new();
        let key = Bytes::from(vec![0xFF, b'c', b'n', b't']);

        // Start with 10
        let initial = CounterMergeOperator::encode(10);

        // Subtract 3
        let result = op
            .merge(&key, Some(initial), encode_counter_delta(-3))
            .unwrap();
        assert_eq!(CounterMergeOperator::decode(&result), 7);
    }

    #[test]
    fn test_counter_merge_clamp_to_zero() {
        let op = CounterMergeOperator::new();
        let key = Bytes::from(vec![0xFF, b'c', b'n', b't']);

        // Start with 5, subtract 10 -> should clamp to 0
        let initial = CounterMergeOperator::encode(5);
        let result = op
            .merge(&key, Some(initial), encode_counter_delta(-10))
            .unwrap();
        assert_eq!(CounterMergeOperator::decode(&result), 0);
    }

    #[test]
    fn test_counter_merge_batch() {
        let op = CounterMergeOperator::new();
        let key = Bytes::from(vec![0xFF, b'c', b'n', b't']);

        let operands = vec![
            encode_counter_delta(5),
            encode_counter_delta(3),
            encode_counter_delta(-2),
            encode_counter_delta(10),
        ];

        // 0 + 5 + 3 - 2 + 10 = 16
        let result = op.merge_batch(&key, None, &operands).unwrap();
        assert_eq!(CounterMergeOperator::decode(&result), 16);
    }

    // =========================================================================
    // Combined Helix Merge Operator Tests
    // =========================================================================

    #[test]
    fn test_helix_merge_dispatches_to_edge() {
        let op = HelixMergeOperator::new();
        let key = Bytes::from(vec![0x00, 0, 0, 0, 0, 0, 0, 0, 1]); // Adjacency key

        let delta = encode_edge_delta(EdgeDeltaOp::AddOut, 42);
        let result = op.merge(&key, None, delta).unwrap();

        let edges = decode_edges(&result).unwrap();
        assert!(edges.contains_out(42));
    }

    #[test]
    fn test_helix_merge_dispatches_hnsw_layer0_key_to_layer0_codec() {
        let op = HelixMergeOperator::new();
        let key = Bytes::from(vec![
            0xF0, // vector-hot keyspace prefix
            0, 0, 0, 0, 0, 0, 0, 1,    // index_id
            0x16, // kind=l0_vec_ks
            0, 0, 0, 0, 0, 0, 0, 7, // node_id
        ]);

        let delta = encode_edge_delta(EdgeDeltaOp::AddOut, 42);
        let result = op.merge(&key, None, delta).unwrap();

        let neighbors = decode_layer0_neighbors(&result).unwrap();
        assert_eq!(neighbors, vec![42]);
    }

    #[test]
    fn test_helix_merge_does_not_treat_property_index_as_edge() {
        let op = HelixMergeOperator::new();
        // Property-index key prefix [0x03][0x03] but not HNSW l0 shape (length/kind mismatch).
        let key = Bytes::from(vec![
            0x03, 0x03, 0x00, // keyspace, edge-range index type, out direction
            0, 0, 0, 0, 0, 0, 0, 1, // source
            0, 0, 0, 2, // prop hash
            b'1', b'2', b'3', // value bytes
            0, 0, 0, 0, 0, 0, 0, 9, // edge id suffix
        ]);

        let value = Bytes::from_static(b"property-index-value");
        let result = op.merge(&key, None, value.clone()).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_helix_merge_dispatches_to_counter() {
        let op = HelixMergeOperator::new();
        let key = Bytes::from(vec![0xFF, b'c', b'n', b't']); // Metadata key

        let delta = encode_counter_delta(5);
        let result = op.merge(&key, None, delta).unwrap();

        assert_eq!(CounterMergeOperator::decode(&result), 5);
    }

    #[test]
    fn test_helix_merge_other_keys() {
        let op = HelixMergeOperator::new();
        let key = Bytes::from(vec![0x02, 0, 0, 0, 0, 0, 0, 0, 1]); // Node property key

        let value = Bytes::from("some data");
        let result = op.merge(&key, None, value.clone()).unwrap();

        // Last-writer-wins for unknown key types
        assert_eq!(result, value);
    }
}
