//! SlateDB merge operator for Helix-owned keyspaces.

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::{MergeOperator, MergeOperatorError};

use crate::encoding::keys::tenant::DataScope;
use crate::encoding::v1::keys::vectors::{KEY_KIND_LAYER0_VEC_KS, VECTOR_HOT_KEYSPACE_PREFIX};
use crate::encoding::v1::keys::{DataKeyKind, KeyPrefix};
use crate::encoding::v1::values::{edges, vectors};
use crate::encoding::v2::keys::{Key as V2Key, ScopedKey as V2ScopedKey};
use crate::encoding::v2::values::SecondaryEqualityBitmapValue;
use crate::encoding::NodeId;

const EDGE_DELTA_MIN_LEN: usize = core::mem::size_of::<u8>();
const EDGE_DELTA_NODE_LEN: usize = core::mem::size_of::<u8>() + core::mem::size_of::<NodeId>();
const LAYER0_SIMHASH_LEN: usize = core::mem::size_of::<u8>() + core::mem::size_of::<u64>();
const ADJACENCY_PREFIX: u8 = KeyPrefix::Adjacency.as_u8();
const PROPERTY_INDEX_PREFIX: u8 = KeyPrefix::PropertyIndex.as_u8();
const EDGE_PAIR_INDEX_PREFIX: u8 = KeyPrefix::EdgePairIndex.as_u8();
const METADATA_PREFIX: u8 = KeyPrefix::Metadata.as_u8();
const VECTOR_INDEX_ID_LEN: usize = core::mem::size_of::<u64>();
const VECTOR_KIND_OFFSET: usize = core::mem::size_of::<u8>() + VECTOR_INDEX_ID_LEN;
const VECTOR_LAYER0_KEY_LEN: usize = core::mem::size_of::<u8>()
    + VECTOR_INDEX_ID_LEN
    + core::mem::size_of::<u8>()
    + size_of::<NodeId>();
const BITMAP_DELTA_MAGIC: &[u8; 8] = b"HLXRBM1\0";
const BITMAP_DELTA_OP_LEN: usize = core::mem::size_of::<u8>();
const BITMAP_DELTA_ID_LEN: usize = core::mem::size_of::<u64>();
const BITMAP_DELTA_LEN: usize =
    BITMAP_DELTA_MAGIC.len() + BITMAP_DELTA_OP_LEN + BITMAP_DELTA_ID_LEN;
const BITMAP_ADD: u8 = 0x00;

pub(crate) fn encode_bitmap_add(id: u64) -> Bytes {
    let mut bytes = Vec::with_capacity(BITMAP_DELTA_LEN);
    bytes.extend_from_slice(BITMAP_DELTA_MAGIC);
    bytes.push(BITMAP_ADD);
    bytes.extend_from_slice(&id.to_be_bytes());
    Bytes::from(bytes)
}

fn decode_bitmap_add(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != BITMAP_DELTA_LEN
        || &bytes[0..BITMAP_DELTA_MAGIC.len()] != BITMAP_DELTA_MAGIC
        || bytes[BITMAP_DELTA_MAGIC.len()] != BITMAP_ADD
    {
        return None;
    }
    Some(u64::from_be_bytes(
        bytes[BITMAP_DELTA_MAGIC.len() + BITMAP_DELTA_OP_LEN
            ..BITMAP_DELTA_MAGIC.len() + BITMAP_DELTA_OP_LEN + BITMAP_DELTA_ID_LEN]
            .try_into()
            .expect("validated bitmap delta has exactly eight id bytes"),
    ))
}

#[derive(Debug, Clone, Default)]
struct BitmapMergeOperator;

impl BitmapMergeOperator {
    fn apply(bitmap: &mut RoaringTreemap, bytes: &[u8]) -> Result<(), MergeOperatorError> {
        if let Some(id) = decode_bitmap_add(bytes) {
            bitmap.insert(id);
            return Ok(());
        }
        let decoded = SecondaryEqualityBitmapValue::decode(bytes).map_err(merge_decode_error)?;
        *bitmap |= decoded.ids();
        Ok(())
    }

    fn encode(bitmap: &RoaringTreemap) -> Result<Bytes, MergeOperatorError> {
        let mut bytes = Vec::new();
        bitmap
            .serialize_into(&mut bytes)
            .map_err(merge_decode_error)?;
        Ok(Bytes::from(bytes))
    }
}

impl MergeOperator for BitmapMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut bitmap = RoaringTreemap::new();
        if let Some(existing) = existing_value {
            Self::apply(&mut bitmap, &existing)?;
        }
        if let Some(id) = decode_bitmap_add(&operand) {
            bitmap.insert(id);
        } else {
            Self::apply(&mut bitmap, &operand)?;
        }
        Self::encode(&bitmap)
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut bitmap = RoaringTreemap::new();
        if let Some(existing) = existing_value {
            Self::apply(&mut bitmap, &existing)?;
        }
        for operand in operands.iter().rev() {
            Self::apply(&mut bitmap, operand)?;
        }
        Self::encode(&bitmap)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EdgeDeltaOp {
    AddOut,
    RemoveOut,
    AddIn,
    RemoveIn,
    ResetOut,
}

impl EdgeDeltaOp {
    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x00 => Some(Self::AddOut),
            0x01 => Some(Self::RemoveOut),
            0x02 => Some(Self::AddIn),
            0x03 => Some(Self::RemoveIn),
            0x04 => Some(Self::ResetOut),
            _ => None,
        }
    }
}

fn decode_edge_delta(data: &[u8]) -> Option<(EdgeDeltaOp, NodeId)> {
    if data.len() < EDGE_DELTA_MIN_LEN {
        return None;
    }
    let op = EdgeDeltaOp::from_byte(data[0])?;
    if op == EdgeDeltaOp::ResetOut {
        return Some((op, 0));
    }
    if data.len() < EDGE_DELTA_NODE_LEN {
        return None;
    }
    Some((
        op,
        NodeId::from_be_bytes(
            data[EDGE_DELTA_MIN_LEN..EDGE_DELTA_MIN_LEN + core::mem::size_of::<NodeId>()]
                .try_into()
                .expect("edge delta node id slice is 8 bytes"),
        ),
    ))
}

fn is_edge_delta(data: &[u8]) -> bool {
    data.first()
        .is_some_and(|byte| EdgeDeltaOp::from_byte(*byte).is_some())
}

#[derive(Debug, Clone, Default)]
struct EdgeMergeOperator;

impl EdgeMergeOperator {
    fn apply_delta(edges: &mut edges::Edges, op: EdgeDeltaOp, node_id: NodeId) {
        match op {
            EdgeDeltaOp::AddOut => edges.add_out(node_id),
            EdgeDeltaOp::RemoveOut => {
                edges.remove_out(node_id);
            }
            EdgeDeltaOp::AddIn => edges.add_in(node_id),
            EdgeDeltaOp::RemoveIn => {
                edges.remove_in(node_id);
            }
            EdgeDeltaOp::ResetOut => edges.nxts_out.clear(),
        }
    }

    fn apply_operand(edges: &mut edges::Edges, operand: &[u8]) {
        if let Some((op, node_id)) = decode_edge_delta(operand) {
            Self::apply_delta(edges, op, node_id);
            return;
        }
        if let Ok(other) = edges::decode_edges(operand) {
            edges.merge(&other);
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
        let mut merged = existing_value
            .as_deref()
            .filter(|value| !is_edge_delta(value))
            .map(edges::decode_edges)
            .transpose()
            .map_err(merge_decode_error)?
            .unwrap_or_default();
        if let Some(existing) = existing_value
            .as_deref()
            .filter(|value| is_edge_delta(value))
        {
            Self::apply_operand(&mut merged, existing);
        }
        Self::apply_operand(&mut merged, &operand);
        Ok(edges::encode_edges(&merged))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut merged = existing_value
            .as_deref()
            .filter(|value| !is_edge_delta(value))
            .map(edges::decode_edges)
            .transpose()
            .map_err(merge_decode_error)?
            .unwrap_or_default();
        if let Some(existing) = existing_value
            .as_deref()
            .filter(|value| is_edge_delta(value))
        {
            Self::apply_operand(&mut merged, existing);
        }
        for operand in operands.iter().rev() {
            Self::apply_operand(&mut merged, operand);
        }
        Ok(edges::encode_edges(&merged))
    }
}

const LAYER0_SIMHASH_SET: u8 = 0x05;
const LAYER0_SIMHASH_CLEAR: u8 = 0x06;

fn decode_layer0_simhash_operand(data: &[u8]) -> Option<Option<u64>> {
    match data.first().copied() {
        Some(LAYER0_SIMHASH_SET) if data.len() == LAYER0_SIMHASH_LEN => {
            Some(Some(u64::from_le_bytes(
                data[EDGE_DELTA_MIN_LEN..EDGE_DELTA_MIN_LEN + core::mem::size_of::<u64>()]
                    .try_into()
                    .expect("simhash operand slice is 8 bytes"),
            )))
        }
        Some(LAYER0_SIMHASH_CLEAR) if data.len() == EDGE_DELTA_MIN_LEN => Some(None),
        _ => None,
    }
}

#[derive(Debug, Clone, Default)]
struct Layer0State {
    neighbors: Vec<NodeId>,
    simhash_bits: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct Layer0NeighborMergeOperator;

impl Layer0NeighborMergeOperator {
    fn apply_delta(state: &mut Layer0State, op: EdgeDeltaOp, node_id: NodeId) {
        match op {
            EdgeDeltaOp::AddOut => {
                if let Err(index) = state.neighbors.binary_search(&node_id) {
                    state.neighbors.insert(index, node_id);
                }
            }
            EdgeDeltaOp::RemoveOut => {
                if let Ok(index) = state.neighbors.binary_search(&node_id) {
                    state.neighbors.remove(index);
                }
            }
            EdgeDeltaOp::ResetOut => state.neighbors.clear(),
            EdgeDeltaOp::AddIn | EdgeDeltaOp::RemoveIn => {}
        }
    }

    fn apply_operand(state: &mut Layer0State, operand: &[u8]) -> Result<(), MergeOperatorError> {
        if let Some(simhash_bits) = decode_layer0_simhash_operand(operand) {
            state.simhash_bits = simhash_bits;
            return Ok(());
        }
        if let Some((op, node_id)) = decode_edge_delta(operand) {
            Self::apply_delta(state, op, node_id);
            return Ok(());
        }

        let (neighbors, simhash_bits) =
            vectors::decode_layer0_neighbors_and_simhash(operand).map_err(merge_decode_error)?;
        state.neighbors = neighbors;
        if operand.first().copied() == Some(vectors::ENCODING_TYPE_LAYER0_RECORD) {
            state.simhash_bits = simhash_bits;
        }
        Ok(())
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
        if let Some(existing) = existing_value {
            Self::apply_operand(&mut state, &existing)?;
        }
        Self::apply_operand(&mut state, &operand)?;
        Ok(vectors::encode_layer0_record(
            &state.neighbors,
            state.simhash_bits,
        ))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut state = Layer0State::default();
        if let Some(existing) = existing_value {
            Self::apply_operand(&mut state, &existing)?;
        }
        for operand in operands.iter().rev() {
            Self::apply_operand(&mut state, operand)?;
        }
        Ok(vectors::encode_layer0_record(
            &state.neighbors,
            state.simhash_bits,
        ))
    }
}

#[derive(Debug, Clone, Default)]
struct CounterMergeOperator;

impl CounterMergeOperator {
    fn decode(data: &[u8]) -> i64 {
        data.get(0..core::mem::size_of::<i64>())
            .and_then(|bytes| bytes.try_into().ok())
            .map(i64::from_be_bytes)
            .unwrap_or_default()
    }

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
        let existing = existing_value
            .as_deref()
            .map(Self::decode)
            .unwrap_or_default();
        let delta = Self::decode(&operand);
        Ok(Self::encode((existing + delta).max(0)))
    }

    fn merge_batch(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let mut total = existing_value
            .as_deref()
            .map(Self::decode)
            .unwrap_or_default();
        for operand in operands {
            total += Self::decode(operand);
        }
        Ok(Self::encode(total.max(0)))
    }
}

/// Combined merge operator for durable Helix storage keyspaces.
#[derive(Debug, Clone, Default)]
pub(crate) struct HelixMergeOperator {
    edge: EdgeMergeOperator,
    bitmap: BitmapMergeOperator,
    layer0: Layer0NeighborMergeOperator,
    counter: CounterMergeOperator,
}

impl HelixMergeOperator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn is_hnsw_layer0_key(key: &[u8]) -> bool {
        key.len() == VECTOR_LAYER0_KEY_LEN
            && key[0] == VECTOR_HOT_KEYSPACE_PREFIX
            && key[VECTOR_KIND_OFFSET] == KEY_KIND_LAYER0_VEC_KS
    }

    fn key_type(key: &[u8]) -> MergeKeyType {
        if is_v4_secondary_equality_bitmap_key(key) {
            return MergeKeyType::Bitmap;
        }
        let logical = logical_key(key);
        if Self::is_hnsw_layer0_key(logical) {
            return MergeKeyType::Layer0;
        }
        match logical.first().copied() {
            Some(prefix) if prefix == KeyPrefix::Adjacency.as_u8() => MergeKeyType::Edge,
            Some(prefix) if prefix == KeyPrefix::EdgePairIndex.as_u8() => MergeKeyType::Bitmap,
            Some(prefix)
                if prefix == KeyPrefix::PropertyIndex.as_u8()
                    && matches!(
                        DataKeyKind::parse_from_slice(logical),
                        Ok(DataKeyKind::PropertyIndex(_))
                    ) =>
            {
                MergeKeyType::Bitmap
            }
            Some(prefix) if prefix == KeyPrefix::Metadata.as_u8() => MergeKeyType::Counter,
            _ => MergeKeyType::Other,
        }
    }
}

fn is_v4_secondary_equality_bitmap_key(key: &[u8]) -> bool {
    matches!(
        V2Key::parse_data_from_slice(key),
        Ok(V2Key::Data {
            kind: V2ScopedKey::SecondaryEqualityBitmap(_),
            ..
        })
    )
}

impl MergeOperator for HelixMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        match Self::key_type(key) {
            MergeKeyType::Edge => self.edge.merge(key, existing_value, operand),
            MergeKeyType::Bitmap => self.bitmap.merge(key, existing_value, operand),
            MergeKeyType::Layer0 => self.layer0.merge(key, existing_value, operand),
            MergeKeyType::Counter => self.counter.merge(key, existing_value, operand),
            MergeKeyType::Other => Ok(operand),
        }
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        match Self::key_type(key) {
            MergeKeyType::Edge => self.edge.merge_batch(key, existing_value, operands),
            MergeKeyType::Bitmap => self.bitmap.merge_batch(key, existing_value, operands),
            MergeKeyType::Layer0 => self.layer0.merge_batch(key, existing_value, operands),
            MergeKeyType::Counter => self.counter.merge_batch(key, existing_value, operands),
            MergeKeyType::Other => operands
                .first()
                .cloned()
                .or(existing_value)
                .ok_or(MergeOperatorError::EmptyBatch),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeKeyType {
    Edge,
    Bitmap,
    Layer0,
    Counter,
    Other,
}

fn logical_key(key: &[u8]) -> &[u8] {
    if matches!(
        key.first().copied(),
        Some(METADATA_PREFIX | VECTOR_HOT_KEYSPACE_PREFIX)
    ) || key.len() == core::mem::size_of::<u8>() + core::mem::size_of::<NodeId>()
        && key.first().copied() == Some(ADJACENCY_PREFIX)
        || key.len()
            == core::mem::size_of::<u8>()
                + core::mem::size_of::<NodeId>()
                + core::mem::size_of::<NodeId>()
            && key.first().copied() == Some(EDGE_PAIR_INDEX_PREFIX)
        || key.first().copied() == Some(PROPERTY_INDEX_PREFIX)
            && matches!(
                DataKeyKind::parse_from_slice(key),
                Ok(DataKeyKind::PropertyIndex(_))
            )
    {
        return key;
    }

    if let Some((_, tenant_logical)) = DataScope::strip_tenant_envelope(key)
        && matches!(
            tenant_logical.first().copied(),
            Some(
                ADJACENCY_PREFIX
                    | METADATA_PREFIX
                    | VECTOR_HOT_KEYSPACE_PREFIX
                    | PROPERTY_INDEX_PREFIX
                    | EDGE_PAIR_INDEX_PREFIX
            )
        )
    {
        return tenant_logical;
    }
    key
}

fn merge_decode_error(error: impl std::fmt::Display) -> MergeOperatorError {
    MergeOperatorError::Callback {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::indexes::range::RangeIndexDirection;
    use crate::encoding::v1::keys::tenant::{DataScope, TenantId};
    use crate::encoding::v1::keys::{
        AdjacencyKey, DataKeyKind, EdgePairIndexKey, Key, NodePropertyKey,
    };
    use crate::encoding::v1::property::equality_value::{
        project_equality_value, EqualityValueProjection,
    };
    use crate::encoding::v2::keys::{
        CanonicalSecondaryValue, PartitionFingerprint, SecondaryEntryKey, SecondaryEntryLane,
        SecondaryEqualityBitmapKey, TextManifestRootKey, VectorPartitionMappingKey,
    };
    use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};

    fn edge_delta(op: u8, node_id: NodeId) -> Bytes {
        let mut bytes = vec![op];
        bytes.extend_from_slice(&node_id.to_be_bytes());
        Bytes::from(bytes)
    }

    fn v4_bitmap_key(scope: DataScope) -> Bytes {
        let EqualityValueProjection::Indexed(value) = project_equality_value(
            &crate::encoding::property::property_value::PropertyValue::String("shared".to_string()),
        ) else {
            panic!("string equality value is indexable");
        };
        V2Key::Data {
            scope,
            kind: V2ScopedKey::SecondaryEqualityBitmap(
                SecondaryEqualityBitmapKey::try_new(
                    IndexId::new(7).unwrap(),
                    IndexGenerationId::new(9).unwrap(),
                    IndexElementKind::Node,
                    value,
                )
                .unwrap(),
            ),
        }
        .to_bytes()
    }

    fn portable_bitmap(ids: impl IntoIterator<Item = u64>) -> Bytes {
        BitmapMergeOperator::encode(&RoaringTreemap::from_iter(ids))
            .expect("bitmap fixture encodes")
    }

    fn decode_bitmap(bytes: Bytes) -> Vec<u64> {
        RoaringTreemap::deserialize_from(Cursor::new(bytes))
            .expect("merged bitmap decodes")
            .iter()
            .collect()
    }

    #[test]
    fn edge_merge_batch_applies_oldest_to_newest() {
        let key = Key::Data {
            scope: crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
            kind: DataKeyKind::Adjacency(AdjacencyKey::new(1)),
        }
        .to_bytes();
        let operator = HelixMergeOperator::new();
        let merged = operator
            .merge_batch(
                &key,
                None,
                &[
                    edge_delta(0x00, 7),
                    edge_delta(0x01, 5),
                    edge_delta(0x00, 5),
                ],
            )
            .expect("merge succeeds");
        let edges = edges::decode_edges(&merged).expect("edges decode");
        assert_eq!(edges.iter_out().collect::<Vec<_>>(), vec![7]);
    }

    #[test]
    fn bitmap_merge_preserves_base_and_deduplicates_additions() {
        let key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(1, 3)),
        }
        .to_bytes();
        let mut base = RoaringTreemap::new();
        base.insert(41);
        let base = BitmapMergeOperator::encode(&base).expect("base bitmap encodes");
        let operator = HelixMergeOperator::new();

        let merged = operator
            .merge_batch(
                &key,
                None,
                &[base, encode_bitmap_add(42), encode_bitmap_add(42)],
            )
            .expect("bitmap operands merge");
        let bitmap =
            RoaringTreemap::deserialize_from(Cursor::new(merged)).expect("merged bitmap decodes");

        assert_eq!(bitmap.iter().collect::<Vec<_>>(), vec![41, 42]);
    }

    #[test]
    fn v4_bitmap_merge_covers_absent_existing_single_multiple_and_ordering() {
        let key = v4_bitmap_key(DataScope::LegacyUnscoped);
        let operator = HelixMergeOperator::new();

        let absent = operator
            .merge(&key, None, encode_bitmap_add(8))
            .expect("absent-base bitmap merge succeeds");
        assert_eq!(decode_bitmap(absent), vec![8]);

        let existing = operator
            .merge(&key, Some(portable_bitmap([1, 8])), encode_bitmap_add(13))
            .expect("existing-base bitmap merge succeeds");
        assert_eq!(decode_bitmap(existing), vec![1, 8, 13]);

        let operands = [
            encode_bitmap_add(21),
            portable_bitmap([3, 5]),
            encode_bitmap_add(3),
            encode_bitmap_add(21),
        ];
        let forward = operator
            .merge_batch(&key, Some(portable_bitmap([1, 3])), &operands)
            .expect("multi-operand bitmap merge succeeds");
        let mut reversed = operands;
        reversed.reverse();
        let reverse = operator
            .merge_batch(&key, Some(portable_bitmap([1, 3])), &reversed)
            .expect("reordered bitmap merge succeeds");
        assert_eq!(decode_bitmap(forward), vec![1, 3, 5, 21]);
        assert_eq!(decode_bitmap(reverse), vec![1, 3, 5, 21]);
    }

    #[test]
    fn v4_bitmap_merge_rejects_malformed_bases_and_operands() {
        let key = v4_bitmap_key(DataScope::LegacyUnscoped);
        let operator = HelixMergeOperator::new();
        assert!(operator
            .merge(
                &key,
                Some(Bytes::from_static(b"not-roaring")),
                encode_bitmap_add(1)
            )
            .is_err());
        assert!(operator
            .merge(&key, Some(portable_bitmap([1])), Bytes::from_static(b"bad"))
            .is_err());
        let mut malformed_delta = encode_bitmap_add(1).to_vec();
        malformed_delta[BITMAP_DELTA_MAGIC.len()] = 0xFF;
        assert!(operator
            .merge_batch(
                &key,
                Some(portable_bitmap([1])),
                &[Bytes::from(malformed_delta)],
            )
            .is_err());

        let mut trailing_base = portable_bitmap([1]).to_vec();
        trailing_base.push(0xFF);
        assert!(operator
            .merge(
                &key,
                Some(Bytes::from(trailing_base.clone())),
                encode_bitmap_add(2),
            )
            .is_err());
        assert!(operator
            .merge_batch(
                &key,
                Some(Bytes::from(trailing_base)),
                &[encode_bitmap_add(2)],
            )
            .is_err());

        let mut trailing_operand = portable_bitmap([2]).to_vec();
        trailing_operand.push(0xFF);
        assert!(operator
            .merge(
                &key,
                Some(portable_bitmap([1])),
                Bytes::from(trailing_operand.clone()),
            )
            .is_err());
        assert!(operator
            .merge_batch(
                &key,
                Some(portable_bitmap([1])),
                &[Bytes::from(trailing_operand)],
            )
            .is_err());
    }

    #[test]
    fn v4_secondary_equality_bitmap_uses_typed_bitmap_merge_for_each_scope() {
        let EqualityValueProjection::Indexed(value) = project_equality_value(
            &crate::encoding::property::property_value::PropertyValue::String("shared".to_string()),
        ) else {
            panic!("string equality value is indexable");
        };
        let kind = V2ScopedKey::SecondaryEqualityBitmap(
            SecondaryEqualityBitmapKey::try_new(
                IndexId::new(7).unwrap(),
                IndexGenerationId::new(9).unwrap(),
                IndexElementKind::Node,
                value,
            )
            .unwrap(),
        );
        for scope in [
            DataScope::LegacyUnscoped,
            DataScope::Tenant(TenantId::from_u128(
                0x0600_0000_0000_0000_0000_0000_0000_0000,
            )),
            DataScope::Tenant(TenantId::from_u128(
                0x0601_0000_0000_0000_0000_0000_0000_0000,
            )),
            DataScope::Tenant(TenantId::from_u128(
                0xFD00_0000_0000_0000_0000_0000_0000_0000,
            )),
        ] {
            let key = V2Key::Data {
                scope,
                kind: kind.clone(),
            }
            .to_bytes();
            if matches!(scope, DataScope::Tenant(_)) {
                assert_eq!(key.first().copied(), Some(0xFD));
            }
            let merged = HelixMergeOperator::new()
                .merge_batch(&key, None, &[encode_bitmap_add(5), encode_bitmap_add(8)])
                .unwrap();
            let bitmap = RoaringTreemap::deserialize_from(Cursor::new(merged)).unwrap();
            assert_eq!(bitmap, RoaringTreemap::from_iter([5, 8]));
        }
    }

    #[test]
    fn only_typed_v4_equality_bitmap_keys_use_the_new_bitmap_dispatch() {
        let index_id = IndexId::new(7).unwrap();
        let generation = IndexGenerationId::new(9).unwrap();
        let v3_equality = SecondaryEntryKey::try_new(
            index_id,
            generation,
            SecondaryEntryLane::NodeEquality,
            CanonicalSecondaryValue::equality_string("shared"),
            Some(IndexEntityId::new(11)),
        )
        .unwrap();
        let v3_range = SecondaryEntryKey::try_new(
            index_id,
            generation,
            SecondaryEntryLane::NodeRangeAscending,
            CanonicalSecondaryValue::range_string(RangeIndexDirection::Asc, "shared"),
            Some(IndexEntityId::new(11)),
        )
        .unwrap();
        let negative_keys = [
            V2Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: V2ScopedKey::SecondaryEntry(v3_equality),
            }
            .to_bytes(),
            V2Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: V2ScopedKey::SecondaryEntry(v3_range),
            }
            .to_bytes(),
            V2Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: V2ScopedKey::TextManifestRoot(TextManifestRootKey {
                    index_id,
                    generation,
                    partition: PartitionFingerprint::new([0x22; 32]),
                }),
            }
            .to_bytes(),
            V2Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: V2ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
                    index_id,
                    generation,
                    partition: PartitionFingerprint::new([0x33; 32]),
                }),
            }
            .to_bytes(),
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(11)),
            }
            .to_bytes(),
        ];
        for key in negative_keys {
            assert_ne!(HelixMergeOperator::key_type(&key), MergeKeyType::Bitmap);
        }

        let valid = v4_bitmap_key(DataScope::LegacyUnscoped);
        assert_eq!(HelixMergeOperator::key_type(&valid), MergeKeyType::Bitmap);
        for truncated_len in 0..valid.len() {
            assert_ne!(
                HelixMergeOperator::key_type(&valid[0..truncated_len]),
                MergeKeyType::Bitmap
            );
        }
    }

    #[tokio::test]
    async fn concurrent_v4_additions_survive_cold_reopen() {
        let store = Arc::new(InMemory::new());
        let path = "merge-operator-concurrent-v4-additions";
        let db = Arc::new(
            slatedb::Db::builder(path, store.clone())
                .with_merge_operator(Arc::new(HelixMergeOperator::new()))
                .build()
                .await
                .expect("concurrent bitmap database opens"),
        );
        let key = v4_bitmap_key(DataScope::Tenant(TenantId::from_u128(
            0xFD00_0000_0000_0000_0000_0000_0000_0042,
        )));
        let mut tasks = tokio::task::JoinSet::new();
        for id in 0..128 {
            let db = Arc::clone(&db);
            let key = key.clone();
            tasks.spawn(async move { db.merge(&key, encode_bitmap_add(id)).await });
        }
        while let Some(result) = tasks.join_next().await {
            result
                .expect("concurrent bitmap task joins")
                .expect("concurrent bitmap merge succeeds");
        }
        let expected = (0..128).collect::<Vec<_>>();
        assert_eq!(
            decode_bitmap(
                db.get(&key)
                    .await
                    .expect("concurrent bitmap reads")
                    .expect("concurrent bitmap exists")
            ),
            expected
        );
        db.close().await.expect("concurrent bitmap database closes");
        drop(db);

        let reopened = slatedb::Db::builder(path, store)
            .with_merge_operator(Arc::new(HelixMergeOperator::new()))
            .build()
            .await
            .expect("concurrent bitmap database reopens");
        assert_eq!(
            decode_bitmap(
                reopened
                    .get(&key)
                    .await
                    .expect("reopened bitmap reads")
                    .expect("reopened bitmap exists")
            ),
            expected
        );
        reopened
            .close()
            .await
            .expect("reopened bitmap database closes");
    }

    #[test]
    fn bitmap_add_operands_commute_for_every_existing_bitmap() {
        let key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(1, 3)),
        }
        .to_bytes();
        let mut existing = RoaringTreemap::new();
        existing.insert(41);
        let existing = BitmapMergeOperator::encode(&existing).expect("existing bitmap encodes");
        let operator = HelixMergeOperator::new();

        let left_then_right = operator
            .merge(&key, Some(existing.clone()), encode_bitmap_add(42))
            .and_then(|value| operator.merge(&key, Some(value), encode_bitmap_add(43)))
            .expect("left-then-right bitmap operands merge");
        let right_then_left = operator
            .merge(&key, Some(existing), encode_bitmap_add(43))
            .and_then(|value| operator.merge(&key, Some(value), encode_bitmap_add(42)))
            .expect("right-then-left bitmap operands merge");

        let left_then_right = RoaringTreemap::deserialize_from(Cursor::new(left_then_right))
            .expect("left-then-right bitmap decodes");
        let right_then_left = RoaringTreemap::deserialize_from(Cursor::new(right_then_left))
            .expect("right-then-left bitmap decodes");
        assert_eq!(left_then_right, right_then_left);
        assert_eq!(left_then_right.iter().collect::<Vec<_>>(), vec![41, 42, 43],);
    }

    #[test]
    fn layer0_merge_preserves_simhash_across_neighbor_delta() {
        let mut key = vec![VECTOR_HOT_KEYSPACE_PREFIX];
        key.extend_from_slice(&9_u64.to_be_bytes());
        key.push(KEY_KIND_LAYER0_VEC_KS);
        key.extend_from_slice(&3_u64.to_be_bytes());
        let operator = HelixMergeOperator::new();
        let mut simhash = vec![LAYER0_SIMHASH_SET];
        simhash.extend_from_slice(&0x0102_0304_0506_0708_u64.to_le_bytes());
        let merged = operator
            .merge_batch(
                &Bytes::from(key),
                None,
                &[edge_delta(0x00, 7), Bytes::from(simhash)],
            )
            .expect("merge succeeds");
        assert_eq!(
            vectors::decode_layer0_neighbors_and_simhash(&merged).expect("layer0 decodes"),
            (vec![7], Some(0x0102_0304_0506_0708))
        );
    }

    #[test]
    fn edge_delta_decoder_and_state_machine_cover_every_closed_operation() {
        assert_eq!(decode_edge_delta(&[]), None);
        assert_eq!(decode_edge_delta(&[0xFF]), None);
        assert_eq!(decode_edge_delta(&[0x00]), None);
        assert_eq!(decode_edge_delta(&[0x04]), Some((EdgeDeltaOp::ResetOut, 0)));
        assert!(is_edge_delta(&[0x00]));
        assert!(!is_edge_delta(&[]));

        let mut state = edges::Edges::new();
        for (operation, node_id) in [
            (EdgeDeltaOp::AddOut, 11),
            (EdgeDeltaOp::AddIn, 12),
            (EdgeDeltaOp::RemoveOut, 11),
            (EdgeDeltaOp::RemoveIn, 12),
        ] {
            EdgeMergeOperator::apply_delta(&mut state, operation, node_id);
        }
        assert_eq!(state.num_edges_out(), 0);
        assert_eq!(state.num_edges_in(), 0);

        state.add_out(13);
        EdgeMergeOperator::apply_delta(&mut state, EdgeDeltaOp::ResetOut, 0);
        assert_eq!(state.num_edges_out(), 0);

        let mut encoded = edges::Edges::new();
        encoded.add_out(17);
        encoded.add_in(19);
        EdgeMergeOperator::apply_operand(&mut state, &edges::encode_edges(&encoded));
        EdgeMergeOperator::apply_operand(&mut state, b"not-an-edge-value");
        assert!(state.contains_out(17));
        assert!(state.contains_in(19));
    }

    #[test]
    fn edge_merge_covers_existing_delta_value_and_decode_failure_contracts() {
        let key = Bytes::from_static(b"edge");
        let operator = EdgeMergeOperator;

        let merged = operator
            .merge(&key, Some(edge_delta(0x00, 5)), edge_delta(0x02, 7))
            .expect("delta-valued existing state is applied");
        let decoded = edges::decode_edges(&merged).expect("merged edges decode");
        assert!(decoded.contains_out(5));
        assert!(decoded.contains_in(7));

        let batch = operator
            .merge_batch(
                &key,
                Some(edge_delta(0x00, 3)),
                &[edge_delta(0x04, 0), edge_delta(0x00, 9)],
            )
            .expect("batch deltas merge from oldest to newest");
        let decoded = edges::decode_edges(&batch).expect("batch edges decode");
        assert_eq!(decoded.iter_out().collect::<Vec<_>>(), Vec::<u64>::new());

        assert!(operator
            .merge(
                &key,
                Some(Bytes::from_static(b"malformed-base")),
                edge_delta(0x00, 1),
            )
            .is_err());
        assert!(operator
            .merge_batch(
                &key,
                Some(Bytes::from_static(b"malformed-base")),
                &[edge_delta(0x00, 1)],
            )
            .is_err());
    }

    #[test]
    fn layer_zero_operand_contract_covers_clear_reset_deduplication_and_failures() {
        let bits = 0xABCD_EF01_2345_6789_u64;
        let mut set = vec![LAYER0_SIMHASH_SET];
        set.extend_from_slice(&bits.to_le_bytes());
        assert_eq!(decode_layer0_simhash_operand(&set), Some(Some(bits)));
        assert_eq!(
            decode_layer0_simhash_operand(&[LAYER0_SIMHASH_CLEAR]),
            Some(None)
        );
        assert_eq!(decode_layer0_simhash_operand(&[LAYER0_SIMHASH_SET]), None);
        assert_eq!(
            decode_layer0_simhash_operand(&[LAYER0_SIMHASH_CLEAR, 0]),
            None
        );

        let mut state = Layer0State::default();
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::AddOut, 9);
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::AddOut, 9);
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::AddOut, 3);
        assert_eq!(state.neighbors, vec![3, 9]);
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::RemoveOut, 7);
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::RemoveOut, 3);
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::AddIn, 1);
        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::RemoveIn, 1);
        assert_eq!(state.neighbors, vec![9]);

        Layer0NeighborMergeOperator::apply_operand(&mut state, &set)
            .expect("SimHash set operand is valid");
        assert_eq!(state.simhash_bits, Some(bits));
        Layer0NeighborMergeOperator::apply_operand(&mut state, &[LAYER0_SIMHASH_CLEAR])
            .expect("SimHash clear operand is valid");
        assert_eq!(state.simhash_bits, None);

        Layer0NeighborMergeOperator::apply_operand(
            &mut state,
            &vectors::encode_layer0_neighbors(&[4, 2]),
        )
        .expect("legacy neighbor value is valid");
        assert_eq!(state.neighbors, vec![2, 4]);
        assert_eq!(state.simhash_bits, None);
        Layer0NeighborMergeOperator::apply_operand(
            &mut state,
            &vectors::encode_layer0_record(&[8], Some(bits)),
        )
        .expect("current layer-zero value is valid");
        assert_eq!(state.neighbors, vec![8]);
        assert_eq!(state.simhash_bits, Some(bits));

        Layer0NeighborMergeOperator::apply_delta(&mut state, EdgeDeltaOp::ResetOut, 0);
        assert!(state.neighbors.is_empty());
        assert!(
            Layer0NeighborMergeOperator::apply_operand(&mut state, b"malformed-layer-zero")
                .is_err()
        );
    }

    #[test]
    fn counter_other_and_dispatch_contracts_cover_every_merge_shape() {
        let counter = CounterMergeOperator;
        let key = Bytes::from_static(b"counter");
        assert_eq!(CounterMergeOperator::decode(b"short"), 0);
        assert_eq!(CounterMergeOperator::decode(&7_i64.to_be_bytes()), 7);
        assert_eq!(
            CounterMergeOperator::decode(
                &counter
                    .merge(
                        &key,
                        Some(CounterMergeOperator::encode(2)),
                        CounterMergeOperator::encode(-7),
                    )
                    .expect("counter merge succeeds")
            ),
            0
        );
        assert_eq!(
            CounterMergeOperator::decode(
                &counter
                    .merge_batch(
                        &key,
                        None,
                        &[
                            CounterMergeOperator::encode(3),
                            CounterMergeOperator::encode(4),
                        ],
                    )
                    .expect("counter batch succeeds")
            ),
            7
        );

        let operator = HelixMergeOperator::new();
        let other = Bytes::from_static(b"unowned-key");
        assert_eq!(
            operator
                .merge(
                    &other,
                    Some(Bytes::from_static(b"old")),
                    Bytes::from_static(b"new")
                )
                .expect("other merge returns the operand"),
            Bytes::from_static(b"new")
        );
        assert_eq!(
            operator
                .merge_batch(
                    &other,
                    Some(Bytes::from_static(b"old")),
                    &[Bytes::from_static(b"first"), Bytes::from_static(b"second")],
                )
                .expect("other batch returns its first operand"),
            Bytes::from_static(b"first")
        );
        assert_eq!(
            operator
                .merge_batch(&other, Some(Bytes::from_static(b"old")), &[])
                .expect("other empty batch preserves existing state"),
            Bytes::from_static(b"old")
        );
        assert!(operator.merge_batch(&other, None, &[]).is_err());

        assert_eq!(
            HelixMergeOperator::key_type(&[METADATA_PREFIX]),
            MergeKeyType::Counter
        );
        assert_eq!(HelixMergeOperator::key_type(&other), MergeKeyType::Other);
    }
}
