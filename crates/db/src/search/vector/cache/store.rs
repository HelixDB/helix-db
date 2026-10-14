//! Resident snapshot of HNSW upper-layer and SimHash rows.
//!
//! This cache is scoped to a single vector index (`index_id`) and stores only
//! upper-layer traversal data:
//! - upper neighbors (`kind=0x11`)
//! - simhash (`kind=0x12`)
//! - upper vectors (`kind=0x13`)
//!
//! Layer-0 neighbor rows intentionally remain on the normal DB/foyer path.
//! Upper rows are grouped node-first so deleting one entity never scans
//! unrelated cached nodes. Hydration validates, measures, and admits one row
//! at a time. A bounded partial store remains correct because absent rows fall
//! back to the caller's stable storage view without mutating the snapshot.
//!
//! # Resident layout
//!
//! Upper-row bytes are copied into slab chunks ([`VectorMemorySlab`])
//! instead of retaining the zero-copy scan values, which would keep whole
//! SlateDB block buffers alive. Each node keeps one compact index entry
//! ([`UpperNodeRows`]) holding `(chunk, offset, len)` locations for its
//! vector and its usually single upper-neighbor layer. Lookups return
//! [`Bytes`] slices of the owning chunk, so callers see the same bytes and
//! type as before.
//!
//! # Accounting
//!
//! A load charges the bytes the finished store keeps: every row's exact
//! length, a handle per slab chunk, and an upper bound on hash-index memory
//! per entry ([`hash_index_entry_bytes`]). A bounded load admits the longest
//! scan prefix whose charge fits, as if each row cost exactly its charge. The
//! open chunk's spare capacity (at most [`VECTOR_MEMORY_SLAB_CHUNK_BYTES`])
//! is a transient of the running load: sealing copies a partly filled chunk
//! into an exact-size allocation, so the spare bytes are freed under every
//! allocator. Evicted rows stay charged until the store is replaced, because
//! their chunk bytes are only released when the whole store drops. Allocator
//! size-class rounding and headers, and fixed per-store overhead independent
//! of row count (the hash maps' shard arrays and their smallest tables), are
//! not charged.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use slatedb::config::ScanOptions;
use tokio::sync::{watch, Mutex, MutexGuard};

use slatedb::DbReadOps;

#[cfg(feature = "production-coverage")]
use crate::encoding::error::EncodingError;
use crate::encoding::keys::{scope::DataScope, DataKey, DataKeyKind};
use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorMemoryPrefixKey};
#[cfg(test)]
use crate::encoding::v2::keys::indexes::vector::{
    VectorSimHashKey, VectorUpperNeighborsKey, VectorUpperVectorKey,
};
#[cfg(feature = "production-coverage")]
use crate::encoding::v2::values::indexes::vector::neighbors::encode_upper_neighbors;
use crate::encoding::v2::values::indexes::vector::simhash::decode_simhash;
#[cfg(test)]
use crate::encoding::v2::values::indexes::vector::simhash::encode_simhash;
use crate::encoding::NodeId;
use crate::error::HelixDbError;
use crate::search::vector::simhash::SimHash;
use crate::search::vector::storage::{SimHashRow, VectorRowKeyspace, VectorRows};

const VECTOR_MEMORY_LOAD_MAX_FETCH_TASKS: usize = 4;
/// Capacity of a slab chunk; a longer row gets a chunk of its own length.
///
/// Every lookup bumps its chunk's reference count, so chunks stay small
/// enough that concurrent searches spread over many counters; sealing trims
/// each chunk to its length, so the size costs no tail waste.
const VECTOR_MEMORY_SLAB_CHUNK_BYTES: usize = 16 * 1024;
/// Charged bytes for one chunk's handle: at most two table slots (buckets
/// double) and the `Bytes` shared header, rounded up to a 32-byte class.
const SLAB_CHUNK_HANDLE_BYTES: u64 = 2 * core::mem::size_of::<OnceLock<Bytes>>() as u64 + 32;
/// Charged index bytes for one SimHash entry.
const SIMHASH_INDEX_ENTRY_BYTES: u64 = hash_index_entry_bytes::<NodeId, SimHash>();
/// Charged index bytes for one node's upper-row entry.
const UPPER_NODE_INDEX_ENTRY_BYTES: u64 = hash_index_entry_bytes::<NodeId, UpperNodeRows>();
/// Heap bytes of one spilled upper-neighbor layer location.
const UPPER_LAYER_SLOT_BYTES: u64 = core::mem::size_of::<(u16, SlabRow)>() as u64;

/// Summary returned after hydrating a vector memory store.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct VectorMemoryStoreLoadSummary {
    /// Number of vector-hot rows loaded into the memory store.
    pub(crate) loaded_entries: usize,
    /// Approximate resident bytes admitted by the loaded rows.
    pub(crate) estimated_bytes: u64,
    /// Why hydration stopped after publishing the admitted rows.
    pub(crate) completion: VectorMemoryStoreLoadCompletion,
}

/// Terminal result of one bounded vector-memory hydration scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum VectorMemoryStoreLoadCompletion {
    /// The complete physical memory-row prefix was consumed.
    #[default]
    Complete,
    /// The next validated row would exceed the supplied admission budget.
    BudgetExhausted,
    /// Shutdown was observed before the complete prefix was consumed.
    Shutdown,
}

/// Maximum additional resident bytes one hydration may admit.
///
/// The bounded form intentionally accepts zero because fair-share planning can
/// assign no remaining capacity to an index without inventing a sentinel.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorMemoryAdmissionBudget {
    /// Admit every validated row in the physical prefix.
    Unbounded,
    /// Admit at most this many estimated resident bytes; zero is valid when a
    /// caller has already assigned the complete global budget elsewhere.
    Bounded(u64),
}

impl VectorMemoryAdmissionBudget {
    /// Returns the budget left after `charged` bytes were admitted.
    const fn remaining(self, charged: u64) -> Self {
        match self {
            Self::Unbounded => Self::Unbounded,
            Self::Bounded(bytes) => Self::Bounded(bytes.saturating_sub(charged)),
        }
    }

    /// Returns whether `bytes` more fit in this budget.
    const fn admits(self, bytes: u64) -> bool {
        match self {
            Self::Unbounded => true,
            Self::Bounded(remaining) => bytes <= remaining,
        }
    }
}

/// Resident snapshot of upper-layer and SimHash rows for one physical index.
pub(crate) struct VectorMemoryStore {
    scope: DataScope,
    index_id: u64,
    visible_seq: u64,
    simhashes: DashMap<NodeId, SimHash>,
    upper_nodes: DashMap<NodeId, UpperNodeRows>,
    slab: VectorMemorySlab,
    estimated_bytes: AtomicU64,
}

/// Transaction-local set of memory-store rows modified by vector writes.
#[derive(Debug, Default)]
pub(crate) struct VectorMemoryDirtyRows {
    dirty_nodes: DashSet<NodeId>,
    dirty_upper_neighbors: DashSet<(u16, NodeId)>,
}

/// Shared, ref-counted rows that are being committed and must bypass memory cache.
///
/// One fence exists per cache identity. `pending_commits` counts storage
/// commits between their pre-commit acquisition and their resolution, and
/// `generation` advances once per resolved or abandoned commit that may have
/// changed rows, so a store can be proven current against both.
pub(crate) struct VectorMemoryPendingDirtyRows {
    dirty_nodes: DashMap<NodeId, usize>,
    dirty_upper_neighbors: DashMap<(u16, NodeId), usize>,
    dirty_all: AtomicUsize,
    pending_commits: AtomicUsize,
    generation: AtomicU64,
    publish_lock: Mutex<()>,
}

pub(crate) struct VectorMemoryPendingDirtyGuard {
    pending: Arc<VectorMemoryPendingDirtyRows>,
    dirty_nodes: Vec<NodeId>,
    dirty_upper_neighbors: Vec<(u16, NodeId)>,
    dirty_all: bool,
}

/// Physical read accounting for resident-snapshot-aware SimHash lookup.
#[derive(Debug, Default, Clone, Copy)]
pub(in crate::search::vector) struct SimHashReadStats {
    /// Number of logical SimHash row reads.
    pub(in crate::search::vector) reads: usize,
    /// Number of physical batch calls used by those reads.
    pub(in crate::search::vector) multi_get_calls: usize,
    /// Time spent fetching SimHash rows from the stable read view.
    pub(in crate::search::vector) fetch_ns: u64,
}

/// Complete resident-memory capability attached to one vector-index handle.
///
/// Each variant encodes one valid ownership mode: a handle either reads
/// nothing shared, or combines a resident snapshot with its pending commit
/// fences. Writers are always uncached; a transaction's dirty rows are
/// recorded from its planned writes ([`super::commit::VectorCacheWriteSet`]).
pub(crate) enum VectorMemoryAccess {
    /// No shared resident store is attached.
    Uncached,
    /// A managed reader combines immutable lookup with commit-window fences.
    ReadSnapshot {
        /// Exact-index resident store retained by the managed cache read guard.
        store: Arc<VectorMemoryStore>,
        /// Rows currently committing in another retained write set.
        pending: Arc<VectorMemoryPendingDirtyRows>,
    },
}

impl VectorMemoryAccess {
    /// Constructs a handle with no shared cache capability.
    pub(crate) const fn uncached() -> Self {
        Self::Uncached
    }

    /// Grants immutable lookup with commit-window fences to a managed reader.
    pub(crate) fn read_snapshot(
        store: Arc<VectorMemoryStore>,
        pending: Arc<VectorMemoryPendingDirtyRows>,
    ) -> Self {
        Self::ReadSnapshot { store, pending }
    }

    /// Returns the resident store available to this handle, when one exists.
    pub(crate) const fn store(&self) -> Option<&Arc<VectorMemoryStore>> {
        match self {
            Self::ReadSnapshot { store, .. } => Some(store),
            Self::Uncached => None,
        }
    }

    /// Returns whether shared lookup is fenced for this node.
    pub(crate) fn is_node_dirty(&self, node_id: NodeId) -> bool {
        match self {
            Self::ReadSnapshot { pending, .. } => pending.is_node_dirty(node_id),
            Self::Uncached => false,
        }
    }

    /// Returns whether shared lookup is fenced for one upper-neighbor row.
    pub(crate) fn is_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) -> bool {
        match self {
            Self::ReadSnapshot { pending, .. } => pending.is_upper_neighbors_dirty(layer, node_id),
            Self::Uncached => false,
        }
    }

    /// Reads upper-vector rows through this handle's complete cache capability.
    ///
    /// Results preserve caller order. Dirty rows and cache misses come from the
    /// authoritative read view in one typed batch. Request handles never mutate
    /// the registry-owned resident store.
    pub(crate) async fn read_upper_vector_rows<R>(
        &self,
        read: &R,
        keyspace: &VectorRowKeyspace,
        node_ids: &[NodeId],
    ) -> Result<Vec<Option<Bytes>>, HelixDbError>
    where
        R: DbReadOps + Send + Sync + ?Sized,
    {
        let mut rows = vec![None; node_ids.len()];
        let mut fetch_positions = Vec::new();
        let mut fetch_ids = Vec::new();

        for (position, &node_id) in node_ids.iter().enumerate() {
            if !self.is_node_dirty(node_id)
                && let Some(store) = self.store()
                && let Some(value) = store.get_upper_vector(node_id)
            {
                rows[position] = Some(value);
                continue;
            }
            fetch_positions.push(position);
            fetch_ids.push(node_id);
        }

        if fetch_ids.is_empty() {
            return Ok(rows);
        }

        let fetched = VectorRows::new(read, keyspace)
            .upper_vector_rows(&fetch_ids)
            .await?;
        for (position, value) in fetch_positions.into_iter().zip(fetched) {
            rows[position] = value;
        }
        Ok(rows)
    }

    /// Reads one upper-vector row through the same batch hydration contract.
    pub(crate) async fn read_upper_vector_row<R>(
        &self,
        read: &R,
        keyspace: &VectorRowKeyspace,
        node_id: NodeId,
    ) -> Result<Option<Bytes>, HelixDbError>
    where
        R: DbReadOps + Send + Sync + ?Sized,
    {
        let mut rows = self
            .read_upper_vector_rows(read, keyspace, &[node_id])
            .await?;
        Ok(rows.pop().unwrap_or(None))
    }

    /// Reads one upper-neighbor row from the resident snapshot or stable storage.
    pub(crate) async fn read_upper_neighbors<R>(
        &self,
        read: &R,
        keyspace: &VectorRowKeyspace,
        layer: u16,
        node_id: NodeId,
    ) -> Result<Option<Vec<NodeId>>, HelixDbError>
    where
        R: DbReadOps + Send + Sync + ?Sized,
    {
        if !self.is_upper_neighbors_dirty(layer, node_id)
            && let Some(store) = self.store()
            && let Some(value) = store.get_upper_neighbors_bytes(layer, node_id)
        {
            return Ok(Some(
                crate::encoding::v2::values::indexes::vector::neighbors::decode_upper_neighbors(
                    &value,
                )?,
            ));
        }

        let value = VectorRows::new(read, keyspace)
            .upper_neighbors(layer, node_id)
            .await?;
        Ok(value)
    }

    /// Fills an operation-local SimHash map through dirty-aware shared caching.
    ///
    /// The method reports exact stable-view reads. Corrupt deployed rows fail
    /// closed with index and operation context before entering either cache.
    pub(in crate::search::vector) async fn fill_simhash_cache<const COLLECT_TIMING: bool, R>(
        &self,
        read: &R,
        keyspace: &VectorRowKeyspace,
        node_ids: &[NodeId],
        local_cache: &mut HashMap<NodeId, Option<SimHash>>,
        context: &'static str,
    ) -> Result<SimHashReadStats, HelixDbError>
    where
        R: DbReadOps + Send + Sync + ?Sized,
    {
        let mut stats = SimHashReadStats::default();
        let mut fetch_ids = Vec::new();
        for &node_id in node_ids {
            if local_cache.contains_key(&node_id) {
                continue;
            }
            if !self.is_node_dirty(node_id)
                && let Some(store) = self.store()
                && let Some(hash) = store.get_simhash(node_id)
            {
                local_cache.insert(node_id, Some(hash));
                continue;
            }
            fetch_ids.push(node_id);
        }

        if fetch_ids.is_empty() {
            return Ok(stats);
        }

        let fetch_start = COLLECT_TIMING.then(Instant::now);
        let fetched = VectorRows::new(read, keyspace)
            .simhash_rows(&fetch_ids)
            .await?;
        stats.multi_get_calls = 1;
        stats.reads = fetch_ids.len();

        for (node_id, row) in fetch_ids.into_iter().zip(fetched) {
            match row {
                SimHashRow::Present(hash) => {
                    local_cache.insert(node_id, Some(hash));
                }
                SimHashRow::Missing => {
                    local_cache.insert(node_id, None);
                }
                SimHashRow::Corrupt => {
                    return Err(HelixDbError::InvariantViolation(format!(
                        "invalid simhash row for node {node_id} in index {} while {context}",
                        keyspace.index_id()
                    )));
                }
            }
        }
        if let Some(fetch_start) = fetch_start {
            stats.fetch_ns = fetch_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        }
        Ok(stats)
    }

    /// Reads caller-ordered SimHash rows without allocating an operation-local map.
    pub(in crate::search::vector) async fn read_simhash_rows_counted<
        const COLLECT_TIMING: bool,
        R,
    >(
        &self,
        read: &R,
        keyspace: &VectorRowKeyspace,
        node_ids: &[NodeId],
        context: &'static str,
    ) -> Result<(Vec<Option<SimHash>>, SimHashReadStats), HelixDbError>
    where
        R: DbReadOps + Send + Sync + ?Sized,
    {
        let mut stats = SimHashReadStats::default();
        let mut values = vec![None; node_ids.len()];
        let mut fetch_positions = Vec::new();
        let mut fetch_ids = Vec::new();
        for (position, &node_id) in node_ids.iter().enumerate() {
            if !self.is_node_dirty(node_id)
                && let Some(store) = self.store()
                && let Some(hash) = store.get_simhash(node_id)
            {
                values[position] = Some(hash);
                continue;
            }
            fetch_positions.push(position);
            fetch_ids.push(node_id);
        }
        if fetch_ids.is_empty() {
            return Ok((values, stats));
        }

        let fetch_start = COLLECT_TIMING.then(Instant::now);
        let fetched = VectorRows::new(read, keyspace)
            .simhash_rows(&fetch_ids)
            .await?;
        stats.multi_get_calls = 1;
        stats.reads = fetch_ids.len();
        for ((position, node_id), row) in fetch_positions.into_iter().zip(fetch_ids).zip(fetched) {
            values[position] = match row {
                SimHashRow::Present(hash) => Some(hash),
                SimHashRow::Missing => None,
                SimHashRow::Corrupt => {
                    return Err(HelixDbError::InvariantViolation(format!(
                        "invalid simhash row for node {node_id} in index {} while {context}",
                        keyspace.index_id()
                    )));
                }
            };
        }
        if let Some(fetch_start) = fetch_start {
            stats.fetch_ns = fetch_start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        }
        Ok((values, stats))
    }
}

impl VectorMemoryDirtyRows {
    pub(crate) fn mark_node_dirty(&self, node_id: NodeId) {
        self.dirty_nodes.insert(node_id);
    }

    pub(crate) fn mark_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) {
        self.dirty_upper_neighbors.insert((layer, node_id));
    }

    pub(crate) fn dirty_nodes(&self) -> Vec<NodeId> {
        self.dirty_nodes.iter().map(|entry| *entry).collect()
    }

    pub(crate) fn dirty_upper_neighbors(&self) -> Vec<(u16, NodeId)> {
        self.dirty_upper_neighbors
            .iter()
            .map(|entry| *entry)
            .collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.dirty_nodes.is_empty() && self.dirty_upper_neighbors.is_empty()
    }
}

impl VectorMemoryPendingDirtyRows {
    pub(crate) fn new() -> Self {
        Self {
            dirty_nodes: DashMap::new(),
            dirty_upper_neighbors: DashMap::new(),
            dirty_all: AtomicUsize::new(0),
            pending_commits: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            publish_lock: Mutex::new(()),
        }
    }

    /// Registers one storage commit and fences its rows until the guard drops.
    pub(crate) fn acquire(
        self: &Arc<Self>,
        dirty_rows: &VectorMemoryDirtyRows,
    ) -> VectorMemoryPendingDirtyGuard {
        self.pending_commits.fetch_add(1, Ordering::AcqRel);
        let dirty_nodes = dirty_rows.dirty_nodes();
        let dirty_upper_neighbors = dirty_rows.dirty_upper_neighbors();

        for &node_id in &dirty_nodes {
            Self::increment(&self.dirty_nodes, node_id);
        }
        for &row in &dirty_upper_neighbors {
            Self::increment(&self.dirty_upper_neighbors, row);
        }

        VectorMemoryPendingDirtyGuard {
            pending: Arc::clone(self),
            dirty_nodes,
            dirty_upper_neighbors,
            dirty_all: false,
        }
    }

    pub(crate) fn acquire_all(self: &Arc<Self>) -> VectorMemoryPendingDirtyGuard {
        self.dirty_all.fetch_add(1, Ordering::AcqRel);
        VectorMemoryPendingDirtyGuard {
            pending: Arc::clone(self),
            dirty_nodes: Vec::new(),
            dirty_upper_neighbors: Vec::new(),
            dirty_all: true,
        }
    }

    pub(crate) async fn lock_publish(&self) -> MutexGuard<'_, ()> {
        self.publish_lock.lock().await
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Advances the commit generation and returns the generation it replaced.
    pub(crate) fn bump_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel)
    }

    /// Returns whether a storage commit on this identity is still unresolved.
    pub(crate) fn has_pending_commits(&self) -> bool {
        self.pending_commits.load(Ordering::Acquire) > 0
    }

    pub(crate) fn is_all_dirty(&self) -> bool {
        self.dirty_all.load(Ordering::Acquire) > 0
    }

    pub(crate) fn is_node_dirty(&self, node_id: NodeId) -> bool {
        self.is_all_dirty() || self.dirty_nodes.contains_key(&node_id)
    }

    pub(crate) fn is_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) -> bool {
        self.is_all_dirty()
            || self.dirty_nodes.contains_key(&node_id)
            || self.dirty_upper_neighbors.contains_key(&(layer, node_id))
    }

    fn increment<K>(map: &DashMap<K, usize>, key: K)
    where
        K: Eq + std::hash::Hash,
    {
        match map.entry(key) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
    }

    fn decrement<K>(map: &DashMap<K, usize>, key: K)
    where
        K: Eq + std::hash::Hash,
    {
        if let Entry::Occupied(mut entry) = map.entry(key) {
            let count = entry.get_mut();
            if *count <= 1 {
                entry.remove();
            } else {
                *count -= 1;
            }
        }
    }
}

impl Default for VectorMemoryPendingDirtyRows {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for VectorMemoryPendingDirtyGuard {
    fn drop(&mut self) {
        for &node_id in &self.dirty_nodes {
            VectorMemoryPendingDirtyRows::decrement(&self.pending.dirty_nodes, node_id);
        }
        for &row in &self.dirty_upper_neighbors {
            VectorMemoryPendingDirtyRows::decrement(&self.pending.dirty_upper_neighbors, row);
        }
        let counter = if self.dirty_all {
            &self.pending.dirty_all
        } else {
            &self.pending.pending_commits
        };
        counter.fetch_sub(1, Ordering::AcqRel);
    }
}

impl VectorMemoryStore {
    /// Create an empty memory store for `index_id`.
    pub fn new(scope: DataScope, index_id: u64, visible_seq: u64) -> Self {
        Self {
            scope,
            index_id,
            visible_seq,
            simhashes: DashMap::new(),
            // A node's vector and neighbor lookups take the same shard's read
            // lock, so this map gets twice dashmap's default shard count
            // (eight per core); the default made contended lookups slower
            // than two separate maps.
            upper_nodes: DashMap::with_shard_amount(
                (std::thread::available_parallelism().map_or(1, usize::from) * 8)
                    .next_power_of_two(),
            ),
            slab: VectorMemorySlab::default(),
            estimated_bytes: AtomicU64::new(0),
        }
    }

    /// Return the index id for this store.
    pub fn index_id(&self) -> u64 {
        self.index_id
    }

    /// Data namespace this cache is isolated to.
    pub fn scope(&self) -> DataScope {
        self.scope
    }

    /// Highest DB sequence this store may contain rows from.
    pub fn visible_seq(&self) -> u64 {
        self.visible_seq
    }

    /// Return whether this store is safe for a transaction snapshot.
    pub fn is_visible_to_snapshot(&self, snapshot_seq: u64) -> bool {
        self.visible_seq == snapshot_seq
    }

    /// Return whether this store is old enough for a commit-fenced snapshot.
    ///
    /// This is only the sequence half of writer visibility; the registry must
    /// also prove through the commit fence that no newer write is missing.
    pub fn is_usable_for_writer_snapshot(&self, snapshot_seq: u64) -> bool {
        self.visible_seq <= snapshot_seq
    }

    /// Return the resident bytes charged by the last load.
    ///
    /// The charge is exact for row bytes and chunk handles and an upper bound
    /// for index memory (see the module's accounting notes). Rows evicted
    /// afterwards stay charged until the store is replaced, and rows inserted
    /// directly through the `insert_*` methods are not charged.
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    /// SimHash lookup.
    pub fn get_simhash(&self, node_id: NodeId) -> Option<SimHash> {
        self.simhashes.get(&node_id).map(|entry| *entry)
    }

    /// SimHash upsert for fixtures; production rows enter through hydration.
    #[cfg(any(test, feature = "production-coverage"))]
    pub fn insert_simhash(&self, node_id: NodeId, hash: SimHash) {
        self.simhashes.insert(node_id, hash);
    }

    /// Remove SimHash entry.
    pub fn remove_simhash(&self, node_id: NodeId) {
        self.simhashes.remove(&node_id);
    }

    /// Upper-neighbor raw-bytes lookup.
    pub fn get_upper_neighbors_bytes(&self, layer: u16, node_id: NodeId) -> Option<Bytes> {
        let row = self.upper_nodes.get(&node_id)?.neighbors.get(layer)?;
        self.slab.read(row)
    }

    /// Upper-neighbor raw-bytes upsert for fixtures, copying `bytes` into
    /// the slab without charging it; production rows enter through hydration.
    #[cfg(any(test, feature = "production-coverage"))]
    pub fn insert_upper_neighbors_bytes(&self, layer: u16, node_id: NodeId, bytes: Bytes) {
        self.insert_upper_row(node_id, UpperRowKind::Neighbors(layer), &bytes);
    }

    /// Encodes and inserts one validated upper-neighbor list for contract tests.
    ///
    /// Production hydration retains the raw-byte insertion boundary because it
    /// validates row framing before admission. This decoded boundary exists only
    /// for feature-gated production contract coverage.
    #[cfg(feature = "production-coverage")]
    pub fn insert_upper_neighbors(
        &self,
        layer: u16,
        node_id: NodeId,
        neighbors: &[NodeId],
    ) -> Result<(), EncodingError> {
        self.insert_upper_neighbors_bytes(layer, node_id, encode_upper_neighbors(neighbors)?);
        Ok(())
    }

    /// Remove one upper-neighbor row.
    pub fn remove_upper_neighbors(&self, layer: u16, node_id: NodeId) {
        self.update_upper_node(node_id, |rows| rows.neighbors.remove(layer));
    }

    /// Upper-vector raw-bytes lookup.
    pub fn get_upper_vector(&self, node_id: NodeId) -> Option<Bytes> {
        let row = self.upper_nodes.get(&node_id)?.vector?;
        self.slab.read(row)
    }

    /// Upper-vector raw-bytes upsert for fixtures, copying `bytes` into the
    /// slab without charging it; production rows enter through hydration.
    #[cfg(any(test, feature = "production-coverage"))]
    pub fn insert_upper_vector(&self, node_id: NodeId, bytes: Bytes) {
        self.insert_upper_row(node_id, UpperRowKind::Vector, &bytes);
    }

    /// Remove upper-vector row.
    #[cfg(any(test, feature = "production-coverage"))]
    pub fn remove_upper_vector(&self, node_id: NodeId) {
        self.update_upper_node(node_id, |rows| rows.vector = None);
    }

    /// Remove all cache rows for one node.
    pub fn remove_node(&self, node_id: NodeId) {
        self.remove_simhash(node_id);
        self.upper_nodes.remove(&node_id);
    }

    /// Remove every row from this store and release its open slab chunk.
    ///
    /// Contract: sealed chunks, which hold the bulk of the row bytes, are
    /// freed only when the last reference to the store drops, so a caller that
    /// keeps a cleared store alive keeps that memory resident while
    /// [`Self::estimated_bytes`] reports zero. Every registry clear discards
    /// an unpublished store, or a retired one with no active readers, and
    /// then drops it. Releasing sealed chunks here would need a lock or an
    /// atomic swap on every lookup. Bytes already returned by lookups stay
    /// valid; they own their chunk.
    pub fn clear(&self) {
        self.simhashes.clear();
        self.simhashes.shrink_to_fit();
        self.upper_nodes.clear();
        self.upper_nodes.shrink_to_fit();
        self.slab.clear();
        self.estimated_bytes.store(0, Ordering::Relaxed);
    }

    /// Copies one uncharged row into the slab and indexes it.
    #[cfg(any(test, feature = "production-coverage"))]
    fn insert_upper_row(&self, node_id: NodeId, kind: UpperRowKind, bytes: &[u8]) {
        let appended = self
            .slab
            .append(bytes, VectorMemoryAdmissionBudget::Unbounded)
            .expect("an unbounded slab append always admits its row");
        self.upsert_upper_row(node_id, kind, appended.row);
    }

    /// Points `node_id`'s `kind` row at `row`, replacing any previous location.
    fn upsert_upper_row(&self, node_id: NodeId, kind: UpperRowKind, row: SlabRow) {
        match self.upper_nodes.entry(node_id) {
            Entry::Occupied(mut entry) => entry.get_mut().upsert(kind, row),
            Entry::Vacant(entry) => {
                let mut rows = UpperNodeRows {
                    vector: None,
                    neighbors: UpperNeighborRows::Empty,
                };
                rows.upsert(kind, row);
                entry.insert(rows);
            }
        }
    }

    /// Applies `update` to one node's rows and drops the entry once it is empty.
    fn update_upper_node(&self, node_id: NodeId, update: impl FnOnce(&mut UpperNodeRows)) {
        let Entry::Occupied(mut entry) = self.upper_nodes.entry(node_id) else {
            return;
        };
        update(entry.get_mut());
        if entry.get().is_empty() {
            entry.remove();
        }
    }

    /// Admits one validated row when its charge fits `budget`.
    ///
    /// Returns the charged bytes, or `None` without changing the store when
    /// the row does not fit. Upper-row bytes are copied, so the scan's block
    /// buffer is released as soon as the caller drops the row.
    fn admit(
        &self,
        row: VectorMemoryHydrationRow,
        budget: VectorMemoryAdmissionBudget,
    ) -> Option<u64> {
        let (node_id, kind, value) = match row {
            VectorMemoryHydrationRow::SimHash { node_id, value } => {
                return match self.simhashes.entry(node_id) {
                    Entry::Occupied(mut entry) => {
                        entry.insert(value);
                        Some(0)
                    }
                    Entry::Vacant(entry) => budget.admits(SIMHASH_INDEX_ENTRY_BYTES).then(|| {
                        entry.insert(value);
                        SIMHASH_INDEX_ENTRY_BYTES
                    }),
                };
            }
            VectorMemoryHydrationRow::Neighbors {
                layer,
                node_id,
                value,
            } => (node_id, UpperRowKind::Neighbors(layer), value),
            VectorMemoryHydrationRow::Vector { node_id, value } => {
                (node_id, UpperRowKind::Vector, value)
            }
        };
        let index_bytes = self
            .upper_nodes
            .get(&node_id)
            .map_or(UPPER_NODE_INDEX_ENTRY_BYTES, |rows| {
                rows.index_growth_bytes(kind)
            });
        if !budget.admits(index_bytes) {
            return None;
        }
        let appended = self.slab.append(&value, budget.remaining(index_bytes))?;
        self.upsert_upper_row(node_id, kind, appended.row);
        Some(index_bytes + appended.charged)
    }

    /// Hydrates a descriptor-bound unpublished store with fail-closed parsing.
    ///
    /// Malformed keys or SimHash values abort publication. The caller owns an
    /// off-registry store and must discard it on error or shutdown;
    /// successfully budget-limited rows remain a safe lookup store.
    pub(crate) async fn load_descriptor_bound_with_budget<R>(
        &self,
        read: &R,
        budget: VectorMemoryAdmissionBudget,
        shutdown: Option<&mut watch::Receiver<bool>>,
    ) -> Result<VectorMemoryStoreLoadSummary, HelixDbError>
    where
        R: DbReadOps + Send + Sync,
    {
        self.load_from_read_inner(read, budget, shutdown).await
    }

    async fn load_from_read_inner<R>(
        &self,
        read: &R,
        budget: VectorMemoryAdmissionBudget,
        mut shutdown: Option<&mut watch::Receiver<bool>>,
    ) -> Result<VectorMemoryStoreLoadSummary, HelixDbError>
    where
        R: DbReadOps + Send + Sync,
    {
        let prefix = DataKey::Data {
            scope: self.scope,
            kind: DataKeyKind::Vector(VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(
                self.index_id,
            ))),
        }
        .to_bytes();
        let options = ScanOptions::default()
            .with_cache_blocks(false)
            .with_max_fetch_tasks(VECTOR_MEMORY_LOAD_MAX_FETCH_TASKS.max(1));
        let mut iter = read.scan_prefix_with_options(prefix, .., &options).await?;
        let mut loaded = 0usize;
        let mut estimated_bytes = 0u64;
        let mut completion = VectorMemoryStoreLoadCompletion::Complete;
        // Rows this load admits must never share an uncharged chunk.
        self.slab.seal();

        loop {
            let shutdown_requested = shutdown.as_ref().is_some_and(|rx| *rx.borrow());
            if shutdown_requested {
                completion = VectorMemoryStoreLoadCompletion::Shutdown;
                break;
            }

            let maybe_kv = if let Some(shutdown_rx) = shutdown.as_deref_mut() {
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        match changed {
                            Ok(()) => continue,
                            Err(_) => {
                                completion = VectorMemoryStoreLoadCompletion::Shutdown;
                                break;
                            }
                        }
                    }
                    next = iter.next() => next?,
                }
            } else {
                iter.next().await?
            };

            let Some(kv) = maybe_kv else {
                break;
            };

            let Some(logical_key) = self.scope.strip_key(&kv.key) else {
                return Err(HelixDbError::InvariantViolation(
                    "vector memory scan returned key outside data scope".to_string(),
                ));
            };

            let parsed = match VectorKey::parse_from_slice(logical_key) {
                Ok(parsed) => parsed,
                Err(error) => {
                    return Err(HelixDbError::InvariantViolation(format!(
                        "descriptor-bound vector cache hydration found malformed key: {error}"
                    )));
                }
            };
            let row = match parsed {
                VectorKey::UpperNeighbors(key) => Some(VectorMemoryHydrationRow::Neighbors {
                    layer: key.layer(),
                    node_id: key.node_id(),
                    value: kv.value,
                }),
                VectorKey::SimHash(key) => match decode_simhash(&kv.value) {
                    Ok(bits) => Some(VectorMemoryHydrationRow::SimHash {
                        node_id: key.node_id(),
                        value: SimHash::from_bits(bits),
                    }),
                    Err(error) => {
                        return Err(HelixDbError::InvariantViolation(format!(
                            "descriptor-bound vector cache hydration found malformed SimHash: {error}"
                        )));
                    }
                },
                VectorKey::UpperVector(key) => Some(VectorMemoryHydrationRow::Vector {
                    node_id: key.node_id(),
                    value: kv.value,
                }),
                VectorKey::IndexMetadata(_)
                | VectorKey::IndexPrefix(_)
                | VectorKey::TxnGuard(_)
                | VectorKey::Layer0Neighbors(_)
                | VectorKey::VectorPrefix(_)
                | VectorKey::Vector(_)
                | VectorKey::EntryCandidatePrefix(_)
                | VectorKey::EntryCandidateSorted(_)
                | VectorKey::EntryCandidateNode(_)
                | VectorKey::SimHashDirectoryPrefix(_)
                | VectorKey::SimHashDirectory(_)
                | VectorKey::MemoryPrefix(_)
                | VectorKey::L0Prefix(_)
                | VectorKey::ReverseEdgePrefix(_)
                | VectorKey::ReverseEdge(_) => None,
            };
            let Some(row) = row else {
                continue;
            };
            let Some(row_bytes) = self.admit(row, budget.remaining(estimated_bytes)) else {
                completion = VectorMemoryStoreLoadCompletion::BudgetExhausted;
                break;
            };
            let Some(candidate_total) = estimated_bytes.checked_add(row_bytes) else {
                return Err(HelixDbError::InvariantViolation(
                    "vector memory admission byte count overflowed".to_string(),
                ));
            };
            let Some(next_loaded) = loaded.checked_add(1) else {
                return Err(HelixDbError::InvariantViolation(
                    "vector memory admitted entry count overflowed".to_string(),
                ));
            };
            loaded = next_loaded;
            estimated_bytes = candidate_total;
        }

        self.slab.seal();
        self.estimated_bytes
            .store(estimated_bytes, Ordering::Relaxed);
        Ok(VectorMemoryStoreLoadSummary {
            loaded_entries: loaded,
            estimated_bytes,
            completion,
        })
    }
}

/// Upper bound on hash-index bytes per live entry of a map from `K` to `V`.
///
/// `hashbrown` stores a one-byte control word beside each `(K, V)` bucket,
/// fills at most 7/8 of its buckets, and doubles when full, so a table that has
/// just grown is 7/16 full. Charging 16/7 buckets per entry therefore covers
/// every table once it has outgrown its smallest sizes.
const fn hash_index_entry_bytes<K, V>() -> u64 {
    let bucket = core::mem::size_of::<(K, V)>() as u64 + 1;
    (bucket * 16).div_ceil(7)
}

/// Charge for one upper row of `value_len` bytes admitted into an empty
/// store: its index entry, its bytes, and the handle of the chunk it opens.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) const fn isolated_upper_row_admission_bytes(value_len: u64) -> u64 {
    UPPER_NODE_INDEX_ENTRY_BYTES + SLAB_CHUNK_HANDLE_BYTES + value_len
}

/// One fully validated cache row held only until its admission decision.
enum VectorMemoryHydrationRow {
    Neighbors {
        layer: u16,
        node_id: NodeId,
        value: Bytes,
    },
    SimHash {
        node_id: NodeId,
        value: SimHash,
    },
    Vector {
        node_id: NodeId,
        value: Bytes,
    },
}

/// Which upper row of a node an operation addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpperRowKind {
    /// The node's upper-layer vector row.
    Vector,
    /// The node's upper-neighbor row on this layer.
    Neighbors(u16),
}

/// Location of one row's bytes inside a [`VectorMemorySlab`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlabRow {
    chunk: u32,
    offset: u32,
    len: u32,
}

impl SlabRow {
    /// Location of a zero-length row, which never occupies a chunk.
    const EMPTY: Self = Self {
        chunk: 0,
        offset: 0,
        len: 0,
    };

    /// Byte range of this row inside its chunk.
    fn range(self) -> Range<usize> {
        // u32 -> usize is lossless on every supported target.
        let start = self.offset as usize;
        start..start + self.len as usize
    }
}

/// Upper rows resident for one node; an empty value is never stored.
#[derive(Debug)]
struct UpperNodeRows {
    vector: Option<SlabRow>,
    neighbors: UpperNeighborRows,
}

impl UpperNodeRows {
    fn is_empty(&self) -> bool {
        self.vector.is_none() && self.neighbors.is_empty()
    }

    /// Heap bytes inserting a `kind` row would add beyond this entry.
    fn index_growth_bytes(&self, kind: UpperRowKind) -> u64 {
        match kind {
            UpperRowKind::Vector => 0,
            UpperRowKind::Neighbors(layer) => self.neighbors.growth_bytes(layer),
        }
    }

    fn upsert(&mut self, kind: UpperRowKind, row: SlabRow) {
        match kind {
            UpperRowKind::Vector => self.vector = Some(row),
            UpperRowKind::Neighbors(layer) => self.neighbors.upsert(layer, row),
        }
    }
}

/// Upper-neighbor row locations of one node, inline for the common case of a
/// single upper layer.
#[derive(Debug)]
enum UpperNeighborRows {
    Empty,
    One(u16, SlabRow),
    /// Two or more rows, strictly ascending by layer.
    Many(Box<[(u16, SlabRow)]>),
}

impl UpperNeighborRows {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn get(&self, layer: u16) -> Option<SlabRow> {
        match self {
            Self::Empty => None,
            Self::One(row_layer, row) => (*row_layer == layer).then_some(*row),
            Self::Many(rows) => rows
                .binary_search_by_key(&layer, |(row_layer, _)| *row_layer)
                .ok()
                .map(|position| rows[position].1),
        }
    }

    /// Heap bytes [`Self::upsert`] of `layer` would add.
    fn growth_bytes(&self, layer: u16) -> u64 {
        match self {
            Self::Empty => 0,
            Self::One(row_layer, _) if *row_layer == layer => 0,
            Self::One(..) => 2 * UPPER_LAYER_SLOT_BYTES,
            Self::Many(rows) => {
                match rows.binary_search_by_key(&layer, |(row_layer, _)| *row_layer) {
                    Ok(_) => 0,
                    Err(_) => UPPER_LAYER_SLOT_BYTES,
                }
            }
        }
    }

    fn upsert(&mut self, layer: u16, row: SlabRow) {
        *self = match core::mem::replace(self, Self::Empty) {
            Self::Empty => Self::One(layer, row),
            Self::One(row_layer, _) if row_layer == layer => Self::One(layer, row),
            Self::One(row_layer, existing) => {
                let mut rows = [(row_layer, existing), (layer, row)];
                rows.sort_unstable_by_key(|(row_layer, _)| *row_layer);
                Self::Many(Box::new(rows))
            }
            Self::Many(mut rows) => {
                match rows.binary_search_by_key(&layer, |(row_layer, _)| *row_layer) {
                    Ok(position) => {
                        rows[position].1 = row;
                        Self::Many(rows)
                    }
                    Err(position) => Self::Many(
                        rows[..position]
                            .iter()
                            .copied()
                            .chain([(layer, row)])
                            .chain(rows[position..].iter().copied())
                            .collect(),
                    ),
                }
            }
        };
    }

    fn remove(&mut self, layer: u16) {
        *self = match core::mem::replace(self, Self::Empty) {
            Self::One(row_layer, row) if row_layer != layer => Self::One(row_layer, row),
            Self::Empty | Self::One(..) => Self::Empty,
            Self::Many(rows) => {
                let kept: Vec<(u16, SlabRow)> = rows
                    .iter()
                    .copied()
                    .filter(|(row_layer, _)| *row_layer != layer)
                    .collect();
                match kept.as_slice() {
                    [(row_layer, row)] => Self::One(*row_layer, *row),
                    _ => Self::Many(kept.into_boxed_slice()),
                }
            }
        };
    }
}

/// Result of copying one row into the slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlabAppend {
    row: SlabRow,
    /// The row's length, plus [`SLAB_CHUNK_HANDLE_BYTES`] when it opened a
    /// chunk.
    charged: u64,
}

/// Append-only byte arena that owns the store's upper-row bytes.
///
/// Rows are copied into one open chunk. When the next row does not fit, the
/// open chunk is trimmed to its length, frozen into an immutable [`Bytes`],
/// and published once in [`SealedChunks`], which lookups read with plain
/// atomic loads. Rows still in the open chunk (only during an unpublished
/// load, or after fixture inserts) are copied out under its lock. Chunks
/// stay alive until the slab and every returned slice of them drop.
struct VectorMemorySlab {
    sealed: SealedChunks,
    open: parking_lot::Mutex<OpenChunk>,
}

impl Default for VectorMemorySlab {
    fn default() -> Self {
        Self {
            sealed: SealedChunks {
                buckets: std::array::from_fn(|_| OnceLock::new()),
            },
            open: parking_lot::Mutex::new(OpenChunk { id: 0, bytes: None }),
        }
    }
}

/// The chunk rows are appended to, and the id it takes when sealed.
struct OpenChunk {
    /// Id of the open chunk; ids below it are sealed or were skipped by a
    /// clear, so a released open chunk's id is never reused.
    id: u32,
    /// Open chunk bytes, never empty while present.
    bytes: Option<Vec<u8>>,
}

/// Lock-free, append-only table of sealed chunks.
///
/// Bucket `b` holds the `2^b` ids `2^b - 1 ..= 2^(b + 1) - 2`, so 33 buckets
/// cover every `u32` id. A bucket is allocated when its first chunk seals, so
/// slots never move and each is set once.
struct SealedChunks {
    buckets: [OnceLock<Box<[OnceLock<Bytes>]>>; u32::BITS as usize + 1],
}

impl SealedChunks {
    /// Bucket and slot of chunk `id`.
    fn position(id: u32) -> (usize, usize) {
        let ordinal = u64::from(id) + 1;
        let bucket = 63 - ordinal.leading_zeros();
        // Both values are below 2^32, so they fit usize on supported targets.
        (bucket as usize, (ordinal - (1 << bucket)) as usize)
    }

    fn get(&self, id: u32) -> Option<&Bytes> {
        let (bucket, slot) = Self::position(id);
        self.buckets[bucket].get()?[slot].get()
    }

    /// Publishes `chunk` under `id`; each id is published at most once.
    fn publish(&self, id: u32, chunk: Bytes) {
        let (bucket, slot) = Self::position(id);
        let slots = self.buckets[bucket]
            .get_or_init(|| (0..1usize << bucket).map(|_| OnceLock::new()).collect());
        assert!(
            slots[slot].set(chunk).is_ok(),
            "vector memory slab chunk {id} was sealed twice"
        );
    }
}

impl VectorMemorySlab {
    /// Returns the bytes at `row`, or `None` when its open chunk was released.
    fn read(&self, row: SlabRow) -> Option<Bytes> {
        if row.len == 0 {
            return Some(Bytes::new());
        }
        let Some(chunk) = self.sealed.get(row.chunk) else {
            // Sealing publishes and advances the open id under this lock, so
            // a row not in the open chunk now is sealed or was released.
            let open = self.open.lock();
            return match &open.bytes {
                Some(bytes) if open.id == row.chunk => {
                    Some(Bytes::copy_from_slice(&bytes[row.range()]))
                }
                _ => self
                    .sealed
                    .get(row.chunk)
                    .map(|chunk| chunk.slice(row.range())),
            };
        };
        Some(chunk.slice(row.range()))
    }

    /// Copies `bytes` into the open chunk, first sealing it and opening a new
    /// chunk of [`VECTOR_MEMORY_SLAB_CHUNK_BYTES`] (or the row's length, when
    /// longer) when it lacks room.
    ///
    /// Charges the row's length, plus [`SLAB_CHUNK_HANDLE_BYTES`] when it
    /// opens a chunk, and returns `None`, leaving the slab unchanged, when
    /// that charge exceeds `budget`. The open chunk's spare capacity is not
    /// charged: sealing frees it. Rows must fit `u32` lengths, which SlateDB
    /// enforces for every value.
    fn append(&self, bytes: &[u8], budget: VectorMemoryAdmissionBudget) -> Option<SlabAppend> {
        let len = u32::try_from(bytes.len()).expect("SlateDB values fit u32 lengths");
        if len == 0 {
            return Some(SlabAppend {
                row: SlabRow::EMPTY,
                charged: 0,
            });
        }
        let mut open = self.open.lock();
        let fits = open
            .bytes
            .as_ref()
            .is_some_and(|chunk| chunk.capacity() - chunk.len() >= bytes.len());
        let handle = if fits { 0 } else { SLAB_CHUNK_HANDLE_BYTES };
        let charged = u64::from(len) + handle;
        if !budget.admits(charged) {
            return None;
        }
        if !fits {
            self.seal_locked(&mut open);
        }
        let id = open.id;
        // A chunk holds one u32-length row or at most the chunk size, so
        // offsets fit u32.
        let chunk = open.bytes.get_or_insert_with(|| {
            Vec::with_capacity(VECTOR_MEMORY_SLAB_CHUNK_BYTES.max(bytes.len()))
        });
        let offset = u32::try_from(chunk.len()).expect("slab chunk offsets fit u32");
        chunk.extend_from_slice(bytes);
        Some(SlabAppend {
            row: SlabRow {
                chunk: id,
                offset,
                len,
            },
            charged,
        })
    }

    /// Seals the open chunk, if any.
    fn seal(&self) {
        self.seal_locked(&mut self.open.lock());
    }

    /// Publishes the open chunk under its id, trimmed to its length.
    ///
    /// A chunk with spare capacity is copied into an exact-size allocation
    /// and dropped, because shrinking in place may leave the spare bytes
    /// resident (mimalloc keeps a block that shrinks by less than half).
    fn seal_locked(&self, open: &mut OpenChunk) {
        let Some(chunk) = open.bytes.take() else {
            return;
        };
        let sealed = if chunk.len() == chunk.capacity() {
            Bytes::from(chunk)
        } else {
            Bytes::copy_from_slice(&chunk)
        };
        self.sealed.publish(open.id, sealed);
        open.id = open
            .id
            .checked_add(1)
            .expect("vector memory slab chunk ids fit u32");
    }

    /// Releases the open chunk and skips its id. Sealed chunks are released
    /// when the slab drops, which follows every production clear.
    fn clear(&self) {
        let mut open = self.open.lock();
        if open.bytes.take().is_some() {
            open.id = open
                .id
                .checked_add(1)
                .expect("vector memory slab chunk ids fit u32");
        }
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../../tests/production_support/vector/memory_store.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
#[path = "store_tests.rs"]
mod oracle_tests;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};
    use tokio::sync::watch;

    use super::*;

    /// Proves the uncached capability cannot accidentally read or mutate cache state.
    #[test]
    fn memory_access_uncached_has_no_cache_capability() {
        let access = VectorMemoryAccess::uncached();

        assert!(access.store().is_none());
        assert!(!access.is_node_dirty(7));
        assert!(!access.is_upper_neighbors_dirty(2, 7));
    }

    /// Proves managed reads carry their pending commit fences.
    #[test]
    fn memory_access_reads_pending_dirty_rows() {
        let store = Arc::new(VectorMemoryStore::new(DataScope::LegacyUnscoped, 42, 10));
        let pending = Arc::new(VectorMemoryPendingDirtyRows::new());
        let pending_source = VectorMemoryDirtyRows::default();
        pending_source.mark_upper_neighbors_dirty(3, 11);
        let _pending_guard = pending.acquire(&pending_source);

        let pending_access = VectorMemoryAccess::read_snapshot(Arc::clone(&store), pending);
        assert!(pending_access
            .store()
            .is_some_and(|attached| Arc::ptr_eq(attached, &store)));
        assert!(pending_access.is_upper_neighbors_dirty(3, 11));
    }

    /// Opens an isolated in-memory SlateDB for hydration tests.
    async fn test_db(name: &str) -> Arc<Db> {
        let object_store = Arc::new(InMemory::new());
        Arc::new(
            Db::open(name, object_store)
                .await
                .expect("test db should open"),
        )
    }

    #[tokio::test]
    async fn descriptor_bound_load_hydrates_supported_rows() {
        let db = test_db("memory_store_hydrates_rows").await;
        let index_id = crate::search::vector::index_id_from_name("memory_store_hydrates_rows_idx");
        let other_index_id = index_id.wrapping_add(1);

        let upper_neighbors_key =
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 3, 101)).to_bytes();
        let simhash_key = VectorKey::SimHash(VectorSimHashKey::new(index_id, 101)).to_bytes();
        let upper_vector_key =
            VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 101)).to_bytes();

        let foreign_simhash_key =
            VectorKey::SimHash(VectorSimHashKey::new(other_index_id, 101)).to_bytes();

        let raw = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("raw tx should open");
        raw.put(&upper_neighbors_key, Bytes::from_static(&[0, 1, 2]))
            .expect("put upper neighbors");
        raw.put(
            &simhash_key,
            Bytes::copy_from_slice(&encode_simhash(0x0123_4567_89AB_CDEF)),
        )
        .expect("put simhash");
        raw.put(&upper_vector_key, Bytes::from_static(&[9, 8, 7, 6]))
            .expect("put upper vector");
        raw.put(
            &foreign_simhash_key,
            Bytes::copy_from_slice(&encode_simhash(0xFFFF)),
        )
        .expect("put foreign simhash");
        raw.commit().await.expect("raw tx commit");

        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        let summary = store
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Unbounded,
                None,
            )
            .await
            .expect("load should succeed");

        assert_eq!(
            summary.loaded_entries, 3,
            "only same-index supported rows should hydrate"
        );
        assert_eq!(
            summary.completion,
            VectorMemoryStoreLoadCompletion::Complete
        );
        assert!(summary.estimated_bytes > 0);
        assert_eq!(store.estimated_bytes(), summary.estimated_bytes);
        assert_eq!(
            store
                .get_upper_neighbors_bytes(3, 101)
                .expect("upper neighbors cached")
                .as_ref(),
            &[0, 1, 2]
        );
        assert_eq!(
            store.get_simhash(101).expect("simhash cached").bits(),
            0x0123_4567_89AB_CDEF
        );
        assert_eq!(
            store
                .get_upper_vector(101)
                .expect("upper vector cached")
                .as_ref(),
            &[9, 8, 7, 6]
        );
    }

    #[tokio::test]
    async fn descriptor_bound_load_rejects_malformed_rows() {
        let db = test_db("memory_store_skips_invalid_rows").await;
        let index_id =
            crate::search::vector::index_id_from_name("memory_store_skips_invalid_rows_idx");

        let valid_upper_neighbors_key =
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 2, 55)).to_bytes();
        let valid_simhash_key = VectorKey::SimHash(VectorSimHashKey::new(index_id, 55)).to_bytes();
        let valid_upper_vector_key =
            VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 55)).to_bytes();

        let mut invalid_upper_neighbors_key = valid_upper_neighbors_key.to_vec();
        invalid_upper_neighbors_key.pop();
        let mut invalid_upper_vector_key = valid_upper_vector_key.to_vec();
        invalid_upper_vector_key.pop();
        let mut unknown_kind_key = VectorKey::SimHash(VectorSimHashKey::new(index_id, 55))
            .to_bytes()
            .to_vec();
        unknown_kind_key[9] = 0x7F;

        let raw = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("raw tx should open");
        raw.put(&valid_upper_neighbors_key, Bytes::from_static(&[1, 2, 3]))
            .expect("put valid upper neighbors");
        raw.put(
            &valid_simhash_key,
            Bytes::copy_from_slice(&encode_simhash(123)),
        )
        .expect("put valid simhash");
        raw.put(&valid_upper_vector_key, Bytes::from_static(&[4, 5, 6]))
            .expect("put valid upper vector");

        raw.put(&invalid_upper_neighbors_key, Bytes::from_static(&[9]))
            .expect("put invalid upper neighbors");
        raw.put(&invalid_upper_vector_key, Bytes::from_static(&[8]))
            .expect("put invalid upper vector");
        raw.put(&valid_simhash_key[..17], Bytes::from_static(&[1, 2, 3]))
            .expect("put malformed simhash key");
        raw.put(&unknown_kind_key, Bytes::from_static(&[7, 7, 7]))
            .expect("put unknown kind");
        raw.put(
            VectorKey::SimHash(VectorSimHashKey::new(index_id, 999)).to_bytes(),
            Bytes::from_static(&[0xAA]),
        )
        .expect("put invalid simhash payload");
        raw.commit().await.expect("raw tx commit");

        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        assert!(
            store
                .load_descriptor_bound_with_budget(
                    db.as_ref(),
                    VectorMemoryAdmissionBudget::Unbounded,
                    None,
                )
                .await
                .is_err(),
            "descriptor-bound hydration must reject the same malformed prefix"
        );
    }

    #[tokio::test]
    async fn descriptor_bound_load_exits_before_scan_when_shutdown_is_signaled() {
        let db = test_db("memory_store_shutdown_short_circuit").await;
        let index_id =
            crate::search::vector::index_id_from_name("memory_store_shutdown_short_circuit_idx");

        let simhash_key = VectorKey::SimHash(VectorSimHashKey::new(index_id, 77)).to_bytes();
        let raw = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("raw tx should open");
        raw.put(
            &simhash_key,
            Bytes::copy_from_slice(&encode_simhash(0xABCD)),
        )
        .expect("put simhash");
        raw.commit().await.expect("raw tx commit");

        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let _ = shutdown_tx.send(true);

        let summary = store
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Unbounded,
                Some(&mut shutdown_rx),
            )
            .await
            .expect("load should short-circuit cleanly");

        assert_eq!(summary.loaded_entries, 0);
        assert_eq!(summary.estimated_bytes, 0);
        assert_eq!(
            summary.completion,
            VectorMemoryStoreLoadCompletion::Shutdown
        );
        assert!(store.get_simhash(77).is_none());
    }

    #[tokio::test]
    async fn bounded_load_stops_before_the_first_row_that_exceeds_admission() {
        let db = test_db("memory_store_incremental_admission").await;
        let index_id =
            crate::search::vector::index_id_from_name("memory_store_incremental_admission_idx");
        let first_key =
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 1, 7)).to_bytes();
        let second_key =
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 1, 8)).to_bytes();
        let first_value = Bytes::from_static(&[1, 2, 3]);
        let second_value = Bytes::from_static(&[4, 5, 6]);
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(&first_key, first_value.clone()).unwrap();
        tx.put(&second_key, second_value).unwrap();
        tx.commit().await.unwrap();

        let first_row_bytes =
            isolated_upper_row_admission_bytes(u64::try_from(first_value.len()).unwrap());
        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        let summary = store
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Bounded(first_row_bytes),
                None,
            )
            .await
            .unwrap();

        assert_eq!(summary.loaded_entries, 1);
        assert_eq!(summary.estimated_bytes, first_row_bytes);
        assert_eq!(
            summary.completion,
            VectorMemoryStoreLoadCompletion::BudgetExhausted
        );
        assert!(store.get_upper_neighbors_bytes(1, 7).is_some());
        assert!(store.get_upper_neighbors_bytes(1, 8).is_none());

        let empty = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        let summary = empty
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Bounded(first_row_bytes - 1),
                None,
            )
            .await
            .unwrap();
        assert_eq!(summary.loaded_entries, 0);
        assert_eq!(summary.estimated_bytes, 0);
        assert_eq!(
            summary.completion,
            VectorMemoryStoreLoadCompletion::BudgetExhausted
        );
    }

    #[test]
    fn test_visible_seq_gates_snapshot_eligibility() {
        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, 42, 10);

        assert_eq!(store.visible_seq(), 10);
        assert!(store.is_visible_to_snapshot(10));
        assert!(!store.is_visible_to_snapshot(11));
        assert!(!store.is_visible_to_snapshot(9));

        assert!(store.is_usable_for_writer_snapshot(10));
        assert!(store.is_usable_for_writer_snapshot(11));
        assert!(!store.is_usable_for_writer_snapshot(9));
    }

    #[test]
    fn test_remove_node_clears_all_row_types_for_node() {
        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, 42, u64::MAX);
        store.insert_simhash(7, SimHash::from_bits(7));
        store.insert_upper_vector(7, Bytes::from_static(&[1, 1, 1]));
        store.insert_upper_neighbors_bytes(1, 7, Bytes::from_static(&[2, 2]));
        store.insert_upper_neighbors_bytes(2, 7, Bytes::from_static(&[3, 3]));

        store.insert_upper_neighbors_bytes(2, 99, Bytes::from_static(&[9]));

        store.remove_node(7);

        assert!(store.get_simhash(7).is_none());
        assert!(store.get_upper_vector(7).is_none());
        assert!(store.get_upper_neighbors_bytes(1, 7).is_none());
        assert!(store.get_upper_neighbors_bytes(2, 7).is_none());
        assert!(
            store.get_upper_neighbors_bytes(2, 99).is_some(),
            "removal should not affect other nodes"
        );
    }

    #[test]
    fn node_primary_removal_does_not_scan_100_000_unrelated_neighbor_rows() {
        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, 42, u64::MAX);
        for node_id in 0..100_000 {
            store.insert_upper_neighbors_bytes(1, node_id, Bytes::from_static(&[1]));
        }
        store.insert_upper_neighbors_bytes(2, 50_000, Bytes::from_static(&[2]));
        assert_eq!(store.upper_nodes.len(), 100_000);

        store.remove_node(50_000);

        assert_eq!(store.upper_nodes.len(), 99_999);
        assert!(store.get_upper_neighbors_bytes(1, 49_999).is_some());
        assert!(store.get_upper_neighbors_bytes(1, 50_000).is_none());
        assert!(store.get_upper_neighbors_bytes(2, 50_000).is_none());
        assert!(store.get_upper_neighbors_bytes(1, 50_001).is_some());
    }

    /// Pins the compact per-node index layout the accounting relies on.
    #[test]
    fn index_entries_stay_compact() {
        assert_eq!(core::mem::size_of::<SlabRow>(), 12);
        assert_eq!(core::mem::size_of::<UpperNodeRows>(), 40);
        assert_eq!(UPPER_NODE_INDEX_ENTRY_BYTES, 112);
        assert_eq!(SIMHASH_INDEX_ENTRY_BYTES, 39);
        assert_eq!(UPPER_LAYER_SLOT_BYTES, 16);
        assert_eq!(hash_index_entry_bytes::<u8, ()>(), 5);
    }

    /// Layers stay sorted, collapse back to inline storage, and report the
    /// heap growth each upsert causes.
    #[test]
    fn upper_neighbor_rows_keep_layers_sorted_and_report_growth() {
        let row = |chunk| SlabRow {
            chunk,
            offset: 0,
            len: 1,
        };
        let mut rows = UpperNeighborRows::Empty;
        assert!(rows.is_empty());
        assert_eq!(rows.get(1), None);
        assert_eq!(rows.growth_bytes(3), 0);
        rows.remove(3);
        assert!(rows.is_empty());

        rows.upsert(3, row(3));
        assert!(matches!(rows, UpperNeighborRows::One(3, _)));
        assert_eq!(rows.growth_bytes(3), 0);
        rows.upsert(3, row(30));
        assert_eq!(rows.get(3), Some(row(30)));
        assert_eq!(rows.get(1), None);
        rows.remove(1);
        assert_eq!(rows.get(3), Some(row(30)));

        assert_eq!(rows.growth_bytes(1), 2 * UPPER_LAYER_SLOT_BYTES);
        rows.upsert(1, row(1));
        assert_eq!(rows.growth_bytes(2), UPPER_LAYER_SLOT_BYTES);
        rows.upsert(2, row(2));
        assert_eq!(rows.growth_bytes(2), 0);
        rows.upsert(2, row(20));
        rows.upsert(5, row(5));
        let UpperNeighborRows::Many(layers) = &rows else {
            panic!("four layers spill to the heap");
        };
        assert_eq!(
            layers.iter().map(|(layer, _)| *layer).collect::<Vec<_>>(),
            [1, 2, 3, 5]
        );
        assert_eq!(rows.get(2), Some(row(20)));
        assert_eq!(rows.get(4), None);

        rows.remove(4);
        rows.remove(2);
        rows.remove(5);
        assert!(matches!(rows, UpperNeighborRows::Many(ref layers) if layers.len() == 2));
        rows.remove(1);
        assert!(matches!(rows, UpperNeighborRows::One(3, _)));
        rows.remove(3);
        assert!(rows.is_empty());
    }

    /// Rows read back from the open chunk and from sealed chunks, empty rows
    /// never occupy a chunk, and a released open chunk's id is never reused.
    #[test]
    fn slab_reads_open_and_sealed_rows_and_never_aliases_after_clear() {
        let slab = VectorMemorySlab::default();
        slab.seal();
        assert!(
            slab.sealed.get(0).is_none(),
            "sealing without an open chunk is a no-op"
        );
        let empty = slab
            .append(&[], VectorMemoryAdmissionBudget::Bounded(0))
            .unwrap();
        assert_eq!(empty.row, SlabRow::EMPTY);
        assert_eq!(empty.charged, 0);
        assert_eq!(slab.read(empty.row).as_deref(), Some(&[][..]));

        let first = slab
            .append(b"first", VectorMemoryAdmissionBudget::Unbounded)
            .unwrap();
        assert_eq!(first.charged, 5 + SLAB_CHUNK_HANDLE_BYTES);
        let second = slab
            .append(b"second", VectorMemoryAdmissionBudget::Bounded(6))
            .unwrap();
        assert_eq!(
            second.charged, 6,
            "a row that fits the open chunk opens none"
        );
        assert_eq!(second.row.chunk, first.row.chunk);
        assert_eq!(second.row.offset, 5);
        assert_eq!(slab.read(first.row).as_deref(), Some(&b"first"[..]));

        slab.seal();
        assert_eq!(slab.read(second.row).as_deref(), Some(&b"second"[..]));
        assert_eq!(slab.sealed.get(first.row.chunk).map(Bytes::len), Some(11));

        let open = slab
            .append(b"open", VectorMemoryAdmissionBudget::Unbounded)
            .unwrap();
        assert_eq!(open.row.chunk, first.row.chunk + 1);
        slab.clear();
        assert_eq!(slab.read(open.row), None, "the open chunk is released");
        assert_eq!(
            slab.read(first.row).as_deref(),
            Some(&b"first"[..]),
            "sealed chunks live until the slab drops"
        );
        slab.clear();

        let after = slab
            .append(b"after", VectorMemoryAdmissionBudget::Unbounded)
            .unwrap();
        assert_eq!(after.row.chunk, open.row.chunk + 1);
        assert_eq!(slab.read(after.row).as_deref(), Some(&b"after"[..]));
        assert_eq!(slab.read(open.row), None, "a skipped id never aliases");
        assert_eq!(
            slab.read(SlabRow {
                chunk: after.row.chunk + 1,
                offset: 0,
                len: 1,
            }),
            None,
            "an id past the open chunk is a miss"
        );
    }

    /// Chunk ids map onto doubling buckets without gaps or overlap.
    #[test]
    fn sealed_chunk_positions_fill_doubling_buckets() {
        assert_eq!(SealedChunks::position(0), (0, 0));
        assert_eq!(SealedChunks::position(1), (1, 0));
        assert_eq!(SealedChunks::position(2), (1, 1));
        assert_eq!(SealedChunks::position(3), (2, 0));
        assert_eq!(SealedChunks::position(6), (2, 3));
        assert_eq!(SealedChunks::position(7), (3, 0));
        assert_eq!(SealedChunks::position(u32::MAX - 1), (31, (1 << 31) - 1));
        assert_eq!(SealedChunks::position(u32::MAX), (32, 0));
        let slab = VectorMemorySlab::default();
        for id in 0..20u32 {
            slab.sealed.publish(id, Bytes::from(vec![id as u8]));
        }
        for id in 0..20u32 {
            assert_eq!(slab.sealed.get(id).map(|chunk| chunk[0]), Some(id as u8));
        }
        assert_eq!(slab.sealed.get(20), None);
        assert_eq!(slab.sealed.get(40), None);
    }

    /// Appends charge the row plus a handle for each chunk they open, refuse
    /// a row whose charge exceeds the budget without changing the slab, and
    /// give a row longer than the chunk size a chunk of its own.
    #[test]
    fn slab_appends_charge_rows_and_opened_chunks() {
        let chunk = VECTOR_MEMORY_SLAB_CHUNK_BYTES;
        let handle = SLAB_CHUNK_HANDLE_BYTES;
        let slab = VectorMemorySlab::default();
        assert_eq!(
            slab.append(b"four", VectorMemoryAdmissionBudget::Bounded(handle + 3)),
            None
        );
        assert_eq!(
            slab.append(b"four", VectorMemoryAdmissionBudget::Bounded(3)),
            None,
            "a budget below the handle refuses every new chunk"
        );
        assert!(slab.open.lock().bytes.is_none());

        let exact = slab
            .append(b"four", VectorMemoryAdmissionBudget::Bounded(handle + 4))
            .unwrap();
        assert_eq!(exact.charged, handle + 4);
        assert_eq!(
            slab.open.lock().bytes.as_ref().map(Vec::capacity),
            Some(chunk)
        );
        assert_eq!(
            slab.append(b"next", VectorMemoryAdmissionBudget::Bounded(3)),
            None,
            "a row that fits the open chunk still needs its length"
        );
        let next = slab
            .append(b"next", VectorMemoryAdmissionBudget::Bounded(4))
            .unwrap();
        assert_eq!(
            (next.row.chunk, next.row.offset, next.charged),
            (exact.row.chunk, 4, 4)
        );

        let tail = vec![7; chunk - 8];
        let filled = slab
            .append(
                &tail,
                VectorMemoryAdmissionBudget::Bounded(tail.len() as u64),
            )
            .unwrap();
        assert_eq!(
            filled.row.chunk, exact.row.chunk,
            "the row fills the chunk exactly"
        );
        let opener = slab
            .append(b"x", VectorMemoryAdmissionBudget::Unbounded)
            .unwrap();
        assert_eq!(opener.row.chunk, exact.row.chunk + 1);
        assert_eq!(opener.charged, handle + 1);

        let oversized = vec![9; chunk + 1];
        let large = slab
            .append(
                &oversized,
                VectorMemoryAdmissionBudget::Bounded(handle + chunk as u64 + 1),
            )
            .unwrap();
        assert_eq!(large.row.chunk, opener.row.chunk + 1);
        assert_eq!(large.charged, handle + chunk as u64 + 1);
        assert_eq!(
            slab.open.lock().bytes.as_ref().map(Vec::capacity),
            Some(chunk + 1),
            "a long row gets a chunk of its own length"
        );
        slab.seal();
        assert_eq!(slab.read(large.row).as_deref(), Some(&oversized[..]));
        assert_eq!(slab.read(filled.row).as_deref(), Some(&tail[..]));
        assert_eq!(slab.read(opener.row).as_deref(), Some(&b"x"[..]));
        assert_eq!(slab.read(exact.row).as_deref(), Some(&b"four"[..]));
        assert_eq!(slab.read(next.row).as_deref(), Some(&b"next"[..]));
    }

    /// Sealing trims a partly filled chunk by copying it into an exact-size
    /// allocation, so its spare capacity is freed under any allocator, and
    /// publishes a full chunk without copying.
    #[test]
    fn sealing_copies_partly_filled_chunks_into_exact_allocations() {
        let slab = VectorMemorySlab::default();
        let partial = slab
            .append(b"partial", VectorMemoryAdmissionBudget::Unbounded)
            .unwrap();
        let partial_buffer = slab.open.lock().bytes.as_ref().map(|chunk| chunk.as_ptr());
        slab.seal();
        let sealed = slab.sealed.get(partial.row.chunk).unwrap();
        assert_eq!(sealed.len(), 7);
        assert_ne!(
            Some(sealed.as_ptr()),
            partial_buffer,
            "a partly filled chunk is copied"
        );

        let full = vec![3; VECTOR_MEMORY_SLAB_CHUNK_BYTES + 5];
        let whole = slab
            .append(&full, VectorMemoryAdmissionBudget::Unbounded)
            .unwrap();
        let whole_buffer = slab.open.lock().bytes.as_ref().map(|chunk| chunk.as_ptr());
        slab.seal();
        let sealed = slab.sealed.get(whole.row.chunk).unwrap();
        assert_eq!(
            Some(sealed.as_ptr()),
            whole_buffer,
            "a full chunk is moved, not copied"
        );
        assert_eq!(&sealed[..], &full[..]);
    }

    /// A load charges every row byte, each opened chunk's handle, and each
    /// index entry; a budget equal to that charge admits every row.
    #[tokio::test]
    async fn load_charges_exact_slab_and_index_bytes() {
        let db = test_db("memory_store_exact_charge").await;
        let index_id = crate::search::vector::index_id_from_name("memory_store_exact_charge_idx");
        let rows = [
            (
                VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 1, 5)).to_bytes(),
                Bytes::from_static(&[1; 10]),
            ),
            (
                VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 1, 6)).to_bytes(),
                Bytes::from_static(&[2; 20]),
            ),
            (
                VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 2, 5)).to_bytes(),
                Bytes::from_static(&[3; 30]),
            ),
            (
                VectorKey::SimHash(VectorSimHashKey::new(index_id, 5)).to_bytes(),
                Bytes::copy_from_slice(&encode_simhash(5)),
            ),
            (
                VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 5)).to_bytes(),
                Bytes::from_static(&[4; 40]),
            ),
            (
                VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 7)).to_bytes(),
                Bytes::new(),
            ),
        ];
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (key, value) in &rows {
            tx.put(key, value.clone()).unwrap();
        }
        tx.commit().await.unwrap();

        let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        // A fixture row inserted before the load must not absorb charged rows.
        store.insert_upper_vector(99, Bytes::from_static(b"fixture"));
        let summary = store
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Unbounded,
                None,
            )
            .await
            .unwrap();
        let expected = 3 * UPPER_NODE_INDEX_ENTRY_BYTES
            + 2 * UPPER_LAYER_SLOT_BYTES
            + SIMHASH_INDEX_ENTRY_BYTES
            + SLAB_CHUNK_HANDLE_BYTES
            + 10
            + 20
            + 30
            + 40;
        assert_eq!(summary.loaded_entries, rows.len());
        assert_eq!(summary.estimated_bytes, expected);
        assert_eq!(store.estimated_bytes(), expected);
        assert_eq!(
            [0, 1, 2].map(|id| store.slab.sealed.get(id).map(Bytes::len)),
            [Some(b"fixture".len()), Some(100), None],
            "the fixture chunk stays separate and the load's chunk is shrunk"
        );
        assert!(store.slab.open.lock().bytes.is_none());
        assert_eq!(store.get_upper_vector(7).as_deref(), Some(&[][..]));
        assert_eq!(store.get_upper_vector(99).as_deref(), Some(&b"fixture"[..]));

        let bounded = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
        let summary = bounded
            .load_descriptor_bound_with_budget(
                db.as_ref(),
                VectorMemoryAdmissionBudget::Bounded(expected),
                None,
            )
            .await
            .unwrap();
        assert_eq!(summary.loaded_entries, rows.len());
        assert_eq!(summary.estimated_bytes, expected);
        assert_eq!(
            summary.completion,
            VectorMemoryStoreLoadCompletion::Complete
        );
    }

    /// A bounded load admits exactly the longest scan prefix whose charge,
    /// modelled row by row, fits its budget, and charges that prefix exactly.
    #[tokio::test]
    async fn bounded_loads_admit_the_longest_prefix_whose_charge_fits() {
        let db = test_db("memory_store_prefix_charges").await;
        let index_id = crate::search::vector::index_id_from_name("memory_store_prefix_charges_idx");
        let chunk = VECTOR_MEMORY_SLAB_CHUNK_BYTES;
        // Lengths cross chunk boundaries, fill a chunk exactly, exceed the
        // chunk size, and include empty rows.
        let lens = [
            0,
            1,
            700,
            chunk - 701,
            6_000,
            6_000,
            6_000,
            chunk + 3,
            64,
            0,
            9_000,
            512,
        ];
        // Persisted key, value, and the upper row it fills (none for SimHashes).
        type ScanRow = (Bytes, Bytes, Option<(NodeId, UpperRowKind)>);
        let mut rows: Vec<ScanRow> = Vec::new();
        for (node_id, len) in (0u64..).zip(lens) {
            for layer in 1..=u16::try_from(node_id % 3).unwrap() + 1 {
                let key = VectorUpperNeighborsKey::new(index_id, layer, node_id);
                rows.push((
                    VectorKey::UpperNeighbors(key).to_bytes(),
                    Bytes::from(vec![
                        u8::try_from(layer).unwrap();
                        len / 4 + usize::from(layer)
                    ]),
                    Some((node_id, UpperRowKind::Neighbors(layer))),
                ));
            }
            rows.push((
                VectorKey::SimHash(VectorSimHashKey::new(index_id, node_id)).to_bytes(),
                Bytes::copy_from_slice(&encode_simhash(node_id)),
                None,
            ));
            rows.push((
                VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, node_id)).to_bytes(),
                Bytes::from(vec![node_id.to_le_bytes()[0]; len]),
                Some((node_id, UpperRowKind::Vector)),
            ));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (key, value, _) in &rows {
            tx.put(key, value.clone()).unwrap();
        }
        tx.commit().await.unwrap();

        // prefix_charges[k] is the modelled charge of the first k scan rows.
        let mut prefix_charges = vec![0u64];
        let mut layers: HashMap<NodeId, usize> = HashMap::new();
        let mut room = 0usize;
        for (_, value, target) in &rows {
            let index = match target {
                None => SIMHASH_INDEX_ENTRY_BYTES,
                Some((node_id, kind)) => {
                    let new_node = !layers.contains_key(node_id);
                    let count = layers.entry(*node_id).or_default();
                    if matches!(kind, UpperRowKind::Neighbors(_)) {
                        *count += 1;
                    }
                    match (new_node, kind, *count) {
                        (true, ..) => UPPER_NODE_INDEX_ENTRY_BYTES,
                        (false, UpperRowKind::Neighbors(_), 2) => 2 * UPPER_LAYER_SLOT_BYTES,
                        (false, UpperRowKind::Neighbors(_), 3..) => UPPER_LAYER_SLOT_BYTES,
                        (false, ..) => 0,
                    }
                }
            };
            // SimHashes are decoded into the index, never copied to the slab.
            let len = if target.is_some() { value.len() } else { 0 };
            let slab = if len == 0 {
                0
            } else if len <= room {
                room -= len;
                len as u64
            } else {
                room = chunk.max(len) - len;
                len as u64 + SLAB_CHUNK_HANDLE_BYTES
            };
            prefix_charges.push(prefix_charges.last().unwrap() + index + slab);
        }

        let load = |budget| {
            let db = Arc::clone(&db);
            async move {
                let store = VectorMemoryStore::new(DataScope::LegacyUnscoped, index_id, u64::MAX);
                let summary = store
                    .load_descriptor_bound_with_budget(db.as_ref(), budget, None)
                    .await
                    .unwrap();
                (store, summary)
            }
        };
        let (_, full) = load(VectorMemoryAdmissionBudget::Unbounded).await;
        assert_eq!(full.loaded_entries, rows.len());
        assert_eq!(full.estimated_bytes, *prefix_charges.last().unwrap());

        for budget in prefix_charges
            .iter()
            .flat_map(|&charge| [charge, charge.saturating_sub(1)])
        {
            let admitted = prefix_charges
                .iter()
                .rposition(|&charge| charge <= budget)
                .unwrap();
            let (store, summary) = load(VectorMemoryAdmissionBudget::Bounded(budget)).await;
            assert_eq!(summary.loaded_entries, admitted, "budget {budget}");
            assert_eq!(
                summary.estimated_bytes, prefix_charges[admitted],
                "budget {budget}"
            );
            let completion = if admitted == rows.len() {
                VectorMemoryStoreLoadCompletion::Complete
            } else {
                VectorMemoryStoreLoadCompletion::BudgetExhausted
            };
            assert_eq!(summary.completion, completion, "budget {budget}");
            for (position, (_, value, target)) in rows.iter().enumerate() {
                let found = match target {
                    None => continue,
                    Some((node_id, UpperRowKind::Vector)) => store.get_upper_vector(*node_id),
                    Some((node_id, UpperRowKind::Neighbors(layer))) => {
                        store.get_upper_neighbors_bytes(*layer, *node_id)
                    }
                };
                assert_eq!(found.as_ref(), (position < admitted).then_some(value));
            }
        }
    }

    /// Readers follow rows while fixture inserts fill and seal chunks.
    #[test]
    fn concurrent_reads_follow_rows_across_chunk_seals() {
        let store = Arc::new(VectorMemoryStore::new(DataScope::LegacyUnscoped, 1, 0));
        let row = |node_id: NodeId| Bytes::from(vec![node_id.to_le_bytes()[0]; 64 * 1024]);
        let written = Arc::new(AtomicU64::new(0));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let store = Arc::clone(&store);
                let written = Arc::clone(&written);
                scope.spawn(move || {
                    while written.load(Ordering::Acquire) < 64 {
                        let visible = written.load(Ordering::Acquire);
                        for node_id in 0..visible {
                            assert_eq!(store.get_upper_vector(node_id), Some(row(node_id)));
                        }
                    }
                });
            }
            for node_id in 0..64 {
                store.insert_upper_vector(node_id, row(node_id));
                written.store(node_id + 1, Ordering::Release);
            }
        });
        assert!(store.slab.sealed.get(2).is_some());
        for node_id in 0..64 {
            assert_eq!(store.get_upper_vector(node_id), Some(row(node_id)));
        }
    }

    #[test]
    fn dirty_rows_track_nodes_and_layer_specific_neighbors() {
        let rows = VectorMemoryDirtyRows::default();

        assert!(rows.is_empty());

        rows.mark_upper_neighbors_dirty(2, 7);
        assert!(!rows.is_empty());
        assert!(rows.dirty_nodes().is_empty());

        rows.mark_node_dirty(9);
        assert_eq!(rows.dirty_nodes(), vec![9]);
        assert_eq!(rows.dirty_upper_neighbors(), vec![(2, 7)]);
    }

    #[tokio::test]
    async fn pending_dirty_guards_reference_count_rows_and_all_dirty_state() {
        let pending = Arc::new(VectorMemoryPendingDirtyRows::new());
        let rows = VectorMemoryDirtyRows::default();
        rows.mark_node_dirty(7);
        rows.mark_upper_neighbors_dirty(2, 9);

        let absent = DashMap::<NodeId, usize>::new();
        VectorMemoryPendingDirtyRows::decrement(&absent, 99);
        assert!(absent.is_empty(), "decrementing an absent row is a no-op");

        assert_eq!(pending.generation(), 0);
        assert_eq!(pending.bump_generation(), 0);
        assert_eq!(pending.generation(), 1);
        let publish_guard = pending.lock_publish().await;
        drop(publish_guard);

        assert!(!pending.has_pending_commits());
        let first = pending.acquire(&rows);
        let second = pending.acquire(&rows);
        assert!(pending.has_pending_commits());
        assert!(pending.is_node_dirty(7));
        assert!(pending.is_upper_neighbors_dirty(4, 7));
        assert!(pending.is_upper_neighbors_dirty(2, 9));
        drop(first);
        assert!(
            pending.is_node_dirty(7),
            "the second guard still owns the row"
        );
        assert!(pending.has_pending_commits());
        drop(second);
        assert!(!pending.has_pending_commits());
        assert!(!pending.is_node_dirty(7));
        assert!(!pending.is_upper_neighbors_dirty(2, 9));

        let first_all = pending.acquire_all();
        let second_all = pending.acquire_all();
        assert!(pending.is_all_dirty());
        assert!(
            !pending.has_pending_commits(),
            "retirement fences every row without counting as a storage commit"
        );

        drop(first_all);
        assert!(pending.is_all_dirty());
        drop(second_all);
        assert!(!pending.is_all_dirty());
    }
}
