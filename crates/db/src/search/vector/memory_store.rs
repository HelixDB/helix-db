//! Resident snapshot of HNSW upper-layer and SimHash rows.
//!
//! This cache is scoped to a single vector index (`index_id`) and stores only
//! upper-layer traversal data:
//! - upper neighbors (`kind=0x11`)
//! - simhash (`kind=0x12`)
//! - upper vectors (`kind=0x13`)
//!
//! Layer-0 neighbor rows intentionally remain on the normal DB/foyer path.
//! Upper neighbors are grouped node-first so deleting one entity never scans
//! unrelated cached nodes. Hydration validates, measures, and admits one row
//! at a time. A bounded partial store remains correct because absent rows fall
//! back to the caller's stable storage view without mutating the snapshot.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::{DashMap, DashSet};
use slatedb::config::ScanOptions;
use tokio::sync::{watch, Mutex, MutexGuard};

use slatedb::DbReadOps;

use super::simhash::SimHash;
use super::storage::{SimHashRow, VectorRowKeyspace, VectorRows};
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

const VECTOR_MEMORY_LOAD_MAX_FETCH_TASKS: usize = 4;
const VECTOR_MEMORY_ENTRY_OVERHEAD_BYTES: u64 = 64;

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
    /// Returns whether a checked candidate total remains within this budget.
    const fn admits(self, candidate_total: u64) -> bool {
        match self {
            Self::Unbounded => true,
            Self::Bounded(bytes) => candidate_total <= bytes,
        }
    }
}

/// Resident snapshot of upper-layer and SimHash rows for one physical index.
pub(crate) struct VectorMemoryStore {
    scope: DataScope,
    index_id: u64,
    visible_seq: u64,
    simhashes: DashMap<NodeId, SimHash>,
    upper_neighbors: DashMap<NodeId, BTreeMap<u16, Bytes>>,
    upper_vectors: DashMap<NodeId, Bytes>,
    estimated_bytes: AtomicU64,
}

/// Transaction-local set of memory-store rows modified by vector writes.
#[derive(Debug, Default)]
pub(crate) struct VectorMemoryDirtyRows {
    dirty_nodes: DashSet<NodeId>,
    dirty_upper_neighbors: DashSet<(u16, NodeId)>,
}

/// Shared, ref-counted rows that are being committed and must bypass memory cache.
pub(crate) struct VectorMemoryPendingDirtyRows {
    dirty_nodes: DashMap<NodeId, usize>,
    dirty_upper_neighbors: DashMap<(u16, NodeId), usize>,
    dirty_all: AtomicUsize,
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
pub(super) struct SimHashReadStats {
    /// Number of logical SimHash row reads.
    pub(super) reads: usize,
    /// Number of physical batch calls used by those reads.
    pub(super) multi_get_calls: usize,
    /// Time spent fetching SimHash rows from the stable read view.
    pub(super) fetch_ns: u64,
}

/// Complete resident-memory capability attached to one vector-index handle.
///
/// Each variant encodes one valid ownership mode. A handle cannot independently
/// combine a resident snapshot, dirty tracking, and pending commit fences. This
/// keeps managed snapshot lookup and transaction-local write tracking mutually
/// explicit without changing any persisted row or in-memory payload format.
pub(crate) enum VectorMemoryAccess {
    /// No shared resident store or transaction-local dirty tracker is attached.
    Uncached,
    /// A managed reader combines immutable lookup with commit-window fences.
    ReadSnapshot {
        /// Exact-index resident store retained by the managed cache read guard.
        store: Arc<VectorMemoryStore>,
        /// Rows currently committing in another retained write set.
        pending: Arc<VectorMemoryPendingDirtyRows>,
    },
    /// A managed write tracks dirty rows without consulting a resident store.
    WriteTracking {
        /// Transaction-local rows fenced at commit and discarded on abort.
        dirty: Arc<VectorMemoryDirtyRows>,
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

    /// Constructs a write handle that tracks dirty rows without a resident store.
    pub(crate) fn write_tracking(dirty: Arc<VectorMemoryDirtyRows>) -> Self {
        Self::WriteTracking { dirty }
    }

    /// Returns the resident store available to this handle, when one exists.
    pub(crate) const fn store(&self) -> Option<&Arc<VectorMemoryStore>> {
        match self {
            Self::ReadSnapshot { store, .. } => Some(store),
            Self::Uncached | Self::WriteTracking { .. } => None,
        }
    }

    /// Returns whether shared lookup is fenced for this node.
    pub(crate) fn is_node_dirty(&self, node_id: NodeId) -> bool {
        match self {
            Self::ReadSnapshot { pending, .. } => pending.is_node_dirty(node_id),
            Self::WriteTracking { dirty } => dirty.is_node_dirty(node_id),
            Self::Uncached => false,
        }
    }

    /// Returns whether shared lookup is fenced for one upper-neighbor row.
    pub(crate) fn is_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) -> bool {
        match self {
            Self::ReadSnapshot { pending, .. } => pending.is_upper_neighbors_dirty(layer, node_id),
            Self::WriteTracking { dirty } => dirty.is_upper_neighbors_dirty(layer, node_id),
            Self::Uncached => false,
        }
    }

    /// Marks a node unsafe for shared lookup in the current write transaction.
    pub(crate) fn mark_node_dirty(&self, node_id: NodeId) {
        match self {
            Self::WriteTracking { dirty } => dirty.mark_node_dirty(node_id),
            Self::Uncached | Self::ReadSnapshot { .. } => {}
        }
    }

    /// Marks one upper-neighbor row unsafe for shared lookup in the current write.
    pub(crate) fn mark_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) {
        match self {
            Self::WriteTracking { dirty } => dirty.mark_upper_neighbors_dirty(layer, node_id),
            Self::Uncached | Self::ReadSnapshot { .. } => {}
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
    pub(super) async fn fill_simhash_cache<const COLLECT_TIMING: bool, R>(
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
    pub(super) async fn read_simhash_rows_counted<const COLLECT_TIMING: bool, R>(
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

    pub(crate) fn is_node_dirty(&self, node_id: NodeId) -> bool {
        self.dirty_nodes.contains(&node_id)
    }

    pub(crate) fn is_upper_neighbors_dirty(&self, layer: u16, node_id: NodeId) -> bool {
        self.dirty_nodes.contains(&node_id)
            || self.dirty_upper_neighbors.contains(&(layer, node_id))
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
            generation: AtomicU64::new(0),
            publish_lock: Mutex::new(()),
        }
    }

    pub(crate) fn acquire(
        self: &Arc<Self>,
        dirty_rows: &VectorMemoryDirtyRows,
    ) -> VectorMemoryPendingDirtyGuard {
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

    pub(crate) fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
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
        if self.dirty_all {
            self.pending.dirty_all.fetch_sub(1, Ordering::AcqRel);
        }
        for &node_id in &self.dirty_nodes {
            VectorMemoryPendingDirtyRows::decrement(&self.pending.dirty_nodes, node_id);
        }
        for &row in &self.dirty_upper_neighbors {
            VectorMemoryPendingDirtyRows::decrement(&self.pending.dirty_upper_neighbors, row);
        }
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
            upper_neighbors: DashMap::new(),
            upper_vectors: DashMap::new(),
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

    /// Return whether this store can be used by a write transaction snapshot.
    #[cfg(any(test, feature = "production-coverage"))]
    pub fn is_usable_for_writer_snapshot(&self, snapshot_seq: u64) -> bool {
        self.visible_seq <= snapshot_seq
    }

    /// Return the estimated resident bytes populated by the last load.
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    /// SimHash lookup.
    pub fn get_simhash(&self, node_id: NodeId) -> Option<SimHash> {
        self.simhashes.get(&node_id).map(|entry| *entry)
    }

    /// SimHash upsert.
    pub fn insert_simhash(&self, node_id: NodeId, hash: SimHash) {
        self.simhashes.insert(node_id, hash);
    }

    /// Remove SimHash entry.
    pub fn remove_simhash(&self, node_id: NodeId) {
        self.simhashes.remove(&node_id);
    }

    /// Upper-neighbor raw-bytes lookup.
    pub fn get_upper_neighbors_bytes(&self, layer: u16, node_id: NodeId) -> Option<Bytes> {
        self.upper_neighbors
            .get(&node_id)
            .and_then(|layers| layers.get(&layer).cloned())
    }

    /// Upper-neighbor raw-bytes upsert.
    pub fn insert_upper_neighbors_bytes(&self, layer: u16, node_id: NodeId, bytes: Bytes) {
        match self.upper_neighbors.entry(node_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(layer, bytes);
            }
            Entry::Vacant(entry) => {
                entry.insert(BTreeMap::from([(layer, bytes)]));
            }
        }
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
        let Entry::Occupied(mut entry) = self.upper_neighbors.entry(node_id) else {
            return;
        };
        entry.get_mut().remove(&layer);
        if entry.get().is_empty() {
            entry.remove();
        }
    }

    /// Remove all upper-neighbor rows for this node across all layers.
    pub fn remove_upper_neighbors_for_node(&self, node_id: NodeId) {
        self.upper_neighbors.remove(&node_id);
    }

    /// Upper-vector raw-bytes lookup.
    pub fn get_upper_vector(&self, node_id: NodeId) -> Option<Bytes> {
        self.upper_vectors.get(&node_id).map(|entry| entry.clone())
    }

    /// Upper-vector raw-bytes upsert.
    pub fn insert_upper_vector(&self, node_id: NodeId, bytes: Bytes) {
        self.upper_vectors.insert(node_id, bytes);
    }

    /// Remove upper-vector row.
    pub fn remove_upper_vector(&self, node_id: NodeId) {
        self.upper_vectors.remove(&node_id);
    }

    /// Remove all cache rows for one node.
    pub fn remove_node(&self, node_id: NodeId) {
        self.remove_simhash(node_id);
        self.remove_upper_vector(node_id);
        self.remove_upper_neighbors_for_node(node_id);
    }

    /// Remove every row from this store.
    pub fn clear(&self) {
        self.simhashes.clear();
        self.upper_vectors.clear();
        self.upper_neighbors.clear();
        self.estimated_bytes.store(0, Ordering::Relaxed);
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
            let row_bytes = estimated_entry_bytes(kv.key.len(), row.value_len())?;
            let Some(candidate_total) = estimated_bytes.checked_add(row_bytes) else {
                return Err(HelixDbError::InvariantViolation(
                    "vector memory admission byte count overflowed".to_string(),
                ));
            };
            if !budget.admits(candidate_total) {
                completion = VectorMemoryStoreLoadCompletion::BudgetExhausted;
                break;
            }
            row.insert_into(self);
            let Some(next_loaded) = loaded.checked_add(1) else {
                return Err(HelixDbError::InvariantViolation(
                    "vector memory admitted entry count overflowed".to_string(),
                ));
            };
            loaded = next_loaded;
            estimated_bytes = candidate_total;
        }

        self.estimated_bytes
            .store(estimated_bytes, Ordering::Relaxed);
        Ok(VectorMemoryStoreLoadSummary {
            loaded_entries: loaded,
            estimated_bytes,
            completion,
        })
    }
}

#[inline]
/// Computes the checked resident estimate before a row is admitted.
fn estimated_entry_bytes(key_len: usize, value_len: usize) -> Result<u64, HelixDbError> {
    let Ok(key_len) = u64::try_from(key_len) else {
        return Err(HelixDbError::InvariantViolation(
            "vector memory key length exceeds u64".to_string(),
        ));
    };
    let Ok(value_len) = u64::try_from(value_len) else {
        return Err(HelixDbError::InvariantViolation(
            "vector memory value length exceeds u64".to_string(),
        ));
    };
    let Some(estimated) = key_len
        .checked_add(value_len)
        .and_then(|bytes| bytes.checked_add(VECTOR_MEMORY_ENTRY_OVERHEAD_BYTES))
    else {
        return Err(HelixDbError::InvariantViolation(
            "vector memory entry byte estimate overflowed".to_string(),
        ));
    };
    Ok(estimated)
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

impl VectorMemoryHydrationRow {
    /// Returns the current persisted value length used by admission accounting.
    fn value_len(&self) -> usize {
        match self {
            Self::Neighbors { value, .. } | Self::Vector { value, .. } => value.len(),
            Self::SimHash { .. } => core::mem::size_of::<u64>(),
        }
    }

    /// Moves one admitted row into its node-primary resident collection.
    fn insert_into(self, store: &VectorMemoryStore) {
        match self {
            Self::Neighbors {
                layer,
                node_id,
                value,
            } => store.insert_upper_neighbors_bytes(layer, node_id, value),
            Self::SimHash { node_id, value } => store.insert_simhash(node_id, value),
            Self::Vector { node_id, value } => store.insert_upper_vector(node_id, value),
        }
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/memory_store.rs"]
pub(crate) mod production_contracts;

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

    /// Proves managed reads and writes carry only their respective fence state.
    #[test]
    fn memory_access_tracks_write_local_and_read_pending_dirty_rows() {
        let store = Arc::new(VectorMemoryStore::new(DataScope::LegacyUnscoped, 42, 10));
        let dirty = Arc::new(VectorMemoryDirtyRows::default());
        let pending = Arc::new(VectorMemoryPendingDirtyRows::new());
        let pending_source = VectorMemoryDirtyRows::default();
        pending_source.mark_upper_neighbors_dirty(3, 11);
        let _pending_guard = pending.acquire(&pending_source);

        let local_access = VectorMemoryAccess::write_tracking(Arc::clone(&dirty));
        local_access.mark_node_dirty(7);
        local_access.mark_upper_neighbors_dirty(2, 9);
        assert!(local_access.store().is_none());
        assert!(local_access.is_node_dirty(7));
        assert!(local_access.is_upper_neighbors_dirty(2, 9));

        let pending_access = VectorMemoryAccess::read_snapshot(Arc::clone(&store), pending);
        assert!(pending_access
            .store()
            .is_some_and(|attached| Arc::ptr_eq(attached, &store)));
        assert!(pending_access.is_upper_neighbors_dirty(3, 11));
    }

    /// Proves writes can fence rows even before a resident store is published.
    #[test]
    fn memory_access_write_tracking_has_dirty_state_without_store() {
        let dirty = Arc::new(VectorMemoryDirtyRows::default());
        let access = VectorMemoryAccess::write_tracking(Arc::clone(&dirty));

        access.mark_node_dirty(7);
        access.mark_upper_neighbors_dirty(2, 9);

        assert!(access.store().is_none());
        assert!(access.is_node_dirty(7));
        assert!(access.is_upper_neighbors_dirty(2, 9));
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
        let index_id = super::super::index_id_from_name("memory_store_hydrates_rows_idx");
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
        let index_id = super::super::index_id_from_name("memory_store_skips_invalid_rows_idx");

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
        let index_id = super::super::index_id_from_name("memory_store_shutdown_short_circuit_idx");

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
        let index_id = super::super::index_id_from_name("memory_store_incremental_admission_idx");
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

        let first_row_bytes = estimated_entry_bytes(first_key.len(), first_value.len()).unwrap();
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
        assert_eq!(store.upper_neighbors.len(), 100_000);

        store.remove_node(50_000);

        assert_eq!(store.upper_neighbors.len(), 99_999);
        assert!(store.get_upper_neighbors_bytes(1, 49_999).is_some());
        assert!(store.get_upper_neighbors_bytes(1, 50_000).is_none());
        assert!(store.get_upper_neighbors_bytes(2, 50_000).is_none());
        assert!(store.get_upper_neighbors_bytes(1, 50_001).is_some());
    }

    #[test]
    fn dirty_rows_track_nodes_and_layer_specific_neighbors() {
        let rows = VectorMemoryDirtyRows::default();

        assert!(rows.is_empty());
        assert!(!rows.is_node_dirty(7));
        assert!(!rows.is_upper_neighbors_dirty(2, 7));

        rows.mark_upper_neighbors_dirty(2, 7);
        assert!(!rows.is_empty());
        assert!(rows.is_upper_neighbors_dirty(2, 7));
        assert!(!rows.is_upper_neighbors_dirty(3, 7));

        rows.mark_node_dirty(9);
        assert!(rows.is_node_dirty(9));
        assert!(rows.is_upper_neighbors_dirty(1, 9));
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
        pending.bump_generation();
        assert_eq!(pending.generation(), 1);
        let publish_guard = pending.lock_publish().await;
        drop(publish_guard);

        let first = pending.acquire(&rows);
        let second = pending.acquire(&rows);
        assert!(pending.is_node_dirty(7));
        assert!(pending.is_upper_neighbors_dirty(4, 7));
        assert!(pending.is_upper_neighbors_dirty(2, 9));
        drop(first);
        assert!(
            pending.is_node_dirty(7),
            "the second guard still owns the row"
        );
        drop(second);
        assert!(!pending.is_node_dirty(7));
        assert!(!pending.is_upper_neighbors_dirty(2, 9));

        let first_all = pending.acquire_all();
        let second_all = pending.acquire_all();
        assert!(pending.is_all_dirty());

        drop(first_all);
        assert!(pending.is_all_dirty());
        drop(second_all);
        assert!(!pending.is_all_dirty());
    }
}
