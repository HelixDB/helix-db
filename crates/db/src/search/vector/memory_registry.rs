//! Descriptor-bound lifecycle registry for vector memory-cache generations.
//!
//! [`VectorCacheRegistry`] is the only owner of cache entries whose identity is
//! derived from a [`ValidatedVectorGenerationHandle`]. Each entry follows the
//! explicit `Hydrating -> Ready -> Retiring -> Closed` lifecycle. Any operation
//! must hold a [`VectorCacheReadGuard`] while using its store; retirement
//! changes the entry to `Retiring`, rejects new guards, waits for existing
//! guards and hydration to finish, acquires the independent all-dirty
//! publication guard, clears the exact store, and only then reaches `Closed`.
//!
//! Closed entries deliberately remain registered until physical cleanup calls
//! [`VectorCacheRegistry::forget_closed`]. That tombstone prevents a concurrent
//! or stale background hydration from reopening a generation after retirement
//! but before its current-format rows have been deleted. The registry is
//! process-local disposable state and writes no database key or value.

use std::collections::{hash_map, HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use super::memory_store::{
    VectorMemoryDirtyRows, VectorMemoryPendingDirtyGuard, VectorMemoryPendingDirtyRows,
    VectorMemoryStore,
};
use super::{ValidatedVectorCleanupAuthority, ValidatedVectorGenerationHandle};
use crate::encoding::keys::scope::DataScope;

/// Complete canonical-record identity for one vector cache generation.
///
/// Construction is intentionally private to [`ValidatedVectorGenerationHandle`]
/// projection, so callers cannot combine an index ID, generation, scope, or
/// semantic field from different sources.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct VectorCacheIdentity {
    scope: DataScope,
    index_id: crate::index_lifecycle::IndexId,
    generation: crate::index_lifecycle::IndexGenerationId,
    physical_index_id: crate::index_lifecycle::VectorPhysicalIndexId,
    record_revision: crate::index_lifecycle::IndexRevision,
}

impl VectorCacheIdentity {
    /// Projects the complete generation identity into an opaque cache-map key.
    pub(crate) fn from_validated(handle: &ValidatedVectorGenerationHandle) -> Self {
        let identity = handle.identity();
        Self {
            scope: identity.scope(),
            index_id: identity.index_id(),
            generation: identity.generation(),
            physical_index_id: identity.physical_index_id(),
            record_revision: identity.record_revision(),
        }
    }

    /// Returns the exact data scope bound by the validated generation.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Returns the current-format `u64` namespace derived from the full name.
    pub(crate) const fn physical_index_id(&self) -> u64 {
        self.physical_index_id.get()
    }

    /// Returns the non-zero lifecycle generation in this complete identity.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn generation(&self) -> crate::index_lifecycle::IndexGenerationId {
        self.generation
    }

    /// Returns the stable logical index owning this cache entry.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn index_id(&self) -> crate::index_lifecycle::IndexId {
        self.index_id
    }

    /// Returns the exact canonical revision that authorized admission.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn record_revision(&self) -> crate::index_lifecycle::IndexRevision {
        self.record_revision
    }
}

/// Generation-wide fence installed before any partition namespace is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct VectorCacheGenerationFence {
    scope: DataScope,
    index_id: crate::index_lifecycle::IndexId,
    generation: crate::index_lifecycle::IndexGenerationId,
}

impl VectorCacheGenerationFence {
    fn from_identity(identity: &VectorCacheIdentity) -> Self {
        Self {
            scope: identity.scope,
            index_id: identity.index_id,
            generation: identity.generation,
        }
    }

    fn from_cleanup(authority: &ValidatedVectorCleanupAuthority) -> Self {
        Self {
            scope: authority.scope(),
            index_id: authority.index_id(),
            generation: authority.generation(),
        }
    }

    fn matches(self, identity: &VectorCacheIdentity) -> bool {
        self == Self::from_identity(identity)
    }
}

/// Runtime state of one complete vector cache identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheLifecycle {
    /// A store is being populated but is not visible to readers.
    Hydrating,
    /// A fully published store may issue cache read guards.
    Ready,
    /// New guards are rejected while hydration/readers/publication drain.
    Retiring,
    /// The store is cleared and may no longer issue guards or be hydrated.
    Closed,
}

struct ResidentVectorCache {
    store: Arc<VectorMemoryStore>,
    active_readers: usize,
    refresh_inflight: bool,
}

enum VectorCacheEntryState {
    Hydrating,
    Ready(ResidentVectorCache),
    Retiring {
        resident: Option<ResidentVectorCache>,
        hydration_inflight: bool,
    },
    Closed,
}

impl VectorCacheEntryState {
    /// Projects private payload state into the externally testable lifecycle.
    const fn lifecycle(&self) -> VectorCacheLifecycle {
        match self {
            Self::Hydrating => VectorCacheLifecycle::Hydrating,
            Self::Ready(_) => VectorCacheLifecycle::Ready,
            Self::Retiring { .. } => VectorCacheLifecycle::Retiring,
            Self::Closed => VectorCacheLifecycle::Closed,
        }
    }
}

/// One complete-identity cache entry with reader and publication coordination.
pub(crate) struct VectorMemoryCacheEntry {
    identity: VectorCacheIdentity,
    pending_dirty: Arc<VectorMemoryPendingDirtyRows>,
    state: Mutex<VectorCacheEntryState>,
    changed: Notify,
}

impl VectorMemoryCacheEntry {
    /// Creates an unpublished entry for a single validated generation.
    fn hydrating(identity: VectorCacheIdentity) -> Self {
        Self {
            identity,
            pending_dirty: Arc::new(VectorMemoryPendingDirtyRows::new()),
            state: Mutex::new(VectorCacheEntryState::Hydrating),
            changed: Notify::new(),
        }
    }

    /// Creates the non-readable tombstone used when drop beats cache admission.
    fn closed(identity: VectorCacheIdentity) -> Self {
        Self {
            identity,
            pending_dirty: Arc::new(VectorMemoryPendingDirtyRows::new()),
            state: Mutex::new(VectorCacheEntryState::Closed),
            changed: Notify::new(),
        }
    }

    /// Returns the complete identity that every read guard retains.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn identity(&self) -> &VectorCacheIdentity {
        &self.identity
    }

    /// Returns the current typed lifecycle without exposing resident payloads.
    pub(crate) fn lifecycle(&self) -> VectorCacheLifecycle {
        self.state.lock().lifecycle()
    }

    fn estimated_bytes(&self) -> u64 {
        match &*self.state.lock() {
            VectorCacheEntryState::Ready(resident) => resident.store.estimated_bytes(),
            VectorCacheEntryState::Hydrating
            | VectorCacheEntryState::Retiring { .. }
            | VectorCacheEntryState::Closed => 0,
        }
    }

    /// Publishes a completely hydrated store or discards it after retirement.
    ///
    /// Hydration callers must build the store off-entry and invoke this exactly
    /// once. If drop changed the entry to `Retiring`, the unpublished store is
    /// cleared and retirement is notified instead of exposing partial or stale
    /// rows. Calling this in `Ready` or `Closed` is an invariant violation.
    pub(crate) fn finish_hydration(&self, store: Arc<VectorMemoryStore>) -> bool {
        assert_eq!(store.scope(), self.identity.scope());
        assert_eq!(store.index_id(), self.identity.physical_index_id());
        let published = {
            let mut state = self.state.lock();
            match &mut *state {
                VectorCacheEntryState::Hydrating => {
                    *state = VectorCacheEntryState::Ready(ResidentVectorCache {
                        store,
                        active_readers: 0,
                        refresh_inflight: false,
                    });
                    true
                }
                VectorCacheEntryState::Retiring {
                    hydration_inflight, ..
                } => {
                    store.clear();
                    *hydration_inflight = false;
                    false
                }
                VectorCacheEntryState::Ready(_) | VectorCacheEntryState::Closed => {
                    panic!("vector cache hydration may finish exactly once")
                }
            }
        };
        self.changed.notify_waiters();
        published
    }

    /// Cancels unpublished initial hydration and wakes retirement waiters.
    ///
    /// Background hydration owns this transition through an RAII permit. A
    /// dropped permit cannot strand the entry in `Hydrating`; an active caller
    /// may subsequently forget the closed failed entry and retry on a later
    /// pass while the shared lifecycle gate still protects the generation.
    fn cancel_initial_hydration(&self) {
        {
            let mut state = self.state.lock();
            match &mut *state {
                VectorCacheEntryState::Hydrating => {
                    *state = VectorCacheEntryState::Closed;
                }
                VectorCacheEntryState::Retiring {
                    hydration_inflight, ..
                } => {
                    *hydration_inflight = false;
                }
                VectorCacheEntryState::Ready(_) | VectorCacheEntryState::Closed => {}
            }
        }
        self.changed.notify_waiters();
    }

    /// Claims the single immutable refresh slot while readers retain the old store.
    fn begin_refresh(self: &Arc<Self>) -> Option<VectorCacheRefresh> {
        {
            let mut state = self.state.lock();
            let VectorCacheEntryState::Ready(resident) = &mut *state else {
                return None;
            };
            if resident.refresh_inflight {
                return None;
            }
            resident.refresh_inflight = true;
        }
        Some(VectorCacheRefresh {
            entry: Arc::clone(self),
            observed_dirty_generation: self.pending_dirty.generation(),
            completed: false,
        })
    }

    /// Releases a refresh reservation without changing the published store.
    fn cancel_refresh(&self) {
        {
            let mut state = self.state.lock();
            match &mut *state {
                VectorCacheEntryState::Ready(resident) => {
                    resident.refresh_inflight = false;
                }
                VectorCacheEntryState::Retiring {
                    hydration_inflight, ..
                } => {
                    *hydration_inflight = false;
                }
                VectorCacheEntryState::Closed => {}
                VectorCacheEntryState::Hydrating => {
                    unreachable!("a refresh reservation starts only from Ready")
                }
            }
        }
        self.changed.notify_waiters();
    }

    /// Acquires active-reader ownership only from a fully ready generation.
    ///
    /// The guard retains both the exact identity and immutable store `Arc`.
    /// `Hydrating`, `Retiring`, and `Closed` are explicit non-readable states;
    /// callers fall back to storage rather than guessing cache compatibility.
    pub(crate) fn acquire_read_guard(
        self: &Arc<Self>,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        let store = {
            let mut state = self.state.lock();
            let VectorCacheEntryState::Ready(resident) = &mut *state else {
                return Err(VectorCacheReadGuardError::Unavailable(state.lifecycle()));
            };
            resident.active_readers = resident
                .active_readers
                .checked_add(1)
                .expect("process-local vector cache reader count cannot overflow");
            Arc::clone(&resident.store)
        };
        Ok(VectorCacheReadGuard {
            entry: Arc::clone(self),
            store,
        })
    }

    /// Retires, drains, clears, and closes this exact generation entry.
    ///
    /// The first call atomically rejects new guards. It then waits for any
    /// unpublished hydration and active guards, independently acquires the
    /// all-dirty guard plus publication lock, clears the store, and publishes
    /// `Closed`. Cancellation leaves `Retiring`, so a retry resumes safely.
    async fn retire(&self) {
        {
            let mut state = self.state.lock();
            match &mut *state {
                VectorCacheEntryState::Hydrating => {
                    *state = VectorCacheEntryState::Retiring {
                        resident: None,
                        hydration_inflight: true,
                    };
                }
                VectorCacheEntryState::Ready(_) => {
                    let VectorCacheEntryState::Ready(mut resident) =
                        std::mem::replace(&mut *state, VectorCacheEntryState::Closed)
                    else {
                        unreachable!("ready state was matched before replacement")
                    };
                    let hydration_inflight = resident.refresh_inflight;
                    resident.refresh_inflight = false;
                    *state = VectorCacheEntryState::Retiring {
                        resident: Some(resident),
                        hydration_inflight,
                    };
                }
                VectorCacheEntryState::Retiring { .. } => {}
                VectorCacheEntryState::Closed => return,
            }
        }
        self.changed.notify_waiters();

        loop {
            let changed = self.changed.notified();
            let drained = {
                let state = self.state.lock();
                match &*state {
                    VectorCacheEntryState::Retiring {
                        resident,
                        hydration_inflight,
                    } => {
                        !*hydration_inflight
                            && resident
                                .as_ref()
                                .is_none_or(|resident| resident.active_readers == 0)
                    }
                    VectorCacheEntryState::Closed => return,
                    VectorCacheEntryState::Hydrating | VectorCacheEntryState::Ready(_) => {
                        unreachable!("retirement cannot return to a guard-admitting state")
                    }
                }
            };
            if drained {
                break;
            }
            changed.await;
        }

        let _all_dirty = self.pending_dirty.acquire_all();
        let _publication = self.pending_dirty.lock_publish().await;
        {
            let mut state = self.state.lock();
            let VectorCacheEntryState::Retiring {
                resident,
                hydration_inflight: false,
            } = &mut *state
            else {
                if matches!(&*state, VectorCacheEntryState::Closed) {
                    return;
                }
                unreachable!("retirement drain proof changed before close")
            };
            if let Some(resident) = resident.take() {
                assert_eq!(resident.active_readers, 0);
                resident.store.clear();
            }
            *state = VectorCacheEntryState::Closed;
        }
        self.changed.notify_waiters();
    }
}

/// Exclusive ownership of an unpublished first store for one generation.
pub(crate) struct VectorCacheInitialHydration {
    entry: Arc<VectorMemoryCacheEntry>,
    observed_dirty_generation: u64,
    completed: bool,
}

impl VectorCacheInitialHydration {
    /// Publishes the first immutable store if no commit crossed its snapshot.
    pub(crate) async fn finish(mut self, store: Arc<VectorMemoryStore>) -> bool {
        let _publication = self.entry.pending_dirty.lock_publish().await;
        if self.entry.pending_dirty.generation() != self.observed_dirty_generation {
            store.clear();
            self.entry.cancel_initial_hydration();
            self.completed = true;
            return false;
        }
        let published = self.entry.finish_hydration(store);
        self.completed = true;
        published
    }
}

impl Drop for VectorCacheInitialHydration {
    fn drop(&mut self) {
        if !self.completed {
            self.entry.cancel_initial_hydration();
        }
    }
}

/// Exclusive refresh reservation retaining the currently published store.
pub(crate) struct VectorCacheRefresh {
    entry: Arc<VectorMemoryCacheEntry>,
    observed_dirty_generation: u64,
    completed: bool,
}

impl VectorCacheRefresh {
    /// Atomically publishes a newer immutable store under the commit lock.
    ///
    /// Existing guards keep their previous `Arc`. Equal visibility may replace
    /// a store to enforce a smaller admission share; older visibility is
    /// discarded. A commit-generation change or retirement always wins.
    pub(crate) async fn finish(mut self, store: Arc<VectorMemoryStore>) -> bool {
        assert_eq!(store.scope(), self.entry.identity.scope());
        assert_eq!(store.index_id(), self.entry.identity.physical_index_id());
        let _publication = self.entry.pending_dirty.lock_publish().await;
        if self.entry.pending_dirty.generation() != self.observed_dirty_generation {
            store.clear();
            self.entry.cancel_refresh();
            self.completed = true;
            return false;
        }
        let published = {
            let mut state = self.entry.state.lock();
            match &mut *state {
                VectorCacheEntryState::Ready(resident) => {
                    assert!(resident.refresh_inflight);
                    resident.refresh_inflight = false;
                    if store.visible_seq() >= resident.store.visible_seq() {
                        resident.store = store;
                        true
                    } else {
                        store.clear();
                        false
                    }
                }
                VectorCacheEntryState::Retiring {
                    hydration_inflight, ..
                } => {
                    store.clear();
                    *hydration_inflight = false;
                    false
                }
                VectorCacheEntryState::Closed => {
                    store.clear();
                    false
                }
                VectorCacheEntryState::Hydrating => {
                    unreachable!("a refresh reservation cannot return to Hydrating")
                }
            }
        };
        self.completed = true;
        self.entry.changed.notify_waiters();
        published
    }
}

impl Drop for VectorCacheRefresh {
    fn drop(&mut self) {
        if !self.completed {
            self.entry.cancel_refresh();
        }
    }
}

/// Typed result of attempting one single-flight hydration reservation.
pub(crate) enum VectorCacheHydration {
    /// The caller owns the first unpublished store for this identity.
    Initial(VectorCacheInitialHydration),
    /// The caller may build a replacement while readers retain the old store.
    Refresh(VectorCacheRefresh),
    /// Another owner is hydrating/refreshing or the generation is retiring/closed.
    Unavailable(VectorCacheLifecycle),
}

/// Active-reader ownership for one exact vector cache generation.
pub(crate) struct VectorCacheReadGuard {
    entry: Arc<VectorMemoryCacheEntry>,
    store: Arc<VectorMemoryStore>,
}

impl VectorCacheReadGuard {
    /// Returns the exact identity whose active-reader count this guard owns.
    #[cfg(feature = "production-coverage")]
    pub(crate) fn identity(&self) -> &VectorCacheIdentity {
        self.entry.identity()
    }

    /// Returns the resident store while retaining active-reader ownership.
    pub(crate) fn store(&self) -> &Arc<VectorMemoryStore> {
        &self.store
    }

    /// Returns shared commit-window dirty tracking for cache bypass checks.
    pub(crate) fn pending_dirty(&self) -> &Arc<VectorMemoryPendingDirtyRows> {
        &self.entry.pending_dirty
    }
}

/// Pre-commit fence for one exact generation's transaction-local dirty rows.
pub(crate) struct VectorCachePendingCommit {
    entry: Arc<VectorMemoryCacheEntry>,
    dirty_rows: Arc<VectorMemoryDirtyRows>,
    _pending_guard: VectorMemoryPendingDirtyGuard,
}

/// Storage evidence authorizing cache eviction after a graph transaction.
enum VectorCacheCommitEvidence {
    /// SlateDB returned the exact committed sequence in its write handle.
    Sequence(u64),
}

impl VectorCachePendingCommit {
    /// Evicts committed rows while holding the entry publication lock.
    ///
    /// The storage commit sequence is supplied by SlateDB's `WriteHandle`, not
    /// synthesized. This phase deliberately publishes no replacement rows: it
    /// evicts only, so a later snapshot either uses an independently hydrated
    /// exact-sequence store or falls back to storage.
    pub(crate) async fn evict_after_commit(self, committed_sequence: u64) {
        self.evict(VectorCacheCommitEvidence::Sequence(committed_sequence))
            .await;
    }

    /// Evicts the exact dirty rows under their publication fence.
    async fn evict(self, evidence: VectorCacheCommitEvidence) {
        let _publication = self.entry.pending_dirty.lock_publish().await;
        {
            let state = self.entry.state.lock();
            let resident = match &*state {
                VectorCacheEntryState::Ready(resident)
                | VectorCacheEntryState::Retiring {
                    resident: Some(resident),
                    ..
                } => resident,
                VectorCacheEntryState::Hydrating
                | VectorCacheEntryState::Retiring { resident: None, .. }
                | VectorCacheEntryState::Closed => {
                    drop(state);
                    self.entry.pending_dirty.bump_generation();
                    return;
                }
            };
            match evidence {
                VectorCacheCommitEvidence::Sequence(committed_sequence) => {
                    debug_assert!(resident.store.visible_seq() <= committed_sequence);
                }
            }
            for node_id in self.dirty_rows.dirty_nodes() {
                resident.store.remove_node(node_id);
            }
            for (layer, node_id) in self.dirty_rows.dirty_upper_neighbors() {
                resident.store.remove_upper_neighbors(layer, node_id);
            }
        }
        self.entry.pending_dirty.bump_generation();
    }
}

impl Drop for VectorCacheReadGuard {
    fn drop(&mut self) {
        {
            let mut state = self.entry.state.lock();
            let resident = match &mut *state {
                VectorCacheEntryState::Ready(resident)
                | VectorCacheEntryState::Retiring {
                    resident: Some(resident),
                    ..
                } => resident,
                VectorCacheEntryState::Hydrating
                | VectorCacheEntryState::Retiring { resident: None, .. }
                | VectorCacheEntryState::Closed => {
                    unreachable!("a live vector cache read guard must retain resident state")
                }
            };
            resident.active_readers = resident
                .active_readers
                .checked_sub(1)
                .expect("a vector cache read guard releases one acquired reader");
        }
        self.entry.changed.notify_waiters();
    }
}

/// Reason an exact vector cache entry cannot issue a read guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum VectorCacheReadGuardError {
    /// No cache entry has been admitted for this exact descriptor identity.
    #[error("vector cache generation is absent")]
    Absent,
    /// Only `Ready` entries may be read; callers should use durable storage.
    #[error("vector cache generation is not readable while {0:?}")]
    Unavailable(VectorCacheLifecycle),
}

/// Outcome of closing an exact generation in the process-local registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheRetirement {
    /// Drop installed a closed tombstone before any cache admission occurred.
    ClosedEmpty,
    /// A hydrating or resident matching entry was drained and closed.
    ClosedResident,
}

/// Registry keyed by the complete validated vector generation descriptor.
#[derive(Default)]
struct VectorCacheRegistryState {
    entries: HashMap<VectorCacheIdentity, Arc<VectorMemoryCacheEntry>>,
    retired_generations: HashSet<VectorCacheGenerationFence>,
}

/// Atomic owner of exact cache entries and generation-wide retirement fences.
#[derive(Default)]
pub(crate) struct VectorCacheRegistry {
    state: RwLock<VectorCacheRegistryState>,
}

impl VectorCacheRegistry {
    /// Sum the approximate bytes in currently published resident stores.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        self.state
            .read()
            .entries
            .values()
            .fold(0_u64, |total, entry| {
                total.saturating_add(entry.estimated_bytes())
            })
    }

    /// Claims initial hydration or refresh ownership for an exact descriptor.
    ///
    /// This is the background loader's only entry point. It prevents duplicate
    /// work per identity and represents unavailable lifecycle states explicitly
    /// instead of returning an entry plus loosely related booleans.
    pub(crate) fn prepare_hydration(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> VectorCacheHydration {
        let (entry, owns_initial) = self.entry_for(handle);
        if owns_initial {
            let observed_dirty_generation = entry.pending_dirty.generation();
            return VectorCacheHydration::Initial(VectorCacheInitialHydration {
                entry,
                observed_dirty_generation,
                completed: false,
            });
        }
        match entry.begin_refresh() {
            Some(refresh) => VectorCacheHydration::Refresh(refresh),
            None => VectorCacheHydration::Unavailable(entry.lifecycle()),
        }
    }

    /// Acquires pending dirty ownership before one storage commit.
    ///
    /// Absent entries need no fence. Empty write sets likewise return `None`.
    /// The returned guard must live across `DbTransaction::commit`; dropping it
    /// after a conflict or abort publishes nothing.
    pub(crate) fn prepare_commit(
        &self,
        write: &super::write_cache::VectorCacheWriteEntry,
    ) -> Option<VectorCachePendingCommit> {
        let dirty_rows = write.dirty_rows()?;
        if dirty_rows.is_empty() {
            return None;
        }
        let identity = VectorCacheIdentity::from_validated(write.handle());
        let entry = self.state.read().entries.get(&identity).cloned()?;
        let pending_guard = entry.pending_dirty.acquire(dirty_rows);
        Some(VectorCachePendingCommit {
            entry,
            dirty_rows: Arc::clone(dirty_rows),
            _pending_guard: pending_guard,
        })
    }

    /// Acquires a cache read guard only for an already admitted exact generation.
    ///
    /// Read factories use this non-creating lookup so a cache miss cannot
    /// accidentally claim hydration ownership. `Hydrating`, `Retiring`, and
    /// `Closed` entries remain explicit storage-fallback states.
    pub(crate) fn read_guard_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        let identity = VectorCacheIdentity::from_validated(handle);
        let Some(entry) = self.state.read().entries.get(&identity).cloned() else {
            return Err(VectorCacheReadGuardError::Absent);
        };
        entry.acquire_read_guard()
    }

    /// Returns the single hydrating/ready/retiring/closed entry for `handle`.
    ///
    /// The boolean is true only for the caller that inserted a new `Hydrating`
    /// entry and therefore owns hydration. Closed entries are returned rather
    /// than replaced; recreation must carry a distinct lifecycle generation.
    pub(crate) fn entry_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> (Arc<VectorMemoryCacheEntry>, bool) {
        let identity = VectorCacheIdentity::from_validated(handle);
        let mut state = self.state.write();
        let retired = state
            .retired_generations
            .contains(&VectorCacheGenerationFence::from_identity(&identity));
        match state.entries.entry(identity) {
            hash_map::Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
            hash_map::Entry::Vacant(entry) => {
                let cache_entry = Arc::new(if retired {
                    VectorMemoryCacheEntry::closed(entry.key().clone())
                } else {
                    VectorMemoryCacheEntry::hydrating(entry.key().clone())
                });
                entry.insert(Arc::clone(&cache_entry));
                (cache_entry, !retired)
            }
        }
    }

    /// Closes the exact descriptor identity and retains a closed tombstone.
    ///
    /// Drop calls this before the first physical delete. Existing cache read
    /// guards keep their immutable `Arc` alive while new guards are rejected.
    /// If no cache was admitted, a closed tombstone still prevents hydration
    /// from racing later physical cleanup.
    pub(crate) async fn retire(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> VectorCacheRetirement {
        let identity = VectorCacheIdentity::from_validated(handle);
        let entry = {
            let mut state = self.state.write();
            match state.entries.entry(identity) {
                hash_map::Entry::Occupied(entry) => Some(Arc::clone(entry.get())),
                hash_map::Entry::Vacant(entry) => {
                    let cache_entry = Arc::new(VectorMemoryCacheEntry::closed(entry.key().clone()));
                    entry.insert(cache_entry);
                    None
                }
            }
        };
        let Some(entry) = entry else {
            return VectorCacheRetirement::ClosedEmpty;
        };
        entry.retire().await;
        VectorCacheRetirement::ClosedResident
    }

    /// Atomically fences and drains every cache revision/partition in a generation.
    ///
    /// Installing the generation fence and collecting existing entries happen
    /// under one lock. A concurrent stale hydration therefore either appears in
    /// the collected set or observes the fence and receives a closed entry.
    pub(crate) async fn retire_cleanup_generation(
        &self,
        authority: &ValidatedVectorCleanupAuthority,
    ) -> usize {
        let fence = VectorCacheGenerationFence::from_cleanup(authority);
        let entries = {
            let mut state = self.state.write();
            state.retired_generations.insert(fence);
            state
                .entries
                .iter()
                .filter(|(identity, _)| fence.matches(identity))
                .map(|(_, entry)| Arc::clone(entry))
                .collect::<Vec<_>>()
        };
        let count = entries.len();
        for entry in entries {
            entry.retire().await;
        }
        count
    }

    /// Removes a generation fence after its terminal durable cleanup commit.
    ///
    /// The caller invokes this only from the outbox post-commit hook. Every
    /// matching entry must already be closed; otherwise the fence is retained.
    pub(crate) fn forget_cleanup_generation(
        &self,
        authority: &ValidatedVectorCleanupAuthority,
    ) -> bool {
        let fence = VectorCacheGenerationFence::from_cleanup(authority);
        let mut state = self.state.write();
        if state.entries.iter().any(|(identity, entry)| {
            fence.matches(identity) && entry.lifecycle() != VectorCacheLifecycle::Closed
        }) {
            return false;
        }
        state.entries.retain(|identity, _| !fence.matches(identity));
        state.retired_generations.remove(&fence)
    }

    /// Removes a closed tombstone only after exact physical absence is durable.
    ///
    /// Returning `false` means the identity was absent or not yet closed. This
    /// prevents cleanup from accidentally making a `Hydrating`, `Ready`, or
    /// `Retiring` identity insertable again.
    pub(crate) fn forget_closed(&self, identity: &VectorCacheIdentity) -> bool {
        let mut state = self.state.write();
        let Some(entry) = state.entries.get(identity) else {
            return false;
        };
        if entry.lifecycle() != VectorCacheLifecycle::Closed {
            return false;
        }
        state.entries.remove(identity);
        true
    }

    /// Forgets a tombstone by the same validated handle used for retirement.
    ///
    /// Physical cleanup uses this after its transaction has deleted the exact
    /// descriptor and ownership record. Keeping projection here prevents the
    /// cleaner from reconstructing a partial cache identity independently.
    pub(crate) fn forget_validated_closed(&self, handle: &ValidatedVectorGenerationHandle) -> bool {
        self.forget_closed(&VectorCacheIdentity::from_validated(handle))
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/memory_registry.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::search::vector::distance::Cosine;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

    fn validated_exact(
        scope: DataScope,
        index_id: u64,
        generation: u64,
        physical_index_id: u64,
        record_revision: u64,
    ) -> ValidatedVectorGenerationHandle {
        let identity = VectorGenerationIdentity::try_new(
            scope,
            index_id,
            format!("vector-cache-generation-{generation}"),
            physical_index_id,
            NonZeroU64::new(generation).unwrap(),
            record_revision,
            crate::index_lifecycle::IndexElementKind::Node,
            VectorDimension::try_new(3).unwrap(),
        )
        .unwrap();
        ValidatedVectorGenerationHandle::create_current::<Cosine>(identity).unwrap()
    }

    fn validated(generation: u64) -> ValidatedVectorGenerationHandle {
        validated_exact(DataScope::LegacyUnscoped, 7, generation, 70, 1)
    }

    fn cleaning_authority() -> (
        ValidatedVectorCleanupAuthority,
        ValidatedVectorGenerationHandle,
    ) {
        let definition = crate::config::VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            crate::search::vector::VectorDistanceMetric::Cosine,
        )
        .unwrap();
        let definition =
            crate::index_lifecycle::ValidatedVectorIndexDefinition::try_from_runtime(&definition)
                .unwrap();
        let build_operation = crate::index_lifecycle::IndexOperationId::new_v4();
        let active = crate::index_lifecycle::IndexRecordV2::building(
            crate::index_lifecycle::IndexId::new(7).unwrap(),
            crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(definition.clone()),
            crate::index_lifecycle::IndexRevision::initial(),
            crate::index_lifecycle::PhysicalGeneration::Vector {
                generation: crate::index_lifecycle::IndexGenerationId::initial(),
                layout: crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: crate::index_lifecycle::VectorPhysicalIndexId::new(70)
                        .unwrap(),
                },
                descriptor: crate::index_lifecycle::VectorGenerationDescriptor::for_definition(
                    &definition,
                ),
            },
            build_operation,
        )
        .unwrap()
        .transition(crate::index_lifecycle::IndexStateTransition::Activate)
        .unwrap();
        let active_handle = crate::index_lifecycle::ActiveIndexHandle::try_from_record(
            DataScope::LegacyUnscoped,
            &active,
        )
        .unwrap();
        let generation = ValidatedVectorGenerationHandle::try_from_active::<Cosine>(
            &active_handle,
            crate::index_lifecycle::VectorPhysicalIndexId::new(70).unwrap(),
        )
        .unwrap();
        let drop_operation = crate::index_lifecycle::IndexOperationId::new_v4();
        let dropping = active
            .transition(crate::index_lifecycle::IndexStateTransition::BeginDrop {
                drop_operation_id: drop_operation,
            })
            .unwrap();
        let authority = ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
            DataScope::LegacyUnscoped,
            &dropping,
            drop_operation,
        )
        .unwrap();
        (authority, generation)
    }

    fn store(identity: &VectorCacheIdentity) -> Arc<VectorMemoryStore> {
        Arc::new(VectorMemoryStore::new(
            identity.scope(),
            identity.physical_index_id(),
            0,
        ))
    }

    #[test]
    fn identity_is_full_descriptor_and_generation_specific() {
        let first = VectorCacheIdentity::from_validated(&validated(1));
        let same = VectorCacheIdentity::from_validated(&validated(1));
        let successor = VectorCacheIdentity::from_validated(&validated(2));
        let another_scope = VectorCacheIdentity::from_validated(&validated_exact(
            DataScope::Tenant(crate::encoding::keys::scope::TenantId::from_u128(1)),
            7,
            1,
            70,
            1,
        ));
        let another_index = VectorCacheIdentity::from_validated(&validated_exact(
            DataScope::LegacyUnscoped,
            8,
            1,
            70,
            1,
        ));
        let another_physical = VectorCacheIdentity::from_validated(&validated_exact(
            DataScope::LegacyUnscoped,
            7,
            1,
            71,
            1,
        ));
        let another_revision = VectorCacheIdentity::from_validated(&validated_exact(
            DataScope::LegacyUnscoped,
            7,
            1,
            70,
            2,
        ));

        assert_eq!(first, same);
        assert_ne!(first, successor);
        assert_ne!(first, another_scope);
        assert_ne!(first, another_index);
        assert_ne!(first, another_physical);
        assert_ne!(first, another_revision);
        assert_eq!(
            first.generation(),
            crate::index_lifecycle::IndexGenerationId::initial()
        );
        assert_eq!(
            first.index_id(),
            crate::index_lifecycle::IndexId::new(7).unwrap()
        );
        assert_eq!(first.physical_index_id(), 70);
        assert_eq!(
            first.record_revision(),
            crate::index_lifecycle::IndexRevision::initial()
        );
    }

    #[tokio::test]
    async fn retirement_rejects_new_guards_and_waits_for_active_reader() {
        let registry = Arc::new(VectorCacheRegistry::default());
        let handle = validated(1);
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(store(entry.identity())));
        let guard = entry.acquire_read_guard().unwrap();
        let retirement_registry = Arc::clone(&registry);
        let retirement_handle = handle.clone();
        let retirement =
            tokio::spawn(async move { retirement_registry.retire(&retirement_handle).await });

        tokio::task::yield_now().await;
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Retiring);
        assert!(matches!(
            entry.acquire_read_guard(),
            Err(VectorCacheReadGuardError::Unavailable(
                VectorCacheLifecycle::Retiring
            ))
        ));
        assert!(!retirement.is_finished());
        drop(guard);

        assert_eq!(
            retirement.await.unwrap(),
            VectorCacheRetirement::ClosedResident
        );
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Closed);
        assert!(entry.acquire_read_guard().is_err());
    }

    #[tokio::test]
    async fn generation_cleanup_fence_closes_late_hydration_until_post_commit_forget() {
        let registry = VectorCacheRegistry::default();
        let (authority, stale_active_generation) = cleaning_authority();

        assert_eq!(registry.retire_cleanup_generation(&authority).await, 0);
        let (closed, owns_hydration) = registry.entry_for(&stale_active_generation);
        assert!(!owns_hydration);
        assert_eq!(closed.lifecycle(), VectorCacheLifecycle::Closed);
        assert!(registry.forget_cleanup_generation(&authority));

        let (reopened, owns_hydration) = registry.entry_for(&stale_active_generation);
        assert!(owns_hydration);
        assert_eq!(reopened.lifecycle(), VectorCacheLifecycle::Hydrating);
    }

    #[tokio::test]
    async fn retiring_hydration_discards_store_and_closed_tombstone_blocks_reopen() {
        let registry = Arc::new(VectorCacheRegistry::default());
        let handle = validated(1);
        let identity = VectorCacheIdentity::from_validated(&handle);
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        let retirement_registry = Arc::clone(&registry);
        let retirement_handle = handle.clone();
        let retirement =
            tokio::spawn(async move { retirement_registry.retire(&retirement_handle).await });

        tokio::task::yield_now().await;
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Retiring);
        let unpublished = store(&identity);
        unpublished.insert_upper_vector(9, Bytes::from_static(b"stale"));
        assert!(!entry.finish_hydration(Arc::clone(&unpublished)));
        assert!(unpublished.get_upper_vector(9).is_none());
        assert_eq!(
            retirement.await.unwrap(),
            VectorCacheRetirement::ClosedResident
        );

        let (same_entry, owns_hydration) = registry.entry_for(&handle);
        assert!(!owns_hydration);
        assert!(Arc::ptr_eq(&entry, &same_entry));
        assert_eq!(same_entry.lifecycle(), VectorCacheLifecycle::Closed);
        assert!(registry.forget_closed(&identity));
        let (replacement, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(!Arc::ptr_eq(&entry, &replacement));
    }

    #[tokio::test]
    async fn successor_generation_never_reuses_closed_predecessor_entry() {
        let registry = VectorCacheRegistry::default();
        let first = validated(1);
        let successor = validated(2);
        let (first_entry, _) = registry.entry_for(&first);
        assert!(first_entry.finish_hydration(store(first_entry.identity())));
        assert_eq!(
            registry.retire(&first).await,
            VectorCacheRetirement::ClosedResident
        );

        let (successor_entry, owns_hydration) = registry.entry_for(&successor);
        assert!(owns_hydration);
        assert_ne!(first_entry.identity(), successor_entry.identity());
        assert_eq!(first_entry.lifecycle(), VectorCacheLifecycle::Closed);
        assert_eq!(successor_entry.lifecycle(), VectorCacheLifecycle::Hydrating);
    }

    #[tokio::test]
    async fn drop_before_admission_installs_closed_tombstone() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);

        assert_eq!(
            registry.retire(&handle).await,
            VectorCacheRetirement::ClosedEmpty
        );
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(!owns_hydration);
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Closed);
        assert!(registry.forget_validated_closed(&handle));
    }

    #[tokio::test]
    async fn pending_commit_fences_then_evicts_while_abort_publishes_nothing() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let identity = VectorCacheIdentity::from_validated(&handle);
        let store = store(&identity);
        store.insert_simhash(7, crate::search::vector::SimHash::from_bits(11));
        store.insert_upper_vector(7, Bytes::from_static(b"vector"));
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(Arc::clone(&store)));

        let writes = super::super::write_cache::VectorCacheWriteSet::default();
        writes.dirty_rows_for(&handle).mark_node_dirty(7);
        let write = writes.entries().pop().unwrap();
        let aborted = registry.prepare_commit(&write).unwrap();
        assert!(entry.pending_dirty.is_node_dirty(7));
        drop(aborted);
        assert!(!entry.pending_dirty.is_node_dirty(7));
        assert!(store.get_upper_vector(7).is_some());
        assert_eq!(entry.pending_dirty.generation(), 0);

        let committed = registry.prepare_commit(&write).unwrap();
        assert!(entry.pending_dirty.is_node_dirty(7));
        committed.evict_after_commit(store.visible_seq() + 1).await;
        assert!(!entry.pending_dirty.is_node_dirty(7));
        assert!(store.get_simhash(7).is_none());
        assert!(store.get_upper_vector(7).is_none());
        assert_eq!(entry.pending_dirty.generation(), 1);
    }

    #[tokio::test]
    async fn hydration_reservations_publish_immutable_newer_stores_single_flight() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        let first = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            1,
        ));
        first.insert_upper_vector(7, Bytes::from_static(b"first"));
        assert!(initial.finish(Arc::clone(&first)).await);
        let old_guard = registry.read_guard_for(&handle).unwrap();

        let refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must grant one refresh")
            }
        };
        assert!(matches!(
            registry.prepare_hydration(&handle),
            VectorCacheHydration::Unavailable(VectorCacheLifecycle::Ready)
        ));
        let second = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            2,
        ));
        second.insert_upper_vector(7, Bytes::from_static(b"second"));
        assert!(refresh.finish(Arc::clone(&second)).await);

        assert_eq!(old_guard.store().visible_seq(), 1);
        assert_eq!(
            old_guard.store().get_upper_vector(7).unwrap().as_ref(),
            b"first"
        );
        let new_guard = registry.read_guard_for(&handle).unwrap();
        assert_eq!(new_guard.store().visible_seq(), 2);
        assert_eq!(
            new_guard.store().get_upper_vector(7).unwrap().as_ref(),
            b"second"
        );
        let equal_refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must allow an equal-snapshot budget refresh")
            }
        };
        let equal = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            2,
        ));
        assert!(equal_refresh.finish(Arc::clone(&equal)).await);
        assert!(registry
            .read_guard_for(&handle)
            .unwrap()
            .store()
            .get_upper_vector(7)
            .is_none());
        assert_eq!(
            new_guard.store().get_upper_vector(7).unwrap().as_ref(),
            b"second"
        );
    }

    #[tokio::test]
    async fn commit_generation_changes_discard_initial_and_refresh_hydration() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        initial.entry.pending_dirty.bump_generation();
        let unpublished = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            1,
        ));
        unpublished.insert_upper_vector(7, Bytes::from_static(b"stale"));
        assert!(!initial.finish(Arc::clone(&unpublished)).await);
        assert!(unpublished.get_upper_vector(7).is_none());
        assert!(registry.forget_validated_closed(&handle));

        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("forgotten failed hydration must be retryable")
            }
        };
        let resident = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            2,
        ));
        resident.insert_upper_vector(7, Bytes::from_static(b"resident"));
        assert!(initial.finish(Arc::clone(&resident)).await);
        let refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("resident identity must grant refresh")
            }
        };
        refresh.entry.pending_dirty.bump_generation();
        let replacement = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            3,
        ));
        replacement.insert_upper_vector(7, Bytes::from_static(b"stale-refresh"));
        assert!(!refresh.finish(Arc::clone(&replacement)).await);
        assert!(replacement.get_upper_vector(7).is_none());
        assert!(Arc::ptr_eq(
            registry.read_guard_for(&handle).unwrap().store(),
            &resident
        ));
    }

    #[test]
    fn dropped_initial_hydration_closes_and_wakes_the_entry() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        drop(initial);

        assert!(matches!(
            registry.prepare_hydration(&handle),
            VectorCacheHydration::Unavailable(VectorCacheLifecycle::Closed)
        ));
        assert!(registry.forget_validated_closed(&handle));
    }

    #[tokio::test]
    async fn retirement_waits_for_refresh_and_discards_its_unpublished_store() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        let first = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            1,
        ));
        assert!(initial.finish(first).await);
        let refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must grant refresh")
            }
        };
        let replacement = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            2,
        ));
        replacement.insert_upper_vector(7, Bytes::from_static(b"unpublished"));
        let retirement = registry.retire(&handle);
        tokio::pin!(retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut retirement)
                .await
                .is_err()
        );

        assert!(!refresh.finish(Arc::clone(&replacement)).await);
        assert_eq!(retirement.await, VectorCacheRetirement::ClosedResident);
        assert!(replacement.get_upper_vector(7).is_none());
        assert!(matches!(
            registry.read_guard_for(&handle),
            Err(VectorCacheReadGuardError::Unavailable(
                VectorCacheLifecycle::Closed
            ))
        ));
    }
}
