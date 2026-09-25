//! Descriptor-bound lifecycle registry for vector memory-cache generations.
//!
//! [`VectorCacheRegistry`] is the only owner of cache entries whose identity is
//! derived from a [`ValidatedVectorGenerationHandle`]. Each entry follows the
//! explicit `Vacant <-> Hydrating -> Ready -> Retiring -> Closed` lifecycle.
//! Any operation must hold a [`VectorCacheReadGuard`] while using its store;
//! retirement changes the entry to `Retiring`, rejects new guards, waits for
//! existing guards and hydration to finish, acquires the independent all-dirty
//! publication guard, clears the exact store, and only then reaches `Closed`.
//!
//! Every identity has one commit fence owned by the registry rather than by
//! its entry, so a storage commit is fenced even before the first hydration
//! creates the entry and even if the entry is replaced while the commit is in
//! flight. [`VectorCacheVisibility`] decides how a published store's hydration
//! sequence and that fence authorize a request snapshot.
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

use super::store;
use super::store::{
    VectorMemoryDirtyRows, VectorMemoryPendingDirtyGuard, VectorMemoryPendingDirtyRows,
    VectorMemoryStore,
};
use crate::encoding::keys::scope::DataScope;
use crate::search::vector::{ValidatedVectorCleanupAuthority, ValidatedVectorGenerationHandle};

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

/// How a registry proves that a published store is current for a request snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum VectorCacheVisibility {
    /// Only a store hydrated at exactly the request sequence is current.
    ///
    /// Sound for every storage source, including reader nodes, which observe
    /// writer commits without passing any commit fence.
    #[default]
    ExactSequence,
    /// A store hydrated at or before the request sequence is current while its
    /// identity has no unresolved commit and every resolved commit was evicted
    /// from it.
    ///
    /// Sound only when every commit that changes this registry's vector rows
    /// passes [`VectorCacheRegistry::prepare_commit`] and is resolved with its
    /// storage outcome, which holds on the writer node that owns the registry.
    CommitFenced,
}

/// Why a ready store is not current for one request snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheStaleness {
    /// The store's hydration sequence cannot serve the request sequence.
    SnapshotSequence,
    /// A commit on this identity is unresolved, so its rows may be missing.
    CommitInFlight,
    /// A commit's rows may have changed without being evicted from this store.
    UnevictedCommit,
}

/// Process-local state one hydration pass may release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheSweep {
    /// Writer node: retirement owns every entry, so only fences guarding no
    /// entry and no unresolved commit are released.
    OrphanFences,
    /// Reader node: nothing retires entries, so entries of the swept scope
    /// outside the Active inventory are released before orphaned fences.
    InactiveEntries,
}

/// Runtime state of one complete vector cache identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheLifecycle {
    /// No store is published and no hydration owns the entry; the next pass
    /// may reserve initial hydration.
    Vacant,
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
    admission: store::VectorMemoryAdmissionBudget,
    /// Fence generation observed before the store's snapshot. A newer fence
    /// generation means a commit may have added rows the store never loaded,
    /// so the next refresh rescans instead of retaining it.
    hydrated_dirty_generation: u64,
    /// Fence generation through which every commit's rows were evicted from
    /// this store; commit-fenced guards require it to equal the fence.
    evicted_dirty_generation: u64,
}

enum VectorCacheEntryState {
    Vacant,
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
            Self::Vacant => VectorCacheLifecycle::Vacant,
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
    /// The identity's registry-owned commit fence, shared with any successor entry.
    pending_dirty: Arc<VectorMemoryPendingDirtyRows>,
    state: Mutex<VectorCacheEntryState>,
    changed: Notify,
}

impl VectorMemoryCacheEntry {
    /// Creates an entry bound to its identity's registry-owned commit fence.
    fn new(
        identity: VectorCacheIdentity,
        pending_dirty: Arc<VectorMemoryPendingDirtyRows>,
        state: VectorCacheEntryState,
    ) -> Self {
        Self {
            identity,
            pending_dirty,
            state: Mutex::new(state),
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
            VectorCacheEntryState::Vacant
            | VectorCacheEntryState::Hydrating
            | VectorCacheEntryState::Retiring { .. }
            | VectorCacheEntryState::Closed => 0,
        }
    }

    /// Publishes a manually constructed fixture without a storage scan.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn finish_hydration(&self, store: Arc<VectorMemoryStore>) -> bool {
        self.publish_initial(
            store,
            store::VectorMemoryAdmissionBudget::Unbounded,
            self.pending_dirty.generation(),
        )
    }

    /// Publishes a completely hydrated store or discards it after retirement.
    ///
    /// Hydration callers must build the store off-entry and invoke this exactly
    /// once. If drop changed the entry to `Retiring`, the unpublished store is
    /// cleared and retirement is notified instead of exposing partial or stale
    /// rows. Calling this outside `Hydrating` or `Retiring` is an invariant
    /// violation.
    fn publish_initial(
        &self,
        store: Arc<VectorMemoryStore>,
        admission: store::VectorMemoryAdmissionBudget,
        hydrated_dirty_generation: u64,
    ) -> bool {
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
                        admission,
                        hydrated_dirty_generation,
                        evicted_dirty_generation: hydrated_dirty_generation,
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
                VectorCacheEntryState::Vacant
                | VectorCacheEntryState::Ready(_)
                | VectorCacheEntryState::Closed => {
                    panic!("vector cache hydration may finish exactly once from Hydrating")
                }
            }
        };
        self.changed.notify_waiters();
        published
    }

    /// Cancels unpublished initial hydration and wakes retirement waiters.
    ///
    /// Background hydration owns this transition through an RAII permit. A
    /// dropped or commit-discarded permit returns the entry to `Vacant`, so a
    /// later pass hydrates it again, while retirement keeps its own tombstone
    /// path and only learns that hydration drained.
    fn cancel_initial_hydration(&self) {
        {
            let mut state = self.state.lock();
            match &mut *state {
                VectorCacheEntryState::Hydrating => {
                    *state = VectorCacheEntryState::Vacant;
                }
                VectorCacheEntryState::Retiring {
                    hydration_inflight, ..
                } => {
                    *hydration_inflight = false;
                }
                VectorCacheEntryState::Vacant
                | VectorCacheEntryState::Ready(_)
                | VectorCacheEntryState::Closed => {}
            }
        }
        self.changed.notify_waiters();
    }

    /// Claims the single hydration slot of an existing entry.
    ///
    /// A `Vacant` entry grants a new initial hydration and a `Ready` entry
    /// grants one refresh while readers retain the published store. Both
    /// reservations observe the fence generation before the caller's snapshot.
    /// Every other state is an explicit unavailable lifecycle.
    fn reserve_hydration(
        self: &Arc<Self>,
        visibility: VectorCacheVisibility,
        admission: store::VectorMemoryAdmissionBudget,
    ) -> VectorCacheHydration {
        let mut state = self.state.lock();
        match &mut *state {
            VectorCacheEntryState::Vacant => {
                *state = VectorCacheEntryState::Hydrating;
                VectorCacheHydration::Initial(VectorCacheInitialHydration {
                    entry: Arc::clone(self),
                    observed_dirty_generation: self.pending_dirty.generation(),
                    admission,
                    completed: false,
                })
            }
            VectorCacheEntryState::Ready(resident) if !resident.refresh_inflight => {
                resident.refresh_inflight = true;
                VectorCacheHydration::Refresh(VectorCacheRefresh {
                    entry: Arc::clone(self),
                    observed_dirty_generation: self.pending_dirty.generation(),
                    visibility,
                    admission,
                    completed: false,
                })
            }
            unavailable @ (VectorCacheEntryState::Hydrating
            | VectorCacheEntryState::Ready(_)
            | VectorCacheEntryState::Retiring { .. }
            | VectorCacheEntryState::Closed) => {
                VectorCacheHydration::Unavailable(unavailable.lifecycle())
            }
        }
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
                VectorCacheEntryState::Vacant | VectorCacheEntryState::Hydrating => {
                    unreachable!("a refresh reservation starts only from Ready")
                }
            }
        }
        self.changed.notify_waiters();
    }

    /// Acquires active-reader ownership of a store proven current for one snapshot.
    ///
    /// The guard retains both the exact identity and immutable store `Arc`.
    /// `Vacant`, `Hydrating`, `Retiring`, and `Closed` are explicit
    /// non-readable states, and a ready store that `visibility` cannot prove
    /// current is rejected with its [`VectorCacheStaleness`]; callers fall back
    /// to storage rather than guessing cache compatibility. The checks run
    /// under the entry lock that commit eviction also holds, so a guard is
    /// never granted between a commit's row eviction and its generation advance.
    pub(crate) fn acquire_read_guard(
        self: &Arc<Self>,
        visibility: VectorCacheVisibility,
        snapshot_seq: u64,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        self.grant_read_guard(|resident| match visibility {
            VectorCacheVisibility::ExactSequence
                if !resident.store.is_visible_to_snapshot(snapshot_seq) =>
            {
                Err(VectorCacheStaleness::SnapshotSequence)
            }
            VectorCacheVisibility::CommitFenced
                if !resident.store.is_usable_for_writer_snapshot(snapshot_seq) =>
            {
                Err(VectorCacheStaleness::SnapshotSequence)
            }
            VectorCacheVisibility::CommitFenced if self.pending_dirty.has_pending_commits() => {
                Err(VectorCacheStaleness::CommitInFlight)
            }
            VectorCacheVisibility::CommitFenced
                if self.pending_dirty.generation() != resident.evicted_dirty_generation =>
            {
                Err(VectorCacheStaleness::UnevictedCommit)
            }
            VectorCacheVisibility::ExactSequence | VectorCacheVisibility::CommitFenced => Ok(()),
        })
    }

    /// Acquires the published store without a currency proof, for inspection only.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn acquire_resident_guard(
        self: &Arc<Self>,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        self.grant_read_guard(|_| Ok(()))
    }

    /// Grants a guard on the published store once `current` accepts it.
    fn grant_read_guard(
        self: &Arc<Self>,
        current: impl FnOnce(&ResidentVectorCache) -> Result<(), VectorCacheStaleness>,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        let store = {
            let mut state = self.state.lock();
            let VectorCacheEntryState::Ready(resident) = &mut *state else {
                return Err(VectorCacheReadGuardError::Unavailable(state.lifecycle()));
            };
            current(resident).map_err(VectorCacheReadGuardError::NotCurrent)?;
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
                VectorCacheEntryState::Vacant => {
                    *state = VectorCacheEntryState::Retiring {
                        resident: None,
                        hydration_inflight: false,
                    };
                }
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
                    VectorCacheEntryState::Vacant
                    | VectorCacheEntryState::Hydrating
                    | VectorCacheEntryState::Ready(_) => {
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
    admission: store::VectorMemoryAdmissionBudget,
    completed: bool,
}

impl VectorCacheInitialHydration {
    /// Publishes the first immutable store if no commit crossed its snapshot.
    ///
    /// A commit that advanced the fence since the reservation discards the
    /// store and returns the entry to `Vacant` for the next pass.
    pub(crate) async fn finish(mut self, store: Arc<VectorMemoryStore>) -> bool {
        let _publication = self.entry.pending_dirty.lock_publish().await;
        if self.entry.pending_dirty.generation() != self.observed_dirty_generation {
            store.clear();
            self.entry.cancel_initial_hydration();
            self.completed = true;
            return false;
        }
        let published =
            self.entry
                .publish_initial(store, self.admission, self.observed_dirty_generation);
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
    visibility: VectorCacheVisibility,
    admission: store::VectorMemoryAdmissionBudget,
    completed: bool,
}

impl VectorCacheRefresh {
    /// Releases this reservation without scanning when the published store is
    /// still visible to the fresh snapshot, matches the assigned budget, and no
    /// commit advanced the fence since it was hydrated.
    ///
    /// Under [`VectorCacheVisibility::CommitFenced`] a newer snapshot alone
    /// never forces a rescan, so writes that touch no fenced vector row keep
    /// the store. Returns retained resident bytes for the caller's admission
    /// accounting. The caller must acquire its snapshot after reserving this
    /// refresh.
    pub(crate) async fn retain_if_current(&mut self, snapshot_seq: u64) -> Option<u64> {
        let _publication = self.entry.pending_dirty.lock_publish().await;
        let retained_bytes = {
            let mut state = self.entry.state.lock();
            let VectorCacheEntryState::Ready(resident) = &mut *state else {
                return None;
            };
            assert!(resident.refresh_inflight);
            let visible = match self.visibility {
                VectorCacheVisibility::ExactSequence => {
                    resident.store.is_visible_to_snapshot(snapshot_seq)
                }
                VectorCacheVisibility::CommitFenced => {
                    resident.store.is_usable_for_writer_snapshot(snapshot_seq)
                }
            };
            if !visible
                || resident.admission != self.admission
                || resident.hydrated_dirty_generation != self.observed_dirty_generation
                || self.entry.pending_dirty.generation() != self.observed_dirty_generation
            {
                return None;
            }
            resident.refresh_inflight = false;
            resident.store.estimated_bytes()
        };
        self.completed = true;
        self.entry.changed.notify_waiters();
        Some(retained_bytes)
    }

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
                        resident.admission = self.admission;
                        resident.hydrated_dirty_generation = self.observed_dirty_generation;
                        resident.evicted_dirty_generation = self.observed_dirty_generation;
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
                VectorCacheEntryState::Vacant | VectorCacheEntryState::Hydrating => {
                    unreachable!("a refresh reservation cannot leave Ready for Vacant or Hydrating")
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

/// Storage outcome that resolves one pending commit fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheCommitOutcome {
    /// Storage rejected the batch before applying it, so no cached row changed.
    Rejected,
    /// Storage applied the batch, or failed after it may have been applied.
    MaybeApplied,
}

/// Pre-commit fence for one exact generation's transaction-local dirty rows.
///
/// It holds the identity's registry-owned fence from before the storage commit
/// until [`Self::resolve`] observes the storage outcome, so commit-fenced reads
/// never attach a store while this commit's rows may be visible but not yet
/// evicted.
pub(crate) struct VectorCachePendingCommit {
    /// Entry registered when the commit was prepared, if one existed.
    entry: Option<Arc<VectorMemoryCacheEntry>>,
    fence: Arc<VectorMemoryPendingDirtyRows>,
    dirty_rows: Arc<VectorMemoryDirtyRows>,
    _pending_guard: VectorMemoryPendingDirtyGuard,
    resolved: bool,
}

impl VectorCachePendingCommit {
    /// Releases the fence once the storage outcome is known.
    ///
    /// A rejected batch releases it unchanged. Any other outcome evicts the
    /// dirty rows from the published store under the publication lock and
    /// advances the fence generation; the store stays attachable only if every
    /// earlier commit was evicted from it too. Nothing is republished here: a
    /// later snapshot either uses the evicted store, whose absent rows fall
    /// back to storage, or an independently hydrated store.
    pub(crate) async fn resolve(mut self, outcome: VectorCacheCommitOutcome) {
        self.resolved = true;
        match outcome {
            VectorCacheCommitOutcome::Rejected => {}
            VectorCacheCommitOutcome::MaybeApplied => {
                let _publication = self.fence.lock_publish().await;
                let Some(entry) = &self.entry else {
                    // A store first published after preparation recorded the
                    // pre-advance generation, so it stays unattachable until a
                    // refresh observes this commit.
                    self.fence.bump_generation();
                    return;
                };
                let mut state = entry.state.lock();
                let (VectorCacheEntryState::Ready(resident)
                | VectorCacheEntryState::Retiring {
                    resident: Some(resident),
                    ..
                }) = &mut *state
                else {
                    self.fence.bump_generation();
                    return;
                };
                for node_id in self.dirty_rows.dirty_nodes() {
                    resident.store.remove_node(node_id);
                }
                for (layer, node_id) in self.dirty_rows.dirty_upper_neighbors() {
                    resident.store.remove_upper_neighbors(layer, node_id);
                }
                let replaced = self.fence.bump_generation();
                if resident.evicted_dirty_generation == replaced {
                    // Mirrors the wrapping `fetch_add` that advanced the fence.
                    resident.evicted_dirty_generation = replaced.wrapping_add(1);
                }
            }
        }
    }
}

impl Drop for VectorCachePendingCommit {
    /// Invalidates conservatively when the storage outcome was never observed.
    ///
    /// Advancing the fence without advancing any store's evicted generation
    /// leaves every published store unattachable to commit-fenced reads until
    /// a refresh that observes the advance rehydrates it; guards held already
    /// were granted before this commit was prepared, so their snapshots precede
    /// it. Production commits resolve through `commit_fenced`, whose detached
    /// task the caller cannot cancel, so this path follows only a panic or
    /// runtime shutdown.
    fn drop(&mut self) {
        if !self.resolved {
            self.fence.bump_generation();
        }
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
                VectorCacheEntryState::Vacant
                | VectorCacheEntryState::Hydrating
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
    /// The published store is not proven current for the request snapshot.
    #[error("vector cache store is not current for the request snapshot: {0:?}")]
    NotCurrent(VectorCacheStaleness),
}

/// Outcome of closing an exact generation in the process-local registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheRetirement {
    /// Drop installed a closed tombstone before any cache admission occurred.
    ClosedEmpty,
    /// An existing matching entry was drained and closed.
    ClosedResident,
}

/// Registry keyed by the complete validated vector generation descriptor.
#[derive(Default)]
struct VectorCacheRegistryState {
    entries: HashMap<VectorCacheIdentity, Arc<VectorMemoryCacheEntry>>,
    /// One commit fence per identity, outliving entries so a commit prepared
    /// before an entry exists, or while one is replaced, still fences it.
    fences: HashMap<VectorCacheIdentity, Arc<VectorMemoryPendingDirtyRows>>,
    retired_generations: HashSet<VectorCacheGenerationFence>,
}

/// Atomic owner of exact cache entries, commit fences, and retirement fences.
#[derive(Default)]
pub(crate) struct VectorCacheRegistry {
    state: RwLock<VectorCacheRegistryState>,
    visibility: VectorCacheVisibility,
}

impl VectorCacheRegistry {
    /// Creates an empty registry whose read guards prove currency with `visibility`.
    pub(crate) fn new(visibility: VectorCacheVisibility) -> Self {
        Self {
            state: RwLock::default(),
            visibility,
        }
    }

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
        admission: store::VectorMemoryAdmissionBudget,
    ) -> VectorCacheHydration {
        let (entry, owns_initial) = self.entry_for(handle);
        if owns_initial {
            let observed_dirty_generation = entry.pending_dirty.generation();
            return VectorCacheHydration::Initial(VectorCacheInitialHydration {
                entry,
                observed_dirty_generation,
                admission,
                completed: false,
            });
        }
        entry.reserve_hydration(self.visibility, admission)
    }

    /// Acquires the identity's commit fence before one storage commit.
    ///
    /// The fence is created when absent, so a commit is fenced even before the
    /// first hydration creates the cache entry; an entry created later shares
    /// the fence, and its hydration observes this commit's generation advance.
    /// Empty write sets return `None`. The returned value must live across
    /// `DbTransaction::commit` and be resolved with the storage outcome.
    pub(crate) fn prepare_commit(
        &self,
        write: &super::commit::VectorCacheWriteEntry,
    ) -> Option<VectorCachePendingCommit> {
        let dirty_rows = write.dirty_rows()?;
        if dirty_rows.is_empty() {
            return None;
        }
        let identity = VectorCacheIdentity::from_validated(write.handle());
        let (entry, fence) = {
            let mut state = self.state.write();
            let fence = Arc::clone(state.fences.entry(identity.clone()).or_default());
            (state.entries.get(&identity).cloned(), fence)
        };
        let pending_guard = fence.acquire(dirty_rows);
        Some(VectorCachePendingCommit {
            entry,
            fence,
            dirty_rows: Arc::clone(dirty_rows),
            _pending_guard: pending_guard,
            resolved: false,
        })
    }

    /// Acquires a guard on a store proven current for `snapshot_seq`.
    ///
    /// Read factories use this non-creating lookup so a cache miss cannot
    /// accidentally claim hydration ownership. Absent, unreadable, and
    /// not-current entries are explicit storage-fallback results.
    pub(crate) fn read_guard_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
        snapshot_seq: u64,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        let identity = VectorCacheIdentity::from_validated(handle);
        let Some(entry) = self.state.read().entries.get(&identity).cloned() else {
            return Err(VectorCacheReadGuardError::Absent);
        };
        entry.acquire_read_guard(self.visibility, snapshot_seq)
    }

    /// Acquires the published store without a currency proof, for inspection only.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn resident_guard_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> Result<VectorCacheReadGuard, VectorCacheReadGuardError> {
        let identity = VectorCacheIdentity::from_validated(handle);
        let Some(entry) = self.state.read().entries.get(&identity).cloned() else {
            return Err(VectorCacheReadGuardError::Absent);
        };
        entry.acquire_resident_guard()
    }

    /// Returns the single entry for `handle`, creating a hydrating one if absent.
    ///
    /// The boolean is true only for the caller that inserted a new `Hydrating`
    /// entry and therefore owns hydration. Existing entries in any lifecycle,
    /// including closed tombstones, are returned rather than replaced;
    /// recreation must carry a distinct lifecycle generation.
    pub(crate) fn entry_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> (Arc<VectorMemoryCacheEntry>, bool) {
        let identity = VectorCacheIdentity::from_validated(handle);
        let mut state = self.state.write();
        let VectorCacheRegistryState {
            entries,
            fences,
            retired_generations,
        } = &mut *state;
        let retired =
            retired_generations.contains(&VectorCacheGenerationFence::from_identity(&identity));
        match entries.entry(identity) {
            hash_map::Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
            hash_map::Entry::Vacant(entry) => {
                let cache_entry = Arc::new(VectorMemoryCacheEntry::new(
                    entry.key().clone(),
                    Arc::clone(fences.entry(entry.key().clone()).or_default()),
                    if retired {
                        VectorCacheEntryState::Closed
                    } else {
                        VectorCacheEntryState::Hydrating
                    },
                ));
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
            let VectorCacheRegistryState {
                entries, fences, ..
            } = &mut *state;
            match entries.entry(identity) {
                hash_map::Entry::Occupied(entry) => Some(Arc::clone(entry.get())),
                hash_map::Entry::Vacant(entry) => {
                    let cache_entry = Arc::new(VectorMemoryCacheEntry::new(
                        entry.key().clone(),
                        Arc::clone(fences.entry(entry.key().clone()).or_default()),
                        VectorCacheEntryState::Closed,
                    ));
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

    /// Releases process-local state that no Active generation or commit needs.
    ///
    /// With [`VectorCacheSweep::InactiveEntries`] (reader nodes, which never
    /// run drop or partition retirement) entries of `scope` outside `active`
    /// are removed first; `Retiring` and `Closed` entries are retirement
    /// tombstones owned by physical cleanup and are always kept. Removing any
    /// other entry only causes storage fallback: a retained read guard keeps
    /// its own entry and store alive until it is dropped. Every sweep then
    /// drops fences that only this map references: they guard no entry and no
    /// unresolved commit, and the next commit or entry for that identity
    /// creates a fresh one.
    pub(crate) fn sweep(
        &self,
        scope: DataScope,
        active: &HashSet<VectorCacheIdentity>,
        sweep: VectorCacheSweep,
    ) {
        let mut state = self.state.write();
        let VectorCacheRegistryState {
            entries, fences, ..
        } = &mut *state;
        match sweep {
            VectorCacheSweep::OrphanFences => {}
            VectorCacheSweep::InactiveEntries => entries.retain(|identity, entry| {
                identity.scope() != scope
                    || active.contains(identity)
                    || matches!(
                        entry.lifecycle(),
                        VectorCacheLifecycle::Retiring | VectorCacheLifecycle::Closed
                    )
            }),
        }
        fences.retain(|identity, fence| {
            entries.contains_key(identity) || Arc::strong_count(fence) > 1
        });
    }

    /// Removes a generation fence after its terminal durable cleanup commit.
    ///
    /// The caller invokes this only from the outbox post-commit hook. Every
    /// matching entry must already be closed; otherwise the fence is retained.
    pub(crate) fn forget_cleanup_generation(
        &self,
        authority: &ValidatedVectorCleanupAuthority,
    ) -> bool {
        let generation = VectorCacheGenerationFence::from_cleanup(authority);
        let mut state = self.state.write();
        let VectorCacheRegistryState {
            entries,
            fences,
            retired_generations,
        } = &mut *state;
        if entries.iter().any(|(identity, entry)| {
            generation.matches(identity) && entry.lifecycle() != VectorCacheLifecycle::Closed
        }) {
            return false;
        }
        entries.retain(|identity, _| !generation.matches(identity));
        fences.retain(|identity, fence| {
            !generation.matches(identity) || Arc::strong_count(fence) > 1
        });
        retired_generations.remove(&generation)
    }

    /// Removes a closed tombstone only after exact physical absence is durable.
    ///
    /// Returning `false` means the identity was absent or not yet closed. This
    /// prevents cleanup from accidentally making a `Vacant`, `Hydrating`,
    /// `Ready`, or `Retiring` identity insertable again. The identity's fence
    /// is released too unless an unresolved commit or retained guard holds it.
    pub(crate) fn forget_closed(&self, identity: &VectorCacheIdentity) -> bool {
        let mut state = self.state.write();
        let Some(entry) = state.entries.get(identity) else {
            return false;
        };
        if entry.lifecycle() != VectorCacheLifecycle::Closed {
            return false;
        }
        state.entries.remove(identity);
        if state
            .fences
            .get(identity)
            .is_some_and(|fence| Arc::strong_count(fence) == 1)
        {
            state.fences.remove(identity);
        }
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
#[path = "../../../../tests/production_support/vector/memory_registry.rs"]
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

    /// Builds a store hydrated at `visible_seq` that caches `node_id` as `value`.
    fn store_at(
        handle: &ValidatedVectorGenerationHandle,
        visible_seq: u64,
        node_id: u64,
        value: &'static [u8],
    ) -> Arc<VectorMemoryStore> {
        let store = Arc::new(VectorMemoryStore::new(
            handle.scope(),
            handle.physical_index_id(),
            visible_seq,
        ));
        store.insert_upper_vector(node_id, Bytes::from_static(value));
        store
    }

    /// Prepares one commit whose write set dirties `node_id`.
    fn prepare_dirty_commit(
        registry: &VectorCacheRegistry,
        handle: &ValidatedVectorGenerationHandle,
        node_id: u64,
    ) -> VectorCachePendingCommit {
        let writes = super::super::commit::VectorCacheWriteSet::default();
        writes.dirty_rows_for(handle).mark_node_dirty(node_id);
        registry
            .prepare_commit(&writes.entries().pop().unwrap())
            .expect("a non-empty write set is fenced")
    }

    /// Publishes `store` through the production initial-hydration reservation.
    async fn publish_initial(
        registry: &VectorCacheRegistry,
        handle: &ValidatedVectorGenerationHandle,
        store: Arc<VectorMemoryStore>,
    ) -> bool {
        let VectorCacheHydration::Initial(initial) =
            registry.prepare_hydration(handle, store::VectorMemoryAdmissionBudget::Unbounded)
        else {
            panic!("the identity must grant initial hydration");
        };
        initial.finish(store).await
    }

    /// Reserves the single refresh of a ready identity.
    fn reserve_refresh(
        registry: &VectorCacheRegistry,
        handle: &ValidatedVectorGenerationHandle,
    ) -> VectorCacheRefresh {
        let VectorCacheHydration::Refresh(refresh) =
            registry.prepare_hydration(handle, store::VectorMemoryAdmissionBudget::Unbounded)
        else {
            panic!("a ready identity must grant one refresh");
        };
        refresh
    }

    /// Returns the staleness that rejects a guard for `snapshot_seq`, if any.
    fn staleness(
        registry: &VectorCacheRegistry,
        handle: &ValidatedVectorGenerationHandle,
        snapshot_seq: u64,
    ) -> Option<VectorCacheStaleness> {
        match registry.read_guard_for(handle, snapshot_seq) {
            Ok(_) => None,
            Err(VectorCacheReadGuardError::NotCurrent(staleness)) => Some(staleness),
            Err(error) => panic!("a ready store must be readable or not current: {error}"),
        }
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
        let guard = entry.acquire_resident_guard().unwrap();
        let retirement_registry = Arc::clone(&registry);
        let retirement_handle = handle.clone();
        let retirement =
            tokio::spawn(async move { retirement_registry.retire(&retirement_handle).await });

        tokio::task::yield_now().await;
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Retiring);
        assert!(matches!(
            entry.acquire_resident_guard(),
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
        assert!(entry.acquire_resident_guard().is_err());
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
    async fn inactive_sweep_releases_entries_but_keeps_retirement_tombstones() {
        let registry = VectorCacheRegistry::default();
        let kept = validated(1);
        let released = validated(2);
        let retired = validated(3);
        let other_scope = validated_exact(
            DataScope::Tenant(crate::encoding::keys::scope::TenantId::from_u128(1)),
            7,
            1,
            70,
            1,
        );
        for handle in [&kept, &released, &other_scope] {
            let (entry, owns_hydration) = registry.entry_for(handle);
            assert!(owns_hydration);
            assert!(entry.finish_hydration(store(entry.identity())));
        }
        assert_eq!(
            registry.retire(&retired).await,
            VectorCacheRetirement::ClosedEmpty
        );
        let released_guard = registry.resident_guard_for(&released).unwrap();

        registry.sweep(
            DataScope::LegacyUnscoped,
            &HashSet::from([VectorCacheIdentity::from_validated(&kept)]),
            VectorCacheSweep::InactiveEntries,
        );

        assert!(registry.resident_guard_for(&kept).is_ok());
        assert!(matches!(
            registry.resident_guard_for(&released),
            Err(VectorCacheReadGuardError::Absent)
        ));
        assert!(
            registry.resident_guard_for(&other_scope).is_ok(),
            "the sweep is bounded to its own scope"
        );
        let (tombstone, owns_hydration) = registry.entry_for(&retired);
        assert!(!owns_hydration, "retirement tombstones survive the sweep");
        assert_eq!(tombstone.lifecycle(), VectorCacheLifecycle::Closed);
        assert_eq!(
            released_guard.store().visible_seq(),
            0,
            "a retained guard keeps its released store usable"
        );
        drop(released_guard);
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

        let writes = super::super::commit::VectorCacheWriteSet::default();
        writes.dirty_rows_for(&handle).mark_node_dirty(7);
        let write = writes.entries().pop().unwrap();
        let aborted = registry.prepare_commit(&write).unwrap();
        assert!(entry.pending_dirty.is_node_dirty(7));
        assert!(entry.pending_dirty.has_pending_commits());
        aborted.resolve(VectorCacheCommitOutcome::Rejected).await;
        assert!(!entry.pending_dirty.is_node_dirty(7));
        assert!(!entry.pending_dirty.has_pending_commits());
        assert!(store.get_upper_vector(7).is_some());
        assert_eq!(entry.pending_dirty.generation(), 0);

        let committed = registry.prepare_commit(&write).unwrap();
        assert!(entry.pending_dirty.is_node_dirty(7));
        committed
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;
        assert!(!entry.pending_dirty.is_node_dirty(7));
        assert!(store.get_simhash(7).is_none());
        assert!(store.get_upper_vector(7).is_none());
        assert_eq!(entry.pending_dirty.generation(), 1);
    }

    #[tokio::test]
    async fn hydration_reservations_publish_immutable_newer_stores_single_flight() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
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
        let old_guard = registry.resident_guard_for(&handle).unwrap();

        let refresh = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must grant one refresh")
            }
        };
        assert!(matches!(
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded),
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
        let new_guard = registry.resident_guard_for(&handle).unwrap();
        assert_eq!(new_guard.store().visible_seq(), 2);
        assert_eq!(
            new_guard.store().get_upper_vector(7).unwrap().as_ref(),
            b"second"
        );
        let equal_refresh = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
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
            .resident_guard_for(&handle)
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
        let initial = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
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
        let (discarded, owns_hydration) = registry.entry_for(&handle);
        assert!(!owns_hydration);
        assert_eq!(discarded.lifecycle(), VectorCacheLifecycle::Vacant);
        assert!(
            !registry.forget_validated_closed(&handle),
            "a discarded hydration is not a retirement tombstone"
        );

        let initial = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("a vacant entry must grant a new initial hydration")
            }
        };
        let resident = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            2,
        ));
        resident.insert_upper_vector(7, Bytes::from_static(b"resident"));
        assert!(initial.finish(Arc::clone(&resident)).await);
        let refresh = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
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
            registry.resident_guard_for(&handle).unwrap().store(),
            &resident
        ));
    }

    #[test]
    fn dropped_initial_hydration_returns_the_entry_to_vacant() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        let entry = Arc::clone(&initial.entry);
        drop(initial);

        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Vacant);
        assert!(matches!(
            entry.acquire_resident_guard(),
            Err(VectorCacheReadGuardError::Unavailable(
                VectorCacheLifecycle::Vacant
            ))
        ));
        assert_eq!(entry.estimated_bytes(), 0);
        assert!(!registry.forget_validated_closed(&handle));
        let VectorCacheHydration::Initial(retry) =
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        else {
            panic!("a vacant entry grants a new initial hydration");
        };
        assert!(matches!(
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded),
            VectorCacheHydration::Unavailable(VectorCacheLifecycle::Hydrating)
        ));
        drop(retry);
    }

    #[tokio::test]
    async fn retirement_waits_for_refresh_and_discards_its_unpublished_store() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
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
        let refresh = match registry
            .prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        {
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
            registry.resident_guard_for(&handle),
            Err(VectorCacheReadGuardError::Unavailable(
                VectorCacheLifecycle::Closed
            ))
        ));
    }

    #[tokio::test]
    async fn exact_sequence_visibility_attaches_only_the_hydrated_sequence() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        assert!(publish_initial(&registry, &handle, store_at(&handle, 5, 7, b"cached")).await);

        assert_eq!(staleness(&registry, &handle, 5), None);
        assert_eq!(
            staleness(&registry, &handle, 4),
            Some(VectorCacheStaleness::SnapshotSequence)
        );
        assert_eq!(
            staleness(&registry, &handle, 6),
            Some(VectorCacheStaleness::SnapshotSequence),
            "without commit fences a newer snapshot may see rows the store lacks"
        );
        let mut refresh = reserve_refresh(&registry, &handle);
        assert!(refresh.retain_if_current(5).await.is_some());
        let mut refresh = reserve_refresh(&registry, &handle);
        assert_eq!(
            refresh.retain_if_current(6).await,
            None,
            "a newer exact sequence always rehydrates"
        );
    }

    #[tokio::test]
    async fn commit_fenced_visibility_requires_an_older_store_and_a_quiet_evicted_fence() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let resident = store_at(&handle, 5, 7, b"stale");
        resident.insert_upper_vector(8, Bytes::from_static(b"untouched"));
        assert!(publish_initial(&registry, &handle, Arc::clone(&resident)).await);

        assert_eq!(
            staleness(&registry, &handle, 4),
            Some(VectorCacheStaleness::SnapshotSequence)
        );
        assert_eq!(staleness(&registry, &handle, 5), None);
        assert_eq!(
            staleness(&registry, &handle, 9),
            None,
            "writes that pass no vector fence keep an older store current"
        );

        let rejected = prepare_dirty_commit(&registry, &handle, 7);
        assert_eq!(
            staleness(&registry, &handle, 9),
            Some(VectorCacheStaleness::CommitInFlight)
        );
        rejected.resolve(VectorCacheCommitOutcome::Rejected).await;
        assert_eq!(staleness(&registry, &handle, 9), None);
        assert!(resident.get_upper_vector(7).is_some());

        let applied = prepare_dirty_commit(&registry, &handle, 7);
        assert_eq!(
            staleness(&registry, &handle, 9),
            Some(VectorCacheStaleness::CommitInFlight)
        );
        applied
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;
        let guard = registry
            .read_guard_for(&handle, 9)
            .expect("an evicted store stays current for newer snapshots");
        assert!(guard.store().get_upper_vector(7).is_none());
        assert!(guard.store().get_upper_vector(8).is_some());
        drop(guard);

        let second = prepare_dirty_commit(&registry, &handle, 8);
        second.resolve(VectorCacheCommitOutcome::MaybeApplied).await;
        assert_eq!(
            staleness(&registry, &handle, 9),
            None,
            "consecutive evictions keep advancing the store's evicted generation"
        );
        assert!(resident.get_upper_vector(8).is_none());
    }

    #[tokio::test]
    async fn unresolved_commit_drop_invalidates_until_a_refresh_rehydrates() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let resident = store_at(&handle, 5, 7, b"maybe-stale");
        assert!(publish_initial(&registry, &handle, Arc::clone(&resident)).await);

        let abandoned = prepare_dirty_commit(&registry, &handle, 7);
        drop(abandoned);

        assert_eq!(
            staleness(&registry, &handle, 9),
            Some(VectorCacheStaleness::UnevictedCommit),
            "an unobserved outcome may have changed rows the store still holds"
        );
        let mut refresh = reserve_refresh(&registry, &handle);
        assert_eq!(refresh.retain_if_current(9).await, None);
        assert!(refresh.finish(store_at(&handle, 9, 7, b"fresh")).await);
        let guard = registry.read_guard_for(&handle, 9).unwrap();
        assert_eq!(
            guard.store().get_upper_vector(7).as_deref(),
            Some(b"fresh".as_slice())
        );
        assert_eq!(
            staleness(&registry, &handle, 8),
            Some(VectorCacheStaleness::SnapshotSequence)
        );
        assert_eq!(
            resident.get_upper_vector(7).as_deref(),
            Some(b"maybe-stale".as_slice()),
            "the superseded store is never attached again"
        );
    }

    #[tokio::test]
    async fn commit_fenced_refresh_retains_across_sequences_until_a_commit_resolves() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let resident = store_at(&handle, 3, 7, b"resident");
        assert!(publish_initial(&registry, &handle, Arc::clone(&resident)).await);

        let mut refresh = reserve_refresh(&registry, &handle);
        assert_eq!(
            refresh.retain_if_current(10).await,
            Some(resident.estimated_bytes()),
            "non-vector commits advance the snapshot without forcing a rescan"
        );
        assert!(Arc::ptr_eq(
            registry.read_guard_for(&handle, 10).unwrap().store(),
            &resident
        ));

        prepare_dirty_commit(&registry, &handle, 9)
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;
        let mut refresh = reserve_refresh(&registry, &handle);
        assert_eq!(
            refresh.retain_if_current(10).await,
            None,
            "a resolved vector commit may have added rows, so the store is rescanned"
        );
        assert!(refresh.finish(store_at(&handle, 10, 9, b"added")).await);
        let mut refresh = reserve_refresh(&registry, &handle);
        assert!(refresh.retain_if_current(12).await.is_some());
    }

    #[tokio::test]
    async fn commit_prepared_before_admission_discards_a_crossing_initial_hydration() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let pending = prepare_dirty_commit(&registry, &handle, 7);
        assert!(matches!(
            registry.resident_guard_for(&handle),
            Err(VectorCacheReadGuardError::Absent)
        ));

        let VectorCacheHydration::Initial(initial) =
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        else {
            panic!("the first hydration owns the new entry");
        };
        assert!(
            Arc::ptr_eq(&initial.entry.pending_dirty, &pending.fence),
            "an entry created after the commit adopts the commit's fence"
        );
        let crossed = store_at(&handle, 1, 7, b"pre-commit");
        pending
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;

        assert!(!initial.finish(Arc::clone(&crossed)).await);
        assert!(crossed.get_upper_vector(7).is_none());
        assert!(publish_initial(&registry, &handle, store_at(&handle, 2, 7, b"post-commit")).await);
        let guard = registry.read_guard_for(&handle, 2).unwrap();
        assert_eq!(
            guard.store().get_upper_vector(7).as_deref(),
            Some(b"post-commit".as_slice())
        );
    }

    #[tokio::test]
    async fn store_published_during_an_unadmitted_commit_waits_for_a_refresh() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let pending = prepare_dirty_commit(&registry, &handle, 7);
        assert!(
            publish_initial(&registry, &handle, store_at(&handle, 1, 7, b"pre-commit")).await,
            "publication before the commit resolves observes an unchanged generation"
        );
        assert_eq!(
            staleness(&registry, &handle, 5),
            Some(VectorCacheStaleness::CommitInFlight)
        );

        pending
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;

        assert_eq!(
            staleness(&registry, &handle, 5),
            Some(VectorCacheStaleness::UnevictedCommit),
            "the commit had no entry to evict from, so the store is never proven current"
        );
        let mut refresh = reserve_refresh(&registry, &handle);
        assert_eq!(refresh.retain_if_current(5).await, None);
        assert!(
            refresh
                .finish(store_at(&handle, 5, 7, b"post-commit"))
                .await
        );
        assert_eq!(staleness(&registry, &handle, 5), None);
    }

    #[tokio::test]
    async fn store_superseded_while_a_commit_is_pending_is_never_attached_again() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let superseded = store_at(&handle, 1, 7, b"old");
        assert!(publish_initial(&registry, &handle, Arc::clone(&superseded)).await);
        let earlier_reader = registry.read_guard_for(&handle, 1).unwrap();

        let pending = prepare_dirty_commit(&registry, &handle, 7);
        let refresh = reserve_refresh(&registry, &handle);
        let replacement = store_at(&handle, 3, 7, b"new");
        assert!(
            refresh.finish(Arc::clone(&replacement)).await,
            "a refresh finishing before the commit resolves may publish"
        );
        assert_eq!(
            staleness(&registry, &handle, 3),
            Some(VectorCacheStaleness::CommitInFlight),
            "no reader may attach either store while the commit is unresolved"
        );
        pending
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;

        let later_reader = registry.read_guard_for(&handle, 3).unwrap();
        assert!(Arc::ptr_eq(later_reader.store(), &replacement));
        assert!(
            replacement.get_upper_vector(7).is_none(),
            "the commit evicts from the store that is resident when it resolves"
        );
        assert!(
            Arc::ptr_eq(earlier_reader.store(), &superseded),
            "a reader whose snapshot precedes the commit keeps its immutable store"
        );
        assert_eq!(
            staleness(&registry, &handle, 2),
            Some(VectorCacheStaleness::SnapshotSequence),
            "the superseded store can no longer be granted to any snapshot"
        );
    }

    #[tokio::test]
    async fn refresh_reserved_before_a_commit_resolves_is_discarded() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let resident = store_at(&handle, 1, 7, b"old");
        assert!(publish_initial(&registry, &handle, Arc::clone(&resident)).await);
        let pending = prepare_dirty_commit(&registry, &handle, 7);
        let refresh = reserve_refresh(&registry, &handle);
        let crossed = store_at(&handle, 2, 7, b"crossed");

        pending
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;

        assert!(!refresh.finish(Arc::clone(&crossed)).await);
        assert!(crossed.get_upper_vector(7).is_none());
        assert!(Arc::ptr_eq(
            registry.read_guard_for(&handle, 2).unwrap().store(),
            &resident
        ));
        assert!(resident.get_upper_vector(7).is_none());
    }

    #[tokio::test]
    async fn commit_fences_outlive_replaced_entries_and_are_pruned_once_orphaned() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let identity = VectorCacheIdentity::from_validated(&handle);
        assert!(publish_initial(&registry, &handle, store_at(&handle, 1, 7, b"old")).await);
        let pending = prepare_dirty_commit(&registry, &handle, 7);

        registry.sweep(
            DataScope::LegacyUnscoped,
            &HashSet::new(),
            VectorCacheSweep::InactiveEntries,
        );
        assert!(matches!(
            registry.resident_guard_for(&handle),
            Err(VectorCacheReadGuardError::Absent)
        ));
        assert!(
            registry.state.read().fences.contains_key(&identity),
            "an unresolved commit keeps its fence after the entry is released"
        );
        let VectorCacheHydration::Initial(initial) =
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        else {
            panic!("a released identity hydrates again");
        };
        assert!(Arc::ptr_eq(&initial.entry.pending_dirty, &pending.fence));
        pending
            .resolve(VectorCacheCommitOutcome::MaybeApplied)
            .await;
        assert!(
            !initial.finish(store_at(&handle, 1, 7, b"crossed")).await,
            "the replacement entry observes the commit through the shared fence"
        );

        registry.sweep(
            DataScope::LegacyUnscoped,
            &HashSet::new(),
            VectorCacheSweep::InactiveEntries,
        );
        assert!(registry.state.read().fences.is_empty());

        let kept = validated(2);
        assert!(publish_initial(&registry, &kept, store_at(&kept, 1, 7, b"kept")).await);
        let unadmitted = validated(3);
        let pending = prepare_dirty_commit(&registry, &unadmitted, 7);
        registry.sweep(
            DataScope::LegacyUnscoped,
            &HashSet::new(),
            VectorCacheSweep::OrphanFences,
        );
        assert_eq!(registry.state.read().fences.len(), 2);
        pending.resolve(VectorCacheCommitOutcome::Rejected).await;
        registry.sweep(
            DataScope::LegacyUnscoped,
            &HashSet::new(),
            VectorCacheSweep::OrphanFences,
        );
        assert!(
            registry.resident_guard_for(&kept).is_ok(),
            "writer sweeps never release entries"
        );
        let fences = &registry.state.read().fences;
        assert_eq!(fences.len(), 1);
        assert!(fences.contains_key(&VectorCacheIdentity::from_validated(&kept)));
    }

    #[tokio::test]
    async fn forgetting_tombstones_releases_fences_not_held_by_unresolved_commits() {
        let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
        let handle = validated(1);
        let identity = VectorCacheIdentity::from_validated(&handle);
        let pending = prepare_dirty_commit(&registry, &handle, 7);
        assert_eq!(
            registry.retire(&handle).await,
            VectorCacheRetirement::ClosedEmpty
        );
        assert!(registry.forget_closed(&identity));
        assert!(registry.state.read().fences.contains_key(&identity));
        pending.resolve(VectorCacheCommitOutcome::Rejected).await;

        assert_eq!(
            registry.retire(&handle).await,
            VectorCacheRetirement::ClosedEmpty
        );
        assert!(registry.forget_closed(&identity));
        assert!(registry.state.read().fences.is_empty());

        let (authority, generation) = cleaning_authority();
        assert!(publish_initial(&registry, &generation, store_at(&generation, 1, 7, b"x")).await);
        assert_eq!(registry.retire_cleanup_generation(&authority).await, 1);
        assert!(registry.forget_cleanup_generation(&authority));
        assert!(registry.state.read().fences.is_empty());
    }

    #[tokio::test]
    async fn vacant_entries_retire_directly_to_closed() {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let VectorCacheHydration::Initial(initial) =
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded)
        else {
            panic!("absent identity must grant initial hydration");
        };
        let entry = Arc::clone(&initial.entry);
        drop(initial);
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Vacant);

        assert_eq!(
            registry.retire(&handle).await,
            VectorCacheRetirement::ClosedResident
        );
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Closed);
        assert!(matches!(
            registry.prepare_hydration(&handle, store::VectorMemoryAdmissionBudget::Unbounded),
            VectorCacheHydration::Unavailable(VectorCacheLifecycle::Closed)
        ));
        assert!(registry.forget_validated_closed(&handle));
    }
}
