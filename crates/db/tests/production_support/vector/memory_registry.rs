//! Production-codec contracts for the vector memory-registry state machine.
//!
//! This module is attached as a feature-gated child of the production registry
//! module, which gives the scenarios access to private state-machine boundaries
//! without widening the default crate API. The contracts exercise the real
//! registry, descriptor identity, memory store, and write-cache types; they
//! create no alternate codec or persistence path.

use std::num::NonZeroU64;

use bytes::Bytes;

use crate::search::vector::distance::Cosine;
use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

use super::*;

/// Builds a validated current-format generation for registry contracts.
///
/// Each generation uses the production descriptor validation path so registry
/// identity assertions cover the same values accepted by runtime publication.
fn validated(generation: u64) -> ValidatedVectorGenerationHandle {
    let identity = VectorGenerationIdentity::try_new(
        DataScope::LegacyUnscoped,
        7,
        format!("vector-cache-generation-{generation}"),
        70,
        NonZeroU64::new(generation).unwrap(),
        1,
        crate::index_v2::IndexElementKind::Node,
        VectorDimension::try_new(3).unwrap(),
    )
    .unwrap();
    ValidatedVectorGenerationHandle::create_current::<Cosine>(identity).unwrap()
}

/// Builds an unpublished store whose identity is derived from the descriptor.
///
/// Callers must publish it through a hydration or refresh guard; dropping or
/// rejecting that guard verifies that unpublished cache contents are cleared.
fn store(identity: &VectorCacheIdentity) -> Arc<VectorMemoryStore> {
    Arc::new(VectorMemoryStore::new(
        identity.scope(),
        identity.physical_index_id(),
        0,
    ))
}

/// Exercises cache identity, hydration, refresh, lease, retirement, and commit arms.
///
/// The owning module re-exports this runner only under `production-coverage`.
/// The integration harness invokes it as one contract so the measured lines
/// belong to production modules rather than to this support source.
pub(crate) async fn run() {
    let first = VectorCacheIdentity::from_validated(&validated(1));
    let same = VectorCacheIdentity::from_validated(&validated(1));
    let successor = VectorCacheIdentity::from_validated(&validated(2));
    assert_eq!(first, same);
    assert_ne!(first, successor);
    assert_eq!(
        first.generation(),
        crate::index_v2::IndexGenerationId::initial()
    );
    assert_eq!(first.index_id(), crate::index_v2::IndexId::new(7).unwrap());
    assert_eq!(first.physical_index_id(), 70);
    assert_eq!(
        first.record_revision(),
        crate::index_v2::IndexRevision::initial()
    );

    {
        let registry = Arc::new(VectorCacheRegistry::default());
        let handle = validated(1);
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(store(entry.identity())));
        let lease = entry.acquire_lease().unwrap();
        assert_eq!(lease.identity(), entry.identity());
        let retirement_registry = Arc::clone(&registry);
        let retirement_handle = handle.clone();
        let retirement =
            tokio::spawn(async move { retirement_registry.retire(&retirement_handle).await });
        tokio::task::yield_now().await;
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Retiring);
        assert!(matches!(
            entry.acquire_lease(),
            Err(VectorCacheLeaseError::Unavailable(
                VectorCacheLifecycle::Retiring
            ))
        ));
        assert!(!retirement.is_finished());
        drop(lease);
        assert_eq!(
            retirement.await.unwrap(),
            VectorCacheRetirement::ClosedResident
        );
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Closed);
        assert!(entry.acquire_lease().is_err());
        assert_eq!(
            registry.retire(&handle).await,
            VectorCacheRetirement::ClosedResident
        );
    }

    {
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

    {
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

    {
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

    {
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

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(store(entry.identity())));
        let duplicate_finish = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            entry.finish_hydration(store(entry.identity()))
        }));
        assert!(duplicate_finish.is_err());

        let closed_handle = validated(2);
        assert_eq!(
            registry.retire(&closed_handle).await,
            VectorCacheRetirement::ClosedEmpty
        );
        let (closed_entry, owns_hydration) = registry.entry_for(&closed_handle);
        assert!(!owns_hydration);
        let closed_finish = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            closed_entry.finish_hydration(store(closed_entry.identity()))
        }));
        assert!(closed_finish.is_err());
    }

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let identity = VectorCacheIdentity::from_validated(&handle);
        let resident = store(&identity);
        resident.insert_simhash(7, crate::search::vector::SimHash::from_bits(11));
        resident.insert_upper_vector(7, Bytes::from_static(b"vector"));
        resident.insert_upper_neighbors(2, 9, &[7]).unwrap();
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(Arc::clone(&resident)));
        let lease = entry.acquire_lease().unwrap();

        let writes = super::super::write_cache::VectorCacheWriteSet::default();
        let dirty_rows = writes.dirty_rows_for(&handle);
        dirty_rows.mark_node_dirty(7);
        dirty_rows.mark_upper_neighbors_dirty(2, 9);
        let pending = registry
            .prepare_commit(&writes.entries().pop().unwrap())
            .unwrap();

        let retirement = registry.retire(&handle);
        tokio::pin!(retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut retirement)
                .await
                .is_err()
        );
        assert_eq!(entry.lifecycle(), VectorCacheLifecycle::Retiring);
        pending.evict_after_commit(resident.visible_seq() + 1).await;
        assert!(resident.get_simhash(7).is_none());
        assert!(resident.get_upper_vector(7).is_none());
        assert!(resident.get_upper_neighbors_bytes(2, 9).is_none());
        drop(lease);
        assert_eq!(retirement.await, VectorCacheRetirement::ClosedResident);
    }

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        let writes = super::super::write_cache::VectorCacheWriteSet::default();
        writes.dirty_rows_for(&handle).mark_node_dirty(7);
        let pending = registry
            .prepare_commit(&writes.entries().pop().unwrap())
            .unwrap();

        let retirement = registry.retire(&handle);
        tokio::pin!(retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut retirement)
                .await
                .is_err()
        );
        pending.evict_after_commit(1).await;
        drop(initial);
        assert_eq!(retirement.await, VectorCacheRetirement::ClosedResident);
    }

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(store(entry.identity())));
        let lease = entry.acquire_lease().unwrap();

        let first_retirement = registry.retire(&handle);
        tokio::pin!(first_retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut first_retirement,)
                .await
                .is_err()
        );
        let second_retirement = registry.retire(&handle);
        tokio::pin!(second_retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut second_retirement,)
                .await
                .is_err()
        );
        drop(lease);
        assert_eq!(
            first_retirement.await,
            VectorCacheRetirement::ClosedResident
        );
        assert_eq!(
            second_retirement.await,
            VectorCacheRetirement::ClosedResident
        );
    }

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        assert!(matches!(
            registry.lease_for(&handle),
            Err(VectorCacheLeaseError::Absent)
        ));
        let empty_writes = super::super::write_cache::VectorCacheWriteSet::default();
        let empty_write = empty_writes.dirty_rows_for(&handle);
        assert!(registry
            .prepare_commit(&empty_writes.entries().pop().unwrap())
            .is_none());
        empty_write.mark_node_dirty(7);
        assert!(registry
            .prepare_commit(&empty_writes.entries().pop().unwrap())
            .is_none());
        let identity = VectorCacheIdentity::from_validated(&handle);
        assert!(!registry.forget_closed(&identity));

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
        assert!(!registry.forget_closed(&identity));
        let old_lease = registry.lease_for(&handle).unwrap();
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
        assert_eq!(old_lease.store().visible_seq(), 1);
        assert_eq!(
            old_lease.store().get_upper_vector(7).unwrap().as_ref(),
            b"first"
        );
        let new_lease = registry.lease_for(&handle).unwrap();
        assert_eq!(new_lease.store().visible_seq(), 2);
        assert_eq!(
            new_lease.store().get_upper_vector(7).unwrap().as_ref(),
            b"second"
        );

        let cancelled_refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must grant refresh")
            }
        };
        drop(cancelled_refresh);
        let stale_refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("cancelled refresh must release its reservation")
            }
        };
        let stale = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            1,
        ));
        stale.insert_upper_vector(7, Bytes::from_static(b"stale"));
        assert!(!stale_refresh.finish(Arc::clone(&stale)).await);
        assert!(stale.get_upper_vector(7).is_none());

        let equal_refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must allow equal-sequence budget replacement")
            }
        };
        let equal = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            2,
        ));
        assert!(equal_refresh.finish(Arc::clone(&equal)).await);
        assert!(registry
            .lease_for(&handle)
            .unwrap()
            .store()
            .get_upper_vector(7)
            .is_none());
        assert_eq!(
            new_lease.store().get_upper_vector(7).unwrap().as_ref(),
            b"second"
        );

        let dirty_refresh = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Refresh(refresh) => refresh,
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("ready identity must grant a fenced refresh")
            }
        };
        dirty_refresh.entry.pending_dirty.bump_generation();
        let dirty = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            3,
        ));
        dirty.insert_upper_vector(7, Bytes::from_static(b"dirty"));
        assert!(!dirty_refresh.finish(Arc::clone(&dirty)).await);
        assert!(dirty.get_upper_vector(7).is_none());
    }

    {
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

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        initial.entry.pending_dirty.bump_generation();
        let stale = Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            1,
        ));
        stale.insert_upper_vector(7, Bytes::from_static(b"stale"));
        assert!(!initial.finish(Arc::clone(&stale)).await);
        assert!(stale.get_upper_vector(7).is_none());
        assert!(registry.forget_validated_closed(&handle));
    }

    {
        let registry = VectorCacheRegistry::default();
        let handle = validated(1);
        let initial = match registry.prepare_hydration(&handle) {
            VectorCacheHydration::Initial(initial) => initial,
            VectorCacheHydration::Refresh(_) | VectorCacheHydration::Unavailable(_) => {
                panic!("absent identity must grant initial hydration")
            }
        };
        let retirement = registry.retire(&handle);
        tokio::pin!(retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut retirement)
                .await
                .is_err()
        );
        drop(initial);
        assert_eq!(retirement.await, VectorCacheRetirement::ClosedResident);
    }

    {
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
            registry.lease_for(&handle),
            Err(VectorCacheLeaseError::Unavailable(
                VectorCacheLifecycle::Closed
            ))
        ));
    }

    {
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
        let retirement = registry.retire(&handle);
        tokio::pin!(retirement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut retirement)
                .await
                .is_err()
        );
        drop(refresh);
        assert_eq!(retirement.await, VectorCacheRetirement::ClosedResident);
    }
}
