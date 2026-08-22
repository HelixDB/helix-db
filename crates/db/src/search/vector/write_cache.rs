//! Transaction-owned vector cache mutation tracking.
//!
//! [`VectorCacheWriteSet`] groups the one post-commit cache effect for each
//! complete validated generation identity. Ordinary writes track dirty rows;
//! physical partition reclamation replaces that effect with exact retirement.
//! The set itself performs no shared-cache mutation. Commit code publishes its
//! immutable snapshot only after durable storage commit, while abort drops it.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::memory_registry::VectorCacheIdentity;
use super::memory_store::VectorMemoryDirtyRows;
use super::ValidatedVectorGenerationHandle;

/// One exact generation and its transaction-owned post-commit cache effect.
#[derive(Debug, Clone)]
pub(crate) struct VectorCacheWriteEntry {
    handle: ValidatedVectorGenerationHandle,
    effect: VectorCacheCommitEffect,
}

/// Only one post-commit cache effect can own an exact physical generation.
#[derive(Debug, Clone)]
enum VectorCacheCommitEffect {
    /// Evict rows whose durable storage changed.
    EvictDirty(Arc<VectorMemoryDirtyRows>),
    /// Close and forget an empty physical partition after its rows disappear.
    Retire,
}

impl VectorCacheWriteEntry {
    /// Returns the descriptor proof used to locate the registry entry.
    pub(crate) const fn handle(&self) -> &ValidatedVectorGenerationHandle {
        &self.handle
    }

    /// Returns the transaction-local rows to fence and evict at commit.
    pub(crate) const fn dirty_rows(&self) -> Option<&Arc<VectorMemoryDirtyRows>> {
        match &self.effect {
            VectorCacheCommitEffect::EvictDirty(dirty_rows) => Some(dirty_rows),
            VectorCacheCommitEffect::Retire => None,
        }
    }

    /// Returns the exact handle only for a post-commit physical retirement.
    pub(crate) const fn retirement(&self) -> Option<&ValidatedVectorGenerationHandle> {
        match self.effect {
            VectorCacheCommitEffect::EvictDirty(_) => None,
            VectorCacheCommitEffect::Retire => Some(&self.handle),
        }
    }
}

/// Complete vector cache write ownership for one database transaction.
#[derive(Debug)]
pub(crate) struct VectorCacheWriteSet {
    entries: Mutex<HashMap<VectorCacheIdentity, VectorCacheWriteEntry>>,
    simhasher_registry: Arc<super::SimHasherRegistry>,
}

impl VectorCacheWriteSet {
    /// Creates transaction tracking bound to its database's projection owner.
    pub(crate) fn new(simhasher_registry: Arc<super::SimHasherRegistry>) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            simhasher_registry,
        }
    }

    /// Clones the projection owner for exact vector-index construction.
    pub(crate) fn simhasher_registry(&self) -> Arc<super::SimHasherRegistry> {
        Arc::clone(&self.simhasher_registry)
    }

    /// Returns the single dirty tracker for an exact validated generation.
    ///
    /// Repeated mutations in one transaction share the tracker. Full identity
    /// equality is checked by the map key, so logical-name reuse across
    /// generations cannot merge write sets.
    pub(crate) fn dirty_rows_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> Arc<VectorMemoryDirtyRows> {
        let identity = VectorCacheIdentity::from_validated(handle);
        let mut entries = self.entries.lock();
        let entry = entries
            .entry(identity)
            .or_insert_with(|| VectorCacheWriteEntry {
                handle: handle.clone(),
                effect: VectorCacheCommitEffect::EvictDirty(Arc::new(
                    VectorMemoryDirtyRows::default(),
                )),
            });
        let VectorCacheCommitEffect::EvictDirty(dirty_rows) = &entry.effect else {
            panic!("a retired vector cache generation cannot receive later physical writes");
        };
        Arc::clone(dirty_rows)
    }

    /// Replaces dirty-row eviction with exact post-commit physical retirement.
    ///
    /// The shared registry remains untouched until storage commits. Dropping
    /// the transaction therefore discards this effect without closing a cache
    /// entry that still has durable physical ownership.
    pub(crate) fn retire_after_commit(&self, handle: &ValidatedVectorGenerationHandle) {
        self.entries.lock().insert(
            VectorCacheIdentity::from_validated(handle),
            VectorCacheWriteEntry {
                handle: handle.clone(),
                effect: VectorCacheCommitEffect::Retire,
            },
        );
    }

    /// Takes a stable snapshot for pre-commit pending-guard acquisition.
    pub(crate) fn entries(&self) -> Vec<VectorCacheWriteEntry> {
        self.entries.lock().values().cloned().collect()
    }
}

impl Default for VectorCacheWriteSet {
    fn default() -> Self {
        Self::new(Arc::new(super::SimHasherRegistry::default()))
    }
}

#[cfg(feature = "production-coverage")]
pub(crate) mod production_contracts {
    use std::num::NonZeroU64;
    use std::panic::AssertUnwindSafe;

    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::search::vector::distance::Cosine;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

    /// Proves retirement closes the transaction-local physical-write state.
    pub(crate) fn run() {
        let handle = ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                8,
                "production-write-cache-retirement".to_string(),
                80,
                NonZeroU64::MIN,
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let writes = VectorCacheWriteSet::default();
        writes.retire_after_commit(&handle);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            drop(writes.dirty_rows_for(&handle));
        }));
        assert!(result.is_err(), "retired generations reject later writes");
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::search::vector::distance::Cosine;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

    /// Builds a distinct descriptor identity for write-set isolation tests.
    fn handle(generation: u64) -> ValidatedVectorGenerationHandle {
        ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                8,
                format!("write-cache-generation-{generation}"),
                80,
                NonZeroU64::new(generation).unwrap(),
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn write_set_shares_exact_identity_and_isolates_successors() {
        let writes = VectorCacheWriteSet::default();
        let first = handle(1);
        let successor = handle(2);
        let first_rows = writes.dirty_rows_for(&first);
        let same_rows = writes.dirty_rows_for(&first);
        let successor_rows = writes.dirty_rows_for(&successor);

        assert!(Arc::ptr_eq(&first_rows, &same_rows));
        assert!(!Arc::ptr_eq(&first_rows, &successor_rows));
        first_rows.mark_node_dirty(7);
        assert!(same_rows.is_node_dirty(7));
        assert!(!successor_rows.is_node_dirty(7));
        assert_eq!(writes.entries().len(), 2);
    }

    #[test]
    fn retirement_replaces_dirty_eviction_for_one_exact_generation() {
        let writes = VectorCacheWriteSet::default();
        let retired = handle(1);
        writes.dirty_rows_for(&retired).mark_node_dirty(7);
        writes.retire_after_commit(&retired);

        let entries = writes.entries();
        let [entry] = entries.as_slice() else {
            panic!("one exact cache effect remains")
        };
        assert!(entry.dirty_rows().is_none());
        assert_eq!(entry.retirement(), Some(&retired));
    }
}
