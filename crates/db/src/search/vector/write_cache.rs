//! Transaction-owned vector cache mutation tracking.
//!
//! [`VectorCacheWriteSet`] groups dirty cache rows by the complete validated
//! generation identity. Vector mutation handles receive only the tracker for
//! their exact descriptor. The set itself performs no shared-cache mutation;
//! commit code later converts its immutable entry snapshot into registry
//! pending guards, while abort simply drops it.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::memory_registry::VectorCacheIdentity;
use super::memory_store::VectorMemoryDirtyRows;
use super::ValidatedVectorGenerationHandle;

/// One exact generation and the rows dirtied by a transaction.
#[derive(Debug, Clone)]
pub(crate) struct VectorCacheWriteEntry {
    handle: ValidatedVectorGenerationHandle,
    dirty_rows: Arc<VectorMemoryDirtyRows>,
}

impl VectorCacheWriteEntry {
    /// Returns the descriptor proof used to locate the registry entry.
    pub(crate) const fn handle(&self) -> &ValidatedVectorGenerationHandle {
        &self.handle
    }

    /// Returns the transaction-local rows to fence and evict at commit.
    pub(crate) fn dirty_rows(&self) -> &Arc<VectorMemoryDirtyRows> {
        &self.dirty_rows
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
        Arc::clone(
            &entries
                .entry(identity)
                .or_insert_with(|| VectorCacheWriteEntry {
                    handle: handle.clone(),
                    dirty_rows: Arc::new(VectorMemoryDirtyRows::default()),
                })
                .dirty_rows,
        )
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

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::encoding::keys::tenant::DataScope;
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
                crate::index_v2::IndexElementKind::Node,
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
}
