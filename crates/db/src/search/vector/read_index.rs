//! Descriptor-bound vector read-index construction.
//!
//! Production search receives [`ValidatedVectorReadIndex`] instead of choosing
//! an index name, scope, cache, and visibility independently. Managed indexes
//! may attach only a cache read guard for the exact validated generation when the
//! cache hydration sequence equals the request's SlateDB snapshot sequence.
//! All other states fall back to `DbReadOps` without observing cache contents.

use std::sync::Arc;

use slatedb::DbReadOps;

use super::memory_registry::{VectorCacheReadGuard, VectorCacheRegistry};
use super::{
    Distance, RestrictedVectorCandidates, SearchParams, SearchResult,
    ValidatedVectorGenerationHandle, VectorIndex, VectorIndexMetadata,
};
use crate::error::HelixDbError;

/// Storage visibility evidence available to a vector read factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorReadVisibility {
    /// SlateDB supplied an exact comparable snapshot sequence.
    Comparable(u64),
    /// The read source exposes no sequence safe for cache comparison.
    Unavailable,
}

/// Vector search façade bound to one physical identity and cache read guard.
pub(crate) struct ValidatedVectorReadIndex<D: Distance> {
    index: VectorIndex<D>,
    /// Retains active-reader ownership for the lifetime of the bound facade.
    _cache_read_guard: Option<VectorCacheReadGuard>,
}

impl<D: Distance> ValidatedVectorReadIndex<D> {
    /// Constructs a managed reader from one validated descriptor handle.
    ///
    /// A ready resident snapshot is attached when its full identity and
    /// hydration sequence both match. Missing, stale, newer, hydrating,
    /// retiring, and closed entries are safe storage fallbacks. The retained
    /// guard fences cache retirement until this façade is dropped.
    pub(crate) fn managed(
        handle: &ValidatedVectorGenerationHandle,
        registry: &VectorCacheRegistry,
        simhasher_registry: Arc<super::SimHasherRegistry>,
        visibility: VectorReadVisibility,
    ) -> Result<Self, super::VectorGenerationValidationError> {
        handle.validate_distance::<D>()?;
        let mut index = VectorIndex::from_generation(handle)
            .with_simhasher_registry(simhasher_registry)
            .with_simhash_identity(handle.simhash_identity());
        let cache_read_guard = match visibility {
            VectorReadVisibility::Comparable(sequence) => registry
                .read_guard_for(handle)
                .ok()
                .filter(|guard| guard.store().is_visible_to_snapshot(sequence)),
            VectorReadVisibility::Unavailable => None,
        };
        if let Some(guard) = &cache_read_guard {
            index = index.with_managed_read_cache(
                Arc::clone(guard.store()),
                Arc::clone(guard.pending_dirty()),
            )?;
        }
        Ok(Self {
            index,
            _cache_read_guard: cache_read_guard,
        })
    }

    /// Reads current physical metadata through the caller's request view.
    pub(crate) async fn get_metadata(
        &self,
        read: &(impl DbReadOps + Send + Sync),
    ) -> Result<Option<VectorIndexMetadata>, HelixDbError> {
        self.index.get_metadata(read).await
    }

    /// Runs HNSW search while retaining any exact-generation cache guard.
    pub(crate) async fn search(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        self.index.search(read, query, params).await
    }

    /// Ranks only IDs produced by an upstream graph traversal.
    pub(crate) async fn search_restricted(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
        candidates: &RestrictedVectorCandidates,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        self.index
            .search_restricted(read, query, params, candidates)
            .await
    }

    #[cfg(test)]
    fn has_cache_read_guard(&self) -> bool {
        self._cache_read_guard.is_some()
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/read_index.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::search::vector::distance::{Cosine, Euclidean};
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity, VectorMemoryStore};

    /// Builds one complete validated generation for factory boundary tests.
    fn handle() -> ValidatedVectorGenerationHandle {
        ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                4,
                "managed-read-factory".to_string(),
                40,
                NonZeroU64::MIN,
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn managed_factory_requires_exact_visibility_and_distance_identity() {
        let handle = handle();
        let registry = VectorCacheRegistry::default();
        let simhasher_registry = Arc::new(super::super::SimHasherRegistry::default());
        let (entry, owns_hydration) = registry.entry_for(&handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(Arc::new(VectorMemoryStore::new(
            DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            9,
        ))));

        let exact = ValidatedVectorReadIndex::<Cosine>::managed(
            &handle,
            &registry,
            Arc::clone(&simhasher_registry),
            VectorReadVisibility::Comparable(9),
        )
        .unwrap();
        assert!(exact.has_cache_read_guard());

        let stale = ValidatedVectorReadIndex::<Cosine>::managed(
            &handle,
            &registry,
            Arc::clone(&simhasher_registry),
            VectorReadVisibility::Comparable(10),
        )
        .unwrap();
        assert!(!stale.has_cache_read_guard());
        let unavailable = ValidatedVectorReadIndex::<Cosine>::managed(
            &handle,
            &registry,
            Arc::clone(&simhasher_registry),
            VectorReadVisibility::Unavailable,
        )
        .unwrap();
        assert!(!unavailable.has_cache_read_guard());
        assert!(matches!(
            ValidatedVectorReadIndex::<Euclidean>::managed(
                &handle,
                &registry,
                simhasher_registry,
                VectorReadVisibility::Comparable(9),
            ),
            Err(super::super::VectorGenerationValidationError::MetricMismatch)
        ));
    }
}
