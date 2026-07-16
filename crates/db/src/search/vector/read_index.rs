//! Descriptor-bound vector read-index construction.
//!
//! Production search receives [`ValidatedVectorReadIndex`] instead of choosing
//! an index name, scope, cache, and visibility independently. Managed indexes
//! may attach only a lease for the exact validated generation and only when the
//! cache hydration sequence equals the request's SlateDB snapshot sequence.
//! All other states fall back to `DbReadOps` without observing cache contents.

use std::sync::Arc;

use slatedb::DbReadOps;

use super::memory_registry::{VectorCacheLease, VectorCacheRegistry};
use super::{
    Distance, SearchParams, SearchResult, ValidatedVectorGenerationHandle, VectorIndex,
    VectorIndexMetadata,
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

/// Vector search façade bound to one physical identity and cache lease.
pub(crate) struct ValidatedVectorReadIndex<D: Distance> {
    index: VectorIndex<D>,
    /// Retains active-reader ownership for the lifetime of the bound facade.
    _cache_lease: Option<VectorCacheLease>,
}

impl<D: Distance> ValidatedVectorReadIndex<D> {
    /// Constructs a managed reader from one validated descriptor handle.
    ///
    /// A ready resident snapshot is attached when its full identity and
    /// hydration sequence both match. Missing, stale, newer, hydrating,
    /// retiring, and closed entries are safe storage fallbacks. The retained
    /// lease fences cache retirement until this façade is dropped.
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
        let cache_lease = match visibility {
            VectorReadVisibility::Comparable(sequence) => registry
                .lease_for(handle)
                .ok()
                .filter(|lease| lease.store().is_visible_to_snapshot(sequence)),
            VectorReadVisibility::Unavailable => None,
        };
        if let Some(lease) = &cache_lease {
            index = index.with_managed_read_cache(
                Arc::clone(lease.store()),
                Arc::clone(lease.pending_dirty()),
            )?;
        }
        Ok(Self {
            index,
            _cache_lease: cache_lease,
        })
    }

    /// Reads current physical metadata through the caller's request view.
    pub(crate) async fn get_metadata(
        &self,
        read: &(impl DbReadOps + Send + Sync),
    ) -> Result<Option<VectorIndexMetadata>, HelixDbError> {
        self.index.get_metadata(read).await
    }

    /// Runs HNSW search while retaining any exact-generation cache lease.
    pub(crate) async fn search(
        &self,
        read: &(impl DbReadOps + Send + Sync),
        query: &[f32],
        params: &SearchParams,
    ) -> Result<Vec<SearchResult>, HelixDbError> {
        self.index.search(read, query, params).await
    }

    #[cfg(test)]
    fn has_cache_lease(&self) -> bool {
        self._cache_lease.is_some()
    }
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/read_index.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::encoding::keys::tenant::DataScope;
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
                crate::index_v2::IndexElementKind::Node,
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
        assert!(exact.has_cache_lease());

        let stale = ValidatedVectorReadIndex::<Cosine>::managed(
            &handle,
            &registry,
            Arc::clone(&simhasher_registry),
            VectorReadVisibility::Comparable(10),
        )
        .unwrap();
        assert!(!stale.has_cache_lease());
        let unavailable = ValidatedVectorReadIndex::<Cosine>::managed(
            &handle,
            &registry,
            Arc::clone(&simhasher_registry),
            VectorReadVisibility::Unavailable,
        )
        .unwrap();
        assert!(!unavailable.has_cache_lease());
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
