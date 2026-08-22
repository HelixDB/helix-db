//! Descriptor-bound construction for managed vector mutation indexes.
//!
//! [`managed_vector_write_index`] is the only production boundary that may
//! attach transaction-local cache dirty tracking to a [`VectorIndex`]. It
//! validates the requested distance semantic against the complete generation
//! descriptor before combining physical identity and write state. Legacy
//! mutations remain cache-disabled and construct their deployed physical index
//! directly at the compatibility call site.

use std::sync::Arc;

use super::memory_store::VectorMemoryDirtyRows;
use super::{
    Distance, ValidatedVectorGenerationHandle, VectorGenerationValidationError, VectorIndex,
};

/// Constructs one managed mutation index bound to an exact generation handle.
///
/// The returned index records every changed hot-cache row in `dirty_rows` but
/// never reads or mutates a shared resident store during the transaction.
/// Commit later fences and evicts those rows through the registry; abort drops
/// the tracker without a cache effect.
pub(crate) fn managed_vector_write_index<D: Distance>(
    handle: &ValidatedVectorGenerationHandle,
    dirty_rows: Arc<VectorMemoryDirtyRows>,
    simhasher_registry: Arc<super::SimHasherRegistry>,
) -> Result<VectorIndex<D>, VectorGenerationValidationError> {
    handle.validate_distance::<D>()?;
    Ok(VectorIndex::from_generation(handle)
        .with_simhasher_registry(simhasher_registry)
        .with_simhash_identity(handle.simhash_identity())
        .with_write_dirty_rows(dirty_rows))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::search::vector::distance::{Cosine, Euclidean};
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

    /// Builds one cosine descriptor for factory compatibility tests.
    fn handle() -> ValidatedVectorGenerationHandle {
        ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                9,
                "managed-write-factory".to_string(),
                90,
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
    fn factory_rejects_distance_mismatch_before_index_construction() {
        let handle = handle();
        let dirty = Arc::new(VectorMemoryDirtyRows::default());
        let registry = Arc::new(super::super::SimHasherRegistry::default());
        assert!(managed_vector_write_index::<Cosine>(
            &handle,
            Arc::clone(&dirty),
            Arc::clone(&registry),
        )
        .is_ok());
        assert!(managed_vector_write_index::<Euclidean>(&handle, dirty, registry).is_err());
    }
}
