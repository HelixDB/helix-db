//! Typed V2 storage marker, allocator, and global queue-pointer values.

use crate::encoding::v2::keys::scope::DataScope;

use super::{
    IndexGenerationId, IndexId, IndexOperationId, IndexOperationRevision, VectorPhysicalIndexId,
};

/// Canonical V2 index format number written by this implementation.
pub(crate) const CURRENT_INDEX_STORAGE_VERSION: u16 = 0x0004;

/// Decoded non-zero index storage format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct IndexStorageVersion(u16);

impl IndexStorageVersion {
    pub(crate) const CURRENT: Self = Self(CURRENT_INDEX_STORAGE_VERSION);

    pub(crate) fn new(value: u16) -> Result<Self, crate::encoding::error::EncodingError> {
        if value == 0 {
            return Err(crate::encoding::error::EncodingError::Custom(
                "index storage version must be non-zero".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub(crate) const fn get(self) -> u16 {
        self.0
    }
}

/// Typed next logical index ID watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LogicalIndexIdWatermark {
    pub(crate) next_id: IndexId,
}

/// Typed next vector physical ID watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VectorPhysicalIdWatermark {
    pub(crate) next_id: VectorPhysicalIndexId,
}

impl VectorPhysicalIdWatermark {
    /// Returns an adoptable pre-V2 physical ID only when it is non-zero and
    /// has never fallen below the V2 allocator watermark.
    pub(crate) fn eligible_legacy_source(
        self,
        raw_physical_id: u64,
    ) -> Option<VectorPhysicalIndexId> {
        let physical_index_id = VectorPhysicalIndexId::new(raw_physical_id).ok()?;
        (physical_index_id >= self.next_id).then_some(physical_index_id)
    }
}

/// Durable ownership state for one hash-derived pre-V2 vector namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum LegacyVectorPhysicalReservation {
    /// The physical rows remain owned exclusively by a persisted legacy definition.
    LegacySource,
    /// A hidden V2 generation is validating the unchanged legacy rows.
    AdoptionBuilding {
        index_id: IndexId,
        generation: IndexGenerationId,
        operation_id: IndexOperationId,
    },
    /// The exact active V2 generation owns the imported physical rows.
    AdoptedActive {
        index_id: IndexId,
        generation: IndexGenerationId,
    },
    /// A blocking migration owns deletion of the legacy-only namespace.
    RetiringSource {
        index_id: IndexId,
        generation: IndexGenerationId,
    },
}

impl LegacyVectorPhysicalReservation {
    pub(crate) const fn begin_adoption(
        self,
        index_id: IndexId,
        generation: IndexGenerationId,
        operation_id: IndexOperationId,
    ) -> Option<Self> {
        match self {
            Self::LegacySource => Some(Self::AdoptionBuilding {
                index_id,
                generation,
                operation_id,
            }),
            Self::AdoptionBuilding { .. }
            | Self::AdoptedActive { .. }
            | Self::RetiringSource { .. } => None,
        }
    }

    /// Fences a legacy source for cleanup after its exact V2 generation is active.
    pub(crate) const fn begin_retirement(
        self,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Option<Self> {
        match self {
            Self::LegacySource => Some(Self::RetiringSource {
                index_id,
                generation,
            }),
            Self::AdoptionBuilding { .. }
            | Self::AdoptedActive { .. }
            | Self::RetiringSource { .. } => None,
        }
    }

    pub(crate) fn activate(
        self,
        index_id: IndexId,
        generation: IndexGenerationId,
        operation_id: IndexOperationId,
    ) -> Option<Self> {
        match self {
            Self::AdoptionBuilding {
                index_id: owner,
                generation: owner_generation,
                operation_id: owner_operation,
            } if owner == index_id
                && owner_generation == generation
                && owner_operation == operation_id =>
            {
                Some(Self::AdoptedActive {
                    index_id,
                    generation,
                })
            }
            Self::LegacySource
            | Self::AdoptionBuilding { .. }
            | Self::AdoptedActive { .. }
            | Self::RetiringSource { .. } => None,
        }
    }

    pub(crate) fn abort(
        self,
        index_id: IndexId,
        generation: IndexGenerationId,
        operation_id: IndexOperationId,
    ) -> Option<Self> {
        match self {
            Self::AdoptionBuilding {
                index_id: owner,
                generation: owner_generation,
                operation_id: owner_operation,
            } if owner == index_id
                && owner_generation == generation
                && owner_operation == operation_id =>
            {
                Some(Self::LegacySource)
            }
            Self::LegacySource
            | Self::AdoptionBuilding { .. }
            | Self::AdoptedActive { .. }
            | Self::RetiringSource { .. } => None,
        }
    }

    pub(crate) fn is_owned_by(self, index_id: IndexId, generation: IndexGenerationId) -> bool {
        matches!(
            self,
            Self::AdoptedActive {
                index_id: owner,
                generation: owner_generation,
            } if owner == index_id && owner_generation == generation
        )
    }
}

/// Global operation pointer value cross-checked with its scoped operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OperationQueuePointerValue {
    pub(crate) scope: DataScope,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) record_revision: IndexOperationRevision,
}

/// Manifest revision observed when one Active compaction pointer was staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextCompactionPointerValue {
    pub(crate) revision: super::TextManifestRevision,
}

/// Values used only under the global V2 keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexV2MetadataValue {
    StorageVersion(IndexStorageVersion),
    LogicalIndexIdWatermark(LogicalIndexIdWatermark),
    VectorPhysicalIdWatermark(VectorPhysicalIdWatermark),
    OperationQueuePointer(OperationQueuePointerValue),
    LegacyVectorPhysicalReservation(LegacyVectorPhysicalReservation),
    TextCompactionPointer(TextCompactionPointerValue),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_vector_reservation_allows_only_exact_owner_transitions() {
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let operation_id = IndexOperationId::new_v4();
        let other_operation = IndexOperationId::new_v4();

        let building = LegacyVectorPhysicalReservation::LegacySource
            .begin_adoption(index_id, generation, operation_id)
            .expect("legacy source begins one adoption");
        assert!(building
            .activate(index_id, generation, other_operation)
            .is_none());
        assert_eq!(
            building.abort(index_id, generation, operation_id),
            Some(LegacyVectorPhysicalReservation::LegacySource)
        );
        let active = building
            .activate(index_id, generation, operation_id)
            .expect("exact building owner activates");
        assert!(active.is_owned_by(index_id, generation));
        assert!(active.abort(index_id, generation, operation_id).is_none());
        let retiring = LegacyVectorPhysicalReservation::LegacySource
            .begin_retirement(index_id, generation)
            .expect("legacy source begins one retirement");
        assert_eq!(
            retiring,
            LegacyVectorPhysicalReservation::RetiringSource {
                index_id,
                generation,
            }
        );
        assert!(retiring
            .begin_adoption(index_id, generation, operation_id)
            .is_none());
    }

    #[test]
    fn legacy_vector_adoption_eligibility_excludes_zero_and_consumed_ids() {
        let watermark = VectorPhysicalIdWatermark {
            next_id: VectorPhysicalIndexId::new(5).unwrap(),
        };
        assert_eq!(watermark.eligible_legacy_source(0), None);
        assert_eq!(watermark.eligible_legacy_source(4), None);
        assert_eq!(
            watermark.eligible_legacy_source(5),
            Some(VectorPhysicalIndexId::new(5).unwrap())
        );
        assert_eq!(
            watermark.eligible_legacy_source(u64::MAX),
            Some(VectorPhysicalIndexId::new(u64::MAX).unwrap())
        );
    }
}
