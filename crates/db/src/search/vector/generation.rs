//! Canonical V2 vector-generation capabilities.
//!
//! A handle can be projected only from an active canonical index record plus a
//! physical vector ID authorized by that record or its checked tenant mapping.
//! It binds row construction, distance semantics, SimHash identity, and cache
//! identity to the same scope, logical index, generation, physical ID, and
//! record revision. No physical namespace is derived from a display name.

#[cfg(any(test, feature = "production-coverage"))]
use std::num::NonZeroU64;
use std::num::{NonZeroU16, NonZeroUsize};

use crate::encoding::keys::scope::DataScope;
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexElementKind, IndexGenerationId, IndexId, IndexOperationId,
    IndexRecordV2, IndexRevision, IndexStateV2, PhysicalGeneration,
    ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition, VectorPhysicalIndexId,
    VectorPhysicalLayout, VectorRoutingLayoutV2,
};
use crate::search::vector::distance::{ActiveVectorSemantics, Distance};
use crate::search::vector::simhash_registry::SimHashIdentity;
use crate::search::vector::{VectorDimension, VectorDimensionError, VectorDistanceMetric};

/// Seed used by every currently supported persisted SimHash row.
pub(crate) const CURRENT_SIMHASH_SEED: u64 = 42;
/// Algorithm identity used by every currently supported persisted SimHash row.
pub(crate) const CURRENT_SIMHASH_ALGORITHM_VERSION: u16 = 1;

/// Complete expected identity supplied by a canonical lifecycle record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VectorGenerationIdentity {
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    physical_index_id: VectorPhysicalIndexId,
    record_revision: IndexRevision,
    physical_name: String,
    entity_kind: IndexElementKind,
    dimension: VectorDimension,
}

impl VectorGenerationIdentity {
    /// Constructs an exact identity for cache and factory contract tests.
    ///
    /// Every independent identity axis is explicit so tests cannot silently
    /// inherit a default and miss cache-aliasing regressions.
    #[cfg(any(test, feature = "production-coverage"))]
    #[allow(
        clippy::too_many_arguments,
        reason = "the test contract must expose every independent generation identity axis"
    )]
    pub(crate) fn try_new(
        scope: DataScope,
        index_id: u64,
        physical_name: String,
        physical_index_id: u64,
        generation: NonZeroU64,
        record_revision: u64,
        entity_kind: IndexElementKind,
        dimension: VectorDimension,
    ) -> Result<Self, VectorGenerationValidationError> {
        if physical_name.is_empty() {
            return Err(VectorGenerationValidationError::EmptyPhysicalName);
        }
        Ok(Self {
            scope,
            index_id: IndexId::new(index_id)?,
            generation: IndexGenerationId::new(generation.get())?,
            physical_index_id: VectorPhysicalIndexId::new(physical_index_id)?,
            record_revision: IndexRevision::new(record_revision)?,
            physical_name,
            entity_kind,
            dimension,
        })
    }

    /// Returns the storage scope.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Returns the stable logical index ID.
    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the complete physical name.
    pub(crate) fn physical_name(&self) -> &str {
        &self.physical_name
    }

    /// Returns the compact physical namespace.
    pub(crate) const fn physical_index_id(&self) -> VectorPhysicalIndexId {
        self.physical_index_id
    }

    /// Returns the non-zero generation.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    /// Returns the exact active record revision authorizing physical access.
    pub(crate) const fn record_revision(&self) -> IndexRevision {
        self.record_revision
    }

    /// Returns the entity kind used by test-only capability construction.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn entity_kind(&self) -> IndexElementKind {
        self.entity_kind
    }

    /// Returns the validated vector dimension.
    pub(crate) const fn dimension(&self) -> VectorDimension {
        self.dimension
    }
}

/// Temporary opaque vector capability for process-local cache tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedVectorGenerationHandle {
    identity: VectorGenerationIdentity,
    metric: VectorDistanceMetric,
    definition: ValidatedVectorIndexDefinition,
    routing_layout: VectorRoutingLayoutV2,
}

/// Hidden-generation capability issued only to the matching build operation.
///
/// This type couples physical row authority with the proof that lets the HNSW
/// mutation core skip an unnecessary delete before its first source-scan
/// insertion. It cannot be projected from `Active`, `Dropping`, or another
/// build operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedVectorBuildGenerationHandle {
    generation: ValidatedVectorGenerationHandle,
    fresh_insert: super::mutation::FreshVectorBuildProof,
}

/// Canonical authority for one vector generation already committed to cleanup.
///
/// The authority is generation-wide because partitioned cleanup must retire
/// every cache identity before it enumerates physical partition mappings. A
/// concrete HNSW namespace can be projected only by supplying its allocated
/// physical ID through [`Self::physical_generation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedVectorCleanupAuthority {
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    record_revision: IndexRevision,
    layout: VectorPhysicalLayout,
    descriptor: crate::index_lifecycle::VectorGenerationDescriptor,
    definition: ValidatedVectorIndexDefinition,
}

impl ValidatedVectorCleanupAuthority {
    /// Projects cleanup authority from the exact aborting or dropping operation.
    pub(crate) fn try_from_cleaning<D: Distance>(
        scope: DataScope,
        record: &IndexRecordV2,
        operation_id: IndexOperationId,
    ) -> Result<Self, VectorGenerationValidationError> {
        let (physical, owner) = match record.state() {
            IndexStateV2::Aborting {
                physical,
                build_operation_id,
            } => (physical, *build_operation_id),
            IndexStateV2::Dropping {
                physical,
                drop_operation_id,
            } => (physical, *drop_operation_id),
            IndexStateV2::Building { .. }
            | IndexStateV2::Active { .. }
            | IndexStateV2::Dropped { .. } => {
                return Err(VectorGenerationValidationError::NotCleaningVectorRecord);
            }
        };
        if owner != operation_id {
            return Err(VectorGenerationValidationError::CleanupOperationMismatch);
        }
        let PhysicalGeneration::Vector {
            generation,
            layout,
            descriptor,
        } = physical
        else {
            return Err(VectorGenerationValidationError::NotCleaningVectorRecord);
        };
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            return Err(VectorGenerationValidationError::NotCleaningVectorRecord);
        };
        let physical_index_id = match layout {
            VectorPhysicalLayout::Unpartitioned { physical_index_id } => *physical_index_id,
            VectorPhysicalLayout::Partitioned => VectorPhysicalIndexId::initial(),
        };
        ValidatedVectorGenerationHandle::try_from_parts::<D>(
            scope,
            record.index_id(),
            *generation,
            record.revision(),
            *layout,
            *descriptor,
            definition,
            physical_index_id,
        )?;
        Ok(Self {
            scope,
            index_id: record.index_id(),
            generation: *generation,
            record_revision: record.revision(),
            layout: *layout,
            descriptor: *descriptor,
            definition: definition.clone(),
        })
    }

    /// Returns the exact storage scope covered by the cleanup fence.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Returns the stable logical index covered by the cleanup fence.
    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the exact generation covered by the cleanup fence.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    /// Returns the physical layout whose namespaces cleanup may enumerate.
    pub(crate) const fn layout(&self) -> VectorPhysicalLayout {
        self.layout
    }

    /// Projects one concrete HNSW namespace under this cleanup authority.
    pub(crate) fn physical_generation<D: Distance>(
        &self,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<ValidatedVectorGenerationHandle, VectorGenerationValidationError> {
        ValidatedVectorGenerationHandle::try_from_parts::<D>(
            self.scope,
            self.index_id,
            self.generation,
            self.record_revision,
            self.layout,
            self.descriptor,
            &self.definition,
            physical_index_id,
        )
    }
}

impl ValidatedVectorBuildGenerationHandle {
    /// Projects one exact hidden generation from canonical durable ownership.
    ///
    /// Unpartitioned records authorize only their embedded physical ID.
    /// Partitioned callers must first obtain the physical ID through the V2
    /// mapping repository in the lifecycle transaction.
    pub(crate) fn try_from_building<D: Distance>(
        scope: DataScope,
        record: &IndexRecordV2,
        operation_id: IndexOperationId,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<Self, VectorGenerationValidationError> {
        let IndexStateV2::Building {
            physical:
                PhysicalGeneration::Vector {
                    generation,
                    layout,
                    descriptor,
                },
            build_operation_id,
        } = record.state()
        else {
            return Err(VectorGenerationValidationError::NotBuildingVectorRecord);
        };
        if *build_operation_id != operation_id {
            return Err(VectorGenerationValidationError::BuildOperationMismatch);
        }
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            return Err(VectorGenerationValidationError::NotBuildingVectorRecord);
        };
        let generation = ValidatedVectorGenerationHandle::try_from_parts::<D>(
            scope,
            record.index_id(),
            *generation,
            record.revision(),
            *layout,
            *descriptor,
            definition,
            physical_index_id,
        )?;
        Ok(Self {
            generation,
            fresh_insert: super::mutation::FreshVectorBuildProof::for_building_generation(),
        })
    }

    /// Borrows the descriptor-bound physical generation capability.
    pub(crate) const fn generation(&self) -> &ValidatedVectorGenerationHandle {
        &self.generation
    }

    /// Returns the freshness proof consumed by deterministic source scanning.
    pub(crate) const fn fresh_insert_proof(&self) -> super::mutation::FreshVectorBuildProof {
        self.fresh_insert
    }
}

impl ValidatedVectorGenerationHandle {
    /// Projects the currently supported runtime kernel from canonical Active state.
    ///
    /// Cache hydration is metric-agnostic after it has validated the complete
    /// descriptor. This constructor performs the closed metric dispatch once so
    /// the background loader cannot select a kernel independently from the
    /// canonical generation.
    pub(crate) fn try_from_active_current(
        active: &ActiveIndexHandle,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<Self, VectorGenerationValidationError> {
        let ActiveIndexHandle::Vector { descriptor, .. } = active else {
            return Err(VectorGenerationValidationError::NotVectorHandle);
        };
        match descriptor.metric() {
            VectorDistanceMetric::Cosine => Self::try_from_active::<
                crate::search::vector::distance::Cosine,
            >(active, physical_index_id),
            VectorDistanceMetric::Euclidean => Self::try_from_active::<
                crate::search::vector::distance::Euclidean,
            >(active, physical_index_id),
            VectorDistanceMetric::Manhattan => Self::try_from_active::<
                crate::search::vector::distance::Manhattan,
            >(active, physical_index_id),
        }
    }

    /// Projects a generation capability from canonical active state.
    ///
    /// Unpartitioned records must supply their exact embedded physical ID.
    /// Partitioned callers supply the ID returned by the checked V2 mapping
    /// repository for the requested tenant partition.
    pub(crate) fn try_from_active<D: Distance>(
        active: &ActiveIndexHandle,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<Self, VectorGenerationValidationError> {
        let ActiveIndexHandle::Vector {
            scope,
            identity: _,
            index_id,
            generation,
            record_revision,
            definition,
            layout,
            descriptor,
        } = active
        else {
            return Err(VectorGenerationValidationError::NotVectorHandle);
        };
        Self::try_from_parts::<D>(
            *scope,
            *index_id,
            *generation,
            *record_revision,
            *layout,
            *descriptor,
            definition,
            physical_index_id,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the capability binds every independent durable generation identity axis"
    )]
    fn try_from_parts<D: Distance>(
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        record_revision: IndexRevision,
        layout: VectorPhysicalLayout,
        descriptor: crate::index_lifecycle::VectorGenerationDescriptor,
        definition: &ValidatedVectorIndexDefinition,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<Self, VectorGenerationValidationError> {
        if let VectorPhysicalLayout::Unpartitioned {
            physical_index_id: authorized,
        } = layout
            && authorized != physical_index_id
        {
            return Err(VectorGenerationValidationError::PhysicalIndexMismatch);
        }
        if !descriptor.matches_definition(definition) {
            return Err(VectorGenerationValidationError::DescriptorMismatch);
        }
        let Some(semantics) = ActiveVectorSemantics::for_distance::<D>() else {
            return Err(VectorGenerationValidationError::UnboundDistance(D::name()));
        };
        let metric = match semantics.metric() {
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Cosine => {
                VectorDistanceMetric::Cosine
            }
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Euclidean => {
                VectorDistanceMetric::Euclidean
            }
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Manhattan => {
                VectorDistanceMetric::Manhattan
            }
        };
        let codec = match semantics.codec() {
            crate::encoding::v2::values::indexes::vector::ActiveVectorCodec::F32V1 => {
                crate::index_lifecycle::ActiveVectorCodecV2::F32V1
            }
        };
        let score_semantic = match semantics.score() {
            crate::encoding::v2::values::indexes::vector::ActiveScoreSemantic::CosineHalfF32V1 => {
                crate::index_lifecycle::VectorScoreSemanticV2::CosineHalfF32V1
            }
            crate::encoding::v2::values::indexes::vector::ActiveScoreSemantic::SquaredEuclideanF32V1 => {
                crate::index_lifecycle::VectorScoreSemanticV2::SquaredEuclideanF32V1
            }
            crate::encoding::v2::values::indexes::vector::ActiveScoreSemantic::ManhattanF32V1 => {
                crate::index_lifecycle::VectorScoreSemanticV2::ManhattanF32V1
            }
        };
        let cosine_norm_policy = match semantics.cosine_norm() {
            Some(
                crate::encoding::v2::values::indexes::vector::CosineNormPolicyId::RejectZeroScaledL2V1,
            ) => crate::index_lifecycle::CosineNormPolicyV2::RejectZeroScaledL2V1,
            None => crate::index_lifecycle::CosineNormPolicyV2::NotApplicable,
        };
        if metric != descriptor.metric()
            || codec != descriptor.codec()
            || score_semantic != descriptor.score_semantic()
            || cosine_norm_policy != descriptor.cosine_norm_policy()
        {
            return Err(VectorGenerationValidationError::MetricMismatch);
        }
        let physical_name = format!(
            "v2-vector-{}-{}-{}",
            index_id.get(),
            generation.get(),
            physical_index_id.get()
        );
        Ok(Self {
            identity: VectorGenerationIdentity {
                scope,
                index_id,
                generation,
                physical_index_id,
                record_revision,
                physical_name,
                entity_kind: definition.element_kind(),
                dimension: VectorDimension::try_new(descriptor.dimension() as usize)?,
            },
            metric,
            definition: definition.clone(),
            routing_layout: descriptor.routing_layout(),
        })
    }

    /// Constructs a current f32 capability from canonical generation state.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn create_current<D: Distance>(
        identity: VectorGenerationIdentity,
    ) -> Result<Self, VectorGenerationValidationError> {
        let Some(semantics) = ActiveVectorSemantics::for_distance::<D>() else {
            return Err(VectorGenerationValidationError::UnboundDistance(D::name()));
        };
        let metric = match semantics.metric() {
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Cosine => {
                VectorDistanceMetric::Cosine
            }
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Euclidean => {
                VectorDistanceMetric::Euclidean
            }
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Manhattan => {
                VectorDistanceMetric::Manhattan
            }
        };
        let definition = match identity.entity_kind() {
            IndexElementKind::Node => crate::config::VectorIndexDefinition::new_node(
                "TestVector",
                "embedding",
                identity.dimension().get(),
                metric,
            ),
            IndexElementKind::Edge => crate::config::VectorIndexDefinition::new_edge(
                "TestVector",
                "embedding",
                identity.dimension().get(),
                metric,
            ),
        }
        .expect("test vector identity has a valid dimension");
        let definition = ValidatedVectorIndexDefinition::try_from_runtime(&definition)
            .expect("default test vector definition satisfies V2 validation");
        Ok(Self {
            identity,
            metric,
            definition,
            routing_layout: VectorRoutingLayoutV2::SimHashDirectoryV1,
        })
    }

    /// Rechecks that the capability belongs to distance kernel `D`.
    pub(crate) fn validate_distance<D: Distance>(
        &self,
    ) -> Result<(), VectorGenerationValidationError> {
        let Some(semantics) = ActiveVectorSemantics::for_distance::<D>() else {
            return Err(VectorGenerationValidationError::UnboundDistance(D::name()));
        };
        let metric = match semantics.metric() {
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Cosine => {
                VectorDistanceMetric::Cosine
            }
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Euclidean => {
                VectorDistanceMetric::Euclidean
            }
            crate::encoding::v2::values::indexes::vector::ActiveMetricKind::Manhattan => {
                VectorDistanceMetric::Manhattan
            }
        };
        if metric != self.metric {
            return Err(VectorGenerationValidationError::MetricMismatch);
        }
        Ok(())
    }

    /// Returns the complete identity for cache-key projection.
    pub(crate) const fn identity(&self) -> &VectorGenerationIdentity {
        &self.identity
    }

    /// Returns the exact canonical physical configuration for this generation.
    pub(crate) const fn definition(&self) -> &ValidatedVectorIndexDefinition {
        &self.definition
    }

    /// Returns the descriptor-proven distance metric for closed runtime dispatch.
    pub(crate) const fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    /// Returns whether this complete generation owns the SimHash directory.
    pub(crate) const fn has_simhash_directory(&self) -> bool {
        matches!(
            self.routing_layout,
            VectorRoutingLayoutV2::SimHashDirectoryV1
        )
    }

    /// Returns the storage scope.
    pub(crate) const fn scope(&self) -> DataScope {
        self.identity.scope()
    }

    /// Returns the proven dimension.
    pub(crate) const fn dimension(&self) -> VectorDimension {
        self.identity.dimension()
    }

    /// Returns the complete physical name.
    pub(crate) fn physical_name(&self) -> &str {
        self.identity.physical_name()
    }

    /// Returns the compact physical namespace.
    pub(crate) const fn physical_index_id(&self) -> u64 {
        self.identity.physical_index_id().get()
    }

    /// Returns the deterministic SimHash projection identity.
    pub(crate) fn simhash_identity(&self) -> SimHashIdentity {
        SimHashIdentity::new(
            NonZeroUsize::new(self.dimension().get())
                .expect("validated vector dimensions are non-zero"),
            CURRENT_SIMHASH_SEED,
            NonZeroU16::new(CURRENT_SIMHASH_ALGORITHM_VERSION)
                .expect("current SimHash algorithm version is nonzero"),
        )
    }
}

/// Failure to establish a temporary vector generation capability.
#[derive(Debug, thiserror::Error)]
pub(crate) enum VectorGenerationValidationError {
    /// Test-constructed physical names are never empty.
    #[cfg(any(test, feature = "production-coverage"))]
    #[error("physical vector generation name must not be empty")]
    EmptyPhysicalName,
    /// A non-vector active handle cannot authorize vector rows.
    #[error("active index handle does not describe a vector generation")]
    NotVectorHandle,
    /// A hidden builder capability requires an exact vector `Building` record.
    #[error("canonical index record is not a building vector generation")]
    NotBuildingVectorRecord,
    /// A cleanup capability requires an exact aborting or dropping vector record.
    #[error("canonical index record is not cleaning a vector generation")]
    NotCleaningVectorRecord,
    /// The requested builder does not own the canonical hidden generation.
    #[error("vector build operation does not own the canonical generation")]
    BuildOperationMismatch,
    /// The requested cleaner does not own the canonical generation.
    #[error("vector cleanup operation does not own the canonical generation")]
    CleanupOperationMismatch,
    /// An unpartitioned handle supplied a different physical vector ID.
    #[error("physical vector ID does not match the active generation")]
    PhysicalIndexMismatch,
    /// The selected runtime kernel does not match the capability semantics.
    #[error("vector generation semantics do not match the runtime distance kernel")]
    MetricMismatch,
    /// The canonical definition and generation descriptor disagree.
    #[error("vector generation descriptor does not match its canonical definition")]
    DescriptorMismatch,
    /// A resident cache belongs to another scope or physical index.
    #[error("vector memory store identity does not match the bound row keyspace")]
    CacheIdentityMismatch,
    /// The runtime distance has no stable semantic identity.
    #[error("vector distance '{0}' has no stable durable semantic identity")]
    UnboundDistance(&'static str),
    /// A test identity contained a zero or exhausted V2 identifier.
    #[error(transparent)]
    InvalidIdentity(#[from] crate::index_lifecycle::IndexV2ModelError),
    /// A canonical descriptor dimension could not enter the vector core.
    #[error(transparent)]
    InvalidDimension(#[from] VectorDimensionError),
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/generation.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::VectorIndexDefinition;
    use crate::index_lifecycle::{
        IndexOperationId, IndexRecordV2, IndexStateTransition, PhysicalGeneration,
        ValidatedDynamicIndexDefinition, VectorGenerationDescriptor,
    };
    use crate::search::vector::distance::{Cosine, Euclidean};

    fn building_vector() -> (IndexRecordV2, IndexOperationId, VectorPhysicalIndexId) {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .unwrap(),
        )
        .unwrap();
        let descriptor = match &definition {
            ValidatedDynamicIndexDefinition::Vector(definition) => {
                VectorGenerationDescriptor::for_definition(definition)
            }
            ValidatedDynamicIndexDefinition::Secondary(_)
            | ValidatedDynamicIndexDefinition::Text(_) => unreachable!(),
        };
        let physical_index_id = VectorPhysicalIndexId::new(29).unwrap();
        let operation_id = IndexOperationId::new_v4();
        let record = IndexRecordV2::building(
            IndexId::new(11).unwrap(),
            definition,
            IndexRevision::new(7).unwrap(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::new(5).unwrap(),
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                descriptor,
            },
            operation_id,
        )
        .unwrap();
        (record, operation_id, physical_index_id)
    }

    fn active_vector() -> (ActiveIndexHandle, VectorPhysicalIndexId) {
        let (record, _, physical_index_id) = building_vector();
        let record = record.transition(IndexStateTransition::Activate).unwrap();
        (
            ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record).unwrap(),
            physical_index_id,
        )
    }

    #[test]
    fn active_projection_binds_every_physical_and_cache_identity_component() {
        let (active, physical_index_id) = active_vector();
        let handle =
            ValidatedVectorGenerationHandle::try_from_active::<Cosine>(&active, physical_index_id)
                .unwrap();

        assert_eq!(handle.identity().scope(), DataScope::LegacyUnscoped);
        assert_eq!(handle.identity().index_id(), IndexId::new(11).unwrap());
        assert_eq!(
            handle.identity().generation(),
            IndexGenerationId::new(5).unwrap()
        );
        assert_eq!(handle.physical_index_id(), 29);
        assert_eq!(
            handle.identity().record_revision(),
            active.record_revision()
        );
        assert_eq!(handle.dimension().get(), 3);
        assert!(
            ValidatedVectorGenerationHandle::try_from_active::<Euclidean>(
                &active,
                physical_index_id
            )
            .is_err()
        );
        assert!(matches!(
            ValidatedVectorGenerationHandle::try_from_active::<Cosine>(
                &active,
                VectorPhysicalIndexId::new(30).unwrap()
            ),
            Err(VectorGenerationValidationError::PhysicalIndexMismatch)
        ));
    }

    #[test]
    fn building_projection_requires_exact_operation_and_physical_ownership() {
        let (record, operation_id, physical_index_id) = building_vector();
        let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<Cosine>(
            DataScope::LegacyUnscoped,
            &record,
            operation_id,
            physical_index_id,
        )
        .unwrap();

        assert_eq!(handle.generation().physical_index_id(), 29);
        assert_eq!(
            handle.generation().identity().record_revision(),
            record.revision()
        );
        let _fresh_insert_proof = handle.fresh_insert_proof();

        assert!(matches!(
            ValidatedVectorBuildGenerationHandle::try_from_building::<Cosine>(
                DataScope::LegacyUnscoped,
                &record,
                IndexOperationId::new_v4(),
                physical_index_id,
            ),
            Err(VectorGenerationValidationError::BuildOperationMismatch)
        ));
        assert!(matches!(
            ValidatedVectorBuildGenerationHandle::try_from_building::<Cosine>(
                DataScope::LegacyUnscoped,
                &record,
                operation_id,
                VectorPhysicalIndexId::new(30).unwrap(),
            ),
            Err(VectorGenerationValidationError::PhysicalIndexMismatch)
        ));
        assert!(matches!(
            ValidatedVectorBuildGenerationHandle::try_from_building::<Euclidean>(
                DataScope::LegacyUnscoped,
                &record,
                operation_id,
                physical_index_id,
            ),
            Err(VectorGenerationValidationError::MetricMismatch)
        ));

        let active = record.transition(IndexStateTransition::Activate).unwrap();
        assert!(matches!(
            ValidatedVectorBuildGenerationHandle::try_from_building::<Cosine>(
                DataScope::LegacyUnscoped,
                &active,
                operation_id,
                physical_index_id,
            ),
            Err(VectorGenerationValidationError::NotBuildingVectorRecord)
        ));
    }
}
