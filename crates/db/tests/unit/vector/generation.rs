//! Production contracts for descriptor-bound vector generation capabilities.
//!
//! This child module exercises the real capability constructors
//! through canonical V2 lifecycle records. It proves physical, operation,
//! metric, layout, and descriptor mismatches fail closed without introducing a
//! test-only row format or enabling any reserved vector codec.

use std::num::NonZeroU64;

use bytemuck::{Pod, Zeroable};

use super::*;
use crate::config::{SecondaryIndexDefinition, VectorIndexDefinition};
use crate::index_lifecycle::{IndexStateTransition, VectorGenerationDescriptor};
use crate::search::vector::distance::{Cosine, Euclidean, Manhattan};
use crate::search::vector::unaligned_vector::UnalignedVector;

/// Unsupported process-local kernel used to prove durable binding fails closed.
#[derive(Debug, Clone)]
enum UnsupportedDistance {}

/// Trivial header required by the unsupported process-local kernel.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
struct UnsupportedHeader(f32);

impl Distance for UnsupportedDistance {
    type Header = UnsupportedHeader;
    type VectorCodec = f32;

    fn name() -> &'static str {
        "unsupported"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {
        UnsupportedHeader(0.0)
    }

    fn distance(
        _left: &crate::search::vector::Item<Self>,
        _right: &crate::search::vector::Item<Self>,
    ) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

impl crate::search::vector::distance::sealed::Sealed for UnsupportedDistance {}

/// Constructs one canonical vector building record and its physical authority.
fn building_vector(
    metric: VectorDistanceMetric,
    element_kind: IndexElementKind,
    partitioned: bool,
) -> (IndexRecordV2, IndexOperationId, VectorPhysicalIndexId) {
    let definition = match element_kind {
        IndexElementKind::Node => {
            VectorIndexDefinition::new_node("Document", "embedding", 3, metric)
        }
        IndexElementKind::Edge => VectorIndexDefinition::new_edge("Rel", "embedding", 3, metric),
    }
    .expect("generation definition validates");
    let definition = if partitioned {
        definition
            .with_tenant_property("tenant")
            .expect("partitioned generation definition validates")
    } else {
        definition
    };
    let definition = ValidatedDynamicIndexDefinition::try_from(definition)
        .expect("generation definition converts to V2");
    let ValidatedDynamicIndexDefinition::Vector(vector) = &definition else {
        panic!("generation fixture constructs a vector definition");
    };
    let physical_index_id =
        VectorPhysicalIndexId::new(29).expect("generation physical ID is non-zero");
    let layout = if partitioned {
        VectorPhysicalLayout::Partitioned
    } else {
        VectorPhysicalLayout::Unpartitioned { physical_index_id }
    };
    let descriptor = VectorGenerationDescriptor::for_definition(vector);
    let operation_id = IndexOperationId::new_v4();
    let record = IndexRecordV2::building(
        IndexId::new(11).expect("generation index ID is non-zero"),
        definition,
        IndexRevision::new(7).expect("generation revision is valid"),
        PhysicalGeneration::Vector {
            generation: IndexGenerationId::new(5).expect("generation ID is non-zero"),
            layout,
            descriptor,
        },
        operation_id,
    )
    .expect("generation building record validates");
    (record, operation_id, physical_index_id)
}

/// Constructs one canonical non-vector building record and its operation.
fn building_secondary() -> (IndexRecordV2, IndexOperationId) {
    let definition = SecondaryIndexDefinition::node_equality("User", "email")
        .expect("secondary generation definition validates")
        .try_into()
        .expect("secondary generation definition converts to V2");
    let operation_id = IndexOperationId::new_v4();
    let record = IndexRecordV2::building(
        IndexId::new(41).expect("secondary generation index ID is non-zero"),
        definition,
        IndexRevision::initial(),
        PhysicalGeneration::Secondary {
            generation: IndexGenerationId::initial(),
        },
        operation_id,
    )
    .expect("secondary generation building record validates");
    (record, operation_id)
}

/// Constructs one canonical non-vector Active handle.
fn active_secondary() -> ActiveIndexHandle {
    let (record, _) = building_secondary();
    let record = record
        .transition(IndexStateTransition::Activate)
        .expect("secondary generation record activates");
    ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
        .expect("secondary generation Active handle validates")
}

/// Constructs an explicit test identity with every cache axis visible.
fn identity(element_kind: IndexElementKind, physical_name: &str) -> VectorGenerationIdentity {
    VectorGenerationIdentity::try_new(
        DataScope::LegacyUnscoped,
        11,
        physical_name.to_owned(),
        29,
        NonZeroU64::new(5).expect("generation ID is non-zero"),
        7,
        element_kind,
        VectorDimension::try_new(3).expect("generation dimension is non-zero"),
    )
    .expect("generation identity validates")
}

/// Exercises every constructible generation authority and fail-closed branch.
#[test]
fn generation_handles_retain_descriptor_bound_authority() {
    assert!(matches!(
        VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            11,
            String::new(),
            29,
            NonZeroU64::new(5).unwrap(),
            7,
            IndexElementKind::Node,
            VectorDimension::try_new(3).unwrap(),
        ),
        Err(VectorGenerationValidationError::EmptyPhysicalName)
    ));
    for (index_id, physical_index_id, record_revision) in [(0, 29, 7), (11, 0, 7), (11, 29, 0)] {
        assert!(matches!(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                index_id,
                "invalid-identity".to_owned(),
                physical_index_id,
                NonZeroU64::new(5).unwrap(),
                record_revision,
                IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            ),
            Err(VectorGenerationValidationError::InvalidIdentity(_))
        ));
    }

    let cosine = ValidatedVectorGenerationHandle::create_current::<Cosine>(identity(
        IndexElementKind::Node,
        "cosine-node",
    ))
    .expect("cosine generation capability validates");
    assert_eq!(cosine.scope(), DataScope::LegacyUnscoped);
    assert_eq!(cosine.physical_name(), "cosine-node");
    assert_eq!(cosine.physical_index_id(), 29);
    assert_eq!(cosine.dimension().get(), 3);
    assert_eq!(cosine.identity().index_id(), IndexId::new(11).unwrap());
    assert_eq!(
        cosine.identity().generation(),
        IndexGenerationId::new(5).unwrap()
    );
    assert_eq!(
        cosine.identity().record_revision(),
        IndexRevision::new(7).unwrap()
    );
    assert_eq!(cosine.definition().element_kind(), IndexElementKind::Node);
    assert_eq!(cosine.simhash_identity().dimension().get(), 3);
    assert!(cosine.has_simhash_directory());
    cosine
        .validate_distance::<Cosine>()
        .expect("cosine capability retains cosine semantics");
    assert!(matches!(
        cosine.validate_distance::<Euclidean>(),
        Err(VectorGenerationValidationError::MetricMismatch)
    ));
    assert!(matches!(
        cosine.validate_distance::<UnsupportedDistance>(),
        Err(VectorGenerationValidationError::UnboundDistance(
            "unsupported"
        ))
    ));

    let euclidean = ValidatedVectorGenerationHandle::create_current::<Euclidean>(identity(
        IndexElementKind::Node,
        "euclidean-node",
    ))
    .expect("Euclidean generation capability validates");
    euclidean
        .validate_distance::<Euclidean>()
        .expect("Euclidean capability retains Euclidean semantics");
    let manhattan = ValidatedVectorGenerationHandle::create_current::<Manhattan>(identity(
        IndexElementKind::Edge,
        "manhattan-edge",
    ))
    .expect("Manhattan generation capability validates");
    assert_eq!(
        manhattan.definition().element_kind(),
        IndexElementKind::Edge
    );
    manhattan
        .validate_distance::<Manhattan>()
        .expect("Manhattan capability retains Manhattan semantics");
    assert!(matches!(
        ValidatedVectorGenerationHandle::create_current::<UnsupportedDistance>(identity(
            IndexElementKind::Node,
            "unsupported",
        )),
        Err(VectorGenerationValidationError::UnboundDistance(
            "unsupported"
        ))
    ));

    for metric in [
        VectorDistanceMetric::Cosine,
        VectorDistanceMetric::Euclidean,
        VectorDistanceMetric::Manhattan,
    ] {
        let (record, _, physical_index_id) = building_vector(metric, IndexElementKind::Node, false);
        let active_record = record
            .transition(IndexStateTransition::Activate)
            .expect("generation record activates");
        let active = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active_record)
            .expect("vector Active handle validates");
        let handle =
            ValidatedVectorGenerationHandle::try_from_active_current(&active, physical_index_id)
                .expect("current metric dispatch validates");
        assert_eq!(handle.physical_index_id(), 29);
        assert_eq!(handle.physical_name(), "v2-vector-11-5-29");
        assert!(handle.has_simhash_directory());
    }

    let non_vector = active_secondary();
    assert!(matches!(
        ValidatedVectorGenerationHandle::try_from_active_current(
            &non_vector,
            VectorPhysicalIndexId::initial(),
        ),
        Err(VectorGenerationValidationError::NotVectorHandle)
    ));
    assert!(matches!(
        ValidatedVectorGenerationHandle::try_from_active::<Cosine>(
            &non_vector,
            VectorPhysicalIndexId::initial(),
        ),
        Err(VectorGenerationValidationError::NotVectorHandle)
    ));

    let (record, operation_id, physical_index_id) =
        building_vector(VectorDistanceMetric::Cosine, IndexElementKind::Node, false);
    let build = ValidatedVectorBuildGenerationHandle::try_from_building::<Cosine>(
        DataScope::LegacyUnscoped,
        &record,
        operation_id,
        physical_index_id,
    )
    .expect("matching build authority validates");
    assert_eq!(build.generation().physical_index_id(), 29);
    let _fresh_insert = build.fresh_insert_proof();
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
    let active_record = record
        .transition(IndexStateTransition::Activate)
        .expect("generation record activates");
    let active = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active_record)
        .expect("cosine Active handle validates");
    assert!(matches!(
        ValidatedVectorGenerationHandle::try_from_active::<Euclidean>(&active, physical_index_id,),
        Err(VectorGenerationValidationError::MetricMismatch)
    ));
    assert!(matches!(
        ValidatedVectorGenerationHandle::try_from_active::<UnsupportedDistance>(
            &active,
            physical_index_id,
        ),
        Err(VectorGenerationValidationError::UnboundDistance(
            "unsupported"
        ))
    ));
    assert!(matches!(
        ValidatedVectorBuildGenerationHandle::try_from_building::<Cosine>(
            DataScope::LegacyUnscoped,
            &active_record,
            operation_id,
            physical_index_id,
        ),
        Err(VectorGenerationValidationError::NotBuildingVectorRecord)
    ));

    let aborting = record
        .transition(IndexStateTransition::BeginAbort)
        .expect("generation record begins abort");
    let abort = ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
        DataScope::LegacyUnscoped,
        &aborting,
        operation_id,
    )
    .expect("matching abort authority validates");
    assert!(matches!(
        ValidatedVectorCleanupAuthority::try_from_cleaning::<Euclidean>(
            DataScope::LegacyUnscoped,
            &aborting,
            operation_id,
        ),
        Err(VectorGenerationValidationError::MetricMismatch)
    ));
    assert_eq!(abort.scope(), DataScope::LegacyUnscoped);
    assert_eq!(abort.index_id(), IndexId::new(11).unwrap());
    assert_eq!(abort.generation(), IndexGenerationId::new(5).unwrap());
    assert_eq!(
        abort.layout(),
        VectorPhysicalLayout::Unpartitioned { physical_index_id }
    );
    assert_eq!(
        abort
            .physical_generation::<Cosine>(physical_index_id)
            .expect("abort physical generation validates")
            .physical_index_id(),
        29
    );
    assert!(matches!(
        ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
            DataScope::LegacyUnscoped,
            &aborting,
            IndexOperationId::new_v4(),
        ),
        Err(VectorGenerationValidationError::CleanupOperationMismatch)
    ));
    assert!(matches!(
        ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
            DataScope::LegacyUnscoped,
            &record,
            operation_id,
        ),
        Err(VectorGenerationValidationError::NotCleaningVectorRecord)
    ));

    let (secondary, secondary_operation_id) = building_secondary();
    let secondary = secondary
        .transition(IndexStateTransition::BeginAbort)
        .expect("secondary generation begins abort");
    assert!(matches!(
        ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
            DataScope::LegacyUnscoped,
            &secondary,
            secondary_operation_id,
        ),
        Err(VectorGenerationValidationError::NotCleaningVectorRecord)
    ));
    assert!(matches!(
        ValidatedVectorCleanupAuthority::try_from_cleaning::<Euclidean>(
            DataScope::LegacyUnscoped,
            &secondary,
            secondary_operation_id,
        ),
        Err(VectorGenerationValidationError::NotCleaningVectorRecord)
    ));
    assert!(matches!(
        ValidatedVectorCleanupAuthority::try_from_cleaning::<Manhattan>(
            DataScope::LegacyUnscoped,
            &secondary,
            secondary_operation_id,
        ),
        Err(VectorGenerationValidationError::NotCleaningVectorRecord)
    ));

    let drop_operation_id = IndexOperationId::new_v4();
    let dropping = active_record
        .transition(IndexStateTransition::BeginDrop { drop_operation_id })
        .expect("generation record begins drop");
    ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
        DataScope::LegacyUnscoped,
        &dropping,
        drop_operation_id,
    )
    .expect("matching drop authority validates");

    let (partitioned, partition_operation_id, _) =
        building_vector(VectorDistanceMetric::Cosine, IndexElementKind::Node, true);
    let partitioned = partitioned
        .transition(IndexStateTransition::BeginAbort)
        .expect("partitioned generation begins abort");
    let cleanup = ValidatedVectorCleanupAuthority::try_from_cleaning::<Cosine>(
        DataScope::LegacyUnscoped,
        &partitioned,
        partition_operation_id,
    )
    .expect("partitioned cleanup authority validates");
    let partition_physical = VectorPhysicalIndexId::new(77).unwrap();
    assert_eq!(
        cleanup
            .physical_generation::<Cosine>(partition_physical)
            .expect("mapped partition generation validates")
            .physical_index_id(),
        77
    );

    let (record, _, physical_index_id) =
        building_vector(VectorDistanceMetric::Cosine, IndexElementKind::Node, false);
    let active_record = record.transition(IndexStateTransition::Activate).unwrap();
    let mut forged =
        ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active_record).unwrap();
    let alternate =
        VectorIndexDefinition::new_node("Document", "embedding", 4, VectorDistanceMetric::Cosine)
            .unwrap();
    let alternate = ValidatedVectorIndexDefinition::try_from_runtime(&alternate).unwrap();
    let ActiveIndexHandle::Vector { descriptor, .. } = &mut forged else {
        panic!("generation fixture constructs a vector handle");
    };
    *descriptor = VectorGenerationDescriptor::for_definition(&alternate);
    assert!(matches!(
        ValidatedVectorGenerationHandle::try_from_active::<Cosine>(&forged, physical_index_id),
        Err(VectorGenerationValidationError::DescriptorMismatch)
    ));
}
