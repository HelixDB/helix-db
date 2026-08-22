//! Production contracts for compact V2 metadata and loaded-catalog types.
//!
//! These scenarios invoke the deployed constructors and projections directly.
//! They persist nothing: their purpose is to cover non-zero storage-version
//! validation, all three Active-generation handle variants, family-specific
//! definitions, scope matching, configured-state separation, and duplicate
//! insertion failure.

use crate::config::{SecondaryIndexDefinition, TextIndexDefinition, VectorIndexDefinition};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::error::{HelixDbError, IndexFamily};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
    IndexStateTransition, IndexStorageVersion, LoadedV2ScopeCatalog, PhysicalGeneration,
    ValidatedDynamicIndexDefinition, VectorGenerationDescriptor, VectorPhysicalIndexId,
    VectorPhysicalLayout,
};
use crate::search::vector::VectorDistanceMetric;

/// Returns one Active record for each closed dynamic-index family.
fn active_records() -> [IndexRecordV2; 3] {
    let secondary = ValidatedDynamicIndexDefinition::try_from(
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("secondary definition validates"),
    )
    .expect("secondary definition converts to V2");
    let vector = ValidatedDynamicIndexDefinition::try_from(
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("vector definition validates"),
    )
    .expect("vector definition converts to V2");
    let text = ValidatedDynamicIndexDefinition::try_from(
        TextIndexDefinition::new_node("Document", "body").expect("text definition validates"),
    )
    .expect("text definition converts to V2");
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &vector else {
        panic!("vector fixture retains its validated family");
    };
    let vector_descriptor = VectorGenerationDescriptor::for_definition(vector_definition);

    [
        IndexRecordV2::building(
            IndexId::new(1).expect("index ID is non-zero"),
            secondary,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::new(1).expect("generation is non-zero"),
            },
            IndexOperationId::from_bytes([1; 16]).expect("operation ID is non-nil"),
        )
        .expect("secondary building record validates")
        .transition(IndexStateTransition::Activate)
        .expect("secondary record activates"),
        IndexRecordV2::building(
            IndexId::new(2).expect("index ID is non-zero"),
            vector,
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::new(2).expect("generation is non-zero"),
                layout: VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::new(2)
                        .expect("physical index ID is non-zero"),
                },
                descriptor: vector_descriptor,
            },
            IndexOperationId::from_bytes([2; 16]).expect("operation ID is non-nil"),
        )
        .expect("vector building record validates")
        .transition(IndexStateTransition::Activate)
        .expect("vector record activates"),
        IndexRecordV2::building(
            IndexId::new(3).expect("index ID is non-zero"),
            text,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::new(3).expect("generation is non-zero"),
            },
            IndexOperationId::from_bytes([3; 16]).expect("operation ID is non-nil"),
        )
        .expect("text building record validates")
        .transition(IndexStateTransition::Activate)
        .expect("text record activates"),
    ]
}

/// Runs metadata rejection and every loaded-catalog projection contract.
pub(crate) fn run() {
    assert_eq!(
        IndexStorageVersion::CURRENT.get(),
        crate::index_lifecycle::CURRENT_INDEX_STORAGE_VERSION
    );
    assert!(IndexStorageVersion::new(0).is_err());
    assert_eq!(
        IndexStorageVersion::new(IndexStorageVersion::CURRENT.get() + 1)
            .expect("non-zero future version remains representable")
            .get(),
        IndexStorageVersion::CURRENT.get() + 1
    );

    let scope = DataScope::Tenant(TenantId::from_u128(u128::from_be_bytes([0xFE; 16])));
    let records = active_records();
    let handles = records
        .iter()
        .map(|record| {
            ActiveIndexHandle::try_from_record(scope, record)
                .expect("Active record projects one exact handle")
        })
        .collect::<Vec<_>>();
    for (record, handle) in records.iter().zip(&handles) {
        assert_eq!(handle.scope(), scope);
        assert_eq!(handle.index_id(), record.index_id());
        assert_eq!(handle.generation(), record.state().generation());
        assert_eq!(handle.record_revision(), record.revision());
        assert_eq!(handle.identity(), record.identity());
        assert!(handle.matches_record(scope, record));
        assert!(!handle.matches_record(DataScope::LegacyUnscoped, record));
    }
    assert_eq!(handles[0].family(), IndexFamily::Secondary);
    assert!(handles[0].secondary_definition().is_some());
    assert!(handles[0].text_definition().is_none());
    assert_eq!(handles[1].family(), IndexFamily::Vector);
    assert!(handles[1].secondary_definition().is_none());
    assert!(handles[1].text_definition().is_none());
    assert_eq!(handles[2].family(), IndexFamily::Text);
    assert!(handles[2].secondary_definition().is_none());
    assert!(handles[2].text_definition().is_some());

    let mut catalog = LoadedV2ScopeCatalog::new(scope);
    assert_eq!(catalog.scope(), scope);
    assert_eq!(catalog.active_handles().count(), 0);
    for record in &records {
        catalog
            .insert_active(record)
            .expect("distinct Active identity inserts");
    }
    assert_eq!(catalog.active_handles().count(), 3);
    let secondary_key = crate::config::scoped_secondary_index_property("User", "email");
    assert!(catalog
        .runtime()
        .contains_node_equality_scoped(&secondary_key));
    assert!(matches!(
        catalog
            .insert_active(&records[0])
            .expect_err("duplicate Active identity fails closed"),
        HelixDbError::IndexCatalogCorruption(_)
    ));
}
