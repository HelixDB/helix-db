//! Production contracts for descriptor-bound vector cache hydration.
//!
//! The feature-gated runner exercises the real Active-generation loader using
//! current V1 vector and Index V2 mapping rows. It deliberately lives outside
//! the measured production tree and changes no persisted representation.

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::config::{SecondaryIndexDefinition, VectorIndexDefinition};
use crate::encoding::v2::keys::indexes::vector::{
    VectorKey, VectorMemoryPrefixKey, VectorUpperVectorKey,
};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::SecondaryEntryLane;
use crate::encoding::v2::keys::VectorPartitionMappingKey;
use crate::encoding::v2::keys::{DataKey as GraphKey, DataKeyKind};
use crate::encoding::v2::values::{encode_partition_mapping, encode_secondary_entry};
use crate::index_lifecycle::work::{
    SecondaryEntryValue, VectorPartitionMappingValue, VectorTenantPartition,
};
use crate::index_lifecycle::{
    IndexEntityId, IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
    IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
    VectorGenerationDescriptor, VectorPhysicalIndexId,
};
use crate::search::vector::VectorDistanceMetric;

/// Opens one isolated database for a hydration contract.
async fn raw_db(name: &str) -> Db {
    Db::builder(name, Arc::new(InMemory::new()))
        .build()
        .await
        .expect("hydration contract database opens")
}

/// Constructs matching Active and physical handles without bypassing V2 validation.
fn active_vector(
    scope: DataScope,
    index_id: u64,
    physical_index_id: u64,
    partitioned: bool,
) -> (ActiveIndexHandle, ValidatedVectorGenerationHandle) {
    let mut definition = VectorIndexDefinition::new_node(
        "Document",
        "embedding",
        3,
        VectorDistanceMetric::Euclidean,
    )
    .expect("vector hydration definition validates");
    if partitioned {
        definition = definition
            .with_tenant_property("tenant")
            .expect("partitioned hydration definition validates");
    }
    let definition = ValidatedDynamicIndexDefinition::try_from(definition)
        .expect("hydration definition converts to V2");
    let ValidatedDynamicIndexDefinition::Vector(vector) = &definition else {
        panic!("hydration fixture constructs a vector definition");
    };
    let physical_index_id =
        VectorPhysicalIndexId::new(physical_index_id).expect("hydration physical ID is non-zero");
    let layout = if partitioned {
        VectorPhysicalLayout::Partitioned
    } else {
        VectorPhysicalLayout::Unpartitioned { physical_index_id }
    };
    let descriptor = VectorGenerationDescriptor::for_definition(vector);
    let record = IndexRecordV2::building(
        IndexId::new(index_id).expect("hydration index ID is non-zero"),
        definition,
        IndexRevision::initial(),
        PhysicalGeneration::Vector {
            generation: IndexGenerationId::initial(),
            layout,
            descriptor,
        },
        IndexOperationId::new_v4(),
    )
    .expect("hydration building record validates")
    .transition(IndexStateTransition::Activate)
    .expect("hydration record activates");
    let active = ActiveIndexHandle::try_from_record(scope, &record)
        .expect("hydration Active handle validates");
    let physical =
        ValidatedVectorGenerationHandle::try_from_active_current(&active, physical_index_id)
            .expect("hydration physical handle matches Active descriptor");
    (active, physical)
}

/// Constructs a non-vector Active handle to prove hydration ignores other families.
fn active_secondary(scope: DataScope) -> ActiveIndexHandle {
    let definition = SecondaryIndexDefinition::node_equality("User", "email")
        .expect("secondary hydration definition validates")
        .try_into()
        .expect("secondary hydration definition converts to V2");
    let record = IndexRecordV2::building(
        IndexId::new(900).expect("secondary hydration ID is non-zero"),
        definition,
        IndexRevision::initial(),
        PhysicalGeneration::Secondary {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::new_v4(),
    )
    .expect("secondary hydration record validates")
    .transition(IndexStateTransition::Activate)
    .expect("secondary hydration record activates");
    ActiveIndexHandle::try_from_record(scope, &record).expect("secondary Active handle validates")
}

/// Replaces the canonical descriptor with another valid metric descriptor.
///
/// Catalog loading normally makes this state impossible. Hydration still
/// rejects it because its input is a retained runtime capability and must fail
/// closed if that capability is corrupted in memory.
fn with_mismatched_descriptor(mut active: ActiveIndexHandle) -> ActiveIndexHandle {
    let definition =
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("mismatched descriptor definition validates");
    let definition = ValidatedDynamicIndexDefinition::try_from(definition)
        .expect("mismatched descriptor definition converts to V2");
    let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
        panic!("mismatched descriptor fixture constructs a vector definition")
    };
    let ActiveIndexHandle::Vector { descriptor, .. } = &mut active else {
        panic!("mismatched descriptor fixture receives a vector handle")
    };
    *descriptor = VectorGenerationDescriptor::for_definition(&definition);
    active
}

/// Builds one deployed upper-vector row key.
fn upper_vector_key(scope: DataScope, physical_index_id: u64, node_id: u64) -> Bytes {
    GraphKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
            physical_index_id,
            node_id,
        ))),
    }
    .to_bytes()
}

/// Persists one exact partition mapping through the current work-value codec.
async fn put_partition_mapping(
    db: &Db,
    scope: DataScope,
    index_id: IndexId,
    key_partition: &VectorTenantPartition,
    value_partition: VectorTenantPartition,
    physical_index_id: VectorPhysicalIndexId,
) {
    let key = IndexKey::Data {
        scope,
        kind: ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
            index_id,
            generation: IndexGenerationId::initial(),
            partition: key_partition.fingerprint(),
        }),
    }
    .to_bytes();
    let value = encode_partition_mapping(&VectorPartitionMappingValue {
        index_id,
        generation: IndexGenerationId::initial(),
        partition: value_partition,
        physical_index_id,
    });
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("partition mapping transaction opens");
    transaction
        .put(key, value)
        .expect("partition mapping stages");
    transaction
        .commit()
        .await
        .expect("partition mapping commits");
}

/// Covers empty inventories, family filtering, and zero-entry publication rules.
async fn run_empty_contracts() {
    assert_eq!(
        VectorCacheHydrationBudget::from_optional_bytes(Some(7)).bytes(),
        Some(7)
    );
    assert_eq!(
        VectorCacheHydrationBudget::from_optional_bytes(None).bytes(),
        None
    );
    let db = raw_db("production-vector-hydration-empty").await;
    let registry = VectorCacheRegistry::default();
    hydrate_active_generations(
        &db,
        vec![active_secondary(DataScope::LegacyUnscoped)],
        &registry,
        VectorCacheHydrationBudget::Bounded(1),
        None,
    )
    .await
    .expect("non-vector Active handles are ignored");

    let (mismatched, _) = active_vector(DataScope::LegacyUnscoped, 16, 161, false);
    assert!(matches!(
        hydrate_active_generations(
            &db,
            vec![with_mismatched_descriptor(mismatched)],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));

    let (active, physical) = active_vector(DataScope::LegacyUnscoped, 1, 11, false);
    hydrate_active_generations(
        &db,
        vec![active.clone()],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .expect("empty unbounded hydration completes");
    assert!(registry.read_guard_for(&physical).is_err());
    hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Bounded(0),
        None,
    )
    .await
    .expect("zero-budget initial hydration completes without publication");
    assert!(registry.read_guard_for(&physical).is_err());

    let (reserved_active, reserved_physical) =
        active_vector(DataScope::LegacyUnscoped, 12, 121, false);
    let reservation = registry.prepare_hydration(&reserved_physical);
    hydrate_active_generations(
        &db,
        vec![reserved_active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .expect("an independently reserved generation is skipped");
    drop(reservation);
    db.close().await.expect("empty hydration database closes");
}

/// Covers initial publication, immutable refresh, and deterministic budget shares.
async fn run_refresh_and_budget_contracts() {
    const ENTRY_OVERHEAD_BYTES: u64 = 64;
    let db = raw_db("production-vector-hydration-refresh").await;
    let scope = DataScope::LegacyUnscoped;
    let (active, physical) = active_vector(scope, 2, 21, false);
    let first_key = upper_vector_key(scope, 21, 1);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&first_key, Bytes::from_static(b"first"))
        .unwrap();
    transaction.commit().await.unwrap();
    let registry = VectorCacheRegistry::default();
    hydrate_active_generations(
        &db,
        vec![active.clone()],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .expect("initial hydration publishes");
    let first = registry
        .read_guard_for(&physical)
        .expect("initial store has a read guard");

    let second_key = upper_vector_key(scope, 21, 2);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(second_key, Bytes::from_static(b"second"))
        .unwrap();
    transaction.commit().await.unwrap();
    hydrate_active_generations(
        &db,
        vec![active.clone()],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .expect("refresh hydration publishes");
    let refreshed = registry
        .read_guard_for(&physical)
        .expect("refreshed store has a read guard");
    assert!(refreshed.store().get_upper_vector(2).is_some());
    assert!(first.store().get_upper_vector(2).is_none());
    hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Bounded(0),
        None,
    )
    .await
    .expect("zero-budget refresh publishes an exact empty snapshot");
    assert_eq!(
        registry
            .read_guard_for(&physical)
            .expect("empty refresh remains a valid store")
            .store()
            .estimated_bytes(),
        0
    );

    let (low_active, low) = active_vector(scope, 3, 31, false);
    let (high_active, high) = active_vector(scope, 4, 41, false);
    let value = Bytes::from_static(b"equal-size");
    let low_key = upper_vector_key(scope, 31, 1);
    let high_key = upper_vector_key(scope, 41, 1);
    assert_eq!(low_key.len(), high_key.len());
    let row_bytes = u64::try_from(low_key.len() + value.len())
        .expect("hydration row length fits u64")
        + ENTRY_OVERHEAD_BYTES;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.put(low_key, value.clone()).unwrap();
    transaction.put(high_key, value).unwrap();
    transaction.commit().await.unwrap();
    hydrate_active_generations(
        &db,
        vec![high_active, low_active],
        &registry,
        VectorCacheHydrationBudget::Bounded(row_bytes * 2 - 1),
        None,
    )
    .await
    .expect("sorted deterministic budget hydration completes");
    assert_eq!(
        registry
            .read_guard_for(&low)
            .expect("lower sorted target receives remainder")
            .store()
            .estimated_bytes(),
        row_bytes
    );
    assert!(registry.read_guard_for(&high).is_err());
    db.close().await.expect("refresh hydration database closes");
}

/// Covers partition mapping validation, scope identity, and duplicate ownership.
async fn run_partition_contracts() {
    let scope = DataScope::LegacyUnscoped;
    let db = raw_db("production-vector-hydration-partition").await;
    let index_id = IndexId::new(5).unwrap();
    let physical_id = VectorPhysicalIndexId::new(51).unwrap();
    let (active, physical) = active_vector(scope, index_id.get(), physical_id.get(), true);
    let partition = VectorTenantPartition::try_new(Bytes::from_static(b"tenant-a")).unwrap();
    put_partition_mapping(
        &db,
        scope,
        index_id,
        &partition,
        partition.clone(),
        physical_id,
    )
    .await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            upper_vector_key(scope, physical_id.get(), 3),
            Bytes::from_static(b"partitioned"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    let registry = VectorCacheRegistry::default();
    hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .expect("valid partition mapping hydrates");
    assert!(registry.read_guard_for(&physical).is_ok());
    db.close()
        .await
        .expect("partition hydration database closes");

    let db = raw_db("production-vector-hydration-partition-descriptor-mismatch").await;
    let index_id = IndexId::new(17).unwrap();
    let physical_id = VectorPhysicalIndexId::new(171).unwrap();
    let (active, _) = active_vector(scope, index_id.get(), physical_id.get(), true);
    let partition = VectorTenantPartition::try_new(Bytes::from_static(b"descriptor")).unwrap();
    put_partition_mapping(
        &db,
        scope,
        index_id,
        &partition,
        partition.clone(),
        physical_id,
    )
    .await;
    assert!(matches!(
        hydrate_active_generations(
            &db,
            vec![with_mismatched_descriptor(active)],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close()
        .await
        .expect("partition descriptor-mismatch database closes");

    let db = raw_db("production-vector-hydration-partition-mismatch").await;
    let index_id = IndexId::new(6).unwrap();
    let physical_id = VectorPhysicalIndexId::new(61).unwrap();
    let (active, physical) = active_vector(scope, index_id.get(), physical_id.get(), true);
    let key_partition = VectorTenantPartition::try_new(Bytes::from_static(b"tenant-key")).unwrap();
    let value_partition =
        VectorTenantPartition::try_new(Bytes::from_static(b"tenant-value")).unwrap();
    put_partition_mapping(
        &db,
        scope,
        index_id,
        &key_partition,
        value_partition,
        physical_id,
    )
    .await;
    assert!(hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .is_err());
    assert!(registry.read_guard_for(&physical).is_err());
    db.close()
        .await
        .expect("mismatch hydration database closes");

    let db = raw_db("production-vector-hydration-wrong-value-kind").await;
    let index_id = IndexId::new(13).unwrap();
    let physical_id = VectorPhysicalIndexId::new(131).unwrap();
    let (active, _) = active_vector(scope, index_id.get(), physical_id.get(), true);
    let partition = VectorTenantPartition::try_new(Bytes::from_static(b"wrong-kind")).unwrap();
    let key = IndexKey::Data {
        scope,
        kind: ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
            index_id,
            generation: IndexGenerationId::initial(),
            partition: partition.fingerprint(),
        }),
    }
    .to_bytes();
    let value = encode_secondary_entry(&SecondaryEntryValue {
        index_id,
        generation: IndexGenerationId::initial(),
        lane: SecondaryEntryLane::NodeEquality,
        entity_id: IndexEntityId::initial(),
    });
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.put(key, value).unwrap();
    transaction.commit().await.unwrap();
    assert!(hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .is_err());
    db.close()
        .await
        .expect("wrong-kind hydration database closes");

    let db = raw_db("production-vector-hydration-partition-duplicate").await;
    let index_id = IndexId::new(14).unwrap();
    let physical_id = VectorPhysicalIndexId::new(141).unwrap();
    let (active, _) = active_vector(scope, index_id.get(), physical_id.get(), true);
    let first_partition = VectorTenantPartition::try_new(Bytes::from_static(b"first")).unwrap();
    let second_partition = VectorTenantPartition::try_new(Bytes::from_static(b"second")).unwrap();
    put_partition_mapping(
        &db,
        scope,
        index_id,
        &first_partition,
        first_partition.clone(),
        physical_id,
    )
    .await;
    put_partition_mapping(
        &db,
        scope,
        index_id,
        &second_partition,
        second_partition.clone(),
        physical_id,
    )
    .await;
    assert!(hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .is_err());
    db.close()
        .await
        .expect("duplicate partition database closes");

    let db = raw_db("production-vector-hydration-duplicate").await;
    let (first_active, first) = active_vector(scope, 7, 71, false);
    let (second_active, second) = active_vector(scope, 8, 71, false);
    let duplicate_registry = VectorCacheRegistry::default();
    assert!(hydrate_active_generations(
        &db,
        vec![first_active, second_active],
        &duplicate_registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .is_err());
    assert!(duplicate_registry.read_guard_for(&first).is_err());
    assert!(duplicate_registry.read_guard_for(&second).is_err());

    let first_scope = DataScope::Tenant(TenantId::from_u128(1));
    let second_scope = DataScope::Tenant(TenantId::from_u128(2));
    let (first_active, first) = active_vector(first_scope, 9, 81, false);
    let (second_active, second) = active_vector(second_scope, 9, 81, false);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            upper_vector_key(first_scope, 81, 1),
            Bytes::from_static(b"first-scope"),
        )
        .unwrap();
    transaction
        .put(
            upper_vector_key(second_scope, 81, 1),
            Bytes::from_static(b"second-scope"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    hydrate_active_generations(
        &db,
        vec![second_active, first_active],
        &duplicate_registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .expect("same physical ID in distinct scopes hydrates");
    assert!(duplicate_registry.read_guard_for(&first).is_ok());
    assert!(duplicate_registry.read_guard_for(&second).is_ok());
    db.close()
        .await
        .expect("duplicate hydration database closes");
}

/// Covers cancellation and malformed physical-row failure without publication.
async fn run_shutdown_and_corruption_contracts() {
    let scope = DataScope::LegacyUnscoped;
    let db = raw_db("production-vector-hydration-shutdown").await;
    let (active, physical) = active_vector(scope, 10, 101, false);
    let registry = VectorCacheRegistry::default();
    let (_shutdown_tx, mut shutdown_rx) = watch::channel(true);
    hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        Some(&mut shutdown_rx),
    )
    .await
    .expect("pre-signalled shutdown cancels hydration");
    assert!(registry.read_guard_for(&physical).is_err());
    db.close()
        .await
        .expect("shutdown hydration database closes");

    let db = raw_db("production-vector-hydration-closed-shutdown").await;
    let (active, physical) = active_vector(scope, 15, 151, false);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            upper_vector_key(scope, 151, 1),
            Bytes::from_static(b"cancelled"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    drop(shutdown_tx);
    hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        Some(&mut shutdown_rx),
    )
    .await
    .expect("closed shutdown channel cancels an active load");
    assert!(registry.read_guard_for(&physical).is_err());
    db.close()
        .await
        .expect("closed-shutdown hydration database closes");

    let db = raw_db("production-vector-hydration-corrupt").await;
    let (active, physical) = active_vector(scope, 11, 111, false);
    let mut malformed = GraphKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(111))),
    }
    .to_bytes()
    .to_vec();
    malformed.push(0xFE);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(malformed, Bytes::from_static(b"malformed"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(hydrate_active_generations(
        &db,
        vec![active],
        &registry,
        VectorCacheHydrationBudget::Unbounded,
        None,
    )
    .await
    .is_err());
    assert!(registry.read_guard_for(&physical).is_err());
    db.close().await.expect("corrupt hydration database closes");
}

/// Exercises every production hydration ownership and admission boundary.
pub(crate) async fn run() {
    run_empty_contracts().await;
    run_refresh_and_budget_contracts().await;
    run_partition_contracts().await;
    run_shutdown_and_corruption_contracts().await;
}
