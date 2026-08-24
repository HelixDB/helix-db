//! Descriptor-bound background hydration for V2 vector caches.
//!
//! The runtime supplies canonical [`ActiveIndexHandle`] values and one stable
//! SlateDB snapshot. This module enumerates only physical namespaces owned by
//! those Active generations, validates every tenant-partition mapping through
//! the canonical key/value codecs, divides the configured budget deterministically,
//! and publishes completed stores through [`VectorCacheRegistry`]. Partial
//! budget-limited stores are safe because managed reads fall back to the same
//! snapshot for every absent row. Corrupt or cancelled loads never publish.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::watch;

use super::memory_registry::{VectorCacheHydration, VectorCacheRegistry};
use super::memory_store::{
    VectorMemoryAdmissionBudget, VectorMemoryStore, VectorMemoryStoreLoadCompletion,
};
use super::ValidatedVectorGenerationHandle;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
#[cfg(test)]
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::keys::{RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_partition_mapping;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{ActiveIndexHandle, VectorPhysicalLayout};

/// Runtime share assigned to one scope after the configured global budget is split.
///
/// A scope may legitimately receive zero bytes when the positive global budget
/// is smaller than the loaded-scope count, while `Unbounded` remains reachable
/// only from the test-only configuration state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorCacheHydrationBudget {
    /// Exact maximum resident row bytes for this scope.
    Bounded(u64),
    /// Test-only unbounded admission inherited from configuration.
    Unbounded,
}

impl VectorCacheHydrationBudget {
    /// Converts the optional byte representation used by validated configuration.
    pub(crate) const fn from_optional_bytes(bytes: Option<u64>) -> Self {
        match bytes {
            Some(bytes) => Self::Bounded(bytes),
            None => Self::Unbounded,
        }
    }

    /// Returns the bounded byte ceiling, when one exists.
    const fn bytes(self) -> Option<u64> {
        match self {
            Self::Bounded(bytes) => Some(bytes),
            Self::Unbounded => None,
        }
    }
}

/// Hydrates every concrete physical namespace owned by the supplied Active records.
///
/// Partition mappings are enumerated from one stable inventory snapshot. Each
/// cache reservation is acquired before its own fresh data snapshot so a graph
/// commit either evicts the published store or changes the reservation's commit
/// generation and forces the unpublished store to be discarded.
pub(crate) async fn hydrate_active_generations(
    db: &slatedb::Db,
    active: Vec<ActiveIndexHandle>,
    registry: &VectorCacheRegistry,
    budget: VectorCacheHydrationBudget,
    mut shutdown: Option<&mut watch::Receiver<bool>>,
) -> Result<()> {
    let inventory = db.snapshot().await?;
    let mut targets = Vec::new();
    let mut physical_ids = HashSet::new();
    for active in active {
        let ActiveIndexHandle::Vector {
            scope,
            index_id,
            generation,
            layout,
            ..
        } = &active
        else {
            continue;
        };
        match layout {
            VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                if !physical_ids.insert((*scope, physical_index_id.get())) {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "two Active vector generations in one scope own the same physical index ID"
                            .to_string(),
                    ));
                }
                targets.push(
                    ValidatedVectorGenerationHandle::try_from_active_current(
                        &active,
                        *physical_index_id,
                    )
                    .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?,
                );
            }
            VectorPhysicalLayout::Partitioned => {
                let prefix = IndexKey::data_prefix(
                    *scope,
                    ScopedKey::generation_prefix(
                        RecordKind::VectorPartitionMapping,
                        *index_id,
                        *generation,
                    ),
                );
                let mut mappings = inventory.scan_prefix(prefix, ..).await?;
                while let Some(row) = mappings.next().await? {
                    let IndexKey::Data {
                        kind: ScopedKey::VectorPartitionMapping(mapping_key),
                        ..
                    } = IndexKey::parse_from_slice(*scope, &row.key)?
                    else {
                        return Err(HelixDbError::IndexCatalogCorruption(
                            "vector partition prefix yielded another key kind".to_string(),
                        ));
                    };
                    let mapping = decode_partition_mapping(&row.value)?;
                    if mapping_key.index_id != *index_id
                        || mapping_key.generation != *generation
                        || mapping.index_id != *index_id
                        || mapping.generation != *generation
                        || mapping_key.partition != mapping.partition.fingerprint()
                    {
                        return Err(HelixDbError::IndexCatalogCorruption(
                            "vector partition mapping key and value disagree".to_string(),
                        ));
                    }
                    if !physical_ids.insert((*scope, mapping.physical_index_id.get())) {
                        return Err(HelixDbError::IndexCatalogCorruption(
                            "two Active vector partitions in one scope own the same physical index ID"
                                .to_string(),
                        ));
                    }
                    targets.push(
                        ValidatedVectorGenerationHandle::try_from_active_current(
                            &active,
                            mapping.physical_index_id,
                        )
                        .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?,
                    );
                }
            }
        }
    }

    targets.sort_unstable_by_key(|handle| {
        let identity = handle.identity();
        (
            identity.scope(),
            identity.index_id().get(),
            identity.generation().get(),
            identity.physical_index_id().get(),
            identity.record_revision().get(),
        )
    });
    let Ok(target_count) = u64::try_from(targets.len()) else {
        return Err(HelixDbError::InvariantViolation(
            "vector cache hydration target count exceeds u64".to_string(),
        ));
    };
    if target_count == 0 {
        return Ok(());
    }

    let mut admitted_bytes = 0u64;
    for (ordinal, handle) in targets.into_iter().enumerate() {
        if shutdown.as_ref().is_some_and(|receiver| *receiver.borrow()) {
            break;
        }
        let Ok(ordinal) = u64::try_from(ordinal) else {
            return Err(HelixDbError::InvariantViolation(
                "vector cache hydration ordinal exceeds u64".to_string(),
            ));
        };
        let admission = match budget.bytes() {
            Some(bytes) => {
                let equal_share = bytes / target_count;
                let remainder = bytes % target_count;
                VectorMemoryAdmissionBudget::Bounded(equal_share + u64::from(ordinal < remainder))
            }
            None => VectorMemoryAdmissionBudget::Unbounded,
        };
        let hydration = registry.prepare_hydration(&handle);
        match &hydration {
            VectorCacheHydration::Unavailable(lifecycle) => {
                tracing::debug!(
                    ?lifecycle,
                    physical_index_id = handle.physical_index_id(),
                    "skipping unavailable vector cache hydration"
                );
                continue;
            }
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Refresh(_) => {}
        }
        if admission == VectorMemoryAdmissionBudget::Bounded(0)
            && matches!(&hydration, VectorCacheHydration::Initial(_))
        {
            drop(hydration);
            registry.forget_validated_closed(&handle);
            continue;
        }
        let snapshot = db.snapshot().await?;
        let store = Arc::new(VectorMemoryStore::new(
            handle.scope(),
            handle.physical_index_id(),
            snapshot.seq(),
        ));
        let loaded = store
            .load_descriptor_bound_with_budget(
                snapshot.as_ref(),
                admission,
                shutdown.as_deref_mut(),
            )
            .await;
        let summary = match loaded {
            Ok(summary) => summary,
            Err(error) => {
                drop(hydration);
                registry.forget_validated_closed(&handle);
                return Err(error);
            }
        };
        if summary.completion == VectorMemoryStoreLoadCompletion::Shutdown {
            drop(hydration);
            registry.forget_validated_closed(&handle);
            break;
        }
        if summary.loaded_entries == 0 && matches!(&hydration, VectorCacheHydration::Initial(_)) {
            drop(hydration);
            registry.forget_validated_closed(&handle);
            continue;
        }
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(summary.estimated_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "vector cache admitted byte count overflowed u64".to_string(),
            ));
        };
        admitted_bytes = next_admitted_bytes;
        if budget
            .bytes()
            .is_some_and(|configured| admitted_bytes > configured)
        {
            return Err(HelixDbError::InvariantViolation(
                "vector cache hydration exceeded its configured budget".to_string(),
            ));
        }
        match hydration {
            VectorCacheHydration::Initial(initial) => {
                initial.finish(store).await;
            }
            VectorCacheHydration::Refresh(refresh) => {
                refresh.finish(store).await;
            }
            VectorCacheHydration::Unavailable(_) => {
                return Err(HelixDbError::InvariantViolation(
                    "unavailable vector hydration reached storage completion".to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/hydration.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::VectorIndexDefinition;
    use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorUpperVectorKey};
    use crate::encoding::v2::keys::scope::{DataScope, TenantId};
    use crate::encoding::v2::keys::VectorPartitionMappingKey;
    use crate::encoding::v2::values::encode_partition_mapping;
    use crate::index_lifecycle::work::{VectorPartitionMappingValue, VectorTenantPartition};
    use crate::index_lifecycle::{
        IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
        IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
        VectorGenerationDescriptor, VectorPhysicalIndexId,
    };
    use crate::search::vector::VectorDistanceMetric;

    async fn raw_db(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap()
    }

    fn active_vector(
        scope: crate::encoding::v2::keys::scope::DataScope,
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
        .unwrap();
        if partitioned {
            definition = definition.with_tenant_property("tenant").unwrap();
        }
        let definition = ValidatedDynamicIndexDefinition::try_from(definition).unwrap();
        let ValidatedDynamicIndexDefinition::Vector(vector) = &definition else {
            unreachable!("the fixture constructs a vector definition")
        };
        let physical_index_id = VectorPhysicalIndexId::new(physical_index_id).unwrap();
        let layout = if partitioned {
            VectorPhysicalLayout::Partitioned
        } else {
            VectorPhysicalLayout::Unpartitioned { physical_index_id }
        };
        let descriptor = VectorGenerationDescriptor::for_definition(vector);
        let record = IndexRecordV2::building(
            IndexId::new(index_id).unwrap(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::initial(),
                layout,
                descriptor,
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let active = ActiveIndexHandle::try_from_record(scope, &record).unwrap();
        let handle =
            ValidatedVectorGenerationHandle::try_from_active_current(&active, physical_index_id)
                .unwrap();
        (active, handle)
    }

    #[tokio::test]
    async fn active_hydration_publishes_exact_snapshot_and_refreshes_immutably() {
        let db = raw_db("vector-cache-active-hydration").await;
        let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
        let physical_index_id = 71;
        let (active, handle) = active_vector(scope, 7, physical_index_id, false);
        let first_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                physical_index_id,
                1,
            ))),
        }
        .to_bytes();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(first_key, Bytes::from_static(b"first"))
            .unwrap();
        transaction.commit().await.unwrap();

        let registry = VectorCacheRegistry::default();
        let first_snapshot = db.snapshot().await.unwrap();
        hydrate_active_generations(
            &db,
            vec![active.clone()],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        let first_guard = registry.read_guard_for(&handle).unwrap();
        assert_eq!(first_guard.store().visible_seq(), first_snapshot.seq());
        assert_eq!(
            first_guard.store().get_upper_vector(1).as_deref(),
            Some(b"first".as_slice())
        );

        let second_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                physical_index_id,
                2,
            ))),
        }
        .to_bytes();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(second_key, Bytes::from_static(b"second"))
            .unwrap();
        transaction.commit().await.unwrap();
        let second_snapshot = db.snapshot().await.unwrap();
        hydrate_active_generations(
            &db,
            vec![active.clone()],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        let second_guard = registry.read_guard_for(&handle).unwrap();
        assert_eq!(second_guard.store().visible_seq(), second_snapshot.seq());
        assert!(second_guard.store().get_upper_vector(2).is_some());
        assert!(first_guard.store().get_upper_vector(2).is_none());

        hydrate_active_generations(
            &db,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Bounded(0),
            None,
        )
        .await
        .unwrap();
        let empty_guard = registry.read_guard_for(&handle).unwrap();
        assert_eq!(empty_guard.store().estimated_bytes(), 0);
        assert!(empty_guard.store().get_upper_vector(1).is_none());
        assert!(empty_guard.store().get_upper_vector(2).is_none());
        assert!(second_guard.store().get_upper_vector(2).is_some());
    }

    #[tokio::test]
    async fn partitioned_hydration_requires_a_cross_checked_v2_mapping() {
        let db = raw_db("vector-cache-partitioned-hydration").await;
        let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
        let index_id = IndexId::new(9).unwrap();
        let physical_index_id = VectorPhysicalIndexId::new(91).unwrap();
        let (active, handle) = active_vector(scope, index_id.get(), physical_index_id.get(), true);
        let partition = VectorTenantPartition::try_new(Bytes::from_static(b"tenant-a")).unwrap();
        let mapping_key = IndexKey::Data {
            scope,
            kind: ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
                index_id,
                generation: IndexGenerationId::initial(),
                partition: partition.fingerprint(),
            }),
        }
        .to_bytes();
        let mapping = encode_partition_mapping(&VectorPartitionMappingValue {
            index_id,
            generation: IndexGenerationId::initial(),
            partition,
            physical_index_id,
        });
        let vector_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                physical_index_id.get(),
                3,
            ))),
        }
        .to_bytes();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction.put(mapping_key, mapping).unwrap();
        transaction
            .put(vector_key, Bytes::from_static(b"partitioned"))
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
        .unwrap();
        let guard = registry.read_guard_for(&handle).unwrap();
        assert_eq!(
            guard.store().get_upper_vector(3).as_deref(),
            Some(b"partitioned".as_slice())
        );
    }

    #[tokio::test]
    async fn hydration_sorts_targets_before_dividing_the_budget() {
        const ENTRY_OVERHEAD_BYTES: u64 = 64;

        let db = raw_db("vector-cache-fair-hydration").await;
        let scope = DataScope::LegacyUnscoped;
        let low_physical_id = 101;
        let high_physical_id = 202;
        let (low_active, low_handle) = active_vector(scope, 10, low_physical_id, false);
        let (high_active, high_handle) = active_vector(scope, 20, high_physical_id, false);
        let low_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                low_physical_id,
                1,
            ))),
        }
        .to_bytes();
        let high_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                high_physical_id,
                1,
            ))),
        }
        .to_bytes();
        let value = Bytes::from_static(b"equal-size");
        assert_eq!(low_key.len(), high_key.len());
        let row_bytes = u64::try_from(low_key.len() + value.len()).unwrap() + ENTRY_OVERHEAD_BYTES;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction.put(low_key, value.clone()).unwrap();
        transaction.put(high_key, value.clone()).unwrap();
        transaction.commit().await.unwrap();

        let registry = VectorCacheRegistry::default();
        hydrate_active_generations(
            &db,
            vec![high_active, low_active],
            &registry,
            VectorCacheHydrationBudget::Bounded(row_bytes * 2 - 1),
            None,
        )
        .await
        .unwrap();

        let low = registry.read_guard_for(&low_handle).unwrap();
        assert_eq!(low.store().estimated_bytes(), row_bytes);
        assert_eq!(low.store().get_upper_vector(1).as_deref(), Some(&value[..]));
        assert!(registry.read_guard_for(&high_handle).is_err());
    }

    #[tokio::test]
    async fn the_same_physical_id_is_valid_in_distinct_scopes() {
        let db = raw_db("vector-cache-scoped-physical-id").await;
        let first_scope = DataScope::Tenant(TenantId::from_u128(1));
        let second_scope = DataScope::Tenant(TenantId::from_u128(2));
        let physical_index_id = 303;
        let (first_active, first_handle) = active_vector(first_scope, 30, physical_index_id, false);
        let (second_active, second_handle) =
            active_vector(second_scope, 30, physical_index_id, false);
        let first_key = DataKey::Data {
            scope: first_scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                physical_index_id,
                1,
            ))),
        }
        .to_bytes();
        let second_key = DataKey::Data {
            scope: second_scope,
            kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                physical_index_id,
                1,
            ))),
        }
        .to_bytes();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(first_key, Bytes::from_static(b"first-scope"))
            .unwrap();
        transaction
            .put(second_key, Bytes::from_static(b"second-scope"))
            .unwrap();
        transaction.commit().await.unwrap();

        let registry = VectorCacheRegistry::default();
        hydrate_active_generations(
            &db,
            vec![second_active, first_active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();

        let first = registry.read_guard_for(&first_handle).unwrap();
        let second = registry.read_guard_for(&second_handle).unwrap();
        assert_eq!(
            first.store().get_upper_vector(1).as_deref(),
            Some(b"first-scope".as_slice())
        );
        assert_eq!(
            second.store().get_upper_vector(1).as_deref(),
            Some(b"second-scope".as_slice())
        );
    }

    #[tokio::test]
    async fn duplicate_physical_ids_in_one_scope_fail_before_publication() {
        let db = raw_db("vector-cache-duplicate-physical-id").await;
        let scope = DataScope::LegacyUnscoped;
        let (first_active, first_handle) = active_vector(scope, 40, 404, false);
        let (second_active, second_handle) = active_vector(scope, 41, 404, false);
        let registry = VectorCacheRegistry::default();

        let error = hydrate_active_generations(
            &db,
            vec![first_active, second_active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(registry.read_guard_for(&first_handle).is_err());
        assert!(registry.read_guard_for(&second_handle).is_err());
    }

    #[tokio::test]
    async fn mismatched_partition_mapping_fails_before_publication() {
        let db = raw_db("vector-cache-mismatched-partition").await;
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::new(50).unwrap();
        let physical_index_id = VectorPhysicalIndexId::new(505).unwrap();
        let (active, handle) = active_vector(scope, index_id.get(), physical_index_id.get(), true);
        let key_partition =
            VectorTenantPartition::try_new(Bytes::from_static(b"tenant-key")).unwrap();
        let value_partition =
            VectorTenantPartition::try_new(Bytes::from_static(b"tenant-value")).unwrap();
        let mapping_key = IndexKey::Data {
            scope,
            kind: ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
                index_id,
                generation: IndexGenerationId::initial(),
                partition: key_partition.fingerprint(),
            }),
        }
        .to_bytes();
        let mapping = encode_partition_mapping(&VectorPartitionMappingValue {
            index_id,
            generation: IndexGenerationId::initial(),
            partition: value_partition,
            physical_index_id,
        });
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction.put(mapping_key, mapping).unwrap();
        transaction.commit().await.unwrap();

        let registry = VectorCacheRegistry::default();
        let error = hydrate_active_generations(
            &db,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(registry.read_guard_for(&handle).is_err());
    }

    #[tokio::test]
    async fn shutdown_before_hydration_publishes_nothing() {
        let db = raw_db("vector-cache-shutdown-hydration").await;
        let scope = DataScope::LegacyUnscoped;
        let (active, handle) = active_vector(scope, 60, 606, false);
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
        .unwrap();

        assert!(registry.read_guard_for(&handle).is_err());
    }
}
