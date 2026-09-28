//! Descriptor-bound background hydration for V2 vector caches.
//!
//! The runtime supplies canonical [`ActiveIndexHandle`] values for one scope and
//! a [`VectorCacheSnapshotSource`] for the node's storage. This module
//! enumerates only physical namespaces owned by those Active generations,
//! validates every tenant-partition mapping through the canonical key/value
//! codecs, divides the configured budget deterministically, releases reader
//! entries whose generations left the inventory, and publishes stores through
//! [`VectorCacheRegistry`]. Partial budget-limited stores are safe because
//! managed reads fall back to the same snapshot for every absent row. Corrupt
//! or cancelled loads never publish.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::watch;

use super::registry::{
    VectorCacheHydration, VectorCacheIdentity, VectorCacheRegistry, VectorCacheSweep,
};
use super::store::{
    VectorMemoryAdmissionBudget, VectorMemoryStore, VectorMemoryStoreLoadCompletion,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
#[cfg(test)]
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::keys::{RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_partition_mapping;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{ActiveIndexHandle, VectorPhysicalLayout};
use crate::search::vector::ValidatedVectorGenerationHandle;

/// Storage handle whose stable snapshots feed one hydration pass.
///
/// Both variants pin SlateDB snapshots in the sequence domain that request read
/// views report, so a published store's `visible_seq` is directly comparable
/// with a request snapshot on the same node.
#[derive(Clone, Copy)]
pub(crate) enum VectorCacheSnapshotSource<'a> {
    /// Writer node: snapshots include every locally committed write.
    Writer(&'a slatedb::Db),
    /// Reader node: snapshots include the WAL and manifest state its poller applied.
    Reader(&'a slatedb::DbReader),
}

impl VectorCacheSnapshotSource<'_> {
    /// Pins the node's latest visible state.
    async fn snapshot(self) -> Result<Arc<slatedb::DbSnapshot>> {
        Ok(match self {
            Self::Writer(db) => db.snapshot().await?,
            Self::Reader(reader) => reader.snapshot().await?,
        })
    }
}

/// Resolves with the reader's applied sequence once it rises above `current_through`.
///
/// `sequence` projects the applied sequence out of a reader status. Status
/// changes that keep it, such as manifest-only updates, never resolve this,
/// and neither does a closed status channel: a closed reader applies no newer
/// state.
pub(crate) async fn reader_advances_past<T>(
    status: &mut watch::Receiver<T>,
    sequence: impl Fn(&T) -> u64,
    current_through: u64,
) -> u64 {
    let Ok(advanced) = status
        .wait_for(|status| sequence(status) > current_through)
        .await
    else {
        return std::future::pending().await;
    };
    sequence(&advanced)
}

/// Whether a reader outran any load of one hydration pass.
///
/// Ordered so that combining passes keeps the outrun one:
/// `Settled < Outrun`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum VectorCacheHydrationOutcome {
    /// Every target was retained, published, discarded by a crossing commit,
    /// or skipped as unavailable.
    Settled,
    /// A reader node applied newer state while at least one target was
    /// loading, and that target's store was dropped unpublished because it
    /// could never attach.
    Outrun,
}

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

/// Hydrates every concrete physical namespace owned by `scope`'s Active records.
///
/// Partition mappings are enumerated from one stable inventory snapshot. Each
/// cache reservation is acquired before its own fresh data snapshot so a graph
/// commit either evicts the published store or changes the reservation's commit
/// generation and forces the unpublished store to be discarded. On a reader
/// node, which never runs retirement, entries of `scope` outside the validated
/// inventory are released before any load so dropped generations are not
/// retained.
///
/// A reader store attaches only to request snapshots at the exact sequence it
/// was hydrated at, so a load the reader outruns could never serve. Each
/// reader load races the reader's status: once the reader applies state past
/// that target's snapshot, only that load is dropped with its reservation,
/// which keeps a published store or returns an initial entry to `Vacant`,
/// and the pass continues with the next target on a fresh snapshot. The pass
/// then reports [`VectorCacheHydrationOutcome::Outrun`]. Writer loads always
/// run to completion.
pub(crate) async fn hydrate_active_generations(
    source: VectorCacheSnapshotSource<'_>,
    scope: DataScope,
    active: Vec<ActiveIndexHandle>,
    registry: &VectorCacheRegistry,
    budget: VectorCacheHydrationBudget,
    mut shutdown: Option<&mut watch::Receiver<bool>>,
) -> Result<VectorCacheHydrationOutcome> {
    let mut reader_status = match source {
        VectorCacheSnapshotSource::Writer(_) => None,
        VectorCacheSnapshotSource::Reader(reader) => Some(reader.subscribe()),
    };
    let mut outcome = VectorCacheHydrationOutcome::Settled;
    let inventory = source.snapshot().await?;
    let mut targets = Vec::new();
    let mut physical_ids = HashSet::new();
    for active in active {
        let ActiveIndexHandle::Vector {
            scope: active_scope,
            index_id,
            generation,
            layout,
            ..
        } = &active
        else {
            continue;
        };
        if *active_scope != scope {
            return Err(HelixDbError::InvariantViolation(
                "vector cache hydration received an Active generation from another scope"
                    .to_string(),
            ));
        }
        match layout {
            VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                if !physical_ids.insert(physical_index_id.get()) {
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
                    scope,
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
                    } = IndexKey::parse_from_slice(scope, &row.key)?
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
                    if !physical_ids.insert(mapping.physical_index_id.get()) {
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
    registry.sweep(
        scope,
        &targets
            .iter()
            .map(VectorCacheIdentity::from_validated)
            .collect(),
        match source {
            // Writer entries are released only by retirement, whose tombstones
            // outlive the Active inventory until physical cleanup completes.
            VectorCacheSnapshotSource::Writer(_) => VectorCacheSweep::OrphanFences,
            VectorCacheSnapshotSource::Reader(_) => VectorCacheSweep::InactiveEntries,
        },
    );
    if target_count == 0 {
        return Ok(outcome);
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
        let mut hydration = registry.prepare_hydration(&handle, admission);
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
        // The status is read before the snapshot: the poller reports a new
        // sequence before it installs the state, so a sequence above both
        // values proves the reader moved past this snapshot.
        let reader_observed = reader_status
            .as_ref()
            .map(|status| status.borrow().durable_seq);
        let snapshot = source.snapshot().await?;
        let retained_bytes = match &mut hydration {
            VectorCacheHydration::Refresh(refresh) => {
                refresh.retain_if_current(snapshot.seq()).await
            }
            VectorCacheHydration::Initial(_) | VectorCacheHydration::Unavailable(_) => None,
        };
        let (estimated_bytes, publication) = match retained_bytes {
            Some(bytes) => (bytes, None),
            None => {
                let store = Arc::new(VectorMemoryStore::new(
                    handle.scope(),
                    handle.physical_index_id(),
                    snapshot.seq(),
                ));
                let load = store.load_descriptor_bound_with_budget(
                    snapshot.as_ref(),
                    admission,
                    shutdown.as_deref_mut(),
                );
                // An advance that is ready with a finished load still wins:
                // that store is already stale.
                let loaded = match reader_status.as_mut().zip(reader_observed) {
                    Some((status, observed)) => tokio::select! {
                        biased;
                        _ = reader_advances_past(
                            status,
                            |reader: &slatedb::DbStatus| reader.durable_seq,
                            observed.max(snapshot.seq()),
                        ) => None,
                        loaded = load => Some(loaded),
                    },
                    None => Some(load.await),
                };
                let Some(loaded) = loaded else {
                    // Dropping the reservation keeps a refreshed entry's
                    // published store or returns an initial entry to `Vacant`.
                    tracing::debug!(
                        physical_index_id = handle.physical_index_id(),
                        "reader advanced during a vector cache load"
                    );
                    outcome = VectorCacheHydrationOutcome::Outrun;
                    continue;
                };
                let summary = match loaded {
                    Ok(summary) => summary,
                    // Dropping the reservation returns an initial entry to
                    // `Vacant` or keeps a refreshed entry's published store.
                    Err(error) => return Err(error),
                };
                if summary.completion == VectorMemoryStoreLoadCompletion::Shutdown {
                    break;
                }
                (summary.estimated_bytes, Some((hydration, store)))
            }
        };
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(estimated_bytes) else {
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
        let Some((hydration, store)) = publication else {
            continue;
        };
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
    Ok(outcome)
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../../tests/production_support/vector/hydration.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn idle_refresh_and_production_contracts() {
        super::production_contracts::run().await;
    }

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{
        path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult,
    };
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
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![active.clone()],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        let first_guard = registry.resident_guard_for(&handle).unwrap();
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
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![active.clone()],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        let second_guard = registry.resident_guard_for(&handle).unwrap();
        assert_eq!(second_guard.store().visible_seq(), second_snapshot.seq());
        assert!(second_guard.store().get_upper_vector(2).is_some());
        assert!(first_guard.store().get_upper_vector(2).is_none());

        hydrate_active_generations(
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Bounded(0),
            None,
        )
        .await
        .unwrap();
        let empty_guard = registry.resident_guard_for(&handle).unwrap();
        assert_eq!(empty_guard.store().estimated_bytes(), 0);
        assert!(empty_guard.store().get_upper_vector(1).is_none());
        assert!(empty_guard.store().get_upper_vector(2).is_none());
        assert!(second_guard.store().get_upper_vector(2).is_some());
    }

    #[tokio::test]
    async fn reader_hydration_publishes_exact_reader_sequences_and_sweeps_inactive_entries() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("vector-cache-reader-hydration", Arc::clone(&object_store))
            .build()
            .await
            .unwrap();
        let scope = DataScope::LegacyUnscoped;
        let (active, handle) = active_vector(scope, 7, 71, false);
        let (inactive, inactive_handle) = active_vector(scope, 8, 81, false);
        let upper_vector = |physical_index_id, node_id| {
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                    physical_index_id,
                    node_id,
                ))),
            }
            .to_bytes()
        };
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(upper_vector(71, 1), Bytes::from_static(b"first"))
            .unwrap();
        transaction
            .put(upper_vector(81, 1), Bytes::from_static(b"inactive"))
            .unwrap();
        transaction.commit().await.unwrap();
        db.flush().await.unwrap();
        let reader = slatedb::DbReader::open(
            "vector-cache-reader-hydration",
            object_store,
            None,
            slatedb::config::DbReaderOptions {
                manifest_poll_interval: std::time::Duration::from_millis(10),
                wal_poll_interval: std::time::Duration::from_millis(10),
                ..slatedb::config::DbReaderOptions::default()
            },
        )
        .await
        .unwrap();
        let registry = VectorCacheRegistry::default();

        hydrate_active_generations(
            VectorCacheSnapshotSource::Reader(&reader),
            scope,
            vec![active.clone(), inactive.clone()],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        let first_seq = reader.snapshot().await.unwrap().seq();
        let first = registry.resident_guard_for(&handle).unwrap();
        assert_eq!(first.store().visible_seq(), first_seq);
        assert_eq!(
            first.store().get_upper_vector(1).as_deref(),
            Some(b"first".as_slice())
        );
        assert!(registry.resident_guard_for(&inactive_handle).is_ok());
        hydrate_active_generations(
            VectorCacheSnapshotSource::Reader(&reader),
            scope,
            vec![active.clone(), inactive],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(
                first.store(),
                registry.resident_guard_for(&handle).unwrap().store()
            ),
            "an unchanged reader sequence retains the published store"
        );

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(upper_vector(71, 2), Bytes::from_static(b"second"))
            .unwrap();
        transaction.commit().await.unwrap();
        db.flush().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while reader.snapshot().await.unwrap().seq() == first_seq {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("reader applies the writer commit");
        hydrate_active_generations(
            VectorCacheSnapshotSource::Reader(&reader),
            scope,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();

        let second = registry.resident_guard_for(&handle).unwrap();
        assert_eq!(
            second.store().visible_seq(),
            reader.snapshot().await.unwrap().seq()
        );
        assert!(second.store().visible_seq() > first_seq);
        assert_eq!(
            second.store().get_upper_vector(2).as_deref(),
            Some(b"second".as_slice())
        );
        assert!(
            first.store().get_upper_vector(2).is_none(),
            "a retained guard keeps its immutable older snapshot"
        );
        assert!(
            matches!(
                registry.resident_guard_for(&inactive_handle),
                Err(super::super::registry::VectorCacheReadGuardError::Absent)
            ),
            "readers release generations that left the Active inventory"
        );
        drop((first, second));
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn commit_fenced_hydration_retains_across_unfenced_sequences_and_rescans_after_commits() {
        let db = raw_db("vector-cache-commit-fenced-hydration").await;
        let scope = DataScope::LegacyUnscoped;
        let (active, handle) = active_vector(scope, 7, 71, false);
        let upper_vector = |node_id| {
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                    71, node_id,
                ))),
            }
            .to_bytes()
        };
        db.put(upper_vector(1), Bytes::from_static(b"first"))
            .await
            .unwrap();
        let registry =
            VectorCacheRegistry::new(super::super::registry::VectorCacheVisibility::CommitFenced);
        let hydrate = || {
            hydrate_active_generations(
                VectorCacheSnapshotSource::Writer(&db),
                scope,
                vec![active.clone()],
                &registry,
                VectorCacheHydrationBudget::Unbounded,
                None,
            )
        };
        hydrate().await.unwrap();
        let first = registry.resident_guard_for(&handle).unwrap();

        db.put(b"unrelated-graph-row", Bytes::from_static(b"row"))
            .await
            .unwrap();
        let newer_seq = db.snapshot().await.unwrap().seq();
        assert!(newer_seq > first.store().visible_seq());
        hydrate().await.unwrap();
        assert!(
            Arc::ptr_eq(
                first.store(),
                registry.resident_guard_for(&handle).unwrap().store()
            ),
            "an unfenced sequence advance does not rescan a commit-fenced store"
        );
        assert!(registry.read_guard_for(&handle, newer_seq).is_ok());

        let writes = super::super::commit::VectorCacheWriteSet::default();
        writes.dirty_rows_for(&handle).mark_node_dirty(2);
        let fences = writes
            .entries()
            .iter()
            .filter_map(|write| registry.prepare_commit(write))
            .collect::<Vec<_>>();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(upper_vector(2), Bytes::from_static(b"second"))
            .unwrap();
        super::super::commit::commit_fenced(transaction, fences)
            .await
            .unwrap();
        hydrate().await.unwrap();

        let rescanned = registry.resident_guard_for(&handle).unwrap();
        assert!(!Arc::ptr_eq(first.store(), rescanned.store()));
        assert_eq!(
            rescanned.store().get_upper_vector(2).as_deref(),
            Some(b"second".as_slice())
        );
        assert!(first.store().get_upper_vector(2).is_none());
        drop((first, rescanned));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn hydration_rejects_an_active_generation_from_another_scope() {
        let db = raw_db("vector-cache-foreign-scope").await;
        let (foreign, handle) =
            active_vector(DataScope::Tenant(TenantId::from_u128(9)), 5, 55, false);
        let registry = VectorCacheRegistry::default();

        let error = hydrate_active_generations(
            VectorCacheSnapshotSource::Writer(&db),
            DataScope::LegacyUnscoped,
            vec![foreign],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, HelixDbError::InvariantViolation(_)));
        assert!(registry.resident_guard_for(&handle).is_err());
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
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap();
        let guard = registry.resident_guard_for(&handle).unwrap();
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
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![high_active, low_active],
            &registry,
            VectorCacheHydrationBudget::Bounded(row_bytes * 2 - 1),
            None,
        )
        .await
        .unwrap();

        let low = registry.resident_guard_for(&low_handle).unwrap();
        assert_eq!(low.store().estimated_bytes(), row_bytes);
        assert_eq!(low.store().get_upper_vector(1).as_deref(), Some(&value[..]));
        assert_eq!(
            registry
                .resident_guard_for(&high_handle)
                .unwrap()
                .store()
                .estimated_bytes(),
            0
        );
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
        for (scope, active) in [(second_scope, second_active), (first_scope, first_active)] {
            hydrate_active_generations(
                VectorCacheSnapshotSource::Writer(&db),
                scope,
                vec![active],
                &registry,
                VectorCacheHydrationBudget::Unbounded,
                None,
            )
            .await
            .unwrap();
        }

        let first = registry.resident_guard_for(&first_handle).unwrap();
        let second = registry.resident_guard_for(&second_handle).unwrap();
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
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![first_active, second_active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(registry.resident_guard_for(&first_handle).is_err());
        assert!(registry.resident_guard_for(&second_handle).is_err());
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
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(registry.resident_guard_for(&handle).is_err());
    }

    #[tokio::test]
    async fn reader_advance_resolves_only_above_the_current_sequence() {
        let (status, mut observed) = watch::channel((5_u64, 0_u64));
        assert_eq!(
            reader_advances_past(&mut observed, |(sequence, _)| *sequence, 4).await,
            5,
            "a sequence already above the bound resolves at once"
        );

        let mut advance = Box::pin(reader_advances_past(
            &mut observed,
            |(sequence, _)| *sequence,
            5,
        ));
        for manifest in 1..=3 {
            status.send_replace((5, manifest));
            assert!(
                futures::poll!(advance.as_mut()).is_pending(),
                "a manifest-only status change keeps the sequence"
            );
        }
        status.send_replace((7, 3));
        assert_eq!(advance.await, 7);
    }

    /// How the gated object store treats reads of compacted SSTs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SstReads {
        Open,
        Held,
    }

    /// In-memory object store that can hold reads of compacted SSTs, so a
    /// reader load stays in flight while the reader replays newer WAL.
    #[derive(Debug)]
    struct GatedSstStore {
        inner: InMemory,
        reads: watch::Sender<SstReads>,
        /// Counts compacted SST reads that found the gate held.
        held_reads: watch::Sender<usize>,
    }

    impl std::fmt::Display for GatedSstStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("gated-sst-memory")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for GatedSstStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            if location.as_ref().contains("/compacted/") {
                let mut reads = self.reads.subscribe();
                if *reads.borrow_and_update() == SstReads::Held {
                    self.held_reads.send_modify(|held| *held += 1);
                }
                reads
                    .wait_for(|reads| *reads == SstReads::Open)
                    .await
                    .expect("the gate outlives its store");
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn reader_drops_only_the_load_it_outruns_and_loads_later_targets_fresh() {
        let path = "vector-cache-reader-outrun";
        let gate = Arc::new(GatedSstStore {
            inner: InMemory::new(),
            reads: watch::Sender::new(SstReads::Open),
            held_reads: watch::Sender::new(0),
        });
        let object_store: Arc<dyn ObjectStore> = Arc::clone(&gate) as Arc<dyn ObjectStore>;
        let db = Db::builder(path, Arc::clone(&object_store))
            .build()
            .await
            .unwrap();
        let scope = DataScope::LegacyUnscoped;
        let (outrun, outrun_handle) = active_vector(scope, 7, 71, false);
        let (fresh, fresh_handle) = active_vector(scope, 8, 81, false);
        let upper_vector = |physical_index_id, node_id| {
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::UpperVector(VectorUpperVectorKey::new(
                    physical_index_id,
                    node_id,
                ))),
            }
            .to_bytes()
        };
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(upper_vector(71, 1), Bytes::from_static(b"outrun"))
            .unwrap();
        transaction
            .put(upper_vector(81, 1), Bytes::from_static(b"fresh"))
            .unwrap();
        transaction.commit().await.unwrap();
        // Both rows live only in a compacted SST, so each load reads through the gate.
        db.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await
        .unwrap();
        let reader = slatedb::DbReader::open(
            path,
            object_store,
            None,
            slatedb::config::DbReaderOptions {
                manifest_poll_interval: std::time::Duration::from_millis(10),
                wal_poll_interval: std::time::Duration::from_millis(10),
                ..slatedb::config::DbReaderOptions::default()
            },
        )
        .await
        .unwrap();
        let hydrated_seq = reader.snapshot().await.unwrap().seq();
        let registry = VectorCacheRegistry::default();

        gate.reads.send_replace(SstReads::Held);
        let hydrate = hydrate_active_generations(
            VectorCacheSnapshotSource::Reader(&reader),
            scope,
            vec![fresh, outrun],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            None,
        );
        let advance = async {
            // The first target in canonical order is loading and waits on the gate.
            gate.held_reads
                .subscribe()
                .wait_for(|held| *held > 0)
                .await
                .unwrap();
            db.put(b"unrelated-graph-row", Bytes::from_static(b"row"))
                .await
                .unwrap();
            reader
                .subscribe()
                .wait_for(|status| status.durable_seq > hydrated_seq)
                .await
                .unwrap();
            gate.reads.send_replace(SstReads::Open);
        };
        let (outcome, ()) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            tokio::join!(hydrate, advance)
        })
        .await
        .expect("the pass finishes once the gate opens");

        assert_eq!(outcome.unwrap(), VectorCacheHydrationOutcome::Outrun);
        assert!(
            matches!(
                registry.resident_guard_for(&outrun_handle),
                Err(
                    super::super::registry::VectorCacheReadGuardError::Unavailable(
                        super::super::registry::VectorCacheLifecycle::Vacant
                    )
                )
            ),
            "the outrun load publishes nothing and leaves its entry for the next pass"
        );
        let fresh_guard = registry.resident_guard_for(&fresh_handle).unwrap();
        assert!(
            fresh_guard.store().visible_seq() > hydrated_seq,
            "the next target loads from a fresh snapshot the reader has not moved past"
        );
        assert_eq!(
            fresh_guard.store().visible_seq(),
            reader.snapshot().await.unwrap().seq()
        );
        assert_eq!(
            fresh_guard.store().get_upper_vector(1).as_deref(),
            Some(b"fresh".as_slice())
        );
        drop(fresh_guard);
        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn reader_advance_never_resolves_after_its_status_channel_closes() {
        let (status, mut observed) = watch::channel(5_u64);
        drop(status);

        let mut advance = Box::pin(reader_advances_past(&mut observed, |sequence| *sequence, 5));
        assert!(futures::poll!(advance.as_mut()).is_pending());
        assert!(futures::poll!(advance.as_mut()).is_pending());
    }

    #[tokio::test]
    async fn shutdown_before_hydration_publishes_nothing() {
        let db = raw_db("vector-cache-shutdown-hydration").await;
        let scope = DataScope::LegacyUnscoped;
        let (active, handle) = active_vector(scope, 60, 606, false);
        let registry = VectorCacheRegistry::default();
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(true);

        hydrate_active_generations(
            VectorCacheSnapshotSource::Writer(&db),
            scope,
            vec![active],
            &registry,
            VectorCacheHydrationBudget::Unbounded,
            Some(&mut shutdown_rx),
        )
        .await
        .unwrap();

        assert!(registry.resident_guard_for(&handle).is_err());
    }
}
