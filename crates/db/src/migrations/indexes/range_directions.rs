//! Blocking, bounded catalog migration to independent range directions.
//!
//! V6 without the readiness marker fences serving before the first metadata
//! rewrite. Each canonical record and its sole retained operation move in one
//! serializable transaction. Physical entries, generations, queue pointers and
//! progress cursors are unchanged. Repeated scans need no persisted cursor.

#[cfg(test)]
use bytes::Bytes;
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::encoding::v2::keys::metadata::MetadataKey;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey, ScopedKey};
use crate::encoding::v2::legacy::range_identity;
use crate::encoding::v2::values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{self as lifecycle, IndexStateV2};

pub(crate) async fn ready(reader: &(impl DbReadOps + Sync)) -> Result<bool> {
    match reader
        .get(MetadataKey::range_catalog_ready().to_bytes())
        .await?
        .as_deref()
    {
        None => Ok(false),
        Some(value) => values::RangeCatalogReady::decode(value)
            .map(|_| true)
            .map_err(Into::into),
    }
}

pub(crate) fn stage_ready(transaction: &DbTransaction) -> Result<()> {
    transaction.put(
        MetadataKey::range_catalog_ready().to_bytes(),
        values::RangeCatalogReady.encode(),
    )?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Boundary {
    Fenced,
    PairStaged,
    PairCommitted,
    Verified,
    Ready,
}

pub(crate) async fn migrate(db: &Db) -> Result<()> {
    migrate_inner(db, |_| Ok(())).await
}

async fn migrate_inner(db: &Db, mut checkpoint: impl FnMut(Boundary) -> Result<()>) -> Result<()> {
    if ready(db).await? {
        let marker = db
            .get(
                ManagedIndexKey::Global {
                    kind: GlobalKey::StorageVersion,
                }
                .to_bytes(),
            )
            .await?;
        let version = marker
            .as_deref()
            .map(values::decode_metadata_value)
            .transpose()?;
        if version
            != Some(lifecycle::IndexV2MetadataValue::StorageVersion(
                lifecycle::IndexStorageVersion::CURRENT,
            ))
        {
            return Err(corruption("range readiness requires storage V6"));
        }
        return Ok(());
    }
    // Validate every range pair and detect orphan range operations before fencing.
    validate_catalog(db, false).await?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    transaction.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        values::encode_metadata_value(&lifecycle::IndexV2MetadataValue::StorageVersion(
            lifecycle::IndexStorageVersion::CURRENT,
        )),
    )?;
    transaction.commit().await?;
    checkpoint(Boundary::Fenced)?;

    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        let Some((scope, CatalogRow::Index)) = catalog_key(&row.key)? else {
            continue;
        };
        let definition = values::decode_pre_direction_index_record(&row.value)?;
        if !definition.identity().family().is_range() {
            continue;
        }
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let Some(value) = transaction.get(&row.key).await? else {
            return Err(corruption(
                "catalog record disappeared during direction migration",
            ));
        };
        let (record, operation) = read_pair(&transaction, scope, &row.key, &value, false).await?;
        let destination =
            lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity());
        if destination != row.key {
            if transaction.get(&destination).await?.is_some() {
                return Err(corruption("direction migration destination already exists"));
            }
            transaction.put(destination, values::encode_index_record(&record))?;
            transaction.put(
                lifecycle::outbox::scoped_operation_key(scope, operation.operation_id()),
                values::encode_operation_record(&operation),
            )?;
            transaction.delete(row.key)?;
            checkpoint(Boundary::PairStaged)?;
            transaction.commit().await?;
            checkpoint(Boundary::PairCommitted)?;
        } else {
            transaction.rollback();
        }
    }
    validate_catalog(db, true).await?;
    checkpoint(Boundary::Verified)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    stage_ready(&transaction)?;
    transaction.commit().await?;
    checkpoint(Boundary::Ready)?;
    Ok(())
}

/// Recognizes catalog rows without treating malformed catalog keys as absence.
enum CatalogRow {
    Index,
    Operation(lifecycle::IndexOperationId),
}

fn catalog_key(key: &[u8]) -> Result<Option<(DataScope, CatalogRow)>> {
    let (scope, logical) = match DataScope::strip_tenant_envelope(key) {
        Some((tenant, logical)) => (DataScope::Tenant(tenant), logical),
        None => (DataScope::LegacyUnscoped, key),
    };
    if !logical.starts_with(&[ScopedKey::key_prefix(), 0x01])
        && !logical.starts_with(&[ScopedKey::key_prefix(), 0x02])
    {
        return Ok(None);
    }
    if logical[1] == 0x01 {
        let ScopedKey::IndexRecord(_) = ScopedKey::parse_from_slice(logical)? else {
            return Err(corruption(
                "index catalog prefix decoded as another key kind",
            ));
        };
        Ok(Some((scope, CatalogRow::Index)))
    } else {
        let ScopedKey::Operation(key) = ScopedKey::parse_from_slice(logical)? else {
            return Err(corruption(
                "operation catalog prefix decoded as another key kind",
            ));
        };
        Ok(Some((scope, CatalogRow::Operation(key.operation_id))))
    }
}

async fn validate_catalog(db: &Db, current: bool) -> Result<()> {
    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        let Some((scope, key)) = catalog_key(&row.key)? else {
            continue;
        };
        match key {
            CatalogRow::Index => {
                let definition = values::decode_pre_direction_index_record(&row.value)?;
                if !definition.identity().family().is_range() {
                    continue;
                }
                let (record, _) = read_pair(db, scope, &row.key, &row.value, current).await?;
                let destination =
                    lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity());
                if destination != row.key && db.get(&destination).await?.is_some() {
                    return Err(corruption("direction migration destination already exists"));
                }
            }
            CatalogRow::Operation(operation_id) => {
                let operation = values::decode_operation_record(&row.value)?;
                if !operation.identity().family().is_range() {
                    continue;
                }
                if operation_id != operation.operation_id() {
                    return Err(corruption("operation key differs from its record"));
                }
                let canonical =
                    lifecycle::outbox::scoped_index_key_for_identity(scope, operation.identity());
                let Some(value) = db.get(&canonical).await? else {
                    return Err(corruption("direction migration found an orphan operation"));
                };
                let (_, linked) = read_pair(db, scope, &canonical, &value, current).await?;
                if linked.operation_id() != operation.operation_id() {
                    return Err(corruption(
                        "direction migration found an unlinked operation",
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn read_pair(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    key: &[u8],
    value: &[u8],
    current: bool,
) -> Result<(lifecycle::IndexRecordV2, lifecycle::IndexOperationRecord)> {
    let record = if current {
        values::decode_index_record(value)?
    } else {
        values::decode_pre_direction_index_record(value)?
    };
    let canonical = lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity());
    let legacy = lifecycle::outbox::scoped_index_key_for_identity(
        scope,
        &range_identity::undirected(record.identity()),
    );
    if key != canonical && (current || key != legacy) {
        return Err(corruption("range catalog key differs from its definition"));
    }
    if key == canonical && canonical != legacy {
        // A committed move rewrites the key and value together. A directed key
        // with an old value is corruption, not a valid restart boundary.
        values::decode_index_record(value)?;
    }
    let operation_id = match record.state() {
        IndexStateV2::Building {
            build_operation_id, ..
        }
        | IndexStateV2::Aborting {
            build_operation_id, ..
        } => *build_operation_id,
        IndexStateV2::Active {
            completed_build_operation_id,
            ..
        } => *completed_build_operation_id,
        IndexStateV2::Dropping {
            drop_operation_id, ..
        } => *drop_operation_id,
        IndexStateV2::Dropped {
            completed_operation_id,
            ..
        } => *completed_operation_id,
    };
    let operation_key = lifecycle::outbox::scoped_operation_key(scope, operation_id);
    let Some(value) = reader.get(operation_key).await? else {
        return Err(corruption("range catalog retained operation is missing"));
    };
    let operation = values::decode_operation_record(&value)?;
    let operation = if !current
        && key != canonical
        && operation.identity() == &range_identity::undirected(record.identity())
    {
        lifecycle::IndexOperationRecord::try_new_with_queue_schedule(
            operation.operation_id(),
            operation.index_id(),
            record.identity().clone(),
            operation.generation(),
            operation.index_record_revision(),
            operation.operation_revision(),
            operation.kind(),
            operation.family(),
            operation.progress().clone(),
            operation.attempt(),
            operation.execution_state().clone(),
            operation.queue_schedule(),
        )
        .map_err(|error| corruption(&error.to_string()))?
    } else {
        operation
    };
    let pointer = reader
        .get(
            ManagedIndexKey::Global {
                kind: GlobalKey::OperationPointer(operation_id),
            }
            .to_bytes(),
        )
        .await?
        .map(|value| values::decode_metadata_value(&value))
        .transpose()?;
    let pointer = match pointer {
        Some(lifecycle::IndexV2MetadataValue::OperationQueuePointer(pointer)) => Some(pointer),
        None => None,
        Some(_) => return Err(corruption("operation pointer has the wrong value kind")),
    };
    lifecycle::outbox::validate_link(scope, &record, &operation, pointer.as_ref())?;
    Ok((record, operation))
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RangeIndexDirection, SecondaryIndexDefinition};
    use crate::encoding::v2::keys::scope::TenantId;
    use crate::encoding::v2::keys::{DataKey, DataKeyKind, EdgePropertyByIdKey, NodePropertyKey};
    use crate::index_lifecycle as lifecycle;
    use crate::index_lifecycle::*;
    use slatedb::object_store::memory::InMemory;
    use std::sync::Arc;

    #[derive(Clone, Copy, Debug)]
    enum Phase {
        Building,
        Active,
        Aborting,
        Aborted,
        Claimed,
        Dropping,
        Dropped,
        Blocked,
    }

    async fn fixture(
        db: &Db,
        scope: DataScope,
        edge: bool,
        direction: RangeIndexDirection,
        phase: Phase,
    ) -> (IndexRecordV2, IndexOperationRecord, Bytes, Bytes) {
        let definition: ValidatedDynamicIndexDefinition = if edge {
            SecondaryIndexDefinition::edge_range_with_direction("Item", "value", direction).unwrap()
        } else {
            SecondaryIndexDefinition::node_range_with_direction("Item", "value", direction).unwrap()
        }
        .try_into()
        .unwrap();
        let op_id = IndexOperationId::from_bytes([7; 16]).unwrap();
        let record = IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            op_id,
        )
        .unwrap();
        let source = DataKey::Data {
            scope,
            kind: if edge {
                DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(8))
            } else {
                DataKeyKind::NodeProperty(NodePropertyKey::new(8))
            },
        }
        .to_bytes();
        let operation = IndexOperationRecord::try_new(
            op_id,
            record.index_id(),
            record.identity().clone(),
            record.state().generation(),
            record.revision(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Scan(SourceScanProgress {
                    inclusive_upper_bound: IndexCursor::try_new(source.clone()).unwrap(),
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let physical = PhysicalGeneration::Secondary {
            generation: record.state().generation(),
        };
        let cleanup = SecondaryCleanupProgress::Finalize(NoCursorProgress::default());
        let (state, progress, execution) = match phase {
            Phase::Building => (
                record.state().clone(),
                operation.progress().clone(),
                operation.execution_state().clone(),
            ),
            Phase::Active => (
                IndexStateV2::Active {
                    physical,
                    completed_build_operation_id: op_id,
                },
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Activate(NoCursorProgress::default()),
                )),
                IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Succeeded,
                )),
            ),
            Phase::Aborting => (
                IndexStateV2::Aborting {
                    physical,
                    build_operation_id: op_id,
                },
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(cleanup)),
                operation.execution_state().clone(),
            ),
            Phase::Aborted => (
                IndexStateV2::Dropped {
                    last_generation: record.state().generation(),
                    completed_operation_id: op_id,
                },
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(cleanup)),
                IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Aborted,
                )),
            ),
            Phase::Claimed => (
                record.state().clone(),
                operation.progress().clone(),
                IndexOperationExecutionState::Claimed(OperationClaim {
                    writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                    sequence: ClaimSequence::new(8).unwrap(),
                }),
            ),
            Phase::Dropping => (
                IndexStateV2::Dropping {
                    physical,
                    drop_operation_id: op_id,
                },
                IndexOperationProgress::SecondaryCleanup(cleanup),
                operation.execution_state().clone(),
            ),
            Phase::Dropped => (
                IndexStateV2::Dropped {
                    last_generation: record.state().generation(),
                    completed_operation_id: op_id,
                },
                IndexOperationProgress::SecondaryCleanup(cleanup),
                IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
            ),
            Phase::Blocked => (
                record.state().clone(),
                operation.progress().clone(),
                IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                    entity_kind: record.identity().element_kind(),
                    entity_id: IndexEntityId::new(8),
                }),
            ),
        };
        let record = IndexRecordV2::try_new(
            record.index_id(),
            record.identity().clone(),
            record.definition().clone(),
            record.revision(),
            state,
        )
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            op_id,
            record.index_id(),
            record.identity().clone(),
            record.state().generation(),
            record.revision(),
            operation.operation_revision(),
            progress.kind(),
            operation.family(),
            progress,
            operation.attempt(),
            execution,
        )
        .unwrap();
        // Frozen pre-V6 framing: version/kind, ID, undirected family. The
        // definition's direction remains in place. No migration encoder is used.
        let mut old_record = values::encode_index_record(&record).to_vec();
        old_record[2 + core::mem::size_of::<u64>()] = 0x02;
        assert_eq!(
            values::decode_index_record(&old_record).is_ok(),
            direction == RangeIndexDirection::Asc
        );
        assert_eq!(
            values::decode_pre_direction_index_record(&old_record).unwrap(),
            record
        );
        let mut old_operation = values::encode_operation_record(&operation).to_vec();
        old_operation[2 + 16 + core::mem::size_of::<u64>()] = 0x02;
        let key = lifecycle::outbox::scoped_index_key_for_identity(
            scope,
            &range_identity::undirected(record.identity()),
        );
        let tx = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        tx.put(key.clone(), Bytes::from(old_record)).unwrap();
        tx.put(
            lifecycle::outbox::scoped_operation_key(scope, op_id),
            Bytes::from(old_operation),
        )
        .unwrap();
        if matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Queued { .. } | IndexOperationExecutionState::Claimed(_)
        ) {
            tx.put(
                ManagedIndexKey::Global {
                    kind: GlobalKey::OperationPointer(op_id),
                }
                .to_bytes(),
                values::encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                    OperationQueuePointerValue {
                        scope,
                        index_id: record.index_id(),
                        generation: record.state().generation(),
                        record_revision: operation.operation_revision(),
                    },
                )),
            )
            .unwrap();
        }
        tx.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
            values::encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(4).unwrap(),
            )),
        )
        .unwrap();
        tx.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::LogicalIndexIdWatermark,
            }
            .to_bytes(),
            values::encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::new(2).unwrap(),
                },
            )),
        )
        .unwrap();
        tx.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::VectorPhysicalIdWatermark,
            }
            .to_bytes(),
            values::encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                },
            )),
        )
        .unwrap();
        crate::migrations::stage_current_storage_schema_ready(&tx).unwrap();
        crate::migrations::stage_index_storage_v4_cleanup_ready(&tx).unwrap();
        use crate::encoding::v2::keys;
        let lane = match (edge, direction) {
            (false, RangeIndexDirection::Asc) => keys::SecondaryEntryLane::NodeRangeAscending,
            (false, RangeIndexDirection::Desc) => keys::SecondaryEntryLane::NodeRangeDescending,
            (true, RangeIndexDirection::Asc) => keys::SecondaryEntryLane::EdgeRangeAscending,
            (true, RangeIndexDirection::Desc) => keys::SecondaryEntryLane::EdgeRangeDescending,
        };
        let physical = ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::SecondaryEntry(
                keys::SecondaryEntryKey::try_new(
                    record.index_id(),
                    record.state().generation(),
                    lane,
                    keys::CanonicalSecondaryValue::range_string(
                        lane.range_direction().unwrap(),
                        "frozen",
                    ),
                    Some(IndexEntityId::new(8)),
                )
                .unwrap(),
            ),
        }
        .to_bytes();
        tx.put(
            physical.clone(),
            values::encode_secondary_entry(&crate::index_lifecycle::work::SecondaryEntryValue {
                index_id: record.index_id(),
                generation: record.state().generation(),
                lane,
                entity_id: IndexEntityId::new(8),
            }),
        )
        .unwrap();
        tx.put(source.clone(), Bytes::from_static(b"graph bytes unchanged"))
            .unwrap();
        tx.commit().await.unwrap();
        (record, operation, key, physical)
    }

    #[tokio::test]
    async fn legacy_reader_blocker_is_requeued_once_across_migration_restarts() {
        for scope in [
            DataScope::LegacyUnscoped,
            DataScope::Tenant(TenantId::from_u128(9)),
        ] {
            for edge in [false, true] {
                for phase in [Phase::Aborting, Phase::Dropping] {
                    for boundary in [
                        Boundary::Fenced,
                        Boundary::PairStaged,
                        Boundary::PairCommitted,
                        Boundary::Verified,
                        Boundary::Ready,
                    ] {
                        let store = Arc::new(InMemory::new());
                        let db = Db::builder("range-legacy-retry", store.clone())
                            .build()
                            .await
                            .unwrap();
                        let (record, operation, old_key, physical) =
                            fixture(&db, scope, edge, RangeIndexDirection::Desc, phase).await;
                        let blocked = operation
                            .claim(OperationClaim {
                                writer_epoch: WriterEpoch::from_bytes([9; 16]).unwrap(),
                                sequence: ClaimSequence::new(1).unwrap(),
                            })
                            .unwrap()
                            .block(IndexOperationBlocker::InvariantViolation)
                            .unwrap();
                        let expected = blocked.retry().unwrap();
                        let operation_key = lifecycle::outbox::scoped_operation_key(
                            scope,
                            operation.operation_id(),
                        );
                        let pointer_key = ManagedIndexKey::Global {
                            kind: GlobalKey::OperationPointer(operation.operation_id()),
                        }
                        .to_bytes();
                        let mut legacy = values::encode_operation_record(&blocked).to_vec();
                        legacy[2 + 16 + core::mem::size_of::<u64>()] = 0x02;
                        assert_eq!(legacy.last(), Some(&0x07));
                        *legacy.last_mut().unwrap() = 0x05;
                        assert!(
                            values::decode_operation_record_with_compatibility(&legacy)
                                .unwrap()
                                .1
                        );
                        let transaction = db
                            .begin(IsolationLevel::SerializableSnapshot)
                            .await
                            .unwrap();
                        transaction
                            .put(operation_key.clone(), Bytes::from(legacy))
                            .unwrap();
                        transaction.delete(pointer_key.clone()).unwrap();
                        transaction.commit().await.unwrap();
                        let physical_value = db.get(&physical).await.unwrap();
                        assert!(migrate_inner(&db, |at| {
                            if at == boundary {
                                Err(corruption("simulated process failure"))
                            } else {
                                Ok(())
                            }
                        })
                        .await
                        .is_err());
                        db.close().await.unwrap();
                        let db = Db::builder("range-legacy-retry", store)
                            .build()
                            .await
                            .unwrap();
                        crate::migrations::startup::bootstrap_writer(&db)
                            .await
                            .unwrap();
                        assert_eq!(
                            lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
                                &db, scope
                            )
                            .await
                            .unwrap(),
                            0
                        );
                        let value = db.get(&operation_key).await.unwrap().unwrap();
                        assert_eq!(
                            values::decode_operation_record_with_compatibility(&value).unwrap(),
                            (expected.clone(), false),
                            "{scope:?} {edge} {phase:?} {boundary:?}"
                        );
                        let pointer = lifecycle::OperationQueuePointerValue {
                            scope,
                            index_id: expected.index_id(),
                            generation: expected.generation(),
                            record_revision: expected.operation_revision(),
                        };
                        assert_eq!(
                            db.get(&pointer_key).await.unwrap(),
                            Some(values::encode_metadata_value(
                                &IndexV2MetadataValue::OperationQueuePointer(pointer)
                            ))
                        );
                        let canonical = lifecycle::outbox::scoped_index_key_for_identity(
                            scope,
                            record.identity(),
                        );
                        assert_eq!(
                            db.get(&canonical).await.unwrap(),
                            Some(values::encode_index_record(&record))
                        );
                        assert!(db.get(&old_key).await.unwrap().is_none());
                        assert_eq!(db.get(&physical).await.unwrap(), physical_value);
                        let before = db.snapshot().await.unwrap().seq();
                        migrate(&db).await.unwrap();
                        assert_eq!(db.snapshot().await.unwrap().seq(), before);
                        db.close().await.unwrap();
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn every_direction_migration_commit_boundary_resumes_after_reopen() {
        for scope in [
            DataScope::LegacyUnscoped,
            DataScope::Tenant(TenantId::from_u128(9)),
        ] {
            for edge in [false, true] {
                for boundary in [
                    Boundary::Fenced,
                    Boundary::PairStaged,
                    Boundary::PairCommitted,
                    Boundary::Verified,
                    Boundary::Ready,
                ] {
                    let store = Arc::new(InMemory::new());
                    let db = Db::builder("range-restart", store.clone())
                        .build()
                        .await
                        .unwrap();
                    let (record, operation, old_key, physical) =
                        fixture(&db, scope, edge, RangeIndexDirection::Desc, Phase::Building).await;
                    assert!(matches!(
                        lifecycle::repository::require_reader_bootstrap_or_legacy(&db).await,
                        Err(HelixDbError::WriterMigrationRequired { .. })
                    ));
                    let physical_value = db.get(&physical).await.unwrap();
                    assert!(physical_value.is_some());
                    let result = migrate_inner(&db, |at| {
                        if at == boundary {
                            Err(corruption("simulated process failure"))
                        } else {
                            Ok(())
                        }
                    })
                    .await;
                    assert!(result.is_err(), "{boundary:?}");
                    let gated =
                        lifecycle::repository::require_reader_bootstrap_or_legacy(&db).await;
                    assert_eq!(gated.is_ok(), boundary == Boundary::Ready);
                    db.close().await.unwrap();
                    let db = Db::builder("range-restart", store).build().await.unwrap();
                    crate::migrations::startup::bootstrap_writer(&db)
                        .await
                        .unwrap();
                    assert!(ready(&db).await.unwrap());
                    assert!(db.get(&old_key).await.unwrap().is_none());
                    assert_eq!(db.get(&physical).await.unwrap(), physical_value);
                    let key =
                        lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity());
                    let value = db.get(&key).await.unwrap().unwrap();
                    assert_eq!(values::decode_index_record(&value).unwrap(), record);
                    let value = db
                        .get(lifecycle::outbox::scoped_operation_key(
                            scope,
                            operation.operation_id(),
                        ))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(values::decode_operation_record(&value).unwrap(), operation);
                    assert!(
                        lifecycle::repository::require_reader_bootstrap_or_legacy(&db)
                            .await
                            .is_ok()
                    );
                    let before = db.snapshot().await.unwrap().seq();
                    migrate(&db).await.unwrap();
                    assert_eq!(db.snapshot().await.unwrap().seq(), before);
                    db.close().await.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn migration_preserves_every_retained_ddl_state_and_direction() {
        for phase in [
            Phase::Building,
            Phase::Active,
            Phase::Aborting,
            Phase::Aborted,
            Phase::Claimed,
            Phase::Dropping,
            Phase::Dropped,
            Phase::Blocked,
        ] {
            for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
                let db = Db::builder("range-state", Arc::new(InMemory::new()))
                    .build()
                    .await
                    .unwrap();
                let scope = DataScope::Tenant(TenantId::from_u128(7));
                let (record, operation, _, physical) =
                    fixture(&db, scope, true, direction, phase).await;
                let source = DataKey::Data {
                    scope,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(8)),
                }
                .to_bytes();
                let graph = db.get(&source).await.unwrap();
                let physical_value = db.get(&physical).await.unwrap();
                assert!(physical_value.is_some());
                migrate(&db).await.unwrap();
                let key =
                    lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity());
                let value = db.get(&key).await.unwrap().unwrap();
                assert_eq!(
                    read_pair(&db, scope, &key, &value, true).await.unwrap(),
                    (record, operation)
                );
                assert_eq!(db.get(&source).await.unwrap(), graph);
                assert_eq!(db.get(&physical).await.unwrap(), physical_value);
                db.close().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn readiness_cannot_override_an_old_or_malformed_storage_version() {
        for marker in [
            None,
            Some(values::encode_metadata_value(
                &IndexV2MetadataValue::StorageVersion(IndexStorageVersion::new(4).unwrap()),
            )),
            Some(Bytes::from_static(b"malformed")),
            Some(values::encode_metadata_value(
                &IndexV2MetadataValue::LogicalIndexIdWatermark(LogicalIndexIdWatermark {
                    next_id: IndexId::initial(),
                }),
            )),
        ] {
            let db = Db::builder("range-readiness", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap();
            let tx = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            stage_ready(&tx).unwrap();
            marker
                .into_iter()
                .try_for_each(|marker| {
                    tx.put(
                        ManagedIndexKey::Global {
                            kind: GlobalKey::StorageVersion,
                        }
                        .to_bytes(),
                        marker,
                    )
                })
                .unwrap();
            tx.commit().await.unwrap();
            let before = db.snapshot().await.unwrap().seq();
            assert!(migrate(&db).await.is_err());
            assert_eq!(db.snapshot().await.unwrap().seq(), before);
            db.close().await.unwrap();
        }
    }

    #[cfg(feature = "migration-parity")]
    #[tokio::test]
    async fn legacy_equality_fixture_rejects_directed_catalogs_without_writes() {
        let db = Db::builder("range-parity", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        fixture(
            &db,
            DataScope::LegacyUnscoped,
            false,
            RangeIndexDirection::Desc,
            Phase::Active,
        )
        .await;
        migrate(&db).await.unwrap();
        let before = db.snapshot().await.unwrap().seq();
        assert!(crate::migrations::make_legacy_equality_fixture(&db, 3)
            .await
            .is_err());
        assert_eq!(db.snapshot().await.unwrap().seq(), before);
        assert!(ready(&db).await.unwrap());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_and_conflicting_catalogs_fail_before_any_migration_write() {
        for damage in 0..8 {
            let db = Db::builder("range-corruption", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap();
            let scope = DataScope::LegacyUnscoped;
            let (record, operation, old_key, _physical) = fixture(
                &db,
                scope,
                false,
                RangeIndexDirection::Desc,
                Phase::Building,
            )
            .await;
            let tx = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            match damage {
                0 => tx
                    .delete(lifecycle::outbox::scoped_operation_key(
                        scope,
                        operation.operation_id(),
                    ))
                    .unwrap(),
                1 => tx
                    .put(
                        lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity()),
                        values::encode_index_record(&record),
                    )
                    .unwrap(),
                2 => tx.put(old_key, Bytes::from_static(b"malformed")).unwrap(),
                3 => tx
                    .delete(
                        ManagedIndexKey::Global {
                            kind: GlobalKey::OperationPointer(operation.operation_id()),
                        }
                        .to_bytes(),
                    )
                    .unwrap(),
                4 => tx.delete(old_key).unwrap(),
                5 => {
                    use crate::encoding::v2::keys;
                    let wrong_cursor = ManagedIndexKey::Data {
                        scope,
                        kind: ScopedKey::SecondaryEntry(
                            keys::SecondaryEntryKey::try_new(
                                record.index_id(),
                                record.state().generation(),
                                keys::SecondaryEntryLane::NodeRangeAscending,
                                keys::CanonicalSecondaryValue::range_string(
                                    crate::encoding::indexes::range::RangeIndexDirection::Asc,
                                    "wrong",
                                ),
                                Some(IndexEntityId::new(8)),
                            )
                            .unwrap(),
                        ),
                    }
                    .to_bytes();
                    let wrong = IndexOperationRecord::try_new(
                        operation.operation_id(),
                        operation.index_id(),
                        operation.identity().clone(),
                        operation.generation(),
                        operation.index_record_revision(),
                        operation.operation_revision(),
                        operation.kind(),
                        operation.family(),
                        IndexOperationProgress::SecondaryBuild(
                            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Validate(
                                PrefixScanProgress {
                                    cursor: Some(IndexCursor::try_new(wrong_cursor).unwrap()),
                                    counters: OperationCounters::default(),
                                },
                            )),
                        ),
                        operation.attempt(),
                        operation.execution_state().clone(),
                    )
                    .unwrap();
                    tx.put(
                        lifecycle::outbox::scoped_operation_key(scope, operation.operation_id()),
                        values::encode_operation_record(&wrong),
                    )
                    .unwrap();
                }
                6 => tx
                    .put(
                        ManagedIndexKey::Global {
                            kind: GlobalKey::OperationPointer(operation.operation_id()),
                        }
                        .to_bytes(),
                        values::encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                            IndexStorageVersion::CURRENT,
                        )),
                    )
                    .unwrap(),
                7 => {
                    let value = tx.get(&old_key).await.unwrap().unwrap();
                    tx.delete(old_key).unwrap();
                    tx.put(
                        lifecycle::outbox::scoped_index_key_for_identity(scope, record.identity()),
                        value,
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
            tx.commit().await.unwrap();
            let before = db.snapshot().await.unwrap().seq();
            assert!(migrate(&db).await.is_err());
            assert_eq!(db.snapshot().await.unwrap().seq(), before);
            assert!(!ready(&db).await.unwrap());
            db.close().await.unwrap();
        }
    }
}
