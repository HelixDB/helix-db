//! Blocking, bounded catalog migration to independent range directions.
//!
//! V6 without the readiness marker fences serving before the first metadata
//! rewrite. Each canonical record and its sole retained operation move in one
//! serializable transaction. Physical entries, generations, queue pointers and
//! progress cursors are unchanged. Repeated scans need no persisted cursor.

use bytes::Bytes;
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::encoding::v2::keys::metadata::MetadataKey;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey, ScopedKey};
use crate::encoding::v2::legacy::range_identity;
use crate::encoding::v2::values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{self as lifecycle, IndexStateV2};

const READY: &[u8] = b"kv_migration_ready:range_directions";

pub(crate) async fn ready(reader: &(impl DbReadOps + Sync)) -> Result<bool> {
    match reader
        .get(MetadataKey::new(READY).to_bytes())
        .await?
        .as_deref()
    {
        None => Ok(false),
        Some(b"1") => Ok(true),
        Some(_) => Err(corruption("range direction readiness marker is malformed")),
    }
}

pub(crate) fn stage_ready(transaction: &DbTransaction) -> Result<()> {
    transaction.put(MetadataKey::new(READY).to_bytes(), Bytes::from_static(b"1"))?;
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
    // Validate every linked pair and detect orphan operations before fencing.
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
        let Some((scope, ScopedKey::IndexRecord(_))) = catalog_key(&row.key)? else {
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
fn catalog_key(key: &[u8]) -> Result<Option<(DataScope, ScopedKey)>> {
    let (scope, logical) = match DataScope::strip_tenant_envelope(key) {
        Some((tenant, logical)) => (DataScope::Tenant(tenant), logical),
        None => (DataScope::LegacyUnscoped, key),
    };
    if !logical.starts_with(&[ScopedKey::key_prefix(), 0x01])
        && !logical.starts_with(&[ScopedKey::key_prefix(), 0x02])
    {
        return Ok(None);
    }
    Ok(Some((scope, ScopedKey::parse_from_slice(logical)?)))
}

async fn validate_catalog(db: &Db, current: bool) -> Result<()> {
    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        let Some((scope, key)) = catalog_key(&row.key)? else {
            continue;
        };
        match key {
            ScopedKey::IndexRecord(_) => {
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
            ScopedKey::Operation(key) => {
                let operation = values::decode_operation_record(&row.value)?;
                if !operation.identity().family().is_range() {
                    continue;
                }
                if key.operation_id != operation.operation_id() {
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
            _ => unreachable!("catalog_key only accepts index and operation prefixes"),
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

    async fn fixture(
        db: &Db,
        scope: DataScope,
        edge: bool,
        direction: RangeIndexDirection,
    ) -> (IndexRecordV2, IndexOperationRecord, Bytes) {
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
        // Frozen pre-V6 framing: version/kind, ID, undirected family. The
        // definition's direction remains in place. No migration encoder is used.
        let mut old_record = values::encode_index_record(&record).to_vec();
        old_record[2 + core::mem::size_of::<u64>()] = 0x02;
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
        tx.put(source.clone(), Bytes::from_static(b"graph bytes unchanged"))
            .unwrap();
        tx.commit().await.unwrap();
        (record, operation, key)
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
                    let (record, operation, old_key) =
                        fixture(&db, scope, edge, RangeIndexDirection::Desc).await;
                    assert!(matches!(
                        lifecycle::repository::require_reader_bootstrap_or_legacy(&db).await,
                        Err(HelixDbError::WriterMigrationRequired { .. })
                    ));
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
    async fn malformed_and_conflicting_catalogs_fail_before_any_migration_write() {
        for damage in 0..4 {
            let db = Db::builder("range-corruption", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap();
            let scope = DataScope::LegacyUnscoped;
            let (record, operation, old_key) =
                fixture(&db, scope, false, RangeIndexDirection::Desc).await;
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
