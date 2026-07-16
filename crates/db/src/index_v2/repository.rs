//! Durable V2 bootstrap, catalog loading, allocation, and handle validation.
//!
//! This is the sole boundary that turns typed V2 keys and values into SlateDB
//! operations. Writer bootstrap uses serializable-snapshot isolation and a
//! complete logical-keyspace scan so marker initialization cannot race another
//! metadata write.

use bytes::Bytes;
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::config::RuntimeIndexCatalog;
#[cfg(test)]
use crate::encoding::v1::keys::index_v2::GlobalIndexV2Kind;
use crate::encoding::v1::keys::index_v2::{
    GlobalIndexV2Key, IndexV2Key, IndexV2RecordKind, TextIntentOwnedKey, VectorPartitionMappingKey,
    GLOBAL_INDEX_V2_SENTINEL,
};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};
#[cfg(test)]
use crate::encoding::v1::values::index_v2::decode_operation_record;
use crate::encoding::v1::values::index_v2::{
    decode_index_record, decode_metadata_value, decode_work_value, encode_metadata_value,
    encode_work_value, IndexV2WorkValue,
};
use crate::error::{HelixDbError, Result};

use super::work::{TextUploadIntentValue, VectorPartitionMappingValue, VectorTenantPartition};
#[cfg(test)]
use super::BlobGcRunId;
use super::{
    ActiveIndexHandle, IndexGenerationId, IndexId, IndexIdentity, IndexOperationId,
    IndexOperationRecord, IndexRecordV2, IndexStorageVersion, IndexV2MetadataValue,
    LoadedV2ScopeCatalog, LogicalIndexIdWatermark, TextUploadIntentId, VectorPhysicalIdWatermark,
    VectorPhysicalIndexId, VectorPhysicalLayout,
};

const UUID_ALLOCATION_ATTEMPTS: usize = 16;

fn global_key(key: GlobalIndexV2Key) -> Bytes {
    Key::Global {
        kind: GlobalKeyKind::IndexV2(key),
    }
    .to_bytes()
}

fn metadata_or_migration_required(
    value: &[u8],
    role: &'static str,
) -> Result<IndexV2MetadataValue> {
    decode_metadata_value(value).map_err(|error| HelixDbError::MigrationRequired {
        reason: format!("malformed V2 {role}: {error}"),
    })
}

/// Initializes an empty writer or validates its complete V2 bootstrap tuple.
pub(crate) async fn bootstrap_writer(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let marker_key = global_key(GlobalIndexV2Key::StorageVersion);
    let logical_key = global_key(GlobalIndexV2Key::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark);
    let marker = transaction.get(&marker_key).await?;
    let logical = transaction.get(&logical_key).await?;
    let vector = transaction.get(&vector_key).await?;

    let Some(marker) = marker else {
        if logical.is_some() || vector.is_some() {
            return Err(HelixDbError::MigrationRequired {
                reason: "V2 storage bootstrap is partial".to_string(),
            });
        }
        let mut rows = transaction.scan(..).await?;
        if rows.next().await?.is_some() {
            return Err(HelixDbError::MigrationRequired {
                reason: "non-empty storage has no V2 marker".to_string(),
            });
        }
        transaction.put(
            marker_key,
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::CURRENT,
            )),
        )?;
        transaction.put(
            logical_key,
            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::initial(),
                },
            )),
        )?;
        transaction.put(
            vector_key,
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                },
            )),
        )?;
        transaction.commit().await?;
        return Ok(());
    };

    validate_bootstrap_values(&marker, logical.as_deref(), vector.as_deref())
}

/// Validates V2 bootstrap without ever initializing read-only storage.
pub(crate) async fn require_reader_bootstrap(reader: &(impl DbReadOps + Sync)) -> Result<()> {
    let marker_key = global_key(GlobalIndexV2Key::StorageVersion);
    let logical_key = global_key(GlobalIndexV2Key::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark);
    let Some(marker) = reader.get(marker_key).await? else {
        return Err(HelixDbError::MigrationRequired {
            reason: "read-only storage has no V2 marker".to_string(),
        });
    };
    let logical = reader.get(logical_key).await?;
    let vector = reader.get(vector_key).await?;
    validate_bootstrap_values(&marker, logical.as_deref(), vector.as_deref())
}

/// Stages a real write of the validated storage marker as a writer-fence barrier.
///
/// A read-only SlateDB transaction may commit without exercising the writer
/// fence. Rewriting the exact canonical marker bytes preserves the persisted
/// format while requiring commit to prove that no newer writer work committed
/// after this transaction's snapshot. Callers must commit the same transaction
/// before treating an absence observed by it as authoritative.
pub(crate) async fn stage_writer_continuity_barrier(transaction: &DbTransaction) -> Result<()> {
    let marker_key = global_key(GlobalIndexV2Key::StorageVersion);
    let logical_key = global_key(GlobalIndexV2Key::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark);
    let Some(marker) = transaction.get(&marker_key).await? else {
        return Err(HelixDbError::MigrationRequired {
            reason: "writer continuity barrier found no V2 storage marker".to_string(),
        });
    };
    let logical = transaction.get(logical_key).await?;
    let vector = transaction.get(vector_key).await?;
    validate_bootstrap_values(&marker, logical.as_deref(), vector.as_deref())?;
    transaction.put(marker_key, marker)?;
    Ok(())
}

fn validate_bootstrap_values(
    marker: &[u8],
    logical: Option<&[u8]>,
    vector: Option<&[u8]>,
) -> Result<()> {
    let IndexV2MetadataValue::StorageVersion(version) =
        metadata_or_migration_required(marker, "storage marker")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage marker contains the wrong value kind".to_string(),
        });
    };
    if version < IndexStorageVersion::CURRENT {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "index storage version {} predates required version {}",
                version.get(),
                IndexStorageVersion::CURRENT.get()
            ),
        });
    }
    if version > IndexStorageVersion::CURRENT {
        return Err(HelixDbError::UnsupportedIndexStorageVersion {
            found: version.get(),
            supported: IndexStorageVersion::CURRENT.get(),
        });
    }
    let Some(logical) = logical else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 logical index watermark is missing".to_string(),
        });
    };
    let Some(vector) = vector else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 vector physical watermark is missing".to_string(),
        });
    };
    if !matches!(
        metadata_or_migration_required(logical, "logical index watermark")?,
        IndexV2MetadataValue::LogicalIndexIdWatermark(_)
    ) || !matches!(
        metadata_or_migration_required(vector, "vector physical watermark")?,
        IndexV2MetadataValue::VectorPhysicalIdWatermark(_)
    ) {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 allocator record contains the wrong value kind".to_string(),
        });
    }
    Ok(())
}

/// Loads and key/value-cross-validates every canonical record for one scope.
pub(crate) async fn load_scope_catalog(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    configured: RuntimeIndexCatalog,
) -> Result<LoadedV2ScopeCatalog> {
    let logical_prefix = IndexV2Key::logical_prefix(IndexV2RecordKind::IndexRecord);
    let physical_prefix = Key::data_prefix(scope, logical_prefix);
    let mut rows = reader.scan_prefix(&physical_prefix, ..).await?;
    let mut loaded = LoadedV2ScopeCatalog::new(scope, configured);
    while let Some(row) = rows.next().await? {
        let parsed = Key::parse_from_slice(scope, &row.key)?;
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::IndexRecord(key)),
            ..
        } = parsed
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "index-record prefix yielded a different typed key".to_string(),
            ));
        };
        let record = decode_index_record(&row.value)?;
        if key.identity != *record.identity() {
            return Err(HelixDbError::IndexCatalogCorruption(
                "canonical index key identity differs from its value".to_string(),
            ));
        }
        loaded.insert_active(&record)?;
    }
    Ok(loaded)
}

/// Point-loads one canonical identity through the caller's stable view.
///
/// Keeping absence distinct from a present non-Active record lets secondary
/// serving retain configured legacy indexes while failing closed for a V2
/// identity that is building, aborting, dropping, or dropped.
pub(crate) async fn load_index_record(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    identity: &IndexIdentity,
) -> Result<Option<IndexRecordV2>> {
    let key = Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(IndexV2Key::index_record(identity.clone())),
    }
    .to_bytes();
    let Some(value) = reader.get(key).await? else {
        return Ok(None);
    };
    let record = decode_index_record(&value)?;
    if record.identity() != identity {
        return Err(HelixDbError::IndexCatalogCorruption(
            "canonical index point-read returned a different logical identity".to_string(),
        ));
    }
    Ok(Some(record))
}

/// Point-loads one canonical identity and projects only exact active state.
///
/// Request paths use this boundary through their stable SlateDB view so worker
/// activation and DDL retirement cannot be hidden behind a stale process-local
/// catalog snapshot. Non-active and absent records deliberately return `None`.
pub(crate) async fn load_active_handle(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    identity: &IndexIdentity,
) -> Result<Option<ActiveIndexHandle>> {
    Ok(load_index_record(reader, scope, identity)
        .await?
        .as_ref()
        .and_then(|record| ActiveIndexHandle::try_from_record(scope, record)))
}

/// Re-reads the canonical record and rejects a stale physical authorization.
pub(crate) async fn revalidate_active_handle(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
) -> Result<()> {
    revalidate_active_handle_row(reader, handle)
        .await
        .map(|_| ())
}

/// Re-reads one exact Active record and returns its canonical serialized row.
///
/// Bounded mutation preflight uses the returned key/value bytes for exact input
/// accounting. Callers that need only stale-generation validation should use
/// [`revalidate_active_handle`].
pub(crate) async fn revalidate_active_handle_row(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
) -> Result<(Bytes, Bytes)> {
    let logical = IndexV2Key::index_record(handle.identity().clone());
    let key = Key::Data {
        scope: handle.scope(),
        kind: DataKeyKind::IndexV2(logical),
    }
    .to_bytes();
    let Some(value) = reader.get(&key).await? else {
        return Err(stale_generation(handle));
    };
    let record = decode_index_record(&value)?;
    if !handle.matches_record(handle.scope(), &record) {
        return Err(stale_generation(handle));
    }
    Ok((key, value))
}

/// Resolves one global operation pointer and cross-checks its scoped record.
///
/// A missing global pointer means there is no runnable operation. Once a
/// pointer exists, a missing or disagreeing scoped record is catalog
/// corruption rather than stale work that a caller may silently ignore.
#[cfg(test)]
pub(crate) async fn load_operation_from_pointer(
    reader: &(impl DbReadOps + Sync),
    operation_id: IndexOperationId,
) -> Result<Option<IndexOperationRecord>> {
    let pointer_key = global_key(GlobalIndexV2Key::OperationPointer(operation_id));
    let Some(pointer_value) = reader.get(pointer_key).await? else {
        return Ok(None);
    };
    let IndexV2MetadataValue::OperationQueuePointer(pointer) =
        decode_metadata_value(&pointer_value)?
    else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "operation pointer key contains the wrong V2 value kind".to_string(),
        ));
    };
    let operation_key = Key::Data {
        scope: pointer.scope,
        kind: DataKeyKind::IndexV2(IndexV2Key::operation(operation_id)),
    }
    .to_bytes();
    let Some(operation_value) = reader.get(operation_key).await? else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "operation pointer has no scoped operation record".to_string(),
        ));
    };
    let operation = decode_operation_record(&operation_value)?;
    if operation.operation_id() != operation_id
        || operation.index_id() != pointer.index_id
        || operation.generation() != pointer.generation
        || operation.operation_revision() != pointer.record_revision
    {
        return Err(HelixDbError::IndexCatalogCorruption(
            "operation pointer disagrees with its scoped operation record".to_string(),
        ));
    }
    if !operation_record_cursors_are_valid(pointer.scope, &operation) {
        return Err(HelixDbError::IndexCatalogCorruption(
            "operation record contains a cursor outside its typed V2 scope".to_string(),
        ));
    }
    Ok(Some(operation))
}

fn complete_cursor_is_valid(scope: DataScope, cursor: &[u8]) -> bool {
    const SENTINEL_OFFSET: usize = 0;
    let is_global = cursor.len() >= GLOBAL_INDEX_V2_SENTINEL.len()
        && cursor[SENTINEL_OFFSET..SENTINEL_OFFSET + GLOBAL_INDEX_V2_SENTINEL.len()]
            == GLOBAL_INDEX_V2_SENTINEL;
    if is_global {
        GlobalIndexV2Key::parse_from_slice(cursor).is_ok()
    } else {
        Key::parse_from_slice(scope, cursor).is_ok()
    }
}

/// Validates every persisted operation cursor against an exact V1 scoped or
/// global key parser before a lifecycle transaction accepts it.
pub(super) fn operation_cursors_are_valid(
    scope: DataScope,
    progress: &super::IndexOperationProgress,
) -> bool {
    progress.cursors_are_valid(|cursor| complete_cursor_is_valid(scope, cursor.as_bytes()))
}

/// Validates generic resume keys plus the exact owner-bound text artifact key.
pub(super) fn operation_record_cursors_are_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> bool {
    let super::IndexOperationProgress::TextBuild(super::TextBuildProgress::Constructing(stage)) =
        operation.progress()
    else {
        return operation_cursors_are_valid(scope, operation.progress());
    };
    if let super::TextBuildStage::ValidateManifests(progress) = stage {
        let (cursor, lane) = match progress {
            super::TextManifestValidationProgress::Pages(progress) => {
                (progress.cursor(), IndexV2RecordKind::TextManifestPage)
            }
            super::TextManifestValidationProgress::Roots(progress) => (
                progress.cursor.as_ref(),
                IndexV2RecordKind::TextManifestRoot,
            ),
            super::TextManifestValidationProgress::UploadIntents(progress) => (
                progress.cursor.as_ref(),
                IndexV2RecordKind::TextUploadIntent,
            ),
        };
        let Some(cursor) = cursor else {
            return true;
        };
        let Ok(Key::Data {
            kind: DataKeyKind::IndexV2(key),
            ..
        }) = Key::parse_from_slice(scope, cursor.as_bytes())
        else {
            return false;
        };
        let (kind, index_id, generation, partition, page) = match key {
            IndexV2Key::TextManifestPage(key) => (
                IndexV2RecordKind::TextManifestPage,
                key.root.index_id,
                key.root.generation,
                Some(key.root.partition),
                Some(key.page),
            ),
            IndexV2Key::TextManifestRoot(key) => (
                IndexV2RecordKind::TextManifestRoot,
                key.index_id,
                key.generation,
                Some(key.partition),
                None,
            ),
            IndexV2Key::TextUploadIntent(key) => (
                IndexV2RecordKind::TextUploadIntent,
                key.index_id,
                key.generation,
                None,
                None,
            ),
            IndexV2Key::IndexRecord(_)
            | IndexV2Key::Operation(_)
            | IndexV2Key::BuildDelta(_)
            | IndexV2Key::AppliedState(_)
            | IndexV2Key::SecondaryEntry(_)
            | IndexV2Key::TextBuildArtifact(_)
            | IndexV2Key::BlobGcCandidate(_)
            | IndexV2Key::TextEntityState(_)
            | IndexV2Key::ActiveMutationCommitProof(_)
            | IndexV2Key::VectorPartitionMapping(_) => return false,
        };
        let partition_matches = match progress {
            super::TextManifestValidationProgress::Pages(progress) => {
                progress.partition().is_none_or(|expected| {
                    partition
                        .is_some_and(|actual| actual.as_bytes() == expected.partition_fingerprint())
                        && page.is_some_and(|actual| {
                            actual.checked_add(1) == Some(expected.next_page())
                        })
                })
            }
            super::TextManifestValidationProgress::Roots(_)
            | super::TextManifestValidationProgress::UploadIntents(_) => true,
        };
        return kind == lane
            && index_id == operation.index_id()
            && generation == operation.generation()
            && partition_matches;
    }
    if !operation_cursors_are_valid(scope, operation.progress()) {
        return false;
    }
    let (artifact_cursor, delta_cursor, compaction_inputs) = match stage {
        super::TextBuildStage::AwaitUpload(progress) => (progress.artifact_key(), None, None),
        super::TextBuildStage::AwaitCatchUpUpload(progress) => {
            (progress.artifact_key(), Some(progress.delta_key()), None)
        }
        super::TextBuildStage::AwaitCompactionUpload(progress) => (
            progress.artifact_key(),
            None,
            Some(progress.input_artifact_keys()),
        ),
        super::TextBuildStage::PrepareManifests(progress) => {
            let Some(cursor) = progress.cursor.as_ref() else {
                return true;
            };
            (cursor, None, None)
        }
        super::TextBuildStage::ScanSource(_)
        | super::TextBuildStage::ScanPartitions(_)
        | super::TextBuildStage::CatchUp(_)
        | super::TextBuildStage::Compact(_)
        | super::TextBuildStage::ValidateManifests(_)
        | super::TextBuildStage::Activate(_) => return true,
    };
    let Ok(Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(artifact)),
        ..
    }) = Key::parse_from_slice(scope, artifact_cursor.as_bytes())
    else {
        return false;
    };
    if artifact.root.index_id != operation.index_id()
        || artifact.root.generation != operation.generation()
    {
        return false;
    }
    if let Some(compaction_inputs) = compaction_inputs {
        for input in compaction_inputs {
            let Ok(Key::Data {
                kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(input_artifact)),
                ..
            }) = Key::parse_from_slice(scope, input.as_bytes())
            else {
                return false;
            };
            if input_artifact.root.index_id != operation.index_id()
                || input_artifact.root.generation != operation.generation()
                || input_artifact.root.partition != artifact.root.partition
                || input_artifact == artifact
            {
                return false;
            }
        }
    }
    let Some(delta_cursor) = delta_cursor else {
        return true;
    };
    let Ok(Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::BuildDelta(delta)),
        ..
    }) = Key::parse_from_slice(scope, delta_cursor.as_bytes())
    else {
        return false;
    };
    delta.index_id == operation.index_id() && delta.generation == operation.generation()
}

/// Resolves one global upload pointer and cross-checks its scoped intent.
///
/// The pointer owns the scope, index, generation, and expected intent revision,
/// so a worker never guesses tenant ownership or scans scopes to find an ID.
pub(crate) async fn load_upload_from_pointer(
    reader: &(impl DbReadOps + Sync),
    intent_id: TextUploadIntentId,
) -> Result<Option<TextUploadIntentValue>> {
    let pointer_key = global_key(GlobalIndexV2Key::UploadPointer(intent_id));
    let Some(pointer_value) = reader.get(pointer_key).await? else {
        return Ok(None);
    };
    let IndexV2MetadataValue::UploadQueuePointer(pointer) = decode_metadata_value(&pointer_value)?
    else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "upload pointer key contains the wrong V2 value kind".to_string(),
        ));
    };
    let intent_key = Key::Data {
        scope: pointer.scope,
        kind: DataKeyKind::IndexV2(IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id: pointer.index_id,
            generation: pointer.generation,
            intent_id,
        })),
    }
    .to_bytes();
    let Some(intent_value) = reader.get(intent_key).await? else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "upload pointer has no scoped text intent".to_string(),
        ));
    };
    let IndexV2WorkValue::TextUploadIntent(intent) = decode_work_value(&intent_value)? else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "upload pointer key resolves to the wrong V2 value kind".to_string(),
        ));
    };
    if intent.intent_id != intent_id
        || intent.index_id != pointer.index_id
        || intent.generation != pointer.generation
        || intent.revision != pointer.record_revision
    {
        return Err(HelixDbError::IndexCatalogCorruption(
            "upload pointer disagrees with its scoped text intent".to_string(),
        ));
    }
    Ok(Some(*intent))
}

fn stale_generation(handle: &ActiveIndexHandle) -> HelixDbError {
    HelixDbError::StaleIndexGeneration {
        index_id: handle.index_id().get(),
        generation: handle.generation().get(),
        record_revision: handle.record_revision().get(),
    }
}

/// Reserves the current logical ID and advances its watermark in `transaction`.
pub(crate) async fn allocate_index_id(transaction: &DbTransaction) -> Result<IndexId> {
    let key = global_key(GlobalIndexV2Key::LogicalIndexIdWatermark);
    let Some(value) = transaction.get(&key).await? else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 logical index watermark is missing".to_string(),
        });
    };
    let IndexV2MetadataValue::LogicalIndexIdWatermark(watermark) =
        metadata_or_migration_required(&value, "logical index watermark")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 logical index watermark contains the wrong value kind".to_string(),
        });
    };
    if watermark.next_id.get() == u64::MAX {
        return Err(HelixDbError::IdentifierExhausted("logical index ID"));
    }
    let allocated = watermark.next_id;
    let next_id = allocated.checked_next()?;
    transaction.put(
        key,
        encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
            LogicalIndexIdWatermark { next_id },
        )),
    )?;
    Ok(allocated)
}

/// Reserves the current vector physical ID and advances its watermark.
pub(crate) async fn allocate_vector_physical_id(
    transaction: &DbTransaction,
) -> Result<VectorPhysicalIndexId> {
    let allocated = peek_vector_physical_id(transaction).await?;
    let key = global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark);
    let next_id = allocated.checked_next()?;
    transaction.put(
        key,
        encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
            VectorPhysicalIdWatermark { next_id },
        )),
    )?;
    Ok(allocated)
}

/// Reads the exact physical ID that the caller's transaction can next reserve.
///
/// Vector builders use this non-mutating preview in a disposable HNSW planning
/// transaction. Only after the complete write set passes admission does the
/// lifecycle transaction call [`allocate_vector_physical_id`] and assert that
/// it received this same ID. Serializable conflict tracking prevents a
/// concurrent allocator from invalidating that proof silently.
pub(crate) async fn peek_vector_physical_id(
    reader: &(impl DbReadOps + Sync),
) -> Result<VectorPhysicalIndexId> {
    let key = global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark);
    let Some(value) = reader.get(&key).await? else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 vector physical watermark is missing".to_string(),
        });
    };
    let IndexV2MetadataValue::VectorPhysicalIdWatermark(watermark) =
        metadata_or_migration_required(&value, "vector physical watermark")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 vector physical watermark contains the wrong value kind".to_string(),
        });
    };
    if watermark.next_id.get() == u64::MAX {
        return Err(HelixDbError::IdentifierExhausted(
            "vector physical index ID",
        ));
    }
    Ok(watermark.next_id)
}

/// Resolves one exact tenant partition through a validated partitioned layout.
///
/// Reads never allocate. The key fingerprint and every repeated value field are
/// cross-checked before the physical ID can authorize HNSW access.
pub(crate) async fn load_vector_partition_mapping(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    layout: VectorPhysicalLayout,
    partition: &VectorTenantPartition,
) -> Result<Option<VectorPhysicalIndexId>> {
    if layout != VectorPhysicalLayout::Partitioned {
        return Err(HelixDbError::IndexCatalogCorruption(
            "vector partition mapping requested for an unpartitioned generation".to_string(),
        ));
    }
    let logical = IndexV2Key::VectorPartitionMapping(VectorPartitionMappingKey {
        index_id,
        generation,
        partition: partition.fingerprint(),
    });
    let key = Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(logical),
    }
    .to_bytes();
    let Some(value) = reader.get(key).await? else {
        return Ok(None);
    };
    let IndexV2WorkValue::VectorPartitionMapping(mapping) = decode_work_value(&value)? else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "vector partition mapping key contains another V2 value kind".to_string(),
        ));
    };
    if mapping.index_id != index_id
        || mapping.generation != generation
        || &mapping.partition != partition
        || mapping.partition.fingerprint() != partition.fingerprint()
    {
        return Err(HelixDbError::IndexCatalogCorruption(
            "vector partition mapping key and value disagree".to_string(),
        ));
    }
    Ok(Some(mapping.physical_index_id))
}

/// Resolves or atomically creates one tenant partition mapping.
///
/// The mapping and physical-ID watermark are staged in the caller's graph or
/// builder transaction. Concurrent first writers therefore conflict and retry
/// instead of publishing two physical namespaces for one partition.
pub(crate) async fn stage_vector_partition_mapping(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    layout: VectorPhysicalLayout,
    partition: &VectorTenantPartition,
) -> Result<VectorPhysicalIndexId> {
    match load_vector_partition_mapping(transaction, scope, index_id, generation, layout, partition)
        .await?
    {
        Some(physical_index_id) => Ok(physical_index_id),
        None => {
            let physical_index_id = allocate_vector_physical_id(transaction).await?;
            let logical = IndexV2Key::VectorPartitionMapping(VectorPartitionMappingKey {
                index_id,
                generation,
                partition: partition.fingerprint(),
            });
            let key = Key::Data {
                scope,
                kind: DataKeyKind::IndexV2(logical),
            }
            .to_bytes();
            transaction.put(
                key,
                encode_work_value(&IndexV2WorkValue::VectorPartitionMapping(
                    VectorPartitionMappingValue {
                        index_id,
                        generation,
                        partition: partition.clone(),
                        physical_index_id,
                    },
                )),
            )?;
            Ok(physical_index_id)
        }
    }
}

/// Finds an unused operation ID without writing outside the caller's transaction.
pub(crate) async fn allocate_operation_id(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<IndexOperationId> {
    allocate_operation_id_from(
        transaction,
        scope,
        std::iter::repeat_with(IndexOperationId::new_v4).take(UUID_ALLOCATION_ATTEMPTS),
        UUID_ALLOCATION_ATTEMPTS,
    )
    .await
}

async fn allocate_operation_id_from(
    transaction: &DbTransaction,
    scope: DataScope,
    candidates: impl IntoIterator<Item = IndexOperationId>,
    attempts: usize,
) -> Result<IndexOperationId> {
    for candidate in candidates {
        let scoped = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::operation(candidate)),
        }
        .to_bytes();
        let pointer = global_key(GlobalIndexV2Key::OperationPointer(candidate));
        if transaction.get(scoped).await?.is_none() && transaction.get(pointer).await?.is_none() {
            return Ok(candidate);
        }
    }
    Err(HelixDbError::IdentifierAllocationFailed {
        kind: "index operation ID",
        attempts,
    })
}

#[cfg(test)]
async fn allocate_text_upload_intent_id_from(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    candidates: impl IntoIterator<Item = TextUploadIntentId>,
    attempts: usize,
) -> Result<TextUploadIntentId> {
    for candidate in candidates {
        let scoped = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                index_id,
                generation,
                intent_id: candidate,
            })),
        }
        .to_bytes();
        let pointer = global_key(GlobalIndexV2Key::UploadPointer(candidate));
        if transaction.get(scoped).await?.is_none() && transaction.get(pointer).await?.is_none() {
            return Ok(candidate);
        }
    }
    Err(HelixDbError::IdentifierAllocationFailed {
        kind: "text upload intent ID",
        attempts,
    })
}

#[cfg(test)]
async fn allocate_blob_gc_run_id_from(
    transaction: &DbTransaction,
    candidates: impl IntoIterator<Item = BlobGcRunId>,
    attempts: usize,
) -> Result<BlobGcRunId> {
    for candidate in candidates {
        let root = global_key(GlobalIndexV2Key::BlobGcRunRoot(candidate));
        if transaction.get(root).await?.is_some() {
            continue;
        }
        let mut mark_prefix =
            GlobalIndexV2Key::logical_prefix(GlobalIndexV2Kind::BlobGcReachabilityMark).to_vec();
        mark_prefix.extend_from_slice(candidate.as_bytes());
        if transaction
            .scan_prefix(Bytes::from(mark_prefix), ..)
            .await?
            .next()
            .await?
            .is_some()
        {
            continue;
        }
        let mut member_prefix =
            GlobalIndexV2Key::logical_prefix(GlobalIndexV2Kind::BlobGcCandidateMember).to_vec();
        member_prefix.extend_from_slice(candidate.as_bytes());
        if transaction
            .scan_prefix(Bytes::from(member_prefix), ..)
            .await?
            .next()
            .await?
            .is_none()
        {
            return Ok(candidate);
        }
    }
    Err(HelixDbError::IdentifierAllocationFailed {
        kind: "blob GC run ID",
        attempts,
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use slatedb::object_store::{memory::InMemory, ObjectStore};

    use crate::config::SecondaryIndexDefinition;
    use crate::encoding::v1::keys::index_v2::{
        BlobGcPass, BlobHash, IndexEntity, IndexEntityStateKey, TextBuildArtifactKey,
        TextIntentOwnedKey, TextManifestPageKey, TextManifestRootKey,
    };
    use crate::encoding::v1::keys::tenant::TenantId;
    use crate::encoding::v1::values::index_v2::{
        encode_index_record, encode_operation_record, encode_work_value,
    };
    use crate::index_v2::work::{
        BlobRef, SplitRef, TextUploadAttachment, TextUploadOwner, TextUploadPhase,
        TextUploadWorkState,
    };
    use crate::index_v2::{
        BlobPublicationPermitId, IndexComponent, IndexElementKind, IndexEntityId,
        IndexGenerationId, IndexIdentity, IndexIdentityFamily, IndexOperationExecutionState,
        IndexOperationFamily, IndexOperationId, IndexOperationKind, IndexOperationProgress,
        IndexOperationRevision, IndexRecordV2, IndexRevision, IndexStateTransition,
        NoCursorProgress, OperationCounters, PhysicalGeneration, PrefixScanProgress,
        SecondaryBuildProgress, SecondaryBuildStage, SourceScanProgress, TextBuildProgress,
        TextBuildStage, TextBuildUploadProgress, TextCatchUpUploadProgress,
        TextCompactionUploadProgress, TextIntentRevision, TextManifestPageValidationProgress,
        TextManifestPartitionValidation, TextManifestRevision, TextManifestValidationProgress,
        TextPartition, ValidatedDynamicIndexDefinition,
    };

    use super::*;

    async fn raw_db(name: &str) -> Db {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        Db::open(name, store).await.unwrap()
    }

    async fn put_bootstrap_tuple(db: &Db, version: u16) {
        db.put(
            global_key(GlobalIndexV2Key::StorageVersion),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(version).unwrap(),
            )),
        )
        .await
        .unwrap();
        db.put(
            global_key(GlobalIndexV2Key::LogicalIndexIdWatermark),
            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
        db.put(
            global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
    }

    fn active_secondary(index_id: u64) -> IndexRecordV2 {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        IndexRecordV2::building(
            IndexId::new(index_id).unwrap(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap()
    }

    fn queued_secondary_operation(operation_id: IndexOperationId) -> IndexOperationRecord {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        IndexOperationRecord::try_new(
            operation_id,
            IndexId::initial(),
            definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Activate(NoCursorProgress::default()),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap()
    }

    fn queued_text_intent(intent_id: TextUploadIntentId) -> TextUploadIntentValue {
        let split = SplitRef::try_new(BlobRef::new([13; 32], 100), 80, 20, 10, 100).unwrap();
        TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            IndexId::initial(),
            IndexIdentity::new(
                IndexIdentityFamily::Text,
                IndexElementKind::Node,
                IndexComponent::try_new("label", "Doc").unwrap(),
                IndexComponent::try_new("property", "body").unwrap(),
            ),
            IndexGenerationId::initial(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([14; 16]).unwrap(),
            TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([15; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
            0,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap()
    }

    async fn put_record(db: &Db, scope: DataScope, record: &IndexRecordV2) {
        let key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::index_record(record.identity().clone())),
        }
        .to_bytes();
        db.put(key, encode_index_record(record)).await.unwrap();
    }

    #[tokio::test]
    async fn empty_writer_initializes_exact_marker_and_watermarks_atomically() {
        let db = raw_db("index-v2-bootstrap-empty").await;
        bootstrap_writer(&db).await.unwrap();

        assert_eq!(
            db.get(global_key(GlobalIndexV2Key::StorageVersion))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &[0x01, 0x01, 0x00, 0x02]
        );
        assert_eq!(
            db.get(global_key(GlobalIndexV2Key::LogicalIndexIdWatermark))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &[0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        );
        assert_eq!(
            db.get(global_key(GlobalIndexV2Key::VectorPhysicalIdWatermark))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &[0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        );
        bootstrap_writer(&db).await.unwrap();
        require_reader_bootstrap(&db).await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn writer_continuity_barrier_rewrites_only_the_validated_marker() {
        let db = raw_db("index-v2-writer-continuity-barrier").await;
        bootstrap_writer(&db).await.unwrap();
        let marker_key = global_key(GlobalIndexV2Key::StorageVersion);
        let marker_before = db.get(&marker_key).await.unwrap().unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage_writer_continuity_barrier(&transaction).await.unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(db.get(marker_key).await.unwrap().unwrap(), marker_before);
        db.close().await.unwrap();

        let missing = raw_db("index-v2-writer-continuity-missing-marker").await;
        let transaction = missing
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            stage_writer_continuity_barrier(&transaction).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        drop(transaction);
        missing.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_marker_never_initializes_reader_or_nonempty_writer() {
        let empty = raw_db("index-v2-reader-empty-missing").await;
        assert!(matches!(
            require_reader_bootstrap(&empty).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert!(empty
            .get(global_key(GlobalIndexV2Key::StorageVersion))
            .await
            .unwrap()
            .is_none());
        empty.close().await.unwrap();

        let old_development = raw_db("index-v2-old-development-prefix").await;
        old_development
            .put(
                Bytes::from_static(b"\x06\x01old-development-index-record"),
                Bytes::from_static(b"development"),
            )
            .await
            .unwrap();
        assert!(matches!(
            bootstrap_writer(&old_development).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert!(old_development
            .get(global_key(GlobalIndexV2Key::StorageVersion))
            .await
            .unwrap()
            .is_none());
        old_development.close().await.unwrap();
    }

    #[tokio::test]
    async fn older_newer_malformed_and_partial_bootstraps_fail_closed() {
        let older = raw_db("index-v2-bootstrap-older").await;
        put_bootstrap_tuple(&older, 1).await;
        assert!(matches!(
            bootstrap_writer(&older).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        older.close().await.unwrap();

        let newer = raw_db("index-v2-bootstrap-newer").await;
        put_bootstrap_tuple(&newer, 3).await;
        assert!(matches!(
            require_reader_bootstrap(&newer).await,
            Err(HelixDbError::UnsupportedIndexStorageVersion {
                found: 3,
                supported: 2
            })
        ));
        newer.close().await.unwrap();

        let malformed = raw_db("index-v2-bootstrap-malformed").await;
        put_bootstrap_tuple(&malformed, 2).await;
        malformed
            .put(
                global_key(GlobalIndexV2Key::StorageVersion),
                Bytes::from_static(b"malformed"),
            )
            .await
            .unwrap();
        assert!(matches!(
            bootstrap_writer(&malformed).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        malformed.close().await.unwrap();

        let partial = raw_db("index-v2-bootstrap-partial").await;
        partial
            .put(
                global_key(GlobalIndexV2Key::StorageVersion),
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::CURRENT,
                )),
            )
            .await
            .unwrap();
        assert!(matches!(
            bootstrap_writer(&partial).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        partial.close().await.unwrap();
    }

    #[tokio::test]
    async fn catalog_loads_only_active_records_from_the_exact_scope() {
        let db = raw_db("index-v2-catalog-active-scope").await;
        bootstrap_writer(&db).await.unwrap();
        let active = active_secondary(7);
        put_record(&db, DataScope::LegacyUnscoped, &active).await;

        let tenant = DataScope::Tenant(TenantId::from_u128(42));
        let tenant_record = active_secondary(8);
        put_record(&db, tenant, &tenant_record).await;

        let catalog =
            load_scope_catalog(&db, DataScope::LegacyUnscoped, RuntimeIndexCatalog::new())
                .await
                .unwrap();
        assert_eq!(catalog.active_handles().count(), 1);
        assert_eq!(
            catalog.handle(active.identity()).unwrap().index_id(),
            active.index_id()
        );
        let key = crate::config::scoped_secondary_index_property("User", "email");
        assert!(catalog.runtime().contains_node_equality_scoped(&key));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn active_handle_revalidation_detects_revision_and_state_changes() {
        let db = raw_db("index-v2-handle-revalidation").await;
        bootstrap_writer(&db).await.unwrap();
        let active = active_secondary(9);
        put_record(&db, DataScope::LegacyUnscoped, &active).await;
        let handle =
            ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active).unwrap();
        revalidate_active_handle(&db, &handle).await.unwrap();

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::new_v4(),
            })
            .unwrap();
        put_record(&db, DataScope::LegacyUnscoped, &dropping).await;
        assert!(matches!(
            revalidate_active_handle(&db, &handle).await,
            Err(HelixDbError::StaleIndexGeneration { .. })
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn active_handle_point_load_observes_state_through_each_read_view() {
        let db = raw_db("index-v2-active-handle-point-load").await;
        bootstrap_writer(&db).await.unwrap();
        let active = active_secondary(91);
        put_record(&db, DataScope::LegacyUnscoped, &active).await;

        let snapshot = db.snapshot().await.unwrap();
        let loaded = load_active_handle(
            snapshot.as_ref(),
            DataScope::LegacyUnscoped,
            active.identity(),
        )
        .await
        .unwrap()
        .expect("active record projects through the snapshot");
        assert_eq!(loaded.index_id(), active.index_id());

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::new_v4(),
            })
            .unwrap();
        put_record(&db, DataScope::LegacyUnscoped, &dropping).await;
        assert!(
            load_active_handle(&db, DataScope::LegacyUnscoped, active.identity())
                .await
                .unwrap()
                .is_none()
        );
        assert!(load_active_handle(
            snapshot.as_ref(),
            DataScope::LegacyUnscoped,
            active.identity(),
        )
        .await
        .unwrap()
        .is_some());
        drop(snapshot);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn active_handle_read_conflicts_with_concurrent_ddl_record_change() {
        let db = raw_db("index-v2-handle-serializable-conflict").await;
        bootstrap_writer(&db).await.unwrap();
        let active = active_secondary(10);
        put_record(&db, DataScope::LegacyUnscoped, &active).await;
        let handle =
            ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active).unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        revalidate_active_handle(&transaction, &handle)
            .await
            .unwrap();

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::from_bytes([10; 16]).unwrap(),
            })
            .unwrap();
        put_record(&db, DataScope::LegacyUnscoped, &dropping).await;

        let tenant_record = active_secondary(11);
        transaction
            .put(
                Key::Data {
                    scope: DataScope::Tenant(TenantId::from_u128(11)),
                    kind: DataKeyKind::IndexV2(IndexV2Key::index_record(
                        tenant_record.identity().clone(),
                    )),
                }
                .to_bytes(),
                encode_index_record(&tenant_record),
            )
            .unwrap();
        assert!(transaction.commit().await.is_err());
        db.close().await.unwrap();
    }

    #[test]
    fn complete_operation_cursors_require_an_exact_global_or_scoped_key() {
        let unscoped_record = active_secondary(12);
        let unscoped = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::IndexV2(IndexV2Key::index_record(
                unscoped_record.identity().clone(),
            )),
        }
        .to_bytes();
        assert!(complete_cursor_is_valid(
            DataScope::LegacyUnscoped,
            &unscoped
        ));

        let all_fe_tenant = DataScope::Tenant(TenantId::from_u128(u128::from_be_bytes([0xFE; 16])));
        let tenant_record = active_secondary(13);
        let tenant = Key::Data {
            scope: all_fe_tenant,
            kind: DataKeyKind::IndexV2(IndexV2Key::index_record(tenant_record.identity().clone())),
        }
        .to_bytes();
        assert!(complete_cursor_is_valid(all_fe_tenant, &tenant));
        assert!(!complete_cursor_is_valid(
            DataScope::Tenant(TenantId::from_u128(42)),
            &tenant
        ));

        let global = global_key(GlobalIndexV2Key::StorageVersion);
        assert!(complete_cursor_is_valid(all_fe_tenant, &global));
        let mut trailing_global = global.to_vec();
        trailing_global.push(0);
        assert!(!complete_cursor_is_valid(all_fe_tenant, &trailing_global));
    }

    #[test]
    fn awaiting_text_upload_requires_its_exact_scoped_artifact_owner() {
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let operation_cursor = |byte| {
            super::super::IndexCursor::try_new(
                Key::Data {
                    scope,
                    kind: DataKeyKind::IndexV2(IndexV2Key::operation(
                        IndexOperationId::from_bytes([byte; 16]).unwrap(),
                    )),
                }
                .to_bytes(),
            )
            .unwrap()
        };
        let operation = |artifact_key| {
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x41; 16]).unwrap(),
                index_id,
                IndexIdentity::new(
                    IndexIdentityFamily::Text,
                    IndexElementKind::Node,
                    IndexComponent::try_new("label", "Doc").unwrap(),
                    IndexComponent::try_new("property", "body").unwrap(),
                ),
                generation,
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Build,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::AwaitUpload(
                        TextBuildUploadProgress::try_new(
                            SourceScanProgress {
                                inclusive_upper_bound: operation_cursor(0x50),
                                cursor: None,
                                counters: OperationCounters::default(),
                            },
                            operation_cursor(0x40),
                            OperationCounters {
                                entities: 1,
                                input_bytes: 1,
                                output_operations: 1,
                                output_bytes: 1,
                            },
                            super::super::IndexCursor::try_new(artifact_key).unwrap(),
                            TextUploadIntentId::from_bytes([0x42; 16]).unwrap(),
                        )
                        .unwrap(),
                    ),
                )),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap()
        };
        let artifact_key = |owner_index_id, owner_generation, owner_scope| {
            Key::Data {
                scope: owner_scope,
                kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
                    root: TextManifestRootKey {
                        index_id: owner_index_id,
                        generation: owner_generation,
                        partition: TextPartition::Unpartitioned.fingerprint(),
                    },
                    ordinal: 0,
                })),
            }
            .to_bytes()
        };

        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(artifact_key(index_id, generation, scope)),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(operation_cursor(0x43).as_bytes().clone()),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(artifact_key(IndexId::new(2).unwrap(), generation, scope)),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(artifact_key(
                index_id,
                IndexGenerationId::new(2).unwrap(),
                scope,
            )),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(artifact_key(
                index_id,
                generation,
                DataScope::Tenant(TenantId::from_u128(7)),
            )),
        ));
    }

    #[test]
    fn awaiting_text_catch_up_upload_requires_its_exact_delta_owner() {
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let identity = IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Doc").unwrap(),
            IndexComponent::try_new("property", "body").unwrap(),
        );
        let artifact_key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
                root: TextManifestRootKey {
                    index_id,
                    generation,
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                ordinal: 0,
            })),
        }
        .to_bytes();
        let delta_key = |delta_index_id, delta_generation, delta_scope| {
            Key::Data {
                scope: delta_scope,
                kind: DataKeyKind::IndexV2(IndexV2Key::BuildDelta(IndexEntityStateKey {
                    index_id: delta_index_id,
                    generation: delta_generation,
                    entity: IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::initial(),
                    },
                })),
            }
            .to_bytes()
        };
        let operation = |delta_key: Bytes| {
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x51; 16]).unwrap(),
                index_id,
                identity.clone(),
                generation,
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Build,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::AwaitCatchUpUpload(
                        TextCatchUpUploadProgress::try_new(
                            PrefixScanProgress {
                                cursor: None,
                                counters: OperationCounters::default(),
                            },
                            super::super::IndexCursor::try_new(delta_key).unwrap(),
                            OperationCounters {
                                entities: 1,
                                input_bytes: 1,
                                output_operations: 2,
                                output_bytes: 1,
                            },
                            super::super::IndexCursor::try_new(artifact_key.clone()).unwrap(),
                            TextUploadIntentId::from_bytes([0x52; 16]).unwrap(),
                        )
                        .unwrap(),
                    ),
                )),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap()
        };

        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(delta_key(index_id, generation, scope)),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(delta_key(IndexId::new(2).unwrap(), generation, scope)),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(delta_key(
                index_id,
                IndexGenerationId::new(2).unwrap(),
                scope,
            )),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(delta_key(
                index_id,
                generation,
                DataScope::Tenant(TenantId::from_u128(8)),
            )),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(
                Key::Data {
                    scope,
                    kind: DataKeyKind::IndexV2(IndexV2Key::operation(
                        IndexOperationId::from_bytes([0x53; 16]).unwrap(),
                    )),
                }
                .to_bytes(),
            ),
        ));
    }

    #[test]
    fn preparing_text_manifests_requires_exact_artifact_cursor() {
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let partition = TextPartition::Unpartitioned.fingerprint();
        let artifact_key = |owner_index_id, owner_generation, owner_scope| {
            Key::Data {
                scope: owner_scope,
                kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
                    root: TextManifestRootKey {
                        index_id: owner_index_id,
                        generation: owner_generation,
                        partition,
                    },
                    ordinal: 0,
                })),
            }
            .to_bytes()
        };
        let operation = |cursor: Option<Bytes>| {
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x59; 16]).unwrap(),
                index_id,
                IndexIdentity::new(
                    IndexIdentityFamily::Text,
                    IndexElementKind::Node,
                    IndexComponent::try_new("label", "Doc").unwrap(),
                    IndexComponent::try_new("property", "body").unwrap(),
                ),
                generation,
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Build,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::PrepareManifests(PrefixScanProgress {
                        cursor: cursor
                            .map(|cursor| super::super::IndexCursor::try_new(cursor).unwrap()),
                        counters: OperationCounters::default(),
                    }),
                )),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap()
        };

        assert!(operation_record_cursors_are_valid(scope, &operation(None)));
        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(Some(artifact_key(index_id, generation, scope))),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(Some(artifact_key(
                IndexId::new(2).unwrap(),
                generation,
                scope,
            ))),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(Some(artifact_key(
                index_id,
                IndexGenerationId::new(2).unwrap(),
                scope,
            ))),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(Some(artifact_key(
                index_id,
                generation,
                DataScope::Tenant(TenantId::from_u128(7)),
            ))),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(Some(
                Key::Data {
                    scope,
                    kind: DataKeyKind::IndexV2(IndexV2Key::operation(
                        IndexOperationId::from_bytes([0x5A; 16]).unwrap(),
                    )),
                }
                .to_bytes(),
            )),
        ));
    }

    #[test]
    fn validating_text_manifests_requires_the_exact_lane_and_owner_cursor() {
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let partition = TextPartition::Unpartitioned.fingerprint();
        let cursor = |key| {
            super::super::IndexCursor::try_new(
                Key::Data {
                    scope,
                    kind: DataKeyKind::IndexV2(key),
                }
                .to_bytes(),
            )
            .unwrap()
        };
        let page_cursor = cursor(IndexV2Key::TextManifestPage(TextManifestPageKey {
            root: TextManifestRootKey {
                index_id,
                generation,
                partition,
            },
            page: 0,
        }));
        let wrong_page_cursor = cursor(IndexV2Key::TextManifestPage(TextManifestPageKey {
            root: TextManifestRootKey {
                index_id,
                generation,
                partition,
            },
            page: 1,
        }));
        let root_cursor = cursor(IndexV2Key::TextManifestRoot(TextManifestRootKey {
            index_id,
            generation,
            partition,
        }));
        let intent_cursor = cursor(IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id,
            generation,
            intent_id: TextUploadIntentId::from_bytes([0x61; 16]).unwrap(),
        }));
        let forbidden_cursor = cursor(IndexV2Key::operation(
            IndexOperationId::from_bytes([0x62; 16]).unwrap(),
        ));
        let malformed_cursor =
            super::super::IndexCursor::try_new(Bytes::from_static(b"not-a-v1-key")).unwrap();
        let operation = |progress| {
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x60; 16]).unwrap(),
                index_id,
                IndexIdentity::new(
                    IndexIdentityFamily::Text,
                    IndexElementKind::Node,
                    IndexComponent::try_new("label", "Doc").unwrap(),
                    IndexComponent::try_new("property", "body").unwrap(),
                ),
                generation,
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Build,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(progress),
                )),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap()
        };
        let pages = |cursor, fingerprint| {
            TextManifestValidationProgress::Pages(
                TextManifestPageValidationProgress::try_new(
                    cursor,
                    Some(
                        TextManifestPartitionValidation::try_new(
                            fingerprint,
                            TextManifestRevision::new(3).unwrap(),
                            2,
                            2,
                            1,
                            1,
                        )
                        .unwrap(),
                    ),
                    OperationCounters::default(),
                )
                .unwrap(),
            )
        };

        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(TextManifestValidationProgress::initial(
                OperationCounters::default()
            )),
        ));
        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(pages(Some(page_cursor.clone()), *partition.as_bytes())),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(pages(Some(page_cursor.clone()), [0xFF; 32])),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(pages(Some(root_cursor.clone()), *partition.as_bytes())),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(pages(Some(wrong_page_cursor), *partition.as_bytes())),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(pages(Some(forbidden_cursor), *partition.as_bytes())),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(pages(Some(malformed_cursor), *partition.as_bytes())),
        ));
        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: Some(root_cursor.clone()),
                counters: OperationCounters::default(),
            })),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: Some(intent_cursor.clone()),
                counters: OperationCounters::default(),
            })),
        ));
        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(TextManifestValidationProgress::UploadIntents(
                PrefixScanProgress {
                    cursor: Some(intent_cursor),
                    counters: OperationCounters::default(),
                }
            )),
        ));
        let foreign_root = cursor(IndexV2Key::TextManifestRoot(TextManifestRootKey {
            index_id: IndexId::new(2).unwrap(),
            generation,
            partition,
        }));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: Some(foreign_root),
                counters: OperationCounters::default(),
            })),
        ));
    }

    #[test]
    fn awaiting_text_compaction_upload_requires_exact_same_partition_artifacts() {
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let partition = TextPartition::Unpartitioned.fingerprint();
        let other_partition = TextPartition::try_tenant_value(Bytes::from_static(b"tenant"))
            .unwrap()
            .fingerprint();
        let artifact_key =
            |owner_index_id, owner_generation, owner_partition, ordinal, owner_scope| {
                Key::Data {
                    scope: owner_scope,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(
                        TextBuildArtifactKey {
                            root: TextManifestRootKey {
                                index_id: owner_index_id,
                                generation: owner_generation,
                                partition: owner_partition,
                            },
                            ordinal,
                        },
                    )),
                }
                .to_bytes()
            };
        let operation = |inputs: Vec<Bytes>, output: Bytes| {
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x61; 16]).unwrap(),
                index_id,
                IndexIdentity::new(
                    IndexIdentityFamily::Text,
                    IndexElementKind::Node,
                    IndexComponent::try_new("label", "Doc").unwrap(),
                    IndexComponent::try_new("property", "body").unwrap(),
                ),
                generation,
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Build,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::AwaitCompactionUpload(
                        TextCompactionUploadProgress::try_new(
                            PrefixScanProgress {
                                cursor: None,
                                counters: OperationCounters::default(),
                            },
                            inputs
                                .into_iter()
                                .map(|key| super::super::IndexCursor::try_new(key).unwrap())
                                .collect(),
                            OperationCounters {
                                entities: 1,
                                input_bytes: 1,
                                output_operations: 1,
                                output_bytes: 1,
                            },
                            super::super::IndexCursor::try_new(output).unwrap(),
                            TextUploadIntentId::from_bytes([0x62; 16]).unwrap(),
                        )
                        .unwrap(),
                    ),
                )),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap()
        };
        let exact_inputs = vec![
            artifact_key(index_id, generation, partition, 1, scope),
            artifact_key(index_id, generation, partition, 2, scope),
        ];
        let exact_output = artifact_key(index_id, generation, partition, 3, scope);

        assert!(operation_record_cursors_are_valid(
            scope,
            &operation(exact_inputs.clone(), exact_output.clone()),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(
                vec![
                    artifact_key(IndexId::new(2).unwrap(), generation, partition, 1, scope),
                    artifact_key(IndexId::new(2).unwrap(), generation, partition, 2, scope),
                ],
                exact_output.clone(),
            ),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(
                vec![
                    artifact_key(index_id, generation, other_partition, 1, scope),
                    artifact_key(index_id, generation, other_partition, 2, scope),
                ],
                exact_output.clone(),
            ),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(
                exact_inputs.clone(),
                artifact_key(index_id, generation, other_partition, 3, scope),
            ),
        ));
        let tenant_scope = DataScope::Tenant(TenantId::from_u128(9));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(
                vec![
                    artifact_key(index_id, generation, partition, 1, tenant_scope),
                    artifact_key(index_id, generation, partition, 2, tenant_scope),
                ],
                exact_output,
            ),
        ));
        assert!(!operation_record_cursors_are_valid(
            scope,
            &operation(
                vec![
                    Key::Data {
                        scope,
                        kind: DataKeyKind::IndexV2(IndexV2Key::operation(
                            IndexOperationId::from_bytes([0x70; 16]).unwrap(),
                        )),
                    }
                    .to_bytes(),
                    Key::Data {
                        scope,
                        kind: DataKeyKind::IndexV2(IndexV2Key::operation(
                            IndexOperationId::from_bytes([0x71; 16]).unwrap(),
                        )),
                    }
                    .to_bytes(),
                ],
                artifact_key(index_id, generation, partition, 3, scope),
            ),
        ));
    }

    #[tokio::test]
    async fn queue_pointer_reads_cross_check_scope_identity_generation_and_revision() {
        let db = raw_db("index-v2-pointer-cross-checks").await;
        bootstrap_writer(&db).await.unwrap();
        let scope = DataScope::Tenant(TenantId::from_u128(42));

        let operation_id = IndexOperationId::from_bytes([21; 16]).unwrap();
        let operation = queued_secondary_operation(operation_id);
        db.put(
            Key::Data {
                scope,
                kind: DataKeyKind::IndexV2(IndexV2Key::operation(operation_id)),
            }
            .to_bytes(),
            encode_operation_record(&operation),
        )
        .await
        .unwrap();
        db.put(
            global_key(GlobalIndexV2Key::OperationPointer(operation_id)),
            encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                super::super::OperationQueuePointerValue {
                    scope,
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    record_revision: operation.operation_revision(),
                },
            )),
        )
        .await
        .unwrap();
        assert_eq!(
            load_operation_from_pointer(&db, operation_id)
                .await
                .unwrap(),
            Some(operation.clone())
        );

        let intent_id = TextUploadIntentId::from_bytes([22; 16]).unwrap();
        let intent = queued_text_intent(intent_id);
        db.put(
            Key::Data {
                scope,
                kind: DataKeyKind::IndexV2(IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                    index_id: intent.index_id,
                    generation: intent.generation,
                    intent_id,
                })),
            }
            .to_bytes(),
            encode_work_value(&IndexV2WorkValue::TextUploadIntent(Box::new(
                intent.clone(),
            ))),
        )
        .await
        .unwrap();
        db.put(
            global_key(GlobalIndexV2Key::UploadPointer(intent_id)),
            encode_metadata_value(&IndexV2MetadataValue::UploadQueuePointer(
                super::super::UploadQueuePointerValue {
                    scope,
                    index_id: intent.index_id,
                    generation: intent.generation,
                    record_revision: intent.revision,
                },
            )),
        )
        .await
        .unwrap();
        assert_eq!(
            load_upload_from_pointer(&db, intent_id).await.unwrap(),
            Some(intent)
        );

        db.put(
            global_key(GlobalIndexV2Key::OperationPointer(operation_id)),
            encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                super::super::OperationQueuePointerValue {
                    scope,
                    index_id: IndexId::new(2).unwrap(),
                    generation: operation.generation(),
                    record_revision: operation.operation_revision(),
                },
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_operation_from_pointer(&db, operation_id).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn allocator_commit_and_abort_preserve_transactional_high_watermarks() {
        let db = raw_db("index-v2-allocator-transactions").await;
        bootstrap_writer(&db).await.unwrap();

        let aborted = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(allocate_index_id(&aborted).await.unwrap().get(), 1);
        drop(aborted);

        let committed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(allocate_index_id(&committed).await.unwrap().get(), 1);
        assert_eq!(peek_vector_physical_id(&committed).await.unwrap().get(), 1);
        assert_eq!(
            allocate_vector_physical_id(&committed).await.unwrap().get(),
            1
        );
        assert_eq!(peek_vector_physical_id(&committed).await.unwrap().get(), 2);
        committed.commit().await.unwrap();

        let next = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(allocate_index_id(&next).await.unwrap().get(), 2);
        assert_eq!(peek_vector_physical_id(&next).await.unwrap().get(), 2);
        assert_eq!(allocate_vector_physical_id(&next).await.unwrap().get(), 2);
        drop(next);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn vector_partition_mapping_is_atomic_idempotent_and_cross_checked() {
        let db = raw_db("index-v2-vector-partition-mapping").await;
        bootstrap_writer(&db).await.unwrap();
        let scope = DataScope::Tenant(TenantId::from_u128(42));
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let partition = VectorTenantPartition::try_new(Bytes::from_static(b"acme")).unwrap();

        let committed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let physical_index_id = stage_vector_partition_mapping(
            &committed,
            scope,
            index_id,
            generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap();
        assert_eq!(physical_index_id, VectorPhysicalIndexId::initial());
        assert_eq!(
            stage_vector_partition_mapping(
                &committed,
                scope,
                index_id,
                generation,
                VectorPhysicalLayout::Partitioned,
                &partition,
            )
            .await
            .unwrap(),
            physical_index_id
        );
        committed.commit().await.unwrap();
        assert_eq!(
            load_vector_partition_mapping(
                &db,
                scope,
                index_id,
                generation,
                VectorPhysicalLayout::Partitioned,
                &partition,
            )
            .await
            .unwrap(),
            Some(physical_index_id)
        );
        assert!(matches!(
            load_vector_partition_mapping(
                &db,
                scope,
                index_id,
                generation,
                VectorPhysicalLayout::Unpartitioned { physical_index_id },
                &partition,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));

        let aborted_partition =
            VectorTenantPartition::try_new(Bytes::from_static(b"aborted")).unwrap();
        let aborted = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            stage_vector_partition_mapping(
                &aborted,
                scope,
                index_id,
                generation,
                VectorPhysicalLayout::Partitioned,
                &aborted_partition,
            )
            .await
            .unwrap()
            .get(),
            2
        );
        drop(aborted);
        assert_eq!(
            load_vector_partition_mapping(
                &db,
                scope,
                index_id,
                generation,
                VectorPhysicalLayout::Partitioned,
                &aborted_partition,
            )
            .await
            .unwrap(),
            None
        );

        let key = Key::Data {
            scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::VectorPartitionMapping(
                VectorPartitionMappingKey {
                    index_id,
                    generation,
                    partition: partition.fingerprint(),
                },
            )),
        }
        .to_bytes();
        db.put(
            key,
            encode_work_value(&IndexV2WorkValue::VectorPartitionMapping(
                VectorPartitionMappingValue {
                    index_id,
                    generation,
                    partition: VectorTenantPartition::try_new(Bytes::from_static(b"other"))
                        .unwrap(),
                    physical_index_id,
                },
            )),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_vector_partition_mapping(
                &db,
                scope,
                index_id,
                generation,
                VectorPhysicalLayout::Partitioned,
                &partition,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn numeric_allocator_rejects_the_exhaustion_sentinel_without_writing() {
        let db = raw_db("index-v2-allocator-exhaustion").await;
        bootstrap_writer(&db).await.unwrap();
        db.put(
            global_key(GlobalIndexV2Key::LogicalIndexIdWatermark),
            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::new(u64::MAX).unwrap(),
                },
            )),
        )
        .await
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(matches!(
            allocate_index_id(&transaction).await,
            Err(HelixDbError::IdentifierExhausted("logical index ID"))
        ));
        drop(transaction);
        let value = db
            .get(global_key(GlobalIndexV2Key::LogicalIndexIdWatermark))
            .await
            .unwrap()
            .unwrap();
        let IndexV2MetadataValue::LogicalIndexIdWatermark(watermark) =
            decode_metadata_value(&value).unwrap()
        else {
            panic!("logical watermark retains its value kind")
        };
        assert_eq!(watermark.next_id.get(), u64::MAX);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn uuid_allocators_retry_every_durable_collision_lane() {
        let db = raw_db("index-v2-uuid-collisions").await;
        bootstrap_writer(&db).await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        let operation_row_collision = IndexOperationId::from_bytes([1; 16]).unwrap();
        let operation_pointer_collision = IndexOperationId::from_bytes([2; 16]).unwrap();
        let operation_free = IndexOperationId::from_bytes([3; 16]).unwrap();
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::operation(operation_row_collision)),
                }
                .to_bytes(),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        transaction
            .put(
                global_key(GlobalIndexV2Key::OperationPointer(
                    operation_pointer_collision,
                )),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        assert_eq!(
            allocate_operation_id_from(
                &transaction,
                DataScope::LegacyUnscoped,
                [
                    operation_row_collision,
                    operation_pointer_collision,
                    operation_free,
                ],
                3,
            )
            .await
            .unwrap(),
            operation_free
        );

        let intent_row_collision = TextUploadIntentId::from_bytes([4; 16]).unwrap();
        let intent_pointer_collision = TextUploadIntentId::from_bytes([5; 16]).unwrap();
        let intent_free = TextUploadIntentId::from_bytes([6; 16]).unwrap();
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                        index_id,
                        generation,
                        intent_id: intent_row_collision,
                    })),
                }
                .to_bytes(),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        transaction
            .put(
                global_key(GlobalIndexV2Key::UploadPointer(intent_pointer_collision)),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        assert_eq!(
            allocate_text_upload_intent_id_from(
                &transaction,
                DataScope::LegacyUnscoped,
                index_id,
                generation,
                [intent_row_collision, intent_pointer_collision, intent_free],
                3,
            )
            .await
            .unwrap(),
            intent_free
        );

        let root_collision = BlobGcRunId::from_bytes([7; 16]).unwrap();
        let mark_collision = BlobGcRunId::from_bytes([8; 16]).unwrap();
        let member_collision = BlobGcRunId::from_bytes([9; 16]).unwrap();
        let run_free = BlobGcRunId::from_bytes([10; 16]).unwrap();
        transaction
            .put(
                global_key(GlobalIndexV2Key::BlobGcRunRoot(root_collision)),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        transaction
            .put(
                global_key(GlobalIndexV2Key::BlobGcReachabilityMark {
                    run_id: mark_collision,
                    pass: BlobGcPass::First,
                    scan_attempt: NonZeroU64::MIN,
                    blob_hash: BlobHash::new([11; 32]),
                }),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        transaction
            .put(
                global_key(GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id: member_collision,
                    blob_hash: BlobHash::new([12; 32]),
                }),
                Bytes::from_static(b"occupied"),
            )
            .unwrap();
        assert_eq!(
            allocate_blob_gc_run_id_from(
                &transaction,
                [root_collision, mark_collision, member_collision, run_free],
                4,
            )
            .await
            .unwrap(),
            run_free
        );
        assert!(matches!(
            allocate_blob_gc_run_id_from(&transaction, [root_collision], 1).await,
            Err(HelixDbError::IdentifierAllocationFailed {
                kind: "blob GC run ID",
                attempts: 1,
            })
        ));
        drop(transaction);
        db.close().await.unwrap();
    }
}
