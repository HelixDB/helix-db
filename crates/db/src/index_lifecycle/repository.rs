//! Durable V2 bootstrap, catalog loading, allocation, and handle validation.
//!
//! This is the sole boundary that turns typed V2 keys and values into SlateDB
//! operations. Writer bootstrap uses serializable-snapshot isolation and a
//! complete logical-keyspace scan so marker initialization cannot race another
//! metadata write.

use bytes::Bytes;
#[cfg(test)]
use slatedb::{Db, IsolationLevel};
use slatedb::{DbReadOps, DbTransaction};

use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorStorageLane};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::keys::{DataKey as GraphKey, DataKeyKind, KeyPrefix};
use crate::encoding::v2::keys::{
    GlobalKey, RecordKind, ScopedKey, VectorPartitionMappingKey, GLOBAL_SENTINEL,
};
use crate::encoding::v2::values::{
    decode_index_record, decode_metadata_value, decode_partition_mapping, encode_metadata_value,
    encode_partition_mapping,
};
use crate::error::{HelixDbError, Result, WriterMigrationRequirement};

use super::work::{VectorPartitionMappingValue, VectorTenantPartition};
use super::{
    ActiveIndexHandle, IndexElementKind, IndexGenerationId, IndexId, IndexIdentity,
    IndexIdentityFamily, IndexOperationId, IndexOperationRecord, IndexRecordV2,
    IndexStorageVersion, IndexV2MetadataValue, LegacyVectorPhysicalReservation,
    LoadedV2ScopeCatalog, LogicalIndexIdWatermark, VectorPhysicalIdWatermark,
    VectorPhysicalIndexId, VectorPhysicalLayout,
};

const UUID_ALLOCATION_ATTEMPTS: usize = 16;

fn global_key(key: GlobalKey) -> Bytes {
    ManagedIndexKey::Global { kind: key }.to_bytes()
}

fn metadata_or_migration_required(
    value: &[u8],
    role: &'static str,
) -> Result<IndexV2MetadataValue> {
    decode_metadata_value(value).map_err(|error| HelixDbError::MigrationRequired {
        reason: format!("malformed V2 {role}: {error}"),
    })
}

/// Reader-visible storage formats with explicit physical-read compatibility.
///
/// Adding a storage migration does not make it reader-compatible. A future
/// migration must add a new variant and exhaustively define its read behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReaderStorageCompatibility {
    /// All physical reads use the current storage format.
    Current,
    /// V2/V3 non-unique equality entries are unioned with V4 equality bitmaps.
    LegacyEqualityUnion,
}

impl ReaderStorageCompatibility {
    /// Accepts the only monotonic compatibility transition for an open reader.
    pub(crate) fn advance(self, observed: Self) -> Result<Self> {
        match (self, observed) {
            (Self::Current, Self::Current) | (Self::LegacyEqualityUnion, Self::Current) => {
                Ok(Self::Current)
            }
            (Self::LegacyEqualityUnion, Self::LegacyEqualityUnion) => Ok(Self::LegacyEqualityUnion),
            (Self::Current, Self::LegacyEqualityUnion) => Err(HelixDbError::MigrationRequired {
                reason: "reader storage compatibility moved backwards from current to legacy"
                    .to_string(),
            }),
        }
    }
}

/// Accepts a reader-compatible complete bootstrap tuple or a pristine legacy store.
///
/// This is intentionally validation-only. Valid writer-resumable storage is
/// reported through [`HelixDbError::WriterMigrationRequired`], while partial or
/// malformed metadata remains a fatal migration error.
pub(crate) async fn require_reader_bootstrap_or_legacy(
    reader: &(impl DbReadOps + Send + Sync),
) -> Result<ReaderStorageCompatibility> {
    crate::membership_delta::read_write_mode(reader).await?;
    let tenant_envelope_ready = crate::migrations::tenant_key_envelope_ready(reader).await?;
    let marker_key = global_key(GlobalKey::StorageVersion);
    let logical_key = global_key(GlobalKey::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    let marker = reader.get(marker_key).await?;
    let logical = reader.get(logical_key).await?;
    let vector = reader.get(vector_key).await?;
    if let Some(marker) = marker {
        let bootstrap = validate_bootstrap_values(&marker, logical.as_deref(), vector.as_deref())?;
        let progress =
            crate::migrations::storage_schema_progress(reader, DataScope::LegacyUnscoped).await?;
        let cleanup_ready = crate::migrations::index_storage_v4_cleanup_ready(reader).await?;
        if bootstrap == ValidatedReaderBootstrap::LegacyEqualityUnion && cleanup_ready {
            return Err(HelixDbError::MigrationRequired {
                reason: "index storage V4 cleanup is marked complete beside legacy storage"
                    .to_string(),
            });
        }
        return match (bootstrap, progress, tenant_envelope_ready) {
            (
                ValidatedReaderBootstrap::LegacyEqualityUnion,
                crate::migrations::StorageSchemaProgress::Complete,
                true,
            ) => Ok(ReaderStorageCompatibility::LegacyEqualityUnion),
            (
                ValidatedReaderBootstrap::Current,
                crate::migrations::StorageSchemaProgress::Complete,
                true,
            ) => Ok(ReaderStorageCompatibility::Current),
            (
                ValidatedReaderBootstrap::Current | ValidatedReaderBootstrap::LegacyEqualityUnion,
                crate::migrations::StorageSchemaProgress::NotStarted
                | crate::migrations::StorageSchemaProgress::GraphReady
                | crate::migrations::StorageSchemaProgress::IndexReady,
                _,
            )
            | (
                ValidatedReaderBootstrap::Current | ValidatedReaderBootstrap::LegacyEqualityUnion,
                crate::migrations::StorageSchemaProgress::Complete,
                false,
            ) => Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            }),
        };
    }
    if logical.is_some() || vector.is_some() {
        return Err(HelixDbError::MigrationRequired {
            reason: "read-only storage has a partial V2 bootstrap tuple".to_string(),
        });
    }
    let mut rows = reader.scan(..).await?;
    while let Some(row) = rows.next().await? {
        let is_global_v2 = row.key.starts_with(&GLOBAL_SENTINEL);
        let is_unscoped_v2 = row.key.first().copied() == Some(ScopedKey::key_prefix());
        let is_tenant_v2 = DataScope::strip_tenant_envelope(&row.key)
            .is_some_and(|(_, logical)| ScopedKey::parse_from_slice(logical).is_ok());
        let is_legacy_tenant =
            crate::migrations::legacy_key_requires_migration(row.key, row.value)?;
        if tenant_envelope_ready && is_legacy_tenant {
            return Err(HelixDbError::MigrationRequired {
                reason: "tenant key envelope readiness is inconsistent with a legacy tenant key"
                    .to_string(),
            });
        }
        if !tenant_envelope_ready
            && (is_global_v2 || is_unscoped_v2 || is_tenant_v2 || is_legacy_tenant)
        {
            return Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            });
        }
        if is_global_v2 || is_unscoped_v2 {
            return Err(HelixDbError::MigrationRequired {
                reason: "read-only storage has V2 rows without the complete bootstrap tuple"
                    .to_string(),
            });
        }
    }
    if tenant_envelope_ready {
        Err(HelixDbError::WriterMigrationRequired {
            requirement: WriterMigrationRequirement::IncompleteStorageSchema,
        })
    } else {
        Ok(ReaderStorageCompatibility::Current)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidatedReaderBootstrap {
    Current,
    LegacyEqualityUnion,
}

fn validate_bootstrap_values(
    marker: &[u8],
    logical: Option<&[u8]>,
    vector: Option<&[u8]>,
) -> Result<ValidatedReaderBootstrap> {
    let IndexV2MetadataValue::StorageVersion(version) =
        metadata_or_migration_required(marker, "storage marker")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage marker contains the wrong value kind".to_string(),
        });
    };
    let minimum_writer_version =
        IndexStorageVersion::new(0x0002).expect("the V2 storage version is non-zero");
    if version < minimum_writer_version {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "index storage version {} predates required version {}; recreate this development database",
                version.get(),
                IndexStorageVersion::CURRENT.get()
            ),
        });
    }
    if version > IndexStorageVersion::MAX_SUPPORTED {
        return Err(HelixDbError::UnsupportedIndexStorageVersion {
            found: version.get(),
            supported: IndexStorageVersion::MAX_SUPPORTED.get(),
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
    match version.get() {
        0x0002 | 0x0003 => Ok(ValidatedReaderBootstrap::LegacyEqualityUnion),
        0x0004 | 0x0005 => Err(HelixDbError::WriterMigrationRequired {
            requirement: WriterMigrationRequirement::StorageVersion {
                found: version.get(),
                target: IndexStorageVersion::CURRENT.get(),
            },
        }),
        0x0006 => Ok(ValidatedReaderBootstrap::Current),
        _ => unreachable!("unsupported storage versions returned before compatibility dispatch"),
    }
}

/// Loads and key/value-cross-validates every canonical record for one scope.
pub(crate) async fn load_scope_catalog(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
) -> Result<LoadedV2ScopeCatalog> {
    let logical_prefix = ScopedKey::logical_prefix(RecordKind::IndexRecord);
    let physical_prefix = ManagedIndexKey::data_prefix(scope, logical_prefix);
    let mut rows = reader.scan_prefix(&physical_prefix, ..).await?;
    let mut loaded = LoadedV2ScopeCatalog::new(scope);
    while let Some(row) = rows.next().await? {
        let parsed = ManagedIndexKey::parse_from_slice(scope, &row.key)?;
        let ManagedIndexKey::Data {
            kind: ScopedKey::IndexRecord(key),
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
    let key = ManagedIndexKey::Data {
        scope,
        kind: ScopedKey::index_record(identity.clone()),
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
    let logical = ScopedKey::index_record(handle.identity().clone());
    let key = ManagedIndexKey::Data {
        scope: handle.scope(),
        kind: logical,
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

fn complete_cursor_is_valid(scope: DataScope, cursor: &[u8]) -> bool {
    const SENTINEL_OFFSET: usize = 0;
    let is_global = cursor.len() >= GLOBAL_SENTINEL.len()
        && cursor[SENTINEL_OFFSET..SENTINEL_OFFSET + GLOBAL_SENTINEL.len()] == GLOBAL_SENTINEL;
    if is_global {
        GlobalKey::parse_from_slice(cursor).is_ok()
    } else {
        ManagedIndexKey::parse_from_slice(scope, cursor).is_ok()
            || GraphKey::parse_from_slice(scope, cursor).is_ok()
    }
}

/// Validates every persisted operation cursor against an exact V1, V2, or
/// global key parser before a lifecycle transaction accepts it.
pub(super) fn operation_cursors_are_valid(
    scope: DataScope,
    progress: &super::IndexOperationProgress,
) -> bool {
    progress.cursors_are_valid(|cursor| complete_cursor_is_valid(scope, cursor.as_bytes()))
}

#[derive(Debug, Clone, Copy)]
enum ScopedCursorExpectation {
    AppliedState,
    SecondaryEntry,
    TextEntityState,
    TextBuildArtifact,
    TextManifestPage,
    TextManifestRoot,
    VectorPartitionMapping,
    TextCleanupMetadata,
}

fn graph_source_upper_bound_is_valid(
    scope: DataScope,
    element_kind: IndexElementKind,
    cursor: &[u8],
) -> bool {
    matches!(
        (element_kind, GraphKey::parse_from_slice(scope, cursor)),
        (
            IndexElementKind::Node,
            Ok(GraphKey::Data {
                scope: cursor_scope,
                kind: DataKeyKind::NodeProperty(_),
            })
        ) if cursor_scope == scope
    ) || matches!(
        (element_kind, GraphKey::parse_from_slice(scope, cursor)),
        (
            IndexElementKind::Edge,
            Ok(GraphKey::Data {
                scope: cursor_scope,
                kind: DataKeyKind::EdgePropertyById(_),
            })
        ) if cursor_scope == scope
    )
}

fn graph_source_cursor_is_valid(
    scope: DataScope,
    element_kind: IndexElementKind,
    cursor: &[u8],
) -> bool {
    let prefix = match element_kind {
        IndexElementKind::Node => KeyPrefix::NodeProperty,
        IndexElementKind::Edge => KeyPrefix::EdgePropertyById,
    };
    let source_prefix = GraphKey::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()));
    cursor.starts_with(&source_prefix)
        && matches!(
            GraphKey::parse_from_slice(scope, cursor),
            Ok(GraphKey::Data {
                scope: cursor_scope,
                ..
            }) if cursor_scope == scope
        )
}

fn secondary_lane_matches_identity(
    identity: &IndexIdentity,
    lane: crate::encoding::v2::keys::SecondaryEntryLane,
) -> bool {
    use crate::encoding::v2::keys::SecondaryEntryLane;

    matches!(
        (identity.family(), identity.element_kind(), lane),
        (
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
            SecondaryEntryLane::NodeEquality | SecondaryEntryLane::NodeUniqueEquality,
        ) | (
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Edge,
            SecondaryEntryLane::EdgeEquality,
        ) | (
            IndexIdentityFamily::SecondaryRange,
            IndexElementKind::Node,
            SecondaryEntryLane::NodeRangeAscending | SecondaryEntryLane::NodeRangeDescending,
        ) | (
            IndexIdentityFamily::SecondaryRange,
            IndexElementKind::Edge,
            SecondaryEntryLane::EdgeRangeAscending | SecondaryEntryLane::EdgeRangeDescending,
        )
    )
}

fn scoped_cursor_is_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
    expectation: ScopedCursorExpectation,
    cursor: &[u8],
) -> bool {
    let Ok(ManagedIndexKey::Data {
        scope: cursor_scope,
        kind,
    }) = ManagedIndexKey::parse_from_slice(scope, cursor)
    else {
        return false;
    };
    if cursor_scope != scope {
        return false;
    }
    let index_id = operation.index_id();
    let generation = operation.generation();
    let element_kind = operation.identity().element_kind();
    match (expectation, kind) {
        (ScopedCursorExpectation::AppliedState, ScopedKey::AppliedState(key)) => {
            key.index_id == index_id
                && key.generation == generation
                && key.entity.kind == element_kind
        }
        (ScopedCursorExpectation::SecondaryEntry, ScopedKey::SecondaryEntry(key)) => {
            key.index_id() == index_id
                && key.generation() == generation
                && secondary_lane_matches_identity(operation.identity(), key.lane())
        }
        (ScopedCursorExpectation::SecondaryEntry, ScopedKey::SecondaryEqualityBitmap(key)) => {
            key.index_id == index_id
                && key.generation == generation
                && key.element_kind == element_kind
                && operation.identity().family() == IndexIdentityFamily::SecondaryEquality
        }
        (ScopedCursorExpectation::TextEntityState, ScopedKey::TextEntityState(key)) => {
            key.root.index_id == index_id
                && key.root.generation == generation
                && key.entity.kind == element_kind
        }
        (ScopedCursorExpectation::TextBuildArtifact, ScopedKey::TextBuildArtifact(key)) => {
            key.root.index_id == index_id && key.root.generation == generation
        }
        (ScopedCursorExpectation::TextManifestPage, ScopedKey::TextManifestPage(key)) => {
            key.root.index_id == index_id && key.root.generation == generation
        }
        (ScopedCursorExpectation::TextManifestRoot, ScopedKey::TextManifestRoot(key)) => {
            key.index_id == index_id && key.generation == generation
        }
        (
            ScopedCursorExpectation::VectorPartitionMapping,
            ScopedKey::VectorPartitionMapping(key),
        ) => key.index_id == index_id && key.generation == generation,
        (ScopedCursorExpectation::TextCleanupMetadata, key) => match key {
            ScopedKey::TextBuildArtifact(key) => {
                key.root.index_id == index_id && key.root.generation == generation
            }
            ScopedKey::TextManifestPage(key) => {
                key.root.index_id == index_id && key.root.generation == generation
            }
            ScopedKey::TextManifestRoot(key) => {
                key.index_id == index_id && key.generation == generation
            }
            ScopedKey::TextEntityState(key) => {
                key.root.index_id == index_id
                    && key.root.generation == generation
                    && key.entity.kind == element_kind
            }
            ScopedKey::TextCorpusStatistics(key) => {
                key.index_id == index_id && key.generation == generation
            }
            ScopedKey::TextTermStatistics(key) => {
                key.corpus.index_id == index_id && key.corpus.generation == generation
            }
            ScopedKey::TextStatisticsEntity(key) => {
                key.index_id == index_id
                    && key.generation == generation
                    && key.entity.kind == element_kind
            }
            ScopedKey::BuildDelta(key) | ScopedKey::AppliedState(key) => {
                key.index_id == index_id
                    && key.generation == generation
                    && key.entity.kind == element_kind
            }
            ScopedKey::IndexRecord(_)
            | ScopedKey::Operation(_)
            | ScopedKey::SecondaryEntry(_)
            | ScopedKey::VectorPartitionMapping(_)
            | ScopedKey::SecondaryEqualityBitmap(_) => false,
        },
        (
            ScopedCursorExpectation::AppliedState
            | ScopedCursorExpectation::SecondaryEntry
            | ScopedCursorExpectation::TextEntityState
            | ScopedCursorExpectation::TextBuildArtifact
            | ScopedCursorExpectation::TextManifestPage
            | ScopedCursorExpectation::TextManifestRoot
            | ScopedCursorExpectation::VectorPartitionMapping,
            _,
        ) => false,
    }
}

fn source_progress_is_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &super::SourceScanProgress,
) -> bool {
    let element_kind = operation.identity().element_kind();
    graph_source_upper_bound_is_valid(
        scope,
        element_kind,
        progress.inclusive_upper_bound.as_bytes(),
    ) && progress.cursor.as_ref().is_none_or(|cursor| {
        graph_source_cursor_is_valid(scope, element_kind, cursor.as_bytes())
            && cursor.as_bytes() <= progress.inclusive_upper_bound.as_bytes()
    })
}

fn prefix_progress_is_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
    progress: &super::PrefixScanProgress,
    expectation: ScopedCursorExpectation,
) -> bool {
    progress.cursor.as_ref().is_none_or(|cursor| {
        scoped_cursor_is_valid(scope, operation, expectation, cursor.as_bytes())
    })
}

fn text_partition_upper_bound_is_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &[u8],
) -> bool {
    matches!(
        ManagedIndexKey::parse_from_slice(scope, cursor),
        Ok(ManagedIndexKey::Data {
            scope: cursor_scope,
            kind: ScopedKey::TextEntityState(key),
        }) if cursor_scope == scope
            && key.root.index_id == operation.index_id()
            && key.root.generation == operation.generation()
            && key.root.partition == crate::encoding::v2::keys::PartitionFingerprint::new([u8::MAX; 32])
            && key.entity.kind == IndexElementKind::Edge
            && key.entity.id == super::IndexEntityId::new(u64::MAX)
    )
}

fn legacy_vector_physical_id(operation: &IndexOperationRecord) -> u64 {
    let element_type = match operation.identity().element_kind() {
        IndexElementKind::Node => crate::config::VectorElementType::Node,
        IndexElementKind::Edge => crate::config::VectorElementType::Edge,
    };
    let physical_name = crate::search::vector_index_name(
        element_type,
        operation.identity().label().as_str(),
        operation.identity().property().as_str(),
    );
    crate::search::vector::index_id_from_name(&physical_name)
}

fn legacy_vector_cursor_is_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
    lane: super::LegacyVectorValidationLane,
    cursor: &[u8],
) -> bool {
    let Ok(GraphKey::Data {
        scope: cursor_scope,
        kind: DataKeyKind::Vector(key),
    }) = GraphKey::parse_from_slice(scope, cursor)
    else {
        return false;
    };
    let expected_lane = match lane {
        super::LegacyVectorValidationLane::Core => VectorStorageLane::Core,
        super::LegacyVectorValidationLane::Hot => VectorStorageLane::Hot,
        super::LegacyVectorValidationLane::Layer0 => VectorStorageLane::Layer0,
    };
    cursor_scope == scope
        && key.index_id() == legacy_vector_physical_id(operation)
        && key.storage_lane() == expected_lane
        && !matches!(
            key,
            VectorKey::IndexPrefix(_)
                | VectorKey::MemoryPrefix(_)
                | VectorKey::L0Prefix(_)
                | VectorKey::EntryCandidatePrefix(_)
                | VectorKey::ReverseEdgePrefix(_)
        )
}

fn legacy_directory_cursor_is_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: &[u8],
) -> bool {
    matches!(
        GraphKey::parse_from_slice(scope, cursor),
        Ok(GraphKey::Data {
            scope: cursor_scope,
            kind: DataKeyKind::Vector(VectorKey::SimHashDirectory(key)),
        }) if cursor_scope == scope && key.index_id() == legacy_vector_physical_id(operation)
    )
}

/// Validates every cursor against the exact stage, scope, and generation that
/// will consume it. A syntactically valid key from another lane is rejected.
pub(crate) fn operation_record_cursors_are_valid(
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> bool {
    use super::{
        IndexOperationProgress, SecondaryBuildProgress, SecondaryBuildStage,
        SecondaryCleanupProgress, TextBuildProgress, TextBuildStage, TextCleanupProgress,
        TextManifestValidationProgress, VectorBuildProgress, VectorBuildStage,
        VectorCleanupProgress,
    };

    match operation.progress() {
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) => {
            match stage {
                SecondaryBuildStage::Scan(progress) => {
                    source_progress_is_valid(scope, operation, progress)
                }
                SecondaryBuildStage::CatchUp(progress) => progress.cursor.is_none(),
                SecondaryBuildStage::Validate(progress) => prefix_progress_is_valid(
                    scope,
                    operation,
                    progress,
                    ScopedCursorExpectation::AppliedState,
                ),
                SecondaryBuildStage::Activate(_) => true,
            }
        }
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(progress))
        | IndexOperationProgress::SecondaryCleanup(progress) => match progress {
            SecondaryCleanupProgress::DeleteEntries(progress) => prefix_progress_is_valid(
                scope,
                operation,
                progress,
                ScopedCursorExpectation::SecondaryEntry,
            ),
            SecondaryCleanupProgress::DeleteDeltas(progress) => progress.cursor.is_none(),
            SecondaryCleanupProgress::Finalize(_) => true,
        },
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)) => {
            match stage {
                VectorBuildStage::AdoptLegacy(progress) => {
                    progress.cursor.as_ref().is_none_or(|cursor| {
                        legacy_vector_cursor_is_valid(
                            scope,
                            operation,
                            progress.lane,
                            cursor.as_bytes(),
                        )
                    })
                }
                VectorBuildStage::ValidateAdoptedDirectory(progress) => {
                    progress.expected_markers == progress.counters.output_operations
                        && progress.verified_markers <= progress.expected_markers
                        && match progress.cursor.as_ref() {
                            None => progress.verified_markers == 0,
                            Some(cursor) => {
                                progress.verified_markers != 0
                                    && legacy_directory_cursor_is_valid(
                                        scope,
                                        operation,
                                        cursor.as_bytes(),
                                    )
                            }
                        }
                }
                VectorBuildStage::Scan(progress) => {
                    source_progress_is_valid(scope, operation, progress)
                }
                VectorBuildStage::CatchUp(progress) => progress.cursor.is_none(),
                VectorBuildStage::ValidateDescriptor(progress) => {
                    progress.cursor.as_ref().is_none_or(|cursor| {
                        scoped_cursor_is_valid(
                            scope,
                            operation,
                            ScopedCursorExpectation::AppliedState,
                            cursor.as_bytes(),
                        ) || scoped_cursor_is_valid(
                            scope,
                            operation,
                            ScopedCursorExpectation::VectorPartitionMapping,
                            cursor.as_bytes(),
                        )
                    })
                }
                VectorBuildStage::Activate(_) => true,
            }
        }
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(progress))
        | IndexOperationProgress::VectorCleanup(progress) => match progress {
            VectorCleanupProgress::RetireCache(_) | VectorCleanupProgress::Finalize(_) => true,
            VectorCleanupProgress::DeletePhysical(progress) => prefix_progress_is_valid(
                scope,
                operation,
                progress,
                ScopedCursorExpectation::VectorPartitionMapping,
            ),
            VectorCleanupProgress::DeleteDeltas(progress) => progress.cursor.is_none(),
        },
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) => match stage {
            TextBuildStage::ScanSource(progress) => {
                source_progress_is_valid(scope, operation, progress)
            }
            TextBuildStage::ScanPartitions(progress) => {
                text_partition_upper_bound_is_valid(
                    scope,
                    operation,
                    progress.inclusive_upper_bound.as_bytes(),
                ) && progress.cursor.as_ref().is_none_or(|cursor| {
                    scoped_cursor_is_valid(
                        scope,
                        operation,
                        ScopedCursorExpectation::TextEntityState,
                        cursor.as_bytes(),
                    ) && cursor.as_bytes() <= progress.inclusive_upper_bound.as_bytes()
                })
            }
            TextBuildStage::CatchUp(progress) => progress.cursor.is_none(),
            TextBuildStage::Compact(progress) | TextBuildStage::PrepareManifests(progress) => {
                prefix_progress_is_valid(
                    scope,
                    operation,
                    progress,
                    ScopedCursorExpectation::TextBuildArtifact,
                )
            }
            TextBuildStage::ValidateManifests(progress) => match progress {
                TextManifestValidationProgress::Pages(progress) => {
                    progress.cursor().is_none_or(|cursor| {
                        let Some(expected) = progress.partition() else {
                            return scoped_cursor_is_valid(
                                scope,
                                operation,
                                ScopedCursorExpectation::TextManifestPage,
                                cursor.as_bytes(),
                            );
                        };
                        let Ok(ManagedIndexKey::Data {
                            kind: ScopedKey::TextManifestPage(key),
                            ..
                        }) = ManagedIndexKey::parse_from_slice(scope, cursor.as_bytes())
                        else {
                            return false;
                        };
                        scoped_cursor_is_valid(
                            scope,
                            operation,
                            ScopedCursorExpectation::TextManifestPage,
                            cursor.as_bytes(),
                        ) && key.root.partition.as_bytes() == expected.partition_fingerprint()
                            && key.page.checked_add(1) == Some(expected.next_page())
                    })
                }
                TextManifestValidationProgress::Roots(progress) => prefix_progress_is_valid(
                    scope,
                    operation,
                    progress,
                    ScopedCursorExpectation::TextManifestRoot,
                ),
                TextManifestValidationProgress::EntityStates(progress) => prefix_progress_is_valid(
                    scope,
                    operation,
                    progress,
                    ScopedCursorExpectation::TextEntityState,
                ),
            },
            TextBuildStage::Activate(_) => true,
        },
        IndexOperationProgress::TextBuild(TextBuildProgress::Aborting(progress))
        | IndexOperationProgress::TextCleanup(progress) => match progress {
            TextCleanupProgress::DeleteMetadata(progress) => prefix_progress_is_valid(
                scope,
                operation,
                progress,
                ScopedCursorExpectation::TextCleanupMetadata,
            ),
            TextCleanupProgress::Finalize(_) => true,
        },
    }
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
    let key = global_key(GlobalKey::LogicalIndexIdWatermark);
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
    let key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    let mut candidate = peek_vector_physical_id(transaction).await?;
    loop {
        let next_id = candidate.checked_next()?;
        transaction.put(
            &key,
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark { next_id },
            )),
        )?;
        if load_legacy_vector_physical_reservation(transaction, candidate)
            .await?
            .is_none()
        {
            return Ok(candidate);
        }
        candidate = next_id;
    }
}

/// Loads one exact imported-namespace reservation and rejects another value kind.
pub(crate) async fn load_legacy_vector_physical_reservation(
    read: &(impl DbReadOps + Sync),
    physical_index_id: VectorPhysicalIndexId,
) -> Result<Option<LegacyVectorPhysicalReservation>> {
    let key = global_key(GlobalKey::LegacyVectorPhysicalReservation(
        physical_index_id,
    ));
    let Some(value) = read.get(key).await? else {
        return Ok(None);
    };
    let IndexV2MetadataValue::LegacyVectorPhysicalReservation(reservation) =
        metadata_or_migration_required(&value, "legacy vector physical reservation")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "legacy vector physical reservation contains the wrong value kind".to_string(),
        });
    };
    Ok(Some(reservation))
}

/// Loads the raw next-ID watermark before reservation skipping.
pub(crate) async fn load_vector_physical_watermark(
    read: &(impl DbReadOps + Sync),
) -> Result<VectorPhysicalIdWatermark> {
    let key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    let Some(value) = read.get(key).await? else {
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
    Ok(watermark)
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
    let watermark = load_vector_physical_watermark(reader).await?;
    let mut candidate = watermark.next_id;
    loop {
        if candidate.get() == u64::MAX {
            return Err(HelixDbError::IdentifierExhausted(
                "vector physical index ID",
            ));
        }
        if load_legacy_vector_physical_reservation(reader, candidate)
            .await?
            .is_none()
        {
            return Ok(candidate);
        }
        candidate = candidate.checked_next()?;
    }
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
    let key = vector_partition_mapping_key(scope, index_id, generation, partition);
    let Some(value) = reader.get(key).await? else {
        return Ok(None);
    };
    let mapping = super::expect_typed_value(
        decode_partition_mapping(&value),
        "vector partition mapping key contains another value kind",
    )?;
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
            let key = vector_partition_mapping_key(scope, index_id, generation, partition);
            transaction.put(
                key,
                encode_partition_mapping(&VectorPartitionMappingValue {
                    index_id,
                    generation,
                    partition: partition.clone(),
                    physical_index_id,
                }),
            )?;
            Ok(physical_index_id)
        }
    }
}

/// Deletes one exact empty tenant mapping in its owning graph transaction.
///
/// Re-reading and cross-checking the typed value makes a stale physical ID or
/// partition fingerprint fail closed. The caller must first prove every
/// physical lane empty except the metadata and optional transaction guard that
/// it deletes in the same transaction.
pub(crate) async fn stage_delete_vector_partition_mapping(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    layout: VectorPhysicalLayout,
    partition: &VectorTenantPartition,
    physical_index_id: VectorPhysicalIndexId,
) -> Result<()> {
    let Some(mapped_physical_id) =
        load_vector_partition_mapping(transaction, scope, index_id, generation, layout, partition)
            .await?
    else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "empty vector partition lost its mapping before reclamation".to_string(),
        ));
    };
    if mapped_physical_id != physical_index_id {
        return Err(HelixDbError::IndexCatalogCorruption(
            "empty vector partition mapping changed physical ownership".to_string(),
        ));
    }
    transaction.delete(vector_partition_mapping_key(
        scope, index_id, generation, partition,
    ))?;
    Ok(())
}

fn vector_partition_mapping_key(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    partition: &VectorTenantPartition,
) -> Bytes {
    ManagedIndexKey::Data {
        scope,
        kind: ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        }),
    }
    .to_bytes()
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
        let scoped = ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::operation(candidate),
        }
        .to_bytes();
        let pointer = global_key(GlobalKey::OperationPointer(candidate));
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
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::v2::keys::metadata::MetadataKey;
    use crate::encoding::v2::keys::scope::{TenantId, TENANT_KEY_PREFIX};
    use crate::encoding::v2::keys::{DataKey as GraphKey, DataKeyKind};
    use crate::migrations::startup::bootstrap_writer;

    #[test]
    fn storage_versions_with_canonical_labels_are_current() {
        assert_eq!(IndexStorageVersion::CURRENT.get(), 0x0006);
        let logical = encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
            LogicalIndexIdWatermark {
                next_id: IndexId::initial(),
            },
        ));
        let vector = encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
            VectorPhysicalIdWatermark {
                next_id: VectorPhysicalIndexId::initial(),
            },
        ));
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        ));
        assert_eq!(
            validate_bootstrap_values(&marker, Some(&logical), Some(&vector))
                .expect("canonical-label storage is reader-current"),
            ValidatedReaderBootstrap::Current
        );
        for found in [0x0004, 0x0005] {
            let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(found).unwrap(),
            ));
            assert!(matches!(
                validate_bootstrap_values(&marker, Some(&logical), Some(&vector)),
                Err(HelixDbError::WriterMigrationRequired {
                    requirement: WriterMigrationRequirement::StorageVersion {
                        found: refused,
                        target: 0x0006,
                    },
                }) if refused == found
            ));
        }
        for legacy in [0x0002, 0x0003] {
            let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(legacy).unwrap(),
            ));
            assert_eq!(
                validate_bootstrap_values(&marker, Some(&logical), Some(&vector))
                    .expect("only versions two and three have explicit reader compatibility"),
                ValidatedReaderBootstrap::LegacyEqualityUnion
            );
        }
    }

    #[test]
    fn reader_storage_compatibility_only_advances_monotonically() {
        assert_eq!(
            ReaderStorageCompatibility::LegacyEqualityUnion
                .advance(ReaderStorageCompatibility::LegacyEqualityUnion)
                .unwrap(),
            ReaderStorageCompatibility::LegacyEqualityUnion
        );
        assert_eq!(
            ReaderStorageCompatibility::LegacyEqualityUnion
                .advance(ReaderStorageCompatibility::Current)
                .unwrap(),
            ReaderStorageCompatibility::Current
        );
        assert_eq!(
            ReaderStorageCompatibility::Current
                .advance(ReaderStorageCompatibility::Current)
                .unwrap(),
            ReaderStorageCompatibility::Current
        );
        assert!(matches!(
            ReaderStorageCompatibility::Current
                .advance(ReaderStorageCompatibility::LegacyEqualityUnion),
            Err(HelixDbError::MigrationRequired { .. })
        ));
    }

    #[test]
    fn storage_version_one_requires_migration() {
        let version_one =
            IndexStorageVersion::new(0x0001).expect("storage version one remains representable");
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(version_one));
        let error = validate_bootstrap_values(&marker, None, None)
            .expect_err("storage version one requires migration");
        let HelixDbError::MigrationRequired { reason } = error else {
            panic!("storage version one returns MigrationRequired");
        };
        assert_eq!(
            reason,
            format!(
                "index storage version {} predates required version {}; recreate this development database",
                version_one.get(),
                IndexStorageVersion::CURRENT.get()
            )
        );
    }

    #[test]
    fn storage_version_after_max_supported_is_rejected() {
        let unsupported = IndexStorageVersion::new(IndexStorageVersion::MAX_SUPPORTED.get() + 1)
            .expect("the next storage version remains representable");
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(unsupported));
        assert!(matches!(
            validate_bootstrap_values(&marker, None, None),
            Err(HelixDbError::UnsupportedIndexStorageVersion {
                found: 0x0007,
                supported: 0x0006,
            })
        ));
    }

    async fn put_bootstrap_tuple(db: &Db, version: IndexStorageVersion) {
        db.put(
            global_key(GlobalKey::StorageVersion),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(version)),
        )
        .await
        .unwrap();
        db.put(
            global_key(GlobalKey::LogicalIndexIdWatermark),
            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
        db.put(
            global_key(GlobalKey::VectorPhysicalIdWatermark),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
    }

    async fn all_rows(db: &Db) -> Vec<(Bytes, Bytes)> {
        let mut rows = db.scan(..).await.unwrap();
        let mut collected = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            collected.push((row.key, row.value));
        }
        collected
    }

    #[tokio::test]
    async fn rejected_writer_preflight_preserves_every_byte() {
        for rejected_state in ["malformed", "partial", "v1", "unsupported"] {
            let db = Db::builder(
                format!("writer-preflight-preserves-{rejected_state}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap();
            let tenant = TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3").unwrap();
            assert_eq!(tenant.as_u128().to_be_bytes()[0], 0x01);
            let logical =
                DataKeyKind::NodeProperty(crate::encoding::v2::keys::NodePropertyKey::new(11));
            let mut tenant_key = Vec::new();
            tenant_key.extend_from_slice(&tenant.as_u128().to_be_bytes());
            logical.encode_into(&mut tenant_key);
            db.put(&tenant_key, Bytes::from_static(b"tenant-row"))
                .await
                .unwrap();

            match rejected_state {
                "malformed" => {
                    put_bootstrap_tuple(&db, IndexStorageVersion::CURRENT).await;
                    db.put(
                        global_key(GlobalKey::StorageVersion),
                        Bytes::from_static(b"malformed"),
                    )
                    .await
                    .unwrap();
                }
                "partial" => {
                    db.put(
                        global_key(GlobalKey::StorageVersion),
                        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                            IndexStorageVersion::CURRENT,
                        )),
                    )
                    .await
                    .unwrap();
                }
                "v1" => {
                    put_bootstrap_tuple(&db, IndexStorageVersion::new(0x0001).unwrap()).await;
                }
                "unsupported" => {
                    put_bootstrap_tuple(
                        &db,
                        IndexStorageVersion::new(IndexStorageVersion::MAX_SUPPORTED.get() + 1)
                            .unwrap(),
                    )
                    .await;
                }
                _ => unreachable!(),
            }
            let before = all_rows(&db).await;

            assert!(bootstrap_writer(&db).await.is_err());

            assert_eq!(
                all_rows(&db).await,
                before,
                "rejected state: {rejected_state}"
            );
            assert!(!crate::migrations::tenant_key_envelope_ready(&db)
                .await
                .unwrap());
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn markerless_bootstrap_resumes_after_tenant_migration_completed() {
        let db = Db::builder(
            "markerless-bootstrap-after-tenant-migration",
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .unwrap();
        let scope =
            DataScope::Tenant(TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3").unwrap());
        let migrated_key = ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::operation(IndexOperationId::from_bytes([0x11; 16]).unwrap()),
        }
        .to_bytes();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction
            .put(&migrated_key, Bytes::from_static(b"already-migrated"))
            .unwrap();
        crate::migrations::stage_tenant_key_envelope_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();

        assert!(matches!(
            require_reader_bootstrap_or_legacy(&db).await,
            Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            })
        ));
        bootstrap_writer(&db).await.unwrap();

        assert_eq!(
            db.get(migrated_key).await.unwrap(),
            Some(Bytes::from_static(b"already-migrated"))
        );
        let marker = db
            .get(global_key(GlobalKey::StorageVersion))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_metadata_value(&marker).unwrap(),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_migration_readiness_alone_requires_writer_bootstrap() {
        let db = Db::builder(
            "tenant-migration-ready-before-bootstrap",
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_tenant_key_envelope_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();

        assert!(matches!(
            require_reader_bootstrap_or_legacy(&db).await,
            Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            })
        ));
        bootstrap_writer(&db).await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_migration_readiness_beside_legacy_row_fails_closed() {
        let db = Db::builder(
            "tenant-migration-ready-with-legacy-row",
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .unwrap();
        let tenant = TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3").unwrap();
        let kind = DataKeyKind::NodeProperty(crate::encoding::v2::keys::NodePropertyKey::new(11));
        let mut legacy_key = Vec::new();
        legacy_key.extend_from_slice(&tenant.as_u128().to_be_bytes());
        kind.encode_into(&mut legacy_key);
        db.put(legacy_key, Bytes::from_static(b"legacy-row"))
            .await
            .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_tenant_key_envelope_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        let before = all_rows(&db).await;

        assert!(matches!(
            require_reader_bootstrap_or_legacy(&db).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason
                    == "tenant key envelope readiness is inconsistent with a legacy tenant key"
        ));
        assert!(matches!(
            bootstrap_writer(&db).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason
                    == "tenant key envelope readiness is inconsistent with a legacy tenant key"
        ));
        assert_eq!(all_rows(&db).await, before);
        db.close().await.unwrap();
    }

    #[test]
    fn tenant_v2_cursor_uses_the_v2_envelope_parser() {
        let scope = DataScope::Tenant(TenantId::from_u128(7));
        let cursor = ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::operation(IndexOperationId::from_bytes([0x11; 16]).unwrap()),
        }
        .to_bytes();

        assert!(complete_cursor_is_valid(scope, &cursor));
        assert!(!complete_cursor_is_valid(
            DataScope::Tenant(TenantId::from_u128(8)),
            &cursor
        ));
    }

    #[tokio::test]
    async fn markerless_tenant_v2_storage_requires_writer_bootstrap() {
        let db = Db::builder("markerless-tenant-v2-reader", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let scope = DataScope::Tenant(TenantId::from_u128(7));
        db.put(
            ManagedIndexKey::Data {
                scope,
                kind: ScopedKey::operation(IndexOperationId::from_bytes([0x11; 16]).unwrap()),
            }
            .to_bytes(),
            Bytes::from_static(b"markerless"),
        )
        .await
        .unwrap();

        assert!(matches!(
            require_reader_bootstrap_or_legacy(&db).await,
            Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            })
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn markerless_unscoped_legacy_storage_remains_reader_compatible() {
        let db = Db::builder(
            "markerless-unscoped-legacy-reader",
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .unwrap();
        db.put(
            GraphKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::IndexMetadata(MetadataKey::next_node_id_key()),
            }
            .to_bytes(),
            Bytes::copy_from_slice(&1_u64.to_be_bytes()),
        )
        .await
        .unwrap();

        require_reader_bootstrap_or_legacy(&db).await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn markerless_v2_envelopes_fail_closed_for_readers_and_writers() {
        let adversarial_tenant = TenantId::from_u128(u128::from_be_bytes([0xFD; 16]));
        let operation_id = IndexOperationId::from_bytes([0x11; 16]).unwrap();
        let cases = [
            (
                "global",
                ManagedIndexKey::Global {
                    kind: GlobalKey::OperationPointer(operation_id),
                }
                .to_bytes(),
            ),
            (
                "unscoped",
                ManagedIndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::operation(operation_id),
                }
                .to_bytes(),
            ),
            (
                "tenant",
                ManagedIndexKey::Data {
                    scope: DataScope::Tenant(adversarial_tenant),
                    kind: ScopedKey::operation(operation_id),
                }
                .to_bytes(),
            ),
        ];

        for (name, key) in cases {
            let db = Db::builder(format!("markerless-v2-{name}"), Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap();
            db.put(&key, Bytes::from_static(b"markerless"))
                .await
                .unwrap();

            assert!(matches!(
                require_reader_bootstrap_or_legacy(&db).await,
                Err(HelixDbError::WriterMigrationRequired {
                    requirement: WriterMigrationRequirement::IncompleteStorageSchema,
                })
            ));
            assert!(matches!(
                bootstrap_writer(&db).await,
                Err(HelixDbError::MigrationRequired { reason })
                    if reason == "V2 storage rows exist without the complete bootstrap tuple"
            ));
            assert_eq!(
                db.get(&key).await.unwrap(),
                Some(Bytes::from_static(b"markerless"))
            );
            assert_eq!(
                db.get(global_key(GlobalKey::StorageVersion)).await.unwrap(),
                None
            );
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn legacy_tenant_id_starting_with_the_marker_is_migrated() {
        let scope = DataScope::Tenant(TenantId::from_u128(u128::from_be_bytes([0xFD; 16])));
        let DataScope::Tenant(tenant) = scope else {
            unreachable!()
        };
        let kind = DataKeyKind::IndexMetadata(MetadataKey::next_node_id_key());
        let mut legacy_key = Vec::new();
        legacy_key.extend_from_slice(&tenant.as_u128().to_be_bytes());
        kind.encode_into(&mut legacy_key);
        let legacy_key = Bytes::from(legacy_key);
        let current_key = GraphKey::Data { scope, kind }.to_bytes();
        assert_eq!(legacy_key.first().copied(), Some(TENANT_KEY_PREFIX));
        assert_eq!(current_key.first().copied(), Some(TENANT_KEY_PREFIX));
        assert_eq!(
            current_key.len(),
            legacy_key.len() + core::mem::size_of::<u8>()
        );
        let db = Db::builder("adversarial-v1-tenant-prefix", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        db.put(&legacy_key, Bytes::from_static(b"v1-value"))
            .await
            .unwrap();

        assert!(matches!(
            require_reader_bootstrap_or_legacy(&db).await,
            Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            })
        ));
        bootstrap_writer(&db)
            .await
            .expect("writer bootstrap migrates the legacy tenant row");
        assert_eq!(db.get(&legacy_key).await.unwrap(), None);
        assert_eq!(
            db.get(&current_key).await.unwrap(),
            Some(Bytes::from_static(b"v1-value"))
        );
        let marker = db
            .get(global_key(GlobalKey::StorageVersion))
            .await
            .unwrap()
            .expect("writer publishes the bootstrap marker");
        assert_eq!(
            decode_metadata_value(&marker).unwrap(),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );
        assert!(crate::migrations::index_storage_v4_cleanup_ready(&db)
            .await
            .unwrap());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn markerless_v4_cleanup_runs_once_and_publishes_readiness() {
        let db = Db::builder("markerless-v4-cleanup", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        put_bootstrap_tuple(&db, IndexStorageVersion::CURRENT).await;
        assert!(!crate::migrations::index_storage_v4_cleanup_ready(&db)
            .await
            .unwrap());

        bootstrap_writer(&db).await.unwrap();

        assert!(crate::migrations::index_storage_v4_cleanup_ready(&db)
            .await
            .unwrap());
        bootstrap_writer(&db).await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn malformed_or_premature_v4_cleanup_readiness_fails_closed() {
        let orphan = Db::builder("orphan-v4-cleanup", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let transaction = orphan
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_index_storage_v4_cleanup_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        assert!(matches!(
            bootstrap_writer(&orphan).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason == "V2 storage bootstrap is partial"
        ));
        assert_eq!(
            orphan
                .get(global_key(GlobalKey::StorageVersion))
                .await
                .unwrap(),
            None
        );
        orphan.close().await.unwrap();

        let malformed = Db::builder("malformed-v4-cleanup", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        put_bootstrap_tuple(&malformed, IndexStorageVersion::CURRENT).await;
        malformed
            .put(
                GraphKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexMetadata(MetadataKey::new(
                        b"kv_migration_ready:index_storage_v4_cleanup",
                    )),
                }
                .to_bytes(),
                Bytes::from_static(b"invalid"),
            )
            .await
            .unwrap();
        assert!(matches!(
            bootstrap_writer(&malformed).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason == "index storage V4 cleanup readiness marker is malformed"
        ));
        malformed.close().await.unwrap();

        let premature = Db::builder("premature-v4-cleanup", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        put_bootstrap_tuple(&premature, IndexStorageVersion::new(0x0003).unwrap()).await;
        let transaction = premature
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_index_storage_v4_cleanup_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        assert!(matches!(
            bootstrap_writer(&premature).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason
                    == "index storage V4 cleanup is marked complete beside storage version 3"
        ));
        assert!(matches!(
            require_reader_bootstrap_or_legacy(&premature).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason == "index storage V4 cleanup is marked complete beside legacy storage"
        ));
        premature.close().await.unwrap();
    }

    #[test]
    fn bootstrap_tuple_matrix_rejects_every_incomplete_or_cross_typed_shape() {
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        ));
        let logical = encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
            LogicalIndexIdWatermark {
                next_id: IndexId::initial(),
            },
        ));
        let vector = encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
            VectorPhysicalIdWatermark {
                next_id: VectorPhysicalIndexId::initial(),
            },
        ));
        for (candidate_logical, candidate_vector) in [
            (None, None),
            (Some(logical.as_ref()), None),
            (None, Some(vector.as_ref())),
            (Some(vector.as_ref()), Some(logical.as_ref())),
        ] {
            assert!(matches!(
                validate_bootstrap_values(&marker, candidate_logical, candidate_vector),
                Err(HelixDbError::MigrationRequired { .. })
            ));
        }
    }

    #[tokio::test]
    async fn incomplete_older_readers_require_writer_migration_without_writing() {
        for version_number in [0x0002, 0x0003] {
            let db = Db::builder(
                format!("index-lifecycle-reader-migration-{version_number}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap();
            let version = IndexStorageVersion::new(version_number).unwrap();
            put_bootstrap_tuple(&db, version).await;
            let marker_key = global_key(GlobalKey::StorageVersion);
            let marker_before = db.get(&marker_key).await.unwrap().unwrap();

            let before = all_rows(&db).await;
            let error = require_reader_bootstrap_or_legacy(&db)
                .await
                .expect_err("an incomplete older store requires writer-owned migration");

            assert!(matches!(
                error,
                HelixDbError::WriterMigrationRequired {
                    requirement: WriterMigrationRequirement::IncompleteStorageSchema,
                }
            ));
            assert_eq!(db.get(marker_key).await.unwrap().unwrap(), marker_before);
            assert_eq!(all_rows(&db).await, before);
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn current_incomplete_reader_requires_writer_migration() {
        let db = Db::builder("index-v3-reader-migration", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        bootstrap_writer(&db).await.unwrap();

        assert!(matches!(
            require_reader_bootstrap_or_legacy(&db).await,
            Err(HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            })
        ));
    }

    #[test]
    fn partial_v2_tuple_is_not_writer_promotable() {
        let version_two = IndexStorageVersion::new(0x0002).unwrap();
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(version_two));

        assert!(matches!(
            validate_bootstrap_values(&marker, None, None),
            Err(HelixDbError::MigrationRequired { .. })
        ));
    }

    #[tokio::test]
    async fn writer_atomically_upgrades_a_complete_v2_tuple() {
        let db = Db::builder("index-v3-storage-upgrade", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        put_bootstrap_tuple(&db, IndexStorageVersion::new(0x0002).unwrap()).await;

        bootstrap_writer(&db).await.unwrap();

        let marker = db
            .get(global_key(GlobalKey::StorageVersion))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_metadata_value(&marker).unwrap(),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );
    }
}
