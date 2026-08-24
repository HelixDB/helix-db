//! Blocking V3-to-V4 managed-index migration.
//!
//! The V3 marker remains authoritative while tenant envelopes and Active
//! equality generations are rebuilt and verified. Publication is one final
//! marker write; V3 rows are deleted only afterward, so every committed prefix
//! is restart-safe.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::{Db, IsolationLevel};

use crate::encoding::property::decode_properties;
use crate::encoding::v2::keys::scope::DataScope;
#[cfg(test)]
use crate::encoding::v2::keys::scope::TenantId;
#[cfg(feature = "migration-parity")]
use crate::encoding::v2::keys::SecondaryEntryKey;
use crate::encoding::v2::keys::{
    CanonicalSecondaryValue, GlobalKey, IndexEntity, ManagedIndexKey, RecordKind, ScopedKey,
    SecondaryEntryLane, SecondaryEqualityBitmapKey,
};
#[cfg(feature = "migration-parity")]
use crate::encoding::v2::values::decode_metadata_value;
#[cfg(feature = "migration-parity")]
use crate::encoding::v2::values::encode_secondary_entry;
use crate::encoding::v2::values::{
    decode_index_record, decode_operation_record, encode_metadata_value, encode_operation_record,
    SecondaryEqualityBitmapValue,
};
use crate::error::{HelixDbError, Result};

use super::operation::{
    IndexOperationExecutionState, IndexOperationProgress, SecondaryBuildProgress,
    SecondaryBuildStage, SourceScanProgress,
};
#[cfg(test)]
use super::tenant_envelope_migration;
#[cfg(feature = "migration-parity")]
use super::IndexElementKind;
use super::{
    IndexOperationRecord, IndexStateV2, IndexStorageVersion, IndexV2MetadataValue,
    OperationCounters, OperationQueuePointerValue, PhysicalGeneration,
    ValidatedDynamicIndexDefinition, ValidatedSecondaryIndexDefinition,
};

const MIGRATION_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone)]
struct EqualityGeneration {
    scope: DataScope,
    definition: ValidatedSecondaryIndexDefinition,
    index_id: super::IndexId,
    generation: super::IndexGenerationId,
}

#[derive(Debug, Clone)]
struct BuildingEqualityGeneration {
    generation: EqualityGeneration,
    operation_id: super::IndexOperationId,
}

#[derive(Debug, Default)]
struct MigrationCatalog {
    active: Vec<EqualityGeneration>,
    building: Vec<BuildingEqualityGeneration>,
}

/// Stable restart boundaries spanning every durable migration phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EqualityBitmapMigrationFailpoint {
    InitializationBefore,
    InitializationAfter,
    BatchBefore,
    BatchAfter,
    VerificationBefore,
    VerificationAfter,
    PublicationBefore,
    PublicationAfter,
    CleanupBefore,
    CleanupAfter,
}

impl EqualityBitmapMigrationFailpoint {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 10] = [
        Self::InitializationBefore,
        Self::InitializationAfter,
        Self::BatchBefore,
        Self::BatchAfter,
        Self::VerificationBefore,
        Self::VerificationAfter,
        Self::PublicationBefore,
        Self::PublicationAfter,
        Self::CleanupBefore,
        Self::CleanupAfter,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::InitializationBefore => "initialization_before",
            Self::InitializationAfter => "initialization_after",
            Self::BatchBefore => "batch_before",
            Self::BatchAfter => "batch_after",
            Self::VerificationBefore => "verification_before",
            Self::VerificationAfter => "verification_after",
            Self::PublicationBefore => "publication_before",
            Self::PublicationAfter => "publication_after",
            Self::CleanupBefore => "cleanup_before",
            Self::CleanupAfter => "cleanup_after",
        }
    }
}

static INJECTED_FAILPOINT: Mutex<Option<EqualityBitmapMigrationFailpoint>> = Mutex::new(None);
static FAILPOINT_TRIGGERED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn inject_once(failpoint: EqualityBitmapMigrationFailpoint) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation(
            "equality bitmap migration failpoint mutex was poisoned".to_string(),
        )
    })?;
    *injected = Some(failpoint);
    FAILPOINT_TRIGGERED.store(false, Ordering::SeqCst);
    Ok(())
}

#[cfg(test)]
pub(crate) fn was_triggered() -> bool {
    FAILPOINT_TRIGGERED.load(Ordering::SeqCst)
}

fn trip(failpoint: EqualityBitmapMigrationFailpoint) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation(
            "equality bitmap migration failpoint mutex was poisoned".to_string(),
        )
    })?;
    if *injected == Some(failpoint) {
        *injected = None;
        FAILPOINT_TRIGGERED.store(true, Ordering::SeqCst);
        return Err(injected_error(failpoint));
    }
    drop(injected);

    if std::env::var("HELIX_EQUALITY_BITMAP_MIGRATION_FAILPOINT").as_deref()
        != Ok(failpoint.as_str())
    {
        return Ok(());
    }
    if std::env::var("HELIX_EQUALITY_BITMAP_MIGRATION_FAIL_ACTION").as_deref() == Ok("abort") {
        std::process::abort();
    }
    Err(injected_error(failpoint))
}

fn injected_error(failpoint: EqualityBitmapMigrationFailpoint) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "injected equality bitmap migration failpoint {}",
        failpoint.as_str()
    ))
}

pub(super) async fn migrate_v3_to_v4(db: &Db) -> Result<()> {
    trip(EqualityBitmapMigrationFailpoint::InitializationBefore)?;
    let catalog = discover_catalog(db).await?;
    trip(EqualityBitmapMigrationFailpoint::InitializationAfter)?;

    for generation in &catalog.active {
        rebuild_active_generation(db, generation).await?;
    }
    for generation in &catalog.building {
        restart_building_generation(db, generation).await?;
    }

    trip(EqualityBitmapMigrationFailpoint::VerificationBefore)?;
    trip(EqualityBitmapMigrationFailpoint::VerificationAfter)?;

    trip(EqualityBitmapMigrationFailpoint::PublicationBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    transaction.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        )),
    )?;
    transaction.commit().await?;
    trip(EqualityBitmapMigrationFailpoint::PublicationAfter)?;

    cleanup_v3_nonunique_equality_rows(db).await
}

pub(super) async fn cleanup_v3_nonunique_equality_rows(db: &Db) -> Result<()> {
    trip(EqualityBitmapMigrationFailpoint::CleanupBefore)?;
    clear_all_v3_nonunique_equality_rows(db).await?;
    trip(EqualityBitmapMigrationFailpoint::CleanupAfter)?;
    verify_v3_equality_absent_and_publish_cleanup(db).await
}

/// Adds legacy non-unique equality rows and publishes an exact V2/V3 fixture.
///
/// This inverse exists only for the migration-parity and Kind acceptance
/// harnesses. Existing bitmaps remain as a valid partial-migration state so
/// already-open current readers stay correct. The caller must quiesce every
/// write before invoking it.
#[cfg(feature = "migration-parity")]
pub(crate) async fn make_legacy_equality_fixture(db: &Db, version: u16) -> Result<()> {
    if !matches!(version, 2 | 3) {
        return Err(HelixDbError::Config(format!(
            "legacy equality fixture version must be 2 or 3, got {version}"
        )));
    }
    let marker = db
        .get(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
        )
        .await?
        .ok_or_else(|| corruption("legacy fixture source has no storage version"))?;
    let IndexV2MetadataValue::StorageVersion(source_version) = decode_metadata_value(&marker)?
    else {
        return Err(corruption(
            "legacy fixture source has a non-version storage marker",
        ));
    };
    let target_version = IndexStorageVersion::new(version)?;
    if source_version != IndexStorageVersion::CURRENT && source_version != target_version {
        return Err(corruption(
            "legacy fixture source must be current or already at the requested version",
        ));
    }

    let catalog = discover_catalog(db).await?;
    if !catalog.building.is_empty() {
        return Err(corruption(
            "legacy equality fixture cannot contain a building equality generation",
        ));
    }
    for generation in &catalog.active {
        copy_bitmap_generation_to_legacy_entries(db, generation).await?;
    }

    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    transaction.delete(Bytes::from_static(
        b"\xFFkv_migration_ready:index_storage_v4_cleanup",
    ))?;
    transaction.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(target_version)),
    )?;
    transaction.commit().await?;
    Ok(())
}

#[cfg(feature = "migration-parity")]
async fn copy_bitmap_generation_to_legacy_entries(
    db: &Db,
    generation: &EqualityGeneration,
) -> Result<()> {
    let mut rows = db.scan_prefix(bitmap_prefix(generation), ..).await?;
    while let Some(row) = rows.next().await? {
        let ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::SecondaryEqualityBitmap(key),
        } = ManagedIndexKey::parse_from_slice(generation.scope, &row.key)?
        else {
            return Err(corruption("V4 equality prefix yielded another key kind"));
        };
        if scope != generation.scope
            || key.index_id != generation.index_id
            || key.generation != generation.generation
            || key.element_kind != generation.definition.element_kind()
        {
            return Err(corruption("V4 equality row escaped its fixture generation"));
        }
        let lane = match key.element_kind {
            IndexElementKind::Node => SecondaryEntryLane::NodeEquality,
            IndexElementKind::Edge => SecondaryEntryLane::EdgeEquality,
        };
        let ids = SecondaryEqualityBitmapValue::decode(&row.value)?.into_ids();
        if ids.is_empty() {
            return Err(corruption("V4 equality fixture bitmap must not be empty"));
        }
        let mut ids = ids.into_iter();
        loop {
            let batch = ids.by_ref().take(MIGRATION_BATCH_SIZE).collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
            for entity_id in batch {
                let entity_id = super::IndexEntityId::new(entity_id);
                transaction.put(
                    ManagedIndexKey::Data {
                        scope: generation.scope,
                        kind: ScopedKey::SecondaryEntry(SecondaryEntryKey::try_new(
                            generation.index_id,
                            generation.generation,
                            lane,
                            CanonicalSecondaryValue::Equality(key.value.clone()),
                            Some(entity_id),
                        )?),
                    }
                    .to_bytes(),
                    encode_secondary_entry(&super::work::SecondaryEntryValue {
                        index_id: generation.index_id,
                        generation: generation.generation,
                        lane,
                        entity_id,
                    }),
                )?;
            }
            transaction.commit().await?;
        }
    }
    Ok(())
}

async fn discover_catalog(db: &Db) -> Result<MigrationCatalog> {
    let mut catalog = MigrationCatalog::default();
    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        let candidates = index_record_candidates(&row.key)?;
        if candidates.is_empty() {
            continue;
        }
        let record = decode_index_record(&row.value)?;
        let mut matching = candidates.into_iter().filter_map(|(scope, kind)| {
            let ScopedKey::IndexRecord(key) = kind else {
                return None;
            };
            (key.identity == *record.identity()).then_some((scope, key))
        });
        let Some((scope, _)) = matching.next() else {
            return Err(corruption(
                "index-record migration key/value identities disagree",
            ));
        };
        if matching.next().is_some() {
            return Err(corruption(
                "index-record migration key has ambiguous physical scope",
            ));
        }
        let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition() else {
            continue;
        };
        if !super::secondary::definition_uses_equality_bitmap(definition) {
            continue;
        }
        let generation = EqualityGeneration {
            scope,
            definition: definition.clone(),
            index_id: record.index_id(),
            generation: record.state().generation(),
        };
        match record.state() {
            IndexStateV2::Active {
                physical: PhysicalGeneration::Secondary { .. },
                ..
            } => catalog.active.push(generation),
            IndexStateV2::Building {
                physical: PhysicalGeneration::Secondary { .. },
                build_operation_id,
            } => catalog.building.push(BuildingEqualityGeneration {
                generation,
                operation_id: *build_operation_id,
            }),
            IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. }
            | IndexStateV2::Dropped { .. } => {}
            IndexStateV2::Active { .. } | IndexStateV2::Building { .. } => {
                return Err(corruption(
                    "secondary equality record owns another physical family",
                ));
            }
        }
    }
    Ok(catalog)
}

fn index_record_candidates(key: &[u8]) -> Result<Vec<(DataScope, ScopedKey)>> {
    let Ok(ManagedIndexKey::Data { scope, kind }) = ManagedIndexKey::parse_data_from_slice(key)
    else {
        return Ok(Vec::new());
    };
    if matches!(kind, ScopedKey::IndexRecord(_)) {
        Ok(vec![(scope, kind)])
    } else {
        Ok(Vec::new())
    }
}

async fn rebuild_active_generation(db: &Db, generation: &EqualityGeneration) -> Result<()> {
    clear_prefix(db, &bitmap_prefix(generation)).await?;
    let source_prefix =
        super::secondary::source_prefix(generation.scope, generation.definition.element_kind());
    let mut rows = db.scan_prefix(source_prefix, ..).await?;
    loop {
        let mut additions = BTreeMap::<Bytes, RoaringTreemap>::new();
        let mut source_rows = 0;
        while source_rows < MIGRATION_BATCH_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            source_rows += 1;
            let Some(entity_id) = super::secondary::source_entity(
                generation.scope,
                generation.definition.element_kind(),
                &row.key,
            )?
            else {
                continue;
            };
            let Some(key) = authoritative_bitmap_key(generation, entity_id, &row.value)? else {
                continue;
            };
            additions.entry(key).or_default().insert(entity_id.get());
        }
        if source_rows == 0 {
            break;
        }
        if additions.is_empty() {
            continue;
        }

        trip(EqualityBitmapMigrationFailpoint::BatchBefore)?;
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        for (key, ids) in additions {
            transaction.merge(key, SecondaryEqualityBitmapValue::new(ids).encode())?;
        }
        transaction.commit().await?;
        trip(EqualityBitmapMigrationFailpoint::BatchAfter)?;
    }

    trip(EqualityBitmapMigrationFailpoint::VerificationBefore)?;
    verify_graph_to_bitmaps(db, generation).await?;
    verify_bitmaps_to_graph(db, generation).await?;
    trip(EqualityBitmapMigrationFailpoint::VerificationAfter)
}

async fn verify_graph_to_bitmaps(db: &Db, generation: &EqualityGeneration) -> Result<()> {
    let source_prefix =
        super::secondary::source_prefix(generation.scope, generation.definition.element_kind());
    let mut rows = db.scan_prefix(source_prefix, ..).await?;
    loop {
        let mut expected = BTreeMap::<Bytes, RoaringTreemap>::new();
        let mut source_rows = 0;
        while source_rows < MIGRATION_BATCH_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            source_rows += 1;
            let Some(entity_id) = super::secondary::source_entity(
                generation.scope,
                generation.definition.element_kind(),
                &row.key,
            )?
            else {
                continue;
            };
            let Some(key) = authoritative_bitmap_key(generation, entity_id, &row.value)? else {
                continue;
            };
            expected.entry(key).or_default().insert(entity_id.get());
        }
        if source_rows == 0 {
            return Ok(());
        }
        if expected.is_empty() {
            continue;
        }

        let keys = expected.keys().cloned().collect::<Vec<_>>();
        let values = db.multi_get(&keys).await?;
        for ((_, expected_ids), value) in expected.into_iter().zip(values) {
            let Some(value) = value else {
                return Err(corruption(
                    "authoritative graph value is absent from V4 equality bitmaps",
                ));
            };
            let actual = SecondaryEqualityBitmapValue::decode(&value)?.into_ids();
            if !expected_ids.is_subset(&actual) {
                return Err(corruption(
                    "authoritative graph membership is absent from a V4 equality bitmap",
                ));
            }
        }
    }
}

async fn verify_bitmaps_to_graph(db: &Db, generation: &EqualityGeneration) -> Result<()> {
    let prefix = bitmap_prefix(generation);
    let mut rows = db.scan_prefix(prefix, ..).await?;
    while let Some(row) = rows.next().await? {
        let ManagedIndexKey::Data {
            kind: ScopedKey::SecondaryEqualityBitmap(key),
            ..
        } = ManagedIndexKey::parse_from_slice(generation.scope, &row.key)?
        else {
            return Err(corruption("V4 equality prefix yielded another key kind"));
        };
        if key.index_id != generation.index_id
            || key.generation != generation.generation
            || key.element_kind != generation.definition.element_kind()
        {
            return Err(corruption("V4 equality row escaped its generation prefix"));
        }
        let actual = SecondaryEqualityBitmapValue::decode(&row.value)?.into_ids();
        if actual.is_empty() {
            return Err(corruption("V4 equality bitmap must not be empty"));
        }
        let mut entity_ids = actual.iter();
        loop {
            let batch = entity_ids
                .by_ref()
                .take(MIGRATION_BATCH_SIZE)
                .map(super::IndexEntityId::new)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let property_keys = batch
                .iter()
                .map(|entity_id| {
                    super::secondary::authoritative_property_key(
                        generation.scope,
                        IndexEntity {
                            kind: generation.definition.element_kind(),
                            id: *entity_id,
                        },
                    )
                })
                .collect::<Vec<_>>();
            let properties = db.multi_get(&property_keys).await?;
            for (entity_id, properties) in batch.into_iter().zip(properties) {
                let Some(properties) = properties else {
                    return Err(corruption(
                        "V4 equality bitmap contains an absent graph entity",
                    ));
                };
                let expected = authoritative_bitmap_key(generation, entity_id, &properties)?;
                if expected.as_ref() != Some(&row.key) {
                    return Err(corruption(
                        "V4 equality bitmap member differs from authoritative graph state",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn authoritative_bitmap_key(
    generation: &EqualityGeneration,
    entity_id: super::IndexEntityId,
    properties: &[u8],
) -> Result<Option<Bytes>> {
    let properties = decode_properties(properties)?;
    let canonical =
        super::secondary::canonical_value(&generation.definition, &properties, entity_id)
            .map_err(|_| corruption("authoritative equality value cannot be indexed"))?;
    match canonical {
        Some(CanonicalSecondaryValue::Equality(value)) => Ok(Some(
            ManagedIndexKey::Data {
                scope: generation.scope,
                kind: ScopedKey::SecondaryEqualityBitmap(SecondaryEqualityBitmapKey::try_new(
                    generation.index_id,
                    generation.generation,
                    generation.definition.element_kind(),
                    value,
                )?),
            }
            .to_bytes(),
        )),
        Some(CanonicalSecondaryValue::Range(_)) => Err(corruption(
            "authoritative equality generation produced a range value",
        )),
        None => Ok(None),
    }
}

async fn restart_building_generation(db: &Db, building: &BuildingEqualityGeneration) -> Result<()> {
    for prefix in [
        ManagedIndexKey::data_prefix(
            building.generation.scope,
            ScopedKey::generation_prefix(
                RecordKind::SecondaryEntry,
                building.generation.index_id,
                building.generation.generation,
            ),
        ),
        bitmap_prefix(&building.generation),
        ManagedIndexKey::data_prefix(
            building.generation.scope,
            ScopedKey::generation_prefix(
                RecordKind::BuildDelta,
                building.generation.index_id,
                building.generation.generation,
            ),
        ),
        ManagedIndexKey::data_prefix(
            building.generation.scope,
            ScopedKey::generation_prefix(
                RecordKind::AppliedState,
                building.generation.index_id,
                building.generation.generation,
            ),
        ),
    ] {
        clear_prefix(db, &prefix).await?;
    }

    let upper_bound = super::lifecycle::capture_source_upper_bound(
        db,
        building.generation.scope,
        building.generation.definition.element_kind(),
    )
    .await?;
    let operation_key = ManagedIndexKey::Data {
        scope: building.generation.scope,
        kind: ScopedKey::operation(building.operation_id),
    }
    .to_bytes();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let operation_bytes = transaction.get(&operation_key).await?;
    let Some(operation_bytes) = operation_bytes else {
        return Err(corruption(
            "Building equality generation has no lifecycle operation",
        ));
    };
    let operation = decode_operation_record(&operation_bytes)?;
    if operation.operation_id() != building.operation_id
        || operation.index_id() != building.generation.index_id
        || operation.generation() != building.generation.generation
    {
        return Err(corruption(
            "Building equality generation operation ownership mismatch",
        ));
    }
    let restarted = IndexOperationRecord::try_new(
        operation.operation_id(),
        operation.index_id(),
        operation.identity().clone(),
        operation.generation(),
        operation.index_record_revision(),
        operation.operation_revision().checked_next()?,
        operation.kind(),
        operation.family(),
        IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
            SecondaryBuildStage::Scan(SourceScanProgress {
                inclusive_upper_bound: upper_bound,
                cursor: None,
                counters: OperationCounters::default(),
            }),
        )),
        operation.attempt(),
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .map_err(|error| corruption(&error.to_string()))?;
    transaction.put(operation_key, encode_operation_record(&restarted))?;
    transaction.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::OperationPointer(building.operation_id),
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
            OperationQueuePointerValue {
                scope: building.generation.scope,
                index_id: building.generation.index_id,
                generation: building.generation.generation,
                record_revision: restarted.operation_revision(),
            },
        )),
    )?;
    transaction.commit().await?;
    Ok(())
}

async fn clear_prefix(db: &Db, prefix: &Bytes) -> Result<()> {
    loop {
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mut rows = transaction.scan_prefix(prefix, ..).await?;
        let mut keys = Vec::with_capacity(MIGRATION_BATCH_SIZE);
        while keys.len() < MIGRATION_BATCH_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            keys.push(row.key);
        }
        if keys.is_empty() {
            transaction.rollback();
            return Ok(());
        }
        for key in keys {
            transaction.delete(key)?;
        }
        trip(EqualityBitmapMigrationFailpoint::BatchBefore)?;
        transaction.commit().await?;
        trip(EqualityBitmapMigrationFailpoint::BatchAfter)?;
    }
}

async fn clear_all_v3_nonunique_equality_rows(db: &Db) -> Result<()> {
    let mut rows = db.scan(..).await?;
    let mut keys = Vec::with_capacity(MIGRATION_BATCH_SIZE);
    while let Some(row) = rows.next().await? {
        if !is_v3_nonunique_equality_key(&row.key) {
            continue;
        }
        keys.push(row.key);
        if keys.len() == MIGRATION_BATCH_SIZE {
            delete_v3_equality_keys(db, core::mem::take(&mut keys)).await?;
        }
    }
    if !keys.is_empty() {
        delete_v3_equality_keys(db, keys).await?;
    }
    Ok(())
}

async fn delete_v3_equality_keys(db: &Db, keys: Vec<Bytes>) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    for key in keys {
        transaction.delete(key)?;
    }
    trip(EqualityBitmapMigrationFailpoint::BatchBefore)?;
    transaction.commit().await?;
    trip(EqualityBitmapMigrationFailpoint::BatchAfter)
}

async fn verify_v3_equality_absent_and_publish_cleanup(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let mut rows = transaction.scan(..).await?;
    while let Some(row) = rows.next().await? {
        if is_v3_nonunique_equality_key(&row.key) {
            return Err(corruption(
                "V3 non-unique equality row remained after global cleanup",
            ));
        }
    }
    crate::migrations::stage_index_storage_v4_cleanup_ready(&transaction)?;
    transaction.commit().await?;
    Ok(())
}

fn is_v3_nonunique_equality_key(key: &[u8]) -> bool {
    let Ok(ManagedIndexKey::Data {
        kind: ScopedKey::SecondaryEntry(entry),
        ..
    }) = ManagedIndexKey::parse_data_from_slice(key)
    else {
        return false;
    };
    matches!(
        entry.lane(),
        SecondaryEntryLane::NodeEquality | SecondaryEntryLane::EdgeEquality
    ) && entry.entity_id().is_some()
}

fn bitmap_prefix(generation: &EqualityGeneration) -> Bytes {
    ManagedIndexKey::data_prefix(
        generation.scope,
        ScopedKey::secondary_equality_bitmap_prefix(
            generation.index_id,
            generation.generation,
            generation.definition.element_kind(),
        ),
    )
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use helix_ast::batch::read_batch;
    use helix_ast::expr::Predicate;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;
    use helix_ast::value::PropertyValue as AstPropertyValue;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;

    use super::*;
    use crate::config::SecondaryIndexDefinition;
    use crate::encoding::property::{encode_properties, Property};
    use crate::encoding::v2::keys::{
        CanonicalSecondaryValue, SecondaryEntryKey, SecondaryEntryLane,
    };
    use crate::encoding::v2::keys::{
        DataKey as GraphKey, DataKeyKind, EdgePropertyByIdKey, NodePropertyKey,
    };
    use crate::encoding::v2::values::{
        decode_metadata_value, encode_index_record, encode_secondary_entry,
    };
    use crate::index_lifecycle::work::SecondaryEntryValue;
    use crate::index_lifecycle::{
        IndexGenerationId, IndexId, IndexOperationFamily, IndexOperationKind,
        IndexOperationRevision, IndexRecordV2, IndexRevision, LogicalIndexIdWatermark,
        PrefixScanProgress, VectorPhysicalIdWatermark, VectorPhysicalIndexId,
    };

    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn v3_db(name: &str) -> Db {
        legacy_db_with_store(name, Arc::new(InMemory::new()), 3).await
    }

    async fn v3_db_with_store(name: &str, store: Arc<dyn ObjectStore>) -> Db {
        legacy_db_with_store(name, store, 3).await
    }

    async fn legacy_db_with_store(name: &str, store: Arc<dyn ObjectStore>, version: u16) -> Db {
        let db = Db::builder(name, store)
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(version).unwrap(),
            )),
        )
        .await
        .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::LogicalIndexIdWatermark,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::new(2).unwrap(),
                },
            )),
        )
        .await
        .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::VectorPhysicalIdWatermark,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
        db
    }

    async fn assert_reader_queries(reader: &crate::HelixDB) {
        let response = reader
            .query(QueryRequest::read(
                read_batch()
                    .var_as(
                        "point",
                        g().n_with_label_where("User", Predicate::eq("email", "shared"))
                            .count(),
                    )
                    .var_as(
                        "membership",
                        g().n_with_label_where(
                            "User",
                            Predicate::is_in(
                                "email",
                                AstPropertyValue::StringArray(vec![
                                    "shared".to_string(),
                                    "missing".to_string(),
                                ]),
                            ),
                        )
                        .count(),
                    )
                    .var_as("ordinary", g().n_with_label("User").count())
                    .returning(["point", "membership", "ordinary"]),
            ))
            .await
            .expect("reader queries remain executable through V3-to-V4 migration");
        assert_eq!(response["point"], serde_json::Value::from(2));
        assert_eq!(response["membership"], serde_json::Value::from(2));
        assert_eq!(response["ordinary"], serde_json::Value::from(2));
    }

    async fn assert_node_and_edge_reader_queries(reader: &crate::HelixDB) {
        let values =
            AstPropertyValue::StringArray(vec!["shared".to_string(), "missing".to_string()]);
        let response = reader
            .query(QueryRequest::read(
                read_batch()
                    .var_as(
                        "node_point",
                        g().n_with_label_where("User", Predicate::eq("email", "shared"))
                            .count(),
                    )
                    .var_as(
                        "node_membership",
                        g().n_with_label_where("User", Predicate::is_in("email", values.clone()))
                            .count(),
                    )
                    .var_as(
                        "edge_point",
                        g().e_with_label_where("KNOWS", Predicate::eq("kind", "shared"))
                            .count(),
                    )
                    .var_as(
                        "edge_membership",
                        g().e_with_label_where("KNOWS", Predicate::is_in("kind", values))
                            .count(),
                    )
                    .var_as("nodes", g().n_with_label("User").count())
                    .var_as("edges", g().e_with_label("KNOWS").count())
                    .returning([
                        "node_point",
                        "node_membership",
                        "edge_point",
                        "edge_membership",
                        "nodes",
                        "edges",
                    ]),
            ))
            .await
            .expect("complete legacy reader executes indexed and ordinary reads");
        for name in [
            "node_point",
            "node_membership",
            "edge_point",
            "edge_membership",
            "nodes",
            "edges",
        ] {
            assert_eq!(response[name], serde_json::Value::from(2), "{name}");
        }
    }

    fn equality_definition() -> ValidatedDynamicIndexDefinition {
        SecondaryIndexDefinition::node_equality("User", "email")
            .unwrap()
            .try_into()
            .unwrap()
    }

    async fn put_graph_entity(db: &Db, scope: DataScope, entity_id: u64, email: &str) {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                GraphKey::Data {
                    scope,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
                }
                .to_bytes(),
                encode_properties(&[
                    Property::string("$label", "User"),
                    Property::string("email", email),
                ]),
            )
            .unwrap();
        crate::search::add_to_equality_index_scoped(
            &transaction,
            "$label",
            "User",
            entity_id,
            scope,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
    }

    async fn put_edge_graph_entity(db: &Db, scope: DataScope, entity_id: u64, kind: &str) {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                GraphKey::Data {
                    scope,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity_id)),
                }
                .to_bytes(),
                encode_properties(&[
                    Property::string("$label", "KNOWS"),
                    Property::string("kind", kind),
                ]),
            )
            .unwrap();
        crate::search::store_edge_endpoints_scoped(
            &transaction,
            entity_id,
            entity_id + 100,
            entity_id + 200,
            scope,
        )
        .await
        .unwrap();
        crate::search::add_to_global_edge_label_index_scoped(
            &transaction,
            "KNOWS",
            entity_id,
            scope,
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
    }

    async fn put_v3_entry(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
        entity_id: u64,
        email: &str,
    ) {
        let entity_id = super::super::IndexEntityId::new(entity_id);
        let key = tenant_envelope_migration::legacy_data_key(
            scope,
            ScopedKey::SecondaryEntry(
                SecondaryEntryKey::try_new(
                    index_id,
                    generation,
                    SecondaryEntryLane::NodeEquality,
                    CanonicalSecondaryValue::equality_string(email),
                    Some(entity_id),
                )
                .unwrap(),
            ),
        );
        db.put(
            key,
            encode_secondary_entry(&SecondaryEntryValue {
                index_id,
                generation,
                lane: SecondaryEntryLane::NodeEquality,
                entity_id,
            }),
        )
        .await
        .unwrap();
    }

    async fn put_v3_edge_entry(
        db: &Db,
        generation: &EqualityGeneration,
        entity_id: u64,
        kind: &str,
    ) {
        let entity_id = super::super::IndexEntityId::new(entity_id);
        let lane = SecondaryEntryLane::EdgeEquality;
        let key = tenant_envelope_migration::legacy_data_key(
            generation.scope,
            ScopedKey::SecondaryEntry(
                SecondaryEntryKey::try_new(
                    generation.index_id,
                    generation.generation,
                    lane,
                    CanonicalSecondaryValue::equality_string(kind),
                    Some(entity_id),
                )
                .unwrap(),
            ),
        );
        db.put(
            key,
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: generation.index_id,
                generation: generation.generation,
                lane,
                entity_id,
            }),
        )
        .await
        .unwrap();
    }

    async fn put_active_index(db: &Db, scope: DataScope) -> EqualityGeneration {
        put_active_definition(db, scope, equality_definition(), IndexId::initial()).await
    }

    async fn put_active_definition(
        db: &Db,
        scope: DataScope,
        definition: ValidatedDynamicIndexDefinition,
        index_id: IndexId,
    ) -> EqualityGeneration {
        let generation = IndexGenerationId::initial();
        let building = IndexRecordV2::building(
            index_id,
            definition.clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary { generation },
            super::super::IndexOperationId::new_v4(),
        )
        .unwrap();
        let active = building
            .transition(super::super::IndexStateTransition::Activate)
            .unwrap();
        db.put(
            tenant_envelope_migration::legacy_data_key(
                scope,
                ScopedKey::index_record(definition.identity()),
            ),
            encode_index_record(&active),
        )
        .await
        .unwrap();
        let ValidatedDynamicIndexDefinition::Secondary(definition) = definition else {
            unreachable!()
        };
        EqualityGeneration {
            scope,
            definition,
            index_id,
            generation,
        }
    }

    async fn setup_active_fixture(name: &str) -> (Db, EqualityGeneration) {
        let db = v3_db(name).await;
        let generation = put_active_index(&db, DataScope::LegacyUnscoped).await;
        for entity_id in 0..2 {
            put_graph_entity(&db, generation.scope, entity_id, "shared").await;
            put_v3_entry(
                &db,
                generation.scope,
                generation.index_id,
                generation.generation,
                entity_id,
                "shared",
            )
            .await;
        }
        (db, generation)
    }

    #[tokio::test]
    async fn complete_v2_and_v3_readers_query_legacy_node_and_edge_equality() {
        for version in [2, 3] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = format!("reader-compatible-storage-v{version}");
            let db = legacy_db_with_store(&database, Arc::clone(&store), version).await;
            let node = put_active_index(&db, DataScope::LegacyUnscoped).await;
            let edge = put_active_definition(
                &db,
                DataScope::LegacyUnscoped,
                SecondaryIndexDefinition::edge_equality("KNOWS", "kind")
                    .unwrap()
                    .try_into()
                    .unwrap(),
                IndexId::new(2).unwrap(),
            )
            .await;
            for entity_id in 1..=2 {
                put_graph_entity(&db, node.scope, entity_id, "shared").await;
                put_v3_entry(
                    &db,
                    node.scope,
                    node.index_id,
                    node.generation,
                    entity_id,
                    "shared",
                )
                .await;
                let edge_id = entity_id + 10;
                put_edge_graph_entity(&db, edge.scope, edge_id, "shared").await;
                put_v3_edge_entry(&db, &edge, edge_id, "shared").await;
            }
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            crate::migrations::stage_reader_compatible_storage_schema_for_tests(&transaction)
                .unwrap();
            transaction.commit().await.unwrap();
            db.flush().await.unwrap();

            let reader = crate::HelixDB::open_reader_with_object_store_for_tests(
                database,
                Arc::clone(&store),
            )
            .await
            .expect("complete V2/V3 storage opens in explicit union mode");
            assert_eq!(
                reader.reader_storage_compatibility_for_tests(),
                super::super::repository::ReaderStorageCompatibility::LegacyEqualityUnion
            );
            assert_node_and_edge_reader_queries(&reader).await;
            reader.close().await.unwrap();
            db.close().await.unwrap();
        }
    }

    #[cfg(feature = "migration-parity")]
    #[tokio::test]
    async fn migration_parity_fixture_adds_v2_and_v3_rows_without_removing_bitmaps() {
        let _guard = TEST_LOCK.lock().await;
        for version in [2, 3] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = format!("migration-parity-equality-v{version}");
            let db = legacy_db_with_store(&database, Arc::clone(&store), 3).await;
            let generation = put_active_index(&db, DataScope::LegacyUnscoped).await;
            for entity_id in 0..2 {
                put_graph_entity(&db, generation.scope, entity_id, "shared").await;
                put_v3_entry(
                    &db,
                    generation.scope,
                    generation.index_id,
                    generation.generation,
                    entity_id,
                    "shared",
                )
                .await;
            }
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            crate::migrations::stage_reader_compatible_storage_schema_for_tests(&transaction)
                .unwrap();
            transaction.commit().await.unwrap();
            migrate_v3_to_v4(&db).await.unwrap();

            make_legacy_equality_fixture(&db, version).await.unwrap();
            db.flush().await.unwrap();
            assert!(!db
                .scan_prefix(bitmap_prefix(&generation), ..)
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_none());
            assert!(!db
                .scan_prefix(
                    ManagedIndexKey::data_prefix(
                        generation.scope,
                        ScopedKey::generation_prefix(
                            RecordKind::SecondaryEntry,
                            generation.index_id,
                            generation.generation,
                        ),
                    ),
                    ..,
                )
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_none());

            let reader = crate::HelixDB::open_reader_with_object_store_for_tests(
                database,
                Arc::clone(&store),
            )
            .await
            .expect("converted V2/V3 fixture is reader-compatible");
            assert_reader_queries(&reader).await;

            migrate_v3_to_v4(&db).await.unwrap();
            db.flush().await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    assert_reader_queries(&reader).await;
                    if reader.reader_storage_compatibility_for_tests()
                        == super::super::repository::ReaderStorageCompatibility::Current
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the fixture reader switches to current after migration publication");
            assert_active_migrated(&db, &generation).await;

            reader.close().await.unwrap();
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn one_open_reader_remains_correct_across_every_v3_to_v4_boundary() {
        let _guard = TEST_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "v4-open-reader-boundaries";
        let db = v3_db_with_store(database, Arc::clone(&store)).await;
        let generation = put_active_index(&db, DataScope::LegacyUnscoped).await;
        for entity_id in 1..=2 {
            put_graph_entity(&db, generation.scope, entity_id, "shared").await;
            put_v3_entry(
                &db,
                generation.scope,
                generation.index_id,
                generation.generation,
                entity_id,
                "shared",
            )
            .await;
        }
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_reader_compatible_storage_schema_for_tests(&transaction).unwrap();
        transaction.commit().await.unwrap();
        db.flush().await.unwrap();

        let reader =
            crate::HelixDB::open_reader_with_object_store_for_tests(database, Arc::clone(&store))
                .await
                .expect("complete V3 storage opens in union mode");
        assert_eq!(
            reader.reader_storage_compatibility_for_tests(),
            super::super::repository::ReaderStorageCompatibility::LegacyEqualityUnion
        );
        assert_reader_queries(&reader).await;

        inject_once(EqualityBitmapMigrationFailpoint::BatchAfter).unwrap();
        assert!(migrate_v3_to_v4(&db).await.is_err());
        db.flush().await.unwrap();
        assert_reader_queries(&reader).await;

        inject_once(EqualityBitmapMigrationFailpoint::PublicationAfter).unwrap();
        assert!(migrate_v3_to_v4(&db).await.is_err());
        db.flush().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                assert_reader_queries(&reader).await;
                if reader.reader_storage_compatibility_for_tests()
                    == super::super::repository::ReaderStorageCompatibility::Current
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the open reader switches monotonically to current storage");

        cleanup_v3_nonunique_equality_rows(&db).await.unwrap();
        db.flush().await.unwrap();
        assert_reader_queries(&reader).await;

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    async fn assert_active_migrated(db: &Db, generation: &EqualityGeneration) {
        let marker = db
            .get(
                ManagedIndexKey::Global {
                    kind: GlobalKey::StorageVersion,
                }
                .to_bytes(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_metadata_value(&marker).unwrap(),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );
        assert!(crate::migrations::index_storage_v4_cleanup_ready(db)
            .await
            .unwrap());

        let mut rows = db.scan_prefix(bitmap_prefix(generation), ..).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert!(rows.next().await.unwrap().is_none());
        assert_eq!(
            SecondaryEqualityBitmapValue::decode(&row.value)
                .unwrap()
                .into_ids(),
            RoaringTreemap::from_iter([0, 1])
        );
        let v3_prefix = ManagedIndexKey::data_prefix(
            generation.scope,
            ScopedKey::generation_prefix(
                RecordKind::SecondaryEntry,
                generation.index_id,
                generation.generation,
            ),
        );
        assert!(db
            .scan_prefix(v3_prefix, ..)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn active_v3_rows_rebuild_to_one_verified_v4_bitmap() {
        let _guard = TEST_LOCK.lock().await;
        let (db, generation) = setup_active_fixture("v4-equality-active-migration").await;

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        assert_active_migrated(&db, &generation).await;
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn completed_v4_startup_does_not_enter_cleanup() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("completed-v4-startup-skips-cleanup").await;
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::CURRENT,
            )),
        )
        .await
        .unwrap();

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();
        inject_once(EqualityBitmapMigrationFailpoint::CleanupBefore).unwrap();

        super::super::repository::bootstrap_writer(&db)
            .await
            .expect("completed V4 startup skips cleanup discovery");
        assert!(!was_triggered());

        assert!(cleanup_v3_nonunique_equality_rows(&db).await.is_err());
        assert!(was_triggered());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_removes_orphaned_v3_rows_across_every_scope() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("v4-global-orphan-cleanup").await;
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::CURRENT,
            )),
        )
        .await
        .unwrap();
        let tenant =
            DataScope::Tenant(TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3").unwrap());
        put_v3_entry(
            &db,
            DataScope::LegacyUnscoped,
            IndexId::new(31).unwrap(),
            IndexGenerationId::new(41).unwrap(),
            51,
            "orphan-unscoped",
        )
        .await;
        put_v3_entry(
            &db,
            tenant,
            IndexId::new(32).unwrap(),
            IndexGenerationId::new(42).unwrap(),
            52,
            "orphan-tenant",
        )
        .await;

        let edge_id = super::super::IndexEntityId::new(53);
        let edge_kind = ScopedKey::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::new(33).unwrap(),
                IndexGenerationId::new(43).unwrap(),
                SecondaryEntryLane::EdgeEquality,
                CanonicalSecondaryValue::equality_string("orphan-edge"),
                Some(edge_id),
            )
            .unwrap(),
        );
        db.put(
            tenant_envelope_migration::legacy_data_key(tenant, edge_kind),
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: IndexId::new(33).unwrap(),
                generation: IndexGenerationId::new(43).unwrap(),
                lane: SecondaryEntryLane::EdgeEquality,
                entity_id: edge_id,
            }),
        )
        .await
        .unwrap();

        let unique_id = super::super::IndexEntityId::new(54);
        let unique_kind = ScopedKey::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::new(34).unwrap(),
                IndexGenerationId::new(44).unwrap(),
                SecondaryEntryLane::NodeUniqueEquality,
                CanonicalSecondaryValue::equality_string("preserved-unique"),
                None,
            )
            .unwrap(),
        );
        let unique_key = ManagedIndexKey::Data {
            scope: tenant,
            kind: unique_kind.clone(),
        }
        .to_bytes();
        let unique_value = encode_secondary_entry(&SecondaryEntryValue {
            index_id: IndexId::new(34).unwrap(),
            generation: IndexGenerationId::new(44).unwrap(),
            lane: SecondaryEntryLane::NodeUniqueEquality,
            entity_id: unique_id,
        });
        db.put(
            tenant_envelope_migration::legacy_data_key(tenant, unique_kind),
            unique_value.clone(),
        )
        .await
        .unwrap();

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        let mut rows = db.scan(..).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            assert!(
                !is_v3_nonunique_equality_key(&row.key),
                "orphaned V3 row remained at {:?}",
                row.key
            );
        }
        assert_eq!(db.get(unique_key).await.unwrap(), Some(unique_value));
        assert!(crate::migrations::index_storage_v4_cleanup_ready(&db)
            .await
            .unwrap());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn completed_cleanup_is_rechecked_after_tenant_migration() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("v4-cleanup-before-tenant-envelope").await;
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::CURRENT,
            )),
        )
        .await
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_index_storage_v4_cleanup_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        let tenant =
            DataScope::Tenant(TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3").unwrap());
        put_v3_entry(
            &db,
            tenant,
            IndexId::new(35).unwrap(),
            IndexGenerationId::new(45).unwrap(),
            55,
            "orphan-after-cleanup",
        )
        .await;
        assert!(!crate::migrations::tenant_key_envelope_ready(&db)
            .await
            .unwrap());

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        let mut rows = db.scan(..).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            assert!(!is_v3_nonunique_equality_key(&row.key));
        }
        assert!(crate::migrations::tenant_key_envelope_ready(&db)
            .await
            .unwrap());
        assert!(crate::migrations::index_storage_v4_cleanup_ready(&db)
            .await
            .unwrap());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn active_v3_rows_stream_across_multiple_source_batches() {
        let _guard = TEST_LOCK.lock().await;
        const ENTITY_COUNT: u64 = (MIGRATION_BATCH_SIZE as u64 * 2) + 1;
        let db = v3_db("v4-equality-bounded-source-batches").await;
        let generation = put_active_index(&db, DataScope::LegacyUnscoped).await;
        let properties = encode_properties(&[
            Property::string("$label", "User"),
            Property::string("email", "shared"),
        ]);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        for entity_id in 0..ENTITY_COUNT {
            transaction
                .put(
                    GraphKey::Data {
                        scope: generation.scope,
                        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
                    }
                    .to_bytes(),
                    properties.clone(),
                )
                .unwrap();
            let entity_id = super::super::IndexEntityId::new(entity_id);
            transaction
                .put(
                    tenant_envelope_migration::legacy_data_key(
                        generation.scope,
                        ScopedKey::SecondaryEntry(
                            SecondaryEntryKey::try_new(
                                generation.index_id,
                                generation.generation,
                                SecondaryEntryLane::NodeEquality,
                                CanonicalSecondaryValue::equality_string("shared"),
                                Some(entity_id),
                            )
                            .unwrap(),
                        ),
                    ),
                    encode_secondary_entry(&SecondaryEntryValue {
                        index_id: generation.index_id,
                        generation: generation.generation,
                        lane: SecondaryEntryLane::NodeEquality,
                        entity_id,
                    }),
                )
                .unwrap();
        }
        transaction.commit().await.unwrap();

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        let mut rows = db
            .scan_prefix(bitmap_prefix(&generation), ..)
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert!(rows.next().await.unwrap().is_none());
        let bitmap = SecondaryEqualityBitmapValue::decode(&row.value)
            .unwrap()
            .into_ids();
        assert_eq!(bitmap.len(), ENTITY_COUNT);
        assert_eq!(bitmap.min(), Some(0));
        assert_eq!(bitmap.max(), Some(ENTITY_COUNT - 1));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn bounded_verification_rejects_missing_and_extra_membership() {
        let _guard = TEST_LOCK.lock().await;
        let (db, generation) = setup_active_fixture("v4-equality-bounded-verification").await;
        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        let mut rows = db
            .scan_prefix(bitmap_prefix(&generation), ..)
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let mut bitmap = SecondaryEqualityBitmapValue::decode(&row.value)
            .unwrap()
            .into_ids();
        bitmap.remove(1);
        db.put(
            row.key.clone(),
            SecondaryEqualityBitmapValue::new(bitmap.clone()).encode(),
        )
        .await
        .unwrap();
        assert!(matches!(
            verify_graph_to_bitmaps(&db, &generation).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("membership is absent")
        ));

        bitmap.insert(1);
        bitmap.insert(99);
        db.put(row.key, SecondaryEqualityBitmapValue::new(bitmap).encode())
            .await
            .unwrap();
        assert!(matches!(
            verify_bitmaps_to_graph(&db, &generation).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("absent graph entity")
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_catalogs_migrate_without_changing_unrelated_bytes() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("v4-equality-tenant-migration").await;
        let tenant = DataScope::Tenant(TenantId::from_u128(
            0x0601_0000_0000_0000_0000_0000_0000_0000,
        ));
        let unscoped = put_active_index(&db, DataScope::LegacyUnscoped).await;
        let tenant_generation = put_active_index(&db, tenant).await;
        let unique_entity = super::super::IndexEntityId::new(99);
        let unique_kind = ScopedKey::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::new(2).unwrap(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::NodeUniqueEquality,
                CanonicalSecondaryValue::equality_string("unique"),
                None,
            )
            .unwrap(),
        );
        let old_unique_key =
            tenant_envelope_migration::legacy_data_key(tenant, unique_kind.clone());
        let new_unique_key = ManagedIndexKey::Data {
            scope: tenant,
            kind: unique_kind,
        }
        .to_bytes();
        let unique_value = encode_secondary_entry(&SecondaryEntryValue {
            index_id: IndexId::new(2).unwrap(),
            generation: IndexGenerationId::initial(),
            lane: SecondaryEntryLane::NodeUniqueEquality,
            entity_id: unique_entity,
        });
        db.put(old_unique_key.clone(), unique_value.clone())
            .await
            .unwrap();
        for entity_id in 0..2 {
            for generation in [&unscoped, &tenant_generation] {
                put_graph_entity(&db, generation.scope, entity_id, "shared").await;
                put_v3_entry(
                    &db,
                    generation.scope,
                    generation.index_id,
                    generation.generation,
                    entity_id,
                    "shared",
                )
                .await;
            }
        }
        let unrelated_key = Bytes::from_static(b"\xfeunrelated-migration-fixture");
        let unrelated_value = Bytes::from_static(b"preserve-exactly");
        db.put(unrelated_key.clone(), unrelated_value.clone())
            .await
            .unwrap();

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        assert_active_migrated(&db, &unscoped).await;
        assert_active_migrated(&db, &tenant_generation).await;
        assert_eq!(db.get(&new_unique_key).await.unwrap(), Some(unique_value));
        assert_eq!(db.get(&old_unique_key).await.unwrap(), None);
        let old_catalog_key = tenant_envelope_migration::legacy_data_key(
            tenant,
            ScopedKey::index_record(tenant_generation.definition.identity()),
        );
        let new_catalog_key = ManagedIndexKey::Data {
            scope: tenant,
            kind: ScopedKey::index_record(tenant_generation.definition.identity()),
        }
        .to_bytes();
        assert_eq!(db.get(old_catalog_key).await.unwrap(), None);
        assert!(db.get(new_catalog_key).await.unwrap().is_some());
        assert_eq!(db.get(unrelated_key).await.unwrap(), Some(unrelated_value));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_migration_rejects_conflicting_destination_values() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("v4-tenant-envelope-conflict").await;
        let tenant = DataScope::Tenant(TenantId::from_u128(7));
        let _ = put_active_index(&db, tenant).await;
        let kind = ScopedKey::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::new(2).unwrap(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::NodeUniqueEquality,
                CanonicalSecondaryValue::equality_string("unique"),
                None,
            )
            .unwrap(),
        );
        let old_key = tenant_envelope_migration::legacy_data_key(tenant, kind.clone());
        let new_key = ManagedIndexKey::Data {
            scope: tenant,
            kind,
        }
        .to_bytes();
        let value = |entity_id| {
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: IndexId::new(2).unwrap(),
                generation: IndexGenerationId::initial(),
                lane: SecondaryEntryLane::NodeUniqueEquality,
                entity_id: super::super::IndexEntityId::new(entity_id),
            })
        };
        db.put(old_key, value(1)).await.unwrap();
        db.put(new_key, value(2)).await.unwrap();

        assert!(matches!(
            super::super::repository::bootstrap_writer(&db).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("destination conflicts with its legacy source")
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_migration_preserves_managed_rows_without_a_catalog_root() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("v4-tenant-envelope-orphan").await;
        let tenant = DataScope::Tenant(TenantId::from_u128(7));
        let kind = ScopedKey::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::new(2).unwrap(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::NodeUniqueEquality,
                CanonicalSecondaryValue::equality_string("orphan"),
                None,
            )
            .unwrap(),
        );
        let old_key = tenant_envelope_migration::legacy_data_key(tenant, kind.clone());
        let new_key = ManagedIndexKey::Data {
            scope: tenant,
            kind,
        }
        .to_bytes();
        let value = encode_secondary_entry(&SecondaryEntryValue {
            index_id: IndexId::new(2).unwrap(),
            generation: IndexGenerationId::initial(),
            lane: SecondaryEntryLane::NodeUniqueEquality,
            entity_id: super::super::IndexEntityId::new(1),
        });
        db.put(&old_key, value.clone()).await.unwrap();

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();
        assert_eq!(db.get(old_key).await.unwrap(), None);
        assert_eq!(db.get(new_key).await.unwrap(), Some(value));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn every_migration_failpoint_restarts_deterministically() {
        let _guard = TEST_LOCK.lock().await;
        for (ordinal, failpoint) in EqualityBitmapMigrationFailpoint::ALL
            .into_iter()
            .enumerate()
        {
            let (db, generation) =
                setup_active_fixture(&format!("v4-equality-restart-{ordinal}")).await;
            inject_once(failpoint).unwrap();
            assert!(super::super::repository::bootstrap_writer(&db)
                .await
                .is_err());
            assert!(was_triggered());

            super::super::repository::bootstrap_writer(&db)
                .await
                .unwrap();
            assert_active_migrated(&db, &generation).await;
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn incomplete_building_generation_restarts_its_existing_operation() {
        let _guard = TEST_LOCK.lock().await;
        let db = v3_db("v4-equality-building-reset").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = equality_definition();
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let operation_id = super::super::IndexOperationId::new_v4();
        let record = IndexRecordV2::building(
            index_id,
            definition.clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary { generation },
            operation_id,
        )
        .unwrap();
        db.put(
            ManagedIndexKey::Data {
                scope,
                kind: ScopedKey::index_record(definition.identity()),
            }
            .to_bytes(),
            encode_index_record(&record),
        )
        .await
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            operation_id,
            index_id,
            definition.identity(),
            generation,
            record.revision(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::CatchUp(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters {
                        entities: 9,
                        ..OperationCounters::default()
                    },
                }),
            )),
            3,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        db.put(
            ManagedIndexKey::Data {
                scope,
                kind: ScopedKey::operation(operation_id),
            }
            .to_bytes(),
            encode_operation_record(&operation),
        )
        .await
        .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::OperationPointer(operation_id),
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::OperationQueuePointer(
                OperationQueuePointerValue {
                    scope,
                    index_id,
                    generation,
                    record_revision: operation.operation_revision(),
                },
            )),
        )
        .await
        .unwrap();
        put_graph_entity(&db, scope, 0, "shared").await;
        put_v3_entry(&db, scope, index_id, generation, 0, "shared").await;

        super::super::repository::bootstrap_writer(&db)
            .await
            .unwrap();

        let restarted = db
            .get(
                ManagedIndexKey::Data {
                    scope,
                    kind: ScopedKey::operation(operation_id),
                }
                .to_bytes(),
            )
            .await
            .unwrap()
            .map(|bytes| decode_operation_record(&bytes).unwrap())
            .unwrap();
        assert_eq!(restarted.operation_id(), operation_id);
        assert_eq!(restarted.attempt(), 3);
        assert_eq!(restarted.operation_revision().get(), 2);
        assert!(matches!(
            restarted.progress(),
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Scan(SourceScanProgress {
                    cursor: None,
                    counters: OperationCounters { entities: 0, .. },
                    ..
                })
            ))
        ));
        let v3_prefix = ManagedIndexKey::data_prefix(
            scope,
            ScopedKey::generation_prefix(RecordKind::SecondaryEntry, index_id, generation),
        );
        assert!(db
            .scan_prefix(v3_prefix, ..)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none());
        db.close().await.unwrap();
    }
}
