//! Blocking migration from `[tenant_id][logical_key]` to
//! `[0xFD][tenant_id][logical_key]`.
//!
//! The readiness marker is published only after every typed legacy tenant key
//! has an identical typed destination and every source has been removed. The
//! phases are deliberately idempotent so writer startup can resume after any
//! interrupted commit.

use bytes::{BufMut, Bytes};
use slatedb::{Db, IsolationLevel};

use crate::encoding::v2::keys::scope::DataScope;
#[cfg(test)]
use crate::encoding::v2::keys::scope::{TenantId, TENANT_ID_LEN, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::DataKeyKind;
use crate::encoding::v2::keys::{GlobalKey, ScopedKey};
use crate::encoding::v2::legacy::tenant_envelope::LegacyTenantEnvelope;
use crate::encoding::v2::values::{
    decode_applied_state, decode_build_artifact, decode_build_delta, decode_corpus_statistics,
    decode_index_record, decode_manifest_page, decode_manifest_root, decode_operation_record,
    decode_partition_mapping, decode_secondary_entry, decode_statistics_entity,
    decode_term_statistics, decode_text_entity_state, encode_operation_record,
    SecondaryEqualityBitmapValue,
};
use crate::error::{HelixDbError, Result};

use crate::index_lifecycle::IndexCursor;

const MIGRATION_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone)]
enum TenantLogicalKey {
    Graph,
    Managed(ScopedKey),
}

#[derive(Debug)]
struct TenantKeyMigrationRow {
    source_key: Bytes,
    destination_key: Bytes,
    destination_value: Bytes,
}

/// Migrates every typed tenant-owned key before any other writer bootstrap.
pub(crate) async fn migrate_all_tenant_keys(db: &Db) -> Result<()> {
    if crate::migrations::tenant_key_envelope_ready(db).await? {
        return Ok(());
    }

    copy_legacy_tenant_keys(db).await?;
    verify_legacy_tenant_keys(db).await?;
    cleanup_legacy_tenant_keys(db).await?;
    publish_completion(db).await
}

async fn copy_legacy_tenant_keys(db: &Db) -> Result<()> {
    let mut rows = db.scan(..).await?;
    let mut batch = Vec::with_capacity(MIGRATION_BATCH_SIZE);
    while let Some(row) = rows.next().await? {
        let Some(row) = migration_row(row.key, row.value)? else {
            continue;
        };
        batch.push(row);
        if batch.len() == MIGRATION_BATCH_SIZE {
            copy_batch(db, core::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        copy_batch(db, batch).await?;
    }
    Ok(())
}

async fn copy_batch(db: &Db, batch: Vec<TenantKeyMigrationRow>) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let destination_keys = batch
        .iter()
        .map(|row| row.destination_key.clone())
        .collect::<Vec<_>>();
    let destination_values = transaction.multi_get(&destination_keys).await?;
    let mut changed = false;
    for (row, destination) in batch.into_iter().zip(destination_values) {
        match destination {
            Some(destination) if destination == row.destination_value => {}
            Some(_) => {
                return Err(corruption(
                    "tenant key destination conflicts with its legacy source",
                ));
            }
            None => {
                transaction.put(row.destination_key, row.destination_value)?;
                changed = true;
            }
        }
    }
    if changed {
        transaction.commit().await?;
    } else {
        transaction.rollback();
    }
    Ok(())
}

async fn verify_legacy_tenant_keys(db: &Db) -> Result<()> {
    let mut rows = db.scan(..).await?;
    let mut batch = Vec::with_capacity(MIGRATION_BATCH_SIZE);
    while let Some(row) = rows.next().await? {
        let Some(row) = migration_row(row.key, row.value)? else {
            continue;
        };
        batch.push(row);
        if batch.len() == MIGRATION_BATCH_SIZE {
            verify_batch(db, core::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        verify_batch(db, batch).await?;
    }
    Ok(())
}

async fn verify_batch(db: &Db, batch: Vec<TenantKeyMigrationRow>) -> Result<()> {
    let destination_keys = batch
        .iter()
        .map(|row| row.destination_key.clone())
        .collect::<Vec<_>>();
    let destination_values = db.multi_get(&destination_keys).await?;
    if batch
        .into_iter()
        .zip(destination_values)
        .any(|(row, destination)| destination.as_ref() != Some(&row.destination_value))
    {
        return Err(corruption(
            "tenant key destination differs from its legacy source",
        ));
    }
    Ok(())
}

async fn cleanup_legacy_tenant_keys(db: &Db) -> Result<()> {
    let mut rows = db.scan(..).await?;
    let mut batch = Vec::with_capacity(MIGRATION_BATCH_SIZE);
    while let Some(row) = rows.next().await? {
        let Some(row) = migration_row(row.key, row.value)? else {
            continue;
        };
        batch.push(row);
        if batch.len() == MIGRATION_BATCH_SIZE {
            cleanup_batch(db, core::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        cleanup_batch(db, batch).await?;
    }
    Ok(())
}

async fn cleanup_batch(db: &Db, batch: Vec<TenantKeyMigrationRow>) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let destination_keys = batch
        .iter()
        .map(|row| row.destination_key.clone())
        .collect::<Vec<_>>();
    let destination_values = transaction.multi_get(&destination_keys).await?;
    for (row, destination) in batch.into_iter().zip(destination_values) {
        if destination.as_ref() != Some(&row.destination_value) {
            return Err(corruption(
                "tenant key cleanup observed an unverified destination",
            ));
        }
        transaction.delete(row.source_key)?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn publish_completion(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let mut rows = transaction.scan(..).await?;
    while let Some(row) = rows.next().await? {
        if migration_row(row.key, row.value)?.is_some() {
            return Err(corruption(
                "legacy tenant key remained when publishing migration readiness",
            ));
        }
    }
    crate::migrations::stage_tenant_key_envelope_ready(&transaction)?;
    transaction.commit().await?;
    Ok(())
}

fn migration_row(key: Bytes, value: Bytes) -> Result<Option<TenantKeyMigrationRow>> {
    if current_key_is_typed(&key, &value)? {
        return Ok(None);
    }
    let Some(envelope) = LegacyTenantEnvelope::parse_candidate(&key) else {
        return Ok(None);
    };
    let Some(kind) = parse_logical_key(envelope.logical_key(), &value)? else {
        return Ok(None);
    };
    let tenant = envelope.tenant();
    let scope = DataScope::Tenant(tenant);
    let destination_value = migrate_legacy_value(scope, &kind, value)?;
    let mut destination_key = Vec::with_capacity(core::mem::size_of::<u8>() + key.len());
    scope.encode_key_prefix(&mut destination_key);
    destination_key.put_slice(envelope.logical_key());
    Ok(Some(TenantKeyMigrationRow {
        source_key: key,
        destination_key: Bytes::from(destination_key),
        destination_value,
    }))
}

/// Returns whether one markerless row needs the blocking tenant-envelope rewrite.
pub(crate) fn legacy_key_requires_migration(key: Bytes, value: Bytes) -> Result<bool> {
    Ok(migration_row(key, value)?.is_some())
}

fn current_key_is_typed(key: &[u8], _value: &[u8]) -> Result<bool> {
    if GlobalKey::parse_from_slice(key).is_ok() {
        return Ok(true);
    }
    if let Some((_, logical)) = DataScope::strip_tenant_envelope(key)
        && parse_logical_key_without_value(logical)
    {
        return Ok(true);
    }
    Ok(parse_logical_key_without_value(key))
}

fn parse_logical_key(logical: &[u8], value: &[u8]) -> Result<Option<TenantLogicalKey>> {
    if let Ok(kind) = ScopedKey::parse_from_slice(logical) {
        validate_managed_value(&kind, value)?;
        return Ok(Some(TenantLogicalKey::Managed(kind)));
    }
    if DataKeyKind::parse_from_slice(logical).is_ok() {
        return Ok(Some(TenantLogicalKey::Graph));
    }
    Ok(None)
}

fn validate_managed_value(kind: &ScopedKey, value: &[u8]) -> Result<()> {
    match kind {
        ScopedKey::IndexRecord(_) => {
            let _ = decode_index_record(value)?;
        }
        ScopedKey::Operation(_) => {
            let _ = decode_operation_record(value)?;
        }
        ScopedKey::BuildDelta(_) => {
            let _ = decode_build_delta(value)?;
        }
        ScopedKey::AppliedState(_) => {
            let _ = decode_applied_state(value)?;
        }
        ScopedKey::SecondaryEntry(key) => {
            let _ = decode_secondary_entry(key.lane(), value)?;
        }
        ScopedKey::SecondaryEqualityBitmap(_) => {
            let _ = SecondaryEqualityBitmapValue::decode(value)?;
        }
        ScopedKey::TextManifestRoot(_) => {
            let _ = decode_manifest_root(value)?;
        }
        ScopedKey::TextManifestPage(_) => {
            let _ = decode_manifest_page(value)?;
        }
        ScopedKey::TextBuildArtifact(_) => {
            let _ = decode_build_artifact(value)?;
        }
        ScopedKey::TextEntityState(_) => {
            let _ = decode_text_entity_state(value)?;
        }
        ScopedKey::VectorPartitionMapping(_) => {
            let _ = decode_partition_mapping(value)?;
        }
        ScopedKey::TextCorpusStatistics(_) => {
            let _ = decode_corpus_statistics(value)?;
        }
        ScopedKey::TextTermStatistics(_) => {
            let _ = decode_term_statistics(value)?;
        }
        ScopedKey::TextStatisticsEntity(_) => {
            let _ = decode_statistics_entity(value)?;
        }
    }
    Ok(())
}

fn migrate_legacy_value(scope: DataScope, kind: &TenantLogicalKey, value: Bytes) -> Result<Bytes> {
    let TenantLogicalKey::Managed(ScopedKey::Operation(_)) = kind else {
        return Ok(value);
    };
    let operation = decode_operation_record(&value)?;
    let mut changed = false;
    let migrated = operation.try_map_cursors(|cursor| {
        let migrated = migrate_legacy_cursor(scope, cursor)?;
        changed |= &migrated != cursor;
        Ok::<IndexCursor, HelixDbError>(migrated)
    })?;
    if changed {
        Ok(encode_operation_record(&migrated))
    } else {
        Ok(value)
    }
}

fn migrate_legacy_cursor(scope: DataScope, cursor: &IndexCursor) -> Result<IndexCursor> {
    if GlobalKey::parse_from_slice(cursor.as_bytes()).is_ok() {
        return Ok(cursor.clone());
    }
    if let Some((cursor_tenant, logical)) = DataScope::strip_tenant_envelope(cursor.as_bytes())
        && scope == DataScope::Tenant(cursor_tenant)
        && parse_logical_key_without_value(logical)
    {
        return Ok(cursor.clone());
    }
    let DataScope::Tenant(tenant) = scope else {
        return Err(corruption("legacy tenant cursor has an unscoped owner"));
    };
    let bytes = cursor.as_bytes();
    let Some(envelope) = LegacyTenantEnvelope::parse_candidate(bytes) else {
        return Err(corruption("legacy tenant cursor is not a typed tenant key"));
    };
    if envelope.tenant() != tenant || !parse_logical_key_without_value(envelope.logical_key()) {
        return Err(corruption("legacy tenant cursor is not a typed tenant key"));
    }
    let mut migrated = Vec::with_capacity(core::mem::size_of::<u8>() + bytes.len());
    scope.encode_key_prefix(&mut migrated);
    migrated.put_slice(envelope.logical_key());
    IndexCursor::try_new(Bytes::from(migrated)).map_err(|error| corruption(&error.to_string()))
}

fn parse_logical_key_without_value(logical: &[u8]) -> bool {
    ScopedKey::parse_from_slice(logical).is_ok() || DataKeyKind::parse_from_slice(logical).is_ok()
}

#[cfg(test)]
pub(crate) fn legacy_data_key(scope: DataScope, kind: ScopedKey) -> Bytes {
    let mut logical = Vec::with_capacity(kind.encoded_len());
    kind.encode_into(&mut logical);
    match scope {
        DataScope::LegacyUnscoped => Bytes::from(logical),
        DataScope::Tenant(tenant) => {
            crate::encoding::v2::legacy::tenant_envelope::encode_for_contract(tenant, &logical)
        }
    }
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::indexes::range::RangeIndexDirection;
    use crate::encoding::v2::keys::{
        CanonicalSecondaryValue, IndexEntity, IndexEntityStateKey, IndexOperationKey,
        ManagedIndexKey, PartitionFingerprint, SecondaryEntryKey, SecondaryEntryLane,
        TextBuildArtifactKey, TextEntityStateKey, TextManifestRootKey, VectorPartitionMappingKey,
    };
    use crate::encoding::v2::keys::{DataKey as GraphKey, DataKeyKind, NodePropertyKey};
    use crate::encoding::v2::values::{encode_build_delta, encode_operation_record};
    use crate::index_lifecycle::work::CoalescedBuildDeltaValue;
    use crate::index_lifecycle::{
        IndexComponent, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexIdentity,
        IndexIdentityFamily, IndexOperationExecutionState, IndexOperationFamily, IndexOperationId,
        IndexOperationProgress, IndexOperationRecord, IndexOperationRevision, IndexRevision,
        OperationCounters, PrefixScanProgress, SecondaryBuildProgress, SecondaryBuildStage,
        SecondaryCleanupProgress, TextBuildProgress, TextBuildStage, TextCleanupProgress,
        VectorBuildProgress, VectorBuildStage, VectorCleanupProgress,
    };

    fn index_id() -> IndexId {
        IndexId::new(7).unwrap()
    }

    fn generation() -> IndexGenerationId {
        IndexGenerationId::new(9).unwrap()
    }

    fn identity(family: IndexOperationFamily) -> IndexIdentity {
        let family = match family {
            IndexOperationFamily::Secondary => IndexIdentityFamily::SecondaryRange,
            IndexOperationFamily::Vector => IndexIdentityFamily::Vector,
            IndexOperationFamily::Text => IndexIdentityFamily::Text,
        };
        IndexIdentity::new(
            family,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Document").unwrap(),
            IndexComponent::try_new("property", "value").unwrap(),
        )
    }

    fn operation(ordinal: u8, progress: IndexOperationProgress) -> IndexOperationRecord {
        let family = progress.family();
        let kind = progress.kind();
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([ordinal; 16]).unwrap(),
            index_id(),
            identity(family),
            generation(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            kind,
            family,
            progress,
            1,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap()
    }

    fn migrated_cursor(scope: DataScope, kind: ScopedKey) -> (IndexCursor, Bytes) {
        let legacy = IndexCursor::try_new(legacy_data_key(scope, kind.clone())).unwrap();
        let current = ManagedIndexKey::Data { scope, kind }.to_bytes();
        (legacy, current)
    }

    fn prefix(cursor: IndexCursor) -> PrefixScanProgress {
        PrefixScanProgress {
            cursor: Some(cursor),
            counters: OperationCounters::default(),
        }
    }

    fn operation_cases(scope: DataScope) -> Vec<(IndexOperationRecord, Bytes)> {
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(11),
        };
        let applied = || {
            ScopedKey::AppliedState(IndexEntityStateKey {
                index_id: index_id(),
                generation: generation(),
                entity,
            })
        };
        let (secondary_build, secondary_build_current) = migrated_cursor(scope, applied());
        let (vector_build, vector_build_current) = migrated_cursor(scope, applied());
        let mapping = || {
            ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
                index_id: index_id(),
                generation: generation(),
                partition: PartitionFingerprint::new([0x21; 32]),
            })
        };
        let (vector_cleanup, vector_cleanup_current) = migrated_cursor(scope, mapping());
        let range = ScopedKey::SecondaryEntry(
            SecondaryEntryKey::try_new(
                index_id(),
                generation(),
                SecondaryEntryLane::NodeRangeAscending,
                CanonicalSecondaryValue::range_string(RangeIndexDirection::Asc, "shared"),
                Some(entity.id),
            )
            .unwrap(),
        );
        let (secondary_cleanup, secondary_cleanup_current) = migrated_cursor(scope, range);
        let root = TextManifestRootKey {
            index_id: index_id(),
            generation: generation(),
            partition: PartitionFingerprint::new([0x22; 32]),
        };
        let (text_build, text_build_current) = migrated_cursor(
            scope,
            ScopedKey::TextBuildArtifact(TextBuildArtifactKey { root, ordinal: 3 }),
        );
        let (text_cleanup, text_cleanup_current) = migrated_cursor(
            scope,
            ScopedKey::TextEntityState(TextEntityStateKey { root, entity }),
        );

        vec![
            (
                operation(
                    1,
                    IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                        SecondaryBuildStage::Validate(prefix(secondary_build)),
                    )),
                ),
                secondary_build_current,
            ),
            (
                operation(
                    2,
                    IndexOperationProgress::SecondaryCleanup(
                        SecondaryCleanupProgress::DeleteEntries(prefix(secondary_cleanup)),
                    ),
                ),
                secondary_cleanup_current,
            ),
            (
                operation(
                    3,
                    IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                        VectorBuildStage::ValidateDescriptor(prefix(vector_build)),
                    )),
                ),
                vector_build_current,
            ),
            (
                operation(
                    4,
                    IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeletePhysical(
                        prefix(vector_cleanup),
                    )),
                ),
                vector_cleanup_current,
            ),
            (
                operation(
                    5,
                    IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                        TextBuildStage::PrepareManifests(prefix(text_build)),
                    )),
                ),
                text_build_current,
            ),
            (
                operation(
                    6,
                    IndexOperationProgress::TextCleanup(TextCleanupProgress::DeleteMetadata(
                        prefix(text_cleanup),
                    )),
                ),
                text_cleanup_current,
            ),
        ]
    }

    #[test]
    fn managed_cursors_are_reframed_for_every_lifecycle_family() {
        let scope = DataScope::Tenant(TenantId::from_u128(0xABCD));
        for (operation, expected_cursor) in operation_cases(scope) {
            assert!(
                !crate::index_lifecycle::repository::operation_record_cursors_are_valid(
                    scope, &operation,
                )
            );
            let encoded = encode_operation_record(&operation);
            let migrated = migrate_legacy_value(
                scope,
                &TenantLogicalKey::Managed(ScopedKey::operation(operation.operation_id())),
                encoded,
            )
            .unwrap();
            let migrated = decode_operation_record(&migrated).unwrap();

            assert_eq!(
                migrated.operation_revision(),
                operation.operation_revision()
            );
            assert_eq!(migrated.execution_state(), operation.execution_state());
            assert!(
                crate::index_lifecycle::repository::operation_record_cursors_are_valid(
                    scope, &migrated,
                )
            );
            assert!(migrated
                .progress()
                .cursors_are_valid(|cursor| cursor.as_bytes() == &expected_cursor));
        }
    }

    #[test]
    fn current_graph_cursors_and_non_operation_values_remain_zero_copy() {
        let scope = DataScope::Tenant(TenantId::from_u128(0xABCD));
        let graph_cursor = IndexCursor::try_new(
            GraphKey::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(11)),
            }
            .to_bytes(),
        )
        .unwrap();
        let operation = operation(
            7,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Scan(crate::index_lifecycle::SourceScanProgress {
                    inclusive_upper_bound: graph_cursor.clone(),
                    cursor: Some(graph_cursor),
                    counters: OperationCounters::default(),
                }),
            )),
        );
        let encoded = encode_operation_record(&operation);
        let encoded_pointer = encoded.as_ptr();
        let migrated = migrate_legacy_value(
            scope,
            &TenantLogicalKey::Managed(ScopedKey::operation(operation.operation_id())),
            encoded,
        )
        .unwrap();
        assert_eq!(migrated.as_ptr(), encoded_pointer);

        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(11),
        };
        let kind = ScopedKey::BuildDelta(IndexEntityStateKey {
            index_id: index_id(),
            generation: generation(),
            entity,
        });
        let encoded = encode_build_delta(&CoalescedBuildDeltaValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: entity.kind,
            entity_id: entity.id,
        });
        let encoded_pointer = encoded.as_ptr();
        let migrated =
            migrate_legacy_value(scope, &TenantLogicalKey::Managed(kind), encoded).unwrap();
        assert_eq!(migrated.as_ptr(), encoded_pointer);
    }

    #[tokio::test]
    async fn startup_migrates_graph_and_managed_keys_and_is_idempotent() {
        let db = Db::builder("one-byte-tenant-migration", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let tenant = TenantId::from_u128(0x0102_0304_0506_0708_1112_1314_1516_1718);
        let scope = DataScope::Tenant(tenant);
        let graph_logical = DataKeyKind::NodeProperty(NodePropertyKey::new(11)).to_bytes();
        let mut old_graph = Vec::with_capacity(TENANT_ID_LEN + graph_logical.len());
        old_graph.put_u128(tenant.as_u128());
        old_graph.put_slice(&graph_logical);
        let old_graph = Bytes::from(old_graph);
        let graph_value = Bytes::from_static(b"graph-value");

        let operation_kind = ScopedKey::Operation(IndexOperationKey {
            operation_id: IndexOperationId::from_bytes([7; 16]).unwrap(),
        });
        let old_cursor = IndexCursor::try_new(old_graph.clone()).unwrap();
        let old_operation = legacy_data_key(scope, operation_kind.clone());
        db.put(&old_graph, graph_value.clone()).await.unwrap();
        let operation = operation(
            7,
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                prefix(old_cursor),
            )),
        );
        db.put(&old_operation, encode_operation_record(&operation))
            .await
            .unwrap();

        migrate_all_tenant_keys(&db).await.unwrap();
        migrate_all_tenant_keys(&db).await.unwrap();

        let new_graph = GraphKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(11)),
        }
        .to_bytes();
        assert_eq!(
            new_graph.as_ref(),
            [&[TENANT_KEY_PREFIX], old_graph.as_ref()].concat()
        );
        assert_eq!(db.get(&old_graph).await.unwrap(), None);
        assert_eq!(db.get(&new_graph).await.unwrap(), Some(graph_value));
        assert_eq!(db.get(&old_operation).await.unwrap(), None);
        let migrated = db
            .get(
                ManagedIndexKey::Data {
                    scope,
                    kind: operation_kind,
                }
                .to_bytes(),
            )
            .await
            .unwrap()
            .map(|value| decode_operation_record(&value).unwrap())
            .unwrap();
        assert!(migrated
            .progress()
            .cursors_are_valid(|cursor| cursor.as_bytes() == &new_graph));
        assert!(crate::migrations::tenant_key_envelope_ready(&db)
            .await
            .unwrap());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn conflicting_destination_fails_without_deleting_the_source() {
        let db = Db::builder("one-byte-tenant-conflict", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        let tenant = TenantId::from_u128(0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00);
        let logical = DataKeyKind::NodeProperty(NodePropertyKey::new(11)).to_bytes();
        let mut old = Vec::with_capacity(TENANT_ID_LEN + logical.len());
        old.put_u128(tenant.as_u128());
        old.put_slice(&logical);
        let old = Bytes::from(old);
        let new = GraphKey::Data {
            scope: DataScope::Tenant(tenant),
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(11)),
        }
        .to_bytes();
        db.put(&old, Bytes::from_static(b"source")).await.unwrap();
        db.put(&new, Bytes::from_static(b"conflict")).await.unwrap();

        assert!(matches!(
            migrate_all_tenant_keys(&db).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(
            db.get(&old).await.unwrap(),
            Some(Bytes::from_static(b"source"))
        );
        assert!(!crate::migrations::tenant_key_envelope_ready(&db)
            .await
            .unwrap());
        db.close().await.unwrap();
    }

    #[test]
    fn unchanged_values_remain_zero_copy() {
        let tenant = TenantId::from_u128(0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00);
        let logical = DataKeyKind::NodeProperty(NodePropertyKey::new(11)).to_bytes();
        let mut old = Vec::with_capacity(TENANT_ID_LEN + logical.len());
        old.put_u128(tenant.as_u128());
        old.put_slice(&logical);
        let value = Bytes::from_static(b"unchanged");
        let value_pointer = value.as_ptr();

        let row = migration_row(Bytes::from(old), value).unwrap().unwrap();

        assert_eq!(row.destination_value.as_ptr(), value_pointer);
    }

    #[test]
    fn valid_unscoped_rows_win_over_a_coincidental_tenant_suffix() {
        use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;

        let key = GraphKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePropertyPair(LegacyEdgePropertyPairKey::new(1, 0xFF)),
        }
        .to_bytes();
        assert!(DataKeyKind::parse_from_slice(&key).is_ok());
        assert!(DataKeyKind::parse_from_slice(&key[TENANT_ID_LEN..]).is_ok());

        assert!(migration_row(key, Bytes::from_static(b"unscoped"))
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn restart_after_copy_or_cleanup_finishes_idempotently() {
        for (name, cleanup_before_restart) in [("after-copy", false), ("after-cleanup", true)] {
            let db = Db::builder(
                format!("one-byte-tenant-restart-{name}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap();
            let tenant = TenantId::from_u128(0x1122_3344_5566_7788_99AA_BBCC_DDEE_FF00);
            let logical = DataKeyKind::NodeProperty(NodePropertyKey::new(11)).to_bytes();
            let mut old = Vec::with_capacity(TENANT_ID_LEN + logical.len());
            old.put_u128(tenant.as_u128());
            old.put_slice(&logical);
            let old = Bytes::from(old);
            let new = GraphKey::Data {
                scope: DataScope::Tenant(tenant),
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(11)),
            }
            .to_bytes();
            db.put(&old, Bytes::from_static(b"value")).await.unwrap();

            copy_legacy_tenant_keys(&db).await.unwrap();
            assert!(db.get(&old).await.unwrap().is_some());
            assert!(db.get(&new).await.unwrap().is_some());
            if cleanup_before_restart {
                verify_legacy_tenant_keys(&db).await.unwrap();
                cleanup_legacy_tenant_keys(&db).await.unwrap();
                assert_eq!(db.get(&old).await.unwrap(), None);
            }
            assert!(!crate::migrations::tenant_key_envelope_ready(&db)
                .await
                .unwrap());

            migrate_all_tenant_keys(&db).await.unwrap();

            assert_eq!(db.get(&old).await.unwrap(), None);
            assert_eq!(
                db.get(&new).await.unwrap(),
                Some(Bytes::from_static(b"value"))
            );
            assert!(crate::migrations::tenant_key_envelope_ready(&db)
                .await
                .unwrap());
            db.close().await.unwrap();
        }
    }
}
