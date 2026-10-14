use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;

fn legacy_secondary(
    element_type: LegacySecondaryElementType,
    kind: LegacySecondaryKind,
    unique: bool,
    direction: LegacyRangeDirection,
) -> LegacySecondaryIndexDefinition {
    LegacySecondaryIndexDefinition {
        element_type,
        kind,
        label: "Document".to_string(),
        property: "score".to_string(),
        unique,
        direction,
    }
}

#[test]
fn legacy_secondary_definition_matrix_accepts_only_representable_algorithms() {
    for element_type in [
        LegacySecondaryElementType::Node,
        LegacySecondaryElementType::Edge,
    ] {
        for kind in [LegacySecondaryKind::Equality, LegacySecondaryKind::Range] {
            for unique in [false, true] {
                for direction in [LegacyRangeDirection::Asc, LegacyRangeDirection::Desc] {
                    let result =
                        legacy_secondary(element_type, kind, unique, direction).into_runtime();
                    let expected_valid = matches!(
                        (element_type, kind, unique, direction),
                        (
                            LegacySecondaryElementType::Node,
                            LegacySecondaryKind::Equality,
                            false | true,
                            LegacyRangeDirection::Asc,
                        ) | (
                            LegacySecondaryElementType::Node | LegacySecondaryElementType::Edge,
                            LegacySecondaryKind::Range,
                            false,
                            LegacyRangeDirection::Asc | LegacyRangeDirection::Desc,
                        ) | (
                            LegacySecondaryElementType::Edge,
                            LegacySecondaryKind::Equality,
                            false,
                            LegacyRangeDirection::Asc,
                        )
                    );
                    assert_eq!(
                        result.is_ok(),
                        expected_valid,
                        "unexpected legacy matrix result for {element_type:?}/{kind:?}/{unique}/{direction:?}"
                    );
                }
            }
        }
    }
}

fn legacy_vector(element_type: LegacyVectorElementType) -> LegacyVectorIndexDefinition {
    LegacyVectorIndexDefinition {
        element_type,
        label: "Document".to_string(),
        property: "embedding".to_string(),
        tenant_property: Some("tenant".to_string()),
        dimension: 3,
        metric: LegacyVectorMetric::Cosine,
        m: 8,
        m0: 16,
        ef_construction: 64,
        ml: 0.5,
        simhash_threshold: 64,
        sampling_ratio: 0.25,
        adaptive_enabled: true,
        adaptive_failure_prob: 0.01,
    }
}

fn legacy_text(element_type: LegacyTextElementType) -> LegacyTextIndexDefinition {
    LegacyTextIndexDefinition {
        element_type,
        label: "Document".to_string(),
        property: "body".to_string(),
        tenant_property: Some("tenant".to_string()),
        analyzer: LegacyTextAnalyzer::StandardStemEn,
        positions_enabled: true,
    }
}

#[test]
fn legacy_dynamic_definition_families_preserve_identity_and_runtime_shape() {
    use crate::index_lifecycle::{IndexDefinitionFamily, IndexElementKind};

    let definitions = [
        LegacyDynamicIndexDefinition::Secondary(legacy_secondary(
            LegacySecondaryElementType::Node,
            LegacySecondaryKind::Equality,
            true,
            LegacyRangeDirection::Asc,
        )),
        LegacyDynamicIndexDefinition::Vector(legacy_vector(LegacyVectorElementType::Node)),
        LegacyDynamicIndexDefinition::Vector(legacy_vector(LegacyVectorElementType::Edge)),
        LegacyDynamicIndexDefinition::Text(legacy_text(LegacyTextElementType::Node)),
        LegacyDynamicIndexDefinition::Text(legacy_text(LegacyTextElementType::Edge)),
    ];

    for definition in definitions {
        let key = definition.key();
        let identity = key.identity().expect("legacy identity validates");
        let validated = definition
            .clone()
            .into_validated()
            .expect("legacy definition validates");
        assert_eq!(identity, validated.identity());
        match validated.family() {
            IndexDefinitionFamily::Secondary => {
                assert!(matches!(key, LegacyDynamicIndexKey::Secondary(_)));
                assert_eq!(identity.element_kind(), IndexElementKind::Node);
            }
            IndexDefinitionFamily::Vector => {
                assert!(matches!(key, LegacyDynamicIndexKey::Vector { .. }));
            }
            IndexDefinitionFamily::Text => {
                assert!(matches!(key, LegacyDynamicIndexKey::Text { .. }));
            }
        }
    }
}

#[test]
fn legacy_catalog_rows_round_trip_definition_and_tombstone_variants() {
    use crate::index_lifecycle::ValidatedDynamicIndexDefinition;

    let definitions = [
        ValidatedDynamicIndexDefinition::try_from(
            config::SecondaryIndexDefinition::node_equality("Document", "score").unwrap(),
        )
        .unwrap(),
        ValidatedDynamicIndexDefinition::try_from(
            config::VectorIndexDefinition::new_edge(
                "Document",
                "embedding",
                3,
                crate::search::vector::VectorDistanceMetric::Euclidean,
            )
            .unwrap(),
        )
        .unwrap(),
        ValidatedDynamicIndexDefinition::try_from(
            config::TextIndexDefinition::new_node("Document", "body").unwrap(),
        )
        .unwrap(),
    ];

    for definition in definitions {
        for tombstone in [false, true] {
            let (key, value) = migration_parity_legacy_catalog_row(&definition, tombstone)
                .expect("legacy catalog row encodes");
            assert!(!key.is_empty());
            let decoded: LegacyDynamicIndexCatalogEntry =
                serde_json::from_slice(&value).expect("legacy catalog row decodes");
            assert_eq!(
                matches!(decoded, LegacyDynamicIndexCatalogEntry::Tombstone { .. }),
                tombstone
            );
        }
    }
}

#[test]
fn migration_identity_mode_stage_and_resume_contracts_are_exhaustive() {
    let ids = [
        MigrationId::GraphFormatV1Rewrite,
        MigrationId::LegacyVectorPropertyMaterialization,
        MigrationId::LegacyVectorPhysicalCleanup,
        MigrationId::GraphFormatV1Cleanup,
    ];
    let expected_initial = [
        MigrationStage::PropertyIndexes,
        MigrationStage::NodeProperties,
        MigrationStage::FenceLegacyVectorSources,
        MigrationStage::LegacyEdgePairs,
    ];
    for (id, initial) in ids.into_iter().zip(expected_initial) {
        assert!(!id.storage_name().is_empty());
        assert!(!id.log_name().is_empty());
        assert_eq!(id.initial_stage(), initial);
        let encoded = serde_json::to_vec(&id).unwrap();
        assert_eq!(serde_json::from_slice::<MigrationId>(&encoded).unwrap(), id);
    }

    for mode in [MigrationMode::BlockingStartup, MigrationMode::Background] {
        assert!(!mode.log_name().is_empty());
        let encoded = serde_json::to_vec(&mode).unwrap();
        assert_eq!(
            serde_json::from_slice::<MigrationMode>(&encoded).unwrap(),
            mode
        );
    }

    let stages = [
        MigrationStage::PropertyIndexes,
        MigrationStage::NodeProperties,
        MigrationStage::LegacyEdgePairs,
        MigrationStage::EdgeEndpoints,
        MigrationStage::FenceLegacyVectorSources,
        MigrationStage::LegacyVectorHotRows,
        MigrationStage::LegacyVectorLayer0Rows,
        MigrationStage::LegacyVectorCoreRows,
        MigrationStage::LegacyVectorDefinitions,
        MigrationStage::ReleaseLegacyVectorReservations,
    ];
    for stage in stages {
        assert!(!stage.prefix(DataScope::LegacyUnscoped).is_empty());
        assert!(!stage.log_name().is_empty());
    }

    assert!(MigrationResumeKey::new(Vec::new()).is_none());
    assert!(MigrationResumeKey::try_from(Vec::new()).is_err());
    let resume = MigrationResumeKey::new(vec![1, 2, 3]).unwrap();
    assert_eq!(resume.as_bytes(), &[1, 2, 3]);
    assert_eq!(Vec::<u8>::from(resume.clone()), vec![1, 2, 3]);
    let encoded = serde_json::to_vec(&resume).unwrap();
    assert_eq!(
        serde_json::from_slice::<MigrationResumeKey>(&encoded).unwrap(),
        resume
    );
}

#[test]
fn migration_job_state_machine_preserves_counters_and_rejects_invalid_transitions() {
    let resume = MigrationResumeKey::new(vec![7]).unwrap();
    let mut running = MigrationJob::new(
        MigrationId::GraphFormatV1Rewrite,
        MigrationMode::BlockingStartup,
    );
    assert!(running.is_runnable());
    assert!(!running.is_completed());
    assert!(!running.is_failed());
    assert_eq!(
        running.state.running_stage(),
        Some(MigrationStage::PropertyIndexes)
    );
    assert_eq!(running.state.processed_rows(), 0);
    assert_eq!(running.state.log_name(), "running");
    running.record_advanced(resume.clone(), u64::MAX);
    running.record_advanced(resume.clone(), 1);
    assert_eq!(running.state.processed_rows(), u64::MAX);
    running.advance_stage(MigrationStage::NodeProperties);
    assert_eq!(
        running.state.running_stage(),
        Some(MigrationStage::NodeProperties)
    );
    running.fail("failed");
    assert!(running.is_failed());
    assert_eq!(running.state.running_stage(), None);
    assert_eq!(running.state.log_name(), "failed");

    let failed_before = running.clone();
    running.record_advanced(resume.clone(), 1);
    running.fail("ignored");
    assert_eq!(running, failed_before);
    running.retry();
    assert!(running.is_runnable());
    assert_eq!(running.state.processed_rows(), u64::MAX);
    running.complete();
    assert!(running.is_completed());
    assert_eq!(running.state.log_name(), "completed");
    assert_eq!(running.state.running_stage(), None);

    let completed_before = running.clone();
    running.record_advanced(resume, 1);
    running.advance_stage(MigrationStage::EdgeEndpoints);
    running.complete();
    running.fail("ignored");
    running.retry();
    assert_eq!(running, completed_before);

    let key = MigrationJobKey::new(DataScope::LegacyUnscoped, MigrationId::GraphFormatV1Cleanup);
    assert!(!key.as_ref().is_empty());
    assert_eq!(key.clone().into_bytes().as_ref(), key.as_ref());
    assert!(!migration_job_scan_prefix_scoped(DataScope::LegacyUnscoped).is_empty());
    const {
        assert!(!MigrationStep::IDLE.advanced);
        assert!(MigrationStep::IDLE.rows == 0);
        assert!(MigrationStep::IDLE.admitted_bytes == 0);
    }
}

#[test]
fn legacy_range_direction_preserves_both_physical_directions() {
    assert_eq!(
        legacy_range_direction(config::RangeIndexDirection::Asc),
        crate::encoding::v2::keys::indexes::range::RangeIndexDirection::Asc
    );
    assert_eq!(
        legacy_range_direction(config::RangeIndexDirection::Desc),
        crate::encoding::v2::keys::indexes::range::RangeIndexDirection::Desc
    );
}

#[tokio::test]
async fn durable_job_and_readiness_markers_cover_absent_present_malformed_and_ordered_states() {
    let db = Db::open("migration-readiness-contracts", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let id = MigrationId::GraphFormatV1Rewrite;
    assert!(!migration_completed(&db, scope, id).await.unwrap());
    assert_eq!(migration_processed_rows(&db, scope, id).await.unwrap(), 0);
    ensure_migration_job(&db, scope, id, MigrationMode::BlockingStartup)
        .await
        .unwrap();
    ensure_migration_job(&db, scope, id, MigrationMode::BlockingStartup)
        .await
        .unwrap();
    assert!(!migration_completed(&db, scope, id).await.unwrap());
    let mut completed = MigrationJob::new(id, MigrationMode::BlockingStartup);
    completed.record_advanced(MigrationResumeKey::new(vec![1]).unwrap(), 7);
    completed.complete();
    db.put(
        MigrationJobKey::new(scope, id).into_bytes(),
        encode_json(&completed).unwrap(),
    )
    .await
    .unwrap();
    assert!(migration_completed(&db, scope, id).await.unwrap());
    assert_eq!(migration_processed_rows(&db, scope, id).await.unwrap(), 7);

    assert!(!graph_format_v1_ready(&db, scope).await.unwrap());
    assert!(!index_v2_migration_ready(&db, scope).await.unwrap());
    assert!(!storage_schema_complete(&db, scope).await.unwrap());
    assert!(!index_storage_v4_cleanup_ready(&db).await.unwrap());
    assert!(!tenant_key_envelope_ready(&db).await.unwrap());
    ensure_graph_format_v1_ready(&db, scope).await.unwrap();
    ensure_graph_format_v1_ready(&db, scope).await.unwrap();
    assert!(graph_format_v1_ready(&db, scope).await.unwrap());
    assert_eq!(
        storage_schema_progress(&db, scope).await.unwrap(),
        StorageSchemaProgress::GraphReady
    );

    db.put(
        scoped_metadata_key(scope, INDEX_V2_MIGRATION_READY),
        Bytes::from_static(b"1"),
    )
    .await
    .unwrap();
    assert!(index_v2_migration_ready(&db, scope).await.unwrap());
    assert_eq!(
        storage_schema_progress(&db, scope).await.unwrap(),
        StorageSchemaProgress::IndexReady
    );
    publish_storage_schema_completion(&db, scope).await.unwrap();
    publish_storage_schema_completion(&db, scope).await.unwrap();
    assert!(storage_schema_complete(&db, scope).await.unwrap());
    assert_eq!(
        storage_schema_progress(&db, scope).await.unwrap(),
        StorageSchemaProgress::Complete
    );

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    stage_index_storage_v4_cleanup_ready(&transaction).unwrap();
    stage_tenant_key_envelope_ready(&transaction).unwrap();
    transaction.commit().await.unwrap();
    assert!(index_storage_v4_cleanup_ready(&db).await.unwrap());
    assert!(tenant_key_envelope_ready(&db).await.unwrap());

    db.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_STORAGE_V4_CLEANUP_READY),
        Bytes::from_static(b"malformed"),
    )
    .await
    .unwrap();
    assert!(matches!(
        index_storage_v4_cleanup_ready(&db).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    db.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_STORAGE_V4_CLEANUP_READY),
        Bytes::from_static(b"1"),
    )
    .await
    .unwrap();
    db.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, TENANT_KEY_ENVELOPE_READY),
        Bytes::from_static(b"malformed"),
    )
    .await
    .unwrap();
    assert!(matches!(
        tenant_key_envelope_ready(&db).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    db.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, TENANT_KEY_ENVELOPE_READY),
        Bytes::from_static(b"1"),
    )
    .await
    .unwrap();

    db.delete(scoped_metadata_key(scope, GRAPH_FORMAT_V1_READY))
        .await
        .unwrap();
    assert!(matches!(
        storage_schema_progress(&db, scope).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    assert!(matches!(
        publish_storage_schema_completion(&db, scope).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    db.put(
        scoped_metadata_key(scope, GRAPH_FORMAT_V1_READY),
        Bytes::from_static(b"malformed"),
    )
    .await
    .unwrap();
    assert!(matches!(
        storage_schema_progress(&db, scope).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    db.close().await.unwrap();
}

#[tokio::test]
async fn legacy_physical_retirement_dispatches_every_family_and_direction() {
    let db = Db::open("migration-retirement-contracts", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let scope = DataScope::LegacyUnscoped;
    let definitions = [
        config::SecondaryIndexDefinition::node_equality("Document", "value").unwrap(),
        config::SecondaryIndexDefinition::node_range("Document", "value").unwrap(),
        config::SecondaryIndexDefinition::node_range_desc("Document", "value").unwrap(),
        config::SecondaryIndexDefinition::edge_equality("REL", "value").unwrap(),
        config::SecondaryIndexDefinition::edge_range("REL", "value").unwrap(),
        config::SecondaryIndexDefinition::edge_range_desc("REL", "value").unwrap(),
    ];
    for definition in definitions {
        let validated =
            crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(definition).unwrap();
        retire_legacy_physical_rows(&transaction, scope, &validated)
            .await
            .unwrap();
    }

    let text = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
        config::TextIndexDefinition::new_node("Document", "body").unwrap(),
    )
    .unwrap();
    retire_legacy_physical_rows(&transaction, scope, &text)
        .await
        .unwrap();
    let vector = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
        config::VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            crate::search::vector::VectorDistanceMetric::Euclidean,
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        retire_legacy_physical_rows(&transaction, scope, &vector).await,
        Err(HelixDbError::InvariantViolation(_))
    ));
    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn legacy_tombstone_retirement_requires_exact_source_and_v2_absence() {
    let db = Db::open(
        "migration-tombstone-retirement-contracts",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let identity = LegacyDynamicIndexDefinition::Secondary(legacy_secondary(
        LegacySecondaryElementType::Node,
        LegacySecondaryKind::Equality,
        false,
        LegacyRangeDirection::Asc,
    ))
    .key();
    let storage_key = scoped_metadata_key(scope, b"external/legacy/tombstone");
    db.put(storage_key.clone(), Bytes::from_static(b"legacy"))
        .await
        .unwrap();
    retire_legacy_definition_row(&db, scope, storage_key.clone(), None, &identity)
        .await
        .unwrap();
    assert!(db.get(&storage_key).await.unwrap().is_none());
    assert!(matches!(
        retire_legacy_definition_row(&db, scope, storage_key.clone(), None, &identity).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));

    let expected = LegacyDynamicIndexDefinition::Secondary(legacy_secondary(
        LegacySecondaryElementType::Node,
        LegacySecondaryKind::Equality,
        false,
        LegacyRangeDirection::Asc,
    ))
    .into_validated()
    .unwrap();
    assert!(matches!(
        retire_legacy_definition_row(&db, scope, storage_key, Some(&expected), &identity).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    db.close().await.unwrap();
}

#[test]
fn receipt_projection_preserves_accepted_existing_and_active_shapes() {
    use crate::index_lifecycle::{IndexDdlReceipt, IndexGenerationId, IndexId, IndexOperationId};

    let operation_id = IndexOperationId::from_bytes([7; 16]).unwrap();
    assert_eq!(
        receipt_operation_id(IndexDdlReceipt::Accepted {
            operation_id,
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
        }),
        Some(operation_id)
    );
    assert_eq!(
        receipt_operation_id(IndexDdlReceipt::ExistingOperation { operation_id }),
        Some(operation_id)
    );
    assert_eq!(
        receipt_operation_id(IndexDdlReceipt::AlreadyActive {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
        }),
        None
    );
}

#[tokio::test]
async fn empty_migration_controller_executes_every_stage_and_catalog_family_to_completion() {
    let raw = Arc::new(
        Db::open(
            "migration-empty-controller-contracts",
            Arc::new(InMemory::new()),
        )
        .await
        .unwrap(),
    );
    let config = crate::config::DbConfig::default();
    let writer = crate::HelixWriter::new(
        Arc::clone(&raw),
        config.id_lease_size(),
        config.id_lease_refill_threshold(),
    );
    let scope = DataScope::LegacyUnscoped;

    assert!(
        !process_migration_once_by_id(
            &writer,
            scope,
            config.migrations(),
            MigrationId::GraphFormatV1Rewrite,
        )
        .await
        .unwrap(),
        "an absent migration job is explicitly idle"
    );
    for id in [
        MigrationId::GraphFormatV1Rewrite,
        MigrationId::LegacyVectorPropertyMaterialization,
        MigrationId::LegacyVectorPhysicalCleanup,
        MigrationId::GraphFormatV1Cleanup,
    ] {
        ensure_migration_job(&raw, scope, id, MigrationMode::Background)
            .await
            .unwrap();
        let mut turns = 0;
        while !migration_completed(raw.as_ref(), scope, id).await.unwrap() {
            assert!(
                process_migration_once_by_id(&writer, scope, config.migrations(), id)
                    .await
                    .unwrap(),
                "runnable empty migration {id:?} advances one closed stage"
            );
            turns += 1;
            assert!(turns <= 16, "empty migration has a bounded stage family");
        }
        assert!(turns > 0);
        assert!(
            !process_migration_once_by_id(&writer, scope, config.migrations(), id)
                .await
                .unwrap(),
            "a completed migration is explicitly idle"
        );
    }
    assert!(!process_migration_once(&writer, scope, config.migrations())
        .await
        .unwrap());
    drop(writer);
    let Ok(raw) = Arc::try_unwrap(raw) else {
        panic!("migration writer releases its database")
    };
    raw.close().await.unwrap();
}
