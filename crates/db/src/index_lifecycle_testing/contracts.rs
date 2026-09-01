//! Deterministic Index V2 lifecycle acceptance contracts.
//!
//! Every case uses the feature-gated controller to invoke the installed
//! production drivers. Shape construction is exhaustive over currently
//! supported secondary forms, vector metrics/partitioning, text analyzers,
//! positions, and node/edge ownership. The fixtures persist only deployed
//! keys and values.

use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::{
    SearchIndexBackfillLimits, SearchIndexBatchLimits, SecondaryIndexDefinition,
    SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning, TextAnalyzerKind,
    TextElementType, TextIndexDefinition, VectorElementType, VectorIndexDefinition,
};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys::indexes::vector::VectorStorageLane;
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{DataKey, DataKeyKind, EdgePropertyByIdKey, NodePropertyKey};
use crate::encoding::v2::keys::{RecordKind, ScopedKey};
use crate::error::HelixDbError;
use crate::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionScalar, ExecutionValue,
};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexDdlReceipt, IndexDefinitionFamily, IndexElementKind,
    IndexOperationStage, IndexOperationStatus, IndexStateV2, PhysicalGeneration,
    ValidatedDynamicIndexDefinition, VectorPhysicalLayout,
};
use crate::search::vector::distance::Cosine;
use crate::search::vector::{ValidatedVectorGenerationHandle, VectorDistanceMetric, VectorIndex};
use crate::search::{text_index_name, vector_index_name};
use crate::{DbConfig, HelixDB, HelixDbSource};

use super::{
    LifecycleCheckpoint, LifecycleStepEvidence, LifecycleStepOutcome, LifecycleTestController,
    LifecycleTestScheduling, LifecycleWorkTarget,
};

mod all_index_validation;

const MAXIMUM_CONTROLLER_TURNS: usize = 4_096;

/// Runs all deterministic DDL shape and convergence contracts.
pub(super) async fn run() {
    run_active_drop_snapshot_survival().await;
    run_active_vector_drop_snapshot_survival().await;
    run_family_shapes().await;
    run_create_drop_state_matrix().await;
    run_building_drop_stage_matrix().await;
    run_scope_isolation().await;
}

/// Runs retry-safe concurrent lifecycle races.
pub(super) async fn run_races() {
    run_concurrent_create_convergence().await;
    run_conflicting_create_convergence().await;
    run_activation_and_drop_serialization().await;
    run_active_drop_and_mutation_serialization().await;
    run_automatic_worker_convergence().await;
}

/// Proves two consecutive errors preserve one recoverable family checkpoint.
pub(super) async fn run_repeated_faults() {
    let definitions: [ValidatedDynamicIndexDefinition; 3] = [
        SecondaryIndexDefinition::node_equality("FaultSecondary", "value")
            .expect("fault secondary validates")
            .try_into()
            .expect("fault secondary converts"),
        VectorIndexDefinition::new_node("FaultVector", "value", 3, VectorDistanceMetric::Cosine)
            .expect("fault vector validates")
            .try_into()
            .expect("fault vector converts"),
        TextIndexDefinition::new_node("FaultText", "value")
            .expect("fault text validates")
            .try_into()
            .expect("fault text converts"),
    ];
    for (ordinal, definition) in definitions.into_iter().enumerate() {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!("index-lifecycle-lifecycle-repeated-fault-{ordinal}"),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("repeated-fault writer opens");
        let controller = LifecycleTestController::new();
        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition,
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("repeated-fault build is accepted")
        else {
            panic!("fresh repeated-fault build must enqueue");
        };
        let target = LifecycleWorkTarget::Operation {
            scope: DataScope::LegacyUnscoped,
            operation_id,
        };
        let initial = controller
            .inspect(&db, target)
            .await
            .expect("initial repeated-fault checkpoint is readable");
        let mut claimed = None;
        for _ in 0..2 {
            super::inject_index_outbox_error_once("commit_before")
                .expect("one recoverable commit error installs");
            let error = controller
                .advance(&db, target)
                .await
                .expect_err("injected commit boundary returns an error");
            assert!(error.to_string().contains("commit_before"));
            assert!(super::index_outbox_error_was_triggered());
            let checkpoint = controller
                .inspect(&db, target)
                .await
                .expect("claimed repeated-fault checkpoint is readable");
            if let Some(previous) = claimed {
                let (
                    LifecycleCheckpoint::Present {
                        stage: previous_stage,
                        durable_revision: previous_revision,
                        progress: previous_progress,
                    },
                    LifecycleCheckpoint::Present {
                        stage,
                        durable_revision,
                        progress,
                    },
                ) = (previous, checkpoint)
                else {
                    panic!("repeated operation errors must retain present checkpoints");
                };
                assert_eq!(stage, previous_stage);
                assert_eq!(progress, previous_progress);
                assert!(durable_revision > previous_revision);
            }
            claimed = Some(checkpoint);
        }
        assert_ne!(
            claimed,
            Some(initial),
            "claim durability must be observable"
        );
        assert!(matches!(
            drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        db.close().await.expect("repeated-fault writer closes");
    }
}

/// Proves every tenant lifecycle family can cold-reopen after each committed
/// checkpoint without losing ownership, repeating progress, or leaving one
/// generation behind after ordinary and abort cleanup.
pub(super) async fn run_tenant_reopen_recovery() {
    for (ordinal, family) in [
        IndexDefinitionFamily::Secondary,
        IndexDefinitionFamily::Vector,
        IndexDefinitionFamily::Text,
    ]
    .into_iter()
    .enumerate()
    {
        run_family_tenant_reopen_recovery(ordinal, family).await;
    }
}

fn one_row_lifecycle_config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let one_row_batch = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        batch.max_input_bytes(),
        batch.max_output_operations(),
        batch.max_output_bytes(),
        batch.max_single_vector_output_bytes(),
    )
    .expect("one-row lifecycle batch retains valid byte ceilings");
    let backfill = SearchIndexBackfillLimits::try_new(
        one_row_batch,
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("one-row lifecycle backfill limits remain internally consistent");
    DbConfig::new()
        .with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_batch_rows(
                SecondaryIndexLifecycleBatchRows::new(1)
                    .expect("one-row secondary lifecycle batch is positive"),
            ),
        )
        .with_search_index_backfill_limits(backfill)
}

fn recovery_definition(
    family: IndexDefinitionFamily,
    label: &str,
) -> ValidatedDynamicIndexDefinition {
    match family {
        IndexDefinitionFamily::Secondary => SecondaryIndexDefinition::node_equality(label, "value")
            .expect("recovery secondary definition validates")
            .try_into()
            .expect("recovery secondary definition converts"),
        IndexDefinitionFamily::Vector => {
            VectorIndexDefinition::new_node(label, "value", 3, VectorDistanceMetric::Cosine)
                .expect("recovery vector definition validates")
                .try_into()
                .expect("recovery vector definition converts")
        }
        IndexDefinitionFamily::Text => TextIndexDefinition::new_node(label, "value")
            .expect("recovery text definition validates")
            .try_into()
            .expect("recovery text definition converts"),
    }
}

async fn seed_recovery_rows(
    db: &HelixDB,
    controller: &LifecycleTestController,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
) -> std::ops::Range<u64> {
    let label = definition.identity().label().as_str().to_string();
    let family = definition.family();
    controller
        .seed_node_property_rows(
            db,
            scope,
            NonZeroU64::new(3).expect("recovery source count is positive"),
            NonZeroUsize::MIN,
            move |entity_id| {
                let value = match family {
                    IndexDefinitionFamily::Secondary => {
                        Property::string("value", format!("shared-{label}"))
                    }
                    IndexDefinitionFamily::Vector => {
                        Property::f32_array("value", vec![entity_id as f32 + 1.0, 1.0, 0.5])
                    }
                    IndexDefinitionFamily::Text => Property::string(
                        "value",
                        format!("recovery document {entity_id} for {label}"),
                    ),
                };
                vec![Property::string("$label", label.clone()), value]
            },
        )
        .await
        .expect("recovery source rows seed")
}

async fn reopen_recovery_writer(
    db: &mut HelixDB,
    database: &str,
    object_store: &Arc<dyn ObjectStore>,
    config: &DbConfig,
) {
    db.close()
        .await
        .expect("recovery writer closes at its durable checkpoint");
    *db = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        database,
        Arc::clone(object_store),
        config.clone(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("recovery writer cold-reopens");
}

fn assert_checkpoint_did_not_regress(before: LifecycleCheckpoint, after: LifecycleCheckpoint) {
    let (
        LifecycleCheckpoint::Present {
            durable_revision: before_revision,
            progress: before_progress,
            ..
        },
        LifecycleCheckpoint::Present {
            durable_revision: after_revision,
            progress: after_progress,
            ..
        },
    ) = (before, after)
    else {
        return;
    };
    assert!(after_revision >= before_revision);
    assert!(after_progress.entities >= before_progress.entities);
    assert!(after_progress.input_bytes >= before_progress.input_bytes);
    assert!(after_progress.output_operations >= before_progress.output_operations);
    assert!(after_progress.output_bytes >= before_progress.output_bytes);
}

async fn drive_to_terminal_with_reopen(
    db: &mut HelixDB,
    database: &str,
    object_store: &Arc<dyn ObjectStore>,
    config: &DbConfig,
    controller: &LifecycleTestController,
    scope: DataScope,
    operation_id: crate::index_lifecycle::IndexOperationId,
) -> IndexOperationStatus {
    let target = LifecycleWorkTarget::Operation {
        scope,
        operation_id,
    };
    let logical_start = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("recovery contract clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("recovery contract time fits u64 milliseconds");
    let mut expected_checkpoint = controller
        .inspect(db, target)
        .await
        .expect("initial recovery checkpoint is readable");
    for turn in 0..MAXIMUM_CONTROLLER_TURNS {
        let status = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("recovery operation remains readable");
        let terminal = matches!(
            status,
            IndexOperationStatus::Succeeded { .. } | IndexOperationStatus::Aborted { .. }
        );
        if terminal {
            let page = controller
                .discover(
                    db,
                    NonZeroUsize::new(1_024).expect("recovery discovery bound is positive"),
                )
                .await
                .expect("terminal recovery discovery succeeds");
            assert!(page.targets.is_empty());
            let evidence = controller
                .advance_at_unix_millis(db, target, logical_start)
                .await
                .expect("completed recovery operation remains idempotent");
            assert_eq!(evidence.outcome, LifecycleStepOutcome::AlreadyTerminal);
            assert_eq!(evidence.before, evidence.after);
            reopen_recovery_writer(db, database, object_store, config).await;
            assert_eq!(
                controller
                    .inspect(db, target)
                    .await
                    .expect("terminal checkpoint survives its final reopen"),
                evidence.after
            );
            return status;
        }
        assert!(!matches!(status, IndexOperationStatus::Blocked { .. }));
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("recovery controller turn fits u64")
                .saturating_mul(60_000),
        );
        let evidence = controller
            .advance_at_unix_millis(db, target, logical_now)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "recovery production step in {database} for {:?} failed: {error}",
                    status.common().stage,
                )
            });
        assert_eq!(evidence.before, expected_checkpoint);
        assert_monotonic_step(&evidence);
        assert_checkpoint_did_not_regress(evidence.before, evidence.after);
        expected_checkpoint = evidence.after;
        reopen_recovery_writer(db, database, object_store, config).await;
        let reopened = controller
            .inspect(db, target)
            .await
            .expect("persisted recovery checkpoint is readable after reopen");
        assert_eq!(reopened, expected_checkpoint);
    }
    panic!("recovery operation exceeded its controller-turn bound");
}

async fn advance_once_with_reopen(
    db: &mut HelixDB,
    database: &str,
    object_store: &Arc<dyn ObjectStore>,
    config: &DbConfig,
    controller: &LifecycleTestController,
    target: LifecycleWorkTarget,
) {
    let evidence = controller
        .advance(db, target)
        .await
        .expect("pre-abort recovery step succeeds");
    assert_monotonic_step(&evidence);
    reopen_recovery_writer(db, database, object_store, config).await;
    assert_eq!(
        controller
            .inspect(db, target)
            .await
            .expect("pre-abort checkpoint survives reopen"),
        evidence.after
    );
}

async fn assert_generation_rows_absent(
    db: &HelixDB,
    scope: DataScope,
    index_id: crate::index_lifecycle::IndexId,
    generation: crate::index_lifecycle::IndexGenerationId,
) {
    let writer = db
        .lifecycle_test_writer_db()
        .expect("generation cleanup assertion has writer storage");
    for kind in [
        RecordKind::BuildDelta,
        RecordKind::AppliedState,
        RecordKind::SecondaryEntry,
        RecordKind::TextManifestRoot,
        RecordKind::TextManifestPage,
        RecordKind::TextBuildArtifact,
        RecordKind::TextEntityState,
        RecordKind::VectorPartitionMapping,
        RecordKind::TextCorpusStatistics,
        RecordKind::TextTermStatistics,
        RecordKind::TextStatisticsEntity,
        RecordKind::SecondaryEqualityBitmap,
    ] {
        let prefix = crate::encoding::v2::keys::ManagedIndexKey::data_prefix(
            scope,
            ScopedKey::generation_prefix(kind, index_id, generation),
        );
        let mut rows = writer
            .scan_prefix(&prefix, ..)
            .await
            .expect("generation cleanup lane remains readable");
        assert!(
            rows.next()
                .await
                .expect("generation cleanup lane scan succeeds")
                .is_none(),
            "cleanup retained a {kind:?} row"
        );
    }
}

async fn assert_vector_namespace_absent(
    db: &HelixDB,
    scope: DataScope,
    physical_index_id: crate::index_lifecycle::VectorPhysicalIndexId,
) {
    let writer = db
        .lifecycle_test_writer_db()
        .expect("vector cleanup assertion has writer storage");
    for lane in VectorStorageLane::ALL {
        let prefix = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(lane.prefix_key(physical_index_id.get())),
        }
        .to_bytes();
        let mut rows = writer
            .scan_prefix(&prefix, ..)
            .await
            .expect("vector physical lane remains readable");
        assert!(
            rows.next()
                .await
                .expect("vector physical lane scan succeeds")
                .is_none(),
            "cleanup retained a {lane:?} physical vector row"
        );
    }
}

async fn run_family_tenant_reopen_recovery(ordinal: usize, family: IndexDefinitionFamily) {
    let database = format!("index-lifecycle-tenant-reopen-{ordinal}");
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = one_row_lifecycle_config();
    let scope = DataScope::Tenant(TenantId::from_u128(
        0xFD00_0000_0000_0000_0000_0000_0000_0100 + ordinal as u128,
    ));
    let mut db = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        &database,
        Arc::clone(&object_store),
        config.clone(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("tenant recovery writer opens");
    let controller = LifecycleTestController::new();

    let definition = recovery_definition(family, &format!("ReopenActive{ordinal}"));
    let _entity_ids = seed_recovery_rows(&db, &controller, scope, &definition).await;
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            scope,
            definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("tenant recovery build is accepted")
    else {
        panic!("fresh tenant recovery build must enqueue");
    };
    assert!(matches!(
        drive_to_terminal_with_reopen(
            &mut db,
            &database,
            &object_store,
            &config,
            &controller,
            scope,
            operation_id,
        )
        .await,
        IndexOperationStatus::Succeeded { .. }
    ));
    let active = crate::index_lifecycle::repository::load_index_record(
        db.lifecycle_test_writer_db()
            .expect("tenant recovery writer storage is available"),
        scope,
        &definition.identity(),
    )
    .await
    .expect("tenant recovery canonical row decodes")
    .expect("tenant recovery canonical row exists");
    assert!(matches!(active.state(), IndexStateV2::Active { .. }));
    let index_id = active.index_id();
    let generation = active.state().generation();
    let vector_physical_id = match active.state().physical() {
        Some(PhysicalGeneration::Vector {
            layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
            ..
        }) => Some(*physical_index_id),
        Some(PhysicalGeneration::Vector {
            layout: VectorPhysicalLayout::Partitioned,
            ..
        }) => panic!("recovery vector fixture is unpartitioned"),
        Some(PhysicalGeneration::Secondary { .. } | PhysicalGeneration::Text { .. }) | None => None,
    };
    let IndexDdlReceipt::Accepted {
        operation_id: drop_operation_id,
        ..
    } = controller
        .drop_index(&db, scope, &definition)
        .await
        .expect("active tenant recovery index accepts DROP")
    else {
        panic!("active tenant recovery DROP must enqueue cleanup");
    };
    assert!(matches!(
        drive_to_terminal_with_reopen(
            &mut db,
            &database,
            &object_store,
            &config,
            &controller,
            scope,
            drop_operation_id,
        )
        .await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert_generation_rows_absent(&db, scope, index_id, generation).await;
    if let Some(physical_index_id) = vector_physical_id {
        assert_vector_namespace_absent(&db, scope, physical_index_id).await;
    }

    let abort_definition = recovery_definition(family, &format!("ReopenAbort{ordinal}"));
    let _abort_entity_ids = seed_recovery_rows(&db, &controller, scope, &abort_definition).await;
    let IndexDdlReceipt::Accepted {
        operation_id: abort_operation_id,
        index_id: abort_index_id,
        generation: abort_generation,
    } = controller
        .create_index(
            &db,
            scope,
            abort_definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("tenant recovery abort build is accepted")
    else {
        panic!("fresh tenant recovery abort build must enqueue");
    };
    let abort_target = LifecycleWorkTarget::Operation {
        scope,
        operation_id: abort_operation_id,
    };
    advance_once_with_reopen(
        &mut db,
        &database,
        &object_store,
        &config,
        &controller,
        abort_target,
    )
    .await;
    assert!(!matches!(
        db.get_index_operation(scope, abort_operation_id)
            .await
            .expect("pre-abort recovery operation remains readable"),
        IndexOperationStatus::Succeeded { .. } | IndexOperationStatus::Aborted { .. }
    ));
    assert_eq!(
        controller
            .drop_index(&db, scope, &abort_definition)
            .await
            .expect("tenant recovery build converts to abort cleanup"),
        IndexDdlReceipt::ExistingOperation {
            operation_id: abort_operation_id,
        }
    );
    assert!(matches!(
        drive_to_terminal_with_reopen(
            &mut db,
            &database,
            &object_store,
            &config,
            &controller,
            scope,
            abort_operation_id,
        )
        .await,
        IndexOperationStatus::Aborted { .. }
    ));
    assert_generation_rows_absent(&db, scope, abort_index_id, abort_generation).await;
    db.close().await.expect("tenant recovery writer closes");
}

/// Proves simultaneous idempotent CREATE calls converge on one operation.
async fn run_concurrent_create_convergence() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-concurrent-create".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("concurrent CREATE writer opens");
    let controller = LifecycleTestController::new();
    let definitions: [ValidatedDynamicIndexDefinition; 3] = [
        SecondaryIndexDefinition::node_equality("ConcurrentSecondary", "value")
            .expect("concurrent secondary validates")
            .try_into()
            .expect("concurrent secondary converts"),
        VectorIndexDefinition::new_node(
            "ConcurrentVector",
            "value",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("concurrent vector validates")
        .try_into()
        .expect("concurrent vector converts"),
        TextIndexDefinition::new_node("ConcurrentText", "value")
            .expect("concurrent text validates")
            .try_into()
            .expect("concurrent text converts"),
    ];
    for definition in definitions {
        let left_definition = definition.clone();
        let right_definition = definition.clone();
        let (left, right) = tokio::join!(
            controller.create_index(
                &db,
                DataScope::LegacyUnscoped,
                left_definition,
                helix_planner::ir::IndexCreateMode::IfNotExists,
            ),
            controller.create_index(
                &db,
                DataScope::LegacyUnscoped,
                right_definition,
                helix_planner::ir::IndexCreateMode::IfNotExists,
            ),
        );
        let left = match left {
            Ok(receipt) => receipt,
            Err(error) if error.is_transaction_conflict() => controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await
                .expect("left conflicting CREATE retries to one receipt"),
            Err(error) => panic!("left concurrent CREATE failed unexpectedly: {error}"),
        };
        let right = match right {
            Ok(receipt) => receipt,
            Err(error) if error.is_transaction_conflict() => controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await
                .expect("right conflicting CREATE retries to one receipt"),
            Err(error) => panic!("right concurrent CREATE failed unexpectedly: {error}"),
        };
        let receipts = [left, right];
        let operation_ids = receipts
            .iter()
            .map(|receipt| match receipt {
                IndexDdlReceipt::Accepted { operation_id, .. }
                | IndexDdlReceipt::ExistingOperation { operation_id } => *operation_id,
                IndexDdlReceipt::AlreadyActive { .. } => {
                    panic!("concurrent CREATE cannot observe Active before explicit stepping")
                }
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(operation_ids.len(), 1);
        assert_eq!(
            receipts
                .iter()
                .filter(|receipt| matches!(receipt, IndexDdlReceipt::Accepted { .. }))
                .count(),
            1,
            "exactly one concurrent caller owns operation creation"
        );
        let operation_id = *operation_ids
            .first()
            .expect("concurrent CREATE produced one operation");
        assert!(matches!(
            drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        let IndexDdlReceipt::Accepted {
            operation_id: drop_operation_id,
            ..
        } = controller
            .drop_index(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("concurrent CREATE fixture accepts cleanup")
        else {
            panic!("first active cleanup is accepted");
        };
        assert!(matches!(
            drive_to_terminal(
                &db,
                &controller,
                DataScope::LegacyUnscoped,
                drop_operation_id,
            )
            .await,
            IndexOperationStatus::Succeeded { .. }
        ));
    }
    db.close().await.expect("concurrent CREATE writer closes");
}

/// Proves simultaneous incompatible definitions cannot create two generations.
async fn run_conflicting_create_convergence() {
    let conflicts: [(
        ValidatedDynamicIndexDefinition,
        ValidatedDynamicIndexDefinition,
    ); 3] = [
        (
            SecondaryIndexDefinition::node_equality("ConflictSecondary", "value")
                .expect("secondary equality validates")
                .try_into()
                .expect("secondary equality converts"),
            SecondaryIndexDefinition::node_unique_equality("ConflictSecondary", "value")
                .expect("secondary uniqueness validates")
                .try_into()
                .expect("secondary uniqueness converts"),
        ),
        (
            VectorIndexDefinition::new_node(
                "ConflictVector",
                "value",
                3,
                VectorDistanceMetric::Cosine,
            )
            .expect("cosine vector validates")
            .try_into()
            .expect("cosine vector converts"),
            VectorIndexDefinition::new_node(
                "ConflictVector",
                "value",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("Euclidean vector validates")
            .try_into()
            .expect("Euclidean vector converts"),
        ),
        (
            TextIndexDefinition::new_node("ConflictText", "value")
                .expect("standard text validates")
                .try_into()
                .expect("standard text converts"),
            TextIndexDefinition::new_node("ConflictText", "value")
                .expect("stemmed text validates")
                .with_analyzer(TextAnalyzerKind::StandardStemEn)
                .try_into()
                .expect("stemmed text converts"),
        ),
    ];

    for (ordinal, (left_definition, right_definition)) in conflicts.into_iter().enumerate() {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!("index-lifecycle-lifecycle-conflicting-create-{ordinal}"),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("conflicting CREATE writer opens");
        let controller = LifecycleTestController::new();
        let (left, right) = tokio::join!(
            controller.create_index(
                &db,
                DataScope::LegacyUnscoped,
                left_definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            ),
            controller.create_index(
                &db,
                DataScope::LegacyUnscoped,
                right_definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            ),
        );

        let (winner, loser_definition, loser) = match (left, right) {
            (Ok(receipt), Err(error)) => (receipt, right_definition, error),
            (Err(error), Ok(receipt)) => (receipt, left_definition, error),
            (left, right) => panic!(
                "conflicting CREATE must have one transaction winner: left={left:?}, right={right:?}"
            ),
        };
        let IndexDdlReceipt::Accepted { operation_id, .. } = winner else {
            panic!("conflicting fresh CREATE winner must own the accepted build");
        };
        if loser.is_transaction_conflict() {
            let retry = controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    loser_definition,
                    ir::IndexCreateMode::ErrorIfExists,
                )
                .await;
            assert!(matches!(
                retry,
                Err(HelixDbError::IndexDefinitionConflict { .. })
            ));
        } else {
            assert!(matches!(
                loser,
                HelixDbError::IndexDefinitionConflict { .. }
            ));
        }
        let status = db
            .abort_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("conflicting CREATE winner can be explicitly aborted");
        assert!(matches!(
            status,
            IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
        ));
        assert!(matches!(
            drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
            IndexOperationStatus::Aborted { .. }
        ));
        db.close().await.expect("conflicting CREATE writer closes");
    }
}

/// Proves both legal serial orders at the activation/DROP boundary.
async fn run_activation_and_drop_serialization() {
    for drop_wins in [true, false] {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!(
                    "index-lifecycle-lifecycle-activation-drop-{}",
                    if drop_wins { "drop" } else { "activation" }
                ),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("activation/DROP serialization writer opens");
        let controller = LifecycleTestController::new();
        let definition: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_equality("ActivationRace", "value")
                .expect("activation-race definition validates")
                .try_into()
                .expect("activation-race definition converts");
        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("activation-race build is accepted")
        else {
            panic!("fresh activation-race build must enqueue");
        };
        drive_until_stage(
            &db,
            &controller,
            DataScope::LegacyUnscoped,
            operation_id,
            IndexOperationStage::Activate,
        )
        .await;

        if drop_wins {
            assert_eq!(
                controller
                    .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                    .await
                    .expect("DROP serializes before activation"),
                IndexDdlReceipt::ExistingOperation { operation_id }
            );
            assert_identity_hidden(&db, DataScope::LegacyUnscoped, &definition).await;
            assert!(matches!(
                drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
                IndexOperationStatus::Aborted { .. }
            ));
        } else {
            let activation = controller
                .advance(
                    &db,
                    LifecycleWorkTarget::Operation {
                        scope: DataScope::LegacyUnscoped,
                        operation_id,
                    },
                )
                .await
                .expect("activation serializes before DROP");
            assert_eq!(activation.outcome, LifecycleStepOutcome::Completed);
            assert_identity_active(&db, DataScope::LegacyUnscoped, &definition).await;
            let IndexDdlReceipt::Accepted {
                operation_id: drop_operation_id,
                ..
            } = controller
                .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                .await
                .expect("post-activation DROP creates cleanup")
            else {
                panic!("post-activation DROP must own a distinct cleanup operation");
            };
            assert_ne!(drop_operation_id, operation_id);
            assert!(matches!(
                drive_to_terminal(
                    &db,
                    &controller,
                    DataScope::LegacyUnscoped,
                    drop_operation_id,
                )
                .await,
                IndexOperationStatus::Succeeded { .. }
            ));
        }
        db.close()
            .await
            .expect("activation/DROP serialization writer closes");
    }
}

/// Proves a graph transaction spanning active DROP conflicts and retries safely.
async fn run_active_drop_and_mutation_serialization() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-active-drop-mutation".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("active DROP/mutation writer opens");
    let controller = LifecycleTestController::new();
    let scope = DataScope::LegacyUnscoped;
    let entity_id = allocate_node_ids(&db, 1).await.start;
    let before = document_with_values(
        "DropMutation",
        "before@example.com",
        "before-alias",
        "before-unique",
    );
    let after = document_with_values(
        "DropMutation",
        "after@example.com",
        "after-alias",
        "after-unique",
    );
    put_source(&db, scope, entity_id, &before).await;
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("DropMutation", "email")
            .expect("DROP/mutation definition validates")
            .try_into()
            .expect("DROP/mutation definition converts");
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            scope,
            definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("DROP/mutation build is accepted")
    else {
        panic!("fresh DROP/mutation build must enqueue");
    };
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));

    let writer = db
        .lifecycle_test_writer_db()
        .expect("DROP/mutation writer storage is available");
    let transaction = writer
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("spanning graph transaction begins");
    let mutations = crate::index_lifecycle::secondary::load_mutation_set(&transaction, scope)
        .await
        .expect("spanning graph transaction captures Active authority");
    crate::index_lifecycle::secondary::maintain_entity(
        &transaction,
        scope,
        &mutations,
        IndexElementKind::Node,
        entity_id,
        &before,
        &after,
    )
    .await
    .expect("spanning graph transaction stages index maintenance");
    transaction
        .put(source_key(scope, entity_id), encode_properties(&after))
        .expect("spanning graph transaction stages its source row");

    let IndexDdlReceipt::Accepted {
        operation_id: drop_operation_id,
        ..
    } = controller
        .drop_index(&db, scope, &definition)
        .await
        .expect("active DROP serializes first")
    else {
        panic!("active DROP must enqueue cleanup");
    };
    assert!(
        transaction.commit().await.is_err(),
        "transaction retaining old Active authority must conflict with DROP"
    );
    mutate_source(&db, scope, entity_id, &before, &after)
        .await
        .expect("graph mutation retries safely after DROP hides the index");
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, drop_operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    let stored = writer
        .get(source_key(scope, entity_id))
        .await
        .expect("retried graph row remains readable")
        .expect("retried graph row remains present");
    assert_eq!(stored, encode_properties(&after));
    db.close()
        .await
        .expect("active DROP/mutation writer closes");
}

/// Runs real background workers concurrently with bounded public writers.
async fn run_automatic_worker_convergence() {
    for (ordinal, family) in [
        PublicMutationFamily::Secondary,
        PublicMutationFamily::Vector,
        PublicMutationFamily::Text,
    ]
    .into_iter()
    .enumerate()
    {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!("index-lifecycle-lifecycle-automatic-worker-{ordinal}"),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Automatic,
        )
        .await
        .expect("automatic lifecycle writer opens");
        let controller = LifecycleTestController::new();
        let scope = DataScope::LegacyUnscoped;
        controller
            .seed_node_property_rows(
                &db,
                scope,
                NonZeroU64::new(256).expect("automatic seed count is positive"),
                NonZeroUsize::new(64).expect("automatic seed batch is positive"),
                |entity_id| family.source_properties(entity_id),
            )
            .await
            .expect("automatic lifecycle source rows seed");
        let definition = family.definition();
        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .create_index(
                &db,
                scope,
                definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("automatic lifecycle build is accepted")
        else {
            panic!("fresh automatic lifecycle build must enqueue");
        };
        let plans = [
            public_add_node_plan(family, 10),
            public_add_node_plan(family, 11),
            public_add_node_plan(family, 12),
            public_add_node_plan(family, 13),
        ];
        let writes =
            futures::future::join_all(plans.iter().map(|plan| execute_write_with_retry(&db, plan)))
                .await;
        assert!(
            writes.into_iter().all(|result| result.is_ok()),
            "bounded concurrent writers must not exhaust serialization retries"
        );
        assert!(matches!(
            wait_for_automatic_terminal(&db, scope, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        assert_identity_active(&db, scope, &definition).await;
        for mutation_ordinal in 10..14 {
            assert_public_indexed_read(&db, family, mutation_ordinal).await;
        }
        let metrics = controller
            .automatic_metrics(&db)
            .expect("automatic metrics snapshot is available");
        assert!(metrics.operation_steps > 0);
        assert!(metrics
            .stage_transitions
            .iter()
            .any(|transition| { transition.count > 0 }));

        let IndexDdlReceipt::Accepted {
            operation_id: drop_operation_id,
            ..
        } = controller
            .drop_index(&db, scope, &definition)
            .await
            .expect("automatic active DROP is accepted")
        else {
            panic!("automatic active DROP must enqueue cleanup");
        };
        assert!(matches!(
            wait_for_automatic_terminal(&db, scope, drop_operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        assert_identity_hidden(&db, scope, &definition).await;
        db.close().await.expect("automatic lifecycle writer closes");
    }
}

/// Polls one automatic operation with a fixed parallel-suite timeout.
async fn wait_for_automatic_terminal(
    db: &HelixDB,
    scope: DataScope,
    operation_id: crate::index_lifecycle::IndexOperationId,
) -> IndexOperationStatus {
    let mut last_status = None;
    for _ in 0..4_800 {
        let status = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("automatic operation remains readable");
        if matches!(
            status,
            IndexOperationStatus::Succeeded { .. }
                | IndexOperationStatus::Blocked { .. }
                | IndexOperationStatus::Aborted { .. }
        ) {
            return status;
        }
        last_status = Some(status);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("automatic operation exceeded its 120-second deterministic timeout: {last_status:?}");
}

/// Runs data-bearing backfill/mutation and uniqueness contracts.
pub(super) async fn run_mutations() {
    run_secondary_mutation_interleaving().await;
    run_secondary_edge_mutation_interleaving().await;
    run_every_family_public_mutation_interleaving().await;
    run_partitioned_search_edge_tenant_moves().await;
}

/// Runs late insert, update, and delete races at every family validation boundary.
pub(super) async fn run_all_index_validation_mutations() {
    all_index_validation::run().await;
}

/// Interleaves source changes with two builds and proves unique repair/retry.
async fn run_secondary_mutation_interleaving() {
    let config = DbConfig::new().with_secondary_index_lifecycle_tuning(
        SecondaryIndexLifecycleTuning::default().with_batch_rows(
            SecondaryIndexLifecycleBatchRows::new(4).expect("test batch is positive"),
        ),
    );
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-secondary-mutations".to_string(),
        },
        config,
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("secondary mutation writer opens");
    let controller = LifecycleTestController::new();
    let scope = DataScope::LegacyUnscoped;
    let initial_ids = allocate_node_ids(&db, 16).await;
    for entity_id in initial_ids.clone() {
        put_source(
            &db,
            scope,
            entity_id,
            &document(entity_id, "IndexedUser", None),
        )
        .await;
    }

    let email_definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("IndexedUser", "email")
            .expect("email definition validates")
            .try_into()
            .expect("email definition converts");
    let IndexDdlReceipt::Accepted {
        operation_id: email_operation_id,
        ..
    } = controller
        .create_index(
            &db,
            scope,
            email_definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("email build is accepted")
    else {
        panic!("fresh email definition must enqueue");
    };
    let first_scan = controller
        .advance(
            &db,
            LifecycleWorkTarget::Operation {
                scope,
                operation_id: email_operation_id,
            },
        )
        .await
        .expect("first bounded email scan commits");
    assert_monotonic_step(&first_scan);
    assert_eq!(first_scan.resources.entities, 4);

    let first_before = document(initial_ids.start, "IndexedUser", None);
    let first_after = document_with_values(
        "IndexedUser",
        "updated-before-watermark@example.com",
        "alias-updated-before-watermark",
        format!("unique-{}", initial_ids.start),
    );
    mutate_source(&db, scope, initial_ids.start, &first_before, &first_after)
        .await
        .expect("already-scanned update commits as one build delta");

    let removed_id = initial_ids.start + 1;
    let removed_before = document(removed_id, "IndexedUser", None);
    let removed_after = vec![Property::new(
        "$label",
        PropertyValue::String("IndexedUser".to_string()),
    )];
    mutate_source(&db, scope, removed_id, &removed_before, &removed_after)
        .await
        .expect("property removal commits as one build delta");
    let readded_after = document_with_values(
        "IndexedUser",
        "readded-after-removal@example.com",
        "readded-after-removal-alias",
        format!("unique-{removed_id}"),
    );
    mutate_source(&db, scope, removed_id, &removed_after, &readded_after)
        .await
        .expect("property re-addition coalesces into the same build delta");

    let relabelled_id = initial_ids.start + 2;
    let relabelled_before = document(relabelled_id, "IndexedUser", None);
    let relabelled_after = document(relabelled_id, "OtherUser", None);
    mutate_source(
        &db,
        scope,
        relabelled_id,
        &relabelled_before,
        &relabelled_after,
    )
    .await
    .expect("label change commits as one build delta");

    let repeated_id = initial_ids.start + 3;
    let repeated_before = document(repeated_id, "IndexedUser", None);
    let repeated_middle = document_with_values(
        "IndexedUser",
        "repeated-middle@example.com",
        "repeated-middle-alias",
        format!("unique-{repeated_id}"),
    );
    let repeated_after = document_with_values(
        "IndexedUser",
        "repeated-final@example.com",
        "repeated-final-alias",
        format!("unique-{repeated_id}"),
    );
    mutate_source(&db, scope, repeated_id, &repeated_before, &repeated_middle)
        .await
        .expect("first repeated update commits");
    mutate_source(&db, scope, repeated_id, &repeated_middle, &repeated_after)
        .await
        .expect("second repeated update coalesces");

    let inserted_id = allocate_node_ids(&db, 1).await.start;
    let inserted = document_with_values(
        "IndexedUser",
        "inserted-after-watermark@example.com",
        "inserted-after-watermark-alias",
        format!("unique-{inserted_id}"),
    );
    mutate_source(&db, scope, inserted_id, &[], &inserted)
        .await
        .expect("post-watermark insert commits as one build delta");

    let deleted_id = initial_ids.start + 5;
    let deleted_before = document(deleted_id, "IndexedUser", None);
    mutate_source(&db, scope, deleted_id, &deleted_before, &[])
        .await
        .expect("pre-scan delete commits as one build delta");

    drive_until_stage(
        &db,
        &controller,
        scope,
        email_operation_id,
        IndexOperationStage::Activate,
    )
    .await;
    let spanning_id = initial_ids.start + 7;
    let spanning_before = document(spanning_id, "IndexedUser", None);
    let spanning_after = document_with_values(
        "IndexedUser",
        "retried-across-activation@example.com",
        "retried-across-activation-alias",
        format!("unique-{spanning_id}"),
    );
    let writer = db
        .lifecycle_test_writer_db()
        .expect("activation-spanning mutation has writer storage");
    let transaction = writer
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("activation-spanning mutation begins");
    let mutations = crate::index_lifecycle::secondary::load_mutation_set(&transaction, scope)
        .await
        .expect("activation-spanning mutation captures Building authority");
    crate::index_lifecycle::secondary::maintain_entity(
        &transaction,
        scope,
        &mutations,
        IndexElementKind::Node,
        spanning_id,
        &spanning_before,
        &spanning_after,
    )
    .await
    .expect("activation-spanning mutation stages index work");
    transaction
        .put(
            source_key(scope, spanning_id),
            encode_properties(&spanning_after),
        )
        .expect("activation-spanning mutation stages graph work");
    let activated = controller
        .advance(
            &db,
            LifecycleWorkTarget::Operation {
                scope,
                operation_id: email_operation_id,
            },
        )
        .await
        .expect("email generation activates before spanning mutation");
    assert_eq!(activated.outcome, LifecycleStepOutcome::Completed);
    assert!(
        transaction.commit().await.is_err(),
        "transaction retaining Building authority must conflict with activation"
    );
    mutate_source(&db, scope, spanning_id, &spanning_before, &spanning_after)
        .await
        .expect("activation-spanning mutation retries against Active authority");
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, email_operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert_equality(&db, scope, &email_definition, "email-15@example.com", [15]).await;
    assert_equality(
        &db,
        scope,
        &email_definition,
        "updated-before-watermark@example.com",
        [initial_ids.start],
    )
    .await;
    assert_equality(
        &db,
        scope,
        &email_definition,
        "inserted-after-watermark@example.com",
        [inserted_id],
    )
    .await;
    assert_equality(&db, scope, &email_definition, "email-1@example.com", []).await;
    assert_equality(
        &db,
        scope,
        &email_definition,
        "readded-after-removal@example.com",
        [removed_id],
    )
    .await;
    assert_equality(&db, scope, &email_definition, "email-5@example.com", []).await;
    assert_equality(
        &db,
        scope,
        &email_definition,
        "retried-across-activation@example.com",
        [spanning_id],
    )
    .await;
    assert_build_deltas_empty(&db, scope, &email_definition).await;

    let alias_definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("IndexedUser", "alias")
            .expect("alias definition validates")
            .try_into()
            .expect("alias definition converts");
    let IndexDdlReceipt::Accepted {
        operation_id: alias_operation_id,
        ..
    } = controller
        .create_index(
            &db,
            scope,
            alias_definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("second build is accepted while email is Active")
    else {
        panic!("fresh alias definition must enqueue");
    };
    let both_id = initial_ids.start + 4;
    let both_before = document(both_id, "IndexedUser", None);
    let both_after = document_with_values(
        "IndexedUser",
        "active-email-updated@example.com",
        "building-alias-updated",
        format!("unique-{both_id}"),
    );
    mutate_source(&db, scope, both_id, &both_before, &both_after)
        .await
        .expect("one transaction maintains Active email and Building alias");
    assert_equality(
        &db,
        scope,
        &email_definition,
        "active-email-updated@example.com",
        [both_id],
    )
    .await;
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, alias_operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert_equality(
        &db,
        scope,
        &alias_definition,
        "building-alias-updated",
        [both_id],
    )
    .await;
    assert_build_deltas_empty(&db, scope, &alias_definition).await;

    let unique_ids = allocate_node_ids(&db, 2).await;
    let duplicate = "duplicate-while-building";
    for entity_id in unique_ids.clone() {
        let unique = if entity_id == unique_ids.start {
            duplicate.to_string()
        } else {
            format!("initial-unique-{entity_id}")
        };
        let properties = document_with_values(
            "IndexedUser",
            format!("unique-source-{entity_id}@example.com"),
            format!("unique-source-alias-{entity_id}"),
            unique,
        );
        mutate_source(&db, scope, entity_id, &[], &properties)
            .await
            .expect("unique source row commits before unique build");
    }
    let unique_definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_unique_equality("IndexedUser", "unique")
            .expect("unique definition validates")
            .try_into()
            .expect("unique definition converts");
    let IndexDdlReceipt::Accepted {
        operation_id: unique_operation_id,
        ..
    } = controller
        .create_index(
            &db,
            scope,
            unique_definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("unique build is accepted")
    else {
        panic!("fresh unique definition must enqueue");
    };
    drive_until_stage(
        &db,
        &controller,
        scope,
        unique_operation_id,
        IndexOperationStage::Validate,
    )
    .await;
    let conflicting_id = unique_ids.end - 1;
    let conflicting_before = document_with_values(
        "IndexedUser",
        format!("unique-source-{conflicting_id}@example.com"),
        format!("unique-source-alias-{conflicting_id}"),
        format!("initial-unique-{conflicting_id}"),
    );
    let conflicting_after = document_with_values(
        "IndexedUser",
        format!("unique-source-{conflicting_id}@example.com"),
        format!("unique-source-alias-{conflicting_id}"),
        duplicate,
    );
    mutate_source(
        &db,
        scope,
        conflicting_id,
        &conflicting_before,
        &conflicting_after,
    )
    .await
    .expect("duplicate introduced during validation commits as build delta");
    let blocked = drive_to_terminal(&db, &controller, scope, unique_operation_id).await;
    assert!(matches!(blocked, IndexOperationStatus::Blocked { .. }));

    let repaired_id = unique_ids.end - 1;
    let repaired_before = document_with_values(
        "IndexedUser",
        format!("unique-source-{repaired_id}@example.com"),
        format!("unique-source-alias-{repaired_id}"),
        duplicate,
    );
    let repaired_after = document_with_values(
        "IndexedUser",
        format!("unique-source-{repaired_id}@example.com"),
        format!("unique-source-alias-{repaired_id}"),
        "repaired-unique-value",
    );
    mutate_source(&db, scope, repaired_id, &repaired_before, &repaired_after)
        .await
        .expect("duplicate repair commits while build is blocked");
    let retried = db
        .retry_index_operation(scope, unique_operation_id)
        .await
        .expect("unique build retries from its exact checkpoint");
    assert_eq!(retried.common().stage, blocked.common().stage);
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, unique_operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert_equality(
        &db,
        scope,
        &unique_definition,
        duplicate,
        [unique_ids.start],
    )
    .await;

    let conflicting_before = repaired_after;
    let conflicting_after = document_with_values(
        "IndexedUser",
        format!("unique-source-{repaired_id}@example.com"),
        format!("unique-source-alias-{repaired_id}"),
        duplicate,
    );
    assert!(
        mutate_source(
            &db,
            scope,
            repaired_id,
            &conflicting_before,
            &conflicting_after,
        )
        .await
        .is_err(),
        "Active unique duplicate must reject the graph transaction"
    );
    assert_equality(
        &db,
        scope,
        &unique_definition,
        "repaired-unique-value",
        [repaired_id],
    )
    .await;

    let abort_ids = allocate_node_ids(&db, 2).await;
    for entity_id in abort_ids {
        put_source(
            &db,
            scope,
            entity_id,
            &document_with_values(
                "AbortUnique",
                format!("abort-{entity_id}@example.com"),
                format!("abort-alias-{entity_id}"),
                "abort-duplicate",
            ),
        )
        .await;
    }
    let abort_definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_unique_equality("AbortUnique", "unique")
            .expect("blocked-abort definition validates")
            .try_into()
            .expect("blocked-abort definition converts");
    let IndexDdlReceipt::Accepted {
        operation_id: abort_operation_id,
        ..
    } = controller
        .create_index(
            &db,
            scope,
            abort_definition,
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("blocked-abort build is accepted")
    else {
        panic!("fresh blocked-abort build must enqueue");
    };
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, abort_operation_id).await,
        IndexOperationStatus::Blocked { .. }
    ));
    let aborting = db
        .abort_index_operation(scope, abort_operation_id)
        .await
        .expect("explicit abort converts a blocked build into cleanup");
    assert!(matches!(
        aborting,
        IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
    ));
    assert_eq!(
        db.abort_index_operation(scope, abort_operation_id)
            .await
            .expect("repeated explicit abort converges")
            .common()
            .stage,
        aborting.common().stage
    );
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, abort_operation_id).await,
        IndexOperationStatus::Aborted { .. }
    ));
    db.close().await.expect("secondary mutation writer closes");
}

/// Exercises edge inserts, updates, deletes, and Active maintenance.
async fn run_secondary_edge_mutation_interleaving() {
    let config = DbConfig::new().with_secondary_index_lifecycle_tuning(
        SecondaryIndexLifecycleTuning::default().with_batch_rows(
            SecondaryIndexLifecycleBatchRows::new(2).expect("edge scan batch is positive"),
        ),
    );
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-secondary-edge-mutations".to_string(),
        },
        config,
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("secondary edge mutation writer opens");
    let controller = LifecycleTestController::new();
    let scope = DataScope::LegacyUnscoped;
    let edge_ids = allocate_edge_ids(&db, 8).await;
    for edge_id in edge_ids.clone() {
        put_edge_source(&db, scope, edge_id, &document(edge_id, "IndexedEdge", None)).await;
    }
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::edge_equality("IndexedEdge", "email")
            .expect("edge equality definition validates")
            .try_into()
            .expect("edge equality definition converts");
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            scope,
            definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("edge equality build is accepted")
    else {
        panic!("fresh edge equality build must enqueue");
    };
    let first_scan = controller
        .advance(
            &db,
            LifecycleWorkTarget::Operation {
                scope,
                operation_id,
            },
        )
        .await
        .expect("first bounded edge scan commits");
    assert_eq!(first_scan.resources.entities, 2);

    let updated_id = edge_ids.start;
    let updated_before = document(updated_id, "IndexedEdge", None);
    let updated_after = document_with_values(
        "IndexedEdge",
        "edge-updated-building@example.com",
        "edge-updated-building-alias",
        format!("unique-{updated_id}"),
    );
    mutate_edge_source(&db, scope, updated_id, &updated_before, &updated_after)
        .await
        .expect("already-scanned edge update records a build delta");
    let deleted_id = edge_ids.start + 3;
    mutate_edge_source(
        &db,
        scope,
        deleted_id,
        &document(deleted_id, "IndexedEdge", None),
        &[],
    )
    .await
    .expect("edge delete before scan records a build delta");
    let inserted_id = allocate_edge_ids(&db, 1).await.start;
    let inserted = document_with_values(
        "IndexedEdge",
        "edge-inserted-after-watermark@example.com",
        "edge-inserted-after-watermark-alias",
        format!("unique-{inserted_id}"),
    );
    mutate_edge_source(&db, scope, inserted_id, &[], &inserted)
        .await
        .expect("post-watermark edge insert records a build delta");

    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert_equality(
        &db,
        scope,
        &definition,
        "edge-updated-building@example.com",
        [updated_id],
    )
    .await;
    assert_equality(
        &db,
        scope,
        &definition,
        "edge-inserted-after-watermark@example.com",
        [inserted_id],
    )
    .await;
    assert_equality(
        &db,
        scope,
        &definition,
        &format!("email-{deleted_id}@example.com"),
        [],
    )
    .await;

    let active_id = edge_ids.start + 2;
    let active_before = document(active_id, "IndexedEdge", None);
    let active_after = document_with_values(
        "IndexedEdge",
        "edge-updated-active@example.com",
        "edge-updated-active-alias",
        format!("unique-{active_id}"),
    );
    mutate_edge_source(&db, scope, active_id, &active_before, &active_after)
        .await
        .expect("Active edge mutation maintains physical rows directly");
    assert_equality(
        &db,
        scope,
        &definition,
        "edge-updated-active@example.com",
        [active_id],
    )
    .await;
    assert_build_deltas_empty(&db, scope, &definition).await;

    let IndexDdlReceipt::Accepted {
        operation_id: drop_operation_id,
        ..
    } = controller
        .drop_index(&db, scope, &definition)
        .await
        .expect("edge mutation fixture accepts cleanup")
    else {
        panic!("active edge mutation fixture must enqueue cleanup");
    };
    assert!(matches!(
        drive_to_terminal(&db, &controller, scope, drop_operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    db.close()
        .await
        .expect("secondary edge mutation writer closes");
}

const PUBLIC_MUTATION_LABEL: &str = "LifecycleMutation";
const PUBLIC_MUTATION_PROPERTY: &str = "value";
const PUBLIC_MUTATION_TENANT: &str = "tenant";

/// Family-refined public write/read fixture used by the mutation matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicMutationFamily {
    Secondary,
    Vector,
    Text,
}

impl PublicMutationFamily {
    /// Returns the exact validated definition installed by this fixture.
    fn definition(self) -> ValidatedDynamicIndexDefinition {
        match self {
            Self::Secondary => SecondaryIndexDefinition::node_equality(
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
            )
            .expect("public secondary mutation definition validates")
            .try_into()
            .expect("public secondary mutation definition converts"),
            Self::Vector => VectorIndexDefinition::new_node(
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("public vector mutation definition validates")
            .try_into()
            .expect("public vector mutation definition converts"),
            Self::Text => {
                TextIndexDefinition::new_node(PUBLIC_MUTATION_LABEL, PUBLIC_MUTATION_PROPERTY)
                    .expect("public text mutation definition validates")
                    .try_into()
                    .expect("public text mutation definition converts")
            }
        }
    }

    /// Returns one deterministic source document for initial backfill.
    fn source_properties(self, entity_id: u64) -> Vec<Property> {
        let value = match self {
            Self::Secondary => {
                Property::string(PUBLIC_MUTATION_PROPERTY, format!("seed-{entity_id}"))
            }
            Self::Vector => Property::f32_array(
                PUBLIC_MUTATION_PROPERTY,
                vec![
                    entity_id as f32,
                    entity_id as f32 + 1.0,
                    entity_id as f32 + 2.0,
                ],
            ),
            Self::Text => Property::string(
                PUBLIC_MUTATION_PROPERTY,
                format!("shared lifecycle seedtoken{entity_id}"),
            ),
        };
        vec![Property::string("$label", PUBLIC_MUTATION_LABEL), value]
    }

    /// Returns one value that can be recovered through the family index.
    fn inserted_value(self, ordinal: u64) -> AstPropertyValue {
        match self {
            Self::Secondary => AstPropertyValue::String(format!("inserted-{ordinal}")),
            Self::Vector => {
                let ordinal = ordinal as f32;
                AstPropertyValue::F32Array(vec![
                    -10_000.0 - ordinal,
                    -20_000.0 - ordinal,
                    -30_000.0 - ordinal,
                ])
            }
            Self::Text => AstPropertyValue::String(format!(
                "shared lifecycle uniquelifecycleinserted{ordinal}"
            )),
        }
    }

    /// Returns the authoritative persisted property expected for one insert.
    fn inserted_property(self, ordinal: u64) -> Property {
        match self {
            Self::Secondary => {
                Property::string(PUBLIC_MUTATION_PROPERTY, format!("inserted-{ordinal}"))
            }
            Self::Vector => {
                let ordinal = ordinal as f32;
                Property::f32_array(
                    PUBLIC_MUTATION_PROPERTY,
                    vec![
                        -10_000.0 - ordinal,
                        -20_000.0 - ordinal,
                        -30_000.0 - ordinal,
                    ],
                )
            }
            Self::Text => Property::string(
                PUBLIC_MUTATION_PROPERTY,
                format!("shared lifecycle uniquelifecycleinserted{ordinal}"),
            ),
        }
    }
}

/// Runs real interpreter writes while every family is Building and Active.
async fn run_every_family_public_mutation_interleaving() {
    for (ordinal, family) in [
        PublicMutationFamily::Secondary,
        PublicMutationFamily::Vector,
        PublicMutationFamily::Text,
    ]
    .into_iter()
    .enumerate()
    {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!("index-lifecycle-lifecycle-public-mutation-{ordinal}"),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("public mutation writer opens");
        let controller = LifecycleTestController::new();
        let scope = DataScope::LegacyUnscoped;
        let _seeded = controller
            .seed_node_property_rows(
                &db,
                scope,
                NonZeroU64::new(32).expect("mutation seed count is positive"),
                NonZeroUsize::new(8).expect("mutation seed batch is positive"),
                |entity_id| family.source_properties(entity_id),
            )
            .await
            .expect("public mutation source rows seed");
        let definition = family.definition();
        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .create_index(
                &db,
                scope,
                definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("public mutation build is accepted")
        else {
            panic!("fresh public mutation build must enqueue");
        };
        assert_identity_hidden(&db, scope, &definition).await;
        let first_step = controller
            .advance(
                &db,
                LifecycleWorkTarget::Operation {
                    scope,
                    operation_id,
                },
            )
            .await
            .expect("public mutation build commits its first step");
        assert_monotonic_step(&first_step);

        let first_plan = public_add_node_plan(family, 0);
        let second_plan = public_add_node_plan(family, 1);
        let (first_write, second_write) = tokio::join!(
            execute_write_with_retry(&db, &first_plan),
            execute_write_with_retry(&db, &second_plan),
        );
        first_write.expect("first concurrent Building write commits after bounded retries");
        second_write.expect("second concurrent Building write commits after bounded retries");
        assert_build_delta_count_at_least(&db, scope, &definition, 2).await;

        assert!(matches!(
            drive_to_terminal(&db, &controller, scope, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        assert_identity_active(&db, scope, &definition).await;
        assert_build_deltas_empty(&db, scope, &definition).await;
        let first_id = assert_public_indexed_read(&db, family, 0).await;
        let second_id = assert_public_indexed_read(&db, family, 1).await;
        assert_ne!(first_id, second_id);

        execute_write_with_retry(&db, &public_add_node_plan(family, 2))
            .await
            .expect("Active public mutation commits after bounded retries");
        let active_id = assert_public_indexed_read(&db, family, 2).await;
        assert_ne!(active_id, first_id);
        assert_ne!(active_id, second_id);
        assert_build_deltas_empty(&db, scope, &definition).await;

        let IndexDdlReceipt::Accepted {
            operation_id: drop_operation_id,
            ..
        } = controller
            .drop_index(&db, scope, &definition)
            .await
            .expect("public mutation fixture accepts cleanup")
        else {
            panic!("active public mutation fixture must enqueue cleanup");
        };
        assert!(matches!(
            drive_to_terminal(&db, &controller, scope, drop_operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        db.close().await.expect("public mutation writer closes");
    }
}

/// Moves partitioned vector/text edges during Building and Active states.
async fn run_partitioned_search_edge_tenant_moves() {
    for (family_ordinal, family) in [PublicMutationFamily::Vector, PublicMutationFamily::Text]
        .into_iter()
        .enumerate()
    {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!("index-lifecycle-lifecycle-partitioned-edge-{family_ordinal}"),
            },
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("partitioned edge mutation writer opens");
        let controller = LifecycleTestController::new();
        let scope = DataScope::LegacyUnscoped;
        let endpoint_ids = allocate_node_ids(&db, 2).await;
        for endpoint_id in endpoint_ids.clone() {
            put_source(
                &db,
                scope,
                endpoint_id,
                &[Property::string("$label", "LifecycleEndpoint")],
            )
            .await;
        }
        let definition: ValidatedDynamicIndexDefinition = match family {
            PublicMutationFamily::Vector => VectorIndexDefinition::new_edge(
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("partitioned edge vector validates")
            .with_tenant_property(PUBLIC_MUTATION_TENANT)
            .expect("partitioned edge vector tenant validates")
            .try_into()
            .expect("partitioned edge vector converts"),
            PublicMutationFamily::Text => {
                TextIndexDefinition::new_edge(PUBLIC_MUTATION_LABEL, PUBLIC_MUTATION_PROPERTY)
                    .expect("partitioned edge text validates")
                    .with_tenant_property(PUBLIC_MUTATION_TENANT)
                    .expect("partitioned edge text tenant validates")
                    .try_into()
                    .expect("partitioned edge text converts")
            }
            PublicMutationFamily::Secondary => {
                unreachable!("secondary has a dedicated edge matrix")
            }
        };
        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .create_index(
                &db,
                scope,
                definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("partitioned edge build is accepted")
        else {
            panic!("fresh partitioned edge build must enqueue");
        };
        controller
            .advance(
                &db,
                LifecycleWorkTarget::Operation {
                    scope,
                    operation_id,
                },
            )
            .await
            .expect("partitioned edge build advances before mutation");

        let ordinal = 40 + family_ordinal as u64;
        let from_param = public_name("from");
        let added = execute_write_with_bindings_retry(
            &db,
            &public_add_edge_plan(family, ordinal, endpoint_ids.end - 1),
            context::ParamBindings::default().with_value(
                from_param,
                AstPropertyValue::I64(
                    i64::try_from(endpoint_ids.start).expect("endpoint ID fits i64"),
                ),
            ),
        )
        .await
        .expect("partitioned edge insert commits while Building");
        let Some(ExecutionValue::Stream(rows)) = added.last else {
            panic!("edge insert returns its created row");
        };
        let Some(ExecutionRow {
            current: Some(ElementRef::Edge(edge_id)),
            ..
        }) = rows.first()
        else {
            panic!("edge insert returns one edge ID");
        };
        let edge_id = *edge_id;
        execute_write_with_bindings_retry(
            &db,
            &public_set_edge_tenant_plan("tenant-building"),
            edge_binding(edge_id),
        )
        .await
        .expect("tenant move commits while partitioned edge index is Building");
        assert_build_delta_count_at_least(&db, scope, &definition, 1).await;
        assert!(matches!(
            drive_to_terminal(&db, &controller, scope, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        assert_identity_active(&db, scope, &definition).await;
        assert_build_deltas_empty(&db, scope, &definition).await;
        assert_edge_indexed_read(&db, family, ordinal, "tenant-initial", []).await;
        assert_edge_indexed_read(&db, family, ordinal, "tenant-building", [edge_id]).await;

        execute_write_with_bindings_retry(
            &db,
            &public_set_edge_tenant_plan("tenant-active"),
            edge_binding(edge_id),
        )
        .await
        .expect("tenant move commits while partitioned edge index is Active");
        assert_edge_indexed_read(&db, family, ordinal, "tenant-building", []).await;
        assert_edge_indexed_read(&db, family, ordinal, "tenant-active", [edge_id]).await;

        execute_write_with_retry(&db, &public_drop_edge_plan(edge_id))
            .await
            .expect("partitioned edge delete commits while Active");
        assert_edge_indexed_read(&db, family, ordinal, "tenant-active", []).await;
        let IndexDdlReceipt::Accepted {
            operation_id: drop_operation_id,
            ..
        } = controller
            .drop_index(&db, scope, &definition)
            .await
            .expect("partitioned edge fixture accepts cleanup")
        else {
            panic!("partitioned edge cleanup must enqueue");
        };
        assert!(matches!(
            drive_to_terminal(&db, &controller, scope, drop_operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        db.close()
            .await
            .expect("partitioned edge mutation writer closes");
    }
}

/// Retries only expected serializable conflicts and rejects exhaustion.
async fn execute_write_with_retry(
    db: &HelixDB,
    plan: &exec::ExecutablePlan,
) -> crate::error::Result<ExecutionResult> {
    execute_write_with_bindings_retry(db, plan, context::ParamBindings::default()).await
}

async fn execute_write_with_bindings_retry(
    db: &HelixDB,
    plan: &exec::ExecutablePlan,
    bindings: context::ParamBindings,
) -> crate::error::Result<ExecutionResult> {
    for _ in 0..8 {
        match db.execute(plan, bindings.clone()).await {
            Ok(result) => return Ok(result),
            Err(error) if error.is_transaction_conflict() => continue,
            Err(error) => return Err(error),
        }
    }
    Err(HelixDbError::InvariantViolation(
        "public lifecycle mutation exhausted eight serializable retries".to_string(),
    ))
}

/// Counts coalesced BUILD deltas without retaining entity-sized test state.
async fn assert_build_delta_count_at_least(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    minimum: usize,
) {
    let writer = db
        .lifecycle_test_writer_db()
        .expect("delta count has writer storage");
    let record = crate::index_lifecycle::repository::load_index_record(
        writer,
        scope,
        &definition.identity(),
    )
    .await
    .expect("delta count canonical row decodes")
    .expect("delta count canonical row exists");
    let prefix = DataKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(
            RecordKind::BuildDelta,
            record.index_id(),
            record.state().generation(),
        ),
    );
    let mut rows = writer
        .scan_prefix(&prefix, ..)
        .await
        .expect("build-delta count remains readable");
    let mut count = 0usize;
    while rows
        .next()
        .await
        .expect("build-delta count scan succeeds")
        .is_some()
    {
        count = count.saturating_add(1);
    }
    assert!(
        count >= minimum,
        "Building public writes must retain at least {minimum} coalesced deltas, observed {count}"
    );
}

/// Verifies one Building/Active mutation through the public indexed read path.
async fn assert_public_indexed_read(
    db: &HelixDB,
    family: PublicMutationFamily,
    ordinal: u64,
) -> u64 {
    let result = db
        .execute(
            &public_indexed_read_plan(family, ordinal),
            context::ParamBindings::default(),
        )
        .await
        .expect("public indexed read succeeds");
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("public indexed read must return projected scalars");
    };
    let ids = values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::NodeId(id) = value else {
                panic!("public indexed read must return only node IDs");
            };
            id
        })
        .collect::<Vec<_>>();
    if family == PublicMutationFamily::Text {
        assert_eq!(ids.len(), 1);
    }
    let entity_id = *ids
        .first()
        .expect("family-specific indexed read returns one authoritative entity");
    let stored = db
        .lifecycle_test_writer_db()
        .expect("public indexed read has writer storage")
        .get(source_key(DataScope::LegacyUnscoped, entity_id))
        .await
        .expect("indexed entity source row remains readable")
        .expect("indexed entity source row remains present");
    let properties = crate::encoding::property::decode_properties(&stored)
        .expect("indexed entity source properties decode");
    assert!(properties.contains(&family.inserted_property(ordinal)));
    entity_id
}

/// Allocates one contiguous authoritative node-ID range.
async fn allocate_node_ids(db: &HelixDB, count: u64) -> std::ops::Range<u64> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("lifecycle mutation contracts require writer storage");
    };
    writer
        .node_ids()
        .allocate_batch(count)
        .await
        .expect("test node IDs allocate")
}

/// Allocates one contiguous authoritative edge-ID range.
async fn allocate_edge_ids(db: &HelixDB, count: u64) -> std::ops::Range<u64> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("lifecycle edge contracts require writer storage");
    };
    writer
        .edge_ids()
        .allocate_batch(count)
        .await
        .expect("test edge IDs allocate")
}

/// Returns one typed authoritative node-property key.
fn source_key(scope: DataScope, entity_id: u64) -> bytes::Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
    }
    .to_bytes()
}

/// Returns one typed authoritative edge-property key.
fn edge_source_key(scope: DataScope, entity_id: u64) -> bytes::Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity_id)),
    }
    .to_bytes()
}

/// Returns one complete deterministic source document.
fn document(entity_id: u64, label: &str, unique: Option<&str>) -> Vec<Property> {
    document_with_values(
        label,
        format!("email-{entity_id}@example.com"),
        format!("alias-{entity_id}"),
        unique
            .map(str::to_string)
            .unwrap_or_else(|| format!("unique-{entity_id}")),
    )
}

/// Returns one complete source document with explicit indexed values.
fn document_with_values(
    label: impl Into<String>,
    email: impl Into<String>,
    alias: impl Into<String>,
    unique: impl Into<String>,
) -> Vec<Property> {
    vec![
        Property::new("$label", PropertyValue::String(label.into())),
        Property::new("email", PropertyValue::String(email.into())),
        Property::new("alias", PropertyValue::String(alias.into())),
        Property::new("unique", PropertyValue::String(unique.into())),
    ]
}

/// Writes setup data before any lifecycle definition exists.
async fn put_source(db: &HelixDB, scope: DataScope, entity_id: u64, properties: &[Property]) {
    db.lifecycle_test_writer_db()
        .expect("source setup has writer storage")
        .put(source_key(scope, entity_id), encode_properties(properties))
        .await
        .expect("source setup row commits");
}

/// Writes edge setup data before any lifecycle definition exists.
async fn put_edge_source(db: &HelixDB, scope: DataScope, entity_id: u64, properties: &[Property]) {
    db.lifecycle_test_writer_db()
        .expect("edge source setup has writer storage")
        .put(
            edge_source_key(scope, entity_id),
            encode_properties(properties),
        )
        .await
        .expect("edge source setup row commits");
}

/// Atomically maintains every applicable secondary generation and graph source.
async fn mutate_source(
    db: &HelixDB,
    scope: DataScope,
    entity_id: u64,
    before: &[Property],
    after: &[Property],
) -> crate::error::Result<()> {
    let _scope_permit = db.index_mutation_scope_permit(scope).await;
    let writer = db.lifecycle_test_writer_db()?;
    let transaction = writer.begin(IsolationLevel::SerializableSnapshot).await?;
    let mutations =
        crate::index_lifecycle::secondary::load_mutation_set(&transaction, scope).await?;
    crate::index_lifecycle::secondary::maintain_entity(
        &transaction,
        scope,
        &mutations,
        IndexElementKind::Node,
        entity_id,
        before,
        after,
    )
    .await?;
    if after.is_empty() {
        transaction.delete(source_key(scope, entity_id))?;
    } else {
        transaction.put(source_key(scope, entity_id), encode_properties(after))?;
    }
    transaction.commit().await?;
    Ok(())
}

/// Atomically maintains every applicable secondary edge generation and source.
async fn mutate_edge_source(
    db: &HelixDB,
    scope: DataScope,
    entity_id: u64,
    before: &[Property],
    after: &[Property],
) -> crate::error::Result<()> {
    let _scope_permit = db.index_mutation_scope_permit(scope).await;
    let writer = db.lifecycle_test_writer_db()?;
    let transaction = writer.begin(IsolationLevel::SerializableSnapshot).await?;
    let mutations =
        crate::index_lifecycle::secondary::load_mutation_set(&transaction, scope).await?;
    crate::index_lifecycle::secondary::maintain_entity(
        &transaction,
        scope,
        &mutations,
        IndexElementKind::Edge,
        entity_id,
        before,
        after,
    )
    .await?;
    if after.is_empty() {
        transaction.delete(edge_source_key(scope, entity_id))?;
    } else {
        transaction.put(edge_source_key(scope, entity_id), encode_properties(after))?;
    }
    transaction.commit().await?;
    Ok(())
}

/// Compares one active physical equality lookup with an exact ID set.
async fn assert_equality<const N: usize>(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    value: &str,
    expected: [u64; N],
) {
    let record = crate::index_lifecycle::repository::load_index_record(
        db.lifecycle_test_writer_db()
            .expect("equality lookup has writer storage"),
        scope,
        &definition.identity(),
    )
    .await
    .expect("active equality canonical row decodes")
    .expect("active equality canonical row exists");
    let handle = ActiveIndexHandle::try_from_record(scope, &record)
        .expect("active equality record projects one handle");
    let actual = crate::index_lifecycle::secondary::lookup_active_equality_generation(
        db.lifecycle_test_writer_db()
            .expect("equality lookup has writer storage"),
        &handle,
        &PropertyValue::String(value.to_string()),
    )
    .await
    .expect("active equality lookup succeeds")
    .iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected.into_iter().collect());
}

/// Proves activation removed every coalesced build-delta row for a generation.
async fn assert_build_deltas_empty(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
) {
    let writer = db
        .lifecycle_test_writer_db()
        .expect("delta scan has writer storage");
    let record = crate::index_lifecycle::repository::load_index_record(
        writer,
        scope,
        &definition.identity(),
    )
    .await
    .expect("delta canonical row decodes")
    .expect("delta canonical row exists");
    let prefix = DataKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(
            RecordKind::BuildDelta,
            record.index_id(),
            record.state().generation(),
        ),
    );
    let mut rows = writer
        .scan_prefix(&prefix, ..)
        .await
        .expect("build-delta lane remains readable");
    assert!(
        rows.next()
            .await
            .expect("build-delta scan succeeds")
            .is_none(),
        "Active generation must not retain build deltas"
    );
}

/// Refreshes the planner catalog and proves one non-Active identity is hidden.
async fn assert_identity_hidden(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
) {
    db.refresh_runtime_catalog(scope)
        .await
        .expect("planner catalog refresh succeeds");
    assert!(
        db.active_index_handles_loaded(scope)
            .iter()
            .all(|handle| handle.identity() != &definition.identity()),
        "Building, Aborting, Dropping, and Dropped generations must be planner-hidden"
    );
}

/// Refreshes the planner catalog and proves one exact identity is Active.
async fn assert_identity_active(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
) {
    db.refresh_runtime_catalog(scope)
        .await
        .expect("planner catalog refresh succeeds");
    assert_eq!(
        db.active_index_handles_loaded(scope)
            .iter()
            .filter(|handle| handle.identity() == &definition.identity())
            .count(),
        1,
        "activated identity must expose exactly one generation"
    );
}

/// Builds and drops every supported family definition through explicit steps.
async fn run_family_shapes() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-family-shapes".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("family-shape writer opens");
    let controller = LifecycleTestController::new();
    for definition in family_shapes() {
        let identity = definition.identity();
        let receipt = controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("fresh family shape is accepted");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("fresh family shape must enqueue one build");
        };
        assert!(matches!(
            drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
            IndexOperationStatus::Succeeded { .. }
        ));
        let active = crate::index_lifecycle::repository::load_index_record(
            db.lifecycle_test_writer_db()
                .expect("shape writer storage is available"),
            DataScope::LegacyUnscoped,
            &identity,
        )
        .await
        .expect("shape canonical row decodes")
        .expect("shape canonical row exists");
        assert!(matches!(active.state(), IndexStateV2::Active { .. }));

        let drop = controller
            .drop_index(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("active family shape accepts DROP");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_operation_id,
            ..
        } = drop
        else {
            panic!("first active DROP must enqueue cleanup");
        };
        assert!(matches!(
            drive_to_terminal(
                &db,
                &controller,
                DataScope::LegacyUnscoped,
                drop_operation_id,
            )
            .await,
            IndexOperationStatus::Succeeded { .. }
        ));
        let dropped = crate::index_lifecycle::repository::load_index_record(
            db.lifecycle_test_writer_db()
                .expect("shape writer storage is available"),
            DataScope::LegacyUnscoped,
            &identity,
        )
        .await
        .expect("dropped shape canonical row decodes")
        .expect("dropped shape canonical row remains retained");
        assert!(matches!(dropped.state(), IndexStateV2::Dropped { .. }));
    }
    db.close().await.expect("family-shape writer closes");
}

/// Proves CREATE/DROP convergence for Building, Active, Aborting, and Dropping.
async fn run_create_drop_state_matrix() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-create-drop-matrix".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("CREATE/DROP matrix writer opens");
    let controller = LifecycleTestController::new();
    for (ordinal, definition) in representative_families().into_iter().enumerate() {
        let first = controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::IfNotExists,
            )
            .await
            .expect("absent definition accepts one build");
        let IndexDdlReceipt::Accepted {
            operation_id,
            generation,
            ..
        } = first
        else {
            panic!("absent definition must be accepted");
        };
        assert_eq!(
            controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await
                .expect("IF NOT EXISTS converges on Building"),
            IndexDdlReceipt::ExistingOperation { operation_id }
        );
        assert!(matches!(
            controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::ErrorIfExists,
                )
                .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));

        let abort = controller
            .drop_index(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("DROP converts Building into abort cleanup");
        assert_eq!(abort, IndexDdlReceipt::ExistingOperation { operation_id });
        assert_eq!(
            controller
                .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                .await
                .expect("repeated building DROP converges"),
            abort
        );
        assert!(matches!(
            controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await,
            Err(HelixDbError::IndexBusy { state: "aborting" })
        ));
        assert!(matches!(
            drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
            IndexOperationStatus::Aborted { .. }
        ));

        let recreated = controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("Dropped definition accepts recreation");
        let IndexDdlReceipt::Accepted {
            operation_id: recreated_operation_id,
            generation: recreated_generation,
            ..
        } = recreated
        else {
            panic!("recreation must enqueue a new build");
        };
        assert_eq!(recreated_generation.get(), generation.get() + 1);
        assert!(matches!(
            drive_to_terminal(
                &db,
                &controller,
                DataScope::LegacyUnscoped,
                recreated_operation_id,
            )
            .await,
            IndexOperationStatus::Succeeded { .. }
        ));
        assert!(matches!(
            controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await
                .expect("IF NOT EXISTS converges on Active"),
            IndexDdlReceipt::AlreadyActive { .. }
        ));
        assert!(matches!(
            controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::ErrorIfExists,
                )
                .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));

        let active_drop = controller
            .drop_index(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("Active definition accepts one DROP");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_operation_id,
            ..
        } = active_drop
        else {
            panic!("Active DROP must enqueue one cleanup operation");
        };
        assert_eq!(
            controller
                .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                .await
                .expect("repeated active DROP converges"),
            IndexDdlReceipt::ExistingOperation {
                operation_id: drop_operation_id,
            }
        );
        assert!(matches!(
            controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    helix_planner::ir::IndexCreateMode::IfNotExists,
                )
                .await,
            Err(HelixDbError::IndexBusy { state: "dropping" })
        ));
        assert!(matches!(
            drive_to_terminal(
                &db,
                &controller,
                DataScope::LegacyUnscoped,
                drop_operation_id,
            )
            .await,
            IndexOperationStatus::Succeeded { .. }
        ));
        assert!(
            controller
                .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                .await
                .is_err(),
            "terminal DROP case {ordinal} must report logical absence"
        );
    }
    db.close().await.expect("CREATE/DROP matrix writer closes");
}

/// Exact pre-activation checkpoint at which DROP is issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildingDropCheckpoint {
    /// No worker has claimed the accepted operation.
    Queued,
    /// The secondary scan has committed this many bounded batches.
    SecondaryScanBatches(NonZeroUsize),
    /// The operation has reached this typed stage without executing it.
    Stage(IndexOperationStage),
}

/// Enumerates every applicable build checkpoint for one family.
fn building_drop_checkpoints(family: IndexDefinitionFamily) -> Vec<BuildingDropCheckpoint> {
    let mut checkpoints = vec![BuildingDropCheckpoint::Queued];
    match family {
        IndexDefinitionFamily::Secondary => {
            checkpoints.extend([
                BuildingDropCheckpoint::SecondaryScanBatches(
                    NonZeroUsize::new(1).expect("one scan batch is positive"),
                ),
                BuildingDropCheckpoint::SecondaryScanBatches(
                    NonZeroUsize::new(2).expect("two scan batches are positive"),
                ),
                BuildingDropCheckpoint::Stage(IndexOperationStage::CatchUp),
                BuildingDropCheckpoint::Stage(IndexOperationStage::Validate),
                BuildingDropCheckpoint::Stage(IndexOperationStage::Activate),
            ]);
        }
        IndexDefinitionFamily::Vector => checkpoints.extend([
            BuildingDropCheckpoint::Stage(IndexOperationStage::CatchUp),
            BuildingDropCheckpoint::Stage(IndexOperationStage::ValidateDescriptor),
            BuildingDropCheckpoint::Stage(IndexOperationStage::Activate),
        ]),
        IndexDefinitionFamily::Text => checkpoints.extend([
            BuildingDropCheckpoint::Stage(IndexOperationStage::ScanPartitions),
            BuildingDropCheckpoint::Stage(IndexOperationStage::CatchUp),
            BuildingDropCheckpoint::Stage(IndexOperationStage::Compact),
            BuildingDropCheckpoint::Stage(IndexOperationStage::PrepareManifests),
            BuildingDropCheckpoint::Stage(IndexOperationStage::ValidateManifests),
            BuildingDropCheckpoint::Stage(IndexOperationStage::Activate),
        ]),
    }
    checkpoints
}

/// Drops every family before activation at each applicable typed checkpoint.
async fn run_building_drop_stage_matrix() {
    for (family_ordinal, definition) in representative_families().into_iter().enumerate() {
        for (checkpoint_ordinal, checkpoint) in building_drop_checkpoints(definition.family())
            .into_iter()
            .enumerate()
        {
            let config = DbConfig::new().with_secondary_index_lifecycle_tuning(
                SecondaryIndexLifecycleTuning::default().with_batch_rows(
                    SecondaryIndexLifecycleBatchRows::new(2)
                        .expect("drop matrix scan batch is positive"),
                ),
            );
            let db = HelixDB::open_for_index_lifecycle_testing(
                HelixDbSource::InMemory {
                    database: format!(
                        "index-lifecycle-lifecycle-building-drop-{family_ordinal}-{checkpoint_ordinal}"
                    ),
                },
                config,
                LifecycleTestScheduling::Explicit,
            )
            .await
            .expect("building DROP stage writer opens");
            let controller = LifecycleTestController::new();
            match checkpoint {
                BuildingDropCheckpoint::SecondaryScanBatches(_) => {
                    let ids = allocate_node_ids(&db, 8).await;
                    for entity_id in ids {
                        put_source(
                            &db,
                            DataScope::LegacyUnscoped,
                            entity_id,
                            &[
                                Property::string("$label", "MatrixSecondary"),
                                Property::string("value", format!("drop-{entity_id}")),
                            ],
                        )
                        .await;
                    }
                }
                BuildingDropCheckpoint::Queued | BuildingDropCheckpoint::Stage(_) => {}
            }
            if definition.family() == IndexDefinitionFamily::Text {
                controller
                    .seed_node_property_rows(
                        &db,
                        DataScope::LegacyUnscoped,
                        NonZeroU64::new(2).expect("text source count is positive"),
                        NonZeroUsize::MIN,
                        |entity_id| {
                            vec![
                                Property::string("$label", "MatrixText"),
                                Property::string(
                                    "value",
                                    format!("building drop source {entity_id}"),
                                ),
                            ]
                        },
                    )
                    .await
                    .expect("text DROP fixture source rows seed");
            }
            let IndexDdlReceipt::Accepted { operation_id, .. } = controller
                .create_index(
                    &db,
                    DataScope::LegacyUnscoped,
                    definition.clone(),
                    ir::IndexCreateMode::ErrorIfExists,
                )
                .await
                .expect("fresh staged DROP build is accepted")
            else {
                panic!("fresh staged DROP build must enqueue");
            };
            match checkpoint {
                BuildingDropCheckpoint::Queued => {}
                BuildingDropCheckpoint::SecondaryScanBatches(batch_count) => {
                    for _ in 0..batch_count.get() {
                        let evidence = controller
                            .advance(
                                &db,
                                LifecycleWorkTarget::Operation {
                                    scope: DataScope::LegacyUnscoped,
                                    operation_id,
                                },
                            )
                            .await
                            .expect("bounded staged scan commits");
                        assert_monotonic_step(&evidence);
                        assert!(evidence.resources.entities > 0);
                    }
                    assert_eq!(
                        db.get_index_operation(DataScope::LegacyUnscoped, operation_id)
                            .await
                            .expect("mid-scan operation is readable")
                            .common()
                            .stage,
                        IndexOperationStage::Scan,
                        "bounded scan fixture must remain before its watermark"
                    );
                }
                BuildingDropCheckpoint::Stage(stage) => {
                    drive_until_stage(
                        &db,
                        &controller,
                        DataScope::LegacyUnscoped,
                        operation_id,
                        stage,
                    )
                    .await;
                }
            }

            assert_identity_hidden(&db, DataScope::LegacyUnscoped, &definition).await;
            let receipt = controller
                .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                .await
                .expect("DROP converts the exact building operation");
            assert_eq!(receipt, IndexDdlReceipt::ExistingOperation { operation_id });
            assert_eq!(
                controller
                    .drop_index(&db, DataScope::LegacyUnscoped, &definition)
                    .await
                    .expect("repeated staged DROP converges"),
                receipt
            );
            assert!(matches!(
                controller
                    .create_index(
                        &db,
                        DataScope::LegacyUnscoped,
                        definition.clone(),
                        ir::IndexCreateMode::IfNotExists,
                    )
                    .await,
                Err(HelixDbError::IndexBusy { state: "aborting" })
            ));
            assert!(matches!(
                db.get_index_operation(DataScope::LegacyUnscoped, operation_id)
                    .await
                    .expect("aborting operation remains readable")
                    .common()
                    .stage,
                IndexOperationStage::AbortingDeleteEntries
                    | IndexOperationStage::AbortingRetireCache
                    | IndexOperationStage::AbortingDeleteMetadata
            ));
            assert!(matches!(
                drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
                IndexOperationStatus::Aborted { .. }
            ));
            let record = crate::index_lifecycle::repository::load_index_record(
                db.lifecycle_test_writer_db()
                    .expect("staged DROP writer storage is available"),
                DataScope::LegacyUnscoped,
                &definition.identity(),
            )
            .await
            .expect("staged DROP canonical row decodes")
            .expect("staged DROP canonical row remains retained");
            assert!(matches!(record.state(), IndexStateV2::Dropped { .. }));
            db.close().await.expect("building DROP stage writer closes");
        }
    }
}

/// Proves an old SlateDB snapshot keeps reading its generation after DROP cleanup.
async fn run_active_drop_snapshot_survival() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-active-drop-snapshot-survival".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("active DROP snapshot-survival writer opens");
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("DrainUser", "value")
            .expect("snapshot-survival definition validates")
            .try_into()
            .expect("snapshot-survival definition converts");
    let entity_id = allocate_node_ids(&db, 1).await.start;
    put_source(
        &db,
        DataScope::LegacyUnscoped,
        entity_id,
        &[
            Property::string("$label", "DrainUser"),
            Property::string("value", "snapshot-visible"),
        ],
    )
    .await;
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("snapshot-survival build is accepted")
    else {
        panic!("fresh snapshot-survival build must enqueue");
    };
    assert!(matches!(
        drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id,).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    let active = crate::index_lifecycle::repository::load_index_record(
        db.lifecycle_test_writer_db()
            .expect("snapshot-survival storage is available"),
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("snapshot-survival canonical row decodes")
    .expect("snapshot-survival canonical row exists");
    let active_handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
        .expect("active snapshot fixture projects its physical generation");
    let snapshot = db
        .lifecycle_test_writer_db()
        .expect("snapshot fixture has writer storage")
        .snapshot()
        .await
        .expect("pre-DROP snapshot opens");
    assert_eq!(
        crate::index_lifecycle::secondary::lookup_active_equality_generation(
            snapshot.as_ref(),
            &active_handle,
            &PropertyValue::String("snapshot-visible".to_string()),
        )
        .await
        .expect("pre-DROP snapshot reads its generation")
        .into_iter()
        .collect::<Vec<_>>(),
        vec![entity_id]
    );
    let IndexDdlReceipt::Accepted {
        operation_id: drop_operation_id,
        ..
    } = controller
        .drop_index(&db, DataScope::LegacyUnscoped, &definition)
        .await
        .expect("active snapshot-survival DROP is accepted")
    else {
        panic!("active snapshot-survival DROP must enqueue cleanup");
    };
    assert_identity_hidden(&db, DataScope::LegacyUnscoped, &definition).await;
    assert!(matches!(
        drive_to_terminal(
            &db,
            &controller,
            DataScope::LegacyUnscoped,
            drop_operation_id,
        )
        .await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert_eq!(
        crate::index_lifecycle::secondary::lookup_active_equality_generation(
            snapshot.as_ref(),
            &active_handle,
            &PropertyValue::String("snapshot-visible".to_string()),
        )
        .await
        .expect("old snapshot keeps its physical generation after cleanup")
        .into_iter()
        .collect::<Vec<_>>(),
        vec![entity_id]
    );
    let current = db
        .lifecycle_test_writer_db()
        .expect("snapshot fixture has writer storage")
        .snapshot()
        .await
        .expect("post-DROP snapshot opens");
    assert!(
        crate::index_lifecycle::secondary::lookup_active_equality_generation(
            current.as_ref(),
            &active_handle,
            &PropertyValue::String("snapshot-visible".to_string()),
        )
        .await
        .expect("fresh snapshot observes cleanup")
        .is_empty()
    );
    db.close()
        .await
        .expect("active DROP snapshot-survival writer closes");
}

/// Proves vector physical rows remain readable through a pre-DROP snapshot.
async fn run_active_vector_drop_snapshot_survival() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-active-vector-drop-snapshot-survival".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("active vector DROP snapshot-survival writer opens");
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("SnapshotVector", "value", 3, VectorDistanceMetric::Cosine)
            .expect("snapshot vector definition validates")
            .try_into()
            .expect("snapshot vector definition converts");
    let entity_id = allocate_node_ids(&db, 1).await.start;
    put_source(
        &db,
        DataScope::LegacyUnscoped,
        entity_id,
        &[
            Property::string("$label", "SnapshotVector"),
            Property::f32_array("value", vec![1.0, 0.0, 0.0]),
        ],
    )
    .await;
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            DataScope::LegacyUnscoped,
            definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("snapshot vector build is accepted")
    else {
        panic!("fresh snapshot vector build must enqueue");
    };
    assert!(matches!(
        drive_to_terminal(&db, &controller, DataScope::LegacyUnscoped, operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    let active = crate::index_lifecycle::repository::load_index_record(
        db.lifecycle_test_writer_db()
            .expect("snapshot vector storage is available"),
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("snapshot vector canonical row decodes")
    .expect("snapshot vector canonical row exists");
    let active_handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
        .expect("active snapshot vector projects its physical generation");
    let ActiveIndexHandle::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    } = &active_handle
    else {
        panic!("snapshot vector fixture must be unpartitioned");
    };
    let generation = ValidatedVectorGenerationHandle::try_from_active::<Cosine>(
        &active_handle,
        *physical_index_id,
    )
    .expect("snapshot vector generation validates");
    let vector_index = VectorIndex::<Cosine>::from_generation(&generation);
    let snapshot = db
        .lifecycle_test_writer_db()
        .expect("snapshot vector fixture has writer storage")
        .snapshot()
        .await
        .expect("pre-DROP vector snapshot opens");
    assert!(vector_index
        .get_item(snapshot.as_ref(), entity_id)
        .await
        .expect("pre-DROP vector snapshot reads its generation")
        .is_some());
    let IndexDdlReceipt::Accepted {
        operation_id: drop_operation_id,
        ..
    } = controller
        .drop_index(&db, DataScope::LegacyUnscoped, &definition)
        .await
        .expect("active snapshot vector DROP is accepted")
    else {
        panic!("active snapshot vector DROP must enqueue cleanup");
    };
    assert_identity_hidden(&db, DataScope::LegacyUnscoped, &definition).await;
    assert!(matches!(
        drive_to_terminal(
            &db,
            &controller,
            DataScope::LegacyUnscoped,
            drop_operation_id,
        )
        .await,
        IndexOperationStatus::Succeeded { .. }
    ));
    assert!(vector_index
        .get_item(snapshot.as_ref(), entity_id)
        .await
        .expect("old vector snapshot keeps its physical generation after cleanup")
        .is_some());
    let current = db
        .lifecycle_test_writer_db()
        .expect("snapshot vector fixture has writer storage")
        .snapshot()
        .await
        .expect("post-DROP vector snapshot opens");
    assert!(vector_index
        .get_item(current.as_ref(), entity_id)
        .await
        .expect("fresh vector snapshot observes cleanup")
        .is_none());
    db.close()
        .await
        .expect("active vector DROP snapshot-survival writer closes");
}

/// Proves the same logical identity remains isolated across tenant scopes.
async fn run_scope_isolation() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-lifecycle-scope-isolation".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("scope-isolation writer opens");
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("ScopedUser", "email")
            .expect("scope definition validates")
            .try_into()
            .expect("scope definition converts to V2");
    let scopes = [
        DataScope::Tenant(TenantId::from_u128(1)),
        DataScope::Tenant(TenantId::from_u128(2)),
    ];
    let mut operations = Vec::new();
    for scope in scopes {
        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .create_index(
                &db,
                scope,
                definition.clone(),
                helix_planner::ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("same identity is accepted in each tenant scope")
        else {
            panic!("fresh tenant-scoped definition must enqueue");
        };
        operations.push((scope, operation_id));
    }
    assert_ne!(operations[0].1, operations[1].1);
    for (scope, operation_id) in operations {
        assert!(matches!(
            drive_to_terminal(&db, &controller, scope, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ));
    }
    db.close().await.expect("scope-isolation writer closes");
}

/// Advances all discovered lanes until one exact operation becomes terminal.
async fn drive_to_terminal(
    db: &HelixDB,
    controller: &LifecycleTestController,
    scope: DataScope,
    operation_id: crate::index_lifecycle::IndexOperationId,
) -> IndexOperationStatus {
    let logical_start = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("lifecycle contract clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("lifecycle contract time fits u64 milliseconds");
    let operation_target = LifecycleWorkTarget::Operation {
        scope,
        operation_id,
    };
    for turn in 0..MAXIMUM_CONTROLLER_TURNS {
        let status = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("driven operation remains readable");
        if matches!(status, IndexOperationStatus::Blocked { .. }) {
            return status;
        }
        let terminal = matches!(
            status,
            IndexOperationStatus::Succeeded { .. } | IndexOperationStatus::Aborted { .. }
        );
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("controller turn fits u64")
                .saturating_mul(60_000),
        );
        if !terminal {
            let evidence = controller
                .advance_at_unix_millis(db, operation_target, logical_now)
                .await
                .expect("explicit production operation step succeeds");
            assert_monotonic_step(&evidence);
        }
        let page = controller
            .discover(
                db,
                NonZeroUsize::new(1_024).expect("discovery bound is positive"),
            )
            .await
            .expect("explicit lifecycle work remains discoverable");
        assert!(page.exhausted, "small contract must fit one bounded page");
        let status_after = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("driven operation remains readable after its step");
        if matches!(status_after, IndexOperationStatus::Blocked { .. }) {
            return status_after;
        }
        if matches!(
            status_after,
            IndexOperationStatus::Succeeded { .. } | IndexOperationStatus::Aborted { .. }
        ) && page.targets.is_empty()
        {
            return status_after;
        }
        assert!(
            !page.targets.is_empty(),
            "nonterminal operation must retain discoverable work"
        );
        for target in page.targets {
            if target == operation_target {
                continue;
            }
            let evidence = controller
                .advance_at_unix_millis(db, target, logical_now)
                .await
                .expect("explicit production child step succeeds");
            assert_monotonic_step(&evidence);
        }
    }
    panic!("operation exceeded its deterministic controller-turn bound");
}

/// Advances all work until one operation reaches an exact nonterminal stage.
async fn drive_until_stage(
    db: &HelixDB,
    controller: &LifecycleTestController,
    scope: DataScope,
    operation_id: crate::index_lifecycle::IndexOperationId,
    expected_stage: IndexOperationStage,
) {
    let logical_start = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("lifecycle contract clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("lifecycle contract time fits u64 milliseconds");
    let operation_target = LifecycleWorkTarget::Operation {
        scope,
        operation_id,
    };
    for turn in 0..MAXIMUM_CONTROLLER_TURNS {
        let status = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("staged operation remains readable");
        if status.common().stage == expected_stage {
            return;
        }
        assert!(
            matches!(
                status,
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
            ),
            "operation terminated before reaching {expected_stage:?}: {status:?}"
        );
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("controller turn fits u64")
                .saturating_mul(60_000),
        );
        let evidence = controller
            .advance_at_unix_millis(db, operation_target, logical_now)
            .await
            .expect("staged production operation step succeeds");
        assert_monotonic_step(&evidence);
        let status_after = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("staged operation remains readable after its step");
        if status_after.common().stage == expected_stage {
            return;
        }
        let page = controller
            .discover(
                db,
                NonZeroUsize::new(1_024).expect("discovery bound is positive"),
            )
            .await
            .expect("staged lifecycle work remains discoverable");
        assert!(page.exhausted, "small staged contract fits one page");
        for target in page.targets {
            if target == operation_target {
                continue;
            }
            let evidence = controller
                .advance_at_unix_millis(db, target, logical_now)
                .await
                .expect("staged production child step succeeds");
            assert_monotonic_step(&evidence);
        }
    }
    panic!("operation did not reach {expected_stage:?} within its turn bound");
}

/// Checks revision monotonicity and successful non-waiting checkpoint movement.
fn assert_monotonic_step(evidence: &LifecycleStepEvidence) {
    if let (
        LifecycleCheckpoint::Present {
            durable_revision: before,
            ..
        },
        LifecycleCheckpoint::Present {
            durable_revision: after,
            ..
        },
    ) = (evidence.before, evidence.after)
    {
        assert!(after >= before, "durable revision must not regress");
    }
    if matches!(
        evidence.outcome,
        LifecycleStepOutcome::Progressed
            | LifecycleStepOutcome::Completed
            | LifecycleStepOutcome::StalePointerRemoved
    ) {
        assert_ne!(
            evidence.before, evidence.after,
            "successful non-waiting work must move a checkpoint or terminate"
        );
    }
    assert_ne!(
        evidence.elapsed_micros,
        u64::MAX,
        "test-step elapsed time must fit report representation"
    );
}

/// Returns one identity-distinct representative from every family.
fn representative_families() -> Vec<ValidatedDynamicIndexDefinition> {
    vec![
        SecondaryIndexDefinition::node_equality("MatrixSecondary", "value")
            .expect("secondary matrix definition validates")
            .try_into()
            .expect("secondary matrix definition converts"),
        VectorIndexDefinition::new_node("MatrixVector", "value", 3, VectorDistanceMetric::Cosine)
            .expect("vector matrix definition validates")
            .try_into()
            .expect("vector matrix definition converts"),
        TextIndexDefinition::new_node("MatrixText", "value")
            .expect("text matrix definition validates")
            .try_into()
            .expect("text matrix definition converts"),
    ]
}

/// Enumerates every currently supported small family shape.
fn family_shapes() -> Vec<ValidatedDynamicIndexDefinition> {
    let secondary = [
        SecondaryIndexDefinition::node_equality("ShapeNodeEq", "value")
            .expect("node equality validates"),
        SecondaryIndexDefinition::node_unique_equality("ShapeNodeUnique", "value")
            .expect("node unique equality validates"),
        SecondaryIndexDefinition::node_range("ShapeNodeRangeAsc", "value")
            .expect("ascending node range validates"),
        SecondaryIndexDefinition::node_range_desc("ShapeNodeRangeDesc", "value")
            .expect("descending node range validates"),
        SecondaryIndexDefinition::edge_equality("ShapeEdgeEq", "value")
            .expect("edge equality validates"),
        SecondaryIndexDefinition::edge_range("ShapeEdgeRangeAsc", "value")
            .expect("ascending edge range validates"),
        SecondaryIndexDefinition::edge_range_desc("ShapeEdgeRangeDesc", "value")
            .expect("descending edge range validates"),
    ];
    let mut definitions = secondary
        .into_iter()
        .map(|definition| {
            definition
                .try_into()
                .expect("secondary shape converts to V2")
        })
        .collect::<Vec<_>>();

    for (metric_ordinal, metric) in [
        VectorDistanceMetric::Cosine,
        VectorDistanceMetric::Euclidean,
        VectorDistanceMetric::Manhattan,
    ]
    .into_iter()
    .enumerate()
    {
        for edge in [false, true] {
            for partitioned in [false, true] {
                let label = format!(
                    "ShapeVector{metric_ordinal}{}{}",
                    if edge { "Edge" } else { "Node" },
                    if partitioned { "Partitioned" } else { "Global" }
                );
                let definition = if edge {
                    VectorIndexDefinition::new_edge(&label, "value", 3, metric)
                } else {
                    VectorIndexDefinition::new_node(&label, "value", 3, metric)
                }
                .expect("vector shape validates");
                let definition = if partitioned {
                    definition
                        .with_tenant_property("tenant")
                        .expect("vector tenant property validates")
                } else {
                    definition
                };
                definitions.push(definition.try_into().expect("vector shape converts to V2"));
            }
        }
    }

    for (analyzer_ordinal, analyzer) in [
        TextAnalyzerKind::Standard,
        TextAnalyzerKind::StandardStemEn,
        TextAnalyzerKind::WhitespaceLowercase,
    ]
    .into_iter()
    .enumerate()
    {
        for edge in [false, true] {
            for partitioned in [false, true] {
                for positions in [false, true] {
                    let label = format!(
                        "ShapeText{analyzer_ordinal}{}{}{}",
                        if edge { "Edge" } else { "Node" },
                        if partitioned { "Partitioned" } else { "Global" },
                        if positions {
                            "Positions"
                        } else {
                            "NoPositions"
                        }
                    );
                    let definition = if edge {
                        TextIndexDefinition::new_edge(&label, "value")
                    } else {
                        TextIndexDefinition::new_node(&label, "value")
                    }
                    .expect("text shape validates")
                    .with_analyzer(analyzer)
                    .with_positions_enabled(positions);
                    let definition = if partitioned {
                        definition
                            .with_tenant_property("tenant")
                            .expect("text tenant property validates")
                    } else {
                        definition
                    };
                    definitions.push(definition.try_into().expect("text shape converts to V2"));
                }
            }
        }
    }
    definitions
}

/// Constructs one planner identifier for deterministic lifecycle fixtures.
fn public_name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("public lifecycle fixture identifiers are non-empty")
}

/// Constructs one executable step with neutral scheduling metadata.
fn public_step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("public lifecycle step IDs are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

/// Validates one fixture DAG through the production executable-plan boundary.
fn public_executable(
    kind: ir::PlanKind,
    steps: Vec<exec::ExecStep>,
    root: usize,
) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps)
            .expect("public lifecycle fixture plan is non-empty"),
        exec::ExecStepId::new(root).expect("public lifecycle root ID is positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("public lifecycle fixture dependencies form an executable plan")
}

/// Builds one public node insert for the selected physical family.
fn public_add_node_plan(family: PublicMutationFamily, ordinal: u64) -> exec::ExecutablePlan {
    public_executable(
        ir::PlanKind::Write,
        vec![public_step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: public_name(PUBLIC_MUTATION_LABEL),
                    properties: ir::PropertyAssignments::try_from_vec(vec![(
                        public_name(PUBLIC_MUTATION_PROPERTY),
                        ir::PropertyInputPlan::Value(family.inserted_value(ordinal)),
                    )])
                    .expect("public lifecycle insert property is unique"),
                },
            },
        )],
        1,
    )
}

fn public_add_edge_plan(
    family: PublicMutationFamily,
    ordinal: u64,
    to: u64,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("public edge access ID is positive");
    public_executable(
        ir::PlanKind::Write,
        vec![
            public_step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: public_name("from"),
                        },
                    )),
                },
            ),
            public_step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: public_name(PUBLIC_MUTATION_LABEL),
                        to: ir::NodeTargetPlan::PointIds {
                            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(to))
                                .expect("public edge target is non-empty"),
                        },
                        properties: ir::PropertyAssignments::try_from_vec(vec![
                            (
                                public_name(PUBLIC_MUTATION_PROPERTY),
                                ir::PropertyInputPlan::Value(family.inserted_value(ordinal)),
                            ),
                            (
                                public_name(PUBLIC_MUTATION_TENANT),
                                ir::PropertyInputPlan::Value(AstPropertyValue::String(
                                    "tenant-initial".to_string(),
                                )),
                            ),
                        ])
                        .expect("public edge properties are unique"),
                    },
                },
            ),
        ],
        2,
    )
}

fn public_set_edge_tenant_plan(tenant: &str) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("public edge access ID is positive");
    public_executable(
        ir::PlanKind::Write,
        vec![
            public_step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::FromParam {
                            param: public_name("edge"),
                        },
                    )),
                },
            ),
            public_step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::SetProperty {
                        name: public_name(PUBLIC_MUTATION_TENANT),
                        value: ir::PropertyInputPlan::Value(AstPropertyValue::String(
                            tenant.to_string(),
                        )),
                    },
                },
            ),
        ],
        2,
    )
}

fn public_drop_edge_plan(edge_id: u64) -> exec::ExecutablePlan {
    public_executable(
        ir::PlanKind::Write,
        vec![public_step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::DropEdgeByIdSource {
                    edges: ir::EdgeTargetPlan::PointIds {
                        ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(edge_id))
                            .expect("public edge delete target is non-empty"),
                    },
                },
            },
        )],
        1,
    )
}

fn edge_binding(edge_id: u64) -> context::ParamBindings {
    context::ParamBindings::default().with_value(
        public_name("edge"),
        AstPropertyValue::I64(i64::try_from(edge_id).expect("edge ID fits i64")),
    )
}

async fn assert_edge_indexed_read<const N: usize>(
    db: &HelixDB,
    family: PublicMutationFamily,
    ordinal: u64,
    tenant: &str,
    expected: [u64; N],
) {
    let result = db
        .execute(
            &public_edge_indexed_read_plan(family, ordinal, tenant),
            context::ParamBindings::default(),
        )
        .await
        .expect("partitioned edge indexed read succeeds");
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("partitioned edge indexed read returns projected scalars");
    };
    let actual = values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::EdgeId(edge_id) = value else {
                panic!("partitioned edge indexed read returns only edge IDs");
            };
            edge_id
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected.into_iter().collect());
}

fn public_edge_indexed_read_plan(
    family: PublicMutationFamily,
    ordinal: u64,
    tenant: &str,
) -> exec::ExecutablePlan {
    let index = ir::SearchIndexPlan {
        index_id: public_name(&match family {
            PublicMutationFamily::Vector => vector_index_name(
                VectorElementType::Edge,
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
            ),
            PublicMutationFamily::Text => text_index_name(
                TextElementType::Edge,
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
            ),
            PublicMutationFamily::Secondary => {
                unreachable!("secondary has a dedicated edge indexed-read matrix")
            }
        }),
        tenant: ir::SearchTenantPlan::ScopedValue {
            property: public_name(PUBLIC_MUTATION_TENANT),
            value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(
                AstPropertyValue::String(tenant.to_string()),
            ))
            .expect("public edge tenant is non-null"),
        },
    };
    let access = match family {
        PublicMutationFamily::Vector => {
            let AstPropertyValue::F32Array(query) = family.inserted_value(ordinal) else {
                unreachable!("vector public mutation fixture returns a vector")
            };
            exec::ExecEdgeAccessPlan::VectorSearch {
                key: catalog::EdgeSearchIndexKey::try_new(
                    PUBLIC_MUTATION_LABEL,
                    PUBLIC_MUTATION_PROPERTY,
                )
                .expect("public edge vector key validates"),
                index,
                query_vector: ir::VectorQueryInputPlan::Vector(
                    ir::SearchVector::new(query)
                        .expect("public edge vector query is finite and non-empty"),
                ),
                k: ir::SearchLimitPlan::Literal(NonZeroUsize::MIN),
            }
        }
        PublicMutationFamily::Text => exec::ExecEdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new(
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
            )
            .expect("public edge text key validates"),
            index,
            query_text: ir::TextQueryInputPlan::Text(public_name(&format!(
                "uniquelifecycleinserted{ordinal}"
            ))),
            k: ir::SearchLimitPlan::Literal(
                NonZeroUsize::new(10).expect("public edge text limit is positive"),
            ),
        },
        PublicMutationFamily::Secondary => {
            unreachable!("secondary has a dedicated edge indexed-read matrix")
        }
    };
    let access_id = exec::ExecStepId::new(1).expect("public edge access ID is positive");
    public_executable(
        ir::PlanKind::Read,
        vec![
            public_step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(access)),
                },
            ),
            public_step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

/// Builds one public indexed read for an inserted family-specific value.
fn public_indexed_read_plan(family: PublicMutationFamily, ordinal: u64) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("public lifecycle access ID is positive");
    let access = match family {
        PublicMutationFamily::Secondary => exec::ExecNodeAccessPlan::exact_equality(
            catalog::NodeEqualityIndexMeta::new(public_name(&format!(
                "node_eq:{PUBLIC_MUTATION_LABEL}:{PUBLIC_MUTATION_PROPERTY}"
            ))),
            catalog::ScopedPropertyKey::try_new(PUBLIC_MUTATION_LABEL, PUBLIC_MUTATION_PROPERTY)
                .expect("public lifecycle equality key validates"),
            ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(AstPropertyValue::String(format!(
                    "inserted-{ordinal}"
                )))
                .expect("public lifecycle equality literal is indexable"),
            ),
        ),
        PublicMutationFamily::Vector => {
            let AstPropertyValue::F32Array(query) = family.inserted_value(ordinal) else {
                unreachable!("vector public mutation fixture returns a vector")
            };
            exec::ExecNodeAccessPlan::VectorSearch {
                key: catalog::NodeSearchIndexKey::try_new(
                    PUBLIC_MUTATION_LABEL,
                    PUBLIC_MUTATION_PROPERTY,
                )
                .expect("public lifecycle vector key validates"),
                index: ir::SearchIndexPlan {
                    index_id: public_name(&vector_index_name(
                        VectorElementType::Node,
                        PUBLIC_MUTATION_LABEL,
                        PUBLIC_MUTATION_PROPERTY,
                    )),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_vector: ir::VectorQueryInputPlan::Vector(
                    ir::SearchVector::new(query)
                        .expect("public lifecycle vector query is finite and non-empty"),
                ),
                k: ir::SearchLimitPlan::Literal(NonZeroUsize::MIN),
            }
        }
        PublicMutationFamily::Text => exec::ExecNodeAccessPlan::TextSearch {
            key: catalog::NodeSearchIndexKey::try_new(
                PUBLIC_MUTATION_LABEL,
                PUBLIC_MUTATION_PROPERTY,
            )
            .expect("public lifecycle text key validates"),
            index: ir::SearchIndexPlan {
                index_id: public_name(&text_index_name(
                    TextElementType::Node,
                    PUBLIC_MUTATION_LABEL,
                    PUBLIC_MUTATION_PROPERTY,
                )),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_text: ir::TextQueryInputPlan::Text(public_name(&format!(
                "uniquelifecycleinserted{ordinal}"
            ))),
            k: ir::SearchLimitPlan::Literal(
                NonZeroUsize::new(10).expect("public lifecycle text limit is positive"),
            ),
        },
    };
    public_executable(
        ir::PlanKind::Read,
        vec![
            public_step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(access)),
                },
            ),
            public_step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}
