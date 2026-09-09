//! Late-mutation convergence across every supported managed index shape.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::{catalog, context, exec, ir};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::{TextElementType, VectorElementType};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, encode_properties, Property};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{
    DataKey, DataKeyKind, EdgeEndpointsKey, IndexEntity, IndexEntityStateKey, ManagedIndexKey,
    RecordKind, ScopedKey,
};
use crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue;
use crate::execution::interpreter::{ExecutionScalar, ExecutionValue};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexDefinitionFamily, IndexElementKind, IndexOperationId,
    IndexOperationStage, IndexOperationStatus, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition,
};
use crate::index_lifecycle_testing::{
    LifecycleCheckpoint, LifecycleStage, LifecycleTestController, LifecycleWorkTarget,
    TextManifestValidationLane,
};
use crate::search::{text_index_name, vector_index_name};
use crate::HelixDB;

use super::{
    allocate_edge_ids, allocate_node_ids, assert_build_delta_count_at_least,
    assert_build_deltas_empty, assert_identity_active, assert_monotonic_step, drive_to_terminal,
    edge_source_key, family_shapes, mutate_edge_source, mutate_source, public_executable,
    public_name, public_step, put_edge_source, put_source, source_key, MAXIMUM_CONTROLLER_TURNS,
};

const INDEX_PROPERTY: &str = "value";
const TENANT_PROPERTY: &str = "tenant";
const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
const TENANT_C: &str = "tenant-c";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecondaryLatePoint {
    Scan,
    CatchUp,
    Validate,
    Activate,
}

impl SecondaryLatePoint {
    const ALL: [Self; 4] = [Self::Scan, Self::CatchUp, Self::Validate, Self::Activate];

    const fn stage(self) -> IndexOperationStage {
        match self {
            Self::Scan => IndexOperationStage::Scan,
            Self::CatchUp => IndexOperationStage::CatchUp,
            Self::Validate => IndexOperationStage::Validate,
            Self::Activate => IndexOperationStage::Activate,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorLatePoint {
    Scan,
    CatchUp,
    ValidateDescriptor,
    Activate,
}

impl VectorLatePoint {
    const ALL: [Self; 4] = [
        Self::Scan,
        Self::CatchUp,
        Self::ValidateDescriptor,
        Self::Activate,
    ];

    const fn stage(self) -> IndexOperationStage {
        match self {
            Self::Scan => IndexOperationStage::Scan,
            Self::CatchUp => IndexOperationStage::CatchUp,
            Self::ValidateDescriptor => IndexOperationStage::ValidateDescriptor,
            Self::Activate => IndexOperationStage::Activate,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextLatePoint {
    Compact,
    PrepareManifests,
    ValidatePages,
    ValidateRoots,
    ValidateEntityStates,
    Activate,
}

impl TextLatePoint {
    const ALL: [Self; 6] = [
        Self::Compact,
        Self::PrepareManifests,
        Self::ValidatePages,
        Self::ValidateRoots,
        Self::ValidateEntityStates,
        Self::Activate,
    ];

    const fn stage(self) -> IndexOperationStage {
        match self {
            Self::Compact => IndexOperationStage::Compact,
            Self::PrepareManifests => IndexOperationStage::PrepareManifests,
            Self::ValidatePages | Self::ValidateRoots | Self::ValidateEntityStates => {
                IndexOperationStage::ValidateManifests
            }
            Self::Activate => IndexOperationStage::Activate,
        }
    }

    const fn lane(self) -> Option<TextManifestValidationLane> {
        match self {
            Self::ValidatePages => Some(TextManifestValidationLane::Pages),
            Self::ValidateRoots => Some(TextManifestValidationLane::Roots),
            Self::ValidateEntityStates => Some(TextManifestValidationLane::EntityStates),
            Self::Compact | Self::PrepareManifests | Self::Activate => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureValue {
    Initial(u8),
    Intermediate,
    Updated,
    Inserted,
}

/// Runs 220 exact tenant-scope races: 28 secondary, 48 vector, and 144 text.
pub(super) async fn run() {
    let mut ordinal = 0usize;
    let mut secondary_cases = 0usize;
    let mut vector_cases = 0usize;
    let mut text_cases = 0usize;
    for definition in family_shapes() {
        match definition.family() {
            IndexDefinitionFamily::Secondary => {
                for point in SecondaryLatePoint::ALL {
                    run_case(ordinal, definition.clone(), point.stage(), None).await;
                    ordinal += 1;
                    secondary_cases += 1;
                }
            }
            IndexDefinitionFamily::Vector => {
                for point in VectorLatePoint::ALL {
                    run_case(ordinal, definition.clone(), point.stage(), None).await;
                    ordinal += 1;
                    vector_cases += 1;
                }
            }
            IndexDefinitionFamily::Text => {
                for point in TextLatePoint::ALL {
                    run_case(ordinal, definition.clone(), point.stage(), point.lane()).await;
                    ordinal += 1;
                    text_cases += 1;
                }
            }
        }
    }
    assert_eq!(
        secondary_cases, 28,
        "every secondary shape and lifecycle stage runs"
    );
    assert_eq!(
        vector_cases, 48,
        "every vector shape and lifecycle stage runs"
    );
    assert_eq!(text_cases, 144, "every text shape and late stage runs");
    assert_eq!(ordinal, 220, "the complete all-index race matrix runs");
}

async fn run_case(
    ordinal: usize,
    definition: ValidatedDynamicIndexDefinition,
    stage: IndexOperationStage,
    text_lane: Option<TextManifestValidationLane>,
) {
    let scope = DataScope::Tenant(TenantId::from_u128(
        0xFD00_0000_0000_0000_0000_0000_1000_0000 + ordinal as u128,
    ));
    let database = format!("index-lifecycle-all-index-validation-{ordinal}");
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut db = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        &database,
        Arc::clone(&object_store),
        crate::DbConfig::new(),
        crate::index_lifecycle_testing::LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("all-index validation writer opens");
    let controller = LifecycleTestController::new();
    let element_kind = definition.identity().element_kind();
    let initial_ids = match element_kind {
        IndexElementKind::Node => allocate_node_ids(&db, 3).await,
        IndexElementKind::Edge => allocate_edge_ids(&db, 3).await,
    };
    for (source_ordinal, entity_id) in initial_ids.clone().enumerate() {
        let properties = fixture_properties(
            &definition,
            FixtureValue::Initial(
                u8::try_from(source_ordinal).expect("three source rows fit one fixture ordinal"),
            ),
        );
        match element_kind {
            IndexElementKind::Node => put_source(&db, scope, entity_id, &properties).await,
            IndexElementKind::Edge => {
                put_edge_source(&db, scope, entity_id, &properties).await;
                put_edge_endpoints(&db, scope, entity_id).await;
            }
        }
    }
    let crate::index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            scope,
            definition.clone(),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("all-index validation build is accepted")
    else {
        panic!("fresh all-index validation build must enqueue");
    };

    if matches!(definition, ValidatedDynamicIndexDefinition::Secondary(_))
        && stage != IndexOperationStage::Scan
    {
        drive_until_exact_stage(
            &db,
            &controller,
            scope,
            operation_id,
            IndexOperationStage::CatchUp,
        )
        .await;
        assert_secondary_applied_state_present(
            &db,
            scope,
            operation_id,
            &definition,
            [
                initial_ids.start,
                initial_ids.start + 1,
                initial_ids.start + 2,
            ],
        )
        .await;
    }
    drive_until_exact_stage(&db, &controller, scope, operation_id, stage).await;
    if let Some(lane) = text_lane {
        drive_until_text_validation_lane(&db, &controller, scope, operation_id, lane).await;
    }

    let updated_id = initial_ids.start;
    let deleted_id = initial_ids.start + 1;
    let stable_id = initial_ids.start + 2;
    if matches!(definition, ValidatedDynamicIndexDefinition::Secondary(_))
        && stage == IndexOperationStage::Validate
    {
        assert_secondary_applied_state_present(
            &db,
            scope,
            operation_id,
            &definition,
            [updated_id, deleted_id, stable_id],
        )
        .await;
    }
    let inserted_id = match element_kind {
        IndexElementKind::Node => allocate_node_ids(&db, 1).await.start,
        IndexElementKind::Edge => allocate_edge_ids(&db, 1).await.start,
    };
    if element_kind == IndexElementKind::Edge {
        put_edge_endpoints(&db, scope, inserted_id).await;
    }
    let partitioned_vector = matches!(
        &definition,
        ValidatedDynamicIndexDefinition::Vector(definition)
            if definition.tenant_property().is_some()
    );
    let intermediate_tenant = partitioned_vector.then_some(TENANT_B);
    let final_tenant = partitioned_vector.then_some(TENANT_C);
    mutate_entity(
        &db,
        scope,
        &definition,
        updated_id,
        &fixture_properties(&definition, FixtureValue::Initial(0)),
        &fixture_properties_with_tenant(
            &definition,
            FixtureValue::Intermediate,
            intermediate_tenant,
        ),
    )
    .await;
    mutate_entity(
        &db,
        scope,
        &definition,
        updated_id,
        &fixture_properties_with_tenant(
            &definition,
            FixtureValue::Intermediate,
            intermediate_tenant,
        ),
        &fixture_properties_with_tenant(&definition, FixtureValue::Updated, final_tenant),
    )
    .await;
    mutate_entity(
        &db,
        scope,
        &definition,
        deleted_id,
        &fixture_properties(&definition, FixtureValue::Initial(1)),
        &fixture_properties_with_tenant(
            &definition,
            FixtureValue::Intermediate,
            intermediate_tenant,
        ),
    )
    .await;
    mutate_entity(
        &db,
        scope,
        &definition,
        deleted_id,
        &fixture_properties_with_tenant(
            &definition,
            FixtureValue::Intermediate,
            intermediate_tenant,
        ),
        &[],
    )
    .await;
    mutate_entity(
        &db,
        scope,
        &definition,
        inserted_id,
        &[],
        &fixture_properties_with_tenant(
            &definition,
            FixtureValue::Intermediate,
            intermediate_tenant,
        ),
    )
    .await;
    mutate_entity(
        &db,
        scope,
        &definition,
        inserted_id,
        &fixture_properties_with_tenant(
            &definition,
            FixtureValue::Intermediate,
            intermediate_tenant,
        ),
        &fixture_properties_with_tenant(&definition, FixtureValue::Inserted, final_tenant),
    )
    .await;
    assert_build_delta_count_at_least(&db, scope, &definition, 3).await;

    if !matches!(definition, ValidatedDynamicIndexDefinition::Text(_)) {
        db.close()
            .await
            .expect("secondary/vector writer closes with pending deltas");
        db = HelixDB::open_with_object_store_for_index_lifecycle_testing(
            &database,
            Arc::clone(&object_store),
            crate::DbConfig::new(),
            crate::index_lifecycle_testing::LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("secondary/vector writer reopens with pending deltas");
        assert_build_delta_count_at_least(&db, scope, &definition, 3).await;
        assert_eq!(
            db.get_index_operation(scope, operation_id)
                .await
                .expect("reopened operation remains readable")
                .common()
                .stage,
            stage,
            "{definition:?} must preserve {stage:?} while pending deltas survive a cold reopen"
        );
    }

    let evidence = controller
        .advance(
            &db,
            LifecycleWorkTarget::Operation {
                scope,
                operation_id,
            },
        )
        .await
        .expect("late validation mutation re-entry step succeeds");
    assert_monotonic_step(&evidence);
    if !matches!(
        stage,
        IndexOperationStage::Scan | IndexOperationStage::CatchUp
    ) {
        let reentered = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("re-entered operation remains readable");
        assert_eq!(
            reentered.common().stage,
            IndexOperationStage::CatchUp,
            "{definition:?} must leave {stage:?} for catch-up after late mutations"
        );
    }

    let terminal = drive_to_terminal(&db, &controller, scope, operation_id).await;
    assert!(
        matches!(terminal, IndexOperationStatus::Succeeded { .. }),
        "{definition:?} did not activate after a late mutation at {stage:?}: {terminal:?}"
    );
    assert_identity_active(&db, scope, &definition).await;
    assert_build_deltas_empty(&db, scope, &definition).await;
    assert_source_rows(
        &db,
        scope,
        &definition,
        updated_id,
        deleted_id,
        stable_id,
        inserted_id,
    )
    .await;
    assert_active_results(&db, scope, &definition, updated_id, stable_id, inserted_id).await;
    db.close()
        .await
        .expect("all-index validation writer closes");
}

async fn assert_secondary_applied_state_present<const N: usize>(
    db: &HelixDB,
    scope: DataScope,
    operation_id: IndexOperationId,
    definition: &ValidatedDynamicIndexDefinition,
    entity_ids: [u64; N],
) {
    let writer = db
        .lifecycle_test_writer_db()
        .expect("secondary applied-state assertion has writer storage");
    let record = crate::index_lifecycle::repository::load_index_record(
        writer,
        scope,
        &definition.identity(),
    )
    .await
    .expect("building secondary record decodes")
    .expect("building secondary record exists");
    for entity_id in entity_ids {
        let key = ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::AppliedState(IndexEntityStateKey {
                index_id: record.index_id(),
                generation: record.state().generation(),
                entity: IndexEntity {
                    kind: definition.identity().element_kind(),
                    id: crate::index_lifecycle::IndexEntityId::new(entity_id),
                },
            }),
        }
        .to_bytes();
        if writer
            .get(key)
            .await
            .expect("secondary applied-state row is readable")
            .is_none()
        {
            let prefix = ManagedIndexKey::data_prefix(
                scope,
                ScopedKey::generation_prefix(
                    RecordKind::AppliedState,
                    record.index_id(),
                    record.state().generation(),
                ),
            );
            let mut rows = writer
                .scan_prefix(&prefix, ..)
                .await
                .expect("secondary applied-state prefix is readable");
            let mut count = 0usize;
            while rows
                .next()
                .await
                .expect("secondary applied-state prefix scan succeeds")
                .is_some()
            {
                count += 1;
            }
            let operation =
                crate::index_lifecycle::outbox::read_queued_operation(writer, operation_id)
                    .await
                    .expect("secondary operation is readable")
                    .expect("secondary operation remains queued")
                    .1;
            panic!(
                "{definition:?} lost applied state for entity {entity_id} before late mutation; index {} generation {} retains {count} applied rows; progress {:?}",
                record.index_id().get(),
                record.state().generation().get(),
                operation.progress(),
            );
        }
    }
}

async fn drive_until_text_validation_lane(
    db: &HelixDB,
    controller: &LifecycleTestController,
    scope: DataScope,
    operation_id: IndexOperationId,
    expected: TextManifestValidationLane,
) {
    let target = LifecycleWorkTarget::Operation {
        scope,
        operation_id,
    };
    let logical_start = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("all-index validation clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("all-index validation time fits u64 milliseconds");
    for turn in 0..MAXIMUM_CONTROLLER_TURNS {
        if controller
            .text_manifest_validation_lane(db, scope, operation_id)
            .await
            .expect("text validation lane is readable")
            == Some(expected)
        {
            return;
        }
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("all-index validation turn fits u64")
                .saturating_mul(60_000),
        );
        let evidence = controller
            .advance_at_unix_millis(db, target, logical_now)
            .await
            .expect("text validation lane operation step succeeds");
        assert_monotonic_step(&evidence);
        let page = controller
            .discover(
                db,
                NonZeroUsize::new(1_024).expect("all-index discovery bound is positive"),
            )
            .await
            .expect("text validation child work remains discoverable");
        for child in page.targets {
            if child == target {
                continue;
            }
            let evidence = controller
                .advance_at_unix_millis(db, child, logical_now)
                .await
                .expect("text validation child step succeeds");
            assert_monotonic_step(&evidence);
        }
    }
    panic!("text validation lane {expected:?} was not reached");
}

async fn drive_until_exact_stage(
    db: &HelixDB,
    controller: &LifecycleTestController,
    scope: DataScope,
    operation_id: IndexOperationId,
    expected: IndexOperationStage,
) {
    let target = LifecycleWorkTarget::Operation {
        scope,
        operation_id,
    };
    let logical_start = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("all-index validation clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("all-index validation time fits u64 milliseconds");
    for turn in 0..MAXIMUM_CONTROLLER_TURNS {
        if matches!(
            controller
                .inspect(db, target)
                .await
                .expect("all-index validation checkpoint is readable"),
            LifecycleCheckpoint::Present {
                stage: LifecycleStage::Operation(actual),
                ..
            } if actual == expected
        ) {
            return;
        }
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("controller turn fits u64")
                .saturating_mul(60_000),
        );
        let evidence = controller
            .advance_at_unix_millis(db, target, logical_now)
            .await
            .expect("all-index validation operation step succeeds");
        assert_monotonic_step(&evidence);
        if matches!(
            evidence.after,
            LifecycleCheckpoint::Present {
                stage: LifecycleStage::Operation(actual),
                ..
            } if actual == expected
        ) {
            return;
        }
        let page = controller
            .discover(
                db,
                NonZeroUsize::new(1_024).expect("all-index discovery bound is positive"),
            )
            .await
            .expect("all-index validation child work is discoverable");
        assert!(page.exhausted, "small all-index contract fits one page");
        for child in page.targets {
            if child == target {
                continue;
            }
            let evidence = controller
                .advance_at_unix_millis(db, child, logical_now)
                .await
                .expect("all-index validation child step succeeds");
            assert_monotonic_step(&evidence);
        }
    }
    panic!("operation did not reach exact live stage {expected:?}");
}

fn fixture_properties(
    definition: &ValidatedDynamicIndexDefinition,
    value: FixtureValue,
) -> Vec<Property> {
    fixture_properties_with_tenant(definition, value, None)
}

fn fixture_properties_with_tenant(
    definition: &ValidatedDynamicIndexDefinition,
    value: FixtureValue,
    tenant: Option<&str>,
) -> Vec<Property> {
    let mut properties = vec![Property::string(
        "$label",
        definition.identity().label().as_str(),
    )];
    let property = match definition {
        ValidatedDynamicIndexDefinition::Secondary(_) => Property::string(
            INDEX_PROPERTY,
            match value {
                FixtureValue::Initial(ordinal) => format!("initial-{ordinal}"),
                FixtureValue::Intermediate => "intermediate".to_string(),
                FixtureValue::Updated => "updated".to_string(),
                FixtureValue::Inserted => "inserted".to_string(),
            },
        ),
        ValidatedDynamicIndexDefinition::Vector(_) => Property::f32_array(
            INDEX_PROPERTY,
            match value {
                FixtureValue::Initial(0) => vec![1.0, 0.1, 0.1],
                FixtureValue::Initial(1) => vec![0.1, 1.0, 0.1],
                FixtureValue::Initial(_) => vec![0.1, 0.1, 1.0],
                FixtureValue::Intermediate => vec![-0.2, 0.1, 1.0],
                FixtureValue::Updated => vec![-1.0, 0.2, 0.1],
                FixtureValue::Inserted => vec![0.2, -1.0, 0.1],
            },
        ),
        ValidatedDynamicIndexDefinition::Text(_) => Property::string(
            INDEX_PROPERTY,
            match value {
                FixtureValue::Initial(ordinal) => {
                    format!("initialvalidationtoken{ordinal}")
                }
                FixtureValue::Intermediate => "intermediatevalidationtoken".to_string(),
                FixtureValue::Updated => "updatedvalidationtoken".to_string(),
                FixtureValue::Inserted => "insertedvalidationtoken".to_string(),
            },
        ),
    };
    properties.push(property);
    let partitioned = match definition {
        ValidatedDynamicIndexDefinition::Secondary(_) => false,
        ValidatedDynamicIndexDefinition::Vector(definition) => {
            definition.tenant_property().is_some()
        }
        ValidatedDynamicIndexDefinition::Text(definition) => definition.tenant_property().is_some(),
    };
    if partitioned {
        properties.push(Property::string(
            TENANT_PROPERTY,
            tenant.unwrap_or(TENANT_A),
        ));
    }
    properties
}

async fn put_edge_endpoints(db: &HelixDB, scope: DataScope, entity_id: u64) {
    db.lifecycle_test_writer_db()
        .expect("edge endpoint fixture has writer storage")
        .put(
            DataKey::Data {
                scope,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(entity_id)),
            }
            .to_bytes(),
            EdgeEndpointsValue::new(0, 1).encode(),
        )
        .await
        .expect("edge endpoint fixture commits");
}

async fn mutate_entity(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    entity_id: u64,
    before: &[Property],
    after: &[Property],
) {
    let element_kind = definition.identity().element_kind();
    match definition {
        ValidatedDynamicIndexDefinition::Secondary(_) => match element_kind {
            IndexElementKind::Node => mutate_source(db, scope, entity_id, before, after).await,
            IndexElementKind::Edge => mutate_edge_source(db, scope, entity_id, before, after).await,
        }
        .expect("late secondary mutation commits"),
        ValidatedDynamicIndexDefinition::Vector(_) => {
            let writer = db
                .lifecycle_test_writer_db()
                .expect("late vector mutation has writer storage");
            let transaction = writer
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("late vector mutation transaction begins");
            let mutations = crate::index_lifecycle::vector::load_mutation_set(&transaction, scope)
                .await
                .expect("late vector mutation loads its building generation");
            let cache_writes = crate::search::vector::VectorCacheWriteSet::default();
            crate::index_lifecycle::vector::maintain_entity(
                &transaction,
                scope,
                &mutations,
                &cache_writes,
                crate::index_lifecycle::vector::VectorEntityMutation::new(
                    element_kind,
                    entity_id,
                    before,
                    after,
                ),
            )
            .await
            .expect("late vector mutation records its build delta");
            stage_source_row(&transaction, scope, element_kind, entity_id, after);
            transaction
                .commit()
                .await
                .expect("late vector source and build delta commit atomically");
        }
        ValidatedDynamicIndexDefinition::Text(_) => {
            let writer = db
                .lifecycle_test_writer_db()
                .expect("late text mutation has writer storage");
            let transaction = writer
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("late text mutation transaction begins");
            let mutations =
                crate::index_lifecycle::text::mutation::load_mutation_set(&transaction, scope)
                    .await
                    .expect("late text mutation loads its building generation");
            let prepared = crate::index_lifecycle::text::mutation::prepare_text_build_deltas(
                &transaction,
                scope,
                &mutations,
                crate::index_lifecycle::text::mutation::TextEntityMutation::new(
                    element_kind,
                    entity_id,
                    before,
                    after,
                ),
            )
            .await
            .expect("late text mutation prepares its build delta");
            let validated = crate::index_lifecycle::text::mutation::validate_text_build_deltas(
                &transaction,
                &prepared,
            )
            .await
            .expect("late text mutation revalidates its build delta");
            crate::index_lifecycle::text::mutation::stage_validated_text_build_deltas(
                &transaction,
                validated,
            )
            .expect("late text mutation stages its validated build delta");
            stage_source_row(&transaction, scope, element_kind, entity_id, after);
            transaction
                .commit()
                .await
                .expect("late text source, statistics, and build delta commit atomically");
        }
    }
}

fn stage_source_row(
    transaction: &slatedb::DbTransaction,
    scope: DataScope,
    element_kind: IndexElementKind,
    entity_id: u64,
    after: &[Property],
) {
    let key = match element_kind {
        IndexElementKind::Node => source_key(scope, entity_id),
        IndexElementKind::Edge => edge_source_key(scope, entity_id),
    };
    if after.is_empty() {
        transaction.delete(key).expect("late source delete stages");
    } else {
        transaction
            .put(key, encode_properties(after))
            .expect("late source replacement stages");
    }
}

async fn assert_source_rows(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    updated_id: u64,
    deleted_id: u64,
    stable_id: u64,
    inserted_id: u64,
) {
    let writer = db
        .lifecycle_test_writer_db()
        .expect("all-index source assertion has writer storage");
    let key = |entity_id| match definition.identity().element_kind() {
        IndexElementKind::Node => source_key(scope, entity_id),
        IndexElementKind::Edge => edge_source_key(scope, entity_id),
    };
    for (entity_id, expected) in [
        (updated_id, FixtureValue::Updated),
        (stable_id, FixtureValue::Initial(2)),
        (inserted_id, FixtureValue::Inserted),
    ] {
        let stored = writer
            .get(key(entity_id))
            .await
            .expect("authoritative source read succeeds")
            .expect("authoritative source row remains present");
        assert_eq!(
            decode_properties(&stored).expect("authoritative source properties decode"),
            fixture_properties_with_tenant(
                definition,
                expected,
                matches!(
                    definition,
                    ValidatedDynamicIndexDefinition::Vector(definition)
                        if definition.tenant_property().is_some()
                            && matches!(expected, FixtureValue::Updated | FixtureValue::Inserted)
                )
                .then_some(TENANT_C),
            )
        );
    }
    assert!(
        writer
            .get(key(deleted_id))
            .await
            .expect("deleted authoritative source read succeeds")
            .is_none(),
        "deleted source row must remain absent"
    );
}

async fn assert_active_results(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    updated_id: u64,
    stable_id: u64,
    inserted_id: u64,
) {
    match definition {
        ValidatedDynamicIndexDefinition::Secondary(definition) => {
            assert_secondary_results(db, scope, definition, updated_id, stable_id, inserted_id)
                .await;
        }
        ValidatedDynamicIndexDefinition::Vector(_) => {
            let partitioned = matches!(
                definition,
                ValidatedDynamicIndexDefinition::Vector(definition)
                    if definition.tenant_property().is_some()
            );
            let final_tenant = if partitioned { TENANT_C } else { TENANT_A };
            assert_search_results(
                db,
                scope,
                definition,
                SearchFixture::Vector(FixtureValue::Updated, final_tenant),
                [updated_id],
            )
            .await;
            assert_search_results(
                db,
                scope,
                definition,
                SearchFixture::Vector(FixtureValue::Inserted, final_tenant),
                [inserted_id],
            )
            .await;
            if partitioned {
                assert_search_results(
                    db,
                    scope,
                    definition,
                    SearchFixture::VectorAll(TENANT_A),
                    [stable_id],
                )
                .await;
                assert_search_results(
                    db,
                    scope,
                    definition,
                    SearchFixture::VectorAll(TENANT_B),
                    [],
                )
                .await;
                assert_search_results(
                    db,
                    scope,
                    definition,
                    SearchFixture::VectorAll(TENANT_C),
                    [updated_id, inserted_id],
                )
                .await;
            } else {
                assert_search_results(
                    db,
                    scope,
                    definition,
                    SearchFixture::VectorAll(TENANT_A),
                    [updated_id, stable_id, inserted_id],
                )
                .await;
            }
        }
        ValidatedDynamicIndexDefinition::Text(_) => {
            assert_search_results(
                db,
                scope,
                definition,
                SearchFixture::Text("updatedvalidationtoken"),
                [updated_id],
            )
            .await;
            assert_search_results(
                db,
                scope,
                definition,
                SearchFixture::Text("insertedvalidationtoken"),
                [inserted_id],
            )
            .await;
            assert_search_results(
                db,
                scope,
                definition,
                SearchFixture::Text("initialvalidationtoken1"),
                [],
            )
            .await;
            assert_search_results(
                db,
                scope,
                definition,
                SearchFixture::Text("initialvalidationtoken2"),
                [stable_id],
            )
            .await;
        }
    }
}

async fn assert_secondary_results(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    updated_id: u64,
    stable_id: u64,
    inserted_id: u64,
) {
    let dynamic = ValidatedDynamicIndexDefinition::Secondary(definition.clone());
    let record = crate::index_lifecycle::repository::load_index_record(
        db.lifecycle_test_writer_db()
            .expect("secondary result assertion has writer storage"),
        scope,
        &dynamic.identity(),
    )
    .await
    .expect("active secondary record decodes")
    .expect("active secondary record exists");
    let handle = ActiveIndexHandle::try_from_record(scope, &record)
        .expect("active secondary record projects one handle");
    match definition {
        ValidatedSecondaryIndexDefinition::NodeEquality { .. }
        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
            for (value, expected) in [
                ("updated", BTreeSet::from([updated_id])),
                ("initial-1", BTreeSet::new()),
                ("initial-2", BTreeSet::from([stable_id])),
                ("inserted", BTreeSet::from([inserted_id])),
            ] {
                let actual = crate::index_lifecycle::secondary::lookup_active_equality_generation(
                    db.lifecycle_test_writer_db()
                        .expect("secondary equality result has writer storage"),
                    &handle,
                    &PropertyValue::String(value.to_string()),
                )
                .await
                .expect("secondary equality result remains readable")
                .iter()
                .collect::<BTreeSet<_>>();
                assert_eq!(
                    actual, expected,
                    "secondary equality result differs for {definition:?} value {value}"
                );
            }
        }
        ValidatedSecondaryIndexDefinition::NodeRange { .. }
        | ValidatedSecondaryIndexDefinition::EdgeRange { .. } => {
            let actual =
                crate::index_lifecycle::secondary::scan_active_range_generation_with_membership(
                    db.lifecycle_test_writer_db()
                        .expect("secondary range result has writer storage"),
                    &handle,
                    None,
                    None,
                    &[],
                )
                .await
                .expect("secondary range result remains readable")
                .into_iter()
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, BTreeSet::from([updated_id, stable_id, inserted_id]));
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SearchFixture {
    Vector(FixtureValue, &'static str),
    VectorAll(&'static str),
    Text(&'static str),
}

impl SearchFixture {
    const fn tenant(self) -> &'static str {
        match self {
            Self::Vector(_, tenant) | Self::VectorAll(tenant) => tenant,
            Self::Text(_) => TENANT_A,
        }
    }
}

async fn assert_search_results<const N: usize>(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    fixture: SearchFixture,
    expected: [u64; N],
) {
    let result = db
        .execute_scoped(
            &search_plan(definition, fixture),
            context::ParamBindings::default(),
            scope,
        )
        .await
        .expect("active search result remains readable");
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("active search result returns projected IDs");
    };
    let actual = values
        .into_iter()
        .map(
            |value| match (definition.identity().element_kind(), value) {
                (IndexElementKind::Node, ExecutionScalar::NodeId(entity_id))
                | (IndexElementKind::Edge, ExecutionScalar::EdgeId(entity_id)) => entity_id,
                (element_kind, value) => {
                    panic!("{element_kind:?} search returned another scalar: {value:?}")
                }
            },
        )
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual,
        expected.into_iter().collect(),
        "search result differs for {definition:?} fixture {fixture:?}"
    );
}

fn search_plan(
    definition: &ValidatedDynamicIndexDefinition,
    fixture: SearchFixture,
) -> exec::ExecutablePlan {
    let index = search_index_plan(definition, fixture.tenant());
    let access = match (definition, fixture) {
        (ValidatedDynamicIndexDefinition::Vector(definition), SearchFixture::Vector(value, _)) => {
            search_access_vector(definition, index, vector_value(value), NonZeroUsize::MIN)
        }
        (ValidatedDynamicIndexDefinition::Vector(definition), SearchFixture::VectorAll(_)) => {
            search_access_vector(
                definition,
                index,
                vector_value(FixtureValue::Updated),
                NonZeroUsize::new(10).expect("all-vector search limit is positive"),
            )
        }
        (ValidatedDynamicIndexDefinition::Text(definition), SearchFixture::Text(query)) => {
            match definition.element_kind() {
                IndexElementKind::Node => {
                    exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::TextSearch {
                        key: catalog::NodeSearchIndexKey::try_new(
                            definition.label().as_str(),
                            definition.property().as_str(),
                        )
                        .expect("node text validation key is valid"),
                        index,
                        query_text: ir::TextQueryInputPlan::Text(public_name(query)),
                        k: ir::SearchLimitPlan::Literal(
                            NonZeroUsize::new(10).expect("text validation limit is positive"),
                        ),
                    })
                }
                IndexElementKind::Edge => {
                    let key = catalog::EdgeSearchIndexKey::try_new(
                        definition.label().as_str(),
                        definition.property().as_str(),
                    )
                    .expect("edge text validation key is valid");
                    exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::TextSearch {
                        key,
                        index,
                        query_text: ir::TextQueryInputPlan::Text(public_name(query)),
                        k: ir::SearchLimitPlan::Literal(
                            NonZeroUsize::new(10).expect("text validation limit is positive"),
                        ),
                    })
                }
            }
        }
        (definition, fixture) => {
            panic!("invalid search fixture {fixture:?} for {definition:?}")
        }
    };
    let access_id = exec::ExecStepId::new(1).expect("validation access ID is positive");
    public_executable(
        ir::PlanKind::Read,
        vec![
            public_step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(access),
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

fn search_access_vector(
    definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    index: ir::SearchIndexPlan,
    query: Vec<f32>,
    k: NonZeroUsize,
) -> exec::ExecAccessPlan {
    let query_vector = ir::VectorQueryInputPlan::Vector(
        ir::SearchVector::new(query).expect("validation vector query is finite and non-empty"),
    );
    match definition.element_kind() {
        IndexElementKind::Node => {
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::VectorSearch {
                key: catalog::NodeSearchIndexKey::try_new(
                    definition.label().as_str(),
                    definition.property().as_str(),
                )
                .expect("node vector validation key is valid"),
                index,
                query_vector,
                k: ir::SearchLimitPlan::Literal(k),
            })
        }
        IndexElementKind::Edge => {
            exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::VectorSearch {
                key: catalog::EdgeSearchIndexKey::try_new(
                    definition.label().as_str(),
                    definition.property().as_str(),
                )
                .expect("edge vector validation key is valid"),
                index,
                query_vector,
                k: ir::SearchLimitPlan::Literal(k),
            })
        }
    }
}

fn search_index_plan(
    definition: &ValidatedDynamicIndexDefinition,
    tenant_value: &str,
) -> ir::SearchIndexPlan {
    let (index_id, tenant_property) = match definition {
        ValidatedDynamicIndexDefinition::Vector(definition) => (
            vector_index_name(
                match definition.element_kind() {
                    IndexElementKind::Node => VectorElementType::Node,
                    IndexElementKind::Edge => VectorElementType::Edge,
                },
                definition.label().as_str(),
                definition.property().as_str(),
            ),
            definition.tenant_property(),
        ),
        ValidatedDynamicIndexDefinition::Text(definition) => (
            text_index_name(
                match definition.element_kind() {
                    IndexElementKind::Node => TextElementType::Node,
                    IndexElementKind::Edge => TextElementType::Edge,
                },
                definition.label().as_str(),
                definition.property().as_str(),
            ),
            definition.tenant_property(),
        ),
        ValidatedDynamicIndexDefinition::Secondary(_) => {
            panic!("secondary validation uses exact physical reads")
        }
    };
    let tenant = tenant_property.map_or(ir::SearchTenantPlan::Unscoped, |property| {
        ir::SearchTenantPlan::ScopedValue {
            property: public_name(property.as_str()),
            value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(
                AstPropertyValue::String(tenant_value.to_string()),
            ))
            .expect("validation tenant value is non-null"),
        }
    });
    ir::SearchIndexPlan {
        index_id: public_name(&index_id),
        tenant,
    }
}

fn vector_value(value: FixtureValue) -> Vec<f32> {
    match value {
        FixtureValue::Initial(0) => vec![1.0, 0.1, 0.1],
        FixtureValue::Initial(1) => vec![0.1, 1.0, 0.1],
        FixtureValue::Initial(_) => vec![0.1, 0.1, 1.0],
        FixtureValue::Intermediate => vec![-0.2, 0.1, 1.0],
        FixtureValue::Updated => vec![-1.0, 0.2, 0.1],
        FixtureValue::Inserted => vec![0.2, -1.0, 0.1],
    }
}
