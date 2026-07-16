//! Fixed-shape production lifecycle scale orchestration.
//!
//! Authoritative graph rows are seeded with the deployed typed key/value
//! codecs so setup does not spend 100,000 interpreter transactions. Every
//! index operation after setup crosses the public physical-plan interpreter,
//! durable outbox, supervised worker, catalog refresh, and public indexed-read
//! boundary. The test uses only current f32 vectors; deferred f16 and binary
//! codecs are neither activated nor persisted here.

use std::num::NonZeroUsize;
use std::ops::Bound;
use std::time::{Duration, Instant};

use slatedb::IsolationLevel;

use crate::config::{TextElementType, VectorElementType};
use crate::encoding::property::property_value::PropertyValue as StoredPropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::{DataScope, TenantId};
use crate::encoding::v1::keys::{DataKeyKind, Key, NodePropertyKey};
use crate::execution::interpreter::{ExecutionResult, ExecutionScalar, ExecutionValue};
use crate::index_v2::{
    IndexDdlReceipt, IndexOperationStage, IndexOperationStatus, IndexOperationStatusCommon,
    PublicIndexFamily,
};
use crate::search::{text_index_name, vector_index_name};
use crate::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::value::PropertyValue as PlannerPropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};

const ENTITY_COUNT: usize = 100_000;
const TENANT_COUNT: usize = 16;
const VECTOR_DIMENSION: usize = 128;
const SEED_BATCH_ROWS: usize = 512;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const LABEL: &str = "ScaleDocument";
const NON_UNIQUE_PROPERTY: &str = "group";
const UNIQUE_PROPERTY: &str = "external_id";
const VECTOR_PROPERTY: &str = "embedding";
const TEXT_PROPERTY: &str = "body";

/// Constructs a validated planner identifier used by scale fixtures.
fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("scale fixture identifiers are non-empty")
}

/// Constructs one executable step with neutral scheduling metadata.
fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("scale fixture step ids are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

/// Seals a fixture DAG behind the production executable-plan validator.
fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("scale fixture plans are non-empty"),
        exec::ExecStepId::new(root).expect("scale fixture root ids are positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("scale fixture dependencies form a valid executable plan")
}

/// Builds one public CREATE plan for an already validated family definition.
fn create_plan(spec: ir::IndexDdlCreateSpec) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec,
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds one public DROP plan for an active family definition.
fn drop_plan(spec: ir::IndexDdlDropSpec) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Drop { spec },
            },
        )],
        1,
    )
}

/// Builds one node equality lookup followed by an ID projection.
fn equality_search_plan(property: &str, value: PlannerPropertyValue) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::EqualityIndex {
                            index: catalog::NodeEqualityIndexMeta::new(name(&format!(
                                "node_eq:{LABEL}:{property}"
                            ))),
                            key: catalog::ScopedPropertyKey::try_new(LABEL, property)
                                .expect("scale equality key is valid"),
                            value: ir::IndexValue::Literal(
                                ir::SecondaryIndexLiteral::new(value)
                                    .expect("scale equality literal is indexable"),
                            ),
                        },
                    )),
                },
            ),
            step(
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

/// Builds one top-one 128D vector lookup followed by an ID projection.
fn vector_search_plan(query: Vec<f32>) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::VectorSearch {
                            key: catalog::NodeSearchIndexKey::try_new(LABEL, VECTOR_PROPERTY)
                                .expect("scale vector key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&vector_index_name(
                                    VectorElementType::Node,
                                    LABEL,
                                    VECTOR_PROPERTY,
                                )),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_vector: ir::VectorQueryInputPlan::Vector(
                                ir::SearchVector::new(query)
                                    .expect("scale query vector is non-empty and finite"),
                            ),
                            k: ir::SearchLimitPlan::Literal(NonZeroUsize::MIN),
                        },
                    )),
                },
            ),
            step(
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

/// Builds one top-ten text lookup followed by an ID projection.
fn text_search_plan(query: &str) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("scale access id is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::TextSearch {
                            key: catalog::NodeSearchIndexKey::try_new(LABEL, TEXT_PROPERTY)
                                .expect("scale text key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&text_index_name(
                                    TextElementType::Node,
                                    LABEL,
                                    TEXT_PROPERTY,
                                )),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_text: ir::TextQueryInputPlan::Text(name(query)),
                            k: ir::SearchLimitPlan::Literal(
                                NonZeroUsize::new(10).expect("scale text limit is positive"),
                            ),
                        },
                    )),
                },
            ),
            step(
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

/// Converts an indexed-read projection without accepting mixed scalar kinds.
fn projected_node_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("scale indexed read should return projected scalars");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::NodeId(id) = value else {
                panic!("scale node projection should contain only node ids");
            };
            id
        })
        .collect()
}

/// Returns a source key in exactly one typed data scope.
fn source_key(scope: DataScope, entity_id: u64) -> bytes::Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
    }
    .to_bytes()
}

/// Returns the deterministic 128D source vector for one entity.
fn vector(entity_id: u64) -> Vec<f32> {
    let mut state = entity_id.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut vector = Vec::with_capacity(VECTOR_DIMENSION);
    for _ in 0..VECTOR_DIMENSION {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let centered = i32::from((state & 0xffff) as u16) - 32_768;
        vector.push(centered as f32 / 32_768.0);
    }
    vector
}

/// Returns the complete authoritative property row for one unscoped entity.
fn unscoped_properties(entity_id: u64) -> Vec<Property> {
    let group = if entity_id < 2 {
        "shared-target".to_string()
    } else {
        format!("group-{entity_id}")
    };
    let body = if entity_id == 0 {
        "common scale text uniqueneedle".to_string()
    } else {
        format!("common scale text bucket{}", entity_id % 1_000)
    };
    vec![
        Property::new("$label", StoredPropertyValue::String(LABEL.to_string())),
        Property::new(NON_UNIQUE_PROPERTY, StoredPropertyValue::String(group)),
        Property::new(
            UNIQUE_PROPERTY,
            StoredPropertyValue::String(format!("external-{entity_id}")),
        ),
        Property::new(
            VECTOR_PROPERTY,
            StoredPropertyValue::F32Array(vector(entity_id)),
        ),
        Property::new(TEXT_PROPERTY, StoredPropertyValue::String(body)),
    ]
}

/// Returns the authoritative property row for one tenant-scale entity.
fn tenant_properties(entity_id: u64) -> Vec<Property> {
    vec![
        Property::new("$label", StoredPropertyValue::String(LABEL.to_string())),
        Property::new(
            NON_UNIQUE_PROPERTY,
            StoredPropertyValue::String(format!("tenant-group-{}", entity_id % 256)),
        ),
    ]
}

/// Seeds one scope in bounded transactions using only canonical graph codecs.
async fn seed_scope(
    db: &slatedb::Db,
    scope: DataScope,
    start_id: usize,
    entity_count: usize,
    properties: fn(u64) -> Vec<Property>,
) {
    for batch_start in (0..entity_count).step_by(SEED_BATCH_ROWS) {
        let batch_end = entity_count.min(batch_start.saturating_add(SEED_BATCH_ROWS));
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("scale seed transaction opens");
        for offset in batch_start..batch_end {
            let entity_id =
                u64::try_from(start_id.saturating_add(offset)).expect("scale entity id fits u64");
            transaction
                .put(
                    source_key(scope, entity_id),
                    encode_properties(&properties(entity_id)),
                )
                .expect("scale source row stages");
        }
        transaction
            .commit()
            .await
            .expect("scale source batch commits");
    }
}

/// Extracts the durable operation ID returned through the public interpreter.
fn accepted_operation_id(result: ExecutionResult) -> crate::index_v2::IndexOperationId {
    let Some(ExecutionValue::IndexDdlReceipt(receipt)) = result.last else {
        panic!("scale DDL should return a durable receipt");
    };
    match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => {
            panic!("fresh scale DDL should not find an already-active index")
        }
    }
}

/// Waits for one accepted operation and refreshes its exact scoped catalog.
async fn execute_ddl_to_success(
    db: &HelixDB,
    scope: DataScope,
    plan: &exec::ExecutablePlan,
) -> IndexOperationStatusCommon {
    let started = Instant::now();
    let mut next_progress_log = Duration::from_secs(30);
    let operation_id = accepted_operation_id(
        db.execute_scoped(plan, context::ParamBindings::default(), scope)
            .await
            .expect("scale DDL is durably accepted"),
    );
    let status = tokio::time::timeout(OPERATION_TIMEOUT, async {
        loop {
            match db
                .get_index_operation(scope, operation_id)
                .await
                .expect("accepted scale operation remains readable")
            {
                status @ (IndexOperationStatus::Queued { .. }
                | IndexOperationStatus::Running { .. }) => {
                    if started.elapsed() >= next_progress_log {
                        let common = status.common();
                        eprintln!(
                            "index_v2_scale_progress family={:?} stage={:?} entities={} output_operations={} claims={} elapsed_ms={}",
                            common.family,
                            common.stage,
                            common.progress.entities,
                            common.progress.output_operations,
                            common.attempt,
                            started.elapsed().as_millis(),
                        );
                        next_progress_log = next_progress_log.saturating_add(Duration::from_secs(30));
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                status @ IndexOperationStatus::Succeeded { .. } => break status,
                IndexOperationStatus::Blocked {
                    common,
                    blocker_code,
                    ..
                } => panic!(
                    "scale {:?} operation blocked at {:?}: {:?}",
                    common.family, common.stage, blocker_code
                ),
                IndexOperationStatus::Aborted { common } => panic!(
                    "scale {:?} operation was unexpectedly aborted at {:?}",
                    common.family, common.stage
                ),
            }
        }
    })
    .await
    .expect("scale operation should converge within one hour");
    db.planner_context_scoped(context::ParamBindings::default(), scope)
        .await
        .expect("terminal scale DDL is visible through the loaded catalog");
    status.common().clone()
}

/// Emits one stable release-observation line for the audit ledger.
fn record_measurement(name: &str, elapsed: Duration, status: &IndexOperationStatusCommon) {
    eprintln!(
        "index_v2_scale name={name} family={:?} entities={} input_bytes={} output_operations={} output_bytes={} claims={} elapsed_ms={}",
        status.family,
        status.progress.entities,
        status.progress.input_bytes,
        status.progress.output_operations,
        status.progress.output_bytes,
        status.attempt,
        elapsed.as_millis(),
    );
}

/// Proves terminal DROP removed every transient and physical V2 row in the tested scopes.
async fn assert_no_lifecycle_residue(db: &HelixDB, scopes: &[DataScope]) {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("scale residue checks require writer storage");
    };
    for kind in [
        index_keys::GlobalIndexV2Kind::OperationPointer,
        index_keys::GlobalIndexV2Kind::UploadPointer,
        index_keys::GlobalIndexV2Kind::BlobGcRunRoot,
        index_keys::GlobalIndexV2Kind::BlobGcReachabilityMark,
        index_keys::GlobalIndexV2Kind::BlobReachabilityReference,
        index_keys::GlobalIndexV2Kind::BlobGcCandidateMember,
    ] {
        let prefix = index_keys::GlobalIndexV2Key::logical_prefix(kind);
        let mut rows = writer
            .db()
            .scan_prefix(
                &prefix,
                (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
            )
            .await
            .expect("global V2 residue lane remains readable");
        assert!(
            rows.next()
                .await
                .expect("global V2 residue scan succeeds")
                .is_none(),
            "terminal DROP retained a global {kind:?} row"
        );
    }

    for scope in scopes {
        for kind in [
            index_keys::IndexV2RecordKind::BuildDelta,
            index_keys::IndexV2RecordKind::AppliedState,
            index_keys::IndexV2RecordKind::SecondaryEntry,
            index_keys::IndexV2RecordKind::TextManifestRoot,
            index_keys::IndexV2RecordKind::TextManifestPage,
            index_keys::IndexV2RecordKind::TextUploadIntent,
            index_keys::IndexV2RecordKind::TextBuildArtifact,
            index_keys::IndexV2RecordKind::BlobGcCandidate,
            index_keys::IndexV2RecordKind::BlobGcState,
            index_keys::IndexV2RecordKind::TextEntityState,
            index_keys::IndexV2RecordKind::ActiveMutationCommitProof,
            index_keys::IndexV2RecordKind::BlobReachabilityReference,
            index_keys::IndexV2RecordKind::VectorPartitionMapping,
        ] {
            let prefix = Key::data_prefix(*scope, index_keys::IndexV2Key::logical_prefix(kind));
            let mut rows = writer
                .db()
                .scan_prefix(
                    &prefix,
                    (Bound::Unbounded, Bound::<bytes::Bytes>::Unbounded),
                )
                .await
                .expect("scoped V2 residue lane remains readable");
            assert!(
                rows.next()
                    .await
                    .expect("scoped V2 residue scan succeeds")
                    .is_none(),
                "terminal DROP retained a scoped {kind:?} row in {scope:?}"
            );
        }
    }
}

/// Executes one CREATE and records its durable bounded-work counters.
async fn build(
    db: &HelixDB,
    scope: DataScope,
    name: &str,
    spec: ir::IndexDdlCreateSpec,
) -> IndexOperationStatusCommon {
    let started = Instant::now();
    let status = execute_ddl_to_success(db, scope, &create_plan(spec)).await;
    record_measurement(name, started.elapsed(), &status);
    status
}

/// Executes one DROP through the same public lifecycle boundary.
async fn drop_index(db: &HelixDB, scope: DataScope, spec: ir::IndexDdlDropSpec) {
    let status = execute_ddl_to_success(db, scope, &drop_plan(spec)).await;
    assert_eq!(
        status.stage,
        IndexOperationStage::Finalize,
        "successful DROP should retain its terminal cleanup checkpoint"
    );
}

/// Opens a coordinated writer and seeds the fixed unscoped authoritative shape.
async fn open_seeded_unscoped(database: &str) -> HelixDB {
    assert_eq!(
        ENTITY_COUNT, 100_000,
        "release scale shape must remain fixed"
    );
    let token = ProcessLocalDatabaseToken::new(database).expect("scale database token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("scale writer opens with both lifecycle coordinators");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("scale database should be a writer");
    };

    let seed_started = Instant::now();
    let unscoped_ids = writer
        .node_ids()
        .allocate_batch(u64::try_from(ENTITY_COUNT).expect("scale entity count fits u64"))
        .await
        .expect("unscoped scale IDs are durably reserved");
    assert_eq!(unscoped_ids, 0..100_000);
    seed_scope(
        writer.db(),
        DataScope::LegacyUnscoped,
        usize::try_from(unscoped_ids.start).expect("unscoped start ID fits usize"),
        ENTITY_COUNT,
        unscoped_properties,
    )
    .await;
    eprintln!(
        "index_v2_scale name=seed_unscoped entities={ENTITY_COUNT} batches={} elapsed_ms={}",
        ENTITY_COUNT.div_ceil(SEED_BATCH_ROWS),
        seed_started.elapsed().as_millis(),
    );
    db
}

/// Runs secondary, text, multi-scope, and cleanup scale oracles.
pub(super) async fn run_secondary_text_tenant() {
    assert_eq!(TENANT_COUNT, 16, "tenant release shape must remain fixed");
    let db = open_seeded_unscoped("index-v2-production-secondary-text-scale").await;
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("scale database should be a writer");
    };

    let property_key = |property| {
        catalog::ScopedPropertyKey::try_new(LABEL, property)
            .expect("scale scoped property key is valid")
    };
    let non_unique_status = build(
        &db,
        DataScope::LegacyUnscoped,
        "secondary_non_unique",
        ir::IndexDdlCreateSpec::NodeEquality {
            key: property_key(NON_UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
    )
    .await;
    assert_eq!(non_unique_status.family, PublicIndexFamily::Secondary);
    let mut actual = projected_node_ids(
        db.execute(
            &equality_search_plan(
                NON_UNIQUE_PROPERTY,
                PlannerPropertyValue::String("shared-target".to_string()),
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("non-unique scale search succeeds"),
    );
    actual.sort_unstable();
    assert_eq!(actual, vec![0, 1]);

    build(
        &db,
        DataScope::LegacyUnscoped,
        "secondary_unique",
        ir::IndexDdlCreateSpec::NodeEquality {
            key: property_key(UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::Unique,
        },
    )
    .await;
    assert_eq!(
        projected_node_ids(
            db.execute(
                &equality_search_plan(
                    UNIQUE_PROPERTY,
                    PlannerPropertyValue::String("external-99999".to_string()),
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("unique scale search succeeds"),
        ),
        vec![99_999]
    );

    build(
        &db,
        DataScope::LegacyUnscoped,
        "text_paged",
        ir::IndexDdlCreateSpec::NodeText {
            key: property_key(TEXT_PROPERTY),
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .await;
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan("uniqueneedle"),
                context::ParamBindings::default(),
            )
            .await
            .expect("paged scale text search succeeds"),
        ),
        vec![0]
    );

    let tenant_seed_started = Instant::now();
    let tenant_ids = writer
        .node_ids()
        .allocate_batch(u64::try_from(ENTITY_COUNT).expect("scale entity count fits u64"))
        .await
        .expect("tenant scale IDs are durably reserved");
    let tenant_start = usize::try_from(tenant_ids.start).expect("tenant start ID fits usize");
    let mut distributed_rows = 0_usize;
    for tenant_ordinal in 0..TENANT_COUNT {
        let tenant_rows = ENTITY_COUNT / TENANT_COUNT;
        let scope = DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ));
        seed_scope(
            writer.db(),
            scope,
            tenant_start.saturating_add(distributed_rows),
            tenant_rows,
            tenant_properties,
        )
        .await;
        distributed_rows = distributed_rows
            .checked_add(tenant_rows)
            .expect("distributed tenant row count remains bounded");
        build(
            &db,
            scope,
            &format!("tenant_{tenant_ordinal}_secondary"),
            ir::IndexDdlCreateSpec::NodeEquality {
                key: property_key(NON_UNIQUE_PROPERTY),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        )
        .await;
        assert!(!projected_node_ids(
            db.execute_scoped(
                &equality_search_plan(
                    NON_UNIQUE_PROPERTY,
                    PlannerPropertyValue::String("tenant-group-0".to_string()),
                ),
                context::ParamBindings::default(),
                scope,
            )
            .await
            .expect("tenant scale search succeeds"),
        )
        .is_empty());
    }
    assert_eq!(distributed_rows, ENTITY_COUNT);
    eprintln!(
        "index_v2_scale name=tenant_workload tenants={TENANT_COUNT} entities={ENTITY_COUNT} elapsed_ms={}",
        tenant_seed_started.elapsed().as_millis(),
    );

    for tenant_ordinal in 0..TENANT_COUNT {
        let scope = DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ));
        drop_index(
            &db,
            scope,
            ir::IndexDdlDropSpec::NodeEquality {
                key: property_key(NON_UNIQUE_PROPERTY),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        )
        .await;
    }
    for spec in [
        ir::IndexDdlDropSpec::NodeText {
            key: property_key(TEXT_PROPERTY),
        },
        ir::IndexDdlDropSpec::NodeEquality {
            key: property_key(UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::Unique,
        },
        ir::IndexDdlDropSpec::NodeEquality {
            key: property_key(NON_UNIQUE_PROPERTY),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        },
    ] {
        drop_index(&db, DataScope::LegacyUnscoped, spec).await;
    }

    assert!(db
        .execute(
            &text_search_plan("uniqueneedle"),
            context::ParamBindings::default(),
        )
        .await
        .is_err());
    let mut scopes = vec![DataScope::LegacyUnscoped];
    scopes.extend((0..TENANT_COUNT).map(|tenant_ordinal| {
        DataScope::Tenant(TenantId::from_u128(
            u128::try_from(tenant_ordinal + 1).expect("tenant ordinal fits u128"),
        ))
    }));
    assert_no_lifecycle_residue(&db, &scopes).await;
    db.close().await.expect("scale writer closes cleanly");
}

/// Runs one text CREATE/search/DROP fixture at an exact authoritative row count.
async fn run_text_drop_fixture(database: &str, measurement: &str, entity_count: usize) {
    let token =
        ProcessLocalDatabaseToken::new(database).expect("text DROP smoke database token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("text DROP smoke writer opens with lifecycle coordinators");
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("text DROP smoke database should be a writer");
    };
    let ids = writer
        .node_ids()
        .allocate_batch(u64::try_from(entity_count).expect("text DROP smoke row count fits u64"))
        .await
        .expect("text DROP smoke IDs are durably reserved");
    assert_eq!(
        ids,
        0..u64::try_from(entity_count).expect("text DROP smoke row count fits u64")
    );
    seed_scope(
        writer.db(),
        DataScope::LegacyUnscoped,
        0,
        entity_count,
        unscoped_properties,
    )
    .await;

    let key = catalog::ScopedPropertyKey::try_new(LABEL, TEXT_PROPERTY)
        .expect("text DROP smoke key is valid");
    build(
        &db,
        DataScope::LegacyUnscoped,
        measurement,
        ir::IndexDdlCreateSpec::NodeText {
            key: key.clone(),
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .await;
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan("uniqueneedle"),
                context::ParamBindings::default(),
            )
            .await
            .expect("text DROP smoke search succeeds"),
        ),
        vec![0]
    );
    drop_index(
        &db,
        DataScope::LegacyUnscoped,
        ir::IndexDdlDropSpec::NodeText { key },
    )
    .await;
    assert_no_lifecycle_residue(&db, &[DataScope::LegacyUnscoped]).await;
    db.close()
        .await
        .expect("text DROP smoke writer closes cleanly");
}

/// Reproduces text publication-to-DROP handoff with one compact split.
pub(super) async fn run_text_drop_smoke() {
    run_text_drop_fixture(
        "index-v2-production-text-drop-smoke",
        "text_drop_smoke",
        100,
    )
    .await;
}

/// Reproduces text publication-to-DROP handoff after multi-split compaction.
pub(super) async fn run_text_drop_multi_split_smoke() {
    run_text_drop_fixture(
        "index-v2-production-text-drop-multi-split-smoke",
        "text_drop_multi_split_smoke",
        10_000,
    )
    .await;
}

/// Runs the isolated 100k 128D f32 vector scale and brute-force oracle.
pub(super) async fn run_vector() {
    assert_eq!(
        VECTOR_DIMENSION, 128,
        "vector release shape must remain fixed"
    );
    let db = open_seeded_unscoped("index-v2-production-vector-scale").await;
    let property_key = catalog::ScopedPropertyKey::try_new(LABEL, VECTOR_PROPERTY)
        .expect("scale vector property key is valid");

    build(
        &db,
        DataScope::LegacyUnscoped,
        "vector_f32_128",
        ir::IndexDdlCreateSpec::NodeVector {
            key: property_key.clone(),
            dimension: ir::VectorIndexDimension::new(VECTOR_DIMENSION)
                .expect("scale vector dimension is positive"),
            metric: ir::VectorIndexMetric::Euclidean,
            scope: catalog::SearchIndexScope::Unscoped,
        },
    )
    .await;
    let query = vector(0);
    let brute_force = (0..u64::try_from(ENTITY_COUNT).expect("scale entity count fits u64"))
        .min_by(|left, right| {
            let left_distance = vector(*left)
                .iter()
                .zip(&query)
                .map(|(candidate, query)| (candidate - query).powi(2))
                .sum::<f32>();
            let right_distance = vector(*right)
                .iter()
                .zip(&query)
                .map(|(candidate, query)| (candidate - query).powi(2))
                .sum::<f32>();
            left_distance.total_cmp(&right_distance)
        })
        .expect("scale oracle contains entities");
    assert_eq!(brute_force, 0);
    assert_eq!(
        projected_node_ids(
            db.execute(
                &vector_search_plan(query),
                context::ParamBindings::default(),
            )
            .await
            .expect("128D scale vector search succeeds"),
        ),
        vec![brute_force]
    );

    drop_index(
        &db,
        DataScope::LegacyUnscoped,
        ir::IndexDdlDropSpec::NodeVector { key: property_key },
    )
    .await;
    assert!(db
        .execute(
            &vector_search_plan(vector(0)),
            context::ParamBindings::default(),
        )
        .await
        .is_err());
    assert_no_lifecycle_residue(&db, &[DataScope::LegacyUnscoped]).await;
    db.close()
        .await
        .expect("vector scale writer closes cleanly");
}
