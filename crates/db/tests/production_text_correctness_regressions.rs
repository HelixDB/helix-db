mod text_correctness_support;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use db::config::{
    DbConfig, SearchIndexBackfillLimits, SearchIndexBatchLimits, TextAnalyzerKind,
    TextIndexDefinition, VectorIndexDefinition,
};
use db::encoding::v2::keys::scope::DataScope;
use db::execution::interpreter::{ElementRef, ExecutionResult, ExecutionRow, ExecutionValue};
use db::index_lifecycle::{
    IndexDdlReceipt, IndexOperationId, IndexOperationStage, IndexOperationStatus,
    ValidatedDynamicIndexDefinition,
};
use db::index_lifecycle_testing::{
    arm_text_search_page_barrier, LifecycleTestController, LifecycleTestScheduling,
    LifecycleWorkTarget, TextManifestValidationLane,
};
use db::search::vector::VectorDistanceMetric;
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::batch;
use helix_ast::graph::NodeRef;
use helix_ast::projection::BindingProjection;
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use helix_ast::value::{PropertyInput, PropertyValue};
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};
use slatedb::object_store::path::Path;
use slatedb::object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutPayload};
use text_correctness_support::{
    analyze_text, search_live_corpus, BarrierObjectStore, OracleDocument, OracleHit,
    ORACLE_MAX_TOKEN_LEN,
};

const LABEL: &str = "FtsCorrectnessDocument";
const EDGE_LABEL: &str = "FTS_CORRECTNESS_LINK";
const PROPERTY: &str = "body";
const TENANT_PROPERTY: &str = "tenant_id";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const TRANSACTION_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
static PAGE_BARRIER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn independent_oracle_analyzers_have_golden_terms() {
    assert_eq!(
        analyze_text(TextAnalyzerKind::Standard, "Running QUICKLY, 2026-07-24"),
        ["running", "quickly", "2026", "07", "24"]
    );
    assert_eq!(
        analyze_text(
            TextAnalyzerKind::StandardStemEn,
            "Running runners jumped quickly"
        ),
        ["run", "runner", "jump", "quick"]
    );
    assert_eq!(
        analyze_text(TextAnalyzerKind::WhitespaceLowercase, "Alpha-Beta GAMMA"),
        ["alpha-beta", "gamma"]
    );
}

#[test]
fn independent_oracle_is_deterministic_and_deduplicates_query_terms() {
    let documents = [
        OracleDocument {
            entity_id: 1,
            text: "alpha alpha beta",
        },
        OracleDocument {
            entity_id: 2,
            text: "alpha gamma",
        },
        OracleDocument {
            entity_id: 3,
            text: "gamma",
        },
    ];
    let first = search_live_corpus(TextAnalyzerKind::Standard, &documents, "alpha alpha", 10);
    let second = search_live_corpus(TextAnalyzerKind::Standard, &documents, "alpha", 10);
    assert_eq!(first, second);
    assert_eq!(
        first.iter().map(|hit| hit.entity_id).collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(first
        .iter()
        .all(|hit| f32::from_bits(hit.score_bits).is_finite()));
}

#[test]
fn independent_oracle_has_stable_ties_and_tantivy_token_length_boundaries() {
    let tied = [
        OracleDocument {
            entity_id: 30,
            text: "same score",
        },
        OracleDocument {
            entity_id: 10,
            text: "same score",
        },
        OracleDocument {
            entity_id: 20,
            text: "same score",
        },
    ];
    assert_eq!(
        search_live_corpus(TextAnalyzerKind::Standard, &tied, "same", 2)
            .into_iter()
            .map(|hit| hit.entity_id)
            .collect::<Vec<_>>(),
        [10, 20]
    );

    let maximum = "a".repeat(ORACLE_MAX_TOKEN_LEN);
    let oversized = "b".repeat(ORACLE_MAX_TOKEN_LEN + 1);
    assert_eq!(
        analyze_text(TextAnalyzerKind::WhitespaceLowercase, &maximum),
        std::slice::from_ref(&maximum)
    );
    assert!(analyze_text(TextAnalyzerKind::WhitespaceLowercase, &oversized).is_empty());
    let boundary_documents = [
        OracleDocument {
            entity_id: 1,
            text: &maximum,
        },
        OracleDocument {
            entity_id: 2,
            text: &oversized,
        },
    ];
    assert_eq!(
        search_live_corpus(
            TextAnalyzerKind::WhitespaceLowercase,
            &boundary_documents,
            &maximum,
            10,
        )
        .into_iter()
        .map(|hit| hit.entity_id)
        .collect::<Vec<_>>(),
        [1]
    );
    assert!(search_live_corpus(
        TextAnalyzerKind::WhitespaceLowercase,
        &boundary_documents,
        &oversized,
        10,
    )
    .is_empty());
}

#[tokio::test]
async fn object_store_barrier_blocks_only_the_armed_read_and_records_deletion() {
    let store = Arc::new(BarrierObjectStore::default());
    let blocked_path = Path::from("fts/blobs/blocked");
    let other_path = Path::from("fts/blobs/other");
    store
        .put(&blocked_path, PutPayload::from_static(b"blocked"))
        .await
        .expect("blocked fixture uploads");
    store
        .put(&other_path, PutPayload::from_static(b"other"))
        .await
        .expect("control fixture uploads");
    store.arm_read(blocked_path.clone());

    let blocked_store = Arc::clone(&store);
    let blocked_path_for_task = blocked_path.clone();
    let blocked = tokio::spawn(async move {
        blocked_store
            .get_opts(&blocked_path_for_task, GetOptions::default())
            .await
    });
    store.wait_until_read_is_blocked().await;
    assert!(!blocked.is_finished());
    store
        .get(&other_path)
        .await
        .expect("unarmed reads remain available");

    store.release_read();
    blocked
        .await
        .expect("blocked task joins")
        .expect("released read succeeds");
    store.delete(&blocked_path).await.expect("fixture deletes");
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(
        store.deleted_paths().contains(&blocked_path),
        "the wrapper records deletion of the armed object"
    );
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("fixture identifier is non-empty")
}

fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("fixture step ID is positive"),
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

fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("fixture plan is non-empty"),
        exec::ExecStepId::new(root).expect("fixture root ID is positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("fixture dependencies form a valid executable plan")
}

fn assignments(items: Vec<(&str, PropertyValue)>) -> ir::PropertyAssignments {
    ir::PropertyAssignments::try_from_vec(
        items
            .into_iter()
            .map(|(property, value)| (name(property), ir::PropertyInputPlan::Value(value)))
            .collect(),
    )
    .expect("fixture property names are unique")
}

fn add_node_plan(label: &str, properties: Vec<(&str, PropertyValue)>) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(label),
                    properties: assignments(properties),
                },
            },
        )],
        1,
    )
}

fn add_edge_plan(
    from_param: ir::NonEmptyString,
    to: u64,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access ID is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: from_param },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: name(label),
                        to: ir::NodeTargetPlan::PointIds {
                            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(to))
                                .expect("fixture edge target is non-empty"),
                        },
                        properties: assignments(properties),
                    },
                },
            ),
        ],
        2,
    )
}

fn node_mutation_plan(
    parameter: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access ID is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: parameter },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation { plan: mutation },
            ),
        ],
        2,
    )
}

fn edge_mutation_plan(
    parameter: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access ID is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::FromParam { param: parameter },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation { plan: mutation },
            ),
        ],
        2,
    )
}

fn text_search_plan(label: &str, property: &str, query: &str) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Read,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::TextSearch {
                        key: catalog::NodeSearchIndexKey::try_new(label, property)
                            .expect("fixture text search key is valid"),
                        index: ir::SearchIndexPlan {
                            index_id: name(&db::search::text_index_name(
                                db::config::TextElementType::Node,
                                label,
                                property,
                            )),
                            tenant: ir::SearchTenantPlan::Unscoped,
                        },
                        query_text: ir::TextQueryInputPlan::Text(name(query)),
                        k: ir::SearchLimitPlan::Literal(
                            NonZeroUsize::new(32).expect("fixture limit is positive"),
                        ),
                    },
                )),
            },
        )],
        1,
    )
}

fn created_node_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("node insertion returns a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("node insertion returns one current node");
    };
    *id
}

fn created_edge_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("edge insertion returns a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Edge(id)),
        ..
    }) = rows.first()
    else {
        panic!("edge insertion returns one current edge");
    };
    *id
}

fn receipt_operation_id(receipt: IndexDdlReceipt) -> IndexOperationId {
    match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => {
            panic!("fresh fixture definition is not already active")
        }
    }
}

async fn wait_for_terminal(db: &HelixDB, operation_id: IndexOperationId) -> IndexOperationStatus {
    let started = Instant::now();
    loop {
        let status = db
            .get_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("fixture operation remains readable");
        if matches!(
            status,
            IndexOperationStatus::Succeeded { .. }
                | IndexOperationStatus::Blocked { .. }
                | IndexOperationStatus::Aborted { .. }
        ) {
            return status;
        }
        assert!(
            started.elapsed() < OPERATION_TIMEOUT,
            "fixture operation did not terminate: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn activate_text_definition(
    db: &HelixDB,
    controller: &LifecycleTestController,
    definition: ValidatedDynamicIndexDefinition,
) {
    let operation_id = receipt_operation_id(
        controller
            .create_index(
                db,
                DataScope::LegacyUnscoped,
                definition,
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("fixture text CREATE is accepted"),
    );
    assert!(
        matches!(
            wait_for_terminal(db, operation_id).await,
            IndexOperationStatus::Succeeded { .. }
        ),
        "fixture text CREATE reaches Active"
    );
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("Active text definition refreshes into the planner catalog");
}

async fn execute_with_transaction_retry(
    db: &HelixDB,
    plan: &exec::ExecutablePlan,
    bindings: context::ParamBindings,
    expectation: &str,
) -> ExecutionResult {
    let retry_deadline = Instant::now() + TRANSACTION_RETRY_TIMEOUT;
    loop {
        match db.execute(plan, bindings.clone()).await {
            Ok(result) => break result,
            Err(error) if error.is_transaction_conflict() && Instant::now() < retry_deadline => {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("{expectation}: {error}"),
        }
    }
}

async fn insert_node(db: &HelixDB, text: &str) -> u64 {
    created_node_id(
        execute_with_transaction_retry(
            db,
            &add_node_plan(LABEL, vec![(PROPERTY, PropertyValue::from(text))]),
            context::ParamBindings::default(),
            "fixture node insertion commits",
        )
        .await,
    )
}

async fn insert_edge(db: &HelixDB, from: u64, to: u64, text: &str) -> u64 {
    let parameter = name("fragmented_edge_from");
    created_edge_id(
        execute_with_transaction_retry(
            db,
            &add_edge_plan(
                parameter.clone(),
                to,
                EDGE_LABEL,
                vec![(PROPERTY, PropertyValue::from(text))],
            ),
            context::ParamBindings::default().with_value(
                parameter,
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
            "fixture edge insertion commits",
        )
        .await,
    )
}

fn internal_text_hits(
    searches: Vec<db::migration_parity::MigrationParityTextSearch>,
) -> Vec<OracleHit> {
    let [search] = searches.as_slice() else {
        panic!(
            "fixture contains exactly one active text index, observed {}",
            searches.len()
        );
    };
    search
        .hits
        .iter()
        .map(|hit| OracleHit {
            entity_id: hit.entity_id,
            score_bits: hit.score_bits,
        })
        .collect()
}

fn query_node_ids(db_response: &serde_json::Value, variable: &str) -> Vec<u64> {
    db_response[variable]
        .as_array()
        .expect("fixture ID projection returns an array")
        .iter()
        .map(|value| value.as_u64().expect("fixture ID is unsigned"))
        .collect()
}

async fn scoped_node_ids(db: &HelixDB, tenant: &str, query: &str) -> Vec<u64> {
    scoped_node_ids_value(db, PropertyValue::from(tenant), query).await
}

async fn scoped_node_ids_value(db: &HelixDB, tenant: PropertyValue, query: &str) -> Vec<u64> {
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_nodes(LABEL, PROPERTY, query, 32, Some(tenant))
                        .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .expect("tenant node search succeeds");
    query_node_ids(&response, "ids")
}

async fn scoped_edge_ids(db: &HelixDB, tenant: &str, query: &str) -> Vec<u64> {
    scoped_edge_ids_value(db, PropertyValue::from(tenant), query).await
}

async fn scoped_edge_ids_value(db: &HelixDB, tenant: PropertyValue, query: &str) -> Vec<u64> {
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_edges(EDGE_LABEL, PROPERTY, query, 32, Some(tenant))
                        .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .expect("tenant edge search succeeds");
    query_node_ids(&response, "ids")
}

fn unscoped_node_text_ids_request(query: &str) -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as(
                "ids",
                traversal::g()
                    .text_search_nodes(LABEL, PROPERTY, query, 32, None)
                    .id(),
            )
            .returning(["ids"]),
    )
}

async fn open_current_v2_text_fixture(
    database: &str,
    text: Option<&str>,
) -> (HelixDB, Option<u64>, TextIndexDefinition) {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: database.to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("current V2 text fixture opens");
    let entity_id = match text {
        Some(text) => Some(insert_node(&db, text).await),
        None => None,
    };
    let definition =
        TextIndexDefinition::new_node(LABEL, PROPERTY).expect("current V2 definition validates");
    activate_text_definition(
        &db,
        &LifecycleTestController::new(),
        definition
            .clone()
            .try_into()
            .expect("current V2 definition converts"),
    )
    .await;
    assert_eq!(
        db.migration_parity_v2_state()
            .await
            .expect("current V2 state reads")
            .storage_version,
        Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION),
        "the fixture must use the current storage format"
    );
    (db, entity_id, definition)
}

async fn open_current_v2_edge_fixture(
    database: &str,
    text: Option<&str>,
) -> (HelixDB, u64, u64, Option<u64>, TextIndexDefinition) {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: database.to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("current V2 edge fixture opens");
    let from = created_node_id(
        db.execute(
            &add_node_plan("FtsCurrentV2Endpoint", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("first current V2 edge endpoint commits"),
    );
    let to = created_node_id(
        db.execute(
            &add_node_plan("FtsCurrentV2Endpoint", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("second current V2 edge endpoint commits"),
    );
    let entity_id = match text {
        Some(text) => Some(insert_edge(&db, from, to, text).await),
        None => None,
    };
    let definition =
        TextIndexDefinition::new_edge(EDGE_LABEL, PROPERTY).expect("edge definition validates");
    activate_text_definition(
        &db,
        &LifecycleTestController::new(),
        definition
            .clone()
            .try_into()
            .expect("current V2 edge definition converts"),
    )
    .await;
    assert_eq!(
        db.migration_parity_v2_state()
            .await
            .expect("current V2 edge state reads")
            .storage_version,
        Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION),
        "the edge fixture must use the current storage format"
    );
    (db, from, to, entity_id, definition)
}

async fn open_active_tenant_fixture(database: &str) -> (HelixDB, u64, u64) {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: database.to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("tenant text fixture opens");
    let from = created_node_id(
        db.execute(
            &add_node_plan("Endpoint", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("first endpoint commits"),
    );
    let to = created_node_id(
        db.execute(
            &add_node_plan("Endpoint", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("second endpoint commits"),
    );
    let controller = LifecycleTestController::new();
    for definition in [
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("tenant node definition validates")
            .with_tenant_property(TENANT_PROPERTY)
            .expect("tenant node property validates")
            .try_into()
            .expect("tenant node definition converts"),
        TextIndexDefinition::new_edge(EDGE_LABEL, PROPERTY)
            .expect("tenant edge definition validates")
            .with_tenant_property(TENANT_PROPERTY)
            .expect("tenant edge property validates")
            .try_into()
            .expect("tenant edge definition converts"),
    ] {
        activate_text_definition(&db, &controller, definition).await;
    }
    (db, from, to)
}

async fn rollback_snapshot(db: &HelixDB) -> db::migration_parity::MigrationParitySnapshot {
    let mut snapshot = db
        .migration_parity_snapshot()
        .await
        .expect("rollback evidence remains readable");
    snapshot.allocator_watermarks.clear();
    snapshot
}

async fn bm25_mismatch(
    db: &HelixDB,
    analyzer: TextAnalyzerKind,
    documents: &BTreeMap<u64, String>,
    query: &str,
    k: usize,
) -> Option<String> {
    let oracle_documents = documents
        .iter()
        .map(|(entity_id, text)| OracleDocument {
            entity_id: *entity_id,
            text,
        })
        .collect::<Vec<_>>();
    let expected = search_live_corpus(analyzer, &oracle_documents, query, k);
    let actual = internal_text_hits(
        db.migration_parity_text_search(query, k)
            .await
            .expect("internal text score observer succeeds"),
    );
    (actual != expected).then(|| {
        format!(
            "analyzer={} query={query:?} k={k}: expected {expected:?}, actual {actual:?}",
            analyzer.as_str()
        )
    })
}

async fn tenant_bm25_mismatch(
    db: &HelixDB,
    analyzer: TextAnalyzerKind,
    tenant: &str,
    documents: &BTreeMap<u64, String>,
    query: &str,
    k: usize,
) -> Option<String> {
    let oracle_documents = documents
        .iter()
        .map(|(entity_id, text)| OracleDocument {
            entity_id: *entity_id,
            text,
        })
        .collect::<Vec<_>>();
    let expected = search_live_corpus(analyzer, &oracle_documents, query, k);
    let actual = internal_text_hits(
        db.migration_parity_text_search_tenant(tenant, query, k)
            .await
            .expect("tenant text score observer succeeds"),
    );
    (actual != expected).then(|| {
        format!(
            "tenant={tenant:?} analyzer={} query={query:?} k={k}: expected {expected:?}, actual {actual:?}",
            analyzer.as_str()
        )
    })
}

fn fragmented_bm25_config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let one_document_batches = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        batch.max_input_bytes(),
        batch.max_output_operations(),
        batch.max_output_bytes(),
        batch.max_single_vector_output_bytes(),
    )
    .expect("one-document text batches preserve all byte budgets");
    DbConfig::new().with_search_index_backfill_limits(
        SearchIndexBackfillLimits::try_new(
            one_document_batches,
            defaults.edge_property_read_batch(),
            defaults.text_artifacts(),
            defaults.text_compaction(),
        )
        .expect("fragmented text limits preserve cross-budget invariants"),
    )
}

#[tokio::test]
async fn monolithic_bm25_matches_independent_live_corpus_control() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "fts-monolithic-bm25-control".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("monolithic BM25 control opens");
    let controller = LifecycleTestController::new();
    let mut documents = BTreeMap::new();
    for text in [
        "alpha",
        "alpha filler filler filler filler filler filler filler",
        "beta",
        "",
    ] {
        let entity_id = insert_node(&db, text).await;
        documents.insert(entity_id, text.to_string());
    }
    let definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("control definition validates")
            .try_into()
            .expect("control definition converts");
    activate_text_definition(&db, &controller, definition).await;

    assert_eq!(
        bm25_mismatch(&db, TextAnalyzerKind::Standard, &documents, "alpha", 32).await,
        None
    );
    db.close().await.expect("monolithic control closes");
}

#[tokio::test]
async fn unrelated_active_text_mutations_do_not_create_statistics_markers() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "fts-active-absent-marker-amplification".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("active absent-marker fixture opens");
    let from = created_node_id(
        db.execute(
            &add_node_plan("FtsAbsentMarkerEndpoint", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("first edge endpoint commits before text activation"),
    );
    let to = created_node_id(
        db.execute(
            &add_node_plan("FtsAbsentMarkerEndpoint", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("second edge endpoint commits before text activation"),
    );
    let controller = LifecycleTestController::new();
    for definition in [
        TextIndexDefinition::new_node(LABEL, PROPERTY).expect("first node definition validates"),
        TextIndexDefinition::new_node(LABEL, "title").expect("second node definition validates"),
        TextIndexDefinition::new_edge(EDGE_LABEL, PROPERTY)
            .expect("first edge definition validates"),
        TextIndexDefinition::new_edge(EDGE_LABEL, "title")
            .expect("second edge definition validates"),
    ] {
        activate_text_definition(
            &db,
            &controller,
            definition
                .try_into()
                .expect("absent-marker definition converts"),
        )
        .await;
    }
    let baseline = db
        .migration_parity_v2_state()
        .await
        .expect("empty Active generations expose their physical baseline");
    assert!(baseline.text_corpus_statistics.is_empty());
    assert!(baseline.text_term_statistics.is_empty());
    assert!(baseline.text_entity_statistics.is_empty());

    let wrong_label_node = created_node_id(
        db.execute(
            &add_node_plan(
                "FtsAbsentMarkerOtherNode",
                vec![
                    (PROPERTY, PropertyValue::from("wrong label body")),
                    ("title", PropertyValue::from("wrong label title")),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("wrong-label node commits"),
    );
    let missing_text_node = created_node_id(
        db.execute(
            &add_node_plan(
                LABEL,
                vec![("unrelated", PropertyValue::from("missing indexed text"))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("matching-label node without indexed text commits"),
    );
    let from_parameter = name("fts_absent_marker_edge_from");
    let wrong_label_edge = created_edge_id(
        db.execute(
            &add_edge_plan(
                from_parameter.clone(),
                to,
                "FTS_ABSENT_MARKER_OTHER_EDGE",
                vec![
                    (PROPERTY, PropertyValue::from("wrong label edge body")),
                    ("title", PropertyValue::from("wrong label edge title")),
                ],
            ),
            context::ParamBindings::default().with_value(
                from_parameter.clone(),
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
        )
        .await
        .expect("wrong-label edge commits"),
    );
    let missing_text_edge = created_edge_id(
        db.execute(
            &add_edge_plan(
                from_parameter.clone(),
                to,
                EDGE_LABEL,
                vec![("unrelated", PropertyValue::from("missing indexed text"))],
            ),
            context::ParamBindings::default().with_value(
                from_parameter,
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
        )
        .await
        .expect("matching-label edge without indexed text commits"),
    );

    for (ordinal, entity_id) in [wrong_label_node, missing_text_node]
        .into_iter()
        .enumerate()
    {
        let parameter = name(&format!("fts_absent_marker_node_{ordinal}"));
        execute_with_transaction_retry(
            &db,
            &node_mutation_plan(
                parameter.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name("unrelated"),
                    value: ir::PropertyInputPlan::Value(PropertyValue::I64(
                        i64::try_from(ordinal).expect("fixture ordinal fits i64"),
                    )),
                },
            ),
            context::ParamBindings::default().with_value(
                parameter,
                PropertyValue::I64(i64::try_from(entity_id).expect("node ID fits i64")),
            ),
            "unrelated node replacement commits",
        )
        .await;
    }
    for (ordinal, entity_id) in [wrong_label_edge, missing_text_edge]
        .into_iter()
        .enumerate()
    {
        let parameter = name(&format!("fts_absent_marker_edge_{ordinal}"));
        db.execute(
            &edge_mutation_plan(
                parameter.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name("unrelated"),
                    value: ir::PropertyInputPlan::Value(PropertyValue::I64(
                        i64::try_from(ordinal).expect("fixture ordinal fits i64"),
                    )),
                },
            ),
            context::ParamBindings::default().with_value(
                parameter,
                PropertyValue::I64(i64::try_from(entity_id).expect("edge ID fits i64")),
            ),
        )
        .await
        .expect("unrelated edge replacement commits");
    }
    assert_eq!(
        db.migration_parity_v2_state()
            .await
            .expect("post-mutation V2 evidence reads"),
        baseline,
        "unrelated graph mutations must produce no Index V2 artifacts"
    );

    let matching_node = created_node_id(
        db.execute(
            &add_node_plan(
                LABEL,
                vec![
                    (PROPERTY, PropertyValue::from("active marker control body")),
                    ("title", PropertyValue::from("active marker control title")),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("matching node control commits"),
    );
    let matching_edge_parameter = name("fts_absent_marker_matching_edge_from");
    let matching_edge = created_edge_id(
        db.execute(
            &add_edge_plan(
                matching_edge_parameter.clone(),
                to,
                EDGE_LABEL,
                vec![
                    (PROPERTY, PropertyValue::from("active marker control body")),
                    ("title", PropertyValue::from("active marker control title")),
                ],
            ),
            context::ParamBindings::default().with_value(
                matching_edge_parameter,
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
        )
        .await
        .expect("matching edge control commits"),
    );
    let populated = db
        .migration_parity_v2_state()
        .await
        .expect("populated Active generations expose their statistics");
    assert_eq!(populated.text_corpus_statistics.len(), 4);
    assert!(!populated.text_term_statistics.is_empty());
    assert_eq!(populated.text_entity_statistics.len(), 4);
    assert!(populated.text_entity_statistics.iter().all(|statistics| {
        [matching_node, matching_edge].contains(&statistics.entity_id)
            && matches!(
                statistics.contribution,
                db::migration_parity::MigrationParityTextEntityContribution::Present { .. }
            )
    }));

    let node_response = db
        .query(unscoped_node_text_ids_request("active marker control"))
        .await
        .expect("matching node control remains searchable");
    assert_eq!(query_node_ids(&node_response, "ids"), [matching_node]);
    let edge_response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_edges(EDGE_LABEL, PROPERTY, "active marker control", 32, None)
                        .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .expect("matching edge control remains searchable");
    assert_eq!(query_node_ids(&edge_response, "ids"), [matching_edge]);
    db.close()
        .await
        .expect("active absent-marker fixture closes");
}

#[tokio::test]
async fn obsolete_v2_nonempty_text_state_without_statistics_fails_closed() {
    let (empty, None, _) = open_current_v2_text_fixture("fts-current-v2-empty-control", None).await
    else {
        panic!("empty V2 fixture has no graph entity")
    };
    let empty_state = empty
        .migration_parity_v2_state()
        .await
        .expect("empty V2 evidence reads");
    assert!(empty_state.text_corpus_statistics.is_empty());
    assert!(empty_state.text_term_statistics.is_empty());
    assert!(empty_state.text_entity_statistics.is_empty());
    let empty_response = empty
        .query(unscoped_node_text_ids_request("v2statneedle"))
        .await
        .expect("a genuinely empty V2 text root remains searchable");
    assert!(query_node_ids(&empty_response, "ids").is_empty());
    let first_entity = insert_node(&empty, "firstactiveinsert").await;
    let first_insert_state = empty
        .migration_parity_v2_state()
        .await
        .expect("first Active insert statistics read");
    assert_eq!(
        first_insert_state
            .text_corpus_statistics
            .iter()
            .map(|statistics| statistics.document_count)
            .collect::<Vec<_>>(),
        [1],
        "the first document in a canonical empty root creates its corpus"
    );
    assert_eq!(first_insert_state.text_entity_statistics.len(), 1);
    let first_insert_response = empty
        .query(unscoped_node_text_ids_request("firstactiveinsert"))
        .await
        .expect("the first Active insert remains searchable");
    assert_eq!(
        query_node_ids(&first_insert_response, "ids"),
        [first_entity]
    );
    empty.close().await.expect("empty V2 control closes");

    let (missing_corpus, Some(corpus_entity), corpus_definition) = open_current_v2_text_fixture(
        "fts-obsolete-v2-missing-corpus",
        Some("v2statneedle companion"),
    )
    .await
    else {
        panic!("populated V2 fixture returns its graph entity")
    };
    let complete_state = missing_corpus
        .migration_parity_v2_state()
        .await
        .expect("complete populated V2 evidence reads");
    assert_eq!(
        complete_state.storage_version,
        Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION)
    );
    assert_eq!(complete_state.text_corpus_statistics.len(), 1);
    assert!(!complete_state.text_term_statistics.is_empty());
    assert!(complete_state
        .text_entity_statistics
        .iter()
        .any(|statistics| statistics.entity_id == corpus_entity));
    let complete_response = missing_corpus
        .query(unscoped_node_text_ids_request("v2statneedle"))
        .await
        .expect("complete populated V2 generation remains searchable");
    assert_eq!(query_node_ids(&complete_response, "ids"), [corpus_entity]);

    missing_corpus
        .migration_parity_damage_text_statistics(
            &corpus_definition,
            db::migration_parity::MigrationParityTextStatisticsDamage::MissingCorpus {
                tenant: None,
            },
        )
        .await
        .expect("corpus-row damage is confined to the feature-gated fixture");
    let damaged_corpus_state = missing_corpus
        .migration_parity_v2_state()
        .await
        .expect("damaged corpus evidence reads");
    assert!(damaged_corpus_state.text_corpus_statistics.is_empty());
    assert!(!damaged_corpus_state.text_term_statistics.is_empty());
    assert!(damaged_corpus_state
        .text_entity_statistics
        .iter()
        .any(|statistics| statistics.entity_id == corpus_entity));
    let corpus_error = missing_corpus
        .query(unscoped_node_text_ids_request("v2statneedle"))
        .await
        .expect_err("non-empty marker-2 root without corpus statistics fails closed");
    let db::error::HelixDbError::IndexCatalogCorruption(corpus_reason) = corpus_error else {
        panic!("missing corpus returned the wrong error: {corpus_error}")
    };
    assert!(
        corpus_reason.contains("no corpus statistics"),
        "missing corpus reports its exact corruption category: {corpus_reason}"
    );
    missing_corpus
        .close()
        .await
        .expect("missing-corpus fixture closes");

    let (missing_marker, Some(marker_entity), marker_definition) = open_current_v2_text_fixture(
        "fts-obsolete-v2-missing-entity-marker",
        Some("v2statneedle companion"),
    )
    .await
    else {
        panic!("populated marker fixture returns its graph entity")
    };
    let marker_control = missing_marker
        .query(unscoped_node_text_ids_request("v2statneedle"))
        .await
        .expect("complete entity-marker control remains searchable");
    assert_eq!(query_node_ids(&marker_control, "ids"), [marker_entity]);
    missing_marker
        .migration_parity_damage_text_statistics(
            &marker_definition,
            db::migration_parity::MigrationParityTextStatisticsDamage::MissingEntityMarker {
                entity_id: marker_entity,
            },
        )
        .await
        .expect("entity-marker damage is confined to the feature-gated fixture");
    let damaged_marker_state = missing_marker
        .migration_parity_v2_state()
        .await
        .expect("damaged marker evidence reads");
    assert_eq!(damaged_marker_state.text_corpus_statistics.len(), 1);
    assert!(!damaged_marker_state.text_term_statistics.is_empty());
    assert!(damaged_marker_state
        .text_entity_statistics
        .iter()
        .all(|statistics| statistics.entity_id != marker_entity));
    let marker_error = missing_marker
        .query(unscoped_node_text_ids_request("v2statneedle"))
        .await
        .expect_err("live marker-2 hit without an entity marker fails closed");
    let db::error::HelixDbError::IndexCatalogCorruption(marker_reason) = marker_error else {
        panic!("missing entity marker returned the wrong error: {marker_error}")
    };
    assert!(
        marker_reason.contains("no statistics marker"),
        "missing entity marker reports its exact corruption category: {marker_reason}"
    );
    missing_marker
        .close()
        .await
        .expect("missing-marker fixture closes");
}

#[tokio::test]
async fn active_node_insert_rejects_nonempty_root_without_corpus_statistics() {
    let (db, Some(existing), definition) = open_current_v2_text_fixture(
        "fts-active-node-missing-corpus-regression",
        Some("existingnodeonly"),
    )
    .await
    else {
        panic!("populated node fixture returns its graph entity")
    };
    let valid_append = insert_node(&db, "validnodeappendonly").await;
    let valid_documents = BTreeMap::from([
        (existing, "existingnodeonly".to_string()),
        (valid_append, "validnodeappendonly".to_string()),
    ]);
    assert_eq!(
        bm25_mismatch(
            &db,
            TextAnalyzerKind::Standard,
            &valid_documents,
            "existingnodeonly validnodeappendonly",
            32,
        )
        .await,
        None,
        "a complete populated node generation accepts a valid append"
    );

    db.migration_parity_damage_text_statistics(
        &definition,
        db::migration_parity::MigrationParityTextStatisticsDamage::MissingCorpus { tenant: None },
    )
    .await
    .expect("node corpus damage is confined to the feature-gated fixture");
    let before = rollback_snapshot(&db).await;
    let error = db
        .execute(
            &add_node_plan(
                LABEL,
                vec![(PROPERTY, PropertyValue::from("rejectednodeappendonly"))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect_err("a non-empty node root without corpus statistics must fail closed");
    let db::error::HelixDbError::IndexCatalogCorruption(reason) = error else {
        panic!("damaged node corpus returned the wrong error: {error}")
    };
    assert!(
        reason.contains("no corpus statistics"),
        "damaged node corpus reports its exact corruption category: {reason}"
    );
    assert_eq!(
        rollback_snapshot(&db).await,
        before,
        "the rejected node insert must roll back graph, manifests, statistics, and outbox rows"
    );
    db.close()
        .await
        .expect("damaged node corpus fixture closes");
}

#[tokio::test]
async fn active_edge_insert_rejects_nonempty_root_without_corpus_statistics() {
    let (db, from, to, Some(existing), definition) = open_current_v2_edge_fixture(
        "fts-active-edge-missing-corpus-regression",
        Some("existingedgeonly"),
    )
    .await
    else {
        panic!("populated edge fixture returns its graph entity")
    };
    let valid_append = insert_edge(&db, from, to, "validedgeappendonly").await;
    let valid_documents = BTreeMap::from([
        (existing, "existingedgeonly".to_string()),
        (valid_append, "validedgeappendonly".to_string()),
    ]);
    assert_eq!(
        bm25_mismatch(
            &db,
            TextAnalyzerKind::Standard,
            &valid_documents,
            "existingedgeonly validedgeappendonly",
            32,
        )
        .await,
        None,
        "a complete populated edge generation accepts a valid append"
    );

    db.migration_parity_damage_text_statistics(
        &definition,
        db::migration_parity::MigrationParityTextStatisticsDamage::MissingCorpus { tenant: None },
    )
    .await
    .expect("edge corpus damage is confined to the feature-gated fixture");
    let before = rollback_snapshot(&db).await;
    let from_parameter = name("damaged_edge_corpus_from");
    let error = db
        .execute(
            &add_edge_plan(
                from_parameter.clone(),
                to,
                EDGE_LABEL,
                vec![(PROPERTY, PropertyValue::from("rejectededgeappendonly"))],
            ),
            context::ParamBindings::default().with_value(
                from_parameter,
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
        )
        .await
        .expect_err("a non-empty edge root without corpus statistics must fail closed");
    let db::error::HelixDbError::IndexCatalogCorruption(reason) = error else {
        panic!("damaged edge corpus returned the wrong error: {error}")
    };
    assert!(
        reason.contains("no corpus statistics"),
        "damaged edge corpus reports its exact corruption category: {reason}"
    );
    assert_eq!(
        rollback_snapshot(&db).await,
        before,
        "the rejected edge insert must roll back graph, manifests, statistics, and outbox rows"
    );
    db.close()
        .await
        .expect("damaged edge corpus fixture closes");
}

#[tokio::test]
async fn fragmented_bm25_matches_monolithic_live_corpus_across_history() {
    let mut mismatches = Vec::new();
    for (ordinal, analyzer) in [
        TextAnalyzerKind::Standard,
        TextAnalyzerKind::StandardStemEn,
        TextAnalyzerKind::WhitespaceLowercase,
    ]
    .into_iter()
    .enumerate()
    {
        let db = HelixDB::open_for_index_lifecycle_testing(
            HelixDbSource::InMemory {
                database: format!("fts-fragmented-bm25-{ordinal}"),
            },
            fragmented_bm25_config(),
            LifecycleTestScheduling::Automatic,
        )
        .await
        .expect("fragmented BM25 fixture opens");
        let controller = LifecycleTestController::new();
        let mut documents = BTreeMap::new();
        for text in [
            "alpha",
            "alpha filler filler filler filler filler filler filler",
            "tie alpha",
            "tie alpha",
            "",
            "---",
        ] {
            let entity_id = insert_node(&db, text).await;
            documents.insert(entity_id, text.to_string());
        }
        let definition: ValidatedDynamicIndexDefinition =
            TextIndexDefinition::new_node(LABEL, PROPERTY)
                .expect("fragmented definition validates")
                .with_analyzer(analyzer)
                .try_into()
                .expect("fragmented definition converts");
        activate_text_definition(&db, &controller, definition.clone()).await;

        for text in [
            "alpha alpha",
            "alpha alpha alpha long long long long",
            "unrelated",
        ] {
            let entity_id = insert_node(&db, text).await;
            documents.insert(entity_id, text.to_string());
        }
        controller
            .repage_active_text_manifest_for_testing(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("fragmented active splits are distributed across manifest pages");
        for (query, k) in [
            ("alpha", 1),
            ("alpha", 32),
            ("alpha alpha", 32),
            ("tie", 2),
            ("---", 32),
            ("alpha", 0),
        ] {
            if let Some(mismatch) = bm25_mismatch(&db, analyzer, &documents, query, k).await {
                mismatches.push(format!("after active inserts: {mismatch}"));
            }
        }

        let (&updated_id, _) = documents
            .iter()
            .next()
            .expect("fragmented fixture has a document");
        let parameter = name("fragmented_update");
        execute_with_transaction_retry(
            &db,
            &node_mutation_plan(
                parameter.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name(PROPERTY),
                    value: ir::PropertyInputPlan::Value(PropertyValue::from(
                        "alpha alpha alpha alpha stalewinner",
                    )),
                },
            ),
            context::ParamBindings::default().with_value(
                parameter,
                PropertyValue::I64(
                    i64::try_from(updated_id).expect("fixture node ID fits signed input"),
                ),
            ),
            "fragmented update commits",
        )
        .await;
        documents.insert(
            updated_id,
            "alpha alpha alpha alpha stalewinner".to_string(),
        );
        if let Some(mismatch) = bm25_mismatch(&db, analyzer, &documents, "alpha", 32).await {
            mismatches.push(format!("after update: {mismatch}"));
        }

        let deleted_id = *documents
            .keys()
            .nth(1)
            .expect("fragmented fixture has a second document");
        let parameter = name("fragmented_delete");
        execute_with_transaction_retry(
            &db,
            &node_mutation_plan(parameter.clone(), exec::ExecMutationPlan::Drop),
            context::ParamBindings::default().with_value(
                parameter,
                PropertyValue::I64(
                    i64::try_from(deleted_id).expect("fixture node ID fits signed input"),
                ),
            ),
            "fragmented deletion commits",
        )
        .await;
        documents.remove(&deleted_id);
        let replacement = insert_node(&db, "alpha replacement").await;
        documents.insert(replacement, "alpha replacement".to_string());
        if let Some(mismatch) = bm25_mismatch(&db, analyzer, &documents, "alpha", 32).await {
            mismatches.push(format!("after delete and reinsert: {mismatch}"));
        }
        db.close().await.expect("fragmented fixture closes");
    }

    let edge_db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "fts-fragmented-bm25-edge".to_string(),
        },
        fragmented_bm25_config(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("fragmented edge fixture opens");
    let edge_controller = LifecycleTestController::new();
    let from = created_node_id(
        edge_db
            .execute(
                &add_node_plan("Endpoint", Vec::new()),
                context::ParamBindings::default(),
            )
            .await
            .expect("first fragmented edge endpoint commits"),
    );
    let to = created_node_id(
        edge_db
            .execute(
                &add_node_plan("Endpoint", Vec::new()),
                context::ParamBindings::default(),
            )
            .await
            .expect("second fragmented edge endpoint commits"),
    );
    let mut edge_documents = BTreeMap::new();
    for text in [
        "alpha",
        "alpha filler filler filler filler filler filler filler",
        "tie alpha",
        "tie alpha",
        "",
        "---",
    ] {
        let entity_id = insert_edge(&edge_db, from, to, text).await;
        edge_documents.insert(entity_id, text.to_string());
    }
    let edge_definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_edge(EDGE_LABEL, PROPERTY)
            .expect("fragmented edge definition validates")
            .try_into()
            .expect("fragmented edge definition converts");
    activate_text_definition(&edge_db, &edge_controller, edge_definition.clone()).await;
    for text in [
        "alpha alpha",
        "alpha alpha alpha long long long long",
        "unrelated",
    ] {
        let entity_id = insert_edge(&edge_db, from, to, text).await;
        edge_documents.insert(entity_id, text.to_string());
    }
    edge_controller
        .repage_active_text_manifest_for_testing(
            &edge_db,
            DataScope::LegacyUnscoped,
            &edge_definition,
        )
        .await
        .expect("fragmented edge splits are distributed across manifest pages");
    for (query, k) in [
        ("alpha", 1),
        ("alpha", 32),
        ("alpha alpha", 32),
        ("tie", 2),
        ("---", 32),
        ("alpha", 0),
    ] {
        if let Some(mismatch) = bm25_mismatch(
            &edge_db,
            TextAnalyzerKind::Standard,
            &edge_documents,
            query,
            k,
        )
        .await
        {
            mismatches.push(format!("edge after active inserts: {mismatch}"));
        }
    }
    let updated_edge = *edge_documents
        .keys()
        .next()
        .expect("fragmented edge fixture has an edge");
    let parameter = name("fragmented_edge_update");
    execute_with_transaction_retry(
        &edge_db,
        &edge_mutation_plan(
            parameter.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name(PROPERTY),
                value: ir::PropertyInputPlan::Value(PropertyValue::from(
                    "alpha alpha alpha alpha stalewinner",
                )),
            },
        ),
        context::ParamBindings::default().with_value(
            parameter,
            PropertyValue::I64(
                i64::try_from(updated_edge).expect("fixture edge ID fits signed input"),
            ),
        ),
        "fragmented edge update commits",
    )
    .await;
    edge_documents.insert(
        updated_edge,
        "alpha alpha alpha alpha stalewinner".to_string(),
    );
    if let Some(mismatch) = bm25_mismatch(
        &edge_db,
        TextAnalyzerKind::Standard,
        &edge_documents,
        "alpha",
        32,
    )
    .await
    {
        mismatches.push(format!("edge after update: {mismatch}"));
    }
    let deleted_edge = *edge_documents
        .keys()
        .nth(1)
        .expect("fragmented edge fixture has a second edge");
    let parameter = name("fragmented_edge_delete");
    execute_with_transaction_retry(
        &edge_db,
        &edge_mutation_plan(
            parameter.clone(),
            exec::ExecMutationPlan::DropEdgeByIdFromInput {
                edges: ir::EdgeTargetPlan::PointIds {
                    ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(deleted_edge))
                        .expect("fragmented edge deletion target is non-empty"),
                },
            },
        ),
        context::ParamBindings::default().with_value(
            parameter,
            PropertyValue::I64(
                i64::try_from(deleted_edge).expect("fixture edge ID fits signed input"),
            ),
        ),
        "fragmented edge deletion commits",
    )
    .await;
    edge_documents.remove(&deleted_edge);
    let replacement = insert_edge(&edge_db, from, to, "alpha replacement").await;
    edge_documents.insert(replacement, "alpha replacement".to_string());
    if let Some(mismatch) = bm25_mismatch(
        &edge_db,
        TextAnalyzerKind::Standard,
        &edge_documents,
        "alpha",
        32,
    )
    .await
    {
        mismatches.push(format!("edge after delete and reinsert: {mismatch}"));
    }
    edge_db
        .close()
        .await
        .expect("fragmented edge fixture closes");

    let tenant_db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "fts-fragmented-bm25-tenant-move".to_string(),
        },
        fragmented_bm25_config(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("fragmented tenant fixture opens");
    let tenant_controller = LifecycleTestController::new();
    let mut tenant_a_documents = BTreeMap::new();
    let mut tenant_b_documents = BTreeMap::new();
    for (tenant, text) in [
        ("tenant-a", "alpha"),
        (
            "tenant-a",
            "alpha filler filler filler filler filler filler filler",
        ),
        ("tenant-a", "tie alpha"),
        ("tenant-a", ""),
        ("tenant-b", "alpha alpha"),
        ("tenant-b", "unrelated"),
    ] {
        let entity_id = created_node_id(
            tenant_db
                .execute(
                    &add_node_plan(
                        LABEL,
                        vec![
                            (PROPERTY, PropertyValue::from(text)),
                            (TENANT_PROPERTY, PropertyValue::from(tenant)),
                        ],
                    ),
                    context::ParamBindings::default(),
                )
                .await
                .expect("tenant BM25 source commits"),
        );
        if tenant == "tenant-a" {
            tenant_a_documents.insert(entity_id, text.to_string());
        } else {
            tenant_b_documents.insert(entity_id, text.to_string());
        }
    }
    let tenant_definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("tenant BM25 definition validates")
            .with_tenant_property(TENANT_PROPERTY)
            .expect("tenant BM25 partition validates")
            .try_into()
            .expect("tenant BM25 definition converts");
    activate_text_definition(&tenant_db, &tenant_controller, tenant_definition).await;
    for text in ["alpha alpha alpha", "alpha replacement"] {
        let entity_id = created_node_id(
            execute_with_transaction_retry(
                &tenant_db,
                &add_node_plan(
                    LABEL,
                    vec![
                        (PROPERTY, PropertyValue::from(text)),
                        (TENANT_PROPERTY, PropertyValue::from("tenant-a")),
                    ],
                ),
                context::ParamBindings::default(),
                "active tenant BM25 source commits",
            )
            .await,
        );
        tenant_a_documents.insert(entity_id, text.to_string());
    }
    for (tenant, documents) in [
        ("tenant-a", &tenant_a_documents),
        ("tenant-b", &tenant_b_documents),
    ] {
        if let Some(mismatch) = tenant_bm25_mismatch(
            &tenant_db,
            TextAnalyzerKind::Standard,
            tenant,
            documents,
            "alpha",
            32,
        )
        .await
        {
            mismatches.push(format!("before tenant move: {mismatch}"));
        }
    }
    let moved = *tenant_a_documents
        .iter()
        .find(|(_, text)| text.as_str() == "alpha alpha alpha")
        .map(|(entity_id, _)| entity_id)
        .expect("tenant fixture contains its active high scorer");
    let moved_text = tenant_a_documents
        .remove(&moved)
        .expect("tenant move source is tracked");
    let parameter = name("fragmented_tenant_move");
    execute_with_transaction_retry(
        &tenant_db,
        &node_mutation_plan(
            parameter.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name(TENANT_PROPERTY),
                value: ir::PropertyInputPlan::Value(PropertyValue::from("tenant-b")),
            },
        ),
        context::ParamBindings::default().with_value(
            parameter,
            PropertyValue::I64(
                i64::try_from(moved).expect("tenant fixture node ID fits signed input"),
            ),
        ),
        "fragmented tenant move commits",
    )
    .await;
    tenant_b_documents.insert(moved, moved_text);
    for (tenant, documents) in [
        ("tenant-a", &tenant_a_documents),
        ("tenant-b", &tenant_b_documents),
    ] {
        if let Some(mismatch) = tenant_bm25_mismatch(
            &tenant_db,
            TextAnalyzerKind::Standard,
            tenant,
            documents,
            "alpha",
            32,
        )
        .await
        {
            mismatches.push(format!("after tenant move: {mismatch}"));
        }
    }
    tenant_db
        .close()
        .await
        .expect("fragmented tenant fixture closes");

    assert!(
        mismatches.is_empty(),
        "split-local BM25 diverged from one live-corpus oracle:\n{}",
        mismatches.join("\n")
    );
}

#[tokio::test]
async fn vector_distance_virtual_property_materializes_and_serializes_control() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "fts-public-score-vector-control".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("vector virtual-property control opens");
    let controller = LifecycleTestController::new();
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "document",
                traversal::g().add_n(
                    LABEL,
                    vec![("embedding", PropertyInput::from(vec![1.0_f32, 0.0]))],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("vector control source commits");
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node(LABEL, "embedding", 2, VectorDistanceMetric::Euclidean)
            .expect("vector control definition validates")
            .try_into()
            .expect("vector control definition converts");
    activate_text_definition(&db, &controller, definition).await;

    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "matches",
                    traversal::g()
                        .vector_search_nodes(LABEL, "embedding", vec![1.0_f32, 0.0], 1, None)
                        .bind("match")
                        .project_bindings(vec![
                            BindingProjection::current("$distance", "current_distance"),
                            BindingProjection::binding("match", "$distance", "bound_distance"),
                            BindingProjection::current("$score", "score"),
                        ]),
                )
                .returning(["matches"]),
        ))
        .await
        .expect("vector control query serializes");
    assert_eq!(
        response,
        serde_json::json!({
            "matches": [{ "current_distance": 0.0, "bound_distance": 0.0 }],
        })
    );
    db.close().await.expect("vector control closes");
}

#[tokio::test]
async fn public_text_rows_expose_raw_score_for_nodes_edges_and_bindings() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "fts-public-text-score-regression".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("public text score fixture opens");
    let controller = LifecycleTestController::new();
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n(
                    LABEL,
                    vec![
                        (PROPERTY, PropertyInput::from("alpha alpha")),
                        ("$score", PropertyInput::from(999.0_f64)),
                    ],
                ),
            )
            .var_as(
                "second",
                traversal::g().add_n(
                    LABEL,
                    vec![(PROPERTY, PropertyInput::from("alpha filler filler"))],
                ),
            )
            .var_as(
                "edge",
                traversal::g().n(NodeRef::id(0)).add_e(
                    EDGE_LABEL,
                    NodeRef::id(1),
                    vec![
                        (PROPERTY, PropertyInput::from("alpha alpha")),
                        ("$score", PropertyInput::from(777.0_f64)),
                    ],
                ),
            )
            .var_as(
                "reverse_edge",
                traversal::g().n(NodeRef::id(1)).add_e(
                    EDGE_LABEL,
                    NodeRef::id(0),
                    vec![(PROPERTY, PropertyInput::from("alpha filler filler"))],
                ),
            )
            .returning(["first", "second", "edge", "reverse_edge"]),
    ))
    .await
    .expect("public score source corpus commits");

    for definition in [
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("node score definition validates")
            .try_into()
            .expect("node score definition converts"),
        TextIndexDefinition::new_edge(EDGE_LABEL, PROPERTY)
            .expect("edge score definition validates")
            .try_into()
            .expect("edge score definition converts"),
    ] {
        activate_text_definition(&db, &controller, definition).await;
    }

    let expected = search_live_corpus(
        TextAnalyzerKind::Standard,
        &[
            OracleDocument {
                entity_id: 0,
                text: "alpha alpha",
            },
            OracleDocument {
                entity_id: 1,
                text: "alpha filler filler",
            },
        ],
        "alpha",
        10,
    )
    .into_iter()
    .map(|hit| {
        serde_json::json!({
            "id": i64::try_from(hit.entity_id).expect("fixture ID fits i64"),
            "score": f64::from(f32::from_bits(hit.score_bits)),
            "bound_score": f64::from(f32::from_bits(hit.score_bits)),
        })
    })
    .collect::<Vec<_>>();
    let node_response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "matches",
                    traversal::g()
                        .text_search_nodes(LABEL, PROPERTY, "alpha", 10, None)
                        .bind("match")
                        .project_bindings(vec![
                            BindingProjection::current("$id", "id"),
                            BindingProjection::current("$score", "score"),
                            BindingProjection::binding("match", "$score", "bound_score"),
                            BindingProjection::current("$distance", "distance"),
                        ]),
                )
                .returning(["matches"]),
        ))
        .await
        .expect("public node text result serializes");
    let edge_response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "matches",
                    traversal::g()
                        .text_search_edges(EDGE_LABEL, PROPERTY, "alpha", 10, None)
                        .bind("match")
                        .project_bindings(vec![
                            BindingProjection::current("$id", "id"),
                            BindingProjection::current("$score", "score"),
                            BindingProjection::binding("match", "$score", "bound_score"),
                            BindingProjection::current("$distance", "distance"),
                        ]),
                )
                .returning(["matches"]),
        ))
        .await
        .expect("public edge text result serializes");
    let expected = serde_json::Value::Array(expected);
    assert_eq!(node_response["matches"], expected);
    assert_eq!(edge_response["matches"], expected);

    let raw_rows = db
        .execute(
            &text_search_plan(LABEL, PROPERTY, "alpha"),
            context::ParamBindings::default(),
        )
        .await
        .expect("public text access materializes");
    let Some(ExecutionValue::Stream(rows)) = raw_rows.last else {
        panic!("public text access materializes rows");
    };
    let score = name("$score");
    let distance = name("$distance");
    assert!(
        rows.iter().all(|row| {
            format!("{:?}", row.virtual_properties.get(&score)).starts_with("Some(F64(")
                && row.virtual_properties.get(&distance).is_none()
        }),
        "text rows must carry finite raw BM25 as $score and no $distance: {rows:?}"
    );
    db.close().await.expect("public text score fixture closes");
}

#[tokio::test]
async fn valid_tenant_text_mutations_remain_searchable_control() {
    let (db, from, to) = open_active_tenant_fixture("fts-valid-tenant-control").await;
    let node = created_node_id(
        db.execute(
            &add_node_plan(
                LABEL,
                vec![
                    (PROPERTY, PropertyValue::from("tenantcontrol")),
                    (TENANT_PROPERTY, PropertyValue::from("tenant-a")),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("valid tenant node commits"),
    );
    let from_parameter = name("from");
    let edge = created_edge_id(
        db.execute(
            &add_edge_plan(
                from_parameter.clone(),
                to,
                EDGE_LABEL,
                vec![
                    (PROPERTY, PropertyValue::from("tenantcontrol")),
                    (TENANT_PROPERTY, PropertyValue::from("tenant-a")),
                ],
            ),
            context::ParamBindings::default().with_value(
                from_parameter,
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
        )
        .await
        .expect("valid tenant edge commits"),
    );
    assert_eq!(
        scoped_node_ids(&db, "tenant-a", "tenantcontrol").await,
        [node]
    );
    assert_eq!(
        scoped_edge_ids(&db, "tenant-a", "tenantcontrol").await,
        [edge]
    );

    let node_parameter = name("valid_node_move");
    db.execute(
        &node_mutation_plan(
            node_parameter.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name(TENANT_PROPERTY),
                value: ir::PropertyInputPlan::Value(PropertyValue::from("tenant-b")),
            },
        ),
        context::ParamBindings::default().with_value(
            node_parameter,
            PropertyValue::I64(i64::try_from(node).expect("node ID fits i64")),
        ),
    )
    .await
    .expect("valid node tenant move commits");
    let edge_parameter = name("valid_edge_move");
    db.execute(
        &edge_mutation_plan(
            edge_parameter.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name(TENANT_PROPERTY),
                value: ir::PropertyInputPlan::Value(PropertyValue::from("tenant-b")),
            },
        ),
        context::ParamBindings::default().with_value(
            edge_parameter,
            PropertyValue::I64(i64::try_from(edge).expect("edge ID fits i64")),
        ),
    )
    .await
    .expect("valid edge tenant move commits");
    assert!(scoped_node_ids(&db, "tenant-a", "tenantcontrol")
        .await
        .is_empty());
    assert!(scoped_edge_ids(&db, "tenant-a", "tenantcontrol")
        .await
        .is_empty());
    assert_eq!(
        scoped_node_ids(&db, "tenant-b", "tenantcontrol").await,
        [node]
    );
    assert_eq!(
        scoped_edge_ids(&db, "tenant-b", "tenantcontrol").await,
        [edge]
    );
    db.close().await.expect("valid tenant control closes");
}

#[tokio::test]
async fn every_non_null_tenant_value_remains_searchable_control() {
    let (db, from, to) = open_active_tenant_fixture("fts-polymorphic-tenant-control").await;
    let tenants = [
        ("i64", PropertyValue::I64(7)),
        ("bool", PropertyValue::Bool(true)),
        ("empty", PropertyValue::from("")),
        (
            "array",
            PropertyValue::Array(vec![PropertyValue::I64(1), PropertyValue::from("west")]),
        ),
        (
            "object",
            PropertyValue::Object(BTreeMap::from([(
                "region".to_string(),
                PropertyValue::from("west"),
            )])),
        ),
    ];
    let mut entities = Vec::new();
    for (ordinal, (case, tenant)) in tenants.iter().enumerate() {
        let node_text = format!("tenanttypenode{ordinal}");
        let node = created_node_id(
            db.execute(
                &add_node_plan(
                    LABEL,
                    vec![
                        (PROPERTY, PropertyValue::from(node_text.clone())),
                        (TENANT_PROPERTY, tenant.clone()),
                    ],
                ),
                context::ParamBindings::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("{case} node tenant must remain valid: {error}")),
        );
        let edge_text = format!("tenanttypeedge{ordinal}");
        let parameter = name(&format!("tenant_type_edge_from_{ordinal}"));
        let edge = created_edge_id(
            db.execute(
                &add_edge_plan(
                    parameter.clone(),
                    to,
                    EDGE_LABEL,
                    vec![
                        (PROPERTY, PropertyValue::from(edge_text.clone())),
                        (TENANT_PROPERTY, tenant.clone()),
                    ],
                ),
                context::ParamBindings::default().with_value(
                    parameter,
                    PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
                ),
            )
            .await
            .unwrap_or_else(|error| panic!("{case} edge tenant must remain valid: {error}")),
        );
        assert_eq!(
            scoped_node_ids_value(&db, tenant.clone(), &node_text).await,
            [node],
            "{case} node tenant is searchable"
        );
        assert_eq!(
            scoped_edge_ids_value(&db, tenant.clone(), &edge_text).await,
            [edge],
            "{case} edge tenant is searchable"
        );
        entities.push((node, edge, node_text, edge_text));
    }

    for (ordinal, (node, edge, node_text, edge_text)) in entities.into_iter().enumerate() {
        let (source_case, source) = &tenants[ordinal];
        let (target_case, target) = &tenants[(ordinal + 1) % tenants.len()];
        let node_parameter = name(&format!("tenant_type_node_move_{ordinal}"));
        db.execute(
            &node_mutation_plan(
                node_parameter.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name(TENANT_PROPERTY),
                    value: ir::PropertyInputPlan::Value(target.clone()),
                },
            ),
            context::ParamBindings::default().with_value(
                node_parameter,
                PropertyValue::I64(i64::try_from(node).expect("node ID fits i64")),
            ),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("{source_case}-to-{target_case} node move must remain valid: {error}")
        });
        let edge_parameter = name(&format!("tenant_type_edge_move_{ordinal}"));
        db.execute(
            &edge_mutation_plan(
                edge_parameter.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name(TENANT_PROPERTY),
                    value: ir::PropertyInputPlan::Value(target.clone()),
                },
            ),
            context::ParamBindings::default().with_value(
                edge_parameter,
                PropertyValue::I64(i64::try_from(edge).expect("edge ID fits i64")),
            ),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("{source_case}-to-{target_case} edge move must remain valid: {error}")
        });
        assert!(scoped_node_ids_value(&db, source.clone(), &node_text)
            .await
            .is_empty());
        assert!(scoped_edge_ids_value(&db, source.clone(), &edge_text)
            .await
            .is_empty());
        assert_eq!(
            scoped_node_ids_value(&db, target.clone(), &node_text).await,
            [node]
        );
        assert_eq!(
            scoped_edge_ids_value(&db, target.clone(), &edge_text).await,
            [edge]
        );
    }
    db.close().await.expect("polymorphic tenant control closes");
}

#[tokio::test]
async fn invalid_tenant_active_mutations_fail_closed_and_roll_back_graph_and_index() {
    let (db, from, to) = open_active_tenant_fixture("fts-invalid-tenant-regression").await;
    let mut failures = Vec::new();
    let invalid_tenants = [("null", Some(PropertyValue::Null)), ("missing", None)];

    for (case, tenant) in invalid_tenants.clone() {
        let before = rollback_snapshot(&db).await;
        let mut properties = vec![(PROPERTY, PropertyValue::from("invalidtenantnode"))];
        if let Some(tenant) = tenant {
            properties.push((TENANT_PROPERTY, tenant));
        }
        match db
            .execute(
                &add_node_plan(LABEL, properties),
                context::ParamBindings::default(),
            )
            .await
        {
            Err(error) if error.index_error_code() == Some("invalid_index_source_data") => {}
            Err(error) => failures.push(format!(
                "node insert {case} returned the wrong error: {error}"
            )),
            Ok(result) => {
                let entity_id = created_node_id(result);
                let response = db
                    .query(QueryRequest::read(
                        batch::read_batch()
                            .var_as("ids", traversal::g().n(NodeRef::id(entity_id)).id())
                            .returning(["ids"]),
                    ))
                    .await
                    .expect("invalid node rollback probe reads");
                failures.push(format!(
                    "node insert {case} committed graph row {entity_id}; rollback probe={:?}",
                    query_node_ids(&response, "ids")
                ));
            }
        }
        let after = rollback_snapshot(&db).await;
        if after != before {
            failures.push(format!(
                "node insert {case} changed graph/index/outbox state despite required rollback"
            ));
        }
    }

    for (case, tenant) in invalid_tenants {
        let before = rollback_snapshot(&db).await;
        let mut properties = vec![(PROPERTY, PropertyValue::from("invalidtenantedge"))];
        if let Some(tenant) = tenant {
            properties.push((TENANT_PROPERTY, tenant));
        }
        let from_parameter = name(&format!("invalid_edge_from_{case}"));
        match db
            .execute(
                &add_edge_plan(from_parameter.clone(), to, EDGE_LABEL, properties),
                context::ParamBindings::default().with_value(
                    from_parameter,
                    PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
                ),
            )
            .await
        {
            Err(error) if error.index_error_code() == Some("invalid_index_source_data") => {}
            Err(error) => failures.push(format!(
                "edge insert {case} returned the wrong error: {error}"
            )),
            Ok(result) => failures.push(format!(
                "edge insert {case} committed graph row {}",
                created_edge_id(result)
            )),
        }
        let after = rollback_snapshot(&db).await;
        if after != before {
            failures.push(format!(
                "edge insert {case} changed graph/index/outbox state despite required rollback"
            ));
        }
    }

    let valid_node = created_node_id(
        db.execute(
            &add_node_plan(
                LABEL,
                vec![
                    (PROPERTY, PropertyValue::from("invalidtenantupdate")),
                    (TENANT_PROPERTY, PropertyValue::from("tenant-a")),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("valid update source commits"),
    );
    let valid_edge_parameter = name("valid_edge_from");
    let valid_edge = created_edge_id(
        db.execute(
            &add_edge_plan(
                valid_edge_parameter.clone(),
                to,
                EDGE_LABEL,
                vec![
                    (PROPERTY, PropertyValue::from("invalidtenantupdate")),
                    (TENANT_PROPERTY, PropertyValue::from("tenant-a")),
                ],
            ),
            context::ParamBindings::default().with_value(
                valid_edge_parameter,
                PropertyValue::I64(i64::try_from(from).expect("endpoint ID fits i64")),
            ),
        )
        .await
        .expect("valid edge update source commits"),
    );

    for (case, mutation) in [
        (
            "node_null_move",
            exec::ExecMutationPlan::SetProperty {
                name: name(TENANT_PROPERTY),
                value: ir::PropertyInputPlan::Value(PropertyValue::Null),
            },
        ),
        (
            "node_missing_tenant_retirement",
            exec::ExecMutationPlan::RemoveProperty {
                name: name(TENANT_PROPERTY),
            },
        ),
    ] {
        let before = rollback_snapshot(&db).await;
        let parameter = name(case);
        match db
            .execute(
                &node_mutation_plan(parameter.clone(), mutation),
                context::ParamBindings::default().with_value(
                    parameter,
                    PropertyValue::I64(i64::try_from(valid_node).expect("valid node ID fits i64")),
                ),
            )
            .await
        {
            Err(error) if error.index_error_code() == Some("invalid_index_source_data") => {}
            Err(error) => failures.push(format!("{case} returned the wrong error: {error}")),
            Ok(_) => failures.push(format!("{case} committed instead of rolling back")),
        }
        let after = rollback_snapshot(&db).await;
        if after != before {
            failures.push(format!(
                "{case} changed graph/index/outbox state despite required rollback"
            ));
        }
    }

    for (case, mutation) in [
        (
            "edge_null_move",
            exec::ExecMutationPlan::SetProperty {
                name: name(TENANT_PROPERTY),
                value: ir::PropertyInputPlan::Value(PropertyValue::Null),
            },
        ),
        (
            "edge_missing_tenant_retirement",
            exec::ExecMutationPlan::RemoveProperty {
                name: name(TENANT_PROPERTY),
            },
        ),
    ] {
        let before = rollback_snapshot(&db).await;
        let edge_parameter = name(case);
        match db
            .execute(
                &edge_mutation_plan(edge_parameter.clone(), mutation),
                context::ParamBindings::default().with_value(
                    edge_parameter,
                    PropertyValue::I64(i64::try_from(valid_edge).expect("valid edge ID fits i64")),
                ),
            )
            .await
        {
            Err(error) if error.index_error_code() == Some("invalid_index_source_data") => {}
            Err(error) => failures.push(format!("{case} returned the wrong error: {error}")),
            Ok(_) => failures.push(format!("{case} committed instead of rolling back")),
        }
        let after = rollback_snapshot(&db).await;
        if after != before {
            failures.push(format!(
                "{case} changed graph/index/outbox state despite required rollback"
            ));
        }
    }

    let node_membership = scoped_node_ids(&db, "tenant-a", "invalidtenantupdate").await;
    if node_membership != [valid_node] {
        failures.push(format!(
            "rejected node mutations changed authoritative membership: {node_membership:?}"
        ));
    }
    let edge_membership = scoped_edge_ids(&db, "tenant-a", "invalidtenantupdate").await;
    if edge_membership != [valid_edge] {
        failures.push(format!(
            "rejected edge mutations changed authoritative membership: {edge_membership:?}"
        ));
    }
    assert!(
        failures.is_empty(),
        "active tenant validation diverged from build/catch-up and committed partial state:\n{}",
        failures.join("\n")
    );
    db.close().await.expect("invalid tenant fixture closes");
}

fn logical_start_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fixture clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("fixture time fits u64 milliseconds")
}

async fn drive_children_once(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_target: LifecycleWorkTarget,
    logical_now: u64,
) -> Result<(), String> {
    let page = controller
        .discover(
            db,
            NonZeroUsize::new(1_024).expect("discovery limit is positive"),
        )
        .await
        .map_err(|error| error.to_string())?;
    if !page.exhausted {
        return Err("small lifecycle fixture exceeded one discovery page".to_string());
    }
    for target in page.targets {
        if target == operation_target {
            continue;
        }
        controller
            .advance_at_unix_millis(db, target, logical_now)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn drive_until_stage(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_id: IndexOperationId,
    expected: IndexOperationStage,
) -> Result<(), String> {
    let target = LifecycleWorkTarget::Operation {
        scope: DataScope::LegacyUnscoped,
        operation_id,
    };
    let logical_start = logical_start_millis();
    for turn in 0..4_096 {
        let status = db
            .get_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .map_err(|error| error.to_string())?;
        if status.common().stage == expected {
            return Ok(());
        }
        if !matches!(
            status,
            IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
        ) {
            return Err(format!(
                "operation terminated before {expected:?}: {status:?}"
            ));
        }
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("turn fits u64")
                .saturating_mul(60_000),
        );
        controller
            .advance_at_unix_millis(db, target, logical_now)
            .await
            .map_err(|error| error.to_string())?;
        drive_children_once(db, controller, target, logical_now).await?;
    }
    Err(format!("operation did not reach {expected:?}"))
}

async fn drive_to_terminal_explicit(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_id: IndexOperationId,
) -> Result<IndexOperationStatus, String> {
    let target = LifecycleWorkTarget::Operation {
        scope: DataScope::LegacyUnscoped,
        operation_id,
    };
    let logical_start = logical_start_millis();
    for turn in 0..4_096 {
        let status = db
            .get_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .map_err(|error| error.to_string())?;
        let terminal = matches!(
            status,
            IndexOperationStatus::Succeeded { .. }
                | IndexOperationStatus::Blocked { .. }
                | IndexOperationStatus::Aborted { .. }
        );
        let logical_now = logical_start.saturating_add(
            u64::try_from(turn)
                .expect("turn fits u64")
                .saturating_mul(60_000),
        );
        if terminal {
            let page = controller
                .discover(
                    db,
                    NonZeroUsize::new(1_024).expect("discovery limit is positive"),
                )
                .await
                .map_err(|error| error.to_string())?;
            if page.targets.is_empty() {
                return Ok(status);
            }
            for discovered in page.targets {
                if discovered == target {
                    continue;
                }
                controller
                    .advance_at_unix_millis(db, discovered, logical_now)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            continue;
        }
        controller
            .advance_at_unix_millis(db, target, logical_now)
            .await
            .map_err(|error| error.to_string())?;
        drive_children_once(db, controller, target, logical_now).await?;
    }
    Err("operation exceeded the explicit lifecycle turn bound".to_string())
}

#[derive(Debug, Clone, Copy)]
enum LateDeltaPause {
    Stage(IndexOperationStage),
    StageAfterSteps {
        stage: IndexOperationStage,
        steps: usize,
    },
    Validation(TextManifestValidationLane),
}

async fn drive_to_late_delta_pause(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_id: IndexOperationId,
    pause: LateDeltaPause,
) -> Result<(), String> {
    let target = LifecycleWorkTarget::Operation {
        scope: DataScope::LegacyUnscoped,
        operation_id,
    };
    match pause {
        LateDeltaPause::Stage(stage) => {
            drive_until_stage(db, controller, operation_id, stage).await
        }
        LateDeltaPause::StageAfterSteps { stage, steps } => {
            drive_until_stage(db, controller, operation_id, stage).await?;
            let logical_start = logical_start_millis();
            for turn in 0..steps {
                let logical_now = logical_start.saturating_add(
                    u64::try_from(turn)
                        .expect("turn fits u64")
                        .saturating_mul(60_000),
                );
                controller
                    .advance_at_unix_millis(db, target, logical_now)
                    .await
                    .map_err(|error| error.to_string())?;
                drive_children_once(db, controller, target, logical_now).await?;
            }
            let actual = db
                .get_index_operation(DataScope::LegacyUnscoped, operation_id)
                .await
                .map_err(|error| error.to_string())?
                .common()
                .stage;
            if actual != stage {
                return Err(format!(
                    "{steps} bounded steps moved {stage:?} to {actual:?}"
                ));
            }
            Ok(())
        }
        LateDeltaPause::Validation(expected) => {
            drive_until_stage(
                db,
                controller,
                operation_id,
                IndexOperationStage::ValidateManifests,
            )
            .await?;
            let logical_start = logical_start_millis();
            for turn in 0..32 {
                if controller
                    .text_manifest_validation_lane(db, DataScope::LegacyUnscoped, operation_id)
                    .await
                    .map_err(|error| error.to_string())?
                    == Some(expected)
                {
                    return Ok(());
                }
                let logical_now = logical_start.saturating_add(
                    u64::try_from(turn)
                        .expect("turn fits u64")
                        .saturating_mul(60_000),
                );
                controller
                    .advance_at_unix_millis(db, target, logical_now)
                    .await
                    .map_err(|error| error.to_string())?;
                drive_children_once(db, controller, target, logical_now).await?;
            }
            Err(format!("validation lane {expected:?} was not reached"))
        }
    }
}

async fn run_late_delta_case(ordinal: usize, pause: LateDeltaPause) -> Result<(), String> {
    let token = ProcessLocalDatabaseToken::new(format!("fts-late-delta-{ordinal}"))
        .map_err(|error| error.to_string())?;
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemoryToken {
            token: token.clone(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .map_err(|error| error.to_string())?;
    insert_node(&db, "initialbuilddocument").await;
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("late-delta definition validates")
            .try_into()
            .expect("late-delta definition converts");
    let operation_id = receipt_operation_id(
        controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition,
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .map_err(|error| error.to_string())?,
    );
    drive_to_late_delta_pause(&db, &controller, operation_id, pause).await?;
    let late_id = insert_node(&db, "latedeltadocument").await;
    let terminal = drive_to_terminal_explicit(&db, &controller, operation_id).await?;
    if !matches!(terminal, IndexOperationStatus::Succeeded { .. }) {
        return Err(format!("late delta ended in {terminal:?}"));
    }
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .map_err(|error| error.to_string())?;
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_nodes(LABEL, PROPERTY, "latedeltadocument", 10, None)
                        .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .map_err(|error| error.to_string())?;
    if query_node_ids(&response, "ids") != [late_id] {
        return Err(format!(
            "late document {late_id} is not authoritative after activation: {response}"
        ));
    }
    let remaining = controller
        .discover(
            &db,
            NonZeroUsize::new(1_024).expect("discovery limit is positive"),
        )
        .await
        .map_err(|error| error.to_string())?;
    if !remaining.targets.is_empty() {
        return Err(format!(
            "terminal late-delta operation left runnable lifecycle work: {:?}",
            remaining.targets
        ));
    }
    db.close().await.map_err(|error| error.to_string())
}

#[tokio::test]
async fn build_delta_before_manifest_preparation_converges_control() {
    run_late_delta_case(0, LateDeltaPause::Stage(IndexOperationStage::CatchUp))
        .await
        .expect("a late delta before manifest preparation converges");
}

#[tokio::test]
async fn active_insert_accepts_empty_build_root_with_zero_corpus_statistics() {
    let token = ProcessLocalDatabaseToken::new("fts-empty-build-root-active-append")
        .expect("empty BUILD root token validates");
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemoryToken {
            token: token.clone(),
        },
        fragmented_bm25_config(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("empty BUILD root fixture opens");
    let initial_ids = [
        insert_node(&db, "firstdeletedbeforemanifest").await,
        insert_node(&db, "seconddeletedbeforemanifest").await,
    ];
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("empty BUILD root definition validates")
            .try_into()
            .expect("empty BUILD root definition converts");
    let operation_id = receipt_operation_id(
        controller
            .create_index(
                &db,
                DataScope::LegacyUnscoped,
                definition,
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("empty BUILD root CREATE is accepted"),
    );
    drive_until_stage(&db, &controller, operation_id, IndexOperationStage::CatchUp)
        .await
        .expect("source scan accounts the initial document before its deletion");

    for (ordinal, initial_id) in initial_ids.into_iter().enumerate() {
        let parameter = name(&format!("empty_build_root_deleted_node_{ordinal}"));
        db.execute(
            &node_mutation_plan(parameter.clone(), exec::ExecMutationPlan::Drop),
            context::ParamBindings::default().with_value(
                parameter,
                PropertyValue::I64(i64::try_from(initial_id).expect("fixture node ID fits i64")),
            ),
        )
        .await
        .expect("the indexed BUILD source is deleted");
    }
    let terminal = drive_to_terminal_explicit(&db, &controller, operation_id)
        .await
        .expect("empty BUILD root lifecycle converges");
    assert!(
        matches!(terminal, IndexOperationStatus::Succeeded { .. }),
        "empty BUILD root reaches Active: {terminal:?}"
    );
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("empty BUILD root Active definition refreshes");
    let empty_state = db
        .migration_parity_v2_state()
        .await
        .expect("empty BUILD root statistics read");
    assert_eq!(
        empty_state
            .text_corpus_statistics
            .iter()
            .map(|statistics| (statistics.document_count, statistics.total_token_count))
            .collect::<Vec<_>>(),
        [(0, 0)],
        "BUILD retains explicit empty-corpus accounting after compacting every stale split"
    );

    let runtime_definition =
        TextIndexDefinition::new_node(LABEL, PROPERTY).expect("damage definition validates");
    db.migration_parity_damage_text_statistics(
        &runtime_definition,
        db::migration_parity::MigrationParityTextStatisticsDamage::ReplaceCorpus {
            tenant: None,
            document_count: 1,
            total_token_count: 1,
        },
    )
    .await
    .expect("live corpus damage is confined to the feature-gated fixture");
    let error = db
        .query(unscoped_node_text_ids_request("corruptemptyrootstatistics"))
        .await
        .expect_err("an empty root with live corpus statistics must fail closed");
    let db::error::HelixDbError::IndexCatalogCorruption(reason) = error else {
        panic!("empty-root corpus damage returned the wrong error: {error}")
    };
    assert_eq!(
        reason, "empty Active text manifest retains non-empty corpus statistics",
        "empty-root corpus damage reports its exact corruption category"
    );

    db.migration_parity_damage_text_statistics(
        &runtime_definition,
        db::migration_parity::MigrationParityTextStatisticsDamage::ReplaceCorpus {
            tenant: None,
            document_count: 0,
            total_token_count: 0,
        },
    )
    .await
    .expect("canonical empty-corpus accounting is restored through typed rows");
    let replacement = insert_node(&db, "replacementafteremptybuild").await;
    let response = db
        .query(unscoped_node_text_ids_request("replacementafteremptybuild"))
        .await
        .expect("the first Active append over an accounted empty corpus remains searchable");
    assert_eq!(query_node_ids(&response, "ids"), [replacement]);
    let populated_state = db
        .migration_parity_v2_state()
        .await
        .expect("replacement Active statistics read");
    assert_eq!(
        populated_state
            .text_corpus_statistics
            .iter()
            .map(|statistics| (statistics.document_count, statistics.total_token_count))
            .collect::<Vec<_>>(),
        [(1, 1)]
    );
    db.close().await.expect("empty BUILD root fixture closes");
}

#[tokio::test]
async fn late_build_delta_after_catch_up_converges_to_active() {
    let mut failures = Vec::new();
    for (ordinal, (name, pause)) in [
        (
            "after-catch-up",
            LateDeltaPause::Stage(IndexOperationStage::Compact),
        ),
        (
            "after-compaction",
            LateDeltaPause::Stage(IndexOperationStage::PrepareManifests),
        ),
        (
            "partial-manifest-preparation",
            LateDeltaPause::StageAfterSteps {
                stage: IndexOperationStage::PrepareManifests,
                steps: 1,
            },
        ),
        (
            "manifest-pages-validation",
            LateDeltaPause::Validation(TextManifestValidationLane::Pages),
        ),
        (
            "manifest-roots-validation",
            LateDeltaPause::Validation(TextManifestValidationLane::Roots),
        ),
        (
            "entity-states-validation",
            LateDeltaPause::Validation(TextManifestValidationLane::EntityStates),
        ),
        (
            "before-activation",
            LateDeltaPause::Stage(IndexOperationStage::Activate),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if let Err(error) = run_late_delta_case(ordinal + 1, pause).await {
            failures.push(format!("{name}: {error}"));
        }
    }
    assert!(
        failures.is_empty(),
        "late BuildDelta must re-enter catch-up without rejecting a populated manifest root:\n{}",
        failures.join("\n")
    );
}

fn text_row_node_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("text search returns rows");
    };
    rows.into_iter()
        .map(|row| {
            let Some(ElementRef::Node(entity_id)) = row.current else {
                panic!("node text search returns node rows");
            };
            entity_id
        })
        .collect()
}

fn blob_path(database: &str, hash: [u8; 32]) -> Path {
    let hex = hash
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Path::from(format!("{}/fts/blobs/{hex}", database.trim_matches('/')))
}

struct DropRaceFixture {
    db: Arc<HelixDB>,
    controller: LifecycleTestController,
    definition: ValidatedDynamicIndexDefinition,
    store: Arc<BarrierObjectStore>,
    later_page_paths: Vec<Path>,
}

async fn activate_drop_race_index(
    db: &HelixDB,
    controller: &LifecycleTestController,
    definition: &ValidatedDynamicIndexDefinition,
) {
    let operation_id = receipt_operation_id(
        controller
            .create_index(
                db,
                DataScope::LegacyUnscoped,
                definition.clone(),
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("DROP/search CREATE is accepted"),
    );
    assert!(matches!(
        drive_to_terminal_explicit(db, controller, operation_id)
            .await
            .expect("DROP/search CREATE converges"),
        IndexOperationStatus::Succeeded { .. }
    ));
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("DROP/search Active catalog refreshes");
}

async fn open_drop_race_fixture(database: &str) -> DropRaceFixture {
    let store = Arc::new(BarrierObjectStore::default());
    open_drop_race_fixture_on_store(database, store).await
}

async fn open_drop_race_fixture_on_store(
    database: &str,
    store: Arc<BarrierObjectStore>,
) -> DropRaceFixture {
    let object_store: Arc<dyn slatedb::object_store::ObjectStore> = store.clone();
    let db = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        database,
        object_store,
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("DROP/search race fixture opens");
    insert_node(&db, "dropneedle first").await;
    insert_node(&db, "dropneedle second").await;
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("DROP/search definition validates")
            .try_into()
            .expect("DROP/search definition converts");
    activate_drop_race_index(&db, &controller, &definition).await;

    let donor_database = format!("{database}-donor");
    let donor_store: Arc<dyn slatedb::object_store::ObjectStore> = store.clone();
    let donor = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        &donor_database,
        donor_store,
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("DROP/search donor fixture opens");
    insert_node(&donor, "dropneedle first").await;
    insert_node(&donor, "dropneedle second").await;
    activate_drop_race_index(&donor, &controller, &definition).await;

    let later_page_paths = controller
        .install_donor_split_for_search_race(&db, &donor, DataScope::LegacyUnscoped, &definition)
        .await
        .expect("DROP/search manifest is split into two pages")
        .into_iter()
        .map(|hash| blob_path(database, hash))
        .collect();
    donor
        .close()
        .await
        .expect("DROP/search donor fixture closes");
    DropRaceFixture {
        db: Arc::new(db),
        controller,
        definition,
        store,
        later_page_paths,
    }
}

fn spawn_text_search(
    db: Arc<HelixDB>,
) -> tokio::task::JoinHandle<Result<Vec<u64>, db::error::HelixDbError>> {
    tokio::spawn(async move {
        db.execute(
            &text_search_plan(LABEL, PROPERTY, "dropneedle"),
            context::ParamBindings::default(),
        )
        .await
        .map(text_row_node_ids)
    })
}

#[tokio::test]
async fn direct_object_store_drop_is_metadata_only_and_preserves_snapshot_search() {
    const DATABASE: &str = "fts-direct-publication-drop-regression";
    let _page_barrier_guard = PAGE_BARRIER_TEST_LOCK.lock().await;
    let fixture = open_drop_race_fixture(DATABASE).await;
    fixture
        .db
        .flush_writer()
        .await
        .expect("active text state becomes reader-visible");

    let object_store: Arc<dyn ObjectStore> = fixture.store.clone();
    let reader = HelixDB::open_reader_with_object_store(DATABASE, object_store)
        .await
        .expect("direct object-store reader opens");
    assert_eq!(
        text_row_node_ids(
            reader
                .execute(
                    &text_search_plan(LABEL, PROPERTY, "dropneedle"),
                    context::ParamBindings::default(),
                )
                .await
                .expect("direct object-store reader searches")
        ),
        [0, 1]
    );
    let warm = reader
        .warm_fts_cache()
        .await
        .expect("direct object-store reader warms text splits");
    assert_eq!(warm.generation_count, 1);
    assert!(warm.split_count >= 2);
    assert_eq!(warm.warm_errors, 0);
    reader.close().await.expect("direct reader closes");

    let barrier = arm_text_search_page_barrier(1);
    let mut search = spawn_text_search(Arc::clone(&fixture.db));
    tokio::select! {
        () = barrier.wait_until_entered() => {}
        result = &mut search => panic!("snapshot search completed before the page barrier: {result:?}"),
    }
    let operation_id = receipt_operation_id(
        fixture
            .controller
            .drop_index(&fixture.db, DataScope::LegacyUnscoped, &fixture.definition)
            .await
            .expect("metadata-only DROP is accepted"),
    );
    assert!(matches!(
        drive_to_terminal_explicit(&fixture.db, &fixture.controller, operation_id)
            .await
            .expect("metadata-only DROP converges"),
        IndexOperationStatus::Succeeded { .. }
    ));
    assert!(
        fixture.store.deleted_paths().is_empty(),
        "text DROP must never call object-store delete"
    );

    barrier.release();
    assert_eq!(
        search
            .await
            .expect("snapshot search task joins")
            .expect("pre-DROP snapshot search succeeds"),
        [0, 1]
    );
    drop(barrier);
    assert!(
        fixture
            .later_page_paths
            .iter()
            .all(|path| !fixture.store.deleted_paths().contains(path)),
        "all uploaded split blobs remain after DROP"
    );

    let Ok(writer) = Arc::try_unwrap(fixture.db) else {
        panic!("DROP fixture owns the only writer reference");
    };
    writer.close().await.expect("direct writer closes");
    let object_store: Arc<dyn ObjectStore> = fixture.store;
    let reader = HelixDB::open_reader_with_object_store(DATABASE, object_store)
        .await
        .expect("post-DROP direct reader opens");
    let error = reader
        .execute(
            &text_search_plan(LABEL, PROPERTY, "dropneedle"),
            context::ParamBindings::default(),
        )
        .await
        .expect_err("dropped index is absent from a fresh reader");
    assert_eq!(error.index_error_code(), Some("index_not_found"));
    reader.close().await.expect("post-DROP reader closes");
}

#[tokio::test]
async fn disk_writer_reader_reopen_and_drop_need_no_runtime_authority() {
    const DATABASE: &str = "fts-disk-reader-reopen-regression";
    let root = tempfile::tempdir().expect("disk reader root is created");
    let source = || HelixDbSource::Disk {
        root: root.path().to_path_buf(),
        database: DATABASE.to_string(),
    };
    let writer = HelixDB::open_for_index_lifecycle_testing(
        source(),
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("disk lifecycle writer opens");
    assert_eq!(insert_node(&writer, "diskneedle").await, 0);
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("disk text definition validates")
            .try_into()
            .expect("disk text definition converts");
    activate_drop_race_index(&writer, &controller, &definition).await;
    writer
        .flush_writer()
        .await
        .expect("disk active state becomes reader-visible");
    writer.close().await.expect("disk lifecycle writer closes");

    let reader = HelixDB::open_reader(source())
        .await
        .expect("disk reader reopens");
    assert_eq!(
        text_row_node_ids(
            reader
                .execute(
                    &text_search_plan(LABEL, PROPERTY, "diskneedle"),
                    context::ParamBindings::default(),
                )
                .await
                .expect("disk reader text search succeeds")
        ),
        [0]
    );
    reader.close().await.expect("disk reader closes");

    let writer = HelixDB::open_for_index_lifecycle_testing(
        source(),
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("disk writer reopens");
    let operation_id = receipt_operation_id(
        controller
            .drop_index(&writer, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("disk DROP is accepted"),
    );
    assert!(matches!(
        drive_to_terminal_explicit(&writer, &controller, operation_id)
            .await
            .expect("disk DROP converges"),
        IndexOperationStatus::Succeeded { .. }
    ));
    writer
        .flush_writer()
        .await
        .expect("disk DROP becomes reader-visible");
    writer.close().await.expect("disk writer closes after DROP");

    let reader = HelixDB::open_reader(source())
        .await
        .expect("post-DROP disk reader opens");
    let error = reader
        .execute(
            &text_search_plan(LABEL, PROPERTY, "diskneedle"),
            context::ParamBindings::default(),
        )
        .await
        .expect_err("dropped disk index is absent");
    assert_eq!(error.index_error_code(), Some("index_not_found"));
    reader.close().await.expect("post-DROP disk reader closes");
}

#[tokio::test]
async fn active_text_upload_failure_aborts_graph_and_index_transaction() {
    const DATABASE: &str = "fts-active-upload-failure-regression";
    let fixture = open_drop_race_fixture(DATABASE).await;
    let before = rollback_snapshot(&fixture.db).await;
    let put_count = fixture.store.text_put_count();
    fixture.store.fail_next_text_put();

    let error = fixture
        .db
        .execute(
            &add_node_plan(
                LABEL,
                vec![(PROPERTY, PropertyValue::from("uploadfailure"))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect_err("injected split upload failure aborts the request");
    assert!(
        error
            .to_string()
            .contains("injected content-addressed text upload failure"),
        "upload error remains attributable: {error}"
    );
    assert_eq!(fixture.store.text_put_count(), put_count + 1);
    assert_eq!(rollback_snapshot(&fixture.db).await, before);
    assert!(text_row_node_ids(
        fixture
            .db
            .execute(
                &text_search_plan(LABEL, PROPERTY, "uploadfailure"),
                context::ParamBindings::default(),
            )
            .await
            .expect("post-failure search remains valid")
    )
    .is_empty());

    let retry_id = insert_node(&fixture.db, "uploadfailure").await;
    assert_eq!(
        text_row_node_ids(
            fixture
                .db
                .execute(
                    &text_search_plan(LABEL, PROPERTY, "uploadfailure"),
                    context::ParamBindings::default(),
                )
                .await
                .expect("retry search succeeds")
        ),
        [retry_id]
    );
    let Ok(writer) = Arc::try_unwrap(fixture.db) else {
        panic!("upload-failure fixture owns the only writer reference");
    };
    writer.close().await.expect("upload-failure writer closes");
}

#[tokio::test]
async fn build_restart_after_upload_preserves_the_orphan_and_completes() {
    const DATABASE: &str = "fts-build-upload-restart-regression";
    let store = Arc::new(BarrierObjectStore::default());
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        DATABASE,
        object_store,
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("restart fixture writer opens");
    assert_eq!(insert_node(&writer, "restartneedle").await, 0);
    let controller = LifecycleTestController::new();
    let definition: ValidatedDynamicIndexDefinition =
        TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("restart text definition validates")
            .try_into()
            .expect("restart text definition converts");
    let operation_id = receipt_operation_id(
        controller
            .create_index(
                &writer,
                DataScope::LegacyUnscoped,
                definition,
                ir::IndexCreateMode::ErrorIfExists,
            )
            .await
            .expect("restart CREATE is accepted"),
    );
    let target = LifecycleWorkTarget::Operation {
        scope: DataScope::LegacyUnscoped,
        operation_id,
    };
    drive_until_stage(
        &writer,
        &controller,
        operation_id,
        IndexOperationStage::ScanPartitions,
    )
    .await
    .expect("restart fixture reaches its direct-upload stage");
    controller
        .advance(&writer, target)
        .await
        .expect("first partition turn creates the empty manifest root");
    assert_eq!(
        store.text_put_count(),
        0,
        "manifest-root initialization does not upload a split"
    );
    db::index_lifecycle_testing::inject_index_outbox_error_once("commit_before")
        .expect("commit failpoint installs");
    let error = controller
        .advance(&writer, target)
        .await
        .expect_err("upload succeeds before the injected transaction failure");
    assert!(
        error.to_string().contains("commit_before"),
        "commit failure remains attributable: {error}"
    );
    let paths_after_failure = store.text_blob_paths().await;
    assert_eq!(paths_after_failure.len(), 1);
    let puts_after_failure = store.text_put_count();
    writer.close().await.expect("failed worker runtime closes");

    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = HelixDB::open_with_object_store_for_index_lifecycle_testing(
        DATABASE,
        object_store,
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("replacement worker runtime opens");
    assert!(matches!(
        drive_to_terminal_explicit(&writer, &controller, operation_id)
            .await
            .expect("replacement worker attaches the uploaded split"),
        IndexOperationStatus::Succeeded { .. }
    ));
    assert!(
        store.text_put_count() > puts_after_failure,
        "replacement worker rebuilds and uploads the interrupted batch"
    );
    let orphan_path = paths_after_failure
        .first()
        .expect("failed attachment left one immutable upload");
    assert!(
        store.text_blob_paths().await.contains(orphan_path),
        "later build phases never delete the unattached content-addressed blob"
    );
    writer
        .planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("replacement catalog refreshes");
    assert_eq!(
        text_row_node_ids(
            writer
                .execute(
                    &text_search_plan(LABEL, PROPERTY, "restartneedle"),
                    context::ParamBindings::default(),
                )
                .await
                .expect("replacement search succeeds")
        ),
        [0]
    );
    writer.close().await.expect("replacement writer closes");
}
