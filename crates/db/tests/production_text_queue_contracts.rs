//! Production-linked contracts for queued text publication, text build
//! limits, Active text compaction, and searches over unpublished index work.
//!
//! Every contract drives public DDL, graph writes, searches, and explicit
//! lifecycle steps through the compiled library. Build contracts pause a real
//! text build at an exact stage or validation lane; the feature-gated damage
//! helpers then alter one durable row or split object, and the production step
//! that follows decides the outcome. Overlay contracts hold publication with
//! explicit scheduling so committed work stays pending while searches run.

use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use db::config::{
    DbConfig, IndexOperationQueueTuning, SearchIndexBackfillLimitError, SearchIndexBackfillLimits,
    SearchIndexBatchLimits, TextBackfillCompactionLimits, TextIndexDefinition,
    VectorIndexDefinition,
};
use db::encoding::v2::keys::scope::DataScope;
use db::error::{HelixDbError, IndexBackpressureResource};
use db::index_lifecycle::{
    IndexDdlReceipt, IndexOperationBlockerCode, IndexOperationId, IndexOperationStage,
    IndexOperationStatus, ValidatedDynamicIndexDefinition,
};
use db::index_lifecycle_testing::{
    LifecycleTestController, LifecycleTestScheduling, LifecycleWorkTarget,
    TextManifestValidationLane,
};
use db::production_coverage::{
    damage_text_build, damage_text_split_object, restore_text_split_object,
    text_compaction_pointer_count, text_manifest_row_counts, text_manifest_split_counts,
    TextBuildDamage, TextSplitObjectDamage,
};
use db::query_service::{HelixQueryService, QueryFailureClass};
use db::search::vector::VectorDistanceMetric;
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::batch;
use helix_ast::error_code::QueryErrorCode;
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryRequest, QueryValue, SearchConsistency};
use helix_ast::traversal;
use helix_ast::value::{PropertyInput, PropertyValue};
use helix_planner::{context, ir, planning};
use serde_json::Value;

const DOC: &str = "QueueTextDocument";
const TENANT_DOC: &str = "QueueTenantDocument";
const BODY: &str = "body";
const EMBEDDING: &str = "embedding";
const TENANT: &str = "tenant";
/// Documented ceiling on physical results one overlaid search may skip past
/// committed but unpublished work (`MAX_SUPPRESSED_SEARCH_RESULTS`).
const SUPPRESSION_LIMIT: usize = 800;
/// Text every suppression fixture document moves to: it still matches
/// `alpha`, but its length ranks it below the one-word published documents.
const MOVED_BODY: &str = "alpha bravo charlie delta echo foxtrot golf hotel india juliet";
const OPERATION_TURNS: usize = 4_096;
const COMPACTION_TIMEOUT: Duration = Duration::from_secs(120);
const CONTRACT_STACK_BYTES: usize = 16 * 1024 * 1024;

/// Runs one contract on a multi-thread runtime with large stacks.
///
/// Explicit build steps compose the complete text and vector driver futures,
/// which exceed default debug-build thread stacks; production sizing is
/// unchanged.
fn run_contract<F, Fut>(name: &'static str, contract: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(CONTRACT_STACK_BYTES)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(CONTRACT_STACK_BYTES)
                .build()
                .expect("contract runtime builds")
                .block_on(contract());
        })
        .expect("contract thread starts")
        .join()
        .expect("contract completes");
}

// ---------------------------------------------------------------------------
// Database, lifecycle, and query harness.
// ---------------------------------------------------------------------------

/// Opens one writer whose process-local token survives a reopen.
async fn open(
    name: &str,
    config: DbConfig,
    scheduling: LifecycleTestScheduling,
) -> (ProcessLocalDatabaseToken, HelixDB) {
    let token = ProcessLocalDatabaseToken::new(name).expect("fixture token is valid");
    let db = reopen(&token, config, scheduling).await;
    (token, db)
}

async fn reopen(
    token: &ProcessLocalDatabaseToken,
    config: DbConfig,
    scheduling: LifecycleTestScheduling,
) -> HelixDB {
    HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemoryToken {
            token: token.clone(),
        },
        config,
        scheduling,
    )
    .await
    .expect("fixture writer opens")
}

fn text_definition(tenant: bool) -> TextIndexDefinition {
    let label = if tenant { TENANT_DOC } else { DOC };
    TextIndexDefinition::new_node(label, BODY)
        .and_then(|definition| definition.with_tenant_property_option(tenant.then_some(TENANT)))
        .expect("fixture text definition validates")
}

fn vector_definition(tenant: bool) -> VectorIndexDefinition {
    let label = if tenant { TENANT_DOC } else { DOC };
    VectorIndexDefinition::new_node(label, EMBEDDING, 2, VectorDistanceMetric::Euclidean)
        .and_then(|definition| definition.with_tenant_property_option(tenant.then_some(TENANT)))
        .expect("fixture vector definition validates")
}

/// Returns a strictly increasing logical wall clock shared by every contract.
///
/// Each explicit step observes ten minutes more than the previous one, so any
/// retry deadline a step persists has passed by a later step.
fn logical_now() -> u64 {
    static START: OnceLock<u64> = OnceLock::new();
    static TICKS: AtomicU64 = AtomicU64::new(0);
    let start = *START.get_or_init(|| {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("fixture clock is after the Unix epoch")
                .as_millis(),
        )
        .expect("fixture time fits u64 milliseconds")
    });
    start.saturating_add(
        TICKS
            .fetch_add(1, Ordering::Relaxed)
            .saturating_mul(600_000),
    )
}

fn operation_target(operation_id: IndexOperationId) -> LifecycleWorkTarget {
    LifecycleWorkTarget::Operation {
        scope: DataScope::LegacyUnscoped,
        operation_id,
    }
}

async fn status(db: &HelixDB, operation_id: IndexOperationId) -> IndexOperationStatus {
    db.get_index_operation(DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("fixture operation remains readable")
}

const fn is_terminal(status: &IndexOperationStatus) -> bool {
    matches!(
        status,
        IndexOperationStatus::Succeeded { .. }
            | IndexOperationStatus::Blocked { .. }
            | IndexOperationStatus::Aborted { .. }
    )
}

/// Accepts one BUILD without advancing it.
async fn create(
    db: &HelixDB,
    controller: &LifecycleTestController,
    definition: impl TryInto<ValidatedDynamicIndexDefinition, Error = impl std::fmt::Debug>,
) -> IndexOperationId {
    let receipt = controller
        .create_index(
            db,
            DataScope::LegacyUnscoped,
            definition.try_into().expect("fixture definition converts"),
            ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("fixture BUILD is accepted");
    match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => panic!("fixture definition is fresh"),
    }
}

/// Advances one operation until it terminates, then drains any other work.
async fn drive_to_terminal(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_id: IndexOperationId,
) -> IndexOperationStatus {
    for _ in 0..OPERATION_TURNS {
        let current = status(db, operation_id).await;
        if is_terminal(&current) {
            let page = controller
                .discover(db, NonZeroUsize::new(1_024).expect("positive"))
                .await
                .expect("lifecycle work is discoverable");
            let others = page
                .targets
                .into_iter()
                .filter(|target| *target != operation_target(operation_id))
                .collect::<Vec<_>>();
            if others.is_empty() {
                return current;
            }
            for other in others {
                controller
                    .advance_at_unix_millis(db, other, logical_now())
                    .await
                    .expect("drained lifecycle work advances");
            }
            continue;
        }
        controller
            .advance_at_unix_millis(db, operation_target(operation_id), logical_now())
            .await
            .expect("fixture operation advances");
    }
    panic!("operation exceeded {OPERATION_TURNS} explicit turns");
}

/// Where a build pauses before a contract changes its durable state.
#[derive(Debug, Clone, Copy)]
enum Pause {
    Stage(IndexOperationStage),
    Lane(TextManifestValidationLane),
}

async fn drive_to_pause(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_id: IndexOperationId,
    pause: Pause,
) {
    for _ in 0..OPERATION_TURNS {
        let current = status(db, operation_id).await;
        assert!(
            !is_terminal(&current),
            "build terminated before {pause:?}: {current:?}"
        );
        let reached = match pause {
            Pause::Stage(stage) => current.common().stage == stage,
            Pause::Lane(lane) => {
                controller
                    .text_manifest_validation_lane(db, DataScope::LegacyUnscoped, operation_id)
                    .await
                    .expect("validation lane is observable")
                    == Some(lane)
            }
        };
        if reached {
            return;
        }
        controller
            .advance_at_unix_millis(db, operation_target(operation_id), logical_now())
            .await
            .expect("fixture operation advances");
    }
    panic!("build never reached {pause:?}");
}

/// Creates and fully builds one index, which must activate.
async fn build(
    db: &HelixDB,
    controller: &LifecycleTestController,
    definition: impl TryInto<ValidatedDynamicIndexDefinition, Error = impl std::fmt::Debug>,
) -> IndexOperationStatus {
    let operation_id = create(db, controller, definition).await;
    let terminal = drive_to_terminal(db, controller, operation_id).await;
    assert!(
        matches!(terminal, IndexOperationStatus::Succeeded { .. }),
        "fixture build activates: {terminal:?}"
    );
    terminal
}

/// Runs one request, retrying only transaction conflicts with the worker.
async fn query(db: &HelixDB, request: QueryRequest) -> Result<Value, HelixDbError> {
    let started = Instant::now();
    loop {
        match db.query(request.clone()).await {
            Err(error)
                if error.is_transaction_conflict()
                    && started.elapsed() < Duration::from_secs(30) =>
            {
                tokio::task::yield_now().await;
            }
            result => return result,
        }
    }
}

async fn write(db: &HelixDB, write: batch::WriteBatch) -> Value {
    query(db, QueryRequest::write(write))
        .await
        .unwrap_or_else(|error| panic!("fixture write commits: {error}"))
}

/// Reads one projected ID binding, which the planner emits as an array, or
/// as one optional scalar when it proves at most one result.
fn ids(response: &Value, variable: &str) -> Vec<u64> {
    match &response[variable] {
        Value::Array(values) => values
            .iter()
            .map(|value| value.as_u64().expect("fixture ID is unsigned"))
            .collect(),
        Value::Number(value) => vec![value.as_u64().expect("fixture ID is unsigned")],
        Value::Null => Vec::new(),
        other @ (Value::Bool(_) | Value::String(_) | Value::Object(_)) => {
            panic!("{variable} is not an ID binding: {other}")
        }
    }
}

fn sorted(mut values: Vec<u64>) -> Vec<u64> {
    values.sort_unstable();
    values
}

fn document(
    body: &str,
    embedding: Option<[f32; 2]>,
    tenant: Option<&str>,
) -> Vec<(&'static str, PropertyInput)> {
    [
        (BODY, Some(PropertyInput::from(body))),
        (
            EMBEDDING,
            embedding.map(|point| PropertyInput::from(point.to_vec())),
        ),
        (TENANT, tenant.map(PropertyInput::from)),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|value| (name, value)))
    .collect()
}

/// Inserts documents in one transaction and returns their IDs in order.
async fn insert(
    db: &HelixDB,
    label: &str,
    documents: Vec<Vec<(&'static str, PropertyInput)>>,
) -> Vec<u64> {
    let names = (0..documents.len())
        .map(|ordinal| format!("d{ordinal}"))
        .collect::<Vec<_>>();
    let request = documents.into_iter().zip(&names).fold(
        batch::write_batch(),
        |request, (properties, name)| {
            request.var_as(name, traversal::g().add_n(label, properties).id())
        },
    );
    let response = write(db, request.returning(names.clone())).await;
    names.iter().map(|name| ids(&response, name)[0]).collect()
}

async fn delete(db: &HelixDB, id: u64) {
    write(
        db,
        batch::write_batch()
            .var_as("deleted", traversal::g().n(NodeRef::id(id)).drop())
            .returning(Vec::<String>::new()),
    )
    .await;
}

async fn publish(db: &HelixDB) -> u64 {
    let released = db
        .publish_index_queues_for_lifecycle_testing()
        .await
        .expect("queued index work publishes");
    assert_eq!(
        db.index_operation_queue_stats().pending_operations,
        0,
        "publication drains every queue"
    );
    released
}

fn read(
    variable_source: traversal::Traversal<traversal::OnNodes>,
    consistency: SearchConsistency,
) -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as("ids", variable_source.id())
            .returning(["ids"]),
    )
    .with_search_consistency(consistency)
    .expect("reads accept every search consistency")
}

async fn search(
    db: &HelixDB,
    source: traversal::Traversal<traversal::OnNodes>,
    consistency: SearchConsistency,
) -> Result<Vec<u64>, HelixDbError> {
    query(db, read(source, consistency))
        .await
        .map(|response| ids(&response, "ids"))
}

fn text(
    query_text: &str,
    k: usize,
    tenant: Option<&str>,
) -> traversal::Traversal<traversal::OnNodes> {
    let label = if tenant.is_some() { TENANT_DOC } else { DOC };
    traversal::g().text_search_nodes(label, BODY, query_text, k, tenant.map(PropertyValue::from))
}

fn nearest(point: [f32; 2], k: usize) -> traversal::Traversal<traversal::OnNodes> {
    traversal::g().vector_search_nodes(DOC, EMBEDDING, point.to_vec(), k, None)
}

/// Requires `error` to be retryable suppressed-result backpressure.
fn assert_suppressed(error: &HelixDbError, context: &str) {
    let HelixDbError::IndexBackpressure {
        resource,
        requested,
        limit,
        ..
    } = error
    else {
        panic!("{context}: expected suppression backpressure, got {error}");
    };
    assert_eq!(
        *resource,
        IndexBackpressureResource::SuppressedSearchResults,
        "{context}"
    );
    assert_eq!(*limit, SUPPRESSION_LIMIT as u64, "{context}");
    assert!(
        *requested > *limit,
        "{context}: {requested} exceeds {limit}"
    );
    assert!(error.is_index_backpressure(), "{context}");
    assert_eq!(error.error_code(), QueryErrorCode::IndexBackpressure);
    assert!(
        error.to_string().contains("suppressed_search_results"),
        "{context}: {error}"
    );
}

/// Runs text and vector searches whose tenant expression evaluates to null.
async fn assert_null_tenant_selects_nothing(db: &HelixDB) {
    for (family, source) in [
        (
            "text",
            traversal::g().text_search_nodes_with(
                TENANT_DOC,
                BODY,
                PropertyInput::from("gamma"),
                5_usize,
                Some(PropertyInput::param("tenant")),
            ),
        ),
        (
            "vector",
            traversal::g().vector_search_nodes_with(
                TENANT_DOC,
                EMBEDDING,
                PropertyInput::from(vec![1.0_f32, 0.0]),
                5_usize,
                Some(PropertyInput::param("tenant")),
            ),
        ),
    ] {
        let response = query(
            db,
            read(source, SearchConsistency::Strong)
                .with_parameter_value("tenant", QueryValue::Null),
        )
        .await
        .unwrap_or_else(|error| panic!("{family} null-tenant search succeeds: {error}"));
        assert!(ids(&response, "ids").is_empty(), "{family}: {response}");
    }
}

// ---------------------------------------------------------------------------
// Search overlays of committed but unpublished work.
// ---------------------------------------------------------------------------

/// Strong searches whose answer lies behind more superseded physical results
/// than the suppression limit fail with retryable backpressure, in read and
/// write requests alike; eventual reads overlay only the oldest entities and
/// settle; publication clears the condition.
#[test]
fn searches_past_the_suppression_limit_fail_strongly_and_settle_eventually() {
    run_contract(
        "searches_past_the_suppression_limit_fail_strongly_and_settle_eventually",
        searches_past_the_suppression_limit_fail_strongly_and_settle_eventually_contract,
    );
}

async fn searches_past_the_suppression_limit_fail_strongly_and_settle_eventually_contract() {
    let (_token, db) = open(
        "queue-text-suppression-limit",
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let controller = LifecycleTestController::new();
    build(&db, &controller, text_definition(false)).await;
    build(&db, &controller, vector_definition(false)).await;

    let count = SUPPRESSION_LIMIT + 20;
    let mut inserted = Vec::with_capacity(count);
    for chunk in (0..count).collect::<Vec<_>>().chunks(100) {
        let documents = chunk
            .iter()
            .map(|ordinal| document("alpha", Some([*ordinal as f32 * 0.001, 0.0]), None))
            .collect();
        inserted.extend(insert(&db, DOC, documents).await);
    }
    assert_eq!(publish(&db).await, 2 * count as u64);

    // Every published document moves, unpublished: far from the vector query,
    // and into a longer text that still matches but ranks below every
    // published document.
    write(
        &db,
        batch::write_batch()
            .var_as(
                "moved",
                traversal::g()
                    .n_with_label(DOC)
                    .set_property(BODY, MOVED_BODY)
                    .set_property(EMBEDDING, vec![100.0_f32, 100.0]),
            )
            .returning(Vec::<String>::new()),
    )
    .await;
    assert_eq!(
        db.index_operation_queue_stats().pending_operations,
        2 * count as u64
    );

    for (family, source) in [
        ("vector", nearest([0.0, 0.0], 2)),
        ("text", text("alpha", 2, None)),
    ] {
        let error = search(&db, source.clone(), SearchConsistency::Strong)
            .await
            .expect_err("a strong search never answers past the suppression limit");
        assert_suppressed(&error, family);
        let settled = search(&db, source, SearchConsistency::Eventual)
            .await
            .unwrap_or_else(|error| panic!("{family} eventual search settles: {error}"));
        assert_eq!(settled.len(), 2, "{family} eventual search keeps k results");
        assert!(
            settled.iter().all(|id| inserted.contains(id)),
            "{family}: {settled:?}"
        );
    }

    // A write request searches strongly through its own transaction.
    let error = query(
        &db,
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "unindexed",
                    traversal::g().add_n("QueueUnindexed", Vec::<(&str, PropertyInput)>::new()),
                )
                .var_as("ids", text("alpha", 2, None).id())
                .returning(["ids"]),
        ),
    )
    .await
    .expect_err("a write's strong search never answers past the suppression limit");
    assert_suppressed(&error, "write request");

    assert_eq!(publish(&db).await, 2 * count as u64);
    for (family, source) in [
        ("text", text("juliet", 5, None)),
        ("text", text("alpha", 5, None)),
        ("vector", nearest([100.0, 100.0], 5)),
    ] {
        assert_eq!(
            search(&db, source, SearchConsistency::Strong)
                .await
                .unwrap_or_else(|error| panic!("published {family} search succeeds: {error}"))
                .len(),
            5
        );
    }
    db.close().await.expect("fixture closes");
}

/// Overlays over traversal-restricted candidates, empty queries, absent
/// tenants, a write's own changes to already pending entities, and eventual
/// reads return exactly the latest committed state.
#[test]
fn overlays_answer_restricted_tenant_local_and_eventual_searches() {
    run_contract(
        "overlays_answer_restricted_tenant_local_and_eventual_searches",
        overlays_answer_restricted_tenant_local_and_eventual_searches_contract,
    );
}

async fn overlays_answer_restricted_tenant_local_and_eventual_searches_contract() {
    let (_token, db) = open(
        "queue-text-overlay-shapes",
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let controller = LifecycleTestController::new();
    for tenant in [false, true] {
        build(&db, &controller, text_definition(tenant)).await;
        build(&db, &controller, vector_definition(tenant)).await;
    }
    let [first, second, third] = insert(
        &db,
        DOC,
        vec![
            document("gamma one", Some([1.0, 0.0]), None),
            document("gamma two", Some([2.0, 0.0]), None),
            document("gamma three", Some([3.0, 0.0]), None),
        ],
    )
    .await[..] else {
        panic!("three documents are inserted");
    };
    let [tenanted] = insert(
        &db,
        TENANT_DOC,
        vec![document("gamma tenant", Some([1.0, 0.0]), Some("t1"))],
    )
    .await[..] else {
        panic!("one tenant document is inserted");
    };
    publish(&db).await;

    // Committed but unpublished changes to one document of each label.
    write(
        &db,
        batch::write_batch()
            .var_as(
                "moved",
                traversal::g()
                    .n(NodeRef::id(first))
                    .set_property(BODY, "delta")
                    .set_property(EMBEDDING, vec![1.5_f32, 0.0]),
            )
            .returning(Vec::<String>::new()),
    )
    .await;
    let [pending_tenanted] = insert(
        &db,
        TENANT_DOC,
        vec![document("gamma pending", Some([2.0, 0.0]), Some("t1"))],
    )
    .await[..] else {
        panic!("one pending tenant document is inserted");
    };

    // Restricted vector search: the superseded candidate is scored from the
    // queue, the others physically.
    assert_eq!(
        search(
            &db,
            traversal::g()
                .n_with_label(DOC)
                .vector_search(DOC, EMBEDDING, vec![0.0, 0.0], 3, None),
            SearchConsistency::Strong,
        )
        .await
        .expect("restricted vector search succeeds"),
        [first, second, third]
    );
    // Restricted text search whose every candidate is superseded.
    assert_eq!(
        search(
            &db,
            traversal::g()
                .n(NodeRef::id(first))
                .text_search(DOC, BODY, "delta", 5, None),
            SearchConsistency::Strong,
        )
        .await
        .expect("fully superseded restricted text search succeeds"),
        [first]
    );
    // Restricted text search over no candidates.
    assert!(search(
        &db,
        traversal::g()
            .n_with_label_where(DOC, Predicate::eq(BODY, "no such body"))
            .text_search(DOC, BODY, "gamma", 5, None),
        SearchConsistency::Strong,
    )
    .await
    .expect("empty restricted text search succeeds")
    .is_empty());
    // A query without indexable terms matches nothing, pending or not.
    assert!(search(&db, text("!!!", 5, None), SearchConsistency::Strong)
        .await
        .expect("termless text search succeeds")
        .is_empty());
    // Eventual reads overlay the same committed change.
    assert_eq!(
        search(&db, text("delta", 5, None), SearchConsistency::Eventual)
            .await
            .expect("eventual text search succeeds"),
        [first]
    );
    assert_eq!(
        sorted(
            search(&db, text("gamma", 5, None), SearchConsistency::Eventual)
                .await
                .expect("eventual text search succeeds")
        ),
        [second, third]
    );

    // A null tenant selects no partition, even with pending work.
    assert_null_tenant_selects_nothing(&db).await;
    assert_eq!(
        sorted(
            search(&db, text("gamma", 5, Some("t1")), SearchConsistency::Strong)
                .await
                .expect("tenant text search succeeds")
        ),
        sorted(vec![tenanted, pending_tenanted])
    );

    // A write changing an already pending entity overlays its own change.
    let response = write(
        &db,
        batch::write_batch()
            .var_as(
                "changed",
                traversal::g()
                    .n(NodeRef::id(first))
                    .set_property(BODY, "epsilon"),
            )
            .var_as("ids", text("epsilon", 5, None).id())
            .var_as("nearest", nearest([1.5, 0.0], 1).id())
            .var_as(
                "restricted",
                traversal::g()
                    .n_with_label(DOC)
                    .vector_search(DOC, EMBEDDING, vec![0.0, 0.0], 3, None)
                    .id(),
            )
            .returning(["ids", "nearest", "restricted"]),
    )
    .await;
    assert_eq!(ids(&response, "ids"), [first]);
    assert_eq!(ids(&response, "nearest"), [first]);
    assert_eq!(ids(&response, "restricted"), [first, second, third]);

    // Queued admission measures non-ASCII text exactly.
    let [accented] = insert(
        &db,
        DOC,
        vec![document("naïve café", Some([9.0, 9.0]), None)],
    )
    .await[..] else {
        panic!("one accented document is inserted");
    };
    publish(&db).await;
    assert_eq!(
        search(&db, text("café", 5, None), SearchConsistency::Strong)
            .await
            .expect("accented text search succeeds"),
        [accented]
    );
    assert_eq!(
        search(&db, text("epsilon", 5, None), SearchConsistency::Strong)
            .await
            .expect("published text search succeeds"),
        [first]
    );
    // With nothing pending, the same searches read only published state.
    assert_null_tenant_selects_nothing(&db).await;
    assert!(search(&db, text("!!!", 5, None), SearchConsistency::Strong)
        .await
        .expect("termless published text search succeeds")
        .is_empty());
    db.close().await.expect("fixture closes");
}

/// Plans prepared while an index was Active fail closed once a DROP retires
/// it: searches executed from them report the index as unavailable or
/// missing instead of reading the dropping generation or its queue.
#[test]
fn stale_search_plans_fail_closed_after_drop() {
    run_contract(
        "stale_search_plans_fail_closed_after_drop",
        stale_search_plans_fail_closed_after_drop_contract,
    );
}

async fn stale_search_plans_fail_closed_after_drop_contract() {
    let (_token, db) = open(
        "queue-text-stale-plans",
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let controller = LifecycleTestController::new();
    build(&db, &controller, text_definition(false)).await;
    build(&db, &controller, vector_definition(false)).await;
    insert(&db, DOC, vec![document("alpha", Some([1.0, 0.0]), None)]).await;
    publish(&db).await;

    let planner = db
        .planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("the Active catalog plans");
    let read_plan = |source: traversal::Traversal<traversal::OnNodes>| {
        planning::plan_read_batch(
            &batch::read_batch()
                .var_as("ids", source.id())
                .returning(["ids"]),
            &planner,
        )
        .expect("an Active search plans")
    };
    let plans = [
        ("text read", read_plan(text("alpha", 5, None))),
        ("vector read", read_plan(nearest([0.0, 0.0], 5))),
        (
            "vector write",
            planning::plan_write_batch(
                &batch::write_batch()
                    .var_as(
                        "unindexed",
                        traversal::g().add_n("QueueUnindexed", Vec::<(&str, PropertyInput)>::new()),
                    )
                    .var_as("ids", nearest([0.0, 0.0], 5).id())
                    .returning(["ids"]),
                &planner,
            )
            .expect("an Active write search plans"),
        ),
    ];
    for definition in [
        ValidatedDynamicIndexDefinition::try_from(text_definition(false))
            .expect("text definition converts"),
        ValidatedDynamicIndexDefinition::try_from(vector_definition(false))
            .expect("vector definition converts"),
    ] {
        controller
            .drop_index(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("an Active index accepts DROP");
    }
    for (shape, plan) in plans {
        let error = db
            .execute(&plan, context::ParamBindings::default())
            .await
            .expect_err("a stale plan never searches a dropping generation");
        // Reads resolve the generation in their pinned view; writes reload
        // their mutation catalog first and no longer find the definition.
        let fails_closed = match shape {
            "vector write" => matches!(error, HelixDbError::IndexNotFound(_)),
            _ => matches!(error, HelixDbError::IndexLifecycleUnavailable { .. }),
        };
        assert!(fails_closed, "{shape}: {error}");
    }
    db.close().await.expect("fixture closes");
}

// ---------------------------------------------------------------------------
// Active text compaction.
// ---------------------------------------------------------------------------

/// Backfill limits whose Active compaction merges every two equal-tier splits.
fn pairwise_compaction_config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    DbConfig::new().with_search_index_backfill_limits(
        SearchIndexBackfillLimits::try_new(
            defaults.batch(),
            defaults.edge_property_read_batch(),
            defaults.text_artifacts(),
            TextBackfillCompactionLimits::new(
                NonZeroUsize::new(2).expect("positive"),
                compaction.max_input_bytes(),
                compaction.max_temporary_disk_bytes(),
                compaction.max_output_blob_bytes(),
                compaction.max_manifest_bytes(),
            ),
        )
        .expect("pairwise compaction limits validate"),
    )
}

async fn tenant_ids(db: &HelixDB, tenant: &str) -> Vec<u64> {
    sorted(
        search(
            db,
            text("alpha", 10, Some(tenant)),
            SearchConsistency::Strong,
        )
        .await
        .expect("tenant text search succeeds"),
    )
}

/// Each queued publication appends one split and schedules its page for
/// compaction. After a restart with automatic scheduling, the compactor
/// merges a page's live splits into one, and retires a page whose documents
/// were all deleted to a single placeholder split, without changing results.
#[test]
fn active_compaction_merges_live_splits_and_retires_stale_pages() {
    run_contract(
        "active_compaction_merges_live_splits_and_retires_stale_pages",
        active_compaction_merges_live_splits_and_retires_stale_pages_contract,
    );
}

async fn active_compaction_merges_live_splits_and_retires_stale_pages_contract() {
    let config = pairwise_compaction_config();
    let (token, db) = open(
        "queue-text-active-compaction",
        config.clone(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let controller = LifecycleTestController::new();
    let built = build(&db, &controller, text_definition(true)).await;
    let (index_id, generation) = (built.common().index_id, built.common().generation);

    // One publication per document, so every document owns one split.
    let mut live = Vec::new();
    for body in ["alpha one", "alpha two"] {
        live.extend(insert(&db, TENANT_DOC, vec![document(body, None, Some("live"))]).await);
        publish(&db).await;
    }
    let mut gone = Vec::new();
    for body in ["alpha six", "alpha ten"] {
        gone.extend(insert(&db, TENANT_DOC, vec![document(body, None, Some("gone"))]).await);
        publish(&db).await;
    }
    for id in &gone {
        delete(&db, *id).await;
    }
    publish(&db).await;
    let live = sorted(live);
    assert_eq!(tenant_ids(&db, "live").await, live);
    assert!(tenant_ids(&db, "gone").await.is_empty());
    assert_eq!(
        text_manifest_split_counts(&db, index_id, generation).await,
        [2, 2],
        "every publication appended one split"
    );
    assert_eq!(
        text_compaction_pointer_count(&db).await,
        2,
        "each partition page is scheduled"
    );
    db.close().await.expect("explicit fixture closes");

    let db = reopen(&token, config, LifecycleTestScheduling::Automatic).await;
    let started = Instant::now();
    while text_compaction_pointer_count(&db).await != 0 {
        assert!(
            started.elapsed() < COMPACTION_TIMEOUT,
            "automatic compaction retires every pointer"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        text_manifest_split_counts(&db, index_id, generation).await,
        [1, 1],
        "live splits merge and an all-stale page keeps one placeholder split"
    );
    assert_eq!(tenant_ids(&db, "live").await, live);
    assert!(tenant_ids(&db, "gone").await.is_empty());
    db.close().await.expect("automatic fixture closes");
}

/// A compaction pointer names one exact generation, so it outlives that
/// generation when the index is dropped, or dropped and built again. After a
/// restart with automatic scheduling, the compactor discards each stale
/// pointer without rewriting the rebuilt generation, whose searches keep
/// answering.
#[test]
fn compaction_discards_pointers_of_dropped_and_rebuilt_generations() {
    run_contract(
        "compaction_discards_pointers_of_dropped_and_rebuilt_generations",
        compaction_discards_pointers_of_dropped_and_rebuilt_generations_contract,
    );
}

async fn compaction_discards_pointers_of_dropped_and_rebuilt_generations_contract() {
    let config = pairwise_compaction_config();
    let definition = ValidatedDynamicIndexDefinition::try_from(text_definition(true))
        .expect("text definition converts");
    for rebuild in [false, true] {
        let (token, db) = open(
            &format!("queue-text-stale-pointer-{rebuild}"),
            config.clone(),
            LifecycleTestScheduling::Explicit,
        )
        .await;
        let controller = LifecycleTestController::new();
        build(&db, &controller, text_definition(true)).await;
        let mut stale = Vec::new();
        for body in ["alpha one", "alpha two"] {
            stale.extend(insert(&db, TENANT_DOC, vec![document(body, None, Some("stale"))]).await);
            publish(&db).await;
        }
        assert_eq!(
            text_compaction_pointer_count(&db).await,
            1,
            "the page is scheduled"
        );

        let IndexDdlReceipt::Accepted { operation_id, .. } = controller
            .drop_index(&db, DataScope::LegacyUnscoped, &definition)
            .await
            .expect("an Active index accepts DROP")
        else {
            panic!("DROP of an Active index starts a new operation");
        };
        let dropped = drive_to_terminal(&db, &controller, operation_id).await;
        assert!(
            matches!(dropped, IndexOperationStatus::Succeeded { .. }),
            "{dropped:?}"
        );
        assert_eq!(
            text_compaction_pointer_count(&db).await,
            1,
            "DROP leaves the pointer to its generation"
        );
        let rebuilt = if rebuild {
            let built = build(&db, &controller, text_definition(true)).await;
            let (index_id, generation) = (built.common().index_id, built.common().generation);
            let splits = text_manifest_split_counts(&db, index_id, generation).await;
            Some((index_id, generation, splits))
        } else {
            None
        };
        db.close().await.expect("explicit fixture closes");

        let db = reopen(&token, config.clone(), LifecycleTestScheduling::Automatic).await;
        let started = Instant::now();
        while text_compaction_pointer_count(&db).await != 0 {
            assert!(
                started.elapsed() < COMPACTION_TIMEOUT,
                "automatic compaction discards the stale pointer (rebuild {rebuild})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let Some((index_id, generation, splits)) = rebuilt else {
            db.close().await.expect("automatic fixture closes");
            continue;
        };
        assert_eq!(
            text_manifest_split_counts(&db, index_id, generation).await,
            splits,
            "the rebuilt generation is not rewritten"
        );
        assert_eq!(tenant_ids(&db, "stale").await, sorted(stale));
        db.close().await.expect("automatic fixture closes");
    }
}

// ---------------------------------------------------------------------------
// Build limits.
// ---------------------------------------------------------------------------

/// Backfill limits that differ from the defaults; `None` keeps a default.
#[derive(Debug, Clone, Copy, Default)]
struct Overrides {
    entities: Option<usize>,
    input_bytes: Option<u64>,
    output_operations: Option<u64>,
    manifest_bytes: Option<u64>,
    fan_in: Option<usize>,
}

/// Builds validated backfill limits from defaults with `overrides` applied.
fn limits(
    overrides: Overrides,
) -> Result<SearchIndexBackfillLimits, SearchIndexBackfillLimitError> {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let compaction = defaults.text_compaction();
    SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            overrides
                .entities
                .map_or(Some(batch.max_entities()), NonZeroUsize::new)
                .expect("positive entities"),
            overrides
                .input_bytes
                .map_or(Some(batch.max_input_bytes()), NonZeroU64::new)
                .expect("positive input"),
            overrides
                .output_operations
                .map_or(Some(batch.max_output_operations()), NonZeroU64::new)
                .expect("positive operations"),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .expect("batch limits validate"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        TextBackfillCompactionLimits::new(
            overrides
                .fan_in
                .map_or(Some(compaction.max_fan_in()), NonZeroUsize::new)
                .expect("positive fan-in"),
            compaction.max_input_bytes(),
            compaction.max_temporary_disk_bytes(),
            compaction.max_output_blob_bytes(),
            overrides
                .manifest_bytes
                .map_or(Some(compaction.max_manifest_bytes()), NonZeroU64::new)
                .expect("positive manifest"),
        ),
    )
}

/// Returns the smallest value `accepts` admits in `1..=upper`, requiring every
/// smaller value to be rejected as too small for the smallest document.
fn smallest_admitted(
    upper: u64,
    accepts: impl Fn(u64) -> Result<SearchIndexBackfillLimits, SearchIndexBackfillLimitError>,
) -> u64 {
    let (mut rejected, mut admitted) = (0_u64, upper);
    assert!(accepts(upper).is_ok(), "{upper} is admitted");
    while admitted - rejected > 1 {
        let middle = rejected + (admitted - rejected) / 2;
        match accepts(middle) {
            Ok(_) => admitted = middle,
            Err(error) => {
                assert!(
                    matches!(
                        error,
                        SearchIndexBackfillLimitError::TextDocumentAllowanceTooSmall { .. }
                    ),
                    "{middle} is rejected as too small: {error}"
                );
                assert!(!error.to_string().is_empty());
                rejected = middle;
            }
        }
    }
    admitted
}

/// One manifest page holds exactly one split of any fixture partition.
fn one_split_manifest_bytes() -> u64 {
    let smallest = smallest_admitted(4 * 1024 * 1024, |bytes| {
        limits(Overrides {
            manifest_bytes: Some(bytes),
            ..Overrides::default()
        })
    });
    // A tenant page carries its partition bytes; a second split reference
    // (hash, sizes, and pruning bloom) is far larger than this slack.
    smallest + 48
}

struct BuildOutcome {
    terminal: IndexOperationStatus,
    pages: usize,
}

/// Builds `definition` over `documents` inserted before CREATE.
async fn build_fixture(
    name: &str,
    config: DbConfig,
    tenant: bool,
    documents: Vec<Vec<(&'static str, PropertyInput)>>,
) -> (HelixDB, Vec<u64>, BuildOutcome) {
    let (_token, db) = open(name, config, LifecycleTestScheduling::Explicit).await;
    let label = if tenant { TENANT_DOC } else { DOC };
    let inserted = insert(&db, label, documents).await;
    let controller = LifecycleTestController::new();
    let operation_id = create(&db, &controller, text_definition(tenant)).await;
    let terminal = drive_to_terminal(&db, &controller, operation_id).await;
    let (_, pages) = text_manifest_row_counts(
        &db,
        terminal.common().index_id,
        terminal.common().generation,
    )
    .await;
    (db, inserted, BuildOutcome { terminal, pages })
}

fn blocker(status: &IndexOperationStatus) -> Option<IndexOperationBlockerCode> {
    match status {
        IndexOperationStatus::Blocked { blocker_code, .. } => Some(*blocker_code),
        IndexOperationStatus::Queued { .. }
        | IndexOperationStatus::Running { .. }
        | IndexOperationStatus::Succeeded { .. }
        | IndexOperationStatus::Aborted { .. } => None,
    }
}

/// Words that make each input-budget fixture document distinct.
const PARTITION_WORDS: [&str; 6] = ["bravo", "charlie", "delta", "echo", "foxtrot", "golf"];

/// Across batch input budgets from far too small to ample, a partitioned
/// build either activates with the exact documents or blocks with a typed
/// size limit, and once a budget activates every larger one does too. Small
/// budgets split scans and validation into single-row steps.
#[test]
fn text_builds_activate_or_block_typed_across_input_budgets() {
    run_contract(
        "text_builds_activate_or_block_typed_across_input_budgets",
        text_builds_activate_or_block_typed_across_input_budgets_contract,
    );
}

async fn text_builds_activate_or_block_typed_across_input_budgets_contract() {
    let manifest = one_split_manifest_bytes();
    let mut activated = Vec::new();
    let mut blocked = Vec::new();
    for budget in (128_u64..=2_048).step_by(96) {
        let config = DbConfig::new().with_search_index_backfill_limits(
            limits(Overrides {
                input_bytes: Some(budget),
                manifest_bytes: Some(manifest),
                ..Overrides::default()
            })
            .expect("budget limits validate"),
        );
        let (db, inserted, outcome) = build_fixture(
            &format!("queue-text-input-budget-{budget}"),
            config,
            true,
            ["a", "b"]
                .into_iter()
                .flat_map(|tenant| {
                    PARTITION_WORDS
                        .into_iter()
                        .map(move |word| document(&format!("alpha {word}"), None, Some(tenant)))
                })
                .collect(),
        )
        .await;
        let per_tenant = PARTITION_WORDS.len();
        match (&outcome.terminal, blocker(&outcome.terminal)) {
            (IndexOperationStatus::Succeeded { .. }, None) => {
                for (tenant, expected) in [
                    ("a", &inserted[..per_tenant]),
                    ("b", &inserted[per_tenant..]),
                ] {
                    assert_eq!(
                        sorted(
                            search(
                                &db,
                                text("alpha", 10, Some(tenant)),
                                SearchConsistency::Strong
                            )
                            .await
                            .expect("activated tenant search succeeds")
                        ),
                        sorted(expected.to_vec()),
                        "budget {budget} tenant {tenant}"
                    );
                }
                activated.push((budget, outcome.pages));
            }
            (
                IndexOperationStatus::Blocked { .. },
                Some(
                    IndexOperationBlockerCode::OversizedEntity
                    | IndexOperationBlockerCode::ManifestLimit,
                ),
            ) => blocked.push(budget),
            (terminal, _) => panic!("budget {budget} ended untyped: {terminal:?}"),
        }
        db.close().await.expect("fixture closes");
    }
    assert!(
        blocked.contains(&128),
        "the smallest budget blocks: {activated:?}"
    );
    assert!(
        activated.iter().any(|(budget, _)| *budget == 2_048),
        "an ample budget activates: {blocked:?}"
    );
    let smallest_activated = activated
        .iter()
        .map(|(budget, _)| *budget)
        .min()
        .expect("some budget activates");
    assert!(
        blocked.iter().all(|budget| *budget < smallest_activated),
        "a budget that activates never blocks a larger one: {blocked:?} {activated:?}"
    );
    assert!(
        activated.iter().all(|(_, pages)| *pages >= 2),
        "every activated partition has a page: {activated:?}"
    );
}

/// Budgets lowered between a build's source scan and a later stage block
/// that stage with a typed limit rather than commit an oversized step: a
/// tenant page larger than the new manifest limit blocks manifest
/// preparation, and small input budgets block or split the partition scan.
#[test]
fn budgets_lowered_mid_build_block_later_stages_typed() {
    run_contract(
        "budgets_lowered_mid_build_block_later_stages_typed",
        budgets_lowered_mid_build_block_later_stages_typed_contract,
    );
}

async fn budgets_lowered_mid_build_block_later_stages_typed_contract() {
    let smallest_manifest = smallest_admitted(4 * 1024 * 1024, |bytes| {
        limits(Overrides {
            manifest_bytes: Some(bytes),
            ..Overrides::default()
        })
    });
    let tenant = "a-tenant-whose-partition-outgrows-the-smallest-page";
    let cases = [
        (
            IndexOperationStage::PrepareManifests,
            limits(Overrides {
                manifest_bytes: Some(smallest_manifest),
                ..Overrides::default()
            }),
        ),
        (
            IndexOperationStage::ScanPartitions,
            limits(Overrides {
                input_bytes: Some(64),
                ..Overrides::default()
            }),
        ),
        (
            IndexOperationStage::ScanPartitions,
            limits(Overrides {
                input_bytes: Some(192),
                ..Overrides::default()
            }),
        ),
        (
            IndexOperationStage::ScanPartitions,
            limits(Overrides {
                input_bytes: Some(320),
                ..Overrides::default()
            }),
        ),
    ];
    for (ordinal, (stage, lowered)) in cases.into_iter().enumerate() {
        let lowered = lowered.expect("lowered limits validate");
        let (token, db) = open(
            &format!("queue-text-lowered-budget-{ordinal}"),
            DbConfig::new(),
            LifecycleTestScheduling::Explicit,
        )
        .await;
        let inserted = insert(
            &db,
            TENANT_DOC,
            vec![
                document("alpha bravo", None, Some(tenant)),
                document("alpha charlie", None, Some(tenant)),
            ],
        )
        .await;
        let controller = LifecycleTestController::new();
        let operation_id = create(&db, &controller, text_definition(true)).await;
        drive_to_pause(&db, &controller, operation_id, Pause::Stage(stage)).await;
        db.close().await.expect("scanned fixture closes");

        let db = reopen(
            &token,
            DbConfig::new().with_search_index_backfill_limits(lowered),
            LifecycleTestScheduling::Explicit,
        )
        .await;
        let terminal = drive_to_terminal(&db, &controller, operation_id).await;
        match (blocker(&terminal), stage) {
            (
                Some(IndexOperationBlockerCode::ManifestLimit),
                IndexOperationStage::PrepareManifests,
            )
            | (
                Some(
                    IndexOperationBlockerCode::ManifestLimit
                    | IndexOperationBlockerCode::OversizedEntity,
                ),
                IndexOperationStage::ScanPartitions,
            ) => assert_eq!(terminal.common().stage, stage, "{terminal:?}"),
            (None, IndexOperationStage::ScanPartitions) if ordinal > 1 => {
                assert!(
                    matches!(terminal, IndexOperationStatus::Succeeded { .. }),
                    "{terminal:?}"
                );
                assert_eq!(
                    sorted(
                        search(
                            &db,
                            text("alpha", 10, Some(tenant)),
                            SearchConsistency::Strong
                        )
                        .await
                        .expect("split-scan build serves searches")
                    ),
                    sorted(inserted)
                );
            }
            (code, _) => {
                panic!("lowered budget {ordinal} at {stage:?} ended in {code:?}: {terminal:?}")
            }
        }
        db.close().await.expect("lowered fixture closes");
    }
}

/// A partition scan reads each partition's manifest root before it admits a
/// document, so an input budget smaller than one root blocks the scan with a
/// typed manifest limit: for a tenant partition whose budget was lowered
/// after the source scan, and for the lone root of an empty unpartitioned
/// index. Abort cleanup under that budget cannot admit one of the tenant
/// generation's rows either, so it blocks until the budget is restored and
/// the abort retried; the empty generation has no rows and aborts at once.
#[test]
fn input_budgets_below_one_manifest_root_block_partition_scans() {
    run_contract(
        "input_budgets_below_one_manifest_root_block_partition_scans",
        input_budgets_below_one_manifest_root_block_partition_scans_contract,
    );
}

async fn input_budgets_below_one_manifest_root_block_partition_scans_contract() {
    let one_byte = DbConfig::new().with_search_index_backfill_limits(
        limits(Overrides {
            input_bytes: Some(1),
            ..Overrides::default()
        })
        .expect("a one-byte input budget validates"),
    );
    for tenant in [true, false] {
        let name = format!("queue-text-root-budget-{tenant}");
        let controller = LifecycleTestController::new();
        let (token, db, operation_id) = if tenant {
            let (token, db) = open(&name, DbConfig::new(), LifecycleTestScheduling::Explicit).await;
            insert(
                &db,
                TENANT_DOC,
                vec![document("alpha bravo", None, Some("a"))],
            )
            .await;
            let operation_id = create(&db, &controller, text_definition(true)).await;
            drive_to_pause(
                &db,
                &controller,
                operation_id,
                Pause::Stage(IndexOperationStage::ScanPartitions),
            )
            .await;
            db.close().await.expect("scanned fixture closes");
            let db = reopen(&token, one_byte.clone(), LifecycleTestScheduling::Explicit).await;
            (token, db, operation_id)
        } else {
            let (token, db) =
                open(&name, one_byte.clone(), LifecycleTestScheduling::Explicit).await;
            let operation_id = create(&db, &controller, text_definition(false)).await;
            (token, db, operation_id)
        };
        let terminal = drive_to_terminal(&db, &controller, operation_id).await;
        assert_eq!(
            blocker(&terminal),
            Some(IndexOperationBlockerCode::ManifestLimit),
            "tenant {tenant}: {terminal:?}"
        );
        assert_eq!(
            terminal.common().stage,
            IndexOperationStage::ScanPartitions,
            "tenant {tenant}: {terminal:?}"
        );

        db.abort_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("a blocked build accepts abort");
        let aborting = drive_to_terminal(&db, &controller, operation_id).await;
        if !tenant {
            assert!(
                matches!(aborting, IndexOperationStatus::Aborted { .. }),
                "an empty generation aborts at once: {aborting:?}"
            );
            db.close().await.expect("empty fixture closes");
            continue;
        }
        assert_eq!(
            blocker(&aborting),
            Some(IndexOperationBlockerCode::InvariantViolation),
            "cleanup cannot admit one row: {aborting:?}"
        );
        db.close().await.expect("blocked cleanup fixture closes");
        let db = reopen(&token, DbConfig::new(), LifecycleTestScheduling::Explicit).await;
        db.retry_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("a blocked abort accepts retry");
        let aborted = drive_to_terminal(&db, &controller, operation_id).await;
        assert!(
            matches!(aborted, IndexOperationStatus::Aborted { .. }),
            "restored budget completes the abort: {aborted:?}"
        );
        db.close().await.expect("aborted fixture closes");
    }
}

/// Across transaction operation budgets, documents whose per-document share
/// cannot fit block the build before any of their rows are staged; larger
/// budgets split scans and activate. Queued admission rejects the same
/// oversized document once the index is Active.
#[test]
fn per_document_operation_allowances_block_builds_and_queued_writes() {
    run_contract(
        "per_document_operation_allowances_block_builds_and_queued_writes",
        per_document_operation_allowances_block_builds_and_queued_writes_contract,
    );
}

async fn per_document_operation_allowances_block_builds_and_queued_writes_contract() {
    let smallest = smallest_admitted(1_024, |operations| {
        limits(Overrides {
            output_operations: Some(operations),
            ..Overrides::default()
        })
    });
    let mut outcomes = Vec::new();
    for operations in (smallest..=smallest + 40).step_by(4) {
        let config = DbConfig::new().with_search_index_backfill_limits(
            limits(Overrides {
                output_operations: Some(operations),
                ..Overrides::default()
            })
            .expect("operation limits validate"),
        );
        let (db, inserted, outcome) = build_fixture(
            &format!("queue-text-operation-budget-{operations}"),
            config,
            false,
            vec![
                document("alpha bravo charlie", None, None),
                document("alpha delta echo", None, None),
                document("alpha foxtrot golf", None, None),
            ],
        )
        .await;
        match blocker(&outcome.terminal) {
            None => {
                assert!(
                    matches!(outcome.terminal, IndexOperationStatus::Succeeded { .. }),
                    "{operations}: {:?}",
                    outcome.terminal
                );
                assert_eq!(
                    sorted(
                        search(&db, text("alpha", 10, None), SearchConsistency::Strong)
                            .await
                            .expect("activated search succeeds")
                    ),
                    sorted(inserted),
                    "budget {operations}"
                );
                // Twenty unique terms need a larger per-document operation
                // share than the smaller activating budgets allow.
                let oversized = (0..20)
                    .map(|term| format!("term{term}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                let error = query(
                    &db,
                    QueryRequest::write(
                        batch::write_batch()
                            .var_as(
                                "big",
                                traversal::g().add_n(DOC, document(&oversized, None, None)),
                            )
                            .returning(Vec::<String>::new()),
                    ),
                )
                .await;
                let rejected = error.map_or_else(
                    |error| {
                        assert_eq!(
                            error.error_code(),
                            QueryErrorCode::ActiveTextMutationLimitExceeded,
                            "{error}"
                        );
                        assert!(!error.to_string().is_empty());
                        true
                    },
                    |_| false,
                );
                outcomes.push((operations, "activated", rejected));
            }
            Some(IndexOperationBlockerCode::OversizedEntity) => {
                assert_eq!(outcome.terminal.common().stage, IndexOperationStage::Scan);
                outcomes.push((operations, "oversized", false));
            }
            Some(code) => panic!("{operations}: unexpected blocker {code:?}"),
        }
        db.close().await.expect("fixture closes");
    }
    assert_eq!(outcomes[0].1, "oversized", "{outcomes:?}");
    assert!(
        outcomes
            .iter()
            .any(|(_, outcome, rejected)| *outcome == "activated" && *rejected),
        "some activated index rejects the oversized write: {outcomes:?}"
    );
    assert_eq!(
        outcomes.last().expect("sweep ran").1,
        "activated",
        "{outcomes:?}"
    );
}

// ---------------------------------------------------------------------------
// Damaged builds.
// ---------------------------------------------------------------------------

/// One split per page and one row per step, so every lane walks several rows.
fn paged_config() -> DbConfig {
    DbConfig::new().with_search_index_backfill_limits(
        limits(Overrides {
            entities: Some(1),
            manifest_bytes: Some(one_split_manifest_bytes()),
            fan_in: Some(1),
            ..Overrides::default()
        })
        .expect("paged limits validate"),
    )
}

/// Pauses a two-document build, applies `damage`, and returns the terminal
/// status of the next steps followed by the status after an abort.
async fn damaged_build(
    ordinal: usize,
    pause: Pause,
    damage: TextBuildDamage,
) -> (IndexOperationStatus, IndexOperationStatus) {
    let (_token, db) = open(
        &format!("queue-text-damaged-build-{ordinal}"),
        paged_config(),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    insert(
        &db,
        DOC,
        vec![
            document("alpha bravo", None, None),
            document("alpha charlie", None, None),
        ],
    )
    .await;
    let controller = LifecycleTestController::new();
    let operation_id = create(&db, &controller, text_definition(false)).await;
    drive_to_pause(&db, &controller, operation_id, pause).await;
    let paused = status(&db, operation_id).await;
    if matches!(pause, Pause::Lane(_)) {
        assert_eq!(
            text_manifest_row_counts(&db, paused.common().index_id, paused.common().generation)
                .await,
            (1, 2),
            "each document's split owns one manifest page"
        );
    }
    damage_text_build(
        &db,
        paused.common().index_id,
        paused.common().generation,
        damage,
    )
    .await;
    let terminal = drive_to_terminal(&db, &controller, operation_id).await;
    db.abort_index_operation(DataScope::LegacyUnscoped, operation_id)
        .await
        .expect("a blocked build accepts abort");
    let aborted = drive_to_terminal(&db, &controller, operation_id).await;
    db.close().await.expect("fixture closes");
    (terminal, aborted)
}

/// Every damaged manifest page, root, corpus, entity state, statistics
/// marker, and pre-queue build delta blocks the build as an invariant
/// violation at the lane that reads it, and abort cleanup still converges. A
/// root that already counts every addressable page blocks manifest
/// preparation with a typed manifest limit instead of overflowing.
#[test]
fn damaged_build_rows_block_validation_until_aborted() {
    run_contract(
        "damaged_build_rows_block_validation_until_aborted",
        damaged_build_rows_block_validation_until_aborted_contract,
    );
}

async fn damaged_build_rows_block_validation_until_aborted_contract() {
    use TextBuildDamage::{
        CorpusMissing, EntityStateAhead, EntityStateUndecodable, MarkerAbsent, MarkerForeign,
        MarkerMissing, MarkerUndecodable, PageSplitDuplicated, PageSplitUncounted, PageUndecodable,
        PageZeroMissing, PreQueueDelta, RootEmptied, RootMissing, RootPagesExhausted,
        RootRepartitioned, RootUndecodable,
    };
    use TextManifestValidationLane::{EntityStates, Pages, Roots};
    let cases = [
        (Pause::Lane(Pages), PageUndecodable),
        (Pause::Lane(Pages), RootMissing),
        (Pause::Lane(Pages), RootUndecodable),
        (Pause::Lane(Pages), RootEmptied),
        (Pause::Lane(Pages), RootRepartitioned),
        (Pause::Lane(Pages), PageZeroMissing),
        (Pause::Lane(Pages), PageSplitUncounted),
        (Pause::Lane(Pages), PageSplitDuplicated),
        (Pause::Lane(Pages), PreQueueDelta),
        (Pause::Lane(Roots), RootUndecodable),
        (Pause::Lane(Roots), RootRepartitioned),
        (Pause::Lane(Roots), CorpusMissing),
        (Pause::Lane(Roots), PageZeroMissing),
        (Pause::Lane(EntityStates), EntityStateUndecodable),
        (Pause::Lane(EntityStates), RootMissing),
        (Pause::Lane(EntityStates), RootUndecodable),
        (Pause::Lane(EntityStates), EntityStateAhead),
        (Pause::Lane(EntityStates), MarkerMissing),
        (Pause::Lane(EntityStates), MarkerUndecodable),
        (Pause::Lane(EntityStates), MarkerForeign),
        (Pause::Lane(EntityStates), MarkerAbsent),
        (Pause::Stage(IndexOperationStage::Compact), PreQueueDelta),
        (
            Pause::Stage(IndexOperationStage::PrepareManifests),
            PreQueueDelta,
        ),
        (Pause::Stage(IndexOperationStage::Activate), PreQueueDelta),
    ];
    let exhausted_ordinal = cases.len();
    for (ordinal, (pause, damage)) in cases.into_iter().enumerate() {
        let (terminal, aborted) = damaged_build(ordinal, pause, damage).await;
        assert_eq!(
            blocker(&terminal),
            Some(IndexOperationBlockerCode::InvariantViolation),
            "{pause:?} {damage:?}: {terminal:?}"
        );
        assert!(
            matches!(aborted, IndexOperationStatus::Aborted { .. }),
            "{pause:?} {damage:?} aborts: {aborted:?}"
        );
    }

    let (terminal, aborted) = damaged_build(
        exhausted_ordinal,
        Pause::Stage(IndexOperationStage::PrepareManifests),
        RootPagesExhausted,
    )
    .await;
    assert_eq!(
        blocker(&terminal),
        Some(IndexOperationBlockerCode::ManifestLimit),
        "{terminal:?}"
    );
    assert_eq!(
        terminal.common().stage,
        IndexOperationStage::PrepareManifests
    );
    assert!(
        matches!(aborted, IndexOperationStatus::Aborted { .. }),
        "an exhausted root aborts: {aborted:?}"
    );
}

/// A split object that is missing or whose size disagrees with its manifest
/// blocks validation; restoring the object and retrying activates the build,
/// and later queued publications append pages to it.
#[test]
fn split_object_faults_block_validation_until_restored() {
    run_contract(
        "split_object_faults_block_validation_until_restored",
        split_object_faults_block_validation_until_restored_contract,
    );
}

async fn split_object_faults_block_validation_until_restored_contract() {
    for damage in [
        TextSplitObjectDamage::Resized,
        TextSplitObjectDamage::Missing,
    ] {
        let (_token, db) = open(
            &format!("queue-text-split-object-{damage:?}"),
            paged_config(),
            LifecycleTestScheduling::Explicit,
        )
        .await;
        let inserted = insert(
            &db,
            DOC,
            vec![
                document("alpha bravo", None, None),
                document("alpha charlie", None, None),
            ],
        )
        .await;
        let controller = LifecycleTestController::new();
        let operation_id = create(&db, &controller, text_definition(false)).await;
        drive_to_pause(
            &db,
            &controller,
            operation_id,
            Pause::Lane(TextManifestValidationLane::Pages),
        )
        .await;
        let paused = status(&db, operation_id).await;
        let (index_id, generation) = (paused.common().index_id, paused.common().generation);
        let original = damage_text_split_object(&db, index_id, generation, damage).await;
        let terminal = drive_to_terminal(&db, &controller, operation_id).await;
        assert_eq!(
            blocker(&terminal),
            Some(IndexOperationBlockerCode::InvariantViolation),
            "{damage:?}: {terminal:?}"
        );
        assert_eq!(
            terminal.common().stage,
            IndexOperationStage::ValidateManifests
        );

        // Restoring the exact object and retrying resumes at the same page.
        restore_text_split_object(&db, index_id, generation, original).await;
        db.retry_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("a blocked build accepts retry");
        let terminal = drive_to_terminal(&db, &controller, operation_id).await;
        assert!(
            matches!(terminal, IndexOperationStatus::Succeeded { .. }),
            "{damage:?}: {terminal:?}"
        );
        assert_eq!(
            sorted(
                search(&db, text("alpha", 10, None), SearchConsistency::Strong)
                    .await
                    .expect("restored build serves searches")
            ),
            sorted(inserted.clone())
        );
        // Every page already holds its one split, so a queued publication
        // appends the next contiguous page.
        let (_, pages_before) = text_manifest_row_counts(&db, index_id, generation).await;
        let mut expected = inserted;
        expected.extend(insert(&db, DOC, vec![document("alpha delta", None, None)]).await);
        publish(&db).await;
        let (_, pages_after) = text_manifest_row_counts(&db, index_id, generation).await;
        assert_eq!(pages_after, pages_before + 1, "{damage:?}");
        assert_eq!(
            sorted(
                search(&db, text("alpha", 10, None), SearchConsistency::Strong)
                    .await
                    .expect("appended page serves searches")
            ),
            sorted(expected)
        );
        db.close().await.expect("fixture closes");
    }
}

// ---------------------------------------------------------------------------
// Strong text search bound.
// ---------------------------------------------------------------------------

/// Text publications analyze at most this much; the strong bound is separate.
const PUBLICATION_ANALYSIS_BYTES: u64 = 64 * 1024;

/// A writer whose text publications analyze at most
/// [`PUBLICATION_ANALYSIS_BYTES`] and whose strong text searches analyze at
/// most `strong` bytes, or the default bound.
fn strong_text_bound_config(strong: Option<u64>) -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let compaction = defaults.text_compaction();
    let limits = SearchIndexBackfillLimits::try_new(
        defaults.batch(),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        TextBackfillCompactionLimits::new(
            compaction.max_fan_in(),
            NonZeroU64::new(PUBLICATION_ANALYSIS_BYTES).expect("positive"),
            compaction.max_temporary_disk_bytes(),
            compaction.max_output_blob_bytes(),
            NonZeroU64::new(8 * 1024).expect("positive"),
        ),
    )
    .expect("publication analysis limits validate");
    let tuning = strong.map_or_else(IndexOperationQueueTuning::default, |bytes| {
        IndexOperationQueueTuning::default().with_strong_text_search_max_analysis_bytes(
            NonZeroU64::new(bytes).expect("positive bound"),
        )
    });
    DbConfig::new()
        .with_search_index_backfill_limits(limits)
        .with_index_operation_queue_tuning(tuning)
}

/// Requires `error` to be retryable strong text bound backpressure at `limit`.
fn assert_past_the_strong_text_bound(error: &HelixDbError, limit: u64, context: &str) {
    assert!(
        matches!(
            error,
            HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::PendingTextAnalysisBytes,
                requested,
                limit: refused,
                ..
            } if *refused == limit && *requested > limit
        ),
        "{context}: {error}"
    );
    assert!(error.is_index_backpressure(), "{context}");
    assert_eq!(error.error_code(), QueryErrorCode::IndexBackpressure);
    assert!(
        error.to_string().contains("pending_text_analysis_bytes"),
        "{context}: {error}"
    );
}

/// Strong text searches include every unpublished document of their
/// partition up to their own bound, far past what one text publication
/// analyzes. Past it, read and write requests fail with retryable
/// `pending_text_analysis_bytes` backpressure, which transports classify as
/// backpressure and the queue stats count, until publication; eventual
/// searches keep their publication-sized overlay, and a write's own text
/// alone past the bound fails it without retry.
#[test]
fn strong_text_searches_reach_their_own_bound_and_fail_past_it_until_publication() {
    run_contract(
        "strong_text_searches_reach_their_own_bound_and_fail_past_it_until_publication",
        strong_text_searches_reach_their_own_bound_and_fail_past_it_until_publication_contract,
    );
}

async fn strong_text_searches_reach_their_own_bound_and_fail_past_it_until_publication_contract() {
    const DOCS: usize = 40;
    // About 4.4 KB of analysis each: 40 are 2.7 times one publication's.
    const BOUND: u64 = 100 * 1024;
    let body = format!("alpha{}", " ".repeat(4 * 1024 - "alpha".len()));
    let documents = |count: usize| {
        (0..count)
            .map(|_| document(&body, None, None))
            .collect::<Vec<_>>()
    };
    let (token, db) = open(
        "queue-text-strong-bound",
        strong_text_bound_config(None),
        LifecycleTestScheduling::Explicit,
    )
    .await;
    let controller = LifecycleTestController::new();
    build(&db, &controller, text_definition(false)).await;
    let inserted = sorted(insert(&db, DOC, documents(DOCS)).await);
    let strong = search(&db, text("alpha", DOCS, None), SearchConsistency::Strong)
        .await
        .expect("the default bound covers the backlog");
    assert_eq!(
        sorted(strong),
        inserted,
        "exact past the publication budget"
    );
    let eventual = search(&db, text("alpha", DOCS, None), SearchConsistency::Eventual)
        .await
        .expect("eventual searches never fail for backlog");
    assert!(
        !eventual.is_empty() && eventual.len() < DOCS,
        "eventual overlays one publication's analysis: {eventual:?}"
    );
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        0
    );
    db.close().await.expect("fixture closes");

    let db = Arc::new(
        reopen(
            &token,
            strong_text_bound_config(Some(BOUND)),
            LifecycleTestScheduling::Explicit,
        )
        .await,
    );
    let error = search(&db, text("alpha", 5, None), SearchConsistency::Strong)
        .await
        .expect_err("the backlog exceeds the lowered bound");
    assert_past_the_strong_text_bound(&error, BOUND, "read");
    let error = HelixQueryService::new(Arc::clone(&db))
        .execute_query(read(text("alpha", 5, None), SearchConsistency::Strong))
        .await
        .expect_err("still past the bound");
    assert_eq!(error.classify(), QueryFailureClass::Backpressure);
    assert_eq!(
        search(&db, text("alpha", DOCS, None), SearchConsistency::Eventual)
            .await
            .expect("eventual searches keep their own budget"),
        eventual
    );
    // A write's search sees the same committed backlog and rolls it back.
    let error = query(
        &db,
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(DOC, document("alpha", None, None)),
                )
                .var_as("ids", text("alpha", 5, None).id())
                .returning(["ids"]),
        ),
    )
    .await
    .expect_err("a write's strong search never analyzes past the bound");
    assert_past_the_strong_text_bound(&error, BOUND, "write");
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        3
    );

    assert_eq!(publish(&db).await, DOCS as u64);
    assert_eq!(
        sorted(
            search(&db, text("alpha", DOCS, None), SearchConsistency::Strong)
                .await
                .expect("nothing is pending")
        ),
        inserted,
        "the rejected write rolled back"
    );
    // A write's own text alone past the bound can never be searched.
    let own = documents(30).into_iter().enumerate().fold(
        batch::write_batch(),
        |write, (ordinal, properties)| {
            write.var_as(
                &format!("d{ordinal}"),
                traversal::g().add_n(DOC, properties),
            )
        },
    );
    let error = query(
        &db,
        QueryRequest::write(
            own.var_as("ids", text("alpha", 5, None).id())
                .returning(["ids"]),
        ),
    )
    .await
    .expect_err("a write's own text past the bound fails it");
    assert!(
        matches!(
            error,
            HelixDbError::IndexOperationBatchTooLarge {
                resource: db::error::IndexOperationBatchResource::PendingTextAnalysisBytes,
                limit,
                ..
            } if limit == BOUND
        ),
        "{error}"
    );
    assert!(!error.is_index_backpressure());
    assert_eq!(
        db.index_operation_queue_stats()
            .strong_text_search_rejections,
        3,
        "a write's own text is not counted"
    );
    db.close().await.expect("fixture closes");
}
