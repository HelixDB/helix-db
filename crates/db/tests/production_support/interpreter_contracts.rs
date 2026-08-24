//! Production-linked interpreter contracts that require controlled internal state.
//!
//! The feature-gated integration target compiles the unchanged interpreter
//! without `cfg(test)`. These fixtures retain deterministic control over a
//! transaction conflict and request-read ownership while asserting only
//! durable rows and typed outcomes.

use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use helix_ast::index::RangeIndexDirection;
use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::{catalog, context, exec, ir};
use roaring::RoaringTreemap;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbReadOps, IsolationLevel};

use super::*;
use crate::config::{DbConfig, TextIndexDefinition};
use crate::encoding::indexes::label::{EdgeLabelKey, EdgeLabelNeighborKey};
use crate::encoding::indexes::{
    hash_property_name, hash_property_value, EdgeDirection, PropertyIndexKey,
};
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    AdjacencyKey, DataKey, DataKeyKind, EdgeEndpointsKey, EdgePairIndexKey, NodePropertyKey,
};
use crate::encoding::v2::values as index_values;
use crate::encoding::v2::values::indexes::vector::{ActiveScoreSemantic, VectorEntityKind};
use crate::encoding::v2::values::{adjacency as edges, indexes as secondary};
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
    IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
};
use crate::search::vector::distance::{Cosine, Distance, Manhattan};
use crate::search::vector::{
    DistanceScore, RestrictedVectorCandidates, SearchParams, SearchResult, TypedVectorSearchResult,
    VectorIndex, VectorIndexConfig,
};
use crate::{HelixDbSource, ProcessLocalDatabaseToken};

/// Borrows the writer storage owned by one production interpreter fixture.
fn writer_db(db: &crate::HelixDB) -> &Db {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("production interpreter fixture must be a writer");
    };
    writer.db()
}

/// Proves planning catalog authority transfers through write-open and that an
/// invalidated volatile proof takes the locked refresh path.
pub async fn run_catalog_write_open_authority_contracts() {
    let db = Arc::new(
        crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "production-catalog-write-open-authority".to_owned(),
        })
        .await
        .expect("catalog write-open fixture opens"),
    );
    let scope = DataScope::LegacyUnscoped;
    let prepared = db
        .planner_context_scoped_prepared(context::ParamBindings::default(), scope)
        .await
        .expect("planning catalog authority prepares");
    let mut execution = ExecutionContext::new_scoped_controlled_with_catalog_freshness(
        db.as_ref(),
        context::ParamBindings::default(),
        scope,
        crate::execution_control::ExecutionControl::unlimited(),
        runtime_context::PendingCatalogFreshness::Prepared(prepared.into_catalog_proof()),
    );

    let catalog_change = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            let permit = db
                .inner
                .index_scope_gates
                .catalog_change_permit(scope)
                .await;
            drop(permit);
        })
    };
    tokio::task::yield_now().await;
    assert!(
        !catalog_change.is_finished(),
        "planning authority excludes catalog changes until write-open"
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        execution.enable_request_write_scope(),
    )
    .await
    .expect("prepared write-open does not deadlock")
    .expect("prepared write-open succeeds");
    tokio::time::timeout(std::time::Duration::from_secs(5), catalog_change)
        .await
        .expect("write-open releases catalog authority")
        .expect("catalog-change waiter joins");

    let lifecycle = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            let permit = db.inner.index_scope_gates.lifecycle_permit(scope).await;
            drop(permit);
        })
    };
    tokio::task::yield_now().await;
    assert!(
        !lifecycle.is_finished(),
        "opened mutation view excludes lifecycle publication"
    );
    execution.abort_request_write_scope();
    tokio::time::timeout(std::time::Duration::from_secs(5), lifecycle)
        .await
        .expect("dropping the write view releases mutation authority")
        .expect("lifecycle waiter joins");
    drop(execution);

    let prepared = db
        .planner_context_scoped_prepared(context::ParamBindings::default(), scope)
        .await
        .expect("second planning catalog authority prepares");
    db.refresh_runtime_catalog(scope)
        .await
        .expect("overlapping refresh invalidates the volatile proof generation");
    let mut invalidated = ExecutionContext::new_scoped_controlled_with_catalog_freshness(
        db.as_ref(),
        context::ParamBindings::default(),
        scope,
        crate::execution_control::ExecutionControl::unlimited(),
        runtime_context::PendingCatalogFreshness::Prepared(prepared.into_catalog_proof()),
    );
    invalidated
        .enable_request_write_scope()
        .await
        .expect("invalidated proof reloads while holding catalog authority");
    invalidated.abort_request_write_scope();
    invalidated.discard_pending_catalog_freshness();
    drop(invalidated);
    db.close().await.expect("catalog write-open fixture closes");
}

/// Proves closed request-mode composition and the operation-owned mutation path.
pub async fn run_request_mode_and_isolated_mutation_contracts() {
    assert_eq!(
        RequestSideEffects::IndexDdl.combine(RequestSideEffects::IndexDdl),
        RequestSideEffects::IndexDdl
    );
    for (left, right) in [
        (
            RequestSideEffects::GraphMutation,
            RequestSideEffects::IndexDdl,
        ),
        (
            RequestSideEffects::IndexDdl,
            RequestSideEffects::GraphMutation,
        ),
        (RequestSideEffects::Mixed, RequestSideEffects::GraphMutation),
        (RequestSideEffects::GraphMutation, RequestSideEffects::Mixed),
    ] {
        assert_eq!(left.combine(right), RequestSideEffects::Mixed);
    }

    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "production-isolated-mutation-scope".to_owned(),
    })
    .await
    .expect("isolated mutation fixture opens");
    let mut execution = ExecutionContext::new_scoped(
        &db,
        context::ParamBindings::default(),
        DataScope::LegacyUnscoped,
    );
    let created = execution
        .execute_mutation(
            ExecutionValue::Stream(Vec::new()),
            &exec::ExecMutationPlan::AddNodeSource {
                label: ir::NonEmptyString::new("Isolated").expect("label is non-empty"),
                properties: ir::PropertyAssignments::try_from_vec(Vec::new())
                    .expect("empty assignments are valid"),
            },
        )
        .await
        .expect("operation-owned mutation commits");
    assert_eq!(
        created,
        ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(0))])
    );
    let node_key = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(0)),
    }
    .to_bytes();
    let stored = writer_db(&db)
        .get(node_key)
        .await
        .expect("isolated node reads")
        .expect("isolated node is durable before the context closes");
    assert_eq!(
        crate::encoding::v2::values::property::decode_properties(&stored)
            .expect("isolated node properties decode"),
        vec![Property::string("$label", "Isolated")]
    );
    drop(execution);
    db.close().await.expect("isolated mutation fixture closes");
}

/// Encodes one unscoped V2 logical key through the canonical V1 boundary.
fn scoped_key(logical: index_keys::ScopedKey) -> Bytes {
    index_keys::ManagedIndexKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: logical,
    }
    .to_bytes()
}

/// Captures every scoped V2 key/value so conflicts cannot hide lane-specific writes.
async fn scoped_v2_snapshot(db: &Db) -> Vec<(Bytes, Bytes)> {
    let prefix = index_keys::ManagedIndexKey::data_prefix(
        DataScope::LegacyUnscoped,
        Bytes::from(vec![index_keys::ScopedKey::key_prefix()]),
    );
    let mut rows = db
        .scan_prefix(&prefix, ..)
        .await
        .expect("scoped V2 scan succeeds");
    let mut snapshot = Vec::new();
    while let Some(row) = rows.next().await.expect("scoped V2 row reads") {
        snapshot.push((row.key, row.value));
    }
    snapshot
}

/// Lists only immutable text blobs, excluding SlateDB's own object-store rows.
async fn text_blob_paths(object_store: &Arc<dyn ObjectStore>) -> BTreeSet<String> {
    let mut rows = object_store.list(None);
    let mut paths = BTreeSet::new();
    while let Some(row) = rows.next().await {
        let row = row.expect("text blob listing succeeds");
        let path = row.location.to_string();
        if path.contains("/fts/blobs/") {
            paths.insert(path);
        }
    }
    paths
}

async fn exercise_stable_restricted_metric<D: Distance>(db: &Db, name: &str) {
    let index = VectorIndex::<D>::new(name);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &transaction,
            VectorIndexConfig::new(index.name(), "embedding", 2)
                .with_m(2)
                .with_m0(4)
                .with_ef_construction(8),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    for (node_id, vector) in [
        (1, [1.0, 0.0]),
        (2, [0.9, 0.1]),
        (3, [0.0, 1.0]),
        (4, [-1.0, 0.0]),
    ] {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.insert(&transaction, node_id, &vector).await.unwrap();
        transaction.commit().await.unwrap();
    }

    let view = read_view::StableRequestReadView::WriterTransaction(
        db.begin(IsolationLevel::Snapshot).await.unwrap(),
    );
    let exact = RestrictedVectorCandidates::from_ids([1, 2, 3]).unwrap();
    let exact_results = index
        .search_restricted(&view, &[1.0, 0.0], &SearchParams::new(2).unwrap(), &exact)
        .await
        .unwrap();
    assert_eq!(exact_results.len(), 2);

    let filtered = RestrictedVectorCandidates::from_ids(1..=257).unwrap();
    let filtered_results = index
        .search_restricted(
            &view,
            &[1.0, 0.0],
            &SearchParams::new(2).unwrap(),
            &filtered,
        )
        .await
        .unwrap();
    assert!(!filtered_results.is_empty());
    assert!(filtered_results
        .iter()
        .all(|result| filtered.contains(result.entity_id())));

    let prefix = Bytes::new();
    let mut rows = view.scan_prefix(prefix.clone(), ..).await.unwrap();
    assert!(rows.next().await.unwrap().is_some());
    let mut rows = view
        .scan_prefix(
            &prefix,
            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
        )
        .await
        .unwrap();
    assert!(rows.next().await.unwrap().is_some());
}

async fn run_stable_vector_read_contracts() {
    let db = Db::open(
        "production-interpreter-stable-vector-reads",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    exercise_stable_restricted_metric::<Cosine>(&db, "stable-vector-cosine").await;
    exercise_stable_restricted_metric::<Manhattan>(&db, "stable-vector-manhattan").await;

    let snapshot = db.snapshot().await.unwrap();
    let view = read_view::StableRequestReadView::ReaderSnapshot(snapshot);
    let mut rows = view.scan_prefix(Bytes::new(), ..).await.unwrap();
    assert!(rows.next().await.unwrap().is_some());
    drop(rows);
    drop(view);
    db.close().await.unwrap();
}

fn interpreter_vector_result(kind: VectorEntityKind, entity_id: u64) -> TypedVectorSearchResult {
    TypedVectorSearchResult::from_physical(
        kind,
        ActiveScoreSemantic::ManhattanF32V1,
        SearchResult::new(entity_id, DistanceScore::try_new(0.25).unwrap()),
    )
}

/// Exercises executable-value shapes, dependency composition, and typed row dispatch.
async fn run_value_dependency_and_row_contracts() {
    let db = crate::HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-value-dependency-rows".to_string(),
    })
    .await
    .expect("interpreter value fixture opens");
    let raw = writer_db(&db);
    let transaction = raw.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(7)),
            }
            .to_bytes(),
            crate::encoding::v2::values::property::encode_properties(&[Property::string(
                "$label", "Document",
            )]),
        )
        .unwrap();
    transaction
        .put(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(9)),
            }
            .to_bytes(),
            Bytes::from_static(b"present"),
        )
        .unwrap();
    transaction.commit().await.unwrap();

    let mut context = ExecutionContext::new_scoped(
        &db,
        context::ParamBindings::default(),
        DataScope::LegacyUnscoped,
    );
    context.enable_request_read_view().await.unwrap();

    assert_eq!(
        interpreter_vector_result(VectorEntityKind::Node, 7)
            .entity_id()
            .local_id(),
        7
    );
    assert_eq!(
        interpreter_vector_result(VectorEntityKind::Edge, 9)
            .entity_id()
            .local_id(),
        9
    );

    let step = |id| exec::ExecStepId::new(id).unwrap();
    let missing = context.dependency_values(&[step(7)]).unwrap_err();
    assert!(missing
        .to_string()
        .contains("dependency step 7 has not executed"));
    context.step_outputs.insert(
        step(1),
        ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(7))]),
    );
    context.step_outputs.insert(
        step(2),
        ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(9))]),
    );
    assert_eq!(
        context.dependency_input(&[step(1), step(2)]).unwrap().len(),
        2
    );
    context
        .step_outputs
        .insert(step(3), ExecutionValue::Count(2));
    context
        .step_outputs
        .insert(step(4), ExecutionValue::Bool(true));
    context.step_outputs.insert(
        step(5),
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(7)]),
    );
    assert_eq!(
        context
            .dependency_input(&[step(3), step(4), step(5)])
            .unwrap()
            .len(),
        3
    );
    assert!(context
        .dependency_input(&[step(1), step(3)])
        .unwrap_err()
        .to_string()
        .contains("mixed stream and scalar"));
    context.step_outputs.insert(
        step(6),
        ExecutionValue::FoldedStream(FoldedStream::new(vec![ExecutionRow::empty()])),
    );
    assert!(context
        .dependency_input(&[step(6), step(6)])
        .unwrap_err()
        .to_string()
        .contains("folded stream"));
    context.step_outputs.insert(
        step(8),
        ExecutionValue::IndexDdlReceipt(
            crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
                operation_id: IndexOperationId::from_bytes([8; 16]).unwrap(),
            },
        ),
    );
    assert!(context
        .dependency_input(&[step(8), step(8)])
        .unwrap_err()
        .to_string()
        .contains("index lifecycle"));

    assert_eq!(ExecutionValue::Count(3).len(), 3);
    assert_eq!(ExecutionValue::Bool(true).len(), 1);
    assert!(ExecutionValue::Bool(false).is_empty());
    assert_eq!(
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(7), ExecutionScalar::EdgeId(9),])
            .len(),
        2
    );
    assert_eq!(
        ExecutionValue::IndexDdlReceipt(
            crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
                operation_id: IndexOperationId::from_bytes([9; 16]).unwrap(),
            }
        )
        .len(),
        1
    );

    let empty_virtual = RowVirtualProperties::empty();
    let populated_virtual = RowVirtualProperties::from_one(
        ir::NonEmptyString::new("score").unwrap(),
        DbPropertyValue::I64(7),
    );
    assert_eq!(
        empty_virtual.partial_cmp(&populated_virtual),
        Some(empty_virtual.cmp(&populated_virtual))
    );
    let mut empty_sack = ExecutionRow::empty();
    let mut populated_sack = ExecutionRow::empty();
    populated_sack.set_sack(DbPropertyValue::I64(7));
    populated_sack = populated_sack.mark_sack_visible();
    assert_eq!(
        empty_sack.sack.partial_cmp(&populated_sack.sack),
        Some(empty_sack.sack.cmp(&populated_sack.sack))
    );
    empty_sack.bindings.insert(
        ir::NonEmptyString::new("bound").unwrap(),
        ElementRef::Node(7),
    );
    empty_sack.path_visible = true;
    let distinct = context
        .distinct(ExecutionValue::Stream(vec![
            empty_sack.clone(),
            empty_sack,
            populated_sack,
        ]))
        .unwrap();
    assert_eq!(distinct.len(), 2);

    assert_eq!(context.node_rows(vec![u64::MAX, 7]).await.unwrap().len(), 1);
    assert_eq!(context.edge_rows(vec![u64::MAX, 9]).await.unwrap().len(), 1);
    assert_eq!(
        context
            .node_search_rows(vec![
                interpreter_vector_result(VectorEntityKind::Node, 7),
                interpreter_vector_result(VectorEntityKind::Node, u64::MAX),
            ])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        context
            .edge_search_rows(vec![
                interpreter_vector_result(VectorEntityKind::Edge, 9),
                interpreter_vector_result(VectorEntityKind::Edge, u64::MAX),
            ])
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(context
        .node_search_rows(vec![interpreter_vector_result(VectorEntityKind::Edge, 9,)])
        .await
        .is_err());
    assert!(context
        .edge_search_rows(vec![interpreter_vector_result(VectorEntityKind::Node, 7,)])
        .await
        .is_err());

    let label = ir::NonEmptyString::new("$label").unwrap();
    let empty_row = ExecutionRow::empty();
    assert_eq!(
        context.row_property(&empty_row, &label).await.unwrap(),
        None
    );
    assert!(context.row_properties(&empty_row).await.unwrap().is_empty());
    let missing_node = ExecutionRow::current(ElementRef::Node(u64::MAX));
    assert_eq!(
        context.row_property(&missing_node, &label).await.unwrap(),
        None
    );
    assert!(context
        .row_properties(&missing_node)
        .await
        .unwrap()
        .is_empty());
    let missing_edge = ExecutionRow::current(ElementRef::Edge(u64::MAX));
    for property in ["$from", "$to", "$from.$id", "$to.$label"] {
        let property = ir::NonEmptyString::new(property).unwrap();
        assert_eq!(
            context
                .row_property(&missing_edge, &property)
                .await
                .unwrap(),
            None
        );
    }

    for aggregate in [
        ir::AggregatePlan::Group(label.clone()),
        ir::AggregatePlan::GroupCount(label.clone()),
        ir::AggregatePlan::AggregateBy {
            function: helix_ast::traversal::AggregateFunction::Count,
            property: label,
        },
    ] {
        assert!(context
            .aggregate(
                ExecutionValue::Stream(vec![ExecutionRow::empty()]),
                &aggregate,
            )
            .await
            .is_err());
    }

    for plan in [
        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan),
        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::AllScan),
    ] {
        assert_eq!(context.execute_access(&plan).await.unwrap().len(), 1);
    }

    context.close_request_read_view().unwrap();
    db.close().await.expect("interpreter value fixture closes");
}

/// Exercises scheduler fan-out/fan-in transfer and row projection through the
/// unchanged production interpreter. This lives outside the measured source
/// tree so the contract adds no test-harness lines to the production metric.
pub(crate) async fn run_scheduler_and_projection_contracts() {
    let db = test_support::open_db("production-scheduler-transfer-contracts").await;
    let id = |value| exec::ExecStepId::new(value).expect("positive step ID");
    let first = test_support::step(1, Vec::new(), exec::ExecOp::Noop);
    let second = test_support::step(2, vec![id(1)], exec::ExecOp::Noop);
    let third = test_support::step(3, vec![id(1)], exec::ExecOp::Noop);
    let root = test_support::step(4, vec![id(2), id(3)], exec::ExecOp::Noop);
    let plan = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::from_one_and_rest(first, vec![second, third, root]),
        id(4),
        helix_planner::trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("fan-out/fan-in plan validates");
    assert!(matches!(
        &plan.execution_order().stages()[1],
        exec::ExecExecutionStage::Parallel(_)
    ));
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context
        .execute_steps(plan.steps(), plan.execution_order(), plan.root())
        .await
        .expect("fan-out/fan-in plan executes");
    assert!(context.step_outputs.get(&id(4)).is_some());

    let ready = (1..=5)
        .map(|step_id| test_support::step(step_id, Vec::new(), exec::ExecOp::Noop))
        .collect::<Vec<_>>();
    let root = test_support::step(6, (1..=5).map(id).collect(), exec::ExecOp::Noop);
    let mut steps = ready;
    steps.push(root);
    let plan = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("six scheduler steps are non-empty"),
        id(6),
        helix_planner::trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("wide scheduler plan validates");
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context
        .execute_steps(plan.steps(), plan.execution_order(), plan.root())
        .await
        .expect("wide scheduler plan executes");
    assert!(context.step_outputs.get(&id(6)).is_some());
    db.close().await.expect("scheduler fixture closes");

    let db = test_support::open_db("production-row-projection-contracts").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", AstPropertyValue::from("ada"))],
    )
    .await;
    let bob = test_support::add_user(&db, "bob").await;
    let edge = test_support::add_edge(&db, ada, bob, "KNOWS").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context
        .enable_request_read_view()
        .await
        .expect("projection request view opens");

    let rows = || {
        vec![
            ExecutionRow::current(ElementRef::Node(ada)),
            ExecutionRow::empty(),
            ExecutionRow::current(ElementRef::Edge(edge)),
        ]
    };
    assert_eq!(
        context
            .project(ExecutionValue::Stream(rows()), &ir::ProjectionPlan::Id)
            .await
            .expect("ID projection executes")
            .len(),
        2
    );
    let missing = ir::PropertyNames::new(ir::AtLeast::<_, 1>::from_one(
        ir::NonEmptyString::new("missing").expect("property is non-empty"),
    ))
    .expect("one property is unique");
    assert!(context
        .project(
            ExecutionValue::Stream(rows()),
            &ir::ProjectionPlan::Values(missing),
        )
        .await
        .expect("missing-value projection executes")
        .is_empty());
    assert_eq!(
        context
            .project(
                ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(ada))]),
                &ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
            )
            .await
            .expect("value-map projection executes")
            .len(),
        1
    );
    let virtual_label = ExecutionRow::current_with_virtual_properties(
        ElementRef::Node(u64::MAX),
        RowVirtualProperties::from_one(
            ir::NonEmptyString::new("$label").expect("label key is non-empty"),
            DbPropertyValue::String("Virtual".to_string()),
        ),
    );
    assert_eq!(
        context
            .project(
                ExecutionValue::Stream(vec![
                    ExecutionRow::current(ElementRef::Node(ada)),
                    virtual_label,
                ]),
                &ir::ProjectionPlan::Label,
            )
            .await
            .expect("label projection executes")
            .len(),
        2
    );
    assert_eq!(
        context
            .project(
                ExecutionValue::Stream(vec![
                    ExecutionRow::current(ElementRef::Node(ada)),
                    ExecutionRow::current(ElementRef::Edge(u64::MAX)),
                    ExecutionRow::current(ElementRef::Edge(edge)),
                ]),
                &ir::ProjectionPlan::EdgeProperties,
            )
            .await
            .expect("edge-property projection executes")
            .len(),
        1
    );
    context
        .close_request_read_view()
        .expect("projection request view closes");
    db.close().await.expect("projection fixture closes");
}

/// Seeds one canonical Active text generation before the runtime opens.
async fn seed_active_text_generation(
    database: &str,
    object_store: Arc<dyn ObjectStore>,
    definition: &TextIndexDefinition,
) -> index_keys::TextManifestRootKey {
    seed_active_text_generation_with(
        database,
        object_store,
        definition,
        IndexId::initial(),
        crate::index_lifecycle::TextManifestRevision::initial(),
        true,
    )
    .await
    .expect("the unpartitioned fixture seeds a manifest root")
}

async fn seed_active_text_generation_with(
    database: &str,
    object_store: Arc<dyn ObjectStore>,
    definition: &TextIndexDefinition,
    index_id: IndexId,
    revision: crate::index_lifecycle::TextManifestRevision,
    seed_unpartitioned_root: bool,
) -> Option<index_keys::TextManifestRootKey> {
    let raw = Db::builder(database, object_store)
        .build()
        .await
        .expect("raw Active-text fixture opens");
    crate::index_lifecycle::repository::bootstrap_writer(&raw)
        .await
        .expect("raw Active-text fixture bootstraps");
    let active = IndexRecordV2::building(
        index_id,
        ValidatedDynamicIndexDefinition::try_from(definition.clone())
            .expect("text definition validates for V2"),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([0x91; 16]).expect("operation ID is non-nil"),
    )
    .expect("building text record validates")
    .transition(IndexStateTransition::Activate)
    .expect("text record activates");
    let root = seed_unpartitioned_root.then_some(index_keys::TextManifestRootKey {
        index_id,
        generation: IndexGenerationId::initial(),
        partition: crate::index_lifecycle::work::TextPartition::Unpartitioned.fingerprint(),
    });
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("Active-text seed transaction opens");
    transaction
        .put(
            scoped_key(index_keys::ScopedKey::index_record(
                active.identity().clone(),
            )),
            index_values::encode_index_record(&active),
        )
        .expect("Active text record stages");
    if let Some(root) = root {
        transaction
            .put(
                scoped_key(index_keys::ScopedKey::TextManifestRoot(root)),
                index_values::encode_manifest_root(
                    &crate::index_lifecycle::work::TextManifestRootValue::try_new(
                        index_id,
                        IndexGenerationId::initial(),
                        crate::index_lifecycle::work::TextPartition::Unpartitioned,
                        revision,
                        0,
                        0,
                    )
                    .expect("an empty manifest accepts any live revision"),
                ),
            )
            .expect("empty Active manifest stages");
    }
    transaction
        .commit()
        .await
        .expect("Active-text fixture commits");
    raw.close().await.expect("raw Active-text fixture closes");
    root
}

/// Runs a real serializable graph conflict through Active-text resolution.
pub(crate) async fn run_active_text_graph_conflict() {
    let database = "production-interpreter-active-text-conflict";
    let token = ProcessLocalDatabaseToken::new(database).expect("process-local token validates");
    let object_store = token.object_store();
    let definition = TextIndexDefinition::new_node("Document", "body")
        .expect("Active text definition validates");
    let root = seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
    let db =
        crate::HelixDB::open_with_config(HelixDbSource::InMemoryToken { token }, DbConfig::new())
            .await
            .expect("Active-text production fixture opens");
    let v2_before = scoped_v2_snapshot(writer_db(&db)).await;
    let blobs_before = text_blob_paths(&object_store).await;
    let mut execution = ExecutionContext::new_scoped(
        &db,
        context::ParamBindings::default(),
        DataScope::LegacyUnscoped,
    );
    execution
        .enable_request_write_scope()
        .await
        .expect("request write scope opens");
    let created = execution
        .execute_mutation(
            ExecutionValue::Stream(Vec::new()),
            &exec::ExecMutationPlan::AddNodeSource {
                label: ir::NonEmptyString::new("Document").expect("label is non-empty"),
                properties: ir::PropertyAssignments::try_from_vec(vec![(
                    ir::NonEmptyString::new("body").expect("property is non-empty"),
                    ir::PropertyInputPlan::Value(AstPropertyValue::from(
                        "losing production Active-text mutation",
                    )),
                )])
                .expect("property assignment validates"),
            },
        )
        .await
        .expect("Active-text mutation stages");
    assert_eq!(
        created,
        ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(0))])
    );
    execution
        .flush_active_index_mutations()
        .await
        .expect("the explicit read barrier publishes the pending text epoch");
    let blobs_after_upload = text_blob_paths(&object_store).await;
    let uploaded_orphans = blobs_after_upload
        .difference(&blobs_before)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        uploaded_orphans.len(),
        1,
        "the prepared Active mutation uploads exactly one immutable blob"
    );

    let graph_key = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(0)),
    }
    .to_bytes();
    let competing_properties =
        crate::encoding::v2::values::property::encode_properties(&[Property::string(
            "$label",
            "Competing",
        )]);
    let competing = writer_db(&db)
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("competing graph transaction opens");
    competing
        .put(&graph_key, competing_properties.clone())
        .expect("competing graph row stages");
    competing
        .commit()
        .await
        .expect("competing graph row commits");

    let error = execution
        .commit_request_write_scope()
        .await
        .expect_err("losing graph transaction conflicts");
    assert!(error.is_transaction_conflict());
    assert_eq!(
        writer_db(&db)
            .get(&graph_key)
            .await
            .expect("competing graph row reads"),
        Some(competing_properties)
    );
    let root_value = writer_db(&db)
        .get(scoped_key(index_keys::ScopedKey::TextManifestRoot(root)))
        .await
        .expect("Active manifest root reads")
        .expect("Active manifest root remains present");
    let root =
        index_values::decode_manifest_root(&root_value).expect("Active manifest root decodes");
    assert!(root.page_count() == 0 && root.split_count() == 0);
    assert_eq!(
        scoped_v2_snapshot(writer_db(&db)).await,
        v2_before,
        "the losing transaction must not change any scoped Index V2 lane"
    );
    db.close()
        .await
        .expect("Active-text production fixture closes");
    assert_eq!(
        text_blob_paths(&object_store).await,
        blobs_after_upload,
        "database close must not delete the conflict orphan"
    );
    assert!(uploaded_orphans.is_subset(&blobs_after_upload));
}

/// Proves internal storage and index reads fail closed without a request view.
pub(crate) async fn run_request_read_view_guards() {
    let empty = FoldedStream::new(Vec::new());
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    let populated = FoldedStream::new(vec![ExecutionRow::current(ElementRef::Node(1))]);
    assert!(!populated.is_empty());
    assert_eq!(populated.len(), 1);

    let db = crate::HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-request-view-guards".to_string(),
    })
    .await
    .expect("request-view production fixture opens");
    let _interpreter = Interpreter::new(&db, context::ParamBindings::default());
    let mut context = ExecutionContext::new_scoped(
        &db,
        context::ParamBindings::default(),
        DataScope::LegacyUnscoped,
    );
    let key = Bytes::from_static(b"guard-key");

    for (error, expected) in [
        (
            context
                .get_raw(&key)
                .await
                .expect_err("point reads require a request view"),
            "storage read escaped its request read view",
        ),
        (
            context
                .multi_get_raw(std::slice::from_ref(&key))
                .await
                .expect_err("multi-gets require a request view"),
            "storage multi-get escaped its request read view",
        ),
        (
            context
                .scan_raw_range_limited(
                    Bytes::from_static(b"guard-range-start"),
                    Bytes::from_static(b"guard-range-end"),
                    None,
                )
                .await
                .expect_err("range scans require a request view"),
            "storage range scan escaped its request read view",
        ),
        (
            context
                .scan_raw_prefix_limited(Bytes::from_static(b"guard-prefix"), None)
                .await
                .expect_err("prefix scans require a request view"),
            "storage prefix scan escaped its request read view",
        ),
    ] {
        let HelixDbError::InvariantViolation(message) = error else {
            panic!("request-view guard returned the wrong error: {error}");
        };
        assert_eq!(message, expected);
    }

    let property_key =
        catalog::ScopedPropertyKey::try_new("Document", "rank").expect("equality key validates");
    let property_value = ir::IndexValue::Literal(
        ir::SecondaryIndexLiteral::new(AstPropertyValue::from(7_i64))
            .expect("equality value validates"),
    );
    let range_key =
        catalog::ScopedPropertyDirectionKey::try_new("Document", "rank", RangeIndexDirection::Asc)
            .expect("range key validates");
    for (plan, expected) in [
        (
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::exact_equality(
                catalog::NodeEqualityIndexMeta::new(
                    ir::NonEmptyString::new("node_eq:Document:rank")
                        .expect("index ID is non-empty"),
                ),
                property_key.clone(),
                property_value.clone(),
            )),
            "exact secondary equality point read escaped its request read view",
        ),
        (
            exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("FOLLOWS").expect("label is non-empty"),
            }),
            "global edge label lookup escaped its request read view",
        ),
        (
            exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::exact_equality(
                catalog::EdgeEqualityIndexMeta::new(
                    ir::NonEmptyString::new("edge_eq:Document:rank")
                        .expect("index ID is non-empty"),
                ),
                property_key,
                property_value,
            )),
            "exact secondary equality point read escaped its request read view",
        ),
        (
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::RangeIndex {
                index: catalog::NodeRangeIndexMeta::new(
                    ir::NonEmptyString::new("guard-node-range").expect("index ID is non-empty"),
                ),
                key: range_key.clone(),
                range: ir::IndexRange::All,
            }),
            "node secondary range lookup escaped its request read view",
        ),
        (
            exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::RangeIndex {
                index: catalog::EdgeRangeIndexMeta::new(
                    ir::NonEmptyString::new("guard-edge-range").expect("index ID is non-empty"),
                ),
                key: range_key,
                range: ir::IndexRange::All,
            }),
            "edge secondary range lookup escaped its request read view",
        ),
    ] {
        let error = context
            .execute_access(&plan)
            .await
            .expect_err("index reads require a request view");
        let HelixDbError::InvariantViolation(message) = error else {
            panic!("request-view guard returned the wrong error: {error}");
        };
        assert_eq!(message, expected);
    }

    for (error, expected) in [
        (
            context
                .lookup_out_neighbors_by_label(1, "FOLLOWS")
                .await
                .expect_err("out-neighbor reads require a request view"),
            "out-neighbor label lookup escaped its request read view",
        ),
        (
            context
                .lookup_in_neighbors_by_label(1, "FOLLOWS")
                .await
                .expect_err("in-neighbor reads require a request view"),
            "in-neighbor label lookup escaped its request read view",
        ),
        (
            context
                .get_edge_endpoints(1)
                .await
                .expect_err("endpoint reads require a request view"),
            "edge endpoint lookup escaped its request read view",
        ),
    ] {
        let HelixDbError::InvariantViolation(message) = error else {
            panic!("request-view guard returned the wrong error: {error}");
        };
        assert_eq!(message, expected);
    }
    db.close()
        .await
        .expect("request-view production fixture closes");
    run_value_dependency_and_row_contracts().await;
    run_stable_vector_read_contracts().await;
}

fn topology_node_label_key(scope: DataScope, label: &str) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
            crate::encoding::indexes::equality::EqualityIndexKey::new(
                hash_property_name("$label"),
                hash_property_value(label),
            ),
        )),
    }
    .to_bytes()
}

fn topology_edge_pair_key(scope: DataScope, from: u64, to: u64) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(from, to)),
    }
    .to_bytes()
}

fn topology_adjacency_key(scope: DataScope, node: u64) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
    }
    .to_bytes()
}

fn topology_edge_label_neighbor_key(
    scope: DataScope,
    direction: EdgeDirection,
    node: u64,
    label: &str,
) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(direction, node, hash_property_value(label)),
        )),
    }
    .to_bytes()
}

fn topology_global_edge_label_key(scope: DataScope, label: &str) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
            hash_property_value(label),
        ))),
    }
    .to_bytes()
}

fn topology_bitmap(ids: impl IntoIterator<Item = u64>) -> Bytes {
    secondary::SecondaryEqualityValue::encode_ids(&ids.into_iter().collect::<RoaringTreemap>())
}

async fn topology_bitmap_ids(transaction: &slatedb::DbTransaction, key: &Bytes) -> Vec<u64> {
    transaction
        .get(key)
        .await
        .expect("topology bitmap reads")
        .map(|value| {
            secondary::SecondaryEqualityValue::decode(&value)
                .expect("topology bitmap decodes")
                .into_ids()
                .iter()
                .collect()
        })
        .unwrap_or_default()
}

async fn topology_contract_db(name: &str) -> Db {
    Db::builder(
        format!("production-topology-mutation-contract/{name}"),
        Arc::new(InMemory::new()),
    )
    .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
    .build()
    .await
    .expect("topology contract database opens")
}

/// Exercises the production topology collector without compiling its unit-test cfg.
pub(crate) async fn run_topology_mutation_contracts() {
    use helix_planner::ir::ExpandDirection;

    use super::mutation::topology::TopologyMutationRuntime;

    let scope = DataScope::LegacyUnscoped;
    let db = topology_contract_db("order-overlay-and-multigraph").await;
    let label_key = topology_node_label_key(scope, "Person");
    let emptied_label_key = topology_node_label_key(scope, "Removed");
    let pair_key = topology_edge_pair_key(scope, 1, 2);
    let seeded_adjacency_key = topology_adjacency_key(scope, 7);
    let snapshot_only_key = topology_node_label_key(scope, "SnapshotOnly");
    let seed = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("topology seed transaction opens");
    seed.put(&label_key, topology_bitmap([1, 2, 3]))
        .expect("node-label baseline stages");
    seed.put(&emptied_label_key, topology_bitmap([99]))
        .expect("empty-row baseline stages");
    seed.put(&pair_key, topology_bitmap([10, 11]))
        .expect("edge-pair baseline stages");
    seed.put(&snapshot_only_key, topology_bitmap([42]))
        .expect("snapshot-only baseline stages");
    let mut seeded_adjacency = edges::Edges::new();
    seeded_adjacency.add_out(8);
    seeded_adjacency.add_in(6);
    seed.put(
        &seeded_adjacency_key,
        edges::encode_edges(&seeded_adjacency),
    )
    .expect("adjacency baseline stages");
    seed.commit().await.expect("topology baseline commits");

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("topology mutation transaction opens");
    let mut runtime = TopologyMutationRuntime::default();
    runtime.remove_node_label(scope, "Person", 1).unwrap();
    runtime.add_node_label(scope, "Person", 1).unwrap();
    runtime.add_node_label(scope, "Person", 2).unwrap();
    runtime.remove_node_label(scope, "Person", 2).unwrap();
    runtime.remove_node_label(scope, "Person", 3).unwrap();
    runtime.add_node_label(scope, "Person", 4).unwrap();
    runtime.add_node_label(scope, "Person", 5).unwrap();
    runtime.remove_node_label(scope, "Person", 5).unwrap();
    runtime.remove_node_label(scope, "Person", 6).unwrap();
    runtime.add_node_label(scope, "Person", 6).unwrap();
    runtime.remove_node_label(scope, "Removed", 99).unwrap();
    runtime.remove_edge_pair(scope, 1, 2, 10).unwrap();
    runtime.add_edge_pair(scope, 1, 2, 12).unwrap();
    runtime.add_edge_pair(scope, 2, 3, 20).unwrap();
    runtime.add_edge_pair(scope, 2, 3, 21).unwrap();
    runtime
        .remove_adjacency(scope, 7, 8, ExpandDirection::Out)
        .unwrap();
    runtime
        .add_adjacency(scope, 7, 9, ExpandDirection::Out)
        .unwrap();
    runtime
        .remove_adjacency(scope, 7, 6, ExpandDirection::In)
        .unwrap();
    runtime
        .add_adjacency(scope, 7, 5, ExpandDirection::In)
        .unwrap();
    runtime
        .add_adjacency(scope, 7, 7, ExpandDirection::Both)
        .unwrap();
    runtime.add_edge_label(scope, 1, 2, "LINK", 10).unwrap();
    runtime.add_edge_label(scope, 1, 2, "LINK", 11).unwrap();
    runtime.add_edge_label(scope, 1, 1, "LINK", 12).unwrap();
    runtime.flush(&transaction).await.unwrap();

    assert_eq!(
        topology_bitmap_ids(&transaction, &label_key).await,
        vec![1, 4, 6]
    );
    assert!(transaction.get(&emptied_label_key).await.unwrap().is_none());
    assert_eq!(
        topology_bitmap_ids(&transaction, &pair_key).await,
        vec![11, 12]
    );
    assert_eq!(
        topology_bitmap_ids(&transaction, &topology_edge_pair_key(scope, 2, 3)).await,
        vec![20, 21]
    );
    assert_eq!(
        topology_bitmap_ids(
            &transaction,
            &topology_edge_label_neighbor_key(scope, EdgeDirection::Out, 1, "LINK"),
        )
        .await,
        vec![1, 2]
    );
    assert_eq!(
        topology_bitmap_ids(
            &transaction,
            &topology_edge_label_neighbor_key(scope, EdgeDirection::In, 2, "LINK"),
        )
        .await,
        vec![1]
    );
    assert_eq!(
        topology_bitmap_ids(&transaction, &topology_global_edge_label_key(scope, "LINK")).await,
        vec![10, 11, 12]
    );
    let adjacency = edges::decode_edges(
        &transaction
            .get(&seeded_adjacency_key)
            .await
            .unwrap()
            .expect("coalesced adjacency exists"),
    )
    .unwrap();
    assert_eq!(adjacency.iter_out().collect::<Vec<_>>(), vec![7, 9]);
    assert_eq!(adjacency.iter_in().collect::<Vec<_>>(), vec![5, 7]);

    runtime.remove_edge_pair(scope, 1, 2, 11).unwrap();
    runtime.remove_edge_pair(scope, 1, 2, 12).unwrap();
    runtime.remove_global_edge_label(scope, "LINK", 10).unwrap();
    runtime
        .remove_edge_label_neighbors(scope, 1, 2, "LINK")
        .unwrap();
    runtime
        .remove_adjacency(scope, 7, 7, ExpandDirection::Both)
        .unwrap();
    runtime
        .remove_adjacency(scope, 7, 9, ExpandDirection::Out)
        .unwrap();
    runtime
        .add_adjacency(scope, 7, 10, ExpandDirection::Out)
        .unwrap();
    runtime.flush(&transaction).await.unwrap();

    assert!(transaction.get(&pair_key).await.unwrap().is_none());
    assert_eq!(
        topology_bitmap_ids(
            &transaction,
            &topology_edge_label_neighbor_key(scope, EdgeDirection::Out, 1, "LINK"),
        )
        .await,
        vec![1]
    );
    assert_eq!(
        topology_bitmap_ids(&transaction, &topology_global_edge_label_key(scope, "LINK")).await,
        vec![11, 12]
    );
    let adjacency = edges::decode_edges(
        &transaction
            .get(&seeded_adjacency_key)
            .await
            .unwrap()
            .expect("second adjacency epoch exists"),
    )
    .unwrap();
    assert_eq!(adjacency.iter_out().collect::<Vec<_>>(), vec![10]);
    assert_eq!(adjacency.iter_in().collect::<Vec<_>>(), vec![5]);

    let observed = runtime
        .observe(
            &transaction,
            &[
                label_key.clone(),
                snapshot_only_key.clone(),
                label_key.clone(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        observed[0].as_ref().map(Bytes::as_ref),
        observed[2].as_ref().map(Bytes::as_ref)
    );
    assert!(observed[0].is_some());
    assert_eq!(
        secondary::SecondaryEqualityValue::decode(observed[1].as_ref().unwrap())
            .unwrap()
            .into_ids()
            .iter()
            .collect::<Vec<_>>(),
        vec![42]
    );

    runtime.prepare(&transaction).await.unwrap();
    assert!(runtime.flush(&transaction).await.is_err());
    assert!(runtime.add_node_label(scope, "Person", 7).is_err());
    runtime.consume_prepared().unwrap();
    transaction.commit().await.unwrap();
    db.close().await.unwrap();

    let db = topology_contract_db("invalid-state-and-rollback").await;
    assert!(TopologyMutationRuntime::default()
        .consume_prepared()
        .is_err());
    let mut pending = TopologyMutationRuntime::default();
    pending.add_node_label(scope, "Pending", 1).unwrap();
    assert!(pending.consume_prepared().is_err());
    let rollback = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let rollback_key = topology_node_label_key(scope, "Rollback");
    let mut rollback_runtime = TopologyMutationRuntime::default();
    rollback_runtime
        .add_node_label(scope, "Rollback", 1)
        .unwrap();
    rollback_runtime.flush(&rollback).await.unwrap();
    drop(rollback);
    assert!(db.get(&rollback_key).await.unwrap().is_none());

    let missing = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let missing_label_key = topology_node_label_key(scope, "Missing");
    let missing_adjacency_key = topology_adjacency_key(scope, 404);
    let mut missing_runtime = TopologyMutationRuntime::default();
    assert!(missing_runtime
        .observe(&missing, &[])
        .await
        .unwrap()
        .is_empty());
    missing_runtime
        .remove_node_label(scope, "Missing", 1)
        .unwrap();
    missing_runtime
        .remove_adjacency(scope, 404, 405, ExpandDirection::Both)
        .unwrap();
    missing_runtime.flush(&missing).await.unwrap();
    assert!(missing.get(&missing_label_key).await.unwrap().is_none());
    assert!(missing.get(&missing_adjacency_key).await.unwrap().is_none());
    missing_runtime.prepare(&missing).await.unwrap();
    missing_runtime.consume_prepared().unwrap();
    missing.commit().await.unwrap();

    for (key, corrupt, mutate_adjacency) in [
        (
            topology_node_label_key(scope, "Corrupt"),
            Bytes::from_static(b"not-a-bitmap"),
            false,
        ),
        (
            topology_adjacency_key(scope, 88),
            Bytes::from_static(b"not-adjacency"),
            true,
        ),
    ] {
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        seed.put(&key, corrupt.clone()).unwrap();
        seed.commit().await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut runtime = TopologyMutationRuntime::default();
        if mutate_adjacency {
            runtime
                .remove_adjacency(scope, 88, 1, ExpandDirection::Out)
                .unwrap();
        } else {
            runtime.remove_node_label(scope, "Corrupt", 1).unwrap();
        }
        assert!(runtime.flush(&transaction).await.is_err());
        drop(transaction);
        assert_eq!(db.get(&key).await.unwrap(), Some(corrupt));
    }
    db.close().await.unwrap();

    let db = topology_contract_db("serializable-conflict").await;
    let conflict_key = topology_node_label_key(scope, "Conflict");
    let seed = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    seed.put(&conflict_key, topology_bitmap([1])).unwrap();
    seed.commit().await.unwrap();
    let losing = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut losing_runtime = TopologyMutationRuntime::default();
    losing_runtime
        .remove_node_label(scope, "Conflict", 1)
        .unwrap();
    losing_runtime.flush(&losing).await.unwrap();
    let winning = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    winning.put(&conflict_key, topology_bitmap([1, 2])).unwrap();
    winning.commit().await.unwrap();
    losing_runtime.prepare(&losing).await.unwrap();
    losing_runtime.consume_prepared().unwrap();
    assert!(losing.commit().await.is_err());
    assert_eq!(
        secondary::SecondaryEqualityValue::decode(
            &db.get(&conflict_key)
                .await
                .unwrap()
                .expect("winner remains")
        )
        .unwrap()
        .into_ids()
        .iter()
        .collect::<Vec<_>>(),
        vec![1, 2]
    );
    db.close().await.unwrap();
}

#[cfg(feature = "index-lifecycle-testing")]
mod text_transaction_benchmark {
    use std::fmt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use futures::stream::BoxStream;
    use helix_planner::{cost, properties, trace};
    use slatedb::object_store::local::LocalFileSystem;
    use slatedb::object_store::path::Path;
    use slatedb::object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    };

    use super::*;
    use crate::config::{CacheConfig, CacheMode, TextElementType, VectorMemorySettings};
    use crate::index_lifecycle_testing::LifecycleTestScheduling;
    use crate::search::text_index_name;

    const LABEL: &str = "TextTransactionBenchmarkDocument";
    const PROPERTY: &str = "body";
    const QUERY: &str = "transactionbatchtoken";
    const QUIESCENCE_POLL: Duration = Duration::from_millis(25);
    const QUIESCENCE_STABLE: Duration = Duration::from_millis(750);
    const QUIESCENCE_TIMEOUT: Duration = Duration::from_secs(120);

    /// One deterministic text-transaction benchmark case.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TextTransactionBatchBenchmarkCase {
        /// Inserts committed by one transaction.
        pub batch_size: usize,
        /// Synthetic delay applied to each FTS blob upload.
        pub upload_latency_millis: u64,
    }

    impl TextTransactionBatchBenchmarkCase {
        /// Validates one non-empty benchmark case.
        pub fn try_new(batch_size: usize, upload_latency_millis: u64) -> Result<Self> {
            if batch_size == 0 {
                return Err(HelixDbError::Config(
                    "text transaction benchmark batch size must be positive".to_string(),
                ));
            }
            Ok(Self {
                batch_size,
                upload_latency_millis,
            })
        }
    }

    /// Measurements from one clean local-store benchmark sample.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TextTransactionBatchBenchmarkSample {
        /// Executed benchmark case.
        pub case: TextTransactionBatchBenchmarkCase,
        /// End-to-end transaction latency.
        pub transaction_latency_ns: u64,
        /// FTS immutable blob uploads performed by the transaction.
        pub upload_count: u64,
        /// FTS immutable payload bytes uploaded by the transaction.
        pub upload_bytes: u64,
        /// Manifest split-count growth caused by the transaction.
        pub manifest_split_growth: u64,
        /// Cold immediate search latency while compaction is held.
        pub immediate_search_latency_ns: u64,
        /// Cold search latency after automatic compaction reaches quiescence.
        pub post_compaction_search_latency_ns: u64,
        /// Split count after compaction reaches quiescence.
        pub post_compaction_split_count: u64,
        /// Stable digest of the immediate and post-compaction result IDs.
        pub search_digest: String,
    }

    #[derive(Debug)]
    struct MeasuredObjectStore {
        inner: Arc<dyn ObjectStore>,
        upload_latency: Duration,
        fts_puts: AtomicU64,
        fts_bytes: AtomicU64,
    }

    impl MeasuredObjectStore {
        fn new(inner: Arc<dyn ObjectStore>, upload_latency: Duration) -> Self {
            Self {
                inner,
                upload_latency,
                fts_puts: AtomicU64::new(0),
                fts_bytes: AtomicU64::new(0),
            }
        }

        fn reset_fts_uploads(&self) {
            self.fts_puts.store(0, Ordering::Relaxed);
            self.fts_bytes.store(0, Ordering::Relaxed);
        }

        fn fts_uploads(&self) -> (u64, u64) {
            (
                self.fts_puts.load(Ordering::Relaxed),
                self.fts_bytes.load(Ordering::Relaxed),
            )
        }

        fn is_fts_blob(location: &Path) -> bool {
            location.as_ref().contains("/fts/blobs/")
        }
    }

    impl fmt::Display for MeasuredObjectStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("text-transaction-benchmark-local-store")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MeasuredObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if Self::is_fts_blob(location) {
                if !self.upload_latency.is_zero() {
                    tokio::time::sleep(self.upload_latency).await;
                }
                self.fts_puts.fetch_add(1, Ordering::Relaxed);
                self.fts_bytes.fetch_add(
                    u64::try_from(payload.content_length()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn benchmark_config() -> DbConfig {
        DbConfig::new().with_cache(CacheConfig::new(
            VectorMemorySettings::default(),
            CacheMode::VectorMemoryOnly,
        ))
    }

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).expect("benchmark identifier is non-empty")
    }

    fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
        exec::ExecStep {
            id: exec::ExecStepId::new(id).expect("benchmark step IDs are positive"),
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

    fn executable(
        kind: ir::PlanKind,
        steps: Vec<exec::ExecStep>,
        root: usize,
    ) -> exec::ExecutablePlan {
        exec::ExecutablePlan::new(
            kind,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::try_from_vec(steps).expect("benchmark plan is non-empty"),
            exec::ExecStepId::new(root).expect("benchmark root ID is positive"),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("benchmark dependencies form a valid executable plan")
    }

    fn insert_plan(batch_size: usize) -> exec::ExecutablePlan {
        let mut steps = Vec::with_capacity(batch_size);
        for offset in 0..batch_size {
            let id = offset + 1;
            let dependencies = (id > 1)
                .then(|| exec::ExecStepId::new(id - 1).expect("previous step ID is positive"))
                .into_iter()
                .collect();
            let properties = ir::PropertyAssignments::try_from_vec(vec![(
                name(PROPERTY),
                ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                    "{QUERY} document {offset:04}"
                ))),
            )])
            .expect("benchmark property assignment validates");
            steps.push(step(
                id,
                dependencies,
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: name(LABEL),
                        properties,
                    },
                },
            ));
        }
        executable(ir::PlanKind::Write, steps, batch_size)
    }

    fn dual_index_insert_plan(batch_size: usize) -> exec::ExecutablePlan {
        let mut steps = Vec::with_capacity(batch_size);
        for offset in 0..batch_size {
            let id = offset + 1;
            let dependencies = (id > 1)
                .then(|| exec::ExecStepId::new(id - 1).expect("previous step ID is positive"))
                .into_iter()
                .collect();
            let properties = ir::PropertyAssignments::try_from_vec(vec![
                (
                    name(PROPERTY),
                    ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                        "{QUERY} body {offset:04}"
                    ))),
                ),
                (
                    name("title"),
                    ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                        "{QUERY} title {offset:04}"
                    ))),
                ),
            ])
            .expect("dual-index assignments validate");
            steps.push(step(
                id,
                dependencies,
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: name(LABEL),
                        properties,
                    },
                },
            ));
        }
        executable(ir::PlanKind::Write, steps, batch_size)
    }

    fn partitioned_insert_plan() -> exec::ExecutablePlan {
        let mut steps = Vec::with_capacity(2);
        for (offset, tenant) in ["acme", "globex"].into_iter().enumerate() {
            let id = offset + 1;
            let dependencies = (id > 1)
                .then(|| exec::ExecStepId::new(id - 1).expect("previous step ID is positive"))
                .into_iter()
                .collect();
            let properties = ir::PropertyAssignments::try_from_vec(vec![
                (
                    name(PROPERTY),
                    ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                        "{QUERY} tenant {tenant}"
                    ))),
                ),
                (
                    name("tenant"),
                    ir::PropertyInputPlan::Value(AstPropertyValue::from(tenant)),
                ),
            ])
            .expect("partitioned assignments validate");
            steps.push(step(
                id,
                dependencies,
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: name(LABEL),
                        properties,
                    },
                },
            ));
        }
        executable(ir::PlanKind::Write, steps, 2)
    }

    fn delete_all_plan() -> exec::ExecutablePlan {
        let access = exec::ExecStepId::new(1).expect("access step ID is positive");
        executable(
            ir::PlanKind::Write,
            vec![
                step(
                    1,
                    Vec::new(),
                    exec::ExecOp::Access {
                        plan: Box::new(exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::AllScan,
                        )),
                    },
                ),
                step(
                    2,
                    vec![access],
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::Drop,
                    },
                ),
            ],
            2,
        )
    }

    fn create_then_delete_plan() -> exec::ExecutablePlan {
        let created = exec::ExecStepId::new(1).expect("create step ID is positive");
        executable(
            ir::PlanKind::Write,
            vec![
                step(
                    1,
                    Vec::new(),
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::AddNodeSource {
                            label: name(LABEL),
                            properties: ir::PropertyAssignments::try_from_vec(vec![(
                                name(PROPERTY),
                                ir::PropertyInputPlan::Value(AstPropertyValue::from(
                                    "create then delete",
                                )),
                            )])
                            .expect("create/delete properties validate"),
                        },
                    },
                ),
                step(
                    2,
                    vec![created],
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::Drop,
                    },
                ),
            ],
            2,
        )
    }

    fn access_and_mutate_plan(mutations: Vec<exec::ExecMutationPlan>) -> exec::ExecutablePlan {
        let mut steps = Vec::with_capacity(mutations.len() + 1);
        steps.push(step(
            1,
            Vec::new(),
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::FromParam {
                        param: name("target"),
                    },
                )),
            },
        ));
        for (offset, plan) in mutations.into_iter().enumerate() {
            let id = offset + 2;
            steps.push(step(
                id,
                vec![exec::ExecStepId::new(id - 1).expect("previous step ID is positive")],
                exec::ExecOp::Mutation { plan },
            ));
        }
        let root = steps.len();
        executable(ir::PlanKind::Write, steps, root)
    }

    fn set_body(value: &str) -> exec::ExecMutationPlan {
        set_property(PROPERTY, value)
    }

    fn set_property(property: &str, value: &str) -> exec::ExecMutationPlan {
        exec::ExecMutationPlan::SetProperty {
            name: name(property),
            value: ir::PropertyInputPlan::Value(AstPropertyValue::from(value)),
        }
    }

    fn write_read_write_plan() -> exec::ExecutablePlan {
        let first = exec::ExecStepId::new(1).expect("first step ID is positive");
        let read = exec::ExecStepId::new(2).expect("read step ID is positive");
        let properties = |suffix: &str| {
            ir::PropertyAssignments::try_from_vec(vec![(
                name(PROPERTY),
                ir::PropertyInputPlan::Value(AstPropertyValue::from(format!("{QUERY} {suffix}"))),
            )])
            .expect("barrier properties validate")
        };
        executable(
            ir::PlanKind::Write,
            vec![
                step(
                    1,
                    Vec::new(),
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::AddNodeSource {
                            label: name(LABEL),
                            properties: properties("before read"),
                        },
                    },
                ),
                step(
                    2,
                    vec![first],
                    exec::ExecOp::Access {
                        plan: Box::new(exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::TextSearch {
                                key: catalog::NodeSearchIndexKey::try_new(LABEL, PROPERTY)
                                    .expect("barrier search key validates"),
                                index: ir::SearchIndexPlan {
                                    index_id: name(&text_index_name(
                                        TextElementType::Node,
                                        LABEL,
                                        PROPERTY,
                                    )),
                                    tenant: ir::SearchTenantPlan::Unscoped,
                                },
                                query_text: ir::TextQueryInputPlan::Text(name(QUERY)),
                                k: ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::MIN),
                            },
                        )),
                    },
                ),
                step(
                    3,
                    vec![read],
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::AddNodeSource {
                            label: name(LABEL),
                            properties: properties("after read"),
                        },
                    },
                ),
            ],
            3,
        )
    }

    fn search_plan_for(query: &str, batch_size: usize) -> exec::ExecutablePlan {
        let access_id = exec::ExecStepId::new(1).expect("benchmark access ID is positive");
        executable(
            ir::PlanKind::Read,
            vec![
                step(
                    1,
                    Vec::new(),
                    exec::ExecOp::Access {
                        plan: Box::new(exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::TextSearch {
                                key: catalog::NodeSearchIndexKey::try_new(LABEL, PROPERTY)
                                    .expect("benchmark text key validates"),
                                index: ir::SearchIndexPlan {
                                    index_id: name(&text_index_name(
                                        TextElementType::Node,
                                        LABEL,
                                        PROPERTY,
                                    )),
                                    tenant: ir::SearchTenantPlan::Unscoped,
                                },
                                query_text: ir::TextQueryInputPlan::Text(name(query)),
                                k: ir::SearchLimitPlan::Literal(
                                    std::num::NonZeroUsize::new(batch_size)
                                        .expect("benchmark result limit is positive"),
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

    async fn manifest_split_count(db: &Db, root: index_keys::TextManifestRootKey) -> Result<u64> {
        let bytes = db
            .get(scoped_key(index_keys::ScopedKey::TextManifestRoot(root)))
            .await?
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "text transaction benchmark manifest root disappeared".to_string(),
                )
            })?;
        let value = index_values::decode_manifest_root(&bytes)?;
        Ok(value.split_count())
    }

    async fn manifest_root(
        db: &Db,
        root: index_keys::TextManifestRootKey,
    ) -> Result<crate::index_lifecycle::work::TextManifestRootValue> {
        let bytes = db
            .get(scoped_key(index_keys::ScopedKey::TextManifestRoot(root)))
            .await?
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "text transaction contract manifest root disappeared".to_string(),
                )
            })?;
        let value = index_values::decode_manifest_root(&bytes)?;
        Ok(value)
    }

    async fn entity_state(
        db: &Db,
        root: index_keys::TextManifestRootKey,
        entity_id: u64,
    ) -> Result<crate::index_lifecycle::work::TextEntityStateValue> {
        let key = scoped_key(index_keys::ScopedKey::TextEntityState(
            index_keys::TextEntityStateKey {
                root,
                entity: index_keys::IndexEntity {
                    kind: crate::index_lifecycle::IndexElementKind::Node,
                    id: crate::index_lifecycle::IndexEntityId::new(entity_id),
                },
            },
        ));
        let bytes = db.get(key).await?.ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "text transaction contract entity state disappeared".to_string(),
            )
        })?;
        let state = index_values::decode_text_entity_state(&bytes)?;
        Ok(state)
    }

    async fn compaction_pointer_revision(
        db: &Db,
        definition: &TextIndexDefinition,
        root: index_keys::TextManifestRootKey,
        page: u32,
    ) -> Result<u64> {
        let identity = ValidatedDynamicIndexDefinition::try_from(definition.clone())
            .expect("text definition validates for V2")
            .identity();
        let target = index_keys::TextCompactionTarget::try_new(
            DataScope::LegacyUnscoped,
            identity,
            root.index_id,
            root.generation,
            root.partition,
            page,
        )?;
        let key = index_keys::ManagedIndexKey::Global {
            kind: index_keys::GlobalKey::TextCompactionPointer(target),
        }
        .to_bytes();
        let bytes = db.get(key).await?.ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "text transaction contract compaction pointer disappeared".to_string(),
            )
        })?;
        let crate::index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(pointer) =
            index_values::decode_metadata_value(&bytes)?
        else {
            return Err(HelixDbError::InvariantViolation(
                "text transaction contract pointer key contains another value kind".to_string(),
            ));
        };
        Ok(pointer.revision.get())
    }

    fn tenant_root(tenant: &str) -> index_keys::TextManifestRootKey {
        let tenant_value = Property::string("tenant", tenant).value;
        let partition = crate::index_lifecycle::work::TextPartition::try_tenant_value(
            crate::encoding::v2::values::property::encode_index_partition_value(&tenant_value),
        )
        .expect("tenant partition validates");
        index_keys::TextManifestRootKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: partition.fingerprint(),
        }
    }

    fn projected_ids(result: ExecutionResult) -> Result<Vec<u64>> {
        let Some(ExecutionValue::Scalars(values)) = result.last else {
            return Err(HelixDbError::InvariantViolation(
                "text transaction benchmark projection returned another value shape".to_string(),
            ));
        };
        values
            .into_iter()
            .map(|value| match value {
                ExecutionScalar::NodeId(id) => Ok(id),
                ExecutionScalar::EdgeId(_)
                | ExecutionScalar::String(_)
                | ExecutionScalar::Value(_)
                | ExecutionScalar::Object(_) => Err(HelixDbError::InvariantViolation(
                    "text transaction benchmark projection returned a non-node ID".to_string(),
                )),
            })
            .collect()
    }

    async fn search(db: &crate::HelixDB, batch_size: usize) -> Result<(u64, Vec<u64>)> {
        search_for(db, QUERY, batch_size).await
    }

    async fn search_for(
        db: &crate::HelixDB,
        query: &str,
        batch_size: usize,
    ) -> Result<(u64, Vec<u64>)> {
        let started = Instant::now();
        let result = db
            .execute(
                &search_plan_for(query, batch_size),
                context::ParamBindings::default(),
            )
            .await?;
        let latency = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let mut ids = projected_ids(result)?;
        ids.sort_unstable();
        Ok((latency, ids))
    }

    fn digest(ids: &[u64]) -> String {
        let mut bytes = Vec::with_capacity(core::mem::size_of_val(ids));
        for id in ids {
            bytes.extend_from_slice(&id.to_be_bytes());
        }
        format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&bytes))
    }

    async fn wait_for_manifest_quiescence(
        db: &crate::HelixDB,
        store: &MeasuredObjectStore,
        root: index_keys::TextManifestRootKey,
    ) -> Result<u64> {
        let started = Instant::now();
        let mut observation = (
            manifest_split_count(writer_db(db), root).await?,
            store.fts_uploads().0,
        );
        let mut stable_since = Instant::now();
        loop {
            tokio::time::sleep(QUIESCENCE_POLL).await;
            let next = (
                manifest_split_count(writer_db(db), root).await?,
                store.fts_uploads().0,
            );
            if next != observation {
                observation = next;
                stable_since = Instant::now();
            } else if stable_since.elapsed() >= QUIESCENCE_STABLE {
                return Ok(observation.0);
            }
            if started.elapsed() >= QUIESCENCE_TIMEOUT {
                return Err(HelixDbError::Config(
                    "text transaction benchmark compaction did not quiesce".to_string(),
                ));
            }
        }
    }

    /// Proves destination batching, delete-only publication, versions, partitions, and barriers.
    pub async fn run_text_transaction_batching_contracts() {
        let temporary = tempfile::tempdir().expect("batching contract directory creates");
        let local: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temporary.path())
                .expect("batching contract store opens"),
        );
        let measured = Arc::new(MeasuredObjectStore::new(local, Duration::ZERO));
        let object_store: Arc<dyn ObjectStore> = measured.clone();
        let database = "text-transaction-batching-contract";
        let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("batching text definition validates");
        let root =
            seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
        let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
            database,
            Arc::clone(&object_store),
            benchmark_config(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("batching contract database opens");

        measured.reset_fts_uploads();
        db.execute(&insert_plan(100), context::ParamBindings::default())
            .await
            .expect("one hundred inserts commit");
        assert_eq!(measured.fts_uploads().0, 1);
        let inserted_root = manifest_root(writer_db(&db), root)
            .await
            .expect("inserted root reads");
        assert_eq!(inserted_root.split_count(), 1);
        for entity_id in 0..100 {
            let state = entity_state(writer_db(&db), root, entity_id)
                .await
                .expect("batched live state reads");
            assert!(state.live);
            assert_eq!(state.logical_version.get(), inserted_root.revision().get());
        }
        let page_key = scoped_key(index_keys::ScopedKey::TextManifestPage(
            index_keys::TextManifestPageKey { root, page: 0 },
        ));
        let page_bytes = writer_db(&db)
            .get(page_key)
            .await
            .expect("batched manifest page reads")
            .expect("batched manifest page exists");
        let page = index_values::decode_manifest_page(&page_bytes).expect("batched page decodes");
        assert_eq!(page.entries().len(), 1);
        assert!(page.entries()[0]
            .pruning()
            .may_match_any([QUERY.as_bytes()]));
        let (_, inserted_ids) = search(&db, 100)
            .await
            .expect("batched inserts are searchable");
        assert_eq!(inserted_ids.len(), 100);

        measured.reset_fts_uploads();
        db.execute(
            &access_and_mutate_plan(vec![
                set_body("intermediateonlytoken"),
                set_body(&format!("{QUERY} finalonlytoken")),
            ]),
            context::ParamBindings::default().with_value(name("target"), AstPropertyValue::I64(0)),
        )
        .await
        .expect("two updates for one entity coalesce");
        assert_eq!(measured.fts_uploads().0, 1);
        let updated_root = manifest_root(writer_db(&db), root)
            .await
            .expect("updated root reads");
        assert_eq!(updated_root.split_count(), 2);
        assert_eq!(
            updated_root.revision().get(),
            inserted_root.revision().get() + 1
        );
        assert!(search_for(&db, "intermediateonlytoken", 100)
            .await
            .expect("intermediate update results read")
            .1
            .is_empty());
        assert_eq!(
            search_for(&db, "finalonlytoken", 100)
                .await
                .expect("final update results read")
                .1,
            vec![0]
        );

        measured.reset_fts_uploads();
        db.execute(
            &create_then_delete_plan(),
            context::ParamBindings::default(),
        )
        .await
        .expect("create/delete net-zero epoch commits");
        assert_eq!(measured.fts_uploads().0, 0);
        assert_eq!(
            manifest_root(writer_db(&db), root)
                .await
                .expect("net-zero root reads"),
            updated_root
        );

        measured.reset_fts_uploads();
        db.execute(&delete_all_plan(), context::ParamBindings::default())
            .await
            .expect("delete-only epoch commits");
        assert_eq!(measured.fts_uploads().0, 0);
        let deleted_root = manifest_root(writer_db(&db), root)
            .await
            .expect("delete-only root reads");
        assert_eq!(deleted_root.split_count(), 2);
        assert_eq!(
            deleted_root.revision().get(),
            updated_root.revision().get() + 1
        );
        assert_eq!(
            compaction_pointer_revision(writer_db(&db), &definition, root, 0)
                .await
                .expect("delete-only tail pointer reads"),
            deleted_root.revision().get()
        );
        for entity_id in 0..100 {
            let state = entity_state(writer_db(&db), root, entity_id)
                .await
                .expect("batched dead state reads");
            assert!(!state.live);
            assert_eq!(state.logical_version.get(), deleted_root.revision().get());
        }
        assert!(search(&db, 100)
            .await
            .expect("delete-only results read")
            .1
            .is_empty());

        measured.reset_fts_uploads();
        db.execute(&write_read_write_plan(), context::ParamBindings::default())
            .await
            .expect("write/read/write request commits both flush epochs");
        assert_eq!(measured.fts_uploads().0, 2);
        let barrier_root = manifest_root(writer_db(&db), root)
            .await
            .expect("barrier root reads");
        assert_eq!(barrier_root.split_count(), 4);
        assert_eq!(
            entity_state(writer_db(&db), root, 101)
                .await
                .expect("pre-read entity state reads")
                .logical_version
                .get(),
            deleted_root.revision().get() + 1
        );
        assert_eq!(
            entity_state(writer_db(&db), root, 102)
                .await
                .expect("post-read entity state reads")
                .logical_version
                .get(),
            barrier_root.revision().get()
        );

        measured.reset_fts_uploads();
        db.execute(
            &access_and_mutate_plan(vec![
                set_body("temporary replacement"),
                exec::ExecMutationPlan::Drop,
            ]),
            context::ParamBindings::default()
                .with_value(name("target"), AstPropertyValue::I64(101)),
        )
        .await
        .expect("update/delete coalesces to a retirement");
        assert_eq!(measured.fts_uploads().0, 0);
        let update_delete_root = manifest_root(writer_db(&db), root)
            .await
            .expect("update/delete root reads");
        assert_eq!(update_delete_root.split_count(), 4);
        assert_eq!(
            update_delete_root.revision().get(),
            barrier_root.revision().get() + 1
        );
        let retired = entity_state(writer_db(&db), root, 101)
            .await
            .expect("update/delete state reads");
        assert!(!retired.live);
        assert_eq!(
            retired.logical_version.get(),
            update_delete_root.revision().get()
        );
        db.close().await.expect("batching contract database closes");
        drop(object_store);
        drop(measured);
        temporary
            .close()
            .expect("batching contract directory removes");

        let temporary = tempfile::tempdir().expect("multi-index contract directory creates");
        let local: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temporary.path())
                .expect("multi-index contract store opens"),
        );
        let measured = Arc::new(MeasuredObjectStore::new(local, Duration::ZERO));
        let object_store: Arc<dyn ObjectStore> = measured.clone();
        let database = "text-transaction-multi-index-contract";
        let body =
            TextIndexDefinition::new_node(LABEL, PROPERTY).expect("body definition validates");
        let title =
            TextIndexDefinition::new_node(LABEL, "title").expect("title definition validates");
        let body_root = seed_active_text_generation_with(
            database,
            Arc::clone(&object_store),
            &body,
            IndexId::new(1).expect("body index ID is non-zero"),
            crate::index_lifecycle::TextManifestRevision::new(3)
                .expect("body revision is non-zero"),
            true,
        )
        .await
        .expect("body root seeds");
        let title_root = seed_active_text_generation_with(
            database,
            Arc::clone(&object_store),
            &title,
            IndexId::new(2).expect("title index ID is non-zero"),
            crate::index_lifecycle::TextManifestRevision::new(9)
                .expect("title revision is non-zero"),
            true,
        )
        .await
        .expect("title root seeds");
        let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
            database,
            Arc::clone(&object_store),
            benchmark_config(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("multi-index contract database opens");
        measured.reset_fts_uploads();
        db.execute(
            &dual_index_insert_plan(10),
            context::ParamBindings::default(),
        )
        .await
        .expect("dual-index inserts commit");
        assert_eq!(measured.fts_uploads().0, 2);
        let body_value = manifest_root(writer_db(&db), body_root)
            .await
            .expect("body root reads");
        let title_value = manifest_root(writer_db(&db), title_root)
            .await
            .expect("title root reads");
        assert_eq!(
            (body_value.split_count(), body_value.revision().get()),
            (1, 4)
        );
        assert_eq!(
            (title_value.split_count(), title_value.revision().get()),
            (1, 10)
        );
        for entity_id in 0..10 {
            assert_eq!(
                entity_state(writer_db(&db), body_root, entity_id)
                    .await
                    .expect("body state reads")
                    .logical_version
                    .get(),
                4
            );
            assert_eq!(
                entity_state(writer_db(&db), title_root, entity_id)
                    .await
                    .expect("title state reads")
                    .logical_version
                    .get(),
                10
            );
        }
        db.close()
            .await
            .expect("multi-index contract database closes");
        drop(object_store);
        drop(measured);
        temporary
            .close()
            .expect("multi-index contract directory removes");

        let temporary = tempfile::tempdir().expect("partition contract directory creates");
        let local: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temporary.path())
                .expect("partition contract store opens"),
        );
        let measured = Arc::new(MeasuredObjectStore::new(local, Duration::ZERO));
        let object_store: Arc<dyn ObjectStore> = measured.clone();
        let database = "text-transaction-partition-contract";
        let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("partitioned definition validates")
            .with_tenant_property("tenant")
            .expect("tenant property validates");
        assert!(seed_active_text_generation_with(
            database,
            Arc::clone(&object_store),
            &definition,
            IndexId::initial(),
            crate::index_lifecycle::TextManifestRevision::initial(),
            false,
        )
        .await
        .is_none());
        let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
            database,
            Arc::clone(&object_store),
            benchmark_config(),
            LifecycleTestScheduling::Explicit,
        )
        .await
        .expect("partition contract database opens");
        measured.reset_fts_uploads();
        db.execute(
            &partitioned_insert_plan(),
            context::ParamBindings::default(),
        )
        .await
        .expect("two-partition inserts commit");
        assert_eq!(measured.fts_uploads().0, 2);
        for (entity_id, tenant) in [(0, "acme"), (1, "globex")] {
            let root = tenant_root(tenant);
            let root_value = manifest_root(writer_db(&db), root)
                .await
                .expect("partition root reads");
            assert_eq!(root_value.split_count(), 1);
            let state = entity_state(writer_db(&db), root, entity_id)
                .await
                .expect("partition state reads");
            assert!(state.live);
            assert_eq!(state.logical_version.get(), root_value.revision().get());
        }

        let acme_root = tenant_root("acme");
        let acme_before = manifest_root(writer_db(&db), acme_root)
            .await
            .expect("source partition root reads");
        measured.reset_fts_uploads();
        db.execute(
            &access_and_mutate_plan(vec![
                set_property("tenant", "temporary-partition"),
                set_property("tenant", "initech"),
            ]),
            context::ParamBindings::default().with_value(name("target"), AstPropertyValue::I64(0)),
        )
        .await
        .expect("two tenant changes coalesce to one partition move");
        assert_eq!(measured.fts_uploads().0, 1);
        let acme_after = manifest_root(writer_db(&db), acme_root)
            .await
            .expect("retired source partition root reads");
        assert_eq!(acme_after.split_count(), acme_before.split_count());
        assert_eq!(
            acme_after.revision().get(),
            acme_before.revision().get() + 1
        );
        let source_state = entity_state(writer_db(&db), acme_root, 0)
            .await
            .expect("retired source partition state reads");
        assert!(!source_state.live);
        assert_eq!(
            source_state.logical_version.get(),
            acme_after.revision().get()
        );

        let final_root = tenant_root("initech");
        let final_value = manifest_root(writer_db(&db), final_root)
            .await
            .expect("final partition root reads");
        assert_eq!(final_value.split_count(), 1);
        let final_state = entity_state(writer_db(&db), final_root, 0)
            .await
            .expect("final partition state reads");
        assert!(final_state.live);
        assert_eq!(
            final_state.logical_version.get(),
            final_value.revision().get()
        );

        let intermediate_key = scoped_key(index_keys::ScopedKey::TextManifestRoot(tenant_root(
            "temporary-partition",
        )));
        assert!(writer_db(&db)
            .get(intermediate_key)
            .await
            .expect("intermediate partition root lookup succeeds")
            .is_none());
        db.close()
            .await
            .expect("partition contract database closes");
        drop(object_store);
        drop(measured);
        temporary
            .close()
            .expect("partition contract directory removes");
    }

    /// Runs one clean local-storage sample through production mutation and search paths.
    pub async fn run_text_transaction_batch_benchmark_sample(
        case: TextTransactionBatchBenchmarkCase,
    ) -> Result<TextTransactionBatchBenchmarkSample> {
        let temporary = tempfile::tempdir().expect("benchmark temporary directory creates");
        let local: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temporary.path())?);
        let measured = Arc::new(MeasuredObjectStore::new(
            local,
            Duration::from_millis(case.upload_latency_millis),
        ));
        let object_store: Arc<dyn ObjectStore> = measured.clone();
        let database = "text-transaction-benchmark";
        let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("benchmark text definition validates");
        let root =
            seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
        let config = benchmark_config();
        let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
            database,
            Arc::clone(&object_store),
            config.clone(),
            LifecycleTestScheduling::Explicit,
        )
        .await?;
        let splits_before = manifest_split_count(writer_db(&db), root).await?;
        measured.reset_fts_uploads();
        let transaction_started = Instant::now();
        db.execute(
            &insert_plan(case.batch_size),
            context::ParamBindings::default(),
        )
        .await?;
        let transaction_latency_ns =
            u64::try_from(transaction_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let (upload_count, upload_bytes) = measured.fts_uploads();
        let splits_after = manifest_split_count(writer_db(&db), root).await?;
        let (immediate_search_latency_ns, immediate_ids) = search(&db, case.batch_size).await?;
        if immediate_ids.len() != case.batch_size {
            return Err(HelixDbError::InvariantViolation(format!(
                "text transaction benchmark expected {} immediate hits, found {}",
                case.batch_size,
                immediate_ids.len()
            )));
        }
        db.close().await?;

        let db = crate::HelixDB::open_with_object_store_and_config(
            database,
            Arc::clone(&object_store),
            config,
        )
        .await?;
        let post_compaction_split_count =
            wait_for_manifest_quiescence(&db, &measured, root).await?;
        let (post_compaction_search_latency_ns, post_ids) = search(&db, case.batch_size).await?;
        if immediate_ids != post_ids {
            return Err(HelixDbError::InvariantViolation(
                "text transaction benchmark search results changed after compaction".to_string(),
            ));
        }
        let search_digest = digest(&immediate_ids);
        db.close().await?;
        drop(object_store);
        drop(measured);
        temporary
            .close()
            .expect("benchmark temporary directory removes");

        Ok(TextTransactionBatchBenchmarkSample {
            case,
            transaction_latency_ns,
            upload_count,
            upload_bytes,
            manifest_split_growth: splits_after.saturating_sub(splits_before),
            immediate_search_latency_ns,
            post_compaction_search_latency_ns,
            post_compaction_split_count,
            search_digest,
        })
    }
}

#[cfg(feature = "index-lifecycle-testing")]
pub use text_transaction_benchmark::{
    run_text_transaction_batch_benchmark_sample, run_text_transaction_batching_contracts,
    TextTransactionBatchBenchmarkCase, TextTransactionBatchBenchmarkSample,
};
