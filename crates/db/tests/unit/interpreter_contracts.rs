//! Interpreter contracts that require controlled internal state.
//!
//! These fixtures retain deterministic control over a transaction conflict and
//! request-read ownership while asserting only durable rows and typed outcomes.

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

use super::text_fixtures::{scoped_key, seed_active_text_generation};
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
use crate::index_lifecycle::IndexOperationId;
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
#[tokio::test]
async fn catalog_authority_transfers_through_write_open() {
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
#[tokio::test]
async fn request_modes_preserve_isolated_mutation_ownership() {
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

/// Proves membership retention decisions the public planner cannot reach.
///
/// One operation's writes are homogeneous, so only a transaction's sequence
/// of transitions shows a label's property footprint growing and being
/// absorbed by a node write. Validated plans never carry a range set, but a
/// hand-built one resolves per row, is stored, and is forgotten by every
/// write to its label, since it reads more than point indexes; its rows stay
/// equal to the per-row filter's, including after the write that drops the
/// row. A request that executes more distinct sets than the cache holds
/// keeps only the newest ones.
#[tokio::test]
async fn membership_retention_follows_write_footprints() {
    use crate::index_lifecycle::graph_mutation::{
        CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit,
        PropertyEditOutcome,
    };

    let row = CanonicalPropertyRow::new(vec![Property::string("$label", "Item")]);
    let replace = |property: &str| {
        let PropertyEditOutcome::Changed(transition) = GraphMutationTransition::edit(
            DataScope::LegacyUnscoped,
            GraphEntity::node(1),
            row.clone(),
            PropertyEdit::set(Property::string(property, "v")),
        ) else {
            panic!("the edit changes the row");
        };
        transition
    };
    let mut writes = mutation::NodeIndexWrites::default();
    writes.record(&replace("title"));
    writes.record(&replace("kind"));
    assert_eq!(
        writes.label("Item"),
        Some(&mutation::LabelWrites::Properties(
            ["kind".into(), "title".into()].into()
        ))
    );
    writes.record(&GraphMutationTransition::create(
        DataScope::LegacyUnscoped,
        GraphEntity::node(2),
        row.clone(),
    ));
    writes.record(&replace("status"));
    assert_eq!(writes.label("Item"), Some(&mutation::LabelWrites::Nodes));

    let db = test_support::open_db_with_config(
        test_support::in_memory_config("production-membership-retention-range")
            .with_range_index("Item", "rank"),
    )
    .await;
    let item = test_support::add_node_with_properties(
        &db,
        "Item",
        vec![("rank", helix_ast::value::PropertyValue::I64(5))],
    )
    .await;
    let key =
        catalog::ScopedPropertyDirectionKey::try_new("Item", "rank", RangeIndexDirection::Asc)
            .expect("range key is valid");
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::gte("rank", 3_i64))
        .expect("range predicate is valid");
    let range = exec::ExecOp::IndexMembership {
        plan: Box::new(exec::ExecNodeIndexMembershipPlan {
            set: exec::ExecNodeMembershipSet::Index {
                set: exec::ExecNodeSecondarySetPlan::Range(exec::ExecNodeSecondaryRangePlan {
                    index: catalog::IndexCatalogSnapshot::default()
                        .with_node_range(key.clone())
                        .node_range[&key]
                        .clone(),
                    key,
                    range: ir::IndexRange::All,
                    iteration: ir::RangeScanIteration::Forward,
                }),
                label: test_support::name("Item"),
                outside_label: ir::NodeMembershipOutsideLabel::Evaluate,
            },
            predicate: predicate.clone(),
            residual: None,
        }),
    };
    let mut execution = ExecutionContext::new(&db, context::ParamBindings::default());
    execution
        .enable_request_write_scope()
        .await
        .expect("request transaction opens");
    let kept = ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(item))]);
    // The row has rank 5, then 4, then 2: kept, kept, dropped.
    for (write, kept_rows) in [(Some(4), 1), (Some(2), 1), (None, 0)] {
        execution
            .flush_active_index_mutations()
            .await
            .expect("pending writes flush");
        let membership = execution
            .execute_op(&range, kept.clone())
            .await
            .expect("range membership evaluates rows");
        let filter = execution
            .execute_op(
                &exec::ExecOp::Filter {
                    predicate: predicate.clone(),
                },
                kept.clone(),
            )
            .await
            .expect("per-row filter evaluates rows");
        assert_eq!(membership, filter);
        let ExecutionValue::Stream(rows) = membership else {
            panic!("membership returns rows");
        };
        assert_eq!(rows.len(), kept_rows);
        assert_eq!(execution.prepared_memberships.len(), 1);
        let Some(rank) = write else {
            break;
        };
        execution
            .execute_op(
                &exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::SetProperty {
                        name: test_support::name("rank"),
                        value: ir::PropertyInputPlan::Value(helix_ast::value::PropertyValue::I64(
                            rank,
                        )),
                    },
                },
                kept.clone(),
            )
            .await
            .expect("rank update succeeds");
        assert_eq!(execution.prepared_memberships.len(), 0);
    }

    // One more distinct `$label` set than the cache holds evicts the oldest.
    let capacity = super::stream::PreparedMemberships::MAX_ENTRIES;
    for label in 0..=capacity {
        let label = test_support::name(&format!("L{label}"));
        execution
            .execute_op(
                &exec::ExecOp::IndexMembership {
                    plan: Box::new(exec::ExecNodeIndexMembershipPlan {
                        set: exec::ExecNodeMembershipSet::Labels(ir::AtLeast::from_one(
                            label.clone(),
                        )),
                        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(
                            "$label",
                            label.to_string(),
                        ))
                        .expect("label predicate is valid"),
                        residual: None,
                    }),
                },
                kept.clone(),
            )
            .await
            .expect("label membership resolves");
    }
    assert_eq!(execution.prepared_memberships.len(), capacity);
    execution.abort_request_write_scope();
    drop(execution);
    db.close().await.expect("range retention fixture closes");
}

/// Proves index status stays on the open graph write and does not publish it.
#[tokio::test]
async fn get_operation_keeps_open_graph_write() {
    let status = exec::ExecOp::IndexDdl {
        plan: ir::IndexDdlPlan::GetOperation {
            operation_id: ir::IndexOperationId::try_new("07070707-0707-0707-0707-070707070707")
                .expect("canonical lowercase operation ID"),
        },
    };
    let first = exec::ExecStepId::new(1).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Write,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("User"),
                        properties: test_support::assignments(Vec::new()),
                    },
                },
            ),
            test_support::step(2, vec![first], status.clone()),
        ],
        2,
    );
    assert_eq!(
        RequestSideEffects::operation(&status),
        RequestSideEffects::None
    );
    assert_eq!(
        RequestExecutionMode::try_from(&plan).expect("status is a graph-write companion"),
        RequestExecutionMode::GraphWrite
    );

    let db = test_support::open_db("production-get-op-keeps-write").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    let created = context
        .execute_step(
            &test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::IndexDdl {
                    plan: ir::IndexDdlPlan::Create {
                        spec: ir::IndexDdlCreateSpec::NodeEquality {
                            key: catalog::ScopedPropertyKey::try_new("User", "email")
                                .expect("scoped key"),
                            uniqueness: catalog::IndexUniqueness::NonUnique,
                        },
                        mode: ir::IndexCreateMode::ErrorIfExists,
                    },
                },
            ),
            None,
        )
        .await
        .expect("create enqueues a durable operation");
    let ExecutionValue::IndexDdlReceipt(crate::index_lifecycle::IndexDdlReceipt::Accepted {
        operation_id,
        ..
    }) = created
    else {
        panic!("new index create must be accepted");
    };
    let operation_id = ir::IndexOperationId::try_new(operation_id.as_uuid().to_string())
        .expect("receipt UUID is a canonical lowercase operation ID");

    context
        .enable_request_write_scope()
        .await
        .expect("request write opens");
    let added = context
        .execute_step(
            &test_support::step(
                2,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("User"),
                        properties: test_support::assignments(vec![(
                            "email",
                            AstPropertyValue::from("uncommitted@example.com"),
                        )]),
                    },
                },
            ),
            None,
        )
        .await
        .expect("graph write stays on the request transaction");
    let ExecutionValue::Stream(rows) = added else {
        panic!("node write should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("node write should return a node row");
    };
    let node_key = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(*id)),
    }
    .to_bytes();
    assert!(
        writer_db(&db).get(&node_key).await.expect("peek").is_none(),
        "uncommitted graph write must not be visible on a snapshot"
    );

    let got = context
        .execute_step(
            &test_support::step(
                3,
                Vec::new(),
                exec::ExecOp::IndexDdl {
                    plan: ir::IndexDdlPlan::GetOperation { operation_id },
                },
            ),
            None,
        )
        .await
        .expect("status read stays on the open graph write");
    assert!(matches!(got, ExecutionValue::IndexOperationStatus(_)));
    assert!(context.has_request_write_scope());
    assert!(writer_db(&db).get(&node_key).await.expect("peek").is_none());

    let missing = context
        .execute_step(&test_support::step(4, Vec::new(), status), None)
        .await
        .expect_err("unknown operation ID is a status miss, not a commit");
    assert!(matches!(
        missing,
        HelixDbError::IndexOperationNotFound { .. }
    ));
    assert!(context.has_request_write_scope());
    assert!(writer_db(&db).get(&node_key).await.expect("peek").is_none());

    context
        .commit_request_write_scope()
        .await
        .expect("request write commits");
    assert!(writer_db(&db)
        .get(&node_key)
        .await
        .expect("committed read")
        .is_some());
    db.close().await.expect("production get-op fixture closes");
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
    // The directory makes the filtered walk probe it through the request view.
    let index = VectorIndex::<D>::new(name).with_simhash_directory();
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

    // One past the restricted exact-scan cardinality cap (8,192) routes the
    // request view through the filtered graph walk.
    let filtered = RestrictedVectorCandidates::from_ids(1..=8_193).unwrap();
    let (filtered_results, filtered_stats) = index
        .search_restricted_with_stats(
            &view,
            &[1.0, 0.0],
            &SearchParams::new(2).unwrap(),
            &filtered,
        )
        .await
        .unwrap();
    assert_eq!(
        filtered_stats.strategy,
        Some(crate::search::vector::RestrictedSearchStrategy::FilteredGraph)
    );
    assert!(filtered_stats.directory_scan_calls > 0);
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
    let view = read_view::StableRequestReadView::ReaderSnapshot {
        snapshot,
        compatibility: crate::index_lifecycle::repository::ReaderStorageCompatibility::Current,
    };
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
    let missing_node = ExecutionRow::current(ElementRef::Node(u64::MAX));
    assert_eq!(
        context.row_property(&missing_node, &label).await.unwrap(),
        None
    );
    // Neither an empty row nor a missing record has stored properties.
    assert_eq!(
        context
            .project(
                ExecutionValue::Stream(vec![empty_row, missing_node]),
                &ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
            )
            .await
            .unwrap(),
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Object(std::collections::BTreeMap::new()),
            ExecutionScalar::Object(std::collections::BTreeMap::from([(
                "$id".to_string(),
                DbPropertyValue::I64(i64::MAX),
            )])),
        ])
    );
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
/// unchanged production interpreter.
#[tokio::test]
async fn scheduler_and_projection_paths_execute_their_encoded_shapes() {
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
        .execute_steps(
            plan.steps(),
            plan.execution_order(),
            plan.root(),
            plan.execution_program(),
        )
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
        .execute_steps(
            plan.steps(),
            plan.execution_order(),
            plan.root(),
            plan.execution_program(),
        )
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

/// Runs filtered node vector searches through the public request path, where
/// the scheduler fuses each index-set access into its search, beside shapes
/// that must run unfused, and proves the pairing rejects every pair that is
/// not an exclusive node ID set feeding a node search.
#[tokio::test]
async fn filtered_node_vector_searches_fuse_index_set_accesses() {
    use helix_ast::{batch, expr::Predicate, query::QueryRequest, traversal, value::PropertyInput};

    let db = test_support::open_db_with_config(
        test_support::in_memory_config("production-fused-vector-search")
            .with_node_vector_index(
                "Doc",
                "embedding",
                2,
                crate::search::vector::VectorDistanceMetric::Euclidean,
            )
            .with_equality_index("Doc", "category")
            .with_equality_index("Doc", "status")
            .with_range_index("Doc", "rank"),
    )
    .await;
    // The node of rank r sits at [r, 0], is in category a when r is even
    // and open while r < 3.
    let seed = (0..6_i64)
        .fold(batch::write_batch(), |write, rank| {
            write.var_as(
                &format!("d{rank}"),
                traversal::g().add_n(
                    "Doc",
                    vec![
                        ("embedding", PropertyInput::from(vec![rank as f32, 0.0])),
                        (
                            "category",
                            PropertyInput::from(if rank % 2 == 0 { "a" } else { "b" }),
                        ),
                        (
                            "status",
                            PropertyInput::from(if rank < 3 { "open" } else { "closed" }),
                        ),
                        ("rank", PropertyInput::from(rank)),
                    ],
                ),
            )
        })
        .returning((0..6).map(|rank| format!("d{rank}")));
    let created = db
        .query(QueryRequest::write(seed))
        .await
        .expect("fixture nodes are written");
    let ids = (0..6)
        .map(|rank| {
            created[format!("d{rank}")][0]["$id"]
                .as_u64()
                .expect("node ID")
        })
        .collect::<Vec<_>>();
    let hit_ids = |response: &serde_json::Value| {
        response["hits"]
            .as_array()
            .expect("hits are rows")
            .iter()
            .map(|hit| hit["$id"].as_u64().expect("hit ID"))
            .collect::<Vec<_>>()
    };
    let search = |source: traversal::Traversal<traversal::OnNodes>| {
        batch::read_batch()
            .var_as(
                "hits",
                source.vector_search("Doc", "embedding", vec![0.0, 0.0], 2, None),
            )
            .returning(["hits"])
    };
    let docs = |predicate| traversal::g().n_with_label_where("Doc", predicate);
    // Label, equality bitmap, secondary set and an empty set fuse; a range
    // scan and a residual filter do not.
    for (source, fused, ranks) in [
        (traversal::g().n_with_label("Doc"), true, &[0, 1][..]),
        (docs(Predicate::eq("category", "a")), true, &[0, 2][..]),
        (
            docs(Predicate::and(vec![
                Predicate::eq("category", "a"),
                Predicate::eq("status", "open"),
            ])),
            true,
            &[0, 2][..],
        ),
        (docs(Predicate::eq("category", "none")), true, &[][..]),
        (docs(Predicate::gt("rank", 3_i64)), false, &[4, 5][..]),
        (docs(Predicate::eq("unindexed", "x")), false, &[][..]),
    ] {
        let read = search(source);
        let prepared = db
            .planner_context_scoped_prepared(
                context::ParamBindings::default(),
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("planner context opens");
        let plan = helix_planner::planning::plan_read_batch(&read, prepared.context())
            .expect("read plans");
        assert_eq!(
            super::scheduler::fusion::plan(
                &plan.steps().iter().map(|step| (step.id, step)).collect(),
                &super::scheduler::output_uses(plan.steps(), plan.root()).expect("use plan"),
                plan.execution_program(),
            )
            .len(),
            if fused { 2 } else { 0 },
            "{:#?}",
            plan.steps()
        );
        let response = db
            .query(QueryRequest::read(read))
            .await
            .expect("search executes");
        assert_eq!(
            hit_ids(&response),
            ranks.iter().map(|rank| ids[*rank]).collect::<Vec<_>>()
        );
    }

    // A request that writes reads its set when its access runs.
    let response = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![0.5_f32, 0.0])),
                            ("category", PropertyInput::from("a")),
                        ],
                    ),
                )
                .var_as(
                    "hits",
                    docs(Predicate::eq("category", "a")).vector_search(
                        "Doc",
                        "embedding",
                        vec![0.0, 0.0],
                        2,
                        None,
                    ),
                )
                .returning(["created", "hits"]),
        ))
        .await
        .expect("write executes");
    assert_eq!(
        hit_ids(&response),
        vec![
            ids[0],
            response["created"][0]["$id"].as_u64().expect("created ID")
        ]
    );

    // A search over two inputs, an edge search and an edge set never pair.
    let prepared = db
        .planner_context_scoped_prepared(
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("planner context opens");
    let plan = helix_planner::planning::plan_read_batch(
        &search(docs(Predicate::eq("category", "a"))),
        prepared.context(),
    )
    .expect("read plans");
    let mut steps = plan.steps().to_vec();
    steps.push(test_support::step(3, Vec::new(), exec::ExecOp::Noop));
    steps[1]
        .dependencies
        .push(exec::ExecStepId::new(3).expect("positive step ID"));
    assert!(super::scheduler::fusion::plan(
        &steps.iter().map(|step| (step.id, step)).collect(),
        &super::scheduler::output_uses(&steps, plan.root()).expect("use plan"),
        plan.execution_program(),
    )
    .is_empty());
    let (exec::ExecOp::Access { plan: access }, exec::ExecOp::VectorSearch { plan: search }) =
        (&steps[0].op, &steps[1].op)
    else {
        panic!("planned an access feeding a search: {steps:#?}");
    };
    assert!(super::access::IdSetVectorSearch::new(access, search).is_some());
    let ir::RestrictedVectorSearchPlan::Nodes {
        index,
        query_vector,
        k,
        ..
    } = search.as_ref()
    else {
        panic!("planned a node search: {search:?}");
    };
    let edge_search = ir::RestrictedVectorSearchPlan::Edges {
        key: catalog::EdgeSearchIndexKey::try_new("Doc", "embedding").expect("edge key"),
        index: index.clone(),
        query_vector: query_vector.clone(),
        k: k.clone(),
    };
    assert!(super::access::IdSetVectorSearch::new(access, &edge_search).is_none());
    let edge_set = exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::LabelScan {
        label: test_support::name("Doc"),
    });
    assert!(super::access::IdSetVectorSearch::new(&edge_set, search).is_none());
    db.close().await.expect("fused search fixture closes");
}

/// Runs a real serializable graph conflict through a queued Active-text write.
///
/// The losing transaction staged a text operation: nothing is uploaded, no
/// queue operand or index row commits, and its admission charge is released.
#[tokio::test]
async fn active_text_graph_conflict_resolves_fail_closed() {
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
    assert_eq!(
        text_blob_paths(&object_store).await,
        blobs_before,
        "staging a queued Active-text write uploads nothing"
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
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (
            stats.pending_operations,
            stats.uncertain_operations,
            stats.committed_operations
        ),
        (0, 0, 0),
        "a definite conflict commits no operation and releases its charge"
    );
    db.close()
        .await
        .expect("Active-text production fixture closes");
    assert_eq!(
        text_blob_paths(&object_store).await,
        blobs_before,
        "a losing graph transaction never leaves text blobs"
    );
}

/// Proves internal storage and index reads fail closed without a request view,
/// through the same arms non-test builds compile.
#[tokio::test]
async fn request_read_view_guards_fail_closed() {
    let _strict = StrictRequestViews::hold();
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
        (
            context
                .open_raw_range(
                    Bytes::from_static(b"guard-range-start"),
                    Bytes::from_static(b"guard-range-end"),
                )
                .await
                .err()
                .expect("resumable range scans require a request view"),
            "storage range scan escaped its request read view",
        ),
        (
            context
                .open_raw_prefix(Bytes::from_static(b"guard-prefix"))
                .await
                .err()
                .expect("resumable prefix scans require a request view"),
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
                iteration: helix_planner::ir::RangeScanIteration::Forward,
                index: catalog::NodeRangeIndexMeta::new(
                    ir::NonEmptyString::new("guard-node-range").expect("index ID is non-empty"),
                ),
                key: range_key.clone(),
                range: ir::IndexRange::All,
            }),
            "range cursor escaped request read view",
        ),
        (
            exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::RangeIndex {
                iteration: helix_planner::ir::RangeScanIteration::Forward,
                index: catalog::EdgeRangeIndexMeta::new(
                    ir::NonEmptyString::new("guard-edge-range").expect("index ID is non-empty"),
                ),
                key: range_key,
                range: ir::IndexRange::All,
            }),
            "range cursor escaped request read view",
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

/// Exercises the production topology collector.
#[tokio::test]
async fn topology_mutations_preserve_transactional_semantics() {
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
    runtime.flush(&transaction).unwrap();

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
    runtime.flush(&transaction).unwrap();

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

    runtime.prepare(&transaction).unwrap();
    assert!(runtime.flush(&transaction).is_err());
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
    rollback_runtime.flush(&rollback).unwrap();
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
    missing_runtime.flush(&missing).unwrap();
    assert!(missing.get(&missing_label_key).await.unwrap().is_none());
    assert!(missing.get(&missing_adjacency_key).await.unwrap().is_none());
    missing_runtime.prepare(&missing).unwrap();
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
        // Helix refuses to store a membership row it could not merge into.
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert!(crate::transaction::Mutation::put(&seed, &key, &corrupt).is_err());
        assert!(
            crate::transaction::Mutation::put_bytes(&seed, key.clone(), corrupt.clone()).is_err()
        );
        seed.commit().await.unwrap();
        assert_eq!(db.get(&key).await.unwrap(), None);
        // A row corrupted outside Helix and already flushed is never read by
        // staging; its delta flushes alone and reads of the row fail closed.
        db.put(&key, corrupt.clone()).await.unwrap();
        let memtable = slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        };
        db.flush_with_options(memtable.clone()).await.unwrap();
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
        runtime.prepare(&transaction).unwrap();
        runtime.consume_prepared().unwrap();
        transaction.commit().await.unwrap();
        db.flush_with_options(memtable).await.unwrap();
        assert!(db.get(&key).await.is_err());
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
    losing_runtime.flush(&losing).unwrap();
    let winning = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    winning.put(&conflict_key, topology_bitmap([1, 2])).unwrap();
    winning.commit().await.unwrap();
    losing_runtime.prepare(&losing).unwrap();
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
