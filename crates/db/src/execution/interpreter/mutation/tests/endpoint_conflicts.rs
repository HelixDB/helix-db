//! An edge insert needs its endpoints to exist, not to stay unchanged.
//!
//! Each race here pins one interleaving: a request stages its edge, a second
//! request runs and commits, and the first then commits. A concurrent property
//! write must leave the insert alone. A concurrent node delete must still
//! exclude it, in either commit order, so no edge survives its endpoint.
use super::support::*;
use crate::execution::interpreter::{mutation::DeletionTargets, test_support};
use crate::query_resources::Budget;
use helix_planner::{context, relational as r};
use serde_json::json;
use std::sync::Arc;

/// How the edge insert under test reached its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    /// `g().n(id).add_e(..)`: a KV read yields the source row.
    KvRead,
    /// `g().n(ids).add_e(..)`: a KV batch yields the source rows.
    KvMultiGet,
    /// `g().n(param).add_e(..)`: the pull source checks the parameter's IDs.
    Param,
    /// The input row already names the source.
    Bound,
}

const SOURCES: [Source; 4] = [
    Source::Bound,
    Source::KvRead,
    Source::KvMultiGet,
    Source::Param,
];

fn is_conflict(error: &HelixDbError) -> bool {
    matches!(error, HelixDbError::Storage(error) if error.kind() == slatedb::ErrorKind::Transaction)
}

fn user(id: u64) -> exec::KvReadPlan {
    exec::KvReadPlan::Get {
        key: exec::ElementKeyspace::NodeProperty.point_key(id),
    }
}

/// A budgeted request, as every Cypher and query-service write is.
fn request(db: &HelixDB) -> ExecutionContext<'_> {
    let mut ctx = ExecutionContext::new(db, context::ParamBindings::default());
    ctx.row_memory = Some(Budget::new(16 * 1024 * 1024));
    ctx
}

/// A budgeted request whose `ids` parameter names `from`.
fn request_from(db: &HelixDB, from: u64) -> ExecutionContext<'_> {
    let parameters = context::ParamBindings::default().with_value(
        name("ids"),
        PropertyValue::I64Array(vec![i64::try_from(from).expect("test ids fit i64")]),
    );
    let mut ctx = ExecutionContext::new(db, parameters);
    ctx.row_memory = Some(Budget::new(16 * 1024 * 1024));
    ctx
}

/// Stages `from -[R]-> to` in `ctx` and leaves it uncommitted.
async fn stage_edge_in(ctx: &mut ExecutionContext<'_>, from: u64, to: u64, source: Source) {
    ctx.enable_request_write_scope().await.unwrap();
    let input = match source {
        Source::KvRead => ctx.execute_kv_read(&user(from)).await.unwrap(),
        Source::KvMultiGet => ctx
            .execute_kv_read(&exec::KvReadPlan::MultiGet(
                exec::KvMultiGetPlan::new(
                    vec![
                        exec::ElementKeyspace::NodeProperty.point_key(from),
                        exec::ElementKeyspace::NodeProperty.point_key(u64::MAX),
                    ],
                    helix_planner::properties::KeyLocality::Close,
                    helix_planner::properties::PositiveUsize::new(4).unwrap(),
                )
                .unwrap(),
            ))
            .await
            .map(|rows| {
                assert_eq!(
                    rows,
                    ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(from))])
                );
                rows
            })
            .unwrap(),
        Source::Param => ctx
            .execute_access(&exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromParam { param: name("ids") },
            ))
            .await
            .unwrap(),
        Source::Bound => {
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(from))])
        }
    };
    ctx.execute_mutation(
        input,
        &exec::ExecMutationPlan::AddEdge {
            label: name("R"),
            to: ir::NodeTargetPlan::PointIds {
                ids: crate::execution::interpreter::test_support::ids(vec![to]),
            },
            properties: ir::PropertyAssignments::default(),
        },
    )
    .await
    .unwrap();
}

/// Opens a request, stages `from -[R]-> to` and leaves it uncommitted.
async fn stage_edge<'a>(
    db: &'a HelixDB,
    from: u64,
    to: u64,
    source: Source,
) -> ExecutionContext<'a> {
    let mut ctx = request_from(db, from);
    stage_edge_in(&mut ctx, from, to, source).await;
    ctx
}

async fn stored_edges(db: &HelixDB) -> serde_json::Value {
    db.cypher(crate::cypher::Request::new(
        "MATCH ()-[r]->() RETURN count(r)",
    ))
    .await
    .unwrap()
    .rows[0][0]
        .clone()
}

/// Commits a property write to `node` through its own request.
async fn set_property(db: &HelixDB, node: u64, value: i64) {
    let mut ctx = ExecutionContext::new(db, context::ParamBindings::default());
    ctx.enable_request_write_scope().await.unwrap();
    let input = ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(node))]);
    ctx.execute_mutation(
        input,
        &exec::ExecMutationPlan::SetProperty {
            name: name("touched"),
            value: ir::PropertyInputPlan::Value(PropertyValue::I64(value)),
        },
    )
    .await
    .unwrap();
    ctx.commit_request_write_scope().await.unwrap();
}

#[tokio::test]
async fn property_writes_never_abort_an_edge_insert_on_either_endpoint() {
    for source in SOURCES {
        for self_loop in [false, true] {
            let db = test_support::open_db("endpoint-property-race").await;
            let from = test_support::add_user(&db, "from").await;
            let other = test_support::add_user(&db, "to").await;
            let to = if self_loop { from } else { other };
            let mut insert = stage_edge(&db, from, to, source).await;
            set_property(&db, from, 1).await;
            set_property(&db, to, 2).await;
            insert.commit_request_write_scope().await.unwrap();
            drop(insert);
            assert_eq!(stored_edges(&db).await, json!(1));
            let touched = db
                .cypher(crate::cypher::Request::new(
                    "MATCH (n:User) WHERE n.touched IS NOT NULL RETURN count(n)",
                ))
                .await
                .unwrap();
            let expected = if self_loop { 1 } else { 2 };
            assert_eq!(touched.rows, vec![vec![json!(expected)]], "{source:?}");
            db.close().await.unwrap();
        }
    }
}

/// A delete that commits first aborts the insert, on either endpoint and
/// whether or not the delete detaches anything.
#[tokio::test]
async fn a_committed_node_delete_aborts_an_edge_insert_on_either_endpoint() {
    for source in SOURCES {
        for victim_is_source in [false, true] {
            for detach in [false, true] {
                let db = test_support::open_db("endpoint-delete-first").await;
                let from = test_support::add_user(&db, "from").await;
                let to = test_support::add_user(&db, "to").await;
                let victim = if victim_is_source { from } else { to };
                let mut insert = stage_edge(&db, from, to, source).await;

                let budget = Budget::new(16 * 1024 * 1024);
                let mut delete = ExecutionContext::new(&db, context::ParamBindings::default());
                delete.row_memory = Some(budget.clone());
                delete.enable_request_write_scope().await.unwrap();
                let mut targets = DeletionTargets::new(&budget).unwrap();
                targets.insert(r::Entity::Node(victim)).unwrap();
                delete.row_delete_entities(targets, detach).await.unwrap();
                delete.commit_request_write_scope().await.unwrap();

                let error = insert.commit_request_write_scope().await.unwrap_err();
                assert!(is_conflict(&error), "{error:?}");
                drop((insert, delete));
                assert_eq!(stored_edges(&db).await, json!(0));
                db.close().await.unwrap();
            }
        }
    }
}

/// An insert that commits first aborts a delete that already looked at the
/// node, so the delete can never strand the new edge.
#[tokio::test]
async fn a_committed_edge_insert_aborts_a_node_delete_that_missed_it() {
    for source in SOURCES {
        for victim_is_source in [false, true] {
            let db = test_support::open_db("endpoint-insert-first").await;
            let from = test_support::add_user(&db, "from").await;
            let to = test_support::add_user(&db, "to").await;
            let victim = if victim_is_source { from } else { to };

            let budget = Budget::new(16 * 1024 * 1024);
            let mut delete = ExecutionContext::new(&db, context::ParamBindings::default());
            delete.row_memory = Some(budget.clone());
            delete.enable_request_write_scope().await.unwrap();
            let mut targets = DeletionTargets::new(&budget).unwrap();
            targets.insert(r::Entity::Node(victim)).unwrap();
            delete.row_delete_entities(targets, false).await.unwrap();

            let mut insert = stage_edge(&db, from, to, source).await;
            insert.commit_request_write_scope().await.unwrap();

            let error = delete.commit_request_write_scope().await.unwrap_err();
            assert!(is_conflict(&error), "{error:?}");
            drop((insert, delete));
            assert_eq!(stored_edges(&db).await, json!(1));
            let nodes = db
                .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(n)"))
                .await
                .unwrap();
            assert_eq!(nodes.rows, vec![vec![json!(2)]]);
            db.close().await.unwrap();
        }
    }
}

/// A read this transaction's own writes do not protect stays a dependency, so
/// a plan that merely checks a node keeps today's isolation.
#[tokio::test]
async fn existence_reads_without_an_adjacency_write_keep_their_dependency() {
    let db = test_support::open_db("endpoint-uncovered-read").await;
    let checked = test_support::add_user(&db, "checked").await;
    let written = test_support::add_user(&db, "written").await;

    let mut checking = request(&db);
    checking.enable_request_write_scope().await.unwrap();
    checking.execute_kv_read(&user(checked)).await.unwrap();
    checking
        .execute_mutation(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(written))]),
            &exec::ExecMutationPlan::SetProperty {
                name: name("touched"),
                value: ir::PropertyInputPlan::Value(PropertyValue::I64(1)),
            },
        )
        .await
        .unwrap();
    set_property(&db, checked, 1).await;
    let error = checking.commit_request_write_scope().await.unwrap_err();
    assert!(is_conflict(&error), "{error:?}");
    drop(checking);
    db.close().await.unwrap();
}

/// An edge covers exactly its own endpoints: a third node checked in the same
/// transaction stays a dependency.
#[tokio::test]
async fn an_edge_covers_only_the_nodes_it_touches() {
    let db = test_support::open_db("endpoint-partial-cover").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let checked = test_support::add_user(&db, "checked").await;
    let mut insert = stage_edge(&db, from, to, Source::KvRead).await;
    insert.node_rows(vec![checked]).await.unwrap();
    set_property(&db, from, 1).await;
    set_property(&db, checked, 1).await;
    let error = insert.commit_request_write_scope().await.unwrap_err();
    assert!(is_conflict(&error), "{error:?}");
    drop(insert);
    assert_eq!(stored_edges(&db).await, json!(0));
    db.close().await.unwrap();
}

/// An absent node has no adjacency write to rely on, so a request that saw
/// nothing conflicts with a row that appears at that key.
#[tokio::test]
async fn absent_nodes_keep_their_dependency() {
    let db = test_support::open_db("endpoint-absent-read").await;
    let written = test_support::add_user(&db, "written").await;
    let mut checking = request(&db);
    checking.enable_request_write_scope().await.unwrap();
    let missing = written + 1;
    assert_eq!(
        checking.execute_kv_read(&user(missing)).await.unwrap(),
        ExecutionValue::Stream(Vec::new())
    );
    checking
        .execute_mutation(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(written))]),
            &exec::ExecMutationPlan::SetProperty {
                name: name("touched"),
                value: ir::PropertyInputPlan::Value(PropertyValue::I64(1)),
            },
        )
        .await
        .unwrap();
    test_support::add_user(&db, "appears").await;
    let error = checking.commit_request_write_scope().await.unwrap_err();
    assert!(is_conflict(&error), "{error:?}");
    drop(checking);
    db.close().await.unwrap();
}

/// A node seen absent stays a dependency even if a merge touches its adjacency
/// row: nothing about that merge says the node stays absent.
#[tokio::test]
async fn an_adjacency_merge_does_not_cover_an_absent_read() {
    let db = test_support::open_db("endpoint-absent-covered").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let mut insert = stage_edge(&db, from, to, Source::Bound).await;
    // Stand in for a read that saw `to` absent before this transaction
    // created it: the row an edge touches has no other way to be absent.
    let active = insert.active_write_tx().unwrap();
    active.index_context.admit_existence_reads(1).unwrap();
    active
        .index_context
        .record_existence_reads([(to, false)].into_iter());
    set_property(&db, to, 1).await;
    let error = insert.commit_request_write_scope().await.unwrap_err();
    assert!(is_conflict(&error), "{error:?}");
    drop(insert);
    db.close().await.unwrap();
}

/// Tenants keep separate adjacency keys, so a race is judged inside its scope.
#[tokio::test]
async fn tenant_requests_keep_every_race_outcome() {
    use crate::encoding::v2::keys::scope::{DataScope, TenantId};
    let db = test_support::open_db("endpoint-tenant-races").await;
    let tenant = DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000001").unwrap());
    let scoped = || {
        let mut ctx = ExecutionContext::new_scoped(&db, context::ParamBindings::default(), tenant);
        ctx.row_memory = Some(Budget::new(16 * 1024 * 1024));
        ctx
    };
    let mut seed = scoped();
    let from = seed.row_create_node("N", vec![]).await.unwrap();
    let to = seed.row_create_node("N", vec![]).await.unwrap();
    drop(seed);

    // A property write leaves the insert alone.
    let mut insert = scoped();
    stage_edge_in(&mut insert, from, to, Source::KvRead).await;
    let mut writer = scoped();
    writer.enable_request_write_scope().await.unwrap();
    writer
        .execute_mutation(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(from))]),
            &exec::ExecMutationPlan::SetProperty {
                name: name("touched"),
                value: ir::PropertyInputPlan::Value(PropertyValue::I64(1)),
            },
        )
        .await
        .unwrap();
    writer.commit_request_write_scope().await.unwrap();
    insert.commit_request_write_scope().await.unwrap();

    // A delete of the same node aborts the next insert.
    let mut insert = scoped();
    stage_edge_in(&mut insert, from, to, Source::KvRead).await;
    let mut delete = scoped();
    let mut targets = DeletionTargets::new(delete.row_budget()).unwrap();
    targets.insert(r::Entity::Node(from)).unwrap();
    delete.enable_request_write_scope().await.unwrap();
    delete.row_delete_entities(targets, true).await.unwrap();
    delete.commit_request_write_scope().await.unwrap();
    let error = insert.commit_request_write_scope().await.unwrap_err();
    assert!(is_conflict(&error), "{error:?}");
    drop((insert, delete, writer));
    db.close().await.unwrap();
}

/// The budget refuses existence reads it cannot hold a dependency for before
/// the request stages any work on their result.
#[tokio::test]
async fn existence_reads_admit_their_dependency_before_reading() {
    let db = test_support::open_db("endpoint-read-admission").await;
    let node = test_support::add_user(&db, "node").await;
    let mut ctx = request(&db);
    ctx.enable_request_write_scope().await.unwrap();
    let budget = ctx.row_budget().clone();
    let occupied = budget.reserve(budget.available()).unwrap();
    let error = ctx.execute_kv_read(&user(node)).await.unwrap_err();
    assert!(
        matches!(error, HelixDbError::QueryMemoryLimitExceeded),
        "{error:?}"
    );
    drop(occupied);
    ctx.abort_request_write_scope();
    drop(ctx);
    db.close().await.unwrap();
}

/// Reading a row's content is a real dependency, whatever edge follows it: a
/// `MATCH` on a property must not create an edge from a node that no longer
/// matches.
#[tokio::test]
async fn reading_endpoint_properties_keeps_the_conflict() {
    let db = test_support::open_db("endpoint-content-read").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let mut insert = stage_edge(&db, from, to, Source::Bound).await;
    let key = insert.storage_key(crate::encoding::v2::keys::DataKeyKind::NodeProperty(
        crate::encoding::v2::keys::NodePropertyKey::new(from),
    ));
    insert.get_raw(&key).await.unwrap().expect("the row exists");
    set_property(&db, from, 1).await;
    let error = insert.commit_request_write_scope().await.unwrap_err();
    assert!(is_conflict(&error), "{error:?}");
    drop(insert);
    assert_eq!(stored_edges(&db).await, json!(0));
    db.close().await.unwrap();
}

/// The transaction sees its own writes without tracking them.
#[tokio::test]
async fn edge_inserts_see_nodes_this_transaction_created_or_deleted() {
    let db = test_support::open_db("endpoint-own-writes").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N {k:1})"))
        .await
        .unwrap();
    let error = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:N) DELETE n CREATE (n)-[:R]->(:M)",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::cypher::Error::Storage(HelixDbError::NodeNotFound(_))
        ),
        "{error:?}"
    );
    let response = db
        .cypher(crate::cypher::Request::new(
            "CREATE (a:Fresh)-[:R]->(b:Fresh) RETURN a=b",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(false)]]);
    db.close().await.unwrap();
}

/// One retry of a request that lost a conflict.
enum Attempt {
    Committed,
    Conflict,
    /// The node is gone, so this operation is finished.
    Gone,
}

/// Concurrent inserts, property writes and detach deletes across enough nodes
/// that most racing pairs share no merge token, so only a delete's tombstone
/// can exclude them. Conflicts retry. Afterwards every stored edge must have
/// both endpoints and be reachable through the pattern scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn racing_inserts_and_deletes_leave_no_dangling_edges() {
    const NODES: usize = 257;
    let db = Arc::new(test_support::open_db("endpoint-stress").await);
    let mut hubs = Vec::new();
    for index in 0..NODES {
        hubs.push(test_support::add_user(&db, &format!("node-{index}")).await);
    }
    let hubs = Arc::new(hubs);
    let workers = (0..8_u64)
        .map(|worker| {
            let (db, hubs) = (Arc::clone(&db), Arc::clone(&hubs));
            tokio::spawn(async move {
                for round in 0..64_usize {
                    let index = worker as usize * 37 + round * 11;
                    let (from, to) = (hubs[index % NODES], hubs[(index * 5 + 1) % NODES]);
                    loop {
                        let attempt = match (worker + round as u64) % 16 {
                            0..=3 => {
                                let budget = Budget::new(16 * 1024 * 1024);
                                let mut ctx =
                                    ExecutionContext::new(&db, context::ParamBindings::default());
                                ctx.row_memory = Some(budget.clone());
                                ctx.enable_request_write_scope().await.unwrap();
                                let mut targets = DeletionTargets::new(&budget).unwrap();
                                targets.insert(r::Entity::Node(from)).unwrap();
                                ctx.row_delete_entities(targets, true).await.unwrap();
                                match ctx.commit_request_write_scope().await {
                                    Ok(()) => Attempt::Committed,
                                    Err(error) if is_conflict(&error) => Attempt::Conflict,
                                    Err(error) => panic!("{error:?}"),
                                }
                            }
                            4..=7 => {
                                let mut ctx =
                                    ExecutionContext::new(&db, context::ParamBindings::default());
                                ctx.enable_request_write_scope().await.unwrap();
                                let input = ExecutionValue::Stream(vec![ExecutionRow::current(
                                    ElementRef::Node(to),
                                )]);
                                let staged = ctx
                                    .execute_mutation(
                                        input,
                                        &exec::ExecMutationPlan::SetProperty {
                                            name: name("touched"),
                                            value: ir::PropertyInputPlan::Value(
                                                PropertyValue::I64(round as i64),
                                            ),
                                        },
                                    )
                                    .await;
                                match staged {
                                    Ok(_) => match ctx.commit_request_write_scope().await {
                                        Ok(()) => Attempt::Committed,
                                        Err(error) if is_conflict(&error) => Attempt::Conflict,
                                        Err(error) => panic!("{error:?}"),
                                    },
                                    // The node is already gone.
                                    Err(HelixDbError::InvariantViolation(_)) => Attempt::Gone,
                                    Err(error) => panic!("{error:?}"),
                                }
                            }
                            _ => {
                                let mut ctx =
                                    ExecutionContext::new(&db, context::ParamBindings::default());
                                ctx.enable_request_write_scope().await.unwrap();
                                let input = ctx.execute_kv_read(&user(from)).await.unwrap();
                                let staged = ctx
                                    .execute_mutation(
                                        input,
                                        &exec::ExecMutationPlan::AddEdge {
                                            label: name("R"),
                                            to: ir::NodeTargetPlan::PointIds {
                                                ids: test_support::ids(vec![to]),
                                            },
                                            properties: ir::PropertyAssignments::default(),
                                        },
                                    )
                                    .await;
                                match staged {
                                    Ok(_) => match ctx.commit_request_write_scope().await {
                                        Ok(()) => Attempt::Committed,
                                        Err(error) if is_conflict(&error) => Attempt::Conflict,
                                        Err(error) => panic!("{error:?}"),
                                    },
                                    Err(HelixDbError::NodeNotFound(_)) => Attempt::Gone,
                                    Err(error) => panic!("{error:?}"),
                                }
                            }
                        };
                        match attempt {
                            Attempt::Committed | Attempt::Gone => break,
                            Attempt::Conflict => tokio::task::yield_now().await,
                        }
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.await.unwrap();
    }

    use crate::encoding::v2::keys;
    let mut scan = db.inner_db().scan(..).await.unwrap();
    let mut live = std::collections::BTreeSet::new();
    let mut endpoints = Vec::new();
    while let Some(row) = scan.next().await.unwrap() {
        let Some(key) = keys::scope::DataScope::LegacyUnscoped.strip_key(&row.key) else {
            continue;
        };
        match keys::DataKeyKind::parse_from_slice(key) {
            Ok(keys::DataKeyKind::NodeProperty(key)) => {
                live.insert(key.node_id());
            }
            Ok(keys::DataKeyKind::EdgeEndpoints(_)) => {
                let value =
                    crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue::decode(
                        &row.value,
                    )
                    .unwrap();
                endpoints.push((value.source(), value.target()));
            }
            _ => {}
        }
    }
    assert!(
        endpoints
            .iter()
            .all(|(from, to)| live.contains(from) && live.contains(to)),
        "an edge outlived an endpoint: {endpoints:?} over {live:?}"
    );
    assert_eq!(
        stored_edges(&db).await,
        json!(endpoints.len()),
        "the pattern scan reaches every stored edge"
    );
    Arc::try_unwrap(db)
        .unwrap_or_else(|_| panic!("the workers finished"))
        .close()
        .await
        .unwrap();
}

/// Edge inserts never lose to property writes, however busy the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn busy_nodes_abort_no_edge_insert_under_concurrent_property_writes() {
    let db = Arc::new(test_support::open_db("endpoint-busy-node").await);
    let hub = test_support::add_user(&db, "hub").await;
    let spoke = test_support::add_user(&db, "spoke").await;
    let writer = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            let mut round = 0;
            while round < 512 {
                let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                ctx.enable_request_write_scope().await.unwrap();
                let input =
                    ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(hub))]);
                ctx.execute_mutation(
                    input,
                    &exec::ExecMutationPlan::SetProperty {
                        name: name("touched"),
                        value: ir::PropertyInputPlan::Value(PropertyValue::I64(round)),
                    },
                )
                .await
                .unwrap();
                ctx.commit_request_write_scope().await.unwrap();
                round += 1;
            }
        })
    };
    let mut inserts = 0;
    while !writer.is_finished() || inserts < 64 {
        let mut insert = stage_edge(&db, hub, spoke, Source::KvRead).await;
        tokio::task::yield_now().await;
        insert
            .commit_request_write_scope()
            .await
            .expect("a property write cannot abort this insert");
        inserts += 1;
    }
    writer.await.unwrap();
    assert_eq!(stored_edges(&db).await, json!(inserts));
    Arc::try_unwrap(db)
        .unwrap_or_else(|_| panic!("the writer finished"))
        .close()
        .await
        .unwrap();
}
