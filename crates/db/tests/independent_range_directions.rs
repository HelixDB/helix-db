//! Public AST -> planner -> executor contracts for independent range indexes.

use db::encoding::v2::keys::scope::{DataScope, TenantId};
use db::{HelixDB, HelixDbSource};
use helix_ast::expr::{Expr, Predicate, StreamBound};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index::{IndexSpec, RangeIndexDirection};
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::traversal::Order;
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};
use helix_planner::{context, planning};
use std::time::Duration;

async fn ddl(db: &HelixDB, scope: DataScope, spec: IndexSpec, drop: bool) -> serde_json::Value {
    let operation = if drop {
        traversal::g().drop_index(spec)
    } else {
        traversal::g().create_index_if_not_exists(spec)
    };
    let receipt = db
        .query_scoped(
            QueryRequest::write(
                batch::write_batch()
                    .var_as("op", operation)
                    .returning(["op"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let Some(id) = receipt["op"]["operation_id"].as_str() else {
        return receipt;
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = db
                .query_scoped(
                    QueryRequest::read(
                        batch::read_batch()
                            .var_as("status", traversal::g().get_index_operation(id))
                            .returning(["status"]),
                    ),
                    scope,
                )
                .await
                .unwrap();
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::task::yield_now().await,
                _ => panic!("DDL failed: {status}"),
            }
        }
    })
    .await
    .unwrap();
    receipt
}

fn spec(edge: bool, direction: RangeIndexDirection) -> IndexSpec {
    if edge {
        IndexSpec::edge_range_with_direction("Item", "value", direction)
    } else {
        IndexSpec::node_range_with_direction("Item", "value", direction)
    }
}

async fn ordered_ids(
    db: &HelixDB,
    scope: DataScope,
    edge: bool,
    order: Order,
    since: i64,
    limit: usize,
) -> Vec<u64> {
    let predicate = Predicate::and(vec![
        Predicate::eq("group", "keep"),
        Predicate::gte("value", since),
        Predicate::and(vec![Predicate::gte("before", 0)]),
    ]);
    let limit_bound = StreamBound::expr(Expr::param("limit"));
    let query = if edge {
        traversal::g()
            .e_with_label_where("Item", predicate)
            .order_by("value", order)
            .limit(limit_bound)
            .id()
    } else {
        traversal::g()
            .n_with_label_where("Item", predicate)
            .order_by("value", order)
            .limit(limit_bound)
            .id()
    };
    let mut request =
        QueryRequest::read(batch::read_batch().var_as("ids", query).returning(["ids"]));
    request.insert_parameter_value("limit", QueryValue::I64(limit as i64));
    let response = db.query_scoped(request, scope).await.unwrap();
    response["ids"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|id| id.as_u64().unwrap())
        .collect()
}

async fn add_item(db: &HelixDB, scope: DataScope, edge: bool, value: i64, group: &str) -> u64 {
    let properties = vec![
        ("value", PropertyInput::from(value)),
        ("group", PropertyInput::from(group)),
        ("before", PropertyInput::from(value)),
    ];
    let write = if edge {
        batch::write_batch()
            .var_as(
                "from",
                traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "to",
                traversal::g().add_n("Endpoint", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "id",
                traversal::g()
                    .n(NodeRef::var("from"))
                    .add_e("Item", NodeRef::var("to"), properties)
                    .id(),
            )
            .returning(["id"])
    } else {
        batch::write_batch()
            .var_as("id", traversal::g().add_n("Item", properties).id())
            .returning(["id"])
    };
    db.query_scoped(QueryRequest::write(write), scope)
        .await
        .unwrap()["id"][0]
        .as_u64()
        .unwrap()
}

#[test]
fn both_directions_are_independent_through_public_ddl_and_planned_queries() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(run_contract());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "independent-range-directions".into(),
    })
    .await
    .unwrap();
    for scope in [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000009").unwrap()),
        DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap()),
    ] {
        for edge in [false, true] {
            let mut rows = Vec::new();
            for ordinal in 0..12i64 {
                let id = add_item(
                    &db,
                    scope,
                    edge,
                    ordinal / 2,
                    if ordinal % 3 == 0 { "skip" } else { "keep" },
                )
                .await;
                if ordinal % 3 != 0 {
                    rows.push((ordinal / 2, id));
                }
            }
            let equality = if edge {
                IndexSpec::edge_equality("Item", "group")
            } else {
                IndexSpec::node_equality("Item", "group")
            };
            ddl(&db, scope, equality, false).await;
            let asc = ddl(&db, scope, spec(edge, RangeIndexDirection::Asc), false).await;
            let desc = ddl(&db, scope, spec(edge, RangeIndexDirection::Desc), false).await;
            assert_ne!(asc["op"]["index_id"], desc["op"]["index_id"]);
            // Writes after activation must maintain both physical indexes.
            let live_id = add_item(&db, scope, edge, 7, "keep").await;
            let update = if edge {
                traversal::g()
                    .e(EdgeRef::id(live_id))
                    .set_property("value", 8)
                    .id()
            } else {
                traversal::g()
                    .n(NodeRef::id(live_id))
                    .set_property("value", 8)
                    .id()
            };
            db.query_scoped(
                QueryRequest::write(
                    batch::write_batch()
                        .var_as("changed", update)
                        .returning(["changed"]),
                ),
                scope,
            )
            .await
            .unwrap();
            rows.push((8, live_id));
            let extra = if edge {
                IndexSpec::edge_range("Item", "before")
            } else {
                IndexSpec::node_range("Item", "before")
            };
            ddl(&db, scope, extra, false).await;
            for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
                let repeated = ddl(&db, scope, spec(edge, direction), false).await;
                assert_eq!(repeated["op"]["kind"], "already_active");
            }
            for order in [Order::Asc, Order::Desc] {
                for since in [0, 3, 99] {
                    for limit in [0, 1, 5, 30] {
                        let actual = ordered_ids(&db, scope, edge, order, since, limit).await;
                        let mut expected = rows
                            .iter()
                            .copied()
                            .filter(|(value, _)| *value >= since)
                            .collect::<Vec<_>>();
                        expected.sort_by(|(a, aid), (b, bid)| {
                            (if order == Order::Asc {
                                a.cmp(b)
                            } else {
                                b.cmp(a)
                            })
                            .then(aid.cmp(bid))
                        });
                        let expected = expected
                            .into_iter()
                            .take(limit)
                            .map(|(_, id)| id)
                            .collect::<Vec<_>>();
                        assert_eq!(actual, expected, "scope={scope:?} edge={edge} order={order:?} since={since} limit={limit}");
                    }
                }
            }
            // The actual planner sees both catalog directions. Intersect
            // lowering must drive from value DESC for this order request.
            let predicate = Predicate::and(vec![
                Predicate::eq("group", "keep"),
                Predicate::gte("value", 0),
                Predicate::gte("before", 0),
            ]);
            let read = if edge {
                traversal::g()
                    .e_with_label_where("Item", predicate)
                    .order_by("value", Order::Desc)
                    .limit(5)
                    .id()
            } else {
                traversal::g()
                    .n_with_label_where("Item", predicate)
                    .order_by("value", Order::Desc)
                    .limit(5)
                    .id()
            };
            let read = batch::read_batch().var_as("ids", read).returning(["ids"]);
            let ctx = db
                .planner_context_scoped(context::ParamBindings::default(), scope)
                .await
                .unwrap();
            let plan = planning::plan_read_batch(&read, &ctx).unwrap();
            let debug = format!("{plan:?}");
            assert!(debug.contains("direction: Desc"), "{debug}");
            assert!(!debug.contains("Sort"), "{debug}");
            let before = ordered_ids(&db, scope, edge, Order::Asc, 0, 30).await;
            ddl(&db, scope, spec(edge, RangeIndexDirection::Desc), true).await;
            assert_eq!(
                ordered_ids(&db, scope, edge, Order::Asc, 0, 30).await,
                before
            );
            ddl(&db, scope, spec(edge, RangeIndexDirection::Desc), false).await;
            assert_eq!(
                ordered_ids(&db, scope, edge, Order::Asc, 0, 30).await,
                before
            );
            let delete = if edge {
                traversal::g().drop_edge_by_id(EdgeRef::id(live_id))
            } else {
                traversal::g().n(NodeRef::id(live_id)).drop()
            };
            db.query_scoped(
                QueryRequest::write(
                    batch::write_batch()
                        .var_as("deleted", delete)
                        .returning(Vec::<String>::new()),
                ),
                scope,
            )
            .await
            .unwrap();
            for order in [Order::Asc, Order::Desc] {
                assert!(!ordered_ids(&db, scope, edge, order, 0, 30)
                    .await
                    .contains(&live_id));
            }
        }
    }
    db.close().await.unwrap();
}

#[cfg(feature = "index-lifecycle-testing")]
#[test]
fn aborting_one_backfill_preserves_the_other_direction() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    use db::index_lifecycle::{
                        IndexDdlReceipt, IndexOperationStatus, ValidatedDynamicIndexDefinition,
                    };
                    use db::index_lifecycle_testing::{
                        LifecycleTestController, LifecycleTestScheduling, LifecycleWorkTarget,
                    };
                    let db = HelixDB::open_for_index_lifecycle_testing(
                        HelixDbSource::InMemory {
                            database: "range-abort".into(),
                        },
                        db::DbConfig::new(),
                        LifecycleTestScheduling::Explicit,
                    )
                    .await
                    .unwrap();
                    let controller = LifecycleTestController::new();
                    for edge in [false, true] {
                        let scope = DataScope::Tenant(
                            TenantId::from_ulid_str("00000000000000000000000009").unwrap(),
                        );
                        add_item(&db, scope, edge, 4, "keep").await;
                        let mut definitions = Vec::new();
                        for direction in [
                            db::config::RangeIndexDirection::Asc,
                            db::config::RangeIndexDirection::Desc,
                        ] {
                            let definition: ValidatedDynamicIndexDefinition = if edge {
                                db::config::SecondaryIndexDefinition::edge_range_with_direction(
                                    "Item", "value", direction,
                                )
                                .unwrap()
                            } else {
                                db::config::SecondaryIndexDefinition::node_range_with_direction(
                                    "Item", "value", direction,
                                )
                                .unwrap()
                            }
                            .try_into()
                            .unwrap();
                            let IndexDdlReceipt::Accepted { operation_id, .. } = controller
                                .create_index(
                                    &db,
                                    scope,
                                    definition.clone(),
                                    helix_planner::ir::IndexCreateMode::IfNotExists,
                                )
                                .await
                                .unwrap()
                            else {
                                panic!("new direction must get its own operation")
                            };
                            definitions.push((definition, operation_id));
                        }
                        for _ in 0..64 {
                            if matches!(
                                db.get_index_operation(scope, definitions[0].1)
                                    .await
                                    .unwrap(),
                                IndexOperationStatus::Succeeded { .. }
                            ) {
                                break;
                            }
                            controller
                                .advance(
                                    &db,
                                    LifecycleWorkTarget::Operation {
                                        scope,
                                        operation_id: definitions[0].1,
                                    },
                                )
                                .await
                                .unwrap();
                        }
                        assert!(matches!(
                            db.get_index_operation(scope, definitions[0].1)
                                .await
                                .unwrap(),
                            IndexOperationStatus::Succeeded { .. }
                        ));
                        // Commit a real descending backfill batch before cancellation.
                        controller
                            .advance(
                                &db,
                                LifecycleWorkTarget::Operation {
                                    scope,
                                    operation_id: definitions[1].1,
                                },
                            )
                            .await
                            .unwrap();
                        controller
                            .drop_index(&db, scope, &definitions[1].0)
                            .await
                            .unwrap();
                        for _ in 0..64 {
                            if matches!(
                                db.get_index_operation(scope, definitions[1].1)
                                    .await
                                    .unwrap(),
                                IndexOperationStatus::Aborted { .. }
                            ) {
                                break;
                            }
                            controller
                                .advance(
                                    &db,
                                    LifecycleWorkTarget::Operation {
                                        scope,
                                        operation_id: definitions[1].1,
                                    },
                                )
                                .await
                                .unwrap();
                        }
                        assert!(matches!(
                            db.get_index_operation(scope, definitions[1].1)
                                .await
                                .unwrap(),
                            IndexOperationStatus::Aborted { .. }
                        ));
                        let asc = controller
                            .definition_snapshot(&db, scope, &definitions[0].0)
                            .await
                            .unwrap();
                        let desc = controller
                            .definition_snapshot(&db, scope, &definitions[1].0)
                            .await
                            .unwrap();
                        assert_eq!(asc.state, Some("active"));
                        assert_eq!(desc.state, Some("dropped"));
                        assert_eq!(
                            ordered_ids(&db, scope, edge, Order::Asc, 0, 10).await.len(),
                            1
                        );
                    }
                    db.close().await.unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}
