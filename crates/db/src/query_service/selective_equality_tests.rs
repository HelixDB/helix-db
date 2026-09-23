//! Planner/executor contract with real tenant catalogs and retained snapshots.

use helix_ast::{batch, expr, graph, index, query, traversal, value};
use helix_planner::{context, diagnostics, planning};

use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::{execution_control, index_lifecycle, HelixDB, HelixDbSource};

#[tokio::test]
async fn unique_membership_reader_uses_batch_and_honors_cancellation() {
    let store = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
    let writer = HelixDB::open_with_object_store_for_tests("unique-reader", store.clone())
        .await
        .unwrap();
    writer
        .install_index_for_tests(
            crate::config::SecondaryIndexDefinition::node_unique_equality("Fixture", "key")
                .unwrap()
                .try_into()
                .unwrap(),
        )
        .await
        .unwrap();
    let seed = (0..128_i64).fold(batch::write_batch(), |write, key| {
        write.var_as(
            &format!("node{key}"),
            traversal::g().add_n("Fixture", vec![("key", value::PropertyInput::from(key))]),
        )
    });
    writer
        .query(query::QueryRequest::write(seed))
        .await
        .unwrap();
    let own_write = writer
        .query(query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Fixture",
                        vec![("key", value::PropertyInput::from(129_i64))],
                    ),
                )
                .var_as(
                    "result",
                    traversal::g()
                        .n_with_label_where(
                            "Fixture",
                            expr::Predicate::is_in(
                                "key",
                                value::PropertyValue::I64Array(vec![0, 1, 2, 3, 129]),
                            ),
                        )
                        .values(vec!["key"]),
                )
                .returning(["result"]),
        ))
        .await
        .unwrap();
    assert_eq!(own_write["result"].as_array().unwrap().len(), 5);
    assert!(own_write["result"]
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["key"] == 129));
    writer.flush_writer().await.unwrap();
    let reader = HelixDB::open_reader_with_object_store_for_tests("unique-reader", store)
        .await
        .unwrap();
    let read = batch::read_batch()
        .var_as(
            "result",
            traversal::g()
                .n_with_label_where(
                    "Fixture",
                    expr::Predicate::is_in(
                        "key",
                        value::PropertyValue::I64Array(vec![0, 1, 2, 3, 4, 999]),
                    ),
                )
                .values(vec!["key"]),
        )
        .returning(["result"]);
    let params = context::ParamBindings::default();
    let mixed = reader
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "result",
                    traversal::g()
                        .n_with_label_where(
                            "Fixture",
                            expr::Predicate::is_in(
                                "key",
                                value::PropertyValue::Array(vec![
                                    value::PropertyValue::F32(0.0),
                                    value::PropertyValue::F64(1.0),
                                    value::PropertyValue::I64(2),
                                    value::PropertyValue::F32(3.0),
                                    value::PropertyValue::F64(4.0),
                                ]),
                            ),
                        )
                        .values(vec!["key"]),
                )
                .returning(["result"]),
        ))
        .await
        .unwrap();
    assert_eq!(mixed["result"].as_array().unwrap().len(), 5);
    let prepared = reader
        .planner_context_scoped_prepared(params.clone(), DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let plan = planning::plan_read_batch(&read, prepared.context()).unwrap();
    index_lifecycle::secondary::reset_equality_read_metrics();
    let result = reader
        .execute_prepared_scoped_controlled(
            &plan,
            params.clone(),
            DataScope::LegacyUnscoped,
            execution_control::ExecutionControl::unlimited(),
            prepared.into_catalog_proof(),
        )
        .await
        .unwrap();
    let metrics = index_lifecycle::secondary::equality_read_metrics();
    assert_eq!(metrics.multi_get_calls, 1);
    assert_eq!(metrics.point_reads, 6);
    assert_eq!(metrics.graph_reads, 5);
    assert_eq!(metrics.scans, 0);
    assert_eq!(
        super::QueryResponse::from_execution_result(result)
            .unwrap()
            .returns()["result"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
    let prepared = reader
        .planner_context_scoped_prepared(params.clone(), DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let cancellation = execution_control::ReaderRetirementCancellation::new();
    cancellation.cancel();
    let error = reader
        .execute_prepared_scoped_controlled(
            &plan,
            params,
            DataScope::LegacyUnscoped,
            execution_control::ExecutionControl::unlimited()
                .with_reader_retirement_cancellation(cancellation),
            prepared.into_catalog_proof(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::error::HelixDbError::QueryCancelledByReaderRetirement
    ));
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn unique_membership_preserves_tenant_and_prepared_snapshot() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "unique-membership-snapshot".into(),
    })
    .await
    .unwrap();
    let scopes = ["00000000000000000000000001", "00000000000000000000000002"]
        .map(|id| DataScope::Tenant(TenantId::from_ulid_str(id).unwrap()));
    for (tenant, scope) in scopes.into_iter().enumerate() {
        let receipt = db
            .query_scoped(
                query::QueryRequest::write(
                    batch::write_batch()
                        .var_as(
                            "index",
                            traversal::g().create_index_if_not_exists(
                                index::IndexSpec::node_unique_equality("Fixture", "key"),
                            ),
                        )
                        .returning(["index"]),
                ),
                scope,
            )
            .await
            .unwrap();
        let operation = receipt["index"]["operation_id"].as_str().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let status = db
                    .query_scoped(
                        query::QueryRequest::read(
                            batch::read_batch()
                                .var_as("status", traversal::g().get_index_operation(operation))
                                .returning(["status"]),
                        ),
                        scope,
                    )
                    .await
                    .unwrap();
                match status["status"]["status"].as_str().unwrap() {
                    "succeeded" => break,
                    "queued" | "running" => tokio::task::yield_now().await,
                    other => panic!("index failed: {other}"),
                }
            }
        })
        .await
        .unwrap();
        let write = (0..16_i64).fold(batch::write_batch(), |write, key| {
            write.var_as(
                &format!("node{key}"),
                traversal::g().add_n(
                    "Fixture",
                    vec![
                        ("key", value::PropertyInput::from(key)),
                        ("tenant", value::PropertyInput::from(tenant as i64)),
                    ],
                ),
            )
        });
        db.query_scoped(query::QueryRequest::write(write), scope)
            .await
            .unwrap();
    }
    for (tenant, scope) in scopes.into_iter().enumerate() {
        let read = batch::read_batch()
            .var_as(
                "result",
                traversal::g()
                    .n_with_label_where(
                        "Fixture",
                        expr::Predicate::is_in(
                            "key",
                            value::PropertyValue::I64Array(vec![0, 1, 2, 3, 4, 4, 99]),
                        ),
                    )
                    .values(vec!["key", "tenant"]),
            )
            .returning(["result"]);
        let params = context::ParamBindings::default();
        let prepared = db
            .planner_context_scoped_prepared(params.clone(), scope)
            .await
            .unwrap();
        let (plan, diagnostics) =
            planning::plan_read_batch_with_diagnostics(&read, prepared.context())
                .unwrap()
                .into_parts();
        assert_eq!(diagnostics.statistics.node_accesses.label_scans, 0);
        db.query_scoped(
            query::QueryRequest::write(
                batch::write_batch().var_as(
                    "changed",
                    traversal::g()
                        .n_with_label_where("Fixture", expr::Predicate::eq("key", 0_i64))
                        .set_property("key", 100_i64),
                ),
            ),
            scope,
        )
        .await
        .unwrap();
        let result = db
            .execute_prepared_scoped_controlled(
                &plan,
                params.clone(),
                scope,
                execution_control::ExecutionControl::unlimited(),
                prepared.into_catalog_proof(),
            )
            .await
            .unwrap();
        let response = super::QueryResponse::from_execution_result(result).unwrap();
        let rows = response.returns()["result"].as_array().unwrap();
        assert_eq!(rows.len(), 5);
        assert!(rows
            .iter()
            .all(|row| row["tenant"] == tenant && row["key"].as_i64().unwrap() < 5));
        let fresh = db.execute_scoped(&plan, params, scope).await.unwrap();
        let fresh = super::QueryResponse::from_execution_result(fresh).unwrap();
        assert_eq!(fresh.returns()["result"].as_array().unwrap().len(), 4);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn selective_equality_preserves_tenant_snapshot_and_churn_results() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "selective-equality-snapshots".into(),
    })
    .await
    .unwrap();
    let scopes = ["00000000000000000000000001", "00000000000000000000000002"]
        .map(|id| DataScope::Tenant(TenantId::from_ulid_str(id).unwrap()));
    for (scope_index, scope) in scopes.into_iter().enumerate() {
        for property in ["tenant", "type", "deleted", "zone", "category"] {
            for spec in [
                index::IndexSpec::node_equality("Resource", property),
                index::IndexSpec::edge_equality("Resource", property),
            ] {
                let receipt = db
                    .query_scoped(
                        query::QueryRequest::write(
                            batch::write_batch()
                                .var_as("index", traversal::g().create_index_if_not_exists(spec))
                                .returning(["index"]),
                        ),
                        scope,
                    )
                    .await
                    .unwrap();
                let operation = receipt["index"]["operation_id"].as_str().unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(30), async {
                    loop {
                        let status = db
                            .query_scoped(
                                query::QueryRequest::read(
                                    batch::read_batch()
                                        .var_as(
                                            "status",
                                            traversal::g().get_index_operation(operation),
                                        )
                                        .returning(["status"]),
                                ),
                                scope,
                            )
                            .await
                            .unwrap();
                        match status["status"]["status"].as_str().unwrap() {
                            "succeeded" => break,
                            "queued" | "running" => tokio::task::yield_now().await,
                            state => panic!("index operation failed: {state}"),
                        }
                    }
                })
                .await
                .unwrap();
            }
        }
        let mut write = batch::write_batch()
            .var_as(
                "source",
                traversal::g().add_n("Anchor", Vec::<(&str, value::PropertyInput)>::new()),
            )
            .var_as(
                "target",
                traversal::g().add_n("Anchor", Vec::<(&str, value::PropertyInput)>::new()),
            );
        for ordinal in 0..32_i64 {
            let properties = vec![
                ("tenant", value::PropertyInput::from("one")),
                (
                    "type",
                    value::PropertyInput::from(if ordinal % 2 == 0 { "pod" } else { "service" }),
                ),
                ("deleted", value::PropertyInput::from(ordinal % 5 == 0)),
                ("zone", value::PropertyInput::from("zone-a")),
                ("category", value::PropertyInput::from("synthetic")),
                (
                    "ordinal",
                    value::PropertyInput::from(ordinal + scope_index as i64 * 100),
                ),
            ];
            write = write
                .var_as(
                    &format!("node{ordinal}"),
                    traversal::g().add_n("Resource", properties.clone()),
                )
                .var_as(
                    &format!("edge{ordinal}"),
                    traversal::g().n(graph::NodeRef::var("source")).add_e(
                        "Resource",
                        graph::NodeRef::var("target"),
                        properties,
                    ),
                );
        }
        db.query_scoped(query::QueryRequest::write(write), scope)
            .await
            .unwrap();
    }
    // Cluster-scoped inventory must not stand in for the tenant's catalog.
    assert!(db.index_catalog_snapshot().node_eq.is_empty());
    // An empty union branch must not erase matches, and another tenant's
    // ordinals must not leak through the shared equality predicate.
    for (scope_index, scope) in scopes.into_iter().enumerate() {
        let predicate = expr::Predicate::and(vec![
            expr::Predicate::or(vec![
                expr::Predicate::eq("type", "pod"),
                expr::Predicate::eq("type", "missing"),
            ]),
            expr::Predicate::eq("tenant", "one"),
        ]);
        for traversal in [
            traversal::g()
                .n_with_label_where("Resource", predicate.clone())
                .values(vec!["ordinal"]),
            traversal::g()
                .e_with_label_where("Resource", predicate)
                .values(vec!["ordinal"]),
        ] {
            let result = db
                .query_scoped(
                    query::QueryRequest::read(
                        batch::read_batch()
                            .var_as("result", traversal)
                            .returning(["result"]),
                    ),
                    scope,
                )
                .await
                .unwrap();
            let expected = (0..32)
                .step_by(2)
                .map(|ordinal| serde_json::json!({"ordinal": ordinal + scope_index * 100}))
                .collect::<Vec<_>>();
            assert_eq!(result["result"], serde_json::json!(expected));
        }
    }
    let predicate = expr::Predicate::and(vec![
        expr::Predicate::eq("tenant", "one"),
        expr::Predicate::eq("type", "pod"),
        expr::Predicate::eq_param("deleted", "deleted"),
        expr::Predicate::and(vec![
            expr::Predicate::eq("zone", "zone-a"),
            expr::Predicate::eq("category", "synthetic"),
        ]),
    ]);
    let params = context::ParamBindings::default().with_value(
        helix_planner::ir::NonEmptyString::new("deleted").unwrap(),
        value::PropertyValue::Bool(true),
    );
    for (scope_index, scope) in scopes.into_iter().enumerate() {
        for read in [
            batch::read_batch()
                .var_as(
                    "result",
                    traversal::g()
                        .n_with_label_where("Resource", predicate.clone())
                        .values(vec!["ordinal"]),
                )
                .returning(["result"]),
            batch::read_batch()
                .var_as(
                    "result",
                    traversal::g()
                        .e_with_label_where("Resource", predicate.clone())
                        .values(vec!["ordinal"]),
                )
                .returning(["result"]),
        ] {
            for stale_statistics in [false, true] {
                let prepared = db
                    .planner_context_scoped_prepared(params.clone(), scope)
                    .await
                    .unwrap();
                assert_eq!(prepared.context().stats, context::StatsSnapshot::default());
                let mut planner_context = prepared.context().clone();
                if stale_statistics {
                    for property in ["tenant", "type", "deleted", "zone", "category"] {
                        let key = helix_planner::catalog::ScopedPropertyKey::try_new(
                            "Resource", property,
                        )
                        .unwrap();
                        planner_context.stats = planner_context
                            .stats
                            .with_node_eq_cardinality(key.clone(), 0)
                            .with_edge_eq_cardinality(key, 0);
                    }
                }
                let (plan, diagnostics) =
                    planning::plan_read_batch_with_diagnostics(&read, &planner_context)
                        .unwrap()
                        .into_parts();
                assert!(diagnostics.insights.iter().all(|insight| !matches!(
                    insight,
                    diagnostics::PlannerInsight::UnboundedScan(_)
                )));
                // Advance storage after planning. The prepared read must retain the
                // original graph and bitmap snapshot, including all four matches.
                let literal = expr::Predicate::and(vec![
                    expr::Predicate::eq("tenant", "one"),
                    expr::Predicate::eq("type", "pod"),
                    expr::Predicate::eq("deleted", true),
                ]);
                db.query_scoped(
                    query::QueryRequest::write(
                        batch::write_batch()
                            .var_as(
                                "nodes",
                                traversal::g()
                                    .n_with_label_where("Resource", literal.clone())
                                    .set_property("deleted", false),
                            )
                            .var_as(
                                "edges",
                                traversal::g()
                                    .e_with_label_where("Resource", literal)
                                    .set_property("deleted", false),
                            ),
                    ),
                    scope,
                )
                .await
                .unwrap();
                index_lifecycle::secondary::reset_equality_read_metrics();
                let result = db
                    .execute_prepared_scoped_controlled(
                        &plan,
                        params.clone(),
                        scope,
                        execution_control::ExecutionControl::unlimited(),
                        prepared.into_catalog_proof(),
                    )
                    .await
                    .unwrap();
                let metrics = index_lifecycle::secondary::equality_read_metrics();
                assert_eq!(metrics.scans, 0);
                assert_eq!(metrics.graph_reads, 0);
                assert_eq!(metrics.point_reads, 1);
                let response = super::QueryResponse::from_execution_result(result).unwrap();
                let expected = [0, 10, 20, 30]
                    .map(|ordinal| serde_json::json!({"ordinal": ordinal + scope_index * 100}));
                assert_eq!(response.returns()["result"], serde_json::json!(expected));
                let fresh = db
                    .execute_scoped(&plan, params.clone(), scope)
                    .await
                    .unwrap();
                let fresh = super::QueryResponse::from_execution_result(fresh).unwrap();
                assert_eq!(fresh.returns()["result"], serde_json::json!([]));
                // Reinsert matching membership for the next node/edge iteration.
                db.query_scoped(
                    query::QueryRequest::write(
                        batch::write_batch()
                            .var_as(
                                "nodes",
                                traversal::g()
                                    .n_with_label_where(
                                        "Resource",
                                        expr::Predicate::is_in(
                                            "ordinal",
                                            value::PropertyValue::I64Array(
                                                vec![0, 10, 20, 30]
                                                    .into_iter()
                                                    .map(|n| n + scope_index as i64 * 100)
                                                    .collect(),
                                            ),
                                        ),
                                    )
                                    .set_property("deleted", true),
                            )
                            .var_as(
                                "edges",
                                traversal::g()
                                    .e_with_label_where(
                                        "Resource",
                                        expr::Predicate::is_in(
                                            "ordinal",
                                            value::PropertyValue::I64Array(
                                                vec![0, 10, 20, 30]
                                                    .into_iter()
                                                    .map(|n| n + scope_index as i64 * 100)
                                                    .collect(),
                                            ),
                                        ),
                                    )
                                    .set_property("deleted", true),
                            ),
                    ),
                    scope,
                )
                .await
                .unwrap();
            }
        }
    }
    db.close().await.unwrap();
}
