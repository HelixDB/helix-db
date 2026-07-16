//! Production-linked vector DDL and graph-mutation contracts.
//!
//! These tests execute current planner IR through [`db::HelixDB`] without
//! `cfg(test)` constructors. They wait for accepted asynchronous DDL operations
//! to reach a terminal state before proving that dynamic vector-index creation
//! backfills existing graph rows and that committed node-property mutations
//! keep the active physical generation synchronized.

use std::num::NonZeroUsize;
use std::time::Duration;

use db::config::VectorElementType;
use db::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionScalar, ExecutionValue,
};
use db::index_v2::{IndexDdlReceipt, IndexOperationStatus};
use db::search::vector_index_name;
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::value::PropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};

/// Constructs a non-empty planner identifier used by the executable fixtures.
fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("fixture identifiers are non-empty")
}

/// Constructs one executable step with the planner's neutral scheduling data.
fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("fixture step ids are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

/// Seals a linear fixture DAG behind the same validated plan boundary used by production.
fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("fixture plans are non-empty"),
        exec::ExecStepId::new(root).expect("fixture root ids are positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("fixture dependencies form a valid executable plan")
}

/// Converts literal graph properties into the planner's duplicate-free assignment type.
fn assignments(items: Vec<(&str, PropertyValue)>) -> ir::PropertyAssignments {
    ir::PropertyAssignments::try_from_vec(
        items
            .into_iter()
            .map(|(property, value)| (name(property), ir::PropertyInputPlan::Value(value)))
            .collect(),
    )
    .expect("fixture property names are unique")
}

/// Constructs a non-empty, validated physical ID list for point targets.
fn ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(
        ir::AtLeast::<_, 1>::try_from_vec(values).expect("fixture id lists are non-empty"),
    )
    .expect("fixture ids are valid")
}

/// Builds dynamic node-vector DDL with an explicit dimension and distance identity.
fn node_vector_ddl_plan(
    label: &str,
    property: &str,
    metric: ir::VectorIndexMetric,
) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::NodeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                        dimension: ir::VectorIndexDimension::new(2)
                            .expect("fixture vector dimension is positive"),
                        metric,
                        scope: catalog::SearchIndexScope::Unscoped,
                    },
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds dynamic node-vector drop DDL for the same scoped catalog identity.
fn node_vector_drop_plan(label: &str, property: &str) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Drop {
                    spec: ir::IndexDdlDropSpec::NodeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                    },
                },
            },
        )],
        1,
    )
}

/// Builds dynamic edge-vector DDL with an explicit dimension and distance identity.
fn edge_vector_ddl_plan(label: &str, property: &str) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::EdgeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                        dimension: ir::VectorIndexDimension::new(2)
                            .expect("fixture vector dimension is positive"),
                        metric: ir::VectorIndexMetric::Euclidean,
                        scope: catalog::SearchIndexScope::Unscoped,
                    },
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds a source-node mutation and returns the created row as the plan root.
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

/// Builds an edge mutation whose source is selected through a bound node ID.
fn add_edge_plan(
    from_param: ir::NonEmptyString,
    to: u64,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
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
                        to: ir::NodeTargetPlan::PointIds { ids: ids(vec![to]) },
                        properties: assignments(properties),
                    },
                },
            ),
        ],
        2,
    )
}

/// Builds a top-one vector search followed by an ID projection.
fn node_vector_search_plan(label: &str, property: &str, query: Vec<f32>) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    let index_name = vector_index_name(VectorElementType::Node, label, property);
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::VectorSearch {
                            key: catalog::NodeSearchIndexKey::try_new(label, property)
                                .expect("fixture search key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&index_name),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_vector: ir::VectorQueryInputPlan::Vector(
                                ir::SearchVector::new(query)
                                    .expect("fixture query vector is non-empty and finite"),
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

/// Builds a top-one edge-vector search followed by an ID projection.
fn edge_vector_search_plan(label: &str, property: &str, query: Vec<f32>) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    let index_name = vector_index_name(VectorElementType::Edge, label, property);
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::VectorSearch {
                            key: catalog::EdgeSearchIndexKey::try_new(label, property)
                                .expect("fixture search key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&index_name),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_vector: ir::VectorQueryInputPlan::Vector(
                                ir::SearchVector::new(query)
                                    .expect("fixture query vector is non-empty and finite"),
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

/// Builds a property mutation for a node selected through a bound ID parameter.
fn node_property_mutation_plan(
    node_param: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: node_param },
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

/// Builds a property mutation for an edge selected through a bound ID parameter.
fn edge_property_mutation_plan(
    edge_param: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::FromParam { param: edge_param },
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

/// Builds a source mutation that drops one edge by its exact physical ID.
fn drop_edge_by_id_plan(edge_id: u64) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::DropEdgeByIdSource {
                    edges: ir::EdgeTargetPlan::PointIds {
                        ids: ids(vec![edge_id]),
                    },
                },
            },
        )],
        1,
    )
}

/// Extracts the single node produced by an add-node fixture.
fn created_node_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("add-node fixture should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("add-node fixture should return one node row");
    };
    *id
}

/// Extracts the single edge produced by an add-edge fixture.
fn created_edge_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("add-edge fixture should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Edge(id)),
        ..
    }) = rows.first()
    else {
        panic!("add-edge fixture should return one edge row");
    };
    *id
}

/// Extracts node IDs from a vector-search projection without accepting mixed scalar kinds.
fn projected_node_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("vector-search fixture should return projected scalars");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::NodeId(id) = value else {
                panic!("node-vector projection should contain only node ids");
            };
            id
        })
        .collect()
}

/// Extracts edge IDs from a vector-search projection without accepting mixed scalar kinds.
fn projected_edge_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("vector-search fixture should return projected scalars");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::EdgeId(id) = value else {
                panic!("edge-vector projection should contain only edge ids");
            };
            id
        })
        .collect()
}

/// Executes one DDL plan and waits for its durable operation to terminate.
async fn execute_ddl_to_success(db: &HelixDB, plan: &exec::ExecutablePlan) {
    let result = db
        .execute(plan, context::ParamBindings::default())
        .await
        .expect("fixture DDL is durably accepted");
    let Some(ExecutionValue::IndexDdlReceipt(IndexDdlReceipt::Accepted { operation_id, .. })) =
        result.last
    else {
        panic!("new fixture DDL should return an accepted operation");
    };

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match db
                .get_index_operation(
                    db::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    operation_id,
                )
                .await
                .expect("accepted fixture operation remains readable")
            {
                IndexOperationStatus::Succeeded { .. } => break,
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                IndexOperationStatus::Blocked { .. } | IndexOperationStatus::Aborted { .. } => {
                    panic!("fixture DDL should complete successfully")
                }
            }
        }
    })
    .await
    .expect("fixture DDL worker should converge");
    db.planner_context_scoped(
        context::ParamBindings::default(),
        db::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
    )
    .await
    .expect("terminal DDL is visible through a refreshed planner catalog");
}

/// Executes a vector search with no parameters and returns projected node IDs.
async fn search_node_ids(db: &HelixDB, query: Vec<f32>) -> Vec<u64> {
    projected_node_ids(
        db.execute(
            &node_vector_search_plan("Doc", "embedding", query),
            context::ParamBindings::default(),
        )
        .await
        .expect("fixture vector search succeeds"),
    )
}

/// Executes an edge-vector search with no parameters and returns projected IDs.
async fn search_edge_ids(db: &HelixDB, query: Vec<f32>) -> Vec<u64> {
    projected_edge_ids(
        db.execute(
            &edge_vector_search_plan("LINK", "embedding", query),
            context::ParamBindings::default(),
        )
        .await
        .expect("fixture edge-vector search succeeds"),
    )
}

#[tokio::test]
async fn public_dynamic_vector_ddl_backfills_existing_nodes() {
    let database = "production-vector-ddl-backfill";
    let token = ProcessLocalDatabaseToken::new(database).expect("fixture token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("writer opens");

    let first = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("first node commits before DDL"),
    );
    created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![0.0, 1.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("second node commits before DDL"),
    );

    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;

    assert_eq!(search_node_ids(&db, vec![1.0, 0.0]).await, vec![first]);
    db.close().await.expect("first writer closes");

    let reopened = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("managed writer reopens");
    assert_eq!(
        search_node_ids(&reopened, vec![1.0, 0.0]).await,
        vec![first]
    );
    execute_ddl_to_success(&reopened, &node_vector_drop_plan("Doc", "embedding")).await;
    assert!(reopened
        .execute(
            &node_vector_search_plan("Doc", "embedding", vec![1.0, 0.0]),
            context::ParamBindings::default(),
        )
        .await
        .is_err());
    reopened.close().await.expect("reopened writer closes");
}

#[tokio::test]
async fn public_managed_search_executes_every_active_vector_metric() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-active-metrics")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    let mut node_ids = Vec::new();
    for offset in 0_u16..20 {
        let displacement = f32::from(offset) / 20.0;
        node_ids.push(created_node_id(
            db.execute(
                &add_node_plan(
                    "Doc",
                    vec![
                        (
                            "cosine_embedding",
                            PropertyValue::F32Array(vec![1.0 - displacement, displacement]),
                        ),
                        (
                            "manhattan_embedding",
                            PropertyValue::F32Array(vec![1.0 - displacement, displacement]),
                        ),
                    ],
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("fixture node commits before DDL"),
        ));
    }

    for (property, metric) in [
        ("cosine_embedding", ir::VectorIndexMetric::Cosine),
        ("manhattan_embedding", ir::VectorIndexMetric::Manhattan),
    ] {
        execute_ddl_to_success(&db, &node_vector_ddl_plan("Doc", property, metric)).await;
        assert_eq!(
            projected_node_ids(
                db.execute(
                    &node_vector_search_plan("Doc", property, vec![1.0, 0.0]),
                    context::ParamBindings::default(),
                )
                .await
                .expect("managed metric-specific vector search succeeds"),
            ),
            vec![node_ids[0]]
        );
    }

    db.close().await.expect("writer closes");
}

#[tokio::test]
async fn public_node_mutations_keep_vector_generation_synchronized() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-node-mutations")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;

    let node_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("node insertion updates the vector generation"),
    );
    assert_eq!(search_node_ids(&db, vec![1.0, 0.0]).await, vec![node_id]);

    let node_param = name("node");
    db.execute(
        &node_property_mutation_plan(
            node_param.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name("embedding"),
                value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![0.0, 1.0])),
            },
        ),
        context::ParamBindings::default()
            .with_value(node_param.clone(), PropertyValue::I64(node_id as i64)),
    )
    .await
    .expect("set-property replaces the indexed vector");
    assert_eq!(search_node_ids(&db, vec![0.0, 1.0]).await, vec![node_id]);

    db.execute(
        &node_property_mutation_plan(
            node_param.clone(),
            exec::ExecMutationPlan::RemoveProperty {
                name: name("embedding"),
            },
        ),
        context::ParamBindings::default()
            .with_value(node_param.clone(), PropertyValue::I64(node_id as i64)),
    )
    .await
    .expect("remove-property deletes the indexed vector");
    assert!(search_node_ids(&db, vec![0.0, 1.0]).await.is_empty());

    let dropped_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("replacement node updates the vector generation"),
    );
    db.execute(
        &node_property_mutation_plan(node_param.clone(), exec::ExecMutationPlan::Drop),
        context::ParamBindings::default()
            .with_value(node_param, PropertyValue::I64(dropped_id as i64)),
    )
    .await
    .expect("drop-node removes the indexed vector");
    assert!(search_node_ids(&db, vec![1.0, 0.0]).await.is_empty());
}

#[tokio::test]
async fn public_edge_mutations_keep_vector_generation_synchronized() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-edge-mutations")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(&db, &edge_vector_ddl_plan("LINK", "embedding")).await;

    let source = created_node_id(
        db.execute(
            &add_node_plan("Source", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("source node commits"),
    );
    let target = created_node_id(
        db.execute(
            &add_node_plan("Target", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("target node commits"),
    );
    let source_param = name("source");
    let edge_id = created_edge_id(
        db.execute(
            &add_edge_plan(
                source_param.clone(),
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(source_param.clone(), PropertyValue::I64(source as i64)),
        )
        .await
        .expect("edge insertion updates the vector generation"),
    );
    assert_eq!(search_edge_ids(&db, vec![1.0, 0.0]).await, vec![edge_id]);

    let edge_param = name("edge");
    db.execute(
        &edge_property_mutation_plan(
            edge_param.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name("embedding"),
                value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![0.0, 1.0])),
            },
        ),
        context::ParamBindings::default()
            .with_value(edge_param.clone(), PropertyValue::I64(edge_id as i64)),
    )
    .await
    .expect("edge set-property replaces the indexed vector");
    assert_eq!(search_edge_ids(&db, vec![0.0, 1.0]).await, vec![edge_id]);

    db.execute(
        &edge_property_mutation_plan(
            edge_param.clone(),
            exec::ExecMutationPlan::RemoveProperty {
                name: name("embedding"),
            },
        ),
        context::ParamBindings::default()
            .with_value(edge_param, PropertyValue::I64(edge_id as i64)),
    )
    .await
    .expect("edge remove-property deletes the indexed vector");
    assert!(search_edge_ids(&db, vec![0.0, 1.0]).await.is_empty());

    let dropped_edge_id = created_edge_id(
        db.execute(
            &add_edge_plan(
                source_param.clone(),
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(source_param, PropertyValue::I64(source as i64)),
        )
        .await
        .expect("replacement edge updates the vector generation"),
    );
    db.execute(
        &drop_edge_by_id_plan(dropped_edge_id),
        context::ParamBindings::default(),
    )
    .await
    .expect("drop-edge-by-id removes the indexed vector");
    assert!(search_edge_ids(&db, vec![1.0, 0.0]).await.is_empty());
}
