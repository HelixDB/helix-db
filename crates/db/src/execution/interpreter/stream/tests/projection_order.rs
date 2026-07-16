use super::support::*;

#[tokio::test]
async fn explicit_sort_orders_by_property_and_stable_row_tiebreaker() {
    let db = test_support::open_db("stream-explicit-order").await;
    let score_two_a = test_support::add_node_with_properties(
        &db,
        "Metric",
        vec![("score", PropertyValue::I64(2))],
    )
    .await;
    let missing_score = test_support::add_node_with_properties(
        &db,
        "Metric",
        vec![("name", PropertyValue::from("missing"))],
    )
    .await;
    let score_one = test_support::add_node_with_properties(
        &db,
        "Metric",
        vec![("score", PropertyValue::I64(1))],
    )
    .await;
    let score_two_b = test_support::add_node_with_properties(
        &db,
        "Metric",
        vec![("score", PropertyValue::I64(2))],
    )
    .await;

    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let order_id = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Order {
                    plan: ir::OrderPlan::ExplicitSort(order_keys("score", Order::Asc)),
                },
            ),
            test_support::step(
                3,
                vec![order_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(
                ids_param,
                ids_value(&[score_two_b, missing_score, score_two_a, score_one]),
            ),
        )
        .await
        .expect("explicit order executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(missing_score),
            ExecutionScalar::NodeId(score_one),
            ExecutionScalar::NodeId(score_two_a),
            ExecutionScalar::NodeId(score_two_b),
        ])
    );
}

#[tokio::test]
async fn value_map_projection_applies_selected_property_contract() {
    let db = test_support::open_db("stream-value-map-selected").await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("ada")),
            ("city", PropertyValue::from("london")),
        ],
    )
    .await;
    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::ValueMap(ir::PropertySelection::Selected(
                        property_names(vec!["name"]),
                    )),
                },
            ),
        ],
        2,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param, PropertyValue::I64(ada as i64)),
        )
        .await
        .expect("selected value map executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "name".to_string(),
            DbPropertyValue::String("ada".to_string()),
        )]))])
    );
}

#[tokio::test]
async fn binding_projection_coalesces_and_deduplicates_rows() {
    let db = test_support::open_db("stream-binding-projection-dedup").await;
    let first = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("ada"))],
    )
    .await;
    let duplicate = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("ada"))],
    )
    .await;
    let ids_param = name("ids");
    let binding = name("person");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let bind_id = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Bind(binding.clone())),
                },
            ),
            test_support::step(
                3,
                vec![bind_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::ProjectBindings {
                        projections: binding_projection_items(vec![
                            ir::BindingProjectionPlan::Coalesce {
                                refs: binding_refs(vec![
                                    ir::BindingValueRefPlan {
                                        target: ir::BindingTargetPlan::Current,
                                        source: name("nickname"),
                                    },
                                    ir::BindingValueRefPlan {
                                        target: ir::BindingTargetPlan::Binding(binding.clone()),
                                        source: name("name"),
                                    },
                                ]),
                                alias: name("display"),
                            },
                        ]),
                        dedup: ir::ProjectionDedupMode::Distinct,
                    },
                },
            ),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param, ids_value(&[first, duplicate])),
        )
        .await
        .expect("binding projection executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "display".to_string(),
            DbPropertyValue::String("ada".to_string()),
        )]))])
    );
}

#[tokio::test]
async fn label_and_edge_properties_projection_read_stored_properties() {
    let db = test_support::open_db("stream-projection-label-edge-properties").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let edge_id = test_support::add_edge_with_properties(
        &db,
        alice,
        bob,
        "KNOWS",
        vec![
            ("active", PropertyValue::Bool(true)),
            ("since", PropertyValue::I64(2024)),
        ],
    )
    .await;

    let node_param = name("node");
    let label_plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, node_param.clone()),
            test_support::step(
                2,
                vec![exec::ExecStepId::new(1).expect("positive step id")],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Label,
                },
            ),
        ],
        2,
    );
    let label_result = db
        .execute(
            &label_plan,
            context::ParamBindings::default()
                .with_value(node_param, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("label projection executes")
        .last
        .expect("project step returns a value");
    assert_eq!(
        label_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Value(DbPropertyValue::String(
            "User".to_string(),
        ))])
    );

    let edge_param = name("edge");
    let properties_plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            edge_access_step(1, edge_param.clone()),
            test_support::step(
                2,
                vec![exec::ExecStepId::new(1).expect("positive step id")],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::EdgeProperties,
                },
            ),
        ],
        2,
    );
    let properties_result = db
        .execute(
            &properties_plan,
            context::ParamBindings::default()
                .with_value(edge_param, PropertyValue::I64(edge_id as i64)),
        )
        .await
        .expect("edge properties projection executes")
        .last
        .expect("project step returns a value");
    assert_eq!(
        properties_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            (
                "$label".to_string(),
                DbPropertyValue::String("KNOWS".to_string()),
            ),
            ("active".to_string(), DbPropertyValue::Bool(true)),
            ("since".to_string(), DbPropertyValue::I64(2024)),
        ]))])
    );
}
