use super::support::*;
use std::collections::BTreeMap;

#[tokio::test]
async fn set_and_remove_property_update_stored_node_properties() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-set-remove-node-property".to_string(),
    })
    .await
    .expect("db opens");
    let id = add_user(&db, "alice").await;
    let id_param = name("id");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let set_id = exec::ExecStepId::new(2).expect("positive step id");

    let mutate = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: id_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::SetProperty {
                        name: name("status"),
                        value: ir::PropertyInputPlan::Value(PropertyValue::from("active")),
                    },
                },
            ),
            step(
                3,
                vec![set_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::RemoveProperty { name: name("name") },
                },
            ),
        ],
        3,
    );
    let params = helix_planner::context::ParamBindings::default()
        .with_value(id_param.clone(), PropertyValue::I64(id as i64));
    db.execute(&mutate, params)
        .await
        .expect("property mutation succeeds");

    let status = db
        .execute(
            &access_param_value_plan(id_param.clone(), "status"),
            helix_planner::context::ParamBindings::default()
                .with_value(id_param.clone(), PropertyValue::I64(id as i64)),
        )
        .await
        .expect("status read succeeds");
    assert_eq!(
        status.last,
        Some(ExecutionValue::Scalars(vec![ExecutionScalar::Object(
            BTreeMap::from([(
                "status".to_string(),
                DbPropertyValue::String("active".to_string())
            )])
        )]))
    );

    let removed_name = db
        .execute(
            &access_param_value_plan(id_param.clone(), "name"),
            helix_planner::context::ParamBindings::default()
                .with_value(id_param, PropertyValue::I64(id as i64)),
        )
        .await
        .expect("name read succeeds");
    assert_eq!(removed_name.last, Some(ExecutionValue::Scalars(Vec::new())));
}

#[tokio::test]
async fn add_edge_updates_adjacency_and_labeled_expansion() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-add-edge".to_string(),
    })
    .await
    .expect("db opens");
    let from = add_user(&db, "alice").await;
    let to = add_user(&db, "bob").await;
    let from_param = name("from");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");

    let add_edge = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: from_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: name("KNOWS"),
                        to: ir::NodeTargetPlan::PointIds {
                            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(to))
                                .expect("valid target"),
                        },
                        properties: assignments(Vec::new()),
                    },
                },
            ),
        ],
        2,
    );
    db.execute(
        &add_edge,
        helix_planner::context::ParamBindings::default().with_value(
            from_param.clone(),
            PropertyValue::I64(from.try_into().expect("test id fits i64")),
        ),
    )
    .await
    .expect("edge write succeeds");

    let expand_id = exec::ExecStepId::new(2).expect("positive step id");
    let expand = executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: from_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![exec::ExecStepId::new(1).expect("positive step id")],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        label: ir::ExpandLabelPlan::Label(name("KNOWS")),
                        output: ir::ExpandOutput::Nodes,
                    },
                },
            ),
            step(
                3,
                vec![expand_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    );
    let result = db
        .execute(
            &expand,
            helix_planner::context::ParamBindings::default()
                .with_value(from_param, PropertyValue::I64(from as i64)),
        )
        .await
        .expect("expand succeeds");
    assert_eq!(
        result.last,
        Some(ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(to)]))
    );
}

#[tokio::test]
async fn drop_node_removes_storage_indexes_and_incident_edges() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-drop-node".to_string(),
    })
    .await
    .expect("db opens");
    let alice = add_user(&db, "alice").await;
    let bob = add_user(&db, "bob").await;
    let edge_id = add_edge(&db, alice, bob, "KNOWS").await;
    let node_param = name("node");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let drop_node = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: node_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::Drop,
                },
            ),
        ],
        2,
    );

    let result = db
        .execute(
            &drop_node,
            helix_planner::context::ParamBindings::default()
                .with_value(node_param.clone(), PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("drop node succeeds");
    assert_eq!(result.last, Some(ExecutionValue::Stream(Vec::new())));

    let users = db
        .execute(
            &access_label_count_plan("User"),
            helix_planner::context::ParamBindings::default(),
        )
        .await
        .expect("label count succeeds");
    assert_eq!(users.last, Some(ExecutionValue::Count(1)));

    let dropped_node = db
        .execute(
            &access_param_value_plan(node_param.clone(), "name"),
            helix_planner::context::ParamBindings::default()
                .with_value(node_param, PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("dropped node read succeeds");
    assert_eq!(dropped_node.last, Some(ExecutionValue::Scalars(Vec::new())));

    let incoming = db
        .execute(
            &expand_node_ids_plan(name("bob"), ir::ExpandDirection::In, "KNOWS"),
            helix_planner::context::ParamBindings::default()
                .with_value(name("bob"), PropertyValue::I64(bob as i64)),
        )
        .await
        .expect("incoming expand succeeds");
    assert_eq!(incoming.last, Some(ExecutionValue::Scalars(Vec::new())));

    let dropped_edge = db
        .execute(
            &access_edge_param_id_plan(name("edge")),
            helix_planner::context::ParamBindings::default()
                .with_value(name("edge"), PropertyValue::I64(edge_id as i64)),
        )
        .await
        .expect("dropped edge read succeeds");
    assert_eq!(dropped_edge.last, Some(ExecutionValue::Scalars(Vec::new())));
}

#[tokio::test]
async fn drop_edge_labeled_preserves_other_labels_between_same_pair() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-drop-edge-labeled".to_string(),
    })
    .await
    .expect("db opens");
    let alice = add_user(&db, "alice").await;
    let bob = add_user(&db, "bob").await;
    let knows_id = add_edge(&db, alice, bob, "KNOWS").await;
    let follows_id = add_edge(&db, alice, bob, "FOLLOWS").await;
    let source_param = name("source");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let drop_knows = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: source_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::DropEdgeLabeled {
                        to: ir::NodeTargetPlan::PointIds {
                            ids: ids(vec![bob]),
                        },
                        label: name("KNOWS"),
                    },
                },
            ),
        ],
        2,
    );

    db.execute(
        &drop_knows,
        helix_planner::context::ParamBindings::default()
            .with_value(source_param.clone(), PropertyValue::I64(alice as i64)),
    )
    .await
    .expect("labeled edge drop succeeds");

    let knows_expand = db
        .execute(
            &expand_node_ids_plan(source_param.clone(), ir::ExpandDirection::Out, "KNOWS"),
            helix_planner::context::ParamBindings::default()
                .with_value(source_param.clone(), PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("knows expand succeeds");
    assert_eq!(knows_expand.last, Some(ExecutionValue::Scalars(Vec::new())));

    let follows_expand = db
        .execute(
            &expand_node_ids_plan(source_param, ir::ExpandDirection::Out, "FOLLOWS"),
            helix_planner::context::ParamBindings::default()
                .with_value(name("source"), PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("follows expand succeeds");
    assert_eq!(
        follows_expand.last,
        Some(ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(bob)]))
    );

    let dropped_edge = db
        .execute(
            &access_edge_param_id_plan(name("knows")),
            helix_planner::context::ParamBindings::default()
                .with_value(name("knows"), PropertyValue::I64(knows_id as i64)),
        )
        .await
        .expect("dropped edge read succeeds");
    assert_eq!(dropped_edge.last, Some(ExecutionValue::Scalars(Vec::new())));

    let retained_edge = db
        .execute(
            &access_edge_param_id_plan(name("follows")),
            helix_planner::context::ParamBindings::default()
                .with_value(name("follows"), PropertyValue::I64(follows_id as i64)),
        )
        .await
        .expect("retained edge read succeeds");
    assert_eq!(
        retained_edge.last,
        Some(ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(
            follows_id
        )]))
    );
}

#[tokio::test]
async fn drop_edge_unlabeled_removes_all_edges_between_pair() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-drop-edge-unlabeled".to_string(),
    })
    .await
    .expect("db opens");
    let alice = add_user(&db, "alice").await;
    let bob = add_user(&db, "bob").await;
    let knows_id = add_edge(&db, alice, bob, "KNOWS").await;
    let follows_id = add_edge(&db, alice, bob, "FOLLOWS").await;
    let source_param = name("source");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let drop_pair = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: source_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::DropEdge {
                        to: ir::NodeTargetPlan::PointIds {
                            ids: ids(vec![bob]),
                        },
                    },
                },
            ),
        ],
        2,
    );

    db.execute(
        &drop_pair,
        helix_planner::context::ParamBindings::default()
            .with_value(source_param.clone(), PropertyValue::I64(alice as i64)),
    )
    .await
    .expect("unlabeled edge drop succeeds");

    for (label, edge_param, edge_id) in [
        ("KNOWS", "knows", knows_id),
        ("FOLLOWS", "follows", follows_id),
    ] {
        let expand = db
            .execute(
                &expand_node_ids_plan(source_param.clone(), ir::ExpandDirection::Out, label),
                helix_planner::context::ParamBindings::default()
                    .with_value(source_param.clone(), PropertyValue::I64(alice as i64)),
            )
            .await
            .expect("expand succeeds");
        assert_eq!(expand.last, Some(ExecutionValue::Scalars(Vec::new())));

        let edge = db
            .execute(
                &access_edge_param_id_plan(name(edge_param)),
                helix_planner::context::ParamBindings::default()
                    .with_value(name(edge_param), PropertyValue::I64(edge_id as i64)),
            )
            .await
            .expect("edge read succeeds");
        assert_eq!(edge.last, Some(ExecutionValue::Scalars(Vec::new())));
    }
}

#[tokio::test]
async fn drop_edge_by_id_source_removes_edge_indexes_and_adjacency() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-drop-edge-by-id".to_string(),
    })
    .await
    .expect("db opens");
    let alice = add_user(&db, "alice").await;
    let bob = add_user(&db, "bob").await;
    let edge_id = add_edge(&db, alice, bob, "KNOWS").await;
    let drop_edge = executable(
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
    );

    db.execute(&drop_edge, helix_planner::context::ParamBindings::default())
        .await
        .expect("edge id drop succeeds");

    let dropped_edge = db
        .execute(
            &access_edge_param_id_plan(name("edge")),
            helix_planner::context::ParamBindings::default()
                .with_value(name("edge"), PropertyValue::I64(edge_id as i64)),
        )
        .await
        .expect("dropped edge read succeeds");
    assert_eq!(dropped_edge.last, Some(ExecutionValue::Scalars(Vec::new())));

    let expand = db
        .execute(
            &expand_node_ids_plan(name("source"), ir::ExpandDirection::Out, "KNOWS"),
            helix_planner::context::ParamBindings::default()
                .with_value(name("source"), PropertyValue::I64(alice as i64)),
        )
        .await
        .expect("expand succeeds");
    assert_eq!(expand.last, Some(ExecutionValue::Scalars(Vec::new())));
}

#[tokio::test]
async fn drop_edge_by_id_from_input_requires_non_empty_input() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "mutation-drop-edge-by-id-from-input".to_string(),
    })
    .await
    .expect("db opens");
    let alice = add_user(&db, "alice").await;
    let bob = add_user(&db, "bob").await;
    let edge_id = add_edge(&db, alice, bob, "KNOWS").await;
    let empty_access_id = exec::ExecStepId::new(1).expect("positive step id");
    let empty_input_drop = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
                },
            ),
            step(
                2,
                vec![empty_access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::DropEdgeByIdFromInput {
                        edges: ir::EdgeTargetPlan::PointIds {
                            ids: ids(vec![edge_id]),
                        },
                    },
                },
            ),
        ],
        2,
    );
    db.execute(
        &empty_input_drop,
        helix_planner::context::ParamBindings::default(),
    )
    .await
    .expect("empty-input edge drop succeeds without deleting");

    let retained = db
        .execute(
            &access_edge_param_id_plan(name("edge")),
            helix_planner::context::ParamBindings::default()
                .with_value(name("edge"), PropertyValue::I64(edge_id as i64)),
        )
        .await
        .expect("retained edge read succeeds");
    assert_eq!(
        retained.last,
        Some(ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(
            edge_id
        )]))
    );

    let source_param = name("source");
    let source_access_id = exec::ExecStepId::new(1).expect("positive step id");
    let non_empty_input_drop = executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: source_param.clone(),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![source_access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::DropEdgeByIdFromInput {
                        edges: ir::EdgeTargetPlan::PointIds {
                            ids: ids(vec![edge_id]),
                        },
                    },
                },
            ),
        ],
        2,
    );
    db.execute(
        &non_empty_input_drop,
        helix_planner::context::ParamBindings::default()
            .with_value(source_param, PropertyValue::I64(alice as i64)),
    )
    .await
    .expect("non-empty-input edge drop succeeds");

    let dropped = db
        .execute(
            &access_edge_param_id_plan(name("edge")),
            helix_planner::context::ParamBindings::default()
                .with_value(name("edge"), PropertyValue::I64(edge_id as i64)),
        )
        .await
        .expect("dropped edge read succeeds");
    assert_eq!(dropped.last, Some(ExecutionValue::Scalars(Vec::new())));
}
