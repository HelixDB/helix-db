use super::super::*;

#[test]
fn input_mutation_variants_can_feed_selected_root_stream_suffixes() {
    let projected_add = executable_ast(
        AstNode::Count {
            input: boxed(AstNode::AddN {
                input: Some(boxed(nodes_root())),
                label: "Audit".to_owned(),
                properties: vec![("kind".to_owned(), PropertyInput::from("login"))],
            }),
        },
        PlannerContext::default(),
    );
    assert_eq!(projected_add.steps().len(), 3);
    assert!(matches!(
        &projected_add.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: ExecMutationPlan::AddNodeFromInput { label, .. }
        } if label.as_ref() == "Audit"
    ));
    assert!(matches!(
        &projected_add.steps()[2].op,
        crate::exec::ExecOp::Count { plan }
            if matches!(plan.as_ref(), ExecCountPlan::InputRows { .. })
    ));
    assert_eq!(
        projected_add.steps()[2].dependencies,
        vec![projected_add.steps()[1].id]
    );

    let limited_edge = executable_ast(
        AstNode::Limit {
            input: boxed(AstNode::AddE {
                input: boxed(nodes_root()),
                label: "FOLLOWS".to_owned(),
                to: NodeRef::ids([7]),
                properties: Vec::new(),
            }),
            count: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    assert_eq!(limited_edge.steps().len(), 3);
    // The source read must stay unbounded; the limit is after the mutation barrier.
    assert!(matches!(
        &limited_edge.steps()[0].op,
        crate::exec::ExecOp::KvRead(KvReadPlan::RangeScan { limit: None, .. })
    ));
    assert!(matches!(
        &limited_edge.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: ExecMutationPlan::AddEdge { label, .. }
        } if label.as_ref() == "FOLLOWS"
    ));
    assert!(matches!(
        &limited_edge.steps()[2].op,
        crate::exec::ExecOp::Limit {
            count: StreamBoundPlan::Literal(1),
        }
    ));
    assert_eq!(
        limited_edge.steps()[2].dependencies,
        vec![limited_edge.steps()[1].id]
    );

    let stored_removal = executable_ast(
        AstNode::Store {
            input: boxed(AstNode::RemoveProperty {
                input: boxed(nodes_root()),
                name: "stale".to_owned(),
            }),
            name: "updated".to_owned(),
        },
        PlannerContext::default(),
    );
    assert_eq!(stored_removal.steps().len(), 3);
    assert!(matches!(
        &stored_removal.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: ExecMutationPlan::RemoveProperty { name }
        } if name.as_ref() == "stale"
    ));
    assert!(matches!(
        &stored_removal.steps()[2].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "updated"
    ));
    assert_eq!(
        stored_removal.steps()[2].schedule,
        crate::exec::ExecSchedule::Barrier
    );
    assert_eq!(
        stored_removal.steps()[2].dependencies,
        vec![stored_removal.steps()[1].id]
    );

    let dedup_drop_edge = executable_ast(
        AstNode::Dedup {
            input: boxed(AstNode::DropEdge {
                input: boxed(nodes_root()),
                to: NodeRef::var("targets"),
            }),
        },
        PlannerContext::default(),
    );
    assert_eq!(dedup_drop_edge.steps().len(), 3);
    assert!(matches!(
        &dedup_drop_edge.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: ExecMutationPlan::DropEdge {
                to: NodeTargetPlan::FromVar { variable }
            }
        } if variable.as_ref() == "targets"
    ));
    assert!(matches!(
        &dedup_drop_edge.steps()[2].op,
        crate::exec::ExecOp::Distinct
    ));
    assert_eq!(
        dedup_drop_edge.steps()[2].dependencies,
        vec![dedup_drop_edge.steps()[1].id]
    );

    let projected_drop_edge_by_id = executable_ast(
        AstNode::Count {
            input: boxed(AstNode::DropEdgeById {
                input: Some(boxed(nodes_root())),
                edges: EdgeRef::param("edge_ids"),
            }),
        },
        PlannerContext::default(),
    );
    assert_eq!(projected_drop_edge_by_id.steps().len(), 3);
    assert!(matches!(
        &projected_drop_edge_by_id.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: ExecMutationPlan::DropEdgeByIdFromInput {
                edges: EdgeTargetPlan::FromParam { param }
            }
        } if param.as_ref() == "edge_ids"
    ));
    assert!(matches!(
        &projected_drop_edge_by_id.steps()[2].op,
        crate::exec::ExecOp::Count { plan }
            if matches!(plan.as_ref(), ExecCountPlan::InputRows { .. })
    ));
    assert_eq!(
        projected_drop_edge_by_id.steps()[2].dependencies,
        vec![projected_drop_edge_by_id.steps()[1].id]
    );
}
