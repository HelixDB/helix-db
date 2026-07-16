use super::*;

#[test]
fn executable_entrypoint_lowers_nested_terminal_chains_without_compatibility_fallback() {
    let executable = executable_ast(
        AstNode::Count {
            input: boxed(AstNode::Count {
                input: boxed(nodes_root()),
            }),
        },
        PlannerContext::default(),
    );

    assert_eq!(executable.steps().len(), 3);
    assert!(matches!(
        &executable.steps()[0].op,
        crate::exec::ExecOp::KvRead(crate::exec::KvReadPlan::RangeScan { keyspace, .. })
            if *keyspace == crate::exec::ElementKeyspace::NodeProperty
    ));
    assert!(matches!(
        &executable.steps()[1].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
    assert!(matches!(
        &executable.steps()[2].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
    assert_eq!(
        executable.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
}

#[test]
fn executable_entrypoint_lowers_nested_aggregate_and_state_write_terminals() {
    let aggregate = executable_ast(
        AstNode::Count {
            input: boxed(AstNode::Group {
                input: boxed(nodes_root()),
                property: "kind".to_owned(),
            }),
        },
        PlannerContext::default(),
    );

    assert_eq!(aggregate.steps().len(), 3);
    assert!(matches!(
        &aggregate.steps()[1].op,
        crate::exec::ExecOp::Aggregate {
            aggregate: AggregatePlan::Group(property),
        } if property.as_ref() == "kind"
    ));
    assert_eq!(
        aggregate.steps()[1].schedule,
        crate::exec::ExecSchedule::Barrier
    );
    assert!(matches!(
        &aggregate.steps()[2].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));

    let state_write = executable_ast(
        AstNode::Count {
            input: boxed(AstNode::Store {
                input: boxed(nodes_root()),
                name: "cached".to_owned(),
            }),
        },
        PlannerContext::default(),
    );

    assert_eq!(state_write.steps().len(), 3);
    assert!(matches!(
        &state_write.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "cached"
    ));
    assert_eq!(
        state_write.steps()[1].schedule,
        crate::exec::ExecSchedule::Barrier
    );
    assert_eq!(
        state_write.steps()[2].delivered.effect,
        crate::properties::EffectKind::Barrier
    );
    assert!(matches!(
        &state_write.steps()[2].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
}

#[test]
fn executable_entrypoint_lowers_pipeline_after_terminal_stream_input() {
    let executable = executable_ast(
        AstNode::Dedup {
            input: boxed(AstNode::Count {
                input: boxed(nodes_root()),
            }),
        },
        PlannerContext::default(),
    );

    assert_eq!(executable.steps().len(), 3);
    assert!(matches!(
        &executable.steps()[1].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
    assert!(matches!(
        &executable.steps()[2].op,
        crate::exec::ExecOp::Distinct
    ));
    assert_eq!(
        executable.steps()[2].dependencies,
        vec![crate::exec::ExecStepId::new(2).unwrap()]
    );
}

#[test]
fn executable_entrypoint_lowers_mutation_stream_shapes_without_compatibility_fallback() {
    let mutation_root = || AstNode::AddN {
        input: None,
        label: "User".to_owned(),
        properties: Vec::new(),
    };

    let projected = executable_ast(
        AstNode::Count {
            input: boxed(mutation_root()),
        },
        PlannerContext::default(),
    );
    assert_eq!(projected.steps().len(), 2);
    assert!(matches!(
        &projected.steps()[0].op,
        crate::exec::ExecOp::Mutation {
            plan: crate::exec::ExecMutationPlan::AddNodeSource { label, .. }
        } if label.as_ref() == "User"
    ));
    assert!(matches!(
        &projected.steps()[1].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
    assert_eq!(
        projected.steps()[1].dependencies,
        vec![projected.steps()[0].id]
    );
    assert_eq!(
        projected.steps()[1].delivered.effect,
        crate::properties::EffectKind::Barrier
    );

    let limited = executable_ast(
        AstNode::Limit {
            input: boxed(mutation_root()),
            count: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    assert_eq!(limited.steps().len(), 2);
    assert!(matches!(
        &limited.steps()[1].op,
        crate::exec::ExecOp::Limit {
            count: StreamBoundPlan::Literal(1),
        }
    ));
    assert_eq!(limited.steps()[1].dependencies, vec![limited.steps()[0].id]);
    assert_eq!(
        limited.steps()[1].delivered.effect,
        crate::properties::EffectKind::Barrier
    );

    let stored = executable_ast(
        AstNode::Store {
            input: boxed(mutation_root()),
            name: "created".to_owned(),
        },
        PlannerContext::default(),
    );
    assert_eq!(stored.steps().len(), 2);
    assert!(matches!(
        &stored.steps()[1].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "created"
    ));
    assert_eq!(stored.steps()[1].dependencies, vec![stored.steps()[0].id]);
    assert_eq!(
        stored.steps()[1].schedule,
        crate::exec::ExecSchedule::Barrier
    );
}

#[test]
fn executable_entrypoint_lowers_input_mutation_stream_shapes_without_compatibility_fallback() {
    let mutation_root = || AstNode::SetProperty {
        input: boxed(nodes_root()),
        name: "active".to_owned(),
        value: PropertyInput::from(true),
    };

    let projected = executable_ast(
        AstNode::Count {
            input: boxed(mutation_root()),
        },
        PlannerContext::default(),
    );
    assert_eq!(projected.steps().len(), 3);
    assert!(matches!(
        &projected.steps()[0].op,
        crate::exec::ExecOp::KvRead(_)
    ));
    assert!(matches!(
        &projected.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: crate::exec::ExecMutationPlan::SetProperty { name, .. }
        } if name.as_ref() == "active"
    ));
    assert_eq!(
        projected.steps()[1].dependencies,
        vec![projected.steps()[0].id]
    );
    assert_eq!(
        projected.steps()[1].schedule,
        crate::exec::ExecSchedule::Barrier
    );
    assert!(matches!(
        &projected.steps()[2].op,
        crate::exec::ExecOp::Project {
            projection: ProjectionPlan::Count,
        }
    ));
    assert_eq!(
        projected.steps()[2].dependencies,
        vec![projected.steps()[1].id]
    );
    assert_eq!(
        projected.steps()[2].delivered.effect,
        crate::properties::EffectKind::Barrier
    );

    let limited = executable_ast(
        AstNode::Limit {
            input: boxed(mutation_root()),
            count: StreamBound::Literal(1),
        },
        PlannerContext::default(),
    );
    assert_eq!(limited.steps().len(), 3);
    assert!(matches!(
        &limited.steps()[2].op,
        crate::exec::ExecOp::Limit {
            count: StreamBoundPlan::Literal(1),
        }
    ));
    assert_eq!(limited.steps()[2].dependencies, vec![limited.steps()[1].id]);
    assert_eq!(
        limited.steps()[2].delivered.effect,
        crate::properties::EffectKind::Barrier
    );

    let stored = executable_ast(
        AstNode::Store {
            input: boxed(mutation_root()),
            name: "updated".to_owned(),
        },
        PlannerContext::default(),
    );
    assert_eq!(stored.steps().len(), 3);
    assert!(matches!(
        &stored.steps()[2].op,
        crate::exec::ExecOp::Variable {
            op: crate::exec::ExecVariableOp::Stream(StreamVariableOp::Store(variable))
        } if variable.as_ref() == "updated"
    ));
    assert_eq!(stored.steps()[2].dependencies, vec![stored.steps()[1].id]);
    assert_eq!(
        stored.steps()[2].schedule,
        crate::exec::ExecSchedule::Barrier
    );
}

#[test]
fn executable_entrypoint_rejects_non_stream_terminal_inputs_without_compatibility_fallback() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: None,
            root: AstNode::Count {
                input: boxed(AstNode::DropIndex {
                    spec: IndexSpec::node_equality("User", "email"),
                }),
            },
            condition: None,
        }))],
        returns: Vec::new(),
    };

    let err = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap_err();

    assert!(matches!(err, PlannerError::UnsupportedCascadesPlan { .. }));
}
