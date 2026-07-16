use crate::planning::tests::support::*;

#[test]
fn source_and_stream_filters_remain_executable_filters() {
    let traversal = g()
        .n_with_label_where("User", Predicate::eq("username", "alice"))
        .has("active", true);
    let context = ctx(builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap()));

    let executable = executable_traversal(traversal, context);

    assert!(executable
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));
    assert!(executable.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Filter { predicate }
                if matches!(predicate.as_ref(), Predicate::Eq { .. })
                    || matches!(predicate.as_ref(), Predicate::And { .. })
        )
    }));
}

#[test]
fn variable_filters_wrap_label_indexed_sources() {
    let executable = executable_traversal(
        g().n_with_label("User").within("allowed_users"),
        ctx(builtin_label_indexes()),
    );

    assert!(executable.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Variable {
                op: ExecVariableOp::Stream(StreamVariableOp::Within(variable)),
            } if variable.as_ref() == "allowed_users"
        )
    }));
}

#[test]
fn variable_ops_lower_store_and_source_inject_shapes() {
    let stored = executable_traversal(
        g().n_with_label("User").store("users"),
        ctx(builtin_label_indexes()),
    );
    let injected = executable_traversal(g().inject("users"), PlannerContext::default());

    assert!(stored.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Variable {
                op: ExecVariableOp::Stream(StreamVariableOp::Store(name)),
            } if name.as_ref() == "users"
        )
    }));
    assert!(injected.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Variable {
                op: ExecVariableOp::SourceInject { variable },
            } if variable.as_ref() == "users"
        )
    }));
}

#[test]
fn branch_sub_traversals_inject_context_inside_nested_operations() {
    let executable = executable_traversal(
        g().n_with_label("User").union(vec![
            sub().out(Some("FOLLOWS")).has("active", true),
            sub().in_(Some("MENTIONS")).limit(2usize),
        ]),
        ctx(builtin_label_indexes()),
    );

    let ExecOp::Branch {
        plan: ExecBranchPlan::Union(branches),
    } = &executable
        .steps()
        .iter()
        .find(|step| matches!(step.op, ExecOp::Branch { .. }))
        .expect("expected branch step")
        .op
    else {
        panic!("expected union branch")
    };
    assert_eq!(branches.len(), 2);

    let left = &branches.as_ref()[0];
    assert!(left.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Variable {
                op: ExecVariableOp::SourceInject { variable },
            } if variable.as_ref() == "$context"
        )
    }));
    assert!(left.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Expand { plan }
                if plan.direction == ExpandDirection::Out
                    && plan.output == ExpandOutput::Nodes
                    && matches!(
                        &plan.label,
                        ExpandLabelPlan::Label(label) if label.as_ref() == "FOLLOWS"
                    )
        )
    }));
    assert!(left
        .steps()
        .iter()
        .any(|step| matches!(&step.op, ExecOp::Filter { .. })));

    let right = &branches.as_ref()[1];
    assert!(right.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Variable {
                op: ExecVariableOp::SourceInject { variable },
            } if variable.as_ref() == "$context"
        )
    }));
    assert!(right.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Expand { plan }
                if plan.direction == ExpandDirection::In
                    && plan.output == ExpandOutput::Nodes
                    && matches!(
                        &plan.label,
                        ExpandLabelPlan::Label(label) if label.as_ref() == "MENTIONS"
                    )
        )
    }));
    assert!(right.steps().iter().any(|step| {
        matches!(
            &step.op,
            ExecOp::Limit {
                count: StreamBoundPlan::Literal(2),
            }
        )
    }));
}

#[test]
fn reserved_operations_are_preserved_in_executable_steps() {
    let executable = executable_traversal(
        g().n_with_label("User").fold(),
        ctx(builtin_label_indexes()),
    );

    assert!(executable.steps().iter().any(|step| matches!(
        &step.op,
        ExecOp::Reserved {
            op: ReservedOp::Fold
        }
    )));
}
