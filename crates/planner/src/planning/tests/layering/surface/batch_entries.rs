use super::*;

#[test]
fn batch_conditions_preserve_typed_variable_conditions() {
    let cases = [
        (
            BatchCondition::VarNotEmpty("users".to_string()),
            BatchVariableConditionPlan::VarNotEmpty(NonEmptyString::new("users").unwrap()),
        ),
        (
            BatchCondition::VarEmpty("users".to_string()),
            BatchVariableConditionPlan::VarEmpty(NonEmptyString::new("users").unwrap()),
        ),
        (
            BatchCondition::VarMinSize("users".to_string(), 2),
            BatchVariableConditionPlan::VarMinSize(
                NonEmptyString::new("users").unwrap(),
                NonZeroUsize::new(2).unwrap(),
            ),
        ),
    ];

    for (condition, expected) in cases {
        let batch = read_batch().var_as_if("users", condition, g().n(NodeRef::all()));
        let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

        assert_eq!(plan.steps().len(), 1);
        assert_eq!(plan.steps()[0].condition, ExecCondition::Variable(expected));
    }
}

#[test]
fn followup_batch_condition_preserves_previous_result_condition() {
    let batch = read_batch()
        .var_as("seed", g().n(NodeRef::all()))
        .var_as_if("users", BatchCondition::PrevNotEmpty, g().n(NodeRef::all()));
    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.steps().len(), 2);
    assert_eq!(
        plan.steps()[1].condition,
        ExecCondition::PreviousStepNotEmpty {
            dependency: plan.steps()[0].id,
        }
    );
}

#[test]
fn followup_batch_condition_preserves_variable_conditions() {
    let batch = read_batch()
        .var_as("seed", g().n(NodeRef::all()))
        .var_as_if(
            "users",
            BatchCondition::VarNotEmpty("seed".to_string()),
            g().n(NodeRef::all()),
        );
    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(
        plan.steps()[1].condition,
        ExecCondition::Variable(BatchVariableConditionPlan::VarNotEmpty(
            NonEmptyString::new("seed").unwrap()
        ))
    );
}

#[test]
fn batch_run_output_distinguishes_bound_and_discarded_results() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: None,
            root: AstNode::Nodes {
                reference: NodeRef::all(),
            },
            condition: None,
        }))],
        returns: Vec::new(),
    };
    let plan = crate::planning::plan_read_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.returns(), &ReturnPlan::None);
    assert_eq!(plan.steps()[0].output, BatchOutputPlan::Discard);
    assert_eq!(plan.steps()[0].condition, ExecCondition::Always);
}

#[test]
fn write_batch_entrypoint_plans_nested_foreach_bodies() {
    let body = write_batch().var_as(
        "created",
        g().add_n(
            "Audit",
            vec![("event_id", PropertyInput::param("event_id"))],
        ),
    );
    let batch = write_batch()
        .for_each_param("events", body)
        .returning(["created"]);
    let plan = crate::planning::plan_write_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    let ExecOp::ForEach { param, body } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };
    assert_eq!(param.as_ref(), "events");
    assert_eq!(
        body.steps()[0].output,
        BatchOutputPlan::Bind(NonEmptyString::new("created").unwrap())
    );
    assert!(matches!(
        &body.steps()[0].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::AddNodeSource { .. },
        }
    ));
}

#[test]
fn executable_entrypoint_uses_cascades_selected_foreach_body() {
    let body = write_batch().var_as(
        "created",
        g().add_n(
            "Audit",
            vec![("event_id", PropertyInput::param("event_id"))],
        ),
    );
    let batch = write_batch()
        .for_each_param("events", body)
        .returning(["created"]);

    let plan = crate::planning::plan_write_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(plan.steps().len(), 1);
    assert!(plan.metrics().memo_groups >= 1);
    assert_eq!(plan.metrics().alternatives_considered, 1);
    assert_eq!(plan.metrics().selected_cost, plan.steps()[0].cost);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::ForEach { param, body }
            if param.as_ref() == "events"
                && body.steps().len() == 1
                && matches!(
                    &body.steps()[0].op,
                    crate::exec::ExecOp::Mutation {
                        plan: crate::exec::ExecMutationPlan::AddNodeSource { label, .. }
                    } if label.as_ref() == "Audit"
        )
    ));
}

#[test]
fn executable_entrypoint_uses_cascades_selected_mutation_input() {
    let batch = write_batch()
        .var_as(
            "updated",
            g().n(NodeRef::all()).set_property("active", true),
        )
        .returning(["updated"]);

    let plan = crate::planning::plan_write_batch(&batch, &PlannerContext::default()).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(plan.steps().len(), 2);
    assert!(plan.metrics().memo_groups >= 2);
    assert!(matches!(
        &plan.steps()[0].op,
        crate::exec::ExecOp::KvRead(_)
    ));
    assert_eq!(plan.steps()[1].dependencies, vec![plan.steps()[0].id]);
    assert!(matches!(
        &plan.steps()[1].op,
        crate::exec::ExecOp::Mutation {
            plan: crate::exec::ExecMutationPlan::SetProperty { name, .. }
        } if name.as_ref() == "active"
    ));
    assert!(matches!(
        &plan.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "updated"
    ));
}
