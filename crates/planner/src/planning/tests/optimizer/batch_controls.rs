use crate::planning::tests::support::*;

#[test]
fn ordinary_request_equality_parameters_are_specialized_before_selection() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("MENTIONS", "event_id").unwrap());
    let mut planner_ctx = ctx(indexes);
    planner_ctx.params = ParamBindings::default()
        .with_query_value(
            NonEmptyString::new("node_event").unwrap(),
            QueryValue::String("evt-node".to_owned()),
        )
        .with_query_value(
            NonEmptyString::new("edge_event").unwrap(),
            QueryValue::String("evt-edge".to_owned()),
        );
    let batch = read_batch()
        .var_as(
            "node",
            g().n_with_label_where("Audit", Predicate::eq_param("event_id", "node_event")),
        )
        .var_as(
            "edge",
            g().e_with_label_where("MENTIONS", Predicate::eq_param("event_id", "edge_event")),
        )
        .returning(["node", "edge"]);

    let plan = crate::planning::plan_read_batch(&batch, &planner_ctx).unwrap();
    assert!(matches!(
        &plan.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap {
                    bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { value, .. },
                }) if value.literal().as_property_value() == &PropertyValue::from("evt-node")
            )
    ));
    assert!(matches!(
        &plan.steps()[1].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Edge(ExecEdgeAccessPlan::Bitmap {
                    bitmap: crate::exec::ExecEdgeBitmapExpr::PointRead { value, .. },
                }) if value.literal().as_property_value() == &PropertyValue::from("evt-edge")
            )
    ));
}

#[test]
fn cascades_batch_conditions_preserve_selected_index_runs_and_dependencies() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let batch = read_batch()
        .var_as(
            "seed",
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-1")),
        )
        .var_as_if(
            "prev",
            BatchCondition::PrevNotEmpty,
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-2")),
        )
        .var_as_if(
            "non_empty",
            BatchCondition::VarNotEmpty("seed".to_string()),
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-3")),
        )
        .var_as_if(
            "empty",
            BatchCondition::VarEmpty("seed".to_string()),
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-4")),
        )
        .var_as_if(
            "min_size",
            BatchCondition::VarMinSize("seed".to_string(), 1),
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-5")),
        )
        .returning(["prev", "non_empty", "empty", "min_size"]);

    let plan = crate::planning::plan_read_batch(&batch, &ctx(indexes)).unwrap();
    let steps = plan.steps();

    assert_eq!(steps.len(), 5);
    assert_eq!(steps[0].dependencies, Vec::new());
    for (index, step) in steps.iter().enumerate().skip(1) {
        assert_eq!(
            step.dependencies,
            vec![crate::exec::ExecStepId::new(index).unwrap()]
        );
    }
    assert!(matches!(&steps[0].condition, ExecCondition::Always));
    assert!(matches!(
        &steps[1].condition,
        ExecCondition::PreviousStepNotEmpty { dependency } if dependency.get() == 1
    ));
    assert!(matches!(
        &steps[2].condition,
        ExecCondition::Variable(BatchVariableConditionPlan::VarNotEmpty(name))
            if name.as_ref() == "seed"
    ));
    assert!(matches!(
        &steps[3].condition,
        ExecCondition::Variable(BatchVariableConditionPlan::VarEmpty(name))
            if name.as_ref() == "seed"
    ));
    assert!(matches!(
        &steps[4].condition,
        ExecCondition::Variable(BatchVariableConditionPlan::VarMinSize(name, size))
            if name.as_ref() == "seed" && size.get() == 1
    ));

    for (step, expected) in steps
        .iter()
        .zip(["seed", "prev", "non_empty", "empty", "min_size"])
    {
        assert!(matches!(
            &step.output,
            BatchOutputPlan::Bind(name) if name.as_ref() == expected
        ));
        assert!(matches!(
            &step.op,
            ExecOp::Access { plan }
                if matches!(
                    plan.as_ref(),
                    ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, .. } })
                        if key.label == "Audit" && key.property == "event_id"
                )
        ));
    }
    [
        ExecOpFamily::Filter,
        ExecOpFamily::Order,
        ExecOpFamily::Limit,
        ExecOpFamily::Skip,
        ExecOpFamily::Range,
    ]
    .into_iter()
    .for_each(|family| assert_no_exec_op_family(&plan, family));
    assert!(
        plan.trace().events.iter().any(|event| matches!(
            &event.reason,
            TraceReason::SelectedOptimizerRule(rule)
                if rule.as_ref() == RuleId::known(KnownRuleId::SeedAccessPath).as_ref()
        )),
        "batch index runs should record selected access-path provenance: {:?}",
        plan.trace().events
    );
}

#[test]
fn cascades_repeated_roots_preserve_aliases_while_reusing_memo_work() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let names = (0..32)
        .map(|index| format!("result_{index}"))
        .collect::<Vec<_>>();
    let batch = names.iter().fold(read_batch(), |batch, name| {
        batch.var_as(
            name,
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-1")),
        )
    });

    let plan = crate::planning::plan_read_batch(&batch, &ctx(indexes)).unwrap();

    assert_eq!(plan.steps().len(), names.len());
    for (index, (step, name)) in plan.steps().iter().zip(names.iter()).enumerate() {
        assert!(matches!(
            &step.output,
            BatchOutputPlan::Bind(output) if output.as_ref() == name
        ));
        assert!(matches!(
            &step.op,
            ExecOp::Access { plan }
                if matches!(
                    plan.as_ref(),
                    ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, .. } })
                        if key.label == "Audit" && key.property == "event_id"
                )
        ));
        if index == 0 {
            assert_eq!(step.dependencies, Vec::new());
        } else {
            assert_eq!(
                step.dependencies,
                vec![crate::exec::ExecStepId::new(index).unwrap()]
            );
        }
    }
    assert!(
        plan.metrics().memo_groups <= 8,
        "repeated root reuse should keep memo groups sublinear: {:?}",
        plan.metrics()
    );
    assert!(
        plan.metrics().memo_exprs <= 16,
        "repeated root reuse should keep memo expressions sublinear: {:?}",
        plan.metrics()
    );
    assert!(
        plan.metrics().alternatives_considered <= 8,
        "repeated root reuse should keep selected alternatives sublinear: {:?}",
        plan.metrics()
    );
    assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
    assert_no_exec_op_family(&plan, ExecOpFamily::Order);
    assert_no_exec_window(&plan);
}

#[test]
fn cascades_foreach_body_preserves_selected_index_runs_and_body_conditions() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let body = write_batch()
        .var_as(
            "existing",
            g().n_with_label_where("Audit", Predicate::eq_param("event_id", "event_id")),
        )
        .var_as_if(
            "created",
            BatchCondition::VarEmpty("existing".to_string()),
            g().add_n(
                "Audit",
                vec![("event_id", PropertyInput::param("event_id"))],
            ),
        );
    let batch = write_batch().for_each_param("events", body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(plan.steps().len(), 1);
    let ExecOp::ForEach { param, body } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };
    assert_eq!(param.as_ref(), "events");
    assert_eq!(body.steps().len(), 2);
    assert_eq!(body.root(), body.steps()[1].id);
    assert_eq!(body.steps()[0].dependencies, Vec::new());
    assert_eq!(body.steps()[1].dependencies, vec![body.steps()[0].id]);
    assert!(matches!(&body.steps()[0].condition, ExecCondition::Always));
    assert!(matches!(
        &body.steps()[1].condition,
        ExecCondition::Variable(BatchVariableConditionPlan::VarEmpty(name))
            if name.as_ref() == "existing"
    ));
    assert!(matches!(
        &body.steps()[0].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "existing"
    ));
    assert!(matches!(
        &body.steps()[1].output,
        BatchOutputPlan::Bind(name) if name.as_ref() == "created"
    ));
    assert!(matches!(
        &body.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::DynamicEquality { key, param, .. })
                    if key.label == "Audit"
                        && key.property == "event_id"
                        && param.as_ref() == "event_id"
            )
    ));
    assert!(matches!(
        &body.steps()[1].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::AddNodeSource { label, .. },
        } if label.as_ref() == "Audit"
    ));
    assert!(
        plan.trace()
            .events
            .iter()
            .any(|event| matches!(&event.reason, TraceReason::SelectedForEachBody)),
        "foreach body should be recorded as selected in trace: {:?}",
        plan.trace().events
    );
    assert!(
        plan.trace().events.iter().any(|event| matches!(
            &event.reason,
            TraceReason::SelectedOptimizerRule(rule)
                if rule.as_ref() == RuleId::known(KnownRuleId::SeedAccessPath).as_ref()
        )),
        "foreach body indexed read should record selected access-path provenance: {:?}",
        plan.trace().events
    );
}

#[test]
fn foreach_membership_uses_a_bounded_runtime_equality_domain() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("MENTIONS", "event_id").unwrap());
    let body = write_batch()
        .var_as(
            "nodes",
            g().n_with_label_where("Audit", Predicate::is_in_param("event_id", "event_ids")),
        )
        .var_as(
            "edges",
            g().e_with_label_where("MENTIONS", Predicate::is_in_param("event_id", "event_ids")),
        );
    let batch = write_batch().for_each_param("events", body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();
    let ExecOp::ForEach { body, .. } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };

    assert!(matches!(
        &body.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::DynamicMembership { key, values, .. })
                    if key.label == "Audit"
                        && key.property == "event_id"
                        && values.param().as_ref() == "event_ids"
                        && values.max_values().get() == 64
            )
    ));
    assert!(matches!(
        &body.steps()[1].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Edge(crate::exec::ExecEdgeAccessPlan::DynamicMembership {
                    key,
                    values,
                    ..
                }) if key.label == "MENTIONS"
                    && key.property == "event_id"
                    && values.param().as_ref() == "event_ids"
                    && values.max_values().get() == 64
            )
    ));
}

#[test]
fn foreach_membership_count_preserves_runtime_domain_dispatch() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let body = write_batch().var_as(
        "matching",
        g().n_with_label_where("Audit", Predicate::is_in_param("event_id", "event_ids"))
            .count(),
    );
    let batch = write_batch().for_each_param("events", body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();
    let ExecOp::ForEach { body, .. } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };

    assert!(matches!(
        &body.steps()[0].op,
        ExecOp::Count { plan }
            if matches!(
                plan.as_ref(),
                crate::exec::ExecCountPlan::NodeDynamicMembership(dynamic)
                    if dynamic.key.property == "event_id"
                        && dynamic.values.param().as_ref() == "event_ids"
            )
    ));
}

#[test]
fn foreach_count_keeps_object_field_equality_explicitly_dynamic() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let body = write_batch().var_as(
        "matching",
        g().n_with_label_where("Audit", Predicate::eq_param("event_id", "event_id"))
            .count(),
    );
    let batch = write_batch().for_each_param("events", body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();
    let ExecOp::ForEach { param, body } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };
    assert_eq!(param.as_ref(), "events");
    assert_eq!(body.steps().len(), 1);
    assert_eq!(body.steps()[0].dependencies, Vec::new());
    assert!(
        matches!(
            &body.steps()[0].op,
            ExecOp::Count { plan }
                if matches!(
                    plan.as_ref(),
                    crate::exec::ExecCountPlan::NodeDynamicEquality(dynamic)
                        if dynamic.key.label == "Audit"
                            && dynamic.key.property == "event_id"
                            && dynamic.param.as_ref() == "event_id"
                )
        ),
        "foreach count must encode dynamic equality directly: {:?}",
        body.steps()[0].op,
    );

    let edge_indexes = builtin_label_indexes()
        .with_edge_eq(ScopedPropertyKey::try_new("MENTIONS", "event_id").unwrap());
    let edge_body = write_batch().var_as(
        "matching",
        g().e_with_label_where("MENTIONS", Predicate::eq_param("event_id", "event_id"))
            .count(),
    );
    let edge_batch = write_batch().for_each_param("events", edge_body);
    let edge_plan = crate::planning::plan_write_batch(&edge_batch, &ctx(edge_indexes)).unwrap();
    let ExecOp::ForEach {
        body: edge_body, ..
    } = &edge_plan.steps()[0].op
    else {
        panic!("expected edge foreach entry: {:?}", edge_plan.steps());
    };
    assert!(matches!(
        &edge_body.steps()[0].op,
        ExecOp::Count { plan }
            if matches!(
                plan.as_ref(),
                crate::exec::ExecCountPlan::EdgeDynamicEquality(dynamic)
                    if dynamic.key.label == "MENTIONS"
                        && dynamic.key.property == "event_id"
                        && dynamic.param.as_ref() == "event_id"
            )
    ));
}

#[test]
fn foreach_count_keeps_residual_filters_after_dynamic_equality() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let predicate = Predicate::and(vec![
        Predicate::eq_param("event_id", "event_id"),
        Predicate::contains("message", "accepted"),
    ]);
    let body = write_batch().var_as(
        "matching",
        g().n_with_label_where("Audit", predicate).count(),
    );
    let batch = write_batch().for_each_param("events", body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();
    let ExecOp::ForEach { body, .. } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };
    assert!(matches!(
        &body.steps()[0].op,
        ExecOp::Count { plan }
            if matches!(
                plan.as_ref(),
                crate::exec::ExecCountPlan::Stream(crate::exec::ExecCountStreamPlan {
                    cursor: crate::exec::ExecCountCursorPlan::Filter { input, .. },
                    ..
                }) if matches!(
                    input.as_ref(),
                    crate::exec::ExecCountCursorPlan::NodeDynamicEquality { key, param, .. }
                        if key.property == "event_id" && param.as_ref() == "event_id"
                )
            )
    ));
}

#[test]
fn foreach_count_without_an_equality_index_uses_an_exact_filter_cursor() {
    let body = write_batch().var_as(
        "matching",
        g().n_with_label_where("Audit", Predicate::eq_param("unindexed", "unindexed"))
            .count(),
    );
    let batch = write_batch().for_each_param("events", body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(builtin_label_indexes())).unwrap();
    let ExecOp::ForEach { body, .. } = &plan.steps()[0].op else {
        panic!("expected foreach entry: {:?}", plan.steps());
    };
    assert!(matches!(
        &body.steps()[0].op,
        ExecOp::Count { plan }
            if matches!(
                plan.as_ref(),
                crate::exec::ExecCountPlan::Stream(crate::exec::ExecCountStreamPlan {
                    cursor: crate::exec::ExecCountCursorPlan::Filter { input, .. },
                    ..
                }) if matches!(
                    input.as_ref(),
                    crate::exec::ExecCountCursorPlan::NodeLabelBitmap(label)
                        if label.as_ref() == "Audit"
                )
            )
    ));
}

#[test]
fn cascades_nested_foreach_bodies_preserve_selected_index_runs() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap());
    let indexed_read =
        || g().n_with_label_where("Audit", Predicate::eq_param("event_id", "event_id"));
    let inner_body = write_batch().var_as("child_existing", indexed_read());
    let outer_body = write_batch()
        .var_as("existing", indexed_read())
        .for_each_param("children", inner_body);
    let batch = write_batch().for_each_param("events", outer_body);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(plan.steps().len(), 1);
    let ExecOp::ForEach {
        param,
        body: outer_body,
    } = &plan.steps()[0].op
    else {
        panic!("expected outer foreach entry: {:?}", plan.steps());
    };
    assert_eq!(param.as_ref(), "events");
    assert_eq!(outer_body.steps().len(), 2);
    assert_eq!(outer_body.root(), outer_body.steps()[1].id);
    assert_eq!(outer_body.steps()[0].dependencies, Vec::new());
    assert_eq!(
        outer_body.steps()[1].dependencies,
        vec![outer_body.steps()[0].id]
    );
    assert!(matches!(
        &outer_body.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::DynamicEquality { key, param, .. })
                    if key.label == "Audit"
                        && key.property == "event_id"
                        && param.as_ref() == "event_id"
            )
    ));
    let ExecOp::ForEach {
        param,
        body: inner_body,
    } = &outer_body.steps()[1].op
    else {
        panic!("expected nested foreach entry: {:?}", outer_body.steps());
    };
    assert_eq!(param.as_ref(), "children");
    assert_eq!(inner_body.steps().len(), 1);
    assert_eq!(inner_body.root(), inner_body.steps()[0].id);
    assert_eq!(inner_body.steps()[0].dependencies, Vec::new());
    assert!(matches!(
        &inner_body.steps()[0].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::DynamicEquality { key, param, .. })
                    if key.label == "Audit"
                        && key.property == "event_id"
                        && param.as_ref() == "event_id"
            )
    ));
    assert_eq!(
        plan.trace()
            .events
            .iter()
            .filter(|event| matches!(&event.reason, TraceReason::SelectedForEachBody))
            .count(),
        2,
        "nested foreach bodies should both be selected in trace: {:?}",
        plan.trace().events
    );
    assert!(
        plan.trace()
            .events
            .iter()
            .filter(|event| matches!(
                &event.reason,
                TraceReason::SelectedOptimizerRule(rule)
                    if rule.as_ref() == RuleId::known(KnownRuleId::SeedAccessPath).as_ref()
            ))
            .count()
            >= 2,
        "nested foreach indexed reads should record selected access-path provenance: {:?}",
        plan.trace().events
    );
}
