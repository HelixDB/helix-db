use crate::planning::tests::support::*;

#[test]
fn cascades_mutation_roots_preserve_index_inputs_dependencies_and_payloads() {
    let indexes = builtin_label_indexes()
        .with_node_eq(ScopedPropertyKey::try_new("Audit", "event_id").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("MENTIONS", "event_id").unwrap());
    let batch = write_batch()
        .var_as(
            "created",
            g().add_n(
                "Audit",
                vec![("event_id", PropertyInput::from("evt-created"))],
            ),
        )
        .var_as(
            "updated",
            g().n_with_label_where("Audit", Predicate::eq("event_id", "evt-1"))
                .set_property("status", "updated"),
        )
        .var_as(
            "edge_updated",
            g().e_with_label_where("MENTIONS", Predicate::eq("event_id", "evt-1"))
                .set_property("seen", true),
        )
        .var_as(
            "linked",
            g().n_with_label_where("User", Predicate::eq("username", "alice"))
                .add_e(
                    "MENTIONS",
                    NodeRef::param("targets"),
                    vec![("event_id", PropertyInput::param("event_id"))],
                ),
        )
        .returning(["created", "updated", "edge_updated", "linked"]);

    let plan = crate::planning::plan_write_batch(&batch, &ctx(indexes)).unwrap();
    let steps = plan.steps();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(steps.len(), 7);
    assert_eq!(steps[0].dependencies, Vec::new());
    (1..steps.len()).for_each(|index| {
        assert_eq!(
            steps[index].dependencies,
            vec![crate::exec::ExecStepId::new(index).unwrap()]
        );
    });
    assert!(matches!(
        &steps[0].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::AddNodeSource { label, properties },
        } if label.as_ref() == "Audit"
            && properties
                .as_ref()
                .iter()
                .any(|(name, _value)| name.as_ref() == "event_id")
    ));
    assert!(matches!(
        &steps[1].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, value, .. } })
                    if key.label == "Audit"
                        && key.property == "event_id"
                        && value.literal().as_property_value().as_str() == Some("evt-1")
            )
    ));
    assert!(matches!(
        &steps[2].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::SetProperty { name, value },
        } if name.as_ref() == "status"
            && value == &PropertyInputPlan::new(PropertyInput::from("updated")).unwrap()
    ));
    assert!(matches!(
        &steps[3].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Edge(ExecEdgeAccessPlan::Bitmap { bitmap: crate::exec::ExecEdgeBitmapExpr::PointRead { key, value, .. } })
                    if key.label == "MENTIONS"
                        && key.property == "event_id"
                        && value.literal().as_property_value().as_str() == Some("evt-1")
            )
    ));
    assert!(matches!(
        &steps[4].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::SetProperty { name, value },
        } if name.as_ref() == "seen"
            && value == &PropertyInputPlan::new(PropertyInput::from(true)).unwrap()
    ));
    assert!(matches!(
        &steps[5].op,
        ExecOp::Access { plan }
            if matches!(
                plan.as_ref(),
                ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, value, .. } })
                    if key.label == "User"
                        && key.property == "username"
                        && value.literal().as_property_value().as_str() == Some("alice")
            )
    ));
    assert!(matches!(
        &steps[6].op,
        ExecOp::Mutation {
            plan: ExecMutationPlan::AddEdge {
                label,
                to: NodeTargetPlan::FromParam { param },
                properties,
            },
        } if label.as_ref() == "MENTIONS"
            && param.as_ref() == "targets"
            && properties
                .as_ref()
                .iter()
                .any(|(name, _value)| name.as_ref() == "event_id")
    ));
    for (step, expected) in [
        (&steps[0], "created"),
        (&steps[2], "updated"),
        (&steps[4], "edge_updated"),
        (&steps[6], "linked"),
    ] {
        assert!(matches!(
            &step.output,
            BatchOutputPlan::Bind(name) if name.as_ref() == expected
        ));
    }
    for step in [&steps[1], &steps[3], &steps[5]] {
        assert_eq!(step.output, BatchOutputPlan::Discard);
    }
    assert_selected_root_family(&plan, "mutation");
    assert_selected_rule(&plan, KnownRuleId::SeedRootMutation);
    assert_selected_rule(&plan, KnownRuleId::SeedAccessPath);
    assert_no_exec_op_family(&plan, ExecOpFamily::Filter);
    assert_no_exec_op_family(&plan, ExecOpFamily::Order);
    assert_no_exec_window(&plan);
}

#[test]
fn cascades_index_ddl_roots_preserve_secondary_and_search_specs() {
    let batch = write_batch()
        .var_as(
            "create_unique",
            g().create_index_if_not_exists(IndexSpec::node_unique_equality("Audit", "event_id")),
        )
        .var_as(
            "create_edge_range",
            g().create_index_if_not_exists(IndexSpec::edge_range_desc("MENTIONS", "created_at")),
        )
        .var_as(
            "create_vector",
            g().create_index_if_not_exists(IndexSpec::node_vector(
                "Doc",
                "embedding",
                NonZeroUsize::new(3).unwrap(),
                helix_ast::index::VectorDistanceMetric::Cosine,
                Some("tenant_id"),
            )),
        )
        .var_as(
            "create_text",
            g().create_index_if_not_exists(IndexSpec::node_text("Doc", "body", Some("tenant_id"))),
        )
        .var_as(
            "drop_vector",
            g().drop_index(IndexSpec::edge_vector(
                "MENTIONS",
                "embedding",
                NonZeroUsize::new(4).unwrap(),
                helix_ast::index::VectorDistanceMetric::Euclidean,
                Some("tenant_id"),
            )),
        )
        .var_as(
            "drop_text",
            g().drop_index(IndexSpec::edge_text("MENTIONS", "body", Some("tenant_id"))),
        )
        .returning([
            "create_unique",
            "create_edge_range",
            "create_vector",
            "create_text",
            "drop_vector",
            "drop_text",
        ]);

    let plan = crate::planning::plan_write_batch(&batch, &PlannerContext::default()).unwrap();
    let steps = plan.steps();

    assert_eq!(plan.kind(), PlanKind::Write);
    assert_eq!(steps.len(), 6);
    assert_eq!(steps[0].dependencies, Vec::new());
    (1..steps.len()).for_each(|index| {
        assert_eq!(
            steps[index].dependencies,
            vec![crate::exec::ExecStepId::new(index).unwrap()]
        );
    });
    assert!(matches!(
        &steps[0].op,
        ExecOp::IndexDdl {
            plan: IndexDdlPlan::Create {
                spec: IndexDdlCreateSpec::NodeEquality { key, uniqueness },
                mode: IndexCreateMode::IfNotExists,
            },
        } if key.label == "Audit"
            && key.property == "event_id"
            && *uniqueness == IndexUniqueness::Unique
    ));
    assert!(matches!(
        &steps[1].op,
        ExecOp::IndexDdl {
            plan: IndexDdlPlan::Create {
                spec: IndexDdlCreateSpec::EdgeRange { key },
                mode: IndexCreateMode::IfNotExists,
            },
        } if key.label == "MENTIONS"
            && key.property == "created_at"
            && key.direction == RangeIndexDirection::Desc
    ));
    assert!(matches!(
        &steps[2].op,
        ExecOp::IndexDdl {
            plan: IndexDdlPlan::Create {
                spec:
                    IndexDdlCreateSpec::NodeVector {
                        key,
                        dimension,
                        metric,
                        scope,
                    },
                mode: IndexCreateMode::IfNotExists,
            },
        } if key.label == "Doc"
            && key.property == "embedding"
            && dimension.get() == 3
            && *metric == crate::ir::VectorIndexMetric::Cosine
            && matches!(
                scope,
                SearchIndexScope::Tenant { property } if property.as_ref() == "tenant_id"
            )
    ));
    assert!(matches!(
        &steps[3].op,
        ExecOp::IndexDdl {
            plan: IndexDdlPlan::Create {
                spec: IndexDdlCreateSpec::NodeText { key, scope },
                mode: IndexCreateMode::IfNotExists,
            },
        } if key.label == "Doc"
            && key.property == "body"
            && matches!(
                scope,
                SearchIndexScope::Tenant { property } if property.as_ref() == "tenant_id"
            )
    ));
    assert!(matches!(
        &steps[4].op,
        ExecOp::IndexDdl {
            plan: IndexDdlPlan::Drop {
                spec: IndexDdlDropSpec::EdgeVector { key },
            },
        } if key.label == "MENTIONS" && key.property == "embedding"
    ));
    assert!(matches!(
        &steps[5].op,
        ExecOp::IndexDdl {
            plan: IndexDdlPlan::Drop {
                spec: IndexDdlDropSpec::EdgeText { key },
            },
        } if key.label == "MENTIONS" && key.property == "body"
    ));
    for (step, expected) in steps.iter().zip([
        "create_unique",
        "create_edge_range",
        "create_vector",
        "create_text",
        "drop_vector",
        "drop_text",
    ]) {
        assert!(matches!(
            &step.output,
            BatchOutputPlan::Bind(name) if name.as_ref() == expected
        ));
    }
    assert_selected_root_family(&plan, "index_ddl");
    assert_selected_rule(&plan, KnownRuleId::SeedRootIndexDdl);
}
