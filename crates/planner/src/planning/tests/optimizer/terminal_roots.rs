use crate::planning::tests::support::*;

#[test]
fn cascades_terminal_roots_preserve_index_access_and_terminal_ops() {
    let count = executable_traversal(user_by_username().count(), selective_username_ctx());
    let exists = executable_traversal(user_by_username().exists(), selective_username_ctx());
    let id = executable_traversal(user_by_username().id(), selective_username_ctx());
    let label = executable_traversal(user_by_username().label(), selective_username_ctx());
    let values = executable_traversal(
        user_by_username().values(vec!["username"]),
        selective_username_ctx(),
    );
    let selected_value_map = executable_traversal(
        user_by_username().value_map(Some(vec!["username"])),
        selective_username_ctx(),
    );
    let project = executable_traversal(
        user_by_username().project(vec![Projection::property("username", "username")]),
        selective_username_ctx(),
    );
    let project_bindings = executable_traversal(
        user_by_username().bind("user").project_bindings(vec![
            BindingProjection::binding("user", "username", "bound_username"),
            BindingProjection::current("$id", "current_id"),
        ]),
        selective_username_ctx(),
    );
    let group_count = executable_traversal(
        user_by_username().group_count("username"),
        selective_username_ctx(),
    );
    let aggregate = executable_traversal(
        user_by_username().aggregate_by(AggregateFunction::Count, "username"),
        selective_username_ctx(),
    );

    for plan in [
        &exists,
        &id,
        &label,
        &values,
        &selected_value_map,
        &project,
        &project_bindings,
        &group_count,
        &aggregate,
    ] {
        assert_indexed_user_access(plan);
        assert_no_exec_op_family(plan, ExecOpFamily::Filter);
        assert_no_exec_op_family(plan, ExecOpFamily::Order);
        assert_no_exec_window(plan);
    }
    for plan in [
        &exists,
        &id,
        &label,
        &values,
        &selected_value_map,
        &project,
        &project_bindings,
    ] {
        assert_selected_rule(plan, KnownRuleId::SeedStreamProject);
    }
    assert_selected_rule(&count, KnownRuleId::SeedStreamCardinality);
    for plan in [&group_count, &aggregate] {
        assert_selected_rule(plan, KnownRuleId::SeedStreamAggregate);
    }

    assert!(matches!(
        first_exec_op(&count, |op| matches!(op, ExecOp::Count { .. })),
        ExecOp::Count { .. }
    ));
    assert!(matches!(
        first_exec_op(&exists, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::Exists
        }
    ));
    assert!(matches!(
        first_exec_op(&id, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::Id
        }
    ));
    assert!(matches!(
        first_exec_op(&label, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::Label
        }
    ));
    assert!(matches!(
        first_exec_op(&values, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::Values(names)
        } if names.as_ref().iter().any(|name| name.as_ref() == "username")
    ));
    assert!(matches!(
        first_exec_op(&selected_value_map, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::ValueMap(PropertySelection::Selected(names))
        } if names.as_ref().iter().any(|name| name.as_ref() == "username")
    ));
    assert!(matches!(
        first_exec_op(&project, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::Project(_)
        }
    ));
    assert!(has_exec_op_family(
        &project_bindings,
        ExecOpFamily::Variable
    ));
    assert!(matches!(
        first_exec_op(&project_bindings, |op| matches!(op, ExecOp::Project { .. })),
        ExecOp::Project {
            projection: ProjectionPlan::ProjectBindings {
                projections,
                dedup: ProjectionDedupMode::All,
            }
        } if projections.as_ref().iter().any(|projection| {
            matches!(
                projection,
                BindingProjectionPlan::Property {
                    target: BindingTargetPlan::Binding(name),
                    source,
                    alias,
                } if name.as_ref() == "user"
                    && source.as_ref() == "username"
                    && alias.as_ref() == "bound_username"
            )
        })
    ));
    assert!(matches!(
        first_exec_op(&group_count, |op| matches!(op, ExecOp::Aggregate { .. })),
        ExecOp::Aggregate {
            aggregate: AggregatePlan::GroupCount(property)
        } if property.as_ref() == "username"
    ));
    assert!(matches!(
        first_exec_op(&aggregate, |op| matches!(op, ExecOp::Aggregate { .. })),
        ExecOp::Aggregate {
            aggregate: AggregatePlan::AggregateBy {
                function: AggregateFunction::Count,
                property,
            }
        } if property.as_ref() == "username"
    ));
}

#[test]
fn cascades_reserved_terminal_roots_preserve_index_access_and_reserved_ops() {
    let fold = executable_traversal(user_by_username().fold(), selective_username_ctx());
    let unfold = executable_traversal(user_by_username().fold().unfold(), selective_username_ctx());
    let path = executable_traversal(user_by_username().path(), selective_username_ctx());
    let simple_path =
        executable_traversal(user_by_username().simple_path(), selective_username_ctx());
    let with_sack = executable_traversal(
        user_by_username().with_sack(PropertyValue::from(1)),
        selective_username_ctx(),
    );
    let sack_set = executable_traversal(
        user_by_username().sack_set("score"),
        selective_username_ctx(),
    );
    let sack_add = executable_traversal(
        user_by_username().sack_add("score"),
        selective_username_ctx(),
    );
    let sack_get = executable_traversal(user_by_username().sack_get(), selective_username_ctx());

    for plan in [
        &fold,
        &unfold,
        &path,
        &simple_path,
        &with_sack,
        &sack_set,
        &sack_add,
        &sack_get,
    ] {
        assert_indexed_user_access(plan);
        assert_selected_rule(plan, KnownRuleId::SeedStreamReserved);
        assert_no_exec_op_family(plan, ExecOpFamily::Filter);
        assert_no_exec_op_family(plan, ExecOpFamily::Order);
        assert_no_exec_window(plan);
    }

    assert_first_reserved_op(&fold, |op| matches!(op, ReservedOp::Fold));
    let unfold_ops = unfold
        .steps()
        .iter()
        .filter_map(|step| match &step.op {
            ExecOp::Reserved { op } => Some(op),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        unfold_ops.as_slice(),
        [ReservedOp::Fold, ReservedOp::Unfold]
    ));
    assert_first_reserved_op(&path, |op| matches!(op, ReservedOp::Path));
    assert_first_reserved_op(&simple_path, |op| matches!(op, ReservedOp::SimplePath));
    assert_first_reserved_op(
        &with_sack,
        |op| matches!(op, ReservedOp::WithSack(value) if value == &PropertyValue::from(1)),
    );
    assert_first_reserved_op(
        &sack_set,
        |op| matches!(op, ReservedOp::SackSet(property) if property.as_ref() == "score"),
    );
    assert_first_reserved_op(
        &sack_add,
        |op| matches!(op, ReservedOp::SackAdd(property) if property.as_ref() == "score"),
    );
    assert_first_reserved_op(&sack_get, |op| matches!(op, ReservedOp::SackGet));
}

fn user_by_username() -> Traversal<helix_ast::traversal::OnNodes, ReadOnly> {
    g().n_with_label_where("User", Predicate::eq("username", "alice"))
}

fn selective_username_ctx() -> PlannerContext {
    let key = ScopedPropertyKey::try_new("User", "username").unwrap();
    PlannerContext {
        indexes: builtin_label_indexes().with_node_eq(key.clone()),
        stats: StatsSnapshot::default()
            .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 1_000_000)
            .with_node_eq_cardinality(key, 1),
        ..PlannerContext::default()
    }
}

fn assert_indexed_user_access(plan: &ExecutablePlan) {
    assert_selected_root_family(plan, "terminal");
    assert!(
        matches!(
            unwrapped_first_exec_access(plan),
            ExecAccessPlan::Node(ExecNodeAccessPlan::Bitmap { bitmap: crate::exec::ExecNodeBitmapExpr::PointRead { key, .. } })
                if key.label == "User" && key.property == "username"
        ),
        "expected User.username equality access, got {:?}",
        first_exec_access(plan)
    );
}

fn assert_first_reserved_op(plan: &ExecutablePlan, predicate: impl Fn(&ReservedOp) -> bool) {
    assert!(
        matches!(
            first_exec_op(plan, |op| matches!(op, ExecOp::Reserved { .. })),
            ExecOp::Reserved { op } if predicate(op)
        ),
        "unexpected reserved executable op in plan: {:?}",
        plan.steps()
    );
}
