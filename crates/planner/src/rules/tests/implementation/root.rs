use super::*;

#[test]
fn root_payload_barrier_rules_keep_payload_in_logical_contracts_and_use_tunable_costs() {
    let mutation_rule = RootMutationImplementationRule::default();
    let ddl_rule = RootIndexDdlImplementationRule::default();
    let storage = cost::StorageCostProfile {
        barrier_overhead: cost::LatencyEstimate::micros(17),
        ..cost::StorageCostProfile::default()
    };
    let expected_mutation_cost = storage.barrier();
    let mutation_expr =
        logical::LogicalExpr::RootMutation(logical::RootMutation::new(ir::MutationPlan::AddNode {
            input: ir::MutationInput::Source,
            label: name("User"),
            properties: ir::PropertyAssignments::default(),
        }));
    let ddl_expr =
        logical::LogicalExpr::RootIndexDdl(logical::RootIndexDdl::new(ir::IndexDdlPlan::Drop {
            spec: ir::IndexDdlDropSpec::NodeEquality {
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        }));

    let mutation = physical_alternative(mutation_rule.apply(optimizer::RuleInput {
        expr: &mutation_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let ddl = physical_alternative(ddl_rule.apply(optimizer::RuleInput {
        expr: &ddl_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(mutation.expr, physical::PhysicalExpr::Barrier));
    assert_eq!(mutation.delivered.effect, properties::EffectKind::Barrier);
    assert_eq!(mutation.cost, expected_mutation_cost);
    assert!(matches!(ddl.expr, physical::PhysicalExpr::Barrier));
    assert_eq!(
        ddl.delivered.cardinality,
        properties::CardinalityBounds::exact(1)
    );
    assert_eq!(ddl.cost.latency, cost::LatencyEstimate::micros(17));
    assert_eq!(
        mutation_rule.apply(optimizer::RuleInput {
            expr: &logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
    assert_eq!(
        ddl_rule.apply(optimizer::RuleInput {
            expr: &logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::IndexDdl),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn root_control_flow_rules_keep_payloads_in_logical_contracts_and_use_shared_costs() {
    let branch_rule = RootBranchImplementationRule::default();
    let repeat_rule = RootRepeatImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let branch_expr = optional_branch_expr(node_all_expr(), edge_all_expr());
    let repeat_expr = repeat_root_expr(node_all_expr(), edge_all_expr(), 2);

    let branch = physical_alternative(branch_rule.apply(optimizer::RuleInput {
        expr: &branch_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let repeat = physical_alternative(repeat_rule.apply(optimizer::RuleInput {
        expr: &repeat_expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        branch.expr,
        physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch)
    ));
    assert_eq!(branch.cost, storage.barrier());
    assert_eq!(branch.delivered.effect, properties::EffectKind::Barrier);
    assert!(matches!(
        repeat.expr,
        physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat)
    ));
    assert_eq!(
        repeat.cost,
        storage.stream_operator(storage.default_unknown_scan_rows)
    );
    assert_eq!(repeat.delivered.effect, properties::EffectKind::Barrier);
    assert_eq!(
        branch_rule.apply(optimizer::RuleInput {
            expr: &logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::TraversalControl),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
    assert_eq!(
        repeat_rule.apply(optimizer::RuleInput {
            expr: &logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::TraversalControl),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}

#[test]
fn root_control_flow_empty_rule_collapses_empty_inputs_before_implementation() {
    let rule = RootControlFlowEmptyRule::default();
    let branch_rule = RootBranchImplementationRule::default();
    let repeat_rule = RootRepeatImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let branch = optional_branch_expr(edge_access_expr(ir::EdgeAccessPlan::Empty), node_all_expr());
    let repeat = repeat_root_expr(
        node_access_expr(ir::NodeAccessPlan::Empty),
        edge_all_expr(),
        2,
    );
    let non_empty = optional_branch_expr(node_all_expr(), node_all_expr());

    let branch_access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &branch,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let repeat_access = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &repeat,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "root_control_flow_empty");
    assert!(matches!(
        branch_access,
        logical::AccessPath::Edge(path) if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
    assert!(matches!(
        repeat_access,
        logical::AccessPath::Node(path) if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));
    for expr in [
        non_empty,
        logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::TraversalControl),
    ] {
        assert_eq!(
            rule.apply(optimizer::RuleInput {
                expr: &expr,
                storage: &storage,
                indexes: empty_indexes(),
                planner_limits: default_planner_limits(),
                stats: default_stats(),
            }),
            optimizer::RuleResult::NotApplicable
        );
    }
    assert_eq!(
        branch_rule.apply(optimizer::RuleInput {
            expr: &branch,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
    assert_eq!(
        repeat_rule.apply(optimizer::RuleInput {
            expr: &repeat,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
