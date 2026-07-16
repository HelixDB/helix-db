use super::*;

#[test]
fn access_order_rule_elides_matching_range_index_order_and_singletons() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let range = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        order_keys(),
    );
    let singleton = edge_access_order_expr(
        ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![42]),
        },
        desc_order_keys(),
    );

    let range = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &range,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let singleton = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &singleton,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_order");
    assert!(matches!(
        range,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::RangeIndex { .. })
    ));
    assert!(matches!(
        singleton,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::PointIds { .. })
    ));
}

#[test]
fn access_order_rule_declines_mismatch_multikey_unknown_and_non_candidates() {
    let rule = AccessOrderRule::default();
    let storage = cost::StorageCostProfile::default();
    let mismatched_direction = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        desc_order_keys(),
    );
    let multikey = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        multi_order_keys(),
    );
    let unknown_bound = node_access_order_expr(
        ir::NodeAccessPlan::LabelScan {
            label: name("User"),
        },
        order_keys(),
    );

    for expr in [
        source(properties::ElementKind::Node),
        mismatched_direction,
        multikey,
        unknown_bound,
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
}

#[test]
fn access_order_implementation_rule_keeps_explicit_sort_in_cascades() {
    let rule = AccessOrderImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(9),
        ..cost::StorageCostProfile::default()
    };
    let expr = node_access_order_expr(ir::NodeAccessPlan::AllScan, order_keys());

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_order");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical access-order pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            },
            physical::PhysicalPipelineOp::Sort,
        ]
    ));
    assert!(matches!(
        alternative.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = storage.default_unknown_scan_rows;
    assert_eq!(
        alternative.cost,
        storage.range_scan(rows).serial(storage.explicit_sort(rows))
    );

    let already_satisfied = node_access_order_expr(
        ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![7]),
        },
        order_keys(),
    );
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &already_satisfied,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
