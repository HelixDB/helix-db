use super::*;

#[test]
fn access_distinct_rule_elides_unique_point_ids_and_singleton_access() {
    let rule = AccessDistinctRule::default();
    let storage = cost::StorageCostProfile::default();
    let node_points = node_access_distinct_expr(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![10, 20, 30]),
    });
    let edge_search = edge_access_distinct_expr(edge_text_search(search_limit(1)));
    let unique_equality = node_access_distinct_expr(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new("user_email")
            .unwrap()
            .with_uniqueness(catalog::IndexUniqueness::Unique),
        key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
        value: equality_literal(1),
    });

    let node_points = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &node_points,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let edge_search = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &edge_search,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let unique_equality = logical_access_path(rule.apply(optimizer::RuleInput {
        expr: &unique_equality,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "access_distinct");
    assert!(matches!(
        node_points,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::PointIds { .. })
    ));
    assert!(matches!(
        edge_search,
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::TextSearch { .. })
    ));
    assert!(matches!(
        unique_equality,
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::EqualityIndex { .. })
    ));
}

#[test]
fn access_distinct_rule_declines_unknown_runtime_and_potentially_duplicating_access() {
    let rule = AccessDistinctRule::default();
    let storage = cost::StorageCostProfile::default();
    let label = node_access_distinct_expr(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    });
    let runtime = edge_access_distinct_expr(ir::EdgeAccessPlan::FromParam {
        param: name("edge_ids"),
    });
    let union =
        node_access_distinct_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(1)),
            node_eq_source("User", "age", equality_literal(2)),
        )));

    for expr in [source(properties::ElementKind::Node), label, runtime, union] {
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
fn access_distinct_implementation_rule_keeps_dedup_in_cascades() {
    let rule = AccessDistinctImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(7),
        range_next: cost::LatencyEstimate::micros(11),
        sort_setup: cost::LatencyEstimate::micros(13),
        sort_per_row: cost::LatencyEstimate::micros(17),
        default_unknown_scan_rows: cost::EstimatedRows::rows(9),
        ..cost::StorageCostProfile::default()
    };
    let stats =
        crate::context::StatsSnapshot::default().with_node_label_cardinality(name("User"), 6);
    let expr = node_access_distinct_expr(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    });

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: &stats,
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_distinct");
    let physical::PhysicalExpr::Pipeline(pipeline) = &alternative.expr else {
        panic!("expected physical access-distinct pipeline");
    };
    assert!(matches!(
        pipeline.ops(),
        [
            physical::PhysicalPipelineOp::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::LabelScan,
            },
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
        ]
    ));
    assert_eq!(
        alternative.delivered.materialization,
        properties::Materialization::Materialized
    );
    let rows = cost::EstimatedRows::rows(6);
    assert_eq!(
        alternative.cost,
        storage.range_scan(rows).serial(storage.explicit_sort(rows))
    );

    let already_unique = node_access_distinct_expr(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![10, 20, 30]),
    });
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &already_unique,
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: &stats,
        }),
        optimizer::RuleResult::NotApplicable
    );
}
