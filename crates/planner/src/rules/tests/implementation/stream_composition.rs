use super::*;

#[test]
fn stream_composition_rule_canonicalizes_static_windows_inside_pipelines() {
    let rule = StreamCompositionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = pipeline_expr(vec![
        logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        limit(10),
        skip(3),
        limit(2),
        logical::PureLogicalOp::Project,
    ]);

    let pipeline = logical_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "stream_window_composition");
    assert!(matches!(
        pipeline.ops(),
        [
            logical::PureLogicalOp::Source {
                element: properties::ElementKind::Node
            },
            logical::PureLogicalOp::Range {
                range: ir::StreamRangePlan::Literal(range)
            },
            logical::PureLogicalOp::Project,
        ] if range.start() == 3 && range.end() == 5
    ));
}

#[test]
fn stream_composition_rule_clamps_skips_ranges_and_empty_windows() {
    let rule = StreamCompositionRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = pipeline_expr(vec![range(2, 8), skip(99)]);

    let pipeline = logical_pipeline(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        pipeline.ops(),
        [logical::PureLogicalOp::Range {
            range: ir::StreamRangePlan::Literal(range)
        }] if range.start() == 8 && range.end() == 8
    ));
}

#[test]
fn stream_composition_rule_declines_dynamic_overflow_noop_and_non_pipeline_inputs() {
    let rule = StreamCompositionRule::default();
    let storage = cost::StorageCostProfile::default();
    let dynamic = pipeline_expr(vec![
        limit(10),
        logical::PureLogicalOp::Skip {
            count: ir::StreamBoundPlan::Expr(
                ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("skip")).unwrap(),
            ),
        },
        limit(2),
    ]);
    let overflow = pipeline_expr(vec![skip(usize::MAX), skip(1)]);
    let noop = pipeline_expr(vec![skip(0), skip(0)]);

    for expr in [
        dynamic,
        overflow,
        noop,
        source(properties::ElementKind::Node),
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
