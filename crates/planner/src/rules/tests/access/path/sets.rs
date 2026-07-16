use super::*;

#[test]
fn access_path_rule_costs_sets_and_declines_non_access_inputs() {
    let rule = AccessPathImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let left = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
        ids: element_ids(vec![1, 2]),
    })
    .unwrap();
    let right = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Empty).unwrap();
    let union = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        left.clone(),
        right.clone(),
    )));
    let intersect = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(left, right),
    ));

    let union = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &union,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let intersect = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &intersect,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert!(matches!(
        union.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::SetUnion,
            ..
        }
    ));
    assert_eq!(union.delivered.cardinality.upper(), Some(2));
    assert!(matches!(
        intersect.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::SetIntersection,
            ..
        }
    ));
    assert_eq!(intersect.delivered.cardinality.upper(), Some(0));
    assert_ne!(union.cost, cost::CostVector::ZERO);
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &source(properties::ElementKind::Node),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );
}
