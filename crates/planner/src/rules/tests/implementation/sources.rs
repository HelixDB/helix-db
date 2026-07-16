use super::*;

#[test]
fn source_access_rule_applies_only_to_sources_and_delivers_element_properties() {
    let rule = SourceAccessImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &source(properties::ElementKind::Node),
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_source_access");
    assert_eq!(
        alternative.delivered.element,
        Some(properties::ElementKind::Node)
    );
    assert!(matches!(
        alternative.expr,
        physical::PhysicalExpr::Access {
            element: properties::ElementKind::Node,
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. })
        }
    ));
    assert_eq!(
        rule.apply(optimizer::RuleInput {
            expr: &logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
            storage: &storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
        optimizer::RuleResult::NotApplicable
    );

    let edge = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &source(properties::ElementKind::Edge),
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    assert!(matches!(
        edge.expr,
        physical::PhysicalExpr::Access {
            element: properties::ElementKind::Edge,
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan {
                keyspace,
                ..
            })
        } if keyspace == exec::ElementKeyspace::EdgeEndpoints
    ));
}

#[test]
fn variable_source_rule_keeps_source_injection_payload_in_logical_contract() {
    let rule = VariableSourceImplementationRule::default();
    let storage = cost::StorageCostProfile::default();
    let expr = logical::LogicalExpr::VariableSource(logical::VariableSource::new(
        ir::NonEmptyString::new("users").unwrap(),
    ));

    let alternative = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &expr,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_variable_source");
    assert!(matches!(
        alternative.expr,
        physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable)
    ));
    assert_eq!(
        alternative.delivered,
        properties::DeliveredProperties::default()
    );
    assert_eq!(alternative.cost, storage.source_inject());
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
