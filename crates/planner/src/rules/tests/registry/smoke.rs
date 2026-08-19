use super::*;

#[test]
fn seed_rule_set_runs_through_cascades_optimizer() {
    let rules = SeedRuleSet::default();
    assert_eq!(rules.registry().rule_count(), KnownRuleId::ALL.len());
    let optimizer = rules.optimizer();
    let config = optimizer::OptimizerConfig {
        params: Default::default(),
        late_bound_params: Default::default(),
        limits: crate::context::OptimizerLimits::default(),
        planner_limits: crate::context::PlannerLimits::default(),
        stats: crate::context::StatsSnapshot::default(),
        storage: cost::StorageCostProfile::default(),
        indexes: catalog::IndexCatalogSnapshot::default(),
    };

    let result = optimize(&optimizer, source(properties::ElementKind::Edge), &config);

    assert!(result.memo().group_count() >= 1);
    assert_eq!(result.metrics().alternatives_considered, 1);
    assert!(result.best_alternative(result.root()).is_ok());
}
