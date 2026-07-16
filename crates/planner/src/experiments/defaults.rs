//! Default planner scalability fixture matrix.

use crate::exec;

use super::{fixtures, metrics};

/// Default planning scalability fixtures shared by tests and benchmarks.
pub fn default_planning_scalability_fixtures() -> Vec<fixtures::PlanScalabilityFixture> {
    [
        (fixtures::PlanningScalabilityShape::WideBooleanPredicates, 8),
        (
            fixtures::PlanningScalabilityShape::WideBooleanPredicates,
            32,
        ),
        (
            fixtures::PlanningScalabilityShape::WideBooleanPredicates,
            64,
        ),
        (fixtures::PlanningScalabilityShape::ManyAvailableIndexes, 64),
        (
            fixtures::PlanningScalabilityShape::ManyAvailableIndexes,
            256,
        ),
        (
            fixtures::PlanningScalabilityShape::ManyAvailableIndexes,
            1024,
        ),
        (fixtures::PlanningScalabilityShape::BatchedRootReuse, 8),
        (fixtures::PlanningScalabilityShape::BatchedRootReuse, 64),
        (fixtures::PlanningScalabilityShape::BatchedRootReuse, 256),
        (fixtures::PlanningScalabilityShape::ForEachBodyRootReuse, 8),
        (fixtures::PlanningScalabilityShape::ForEachBodyRootReuse, 64),
        (
            fixtures::PlanningScalabilityShape::ForEachBodyRootReuse,
            256,
        ),
        (fixtures::PlanningScalabilityShape::DeepTraversalChain, 4),
        (fixtures::PlanningScalabilityShape::DeepTraversalChain, 16),
        (fixtures::PlanningScalabilityShape::DeepTraversalChain, 32),
        (fixtures::PlanningScalabilityShape::ManyMemoAlternatives, 8),
        (fixtures::PlanningScalabilityShape::ManyMemoAlternatives, 32),
        (fixtures::PlanningScalabilityShape::ManyMemoAlternatives, 64),
        (
            fixtures::PlanningScalabilityShape::OverLimitIndexDisjunction,
            128,
        ),
        (
            fixtures::PlanningScalabilityShape::OverLimitIndexDisjunction,
            512,
        ),
        (fixtures::PlanningScalabilityShape::BranchHeavyQueries, 4),
        (fixtures::PlanningScalabilityShape::BranchHeavyQueries, 8),
        (fixtures::PlanningScalabilityShape::BranchHeavyQueries, 16),
        (
            fixtures::PlanningScalabilityShape::OrderedRangeWindowPushdown,
            8,
        ),
        (
            fixtures::PlanningScalabilityShape::OrderedRangeWindowPushdown,
            32,
        ),
        (
            fixtures::PlanningScalabilityShape::OrderedRangeWindowPushdown,
            64,
        ),
        (fixtures::PlanningScalabilityShape::MutationHeavyBatches, 8),
        (fixtures::PlanningScalabilityShape::MutationHeavyBatches, 32),
        (fixtures::PlanningScalabilityShape::MutationHeavyBatches, 64),
        (
            fixtures::PlanningScalabilityShape::SearchIndexDdlWorkloads,
            8,
        ),
        (
            fixtures::PlanningScalabilityShape::SearchIndexDdlWorkloads,
            32,
        ),
        (
            fixtures::PlanningScalabilityShape::SearchIndexDdlWorkloads,
            64,
        ),
        (
            fixtures::PlanningScalabilityShape::RuntimeDerivedMixedQueries,
            4,
        ),
        (
            fixtures::PlanningScalabilityShape::RuntimeDerivedMixedQueries,
            8,
        ),
        (
            fixtures::PlanningScalabilityShape::RuntimeDerivedMixedQueries,
            16,
        ),
    ]
    .into_iter()
    .map(|(shape, scale)| {
        fixtures::PlanScalabilityFixture::new(shape, scale)
            .expect("default fixture scale is positive")
    })
    .collect()
}

/// Plan every default fixture and enforce deterministic metric thresholds.
pub fn check_default_planning_scalability_fixtures(
) -> Result<Vec<exec::ExecutablePlan>, metrics::PlanningRegressionError> {
    default_planning_scalability_fixtures()
        .into_iter()
        .map(|fixture| fixture.case().plan_checked())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{
        ExecAccessPlan, ExecEdgeAccessPlan, ExecMutationPlan, ExecNodeAccessPlan, ExecOp,
        ExecVariableOp,
    };
    use crate::ir::{IndexDdlCreateSpec, IndexDdlDropSpec, IndexDdlPlan, OrderPlan, PlanKind};
    use crate::ir::{StreamRangePlan, StreamVariableOp, VectorIndexMetric};
    use std::collections::BTreeSet;

    #[test]
    fn default_scalability_fixtures_have_metric_thresholds() {
        let fixtures = default_planning_scalability_fixtures();
        assert_eq!(fixtures.len(), 35);
        assert!(fixtures
            .iter()
            .all(|fixture| fixture.case().thresholds().max_memo_groups().get() > 0));
        assert!(
            fixtures
                .iter()
                .any(|fixture| fixture.shape()
                    == fixtures::PlanningScalabilityShape::BatchedRootReuse)
        );
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.shape()
                == fixtures::PlanningScalabilityShape::ForEachBodyRootReuse));
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.shape()
                == fixtures::PlanningScalabilityShape::ManyMemoAlternatives));
        assert!(fixtures.iter().any(
            |fixture| fixture.shape() == fixtures::PlanningScalabilityShape::BranchHeavyQueries
        ));
        assert!(fixtures.iter().any(|fixture| {
            fixture.shape() == fixtures::PlanningScalabilityShape::OverLimitIndexDisjunction
        }));
        assert!(fixtures.iter().any(|fixture| {
            fixture.shape() == fixtures::PlanningScalabilityShape::OrderedRangeWindowPushdown
        }));
        assert!(fixtures
            .iter()
            .any(|fixture| fixture.shape()
                == fixtures::PlanningScalabilityShape::MutationHeavyBatches));
        assert!(fixtures.iter().any(|fixture| {
            fixture.shape() == fixtures::PlanningScalabilityShape::SearchIndexDdlWorkloads
        }));
        assert!(fixtures.iter().any(|fixture| {
            fixture.shape() == fixtures::PlanningScalabilityShape::RuntimeDerivedMixedQueries
        }));
        let many_index_thresholds = fixtures
            .iter()
            .filter(|fixture| {
                fixture.shape() == fixtures::PlanningScalabilityShape::ManyAvailableIndexes
            })
            .map(|fixture| fixture.case().thresholds().max_memo_groups().get())
            .collect::<BTreeSet<_>>();
        assert_eq!(many_index_thresholds.len(), 1);
    }

    #[test]
    fn default_scalability_fixtures_stay_within_metric_thresholds() {
        let plans = check_default_planning_scalability_fixtures().unwrap();
        assert_eq!(plans.len(), default_planning_scalability_fixtures().len());
        assert!(plans.iter().all(|plan| !plan.metrics().guardrail_hit));
    }

    #[test]
    fn over_limit_index_disjunction_keeps_memo_work_constant_after_limit() {
        let small = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::OverLimitIndexDisjunction,
            128,
        )
        .unwrap()
        .case()
        .plan_checked()
        .unwrap();
        let large = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::OverLimitIndexDisjunction,
            512,
        )
        .unwrap()
        .case()
        .plan_checked()
        .unwrap();

        assert!(large.metrics().memo_groups <= small.metrics().memo_groups);
        assert!(large.metrics().memo_exprs <= small.metrics().memo_exprs);
        assert!(large.metrics().alternatives_considered <= small.metrics().alternatives_considered);
    }

    #[test]
    fn ordered_range_window_pushdown_uses_range_index_with_tight_read_caps() {
        let scale = 8;
        let plan = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::OrderedRangeWindowPushdown,
            scale,
        )
        .unwrap()
        .case()
        .plan_checked()
        .unwrap();

        let range_accesses = plan
            .steps()
            .iter()
            .filter_map(|step| match &step.op {
                ExecOp::Access { plan } => Some(plan.as_ref()),
                _ => None,
            })
            .map(|access| match access {
                ExecAccessPlan::Limited(limited) => {
                    assert_eq!(limited.limit().get(), 7);
                    limited.source()
                }
                access => panic!("expected pushed range read cap, got {access:?}"),
            })
            .filter(|access| {
                matches!(
                    access,
                    ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, .. })
                        if key.property.as_ref() == "age"
                )
            })
            .count();
        assert_eq!(range_accesses, scale);

        let semantic_ranges = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Range {
                        range: StreamRangePlan::Literal(range)
                    } if range.start() == 2 && range.end() == 7
                )
            })
            .count();
        assert_eq!(semantic_ranges, scale);

        assert!(!plan.steps().iter().any(|step| matches!(
            &step.op,
            ExecOp::Order {
                plan: OrderPlan::ExplicitSort(_)
            }
        )));
    }

    #[test]
    fn mutation_heavy_scalability_fixture_uses_indexed_mutation_inputs() {
        let scale = 8;
        let plan = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::MutationHeavyBatches,
            scale,
        )
        .unwrap()
        .case()
        .plan_checked()
        .unwrap();

        assert_eq!(plan.kind(), PlanKind::Write);
        assert_eq!(plan.steps().len(), scale * 7);

        let node_index_reads = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Access { plan }
                        if matches!(
                            plan.as_ref(),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
                                if (key.label == "Audit" && key.property == "event_id")
                                    || (key.label == "User" && key.property == "username")
                        )
                )
            })
            .count();
        let edge_index_reads = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Access { plan }
                        if matches!(
                            plan.as_ref(),
                            ExecAccessPlan::Edge(ExecEdgeAccessPlan::EqualityIndex { key, .. })
                                if key.label == "MENTIONS" && key.property == "event_id"
                        )
                )
            })
            .count();
        let input_mutations = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Mutation {
                        plan: ExecMutationPlan::SetProperty { .. }
                            | ExecMutationPlan::AddEdge { .. }
                    }
                )
            })
            .count();

        assert_eq!(node_index_reads, scale * 2);
        assert_eq!(edge_index_reads, scale);
        assert_eq!(input_mutations, scale * 3);
        assert!(!plan.metrics().guardrail_hit);
    }

    #[test]
    fn search_index_ddl_scalability_fixture_covers_secondary_and_search_specs() {
        let scale = 8;
        let plan = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::SearchIndexDdlWorkloads,
            scale,
        )
        .unwrap()
        .case()
        .plan_checked()
        .unwrap();

        assert_eq!(plan.kind(), PlanKind::Write);
        assert_eq!(plan.steps().len(), scale * 6);

        let creates = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::IndexDdl {
                        plan: IndexDdlPlan::Create { .. }
                    }
                )
            })
            .count();
        let drops = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::IndexDdl {
                        plan: IndexDdlPlan::Drop { .. }
                    }
                )
            })
            .count();
        assert_eq!(creates, scale * 5);
        assert_eq!(drops, scale);

        assert!(plan.steps().iter().any(|step| matches!(
            &step.op,
            ExecOp::IndexDdl {
                plan: IndexDdlPlan::Create {
                    spec: IndexDdlCreateSpec::NodeVector {
                        metric: VectorIndexMetric::Cosine,
                        ..
                    },
                    ..
                }
            }
        )));
        assert!(plan.steps().iter().any(|step| matches!(
            &step.op,
            ExecOp::IndexDdl {
                plan: IndexDdlPlan::Create {
                    spec: IndexDdlCreateSpec::EdgeVector {
                        metric: VectorIndexMetric::Euclidean,
                        ..
                    },
                    ..
                }
            }
        )));
        assert!(plan.steps().iter().any(|step| matches!(
            &step.op,
            ExecOp::IndexDdl {
                plan: IndexDdlPlan::Drop {
                    spec: IndexDdlDropSpec::EdgeText { .. }
                }
            }
        )));
        assert!(!plan.metrics().guardrail_hit);
    }

    #[test]
    fn runtime_derived_mixed_fixture_covers_query_service_style_workloads() {
        let scale = 4;
        let plan = fixtures::PlanScalabilityFixture::new(
            fixtures::PlanningScalabilityShape::RuntimeDerivedMixedQueries,
            scale,
        )
        .unwrap()
        .case()
        .plan_checked()
        .unwrap();

        assert_eq!(plan.kind(), PlanKind::Write);
        assert_eq!(plan.steps().len(), scale * 13);

        let node_eq_reads = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Access { plan }
                        if matches!(
                            plan.as_ref(),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::EqualityIndex { key, .. })
                                if (key.label == "User" && key.property == "username")
                                    || (key.label == "Audit" && key.property == "event_id")
                        )
                )
            })
            .count();
        let range_reads = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Access { plan }
                        if matches!(
                            plan.as_ref(),
                            ExecAccessPlan::Limited(limited)
                                if matches!(
                                    limited.source(),
                                    ExecAccessPlan::Node(ExecNodeAccessPlan::RangeIndex { key, .. })
                                        if key.label == "User" && key.property == "age"
                                )
                        )
                )
            })
            .count();
        let node_search_reads = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Access { plan }
                        if matches!(
                            plan.as_ref(),
                            ExecAccessPlan::Node(ExecNodeAccessPlan::VectorSearch { key, .. })
                                if key.label == "Doc" && key.property == "embedding"
                        )
                )
            })
            .count();
        let edge_text_searches = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Access { plan }
                        if matches!(
                            plan.as_ref(),
                            ExecAccessPlan::Edge(ExecEdgeAccessPlan::TextSearch { key, .. })
                                if key.label == "MENTIONS" && key.property == "body"
                        )
                )
            })
            .count();
        let variable_ops = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    ExecOp::Variable {
                        op: ExecVariableOp::SourceInject { .. }
                            | ExecVariableOp::Stream(StreamVariableOp::Store(_))
                    }
                )
            })
            .count();
        let mutations = plan
            .steps()
            .iter()
            .filter(|step| matches!(&step.op, ExecOp::Mutation { .. }))
            .count();

        assert_eq!(node_eq_reads, scale * 3);
        assert_eq!(range_reads, scale);
        assert_eq!(node_search_reads, scale);
        assert_eq!(edge_text_searches, scale);
        assert_eq!(variable_ops, scale * 2);
        assert_eq!(mutations, scale * 2);
        assert!(!plan.metrics().guardrail_hit);
    }
}
