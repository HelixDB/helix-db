//! Adjacent access-pipeline filter merge contracts.

use super::super::contracts;
use crate::{ir, logical};

pub(super) fn merge_pipeline_filters(
    pipeline: &logical::AccessPipeline,
) -> contracts::PipelineFilterMerge {
    let contracts::AdjacentFilterPair::Found {
        first_index,
        predicates,
    } = adjacent_filter_pair(pipeline.ops())
    else {
        return contracts::PipelineFilterMerge::NotApplicable(
            contracts::PipelineFilterMergeRejection::NoAdjacentFilters,
        );
    };

    let mut ops = pipeline.ops().to_vec();
    ops[first_index] = logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::conjunction(&predicates),
    };
    ops.remove(first_index + 1);
    contracts::PipelineFilterMerge::Merged(super::access_pipeline_result(
        pipeline.access().clone(),
        ops,
    ))
}

fn adjacent_filter_pair(ops: &[logical::StreamPipelineOp]) -> contracts::AdjacentFilterPair {
    ops.windows(2)
        .enumerate()
        .find_map(|(index, window)| match window {
            [
                logical::StreamPipelineOp::Filter { predicate: inner },
                logical::StreamPipelineOp::Filter { predicate: outer },
            ] => Some(contracts::AdjacentFilterPair::Found {
                first_index: index,
                predicates: ir::AtLeast::<_, 2>::from_pair(inner.clone(), outer.clone()),
            }),
            _ => None,
        })
        .unwrap_or(contracts::AdjacentFilterPair::NotFound)
}

#[cfg(test)]
mod tests {
    use helix_ast::expr;

    use super::*;
    use crate::optimizer;

    fn node_access() -> logical::AccessPath {
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
        ))
    }

    fn limit_op() -> logical::StreamPipelineOp {
        logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(10),
        }
    }

    #[test]
    fn adjacent_filter_pair_returns_validated_pair_contract() {
        let first = ir::PredicatePlan::new(expr::Predicate::eq("active", true)).unwrap();
        let second = ir::PredicatePlan::new(expr::Predicate::eq("tenant", "acme")).unwrap();
        let ops = vec![
            limit_op(),
            logical::StreamPipelineOp::Filter {
                predicate: first.clone(),
            },
            logical::StreamPipelineOp::Filter {
                predicate: second.clone(),
            },
        ];

        assert_eq!(
            adjacent_filter_pair(&ops),
            contracts::AdjacentFilterPair::Found {
                first_index: 1,
                predicates: ir::AtLeast::<_, 2>::from_pair(first, second),
            }
        );
        assert_eq!(
            adjacent_filter_pair(&ops[..2]),
            contracts::AdjacentFilterPair::NotFound
        );
    }

    #[test]
    fn merge_pipeline_filters_uses_validated_conjunction_contract() {
        let first = ir::PredicatePlan::new(expr::Predicate::eq("active", true)).unwrap();
        let second = ir::PredicatePlan::new(expr::Predicate::eq("tenant", "acme")).unwrap();
        let pipeline = logical::AccessPipeline::new(
            node_access(),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter { predicate: first },
                vec![logical::StreamPipelineOp::Filter { predicate: second }],
            ),
        )
        .unwrap();

        let contracts::PipelineFilterMerge::Merged(result) = merge_pipeline_filters(&pipeline)
        else {
            panic!("expected adjacent filters to merge");
        };
        let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs)) = result else {
            panic!("expected logical rewrite");
        };
        let [logical::LogicalExpr::AccessPipeline(rewritten)] = exprs.as_ref() else {
            panic!("expected access pipeline");
        };
        assert!(matches!(
            rewritten.ops(),
            [logical::StreamPipelineOp::Filter { predicate }]
                if matches!(predicate.as_ref(), expr::Predicate::And { predicates } if predicates.len() == 2)
        ));
    }
}
