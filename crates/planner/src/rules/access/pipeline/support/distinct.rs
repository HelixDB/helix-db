//! Access-pipeline distinct simplification contracts.

use super::super::super::order::access_distinct_is_noop;
use super::super::contracts;
use crate::logical;

pub(super) fn simplify_pipeline_distinct(
    pipeline: &logical::AccessPipeline,
) -> contracts::PipelineDistinctSimplification {
    let ops = pipeline.ops();
    // Filters only drop rows, so a distinct after them is a no-op whenever
    // it would be one directly over the access.
    let filters = ops
        .iter()
        .take_while(|op| {
            matches!(
                op,
                logical::StreamPipelineOp::Filter { .. }
                    | logical::StreamPipelineOp::IndexMembership { .. }
            )
        })
        .count();
    if matches!(ops.get(filters), Some(logical::StreamPipelineOp::Distinct))
        && access_distinct_is_noop(&logical::AccessDistinct::new(pipeline.access().clone()))
    {
        let mut ops = ops.to_vec();
        ops.remove(filters);
        return contracts::PipelineDistinctSimplification::Rewritten(
            super::access_pipeline_result(pipeline.access().clone(), ops),
        );
    }

    match adjacent_distinct_pair(ops) {
        contracts::PipelineDistinctPair::Adjacent { first_index } => {
            let mut ops = ops.to_vec();
            ops.remove(first_index + 1);
            contracts::PipelineDistinctSimplification::Rewritten(super::access_pipeline_result(
                pipeline.access().clone(),
                ops,
            ))
        }
        contracts::PipelineDistinctPair::NotFound => {
            contracts::PipelineDistinctSimplification::NotApplicable(
                contracts::PipelineDistinctRejection::NoReducibleDistinct,
            )
        }
    }
}

fn adjacent_distinct_pair(ops: &[logical::StreamPipelineOp]) -> contracts::PipelineDistinctPair {
    ops.windows(2)
        .enumerate()
        .find_map(|(index, window)| {
            matches!(
                window,
                [
                    logical::StreamPipelineOp::Distinct,
                    logical::StreamPipelineOp::Distinct
                ]
            )
            .then_some(contracts::PipelineDistinctPair::Adjacent { first_index: index })
        })
        .unwrap_or(contracts::PipelineDistinctPair::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;

    fn limit_op() -> logical::StreamPipelineOp {
        logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(10),
        }
    }

    #[test]
    fn a_distinct_after_filters_over_a_duplicate_free_access_is_dropped() {
        let filter = || logical::StreamPipelineOp::Filter {
            predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        };
        let pipeline = |source| {
            logical::AccessPipeline::new(
                logical::AccessPath::Node(logical::NodeAccessPath::new(
                    ir::NodeAccessSourcePlan::from_unfiltered(source),
                )),
                ir::AtLeast::<_, 1>::from_one_and_rest(
                    filter(),
                    vec![filter(), logical::StreamPipelineOp::Distinct, limit_op()],
                ),
            )
            .unwrap()
        };
        let ids = ir::NodeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(1, vec![2])).unwrap(),
        };
        assert!(matches!(
            simplify_pipeline_distinct(&pipeline(ids)),
            contracts::PipelineDistinctSimplification::Rewritten(_)
        ));
        assert!(matches!(
            simplify_pipeline_distinct(&pipeline(ir::NodeAccessPlan::AllScan)),
            contracts::PipelineDistinctSimplification::NotApplicable(_)
        ));
    }

    #[test]
    fn adjacent_distinct_pair_reports_first_reducible_pair() {
        let ops = vec![
            logical::StreamPipelineOp::Distinct,
            limit_op(),
            logical::StreamPipelineOp::Distinct,
            logical::StreamPipelineOp::Distinct,
        ];

        assert_eq!(
            adjacent_distinct_pair(&ops),
            contracts::PipelineDistinctPair::Adjacent { first_index: 2 }
        );
        assert_eq!(
            adjacent_distinct_pair(&ops[..3]),
            contracts::PipelineDistinctPair::NotFound
        );
    }
}
