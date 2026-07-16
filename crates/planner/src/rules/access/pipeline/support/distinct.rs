//! Access-pipeline distinct simplification contracts.

use super::super::super::order::access_distinct_is_noop;
use super::super::contracts;
use crate::logical;

pub(super) fn simplify_pipeline_distinct(
    pipeline: &logical::AccessPipeline,
) -> contracts::PipelineDistinctSimplification {
    let ops = pipeline.ops();
    if matches!(ops.first(), Some(logical::StreamPipelineOp::Distinct))
        && access_distinct_is_noop(&logical::AccessDistinct::new(pipeline.access().clone()))
    {
        return contracts::PipelineDistinctSimplification::Rewritten(
            super::access_pipeline_result(pipeline.access().clone(), ops[1..].to_vec()),
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
