//! Scheduler candidate predicates for access-backed stream pipelines.
//!
//! These predicates are conservative: they may return cheap false positives,
//! but they must include every case where the currently implemented local
//! access-pipeline simplification rule can rewrite the pipeline.

use super::{AccessPipeline, StreamPipelineOp, StreamPipelineOpKind};
use crate::logical::AccessSourceKind;

pub(super) fn pipeline_has_local_simplification_candidate(pipeline: &AccessPipeline) -> bool {
    pipeline.access().source_kind() == AccessSourceKind::Empty
        || has_adjacent_ops(pipeline.ops(), StreamPipelineOpKind::Filter)
        || has_distinct_simplification_candidate(pipeline.ops())
}

fn has_distinct_simplification_candidate(ops: &[StreamPipelineOp]) -> bool {
    matches!(ops.first(), Some(StreamPipelineOp::Distinct))
        || has_adjacent_ops(ops, StreamPipelineOpKind::Distinct)
}

fn has_adjacent_ops(ops: &[StreamPipelineOp], kind: StreamPipelineOpKind) -> bool {
    ops.windows(2)
        .any(|window| window[0].kind() == kind && window[1].kind() == kind)
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Predicate;

    use super::*;
    use crate::ir;
    use crate::logical::{AccessPath, NodeAccessPath};

    fn access(source: ir::NodeAccessPlan) -> AccessPath {
        AccessPath::Node(NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(source),
        ))
    }

    fn limit_op() -> StreamPipelineOp {
        StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        }
    }

    fn filter_op(property: &'static str) -> StreamPipelineOp {
        StreamPipelineOp::Filter {
            predicate: ir::PredicatePlan::new(Predicate::eq(property, true)).unwrap(),
        }
    }

    #[test]
    fn local_simplification_candidate_is_conservative_for_known_rewrites() {
        let empty = AccessPipeline::new(
            access(ir::NodeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one(limit_op()),
        )
        .unwrap();
        let adjacent_filters = AccessPipeline::new(
            access(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                filter_op("active"),
                vec![filter_op("verified")],
            ),
        )
        .unwrap();
        let leading_distinct = AccessPipeline::new(
            access(ir::NodeAccessPlan::PointIds {
                ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(1)).unwrap(),
            }),
            ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::Distinct),
        )
        .unwrap();
        let adjacent_distinct = AccessPipeline::new(
            access(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                limit_op(),
                vec![StreamPipelineOp::Distinct, StreamPipelineOp::Distinct],
            ),
        )
        .unwrap();
        let ordinary = AccessPipeline::new(
            access(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one(limit_op()),
        )
        .unwrap();

        assert!(empty.has_local_simplification_candidate());
        assert!(adjacent_filters.has_local_simplification_candidate());
        assert!(leading_distinct.has_local_simplification_candidate());
        assert!(adjacent_distinct.has_local_simplification_candidate());
        assert!(!ordinary.has_local_simplification_candidate());
    }
}
