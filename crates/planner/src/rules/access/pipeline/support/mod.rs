//! Access-pipeline rewrite support contracts.
//!
//! The public rule wrappers live in the parent module. This facade composes
//! narrow support contracts for access-pipeline rebuilds, empty-source
//! collapse, and distinct simplification.

mod distinct;
mod empty;
mod rebuild;

use super::contracts;
use crate::{logical, optimizer, rules};

pub(super) fn access_pipeline_result(
    access: logical::AccessPath,
    ops: Vec<logical::StreamPipelineOp>,
) -> optimizer::RuleResult {
    rebuild::access_pipeline_result(access, ops)
}

pub(super) fn simplify_pipeline(
    pipeline: &logical::AccessPipeline,
) -> contracts::PipelineSimplification {
    let empty = match empty::empty_pipeline_result(pipeline) {
        contracts::EmptyPipelineResult::Empty(access) => {
            return contracts::PipelineSimplification::Rewritten(rules::access_path_result(access));
        }
        contracts::EmptyPipelineResult::NotEmpty(rejection) => rejection,
    };

    let distinct = match distinct::simplify_pipeline_distinct(pipeline) {
        contracts::PipelineDistinctSimplification::Rewritten(result) => {
            return contracts::PipelineSimplification::Rewritten(result);
        }
        contracts::PipelineDistinctSimplification::NotApplicable(rejection) => rejection,
    };

    contracts::PipelineSimplification::NotApplicable(
        contracts::PipelineSimplificationRejection::NoLocalSimplification { empty, distinct },
    )
}

#[cfg(test)]
mod tests {
    use helix_ast::expr;

    use super::*;
    use crate::ir;

    fn node_access() -> logical::AccessPath {
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
        ))
    }

    #[test]
    fn simplify_pipeline_reports_rejection_reasons_for_irreducible_pipeline() {
        let pipeline = logical::AccessPipeline::new(
            node_access(),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Filter {
                predicate: ir::PredicatePlan::new(expr::Predicate::eq("active", true)).unwrap(),
            }),
        )
        .unwrap();

        assert_eq!(
            simplify_pipeline(&pipeline),
            contracts::PipelineSimplification::NotApplicable(
                contracts::PipelineSimplificationRejection::NoLocalSimplification {
                    empty: contracts::EmptyPipelineRejection::NonEmptyAccessSource,
                    distinct: contracts::PipelineDistinctRejection::NoReducibleDistinct,
                }
            )
        );
    }
}
