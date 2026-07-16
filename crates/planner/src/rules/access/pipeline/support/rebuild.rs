//! Access-pipeline reconstruction contracts.

use super::super::contracts;
use crate::{ir, logical, optimizer, rules};

pub(super) fn access_pipeline_result(
    access: logical::AccessPath,
    ops: Vec<logical::StreamPipelineOp>,
) -> optimizer::RuleResult {
    rebuild_access_pipeline(access, ops).into_rule_result()
}

fn rebuild_access_pipeline(
    access: logical::AccessPath,
    ops: Vec<logical::StreamPipelineOp>,
) -> contracts::AccessPipelineRebuild {
    let Some(ops) = ir::AtLeast::<_, 1>::try_from_vec(ops) else {
        return contracts::AccessPipelineRebuild::Collapsed(access);
    };
    match logical::AccessPipeline::new(access, ops) {
        Some(pipeline) => contracts::AccessPipelineRebuild::Pipeline(pipeline),
        None => contracts::AccessPipelineRebuild::NotApplicable(
            contracts::AccessPipelineRebuildRejection::InvalidPipelineShape,
        ),
    }
}

impl contracts::AccessPipelineRebuild {
    fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::Collapsed(access) => rules::access_path_result(access),
            Self::Pipeline(pipeline) => {
                optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
                    ir::AtLeast::<_, 1>::from_one(logical::LogicalExpr::AccessPipeline(pipeline)),
                ))
            }
            Self::NotApplicable(_) => optimizer::RuleResult::NotApplicable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_access() -> logical::AccessPath {
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
        ))
    }

    #[test]
    fn rebuild_access_pipeline_reports_invalid_reconstructed_pipelines() {
        let result = rebuild_access_pipeline(
            node_access(),
            vec![logical::StreamPipelineOp::Window {
                window: logical::AccessWindowRange::new(0, None).unwrap(),
            }],
        );

        assert_eq!(
            result,
            contracts::AccessPipelineRebuild::NotApplicable(
                contracts::AccessPipelineRebuildRejection::InvalidPipelineShape
            )
        );
    }

    #[test]
    fn rebuild_access_pipeline_collapses_empty_suffix_to_access_path() {
        assert!(matches!(
            rebuild_access_pipeline(node_access(), Vec::new()),
            contracts::AccessPipelineRebuild::Collapsed(logical::AccessPath::Node(_))
        ));
    }

    #[test]
    fn access_pipeline_result_keeps_rule_result_contract() {
        assert!(matches!(
            access_pipeline_result(node_access(), Vec::new()),
            optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(exprs))
                if matches!(
                    exprs.as_ref(),
                    [logical::LogicalExpr::AccessPath(logical::AccessPath::Node(_))]
                )
        ));
        assert_eq!(
            access_pipeline_result(
                node_access(),
                vec![logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(0, None).unwrap(),
                }],
            ),
            optimizer::RuleResult::NotApplicable
        );
    }
}
