//! Empty-source access-pipeline collapse contracts.

use super::super::contracts;
use crate::{ir, logical, properties};

pub(super) fn empty_pipeline_result(
    pipeline: &logical::AccessPipeline,
) -> contracts::EmptyPipelineResult {
    let mut element = match pipeline.access() {
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty) =>
        {
            properties::ElementKind::Node
        }
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty) =>
        {
            properties::ElementKind::Edge
        }
        _ => {
            return contracts::EmptyPipelineResult::NotEmpty(
                contracts::EmptyPipelineRejection::NonEmptyAccessSource,
            );
        }
    };

    for op in pipeline.ops() {
        match op {
            logical::StreamPipelineOp::Filter { .. }
            | logical::StreamPipelineOp::Window { .. }
            | logical::StreamPipelineOp::Limit { .. }
            | logical::StreamPipelineOp::Skip { .. }
            | logical::StreamPipelineOp::Range { .. }
            | logical::StreamPipelineOp::Order { .. }
            | logical::StreamPipelineOp::Distinct => {}
            logical::StreamPipelineOp::Expand { plan } => {
                element = match plan.output {
                    ir::ExpandOutput::Nodes => properties::ElementKind::Node,
                    ir::ExpandOutput::Edges => properties::ElementKind::Edge,
                };
            }
            logical::StreamPipelineOp::Variable {
                op:
                    logical::PureStreamVariableOp::Within(_) | logical::PureStreamVariableOp::Without(_),
            } => {}
            logical::StreamPipelineOp::Variable { .. }
            | logical::StreamPipelineOp::VariableWrite { .. } => {
                return contracts::EmptyPipelineResult::NotEmpty(
                    contracts::EmptyPipelineRejection::DataProducingPipelineOp,
                );
            }
        }
    }

    contracts::EmptyPipelineResult::Empty(empty_access_path(element))
}

fn empty_access_path(element: properties::ElementKind) -> logical::AccessPath {
    match element {
        properties::ElementKind::Node => logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty),
        )),
        properties::ElementKind::Edge => logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty),
        )),
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
    fn empty_pipeline_result_distinguishes_empty_sources_from_rejections() {
        let empty_pipeline = logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty),
            )),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap();
        let writing_pipeline = logical::AccessPipeline::new(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty),
            )),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::VariableWrite {
                op: logical::StreamVariableWriteOp::Store(ir::NonEmptyString::from_static("rows")),
            }),
        )
        .unwrap();
        let non_empty_source_pipeline = logical::AccessPipeline::new(
            node_access(),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap();

        assert!(matches!(
            empty_pipeline_result(&empty_pipeline),
            contracts::EmptyPipelineResult::Empty(logical::AccessPath::Node(_))
        ));
        assert_eq!(
            empty_pipeline_result(&writing_pipeline),
            contracts::EmptyPipelineResult::NotEmpty(
                contracts::EmptyPipelineRejection::DataProducingPipelineOp
            )
        );
        assert_eq!(
            empty_pipeline_result(&non_empty_source_pipeline),
            contracts::EmptyPipelineResult::NotEmpty(
                contracts::EmptyPipelineRejection::NonEmptyAccessSource
            )
        );
    }
}
