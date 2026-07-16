//! Selected access-prefix pipeline matching.

use super::super::source;
use super::contracts::{
    SelectedAccessPipelineMatch, SelectedAccessPipelineMismatch, SelectedAccessPipelineParts,
};
use crate::{logical, physical};

pub(in crate::exec::selected::lowering) fn selected_access_pipeline_parts<'a>(
    access_path: &logical::AccessPath,
    pipeline: &'a physical::PhysicalPipeline,
) -> SelectedAccessPipelineMatch<'a> {
    let [physical::PhysicalPipelineOp::Access { element, access }, rest @ ..] = pipeline.ops()
    else {
        return SelectedAccessPipelineMatch::NotMatched(
            SelectedAccessPipelineMismatch::MissingAccessPrefix,
        );
    };
    if *element != access_path.element() {
        return SelectedAccessPipelineMatch::NotMatched(
            SelectedAccessPipelineMismatch::ElementMismatch,
        );
    }
    match source::selected_access_path_match(access_path, access) {
        source::SelectedAccessPathMatch::Matched => {
            SelectedAccessPipelineMatch::Matched(SelectedAccessPipelineParts::new(access, rest))
        }
        source::SelectedAccessPathMatch::NotMatched(reason) => {
            SelectedAccessPipelineMatch::NotMatched(
                SelectedAccessPipelineMismatch::PhysicalAccessMismatch(reason),
            )
        }
    }
}
