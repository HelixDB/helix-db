//! Selected access-filter pipeline matching.

use super::contracts::{
    SelectedAccessFilterPipelineMatch, SelectedAccessFilterPipelineMismatch,
    SelectedAccessPipelineMatch,
};
use super::prefix::selected_access_pipeline_parts;
use crate::{logical, physical};

pub(in crate::exec::selected::lowering) fn selected_access_filter_pipeline_access<'a>(
    filter: &logical::AccessFilter,
    pipeline: &'a physical::PhysicalPipeline,
) -> SelectedAccessFilterPipelineMatch<'a> {
    let parts = match selected_access_pipeline_parts(filter.access(), pipeline) {
        SelectedAccessPipelineMatch::Matched(parts) => parts,
        SelectedAccessPipelineMatch::NotMatched(reason) => {
            return SelectedAccessFilterPipelineMatch::NotMatched(
                SelectedAccessFilterPipelineMismatch::AccessPrefix(reason),
            );
        }
    };
    let (access, ops) = parts.into_parts();
    match ops {
        [physical::PhysicalPipelineOp::ResidualFilter] => {
            SelectedAccessFilterPipelineMatch::Matched(access)
        }
        _ => SelectedAccessFilterPipelineMatch::NotMatched(
            SelectedAccessFilterPipelineMismatch::PhysicalSuffixMismatch,
        ),
    }
}
