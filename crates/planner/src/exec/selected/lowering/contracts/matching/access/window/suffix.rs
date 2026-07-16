//! Selected access-window physical suffix matching.

use super::contracts::{SelectedAccessWindowPipelineMatch, SelectedAccessWindowPipelineMismatch};
use crate::{logical, physical};

pub(in crate::exec::selected::lowering) fn selected_access_window_pipeline_matches(
    window: logical::AccessWindowRange,
    ops: &[physical::PhysicalPipelineOp],
) -> bool {
    selected_access_window_pipeline_match(window, ops).is_matched()
}

pub(super) fn selected_access_window_pipeline_match(
    window: logical::AccessWindowRange,
    ops: &[physical::PhysicalPipelineOp],
) -> SelectedAccessWindowPipelineMatch {
    match (window.start(), window.end(), ops) {
        (0, None, []) => SelectedAccessWindowPipelineMatch::Matched,
        (_, Some(_), [physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range)]) => {
            SelectedAccessWindowPipelineMatch::Matched
        }
        (start, None, [physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip)])
            if start > 0 =>
        {
            SelectedAccessWindowPipelineMatch::Matched
        }
        (0, None, _) => SelectedAccessWindowPipelineMatch::NotMatched(
            SelectedAccessWindowPipelineMismatch::IdentityWindowHasPhysicalOps,
        ),
        (_, Some(_), _) => SelectedAccessWindowPipelineMatch::NotMatched(
            SelectedAccessWindowPipelineMismatch::BoundedWindowNeedsRange,
        ),
        (start, None, _) if start > 0 => SelectedAccessWindowPipelineMatch::NotMatched(
            SelectedAccessWindowPipelineMismatch::OpenWindowNeedsSkip,
        ),
        _ => SelectedAccessWindowPipelineMatch::NotMatched(
            SelectedAccessWindowPipelineMismatch::UnsupportedPhysicalOps,
        ),
    }
}
