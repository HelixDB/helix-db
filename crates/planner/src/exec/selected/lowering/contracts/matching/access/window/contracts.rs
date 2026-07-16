//! Selected access-window physical suffix matching outcomes.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SelectedAccessWindowPipelineMatch {
    Matched,
    NotMatched(SelectedAccessWindowPipelineMismatch),
}

impl SelectedAccessWindowPipelineMatch {
    pub(super) const fn is_matched(self) -> bool {
        matches!(self, Self::Matched)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SelectedAccessWindowPipelineMismatch {
    IdentityWindowHasPhysicalOps,
    BoundedWindowNeedsRange,
    OpenWindowNeedsSkip,
    UnsupportedPhysicalOps,
}
