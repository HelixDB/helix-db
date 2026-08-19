//! Selected access-pipeline matching outcomes.

use super::super::source;
use crate::physical;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessFilterPipelineMatch<'a> {
    Matched(&'a physical::PhysicalAccess),
    NotMatched(SelectedAccessFilterPipelineMismatch),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessFilterPipelineMismatch {
    AccessPrefix(SelectedAccessPipelineMismatch),
    PhysicalSuffixMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessPipelineMatch<'a> {
    Matched(SelectedAccessPipelineParts<'a>),
    NotMatched(SelectedAccessPipelineMismatch),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::exec::selected::lowering) enum SelectedAccessPipelineMismatch {
    MissingAccessPrefix,
    ElementMismatch,
    PhysicalAccessMismatch(source::SelectedAccessPathMismatch),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::exec::selected::lowering) struct SelectedAccessPipelineParts<'a> {
    access: &'a physical::PhysicalAccess,
    ops: &'a [physical::PhysicalPipelineOp],
}

impl<'a> SelectedAccessPipelineParts<'a> {
    pub(super) const fn new(
        access: &'a physical::PhysicalAccess,
        ops: &'a [physical::PhysicalPipelineOp],
    ) -> Self {
        Self { access, ops }
    }

    pub(in crate::exec::selected::lowering) const fn into_parts(
        self,
    ) -> (
        &'a physical::PhysicalAccess,
        &'a [physical::PhysicalPipelineOp],
    ) {
        (self.access, self.ops)
    }
}
