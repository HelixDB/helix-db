//! Parent-local physical prefix proof for selected root-stream inputs.

use super::input::SelectedRootStreamInput;
use crate::exec::selected::SelectedRootConstructionError;
use crate::physical;

/// Physical operators that are localized to a root-stream input.
///
/// Recursive selected root-stream inputs must already own their physical work,
/// so their parent prefix is always empty. Access and variable-source leaves
/// may consume a parent-local prefix because the parent alternative fully
/// describes those leaf streams.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::selected) struct SelectedRootStreamInputPrefix {
    ops: Vec<physical::PhysicalPipelineOp>,
}

impl SelectedRootStreamInputPrefix {
    pub(super) fn new(
        input: &SelectedRootStreamInput,
        ops: &[physical::PhysicalPipelineOp],
    ) -> Result<Self, SelectedRootConstructionError> {
        if !ops.is_empty() && !input.accepts_parent_prefix() {
            return Err(SelectedRootConstructionError::RecursiveRootStreamInputNonLocalizedPrefix);
        }
        Ok(Self { ops: ops.to_vec() })
    }

    pub(in crate::exec::selected) fn as_slice(&self) -> &[physical::PhysicalPipelineOp] {
        &self.ops
    }
}
