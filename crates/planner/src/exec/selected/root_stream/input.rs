//! Recursive selected root-stream input ADT.

use super::pipeline::SelectedRootPipeline;
use super::terminal::SelectedRootTerminalPlan;
use crate::exec::selected::control_flow::{SelectedRootBranch, SelectedRootRepeat};
use crate::exec::selected::count::SelectedRootCount;
use crate::exec::selected::mutation::SelectedRootMutation;
use crate::logical;

/// Selected input supported by a root-stream pipeline or terminal.
///
/// This ADT only admits stream-producing roots. Access and variable-source
/// inputs are fully described by their logical payload and the parent physical
/// prefix; control-flow, pipeline, and terminal inputs carry recursively
/// selected subplans so unselected compatibility children cannot be hidden in a
/// selected executable plan.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedRootStreamInput {
    /// Access-backed stream described directly by the parent alternative.
    Access(logical::AccessStream),
    /// Variable/source stream described directly by the parent alternative.
    VariableSource(logical::VariableSource),
    /// Stream consumes a selected mutation root.
    Mutation(Box<SelectedRootMutation>),
    /// Stream consumes a selected branch root.
    Branch(Box<SelectedRootBranch>),
    /// Stream consumes a selected repeat root.
    Repeat(Box<SelectedRootRepeat>),
    /// Stream consumes a selected root pipeline.
    Pipeline(Box<SelectedRootPipeline>),
    /// Stream consumes another selected root-stream terminal.
    Terminal(Box<SelectedRootTerminalPlan>),
    /// Stream consumes a selected cardinality scalar root.
    Count(Box<SelectedRootCount>),
}

impl SelectedRootStreamInput {
    pub(super) const fn accepts_parent_prefix(&self) -> bool {
        matches!(self, Self::Access(_) | Self::VariableSource(_))
    }
}
