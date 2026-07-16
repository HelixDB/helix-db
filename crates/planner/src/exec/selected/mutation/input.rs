//! Selected mutation input mode.

use crate::exec::selected::run::SelectedExecutableRunRoot;

/// Selected mutation input mode.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedMutationInput {
    /// Mutation is a source operation.
    Source,
    /// Mutation consumes another selected run root.
    FromInput(Box<SelectedExecutableRunRoot>),
}
