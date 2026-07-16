//! Selected batch-entry contracts.
//!
//! Initial and follow-up entries use different condition payload types so
//! previous-result checks are statically impossible before a prior entry exists.

use super::run::SelectedExecutableRunRoot;
use crate::ir;

/// Selected run entry with a condition payload chosen by initial/follow-up
/// position.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedExecutableRunEntry<C> {
    /// Selected run root.
    pub root: SelectedExecutableRunRoot,
    /// Output binding behavior.
    pub output: ir::BatchOutputPlan,
    /// Run condition.
    pub condition: ir::RunConditionPlan<C>,
}

/// Selected `ForEach` batch wrapper.
///
/// The body is another selected batch, so it is non-empty by construction. The
/// parameter is an [`ir::NonEmptyString`], so lowering never has to revalidate
/// the executable binding name.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedForEachBatch {
    param: ir::NonEmptyString,
    body: Box<SelectedExecutableBatchEntries>,
}

impl SelectedForEachBatch {
    /// Build a selected `ForEach` wrapper around a non-empty selected body.
    pub fn new(param: ir::NonEmptyString, body: SelectedExecutableBatchEntries) -> Self {
        Self {
            param,
            body: Box::new(body),
        }
    }

    /// Parameter name bound for each input item.
    pub fn param(&self) -> &ir::NonEmptyString {
        &self.param
    }

    /// Selected nested batch body.
    pub fn body(&self) -> &SelectedExecutableBatchEntries {
        &self.body
    }

    /// Consume the wrapper into its validated parts.
    pub fn into_parts(self) -> (ir::NonEmptyString, SelectedExecutableBatchEntries) {
        (self.param, *self.body)
    }
}

/// Initial selected batch entry. The run condition type excludes
/// previous-result checks, matching the executable batch invariant.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedInitialExecutableBatchEntry {
    /// Selected run entry.
    Run(Box<SelectedExecutableRunEntry<ir::BatchVariableConditionPlan>>),
    /// Execute a selected nested body once per item in a parameter value.
    ForEach(SelectedForEachBatch),
}

/// Follow-up selected batch entry. Previous-result conditions are valid for
/// run entries because a prior root step exists by construction.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedFollowupExecutableBatchEntry {
    /// Selected run entry.
    Run(Box<SelectedExecutableRunEntry<ir::BatchConditionPlan>>),
    /// Execute a selected nested body once per item in a parameter value.
    ForEach(SelectedForEachBatch),
}

/// Non-empty selected executable batch entries.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedExecutableBatchEntries {
    /// A one-entry batch.
    Single(SelectedInitialExecutableBatchEntry),
    /// A first entry plus at least one follow-up entry.
    WithFollowups {
        /// First entry.
        first: SelectedInitialExecutableBatchEntry,
        /// Follow-up entries.
        rest: ir::AtLeast<SelectedFollowupExecutableBatchEntry, 1>,
    },
}
