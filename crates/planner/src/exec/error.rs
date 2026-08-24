use crate::{exec, ir, properties};

use super::{ElementKeyspace, ExecStepId};

/// Executable-plan validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecPlanError {
    /// Multi-get cannot be empty.
    EmptyMultiGet,
    /// Multi-get contained keys from more than one keyspace.
    MixedMultiGetKeyspace {
        /// Expected keyspace.
        expected: ElementKeyspace,
        /// Actual keyspace.
        actual: ElementKeyspace,
    },
    /// Multi-get exceeded the selected batch limit.
    MultiGetBatchTooLarge {
        /// Maximum batch size.
        max: properties::PositiveUsize,
        /// Actual key count.
        actual: usize,
    },
    /// Multi-get original logical input positions were not unique.
    DuplicateMultiGetOriginalPosition {
        /// Duplicate logical input position.
        position: usize,
    },
    /// A complex access plan reached simple access-leaf conversion.
    UnsupportedSimpleAccessLeaf {
        /// Element kind for the rejected access leaf.
        element: properties::ElementKind,
    },
    /// Duplicate step ID.
    DuplicateStepId { id: ExecStepId },
    /// Root step was not present.
    MissingRoot { root: ExecStepId },
    /// Requested return variable has no executable binding.
    MissingReturnBinding { name: ir::NonEmptyString },
    /// Dependency step was not present.
    MissingDependency {
        step: ExecStepId,
        dependency: ExecStepId,
    },
    /// Step depends on itself.
    SelfDependency { step: ExecStepId },
    /// Dependency cycle detected.
    DependencyCycle { step: ExecStepId },
    /// Step is not reachable from the executable root.
    UnreachableStep {
        /// Unreachable step.
        step: ExecStepId,
        /// Root step.
        root: ExecStepId,
    },
    /// Executable DAG step ID cursor cannot allocate another stable ID.
    StepIdSpaceExhausted,
    /// Parallel schedule requires at least two dependencies.
    InvalidParallelDependencyCount { step: ExecStepId, actual: usize },
    /// A count program failed its internal executable-contract validation.
    InvalidCountProgram {
        /// Count step.
        step: ExecStepId,
        /// Exact rejected invariant.
        reason: exec::ExecCountValidationError,
    },
    /// A count step's predecessor shape disagreed with its selected input.
    InvalidCountDependencyCount {
        /// Count step.
        step: ExecStepId,
        /// Selected input contract.
        dependency: exec::ExecCountDependency,
        /// Actual predecessor count.
        actual: usize,
    },
    /// Previous-output condition referenced a step that is not a dependency.
    PreviousConditionMissingDependency {
        /// Step with the condition.
        step: ExecStepId,
        /// Required dependency.
        dependency: ExecStepId,
    },
    /// Execution-stage derivation produced an invalid ready set.
    InvalidExecutionStage {
        /// Number of ready steps in the invalid stage.
        actual: usize,
    },
    /// Execution-order derivation did not emit every validated step.
    IncompleteExecutionOrder {
        /// Number of steps emitted into execution stages.
        emitted: usize,
        /// Number of validated steps in the DAG.
        total: usize,
    },
    /// A selected executable alternative could not be lowered directly
    /// because its source logical expression did not carry enough detail or did
    /// not match the physical operator contract.
    UnsupportedSelectedExecutableAlternative {
        /// Stable rejection reason.
        reason: ir::NonEmptyString,
    },
}

impl std::fmt::Display for ExecPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyMultiGet => f.write_str("multi_get requires at least one key"),
            Self::MixedMultiGetKeyspace { expected, actual } => write!(
                f,
                "multi_get keyspace mismatch: expected `{expected}`, got `{actual}`"
            ),
            Self::MultiGetBatchTooLarge { max, actual } => {
                write!(f, "multi_get batch has {actual} keys, max is {}", max.get())
            }
            Self::DuplicateMultiGetOriginalPosition { position } => write!(
                f,
                "multi_get original input position {position} appears more than once"
            ),
            Self::UnsupportedSimpleAccessLeaf { element } => {
                write!(f, "unsupported simple access leaf for {element:?}")
            }
            Self::DuplicateStepId { id } => write!(f, "duplicate exec step id {}", id.get()),
            Self::MissingRoot { root } => write!(f, "missing root exec step {}", root.get()),
            Self::MissingReturnBinding { name } => {
                write!(f, "return variable `{name}` has no executable binding")
            }
            Self::MissingDependency { step, dependency } => write!(
                f,
                "exec step {} depends on missing step {}",
                step.get(),
                dependency.get()
            ),
            Self::SelfDependency { step } => {
                write!(f, "exec step {} depends on itself", step.get())
            }
            Self::DependencyCycle { step } => {
                write!(f, "dependency cycle reaches exec step {}", step.get())
            }
            Self::UnreachableStep { step, root } => write!(
                f,
                "exec step {} is not reachable from root step {}",
                step.get(),
                root.get()
            ),
            Self::StepIdSpaceExhausted => f.write_str("executable DAG step ID space exhausted"),
            Self::InvalidParallelDependencyCount { step, actual } => write!(
                f,
                "parallel exec step {} has {actual} dependencies, expected at least 2",
                step.get()
            ),
            Self::InvalidCountProgram { step, reason } => write!(
                f,
                "count exec step {} contains an invalid program: {reason:?}",
                step.get()
            ),
            Self::InvalidCountDependencyCount {
                step,
                dependency,
                actual,
            } => write!(
                f,
                "count exec step {} has {actual} predecessors for {dependency:?} input",
                step.get()
            ),
            Self::PreviousConditionMissingDependency { step, dependency } => write!(
                f,
                "exec step {} has previous-result condition on non-dependency step {}",
                step.get(),
                dependency.get()
            ),
            Self::InvalidExecutionStage { actual } => write!(
                f,
                "execution stage has {actual} ready steps, expected at least 1"
            ),
            Self::IncompleteExecutionOrder { emitted, total } => write!(
                f,
                "execution order emitted {emitted} steps, expected {total}"
            ),
            Self::UnsupportedSelectedExecutableAlternative { reason } => {
                write!(f, "unsupported selected executable alternative: {reason}")
            }
        }
    }
}
