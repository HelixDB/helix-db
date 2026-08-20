//! Interpreter-facing executable step order.
//!
//! Executable plans store steps by stable planner ID, not necessarily in a
//! topological order that an interpreter should walk directly. This module
//! derives a deterministic stage order from the validated DAG: every stage is
//! ready after all prior stages complete, barrier steps form serial boundaries,
//! and a parallel stage contains two or more independent non-barrier ready
//! steps.

use serde::{Deserialize, Serialize};

use crate::{ir, properties};

use super::ExecStepId;

/// Validated execution stages for an executable DAG.
///
/// ```
/// use helix_planner::{cost, exec, ir, properties, trace};
///
/// fn step(id: usize, dependencies: Vec<exec::ExecStepId>) -> exec::ExecStep {
///     exec::ExecStep {
///         id: exec::ExecStepId::new(id).unwrap(),
///         dependencies,
///         output: ir::BatchOutputPlan::Discard,
///         semantic_return_shape: None,
///         condition: exec::ExecCondition::Always,
///         op: exec::ExecOp::Noop,
///         schedule: exec::ExecSchedule::Pipeline,
///         delivered: properties::DeliveredProperties::default(),
///         cost: cost::CostVector::ZERO,
///     }
/// }
///
/// let first = exec::ExecStepId::new(1).unwrap();
/// let second = exec::ExecStepId::new(2).unwrap();
/// let root = exec::ExecStepId::new(3).unwrap();
/// let plan = exec::ExecutablePlan::new(
///     ir::PlanKind::Read,
///     ir::ReturnPlan::None,
///     ir::AtLeast::<_, 1>::from_one_and_rest(
///         step(1, Vec::new()),
///         vec![step(2, Vec::new()), step(3, vec![first, second])],
///     ),
///     root,
///     trace::PlanningTrace::default(),
///     exec::PlannerMetrics::default(),
/// )
/// .unwrap();
///
/// assert_eq!(plan.execution_order().step_ids().collect::<Vec<_>>(), vec![first, second, root]);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecExecutionOrder {
    stages: ir::AtLeast<ExecExecutionStage, 1>,
}

impl ExecExecutionOrder {
    pub(in crate::exec) const fn new(stages: ir::AtLeast<ExecExecutionStage, 1>) -> Self {
        Self { stages }
    }

    /// Ordered execution stages.
    pub fn stages(&self) -> &[ExecExecutionStage] {
        self.stages.as_ref()
    }

    /// Step IDs in deterministic executable order.
    pub fn step_ids(&self) -> impl Iterator<Item = ExecStepId> + '_ {
        self.stages.iter().flat_map(ExecExecutionStage::iter)
    }
}

/// One ready stage in an executable DAG.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecExecutionStage {
    /// Exactly one step is ready.
    Single(ExecStepId),
    /// Two or more independent non-barrier steps are ready and may run
    /// concurrently.
    Parallel(ExecParallelStage),
}

impl ExecExecutionStage {
    /// Number of steps in this stage.
    pub fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Parallel(stage) => stage.len(),
        }
    }

    /// Whether this stage has no steps.
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Step IDs in stable order within this stage.
    pub fn iter(&self) -> impl Iterator<Item = ExecStepId> + '_ {
        let (single, parallel) = match self {
            Self::Single(id) => (Some(*id), None),
            Self::Parallel(stage) => (None, Some(stage.ids())),
        };
        single
            .into_iter()
            .chain(parallel.into_iter().flatten().copied())
    }
}

/// Tunable runtime policy for an executable parallel stage.
///
/// ```
/// use helix_planner::{exec, properties};
///
/// let policy = exec::ExecParallelStagePolicy::new(
///     properties::PositiveUsize::at_least_one(4),
///     true,
/// );
/// assert_eq!(policy.max_concurrency().get(), 4);
/// assert!(policy.preserve_order());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecParallelStagePolicy {
    max_concurrency: properties::PositiveUsize,
    preserve_order: bool,
}

impl ExecParallelStagePolicy {
    /// Build a stage policy with a positive concurrency bound.
    pub const fn new(max_concurrency: properties::PositiveUsize, preserve_order: bool) -> Self {
        Self {
            max_concurrency,
            preserve_order,
        }
    }

    /// Build the default policy for an independent ready set.
    pub fn for_ready_width(width: usize) -> Self {
        Self::new(properties::PositiveUsize::at_least_one(width.max(1)), true)
    }

    /// Maximum number of stage tasks the runtime should poll concurrently.
    pub const fn max_concurrency(self) -> properties::PositiveUsize {
        self.max_concurrency
    }

    /// Whether consumers require logical stage order to be restored.
    pub const fn preserve_order(self) -> bool {
        self.preserve_order
    }
}

/// Two or more executable steps plus their bounded parallel runtime policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecParallelStage {
    ids: ir::AtLeast<ExecStepId, 2>,
    policy: ExecParallelStagePolicy,
}

impl ExecParallelStage {
    /// Build a parallel stage from a proven non-empty-at-least-two ID set.
    pub const fn new(ids: ir::AtLeast<ExecStepId, 2>, policy: ExecParallelStagePolicy) -> Self {
        Self { ids, policy }
    }

    /// Step IDs in stable stage order.
    pub fn ids(&self) -> &[ExecStepId] {
        self.ids.as_ref()
    }

    /// Stage runtime policy.
    pub const fn policy(&self) -> ExecParallelStagePolicy {
        self.policy
    }

    /// Maximum number of stage tasks the runtime should poll concurrently.
    pub const fn max_concurrency(&self) -> properties::PositiveUsize {
        self.policy.max_concurrency()
    }

    /// Whether logical stage order must be preserved.
    pub const fn preserve_order(&self) -> bool {
        self.policy.preserve_order()
    }

    /// Number of steps in this stage.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether this stage has no steps.
    ///
    /// Always false because the `AtLeast<_, 2>` payload makes an empty
    /// parallel stage unrepresentable.
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Step IDs in stable order within this stage.
    pub fn iter(&self) -> impl Iterator<Item = ExecStepId> + '_ {
        self.ids.as_ref().iter().copied()
    }
}
