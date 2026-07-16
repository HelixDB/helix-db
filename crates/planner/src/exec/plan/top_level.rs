use serde::{Deserialize, Serialize};

use crate::{cost, ir, logical, physical, trace};

use crate::exec::validation::execution_order;
use crate::exec::{
    selected, ExecCondition, ExecExecutionOrder, ExecPlanError, ExecStep, ExecStepId,
    PlannerMetrics, SelectedExecutableBatchPlanRequest, SelectedExecutablePlanRequest,
};

/// Interpreter-facing executable DAG plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ExecutablePlanUnchecked")]
pub struct ExecutablePlan {
    /// Plan kind.
    kind: ir::PlanKind,
    /// Returned variables.
    returns: ir::ReturnPlan,
    /// DAG steps.
    steps: ir::AtLeast<ExecStep, 1>,
    /// Root step.
    root: ExecStepId,
    /// Deterministic interpreter-ready execution stages, derived during
    /// validation and skipped in serde because it is redundant with `steps`.
    #[serde(skip)]
    execution_order: ExecExecutionOrder,
    /// Planning trace.
    trace: trace::PlanningTrace,
    /// Planner performance metrics.
    metrics: PlannerMetrics,
}

impl ExecutablePlan {
    /// Build and validate an executable plan.
    pub fn new(
        kind: ir::PlanKind,
        returns: ir::ReturnPlan,
        steps: ir::AtLeast<ExecStep, 1>,
        root: ExecStepId,
        trace: trace::PlanningTrace,
        metrics: PlannerMetrics,
    ) -> Result<Self, ExecPlanError> {
        let execution_order = execution_order(&steps, root)?;
        Ok(Self {
            kind,
            returns,
            steps,
            root,
            execution_order,
            trace,
            metrics,
        })
    }

    /// Build a validated executable plan from a Cascades-selected standalone
    /// physical alternative.
    pub fn from_selected_executable_alternative(
        kind: ir::PlanKind,
        returns: ir::ReturnPlan,
        trace: trace::PlanningTrace,
        metrics: PlannerMetrics,
        source_expr: &logical::LogicalExpr,
        alternative: &physical::PhysicalAlternative,
        profile: &cost::StorageCostProfile,
    ) -> Result<Self, ExecPlanError> {
        let subplan = selected::lowering::lower_selected_executable_alternative(
            source_expr,
            alternative,
            profile,
            Vec::new(),
            ir::BatchOutputPlan::Discard,
            ExecCondition::Always,
        )?;
        let (steps, root) = subplan.into_parts();
        Self::new(kind, returns, steps, root, trace, metrics)
    }

    /// Build a validated executable plan from a Cascades-selected standalone
    /// physical alternative with explicit batch output and run condition.
    pub fn from_selected_executable_alternative_with_io(
        request: SelectedExecutablePlanRequest<'_>,
    ) -> Result<Self, ExecPlanError> {
        let subplan = selected::lowering::lower_selected_executable_alternative(
            request.source_expr,
            request.alternative,
            request.profile,
            Vec::new(),
            request.output,
            request.condition,
        )?;
        let (steps, root) = subplan.into_parts();
        Self::new(
            request.kind,
            request.returns,
            steps,
            root,
            request.trace,
            request.metrics,
        )
    }

    /// Build a validated executable plan from selected executable batch entries.
    pub fn from_selected_executable_batch(
        request: SelectedExecutableBatchPlanRequest<'_>,
    ) -> Result<Self, ExecPlanError> {
        let subplan = selected::lowering::lower_selected_executable_batch_entries(
            request.entries,
            request.profile,
        )?;
        let (steps, root) = subplan.into_parts();
        Self::new(
            request.kind,
            request.returns,
            steps,
            root,
            request.trace,
            request.metrics,
        )
    }

    /// Plan kind.
    pub const fn kind(&self) -> ir::PlanKind {
        self.kind
    }

    /// Returned variables.
    pub const fn returns(&self) -> &ir::ReturnPlan {
        &self.returns
    }

    /// DAG steps in stable ID order chosen by the planner.
    pub fn steps(&self) -> &[ExecStep] {
        self.steps.as_ref()
    }

    /// Root step.
    pub const fn root(&self) -> ExecStepId {
        self.root
    }

    /// Deterministic interpreter-ready execution stages.
    pub fn execution_order(&self) -> ExecExecutionOrder {
        self.execution_order.clone()
    }

    /// Planning trace.
    pub const fn trace(&self) -> &trace::PlanningTrace {
        &self.trace
    }

    /// Planner performance metrics.
    pub const fn metrics(&self) -> &PlannerMetrics {
        &self.metrics
    }
}

#[derive(Debug, Deserialize)]
struct ExecutablePlanUnchecked {
    kind: ir::PlanKind,
    returns: ir::ReturnPlan,
    steps: ir::AtLeast<ExecStep, 1>,
    root: ExecStepId,
    trace: trace::PlanningTrace,
    metrics: PlannerMetrics,
}

impl TryFrom<ExecutablePlanUnchecked> for ExecutablePlan {
    type Error = ExecPlanError;

    fn try_from(value: ExecutablePlanUnchecked) -> Result<Self, Self::Error> {
        Self::new(
            value.kind,
            value.returns,
            value.steps,
            value.root,
            value.trace,
            value.metrics,
        )
    }
}
