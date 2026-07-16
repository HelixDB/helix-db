//! Requests that cross into selected executable lowering.
//!
//! These request structs keep planner-facing inputs separate from the selected
//! IR ADTs. Lowering consumes these at the interpreter boundary; contract
//! modules below describe the selected roots and batch entries themselves.

use super::super::{ExecCondition, PlannerMetrics};
use super::batch::SelectedExecutableBatchEntries;
use crate::{cost, ir, logical, physical, trace};

/// Request to lower a Cascades-selected physical alternative into an executable
/// subplan.
pub struct SelectedExecutablePlanRequest<'a> {
    /// Plan kind.
    pub kind: ir::PlanKind,
    /// Returned variables.
    pub returns: ir::ReturnPlan,
    /// Planning trace.
    pub trace: trace::PlanningTrace,
    /// Planner metrics.
    pub metrics: PlannerMetrics,
    /// Logical expression that produced the selected physical alternative.
    pub source_expr: &'a logical::LogicalExpr,
    /// Selected physical alternative.
    pub alternative: &'a physical::PhysicalAlternative,
    /// Storage cost profile.
    pub profile: &'a cost::StorageCostProfile,
    /// Output binding behavior.
    pub output: ir::BatchOutputPlan,
    /// Run condition.
    pub condition: ExecCondition,
}

/// Request to lower selected executable batch entries into a top-level
/// executable plan.
pub struct SelectedExecutableBatchPlanRequest<'a> {
    /// Plan kind.
    pub kind: ir::PlanKind,
    /// Returned variables.
    pub returns: ir::ReturnPlan,
    /// Planning trace.
    pub trace: trace::PlanningTrace,
    /// Planner metrics.
    pub metrics: PlannerMetrics,
    /// Selected batch entries.
    pub entries: SelectedExecutableBatchEntries,
    /// Storage cost profile.
    pub profile: &'a cost::StorageCostProfile,
}
