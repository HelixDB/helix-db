//! Stream operator scheduling contracts.

use crate::exec::ExecSchedule;
use crate::ir;

pub(in crate::exec) fn reserved_schedule(op: &ir::ReservedOp) -> ExecSchedule {
    match op {
        ir::ReservedOp::Fold => ExecSchedule::Barrier,
        ir::ReservedOp::Unfold
        | ir::ReservedOp::Path
        | ir::ReservedOp::SimplePath
        | ir::ReservedOp::WithSack(_)
        | ir::ReservedOp::SackSet(_)
        | ir::ReservedOp::SackAdd(_)
        | ir::ReservedOp::SackGet => ExecSchedule::Pipeline,
    }
}

pub(in crate::exec) fn project_schedule(projection: &ir::ProjectionPlan) -> ExecSchedule {
    match projection {
        ir::ProjectionPlan::Exists
        | ir::ProjectionPlan::ProjectBindings {
            dedup: ir::ProjectionDedupMode::Distinct,
            ..
        } => ExecSchedule::Barrier,
        ir::ProjectionPlan::Id
        | ir::ProjectionPlan::Label
        | ir::ProjectionPlan::Values(_)
        | ir::ProjectionPlan::ValueMap(_)
        | ir::ProjectionPlan::Project(_)
        | ir::ProjectionPlan::ProjectBindings {
            dedup: ir::ProjectionDedupMode::All,
            ..
        }
        | ir::ProjectionPlan::EdgeProperties => ExecSchedule::Pipeline,
    }
}
