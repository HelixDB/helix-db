//! Selected root physical-family mismatch detection.

use crate::{logical, physical};

pub(super) fn selected_root_physical_mismatch(
    source_expr: &logical::LogicalExpr,
    physical_expr: &physical::PhysicalExpr,
) -> bool {
    match source_expr {
        logical::LogicalExpr::RootMutation(_) | logical::LogicalExpr::RootIndexDdl(_) => {
            !matches!(physical_expr, physical::PhysicalExpr::Barrier)
        }
        logical::LogicalExpr::RootBranch(_) => !matches!(
            physical_expr,
            physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch)
        ),
        logical::LogicalExpr::RootRepeat(_) => !matches!(
            physical_expr,
            physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat)
        ),
        logical::LogicalExpr::RootShortestPath(_) => {
            !matches!(physical_expr, physical::PhysicalExpr::ShortestPath)
        }
        logical::LogicalExpr::RootPipeline(_)
        | logical::LogicalExpr::StreamProject(_)
        | logical::LogicalExpr::StreamAggregate(_)
        | logical::LogicalExpr::StreamReserved(_)
        | logical::LogicalExpr::StreamVariableWrite(_) => {
            !matches!(physical_expr, physical::PhysicalExpr::Pipeline(_))
        }
        _ => false,
    }
}
