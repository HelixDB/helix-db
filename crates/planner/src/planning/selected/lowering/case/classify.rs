//! Pairwise selected logical/physical root classification.

use super::super::super::rejection;
use super::super::terminal::TerminalRootPayload;
use super::mismatch::selected_root_physical_mismatch;
use super::SelectedRootPlanCase;
use crate::{error, exec, logical, physical};

impl<'a> SelectedRootPlanCase<'a> {
    pub(in crate::planning::selected::lowering) fn classify(
        source_expr: &'a logical::LogicalExpr,
        physical_expr: &physical::PhysicalExpr,
    ) -> Result<Self, error::PlannerError> {
        match (source_expr, physical_expr) {
            (logical::LogicalExpr::RootIndexDdl(ddl), physical::PhysicalExpr::Barrier) => {
                Ok(Self::IndexDdl(ddl))
            }
            (logical::LogicalExpr::RootMutation(mutation), physical::PhysicalExpr::Barrier) => {
                Ok(Self::Mutation(mutation))
            }
            (
                logical::LogicalExpr::RootBranch(branch),
                physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch),
            ) => Ok(Self::Branch(branch)),
            (
                logical::LogicalExpr::RootRepeat(repeat),
                physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat),
            ) => Ok(Self::Repeat(repeat)),
            (
                logical::LogicalExpr::RootShortestPath(path),
                physical::PhysicalExpr::ShortestPath,
            ) => Ok(Self::ShortestPath(path)),
            (logical::LogicalExpr::RootPipeline(pipeline), physical::PhysicalExpr::Pipeline(_)) => {
                Ok(Self::Pipeline(pipeline))
            }
            (logical::LogicalExpr::StreamProject(project), physical::PhysicalExpr::Pipeline(_)) => {
                Ok(Self::Terminal(TerminalRootPayload::Project(project)))
            }
            (
                logical::LogicalExpr::StreamAggregate(aggregate),
                physical::PhysicalExpr::Pipeline(_),
            ) => Ok(Self::Terminal(TerminalRootPayload::Aggregate(aggregate))),
            (
                logical::LogicalExpr::StreamReserved(reserved),
                physical::PhysicalExpr::Pipeline(_),
            ) => Ok(Self::Terminal(TerminalRootPayload::Reserved(reserved))),
            (
                logical::LogicalExpr::StreamVariableWrite(write),
                physical::PhysicalExpr::Pipeline(_),
            ) => Ok(Self::Terminal(TerminalRootPayload::VariableWrite(write))),
            (
                logical::LogicalExpr::StreamCardinality(cardinality),
                physical::PhysicalExpr::Cardinality(count),
            ) => Ok(Self::Count(cardinality, count.clone())),
            _ if selected_root_physical_mismatch(source_expr, physical_expr) => Err(
                rejection::unsupported(rejection::Reason::SelectedRootPhysicalMismatch),
            ),
            _ => exec::selected_executable_alternative_family(source_expr, physical_expr)
                .map(Self::GenericAlternative)
                .map_err(rejection::unsupported_alternative_construction),
        }
    }
}
