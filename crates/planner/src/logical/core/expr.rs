//! Top-level logical expression and scheduler-family contracts.

use serde::{Deserialize, Serialize};

use super::barrier::BarrierLogicalOp;
use super::pure::PureLogicalOp;
use crate::logical::access::{
    AccessDistinct, AccessFilter, AccessOrder, AccessPath, AccessPipeline, AccessWindow,
};
use crate::logical::pure::{FilterChain, FilterPushdown, PurePipeline};
use crate::logical::root::{
    RootBranch, RootIndexDdl, RootMutation, RootPipeline, RootRepeat, RootShortestPath,
    StreamAggregate, StreamProject, StreamReserved, StreamVariableWrite,
};
use crate::logical::variables::VariableSource;
use crate::properties;

/// Logical expression phase.
///
/// Pure and barrier operations are separated so rules cannot accidentally
/// commute side-effecting work through pure relational rewrites.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalExpr {
    /// Side-effect-free expression.
    Pure(PureLogicalOp),
    /// Variable source injection with executable payload.
    VariableSource(VariableSource),
    /// Side-effect-free operator pipeline.
    PurePipeline(PurePipeline),
    /// Adjacent residual filters that can be merged into one predicate.
    FilterChain(FilterChain),
    /// A filter above a safe pure operator that may be transposed below it.
    FilterPushdown(FilterPushdown),
    /// Residual-free access path candidate.
    AccessPath(AccessPath),
    /// Residual filter applied directly to a residual-free access path.
    AccessFilter(AccessFilter),
    /// Static stream window applied directly to a residual-free access path.
    AccessWindow(AccessWindow),
    /// Required ordering applied directly to a residual-free access path.
    AccessOrder(AccessOrder),
    /// Distinct applied directly to a residual-free access path.
    AccessDistinct(AccessDistinct),
    /// Composed stream operators applied to a residual-free access path.
    AccessPipeline(AccessPipeline),
    /// Composed stream operators applied to a supported root stream.
    RootPipeline(RootPipeline),
    /// Reserved terminal applied directly to a supported root stream.
    StreamReserved(StreamReserved),
    /// Projection terminal applied directly to a supported root stream.
    StreamProject(StreamProject),
    /// Aggregation terminal applied directly to a supported root stream.
    StreamAggregate(StreamAggregate),
    /// State-writing variable terminal applied directly to a supported root stream.
    StreamVariableWrite(StreamVariableWrite),
    /// Root mutation with executable payload.
    RootMutation(RootMutation),
    /// Root index DDL with executable payload.
    RootIndexDdl(RootIndexDdl),
    /// Root branch control flow with executable payload.
    RootBranch(RootBranch),
    /// Root repeat control flow with executable payload.
    RootRepeat(RootRepeat),
    /// Root shortest-path query with executable payload.
    RootShortestPath(RootShortestPath),
    /// Barrier expression.
    Barrier(BarrierLogicalOp),
}

/// Top-level logical expression family used by optimizer rule scheduling.
///
/// This intentionally mirrors `LogicalExpr` at the variant level. Rules that
/// need finer matching, such as individual `PureLogicalOp` variants, keep those
/// checks inside their rule body so scheduler metadata remains stable as pure
/// operator payloads evolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalExprKind {
    /// `LogicalExpr::Pure`.
    Pure,
    /// `LogicalExpr::VariableSource`.
    VariableSource,
    /// `LogicalExpr::PurePipeline`.
    PurePipeline,
    /// `LogicalExpr::FilterChain`.
    FilterChain,
    /// `LogicalExpr::FilterPushdown`.
    FilterPushdown,
    /// `LogicalExpr::AccessPath`.
    AccessPath,
    /// `LogicalExpr::AccessFilter`.
    AccessFilter,
    /// `LogicalExpr::AccessWindow`.
    AccessWindow,
    /// `LogicalExpr::AccessOrder`.
    AccessOrder,
    /// `LogicalExpr::AccessDistinct`.
    AccessDistinct,
    /// `LogicalExpr::AccessPipeline`.
    AccessPipeline,
    /// `LogicalExpr::RootPipeline`.
    RootPipeline,
    /// `LogicalExpr::StreamReserved`.
    StreamReserved,
    /// `LogicalExpr::StreamProject`.
    StreamProject,
    /// `LogicalExpr::StreamAggregate`.
    StreamAggregate,
    /// `LogicalExpr::StreamVariableWrite`.
    StreamVariableWrite,
    /// `LogicalExpr::RootMutation`.
    RootMutation,
    /// `LogicalExpr::RootIndexDdl`.
    RootIndexDdl,
    /// `LogicalExpr::RootBranch`.
    RootBranch,
    /// `LogicalExpr::RootRepeat`.
    RootRepeat,
    /// `LogicalExpr::RootShortestPath`.
    RootShortestPath,
    /// `LogicalExpr::Barrier`.
    Barrier,
}

impl LogicalExprKind {
    /// All top-level logical expression families.
    pub const ALL: [Self; 22] = [
        Self::Pure,
        Self::VariableSource,
        Self::PurePipeline,
        Self::FilterChain,
        Self::FilterPushdown,
        Self::AccessPath,
        Self::AccessFilter,
        Self::AccessWindow,
        Self::AccessOrder,
        Self::AccessDistinct,
        Self::AccessPipeline,
        Self::RootPipeline,
        Self::StreamReserved,
        Self::StreamProject,
        Self::StreamAggregate,
        Self::StreamVariableWrite,
        Self::RootMutation,
        Self::RootIndexDdl,
        Self::RootBranch,
        Self::RootRepeat,
        Self::RootShortestPath,
        Self::Barrier,
    ];
}

impl LogicalExpr {
    /// Return the top-level expression family.
    pub const fn kind(&self) -> LogicalExprKind {
        match self {
            Self::Pure(_) => LogicalExprKind::Pure,
            Self::VariableSource(_) => LogicalExprKind::VariableSource,
            Self::PurePipeline(_) => LogicalExprKind::PurePipeline,
            Self::FilterChain(_) => LogicalExprKind::FilterChain,
            Self::FilterPushdown(_) => LogicalExprKind::FilterPushdown,
            Self::AccessPath(_) => LogicalExprKind::AccessPath,
            Self::AccessFilter(_) => LogicalExprKind::AccessFilter,
            Self::AccessWindow(_) => LogicalExprKind::AccessWindow,
            Self::AccessOrder(_) => LogicalExprKind::AccessOrder,
            Self::AccessDistinct(_) => LogicalExprKind::AccessDistinct,
            Self::AccessPipeline(_) => LogicalExprKind::AccessPipeline,
            Self::RootPipeline(_) => LogicalExprKind::RootPipeline,
            Self::StreamReserved(_) => LogicalExprKind::StreamReserved,
            Self::StreamProject(_) => LogicalExprKind::StreamProject,
            Self::StreamAggregate(_) => LogicalExprKind::StreamAggregate,
            Self::StreamVariableWrite(_) => LogicalExprKind::StreamVariableWrite,
            Self::RootMutation(_) => LogicalExprKind::RootMutation,
            Self::RootIndexDdl(_) => LogicalExprKind::RootIndexDdl,
            Self::RootBranch(_) => LogicalExprKind::RootBranch,
            Self::RootRepeat(_) => LogicalExprKind::RootRepeat,
            Self::RootShortestPath(_) => LogicalExprKind::RootShortestPath,
            Self::Barrier(_) => LogicalExprKind::Barrier,
        }
    }

    /// Return the expression effect kind.
    pub fn effect(&self) -> properties::EffectKind {
        match self {
            Self::Pure(_)
            | Self::VariableSource(_)
            | Self::PurePipeline(_)
            | Self::FilterChain(_)
            | Self::FilterPushdown(_)
            | Self::AccessPath(_)
            | Self::AccessFilter(_)
            | Self::AccessWindow(_)
            | Self::AccessOrder(_)
            | Self::AccessDistinct(_) => properties::EffectKind::Pure,
            Self::AccessPipeline(pipeline) => pipeline.effect(),
            Self::RootPipeline(pipeline) => pipeline.effect(),
            Self::StreamReserved(reserved) => reserved.effect(),
            Self::StreamProject(project) => project.effect(),
            Self::StreamAggregate(aggregate) => aggregate.effect(),
            Self::StreamVariableWrite(_)
            | Self::RootMutation(_)
            | Self::RootIndexDdl(_)
            | Self::RootBranch(_)
            | Self::RootRepeat(_)
            | Self::RootShortestPath(_)
            | Self::Barrier(_) => properties::EffectKind::Barrier,
        }
    }
}
