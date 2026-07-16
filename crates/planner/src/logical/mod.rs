use crate::ir;
#[cfg(test)]
use crate::properties;

mod access;
mod core;
mod memo_children;
mod pure;
mod root;
mod variables;

pub use self::access::{
    AccessDistinct, AccessFilter, AccessOrder, AccessPath, AccessPipeline, AccessSourceKind,
    AccessStream, AccessWindow, AccessWindowRange, EdgeAccessPath, NodeAccessPath,
    StreamPipelineOp, StreamPipelineOpKind,
};
pub use self::core::{
    BarrierLogicalOp, LogicalExpr, LogicalExprKind, PureLogicalOp, PureLogicalOpKind,
};
pub use self::pure::{FilterChain, FilterPushdown, FilterPushdownOp, PurePipeline};
pub use self::root::{
    RootBranch, RootIndexDdl, RootMutation, RootPipeline, RootRepeat, RootShortestPath, RootStream,
    StreamAggregate, StreamProject, StreamReserved, StreamVariableWrite,
};
pub use self::variables::{PureStreamVariableOp, StreamVariableWriteOp, VariableSource};

#[cfg(test)]
mod tests;
