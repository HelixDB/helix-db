//! Physical optimizer contract facade.
//!
//! Physical alternatives are the rule-produced implementation shapes that sit
//! between logical memo expressions and executable DAG lowering. The facade
//! keeps the public `physical::*` contract stable while implementation details
//! live in focused ADT modules.

mod access;
mod alternative;
mod cardinality;
mod expr;
mod pipeline;
mod stream;

pub use self::{
    access::PhysicalAccess,
    alternative::PhysicalAlternative,
    cardinality::{PhysicalCardinality, PhysicalCountPlan},
    expr::PhysicalExpr,
    pipeline::{PhysicalPipeline, PhysicalPipelineOp, PhysicalPipelineTerminalSplit},
    stream::{PhysicalControlOp, PhysicalStreamOp},
};

#[cfg(test)]
mod tests;
