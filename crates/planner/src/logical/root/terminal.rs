//! Terminal contracts over supported root streams.
//!
//! Terminals carry their executable payloads directly, so selected lowering
//! never has to infer semantics from generic physical stream operators.

use serde::{Deserialize, Serialize};

use super::RootStream;
use crate::ir;
use crate::logical::StreamVariableWriteOp;
use crate::properties;

/// Reserved terminal over a supported root stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamReserved {
    input: RootStream,
    op: ir::ReservedOp,
}

impl StreamReserved {
    /// Build a reserved terminal over a supported root stream.
    pub fn new(input: RootStream, op: ir::ReservedOp) -> Self {
        Self { input, op }
    }

    /// Root stream consumed by the terminal.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Reserved operation payload.
    pub const fn op(&self) -> &ir::ReservedOp {
        &self.op
    }

    /// Effect introduced by the reserved stream.
    pub fn effect(&self) -> properties::EffectKind {
        self.input.effect()
    }
}

/// Projection terminal over a supported root stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamProject {
    input: RootStream,
    projection: ir::ProjectionPlan,
}

impl StreamProject {
    /// Build a projection terminal over a supported root stream.
    pub fn new(input: RootStream, projection: ir::ProjectionPlan) -> Self {
        Self { input, projection }
    }

    /// Root stream consumed by the terminal.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Projection payload.
    pub const fn projection(&self) -> &ir::ProjectionPlan {
        &self.projection
    }

    /// Effect introduced by the projected stream.
    pub fn effect(&self) -> properties::EffectKind {
        self.input.effect()
    }
}

/// Aggregation terminal over a supported root stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamAggregate {
    input: RootStream,
    aggregate: ir::AggregatePlan,
}

impl StreamAggregate {
    /// Build an aggregation terminal over a supported root stream.
    pub fn new(input: RootStream, aggregate: ir::AggregatePlan) -> Self {
        Self { input, aggregate }
    }

    /// Root stream consumed by the terminal.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Aggregation payload.
    pub const fn aggregate(&self) -> &ir::AggregatePlan {
        &self.aggregate
    }

    /// Effect introduced by the aggregated stream.
    pub fn effect(&self) -> properties::EffectKind {
        self.input.effect()
    }
}

/// State-writing variable terminal over a supported root stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamVariableWrite {
    input: RootStream,
    op: StreamVariableWriteOp,
}

impl StreamVariableWrite {
    /// Build a variable-write terminal over a supported root stream.
    pub fn new(input: RootStream, op: StreamVariableWriteOp) -> Self {
        Self { input, op }
    }

    /// Root stream consumed by the terminal.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// State-writing variable operation.
    pub const fn op(&self) -> &StreamVariableWriteOp {
        &self.op
    }
}
