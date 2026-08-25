//! Terminal contracts over supported root streams.
//!
//! Terminals carry their executable payloads directly, so selected lowering
//! never has to infer semantics from generic physical stream operators.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::{RootPipeline, RootStream};
use crate::logical::{AccessStream, StreamVariableWriteOp};
use crate::properties;
use crate::{context, ir};

/// Cardinality terminal over a supported root stream.
///
/// This is distinct from projection because cardinality has its own logical,
/// physical, and executable optimization families.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamCardinality {
    input: RootStream,
    params: context::ParamBindings,
    late_bound_params: BTreeSet<ir::NonEmptyString>,
}

impl StreamCardinality {
    /// Build a cardinality terminal.
    pub fn new(input: RootStream) -> Self {
        let input = match input {
            RootStream::Access(AccessStream::Pipeline(pipeline))
                if pipeline.effect() == properties::EffectKind::Barrier =>
            {
                let input = RootStream::Access(AccessStream::Path(pipeline.access().clone()));
                RootStream::Pipeline(Box::new(
                    RootPipeline::new(input, pipeline.ops_at_least().clone())
                        .expect("a validated access pipeline is a valid root pipeline"),
                ))
            }
            input => input,
        };
        Self {
            input,
            params: context::ParamBindings::default(),
            late_bound_params: BTreeSet::new(),
        }
    }

    /// Record runtime scopes whose object fields can replace immutable request
    /// bindings while this cardinality terminal executes.
    pub fn with_planning_bindings(
        mut self,
        params: context::ParamBindings,
        late_bound_params: BTreeSet<ir::NonEmptyString>,
    ) -> Self {
        self.params = params;
        self.late_bound_params = late_bound_params;
        self
    }

    /// Root stream consumed by the terminal.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Immutable request bindings available for planning-time specialization.
    pub const fn params(&self) -> &context::ParamBindings {
        &self.params
    }

    /// Active runtime parameter scopes visible at this terminal.
    pub const fn late_bound_params(&self) -> &BTreeSet<ir::NonEmptyString> {
        &self.late_bound_params
    }

    /// Effect inherited from the input.
    pub fn effect(&self) -> properties::EffectKind {
        self.input.effect()
    }
}

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
