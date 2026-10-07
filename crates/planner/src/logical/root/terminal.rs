//! Terminal contracts over supported root streams.
//!
//! Terminals carry their executable payloads directly, so selected lowering
//! never has to infer semantics from generic physical stream operators.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::{RootPipeline, RootStream};
use crate::logical::{AccessPipeline, AccessStream, StreamVariableWriteOp};
use crate::properties;
use crate::{context, ir};

/// Cardinality terminal over a supported root stream.
///
/// This is distinct from projection because cardinality has its own logical,
/// physical, and executable optimization families.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamCardinality {
    input: RootStream,
    /// The request's bindings, which every expression of one planning
    /// session shares. They are not part of the expression's identity: memo
    /// and selected-root digests serialize every explored expression, so
    /// serializing megabytes of parameters per expression dominated planning
    /// for parameter-heavy requests. A deserialized expression carries no
    /// bindings and plans with none.
    #[serde(skip)]
    params: context::SharedParamBindings,
    late_bound_params: BTreeSet<ir::NonEmptyString>,
}

impl StreamCardinality {
    /// Build a cardinality terminal.
    ///
    /// A counted access pipeline that saves its stream (`.as()`, `.store()`)
    /// splits at the first variable write: the operators before it stay with
    /// the access, so its filters are still index-served, and the write and
    /// everything after it run over that stream.
    pub fn new(input: RootStream) -> Self {
        let input = match input {
            RootStream::Access(AccessStream::Pipeline(pipeline))
                if pipeline.effect() == properties::EffectKind::Barrier =>
            {
                let barrier = pipeline
                    .ops()
                    .iter()
                    .position(|op| op.effect() == properties::EffectKind::Barrier)
                    .expect("a barrier pipeline holds a barrier operator");
                let (prefix, rest) = pipeline.ops().split_at(barrier);
                let prefix = ir::AtLeast::<_, 1>::try_from_vec(prefix.to_vec())
                    .and_then(|prefix| AccessPipeline::new(pipeline.access().clone(), prefix));
                let (input, ops) = match prefix {
                    Some(prefix) => (
                        RootStream::Access(AccessStream::Pipeline(prefix)),
                        ir::AtLeast::try_from_vec(rest.to_vec())
                            .expect("the barrier operator starts the rest"),
                    ),
                    None => (
                        RootStream::Access(AccessStream::Path(pipeline.access().clone())),
                        pipeline.ops_at_least().clone(),
                    ),
                };
                RootStream::Pipeline(Box::new(
                    RootPipeline::new(input, ops)
                        .expect("a validated access pipeline is a valid root pipeline"),
                ))
            }
            input => input,
        };
        Self {
            input,
            params: context::SharedParamBindings::default(),
            late_bound_params: BTreeSet::new(),
        }
    }

    /// Record runtime scopes whose object fields can replace immutable request
    /// bindings while this cardinality terminal executes.
    pub fn with_planning_bindings(
        mut self,
        params: impl Into<context::SharedParamBindings>,
        late_bound_params: BTreeSet<ir::NonEmptyString>,
    ) -> Self {
        self.params = params.into();
        self.late_bound_params = late_bound_params;
        self
    }

    /// Root stream consumed by the terminal.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Immutable request bindings available for planning-time specialization.
    pub fn params(&self) -> &context::ParamBindings {
        &self.params
    }

    /// The same bindings as [`Self::params`], for rewrites that carry them
    /// to a new expression without copying them.
    pub const fn shared_params(&self) -> &context::SharedParamBindings {
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
