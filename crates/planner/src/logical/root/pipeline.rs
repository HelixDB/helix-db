//! Non-empty pipelines over supported root streams.
//!
//! Root pipelines carry executable stream payloads over a `RootStream`, so later
//! terminals can consume the whole pipeline without reconstructing semantics
//! from payload-free physical operators.

use serde::{Deserialize, Serialize};

use super::RootStream;
use crate::ir;
use crate::logical::access::{combine_effect, pipeline_ops_effect, validate_stream_pipeline_ops};
use crate::logical::StreamPipelineOp;
use crate::properties;

/// Non-empty stream pipeline over a supported root stream.
///
/// ```
/// use helix_planner::ir::{AtLeast, NonEmptyString};
/// use helix_planner::logical::{
///     PureStreamVariableOp, RootPipeline, RootStream, StreamPipelineOp, VariableSource,
/// };
///
/// let pipeline = RootPipeline::new(
///     RootStream::VariableSource(VariableSource::new(
///         NonEmptyString::new("seed").unwrap(),
///     )),
///     AtLeast::<_, 1>::from_one(StreamPipelineOp::Variable {
///         op: PureStreamVariableOp::Select(NonEmptyString::new("cached").unwrap()),
///     }),
/// )
/// .unwrap();
///
/// assert_eq!(pipeline.ops().len(), 1);
/// assert_eq!(pipeline.ops_at_least().as_ref().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RootPipeline {
    input: RootStream,
    ops: ir::AtLeast<StreamPipelineOp, 1>,
}

impl RootPipeline {
    /// Build a canonical non-empty root-stream pipeline.
    pub fn new(input: RootStream, ops: ir::AtLeast<StreamPipelineOp, 1>) -> Option<Self> {
        validate_stream_pipeline_ops(ops.as_ref())?;
        Some(Self { input, ops })
    }

    /// Root stream consumed by the pipeline.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Pipeline operators in execution order.
    pub fn ops(&self) -> &[StreamPipelineOp] {
        self.ops.as_ref()
    }

    /// Typed pipeline operators preserving the non-empty invariant.
    pub const fn ops_at_least(&self) -> &ir::AtLeast<StreamPipelineOp, 1> {
        &self.ops
    }

    /// Effect introduced by the whole root pipeline.
    pub fn effect(&self) -> properties::EffectKind {
        combine_effect(self.input.effect(), pipeline_ops_effect(self.ops()))
    }
}
