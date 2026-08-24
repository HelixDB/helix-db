//! Non-empty pipelines over supported root streams.
//!
//! Root pipelines carry executable stream payloads over a `RootStream`, so later
//! terminals can consume the whole pipeline without reconstructing semantics
//! from payload-free physical operators.

use serde::{Deserialize, Serialize};

use super::RootStream;
use crate::ir;
use crate::logical::access::{combine_effect, pipeline_ops_effect, CanonicalStreamPipelineOps};
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
    ops: CanonicalStreamPipelineOps,
}

impl RootPipeline {
    /// Build a canonical non-empty root-stream pipeline.
    pub fn new(input: RootStream, ops: ir::AtLeast<StreamPipelineOp, 1>) -> Option<Self> {
        let ops = CanonicalStreamPipelineOps::new(ops)?;
        Some(Self { input, ops })
    }

    /// Root stream consumed by the pipeline.
    pub const fn input(&self) -> &RootStream {
        &self.input
    }

    /// Pipeline operators in execution order.
    pub fn ops(&self) -> &[StreamPipelineOp] {
        self.ops.as_slice()
    }

    /// Typed pipeline operators preserving the non-empty invariant.
    pub const fn ops_at_least(&self) -> &ir::AtLeast<StreamPipelineOp, 1> {
        self.ops.as_at_least()
    }

    /// Effect introduced by the whole root pipeline.
    pub fn effect(&self) -> properties::EffectKind {
        combine_effect(self.input.effect(), pipeline_ops_effect(self.ops()))
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Predicate;

    use super::*;
    use crate::logical::VariableSource;

    #[test]
    fn root_pipeline_uses_shared_filter_canonicalization() {
        let first = ir::PredicatePlan::new(Predicate::eq("first", true)).unwrap();
        let second = ir::PredicatePlan::new(Predicate::eq("second", true)).unwrap();
        let pipeline = RootPipeline::new(
            RootStream::VariableSource(VariableSource::new(
                ir::NonEmptyString::new("rows").unwrap(),
            )),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                StreamPipelineOp::Filter { predicate: first },
                vec![StreamPipelineOp::Filter { predicate: second }],
            ),
        )
        .unwrap();

        assert!(matches!(
            pipeline.ops(),
            [StreamPipelineOp::Filter { predicate }]
                if predicate.as_ref() == &Predicate::and(vec![
                    Predicate::eq("first", true),
                    Predicate::eq("second", true),
                ])
        ));
        assert_eq!(pipeline.ops_at_least().as_ref().len(), 1);
    }
}
