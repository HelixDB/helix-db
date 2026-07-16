//! Access-backed stream-pipeline operators and invariants.

use serde::{Deserialize, Serialize};

use super::AccessPath;
use crate::{ir, properties};

mod candidates;
mod op;
mod validation;

pub use op::{StreamPipelineOp, StreamPipelineOpKind};
pub(in crate::logical) use validation::{
    combine_effect, pipeline_ops_effect, validate_stream_pipeline_ops,
};

/// Non-empty stream pipeline over a residual-free access path.
///
/// ```
/// use helix_ast::expr::Predicate;
/// use helix_planner::ir::{AtLeast, NodeAccessPlan, NodeAccessSourcePlan, PredicatePlan};
/// use helix_planner::logical::{
///     AccessPath, AccessPipeline, StreamPipelineOp, NodeAccessPath,
/// };
///
/// let access = AccessPath::Node(NodeAccessPath::new(
///     NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap(),
/// ));
/// let predicate = PredicatePlan::new(Predicate::eq("active", true)).unwrap();
/// let pipeline = AccessPipeline::new(
///     access,
///     AtLeast::<_, 1>::from_one(StreamPipelineOp::Filter { predicate }),
/// )
/// .unwrap();
///
/// assert_eq!(pipeline.ops().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessPipeline {
    access: AccessPath,
    ops: ir::AtLeast<StreamPipelineOp, 1>,
}

impl AccessPipeline {
    /// Build a canonical non-empty access-backed pipeline.
    ///
    /// Identity windows and adjacent uncomposed windows are rejected so the
    /// physical implementation can map every logical pipeline operator to one
    /// concrete executable operator.
    pub fn new(access: AccessPath, ops: ir::AtLeast<StreamPipelineOp, 1>) -> Option<Self> {
        validate_stream_pipeline_ops(ops.as_ref())?;
        Some(Self { access, ops })
    }

    /// Residual-free access path at the start of the pipeline.
    pub const fn access(&self) -> &AccessPath {
        &self.access
    }

    /// Pipeline operators in execution order.
    pub fn ops(&self) -> &[StreamPipelineOp] {
        self.ops.as_ref()
    }

    /// First pipeline-operator family.
    pub fn head_op_kind(&self) -> StreamPipelineOpKind {
        self.ops.as_ref()[0].kind()
    }

    /// True when local access-pipeline simplification should inspect this
    /// pipeline.
    ///
    /// This predicate is conservative: `true` does not guarantee that the
    /// simplification rule will rewrite the pipeline, but `false` means the
    /// local rule cannot rewrite it. Optimizer scheduling uses this to avoid
    /// calling whole-pipeline simplification for ordinary suffixes.
    ///
    /// ```
    /// use helix_planner::ir::{AtLeast, NodeAccessPlan, NodeAccessSourcePlan, StreamBoundPlan};
    /// use helix_planner::logical::{
    ///     AccessPath, AccessPipeline, NodeAccessPath, StreamPipelineOp,
    /// };
    ///
    /// let empty = AccessPath::Node(NodeAccessPath::new(
    ///     NodeAccessSourcePlan::new(NodeAccessPlan::Empty).unwrap(),
    /// ));
    /// let pipeline = AccessPipeline::new(
    ///     empty,
    ///     AtLeast::<_, 1>::from_one(StreamPipelineOp::Limit {
    ///         count: StreamBoundPlan::Literal(1),
    ///     }),
    /// )
    /// .unwrap();
    ///
    /// assert!(pipeline.has_local_simplification_candidate());
    /// ```
    pub fn has_local_simplification_candidate(&self) -> bool {
        candidates::pipeline_has_local_simplification_candidate(self)
    }

    /// Effect introduced by the whole access pipeline.
    pub fn effect(&self) -> properties::EffectKind {
        pipeline_ops_effect(self.ops())
    }
}

#[cfg(test)]
mod tests {
    use crate::ir;
    use crate::logical::{AccessPath, NodeAccessPath};

    use super::*;

    fn access(source: ir::NodeAccessPlan) -> AccessPath {
        AccessPath::Node(NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(source),
        ))
    }

    #[test]
    fn access_pipeline_facade_preserves_storage_contracts() {
        let pipeline = AccessPipeline::new(
            access(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap();

        assert_eq!(pipeline.access(), &access(ir::NodeAccessPlan::AllScan));
        assert_eq!(pipeline.ops().len(), 1);
        assert_eq!(pipeline.head_op_kind(), StreamPipelineOpKind::Limit);
        assert_eq!(pipeline.effect(), properties::EffectKind::Pure);
    }
}
