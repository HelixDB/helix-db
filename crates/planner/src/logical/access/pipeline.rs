//! Access-backed stream-pipeline operators and invariants.

use serde::{Deserialize, Serialize};

use super::AccessPath;
use crate::{ir, properties};

mod candidates;
mod canonical;
mod op;
mod validation;

pub(crate) use canonical::canonicalize_stream_pipeline_ops;
pub(in crate::logical) use canonical::CanonicalStreamPipelineOps;
pub use op::{StreamPipelineOp, StreamPipelineOpKind};
pub(in crate::logical) use validation::{combine_effect, pipeline_ops_effect};

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
    ops: CanonicalStreamPipelineOps,
}

impl AccessPipeline {
    /// Build a canonical non-empty access-backed pipeline.
    ///
    /// Identity windows and adjacent uncomposed windows are rejected so the
    /// physical implementation can map every logical pipeline operator to one
    /// concrete executable operator.
    pub fn new(access: AccessPath, ops: ir::AtLeast<StreamPipelineOp, 1>) -> Option<Self> {
        let ops = CanonicalStreamPipelineOps::new(ops)?;
        Some(Self { access, ops })
    }

    /// Residual-free access path at the start of the pipeline.
    pub const fn access(&self) -> &AccessPath {
        &self.access
    }

    /// Pipeline operators in execution order.
    pub fn ops(&self) -> &[StreamPipelineOp] {
        self.ops.as_slice()
    }

    /// Typed pipeline operators preserving the non-empty invariant.
    pub const fn ops_at_least(&self) -> &ir::AtLeast<StreamPipelineOp, 1> {
        self.ops.as_at_least()
    }

    /// First pipeline-operator family.
    pub fn head_op_kind(&self) -> StreamPipelineOpKind {
        self.ops.as_slice()[0].kind()
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
    use helix_ast::expr::Predicate;

    use crate::ir;
    use crate::logical::{AccessPath, NodeAccessPath};

    use super::*;

    fn access(source: ir::NodeAccessPlan) -> AccessPath {
        AccessPath::Node(NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(source),
        ))
    }

    fn filter(property: &str) -> StreamPipelineOp {
        StreamPipelineOp::Filter {
            predicate: ir::PredicatePlan::new(Predicate::eq(property, true)).unwrap(),
        }
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

    #[test]
    fn access_pipeline_canonicalizes_each_adjacent_filter_run() {
        let pipeline = AccessPipeline::new(
            access(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                StreamPipelineOp::Filter {
                    predicate: ir::PredicatePlan::new(Predicate::and(vec![
                        Predicate::eq("first", true),
                        Predicate::eq("second", true),
                    ]))
                    .unwrap(),
                },
                vec![
                    filter("third"),
                    StreamPipelineOp::Limit {
                        count: ir::StreamBoundPlan::Literal(10),
                    },
                    filter("fourth"),
                    filter("fifth"),
                ],
            ),
        )
        .unwrap();

        assert!(matches!(
            pipeline.ops(),
            [
                StreamPipelineOp::Filter { predicate: first },
                StreamPipelineOp::Limit { .. },
                StreamPipelineOp::Filter { predicate: second },
            ] if first.as_ref() == &Predicate::and(vec![
                Predicate::eq("first", true),
                Predicate::eq("second", true),
                Predicate::eq("third", true),
            ]) && second.as_ref() == &Predicate::and(vec![
                Predicate::eq("fourth", true),
                Predicate::eq("fifth", true),
            ])
        ));
    }

    #[test]
    fn access_pipeline_deserialization_enforces_canonical_ops() {
        #[derive(serde::Serialize)]
        struct RawAccessPipeline {
            access: AccessPath,
            ops: ir::AtLeast<StreamPipelineOp, 1>,
        }

        let encoded = serde_json::to_value(RawAccessPipeline {
            access: access(ir::NodeAccessPlan::AllScan),
            ops: ir::AtLeast::<_, 1>::from_one_and_rest(filter("first"), vec![filter("second")]),
        })
        .unwrap();
        let pipeline: AccessPipeline = serde_json::from_value(encoded).unwrap();
        assert!(matches!(
            pipeline.ops(),
            [StreamPipelineOp::Filter { predicate }]
                if predicate.as_ref() == &Predicate::and(vec![
                    Predicate::eq("first", true),
                    Predicate::eq("second", true),
                ])
        ));

        let mut encoded = serde_json::to_value(RawAccessPipeline {
            access: access(ir::NodeAccessPlan::AllScan),
            ops: ir::AtLeast::<_, 1>::from_one(filter("first")),
        })
        .unwrap();
        encoded["ops"] = serde_json::json!([]);
        assert!(serde_json::from_value::<AccessPipeline>(encoded).is_err());

        let encoded = serde_json::to_value(RawAccessPipeline {
            access: access(ir::NodeAccessPlan::AllScan),
            ops: ir::AtLeast::<_, 1>::from_one(StreamPipelineOp::Window {
                window: crate::logical::AccessWindowRange::identity(),
            }),
        })
        .unwrap();
        assert!(serde_json::from_value::<AccessPipeline>(encoded).is_err());
    }
}
