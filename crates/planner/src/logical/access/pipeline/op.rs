//! Stream-pipeline operator payloads.
//!
//! Pipeline operators carry executable payloads directly. That keeps composed
//! streams concrete and makes state-writing variable barriers explicit instead
//! of hiding them in payload-free physical nodes.

use serde::{Deserialize, Serialize};

use crate::logical::variables::{PureStreamVariableOp, StreamVariableWriteOp};
use crate::{ir, properties};

use super::super::AccessWindowRange;

/// Stream operator inside an access-backed or root-backed pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamPipelineOp {
    /// Residual predicate filter.
    Filter {
        /// Predicate.
        predicate: ir::PredicatePlan,
    },
    /// Composed static stream window.
    Window {
        /// Window.
        window: AccessWindowRange,
    },
    /// Dynamic or uncomposed stream limit.
    Limit {
        /// Bound.
        count: ir::StreamBoundPlan,
    },
    /// Dynamic or uncomposed stream skip.
    Skip {
        /// Bound.
        count: ir::StreamBoundPlan,
    },
    /// Dynamic or uncomposed stream range.
    Range {
        /// Range.
        range: ir::StreamRangePlan,
    },
    /// Explicit ordering request.
    Order {
        /// Required order.
        ordering: ir::OrderKeys,
    },
    /// Graph expansion.
    Expand {
        /// Expansion plan.
        plan: ir::ExpandPlan,
    },
    /// Side-effect-free stream variable operation.
    Variable {
        /// Variable operation.
        op: PureStreamVariableOp,
    },
    /// State-writing stream variable operation.
    VariableWrite {
        /// Variable operation.
        op: StreamVariableWriteOp,
    },
    /// Stream deduplication.
    Distinct,
}

/// Stream-pipeline operator family used by fine-grained rule scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamPipelineOpKind {
    /// `StreamPipelineOp::Filter`.
    Filter,
    /// `StreamPipelineOp::Window`.
    Window,
    /// `StreamPipelineOp::Limit`.
    Limit,
    /// `StreamPipelineOp::Skip`.
    Skip,
    /// `StreamPipelineOp::Range`.
    Range,
    /// `StreamPipelineOp::Order`.
    Order,
    /// `StreamPipelineOp::Expand`.
    Expand,
    /// `StreamPipelineOp::Variable`.
    Variable,
    /// `StreamPipelineOp::VariableWrite`.
    VariableWrite,
    /// `StreamPipelineOp::Distinct`.
    Distinct,
}

impl StreamPipelineOpKind {
    /// All stream-pipeline operator families.
    pub const ALL: [Self; 10] = [
        Self::Filter,
        Self::Window,
        Self::Limit,
        Self::Skip,
        Self::Range,
        Self::Order,
        Self::Expand,
        Self::Variable,
        Self::VariableWrite,
        Self::Distinct,
    ];
}

impl StreamPipelineOp {
    /// Return this stream-pipeline operator family.
    pub const fn kind(&self) -> StreamPipelineOpKind {
        match self {
            Self::Filter { .. } => StreamPipelineOpKind::Filter,
            Self::Window { .. } => StreamPipelineOpKind::Window,
            Self::Limit { .. } => StreamPipelineOpKind::Limit,
            Self::Skip { .. } => StreamPipelineOpKind::Skip,
            Self::Range { .. } => StreamPipelineOpKind::Range,
            Self::Order { .. } => StreamPipelineOpKind::Order,
            Self::Expand { .. } => StreamPipelineOpKind::Expand,
            Self::Variable { .. } => StreamPipelineOpKind::Variable,
            Self::VariableWrite { .. } => StreamPipelineOpKind::VariableWrite,
            Self::Distinct => StreamPipelineOpKind::Distinct,
        }
    }

    /// Effect introduced by this pipeline operator.
    pub const fn effect(&self) -> properties::EffectKind {
        match self {
            Self::VariableWrite { .. } => properties::EffectKind::Barrier,
            Self::Filter { .. }
            | Self::Window { .. }
            | Self::Limit { .. }
            | Self::Skip { .. }
            | Self::Range { .. }
            | Self::Order { .. }
            | Self::Expand { .. }
            | Self::Variable { .. }
            | Self::Distinct => properties::EffectKind::Pure,
        }
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Predicate;

    use super::*;

    fn variable(name: &'static str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(name).unwrap()
    }

    #[test]
    fn kind_classifies_every_stream_pipeline_variant() {
        let variants = [
            StreamPipelineOp::Filter {
                predicate: ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
            },
            StreamPipelineOp::Window {
                window: AccessWindowRange::new(1, Some(3)).unwrap(),
            },
            StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(2),
            },
            StreamPipelineOp::Skip {
                count: ir::StreamBoundPlan::Literal(2),
            },
            StreamPipelineOp::Range {
                range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 3).unwrap()),
            },
            StreamPipelineOp::Order {
                ordering: ir::OrderKeys::from(ir::OrderKey {
                    property: variable("age"),
                    order: helix_ast::traversal::Order::Asc,
                }),
            },
            StreamPipelineOp::Expand {
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    output: ir::ExpandOutput::Nodes,
                    label: ir::ExpandLabelPlan::Any,
                },
            },
            StreamPipelineOp::Variable {
                op: PureStreamVariableOp::Bind(variable("row")),
            },
            StreamPipelineOp::VariableWrite {
                op: StreamVariableWriteOp::Store(variable("rows")),
            },
            StreamPipelineOp::Distinct,
        ];

        let kinds = variants.map(|op| op.kind());

        assert_eq!(kinds, StreamPipelineOpKind::ALL);
    }

    #[test]
    fn only_variable_write_is_a_pipeline_barrier() {
        let pure_ops = [
            StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            },
            StreamPipelineOp::Variable {
                op: PureStreamVariableOp::Bind(variable("row")),
            },
            StreamPipelineOp::Distinct,
        ];

        assert!(pure_ops
            .iter()
            .all(|op| op.effect() == properties::EffectKind::Pure));
        assert_eq!(
            StreamPipelineOp::VariableWrite {
                op: StreamVariableWriteOp::As(variable("rows")),
            }
            .effect(),
            properties::EffectKind::Barrier
        );
    }
}
