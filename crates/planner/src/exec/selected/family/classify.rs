//! Logical/physical selected alternative classification.

use super::{SelectedExecutableAlternativeClassification, SelectedExecutableAlternativeFamily};
use crate::{logical, physical, properties};

impl SelectedExecutableAlternativeFamily {
    /// Classify a logical/physical pair as an ordinary selected executable
    /// alternative family.
    pub(crate) fn classify(
        source_expr: &logical::LogicalExpr,
        physical_expr: &physical::PhysicalExpr,
    ) -> SelectedExecutableAlternativeClassification {
        let family = match (source_expr, physical_expr) {
            (
                logical::LogicalExpr::AccessPath(logical::AccessPath::Node(_)),
                physical::PhysicalExpr::Access {
                    element: properties::ElementKind::Node,
                    ..
                },
            ) => Self::NODE_ACCESS_PATH,
            (
                logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(_)),
                physical::PhysicalExpr::Access {
                    element: properties::ElementKind::Edge,
                    ..
                },
            ) => Self::EDGE_ACCESS_PATH,
            (
                logical::LogicalExpr::Pure(logical::PureLogicalOp::Source { element }),
                physical::PhysicalExpr::Access {
                    element: physical_element,
                    access: physical::PhysicalAccess::Kv(_),
                },
            ) if element == physical_element => Self::KV_SOURCE,
            (
                logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
                physical::PhysicalExpr::NoOp,
            ) => Self::NO_OP,
            (
                logical::LogicalExpr::VariableSource(_),
                physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
            ) => Self::VARIABLE_SOURCE,
            (logical::LogicalExpr::AccessFilter(_), physical::PhysicalExpr::Pipeline(_)) => {
                Self::ACCESS_FILTER_PIPELINE
            }
            (logical::LogicalExpr::AccessWindow(_), physical::PhysicalExpr::Pipeline(_)) => {
                Self::ACCESS_WINDOW_PIPELINE
            }
            (logical::LogicalExpr::AccessOrder(_), physical::PhysicalExpr::Pipeline(_)) => {
                Self::ACCESS_ORDER_PIPELINE
            }
            (logical::LogicalExpr::AccessDistinct(_), physical::PhysicalExpr::Pipeline(_)) => {
                Self::ACCESS_DISTINCT_PIPELINE
            }
            (logical::LogicalExpr::AccessPipeline(_), physical::PhysicalExpr::Pipeline(_)) => {
                Self::ACCESS_PIPELINE
            }
            _ => return SelectedExecutableAlternativeClassification::Unsupported,
        };
        SelectedExecutableAlternativeClassification::Classified(family)
    }

    /// Classify a logical/physical pair, returning a typed construction error
    /// when the pair cannot cross the selected executable alternative boundary.
    pub(crate) fn try_classify(
        source_expr: &logical::LogicalExpr,
        physical_expr: &physical::PhysicalExpr,
    ) -> Result<Self, super::super::SelectedAlternativeConstructionError> {
        Self::classify(source_expr, physical_expr).into_result()
    }
}
