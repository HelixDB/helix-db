use crate::{ir, logical, properties};

use super::super::support::{
    access_delivered, empty_delivered, required_to_delivered_ordering, with_cardinality,
};

pub(super) fn delivered_after_pipeline_op(
    delivered: properties::DeliveredProperties,
    op: &logical::PureLogicalOp,
) -> properties::DeliveredProperties {
    match op {
        logical::PureLogicalOp::NoOp => delivered,
        logical::PureLogicalOp::Empty => empty_delivered(),
        logical::PureLogicalOp::Source { element } => access_delivered(*element),
        logical::PureLogicalOp::Filter { .. } => {
            let upper = delivered.cardinality.upper();
            with_cardinality(delivered, upper)
        }
        logical::PureLogicalOp::Limit { count } => properties::DeliveredProperties {
            cardinality: match count {
                ir::StreamBoundPlan::Literal(count) => delivered.cardinality.after_limit(*count),
                ir::StreamBoundPlan::Expr(_) => delivered.cardinality,
            },
            ..delivered
        },
        logical::PureLogicalOp::Skip { count } => properties::DeliveredProperties {
            cardinality: match count {
                ir::StreamBoundPlan::Literal(count) => delivered.cardinality.after_skip(*count),
                ir::StreamBoundPlan::Expr(_) => delivered.cardinality,
            },
            ..delivered
        },
        logical::PureLogicalOp::Range { range } => properties::DeliveredProperties {
            cardinality: match range {
                ir::StreamRangePlan::Literal(range) => delivered
                    .cardinality
                    .after_range(range.start()..range.end()),
                ir::StreamRangePlan::Dynamic(_) => delivered.cardinality,
            },
            ..delivered
        },
        logical::PureLogicalOp::Distinct => {
            let upper = delivered.cardinality.upper();
            properties::DeliveredProperties {
                cardinality: properties::CardinalityBounds::zero_to(upper),
                materialization: properties::Materialization::Materialized,
                ..delivered
            }
        }
        logical::PureLogicalOp::Order { ordering } => properties::DeliveredProperties {
            ordering: required_to_delivered_ordering(ordering.clone()),
            materialization: properties::Materialization::Materialized,
            ..delivered
        },
        logical::PureLogicalOp::Expand { element } => properties::DeliveredProperties {
            element: Some(*element),
            ordering: properties::DeliveredOrdering::Unordered,
            key_locality: properties::KeyLocality::Unknown,
            ..delivered
        },
        logical::PureLogicalOp::Project | logical::PureLogicalOp::Aggregate => {
            let materialization = if matches!(op, logical::PureLogicalOp::Aggregate) {
                properties::Materialization::Materialized
            } else {
                delivered.materialization
            };
            properties::DeliveredProperties {
                element: None,
                materialization,
                ..delivered
            }
        }
        logical::PureLogicalOp::Variable | logical::PureLogicalOp::Reserved => delivered,
    }
}
