use crate::{exec, ir, logical, properties};

use super::cardinality::{with_cardinality, StreamRowUpperBound};

pub(in crate::rules) const fn element_keyspace(
    element: properties::ElementKind,
) -> exec::ElementKeyspace {
    match element {
        properties::ElementKind::Node => exec::ElementKeyspace::NodeProperty,
        properties::ElementKind::Edge => exec::ElementKeyspace::EdgeEndpoints,
    }
}

pub(in crate::rules) fn access_delivered(
    element: properties::ElementKind,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        element: Some(element),
        key_locality: properties::KeyLocality::Unknown,
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn filtered_delivered() -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::zero_to(None),
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn empty_delivered() -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::exact(0),
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn ordered_delivered(
    ordering: properties::RequiredOrdering,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        ordering: match ordering {
            properties::RequiredOrdering::Any => properties::DeliveredOrdering::Unordered,
            properties::RequiredOrdering::ByKeys(keys) => {
                properties::DeliveredOrdering::ByKeys(keys)
            }
        },
        materialization: properties::Materialization::Materialized,
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn barrier_delivered(
    op: &logical::BarrierLogicalOp,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: if matches!(op, logical::BarrierLogicalOp::IndexDdl) {
            properties::CardinalityBounds::exact(1)
        } else {
            properties::CardinalityBounds::unknown()
        },
        materialization: properties::Materialization::Materialized,
        effect: properties::EffectKind::Barrier,
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn preserve_barrier_effect(
    previous: properties::DeliveredProperties,
    mut next: properties::DeliveredProperties,
) -> properties::DeliveredProperties {
    if matches!(previous.effect, properties::EffectKind::Barrier) {
        next.effect = properties::EffectKind::Barrier;
    }
    next
}

pub(in crate::rules) fn stream_variable_delivered_properties(
    delivered: properties::DeliveredProperties,
    op: &logical::PureStreamVariableOp,
) -> properties::DeliveredProperties {
    if op.preserves_cardinality() {
        delivered
    } else if op.preserves_upper_bound() {
        let upper = delivered.cardinality.upper();
        with_cardinality(delivered, upper)
    } else {
        properties::DeliveredProperties {
            element: delivered.element,
            ..properties::DeliveredProperties::default()
        }
    }
}

pub(in crate::rules) fn access_expand_delivered_properties(
    plan: &ir::ExpandPlan,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        element: Some(match plan.output {
            ir::ExpandOutput::Nodes => properties::ElementKind::Node,
            ir::ExpandOutput::Edges => properties::ElementKind::Edge,
        }),
        key_locality: match plan.label {
            ir::ExpandLabelPlan::Any => properties::KeyLocality::Unknown,
            ir::ExpandLabelPlan::Label(_) => properties::KeyLocality::Close,
        },
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn project_output_delivered(
    input: properties::DeliveredProperties,
    projection: &ir::ProjectionPlan,
) -> properties::DeliveredProperties {
    match projection {
        ir::ProjectionPlan::Count | ir::ProjectionPlan::Exists => properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::exact(1),
            materialization: properties::Materialization::Materialized,
            effect: input.effect,
            ..properties::DeliveredProperties::default()
        },
        ir::ProjectionPlan::Id
        | ir::ProjectionPlan::Label
        | ir::ProjectionPlan::Values(_)
        | ir::ProjectionPlan::ValueMap(_)
        | ir::ProjectionPlan::Project(_)
        | ir::ProjectionPlan::ProjectBindings { .. }
        | ir::ProjectionPlan::EdgeProperties => properties::DeliveredProperties {
            element: None,
            ..input
        },
    }
}

pub(in crate::rules) fn aggregate_output_delivered(
    input: properties::DeliveredProperties,
    _aggregate: &ir::AggregatePlan,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        element: None,
        materialization: properties::Materialization::Materialized,
        effect: input.effect,
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn stream_variable_write_delivered_properties(
    delivered: properties::DeliveredProperties,
    _op: &logical::StreamVariableWriteOp,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        effect: properties::EffectKind::Barrier,
        ..delivered
    }
}

pub(in crate::rules) fn reserved_output_delivered(
    delivered: properties::DeliveredProperties,
    op: &ir::ReservedOp,
) -> properties::DeliveredProperties {
    match op {
        ir::ReservedOp::Fold => properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::zero_to(Some(1)),
            materialization: properties::Materialization::Materialized,
            ..delivered
        },
        ir::ReservedOp::Unfold
        | ir::ReservedOp::Path
        | ir::ReservedOp::SimplePath
        | ir::ReservedOp::WithSack(_)
        | ir::ReservedOp::SackSet(_)
        | ir::ReservedOp::SackAdd(_)
        | ir::ReservedOp::SackGet => delivered,
    }
}

pub(in crate::rules) fn required_to_delivered_ordering(
    ordering: properties::RequiredOrdering,
) -> properties::DeliveredOrdering {
    match ordering {
        properties::RequiredOrdering::Any => properties::DeliveredOrdering::Unordered,
        properties::RequiredOrdering::ByKeys(keys) => properties::DeliveredOrdering::ByKeys(keys),
    }
}

pub(in crate::rules) fn materialized_delivered() -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        materialization: properties::Materialization::Materialized,
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::rules) fn bounded_delivered(
    upper: StreamRowUpperBound,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::zero_to(upper.to_cardinality_upper()),
        ..properties::DeliveredProperties::default()
    }
}
