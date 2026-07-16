//! Stream delivered-property transforms.

use crate::{ir, properties};

pub(in crate::exec) fn expand_delivered_properties(
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

pub(in crate::exec) fn filtered_delivered_properties(
    delivered: properties::DeliveredProperties,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::zero_to(delivered.cardinality.upper()),
        ..delivered
    }
}

pub(in crate::exec) fn preserve_barrier_effect(
    previous: properties::DeliveredProperties,
    mut next: properties::DeliveredProperties,
) -> properties::DeliveredProperties {
    if matches!(previous.effect, properties::EffectKind::Barrier) {
        next.effect = properties::EffectKind::Barrier;
    }
    next
}

pub(in crate::exec) fn limit_delivered_properties(
    delivered: properties::DeliveredProperties,
    literal_count: Option<usize>,
) -> properties::DeliveredProperties {
    let cardinality = literal_count.map_or(delivered.cardinality, |limit| {
        delivered.cardinality.after_limit(limit)
    });
    properties::DeliveredProperties {
        cardinality,
        ..delivered
    }
}

pub(in crate::exec) fn skip_delivered_properties(
    delivered: properties::DeliveredProperties,
    literal_count: Option<usize>,
) -> properties::DeliveredProperties {
    let cardinality = literal_count.map_or(delivered.cardinality, |skip| {
        delivered.cardinality.after_skip(skip)
    });
    properties::DeliveredProperties {
        cardinality,
        ..delivered
    }
}

pub(in crate::exec) fn range_delivered_properties(
    delivered: properties::DeliveredProperties,
    literal_bounds: Option<(usize, usize)>,
) -> properties::DeliveredProperties {
    let cardinality = literal_bounds.map_or(delivered.cardinality, |(start, end)| {
        delivered.cardinality.after_range(start..end)
    });
    properties::DeliveredProperties {
        cardinality,
        ..delivered
    }
}

pub(in crate::exec) fn materialized_delivered_properties(
    delivered: properties::DeliveredProperties,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        materialization: properties::Materialization::Materialized,
        ..delivered
    }
}

pub(in crate::exec) fn ordered_delivered_properties(
    delivered: properties::DeliveredProperties,
    ordering: properties::DeliveredOrdering,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        ordering,
        ..delivered
    }
}

#[cfg(test)]
pub(in crate::exec) fn project_delivered_properties(
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

#[cfg(test)]
pub(in crate::exec) fn aggregate_delivered_properties(
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
