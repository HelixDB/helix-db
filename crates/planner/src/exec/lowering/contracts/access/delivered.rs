//! Access delivered-property assembly.

use super::{cardinality, locality, ordering};
use crate::{ir, properties};

pub(in crate::exec) fn element_point_delivered_properties(
    element: properties::ElementKind,
    count: usize,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        element: Some(element),
        cardinality: properties::CardinalityBounds::exact(count),
        key_locality: properties::KeyLocality::Close,
        ..properties::DeliveredProperties::default()
    }
}

pub(in crate::exec) fn node_access_delivered_properties(
    plan: &ir::NodeAccessPlan,
) -> properties::DeliveredProperties {
    access_delivered_properties(
        properties::ElementKind::Node,
        cardinality::node_access_hard_upper_bound(plan),
        cardinality::node_access_exact_cardinality(plan),
        ordering::range_ordering_from_node_access(plan),
        locality::access_key_locality_from_node_access(plan),
    )
}

pub(in crate::exec) fn edge_access_delivered_properties(
    plan: &ir::EdgeAccessPlan,
) -> properties::DeliveredProperties {
    access_delivered_properties(
        properties::ElementKind::Edge,
        cardinality::edge_access_hard_upper_bound(plan),
        cardinality::edge_access_exact_cardinality(plan),
        ordering::range_ordering_from_edge_access(plan),
        locality::access_key_locality_from_edge_access(plan),
    )
}

fn access_delivered_properties(
    element: properties::ElementKind,
    hard_upper_bound: Option<usize>,
    exact: Option<usize>,
    ordering: properties::DeliveredOrdering,
    key_locality: properties::KeyLocality,
) -> properties::DeliveredProperties {
    let cardinality = exact.map_or_else(
        || properties::CardinalityBounds::zero_to(hard_upper_bound),
        properties::CardinalityBounds::exact,
    );
    properties::DeliveredProperties {
        element: Some(element),
        ordering,
        cardinality,
        key_locality,
        ..properties::DeliveredProperties::default()
    }
}
