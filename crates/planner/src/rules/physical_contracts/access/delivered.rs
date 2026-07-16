use crate::{catalog, ir, properties};

pub(super) fn access_delivered_with(
    element: properties::ElementKind,
    cardinality: properties::CardinalityBounds,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        element: Some(element),
        cardinality,
        key_locality: properties::KeyLocality::Unknown,
        ..properties::DeliveredProperties::default()
    }
}

pub(super) fn access_delivered_close(
    element: properties::ElementKind,
) -> properties::DeliveredProperties {
    with_key_locality(
        super::super::support::access_delivered(element),
        properties::KeyLocality::Close,
    )
}

pub(super) fn with_key_locality(
    delivered: properties::DeliveredProperties,
    key_locality: properties::KeyLocality,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        key_locality,
        ..delivered
    }
}

pub(super) fn with_ordering(
    delivered: properties::DeliveredProperties,
    ordering: properties::DeliveredOrdering,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        ordering,
        ..delivered
    }
}

pub(super) fn range_delivered_ordering(
    key: &catalog::ScopedPropertyDirectionKey,
) -> properties::DeliveredOrdering {
    properties::DeliveredOrdering::ByKeys(ir::OrderKeys::from(ir::OrderKey {
        property: key.property.clone(),
        order: match key.direction {
            helix_ast::index::RangeIndexDirection::Asc => helix_ast::traversal::Order::Asc,
            helix_ast::index::RangeIndexDirection::Desc => helix_ast::traversal::Order::Desc,
        },
    }))
}
