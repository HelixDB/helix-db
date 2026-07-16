//! Delivered properties for selected stream variable operators.

use super::super::*;

pub(in crate::exec) fn selected_stream_variable_delivered_properties(
    delivered: properties::DeliveredProperties,
    op: &logical::PureStreamVariableOp,
) -> properties::DeliveredProperties {
    if op.preserves_cardinality() {
        delivered
    } else if op.preserves_upper_bound() {
        let upper = delivered.cardinality.upper();
        filtered_delivered_properties(properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::zero_to(upper),
            ..delivered
        })
    } else {
        properties::DeliveredProperties {
            element: delivered.element,
            ..properties::DeliveredProperties::default()
        }
    }
}

pub(in crate::exec) fn selected_stream_variable_write_delivered_properties(
    delivered: properties::DeliveredProperties,
    _op: &logical::StreamVariableWriteOp,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        effect: properties::EffectKind::Barrier,
        ..delivered
    }
}
