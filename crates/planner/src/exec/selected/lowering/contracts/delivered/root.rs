//! Delivered properties for selected root-stream inputs and reserved terminals.

use super::super::*;
use super::access;

pub(in crate::exec::selected::lowering) fn selected_root_stream_input_delivered_properties(
    input: &SelectedRootStreamInput,
) -> properties::DeliveredProperties {
    match input {
        SelectedRootStreamInput::Access(access) => {
            access::selected_access_stream_delivered_properties(access)
        }
        SelectedRootStreamInput::VariableSource(_) => properties::DeliveredProperties::default(),
        SelectedRootStreamInput::Mutation(root) => root.alternative().delivered().clone(),
        SelectedRootStreamInput::Branch(root) => root.alternative().delivered().clone(),
        SelectedRootStreamInput::Repeat(root) => root.alternative().delivered().clone(),
        SelectedRootStreamInput::Pipeline(root) => root.alternative().delivered().clone(),
        SelectedRootStreamInput::Terminal(root) => root.alternative().delivered().clone(),
        SelectedRootStreamInput::Count(root) => root.alternative().delivered().clone(),
    }
}

#[cfg(test)]
pub(in crate::exec) fn selected_stream_reserved_delivered_properties(
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
