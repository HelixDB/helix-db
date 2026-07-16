//! Pure selected-lowering contract helpers.
//!
//! This module has no executable-DAG state. It validates that selected physical
//! alternatives match their logical contracts, derives delivered properties, and
//! builds small lowering drafts whose costs stay tunable through
//! `StorageCostProfile`.

use super::super::super::{
    edge_access_delivered_properties, edge_access_hard_upper_bound, expand_delivered_properties,
    filtered_delivered_properties, limit_delivered_properties, materialized_delivered_properties,
    node_access_delivered_properties, node_access_hard_upper_bound, ordered_delivered_properties,
    preserve_barrier_effect, range_delivered_properties, skip_delivered_properties,
    stream_bound_literal, stream_range_literal_bounds, ExecCondition, ExecMutationPlan, ExecOp,
    ExecPlanError, ExecSchedule, ExecStepId, StepDraft,
};
use super::super::*;
use super::rejection;
use crate::{cost, ir, logical, physical, properties};

mod delivered;
mod draft;
mod estimate;
mod matching;

#[cfg(test)]
pub(in crate::exec) use delivered::selected_stream_reserved_delivered_properties;
pub(in crate::exec::selected::lowering) use delivered::{
    selected_access_path_delivered_properties, selected_root_stream_input_delivered_properties,
    selected_stream_pipeline_delivered_properties,
};
pub(in crate::exec) use delivered::{
    selected_stream_variable_delivered_properties,
    selected_stream_variable_write_delivered_properties,
};
pub(in crate::exec::selected::lowering) use draft::{
    selected_access_window_step_draft, selected_mutation_step_draft,
};
pub(in crate::exec::selected::lowering) use estimate::{
    estimated_rows_bounded_by, selected_access_path_estimated_rows,
    selected_access_path_hard_upper_bound, selected_rows_for_delivered,
};
pub(in crate::exec::selected::lowering) use matching::{
    selected_access_filter_pipeline_access, selected_access_pipeline_parts,
    selected_access_window_pipeline_matches, selected_edge_access_matches,
    selected_node_access_matches, selected_pipeline_from_ops, selected_stream_pipeline_ops_match,
    SelectedAccessFilterPipelineMatch, SelectedAccessPipelineMatch,
};

pub(super) fn unsupported_selected_alternative(reason: rejection::Reason) -> ExecPlanError {
    rejection::unsupported(reason)
}

pub(super) fn selected_control_schedule(
    delivered: &properties::DeliveredProperties,
) -> ExecSchedule {
    if delivered.effect == properties::EffectKind::Barrier {
        ExecSchedule::Barrier
    } else {
        ExecSchedule::Pipeline
    }
}
