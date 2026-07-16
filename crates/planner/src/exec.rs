#[cfg(test)]
use crate::{catalog, cost, ir, logical, physical, properties, trace};

mod access;
mod error;
mod ids;
mod kv;
mod lowering;
mod metrics;
mod op;
mod order;
mod plan;
pub mod selected;
mod validation;

pub use self::access::*;
pub use self::error::ExecPlanError;
pub use self::ids::*;
pub use self::kv::*;
#[cfg(test)]
pub(in crate::exec) use self::lowering::{
    aggregate_delivered_properties, project_delivered_properties,
};
pub(in crate::exec) use self::lowering::{
    edge_access_cost, edge_access_delivered_properties, edge_access_hard_upper_bound,
    edge_exec_access, element_point_delivered_properties, expand_delivered_properties,
    filtered_delivered_properties, followup_exec_condition, foreach_subplan_cost,
    initial_exec_condition, limit_delivered_properties, materialized_delivered_properties,
    node_access_cost, node_access_delivered_properties, node_access_hard_upper_bound,
    node_exec_access, ordered_delivered_properties, predicate_cost_for_rows,
    preserve_barrier_effect, project_schedule, range_delivered_properties, reserved_schedule,
    skip_delivered_properties, stream_bound_literal, stream_range_literal_bounds,
    ExecutableDagBuilder, SimpleEdgeAccessLeaf, SimpleNodeAccessLeaf, StepDraft,
};
pub use self::metrics::PlannerMetrics;
pub use self::op::*;
pub use self::order::*;
pub use self::plan::{ExecutablePlan, ExecutableSubplan};
#[cfg(test)]
pub(in crate::exec) use self::selected::lowering::SelectedExecutableRejectionReason;
pub use self::selected::*;

#[cfg(test)]
mod tests;
