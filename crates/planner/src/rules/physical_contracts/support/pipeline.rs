use crate::{context, cost, ir, logical, physical, properties};

use super::cardinality::{
    estimated_rows_bounded_by, stream_bound_upper, stream_range_upper, with_cardinality,
};
use super::delivered::{
    access_expand_delivered_properties, preserve_barrier_effect,
    stream_variable_delivered_properties, stream_variable_write_delivered_properties,
};
use super::window::access_window_stream_contract;

pub(in crate::rules) fn physical_pipeline_from_first_and_rest(
    first: physical::PhysicalPipelineOp,
    rest: Vec<physical::PhysicalPipelineOp>,
) -> physical::PhysicalPipeline {
    physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(first, rest))
}

pub(in crate::rules) fn physical_pipeline_from_prefix_and_required_tail(
    prefix: Vec<physical::PhysicalPipelineOp>,
    tail: physical::PhysicalPipelineOp,
) -> physical::PhysicalPipeline {
    let mut prefix = prefix.into_iter();
    match prefix.next() {
        Some(first) => {
            let mut rest = prefix.collect::<Vec<_>>();
            rest.push(tail);
            physical_pipeline_from_first_and_rest(first, rest)
        }
        None => physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(tail)),
    }
}

pub(in crate::rules) fn physical_pipeline_from_prefix_and_required_suffix(
    prefix: Vec<physical::PhysicalPipelineOp>,
    suffix: ir::AtLeast<physical::PhysicalPipelineOp, 1>,
) -> physical::PhysicalPipeline {
    let (suffix_first, suffix_rest) = suffix.into_first_and_rest();
    let mut prefix = prefix.into_iter();
    match prefix.next() {
        Some(first) => {
            let mut rest = prefix.collect::<Vec<_>>();
            rest.push(suffix_first);
            rest.extend(suffix_rest);
            physical_pipeline_from_first_and_rest(first, rest)
        }
        None => physical_pipeline_from_first_and_rest(suffix_first, suffix_rest),
    }
}

pub(in crate::rules) fn stream_pipeline_op_contract(
    op: &logical::StreamPipelineOp,
    delivered: properties::DeliveredProperties,
    rows: cost::EstimatedRows,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    physical::PhysicalPipelineOp,
    properties::DeliveredProperties,
    cost::CostVector,
) {
    match op {
        logical::StreamPipelineOp::Filter { predicate } => {
            let upper = delivered.cardinality.upper();
            (
                physical::PhysicalPipelineOp::ResidualFilter,
                with_cardinality(delivered, upper),
                storage.residual_filter(predicate.as_ref(), rows),
            )
        }
        logical::StreamPipelineOp::IndexMembership { plan } => {
            let upper = delivered.cardinality.upper();
            let (set, label_domain, matches) = membership_set_cost(plan, storage, stats);
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::IndexMembership),
                with_cardinality(delivered, upper),
                storage.index_membership_filter(
                    set,
                    label_domain.map(|(read, label_rows)| cost::MembershipLabelDomain {
                        read,
                        label_rows,
                        predicate: plan.predicate().as_ref(),
                    }),
                    plan.residual().map(AsRef::as_ref),
                    rows,
                    matches,
                ),
            )
        }
        logical::StreamPipelineOp::Window { window } => {
            let (effect, delivered, cost) =
                access_window_stream_contract(delivered, *window, rows, storage);
            (effect.into_pipeline_op(), delivered, cost)
        }
        logical::StreamPipelineOp::Limit { count } => {
            let cardinality = match count {
                ir::StreamBoundPlan::Literal(count) => delivered.cardinality.after_limit(*count),
                ir::StreamBoundPlan::Expr(_) => delivered.cardinality,
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
                properties::DeliveredProperties {
                    cardinality,
                    ..delivered
                },
                storage.stream_operator(estimated_rows_bounded_by(rows, stream_bound_upper(count))),
            )
        }
        logical::StreamPipelineOp::Skip { count } => {
            let cardinality = match count {
                ir::StreamBoundPlan::Literal(count) => delivered.cardinality.after_skip(*count),
                ir::StreamBoundPlan::Expr(_) => delivered.cardinality,
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Skip),
                properties::DeliveredProperties {
                    cardinality,
                    ..delivered
                },
                storage.stream_operator(rows),
            )
        }
        logical::StreamPipelineOp::Range { range } => {
            let cardinality = match range {
                ir::StreamRangePlan::Literal(range) => delivered
                    .cardinality
                    .after_range(range.start()..range.end()),
                ir::StreamRangePlan::Dynamic(_) => delivered.cardinality,
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Range),
                properties::DeliveredProperties {
                    cardinality,
                    ..delivered
                },
                storage.stream_operator(estimated_rows_bounded_by(rows, stream_range_upper(range))),
            )
        }
        logical::StreamPipelineOp::Order { ordering } => (
            physical::PhysicalPipelineOp::Sort,
            properties::DeliveredProperties {
                ordering: properties::DeliveredOrdering::ByKeys(ordering.clone()),
                materialization: properties::Materialization::Materialized,
                ..delivered
            },
            storage.property_sort(rows),
        ),
        logical::StreamPipelineOp::Expand { plan } => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Expand),
            preserve_barrier_effect(delivered, access_expand_delivered_properties(plan)),
            storage.stream_operator(rows),
        ),
        logical::StreamPipelineOp::VectorSearch { plan } => {
            let upper = match plan.as_ref() {
                ir::RestrictedVectorSearchPlan::Nodes { k, .. }
                | ir::RestrictedVectorSearchPlan::Edges { k, .. } => match k {
                    ir::SearchLimitPlan::Literal(k) => Some(k.get()),
                    ir::SearchLimitPlan::Expr(_) => None,
                },
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::VectorSearch),
                properties::DeliveredProperties {
                    cardinality: properties::CardinalityBounds::zero_to(
                        match (delivered.cardinality.upper(), upper) {
                            (Some(delivered), Some(search)) => Some(delivered.min(search)),
                            (delivered, search) => delivered.or(search),
                        },
                    ),
                    materialization: properties::Materialization::Materialized,
                    ordering: properties::DeliveredOrdering::ByKeys(
                        ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one_and_rest(
                            ir::OrderKey {
                                property: ir::NonEmptyString::new("$distance")
                                    .expect("distance virtual property is non-empty"),
                                order: helix_ast::traversal::Order::Asc,
                            },
                            vec![ir::OrderKey {
                                property: ir::NonEmptyString::new("$id")
                                    .expect("ID virtual property is non-empty"),
                                order: helix_ast::traversal::Order::Asc,
                            }],
                        ))
                        .expect("distance and ID ordering keys are unique"),
                    ),
                    effect: properties::EffectKind::OrderSensitive,
                    key_locality: properties::KeyLocality::Unknown,
                    ..delivered
                },
                storage.explicit_sort(rows),
            )
        }
        logical::StreamPipelineOp::TextSearch { plan } => {
            let upper = match plan.as_ref() {
                ir::RestrictedTextSearchPlan::Nodes { k, .. }
                | ir::RestrictedTextSearchPlan::Edges { k, .. } => match k {
                    ir::SearchLimitPlan::Literal(k) => Some(k.get()),
                    ir::SearchLimitPlan::Expr(_) => None,
                },
            };
            (
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::TextSearch),
                properties::DeliveredProperties {
                    cardinality: properties::CardinalityBounds::zero_to(
                        match (delivered.cardinality.upper(), upper) {
                            (Some(delivered), Some(search)) => Some(delivered.min(search)),
                            (delivered, search) => delivered.or(search),
                        },
                    ),
                    materialization: properties::Materialization::Materialized,
                    ordering: properties::DeliveredOrdering::ByKeys(
                        ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one_and_rest(
                            ir::OrderKey {
                                property: ir::NonEmptyString::new("$score")
                                    .expect("score virtual property is non-empty"),
                                order: helix_ast::traversal::Order::Desc,
                            },
                            vec![ir::OrderKey {
                                property: ir::NonEmptyString::new("$id")
                                    .expect("ID virtual property is non-empty"),
                                order: helix_ast::traversal::Order::Asc,
                            }],
                        ))
                        .expect("score and ID ordering keys are unique"),
                    ),
                    effect: properties::EffectKind::OrderSensitive,
                    key_locality: properties::KeyLocality::Unknown,
                    ..delivered
                },
                storage.explicit_sort(rows),
            )
        }
        logical::StreamPipelineOp::Variable { op } => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            stream_variable_delivered_properties(delivered, op),
            storage.stream_operator(rows),
        ),
        logical::StreamPipelineOp::VariableWrite { op } => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Variable),
            stream_variable_write_delivered_properties(delivered, op),
            storage.stream_operator(rows),
        ),
        logical::StreamPipelineOp::Distinct => (
            physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
            properties::DeliveredProperties {
                cardinality: properties::CardinalityBounds::zero_to(delivered.cardinality.upper()),
                materialization: properties::Materialization::Materialized,
                ..delivered
            },
            storage.explicit_sort(rows),
        ),
    }
}

/// What a membership reads before it probes rows: the set read, the
/// label-domain bitmap read alongside it with the label's nodes when
/// outside-label nodes need one, and the rows the set is estimated to hold.
///
/// An index set is priced by its secondary-ID reads, exactly like the same
/// set as a source. A `$label` set reads one bitmap per label concurrently.
pub(in crate::rules) fn membership_set_cost(
    plan: &ir::NodeIndexMembershipPlan,
    storage: &cost::StorageCostProfile,
    stats: &context::StatsSnapshot,
) -> (
    cost::CostVector,
    Option<(cost::CostVector, cost::EstimatedRows)>,
    cost::EstimatedRows,
) {
    match plan.set() {
        ir::NodeMembershipSet::Index {
            set,
            label,
            outside_label,
        } => {
            let contract = super::super::access::access_path_contract(
                &logical::AccessPath::Node(logical::NodeAccessPath::new(set.clone())),
                storage,
                stats,
            );
            (
                contract.secondary_id_cost().unwrap_or(contract.cost),
                membership_label_domain_cost(*outside_label, label, stats, storage),
                contract.estimated_rows,
            )
        }
        ir::NodeMembershipSet::Labels(labels) => {
            let (set, matches) = membership_labels_cost(labels, stats, storage);
            (set, None, matches)
        }
    }
}

/// Concurrent `$label` bitmap reads of a label membership set, and the nodes
/// those labels hold.
pub(in crate::rules) fn membership_labels_cost(
    labels: &ir::AtLeast<ir::NonEmptyString, 1>,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> (cost::CostVector, cost::EstimatedRows) {
    let rows = labels
        .iter()
        .map(|label| node_label_rows(label, stats, storage))
        .collect::<Vec<_>>();
    (
        storage.parallel(
            &rows
                .iter()
                .map(|rows| storage.bitmap_equality_lookup(*rows))
                .collect::<Vec<_>>(),
            storage.max_parallel_kv_reads,
        ),
        cost::EstimatedRows::rows(
            rows.iter()
                .map(|rows| rows.as_rows())
                .fold(0, u64::saturating_add),
        ),
    )
}

/// Label bitmap read needed to drop label nodes outside the set, and the
/// nodes the label holds.
///
/// Label-scoped predicates reject other labels without the bitmap.
pub(in crate::rules) fn membership_label_domain_cost(
    outside_label: ir::NodeMembershipOutsideLabel,
    label: &ir::NonEmptyString,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> Option<(cost::CostVector, cost::EstimatedRows)> {
    match outside_label {
        ir::NodeMembershipOutsideLabel::Reject => None,
        ir::NodeMembershipOutsideLabel::Evaluate => {
            let rows = node_label_rows(label, stats, storage);
            Some((storage.bitmap_equality_lookup(rows), rows))
        }
    }
}

fn node_label_rows(
    label: &ir::NonEmptyString,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    stats
        .node_label_cardinality
        .get(label)
        .copied()
        .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows)
}

pub(in crate::rules) fn access_pipeline_op(
    access_path: &logical::AccessPath,
    access: physical::PhysicalAccess,
) -> physical::PhysicalPipelineOp {
    physical::PhysicalPipelineOp::Access {
        element: access_path.element(),
        access,
    }
}
