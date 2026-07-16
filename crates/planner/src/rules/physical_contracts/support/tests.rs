use super::cardinality::StreamRowUpperBound;
use super::*;
use crate::{cost, ir, logical, properties};

#[test]
fn cardinality_helpers_bound_rows_without_inventing_unknown_upper_bounds() {
    let delivered = with_cardinality(properties::DeliveredProperties::default(), Some(8));

    assert_eq!(
        estimated_pipeline_rows(&delivered, cost::EstimatedRows::rows(100)).as_rows(),
        8
    );
    assert_eq!(
        estimated_rows_bounded_by(
            cost::EstimatedRows::rows(100),
            StreamRowUpperBound::known(7),
        )
        .as_rows(),
        7
    );
    assert_eq!(
        stream_bound_upper(&ir::StreamBoundPlan::Literal(5)),
        StreamRowUpperBound::Known(5)
    );
    assert_eq!(
        stream_bound_upper(&ir::StreamBoundPlan::Expr(
            ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("limit")).unwrap()
        )),
        StreamRowUpperBound::Unknown
    );
}

#[test]
fn delivered_helpers_preserve_barriers_and_terminal_shapes() {
    let barrier = properties::DeliveredProperties {
        effect: properties::EffectKind::Barrier,
        ..properties::DeliveredProperties::default()
    };
    let expanded = super::delivered::access_expand_delivered_properties(&ir::ExpandPlan {
        direction: ir::ExpandDirection::Out,
        output: ir::ExpandOutput::Edges,
        label: ir::ExpandLabelPlan::Any,
    });

    assert_eq!(
        super::delivered::preserve_barrier_effect(barrier, expanded).effect,
        properties::EffectKind::Barrier
    );
    assert_eq!(
        project_output_delivered(
            access_delivered(properties::ElementKind::Node),
            &ir::ProjectionPlan::Count,
        )
        .cardinality,
        properties::CardinalityBounds::exact(1)
    );
    assert_eq!(
        reserved_output_delivered(
            properties::DeliveredProperties::default(),
            &ir::ReservedOp::Fold,
        )
        .cardinality
        .upper(),
        Some(1)
    );
}

#[test]
fn access_window_contracts_emit_minimal_stream_ops() {
    let storage = cost::StorageCostProfile::default();
    let delivered = access_delivered(properties::ElementKind::Node);

    let (effect, identity, cost) = access_window_stream_contract(
        delivered.clone(),
        logical::AccessWindowRange::new(0, None).unwrap(),
        cost::EstimatedRows::rows(10),
        &storage,
    );
    assert!(matches!(
        effect,
        super::window::AccessWindowPhysicalEffect::Identity
    ));
    assert_eq!(identity, delivered);
    assert_eq!(cost, cost::CostVector::ZERO);

    let (effect, ranged, _) = access_window_stream_contract(
        with_cardinality(delivered, Some(10)),
        logical::AccessWindowRange::new(2, Some(5)).unwrap(),
        cost::EstimatedRows::rows(10),
        &storage,
    );
    assert!(matches!(
        effect,
        super::window::AccessWindowPhysicalEffect::Op(crate::physical::PhysicalPipelineOp::Stream(
            crate::physical::PhysicalStreamOp::Range
        ))
    ));
    assert_eq!(ranged.cardinality.upper(), Some(3));
}

#[test]
fn physical_pipeline_builders_preserve_required_non_empty_boundaries() {
    let filter = crate::physical::PhysicalPipelineOp::ResidualFilter;
    let sort = crate::physical::PhysicalPipelineOp::Sort;
    let project =
        crate::physical::PhysicalPipelineOp::Stream(crate::physical::PhysicalStreamOp::Project);
    let aggregate =
        crate::physical::PhysicalPipelineOp::Stream(crate::physical::PhysicalStreamOp::Aggregate);

    let tail_only = physical_pipeline_from_prefix_and_required_tail(Vec::new(), sort.clone());
    assert_eq!(tail_only.ops(), std::slice::from_ref(&sort));

    let with_prefix =
        physical_pipeline_from_prefix_and_required_tail(vec![filter.clone()], sort.clone());
    assert_eq!(with_prefix.ops(), &[filter.clone(), sort]);

    let suffix = ir::AtLeast::<_, 1>::from_one_and_rest(project.clone(), vec![aggregate.clone()]);
    let with_suffix =
        physical_pipeline_from_prefix_and_required_suffix(vec![filter.clone()], suffix);
    assert_eq!(with_suffix.ops(), &[filter, project, aggregate]);
}

#[test]
fn stream_pipeline_contract_tracks_limit_sort_and_variable_write_effects() {
    let storage = cost::StorageCostProfile::default();
    let delivered = access_delivered(properties::ElementKind::Node);

    let (_, limited, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(4),
        },
        delivered.clone(),
        cost::EstimatedRows::rows(100),
        &storage,
    );
    assert_eq!(limited.cardinality.upper(), Some(4));

    let (_, ordered, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::Order {
            ordering: ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one(ir::OrderKey {
                property: ir::NonEmptyString::new("age").unwrap(),
                order: helix_ast::traversal::Order::Asc,
            }))
            .unwrap(),
        },
        delivered.clone(),
        cost::EstimatedRows::rows(100),
        &storage,
    );
    assert!(matches!(
        ordered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        ordered.materialization,
        properties::Materialization::Materialized
    );

    let (_, write, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::VariableWrite {
            op: logical::StreamVariableWriteOp::Store(ir::NonEmptyString::new("rows").unwrap()),
        },
        delivered,
        cost::EstimatedRows::rows(100),
        &storage,
    );
    assert_eq!(write.effect, properties::EffectKind::Barrier);
}

#[test]
fn stream_pipeline_contract_preserves_literal_window_lower_bounds() {
    let storage = cost::StorageCostProfile::default();
    let delivered = properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::new(3, Some(10)).unwrap(),
        ..properties::DeliveredProperties::default()
    };

    let (_, limited, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(4),
        },
        delivered.clone(),
        cost::EstimatedRows::rows(100),
        &storage,
    );
    assert_eq!(
        limited.cardinality,
        properties::CardinalityBounds::new(3, Some(4)).unwrap()
    );

    let (_, skipped, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::Skip {
            count: ir::StreamBoundPlan::Literal(2),
        },
        delivered.clone(),
        cost::EstimatedRows::rows(100),
        &storage,
    );
    assert_eq!(
        skipped.cardinality,
        properties::CardinalityBounds::new(1, Some(8)).unwrap()
    );

    let (_, ranged, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::Range {
            range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(2, 5).unwrap()),
        },
        delivered,
        cost::EstimatedRows::rows(100),
        &storage,
    );
    assert_eq!(
        ranged.cardinality,
        properties::CardinalityBounds::new(1, Some(3)).unwrap()
    );
}
