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
            &ir::ProjectionPlan::Exists,
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
        &crate::context::StatsSnapshot::default(),
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
        &crate::context::StatsSnapshot::default(),
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
        &crate::context::StatsSnapshot::default(),
    );
    assert_eq!(write.effect, properties::EffectKind::Barrier);
}

#[test]
fn restricted_vector_contract_is_pure_order_sensitive_materialized_and_distance_ordered() {
    let storage = cost::StorageCostProfile::default();
    let (_, vector, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::VectorSearch {
            plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                key: crate::catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
                index: ir::SearchIndexPlan {
                    index_id: ir::NonEmptyString::new("idx").unwrap(),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                    vec![1.0_f32, 0.0],
                ))
                .unwrap(),
                k: ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::new(10).unwrap()),
            }),
        },
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::zero_to(Some(100)),
            ..access_delivered(properties::ElementKind::Node)
        },
        cost::EstimatedRows::rows(100),
        &storage,
        &crate::context::StatsSnapshot::default(),
    );

    assert_eq!(vector.cardinality.upper(), Some(10));
    assert_eq!(vector.effect, properties::EffectKind::OrderSensitive);
    assert_eq!(
        vector.materialization,
        properties::Materialization::Materialized
    );
    let properties::DeliveredOrdering::ByKeys(keys) = vector.ordering else {
        panic!("restricted vector search must establish distance ordering");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "$distance");
    assert_eq!(keys.as_ref()[1].property.as_ref(), "$id");
}

#[test]
fn restricted_text_contract_is_order_sensitive_materialized_and_score_ordered() {
    let storage = cost::StorageCostProfile::default();
    let (_, text, _) = stream_pipeline_op_contract(
        &logical::StreamPipelineOp::TextSearch {
            plan: Box::new(ir::RestrictedTextSearchPlan::Nodes {
                key: crate::catalog::NodeSearchIndexKey::try_new("Doc", "body").unwrap(),
                index: ir::SearchIndexPlan {
                    index_id: ir::NonEmptyString::new("idx").unwrap(),
                    tenant: ir::SearchTenantPlan::Unscoped,
                },
                query_text: ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
                    "needle",
                ))
                .unwrap(),
                k: ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::new(10).unwrap()),
            }),
        },
        properties::DeliveredProperties {
            cardinality: properties::CardinalityBounds::zero_to(Some(100)),
            ..access_delivered(properties::ElementKind::Node)
        },
        cost::EstimatedRows::rows(100),
        &storage,
        &crate::context::StatsSnapshot::default(),
    );

    assert_eq!(text.cardinality.upper(), Some(10));
    assert_eq!(text.effect, properties::EffectKind::OrderSensitive);
    assert_eq!(
        text.materialization,
        properties::Materialization::Materialized
    );
    let properties::DeliveredOrdering::ByKeys(keys) = text.ordering else {
        panic!("restricted text search must establish score ordering");
    };
    assert_eq!(keys.as_ref()[0].property.as_ref(), "$score");
    assert_eq!(keys.as_ref()[0].order, helix_ast::traversal::Order::Desc);
    assert_eq!(keys.as_ref()[1].property.as_ref(), "$id");
    assert_eq!(keys.as_ref()[1].order, helix_ast::traversal::Order::Asc);
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
        &crate::context::StatsSnapshot::default(),
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
        &crate::context::StatsSnapshot::default(),
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
        &crate::context::StatsSnapshot::default(),
    );
    assert_eq!(
        ranged.cardinality,
        properties::CardinalityBounds::new(1, Some(3)).unwrap()
    );
}

fn membership_op(predicate: helix_ast::expr::Predicate) -> logical::StreamPipelineOp {
    let key = crate::catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap();
    logical::StreamPipelineOp::IndexMembership {
        plan: Box::new(
            ir::NodeIndexMembershipPlan::new(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
                    index: crate::catalog::IndexCatalogSnapshot::default()
                        .with_node_eq(key.clone())
                        .node_eq[&key]
                        .clone(),
                    key,
                    value: ir::IndexValue::Literal(
                        ir::SecondaryIndexLiteral::new("B".into()).unwrap(),
                    ),
                })
                .unwrap(),
                ir::PredicatePlan::new(predicate).unwrap(),
            )
            .unwrap(),
        ),
    }
}

#[test]
fn membership_contract_prices_what_the_runtime_reads() {
    let storage = cost::StorageCostProfile::default();
    let stats = crate::context::StatsSnapshot::default();
    let unbounded = access_delivered(properties::ElementKind::Node);
    let unscoped = membership_op(helix_ast::expr::Predicate::eq("kind", "B"));
    let scoped = membership_op(helix_ast::expr::Predicate::and(vec![
        helix_ast::expr::Predicate::eq("$label", "Attribute"),
        helix_ast::expr::Predicate::eq("kind", "B"),
    ]));
    let filter = logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("kind", "B")).unwrap(),
    };
    let price = |op: &logical::StreamPipelineOp,
                 delivered: &properties::DeliveredProperties,
                 rows: u64,
                 stats: &crate::context::StatsSnapshot| {
        stream_pipeline_op_contract(
            op,
            delivered.clone(),
            cost::EstimatedRows::rows(rows),
            &storage,
            stats,
        )
    };

    // Past one batch the set reads amortize: an unscoped predicate reads the
    // label bitmap alongside the set and no records at all.
    let (op, _, evaluate) = price(&unscoped, &unbounded, 1_000, &stats);
    let (_, _, reject) = price(&scoped, &unbounded, 1_000, &stats);
    let (_, _, per_row) = price(&filter, &unbounded, 1_000, &stats);
    assert_eq!(
        op,
        crate::physical::PhysicalPipelineOp::Stream(
            crate::physical::PhysicalStreamOp::IndexMembership
        )
    );
    assert!(evaluate.latency < per_row.latency);
    assert!(reject.latency < per_row.latency);
    assert_eq!(evaluate.object_reads, 2);
    assert_eq!(evaluate.authoritative_graph_reads, 0);
    assert_eq!(reject.object_reads, 1);
    assert_eq!(per_row.authoritative_graph_reads, 1_000);

    // A stream proven to fit in one batch never reads the set, so the set read
    // only adds to the filter's work.
    let bounded = with_cardinality(unbounded.clone(), Some(40));
    let (_, membership_delivered, evaluate) = price(&unscoped, &bounded, 40, &stats);
    let (_, _, reject) = price(&scoped, &bounded, 40, &stats);
    let (_, _, per_row) = price(&filter, &bounded, 40, &stats);
    assert_eq!(
        membership_delivered.cardinality,
        properties::CardinalityBounds::zero_to(Some(40))
    );
    assert!(evaluate.latency > per_row.latency);
    assert!(reject.latency > per_row.latency);

    // An unbounded stream estimated within one batch does the work of the
    // filter over the same predicate less one predicate-leaf evaluation.
    for membership in [&unscoped, &scoped] {
        let logical::StreamPipelineOp::IndexMembership { plan } = membership else {
            panic!("expected an index membership op");
        };
        let same_filter = logical::StreamPipelineOp::Filter {
            predicate: plan.predicate().clone(),
        };
        let (_, _, per_row) = price(&same_filter, &unbounded, 10, &stats);
        let (_, _, cost) = price(membership, &unbounded, 10, &stats);
        assert_eq!(cost.object_reads, per_row.object_reads);
        assert_eq!(cost.authoritative_graph_reads, 10);
        assert_eq!(cost.cpu_units + 1, per_row.cpu_units);
        assert_eq!(
            cost.latency.as_micros() + storage.cpu_predicate_eval.as_micros(),
            per_row.latency.as_micros()
        );
    }

    // Just past one batch, and for an empty stream, the set read outweighs
    // the record reads it saves.
    for rows in [257, 0] {
        let (_, _, per_row) = price(&filter, &unbounded, rows, &stats);
        for membership in [&unscoped, &scoped] {
            let (_, _, cost) = price(membership, &unbounded, rows, &stats);
            assert!(cost.latency > per_row.latency, "rows {rows}");
        }
    }

    // A huge label bitmap makes membership lose to the same stream.
    let huge_label = crate::context::StatsSnapshot::default()
        .with_node_label_cardinality(ir::NonEmptyString::new("Attribute").unwrap(), 10_000_000);
    let (_, _, huge) = price(&unscoped, &unbounded, 1_000, &huge_label);
    let (_, _, per_row) = price(&filter, &unbounded, 1_000, &stats);
    assert!(huge.latency > per_row.latency);
}

#[test]
fn membership_pricing_leaves_the_filter_and_delivered_rows_alone() {
    let storage = cost::StorageCostProfile::default();
    let stats = crate::context::StatsSnapshot::default();
    let rows = cost::EstimatedRows::rows(1_000);
    let filter = logical::StreamPipelineOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("kind", "B")).unwrap(),
    };
    let membership = membership_op(helix_ast::expr::Predicate::eq("kind", "B"));
    for upper in [None, Some(40)] {
        let delivered = with_cardinality(access_delivered(properties::ElementKind::Node), upper);
        let (_, _, per_row) =
            stream_pipeline_op_contract(&filter, delivered.clone(), rows, &storage, &stats);
        assert_eq!(per_row, storage.stored_predicate_filter(rows));
        let (_, membership_delivered, _) =
            stream_pipeline_op_contract(&membership, delivered.clone(), rows, &storage, &stats);
        assert_eq!(membership_delivered.cardinality, delivered.cardinality);
    }
}

#[test]
fn row_estimates_carry_through_expansion_and_shrink_after_membership() {
    let storage = cost::StorageCostProfile::default();
    let stats = crate::context::StatsSnapshot::default();
    let unknown = access_delivered(properties::ElementKind::Node);
    let expand = logical::StreamPipelineOp::Expand {
        plan: ir::ExpandPlan {
            direction: ir::ExpandDirection::Out,
            output: ir::ExpandOutput::Nodes,
            label: ir::ExpandLabelPlan::Any,
        },
    };
    let after =
        |op: &logical::StreamPipelineOp, delivered: &properties::DeliveredProperties, rows: u64| {
            estimated_rows_after_op(
                op,
                delivered,
                cost::EstimatedRows::rows(rows),
                &storage,
                &stats,
            )
        };

    // An unknown fan-out never inflates row estimates; index_membership_filter
    // prices unbounded streams itself.
    assert_eq!(after(&expand, &unknown, 1), cost::EstimatedRows::rows(1));
    assert_eq!(
        after(&expand, &unknown, 50_000),
        cost::EstimatedRows::rows(50_000)
    );
    assert_eq!(
        after(&expand, &with_cardinality(unknown.clone(), Some(3)), 1),
        cost::EstimatedRows::rows(3)
    );
    let membership = membership_op(helix_ast::expr::Predicate::eq("kind", "B"));
    assert_eq!(
        after(&membership, &unknown, 1_000),
        storage.default_equality_index_rows
    );
    assert_eq!(
        after(&membership, &unknown, 2),
        cost::EstimatedRows::rows(2)
    );
    assert_eq!(
        after(&logical::StreamPipelineOp::Distinct, &unknown, 7),
        cost::EstimatedRows::rows(7)
    );
}
