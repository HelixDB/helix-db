use super::*;

#[test]
fn access_path_rule_uses_stats_for_label_index_and_filtered_costs() {
    let rule = AccessPathImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(10),
        range_next: cost::LatencyEstimate::micros(2),
        cpu_predicate_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(99),
        ..cost::StorageCostProfile::default()
    };
    let user_label = name("User");
    let user_email = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
    let user_id = catalog::ScopedPropertyKey::try_new("User", "id").unwrap();
    let likes_weight = catalog::ScopedPropertyDirectionKey::try_new(
        "LIKES",
        "weight",
        helix_ast::index::RangeIndexDirection::Desc,
    )
    .unwrap();
    let stats = context::StatsSnapshot::default()
        .with_node_label_cardinality(user_label.clone(), 4)
        .with_node_eq_cardinality(user_email.clone(), 7)
        .with_node_eq_cardinality(user_id.clone(), 3)
        .with_edge_range_cardinality(likes_weight.clone(), 9);

    let label = node_access_expr(ir::NodeAccessPlan::LabelScan { label: user_label });
    let equality = node_access_expr(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
        key: user_email.clone(),
        value: equality_literal(1),
    });
    let range = edge_access_expr(ir::EdgeAccessPlan::RangeIndex {
        index: catalog::EdgeRangeIndexMeta::try_new("likes_weight").unwrap(),
        key: likes_weight,
        range: lower_range(10),
    });

    let label = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &label,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: &stats,
    }));
    let equality = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &equality,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: &stats,
    }));
    let range = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &range,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: &stats,
    }));
    let filtered = node_access_contract(
        &ir::NodeAccessPlan::ScanThenFilter {
            source: ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
                key: user_email,
                value: equality_literal(1),
            })
            .unwrap(),
            residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        },
        &storage,
        &stats,
    );
    let unique_filtered = node_access_contract(
        &ir::NodeAccessPlan::ScanThenFilter {
            source: ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_id")
                    .unwrap()
                    .with_uniqueness(catalog::IndexUniqueness::Unique),
                key: user_id,
                value: equality_literal(42),
            })
            .unwrap(),
            residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        },
        &storage,
        &stats,
    );
    let unique_missing_stats_storage = cost::StorageCostProfile {
        default_unique_equality_rows: cost::UniqueEqualityRows::ZERO,
        ..storage.clone()
    };
    let unique_missing_stats_filtered = node_access_contract(
        &ir::NodeAccessPlan::ScanThenFilter {
            source: ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("external_id")
                    .unwrap()
                    .with_uniqueness(catalog::IndexUniqueness::Unique),
                key: catalog::ScopedPropertyKey::try_new("User", "external_id").unwrap(),
                value: equality_literal(42),
            })
            .unwrap(),
            residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        },
        &unique_missing_stats_storage,
        &stats,
    );
    let union_estimate = access_path_contract(
        &node_access_path(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![1, 2]),
            })
            .unwrap(),
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
                index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                value: equality_literal(1),
            })
            .unwrap(),
        ))),
        &storage,
        &stats,
    );
    let intersection_estimate = access_path_contract(
        &node_access_path(ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
                    ids: element_ids(vec![1, 2]),
                })
                .unwrap(),
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
                    index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
                    key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                    value: equality_literal(1),
                })
                .unwrap(),
            ),
        )),
        &storage,
        &stats,
    );

    assert_eq!(label.cost, storage.range_scan(cost::EstimatedRows::rows(4)));
    assert_eq!(
        equality.cost,
        storage
            .bitmap_equality_lookup(cost::EstimatedRows::rows(7))
            .serial(storage.secondary_row_materialization(cost::EstimatedRows::rows(7)))
    );
    assert_eq!(
        range.cost,
        storage
            .secondary_range_lookup(cost::EstimatedRows::rows(9))
            .serial(storage.secondary_row_materialization(cost::EstimatedRows::rows(9)))
    );
    assert_eq!(
        filtered.cost,
        storage
            .bitmap_equality_lookup(cost::EstimatedRows::rows(7))
            .serial(storage.secondary_row_materialization(cost::EstimatedRows::rows(7)))
            .serial(storage.predicate_eval(cost::EstimatedRows::rows(7)))
    );
    assert_eq!(filtered.estimated_rows, cost::EstimatedRows::rows(7));
    assert_eq!(
        unique_filtered.cost,
        storage
            .unique_equality_lookup(cost::EstimatedRows::rows(1))
            .serial(storage.secondary_row_materialization(cost::EstimatedRows::rows(1)))
            .serial(storage.predicate_eval(cost::EstimatedRows::rows(1)))
    );
    assert_eq!(unique_filtered.estimated_rows, cost::EstimatedRows::rows(1));
    assert_eq!(
        unique_missing_stats_filtered.cost,
        unique_missing_stats_storage
            .unique_equality_lookup(cost::EstimatedRows::ZERO)
            .serial(
                unique_missing_stats_storage
                    .secondary_row_materialization(cost::EstimatedRows::ZERO),
            )
            .serial(unique_missing_stats_storage.predicate_eval(cost::EstimatedRows::ZERO))
    );
    assert_eq!(
        unique_missing_stats_filtered.estimated_rows,
        cost::EstimatedRows::ZERO
    );
    assert_eq!(union_estimate.estimated_rows, cost::EstimatedRows::rows(9));
    assert_eq!(
        intersection_estimate.estimated_rows,
        cost::EstimatedRows::rows(2)
    );
}
