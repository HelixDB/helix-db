use super::*;

#[test]
fn access_path_rule_covers_scan_runtime_search_and_filtered_costs() {
    let rule = AccessPathImplementationRule::default();
    let storage = cost::StorageCostProfile {
        range_seek: cost::LatencyEstimate::micros(10),
        range_next: cost::LatencyEstimate::micros(2),
        source_inject_overhead: cost::LatencyEstimate::micros(7),
        cpu_predicate_eval: cost::LatencyEstimate::micros(3),
        default_unknown_scan_rows: cost::EstimatedRows::rows(5),
        ..cost::StorageCostProfile::default()
    };
    let vector = node_access_expr(ir::NodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: name("user_embedding"),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
            helix_ast::value::PropertyValue::F32Array(vec![0.5]),
        ))
        .unwrap(),
        k: ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::new(3).unwrap()),
    });
    let text = edge_access_expr(ir::EdgeAccessPlan::TextSearch {
        key: catalog::EdgeSearchIndexKey::try_new("LIKES", "comment").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: name("likes_comment"),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_text: ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
            helix_ast::value::PropertyValue::from("great"),
        ))
        .unwrap(),
        k: ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::new(2).unwrap()),
    });
    let runtime = edge_access_expr(ir::EdgeAccessPlan::FromParam {
        param: name("edge_ids"),
    });
    let scan = node_access_expr(ir::NodeAccessPlan::AllScan);

    let vector = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &vector,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let text = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &text,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let runtime = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &runtime,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let scan = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &scan,
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let filtered = node_access_contract(
        &ir::NodeAccessPlan::ScanThenFilter {
            source: ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![42]),
            })
            .unwrap(),
            residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
                .unwrap(),
        },
        &storage,
        default_stats(),
    );

    assert!(matches!(
        vector.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::VectorSearch,
            ..
        }
    ));
    assert_eq!(vector.delivered.cardinality.upper(), Some(3));
    assert_eq!(
        vector.cost,
        storage.range_scan(cost::EstimatedRows::rows(3))
    );
    assert!(matches!(
        text.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::TextSearch,
            ..
        }
    ));
    assert_eq!(text.delivered.cardinality.upper(), Some(2));
    assert!(matches!(
        runtime.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::RuntimeInput,
            ..
        }
    ));
    assert_eq!(runtime.cost, storage.source_inject());
    assert!(matches!(
        scan.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::RangeScan { .. }),
            ..
        }
    ));
    assert_eq!(
        filtered.cost,
        storage
            .point_gets(properties::PositiveUsize::new(1).unwrap())
            .serial(storage.predicate_eval(cost::EstimatedRows::rows(1)))
    );
}
