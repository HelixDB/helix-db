use super::*;

#[test]
fn access_path_rule_implements_point_reads_as_get_multiget_or_split_batches() {
    let rule = AccessPathImplementationRule::default();
    let storage = cost::StorageCostProfile {
        multi_get_setup: cost::LatencyEstimate::micros(10),
        multi_get_per_key: cost::LatencyEstimate::micros(2),
        sstable_filter_probe: cost::LatencyEstimate::micros(3),
        task_overhead: cost::LatencyEstimate::micros(1),
        sparse_key_multi_get_batch: properties::PositiveUsize::new(2).unwrap(),
        max_parallel_kv_reads: properties::PositiveUsize::new(4).unwrap(),
        ..cost::StorageCostProfile::default()
    };

    let single = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &node_access_expr(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![7]),
        }),
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let batched = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &node_access_expr(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![9, 7]),
        }),
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));
    let split = physical_alternative(rule.apply(optimizer::RuleInput {
        expr: &node_access_expr(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![1, 2, 3]),
        }),
        storage: &storage,
        indexes: empty_indexes(),
        planner_limits: default_planner_limits(),
        stats: default_stats(),
    }));

    assert_eq!(rule.metadata().id.as_ref(), "seed_access_path");
    assert!(matches!(
        single.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::Get { .. }),
            ..
        }
    ));
    assert_eq!(
        single.delivered.cardinality,
        properties::CardinalityBounds::exact(1)
    );
    assert!(matches!(
        batched.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::Kv(exec::KvReadPlan::MultiGet(_)),
            ..
        }
    ));
    assert_eq!(
        batched.delivered.cardinality,
        properties::CardinalityBounds::exact(2)
    );
    assert!(matches!(
        split.expr,
        physical::PhysicalExpr::Access {
            access: physical::PhysicalAccess::PointReads {
                locality: properties::KeyLocality::Unknown
            },
            ..
        }
    ));
    assert_eq!(
        split.delivered.cardinality,
        properties::CardinalityBounds::exact(3)
    );
    assert_eq!(
        split.cost,
        storage.parallel(
            &[
                storage.multi_get(
                    properties::PositiveUsize::new(2).unwrap(),
                    properties::KeyLocality::Unknown,
                ),
                storage.multi_get(
                    properties::PositiveUsize::new(1).unwrap(),
                    properties::KeyLocality::Unknown,
                ),
            ],
            storage.max_parallel_kv_reads,
        )
    );
}
