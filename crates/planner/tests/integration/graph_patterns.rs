use helix_planner::{
    context, cost, logical, optimizer, physical, properties, relational as r, rules,
};
use std::collections::BTreeSet;

#[test]
fn cascades_chooses_less_expansion_work_over_the_smallest_start() {
    let storage = cost::StorageCostProfile::default();
    let pattern = r::GraphPatternOrder::new(
        vec![
            r::PatternSource {
                slot: r::Slot(0),
                rows: 1,
                access_cost: storage.range_scan(cost::EstimatedRows::rows(1)),
            },
            r::PatternSource {
                slot: r::Slot(1),
                rows: 10,
                access_cost: storage.range_scan(cost::EstimatedRows::rows(10)),
            },
        ],
        vec![r::PatternExpansion {
            from: r::Slot(0),
            to: r::Slot(1),
            relationship: r::Slot(2),
            forward_rows: 10000,
            reverse_rows: 1,
        }],
        BTreeSet::new(),
    )
    .unwrap();
    let seed = pattern.schedule(&storage);
    assert_eq!(seed.steps[0], r::MatchStep::Scan(r::Slot(0)));
    let config = optimizer::OptimizerConfig::from_context(&context::PlannerContext::default());
    let rules = rules::SeedRuleSet::default();
    let result = rules
        .optimizer()
        .optimize(logical::LogicalExpr::GraphPattern(pattern), &config)
        .unwrap();
    let selected = result.best_alternative(result.roots()[0]).unwrap();
    let physical::PhysicalExpr::GraphPattern(order) = &selected.expr else {
        panic!("pattern alternative");
    };
    let best = order.schedule(&storage);
    assert_eq!(best.steps[0], r::MatchStep::Scan(r::Slot(1)));
    assert!(best.cost.range_nexts < seed.cost.range_nexts);
    assert_eq!(best.estimated_rows, 10);
    assert!(!result.metrics().guardrail_hit);
    assert_eq!(result.metrics().memo_groups, 1);
}

#[test]
fn expansion_priorities_are_explored_and_bound_endpoints_stay_correlated() {
    let sources = (0..3)
        .map(|i| r::PatternSource {
            slot: r::Slot(i),
            rows: 1,
            access_cost: cost::CostVector::ZERO,
        })
        .collect();
    let relationships = vec![
        r::PatternExpansion {
            from: r::Slot(0),
            to: r::Slot(1),
            relationship: r::Slot(3),
            forward_rows: 1000,
            reverse_rows: 1000,
        },
        r::PatternExpansion {
            from: r::Slot(0),
            to: r::Slot(2),
            relationship: r::Slot(4),
            forward_rows: 0,
            reverse_rows: 0,
        },
    ];
    let pattern = r::GraphPatternOrder::new(
        sources,
        relationships,
        BTreeSet::from([r::Slot(0), r::Slot(2)]),
    )
    .unwrap();
    let rules = rules::SeedRuleSet::default();
    let mut context = context::PlannerContext::default();
    context.optimizer_limits.rule_fires = properties::PositiveUsize::at_least_one(4);
    let result = rules
        .optimizer()
        .optimize(
            logical::LogicalExpr::GraphPattern(pattern),
            &optimizer::OptimizerConfig::from_context(&context),
        )
        .unwrap();
    assert!(result.metrics().rule_fires <= 4);
    let alternative = result.best_alternative(result.roots()[0]).unwrap();
    let physical::PhysicalExpr::GraphPattern(order) = &alternative.expr else {
        panic!("pattern alternative");
    };
    let schedule = order.schedule(&context.storage);
    assert!(schedule
        .steps
        .iter()
        .all(|step| !matches!(step, r::MatchStep::Scan(_))));
    assert_eq!(schedule.steps.len(), 2);
    assert_eq!(schedule.cartesian_products, 0);
}

#[test]
fn graph_pattern_contract_validates_deserialization_and_saturates_costs() {
    let source = r::PatternSource {
        slot: r::Slot(0),
        rows: u64::MAX,
        access_cost: cost::CostVector::ZERO,
    };
    assert!(r::GraphPatternOrder::new(vec![], vec![], BTreeSet::new()).is_err());
    assert!(r::GraphPatternOrder::new(
        vec![source.clone(), source.clone()],
        vec![],
        BTreeSet::new()
    )
    .is_err());
    let relationship = r::PatternExpansion {
        from: r::Slot(0),
        to: r::Slot(1),
        relationship: r::Slot(2),
        forward_rows: u64::MAX,
        reverse_rows: u64::MAX,
    };
    assert!(r::GraphPatternOrder::new(
        vec![source.clone()],
        vec![relationship.clone()],
        BTreeSet::new()
    )
    .is_err());
    let pattern = r::GraphPatternOrder::new(
        vec![
            source,
            r::PatternSource {
                slot: r::Slot(1),
                rows: u64::MAX,
                access_cost: cost::CostVector::ZERO,
            },
        ],
        vec![relationship],
        BTreeSet::new(),
    )
    .unwrap();
    let value = serde_json::to_value(&pattern).unwrap();
    assert_eq!(
        serde_json::from_value::<r::GraphPatternOrder>(value.clone()).unwrap(),
        pattern
    );
    let mut invalid = value;
    invalid["source_priority"] = serde_json::json!([0, 0]);
    assert!(serde_json::from_value::<r::GraphPatternOrder>(invalid).is_err());
    let schedule = pattern.schedule(&cost::StorageCostProfile::default());
    assert_eq!(schedule.estimated_rows, u64::MAX);
    assert_eq!(schedule.cost.range_nexts, u64::MAX);
}

#[test]
fn grouping_hashes_match_total_equality_without_rounding_integers() {
    use std::hash::{Hash, Hasher};
    let hash = |value: r::Value| {
        let mut state = std::hash::DefaultHasher::new();
        r::GroupingKey::new(value).unwrap().hash(&mut state);
        state.finish()
    };
    for (a, b) in [
        (r::Value::Integer(1), r::Value::Float(1.0)),
        (r::Value::Integer(0), r::Value::Float(-0.0)),
        (r::Value::Float(f64::NAN), r::Value::Float(-f64::NAN)),
        (
            r::Value::List(vec![r::Value::Integer(1), r::Value::Null]),
            r::Value::List(vec![r::Value::Float(1.0), r::Value::Null]),
        ),
    ] {
        assert_eq!(
            r::GroupingKey::new(a.clone()).unwrap(),
            r::GroupingKey::new(b.clone()).unwrap()
        );
        assert_eq!(hash(a), hash(b));
    }
    let exact = r::GroupingKey::new(r::Value::Integer(9_007_199_254_740_993)).unwrap();
    let rounded = r::GroupingKey::new(r::Value::Float(9_007_199_254_740_992.0)).unwrap();
    assert_ne!(exact, rounded);
    assert_eq!(exact.value(), &r::Value::Integer(9_007_199_254_740_993));
}

#[test]
fn incremental_aggregates_bound_distinct_state_and_keep_failed_updates_atomic() {
    let mut count = r::Accumulator::new(r::Aggregate::Count, true);
    for _ in 0..100_000 {
        count.push(r::Value::Integer(7), 1, 2048).unwrap();
    }
    assert!(count.allocated_bytes() < 2048);
    assert_eq!(count.finish().unwrap(), r::Value::Integer(1));
    for (function, expected) in [
        (r::Aggregate::Sum, r::Value::Integer(6)),
        (r::Aggregate::Avg, r::Value::Float(2.0)),
        (r::Aggregate::Min, r::Value::Integer(1)),
        (r::Aggregate::Max, r::Value::Integer(3)),
    ] {
        let mut state = r::Accumulator::new(function, false);
        state.push(r::Value::Null, 1, 2048).unwrap();
        for value in [1, 2, 3] {
            state.push(r::Value::Integer(value), 1, 2048).unwrap();
        }
        assert_eq!(state.finish().unwrap(), expected);
    }
    let mut sum = r::Accumulator::new(r::Aggregate::Sum, true);
    sum.push(r::Value::Integer(1), 10, 2048).unwrap();
    assert!(sum
        .push(r::Value::String("invalid".into()), 10, 2048)
        .is_err());
    sum.push(r::Value::Integer(2), 10, 2048).unwrap();
    assert_eq!(sum.finish().unwrap(), r::Value::Integer(3));
    let mut collect = r::Accumulator::new(r::Aggregate::Collect, true);
    collect.push(r::Value::Integer(2), 1, 2048).unwrap();
    assert_eq!(
        collect
            .push(r::Value::Integer(3), 1, 2048)
            .unwrap_err()
            .detail,
        "CollectionLimit"
    );
    assert_eq!(
        collect.finish().unwrap(),
        r::Value::List(vec![r::Value::Integer(2)])
    );
    let mut minimum = r::Accumulator::new(r::Aggregate::Min, false);
    assert_eq!(
        minimum
            .push(r::Value::String("x".repeat(4096)), 10, 2048)
            .unwrap_err()
            .detail,
        "MemoryLimit"
    );
    assert_eq!(minimum.finish().unwrap(), r::Value::Null);
}

#[test]
fn correlated_lookup_contracts_and_scan_alternatives_share_the_memo() {
    use helix_planner::catalog;
    let storage = cost::StorageCostProfile::default();
    let source = r::PatternSource {
        slot: r::Slot(0),
        rows: 10_000,
        access_cost: storage.range_scan(cost::EstimatedRows::rows(10_000)),
    };
    let pattern =
        r::GraphPatternOrder::new(vec![source], vec![], BTreeSet::from([r::Slot(1)])).unwrap();
    let lookup = r::PatternLookup {
        slot: r::Slot(0),
        probe: r::Slot(1),
        index: catalog::NodeEqualityIndexMeta::try_new("n-key").unwrap(),
        key: catalog::ScopedPropertyKey::try_new("N", "key").unwrap(),
        estimated_rows: 1,
    };
    for (target, probe) in [(1, 1), (0, 0), (2, 1), (0, 2)] {
        let invalid = r::PatternLookup {
            slot: r::Slot(target),
            probe: r::Slot(probe),
            ..lookup.clone()
        };
        assert!(pattern.clone().with_lookups(vec![invalid]).is_err());
    }
    assert!(pattern
        .clone()
        .with_lookups(vec![lookup.clone(), lookup.clone()])
        .is_err());
    for uniqueness in [
        catalog::IndexUniqueness::Unique,
        catalog::IndexUniqueness::NonUnique,
    ] {
        let lookup = r::PatternLookup {
            index: lookup.index.clone().with_uniqueness(uniqueness),
            ..lookup.clone()
        };
        let indexed = pattern.clone().with_lookups(vec![lookup.clone()]).unwrap();
        let encoded = serde_json::to_value(&indexed).unwrap();
        assert_eq!(
            serde_json::from_value::<r::GraphPatternOrder>(encoded.clone()).unwrap(),
            indexed
        );
        let mut invalid = encoded;
        invalid["lookups"][0]["probe"] = serde_json::json!(99);
        assert!(serde_json::from_value::<r::GraphPatternOrder>(invalid).is_err());
        let rules = rules::SeedRuleSet::default();
        let config = optimizer::OptimizerConfig::from_context(&context::PlannerContext::default());
        let result = rules
            .optimizer()
            .optimize(logical::LogicalExpr::GraphPattern(indexed), &config)
            .unwrap();
        let physical::PhysicalExpr::GraphPattern(selected) =
            &result.best_alternative(result.roots()[0]).unwrap().expr
        else {
            panic!("graph plan")
        };
        let schedule = selected.schedule(&storage);
        assert_eq!(schedule.steps, vec![r::MatchStep::IndexLookup(lookup)]);
        assert_eq!(schedule.estimated_rows, 1);
        assert_eq!(schedule.cartesian_products, 0);
        assert_eq!(result.metrics().memo_groups, 1);
        assert!(!result.metrics().guardrail_hit);
    }
}
