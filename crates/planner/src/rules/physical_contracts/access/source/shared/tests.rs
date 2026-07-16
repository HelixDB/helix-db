use super::*;
use crate::{catalog, context, cost, exec, ir, properties};
use helix_ast::index::RangeIndexDirection;
use std::num::NonZeroUsize;

#[derive(Debug)]
enum TestPlan {
    Empty,
    UniqueEquality(catalog::ScopedPropertyKey),
    NonUniqueEquality(catalog::ScopedPropertyKey),
    Range(catalog::ScopedPropertyDirectionKey),
    Search(ir::SearchLimitPlan),
    Intersect(Vec<TestPlan>),
    Filtered(Box<TestPlan>),
}

struct TestFamily;

impl AccessSourceFamily for TestFamily {
    type Plan = TestPlan;

    fn element() -> properties::ElementKind {
        properties::ElementKind::Node
    }

    fn point_keyspace() -> exec::ElementKeyspace {
        exec::ElementKeyspace::NodeProperty
    }

    fn all_scan_keyspace() -> exec::ElementKeyspace {
        exec::ElementKeyspace::NodeProperty
    }

    fn source_parts(plan: &Self::Plan) -> AccessSourceParts<'_, Self::Plan> {
        match plan {
            TestPlan::Empty => AccessSourceParts::Empty,
            TestPlan::UniqueEquality(key) => AccessSourceParts::EqualityIndex {
                key,
                kind: EqualityIndexKind::Unique,
            },
            TestPlan::NonUniqueEquality(key) => AccessSourceParts::EqualityIndex {
                key,
                kind: EqualityIndexKind::NonUnique,
            },
            TestPlan::Range(key) => AccessSourceParts::RangeIndex { key },
            TestPlan::Search(k) => AccessSourceParts::VectorSearch { k },
            TestPlan::Intersect(plans) => AccessSourceParts::Intersect(plans.iter().collect()),
            TestPlan::Filtered(source) => AccessSourceParts::ScanThenFilter { source },
        }
    }

    fn label_cardinality(
        _stats: &context::StatsSnapshot,
        _label: &ir::NonEmptyString,
    ) -> Option<u64> {
        None
    }

    fn equality_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<u64> {
        stats.node_eq_cardinality.get(key).copied()
    }

    fn range_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<u64> {
        stats.node_range_cardinality.get(key).copied()
    }
}

fn eq_key() -> catalog::ScopedPropertyKey {
    catalog::ScopedPropertyKey::try_new("User", "email").unwrap()
}

fn range_key() -> catalog::ScopedPropertyDirectionKey {
    catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap()
}

#[test]
fn equality_index_contract_distinguishes_unique_cardinality_and_cost_rows() {
    let key = eq_key();
    let stats = context::StatsSnapshot::default().with_node_eq_cardinality(key.clone(), 9);
    let storage = cost::StorageCostProfile::default();

    let unique =
        access_contract::<TestFamily>(&TestPlan::UniqueEquality(key.clone()), &storage, &stats);
    let non_unique =
        access_contract::<TestFamily>(&TestPlan::NonUniqueEquality(key), &storage, &stats);

    assert_eq!(
        unique.delivered.cardinality,
        properties::CardinalityBounds::zero_to(Some(1))
    );
    assert_eq!(
        non_unique.delivered.cardinality,
        properties::CardinalityBounds::unknown()
    );
    assert_eq!(unique.estimated_rows, storage.unique_equality_rows(Some(9)));
    assert_eq!(
        non_unique.estimated_rows,
        storage.equality_index_rows(Some(9))
    );
}

#[test]
fn set_and_filter_contracts_reuse_shared_child_costs() {
    let key = range_key();
    let stats = context::StatsSnapshot::default().with_node_range_cardinality(key.clone(), 5);
    let storage = cost::StorageCostProfile::default();

    let empty = access_contract::<TestFamily>(&TestPlan::Empty, &storage, &stats);
    let filtered = access_contract::<TestFamily>(
        &TestPlan::Filtered(Box::new(TestPlan::Range(key.clone()))),
        &storage,
        &stats,
    );
    let intersection = access_contract::<TestFamily>(
        &TestPlan::Intersect(vec![
            TestPlan::Range(key),
            TestPlan::Search(ir::SearchLimitPlan::Literal(NonZeroUsize::new(3).unwrap())),
        ]),
        &storage,
        &stats,
    );

    assert_eq!(empty.estimated_rows, cost::EstimatedRows::ZERO);
    assert_eq!(
        empty.delivered.cardinality,
        properties::CardinalityBounds::exact(0)
    );
    let range_rows = cost::EstimatedRows::rows(5);
    assert_eq!(filtered.estimated_rows, range_rows);
    assert_eq!(
        filtered.cost,
        storage
            .range_scan(range_rows)
            .serial(storage.predicate_eval(range_rows))
    );
    assert_eq!(
        intersection.delivered.cardinality,
        properties::CardinalityBounds::zero_to(Some(3))
    );
    assert_eq!(intersection.estimated_rows, cost::EstimatedRows::rows(3));
}
