use super::*;
use crate::{catalog, context, cost, exec, ir, properties};
use helix_ast::index::RangeIndexDirection;
use std::num::NonZeroUsize;

static TEST_INDEX_ID: std::sync::LazyLock<ir::NonEmptyString> =
    std::sync::LazyLock::new(|| ir::NonEmptyString::new("node_eq:User:email").unwrap());

#[derive(Debug)]
enum TestPlan {
    Empty,
    UniqueEquality(catalog::ScopedPropertyKey),
    NonUniqueEquality {
        index_id: ir::NonEmptyString,
        key: catalog::ScopedPropertyKey,
    },
    Range(catalog::ScopedPropertyDirectionKey),
    Search(ir::SearchLimitPlan),
    Intersect(Vec<TestPlan>),
    Union(Vec<TestPlan>),
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
                access: test_equality_access(
                    catalog::NodeEqualityIndexMeta::try_new(TEST_INDEX_ID.as_ref())
                        .unwrap()
                        .with_uniqueness(catalog::IndexUniqueness::Unique),
                    key,
                ),
                index_id: &TEST_INDEX_ID,
                key,
                kind: EqualityIndexKind::Unique,
                semantics: ir::EqualityIndexValueSemantics::Indexed,
            },
            TestPlan::NonUniqueEquality { index_id, key } => AccessSourceParts::EqualityIndex {
                access: test_equality_access(
                    catalog::NodeEqualityIndexMeta::try_new(index_id.as_ref()).unwrap(),
                    key,
                ),
                index_id,
                key,
                kind: EqualityIndexKind::NonUnique,
                semantics: ir::EqualityIndexValueSemantics::Indexed,
            },
            TestPlan::Range(key) => AccessSourceParts::RangeIndex { key },
            TestPlan::Search(k) => AccessSourceParts::VectorSearch { k },
            TestPlan::Intersect(plans) => AccessSourceParts::Intersect(plans.iter().collect()),
            TestPlan::Union(plans) => AccessSourceParts::Union(plans.iter().collect()),
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

fn test_equality_access(
    index: catalog::NodeEqualityIndexMeta,
    key: &catalog::ScopedPropertyKey,
) -> crate::physical::PhysicalAccess {
    crate::physical::PhysicalAccess::NodeExact(Box::new(exec::ExecNodeAccessPlan::exact_equality(
        index,
        key.clone(),
        ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(
                "test@example.com",
            ))
            .unwrap(),
        ),
    )))
}

fn eq_key() -> catalog::ScopedPropertyKey {
    catalog::ScopedPropertyKey::try_new("User", "email").unwrap()
}

fn non_unique_equality(index_id: &str, key: catalog::ScopedPropertyKey) -> TestPlan {
    TestPlan::NonUniqueEquality {
        index_id: ir::NonEmptyString::new(index_id).unwrap(),
        key,
    }
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
    let non_unique = access_contract::<TestFamily>(
        &non_unique_equality("node_eq:User:email", key),
        &storage,
        &stats,
    );

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
fn equality_union_batches_only_the_same_logical_index() {
    let key = eq_key();
    let stats = context::StatsSnapshot::default().with_node_eq_cardinality(key.clone(), 9);
    let storage = cost::StorageCostProfile::default();
    let same_index = access_contract::<TestFamily>(
        &TestPlan::Union(vec![
            non_unique_equality("node_eq:User:email", key.clone()),
            non_unique_equality("node_eq:User:email", key.clone()),
        ]),
        &storage,
        &stats,
    );
    let different_indexes = access_contract::<TestFamily>(
        &TestPlan::Union(vec![
            non_unique_equality("node_eq:User:email:primary", key.clone()),
            non_unique_equality("node_eq:User:email:shadow", key),
        ]),
        &storage,
        &stats,
    );

    assert_eq!(same_index.cost.multi_get_calls, 1);
    assert_eq!(same_index.cost.object_reads, 2);
    assert_eq!(different_indexes.cost.multi_get_calls, 0);
    assert_ne!(same_index.cost, different_indexes.cost);
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
            .secondary_range_lookup(range_rows)
            .serial(storage.secondary_row_materialization(range_rows))
            .serial(storage.predicate_eval(range_rows))
    );
    assert_eq!(
        intersection.delivered.cardinality,
        properties::CardinalityBounds::zero_to(Some(3))
    );
    assert_eq!(intersection.estimated_rows, cost::EstimatedRows::rows(3));
}

#[test]
fn intersection_contract_inherits_order_only_from_a_direct_range_child() {
    let key = range_key();
    let stats = context::StatsSnapshot::default().with_node_range_cardinality(key.clone(), 5);
    let storage = cost::StorageCostProfile::default();
    let flat = access_contract::<TestFamily>(
        &TestPlan::Intersect(vec![
            TestPlan::Range(key.clone()),
            non_unique_equality("node_eq:User:email", eq_key()),
        ]),
        &storage,
        &stats,
    );
    let nested = access_contract::<TestFamily>(
        &TestPlan::Intersect(vec![
            TestPlan::Intersect(vec![
                TestPlan::Range(key),
                non_unique_equality("node_eq:User:email", eq_key()),
            ]),
            non_unique_equality("node_eq:User:tenant", eq_key()),
        ]),
        &storage,
        &stats,
    );

    assert!(matches!(
        flat.delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));
    assert_eq!(
        nested.delivered.ordering,
        properties::DeliveredOrdering::Unordered
    );
}
