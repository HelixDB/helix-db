use std::num::NonZeroUsize;

use super::{cardinality, delivered};
use crate::{catalog, ir, properties};

fn ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
}

fn source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::new(plan).unwrap()
}

fn edge_source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
    ir::EdgeAccessSourcePlan::new(plan).unwrap()
}

fn literal_search_limit(value: usize) -> ir::SearchLimitPlan {
    ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
}

fn node_range(direction: helix_ast::index::RangeIndexDirection) -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::RangeIndex {
        index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new("User", "age", direction).unwrap(),
        range: ir::IndexRange::All,
    }
}

fn edge_range(direction: helix_ast::index::RangeIndexDirection) -> ir::EdgeAccessPlan {
    ir::EdgeAccessPlan::RangeIndex {
        index: catalog::EdgeRangeIndexMeta::try_new("edge_range").unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new("FOLLOWS", "score", direction).unwrap(),
        range: ir::IndexRange::All,
    }
}

#[test]
fn hard_upper_bounds_cover_point_search_set_and_filtered_sources() {
    assert_eq!(
        cardinality::node_access_hard_upper_bound(&ir::NodeAccessPlan::PointIds {
            ids: ids(vec![1, 2, 3])
        }),
        Some(3)
    );
    assert_eq!(
        cardinality::node_access_hard_upper_bound(&ir::NodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
            index: ir::SearchIndexPlan {
                index_id: ir::NonEmptyString::new("doc_embedding").unwrap(),
                tenant: ir::SearchTenantPlan::Unscoped,
            },
            query_vector: ir::VectorQueryInputPlan::Vector(
                ir::SearchVector::new(vec![1.0]).unwrap()
            ),
            k: literal_search_limit(5),
        }),
        Some(5)
    );

    let bounded = source(ir::NodeAccessPlan::PointIds {
        ids: ids(vec![1, 2]),
    });
    let unbounded = source(ir::NodeAccessPlan::AllScan);
    let union = ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![bounded.clone(), bounded.clone()]).unwrap(),
    );
    assert_eq!(cardinality::node_access_hard_upper_bound(&union), Some(4));

    let mixed_union = ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::try_from_vec(vec![bounded, unbounded]).unwrap(),
    );
    assert_eq!(
        cardinality::node_access_hard_upper_bound(&mixed_union),
        None
    );

    let filtered = ir::NodeAccessPlan::ScanThenFilter {
        source: source(ir::NodeAccessPlan::PointIds {
            ids: ids(vec![7, 8]),
        }),
        residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    };
    assert_eq!(
        cardinality::node_access_hard_upper_bound(&filtered),
        Some(2)
    );
}

#[test]
fn delivered_properties_preserve_range_order_and_key_locality() {
    let delivered = delivered::node_access_delivered_properties(&node_range(
        helix_ast::index::RangeIndexDirection::Desc,
    ));
    assert_eq!(delivered.element, Some(properties::ElementKind::Node));
    assert_eq!(delivered.key_locality, properties::KeyLocality::Close);
    assert!(matches!(
        delivered.ordering,
        properties::DeliveredOrdering::ByKeys(_)
    ));

    let scan = delivered::node_access_delivered_properties(&ir::NodeAccessPlan::AllScan);
    assert_eq!(scan.key_locality, properties::KeyLocality::Unknown);
    assert_eq!(scan.ordering, properties::DeliveredOrdering::Unordered);
}

#[test]
fn delivered_properties_do_not_inherit_nested_intersection_ordering() {
    let nested_node = source(ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
        source(node_range(helix_ast::index::RangeIndexDirection::Desc)),
        source(ir::NodeAccessPlan::Empty),
    )));
    let node = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
        nested_node,
        source(ir::NodeAccessPlan::Empty),
    ));
    let nested_edge = edge_source(ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
        edge_source(edge_range(helix_ast::index::RangeIndexDirection::Desc)),
        edge_source(ir::EdgeAccessPlan::Empty),
    )));
    let edge = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
        nested_edge,
        edge_source(ir::EdgeAccessPlan::Empty),
    ));

    assert_eq!(
        delivered::node_access_delivered_properties(&node).ordering,
        properties::DeliveredOrdering::Unordered
    );
    assert_eq!(
        delivered::edge_access_delivered_properties(&edge).ordering,
        properties::DeliveredOrdering::Unordered
    );
}

#[test]
fn element_point_delivered_properties_are_exact_and_close() {
    let delivered = delivered::element_point_delivered_properties(properties::ElementKind::Edge, 2);
    assert_eq!(delivered.element, Some(properties::ElementKind::Edge));
    assert_eq!(
        delivered.cardinality,
        properties::CardinalityBounds::exact(2)
    );
    assert_eq!(delivered.key_locality, properties::KeyLocality::Close);
}
