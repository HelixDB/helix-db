use std::num::NonZeroUsize;

use helix_ast::value::{PropertyInput, PropertyValue};

use super::super::{AccessDistinct, AccessOrder, AccessPath, AccessWindow};
use crate::{catalog, ir, logical};

fn node_window(plan: ir::NodeAccessPlan, window: logical::AccessWindowRange) -> AccessWindow {
    AccessWindow::new(
        AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(plan),
        )),
        window,
    )
}

fn node_access(plan: ir::NodeAccessPlan) -> AccessPath {
    AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::from_unfiltered(plan),
    ))
}

fn edge_access(plan: ir::EdgeAccessPlan) -> AccessPath {
    AccessPath::Edge(logical::EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::from_unfiltered(plan),
    ))
}

fn point_ids(values: Vec<u64>) -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::PointIds {
        ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap(),
    }
}

fn vector_search(k: usize) -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: ir::NonEmptyString::new("doc_embedding").unwrap(),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(
            vec![0.1],
        )))
        .unwrap(),
        k: ir::SearchLimitPlan::Literal(NonZeroUsize::new(k).unwrap()),
    }
}

fn range_source(
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
) -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::RangeIndex {
        index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new("User", property, direction).unwrap(),
        range: ir::IndexRange::All,
    }
}

fn edge_range_source(
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
) -> ir::EdgeAccessPlan {
    ir::EdgeAccessPlan::RangeIndex {
        index: catalog::EdgeRangeIndexMeta::try_new("edge_range").unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new("LIKES", property, direction).unwrap(),
        range: ir::IndexRange::All,
    }
}

fn order_key(property: &str, order: helix_ast::traversal::Order) -> ir::OrderKey {
    ir::OrderKey {
        property: ir::NonEmptyString::new(property).unwrap(),
        order,
    }
}

fn order_keys(property: &str, order: helix_ast::traversal::Order) -> ir::OrderKeys {
    ir::OrderKeys::from(order_key(property, order))
}

fn multi_order_keys() -> ir::OrderKeys {
    ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one_and_rest(
        order_key("age", helix_ast::traversal::Order::Desc),
        vec![order_key("name", helix_ast::traversal::Order::Asc)],
    ))
    .unwrap()
}

#[test]
fn access_window_rewrite_candidate_covers_supported_rewrite_families() {
    assert!(
        node_window(
            ir::NodeAccessPlan::AllScan,
            logical::AccessWindowRange::new(0, None).unwrap(),
        )
        .has_rewrite_candidate(),
        "identity windows fold away"
    );
    assert!(
        node_window(
            ir::NodeAccessPlan::AllScan,
            logical::AccessWindowRange::new(2, Some(2)).unwrap(),
        )
        .has_rewrite_candidate(),
        "empty windows fold to empty access"
    );
    assert!(
        node_window(
            point_ids(vec![1, 2, 3]),
            logical::AccessWindowRange::new(1, Some(2)).unwrap()
        )
        .has_rewrite_candidate(),
        "point-id windows may slice the source"
    );
    assert!(
        node_window(
            point_ids(vec![1, 2, 3]),
            logical::AccessWindowRange::new(0, Some(3)).unwrap()
        )
        .has_rewrite_candidate(),
        "prefix windows covering a bounded source fold away"
    );
    assert!(
        node_window(
            vector_search(10),
            logical::AccessWindowRange::new(1, Some(3)).unwrap()
        )
        .has_rewrite_candidate(),
        "bounded search windows may tighten the source prefix"
    );
}

#[test]
fn access_window_rewrite_candidate_rejects_ordinary_scan_windows() {
    assert!(!node_window(
        ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        },
        logical::AccessWindowRange::new(1, Some(3)).unwrap(),
    )
    .has_rewrite_candidate());
    assert!(!node_window(
        vector_search(10),
        logical::AccessWindowRange::new(1, None).unwrap()
    )
    .has_rewrite_candidate());
}

#[test]
fn access_order_predicates_cover_elision_and_range_direction_candidates() {
    let point_order = AccessOrder::new(
        node_access(point_ids(vec![1])),
        order_keys("age", helix_ast::traversal::Order::Asc),
    );
    assert!(point_order.has_order_elision_candidate());
    assert!(!point_order.has_range_direction_candidate());

    let opposite_range_order = AccessOrder::new(
        node_access(range_source(
            "age",
            helix_ast::index::RangeIndexDirection::Asc,
        )),
        order_keys("age", helix_ast::traversal::Order::Desc),
    );
    assert!(!opposite_range_order.has_order_elision_candidate());
    assert!(opposite_range_order.has_range_direction_candidate());

    let already_satisfied_range_order = AccessOrder::new(
        node_access(range_source(
            "age",
            helix_ast::index::RangeIndexDirection::Desc,
        )),
        order_keys("age", helix_ast::traversal::Order::Desc),
    );
    assert!(already_satisfied_range_order.has_order_elision_candidate());
    assert!(!already_satisfied_range_order.has_range_direction_candidate());

    let edge_opposite_range_order = AccessOrder::new(
        edge_access(edge_range_source(
            "weight",
            helix_ast::index::RangeIndexDirection::Desc,
        )),
        order_keys("weight", helix_ast::traversal::Order::Asc),
    );
    assert!(!edge_opposite_range_order.has_order_elision_candidate());
    assert!(edge_opposite_range_order.has_range_direction_candidate());

    let edge_satisfied_range_order = AccessOrder::new(
        edge_access(edge_range_source(
            "weight",
            helix_ast::index::RangeIndexDirection::Asc,
        )),
        order_keys("weight", helix_ast::traversal::Order::Asc),
    );
    assert!(edge_satisfied_range_order.has_order_elision_candidate());
    assert!(!edge_satisfied_range_order.has_range_direction_candidate());
}

#[test]
fn access_order_range_direction_candidate_rejects_non_matching_shapes() {
    let label_order = AccessOrder::new(
        node_access(ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        }),
        order_keys("age", helix_ast::traversal::Order::Desc),
    );
    let edge_scan_order = AccessOrder::new(
        edge_access(ir::EdgeAccessPlan::AllScan),
        order_keys("weight", helix_ast::traversal::Order::Asc),
    );
    let mismatched_property = AccessOrder::new(
        node_access(range_source(
            "score",
            helix_ast::index::RangeIndexDirection::Asc,
        )),
        order_keys("age", helix_ast::traversal::Order::Desc),
    );
    let multikey = AccessOrder::new(
        node_access(range_source(
            "age",
            helix_ast::index::RangeIndexDirection::Asc,
        )),
        multi_order_keys(),
    );

    assert!(!label_order.has_order_elision_candidate());
    assert!(!label_order.has_range_direction_candidate());
    assert!(!edge_scan_order.has_order_elision_candidate());
    assert!(!edge_scan_order.has_range_direction_candidate());
    assert!(!mismatched_property.has_order_elision_candidate());
    assert!(!mismatched_property.has_range_direction_candidate());
    assert!(!multikey.has_order_elision_candidate());
    assert!(!multikey.has_range_direction_candidate());
}

#[test]
fn access_distinct_noop_candidate_covers_unique_sources() {
    let point_distinct = AccessDistinct::new(node_access(point_ids(vec![1, 2, 3])));
    let singleton_search = AccessDistinct::new(node_access(vector_search(1)));
    let scan_distinct = AccessDistinct::new(node_access(ir::NodeAccessPlan::AllScan));

    assert!(point_distinct.has_noop_candidate());
    assert!(singleton_search.has_noop_candidate());
    assert!(!scan_distinct.has_noop_candidate());
}
