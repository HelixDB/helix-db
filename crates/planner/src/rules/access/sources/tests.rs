use super::*;
use helix_ast::index::RangeIndexDirection;

use crate::{catalog, ir, logical};

fn literal(value: i64) -> ir::IndexValue {
    ir::IndexValue::Literal(
        ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(value)).unwrap(),
    )
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

fn ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
}

fn node_source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::new(plan).unwrap()
}

fn edge_source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
    ir::EdgeAccessSourcePlan::new(plan).unwrap()
}

fn literal_limit(value: usize) -> ir::SearchLimitPlan {
    ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::new(value).unwrap())
}

fn dynamic_limit() -> ir::SearchLimitPlan {
    ir::SearchLimitPlan::Expr(
        ir::SearchLimitExprPlan::new(helix_ast::expr::Expr::param("k")).unwrap(),
    )
}

fn search_index(name_value: &str) -> ir::SearchIndexPlan {
    ir::SearchIndexPlan {
        index_id: name(name_value),
        tenant: ir::SearchTenantPlan::Unscoped,
    }
}

fn vector_query() -> ir::VectorQueryInputPlan {
    ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
        helix_ast::value::PropertyValue::F32Array(vec![0.5]),
    ))
    .unwrap()
}

fn text_query() -> ir::TextQueryInputPlan {
    ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
        helix_ast::value::PropertyValue::from("great"),
    ))
    .unwrap()
}

#[test]
fn common_label_recurses_through_homogeneous_sets() {
    let user = name("User");
    let node = node_source(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        node_source(ir::NodeAccessPlan::LabelScan {
            label: user.clone(),
        }),
        node_source(ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("user_email").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            value: literal(1),
        }),
    )));
    let node_path = logical::AccessPath::Node(logical::NodeAccessPath::new(node.clone()));

    assert_eq!(node_source_common_label(&node), Some(&user));
    assert_eq!(access_path_common_label(&node_path), Some(&user));

    let mixed = edge_source(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        edge_source(ir::EdgeAccessPlan::LabelScan {
            label: name("LIKES"),
        }),
        edge_source(ir::EdgeAccessPlan::LabelScan {
            label: name("KNOWS"),
        }),
    )));
    assert!(edge_source_common_label(&mixed).is_none());
}

#[test]
fn direct_label_helpers_cover_index_and_search_families() {
    let user = name("User");
    let likes = name("LIKES");

    for plan in [
        ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        },
        ir::NodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
            index: search_index("user_embedding"),
            query_vector: vector_query(),
            k: literal_limit(3),
        },
        ir::NodeAccessPlan::TextSearch {
            key: catalog::NodeSearchIndexKey::try_new("User", "bio").unwrap(),
            index: search_index("user_bio"),
            query_text: text_query(),
            k: literal_limit(3),
        },
    ] {
        assert_eq!(plan.direct_label(), Some(&user));
    }

    for plan in [
        ir::EdgeAccessPlan::EqualityIndex {
            index: catalog::EdgeEqualityIndexMeta::try_new("likes_weight").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "weight").unwrap(),
            value: literal(1),
        },
        ir::EdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::try_new("likes_created").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "created",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        },
        ir::EdgeAccessPlan::VectorSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
            index: search_index("likes_embedding"),
            query_vector: vector_query(),
            k: literal_limit(3),
        },
        ir::EdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "comment").unwrap(),
            index: search_index("likes_comment"),
            query_text: text_query(),
            k: literal_limit(3),
        },
    ] {
        assert_eq!(plan.direct_label(), Some(&likes));
    }

    assert!(ir::NodeAccessPlan::AllScan.direct_label().is_none());
    assert!(ir::EdgeAccessPlan::AllScan.direct_label().is_none());
}

#[test]
fn path_helpers_preserve_residual_free_boundaries() {
    let AccessPathFromPlan::Access(node_path) =
        node_access_path_from_plan(ir::NodeAccessPlan::Empty)
    else {
        panic!("empty node access should be residual-free");
    };
    assert!(access_path_is_direct_empty(&node_path));
    assert!(matches!(
        empty_access_path_like(&node_path),
        logical::AccessPath::Node(path)
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::Empty)
    ));

    let source = node_source(ir::NodeAccessPlan::AllScan);
    let filtered = ir::NodeAccessPlan::ScanThenFilter {
        source,
        residual: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    };
    assert_eq!(
        node_access_path_from_plan(filtered),
        AccessPathFromPlan::NotResidualFree
    );

    let AccessPathFromPlan::Access(edge_path) =
        edge_access_path_from_plan(ir::EdgeAccessPlan::Empty)
    else {
        panic!("empty edge access should be residual-free");
    };
    assert!(access_path_is_direct_empty(&edge_path));
    assert!(matches!(
        empty_access_path_like(&edge_path),
        logical::AccessPath::Edge(path)
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::Empty)
    ));
}

#[test]
fn hard_cardinality_bounds_cover_point_unique_search_and_sets() {
    let unique = node_source(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new("user_email")
            .unwrap()
            .with_uniqueness(catalog::IndexUniqueness::Unique),
        key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
        value: literal(1),
    });
    assert_eq!(node_source_hard_cardinality_upper_bound(&unique), Some(1));

    let point = edge_source(ir::EdgeAccessPlan::PointIds {
        ids: ids(vec![10, 20, 30]),
    });
    let search = edge_source(ir::EdgeAccessPlan::TextSearch {
        key: catalog::EdgeSearchIndexKey::try_new("LIKES", "comment").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: name("likes_comment"),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_text: ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
            helix_ast::value::PropertyValue::from("great"),
        ))
        .unwrap(),
        k: literal_limit(5),
    });
    let union = edge_source(ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        point.clone(),
        search.clone(),
    )));
    let intersection = edge_source(ir::EdgeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(point, search),
    ));

    assert_eq!(edge_source_hard_cardinality_upper_bound(&union), Some(8));
    assert_eq!(
        edge_source_hard_cardinality_upper_bound(&intersection),
        Some(3)
    );
    assert!(
        node_source_hard_cardinality_upper_bound(&node_source(ir::NodeAccessPlan::AllScan))
            .is_none()
    );

    let dynamic_search = node_source(ir::NodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
        index: search_index("user_embedding"),
        query_vector: vector_query(),
        k: dynamic_limit(),
    });
    assert!(node_source_hard_cardinality_upper_bound(&dynamic_search).is_none());

    let unknown_union = node_source(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        node_source(ir::NodeAccessPlan::PointIds { ids: ids(vec![1]) }),
        node_source(ir::NodeAccessPlan::AllScan),
    )));
    assert!(node_source_hard_cardinality_upper_bound(&unknown_union).is_none());
}

#[test]
fn dedupe_preserves_first_occurrence_order_and_reports_changes() {
    let first = node_source(ir::NodeAccessPlan::LabelScan {
        label: name("User"),
    });
    let duplicate = first.clone();
    let second = node_source(ir::NodeAccessPlan::LabelScan {
        label: name("Account"),
    });
    let mut plans = vec![first.clone(), second.clone(), duplicate];
    let mut changed = false;

    dedupe_node_sources(&mut plans, &mut changed);

    assert!(changed);
    assert_eq!(plans, vec![first, second]);

    changed = false;
    dedupe_node_sources(&mut plans, &mut changed);
    assert!(!changed);
}

#[test]
fn set_constructors_encode_empty_single_and_many_shapes() {
    assert!(matches!(
        node_union_from_sources(Vec::new()),
        ir::NodeAccessPlan::Empty
    ));
    assert!(matches!(
        node_intersection_from_sources(vec![node_source(ir::NodeAccessPlan::AllScan)]),
        ir::NodeAccessPlan::AllScan
    ));
    assert!(matches!(
        node_union_from_sources(vec![
            node_source(ir::NodeAccessPlan::LabelScan {
                label: name("User"),
            }),
            node_source(ir::NodeAccessPlan::LabelScan {
                label: name("Account"),
            }),
        ]),
        ir::NodeAccessPlan::Union(children) if children.as_ref().len() == 2
    ));
    assert!(matches!(
        edge_intersection_from_sources(vec![
            edge_source(ir::EdgeAccessPlan::LabelScan {
                label: name("LIKES"),
            }),
            edge_source(ir::EdgeAccessPlan::LabelScan {
                label: name("KNOWS"),
            }),
        ]),
        ir::EdgeAccessPlan::Intersect(children) if children.as_ref().len() == 2
    ));
    assert!(matches!(
        edge_union_from_sources(Vec::new()),
        ir::EdgeAccessPlan::Empty
    ));
    assert!(matches!(
        edge_union_from_sources(vec![edge_source(ir::EdgeAccessPlan::AllScan)]),
        ir::EdgeAccessPlan::AllScan
    ));
}
