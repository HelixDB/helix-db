use super::*;

#[test]
fn access_costs_and_hard_bounds_cover_access_shapes() {
    let profile = cost::StorageCostProfile {
        default_unknown_scan_rows: cost::EstimatedRows::rows(9),
        default_equality_index_rows: cost::EstimatedRows::rows(5),
        default_range_index_rows: cost::EstimatedRows::rows(9),
        ..cost::StorageCostProfile::default()
    };
    let key = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
    let range_key =
        catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
            .unwrap();
    let unique = catalog::NodeEqualityIndexMeta::try_new("user_email")
        .unwrap()
        .with_uniqueness(catalog::IndexUniqueness::Unique);

    let node_equality = ir::NodeAccessPlan::EqualityIndex {
        index: unique.clone(),
        key: key.clone(),
        value: index_value("alice"),
    };
    let non_unique_node_equality = ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new("user_email_non_unique").unwrap(),
        key: key.clone(),
        value: index_value("bob"),
    };
    let node_range = ir::NodeAccessPlan::RangeIndex {
        iteration: crate::ir::RangeScanIteration::Forward,
        index: catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
        key: range_key.clone(),
        range: lower_range(18),
    };
    let node_search = ir::NodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
        index: search_index_plan(),
        query_vector: ir::VectorQueryInputPlan::Vector(ir::SearchVector::new(vec![0.5]).unwrap()),
        k: literal_search_limit(3),
    };
    let filtered_node = ir::NodeAccessPlan::ScanThenFilter {
        source: node_source(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![1, 2]),
        }),
        residual: predicate(),
    };
    let union_node = ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        node_source(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![3]),
        }),
        node_source(ir::NodeAccessPlan::PointIds {
            ids: element_ids(vec![4, 5]),
        }),
    ));
    let intersect_node = ir::NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
        node_source(node_equality.clone()),
        node_source(node_range.clone()),
    ));

    assert_eq!(node_access_hard_upper_bound(&node_equality), Some(1));
    // A unique read is bounded by its indexed literals; null, and runtime
    // values that may bind null, read label rows and have no bound.
    let literal =
        |value: PropertyValue| ir::SecondaryIndexLiteral::new(value).expect("indexable literal");
    let unique_read = |value| ir::NodeAccessPlan::EqualityIndex {
        index: unique.clone(),
        key: key.clone(),
        value,
    };
    let literal_set = |values: Vec<PropertyValue>| {
        unique_read(ir::IndexValue::LiteralSet(
            ir::AtLeast::try_from_vec(values.into_iter().map(literal).collect()).unwrap(),
        ))
    };
    assert_eq!(
        node_access_hard_upper_bound(&literal_set(vec![
            PropertyValue::from("a"),
            PropertyValue::from("b"),
            PropertyValue::F64(f64::NAN),
        ])),
        Some(2)
    );
    assert_eq!(
        node_access_hard_upper_bound(&literal_set(vec![
            PropertyValue::from("a"),
            PropertyValue::Null,
        ])),
        None
    );
    assert_eq!(
        node_access_hard_upper_bound(&unique_read(ir::IndexValue::Literal(literal(
            PropertyValue::Null
        )))),
        None
    );
    assert_eq!(
        node_access_hard_upper_bound(&unique_read(ir::IndexValue::Param(name("email")))),
        None
    );
    assert_eq!(node_access_hard_upper_bound(&node_search), Some(3));
    assert_eq!(node_access_hard_upper_bound(&filtered_node), Some(2));
    assert_eq!(node_access_hard_upper_bound(&union_node), Some(3));
    assert_eq!(node_access_hard_upper_bound(&intersect_node), Some(1));
    assert_eq!(node_access_hard_upper_bound(&node_range), None);
    assert_eq!(
        node_access_hard_upper_bound(&non_unique_node_equality),
        None
    );
    assert_eq!(node_access_cost(&node_equality, &profile).object_reads, 2);
    assert_eq!(
        node_access_cost(&node_equality, &profile).authoritative_graph_reads,
        1
    );
    assert_eq!(
        node_access_cost(&non_unique_node_equality, &profile).object_reads,
        1
    );
    assert_eq!(node_access_cost(&node_search, &profile).range_nexts, 3);
    assert_eq!(node_access_cost(&node_range, &profile).range_nexts, 9);
    assert_eq!(
        node_access_cost(&node_range, &profile).authoritative_graph_reads,
        9
    );
    assert!(node_access_cost(&filtered_node, &profile).cpu_units >= 2);

    let edge_key = catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap();
    let edge_range_key =
        catalog::ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", RangeIndexDirection::Asc)
            .unwrap();
    let edge_equality = ir::EdgeAccessPlan::EqualityIndex {
        index: catalog::EdgeEqualityIndexMeta::try_new("edge_status").unwrap(),
        key: edge_key,
        value: index_value("active"),
    };
    let edge_range = ir::EdgeAccessPlan::RangeIndex {
        iteration: crate::ir::RangeScanIteration::Forward,
        index: catalog::EdgeRangeIndexMeta::try_new("edge_weight").unwrap(),
        key: edge_range_key,
        range: lower_range(1),
    };
    let edge_search = ir::EdgeAccessPlan::TextSearch {
        key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap(),
        index: search_index_plan(),
        query_text: ir::TextQueryInputPlan::Text(name("hello")),
        k: literal_search_limit(4),
    };
    let filtered_edge = ir::EdgeAccessPlan::ScanThenFilter {
        source: edge_source(ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![7, 8]),
        }),
        residual: predicate(),
    };
    let union_edge = ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        edge_source(ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![9]),
        }),
        edge_source(ir::EdgeAccessPlan::PointIds {
            ids: element_ids(vec![10, 11]),
        }),
    ));
    let intersect_edge = ir::EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
        edge_source(edge_equality.clone()),
        edge_source(edge_range.clone()),
    ));

    assert_eq!(edge_access_hard_upper_bound(&edge_search), Some(4));
    assert_eq!(edge_access_hard_upper_bound(&filtered_edge), Some(2));
    assert_eq!(edge_access_hard_upper_bound(&union_edge), Some(3));
    assert_eq!(edge_access_hard_upper_bound(&intersect_edge), None);
    assert_eq!(edge_access_hard_upper_bound(&edge_range), None);
    assert_eq!(edge_access_cost(&edge_equality, &profile).object_reads, 1);
    assert_eq!(edge_access_cost(&edge_search, &profile).range_nexts, 4);
    assert_eq!(edge_access_cost(&edge_range, &profile).range_nexts, 9);
    assert!(edge_access_cost(&filtered_edge, &profile).cpu_units >= 2);
}

#[test]
fn intersections_price_null_equalities_after_their_other_children() {
    // The executor verifies a null equality only against the rows the other
    // children keep, so it reads them first and the null leaf after them:
    // the slowest other child and the null scan add up.
    let profile = cost::StorageCostProfile::default();
    let equality = |property: &str, value: PropertyValue| {
        node_source(ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new(property).unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
            value: index_value(value),
        })
    };
    let others = [
        equality("status", PropertyValue::from("active")),
        equality("tier", PropertyValue::from("gold")),
    ];
    let with_null = ir::NodeAccessPlan::Intersect(
        ir::AtLeast::try_from_vec(
            others
                .iter()
                .cloned()
                .chain([equality("deleted_at", PropertyValue::Null)])
                .collect(),
        )
        .unwrap(),
    );
    let without_null =
        ir::NodeAccessPlan::Intersect(ir::AtLeast::try_from_vec(others.to_vec()).unwrap());
    let null_scan = profile.null_equality_scan(profile.default_unknown_scan_rows);
    assert!(
        node_access_cost(&with_null, &profile).latency
            >= node_access_cost(&without_null, &profile)
                .latency
                .saturating_add(null_scan.latency),
    );
}

#[test]
fn stream_delivered_properties_preserve_literal_window_lower_bounds() {
    let delivered = properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::new(3, Some(10)).unwrap(),
        ..properties::DeliveredProperties::default()
    };

    assert_eq!(
        limit_delivered_properties(delivered.clone(), Some(4)).cardinality,
        properties::CardinalityBounds::new(3, Some(4)).unwrap()
    );
    assert_eq!(
        skip_delivered_properties(delivered.clone(), Some(2)).cardinality,
        properties::CardinalityBounds::new(1, Some(8)).unwrap()
    );
    assert_eq!(
        range_delivered_properties(delivered, Some((2, 5))).cardinality,
        properties::CardinalityBounds::new(1, Some(3)).unwrap()
    );
}
