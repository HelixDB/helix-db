use crate::catalog::{
    EdgeEqualityIndexMeta, EdgeRangeIndexMeta, EdgeSearchIndexKey, NodeEqualityIndexMeta,
    NodeRangeIndexMeta, NodeSearchIndexKey, ScopedPropertyKey,
};
use crate::planning::tests::support::*;
use crate::planning::{
    edge_cardinality, hard_cardinality_upper_bound, intersection_cardinality, node_cardinality,
    union_cardinality, CardinalityEstimate,
};

fn node_equality_plan_with_value(
    label: &str,
    property: &str,
    unique: bool,
    value: IndexValue,
) -> NodeAccessPlan {
    let index = NodeEqualityIndexMeta::new(
        NonEmptyString::new(format!("node_eq:{label}:{property}")).unwrap(),
    )
    .with_uniqueness(if unique {
        IndexUniqueness::Unique
    } else {
        IndexUniqueness::NonUnique
    });

    NodeAccessPlan::EqualityIndex {
        index,
        key: ScopedPropertyKey::try_new(label, property).unwrap(),
        value,
    }
}

fn node_equality_plan(label: &str, property: &str, unique: bool) -> NodeAccessPlan {
    node_equality_plan_with_value(
        label,
        property,
        unique,
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::from("value")).unwrap()),
    )
}

fn edge_equality_plan(label: &str, property: &str) -> EdgeAccessPlan {
    EdgeAccessPlan::EqualityIndex {
        index: EdgeEqualityIndexMeta::new(
            NonEmptyString::new(format!("edge_eq:{label}:{property}")).unwrap(),
        ),
        key: ScopedPropertyKey::try_new(label, property).unwrap(),
        value: IndexValue::Literal(
            SecondaryIndexLiteral::new(PropertyValue::from("value")).unwrap(),
        ),
    }
}

#[test]
fn hard_cardinality_upper_bounds_cover_plan_internal_ceiling_contracts() {
    let node_points = PhysicalOp::NodeAccess(NodeAccessPlan::PointIds {
        ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(1, vec![2, 3])).unwrap(),
    });
    let edge_point_access = EdgeAccessPlan::PointIds {
        ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap(),
    };
    let edge_points = PhysicalOp::EdgeAccess(edge_point_access.clone());
    let dynamic_bound = StreamBoundPlan::new(StreamBound::expr(Expr::param("limit"))).unwrap();
    let dynamic_range = StreamRangePlan::new(
        StreamBound::expr(Expr::param("start")),
        StreamBound::from(5usize),
    )
    .unwrap();

    assert_eq!(hard_cardinality_upper_bound(&node_points), Some(3));
    assert_eq!(hard_cardinality_upper_bound(&edge_points), Some(2));
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Limit {
            input: Box::new(node_points.clone()),
            count: StreamBoundPlan::Literal(2),
        }),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Limit {
            input: Box::new(node_points.clone()),
            count: dynamic_bound.clone(),
        }),
        Some(3)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::TopN {
            input: Box::new(node_points.clone()),
            keys: OrderKeys::from(OrderKey {
                property: NonEmptyString::new("age").unwrap(),
                order: Order::Asc,
            }),
            count: NonZeroUsize::new(2).unwrap(),
        }),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Skip {
            input: Box::new(node_points.clone()),
            count: StreamBoundPlan::Literal(2),
        }),
        Some(1)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Skip {
            input: Box::new(node_points.clone()),
            count: dynamic_bound,
        }),
        Some(3)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Range {
            input: Box::new(node_points.clone()),
            range: StreamRangePlan::new(StreamBound::from(1usize), StreamBound::from(3usize))
                .unwrap(),
        }),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Range {
            input: Box::new(node_points.clone()),
            range: StreamRangePlan::new(StreamBound::from(5usize), StreamBound::from(8usize))
                .unwrap(),
        }),
        Some(0)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Range {
            input: Box::new(node_points.clone()),
            range: StreamRangePlan::new(
                StreamBound::from(2usize),
                StreamBound::expr(Expr::param("end")),
            )
            .unwrap(),
        }),
        Some(1)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Range {
            input: Box::new(PhysicalOp::NodeAccess(NodeAccessPlan::AllScan)),
            range: StreamRangePlan::new(
                StreamBound::expr(Expr::param("start")),
                StreamBound::from(1usize),
            )
            .unwrap(),
        }),
        Some(1)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Range {
            input: Box::new(node_points.clone()),
            range: StreamRangePlan::new(
                StreamBound::expr(Expr::param("start")),
                StreamBound::expr(Expr::param("end")),
            )
            .unwrap(),
        }),
        Some(3)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Range {
            input: Box::new(node_points.clone()),
            range: dynamic_range,
        }),
        Some(3)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Variable(VariablePlan::Stream {
            input: Box::new(node_points.clone()),
            op: StreamVariableOp::Store(NonEmptyString::new("saved").unwrap()),
        })),
        Some(3)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Variable(VariablePlan::Stream {
            input: Box::new(node_points.clone()),
            op: StreamVariableOp::Select(NonEmptyString::new("saved").unwrap()),
        })),
        None
    );
    for projection in [ProjectionPlan::Count, ProjectionPlan::Exists] {
        assert_eq!(
            hard_cardinality_upper_bound(&PhysicalOp::Project {
                input: Box::new(node_points.clone()),
                projection,
            }),
            Some(1)
        );
    }
    let project_items = ProjectionItems::new(AtLeast::<_, 1>::from_one(ProjectionItem::Property {
        source: NonEmptyString::new("name").unwrap(),
        alias: NonEmptyString::new("name").unwrap(),
    }))
    .unwrap();
    let binding_items =
        BindingProjectionItems::new(AtLeast::<_, 1>::from_one(BindingProjectionPlan::Property {
            target: BindingTargetPlan::Current,
            source: NonEmptyString::new("name").unwrap(),
            alias: NonEmptyString::new("name").unwrap(),
        }))
        .unwrap();
    for projection in [
        ProjectionPlan::Id,
        ProjectionPlan::Label,
        ProjectionPlan::ValueMap(PropertySelection::All),
        ProjectionPlan::Project(project_items),
        ProjectionPlan::ProjectBindings {
            projections: binding_items,
            dedup: ProjectionDedupMode::Distinct,
        },
    ] {
        assert_eq!(
            hard_cardinality_upper_bound(&PhysicalOp::Project {
                input: Box::new(node_points.clone()),
                projection,
            }),
            Some(3)
        );
    }
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Project {
            input: Box::new(edge_points.clone()),
            projection: ProjectionPlan::EdgeProperties,
        }),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::Project {
            input: Box::new(node_points.clone()),
            projection: ProjectionPlan::Values(
                PropertyNames::new(AtLeast::<_, 1>::from_one(
                    NonEmptyString::new("name").unwrap(),
                ))
                .unwrap(),
            ),
        }),
        None
    );
    for aggregate in [
        AggregatePlan::Group(NonEmptyString::new("tenant_id").unwrap()),
        AggregatePlan::GroupCount(NonEmptyString::new("status").unwrap()),
        AggregatePlan::AggregateBy {
            function: AggregateFunction::Mean,
            property: NonEmptyString::new("score").unwrap(),
        },
    ] {
        assert_eq!(
            hard_cardinality_upper_bound(&PhysicalOp::Aggregate {
                input: Box::new(node_points.clone()),
                aggregate,
            }),
            Some(1)
        );
    }
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::NodeAccess(NodeAccessPlan::Empty)),
        Some(0)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::NodeAccess(NodeAccessPlan::Intersect(
            AtLeast::<_, 2>::from_pair(
                NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::AllScan),
                NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::PointIds {
                    ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(11, vec![13])).unwrap(),
                }),
            ),
        ))),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::NodeAccess(NodeAccessPlan::Union(AtLeast::<
            _,
            2,
        >::from_pair(
            NodeAccessSourcePlan::from_unfiltered(node_equality_plan("User", "id", true)),
            NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::PointIds {
                ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(17, vec![19])).unwrap(),
            }),
        ),))),
        Some(3)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::NodeAccess(NodeAccessPlan::ScanThenFilter {
            source: NodeAccessSourcePlan::from_unfiltered(node_equality_plan("User", "id", true)),
            residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
        })),
        Some(1)
    );
    for value in [
        IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::Null).unwrap()),
        IndexValue::Param(NonEmptyString::new("id").unwrap()),
    ] {
        assert_eq!(
            hard_cardinality_upper_bound(&PhysicalOp::NodeAccess(node_equality_plan_with_value(
                "User", "id", true, value
            ))),
            None
        );
    }
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::EdgeAccess(EdgeAccessPlan::Empty)),
        Some(0)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::EdgeAccess(EdgeAccessPlan::Intersect(
            AtLeast::<_, 2>::from_pair(
                EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::AllScan),
                EdgeAccessSourcePlan::from_unfiltered(edge_point_access.clone()),
            ),
        ))),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::EdgeAccess(EdgeAccessPlan::ScanThenFilter {
            source: EdgeAccessSourcePlan::from_unfiltered(edge_point_access.clone()),
            residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
        })),
        Some(2)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::EdgeAccess(EdgeAccessPlan::Union(AtLeast::<
            _,
            2,
        >::from_pair(
            EdgeAccessSourcePlan::from_unfiltered(edge_point_access.clone()),
            EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::PointIds {
                ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(11, vec![13])).unwrap(),
            }),
        ),))),
        Some(4)
    );
    assert_eq!(
        hard_cardinality_upper_bound(&PhysicalOp::EdgeAccess(EdgeAccessPlan::Union(AtLeast::<
            _,
            2,
        >::from_pair(
            EdgeAccessSourcePlan::from_unfiltered(edge_equality_plan("FOLLOWS", "status")),
            EdgeAccessSourcePlan::from_unfiltered(edge_point_access),
        ),))),
        None
    );
}

#[test]
fn node_cardinality_covers_known_unknown_and_nested_sources() {
    let ctx = PlannerContext {
        stats: StatsSnapshot::default()
            .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 100)
            .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "email").unwrap(), 5)
            .with_node_range_cardinality(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
                40,
            ),
        ..PlannerContext::default()
    };
    let range = NodeAccessPlan::RangeIndex {
        index: NodeRangeIndexMeta::new(NonEmptyString::new("node_range:User:age").unwrap()),
        key: ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
            ),
        },
    };

    assert_eq!(
        node_cardinality(&ctx, &NodeAccessPlan::Empty),
        CardinalityEstimate::Known(0)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::PointIds {
                ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(1, vec![2, 3])).unwrap(),
            }
        ),
        CardinalityEstimate::Known(3)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::LabelScan {
                label: NonEmptyString::new("User").unwrap()
            }
        ),
        CardinalityEstimate::Known(100)
    );
    assert_eq!(
        node_cardinality(&ctx, &node_equality_plan("User", "id", true)),
        CardinalityEstimate::Known(1)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &node_equality_plan_with_value(
                "User",
                "id",
                true,
                IndexValue::Literal(SecondaryIndexLiteral::new(PropertyValue::Null).unwrap())
            )
        ),
        CardinalityEstimate::Unknown
    );
    assert_eq!(
        node_cardinality(&ctx, &node_equality_plan("User", "email", false)),
        CardinalityEstimate::Known(5)
    );
    assert_eq!(
        node_cardinality(&ctx, &range),
        CardinalityEstimate::Known(40)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(
                NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
                    label: NonEmptyString::new("User").unwrap(),
                }),
                NodeAccessSourcePlan::from_unfiltered(node_equality_plan("User", "email", false)),
            )),
        ),
        CardinalityEstimate::Known(5)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::Union(AtLeast::<_, 2>::from_pair(
                NodeAccessSourcePlan::from_unfiltered(NodeAccessPlan::LabelScan {
                    label: NonEmptyString::new("User").unwrap(),
                }),
                NodeAccessSourcePlan::from_unfiltered(node_equality_plan("User", "email", false)),
            )),
        ),
        CardinalityEstimate::Known(105)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::ScanThenFilter {
                source: NodeAccessSourcePlan::from_unfiltered(node_equality_plan(
                    "User", "email", false
                )),
                residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
            },
        ),
        CardinalityEstimate::Known(5)
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::FromParam {
                param: NonEmptyString::new("ids").unwrap(),
            },
        ),
        CardinalityEstimate::Unknown
    );
    assert_eq!(
        node_cardinality(
            &ctx,
            &NodeAccessPlan::FromVar {
                variable: NonEmptyString::new("cached").unwrap(),
            },
        ),
        CardinalityEstimate::Unknown
    );
}

#[test]
fn search_and_bound_cardinality_use_literal_k_only() {
    let text = NodeAccessPlan::TextSearch {
        key: NodeSearchIndexKey::try_new("Doc", "body").unwrap(),
        index: SearchIndexPlan {
            index_id: NonEmptyString::new("text:Doc:body").unwrap(),
            tenant: SearchTenantPlan::Unscoped,
        },
        query_text: TextQueryInputPlan::Text(NonEmptyString::new("planner").unwrap()),
        k: SearchLimitPlan::new(StreamBound::from(9usize)).unwrap(),
    };
    let node_vector = NodeAccessPlan::VectorSearch {
        key: NodeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
        index: SearchIndexPlan {
            index_id: NonEmptyString::new("vector:Doc:embedding").unwrap(),
            tenant: SearchTenantPlan::Unscoped,
        },
        query_vector: VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1, 0.2]).unwrap()),
        k: SearchLimitPlan::new(StreamBound::expr(Expr::param("k"))).unwrap(),
    };
    let vector = EdgeAccessPlan::VectorSearch {
        key: EdgeSearchIndexKey::try_new("MENTIONS", "embedding").unwrap(),
        index: SearchIndexPlan {
            index_id: NonEmptyString::new("vector:MENTIONS:embedding").unwrap(),
            tenant: SearchTenantPlan::Unscoped,
        },
        query_vector: VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1, 0.2]).unwrap()),
        k: SearchLimitPlan::new(StreamBound::expr(Expr::param("k"))).unwrap(),
    };
    let edge_text = EdgeAccessPlan::TextSearch {
        key: EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap(),
        index: SearchIndexPlan {
            index_id: NonEmptyString::new("text:MENTIONS:body").unwrap(),
            tenant: SearchTenantPlan::Unscoped,
        },
        query_text: TextQueryInputPlan::Text(NonEmptyString::new("planner").unwrap()),
        k: SearchLimitPlan::new(StreamBound::from(7usize)).unwrap(),
    };

    assert_eq!(
        node_cardinality(&PlannerContext::default(), &text),
        CardinalityEstimate::Known(9)
    );
    assert_eq!(
        node_cardinality(&PlannerContext::default(), &node_vector),
        CardinalityEstimate::Unknown
    );
    assert_eq!(
        edge_cardinality(&PlannerContext::default(), &vector),
        CardinalityEstimate::Unknown
    );
    assert_eq!(
        edge_cardinality(&PlannerContext::default(), &edge_text),
        CardinalityEstimate::Known(7)
    );
    let literal_bound = StreamBoundPlan::new(StreamBound::from(4usize)).unwrap();
    let expression_bound = StreamBoundPlan::new(StreamBound::expr(Expr::param("limit"))).unwrap();
    // Literal stream bounds have a known estimate; expression bounds are runtime-only.
    assert_eq!(
        match literal_bound {
            StreamBoundPlan::Literal(value) => CardinalityEstimate::Known(value as u64),
            StreamBoundPlan::Expr(_) => CardinalityEstimate::Unknown,
        },
        CardinalityEstimate::Known(4)
    );
    assert_eq!(
        match expression_bound {
            StreamBoundPlan::Literal(value) => CardinalityEstimate::Known(value as u64),
            StreamBoundPlan::Expr(_) => CardinalityEstimate::Unknown,
        },
        CardinalityEstimate::Unknown
    );
}

#[test]
fn edge_cardinality_covers_stats_and_unknown_sources() {
    let ctx = PlannerContext {
        stats: StatsSnapshot::default()
            .with_edge_label_cardinality(NonEmptyString::new("FOLLOWS").unwrap(), 1_000)
            .with_edge_eq_cardinality(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(), 80)
            .with_edge_range_cardinality(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
                200,
            ),
        ..PlannerContext::default()
    };
    let range = EdgeAccessPlan::RangeIndex {
        index: EdgeRangeIndexMeta::new(NonEmptyString::new("edge_range:FOLLOWS:since").unwrap()),
        key: ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
            .unwrap(),
        range: IndexRange::Lower {
            lower: IndexBound::Inclusive(
                RangeIndexValue::literal(PropertyValue::from(2020)).unwrap(),
            ),
        },
    };

    assert_eq!(
        edge_cardinality(&ctx, &EdgeAccessPlan::Empty),
        CardinalityEstimate::Known(0)
    );
    assert_eq!(
        edge_cardinality(
            &ctx,
            &EdgeAccessPlan::PointIds {
                ids: ElementIds::new(AtLeast::<_, 1>::from_one_and_rest(1, vec![2])).unwrap(),
            }
        ),
        CardinalityEstimate::Known(2)
    );
    assert_eq!(
        edge_cardinality(
            &ctx,
            &EdgeAccessPlan::LabelScan {
                label: NonEmptyString::new("FOLLOWS").unwrap(),
            },
        ),
        CardinalityEstimate::Known(1_000)
    );
    assert_eq!(
        edge_cardinality(&ctx, &edge_equality_plan("FOLLOWS", "status")),
        CardinalityEstimate::Known(80)
    );
    assert_eq!(
        edge_cardinality(&ctx, &range),
        CardinalityEstimate::Known(200)
    );
    assert_eq!(
        edge_cardinality(
            &ctx,
            &EdgeAccessPlan::Intersect(AtLeast::<_, 2>::from_pair(
                EdgeAccessSourcePlan::from_unfiltered(EdgeAccessPlan::LabelScan {
                    label: NonEmptyString::new("FOLLOWS").unwrap(),
                }),
                EdgeAccessSourcePlan::from_unfiltered(edge_equality_plan("FOLLOWS", "status")),
            )),
        ),
        CardinalityEstimate::Known(80)
    );
    assert_eq!(
        edge_cardinality(
            &ctx,
            &EdgeAccessPlan::ScanThenFilter {
                source: EdgeAccessSourcePlan::from_unfiltered(edge_equality_plan(
                    "FOLLOWS", "status"
                )),
                residual: PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
            },
        ),
        CardinalityEstimate::Known(80)
    );
    assert_eq!(
        edge_cardinality(
            &ctx,
            &EdgeAccessPlan::FromVar {
                variable: NonEmptyString::new("cached_edges").unwrap(),
            },
        ),
        CardinalityEstimate::Unknown
    );
}

#[test]
fn aggregate_cardinality_helpers_handle_known_and_unknown_inputs() {
    assert_eq!(
        intersection_cardinality([
            CardinalityEstimate::Known(20),
            CardinalityEstimate::Known(3),
            CardinalityEstimate::Unknown,
        ]),
        CardinalityEstimate::Known(3)
    );
    assert_eq!(
        intersection_cardinality([CardinalityEstimate::Unknown]),
        CardinalityEstimate::Unknown
    );
    assert_eq!(
        union_cardinality([
            CardinalityEstimate::Known(u64::MAX),
            CardinalityEstimate::Known(1),
        ]),
        CardinalityEstimate::Known(u64::MAX)
    );
    assert_eq!(
        union_cardinality([CardinalityEstimate::Known(10), CardinalityEstimate::Unknown,]),
        CardinalityEstimate::Unknown
    );
}
