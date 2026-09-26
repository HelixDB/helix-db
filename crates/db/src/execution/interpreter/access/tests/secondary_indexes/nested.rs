//! Nested sets must preserve scope and apply limits after set membership.
use super::*;

#[tokio::test]
async fn nested_node_sets_filter_nulls_and_predicates_before_outer_limits() {
    let db = test_support::open_db("nested-node-secondary-sets").await;
    let mut ids = Vec::new();
    for (rank, optional) in [
        ("c", None),
        ("a", Some(PropertyValue::Null)),
        ("b", Some(PropertyValue::from("present"))),
    ] {
        let mut properties = vec![("rank", PropertyValue::from(rank))];
        properties.extend(optional.map(|value| ("optional", value)));
        ids.push(test_support::add_node_with_properties(&db, "User", properties).await);
    }
    test_support::add_node_with_properties(&db, "Other", vec![("rank", PropertyValue::from("a"))])
        .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        71,
        &[("c", ids[0]), ("a", ids[1]), ("b", ids[2])],
    )
    .await;
    let range = exec::ExecNodeSecondaryRangePlan {
        iteration: ir::RangeScanIteration::Forward,
        index: catalog::NodeRangeIndexMeta::new(test_support::name("node_range:User:rank:asc")),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "User",
            "rank",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };
    let nulls = exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
        exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
            key: catalog::ScopedPropertyKey::try_new("User", "optional").unwrap(),
        },
    );
    let predicate = exec::ExecNodeSecondarySetPlan::AuthoritativeScan(
        exec::ExecNodeAuthoritativeScanPredicate::Predicate(
            ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("rank", "a")).unwrap(),
        ),
    );
    let ordered = exec::ExecNodeSecondarySetPlan::OrderedIntersect {
        driver: range.clone(),
        filters: ir::AtLeast::from_one_and_rest(
            nulls.clone(),
            vec![exec::ExecNodeSecondarySetPlan::Range(range)],
        ),
    };
    // The nested ordered result is converted to set semantics at the union.
    // An outer limit must not truncate either input before membership is known.
    let union = exec::ExecNodeSecondarySetPlan::Union {
        driver: Box::new(ordered.clone()),
        rest: ir::AtLeast::from_one(nulls),
    };
    assert_eq!(
        run_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet { set: union.clone() }
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(ids[0]),
            ExecutionScalar::NodeId(ids[1])
        ])
    );
    assert_eq!(
        run_limited_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet { set: union.clone() },
            1
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(ids[0])])
    );
    let intersect = exec::ExecNodeSecondarySetPlan::Intersect {
        driver: Box::new(union),
        rest: ir::AtLeast::from_one(predicate),
    };
    assert_eq!(
        run_limited_node_access(
            &db,
            exec::ExecNodeAccessPlan::SecondarySet { set: intersect },
            1
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(ids[1])])
    );
    // The recursive contract itself also preserves the driver's order when
    // there is no unordered parent; this is used by whole-value consumers.
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    assert_eq!(
        ctx.node_secondary_set_ids(&ordered, None).await.unwrap(),
        vec![ids[1], ids[0]]
    );
    assert_eq!(
        ctx.node_secondary_set_ids(&ordered, properties::PositiveUsize::new(1))
            .await
            .unwrap(),
        vec![ids[1]]
    );
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn nested_edge_sets_keep_edge_scope_and_filter_before_outer_limits() {
    let db = test_support::open_db("nested-edge-secondary-sets").await;
    let from = test_support::add_user(&db, "from").await;
    let to = test_support::add_user(&db, "to").await;
    let mut ids = Vec::new();
    for (rank, optional) in [
        ("c", None),
        ("a", Some(PropertyValue::Null)),
        ("b", Some(PropertyValue::from("present"))),
    ] {
        let mut properties = vec![("rank", PropertyValue::from(rank))];
        properties.extend(optional.map(|value| ("optional", value)));
        ids.push(
            test_support::add_edge_with_properties(&db, from, to, "FOLLOWS", properties).await,
        );
    }
    test_support::add_edge_with_properties(
        &db,
        from,
        to,
        "OTHER",
        vec![("rank", PropertyValue::from("a"))],
    )
    .await;
    seed_active_secondary_generation(
        &db,
        SecondaryIndexDefinition::edge_range("FOLLOWS", "rank").unwrap(),
        72,
        &[("c", ids[0]), ("a", ids[1]), ("b", ids[2])],
    )
    .await;
    let range = exec::ExecEdgeSecondaryRangePlan {
        iteration: ir::RangeScanIteration::Forward,
        index: catalog::EdgeRangeIndexMeta::new(test_support::name("edge_range:FOLLOWS:rank:asc")),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "FOLLOWS",
            "rank",
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range: ir::IndexRange::All,
    };
    let nulls = exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
        exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "optional").unwrap(),
        },
    );
    let predicate = exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(
        exec::ExecEdgeAuthoritativeScanPredicate::Predicate(
            ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("rank", "a")).unwrap(),
        ),
    );
    let ordered = exec::ExecEdgeSecondarySetPlan::OrderedIntersect {
        driver: range.clone(),
        filters: ir::AtLeast::from_one_and_rest(
            nulls.clone(),
            vec![exec::ExecEdgeSecondarySetPlan::Range(range)],
        ),
    };
    let union = exec::ExecEdgeSecondarySetPlan::Union {
        driver: Box::new(ordered.clone()),
        rest: ir::AtLeast::from_one(nulls),
    };
    assert_eq!(
        run_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::SecondarySet { set: union.clone() }
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(ids[0]),
            ExecutionScalar::EdgeId(ids[1])
        ])
    );
    assert_eq!(
        run_limited_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::SecondarySet { set: union.clone() },
            1
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(ids[0])])
    );
    let intersect = exec::ExecEdgeSecondarySetPlan::Intersect {
        driver: Box::new(union),
        rest: ir::AtLeast::from_one(predicate),
    };
    assert_eq!(
        run_limited_edge_access(
            &db,
            exec::ExecEdgeAccessPlan::SecondarySet { set: intersect },
            1
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(ids[1])])
    );
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    assert_eq!(
        ctx.edge_secondary_set_ids(&ordered, None).await.unwrap(),
        vec![ids[1], ids[0]]
    );
    assert_eq!(
        ctx.edge_secondary_set_ids(&ordered, properties::PositiveUsize::new(1))
            .await
            .unwrap(),
        vec![ids[1]]
    );
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}
