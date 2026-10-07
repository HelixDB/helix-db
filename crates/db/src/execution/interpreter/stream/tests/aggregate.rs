use super::support::*;

#[tokio::test]
async fn aggregate_rejects_index_lifecycle_values() {
    let db = test_support::open_db("stream-aggregate-index-lifecycle").await;
    let mut ctx =
        super::super::super::ExecutionContext::new(&db, context::ParamBindings::default());
    let lifecycle = ExecutionValue::IndexDdlReceipt(
        crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
            operation_id: crate::index_lifecycle::IndexOperationId::from_bytes([7; 16]).unwrap(),
        },
    );

    let error = ctx
        .aggregate(lifecycle, &ir::AggregatePlan::Group(name("status")))
        .await
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("aggregate cannot consume an index lifecycle value"));
}

#[tokio::test]
async fn aggregate_by_executes_scalar_aggregate_functions() {
    let db = test_support::open_db("stream-aggregate-by").await;
    let ids = [
        test_support::add_node_with_properties(
            &db,
            "Metric",
            vec![("score", PropertyValue::I64(10))],
        )
        .await,
        test_support::add_node_with_properties(
            &db,
            "Metric",
            vec![("score", PropertyValue::I64(20))],
        )
        .await,
        test_support::add_node_with_properties(
            &db,
            "Metric",
            vec![("score", PropertyValue::I64(30))],
        )
        .await,
        test_support::add_node_with_properties(
            &db,
            "Metric",
            vec![("score", PropertyValue::from("40"))],
        )
        .await,
        test_support::add_node_with_properties(&db, "Metric", vec![("name", "missing".into())])
            .await,
    ];
    let ids_param = name("ids");
    let score = name("score");

    async fn run(
        db: &crate::HelixDB,
        ids_param: &ir::NonEmptyString,
        ids: &[u64],
        score: &ir::NonEmptyString,
        function: AggregateFunction,
    ) -> ExecutionValue {
        let access_id = exec::ExecStepId::new(1).expect("positive step id");
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![
                node_access_step(1, ids_param.clone()),
                test_support::step(
                    2,
                    vec![access_id],
                    exec::ExecOp::Aggregate {
                        aggregate: ir::AggregatePlan::AggregateBy {
                            function,
                            property: score.clone(),
                        },
                    },
                ),
            ],
            2,
        );
        db.execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param.clone(), ids_value(ids)),
        )
        .await
        .expect("aggregate executes")
        .last
        .expect("aggregate step returns a value")
    }

    assert_eq!(
        run(&db, &ids_param, &ids, &score, AggregateFunction::Count).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "score_Count".to_string(),
            DbPropertyValue::F64(4.0)
        )]))])
    );
    assert_eq!(
        run(&db, &ids_param, &ids, &score, AggregateFunction::Sum).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "score_Sum".to_string(),
            DbPropertyValue::F64(100.0)
        )]))])
    );
    assert_eq!(
        run(&db, &ids_param, &ids, &score, AggregateFunction::Mean).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "score_Mean".to_string(),
            DbPropertyValue::F64(25.0)
        )]))])
    );
    assert_eq!(
        run(&db, &ids_param, &ids, &score, AggregateFunction::Min).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "score_Min".to_string(),
            DbPropertyValue::F64(10.0)
        )]))])
    );
    assert_eq!(
        run(&db, &ids_param, &ids, &score, AggregateFunction::Max).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "score_Max".to_string(),
            DbPropertyValue::F64(40.0)
        )]))])
    );
}

#[tokio::test]
async fn group_and_group_count_return_property_rows() {
    let db = test_support::open_db("stream-group-count").await;
    let open_1 =
        test_support::add_node_with_properties(&db, "Ticket", vec![("status", "open".into())])
            .await;
    let closed =
        test_support::add_node_with_properties(&db, "Ticket", vec![("status", "closed".into())])
            .await;
    let open_2 =
        test_support::add_node_with_properties(&db, "Ticket", vec![("status", "open".into())])
            .await;
    let missing =
        test_support::add_node_with_properties(&db, "Ticket", vec![("owner", "ops".into())]).await;
    let ids = [open_1, closed, open_2, missing];
    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Aggregate {
                    aggregate: ir::AggregatePlan::GroupCount(name("status")),
                },
            ),
        ],
        2,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param.clone(), ids_value(&ids)),
        )
        .await
        .expect("group count executes")
        .last
        .expect("aggregate step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Object(BTreeMap::from([
                ("status".to_string(), DbPropertyValue::Null,),
                ("count".to_string(), DbPropertyValue::I64(1)),
            ])),
            ExecutionScalar::Object(BTreeMap::from([
                (
                    "status".to_string(),
                    DbPropertyValue::String("closed".to_string()),
                ),
                ("count".to_string(), DbPropertyValue::I64(1)),
            ])),
            ExecutionScalar::Object(BTreeMap::from([
                (
                    "status".to_string(),
                    DbPropertyValue::String("open".to_string())
                ),
                ("count".to_string(), DbPropertyValue::I64(2)),
            ])),
        ])
    );

    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Aggregate {
                    aggregate: ir::AggregatePlan::Group(name("status")),
                },
            ),
        ],
        2,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param, ids_value(&ids)),
        )
        .await
        .expect("group executes")
        .last
        .expect("aggregate step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Object(BTreeMap::from([
                ("status".to_string(), DbPropertyValue::Null),
                ("count".to_string(), DbPropertyValue::I64(1)),
                (
                    "ids".to_string(),
                    DbPropertyValue::I64Array(vec![missing as i64])
                ),
            ])),
            ExecutionScalar::Object(BTreeMap::from([
                (
                    "status".to_string(),
                    DbPropertyValue::String("closed".to_string()),
                ),
                ("count".to_string(), DbPropertyValue::I64(1)),
                (
                    "ids".to_string(),
                    DbPropertyValue::I64Array(vec![closed as i64])
                ),
            ])),
            ExecutionScalar::Object(BTreeMap::from([
                (
                    "status".to_string(),
                    DbPropertyValue::String("open".to_string())
                ),
                ("count".to_string(), DbPropertyValue::I64(2)),
                (
                    "ids".to_string(),
                    DbPropertyValue::I64Array(vec![open_1 as i64, open_2 as i64])
                ),
            ])),
        ])
    );
}

#[tokio::test]
async fn group_identity_is_canonical_and_typed() {
    let db = test_support::open_db("stream-group-canonical-identity").await;
    let as_i64 = test_support::add_node_with_properties(
        &db,
        "Ticket",
        vec![("age", PropertyValue::I64(42))],
    )
    .await;
    let as_f64 = test_support::add_node_with_properties(
        &db,
        "Ticket",
        vec![("age", PropertyValue::F64(42.0))],
    )
    .await;
    let as_string = test_support::add_node_with_properties(
        &db,
        "Ticket",
        vec![("age", PropertyValue::from("42"))],
    )
    .await;
    let ids = [as_i64, as_f64, as_string];
    let ids_param = name("ids");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Aggregate {
                    aggregate: ir::AggregatePlan::GroupCount(name("age")),
                },
            ),
        ],
        2,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default().with_value(ids_param, ids_value(&ids)),
        )
        .await
        .expect("group count executes")
        .last
        .expect("aggregate step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Object(BTreeMap::from([
                ("age".to_string(), DbPropertyValue::I64(42)),
                ("count".to_string(), DbPropertyValue::I64(2)),
            ])),
            ExecutionScalar::Object(BTreeMap::from([
                ("age".to_string(), DbPropertyValue::String("42".to_string())),
                ("count".to_string(), DbPropertyValue::I64(1)),
            ])),
        ])
    );
}

/// Grouping edges by an endpoint property reads the endpoint's record on
/// demand, after the batch prefetch of the edges themselves. A corrupt
/// endpoint record fails the aggregate instead of grouping under null.
#[tokio::test]
async fn endpoint_property_groups_fail_on_a_corrupt_endpoint_record() {
    use crate::encoding::keys;
    let db = test_support::open_db("stream-group-corrupt-endpoint").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let edge = test_support::add_edge(&db, alice, bob, "KNOWS").await;
    let edges = || ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Edge(edge))]);
    let mut ctx =
        super::super::super::ExecutionContext::new(&db, context::ParamBindings::default());
    assert_eq!(
        ctx.aggregate(edges(), &ir::AggregatePlan::GroupCount(name("$from.name")))
            .await
            .unwrap(),
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([
            (
                "$from.name".to_string(),
                DbPropertyValue::String("alice".to_string()),
            ),
            ("count".to_string(), DbPropertyValue::I64(1)),
        ]))])
    );

    db.inner_db()
        .put(
            keys::DataKey::Data {
                scope: keys::scope::DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(alice)),
            }
            .to_bytes(),
            bytes::Bytes::from_static(b"corrupt endpoint record"),
        )
        .await
        .unwrap();
    for aggregate in [
        ir::AggregatePlan::Group(name("$from.name")),
        ir::AggregatePlan::GroupCount(name("$from.name")),
    ] {
        assert!(
            ctx.aggregate(edges(), &aggregate).await.is_err(),
            "{aggregate:?}"
        );
    }
    db.close().await.unwrap();
}
