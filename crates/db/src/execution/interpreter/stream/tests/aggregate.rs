use super::support::*;

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
                (
                    "status".to_string(),
                    DbPropertyValue::String("closed".to_string()),
                ),
                ("count".to_string(), DbPropertyValue::I64(1)),
            ])),
            ExecutionScalar::Object(BTreeMap::from([
                ("status".to_string(), DbPropertyValue::Null,),
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
