use super::support::*;

#[test]
fn stream_windows_accept_literal_and_parameter_bounds() {
    let params = context::ParamBindings::default().with_value(name("limit"), 2);
    let limit = ir::StreamBoundPlan::new(StreamBound::expr(Expr::param("limit")))
        .expect("valid parameter bound");
    assert_eq!(eval_stream_bound(&limit, &params).unwrap(), 2);
    assert_eq!(row_ids(limit_rows(rows(&[1, 2, 3, 4]), 2)), vec![1, 2]);

    assert_eq!(row_ids(skip_rows(rows(&[1, 2, 3, 4]), 2)), vec![3, 4]);

    let range = ir::StreamRangePlan::new(StreamBound::Literal(1), StreamBound::Literal(3))
        .expect("valid literal range");
    let ir::StreamRangePlan::Literal(range) = range else {
        panic!("expected literal test range");
    };
    assert_eq!(
        row_ids(slice_rows(rows(&[1, 2, 3, 4]), range.start(), range.end())),
        vec![2, 3]
    );
}

#[test]
fn distinct_and_merge_are_deterministic_and_dependency_aware() {
    assert_eq!(
        row_ids(distinct_rows(rows(&[1, 2, 1, 3, 2]))),
        vec![1, 2, 3]
    );
    assert_eq!(
        row_ids(merge_streams(
            vec![rows(&[2, 1]), rows(&[1, 3])],
            exec::ExecMergeMode::Concat,
        ),),
        vec![2, 1, 1, 3]
    );
    assert_eq!(
        row_ids(merge_streams(
            vec![rows(&[2, 1]), rows(&[1, 3])],
            exec::ExecMergeMode::Union,
        ),),
        vec![2, 1, 3]
    );
    assert_eq!(
        row_ids(merge_streams(
            vec![rows(&[2, 1, 2, 4]), rows(&[1, 2, 2]), rows(&[2, 3, 1]),],
            exec::ExecMergeMode::Intersect,
        ),),
        vec![2, 1]
    );
}

#[test]
fn variable_stream_helpers_bind_and_filter_sets() {
    let bound_rows = bind_rows(rows(&[1, 2]), &name("seen"));
    assert_eq!(
        bound_rows[0].bindings.get(&name("seen")),
        Some(&ElementRef::Node(1))
    );

    let allowed = rows(&[1, 3, 5])
        .into_iter()
        .filter_map(|row| row.current)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        row_ids(filter_within_rows(rows(&[1, 2, 3, 4, 5]), &allowed)),
        vec![1, 3, 5]
    );
    assert_eq!(
        row_ids(filter_without_rows(rows(&[1, 2, 3, 4, 5]), &allowed)),
        vec![2, 4]
    );
}

#[tokio::test]
async fn dynamic_range_uses_runtime_parameter_bounds() {
    let db = test_support::open_db("stream-dynamic-range").await;
    let ids = [
        test_support::add_user(&db, "ada").await,
        test_support::add_user(&db, "grace").await,
        test_support::add_user(&db, "katherine").await,
        test_support::add_user(&db, "margaret").await,
    ];
    let ids_param = name("ids");
    let start_param = name("start");
    let end_param = name("end");
    let range = ir::StreamRangePlan::new(
        StreamBound::expr(Expr::param(start_param.as_ref())),
        StreamBound::expr(Expr::param(end_param.as_ref())),
    )
    .expect("valid dynamic range");
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let range_id = exec::ExecStepId::new(2).expect("positive step id");
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, ids_param.clone()),
            test_support::step(2, vec![access_id], exec::ExecOp::Range { range }),
            test_support::step(
                3,
                vec![range_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    );

    let result = db
        .execute(
            &plan,
            context::ParamBindings::default()
                .with_value(ids_param, ids_value(&ids))
                .with_value(start_param, PropertyValue::I64(1))
                .with_value(end_param, PropertyValue::I64(3)),
        )
        .await
        .expect("dynamic range executes")
        .last
        .expect("project step returns a value");

    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(ids[1]),
            ExecutionScalar::NodeId(ids[2]),
        ])
    );
}
