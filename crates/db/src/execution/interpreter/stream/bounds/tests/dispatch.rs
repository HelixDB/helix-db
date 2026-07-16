use super::*;

#[tokio::test]
async fn execution_bounds_apply_to_stream_and_scalar_values() {
    let db = test_support::open_db("stream-bounds-contract").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        row_ids(
            ctx.limit(
                ExecutionValue::Stream(rows(&[1, 2, 3])),
                &ir::StreamBoundPlan::Literal(2),
            )
            .unwrap()
        ),
        vec![1, 2]
    );
    assert_eq!(
        row_ids(
            ctx.skip(
                ExecutionValue::Stream(rows(&[1, 2, 3])),
                &ir::StreamBoundPlan::Literal(1),
            )
            .unwrap()
        ),
        vec![2, 3]
    );
    assert_eq!(
        ctx.limit(scalars(vec![10, 11, 12]), &ir::StreamBoundPlan::Literal(2))
            .unwrap(),
        scalars(vec![10, 11])
    );
    assert_eq!(
        ctx.skip(scalars(vec![10, 11, 12]), &ir::StreamBoundPlan::Literal(1))
            .unwrap(),
        scalars(vec![11, 12])
    );
    assert_eq!(
        ctx.range(
            scalars(vec![10, 11, 12, 13]),
            &ir::StreamRangePlan::new(StreamBound::Literal(1), StreamBound::Literal(3))
                .expect("valid scalar range"),
        )
        .unwrap(),
        scalars(vec![11, 12])
    );
}

#[tokio::test]
async fn execution_bounds_reject_folded_stream_inputs() {
    let db = test_support::open_db("stream-bounds-folded").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let folded = || ExecutionValue::FoldedStream(FoldedStream::new(rows(&[1, 2])));

    assert!(
        error_message(ctx.limit(folded(), &ir::StreamBoundPlan::Literal(1)))
            .contains("limit expected stream input, got folded stream")
    );
    assert!(
        error_message(ctx.skip(folded(), &ir::StreamBoundPlan::Literal(1)))
            .contains("skip expected stream input, got folded stream")
    );
    assert!(error_message(
        ctx.range(
            folded(),
            &ir::StreamRangePlan::new(StreamBound::Literal(0), StreamBound::Literal(1))
                .expect("valid folded range"),
        )
    )
    .contains("range expected stream input, got folded stream"));
}

#[tokio::test]
async fn dynamic_range_uses_runtime_bound_parameters() {
    let db = test_support::open_db("stream-bounds-dynamic-range").await;
    let start = name("start");
    let end = name("end");
    let mut ctx = ExecutionContext::new(
        &db,
        context::ParamBindings::default()
            .with_value(start.clone(), AstPropertyValue::I64(1))
            .with_query_value(end.clone(), QueryValue::I64(3)),
    );
    let range = ir::StreamRangePlan::new(
        StreamBound::expr(Expr::param(start.as_ref())),
        StreamBound::expr(Expr::param(end.as_ref())),
    )
    .expect("valid dynamic range");

    assert_eq!(
        row_ids(
            ctx.range(ExecutionValue::Stream(rows(&[1, 2, 3, 4])), &range)
                .unwrap()
        ),
        vec![2, 3]
    );
}
