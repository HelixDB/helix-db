use super::*;

#[tokio::test]
async fn merge_dispatch_rejects_non_stream_dependencies() {
    let db = test_support::open_db("stream-sets-merge-dispatch").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        row_ids(expect_stream(
            ctx.merge_values(
                vec![stream(&[1, 2]), stream(&[2, 3])],
                exec::ExecMergeMode::Union,
            )
            .unwrap(),
            "merge result",
        )),
        vec![1, 2, 3]
    );
    assert!(error_message(ctx.merge_values(
        vec![stream(&[1]), ExecutionValue::Count(1)],
        exec::ExecMergeMode::Concat
    ))
    .contains("merge expected stream input"));
}

#[test]
fn merge_concat_and_union_are_deterministic() {
    assert_eq!(
        row_ids(set_merge::merge_streams(
            vec![rows(&[2, 1]), rows(&[1, 3])],
            exec::ExecMergeMode::Concat,
        )),
        vec![2, 1, 1, 3]
    );
    assert_eq!(
        row_ids(set_merge::merge_streams(
            vec![rows(&[2, 1]), rows(&[1, 3])],
            exec::ExecMergeMode::Union,
        )),
        vec![2, 1, 3]
    );
}

#[test]
fn merge_intersection_handles_empty_inputs_and_duplicate_first_rows() {
    assert!(set_merge::merge_streams(Vec::new(), exec::ExecMergeMode::Intersect).is_empty());
    assert!(set_merge::merge_streams(
        vec![rows(&[1, 2]), Vec::new()],
        exec::ExecMergeMode::Intersect,
    )
    .is_empty());
    assert_eq!(
        row_ids(set_merge::merge_streams(
            vec![rows(&[1, 1, 2, 3]), rows(&[1, 3]), rows(&[1, 2, 3])],
            exec::ExecMergeMode::Intersect,
        )),
        vec![1, 3]
    );
    assert_eq!(
        row_ids(set_merge::merge_streams(
            vec![rows(&[2, 1, 2, 4]), rows(&[1, 2, 2]), rows(&[2, 3, 1])],
            exec::ExecMergeMode::Intersect,
        )),
        vec![2, 1]
    );
}
