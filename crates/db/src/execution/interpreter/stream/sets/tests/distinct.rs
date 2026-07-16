use super::*;

#[tokio::test]
async fn distinct_dispatch_handles_stream_scalar_and_folded_inputs() {
    let db = test_support::open_db("stream-sets-distinct-dispatch").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        row_ids(expect_stream(
            ctx.distinct(stream(&[1, 2, 1])).unwrap(),
            "distinct result",
        )),
        vec![1, 2]
    );
    assert_eq!(ctx.distinct(scalars(&[7, 7, 8])).unwrap(), scalars(&[7, 8]));
    assert!(error_message(
        ctx.distinct(ExecutionValue::FoldedStream(FoldedStream::new(rows(&[
            1, 2,
        ]))))
    )
    .contains("distinct expected stream input, got folded stream"));
}

#[test]
fn distinct_rows_are_order_preserving() {
    assert_eq!(
        row_ids(set_distinct::distinct_rows(rows(&[1, 2, 1, 3, 2]))),
        vec![1, 2, 3]
    );
}

#[test]
fn distinct_rows_compare_empty_row_state() {
    assert_eq!(
        set_distinct::distinct_rows(vec![ExecutionRow::empty(), ExecutionRow::empty()]).len(),
        1
    );
}

#[test]
fn distinct_rows_use_current_element_and_preserve_first_row_payload() {
    let hidden_path_a = {
        let mut row = ExecutionRow::current(ElementRef::Node(1));
        row.set_current(ElementRef::Node(2));
        row
    };
    let hidden_path_b = ExecutionRow::current(ElementRef::Node(2));
    assert_eq!(
        set_distinct::distinct_rows(vec![hidden_path_a, hidden_path_b]).len(),
        1
    );
    assert_eq!(
        set_distinct::distinct_rows(vec![visible_path_row(1, 2), visible_path_row(3, 2)]).len(),
        1
    );

    let binding = name("source");
    let mut bound_a = ExecutionRow::current(ElementRef::Node(2));
    bound_a
        .bindings
        .insert(binding.clone(), ElementRef::Node(1));
    let mut bound_b = ExecutionRow::current(ElementRef::Node(2));
    bound_b
        .bindings
        .insert(binding.clone(), ElementRef::Node(3));
    let deduped = set_distinct::distinct_rows(vec![bound_a, bound_b]);
    assert_eq!(deduped.len(), 1);
    assert_eq!(
        deduped[0].bindings.get(&binding),
        Some(&ElementRef::Node(1))
    );
}
