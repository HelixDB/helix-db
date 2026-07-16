use std::collections::BTreeSet;

use super::*;

#[tokio::test]
async fn element_set_accepts_stream_shapes_and_rejects_scalar_shapes() {
    let db = test_support::open_db("stream-eval-element-set").await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        ctx.element_set(&ExecutionValue::Stream(vec![
            current_node(1),
            ExecutionRow::empty(),
            current_node(2),
        ]))
        .unwrap(),
        BTreeSet::from([ElementRef::Node(1), ElementRef::Node(2)])
    );
    assert_eq!(
        ctx.element_set(&ExecutionValue::FoldedStream(FoldedStream::new(vec![
            current_node(2),
            current_node(3),
        ])))
        .unwrap(),
        BTreeSet::from([ElementRef::Node(2), ElementRef::Node(3)])
    );
    assert!(ctx
        .element_set(&ExecutionValue::Count(1))
        .unwrap_err()
        .to_string()
        .contains("variable operation expected stream value"));
}
