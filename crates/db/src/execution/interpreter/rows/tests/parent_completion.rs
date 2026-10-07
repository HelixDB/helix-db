use super::*;
use crate::execution::interpreter::test_support;
use futures::FutureExt;

#[tokio::test]
async fn completed_parents_release_payload_after_optional_fallback_and_cannot_resume() {
    let db = test_support::open_db("correlated-parent-release").await;
    let mut context = ExecutionContext::new(&db, helix_planner::context::ParamBindings::default());
    let limits = Limits {
        batch_rows: 3,
        memory_bytes: 64 * 1024,
        ..Default::default()
    };
    context.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    context.enable_request_read_view().await.unwrap();
    let payload_bytes = 4096;
    let rows = memory::Rows::new(
        (0_i64..3)
            .map(|ordinal| {
                vec![
                    r::Value::Integer(ordinal),
                    r::Value::String("p".repeat(payload_bytes)),
                ]
            })
            .collect(),
        context.row_budget(),
    )
    .unwrap();
    let mut parents = Parents::new(rows, context.row_budget()).unwrap();
    let before = context.row_budget().available();
    let pattern = r::Pattern {
        nodes: vec![],
        relationships: vec![],
        paths: vec![],
    };
    let parameters = BTreeMap::new();
    let operation = matches::Match {
        pattern: &pattern,
        optional: true,
        predicate: None,
        demand: usize::MAX,
    };
    let mut batch = Batch::new(3, context.row_budget()).unwrap();
    let outer = parents.get(0).unwrap();
    batch
        .candidate_with(0, super::super::row_bytes(outer), || outer.clone())
        .unwrap();
    batch.complete(0);
    batch.complete(1);
    let output = batch
        .finish(&mut parents, operation, &context, &parameters, limits)
        .await
        .unwrap();
    assert_eq!(output.len(), 2);
    for (row, expected) in output.iter().zip(0_i64..2) {
        assert_eq!(row[0], r::Value::Integer(expected));
        let r::Value::String(payload) = &row[1] else {
            panic!("preserved outer payload")
        };
        assert_eq!(payload.len(), payload_bytes);
        assert!(payload.bytes().all(|byte| byte == b'p'));
    }
    drop(output);
    assert!(context.row_budget().available() >= before + 2 * payload_bytes);
    assert_eq!(parents.get(2).unwrap()[0], r::Value::Integer(2));
    assert!(parents.get(3).is_none());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parents.get(0))).is_err());

    // A filtered candidate still emits its outer row exactly once for OPTIONAL.
    let predicate = r::Expression::Literal(r::Value::Boolean(false));
    let mut batch = Batch::new(2, context.row_budget()).unwrap();
    let outer = parents.get(2).unwrap();
    batch
        .candidate_with(2, super::super::row_bytes(outer), || outer.clone())
        .unwrap();
    batch.complete(2);
    let output = batch
        .finish(
            &mut parents,
            matches::Match {
                predicate: Some(&predicate),
                ..operation
            },
            &context,
            &parameters,
            limits,
        )
        .await
        .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0][0], r::Value::Integer(2));
    drop(output);
    assert!(context.row_budget().available() >= before + 3 * payload_bytes);

    // Producer contract violations must fail loudly instead of silently reading
    // cleared row slots or extending one optional parent twice.
    for candidate in [false, true] {
        let mut batch = Batch::new(1, context.row_budget()).unwrap();
        if candidate {
            batch
                .candidate_with(0, size_of::<r::Row>(), Vec::new)
                .unwrap();
        } else {
            batch.complete(0);
        }
        assert!(std::panic::AssertUnwindSafe(batch.finish(
            &mut parents,
            operation,
            &context,
            &parameters,
            limits
        ))
        .catch_unwind()
        .await
        .is_err());
    }
    drop(parents);
    assert_eq!(context.row_budget().available(), limits.memory_bytes);
    context.close_request_read_view().unwrap();
    db.close().await.unwrap();
}
