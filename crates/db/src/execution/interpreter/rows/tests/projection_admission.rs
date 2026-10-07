use super::super::*;
use crate::execution::interpreter::test_support;
use futures::FutureExt;
use helix_planner::context;

#[derive(Clone, Copy, Debug)]
enum Consumer {
    Plain,
    Sort,
    TopK,
}

#[tokio::test]
async fn projection_copies_only_needed_inputs_after_memory_admission() {
    let db = test_support::open_db("projection-owned-inputs").await;
    const PAYLOAD_BYTES: usize = 256 * 1024;
    for payload in [
        r::Value::String("p".repeat(PAYLOAD_BYTES)),
        r::Value::List(vec![r::Value::String("p".repeat(PAYLOAD_BYTES))]),
        r::Value::Map(BTreeMap::from([(
            "nested".into(),
            r::Value::String("p".repeat(PAYLOAD_BYTES)),
        )])),
    ] {
        for (consumer, needs_original) in [
            (Consumer::Plain, false),
            (Consumer::Sort, false),
            (Consumer::TopK, false),
            (Consumer::Sort, true),
            (Consumer::TopK, true),
        ] {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            let data = vec![vec![payload.clone(), r::Value::Null]];
            let memory_bytes = rows_bytes(&data) + 48 * 1024;
            ctx.row_memory = Some(memory::Budget::new(memory_bytes));
            let rows = Rows::new(data, ctx.row_budget()).unwrap();
            let items = r::ProjectionProgram::new(vec![r::Projection {
                slot: r::Slot(1),
                expression: r::Expression::Literal(r::Value::Integer(1)),
            }])
            .unwrap();
            let ordering = match consumer {
                Consumer::Plain => vec![],
                Consumer::Sort | Consumer::TopK => vec![r::Ordering {
                    expression: r::Expression::Slot(r::Slot(if needs_original { 0 } else { 1 })),
                    descending: false,
                }],
            };
            let limit = matches!(consumer, Consumer::TopK)
                .then_some(r::Expression::Literal(r::Value::Integer(1)));
            let projection = projection::Projection {
                items: &items,
                distinct: false,
                ordering: &ordering,
                predicate: None,
                skip: None,
                limit: limit.as_ref(),
            };
            let parameters = BTreeMap::new();
            let (result, allocated) = crate::allocation_testing::observe(|| {
                ctx.project_rows(
                    rows,
                    2,
                    projection,
                    &parameters,
                    Limits {
                        memory_bytes,
                        ..Default::default()
                    },
                )
                .now_or_never()
            });
            let result = result.expect("scalar projections need no pending storage I/O");
            assert!(allocated.bytes < PAYLOAD_BYTES,
                "{consumer:?}, needs_original={needs_original}: copied a large input before admission: {allocated:?}");
            if needs_original {
                assert!(
                    matches!(result, Err(Error::Query(error)) if error.category=="ResourceLimit" && error.detail=="MemoryLimit")
                );
            } else {
                let rows = result.unwrap();
                assert_eq!(rows.data, vec![vec![r::Value::Null, r::Value::Integer(1)]]);
                drop(rows);
            }
            assert_eq!(ctx.row_budget().available(), memory_bytes);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn sorting_admits_live_keys_cumulatively_before_evaluating_later_keys() {
    let db = test_support::open_db("projection-sort-key-admission").await;
    const PAYLOAD_BYTES: usize = 256 * 1024;
    for payload in [
        r::Value::String("k".repeat(PAYLOAD_BYTES)),
        r::Value::List(vec![r::Value::String("k".repeat(PAYLOAD_BYTES))]),
        r::Value::Map(BTreeMap::from([(
            "nested".into(),
            r::Value::String("k".repeat(PAYLOAD_BYTES)),
        )])),
    ] {
        let parameters = BTreeMap::from([("key".into(), payload)]);
        for consumer in [Consumer::Sort, Consumer::TopK] {
            for late_error in [false, true] {
                let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                let memory_bytes = PAYLOAD_BYTES + 64 * 1024;
                ctx.row_memory = Some(memory::Budget::new(memory_bytes));
                let rows = Rows::new(vec![vec![r::Value::Null]], ctx.row_budget()).unwrap();
                let items = r::ProjectionProgram::new(vec![r::Projection {
                    slot: r::Slot(0),
                    expression: r::Expression::Literal(r::Value::Integer(1)),
                }])
                .unwrap();
                let mut ordering = vec![
                    r::Ordering {
                        expression: r::Expression::Parameter("key".into()),
                        descending: false,
                    };
                    2
                ];
                if late_error {
                    ordering.push(r::Ordering {
                        expression: r::Expression::Binary(
                            r::Binary::Divide,
                            Box::new(r::Expression::Literal(r::Value::Integer(1))),
                            Box::new(r::Expression::Literal(r::Value::Integer(0))),
                        ),
                        descending: false,
                    });
                }
                let limit = matches!(consumer, Consumer::TopK)
                    .then_some(r::Expression::Literal(r::Value::Integer(1)));
                let projection = projection::Projection {
                    items: &items,
                    distinct: false,
                    ordering: &ordering,
                    predicate: None,
                    skip: None,
                    limit: limit.as_ref(),
                };
                let (result, allocated) = crate::allocation_testing::observe(|| {
                    ctx.project_rows(
                        rows,
                        1,
                        projection,
                        &parameters,
                        Limits {
                            memory_bytes,
                            ..Default::default()
                        },
                    )
                    .now_or_never()
                });
                let result = result.expect("scalar sort keys need no pending storage I/O");
                assert!(allocated.bytes <= memory_bytes,
                    "{consumer:?}, late_error={late_error}: key construction exceeded available memory: {allocated:?}");
                assert!(
                    matches!(result, Err(Error::Query(error)) if error.category=="ResourceLimit" && error.detail=="MemoryLimit")
                );
                assert_eq!(ctx.row_budget().available(), memory_bytes);
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn projection_input_masks_allocate_only_for_referenced_bindings() {
    let db = test_support::open_db("projection-input-mask").await;
    let items = r::ProjectionProgram::new(vec![r::Projection {
        slot: r::Slot(0),
        expression: r::Expression::Literal(r::Value::Integer(1)),
    }])
    .unwrap();
    for references in [false, true] {
        let ordering = [r::Ordering {
            expression: if references {
                r::Expression::Slot(r::Slot(1))
            } else {
                r::Expression::Literal(r::Value::Integer(0))
            },
            descending: false,
        }];
        let predicate = r::SelectionProgram::new(if references {
            r::Expression::HasLabel(r::Slot(2), "N".into())
        } else {
            r::Expression::Literal(r::Value::Boolean(true))
        })
        .unwrap();
        let projection = projection::Projection {
            items: &items,
            distinct: false,
            ordering: &ordering,
            predicate: Some(&predicate),
            skip: None,
            limit: None,
        };
        for memory_bytes in [0, 2, 3] {
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.row_memory = Some(memory::Budget::new(memory_bytes));
            let (result, allocated) =
                crate::allocation_testing::observe(|| projection.input_slots(&ctx, 3));
            if !references {
                assert_eq!(
                    allocated.bytes, 0,
                    "constant expressions need no input mask"
                );
            }
            match result {
                Ok(inputs) => {
                    assert!(!references || memory_bytes >= 3);
                    assert!(!inputs.keeps(0));
                    assert_eq!(inputs.keeps(1), references);
                    assert_eq!(inputs.keeps(2), references);
                    assert_eq!(
                        ctx.row_budget().available(),
                        memory_bytes - if references { 3 } else { 0 }
                    );
                    drop(inputs);
                }
                Err(error) => {
                    assert!(references && memory_bytes < 3);
                    assert!(
                        matches!(error, Error::Query(ref error) if error.detail == "MemoryLimit"),
                        "{error}"
                    );
                }
            }
            assert_eq!(ctx.row_budget().available(), memory_bytes);
        }
    }
    db.close().await.unwrap();
}
