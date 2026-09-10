use super::super::*;
use crate::encoding::v2::{keys, values::property};
use futures::StreamExt;
use helix_planner::context;

#[tokio::test]
async fn unwind_hydrates_input_batches_and_releases_owned_expansion_state() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-unwind-hydration").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    for id in 0..65 {
        let mut properties = vec![property::Property::string("$label", "N")];
        if id % 3 != 0 {
            properties.push(property::Property::i64_array("items", vec![id as i64; 4]));
        }
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    id,
                ))),
                property::encode_properties(&properties),
            )
            .await
            .unwrap();
    }
    ctx.enable_request_read_view().await.unwrap();
    let expression =
        r::Expression::Property(Box::new(r::Expression::Slot(r::Slot(0))), "items".into());
    let parameters = BTreeMap::new();
    for batch_rows in [1, 7, 32, 128] {
        let limits = Limits {
            batch_rows,
            memory_bytes: 512 * 1024,
            ..Default::default()
        };
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        let input = Rows::new(
            (0..65)
                .map(|id| vec![r::Value::Entity(r::Entity::Node(id)), r::Value::Null])
                .collect(),
            ctx.row_budget(),
        )
        .unwrap();
        let mut batches =
            Box::pin(ctx.unwind_batches(input, &expression, r::Slot(1), &parameters, limits));
        let mut actual = Vec::new();
        while let Some(batch) = batches.next().await {
            let batch = batch.unwrap();
            assert!(batch.len() <= batch_rows);
            actual.extend(batch.iter().map(|row| row[1].clone()));
        }
        drop(batches);
        let expected: Vec<_> = (0..65)
            .filter(|id| id % 3 != 0)
            .flat_map(|id| std::iter::repeat_n(r::Value::Integer(id), 4))
            .collect();
        assert_eq!(actual, expected);
        let reads = ctx.row_budget().reads();
        assert_eq!(reads.multi_get_keys, 65);
        assert_eq!(reads.multi_get_batches, 65_usize.div_ceil(batch_rows));
        assert_eq!(reads.point_gets, 0);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    // Dropping a partly consumed large range must release its input and cursor.
    let limits = Limits {
        batch_rows: 1,
        memory_bytes: 65536,
        ..Default::default()
    };
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    let input = Rows::new(vec![vec![r::Value::Null]], ctx.row_budget()).unwrap();
    let range = r::Expression::Function(
        r::Function::Range,
        vec![
            r::Expression::Literal(r::Value::Integer(1)),
            r::Expression::Literal(r::Value::Integer(1_000_000_000)),
        ],
    );
    let mut batches = Box::pin(ctx.unwind_batches(input, &range, r::Slot(0), &parameters, limits));
    assert_eq!(
        batches.next().await.unwrap().unwrap().data,
        vec![vec![r::Value::Integer(1)]]
    );
    drop(batches);
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn unwind_errors_and_cancellation_release_hydration_and_output() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-unwind-cleanup").await;
    for cancel in [false, true] {
        let limits = Limits {
            batch_rows: 2,
            memory_bytes: 65536,
            ..Default::default()
        };
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        let input = Rows::new(
            vec![
                vec![r::Value::Integer(1), r::Value::Null],
                vec![r::Value::Integer(0), r::Value::Null],
            ],
            ctx.row_budget(),
        )
        .unwrap();
        let expression = r::Expression::Binary(
            r::Binary::Divide,
            Box::new(r::Expression::Literal(r::Value::Integer(1))),
            Box::new(r::Expression::Slot(r::Slot(0))),
        );
        let parameters = BTreeMap::new();
        if cancel {
            ctx.fail_deadline_after(1);
        }
        let mut batches =
            Box::pin(ctx.unwind_batches(input, &expression, r::Slot(1), &parameters, limits));
        let error = match batches.next().await.unwrap() {
            Err(error) => error,
            Ok(_) => panic!("expected failure"),
        };
        if cancel {
            assert!(matches!(
                error,
                Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)
            ));
        } else {
            assert!(matches!(error, Error::Query(error) if error.detail == "DivisionByZero"));
        }
        drop(batches);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    db.close().await.unwrap();
}

#[test]
fn row_admission_precedes_cloning_and_does_not_copy_overwritten_values() {
    let budget = memory::Budget::new(512);
    let mut output = RowBuffer::new(&budget).unwrap();
    let source = vec![
        r::Value::String("large old slot".repeat(10000)),
        r::Value::Integer(7),
    ];
    let invoked = std::cell::Cell::new(false);
    let error = output
        .push_with(row_bytes(&source), || {
            invoked.set(true);
            source.clone()
        })
        .unwrap_err();
    assert!(matches!(error, Error::Query(error) if error.detail == "MemoryLimit"));
    assert!(!invoked.get());
    output
        .push_replacing(&source, r::Slot(0), r::Value::Integer(9))
        .unwrap();
    assert_eq!(
        output.finish().data,
        vec![vec![r::Value::Integer(9), r::Value::Integer(7)]]
    );
    assert_eq!(budget.available(), 512);
}
