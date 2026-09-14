use super::super::*;
use crate::execution::interpreter::test_support;
use futures::FutureExt;
use helix_planner::context;

#[tokio::test]
async fn direct_count_does_not_copy_discarded_input_payloads() {
    let db = test_support::open_db("direct-count-admission").await;
    const PAYLOAD_BYTES: usize = 256 * 1024;
    for payload in [
        r::Value::String("p".repeat(PAYLOAD_BYTES)),
        r::Value::List(vec![r::Value::String("p".repeat(PAYLOAD_BYTES))]),
        r::Value::Map(BTreeMap::from([(
            "nested".into(),
            r::Value::String("p".repeat(PAYLOAD_BYTES)),
        )])),
    ] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let data = vec![vec![payload, r::Value::Null]];
        let memory_bytes = rows_bytes(&data) + 48 * 1024;
        ctx.row_memory = Some(memory::Budget::new(memory_bytes));
        let rows = Rows::new(data, ctx.row_budget()).unwrap();
        let items = r::ProjectionProgram::new(vec![r::Projection {
            slot: r::Slot(1),
            expression: r::Expression::Aggregate {
                function: r::Aggregate::Count,
                argument: None,
                distinct: false,
            },
        }])
        .unwrap();
        let parameters = BTreeMap::new();
        let projection = projection::Projection {
            items: &items,
            distinct: false,
            ordering: &[],
            predicate: None,
            skip: None,
            limit: None,
        };
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
        let result = result.expect("scalar direct aggregation needs no pending storage I/O");
        assert!(
            allocated.bytes < PAYLOAD_BYTES,
            "direct count copied discarded payload before admission: {allocated:?}"
        );
        let rows = result.unwrap();
        assert_eq!(rows.data, vec![vec![r::Value::Null, r::Value::Integer(1)]]);
        drop(rows);
        assert_eq!(ctx.row_budget().available(), memory_bytes);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn direct_grouping_keys_share_one_live_allowance() {
    let db = test_support::open_db("direct-group-key-admission").await;
    const PAYLOAD_BYTES: usize = 256 * 1024;
    for payload in [
        r::Value::String("k".repeat(PAYLOAD_BYTES)),
        r::Value::List(vec![r::Value::String("k".repeat(PAYLOAD_BYTES))]),
        r::Value::Map(BTreeMap::from([(
            "nested".into(),
            r::Value::String("k".repeat(PAYLOAD_BYTES)),
        )])),
    ] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let memory_bytes = PAYLOAD_BYTES + 64 * 1024;
        ctx.row_memory = Some(memory::Budget::new(memory_bytes));
        let rows = Rows::new(vec![vec![r::Value::Null; 3]], ctx.row_budget()).unwrap();
        let items = r::ProjectionProgram::new(vec![
            r::Projection {
                slot: r::Slot(0),
                expression: r::Expression::Parameter("key".into()),
            },
            r::Projection {
                slot: r::Slot(1),
                expression: r::Expression::Parameter("key".into()),
            },
            r::Projection {
                slot: r::Slot(2),
                expression: r::Expression::Aggregate {
                    function: r::Aggregate::Count,
                    argument: None,
                    distinct: false,
                },
            },
        ])
        .unwrap();
        let parameters = BTreeMap::from([("key".into(), payload)]);
        let projection = projection::Projection {
            items: &items,
            distinct: false,
            ordering: &[],
            predicate: None,
            skip: None,
            limit: None,
        };
        let (result, allocated) = crate::allocation_testing::observe(|| {
            ctx.project_rows(
                rows,
                3,
                projection,
                &parameters,
                Limits {
                    memory_bytes,
                    ..Default::default()
                },
            )
            .now_or_never()
        });
        let result = result.expect("scalar direct grouping needs no pending storage I/O");
        assert!(
            allocated.bytes <= memory_bytes,
            "direct grouping copied keys before admission: {allocated:?}"
        );
        assert!(matches!(result,Err(Error::Query(error)) if error.detail=="MemoryLimit"));
        assert_eq!(ctx.row_budget().available(), memory_bytes);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn empty_global_aggregation_admits_its_null_representative_before_allocation() {
    let db = test_support::open_db("direct-empty-group-admission").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let memory_bytes = 1024;
    ctx.row_memory = Some(memory::Budget::new(memory_bytes));
    let rows = Rows::new(Vec::new(), ctx.row_budget()).unwrap();
    let items = r::ProjectionProgram::new(vec![r::Projection {
        slot: r::Slot(3999),
        expression: r::Expression::Aggregate {
            function: r::Aggregate::Count,
            argument: None,
            distinct: false,
        },
    }])
    .unwrap();
    let parameters = BTreeMap::new();
    let projection = projection::Projection {
        items: &items,
        distinct: false,
        ordering: &[],
        predicate: None,
        skip: None,
        limit: None,
    };
    let (result, allocated) = crate::allocation_testing::observe(|| {
        ctx.project_rows(
            rows,
            4000,
            projection,
            &parameters,
            Limits {
                memory_bytes,
                ..Default::default()
            },
        )
        .now_or_never()
    });
    assert!(
        matches!(result.expect("empty aggregation performs no pending I/O"),Err(Error::Query(error)) if error.detail=="MemoryLimit")
    );
    assert!(
        allocated.bytes < 2048,
        "empty group allocated its wide row before admission: {allocated:?}"
    );
    assert_eq!(ctx.row_budget().available(), memory_bytes);
    db.close().await.unwrap();
}
