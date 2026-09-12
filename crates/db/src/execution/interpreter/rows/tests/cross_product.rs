use super::*;
use crate::execution::interpreter::{rows, test_support};
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn product_positions_handle_sparse_ids_duplicates_empty_inputs_and_drop() {
    let db = test_support::open_db("product-positions").await;
    let limits = Limits {
        memory_bytes: 512 * 1024,
        batch_rows: 2,
        ..Default::default()
    };
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    context.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    context.enable_request_read_view().await.unwrap();
    let expected_ids = [0, 7, u32::MAX as u64, (4_u64 << 32) + 1, u64::MAX];
    for batch_rows in [1, 2, 7] {
        for (parents, populated) in [(0, true), (3, false), (3, true)] {
            let mut ids = bitmap::Builder::new(Some(context.row_budget())).unwrap();
            if populated {
                for id in expected_ids {
                    ids.insert(id).unwrap();
                }
            }
            let mut source = ScanCache::new(
                NodeCursor::Indexed {
                    ids: ids.finish().into_iter(),
                    verify_existence: false,
                },
                context.row_budget(),
            )
            .unwrap();
            let mut input = RowBuffer::new(context.row_budget()).unwrap();
            for parent in 0..parents {
                input
                    .push_with(size_of::<r::Row>() + 2 * size_of::<r::Value>(), || {
                        vec![r::Value::Integer(parent / 2), r::Value::Null]
                    })
                    .unwrap();
            }
            let mut cursor = ScanCursor::new(input.finish(), r::Slot(1));
            let mut actual = Vec::new();
            while let Some(batch) = cursor
                .next_batch(
                    &context,
                    &mut source,
                    Limits {
                        batch_rows,
                        ..limits
                    },
                )
                .await
                .unwrap()
            {
                assert!(batch.len() <= batch_rows);
                actual.extend(batch.into_iter());
            }
            let expected = (0..parents)
                .flat_map(|parent| {
                    expected_ids
                        .into_iter()
                        .filter(move |_| populated)
                        .map(move |id| {
                            vec![
                                r::Value::Integer(parent / 2),
                                r::Value::Entity(r::Entity::Node(id)),
                            ]
                        })
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            assert_eq!(
                source.iter().unwrap().collect::<Vec<_>>(),
                if parents > 0 && populated {
                    expected_ids.to_vec()
                } else {
                    vec![]
                }
            );
            if parents > 0 {
                assert!(matches!(source.source, Source::Complete(_)));
            }
            drop(cursor);
            drop(source);
            assert_eq!(context.row_budget().available(), limits.memory_bytes);
        }
    }
    for stop in ["drop", "cancel", "memory"] {
        let mut ids = bitmap::Builder::new(Some(context.row_budget())).unwrap();
        for id in expected_ids {
            ids.insert(id).unwrap();
        }
        let mut source = ScanCache::new(
            NodeCursor::Indexed {
                ids: ids.finish().into_iter(),
                verify_existence: false,
            },
            context.row_budget(),
        )
        .unwrap();
        let mut input = RowBuffer::new(context.row_budget()).unwrap();
        for _ in 0..4 {
            let row = vec![r::Value::String("x".repeat(16 * 1024)), r::Value::Null];
            input.push_with(rows::row_bytes(&row), || row).unwrap();
        }
        let mut cursor = ScanCursor::new(input.finish(), r::Slot(1));
        drop(
            cursor
                .next_batch(&context, &mut source, limits)
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            source.iter().unwrap().collect::<Vec<_>>(),
            expected_ids[..2].to_vec()
        );
        let retained = limits.memory_bytes - context.row_budget().available();
        assert!(retained >= 4 * 16 * 1024);
        if stop == "drop" {
            drop(
                cursor
                    .next_batch(&context, &mut source, limits)
                    .await
                    .unwrap()
                    .unwrap(),
            );
            drop(
                cursor
                    .next_batch(&context, &mut source, limits)
                    .await
                    .unwrap()
                    .unwrap(),
            );
            let remaining = limits.memory_bytes - context.row_budget().available();
            assert!(
                remaining + 16 * 1024 <= retained,
                "completed parent payload must be reclaimed"
            );
        }
        let held = match stop {
            "memory" => Some(
                context
                    .row_budget()
                    .reserve(context.row_budget().available())
                    .unwrap(),
            ),
            "cancel" => {
                context.fail_deadline_after(0);
                None
            }
            "drop" => None,
            _ => unreachable!(),
        };
        if stop != "drop" {
            let error = cursor
                .next_batch(&context, &mut source, limits)
                .await
                .err()
                .expect("resumption must observe failure");
            assert!(
                matches!(
                    error,
                    crate::cypher::Error::Storage(
                        crate::HelixDbError::QueryDeadlineExceeded
                            | crate::HelixDbError::QueryMemoryLimitExceeded
                    )
                ) || matches!(error, crate::cypher::Error::Query(ref e) if e.detail == "MemoryLimit")
            );
        }
        drop(held);
        drop(cursor);
        drop(source);
        assert_eq!(context.row_budget().available(), limits.memory_bytes);
        context.close_request_read_view().unwrap();
        context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        context.enable_request_read_view().await.unwrap();
    }
    context.close_request_read_view().unwrap();
    drop(context);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cached_product_source_extends_only_on_demand_and_closes_failed_continuations() {
    let db = test_support::open_db("product-build").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,17) AS k CREATE (:N {k:k})",
    ))
    .await
    .unwrap();
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    let limits = Limits {
        memory_bytes: 128 * 1024,
        batch_rows: 3,
        ..Default::default()
    };
    context.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    context.enable_request_read_view().await.unwrap();
    let plan = r::plan(
        helix_cypher::compile("MATCH (n) RETURN count(*)").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let [step] = plan.matches()[&0].sources[0].access.steps() else {
        unreachable!()
    };
    let cursor = context.node_cursor(&step.op).await.unwrap().unwrap();
    let mut source = ScanCache::new(cursor, context.row_budget()).unwrap();
    assert_eq!(context.row_budget().reads().scan_rows, 0);
    // Dropping an unpolled future does not take the source's continuation.
    drop(source.extend(&context, limits));
    assert!(matches!(source.source, Source::Open { .. }));
    assert!(source.extend(&context, limits).await.unwrap());
    assert_eq!(source.iter().unwrap().count(), 3);
    assert_eq!(context.row_budget().reads().scan_rows, 3);
    while source.extend(&context, limits).await.unwrap() {}
    assert_eq!(source.iter().unwrap().count(), 17);
    assert!(matches!(source.source, Source::Complete(_)));
    assert!(!source.extend(&context, limits).await.unwrap());
    drop(source);
    assert_eq!(context.row_budget().available(), limits.memory_bytes);
    for populated in [false, true] {
        let ids = if populated {
            bitmap::Bitmap::singleton(u64::MAX, Some(context.row_budget())).unwrap()
        } else {
            bitmap::Bitmap::empty(Some(context.row_budget())).unwrap()
        };
        let mut source = ScanCache::new(
            NodeCursor::Indexed {
                ids: ids.into_iter(),
                verify_existence: false,
            },
            context.row_budget(),
        )
        .unwrap();
        while source.extend(&context, limits).await.unwrap() {}
        assert_eq!(
            source.iter().unwrap().collect::<Vec<_>>(),
            if populated { vec![u64::MAX] } else { vec![] }
        );
        drop(source);
        assert_eq!(context.row_budget().available(), limits.memory_bytes);
    }
    for remaining in [0, 1, 128] {
        let held = context
            .row_budget()
            .reserve(limits.memory_bytes - remaining)
            .unwrap();
        let result = ScanCache::new(
            NodeCursor::Indexed {
                ids: bitmap::Bitmap::empty(None).unwrap().into_iter(),
                verify_existence: false,
            },
            context.row_budget(),
        );
        assert!(result.is_err());
        drop(held);
        assert_eq!(context.row_budget().available(), limits.memory_bytes);
    }
    for failure in ["memory", "deadline"] {
        let mut ids = bitmap::Builder::new(Some(context.row_budget())).unwrap();
        for id in [7, 9] {
            ids.insert(id).unwrap();
        }
        let mut source = ScanCache::new(
            NodeCursor::Indexed {
                ids: ids.finish().into_iter(),
                verify_existence: false,
            },
            context.row_budget(),
        )
        .unwrap();
        assert!(source
            .extend(
                &context,
                Limits {
                    batch_rows: 1,
                    ..limits
                }
            )
            .await
            .unwrap());
        assert_eq!(source.iter().unwrap().collect::<Vec<_>>(), vec![7]);
        let held = if failure == "memory" {
            Some(
                context
                    .row_budget()
                    .reserve(context.row_budget().available())
                    .unwrap(),
            )
        } else {
            context.fail_deadline_after(0);
            None
        };
        assert!(source.extend(&context, limits).await.is_err());
        assert!(matches!(source.source, Source::Closed));
        assert!(matches!(
            source.iter(),
            Err(crate::cypher::Error::Storage(
                crate::HelixDbError::InvariantViolation(_)
            ))
        ));
        assert!(matches!(
            source.extend(&context, limits).await,
            Err(crate::cypher::Error::Storage(
                crate::HelixDbError::InvariantViolation(_)
            ))
        ));
        drop(held);
        drop(source);
        assert_eq!(context.row_budget().available(), limits.memory_bytes);
    }
    context.close_request_read_view().unwrap();
    drop(context);
    db.close().await.unwrap();
}

#[tokio::test]
async fn disconnected_products_match_an_independent_model_and_optional_boundaries() {
    let db = test_support::open_db("product-model").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:A {k:0}),(b:A {k:1}),(c:B {k:2}),(d:B {k:3}),(a)-[:R]->(c),(b)-[:R]->(d) ",
    ))
    .await
    .unwrap();
    let expected = (0..2)
        .flat_map(|a| {
            (2..4)
                .filter(move |b| a + b > 2)
                .map(move |b| vec![json!(a), json!(b)])
        })
        .collect::<Vec<_>>();
    for (text, mut expected) in [
        ("MATCH (a:A),(b:B) WHERE a.k+b.k>2 RETURN a.k,b.k", expected),
        (
            "MATCH (a:A),(b:B),(c:A) RETURN count(*),sum(a.k+b.k+c.k)",
            vec![vec![json!(8), json!(28)]],
        ),
        (
            "MATCH (a:A),(b:B) RETURN a.k,b.k ORDER BY a.k,b.k DESC SKIP 1 LIMIT 2",
            vec![vec![json!(0), json!(2)], vec![json!(1), json!(3)]],
        ),
        (
            "OPTIONAL MATCH (a:A),(b:Absent) RETURN a,b",
            vec![vec![json!(null), json!(null)]],
        ),
        (
            "MATCH (a:A),(b:Absent) RETURN count(*)",
            vec![vec![json!(0)]],
        ),
        (
            "MATCH (a:A)-[:R]->(b:B),(c:A) RETURN count(*),sum(b.k+c.k)",
            vec![vec![json!(4), json!(12)]],
        ),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let plan = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(plan.batch_consumer(0).is_some(), "{text}");
        let explanation = serde_json::to_value(plan.explain()).unwrap();
        assert_eq!(explanation["operators"][0]["blocking"], json!([]));
        assert!(explanation["notices"]
            .as_array()
            .unwrap()
            .iter()
            .any(|notice| notice["kind"] == "cartesian_product"));
        if !text.contains("ORDER BY") {
            expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
        }
        for batch_rows in [1, 2, 7] {
            for strategy in [
                plan.clone(),
                plan.clone().with_execution(r::RowExecution::Materialized),
                r::RowPlan::reference(query.clone()).unwrap(),
            ] {
                let mut actual = rows::Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &strategy,
                        &std::collections::BTreeMap::new(),
                        Limits {
                            batch_rows,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .rows;
                if !text.contains("ORDER BY") {
                    actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                }
                assert_eq!(actual, expected, "{text}; batch {batch_rows}");
            }
        }
    }
    for text in [
        "MATCH (a:A),(b:B) WHERE 1/0>0 RETURN a LIMIT 0",
        "MATCH (a:A),(b:B) WHERE 1/0>0 RETURN count(*)",
    ] {
        let error = db
            .cypher(crate::cypher::Request::new(text))
            .await
            .unwrap_err();
        assert!(matches!(error,crate::cypher::Error::Query(e) if e.detail=="DivisionByZero"));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn product_cardinality_does_not_determine_retained_memory_or_source_reads() {
    let db = test_support::open_db("product-budget").await;
    db.cypher(crate::cypher::Request::new("UNWIND range(1,64) AS k CREATE (:A {k:k}) WITH k WHERE k<=32 CREATE (:B {k:k}) WITH k WHERE k<=16 CREATE (:C {k:k})")).await.unwrap();
    let text = "MATCH (a:A),(b:B),(c:C) RETURN count(*),sum(a.k),sum(b.k),sum(c.k)";
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(plan.batch_consumer(0).is_some());
    let expected = vec![vec![
        json!(64 * 32 * 16),
        json!((1..=64).sum::<u64>() * 32 * 16),
        json!((1..=32).sum::<u64>() * 64 * 16),
        json!((1..=16).sum::<u64>() * 64 * 32),
    ]];
    for batch_rows in [8, 17] {
        let limits = Limits {
            memory_bytes: 256 * 1024,
            batch_rows,
            ..Default::default()
        };
        let response = rows::Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &std::collections::BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(response.rows, expected);
        assert!(response.resources.peak_memory_bytes <= limits.memory_bytes);
        assert_eq!(response.resources.reads.scans, 0);
        assert_eq!(
            response.resources.reads.point_gets, 3,
            "each label source is opened once"
        );
    }
    let error = rows::Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan.with_execution(r::RowExecution::Materialized),
            &std::collections::BTreeMap::new(),
            Limits {
                memory_bytes: 256 * 1024,
                batch_rows: 8,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error,crate::cypher::Error::Query(ref e) if e.detail=="MemoryLimit")
            || matches!(
                error,
                crate::cypher::Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)
            )
    );
    db.close().await.unwrap();
}
