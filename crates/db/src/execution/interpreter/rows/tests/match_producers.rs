use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn matches_after_barriers_keep_the_actual_input_relation() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("match-barrier-model").with_equality_index("A", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:A {key:0}),(b:A {key:1}),(c:A {key:1}),(:A {key:2}),\
         (a)-[:R]->(b),(a)-[:R]->(c),(b)-[:R]->(b)",
    ))
    .await
    .unwrap();
    for (text, expected) in [
        ("WITH 1 AS key MATCH (a:A {key:key}) RETURN a.key", vec![vec![json!(1)];2]),
        ("UNWIND [1,1,2] AS key WITH DISTINCT key MATCH (a:A {key:key}) RETURN key,a.key ORDER BY key LIMIT 20",vec![vec![json!(1),json!(1)],vec![json!(1),json!(1)],vec![json!(2),json!(2)]]),
        ("UNWIND [1,2,1] AS key WITH key ORDER BY key DESC MATCH (a:A {key:key}) RETURN key,a.key",vec![vec![json!(2),json!(2)],vec![json!(1),json!(1)],vec![json!(1),json!(1)],vec![json!(1),json!(1)],vec![json!(1),json!(1)]]),
        ("UNWIND [1,1] AS key WITH sum(key) AS key MATCH (a:A {key:key}) RETURN a.key",vec![vec![json!(2)]]),
        ("UNWIND [] AS key WITH DISTINCT key MATCH (a:A) RETURN count(*)",vec![vec![json!(0)]]),
        ("UNWIND [] AS key WITH DISTINCT key OPTIONAL MATCH (a:A) RETURN count(*)",vec![vec![json!(0)]]),
        ("UNWIND [] AS key WITH count(*) AS key MATCH (a:A {key:key}) RETURN a.key",vec![vec![json!(0)]]),
        ("UNWIND [3,3,null] AS key WITH DISTINCT key OPTIONAL MATCH (a:A {key:key}) RETURN count(*),count(a)",vec![vec![json!(2),json!(0)]]),
        ("MATCH (a:A {key:0}) WITH DISTINCT a MATCH p=(a)-[:R]->(b) RETURN a.key,b.key,length(p)",vec![vec![json!(0),json!(1),json!(1)];2]),
        ("MATCH (a:A {key:0})-[r:R]->(b) WITH DISTINCT a,r,b MATCH p=(a)-[r]->(b) RETURN a.key,b.key,length(p)",vec![vec![json!(0),json!(1),json!(1)];2]),
        ("MATCH (a:A {key:0}) WITH DISTINCT a MATCH (a),(a) RETURN a.key",vec![vec![json!(0)]]),
        ("WITH null AS a OPTIONAL MATCH (a)-[:R]->(b) RETURN a,b",vec![vec![json!(null),json!(null)]]),
        ("WITH null AS a MATCH (a)-[:R]->(b) RETURN count(*)",vec![vec![json!(0)]]),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let plan = r::plan(query.clone(), &db.planner_context(context::ParamBindings::default())).unwrap();
        let source = *plan.matches().keys().last().unwrap();
        assert!(source > 0 && plan.batch_consumer(source).is_some(), "{text}");
        assert!(plan.pipeline().input_window(source).is_none(), "later producers must drain: {text}");
        for batch_rows in [1,2,7] {
            for strategy in [plan.clone(),plan.clone().with_execution(r::RowExecution::Materialized),r::RowPlan::reference(query.clone()).unwrap()] {
                let result = Interpreter::new(&db,context::ParamBindings::default())
                    .execute_rows(&strategy,&BTreeMap::new(),Limits { batch_rows, ..Default::default() }).await.unwrap();
                assert_eq!(result.rows,expected,"{text}; batch={batch_rows}");
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_distinct_barrier_does_not_force_the_following_product_to_materialize() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("match-barrier-budget").with_equality_index("A", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,31) AS key CREATE (:A {key:key})",
    ))
    .await
    .unwrap();
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,255) AS key CREATE (:B {key:key})",
    ))
    .await
    .unwrap();
    let plan = r::plan(
        helix_cypher::compile("UNWIND range(0,31) AS key WITH DISTINCT key MATCH (a:A {key:key}),(b:B) RETURN count(*),sum(b.key)").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    ).unwrap();
    assert_eq!(plan.batch_consumer(0), None);
    assert_eq!(plan.batch_consumer(2), Some(r::BatchConsumer::Aggregate));
    assert!(plan.matches()[&2]
        .steps
        .iter()
        .any(|step| matches!(step, r::MatchStep::IndexLookup(_))));
    for batch_rows in [1, 8, 17] {
        let limits = Limits {
            batch_rows,
            memory_bytes: 192 * 1024,
            ..Default::default()
        };
        let result = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(
            result.rows,
            vec![vec![json!(32 * 256), json!(32 * 255 * 256 / 2)]]
        );
        assert!(result.resources.peak_memory_bytes <= limits.memory_bytes);
        assert_eq!(result.resources.reads.scans, 0);
    }
    let error = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan.with_execution(r::RowExecution::Materialized),
            &BTreeMap::new(),
            Limits {
                batch_rows: 8,
                memory_bytes: 192 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Query(error) if error.detail=="MemoryLimit"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn matches_after_writes_see_index_changes_and_drain_late_errors() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("match-barrier-writes").with_equality_index("A", "key"),
    )
    .await;
    for (text,expected) in [
        ("CREATE (a:A {key:17}) WITH a,17 AS key MATCH (b:A {key:key}) RETURN a=b,b.key",vec![vec![json!(true),json!(17)]]),
        ("MATCH (a:A {key:17}) SET a.key=18 WITH a,18 AS key MATCH (b:A {key:key}) RETURN a=b,b.key",vec![vec![json!(true),json!(18)]]),
    ] {
        let plan=r::plan(helix_cypher::compile(text).unwrap(),&db.planner_context(context::ParamBindings::default())).unwrap();
        let source=*plan.matches().keys().last().unwrap();
        assert!(source>0 && plan.batch_consumer(source).is_some());
        assert!(plan.matches()[&source].steps.iter().any(|step| matches!(step,r::MatchStep::IndexLookup(_))));
        assert_eq!(db.cypher(crate::cypher::Request::new(text)).await.unwrap().rows,expected);
    }
    db.cypher(crate::cypher::Request::new(
        "CREATE (:B {key:0}),(:B {key:1}),(:B {key:2})",
    ))
    .await
    .unwrap();
    for (text,detail) in [
        ("CREATE (:Rollback {key:1}) WITH 1 AS k MATCH (b:B) WHERE 1/(b.key-2)>0 RETURN 1 LIMIT 0","DivisionByZero"),
        ("UNWIND [1] AS a WITH DISTINCT a OPTIONAL MATCH (a)-[:R]->(b) RETURN b LIMIT 0","ExpectedNode"),
        ("UNWIND [null,1] AS a WITH DISTINCT a OPTIONAL MATCH (a) RETURN a LIMIT 0","ExpectedNode"),
    ] {
        let query=helix_cypher::compile(text).unwrap();
        let plan=r::plan(query.clone(),&db.planner_context(context::ParamBindings::default())).unwrap();
        assert!(plan.batch_consumer(*plan.matches().keys().last().unwrap()).is_some());
        for batch_rows in [1,2,7] {
            for strategy in [plan.clone(),plan.clone().with_execution(r::RowExecution::Materialized),r::RowPlan::reference(query.clone()).unwrap()] {
                let error=Interpreter::new(&db,context::ParamBindings::default()).execute_rows(&strategy,&BTreeMap::new(),Limits { batch_rows,..Default::default() }).await.unwrap_err();
                assert!(matches!(error,Error::Query(error) if error.detail==detail),"{text}");
            }
        }
    }
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (:Rollback) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (a:A {key:17}) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn match_producer_drop_and_cancellation_release_parents_sources_and_frames() {
    use super::super::bound_match::BoundMatch;
    use futures::StreamExt;
    let db = test_support::open_db("match-producer-ownership").await;
    db.cypher(crate::cypher::Request::new("CREATE (:A),(:A),(:B),(:B)"))
        .await
        .unwrap();
    let plan = r::plan(
        helix_cypher::compile("WITH 1 AS key MATCH (a:A),(b:B) RETURN key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let r::Operator::Match {
        pattern,
        optional,
        predicate,
    } = &plan.query().operators()[1]
    else {
        unreachable!()
    };
    let limits = Limits {
        batch_rows: 1,
        memory_bytes: 192 * 1024,
        ..Default::default()
    };
    for cancel_after in [None, Some(0), Some(1), Some(4), Some(12)] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let mut input = RowBuffer::new(ctx.row_budget()).unwrap();
        for _ in 0..32 {
            let mut row = vec![r::Value::Null; plan.query().bindings().len()];
            row[0] = r::Value::Integer(1);
            push_row(&mut input, row, limits).unwrap();
        }
        let parameters = BTreeMap::new();
        let stage = BoundMatch::new(
            matches::Match {
                pattern,
                optional: *optional,
                predicate: predicate.as_deref(),
                demand: usize::MAX,
            },
            &plan.matches()[&1],
        );
        let mut stream = Box::pin(stage.batches(input.finish(), &ctx, &parameters, limits));
        assert_eq!(
            ctx.row_budget().reads(),
            crate::cypher::StorageReadUsage::default()
        );
        if let Some(checkpoints) = cancel_after {
            let first = stream.next().await.unwrap().unwrap();
            assert_eq!(first.len(), 1);
            drop(first);
            assert!(ctx.row_budget().available() < limits.memory_bytes);
            ctx.fail_deadline_after(checkpoints);
            loop {
                match stream.next().await {
                    Some(Ok(_)) => {}
                    Some(Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))) => break,
                    result => panic!(
                        "expected interrupted producer: {}",
                        match result {
                            Some(Err(error)) => format!("{error:?}"),
                            _ => "exhausted".into(),
                        }
                    ),
                }
            }
            assert!(
                stream.next().await.is_none(),
                "a failed producer cannot reopen its sources"
            );
        }
        drop(stream);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    db.close().await.unwrap();
}
