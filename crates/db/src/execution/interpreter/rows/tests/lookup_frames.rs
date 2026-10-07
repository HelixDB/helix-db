use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn index_probe_admission_rejects_before_copying_values_or_opening_storage() {
    use super::super::lookup;
    use futures::FutureExt;
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-probe-admission").with_equality_index("A", "key"),
    )
    .await;
    let plan = r::plan(
        helix_cypher::compile("UNWIND [1] AS key MATCH (a:A {key:key}) RETURN a.key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let [r::MatchStep::IndexLookup(lookup)] = plan.matches()[&1].steps.as_slice() else {
        unreachable!()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1));
    ctx.enable_request_read_view().await.unwrap();
    let (_, error_allocation) = crate::allocation_testing::observe(|| {
        Error::from(crate::HelixDbError::QueryMemoryLimitExceeded)
    });
    for value in [
        r::Value::String("payload".repeat(4096)),
        r::Value::Integer(1),
        r::Value::Float(f64::NAN),
    ] {
        let (result, allocations) = crate::allocation_testing::observe(|| {
            lookup::Probe::new(&ctx, lookup, &value)
                .now_or_never()
                .expect("admission precedes storage awaits")
        });
        assert!(matches!(result,Err(Error::Query(error)) if error.detail=="MemoryLimit"));
        assert_eq!(
            allocations.allocations, error_allocation.allocations,
            "only the structured error may allocate before rejection"
        );
        assert_eq!(allocations.bytes, error_allocation.bytes);
        assert_eq!(ctx.row_budget().available(), 1);
    }
    assert_eq!(
        ctx.row_budget().reads(),
        crate::cypher::StorageReadUsage::default()
    );
    db.close().await.unwrap();
}

/// A probe the index cannot answer exactly asks its caller to scan the source,
/// which then checks the pattern itself: a property of a value that is neither
/// an entity nor a map, and membership in a value that is not a list.
#[tokio::test]
async fn unindexable_probe_values_ask_for_a_source_scan() {
    use super::super::lookup;
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-unindexable-probes").with_equality_index("A", "key"),
    )
    .await;
    let property = r::plan(
        helix_cypher::compile("MATCH (b:B) WITH b MATCH (a:A {key:b.key}) RETURN a.key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let member = r::plan(
        helix_cypher::compile("WITH [1,2] AS keys MATCH (a:A) WHERE a.key IN keys RETURN a.key")
            .unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let [r::MatchStep::IndexLookup(property_lookup)] = property.matches()[&2].steps.as_slice()
    else {
        unreachable!("one property probe")
    };
    let [r::MatchStep::IndexLookup(member_lookup)] = member.matches()[&1].steps.as_slice() else {
        unreachable!("one membership probe")
    };
    assert!(property_lookup.probe_property.is_some());
    assert_eq!(member_lookup.matches, r::LookupMatch::Member);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(64 * 1024));
    ctx.enable_request_read_view().await.unwrap();
    let path = r::Value::Path(r::Path::new(vec![7], Vec::new()).unwrap());
    for value in [
        r::Value::Boolean(true),
        r::Value::Integer(1),
        r::Value::Float(1.0),
        r::Value::String("key".into()),
        r::Value::List(vec![r::Value::Integer(1)]),
        path.clone(),
    ] {
        let mut row = vec![r::Value::Null; property.query().bindings().len()];
        row[property_lookup.probe.0 as usize] = value.clone();
        let rows = [row];
        let mut probes = lookup::ProbeValues::default();
        let probe = probes
            .value(&ctx, property_lookup, &rows, 0, Limits::default())
            .await
            .unwrap();
        assert!(probe.is_none(), "{value:?}");
    }
    for value in [
        r::Value::Boolean(true),
        r::Value::Integer(1),
        r::Value::Float(1.0),
        r::Value::String("key".into()),
        r::Value::Map(BTreeMap::new()),
        r::Value::Entity(r::Entity::Node(7)),
        path,
    ] {
        let probe = lookup::Probe::new(&ctx, member_lookup, &value)
            .await
            .unwrap();
        assert!(matches!(probe, lookup::Probe::Scan), "{value:?}");
    }
    assert_eq!(ctx.row_budget().available(), 64 * 1024);
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

/// Membership and property probes admit every buffer before allocating it.
/// Whatever allowance remains, a probe either reads its candidates or fails
/// with MemoryLimit, and a failure releases every reservation it made.
#[tokio::test]
async fn probe_admission_failures_release_every_reservation() {
    use super::super::lookup;
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-probe-admission-sweep")
            .with_equality_index("A", "key"),
    )
    .await;
    let created = db
        .cypher(crate::cypher::Request::new(
            "CREATE (:A {key:1}),(:A {key:2}),(b:B {key:2}) RETURN id(b)",
        ))
        .await
        .unwrap();
    let b = created.rows[0][0].as_u64().unwrap();
    let property = r::plan(
        helix_cypher::compile("MATCH (b:B) WITH b MATCH (a:A {key:b.key}) RETURN a.key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let member = r::plan(
        helix_cypher::compile("WITH [1,2] AS keys MATCH (a:A) WHERE a.key IN keys RETURN a.key")
            .unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let [r::MatchStep::IndexLookup(property_lookup)] = property.matches()[&2].steps.as_slice()
    else {
        unreachable!("one property probe")
    };
    let [r::MatchStep::IndexLookup(member_lookup)] = member.matches()[&1].steps.as_slice() else {
        unreachable!("one membership probe")
    };
    let limits = Limits {
        memory_bytes: 256 * 1024,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    let mut row = vec![r::Value::Null; property.query().bindings().len()];
    row[property_lookup.probe.0 as usize] = r::Value::Entity(r::Entity::Node(b));
    let rows = [row];
    let members = r::Value::List(vec![
        r::Value::Integer(1),
        r::Value::Integer(2),
        r::Value::Integer(1),
    ]);
    let (mut failures, mut successes) = (0, 0);
    for available in (0..=16 * 1024).step_by(8) {
        let held = ctx
            .row_budget()
            .reserve(limits.memory_bytes - available)
            .unwrap();
        let member_probe = lookup::Probe::new(&ctx, member_lookup, &members)
            .await
            .map(|probe| matches!(probe, lookup::Probe::Index(_)));
        let mut probes = lookup::ProbeValues::default();
        let property_probe = probes
            .value(&ctx, property_lookup, &rows, 0, limits)
            .await
            .map(|value| value == Some(&r::Value::Integer(2)));
        drop(probes);
        for result in [member_probe, property_probe] {
            match result {
                Ok(expected) => {
                    assert!(expected, "{available}");
                    successes += 1;
                }
                Err(Error::Query(error)) if error.detail == "MemoryLimit" => failures += 1,
                Err(Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)) => failures += 1,
                Err(error) => panic!("{available}: {error:?}"),
            }
        }
        drop(held);
        assert_eq!(
            ctx.row_budget().available(),
            limits.memory_bytes,
            "{available}"
        );
    }
    assert!(failures > 0 && successes > 0);
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn indexed_pattern_frames_match_independent_products_and_paths() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("indexed-pattern-model")
            .with_equality_index("A", "key")
            .with_equality_index("B", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:A {key:0}),(b:A {key:1}),(:A {key:1}),(:A {key:2}),\
         (x:B {key:1}),(y:B {key:2}),(a)-[:R]->(x),(b)-[:R]->(x),(b)-[:R]->(y)",
    ))
    .await
    .unwrap();
    let mut products = Vec::new();
    let mut paths = Vec::new();
    for key in [Some(0), Some(1), Some(1), Some(2), Some(3), None] {
        let before = products.len();
        for a in [0, 1, 1, 2] {
            for b in [1, 2] {
                if key == Some(a) && key == Some(b) {
                    products.push(vec![json!(key), json!(a), json!(b)]);
                }
            }
        }
        if before == products.len() {
            products.push(vec![json!(key), json!(null), json!(null)]);
        }
        let before = paths.len();
        for (a, b) in [(0, 1), (1, 1), (1, 2)] {
            if key == Some(a) {
                paths.push(vec![json!(key), json!(a), json!(b), json!(1)]);
            }
        }
        if before == paths.len() {
            paths.push(vec![json!(key), json!(null), json!(null), json!(null)]);
        }
    }
    let mut observed = std::collections::BTreeSet::new();
    for (pattern, projection, mut expected) in [
        (
            "(a:A {key:key}),(b:B {key:key})",
            "key,a.key,b.key",
            products.clone(),
        ),
        (
            "(a:A {key:key}),(b:B) WHERE a.key=b.key",
            "key,a.key,b.key",
            products.clone(),
        ),
        (
            "(a:A),(b:B) WHERE a.key=key AND b.key=key",
            "key,a.key,b.key",
            products.clone(),
        ),
        (
            "(a {key:key}),(a:A),(b:B {key:key})",
            "key,a.key,b.key",
            products,
        ),
        (
            "p=(a:A {key:key})-[:R]->(b:B)",
            "key,a.key,b.key,length(p)",
            paths,
        ),
        (
            "(a:A {key:key}),(b:B)",
            "count(*),count(a)",
            vec![vec![json!(14), json!(12)]],
        ),
    ] {
        let text =
            format!("UNWIND [0,1,1,2,3,null] AS key OPTIONAL MATCH {pattern} RETURN {projection}");
        let query = helix_cypher::compile(&text).unwrap();
        let plan = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(
            matches!(
                plan.batch_consumer(0),
                Some(r::BatchConsumer::Pipeline { .. })
            ),
            "{text}"
        );
        let physical = &plan.matches()[&1];
        assert!(physical.steps.len() > 1);
        assert!(
            physical
                .steps
                .iter()
                .any(|step| matches!(step, r::MatchStep::IndexLookup(_))),
            "{text}"
        );
        for step in &physical.steps {
            observed.insert(match step {
                r::MatchStep::Scan(_) => "scan",
                r::MatchStep::IndexLookup(_) => "index",
                r::MatchStep::Expand { .. } => "expand",
                r::MatchStep::HashJoin { .. } => "hash",
            });
        }
        expected.sort_by_key(|row| serde_json::to_string(row).unwrap());
        for batch_rows in [1, 2, 7] {
            for strategy in [
                plan.clone(),
                plan.clone().with_execution(r::RowExecution::Materialized),
                r::RowPlan::reference(query.clone()).unwrap(),
            ] {
                let mut actual = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &strategy,
                        &BTreeMap::new(),
                        Limits {
                            batch_rows,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .rows;
                actual.sort_by_key(|row| serde_json::to_string(row).unwrap());
                assert_eq!(actual, expected, "{text}; batch={batch_rows}");
            }
        }
    }
    assert_eq!(
        observed,
        ["scan", "index", "expand", "hash"].into_iter().collect()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn lookup_cursor_transfers_release_admission_on_failure_and_drop() {
    use super::super::{expansion_stack::SourceCache, lookup_cursor::LookupCursor};
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-cursor-ownership").with_equality_index("A", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {key:1}),(:A {key:1}),(:A {key:2})",
    ))
    .await
    .unwrap();
    let plan = r::plan(
        helix_cypher::compile("UNWIND [1,2] AS key MATCH (a:A {key:key}) RETURN a.key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let physical = &plan.matches()[&1];
    let [r::MatchStep::IndexLookup(lookup)] = physical.steps.as_slice() else {
        unreachable!()
    };
    let mut closed_transfers = 0;
    let mut deferred_errors = 0;
    for fallback in [false, true] {
        for failure_after in [None, Some(0), Some(1), Some(2), Some(5), Some(16), Some(32)] {
            let limits = Limits {
                batch_rows: 4,
                memory_bytes: 128 * 1024,
                ..Default::default()
            };
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
            ctx.enable_request_read_view().await.unwrap();
            let mut input = RowBuffer::new(ctx.row_budget()).unwrap();
            for _ in 0..64 {
                let mut row = vec![r::Value::Null; plan.query().bindings().len()];
                row[lookup.probe.0 as usize] = if fallback {
                    r::Value::List(vec![r::Value::Integer(1)])
                } else {
                    r::Value::Integer(1)
                };
                push_row(&mut input, row, limits).unwrap();
            }
            let mut cursor = LookupCursor::new(input.finish(), ctx.row_budget()).unwrap();
            let mut cache =
                SourceCache::new(physical, 0, limits.batch_rows, ctx.row_budget()).unwrap();
            let available = ctx.row_budget().available();
            drop(cursor.next_batch(&ctx, lookup, &mut cache, limits));
            assert_eq!(
                ctx.row_budget().available(),
                available,
                "an unpolled future owns no allocation"
            );
            let first = cursor
                .next_batch(&ctx, lookup, &mut cache, limits)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first.len(), 4);
            drop(first);
            if let Some(checkpoints) = failure_after {
                ctx.fail_deadline_after(checkpoints);
                let mut returned_prefix = false;
                loop {
                    match cursor.next_batch(&ctx, lookup, &mut cache, limits).await {
                        Ok(Some(rows)) => {
                            assert!(!rows.is_empty());
                            returned_prefix = true;
                        }
                        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)) => break,
                        Ok(None) => panic!("expected cancellation before input exhaustion"),
                        Err(error) => panic!("unexpected cancellation error: {error:?}"),
                    }
                }
                deferred_errors += usize::from(returned_prefix);
                ctx.fail_deadline_after(usize::MAX);
                match cursor.next_batch(&ctx, lookup, &mut cache, limits).await {
                    Err(Error::Storage(crate::HelixDbError::InvariantViolation(_))) => {
                        closed_transfers += 1
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("input was not exhausted"),
                    Err(error) => panic!("unexpected state after consumed cancellation: {error:?}"),
                }
            }
            drop(cursor);
            drop(cache);
            assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
        }
    }
    assert!(
        closed_transfers > 0,
        "failed source transfers cannot reopen"
    );
    assert!(
        deferred_errors > 0,
        "earlier candidates precede a later source error"
    );
    db.close().await.unwrap();
}

/// A lookup level admits each parent's probe and candidates before reading
/// them. Whatever allowance remains, its first batch either holds every
/// candidate of the parent or fails with MemoryLimit, and dropping the cursor
/// and its source cache releases everything.
#[tokio::test]
async fn lookup_cursor_admission_failures_release_every_reservation() {
    use super::super::{expansion_stack::SourceCache, lookup_cursor::LookupCursor};
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-cursor-admission").with_equality_index("A", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {key:1}),(:A {key:1})",
    ))
    .await
    .unwrap();
    let plan = r::plan(
        helix_cypher::compile("UNWIND [1] AS key MATCH (a:A {key:key}) RETURN a.key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let physical = &plan.matches()[&1];
    let [r::MatchStep::IndexLookup(lookup)] = physical.steps.as_slice() else {
        unreachable!("one lookup")
    };
    let limits = Limits {
        batch_rows: 4,
        memory_bytes: 128 * 1024,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    for fallback in [false, true] {
        let (mut failures, mut successes) = (0, 0);
        for available in (0..=16 * 1024).step_by(8) {
            let mut row = vec![r::Value::Null; plan.query().bindings().len()];
            row[lookup.probe.0 as usize] = if fallback {
                r::Value::List(vec![r::Value::Integer(1)])
            } else {
                r::Value::Integer(1)
            };
            let mut input = RowBuffer::new(ctx.row_budget()).unwrap();
            push_row(&mut input, row, limits).unwrap();
            let mut cursor = LookupCursor::new(input.finish(), ctx.row_budget()).unwrap();
            let mut cache =
                SourceCache::new(physical, 0, limits.batch_rows, ctx.row_budget()).unwrap();
            let held = ctx
                .row_budget()
                .reserve(ctx.row_budget().available().saturating_sub(available))
                .unwrap();
            match cursor.next_batch(&ctx, lookup, &mut cache, limits).await {
                Ok(Some(rows)) => {
                    assert_eq!(rows.len(), 2);
                    successes += 1;
                }
                Err(Error::Query(error)) if error.detail == "MemoryLimit" => failures += 1,
                Err(Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)) => failures += 1,
                Ok(None) => panic!("the parent has candidates"),
                Err(error) => panic!("{available}: {error:?}"),
            }
            drop(held);
            drop(cursor);
            drop(cache);
            assert_eq!(
                ctx.row_budget().available(),
                limits.memory_bytes,
                "fallback={fallback}, available={available}"
            );
        }
        assert!(failures > 0 && successes > 0, "fallback={fallback}");
    }
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn indexed_pattern_fallbacks_share_sources_across_outer_batches() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("indexed-pattern-values")
            .with_equality_index("A", "key")
            .with_unique_equality_index("B", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {key:1}),(:A {key:1}),(:A {key:[1,2]}),\
         (:A {key:'text'}),(:A {key:true}),(:B {key:1}),\
         (:B {key:[1.0,2.0]}),(:B {key:'text'}),(:B {key:true})",
    ))
    .await
    .unwrap();
    let text = "UNWIND $probes AS key OPTIONAL MATCH (a:A {key:key}),(b:B {key:key}) RETURN count(*),count(a),count(b)";
    let query = helix_cypher::compile(text).unwrap();
    let plan = r::plan(
        query.clone(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(matches!(
        plan.batch_consumer(0),
        Some(r::BatchConsumer::Pipeline { .. })
    ));
    assert_eq!(
        plan.matches()[&1]
            .steps
            .iter()
            .filter(|step| matches!(step, r::MatchStep::IndexLookup(_)))
            .count(),
        2
    );
    let parameters = BTreeMap::from([(
        "probes".into(),
        r::Value::List(vec![
            r::Value::Integer(1),
            r::Value::Float(1.0),
            r::Value::List(vec![r::Value::Integer(1), r::Value::Integer(2)]),
            r::Value::List(vec![r::Value::Float(1.0), r::Value::Float(2.0)]),
            r::Value::Map(BTreeMap::new()),
            r::Value::Null,
            r::Value::Boolean(true),
            r::Value::String("text".into()),
            r::Value::Float(f64::NAN),
            r::Value::Float(f64::INFINITY),
        ]),
    )]);
    for batch_rows in [1, 2, 7] {
        for strategy in [
            plan.clone(),
            plan.clone().with_execution(r::RowExecution::Materialized),
            r::RowPlan::reference(query.clone()).unwrap(),
        ] {
            let actual = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &strategy,
                    &parameters,
                    Limits {
                        batch_rows,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(actual.rows, vec![vec![json!(12), json!(8), json!(8)]]);
        }
    }
    // A long duplicate outer stream must retain only admitted candidate batches,
    // even when both physical probes use their original source as a fallback.
    let text = "UNWIND range(1,2048) AS marker WITH marker,[1,2] AS key MATCH (a:A {key:key}),(b:B {key:key}) RETURN count(*),sum(marker)";
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    for batch_rows in [1, 8, 17] {
        let limits = Limits {
            batch_rows,
            memory_bytes: 192 * 1024,
            ..Default::default()
        };
        let actual = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&plan, &BTreeMap::new(), limits)
            .await
            .unwrap();
        assert_eq!(actual.rows, vec![vec![json!(2048), json!(2048 * 2049 / 2)]]);
        assert_eq!(
            actual.resources.reads.point_gets, 2,
            "each fallback source opens once"
        );
        assert_eq!(actual.resources.reads.scans, 0);
        assert!(actual.resources.peak_memory_bytes <= limits.memory_bytes);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn indexed_pattern_boundaries_preserve_writes_and_late_errors() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("indexed-pattern-writes").with_equality_index("A", "key"),
    )
    .await;
    assert_eq!(db.cypher(crate::cypher::Request::new(
        "CREATE (:A {key:7})-[:R]->(:B {key:7}) WITH 1 AS marker UNWIND [7] AS key MATCH (a:A {key:key})-[:R]->(b:B) RETURN a.key,b.key"
    )).await.unwrap().rows, vec![vec![json!(7),json!(7)]]);
    assert_eq!(db.cypher(crate::cypher::Request::new(
        "MATCH (a:A {key:7}) SET a.key=8 WITH 1 AS marker UNWIND [8] AS key MATCH (a:A {key:key})-[:R]->(b:B) RETURN a.key,b.key"
    )).await.unwrap().rows, vec![vec![json!(8),json!(7)]]);
    for (text, detail, indexed) in [
        ("CREATE (:Rollback) WITH 1 AS marker UNWIND [8] AS key MATCH (a:A {key:key})-[:R]->(b:B) RETURN 1/0", "DivisionByZero", true),
        ("UNWIND [null,8] AS key MATCH (a:A {key:key}),(b:B {other:1/0}) RETURN count(*)", "DivisionByZero", false),
        ("UNWIND [null,8] AS key MATCH (a:A),(b:B) WHERE a.key=key AND 1/0>0 RETURN count(*)", "DivisionByZero", false),
        ("UNWIND [null,8] AS key MATCH (a:A),(b:B) WHERE a.key=key AND b.key=$missing RETURN count(*)", "MissingParameter", false),
    ] {
        let plan=r::plan(helix_cypher::compile(text).unwrap(),&db.planner_context(context::ParamBindings::default())).unwrap();
        assert_eq!(plan.matches().values().any(|plan| plan.steps.iter().any(|step| matches!(step,r::MatchStep::IndexLookup(_)))), indexed, "{text}");
        assert!(plan.query().operators().iter().enumerate().any(|(index,operator)|
            matches!(operator,r::Operator::Unwind { .. }) && matches!(plan.batch_consumer(index),Some(r::BatchConsumer::Pipeline { .. }))),
            "the error/rollback test must exercise a streamed MATCH: {text}");
        for batch_rows in [1,2,7] {
            for strategy in [r::RowExecution::Batched,r::RowExecution::Materialized] {
                let error=Interpreter::new(&db,context::ParamBindings::default())
                    .execute_rows(&plan.clone().with_execution(strategy),&BTreeMap::new(),Limits { batch_rows, ..Default::default() }).await.unwrap_err();
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
    db.close().await.unwrap();
}

#[tokio::test]
async fn oversized_string_probes_scan_instead_of_failing_the_lookup() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-oversized-probe")
            .with_equality_index("A", "key")
            .with_unique_equality_index("U", "key"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (:A {key:'x'}), (:U {key:'y'})",
    ))
    .await
    .unwrap();
    // No indexed value can be this long, and storage rejects the lookup key.
    let big = r::Value::String("z".repeat(helix_planner::ir::MAX_INDEXED_EQUALITY_BYTES));
    let parameters = BTreeMap::from([("big".to_owned(), big)]);
    for (text, expected) in [
        (
            "WITH $big AS k MATCH (a:A {key: k}) RETURN count(*)",
            json!([[0]]),
        ),
        (
            "UNWIND [$big, 'x'] AS k MATCH (a:A) WHERE a.key = k RETURN count(*)",
            json!([[1]]),
        ),
        (
            "WITH $big AS k OPTIONAL MATCH (u:U {key: k}) RETURN count(*), count(u)",
            json!([[1, 0]]),
        ),
        (
            "WITH [$big, 'x'] AS keys MATCH (a:A) WHERE a.key IN keys RETURN count(*)",
            json!([[1]]),
        ),
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        assert!(
            plan.matches().values().any(|plan| plan
                .steps
                .iter()
                .any(|step| matches!(step, r::MatchStep::IndexLookup(_)))),
            "{text}"
        );
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let response = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &parameters,
                    Limits::default(),
                )
                .await
                .unwrap_or_else(|error| panic!("{text} {strategy:?}: {error}"));
            assert_eq!(
                serde_json::to_value(response.rows).unwrap(),
                expected,
                "{text} {strategy:?}"
            );
        }
    }
    db.close().await.unwrap();
}

/// A probe property that cannot be read is not an index key. The lookup scans
/// its source instead, so the unreadable value fails the query only once a
/// candidate exists, exactly as a plan without the index would.
#[tokio::test]
async fn unreadable_probe_properties_scan_and_fail_only_with_candidates() {
    use crate::encoding::v2::{
        keys,
        values::property::{self, property_value::PropertyValue as P, Property},
    };
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("lookup-unreadable-probe").with_equality_index("A", "key"),
    )
    .await;
    let created = db
        .cypher(crate::cypher::Request::new("CREATE (b:B {key:1}) RETURN b"))
        .await
        .unwrap();
    let id = created.rows[0][0]["id"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                id,
            ))),
            property::encode_properties(&[
                Property::string("$label", "B"),
                Property::new("key", P::DateTime(1)),
            ]),
        )
        .await
        .unwrap();
    drop(ctx);
    let plan = r::plan(
        helix_cypher::compile("MATCH (b:B) MATCH (a:A {key: b.key}) RETURN a.key").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    assert!(plan.matches()[&1]
        .steps
        .iter()
        .any(|step| matches!(step, r::MatchStep::IndexLookup(_))));
    for candidate in [false, true] {
        if candidate {
            db.cypher(crate::cypher::Request::new("CREATE (:A {key:1})"))
                .await
                .unwrap();
        }
        for strategy in [r::RowExecution::Batched, r::RowExecution::Materialized] {
            let result = Interpreter::new(&db, context::ParamBindings::default())
                .execute_rows(
                    &plan.clone().with_execution(strategy),
                    &BTreeMap::new(),
                    Limits::default(),
                )
                .await;
            match result {
                Ok(response) => {
                    assert!(!candidate, "{strategy:?}");
                    assert!(response.rows.is_empty(), "{strategy:?}");
                }
                Err(error) => assert!(
                    candidate
                        && matches!(error, Error::Query(ref error) if error.detail == "StoredValueType"),
                    "{strategy:?}: {error}"
                ),
            }
        }
    }
    db.close().await.unwrap();
}
