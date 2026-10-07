use super::*;
use crate::execution::interpreter::{rows::memory, test_support};
use futures::StreamExt;
use helix_planner::context;
use std::collections::BTreeMap;

#[tokio::test]
async fn deferred_source_and_validation_share_snapshot_and_write_visibility() {
    use crate::encoding::v2::keys;
    let db = test_support::open_db("cypher-initial-membership-snapshot").await;
    let id = test_support::add_node_with_properties(&db, "N", vec![]).await;
    let plan = r::plan(
        helix_cypher::compile("MATCH (n:N) RETURN n").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let r::Operator::Match {
        pattern,
        optional,
        predicate,
    } = &plan.query().operators()[0]
    else {
        panic!("match fixture")
    };
    let operation = Match {
        pattern,
        optional: *optional,
        predicate: predicate.as_deref(),
        demand: usize::MAX,
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(128 * 1024));
    ctx.enable_request_read_view().await.unwrap();
    // Leave a stale label owner in the latest state. The already-open view
    // must still see both its posting and its node, without mixing epochs.
    db.inner_db()
        .delete(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                id,
            ))),
        )
        .await
        .unwrap();
    for visible in [true, false] {
        let source = ctx
            .initial_match_source(operation, &plan.matches()[&0])
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(source.membership, Membership::FreshLabel(_)));
        let parameters = BTreeMap::new();
        let mut stream = Box::pin(ctx.graph_match_batches(
            source,
            plan.query().bindings().len(),
            &parameters,
            crate::cypher::Limits::default(),
        ));
        let mut found = Vec::new();
        while let Some(batch) = stream.next().await {
            found.extend(batch.unwrap().iter().map(|row| row[0].clone()));
        }
        assert_eq!(
            found,
            if visible {
                vec![r::Value::Entity(r::Entity::Node(id))]
            } else {
                vec![]
            }
        );
        drop(stream);
        assert_eq!(ctx.row_budget().available(), 128 * 1024);
        ctx.close_request_read_view().unwrap();
        if visible {
            ctx.enable_request_read_view().await.unwrap();
        }
    }
    ctx.enable_request_write_scope().await.unwrap();
    let created = ctx.row_create_node("N", vec![]).await.unwrap();
    ctx.flush_active_index_mutations().await.unwrap();
    let source = ctx
        .initial_match_source(operation, &plan.matches()[&0])
        .await
        .unwrap()
        .unwrap();
    let parameters = BTreeMap::new();
    let mut stream = Box::pin(ctx.graph_match_batches(
        source,
        plan.query().bindings().len(),
        &parameters,
        crate::cypher::Limits::default(),
    ));
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch[0][0], r::Value::Entity(r::Entity::Node(created)));
    assert_eq!(batch.len(), 1);
    assert!(stream.next().await.is_none());
    drop(batch);
    drop(stream);
    ctx.abort_request_write_scope();
    assert_eq!(ctx.row_budget().available(), 128 * 1024);
    assert_eq!(
        db.cypher(crate::cypher::Request::new("MATCH (n:N) RETURN count(*)"))
            .await
            .unwrap()
            .rows,
        vec![vec![serde_json::json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn only_exact_initial_label_patterns_defer_membership() {
    let db = test_support::open_db("cypher-initial-membership-contract").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N)-[:R]->(:N)"))
        .await
        .unwrap();
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    ctx.enable_request_read_view().await.unwrap();
    for (text, deferred) in [
        ("MATCH (n:N) RETURN n", true),
        ("MATCH p=(n:N) RETURN p", true),
        ("OPTIONAL MATCH (n:N {key:1}) RETURN n", true),
        ("MATCH (n) RETURN n", false),
        ("MATCH (n:N),(n:N) RETURN n", false),
        ("MATCH (n:N)-[:R]->(m:N) RETURN n,m", false),
        ("MATCH (n:N),(m:N) RETURN n,m", false),
    ] {
        let plan = r::plan(
            helix_cypher::compile(text).unwrap(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let r::Operator::Match {
            pattern,
            optional,
            predicate,
        } = &plan.query().operators()[0]
        else {
            panic!("initial match fixture")
        };
        let operation = Match {
            pattern,
            optional: *optional,
            predicate: predicate.as_deref(),
            demand: usize::MAX,
        };
        let source = ctx
            .initial_match_source(operation, &plan.matches()[&0])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            matches!(source.membership, Membership::FreshLabel(_)),
            deferred,
            "{text}"
        );
        drop(source);
        assert_eq!(ctx.row_budget().available(), 1024 * 1024);
    }
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn initial_source_falls_back_before_opening_incompatible_plans() {
    let db = test_support::open_db("cypher-initial-source-fallback").await;
    let plan = r::plan(
        helix_cypher::compile("MATCH (n:N) RETURN n").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let r::Operator::Match {
        pattern,
        optional,
        predicate,
    } = &plan.query().operators()[0]
    else {
        panic!("match fixture")
    };
    let operation = Match {
        pattern,
        optional: *optional,
        predicate: predicate.as_deref(),
        demand: usize::MAX,
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(128 * 1024));
    // Deliberately exercise the compatibility boundary with physical schedules
    // that require another producer. None may open this initial cursor.
    for case in 0..5 {
        let mut schedule = plan.matches()[&0].clone();
        match case {
            0 => {
                schedule.incoming.insert(r::Slot(0));
            }
            1 => {
                schedule.steps.clear();
            }
            2 => {
                schedule.sources.clear();
            }
            3 => {
                schedule.sources[0].access = test_support::executable(
                    helix_planner::ir::PlanKind::Read,
                    vec![
                        test_support::step(1, vec![], exec::ExecOp::Noop),
                        test_support::step(
                            2,
                            vec![exec::ExecStepId::new(1).unwrap()],
                            exec::ExecOp::Noop,
                        ),
                    ],
                    2,
                )
                .into();
            }
            4 => {
                schedule.sources[0].access = test_support::executable(
                    helix_planner::ir::PlanKind::Read,
                    vec![test_support::step(1, vec![], exec::ExecOp::Noop)],
                    1,
                )
                .into();
            }
            _ => unreachable!(),
        }
        assert!(ctx
            .initial_match_source(operation, &schedule)
            .await
            .unwrap()
            .is_none());
        assert_eq!(ctx.row_budget().reads(), Default::default());
        assert_eq!(ctx.row_budget().available(), 128 * 1024);
    }
    ctx.enable_request_read_view().await.unwrap();
    let scan = r::RowPlan::reference(helix_cypher::compile("MATCH (n) RETURN n").unwrap()).unwrap();
    let r::Operator::Match {
        pattern,
        optional,
        predicate,
    } = &scan.query().operators()[0]
    else {
        panic!("scan fixture")
    };
    let operation = Match {
        pattern,
        optional: *optional,
        predicate: predicate.as_deref(),
        demand: usize::MAX,
    };
    ctx.fail_deadline_after(0);
    assert!(matches!(
        ctx.initial_match_source(operation, &scan.matches()[&0])
            .await,
        Err(crate::cypher::Error::Storage(
            crate::HelixDbError::QueryDeadlineExceeded
        ))
    ));
    assert_eq!(ctx.row_budget().available(), 128 * 1024);
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn deferred_initial_sources_release_ownership_on_drop_cancellation_and_admission_failure() {
    let db = test_support::open_db("cypher-initial-membership-lifetime").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,32) AS key CREATE (:N {key:key})",
    ))
    .await
    .unwrap();
    let plan = r::plan(
        helix_cypher::compile("MATCH (n:N) RETURN n").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let r::Operator::Match {
        pattern,
        optional,
        predicate,
    } = &plan.query().operators()[0]
    else {
        panic!("match fixture")
    };
    let operation = Match {
        pattern,
        optional: *optional,
        predicate: predicate.as_deref(),
        demand: usize::MAX,
    };
    let limits = crate::cypher::Limits {
        memory_bytes: 128 * 1024,
        batch_rows: 1,
        ..Default::default()
    };
    let parameters = BTreeMap::new();
    for stop in [0, 1, 2, 3] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        ctx.enable_request_read_view().await.unwrap();
        let source = ctx
            .initial_match_source(operation, &plan.matches()[&0])
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(source.membership, Membership::FreshLabel(_)));
        let mut stream = Box::pin(ctx.graph_match_batches(
            source,
            plan.query().bindings().len(),
            &parameters,
            limits,
        ));
        let held = if stop == 3 {
            Some(
                ctx.row_budget()
                    .reserve(ctx.row_budget().available())
                    .unwrap(),
            )
        } else {
            None
        };
        if stop == 1 {
            drop(stream.next().await.unwrap().unwrap());
        }
        if stop == 1 || stop == 2 {
            ctx.fail_deadline_after(0);
            assert!(matches!(
                stream.next().await,
                Some(Err(crate::cypher::Error::Storage(
                    crate::HelixDbError::QueryDeadlineExceeded
                )))
            ));
        }
        if stop == 3 {
            assert!(
                matches!(stream.next().await, Some(Err(crate::cypher::Error::Query(error))) if error.detail == "MemoryLimit")
            );
        }
        drop(stream);
        drop(held);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
        ctx.close_request_read_view().unwrap();
    }
    db.close().await.unwrap();
}
