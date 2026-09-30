use super::*;
use crate::execution::interpreter::{test_support, Interpreter};
use helix_planner::context;
use serde_json::json;

/// Equality agrees with the ranking order: rows with equal sort keys still
/// differ by arrival ordinal, so ties keep their arrival order.
#[test]
fn ranked_rows_are_equal_only_at_the_same_rank() {
    let ordering = [r::Ordering {
        expression: r::Expression::Slot(r::Slot(0)),
        descending: true,
    }];
    let ranked = |key: i64, ordinal: usize| RankedRow {
        keys: vec![r::Value::Integer(key)],
        row: Vec::new(),
        ordering: &ordering,
        ordinal,
    };
    assert!(ranked(1, 0) == ranked(1, 0));
    assert!(ranked(1, 0) != ranked(1, 1));
    assert!(ranked(2, 1) != ranked(1, 1));
    // A descending key ranks the larger value first, before earlier arrivals.
    assert!(ranked(2, 1) < ranked(1, 0));
}

/// A WITH predicate over a binding the projection drops cannot follow the
/// projection, so it filters rows before ORDER BY and LIMIT rank them. A null
/// predicate drops its row as false does.
#[tokio::test]
async fn ranked_windows_filter_by_dropped_bindings_before_ranking() {
    let db = test_support::open_db("top-k-dropped-binding-predicate").await;
    for (text, expected) in [
        (
            "UNWIND [3,1,2,5,4] AS x WITH x AS y ORDER BY y LIMIT 2 WHERE x > 2 RETURN y",
            json!([[3], [4]]),
        ),
        (
            "UNWIND [3,null,2,5,4] AS x WITH coalesce(x,0) AS y ORDER BY y DESC LIMIT 2 WHERE x < 5 RETURN y",
            json!([[4], [3]]),
        ),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let plan = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        for strategy in [
            plan.clone(),
            plan.clone().with_execution(r::RowExecution::Materialized),
            r::RowPlan::reference(query).unwrap(),
        ] {
            for batch_rows in [1, 2, 7] {
                let response = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &strategy,
                        &BTreeMap::new(),
                        Limits {
                            batch_rows,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(response.rows).unwrap(),
                    expected,
                    "{text}; batch={batch_rows}"
                );
            }
        }
    }
    db.close().await.unwrap();
}

/// A ranked window admits each retained row before growing its heap. Under
/// any budget the query either returns the ranked rows or fails with
/// MemoryLimit; wide rows make each retained candidate the largest owner.
#[tokio::test]
async fn ranked_window_admission_failures_leave_no_partial_result() {
    let db = test_support::open_db("top-k-admission").await;
    let pad = "p".repeat(8 * 1024);
    let text = format!(
        "UNWIND [3,1,2,5,4] AS x WITH x, '{pad}' AS pad ORDER BY x DESC LIMIT 2 RETURN x, size(pad)"
    );
    let plan = r::plan(
        helix_cypher::compile(&text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let peak = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(&plan, &BTreeMap::new(), Limits::default())
        .await
        .unwrap()
        .resources
        .peak_memory_bytes;
    let (mut failures, mut successes) = (0, 0);
    for memory_bytes in (0..peak).step_by(64).chain([peak]) {
        match Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    memory_bytes,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(response) => {
                assert_eq!(
                    serde_json::to_value(response.rows).unwrap(),
                    json!([[5, 8192], [4, 8192]])
                );
                successes += 1;
            }
            Err(crate::cypher::Error::Query(error)) if error.detail == "MemoryLimit" => {
                failures += 1
            }
            Err(crate::cypher::Error::Storage(crate::HelixDbError::QueryMemoryLimitExceeded)) => {
                failures += 1
            }
            Err(error) => panic!("{memory_bytes}: {error:?}"),
        }
    }
    assert!(failures > 0 && successes > 0);
    db.close().await.unwrap();
}
