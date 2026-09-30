use super::*;
use helix_planner::context;

mod admission_points;
mod aggregation_admission;
mod bound_match;
mod computed_depth;
mod correlated;
mod correlated_sources;
mod cursors;
mod deletion_hydration;
mod direct_aggregation_ownership;
mod direct_aggregation_transfer;
mod distinct_batches;
mod encoding;
mod hash_join;
mod mixed_pipeline;
mod mutation_hydration;
mod mutations;
mod optional_windows;
mod pipeline;
mod projection_admission;
mod projection_chain;
mod projection_ownership;
mod projection_windows;
mod property_admission;
mod reference;
mod requirements;
mod response_admission;
mod row_layout;
mod selection;
mod unwind;
mod values;
mod where_windows;

#[tokio::test]
async fn cancellation_at_execution_checkpoints_rolls_back_rows() {
    for checks in [0, 1, 4, 12, 40] {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: format!("cypher-cancellation-{checks}"),
        })
        .await
        .unwrap();
        let query =
            helix_cypher::compile("UNWIND range(1,1000) AS i CREATE (:Cancelled {i:i})").unwrap();
        let plan = r::plan(query, &context::PlannerContext::default()).unwrap();
        let interpreter = Interpreter::new(&db, context::ParamBindings::default());
        interpreter.ctx.fail_deadline_after(checks);
        let error = interpreter
            .execute_rows(&plan, &BTreeMap::new(), Limits::default())
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)
            ),
            "{checks}: {error:?}"
        );
        let after = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:Cancelled) RETURN count(n)",
            ))
            .await
            .unwrap();
        assert_eq!(after.rows, vec![vec![serde_json::json!(0)]]);
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn every_cancellation_checkpoint_before_commit_rolls_back_a_small_write() {
    let mut reached_commit = false;
    for checkpoints in 0..64 {
        let db =
            crate::execution::interpreter::test_support::open_db("cypher-commit-checkpoints").await;
        let query =
            helix_cypher::compile("CREATE (:Checkpoint {key:1}) RETURN 1 AS value").unwrap();
        let plan = r::plan(
            query,
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let interpreter = Interpreter::new(&db, context::ParamBindings::default());
        interpreter.ctx.fail_deadline_after(checkpoints);
        let result = interpreter
            .execute_rows(&plan, &BTreeMap::new(), Limits::default())
            .await;
        let expected = match result {
            Ok(response) => {
                assert_eq!(response.rows, vec![vec![serde_json::json!(1)]]);
                reached_commit = true;
                1
            }
            Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)) => 0,
            Err(error) => panic!("{checkpoints}: {error:?}"),
        };
        let after = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:Checkpoint) RETURN count(*)",
            ))
            .await
            .unwrap();
        assert_eq!(
            after.rows,
            vec![vec![serde_json::json!(expected)]],
            "checkpoint {checkpoints}"
        );
        db.close().await.unwrap();
        if reached_commit {
            break;
        }
    }
    assert!(
        reached_commit,
        "all pre-commit checkpoints must have been exercised"
    );
}

mod label_completion;
mod node_existence;

mod lookup_frames;
mod match_producers;

/// A relation fits its budget up to exactly its owned row bytes. Every caller
/// admits rows first, so this check is the backstop for an unadmitted build.
#[test]
fn relation_memory_check_accepts_owned_bytes_up_to_the_budget() {
    let rows = vec![
        vec![r::Value::String("x".repeat(64)), r::Value::Null],
        vec![r::Value::List(vec![r::Value::Integer(1); 8])],
    ];
    let bytes = rows_bytes(&rows);
    for (memory_bytes, fits) in [(bytes, true), (bytes - 1, false)] {
        let result = check_memory(
            &rows,
            Limits {
                memory_bytes,
                ..Limits::default()
            },
        );
        assert_eq!(result.is_ok(), fits, "{memory_bytes} bytes");
        let Err(error) = result else {
            continue;
        };
        assert!(matches!(error, Error::Query(error) if error.detail == "MemoryLimit"));
    }
}

/// Response sizing counts the exact JSON encoding without buffering it, so
/// flushing the counter is a no-op that leaves the count unchanged.
#[test]
fn wire_size_counts_serialized_bytes_and_flushes_nothing() {
    let value = serde_json::json!({"columns":["a"],"rows":[[1,"two",null]]});
    let mut wire = WireSize { bytes: 0 };
    serde_json::to_writer(&mut wire, &value).unwrap();
    std::io::Write::flush(&mut wire).unwrap();
    assert_eq!(wire.bytes, serde_json::to_vec(&value).unwrap().len());
}
