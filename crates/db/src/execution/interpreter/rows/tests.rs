use super::*;
use helix_planner::context;

mod cursors;
mod mixed_pipeline;
mod mutations;
mod pipeline;
mod projection_chain;
mod projection_windows;
mod unwind;
mod values;

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
