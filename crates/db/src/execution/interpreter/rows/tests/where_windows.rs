//! A downstream window stops an initial MATCH whose WHERE cannot fail once
//! enough rows pass it. Candidate batches grow from the demand, so a WHERE
//! that rejects most candidates costs a few extra batches, not one per row.
use super::super::*;
use super::optional_windows::{execute, plan};
use serde_json::json;

#[tokio::test]
async fn total_where_predicates_let_windows_stop_initial_matches() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-where-window").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,4095) AS k CREATE (:W {k:k})",
    ))
    .await
    .unwrap();
    for (text, expected, max_keys) in [
        // Every candidate passes, so the demand-sized first batch suffices.
        (
            "MATCH (n:W) WHERE n.k >= 0 WITH n LIMIT 3 RETURN count(*)",
            json!([[3]]),
            Some(64),
        ),
        // Candidates pass only near the end of the label.
        (
            "MATCH (n:W) WHERE n.k >= 4000 WITH n LIMIT 3 RETURN count(*)",
            json!([[3]]),
            None,
        ),
        (
            "MATCH (n:W) WHERE n.k < 10 OR n.k > 4090 WITH n SKIP 2 LIMIT 3 RETURN count(*)",
            json!([[3]]),
            None,
        ),
        (
            "MATCH (n:W) WHERE n.k > 5000 WITH n LIMIT 1 RETURN count(*)",
            json!([[0]]),
            None,
        ),
    ] {
        let plan = plan(&db, text);
        assert!(plan.input_window(0).is_some(), "{text}");
        assert!(plan.batch_consumer(0).is_some(), "{text}");
        let reference = execute(&db, &plan, r::RowExecution::Materialized, 512)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(&reference.rows).unwrap(),
            expected,
            "{text}"
        );
        for batch_rows in [1, 7, 512] {
            let response = execute(&db, &plan, r::RowExecution::Batched, batch_rows)
                .await
                .unwrap();
            assert_eq!(response.rows, reference.rows, "{text} at {batch_rows}");
            let reads = response.resources.reads;
            if let Some(max_keys) = max_keys {
                assert!(
                    reads.multi_get_keys <= max_keys,
                    "{text} at {batch_rows}: {reads:?}"
                );
            }
            // Doubling from the demand reaches the batch width in a few steps;
            // a demand-sized batch per round trip would need over a thousand.
            if batch_rows == 512 {
                assert!(
                    reads.multi_get_batches <= 64,
                    "{text} at {batch_rows}: {reads:?}"
                );
            }
        }
    }
    // A WHERE that can fail keeps draining the source, so its error surfaces.
    let failing = plan(
        &db,
        "MATCH (n:W) WHERE n.k / (n.k - 4000) >= 0 WITH n LIMIT 1 RETURN count(*)",
    );
    assert!(failing.input_window(0).is_none());
    let error = execute(&db, &failing, r::RowExecution::Batched, 512)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, crate::cypher::Error::Query(error) if error.detail == "DivisionByZero"),
        "{error:?}"
    );
    db.close().await.unwrap();
}
