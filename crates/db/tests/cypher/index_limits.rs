use super::database;
use db::cypher;
use helix_ast::{batch, index, query, traversal};
use serde_json::json;

async fn index_state(db: &db::HelixDB, spec: index::IndexSpec) -> String {
    let create = batch::write_batch()
        .var_as("operation", traversal::g().create_index_if_not_exists(spec))
        .returning(["operation"]);
    let receipt = db.query(query::QueryRequest::write(create)).await.unwrap();
    let operation = receipt["operation"]["operation_id"].as_str().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let status_query = batch::read_batch()
                .var_as("status", traversal::g().get_index_operation(operation))
                .returning(["status"]);
            let status = db
                .query(query::QueryRequest::read(status_query))
                .await
                .unwrap();
            match status["status"]["status"].as_str() {
                Some("queued" | "running") => tokio::task::yield_now().await,
                state => return state.unwrap().to_owned(),
            }
        }
    })
    .await
    .expect("index operation settles")
}

fn request(text: &str, parameters: serde_json::Value) -> cypher::Request {
    serde_json::from_value(json!({"query": text, "parameters": parameters})).unwrap()
}

/// SlateDB panics on a key longer than `u16::MAX` bytes. An indexed value
/// whose key cannot fit fails its statement, or blocks an index build over
/// existing data, instead of reaching storage.
#[tokio::test]
async fn indexed_values_beyond_the_storage_key_limit_fail_cleanly() {
    let db = database().await;
    let oversized = "x".repeat(65_500);
    let fitting = "y".repeat(60_000);

    db.cypher(request(
        "CREATE (:Old {s: $s})",
        json!({"s": oversized.clone()}),
    ))
    .await
    .unwrap();
    assert_eq!(
        index_state(&db, index::IndexSpec::node_equality("Old", "s")).await,
        "blocked"
    );
    assert_eq!(
        index_state(&db, index::IndexSpec::node_range("Old", "s")).await,
        "blocked"
    );

    assert_eq!(
        index_state(&db, index::IndexSpec::node_equality("K", "s")).await,
        "succeeded"
    );
    assert_eq!(
        index_state(&db, index::IndexSpec::node_range("R", "s")).await,
        "succeeded"
    );
    for text in ["CREATE (:K {s: $s})", "CREATE (:R {s: $s})"] {
        let error = db
            .cypher(request(text, json!({"s": oversized.clone()})))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("too large"), "{text}: {error}");
    }
    db.cypher(request(
        "CREATE (:K {s: $s})",
        json!({"s": fitting.clone()}),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.cypher(request(
            "MATCH (k:K {s: $s}) RETURN count(*) AS c",
            json!({"s": fitting})
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(1)]]
    );
    db.close().await.unwrap();
}
