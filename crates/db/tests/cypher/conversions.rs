use super::{database, run};
use db::cypher;
use helix_ast::{batch, expr, index, query, traversal};
use serde_json::json;

#[test]
fn decimal_keys_remain_distinct_in_unique_indexes_and_lossless_results() {
    // Match the native index lifecycle suite's stack allowance.
    std::thread::Builder::new()
        .name("cypher-decimal-index-contract".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(verify_decimal_keys());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn verify_decimal_keys() {
    let db = database().await;
    assert_eq!(
        run(&db, "RETURN toInt('9007199254740993.0')").await.rows,
        vec![vec![json!({"$type":"integer", "value":"9007199254740993"})]]
    );
    run(&db, "CREATE (:ExactInteger {key:0})").await;
    let receipt = db
        .query(query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(
                        index::IndexSpec::node_unique_equality("ExactInteger", "key"),
                    ),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    let operation = receipt["operation"]["operation_id"].as_str().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let status = db
                .query(query::QueryRequest::read(
                    batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(operation))
                        .returning(["status"]),
                ))
                .await
                .unwrap();
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::task::yield_now().await,
                state => panic!("unexpected index state: {state:?}"),
            }
        }
    })
    .await
    .unwrap();
    let result = run(&db, "UNWIND ['9007199254740992.0','9007199254740993.0','9223372036854775807.0','-9223372036854775808.0'] AS text CREATE (n:ExactInteger {key:toInteger(text)}) RETURN n.key AS value ORDER BY value").await;
    let keys = [
        i64::MIN,
        9_007_199_254_740_992,
        9_007_199_254_740_993,
        i64::MAX,
    ];
    assert_eq!(result.columns, ["value"]);
    assert_eq!(
        result.rows,
        keys.map(|value| vec![json!({"$type":"integer", "value":value.to_string()})])
    );
    for key in keys {
        let native = db
            .query(query::QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "rows",
                        traversal::g()
                            .n_with_label_where("ExactInteger", expr::Predicate::eq("key", key))
                            .value_map(Some(vec!["key"])),
                    )
                    .returning(["rows"]),
            ))
            .await
            .unwrap();
        assert_eq!(native["rows"].as_array().unwrap().len(), 1, "{key}");
        assert_eq!(native["rows"][0]["key"], json!(key));
    }
    // A later error must leave both the graph and its unique
    // index at the previously committed exact integer value.
    let error = db.cypher(cypher::Request::new("MATCH (n:ExactInteger {key:9007199254740993}) SET n.key=toInteger('9007199254740995.0') RETURN 1/0")).await.unwrap_err();
    assert!(matches!(error, cypher::Error::Query(error)
    if error.category == "ArithmeticError" && error.detail == "DivisionByZero"
        && error.phase == helix_planner::relational::ErrorPhase::Runtime));
    assert_eq!(
        run(
            &db,
            "MATCH (n:ExactInteger {key:9007199254740993}) RETURN count(n)"
        )
        .await
        .rows,
        vec![vec![json!(1)]]
    );
    assert_eq!(
        run(
            &db,
            "MATCH (n:ExactInteger {key:9007199254740995}) RETURN count(n)"
        )
        .await
        .rows,
        vec![vec![json!(0)]]
    );
    // Null from an out-of-range conversion removes the stored
    // property through the existing mutation/index boundary.
    run(
        &db,
        "MATCH (n:ExactInteger {key:9007199254740993}) SET n.key=toInteger('-9223372036854775809')",
    )
    .await;
    assert_eq!(
        run(
            &db,
            "MATCH (n:ExactInteger {key:9007199254740993}) RETURN count(n)"
        )
        .await
        .rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        run(
            &db,
            "MATCH (n:ExactInteger) WHERE n.key IS NULL RETURN count(n)"
        )
        .await
        .rows,
        vec![vec![json!(1)]]
    );
    db.close().await.unwrap();
}
