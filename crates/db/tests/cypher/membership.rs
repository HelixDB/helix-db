use super::{database, run};
use db::cypher;
use helix_ast::{batch, expr, index, query, traversal, value};
use serde_json::json;

#[test]
fn wide_native_and_cypher_membership_match_an_independent_graph_model() {
    // Match the native index lifecycle suite's stack allowance. Keep the async
    // contract separate from runtime/stack setup so its cases stay readable.
    std::thread::Builder::new()
        .name("cypher-membership-contract".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(verify_membership());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn verify_membership() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0,15) AS i CREATE (:Membership {key:i}),(:Other {key:i})",
    )
    .await;
    // Execute before and after index activation, checking the same independent
    // model against both frontend and access choices.
    for indexed in [false, true] {
        if indexed {
            let create = batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(index::IndexSpec::node_equality(
                        "Membership",
                        "key",
                    )),
                )
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
                        Some("succeeded") => break,
                        Some("queued" | "running") => tokio::task::yield_now().await,
                        state => panic!("unexpected index state: {state:?}"),
                    }
                }
            })
            .await
            .unwrap();
        }
        for keys in [
            Vec::new(),
            (0..4096).rev().collect::<Vec<i64>>(),
            vec![7; 4096],
            (100..4196).collect(),
        ] {
            let expected: Vec<_> = (0..16_i64)
                .filter(|key| keys.contains(key))
                .map(|key| vec![json!(key)])
                .collect();
            let request: cypher::Request = serde_json::from_value(json!({
                "query":"MATCH (n:Membership) WHERE n.key IN $keys RETURN n.key ORDER BY n.key",
                "parameters":{"keys":keys}
            }))
            .unwrap();
            assert_eq!(db.cypher(request).await.unwrap().rows, expected);
            let read = batch::read_batch()
                .var_as(
                    "nodes",
                    traversal::g()
                        .n_with_label_where(
                            "Membership",
                            expr::Predicate::is_in("key", value::PropertyValue::I64Array(keys)),
                        )
                        .order_by("key", traversal::Order::Asc)
                        .value_map(Some(vec!["key"])),
                )
                .returning(["nodes"]);
            let native = db.query(query::QueryRequest::read(read)).await.unwrap();
            let actual: Vec<_> = native["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| vec![node["key"].clone()])
                .collect();
            assert_eq!(actual, expected, "indexed: {indexed}");
        }
    }
    db.close().await.unwrap();
}
