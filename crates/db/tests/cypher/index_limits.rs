use super::database;
use db::cypher;
use helix_ast::{batch, expr, index, query, traversal};
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
    let oversized = "x".repeat(usize::from(u16::MAX));
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

/// An unscoped equality key holds 36 bytes around a string and a range key
/// 30, so the longest strings whose keys fit are indexed, including by a
/// build over existing data, found, changed and deleted, and one byte more
/// fails its statement. Lookups of a value too large to have been indexed
/// find nothing rather than failing.
#[tokio::test]
async fn values_at_the_storage_key_limit_stay_maintainable() {
    let db = database().await;
    db.cypher(request(
        "CREATE (:BuiltK {s: $k}), (:BuiltR {s: $r})",
        json!({"k": "x".repeat(65_499), "r": "x".repeat(65_505)}),
    ))
    .await
    .unwrap();
    for spec in [
        index::IndexSpec::node_equality("BuiltK", "s"),
        index::IndexSpec::node_range("BuiltR", "s"),
        index::IndexSpec::node_equality("K", "s"),
        index::IndexSpec::node_unique_equality("U", "s"),
        index::IndexSpec::node_range("R", "s"),
    ] {
        assert_eq!(index_state(&db, spec).await, "succeeded");
    }
    for (label, longest) in [("K", 65_499), ("U", 65_499), ("R", 65_505)] {
        let fitting = "x".repeat(longest);
        let count = async |text: String, s: &str| {
            db.cypher(request(&text, json!({"s": s})))
                .await
                .unwrap()
                .rows
        };
        db.cypher(request(
            &format!("CREATE (:{label} {{s: $s}})"),
            json!({"s": fitting.clone()}),
        ))
        .await
        .unwrap();
        let error = db
            .cypher(request(
                &format!("CREATE (:{label} {{s: $s}})"),
                json!({"s": "x".repeat(longest + 1)}),
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("too large"), "{label}: {error}");
        for text in [
            format!("MATCH (n:{label} {{s: $s}}) RETURN count(*)"),
            format!("MATCH (n:{label}) WHERE n.s >= $s RETURN count(*)"),
            format!("MATCH (n:{label}) RETURN count(*)"),
        ] {
            assert_eq!(
                count(text.clone(), &fitting).await,
                vec![vec![json!(1)]],
                "{text}"
            );
        }
        db.cypher(request(
            &format!("MATCH (n:{label} {{s: $s}}) SET n.s = 'moved'"),
            json!({"s": fitting.clone()}),
        ))
        .await
        .unwrap();
        for (value, expected) in [(fitting.as_str(), 0), ("moved", 1)] {
            assert_eq!(
                count(
                    format!("MATCH (n:{label} {{s: $s}}) RETURN count(*)"),
                    value
                )
                .await,
                vec![vec![json!(expected)]],
                "{label}"
            );
        }
        db.cypher(request(
            &format!("MATCH (n:{label}) SET n.s = $s WITH n DETACH DELETE n"),
            json!({"s": fitting.clone()}),
        ))
        .await
        .unwrap();
        assert_eq!(
            count(
                format!("MATCH (n:{label} {{s: $s}}) RETURN count(*)"),
                &fitting
            )
            .await,
            vec![vec![json!(0)]],
            "{label}"
        );
    }

    let unindexable = "y".repeat(100_000);
    db.cypher(request("CREATE (:K {s: 'z'}), (:R {s: 'z'})", json!({})))
        .await
        .unwrap();
    for (text, expected) in [
        ("MATCH (n:K {s: $s}) RETURN count(*)", 0),
        ("MATCH (n:U {s: $s}) RETURN count(*)", 0),
        ("MATCH (n:R) WHERE n.s >= $s RETURN count(*)", 1),
        ("MATCH (n:R) WHERE n.s < $s RETURN count(*)", 0),
    ] {
        assert_eq!(
            db.cypher(request(text, json!({"s": unindexable.clone()})))
                .await
                .unwrap()
                .rows,
            vec![vec![json!(expected)]],
            "{text}"
        );
    }
    let native = db
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "found",
                    traversal::g()
                        .n_with_label_where("K", expr::Predicate::eq("s", unindexable.clone())),
                )
                .returning(["found"]),
        ))
        .await
        .unwrap();
    assert_eq!(native["found"], json!([]));
    db.close().await.unwrap();
}
