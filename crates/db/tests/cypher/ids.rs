use super::{database, run};
use db::cypher;
use serde_json::json;

fn request(text: &str, parameters: serde_json::Value) -> cypher::Request {
    serde_json::from_value(json!({"query": text, "parameters": parameters})).unwrap()
}

/// `id(n) = v` and `id(n) IN [...]` read the named nodes directly. Each result
/// equals the same query written so that it scans, and pattern validation
/// still rejects nodes of another label, deleted nodes and missing IDs.
#[tokio::test]
async fn id_predicates_read_nodes_directly_with_scan_semantics() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 199) AS i CREATE (:Item {k: i})-[:R]->(:Other {k: i})",
    )
    .await;
    let ids = |label: &str, k: i64| {
        let db = &db;
        let text = format!("MATCH (n:{label} {{k: {k}}}) RETURN id(n) AS id");
        async move { run(db, &text).await.rows[0][0].as_i64().unwrap() }
    };
    let item = ids("Item", 7).await;
    let other = ids("Other", 7).await;
    let deleted = ids("Item", 9).await;
    run(&db, "MATCH (n:Item {k: 9}) DETACH DELETE n").await;
    let missing = 1_000_000;
    for (text, parameters) in [
        (
            "MATCH (n:Item) WHERE id(n) = $id RETURN n.k AS k",
            json!({"id": item}),
        ),
        (
            "MATCH (n:Item) WHERE id(n) = $id RETURN n.k AS k",
            json!({"id": other}),
        ),
        (
            "MATCH (n) WHERE id(n) = $id RETURN n.k AS k",
            json!({"id": other}),
        ),
        (
            "MATCH (n:Item) WHERE id(n) IN $ids RETURN n.k AS k ORDER BY k",
            json!({"ids": [item, other, deleted, missing]}),
        ),
        (
            "MATCH (n:Item) WHERE id(n) = $id RETURN n.k AS k",
            json!({"id": null}),
        ),
        (
            "MATCH (n:Item) WHERE id(n) = $id RETURN n.k AS k",
            json!({"id": -1}),
        ),
        (
            "MATCH (n:Item)-[:R]->(m) WHERE id(n) = $id RETURN m.k AS k",
            json!({"id": item}),
        ),
    ] {
        let direct = db
            .cypher(request(text, parameters.clone()))
            .await
            .unwrap_or_else(|error| panic!("{text}: {error}"));
        // `+ 0` makes the WHERE fallible, so the reference scans the label.
        let scanned = db
            .cypher(request(
                &text.replace("id(n)", "(id(n) + 0)"),
                parameters.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(direct.rows, scanned.rows, "{text} {parameters}");
    }
    let direct = db
        .cypher(request(
            "MATCH (n:Item) WHERE id(n) = $id RETURN n.k AS k",
            json!({"id": item}),
        ))
        .await
        .unwrap();
    assert_eq!(direct.rows, vec![vec![json!(7)]]);
    assert!(
        direct.resources.reads.multi_get_keys + direct.resources.reads.point_gets <= 8,
        "{:?}",
        direct.resources.reads
    );
    db.close().await.unwrap();
}
