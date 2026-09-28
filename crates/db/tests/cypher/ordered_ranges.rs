use super::membership::create_index;
use super::{database, run};
use db::cypher;
use helix_ast::index;
use serde_json::json;

/// ORDER BY a range-bounded, range-indexed property with a LIMIT reads the
/// index in order and stops after the window. Each result equals the same
/// query on a database without indexes; only sort keys are returned, since
/// ties may come in any order.
#[tokio::test]
async fn ordered_range_limits_read_only_the_window() {
    let indexed = database().await;
    let scanned = database().await;
    for db in [&indexed, &scanned] {
        run(
            db,
            "UNWIND range(0, 299) AS i CREATE (:User {uid: i, rank: i % 97, name: 'n' + toString(i)})",
        )
        .await;
        run(
            db,
            "CREATE (:User {uid: 900, rank: 50.5}), (:User {uid: 901}), (:User {uid: 902, rank: 'x'})",
        )
        .await;
    }
    create_index(&indexed, index::IndexSpec::node_range("User", "rank")).await;
    create_index(&indexed, index::IndexSpec::node_range("User", "name")).await;
    let request = |text: &str| -> cypher::Request {
        serde_json::from_value(json!({"query": text, "parameters": {"k": 4}})).unwrap()
    };
    for query in [
        "MATCH (u:User) WHERE u.rank >= 0 RETURN u.rank ORDER BY u.rank DESC LIMIT 5",
        "MATCH (u:User) WHERE u.rank >= 50 RETURN u.rank ORDER BY u.rank LIMIT 5",
        "MATCH (u:User) WHERE u.rank > 10 RETURN u.rank AS r ORDER BY r DESC SKIP 3 LIMIT $k",
        "MATCH (u:User) WHERE u.rank < 60 AND u.name STARTS WITH 'n1' RETURN u.rank ORDER BY u.rank DESC LIMIT 5",
        "MATCH (u:User) WHERE u.name > 'n2' RETURN u.name ORDER BY u.name LIMIT 5",
        "MATCH (u:User) WHERE u.rank >= 0 RETURN u.rank ORDER BY u.rank DESC LIMIT 0",
        "MATCH (u:User) WHERE u.rank > 1000 RETURN u.rank ORDER BY u.rank LIMIT 5",
    ] {
        let direct = indexed.cypher(request(query)).await.unwrap();
        let reference = scanned.cypher(request(query)).await.unwrap();
        assert_eq!(direct.rows, reference.rows, "{query}");
    }
    let direct = indexed
        .cypher(request(
            "MATCH (u:User) WHERE u.rank >= 0 RETURN u.uid ORDER BY u.rank DESC LIMIT 5",
        ))
        .await
        .unwrap();
    let reads = &direct.resources.reads;
    assert!(
        reads.scan_rows <= 16 && reads.point_gets <= 16 && reads.multi_get_keys <= 16,
        "{reads:?}"
    );
    indexed.close().await.unwrap();
    scanned.close().await.unwrap();
}
