use super::membership::create_index;
use super::{database, run};
use db::cypher;
use helix_ast::index;

/// A later MATCH that constrains a node bound by an earlier one lets that
/// earlier MATCH read an index. Each result and error equals the same query
/// on a database without indexes.
#[tokio::test]
async fn later_match_constraints_read_the_earlier_index_with_scan_semantics() {
    let indexed = database().await;
    let scanned = database().await;
    for db in [&indexed, &scanned] {
        run(
            db,
            "UNWIND range(0, 199) AS i CREATE (:User {uid: i, tier: i % 3})",
        )
        .await;
        run(
            db,
            "MATCH (a:User), (b:User) WHERE b.uid = (a.uid + 1) % 200 CREATE (a)-[:F]->(b)",
        )
        .await;
    }
    create_index(
        &indexed,
        index::IndexSpec::node_unique_equality("User", "uid"),
    )
    .await;
    for query in [
        "MATCH (a:User) MATCH (a {uid: 5}) RETURN a.uid",
        "MATCH (a:User) MATCH (a)-[:F]->(b) WHERE a.uid = 5 RETURN b.uid",
        "MATCH (a:User) WITH a MATCH (a {uid: 7})-[:F]->(b {tier: 2}) RETURN b.uid",
        "MATCH (a:User) MATCH (a {uid: 7})-[:F]->(b {tier: 1}) RETURN b.uid",
        "MATCH (a:User) MATCH (a:User {uid: 9}), (b:User {uid: 3}) RETURN a.uid, b.uid",
    ] {
        let direct = run(&indexed, query).await;
        assert_eq!(direct.rows, run(&scanned, query).await.rows, "{query}");
        let reads = &direct.resources.reads;
        assert!(
            reads.point_gets + reads.multi_get_keys < 30,
            "{query}: {reads:?}"
        );
    }
    // A later WHERE that can fail keeps the scan and its evaluation order.
    let query =
        "MATCH (a:User) MATCH (a {uid: 5})-[:F]->(b) WHERE b.uid / (b.uid - 6) > 0 RETURN b";
    let direct = indexed.cypher(cypher::Request::new(query)).await;
    let reference = scanned.cypher(cypher::Request::new(query)).await;
    assert_eq!(
        direct
            .map(|response| response.rows)
            .map_err(|error| error.to_string()),
        reference
            .map(|response| response.rows)
            .map_err(|error| error.to_string()),
    );
    indexed.close().await.unwrap();
    scanned.close().await.unwrap();
}

/// A label from a later MATCH narrows an earlier source only when that
/// source's own pattern constraints cannot fail on the nodes it would skip.
#[tokio::test]
async fn later_match_labels_keep_failing_constraints_evaluated() {
    let db = database().await;
    run(&db, "CREATE (:Post {likes: 3})-[:TAGGED]->(:Tag {kind: 1})").await;
    for query in [
        "MATCH (a)-[:TAGGED]->(t:Tag {kind: 1 / (a.likes - a.likes)}) MATCH (a:User) RETURN count(*)",
        "MATCH (a)-[:TAGGED]->(t:Tag {kind: 1 / (a.likes - a.likes)}) WHERE a:User RETURN count(*)",
        "MATCH (a)-[:TAGGED]->(t:Tag {kind: 1 / (a.likes - a.likes)}) RETURN count(*)",
    ] {
        let error = db
            .cypher(cypher::Request::new(query))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("DivisionByZero"), "{query}: {error}");
    }
    db.close().await.unwrap();
}
