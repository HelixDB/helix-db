use super::membership::create_index;
use super::{database, run};
use helix_ast::index;
use serde_json::json;

/// A step with several relationship types reads each type's adjacency rather
/// than every relationship of the node, and still returns each matching
/// relationship once.
#[tokio::test]
async fn type_alternatives_read_only_their_adjacency() {
    let db = database().await;
    run(
        &db,
        "CREATE (a:User {uid: 1}) WITH a UNWIND range(1, 300) AS i CREATE (a)-[:FOLLOWS]->(:User {uid: i + 1})",
    )
    .await;
    run(
        &db,
        "MATCH (a:User {uid: 1}) CREATE (a)-[:WROTE]->(:Post), (a)-[:WROTE]->(:Post), \
         (a)-[:LIVES_IN]->(c:City), (c)-[:LIVES_IN]->(a), (a)-[:WROTE]->(a)",
    )
    .await;
    create_index(&db, index::IndexSpec::node_unique_equality("User", "uid")).await;
    for (query, expected) in [
        (
            "MATCH (a:User {uid: 1})-[:WROTE|LIVES_IN]->(x) RETURN count(*)",
            4,
        ),
        (
            "MATCH (a:User {uid: 1})-[:LIVES_IN|WROTE|WROTE]->(x) RETURN count(*)",
            4,
        ),
        (
            "MATCH (a:User {uid: 1})<-[:WROTE|LIVES_IN]-(x) RETURN count(*)",
            2,
        ),
        // The self-loop matches once in an undirected step.
        (
            "MATCH (a:User {uid: 1})-[:WROTE|LIVES_IN]-(x) RETURN count(*)",
            5,
        ),
        (
            "MATCH (a:User {uid: 1})-[r:WROTE|MISSING]->(x) RETURN count(r)",
            3,
        ),
        (
            "MATCH (a:User {uid: 1})-[:MISSING|NONE]->(x) RETURN count(*)",
            0,
        ),
    ] {
        let response = run(&db, query).await;
        assert_eq!(response.rows, vec![vec![json!(expected)]], "{query}");
        let reads = &response.resources.reads;
        assert!(
            reads.scan_rows + reads.multi_get_keys + reads.point_gets < 40,
            "{query}: {reads:?}"
        );
    }
    let types = run(
        &db,
        "MATCH (a:User {uid: 1})-[r:WROTE|LIVES_IN]->(x) RETURN type(r) AS t, count(*) AS c ORDER BY t",
    )
    .await;
    assert_eq!(
        types.rows,
        vec![
            vec![json!("LIVES_IN"), json!(1)],
            vec![json!("WROTE"), json!(3)]
        ]
    );
    db.close().await.unwrap();
}
