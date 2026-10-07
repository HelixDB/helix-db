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

/// Several listed types between one pair of nodes, into an already bound
/// node or around a self-loop still return each relationship once, as the
/// same step written with a type test does.
#[tokio::test]
async fn type_alternatives_return_each_relationship_once() {
    let db = database().await;
    run(
        &db,
        "CREATE (a:U {id: 1})-[:LIKES]->(p:P {id: 10}), (a)-[:VIEWED]->(p), (a)-[:LIKES]->(p), \
         (p)-[:VIEWED]->(a), (a)-[:LIKES]->(a), (a)-[:VIEWED]->(a), (a)-[:OTHER]->(p)",
    )
    .await;
    for (typed, tested) in [
        (
            "MATCH (a:U {id: 1})-[r:LIKES|VIEWED]->(x) RETURN type(r) AS t, count(*) ORDER BY t",
            "MATCH (a:U {id: 1})-[r]->(x) WHERE type(r) IN ['LIKES', 'VIEWED'] \
             RETURN type(r) AS t, count(*) ORDER BY t",
        ),
        (
            "MATCH (p:P {id: 10})<-[r:LIKES|VIEWED]-(x) RETURN count(*)",
            "MATCH (p:P {id: 10})<-[r]-(x) WHERE type(r) IN ['LIKES', 'VIEWED'] RETURN count(*)",
        ),
        (
            "MATCH (a:U {id: 1})-[r:LIKES|VIEWED]-(x) RETURN type(r) AS t, count(*) ORDER BY t",
            "MATCH (a:U {id: 1})-[r]-(x) WHERE type(r) IN ['LIKES', 'VIEWED'] \
             RETURN type(r) AS t, count(*) ORDER BY t",
        ),
        (
            "MATCH (a:U {id: 1}), (p:P {id: 10}) WITH a, p \
             MATCH (a)-[r:LIKES|VIEWED|MISSING]->(p) RETURN count(*)",
            "MATCH (a:U {id: 1}), (p:P {id: 10}) WITH a, p \
             MATCH (a)-[r]->(p) WHERE type(r) IN ['LIKES', 'VIEWED', 'MISSING'] RETURN count(*)",
        ),
        (
            "MATCH (a:U {id: 1})-[r:LIKES|VIEWED]->(a) RETURN count(*)",
            "MATCH (a:U {id: 1})-[r]->(a) WHERE type(r) IN ['LIKES', 'VIEWED'] RETURN count(*)",
        ),
        (
            "MATCH (a:U {id: 1})-[:LIKES|VIEWED]->(p:P)-[:VIEWED|LIKES]->(a) RETURN count(*)",
            "MATCH (a:U {id: 1})-[r1]->(p:P)-[r2]->(a) WHERE type(r1) IN ['LIKES', 'VIEWED'] \
             AND type(r2) IN ['LIKES', 'VIEWED'] RETURN count(*)",
        ),
    ] {
        assert_eq!(
            run(&db, typed).await.rows,
            run(&db, tested).await.rows,
            "{typed}"
        );
    }
    db.close().await.unwrap();
}
