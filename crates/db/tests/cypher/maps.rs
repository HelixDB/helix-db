//! Production Cypher writes are visible through every native index route.
use super::{database, run};
use db::cypher;
use helix_ast::{batch, expr, index, query, traversal};
use serde_json::json;

#[test]
fn map_updates_keep_multiple_node_and_edge_indexes_atomic() {
    // Match the existing native index lifecycle contract stack allowance.
    std::thread::Builder::new()
        .name("cypher-map-index-contract".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all().build().unwrap().block_on(async {
                    let db = database().await;
                    run(&db, "CREATE (:MapNode {a:1,b:2})-[:MAP_EDGE {a:1,b:2}]->(:Other), (:MapNode {a:99,b:99})").await;
                    for spec in [
                        index::IndexSpec::node_unique_equality("MapNode", "a"),
                        index::IndexSpec::node_equality("MapNode", "b"),
                        index::IndexSpec::edge_equality("MAP_EDGE", "a"),
                        index::IndexSpec::edge_equality("MAP_EDGE", "b"),
                    ] {
                        let receipt = db.query(query::QueryRequest::write(
                            batch::write_batch().var_as("operation", traversal::g().create_index_if_not_exists(spec))
                                .returning(["operation"]),
                        )).await.unwrap();
                        let operation = receipt["operation"]["operation_id"].as_str().unwrap();
                        tokio::time::timeout(std::time::Duration::from_secs(30), async {
                            loop {
                                let status = db.query(query::QueryRequest::read(
                                    batch::read_batch().var_as("status", traversal::g().get_index_operation(operation))
                                        .returning(["status"]),
                                )).await.unwrap();
                                match status["status"]["status"].as_str() {
                                    Some("succeeded") => break,
                                    Some("queued" | "running") => tokio::task::yield_now().await,
                                    state => panic!("unexpected index state: {state:?}"),
                                }
                            }
                        }).await.unwrap();
                    }
                    assert_eq!(run(&db, "MATCH (n:MapNode {a:1})-[r:MAP_EDGE]->() SET n+={a:3,b:4},r={a:3,b:4} WITH n MATCH (m:MapNode {b:4}) RETURN m.a").await.rows, vec![vec![json!(3)]]);
                    for query in [
                        "MATCH (n:MapNode {a:3})-[r:MAP_EDGE]->() SET n={a:5,b:6},r={a:5,b:6} RETURN 1/0",
                        "MATCH (n:MapNode {a:3})-[r:MAP_EDGE]->() SET n={a:99,b:6},r={a:99,b:6}",
                    ] {
                        assert!(db.cypher(cypher::Request::new(query)).await.is_err(), "{query}");
                    }
                    for (property, current, old) in [("a", 3, 1), ("b", 4, 2)] {
                        for (value, count) in [(current, 1), (old, 0), (5, 0), (6, 0)] {
                            let nodes = db.query(query::QueryRequest::read(
                                batch::read_batch().var_as("rows", traversal::g()
                                    .n_with_label_where("MapNode", expr::Predicate::eq(property, value))
                                    .value_map(Some(vec![property])))
                                    .returning(["rows"]),
                            )).await.unwrap();
                            let edges = db.query(query::QueryRequest::read(
                                batch::read_batch().var_as("rows", traversal::g()
                                    .e_with_label_where("MAP_EDGE", expr::Predicate::eq(property, value))
                                    .value_map(Some(vec![property])))
                                    .returning(["rows"]),
                            )).await.unwrap();
                            assert_eq!(nodes["rows"].as_array().unwrap().len(), count);
                            assert_eq!(edges["rows"].as_array().unwrap().len(), count);
                        }
                    }
                    run(&db, "MATCH (n:MapNode {a:3})-[r:MAP_EDGE]->() SET n={a:7},r={a:7}").await;
                    for query in [
                        "MATCH (n:MapNode {b:4}) RETURN count(n)",
                        "MATCH ()-[r:MAP_EDGE {b:4}]->() RETURN count(r)",
                    ] {
                        assert_eq!(run(&db, query).await.rows, vec![vec![json!(0)]]);
                    }
                    db.flush_writer().await.unwrap();
                    db.close().await.unwrap();
                });
        }).unwrap().join().unwrap();
}
