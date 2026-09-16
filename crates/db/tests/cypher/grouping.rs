use super::{database, run};
use db::cypher;
use helix_planner::relational as r;
use serde_json::json;

#[tokio::test]
async fn ambiguous_grouping_is_rejected_before_any_graph_changes() {
    let db = database().await;
    run(&db, "CREATE (:N {key: 1})").await;
    for query in [
        "CREATE (n:N {key: 9}) RETURN CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "MATCH (n:N) SET n.key = 99 RETURN CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "MATCH (n:N) DELETE n RETURN CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "WITH {dim: {x: 1}} AS b CREATE (:N {key: 9}) RETURN b.dim.x AS x, b.dim.x + count(*) AS c",
    ] {
        let cypher::Error::Query(error) = db.cypher(cypher::Request::new(query)).await.unwrap_err()
        else {
            panic!("expected a compile error: {query}");
        };
        assert_eq!(error.phase, r::ErrorPhase::Compile, "{query}");
        assert_eq!(error.category, "SyntaxError", "{query}");
        assert_eq!(error.detail, "AmbiguousAggregationExpression", "{query}");
        assert_eq!(
            run(&db, "MATCH (n) RETURN labels(n)[0], n.key").await.rows,
            vec![vec![json!("N"), json!(1)]],
            "{query}"
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn grouped_labels_and_nested_access_produce_exact_rows() {
    let db = database().await;
    run(&db, "CREATE (:N {key: 1}), (:N {key: 1}), (:M {key: 2})").await;
    for (query, expected) in [
        ("MATCH (n) RETURN n:N AS label, count(*) AS c ORDER BY label", vec![vec![json!(false),json!(1)],vec![json!(true),json!(2)]]),
        ("MATCH (n) RETURN count(CASE WHEN n:N THEN 1 END) AS c", vec![vec![json!(2)]]),
        ("MATCH (n) WITH n, CASE WHEN n:N THEN count(*) ELSE 0 END AS c RETURN labels(n)[0] AS label, sum(c) AS c ORDER BY label", vec![vec![json!("M"),json!(0)],vec![json!("N"),json!(2)]]),
        ("UNWIND [{dim: {x: 1}}, {dim: {x: 1}}, {dim: {x: 2}}] AS b RETURN b.dim AS dim, b.dim.x + count(*) AS total ORDER BY dim.x", vec![vec![json!({"x":1}),json!(3)],vec![json!({"x":2}),json!(3)]]),
    ] {
        assert_eq!(run(&db,query).await.rows,expected,"{query}");
    }
    db.close().await.unwrap();
}
