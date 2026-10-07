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

#[tokio::test]
async fn computed_group_keys_preserve_first_representation_and_simultaneous_aliases() {
    let db = database().await;
    for (query, expected) in [
        (
            "UNWIND [[1,2],[1.0,2.0],[null,null]] AS pair WITH pair[0] AS a,pair[1] AS b RETURN b AS a,a AS b,count(*) AS n ORDER BY a",
            vec![vec![json!(2),json!(1),json!(2)],vec![json!(null),json!(null),json!(1)]],
        ),
        (
            "UNWIND [{x:[1,null]},{x:[1.0,null]},{x:[]}] AS value RETURN value AS key,count(*) AS n ORDER BY n",
            vec![vec![json!({"x":[]}),json!(1)],vec![json!({"x":[1,null]}),json!(2)]],
        ),
    ] {
        assert_eq!(run(&db, query).await.rows, expected, "{query}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn computed_group_projection_needs_no_per_group_property_reads() {
    for size in [64_usize, 1024] {
        let db = database().await;
        run(
            &db,
            &format!(
                "UNWIND range(0,{}) AS i CREATE (:Grouped {{key:i}})",
                size - 1
            ),
        )
        .await;
        for groups in [16_usize, 64] {
            let response = run(&db, &format!("MATCH (n:Grouped) RETURN n.key % {groups} AS bucket,avg(1e308) AS mean ORDER BY bucket")).await;
            let expected = (0..groups)
                .map(|key| vec![json!(key), json!(1e308)])
                .collect::<Vec<_>>();
            assert_eq!(response.rows, expected);
            let reads = response.resources.reads;
            assert!(
                reads.multi_get_batches <= 3 * size.div_ceil(512),
                "{size} rows, {groups} groups: {reads:?}"
            );
            assert!(reads.multi_get_keys <= 3 * size, "{reads:?}");
            assert!(response.resources.peak_memory_bytes <= 2 * 1024 * 1024);
        }
        db.close().await.unwrap();
    }
}
