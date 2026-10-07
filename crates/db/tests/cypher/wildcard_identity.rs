use super::{database, run};
use db::cypher;
use serde_json::json;

#[tokio::test]
async fn identity_wildcards_preserve_rows_optional_nulls_and_match_uniqueness() {
    let db = database().await;
    run(
        &db,
        "CREATE (a:N {key:1}),(b:N {key:2}),(a)-[:R]->(b),(a)-[:R]->(b)",
    )
    .await;
    for (query, expected) in [
        (
            "UNWIND [2,2,1,null] AS x WITH * WITH * RETURN x ORDER BY x",
            json!([[1], [2], [2], [null]]),
        ),
        ("OPTIONAL MATCH (n:Absent) WITH * RETURN n", json!([[null]])),
        (
            "MATCH (a:N)-[r:R]->(b:N) WITH * MATCH (a)-[s:R]->(b) RETURN count(*)",
            json!([[4]]),
        ),
        (
            "MATCH (a:N)-[r:R]->(b:N) WITH * WITH * RETURN a.key,b.key ORDER BY a.key,b.key",
            json!([[1, 2], [1, 2]]),
        ),
        (
            "UNWIND [2,1,2] AS x WITH DISTINCT * WITH * RETURN x ORDER BY x",
            json!([[1], [2]]),
        ),
        (
            "UNWIND [3,1,2] AS x WITH * ORDER BY x LIMIT 2 WITH * RETURN collect(x)",
            json!([[[1, 2]]]),
        ),
    ] {
        assert_eq!(
            serde_json::to_value(run(&db, query).await.rows).unwrap(),
            expected,
            "{query}"
        );
    }
    let repeated = format!(
        "MATCH (n:N) {}RETURN count(*)",
        "MATCH (n)-->() WITH * WITH * ".repeat(4)
    );
    assert_eq!(run(&db, &repeated).await.rows, vec![vec![json!(16)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn identity_wildcards_preserve_write_barriers_and_late_error_rollback() {
    let db = database().await;
    run(&db, "CREATE (:N {key:1}),(:N {key:2})").await;
    assert_eq!(
        run(&db, "MATCH (n:N) WITH * WITH * CREATE (:N) RETURN count(*)")
            .await
            .rows,
        vec![vec![json!(2)]]
    );
    assert_eq!(
        run(&db, "MATCH (n:N) RETURN count(*)").await.rows,
        vec![vec![json!(4)]]
    );
    for query in [
        "UNWIND [1,0] AS x WITH 1/x AS y WITH * RETURN y LIMIT 1",
        "UNWIND [1,0] AS x WITH 1/x AS y WITH * RETURN y LIMIT 0",
        "MATCH (n:N) WITH * SET n.changed=1 WITH * RETURN 1/0",
        "CREATE (:Rollback) WITH 0 AS x WITH * RETURN 1/x",
    ] {
        let cypher::Error::Query(error) = db.cypher(cypher::Request::new(query)).await.unwrap_err()
        else {
            panic!("query error")
        };
        assert_eq!(error.detail, "DivisionByZero", "{query}");
    }
    assert_eq!(
        run(&db, "MATCH (n:N) RETURN count(n.changed)").await.rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        run(&db, "MATCH (n:Rollback) RETURN count(*)").await.rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}
