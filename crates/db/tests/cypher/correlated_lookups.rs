use super::membership::create_index;
use super::{database, run};
use helix_ast::index;
use serde_json::json;

/// A correlated lookup beside a bound node that the pattern names again
/// still validates that node, and returns what the scanning plan returns.
#[tokio::test]
async fn correlated_lookups_validate_bound_nodes_in_the_same_pattern() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 29) AS i CREATE (:User {uid: i, tier: i % 3})",
    )
    .await;
    create_index(&db, index::IndexSpec::node_unique_equality("User", "uid")).await;
    for (query, expected) in [
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r MATCH (a), (c:User {uid: r}) RETURN c.uid",
            json!([[3]]),
        ),
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r MATCH (c:User {uid: r}), (a:User) \
             RETURN c.uid",
            json!([[3]]),
        ),
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r MATCH (a {tier: 2}), (c:User {uid: r}) \
             RETURN c.uid",
            json!([]),
        ),
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r OPTIONAL MATCH (a {tier: 2}), \
             (c:User {uid: r}) RETURN c.uid",
            json!([[null]]),
        ),
        (
            "MATCH (a:User) WHERE a.uid < 3 WITH a, a.uid + 3 AS r \
             MATCH (a), (c:User) WHERE c.uid = r RETURN c.uid ORDER BY c.uid",
            json!([[3], [4], [5]]),
        ),
    ] {
        let rows = run(&db, query).await.rows;
        assert_eq!(json!(rows), expected, "{query}");
        // `r + 0` cannot use an index, so the reference scans the label.
        let scanned = run(
            &db,
            &query
                .replace("uid: r", "uid: r + 0")
                .replace("= r", "= r + 0"),
        )
        .await
        .rows;
        assert_eq!(rows, scanned, "{query}");
    }
    db.close().await.unwrap();
}
