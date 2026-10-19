use super::{database, run};
use db::cypher;
use helix_planner::relational as r;
use serde_json::json;

/// WHERE filters the rows a WITH returns, so it applies after the clause's
/// DISTINCT, ORDER BY, SKIP and LIMIT, on every row-execution path.
#[tokio::test]
async fn with_where_filters_after_the_window() {
    let db = database().await;
    run(&db, "UNWIND range(0, 9) AS i CREATE (:W {k: i})").await;
    for (query, expected) in [
        // Materialized top-k and skip.
        (
            "UNWIND range(1, 10) AS x WITH x ORDER BY x LIMIT 3 WHERE x > 2 RETURN x",
            vec![vec![json!(3)]],
        ),
        (
            "UNWIND range(1, 10) AS x WITH x SKIP 8 WHERE x < 10 RETURN x",
            vec![vec![json!(9)]],
        ),
        // Distinct and aggregation windows.
        (
            "UNWIND [1, 1, 2, 3, 4] AS x WITH DISTINCT x ORDER BY x LIMIT 2 WHERE x > 1 RETURN x",
            vec![vec![json!(2)]],
        ),
        (
            "UNWIND [1, 1, 2, 3, 3, 3] AS x WITH x, count(*) AS c ORDER BY c DESC LIMIT 2 \
             WHERE c < 3 RETURN x, c",
            vec![vec![json!(1), json!(2)]],
        ),
        // Graph sources feeding batch consumers.
        (
            "MATCH (n:W) WITH n ORDER BY n.k DESC LIMIT 2 WHERE n.k < 9 RETURN n.k",
            vec![vec![json!(8)]],
        ),
        (
            "MATCH (n:W) WITH n.k AS k ORDER BY k SKIP 1 LIMIT 2 WHERE k <> 1 RETURN k",
            vec![vec![json!(2)]],
        ),
        (
            "MATCH (n:W) WITH n LIMIT 4 WHERE n.k >= 0 RETURN count(*) AS c",
            vec![vec![json!(4)]],
        ),
        // Without a window, filtering first returns the same rows.
        (
            "MATCH (n:W) WITH n ORDER BY n.k DESC WHERE n.k < 2 RETURN n.k",
            vec![vec![json!(1)], vec![json!(0)]],
        ),
    ] {
        assert_eq!(run(&db, query).await.rows, expected, "{query}");
    }
    db.close().await.unwrap();
}

/// After SKIP or LIMIT only the WITH's own bindings remain, so a WHERE that
/// names an unprojected binding is rejected instead of filtering before the
/// window, which would change which rows the window keeps.
#[tokio::test]
async fn with_where_after_a_window_uses_only_projected_bindings() {
    let db = database().await;
    run(&db, "CREATE (:H {k: 1}), (:H {k: 2})").await;
    for query in [
        "UNWIND range(1, 10) AS x WITH x AS y ORDER BY y LIMIT 3 WHERE x > 5 RETURN y",
        "MATCH (n:H) WITH n.k AS k ORDER BY k LIMIT 1 WHERE n.k > 1 RETURN k",
        "UNWIND range(1, 10) AS x WITH x AS y SKIP 2 WHERE x > 5 RETURN y",
    ] {
        let cypher::Error::Query(error) = db.cypher(cypher::Request::new(query)).await.unwrap_err()
        else {
            panic!("expected a compile error: {query}");
        };
        assert_eq!(error.phase, r::ErrorPhase::Compile, "{query}");
        assert_eq!(error.detail, "undefined_variable", "{query}");
    }
    // The projected form and an unwindowed WITH keep working.
    assert_eq!(
        run(
            &db,
            "UNWIND range(1, 10) AS x WITH x AS y ORDER BY y LIMIT 3 WHERE y > 5 RETURN y"
        )
        .await
        .rows,
        Vec::<Vec<serde_json::Value>>::new()
    );
    assert_eq!(
        run(&db, "MATCH (n:H) WITH n.k AS k WHERE n.k > 1 RETURN k")
            .await
            .rows,
        vec![vec![json!(2)]]
    );
    db.close().await.unwrap();
}
