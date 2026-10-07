//! Runtime contracts for shared mixed aggregation and ordering.
use super::{database, run};
use db::cypher;
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::test]
async fn ordered_mixed_aggregates_match_an_independent_integer_model() {
    let db = database().await;
    for size in [0, 1, 7, 64, 1024] {
        let mut groups = BTreeMap::<i64, i64>::new();
        let rows: Vec<_> = (0..size)
            .map(|index| {
                let group = index % 7;
                let amount = (index * 37) % 101 - 50;
                *groups.entry(group).or_default() += amount;
                json!({"g":group,"x":amount})
            })
            .collect();
        for (query, offset) in [
            ("UNWIND $rows AS row WITH row.g AS g, row.x AS x RETURN g, sum(x) AS x ORDER BY x + sum(x) DESC, g", 0),
            ("UNWIND $rows AS row WITH row.g AS g, row.x AS x RETURN g, 1 + sum(x) AS x ORDER BY 2 - sum(x), g", 1),
        ] {
            let mut expected: Vec<_> = groups.iter().map(|(&group, &sum)| (group, sum)).collect();
            expected.sort_by(|a,b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let expected: Vec<_> = expected.into_iter().map(|(group,sum)|vec![json!(group),json!(sum+offset)]).collect();
            let request: cypher::Request = serde_json::from_value(json!({"query":query,"parameters":{"rows":rows}})).unwrap();
            let response = db.cypher(request).await.unwrap();
            assert_eq!(response.columns, ["g","x"]);
            assert_eq!(response.rows,expected,"size={size}: {query}");
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_aggregation_preserves_empty_null_and_distinct_results() {
    let db = database().await;
    for (query, expected) in [
        ("UNWIND [] AS x RETURN 1 + count(*) AS c, [sum(x), avg(x), min(x), max(x), collect(x)] AS values", vec![vec![json!(1),json!([0,null,null,null,[]])]]),
        ("UNWIND [] AS x RETURN x, 1 + count(*) AS c", vec![]),
        ("UNWIND [null,1,1,2] AS x WITH x ORDER BY x RETURN [count(*), count(x), count(DISTINCT x), sum(DISTINCT x), avg(x), collect(DISTINCT x)] AS values", vec![vec![json!([4,3,2,3,4.0/3.0,[1,2]])]]),
        ("RETURN 1 + count(*) AS x ORDER BY 2 - count(*)", vec![vec![json!(2)]]),
        ("UNWIND [1,2,2,3,3,3] AS x WITH x AS g, 1 + count(*) AS c WHERE c > 2 RETURN g, c ORDER BY c DESC SKIP 1 LIMIT 1", vec![vec![json!(2),json!(3)]]),
        ("UNWIND [1,2] AS x RETURN DISTINCT count(*) - count(*) AS c ORDER BY count(*)", vec![vec![json!(0)]]),
    ] {
        assert_eq!(run(&db,query).await.rows,expected,"{query}");
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn aggregation_errors_roll_back_before_post_projection_case_and_limits() {
    let db = database().await;
    for query in [
        "CREATE (:N) WITH 0 AS x RETURN CASE WHEN false THEN sum(1 / x) ELSE 0 END AS n",
        "CREATE (:N) WITH 0 AS x RETURN 1 + sum(1 / x) AS n LIMIT 0",
        "CREATE (:N) WITH 0 AS x RETURN coalesce(0, sum(1 / x)) AS n",
    ] {
        let cypher::Error::Query(error) = db.cypher(cypher::Request::new(query)).await.unwrap_err()
        else {
            panic!("expected query error")
        };
        assert_eq!(
            error.phase,
            helix_planner::relational::ErrorPhase::Runtime,
            "{query}"
        );
        assert_eq!(error.category, "ArithmeticError", "{query}");
        assert_eq!(error.detail, "DivisionByZero", "{query}");
        assert_eq!(
            run(&db, "MATCH (n) RETURN count(*)").await.rows,
            vec![vec![json!(0)]],
            "{query}"
        );
    }
    assert_eq!(
        run(&db, "RETURN CASE WHEN false THEN 1 / 0 ELSE 2 END")
            .await
            .rows,
        vec![vec![json!(2)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn mixed_aggregates_stream_large_inputs_and_charge_collection_state() {
    let db = database().await;
    let execute = |query: &str| {
        cypher::execute(
            &db,
            cypher::Request::new(query),
            db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            db::query_service::QueryMode::Execute,
            db::execution_control::ExecutionControl::unlimited(),
            cypher::Limits {
                memory_bytes: 32 * 1024,
                batch_rows: 16,
                collection_items: 3,
                ..Default::default()
            },
        )
    };
    for (size, expected) in [(10, 56_i64), (100_000, 5_000_050_001_i64)] {
        let response = execute(&format!(
            "UNWIND range(1,{size}) AS x RETURN 1 + sum(x) AS total"
        ))
        .await
        .unwrap();
        assert_eq!(response.rows, vec![vec![json!(expected)]]);
        assert!(response.resources.peak_memory_bytes <= 32 * 1024);
    }
    let cypher::Error::Query(error) =
        execute("UNWIND range(1,30) AS x RETURN size(collect(x)) AS n")
            .await
            .unwrap_err()
    else {
        panic!("expected collection limit")
    };
    assert_eq!(error.category, "ResourceLimit");
    assert_eq!(error.detail, "CollectionLimit");
    db.close().await.unwrap();
}

#[tokio::test]
async fn post_aggregation_hydrates_graph_values_and_keeps_optional_nulls() {
    let db = database().await;
    run(&db, "CREATE (:N {key:1}), (:M {key:2})").await;
    for (query,expected) in [
      ("MATCH (n) WITH n ORDER BY n.key RETURN properties(head(collect(n))) AS props",vec![vec![json!({"key":1})]]),
      ("MATCH (n) WITH DISTINCT n AS node ORDER BY n:N RETURN collect(node.key)",vec![vec![json!([2,1])]]),
      ("MATCH (n) WITH DISTINCT n AS node ORDER BY n:N DESC RETURN collect(node.key)",vec![vec![json!([1,2])]]),
      ("MATCH (n) WITH n, CASE WHEN n:N THEN count(*) ELSE 0 END AS c RETURN n.key AS key,c ORDER BY key",vec![vec![json!(1),json!(1)],vec![json!(2),json!(0)]]),
      ("OPTIONAL MATCH (n:Absent) RETURN [count(*),count(n),collect(n)] AS counts",vec![vec![json!([1,0,[]])]]),
    ] {
      assert_eq!(run(&db,query).await.rows,expected,"{query}");
    }
    db.close().await.unwrap();
}
