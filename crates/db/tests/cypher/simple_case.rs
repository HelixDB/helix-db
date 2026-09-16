use super::{database, run};
use db::cypher;
use serde_json::json;

#[tokio::test]
async fn simple_case_results_and_aggregate_arguments_follow_independent_expectations() {
    let db = database().await;
    for (query, expected) in [
        (
            "RETURN CASE 1 WHEN 1 THEN 'first' WHEN 1 THEN 'second' END AS value",
            json!([["first"]]),
        ),
        (
            "RETURN CASE 3 WHEN 1 THEN 'one' END AS value",
            json!([[null]]),
        ),
        (
            "RETURN CASE null WHEN null THEN 'match' ELSE 'else' END AS value",
            json!([["else"]]),
        ),
        (
            "RETURN CASE 1 WHEN 1.0 THEN 'match' ELSE 'else' END AS value",
            json!([["match"]]),
        ),
        (
            "RETURN CASE '1' WHEN 1 THEN 'match' ELSE 'else' END AS value",
            json!([["else"]]),
        ),
        (
            "RETURN CASE [1,null] WHEN [1,null] THEN 'match' ELSE 'else' END AS value",
            json!([["else"]]),
        ),
        (
            "RETURN CASE [1,null] WHEN [2,null] THEN 'match' ELSE 'else' END AS value",
            json!([["else"]]),
        ),
        (
            "RETURN CASE {a:1,b:[2]} WHEN {b:[2],a:1} THEN 'match' ELSE 'else' END AS value",
            json!([["match"]]),
        ),
        (
            "RETURN CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 7 ELSE 1/0 END AS value",
            json!([[7]]),
        ),
        (
            "RETURN CASE 1 WHEN 1 THEN 7 WHEN 1/0 THEN 9 ELSE 1/0 END AS value",
            json!([[7]]),
        ),
        (
            "RETURN CASE CASE 1 WHEN 1 THEN 2 END WHEN 2 THEN 3 END AS value",
            json!([[3]]),
        ),
        (
            "UNWIND [1,2] AS x RETURN CASE sum(x) WHEN 3 THEN count(*) ELSE 0 END AS value",
            json!([[2]]),
        ),
        (
            "UNWIND [] AS x RETURN CASE sum(x) WHEN 0 THEN 1+count(*) ELSE 9 END AS value",
            json!([[1]]),
        ),
    ] {
        assert_eq!(
            serde_json::to_value(run(&db, query).await.rows).unwrap(),
            expected,
            "{query}"
        );
    }
    for query in [
        "CREATE (:CaseWrite) RETURN CASE 1/0 WHEN 1 THEN 7 ELSE 9 END AS value",
        "CREATE (:CaseWrite) RETURN CASE 2 WHEN 1 THEN 7 WHEN 1/0 THEN 9 ELSE 3 END AS value",
        "CREATE (:CaseWrite) WITH 0 AS x RETURN CASE 1 WHEN 1 THEN 7 ELSE sum(1/x) END AS value",
    ] {
        let cypher::Error::Query(error) = db.cypher(cypher::Request::new(query)).await.unwrap_err()
        else {
            panic!("query error")
        };
        assert_eq!(error.category, "ArithmeticError");
        assert_eq!(error.detail, "DivisionByZero");
        assert_eq!(error.phase, helix_planner::relational::ErrorPhase::Runtime);
        assert_eq!(
            run(&db, "MATCH (n:CaseWrite) RETURN count(*)").await.rows,
            vec![vec![json!(0)]]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn simple_case_keeps_graph_identity_and_property_demands_after_scope_changes() {
    let db = database().await;
    run(
        &db,
        "CREATE (a:CaseNode {key:'a',value:1}), (b:CaseNode {key:'b',value:2}), (a)-[:LINK]->(b)",
    )
    .await;
    for (query, expected) in [
        ("MATCH (n:CaseNode) RETURN n.key AS key, CASE n.value WHEN 1 THEN n.key WHEN 2 THEN 'second' ELSE '?' END AS value ORDER BY key",json!([["a","a"],["b","second"]])),
        ("MATCH (a:CaseNode {key:'a'}) OPTIONAL MATCH (a)-[:MISSING]->(b) RETURN CASE b WHEN null THEN 'unexpected' ELSE 'missing' END AS value",json!([["missing"]])),
        ("MATCH p=(a:CaseNode)-[r:LINK]->(b:CaseNode) RETURN CASE p WHEN p THEN length(p) ELSE 0 END AS size, CASE r WHEN r THEN type(r) END AS kind, CASE a WHEN b THEN false ELSE true END AS different",json!([[1,"LINK",true]])),
        ("MATCH (n:CaseNode) WITH n.key AS k,n AS renamed WITH CASE renamed.value WHEN 1 THEN k ELSE 'other' END AS chosen,renamed RETURN chosen,CASE renamed WHEN renamed THEN renamed.key END AS key ORDER BY key",json!([["a","a"],["other","b"]])),
    ] {
        assert_eq!(serde_json::to_value(run(&db,query).await.rows).unwrap(),expected,"{query}");
    }
    db.close().await.unwrap();
}
