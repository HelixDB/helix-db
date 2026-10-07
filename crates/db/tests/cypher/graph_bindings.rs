use super::{database, run};
use db::cypher;
use serde_json::json;

#[tokio::test]
async fn computed_node_and_relationship_aliases_remain_usable_in_patterns_and_updates() {
    let db = database().await;
    run(
        &db,
        "CREATE (a:AliasNode {key:'a'}),(b:AliasNode {key:'b'}),(a)-[:LINK {value:1}]->(b)",
    )
    .await;
    for (query,expected) in [
        ("MATCH (a:AliasNode) WITH CASE WHEN true THEN a END AS selected MATCH (selected) RETURN selected.key AS key ORDER BY key",json!([["a"],["b"]])),
        ("MATCH (a:AliasNode {key:'a'}) WITH CASE a WHEN a THEN a END AS selected SET selected.value=7 RETURN selected:AliasNode AS labeled,selected.value AS value",json!([[true,7]])),
        ("MATCH (a:AliasNode {key:'a'}) WITH CASE WHEN false THEN a END AS selected SET selected.value=99 RETURN selected:AliasNode AS labeled",json!([[null]])),
        ("MATCH (a:AliasNode {key:'a'}) WITH CASE WHEN false THEN a END AS selected MATCH (selected) RETURN count(*) AS count",json!([[0]])),
        ("MATCH ()-[r:LINK]->() WITH CASE WHEN true THEN r END AS selected MATCH ()-[selected]->() RETURN selected.value AS value",json!([[1]])),
        ("MATCH ()-[r:LINK]->() WITH CASE r WHEN r THEN r END AS selected SET selected.value=8 RETURN selected.value AS value",json!([[8]])),
        ("MATCH p=(:AliasNode)-[:LINK]->(:AliasNode) WITH CASE WHEN true THEN p END AS selected RETURN length(selected) AS size",json!([[1]])),
    ] {
        assert_eq!(serde_json::to_value(run(&db,query).await.rows).unwrap(),expected,"{query}");
    }
    assert_eq!(
        run(&db, "MATCH (a:AliasNode {key:'a'}) RETURN a.value")
            .await
            .rows,
        vec![vec![json!(7)]]
    );
    let failure="MATCH (a:AliasNode {key:'a'}) WITH CASE WHEN true THEN a END AS selected SET selected.value=99 RETURN 1/0";
    let cypher::Error::Query(error) = db.cypher(cypher::Request::new(failure)).await.unwrap_err()
    else {
        panic!("query error")
    };
    assert_eq!(error.category, "ArithmeticError");
    assert_eq!(error.detail, "DivisionByZero");
    assert_eq!(
        run(&db, "MATCH (a:AliasNode {key:'a'}) RETURN a.value")
            .await
            .rows,
        vec![vec![json!(7)]]
    );
    run(&db,"MATCH p=(:AliasNode)-[:LINK]->(:AliasNode) WITH CASE p WHEN p THEN p END AS selected DELETE selected").await;
    assert_eq!(
        run(&db, "MATCH (a:AliasNode) RETURN count(*)").await.rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        run(&db, "MATCH ()-[r:LINK]->() RETURN count(*)").await.rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}
