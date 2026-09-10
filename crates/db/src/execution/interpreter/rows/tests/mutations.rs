use super::super::*;
use serde_json::json;

#[tokio::test]
async fn named_create_paths_relationship_updates_and_delete_paths_preserve_atomicity() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-path-crud-boundaries").await;
    let result = db
        .cypher(crate::cypher::Request::new(
            "CREATE p=(a:N {key:1})<-[r:R {old:1,keep:2}]-(b:N {key:2}) RETURN p",
        ))
        .await
        .unwrap();
    let path = &result.rows[0][0];
    assert_eq!(path["nodes"][0]["properties"]["key"], 1);
    assert_eq!(path["nodes"][1]["properties"]["key"], 2);
    assert_eq!(path["relationships"][0]["start"], path["nodes"][1]["id"]);
    assert_eq!(path["relationships"][0]["end"], path["nodes"][0]["id"]);
    let result=db.cypher(crate::cypher::Request::new(
        "MATCH ()-[r:R]->(a) SET r={x:3,keep:4} SET r += {keep:null,y:5} REMOVE r.x RETURN properties(r)",
    )).await.unwrap();
    assert_eq!(result.rows, vec![vec![json!({"y":5})]]);
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH (a:N {key:1}),(b:N {key:2}) SET a=b RETURN properties(a)",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!({"key":2})]]);
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r:R]->() SET r=null RETURN properties(r)",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!({})]]);
    db.cypher(crate::cypher::Request::new("MATCH p=()-[:R]->() DELETE p"))
        .await
        .unwrap();
    let result = db
        .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(*)"))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(0)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn dynamic_mutation_types_and_reserved_keys_roll_back_prior_updates() {
    let db = crate::execution::interpreter::test_support::open_db("cypher-dynamic-mutations").await;
    db.cypher(crate::cypher::Request::new("CREATE (:N {key:1})"))
        .await
        .unwrap();
    for (query, parameter, detail) in [
        (
            "MATCH (n:N) SET n.key=9 SET n=$value",
            json!(false),
            "ExpectedMap",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n += $value",
            json!({"$label":"Bad"}),
            "ReservedPropertyName",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n += $value",
            json!({"":7}),
            "EmptyPropertyName",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n.x=$value",
            json!([1, "two"]),
            "InvalidPropertyType",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n.x=$value",
            json!([null]),
            "InvalidPropertyType",
        ),
        (
            "MATCH (n:N) SET n.key=9 SET n.x=$value",
            json!({"nested":1}),
            "InvalidPropertyType",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x DELETE x",
            json!(1),
            "InvalidArgumentType",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x SET x.y=1",
            json!(1),
            "ExpectedEntity",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x CREATE (x)-[:R]->(:N)",
            json!(null),
            "ExpectedNode",
        ),
        (
            "MATCH (n:N) SET n.key=9 WITH $value AS x CREATE (:N)-[:R]->(x)",
            json!(null),
            "ExpectedNode",
        ),
    ] {
        let mut request = crate::cypher::Request::new(query);
        request
            .parameters
            .insert("value".into(), serde_json::from_value(parameter).unwrap());
        let error = db.cypher(request).await.unwrap_err();
        assert!(
            matches!(&error,Error::Query(error) if error.detail==detail),
            "{query}: {error:?}"
        );
        let response = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:N) RETURN properties(n)",
            ))
            .await
            .unwrap();
        assert_eq!(response.rows, vec![vec![json!({"key":1})]], "{query}");
    }
    let response = db
        .cypher(crate::cypher::Request::new(
            "OPTIONAL MATCH (n:Absent) SET n += {x:1} REMOVE n.x DELETE n RETURN n",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(null)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn late_bound_match_type_errors_roll_back_instead_of_silently_dropping_rows() {
    let db =
        crate::execution::interpreter::test_support::open_db("cypher-bound-pattern-types").await;
    for (pattern, detail) in [
        ("(x)", "ExpectedNode"),
        ("()-[x]->()", "ExpectedRelationship"),
    ] {
        let mut request = crate::cypher::Request::new(format!(
            "CREATE (:Marker) WITH $value AS x MATCH {pattern} RETURN x"
        ));
        request
            .parameters
            .insert("value".into(), helix_ast::query::QueryValue::I64(7));
        let error = db.cypher(request).await.unwrap_err();
        assert!(
            matches!(error,Error::Query(ref error) if error.detail==detail),
            "{error:?}"
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH (n:Marker) RETURN count(*)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(0)]]
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn deleted_relationship_types_survive_but_properties_fail_and_roll_back() {
    let db =
        crate::execution::interpreter::test_support::open_db("deleted-relationship-values").await;
    for deletion in [
        "DELETE r",
        "DELETE r DELETE r",
        "DETACH DELETE a",
        "DELETE p",
    ] {
        let result = db.cypher(crate::cypher::Request::new(format!(
            "CREATE p=(a:N)-[r:R {{key:1}}]->(b:N) {deletion} RETURN type(r),type(head(relationships(p)))",
        ))).await.unwrap();
        assert_eq!(
            result.rows,
            vec![vec![json!("R"), json!("R")]],
            "{deletion}"
        );
        db.cypher(crate::cypher::Request::new("MATCH (n) DETACH DELETE n"))
            .await
            .unwrap();
    }
    db.cypher(crate::cypher::Request::new(
        "CREATE (:N)-[:R {key:1}]->(:N)",
    ))
    .await
    .unwrap();
    for expression in ["r.key", "properties(r)", "keys(r)"] {
        let error = db
            .cypher(crate::cypher::Request::new(format!(
                "MATCH (a)-[r:R]->(b) DELETE r RETURN type(r),{expression}",
            )))
            .await
            .unwrap_err();
        assert!(
            matches!(error,Error::Query(error) if error.category=="EntityNotFound" && error.detail=="DeletedEntityAccess")
        );
        assert_eq!(
            db.cypher(crate::cypher::Request::new(
                "MATCH ()-[r:R]->() RETURN count(*)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(1)]]
        );
    }
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r:R]->() DELETE r RETURN CASE WHEN type(r)='R' THEN 1 ELSE r.key END",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}
