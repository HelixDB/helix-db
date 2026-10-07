use super::{database, run};
use db::cypher;
use serde_json::json;

#[tokio::test]
async fn numeric_averages_preserve_representable_results_and_transaction_rules() {
    let db = database().await;
    for (text, expected) in [
        (
            "UNWIND [9223372036854775807,9223372036854775807] AS x RETURN avg(x)",
            json!(i64::MAX as f64),
        ),
        (
            "UNWIND [-9223372036854775808,-9223372036854775808] AS x RETURN avg(x)",
            json!(i64::MIN as f64),
        ),
        (
            "UNWIND [9223372036854775807,-9223372036854775808] AS x RETURN avg(x)",
            json!(-0.5),
        ),
        ("UNWIND [1e308,1e308] AS x RETURN avg(x)", json!(1e308)),
        ("UNWIND [0.1,0.1,0.1] AS x RETURN avg(x)", json!(0.1)),
        (
            "UNWIND range(1,513) AS i RETURN avg(9007199254740991)",
            json!(9007199254740991_f64),
        ),
        (
            "UNWIND [9007199254740993,9007199254740993,9007199254740993] AS x RETURN avg(x)",
            json!(9007199254740992_f64),
        ),
        (
            "UNWIND [1e308,1e308,-1e308,-1e308] AS x RETURN avg(x)",
            json!(0.0),
        ),
        (
            "UNWIND [9223372036854775807,1.0,-9223372036854775807] AS x RETURN avg(x)",
            json!(1.0 / 3.0),
        ),
        (
            "UNWIND [1,1.0,null,3,3.0] AS x RETURN avg(DISTINCT x)",
            json!(2.0),
        ),
        ("UNWIND [] AS x RETURN avg(x)", json!(null)),
        ("UNWIND [null,null] AS x RETURN avg(x)", json!(null)),
    ] {
        assert_eq!(run(&db, text).await.rows, vec![vec![expected]], "{text}");
    }
    assert_eq!(run(&db,"UNWIND [1e308,1e308] AS x WITH avg(x) AS value CREATE (n:AverageValue {value:value}) RETURN n.value").await.rows,vec![vec![json!(1e308)]]);
    assert_eq!(
        run(&db, "MATCH (n:AverageValue) RETURN n.value").await.rows,
        vec![vec![json!(1e308)]]
    );
    let error = db
        .cypher(cypher::Request::new(
            "CREATE (:AverageRollback) WITH 1 AS unused UNWIND [1,'bad'] AS x RETURN avg(x)",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(error,cypher::Error::Query(error) if error.category=="TypeError" && error.detail=="InvalidArgumentType")
    );
    assert_eq!(
        run(&db, "MATCH (:AverageRollback) RETURN count(*)")
            .await
            .rows,
        vec![vec![json!(0)]]
    );
    let error = db
        .cypher(cypher::Request::new(
            "UNWIND [9223372036854775807,1] AS x RETURN sum(x)",
        ))
        .await
        .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(error) if error.detail=="NumberOutOfRange"));
    db.close().await.unwrap();
}
