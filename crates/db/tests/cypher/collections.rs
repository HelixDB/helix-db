use super::{database, run};
use db::{
    cypher, encoding::v2::keys::scope::DataScope, execution_control::ExecutionControl,
    query_service::QueryMode,
};
use serde_json::json;

#[tokio::test]
async fn collection_limits_apply_to_materialized_lists_and_roll_back_writes() {
    let db = database().await;
    let execute = |query: &str, collection_items| {
        cypher::execute(
            &db,
            cypher::Request::new(query),
            DataScope::LegacyUnscoped,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits {
                collection_items,
                ..Default::default()
            },
        )
    };
    assert_eq!(
        execute("RETURN substring('a猫z',1,1),range(7,7),range(3,1)", 1)
            .await
            .unwrap()
            .rows,
        vec![vec![json!("猫"), json!([7]), json!([])]]
    );
    assert_eq!(
        execute("UNWIND range(1,1000) AS n RETURN count(*),sum(n)", 1)
            .await
            .unwrap()
            .rows,
        vec![vec![json!(1000), json!(500500)]]
    );
    for query in [
        "CREATE (:LimitedList) RETURN [1]+[2]",
        "CREATE (:LimitedList) RETURN [1]+2",
        "CREATE (:LimitedList) RETURN 1+[2]",
        "CREATE (:LimitedList) RETURN keys({a:1,b:2})",
        "CREATE (:LimitedList) WITH 1 AS unused UNWIND range(1,2) AS n RETURN collect(n)",
        "CREATE (:LimitedList) WITH 1 AS unused UNWIND range(1,2) AS n RETURN count(DISTINCT n)",
    ] {
        let error = execute(query, 1).await.unwrap_err();
        assert!(matches!(error, cypher::Error::Query(error)
            if error.category=="ResourceLimit" && error.detail=="CollectionLimit"
                && error.phase==helix_planner::relational::ErrorPhase::Runtime));
        assert_eq!(
            run(&db, "MATCH (n:LimitedList) RETURN count(n)").await.rows,
            vec![vec![json!(0)]]
        );
    }
    assert_eq!(
        execute("CREATE (:LimitedList) RETURN [1]+[2]", 2)
            .await
            .unwrap()
            .rows,
        vec![vec![json!([1, 2])]]
    );
    assert_eq!(
        run(&db, "MATCH (n:LimitedList) RETURN count(n)").await.rows,
        vec![vec![json!(1)]]
    );
    // Stored arrays remain unchanged; resource rejection happens when the
    // expression tries to materialize a list under the smaller query limit.
    run(&db, "CREATE (:StoredList {values:[1,2]})").await;
    let error = execute("MATCH (n:StoredList) RETURN n.values", 1)
        .await
        .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(error) if error.detail=="CollectionLimit"));
    assert_eq!(
        execute("MATCH (n:StoredList) RETURN n.values", 2)
            .await
            .unwrap()
            .rows,
        vec![vec![json!([1, 2])]]
    );
    assert_eq!(
        execute("MATCH (n:StoredList) RETURN keys(n)", 1)
            .await
            .unwrap()
            .rows,
        vec![vec![json!(["values"])]]
    );
    db.close().await.unwrap();
}
