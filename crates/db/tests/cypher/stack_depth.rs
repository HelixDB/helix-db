//! Deeply nested native plans run on the default test thread stack: every
//! level of executor recursion polls in its own small boxed future, so a
//! level costs a few kilobytes rather than every operator's temporaries.
use super::{database, run};
use helix_ast::{batch, query, traversal};

async fn users() -> db::HelixDB {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 31) AS i CREATE (:U {uid: i}) WITH count(*) AS n \
         MATCH (a:U), (b:U) WHERE b.uid = (a.uid + 1) % 32 CREATE (a)-[:F]->(b)",
    )
    .await;
    db
}

#[tokio::test]
async fn nested_for_each_bodies_run_on_the_default_stack() {
    let db = users().await;
    let depth = 40;
    let body = (0..depth).fold(
        batch::read_batch().var_as("c", traversal::g().n_with_label("U").count()),
        |inner, _| batch::read_batch().for_each_param("items", inner),
    );
    // Each iteration binds its object's fields, so every level carries the
    // next level's items.
    let items = (0..depth).fold(
        query::QueryValue::Array(vec![query::QueryValue::Object(Default::default())]),
        |inner, _| {
            query::QueryValue::Array(vec![query::QueryValue::Object(
                [("items".to_owned(), inner)].into(),
            )])
        },
    );
    let response = db
        .query(
            query::QueryRequest::read(body.returning(["c"])).with_parameter_value("items", items),
        )
        .await
        .unwrap();
    assert_eq!(response["c"], 32);
    db.close().await.unwrap();
}

#[tokio::test]
async fn long_expansion_chains_run_on_the_default_stack() {
    let db = users().await;
    let chain = (0..24).fold(traversal::g().n_with_label("U"), |t, _| t.out(Some("F")));
    let response = db
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as("c", chain.count())
                .returning(["c"]),
        ))
        .await
        .unwrap();
    assert_eq!(response["c"], 32);
    db.close().await.unwrap();
}

/// A request built in memory has no parser bounding its nesting, so the
/// query entry rejects one past the limit before telemetry, planning or
/// execution walk it.
#[tokio::test]
async fn requests_nested_past_the_limit_fail_cleanly() {
    let db = users().await;
    let chain = (0..300).fold(traversal::g().n_with_label("U"), |t, _| t.dedup());
    let error = db
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as("c", chain.count())
                .returning(["c"]),
        ))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("nests deeper than 255 levels"),
        "{error}"
    );
    db.close().await.unwrap();
}
