use super::{database, run};
use db::{cypher, HelixDB, HelixDbSource};
use helix_ast::{batch, query, traversal};
use helix_planner::relational as r;
use serde_json::json;

async fn steps(db: &HelixDB, query: &str) -> Vec<r::MatchStep> {
    db.explain_cypher(cypher::Request::new(query))
        .await
        .unwrap()
        .plan()
        .matches()
        .values()
        .flat_map(|plan| plan.steps.clone())
        .collect()
}

async fn count(db: &HelixDB, query: &str) -> serde_json::Value {
    run(db, query).await.rows[0][0].clone()
}

/// Only a typed relationship whose connected nodes are all unbound full scans
/// starts from its edges. A labeled, indexed, bound or untyped pattern keeps
/// its node start, and the native label count agrees with the scan.
#[tokio::test]
async fn unselective_typed_patterns_start_from_their_edges() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0,49) AS i CREATE (a:A {k:i})-[:R {q:i % 3}]->(b:B {k:i}) \
         CREATE (b)-[:S]->(a)",
    )
    .await;
    let scan = r::MatchStep::ScanRelationship { relationship: 0 };
    for query in [
        "MATCH ()-[r:R]->() RETURN count(r)",
        "MATCH (a)<-[r:R]-(b) RETURN count(*)",
        "MATCH (a)-[r:R|S]-(b) RETURN count(*)",
        "MATCH (a)-[r:R]->(a) RETURN count(*)",
        "MATCH (a {k:1})-[r:R]->(b) RETURN count(*)",
        "MATCH ()-[r:R]->() RETURN r.q, count(r)",
        "OPTIONAL MATCH ()-[r:R]->() RETURN count(r)",
    ] {
        assert_eq!(steps(&db, query).await.first(), Some(&scan), "{query}");
    }
    for query in [
        "MATCH (a:A)-[r:R]->(b) RETURN count(*)",
        "MATCH (a)-[r:R]->(b:B) RETURN count(*)",
        "MATCH (a)-[r:R]->(b)-[s:S]->(c:A) RETURN count(*)",
        "MATCH (a)-[r]->(b) RETURN count(*)",
        "MATCH (a)-[r:R]->(b) WHERE id(a) = 0 RETURN count(*)",
        "MATCH (a:A {k:0}) MATCH (a)-[r:R]->(b) RETURN count(*)",
        "MATCH ()-[r:R]->() WITH r LIMIT 1 MATCH (a)-[r:R]->(b) RETURN count(*)",
    ] {
        let plans = steps(&db, query).await;
        assert!(
            !plans
                .iter()
                .skip(usize::from(query.contains("WITH r")))
                .any(|step| matches!(step, r::MatchStep::ScanRelationship { .. })),
            "{query}: {plans:?}"
        );
    }
    for (query, expected) in [
        ("MATCH ()-[r:R]->() RETURN count(r)", 50),
        ("MATCH (a)-[r:R|S]-(b) RETURN count(*)", 200),
        ("MATCH (a)-[r:R]->(b)-[s:S]->(a) RETURN count(*)", 50),
        ("MATCH (a)-[r:R]->(b) WHERE a.k = b.k RETURN count(*)", 50),
        ("MATCH (a:A)-[r:R]->(b:B) RETURN count(*)", 50),
    ] {
        assert_eq!(count(&db, query).await, json!(expected), "{query}");
    }
    // A leading optional scan without matches returns one null row.
    for (query, expected) in [
        ("OPTIONAL MATCH ()-[r:Absent]->() RETURN r", json!([[null]])),
        (
            "OPTIONAL MATCH (a)-[r:R]->(b) WHERE r.q = 99 RETURN a, r",
            json!([[null, null]]),
        ),
        ("OPTIONAL MATCH ()-[r:R]->() RETURN count(r)", json!([[50]])),
    ] {
        assert_eq!(json!(run(&db, query).await.rows), expected, "{query}");
    }
    assert_eq!(
        run(
            &db,
            "MATCH ()-[r:R]->() RETURN r.q AS q, count(r) AS c ORDER BY q"
        )
        .await
        .rows,
        vec![
            vec![json!(0), json!(17)],
            vec![json!(1), json!(17)],
            vec![json!(2), json!(16)]
        ]
    );
    let native = db
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as("c", traversal::g().e_with_label("R").count())
                .returning(["c"]),
        ))
        .await
        .unwrap();
    assert_eq!(native["c"], json!(50));
    db.close().await.unwrap();
}

/// A scan sees the statement's own staged edges and deletions, reads its
/// edges once before a later CREATE adds more, and survives a reopen.
#[tokio::test]
async fn relationship_scans_follow_the_statement_and_storage_lifecycle() {
    let source = HelixDbSource::InMemoryToken {
        token: db::ProcessLocalDatabaseToken::new("cypher-relationship-scan-lifecycle").unwrap(),
    };
    let db = HelixDB::open(source.clone()).await.unwrap();
    assert_eq!(
        count(
            &db,
            "CREATE (:A)-[:R]->(:B), (:A)-[:R]->(:B) WITH 1 AS one MATCH ()-[r:R]->() RETURN count(r)"
        )
        .await,
        json!(2)
    );
    // The scan's rows are fixed before the CREATE they drive runs.
    assert_eq!(
        count(
            &db,
            "MATCH ()-[r:R]->() CREATE (:A)-[:R]->(:B) RETURN count(*)"
        )
        .await,
        json!(2)
    );
    assert_eq!(
        count(&db, "MATCH ()-[r:R]->() RETURN count(r)").await,
        json!(4)
    );
    run(&db, "CREATE (a:A)-[:R]->(a)").await;
    assert_eq!(
        run(
            &db,
            "MATCH (a)-[r:R]->(b) WHERE id(a) <> id(b) WITH r LIMIT 2 DELETE r \
             WITH count(*) AS deleted MATCH ()-[s:R]->() RETURN deleted, count(s)"
        )
        .await
        .rows,
        vec![vec![json!(2), json!(3)]]
    );
    assert_eq!(
        count(&db, "MATCH (a)-[r:R]-(a) RETURN count(*)").await,
        json!(1)
    );
    run(&db, "MATCH (a)-[r:R]->(a) DETACH DELETE a").await;
    db.flush_writer().await.unwrap();
    db.close().await.unwrap();
    let reopened = HelixDB::open(source).await.unwrap();
    assert_eq!(
        count(&reopened, "MATCH ()-[r:R]->() RETURN count(r)").await,
        json!(2)
    );
    assert_eq!(
        count(&reopened, "MATCH (a)-[r:R]-(b) RETURN count(*)").await,
        json!(4)
    );
    reopened.close().await.unwrap();
}
