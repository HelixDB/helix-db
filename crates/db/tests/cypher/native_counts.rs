use super::membership::create_index;
use super::{database, run};
use helix_ast::expr::Predicate;
use helix_ast::{batch, index, query, traversal};

/// A native count must apply every conjunct of a range-driven intersection,
/// including when the count wraps its source in a residual filter, a dedup
/// or a union branch. Cypher's row pipeline is the reference count.
#[tokio::test]
async fn native_counts_over_range_intersections_apply_every_filter() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 299) AS i CREATE (:User {uid: i, rank: i % 60, tier: i % 3, \
         name: 'name' + toString(i % 10)})",
    )
    .await;
    create_index(&db, index::IndexSpec::node_range("User", "rank")).await;
    create_index(&db, index::IndexSpec::node_equality("User", "tier")).await;
    create_index(&db, index::IndexSpec::node_unique_equality("User", "uid")).await;
    create_index(&db, index::IndexSpec::node_equality("User", "name")).await;
    for (predicate, condition, dedup) in [
        (
            Predicate::and(vec![
                Predicate::lt("rank", 30),
                Predicate::eq("tier", 1),
                Predicate::neq("uid", 1),
            ]),
            "n.rank < 30 AND n.tier = 1 AND n.uid <> 1",
            false,
        ),
        (
            Predicate::and(vec![Predicate::lt("rank", 30), Predicate::eq("tier", 1)]),
            "n.rank < 30 AND n.tier = 1",
            true,
        ),
        (
            Predicate::or(vec![
                Predicate::eq("uid", 5),
                Predicate::and(vec![Predicate::eq("tier", 1), Predicate::lt("rank", 30)]),
            ]),
            "n.uid = 5 OR (n.tier = 1 AND n.rank < 30)",
            false,
        ),
        (
            Predicate::and(vec![
                Predicate::gte("rank", 0),
                Predicate::eq("name", "name5"),
            ]),
            "n.rank >= 0 AND n.name = 'name5'",
            false,
        ),
    ] {
        let source = traversal::g().n_with_label_where("User", predicate);
        let count = if dedup {
            source.dedup().count()
        } else {
            source.count()
        };
        let native = db
            .query(query::QueryRequest::read(
                batch::read_batch().var_as("c", count).returning(["c"]),
            ))
            .await
            .unwrap()["c"]
            .as_u64()
            .unwrap();
        let expected = run(
            &db,
            &format!("MATCH (n:User) WHERE {condition} RETURN count(*) AS c"),
        )
        .await
        .rows[0][0]
            .as_u64()
            .unwrap();
        assert_eq!(native, expected, "{condition} dedup={dedup}");
    }

    // Edge range intersections share the count cursor contract.
    run(
        &db,
        "MATCH (a:User), (b:User) WHERE b.uid = (a.uid + 1) % 300 \
         CREATE (a)-[:F {w: a.uid % 60, k: a.uid % 3}]->(b)",
    )
    .await;
    create_index(&db, index::IndexSpec::edge_range("F", "w")).await;
    create_index(&db, index::IndexSpec::edge_equality("F", "k")).await;
    let count = traversal::g()
        .e_with_label_where(
            "F",
            Predicate::and(vec![
                Predicate::lt("w", 30),
                Predicate::eq("k", 1),
                Predicate::neq("w", 1),
            ]),
        )
        .count();
    let native = db
        .query(query::QueryRequest::read(
            batch::read_batch().var_as("c", count).returning(["c"]),
        ))
        .await
        .unwrap()["c"]
        .as_u64()
        .unwrap();
    let expected = run(
        &db,
        "MATCH ()-[r:F]->() WHERE r.w < 30 AND r.k = 1 AND r.w <> 1 RETURN count(*) AS c",
    )
    .await
    .rows[0][0]
        .as_u64()
        .unwrap();
    assert_eq!(native, expected, "edges");
    db.close().await.unwrap();
}
