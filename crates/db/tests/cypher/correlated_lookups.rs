use super::membership::create_index;
use super::{database, run};
use helix_ast::index;
use serde_json::json;

/// A correlated lookup through a unique index reads at most one node per
/// input row. It beats a static equality source whose candidates every row
/// re-checks, and a non-unique lookup written before it.
#[tokio::test]
async fn correlated_unique_lookups_read_one_node_per_row() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 599) AS i CREATE (:User {uid: i, tier: i % 3})",
    )
    .await;
    create_index(&db, index::IndexSpec::node_equality("User", "tier")).await;
    create_index(&db, index::IndexSpec::node_unique_equality("User", "uid")).await;
    for (query, expected) in [
        (
            "UNWIND range(1, 100) AS k MATCH (u:User {uid: k, tier: 1}) RETURN count(*) AS c",
            34,
        ),
        (
            "UNWIND range(1, 100) AS k WITH k, k % 3 AS t \
             MATCH (u:User {tier: t, uid: k}) RETURN count(*) AS c",
            100,
        ),
        (
            "UNWIND range(1, 100) AS k WITH k, k % 3 AS t \
             MATCH (u:User) WHERE u.tier = t AND u.uid = k RETURN count(*) AS c",
            100,
        ),
    ] {
        let response = run(&db, query).await;
        assert_eq!(response.rows, vec![vec![json!(expected)]], "{query}");
        // One unique key and at most one node per row, not a tier's 200 nodes.
        let reads = &response.resources.reads;
        assert!(
            reads.point_gets + reads.multi_get_keys <= 3 * 100,
            "{query}: {reads:?}"
        );
    }
    db.close().await.unwrap();
}

/// A correlated lookup beside a bound node that the pattern names again
/// still validates that node, and returns what the scanning plan returns.
#[tokio::test]
async fn correlated_lookups_validate_bound_nodes_in_the_same_pattern() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 29) AS i CREATE (:User {uid: i, tier: i % 3})",
    )
    .await;
    create_index(&db, index::IndexSpec::node_unique_equality("User", "uid")).await;
    for (query, expected) in [
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r MATCH (a), (c:User {uid: r}) RETURN c.uid",
            json!([[3]]),
        ),
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r MATCH (c:User {uid: r}), (a:User) \
             RETURN c.uid",
            json!([[3]]),
        ),
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r MATCH (a {tier: 2}), (c:User {uid: r}) \
             RETURN c.uid",
            json!([]),
        ),
        (
            "MATCH (a:User {uid: 1}) WITH a, 3 AS r OPTIONAL MATCH (a {tier: 2}), \
             (c:User {uid: r}) RETURN c.uid",
            json!([[null]]),
        ),
        (
            "MATCH (a:User) WHERE a.uid < 3 WITH a, a.uid + 3 AS r \
             MATCH (a), (c:User) WHERE c.uid = r RETURN c.uid ORDER BY c.uid",
            json!([[3], [4], [5]]),
        ),
    ] {
        let rows = run(&db, query).await.rows;
        assert_eq!(json!(rows), expected, "{query}");
        // `r + 0` cannot use an index, so the reference scans the label.
        let scanned = run(
            &db,
            &query
                .replace("uid: r", "uid: r + 0")
                .replace("= r", "= r + 0"),
        )
        .await
        .rows;
        assert_eq!(rows, scanned, "{query}");
    }
    db.close().await.unwrap();
}

/// A property of a node, relationship or map bound by an earlier clause can
/// probe an equality index once per row. Each result equals the same query
/// on a database without indexes, including null, missing, list, map and
/// mistyped probes.
#[tokio::test]
async fn property_probes_read_the_index_with_scan_semantics() {
    let indexed = database().await;
    let scanned = database().await;
    for db in [&indexed, &scanned] {
        run(
            db,
            "UNWIND range(0, 299) AS i CREATE (:User {uid: i, tier: i % 3})",
        )
        .await;
        run(
            db,
            "UNWIND range(0, 19) AS i \
             CREATE (:Post {pid: i, author: i * 7, tags: [i]})",
        )
        .await;
        run(
            db,
            "CREATE (:Post {pid: 100}), (:Post {pid: 101, author: 'x'})",
        )
        .await;
        run(
            db,
            "MATCH (p:Post) WHERE p.pid < 5 CREATE (p)-[:BY {uid: p.pid * 3}]->(:Tag)",
        )
        .await;
    }
    create_index(
        &indexed,
        index::IndexSpec::node_unique_equality("User", "uid"),
    )
    .await;
    create_index(&indexed, index::IndexSpec::node_equality("User", "tier")).await;
    create_index(
        &indexed,
        index::IndexSpec::node_unique_equality("Post", "pid"),
    )
    .await;
    for query in [
        "MATCH (p:Post) MATCH (u:User {uid: p.author}) RETURN p.pid, u.uid ORDER BY p.pid",
        "MATCH (p:Post) MATCH (u:User) WHERE u.uid = p.author RETURN p.pid, u.uid ORDER BY p.pid",
        "MATCH (p:Post) OPTIONAL MATCH (u:User {uid: p.author}) \
         RETURN p.pid, u.uid ORDER BY p.pid",
        "MATCH (p:Post) MATCH (u:User {tier: p.pid}) RETURN p.pid, count(u) ORDER BY p.pid",
        "MATCH (p:Post) MATCH (u:User {uid: p.tags}) RETURN count(*)",
        "MATCH ()-[r:BY]->() MATCH (u:User {uid: r.uid}) RETURN u.uid ORDER BY u.uid",
        "MATCH (p:Post) MATCH (p), (u:User {uid: p.author}) RETURN p.pid, u.uid ORDER BY p.pid",
        // A node or relationship bound earlier in the same pattern.
        "MATCH (p:Post {pid: 3}), (u:User {uid: p.author}) RETURN u.uid",
        "MATCH (p:Post), (u:User {uid: p.author}) RETURN p.pid, u.uid ORDER BY p.pid",
        "MATCH (p:Post), (u:User) WHERE u.uid = p.author RETURN p.pid, u.uid ORDER BY p.pid",
        "MATCH (p:Post)-[r:BY]->(), (u:User {uid: r.uid}) RETURN p.pid, u.uid ORDER BY p.pid",
        "MATCH (p:Post), (u:User {uid: p.tags}) RETURN count(*)",
        "OPTIONAL MATCH (q:Post {pid: 999}) WITH q MATCH (u:User {uid: q.author}) RETURN u.uid",
        "WITH {id: 7} AS m MATCH (u:User {uid: m.id}) RETURN u.uid",
        "WITH {id: null} AS m MATCH (u:User {uid: m.id}) RETURN u.uid",
        "WITH {} AS m MATCH (u:User {uid: m.id}) RETURN u.uid",
    ] {
        let direct = run(&indexed, query).await;
        assert_eq!(direct.rows, run(&scanned, query).await.rows, "{query}");
    }
    for query in [
        "MATCH (p:Post) MATCH (u:User {uid: p.author}) SET u.seen = p.pid RETURN count(*)",
        "MATCH (u:User) WHERE u.seen IS NOT NULL RETURN u.uid, u.seen ORDER BY u.uid",
    ] {
        let direct = run(&indexed, query).await;
        assert_eq!(direct.rows, run(&scanned, query).await.rows, "{query}");
    }
    // One unique key and one node per post, not every user per post.
    let direct = run(
        &indexed,
        "MATCH (p:Post) WHERE p.pid < 20 MATCH (u:User {uid: p.author}) RETURN count(*) AS c",
    )
    .await;
    assert_eq!(direct.rows, vec![vec![json!(20)]]);
    let reads = &direct.resources.reads;
    assert!(reads.point_gets + reads.multi_get_keys < 300, "{reads:?}");
    let direct = run(
        &indexed,
        "MATCH (p:Post {pid: 3}), (u:User {uid: p.author}) RETURN u.uid",
    )
    .await;
    assert_eq!(direct.rows, vec![vec![json!(21)]]);
    let reads = &direct.resources.reads;
    assert!(reads.point_gets + reads.multi_get_keys < 20, "{reads:?}");
    // A deleted node's properties cannot be read. The scan fails only when it
    // has a candidate to check, and so does the lookup.
    create_index(&indexed, index::IndexSpec::node_equality("Nobody", "uid")).await;
    for query in [
        "MATCH (p:Post {pid: 1}) DETACH DELETE p WITH p MATCH (u:User {uid: p.author}) RETURN u",
        "MATCH (p:Post {pid: 1}) DETACH DELETE p WITH p MATCH (u:Nobody {uid: p.author}) RETURN u",
    ] {
        let direct = indexed.cypher(db::cypher::Request::new(query)).await;
        let reference = scanned.cypher(db::cypher::Request::new(query)).await;
        assert_eq!(
            direct
                .as_ref()
                .map(|response| &response.rows)
                .map_err(ToString::to_string),
            reference
                .as_ref()
                .map(|response| &response.rows)
                .map_err(ToString::to_string),
            "{query}"
        );
    }
    indexed.close().await.unwrap();
    scanned.close().await.unwrap();
}
