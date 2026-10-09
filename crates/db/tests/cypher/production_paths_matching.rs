//! Pattern matching strategies behind common Cypher reads and writes: bound
//! pattern variables, correlated index lookups and their scan fallbacks,
//! equality joins, nonblocking pipelines, expansions and resource limits.
//! Each query states its exact result, and index-backed plans also equal the
//! same query on a database without indexes.
use super::membership::create_index;
use super::{database, run};
use db::cypher;
use helix_ast::{index, query::QueryValue};
use helix_planner::relational as r;
use serde_json::{json, Value};

/// Twelve users in a `:F` ring, four posts written by every third user, and
/// the list, boolean, float and string properties the lookup cases probe.
async fn seed_social(db: &db::HelixDB) {
    run(
        db,
        "UNWIND range(0, 11) AS i CREATE (:User {uid: i, tier: i % 3, active: i % 4 = 2, \
         score: toFloat(i) / 2, name: 'u' + toString(i), pair: [i, i + 1]})",
    )
    .await;
    run(
        db,
        "MATCH (a:User), (b:User) WHERE b.uid = (a.uid + 1) % 12 \
         CREATE (a)-[:F {since: a.uid}]->(b)",
    )
    .await;
    run(
        db,
        "UNWIND range(0, 3) AS i CREATE (:Post {pid: i, author: i * 3, tags: [i, i + 1]})",
    )
    .await;
    run(
        db,
        "MATCH (p:Post), (u:User) WHERE u.uid = p.author CREATE (u)-[:WROTE {w: p.pid % 2}]->(p)",
    )
    .await;
}

/// Execute with explicit resource limits and return the typed result.
async fn execute(
    db: &db::HelixDB,
    request: cypher::Request,
    limits: cypher::Limits,
) -> cypher::Result<cypher::Response> {
    cypher::execute(
        db,
        request,
        db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        db::query_service::QueryMode::Execute,
        db::execution_control::ExecutionControl::default(),
        limits,
    )
    .await
}

/// The runtime query error of a statement that must fail.
async fn runtime_error(db: &db::HelixDB, request: cypher::Request) -> r::QueryError {
    let query = request.query.clone();
    let error = db.cypher(request).await.unwrap_err();
    let cypher::Error::Query(error) = error else {
        panic!("{query}: expected a query error, got {error:?}");
    };
    assert_eq!(error.phase, r::ErrorPhase::Runtime, "{query}: {error:?}");
    error
}

/// Integer rows in a canonical order, for plans that stream in any order.
fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|row| row.iter().map(Value::as_i64).collect::<Vec<_>>());
    rows
}

/// A variable bound by an earlier clause must hold a node (or relationship)
/// where a later pattern reuses it. Any other value is a runtime type error,
/// whether the pattern streams into its consumer or is materialized before an
/// ORDER BY; null matches nothing, and OPTIONAL MATCH keeps its row.
#[tokio::test]
async fn bound_pattern_variables_accept_only_graph_values_or_null() {
    let db = database().await;
    run(&db, "CREATE (:User {uid: 1})-[:F]->(:User {uid: 2})").await;
    for (query, detail) in [
        (
            "MATCH (n:User {uid: 1}) WITH [n, 1] AS xs UNWIND xs AS a \
             MATCH (a)-[:F]->(b) RETURN b.uid",
            "expected_node",
        ),
        (
            "MATCH (n:User {uid: 1}) WITH [n, 1] AS xs UNWIND xs AS a \
             MATCH (a)-[:F]->(b) RETURN b.uid ORDER BY b.uid",
            "expected_node",
        ),
        (
            "MATCH ()-[r:F]->() WITH [r, 1] AS xs UNWIND xs AS x \
             MATCH ()-[x]->() RETURN count(*)",
            "expected_relationship",
        ),
        (
            "MATCH ()-[r:F]->() WITH [r, 1] AS xs UNWIND xs AS x \
             MATCH (a)-[x]->(b) RETURN a.uid ORDER BY a.uid",
            "expected_relationship",
        ),
    ] {
        let error = runtime_error(&db, cypher::Request::new(query)).await;
        assert_eq!(
            (error.category.as_str(), error.detail.as_str()),
            ("type_error", detail),
            "{query}"
        );
    }
    for (query, expected) in [
        (
            "MATCH (n:User {uid: 1}) WITH [n, null] AS xs UNWIND xs AS a \
             OPTIONAL MATCH (a)-[:F]->(b) RETURN b.uid ORDER BY b.uid",
            json!([[2], [null]]),
        ),
        (
            "OPTIONAL MATCH (a:Missing) WITH a OPTIONAL MATCH (a)-[:F]->(b) \
             RETURN a, b ORDER BY b",
            json!([[null, null]]),
        ),
        (
            "OPTIONAL MATCH (a:Missing) WITH a OPTIONAL MATCH (a)-[:F]->(b) RETURN a, b",
            json!([[null, null]]),
        ),
        (
            "OPTIONAL MATCH (a:Missing) WITH a MATCH (a)-[:F]->(b) RETURN a, b ORDER BY b",
            json!([]),
        ),
        // No input rows reach the later pattern, so nothing is updated.
        (
            "MATCH (a:Missing) MATCH (a)-[:F]->(b) SET b.touched = true",
            json!([]),
        ),
        (
            "MATCH (n:User) WHERE n.touched RETURN count(*)",
            json!([[0]]),
        ),
    ] {
        assert_eq!(json!(run(&db, query).await.rows), expected, "{query}");
    }
    db.close().await.unwrap();
}

/// A correlated index lookup whose probe the index cannot answer exactly (a
/// list, a string larger than an index key, a deleted node's property) scans
/// the label instead, both when the lookup streams and when an ORDER BY
/// materializes it. Boolean and float probes read the index. Every result
/// and error equals the same query on a database without indexes.
#[tokio::test]
async fn index_lookups_answer_unindexable_probes_as_a_label_scan_would() {
    let indexed = database().await;
    let scanned = database().await;
    for db in [&indexed, &scanned] {
        seed_social(db).await;
    }
    for spec in [
        index::IndexSpec::node_unique_equality("User", "uid"),
        index::IndexSpec::node_unique_equality("User", "name"),
        index::IndexSpec::node_equality("User", "active"),
        index::IndexSpec::node_equality("User", "score"),
        index::IndexSpec::node_equality("User", "pair"),
        index::IndexSpec::node_equality("Nobody", "uid"),
    ] {
        create_index(&indexed, spec).await;
    }
    // Longer than any equality index key, so neither lookup form can use it.
    let long = QueryValue::String("x".repeat(1024 * 1024));
    for (query, expected) in [
        (
            "MATCH (p:Post) MATCH (u:User {pair: p.tags}) RETURN p.pid, u.uid ORDER BY p.pid",
            json!([[0, 0], [1, 1], [2, 2], [3, 3]]),
        ),
        (
            "MATCH (p:Post) MATCH (u:User {pair: p.tags}) RETURN p.pid, u.uid",
            json!([[0, 0], [1, 1], [2, 2], [3, 3]]),
        ),
        (
            "UNWIND [true, false] AS b MATCH (u:User {active: b}) \
             RETURN b, count(*) ORDER BY b",
            json!([[false, 9], [true, 3]]),
        ),
        (
            "UNWIND [1.0, 2.5, 3] AS f MATCH (u:User {score: f}) RETURN u.uid ORDER BY u.uid",
            json!([[2], [5], [6]]),
        ),
        (
            "UNWIND [1.0, 2.5] AS f MATCH (u:User {uid: f}) RETURN u.uid",
            json!([[1]]),
        ),
        (
            "UNWIND [$long, 'u3'] AS n MATCH (u:User {name: n}) RETURN u.uid",
            json!([[3]]),
        ),
        (
            "UNWIND [$long, 'u3'] AS n MATCH (u:User {name: n}) RETURN u.uid ORDER BY u.uid",
            json!([[3]]),
        ),
        (
            "WITH ['u5', $long] AS ids MATCH (u:User) WHERE u.name IN ids RETURN u.uid",
            json!([[5]]),
        ),
    ] {
        let mut responses = Vec::new();
        for db in [&indexed, &scanned] {
            let mut request = cypher::Request::new(query);
            request.parameters.insert("long".into(), long.clone());
            responses.push(json!(db.cypher(request).await.unwrap().rows));
        }
        assert_eq!(responses, [expected.clone(), expected], "{query}");
    }
    // A deleted node's property cannot be read. The lookup falls back to the
    // scan, which fails only when the label has a candidate to check; the
    // failed statement keeps the post, the successful one commits its delete.
    for db in [&indexed, &scanned] {
        let error = runtime_error(
            db,
            cypher::Request::new(
                "MATCH (p:Post {pid: 1}) DETACH DELETE p WITH p \
                 MATCH (u:User {uid: p.author}) RETURN u.uid ORDER BY u.uid",
            ),
        )
        .await;
        assert_eq!(
            (error.category.as_str(), error.detail.as_str()),
            ("entity_not_found", "deleted_entity_access")
        );
        assert_eq!(
            run(
                db,
                "MATCH (p:Post {pid: 1}) DETACH DELETE p WITH p \
                 MATCH (u:Nobody {uid: p.author}) RETURN u.uid ORDER BY u.uid",
            )
            .await
            .rows,
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            run(db, "MATCH (p:Post) RETURN count(*)").await.rows,
            vec![vec![json!(3)]]
        );
    }
    indexed.close().await.unwrap();
    scanned.close().await.unwrap();
}

/// An equality join keeps every build duplicate when one probe bucket spans
/// several output batches, reuses its table for every outer row, and fails a
/// statement whose probe node was deleted without committing its writes.
#[tokio::test]
async fn equality_joins_resume_buckets_and_report_deleted_probes() {
    let db = database().await;
    run(
        &db,
        "CREATE (:Left {key: 1}), (:Left {key: 1}), (:Left {key: 1}), (:Left {key: 2}), \
         (:Left {key: 3}), (:Right {key: 1}), (:Right {key: 1}), (:Right {key: 1}), \
         (:Right {key: 2}), (:Right {key: 4})",
    )
    .await;
    let correlated = "MATCH (a:Left) MATCH (a), (b:Right) WHERE a.key = b.key RETURN count(*)";
    let explanation = db
        .explain_cypher(cypher::Request::new(correlated))
        .await
        .unwrap();
    assert!(explanation
        .plan()
        .matches()
        .values()
        .flat_map(|pattern| &pattern.steps)
        .any(|step| matches!(step, r::MatchStep::HashJoin { .. })));
    // Three keys of 1 on each side and one key of 2 join into ten rows.
    let small = cypher::Limits {
        batch_rows: 2,
        ..cypher::Limits::default()
    };
    for limits in [small, cypher::Limits::default()] {
        for (query, expected) in [
            (
                "MATCH (a:Left), (b:Right) WHERE a.key = b.key RETURN count(*)",
                json!([[10]]),
            ),
            (correlated, json!([[10]])),
            (
                "MATCH (a:Left), (b:Right) WHERE a.key = b.key \
                 RETURN a.key, b.key ORDER BY a.key",
                json!([
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [1, 1],
                    [2, 2]
                ]),
            ),
        ] {
            let response = execute(&db, cypher::Request::new(query), limits)
                .await
                .unwrap();
            assert_eq!(json!(response.rows), expected, "{query}");
        }
    }
    let expected = [1, 2]
        .into_iter()
        .flat_map(|x| std::iter::repeat_n(json!([x, 1]), 9).chain(std::iter::once(json!([x, 2]))))
        .collect::<Vec<_>>();
    assert_eq!(
        json!(
            run(
                &db,
                "UNWIND [1, 2] AS x MATCH (a:Left), (b:Right) WHERE a.key = b.key \
                 RETURN x, a.key AS k ORDER BY x, k",
            )
            .await
            .rows
        ),
        json!(expected)
    );
    for query in [
        "MATCH (a:Left {key: 2}) DETACH DELETE a WITH a \
         MATCH (a), (b:Right) WHERE b.key = a.key RETURN count(*)",
        "MATCH (a:Left {key: 2}) DETACH DELETE a WITH a \
         MATCH (a), (b:Right) WHERE b.key = a.key RETURN b.key ORDER BY b.key",
    ] {
        let error = runtime_error(&db, cypher::Request::new(query)).await;
        assert_eq!(
            (error.category.as_str(), error.detail.as_str()),
            ("entity_not_found", "deleted_entity_access"),
            "{query}"
        );
    }
    assert_eq!(
        run(&db, "MATCH (a:Left) RETURN count(*)").await.rows,
        vec![vec![json!(5)]]
    );
    db.close().await.unwrap();
}

/// Rows from a MATCH flow through later UNWIND, WITH ... WHERE and OPTIONAL
/// MATCH stages without being retained, into a plain projection, a DISTINCT,
/// a top-k ORDER BY or a LIMIT that stops the stream.
#[tokio::test]
async fn pipelines_unwind_filter_and_finish_in_distinct_top_k_or_limit() {
    let db = database().await;
    seed_social(&db).await;
    for (query, expected) in [
        (
            "MATCH (p:Post) UNWIND p.tags AS t RETURN p.pid, t",
            json!([
                [0, 0],
                [0, 1],
                [1, 1],
                [1, 2],
                [2, 2],
                [2, 3],
                [3, 3],
                [3, 4]
            ]),
        ),
        (
            "MATCH (p:Post) UNWIND p.tags AS t RETURN DISTINCT t",
            json!([[0], [1], [2], [3], [4]]),
        ),
        (
            "MATCH (p:Post) UNWIND p.tags AS t WITH p, t WHERE t > 100 RETURN p.pid, t",
            json!([]),
        ),
        (
            "MATCH (u:User) WITH u UNWIND [1, 2] AS x MATCH (u)-[:F]->(v) RETURN DISTINCT x",
            json!([[1], [2]]),
        ),
    ] {
        assert_eq!(
            json!(sorted(run(&db, query).await.rows)),
            expected,
            "{query}"
        );
    }
    for (query, expected) in [
        (
            "MATCH (p:Post) UNWIND p.tags AS t RETURN t ORDER BY t DESC LIMIT 3",
            json!([[4], [3], [3]]),
        ),
        (
            "MATCH (u:User) WITH u UNWIND [1, 2] AS x MATCH (u)-[:F]->(v) \
             RETURN x, v.uid ORDER BY v.uid DESC, x LIMIT 3",
            json!([[1, 11], [2, 11], [1, 10]]),
        ),
        // Users are read in creation order; only the first user's row is kept.
        (
            "MATCH (u:User) OPTIONAL MATCH (u)-[:WROTE]->(p) RETURN u.uid, p.pid LIMIT 1",
            json!([[0, 0]]),
        ),
        (
            "MATCH (u:User) OPTIONAL MATCH (u)-[:WROTE]->(p) WITH u, p LIMIT 2 RETURN u.uid",
            json!([[0], [1]]),
        ),
    ] {
        assert_eq!(json!(run(&db, query).await.rows), expected, "{query}");
    }
    db.close().await.unwrap();
}

/// Expansion keeps only relationships of the requested types and the bound
/// relationship, never reuses a relationship within one pattern, checks
/// relationship properties and path expressions, and resumes a neighborhood
/// larger than one batch where it stopped.
#[tokio::test]
async fn expansions_check_types_bound_relationships_and_resume_neighborhoods() {
    let db = database().await;
    seed_social(&db).await;
    run(
        &db,
        "CREATE (h:Hub {h: 1}), (g:Hub {h: 2}) WITH h, g UNWIND range(1, 5) AS i \
         CREATE (h)-[:R]->(:Leaf {i: i}) WITH g, i WHERE i <= 3 CREATE (g)-[:R]->(:Leaf {i: i})",
    )
    .await;
    run(
        &db,
        "MATCH (a:User {uid: 0}), (b:User {uid: 1}) \
         CREATE (a)-[:G]->(a), (a)-[:G]->(b), (b)-[:G]->(a)",
    )
    .await;
    for (query, expected) in [
        (
            "MATCH (a:User {uid: 0}), (b:User {uid: 1}) WITH a, b \
             MATCH (a)-[:WROTE]->(b) RETURN count(*)",
            json!([[0]]),
        ),
        (
            "MATCH (a:User {uid: 0}), (b:User {uid: 1}) WITH a, b \
             MATCH (a)-[r:F|WROTE]->(b) RETURN type(r), r.since",
            json!([["F", 0]]),
        ),
        (
            "MATCH (a:User)-[r:WROTE {w: 1}]->(b) RETURN a.uid, b.pid ORDER BY b.pid",
            json!([[3, 1], [9, 3]]),
        ),
        (
            "MATCH p = (a:User)-[:F]->(b) WHERE head(nodes(p)).uid = 3 RETURN b.uid",
            json!([[4]]),
        ),
        (
            "MATCH p = (a:User)-[:F]->(b) WHERE last(relationships(p)).since = 7 \
             RETURN a.uid, b.uid",
            json!([[7, 8]]),
        ),
    ] {
        assert_eq!(json!(run(&db, query).await.rows), expected, "{query}");
    }
    // Single-row batches also hold candidates that are all rejected.
    let single = cypher::Limits {
        batch_rows: 1,
        ..cypher::Limits::default()
    };
    let small = cypher::Limits {
        batch_rows: 2,
        ..cypher::Limits::default()
    };
    for limits in [single, small, cypher::Limits::default()] {
        for (query, expected) in [
            // The self-loop cannot close the cycle a second time.
            (
                "MATCH (a:User)-[:G]->(b:User)-[:G]->(a) \
                 RETURN a.uid, b.uid ORDER BY a.uid, b.uid",
                json!([[0, 1], [1, 0]]),
            ),
            (
                "MATCH ()-[r:F {since: 4}]->() WITH r MATCH (x)-[r]->(y) RETURN x.uid, y.uid",
                json!([[4, 5]]),
            ),
            (
                "MATCH (h:Hub)-[:R]->(l) RETURN h.h, count(*), sum(l.i) ORDER BY h.h",
                json!([[1, 5, 15], [2, 3, 6]]),
            ),
            ("MATCH (h:Hub)-[r]->(l) RETURN count(*)", json!([[8]])),
            ("MATCH (u:User)-[:F]->(v) RETURN count(*)", json!([[12]])),
        ] {
            let response = execute(&db, cypher::Request::new(query), limits)
                .await
                .unwrap();
            assert_eq!(json!(response.rows), expected, "{query}");
        }
    }
    db.close().await.unwrap();
}

/// A LIMIT over a filtered label scan stops once it has its rows, while an
/// expression that fails on a later row still fails the whole statement,
/// both behind a LIMIT and in a pattern predicate before ORDER BY.
#[tokio::test]
async fn pattern_demand_stops_early_but_never_hides_errors() {
    let db = database().await;
    seed_social(&db).await;
    for (query, expected) in [
        (
            "MATCH (u:User {active: true}) WITH u LIMIT 2 RETURN u.uid",
            json!([[2], [6]]),
        ),
        (
            "UNWIND range(1, 5) AS x RETURN 10 / x AS y LIMIT 2",
            json!([[10], [5]]),
        ),
    ] {
        assert_eq!(json!(run(&db, query).await.rows), expected, "{query}");
    }
    for query in [
        "UNWIND [1, 2, 0] AS x RETURN 10 / x AS y LIMIT 1",
        "UNWIND [1] AS x MATCH (n:User) WHERE n.uid / (n.uid - 4) > 1 \
         RETURN n.uid ORDER BY n.uid",
    ] {
        let error = runtime_error(&db, cypher::Request::new(query)).await;
        assert_eq!(
            (error.category.as_str(), error.detail.as_str()),
            ("arithmetic_error", "division_by_zero"),
            "{query}"
        );
    }
    db.close().await.unwrap();
}

/// A range-indexed source paired with several outer rows replays its first
/// 65,536 owners in index order and reads the rest of the range once. Every
/// outer row still sees every owner exactly once.
#[tokio::test]
async fn range_sources_past_their_arrival_prefix_replay_every_owner() {
    let db = database().await;
    create_index(&db, index::IndexSpec::node_range("Big", "v")).await;
    run(&db, "UNWIND range(1, 65540) AS i CREATE (:Big {v: i})").await;
    assert_eq!(
        run(
            &db,
            "UNWIND [1, 2] AS x MATCH (b:Big) WHERE b.v >= 0 \
             RETURN x, count(*), sum(b.v), min(b.v), max(b.v) ORDER BY x",
        )
        .await
        .rows,
        [1, 2]
            .into_iter()
            .map(|x| vec![
                json!(x),
                json!(65540),
                json!(65540_i64 * 65541 / 2),
                json!(1),
                json!(65540)
            ])
            .collect::<Vec<_>>()
    );
    db.close().await.unwrap();
}

/// Response and memory limits fail a statement with a typed resource error
/// before returning a partial result, even when no row is produced.
#[tokio::test]
async fn response_and_memory_limits_fail_with_resource_errors() {
    let db = database().await;
    let tiny_response = cypher::Limits {
        result_bytes: 16,
        ..cypher::Limits::default()
    };
    for query in ["MATCH (n:Missing) RETURN n", "RETURN 1 AS x"] {
        let error = execute(&db, cypher::Request::new(query), tiny_response)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, cypher::Error::Query(e)
                if e.category == "resource_limit" && e.detail == "result_limit"
                    && e.phase == r::ErrorPhase::Runtime),
            "{query}: {error:?}"
        );
    }
    // One kilobyte cannot hold the statement's own execution state. The wide
    // schemas need more than their budget for a single row of values.
    for (memory_bytes, width) in [(1024, 1), (32 * 1024, 2000), (64 * 1024, 4000)] {
        assert!(width == 1 || width * size_of::<r::Value>() > memory_bytes);
        let columns = (0..width)
            .map(|i| format!("{i} AS v{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("WITH {columns} RETURN v0");
        let limits = cypher::Limits {
            memory_bytes,
            ..cypher::Limits::default()
        };
        let error = execute(&db, cypher::Request::new(query), limits)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, cypher::Error::Query(e)
                if e.category == "resource_limit" && e.detail == "memory_limit"
                    && e.phase == r::ErrorPhase::Runtime),
            "{memory_bytes} bytes, {width} columns: {error:?}"
        );
    }
    db.close().await.unwrap();
}
