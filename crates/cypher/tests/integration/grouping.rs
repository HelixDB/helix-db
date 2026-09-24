use crate::allocations;
use helix_cypher::compile;
use helix_planner::relational as r;

#[test]
fn mixed_aggregates_require_recognized_grouping_dependencies() {
    for query in [
        "MATCH (n) RETURN CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "MATCH (n) RETURN n:N AS label, CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "CREATE (n:N) RETURN CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "MATCH (n) RETURN {ok: n:N, c: count(*)} AS result",
        "MATCH (n) RETURN [n:N, count(*)] AS result",
        "MATCH (n) RETURN coalesce(n:N, count(*)) AS result",
        "MATCH (n) RETURN CASE WHEN true THEN count(*) ELSE n:N END AS result",
        "MATCH (n) RETURN n.x, CASE WHEN n:N THEN count(*) ELSE 0 END AS result",
        "WITH {dim: {x: 1}} AS b RETURN b.dim.x AS x, b.dim.x + count(*) AS total",
        "WITH {x: 1} AS b RETURN b['x'] AS x, b['x'] + count(*) AS total",
        "WITH {x: 1} AS b RETURN b.x AS x, b.y + count(*) AS total",
        "WITH {x: 1} AS b RETURN b.x + 1 AS x, b.x + 1 + count(*) AS total",
    ] {
        let error = compile(query).expect_err(query);
        assert_eq!(error.phase, r::ErrorPhase::Compile, "{query}");
        assert_eq!(error.category, "SyntaxError", "{query}");
        assert_eq!(error.detail, "AmbiguousAggregationExpression", "{query}");
    }
}

#[test]
fn recognized_keys_and_aggregate_arguments_retain_valid_dependencies() {
    for query in [
        "MATCH (n) RETURN n, CASE WHEN n:N THEN count(*) ELSE 0 END AS c",
        "MATCH (n) RETURN n AS node, {ok: n:N, c: count(*)} AS result",
        "MATCH (n) RETURN n, [n:N, count(*)] AS result",
        "MATCH (n) RETURN n, coalesce(n:N, count(*)) AS result",
        "MATCH (n) RETURN n, CASE WHEN true THEN count(*) ELSE n:N END AS result",
        "MATCH (n) RETURN n:N AS label, count(*) AS c",
        "MATCH (n) RETURN count(CASE WHEN n:N THEN 1 END) AS c",
        "WITH {dim: {x: 1}} AS b RETURN b.dim AS d, b.dim.x + count(*) AS total",
        "WITH {dim: {x: 1}} AS b RETURN b, b.dim.x + count(*) AS total",
        "WITH {x: 1} AS b RETURN b.x AS x, b.x + count(*) AS total",
        "WITH {x: 1} AS b RETURN b, b['x'] + count(*) AS total",
        "WITH {dim: {x: 1}} AS b RETURN sum(b.dim.x) AS total",
        "RETURN $x + count($x) AS result",
        "RETURN CASE WHEN true THEN count(*) ELSE 0 END AS result",
    ] {
        compile(query).unwrap_or_else(|error| panic!("{query}: {error}"));
    }
}

#[test]
fn aggregation_validation_does_not_clone_owned_literal_payloads() {
    for size in [65_536, 1_048_576] {
        let payload = "x".repeat(size);
        for text in [
            format!("RETURN count('{payload}') AS n"),
            format!("RETURN size('{payload}') + count(*) AS n"),
            format!("RETURN size('{payload}') + count(*) AS n ORDER BY n"),
            format!("RETURN count('{payload}') AS n ORDER BY n"),
            format!("WITH 1 AS x RETURN x AS x, x + count('{payload}') AS n"),
        ] {
            let statement = helix_cypher::parse(&text).unwrap();
            let (query, count) = allocations::observe(|| helix_cypher::resolve(&statement));
            let query = query.unwrap();
            // Resolution owns one copy of the syntax literal. Validation must
            // borrow that payload; fixed plan metadata fits this small allowance.
            assert!(count.bytes <= size + 16 * 1024, "{size}: {count:?}");
            let mut literals = 0;
            for operator in query.operators() {
                let r::Operator::Project { items, .. } = operator else {
                    continue;
                };
                for item in items {
                    item.expression.visit(&mut |expression| {
                        let r::Expression::Literal(r::Value::String(value)) = expression else {
                            return;
                        };
                        assert_eq!(value, &payload);
                        literals += 1;
                    });
                }
            }
            assert_eq!(literals, 1);
        }
    }
}

#[test]
fn ordering_reuses_projected_expressions_without_copying_literal_payloads() {
    let size = 1_048_576;
    let payload = "x".repeat(size);
    let text =
        format!("RETURN size('{payload}') + count(*) AS n ORDER BY size('{payload}') + count(*)");
    let statement = helix_cypher::parse(&text).unwrap();
    let (query, count) = allocations::observe(|| helix_cypher::resolve(&statement));
    let query = query.unwrap();
    // The two syntax occurrences each produce one owned resolved literal.
    // Matching ORDER BY discards its occurrence without cloning either payload.
    assert!(count.bytes <= 2 * size + 16 * 1024, "{count:?}");
    let mut retained = 0;
    for operator in query.operators() {
        for expression in operator.expressions() {
            expression.visit(&mut |node| {
                let r::Expression::Literal(r::Value::String(value)) = node else {
                    return;
                };
                assert_eq!(value, &payload);
                retained += 1;
            });
        }
    }
    assert_eq!(retained, 1);
}
