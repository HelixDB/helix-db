use helix_cypher::compile;

#[test]
fn ordering_label_predicates_can_use_a_renamed_projected_node() {
    let query = "MATCH (n) RETURN DISTINCT n AS node ORDER BY n:N";
    assert!(
        compile(query).is_ok(),
        "projected node alias lost inside label predicate"
    );
}
#[test]
fn ordering_can_reuse_a_complete_projected_mixed_aggregate() {
    let query = "RETURN 1 + count(*) AS x ORDER BY 1 + count(*)";
    assert!(
        compile(query).is_ok(),
        "a complete projected aggregation expression must be available to ORDER BY"
    );
}
#[test]
fn ordering_can_reuse_an_aggregate_contained_in_a_mixed_projection() {
    let query = "RETURN 1 + count(*) AS x ORDER BY 2 - count(*)";
    assert!(
        compile(query).is_ok(),
        "an already computed aggregate must be available without a separate public result column"
    );
}
#[test]
fn ordering_aggregate_arguments_keep_the_incoming_scope_under_alias_shadowing() {
    let query = "UNWIND [1,2] AS x RETURN sum(x) AS x ORDER BY x + sum(x)";
    assert!(
        compile(query).is_ok(),
        "aggregate input x must remain distinct from output alias x"
    );
}

#[test]
fn invalid_ordering_aggregates_keep_precedence_over_later_clause_errors() {
    for query in [
        "WITH 1 AS x WITH 1+count(*) ORDER BY sum(x) RETURN 1",
        "WITH 1 AS x RETURN 1+count(*) AS n ORDER BY sum(x) LIMIT missing",
        "WITH 1 AS x WITH 1+count(*) AS n ORDER BY sum(x) WHERE missing RETURN n",
    ] {
        let error = compile(query).unwrap_err();
        assert_eq!(error.phase, helix_planner::relational::ErrorPhase::Compile);
        assert_eq!(error.detail, "UndefinedVariable", "{query}");
        assert_eq!(
            error.message, "ORDER BY aggregate must be projected",
            "{query}"
        );
    }
}
