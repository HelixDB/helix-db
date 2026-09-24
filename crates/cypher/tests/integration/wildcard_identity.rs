use crate::allocations;
use helix_planner::relational as r;

#[test]
fn plain_wildcards_add_no_binding_or_projection_allocation() {
    for width in [1, 8, 32, 128] {
        let prefix = (0..width)
            .map(|i| format!("UNWIND [1] AS v{i} "))
            .collect::<String>();
        let source = helix_cypher::parse(&format!("{prefix}RETURN v0")).unwrap();
        let (expected, baseline) = allocations::observe(|| helix_cypher::resolve(&source));
        let expected = expected.unwrap();
        for repetitions in [1, 16, 128, 1024] {
            let source = helix_cypher::parse(&format!(
                "{prefix}{}RETURN v0",
                "WITH * ".repeat(repetitions)
            ))
            .unwrap();
            let (actual, measured) = allocations::observe(|| helix_cypher::resolve(&source));
            let actual = actual.unwrap();
            assert_eq!(
                measured.allocations, baseline.allocations,
                "width={width}, repetitions={repetitions}"
            );
            assert_eq!(
                measured.bytes, baseline.bytes,
                "width={width}, repetitions={repetitions}"
            );
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn wildcard_modifiers_and_explicit_projections_keep_their_operators() {
    for clause in [
        "WITH DISTINCT *",
        "WITH * WHERE x > 0",
        "WITH * ORDER BY x",
        "WITH * SKIP 0",
        "WITH * LIMIT 1",
        "WITH x",
        "WITH *, 1 AS y",
    ] {
        let source = format!("UNWIND [1,2] AS x {clause} RETURN x");
        let query = helix_cypher::compile(&source).unwrap();
        assert_eq!(query.operators().len(), 3, "{clause}");
        assert!(matches!(query.operators()[1], r::Operator::Project { .. }));
    }
    let query = helix_cypher::compile("UNWIND [1] AS x RETURN *").unwrap();
    assert_eq!(query.operators().len(), 2);
    assert_eq!(query.returns()[0].0, "x");
}

#[test]
fn wildcard_validation_preserves_scope_composition_and_statement_limits() {
    for (query, detail) in [
        ("WITH * RETURN 1", "NoVariablesInScope"),
        ("CREATE (:N) WITH * RETURN 1", "NoVariablesInScope"),
        ("WITH 1 AS x WITH *,* RETURN x", "ColumnNameConflict"),
        ("WITH 1 AS x WITH *", "InvalidClauseComposition"),
        ("RETURN 1 WITH *", "InvalidClauseComposition"),
        ("WITH 1 AS x WITH * RETURN missing", "UndefinedVariable"),
    ] {
        let error = helix_cypher::compile(query).expect_err(query);
        assert_eq!(error.phase, r::ErrorPhase::Compile, "{query}");
        assert_eq!(error.detail, detail, "{query}");
    }
    let query = format!("WITH 1 AS x {}RETURN x", "WITH * ".repeat(4096));
    let error = helix_cypher::compile(&query).unwrap_err();
    assert_eq!(error.detail, "InvalidStatement");
}

#[test]
fn wildcard_prunes_anonymous_bindings_before_later_expansions() {
    for repetitions in [1, 16, 128] {
        let query = helix_cypher::compile(&format!(
            "MATCH (n:N) {}RETURN n",
            "MATCH (n)-->() WITH * WITH * ".repeat(repetitions)
        ))
        .unwrap();
        assert_eq!(query.layout().width(), 3);
        assert_eq!(query.operators().len(), 2 + 2 * repetitions);
        for index in 0..repetitions {
            let contract = &query.contracts()[2 + 2 * index];
            assert_eq!(contract.input().columns().len(), 3);
            assert_eq!(contract.output().slots(), [r::Slot(0)].into());
            assert!(matches!(
                query.operators()[2 + 2 * index],
                r::Operator::Project { .. }
            ));
        }
    }
}

#[test]
fn a_real_projection_releases_anonymous_bindings_and_allows_later_identity_elision() {
    let query = helix_cypher::compile("MATCH () WITH 1 AS x WITH * WITH * RETURN x").unwrap();
    assert_eq!(query.operators().len(), 3);
    assert_eq!(query.contracts()[1].output().columns().len(), 1);
    assert_eq!(query.layout().width(), 2);
}

#[test]
fn wildcard_keeps_anonymous_create_and_named_path_pruning_boundaries() {
    for (source, index, inputs) in [
        ("WITH 1 AS x CREATE (:N) WITH * WITH * RETURN x", 2, 2),
        ("MATCH p=()-->() WITH * WITH * RETURN p", 1, 4),
    ] {
        let query = helix_cypher::compile(source).unwrap();
        assert_eq!(query.operators().len(), index + 2);
        assert!(matches!(
            query.operators()[index],
            r::Operator::Project { .. }
        ));
        assert_eq!(query.contracts()[index].input().columns().len(), inputs);
        assert_eq!(query.contracts()[index].output().columns().len(), 1);
    }
}
