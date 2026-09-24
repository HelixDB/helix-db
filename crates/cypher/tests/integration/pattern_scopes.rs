use crate::allocations;
use helix_planner::relational as r;

#[test]
fn bound_match_resolution_does_not_copy_unrelated_visible_names() {
    let mut expected = [None; 3];
    for width in [1, 16, 128, 512] {
        let patterns = (0..width)
            .map(|slot| format!("(n{slot})"))
            .collect::<Vec<_>>()
            .join(",");
        let baseline = helix_cypher::parse(&format!("MATCH {patterns} RETURN n0")).unwrap();
        for (index, repetitions) in [1, 16, 128].into_iter().enumerate() {
            let syntax = helix_cypher::parse(&format!(
                "MATCH {patterns} {}RETURN n0",
                "MATCH (n0) ".repeat(repetitions)
            ))
            .unwrap();
            let (before, baseline_allocations) =
                allocations::observe(|| helix_cypher::resolve(&baseline));
            let before = before.unwrap();
            let (after, measured) = allocations::observe(|| helix_cypher::resolve(&syntax));
            let after = after.unwrap();
            let extra = (
                measured.allocations - baseline_allocations.allocations,
                measured.bytes - baseline_allocations.bytes,
            );
            let expected = expected[index].get_or_insert(extra);
            assert_eq!(extra, *expected, "width={width}, repetitions={repetitions}");
            assert_eq!(before.bindings(), after.bindings());
            assert_eq!(before.returns(), after.returns());
            assert_eq!(after.operators().len(), 2 + repetitions);
        }
    }
}

#[test]
fn create_distinguishes_incoming_bindings_from_new_and_shadowed_names() {
    for source in [
        "CREATE (a:N),(b:N),(a)-[:R]->(b)",
        "CREATE (a:N) WITH a AS b CREATE (b)-[:R]->(:N) RETURN b",
        "CREATE (a:N) WITH 1 AS x CREATE (a:N) RETURN x,a",
        "MATCH (a:N) CREATE (a)-[:R]->(:N) RETURN a",
        "MATCH () WITH 1 AS x CREATE (`@0`:N) RETURN `@0`",
    ] {
        helix_cypher::compile(source).unwrap_or_else(|error| panic!("{source}: {error}"));
    }
    for source in [
        "MATCH (a:N) CREATE (a:N)",
        "CREATE (a:N),(a:N)",
        "CREATE (a:N),(a)",
        "MATCH ()-[r:R]->() CREATE (:N)-[r:R]->(:N)",
        "CREATE (a:N)-[r:R]->(b:N),(a)-[r:R]->(b)",
        "MATCH (a:N) WITH a AS b CREATE (b {x:1})-[:R]->(:N)",
    ] {
        let error = helix_cypher::compile(source).expect_err(source);
        assert_eq!(error.phase, r::ErrorPhase::Compile, "{source}");
        assert_eq!(error.category, "SyntaxError", "{source}");
        assert_eq!(error.detail, "VariableAlreadyBound", "{source}");
    }
}
