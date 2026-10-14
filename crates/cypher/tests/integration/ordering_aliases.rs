use crate::allocations;
use helix_planner::relational as r;

#[test]
fn ordering_one_new_alias_does_not_index_every_projection() {
    for width in [1, 16, 128, 512] {
        let columns = (0..width)
            .map(|n| format!("{n} AS c{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let plain = helix_cypher::parse(&format!("RETURN {columns}")).unwrap();
        let ordered = helix_cypher::parse(&format!("RETURN {columns} ORDER BY c0")).unwrap();
        let (plain, baseline) = allocations::observe(|| helix_cypher::resolve(&plain));
        plain.unwrap();
        let (ordered, measured) = allocations::observe(|| helix_cypher::resolve(&ordered));
        let ordered = ordered.unwrap();
        let [r::Operator::Project { ordering, .. }] = ordered.operators() else {
            panic!("one projection")
        };
        assert_eq!(ordering.len(), 1);
        assert_eq!(
            ordering[0].expression,
            r::Expression::Slot(ordered.returns()[0].1)
        );
        // Resolving one output slot adds bounded metadata independent of the
        // number of unrelated projected expressions. Parsing is outside both
        // measurements; no wall-clock or allocator-retention assumption is used.
        assert!(
            measured.allocations <= baseline.allocations + 16,
            "width={width}: {baseline:?} -> {measured:?}"
        );
        assert!(
            measured.bytes <= baseline.bytes + 4096,
            "width={width}: {baseline:?} -> {measured:?}"
        );
    }
}

#[test]
fn wildcard_alias_shadowing_and_computed_ordering_keep_valid_scope() {
    for query in [
        "WITH 1 AS x RETURN * ORDER BY x",
        "WITH 1 AS x RETURN x AS x ORDER BY x",
        "WITH 1 AS x RETURN x AS renamed ORDER BY x",
        "WITH 1 AS x RETURN x+1 AS x ORDER BY x+1",
        "WITH 1 AS x RETURN x+1 AS y ORDER BY x+1",
        "WITH 1 AS x RETURN count(x) AS x ORDER BY x + count(x)",
        "WITH 1 AS x RETURN 1+count(x) AS x ORDER BY x",
    ] {
        helix_cypher::compile(query).unwrap_or_else(|error| panic!("{query}: {error}"));
    }
}
