use helix_planner::relational as r;

#[test]
fn computed_graph_aliases_keep_their_inferred_binding_kind() {
    for (query, expected) in [
        (
            "MATCH (a:N) WITH CASE WHEN true THEN a END AS x MATCH (x) RETURN x",
            r::BindingType::Node,
        ),
        (
            "MATCH (a:N) WITH CASE a WHEN a THEN a END AS x MATCH (x) RETURN x",
            r::BindingType::Node,
        ),
        (
            "MATCH (a:N) WITH CASE WHEN false THEN a END AS x RETURN x:N",
            r::BindingType::Node,
        ),
        (
            "MATCH ()-[r:R]->() WITH CASE WHEN true THEN r END AS x MATCH ()-[x]->() RETURN x",
            r::BindingType::Relationship,
        ),
        (
            "MATCH ()-[r:R]->() WITH CASE r WHEN r THEN r END AS x SET x.value=1 RETURN x",
            r::BindingType::Relationship,
        ),
        (
            "MATCH p=()-[:R]->() WITH CASE WHEN true THEN p END AS x RETURN nodes(x)",
            r::BindingType::Path,
        ),
        (
            "MATCH p=()-[:R]->() WITH CASE p WHEN p THEN p END AS x DELETE x",
            r::BindingType::Path,
        ),
    ] {
        let query = helix_cypher::compile(query).unwrap_or_else(|error| panic!("{query}: {error}"));
        let binding = query
            .bindings()
            .iter()
            .find(|binding| binding.name == "x")
            .unwrap();
        assert_eq!(binding.kind, expected);
    }
}

#[test]
fn computed_aliases_reject_incompatible_graph_categories() {
    for query in [
        "MATCH (a:N) WITH CASE WHEN true THEN a END AS x MATCH ()-[x]->() RETURN x",
        "MATCH ()-[r:R]->() WITH CASE WHEN true THEN r END AS x MATCH (x) RETURN x",
        "MATCH p=()-[:R]->() WITH CASE WHEN true THEN p END AS x MATCH (x) RETURN x",
        "WITH CASE WHEN true THEN 1 END AS x MATCH (x) RETURN x",
    ] {
        let error = helix_cypher::compile(query).expect_err(query);
        assert_eq!(error.category, "SyntaxError");
        assert_eq!(error.detail, "VariableTypeConflict");
        assert_eq!(error.phase, r::ErrorPhase::Compile);
    }
}
