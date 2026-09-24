use helix_cypher::syntax;
use helix_planner::relational as r;

#[test]
fn pinned_pattern_punctuation_preserves_direction_and_byte_spans() {
    // Upstream basic-grammar.xml: LeftArrowHead, RightArrowHead, and Dash.
    for dash in [
        '-', '\u{00ad}', '\u{2010}', '\u{2011}', '\u{2012}', '\u{2013}', '\u{2014}', '\u{2015}',
        '\u{2212}', '\u{fe58}', '\u{fe63}', '\u{ff0d}',
    ] {
        for (left, right, expected) in [
            ("", "", r::Direction::Undirected),
            ("<", "", r::Direction::Incoming),
            ("", ">", r::Direction::Outgoing),
            ("<", ">", r::Direction::Undirected),
            ("⟨", "⟩", r::Direction::Undirected),
            ("〈", "〉", r::Direction::Undirected),
            ("﹤", "﹥", r::Direction::Undirected),
            ("＜", "＞", r::Direction::Undirected),
        ] {
            for spacing in ["", " \t ", "/*gap*/"] {
                let text = format!("MATCH p=(a:A){spacing}{left}{spacing}{dash}{spacing}[r:T]{spacing}{dash}{spacing}{right}{spacing}(b:B) RETURN 1 AS value");
                let statement =
                    helix_cypher::parse(&text).unwrap_or_else(|error| panic!("{text}: {error}"));
                let syntax::Clause::Match { patterns, .. } = &statement.clauses[0] else {
                    panic!("match");
                };
                assert_eq!(patterns[0].relationships[0].direction, expected, "{text}");
                assert_eq!(patterns[0].relationships[0].types, ["T"]);
                assert_eq!(patterns[0].nodes.len(), 2);
                let syntax::Clause::Project { items, .. } = &statement.clauses[1] else {
                    panic!("project");
                };
                let syntax::Item::Expression { expression, .. } = &items[0] else {
                    panic!("expression");
                };
                assert_eq!(&text[expression.span.start..expression.span.end], "1");
                helix_cypher::resolve(&statement).unwrap();
            }
        }
    }
    for pattern in [
        "(a)< - -(b)",
        "(a)- - >(b)",
        "(a)< /*gap*/ -[r]- /*gap*/ >(b)",
    ] {
        helix_cypher::compile(&format!("MATCH {pattern} RETURN a")).unwrap();
    }
}

#[test]
fn graph_only_unicode_does_not_change_scalar_operators_or_identifiers() {
    for symbol in [
        '⟨', '〈', '﹤', '＜', '⟩', '〉', '﹥', '＞', '\u{00ad}', '‐', '‑', '‒', '–', '—', '―',
        '−', '﹘', '﹣', '－',
    ] {
        for text in [
            format!("RETURN 1{symbol}2"),
            format!("RETURN {symbol}2"),
            format!("MATCH (n:{symbol}) RETURN n"),
        ] {
            let error = helix_cypher::compile(&text).unwrap_err();
            assert_eq!(error.category, "SyntaxError", "{text}");
            assert_eq!(error.detail, "InvalidUnicodeCharacter", "{text}");
            let span = error.span.unwrap();
            assert_eq!(&text[span.start..span.end], symbol.to_string());
        }
    }
    for text in [
        "MATCH (a)<(b) RETURN a",
        "MATCH (a)-[r]>(b) RETURN a",
        "MATCH (a)->(b) RETURN a",
    ] {
        assert_eq!(
            helix_cypher::compile(text).unwrap_err().category,
            "SyntaxError"
        );
    }
}

#[test]
fn deferred_pattern_expressions_are_recognized_without_reinterpreting_scalars() {
    for text in [
        "RETURN NOT ()-->()",
        "MATCH (n) WHERE (n)-[:T]->() RETURN n",
        "MATCH (n) WHERE NOT (n)<--() RETURN n",
        "MATCH (n) WHERE (n)-[*]->() RETURN n",
        "MATCH (n) WHERE (n)-[*1..3]-() RETURN n",
        "MATCH (n) RETURN CASE WHEN (n)-[*..3]->() THEN 1 ELSE 2 END",
        "MATCH (n) WHERE (n {x:((1+2))})-[r:T {x:[1,2]}]->() RETURN n",
        "MATCH (n) RETURN NOT ((n)-[:T]->())",
        "MATCH (n) RETURN exists((n)-->() )",
        "MATCH (n) RETURN true AND (n)-->()",
        "MATCH (n) RETURN (n)-->() OR false",
    ] {
        let error = helix_cypher::compile(text).unwrap_err();
        assert_eq!(
            (&*error.category, &*error.detail),
            ("UnsupportedFeature", "PatternExpression"),
            "{text}: {error}"
        );
        let span = error.span.unwrap();
        assert!(text[span.start..span.end].starts_with('('));
        assert!(text[span.start..span.end].ends_with(')'));
    }
    for text in [
        "RETURN ()-->()",
        "MATCH (n) RETURN (n)-[]->()",
        "MATCH (n) WITH (n)-[]->() AS x RETURN x",
        "MATCH (n) RETURN size((n)-[:R]->())",
        "MATCH (n) SET n.prop=head(nodes(head((n)-[:R]->()))).foo",
        "MATCH (n) RETURN CASE (n)-->() WHEN true THEN 1 END",
        "MATCH (n) RETURN [(n)-->()]",
    ] {
        let error = helix_cypher::compile(text).unwrap_err();
        assert_eq!(
            (&*error.category, &*error.detail),
            ("SyntaxError", "UnexpectedSyntax"),
            "{text}: {error}"
        );
    }
    for text in [
        "RETURN [()-->() | 1]",
        "MATCH (n) RETURN [(n)-[*1..]->(b) | b]",
        "MATCH (n) RETURN [p=(n)-[:T]->(b) WHERE b.x>1 | nodes(p)]",
    ] {
        let error = helix_cypher::compile(text).unwrap_err();
        assert_eq!(
            (&*error.category, &*error.detail),
            ("UnsupportedFeature", "PatternComprehension"),
            "{text}: {error}"
        );
    }
    for text in [
        "RETURN (1)--(2)",
        "RETURN (1)<-(2)",
        "WITH 1 AS a, 2 AS x RETURN (a)-[x*2]",
        "WITH 1 AS a RETURN [(a)-[2*3]]",
        "RETURN [1,2,3], [], [(1+2)]",
        "RETURN ([{a:((1))}])",
    ] {
        helix_cypher::parse(text).unwrap_or_else(|error| panic!("{text}: {error}"));
    }
    assert_eq!(
        helix_cypher::parse("RETURN (1)-->()").unwrap_err().category,
        "SyntaxError"
    );
    for text in ["RETURN [()-->() nonsense]", "RETURN [p=()-->() nonsense]"] {
        assert_ne!(
            helix_cypher::parse(text).unwrap_err().detail,
            "PatternComprehension",
            "{text}"
        );
    }
}
