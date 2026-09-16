use helix_cypher::compile;
use helix_planner::relational as r;
use std::collections::BTreeSet;

#[test]
fn projection_aliases_shadow_input_names_but_ordinary_scopes_keep_unprojected_inputs() {
    for (projection, suffix, expected_slot) in [
        ("b AS a", "ORDER BY a", 2),
        ("b AS a", "WHERE a > 0", 2),
        ("a AS x", "ORDER BY b", 1),
        ("a AS x", "WHERE b > 0", 1),
        ("DISTINCT a AS x", "ORDER BY a", 2),
        ("a + 1 AS x, count(*) AS n", "ORDER BY a + 1", 2),
    ] {
        let text = format!("WITH 1 AS a, 2 AS b WITH {projection} {suffix} RETURN *");
        let query = compile(&text).unwrap();
        let r::Operator::Project {
            ordering,
            predicate,
            ..
        } = &query.operators()[1]
        else {
            panic!("expected projection");
        };
        let expression = if ordering.is_empty() {
            predicate.as_ref().unwrap().expression()
        } else {
            &ordering[0].expression
        };
        assert_eq!(
            expression.slots(),
            BTreeSet::from([r::Slot(expected_slot)]),
            "{text}"
        );
    }
}

#[test]
fn restricted_scopes_and_errors_keep_their_existing_phase_detail_and_spans() {
    for (text, detail, variable) in [
        (
            "WITH 1 AS a, 2 AS b WITH DISTINCT a AS x ORDER BY b RETURN *",
            "UndefinedVariable",
            None,
        ),
        (
            "WITH 1 AS a WITH DISTINCT a AS x WHERE a > 0 RETURN *",
            "UndefinedVariable",
            Some("a"),
        ),
        (
            "WITH 1 AS a, 2 AS b WITH a AS x, count(*) AS n WHERE b > 0 RETURN *",
            "UndefinedVariable",
            Some("b"),
        ),
        ("WITH 1 AS a WITH a + 1 RETURN a", "NoExpressionAlias", None),
        (
            "WITH 1 AS a WITH a + 1 ORDER BY missing RETURN a",
            "UndefinedVariable",
            Some("missing"),
        ),
        ("WITH 1 AS a RETURN a SKIP a", "NonConstantExpression", None),
        (
            "RETURN 1 LIMIT missing",
            "UndefinedVariable",
            Some("missing"),
        ),
        ("WITH 1 AS a RETURN *, a AS a", "ColumnNameConflict", None),
    ] {
        let error = compile(text).unwrap_err();
        assert_eq!(error.phase, r::ErrorPhase::Compile, "{text}");
        assert_eq!(error.category, "SyntaxError", "{text}");
        assert_eq!(error.detail, detail, "{text}");
        match (variable, error.span) {
            (Some(name), Some(span)) => assert_eq!(&text[span.start..span.end], name, "{text}"),
            (None, None) => {}
            other => panic!("unexpected error span: {other:?}: {text}"),
        }
    }
}

#[test]
fn wildcard_order_and_unicode_name_identity_survive_borrowed_lookup() {
    let query = compile("WITH 2 AS z, 1 AS a RETURN *").unwrap();
    assert_eq!(
        query.returns(),
        &[("a".into(), r::Slot(1)), ("z".into(), r::Slot(0))]
    );
    let query =
        compile("WITH 1 AS `α`, 2 AS `🙂` WITH `🙂` AS `α` ORDER BY `α` RETURN `α`").unwrap();
    let r::Operator::Project {
        items, ordering, ..
    } = &query.operators()[1]
    else {
        panic!("expected projection");
    };
    assert_eq!(
        items.iter().next().unwrap().expression,
        r::Expression::Slot(r::Slot(1))
    );
    assert_eq!(ordering[0].expression, r::Expression::Slot(r::Slot(2)));
}
