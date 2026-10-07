use super::*;
use crate::syntax;
use helix_planner::relational as r;

mod allocation;

#[test]
fn ordinary_payloads_borrow_exact_source_bytes() {
    for (source, expected) in [
        ("MATCH", Kind::Word("MATCH".into())),
        ("δ_name", Kind::Word("δ_name".into())),
        ("123e-4", Kind::Number("123e-4".into())),
        (".5", Kind::Number(".5".into())),
        ("0xFF", Kind::Number("0xFF".into())),
        ("$δ_name", Kind::Parameter("δ_name".into())),
        ("$`a b`", Kind::Parameter("a b".into())),
        ("`a b`", Kind::Escaped("a b".into())),
        (r"`a\b`", Kind::Escaped(r"a\b".into())),
        ("``", Kind::Escaped("".into())),
        ("'α🙂β'", Kind::String("α🙂β".into())),
        ("\"α\nβ\"", Kind::String("α\nβ".into())),
        ("'a\"b'", Kind::String("a\"b".into())),
        ("\"a'b\"", Kind::String("a'b".into())),
        ("''", Kind::String("".into())),
        ("\"\"", Kind::String("".into())),
    ] {
        let tokens = lex(source).unwrap();
        assert_eq!(tokens.len(), 2, "{source}");
        assert_eq!(tokens[0].kind, expected, "{source}");
        assert_eq!(
            tokens[0].span,
            Span {
                start: 0,
                end: source.len()
            }
        );
        let (Kind::Word(Cow::Borrowed(value))
        | Kind::Number(Cow::Borrowed(value))
        | Kind::Parameter(Cow::Borrowed(value))
        | Kind::Escaped(Cow::Borrowed(value))
        | Kind::String(Cow::Borrowed(value))) = &tokens[0].kind
        else {
            panic!("ordinary token allocated: {source}");
        };
        let offset = value.as_ptr() as usize - source.as_ptr() as usize;
        assert_eq!(source.get(offset..offset + value.len()), Some(*value));
        assert_eq!(tokens[1].kind, Kind::End);
        assert_eq!(
            tokens[1].span,
            Span {
                start: source.len(),
                end: source.len()
            }
        );
    }
}

#[test]
fn escape_decoding_preserves_segments_and_allocates_only_decoded_payloads() {
    for (source, expected) in [
        (r"'\n'", Kind::String("\n".into())),
        (r"'α\nβ\tγ'", Kind::String("α\nβ\tγ".into())),
        (
            r#"'a\nb\r\tc\b\f\\\'\"\u0041\U0001F642z'"#,
            Kind::String("a\nb\r\tc\u{8}\u{c}\\'\"A🙂z".into()),
        ),
        (r#""a\"b\\c""#, Kind::String("a\"b\\c".into())),
        ("`α``β``γ`", Kind::Escaped("α`β`γ".into())),
        ("````", Kind::Escaped("`".into())),
        ("$`a``b`", Kind::Parameter("a`b".into())),
        ("$````", Kind::Parameter("`".into())),
        (
            r"'\u0000\uFFFF\U0010FFFF'",
            Kind::String("\0\u{ffff}\u{10ffff}".into()),
        ),
    ] {
        let tokens = lex(source).unwrap();
        assert_eq!(tokens[0].kind, expected, "{source}");
        assert!(matches!(
            &tokens[0].kind,
            Kind::String(Cow::Owned(_))
                | Kind::Escaped(Cow::Owned(_))
                | Kind::Parameter(Cow::Owned(_))
        ));
        assert_eq!(
            tokens[0].span,
            Span {
                start: 0,
                end: source.len()
            }
        );
    }
}

#[test]
fn escaped_payload_capacity_does_not_exceed_character_at_a_time_construction() {
    for length in [
        0, 1, 7, 8, 9, 15, 16, 17, 511, 512, 513, 639, 640, 641, 1023, 1024, 1025, 4095, 4096,
    ] {
        for character in ["a", "é", "🙂"] {
            let prefix = character.repeat(length);
            for (source, expected) in [
                (format!("`{prefix}``tail`"), format!("{prefix}`tail")),
                (format!("`a``{prefix}``tail`"), format!("a`{prefix}`tail")),
                (
                    format!(r"'{prefix}\n{prefix}\u0041'"),
                    format!("{prefix}\n{prefix}A"),
                ),
                (format!(r"'\U0001F642{prefix}\t'"), format!("🙂{prefix}\t")),
            ] {
                // Independent original construction: append each decoded
                // character rather than computing the candidate's capacity.
                let mut reference = String::new();
                for character in expected.chars() {
                    reference.push(character);
                }
                let tokens = lex(&source).unwrap();
                let (Kind::String(Cow::Owned(actual)) | Kind::Escaped(Cow::Owned(actual))) =
                    &tokens[0].kind
                else {
                    panic!("decoded payload");
                };
                assert_eq!(actual, &expected);
                assert!(
                    actual.capacity() <= reference.capacity(),
                    "length={length}, character={character}, actual={}, reference={}",
                    actual.capacity(),
                    reference.capacity()
                );
            }
        }
    }
}

#[test]
fn token_boundaries_and_byte_spans_survive_comments_and_mixed_payloads() {
    let source = "MATCH/*gap*/(n:`α``β`) WHERE n.x>=.5 // tail\nRETURN $`a b`, 'x\\ny'";
    let expected = [
        ("MATCH", Kind::Word("MATCH".into())),
        ("(", Kind::Symbol("(")),
        ("n", Kind::Word("n".into())),
        (":", Kind::Symbol(":")),
        ("`α``β`", Kind::Escaped("α`β".into())),
        (")", Kind::Symbol(")")),
        ("WHERE", Kind::Word("WHERE".into())),
        ("n", Kind::Word("n".into())),
        (".", Kind::Symbol(".")),
        ("x", Kind::Word("x".into())),
        (">=", Kind::Symbol(">=")),
        (".5", Kind::Number(".5".into())),
        ("RETURN", Kind::Word("RETURN".into())),
        ("$`a b`", Kind::Parameter("a b".into())),
        (",", Kind::Symbol(",")),
        ("'x\\ny'", Kind::String("x\ny".into())),
    ];
    let tokens = lex(source).unwrap();
    assert_eq!(tokens.len(), expected.len() + 1);
    let mut offset = 0;
    for (token, (spelling, expected)) in tokens.iter().zip(expected) {
        let start = offset + source[offset..].find(spelling).unwrap();
        assert_eq!(token.kind, expected);
        assert_eq!(
            token.span,
            Span {
                start,
                end: start + spelling.len()
            }
        );
        offset = start + spelling.len();
    }
}

#[test]
fn malformed_quotes_and_unicode_keep_exact_error_details_and_spans() {
    for (source, detail, end, message) in [
        ("$", "UnexpectedSyntax", 1, "expected parameter name"),
        ("$``", "UnexpectedSyntax", 3, "expected parameter name"),
        ("'abc", "UnexpectedSyntax", 4, "unterminated quoted value"),
        ("'abc\\", "UnexpectedSyntax", 5, "unterminated quoted value"),
        ("`abc``", "UnexpectedSyntax", 6, "unterminated quoted value"),
        (r"'\q'", "UnexpectedSyntax", 3, "invalid string escape"),
        (
            r"'\u12'",
            "InvalidUnicodeLiteral",
            3,
            "incomplete Unicode escape",
        ),
        (
            r"'\uD800'",
            "InvalidUnicodeLiteral",
            7,
            "invalid Unicode scalar",
        ),
        (
            r"'\U00110000'",
            "InvalidUnicodeLiteral",
            11,
            "invalid Unicode scalar",
        ),
        (
            r"'\uZZZZ'",
            "InvalidUnicodeLiteral",
            7,
            "invalid Unicode scalar",
        ),
        ("/*abc", "UnexpectedSyntax", 5, "unterminated comment"),
        ("🙂", "InvalidUnicodeCharacter", 4, "unexpected character"),
    ] {
        let error = lex(source).unwrap_err();
        assert_eq!(error.category, "SyntaxError", "{source}");
        assert_eq!(error.detail, detail, "{source}");
        assert_eq!(error.message, message, "{source}");
        assert_eq!(error.span, Some(Span { start: 0, end }), "{source}");
    }
}

#[test]
fn wide_ordinary_tokens_allocate_only_the_token_vector() {
    let control = "+ ".repeat(4096);
    let (tokens, vector_allocations) = allocation::observe(|| lex(&control).unwrap());
    assert_eq!(tokens.len(), 4097);
    assert!(vector_allocations.0 < 32);
    for atom in [
        "identifier".repeat(16),
        "123456789".to_owned(),
        format!("${}", "parameter".repeat(16)),
        format!("$`{}`", "a b".repeat(16)),
        format!("`{}`", "αβ".repeat(16)),
        format!("'{}'", "plain🙂".repeat(16)),
        format!("\"{}\"", "plain🙂".repeat(16)),
    ] {
        let source = format!("{atom} ").repeat(4096);
        let (tokens, allocated) = allocation::observe(|| lex(&source).unwrap());
        assert_eq!(tokens.len(), 4097);
        assert_eq!(allocated, vector_allocations, "{atom}");
    }
}

#[test]
fn owned_public_syntax_outlives_source_and_preserves_escaped_names() {
    let statement = {
        let source = String::from(
            "MATCH (`n``x`:`L``b` {`p``q`:'α\\nβ'}) RETURN $`p``x` AS `a``b`, (`n``x`)",
        );
        crate::parse(&source).unwrap()
    };
    let syntax::Clause::Match { patterns, .. } = &statement.clauses[0] else {
        panic!("match");
    };
    let node = &patterns[0].nodes[0];
    assert_eq!(node.name.as_deref(), Some("n`x"));
    assert_eq!(node.labels, ["L`b"]);
    assert_eq!(node.properties[0].0, "p`q");
    assert_eq!(
        node.properties[0].1.kind,
        syntax::ExprKind::Literal(r::Value::String("α\nβ".into()))
    );
    let syntax::Clause::Project { items, .. } = &statement.clauses[1] else {
        panic!("project");
    };
    let syntax::Item::Expression {
        expression, alias, ..
    } = &items[0]
    else {
        panic!("parameter");
    };
    assert_eq!(expression.kind, syntax::ExprKind::Parameter("p`x".into()));
    assert_eq!(alias.as_deref(), Some("a`b"));
    let syntax::Item::Expression { expression, .. } = &items[1] else {
        panic!("variable");
    };
    assert_eq!(expression.kind, syntax::ExprKind::Variable("n`x".into()));
}

#[test]
fn pattern_probes_preserve_decoded_maps_for_scalar_backtracking() {
    for nesting in [1, 2, 8] {
        let source = format!(
            "RETURN {}{{`a``b`:'\\u0041\\n', nested:['\\t']}}{}",
            "(".repeat(nesting),
            ")".repeat(nesting)
        );
        let statement = crate::parse(&source).unwrap();
        let syntax::Clause::Project { items, .. } = &statement.clauses[0] else {
            panic!("project");
        };
        let syntax::Item::Expression { expression, .. } = &items[0] else {
            panic!("expression");
        };
        let syntax::ExprKind::Map(properties) = &expression.kind else {
            panic!("map");
        };
        assert_eq!(properties[0].0, "a`b");
        assert_eq!(
            properties[0].1.kind,
            syntax::ExprKind::Literal(r::Value::String("A\n".into()))
        );
        assert_eq!(properties[1].0, "nested");
        let syntax::ExprKind::List(values) = &properties[1].1.kind else {
            panic!("list");
        };
        assert_eq!(
            values[0].kind,
            syntax::ExprKind::Literal(r::Value::String("\t".into()))
        );
        assert_eq!(
            &source[expression.span.start..expression.span.end],
            &source["RETURN ".len()..]
        );
    }
}
