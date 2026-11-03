use crate::syntax::{Clause, Expr, ExprKind, Item};
use helix_planner::{ir::AtLeast, relational as r};
use proptest::prelude::*;

/// The same tree with every span cleared, so trees parsed from different text
/// compare by structure alone.
fn strip(expression: &Expr) -> Expr {
    let boxed = |value: &Expr| Box::new(strip(value));
    let kind = match expression.kind() {
        ExprKind::Literal(value) => ExprKind::Literal(value.clone()),
        ExprKind::Variable(name) => ExprKind::Variable(name.clone()),
        ExprKind::Parameter(name) => ExprKind::Parameter(name.clone()),
        ExprKind::PatternPredicate(names) => ExprKind::PatternPredicate(names.clone()),
        ExprKind::Property(value, name) => ExprKind::Property(boxed(value), name.clone()),
        ExprKind::Index(value, index) => ExprKind::Index(boxed(value), boxed(index)),
        ExprKind::Slice { value, start, end } => ExprKind::Slice {
            value: boxed(value),
            start: start.as_deref().map(boxed),
            end: end.as_deref().map(boxed),
        },
        ExprKind::Unary(operator, value) => ExprKind::Unary(*operator, boxed(value)),
        ExprKind::Binary(operator, left, right) => {
            ExprKind::Binary(*operator, boxed(left), boxed(right))
        }
        ExprKind::Connective(connective, operands) => ExprKind::Connective(
            *connective,
            AtLeast::try_from_vec(operands.iter().map(strip).collect()).expect("two operands"),
        ),
        ExprKind::Call {
            name,
            arguments,
            distinct,
            star,
        } => ExprKind::Call {
            name: name.clone(),
            arguments: arguments.iter().map(strip).collect(),
            distinct: *distinct,
            star: *star,
        },
        ExprKind::List(values) => ExprKind::List(values.iter().map(strip).collect()),
        ExprKind::Map(entries) => ExprKind::Map(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), strip(value)))
                .collect(),
        ),
        ExprKind::Case {
            operand,
            branches,
            otherwise,
        } => ExprKind::Case {
            operand: operand.as_deref().map(boxed),
            branches: branches
                .iter()
                .map(|(when, then)| (strip(when), strip(then)))
                .collect(),
            otherwise: otherwise.as_deref().map(boxed),
        },
        ExprKind::HasLabel(value, label) => ExprKind::HasLabel(boxed(value), label.clone()),
    };
    Expr::new(kind, r::Span { start: 0, end: 0 }).expect("depth is unchanged")
}

fn returned(text: &str) -> Expr {
    let statement = crate::parse(text).unwrap_or_else(|error| panic!("{text}: {error:?}"));
    let [Clause::Project { items, .. }] = statement.clauses.as_slice() else {
        panic!("{text} is one projection");
    };
    let [Item::Expression { expression, .. }] = items.as_slice() else {
        panic!("{text} projects one expression");
    };
    strip(expression)
}

fn node(kind: ExprKind) -> Expr {
    Expr::new(kind, r::Span { start: 0, end: 0 }).expect("shallow")
}

fn name() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z][a-z0-9_]{0,3}",
        Just("end".to_owned()),
        Just("NULL".to_owned()),
        Just("Order".to_owned()),
        Just("1st".to_owned()),
        Just("a b`c".to_owned()),
        Just("ünï".to_owned()),
    ]
}

fn leaf() -> impl Strategy<Value = Expr> {
    prop_oneof![
        any::<i64>().prop_map(|value| node(ExprKind::Literal(r::Value::Integer(value)))),
        any::<f64>()
            .prop_filter("literals are finite", |value| value.is_finite())
            .prop_map(|value| node(ExprKind::Literal(r::Value::Float(value)))),
        any::<String>().prop_map(|value| node(ExprKind::Literal(r::Value::String(value)))),
        any::<bool>().prop_map(|value| node(ExprKind::Literal(r::Value::Boolean(value)))),
        Just(node(ExprKind::Literal(r::Value::Null))),
        name().prop_map(|value| node(ExprKind::Variable(value))),
        prop_oneof!["[a-z0-9_]{1,3}", Just("a b".to_owned())]
            .prop_map(|value| node(ExprKind::Parameter(value))),
    ]
}

fn expression() -> impl Strategy<Value = Expr> {
    use r::Binary as B;
    let binary = prop_oneof![
        Just(B::Add),
        Just(B::Subtract),
        Just(B::Multiply),
        Just(B::Divide),
        Just(B::Modulo),
        Just(B::Power),
        Just(B::Equal),
        Just(B::NotEqual),
        Just(B::Less),
        Just(B::LessEqual),
        Just(B::Greater),
        Just(B::GreaterEqual),
        Just(B::In),
        Just(B::StartsWith),
        Just(B::EndsWith),
        Just(B::Contains),
    ];
    let unary = prop_oneof![
        Just(r::Unary::Not),
        Just(r::Unary::Negate),
        Just(r::Unary::Positive),
        Just(r::Unary::IsNull),
        Just(r::Unary::IsNotNull),
    ];
    let connective = prop_oneof![
        Just(r::Connective::And),
        Just(r::Connective::Or),
        Just(r::Connective::Xor),
    ];
    leaf().prop_recursive(4, 24, 3, move |inner| {
        prop_oneof![
            (binary.clone(), inner.clone(), inner.clone()).prop_map(|(operator, left, right)| {
                node(ExprKind::Binary(operator, Box::new(left), Box::new(right)))
            }),
            (unary.clone(), inner.clone())
                .prop_map(|(operator, value)| node(ExprKind::Unary(operator, Box::new(value)))),
            (
                connective.clone(),
                prop::collection::vec(inner.clone(), 2..4)
            )
                .prop_map(|(connective, operands)| node(ExprKind::Connective(
                    connective,
                    AtLeast::try_from_vec(operands).expect("two operands"),
                ))),
            (inner.clone(), name())
                .prop_map(|(value, key)| node(ExprKind::Property(Box::new(value), key))),
            (inner.clone(), name())
                .prop_map(|(value, label)| node(ExprKind::HasLabel(Box::new(value), label))),
            (inner.clone(), inner.clone())
                .prop_map(|(value, index)| node(ExprKind::Index(Box::new(value), Box::new(index)))),
            (
                inner.clone(),
                prop::option::of(inner.clone()),
                prop::option::of(inner.clone())
            )
                .prop_map(|(value, start, end)| node(ExprKind::Slice {
                    value: Box::new(value),
                    start: start.map(Box::new),
                    end: end.map(Box::new),
                })),
            (
                name(),
                prop::collection::vec(inner.clone(), 0..3),
                any::<bool>()
            )
                .prop_map(|(name, arguments, distinct)| node(ExprKind::Call {
                    name: format!("f{name}"),
                    arguments,
                    distinct,
                    star: false,
                })),
            prop::collection::vec(inner.clone(), 0..3)
                .prop_map(|values| node(ExprKind::List(values))),
            prop::collection::vec((name(), inner.clone()), 0..3)
                .prop_map(|entries| node(ExprKind::Map(entries))),
            (
                prop::option::of(inner.clone()),
                prop::collection::vec((inner.clone(), inner.clone()), 1..3),
                prop::option::of(inner.clone())
            )
                .prop_map(|(operand, branches, otherwise)| node(ExprKind::Case {
                    operand: operand.map(Box::new),
                    branches,
                    otherwise: otherwise.map(Box::new),
                })),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4096))]

    /// Printing then parsing rebuilds the same tree, so precedence, signs,
    /// quoting and escapes are all preserved.
    #[test]
    fn printed_expressions_parse_back_to_the_same_tree(value in expression()) {
        let text = format!("RETURN {value}");
        prop_assert_eq!(returned(&text), value, "{}", text);
    }
}

#[test]
fn statements_print_canonically_and_parse_back_identically() {
    let corpus = [
        "MATCH (n) RETURN n",
        "match (p:Person {name: 'Ada', age: 3}) where p.age >= 21 and p.vip return p.name as name order by name desc skip 1 limit 2",
        "OPTIONAL MATCH (a)-[r:KNOWS|LIKES {since: 2020}]->(b)<-[:X]-(c)--(d) RETURN *",
        "MATCH p = (a)-->(b) RETURN p",
        "MATCH (a) WITH DISTINCT a.x AS x ORDER BY x LIMIT 3 WHERE x > 2 RETURN x",
        "UNWIND [1, 2.5, -3, 'it\\'s', null, true] AS `the value` RETURN `the value`",
        "CREATE (a:A {k: $1})-[:T]->(b:B {})",
        "MATCH (n:N) SET n.a = 1, n = {b: 2}, n += {c: 3} REMOVE n.d DETACH DELETE n",
        "MATCH (n) DELETE n",
        "RETURN count(*), count(DISTINCT n.x), CASE WHEN 1 = 2 THEN 3 ELSE 4 END, CASE 1 WHEN 2 THEN 3 END",
        "RETURN (1 + 2) * 3, 1 - (2 - 3), 2 ^ 3 ^ 4, -(-1), NOT (a AND b), (a < b) < c, a < b < c",
        "RETURN n:Label, (n.x)[0], n.x[1..2], n.x[..2], n.x[1..], $p.k, $`a b`",
        "RETURN 'a\\nb\\\\c\\'d', 1e300, 0.1, -0.0",
        "MATCH (`MATCH`:`Weird Label` {`end`: 1}) RETURN `MATCH`.`end` AS `RETURN`",
    ];
    for text in corpus {
        let printed = crate::parse(text)
            .unwrap_or_else(|error| panic!("{text}: {error:?}"))
            .to_string();
        let reprinted = crate::parse(&printed)
            .unwrap_or_else(|error| panic!("{printed}: {error:?}"))
            .to_string();
        assert_eq!(printed, reprinted, "{text}");
    }
}

#[test]
fn printing_is_canonical() {
    for (text, expected) in [
        (
            "match (p:Person) where (p.age > 1) return p.name",
            "MATCH (p:Person) WHERE p.age > 1 RETURN p.name",
        ),
        ("RETURN ((1 + 2)) * 3", "RETURN (1 + 2) * 3"),
        ("RETURN 1 + (2 * 3)", "RETURN 1 + 2 * 3"),
        ("RETURN (a AND b) AND c", "RETURN (a AND b) AND c"),
        ("RETURN a AND (b OR c)", "RETURN a AND (b OR c)"),
        ("RETURN NOT a = b", "RETURN NOT a = b"),
        ("RETURN (NOT a) = b", "RETURN (NOT a) = b"),
        ("RETURN a != b", "RETURN a <> b"),
        ("RETURN -(1)", "RETURN -(1)"),
        ("RETURN - 1", "RETURN -1"),
        ("RETURN 1 - -1", "RETURN 1 - -1"),
        ("RETURN x STARTS  WITH 'a'", "RETURN x STARTS WITH 'a'"),
        (
            "RETURN (a + b) IS NULL, a IN (b IS NULL)",
            "RETURN a + b IS NULL, a IN (b IS NULL)",
        ),
        ("MATCH (a)<-->(b) RETURN 1", "MATCH (a)--(b) RETURN 1"),
        ("RETURN `x`, `a``b`, `null`", "RETURN x, `a``b`, `null`"),
    ] {
        assert_eq!(crate::parse(text).unwrap().to_string(), expected, "{text}");
    }
}

#[test]
fn values_without_literal_syntax_print_as_equivalent_expressions() {
    for (value, expected) in [
        (f64::NAN, "(0.0 / 0.0)"),
        (f64::INFINITY, "(1.0 / 0.0)"),
        (f64::NEG_INFINITY, "(-1.0 / 0.0)"),
    ] {
        assert_eq!(
            node(ExprKind::Literal(r::Value::Float(value))).to_string(),
            expected
        );
    }
    let nested = node(ExprKind::Literal(r::Value::List(vec![
        r::Value::Integer(1),
        r::Value::Map([("k".to_owned(), r::Value::Null)].into()),
    ])));
    assert_eq!(nested.to_string(), "[1, {k: NULL}]");
    assert_eq!(
        node(ExprKind::PatternPredicate(vec!["a".to_owned()])).to_string(),
        "(a)--()"
    );
    assert_eq!(
        node(ExprKind::PatternPredicate(Vec::new())).to_string(),
        "()--()"
    );
}
