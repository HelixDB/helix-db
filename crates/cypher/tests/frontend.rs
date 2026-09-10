use helix_planner::relational::{self as r, GraphValues};
use std::collections::BTreeMap;

struct EmptyGraph;
impl GraphValues for EmptyGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar test performed graph access")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar test performed graph access")
    }
}

fn scalar(text: &str) -> r::Value {
    let query = helix_cypher::compile(text).unwrap();
    let r::Operator::Project { items, .. } = &query.operators()[0] else {
        panic!("expected projection")
    };
    r::Evaluation {
        row: &[],
        parameters: &BTreeMap::new(),
        graph: &EmptyGraph,
        group: None,
        max_value_bytes: usize::MAX,
        max_collection_items: 1000,
    }
    .eval(&items[0].expression)
    .unwrap()
}

#[test]
fn scalar_precedence_and_null_truth_tables() {
    use r::Value as V;
    for (text, expected) in [
        ("RETURN 1 + 2 * 3", V::Integer(7)),
        ("RETURN (1 + 2) * 3", V::Integer(9)),
        ("RETURN 1<-2", V::Boolean(false)),
        ("RETURN -3<-2", V::Boolean(true)),
        ("RETURN (1)<-(2)", V::Boolean(false)),
        ("RETURN (1)--(2)", V::Integer(3)),
        ("RETURN NOT 1 = 2", V::Boolean(true)),
        ("RETURN null AND false", V::Boolean(false)),
        ("RETURN null OR true", V::Boolean(true)),
        ("RETURN null AND true", V::Null),
        ("RETURN null = null", V::Null),
        ("RETURN null IN []", V::Boolean(false)),
        ("RETURN 1 IN [null, 1]", V::Boolean(true)),
        ("RETURN 2 IN [null, 1]", V::Null),
        ("RETURN CASE WHEN null THEN 1 ELSE 2 END", V::Integer(2)),
        (
            "RETURN CASE 2 WHEN 1 THEN 4 WHEN 2 THEN 5 END",
            V::Integer(5),
        ),
        ("RETURN [1,2,3][-1]", V::Integer(3)),
        ("RETURN {a: 4}.a", V::Integer(4)),
        ("RETURN coalesce(null, 4)", V::Integer(4)),
        ("RETURN -9223372036854775808", V::Integer(i64::MIN)),
    ] {
        assert_eq!(scalar(text), expected, "{text}");
    }
}

#[test]
fn profile_examples_resolve() {
    for text in [
        "MATCH (a:Person)-[r:KNOWS]->(b) WHERE a.age > $age RETURN a.name, b, r",
        "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) WHERE m.name = 'Ada' RETURN n, m",
        "UNWIND [1,2,3] AS x WITH x + 1 AS y WHERE y > 2 RETURN sum(y) AS total",
        "CREATE (a:Person {name: 'Ada'}), (b:Person {name: 'Grace'}), (a)-[:KNOWS]->(b) RETURN a,b",
        "MATCH (n:Person) SET n.age = 30, n += {active: true} REMOVE n.old RETURN n",
        "MATCH (n:Person) DETACH DELETE n",
        "MATCH p=(a)-[r]->(b) RETURN nodes(p), relationships(p)",
        "MATCH (n) WITH n.name AS name, count(*) AS total RETURN name, total ORDER BY total DESC LIMIT 10",
    ] {helix_cypher::compile(text).unwrap_or_else(|e|panic!("{text}: {e}"));}
}

#[test]
fn rejects_profile_boundaries_and_scope_errors() {
    for (text, category, detail) in [
        ("CREATE ()", "UnsupportedFeature", "NodeLabelRequired"),
        (
            "MATCH (n:A:B) RETURN n",
            "UnsupportedFeature",
            "MultipleNodeLabels",
        ),
        ("MERGE (n:A)", "UnsupportedFeature", "Merge"),
        (
            "MATCH (n)-[*]->(m) RETURN m",
            "UnsupportedFeature",
            "VariableLengthPattern",
        ),
        (
            "MATCH (n) WITH n.name AS name RETURN n",
            "SyntaxError",
            "UndefinedVariable",
        ),
        (
            "MATCH (n) RETURN n.`$label`",
            "UnsupportedFeature",
            "ReservedPropertyName",
        ),
        ("RETURN 1 AS x, 2 AS x", "SyntaxError", "ColumnNameConflict"),
        ("WITH 1 + 1 RETURN 2", "SyntaxError", "NoExpressionAlias"),
        (
            "RETURN 1; RETURN 2",
            "UnsupportedFeature",
            "MultipleStatements",
        ),
    ] {
        let e = helix_cypher::compile(text).unwrap_err();
        assert_eq!(
            (&*e.category, &*e.detail),
            (category, detail),
            "{text}: {e}"
        );
    }
}

#[test]
fn lexical_boundaries_are_not_reinterpreted() {
    assert_eq!(
        scalar("RETURN '\\u0041\\n'"),
        r::Value::String("A\n".into())
    );
    assert_eq!(scalar("RETURN .5 + 0x10"), r::Value::Float(16.5));
    assert_eq!(
        scalar("RETURN [0,1,2,3][1..3]"),
        r::Value::List(vec![r::Value::Integer(1), r::Value::Integer(2)])
    );
    assert_eq!(
        scalar("/* comment */ RETURN 1 // comment"),
        r::Value::Integer(1)
    );
    for text in [
        "RETURN '\\uD800'",
        "RETURN 'unterminated",
        "RETURN /* unterminated",
        "RETURN 9223372036854775808",
    ] {
        assert!(helix_cypher::compile(text).is_err(), "{text}");
    }
}

proptest::proptest! {
    #[test]
    fn arbitrary_short_input_never_panics(input in ".{0,256}") {let _=helix_cypher::compile(&input);}
}

#[test]
fn escaped_identifiers_strings_and_function_boundaries() {
    assert_eq!(
        scalar("RETURN '\\r\\b\\f\\U00000041'"),
        r::Value::String("\r\u{8}\u{c}A".into())
    );
    assert!(helix_cypher::compile("WITH 1 AS `a``b` RETURN `a``b`").is_ok());
    for query in [
        "WITH 1 AS n UNWIND [2] AS n RETURN n",
        "RETURN 1 MATCH (n)",
        "WITH 1 AS n RETURN n,*",
        "MATCH (n) SET missing = {}",
        "MATCH p=(n) SET p += {}",
        "MATCH (n) SET {a:n}.a.x = 1",
        "MATCH (n) REMOVE n",
        "MATCH (n) REMOVE n.name.x",
        "CREATE (n:``)",
        "CREATE (a:N)-[:``]->(b:N)",
        "CREATE (a:N)-[:A|B]->(b:N)",
        "CREATE (a:N {x:1,x:2})",
        "RETURN count()",
        "RETURN count(DISTINCT *)",
        "RETURN sum(*)",
        "RETURN id(*)",
        "RETURN size(DISTINCT 1)",
        "RETURN last()",
        "RETURN id(1,2)",
        "RETURN CASE 1 END",
        "RETURN [] []",
        "OPTIONAL RETURN 1",
        "SET 1 = 2",
        "RETURN '\\",
        "MATCH shortestPath((a)-->(b)) RETURN a",
        "RETURN allShortestPaths(1)",
    ] {
        assert!(helix_cypher::compile(query).is_err(), "{query}");
    }
    for query in [
        "RETURN last([1,2]),exists({a:1}.a),trim(' a '),ltrim(' a '),rtrim(' a '),toLower('A'),toUpper('a')",
        "MATCH (n:N)-[r:R {x:1}]->(m:N) SET n = {x:1} SET m += {x:2} REMOVE r.x RETURN n,m,r",
        "MATCH p=(a:N)-[:R]->(b:N) WITH p AS q RETURN q",
        "WITH null AS n MATCH p=(n)-[:R]->() RETURN p",
    ] {helix_cypher::compile(query).unwrap_or_else(|e|panic!("{query}: {e}"));}
}

#[test]
fn public_syntax_cannot_bypass_pattern_validation() {
    let mut statement = helix_cypher::parse("MATCH (a)-[r]->(b) RETURN a").unwrap();
    let helix_cypher::syntax::Clause::Match { patterns, .. } = &mut statement.clauses[0] else {
        panic!("match clause");
    };
    patterns[0].nodes.pop();
    assert_eq!(
        helix_cypher::resolve(&statement).unwrap_err().detail,
        "InvalidRelationshipPattern"
    );
    let parsed = helix_cypher::parse("RETURN 1").unwrap();
    let helix_cypher::syntax::Clause::Project { items, .. } = &parsed.clauses[0] else {
        panic!("project");
    };
    let helix_cypher::syntax::Item::Expression { expression, .. } = &items[0] else {
        panic!("expression");
    };
    assert!(matches!(
        expression.kind(),
        helix_cypher::syntax::ExprKind::Literal(_)
    ));
    statement.clauses.clear();
    assert_eq!(
        helix_cypher::resolve(&statement).unwrap_err().detail,
        "InvalidStatement"
    );
}
