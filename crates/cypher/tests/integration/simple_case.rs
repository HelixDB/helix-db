use crate::allocations;
use helix_planner::relational as r;
use std::collections::BTreeMap;

struct NoGraph;
impl r::GraphValues for NoGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar CASE must not request graph properties")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar CASE must not request graph metadata")
    }
}

#[test]
fn simple_case_preserves_equality_and_branch_laziness() {
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: usize::MAX,
        max_value_bytes: usize::MAX,
    };
    for (text, expected) in [
        (
            "RETURN CASE 1 WHEN 1 THEN 7 ELSE 'unused' END AS value",
            r::Value::Integer(7),
        ),
        (
            "RETURN CASE 1 WHEN 1 THEN null ELSE 'unused' END AS value",
            r::Value::Null,
        ),
        (
            "RETURN CASE 1 WHEN 1 THEN 'first' WHEN 1 THEN 'second' END AS value",
            r::Value::String("first".into()),
        ),
        (
            "RETURN CASE 3 WHEN 1 THEN 'one' END AS value",
            r::Value::Null,
        ),
        (
            "RETURN CASE null WHEN null THEN 'match' ELSE 'else' END AS value",
            r::Value::String("else".into()),
        ),
        (
            "RETURN CASE 1 WHEN 1.0 THEN 'match' ELSE 'else' END AS value",
            r::Value::String("match".into()),
        ),
        (
            "RETURN CASE '1' WHEN 1 THEN 'match' ELSE 'else' END AS value",
            r::Value::String("else".into()),
        ),
        (
            "RETURN CASE [1,null] WHEN [1,null] THEN 'match' ELSE 'else' END AS value",
            r::Value::String("else".into()),
        ),
        (
            "RETURN CASE [1,null] WHEN [2,null] THEN 'match' ELSE 'else' END AS value",
            r::Value::String("else".into()),
        ),
        (
            "RETURN CASE {a:1,b:[2]} WHEN {b:[2],a:1} THEN 'match' ELSE 'else' END AS value",
            r::Value::String("match".into()),
        ),
        (
            "RETURN CASE 1 WHEN 0 THEN 1/0 WHEN 1 THEN 7 ELSE 1/0 END AS value",
            r::Value::Integer(7),
        ),
        (
            "RETURN CASE 1 WHEN 1 THEN 7 WHEN 1/0 THEN 9 ELSE 1/0 END AS value",
            r::Value::Integer(7),
        ),
        (
            "RETURN CASE CASE 1 WHEN 1 THEN 2 END WHEN 2 THEN 3 END AS value",
            r::Value::Integer(3),
        ),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let [r::Operator::Project { items, .. }] = query.operators() else {
            panic!("one scalar projection: {text}")
        };
        let value = evaluation.eval(&items[0].expression).unwrap();
        assert_eq!(value, expected, "{text}");
    }
    for text in [
        "RETURN CASE 1/0 WHEN 1 THEN 7 ELSE 9 END AS value",
        "RETURN CASE 2 WHEN 1 THEN 7 WHEN 1/0 THEN 9 ELSE 3 END AS value",
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let [r::Operator::Project { items, .. }] = query.operators() else {
            panic!("one projection")
        };
        let error = evaluation.eval(&items[0].expression).unwrap_err();
        assert_eq!(error.category, "ArithmeticError");
        assert_eq!(error.detail, "DivisionByZero");
        assert_eq!(error.phase, r::ErrorPhase::Runtime);
    }
}

#[test]
fn simple_case_resolution_owns_one_operand_regardless_of_branch_count() {
    for bytes in [1024, 65536, 1048576] {
        for branches in [1, 4, 16, 64] {
            let payload = "x".repeat(bytes);
            let arms = (0..branches)
                .map(|value| format!(" WHEN {value} THEN {value}"))
                .collect::<String>();
            let text = format!("RETURN CASE size('{payload}') {arms} ELSE 0 END AS value");
            let syntax = helix_cypher::parse(&text).unwrap();
            let (query, counts) = allocations::observe(|| helix_cypher::resolve(&syntax));
            let query = query.unwrap();
            let mut literals = 0;
            for operator in query.operators() {
                for expression in operator.expressions() {
                    expression.visit(&mut |node| {
                        let r::Expression::Literal(r::Value::String(value)) = node else {
                            return;
                        };
                        assert_eq!(value, &payload);
                        literals += 1;
                    });
                }
            }
            assert_eq!(literals, 1);
            // One literal copy from syntax, plus bounded scalar-plan metadata.
            assert!(
                counts.bytes <= bytes + 64 * 1024,
                "{bytes}, {branches}: {counts:?}"
            );
        }
    }
}
