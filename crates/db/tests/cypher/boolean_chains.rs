use super::{database, run};
use db::cypher;
use serde_json::json;

/// A Boolean chain is one flat node, and it evaluates its operands as the
/// same chain written left-nested does, including which operand's error it
/// reports.
#[tokio::test]
async fn flat_boolean_chains_evaluate_as_left_nested_chains() {
    let db = database().await;
    run(&db, "UNWIND range(0, 199) AS i CREATE (:User {uid: i})").await;
    let chain = (0..100)
        .map(|value| format!("n.uid = {value}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    assert_eq!(
        run(
            &db,
            &format!("MATCH (n:User) WHERE {chain} RETURN count(*)")
        )
        .await
        .rows,
        vec![vec![json!(100)]]
    );
    let operands = ["true", "false", "null", "(1 / $z = 1)"];
    let request = |text: &str| -> cypher::Request {
        serde_json::from_value(json!({"query": text, "parameters": {"z": 0}})).unwrap()
    };
    for operator in ["AND", "OR", "XOR"] {
        for index in 0..operands.len().pow(4) {
            let terms = (0..4)
                .map(|position| operands[index / operands.len().pow(position) % operands.len()])
                .collect::<Vec<_>>();
            let flat = terms.join(&format!(" {operator} "));
            let nested = terms[1..].iter().fold(terms[0].to_owned(), |left, term| {
                format!("({left} {operator} {term})")
            });
            let result = |text: String| {
                let db = &db;
                async move {
                    db.cypher(request(&format!("RETURN {text} AS v")))
                        .await
                        .map(|response| response.rows)
                        .map_err(|error| error.to_string())
                }
            };
            assert_eq!(result(flat.clone()).await, result(nested).await, "{flat}");
        }
    }
    db.close().await.unwrap();
}

/// A chain's length adds no nesting depth, so a chain of any length compiles
/// inside NOT or another connective and matches the equivalent IN list, and
/// a chain of ten thousand terms plans and evaluates on the default stack.
#[tokio::test]
async fn boolean_chains_of_any_length_nest_and_evaluate() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 199) AS i CREATE (:User {uid: i, active: i % 2 = 0})",
    )
    .await;
    let chain = |terms: usize| {
        (0..terms)
            .map(|value| format!("n.uid = {value}"))
            .collect::<Vec<_>>()
            .join(" OR ")
    };
    let list = |terms: usize| {
        (0..terms)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let count = |condition: String| {
        let db = &db;
        async move {
            run(
                db,
                &format!("MATCH (n:User) WHERE {condition} RETURN count(*)"),
            )
            .await
            .rows
        }
    };
    for terms in 44..=50 {
        for (chained, listed) in [
            (
                format!("n.active = true AND ({})", chain(terms)),
                format!("n.active = true AND n.uid IN [{}]", list(terms)),
            ),
            (
                format!("NOT ({})", chain(terms)),
                format!("NOT n.uid IN [{}]", list(terms)),
            ),
        ] {
            assert_eq!(
                count(chained.clone()).await,
                count(listed).await,
                "{chained}"
            );
        }
    }
    assert_eq!(count(chain(10_000)).await, vec![vec![json!(200)]]);
    db.close().await.unwrap();
}
