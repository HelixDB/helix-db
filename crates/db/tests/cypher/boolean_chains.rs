use super::{database, run};
use db::cypher;
use serde_json::json;

/// A long flat Boolean chain stays within the expression depth limit, and a
/// chain evaluates its operands as the same chain written left-nested does,
/// including which operand's error it reports.
#[tokio::test]
async fn boolean_chains_are_balanced_without_changing_evaluation() {
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
