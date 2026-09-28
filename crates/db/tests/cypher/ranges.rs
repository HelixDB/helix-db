use super::membership::create_index;
use super::{database, run};
use db::cypher;
use helix_ast::index;
use serde_json::json;

/// Each comparison runs before and after a range index exists over numbers,
/// strings and missing values; the unindexed label scan is the reference
/// result. Range indexes reject unorderable stored values such as booleans.
#[tokio::test]
async fn range_comparisons_read_range_indexes_with_cypher_semantics() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0, 199) AS i CREATE (:R {v: i, name: 'i' + toString(i)})",
    )
    .await;
    run(
        &db,
        "CREATE (:R {v: 2.5, name: 'fraction'}), (:R {v: 'm', name: 'm'}), \
         (:R {v: 'b', name: 'b'}), (:R {name: 'missing'}), \
         (:R {v: 9007199254740993, name: 'big-int'}), \
         (:R {v: 9007199254740994.0, name: 'big-float'}), (:Other {v: 500, name: 'other'})",
    )
    .await;
    let cases = [
        ("MATCH (n:R) WHERE n.v > 197 RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v >= 197.5 AND n.v < 199.5 RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v <= 2.5 RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE 3 > n.v RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v > 'a' RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v < 'c' RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v >= $low AND n.v < $high RETURN n.name AS v ORDER BY v", json!({"low": 150, "high": 152})),
        ("MATCH (n:R) WHERE n.v > 9007199254740992 RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v <= 9007199254740992.0 AND n.v > 198 RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:R) WHERE n.v > true RETURN n.name AS v", json!({})),
        ("MATCH (n:R) WHERE n.v > 5 AND n.v < 'z' RETURN n.name AS v", json!({})),
        ("MATCH (n:R) WITH n WHERE n.v < 1 RETURN n.name AS v", json!({})),
        ("MATCH (n:R) WHERE n.v > 196 AND n.name <> 'i199' RETURN n.name AS v ORDER BY v", json!({})),
        // Equalities read the closed range [v, v] with Cypher equality.
        ("MATCH (n:R) WHERE n.v = 150 RETURN n.name AS v", json!({})),
        ("MATCH (n:R {v: 2.5}) RETURN n.name AS v", json!({})),
        ("MATCH (n:R) WHERE n.v = $s RETURN n.name AS v", json!({"s": "m"})),
        ("MATCH (n:R) WHERE n.v = 9007199254740993 RETURN n.name AS v", json!({})),
    ];
    let request = |text: &str, parameters: &serde_json::Value| -> cypher::Request {
        serde_json::from_value(json!({"query": text, "parameters": parameters})).unwrap()
    };
    let mut reference = Vec::new();
    for (text, parameters) in &cases {
        reference.push(
            db.cypher(request(text, parameters))
                .await
                .unwrap_or_else(|e| panic!("{text}: {e:?}")),
        );
    }
    let failing = "MATCH (n:R) WHERE n.v > 5 AND n.v / $z = 1 RETURN n";
    let error = db
        .cypher(request(failing, &json!({"z": 0})))
        .await
        .unwrap_err()
        .to_string();
    let names = |values: &[&str]| {
        values
            .iter()
            .map(|value| vec![json!(value)])
            .collect::<Vec<_>>()
    };
    let rows = |response: &cypher::Response| response.rows.clone();
    assert_eq!(
        reference.iter().map(rows).collect::<Vec<_>>(),
        [
            names(&["big-float", "big-int", "i198", "i199"]),
            names(&["i198", "i199"]),
            names(&["fraction", "i0", "i1", "i2"]),
            names(&["fraction", "i0", "i1", "i2"]),
            names(&["b", "m"]),
            names(&["b"]),
            names(&["i150", "i151"]),
            names(&["big-float", "big-int"]),
            names(&["i199"]),
            names(&[]),
            names(&[]),
            names(&["i0"]),
            names(&["big-float", "big-int", "i197", "i198"]),
            names(&["i150"]),
            names(&["fraction"]),
            names(&["m"]),
            names(&["big-int"]),
        ]
    );
    assert!(error.contains("DivisionByZero"), "{error}");

    create_index(&db, index::IndexSpec::node_range("R", "v")).await;
    for ((text, parameters), expected) in cases.iter().zip(&reference) {
        let actual = db
            .cypher(request(text, parameters))
            .await
            .unwrap_or_else(|e| panic!("{text}: {e:?}"));
        assert_eq!(actual.rows, expected.rows, "{text}");
    }
    assert_eq!(
        db.cypher(request(failing, &json!({"z": 0})))
            .await
            .unwrap_err()
            .to_string(),
        error
    );
    // A range source streams, so a limit hydrates only the demanded nodes.
    let limited = run(
        &db,
        "MATCH (n:R) WHERE n.v >= 10 WITH n LIMIT 3 RETURN count(*) AS c",
    )
    .await;
    assert_eq!(limited.rows, vec![vec![json!(3)]]);
    assert!(
        limited.resources.reads.multi_get_keys <= 32,
        "{:?}",
        limited.resources.reads
    );
    // The label scan validates every R node; the range reads only its bounds.
    let indexed = run(&db, cases[0].0).await;
    assert!(
        indexed.resources.reads.multi_get_keys <= 16
            && reference[0].resources.reads.multi_get_keys >= 200,
        "{:?} vs {:?}",
        indexed.resources.reads,
        reference[0].resources.reads
    );
    db.close().await.unwrap();
}
