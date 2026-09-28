use super::{database, run};
use db::cypher;
use helix_ast::{batch, expr, index, query, traversal, value};
use serde_json::json;

#[test]
fn wide_native_and_cypher_membership_match_an_independent_graph_model() {
    // Match the native index lifecycle suite's stack allowance. Keep the async
    // contract separate from runtime/stack setup so its cases stay readable.
    std::thread::Builder::new()
        .name("cypher-membership-contract".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(verify_membership());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn verify_membership() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0,15) AS i CREATE (:Membership {key:i}),(:Other {key:i})",
    )
    .await;
    // Execute before and after index activation, checking the same independent
    // model against both frontend and access choices.
    for indexed in [false, true] {
        if indexed {
            let create = batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(index::IndexSpec::node_equality(
                        "Membership",
                        "key",
                    )),
                )
                .returning(["operation"]);
            let receipt = db.query(query::QueryRequest::write(create)).await.unwrap();
            let operation = receipt["operation"]["operation_id"].as_str().unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                loop {
                    let status_query = batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(operation))
                        .returning(["status"]);
                    let status = db
                        .query(query::QueryRequest::read(status_query))
                        .await
                        .unwrap();
                    match status["status"]["status"].as_str() {
                        Some("succeeded") => break,
                        Some("queued" | "running") => tokio::task::yield_now().await,
                        state => panic!("unexpected index state: {state:?}"),
                    }
                }
            })
            .await
            .unwrap();
        }
        for keys in [
            Vec::new(),
            (0..4096).rev().collect::<Vec<i64>>(),
            vec![7; 4096],
            (100..4196).collect(),
        ] {
            let expected: Vec<_> = (0..16_i64)
                .filter(|key| keys.contains(key))
                .map(|key| vec![json!(key)])
                .collect();
            let request: cypher::Request = serde_json::from_value(json!({
                "query":"MATCH (n:Membership) WHERE n.key IN $keys RETURN n.key ORDER BY n.key",
                "parameters":{"keys":keys}
            }))
            .unwrap();
            assert_eq!(db.cypher(request).await.unwrap().rows, expected);
            let read = batch::read_batch()
                .var_as(
                    "nodes",
                    traversal::g()
                        .n_with_label_where(
                            "Membership",
                            expr::Predicate::is_in("key", value::PropertyValue::I64Array(keys)),
                        )
                        .order_by("key", traversal::Order::Asc)
                        .value_map(Some(vec!["key"])),
                )
                .returning(["nodes"]);
            let native = db.query(query::QueryRequest::read(read)).await.unwrap();
            let actual: Vec<_> = native["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| vec![node["key"].clone()])
                .collect();
            assert_eq!(actual, expected, "indexed: {indexed}");
        }
        for right in [
            Vec::new(),
            (8..4104).collect::<Vec<i64>>(),
            (4096..8192).collect(),
            vec![7; 4096],
        ] {
            let left: Vec<_> = (0..4096_i64).rev().collect();
            let expected: Vec<_> = (0..16_i64)
                .filter(|key| left.contains(key) && right.contains(key))
                .map(|key| vec![json!(key)])
                .collect();
            let request: cypher::Request = serde_json::from_value(json!({
                "query":"MATCH (n:Membership) WHERE n.key IN $left AND n.key IN $right RETURN n.key ORDER BY n.key",
                "parameters":{"left":left,"right":right}
            })).unwrap();
            assert_eq!(db.cypher(request).await.unwrap().rows, expected);
            let read = batch::read_batch()
                .var_as(
                    "nodes",
                    traversal::g()
                        .n_with_label_where(
                            "Membership",
                            expr::Predicate::and(vec![
                                expr::Predicate::is_in("key", value::PropertyValue::I64Array(left)),
                                expr::Predicate::is_in(
                                    "key",
                                    value::PropertyValue::I64Array(right),
                                ),
                            ]),
                        )
                        .order_by("key", traversal::Order::Asc)
                        .value_map(Some(vec!["key"])),
                )
                .returning(["nodes"]);
            let native = db.query(query::QueryRequest::read(read)).await.unwrap();
            let actual: Vec<_> = native["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| vec![node["key"].clone()])
                .collect();
            assert_eq!(
                actual, expected,
                "intersected membership, indexed: {indexed}"
            );
        }
    }
    db.close().await.unwrap();
}

#[test]
fn literal_and_parameter_membership_uses_equality_indexes_with_cypher_semantics() {
    std::thread::Builder::new()
        .name("cypher-indexed-membership".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(verify_indexed_membership());
        })
        .unwrap()
        .join()
        .unwrap();
}

pub(super) async fn create_index(db: &db::HelixDB, spec: index::IndexSpec) {
    let create = batch::write_batch()
        .var_as("operation", traversal::g().create_index_if_not_exists(spec))
        .returning(["operation"]);
    let receipt = db.query(query::QueryRequest::write(create)).await.unwrap();
    let operation = receipt["operation"]["operation_id"].as_str().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let status_query = batch::read_batch()
                .var_as("status", traversal::g().get_index_operation(operation))
                .returning(["status"]);
            let status = db
                .query(query::QueryRequest::read(status_query))
                .await
                .unwrap();
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::task::yield_now().await,
                state => panic!("unexpected index state: {state:?}"),
            }
        }
    })
    .await
    .unwrap();
}

/// Each case runs before and after its index exists; the unindexed label scan
/// evaluates the complete predicate and is the reference result.
async fn verify_indexed_membership() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(0,199) AS i CREATE (:Item {key:i, alt:toFloat(i), name:'n'+toString(i)})",
    )
    .await;
    run(
        &db,
        "CREATE (:Item {name:'missing'}), (:Item {key:'x', name:'string'}), \
         (:Item {key:true, name:'bool'}), (:Item {key:2.5, name:'fraction'}), \
         (:Item {key:1.0, name:'float-one'}), (:Account {email:'a'}), \
         (:Account {email:'b'}), (:Account {name:'no-email'})",
    )
    .await;
    run(
        &db,
        "UNWIND [10, 12, 14] AS i MATCH (a:Item {key:i}), (b:Item {key:i+1}) CREATE (a)-[:NEXT]->(b)",
    )
    .await;
    // No indexed element can hold a lookup this large, so it never matches.
    let big = "x".repeat(1_100_000);
    let repeated: Vec<i64> = (0..64).chain(0..10).collect();
    let wide: Vec<i64> = (100..165).collect();
    let cases = [
        ("MATCH (n:Item) WHERE n.key IN [1, 2.5, 'x', null, 1] RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [true] RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [] RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN null RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [null] RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [[1], 2] RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN $keys RETURN n.name AS v ORDER BY v", json!({"keys":[3, 4, {"$type":"float","value":"NaN"}]})),
        ("MATCH (n:Item) WHERE n.key IN [1, $p] RETURN n.name AS v ORDER BY v", json!({"p":7})),
        ("MATCH (n:Item) WHERE n.key IN $keys RETURN count(*) AS v", json!({"keys":wide})),
        ("MATCH (n:Item) WHERE n.key IN [1, 2] AND n.name = 'n2' RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [1, 2] AND n.alt IN [2.0, 9.0] RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [5] OR n.name = 'n6' RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:Item) WHERE NOT n.key IN [1] AND n.key < 3 RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (a:Item {name:'n1'}) MATCH (b:Item) WHERE b.key IN [1, 2] AND b.key = a.key RETURN b.name AS v", json!({})),
        ("OPTIONAL MATCH (n:Item) WHERE n.key IN [999] RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [10, 11] RETURN n.name AS v ORDER BY v LIMIT 1", json!({})),
        ("MATCH (a:Account) WHERE a.email IN ['b', 'a', 'zz', null] RETURN a.email AS v ORDER BY v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN [1, $big] RETURN n.name AS v ORDER BY v", json!({"big": big})),
        ("MATCH (n:Item) WHERE n.key IN $keys RETURN n.name AS v", json!({"keys": [big]})),
        ("MATCH (n:Item) WHERE n.key = $big RETURN n.name AS v", json!({"big": big})),
        ("MATCH (n:Item {key: $big}) RETURN n.name AS v", json!({"big": big})),
        ("MATCH (a:Account) WHERE a.email IN ['a', $big] RETURN a.email AS v", json!({"big": big})),
        ("MATCH (n:Item {name: 'n2'}) WHERE n.key IN [2, 3, 4] RETURN n.name AS v", json!({})),
        ("MATCH (a:Item)-[:NEXT]->(b:Item) WHERE a.key IN [10, 12, 13] RETURN a.name AS a, b.name AS b ORDER BY a", json!({})),
        ("UNWIND [1, 2] AS x MATCH (b:Item) WHERE b.key = x AND b.name IN ['n1', 'n2', 'zz'] RETURN b.name AS v ORDER BY v", json!({})),
        ("MATCH (n:Item) WHERE n.key IN $keys RETURN count(*) AS v", json!({"keys": repeated})),
        // Conjuncts that cannot fail keep the index source; every candidate
        // is still checked against the complete predicate.
        ("MATCH (n:Item) WHERE n.key = 5 AND n.alt > 4.0 RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key = 7 AND (n.name STARTS WITH 'z' OR n.alt IS NOT NULL) RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key = 1 AND NOT (n.name = 'float-one') RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key = 9 AND n.key > 'a' RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WHERE n.key = 2 OR n.key = 'x' OR n.key = null RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (n:Item) WHERE n.key = 1 OR 3 = n.key RETURN n.name AS v ORDER BY v", json!({})),
        ("MATCH (a:Account) WHERE a.email = 'a' OR a.email = $e RETURN a.email AS v ORDER BY v", json!({"e": "b"})),
        ("UNWIND [1, 2] AS x MATCH (b:Item) WHERE b.key = x AND b.name <> $skip RETURN b.name AS v ORDER BY v", json!({"skip": "n2"})),
        // WHERE after a pass-through WITH.
        ("MATCH (n:Item) WITH n WHERE n.key = 5 RETURN n.name AS v", json!({})),
        ("MATCH (n:Item) WITH n AS m WHERE m.key IN [6, 1] RETURN m.name AS v ORDER BY v", json!({})),
        ("MATCH (n:Item) WITH n.key AS k, n WHERE k = 8 RETURN n.name AS v", json!({})),
        // A later WHERE that can fail keeps the MATCH's own index source.
        ("MATCH (n:Item) WHERE n.key = 5 WITH n WHERE n.alt + 1 > 0 RETURN n.name AS v", json!({})),
        // Properties of bound nodes cannot fail, so they keep index access.
        ("MATCH (a:Item {key: 10})-[:NEXT]->(b:Item) WHERE b.key > a.key RETURN b.name AS v", json!({})),
        ("MATCH (a:Item {key: 12}) MATCH (b:Item) WHERE b.key = 13 AND b.alt > a.alt RETURN b.name AS v", json!({})),
        // Boolean parameters and projected conditions cannot fail.
        ("MATCH (n:Item) WHERE n.key = 7 AND $flag RETURN n.name AS v", json!({"flag": true})),
        ("MATCH (n:Item) WHERE n.key = 7 AND $flag RETURN n.name AS v", json!({"flag": false})),
        ("MATCH (n:Item) WITH n, n.alt > 3.0 AS big WHERE big AND n.key = 9 RETURN n.name AS v", json!({})),
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
    // Index sources must not skip errors that a label scan reports, and
    // statement-local writes must be visible to them. Writes are reverted.
    let failures = [
        (
            "MATCH (n:Item) WHERE n.key IN $keys RETURN n",
            json!({"keys": 3}),
        ),
        (
            "MATCH (n:Item {name: toString(1 / $z)}) WHERE n.key IN [999] RETURN n.name",
            json!({"z": 0}),
        ),
        (
            "MATCH (n:Item {name: toString(1 / $z), key: 999}) RETURN n.name",
            json!({"z": 0}),
        ),
        (
            "MATCH (n:Item) WHERE n.name = toString(1 / $z) AND n.key IN [] RETURN n.name",
            json!({"z": 0}),
        ),
        // A condition that is not boolean fails on every row.
        (
            "MATCH (n:Item) WHERE n.key = 7 AND $flag RETURN n.name",
            json!({"flag": 3}),
        ),
    ];
    let writes = [
        (
            "MATCH (n:Item {key: 3}) SET n.key = 1003 WITH n MATCH (m:Item) WHERE m.key IN [3, 1003] RETURN m.name AS v",
            "MATCH (n:Item {key: 1003}) SET n.key = 3",
        ),
        (
            "MATCH (a:Account {email: 'a'}) SET a.email = 'c' WITH a MATCH (b:Account) WHERE b.email IN ['a', 'c'] RETURN b.email AS v",
            "MATCH (a:Account {email: 'c'}) SET a.email = 'a'",
        ),
    ];
    let mut errors = Vec::new();
    for (text, parameters) in &failures {
        errors.push(
            db.cypher(request(text, parameters))
                .await
                .unwrap_err()
                .to_string(),
        );
    }
    let mut written = Vec::new();
    for (text, revert) in &writes {
        written.push(run(&db, text).await.rows);
        run(&db, revert).await;
    }
    assert_eq!(written, [vec![vec![json!("n3")]], vec![vec![json!("c")]]]);
    assert!(
        errors[1..4]
            .iter()
            .all(|error| error.contains("DivisionByZero"))
            && errors[4].contains("InvalidArgumentType"),
        "{errors:?}"
    );
    let rows = |response: &cypher::Response| response.rows.clone();
    assert_eq!(
        rows(&reference[0]),
        [["float-one"], ["fraction"], ["n1"], ["string"]].map(|v| vec![json!(v[0])])
    );
    assert_eq!(rows(&reference[1]), vec![vec![json!("bool")]]);
    assert!(reference[2..5]
        .iter()
        .all(|response| response.rows.is_empty()));
    assert_eq!(rows(&reference[5]), vec![vec![json!("n2")]]);
    assert_eq!(
        rows(&reference[6]),
        vec![vec![json!("n3")], vec![json!("n4")]]
    );
    assert_eq!(rows(&reference[8]), vec![vec![json!(65)]]);
    assert_eq!(rows(&reference[14]), vec![vec![json!(null)]]);
    assert_eq!(
        rows(&reference[16]),
        vec![vec![json!("a")], vec![json!("b")]]
    );
    assert_eq!(
        reference[17..22].iter().map(rows).collect::<Vec<_>>(),
        [
            vec![vec![json!("float-one")], vec![json!("n1")]],
            vec![],
            vec![],
            vec![],
            vec![vec![json!("a")]],
        ]
    );
    assert_eq!(rows(&reference[23]).len(), 2);
    // 64 distinct keys, where the stored float 1.0 also equals 1.
    assert_eq!(rows(&reference[25]), vec![vec![json!(65)]]);
    let names = |values: &[&str]| {
        values
            .iter()
            .map(|value| vec![json!(value)])
            .collect::<Vec<_>>()
    };
    assert_eq!(
        reference[26..43].iter().map(rows).collect::<Vec<_>>(),
        [
            names(&["n5"]),
            names(&["n7"]),
            names(&["n1"]),
            names(&[]),
            names(&["n2", "string"]),
            names(&["float-one", "n1", "n3"]),
            names(&["a", "b"]),
            names(&["float-one", "n1"]),
            names(&["n5"]),
            names(&["float-one", "n1", "n6"]),
            names(&["n8"]),
            names(&["n5"]),
            names(&["n11"]),
            names(&["n13"]),
            names(&["n7"]),
            names(&[]),
            names(&["n9"]),
        ]
    );

    create_index(&db, index::IndexSpec::node_equality("Item", "key")).await;
    create_index(&db, index::IndexSpec::node_equality("Item", "name")).await;
    create_index(
        &db,
        index::IndexSpec::node_unique_equality("Account", "email"),
    )
    .await;
    for ((text, parameters), expected) in cases.iter().zip(&reference) {
        let actual = db
            .cypher(request(text, parameters))
            .await
            .unwrap_or_else(|e| panic!("{text}: {e:?}"));
        assert_eq!(actual.rows, expected.rows, "{text}");
    }
    for ((text, parameters), error) in failures.iter().zip(&errors) {
        let actual = db.cypher(request(text, parameters)).await.unwrap_err();
        assert_eq!(&actual.to_string(), error, "{text}");
    }
    for ((text, revert), rows) in writes.iter().zip(&written) {
        assert_eq!(&run(&db, text).await.rows, rows, "{text}");
        run(&db, revert).await;
    }

    // The label scan validates every Item; the index reads only the matches.
    let indexed = run(
        &db,
        "MATCH (n:Item) WHERE n.key IN [1, 2, 3] RETURN n.name AS v",
    )
    .await;
    assert_eq!(indexed.rows.len(), 4);
    assert!(
        indexed.resources.reads.multi_get_keys <= 16,
        "{:?}",
        indexed.resources.reads
    );
    assert!(
        reference[0].resources.reads.multi_get_keys >= 200,
        "{:?}",
        reference[0].resources.reads
    );
    for (text, reference) in [
        (cases[26].0, &reference[26]),
        (cases[31].0, &reference[31]),
        (cases[34].0, &reference[34]),
        (cases[37].0, &reference[37]),
        (cases[39].0, &reference[39]),
    ] {
        let indexed = run(&db, text).await;
        assert!(
            indexed.resources.reads.multi_get_keys <= 16
                && reference.resources.reads.multi_get_keys >= 200,
            "{text}: {:?} vs {:?}",
            indexed.resources.reads,
            reference.resources.reads
        );
    }

    assert_eq!(
        run(&db, "CREATE (:Item {key:500, name:'new'}) WITH 1 AS x MATCH (n:Item) WHERE n.key IN [500, 501] RETURN n.name AS v")
            .await
            .rows,
        vec![vec![json!("new")]]
    );
    assert_eq!(
        run(&db, "MATCH (n:Item {key: 4}) DETACH DELETE n WITH count(*) AS c MATCH (m:Item) WHERE m.key IN [4, 5] RETURN m.name AS v")
            .await
            .rows,
        vec![vec![json!("n5")]]
    );
    db.close().await.unwrap();
}
