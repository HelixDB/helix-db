use db::{cypher, HelixDB, HelixDbSource};
use serde_json::json;

async fn database() -> HelixDB {
    HelixDB::open(HelixDbSource::InMemory {
        database: "cypher-contract".into(),
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn node_scans_feed_aggregation_and_top_k_without_retaining_the_relation() {
    let db = database().await;
    run(&db, "UNWIND range(0,4095) AS i CREATE (:N {key:i})").await;
    let limits = cypher::Limits {
        memory_bytes: 192 * 1024,
        batch_rows: 32,
        ..Default::default()
    };
    let execute = |query: &str| {
        cypher::execute(
            &db,
            cypher::Request::new(query),
            db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            db::query_service::QueryMode::Execute,
            db::execution_control::ExecutionControl::default(),
            limits,
        )
    };
    assert_eq!(
        execute("MATCH (n:N) WHERE n.key % 2=0 RETURN count(*),sum(n.key)")
            .await
            .unwrap()
            .rows,
        vec![vec![json!(2048), json!(4_192_256)]]
    );
    let ranked =
        execute("MATCH (n) WHERE n.key % 2=1 RETURN n.key AS key ORDER BY key DESC SKIP 1 LIMIT 3")
            .await
            .unwrap();
    assert_eq!(
        ranked.rows,
        vec![vec![json!(4093)], vec![json!(4091)], vec![json!(4089)]]
    );
    assert_eq!(ranked.resources.reads.scans, 1);
    assert_eq!(ranked.resources.reads.scan_rows, 4096);
    assert_eq!(ranked.resources.reads.point_gets, 0);
    assert!(ranked.resources.reads.multi_get_batches <= 512);
    assert!(ranked.resources.reads.multi_get_keys <= 4 * 4096);
    assert!(ranked.resources.peak_memory_bytes <= limits.memory_bytes);
    assert_eq!(
        execute("UNWIND range(1,100000) AS x RETURN x ORDER BY x DESC SKIP 1 LIMIT 2")
            .await
            .unwrap()
            .rows,
        vec![vec![json!(99999)], vec![json!(99998)]]
    );
    assert_eq!(
        execute("MATCH (n:N) WITH n ORDER BY n.key LIMIT 1 SET n.touched=true RETURN n.key")
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        execute("MATCH (n:N) WHERE n.touched RETURN count(*)")
            .await
            .unwrap()
            .rows,
        vec![vec![json!(1)]]
    );
    assert_eq!(
        execute("OPTIONAL MATCH (n:Absent) RETURN count(*)")
            .await
            .unwrap()
            .rows,
        vec![vec![json!(1)]]
    );
    db.close().await.unwrap();
}
async fn run(db: &HelixDB, query: &str) -> cypher::Response {
    db.cypher(cypher::Request::new(query))
        .await
        .unwrap_or_else(|e| panic!("{query}: {e:?}"))
}

#[tokio::test]
async fn reads_crud_paths_and_atomic_failure() {
    let db = database().await;
    let result=run(&db,"CREATE (a:Person {name:'Ada', age:30})-[r:KNOWS {since:2020}]->(b:Person {name:'Bob', age:40}) RETURN a.name, b.name, type(r)").await;
    assert_eq!(
        result.rows,
        json!([["Ada", "Bob", "KNOWS"]])
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_array().unwrap().clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        run(
            &db,
            "MATCH p=(a:Person)-[r:KNOWS]->(b) RETURN a.name, b.name, length(p)"
        )
        .await
        .rows,
        vec![vec![json!("Ada"), json!("Bob"), json!(1)]]
    );
    assert_eq!(run(&db,"MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b.age > 50 RETURN a.name, b.name ORDER BY a.name").await.rows,vec![vec![json!("Ada"),json!(null)],vec![json!("Bob"),json!(null)]]);
    let err = db
        .cypher(cypher::Request::new(
            "MATCH (n:Person {name:'Ada'}) SET n.age = 99 DELETE n",
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err,cypher::Error::Query(ref e) if e.detail=="DeleteConnectedNode"),
        "{err:?}"
    );
    assert_eq!(
        run(&db, "MATCH (n:Person {name:'Ada'}) RETURN n.age")
            .await
            .rows,
        vec![vec![json!(30)]]
    );
    run(
        &db,
        "MATCH (n:Person {name:'Ada'}) SET n += {age:null, city:'London'} REMOVE n.city",
    )
    .await;
    assert_eq!(
        run(&db, "MATCH (n:Person {name:'Ada'}) RETURN properties(n)")
            .await
            .rows,
        vec![vec![json!({"name":"Ada"})]]
    );
    run(&db, "MATCH (n:Person {name:'Ada'}) DETACH DELETE n").await;
    assert_eq!(
        run(&db, "MATCH (n) RETURN count(n)").await.rows,
        vec![vec![json!(1)]]
    );
    assert_eq!(
        run(&db, "MATCH ()-[r]->() RETURN count(r)").await.rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn grouping_duplicates_scope_and_empty_aggregation() {
    let db = database().await;
    assert_eq!(
        run(
            &db,
            "UNWIND [1,1,2,null] AS x RETURN x, count(*) AS c ORDER BY x DESC"
        )
        .await
        .rows,
        vec![
            vec![json!(null), json!(1)],
            vec![json!(2), json!(1)],
            vec![json!(1), json!(2)]
        ]
    );
    assert_eq!(
        run(&db, "UNWIND [] AS x WITH count(x) AS c RETURN c + 1 AS y")
            .await
            .rows,
        vec![vec![json!(1)]]
    );
    assert_eq!(
        run(
            &db,
            "UNWIND [3,1,2,1] AS x WITH DISTINCT x ORDER BY x SKIP 1 LIMIT 1 RETURN x"
        )
        .await
        .rows,
        vec![vec![json!(2)]]
    );
    assert_eq!(
        run(&db, "WITH 1 AS x WITH x+1 AS x RETURN x").await.rows,
        vec![vec![json!(2)]]
    );
    assert_eq!(
        run(&db, "RETURN 9223372036854775807 AS n").await.rows,
        vec![vec![
            json!({"$type":"integer","value":"9223372036854775807"})
        ]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn bounded_generators_parameters_and_rollback() {
    use db::{
        encoding::v2::keys::scope::DataScope, execution_control::ExecutionControl,
        query_service::QueryMode,
    };
    let db = database().await;
    let limits = cypher::Limits {
        collection_items: 2,
        memory_bytes: 2 * 1024 * 1024,
        ..cypher::Limits::default()
    };
    let response = cypher::execute(
        &db,
        cypher::Request::new("UNWIND range(1000000,2000000) AS i WITH i LIMIT 3000 RETURN sum(i)"),
        DataScope::LegacyUnscoped,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        limits,
    )
    .await
    .unwrap();
    assert_eq!(response.rows, vec![vec![json!(3_004_498_500_i64)]]);
    let error = cypher::execute(
        &db,
        cypher::Request::new("CREATE (n:N) RETURN range(0,10)"),
        DataScope::LegacyUnscoped,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        limits,
    )
    .await
    .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(e) if e.detail == "CollectionLimit"));
    assert_eq!(
        run(&db, "MATCH (n:N) RETURN count(*)").await.rows,
        vec![vec![json!(0)]]
    );
    let error = db
        .cypher(cypher::Request::new("MATCH (n) RETURN $missing"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, cypher::Error::Query(e) if e.category == "ParameterMissing" && e.phase == helix_planner::relational::ErrorPhase::Compile)
    );
    let request: cypher::Request = serde_json::from_value(json!({"query":"RETURN $i AS i, $m AS m", "parameters":{"i":{"$type":"integer","value":"9223372036854775807"},"m":{"$type":"map","value":{"$type":"literal"}}}})).unwrap();
    assert_eq!(
        db.cypher(request).await.unwrap().rows,
        vec![vec![
            json!({"$type":"integer","value":"9223372036854775807"}),
            json!({"$type":"map","value":{"$type":"literal"}})
        ]]
    );
    assert!(serde_json::from_value::<cypher::Request>(
        json!({"query":"RETURN $i","parameters":{"i":u64::MAX}})
    )
    .is_err());
    let error = cypher::execute(
        &db,
        cypher::Request::new("CREATE (n:N)"),
        DataScope::LegacyUnscoped,
        QueryMode::Execute,
        ExecutionControl::from_timeout(std::time::Duration::ZERO),
        limits,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        cypher::Error::Storage(db::error::HelixDbError::QueryDeadlineExceeded)
    ));
    assert_eq!(
        run(&db, "MATCH (n:N) RETURN count(*)").await.rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn fixed_patterns_agree_with_independent_edge_model() {
    let db = database().await;
    let edges = [(0, 1), (0, 1), (1, 2), (2, 0), (1, 1)];
    run(&db,"CREATE (a:N {i:0}),(b:N {i:1}),(c:N {i:2}), (a)-[:R]->(b),(a)-[:R]->(b),(b)-[:R]->(c),(c)-[:R]->(a),(b)-[:R]->(b)").await;
    let mut expected = Vec::new();
    for (first, (a, b)) in edges.iter().enumerate() {
        for (second, (source, c)) in edges.iter().enumerate() {
            if first != second && b == source {
                expected.push(vec![json!(a), json!(b), json!(c)]);
            }
        }
    }
    expected.sort_by_key(|row| row.iter().map(|v| v.as_i64().unwrap()).collect::<Vec<_>>());
    assert_eq!(
        run(
            &db,
            "MATCH (a:N)-[r:R]->(b:N)-[s:R]->(c:N) RETURN a.i,b.i,c.i ORDER BY a.i,b.i,c.i"
        )
        .await
        .rows,
        expected
    );
    let mut undirected = edges
        .iter()
        .flat_map(|(a, b)| {
            if a == b {
                vec![vec![json!(a), json!(b)]]
            } else {
                vec![vec![json!(a), json!(b)], vec![json!(b), json!(a)]]
            }
        })
        .collect::<Vec<_>>();
    undirected.sort_by_key(|row| row.iter().map(|v| v.as_i64().unwrap()).collect::<Vec<_>>());
    assert_eq!(
        run(
            &db,
            "MATCH (a:N)-[r:R]-(b:N) RETURN a.i,b.i ORDER BY a.i,b.i"
        )
        .await
        .rows,
        undirected
    );
    assert_eq!(
        run(
            &db,
            "WITH null AS n OPTIONAL MATCH p=(n)-[:R]->() RETURN nodes(p),relationships(p)"
        )
        .await
        .rows,
        vec![vec![json!(null), json!(null)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn stored_native_values_do_not_fail_unevaluated_cypher_branches() {
    use helix_ast::{batch, query, traversal, value};
    let db = database().await;
    db.query(query::QueryRequest::write(
        batch::write_batch()
            .var_as(
                "n",
                traversal::g().add_n(
                    "N",
                    vec![
                        ("name", value::PropertyInput::from("Ada")),
                        (
                            "future",
                            value::PropertyInput::Value(value::PropertyValue::DateTime(1)),
                        ),
                    ],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        run(
            &db,
            "MATCH (n:N) RETURN CASE WHEN false THEN n.future ELSE n.name END"
        )
        .await
        .rows,
        vec![vec![json!("Ada")]]
    );
    assert_eq!(
        run(&db, "MATCH (n:N) RETURN keys(n)").await.rows,
        vec![vec![json!(["future", "name"])]]
    );
    let error = db
        .cypher(cypher::Request::new("MATCH (n:N) RETURN n.future"))
        .await
        .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(e) if e.detail == "StoredValueType"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn storage_scopes_and_response_limits_preserve_atomicity() {
    use db::{
        encoding::v2::keys::scope, execution_control::ExecutionControl, query_service::QueryMode,
    };
    let db = database().await;
    let a = scope::DataScope::Tenant(
        scope::TenantId::from_ulid_str("00000000000000000000000001").unwrap(),
    );
    let b = scope::DataScope::Tenant(
        scope::TenantId::from_ulid_str("00000000000000000000000002").unwrap(),
    );
    cypher::execute(
        &db,
        cypher::Request::new("CREATE (:N {name:'Ada'})"),
        a,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        cypher::Limits::default(),
    )
    .await
    .unwrap();
    for namespace in [b, scope::DataScope::LegacyUnscoped] {
        assert_eq!(
            cypher::execute(
                &db,
                cypher::Request::new("MATCH (n:N) RETURN count(n)"),
                namespace,
                QueryMode::Execute,
                ExecutionControl::unlimited(),
                cypher::Limits::default()
            )
            .await
            .unwrap()
            .rows,
            vec![vec![json!(0)]]
        );
    }
    let error = cypher::execute(
        &db,
        cypher::Request::new("MATCH (n:N) SET n.name='changed' RETURN n"),
        a,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        cypher::Limits {
            result_bytes: 8,
            ..cypher::Limits::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(e) if e.detail == "ResultLimit"));
    assert_eq!(
        cypher::execute(
            &db,
            cypher::Request::new("MATCH (n:N) RETURN n.name"),
            a,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits::default()
        )
        .await
        .unwrap()
        .rows,
        vec![vec![json!("Ada")]]
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_commit_without_lost_writes() {
    let db = std::sync::Arc::new(database().await);
    run(&db, "CREATE (:Counter {n:0})").await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let jobs = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                db.cypher(cypher::Request::new("MATCH (n:Counter) SET n.n = n.n + 1"))
                    .await
            })
        })
        .collect::<Vec<_>>();
    let mut committed = 0;
    for job in jobs {
        match job.await.unwrap() {
            Ok(_) => committed += 1,
            Err(cypher::Error::Storage(error)) if error.is_transaction_conflict() => {}
            Err(error) => panic!("unexpected concurrent result: {error}"),
        }
    }
    assert!(committed > 0);
    assert_eq!(
        run(&db, "MATCH (n:Counter) RETURN n.n").await.rows,
        vec![vec![json!(committed)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn live_operator_buffers_share_one_memory_budget() {
    use db::encoding::v2::keys::scope::DataScope;
    use db::execution_control::ExecutionControl;
    use db::query_service::QueryMode;
    let db = database().await;
    run(&db, "UNWIND range(1,30) AS i CREATE (:MemoryTest {i:i})").await;
    let budget = 32 * 1024;
    for query in [
        "MATCH (a:MemoryTest), (b:MemoryTest) RETURN a.i, b.i",
        "UNWIND range(1,400) AS i RETURN collect(i)",
        "UNWIND range(1,400) AS i RETURN i ORDER BY i DESC",
    ] {
        let error = cypher::execute(
            &db,
            cypher::Request::new(query),
            DataScope::LegacyUnscoped,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits {
                memory_bytes: budget,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, cypher::Error::Query(ref error) if error.detail == "MemoryLimit"),
            "{query}: {error:?}"
        );
    }
    let response = cypher::execute(
        &db,
        cypher::Request::new("UNWIND range(1,1000000000) AS i RETURN i LIMIT 3"),
        DataScope::LegacyUnscoped,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        cypher::Limits {
            memory_bytes: budget,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        response.rows,
        vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]
    );
    assert!(response.resources.peak_memory_bytes > 0);
    assert!(response.resources.peak_memory_bytes <= budget);
    assert_eq!(
        run(&db, "MATCH (n:MemoryTest) RETURN count(n)").await.rows,
        vec![vec![json!(30)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn expression_memory_limits_do_not_truncate_values_or_limit_streaming_counts() {
    use db::encoding::v2::keys::scope::DataScope;
    use db::execution_control::ExecutionControl;
    use db::query_service::QueryMode;
    let db = database().await;
    for query in [
        "RETURN range(1,1000000000)",
        "WITH range(1,1000) AS xs RETURN xs + xs",
    ] {
        let error = cypher::execute(
            &db,
            cypher::Request::new(query),
            DataScope::LegacyUnscoped,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits {
                memory_bytes: 32 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error,cypher::Error::Query(ref error) if error.detail == "MemoryLimit"),
            "{error:?}"
        );
    }
    let result = cypher::execute(
        &db,
        cypher::Request::new(
            "UNWIND range(1,30) AS i RETURN count(i), sum(i), avg(i), min(i), max(i)",
        ),
        DataScope::LegacyUnscoped,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        cypher::Limits {
            collection_items: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            json!(30),
            json!(465),
            json!(15.5),
            json!(1),
            json!(30)
        ]]
    );
    let error = cypher::execute(
        &db,
        cypher::Request::new("UNWIND range(1,30) AS i RETURN collect(i)"),
        DataScope::LegacyUnscoped,
        QueryMode::Execute,
        ExecutionControl::unlimited(),
        cypher::Limits {
            collection_items: 3,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(ref error) if error.detail == "CollectionLimit"));
    db.close().await.unwrap();
}

#[tokio::test]
async fn plain_match_windows_bound_rows_and_response_bytes_include_the_envelope() {
    use db::encoding::v2::keys::scope::DataScope;
    use db::execution_control::ExecutionControl;
    use db::query_service::QueryMode;
    let db = database().await;
    run(&db, "UNWIND range(1,1000) AS i CREATE (:WindowTest {i:i})").await;
    for (query, expected) in [
        ("MATCH (n:WindowTest) RETURN n SKIP 2 LIMIT 3", 3),
        ("MATCH (n) RETURN n LIMIT 0", 0),
    ] {
        let result = cypher::execute(
            &db,
            cypher::Request::new(query),
            DataScope::LegacyUnscoped,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits {
                memory_bytes: 32 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result.rows.len(), expected);
        assert!(result.resources.peak_memory_bytes <= 32 * 1024);
    }
    let query = "RETURN {name: 'Ada', values: [1, null, true]} AS result";
    let reference = run(&db, query).await;
    let encoded_bytes = serde_json::to_vec(&reference).unwrap().len();
    for (limit, succeeds) in [(encoded_bytes, true), (encoded_bytes - 1, false)] {
        let result = cypher::execute(
            &db,
            cypher::Request::new(query),
            DataScope::LegacyUnscoped,
            QueryMode::Execute,
            ExecutionControl::unlimited(),
            cypher::Limits {
                result_bytes: limit,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            result.is_ok(),
            succeeds,
            "{result:?}, expected {encoded_bytes} bytes"
        );
        if let Err(error) = result {
            assert!(matches!(error,cypher::Error::Query(ref error) if error.detail=="ResultLimit"));
        }
    }
    db.close().await.unwrap();
}

#[test]
fn cypher_updates_and_rollbacks_keep_native_indexes_consistent() {
    // Use the same stack allowance as the native index lifecycle contract suite.
    std::thread::Builder::new().name("cypher-index-contract".into()).stack_size(16 * 1024 * 1024).spawn(|| {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            use helix_ast::{batch, expr, index, query, traversal};
            let db = database().await;
            run(&db,"CREATE (:Indexed {name:'Ada'})-[:LINK]->(:Indexed {name:'Bob'})").await;
            let receipt = db.query(query::QueryRequest::write(batch::write_batch().var_as("operation",
                traversal::g().create_index_if_not_exists(index::IndexSpec::node_unique_equality("Indexed","name"))).returning(["operation"]))).await.unwrap();
            let operation = receipt["operation"]["operation_id"].as_str().unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(30),async {
                loop {
                    let status = db.query(query::QueryRequest::read(batch::read_batch().var_as("status",
                        traversal::g().get_index_operation(operation)).returning(["status"]))).await.unwrap();
                    match status["status"]["status"].as_str() {
                        Some("succeeded") => break,
                        Some("queued" | "running") => tokio::task::yield_now().await,
                        state => panic!("unexpected index state: {state:?}"),
                    }
                }
            }).await.unwrap();
            assert_eq!(run(&db,"MATCH (n:Indexed {name:'Ada'}) SET n.name='Updated' WITH n MATCH (m:Indexed {name:'Updated'}) RETURN m.name").await.rows,vec![vec![json!("Updated")]]);
            assert!(db.cypher(cypher::Request::new("MATCH (n:Indexed {name:'Updated'}) SET n.name='Changed' WITH n RETURN 1/0")).await.is_err());
            let native = db.query(query::QueryRequest::read(batch::read_batch().var_as("names",
                traversal::g().n_with_label_where("Indexed",expr::Predicate::eq("name","Updated")).value_map(Some(vec!["name".to_owned()]))).returning(["names"]))).await.unwrap();
            assert_eq!(native["names"].as_array().unwrap().len(),1);
            assert_eq!(native["names"][0]["name"],"Updated");
            assert!(db.cypher(cypher::Request::new("CREATE (:Indexed {name:'new'}), (:Indexed {name:'Bob'})")).await.is_err());
            assert_eq!(run(&db,"MATCH (n:Indexed {name:'new'}) RETURN count(n)").await.rows,vec![vec![json!(0)]]);
            run(&db,"MATCH (n:Indexed {name:'Updated'}) DETACH DELETE n").await;
            assert_eq!(run(&db,"MATCH (n:Indexed {name:'Updated'}) RETURN count(n)").await.rows,vec![vec![json!(0)]]);
            assert_eq!(run(&db,"MATCH (n:Indexed {name:'Bob'}) RETURN n.name").await.rows,vec![vec![json!("Bob")]]);
            run(&db,"MATCH (n:Indexed {name:'Bob'}) SET n.name=null").await;
            assert_eq!(run(&db,"MATCH (n:Indexed {name:'Bob'}) RETURN count(n)").await.rows,vec![vec![json!(0)]]);
            db.flush_writer().await.unwrap();
            db.close().await.unwrap();
        });
    }).unwrap().join().unwrap();
}

#[tokio::test]
async fn durable_cypher_data_survives_reopen_and_reader_mode_rejects_writes() {
    let source = HelixDbSource::InMemoryToken {
        token: db::ProcessLocalDatabaseToken::new("cypher-durable").unwrap(),
    };
    let db = HelixDB::open(source.clone()).await.unwrap();
    run(
        &db,
        "CREATE (:Durable {name:'Ada'})-[:KNOWS]->(:Durable {name:'Bob'})",
    )
    .await;
    db.flush_writer().await.unwrap();
    db.close().await.unwrap();
    let reader = HelixDB::open_reader(source.clone()).await.unwrap();
    assert_eq!(
        run(&reader, "MATCH (n:Durable) RETURN n.name ORDER BY n.name")
            .await
            .rows,
        vec![vec![json!("Ada")], vec![json!("Bob")]]
    );
    let error = reader
        .cypher(cypher::Request::new(
            "CREATE (:Durable {name:'ReaderWrite'})",
        ))
        .await
        .unwrap_err();
    assert!(matches!(error,cypher::Error::Query(ref e) if e.detail=="WriterRequired"));
    reader.close().await.unwrap();
    let reopened = HelixDB::open(source).await.unwrap();
    assert_eq!(
        run(&reopened, "MATCH ()-[r:KNOWS]->() RETURN count(r)")
            .await
            .rows,
        vec![vec![json!(1)]]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn equality_join_is_bounded_and_preserves_multiplicity_and_numeric_semantics() {
    let db = database().await;
    run(
        &db,
        "UNWIND range(1,400) AS i CREATE (:Left {key:i}),(:Right {key:i})",
    )
    .await;
    let query = "MATCH (a:Left),(b:Right) WHERE a.key=b.key RETURN count(*) AS count";
    let plan = helix_planner::relational::plan(
        helix_cypher::compile(query).unwrap(),
        &helix_planner::context::PlannerContext::default(),
    )
    .unwrap();
    assert!(plan.matches()[&0]
        .steps
        .iter()
        .any(|step| matches!(step, helix_planner::relational::MatchStep::HashJoin { .. })));
    assert_eq!(plan.matches()[&0].cartesian_products, 0);
    // 160,000 Cartesian candidates exceed either budget. Account for whole
    // sparse property-map nodes: the default hydration batch needs 3 MiB,
    // while smaller batches retain the original 2 MiB execution contract.
    // Neither configuration may truncate the build side or matching rows.
    for (memory_bytes, batch_rows) in [(2 * 1024 * 1024, 64), (3 * 1024 * 1024, 512)] {
        let result = cypher::execute(
            &db,
            cypher::Request::new(query),
            db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            db::query_service::QueryMode::Execute,
            db::execution_control::ExecutionControl::default(),
            cypher::Limits {
                memory_bytes,
                batch_rows,
                ..cypher::Limits::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result.rows, vec![vec![json!(400)]]);
        assert!(result.resources.peak_memory_bytes <= memory_bytes);
    }
    run(
        &db,
        "CREATE (:Left {key:1.0}),(:Left),(:Right),(:Right {key:1.0})",
    )
    .await;
    assert_eq!(run(&db, query).await.rows, vec![vec![json!(403)]]);
    run(
        &db,
        "CREATE (:ExactLeft {key:9007199254740993}),(:ExactRight {key:9007199254740992.0})",
    )
    .await;
    assert_eq!(
        run(
            &db,
            "MATCH (a:ExactLeft),(b:ExactRight) WHERE a.key=b.key RETURN count(*)"
        )
        .await
        .rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        run(
            &db,
            "MATCH (a:Left) OPTIONAL MATCH (a),(b:Missing) WHERE a.key=b.key RETURN count(*)"
        )
        .await
        .rows,
        vec![vec![json!(402)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn generated_rows_feed_grouped_aggregates_in_bounded_batches() {
    let db = database().await;
    let result = cypher::execute(&db,cypher::Request::new("UNWIND range(1,200000) AS x RETURN x%3 AS bucket, count(*) AS n, sum(x) AS total, count(DISTINCT x%2) AS parity ORDER BY bucket SKIP 1 LIMIT 1"),
        db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,db::query_service::QueryMode::Execute,
        db::execution_control::ExecutionControl::default(),cypher::Limits {memory_bytes:32*1024,batch_rows:32,collection_items:2,..cypher::Limits::default()}).await.unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            json!(1),
            json!(66667),
            json!(6666700000_i64),
            json!(2)
        ]]
    );
    assert!(result.resources.peak_memory_bytes <= 32 * 1024);
    assert_eq!(
        run(
            &db,
            "UNWIND [] AS x RETURN count(x),sum(x),avg(x),collect(x)"
        )
        .await
        .rows,
        vec![vec![json!(0), json!(0), json!(null), json!([])]]
    );
    assert_eq!(
        run(
            &db,
            "UNWIND [null,1,2] AS x WITH count(x) AS n CREATE (:Result {count:n}) RETURN n"
        )
        .await
        .rows,
        vec![vec![json!(2)]]
    );
    assert_eq!(
        run(&db, "MATCH (n:Result) RETURN n.count").await.rows,
        vec![vec![json!(2)]]
    );
    db.close().await.unwrap();
}
