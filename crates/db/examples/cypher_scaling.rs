//! Local correctness-checked scaling measurements. Run in release mode and
//! redirect stdout to a file under target; no wall-clock dates are recorded.
use db::{cypher, HelixDB, HelixDbSource};
use helix_ast::{batch, graph, index, query, traversal};
use serde_json::json;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    // Match the existing index lifecycle integration suite's stack allowance.
    std::thread::Builder::new()
        .name("cypher-scaling".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(run())
        })?
        .join()
        .expect("benchmark thread did not panic")
}
async fn run() -> Result<()> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let sizes = if arguments.is_empty() {
        vec![64, 512, 4096]
    } else {
        arguments
            .iter()
            .map(|value| value.parse())
            .collect::<std::result::Result<Vec<usize>, _>>()?
    };
    if sizes.iter().any(|n| !(3..=100_000).contains(n)) {
        return Err("benchmark sizes must be between 3 and 100000".into());
    }
    let mut reports = Vec::new();
    for size in sizes {
        let directory = tempfile::tempdir()?;
        let source = HelixDbSource::Disk {
            root: directory.path().to_owned(),
            database: "cypher-scaling".into(),
        };
        let config = source
            .embedded_default_config()
            .with_query_telemetry(db::config::QueryTelemetry::Disabled);
        let db = HelixDB::open_with_config(source.clone(), config.clone()).await?;
        let setup = fixture(&db, size).await;
        db.flush_writer().await?;
        db.close().await?;
        setup?;
        let db = HelixDB::open_with_config(source, config).await?;
        let measured = measure(&db, size).await;
        db.close().await?;
        reports.extend(measured?);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version":1,"storage":"local_disk","debug_build":cfg!(debug_assertions),
            "memory_measurement":"query admission estimate; excludes unintegrated native buffers",
            "storage_measurement":"foreground object-store reads and cache accesses from SlateDB task-scoped observer",
            "samples":reports
        }))?
    );
    Ok(())
}

async fn fixture(db: &HelixDB, size: usize) -> Result<()> {
    let nodes = db.cypher(cypher::Request::new(format!("UNWIND range(0,{}) AS i CREATE (n:Chain {{key:i}}),(:Left {{key:i}}),(:Right {{key:i}}) RETURN n",size-1))).await?;
    let mut ids = BTreeMap::new();
    for row in nodes.rows {
        let node = &row[0];
        ids.insert(
            node["properties"]["key"].as_u64().ok_or("node key")?,
            node["id"].as_str().ok_or("node ID")?.parse::<u64>()?,
        );
    }
    for start in (0..size).step_by(128) {
        let mut writes = batch::write_batch();
        for n in start..(start + 128).min(size) {
            let target = (n + 1) % size;
            writes = writes.var_as(
                &format!("edge_{n}"),
                traversal::g()
                    .n(graph::NodeRef::id(ids[&(n as u64)]))
                    .add_e(
                        "NEXT",
                        graph::NodeRef::id(ids[&(target as u64)]),
                        Vec::<(&str, helix_ast::value::PropertyValue)>::new(),
                    ),
            );
            // A high-fan-out hub and high-in-degree sink in the same graph.
            if n > 1 {
                writes = writes
                    .var_as(
                        &format!("spoke_{n}"),
                        traversal::g().n(graph::NodeRef::id(ids[&0])).add_e(
                            "SPOKE",
                            graph::NodeRef::id(ids[&(n as u64)]),
                            Vec::<(&str, helix_ast::value::PropertyValue)>::new(),
                        ),
                    )
                    .var_as(
                        &format!("sink_{n}"),
                        traversal::g()
                            .n(graph::NodeRef::id(ids[&(n as u64)]))
                            .add_e(
                                "SINK",
                                graph::NodeRef::id(ids[&1]),
                                Vec::<(&str, helix_ast::value::PropertyValue)>::new(),
                            ),
                    );
            }
            if n % 3 == 0 && n + 2 < size {
                writes = writes
                    .var_as(
                        &format!("triangle_a_{n}"),
                        traversal::g()
                            .n(graph::NodeRef::id(ids[&(n as u64)]))
                            .add_e(
                                "TRIANGLE",
                                graph::NodeRef::id(ids[&((n + 1) as u64)]),
                                Vec::<(&str, helix_ast::value::PropertyValue)>::new(),
                            ),
                    )
                    .var_as(
                        &format!("triangle_b_{n}"),
                        traversal::g()
                            .n(graph::NodeRef::id(ids[&((n + 1) as u64)]))
                            .add_e(
                                "TRIANGLE",
                                graph::NodeRef::id(ids[&((n + 2) as u64)]),
                                Vec::<(&str, helix_ast::value::PropertyValue)>::new(),
                            ),
                    )
                    .var_as(
                        &format!("triangle_c_{n}"),
                        traversal::g()
                            .n(graph::NodeRef::id(ids[&((n + 2) as u64)]))
                            .add_e(
                                "TRIANGLE",
                                graph::NodeRef::id(ids[&(n as u64)]),
                                Vec::<(&str, helix_ast::value::PropertyValue)>::new(),
                            ),
                    );
            }
        }
        db.query(query::QueryRequest::write(writes)).await?;
    }
    let receipt = db
        .query(query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "index",
                    traversal::g().create_index_if_not_exists(
                        index::IndexSpec::node_unique_equality("Chain", "key"),
                    ),
                )
                .returning(["index"]),
        ))
        .await?;
    let operation = receipt["index"]["operation_id"]
        .as_str()
        .ok_or("index operation ID")?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let response = db
                .query(query::QueryRequest::read(
                    batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(operation))
                        .returning(["status"]),
                ))
                .await?;
            match response["status"]["status"].as_str() {
                Some("succeeded") => return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(()),
                Some("queued" | "running") => tokio::task::yield_now().await,
                _ => return Err(format!("index build did not succeed: {response}").into()),
            }
        }
    })
    .await??;
    Ok(())
}

async fn measure(db: &HelixDB, size: usize) -> Result<Vec<serde_json::Value>> {
    let mixed_values = (0..size)
        .flat_map(|key| [key, key + 1])
        .filter(|key| key % 2 == 0)
        .flat_map(|key| [key, key])
        .collect::<Vec<_>>();
    let mut mixed_top = mixed_values.clone();
    mixed_top.sort_unstable_by(|left, right| right.cmp(left));
    mixed_top.truncate(5);
    let mixed_prefix = "MATCH (a:Left) UNWIND [a.key,a.key+1] AS key WITH key WHERE key%2=0 UNWIND [key,key] AS value";
    let queries = [
        (
            "selective_index",
            format!("MATCH (n:Chain {{key:{}}}) RETURN n.key", size / 2),
            json!([[size / 2]]),
        ),
        (
            "correlated_index",
            format!("UNWIND [0,{},null,{}] AS key OPTIONAL MATCH (n:Chain {{key:key}}) RETURN key,n.key ORDER BY key", size / 2, size + 1),
            json!([[0,0],[size / 2,size / 2],[size + 1,null],[null,null]]),
        ),
        (
            "chain",
            "MATCH (a:Chain)-[:NEXT]->(b)-[:NEXT]->(c) RETURN count(*)".into(),
            json!([[size]]),
        ),
        (
            "star",
            "MATCH (a:Chain {key:0})-[:SPOKE]->(b) RETURN count(*)".into(),
            json!([[size - 2]]),
        ),
        (
            "cycle",
            "MATCH (a:Chain)-[:TRIANGLE]->(b)-[:TRIANGLE]->(c)-[:TRIANGLE]->(a) RETURN count(*)"
                .into(),
            json!([[size / 3 * 3]]),
        ),
        (
            "skew",
            "MATCH (a:Chain {key:0})-[:SPOKE]->(b)-[:SINK]->(c) RETURN count(*)".into(),
            json!([[size - 2]]),
        ),
        (
            "join",
            "MATCH (a:Left),(b:Right) WHERE a.key=b.key RETURN count(*)".into(),
            json!([[size]]),
        ),
        (
            "optional",
            "MATCH (a:Left) OPTIONAL MATCH (a)-[:MISSING]->(b) RETURN count(*)".into(),
            json!([[size]]),
        ),
        (
            "aggregation",
            "MATCH (a:Left) RETURN sum(a.key),avg(a.key)".into(),
            json!([[size * (size - 1) / 2, (size - 1) as f64 / 2.0]]),
        ),
        (
            "top_k",
            "MATCH (a:Left) RETURN a.key AS key ORDER BY key DESC LIMIT 3".into(),
            json!([[size - 1], [size - 2], [size - 3]]),
        ),
        (
            "projection_chain",
            "MATCH (a:Left) WITH a.key AS key WITH key+1 AS value RETURN sum(value),avg(value)".into(),
            json!([[size * (size + 1) / 2, (size + 1) as f64 / 2.0]]),
        ),
        (
            "projection_chain_top_k",
            "MATCH (a:Left) WITH a.key AS key WITH key+1 AS value RETURN value ORDER BY value DESC LIMIT 3".into(),
            json!([[size], [size - 1], [size - 2]]),
        ),
        (
            "projection_chain_limit",
            "MATCH (a:Left) WITH a AS first WITH first AS second RETURN 1 AS found LIMIT 1".into(),
            json!([[1]]),
        ),
        (
            "mixed_unwind_aggregation",
            format!("{mixed_prefix} RETURN count(*),sum(value)"),
            json!([[mixed_values.len(), mixed_values.iter().sum::<usize>()]]),
        ),
        (
            "mixed_unwind_top_k",
            format!("{mixed_prefix} RETURN value ORDER BY value DESC LIMIT 5"),
            json!(mixed_top.into_iter().map(|value| [value]).collect::<Vec<_>>()),
        ),
    ];
    let mut measurements = Vec::new();
    for (case, statement, expected) in queries {
        for iteration in 0..4 {
            let observer = slatedb::QueryMetricsObserver::default();
            let started = Instant::now();
            let response = slatedb::scope_query_metrics(
                observer.clone(),
                db.cypher(cypher::Request::new(&statement)),
            )
            .await
            .map_err(|error| format!("{case} at size {size}: {error}"))?;
            let elapsed = started.elapsed().as_micros();
            if serde_json::to_value(&response.rows)? != expected {
                return Err(format!("incorrect {case} result at size {size}").into());
            }
            if case == "projection_chain_limit" && response.resources.reads.multi_get_keys > 8 {
                return Err(format!(
                    "downstream limit failed to bound property reads at size {size}"
                )
                .into());
            }
            if case.starts_with("mixed_unwind_")
                && response.resources.reads.multi_get_batches > 4 * size.div_ceil(512) + 4
            {
                return Err(
                    format!("UNWIND failed to batch input property reads at size {size}").into(),
                );
            }
            let storage = observer.snapshot();
            measurements.push(json!({"case":case,"size":size,"iteration":iteration,"latency_micros":elapsed,
                "planner":response.diagnostics,"peak_memory_bytes":response.resources.peak_memory_bytes,"logical_reads":response.resources.reads,
                "object_storage_reads":storage.object_storage_reads,
                "block_cache_hits":storage.block_cache.hits,"block_cache_misses":storage.block_cache.misses,
                "object_cache_hits":storage.object_cache.hits,"object_cache_misses":storage.object_cache.misses}));
        }
    }
    Ok(measurements)
}
