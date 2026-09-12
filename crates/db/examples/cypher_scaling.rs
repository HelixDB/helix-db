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
        reports.extend(measure_duplicate_join(size).await?);
        reports.extend(measure_product(size).await?);
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

// Keep duplicate-key fixtures separate so adding this case does not change the
// graph topology, source labels or stored payloads of the existing benchmarks.
async fn measure_duplicate_join(size: usize) -> Result<Vec<serde_json::Value>> {
    let directory = tempfile::tempdir()?;
    let source = HelixDbSource::Disk {
        root: directory.path().to_owned(),
        database: "cypher-duplicate-join-scaling".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(db::config::QueryTelemetry::Disabled);
    let db = HelixDB::open_with_config(source.clone(), config.clone()).await?;
    let setup = db.cypher(cypher::Request::new(format!(
        "UNWIND range(0,{}) AS i CREATE (:L {{key:toInteger(i/64)}}),(:R {{key:toInteger(i/64)}})",size-1,
    ))).await;
    db.flush_writer().await?;
    db.close().await?;
    setup?;
    let db = HelixDB::open_with_config(source, config).await?;
    let matches = size / 64 * 64 * 64 + (size % 64).pow(2);
    let measured = measure_cases(
        &db,
        size,
        [(
            "duplicate_join",
            "MATCH (a:L),(b:R) WHERE a.key=b.key RETURN count(*)".into(),
            json!([[matches]]),
        )],
    )
    .await;
    db.close().await?;
    measured
}

// Independent fixture: keep the established graph benchmark denominator stable.
async fn measure_product(size: usize) -> Result<Vec<serde_json::Value>> {
    let directory = tempfile::tempdir()?;
    let source = HelixDbSource::Disk {
        root: directory.path().to_owned(),
        database: "cypher-product-scaling".into(),
    };
    let config = source
        .embedded_default_config()
        .with_query_telemetry(db::config::QueryTelemetry::Disabled);
    let db = HelixDB::open_with_config(source.clone(), config.clone()).await?;
    let inner = size.min(64);
    let setup = db.cypher(cypher::Request::new(format!(
        "CREATE (:Anchor) WITH 1 AS marker UNWIND range(1,{size}) AS k CREATE (:Outer {{key:k}}) WITH k WHERE k<={inner} CREATE (:Inner {{key:k}})"
    ))).await;
    db.flush_writer().await?;
    db.close().await?;
    setup?;
    let db = HelixDB::open_with_config(source, config).await?;
    let measured = measure_cases(
        &db,
        size,
        [
            (
                "product_aggregate",
                "MATCH (a:Outer),(b:Inner) RETURN count(*),sum(a.key),sum(b.key)".into(),
                json!([[
                    size * inner,
                    size * (size + 1) / 2 * inner,
                    inner * (inner + 1) / 2 * size
                ]]),
            ),
            (
                "product_limit",
                "MATCH (a:Outer),(b:Inner) RETURN 1 AS found LIMIT 1".into(),
                json!([[1]]),
            ),
            (
                "product_correlated_aggregate",
                format!("UNWIND range(1,{size}) AS outer MATCH (a:Inner),(b:Anchor) RETURN count(*),sum(outer)"),
                json!([[size * inner, size * (size + 1) / 2 * inner]]),
            ),
            (
                "product_correlated_join",
                format!("UNWIND range(1,{size}) AS outer MATCH (a:Inner),(b:Inner) WHERE a.key=b.key RETURN count(*),sum(outer)"),
                json!([[size * inner, size * (size + 1) / 2 * inner]]),
            ),
        ],
    )
    .await;
    db.close().await?;
    measured
}

async fn measure(db: &HelixDB, size: usize) -> Result<Vec<serde_json::Value>> {
    let correlated_values: Vec<_> = (0..4 * size)
        .map(|index| index % (size + 1))
        .filter(|key| *key < size)
        .collect();
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
            "correlated_index_aggregate",
            format!("UNWIND range(0,{}) AS i WITH i%{} AS key OPTIONAL MATCH (n:Chain {{key:key}}) RETURN count(*),count(n),sum(n.key)", 4 * size - 1, size + 1),
            json!([[4 * size, correlated_values.len(), correlated_values.iter().sum::<usize>()]]),
        ),
        (
            "correlated_index_chain",
            format!("UNWIND range(0,{}) AS i WITH i%{} AS key OPTIONAL MATCH (a:Chain {{key:key}})-[:NEXT]->(b) RETURN count(*),count(a),sum(b.key)",4*size-1,size+1),
            json!([[4*size,correlated_values.len(),correlated_values.iter().map(|key|(key+1)%size).sum::<usize>()]]),
        ),
        (
            "correlated_index_product",
            format!("UNWIND range(0,{}) AS key MATCH (a:Chain {{key:key}}),(b:Left) RETURN count(*),sum(a.key)",size.min(64)-1),
            json!([[size.min(64)*size,size.min(64)*(size.min(64)-1)/2*size]]),
        ),
        (
            "correlated_index_join",
            format!("UNWIND range(0,{}) AS i WITH i%{} AS key OPTIONAL MATCH (a:Chain {{key:key}}),(b:Right) WHERE a.key=b.key RETURN count(*),count(a),sum(b.key)",4*size-1,size+1),
            json!([[4*size,correlated_values.len(),correlated_values.iter().sum::<usize>()]]),
        ),
        (
            "barrier_distinct_product",
            format!("UNWIND range(0,{}) AS i WITH DISTINCT i%{} AS key MATCH (a:Chain {{key:key}}),(b:Left) RETURN count(*),sum(a.key)",2*size.min(64)-1,size.min(64)),
            json!([[size.min(64)*size,size.min(64)*(size.min(64)-1)/2*size]]),
        ),
        (
            "barrier_sorted_top_k",
            format!("UNWIND range(0,{}) AS key WITH key ORDER BY key DESC OPTIONAL MATCH (a:Chain {{key:key}})-[:NEXT]->(b) RETURN b.key ORDER BY b.key DESC LIMIT 5",size.min(64)-1),
            {
                let mut expected=(0..size.min(64)).map(|key|(key+1)%size).collect::<Vec<_>>();
                expected.sort_unstable_by(|a,b|b.cmp(a));
                json!(expected.into_iter().take(5).map(|key|vec![key]).collect::<Vec<_>>())
            },
        ),
        (
            "barrier_aggregate_star",
            "UNWIND [0,0] AS key WITH min(key) AS key MATCH (a:Chain {key:key})-[:SPOKE]->(b) RETURN count(*),sum(b.key)".into(),
            json!([[size-2,size*(size-1)/2-1]]),
        ),
        (
            "star",
            "MATCH (a:Chain {key:0})-[:SPOKE]->(b) RETURN count(*)".into(),
            json!([[size - 2]]),
        ),
        (
            "star_limit",
            "MATCH (a:Chain)-[:SPOKE]->(b) RETURN 1 AS found LIMIT 1".into(),
            json!([[1]]),
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
            "bound_chain",
            "MATCH (a:Chain) OPTIONAL MATCH p=(a)-[:NEXT]->(b)-[:NEXT]->(c) RETURN count(*),count(p),sum(length(p))".into(),
            // NEXT forms one directed cycle. Every start has exactly one
            // two-edge path, with distinct edge identities at these sizes.
            json!([[size,size,2*size]]),
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
    measure_cases(db, size, queries).await
}

async fn measure_cases(
    db: &HelixDB,
    size: usize,
    queries: impl IntoIterator<Item = (&'static str, String, serde_json::Value)>,
) -> Result<Vec<serde_json::Value>> {
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
            if case == "duplicate_join" {
                let matches = size / 64 * 64 * 64 + (size % 64).pow(2);
                // Build IDs scale linearly; probe output must remain bounded by
                // a batch even when each key has 64 duplicates on both sides.
                if response.resources.peak_memory_bytes > 2 * 1024 * 1024 + 64 * size
                    || response.resources.reads.scans != 0
                    || response.resources.reads.point_gets > 2
                    || response.resources.reads.multi_get_batches
                        > 2 * matches.div_ceil(512) + 4 * size.div_ceil(512) + 8
                {
                    return Err(format!("duplicate join exceeded its memory/access guard at size {size}: peak={}, reads={:?}",response.resources.peak_memory_bytes,response.resources.reads).into());
                }
            }
            if case.starts_with("product_")
                && (response.resources.peak_memory_bytes > 2 * 1024 * 1024 + 64 * size
                    || response.resources.reads.scans != 0
                    || response.resources.reads.point_gets != 2)
            {
                return Err(format!("{case} exceeded its memory/source-read guard at size {size}: peak={}, reads={:?}",response.resources.peak_memory_bytes,response.resources.reads).into());
            }
            if case == "product_limit"
                && (response.resources.reads.multi_get_batches > 3
                    || response.resources.reads.multi_get_keys > 4)
            {
                return Err(format!(
                    "product limit read beyond its demanded candidates at size {size}: {:?}",
                    response.resources.reads,
                )
                .into());
            }
            if case == "projection_chain_limit" && response.resources.reads.multi_get_keys > 8 {
                return Err(format!(
                    "downstream limit failed to bound property reads at size {size}"
                )
                .into());
            }
            if case == "correlated_index_aggregate"
                // The allocator-verified sparse map bounds replace the old
                // undercounted 512 KiB estimate. One MiB covers a normal batch;
                // retaining the 16,384-row input would still fail this guard.
                && (response.resources.peak_memory_bytes > 1024 * 1024
                    || response.resources.reads.scans != 0
                    || response.resources.reads.point_gets > 8 * size
                    // Candidate and completion events share each 512-event
                    // batch; validation and aggregation each hydrate its rows.
                    || response.resources.reads.multi_get_batches > 8 * size.div_ceil(256) + 8)
            {
                return Err(format!(
                    "correlated lookup exceeded its memory/access guard at size {size}: peak={}, reads={:?}",
                    response.resources.peak_memory_bytes, response.resources.reads,
                ).into());
            }
            if matches!(
                case,
                "correlated_index_chain"
                    | "correlated_index_product"
                    | "correlated_index_join"
                    | "barrier_distinct_product"
                    | "barrier_sorted_top_k"
                    | "barrier_aggregate_star"
            ) && (response.resources.peak_memory_bytes > 2 * 1024 * 1024 + 64 * size
                || response.resources.reads.scans != 0
                || response.resources.reads.point_gets > 12 * size + 2)
            {
                return Err(format!("{case} exceeded its indexed pattern memory/read guard at size {size}: peak={}, reads={:?}",response.resources.peak_memory_bytes,response.resources.reads).into());
            }
            if case == "star_limit" && response.resources.reads.multi_get_keys > 16 {
                return Err(format!(
                    "limited graph expansion read the whole neighborhood at size {size}"
                )
                .into());
            }
            if matches!(case, "optional" | "bound_chain")
                && response.resources.peak_memory_bytes > 1024 * 1024
            {
                return Err(format!(
                    "{case} retained its correlated relation at size {size}: peak={}",
                    response.resources.peak_memory_bytes,
                )
                .into());
            }
            if case == "bound_chain"
                && (response.resources.reads.scans != 0
                    || response.resources.reads.point_gets > 2 * size + 1
                    || response.resources.reads.multi_get_batches > 7 * size + 64)
            {
                // The two expansions retain per-parent topology reads. Final
                // pattern metadata must share batches across those parents.
                return Err(format!(
                    "bound-chain property hydration regressed at size {size}: {:?}",
                    response.resources.reads,
                )
                .into());
            }
            if case == "star"
                && (response.resources.reads.point_gets > 4
                    || response.resources.reads.multi_get_batches > 6 * size.div_ceil(512) + 4)
            {
                return Err(format!(
                    "star expansion failed to batch edge-pair reads at size {size}"
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
            if matches!(case, "chain" | "cycle")
                && response.resources.reads.multi_get_batches
                    > 2 * size + 10 * (3 * size).div_ceil(512) + 8
            {
                return Err(format!(
                    "{case} failed to batch endpoint reads across parents at size {size}"
                )
                .into());
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
