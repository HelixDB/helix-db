//! Stored size of one database written under each SST block codec.
//!
//! Writes the same deterministic data once per codec, closes the database so
//! every memtable reaches an SST, and reports the bytes of every stored object
//! under the database, grouped by directory. It then reopens the database and
//! times one cold read, so the cost of decompressing blocks is visible.
//!
//! Run in release mode:
//! `cargo run --release -p db --example storage_size -- <count> [none|zstd ...]`.
//! The codec list defaults to `none zstd`. Environment variables:
//!
//! - `STORAGE_SIZE_SHAPE`: `graph` (default) writes `<count>` events, half as
//!   many objects and three edges per event, with equality and range indexes,
//!   and reads a property of every event. `vector` writes `<count>` documents
//!   with a 384-dimension embedding and a title, builds a vector index over
//!   them, and runs one vector search.
//! - `STORAGE_SIZE_S3_BUCKET`: when set, the database lives in this bucket of
//!   an S3-compatible store under the server's default configuration instead
//!   of an embedded disk database. `STORAGE_SIZE_S3_ENDPOINT` and
//!   `STORAGE_SIZE_S3_REGION` (default `us-east-1`) locate it; credentials
//!   come from the usual `AWS_*` variables. Each sample is deleted afterwards.
//! - `STORAGE_SIZE_SETTLE_SECS`: when above zero, the database is reopened
//!   with a one-second garbage-collection age after the cold read, and its
//!   size is reported again once it stops shrinking (or after this many
//!   seconds).
use db::{cypher, HelixDB, HelixDbSource};
use futures::{StreamExt, TryStreamExt};
use helix_ast::prelude::*;
use helix_ast::{index, query::QueryRequest};
use serde_json::{json, Value};
use slatedb::object_store::{self, path::Path as StorePath, ObjectStore};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const OBJECT_TYPES: [&str; 5] = ["orders", "items", "packages", "customers", "products"];
const ACTIVITIES: [&str; 10] = [
    "place order",
    "confirm order",
    "pick item",
    "item out of stock",
    "reorder item",
    "create package",
    "send package",
    "failed delivery",
    "package delivered",
    "payment reminder",
];
const QUALIFIERS: [&str; 4] = ["order", "item", "package", "customer"];
const OBJECTS_PER_EVENT: usize = 3;
const WRITES_PER_REQUEST: usize = 500;
const DIMENSION: usize = 384;
const INDEX_DEADLINE: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Graph,
    Vector,
}

fn main() -> Result<()> {
    std::thread::Builder::new()
        .name("storage-size".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(run())
        })?
        .join()
        .expect("measurement thread did not panic")
}

async fn run() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let count: usize = arguments.next().as_deref().unwrap_or("20000").parse()?;
    let codecs = arguments.collect::<Vec<_>>();
    let codecs = if codecs.is_empty() {
        vec!["none".to_string(), "zstd".to_string()]
    } else {
        codecs
    };
    let shape = match std::env::var("STORAGE_SIZE_SHAPE").as_deref() {
        Err(_) | Ok("graph") => Shape::Graph,
        Ok("vector") => Shape::Vector,
        Ok(other) => return Err(format!("unknown shape {other}").into()),
    };
    if shape == Shape::Graph && count < 2 {
        return Err("the event count must be at least 2, so the graph has objects".into());
    }
    let settle = Duration::from_secs(
        std::env::var("STORAGE_SIZE_SETTLE_SECS")
            .ok()
            .map(|seconds| seconds.parse())
            .transpose()?
            .unwrap_or(0),
    );
    let bucket = std::env::var("STORAGE_SIZE_S3_BUCKET").ok();
    let mut samples = Vec::new();
    for name in codecs {
        let compression = name.parse::<db::config::SstCompression>()?;
        let directory = tempfile::tempdir()?;
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos();
        let database = format!("storage-size-{name}-{nanos}");
        // Disk samples use the embedded disk profile; S3 samples use the
        // server's default configuration on the bucket.
        let (store, source, defaults): (Arc<dyn ObjectStore>, _, _) = match &bucket {
            Some(bucket) => {
                let endpoint = std::env::var("STORAGE_SIZE_S3_ENDPOINT").ok();
                let mut builder = object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(
                        std::env::var("STORAGE_SIZE_S3_REGION")
                            .unwrap_or_else(|_| "us-east-1".to_string()),
                    );
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_allow_http(endpoint.starts_with("http://"))
                        .with_endpoint(endpoint);
                }
                (Arc::new(builder.build()?), None, db::DbConfig::new())
            }
            None => {
                let source = HelixDbSource::Disk {
                    root: directory.path().to_owned(),
                    database: database.clone(),
                };
                let defaults = source.embedded_default_config();
                (
                    Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
                        directory.path(),
                    )?),
                    Some(source),
                    defaults,
                )
            }
        };
        let config = defaults
            .with_query_telemetry(db::config::QueryTelemetry::Disabled)
            .with_sst_compression(compression);
        let settings = config.slate().to_writer_settings(None);
        let open = |config: db::DbConfig| {
            let source = source.clone();
            let store = Arc::clone(&store);
            let database = database.clone();
            async move {
                match source {
                    Some(source) => HelixDB::open_with_config(source, config).await,
                    None => {
                        HelixDB::open_with_object_store_and_config(database, store, config).await
                    }
                }
            }
        };

        let db = open(config.clone()).await?;
        let started = Instant::now();
        let written = match shape {
            Shape::Graph => graph_fixture(&db, count).await,
            Shape::Vector => vector_fixture(&db, count).await,
        };
        db.flush_writer().await?;
        db.close().await?;
        let write_seconds = started.elapsed().as_secs_f64();
        let written = written?;
        let bytes = stored_bytes(&store, &database).await?;

        let db = open(config.clone()).await?;
        let started = Instant::now();
        let read = match shape {
            Shape::Graph => Value::from(
                db.cypher(cypher::Request::new(
                    "MATCH (e:Event) RETURN count(e), sum(e.cost)",
                ))
                .await?
                .rows,
            ),
            Shape::Vector => db
                .query(QueryRequest::read(
                    read_batch()
                        .var_as(
                            "hits",
                            g().vector_search_nodes(
                                "Doc",
                                "embedding",
                                embedding(&mut 0xD1B5_4A32_D192_ED03),
                                10,
                                None,
                            )
                            .id(),
                        )
                        .returning(["hits"]),
                ))
                .await?["hits"]
                .clone(),
        };
        let read_seconds = started.elapsed().as_secs_f64();
        db.close().await?;

        let settled = if settle.is_zero() {
            Value::Null
        } else {
            let age = slatedb::config::GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(1)),
                min_age: Duration::from_secs(1),
                dry_run: false,
            };
            let mut collecting = settings;
            collecting.garbage_collector_options = Some(slatedb::config::GarbageCollectorOptions {
                manifest_options: Some(age),
                wal_options: Some(age),
                compacted_options: Some(age),
                compactions_options: Some(age),
                ..collecting.garbage_collector_options.unwrap_or_default()
            });
            let db = open(config.clone().with_slate_settings(collecting)).await?;
            let started = Instant::now();
            let mut last = stored_bytes(&store, &database).await?;
            let mut unchanged = 0;
            while unchanged < 3 && started.elapsed() < settle {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let now = stored_bytes(&store, &database).await?;
                unchanged = if now == last { unchanged + 1 } else { 0 };
                last = now;
            }
            db.close().await?;
            json!({
                "bytes_by_directory": last,
                "bytes_total": last.values().sum::<u64>(),
                "seconds": started.elapsed().as_secs_f64(),
            })
        };
        if bucket.is_some() {
            let prefix = StorePath::from(database.as_str());
            let locations = store
                .list(Some(&prefix))
                .map_ok(|meta| meta.location)
                .boxed();
            store
                .delete_stream(locations)
                .try_collect::<Vec<_>>()
                .await?;
        }
        samples.push(json!({
            "codec": name,
            "written": written,
            "bytes_by_directory": bytes,
            "bytes_total": bytes.values().sum::<u64>(),
            "write_seconds": write_seconds,
            "cold_read_seconds": read_seconds,
            "cold_read_result": read,
            "settled": settled,
        }));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 2,
            "store": if bucket.is_some() { "s3_server_default" } else { "local_disk_embedded" },
            "shape": match shape { Shape::Graph => "graph", Shape::Vector => "vector" },
            "debug_build": cfg!(debug_assertions),
            "size_measurement": "stored object bytes under the database after close, before garbage collection; `settled` after collection",
            "samples": samples,
        }))?
    );
    Ok(())
}

/// Deterministic xorshift, so every codec stores identical content.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// A deterministic embedding with components in [-1, 1].
fn embedding(state: &mut u64) -> Vec<f32> {
    (0..DIMENSION)
        .map(|_| (next(state) % 2_000_001) as f32 / 1_000_000.0 - 1.0)
        .collect()
}

/// Waits until the index operation in `receipt[name]` succeeds.
async fn wait_for_index(db: &HelixDB, receipt: &Value, name: &str) -> Result<()> {
    let operation = receipt[name]["operation_id"]
        .as_str()
        .ok_or("index operation ID")?;
    tokio::time::timeout(INDEX_DEADLINE, async {
        loop {
            let response = db
                .query(QueryRequest::read(
                    read_batch()
                        .var_as("status", g().get_index_operation(operation))
                        .returning(["status"]),
                ))
                .await?;
            match response["status"]["status"].as_str() {
                Some("succeeded") => return Ok(()),
                Some("queued" | "running") => tokio::time::sleep(Duration::from_millis(20)).await,
                _ => return Err(format!("index build did not succeed: {response}").into()),
            }
        }
    })
    .await?
}

async fn graph_fixture(db: &HelixDB, events: usize) -> Result<Value> {
    let objects = events / 2;
    let indexes = [
        index::IndexSpec::node_unique_equality("Object", "ocel_id"),
        index::IndexSpec::node_equality("Object", "ocel_type"),
        index::IndexSpec::node_unique_equality("Event", "ocel_id"),
        index::IndexSpec::node_equality("Event", "ocel_type"),
        index::IndexSpec::node_range("Event", "ocel_time_us"),
        index::IndexSpec::edge_equality("E2O", "qualifier"),
    ];
    // Wait for every build to finish before writing, so each codec measures
    // the same fully built indexes.
    for spec in indexes {
        let receipt = db
            .query(QueryRequest::write(
                write_batch()
                    .var_as("index", g().create_index_if_not_exists(spec))
                    .returning(["index"]),
            ))
            .await?;
        wait_for_index(db, &receipt, "index").await?;
    }

    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let object_ids = add_nodes(db, objects, |i| {
        (
            "Object",
            vec![
                ("ocel_id", PropertyInput::from(format!("o-{i:08}"))),
                (
                    "ocel_type",
                    PropertyInput::from(OBJECT_TYPES[i % OBJECT_TYPES.len()]),
                ),
            ],
        )
    })
    .await?;
    let event_ids = add_nodes(db, events, |i| {
        let jitter = next(&mut state) % 60_000_000;
        (
            "Event",
            vec![
                ("ocel_id", PropertyInput::from(format!("e-{i:08}"))),
                (
                    "ocel_type",
                    PropertyInput::from(ACTIVITIES[(next(&mut state) as usize) % ACTIVITIES.len()]),
                ),
                (
                    "ocel_time_us",
                    PropertyInput::from(
                        1_600_000_000_000_000_i64 + (i as i64) * 60_000_000 + jitter as i64,
                    ),
                ),
                (
                    "cost",
                    PropertyInput::from((next(&mut state) % 100_000) as i64),
                ),
            ],
        )
    })
    .await?;

    let pairs = event_ids
        .iter()
        .flat_map(|event| (0..OBJECTS_PER_EVENT).map(move |_| *event))
        .map(|event| {
            // Square the uniform draw to skew toward low object ids (hubs).
            let draw = (next(&mut state) % 1_000_000) as f64 / 1_000_000.0;
            let object = object_ids[((draw * draw) * object_ids.len() as f64) as usize];
            let qualifier = QUALIFIERS[(next(&mut state) as usize) % QUALIFIERS.len()];
            (event, object, qualifier)
        })
        .collect::<Vec<_>>();
    for chunk in pairs.chunks(WRITES_PER_REQUEST) {
        let writes = chunk.iter().enumerate().fold(
            write_batch(),
            |writes, (position, (event, object, qualifier))| {
                writes.var_as(
                    &format!("edge_{position}"),
                    g().n(NodeRef::id(*event)).add_e(
                        "E2O",
                        NodeRef::id(*object),
                        vec![("qualifier", PropertyInput::from(*qualifier))],
                    ),
                )
            },
        );
        db.query(QueryRequest::write(writes.returning(Vec::<String>::new())))
            .await?;
    }
    Ok(json!({ "events": events, "objects": objects, "edges": pairs.len() }))
}

async fn vector_fixture(db: &HelixDB, documents: usize) -> Result<Value> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    add_nodes(db, documents, |i| {
        (
            "Doc",
            vec![
                ("title", PropertyInput::from(format!("document {i:08}"))),
                ("embedding", PropertyInput::from(embedding(&mut state))),
            ],
        )
    })
    .await?;
    // Built over the written documents, and finished before the size is taken.
    let receipt = db
        .query(QueryRequest::write(
            write_batch()
                .var_as(
                    "vector",
                    g().create_vector_index_nodes(
                        "Doc",
                        "embedding",
                        NonZeroUsize::new(DIMENSION).ok_or("dimension is nonzero")?,
                        index::VectorDistanceMetric::Cosine,
                        None::<String>,
                    ),
                )
                .returning(["vector"]),
        ))
        .await?;
    wait_for_index(db, &receipt, "vector").await?;
    Ok(json!({ "documents": documents, "dimension": DIMENSION }))
}

async fn add_nodes(
    db: &HelixDB,
    count: usize,
    mut node: impl FnMut(usize) -> (&'static str, Vec<(&'static str, PropertyInput)>),
) -> Result<Vec<u64>> {
    let mut ids = Vec::with_capacity(count);
    for start in (0..count).step_by(WRITES_PER_REQUEST) {
        let end = (start + WRITES_PER_REQUEST).min(count);
        let names = (start..end).map(|i| format!("n{i}")).collect::<Vec<_>>();
        let writes = (start..end)
            .zip(&names)
            .fold(write_batch(), |writes, (i, name)| {
                let (label, properties) = node(i);
                writes.var_as(name, g().add_n(label, properties).id())
            });
        let response = db
            .query(QueryRequest::write(writes.returning(names.clone())))
            .await?;
        for name in &names {
            ids.push(
                first_id(&response[name])
                    .ok_or_else(|| format!("node id in response: {}", response[name]))?,
            );
        }
    }
    Ok(ids)
}

fn first_id(value: &Value) -> Option<u64> {
    match value {
        Value::Object(map) => map
            .get("id")
            .and_then(|id| id.as_u64().or_else(|| id.as_str()?.parse().ok()))
            .or_else(|| map.values().find_map(first_id)),
        Value::Array(items) => items.iter().find_map(first_id),
        Value::Number(number) => number.as_u64(),
        Value::Null | Value::Bool(_) | Value::String(_) => None,
    }
}

/// Bytes of every stored object under `database`, by directory.
async fn stored_bytes(
    store: &Arc<dyn ObjectStore>,
    database: &str,
) -> Result<BTreeMap<String, u64>> {
    let prefix = StorePath::from(database);
    let objects = store.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    Ok(objects
        .into_iter()
        .fold(BTreeMap::new(), |mut totals, meta| {
            let location = meta.location.as_ref();
            let directory = location
                .rsplit_once('/')
                .map_or("", |(directory, _)| directory)
                .trim_start_matches(database)
                .trim_start_matches('/')
                .to_string();
            *totals.entry(directory).or_insert(0) += meta.size;
            totals
        }))
}
