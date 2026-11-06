//! On-disk size of one embedded database written under each SST block codec.
//!
//! Writes the same deterministic event/object graph (node properties, edges
//! with a property, equality and range indexes) once per codec, closes the
//! database so every memtable reaches an SST, and reports the bytes of every
//! file under the database root, grouped by top-level directory. It then
//! reopens the database and times one query that reads a property of every
//! event, so the cost of decompressing blocks on a cold read is visible.
//!
//! Run in release mode:
//! `cargo run --release -p db --example storage_size -- <events> [none|zstd ...]`.
//! The codec list defaults to `none zstd`.
use db::{cypher, HelixDB, HelixDbSource};
use helix_ast::prelude::*;
use helix_ast::{index, query::QueryRequest};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, Instant},
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
    let events: usize = arguments.next().as_deref().unwrap_or("20000").parse()?;
    let codecs = arguments.collect::<Vec<_>>();
    let codecs = if codecs.is_empty() {
        vec!["none".to_string(), "zstd".to_string()]
    } else {
        codecs
    };
    if events < 2 {
        return Err("the event count must be at least 2, so the graph has objects".into());
    }
    let objects = events / 2;
    let mut samples = Vec::new();
    for name in codecs {
        let codec = match name.as_str() {
            "none" => None,
            "zstd" => Some(slatedb::config::CompressionCodec::Zstd),
            other => return Err(format!("unknown codec {other}").into()),
        };
        let directory = tempfile::tempdir()?;
        let source = HelixDbSource::Disk {
            root: directory.path().to_owned(),
            database: "storage-size".into(),
        };
        let defaults = source
            .embedded_default_config()
            .with_query_telemetry(db::config::QueryTelemetry::Disabled);
        let mut settings = defaults.slate().to_writer_settings(None);
        settings.compression_codec = codec;
        let config = defaults.with_slate_settings(settings);

        let db = HelixDB::open_with_config(source.clone(), config.clone()).await?;
        let started = Instant::now();
        let written = fixture(&db, events, objects).await;
        db.flush_writer().await?;
        db.close().await?;
        let write_seconds = started.elapsed().as_secs_f64();
        let edges = written?;
        let bytes = directory_bytes(directory.path())?;

        let db = HelixDB::open_with_config(source, config).await?;
        let started = Instant::now();
        let result = db
            .cypher(cypher::Request::new(
                "MATCH (e:Event) RETURN count(e), sum(e.cost)",
            ))
            .await?;
        let read_seconds = started.elapsed().as_secs_f64();
        db.close().await?;
        samples.push(json!({
            "codec": name,
            "events": events,
            "objects": objects,
            "edges": edges,
            "bytes_by_directory": bytes,
            "bytes_total": bytes.values().sum::<u64>(),
            "write_seconds": write_seconds,
            "cold_full_event_read_seconds": read_seconds,
            "cold_full_event_read_rows": result.rows,
        }));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "storage": "local_disk",
            "debug_build": cfg!(debug_assertions),
            "size_measurement": "file bytes under the database root after close, before garbage collection",
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

async fn fixture(db: &HelixDB, events: usize, objects: usize) -> Result<usize> {
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
        let operation = receipt["index"]["operation_id"]
            .as_str()
            .ok_or("index operation ID")?;
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let response = db
                    .query(QueryRequest::read(
                        read_batch()
                            .var_as("status", g().get_index_operation(operation))
                            .returning(["status"]),
                    ))
                    .await?;
                match response["status"]["status"].as_str() {
                    Some("succeeded") => {
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    }
                    Some("queued" | "running") => tokio::task::yield_now().await,
                    _ => return Err(format!("index build did not succeed: {response}").into()),
                }
            }
        })
        .await??;
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
    Ok(pairs.len())
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

fn directory_bytes(root: &Path) -> Result<BTreeMap<String, u64>> {
    let mut totals = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                pending.push(path);
                continue;
            }
            let relative = path.strip_prefix(root)?;
            let group = relative
                .parent()
                .map(|parent| parent.display().to_string())
                .unwrap_or_default();
            *totals.entry(group).or_insert(0) += entry.metadata()?.len();
        }
    }
    Ok(totals)
}
