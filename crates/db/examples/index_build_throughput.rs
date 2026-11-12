//! How long one tenant's first batch of index builds takes, by worker concurrency.
//!
//! Opens a fresh disk-backed writer, creates the index set one Helix Foundry
//! load asks for (113 equality and range indexes, 10 text indexes partitioned
//! by `foundry_part`) on an empty tenant in one write batch, then polls every
//! operation until each has succeeded. Prints when each finished.
//!
//! `cargo run --release -p db --example index_build_throughput -- <runs>`
//!
//! - `IBT_TASKS`: `total,secondary,vector,text` worker tasks (default: the
//!   database's defaults).
//! - `IBT_FLUSH_MS`: WAL flush interval (default 100, SlateDB's).
//! - `IBT_ROOT`: directory for the databases (default a temp dir).
//! - `IBT_URL`: a running server (`http://host:8080`) to send the same requests
//!   to over `/v2/query` instead, each run in a fresh tenant. The server's own
//!   settings apply, so `IBT_TASKS`, `IBT_FLUSH_MS` and `IBT_ROOT` don't.
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use db::config::{IndexLifecycleConcurrency, IndexLifecycleThroughputTuning};
use db::encoding::keys::scope::{DataScope, TenantId};
use db::{HelixDB, HelixDbSource};
use helix_ast::{batch, index::IndexSpec, query::QueryRequest, traversal};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Where the requests go: an embedded writer, or a server over HTTP.
enum Target {
    Embedded(HelixDB, DataScope),
    Http {
        client: reqwest::Client,
        url: String,
        tenant: String,
    },
}

impl Target {
    async fn query(&self, request: QueryRequest) -> Result<serde_json::Value> {
        match self {
            Self::Embedded(db, scope) => Ok(db.query_scoped(request, *scope).await?),
            Self::Http {
                client,
                url,
                tenant,
            } => {
                let response = client
                    .post(format!("{url}/v2/query"))
                    .header("x-helix-tenant-id", tenant)
                    .header("content-type", "application/json")
                    .body(serde_json::to_vec(&request)?)
                    .send()
                    .await?;
                let status = response.status();
                let body = response.text().await?;
                if !status.is_success() {
                    return Err(format!("{status}: {body}").into());
                }
                Ok(serde_json::from_str(&body)?)
            }
        }
    }
}

const OWN_NODES: [&str; 9] = [
    "FoundryLoad",
    "FoundryDocument",
    "FoundrySection",
    "FoundryMention",
    "FoundryEvidence",
    "FoundryReview",
    "FoundryChunk",
    "FoundryViolation",
    "FoundrySwept",
];
const OWN_EDGES: [&str; 7] = [
    "FOUNDRY_IN_DOCUMENT",
    "FOUNDRY_IN_SECTION",
    "FOUNDRY_MENTION_OF",
    "FOUNDRY_FROM_SECTION",
    "FOUNDRY_FROM_CHUNK",
    "FOUNDRY_CONCERNS",
    "FOUNDRY_ABOUT",
];
const TYPES: usize = 10;
const LINKS: usize = 15;

/// The ontology engine's `ensure_indexes` for an ontology of `TYPES` named
/// types with one name each and `LINKS` links.
fn foundry_specs() -> Vec<IndexSpec> {
    let own = [
        ("FoundryDocument", "doc_id"),
        ("FoundryDocument", "content_hash"),
        ("FoundryDocument", "sweep"),
        ("FoundryEvidence", "fact_key"),
        ("FoundryEvidence", "fact_source"),
        ("FoundryEvidence", "prop_key"),
        ("FoundryEvidence", "row_entity"),
        ("FoundryReview", "chunk_key"),
        ("FoundryReview", "doc_id"),
        ("FoundryReview", "prop_key"),
        ("FoundrySwept", "sweep_key"),
        ("FoundryViolation", "violation_key"),
        ("FoundryViolation", "constraint"),
        ("FoundryChunk", "chunk_key"),
        ("FoundryChunk", "stream"),
    ]
    .map(|(label, prop)| IndexSpec::node_equality(label, prop))
    .into_iter()
    .chain([IndexSpec::node_range("FoundryViolation", "violation_key")]);
    let types = (0..TYPES).flat_map(|t| {
        let ty = format!("Type{t}");
        [
            IndexSpec::node_equality(&ty, "norm_name"),
            IndexSpec::node_range(&ty, "norm_name"),
            IndexSpec::node_equality(&ty, "foundry_part"),
            IndexSpec::node_equality(&ty, "name__nn"),
            IndexSpec::node_text(&ty, "name", Some("foundry_part")),
        ]
    });
    let links = (0..LINKS).map(|l| IndexSpec::edge_equality(format!("LINK_{l}"), "fact_key"));
    let loads = OWN_NODES
        .iter()
        .map(|l| l.to_string())
        .chain((0..TYPES).map(|t| format!("Type{t}")))
        .map(|l| IndexSpec::node_equality(l, "foundry_load"))
        .chain(
            OWN_EDGES
                .iter()
                .map(|l| l.to_string())
                .chain((0..LINKS).map(|l| format!("LINK_{l}")))
                .map(|l| IndexSpec::edge_equality(l, "foundry_load")),
        );
    own.chain(types).chain(links).chain(loads).collect()
}

fn env_tasks() -> Result<IndexLifecycleConcurrency> {
    let Ok(raw) = std::env::var("IBT_TASKS") else {
        return Ok(IndexLifecycleConcurrency::default());
    };
    let n: Vec<usize> = raw.split(',').map(str::parse).collect::<std::result::Result<_, _>>()?;
    let [total, secondary, vector, text] = n[..] else {
        return Err(format!("IBT_TASKS wants four numbers, got {raw}").into());
    };
    Ok(IndexLifecycleConcurrency::try_new(total, secondary, vector, text)?)
}

async fn run(root: &std::path::Path, run: usize) -> Result<()> {
    let flush = std::env::var("IBT_FLUSH_MS").map_or(Ok(100), |v| v.parse::<u64>())?;
    let tasks = env_tasks()?;
    // A fresh tenant per run, so a server never finds the indexes already built.
    let tenant = format!("01M74N56VRTHK2J90HVCVKY{:03}", run % 1000);
    let target = match std::env::var("IBT_URL") {
        Ok(url) => Target::Http {
            client: reqwest::Client::new(),
            url,
            tenant,
        },
        Err(_) => {
            std::fs::create_dir_all(root.join(format!("run-{run}")))?;
            let source = HelixDbSource::Disk {
                root: root.join(format!("run-{run}")),
                database: "ibt".into(),
            };
            let tuning = IndexLifecycleThroughputTuning::new(Default::default(), tasks);
            let config = source
                .embedded_default_config()
                .with_index_lifecycle_throughput_tuning(tuning)
                .with_wal_flush_interval_ms(
                    NonZeroU64::new(flush).ok_or("IBT_FLUSH_MS is zero")?,
                );
            let db = HelixDB::open_with_config(source, config).await?;
            Target::Embedded(db, DataScope::Tenant(TenantId::from_ulid_str(&tenant)?))
        }
    };

    let specs = foundry_specs();
    let names: Vec<String> = (0..specs.len()).map(|i| format!("i{i}")).collect();
    let write = specs
        .iter()
        .zip(&names)
        .fold(batch::write_batch(), |b, (spec, name)| {
            b.var_as(name, traversal::g().create_index_if_not_exists(spec.clone()))
        })
        .returning(names.clone());
    let started = Instant::now();
    let created = target.query(QueryRequest::write(write)).await?;
    let create_ms = started.elapsed().as_millis();
    let mut pending: Vec<(usize, String)> = names
        .iter()
        .enumerate()
        .filter_map(|(i, n)| Some((i, created[n]["operation_id"].as_str()?.to_owned())))
        .collect();
    let operations = pending.len();
    eprintln!("created {operations} operations in {create_ms} ms");
    let mut done_at = Vec::with_capacity(operations);
    let mut reported = Instant::now();
    while !pending.is_empty() {
        if reported.elapsed() >= Duration::from_secs(5) {
            reported = Instant::now();
            eprintln!("{:.0}s: {} pending", started.elapsed().as_secs_f64(), pending.len());
        }
        let read = pending
            .iter()
            .fold(batch::read_batch(), |b, (i, op)| {
                b.var_as(&format!("s{i}"), traversal::g().get_index_operation(op.as_str()))
            })
            .returning(pending.iter().map(|(i, _)| format!("s{i}")).collect::<Vec<_>>());
        let status = target.query(QueryRequest::read(read)).await?;
        let mut still = Vec::with_capacity(pending.len());
        for (i, op) in pending {
            match status[format!("s{i}")]["status"].as_str() {
                Some("succeeded") => done_at.push(started.elapsed()),
                Some("queued" | "running") => still.push((i, op)),
                other => return Err(format!("{:?} is {other:?}", specs[i]).into()),
            }
        }
        pending = still;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let at = |q: usize| done_at[(operations * q / 100).saturating_sub(1)].as_secs_f64();
    println!(
        "run={run} tasks={},{},{},{} flush_ms={flush} indexes={} operations={operations} create_ms={create_ms} p25={:.1}s p50={:.1}s p90={:.1}s all={:.1}s",
        tasks.total_operation_tasks(),
        tasks.secondary_tasks(),
        tasks.vector_tasks(),
        tasks.text_tasks(),
        specs.len(),
        at(25),
        at(50),
        at(90),
        at(100),
    );
    let Target::Embedded(db, _) = target else {
        return Ok(());
    };
    db.close().await?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let runs: usize = std::env::args().nth(1).map_or(Ok(1), |v| v.parse())?;
    let temp = tempfile::tempdir()?;
    let root = std::env::var("IBT_ROOT").map_or(temp.path().to_path_buf(), Into::into);
    for r in 0..runs {
        run(&root, r).await?;
    }
    Ok(())
}
