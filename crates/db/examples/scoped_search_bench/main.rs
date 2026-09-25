//! Scoped search benchmark over a synthetic `Group <- Item -> Attribute` graph.
//!
//! Loads the graph and times global / traversal-scoped vector and BM25 search,
//! reporting latency, recall@k and (embedded mode) object-store GETs.
//!
//! Backends (environment):
//! - `BENCH_HTTP_URL=http://host:6969`: a running server (`POST /v2/query`).
//! - `BENCH_DIR=/path`: embedded, local-filesystem object store.
//! - `BENCH_S3_BUCKET=bucket` (+ `BENCH_S3_REGION`): embedded, S3 object store.
//!
//! Modes: `load` (graph + indexes; `BENCH_VECTOR=before|after|skip`), `index`
//! (build the vector index by backfill), `query` (default).
//!
//! ```text
//! BENCH_DIR=/tmp/bench BENCH_SCALE=0.02 cargo run --release -p db --example scoped_search_bench -- load
//! BENCH_DIR=/tmp/bench cargo run --release -p db --example scoped_search_bench -- query
//! ```

mod fixture;

use std::sync::Arc;
use std::time::{Duration, Instant};

use db::{DbConfig, HelixDB};
use fixture::{env_or, Backend, CountingStore};
use helix_ast::prelude::*;
use slatedb::object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};

fn load_options() -> fixture::LoadOptions {
    fixture::LoadOptions {
        scale: env_or("BENCH_SCALE", 1.0),
        dimension: env_or("BENCH_DIM", 768),
        items_per_batch: env_or("BENCH_ITEMS_PER_BATCH", 4),
        vector: vector_build(),
    }
}

fn object_store() -> Arc<CountingStore> {
    let inner: Arc<dyn ObjectStore> = match std::env::var("BENCH_S3_BUCKET") {
        Ok(bucket) => Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_region(env_or("BENCH_S3_REGION", "us-east-2".to_string()))
                .build()
                .unwrap(),
        ),
        Err(_) => {
            let dir = std::env::var("BENCH_DIR").expect("BENCH_DIR or BENCH_S3_BUCKET is set");
            std::fs::create_dir_all(&dir).unwrap();
            Arc::new(LocalFileSystem::new_with_prefix(&dir).unwrap())
        }
    };
    Arc::new(CountingStore::new(
        inner,
        Duration::from_millis(env_or("BENCH_LATENCY_MS", 0)),
    ))
}

/// Cache config plus `BENCH_BUILD_CACHE_MB`, the vector build planning cache.
fn bench_config() -> DbConfig {
    let config = cache_config();
    let Some(build_cache_mb) = std::env::var("BENCH_BUILD_CACHE_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return config;
    };
    let limits = config
        .search_index_backfill()
        .with_vector_build_cache_bytes(
            std::num::NonZeroU64::new(build_cache_mb * 1024 * 1024)
                .expect("BENCH_BUILD_CACHE_MB is positive"),
        );
    config.with_search_index_backfill_limits(limits)
}

/// Default config; `BENCH_CACHE_DIR` switches to cloud-like hybrid caches on
/// local disk, `BENCH_BLOCK_CACHE_MB` shrinks the in-memory block cache.
fn cache_config() -> DbConfig {
    if let Ok(dir) = std::env::var("BENCH_CACHE_DIR") {
        let dir = std::path::PathBuf::from(dir);
        return DbConfig::new().with_cache(db::config::CacheConfig::new(
            db::config::VectorMemorySettings::default(),
            db::config::CacheMode::Hybrid {
                slate_db: db::config::SlateHybridCacheConfig::try_new(
                    env_or("BENCH_BLOCK_CACHE_MB", 512usize) * 1024 * 1024,
                    dir.join("slate"),
                    32 * 1024 * 1024 * 1024,
                )
                .unwrap(),
                object_store: db::config::SlateObjectStoreCacheSettings::try_new(
                    dir.join("object-store"),
                    Some(64 * 1024 * 1024 * 1024),
                    4 * 1024 * 1024,
                    true,
                    db::config::ObjectStoreWarmLevel::Off,
                    None,
                    1_024,
                )
                .unwrap(),
                slate_warm: db::config::SlateWarmConfig::default(),
                fts: Some(
                    db::config::FtsHybridCacheConfig::try_new(
                        256 * 1024 * 1024,
                        dir.join("fts"),
                        8 * 1024 * 1024 * 1024,
                        db::config::FtsWarmConfig::Off,
                        60,
                    )
                    .unwrap(),
                ),
            },
        ));
    }
    let Some(block_mb) = std::env::var("BENCH_BLOCK_CACHE_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return DbConfig::new();
    };
    DbConfig::new().with_cache(db::config::CacheConfig::new(
        db::config::VectorMemorySettings::default(),
        db::config::CacheMode::Memory {
            slate_db: db::config::SlateMemoryCacheConfig::try_new(
                block_mb * 1024 * 1024,
                128 * 1024 * 1024,
            )
            .unwrap(),
            slate_warm: db::config::SlateWarmConfig::default(),
            fts: Some(db::config::FtsMemoryCacheConfig::default()),
        },
    ))
}

fn vector_build() -> fixture::VectorBuild {
    match std::env::var("BENCH_VECTOR").as_deref() {
        Ok("after") => fixture::VectorBuild::After,
        Ok("skip") => fixture::VectorBuild::Skip,
        Ok("before") | Err(_) => fixture::VectorBuild::Before,
        Ok(other) => panic!("BENCH_VECTOR must be before, after or skip, not {other}"),
    }
}

fn percentile(values: &[f64], percentile: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap());
    sorted[(sorted.len() - 1) * percentile / 100]
}

async fn run_queries(label: &str, backend: &Backend) {
    let (count, dimension) = (env_or("BENCH_QUERIES", 10), env_or("BENCH_DIM", 768));
    // `constant` repeats one uniform vector, far from every embedding: the
    // worst case for graph-guided search and a common synthetic benchmark input.
    let vectors = match std::env::var("BENCH_QUERY").as_deref() {
        Ok("constant") => vec![vec![0.001_f32; dimension]; count],
        _ => fixture::query_vectors(count, dimension),
    };
    let recall = std::env::var_os("BENCH_RECALL").is_some();
    let filter = std::env::var("BENCH_SHAPES").ok();
    println!("\n=== {label} ===");
    let count = fixture::shapes(&vectors[0]).len();
    for shape_index in 0..count {
        let (name, scope, _) = fixture::shapes(&vectors[0]).swap_remove(shape_index);
        if filter
            .as_ref()
            .is_some_and(|filter| !name.contains(filter.as_str()))
        {
            continue;
        }
        let truth = match scope {
            Some((scope, k)) if recall => {
                Some((fixture::candidate_embeddings(backend, scope).await, k))
            }
            _ => None,
        };
        let mut latencies = Vec::new();
        let mut recalls = Vec::new();
        let mut gets = Vec::new();
        let mut error = None;
        for vector in &vectors {
            let (_, _, request) = fixture::shapes(vector).swap_remove(shape_index);
            let (gets_before, _) = backend.gets();
            let started = Instant::now();
            let response = backend.query(request).await;
            latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
            gets.push((backend.gets().0 - gets_before) as f64);
            let response = match response {
                Ok(response) => response,
                Err(message) => {
                    error = Some(message);
                    break;
                }
            };
            let Some((candidates, k)) = &truth else {
                continue;
            };
            let returned = response["r"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|row| row["id"].as_u64())
                .collect::<std::collections::HashSet<_>>();
            let expected = fixture::exact_top_k(candidates, vector, *k);
            let hits = expected.iter().filter(|id| returned.contains(id)).count();
            recalls.push(hits as f64 / expected.len().max(1) as f64);
        }
        if let Some(message) = error {
            println!("{name:<36} ERROR {message}");
            continue;
        }
        let warm = &latencies[1.min(latencies.len() - 1)..];
        let recall_text = match recalls.is_empty() {
            true => String::new(),
            false => format!(
                " recall={:.3} candidates={}",
                recalls.iter().sum::<f64>() / recalls.len() as f64,
                truth.as_ref().map_or(0, |(candidates, _)| candidates.len())
            ),
        };
        println!(
            "{name:<36} first={:>8.1}ms p50={:>8.1}ms p95={:>8.1}ms gets_p50={:>6.0}{recall_text}",
            latencies[0],
            percentile(warm, 50),
            percentile(warm, 95),
            percentile(&gets, 50),
        );
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "query".to_string());
    if let Ok(url) = std::env::var("BENCH_HTTP_URL") {
        let backend = Backend::Http {
            client: reqwest::Client::new(),
            url,
        };
        match mode.as_str() {
            "load" => fixture::load(&backend, load_options()).await,
            "index" => fixture::build_vector_index(&backend, env_or("BENCH_DIM", 768)).await,
            _ => run_queries("server", &backend).await,
        }
        return;
    }

    let database = env_or("BENCH_DB", "scoped-search-bench".to_string());
    let store = object_store();
    let writer = HelixDB::open_with_object_store_and_config(
        database.clone(),
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        bench_config(),
    )
    .await
    .unwrap();
    let backend = Backend::Embedded {
        db: writer,
        store: Arc::clone(&store),
    };
    if mode == "load" || mode == "index" {
        match mode.as_str() {
            "load" => fixture::load(&backend, load_options()).await,
            _ => fixture::build_vector_index(&backend, env_or("BENCH_DIM", 768)).await,
        }
        let Backend::Embedded { db, .. } = backend else {
            unreachable!("embedded backend was constructed above")
        };
        db.close().await.unwrap();
        return;
    }

    let Backend::Embedded { db: writer, .. } = &backend else {
        unreachable!("embedded backend was constructed above")
    };
    writer.wait_for_startup_cache_warm().await;
    writer.refresh_vector_memory_cache().await.unwrap();
    run_queries("writer (fresh vector cache)", &backend).await;

    // Any committed write advances the snapshot sequence past the cache's.
    backend
        .query(QueryRequest::write(write_batch().var_as(
            "touch",
            g().add_n("Unrelated", vec![("x", PropertyValue::from(1i64))]),
        )))
        .await
        .unwrap();
    run_queries("writer (after one unrelated write)", &backend).await;

    writer.flush_writer().await.unwrap();
    let reader = HelixDB::open_reader_with_object_store_and_config(
        database,
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        bench_config(),
    )
    .await
    .unwrap();
    reader.wait_for_startup_cache_warm().await;
    let reader = Backend::Embedded { db: reader, store };
    run_queries("reader", &reader).await;
    let (Backend::Embedded { db: reader, .. }, Backend::Embedded { db: writer, .. }) =
        (reader, backend)
    else {
        unreachable!("embedded backends were constructed above")
    };
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
