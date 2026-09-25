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
//! ```text
//! BENCH_DIR=/tmp/bench BENCH_SCALE=0.02 cargo run --release -p db --example scoped_search_bench -- load
//! BENCH_DIR=/tmp/bench cargo run --release -p db --example scoped_search_bench -- query
//! ```

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use db::{DbConfig, HelixDB};
use futures::stream::BoxStream;
use helix_ast::prelude::*;
use serde_json::Value as JsonValue;
use slatedb::object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, path::Path, CopyOptions, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult,
};

const TOTAL_ATTRIBUTES: f64 = 312_677.0;
const TOTAL_ITEMS: f64 = 4_115.0;
const GROUPS: usize = 25;
const TARGET_GROUP: usize = 3;

/// Object store wrapper counting GETs and optionally injecting per-GET latency.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicU64,
    bytes: AtomicU64,
    latency: Duration,
}

impl CountingStore {
    fn snapshot(&self) -> (u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        if !self.latency.is_zero() {
            tokio::time::sleep(self.latency).await;
        }
        let result = self.inner.get_opts(location, options).await?;
        self.bytes.fetch_add(
            result.range.end.saturating_sub(result.range.start),
            Ordering::Relaxed,
        );
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// SplitMix64: deterministic, dependency-free fixture randomness.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }

    fn gaussian(&mut self) -> f32 {
        let u1 = self.unit().max(f64::MIN_POSITIVE);
        let u2 = self.unit();
        ((-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()) as f32
    }

    fn direction(&mut self, dimension: usize) -> Vec<f32> {
        normalized((0..dimension).map(|_| self.gaussian()).collect())
    }
}

fn normalized(mut vector: Vec<f32>) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    vector.iter_mut().for_each(|value| *value /= norm);
    vector
}

fn mix(parts: &[(&[f32], f32)], dimension: usize) -> Vec<f32> {
    normalized(
        (0..dimension)
            .map(|index| {
                parts
                    .iter()
                    .map(|(vector, weight)| vector[index] * weight)
                    .sum()
            })
            .collect(),
    )
}

/// Attribute kinds with the 72% / 22% / 6% split of the reference workload.
#[derive(Clone, Copy)]
enum Kind {
    A,
    B,
    C,
}

impl Kind {
    const fn name(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
        }
    }
}

struct Fixture {
    dimension: usize,
    kind_centroids: [Vec<f32>; 3],
    group_centroids: Vec<Vec<f32>>,
    topics: Vec<Vec<f32>>,
}

impl Fixture {
    fn new(dimension: usize) -> Self {
        let mut rng = Rng(7);
        Self {
            dimension,
            kind_centroids: [
                rng.direction(dimension),
                rng.direction(dimension),
                rng.direction(dimension),
            ],
            group_centroids: (0..GROUPS).map(|_| rng.direction(dimension)).collect(),
            topics: (0..5_500).map(|_| rng.direction(dimension)).collect(),
        }
    }

    /// Returns `(embedding, text, topic)`; kind B topics are partly group-local.
    fn attribute(&self, rng: &mut Rng, group: usize, kind: Kind) -> (Vec<f32>, String, usize) {
        let noise = rng.direction(self.dimension);
        let (topic, weights) = match kind {
            Kind::A => (rng.below(1_500), [(0, 0.35), (1, 0.8), (2, 0.25), (3, 0.2)]),
            Kind::B if rng.unit() < 0.6 => (
                1_500 + group * 160 + rng.below(160),
                [(0, 0.35), (1, 0.7), (2, 0.35), (3, 0.35)],
            ),
            Kind::B => (
                1_500 + rng.below(4_000),
                [(0, 0.35), (1, 0.7), (2, 0.35), (3, 0.35)],
            ),
            Kind::C => (
                1_500 + rng.below(4_000),
                [(0, 0.4), (1, 0.6), (2, 0.3), (3, 0.35)],
            ),
        };
        let kind_index = kind as usize;
        let parts = [
            &self.kind_centroids[kind_index][..],
            &self.topics[topic][..],
            &self.group_centroids[group][..],
            &noise[..],
        ];
        let vector = mix(
            &weights.map(|(part, weight)| (parts[part], weight)),
            self.dimension,
        );
        (
            vector,
            format!("kind {} topic t{topic} group g{group}", kind.name()),
            topic,
        )
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn group_name(group: usize) -> String {
    format!("group-{group}")
}

/// One query target: an embedded database handle or a server URL.
enum Backend {
    Embedded {
        db: HelixDB,
        store: Arc<CountingStore>,
    },
    Http {
        client: reqwest::Client,
        url: String,
    },
}

impl Backend {
    async fn query(&self, request: QueryRequest) -> Result<JsonValue, String> {
        match self {
            Self::Embedded { db, .. } => db.query(request).await.map_err(|error| error.to_string()),
            Self::Http { client, url } => {
                let response = client
                    .post(format!("{url}/v2/query"))
                    .header("content-type", "application/json")
                    .body(sonic_rs::to_string(&request).map_err(|error| error.to_string())?)
                    .send()
                    .await
                    .map_err(|error| error.to_string())?;
                let status = response.status();
                let body = response.text().await.map_err(|error| error.to_string())?;
                if !status.is_success() {
                    return Err(format!("{status}: {body}"));
                }
                serde_json::from_str(&body).map_err(|error| error.to_string())
            }
        }
    }

    fn gets(&self) -> (u64, u64) {
        match self {
            Self::Embedded { store, .. } => store.snapshot(),
            Self::Http { .. } => (0, 0),
        }
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
    Arc::new(CountingStore {
        inner,
        gets: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        latency: Duration::from_millis(env_or("BENCH_LATENCY_MS", 0)),
    })
}

/// Default config; `BENCH_CACHE_DIR` switches to cloud-like hybrid caches on
/// local disk, `BENCH_BLOCK_CACHE_MB` shrinks the in-memory block cache.
fn bench_config() -> DbConfig {
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

async fn wait_for_operations(backend: &Backend, receipts: &JsonValue, names: &[&str]) {
    for name in names {
        let operation_id = receipts[*name]["operation_id"]
            .as_str()
            .unwrap()
            .to_string();
        let started = Instant::now();
        let mut reported = Instant::now();
        loop {
            let status = backend
                .query(QueryRequest::read(
                    read_batch()
                        .var_as("op", g().get_index_operation(operation_id.clone()))
                        .returning(["op"]),
                ))
                .await
                .unwrap()
                .to_string();
            if status.contains("succeeded") {
                break;
            }
            assert!(
                !status.contains("blocked") && !status.contains("aborted"),
                "index {name} failed: {status}"
            );
            if reported.elapsed() > Duration::from_secs(60) {
                println!(
                    "waiting for {name} ({:.0}s)",
                    started.elapsed().as_secs_f64()
                );
                reported = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn create_indexes(backend: &Backend, names: &[&str], dimension: usize) {
    let batch = names.iter().fold(write_batch(), |batch, name| match *name {
        "vector" => batch.var_as(
            "vector",
            g().create_vector_index_nodes(
                "Attribute",
                "embedding",
                NonZeroUsize::new(dimension).unwrap(),
                VectorDistanceMetric::Cosine,
                None::<String>,
            ),
        ),
        "text" => batch.var_as(
            "text",
            g().create_text_index_nodes("Attribute", "text", None::<String>),
        ),
        "group_name" => batch.var_as(
            "group_name",
            g().create_index_if_not_exists(IndexSpec::node_equality("Group", "name")),
        ),
        "kind" => batch.var_as(
            "kind",
            g().create_index_if_not_exists(IndexSpec::node_equality("Attribute", "kind")),
        ),
        other => panic!("unknown index {other}"),
    });
    let receipts = backend
        .query(QueryRequest::write(batch.returning(names.iter().copied())))
        .await
        .unwrap();
    wait_for_operations(backend, &receipts, names).await;
}

/// When the vector index is built relative to the graph load (`BENCH_VECTOR`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum VectorBuild {
    /// Incremental HNSW inserts during the load (`before`, default).
    Before,
    /// Backfill once the graph is loaded (`after`).
    After,
    /// Graph only; build later with the `index` mode (`skip`).
    Skip,
}

fn vector_build() -> VectorBuild {
    match std::env::var("BENCH_VECTOR").as_deref() {
        Ok("after") => VectorBuild::After,
        Ok("skip") => VectorBuild::Skip,
        Ok("before") | Err(_) => VectorBuild::Before,
        Ok(other) => panic!("BENCH_VECTOR must be before, after or skip, not {other}"),
    }
}

async fn build_vector_index(backend: &Backend, dimension: usize) {
    let started = Instant::now();
    create_indexes(backend, &["vector"], dimension).await;
    println!("vector backfill: {:.0}s", started.elapsed().as_secs_f64());
}

async fn load(backend: &Backend) {
    let scale: f64 = env_or("BENCH_SCALE", 1.0);
    let dimension: usize = env_or("BENCH_DIM", 768);
    let items_per_batch: usize = env_or("BENCH_ITEMS_PER_BATCH", 4);
    let vector = vector_build();
    let fixture = Fixture::new(dimension);
    let item_count = (TOTAL_ITEMS * scale).round() as usize;
    let attributes_per_item = TOTAL_ATTRIBUTES / TOTAL_ITEMS;

    let mut indexes = vec!["text", "group_name", "kind"];
    if vector == VectorBuild::Before {
        indexes.push("vector");
    }
    create_indexes(backend, &indexes, dimension).await;
    let groups = (0..GROUPS).fold(write_batch(), |batch, group| {
        batch.var_as(
            &format!("g{group}"),
            g().add_n("Group", vec![("name", group_name(group))]),
        )
    });
    backend.query(QueryRequest::write(groups)).await.unwrap();

    // Zipf-like group sizes with one large (8%) target group.
    let other_weight = |group: usize| 1.0 / ((group + 2) as f64).powf(0.6);
    let other_total = (0..GROUPS)
        .filter(|group| *group != TARGET_GROUP)
        .map(other_weight)
        .sum::<f64>();
    let weights = (0..GROUPS)
        .map(|group| match group == TARGET_GROUP {
            true => 0.08,
            false => other_weight(group) / other_total * 0.92,
        })
        .collect::<Vec<_>>();
    let mut rng = Rng(42);
    let assigned = (0..item_count)
        .map(|_| {
            let mut draw = rng.unit();
            weights
                .iter()
                .position(|weight| {
                    draw -= weight;
                    draw <= 0.0
                })
                .unwrap_or(GROUPS - 1)
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    let mut attributes = 0usize;
    for (chunk_index, chunk) in assigned.chunks(items_per_batch).enumerate() {
        let mut batch = write_batch();
        for (offset, group) in chunk.iter().copied().enumerate() {
            let owner = format!("item-{:06}", chunk_index * items_per_batch + offset);
            batch = batch
                .var_as(
                    &format!("grp{offset}"),
                    g().n_with_label_where("Group", SourcePredicate::eq("name", group_name(group))),
                )
                .var_as(
                    &format!("i{offset}"),
                    g().add_n("Item", vec![("owner", PropertyValue::from(owner.clone()))]),
                )
                .var_as(
                    &format!("ig{offset}"),
                    g().n(NodeRef::var(format!("i{offset}"))).add_e(
                        "IN_GROUP",
                        NodeRef::var(format!("grp{offset}")),
                        Vec::<(String, PropertyValue)>::new(),
                    ),
                );
            let count = (attributes_per_item * (0.4 + 1.2 * rng.unit())).round() as usize;
            for attribute in 0..count {
                let draw = rng.unit();
                let kind = match draw {
                    draw if draw < 0.72 => Kind::A,
                    draw if draw < 0.94 => Kind::B,
                    _ => Kind::C,
                };
                let (embedding, text, topic) = fixture.attribute(&mut rng, group, kind);
                let name = format!("a{offset}_{attribute}");
                batch = batch
                    .var_as(
                        &name,
                        g().add_n(
                            "Attribute",
                            vec![
                                ("kind", PropertyValue::from(kind.name())),
                                ("owner", PropertyValue::from(owner.clone())),
                                ("topic", PropertyValue::from(topic as i64)),
                                ("text", PropertyValue::from(text)),
                                ("embedding", PropertyValue::from(embedding)),
                            ],
                        ),
                    )
                    .var_as(
                        &format!("e{offset}_{attribute}"),
                        g().n(NodeRef::var(format!("i{offset}"))).add_e(
                            "HAS_ATTRIBUTE",
                            NodeRef::var(name),
                            Vec::<(String, PropertyValue)>::new(),
                        ),
                    );
                attributes += 1;
            }
        }
        // Background index maintenance can conflict with a load batch; a
        // conflicted transaction is rolled back, so retrying is safe.
        let request = QueryRequest::write(batch);
        for attempt in 1.. {
            match backend.query(request.clone()).await {
                Ok(_) => break,
                Err(error) if error.contains("transaction conflict") && attempt < 20 => {
                    tokio::time::sleep(Duration::from_millis(25 * attempt)).await;
                }
                Err(error) => panic!("load batch {chunk_index} failed: {error}"),
            }
        }
        if chunk_index % 50 == 0 {
            let elapsed = started.elapsed().as_secs_f64();
            println!(
                "loaded {attributes} attributes in {elapsed:.0}s ({:.0}/s)",
                attributes as f64 / elapsed
            );
        }
    }
    println!(
        "graph loaded: {item_count} items, {attributes} attributes in {:.0}s",
        started.elapsed().as_secs_f64()
    );
    if vector == VectorBuild::After {
        build_vector_index(backend, dimension).await;
    }
    if let Backend::Embedded { db, .. } = backend {
        db.flush_writer().await.unwrap();
    }
    println!("load complete in {:.0}s", started.elapsed().as_secs_f64());
}

fn group_scope() -> Traversal<OnNodes> {
    g().n_with_label_where(
        "Group",
        SourcePredicate::eq("name", group_name(TARGET_GROUP)),
    )
    .in_(Some("IN_GROUP"))
    .out(Some("HAS_ATTRIBUTE"))
}

fn distance_projection() -> Vec<PropertyProjection> {
    vec![
        PropertyProjection::renamed("$id", "id"),
        PropertyProjection::new("owner"),
        PropertyProjection::renamed("$distance", "distance"),
    ]
}

fn score_projection() -> Vec<PropertyProjection> {
    vec![
        PropertyProjection::new("owner"),
        PropertyProjection::renamed("$score", "score"),
    ]
}

fn read(traversal: Traversal<Terminal>) -> QueryRequest {
    QueryRequest::read(read_batch().var_as("r", traversal).returning(["r"]))
}

/// One benchmark shape: name, optional recall scope and k, and the request.
type Shape = (
    &'static str,
    Option<(Traversal<OnNodes>, usize)>,
    QueryRequest,
);

fn shapes(query: &[f32]) -> Vec<Shape> {
    let vector = query.to_vec();
    let kind_b = || g().n_with_label_where("Attribute", SourcePredicate::eq("kind", "B"));
    let group_kind_b = || group_scope().where_(Predicate::eq("kind", "B"));
    vec![
        ("group traversal count", None, read(group_scope().count())),
        (
            "group kind-B filter count",
            None,
            read(group_kind_b().count()),
        ),
        (
            "global vector top50",
            None,
            read(
                g().vector_search_nodes("Attribute", "embedding", vector.clone(), 50, None)
                    .project(distance_projection()),
            ),
        ),
        (
            "global bm25 top50",
            None,
            read(
                g().text_search_nodes("Attribute", "text", "kind B topic group", 50, None)
                    .project(score_projection()),
            ),
        ),
        (
            "kind-B vector within top50",
            Some((kind_b(), 50)),
            read(
                kind_b()
                    .vector_search("Attribute", "embedding", vector.clone(), 50, None)
                    .project(distance_projection()),
            ),
        ),
        (
            "group vector within k5",
            Some((group_scope(), 5)),
            read(
                group_scope()
                    .vector_search("Attribute", "embedding", vector.clone(), 5, None)
                    .project(distance_projection()),
            ),
        ),
        (
            "group bm25 within top50",
            None,
            read(
                group_scope()
                    .text_search("Attribute", "text", "kind B topic group", 50, None)
                    .project(score_projection()),
            ),
        ),
        (
            "group+kind-B vector within top50",
            Some((group_kind_b(), 50)),
            read(
                group_kind_b()
                    .vector_search("Attribute", "embedding", vector, 50, None)
                    .project(distance_projection()),
            ),
        ),
    ]
}

fn query_vectors(count: usize) -> Vec<Vec<f32>> {
    let fixture = Fixture::new(env_or("BENCH_DIM", 768));
    let mut rng = Rng(9_001);
    (0..count)
        .map(|index| {
            let group = match index % 2 {
                0 => TARGET_GROUP,
                _ => rng.below(GROUPS),
            };
            fixture.attribute(&mut rng, group, Kind::B).0
        })
        .collect()
}

async fn candidate_embeddings(
    backend: &Backend,
    scope: Traversal<OnNodes>,
) -> Vec<(u64, Vec<f32>)> {
    let response = backend
        .query(read(scope.project(vec![
            PropertyProjection::renamed("$id", "id"),
            PropertyProjection::new("embedding"),
        ])))
        .await
        .unwrap();
    response["r"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let embedding = row["embedding"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_f64().unwrap() as f32)
                .collect();
            (row["id"].as_u64().unwrap(), embedding)
        })
        .collect()
}

fn exact_top_k(candidates: &[(u64, Vec<f32>)], query: &[f32], k: usize) -> Vec<u64> {
    let mut scored = candidates
        .iter()
        .map(|(id, vector)| {
            let dot = vector
                .iter()
                .zip(query)
                .map(|(left, right)| left * right)
                .sum::<f32>();
            let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
            (-(dot / norm), *id)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| left.partial_cmp(right).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn percentile(values: &[f64], percentile: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap());
    sorted[(sorted.len() - 1) * percentile / 100]
}

async fn run_queries(label: &str, backend: &Backend) {
    let vectors = query_vectors(env_or("BENCH_QUERIES", 10));
    let recall = std::env::var_os("BENCH_RECALL").is_some();
    let filter = std::env::var("BENCH_SHAPES").ok();
    println!("\n=== {label} ===");
    let count = shapes(&vectors[0]).len();
    for shape_index in 0..count {
        let (name, scope, _) = shapes(&vectors[0]).swap_remove(shape_index);
        if filter
            .as_ref()
            .is_some_and(|filter| !name.contains(filter.as_str()))
        {
            continue;
        }
        let truth = match scope {
            Some((scope, k)) if recall => Some((candidate_embeddings(backend, scope).await, k)),
            _ => None,
        };
        let mut latencies = Vec::new();
        let mut recalls = Vec::new();
        let mut gets = Vec::new();
        let mut error = None;
        for vector in &vectors {
            let (_, _, request) = shapes(vector).swap_remove(shape_index);
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
            let expected = exact_top_k(candidates, vector, *k);
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
            "load" => load(&backend).await,
            "index" => build_vector_index(&backend, env_or("BENCH_DIM", 768)).await,
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
            "load" => load(&backend).await,
            _ => build_vector_index(&backend, env_or("BENCH_DIM", 768)).await,
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
