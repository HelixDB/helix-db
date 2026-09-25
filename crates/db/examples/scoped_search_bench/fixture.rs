//! Synthetic `Group <- Item -> Attribute` fixture and query shapes shared by
//! the scoped search benchmark CLI and its regression gate.
//!
//! Each consumer uses a subset (the gate never opens S3 or HTTP backends).
#![allow(dead_code)]

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use db::HelixDB;
use futures::stream::BoxStream;
use helix_ast::prelude::*;
use serde_json::Value as JsonValue;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};

pub const TOTAL_ATTRIBUTES: f64 = 312_677.0;
pub const TOTAL_ITEMS: f64 = 4_115.0;
pub const GROUPS: usize = 25;
pub const TARGET_GROUP: usize = 3;

/// Object store wrapper counting GETs and optionally injecting per-GET latency.
#[derive(Debug)]
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicU64,
    bytes: AtomicU64,
    latency: Duration,
}

impl CountingStore {
    pub fn new(inner: Arc<dyn ObjectStore>, latency: Duration) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            latency,
        }
    }

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
pub struct Rng(u64);

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

pub fn normalized(mut vector: Vec<f32>) -> Vec<f32> {
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    vector.iter_mut().for_each(|value| *value /= norm);
    vector
}

pub fn mix(parts: &[(&[f32], f32)], dimension: usize) -> Vec<f32> {
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
pub enum Kind {
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

pub struct Fixture {
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

pub fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

pub fn group_name(group: usize) -> String {
    format!("group-{group}")
}

/// One query target: an embedded database handle or a server URL.
pub enum Backend {
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
    pub async fn query(&self, request: QueryRequest) -> Result<JsonValue, String> {
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

    pub fn gets(&self) -> (u64, u64) {
        match self {
            Self::Embedded { store, .. } => store.snapshot(),
            Self::Http { .. } => (0, 0),
        }
    }
}

pub async fn wait_for_operations(backend: &Backend, receipts: &JsonValue, names: &[&str]) {
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

pub async fn create_indexes(backend: &Backend, names: &[&str], dimension: usize) {
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
pub enum VectorBuild {
    /// Incremental HNSW inserts during the load (`before`, default).
    Before,
    /// Backfill once the graph is loaded (`after`).
    After,
    /// Graph only; build later with the `index` mode (`skip`).
    Skip,
}

pub async fn build_vector_index(backend: &Backend, dimension: usize) {
    let started = Instant::now();
    create_indexes(backend, &["vector"], dimension).await;
    println!("vector backfill: {:.0}s", started.elapsed().as_secs_f64());
}

/// Fixture size and vector-index build order for one load.
#[derive(Clone, Copy)]
pub struct LoadOptions {
    /// Fraction of the 312,677-attribute reference graph.
    pub scale: f64,
    pub dimension: usize,
    pub items_per_batch: usize,
    pub vector: VectorBuild,
}

pub async fn load(backend: &Backend, options: LoadOptions) {
    let LoadOptions {
        scale,
        dimension,
        items_per_batch,
        vector,
    } = options;
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

pub fn group_scope() -> Traversal<OnNodes> {
    g().n_with_label_where(
        "Group",
        SourcePredicate::eq("name", group_name(TARGET_GROUP)),
    )
    .in_(Some("IN_GROUP"))
    .out(Some("HAS_ATTRIBUTE"))
}

pub fn distance_projection() -> Vec<PropertyProjection> {
    vec![
        PropertyProjection::renamed("$id", "id"),
        PropertyProjection::new("owner"),
        PropertyProjection::renamed("$distance", "distance"),
    ]
}

pub fn score_projection() -> Vec<PropertyProjection> {
    vec![
        PropertyProjection::new("owner"),
        PropertyProjection::renamed("$score", "score"),
    ]
}

pub fn read(traversal: Traversal<Terminal>) -> QueryRequest {
    QueryRequest::read(read_batch().var_as("r", traversal).returning(["r"]))
}

/// One benchmark shape: name, optional recall scope and k, and the request.
pub type Shape = (
    &'static str,
    Option<(Traversal<OnNodes>, usize)>,
    QueryRequest,
);

pub fn shapes(query: &[f32]) -> Vec<Shape> {
    let vector = query.to_vec();
    let kind_b = || g().n_with_label_where("Attribute", SourcePredicate::eq("kind", "B"));
    // The same scope written as a label scan followed by a stored-property filter.
    let kind_b_where = || {
        g().n_with_label("Attribute")
            .where_(Predicate::eq("kind", "B"))
    };
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
            "kind-B where vector within top50",
            Some((kind_b_where(), 50)),
            read(
                kind_b_where()
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

pub fn query_vectors(count: usize, dimension: usize) -> Vec<Vec<f32>> {
    let fixture = Fixture::new(dimension);
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

pub async fn candidate_embeddings(
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

pub fn exact_top_k(candidates: &[(u64, Vec<f32>)], query: &[f32], k: usize) -> Vec<u64> {
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
