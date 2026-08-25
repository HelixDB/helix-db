#![cfg_attr(feature = "production-coverage", allow(dead_code))]

use std::collections::HashSet;
#[cfg(test)]
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::time::{Duration, Instant};

#[cfg(test)]
use futures::stream::BoxStream;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
#[cfg(test)]
use slatedb::object_store::{path::Path, Result as ObjectStoreResult};
#[cfg(test)]
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use slatedb::IsolationLevel;

use super::*;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorLayer0NeighborsKey,
    VectorSimHashDirectoryKey, VectorSimHashKey,
};
use crate::encoding::v2::values::indexes::vector::encode_layer0_neighbors;
use crate::encoding::v2::values::indexes::vector::markers::encode_simhash_directory_marker_v1;
use crate::search::vector::distance::{Cosine, Euclidean, Manhattan};
use crate::search::vector::simhash::{order_code_from_simhash_bits, SimHashCache};
use crate::search::vector::{encode_item, encode_metadata, VectorIndexConfig, VectorIndexMetadata};

/// Distance type without an active persisted semantic binding.
#[derive(Debug, Clone)]
enum UnboundRestrictedDistance {}

impl Distance for UnboundRestrictedDistance {
    type Header = ();
    type VectorCodec = f32;

    fn name() -> &'static str {
        "production-unbound-restricted-distance"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {}

    fn distance(_left: &Item<Self>, _right: &Item<Self>) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

impl crate::search::vector::distance::sealed::Sealed for UnboundRestrictedDistance {}

#[derive(Debug, Default)]
#[cfg(test)]
struct CountingObjectStore {
    inner: InMemory,
    gets: AtomicU64,
    bytes: AtomicU64,
}

#[cfg(test)]
impl CountingObjectStore {
    fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.gets.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("counting-memory")
    }
}

#[async_trait::async_trait]
#[cfg(test)]
impl ObjectStore for CountingObjectStore {
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

fn vector_for(entity_id: u64, entity_count: u64, dimension: usize) -> Vec<f32> {
    let angle = std::f64::consts::TAU * entity_id as f64 / entity_count as f64;
    let mut vector = vec![0.0; dimension];
    vector[0] = angle.cos() as f32;
    vector[1] = angle.sin() as f32;
    vector
}

fn skip_neighbors(entity_id: u64, entity_count: u64) -> Vec<u64> {
    let mut neighbors = Vec::new();
    let mut offset = 1_u64;
    while offset < entity_count {
        neighbors.push((entity_id - 1 + offset) % entity_count + 1);
        neighbors.push((entity_id - 1 + entity_count - offset % entity_count) % entity_count + 1);
        let Some(next) = offset.checked_mul(2) else {
            break;
        };
        offset = next;
    }
    neighbors.retain(|neighbor| *neighbor != entity_id);
    neighbors.sort_unstable();
    neighbors.dedup();
    neighbors
}

async fn seed_index(
    name: &str,
    entity_count: u64,
    dimension: usize,
) -> (Arc<slatedb::Db>, VectorIndex<Cosine>) {
    seed_index_on_store(name, entity_count, dimension, Arc::new(InMemory::new())).await
}

async fn seed_index_on_store(
    name: &str,
    entity_count: u64,
    dimension: usize,
    object_store: Arc<dyn ObjectStore>,
) -> (Arc<slatedb::Db>, VectorIndex<Cosine>) {
    let db = Arc::new(slatedb::Db::open(name, object_store).await.unwrap());
    let index = VectorIndex::<Cosine>::new(name).with_simhash_directory();
    let simhash = SimHashCache::new(index.id(), dimension);
    const WRITE_BATCH: u64 = 5_000;
    let mut first = 1_u64;
    while first <= entity_count {
        let last = entity_count.min(first + WRITE_BATCH - 1);
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for entity_id in first..=last {
            let vector = vector_for(entity_id, entity_count, dimension);
            let hash = simhash.compute_and_cache(&txn, entity_id, &vector).unwrap();
            let order_code = order_code_from_simhash_bits(hash.bits());
            txn.put(
                index
                    .row_keyspace()
                    .key(VectorKey::Vector(VectorItemKey::new(
                        index.id(),
                        order_code,
                        entity_id,
                    ))),
                encode_item(&Item::<Cosine>::new(vector)),
            )
            .unwrap();
            txn.put(
                index.row_keyspace().key(VectorKey::SimHashDirectory(
                    VectorSimHashDirectoryKey::new(index.id(), order_code, entity_id),
                )),
                encode_simhash_directory_marker_v1(),
            )
            .unwrap();
            txn.put(
                index.row_keyspace().key(VectorKey::Layer0Neighbors(
                    VectorLayer0NeighborsKey::new(index.id(), entity_id),
                )),
                encode_layer0_neighbors(&skip_neighbors(entity_id, entity_count)),
            )
            .unwrap();
        }
        txn.commit().await.unwrap();
        first = last + 1;
    }

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let mut metadata =
        VectorIndexMetadata::new(VectorIndexConfig::new(name, "embedding", dimension));
    metadata.entry_point = Some(1);
    metadata.count = entity_count;
    txn.put(
        index
            .row_keyspace()
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                index.id(),
            ))),
        encode_metadata(&metadata),
    )
    .unwrap();
    txn.commit().await.unwrap();
    (db, index)
}

async fn seed_empty_graph_directory(
    name: &str,
    entity_count: u64,
    dimension: usize,
) -> (Arc<slatedb::Db>, VectorIndex<Cosine>) {
    let db = Arc::new(
        slatedb::Db::open(name, Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let index = VectorIndex::<Cosine>::new(name).with_simhash_directory();
    let simhash = SimHashCache::new(index.id(), dimension);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for entity_id in 1..=entity_count {
        let mut vector = vec![0.0; dimension];
        vector[0] = 1.0;
        let hash = simhash.compute_and_cache(&txn, entity_id, &vector).unwrap();
        let order_code = order_code_from_simhash_bits(hash.bits());
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Vector(VectorItemKey::new(
                    index.id(),
                    order_code,
                    entity_id,
                ))),
            encode_item(&Item::<Cosine>::new(vector)),
        )
        .unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                    index.id(),
                    order_code,
                    entity_id,
                ))),
            encode_simhash_directory_marker_v1(),
        )
        .unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    index.id(),
                    entity_id,
                ))),
            encode_layer0_neighbors(&[]),
        )
        .unwrap();
    }
    let mut metadata =
        VectorIndexMetadata::new(VectorIndexConfig::new(name, "embedding", dimension));
    metadata.entry_point = Some(1);
    metadata.count = entity_count;
    txn.put(
        index
            .row_keyspace()
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                index.id(),
            ))),
        encode_metadata(&metadata),
    )
    .unwrap();
    txn.commit().await.unwrap();
    (db, index)
}

async fn seed_three_edge_filtered_gulf<D: Distance>(
    name: &str,
) -> (Arc<slatedb::Db>, VectorIndex<D>) {
    let db = Arc::new(
        slatedb::Db::open(name, Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let index = VectorIndex::<D>::new(name);
    let simhash = SimHashCache::new(index.id(), 2);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for (entity_id, vector, neighbors) in [
        (1, vec![0.0, 1.0], vec![2]),
        (2, vec![0.0, 1.0], vec![3]),
        (3, vec![0.0, 1.0], vec![1_001]),
        (1_001, vec![1.0, 0.0], Vec::new()),
    ] {
        let hash = simhash.compute_and_cache(&txn, entity_id, &vector).unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Vector(VectorItemKey::new(
                    index.id(),
                    order_code_from_simhash_bits(hash.bits()),
                    entity_id,
                ))),
            encode_item(&Item::<D>::new(vector)),
        )
        .unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    index.id(),
                    entity_id,
                ))),
            encode_layer0_neighbors(&neighbors),
        )
        .unwrap();
    }
    let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::new(name, "embedding", 2));
    metadata.entry_point = Some(1);
    metadata.count = 4;
    txn.put(
        index
            .row_keyspace()
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                index.id(),
            ))),
        encode_metadata(&metadata),
    )
    .unwrap();
    txn.commit().await.unwrap();
    (db, index)
}

async fn seed_competing_filtered_bridges(name: &str) -> (Arc<slatedb::Db>, VectorIndex<Cosine>) {
    let db = Arc::new(
        slatedb::Db::open(name, Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let index = VectorIndex::<Cosine>::new(name);
    let simhash = SimHashCache::new(index.id(), 2);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for (entity_id, vector, neighbors) in [
        (1, vec![0.0, 1.0], vec![2, 3]),
        (2, vec![1.0, 0.0], vec![1_001]),
        (3, vec![-1.0, 0.0], vec![1_002]),
        (1_001, vec![1.0, 0.0], Vec::new()),
        (1_002, vec![1.0, 0.0], Vec::new()),
    ] {
        let hash = simhash.compute_and_cache(&txn, entity_id, &vector).unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Vector(VectorItemKey::new(
                    index.id(),
                    order_code_from_simhash_bits(hash.bits()),
                    entity_id,
                ))),
            encode_item(&Item::<Cosine>::new(vector)),
        )
        .unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    index.id(),
                    entity_id,
                ))),
            encode_layer0_neighbors(&neighbors),
        )
        .unwrap();
    }
    let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::new(name, "embedding", 2));
    metadata.entry_point = Some(1);
    metadata.count = 5;
    txn.put(
        index
            .row_keyspace()
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                index.id(),
            ))),
        encode_metadata(&metadata),
    )
    .unwrap();
    txn.commit().await.unwrap();
    (db, index)
}

fn exact_ids(
    query: &[f32],
    entity_count: u64,
    dimension: usize,
    allowed: &RestrictedVectorCandidates,
    k: usize,
) -> Vec<u64> {
    let query_vector = UnalignedVector::from_slice(query);
    let query_item = Item::<Cosine> {
        header: Cosine::new_header(&query_vector),
        vector: query_vector,
    };
    let mut distances = allowed
        .iter()
        .filter(|entity_id| *entity_id <= entity_count)
        .map(|entity_id| {
            let vector = vector_for(entity_id, entity_count, dimension);
            let item = Item::<Cosine>::new(vector);
            (entity_id, Cosine::distance(&query_item, &item))
        })
        .collect::<Vec<_>>();
    distances.sort_unstable_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap()
            .then_with(|| left.0.cmp(&right.0))
    });
    distances
        .into_iter()
        .take(k)
        .map(|(entity_id, _)| entity_id)
        .collect()
}

#[cfg_attr(all(test, not(feature = "production-coverage")), test)]
fn admission_bounds_exact_work_by_cardinality_or_bytes() {
    let params = SearchParams::new(10).unwrap();
    let exact = RestrictedVectorCandidates::from_ids(1..=256).unwrap();
    let RestrictedVectorCandidates::NonEmpty(exact) = exact else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    assert!(matches!(
        restricted_execution_plan(&exact, 1_536, &params),
        RestrictedExecutionPlan::Exact { .. }
    ));
    assert!(matches!(
        restricted_execution_plan(&exact, 5_000, &params),
        RestrictedExecutionPlan::FilteredGraph { .. }
    ));
    let filtered = RestrictedVectorCandidates::from_ids(1..=257).unwrap();
    let RestrictedVectorCandidates::NonEmpty(filtered) = filtered else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    assert!(matches!(
        restricted_execution_plan(&filtered, 2, &params),
        RestrictedExecutionPlan::FilteredGraph { .. }
    ));

    let benchmark = RestrictedVectorCandidates::from_ids(1..=1_000).unwrap();
    let RestrictedVectorCandidates::NonEmpty(benchmark) = benchmark else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    assert_eq!(params.ef(), 100);
    let RestrictedExecutionPlan::FilteredGraph { budgets, .. } =
        restricted_execution_plan(&benchmark, 1_536, &params)
    else {
        panic!("1,000 DBpedia vectors use the bounded filtered graph");
    };
    assert_eq!(budgets.ef_filtered, 150);
    assert_eq!(budgets.sampled_seeds, FILTERED_SAMPLED_SEEDS);
    assert_eq!(budgets.directory_seeds, FILTERED_DIRECTORY_SEEDS);
    assert_eq!(budgets.vector_payloads, FILTERED_VECTOR_PAYLOAD_LIMIT);

    for (beam_multiplier, expected_ef) in [(2, 200), (4, 400)] {
        let RestrictedExecutionPlan::FilteredGraph { budgets, .. } =
            restricted_execution_plan_with_beam_multiplier(
                &benchmark,
                1_536,
                &params,
                beam_multiplier,
            )
        else {
            panic!("1,000 DBpedia vectors use the bounded filtered graph");
        };
        assert_eq!(budgets.ef_filtered, expected_ef);
        assert_eq!(budgets.sampled_seeds, FILTERED_SAMPLED_SEEDS);
        assert_eq!(budgets.directory_seeds, FILTERED_DIRECTORY_SEEDS);
        assert_eq!(budgets.vector_payloads, FILTERED_VECTOR_PAYLOAD_LIMIT);
    }
    for (beam_percent, expected_ef) in [(100, 100), (150, 150), (200, 200)] {
        let k = RestrictedResultCount::try_new(params.k(), benchmark.len()).unwrap();
        let RestrictedExecutionPlan::FilteredGraph { budgets, .. } =
            restricted_execution_plan_with_beam_percent(
                &benchmark,
                1_536,
                &params,
                k,
                beam_percent,
            )
        else {
            panic!("1,000 DBpedia vectors use the bounded filtered graph");
        };
        assert_eq!(budgets.ef_filtered, expected_ef);
        assert_eq!(budgets.sampled_seeds, FILTERED_SAMPLED_SEEDS);
        assert_eq!(budgets.directory_seeds, FILTERED_DIRECTORY_SEEDS);
        assert_eq!(budgets.vector_payloads, FILTERED_VECTOR_PAYLOAD_LIMIT);
    }

    let candidates = RestrictedVectorCandidates::from_ids(1..=100_000).unwrap();
    let RestrictedVectorCandidates::NonEmpty(candidates) = candidates else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    let sample = candidates.deterministic_sample_ids(256);
    assert_eq!(sample.len(), 256);
    assert!(sample.windows(2).all(|ids| ids[0] < ids[1]));
    let RestrictedVectorCandidates::NonEmpty(one) =
        RestrictedVectorCandidates::from_ids([7]).unwrap()
    else {
        panic!("one candidate is non-empty");
    };
    assert_eq!(one.deterministic_sample_ids(1), vec![7]);
    let RestrictedVectorCandidates::NonEmpty(two) =
        RestrictedVectorCandidates::from_ids([3, 7]).unwrap()
    else {
        panic!("two candidates are non-empty");
    };
    assert_eq!(two.deterministic_sample_ids(1), vec![3]);
    assert_eq!(two.deterministic_sample_ids(8), vec![3, 7]);
}

#[cfg_attr(all(test, not(feature = "production-coverage")), test)]
fn restricted_result_count_clamps_before_enforcing_the_payload_limit() {
    assert_eq!(
        RestrictedResultCount::try_new(MAX_RESTRICTED_RESULT_COUNT, 1_000)
            .unwrap()
            .get(),
        MAX_RESTRICTED_RESULT_COUNT
    );
    assert_eq!(
        RestrictedResultCount::try_new(MAX_RESTRICTED_RESULT_COUNT + 200, 800)
            .unwrap()
            .get(),
        800
    );
    assert_eq!(
        RestrictedResultCount::try_new(MAX_RESTRICTED_RESULT_COUNT + 1, 1_000),
        Err(VectorParameterError::AboveMaximum {
            parameter: "restricted vector search result count",
            maximum: MAX_RESTRICTED_RESULT_COUNT,
            actual: MAX_RESTRICTED_RESULT_COUNT + 1,
        })
    );

    let params = SearchParams::new(MAX_RESTRICTED_RESULT_COUNT).unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1..=1_000).unwrap();
    let RestrictedVectorCandidates::NonEmpty(candidates) = candidates else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    let RestrictedExecutionPlan::FilteredGraph { k, budgets, .. } =
        restricted_execution_plan(&candidates, 1_536, &params)
    else {
        panic!("1,000 candidates use the bounded filtered graph");
    };
    assert_eq!(k.get(), MAX_RESTRICTED_RESULT_COUNT);
    assert_eq!(budgets.vector_payloads, MAX_RESTRICTED_RESULT_COUNT);
    assert!(budgets.vector_payloads >= k.get());
}

#[cfg_attr(all(test, not(feature = "production-coverage")), test)]
fn candidate_states_deduplicate_reject_overflow_and_keep_empty_explicit() {
    assert!(matches!(
        RestrictedVectorCandidates::from_ids([]).unwrap(),
        RestrictedVectorCandidates::Empty
    ));
    let candidates = RestrictedVectorCandidates::from_ids([7, 7, 3]).unwrap();
    let RestrictedVectorCandidates::NonEmpty(candidates) = candidates else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    assert_eq!(candidates.len(), 2);
    assert!(candidates.contains(3));
    assert!(candidates.contains(7));

    let error = RestrictedVectorCandidates::from_ids(0..=MAX_RESTRICTED_CANDIDATES)
        .expect_err("one million and one unique candidates must be rejected");
    assert!(error.to_string().contains("at most 1000000"));
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn oversized_result_count_rejects_before_index_metadata_io() {
    let db = Arc::new(
        slatedb::Db::open(
            "restricted-result-count-short-circuit",
            Arc::new(InMemory::new()),
        )
        .await
        .unwrap(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let index = VectorIndex::<Cosine>::new("missing-index");
    let candidates =
        RestrictedVectorCandidates::from_ids(1..=MAX_RESTRICTED_RESULT_COUNT as u64 + 1).unwrap();
    let error = index
        .search_restricted_with_stats(
            &txn,
            &[1.0, 0.0],
            &SearchParams::new(MAX_RESTRICTED_RESULT_COUNT + 1).unwrap(),
            &candidates,
        )
        .await
        .expect_err("oversized result count fails before missing metadata is read");

    assert_eq!(
        error.to_string(),
        "Query error: restricted vector search result count must be at most 800, got 801"
    );
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn empty_candidates_short_circuit_before_index_metadata_io() {
    let db = Arc::new(
        slatedb::Db::open("restricted-empty-short-circuit", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let index = VectorIndex::<Cosine>::new("missing-index");
    let candidates = RestrictedVectorCandidates::from_ids([]).unwrap();
    let (results, stats) = index
        .search_restricted_with_stats(
            &txn,
            &[1.0, 0.0],
            &SearchParams::new(10).unwrap(),
            &candidates,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    assert!(stats.strategy.is_none());
    assert_eq!(stats.vector_payload_requests, 0);

    let (results, observed) = observe_restricted_search(index.search_restricted(
        &txn,
        &[1.0, 0.0],
        &SearchParams::new(10).unwrap(),
        &candidates,
    ))
    .await;
    assert!(results.unwrap().is_empty());
    let observed = observed.expect("task-local restricted observation is recorded");
    assert!(observed.strategy.is_none());
    assert_eq!(observed.distance_computations, 0);
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn unbound_metric_rejects_after_metadata_without_vector_reads() {
    let db = Arc::new(
        slatedb::Db::open("restricted-unbound-metric", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let index_name = "restricted-unbound-metric-index";
    let bound = VectorIndex::<Cosine>::new(index_name);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    bound
        .create(&txn, VectorIndexConfig::new(index_name, "embedding", 2))
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let unbound = VectorIndex::<UnboundRestrictedDistance>::new(index_name);
    let candidates = RestrictedVectorCandidates::from_ids([1]).unwrap();
    assert!(matches!(
        unbound
            .search_restricted_with_stats(
                &txn,
                &[1.0, 0.0],
                &SearchParams::new(1).unwrap(),
                &candidates,
            )
            .await,
        Err(HelixDbError::Config(_))
    ));
    txn.rollback();
}

async fn assert_exact_metric<D: Distance>(name: &str) {
    let db = Arc::new(
        slatedb::Db::open(name, Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let index = VectorIndex::<D>::new(name);
    let simhash = SimHashCache::new(index.id(), 2);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for (entity_id, vector) in [
        (2, vec![1.0, 0.0]),
        (1, vec![1.0, 0.0]),
        (3, vec![-1.0, 0.0]),
    ] {
        let hash = simhash.compute_and_cache(&txn, entity_id, &vector).unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Vector(VectorItemKey::new(
                    index.id(),
                    order_code_from_simhash_bits(hash.bits()),
                    entity_id,
                ))),
            encode_item(&Item::<D>::new(vector)),
        )
        .unwrap();
        txn.put(
            index
                .row_keyspace()
                .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    index.id(),
                    entity_id,
                ))),
            encode_layer0_neighbors(&[]),
        )
        .unwrap();
    }
    let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::new(name, "embedding", 2));
    metadata.entry_point = Some(1);
    metadata.count = 3;
    txn.put(
        index
            .row_keyspace()
            .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                index.id(),
            ))),
        encode_metadata(&metadata),
    )
    .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids([3, 2, 1]).unwrap();
    let results = index
        .search_restricted(
            &txn,
            &[1.0, 0.0],
            &SearchParams::new(10).unwrap(),
            &candidates,
        )
        .await
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.entity_id())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn exact_scan_is_correct_and_tie_stable_for_every_active_metric() {
    assert_exact_metric::<Cosine>("restricted-exact-cosine").await;
    assert_exact_metric::<Euclidean>("restricted-exact-euclidean").await;
    assert_exact_metric::<Manhattan>("restricted-exact-manhattan").await;
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn exact_scan_omits_absent_ids_but_rejects_missing_companion_rows() {
    const ENTITY_COUNT: u64 = 32;
    let (db, index) = seed_index("restricted-corruption", ENTITY_COUNT, 8).await;
    let candidates = RestrictedVectorCandidates::from_ids([1, 9_999]).unwrap();
    let query = vector_for(1, ENTITY_COUNT, 8);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let results = index
        .search_restricted(&txn, &query, &SearchParams::new(10).unwrap(), &candidates)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_id(), 1);
    drop(txn);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    txn.delete(
        index
            .row_keyspace()
            .key(VectorKey::SimHash(VectorSimHashKey::new(index.id(), 1))),
    )
    .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let error = index
        .search_restricted(&txn, &query, &SearchParams::new(10).unwrap(), &candidates)
        .await
        .expect_err("a present vector entry without its SimHash must fail closed");
    assert!(error.to_string().contains("missing simhash for node 1"));
    drop(txn);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let simhash = index
        .simhash_cache(8)
        .unwrap()
        .get(&txn, 2)
        .await
        .unwrap()
        .unwrap();
    txn.delete(
        index
            .row_keyspace()
            .key(VectorKey::Vector(VectorItemKey::new(
                index.id(),
                order_code_from_simhash_bits(simhash.bits()),
                2,
            ))),
    )
    .unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids([2]).unwrap();
    let error = index
        .search_restricted(&txn, &query, &SearchParams::new(10).unwrap(), &candidates)
        .await
        .expect_err("a SimHash without its canonical payload must fail closed");
    assert!(error
        .to_string()
        .contains("missing canonical vector payload for node 2"));
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn directory_lifecycle_tracks_insert_upsert_and_delete() {
    let db = Arc::new(
        slatedb::Db::open("restricted-directory-lifecycle", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let index =
        VectorIndex::<Cosine>::new("restricted-directory-lifecycle").with_simhash_directory();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(&txn, VectorIndexConfig::new(index.name(), "embedding", 2))
        .await
        .unwrap();
    index.insert(&txn, 7, &[1.0, 0.0]).await.unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        VectorRows::new(&txn, index.row_keyspace())
            .simhash_directory_window(0, u64::MAX, 10)
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.node_id())
            .collect::<Vec<_>>(),
        vec![7]
    );
    drop(txn);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.insert(&txn, 8, &[0.5, 0.5]).await.unwrap();
    txn.rollback();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        VectorRows::new(&txn, index.row_keyspace())
            .simhash_directory_window(0, u64::MAX, 10)
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.node_id())
            .collect::<Vec<_>>(),
        vec![7]
    );
    drop(txn);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.delete(&txn, 7).await.unwrap();
    txn.rollback();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        VectorRows::new(&txn, index.row_keyspace())
            .simhash_directory_window(0, u64::MAX, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    drop(txn);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.insert(&txn, 7, &[0.0, 1.0]).await.unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        VectorRows::new(&txn, index.row_keyspace())
            .simhash_directory_window(0, u64::MAX, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    drop(txn);

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index.delete(&txn, 7).await.unwrap();
    txn.commit().await.unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    assert!(VectorRows::new(&txn, index.row_keyspace())
        .simhash_directory_window(0, u64::MAX, 10)
        .await
        .unwrap()
        .is_empty());
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn directory_entries_seed_vectors_without_re_reading_point_simhash_rows() {
    const ENTITY_COUNT: u64 = 300;
    let (db, index) =
        seed_empty_graph_directory("restricted-directory-direct-token", ENTITY_COUNT, 8).await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1..=ENTITY_COUNT).unwrap();
    let (results, stats) = index
        .search_restricted_with_stats(
            &txn,
            &vector_for(7, ENTITY_COUNT, 8),
            &SearchParams::new(10).unwrap(),
            &candidates,
        )
        .await
        .unwrap();

    assert!(!results.is_empty());
    assert!(stats.directory_hits >= FILTERED_DIRECTORY_SEEDS);
    assert_eq!(stats.simhash_row_requests, FILTERED_SAMPLED_SEEDS);
    assert!(stats.directory_scan_calls <= DIRECTORY_MAX_PROBES);
    assert_eq!(
        stats.directory_scan_calls % DIRECTORY_MAX_CONCURRENT_SCANS,
        0
    );
    assert!(stats.directory_rows <= DIRECTORY_MAX_ROWS);
    assert!(stats.directory_decoded_bytes <= DIRECTORY_MAX_DECODED_BYTES);
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn directoryless_acorn_crosses_a_three_edge_filtered_gulf_without_nonmember_vectors() {
    let (db, index) = seed_three_edge_filtered_gulf::<Cosine>("restricted-three-edge-gulf").await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1_000..=1_256).unwrap();
    let (results, stats) = index
        .search_restricted_with_stats(
            &txn,
            &[1.0, 0.0],
            &SearchParams::new(10).unwrap(),
            &candidates,
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_id(), 1_001);
    assert_eq!(stats.directory_scan_calls, 0);
    assert_eq!(stats.bridge_rows, 3);
    assert!(stats.bridge_frontier_pushes >= stats.bridge_rows);
    assert_eq!(stats.vector_payload_requests, 1);
    assert_eq!(stats.distance_computations, 1);
    assert!(results
        .iter()
        .all(|result| candidates.contains(result.entity_id())));
    assert_directoryless_filtered_metric::<Euclidean>("restricted-three-edge-gulf-test-euclidean")
        .await;
    assert_directoryless_filtered_metric::<Manhattan>("restricted-three-edge-gulf-test-manhattan")
        .await;
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn directoryless_bridge_missing_simhash_fails_closed() {
    let (db, index) =
        seed_three_edge_filtered_gulf::<Cosine>("restricted-bridge-missing-simhash").await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .delete(
            index
                .row_keyspace()
                .key(VectorKey::SimHash(VectorSimHashKey::new(index.id(), 2))),
        )
        .unwrap();
    transaction.commit().await.unwrap();

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1_000..=1_256).unwrap();
    let error = index
        .search_restricted_with_stats(
            &transaction,
            &[1.0, 0.0],
            &SearchParams::new(10).unwrap(),
            &candidates,
        )
        .await
        .expect_err("a bridge neighbor without its SimHash must fail closed");
    assert!(error.to_string().contains("missing simhash for node 2"));
}

async fn assert_directoryless_filtered_metric<D: Distance>(name: &str) {
    let (db, index) = seed_three_edge_filtered_gulf::<D>(name).await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1_000..=1_256).unwrap();
    let (results, stats) = index
        .search_restricted_with_stats(
            &txn,
            &[1.0, 0.0],
            &SearchParams::new(10).unwrap(),
            &candidates,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entity_id(), 1_001);
    assert_eq!(
        stats.strategy,
        Some(RestrictedSearchStrategy::FilteredGraph)
    );
    assert!(results
        .iter()
        .all(|result| candidates.contains(result.entity_id())));
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn simhash_guides_one_bounded_bridge_toward_the_relevant_disconnected_region() {
    let (db, index) = seed_competing_filtered_bridges("restricted-guided-bridge").await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1_001..=1_257).unwrap();
    let RestrictedVectorCandidates::NonEmpty(candidates) = candidates else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    let vector = UnalignedVector::from_slice(&[1.0, 0.0]);
    let item = Item::<Cosine> {
        header: Cosine::new_header(&vector),
        vector,
    };
    let mut stats = RestrictedSearchStats::default();
    let results = index
        .restricted_filter_aware_search(
            &txn,
            RestrictedQuery {
                vector: &[1.0, 0.0],
                item: &item,
                dimension: VectorDimension::try_new(2).unwrap(),
            },
            FilteredGraphPlan {
                state: VectorIndexState::Populated {
                    entry_point: 1,
                    max_layer: 0,
                },
                k: RestrictedResultCount::try_new(1, candidates.len()).unwrap(),
                budgets: FilteredGraphBudgets {
                    ef_filtered: 1,
                    routing_rows: 2,
                    bridge_rows: 2,
                    vector_payloads: 1,
                    sampled_seeds: 0,
                    directory_seeds: 0,
                },
                allowed: &candidates,
            },
            &mut stats,
        )
        .await
        .unwrap();

    assert_eq!(
        results
            .iter()
            .map(|result| result.entity_id())
            .collect::<Vec<_>>(),
        vec![1_001]
    );
    assert_eq!(stats.bridge_rows, 2);
    assert_eq!(stats.vector_payload_requests, 1);
    assert_eq!(stats.distance_computations, 1);
    assert!(stats.bridge_frontier_pushes >= 3);
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn explicit_filtered_budgets_record_the_exact_termination_reason() {
    let (db, index) =
        seed_three_edge_filtered_gulf::<Cosine>("restricted-budget-termination").await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let candidates = RestrictedVectorCandidates::from_ids(1_000..=1_256).unwrap();
    let RestrictedVectorCandidates::NonEmpty(candidates) = candidates else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    let vector = UnalignedVector::from_slice(&[1.0, 0.0]);
    let item = Item::<Cosine> {
        header: Cosine::new_header(&vector),
        vector,
    };
    let query = RestrictedQuery {
        vector: &[1.0, 0.0],
        item: &item,
        dimension: VectorDimension::try_new(2).unwrap(),
    };
    let state = VectorIndexState::Populated {
        entry_point: 1,
        max_layer: 0,
    };

    for (budgets, expected) in [
        (
            FilteredGraphBudgets {
                ef_filtered: 1,
                routing_rows: 0,
                bridge_rows: 1,
                vector_payloads: 1,
                sampled_seeds: 0,
                directory_seeds: 0,
            },
            RestrictedSearchTermination::RoutingBudget,
        ),
        (
            FilteredGraphBudgets {
                ef_filtered: 1,
                routing_rows: 4,
                bridge_rows: 0,
                vector_payloads: 1,
                sampled_seeds: 0,
                directory_seeds: 0,
            },
            RestrictedSearchTermination::BridgeBudget,
        ),
        (
            FilteredGraphBudgets {
                ef_filtered: 1,
                routing_rows: 4,
                bridge_rows: 2,
                vector_payloads: 0,
                sampled_seeds: 0,
                directory_seeds: 0,
            },
            RestrictedSearchTermination::VectorBudget,
        ),
    ] {
        let mut stats = RestrictedSearchStats::default();
        let results = index
            .restricted_filter_aware_search(
                &txn,
                RestrictedQuery {
                    vector: query.vector,
                    item: query.item,
                    dimension: query.dimension,
                },
                FilteredGraphPlan {
                    state,
                    k: RestrictedResultCount::try_new(1, candidates.len()).unwrap(),
                    budgets,
                    allowed: &candidates,
                },
                &mut stats,
            )
            .await
            .unwrap();
        assert!(results.is_empty());
        assert_eq!(stats.termination, Some(expected));
        assert!(stats.routing_rows <= budgets.routing_rows);
        assert!(stats.bridge_rows <= budgets.bridge_rows);
        assert!(stats.vector_payload_requests <= budgets.vector_payloads);
    }

    let direct = RestrictedVectorCandidates::from_ids(1..=257).unwrap();
    let RestrictedVectorCandidates::NonEmpty(direct) = direct else {
        panic!("non-empty input must produce a non-empty candidate set");
    };
    let budgets = FilteredGraphBudgets {
        ef_filtered: 1,
        routing_rows: 4,
        bridge_rows: 2,
        vector_payloads: 0,
        sampled_seeds: 0,
        directory_seeds: 0,
    };
    let mut stats = RestrictedSearchStats::default();
    let results = index
        .restricted_filter_aware_search(
            &txn,
            RestrictedQuery {
                vector: query.vector,
                item: query.item,
                dimension: query.dimension,
            },
            FilteredGraphPlan {
                state,
                k: RestrictedResultCount::try_new(1, direct.len()).unwrap(),
                budgets,
                allowed: &direct,
            },
            &mut stats,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    assert_eq!(
        stats.termination,
        Some(RestrictedSearchTermination::VectorBudget)
    );
}

#[cfg_attr(all(test, not(feature = "production-coverage")), tokio::test)]
async fn exact_and_filter_aware_paths_enforce_membership_and_recall_budgets() {
    const ENTITY_COUNT: u64 = 512;
    const DIMENSION: usize = 8;
    const K: usize = 10;
    let (db, index) =
        seed_index("restricted-correctness-and-recall", ENTITY_COUNT, DIMENSION).await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let params = SearchParams::new(K).unwrap().with_ef(64).unwrap();

    let small = RestrictedVectorCandidates::from_ids((1..=64).chain([10, 10, 9_999])).unwrap();
    let query = vector_for(17, ENTITY_COUNT, DIMENSION);
    let (exact, exact_stats) = index
        .search_restricted_with_stats(&txn, &query, &params, &small)
        .await
        .unwrap();
    assert_eq!(exact_stats.strategy, Some(RestrictedSearchStrategy::Exact));
    assert!(exact
        .iter()
        .all(|result| small.contains(result.entity_id())));
    assert_eq!(
        exact
            .iter()
            .map(|result| result.entity_id())
            .collect::<Vec<_>>(),
        exact_ids(&query, ENTITY_COUNT, DIMENSION, &small, K)
    );

    let allowed = RestrictedVectorCandidates::from_ids(
        (1..=ENTITY_COUNT).filter(|entity_id| entity_id % 3 != 0),
    )
    .unwrap();
    let mut matched = 0_usize;
    let mut observed = 0_usize;
    for query_id in [1, 43, 87, 129, 211, 307, 401, 509] {
        let query = vector_for(query_id, ENTITY_COUNT, DIMENSION);
        let (results, stats) = index
            .search_restricted_with_stats(&txn, &query, &params, &allowed)
            .await
            .unwrap();
        let exact = exact_ids(&query, ENTITY_COUNT, DIMENSION, &allowed, K);
        let exact = exact.into_iter().collect::<HashSet<_>>();
        matched += results
            .iter()
            .filter(|result| exact.contains(&result.entity_id()))
            .count();
        observed += K;
        assert!(results
            .iter()
            .all(|result| allowed.contains(result.entity_id())));
        assert!(stats.directory_scan_calls <= DIRECTORY_MAX_PROBES);
        assert!(stats.directory_rows <= DIRECTORY_MAX_ROWS);
        assert!(stats.directory_decoded_bytes <= DIRECTORY_MAX_DECODED_BYTES);
        assert!(stats.routing_rows <= stats.ef_filtered * 16);
        assert!(stats.bridge_rows <= stats.ef_filtered * 8);
        assert!(stats.vector_payload_requests <= stats.ef_filtered * 8);
        assert_eq!(stats.distance_computations, stats.vector_payload_requests);
    }
    let recall_at_10 = matched as f64 / observed as f64;
    assert!(recall_at_10 >= 0.95, "recall@10 was {recall_at_10}");
}

#[cfg(feature = "production-coverage")]
pub(crate) async fn run() {
    admission_bounds_exact_work_by_cardinality_or_bytes();
    restricted_result_count_clamps_before_enforcing_the_payload_limit();
    candidate_states_deduplicate_reject_overflow_and_keep_empty_explicit();
    oversized_result_count_rejects_before_index_metadata_io().await;
    empty_candidates_short_circuit_before_index_metadata_io().await;
    unbound_metric_rejects_after_metadata_without_vector_reads().await;
    exact_scan_is_correct_and_tie_stable_for_every_active_metric().await;
    exact_scan_omits_absent_ids_but_rejects_missing_companion_rows().await;
    directory_lifecycle_tracks_insert_upsert_and_delete().await;
    directory_entries_seed_vectors_without_re_reading_point_simhash_rows().await;
    directoryless_acorn_crosses_a_three_edge_filtered_gulf_without_nonmember_vectors().await;
    directoryless_bridge_missing_simhash_fails_closed().await;
    simhash_guides_one_bounded_bridge_toward_the_relevant_disconnected_region().await;
    explicit_filtered_budgets_record_the_exact_termination_reason().await;
    exact_and_filter_aware_paths_enforce_membership_and_recall_budgets().await;
    let (exact_db, exact_index) =
        seed_index("restricted-production-full-exact-batch", 256, 8).await;
    let exact_txn = exact_db.begin(IsolationLevel::Snapshot).await.unwrap();
    let exact_candidates = RestrictedVectorCandidates::from_ids(1..=256).unwrap();
    let (exact_results, exact_stats) = exact_index
        .search_restricted_with_stats(
            &exact_txn,
            &vector_for(1, 256, 8),
            &SearchParams::new(10).unwrap(),
            &exact_candidates,
        )
        .await
        .unwrap();
    assert_eq!(exact_results.len(), 10);
    assert_eq!(exact_stats.strategy, Some(RestrictedSearchStrategy::Exact));

    assert!(!RestrictedVectorCandidates::Empty.contains(1));
    assert_eq!(RestrictedVectorCandidates::Empty.iter().count(), 0);
    let validation_candidates = RestrictedVectorCandidates::from_ids([1]).unwrap();
    assert!(matches!(
        exact_index
            .search_restricted_with_stats(
                &exact_txn,
                &[1.0, 0.0],
                &SearchParams::new(1).unwrap(),
                &validation_candidates,
            )
            .await,
        Err(HelixDbError::InvalidDimension { .. })
    ));
    let mut non_finite = vec![0.0; 8];
    non_finite[3] = f32::INFINITY;
    assert!(matches!(
        exact_index
            .search_restricted_with_stats(
                &exact_txn,
                &non_finite,
                &SearchParams::new(1).unwrap(),
                &validation_candidates,
            )
            .await,
        Err(HelixDbError::InvalidVectorComponent { index: 3 })
    ));
    assert!(matches!(
        exact_index
            .search_restricted_with_stats(
                &exact_txn,
                &[0.0; 8],
                &SearchParams::new(1).unwrap(),
                &validation_candidates,
            )
            .await,
        Err(HelixDbError::ZeroNormCosineVector)
    ));

    let db = Arc::new(
        slatedb::Db::open(
            "restricted-production-beam-multiplier",
            Arc::new(InMemory::new()),
        )
        .await
        .unwrap(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let index = VectorIndex::<Cosine>::new("missing-index");
    let present_candidates = RestrictedVectorCandidates::from_ids([1]).unwrap();
    assert!(matches!(
        index
            .search_restricted_with_stats(
                &txn,
                &[1.0, 0.0],
                &SearchParams::new(1).unwrap(),
                &present_candidates,
            )
            .await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    let empty_index = VectorIndex::<Cosine>::new("restricted-production-empty-index");
    let empty_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    empty_index
        .create(
            &empty_txn,
            VectorIndexConfig::new(empty_index.name(), "embedding", 2),
        )
        .await
        .unwrap();
    empty_txn.commit().await.unwrap();
    let empty_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let (results, stats) = empty_index
        .search_restricted_with_stats(
            &empty_txn,
            &[1.0, 0.0],
            &SearchParams::new(1).unwrap(),
            &present_candidates,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    assert!(stats.strategy.is_none());

    let RestrictedVectorCandidates::NonEmpty(present_candidates) = present_candidates else {
        panic!("one candidate is non-empty");
    };
    let query_vector = UnalignedVector::from_slice(&[1.0, 0.0]);
    let query_item = Item::<Cosine> {
        header: Cosine::new_header(&query_vector),
        vector: query_vector,
    };
    let mut direct_stats = RestrictedSearchStats::default();
    let direct = empty_index
        .restricted_filter_aware_search(
            &empty_txn,
            RestrictedQuery {
                vector: &[1.0, 0.0],
                item: &query_item,
                dimension: VectorDimension::try_new(2).unwrap(),
            },
            FilteredGraphPlan {
                state: VectorIndexState::Empty,
                k: RestrictedResultCount::try_new(1, present_candidates.len()).unwrap(),
                budgets: FilteredGraphBudgets {
                    ef_filtered: 1,
                    routing_rows: 1,
                    bridge_rows: 1,
                    vector_payloads: 1,
                    sampled_seeds: 0,
                    directory_seeds: 0,
                },
                allowed: &present_candidates,
            },
            &mut direct_stats,
        )
        .await
        .unwrap();
    assert!(direct.is_empty());
}

#[cfg(test)]
fn percentile(mut values: Vec<Duration>, percentile: usize) -> Duration {
    values.sort_unstable();
    values[(values.len() - 1) * percentile / 100]
}

#[derive(Debug, Clone, Copy)]
#[cfg(test)]
enum BenchmarkCandidateShape {
    Random,
    GraphCommunity,
    Clustered,
    QueryAntiCorrelated,
}

#[cfg(test)]
impl BenchmarkCandidateShape {
    const ALL: [Self; 4] = [
        Self::Random,
        Self::GraphCommunity,
        Self::Clustered,
        Self::QueryAntiCorrelated,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::GraphCommunity => "graph-community",
            Self::Clustered => "clustered",
            Self::QueryAntiCorrelated => "query-anti-correlated",
        }
    }

    fn candidates(self, count: u64) -> RestrictedVectorCandidates {
        let ids: Box<dyn Iterator<Item = u64>> = match self {
            Self::Random => {
                let modulus = count * 2;
                let mut stride = 6_364_136_223_846_793_005_u64 % modulus;
                stride |= 1;
                while gcd(stride, modulus) != 1 {
                    stride = (stride + 2) % modulus;
                }
                Box::new((0..count).map(move |rank| (rank * stride) % modulus + 1))
            }
            Self::GraphCommunity => Box::new(1..=count),
            Self::Clustered => Box::new((0..count).map(|rank| {
                const BLOCK_SIZE: u64 = 64;
                let block = rank / BLOCK_SIZE;
                let offset = rank % BLOCK_SIZE;
                block * BLOCK_SIZE * 2 + offset + 1
            })),
            Self::QueryAntiCorrelated => Box::new(count + 1..=count * 2),
        };
        RestrictedVectorCandidates::from_ids(ids).unwrap()
    }

    fn query_id(self, query_index: u64, query_count: u64, candidate_count: u64) -> u64 {
        let query_domain = match self {
            Self::QueryAntiCorrelated => candidate_count,
            Self::Random | Self::GraphCommunity | Self::Clustered => candidate_count * 2,
        };
        1 + query_index * query_domain / query_count
    }
}

#[cfg(test)]
fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

#[cfg(test)]
async fn benchmark_exact_scan(
    index: &VectorIndex<Cosine>,
    txn: &slatedb::DbTransaction,
    query: &[f32],
    dimension: usize,
    candidates: &RestrictedVectorCandidates,
    k: usize,
) -> (Vec<SearchResult>, RestrictedSearchStats) {
    let RestrictedVectorCandidates::NonEmpty(candidates) = candidates else {
        unreachable!("benchmark shapes are non-empty");
    };
    let query_vector = UnalignedVector::from_slice(query);
    let query_item = Item::<Cosine> {
        header: Cosine::new_header(&query_vector),
        vector: query_vector,
    };
    let mut stats = RestrictedSearchStats::default();
    let k = RestrictedResultCount::try_new(k, candidates.len()).unwrap();
    let results = index
        .restricted_exact_scan(
            txn,
            &query_item,
            VectorDimension::try_new(dimension).unwrap(),
            k,
            candidates,
            &mut stats,
        )
        .await
        .unwrap();
    (results, stats)
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    test,
    ignore = "release-only restricted-search accuracy, tail-latency, and I/O gate"
)]
async fn restricted_search_scale_gate_reports_accuracy_latency_and_io() {
    let entity_counts = std::env::var("HELIX_RESTRICTED_SCALE_COUNTS")
        .unwrap_or_else(|_| "100,1000,10000,100000,1000000".to_string());
    let dimensions = std::env::var("HELIX_RESTRICTED_SCALE_DIMENSIONS")
        .unwrap_or_else(|_| "128,768,1536".to_string());
    let query_count = std::env::var("HELIX_RESTRICTED_SCALE_QUERIES")
        .map_or(24, |value| value.parse::<u64>().unwrap());
    let beam_multipliers = std::env::var("HELIX_RESTRICTED_BEAM_MULTIPLIERS")
        .unwrap_or_else(|_| "2,4".to_string())
        .split(',')
        .map(|value| value.parse::<usize>().unwrap())
        .collect::<Vec<_>>();
    assert!(query_count > 0);
    assert!(beam_multipliers.iter().all(|multiplier| *multiplier > 0));
    let skip_performance_gates = std::env::var_os("HELIX_RESTRICTED_SKIP_PERF_GATES").is_some();
    for candidate_count in entity_counts
        .split(',')
        .map(|value| value.parse::<u64>().unwrap())
    {
        for dimension in dimensions
            .split(',')
            .map(|value| value.parse::<usize>().unwrap())
        {
            let entity_count = candidate_count * 2;
            let name = format!("restricted-scale-{candidate_count}-{dimension}");
            let object_store = Arc::new(CountingObjectStore::default());
            let (seeded_db, index) =
                seed_index_on_store(&name, entity_count, dimension, object_store.clone()).await;
            seeded_db.close().await.unwrap();
            let params = SearchParams::new(10).unwrap().with_ef(96).unwrap();
            for shape in BenchmarkCandidateShape::ALL {
                let allowed = shape.candidates(candidate_count);
                let first_query = vector_for(
                    shape.query_id(0, query_count, candidate_count),
                    entity_count,
                    dimension,
                );

                for beam_multiplier in beam_multipliers.iter().copied() {
                    let db = Arc::new(
                        slatedb::Db::open(name.as_str(), object_store.clone())
                            .await
                            .unwrap(),
                    );
                    object_store.reset();
                    let cold_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
                    index
                        .search_restricted_with_beam_multiplier(
                            &cold_txn,
                            &first_query,
                            &params,
                            &allowed,
                            beam_multiplier,
                        )
                        .await
                        .unwrap();
                    let (cold_gets, cold_bytes) = object_store.snapshot();
                    drop(cold_txn);

                    object_store.reset();
                    let warm_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
                    index
                        .search_restricted_with_beam_multiplier(
                            &warm_txn,
                            &first_query,
                            &params,
                            &allowed,
                            beam_multiplier,
                        )
                        .await
                        .unwrap();
                    let (warm_gets, warm_bytes) = object_store.snapshot();
                    drop(warm_txn);

                    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
                    let mut filtered_latencies = Vec::new();
                    let mut exact_latencies = Vec::new();
                    let mut matched = 0_usize;
                    let mut observed = 0_usize;
                    let mut filtered_vector_bytes = 0_usize;
                    let mut exact_vector_bytes = 0_usize;
                    let mut logical_rows = 0_usize;
                    let mut multi_get_calls = 0_usize;
                    let mut scan_calls = 0_usize;
                    let mut directory_rows = 0_usize;
                    let mut directory_hits = 0_usize;
                    let mut routing_rows = 0_usize;
                    let mut bridge_rows = 0_usize;
                    let mut bridge_frontier_pushes = 0_usize;
                    let mut scored_candidates = 0_usize;
                    let mut vector_payload_requests = 0_usize;
                    let mut termination_counts = [0_usize; 6];
                    for query_index in 0..query_count {
                        let query_id = shape.query_id(query_index, query_count, candidate_count);
                        let query = vector_for(query_id, entity_count, dimension);

                        let exact_started = Instant::now();
                        let (exact, exact_stats) = benchmark_exact_scan(
                            &index,
                            &txn,
                            &query,
                            dimension,
                            &allowed,
                            params.k(),
                        )
                        .await;
                        exact_latencies.push(exact_started.elapsed());

                        let filtered_started = Instant::now();
                        let (results, stats) = index
                            .search_restricted_with_beam_multiplier(
                                &txn,
                                &query,
                                &params,
                                &allowed,
                                beam_multiplier,
                            )
                            .await
                            .unwrap();
                        filtered_latencies.push(filtered_started.elapsed());

                        assert_eq!(results.len(), params.k());
                        assert!(results
                            .iter()
                            .all(|result| allowed.contains(result.entity_id())));
                        assert_eq!(
                            exact
                                .iter()
                                .map(|result| result.entity_id())
                                .collect::<Vec<_>>(),
                            exact_ids(&query, entity_count, dimension, &allowed, params.k()),
                            "exact restricted scan diverged from brute force"
                        );
                        let exact_ids = exact
                            .into_iter()
                            .map(|result| result.entity_id())
                            .collect::<HashSet<_>>();
                        let query_matches = results
                            .iter()
                            .filter(|result| exact_ids.contains(&result.entity_id()))
                            .count();
                        if query_matches < params.k() {
                            eprintln!(
                            "RESTRICTED_VECTOR_RECALL_MISS candidates={candidate_count} dimension={dimension} shape={} query_id={query_id} matches={query_matches} results={:?} exact={exact_ids:?} stats={stats:?}",
                            shape.name(),
                            results
                                .iter()
                                .map(|result| result.entity_id())
                                .collect::<Vec<_>>()
                        );
                        }
                        matched += query_matches;
                        observed += params.k();
                        filtered_vector_bytes =
                            filtered_vector_bytes.saturating_add(stats.vector_bytes);
                        exact_vector_bytes =
                            exact_vector_bytes.saturating_add(exact_stats.vector_bytes);
                        logical_rows = logical_rows
                            .saturating_add(stats.directory_rows)
                            .saturating_add(stats.routing_rows)
                            .saturating_add(stats.bridge_rows)
                            .saturating_add(stats.vector_payload_requests);
                        multi_get_calls = multi_get_calls
                            .saturating_add(stats.simhash_multi_get_calls)
                            .saturating_add(stats.neighbor_multi_get_calls)
                            .saturating_add(stats.vector_multi_get_calls);
                        scan_calls = scan_calls.saturating_add(stats.directory_scan_calls);
                        directory_rows = directory_rows.saturating_add(stats.directory_rows);
                        directory_hits = directory_hits.saturating_add(stats.directory_hits);
                        routing_rows = routing_rows.saturating_add(stats.routing_rows);
                        bridge_rows = bridge_rows.saturating_add(stats.bridge_rows);
                        bridge_frontier_pushes =
                            bridge_frontier_pushes.saturating_add(stats.bridge_frontier_pushes);
                        scored_candidates =
                            scored_candidates.saturating_add(stats.distance_computations);
                        vector_payload_requests =
                            vector_payload_requests.saturating_add(stats.vector_payload_requests);
                        let termination_index = match (stats.strategy, stats.termination) {
                            (Some(RestrictedSearchStrategy::Exact), None) => 5,
                            (
                                Some(RestrictedSearchStrategy::FilteredGraph),
                                Some(RestrictedSearchTermination::Exhausted),
                            ) => 0,
                            (
                                Some(RestrictedSearchStrategy::FilteredGraph),
                                Some(RestrictedSearchTermination::BeamComplete),
                            ) => 1,
                            (
                                Some(RestrictedSearchStrategy::FilteredGraph),
                                Some(RestrictedSearchTermination::RoutingBudget),
                            ) => 2,
                            (
                                Some(RestrictedSearchStrategy::FilteredGraph),
                                Some(RestrictedSearchTermination::BridgeBudget),
                            ) => 3,
                            (
                                Some(RestrictedSearchStrategy::FilteredGraph),
                                Some(RestrictedSearchTermination::VectorBudget),
                            ) => 4,
                            _ => panic!("restricted search records a valid strategy termination"),
                        };
                        termination_counts[termination_index] =
                            termination_counts[termination_index].saturating_add(1);
                        assert!(stats.directory_scan_calls <= DIRECTORY_MAX_PROBES);
                        assert!(stats.directory_rows <= DIRECTORY_MAX_ROWS);
                        assert!(stats.directory_decoded_bytes <= DIRECTORY_MAX_DECODED_BYTES);
                        if stats.strategy == Some(RestrictedSearchStrategy::FilteredGraph) {
                            assert!(stats.routing_rows <= stats.ef_filtered * 16);
                            assert!(stats.bridge_rows <= stats.ef_filtered * 8);
                            assert!(stats.vector_payload_requests <= FILTERED_VECTOR_PAYLOAD_LIMIT);
                        }
                    }
                    drop(txn);
                    db.close().await.unwrap();

                    let recall = matched as f64 / observed as f64;
                    let filtered_p95 = percentile(filtered_latencies.clone(), 95);
                    let exact_p95 = percentile(exact_latencies.clone(), 95);
                    assert!(
                        recall >= 0.95,
                        "recall@10 was {recall} for {} candidates, dimension {dimension}, shape {}",
                        candidate_count,
                        shape.name()
                    );
                    if candidate_count >= 10_000 && !skip_performance_gates {
                        assert!(
                            filtered_p95.saturating_mul(2) <= exact_p95,
                            "filtered p95 must be at least 2x faster than exact scan"
                        );
                        assert!(
                            filtered_vector_bytes.saturating_mul(2) <= exact_vector_bytes,
                            "filtered search must save at least 50% of vector bytes"
                        );
                    }
                    eprintln!(
                    "RESTRICTED_VECTOR_SCALE candidates={candidate_count} dimension={dimension} shape={} beam_multiplier={beam_multiplier} recall_at_10={recall:.6} filtered_p50_us={} filtered_p95_us={} filtered_p99_us={} exact_p95_us={} scored_candidates={scored_candidates} vector_payload_requests={vector_payload_requests} filtered_vector_bytes={filtered_vector_bytes} exact_vector_bytes={exact_vector_bytes} logical_rows={logical_rows} multi_get_calls={multi_get_calls} scan_calls={scan_calls} directory_rows={directory_rows} directory_hits={directory_hits} routing_rows={routing_rows} bridge_rows={bridge_rows} bridge_frontier_pushes={bridge_frontier_pushes} terminations_exhausted_beam_routing_bridge_vector_exact={termination_counts:?} cold_object_gets={cold_gets} cold_object_bytes={cold_bytes} warm_object_gets={warm_gets} warm_object_bytes={warm_bytes}",
                    shape.name(),
                    percentile(filtered_latencies.clone(), 50).as_micros(),
                    filtered_p95.as_micros(),
                    percentile(filtered_latencies, 99).as_micros(),
                    exact_p95.as_micros(),
                );
                }
            }
        }
    }
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(test, ignore = "release-only unfiltered-search regression gate")]
async fn simhash_directory_keeps_unfiltered_search_regression_below_three_percent() {
    let entity_count = std::env::var("HELIX_UNFILTERED_REGRESSION_ENTITIES")
        .map_or(10_000, |value| value.parse::<u64>().unwrap());
    let repetitions = std::env::var("HELIX_UNFILTERED_REGRESSION_QUERIES")
        .map_or(64, |value| value.parse::<u64>().unwrap());
    const DIMENSION: usize = 128;
    let (db, directory_index) =
        seed_index("restricted-unfiltered-regression", entity_count, DIMENSION).await;
    let legacy_index = VectorIndex::<Cosine>::new("restricted-unfiltered-regression");
    let params = SearchParams::new(10).unwrap().with_ef(96).unwrap();
    let warm_query = vector_for(1, entity_count, DIMENSION);
    let warm_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    legacy_index
        .search(&warm_txn, &warm_query, &params)
        .await
        .unwrap();
    directory_index
        .search(&warm_txn, &warm_query, &params)
        .await
        .unwrap();
    drop(warm_txn);

    let mut legacy_elapsed = Duration::ZERO;
    let mut directory_elapsed = Duration::ZERO;
    for query_index in 0..repetitions {
        let query_id = 1 + query_index * entity_count / repetitions;
        let query = vector_for(query_id, entity_count, DIMENSION);
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let (legacy, directory) = if query_index % 2 == 0 {
            let started = Instant::now();
            let legacy = legacy_index.search(&txn, &query, &params).await.unwrap();
            legacy_elapsed += started.elapsed();
            let started = Instant::now();
            let directory = directory_index.search(&txn, &query, &params).await.unwrap();
            directory_elapsed += started.elapsed();
            (legacy, directory)
        } else {
            let started = Instant::now();
            let directory = directory_index.search(&txn, &query, &params).await.unwrap();
            directory_elapsed += started.elapsed();
            let started = Instant::now();
            let legacy = legacy_index.search(&txn, &query, &params).await.unwrap();
            legacy_elapsed += started.elapsed();
            (legacy, directory)
        };
        assert_eq!(
            legacy
                .iter()
                .map(|result| (result.entity_id(), result.score()))
                .collect::<Vec<_>>(),
            directory
                .iter()
                .map(|result| (result.entity_id(), result.score()))
                .collect::<Vec<_>>()
        );
    }
    let regression = directory_elapsed.as_secs_f64() / legacy_elapsed.as_secs_f64() - 1.0;
    eprintln!(
        "UNFILTERED_VECTOR_REGRESSION entities={entity_count} queries={repetitions} legacy_us={} directory_us={} regression={regression:.6}",
        legacy_elapsed.as_micros(),
        directory_elapsed.as_micros(),
    );
    assert!(
        regression <= 0.03,
        "unfiltered search regressed by {:.2}%",
        regression * 100.0
    );
}

#[cfg(test)]
#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    test,
    ignore = "release-only directory mutation-throughput regression gate"
)]
async fn simhash_directory_keeps_mutation_throughput_regression_below_ten_percent() {
    let entity_count = std::env::var("HELIX_DIRECTORY_MUTATION_ENTITIES")
        .map_or(512, |value| value.parse::<u64>().unwrap());
    let legacy_db = Arc::new(
        slatedb::Db::open("restricted-mutation-legacy", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let directory_db = Arc::new(
        slatedb::Db::open("restricted-mutation-directory", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let legacy_index = VectorIndex::<Cosine>::new("restricted-mutation-legacy")
        .with_scripted_layers(vec![0; entity_count as usize])
        .unwrap();
    let directory_index = VectorIndex::<Cosine>::new("restricted-mutation-directory")
        .with_simhash_directory()
        .with_scripted_layers(vec![0; entity_count as usize])
        .unwrap();
    for (db, index) in [
        (&legacy_db, &legacy_index),
        (&directory_db, &directory_index),
    ] {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(&txn, VectorIndexConfig::new(index.name(), "embedding", 8))
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    let mut legacy_elapsed = Duration::ZERO;
    let mut directory_elapsed = Duration::ZERO;
    for entity_id in 1..=entity_count {
        let vector = vector_for(entity_id, entity_count, 8);
        if entity_id % 2 == 0 {
            let txn = legacy_db.begin(IsolationLevel::Snapshot).await.unwrap();
            let started = Instant::now();
            legacy_index.insert(&txn, entity_id, &vector).await.unwrap();
            txn.commit().await.unwrap();
            legacy_elapsed += started.elapsed();

            let txn = directory_db.begin(IsolationLevel::Snapshot).await.unwrap();
            let started = Instant::now();
            directory_index
                .insert(&txn, entity_id, &vector)
                .await
                .unwrap();
            txn.commit().await.unwrap();
            directory_elapsed += started.elapsed();
        } else {
            let txn = directory_db.begin(IsolationLevel::Snapshot).await.unwrap();
            let started = Instant::now();
            directory_index
                .insert(&txn, entity_id, &vector)
                .await
                .unwrap();
            txn.commit().await.unwrap();
            directory_elapsed += started.elapsed();

            let txn = legacy_db.begin(IsolationLevel::Snapshot).await.unwrap();
            let started = Instant::now();
            legacy_index.insert(&txn, entity_id, &vector).await.unwrap();
            txn.commit().await.unwrap();
            legacy_elapsed += started.elapsed();
        }
    }
    let regression = directory_elapsed.as_secs_f64() / legacy_elapsed.as_secs_f64() - 1.0;
    eprintln!(
        "VECTOR_MUTATION_REGRESSION entities={entity_count} legacy_us={} directory_us={} regression={regression:.6}",
        legacy_elapsed.as_micros(),
        directory_elapsed.as_micros(),
    );
    assert!(
        regression <= 0.10,
        "directory mutation throughput regressed by {:.2}%",
        regression * 100.0
    );
}
