//! Opt-in recall and throughput evidence for the 10k/100k vector scale gate.
//!
//! The fixture writes only the deployed f32 metadata, SimHash, canonical item,
//! and layer-zero neighbor codecs, then calls the production search façade.
//! Graph construction is intentionally outside the measurement: deterministic
//! logarithmic skip links make setup practical while retaining a real
//! storage-backed traversal. Run this ignored contract in release mode on both
//! the reviewed baseline and the implementation revision, on the same host.
//! Supplying `HELIX_VECTOR_SCALE_BASELINE_NS_10000` and
//! `HELIX_VECTOR_SCALE_BASELINE_NS_100000` on the implementation run turns the
//! reported baseline medians into an enforced 95% throughput floor.

use std::time::Instant;

use slatedb::object_store::memory::InMemory;
use slatedb::IsolationLevel;

use super::{scale_result_id, VectorIndex};
use crate::encoding::v1::keys::vectors::{
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorLayer0NeighborsKey,
};
use crate::encoding::v1::values::vectors::encode_layer0_neighbors;
use crate::search::vector::distance::Cosine;
use crate::search::vector::item::Item;
use crate::search::vector::simhash::{order_code_from_simhash_bits, SimHashCache};
use crate::search::vector::{
    encode_item, encode_metadata, SearchParams, SimHashMode, VectorIndexConfig, VectorIndexMetadata,
};

const DIMENSION: usize = 2;
const RESULT_COUNT: usize = 10;
const QUERY_COUNT: usize = 24;
const MEASUREMENT_ROUNDS: usize = 7;
const WRITE_BATCH_SIZE: u64 = 5_000;

/// One deterministic scale-gate observation emitted for baseline comparison.
#[derive(Debug)]
struct ScaleObservation {
    entity_count: u64,
    recall_at_10: f64,
    median_search_ns: u128,
}

/// Places an entity on a unit circle without retaining the complete fixture.
fn vector_for(entity_id: u64, entity_count: u64) -> [f32; DIMENSION] {
    let angle = std::f64::consts::TAU * entity_id as f64 / entity_count as f64;
    [angle.cos() as f32, angle.sin() as f32]
}

/// Builds sorted, unique, self-free logarithmic links around the circular ID space.
fn skip_neighbors(entity_id: u64, entity_count: u64) -> Vec<u64> {
    let mut neighbors = Vec::new();
    let mut offset = 1_u64;
    while offset < entity_count {
        let forward = (entity_id - 1 + offset) % entity_count + 1;
        let backward = (entity_id - 1 + entity_count - offset % entity_count) % entity_count + 1;
        if forward != entity_id {
            neighbors.push(forward);
        }
        if backward != entity_id {
            neighbors.push(backward);
        }
        let Some(next) = offset.checked_mul(2) else {
            break;
        };
        offset = next;
    }
    neighbors.sort_unstable();
    neighbors.dedup();
    neighbors
}

/// Computes exact current cosine ordering for one deterministic query.
fn exact_neighbors(query: [f32; DIMENSION], entity_count: u64) -> Vec<u64> {
    let mut distances = (1..=entity_count)
        .map(|entity_id| {
            let vector = vector_for(entity_id, entity_count);
            let dot = query[0] * vector[0] + query[1] * vector[1];
            let query_norm = (query[0] * query[0] + query[1] * query[1]).sqrt();
            let vector_norm = (vector[0] * vector[0] + vector[1] * vector[1]).sqrt();
            let cosine = (dot / (query_norm * vector_norm)).clamp(-1.0, 1.0);
            (entity_id, (1.0 - cosine) / 2.0)
        })
        .collect::<Vec<_>>();
    distances.sort_unstable_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .expect("finite scale vectors produce finite distances")
            .then_with(|| left.0.cmp(&right.0))
    });
    distances
        .into_iter()
        .take(RESULT_COUNT)
        .map(|(entity_id, _)| entity_id)
        .collect()
}

/// Seeds current-format rows without measuring graph construction time.
async fn seed_scale_index(entity_count: u64) -> (std::sync::Arc<slatedb::Db>, VectorIndex<Cosine>) {
    let physical_name = format!("vector-scale-{entity_count}");
    let db = std::sync::Arc::new(
        slatedb::Db::open(physical_name.as_str(), std::sync::Arc::new(InMemory::new()))
            .await
            .expect("scale database opens"),
    );
    let index = VectorIndex::<Cosine>::new(&physical_name);
    let config = VectorIndexConfig::new(&physical_name, "embedding", DIMENSION)
        .with_m(32)
        .with_m0(64)
        .with_ef_construction(200);
    let simhash = SimHashCache::new(index.scale_index_id(), DIMENSION);

    let mut first_entity = 1_u64;
    while first_entity <= entity_count {
        let last_entity = entity_count.min(first_entity + WRITE_BATCH_SIZE - 1);
        let txn = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("scale seed transaction begins");
        for entity_id in first_entity..=last_entity {
            let vector = vector_for(entity_id, entity_count);
            let hash = simhash
                .compute_and_cache(&txn, entity_id, &vector)
                .expect("scale SimHash is encoded");
            let item_key = index.vector_key(VectorKey::Vector(VectorItemKey::new(
                index.scale_index_id(),
                order_code_from_simhash_bits(hash.bits()),
                entity_id,
            )));
            txn.put(item_key, encode_item(&Item::<Cosine>::new(vector.to_vec())))
                .expect("scale item row is staged");

            let neighbor_key = index.vector_key(VectorKey::Layer0Neighbors(
                VectorLayer0NeighborsKey::new(index.scale_index_id(), entity_id),
            ));
            txn.put(
                neighbor_key,
                encode_layer0_neighbors(&skip_neighbors(entity_id, entity_count)),
            )
            .expect("scale neighbor row is staged");
        }
        txn.commit().await.expect("scale seed batch commits");
        first_entity = last_entity + 1;
    }

    let metadata_txn = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("scale metadata transaction begins");
    let mut metadata = VectorIndexMetadata::new(config);
    metadata.entry_point = Some(1);
    metadata.count = entity_count;
    let metadata_key = index.vector_key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
        index.scale_index_id(),
    )));
    metadata_txn
        .put(metadata_key, encode_metadata(&metadata))
        .expect("scale metadata is staged");
    metadata_txn.commit().await.expect("scale metadata commits");
    (db, index)
}

/// Runs exact recall and median production-search latency for one fixture size.
async fn observe_scale(entity_count: u64) -> ScaleObservation {
    let (db, index) = seed_scale_index(entity_count).await;
    let txn = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("scale read transaction begins");
    let params = SearchParams::new(RESULT_COUNT)
        .expect("scale result count is nonzero")
        .with_ef(64)
        .expect("scale search beam covers the result count")
        .with_simhash_mode(SimHashMode::Off)
        .with_pre_simhash_sampling_ratio(1.0)
        .expect("scale sampling ratio is valid");
    let queries = (0..QUERY_COUNT)
        .map(|query_index| {
            let entity_id = 1 + query_index as u64 * (entity_count / QUERY_COUNT as u64);
            vector_for(entity_id, entity_count)
        })
        .collect::<Vec<_>>();

    for query in &queries {
        index
            .search(&txn, query, &params)
            .await
            .expect("scale warm-up search succeeds");
    }

    let mut matched = 0_usize;
    let mut observed = 0_usize;
    let mut round_latencies = Vec::with_capacity(MEASUREMENT_ROUNDS);
    for round in 0..MEASUREMENT_ROUNDS {
        let started = Instant::now();
        for query in &queries {
            let results = index
                .search(&txn, query, &params)
                .await
                .expect("scale measured search succeeds");
            if round == 0 {
                let exact = exact_neighbors(*query, entity_count);
                matched += results
                    .iter()
                    .filter(|result| exact.contains(&scale_result_id(result)))
                    .count();
                observed += RESULT_COUNT;
            }
        }
        round_latencies.push(started.elapsed().as_nanos() / QUERY_COUNT as u128);
    }
    round_latencies.sort_unstable();

    ScaleObservation {
        entity_count,
        recall_at_10: matched as f64 / observed as f64,
        median_search_ns: round_latencies[MEASUREMENT_ROUNDS / 2],
    }
}

/// Produces the deterministic 10k/100k evidence used for the VS-15 aggregate gate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "release-only 10k/100k recall and throughput gate"]
async fn vector_search_scale_gate_reports_recall_and_median_throughput() {
    for entity_count in [10_000_u64, 100_000] {
        let observation = observe_scale(entity_count).await;
        assert!(
            observation.recall_at_10 >= 0.995,
            "{}-entity recall@10 {} is below the 99.5% floor implied by a perfect baseline",
            observation.entity_count,
            observation.recall_at_10,
        );
        let baseline_variable = format!("HELIX_VECTOR_SCALE_BASELINE_NS_{entity_count}");
        let baseline_median_ns = std::env::var(&baseline_variable).ok().map(|value| {
            value
                .parse::<u128>()
                .expect("scale baseline median is an integer number of nanoseconds")
        });
        let throughput_ratio = baseline_median_ns
            .map(|baseline| baseline as f64 / observation.median_search_ns as f64);
        if let Some(throughput_ratio) = throughput_ratio {
            assert!(
                throughput_ratio >= 0.95,
                "{}-entity throughput ratio {throughput_ratio:.6} is below 95% of baseline",
                observation.entity_count,
            );
        }
        eprintln!(
            "VECTOR_SCALE entity_count={} recall_at_10={:.6} median_search_ns={} throughput_ratio={}",
            observation.entity_count,
            observation.recall_at_10,
            observation.median_search_ns,
            throughput_ratio
                .map_or_else(|| "not-supplied".to_string(), |ratio| format!("{ratio:.6}")),
        );
    }
}
