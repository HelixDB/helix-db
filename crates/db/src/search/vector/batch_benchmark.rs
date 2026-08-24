//! Feature-gated, deterministic batched vector mutation benchmark.
//!
//! Setup, verification, digesting, and recall measurement are outside the
//! timed interval. Each sample stages the requested vectors into one
//! Serializable Snapshot transaction and measures staging and commit
//! separately.

use std::collections::HashSet;
use std::fmt::Write;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use sha2::{Digest, Sha256};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::encoding::v2::keys::indexes::vector::VectorStorageLane;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::DataKey;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::IndexElementKind;

use super::distance::{Cosine, Distance, Euclidean, Manhattan};
use super::{
    benchmark_telemetry_snapshot, reset_benchmark_telemetry, ActiveVectorMutationRuntime, Item,
    SearchParams, SearchResult, SimHasherRegistry, ValidatedVectorGenerationHandle,
    VectorCacheWriteSet, VectorDimension, VectorGenerationIdentity, VectorIndex, VectorIndexConfig,
    VectorMutationBenchmarkTelemetry,
};

const PHYSICAL_NAME: &str = "vector-batch-insert-benchmark";
const PROPERTY_NAME: &str = "embedding";
const RECALL_K: usize = 10;
const DEFAULT_MAX_PAYLOAD_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_MAX_ITEMS: usize = 4_096;
const DEFAULT_MAX_NEIGHBORS: usize = 2_048;
const DEFAULT_MAX_SIMHASHES: usize = 4_096;

/// Reviewed distance kernels selectable by the benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorBatchBenchmarkMetric {
    Cosine,
    Euclidean,
    Manhattan,
}

/// Mutation shape staged by one benchmark transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorBatchBenchmarkWorkload {
    Fresh,
    Replacement,
}

/// One validated deterministic benchmark shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct VectorBatchBenchmarkCase {
    pub batch_size: usize,
    pub initial_count: usize,
    pub dimension: usize,
    pub metric: VectorBatchBenchmarkMetric,
    pub workload: VectorBatchBenchmarkWorkload,
}

/// Feature-gated cache limits used to measure insertion-session counterfactuals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct VectorBatchBenchmarkCacheLimits {
    pub max_payload_bytes: u64,
    pub max_items: usize,
    pub max_neighbors: usize,
    pub max_simhashes: usize,
}

impl Default for VectorBatchBenchmarkCacheLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            max_items: DEFAULT_MAX_ITEMS,
            max_neighbors: DEFAULT_MAX_NEIGHBORS,
            max_simhashes: DEFAULT_MAX_SIMHASHES,
        }
    }
}

impl VectorBatchBenchmarkCacheLimits {
    pub fn try_new(
        max_payload_bytes: u64,
        max_items: usize,
        max_neighbors: usize,
        max_simhashes: usize,
    ) -> Result<Self> {
        if max_payload_bytes == 0 || max_items == 0 || max_neighbors == 0 || max_simhashes == 0 {
            return Err(HelixDbError::Config(
                "vector batch benchmark cache limits must be positive".to_string(),
            ));
        }
        Ok(Self {
            max_payload_bytes,
            max_items,
            max_neighbors,
            max_simhashes,
        })
    }
}

impl VectorBatchBenchmarkCase {
    pub fn try_new(
        batch_size: usize,
        dimension: usize,
        metric: VectorBatchBenchmarkMetric,
        workload: VectorBatchBenchmarkWorkload,
    ) -> Result<Self> {
        Self::try_new_with_initial_count(batch_size, 0, dimension, metric, workload)
    }

    pub fn try_new_with_initial_count(
        batch_size: usize,
        initial_count: usize,
        dimension: usize,
        metric: VectorBatchBenchmarkMetric,
        workload: VectorBatchBenchmarkWorkload,
    ) -> Result<Self> {
        if batch_size == 0 {
            return Err(HelixDbError::Config(
                "vector batch benchmark size must be positive".to_string(),
            ));
        }
        if dimension == 0 {
            return Err(HelixDbError::Config(
                "vector batch benchmark dimension must be positive".to_string(),
            ));
        }
        if initial_count.checked_add(batch_size).is_none() {
            return Err(HelixDbError::Config(
                "vector batch benchmark population size overflowed".to_string(),
            ));
        }
        Ok(Self {
            batch_size,
            initial_count,
            dimension,
            metric,
            workload,
        })
    }
}

/// Metrics from one measured transaction and its untimed correctness oracle.
#[derive(Debug, Clone, Serialize)]
pub struct VectorBatchBenchmarkSample {
    pub case: VectorBatchBenchmarkCase,
    pub cache_limits: VectorBatchBenchmarkCacheLimits,
    pub staging_ns: u64,
    pub commit_ns: u64,
    pub total_ns: u64,
    pub vectors_per_second: f64,
    pub telemetry: VectorMutationBenchmarkTelemetry,
    pub unique_final_rows: u64,
    pub unique_final_bytes: u64,
    pub allocated_calls: u64,
    pub allocated_bytes: u64,
    pub graph_digest: String,
    pub recall: f64,
}

impl VectorBatchBenchmarkSample {
    /// Attaches allocation counts measured by the benchmark executable's allocator.
    pub fn with_allocations(mut self, calls: u64, bytes: u64) -> Self {
        self.allocated_calls = calls;
        self.allocated_bytes = bytes;
        self
    }
}

enum BenchmarkIndex {
    Cosine(VectorIndex<Cosine>),
    Euclidean(VectorIndex<Euclidean>),
    Manhattan(VectorIndex<Manhattan>),
}

impl BenchmarkIndex {
    fn new(metric: VectorBatchBenchmarkMetric, layers: Vec<u16>) -> Result<Self> {
        let map_error = |error: super::randomness::ScriptedLayerSelectorError| {
            HelixDbError::Config(format!("invalid benchmark layer script: {error:?}"))
        };
        match metric {
            VectorBatchBenchmarkMetric::Cosine => VectorIndex::new(PHYSICAL_NAME)
                .with_batch_benchmark_contract(layers)
                .map(Self::Cosine)
                .map_err(map_error),
            VectorBatchBenchmarkMetric::Euclidean => VectorIndex::new(PHYSICAL_NAME)
                .with_batch_benchmark_contract(layers)
                .map(Self::Euclidean)
                .map_err(map_error),
            VectorBatchBenchmarkMetric::Manhattan => VectorIndex::new(PHYSICAL_NAME)
                .with_batch_benchmark_contract(layers)
                .map(Self::Manhattan)
                .map_err(map_error),
        }
    }

    fn index_id(&self) -> u64 {
        match self {
            Self::Cosine(index) => index.id(),
            Self::Euclidean(index) => index.id(),
            Self::Manhattan(index) => index.id(),
        }
    }

    async fn create(&self, transaction: &DbTransaction, dimension: usize) -> Result<()> {
        let config = VectorIndexConfig::new(PHYSICAL_NAME, PROPERTY_NAME, dimension)
            .with_m(16)
            .with_m0(32)
            .with_ef_construction(200);
        match self {
            Self::Cosine(index) => index.create(transaction, config).await,
            Self::Euclidean(index) => index.create(transaction, config).await,
            Self::Manhattan(index) => index.create(transaction, config).await,
        }
    }

    async fn insert(
        &self,
        transaction: &DbTransaction,
        entity_id: u64,
        vector: &[f32],
    ) -> Result<()> {
        match self {
            Self::Cosine(index) => index.insert(transaction, entity_id, vector).await,
            Self::Euclidean(index) => index.insert(transaction, entity_id, vector).await,
            Self::Manhattan(index) => index.insert(transaction, entity_id, vector).await,
        }
    }

    async fn search(
        &self,
        transaction: &DbTransaction,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Result<Vec<SearchResult>> {
        let parameters = SearchParams::new(k)
            .and_then(|parameters| parameters.with_ef(ef))
            .map_err(|error| HelixDbError::Config(error.to_string()))?;
        match self {
            Self::Cosine(index) => index.search(transaction, query, &parameters).await,
            Self::Euclidean(index) => index.search(transaction, query, &parameters).await,
            Self::Manhattan(index) => index.search(transaction, query, &parameters).await,
        }
    }
}

/// Prepared state for one sample. Setup is intentionally outside measurement.
pub struct VectorBatchBenchmarkFixture {
    case: VectorBatchBenchmarkCase,
    cache_limits: VectorBatchBenchmarkCacheLimits,
    db: Arc<Db>,
    index: BenchmarkIndex,
    generation: ValidatedVectorGenerationHandle,
    runtime_layers: Vec<u16>,
    vectors: Vec<Vec<f32>>,
    final_vectors: Vec<Vec<f32>>,
}

impl VectorBatchBenchmarkFixture {
    pub async fn prepare(case: VectorBatchBenchmarkCase) -> Result<Self> {
        Self::prepare_with_cache_limits(case, VectorBatchBenchmarkCacheLimits::default()).await
    }

    pub async fn prepare_with_cache_limits(
        case: VectorBatchBenchmarkCase,
        cache_limits: VectorBatchBenchmarkCacheLimits,
    ) -> Result<Self> {
        let replacement_insertions = match case.workload {
            VectorBatchBenchmarkWorkload::Fresh => 0,
            VectorBatchBenchmarkWorkload::Replacement => case.batch_size,
        };
        let setup_insertions = case
            .initial_count
            .checked_add(replacement_insertions)
            .ok_or_else(|| {
                HelixDbError::Config(
                    "vector batch benchmark setup population overflowed".to_string(),
                )
            })?;
        let layers = (0..setup_insertions.saturating_add(case.batch_size))
            .map(scripted_layer)
            .collect();
        let index = BenchmarkIndex::new(case.metric, layers)?;
        let db = Arc::new(
            Db::open(PHYSICAL_NAME, Arc::new(InMemory::new()))
                .await
                .map_err(HelixDbError::from)?,
        );
        let create = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .map_err(HelixDbError::from)?;
        index.create(&create, case.dimension).await?;
        create.commit().await.map_err(HelixDbError::from)?;

        if setup_insertions > 0 {
            let seed = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .map_err(HelixDbError::from)?;
            for ordinal in 0..case.batch_size {
                let vector = fixed_vector(ordinal, case.dimension, false);
                index
                    .insert(
                        &seed,
                        u64::try_from(ordinal).expect("benchmark ordinal fits u64"),
                        &vector,
                    )
                    .await?;
            }
            seed.commit().await.map_err(HelixDbError::from)?;
        }

        let generation_identity = VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            1,
            PHYSICAL_NAME.to_string(),
            index.index_id(),
            NonZeroU64::MIN,
            1,
            IndexElementKind::Node,
            VectorDimension::try_new(case.dimension)
                .map_err(|error| HelixDbError::Config(error.to_string()))?,
        )
        .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?;
        let generation = match case.metric {
            VectorBatchBenchmarkMetric::Cosine => {
                ValidatedVectorGenerationHandle::create_current::<Cosine>(generation_identity)
            }
            VectorBatchBenchmarkMetric::Euclidean => {
                ValidatedVectorGenerationHandle::create_current::<Euclidean>(generation_identity)
            }
            VectorBatchBenchmarkMetric::Manhattan => {
                ValidatedVectorGenerationHandle::create_current::<Manhattan>(generation_identity)
            }
        }
        .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))?;
        let runtime_layers = (setup_insertions..setup_insertions.saturating_add(case.batch_size))
            .map(scripted_layer)
            .collect();
        let replacement = case.workload == VectorBatchBenchmarkWorkload::Replacement;
        let vectors = (0..case.batch_size)
            .map(|ordinal| {
                fixed_vector(
                    case.initial_count.saturating_add(ordinal),
                    case.dimension,
                    replacement,
                )
            })
            .collect::<Vec<_>>();
        let final_count = case
            .initial_count
            .checked_add(case.batch_size)
            .expect("validated benchmark population fits usize");
        let mut final_vectors = (0..final_count)
            .map(|ordinal| fixed_vector(ordinal, case.dimension, false))
            .collect::<Vec<_>>();
        if replacement {
            for (ordinal, vector) in vectors.iter().enumerate() {
                final_vectors[case.initial_count + ordinal] = vector.clone();
            }
        }
        Ok(Self {
            case,
            cache_limits,
            db,
            index,
            generation,
            runtime_layers,
            vectors,
            final_vectors,
        })
    }

    /// Runs exactly one one-transaction sample.
    pub async fn run_sample(&self) -> Result<VectorBatchBenchmarkSample> {
        reset_benchmark_telemetry();
        let total_started = Instant::now();
        let transaction = self
            .db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .map_err(HelixDbError::from)?;
        let cache_writes = VectorCacheWriteSet::new(Arc::new(SimHasherRegistry::default()));
        let mut runtime = ActiveVectorMutationRuntime::new(
            NonZeroU64::new(self.cache_limits.max_payload_bytes)
                .expect("validated batch benchmark retained-payload limit is non-zero"),
        )
        .with_batch_benchmark_layers(self.runtime_layers.clone())
        .with_batch_benchmark_limits(
            self.cache_limits.max_items,
            self.cache_limits.max_neighbors,
            self.cache_limits.max_simhashes,
        );
        let staging_started = Instant::now();
        for (ordinal, vector) in self.vectors.iter().enumerate() {
            runtime
                .upsert(
                    &transaction,
                    &self.generation,
                    &cache_writes,
                    u64::try_from(self.case.initial_count.saturating_add(ordinal))
                        .expect("benchmark ordinal fits u64"),
                    vector,
                    false,
                )
                .await?;
        }
        runtime.prepare(&transaction).await?;
        let staging = staging_started.elapsed();
        let commit_started = Instant::now();
        transaction.commit().await.map_err(HelixDbError::from)?;
        let commit = commit_started.elapsed();
        let total = total_started.elapsed();
        let telemetry = benchmark_telemetry_snapshot();

        let (unique_final_rows, unique_final_bytes, graph_digest) = self.graph_digest().await?;
        let recall = self.recall().await?;
        let total_seconds = total.as_secs_f64();
        Ok(VectorBatchBenchmarkSample {
            case: self.case,
            cache_limits: self.cache_limits,
            staging_ns: u64::try_from(staging.as_nanos()).unwrap_or(u64::MAX),
            commit_ns: u64::try_from(commit.as_nanos()).unwrap_or(u64::MAX),
            total_ns: u64::try_from(total.as_nanos()).unwrap_or(u64::MAX),
            vectors_per_second: self.case.batch_size as f64 / total_seconds,
            telemetry,
            unique_final_rows,
            unique_final_bytes,
            allocated_calls: 0,
            allocated_bytes: 0,
            graph_digest,
            recall,
        })
    }

    async fn graph_digest(&self) -> Result<(u64, u64, String)> {
        let mut digest = Sha256::new();
        let mut rows = 0_u64;
        let mut encoded_bytes = 0_u64;
        for lane in VectorStorageLane::ALL {
            let prefix = DataKey::data_prefix(
                DataScope::LegacyUnscoped,
                lane.prefix_key(self.index.index_id()).to_bytes(),
            );
            let mut iterator = self
                .db
                .scan_prefix(prefix, ..)
                .await
                .map_err(HelixDbError::from)?;
            while let Some(row) = iterator.next().await.map_err(HelixDbError::from)? {
                rows = rows.checked_add(1).ok_or_else(|| {
                    HelixDbError::InvariantViolation(
                        "benchmark vector row count overflowed".to_string(),
                    )
                })?;
                let row_bytes = row.key.len().checked_add(row.value.len()).ok_or_else(|| {
                    HelixDbError::InvariantViolation(
                        "benchmark vector row byte count overflowed".to_string(),
                    )
                })?;
                encoded_bytes = encoded_bytes
                    .checked_add(u64::try_from(row_bytes).unwrap_or(u64::MAX))
                    .ok_or_else(|| {
                        HelixDbError::InvariantViolation(
                            "benchmark vector byte count overflowed".to_string(),
                        )
                    })?;
                digest.update(
                    u64::try_from(row.key.len())
                        .unwrap_or(u64::MAX)
                        .to_be_bytes(),
                );
                digest.update(&row.key);
                digest.update(
                    u64::try_from(row.value.len())
                        .unwrap_or(u64::MAX)
                        .to_be_bytes(),
                );
                digest.update(&row.value);
            }
        }
        let mut encoded_digest = String::with_capacity(64);
        for byte in digest.finalize() {
            write!(&mut encoded_digest, "{byte:02x}")
                .expect("writing into one in-memory string succeeds");
        }
        Ok((rows, encoded_bytes, encoded_digest))
    }

    async fn recall(&self) -> Result<f64> {
        let k = self.case.batch_size.min(RECALL_K);
        let query = &self.vectors[self.case.batch_size / 2];
        let final_count = self.final_vectors.len();
        let transaction = self
            .db
            .begin(IsolationLevel::Snapshot)
            .await
            .map_err(HelixDbError::from)?;
        let approximate = self
            .index
            .search(&transaction, query, k, final_count.max(k))
            .await?;
        transaction.rollback();
        let approximate = approximate
            .into_iter()
            .map(SearchResult::entity_id)
            .collect::<HashSet<_>>();
        let exact = match self.case.metric {
            VectorBatchBenchmarkMetric::Cosine => {
                exact_ids::<Cosine>(&self.final_vectors, query, k)
            }
            VectorBatchBenchmarkMetric::Euclidean => {
                exact_ids::<Euclidean>(&self.final_vectors, query, k)
            }
            VectorBatchBenchmarkMetric::Manhattan => {
                exact_ids::<Manhattan>(&self.final_vectors, query, k)
            }
        };
        let hits = exact
            .into_iter()
            .filter(|entity_id| approximate.contains(entity_id))
            .count();
        Ok(hits as f64 / k as f64)
    }

    pub async fn close(self) -> Result<()> {
        self.db.close().await.map_err(HelixDbError::from)
    }
}

fn scripted_layer(ordinal: usize) -> u16 {
    if ordinal > 0 && ordinal.is_multiple_of(127) {
        2
    } else if ordinal > 0 && ordinal.is_multiple_of(11) {
        1
    } else {
        0
    }
}

fn fixed_vector(ordinal: usize, dimension: usize, replacement: bool) -> Vec<f32> {
    (0..dimension)
        .map(|component| {
            let seed = ordinal
                .wrapping_add(1)
                .wrapping_mul(1_103)
                .wrapping_add(component.wrapping_add(17).wrapping_mul(2_011))
                .wrapping_add(97);
            let magnitude = ((seed % 2_047) + 1) as f32 / 2_048.0;
            let negate = (component.wrapping_add(ordinal) & 1) == usize::from(replacement);
            if negate {
                -magnitude
            } else {
                magnitude
            }
        })
        .collect()
}

fn exact_ids<D: Distance>(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let query = Item::<D>::new(query.to_vec());
    let mut scored = vectors
        .iter()
        .enumerate()
        .map(|(ordinal, vector)| {
            (
                D::distance(&query, &Item::<D>::new(vector.clone())),
                u64::try_from(ordinal).expect("benchmark ordinal fits u64"),
            )
        })
        .collect::<Vec<_>>();
    scored.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    scored
        .into_iter()
        .take(k)
        .map(|(_, entity_id)| entity_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn populated_case_rejects_total_count_overflow() {
        let result = VectorBatchBenchmarkCase::try_new_with_initial_count(
            2,
            usize::MAX,
            128,
            VectorBatchBenchmarkMetric::Cosine,
            VectorBatchBenchmarkWorkload::Fresh,
        );

        assert!(matches!(result, Err(HelixDbError::Config(_))));
    }

    #[test]
    fn cache_limits_reject_zero() {
        let result = VectorBatchBenchmarkCacheLimits::try_new(0, 1, 1, 1);

        assert!(matches!(result, Err(HelixDbError::Config(_))));
    }
}
