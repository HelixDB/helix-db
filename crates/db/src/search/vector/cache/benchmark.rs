//! Fixture for resident vector-memory hydration benchmarks (`bench-internals`).
//!
//! The fixture writes an HNSW-shaped vector-memory prefix (upper neighbors,
//! SimHashes, upper vectors, and skipped layer-0 rows) with the production
//! codecs into a SlateDB on local disk, then reopens it so hydration reads the
//! rows from SSTs exactly as a serving process would.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use slatedb::config::Settings;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::{Db, WriteBatch};

use super::store::{VectorMemoryAdmissionBudget, VectorMemoryStore};
use crate::encoding::keys::scope::DataScope;
use crate::encoding::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::keys::indexes::vector::{
    VectorKey, VectorLayer0NeighborsKey, VectorSimHashKey, VectorUpperNeighborsKey,
    VectorUpperVectorKey,
};
use crate::encoding::v2::values::indexes::vector::item::encode_item_parts;
use crate::encoding::v2::values::indexes::vector::layer0::encode_layer0_neighbors;
use crate::encoding::v2::values::indexes::vector::neighbors::encode_upper_neighbors;
use crate::encoding::v2::values::indexes::vector::simhash::encode_simhash;
use crate::encoding::NodeId;
use crate::error::HelixDbError;

/// Rows written per SlateDB batch while building the fixture.
const BUILD_BATCH_ROWS: usize = 8_192;

/// Shape of the synthetic HNSW graph whose memory rows the fixture writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorMemoryBenchmarkShape {
    /// Number of indexed nodes.
    pub nodes: u64,
    /// Vector dimensions (f32 components).
    pub dimensions: usize,
    /// HNSW `M`: neighbors per upper layer; layer 0 keeps `2 * M`.
    pub max_neighbors: usize,
}

/// SlateDB holding one index's vector-memory rows.
pub struct VectorMemoryBenchmarkFixture {
    db: Db,
    scope: DataScope,
    index_id: u64,
    upper_nodes: Vec<NodeId>,
    upper_rows: usize,
}

/// One hydrated store with its load results.
pub struct VectorMemoryBenchmarkStore {
    store: VectorMemoryStore,
    /// Bytes the load charged to the admission budget.
    pub charged_bytes: u64,
    /// Rows the load admitted.
    pub loaded_entries: usize,
    /// Wall time of the load.
    pub elapsed: Duration,
}

/// Deterministic SplitMix64 step.
fn split_mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl VectorMemoryBenchmarkFixture {
    /// Writes the fixture under `path` and reopens it with compaction and the
    /// block cache disabled, so every hydration reads SST blocks from disk.
    pub async fn build(
        path: &Path,
        shape: VectorMemoryBenchmarkShape,
    ) -> Result<Self, HelixDbError> {
        let scope = DataScope::LegacyUnscoped;
        let index_id = crate::search::vector::index_id_from_name("vector_memory_benchmark");
        let db = Self::open(path).await?;
        let mut random = 0x5EED_u64;
        let level_scale = 1.0 / (shape.max_neighbors as f64).ln();
        let levels: Vec<u16> = (0..shape.nodes)
            .map(|_| {
                let uniform = ((split_mix(&mut random) >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
                (-uniform.ln() * level_scale).floor().min(16.0) as u16
            })
            .collect();
        let neighbors = |random: &mut u64, count: usize| -> Vec<NodeId> {
            (0..count)
                .map(|_| split_mix(random) % shape.nodes)
                .collect()
        };

        let key = |kind: VectorKey| {
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(kind),
            }
            .to_bytes()
        };
        let mut rows: Vec<(Bytes, Bytes)> = Vec::new();
        let mut upper_nodes = Vec::new();
        let mut upper_rows = 0usize;
        for (node_id, level) in (0..shape.nodes).zip(levels.iter().copied()) {
            for layer in 1..=level {
                rows.push((
                    key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                        index_id, layer, node_id,
                    ))),
                    encode_upper_neighbors(&neighbors(&mut random, shape.max_neighbors))?,
                ));
                upper_rows += 1;
            }
            rows.push((
                key(VectorKey::SimHash(VectorSimHashKey::new(index_id, node_id))),
                Bytes::copy_from_slice(&encode_simhash(split_mix(&mut random))),
            ));
            rows.push((
                key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    index_id, node_id,
                ))),
                encode_layer0_neighbors(&neighbors(&mut random, 2 * shape.max_neighbors)),
            ));
            if level > 0 {
                let payload: Vec<u8> = (0..shape.dimensions)
                    .flat_map(|_| (split_mix(&mut random) as f32 / u64::MAX as f32).to_le_bytes())
                    .collect();
                rows.push((
                    key(VectorKey::UpperVector(VectorUpperVectorKey::new(
                        index_id, node_id,
                    ))),
                    encode_item_parts(&1.0f32.to_le_bytes(), &payload),
                ));
                upper_nodes.push(node_id);
                upper_rows += 1;
            }
            if rows.len() >= BUILD_BATCH_ROWS {
                Self::write(&db, &mut rows).await?;
            }
        }
        Self::write(&db, &mut rows).await?;
        db.close().await?;

        Ok(Self {
            db: Self::open(path).await?,
            scope,
            index_id,
            upper_nodes,
            upper_rows,
        })
    }

    async fn open(path: &Path) -> Result<Db, HelixDbError> {
        let object_store = LocalFileSystem::new_with_prefix(path)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
        Ok(
            Db::builder("vector-memory-benchmark", Arc::new(object_store))
                .with_settings(Settings {
                    compactor_options: None,
                    ..Settings::default()
                })
                .with_db_cache_disabled()
                .build()
                .await?,
        )
    }

    async fn write(db: &Db, rows: &mut Vec<(Bytes, Bytes)>) -> Result<(), HelixDbError> {
        let mut batch = WriteBatch::new();
        for (key, value) in rows.drain(..) {
            batch.put(key, value);
        }
        db.write(batch).await?;
        Ok(())
    }

    /// Nodes that own an upper vector, in ascending id order.
    pub fn upper_nodes(&self) -> &[NodeId] {
        &self.upper_nodes
    }

    /// Upper-neighbor and upper-vector rows in the fixture.
    pub fn upper_rows(&self) -> usize {
        self.upper_rows
    }

    /// Hydrates a fresh unbounded store from the reopened SSTs.
    pub async fn hydrate(&self) -> Result<VectorMemoryBenchmarkStore, HelixDbError> {
        let store = VectorMemoryStore::new(self.scope, self.index_id, u64::MAX);
        let started = Instant::now();
        let summary = store
            .load_descriptor_bound_with_budget(
                &self.db,
                VectorMemoryAdmissionBudget::Unbounded,
                None,
            )
            .await?;
        Ok(VectorMemoryBenchmarkStore {
            store,
            charged_bytes: summary.estimated_bytes,
            loaded_entries: summary.loaded_entries,
            elapsed: started.elapsed(),
        })
    }

    /// Closes the reopened database.
    pub async fn close(self) -> Result<(), HelixDbError> {
        Ok(self.db.close().await?)
    }
}

impl VectorMemoryBenchmarkStore {
    /// Reads each node's upper vector, layer-1 neighbors, and SimHash once;
    /// returns the bytes read so callers can keep the work observable.
    pub fn lookup(&self, nodes: &[NodeId]) -> usize {
        nodes
            .iter()
            .map(|&node_id| {
                self.store
                    .get_upper_vector(node_id)
                    .map_or(0, |row| row.len())
                    + self
                        .store
                        .get_upper_neighbors_bytes(1, node_id)
                        .map_or(0, |row| row.len())
                    + usize::from(self.store.get_simhash(node_id).is_some())
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::{VectorMemoryBenchmarkFixture, VectorMemoryBenchmarkShape};

    /// Exercises the vector-memory hydration harness end to end: a fixture
    /// large enough to flush mid-build, a hydration that admits every upper
    /// and SimHash row, and lookups that read each upper node back.
    #[tokio::test]
    async fn hydrates_every_memory_row() {
        let directory = tempfile::tempdir().expect("benchmark directory");
        let shape = VectorMemoryBenchmarkShape {
            nodes: 5_000,
            dimensions: 4,
            max_neighbors: 4,
        };
        let fixture = VectorMemoryBenchmarkFixture::build(directory.path(), shape)
            .await
            .expect("small vector-memory fixture builds");
        assert!(!fixture.upper_nodes().is_empty());
        assert!(fixture.upper_nodes().is_sorted());
        assert!(fixture.upper_rows() > fixture.upper_nodes().len());

        let hydrated = fixture.hydrate().await.expect("fixture hydrates");
        assert_eq!(
            hydrated.loaded_entries,
            fixture.upper_rows() + 5_000,
            "every upper row and SimHash is admitted; layer-0 rows are skipped"
        );
        assert!(hydrated.charged_bytes > 0);
        assert!(!hydrated.elapsed.is_zero(), "the load is timed");
        let vector_bytes = 4 + 4 * 4;
        assert!(
            hydrated.lookup(fixture.upper_nodes()) > fixture.upper_nodes().len() * vector_bytes,
            "every upper node returns its vector, layer-1 neighbors, and SimHash"
        );
        assert_eq!(
            hydrated.lookup(&[u64::MAX]),
            0,
            "unknown nodes read nothing"
        );
        drop(hydrated);
        fixture.close().await.expect("fixture closes");
    }
}
