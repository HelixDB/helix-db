//! Fixed-shape secondary-equality read/write benchmark support.
//!
//! Index creation and physical plan construction finish before the measured
//! phase. Each measured read still acquires its production request catalog and
//! stable storage view. The fixture deliberately maps every indexed field to
//! one shared value so V3 creates one entity-suffixed row per index and entity,
//! while bitmap formats collapse the same logical state to 50 rows.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use helix_ast::prelude::*;
use helix_ast::value::PropertyValue as PlannerPropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};
use serde::Serialize;

use crate::config::SecondaryIndexDefinition;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::SecondaryEqualityBitmapValue;
use crate::execution::interpreter::{ExecutionScalar, ExecutionValue};
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::{HelixDB, HelixDbSource, HelixStorage, Result};

const INDEX_COUNT: usize = 50;
const ENTITY_COUNT: usize = 1_000;
const CORRECTNESS_ENTITY_COUNT: usize = 100;
const READ_SCALE_ENTITY_COUNT: usize = 10_000;
const CONCURRENT_WRITERS: usize = 32;
const READ_OPERATIONS: NonZeroUsize =
    NonZeroUsize::new(1_000).expect("read operation count is positive");
const CONCURRENT_READERS: usize = 32;
const MILLION_BITMAP_CARDINALITY: u64 = 1_000_000;
const LABEL: &str = "SecondaryEqualityHotPathNode";
const SHARED_VALUE: &str = "shared";

/// One insertion scheduling mode in the fixed hot-path workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryEqualityInsertMode {
    /// One writer executes every transaction in order.
    Sequential,
    /// Thirty-two writers execute disjoint logical insert counts concurrently.
    Concurrent,
}

/// One lookup scheduling mode over the populated shared-value bitmap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryEqualityReadMode {
    /// One reader executes every lookup in order.
    Sequential,
    /// Thirty-two readers execute disjoint lookup counts concurrently.
    Concurrent,
}

/// Measured insertion outcome, excluding database and index setup.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SecondaryEqualityInsertSample {
    pub mode: SecondaryEqualityInsertMode,
    pub indexes: usize,
    pub entities: usize,
    pub writers: usize,
    pub elapsed_nanos: u128,
    pub throughput_per_second: f64,
    pub median_latency_nanos: u64,
    pub p95_latency_nanos: u64,
    pub conflicts: u64,
    pub retries: u64,
    pub allocations: u64,
    pub allocated_bytes: u64,
}

impl SecondaryEqualityInsertSample {
    /// Attaches process-global allocator observations captured by the harness.
    pub fn with_allocations(mut self, allocations: u64, allocated_bytes: u64) -> Self {
        self.allocations = allocations;
        self.allocated_bytes = allocated_bytes;
        self
    }
}

/// Repeated full-cardinality equality lookup outcome.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SecondaryEqualityReadSample {
    pub mode: SecondaryEqualityReadMode,
    pub operations: usize,
    pub readers: usize,
    pub result_count: usize,
    pub elapsed_nanos: u128,
    pub throughput_per_second: f64,
    pub median_latency_nanos: u64,
    pub p95_latency_nanos: u64,
    pub point_reads: u64,
    pub multi_get_calls: u64,
    pub scans: u64,
    pub graph_reads: u64,
    pub allocations: u64,
    pub allocated_bytes: u64,
}

impl SecondaryEqualityReadSample {
    /// Attaches process-global allocator observations captured by the harness.
    pub fn with_allocations(mut self, allocations: u64, allocated_bytes: u64) -> Self {
        self.allocations = allocations;
        self.allocated_bytes = allocated_bytes;
        self
    }
}

/// Post-write structural and read-path observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecondaryEqualityInspection {
    pub physical_secondary_rows: u64,
    pub v3_nonunique_rows: u64,
    pub v4_bitmap_rows: u64,
    pub logical_secondary_bytes: u64,
    pub minimum_bitmap_cardinality: u64,
    pub maximum_bitmap_cardinality: u64,
}

/// Exact I/O observations from checking every configured equality index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecondaryEqualityLookupInspection {
    pub lookups: usize,
    pub result_count: usize,
    pub point_reads: u64,
    pub multi_get_calls: u64,
    pub scans: u64,
    pub graph_reads: u64,
}

/// Portable bitmap size and codec cost for one million sequential IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecondaryEqualityMillionBitmapSample {
    pub cardinality: u64,
    pub encoded_bytes: usize,
    pub encode_nanos: u128,
    pub decode_nanos: u128,
}

/// Prepared empty database with 50 Active nonunique equality indexes.
pub struct SecondaryEqualityHotPathFixture {
    db: Arc<HelixDB>,
    insert_plan: Arc<exec::ExecutablePlan>,
    lookup_plans: Vec<Arc<exec::ExecutablePlan>>,
    entity_count: usize,
}

impl SecondaryEqualityHotPathFixture {
    /// Creates the fixed fixture without including setup in benchmark timings.
    pub async fn open(database: impl Into<String>) -> Result<Self> {
        Self::open_with_entity_count(database, ENTITY_COUNT).await
    }

    /// Creates the ten-thousand-node read-scale fixture.
    pub async fn open_read_scale(database: impl Into<String>) -> Result<Self> {
        Self::open_with_entity_count(database, READ_SCALE_ENTITY_COUNT).await
    }

    /// Creates the bounded 50-index by 100-node correctness fixture.
    pub async fn open_correctness(database: impl Into<String>) -> Result<Self> {
        Self::open_with_entity_count(database, CORRECTNESS_ENTITY_COUNT).await
    }

    async fn open_with_entity_count(
        database: impl Into<String>,
        entity_count: usize,
    ) -> Result<Self> {
        assert_eq!(INDEX_COUNT, 50, "hot-path index shape is frozen");
        assert!(
            matches!(
                entity_count,
                CORRECTNESS_ENTITY_COUNT | ENTITY_COUNT | READ_SCALE_ENTITY_COUNT
            ),
            "hot-path entity shape is frozen"
        );
        assert_eq!(
            CONCURRENT_WRITERS, 32,
            "hot-path concurrency shape is frozen"
        );

        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: database.into(),
            })
            .await?,
        );
        db.wait_for_startup_cache_warm().await;

        for ordinal in 0..INDEX_COUNT {
            let definition =
                SecondaryIndexDefinition::node_equality(LABEL, property_name(ordinal))?;
            db.install_index_for_tests(ValidatedDynamicIndexDefinition::try_from(definition)?)
                .await?;
        }

        let properties = (0..INDEX_COUNT)
            .map(|ordinal| (property_name(ordinal), PropertyInput::from(SHARED_VALUE)))
            .collect();
        let insert = write_batch().var_as("node", g().add_n(LABEL, properties));
        let insert_plan = helix_planner::planning::plan_write_batch(
            &insert,
            &db.planner_context(context::ParamBindings::default()),
        )
        .map_err(|error| crate::HelixDbError::Query(error.to_string()))?;

        Ok(Self {
            db,
            insert_plan: Arc::new(insert_plan),
            lookup_plans: (0..INDEX_COUNT)
                .map(|ordinal| Arc::new(equality_search_plan(ordinal)))
                .collect(),
            entity_count,
        })
    }

    /// Executes the fixed insertion workload, retrying only transaction conflicts.
    pub async fn insert(
        &self,
        mode: SecondaryEqualityInsertMode,
    ) -> Result<SecondaryEqualityInsertSample> {
        match mode {
            SecondaryEqualityInsertMode::Sequential => self.insert_sequential().await,
            SecondaryEqualityInsertMode::Concurrent => self.insert_concurrent().await,
        }
    }

    /// Counts V3 rows, V4 bitmap rows, and their visible logical bytes.
    pub async fn inspect(&self) -> Result<SecondaryEqualityInspection> {
        self.db.flush_writer().await?;
        let HelixStorage::Writer(writer) = self.db.storage() else {
            unreachable!("hot-path benchmark opens a writer")
        };
        let v3_prefix = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::SecondaryEntry),
        );
        let mut v3_rows = writer.db().scan_prefix(&v3_prefix, ..).await?;
        let mut v3_nonunique_rows = 0_u64;
        let mut logical_secondary_bytes = 0_u64;
        while let Some(row) = v3_rows.next().await? {
            v3_nonunique_rows = v3_nonunique_rows.saturating_add(1);
            logical_secondary_bytes = logical_secondary_bytes.saturating_add(
                u64::try_from(row.key.len().saturating_add(row.value.len())).unwrap_or(u64::MAX),
            );
        }

        let v4_prefix = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::SecondaryEqualityBitmap),
        );
        let mut v4_rows = writer.db().scan_prefix(&v4_prefix, ..).await?;
        let mut v4_bitmap_rows = 0_u64;
        let mut minimum_bitmap_cardinality = u64::MAX;
        let mut maximum_bitmap_cardinality = 0_u64;
        while let Some(row) = v4_rows.next().await? {
            v4_bitmap_rows = v4_bitmap_rows.saturating_add(1);
            logical_secondary_bytes = logical_secondary_bytes.saturating_add(
                u64::try_from(row.key.len().saturating_add(row.value.len())).unwrap_or(u64::MAX),
            );
            let cardinality = SecondaryEqualityBitmapValue::decode(&row.value)?
                .ids()
                .len();
            minimum_bitmap_cardinality = minimum_bitmap_cardinality.min(cardinality);
            maximum_bitmap_cardinality = maximum_bitmap_cardinality.max(cardinality);
        }
        if v4_bitmap_rows == 0 {
            minimum_bitmap_cardinality = 0;
        }
        Ok(SecondaryEqualityInspection {
            physical_secondary_rows: v3_nonunique_rows.saturating_add(v4_bitmap_rows),
            v3_nonunique_rows,
            v4_bitmap_rows,
            logical_secondary_bytes,
            minimum_bitmap_cardinality,
            maximum_bitmap_cardinality,
        })
    }

    /// Warms the full-cardinality equality lookup outside the measured pass.
    pub async fn prepare_read(&self) -> Result<()> {
        let _ = self.lookup_result_count().await?;
        Ok(())
    }

    /// Decodes every V4 bitmap row for exact state comparison across writers.
    pub async fn decoded_bitmap_rows(&self) -> Result<Vec<(Vec<u8>, Vec<u64>)>> {
        let HelixStorage::Writer(writer) = self.db.storage() else {
            unreachable!("hot-path correctness fixture opens a writer")
        };
        let prefix = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::SecondaryEqualityBitmap),
        );
        let mut rows = writer.db().scan_prefix(&prefix, ..).await?;
        let mut decoded = Vec::new();
        while let Some(row) = rows.next().await? {
            let ids = SecondaryEqualityBitmapValue::decode(&row.value)?
                .ids()
                .iter()
                .collect();
            decoded.push((row.key.to_vec(), ids));
        }
        Ok(decoded)
    }

    /// Checks all 50 indexes against the exact decoded ID set and records the
    /// complete equality-serving I/O contract.
    pub async fn inspect_all_lookups(&self) -> Result<SecondaryEqualityLookupInspection> {
        let rows = self.decoded_bitmap_rows().await?;
        let Some((_, expected)) = rows.first() else {
            return Err(crate::HelixDbError::InvariantViolation(
                "hot-path correctness fixture contains no bitmap rows".to_string(),
            ));
        };
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        for plan in &self.lookup_plans {
            let mut actual = lookup_node_ids(&self.db, plan).await?;
            actual.sort_unstable();
            assert_eq!(&actual, expected);
        }
        let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
        Ok(SecondaryEqualityLookupInspection {
            lookups: self.lookup_plans.len(),
            result_count: expected.len(),
            point_reads: metrics.point_reads,
            multi_get_calls: metrics.multi_get_calls,
            scans: metrics.scans,
            graph_reads: metrics.graph_reads,
        })
    }

    /// Measures repeated full-cardinality equality lookups after warmup.
    pub async fn read(
        &self,
        mode: SecondaryEqualityReadMode,
    ) -> Result<SecondaryEqualityReadSample> {
        self.read_operations(mode, READ_OPERATIONS).await
    }

    /// Measures a positive number of full-cardinality equality lookups after warmup.
    pub async fn read_operations(
        &self,
        mode: SecondaryEqualityReadMode,
        operations: NonZeroUsize,
    ) -> Result<SecondaryEqualityReadSample> {
        let operations = operations.get();
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        let started = Instant::now();
        let mut latencies = match mode {
            SecondaryEqualityReadMode::Sequential => {
                let mut latencies = Vec::with_capacity(operations);
                for _ in 0..operations {
                    let operation_started = Instant::now();
                    let _ = self.lookup_result_count().await?;
                    latencies.push(duration_nanos(operation_started.elapsed()));
                }
                latencies
            }
            SecondaryEqualityReadMode::Concurrent => {
                let mut tasks = tokio::task::JoinSet::new();
                for reader in 0..CONCURRENT_READERS {
                    let db = Arc::clone(&self.db);
                    let plan = Arc::clone(&self.lookup_plans[0]);
                    tasks.spawn(async move {
                        let mut latencies =
                            Vec::with_capacity(operations.div_ceil(CONCURRENT_READERS));
                        for _ in (reader..operations).step_by(CONCURRENT_READERS) {
                            let operation_started = Instant::now();
                            let _ = lookup_result_count(&db, &plan).await?;
                            latencies.push(duration_nanos(operation_started.elapsed()));
                        }
                        Result::<_>::Ok(latencies)
                    });
                }
                let mut latencies = Vec::with_capacity(operations);
                while let Some(result) = tasks.join_next().await {
                    latencies.append(&mut result.map_err(|error| {
                        crate::HelixDbError::InvariantViolation(format!(
                            "hot-path benchmark reader task failed: {error}"
                        ))
                    })??);
                }
                latencies
            }
        };
        let elapsed = started.elapsed();
        assert_eq!(latencies.len(), operations, "every timed lookup completed");
        latencies.sort_unstable();
        let metrics = crate::index_lifecycle::secondary::equality_read_metrics();
        Ok(SecondaryEqualityReadSample {
            mode,
            operations,
            readers: match mode {
                SecondaryEqualityReadMode::Sequential => 1,
                SecondaryEqualityReadMode::Concurrent => CONCURRENT_READERS,
            },
            result_count: self.entity_count,
            elapsed_nanos: elapsed.as_nanos(),
            throughput_per_second: f64::from(operations as u32) / elapsed.as_secs_f64(),
            median_latency_nanos: percentile(&latencies, 50),
            p95_latency_nanos: percentile(&latencies, 95),
            point_reads: metrics.point_reads,
            multi_get_calls: metrics.multi_get_calls,
            scans: metrics.scans,
            graph_reads: metrics.graph_reads,
            allocations: 0,
            allocated_bytes: 0,
        })
    }

    /// Closes this fixture and its background workers.
    pub async fn close(&self) -> Result<()> {
        self.db.close().await
    }

    async fn insert_sequential(&self) -> Result<SecondaryEqualityInsertSample> {
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(self.entity_count);
        let mut conflicts = 0_u64;
        let mut retries = 0_u64;
        for _ in 0..self.entity_count {
            loop {
                let operation_started = Instant::now();
                match self
                    .db
                    .execute(&self.insert_plan, context::ParamBindings::default())
                    .await
                {
                    Ok(_) => {
                        latencies.push(duration_nanos(operation_started.elapsed()));
                        break;
                    }
                    Err(error) if error.is_transaction_conflict() => {
                        conflicts = conflicts.saturating_add(1);
                        retries = retries.saturating_add(1);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(insert_sample(
            SecondaryEqualityInsertMode::Sequential,
            1,
            started.elapsed(),
            latencies,
            self.entity_count,
            conflicts,
            retries,
        ))
    }

    async fn insert_concurrent(&self) -> Result<SecondaryEqualityInsertSample> {
        let started = Instant::now();
        let entity_count = self.entity_count;
        let mut tasks = tokio::task::JoinSet::new();
        for worker in 0..CONCURRENT_WRITERS {
            let db = Arc::clone(&self.db);
            let plan = Arc::clone(&self.insert_plan);
            tasks.spawn(async move {
                let mut latencies = Vec::with_capacity(entity_count.div_ceil(CONCURRENT_WRITERS));
                let mut conflicts = 0_u64;
                let mut retries = 0_u64;
                for _ in (worker..entity_count).step_by(CONCURRENT_WRITERS) {
                    loop {
                        let operation_started = Instant::now();
                        match db.execute(&plan, context::ParamBindings::default()).await {
                            Ok(_) => {
                                latencies.push(duration_nanos(operation_started.elapsed()));
                                break;
                            }
                            Err(error) if error.is_transaction_conflict() => {
                                conflicts = conflicts.saturating_add(1);
                                retries = retries.saturating_add(1);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                Result::<_>::Ok((latencies, conflicts, retries))
            });
        }

        let mut latencies = Vec::with_capacity(entity_count);
        let mut conflicts = 0_u64;
        let mut retries = 0_u64;
        while let Some(result) = tasks.join_next().await {
            let (mut task_latencies, task_conflicts, task_retries) =
                result.map_err(|error| {
                    crate::HelixDbError::InvariantViolation(format!(
                        "hot-path benchmark writer task failed: {error}"
                    ))
                })??;
            latencies.append(&mut task_latencies);
            conflicts = conflicts.saturating_add(task_conflicts);
            retries = retries.saturating_add(task_retries);
        }
        assert_eq!(
            latencies.len(),
            entity_count,
            "every concurrent insertion must complete exactly once"
        );
        Ok(insert_sample(
            SecondaryEqualityInsertMode::Concurrent,
            CONCURRENT_WRITERS,
            started.elapsed(),
            latencies,
            entity_count,
            conflicts,
            retries,
        ))
    }

    async fn lookup_result_count(&self) -> Result<usize> {
        lookup_result_count(&self.db, &self.lookup_plans[0]).await
    }
}

async fn lookup_result_count(db: &HelixDB, plan: &exec::ExecutablePlan) -> Result<usize> {
    Ok(lookup_node_ids(db, plan).await?.len())
}

async fn lookup_node_ids(db: &HelixDB, plan: &exec::ExecutablePlan) -> Result<Vec<u64>> {
    let prepared = db
        .planner_context_scoped_prepared(
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
        )
        .await?;
    let result = db
        .execute_prepared_scoped_controlled(
            plan,
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
            crate::execution_control::ExecutionControl::unlimited(),
            prepared.into_catalog_proof(),
        )
        .await?;
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        return Err(crate::HelixDbError::InvariantViolation(
            "hot-path equality lookup did not return projected scalar IDs".to_string(),
        ));
    };
    values
        .into_iter()
        .map(|value| match value {
            ExecutionScalar::NodeId(id) => Ok(id),
            ExecutionScalar::EdgeId(_)
            | ExecutionScalar::String(_)
            | ExecutionScalar::Value(_)
            | ExecutionScalar::Object(_) => Err(crate::HelixDbError::InvariantViolation(
                "hot-path node lookup returned a non-node scalar".to_string(),
            )),
        })
        .collect()
}

/// Measures the portable V4 value codec at the largest required fixture size.
pub fn benchmark_million_sequential_id_bitmap() -> SecondaryEqualityMillionBitmapSample {
    let ids = roaring::RoaringTreemap::from_iter(0..MILLION_BITMAP_CARDINALITY);
    let encode_started = Instant::now();
    let encoded = SecondaryEqualityBitmapValue::new(ids).encode();
    let encode_nanos = encode_started.elapsed().as_nanos();
    let decode_started = Instant::now();
    let decoded = SecondaryEqualityBitmapValue::decode(&encoded)
        .expect("one-million-ID portable bitmap must decode");
    let decode_nanos = decode_started.elapsed().as_nanos();
    std::hint::black_box(decoded);
    SecondaryEqualityMillionBitmapSample {
        cardinality: MILLION_BITMAP_CARDINALITY,
        encoded_bytes: encoded.len(),
        encode_nanos,
        decode_nanos,
    }
}

fn insert_sample(
    mode: SecondaryEqualityInsertMode,
    writers: usize,
    elapsed: Duration,
    mut latencies: Vec<u64>,
    entities: usize,
    conflicts: u64,
    retries: u64,
) -> SecondaryEqualityInsertSample {
    latencies.sort_unstable();
    let median_latency_nanos = percentile(&latencies, 50);
    let p95_latency_nanos = percentile(&latencies, 95);
    SecondaryEqualityInsertSample {
        mode,
        indexes: INDEX_COUNT,
        entities,
        writers,
        elapsed_nanos: elapsed.as_nanos(),
        throughput_per_second: f64::from(entities as u32) / elapsed.as_secs_f64(),
        median_latency_nanos,
        p95_latency_nanos,
        conflicts,
        retries,
        allocations: 0,
        allocated_bytes: 0,
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    assert!(
        !sorted.is_empty(),
        "benchmark latency samples are non-empty"
    );
    let index = sorted.len().saturating_mul(percentile).div_ceil(100) - 1;
    sorted[index]
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn property_name(ordinal: usize) -> String {
    format!("field_{ordinal:02}")
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("hot-path fixture identifiers are non-empty")
}

fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("hot-path step ids are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

fn equality_search_plan(ordinal: usize) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("hot-path access id is positive");
    exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::exact_equality(
                            catalog::NodeEqualityIndexMeta::new(name(&format!(
                                "node_eq:{LABEL}:{}",
                                property_name(ordinal)
                            ))),
                            catalog::ScopedPropertyKey::try_new(LABEL, property_name(ordinal))
                                .expect("hot-path equality key is valid"),
                            ir::IndexValue::Literal(
                                ir::SecondaryIndexLiteral::new(PlannerPropertyValue::String(
                                    SHARED_VALUE.to_string(),
                                ))
                                .expect("hot-path equality value is indexable"),
                            ),
                        ),
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ])
        .expect("hot-path fixture plan is non-empty"),
        exec::ExecStepId::new(2).expect("hot-path root id is positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("hot-path equality plan is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_nearest_rank() {
        let values = (1..=100).collect::<Vec<_>>();
        assert_eq!(percentile(&values, 50), 50);
        assert_eq!(percentile(&values, 95), 95);
    }

    #[test]
    fn million_sequential_ids_round_trip_portably() {
        let sample = benchmark_million_sequential_id_bitmap();
        assert_eq!(sample.cardinality, MILLION_BITMAP_CARDINALITY);
        assert!(sample.encoded_bytes > 0);
    }
}
