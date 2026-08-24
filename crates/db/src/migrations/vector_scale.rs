//! Fixed-shape scale and benchmark harness for vector migration jobs.
//!
//! Setup writes frozen legacy graph, metadata, SimHash, and canonical payload
//! codecs directly in bounded transactions. Measurements then execute the real
//! property-materialization and physical-retirement controllers without a V2
//! HNSW rebuild obscuring their cost. Every committed controller turn is
//! checked against the configured row and combined-byte limits. SlateDB's
//! default bounded memory cache remains enabled, matching the production
//! `CacheMode::Memory` path and avoiding artificial repeated SST-index reads.
//! The local object-store root lives under `/private/tmp` and is removed by its
//! RAII owner after the database closes.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::BoxStream;
use serde::Serialize;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use slatedb::{Db, IsolationLevel};

use crate::config;
use crate::encoding::property::{self, Property};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorSimHashKey, VectorStorageLane,
};
use crate::encoding::v2::keys::{DataKeyKind, DataKey as Key, NodePropertyKey};
use crate::encoding::v2::values::indexes::vector::simhash;
use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey as IndexKey, ScopedKey};
use crate::encoding::v2::legacy::vector::metadata as legacy_metadata;
use crate::encoding::v2::values::indexes::vector::metadata;
use crate::encoding::v2::values::{
    encode_index_record, encode_metadata_value, encode_operation_record,
};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{
    BuildOperationOutcome, IndexGenerationId, IndexId, IndexOperationExecutionState,
    IndexOperationFamily, IndexOperationId, IndexOperationKind, IndexOperationOutcome,
    IndexOperationProgress, IndexOperationRecord, IndexOperationRevision, IndexRecordV2,
    IndexRevision, IndexStateTransition, IndexStateV2, IndexV2MetadataValue,
    LegacyVectorPhysicalReservation, NoCursorProgress, PhysicalGeneration,
    ValidatedDynamicIndexDefinition, VectorBuildProgress, VectorBuildStage,
    VectorGenerationDescriptor, VectorPhysicalIndexId, VectorPhysicalLayout, VectorRoutingLayoutV2,
};
use crate::search::vector::distance::Cosine;
use crate::search::vector::item::Item;
use crate::search::vector::simhash::order_code_from_simhash_bits;
use crate::search::vector::{self, VectorDistanceMetric, VectorIndexConfig, VectorIndexMetadata};

use super::{
    ensure_migration_job, migration_completed, migration_parity_legacy_catalog_row,
    process_migration_once_by_id_with_catalog_measured, vector_properties, vector_retirement,
    MigrationId, MigrationMode, MigrationRunCatalog,
};

const DIMENSION: usize = 3;
const SEED_BATCH_ROWS: u64 = 4_096;
const MIGRATION_BATCH_ROWS: usize = 4_096;
const MIGRATION_BATCH_BYTES: usize = 16 * 1_024 * 1_024;
const MAX_PHASE_WORKING_SET_BYTES: u64 = 512 * 1_024 * 1_024;
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(10);

/// Serializes disk- and memory-intensive migration scale fixtures.
static VECTOR_MIGRATION_SCALE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// One independently sampled migration phase observation.
#[derive(Debug, Serialize)]
struct PhaseObservation {
    phase: &'static str,
    processed_rows: u64,
    admitted_bytes: u64,
    committed_steps: u64,
    elapsed_millis: u128,
    rows_per_second: f64,
    mebibytes_per_second: f64,
    baseline_rss_bytes: u64,
    peak_rss_bytes: u64,
    working_set_peak_bytes: u64,
    rss_samples: u64,
}

/// Complete report printed by each fixed-shape scale contract.
#[derive(Debug, Serialize)]
struct VectorMigrationScaleReport {
    schema_version: u32,
    entity_count: u64,
    migration_batch_rows: usize,
    migration_batch_bytes: usize,
    maximum_phase_working_set_bytes: u64,
    directory_adoption: DirectoryAdoptionObservation,
    materialization: PhaseObservation,
    retirement: PhaseObservation,
}

/// Exact active-directory work and resource measurements.
#[derive(Debug, Serialize)]
struct DirectoryAdoptionObservation {
    canonical_payloads: u64,
    marker_writes: u64,
    marker_observations: u64,
    validated_rows: u64,
    input_bytes: u64,
    output_bytes: u64,
    committed_batches: u64,
    elapsed_millis: u128,
    payloads_per_second: f64,
    batch_latency_p50_micros: u128,
    batch_latency_p95_micros: u128,
    object_store_requests: u64,
    baseline_rss_bytes: u64,
    peak_rss_bytes: u64,
    working_set_peak_bytes: u64,
    rss_samples: u64,
}

/// Counts actual object-store method calls made by SlateDB during a measured phase.
#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    requests: AtomicU64,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            requests: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.requests.store(0, Ordering::Release);
    }

    fn requests(&self) -> u64 {
        self.requests.load(Ordering::Acquire)
    }

    fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::AcqRel);
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("counting-local")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.record_request();
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.record_request();
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.record_request();
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.record_request();
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.record_request();
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.record_request();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.record_request();
        self.inner.copy_opts(from, to, options).await
    }
}

/// Samples current resident memory without retaining per-sample observations.
struct RssSampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    samples: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl RssSampler {
    fn start() -> Result<(Self, u64)> {
        let baseline = current_rss_bytes()?;
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(baseline));
        let samples = Arc::new(AtomicU64::new(1));
        let task_stop = Arc::clone(&stop);
        let task_peak = Arc::clone(&peak);
        let task_samples = Arc::clone(&samples);
        let task = tokio::spawn(async move {
            while !task_stop.load(Ordering::Acquire) {
                if let Ok(current) = current_rss_bytes() {
                    task_peak.fetch_max(current, Ordering::AcqRel);
                    task_samples.fetch_add(1, Ordering::AcqRel);
                }
                tokio::time::sleep(RSS_SAMPLE_INTERVAL).await;
            }
        });
        Ok((
            Self {
                stop,
                peak,
                samples,
                task,
            },
            baseline,
        ))
    }

    async fn stop(self) -> Result<(u64, u64)> {
        self.stop.store(true, Ordering::Release);
        self.task.await.map_err(|error| {
            HelixDbError::InvariantViolation(format!("RSS sampler task failed: {error}"))
        })?;
        let current = current_rss_bytes()?;
        let peak = self.peak.load(Ordering::Acquire).max(current);
        Ok((peak, self.samples.load(Ordering::Acquire)))
    }
}

/// Runs one fixed entity count and emits its machine-readable benchmark line.
pub(super) async fn run(entity_count: u64) {
    let _scale_guard = VECTOR_MIGRATION_SCALE_LOCK.lock().await;
    let report = run_inner(entity_count)
        .await
        .unwrap_or_else(|error| panic!("vector migration scale fixture failed: {error}"));
    println!(
        "VECTOR_MIGRATION_SCALE {}",
        serde_json::to_string(&report).expect("scale report serializes")
    );
}

async fn run_inner(entity_count: u64) -> Result<VectorMigrationScaleReport> {
    if entity_count == 0 {
        return Err(HelixDbError::Config(
            "vector migration scale entity count must be positive".to_string(),
        ));
    }
    let root = tempfile::Builder::new()
        .prefix("helix-vector-migration-scale-")
        .tempdir_in("/private/tmp")
        .map_err(|error| HelixDbError::Config(format!("scale tempdir failed: {error}")))?;
    let local_store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(root.path()).map_err(|error| {
            HelixDbError::Config(format!("scale local object store failed: {error}"))
        })?,
    );
    let object_store = Arc::new(CountingObjectStore::new(local_store));
    let db_store: Arc<dyn ObjectStore> = object_store.clone();
    let db = Arc::new(
        Db::builder("vector-migration-scale", db_store)
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await?,
    );
    crate::index_lifecycle::repository::bootstrap_writer(db.as_ref()).await?;
    let writer = crate::HelixWriter::new(Arc::clone(&db), config::DbConfig::new().id_lease_size());
    let scope = DataScope::LegacyUnscoped;
    let definition: ValidatedDynamicIndexDefinition = config::VectorIndexDefinition::new_node(
        "MigrationScaleDocument",
        "embedding",
        DIMENSION,
        VectorDistanceMetric::Cosine,
    )?
    .try_into()?;
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("vector runtime definition validates as vector")
    };
    let runtime = vector_definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    let legacy_physical_id = VectorPhysicalIndexId::new(vector::index_id_from_name(&legacy_name))?;
    let current_physical_id =
        VectorPhysicalIndexId::new(legacy_physical_id.get().checked_add(1).ok_or_else(|| {
            HelixDbError::InvariantViolation("scale physical index id cannot advance".to_string())
        })?)?;
    let index_id = IndexId::new(1)?;
    let generation = IndexGenerationId::initial();
    let completed_build_operation_id = IndexOperationId::new_v4();
    let active = IndexRecordV2::building(
        index_id,
        definition.clone(),
        IndexRevision::initial(),
        PhysicalGeneration::Vector {
            generation,
            layout: VectorPhysicalLayout::Unpartitioned {
                physical_index_id: current_physical_id,
            },
            descriptor: VectorGenerationDescriptor::legacy_for_definition(vector_definition),
        },
        completed_build_operation_id,
    )?
    .transition(IndexStateTransition::Activate)?;
    let completed_build = IndexOperationRecord::try_new(
        completed_build_operation_id,
        index_id,
        active.identity().clone(),
        generation,
        active.revision(),
        IndexOperationRevision::initial(),
        IndexOperationKind::Build,
        IndexOperationFamily::Vector,
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Activate(NoCursorProgress::default()),
        )),
        1,
        IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
            BuildOperationOutcome::Succeeded,
        )),
    )
    .map_err(|error| {
        HelixDbError::InvariantViolation(format!(
            "scale completed vector operation is invalid: {error}"
        ))
    })?;
    let vector = vec![1.0_f32, 0.5, 0.25];
    let simhash_bits = 0x0123_4567_89AB_CDEF;
    let original_properties = property::encode_properties(&[
        Property::string("$label", "MigrationScaleDocument"),
        Property::string("title", "retained"),
    ]);
    let item_value = vector::encode_item(&Item::<Cosine>::new(vector));
    let simhash_value = simhash::encode_simhash(simhash_bits);

    let metadata_txn = db.begin(IsolationLevel::Snapshot).await?;
    let (legacy_definition_key, legacy_definition_value) =
        migration_parity_legacy_catalog_row(&definition, false)?;
    metadata_txn.put(legacy_definition_key, legacy_definition_value)?;
    metadata_txn.put(
        IndexKey::Data {
            scope,
            kind: ScopedKey::index_record(active.identity().clone()),
        }
        .to_bytes(),
        encode_index_record(&active),
    )?;
    metadata_txn.put(
        IndexKey::Data {
            scope,
            kind: ScopedKey::operation(completed_build_operation_id),
        }
        .to_bytes(),
        encode_operation_record(&completed_build),
    )?;
    let mut legacy_metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        vector_definition,
        &legacy_name,
    ));
    legacy_metadata.entry_point = Some(1);
    legacy_metadata.count = entity_count;
    metadata_txn.put(
        Key::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                legacy_physical_id.get(),
            ))),
        }
        .to_bytes(),
        Bytes::copy_from_slice(&legacy_metadata::encode_legacy_metadata_for_contract(
            &legacy_metadata,
        )),
    )?;
    metadata_txn.put(
        IndexKey::Global {
            kind: GlobalKey::LegacyVectorPhysicalReservation(legacy_physical_id),
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
            LegacyVectorPhysicalReservation::LegacySource,
        )),
    )?;
    metadata_txn.commit().await?;

    let mut first_entity = 1_u64;
    while first_entity <= entity_count {
        let last_entity = entity_count.min(first_entity.saturating_add(SEED_BATCH_ROWS - 1));
        let transaction = db.begin(IsolationLevel::Snapshot).await?;
        for entity_id in first_entity..=last_entity {
            transaction.put(
                Key::Data {
                    scope,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
                }
                .to_bytes(),
                original_properties.clone(),
            )?;
            transaction.put(
                Key::Data {
                    scope,
                    kind: DataKeyKind::Vector(VectorKey::SimHash(VectorSimHashKey::new(
                        legacy_physical_id.get(),
                        entity_id,
                    ))),
                }
                .to_bytes(),
                simhash_value,
            )?;
            transaction.put(
                Key::Data {
                    scope,
                    kind: DataKeyKind::Vector(VectorKey::Vector(VectorItemKey::new(
                        legacy_physical_id.get(),
                        order_code_from_simhash_bits(simhash_bits),
                        entity_id,
                    ))),
                }
                .to_bytes(),
                item_value.clone(),
            )?;
        }
        transaction.commit().await?;
        first_entity = last_entity.saturating_add(1);
    }
    db.flush().await?;

    let tuning = config::MigrationTuning::default()
        .with_batch_rows(
            config::MigrationBatchRows::new(MIGRATION_BATCH_ROWS)
                .expect("scale migration row limit is positive"),
        )
        .with_batch_bytes(
            config::MigrationBatchBytes::new(MIGRATION_BATCH_BYTES)
                .expect("scale migration byte limit is positive"),
        )
        .with_worker_mode(config::MigrationWorkerMode::Disabled);
    ensure_migration_job(
        db.as_ref(),
        scope,
        MigrationId::LegacyVectorPropertyMaterialization,
        MigrationMode::BlockingStartup,
    )
    .await?;
    let materialization_catalog =
        vector_properties::LegacyVectorPropertyCatalog::load(db.as_ref(), scope).await?;
    let (sampler, materialization_baseline_rss) = RssSampler::start()?;
    let materialization_started = Instant::now();
    let (materialized_rows, materialized_bytes, materialization_steps) = run_job(
        &writer,
        scope,
        tuning,
        MigrationId::LegacyVectorPropertyMaterialization,
        MigrationRunCatalog::VectorProperties(&materialization_catalog),
    )
    .await?;
    let materialization_elapsed = materialization_started.elapsed();
    let (materialization_peak_rss, materialization_samples) = sampler.stop().await?;
    if materialized_rows != entity_count {
        return Err(HelixDbError::InvariantViolation(format!(
            "materialization processed {materialized_rows} rows for {entity_count} entities"
        )));
    }
    assert_materialized_samples(db.as_ref(), scope, entity_count).await?;

    ensure_migration_job(
        db.as_ref(),
        scope,
        MigrationId::LegacyVectorPhysicalCleanup,
        MigrationMode::BlockingStartup,
    )
    .await?;
    let retirement_catalog =
        vector_retirement::LegacyVectorRetirementCatalog::load(db.as_ref(), scope).await?;
    let (sampler, retirement_baseline_rss) = RssSampler::start()?;
    let retirement_started = Instant::now();
    let (retired_rows, retired_bytes, retirement_steps) = run_job(
        &writer,
        scope,
        tuning,
        MigrationId::LegacyVectorPhysicalCleanup,
        MigrationRunCatalog::VectorRetirement(&retirement_catalog),
    )
    .await?;
    let retirement_elapsed = retirement_started.elapsed();
    let (retirement_peak_rss, retirement_samples) = sampler.stop().await?;
    let expected_retirement_rows = entity_count
        .checked_mul(2)
        .and_then(|rows| rows.checked_add(4))
        .ok_or_else(|| {
            HelixDbError::InvariantViolation("scale retirement row count overflowed".to_string())
        })?;
    if retired_rows != expected_retirement_rows {
        return Err(HelixDbError::InvariantViolation(format!(
            "retirement processed {retired_rows} rows, expected {expected_retirement_rows}"
        )));
    }
    assert_retired(db.as_ref(), scope, legacy_physical_id).await?;

    // The active-directory gate is seeded only after the legacy lifecycle
    // phases finish. Real startup runs these migrations in that order, and
    // separating the fixtures keeps each RSS/throughput contract attributable
    // to the phase it measures.
    let current_physical_name = format!(
        "v2-vector-{}-{}-{}",
        index_id.get(),
        generation.get(),
        current_physical_id.get()
    );
    let mut current_metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        vector_definition,
        &current_physical_name,
    ));
    current_metadata.entry_point = Some(1);
    current_metadata.count = 0;
    let directory_metadata_txn = db.begin(IsolationLevel::Snapshot).await?;
    directory_metadata_txn.put(
        Key::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                current_physical_id.get(),
            ))),
        }
        .to_bytes(),
        Bytes::copy_from_slice(&metadata::encode_metadata(&current_metadata)),
    )?;
    directory_metadata_txn.put(
        IndexKey::Global {
            kind: GlobalKey::LegacyVectorPhysicalReservation(current_physical_id),
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
            LegacyVectorPhysicalReservation::AdoptedActive {
                index_id,
                generation,
            },
        )),
    )?;
    directory_metadata_txn.commit().await?;

    let mut first_entity = 1_u64;
    while first_entity <= entity_count {
        let last_entity = entity_count.min(first_entity.saturating_add(SEED_BATCH_ROWS - 1));
        let transaction = db.begin(IsolationLevel::Snapshot).await?;
        for entity_id in first_entity..=last_entity {
            transaction.put(
                Key::Data {
                    scope,
                    kind: DataKeyKind::Vector(VectorKey::SimHash(VectorSimHashKey::new(
                        current_physical_id.get(),
                        entity_id,
                    ))),
                }
                .to_bytes(),
                simhash_value,
            )?;
            transaction.put(
                Key::Data {
                    scope,
                    kind: DataKeyKind::Vector(VectorKey::Vector(VectorItemKey::new(
                        current_physical_id.get(),
                        order_code_from_simhash_bits(simhash_bits),
                        entity_id,
                    ))),
                }
                .to_bytes(),
                item_value.clone(),
            )?;
        }
        transaction.commit().await?;
        first_entity = last_entity.saturating_add(1);
    }
    db.flush().await?;

    let directory_limits = config::SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(MIGRATION_BATCH_ROWS).expect("directory scale row limit is positive"),
        NonZeroU64::new(MIGRATION_BATCH_BYTES as u64)
            .expect("directory scale input limit is positive"),
        NonZeroU64::new(MIGRATION_BATCH_ROWS as u64)
            .expect("directory scale output-operation limit is positive"),
        NonZeroU64::new(MIGRATION_BATCH_BYTES as u64)
            .expect("directory scale output-byte limit is positive"),
        NonZeroU64::new(MIGRATION_BATCH_BYTES as u64)
            .expect("directory scale single-vector limit is positive"),
    )
    .map_err(|error| {
        HelixDbError::Config(format!("directory scale limits are invalid: {error}"))
    })?;
    object_store.reset();
    let (sampler, directory_baseline_rss) = RssSampler::start()?;
    let directory_started = Instant::now();
    let directory = super::vector_simhash_directory::run_measured_for_scale(
        db.as_ref(),
        scope,
        directory_limits,
    )
    .await?;
    db.flush().await?;
    let directory_elapsed = directory_started.elapsed();
    let directory_object_store_requests = object_store.requests();
    let (directory_peak_rss, directory_samples) = sampler.stop().await?;
    if directory.canonical_payloads != entity_count || directory.marker_writes != entity_count {
        return Err(HelixDbError::InvariantViolation(format!(
            "directory adoption processed {} canonical payloads and wrote {} markers for {entity_count} vectors",
            directory.canonical_payloads, directory.marker_writes
        )));
    }
    let published = crate::index_lifecycle::repository::load_index_record(
        db.as_ref(),
        scope,
        active.identity(),
    )
    .await?
    .ok_or_else(|| {
        HelixDbError::InvariantViolation("directory scale canonical record disappeared".to_string())
    })?;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                generation: published_generation,
                layout:
                    VectorPhysicalLayout::Unpartitioned {
                        physical_index_id: published_physical_id,
                    },
                descriptor,
            },
        completed_build_operation_id: published_operation_id,
    } = published.state()
    else {
        return Err(HelixDbError::InvariantViolation(
            "directory scale target is no longer active".to_string(),
        ));
    };
    if *published_generation != generation
        || *published_physical_id != current_physical_id
        || *published_operation_id != completed_build_operation_id
        || descriptor.routing_layout() != VectorRoutingLayoutV2::SimHashDirectoryV1
    {
        return Err(HelixDbError::InvariantViolation(
            "directory publication changed generation identity or failed to publish routing"
                .to_string(),
        ));
    }
    let directory_adoption = directory_observation(
        directory,
        directory_elapsed,
        directory_object_store_requests,
        directory_baseline_rss,
        directory_peak_rss,
        directory_samples,
    )?;

    let materialization = observation(
        "materialization",
        materialized_rows,
        materialized_bytes,
        materialization_steps,
        materialization_elapsed,
        materialization_baseline_rss,
        materialization_peak_rss,
        materialization_samples,
    )?;
    let retirement = observation(
        "retirement",
        retired_rows,
        retired_bytes,
        retirement_steps,
        retirement_elapsed,
        retirement_baseline_rss,
        retirement_peak_rss,
        retirement_samples,
    )?;
    db.close().await?;
    drop(root);
    Ok(VectorMigrationScaleReport {
        schema_version: 2,
        entity_count,
        migration_batch_rows: MIGRATION_BATCH_ROWS,
        migration_batch_bytes: MIGRATION_BATCH_BYTES,
        maximum_phase_working_set_bytes: MAX_PHASE_WORKING_SET_BYTES,
        directory_adoption,
        materialization,
        retirement,
    })
}

async fn run_job(
    writer: &crate::HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
    id: MigrationId,
    catalog: MigrationRunCatalog<'_>,
) -> Result<(u64, u64, u64)> {
    let mut rows = 0_u64;
    let mut admitted_bytes = 0_u64;
    let mut committed_steps = 0_u64;
    while !migration_completed(writer.db(), scope, id).await? {
        let step =
            process_migration_once_by_id_with_catalog_measured(writer, scope, tuning, id, catalog)
                .await?;
        if !step.advanced {
            return Err(HelixDbError::InvariantViolation(format!(
                "scale migration {} stopped before completion",
                id.log_name()
            )));
        }
        if step.rows > u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX)
            || step.admitted_bytes > u64::try_from(tuning.batch_bytes().get()).unwrap_or(u64::MAX)
        {
            return Err(HelixDbError::InvariantViolation(format!(
                "scale migration {} exceeded its committed batch limits",
                id.log_name()
            )));
        }
        rows = rows.checked_add(step.rows).ok_or_else(|| {
            HelixDbError::InvariantViolation("scale migration rows overflowed".to_string())
        })?;
        admitted_bytes = admitted_bytes
            .checked_add(step.admitted_bytes)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "scale migration admitted bytes overflowed".to_string(),
                )
            })?;
        committed_steps = committed_steps.saturating_add(1);
    }
    Ok((rows, admitted_bytes, committed_steps))
}

async fn assert_materialized_samples(db: &Db, scope: DataScope, entity_count: u64) -> Result<()> {
    for entity_id in [1, entity_count.div_ceil(2), entity_count] {
        let value = db
            .get(
                Key::Data {
                    scope,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
                }
                .to_bytes(),
            )
            .await?
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(format!(
                    "materialized scale row {entity_id} is missing"
                ))
            })?;
        let properties = property::decode_properties(&value)?;
        if !properties.contains(&Property::f32_array("embedding", vec![1.0, 0.5, 0.25])) {
            return Err(HelixDbError::InvariantViolation(format!(
                "materialized scale row {entity_id} lost its embedding"
            )));
        }
    }
    Ok(())
}

async fn assert_retired(
    db: &Db,
    scope: DataScope,
    physical_id: VectorPhysicalIndexId,
) -> Result<()> {
    for lane in VectorStorageLane::ALL {
        let prefix = Key::data_prefix(scope, lane.prefix_key(physical_id.get()).to_bytes());
        let mut rows = db.scan_prefix(prefix, ..).await?;
        if rows.next().await?.is_some() {
            return Err(HelixDbError::InvariantViolation(format!(
                "retired scale namespace retains a {lane:?} row"
            )));
        }
    }
    let reservation = IndexKey::Global {
        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_id),
    }
    .to_bytes();
    if db.get(reservation).await?.is_some() {
        return Err(HelixDbError::InvariantViolation(
            "retired scale reservation remains".to_string(),
        ));
    }
    if !super::load_legacy_definition_rows(db, scope)
        .await?
        .is_empty()
    {
        return Err(HelixDbError::InvariantViolation(
            "retired scale legacy definition remains".to_string(),
        ));
    }
    Ok(())
}

fn directory_observation(
    observation: super::vector_simhash_directory::DirectoryScaleObservation,
    elapsed: Duration,
    object_store_requests: u64,
    baseline_rss_bytes: u64,
    peak_rss_bytes: u64,
    rss_samples: u64,
) -> Result<DirectoryAdoptionObservation> {
    let mut batch_latencies = observation.batch_latencies;
    if batch_latencies.is_empty() {
        return Err(HelixDbError::InvariantViolation(
            "directory adoption emitted no measured batches".to_string(),
        ));
    }
    batch_latencies.sort_unstable();
    let percentile = |percent: usize| {
        let index = (batch_latencies.len() - 1).saturating_mul(percent) / 100;
        batch_latencies[index].as_micros()
    };
    let working_set_peak_bytes = peak_rss_bytes.saturating_sub(baseline_rss_bytes);
    if working_set_peak_bytes > MAX_PHASE_WORKING_SET_BYTES {
        return Err(HelixDbError::InvariantViolation(format!(
            "directory adoption working-set peak {working_set_peak_bytes} exceeded {MAX_PHASE_WORKING_SET_BYTES} bytes"
        )));
    }
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    Ok(DirectoryAdoptionObservation {
        canonical_payloads: observation.canonical_payloads,
        marker_writes: observation.marker_writes,
        marker_observations: observation.marker_observations,
        validated_rows: observation.validated_rows,
        input_bytes: observation.input_bytes,
        output_bytes: observation.output_bytes,
        committed_batches: u64::try_from(batch_latencies.len()).map_err(|_| {
            HelixDbError::InvariantViolation(
                "directory adoption batch count overflowed".to_string(),
            )
        })?,
        elapsed_millis: elapsed.as_millis(),
        payloads_per_second: observation.canonical_payloads as f64 / seconds,
        batch_latency_p50_micros: percentile(50),
        batch_latency_p95_micros: percentile(95),
        object_store_requests,
        baseline_rss_bytes,
        peak_rss_bytes,
        working_set_peak_bytes,
        rss_samples,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one report row owns its complete measured phase"
)]
fn observation(
    phase: &'static str,
    rows: u64,
    admitted_bytes: u64,
    committed_steps: u64,
    elapsed: Duration,
    baseline_rss_bytes: u64,
    peak_rss_bytes: u64,
    rss_samples: u64,
) -> Result<PhaseObservation> {
    let working_set_peak_bytes = peak_rss_bytes.saturating_sub(baseline_rss_bytes);
    if working_set_peak_bytes > MAX_PHASE_WORKING_SET_BYTES {
        return Err(HelixDbError::InvariantViolation(format!(
            "{phase} working-set peak {working_set_peak_bytes} exceeded {MAX_PHASE_WORKING_SET_BYTES} bytes"
        )));
    }
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    Ok(PhaseObservation {
        phase,
        processed_rows: rows,
        admitted_bytes,
        committed_steps,
        elapsed_millis: elapsed.as_millis(),
        rows_per_second: rows as f64 / seconds,
        mebibytes_per_second: admitted_bytes as f64 / (1024.0 * 1024.0) / seconds,
        baseline_rss_bytes,
        peak_rss_bytes,
        working_set_peak_bytes,
        rss_samples,
    })
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")
        .map_err(|error| HelixDbError::Config(format!("failed to read process RSS: {error}")))?;
    let resident_pages = statm
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| HelixDbError::Config("process RSS has no resident pages".to_string()))?
        .parse::<u64>()
        .map_err(|error| HelixDbError::Config(format!("invalid process RSS: {error}")))?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(HelixDbError::Config(
            "sysconf(_SC_PAGESIZE) failed".to_string(),
        ));
    }
    Ok(resident_pages.saturating_mul(page_size as u64))
}

#[cfg(target_os = "macos")]
#[allow(
    deprecated,
    reason = "libc exposes the process task port needed by task_info through this stable ABI"
)]
fn current_rss_bytes() -> Result<u64> {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::zeroed();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    let result = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast::<libc::integer_t>(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS {
        return Err(HelixDbError::Config(format!(
            "mach task_info failed with {result}"
        )));
    }
    let info = unsafe { info.assume_init() };
    Ok(info.resident_size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn current_rss_bytes() -> Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(HelixDbError::Config(format!(
            "getrusage failed: {}",
            std::io::Error::last_os_error()
        )));
    }
    let usage = unsafe { usage.assume_init() };
    u64::try_from(usage.ru_maxrss)
        .map(|rss| rss.saturating_mul(1_024))
        .map_err(|error| HelixDbError::Config(format!("invalid process RSS: {error}")))
}
