//! Blocking, resumable adoption of SimHash directories by active legacy vectors.
//!
//! The job keeps the canonical descriptor at `LegacyHnsw` until a compact
//! preflight, one canonical-payload pass, and a compact final proof complete.
//! Every marker batch and its durable cursor commit in the same transaction.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::config::SearchIndexBatchLimits;
use crate::encoding::v1::keys::tenant::DataScope;
#[cfg(test)]
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{GlobalKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::{
    decode_index_record, decode_metadata_value, decode_operation_record, encode_index_record,
    encode_operation_record,
};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexOperationId, IndexRecordV2, IndexStateTransition, IndexStateV2,
    IndexV2MetadataValue, LegacyVectorPhysicalReservation, PhysicalGeneration,
    ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition, VectorPhysicalIndexId,
    VectorPhysicalLayout, VectorRoutingLayoutV2,
};
use crate::search::vector::{
    self, Distance, ValidatedVectorGenerationHandle, VectorIndex, VectorWriteMeasurement,
    VectorWriteRecorder,
};

use super::{
    decode_json, encode_json, scan_bounds_for_prefix, scoped_metadata_key, MigrationResumeKey,
};

const JOB_KEY: &[u8] = b"kv_vector_simhash_directory_v1_migration";
const JOB_VERSION: u8 = 1;

/// Aggregate durable accounting for the complete startup migration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrationCounters {
    validated_rows: u64,
    canonical_payloads: u64,
    marker_count: u64,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
    batches: u64,
}

impl MigrationCounters {
    fn checked_add(
        self,
        validated_rows: u64,
        canonical_payloads: u64,
        marker_count: u64,
        input_bytes: u64,
        writes: VectorWriteMeasurement,
    ) -> Result<Self> {
        Ok(Self {
            validated_rows: self
                .validated_rows
                .checked_add(validated_rows)
                .ok_or_else(|| overflow("validated rows"))?,
            canonical_payloads: self
                .canonical_payloads
                .checked_add(canonical_payloads)
                .ok_or_else(|| overflow("canonical payloads"))?,
            marker_count: self
                .marker_count
                .checked_add(marker_count)
                .ok_or_else(|| overflow("marker count"))?,
            input_bytes: self
                .input_bytes
                .checked_add(input_bytes)
                .ok_or_else(|| overflow("input bytes"))?,
            output_operations: self
                .output_operations
                .checked_add(writes.operations())
                .ok_or_else(|| overflow("output operations"))?,
            output_bytes: self
                .output_bytes
                .checked_add(writes.encoded_bytes())
                .ok_or_else(|| overflow("output bytes"))?,
            batches: self
                .batches
                .checked_add(1)
                .ok_or_else(|| overflow("batch count"))?,
        })
    }
}

/// Stable persisted identity of one target without serializing model internals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrationTarget {
    index_key: MigrationResumeKey,
    index_id: u64,
    generation: u64,
    physical_index_id: u64,
    record_revision: u64,
    completed_build_operation_id: [u8; 16],
}

/// One legal durable stage of the dedicated migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum MigrationState {
    SelectTarget {
        after_index_key: Option<MigrationResumeKey>,
    },
    Preflight {
        target: MigrationTarget,
        cursor: Option<MigrationResumeKey>,
        existing_markers: u64,
    },
    Backfill {
        target: MigrationTarget,
        cursor: Option<MigrationResumeKey>,
        preflight_markers: u64,
        canonical_vectors: u64,
        existing_markers: u64,
        marker_writes: u64,
    },
    Verify {
        target: MigrationTarget,
        cursor: Option<MigrationResumeKey>,
        canonical_vectors: u64,
        marker_writes: u64,
        verified_markers: u64,
    },
    Publish {
        target: MigrationTarget,
        canonical_vectors: u64,
        marker_writes: u64,
        verified_markers: u64,
    },
    Completed,
}

impl MigrationState {
    const fn name(&self) -> &'static str {
        match self {
            Self::SelectTarget { .. } => "select_target",
            Self::Preflight { .. } => "preflight",
            Self::Backfill { .. } => "backfill",
            Self::Verify { .. } => "verify",
            Self::Publish { .. } => "publish",
            Self::Completed => "completed",
        }
    }
}

/// Complete durable job value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrationJob {
    version: u8,
    state: MigrationState,
    completed_targets: u64,
    resume_count: u64,
    counters: MigrationCounters,
}

impl MigrationJob {
    const fn initial() -> Self {
        Self {
            version: JOB_VERSION,
            state: MigrationState::SelectTarget {
                after_index_key: None,
            },
            completed_targets: 0,
            resume_count: 0,
            counters: MigrationCounters {
                validated_rows: 0,
                canonical_payloads: 0,
                marker_count: 0,
                input_bytes: 0,
                output_operations: 0,
                output_bytes: 0,
                batches: 0,
            },
        }
    }

    fn validate(&self) -> Result<()> {
        if self.version != JOB_VERSION {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "unsupported VectorSimHashDirectoryV1 migration version {}",
                    self.version
                ),
            });
        }
        match &self.state {
            MigrationState::Backfill {
                preflight_markers,
                existing_markers,
                ..
            } if existing_markers > preflight_markers => {
                return Err(corruption(
                    "active vector backfill observed more existing markers than preflight",
                ));
            }
            MigrationState::Verify {
                canonical_vectors,
                verified_markers,
                ..
            }
            | MigrationState::Publish {
                canonical_vectors,
                verified_markers,
                ..
            } if verified_markers > canonical_vectors => {
                return Err(corruption(
                    "active vector verification exceeds its canonical vector count",
                ));
            }
            MigrationState::SelectTarget { .. }
            | MigrationState::Preflight { .. }
            | MigrationState::Backfill { .. }
            | MigrationState::Verify { .. }
            | MigrationState::Publish { .. }
            | MigrationState::Completed => {}
        }
        Ok(())
    }
}

/// Runs every durable stage under the process-local scope-exclusive permit.
pub(super) async fn run(
    database: &crate::HelixDB,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
) -> Result<()> {
    let crate::HelixStorage::Writer(writer) = database.storage() else {
        return Err(HelixDbError::WriterModeRequired {
            actual: database.mode().as_str(),
        });
    };
    let _scope_permit = database
        .inner
        .index_scope_gates
        .lifecycle_permit(scope)
        .await;
    ensure_job(writer.db(), scope).await?;
    record_resume(writer.db(), scope).await?;
    loop {
        let job = load_job(writer.db(), scope).await?;
        if matches!(job.state, MigrationState::Completed) {
            tracing::info!(
                migration = "vector_simhash_directory_v1",
                completed_targets = job.completed_targets,
                resume_count = job.resume_count,
                validated_rows = job.counters.validated_rows,
                canonical_payloads = job.counters.canonical_payloads,
                marker_count = job.counters.marker_count,
                input_bytes = job.counters.input_bytes,
                output_operations = job.counters.output_operations,
                output_bytes = job.counters.output_bytes,
                batches = job.counters.batches,
                no_op = job.completed_targets == 0,
                "blocking vector SimHash-directory migration completed"
            );
            break;
        }
        let stage = job.state.name();
        if let Err(error) = process_once(writer.db(), scope, limits, job).await {
            tracing::error!(
                migration = "vector_simhash_directory_v1",
                stage,
                error = %error,
                "blocking vector SimHash-directory migration failed"
            );
            return Err(error);
        }
    }
    drop(_scope_permit);
    database.refresh_runtime_catalog(scope).await
}

async fn ensure_job(db: &Db, scope: DataScope) -> Result<()> {
    let key = job_key(scope);
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    if transaction.get(&key).await?.is_none() {
        transaction.put(&key, encode_json(&MigrationJob::initial())?)?;
        transaction.commit().await?;
    } else {
        transaction.rollback();
    }
    Ok(())
}

async fn record_resume(db: &Db, scope: DataScope) -> Result<()> {
    let key = job_key(scope);
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let Some(value) = transaction.get(&key).await? else {
        return Err(corruption(
            "VectorSimHashDirectoryV1 migration job disappeared",
        ));
    };
    let mut job = decode_json::<MigrationJob>(&value)?;
    job.validate()?;
    if !matches!(
        job.state,
        MigrationState::Completed
            | MigrationState::SelectTarget {
                after_index_key: None
            }
    ) {
        job.resume_count = job
            .resume_count
            .checked_add(1)
            .ok_or_else(|| overflow("resume count"))?;
        transaction.put(&key, encode_json(&job)?)?;
        transaction.commit().await?;
    } else {
        transaction.rollback();
    }
    Ok(())
}

async fn load_job(read: &(impl DbReadOps + Send + Sync), scope: DataScope) -> Result<MigrationJob> {
    let Some(value) = read.get(job_key(scope)).await? else {
        return Err(corruption(
            "VectorSimHashDirectoryV1 migration job is missing",
        ));
    };
    let job = decode_json::<MigrationJob>(&value)?;
    job.validate()?;
    Ok(job)
}

async fn process_once(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
    job: MigrationJob,
) -> Result<()> {
    match job.state.clone() {
        MigrationState::SelectTarget { after_index_key } => {
            select_target(db, scope, limits, job, after_index_key).await
        }
        MigrationState::Preflight { target, .. }
        | MigrationState::Backfill { target, .. }
        | MigrationState::Verify { target, .. } => {
            let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
            let record = load_target_record(&transaction, scope, &target).await?;
            let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
                return Err(corruption("SimHash-directory target changed index family"));
            };
            let metric = definition.metric();
            transaction.rollback();
            match metric {
                vector::VectorDistanceMetric::Cosine => {
                    process_vector_stage::<vector::distance::Cosine>(db, scope, limits, job, target)
                        .await
                }
                vector::VectorDistanceMetric::Euclidean => {
                    process_vector_stage::<vector::distance::Euclidean>(
                        db, scope, limits, job, target,
                    )
                    .await
                }
                vector::VectorDistanceMetric::Manhattan => {
                    process_vector_stage::<vector::distance::Manhattan>(
                        db, scope, limits, job, target,
                    )
                    .await
                }
            }
        }
        MigrationState::Publish { .. } => publish(db, scope, job).await,
        MigrationState::Completed => Ok(()),
    }
}

async fn select_target(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
    mut job: MigrationJob,
    after_index_key: Option<MigrationResumeKey>,
) -> Result<()> {
    let started = std::time::Instant::now();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let prefix = IndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
    let mut rows = transaction
        .scan(scan_bounds_for_prefix(&prefix, after_index_key.as_ref()))
        .await?;
    let mut validated_rows = 0_usize;
    let mut input_bytes = 0_u64;
    let mut last_key = after_index_key;
    let mut exhausted = true;
    let mut selected = None;
    while validated_rows < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let row_bytes = measured_row_bytes(&row.key, &row.value)?;
        let Some(next_input_bytes) = input_bytes.checked_add(row_bytes) else {
            return Err(overflow("target-selection input bytes"));
        };
        if next_input_bytes > limits.max_input_bytes().get() {
            if validated_rows == 0 {
                return Err(oversized(
                    "canonical index record",
                    row_bytes,
                    limits.max_input_bytes().get(),
                ));
            }
            exhausted = false;
            break;
        }
        let record = decode_canonical_record(scope, &row.key, &row.value)?;
        validated_rows += 1;
        input_bytes = next_input_bytes;
        last_key = MigrationResumeKey::new(row.key.to_vec());
        if let Some(target) = target_from_record(&transaction, &row.key, &record).await? {
            selected = Some(target);
            exhausted = false;
            break;
        }
    }
    if validated_rows == limits.max_entities().get() && selected.is_none() {
        exhausted = false;
    }
    job.counters = job.counters.checked_add(
        validated_rows as u64,
        0,
        0,
        input_bytes,
        VectorWriteMeasurement::zero(),
    )?;
    let target_selected = selected.is_some();
    let has_cursor = last_key.is_some();
    job.state = match selected {
        Some(target) => MigrationState::Preflight {
            target,
            cursor: None,
            existing_markers: 0,
        },
        None if exhausted => MigrationState::Completed,
        None => MigrationState::SelectTarget {
            after_index_key: last_key,
        },
    };
    transaction.put(job_key(scope), encode_json(&job)?)?;
    transaction.commit().await?;
    tracing::info!(
        migration = "vector_simhash_directory_v1",
        stage = "select_target",
        validated_rows,
        input_bytes,
        selected = target_selected,
        exhausted,
        has_cursor,
        elapsed_millis = started.elapsed().as_millis(),
        "advanced vector SimHash-directory migration batch"
    );
    Ok(())
}

async fn process_vector_stage<D: Distance>(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
    job: MigrationJob,
    target: MigrationTarget,
) -> Result<()> {
    match job.state.clone() {
        MigrationState::Preflight {
            cursor,
            existing_markers,
            ..
        } => preflight::<D>(db, scope, limits, job, target, cursor, existing_markers).await,
        MigrationState::Backfill {
            cursor,
            preflight_markers,
            canonical_vectors,
            existing_markers,
            marker_writes,
            ..
        } => {
            backfill::<D>(
                db,
                scope,
                limits,
                job,
                target,
                cursor,
                preflight_markers,
                canonical_vectors,
                existing_markers,
                marker_writes,
            )
            .await
        }
        MigrationState::Verify {
            cursor,
            canonical_vectors,
            marker_writes,
            verified_markers,
            ..
        } => {
            verify::<D>(
                db,
                scope,
                limits,
                job,
                target,
                cursor,
                canonical_vectors,
                marker_writes,
                verified_markers,
            )
            .await
        }
        MigrationState::SelectTarget { .. }
        | MigrationState::Publish { .. }
        | MigrationState::Completed => Err(corruption(
            "metric dispatch selected a non-vector migration stage",
        )),
    }
}

async fn preflight<D: Distance>(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
    mut job: MigrationJob,
    target: MigrationTarget,
    cursor: Option<MigrationResumeKey>,
    existing_markers: u64,
) -> Result<()> {
    let started = std::time::Instant::now();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let (record, definition, index) = target_index::<D>(&transaction, scope, &target).await?;
    validate_reservation(&transaction, &record, &target).await?;
    let outcome = index
        .validate_simhash_directory(
            &transaction,
            cursor.as_ref().map(MigrationResumeKey::as_bytes),
            &definition,
            vector::SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            limits.max_entities().get(),
            limits.max_input_bytes().get(),
        )
        .await?;
    let vector::SimHashDirectoryValidationOutcome::Valid {
        last_key,
        markers,
        input_bytes,
        exhausted,
    } = outcome
    else {
        return directory_validation_error(outcome, "preflight");
    };
    let existing_markers = existing_markers
        .checked_add(markers)
        .ok_or_else(|| overflow("preflight marker count"))?;
    job.counters = job.counters.checked_add(
        markers,
        0,
        markers,
        input_bytes,
        VectorWriteMeasurement::zero(),
    )?;
    job.state = if exhausted {
        MigrationState::Backfill {
            target: target.clone(),
            cursor: None,
            preflight_markers: existing_markers,
            canonical_vectors: 0,
            existing_markers: 0,
            marker_writes: 0,
        }
    } else {
        MigrationState::Preflight {
            target: target.clone(),
            cursor: required_cursor(last_key, "preflight")?,
            existing_markers,
        }
    };
    transaction.put(job_key(scope), encode_json(&job)?)?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryPreflightCommitBefore,
    )?;
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryPreflightCommitAfter,
    )?;
    log_batch(
        &target,
        "preflight",
        markers,
        markers,
        input_bytes,
        VectorWriteMeasurement::zero(),
        exhausted,
        cursor.is_some(),
        started,
    );
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the durable stage carries every exact cumulative correspondence count"
)]
async fn backfill<D: Distance>(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
    mut job: MigrationJob,
    target: MigrationTarget,
    cursor: Option<MigrationResumeKey>,
    preflight_markers: u64,
    canonical_vectors: u64,
    existing_markers: u64,
    marker_writes: u64,
) -> Result<()> {
    let started = std::time::Instant::now();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let (record, definition, index) = target_index::<D>(&transaction, scope, &target).await?;
    validate_reservation(&transaction, &record, &target).await?;
    let outcome = index
        .backfill_missing_simhash_directory(
            &transaction,
            cursor.as_ref().map(MigrationResumeKey::as_bytes),
            &definition,
            limits,
        )
        .await?;
    let vector::CanonicalVectorDirectoryBackfillOutcome::Valid {
        last_key,
        canonical_vectors: batch_vectors,
        existing_markers: batch_existing_markers,
        input_bytes,
        directory_entries,
        predicted_directory_writes,
        exhausted,
    } = outcome
    else {
        return canonical_backfill_error(outcome);
    };
    let recorder = VectorWriteRecorder::new();
    let measured_transaction = recorder.bind(&transaction);
    for entry in &directory_entries {
        index.stage_simhash_directory_entry(&measured_transaction, entry)?;
    }
    let actual_writes = measured_transaction.measurement().map_err(|error| {
        HelixDbError::InvariantViolation(format!(
            "active directory write measurement failed: {error}"
        ))
    })?;
    if actual_writes != predicted_directory_writes {
        return Err(corruption(
            "active directory writes differ from their admitted prediction",
        ));
    }
    let canonical_vectors = canonical_vectors
        .checked_add(batch_vectors)
        .ok_or_else(|| overflow("canonical vector count"))?;
    let existing_markers = existing_markers
        .checked_add(batch_existing_markers)
        .ok_or_else(|| overflow("existing marker count"))?;
    let marker_writes = marker_writes
        .checked_add(actual_writes.operations())
        .ok_or_else(|| overflow("marker write count"))?;
    job.counters =
        job.counters
            .checked_add(batch_vectors, batch_vectors, 0, input_bytes, actual_writes)?;
    job.state = if exhausted {
        if existing_markers != preflight_markers
            || canonical_vectors
                != existing_markers
                    .checked_add(marker_writes)
                    .ok_or_else(|| overflow("final marker correspondence"))?
        {
            return Err(corruption(
                "canonical vectors, existing markers, and marker writes do not correspond",
            ));
        }
        MigrationState::Verify {
            target: target.clone(),
            cursor: None,
            canonical_vectors,
            marker_writes,
            verified_markers: 0,
        }
    } else {
        MigrationState::Backfill {
            target: target.clone(),
            cursor: required_cursor(last_key, "backfill")?,
            preflight_markers,
            canonical_vectors,
            existing_markers,
            marker_writes,
        }
    };
    transaction.put(job_key(scope), encode_json(&job)?)?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryBackfillCommitBefore,
    )?;
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::VectorDirectoryBackfillCommitAfter)?;
    log_batch(
        &target,
        "backfill",
        batch_vectors,
        actual_writes.operations(),
        input_bytes,
        actual_writes,
        exhausted,
        cursor.is_some(),
        started,
    );
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the durable stage carries every exact cumulative correspondence count"
)]
async fn verify<D: Distance>(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
    mut job: MigrationJob,
    target: MigrationTarget,
    cursor: Option<MigrationResumeKey>,
    canonical_vectors: u64,
    marker_writes: u64,
    verified_markers: u64,
) -> Result<()> {
    let started = std::time::Instant::now();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let (record, definition, index) = target_index::<D>(&transaction, scope, &target).await?;
    validate_reservation(&transaction, &record, &target).await?;
    let outcome = index
        .validate_simhash_directory(
            &transaction,
            cursor.as_ref().map(MigrationResumeKey::as_bytes),
            &definition,
            vector::SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            limits.max_entities().get(),
            limits.max_input_bytes().get(),
        )
        .await?;
    let vector::SimHashDirectoryValidationOutcome::Valid {
        last_key,
        markers,
        input_bytes,
        exhausted,
    } = outcome
    else {
        return directory_validation_error(outcome, "verification");
    };
    let verified_markers = verified_markers
        .checked_add(markers)
        .ok_or_else(|| overflow("verified marker count"))?;
    if verified_markers > canonical_vectors {
        return Err(corruption(
            "active directory verification found extra markers",
        ));
    }
    job.counters = job.counters.checked_add(
        markers,
        0,
        markers,
        input_bytes,
        VectorWriteMeasurement::zero(),
    )?;
    job.state = if exhausted {
        if verified_markers != canonical_vectors {
            return Err(corruption(
                "active directory verification count differs from canonical vectors",
            ));
        }
        MigrationState::Publish {
            target: target.clone(),
            canonical_vectors,
            marker_writes,
            verified_markers,
        }
    } else {
        MigrationState::Verify {
            target: target.clone(),
            cursor: required_cursor(last_key, "verification")?,
            canonical_vectors,
            marker_writes,
            verified_markers,
        }
    };
    transaction.put(job_key(scope), encode_json(&job)?)?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryVerificationCommitBefore,
    )?;
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryVerificationCommitAfter,
    )?;
    log_batch(
        &target,
        "verify",
        markers,
        markers,
        input_bytes,
        VectorWriteMeasurement::zero(),
        exhausted,
        cursor.is_some(),
        started,
    );
    Ok(())
}

async fn publish(db: &Db, scope: DataScope, mut job: MigrationJob) -> Result<()> {
    let MigrationState::Publish {
        target,
        canonical_vectors,
        marker_writes,
        verified_markers,
    } = job.state.clone()
    else {
        return Err(corruption("publication received another migration stage"));
    };
    if canonical_vectors != verified_markers {
        return Err(corruption(
            "publication requires exact canonical and marker counts",
        ));
    }
    let started = std::time::Instant::now();
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let record = load_target_record(&transaction, scope, &target).await?;
    validate_reservation(&transaction, &record, &target).await?;
    let next_record = record
        .transition(IndexStateTransition::PublishSimHashDirectoryV1)
        .map_err(|error| corruption(error.to_string()))?;
    let operation_id =
        IndexOperationId::from_bytes(target.completed_build_operation_id).map_err(|error| {
            corruption(format!(
                "active directory target has invalid completed operation: {error}"
            ))
        })?;
    let operation_key = IndexKey::Data {
        scope,
        kind: ScopedKey::operation(operation_id),
    }
    .to_bytes();
    let Some(operation_value) = transaction.get(&operation_key).await? else {
        return Err(corruption(
            "active directory target retained operation is missing",
        ));
    };
    let operation = decode_operation_record(&operation_value)?;
    if operation.index_id() != record.index_id()
        || operation.generation() != record.state().generation()
        || operation.index_record_revision() != record.revision()
    {
        return Err(corruption(
            "active directory retained operation disagrees with its canonical record",
        ));
    }
    let next_operation = operation
        .rebind_completed_index_revision(next_record.revision())
        .map_err(|error| corruption(error.to_string()))?;
    job.completed_targets = job
        .completed_targets
        .checked_add(1)
        .ok_or_else(|| overflow("completed target count"))?;
    job.counters = job
        .counters
        .checked_add(0, 0, 0, 0, VectorWriteMeasurement::zero())?;
    job.state = MigrationState::SelectTarget {
        after_index_key: Some(target.index_key.clone()),
    };
    transaction.put(
        target.index_key.as_bytes(),
        encode_index_record(&next_record),
    )?;
    transaction.put(operation_key, encode_operation_record(&next_operation))?;
    transaction.put(job_key(scope), encode_json(&job)?)?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryPublicationCommitBefore,
    )?;
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(
        super::MigrationFailpoint::VectorDirectoryPublicationCommitAfter,
    )?;
    tracing::info!(
        migration = "vector_simhash_directory_v1",
        stage = "publish",
        index_id = target.index_id,
        generation = target.generation,
        physical_index_id = target.physical_index_id,
        canonical_vectors,
        marker_writes,
        verified_markers,
        elapsed_millis = started.elapsed().as_millis(),
        "published active vector SimHash directory"
    );
    Ok(())
}

async fn target_from_record(
    transaction: &DbTransaction,
    index_key: &[u8],
    record: &IndexRecordV2,
) -> Result<Option<MigrationTarget>> {
    let IndexStateV2::Active {
        physical,
        completed_build_operation_id,
    } = record.state()
    else {
        return Ok(None);
    };
    let PhysicalGeneration::Vector {
        generation,
        layout,
        descriptor,
    } = physical
    else {
        return Ok(None);
    };
    if descriptor.routing_layout() == VectorRoutingLayoutV2::SimHashDirectoryV1 {
        return Ok(None);
    }
    let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout else {
        return Ok(None);
    };
    let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
        return Err(corruption(
            "active vector physical has another definition family",
        ));
    };
    if definition.tenant_property().is_some() {
        return Err(corruption(
            "unpartitioned active legacy vector has a tenant-partitioned definition",
        ));
    }
    validate_reservation_record(
        transaction,
        *physical_index_id,
        record.index_id().get(),
        generation.get(),
    )
    .await?;
    Ok(Some(MigrationTarget {
        index_key: MigrationResumeKey::new(index_key.to_vec())
            .ok_or_else(|| corruption("active directory target key is empty"))?,
        index_id: record.index_id().get(),
        generation: generation.get(),
        physical_index_id: physical_index_id.get(),
        record_revision: record.revision().get(),
        completed_build_operation_id: *completed_build_operation_id.as_bytes(),
    }))
}

async fn load_target_record(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &MigrationTarget,
) -> Result<IndexRecordV2> {
    let Some(value) = transaction.get(target.index_key.as_bytes()).await? else {
        return Err(corruption("active directory migration target disappeared"));
    };
    let record = decode_canonical_record(scope, target.index_key.as_bytes(), &value)?;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                generation,
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                descriptor,
            },
        completed_build_operation_id,
    } = record.state()
    else {
        return Err(corruption(
            "active directory migration target changed state or layout",
        ));
    };
    if record.index_id().get() != target.index_id
        || generation.get() != target.generation
        || physical_index_id.get() != target.physical_index_id
        || record.revision().get() != target.record_revision
        || completed_build_operation_id.as_bytes() != &target.completed_build_operation_id
        || descriptor.routing_layout() != VectorRoutingLayoutV2::LegacyHnsw
    {
        return Err(corruption(
            "active directory migration target changed after selection",
        ));
    }
    Ok(record)
}

async fn target_index<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &MigrationTarget,
) -> Result<(
    IndexRecordV2,
    ValidatedVectorIndexDefinition,
    VectorIndex<D>,
)> {
    let record = load_target_record(transaction, scope, target).await?;
    let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
        return Err(corruption(
            "active directory target changed definition family",
        ));
    };
    let definition = definition.clone();
    let active = ActiveIndexHandle::try_from_record(scope, &record)
        .ok_or_else(|| corruption("active directory target no longer projects an active handle"))?;
    let physical_index_id =
        VectorPhysicalIndexId::new(target.physical_index_id).map_err(|error| {
            corruption(format!(
                "active directory target has invalid physical ID: {error}"
            ))
        })?;
    let generation =
        ValidatedVectorGenerationHandle::try_from_active::<D>(&active, physical_index_id)
            .map_err(|error| corruption(error.to_string()))?;
    let index = VectorIndex::<D>::from_generation(&generation);
    Ok((record, definition, index))
}

async fn validate_reservation(
    transaction: &DbTransaction,
    record: &IndexRecordV2,
    target: &MigrationTarget,
) -> Result<()> {
    validate_reservation_record(
        transaction,
        VectorPhysicalIndexId::new(target.physical_index_id)
            .map_err(|error| corruption(error.to_string()))?,
        record.index_id().get(),
        record.state().generation().get(),
    )
    .await
}

async fn validate_reservation_record(
    transaction: &DbTransaction,
    physical_index_id: VectorPhysicalIndexId,
    index_id: u64,
    generation: u64,
) -> Result<()> {
    let key = IndexKey::Global {
        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
    }
    .to_bytes();
    let Some(value) = transaction.get(key).await? else {
        return Err(corruption(
            "active legacy vector has no physical reservation",
        ));
    };
    let IndexV2MetadataValue::LegacyVectorPhysicalReservation(reservation) =
        decode_metadata_value(&value)?
    else {
        return Err(corruption(
            "active legacy reservation key contains another value kind",
        ));
    };
    let LegacyVectorPhysicalReservation::AdoptedActive {
        index_id: owner_index,
        generation: owner_generation,
    } = reservation
    else {
        return Err(corruption(
            "active legacy vector does not own an AdoptedActive reservation",
        ));
    };
    if owner_index.get() != index_id || owner_generation.get() != generation {
        return Err(corruption(
            "active legacy vector reservation belongs to another generation",
        ));
    }
    Ok(())
}

fn decode_canonical_record(scope: DataScope, key: &[u8], value: &[u8]) -> Result<IndexRecordV2> {
    let IndexKey::Data {
        kind: ScopedKey::IndexRecord(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption(
            "canonical index prefix returned another key kind",
        ));
    };
    let record = decode_index_record(value)?;
    if key.identity != *record.identity() {
        return Err(corruption(
            "canonical index key identity differs from its value",
        ));
    }
    Ok(record)
}

fn directory_validation_error(
    outcome: vector::SimHashDirectoryValidationOutcome,
    stage: &'static str,
) -> Result<()> {
    match outcome {
        vector::SimHashDirectoryValidationOutcome::Oversized { observed, limit } => {
            Err(oversized(stage, observed, limit))
        }
        vector::SimHashDirectoryValidationOutcome::Invalid { reason } => Err(corruption(format!(
            "active directory {stage} failed: {reason}"
        ))),
        vector::SimHashDirectoryValidationOutcome::Valid { .. } => {
            Err(corruption("valid directory outcome escaped its stage"))
        }
    }
}

fn canonical_backfill_error(
    outcome: vector::CanonicalVectorDirectoryBackfillOutcome,
) -> Result<()> {
    match outcome {
        vector::CanonicalVectorDirectoryBackfillOutcome::Oversized { observed, limit } => {
            Err(oversized("canonical vector", observed, limit))
        }
        vector::CanonicalVectorDirectoryBackfillOutcome::Invalid { reason } => Err(corruption(
            format!("active directory canonical backfill failed: {reason}"),
        )),
        vector::CanonicalVectorDirectoryBackfillOutcome::Valid { .. } => {
            Err(corruption("valid canonical backfill escaped its stage"))
        }
    }
}

fn required_cursor(key: Option<Bytes>, stage: &'static str) -> Result<Option<MigrationResumeKey>> {
    let Some(key) = key else {
        return Err(corruption(format!(
            "non-exhausted active directory {stage} has no cursor"
        )));
    };
    Ok(Some(MigrationResumeKey::new(key.to_vec()).ok_or_else(
        || corruption("active directory cursor is empty"),
    )?))
}

#[allow(
    clippy::too_many_arguments,
    reason = "migration batch logs expose every required bounded resource without raw keys"
)]
fn log_batch(
    target: &MigrationTarget,
    stage: &'static str,
    validated_rows: u64,
    marker_count: u64,
    input_bytes: u64,
    writes: VectorWriteMeasurement,
    exhausted: bool,
    resumed_from_cursor: bool,
    started: std::time::Instant,
) {
    tracing::info!(
        migration = "vector_simhash_directory_v1",
        stage,
        index_id = target.index_id,
        generation = target.generation,
        physical_index_id = target.physical_index_id,
        validated_rows,
        marker_count,
        input_bytes,
        output_operations = writes.operations(),
        output_bytes = writes.encoded_bytes(),
        exhausted,
        resumed_from_cursor,
        elapsed_millis = started.elapsed().as_millis(),
        "advanced vector SimHash-directory migration batch"
    );
}

fn measured_row_bytes(key: &[u8], value: &[u8]) -> Result<u64> {
    key.len()
        .checked_add(value.len())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| overflow("row bytes"))
}

fn job_key(scope: DataScope) -> Bytes {
    scoped_metadata_key(scope, JOB_KEY)
}

fn overflow(resource: &'static str) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "VectorSimHashDirectoryV1 migration {resource} overflowed"
    ))
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

fn oversized(resource: &'static str, observed: u64, limit: u64) -> HelixDbError {
    HelixDbError::MigrationRequired {
        reason: format!(
            "VectorSimHashDirectoryV1 {resource} requires {observed} bytes but the batch limit is {limit}"
        ),
    }
}

/// Measured output from the fixed-shape release scale harness.
#[cfg(feature = "production-scale")]
pub(super) struct DirectoryScaleObservation {
    pub(super) validated_rows: u64,
    pub(super) canonical_payloads: u64,
    pub(super) marker_observations: u64,
    pub(super) marker_writes: u64,
    pub(super) input_bytes: u64,
    pub(super) output_bytes: u64,
    pub(super) batch_latencies: Vec<std::time::Duration>,
}

/// Runs the real controller without constructing a runtime catalog around the scale fixture.
#[cfg(feature = "production-scale")]
pub(super) async fn run_measured_for_scale(
    db: &Db,
    scope: DataScope,
    limits: SearchIndexBatchLimits,
) -> Result<DirectoryScaleObservation> {
    ensure_job(db, scope).await?;
    let mut batch_latencies = Vec::new();
    loop {
        let job = load_job(db, scope).await?;
        if matches!(job.state, MigrationState::Completed) {
            if job.counters.batches
                != u64::try_from(batch_latencies.len()).map_err(|_| {
                    HelixDbError::InvariantViolation(
                        "directory scale batch latency count overflowed".to_string(),
                    )
                })?
            {
                return Err(corruption(
                    "directory scale batch timings disagree with durable batches",
                ));
            }
            return Ok(DirectoryScaleObservation {
                validated_rows: job.counters.validated_rows,
                canonical_payloads: job.counters.canonical_payloads,
                marker_observations: job.counters.marker_count,
                marker_writes: job.counters.output_operations,
                input_bytes: job.counters.input_bytes,
                output_bytes: job.counters.output_bytes,
                batch_latencies,
            });
        }
        let started = std::time::Instant::now();
        process_once(db, scope, limits, job).await?;
        batch_latencies.push(started.elapsed());
    }
}

/// Projects the dedicated job into migration-parity status without exposing raw cursors.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub(super) async fn parity_status(
    db: &Db,
    scope: DataScope,
) -> Result<Option<super::MigrationParityJobStatus>> {
    let Some(value) = db.get(job_key(scope)).await? else {
        return Ok(None);
    };
    let job = decode_json::<MigrationJob>(&value)?;
    job.validate()?;
    let state = match &job.state {
        MigrationState::Completed => super::MigrationParityState::Completed {
            processed_rows: job.counters.validated_rows,
        },
        state @ (MigrationState::SelectTarget { .. }
        | MigrationState::Preflight { .. }
        | MigrationState::Backfill { .. }
        | MigrationState::Verify { .. }
        | MigrationState::Publish { .. }) => {
            let (stage, has_resume_key) = match state {
                MigrationState::SelectTarget { after_index_key } => (
                    super::MigrationParityStage::VectorDirectorySelectTarget,
                    after_index_key.is_some(),
                ),
                MigrationState::Preflight { cursor, .. } => (
                    super::MigrationParityStage::VectorDirectoryPreflight,
                    cursor.is_some(),
                ),
                MigrationState::Backfill { cursor, .. } => (
                    super::MigrationParityStage::VectorDirectoryBackfill,
                    cursor.is_some(),
                ),
                MigrationState::Verify { cursor, .. } => (
                    super::MigrationParityStage::VectorDirectoryVerify,
                    cursor.is_some(),
                ),
                MigrationState::Publish { .. } => {
                    (super::MigrationParityStage::VectorDirectoryPublish, false)
                }
                MigrationState::Completed => unreachable!(),
            };
            super::MigrationParityState::Running {
                stage,
                processed_rows: job.counters.validated_rows,
                has_resume_key,
            }
        }
    };
    Ok(Some(super::MigrationParityJobStatus {
        id: super::MigrationParityId::VectorSimHashDirectoryV1,
        mode: super::MigrationParityMode::BlockingStartup,
        state,
    }))
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::v1::keys::vectors::{
        VectorKey, VectorSimHashDirectoryKey, VectorStorageLane,
    };
    use crate::encoding::v1::values::vectors::markers::{
        decode_simhash_directory_marker_v1, encode_simhash_directory_marker_v1,
    };
    use crate::encoding::v2::values::encode_metadata_value;
    use crate::index_lifecycle::{
        BuildOperationOutcome, IndexGenerationId, IndexId, IndexOperationExecutionState,
        IndexOperationFamily, IndexOperationKind, IndexOperationOutcome, IndexOperationProgress,
        IndexOperationRecord, IndexOperationRevision, IndexRevision, NoCursorProgress,
        VectorBuildProgress, VectorBuildStage, VectorGenerationDescriptor,
    };
    use crate::search::vector::{
        CanonicalVectorDirectoryBackfillOutcome, SearchParams, SimHashDirectoryValidationMode,
        SimHashDirectoryValidationOutcome, VectorDistanceMetric, VectorIndexConfig,
    };

    #[cfg(not(feature = "production-coverage"))]
    static VECTOR_MIGRATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[cfg(feature = "production-coverage")]
    async fn migration_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        super::super::production_contracts::failpoint_contract_guard().await
    }

    #[cfg(not(feature = "production-coverage"))]
    async fn migration_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        VECTOR_MIGRATION_TEST_LOCK.lock().await
    }

    struct LegacyFixture<D: Distance> {
        record: IndexRecordV2,
        operation: IndexOperationRecord,
        active: ActiveIndexHandle,
        definition: ValidatedVectorIndexDefinition,
        index: VectorIndex<D>,
        physical_index_id: VectorPhysicalIndexId,
    }

    async fn test_db() -> Db {
        Db::builder(
            format!("vector-directory-{}", uuid::Uuid::new_v4()),
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .expect("test database opens")
    }

    fn definition(metric: VectorDistanceMetric, ordinal: u64) -> ValidatedVectorIndexDefinition {
        ValidatedVectorIndexDefinition::try_new(
            crate::index_lifecycle::IndexElementKind::Node,
            format!("Document{ordinal}"),
            "embedding",
            None::<String>,
            3,
            metric,
            16,
            32,
            64,
            0.5,
            4,
            0.75,
            false,
            0.25,
        )
        .expect("test vector definition validates")
    }

    async fn seed_active_legacy<D: Distance>(
        db: &Db,
        ordinal: u64,
        metric: VectorDistanceMetric,
        vectors: &[(u64, [f32; 3])],
    ) -> LegacyFixture<D> {
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(metric, ordinal);
        let index_id = IndexId::new(ordinal).expect("non-zero test index ID");
        let generation = IndexGenerationId::new(ordinal + 10).expect("non-zero generation");
        let physical_index_id =
            VectorPhysicalIndexId::new(ordinal + 100).expect("non-zero physical ID");
        let operation_id = IndexOperationId::from_bytes(u128::from(ordinal + 1).to_be_bytes())
            .expect("non-nil operation ID");
        let record = IndexRecordV2::building(
            index_id,
            ValidatedDynamicIndexDefinition::Vector(definition.clone()),
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation,
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                descriptor: VectorGenerationDescriptor::legacy_for_definition(&definition),
            },
            operation_id,
        )
        .expect("legacy record builds")
        .transition(IndexStateTransition::Activate)
        .expect("legacy record activates");
        let active = ActiveIndexHandle::try_from_record(scope, &record)
            .expect("active legacy record projects a handle");
        let validated =
            ValidatedVectorGenerationHandle::try_from_active::<D>(&active, physical_index_id)
                .expect("active legacy generation validates");
        let index = VectorIndex::<D>::from_generation(&validated);
        let create = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("create transaction opens");
        index
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(&definition, validated.physical_name()),
            )
            .await
            .expect("legacy physical creates");
        create.commit().await.expect("legacy create commits");
        for (entity_id, vector) in vectors {
            let insert = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("insert transaction opens");
            index
                .insert(&insert, *entity_id, vector)
                .await
                .expect("legacy vector inserts");
            insert.commit().await.expect("legacy insert commits");
        }

        let operation = IndexOperationRecord::try_new(
            operation_id,
            index_id,
            definition.identity(),
            generation,
            record.revision(),
            IndexOperationRevision::new(3).expect("operation revision is non-zero"),
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
        .expect("completed build operation validates");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("catalog transaction opens");
        transaction
            .put(
                IndexKey::Data {
                    scope,
                    kind: ScopedKey::index_record(record.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&record),
            )
            .expect("canonical record stages");
        transaction
            .put(
                IndexKey::Data {
                    scope,
                    kind: ScopedKey::operation(operation_id),
                }
                .to_bytes(),
                encode_operation_record(&operation),
            )
            .expect("completed operation stages");
        transaction
            .put(
                IndexKey::Global {
                    kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                }
                .to_bytes(),
                encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::AdoptedActive {
                        index_id,
                        generation,
                    },
                )),
            )
            .expect("active reservation stages");
        transaction.commit().await.expect("catalog seed commits");

        LegacyFixture {
            record,
            operation,
            active,
            definition,
            index,
            physical_index_id,
        }
    }

    fn one_row_limits() -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            NonZeroU64::new(32).unwrap(),
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        )
        .expect("one-row migration limits validate")
    }

    async fn run_to_completion(db: &Db, limits: SearchIndexBatchLimits) -> MigrationJob {
        let scope = DataScope::LegacyUnscoped;
        ensure_job(db, scope).await.expect("job initializes");
        for _ in 0..100 {
            let job = load_job(db, scope).await.expect("job loads");
            if matches!(job.state, MigrationState::Completed) {
                return job;
            }
            process_once(db, scope, limits, job)
                .await
                .expect("migration batch succeeds");
        }
        panic!("migration did not complete in 100 bounded steps")
    }

    async fn stage_one_existing_marker<D: Distance>(db: &Db, fixture: &LegacyFixture<D>) {
        let outcome = fixture
            .index
            .backfill_missing_simhash_directory(db, None, &fixture.definition, one_row_limits())
            .await
            .expect("canonical backfill plans");
        let CanonicalVectorDirectoryBackfillOutcome::Valid {
            directory_entries, ..
        } = outcome
        else {
            panic!("canonical backfill returns valid entries")
        };
        let entry = directory_entries
            .first()
            .expect("fixture has a missing marker");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("marker transaction opens");
        let recorder = VectorWriteRecorder::new();
        fixture
            .index
            .stage_simhash_directory_entry(&recorder.bind(&transaction), entry)
            .expect("existing marker stages");
        transaction.commit().await.expect("existing marker commits");
    }

    async fn non_directory_rows(db: &Db, physical_index_id: u64) -> Vec<(Bytes, Bytes)> {
        let scope = DataScope::LegacyUnscoped;
        let mut rows = Vec::new();
        for lane in VectorStorageLane::ALL {
            let prefix = Key::data_prefix(scope, lane.prefix_key(physical_index_id).to_bytes());
            let mut lane_rows = db.scan_prefix(prefix, ..).await.expect("vector lane scans");
            while let Some(row) = lane_rows.next().await.expect("vector row reads") {
                let logical = scope
                    .strip_key(&row.key)
                    .expect("vector row has the fixture scope");
                if matches!(
                    VectorKey::parse_from_slice(logical).expect("vector row key parses"),
                    VectorKey::SimHashDirectory(_)
                ) {
                    continue;
                }
                rows.push((row.key, row.value));
            }
        }
        rows
    }

    #[tokio::test]
    async fn zero_targets_is_a_durable_no_op() {
        let _failpoint_guard = migration_test_guard().await;
        let db = test_db().await;
        let job = run_to_completion(&db, one_row_limits()).await;
        assert_eq!(job.completed_targets, 0);
        assert_eq!(job.resume_count, 0);
        assert_eq!(job.counters.marker_count, 0);
        assert_eq!(job.counters.output_operations, 0);
    }

    #[tokio::test]
    async fn partial_directory_resumes_and_publishes_exact_correspondence() {
        let _failpoint_guard = migration_test_guard().await;
        let db = test_db().await;
        let fixture = seed_active_legacy::<vector::distance::Cosine>(
            &db,
            1,
            VectorDistanceMetric::Cosine,
            &[
                (1, [1.0, 0.0, 0.0]),
                (2, [0.0, 1.0, 0.0]),
                (3, [0.0, 0.0, 1.0]),
            ],
        )
        .await;
        stage_one_existing_marker(&db, &fixture).await;
        let before = non_directory_rows(&db, fixture.physical_index_id.get()).await;

        ensure_job(&db, DataScope::LegacyUnscoped)
            .await
            .expect("job initializes");
        let initial = load_job(&db, DataScope::LegacyUnscoped)
            .await
            .expect("initial job loads");
        process_once(&db, DataScope::LegacyUnscoped, one_row_limits(), initial)
            .await
            .expect("target selection commits");
        record_resume(&db, DataScope::LegacyUnscoped)
            .await
            .expect("cold resume records");
        let job = run_to_completion(&db, one_row_limits()).await;

        assert_eq!(job.completed_targets, 1);
        assert_eq!(job.resume_count, 1);
        assert_eq!(job.counters.canonical_payloads, 3);
        assert_eq!(job.counters.output_operations, 2);
        assert_eq!(
            non_directory_rows(&db, fixture.physical_index_id.get()).await,
            before,
            "migration preserves every non-directory physical row"
        );
        let record = crate::index_lifecycle::repository::load_index_record(
            &db,
            DataScope::LegacyUnscoped,
            fixture.record.identity(),
        )
        .await
        .expect("published record reads")
        .expect("published record exists");
        assert_eq!(
            record.revision(),
            fixture.record.revision().checked_next().unwrap()
        );
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    generation,
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    descriptor,
                },
            completed_build_operation_id,
        } = record.state()
        else {
            panic!("published record remains the active generation")
        };
        assert_eq!(*generation, fixture.record.state().generation());
        assert_eq!(*physical_index_id, fixture.physical_index_id);
        assert_eq!(
            descriptor.routing_layout(),
            VectorRoutingLayoutV2::SimHashDirectoryV1
        );
        assert_eq!(
            *completed_build_operation_id,
            fixture.operation.operation_id()
        );
        let operation_key = IndexKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: ScopedKey::operation(fixture.operation.operation_id()),
        }
        .to_bytes();
        let operation = decode_operation_record(
            &db.get(operation_key)
                .await
                .expect("operation reads")
                .expect("operation exists"),
        )
        .expect("operation decodes");
        assert_eq!(operation.index_record_revision(), record.revision());
        assert_eq!(
            operation.operation_revision(),
            fixture
                .operation
                .operation_revision()
                .checked_next()
                .unwrap()
        );
        assert_eq!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                fixture.physical_index_id,
            )
            .await
            .expect("reservation reads"),
            Some(LegacyVectorPhysicalReservation::AdoptedActive {
                index_id: record.index_id(),
                generation: record.state().generation(),
            })
        );
        assert!(
            crate::index_lifecycle::repository::revalidate_active_handle(&db, &fixture.active)
                .await
                .is_err(),
            "publication invalidates the stale legacy authorization"
        );
        let active = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
            .expect("published record projects a current handle");
        let generation =
            ValidatedVectorGenerationHandle::try_from_active::<vector::distance::Cosine>(
                &active,
                fixture.physical_index_id,
            )
            .expect("published generation validates");
        let current = VectorIndex::<vector::distance::Cosine>::from_generation(&generation);
        let verification = current
            .validate_simhash_directory(
                &db,
                None,
                &fixture.definition,
                SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
                16,
                8 * 1024 * 1024,
            )
            .await
            .expect("final directory validates");
        assert!(matches!(
            verification,
            SimHashDirectoryValidationOutcome::Valid {
                markers: 3,
                exhausted: true,
                ..
            }
        ));
        let hits = current
            .search(
                &db,
                &[1.0, 0.0, 0.0],
                &SearchParams::new(3).expect("search limit validates"),
            )
            .await
            .expect("unfiltered search remains available");
        assert_eq!(hits.len(), 3);
    }

    #[tokio::test]
    async fn all_supported_metrics_and_multiple_targets_upgrade() {
        let _failpoint_guard = migration_test_guard().await;
        let db = test_db().await;
        seed_active_legacy::<vector::distance::Cosine>(
            &db,
            11,
            VectorDistanceMetric::Cosine,
            &[(1, [1.0, 0.0, 0.0])],
        )
        .await;
        seed_active_legacy::<vector::distance::Euclidean>(
            &db,
            12,
            VectorDistanceMetric::Euclidean,
            &[(2, [1.0, 2.0, 3.0])],
        )
        .await;
        seed_active_legacy::<vector::distance::Manhattan>(
            &db,
            13,
            VectorDistanceMetric::Manhattan,
            &[(3, [3.0, 2.0, 1.0])],
        )
        .await;

        let job = run_to_completion(&db, one_row_limits()).await;
        assert_eq!(job.completed_targets, 3);
        assert_eq!(job.counters.canonical_payloads, 3);
        assert_eq!(job.counters.output_operations, 3);
    }

    #[tokio::test]
    async fn extra_marker_and_wrong_reservation_fail_closed() {
        let _failpoint_guard = migration_test_guard().await;
        let db = test_db().await;
        let fixture = seed_active_legacy::<vector::distance::Cosine>(
            &db,
            21,
            VectorDistanceMetric::Cosine,
            &[(1, [1.0, 0.0, 0.0])],
        )
        .await;
        db.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::Vector(VectorKey::SimHashDirectory(
                    VectorSimHashDirectoryKey::new(fixture.physical_index_id.get(), 77, 999),
                )),
            }
            .to_bytes(),
            encode_simhash_directory_marker_v1(),
        )
        .await
        .expect("extra marker writes");
        ensure_job(&db, DataScope::LegacyUnscoped)
            .await
            .expect("job initializes");
        let job = load_job(&db, DataScope::LegacyUnscoped)
            .await
            .expect("job loads");
        process_once(&db, DataScope::LegacyUnscoped, one_row_limits(), job)
            .await
            .expect("target selection succeeds");
        let job = load_job(&db, DataScope::LegacyUnscoped)
            .await
            .expect("preflight job loads");
        let error = process_once(&db, DataScope::LegacyUnscoped, one_row_limits(), job)
            .await
            .expect_err("extra marker is rejected");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        assert!(decode_simhash_directory_marker_v1(&encode_simhash_directory_marker_v1(),).is_ok());

        let reservation_key = IndexKey::Global {
            kind: GlobalKey::LegacyVectorPhysicalReservation(fixture.physical_index_id),
        }
        .to_bytes();
        db.put(
            reservation_key,
            encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                LegacyVectorPhysicalReservation::LegacySource,
            )),
        )
        .await
        .expect("wrong reservation writes");
        let selection = MigrationJob::initial();
        let error = process_once(&db, DataScope::LegacyUnscoped, one_row_limits(), selection)
            .await
            .expect_err("wrong reservation is rejected during selection");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
    }

    #[tokio::test]
    async fn reader_fallback_precedes_blocking_writer_startup_upgrade() {
        let _failpoint_guard = migration_test_guard().await;
        let token = crate::ProcessLocalDatabaseToken::new(format!(
            "vector-directory-startup-{}",
            uuid::Uuid::new_v4()
        ))
        .expect("process-local token creates");
        let raw = Db::builder(token.database(), token.object_store())
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("raw startup fixture opens");
        crate::index_lifecycle::repository::bootstrap_writer(&raw)
            .await
            .expect("V2 repository bootstraps");
        let fixture = seed_active_legacy::<vector::distance::Cosine>(
            &raw,
            31,
            VectorDistanceMetric::Cosine,
            &[(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])],
        )
        .await;
        let identity = fixture.record.identity().clone();
        raw.put(
            scoped_metadata_key(
                DataScope::LegacyUnscoped,
                super::super::INDEX_V2_MIGRATION_READY,
            ),
            Bytes::from_static(b"1"),
        )
        .await
        .expect("existing V2 readiness marker writes");
        super::super::ensure_graph_format_v1_ready(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("existing graph readiness marker writes");
        super::super::publish_storage_schema_completion(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("existing storage schema completion publishes");
        raw.close().await.expect("raw startup fixture closes");

        let reader = crate::HelixDB::open_reader(crate::HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .expect("reader opens before writer migration");
        let legacy = reader
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .into_iter()
            .find(|handle| handle.identity() == &identity)
            .expect("reader retains the active legacy fallback");
        let ActiveIndexHandle::Vector { descriptor, .. } = legacy else {
            panic!("reader fallback is a vector")
        };
        assert_eq!(
            descriptor.routing_layout(),
            VectorRoutingLayoutV2::LegacyHnsw
        );
        reader.close().await.expect("reader closes");

        let writer = crate::HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .expect("writer performs blocking directory migration");
        let current = writer
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .into_iter()
            .find(|handle| handle.identity() == &identity)
            .expect("writer refreshes the migrated runtime catalog");
        let ActiveIndexHandle::Vector { descriptor, .. } = current else {
            panic!("writer catalog entry is a vector")
        };
        assert_eq!(
            descriptor.routing_layout(),
            VectorRoutingLayoutV2::SimHashDirectoryV1
        );
        let job = load_job(writer.inner_db().as_ref(), DataScope::LegacyUnscoped)
            .await
            .expect("startup migration job reads");
        assert!(matches!(job.state, MigrationState::Completed));
        assert_eq!(job.completed_targets, 1);
        writer.close().await.expect("writer closes");
    }

    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    #[tokio::test]
    async fn every_directory_commit_boundary_recovers_without_duplicate_writes() {
        let _failpoint_guard = migration_test_guard().await;
        const BOUNDARIES: [super::super::MigrationFailpoint; 8] = [
            super::super::MigrationFailpoint::VectorDirectoryPreflightCommitBefore,
            super::super::MigrationFailpoint::VectorDirectoryPreflightCommitAfter,
            super::super::MigrationFailpoint::VectorDirectoryBackfillCommitBefore,
            super::super::MigrationFailpoint::VectorDirectoryBackfillCommitAfter,
            super::super::MigrationFailpoint::VectorDirectoryVerificationCommitBefore,
            super::super::MigrationFailpoint::VectorDirectoryVerificationCommitAfter,
            super::super::MigrationFailpoint::VectorDirectoryPublicationCommitBefore,
            super::super::MigrationFailpoint::VectorDirectoryPublicationCommitAfter,
        ];

        for (ordinal, failpoint) in BOUNDARIES.into_iter().enumerate() {
            let db = test_db().await;
            let fixture = seed_active_legacy::<vector::distance::Cosine>(
                &db,
                u64::try_from(ordinal).unwrap() + 41,
                VectorDistanceMetric::Cosine,
                &[(1, [1.0, 0.0, 0.0])],
            )
            .await;
            ensure_job(&db, DataScope::LegacyUnscoped)
                .await
                .expect("job initializes");
            super::super::inject_migration_failpoint_once(failpoint)
                .expect("directory failpoint injects");
            let mut interrupted = false;
            for _ in 0..32 {
                let job = load_job(&db, DataScope::LegacyUnscoped)
                    .await
                    .expect("job loads before injected boundary");
                match process_once(&db, DataScope::LegacyUnscoped, one_row_limits(), job).await {
                    Ok(()) => {}
                    Err(_) => {
                        interrupted = true;
                        break;
                    }
                }
            }
            assert!(interrupted, "{} interrupts migration", failpoint.as_str());
            assert!(
                super::super::migration_failpoint_was_triggered(),
                "{} must trigger",
                failpoint.as_str()
            );
            record_resume(&db, DataScope::LegacyUnscoped)
                .await
                .expect("cold recovery records its resume");
            let recovered = run_to_completion(&db, one_row_limits()).await;
            assert_eq!(recovered.completed_targets, 1);
            assert_eq!(recovered.resume_count, 1);
            assert_eq!(
                recovered.counters.output_operations,
                1,
                "{} must not duplicate its marker write",
                failpoint.as_str()
            );
            let record = crate::index_lifecycle::repository::load_index_record(
                &db,
                DataScope::LegacyUnscoped,
                fixture.record.identity(),
            )
            .await
            .expect("recovered record reads")
            .expect("recovered record exists");
            let IndexStateV2::Active {
                physical: PhysicalGeneration::Vector { descriptor, .. },
                ..
            } = record.state()
            else {
                panic!("recovered target remains active")
            };
            assert_eq!(
                descriptor.routing_layout(),
                VectorRoutingLayoutV2::SimHashDirectoryV1
            );
        }
    }

    #[test]
    fn durable_job_codec_round_trips_and_rejects_invalid_counts() {
        let mut job = MigrationJob::initial();
        job.state = MigrationState::Verify {
            target: MigrationTarget {
                index_key: MigrationResumeKey::new(vec![1]).unwrap(),
                index_id: 2,
                generation: 3,
                physical_index_id: 4,
                record_revision: 5,
                completed_build_operation_id: [6; 16],
            },
            cursor: Some(MigrationResumeKey::new(vec![7]).unwrap()),
            canonical_vectors: 8,
            marker_writes: 6,
            verified_markers: 7,
        };
        job.counters = MigrationCounters {
            validated_rows: 9,
            canonical_payloads: 8,
            marker_count: 10,
            input_bytes: 11,
            output_operations: 12,
            output_bytes: 13,
            batches: 14,
        };
        let encoded = encode_json(&job).expect("job encodes");
        let decoded = decode_json::<MigrationJob>(&encoded).expect("job decodes");
        assert_eq!(decoded, job);
        decoded.validate().expect("valid counts validate");

        let MigrationState::Verify { target, .. } = decoded.state else {
            unreachable!()
        };
        job.state = MigrationState::Verify {
            target,
            cursor: None,
            canonical_vectors: 1,
            marker_writes: 0,
            verified_markers: 2,
        };
        assert!(job.validate().is_err());
    }
}
