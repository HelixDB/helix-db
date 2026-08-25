//! Bounded outbox driver for hidden vector construction.
//!
//! Each source or catch-up step plans deterministic HNSW writes in a disposable
//! transaction, admits the complete last-write-wins vector write set, and then
//! applies those captured writes in the outbox transaction. The outbox
//! transaction also owns tenant mappings, builder-applied state,
//! delta deletion, and the next durable checkpoint.
//!
//! No vector row codec is defined here. Physical reads and writes remain behind
//! [`crate::search::vector::VectorIndex`] and the typed `encoding/v2` boundary.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rand::{rngs::StdRng, SeedableRng};
use sha2::{Digest, Sha256};
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::{IndexLifecycleScanTuning, SearchIndexBatchLimits};
use crate::encoding::property::{decode_properties, Property};
use crate::encoding::v2::keys::indexes::vector::VectorStorageLane;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, KeyPrefix};
use crate::encoding::v2::keys::{
    GlobalKey, IndexEntity, IndexEntityStateKey, RecordKind, ScopedKey,
};
#[cfg(test)]
use crate::encoding::v2::values::encode_build_delta;
use crate::encoding::v2::values::{
    decode_applied_state, decode_build_delta, decode_index_record, decode_partition_mapping,
    encode_applied_state, encode_metadata_value, encode_partition_mapping,
};
use crate::error::{HelixDbError, Result};
use crate::search::vector::{
    self, Distance, MeasuredVectorTransaction, PlannedVectorMutation,
    ValidatedVectorBuildGenerationHandle, ValidatedVectorCleanupAuthority, VectorBuildSession,
    VectorBuildSessionStats, VectorCleanupRow, VectorDistanceMetric, VectorIndex,
    VectorIndexConfig, VectorWriteMeasurement, VectorWriteRecorder,
};

use super::{vector_document, VectorIndexedDocument};
use crate::index_lifecycle::outbox::{
    IndexOperationDriver, IndexOperationStepExecution, IndexOperationStepPermit,
    IndexOperationStepResult, PreparedIndexOperationStep, StepResourceUsage, VectorPlanningUsage,
};
use crate::index_lifecycle::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaValue, VectorTenantPartition,
};
use crate::index_lifecycle::{
    BuildOperationOutcome, IndexCursor, IndexElementKind, IndexEntityId, IndexGenerationId,
    IndexId, IndexOperationBlocker, IndexOperationFamily, IndexOperationOutcome,
    IndexOperationProgress, IndexOperationRecord, IndexRecordV2, IndexV2MetadataValue,
    LegacyVectorDirectoryValidationProgress, LegacyVectorPhysicalReservation,
    LegacyVectorValidationLane, LegacyVectorValidationProgress, NoCursorProgress,
    OperationCounters, PhysicalGeneration, PrefixScanProgress, SourceScanProgress, TextPartition,
    ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition, VectorBuildProgress,
    VectorBuildStage, VectorCleanupProgress, VectorPhysicalIdWatermark, VectorPhysicalIndexId,
    VectorPhysicalLayout, VectorRoutingLayoutV2,
};

/// Vector lifecycle driver sharing scope gates and the bounded SimHash owner.
pub(crate) struct VectorIndexDriver {
    scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
    cache_registry: Arc<vector::VectorCacheRegistry>,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    scan_tuning: IndexLifecycleScanTuning,
}

impl core::fmt::Debug for VectorIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("VectorIndexDriver")
            .finish_non_exhaustive()
    }
}

impl VectorIndexDriver {
    /// Installs vector work against mutation and cache authorities.
    pub(crate) fn new(
        scope_gates: Arc<crate::index_lifecycle::IndexScopeGates>,
        cache_registry: Arc<vector::VectorCacheRegistry>,
        simhasher_registry: Arc<vector::SimHasherRegistry>,
    ) -> Self {
        Self {
            scope_gates,
            cache_registry,
            simhasher_registry,
            scan_tuning: IndexLifecycleScanTuning::default(),
        }
    }

    /// Applies runtime source-scan prefetching without admitting blocks to cache.
    pub(crate) const fn with_scan_tuning(mut self, scan_tuning: IndexLifecycleScanTuning) -> Self {
        self.scan_tuning = scan_tuning;
        self
    }
}

#[async_trait]
impl IndexOperationDriver for VectorIndexDriver {
    fn family(&self) -> IndexOperationFamily {
        IndexOperationFamily::Vector
    }

    async fn acquire_step_permit(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<Box<dyn IndexOperationStepPermit>> {
        let needs_exclusive = matches!(
            operation.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::AdoptLegacy(_)
                    | VectorBuildStage::ValidateAdoptedDirectory(_)
                    | VectorBuildStage::CatchUp(_)
                    | VectorBuildStage::ValidateDescriptor(_)
                    | VectorBuildStage::Activate(_)
            )) | IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
                | IndexOperationProgress::VectorCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.lifecycle_permit(scope).await));
        }
        Ok(Box::new(()))
    }

    async fn prepare_step(
        &self,
        _db: &Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        _limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        let permit = self.acquire_step_permit(scope, operation).await?;
        Ok(PreparedIndexOperationStep::driver_owned(
            self.family(),
            permit,
        ))
    }

    async fn step(
        &self,
        db: &Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepExecution> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            return Err(corruption("vector operation loaded another family"));
        };
        let step = match operation.progress() {
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)) => {
                match definition.metric() {
                    VectorDistanceMetric::Cosine => {
                        step_build::<vector::distance::Cosine>(
                            db,
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            stage,
                            limits,
                            self.scan_tuning,
                            Arc::clone(&self.simhasher_registry),
                        )
                        .await?
                    }
                    VectorDistanceMetric::Euclidean => {
                        step_build::<vector::distance::Euclidean>(
                            db,
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            stage,
                            limits,
                            self.scan_tuning,
                            Arc::clone(&self.simhasher_registry),
                        )
                        .await?
                    }
                    VectorDistanceMetric::Manhattan => {
                        step_build::<vector::distance::Manhattan>(
                            db,
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            stage,
                            limits,
                            self.scan_tuning,
                            Arc::clone(&self.simhasher_registry),
                        )
                        .await?
                    }
                }
            }
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
            | IndexOperationProgress::VectorCleanup(_) => {
                let (progress, aborting) = match operation.progress() {
                    IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                        progress,
                    )) => (progress, true),
                    IndexOperationProgress::VectorCleanup(progress) => (progress, false),
                    IndexOperationProgress::SecondaryBuild(_)
                    | IndexOperationProgress::TextBuild(_)
                    | IndexOperationProgress::SecondaryCleanup(_)
                    | IndexOperationProgress::TextCleanup(_)
                    | IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(_)) => {
                        return Err(corruption(
                            "vector cleanup dispatch selected a non-cleanup progress state",
                        ));
                    }
                };
                match definition.metric() {
                    VectorDistanceMetric::Cosine => {
                        step_cleanup::<vector::distance::Cosine>(
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            progress,
                            aborting,
                            limits,
                            &self.cache_registry,
                        )
                        .await?
                    }
                    VectorDistanceMetric::Euclidean => {
                        step_cleanup::<vector::distance::Euclidean>(
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            progress,
                            aborting,
                            limits,
                            &self.cache_registry,
                        )
                        .await?
                    }
                    VectorDistanceMetric::Manhattan => {
                        step_cleanup::<vector::distance::Manhattan>(
                            transaction,
                            scope,
                            operation,
                            &record,
                            definition,
                            progress,
                            aborting,
                            limits,
                            &self.cache_registry,
                        )
                        .await?
                    }
                }
            }
            IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => {
                return Err(corruption("vector driver received another family progress"));
            }
        };
        Ok(step.into_execution())
    }

    async fn after_commit(
        &self,
        scope: DataScope,
        index: &IndexRecordV2,
        operation: &IndexOperationRecord,
        committed: crate::index_lifecycle::outbox::CommittedOperationStep,
    ) {
        if committed != crate::index_lifecycle::outbox::CommittedOperationStep::Completed
            || !matches!(
                operation.progress(),
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
                    VectorCleanupProgress::Finalize(_)
                )) | IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(_))
            )
        {
            return;
        }
        let ValidatedDynamicIndexDefinition::Vector(definition) = index.definition() else {
            return;
        };
        let authority = match definition.metric() {
            VectorDistanceMetric::Cosine => ValidatedVectorCleanupAuthority::try_from_cleaning::<
                vector::distance::Cosine,
            >(scope, index, operation.operation_id()),
            VectorDistanceMetric::Euclidean => {
                ValidatedVectorCleanupAuthority::try_from_cleaning::<vector::distance::Euclidean>(
                    scope,
                    index,
                    operation.operation_id(),
                )
            }
            VectorDistanceMetric::Manhattan => {
                ValidatedVectorCleanupAuthority::try_from_cleaning::<vector::distance::Manhattan>(
                    scope,
                    index,
                    operation.operation_id(),
                )
            }
        };
        let Ok(authority) = authority else {
            tracing::error!(
                operation_id = %operation.operation_id().as_uuid(),
                "committed vector cleanup could not reconstruct its cache authority"
            );
            return;
        };
        if !self.cache_registry.forget_cleanup_generation(&authority) {
            tracing::error!(
                operation_id = %operation.operation_id().as_uuid(),
                "committed vector cleanup retained a non-closed cache generation"
            );
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "cleanup binds the exact canonical owner, cache fence, progress, and batch limits"
)]
async fn step_cleanup<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &VectorCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
    cache_registry: &vector::VectorCacheRegistry,
) -> Result<VectorStepResult> {
    let authority = ValidatedVectorCleanupAuthority::try_from_cleaning::<D>(
        scope,
        record,
        operation.operation_id(),
    )
    .map_err(|error| corruption(error.to_string()))?;
    if matches!(
        progress,
        VectorCleanupProgress::DeletePhysical(_)
            | VectorCleanupProgress::DeleteDeltas(_)
            | VectorCleanupProgress::Finalize(_)
    ) {
        cache_registry.retire_cleanup_generation(&authority).await;
    }
    let next = match progress {
        VectorCleanupProgress::RetireCache(progress) => {
            cache_registry.retire_cleanup_generation(&authority).await;
            VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                cursor: None,
                counters: progress.counters,
            })
        }
        VectorCleanupProgress::DeletePhysical(progress) => match authority.layout() {
            VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                if progress.cursor.is_some() {
                    return Err(corruption(
                        "unpartitioned vector cleanup retained a mapping cursor",
                    ));
                }
                if aborting
                    && let Some(reservation) =
                        super::super::repository::load_legacy_vector_physical_reservation(
                            transaction,
                            physical_index_id,
                        )
                        .await?
                {
                    if !matches!(
                        reservation,
                        LegacyVectorPhysicalReservation::AdoptionBuilding { .. }
                    ) {
                        return Err(corruption(
                            "vector abort found a non-building legacy reservation",
                        ));
                    }
                    let handle = authority
                        .physical_generation::<D>(physical_index_id)
                        .map_err(|error| corruption(error.to_string()))?;
                    let legacy = VectorIndex::<D>::from_generation(&handle);
                    let (cleanup, measured) = delete_simhash_directory(
                        transaction,
                        &legacy,
                        definition.element_kind(),
                        progress.counters,
                        limits,
                    )
                    .await?;
                    let PhysicalCleanupOutcome::Progress {
                        counters,
                        namespace_empty,
                        mapping_deleted: false,
                    } = cleanup
                    else {
                        return match cleanup {
                            PhysicalCleanupOutcome::Blocked(blocker) => {
                                Ok(VectorStepResult::ordinary(
                                    IndexOperationStepResult::Blocked(blocker),
                                ))
                            }
                            PhysicalCleanupOutcome::Progress {
                                mapping_deleted: true,
                                ..
                            } => Err(corruption(
                                "legacy directory cleanup deleted a partition mapping",
                            )),
                            PhysicalCleanupOutcome::Progress {
                                mapping_deleted: false,
                                ..
                            } => unreachable!(),
                        };
                    };
                    if !namespace_empty {
                        return Ok(VectorStepResult::vector_writes(
                            progressed_cleanup(
                                true,
                                VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                                    cursor: None,
                                    counters,
                                }),
                            ),
                            measured,
                        ));
                    }
                    let Some(source_reservation) = reservation.abort(
                        operation.index_id(),
                        operation.generation(),
                        operation.operation_id(),
                    ) else {
                        return Err(corruption(
                            "vector abort found a reservation owned by another generation",
                        ));
                    };
                    transaction.put(
                        IndexKey::Global {
                            kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                        }
                        .to_bytes(),
                        encode_metadata_value(
                            &IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                                source_reservation,
                            ),
                        ),
                    )?;
                    return Ok(VectorStepResult::vector_writes(
                        progressed_cleanup(
                            true,
                            VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                                cursor: None,
                                counters,
                            }),
                        ),
                        measured,
                    ));
                }
                let handle = authority
                    .physical_generation::<D>(physical_index_id)
                    .map_err(|error| corruption(error.to_string()))?;
                match delete_physical_namespace::<D>(
                    transaction,
                    &handle,
                    None,
                    definition.element_kind(),
                    progress.counters,
                    limits,
                )
                .await?
                {
                    PhysicalCleanupOutcome::Blocked(blocker) => {
                        return Ok(VectorStepResult::ordinary(
                            IndexOperationStepResult::Blocked(blocker),
                        ));
                    }
                    PhysicalCleanupOutcome::Progress {
                        counters,
                        namespace_empty,
                        mapping_deleted: false,
                    } if namespace_empty => {
                        VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                            cursor: None,
                            counters,
                        })
                    }
                    PhysicalCleanupOutcome::Progress {
                        counters,
                        mapping_deleted: false,
                        ..
                    } => VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                        cursor: None,
                        counters,
                    }),
                    PhysicalCleanupOutcome::Progress {
                        mapping_deleted: true,
                        ..
                    } => {
                        return Err(corruption(
                            "unpartitioned vector cleanup deleted a partition mapping",
                        ));
                    }
                }
            }
            VectorPhysicalLayout::Partitioned => {
                let mapping = current_or_next_mapping(
                    transaction,
                    scope,
                    operation,
                    progress.cursor.as_ref(),
                )
                .await?;
                let Some(mapping) = mapping else {
                    return Ok(VectorStepResult::ordinary(progressed_cleanup(
                        aborting,
                        VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                            cursor: None,
                            counters: progress.counters,
                        }),
                    )));
                };
                let handle = authority
                    .physical_generation::<D>(mapping.value.physical_index_id)
                    .map_err(|error| corruption(error.to_string()))?;
                match delete_physical_namespace::<D>(
                    transaction,
                    &handle,
                    Some(&mapping),
                    definition.element_kind(),
                    progress.counters,
                    limits,
                )
                .await?
                {
                    PhysicalCleanupOutcome::Blocked(blocker) => {
                        return Ok(VectorStepResult::ordinary(
                            IndexOperationStepResult::Blocked(blocker),
                        ));
                    }
                    PhysicalCleanupOutcome::Progress { counters, .. } => {
                        VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                            cursor: Some(mapping.cursor),
                            counters,
                        })
                    }
                }
            }
        },
        VectorCleanupProgress::DeleteDeltas(progress) => {
            if progress.cursor.is_some() {
                return Err(corruption(
                    "vector delta cleanup uses delete-from-prefix rather than a stale cursor",
                ));
            }
            match delete_delta_and_applied_rows(
                transaction,
                scope,
                operation,
                progress.counters,
                limits,
            )
            .await?
            {
                CleanupWorkOutcome::Blocked(blocker) => {
                    return Ok(VectorStepResult::ordinary(
                        IndexOperationStepResult::Blocked(blocker),
                    ));
                }
                CleanupWorkOutcome::Progress {
                    counters,
                    exhausted: false,
                } => VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                    cursor: None,
                    counters,
                }),
                CleanupWorkOutcome::Progress {
                    counters,
                    exhausted: true,
                } => VectorCleanupProgress::Finalize(NoCursorProgress { counters }),
            }
        }
        VectorCleanupProgress::Finalize(_) => {
            if !aborting
                && let VectorPhysicalLayout::Unpartitioned { physical_index_id } =
                    authority.layout()
                && let Some(reservation) =
                    super::super::repository::load_legacy_vector_physical_reservation(
                        transaction,
                        physical_index_id,
                    )
                    .await?
            {
                if !reservation.is_owned_by(operation.index_id(), operation.generation()) {
                    return Err(corruption(
                        "vector drop found a reservation owned by another generation",
                    ));
                }
                transaction.delete(
                    IndexKey::Global {
                        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                    }
                    .to_bytes(),
                )?;
            }
            return Ok(VectorStepResult::ordinary(
                IndexOperationStepResult::Completed(if aborting {
                    IndexOperationOutcome::Build(BuildOperationOutcome::Aborted)
                } else {
                    IndexOperationOutcome::DropSucceeded
                }),
            ));
        }
    };
    Ok(VectorStepResult::ordinary(progressed_cleanup(
        aborting, next,
    )))
}

/// One partition mapping retained until its physical namespace is empty.
struct MappingCleanupRow {
    key: Bytes,
    cursor: IndexCursor,
    input_bytes: u64,
    value: crate::index_lifecycle::work::VectorPartitionMappingValue,
}

async fn current_or_next_mapping(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: Option<&IndexCursor>,
) -> Result<Option<MappingCleanupRow>> {
    let prefix = generation_prefix(
        scope,
        RecordKind::VectorPartitionMapping,
        operation.index_id(),
        operation.generation(),
    );
    if let Some(cursor) = cursor {
        cursor_suffix(&prefix, Some(cursor))?;
        if let Some(value) = transaction.get(cursor.as_bytes()).await? {
            let key = Bytes::copy_from_slice(cursor.as_bytes());
            let decoded = decode_mapping(scope, &key, &value, operation)?;
            return Ok(Some(MappingCleanupRow {
                input_bytes: key.len().saturating_add(value.len()) as u64,
                key,
                cursor: cursor.clone(),
                value: decoded,
            }));
        }
    }
    let start = cursor_suffix(&prefix, cursor)?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let value = decode_mapping(scope, &row.key, &row.value, operation)?;
    Ok(Some(MappingCleanupRow {
        input_bytes: row.key.len().saturating_add(row.value.len()) as u64,
        cursor: IndexCursor::try_new(row.key.clone()).map_err(operation_error)?,
        key: row.key,
        value,
    }))
}

enum PhysicalCleanupOutcome {
    Progress {
        counters: OperationCounters,
        namespace_empty: bool,
        mapping_deleted: bool,
    },
    Blocked(IndexOperationBlocker),
}

async fn delete_simhash_directory<D: Distance>(
    transaction: &DbTransaction,
    index: &VectorIndex<D>,
    entity_kind: IndexElementKind,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<(PhysicalCleanupOutcome, VectorWriteMeasurement)> {
    let mut scan = index.simhash_directory_cleanup_scan(transaction).await?;
    let mut rows = Vec::<VectorCleanupRow>::new();
    let mut input_bytes = 0_u64;
    let mut predicted_output_bytes = 0_u64;
    let mut namespace_empty = true;
    loop {
        if rows.len() >= limits.max_entities().get() {
            namespace_empty = false;
            break;
        }
        let Some(row) = scan.next().await? else {
            break;
        };
        let next_input = input_bytes.saturating_add(row.input_bytes());
        let next_operations = rows.len().saturating_add(1) as u64;
        let next_output_bytes = predicted_output_bytes.saturating_add(row.output_bytes());
        if next_input > limits.max_input_bytes().get()
            || next_operations > limits.max_output_operations().get()
            || next_output_bytes > limits.max_output_bytes().get()
        {
            if rows.is_empty() {
                return Ok((
                    PhysicalCleanupOutcome::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind,
                        entity_id: IndexEntityId::initial(),
                        observed: next_input.max(next_output_bytes),
                        limit: limits
                            .max_input_bytes()
                            .get()
                            .min(limits.max_output_bytes().get()),
                    }),
                    VectorWriteMeasurement::zero(),
                ));
            }
            namespace_empty = false;
            break;
        }
        input_bytes = next_input;
        predicted_output_bytes = next_output_bytes;
        rows.push(row);
    }
    let recorder = VectorWriteRecorder::new();
    let write = recorder.bind(transaction);
    for row in &rows {
        index.stage_cleanup_row(&write, row)?;
    }
    let measured = write.measurement().map_err(measurement_error)?;
    if measured.operations() != rows.len() as u64
        || measured.encoded_bytes() != predicted_output_bytes
    {
        return Err(corruption(
            "SimHash directory cleanup measurement disagrees with staged deletes",
        ));
    }
    let counters = OperationCounters {
        entities: checked_add(
            counters.entities,
            rows.len() as u64,
            "directory cleanup entities",
        )?,
        input_bytes: checked_add(
            counters.input_bytes,
            input_bytes,
            "directory cleanup input bytes",
        )?,
        output_operations: checked_add(
            counters.output_operations,
            measured.operations(),
            "directory cleanup output operations",
        )?,
        output_bytes: checked_add(
            counters.output_bytes,
            measured.encoded_bytes(),
            "directory cleanup output bytes",
        )?,
    };
    Ok((
        PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty,
            mapping_deleted: false,
        },
        measured,
    ))
}

async fn delete_physical_namespace<D: Distance>(
    transaction: &DbTransaction,
    handle: &crate::search::vector::ValidatedVectorGenerationHandle,
    mapping: Option<&MappingCleanupRow>,
    entity_kind: IndexElementKind,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<PhysicalCleanupOutcome> {
    let mapping_input_bytes = mapping.map_or(0, |mapping| mapping.input_bytes);
    if mapping_input_bytes > limits.max_input_bytes().get() {
        return Ok(PhysicalCleanupOutcome::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind,
                entity_id: IndexEntityId::initial(),
                observed: mapping_input_bytes,
                limit: limits.max_input_bytes().get(),
            },
        ));
    }
    let index = VectorIndex::<D>::from_generation(handle);
    let mut scan = index.cleanup_scan(transaction).await?;
    let mut rows = Vec::<VectorCleanupRow>::new();
    let mut input_bytes = mapping_input_bytes;
    let mut predicted_output_bytes = 0_u64;
    let mut namespace_empty = true;
    // Physical storage rows are not decoded source entities. Cleanup retains
    // only typed delete tokens and is bounded by the transaction's input,
    // output-operation, and output-byte ceilings below.
    loop {
        let Some(row) = scan.next().await? else {
            break;
        };
        let next_input = input_bytes.saturating_add(row.input_bytes());
        let next_operations = rows.len().saturating_add(1) as u64;
        let next_output_bytes = predicted_output_bytes.saturating_add(row.output_bytes());
        if next_input > limits.max_input_bytes().get()
            || next_operations > limits.max_output_operations().get()
            || next_output_bytes > limits.max_output_bytes().get()
        {
            if rows.is_empty() {
                return Ok(PhysicalCleanupOutcome::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind,
                        entity_id: IndexEntityId::initial(),
                        observed: next_input.max(next_output_bytes),
                        limit: limits
                            .max_input_bytes()
                            .get()
                            .min(limits.max_output_bytes().get()),
                    },
                ));
            }
            namespace_empty = false;
            break;
        }
        input_bytes = next_input;
        predicted_output_bytes = next_output_bytes;
        rows.push(row);
    }

    let mapping_delete_bytes = mapping.map_or(0, |mapping| mapping.key.len() as u64);
    let can_delete_mapping = namespace_empty
        && mapping.is_some()
        && (rows.len() as u64).saturating_add(1) <= limits.max_output_operations().get()
        && predicted_output_bytes.saturating_add(mapping_delete_bytes)
            <= limits.max_output_bytes().get();
    if namespace_empty && mapping.is_some() && !can_delete_mapping && rows.is_empty() {
        return Ok(PhysicalCleanupOutcome::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind,
                entity_id: IndexEntityId::initial(),
                observed: mapping_input_bytes.max(mapping_delete_bytes),
                limit: limits
                    .max_input_bytes()
                    .get()
                    .min(limits.max_output_bytes().get()),
            },
        ));
    }

    let recorder = VectorWriteRecorder::new();
    let write = recorder.bind(transaction);
    for row in &rows {
        index.stage_cleanup_row(&write, row)?;
    }
    let measured = write.measurement().map_err(measurement_error)?;
    if measured.operations() != rows.len() as u64
        || measured.encoded_bytes() != predicted_output_bytes
    {
        return Err(corruption(
            "vector cleanup token measurement disagrees with staged deletes",
        ));
    }
    if can_delete_mapping {
        let Some(mapping) = mapping else {
            return Err(corruption(
                "vector cleanup admitted a mapping delete without a mapping",
            ));
        };
        transaction.delete(&mapping.key)?;
    }
    let entities = rows.len() as u64 + u64::from(can_delete_mapping && rows.is_empty());
    let counters = OperationCounters {
        entities: checked_add(counters.entities, entities, "cumulative entities")?,
        input_bytes: checked_add(counters.input_bytes, input_bytes, "cumulative input bytes")?,
        output_operations: checked_add(
            counters.output_operations,
            measured.operations() + u64::from(can_delete_mapping),
            "cumulative output operations",
        )?,
        output_bytes: checked_add(
            counters.output_bytes,
            measured.encoded_bytes()
                + if can_delete_mapping {
                    mapping_delete_bytes
                } else {
                    0
                },
            "cumulative output bytes",
        )?,
    };
    Ok(PhysicalCleanupOutcome::Progress {
        counters,
        namespace_empty,
        mapping_deleted: can_delete_mapping,
    })
}

enum CleanupWorkOutcome {
    Progress {
        counters: OperationCounters,
        exhausted: bool,
    },
    Blocked(IndexOperationBlocker),
}

async fn delete_delta_and_applied_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<CleanupWorkOutcome> {
    let mut accounting = VectorBatchAccounting::new(counters, limits);
    let mut exhausted = true;
    for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
        let prefix = generation_prefix(scope, kind, operation.index_id(), operation.generation());
        let mut rows = transaction.scan_prefix(&prefix, ..).await?;
        while accounting.can_read_another() {
            let Some(row) = rows.next().await? else {
                break;
            };
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            let output_bytes = row.key.len() as u64;
            if !accounting.can_admit_input(input_bytes)
                || !accounting.can_admit_output(VectorWriteMeasurement::zero(), 1, output_bytes)
            {
                if accounting.is_empty() {
                    let entity = if kind == RecordKind::BuildDelta {
                        decode_delta(scope, &row.key, &row.value)?.0
                    } else {
                        decode_applied(scope, &row.key, &row.value)?.0
                    };
                    return Ok(CleanupWorkOutcome::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                            observed: input_bytes.max(output_bytes),
                            limit: limits
                                .max_input_bytes()
                                .get()
                                .min(limits.max_output_bytes().get()),
                        },
                    ));
                }
                exhausted = false;
                break;
            }
            transaction.delete(&row.key)?;
            accounting.admit(
                input_bytes,
                VectorWriteMeasurement::zero(),
                0,
                1,
                output_bytes,
            )?;
        }
        if !accounting.can_read_another() {
            exhausted = false;
            break;
        }
        if !exhausted {
            break;
        }
    }
    Ok(CleanupWorkOutcome::Progress {
        counters: accounting.finish()?,
        exhausted,
    })
}

fn progressed_cleanup(aborting: bool, progress: VectorCleanupProgress) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(if aborting {
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(progress))
    } else {
        IndexOperationProgress::VectorCleanup(progress)
    })
}

struct VectorStepResult {
    result: IndexOperationStepResult,
    single_vector_output_bytes: u64,
    physical_operations: u64,
    output_bytes: u64,
    vector_planning: VectorPlanningUsage,
}

impl VectorStepResult {
    fn ordinary(result: IndexOperationStepResult) -> Self {
        Self {
            result,
            single_vector_output_bytes: 0,
            physical_operations: 0,
            output_bytes: 0,
            vector_planning: VectorPlanningUsage::default(),
        }
    }

    fn metadata_transcode(
        result: IndexOperationStepResult,
        measurement: VectorWriteMeasurement,
    ) -> Self {
        Self {
            result,
            single_vector_output_bytes: 0,
            physical_operations: measurement.operations(),
            output_bytes: measurement.encoded_bytes(),
            vector_planning: VectorPlanningUsage::default(),
        }
    }

    fn vector_writes(
        result: IndexOperationStepResult,
        measurement: VectorWriteMeasurement,
    ) -> Self {
        Self {
            result,
            single_vector_output_bytes: 0,
            physical_operations: measurement.operations(),
            output_bytes: measurement.encoded_bytes(),
            vector_planning: VectorPlanningUsage::default(),
        }
    }

    fn with_vector_planning(mut self, vector_planning: VectorPlanningUsage) -> Self {
        self.vector_planning = vector_planning;
        self
    }

    fn into_execution(self) -> IndexOperationStepExecution {
        IndexOperationStepExecution::new(self.result).with_resources(StepResourceUsage {
            physical_operations: self.physical_operations,
            output_bytes: self.output_bytes,
            single_vector_output_bytes: self.single_vector_output_bytes,
            vector_planning: self.vector_planning,
            ..StepResourceUsage::default()
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "one outbox step binds the exact durable operation, descriptor, limits, and runtime projection owner"
)]
async fn step_build<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    stage: &VectorBuildStage,
    limits: SearchIndexBatchLimits,
    scan_tuning: IndexLifecycleScanTuning,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<VectorStepResult> {
    match stage {
        VectorBuildStage::AdoptLegacy(progress) => {
            adopt_legacy::<D>(
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
            )
            .await
        }
        VectorBuildStage::ValidateAdoptedDirectory(progress) => {
            validate_adopted_directory::<D>(
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
            )
            .await
        }
        VectorBuildStage::Scan(progress) => {
            scan_source::<D>(
                db,
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
                scan_tuning,
                simhasher_registry,
            )
            .await
        }
        VectorBuildStage::CatchUp(progress) => {
            catch_up::<D>(
                db,
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
                simhasher_registry,
            )
            .await
        }
        VectorBuildStage::ValidateDescriptor(progress) => Ok(VectorStepResult::ordinary(
            validate_descriptor::<D>(
                db,
                transaction,
                scope,
                operation,
                record,
                definition,
                progress,
                limits,
                simhasher_registry,
            )
            .await?,
        )),
        VectorBuildStage::Activate(progress) => {
            let Some(PhysicalGeneration::Vector { layout, .. }) = record.state().physical() else {
                return Err(corruption(
                    "vector activation is not bound to a physical generation",
                ));
            };
            let physical_index_id = match layout {
                VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                    Some(*physical_index_id)
                }
                VectorPhysicalLayout::Partitioned => None,
            };
            if let Some((physical_index_id, reservation)) = match physical_index_id {
                Some(physical_index_id) => {
                    super::super::repository::load_legacy_vector_physical_reservation(
                        transaction,
                        physical_index_id,
                    )
                    .await?
                    .map(|reservation| (physical_index_id, reservation))
                }
                None => None,
            } {
                let Some(active_reservation) = reservation.activate(
                    operation.index_id(),
                    operation.generation(),
                    operation.operation_id(),
                ) else {
                    return Err(corruption(
                        "vector activation found a non-adoptable physical reservation",
                    ));
                };
                if generation_has_rows(
                    transaction,
                    scope,
                    RecordKind::BuildDelta,
                    operation.index_id(),
                    operation.generation(),
                )
                .await?
                    || generation_has_rows(
                        transaction,
                        scope,
                        RecordKind::AppliedState,
                        operation.index_id(),
                        operation.generation(),
                    )
                    .await?
                {
                    return Err(corruption(
                        "legacy vector adoption unexpectedly produced graph build rows",
                    ));
                }
                let source = crate::migrations::legacy_vector_adoption_source(
                    transaction,
                    scope,
                    definition,
                )
                .await?;
                if crate::search::vector::index_id_from_name(source.physical_name())
                    != physical_index_id.get()
                {
                    return Err(corruption(
                        "legacy vector activation source differs from its reserved namespace",
                    ));
                }
                let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
                    scope,
                    record,
                    operation.operation_id(),
                    physical_index_id,
                )
                .map_err(|error| corruption(error.to_string()))?;
                let legacy = VectorIndex::<D>::for_legacy_migration(source.physical_name(), scope);
                #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorMetadataPublicationBefore,
                )?;
                let measurement = legacy
                    .transcode_legacy_metadata(
                        transaction,
                        definition,
                        handle.generation().physical_name(),
                    )
                    .await?;
                if measurement.operations() != 1 {
                    return Err(corruption(
                        "legacy vector activation did not transcode exactly one metadata row",
                    ));
                }
                #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorMetadataPublicationAfter,
                )?;
                #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorReservationTransitionBefore,
                )?;
                transaction.put(
                    IndexKey::Global {
                        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                    }
                    .to_bytes(),
                    encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                        active_reservation,
                    )),
                )?;
                #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyVectorReservationTransitionAfter,
                )?;
                #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyDefinitionRetirementBefore,
                )?;
                transaction.delete(source.storage_key())?;
                #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                crate::migrations::trip_migration_failpoint(
                    crate::migrations::MigrationFailpoint::LegacyDefinitionRetirementAfter,
                )?;
                tracing::info!(
                    operation_id = %operation.operation_id().as_uuid(),
                    physical_index_id = physical_index_id.get(),
                    metadata_output_bytes = measurement.encoded_bytes(),
                    "adopted legacy vector namespace without rebuilding HNSW rows"
                );
                return Ok(VectorStepResult::metadata_transcode(
                    IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                        BuildOperationOutcome::Succeeded,
                    )),
                    measurement,
                ));
            }
            if generation_has_rows(
                transaction,
                scope,
                RecordKind::BuildDelta,
                operation.index_id(),
                operation.generation(),
            )
            .await?
            {
                return Ok(VectorStepResult::ordinary(progressed_build(
                    VectorBuildStage::CatchUp(PrefixScanProgress {
                        cursor: None,
                        counters: progress.counters,
                    }),
                )));
            }
            if generation_has_rows(
                transaction,
                scope,
                RecordKind::AppliedState,
                operation.index_id(),
                operation.generation(),
            )
            .await?
            {
                return Ok(VectorStepResult::ordinary(progressed_build(
                    VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                        cursor: None,
                        counters: progress.counters,
                    }),
                )));
            }
            Ok(VectorStepResult::ordinary(
                IndexOperationStepResult::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Succeeded,
                )),
            ))
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "legacy validation binds exact catalog, operation, namespace, and batch authorities"
)]
async fn adopt_legacy<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &LegacyVectorValidationProgress,
    limits: SearchIndexBatchLimits,
) -> Result<VectorStepResult> {
    let Some(PhysicalGeneration::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        descriptor,
        ..
    }) = record.state().physical()
    else {
        return Err(corruption(
            "legacy vector adoption is not bound to one unpartitioned physical namespace",
        ));
    };
    let Some(reservation) = super::super::repository::load_legacy_vector_physical_reservation(
        transaction,
        *physical_index_id,
    )
    .await?
    else {
        return Err(corruption(
            "legacy vector adoption lost its physical reservation",
        ));
    };
    if reservation
        != (LegacyVectorPhysicalReservation::AdoptionBuilding {
            index_id: operation.index_id(),
            generation: operation.generation(),
            operation_id: operation.operation_id(),
        })
    {
        return Err(corruption(
            "legacy vector adoption reservation belongs to another generation",
        ));
    }
    let runtime = definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    if crate::search::vector::index_id_from_name(&legacy_name) != physical_index_id.get() {
        return Err(corruption(
            "legacy vector adoption physical ID differs from its deterministic name",
        ));
    }
    let lane = match progress.lane {
        LegacyVectorValidationLane::Core => VectorStorageLane::Core,
        LegacyVectorValidationLane::Hot => VectorStorageLane::Hot,
        LegacyVectorValidationLane::Layer0 => VectorStorageLane::Layer0,
    };
    let started = std::time::Instant::now();
    let legacy = VectorIndex::<D>::for_legacy_migration(legacy_name, scope);
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    crate::migrations::trip_migration_failpoint(
        crate::migrations::MigrationFailpoint::LegacyVectorValidationCheckpointBefore,
    )?;
    let outcome = legacy
        .validate_legacy_physical(
            transaction,
            vector::LegacyVectorValidationPass::new(
                lane,
                match descriptor.routing_layout() {
                    VectorRoutingLayoutV2::LegacyHnsw => {
                        vector::LegacyVectorValidationMode::ReadOnly
                    }
                    VectorRoutingLayoutV2::SimHashDirectoryV1 => {
                        vector::LegacyVectorValidationMode::BackfillSimHashDirectory {
                            max_output_operations: limits.max_output_operations(),
                            max_output_bytes: limits.max_output_bytes(),
                        }
                    }
                },
            ),
            progress
                .cursor
                .as_ref()
                .map(|cursor| cursor.as_bytes().as_ref()),
            definition,
            limits.max_entities().get(),
            limits.max_input_bytes().get(),
        )
        .await?;
    let vector::LegacyVectorValidationOutcome::Valid {
        last_key,
        rows,
        input_bytes,
        exhausted,
        directory_entries,
        predicted_directory_writes,
    } = outcome
    else {
        match outcome {
            vector::LegacyVectorValidationOutcome::Oversized { observed, limit } => {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id: IndexEntityId::initial(),
                        observed,
                        limit,
                    }),
                ));
            }
            vector::LegacyVectorValidationOutcome::Invalid { reason } => {
                tracing::error!(
                    operation_id = %operation.operation_id().as_uuid(),
                    lane = ?progress.lane,
                    reason,
                    "legacy vector physical validation failed"
                );
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
                ));
            }
            vector::LegacyVectorValidationOutcome::Valid { .. } => unreachable!(),
        }
    };
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    crate::migrations::trip_migration_failpoint(
        crate::migrations::MigrationFailpoint::LegacyVectorValidationCheckpointAfter,
    )?;
    let recorder = VectorWriteRecorder::new();
    let measured_transaction = recorder.bind(transaction);
    for entry in &directory_entries {
        legacy.stage_simhash_directory_entry(&measured_transaction, entry)?;
    }
    let actual_directory_writes = measured_transaction
        .measurement()
        .map_err(|error| corruption(format!("directory write measurement failed: {error}")))?;
    if actual_directory_writes != predicted_directory_writes {
        return Err(corruption(
            "typed legacy directory writes differ from their admitted prediction",
        ));
    }
    let counters = OperationCounters {
        entities: checked_add(
            progress.counters.entities,
            rows,
            "legacy validation entities",
        )?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            input_bytes,
            "legacy validation input bytes",
        )?,
        output_operations: checked_add(
            progress.counters.output_operations,
            actual_directory_writes.operations(),
            "legacy directory output operations",
        )?,
        output_bytes: checked_add(
            progress.counters.output_bytes,
            actual_directory_writes.encoded_bytes(),
            "legacy directory output bytes",
        )?,
    };
    tracing::info!(
        operation_id = %operation.operation_id().as_uuid(),
        lane = ?progress.lane,
        rows,
        marker_count = actual_directory_writes.operations(),
        input_bytes,
        output_bytes = actual_directory_writes.encoded_bytes(),
        cursor = ?last_key,
        exhausted,
        elapsed_millis = started.elapsed().as_millis(),
        "validated legacy vector physical checkpoint"
    );
    let next = if exhausted {
        match progress.lane.next() {
            Some(lane) => VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                lane,
                cursor: None,
                counters,
            }),
            None => match descriptor.routing_layout() {
                VectorRoutingLayoutV2::LegacyHnsw => {
                    VectorBuildStage::Activate(NoCursorProgress { counters })
                }
                VectorRoutingLayoutV2::SimHashDirectoryV1 => {
                    VectorBuildStage::ValidateAdoptedDirectory(
                        LegacyVectorDirectoryValidationProgress::initial(
                            counters.output_operations,
                            counters,
                        ),
                    )
                }
            },
        }
    } else {
        let Some(last_key) = last_key else {
            return Err(corruption(
                "non-exhausted legacy validation batch has no completed cursor",
            ));
        };
        VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
            lane: progress.lane,
            cursor: Some(IndexCursor::try_new(last_key).map_err(operation_error)?),
            counters,
        })
    };
    Ok(VectorStepResult::vector_writes(
        progressed_build(next),
        actual_directory_writes,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "directory validation binds exact catalog, operation, namespace, and batch authorities"
)]
async fn validate_adopted_directory<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &LegacyVectorDirectoryValidationProgress,
    limits: SearchIndexBatchLimits,
) -> Result<VectorStepResult> {
    let Some(PhysicalGeneration::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        descriptor,
        ..
    }) = record.state().physical()
    else {
        return Err(corruption(
            "legacy directory validation is not bound to one unpartitioned namespace",
        ));
    };
    if descriptor.routing_layout() != VectorRoutingLayoutV2::SimHashDirectoryV1 {
        return Err(corruption(
            "legacy directory validation is bound to a non-directory descriptor",
        ));
    }
    if progress.expected_markers != progress.counters.output_operations
        || progress.verified_markers > progress.expected_markers
    {
        return Err(corruption(
            "legacy directory validation counters disagree with marker writes",
        ));
    }
    let Some(reservation) = super::super::repository::load_legacy_vector_physical_reservation(
        transaction,
        *physical_index_id,
    )
    .await?
    else {
        return Err(corruption(
            "legacy directory validation lost its physical reservation",
        ));
    };
    if reservation
        != (LegacyVectorPhysicalReservation::AdoptionBuilding {
            index_id: operation.index_id(),
            generation: operation.generation(),
            operation_id: operation.operation_id(),
        })
    {
        return Err(corruption(
            "legacy directory validation reservation belongs to another generation",
        ));
    }
    let runtime = definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    if crate::search::vector::index_id_from_name(&legacy_name) != physical_index_id.get() {
        return Err(corruption(
            "legacy directory validation physical ID differs from its deterministic name",
        ));
    }
    let started = std::time::Instant::now();
    let legacy = VectorIndex::<D>::for_legacy_migration(legacy_name, scope);
    let outcome = legacy
        .validate_simhash_directory(
            transaction,
            progress
                .cursor
                .as_ref()
                .map(|cursor| cursor.as_bytes().as_ref()),
            definition,
            vector::SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
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
        match outcome {
            vector::SimHashDirectoryValidationOutcome::Oversized { observed, limit } => {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id: IndexEntityId::initial(),
                        observed,
                        limit,
                    }),
                ));
            }
            vector::SimHashDirectoryValidationOutcome::Invalid { reason } => {
                tracing::error!(
                    operation_id = %operation.operation_id().as_uuid(),
                    reason,
                    "legacy vector directory validation failed"
                );
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
                ));
            }
            vector::SimHashDirectoryValidationOutcome::Valid { .. } => unreachable!(),
        }
    };
    let verified_markers = checked_add(
        progress.verified_markers,
        markers,
        "verified legacy directory markers",
    )?;
    if verified_markers > progress.expected_markers {
        tracing::error!(
            operation_id = %operation.operation_id().as_uuid(),
            expected_markers = progress.expected_markers,
            verified_markers,
            "legacy vector directory contains extra markers"
        );
        return Ok(VectorStepResult::ordinary(
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
        ));
    }
    let counters = OperationCounters {
        entities: checked_add(
            progress.counters.entities,
            markers,
            "legacy directory validation entities",
        )?,
        input_bytes: checked_add(
            progress.counters.input_bytes,
            input_bytes,
            "legacy directory validation input bytes",
        )?,
        output_operations: progress.counters.output_operations,
        output_bytes: progress.counters.output_bytes,
    };
    tracing::info!(
        operation_id = %operation.operation_id().as_uuid(),
        stage = "validate_adopted_directory",
        markers,
        input_bytes,
        cursor = ?last_key,
        exhausted,
        verified_markers,
        expected_markers = progress.expected_markers,
        elapsed_millis = started.elapsed().as_millis(),
        "validated legacy vector directory checkpoint"
    );
    let next = if exhausted {
        if verified_markers != progress.expected_markers {
            tracing::error!(
                operation_id = %operation.operation_id().as_uuid(),
                expected_markers = progress.expected_markers,
                verified_markers,
                "legacy vector directory marker count is incomplete"
            );
            return Ok(VectorStepResult::ordinary(
                IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidLegacyPhysical),
            ));
        }
        VectorBuildStage::Activate(NoCursorProgress { counters })
    } else {
        let Some(last_key) = last_key else {
            return Err(corruption(
                "non-exhausted legacy directory batch has no completed cursor",
            ));
        };
        VectorBuildStage::ValidateAdoptedDirectory(LegacyVectorDirectoryValidationProgress {
            cursor: Some(IndexCursor::try_new(last_key).map_err(operation_error)?),
            expected_markers: progress.expected_markers,
            verified_markers,
            counters,
        })
    };
    Ok(VectorStepResult::ordinary(progressed_build(next)))
}

#[allow(
    clippy::too_many_arguments,
    reason = "source scanning retains exact operation and physical planning authority"
)]
async fn scan_source<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
    scan_tuning: IndexLifecycleScanTuning,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<VectorStepResult> {
    let source_prefix = source_prefix(scope, definition.element_kind());
    let start = cursor_suffix(&source_prefix, progress.cursor.as_ref())?;
    let upper = cursor_suffix(&source_prefix, Some(&progress.inclusive_upper_bound))?
        .ok_or_else(|| corruption("vector source upper bound is absent"))?;
    match start.as_ref().map(|start| start.cmp(&upper)) {
        Some(std::cmp::Ordering::Greater) => {
            return Err(corruption(
                "vector source cursor exceeds its inclusive upper bound",
            ));
        }
        Some(std::cmp::Ordering::Equal) => {
            return Ok(VectorStepResult::ordinary(progressed_build(
                VectorBuildStage::CatchUp(PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters,
                }),
            )));
        }
        Some(std::cmp::Ordering::Less) | None => {}
    }
    let start = start.map_or(Bound::Unbounded, Bound::Excluded);
    let scan_options = scan_tuning.scan_options();
    let mut rows = transaction
        .scan_prefix_with_options(
            &source_prefix,
            (start, Bound::Included(upper)),
            &scan_options,
        )
        .await?;
    let planning = db.begin(IsolationLevel::Snapshot).await?;
    let planning_recorder = VectorWriteRecorder::new();
    let mut build_session = VectorBuildSession::<D>::new(limits.max_input_bytes());
    let mut accounting = VectorBatchAccounting::new(progress.counters, limits);
    let mut cursor = progress.cursor.clone();
    let mut exhausted = true;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if !accounting.can_admit_input(input_bytes) {
            if accounting.is_empty() {
                let entity_id = source_entity(scope, definition.element_kind(), &row.key)?
                    .unwrap_or(IndexEntityId::initial());
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    }),
                ));
            }
            exhausted = false;
            break;
        }
        let complete_cursor = IndexCursor::try_new(row.key.clone()).map_err(operation_error)?;
        let Some(entity_id) = source_entity(scope, definition.element_kind(), &row.key)? else {
            accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0, 0)?;
            cursor = Some(complete_cursor);
            continue;
        };
        let properties = match decode_properties(&row.value) {
            Ok(properties) => properties,
            Err(_) => {
                return Ok(VectorStepResult::ordinary(invalid_source(
                    definition.element_kind(),
                    entity_id,
                )));
            }
        };
        let document = match vector_document(definition, &properties) {
            Ok(document) => document,
            Err(_) => {
                return Ok(VectorStepResult::ordinary(invalid_source(
                    definition.element_kind(),
                    entity_id,
                )));
            }
        };
        if load_applied(
            transaction,
            scope,
            operation.index_id(),
            operation.generation(),
            definition.element_kind(),
            entity_id,
        )
        .await?
        .is_some()
        {
            return Err(corruption(
                "vector source cursor has not advanced past existing applied state",
            ));
        }
        let outcome = plan_and_apply::<D>(
            &planning,
            &planning_recorder,
            transaction,
            scope,
            operation,
            record,
            definition,
            Arc::clone(&simhasher_registry),
            entity_id,
            None,
            document.as_ref(),
            true,
            false,
            &accounting,
            &mut build_session,
        )
        .await?;
        accounting.record_planning();
        let EntityPlanOutcome::Admitted {
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
            next_partition,
        } = outcome
        else {
            return finish_or_block_scan(
                outcome,
                accounting,
                definition.element_kind(),
                entity_id,
                progress,
                cursor,
                build_session.stats(),
            );
        };
        if next_partition.is_some() {
            stage_applied(
                transaction,
                scope,
                operation,
                definition.element_kind(),
                entity_id,
                next_partition,
            )?;
        }
        accounting.admit(
            input_bytes,
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
        )?;
        cursor = Some(complete_cursor);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let vector_planning = accounting.planning_usage(build_session.stats());
    let (counters, single_vector_output_bytes) = accounting.finish_with_max()?;
    let next = if exhausted {
        VectorBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters,
        })
    } else {
        VectorBuildStage::Scan(SourceScanProgress {
            inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
            cursor,
            counters,
        })
    };
    Ok(VectorStepResult {
        result: progressed_build(next),
        single_vector_output_bytes,
        physical_operations: 0,
        output_bytes: 0,
        vector_planning,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "catch-up retains exact operation and physical planning authority"
)]
async fn catch_up<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<VectorStepResult> {
    let prefix = generation_prefix(
        scope,
        RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    );
    let mut rows = transaction.scan_prefix(&prefix, ..).await?;
    let planning = db.begin(IsolationLevel::Snapshot).await?;
    let planning_recorder = VectorWriteRecorder::new();
    let mut build_session = VectorBuildSession::<D>::new(limits.max_input_bytes());
    let mut accounting = VectorBatchAccounting::new(progress.counters, limits);
    let mut saw_row = false;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        saw_row = true;
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        let (entity, delta) = decode_delta(scope, &row.key, &row.value)?;
        if delta.index_id != operation.index_id()
            || delta.generation != operation.generation()
            || entity.kind != definition.element_kind()
        {
            return Err(corruption("vector delta ownership mismatch"));
        }
        if !accounting.can_admit_input(input_bytes) {
            if accounting.is_empty() {
                return Ok(VectorStepResult::ordinary(
                    IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    }),
                ));
            }
            break;
        }
        let previous = load_applied(
            transaction,
            scope,
            operation.index_id(),
            operation.generation(),
            entity.kind,
            entity.id,
        )
        .await?;
        let properties = read_authoritative_properties(transaction, scope, entity).await?;
        let next = match properties {
            Some(properties) => match vector_document(definition, &properties) {
                Ok(document) => document,
                Err(_) => {
                    return Ok(VectorStepResult::ordinary(invalid_source(
                        entity.kind,
                        entity.id,
                    )));
                }
            },
            None => None,
        };
        let outcome = plan_and_apply::<D>(
            &planning,
            &planning_recorder,
            transaction,
            scope,
            operation,
            record,
            definition,
            Arc::clone(&simhasher_registry),
            entity.id,
            previous.as_ref(),
            next.as_ref(),
            false,
            true,
            &accounting,
            &mut build_session,
        )
        .await?;
        accounting.record_planning();
        let EntityPlanOutcome::Admitted {
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
            next_partition,
        } = outcome
        else {
            if let EntityPlanOutcome::Blocked(blocker) = outcome {
                return Ok(
                    VectorStepResult::ordinary(IndexOperationStepResult::Blocked(blocker))
                        .with_vector_planning(accounting.planning_usage(build_session.stats())),
                );
            }
            break;
        };
        if previous.is_some() || next_partition.is_some() {
            stage_applied(
                transaction,
                scope,
                operation,
                entity.kind,
                entity.id,
                next_partition,
            )?;
        }
        transaction.delete(row.key)?;
        accounting.admit(
            input_bytes,
            vector_writes,
            single_vector_output_bytes,
            lifecycle_operations,
            lifecycle_bytes,
        )?;
    }
    let vector_planning = accounting.planning_usage(build_session.stats());
    let (counters, single_vector_output_bytes) = accounting.finish_with_max()?;
    if saw_row {
        return Ok(VectorStepResult {
            result: progressed_build(VectorBuildStage::CatchUp(PrefixScanProgress {
                cursor: None,
                counters,
            })),
            single_vector_output_bytes,
            physical_operations: 0,
            output_bytes: 0,
            vector_planning,
        });
    }
    Ok(VectorStepResult {
        result: progressed_build(VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
            cursor: None,
            counters,
        })),
        single_vector_output_bytes,
        physical_operations: 0,
        output_bytes: 0,
        vector_planning,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "planning binds the exact operation, descriptor, source state, and target transaction"
)]
async fn plan_and_apply<D: Distance>(
    planning: &DbTransaction,
    planning_recorder: &VectorWriteRecorder,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    entity_id: IndexEntityId,
    previous_partition: Option<&TextPartition>,
    next_document: Option<&VectorIndexedDocument>,
    known_fresh: bool,
    delete_delta: bool,
    accounting: &VectorBatchAccounting,
    build_session: &mut VectorBuildSession<D>,
) -> Result<EntityPlanOutcome> {
    let next_partition = next_document.map(|document| document.partition().clone());
    let next_resolution = match next_document {
        Some(document) => Some(
            resolve_build_physical(
                transaction,
                scope,
                operation,
                record,
                document.partition(),
                true,
            )
            .await?,
        ),
        None => None,
    };
    let previous_resolution = match previous_partition {
        Some(partition)
            if Some(partition) != next_document.map(VectorIndexedDocument::partition) =>
        {
            Some(
                resolve_build_physical(transaction, scope, operation, record, partition, false)
                    .await?,
            )
        }
        Some(_) | None => None,
    };
    let layer = next_document
        .map(|document| deterministic_layer(operation, definition, entity_id, document));
    let planning_write = planning_recorder.bind(planning);
    let checkpoint = planning_write.checkpoint();
    apply_planned_change::<D>(
        &planning_write,
        operation,
        record,
        definition,
        Arc::clone(&simhasher_registry),
        entity_id,
        previous_resolution.as_ref(),
        next_resolution.as_ref(),
        next_document,
        layer,
        known_fresh,
        build_session,
    )
    .await?;
    build_session.flush_all(&planning_write)?;
    build_session.enforce_limits(&planning_write)?;
    let plan: PlannedVectorMutation = planning_write
        .plan_since(checkpoint)
        .map_err(measurement_error)?;
    let entity_vector = plan.measurement();
    let cumulative_vector = planning_write.measurement().map_err(measurement_error)?;
    let applied_transition = match (previous_partition.is_some(), next_partition.as_ref()) {
        (_, Some(partition)) => AppliedStateTransition::Put(partition),
        (true, None) => AppliedStateTransition::Delete,
        (false, None) => AppliedStateTransition::Absent,
    };
    let (lifecycle_operations, lifecycle_bytes) = lifecycle_write_measurement(
        scope,
        operation,
        definition.element_kind(),
        entity_id,
        applied_transition,
        next_resolution.as_ref(),
        delete_delta,
    )?;
    if entity_vector.encoded_bytes() > accounting.limits.max_single_vector_output_bytes().get() {
        return Ok(EntityPlanOutcome::Blocked(
            IndexOperationBlocker::OversizedEntity {
                entity_kind: definition.element_kind(),
                entity_id,
                observed: entity_vector.encoded_bytes(),
                limit: accounting.limits.max_single_vector_output_bytes().get(),
            },
        ));
    }
    if !accounting.can_admit_output(cumulative_vector, lifecycle_operations, lifecycle_bytes) {
        if accounting.is_empty() {
            return Ok(EntityPlanOutcome::Blocked(
                IndexOperationBlocker::OversizedEntity {
                    entity_kind: definition.element_kind(),
                    entity_id,
                    observed: cumulative_vector
                        .encoded_bytes()
                        .saturating_add(accounting.lifecycle_bytes)
                        .saturating_add(lifecycle_bytes),
                    limit: accounting.limits.max_output_bytes().get(),
                },
            ));
        }
        return Ok(EntityPlanOutcome::BatchFull);
    }
    if let Some(resolution) = next_resolution.as_ref()
        && resolution.mapping_is_new
    {
        let Some(partition) = next_partition.clone() else {
            return Err(corruption("new vector mapping has no partition"));
        };
        let partition = VectorTenantPartition::try_from_partition(partition)
            .map_err(|error| corruption(error.to_string()))?;
        let allocated = crate::index_lifecycle::repository::stage_vector_partition_mapping(
            transaction,
            scope,
            operation.index_id(),
            operation.generation(),
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await?;
        if allocated != resolution.physical_index_id {
            return Err(corruption(
                "vector physical allocation changed after admitted planning",
            ));
        }
    }
    plan.apply_to(transaction)?;
    Ok(EntityPlanOutcome::Admitted {
        vector_writes: cumulative_vector,
        single_vector_output_bytes: entity_vector.encoded_bytes(),
        lifecycle_operations,
        lifecycle_bytes,
        next_partition,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one deterministic HNSW plan binds both partition endpoints and exact build authority"
)]
async fn apply_planned_change<D: Distance>(
    write: &MeasuredVectorTransaction<'_>,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    entity_id: IndexEntityId,
    previous: Option<&BuildPhysicalResolution>,
    next: Option<&BuildPhysicalResolution>,
    next_document: Option<&VectorIndexedDocument>,
    layer: Option<u16>,
    known_fresh: bool,
    build_session: &mut VectorBuildSession<D>,
) -> Result<()> {
    if let Some(previous) = previous {
        let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
            previous.scope,
            record,
            operation.operation_id(),
            previous.physical_index_id,
        )
        .map_err(|error| corruption(error.to_string()))?;
        let index = VectorIndex::<D>::from_generation(handle.generation())
            .with_simhasher_registry(Arc::clone(&simhasher_registry));
        index
            .stage_delete_with_build_session(write, entity_id.get(), build_session)
            .await?;
    }
    let (Some(next), Some(document), Some(layer)) = (next, next_document, layer) else {
        return Ok(());
    };
    let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
        next.scope,
        record,
        operation.operation_id(),
        next.physical_index_id,
    )
    .map_err(|error| corruption(error.to_string()))?;
    let index = VectorIndex::<D>::from_generation(handle.generation())
        .with_simhasher_registry(simhasher_registry);
    let metadata = index.get_metadata(write).await?;
    if metadata.is_none() {
        if !next.mapping_is_new
            && !matches!(next.layout, VectorPhysicalLayout::Unpartitioned { .. })
        {
            return Err(corruption(
                "persisted vector partition mapping has no physical metadata",
            ));
        }
        index
            .stage_create(
                write,
                VectorIndexConfig::from_v2_definition(
                    definition,
                    handle.generation().physical_name(),
                ),
            )
            .await?;
    }
    if known_fresh {
        index
            .stage_known_fresh_at_layer_with_session(
                write,
                entity_id.get(),
                document.vector(),
                layer,
                handle.fresh_insert_proof(),
                build_session,
            )
            .await
    } else {
        index
            .stage_upsert_at_layer_with_session(
                write,
                entity_id.get(),
                document.vector(),
                layer,
                build_session,
            )
            .await
    }
}

async fn resolve_build_physical(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    partition: &TextPartition,
    create_missing: bool,
) -> Result<BuildPhysicalResolution> {
    let IndexStateVectorPhysical { layout } = IndexStateVectorPhysical::from_record(record)?;
    match (layout, partition) {
        (
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
            TextPartition::Unpartitioned,
        ) => Ok(BuildPhysicalResolution {
            scope,
            layout,
            physical_index_id,
            mapping_is_new: false,
        }),
        (VectorPhysicalLayout::Partitioned, TextPartition::TenantValue(_)) => {
            let tenant = VectorTenantPartition::try_from_partition(partition.clone())
                .map_err(|error| corruption(error.to_string()))?;
            if let Some(physical_index_id) =
                crate::index_lifecycle::repository::load_vector_partition_mapping(
                    transaction,
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    layout,
                    &tenant,
                )
                .await?
            {
                return Ok(BuildPhysicalResolution {
                    scope,
                    layout,
                    physical_index_id,
                    mapping_is_new: false,
                });
            }
            if !create_missing {
                return Err(corruption(
                    "builder-applied vector partition has no physical mapping",
                ));
            }
            Ok(BuildPhysicalResolution {
                scope,
                layout,
                physical_index_id: crate::index_lifecycle::repository::peek_vector_physical_id(
                    transaction,
                )
                .await?,
                mapping_is_new: true,
            })
        }
        (VectorPhysicalLayout::Unpartitioned { .. }, TextPartition::TenantValue(_))
        | (VectorPhysicalLayout::Partitioned, TextPartition::Unpartitioned) => Err(corruption(
            "vector build document partition disagrees with physical layout",
        )),
    }
}

#[derive(Debug, Clone, Copy)]
struct BuildPhysicalResolution {
    scope: DataScope,
    layout: VectorPhysicalLayout,
    physical_index_id: VectorPhysicalIndexId,
    mapping_is_new: bool,
}

struct IndexStateVectorPhysical {
    layout: VectorPhysicalLayout,
}

impl IndexStateVectorPhysical {
    fn from_record(record: &IndexRecordV2) -> Result<Self> {
        let Some(PhysicalGeneration::Vector { layout, .. }) = record.state().physical() else {
            return Err(corruption(
                "vector operation record has another physical family",
            ));
        };
        Ok(Self { layout: *layout })
    }
}

fn deterministic_layer(
    operation: &IndexOperationRecord,
    definition: &ValidatedVectorIndexDefinition,
    entity_id: IndexEntityId,
    document: &VectorIndexedDocument,
) -> u16 {
    let mut digest = Sha256::new();
    digest.update(operation.index_id().get().to_be_bytes());
    digest.update(operation.generation().get().to_be_bytes());
    digest.update(entity_id.get().to_be_bytes());
    digest.update(document.partition().canonical_bytes());
    let bytes: [u8; 32] = digest.finalize().into();
    let seed = u64::from_be_bytes(
        bytes[..core::mem::size_of::<u64>()]
            .try_into()
            .expect("SHA-256 prefix is eight bytes"),
    );
    let mut rng = StdRng::seed_from_u64(seed);
    vector::select_layer(definition.ml(), &mut rng)
}

#[allow(
    clippy::too_many_arguments,
    reason = "descriptor validation cross-checks the independent database, owner, record, policy, and cache identities"
)]
async fn validate_descriptor<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<IndexOperationStepResult> {
    if generation_has_rows(
        transaction,
        scope,
        RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    )
    .await?
    {
        return Ok(progressed_build(VectorBuildStage::CatchUp(
            PrefixScanProgress {
                cursor: None,
                counters: progress.counters,
            },
        )));
    }
    let cursor_kind = progress
        .cursor
        .as_ref()
        .map(|cursor| IndexKey::parse_from_slice(scope, cursor.as_bytes()))
        .transpose()?
        .and_then(|key| match key {
            IndexKey::Data { kind: key, .. } => Some(key.record_kind()),
            IndexKey::Global { .. } => None,
        });
    if !matches!(cursor_kind, Some(RecordKind::VectorPartitionMapping)) {
        let prefix = generation_prefix(
            scope,
            RecordKind::AppliedState,
            operation.index_id(),
            operation.generation(),
        );
        let start = cursor_suffix(&prefix, progress.cursor.as_ref())?
            .map_or(Bound::Unbounded, Bound::Excluded);
        let mut rows = transaction
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        let mut accounting = VectorBatchAccounting::new(progress.counters, limits);
        let mut cursor = progress.cursor.clone();
        let mut exhausted = true;
        while accounting.can_read_another() {
            let Some(row) = rows.next().await? else {
                break;
            };
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            let (entity, applied) = decode_applied(scope, &row.key, &row.value)?;
            let AppliedFamilyState::Vector(Some(partition)) = applied.state else {
                return Err(corruption(
                    "vector validation found non-vector or empty applied state",
                ));
            };
            if applied.index_id != operation.index_id()
                || applied.generation != operation.generation()
                || entity.kind != definition.element_kind()
            {
                return Err(corruption("vector applied-state ownership mismatch"));
            }
            validate_partition_metadata::<D>(
                transaction,
                scope,
                operation,
                record,
                definition,
                &partition,
                Arc::clone(&simhasher_registry),
            )
            .await?;
            let output_bytes = row.key.len() as u64;
            if !accounting.can_admit_input(input_bytes)
                || !accounting.can_admit_output(VectorWriteMeasurement::zero(), 1, output_bytes)
            {
                if accounting.is_empty() {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                            observed: input_bytes.max(output_bytes),
                            limit: limits
                                .max_input_bytes()
                                .get()
                                .min(limits.max_output_bytes().get()),
                        },
                    ));
                }
                exhausted = false;
                break;
            }
            transaction.delete(&row.key)?;
            accounting.admit(
                input_bytes,
                VectorWriteMeasurement::zero(),
                0,
                1,
                output_bytes,
            )?;
            cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
        }
        if !accounting.can_read_another() {
            exhausted = false;
        }
        let counters = accounting.finish()?;
        if !exhausted {
            return Ok(progressed_build(VectorBuildStage::ValidateDescriptor(
                PrefixScanProgress { cursor, counters },
            )));
        }
        return validate_mappings_or_finish::<D>(
            db,
            transaction,
            scope,
            operation,
            record,
            definition,
            None,
            counters,
            limits,
            simhasher_registry,
        )
        .await;
    }
    validate_mappings_or_finish::<D>(
        db,
        transaction,
        scope,
        operation,
        record,
        definition,
        progress.cursor.as_ref(),
        progress.counters,
        limits,
        simhasher_registry,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "descriptor validation binds exact canonical and physical identities"
)]
async fn validate_mappings_or_finish<D: Distance>(
    db: &Db,
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    cursor: Option<&IndexCursor>,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<IndexOperationStepResult> {
    let IndexStateVectorPhysical { layout } = IndexStateVectorPhysical::from_record(record)?;
    if let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout {
        let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
            scope,
            record,
            operation.operation_id(),
            physical_index_id,
        )
        .map_err(|error| corruption(error.to_string()))?;
        let index = VectorIndex::<D>::from_generation(handle.generation())
            .with_simhasher_registry(simhasher_registry);
        let expected =
            VectorIndexConfig::from_v2_definition(definition, handle.generation().physical_name());
        match index.get_metadata(transaction).await? {
            Some(metadata) => validate_metadata_config(&metadata.config, &expected)?,
            None => {
                let planning = db.begin(IsolationLevel::Snapshot).await?;
                let planning_write = MeasuredVectorTransaction::new(&planning);
                let checkpoint = planning_write.checkpoint();
                index.stage_create(&planning_write, expected).await?;
                let plan: PlannedVectorMutation = planning_write
                    .plan_since(checkpoint)
                    .map_err(measurement_error)?;
                let writes = plan.measurement();
                if writes.operations() > limits.max_output_operations().get()
                    || writes.encoded_bytes() > limits.max_output_bytes().get()
                {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: definition.element_kind(),
                            entity_id: IndexEntityId::initial(),
                            observed: writes.encoded_bytes(),
                            limit: limits.max_output_bytes().get(),
                        },
                    ));
                }
                plan.apply_to(transaction)?;
                let counters = OperationCounters {
                    entities: counters.entities,
                    input_bytes: counters.input_bytes,
                    output_operations: checked_add(
                        counters.output_operations,
                        writes.operations(),
                        "cumulative output operations",
                    )?,
                    output_bytes: checked_add(
                        counters.output_bytes,
                        writes.encoded_bytes(),
                        "cumulative output bytes",
                    )?,
                };
                return Ok(progressed_build(VectorBuildStage::Activate(
                    NoCursorProgress { counters },
                )));
            }
        }
        return Ok(progressed_build(VectorBuildStage::Activate(
            NoCursorProgress { counters },
        )));
    }
    let prefix = generation_prefix(
        scope,
        RecordKind::VectorPartitionMapping,
        operation.index_id(),
        operation.generation(),
    );
    let start = cursor_suffix(&prefix, cursor)?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut accounting = VectorBatchAccounting::new(counters, limits);
    let mut next_cursor = cursor.cloned();
    let mut exhausted = true;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if !accounting.can_admit_input(input_bytes) {
            if accounting.is_empty() {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id: IndexEntityId::initial(),
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    },
                ));
            }
            exhausted = false;
            break;
        }
        let mapping = decode_mapping(scope, &row.key, &row.value, operation)?;
        validate_partition_metadata::<D>(
            transaction,
            scope,
            operation,
            record,
            definition,
            mapping.partition.as_partition(),
            Arc::clone(&simhasher_registry),
        )
        .await?;
        accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0, 0)?;
        next_cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let counters = accounting.finish()?;
    Ok(progressed_build(if exhausted {
        VectorBuildStage::Activate(NoCursorProgress { counters })
    } else {
        VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
            cursor: next_cursor,
            counters,
        })
    }))
}

#[allow(
    clippy::too_many_arguments,
    reason = "metadata validation binds every canonical ownership component"
)]
async fn validate_partition_metadata<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    record: &IndexRecordV2,
    definition: &ValidatedVectorIndexDefinition,
    partition: &TextPartition,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<()> {
    let resolution =
        resolve_build_physical(transaction, scope, operation, record, partition, false).await?;
    let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
        scope,
        record,
        operation.operation_id(),
        resolution.physical_index_id,
    )
    .map_err(|error| corruption(error.to_string()))?;
    let index = VectorIndex::<D>::from_generation(handle.generation())
        .with_simhasher_registry(simhasher_registry);
    let Some(metadata) = index.get_metadata(transaction).await? else {
        return Err(corruption("vector partition has no physical metadata"));
    };
    let expected =
        VectorIndexConfig::from_v2_definition(definition, handle.generation().physical_name());
    validate_metadata_config(&metadata.config, &expected)
}

fn validate_metadata_config(
    actual: &VectorIndexConfig,
    expected: &VectorIndexConfig,
) -> Result<()> {
    if !actual.has_same_physical_contract(expected) {
        return Err(corruption(
            "physical vector metadata disagrees with canonical descriptor",
        ));
    }
    Ok(())
}

/// Closed applied-state write selected by one authoritative entity transition.
#[derive(Debug, Clone, Copy)]
enum AppliedStateTransition<'a> {
    Absent,
    Delete,
    Put(&'a TextPartition),
}

fn lifecycle_write_measurement(
    scope: DataScope,
    operation: &IndexOperationRecord,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    applied_transition: AppliedStateTransition<'_>,
    next_resolution: Option<&BuildPhysicalResolution>,
    delete_delta: bool,
) -> Result<(u64, u64)> {
    let applied_key = applied_key(
        scope,
        operation.index_id(),
        operation.generation(),
        entity_kind,
        entity_id,
    );
    let (mut operations, mut bytes) = match applied_transition {
        AppliedStateTransition::Put(partition) => {
            let value = encode_applied_state(&AppliedEntityStateValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity_kind,
                entity_id,
                state: AppliedFamilyState::Vector(Some(partition.clone())),
            });
            (1_u64, applied_key.len().saturating_add(value.len()) as u64)
        }
        AppliedStateTransition::Delete => (1, applied_key.len() as u64),
        AppliedStateTransition::Absent => (0, 0),
    };
    if delete_delta {
        let delta_key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity: IndexEntity {
                    kind: entity_kind,
                    id: entity_id,
                },
            }),
        );
        operations = operations.saturating_add(1);
        bytes = bytes.saturating_add(delta_key.len() as u64);
    }
    if let Some(resolution) = next_resolution
        && resolution.mapping_is_new
    {
        let AppliedStateTransition::Put(partition) = applied_transition else {
            return Err(corruption("new vector mapping has no partition"));
        };
        let tenant = VectorTenantPartition::try_from_partition(partition.clone())
            .map_err(|error| corruption(error.to_string()))?;
        let mapping_key = scoped_index_key(
            scope,
            ScopedKey::VectorPartitionMapping(
                crate::encoding::v2::keys::VectorPartitionMappingKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: tenant.fingerprint(),
                },
            ),
        );
        let mapping_value =
            encode_partition_mapping(&crate::index_lifecycle::work::VectorPartitionMappingValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: tenant,
                physical_index_id: resolution.physical_index_id,
            });
        let watermark_key = IndexKey::Global {
            kind: GlobalKey::VectorPhysicalIdWatermark,
        }
        .to_bytes();
        let watermark_value = encode_metadata_value(
            &IndexV2MetadataValue::VectorPhysicalIdWatermark(VectorPhysicalIdWatermark {
                next_id: resolution.physical_index_id.checked_next()?,
            }),
        );
        operations = operations.saturating_add(2);
        bytes = bytes
            .saturating_add(mapping_key.len().saturating_add(mapping_value.len()) as u64)
            .saturating_add(watermark_key.len().saturating_add(watermark_value.len()) as u64);
    }
    Ok((operations, bytes))
}

fn stage_applied(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    next_partition: Option<TextPartition>,
) -> Result<()> {
    let key = applied_key(
        scope,
        operation.index_id(),
        operation.generation(),
        entity_kind,
        entity_id,
    );
    match next_partition {
        Some(partition) => transaction.put(
            key,
            encode_applied_state(&AppliedEntityStateValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity_kind,
                entity_id,
                state: AppliedFamilyState::Vector(Some(partition)),
            }),
        )?,
        None => transaction.delete(key)?,
    }
    Ok(())
}

async fn load_applied(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
) -> Result<Option<TextPartition>> {
    let key = applied_key(scope, index_id, generation, entity_kind, entity_id);
    let Some(value) = transaction.get(&key).await? else {
        return Ok(None);
    };
    let (_, applied) = decode_applied(scope, &key, &value)?;
    if applied.index_id != index_id
        || applied.generation != generation
        || applied.entity_kind != entity_kind
        || applied.entity_id != entity_id
    {
        return Err(corruption("vector applied-state key/value mismatch"));
    }
    let AppliedFamilyState::Vector(partition) = applied.state else {
        return Err(corruption(
            "vector generation contains another applied family",
        ));
    };
    Ok(partition)
}

fn applied_key(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
) -> Bytes {
    scoped_index_key(
        scope,
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id,
            generation,
            entity: IndexEntity {
                kind: entity_kind,
                id: entity_id,
            },
        }),
    )
}

fn decode_delta(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
) -> Result<(IndexEntity, CoalescedBuildDeltaValue)> {
    let IndexKey::Data {
        kind: ScopedKey::BuildDelta(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("build-delta prefix yielded another key kind"));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        decode_build_delta(value),
        "build-delta key contains another value kind",
    )?;
    if key.index_id != value.index_id
        || key.generation != value.generation
        || key.entity.kind != value.entity_kind
        || key.entity.id != value.entity_id
    {
        return Err(corruption("build-delta key/value mismatch"));
    }
    Ok((key.entity, value))
}

fn decode_applied(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
) -> Result<(IndexEntity, AppliedEntityStateValue)> {
    let IndexKey::Data {
        kind: ScopedKey::AppliedState(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("applied-state prefix yielded another key kind"));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        decode_applied_state(value),
        "applied-state key contains another value kind",
    )?;
    if key.index_id != value.index_id
        || key.generation != value.generation
        || key.entity.kind != value.entity_kind
        || key.entity.id != value.entity_id
    {
        return Err(corruption("applied-state key/value mismatch"));
    }
    Ok((key.entity, value))
}

fn decode_mapping(
    scope: DataScope,
    key: &[u8],
    value: &[u8],
    operation: &IndexOperationRecord,
) -> Result<crate::index_lifecycle::work::VectorPartitionMappingValue> {
    let IndexKey::Data {
        kind: ScopedKey::VectorPartitionMapping(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("vector mapping prefix yielded another key kind"));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        decode_partition_mapping(value),
        "vector partition mapping key contains another value kind",
    )?;
    if key.index_id != operation.index_id()
        || key.generation != operation.generation()
        || value.index_id != operation.index_id()
        || value.generation != operation.generation()
        || key.partition != value.partition.fingerprint()
    {
        return Err(corruption("vector mapping key/value ownership mismatch"));
    }
    Ok(value)
}

async fn read_authoritative_properties(
    transaction: &DbTransaction,
    scope: DataScope,
    entity: IndexEntity,
) -> Result<Option<Vec<Property>>> {
    let key = match entity.kind {
        IndexElementKind::Node => DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(crate::encoding::v2::keys::NodePropertyKey::new(
                entity.id.get(),
            )),
        }
        .to_bytes(),
        IndexElementKind::Edge => DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(
                crate::encoding::v2::keys::EdgePropertyByIdKey::new(entity.id.get()),
            ),
        }
        .to_bytes(),
    };
    transaction
        .get(key)
        .await?
        .map(|bytes| decode_properties(&bytes).map_err(HelixDbError::from))
        .transpose()
}

async fn load_operation_index(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<IndexRecordV2> {
    let key = scoped_index_key(scope, ScopedKey::index_record(operation.identity().clone()));
    let Some(value) = transaction.get(key).await? else {
        return Err(corruption("vector operation has no canonical index"));
    };
    let record = decode_index_record(&value)?;
    if record.index_id() != operation.index_id()
        || record.identity() != operation.identity()
        || record.revision() != operation.index_record_revision()
        || record.state().generation() != operation.generation()
    {
        return Err(corruption("vector operation/canonical record mismatch"));
    }
    Ok(record)
}

async fn generation_has_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    kind: RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Result<bool> {
    let prefix = generation_prefix(scope, kind, index_id, generation);
    let mut rows = transaction.scan_prefix(prefix, ..).await?;
    Ok(rows.next().await?.is_some())
}

fn source_prefix(scope: DataScope, kind: IndexElementKind) -> Bytes {
    let prefix = match kind {
        IndexElementKind::Node => KeyPrefix::NodeProperty,
        IndexElementKind::Edge => KeyPrefix::EdgePropertyById,
    };
    DataKey::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

fn source_entity(
    scope: DataScope,
    expected: IndexElementKind,
    key: &[u8],
) -> Result<Option<IndexEntityId>> {
    let parsed = DataKey::parse_from_slice(scope, key)?;
    Ok(match (expected, parsed) {
        (
            IndexElementKind::Node,
            DataKey::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.node_id())),
        (
            IndexElementKind::Edge,
            DataKey::Data {
                kind: DataKeyKind::EdgePropertyById(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.edge_id())),
        (IndexElementKind::Edge, DataKey::Data { .. }) => None,
        (IndexElementKind::Node, DataKey::Data { .. }) | (_, DataKey::Global { .. }) => {
            return Err(corruption("vector source prefix yielded another key kind"));
        }
    })
}

fn generation_prefix(
    scope: DataScope,
    kind: RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Bytes {
    IndexKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(kind, index_id, generation),
    )
}

fn cursor_suffix(prefix: &Bytes, cursor: Option<&IndexCursor>) -> Result<Option<Bytes>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let Some(suffix) = cursor.as_bytes().strip_prefix(prefix.as_ref()) else {
        return Err(corruption("vector cursor is outside its exact scan prefix"));
    };
    Ok(Some(Bytes::copy_from_slice(suffix)))
}

fn scoped_index_key(scope: DataScope, key: ScopedKey) -> Bytes {
    IndexKey::Data { scope, kind: key }.to_bytes()
}

fn progressed_build(stage: VectorBuildStage) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
        VectorBuildProgress::Constructing(stage),
    ))
}

fn invalid_source(
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
) -> IndexOperationStepResult {
    IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
        entity_kind,
        entity_id,
    })
}

fn finish_or_block_scan(
    outcome: EntityPlanOutcome,
    accounting: VectorBatchAccounting,
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    progress: &SourceScanProgress,
    cursor: Option<IndexCursor>,
    session_stats: VectorBuildSessionStats,
) -> Result<VectorStepResult> {
    let vector_planning = accounting.planning_usage(session_stats);
    match outcome {
        EntityPlanOutcome::Blocked(blocker) => Ok(VectorStepResult::ordinary(
            IndexOperationStepResult::Blocked(blocker),
        )
        .with_vector_planning(vector_planning)),
        EntityPlanOutcome::BatchFull => {
            let (counters, single_vector_output_bytes) = accounting.finish_with_max()?;
            Ok(VectorStepResult {
                result: progressed_build(VectorBuildStage::Scan(SourceScanProgress {
                    inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                    cursor,
                    counters,
                })),
                single_vector_output_bytes,
                physical_operations: 0,
                output_bytes: 0,
                vector_planning,
            })
        }
        EntityPlanOutcome::Admitted { .. } => Err(corruption(format!(
            "admitted vector entity {entity_kind:?}/{} escaped application",
            entity_id.get()
        ))),
    }
}

enum EntityPlanOutcome {
    Admitted {
        vector_writes: VectorWriteMeasurement,
        single_vector_output_bytes: u64,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
        next_partition: Option<TextPartition>,
    },
    BatchFull,
    Blocked(IndexOperationBlocker),
}

struct VectorBatchAccounting {
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
    entities: usize,
    input_bytes: u64,
    vector_writes: VectorWriteMeasurement,
    max_single_vector_output_bytes: u64,
    lifecycle_operations: u64,
    lifecycle_bytes: u64,
    planning_executions: u64,
}

impl VectorBatchAccounting {
    fn new(counters: OperationCounters, limits: SearchIndexBatchLimits) -> Self {
        Self {
            counters,
            limits,
            entities: 0,
            input_bytes: 0,
            vector_writes: VectorWriteMeasurement::zero(),
            max_single_vector_output_bytes: 0,
            lifecycle_operations: 0,
            lifecycle_bytes: 0,
            planning_executions: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.entities == 0
    }

    fn can_read_another(&self) -> bool {
        self.entities < self.limits.max_entities().get()
    }

    fn can_admit_input(&self, bytes: u64) -> bool {
        self.input_bytes.saturating_add(bytes) <= self.limits.max_input_bytes().get()
    }

    fn can_admit_output(
        &self,
        cumulative_vector: VectorWriteMeasurement,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
    ) -> bool {
        cumulative_vector
            .operations()
            .saturating_add(self.lifecycle_operations)
            .saturating_add(lifecycle_operations)
            <= self.limits.max_output_operations().get()
            && cumulative_vector
                .encoded_bytes()
                .saturating_add(self.lifecycle_bytes)
                .saturating_add(lifecycle_bytes)
                <= self.limits.max_output_bytes().get()
    }

    fn record_planning(&mut self) {
        self.planning_executions = self.planning_executions.saturating_add(1);
    }

    fn planning_usage(&self, stats: VectorBuildSessionStats) -> VectorPlanningUsage {
        VectorPlanningUsage {
            planning_executions: self.planning_executions,
            planned_writes: self.vector_writes.operations(),
            replay_executions: 0,
            item_hits: stats.item_hits(),
            item_misses: stats.item_misses(),
            neighbor_hits: stats.neighbor_hits(),
            neighbor_misses: stats.neighbor_misses(),
            simhash_hits: stats.simhash_hits(),
            simhash_misses: stats.simhash_misses(),
            item_evictions: stats.item_evictions(),
            neighbor_evictions: stats.neighbor_evictions(),
            simhash_evictions: stats.simhash_evictions(),
            dirty_neighbor_flushes: stats.dirty_neighbor_flushes(),
            retained_payload_bytes: stats.max_retained_payload_bytes(),
        }
    }

    fn admit(
        &mut self,
        input_bytes: u64,
        cumulative_vector: VectorWriteMeasurement,
        single_vector_output_bytes: u64,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
    ) -> Result<()> {
        self.entities += 1;
        self.input_bytes = checked_add(self.input_bytes, input_bytes, "batch input bytes")?;
        self.vector_writes = cumulative_vector;
        self.max_single_vector_output_bytes = self
            .max_single_vector_output_bytes
            .max(single_vector_output_bytes);
        self.lifecycle_operations = checked_add(
            self.lifecycle_operations,
            lifecycle_operations,
            "batch lifecycle operations",
        )?;
        self.lifecycle_bytes = checked_add(
            self.lifecycle_bytes,
            lifecycle_bytes,
            "batch lifecycle bytes",
        )?;
        Ok(())
    }

    fn finish(self) -> Result<OperationCounters> {
        Ok(OperationCounters {
            entities: checked_add(
                self.counters.entities,
                self.entities as u64,
                "cumulative entities",
            )?,
            input_bytes: checked_add(
                self.counters.input_bytes,
                self.input_bytes,
                "cumulative input bytes",
            )?,
            output_operations: checked_add(
                self.counters.output_operations,
                self.vector_writes
                    .operations()
                    .saturating_add(self.lifecycle_operations),
                "cumulative output operations",
            )?,
            output_bytes: checked_add(
                self.counters.output_bytes,
                self.vector_writes
                    .encoded_bytes()
                    .saturating_add(self.lifecycle_bytes),
                "cumulative output bytes",
            )?,
        })
    }

    fn finish_with_max(self) -> Result<(OperationCounters, u64)> {
        let max_single_vector_output_bytes = self.max_single_vector_output_bytes;
        Ok((self.finish()?, max_single_vector_output_bytes))
    }
}

fn checked_add(left: u64, right: u64, name: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| corruption(format!("vector {name} overflowed")))
}

fn measurement_error(error: impl std::fmt::Display) -> HelixDbError {
    corruption(format!("vector write measurement failed: {error}"))
}

fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

fn operation_error(error: crate::index_lifecycle::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, VectorIndexDefinition};
    use crate::encoding::property::property_value::PropertyValue;
    use crate::encoding::v2::keys::indexes::vector::VectorKey;
    use crate::encoding::v2::keys::NodePropertyKey;
    use crate::encoding::v2::values::property::encode_properties;
    use crate::index_lifecycle::lifecycle::{
        create_index_operation, create_legacy_vector_adoption_operation, drop_index_operation,
        InitialBuildProgress,
    };
    use crate::index_lifecycle::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, ClaimPermission,
        CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_lifecycle::repository::peek_vector_physical_id;
    use crate::index_lifecycle::vector::{
        load_mutation_set, maintain_entity, VectorEntityMutation,
    };
    use crate::index_lifecycle::{
        ActiveIndexHandle, ClaimSequence, IndexDdlReceipt, IndexOperationId, IndexScopeGates,
        IndexStateV2, WriterEpoch,
    };
    use crate::migrations::startup::bootstrap_writer;
    use crate::search::vector::{
        DistanceScore, SearchParams, SimHashMode, SimHasherRegistry,
        ValidatedVectorGenerationHandle, VectorCacheRegistry, VectorCacheWriteSet,
    };

    const NOW_MILLIS: u64 = 1;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("vector driver test database opens");
        bootstrap_writer(&db)
            .await
            .expect("vector driver test database bootstraps V2 metadata");
        db
    }

    fn driver() -> VectorIndexDriver {
        VectorIndexDriver::new(
            Arc::new(IndexScopeGates::default()),
            Arc::new(VectorCacheRegistry::default()),
            Arc::new(SimHasherRegistry::default()),
        )
    }

    #[test]
    fn batch_admission_uses_cumulative_last_write_wins_vector_measurement() {
        let limits = SearchIndexBackfillLimits::default().batch();
        let mut accounting = VectorBatchAccounting::new(OperationCounters::default(), limits);
        accounting
            .admit(10, VectorWriteMeasurement::for_test(2, 20), 20, 1, 5)
            .unwrap();
        accounting
            .admit(10, VectorWriteMeasurement::for_test(2, 14), 8, 1, 5)
            .unwrap();

        let counters = accounting.finish().unwrap();
        assert_eq!(counters.entities, 2);
        assert_eq!(counters.output_operations, 4);
        assert_eq!(counters.output_bytes, 24);
    }

    /// Exercises diagnostic and typed-error adapters that sit below the
    /// lifecycle state machine but still belong to the production surface.
    #[tokio::test]
    async fn diagnostic_and_error_adapters_preserve_their_error_categories() {
        assert!(format!("{:?}", driver()).contains("VectorIndexDriver"));
        assert!(matches!(
            invalid_source(IndexElementKind::Node, IndexEntityId::initial()),
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_kind: IndexElementKind::Node,
                entity_id,
            }) if entity_id == IndexEntityId::initial()
        ));
        assert!(matches!(
            checked_add(u64::MAX, 1, "fixture"),
            Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("fixture")
        ));
        let db = test_db("vector-driver-error-adapters").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let first = MeasuredVectorTransaction::new(&transaction);
        let foreign_checkpoint = first.checkpoint();
        let second = MeasuredVectorTransaction::new(&transaction);
        let measurement_failure = second
            .plan_since(foreign_checkpoint)
            .expect_err("checkpoint belongs to another recorder");
        assert!(matches!(
            measurement_error(measurement_failure),
            HelixDbError::IndexCatalogCorruption(reason) if reason.contains("measurement")
        ));
        assert!(matches!(
            corruption("fixture corruption"),
            HelixDbError::IndexCatalogCorruption(reason) if reason == "fixture corruption"
        ));
        assert!(matches!(
            operation_error(crate::index_lifecycle::IndexOperationModelError::OversizedCursor {
                actual: 2,
                maximum: 1,
            }),
            HelixDbError::InvariantViolation(reason) if reason.contains("cursor")
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    fn definition(tenant_property: Option<&str>) -> ValidatedDynamicIndexDefinition {
        let runtime = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Euclidean,
        )
        .expect("vector definition validates");
        let runtime = match tenant_property {
            Some(tenant_property) => runtime
                .with_tenant_property(tenant_property)
                .expect("tenant property validates"),
            None => runtime,
        };
        ValidatedDynamicIndexDefinition::Vector(
            ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
                .expect("V2 vector definition validates"),
        )
    }

    fn properties(vector: [f32; 3], tenant: Option<i64>) -> Vec<Property> {
        let mut properties = vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new("embedding", PropertyValue::F32Array(vector.to_vec())),
        ];
        if let Some(tenant) = tenant {
            properties.push(Property::new("account_id", PropertyValue::I64(tenant)));
        }
        properties
    }

    fn source_key(scope: DataScope, entity_id: u64) -> Bytes {
        DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
        }
        .to_bytes()
    }

    fn source_cursor(scope: DataScope, entity_id: u64) -> IndexCursor {
        IndexCursor::try_new(source_key(scope, entity_id)).expect("source key is a valid cursor")
    }

    async fn put_source(db: &Db, scope: DataScope, entity_id: u64, properties: &[Property]) {
        db.put(source_key(scope, entity_id), encode_properties(properties))
            .await
            .expect("vector source is written");
    }

    async fn create_build(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
        upper_entity_id: u64,
    ) -> (IndexOperationId, IndexId, IndexGenerationId) {
        let receipt = create_index_operation(
            db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
            InitialBuildProgress::vector(source_cursor(scope, upper_entity_id)),
        )
        .await
        .expect("vector build is enqueued");
        let IndexDdlReceipt::Accepted {
            operation_id,
            index_id,
            generation,
        } = receipt
        else {
            panic!("new vector definition must enqueue a build");
        };
        (operation_id, index_id, generation)
    }

    async fn drive_one(
        db: &Db,
        driver: &VectorIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
    ) -> CommittedOperationStep {
        let writer_epoch = WriterEpoch::from_bytes([0x6B; 16]).expect("writer epoch is non-nil");
        let observation = observe_operation_pointer(db, operation_id, writer_epoch, NOW_MILLIS)
            .await
            .expect("vector operation pointer is readable");
        let OperationPointerObservation::Eligible(eligible) = observation else {
            panic!("queued vector operation must be eligible: {observation:?}");
        };
        let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
        *claim_sequence = claim_sequence
            .checked_add(1)
            .expect("claim sequence remains bounded");
        let claimed = claim_operation(
            db,
            &eligible,
            writer_epoch,
            sequence,
            NOW_MILLIS,
            ClaimPermission::Normal,
        )
        .await
        .expect("vector claim succeeds")
        .expect("vector revision is claimable");
        execute_claimed_step(db, &claimed, driver, limits, NOW_MILLIS)
            .await
            .expect("vector step commits")
    }

    async fn drive_to_terminal(
        db: &Db,
        driver: &VectorIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
    ) -> CommittedOperationStep {
        for _ in 0..64 {
            let step = drive_one(
                db,
                driver,
                operation_id,
                claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await;
            if step != CommittedOperationStep::Progressed {
                return step;
            }
        }
        panic!("vector operation exceeded bounded test checkpoints")
    }

    async fn read_index(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
    ) -> IndexRecordV2 {
        let key = scoped_index_key(scope, ScopedKey::index_record(definition.identity()));
        let value = db
            .get(key)
            .await
            .expect("canonical vector index is readable")
            .expect("canonical vector index exists");
        decode_index_record(&value).expect("canonical vector index decodes")
    }

    async fn read_operation(
        db: &Db,
        scope: DataScope,
        operation_id: IndexOperationId,
    ) -> IndexOperationRecord {
        let value = db
            .get(crate::index_lifecycle::outbox::scoped_operation_key(
                scope,
                operation_id,
            ))
            .await
            .expect("vector operation is readable")
            .expect("vector operation exists");
        crate::encoding::v2::values::decode_operation_record(&value)
            .expect("vector operation decodes")
    }

    async fn mapping_values(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Vec<crate::index_lifecycle::work::VectorPartitionMappingValue> {
        let prefix = generation_prefix(
            scope,
            RecordKind::VectorPartitionMapping,
            index_id,
            generation,
        );
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("vector mappings are readable");
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.expect("vector mapping row is readable") {
            let value = decode_partition_mapping(&row.value).expect("vector mapping value decodes");
            values.push(value);
        }
        values
    }

    async fn physical_vector_rows(
        db: &Db,
        scope: DataScope,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Vec<(Bytes, Bytes)> {
        let mut result = Vec::new();
        for lane in VectorStorageLane::ALL {
            let prefix = DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(lane.prefix_key(physical_index_id.get())),
            }
            .to_bytes();
            let mut rows = db
                .scan_prefix(prefix, ..)
                .await
                .expect("physical vector lane scans");
            while let Some(row) = rows.next().await.expect("physical vector row reads") {
                result.push((row.key, row.value));
            }
        }
        result.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        result
    }

    async fn mutate_building_source(
        db: &Db,
        scope: DataScope,
        entity_id: u64,
        before: &[Property],
        after: &[Property],
    ) {
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("vector mutation transaction opens");
        let mutations = load_mutation_set(&transaction, scope)
            .await
            .expect("building vector generations load");
        let cache_writes = VectorCacheWriteSet::default();
        maintain_entity(
            &transaction,
            scope,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, entity_id, before, after),
        )
        .await
        .expect("building mutation records its coalesced delta");
        transaction
            .put(source_key(scope, entity_id), encode_properties(after))
            .expect("authoritative vector source update stages");
        transaction
            .commit()
            .await
            .expect("authoritative source and delta commit together");
    }

    #[tokio::test]
    async fn unpartitioned_build_restarts_activates_and_drop_removes_physical_rows() {
        let db = test_db("vector-driver-unpartitioned-build-drop").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        put_source(&db, scope, 1, &properties([3.0, 2.0, 1.0], None)).await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let one_entity = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            NonZeroU64::new(1024).expect("operation limit is positive"),
            NonZeroU64::new(16 * 1024 * 1024).expect("output limit is positive"),
            NonZeroU64::new(16 * 1024 * 1024).expect("entity output is positive"),
        )
        .expect("restart limits validate");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver(), build_id, &mut claim_sequence, one_entity,).await,
            CommittedOperationStep::Progressed
        );
        let restarted = driver();
        assert_eq!(
            drive_to_terminal(&db, &restarted, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = active.state()
        else {
            panic!("completed vector build is active and unpartitioned");
        };
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("active vector record projects a handle");
        let generation = ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active_handle, *physical_index_id)
        .expect("active physical generation validates");
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        assert!(index.get_item(&db, 0).await.unwrap().is_some());
        assert!(index.get_item(&db, 1).await.unwrap().is_some());

        let one_cleanup_operation = SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(1024).expect("source-entity limit is positive"),
            NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            NonZeroU64::MIN,
            NonZeroU64::new(16 * 1024 * 1024).expect("output limit is positive"),
            NonZeroU64::new(16 * 1024 * 1024).expect("entity output is positive"),
        )
        .expect("single-operation cleanup limits validate");
        let limited_cleanup_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("limited vector cleanup transaction opens");
        let PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty,
            mapping_deleted,
        } = delete_physical_namespace::<vector::distance::Euclidean>(
            &limited_cleanup_transaction,
            &generation,
            None,
            IndexElementKind::Node,
            OperationCounters::default(),
            one_cleanup_operation,
        )
        .await
        .expect("limited physical cleanup batch plans")
        else {
            panic!("one physical delete fits the cleanup transaction budgets");
        };
        assert_eq!(counters.entities, 1);
        assert_eq!(counters.output_operations, 1);
        assert!(!namespace_empty);
        assert!(!mapping_deleted);
        drop(limited_cleanup_transaction);

        let cleanup_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("vector cleanup transaction opens");
        let PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty,
            mapping_deleted,
        } = delete_physical_namespace::<vector::distance::Euclidean>(
            &cleanup_transaction,
            &generation,
            None,
            IndexElementKind::Node,
            OperationCounters::default(),
            one_entity,
        )
        .await
        .expect("physical cleanup batch plans")
        else {
            panic!("complete physical namespace fits the cleanup transaction budgets");
        };
        assert!(namespace_empty);
        assert!(!mapping_deleted);
        assert!(
            counters.entities
                > u64::try_from(one_entity.max_entities().get())
                    .expect("source-entity limit fits u64"),
            "physical cleanup rows must not consume the decoded source-entity limit"
        );
        assert_eq!(counters.output_operations, counters.entities);
        drop(cleanup_transaction);

        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("active vector drop is enqueued")
        else {
            panic!("active vector drop creates a new operation");
        };
        assert_eq!(
            drive_one(
                &db,
                &restarted,
                drop_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one(
                &db,
                &restarted,
                drop_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let cleanup_restart = driver();
        assert_eq!(
            drive_to_terminal(&db, &cleanup_restart, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert!(index.get_metadata(&db).await.unwrap().is_none());
        assert!(index
            .cleanup_scan(&db)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none());
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn partitioned_build_catches_up_tenant_move_into_exact_physical_mapping() {
        let db = test_db("vector-driver-partition-catch-up").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(Some("account_id"));
        let before = properties([1.0, 2.0, 3.0], Some(10));
        let after = properties([4.0, 5.0, 6.0], Some(20));
        put_source(&db, scope, 0, &before).await;
        let (build_id, index_id, generation_id) = create_build(&db, scope, &definition, 0).await;
        let mut claim_sequence = 1;
        let driver = driver();
        assert_eq!(
            drive_one(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        mutate_building_source(&db, scope, 0, &before, &after).await;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("active partitioned vector projects a handle");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("test definition is vector");
        };
        let before_document = vector_document(vector_definition, &before)
            .unwrap()
            .expect("before document is indexed");
        let after_document = vector_document(vector_definition, &after)
            .unwrap()
            .expect("after document is indexed");
        let mappings = mapping_values(&db, scope, index_id, generation_id).await;
        assert_eq!(mappings.len(), 2);
        for (document, should_exist) in [(&before_document, false), (&after_document, true)] {
            let mapping = mappings
                .iter()
                .find(|mapping| mapping.partition.as_partition() == document.partition())
                .expect("each observed tenant has one mapping");
            let generation = ValidatedVectorGenerationHandle::try_from_active::<
                vector::distance::Euclidean,
            >(&active_handle, mapping.physical_index_id)
            .expect("mapped active generation validates");
            let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
            assert_eq!(
                index.get_item(&db, 0).await.unwrap().is_some(),
                should_exist
            );
        }
        db.close().await.expect("vector test database closes");
    }

    /// Proves partition mappings remain the cleanup cursor until their entire
    /// physical namespace is gone, including one-row batch restarts.
    #[tokio::test]
    async fn partitioned_drop_resumes_each_mapping_and_removes_every_namespace() {
        let db = test_db("vector-driver-partitioned-drop").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(Some("account_id"));
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
        put_source(&db, scope, 1, &properties([4.0, 5.0, 6.0], Some(20))).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("partitioned Active record projects a handle");
        let mappings = mapping_values(&db, scope, index_id, generation).await;
        assert_eq!(mappings.len(), 2);
        let indexes = mappings
            .iter()
            .map(|mapping| {
                let generation = ValidatedVectorGenerationHandle::try_from_active::<
                    vector::distance::Euclidean,
                >(&active_handle, mapping.physical_index_id)
                .expect("partition mapping validates against the Active handle");
                VectorIndex::<vector::distance::Euclidean>::from_generation(&generation)
            })
            .collect::<Vec<_>>();

        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("partitioned drop enqueues")
        else {
            panic!("partitioned Active drop creates a cleanup operation");
        };
        let one_entity = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        for _ in 0..64 {
            let step = drive_one(&db, &driver, drop_id, &mut claim_sequence, one_entity).await;
            if step == CommittedOperationStep::Completed {
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
        }
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert!(mapping_values(&db, scope, index_id, generation)
            .await
            .is_empty());
        for index in indexes {
            assert!(index
                .cleanup_scan(&db)
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .is_none());
        }
        db.close().await.expect("vector test database closes");
    }

    /// Exercises every cleanup checkpoint rejection before the outbox is
    /// allowed to commit a new durable progress value.
    #[tokio::test]
    async fn cleanup_checkpoint_rejections_and_limit_blockers_are_typed() {
        let db = test_db("vector-driver-cleanup-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("drop operation enqueues")
        else {
            panic!("Active vector drop creates a cleanup operation");
        };
        let record = read_index(&db, scope, &definition).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, drop_id)
            .await
            .unwrap()
            .expect("drop operation exists");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let limits = SearchIndexBackfillLimits::default().batch();

        let stale_cursor = IndexCursor::try_new(Bytes::from_static(b"stale-cursor")).unwrap();
        for progress in [
            VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                cursor: Some(stale_cursor.clone()),
                counters: OperationCounters::default(),
            }),
            VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                cursor: Some(stale_cursor),
                counters: OperationCounters::default(),
            }),
        ] {
            assert!(matches!(
                step_cleanup::<vector::distance::Euclidean>(
                    &transaction,
                    scope,
                    &operation,
                    &record,
                    vector_definition,
                    &progress,
                    false,
                    limits,
                    driver.cache_registry.as_ref(),
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(_))
            ));
        }

        let tiny = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .unwrap();
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                    cursor: None,
                    counters: OperationCounters::default(),
                }),
                false,
                tiny,
                driver.cache_registry.as_ref(),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity { .. })
        ));
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &VectorCleanupProgress::RetireCache(NoCursorProgress::default()),
                false,
                limits,
                driver.cache_registry.as_ref(),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorCleanup(
                VectorCleanupProgress::DeletePhysical(_)
            ))
        ));
        assert!(matches!(
            step_cleanup::<vector::distance::Euclidean>(
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &VectorCleanupProgress::Finalize(NoCursorProgress::default()),
                false,
                limits,
                driver.cache_registry.as_ref(),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Completed(IndexOperationOutcome::DropSucceeded)
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    /// Covers source-bound ordering, indivisible input admission, malformed
    /// documents, and duplicate applied-state rejection before HNSW planning.
    #[tokio::test]
    async fn source_scan_rejects_every_preplanning_boundary() {
        let db = test_db("vector-driver-source-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("build operation exists");
        let record = read_index(&db, scope, &definition).await;
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(initial_progress),
        )) = operation.progress()
        else {
            panic!("new vector build begins at source scan");
        };
        let driver = driver();
        let limits = SearchIndexBackfillLimits::default().batch();

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let equal = SourceScanProgress {
            inclusive_upper_bound: initial_progress.inclusive_upper_bound.clone(),
            cursor: Some(initial_progress.inclusive_upper_bound.clone()),
            counters: OperationCounters::default(),
        };
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &equal,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
                VectorBuildProgress::Constructing(VectorBuildStage::CatchUp(_))
            ))
        ));
        let greater = SourceScanProgress {
            inclusive_upper_bound: initial_progress.inclusive_upper_bound.clone(),
            cursor: Some(source_cursor(scope, 1)),
            counters: OperationCounters::default(),
        };
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &greater,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);

        let tiny = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::MIN,
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
            NonZeroU64::new(1024).unwrap(),
        )
        .unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                tiny,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::OversizedEntity {
                entity_id,
                ..
            }) if entity_id == IndexEntityId::new(0)
        ));
        drop(transaction);

        db.put(source_key(scope, 0), Bytes::from_static(&[0xff]))
            .await
            .unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData {
                entity_id,
                ..
            }) if entity_id == IndexEntityId::new(0)
        ));
        drop(transaction);

        let wrong_dimension = vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new("embedding", PropertyValue::F32Array(vec![1.0, 2.0])),
        ];
        put_source(&db, scope, 0, &wrong_dimension).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
            )
            .await
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvalidSourceData { .. })
        ));
        drop(transaction);

        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        stage_applied(
            &transaction,
            scope,
            &operation,
            IndexElementKind::Node,
            IndexEntityId::new(0),
            Some(TextPartition::Unpartitioned),
        )
        .unwrap();
        assert!(matches!(
            scan_source::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                initial_progress,
                limits,
                IndexLifecycleScanTuning::default(),
                Arc::clone(&driver.simhasher_registry),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("existing applied state")
        ));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn source_scan_attributes_out_of_domain_finite_vector_to_exact_entity() {
        let db = test_db("vector-driver-magnitude-source-attribution").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let limit = crate::search::vector::magnitude_oracle::inclusive_limit(
            VectorDistanceMetric::Euclidean,
            3,
        )
        .unwrap();
        let outside = crate::search::vector::magnitude_oracle::next_up(limit);
        let entity_id = 7;
        put_source(
            &db,
            scope,
            entity_id,
            &properties([outside, 0.0, 0.0], None),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, entity_id).await;
        let mut claim_sequence = 1;
        let outcome = drive_to_terminal(&db, &driver(), operation_id, &mut claim_sequence).await;
        let operation = read_operation(&db, scope, operation_id).await;
        db.close().await.expect("vector test database closes");

        assert_eq!(outcome, CommittedOperationStep::Blocked);
        assert!(matches!(
            operation.execution_state(),
            crate::index_lifecycle::IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidSourceData {
                    entity_kind: IndexElementKind::Node,
                    entity_id: blocked_entity_id,
                }
            ) if *blocked_entity_id == IndexEntityId::new(entity_id)
        ));
    }

    #[tokio::test]
    async fn rejected_active_magnitude_mutation_preserves_every_durable_state_family() {
        let db = test_db("vector-driver-magnitude-active-rollback").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let before_properties = properties([0.0, 0.0, 0.0], None);
        put_source(&db, scope, 1, &before_properties).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver(), operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let record = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = record.state()
        else {
            panic!("fixture vector generation is active and unpartitioned")
        };
        let physical_index_id = *physical_index_id;
        let source_before = db.get(source_key(scope, 1)).await.unwrap();
        let index_key = scoped_index_key(scope, ScopedKey::index_record(definition.identity()));
        let index_before = db.get(&index_key).await.unwrap();
        let operation_key =
            crate::index_lifecycle::outbox::scoped_operation_key(scope, operation_id);
        let operation_before = db.get(&operation_key).await.unwrap();
        let vector_before = physical_vector_rows(&db, scope, physical_index_id).await;

        let limit = crate::search::vector::magnitude_oracle::inclusive_limit(
            VectorDistanceMetric::Euclidean,
            3,
        )
        .unwrap();
        let after_properties = properties(
            [
                crate::search::vector::magnitude_oracle::next_up(limit),
                0.0,
                0.0,
            ],
            None,
        );
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("active magnitude mutation transaction opens");
        transaction
            .put(source_key(scope, 1), encode_properties(&after_properties))
            .expect("authoritative graph update stages");
        let mutations = load_mutation_set(&transaction, scope)
            .await
            .expect("active vector mutation set loads");
        let cache_writes = VectorCacheWriteSet::default();
        let result = maintain_entity(
            &transaction,
            scope,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(
                IndexElementKind::Node,
                1,
                &before_properties,
                &after_properties,
            ),
        )
        .await;
        let cache_entries = cache_writes.entries().len();
        transaction.rollback();

        let source_after = db.get(source_key(scope, 1)).await.unwrap();
        let index_after = db.get(index_key).await.unwrap();
        let operation_after = db.get(operation_key).await.unwrap();
        let vector_after = physical_vector_rows(&db, scope, physical_index_id).await;
        db.close().await.expect("vector test database closes");

        let mut failures = Vec::new();
        match result {
            Err(HelixDbError::VectorComponentMagnitudeExceeded {
                metric: VectorDistanceMetric::Euclidean,
                dimension: 3,
                component_index: 0,
                observed_magnitude,
                inclusive_maximum,
            }) if observed_magnitude == crate::search::vector::magnitude_oracle::next_up(limit)
                && inclusive_maximum == limit => {}
            Err(error) => failures.push(format!(
                "active mutation returned {error:?} instead of its exact magnitude error"
            )),
            Ok(()) => {
                failures.push("active mutation accepted an out-of-domain finite vector".to_string())
            }
        }
        if cache_entries != 0 {
            failures.push(format!(
                "active mutation retained {cache_entries} transaction-local cache effect(s)"
            ));
        }
        if source_after != source_before {
            failures.push("graph source row changed after rollback".to_string());
        }
        if index_after != index_before || operation_after != operation_before {
            failures.push("lifecycle rows changed after rollback".to_string());
        }
        if vector_after != vector_before {
            failures.push("one or more physical vector lanes changed after rollback".to_string());
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[tokio::test]
    async fn invalid_active_physical_row_stays_untouched_until_explicit_drop_recreate() {
        let db = test_db("vector-driver-magnitude-active-recovery").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let valid_properties = properties([0.0, 0.0, 0.0], None);
        put_source(&db, scope, 1, &valid_properties).await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = active.state()
        else {
            panic!("fixture vector generation is active and unpartitioned")
        };
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("active vector record projects a handle");
        let generation = ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active_handle, *physical_index_id)
        .expect("active physical generation validates");
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        let (item_key, _) = physical_vector_rows(&db, scope, *physical_index_id)
            .await
            .into_iter()
            .find(|(key, _)| {
                matches!(
                    DataKey::parse_from_slice(scope, key),
                    Ok(DataKey::Data {
                        kind: DataKeyKind::Vector(VectorKey::Vector(_)),
                        ..
                    })
                )
            })
            .expect("active generation contains its canonical item row");
        let limit = crate::search::vector::magnitude_oracle::inclusive_limit(
            VectorDistanceMetric::Euclidean,
            3,
        )
        .unwrap();
        let outside = crate::search::vector::magnitude_oracle::next_up(limit);
        let invalid_properties = properties([outside, 0.0, 0.0], None);
        let invalid_item =
            crate::search::vector::encode_item(&crate::search::vector::Item::<
                vector::distance::Euclidean,
            >::new(vec![outside, 0.0, 0.0]));
        let injection = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        injection
            .put(source_key(scope, 1), encode_properties(&invalid_properties))
            .unwrap();
        injection
            .put(item_key.clone(), invalid_item.clone())
            .unwrap();
        injection.commit().await.unwrap();

        assert!(matches!(
            index.get_item(&db, 1).await,
            Err(HelixDbError::InvalidVectorItem(
                vector::VectorItemDecodeError::ComponentMagnitudeExceeded {
                    metric: VectorDistanceMetric::Euclidean,
                    dimension: 3,
                    component_index: 0,
                    observed_magnitude,
                    inclusive_maximum,
                }
            )) if observed_magnitude == outside && inclusive_maximum == limit
        ));

        let corrected_properties = properties([0.5, 0.0, 0.0], None);
        let correction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mutations = load_mutation_set(&correction, scope).await.unwrap();
        let cache_writes = VectorCacheWriteSet::default();
        maintain_entity(
            &correction,
            scope,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(
                IndexElementKind::Node,
                1,
                &invalid_properties,
                &corrected_properties,
            ),
        )
        .await
        .expect("authoritative correction does not rewrite invalid physical data");
        assert!(
            cache_writes.entries().is_empty(),
            "authoritative correction must not stage cache effects for invalid physical data"
        );
        correction
            .put(
                source_key(scope, 1),
                encode_properties(&corrected_properties),
            )
            .unwrap();
        correction.commit().await.unwrap();
        assert_eq!(db.get(&item_key).await.unwrap(), Some(invalid_item));
        assert!(matches!(
            index.get_item(&db, 1).await,
            Err(HelixDbError::InvalidVectorItem(
                vector::VectorItemDecodeError::ComponentMagnitudeExceeded { .. }
            ))
        ));

        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("explicit drop enqueues")
        else {
            panic!("active invalid generation requires one explicit drop operation")
        };
        assert_eq!(
            drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(db.get(&item_key).await.unwrap().is_none());

        let (rebuild_id, _, _) = create_build(&db, scope, &definition, 1).await;
        assert_eq!(
            drive_to_terminal(&db, &driver, rebuild_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let rebuilt = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = rebuilt.state()
        else {
            panic!("explicit rebuild activates a fresh vector generation")
        };
        let rebuilt_handle = ActiveIndexHandle::try_from_record(scope, &rebuilt)
            .expect("rebuilt active record projects a handle");
        let rebuilt_generation = ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&rebuilt_handle, *physical_index_id)
        .expect("rebuilt generation validates");
        let rebuilt_index =
            VectorIndex::<vector::distance::Euclidean>::from_generation(&rebuilt_generation);
        assert_eq!(
            rebuilt_index
                .get_item(&db, 1)
                .await
                .unwrap()
                .unwrap()
                .vector
                .to_vec(),
            vec![0.5, 0.0, 0.0]
        );
        db.close().await.expect("vector test database closes");
    }

    /// Verifies descriptor-validation cursors fail on malformed bytes and
    /// dispatch typed non-V2 and mapping keys to their exact scan lanes.
    #[tokio::test]
    async fn descriptor_validation_cursor_dispatch_is_typed() {
        let db = test_db("vector-driver-validation-cursors").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("build operation exists");
        let record = read_index(&db, scope, &definition).await;
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("fixture definition is vector");
        };
        let limits = SearchIndexBackfillLimits::default().batch();
        let driver = driver();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();

        let malformed = PrefixScanProgress {
            cursor: Some(
                IndexCursor::try_new(Bytes::from_static(b"malformed"))
                    .expect("malformed bytes still fit the cursor envelope"),
            ),
            counters: OperationCounters::default(),
        };
        assert!(validate_descriptor::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &malformed,
            limits,
            Arc::clone(&driver.simhasher_registry),
        )
        .await
        .is_err());

        for cursor in [
            source_cursor(scope, 0),
            IndexCursor::try_new(GlobalKey::StorageVersion.to_bytes())
                .expect("storage-version key is a bounded cursor"),
        ] {
            let progress = PrefixScanProgress {
                cursor: Some(cursor),
                counters: OperationCounters::default(),
            };
            validate_descriptor::<vector::distance::Euclidean>(
                &db,
                &transaction,
                scope,
                &operation,
                &record,
                vector_definition,
                &progress,
                limits,
                Arc::clone(&driver.simhasher_registry),
            )
            .await
            .expect_err("typed non-V2 cursor cannot resume the applied-state lane");
        }
        let mapping = PrefixScanProgress {
            cursor: Some(
                IndexCursor::try_new(scoped_index_key(
                    scope,
                    ScopedKey::VectorPartitionMapping(
                        crate::encoding::v2::keys::VectorPartitionMappingKey {
                            index_id,
                            generation,
                            partition: TextPartition::Unpartitioned.fingerprint(),
                        },
                    ),
                ))
                .expect("mapping key is a bounded cursor"),
            ),
            counters: OperationCounters::default(),
        };
        validate_descriptor::<vector::distance::Euclidean>(
            &db,
            &transaction,
            scope,
            &operation,
            &record,
            vector_definition,
            &mapping,
            limits,
            Arc::clone(&driver.simhasher_registry),
        )
        .await
        .expect("typed mapping cursor resumes mapping validation");
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    /// Drives every typed vector work-row decoder through wrong-key,
    /// wrong-value, and key/value-ownership failures and covers helper states
    /// that normal lifecycle construction makes unreachable.
    #[tokio::test]
    async fn typed_row_decoders_and_batch_accounting_fail_closed() {
        let db = test_db("vector-driver-row-boundaries").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let operation = crate::index_lifecycle::outbox::read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("build operation exists");
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(0),
        };
        let other_entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        };
        let delta_key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        );
        let applied_key = applied_key(
            scope,
            index_id,
            generation,
            IndexElementKind::Node,
            IndexEntityId::new(0),
        );
        let delta_value = CoalescedBuildDeltaValue {
            index_id,
            generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
        };
        let applied_value = AppliedEntityStateValue {
            index_id,
            generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
            state: AppliedFamilyState::Vector(Some(TextPartition::Unpartitioned)),
        };
        let encoded_delta = encode_build_delta(&delta_value);
        let encoded_applied = encode_applied_state(&applied_value.clone());

        assert!(matches!(
            decode_delta(scope, &applied_key, &encoded_delta),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another key kind")
        ));
        assert!(matches!(
            decode_delta(scope, &delta_key, &encoded_applied),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another value kind")
        ));
        let mismatched_delta = encode_build_delta(&CoalescedBuildDeltaValue {
            entity_id: other_entity.id,
            ..delta_value
        });
        assert!(matches!(
            decode_delta(scope, &delta_key, &mismatched_delta),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("key/value mismatch")
        ));

        assert!(matches!(
            decode_applied(scope, &delta_key, &encoded_applied),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another key kind")
        ));
        assert!(matches!(
            decode_applied(scope, &applied_key, &encoded_delta),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another value kind")
        ));
        let mismatched_applied = encode_applied_state(&AppliedEntityStateValue {
            entity_id: other_entity.id,
            ..applied_value.clone()
        });
        assert!(matches!(
            decode_applied(scope, &applied_key, &mismatched_applied),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("key/value mismatch")
        ));

        let tenant = VectorTenantPartition::try_new(Bytes::from_static(b"tenant")).unwrap();
        let mapping_key = scoped_index_key(
            scope,
            ScopedKey::VectorPartitionMapping(
                crate::encoding::v2::keys::VectorPartitionMappingKey {
                    index_id,
                    generation,
                    partition: tenant.fingerprint(),
                },
            ),
        );
        let mapping_value = crate::index_lifecycle::work::VectorPartitionMappingValue {
            index_id,
            generation,
            partition: tenant.clone(),
            physical_index_id: VectorPhysicalIndexId::initial(),
        };
        let encoded_mapping = encode_partition_mapping(&mapping_value.clone());
        assert!(matches!(
            decode_mapping(scope, &delta_key, &encoded_mapping, &operation),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another key kind")
        ));
        assert!(matches!(
            decode_mapping(scope, &mapping_key, &encoded_delta, &operation),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another value kind")
        ));
        let mismatched_mapping =
            encode_partition_mapping(&crate::index_lifecycle::work::VectorPartitionMappingValue {
                index_id: IndexId::new(index_id.get() + 1).unwrap(),
                ..mapping_value
            });
        assert!(matches!(
            decode_mapping(scope, &mapping_key, &mismatched_mapping, &operation),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("ownership mismatch")
        ));

        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(applied_key.clone(), mismatched_applied)
            .unwrap();
        assert!(matches!(
            load_applied(
                &transaction,
                scope,
                index_id,
                generation,
                entity.kind,
                entity.id,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("key/value mismatch")
        ));
        transaction
            .put(
                applied_key.clone(),
                encode_applied_state(&AppliedEntityStateValue {
                    state: AppliedFamilyState::Secondary(None),
                    ..applied_value
                }),
            )
            .unwrap();
        assert!(matches!(
            load_applied(
                &transaction,
                scope,
                index_id,
                generation,
                entity.kind,
                entity.id,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("another applied family")
        ));
        stage_applied(
            &transaction,
            scope,
            &operation,
            entity.kind,
            entity.id,
            None,
        )
        .unwrap();
        assert!(load_applied(
            &transaction,
            scope,
            index_id,
            generation,
            entity.kind,
            entity.id,
        )
        .await
        .unwrap()
        .is_none());
        assert!(!generation_has_rows(
            &transaction,
            scope,
            RecordKind::VectorPartitionMapping,
            index_id,
            generation,
        )
        .await
        .unwrap());
        transaction
            .put(delta_key.clone(), encoded_delta.clone())
            .unwrap();
        assert!(generation_has_rows(
            &transaction,
            scope,
            RecordKind::BuildDelta,
            index_id,
            generation,
        )
        .await
        .unwrap());

        assert!(
            read_authoritative_properties(&transaction, scope, other_entity)
                .await
                .unwrap()
                .is_none()
        );
        let edge = IndexEntity {
            kind: IndexElementKind::Edge,
            id: IndexEntityId::new(7),
        };
        let edge_key = DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(
                crate::encoding::v2::keys::EdgePropertyByIdKey::new(edge.id.get()),
            ),
        }
        .to_bytes();
        transaction
            .put(edge_key.clone(), Bytes::from_static(&[0xff]))
            .unwrap();
        assert!(read_authoritative_properties(&transaction, scope, edge)
            .await
            .is_err());
        assert_eq!(
            source_entity(scope, IndexElementKind::Edge, &edge_key).unwrap(),
            Some(edge.id)
        );
        assert_eq!(
            source_entity(scope, IndexElementKind::Edge, &source_key(scope, 0)).unwrap(),
            None
        );
        assert!(source_entity(scope, IndexElementKind::Node, &edge_key).is_err());
        let global = IndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes();
        assert!(source_entity(scope, IndexElementKind::Node, &global).is_err());

        let prefix = source_prefix(scope, IndexElementKind::Node);
        assert_eq!(cursor_suffix(&prefix, None).unwrap(), None);
        assert_eq!(
            cursor_suffix(&prefix, Some(&source_cursor(scope, 0))).unwrap(),
            Some(source_key(scope, 0).slice(prefix.len()..))
        );
        assert!(cursor_suffix(
            &prefix,
            Some(&IndexCursor::try_new(Bytes::from_static(b"outside")).unwrap()),
        )
        .is_err());

        assert!(load_operation_index(&transaction, scope, &operation)
            .await
            .is_ok());
        assert!(matches!(
            load_operation_index(
                &transaction,
                DataScope::Tenant(
                    crate::encoding::v2::keys::scope::TenantId::from_u128(1)
                ),
                &operation,
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason.contains("no canonical index")
        ));

        let limits = SearchIndexBackfillLimits::default().batch();
        let progress = SourceScanProgress {
            inclusive_upper_bound: source_cursor(scope, 0),
            cursor: None,
            counters: OperationCounters::default(),
        };
        assert!(matches!(
            finish_or_block_scan(
                EntityPlanOutcome::Blocked(IndexOperationBlocker::InvariantViolation),
                VectorBatchAccounting::new(OperationCounters::default(), limits),
                entity.kind,
                entity.id,
                &progress,
                None,
                VectorBuildSessionStats::default(),
            )
            .unwrap()
            .result,
            IndexOperationStepResult::Blocked(IndexOperationBlocker::InvariantViolation)
        ));
        assert!(matches!(
            finish_or_block_scan(
                EntityPlanOutcome::BatchFull,
                VectorBatchAccounting::new(OperationCounters::default(), limits),
                entity.kind,
                entity.id,
                &progress,
                None,
                VectorBuildSessionStats::default(),
            )
            .unwrap()
            .result,
            IndexOperationStepResult::Progressed(IndexOperationProgress::VectorBuild(
                VectorBuildProgress::Constructing(VectorBuildStage::Scan(_))
            ))
        ));
        assert!(finish_or_block_scan(
            EntityPlanOutcome::Admitted {
                vector_writes: VectorWriteMeasurement::zero(),
                single_vector_output_bytes: 0,
                lifecycle_operations: 0,
                lifecycle_bytes: 0,
                next_partition: None,
            },
            VectorBatchAccounting::new(OperationCounters::default(), limits),
            entity.kind,
            entity.id,
            &progress,
            None,
            VectorBuildSessionStats::default(),
        )
        .is_err());

        let mut accounting = VectorBatchAccounting::new(
            OperationCounters {
                entities: u64::MAX,
                ..OperationCounters::default()
            },
            limits,
        );
        accounting
            .admit(1, VectorWriteMeasurement::zero(), 0, 1, 1)
            .unwrap();
        assert!(accounting.finish().is_err());

        let mut planning_accounting =
            VectorBatchAccounting::new(OperationCounters::default(), limits);
        planning_accounting.record_planning();
        planning_accounting
            .admit(1, VectorWriteMeasurement::for_test(3, 30), 30, 0, 0)
            .unwrap();
        let planning = planning_accounting.planning_usage(VectorBuildSessionStats::default());
        assert_eq!(planning.planning_executions, 1);
        assert_eq!(planning.planned_writes, 3);
        assert_eq!(planning.replay_executions, 0);

        let record = read_index(&db, scope, &definition).await;
        let unpartitioned = BuildPhysicalResolution {
            scope,
            layout: VectorPhysicalLayout::Unpartitioned {
                physical_index_id: VectorPhysicalIndexId::initial(),
            },
            physical_index_id: VectorPhysicalIndexId::initial(),
            mapping_is_new: true,
        };
        assert!(lifecycle_write_measurement(
            scope,
            &operation,
            entity.kind,
            entity.id,
            AppliedStateTransition::Absent,
            Some(&unpartitioned),
            false,
        )
        .is_err());
        assert!(lifecycle_write_measurement(
            scope,
            &operation,
            entity.kind,
            entity.id,
            AppliedStateTransition::Put(&TextPartition::Unpartitioned),
            Some(&unpartitioned),
            false,
        )
        .is_err());
        assert!(lifecycle_write_measurement(
            scope,
            &operation,
            entity.kind,
            entity.id,
            AppliedStateTransition::Delete,
            None,
            true,
        )
        .is_ok());
        assert!(matches!(record.state(), IndexStateV2::Building { .. }));
        drop(transaction);
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn active_unpartitioned_search_matches_deterministic_brute_force_oracle() {
        const VECTORS: [[f32; 3]; 8] = [
            [0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 2.0, 0.0],
            [0.0, 0.0, 3.0],
            [4.0, 0.0, 0.0],
            [0.0, 5.0, 0.0],
            [0.0, 0.0, 6.0],
            [7.0, 7.0, 7.0],
        ];
        const QUERY: [f32; 3] = [0.25, 0.5, 0.75];

        let db = test_db("vector-driver-brute-force-oracle").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        for (entity_id, vector) in VECTORS.iter().enumerate() {
            put_source(&db, scope, entity_id as u64, &properties(*vector, None)).await;
        }
        let (build_id, _, _) =
            create_build(&db, scope, &definition, VECTORS.len() as u64 - 1).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let active = read_index(&db, scope, &definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = active.state()
        else {
            panic!("completed vector build is active and unpartitioned");
        };
        let active_handle = ActiveIndexHandle::try_from_record(scope, &active)
            .expect("active vector record projects a handle");
        let generation = ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active_handle, *physical_index_id)
        .expect("active physical generation validates");
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        let params = SearchParams::new(VECTORS.len())
            .unwrap()
            .with_ef(VECTORS.len())
            .unwrap()
            .with_simhash_mode(SimHashMode::Off)
            .with_pre_simhash_sampling_ratio(1.0)
            .unwrap();
        let actual = index.search(&db, &QUERY, &params).await.unwrap();

        let mut expected = VECTORS
            .iter()
            .enumerate()
            .map(|(entity_id, vector)| {
                let score = vector
                    .iter()
                    .zip(QUERY)
                    .map(|(component, query)| {
                        let difference = *component - query;
                        difference * difference
                    })
                    .sum::<f32>();
                (
                    entity_id as u64,
                    DistanceScore::try_new(score).expect("oracle score is finite"),
                )
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        assert_eq!(
            actual
                .into_iter()
                .map(|result| (result.entity_id(), result.score()))
                .collect::<Vec<_>>(),
            expected
        );
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn oversized_partition_build_blocks_before_mapping_or_watermark_writes() {
        let db = test_db("vector-driver-block-before-physical").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(Some("account_id"));
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], Some(10))).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let before_watermark = peek_vector_physical_id(&db)
            .await
            .expect("vector watermark is readable");
        let tiny_output = SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(1024 * 1024).expect("input limit is positive"),
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            NonZeroU64::MIN,
        )
        .expect("tiny output policy validates");
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver(), build_id, &mut claim_sequence, tiny_output,).await,
            CommittedOperationStep::Blocked
        );
        assert!(mapping_values(&db, scope, index_id, generation)
            .await
            .is_empty());
        assert_eq!(
            peek_vector_physical_id(&db)
                .await
                .expect("vector watermark remains readable"),
            before_watermark
        );
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn abort_removes_hidden_physical_rows_and_builder_work() {
        let db = test_db("vector-driver-abort-cleanup").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        put_source(&db, scope, 0, &properties([1.0, 2.0, 3.0], None)).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = driver();
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("building vector converts to abort cleanup");
        assert!(matches!(
            receipt,
            IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
        ));
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        for kind in [
            RecordKind::BuildDelta,
            RecordKind::AppliedState,
            RecordKind::VectorPartitionMapping,
        ] {
            let prefix = generation_prefix(scope, kind, index_id, generation);
            let mut rows = db
                .scan_prefix(prefix, ..)
                .await
                .expect("cleanup generation prefix is readable");
            assert!(rows
                .next()
                .await
                .expect("cleanup generation row is readable")
                .is_none());
        }
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn adoption_abort_restores_source_reservation_without_deleting_physical_rows() {
        let db = test_db("vector-driver-adoption-abort").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let physical_index_id = VectorPhysicalIndexId::new(55).expect("fixture ID is nonzero");
        let physical_row_key = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(
                crate::encoding::v2::keys::indexes::vector::VectorKey::SimHash(
                    crate::encoding::v2::keys::indexes::vector::VectorSimHashKey::new(
                        physical_index_id.get(),
                        77,
                    ),
                ),
            ),
        }
        .to_bytes();
        let physical_row_value = Bytes::copy_from_slice(
            &crate::encoding::v2::values::indexes::vector::simhash::encode_simhash(17),
        );
        let directory_keys = [1_u64, 2_u64].map(|node_id| {
            DataKey::Data {
                scope,
                kind: DataKeyKind::Vector(
                    crate::encoding::v2::keys::indexes::vector::VectorKey::SimHashDirectory(
                        crate::encoding::v2::keys::indexes::vector::VectorSimHashDirectoryKey::new(
                            physical_index_id.get(),
                            node_id,
                            node_id,
                        ),
                    ),
                ),
            }
            .to_bytes()
        });
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy source transaction opens");
        transaction
            .put(&physical_row_key, &physical_row_value)
            .expect("legacy physical row stages");
        for directory_key in &directory_keys {
            transaction
                .put(
                    directory_key,
                    crate::encoding::v2::values::indexes::vector::markers::encode_simhash_directory_marker_v1(
                    ),
                )
                .expect("partial directory marker stages");
        }
        transaction
            .put(
                IndexKey::Global {
                    kind: GlobalKey::LegacyVectorPhysicalReservation(physical_index_id),
                }
                .to_bytes(),
                encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::LegacySource,
                )),
            )
            .expect("legacy source reservation stages");
        transaction
            .commit()
            .await
            .expect("legacy source transaction commits");

        let receipt = create_legacy_vector_adoption_operation(
            &db,
            scope,
            definition.clone(),
            physical_index_id,
        )
        .await
        .expect("legacy adoption enqueues");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new legacy adoption must enqueue one build")
        };
        assert!(matches!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                physical_index_id,
            )
            .await
            .expect("building reservation reads"),
            Some(LegacyVectorPhysicalReservation::AdoptionBuilding {
                operation_id: owner_operation,
                ..
            }) if owner_operation == operation_id
        ));
        let receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("adoption converts to abort cleanup");
        assert!(matches!(
            receipt,
            IndexDdlReceipt::ExistingOperation { operation_id: aborted } if aborted == operation_id
        ));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver(), operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert_eq!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                physical_index_id,
            )
            .await
            .expect("restored source reservation reads"),
            Some(LegacyVectorPhysicalReservation::LegacySource)
        );
        assert_eq!(
            db.get(physical_row_key)
                .await
                .expect("legacy physical row reads"),
            Some(physical_row_value),
            "adoption abort must not delete or rewrite legacy physical rows"
        );
        for directory_key in directory_keys {
            assert!(
                db.get(directory_key)
                    .await
                    .expect("partial directory marker reads")
                    .is_none(),
                "adoption abort must delete only its partial directory"
            );
        }
        db.close().await.expect("vector test database closes");
    }

    #[tokio::test]
    async fn adoption_blocks_out_of_domain_legacy_physical_as_invalid() {
        let db = test_db("vector-driver-magnitude-legacy-adoption").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector definition is vector")
        };
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_index_id =
            VectorPhysicalIndexId::new(crate::search::vector::index_id_from_name(&physical_name))
                .expect("legacy fixture physical ID is nonzero");
        let limit = crate::search::vector::magnitude_oracle::inclusive_limit(
            VectorDistanceMetric::Euclidean,
            3,
        )
        .unwrap();
        let outside = crate::search::vector::magnitude_oracle::next_up(limit);
        let mut metadata = crate::search::vector::VectorIndexMetadata::new(
            VectorIndexConfig::from_v2_definition(vector_definition, &physical_name),
        );
        metadata.entry_point = Some(1);
        metadata.count = 1;
        let (legacy_key, legacy_value) =
            crate::migrations::migration_parity_legacy_catalog_row(&definition, false)
                .expect("legacy catalog row encodes");
        let simhash_bits = 0_u64;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy magnitude seed transaction opens");
        transaction
            .put(legacy_key, legacy_value)
            .expect("legacy catalog row stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                            crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
                                physical_index_id.get(),
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("legacy metadata stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::SimHash(
                            crate::encoding::v2::keys::indexes::vector::VectorSimHashKey::new(
                                physical_index_id.get(),
                                1,
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::values::indexes::vector::simhash::encode_simhash(
                        simhash_bits,
                    ),
                ),
            )
            .expect("legacy SimHash stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::Vector(
                            crate::encoding::v2::keys::indexes::vector::VectorItemKey::new(
                                physical_index_id.get(),
                                crate::search::vector::simhash::order_code_from_simhash_bits(
                                    simhash_bits,
                                ),
                                1,
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                crate::search::vector::encode_item(&crate::search::vector::Item::<
                    crate::search::vector::distance::Euclidean,
                >::new(vec![
                    outside, 0.0, 0.0,
                ])),
            )
            .expect("legacy out-of-domain payload stages");
        transaction
            .commit()
            .await
            .expect("legacy magnitude fixture commits");
        crate::migrations::preflight_legacy_vector_reservations(&db)
            .await
            .expect("legacy namespace preflight succeeds");
        let receipt = create_legacy_vector_adoption_operation(
            &db,
            scope,
            definition.clone(),
            physical_index_id,
        )
        .await
        .expect("legacy adoption enqueues");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new legacy adoption must enqueue one build")
        };
        let mut claim_sequence = 1;
        let driver = driver();
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let outcome = drive_one(
            &db,
            &driver,
            operation_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await;
        let operation = read_operation(&db, scope, operation_id).await;
        db.close().await.expect("vector test database closes");

        assert_eq!(outcome, CommittedOperationStep::Blocked);
        assert!(matches!(
            operation.execution_state(),
            crate::index_lifecycle::IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::InvalidLegacyPhysical
            )
        ));
    }

    #[tokio::test]
    async fn adoption_reopens_after_each_validation_lane_and_activation_boundary() {
        let store = Arc::new(InMemory::new());
        let database = "vector-driver-adoption-lane-reopen";
        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("vector adoption database opens");
        bootstrap_writer(&db)
            .await
            .expect("vector adoption database bootstraps");
        let scope = DataScope::LegacyUnscoped;
        let definition = definition(None);
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector definition is vector")
        };
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_index_id =
            VectorPhysicalIndexId::new(crate::search::vector::index_id_from_name(&physical_name))
                .expect("legacy fixture physical ID is nonzero");
        let metadata = crate::search::vector::VectorIndexMetadata::new(
            VectorIndexConfig::from_v2_definition(vector_definition, &physical_name),
        );
        let (legacy_key, legacy_value) =
            crate::migrations::migration_parity_legacy_catalog_row(&definition, false)
                .expect("legacy catalog row encodes");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy seed transaction opens");
        transaction
            .put(legacy_key.clone(), legacy_value)
            .expect("legacy catalog row stages");
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::Vector(
                        crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                            crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
                                physical_index_id.get(),
                            ),
                        ),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("legacy metadata stages");
        transaction
            .commit()
            .await
            .expect("legacy seed transaction commits");
        crate::migrations::preflight_legacy_vector_reservations(&db)
            .await
            .expect("legacy namespace preflight succeeds");
        let receipt = create_legacy_vector_adoption_operation(
            &db,
            scope,
            definition.clone(),
            physical_index_id,
        )
        .await
        .expect("legacy adoption enqueues");
        let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
            panic!("new legacy adoption must enqueue one build")
        };
        let mut claim_sequence = 1;
        let driver = driver();
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                    lane: LegacyVectorValidationLane::Hot,
                    ..
                })
            ))
        ));
        db.close().await.expect("core checkpoint closes");

        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("core checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::AdoptLegacy(LegacyVectorValidationProgress {
                    lane: LegacyVectorValidationLane::Layer0,
                    ..
                })
            ))
        ));
        db.close().await.expect("hot checkpoint closes");

        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("hot checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::ValidateAdoptedDirectory(_)
            ))
        ));
        db.close().await.expect("layer-zero checkpoint closes");

        let db = Db::builder(database, store.clone())
            .build()
            .await
            .expect("directory checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            read_operation(&db, scope, operation_id).await.progress(),
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                VectorBuildStage::Activate(_)
            ))
        ));
        db.close().await.expect("directory checkpoint closes");

        let db = Db::builder(database, store)
            .build()
            .await
            .expect("activation checkpoint reopens");
        assert_eq!(
            drive_one(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Active { .. }
        ));
        assert!(db
            .get(legacy_key)
            .await
            .expect("legacy catalog reads")
            .is_none());
        assert!(matches!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                &db,
                physical_index_id,
            )
            .await
            .expect("active reservation reads"),
            Some(LegacyVectorPhysicalReservation::AdoptedActive { .. })
        ));
        db.close().await.expect("active adoption closes");
    }
}
