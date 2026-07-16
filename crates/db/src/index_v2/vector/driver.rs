//! Bounded outbox driver for hidden vector construction.
//!
//! Each source or catch-up step plans deterministic HNSW writes in a disposable
//! transaction, admits the complete last-write-wins vector write set, and then
//! replays the same physical IDs and selected layers in the outbox transaction.
//! The replay transaction also owns tenant mappings, builder-applied state,
//! delta deletion, and the next durable checkpoint.
//!
//! No vector row codec is defined here. Physical reads and writes remain behind
//! [`crate::search::vector::VectorIndex`] and the typed `encoding/v1` boundary.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use rand::{rngs::StdRng, SeedableRng};
use sha2::{Digest, Sha256};
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::config::SearchIndexBatchLimits;
use crate::encoding::property::{decode_properties, Property};
use crate::encoding::v1::keys::index_v2::{
    GlobalIndexV2Key, IndexEntity, IndexEntityStateKey, IndexV2Key, IndexV2RecordKind,
};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key, KeyPrefix};
use crate::encoding::v1::values::index_v2::{
    decode_index_record, decode_work_value, encode_metadata_value, encode_work_value,
    IndexV2WorkValue,
};
use crate::error::{HelixDbError, Result};
use crate::search::vector::{
    self, Distance, MeasuredVectorTransaction, ValidatedVectorBuildGenerationHandle,
    ValidatedVectorCleanupAuthority, VectorCleanupRow, VectorDistanceMetric, VectorIndex,
    VectorIndexConfig, VectorWriteMeasurement, VectorWriteRecorder,
};

use super::{vector_document, VectorIndexedDocument};
use crate::index_v2::outbox::{
    IndexOperationDriver, IndexOperationStepPermit, IndexOperationStepResult,
    PreparedIndexOperationStep,
};
use crate::index_v2::reader_lease::IndexLeaseCoordinator;
use crate::index_v2::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaValue, VectorTenantPartition,
};
use crate::index_v2::{
    BuildOperationOutcome, DrainProgress, IndexCursor, IndexElementKind, IndexEntityId,
    IndexGenerationId, IndexId, IndexOperationBlocker, IndexOperationFamily, IndexOperationOutcome,
    IndexOperationProgress, IndexOperationRecord, IndexRecordV2, IndexV2MetadataValue,
    NoCursorProgress, OperationCounters, PhysicalGeneration, PrefixScanProgress,
    SourceScanProgress, TextPartition, ValidatedDynamicIndexDefinition,
    ValidatedVectorIndexDefinition, VectorBuildProgress, VectorBuildStage, VectorCleanupProgress,
    VectorPhysicalIdWatermark, VectorPhysicalIndexId, VectorPhysicalLayout,
};

/// Vector lifecycle driver sharing scope gates and the bounded SimHash owner.
pub(crate) struct VectorIndexDriver {
    scope_gates: Arc<crate::index_v2::IndexScopeGates>,
    cache_registry: Arc<vector::VectorCacheRegistry>,
    simhasher_registry: Arc<vector::SimHasherRegistry>,
    reader_leases: Option<Arc<dyn IndexLeaseCoordinator>>,
}

impl core::fmt::Debug for VectorIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("VectorIndexDriver")
            .field("reader_coordination", &self.reader_leases.is_some())
            .finish_non_exhaustive()
    }
}

impl VectorIndexDriver {
    /// Installs vector work against mutation, cache, and reader authorities.
    pub(crate) fn with_reader_leases(
        scope_gates: Arc<crate::index_v2::IndexScopeGates>,
        cache_registry: Arc<vector::VectorCacheRegistry>,
        simhasher_registry: Arc<vector::SimHasherRegistry>,
        reader_leases: Option<Arc<dyn IndexLeaseCoordinator>>,
    ) -> Self {
        Self {
            scope_gates,
            cache_registry,
            simhasher_registry,
            reader_leases,
        }
    }

    /// Creates an isolated process-local reader authority for family unit tests.
    #[cfg(test)]
    fn new(
        scope_gates: Arc<crate::index_v2::IndexScopeGates>,
        cache_registry: Arc<vector::VectorCacheRegistry>,
        simhasher_registry: Arc<vector::SimHasherRegistry>,
    ) -> Self {
        Self::with_reader_leases(
            scope_gates,
            cache_registry,
            simhasher_registry,
            Some(Arc::new(
                crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                    crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
                ),
            )),
        )
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
                VectorBuildStage::CatchUp(_)
                    | VectorBuildStage::ValidateDescriptor(_)
                    | VectorBuildStage::Activate(_)
            )) | IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
                | IndexOperationProgress::VectorCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.exclusive_permit(scope).await));
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
        let Some(prepared) = crate::index_v2::reader_lifecycle::prepare_reader_lifecycle_step(
            self.reader_leases.as_ref(),
            scope,
            operation,
        )
        .await
        else {
            return Ok(PreparedIndexOperationStep::driver_owned(
                self.family(),
                permit,
            ));
        };
        Ok(PreparedIndexOperationStep::reader_lifecycle(
            self.family(),
            permit,
            prepared,
        ))
    }

    async fn step(
        &self,
        db: &Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepResult> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            return Err(corruption("vector operation loaded another family"));
        };
        match operation.progress() {
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
                            Arc::clone(&self.simhasher_registry),
                        )
                        .await
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
                            Arc::clone(&self.simhasher_registry),
                        )
                        .await
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
                            Arc::clone(&self.simhasher_registry),
                        )
                        .await
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
                        unreachable!("outer match selected vector cleanup")
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
                        .await
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
                        .await
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
                        .await
                    }
                }
            }
            IndexOperationProgress::SecondaryBuild(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::SecondaryCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => {
                Err(corruption("vector driver received another family progress"))
            }
        }
    }

    async fn after_commit(
        &self,
        scope: DataScope,
        index: &IndexRecordV2,
        operation: &IndexOperationRecord,
        committed: crate::index_v2::outbox::CommittedOperationStep,
    ) {
        if committed != crate::index_v2::outbox::CommittedOperationStep::Completed
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
) -> Result<IndexOperationStepResult> {
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
            | VectorCleanupProgress::FinishDrain(_)
            | VectorCleanupProgress::Finalize(_)
    ) {
        cache_registry.retire_cleanup_generation(&authority).await;
    }
    let next = match progress {
        VectorCleanupProgress::BeginDrain(progress) => {
            return Ok(IndexOperationStepResult::Blocked(
                if progress.drain_epoch.is_some() {
                    IndexOperationBlocker::InvariantViolation
                } else {
                    IndexOperationBlocker::ReaderCoordinationUnavailable
                },
            ));
        }
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
                        return Ok(IndexOperationStepResult::Blocked(blocker));
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
                    return Ok(progressed_cleanup(
                        aborting,
                        VectorCleanupProgress::DeleteDeltas(PrefixScanProgress {
                            cursor: None,
                            counters: progress.counters,
                        }),
                    ));
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
                        return Ok(IndexOperationStepResult::Blocked(blocker));
                    }
                    PhysicalCleanupOutcome::Progress {
                        counters,
                        mapping_deleted: _,
                        ..
                    } => VectorCleanupProgress::DeletePhysical(PrefixScanProgress {
                        cursor: Some(mapping.cursor),
                        counters,
                    }),
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
                    return Ok(IndexOperationStepResult::Blocked(blocker));
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
                } => VectorCleanupProgress::FinishDrain(DrainProgress {
                    drain_epoch: None,
                    counters,
                }),
            }
        }
        VectorCleanupProgress::FinishDrain(progress) => {
            return Ok(IndexOperationStepResult::Blocked(
                if progress.drain_epoch.is_some() {
                    IndexOperationBlocker::InvariantViolation
                } else {
                    IndexOperationBlocker::ReaderCoordinationUnavailable
                },
            ));
        }
        VectorCleanupProgress::Finalize(_) => {
            return Ok(IndexOperationStepResult::Completed(if aborting {
                IndexOperationOutcome::Build(BuildOperationOutcome::Aborted)
            } else {
                IndexOperationOutcome::DropSucceeded
            }));
        }
    };
    Ok(progressed_cleanup(aborting, next))
}

/// One partition mapping retained until its physical namespace is empty.
struct MappingCleanupRow {
    key: Bytes,
    cursor: IndexCursor,
    input_bytes: u64,
    value: crate::index_v2::work::VectorPartitionMappingValue,
}

async fn current_or_next_mapping(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    cursor: Option<&IndexCursor>,
) -> Result<Option<MappingCleanupRow>> {
    let prefix = generation_prefix(
        scope,
        IndexV2RecordKind::VectorPartitionMapping,
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
    while rows.len() < limits.max_entities().get() {
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
    if rows.len() == limits.max_entities().get() {
        namespace_empty = false;
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
    for kind in [
        IndexV2RecordKind::BuildDelta,
        IndexV2RecordKind::AppliedState,
    ] {
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
                    let entity = match kind {
                        IndexV2RecordKind::BuildDelta => {
                            decode_delta(scope, &row.key, &row.value)?.0
                        }
                        IndexV2RecordKind::AppliedState => {
                            decode_applied(scope, &row.key, &row.value)?.0
                        }
                        IndexV2RecordKind::IndexRecord
                        | IndexV2RecordKind::Operation
                        | IndexV2RecordKind::SecondaryEntry
                        | IndexV2RecordKind::TextManifestRoot
                        | IndexV2RecordKind::TextManifestPage
                        | IndexV2RecordKind::TextUploadIntent
                        | IndexV2RecordKind::TextBuildArtifact
                        | IndexV2RecordKind::BlobGcCandidate
                        | IndexV2RecordKind::BlobGcState
                        | IndexV2RecordKind::TextEntityState
                        | IndexV2RecordKind::ActiveMutationCommitProof
                        | IndexV2RecordKind::BlobReachabilityReference
                        | IndexV2RecordKind::VectorPartitionMapping => {
                            unreachable!("cleanup loop admits only delta and applied rows")
                        }
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
            accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 1, output_bytes)?;
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
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<IndexOperationStepResult> {
    match stage {
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
        VectorBuildStage::ValidateDescriptor(progress) => {
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
            .await
        }
        VectorBuildStage::Activate(progress) => {
            if generation_has_rows(
                transaction,
                scope,
                IndexV2RecordKind::BuildDelta,
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
            if generation_has_rows(
                transaction,
                scope,
                IndexV2RecordKind::AppliedState,
                operation.index_id(),
                operation.generation(),
            )
            .await?
            {
                return Ok(progressed_build(VectorBuildStage::ValidateDescriptor(
                    PrefixScanProgress {
                        cursor: None,
                        counters: progress.counters,
                    },
                )));
            }
            Ok(IndexOperationStepResult::Completed(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
            ))
        }
    }
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
    simhasher_registry: Arc<vector::SimHasherRegistry>,
) -> Result<IndexOperationStepResult> {
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
            return Ok(progressed_build(VectorBuildStage::CatchUp(
                PrefixScanProgress {
                    cursor: None,
                    counters: progress.counters,
                },
            )));
        }
        Some(std::cmp::Ordering::Less) | None => {}
    }
    let start = start.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&source_prefix, (start, Bound::Included(upper)))
        .await?;
    let planning = db.begin(IsolationLevel::Snapshot).await?;
    let planning_recorder = VectorWriteRecorder::new();
    let replay_recorder = VectorWriteRecorder::new();
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
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    },
                ));
            }
            exhausted = false;
            break;
        }
        let complete_cursor = IndexCursor::try_new(row.key.clone()).map_err(operation_error)?;
        let Some(entity_id) = source_entity(scope, definition.element_kind(), &row.key)? else {
            accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0)?;
            cursor = Some(complete_cursor);
            continue;
        };
        let properties = match decode_properties(&row.value) {
            Ok(properties) => properties,
            Err(_) => {
                return Ok(invalid_source(definition.element_kind(), entity_id));
            }
        };
        let document = match vector_document(definition, &properties) {
            Ok(document) => document,
            Err(_) => {
                return Ok(invalid_source(definition.element_kind(), entity_id));
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
        let outcome = plan_and_replay::<D>(
            &planning,
            &planning_recorder,
            transaction,
            &replay_recorder,
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
        )
        .await?;
        let EntityPlanOutcome::Admitted {
            vector_writes,
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
            lifecycle_operations,
            lifecycle_bytes,
        )?;
        cursor = Some(complete_cursor);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let counters = accounting.finish()?;
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
    Ok(progressed_build(next))
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
) -> Result<IndexOperationStepResult> {
    let prefix = generation_prefix(
        scope,
        IndexV2RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    );
    let mut rows = transaction.scan_prefix(&prefix, ..).await?;
    let planning = db.begin(IsolationLevel::Snapshot).await?;
    let planning_recorder = VectorWriteRecorder::new();
    let replay_recorder = VectorWriteRecorder::new();
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
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        observed: input_bytes,
                        limit: limits.max_input_bytes().get(),
                    },
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
                Err(_) => return Ok(invalid_source(entity.kind, entity.id)),
            },
            None => None,
        };
        let outcome = plan_and_replay::<D>(
            &planning,
            &planning_recorder,
            transaction,
            &replay_recorder,
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
        )
        .await?;
        let EntityPlanOutcome::Admitted {
            vector_writes,
            lifecycle_operations,
            lifecycle_bytes,
            next_partition,
        } = outcome
        else {
            if let EntityPlanOutcome::Blocked(blocker) = outcome {
                return Ok(IndexOperationStepResult::Blocked(blocker));
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
            lifecycle_operations,
            lifecycle_bytes,
        )?;
    }
    let counters = accounting.finish()?;
    if saw_row {
        return Ok(progressed_build(VectorBuildStage::CatchUp(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
        )));
    }
    Ok(progressed_build(VectorBuildStage::ValidateDescriptor(
        PrefixScanProgress {
            cursor: None,
            counters,
        },
    )))
}

#[allow(
    clippy::too_many_arguments,
    reason = "planning binds the exact operation, descriptor, source state, and two transaction views"
)]
async fn plan_and_replay<D: Distance>(
    planning: &DbTransaction,
    planning_recorder: &VectorWriteRecorder,
    transaction: &DbTransaction,
    replay_recorder: &VectorWriteRecorder,
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
    )
    .await?;
    let entity_vector = planning_write
        .measurement_since(checkpoint)
        .map_err(measurement_error)?;
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
        let allocated = crate::index_v2::repository::stage_vector_partition_mapping(
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
    let replay_write = replay_recorder.bind(transaction);
    apply_planned_change::<D>(
        &replay_write,
        operation,
        record,
        definition,
        simhasher_registry,
        entity_id,
        previous_resolution.as_ref(),
        next_resolution.as_ref(),
        next_document,
        layer,
        known_fresh,
    )
    .await?;
    let replayed = replay_write.measurement().map_err(measurement_error)?;
    if replayed != cumulative_vector {
        return Err(corruption(
            "vector planning and replay produced different final write sets",
        ));
    }
    Ok(EntityPlanOutcome::Admitted {
        vector_writes: cumulative_vector,
        lifecycle_operations,
        lifecycle_bytes,
        next_partition,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "one deterministic HNSW replay binds both partition endpoints and exact build authority"
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
        index.stage_delete(write, entity_id.get()).await?;
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
            .stage_known_fresh_at_layer(
                write,
                entity_id.get(),
                document.vector(),
                layer,
                handle.fresh_insert_proof(),
            )
            .await
    } else {
        index
            .stage_upsert_at_layer(write, entity_id.get(), document.vector(), layer)
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
                crate::index_v2::repository::load_vector_partition_mapping(
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
                physical_index_id: crate::index_v2::repository::peek_vector_physical_id(
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
        IndexV2RecordKind::BuildDelta,
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
        .map(|cursor| Key::parse_from_slice(scope, cursor.as_bytes()))
        .transpose()?
        .and_then(|key| match key {
            Key::Data {
                kind: DataKeyKind::IndexV2(key),
                ..
            } => Some(key.record_kind()),
            Key::Data { .. } | Key::Global { .. } => None,
        });
    if !matches!(cursor_kind, Some(IndexV2RecordKind::VectorPartitionMapping)) {
        let prefix = generation_prefix(
            scope,
            IndexV2RecordKind::AppliedState,
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
            accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 1, output_bytes)?;
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
                index
                    .stage_create(&planning_write, expected.clone())
                    .await?;
                let writes = planning_write.measurement().map_err(measurement_error)?;
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
                let replay_write = MeasuredVectorTransaction::new(transaction);
                index.stage_create(&replay_write, expected).await?;
                if replay_write.measurement().map_err(measurement_error)? != writes {
                    return Err(corruption(
                        "vector metadata planning and replay produced different write sets",
                    ));
                }
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
        IndexV2RecordKind::VectorPartitionMapping,
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
        accounting.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0)?;
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
            let value = encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                AppliedEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind,
                    entity_id,
                    state: AppliedFamilyState::Vector(Some(partition.clone())),
                },
            ));
            (1_u64, applied_key.len().saturating_add(value.len()) as u64)
        }
        AppliedStateTransition::Delete => (1, applied_key.len() as u64),
        AppliedStateTransition::Absent => (0, 0),
    };
    if delete_delta {
        let delta_key = scoped_index_key(
            scope,
            IndexV2Key::BuildDelta(IndexEntityStateKey {
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
            IndexV2Key::VectorPartitionMapping(
                crate::encoding::v1::keys::index_v2::VectorPartitionMappingKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    partition: tenant.fingerprint(),
                },
            ),
        );
        let mapping_value = encode_work_value(&IndexV2WorkValue::VectorPartitionMapping(
            crate::index_v2::work::VectorPartitionMappingValue {
                index_id: operation.index_id(),
                generation: operation.generation(),
                partition: tenant,
                physical_index_id: resolution.physical_index_id,
            },
        ));
        let watermark_key = Key::Global {
            kind: GlobalKeyKind::IndexV2(GlobalIndexV2Key::VectorPhysicalIdWatermark),
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
            encode_work_value(&IndexV2WorkValue::AppliedEntityState(
                AppliedEntityStateValue {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity_kind,
                    entity_id,
                    state: AppliedFamilyState::Vector(Some(partition)),
                },
            )),
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
        IndexV2Key::AppliedState(IndexEntityStateKey {
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
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::BuildDelta(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption("build-delta prefix yielded another key kind"));
    };
    let IndexV2WorkValue::CoalescedBuildDelta(value) = decode_work_value(value)? else {
        return Err(corruption("build-delta key contains another value kind"));
    };
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
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::AppliedState(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption("applied-state prefix yielded another key kind"));
    };
    let IndexV2WorkValue::AppliedEntityState(value) = decode_work_value(value)? else {
        return Err(corruption("applied-state key contains another value kind"));
    };
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
) -> Result<crate::index_v2::work::VectorPartitionMappingValue> {
    let Key::Data {
        kind: DataKeyKind::IndexV2(IndexV2Key::VectorPartitionMapping(key)),
        ..
    } = Key::parse_from_slice(scope, key)?
    else {
        return Err(corruption("vector mapping prefix yielded another key kind"));
    };
    let IndexV2WorkValue::VectorPartitionMapping(value) = decode_work_value(value)? else {
        return Err(corruption("vector mapping key contains another value kind"));
    };
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
        IndexElementKind::Node => Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(crate::encoding::v1::keys::NodePropertyKey::new(
                entity.id.get(),
            )),
        }
        .to_bytes(),
        IndexElementKind::Edge => Key::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(
                crate::encoding::v1::keys::EdgePropertyByIdKey::new(entity.id.get()),
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
    let key = scoped_index_key(
        scope,
        IndexV2Key::index_record(operation.identity().clone()),
    );
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
    kind: IndexV2RecordKind,
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
    Key::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

fn source_entity(
    scope: DataScope,
    expected: IndexElementKind,
    key: &[u8],
) -> Result<Option<IndexEntityId>> {
    let parsed = Key::parse_from_slice(scope, key)?;
    Ok(match (expected, parsed) {
        (
            IndexElementKind::Node,
            Key::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.node_id())),
        (
            IndexElementKind::Edge,
            Key::Data {
                kind: DataKeyKind::EdgePropertyById(key),
                ..
            },
        ) => Some(IndexEntityId::new(key.edge_id())),
        (IndexElementKind::Edge, Key::Data { .. }) => None,
        (IndexElementKind::Node, Key::Data { .. }) | (_, Key::Global { .. }) => {
            return Err(corruption("vector source prefix yielded another key kind"));
        }
    })
}

fn generation_prefix(
    scope: DataScope,
    kind: IndexV2RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Bytes {
    Key::data_prefix(
        scope,
        IndexV2Key::generation_prefix(kind, index_id, generation),
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

fn scoped_index_key(scope: DataScope, key: IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
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
) -> Result<IndexOperationStepResult> {
    match outcome {
        EntityPlanOutcome::Blocked(blocker) => Ok(IndexOperationStepResult::Blocked(blocker)),
        EntityPlanOutcome::BatchFull => Ok(progressed_build(VectorBuildStage::Scan(
            SourceScanProgress {
                inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                cursor,
                counters: accounting.finish()?,
            },
        ))),
        EntityPlanOutcome::Admitted { .. } => Err(corruption(format!(
            "admitted vector entity {entity_kind:?}/{} escaped replay",
            entity_id.get()
        ))),
    }
}

enum EntityPlanOutcome {
    Admitted {
        vector_writes: VectorWriteMeasurement,
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
    lifecycle_operations: u64,
    lifecycle_bytes: u64,
}

impl VectorBatchAccounting {
    fn new(counters: OperationCounters, limits: SearchIndexBatchLimits) -> Self {
        Self {
            counters,
            limits,
            entities: 0,
            input_bytes: 0,
            vector_writes: VectorWriteMeasurement::zero(),
            lifecycle_operations: 0,
            lifecycle_bytes: 0,
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

    fn admit(
        &mut self,
        input_bytes: u64,
        cumulative_vector: VectorWriteMeasurement,
        lifecycle_operations: u64,
        lifecycle_bytes: u64,
    ) -> Result<()> {
        self.entities += 1;
        self.input_bytes = checked_add(self.input_bytes, input_bytes, "batch input bytes")?;
        self.vector_writes = cumulative_vector;
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

fn operation_error(error: crate::index_v2::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, VectorIndexDefinition};
    use crate::encoding::property::property_value::PropertyValue;
    use crate::encoding::v1::keys::NodePropertyKey;
    use crate::encoding::v1::property::encode_properties;
    use crate::index_v2::lifecycle::{
        create_index_operation, drop_index_operation, InitialBuildProgress,
    };
    use crate::index_v2::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, ClaimPermission,
        CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_v2::repository::{bootstrap_writer, peek_vector_physical_id};
    use crate::index_v2::vector::{load_mutation_set, maintain_entity, VectorEntityMutation};
    use crate::index_v2::{
        ActiveIndexHandle, ClaimSequence, IndexDdlReceipt, IndexOperationId, IndexScopeGates,
        IndexStateV2, WriterEpoch,
    };
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

    fn driver_with_reader_leases(
        reader_leases: Arc<dyn IndexLeaseCoordinator>,
    ) -> VectorIndexDriver {
        VectorIndexDriver::with_reader_leases(
            Arc::new(IndexScopeGates::default()),
            Arc::new(VectorCacheRegistry::default()),
            Arc::new(SimHasherRegistry::default()),
            Some(reader_leases),
        )
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
        Key::Data {
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
        let key = scoped_index_key(scope, IndexV2Key::index_record(definition.identity()));
        let value = db
            .get(key)
            .await
            .expect("canonical vector index is readable")
            .expect("canonical vector index exists");
        decode_index_record(&value).expect("canonical vector index decodes")
    }

    async fn mapping_values(
        db: &Db,
        scope: DataScope,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Vec<crate::index_v2::work::VectorPartitionMappingValue> {
        let prefix = generation_prefix(
            scope,
            IndexV2RecordKind::VectorPartitionMapping,
            index_id,
            generation,
        );
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("vector mappings are readable");
        let mut values = Vec::new();
        while let Some(row) = rows.next().await.expect("vector mapping row is readable") {
            let IndexV2WorkValue::VectorPartitionMapping(value) =
                decode_work_value(&row.value).expect("vector mapping value decodes")
            else {
                panic!("vector mapping prefix contains only mapping values");
            };
            values.push(value);
        }
        values
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
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = Arc::new(
            crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
            ),
        );
        assert_eq!(
            drive_one(
                &db,
                &driver_with_reader_leases(Arc::clone(&reader_leases)),
                build_id,
                &mut claim_sequence,
                one_entity,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let restarted = driver_with_reader_leases(Arc::clone(&reader_leases));
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
        let cleanup_restart = driver_with_reader_leases(reader_leases);
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
            IndexV2RecordKind::BuildDelta,
            IndexV2RecordKind::AppliedState,
            IndexV2RecordKind::VectorPartitionMapping,
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
}
