//! Generation-qualified secondary-index build, serving, mutation, and cleanup.
//!
//! [`SecondaryIndexDriver`] advances one bounded outbox checkpoint at a time.
//! Source scans read authoritative graph property rows; concurrent mutations
//! either maintain an `Active` generation or coalesce one entity delta for a
//! hidden `Building` generation in the same graph transaction. Catch-up always
//! re-reads authoritative state, so a delta is a reconciliation marker rather
//! than an optional copy of a property value.
//!
//! Serving scans only canonical kind-`0x05` rows selected by an exact Active
//! handle. The interpreter owns the surrounding request lease and admits each
//! call as one bounded physical batch before these functions touch storage.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use slatedb::{DbReadOps, DbTransaction};

use crate::config::{RangeIndexDirection, SearchIndexBatchLimits};
use crate::encoding::indexes::hash_property_value;
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, Property};
use crate::encoding::v1::keys::index_v2::{
    CanonicalSecondaryValue, IndexEntity, IndexEntityStateKey, IndexV2Key, IndexV2RecordKind,
    SecondaryEntryKey, SecondaryEntryLane,
};
#[cfg(test)]
use crate::encoding::v1::keys::metadata::MetadataKey;
use crate::encoding::v1::keys::tenant::DataScope;
#[cfg(test)]
use crate::encoding::v1::keys::GlobalKeyKind;
use crate::encoding::v1::keys::{
    DataKeyKind, EdgePropertyByIdKey, Key, KeyPrefix, NodePropertyKey,
};
#[cfg(test)]
use crate::encoding::v1::values::id_allocation::IdAllocationWatermarkValue;
use crate::encoding::v1::values::index_v2::{
    decode_index_record, decode_work_value, encode_work_value, IndexV2WorkValue,
};
use crate::error::{HelixDbError, Result};
use crate::index_v2::outbox::{
    IndexOperationDriver, IndexOperationStepPermit, IndexOperationStepResult,
    PreparedIndexOperationStep,
};
use crate::index_v2::reader_lease::IndexLeaseCoordinator;
use crate::index_v2::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaValue, SecondaryEntryValue,
};
use crate::index_v2::{
    ActiveIndexHandle, BuildOperationOutcome, DrainProgress, IndexCursor, IndexElementKind,
    IndexEntityId, IndexGenerationId, IndexId, IndexOperationBlocker, IndexOperationFamily,
    IndexOperationOutcome, IndexOperationProgress, IndexOperationRecord, IndexRecordV2,
    IndexStateV2, NoCursorProgress, OperationCounters, PrefixScanProgress, SecondaryBuildProgress,
    SecondaryBuildStage, SecondaryCleanupProgress, SourceScanProgress,
    ValidatedDynamicIndexDefinition, ValidatedSecondaryIndexDefinition,
};

use super::IndexScopeGates;

/// Family driver sharing the scope gate and trusted reader coordinator.
pub(crate) struct SecondaryIndexDriver {
    scope_gates: Arc<IndexScopeGates>,
    reader_leases: Option<Arc<dyn IndexLeaseCoordinator>>,
}

impl core::fmt::Debug for SecondaryIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("SecondaryIndexDriver")
            .field("reader_coordination", &self.reader_leases.is_some())
            .finish()
    }
}

impl SecondaryIndexDriver {
    /// Installs secondary lifecycle work against mutation and reader authorities.
    pub(crate) fn with_reader_leases(
        scope_gates: Arc<IndexScopeGates>,
        reader_leases: Option<Arc<dyn IndexLeaseCoordinator>>,
    ) -> Self {
        Self {
            scope_gates,
            reader_leases,
        }
    }

    /// Creates an isolated process-local reader authority for family unit tests.
    #[cfg(test)]
    fn new(scope_gates: Arc<IndexScopeGates>) -> Self {
        Self::with_reader_leases(
            scope_gates,
            Some(Arc::new(
                crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                    crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
                ),
            )),
        )
    }
}

/// One generation and its only legal ordinary-mutation behavior.
#[derive(Debug, Clone)]
struct SecondaryMutationTarget {
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: ValidatedSecondaryIndexDefinition,
    mode: SecondaryMutationMode,
}

/// Closed maintenance choice derived from canonical lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecondaryMutationMode {
    MaintainActive,
    RecordBuildDelta,
}

/// Transaction-local secondary generations loaded from canonical records.
#[derive(Debug, Clone, Default)]
pub(crate) struct SecondaryMutationSet {
    targets: Vec<SecondaryMutationTarget>,
}

impl SecondaryMutationSet {
    /// Returns an empty set for focused configured-index tests.
    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self {
            targets: Vec::new(),
        }
    }
}

/// Loads every secondary generation whose state requires mutation work.
///
/// The scan belongs to the caller's serializable graph transaction. Canonical
/// rows read here therefore conflict with a concurrent activate/drop revision,
/// and `Aborting`/`Dropping` generations cannot accidentally receive new work.
pub(crate) async fn load_mutation_set(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<SecondaryMutationSet> {
    let logical_prefix = IndexV2Key::logical_prefix(IndexV2RecordKind::IndexRecord);
    let physical_prefix = Key::data_prefix(scope, logical_prefix);
    let mut rows = transaction.scan_prefix(&physical_prefix, ..).await?;
    let mut targets = Vec::new();
    while let Some(row) = rows.next().await? {
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::IndexRecord(key)),
            ..
        } = Key::parse_from_slice(scope, &row.key)?
        else {
            return Err(corruption(
                "secondary mutation catalog prefix yielded another key kind",
            ));
        };
        let record = decode_index_record(&row.value)?;
        if key.identity != *record.identity() {
            return Err(corruption(
                "secondary mutation catalog key/value identity mismatch",
            ));
        }
        let definition = match record.definition() {
            ValidatedDynamicIndexDefinition::Secondary(definition) => definition,
            ValidatedDynamicIndexDefinition::Vector(_)
            | ValidatedDynamicIndexDefinition::Text(_) => {
                continue;
            }
        };
        let mode = match record.state() {
            IndexStateV2::Building { .. } => SecondaryMutationMode::RecordBuildDelta,
            IndexStateV2::Active { .. } => SecondaryMutationMode::MaintainActive,
            IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. }
            | IndexStateV2::Dropped { .. } => continue,
        };
        targets.push(SecondaryMutationTarget {
            index_id: record.index_id(),
            generation: record.state().generation(),
            definition: definition.clone(),
            mode,
        });
    }
    Ok(SecondaryMutationSet { targets })
}

/// Maintains every V2 secondary generation affected by one graph entity.
///
/// `before` and `after` are complete authoritative property sets. Passing both
/// makes label moves, property deletion, and entity deletion the same closed
/// operation instead of separate optional flags.
pub(crate) async fn maintain_entity(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &SecondaryMutationSet,
    entity_kind: IndexElementKind,
    entity_id: u64,
    before: &[Property],
    after: &[Property],
) -> Result<()> {
    let entity_id = IndexEntityId::new(entity_id);
    for target in mutations
        .targets
        .iter()
        .filter(|target| target.definition.element_kind() == entity_kind)
    {
        let old_value = canonical_value(&target.definition, before, entity_id)
            .map_err(|error| mutation_value_error(&target.definition, entity_id, error))?;
        let new_value = canonical_value(&target.definition, after, entity_id)
            .map_err(|error| mutation_value_error(&target.definition, entity_id, error))?;
        if old_value == new_value {
            continue;
        }
        match target.mode {
            SecondaryMutationMode::MaintainActive => {
                apply_active_change(transaction, scope, target, entity_id, old_value, new_value)
                    .await?;
            }
            SecondaryMutationMode::RecordBuildDelta => {
                let entity = IndexEntity {
                    kind: entity_kind,
                    id: entity_id,
                };
                let key = scoped_index_key(
                    scope,
                    IndexV2Key::BuildDelta(IndexEntityStateKey {
                        index_id: target.index_id,
                        generation: target.generation,
                        entity,
                    }),
                );
                let value = IndexV2WorkValue::CoalescedBuildDelta(CoalescedBuildDeltaValue {
                    index_id: target.index_id,
                    generation: target.generation,
                    entity_kind,
                    entity_id,
                });
                transaction.put(key, encode_work_value(&value))?;
            }
        }
    }
    Ok(())
}

/// Captures the stable inclusive source key stored in a new secondary build.
///
/// ID allocators lease ranges, so the durable exclusive watermark may be ahead
/// of the last materialized entity. That is safe: the builder scans only rows
/// present in its snapshot, while same-transaction deltas cover later writes
/// at or below the captured ceiling.
#[cfg(test)]
pub(crate) async fn capture_source_upper_bound(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
) -> Result<IndexCursor> {
    super::lifecycle::capture_source_upper_bound(reader, scope, definition.element_kind()).await
}

#[async_trait]
impl IndexOperationDriver for SecondaryIndexDriver {
    fn family(&self) -> IndexOperationFamily {
        IndexOperationFamily::Secondary
    }

    async fn acquire_step_permit(
        &self,
        scope: DataScope,
        operation: &IndexOperationRecord,
    ) -> Result<Box<dyn IndexOperationStepPermit>> {
        let needs_exclusive = matches!(
            operation.progress(),
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::CatchUp(_)
                    | SecondaryBuildStage::Validate(_)
                    | SecondaryBuildStage::Activate(_)
            )) | IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(_))
                | IndexOperationProgress::SecondaryCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.exclusive_permit(scope).await));
        }
        Ok(Box::new(()))
    }

    async fn prepare_step(
        &self,
        _db: &slatedb::Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        _limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        let permit = self.acquire_step_permit(scope, operation).await?;
        let Some(prepared) = super::reader_lifecycle::prepare_reader_lifecycle_step(
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
        _db: &slatedb::Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepResult> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition() else {
            return Err(corruption("secondary operation loaded another family"));
        };
        match operation.progress() {
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) => {
                step_build(transaction, scope, operation, definition, stage, limits).await
            }
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(progress)) => {
                step_cleanup(
                    transaction,
                    scope,
                    operation,
                    definition,
                    progress,
                    true,
                    limits,
                )
                .await
            }
            IndexOperationProgress::SecondaryCleanup(progress) => {
                step_cleanup(
                    transaction,
                    scope,
                    operation,
                    definition,
                    progress,
                    false,
                    limits,
                )
                .await
            }
            IndexOperationProgress::VectorBuild(_)
            | IndexOperationProgress::TextBuild(_)
            | IndexOperationProgress::VectorCleanup(_)
            | IndexOperationProgress::TextCleanup(_) => Err(corruption(
                "secondary driver received another family progress",
            )),
        }
    }
}

async fn step_build(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    stage: &SecondaryBuildStage,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    match stage {
        SecondaryBuildStage::Scan(progress) => {
            scan_source(transaction, scope, operation, definition, progress, limits).await
        }
        SecondaryBuildStage::CatchUp(progress) => {
            catch_up(transaction, scope, operation, definition, progress, limits).await
        }
        SecondaryBuildStage::Validate(progress) => {
            validate_and_release_applied(
                transaction,
                scope,
                operation,
                definition,
                progress,
                limits,
            )
            .await
        }
        SecondaryBuildStage::Activate(progress) => {
            if generation_has_rows(
                transaction,
                scope,
                IndexV2RecordKind::BuildDelta,
                operation.index_id(),
                operation.generation(),
            )
            .await?
            {
                return Ok(progressed_build(SecondaryBuildStage::CatchUp(
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
                return Ok(progressed_build(SecondaryBuildStage::Validate(
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

async fn scan_source(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let source_prefix = source_prefix(scope, definition.element_kind());
    let start = cursor_suffix(&source_prefix, progress.cursor.as_ref())?;
    let upper = cursor_suffix(&source_prefix, Some(&progress.inclusive_upper_bound))?
        .ok_or_else(|| corruption("secondary source upper bound is absent"))?;
    match start.as_ref().map(|start| start.cmp(&upper)) {
        Some(std::cmp::Ordering::Greater) => {
            return Err(corruption(
                "secondary source cursor exceeds its inclusive upper bound",
            ));
        }
        Some(std::cmp::Ordering::Equal) => {
            return Ok(progressed_build(SecondaryBuildStage::CatchUp(
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
    let mut accounting = BatchAccounting::new(progress.counters, limits);
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
            accounting.admit_scan(input_bytes, None)?;
            cursor = Some(complete_cursor);
            continue;
        };
        let properties = match decode_properties(&row.value) {
            Ok(properties) => properties,
            Err(_) => {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: definition.element_kind(),
                        entity_id,
                    },
                ));
            }
        };
        let value = match canonical_value(definition, &properties, entity_id) {
            Ok(value) => value,
            Err(_) => {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: definition.element_kind(),
                        entity_id,
                    },
                ));
            }
        };
        let plan = match reconciliation_plan(
            transaction,
            scope,
            operation.index_id(),
            operation.generation(),
            definition,
            entity_id,
            value,
        )
        .await?
        {
            ReconciliationPlan::Writes(plan) => plan,
            ReconciliationPlan::Blocked(blocker) => {
                return Ok(IndexOperationStepResult::Blocked(blocker));
            }
        };
        if !accounting.can_admit_output(&plan) {
            if accounting.is_empty() {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: definition.element_kind(),
                        entity_id,
                        observed: plan.output_bytes,
                        limit: limits.max_output_bytes().get(),
                    },
                ));
            }
            exhausted = false;
            break;
        }
        plan.stage(transaction)?;
        accounting.admit_scan(input_bytes, Some(&plan))?;
        cursor = Some(complete_cursor);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let counters = accounting.finish()?;
    let next = if exhausted {
        SecondaryBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters,
        })
    } else {
        SecondaryBuildStage::Scan(SourceScanProgress {
            inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
            cursor,
            counters,
        })
    };
    Ok(progressed_build(next))
}

async fn catch_up(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let prefix = generation_prefix(
        scope,
        IndexV2RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    );
    let mut rows = transaction.scan_prefix(&prefix, ..).await?;
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    let mut saw_row = false;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        saw_row = true;
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        let (entity, value) = decode_delta(scope, &row.key, &row.value)?;
        if value.index_id != operation.index_id()
            || value.generation != operation.generation()
            || entity.kind != definition.element_kind()
        {
            return Err(corruption("secondary delta ownership mismatch"));
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
        let properties = read_authoritative_properties(transaction, scope, entity).await?;
        let next_value = match properties {
            Some(properties) => match canonical_value(definition, &properties, entity.id) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(IndexOperationStepResult::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                        },
                    ));
                }
            },
            None => None,
        };
        let plan = match reconciliation_plan(
            transaction,
            scope,
            operation.index_id(),
            operation.generation(),
            definition,
            entity.id,
            next_value,
        )
        .await?
        {
            ReconciliationPlan::Writes(mut plan) => {
                plan.delete(row.key.clone());
                plan
            }
            ReconciliationPlan::Blocked(blocker) => {
                return Ok(IndexOperationStepResult::Blocked(blocker));
            }
        };
        if !accounting.can_admit_output(&plan) {
            if accounting.is_empty() {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        observed: plan.output_bytes,
                        limit: limits.max_output_bytes().get(),
                    },
                ));
            }
            break;
        }
        plan.stage(transaction)?;
        accounting.admit_scan(input_bytes, Some(&plan))?;
    }
    let counters = accounting.finish()?;
    if saw_row {
        return Ok(progressed_build(SecondaryBuildStage::CatchUp(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
        )));
    }
    Ok(progressed_build(SecondaryBuildStage::Validate(
        PrefixScanProgress {
            cursor: None,
            counters,
        },
    )))
}

async fn validate_and_release_applied(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
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
        return Ok(progressed_build(SecondaryBuildStage::CatchUp(
            PrefixScanProgress {
                cursor: None,
                counters: progress.counters,
            },
        )));
    }
    let prefix = generation_prefix(
        scope,
        IndexV2RecordKind::AppliedState,
        operation.index_id(),
        operation.generation(),
    );
    let start =
        cursor_suffix(&prefix, progress.cursor.as_ref())?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    let mut cursor = progress.cursor.clone();
    let mut exhausted = true;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        let (entity, applied) = decode_applied(scope, &row.key, &row.value)?;
        let AppliedFamilyState::Secondary(Some(value)) = applied.state else {
            return Err(corruption(
                "secondary validation found non-secondary or empty applied state",
            ));
        };
        if applied.index_id != operation.index_id()
            || applied.generation != operation.generation()
            || entity.kind != definition.element_kind()
        {
            return Err(corruption("secondary applied-state ownership mismatch"));
        }
        if definition.unique() {
            let properties = read_authoritative_properties(transaction, scope, entity)
                .await?
                .ok_or_else(|| corruption("unique secondary owner source row disappeared"))?;
            let authoritative = canonical_value(definition, &properties, entity.id)
                .map_err(|_| corruption("unique secondary owner source is unsupported"))?;
            if authoritative.as_ref() != Some(&value) {
                return Err(corruption(
                    "unique secondary applied state differs from authoritative source",
                ));
            }
            let entry_key = secondary_entry_key(
                scope,
                operation.index_id(),
                operation.generation(),
                definition,
                value.clone(),
                entity.id,
            )?;
            let Some(entry) = transaction.get(&entry_key).await? else {
                return Err(corruption("unique secondary applied state has no entry"));
            };
            let owner = decode_secondary_entry_value(
                operation.index_id(),
                operation.generation(),
                definition_lane(definition),
                &entry,
            )?;
            if owner != entity.id {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::UniquenessViolation {
                        first_entity_id: owner,
                        second_entity_id: entity.id,
                    },
                ));
            }
        }
        let mut plan = EntityWritePlan::default();
        plan.delete(row.key.clone());
        if !accounting.can_admit_input(input_bytes) || !accounting.can_admit_output(&plan) {
            if accounting.is_empty() {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        observed: input_bytes.max(plan.output_bytes),
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
        plan.stage(transaction)?;
        accounting.admit_scan(input_bytes, Some(&plan))?;
        cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    let counters = accounting.finish()?;
    let next = if exhausted {
        SecondaryBuildStage::Activate(NoCursorProgress { counters })
    } else {
        SecondaryBuildStage::Validate(PrefixScanProgress { cursor, counters })
    };
    Ok(progressed_build(next))
}

async fn step_cleanup(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &SecondaryCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let next = match progress {
        SecondaryCleanupProgress::BeginDrain(progress) => {
            return Ok(IndexOperationStepResult::Blocked(
                if progress.drain_epoch.is_some() {
                    IndexOperationBlocker::InvariantViolation
                } else {
                    IndexOperationBlocker::ReaderCoordinationUnavailable
                },
            ));
        }
        SecondaryCleanupProgress::DeleteEntries(progress) => {
            let cleanup = delete_generation_rows(
                transaction,
                scope,
                operation.index_id(),
                operation.generation(),
                progress,
                definition.element_kind(),
                limits,
            )
            .await?;
            let (cursor, counters, exhausted) = match cleanup {
                CleanupBatch::Progress {
                    cursor,
                    counters,
                    exhausted,
                } => (cursor, counters, exhausted),
                CleanupBatch::Blocked(blocker) => {
                    return Ok(IndexOperationStepResult::Blocked(blocker));
                }
            };
            if exhausted {
                SecondaryCleanupProgress::DeleteDeltas(PrefixScanProgress {
                    cursor: None,
                    counters,
                })
            } else {
                SecondaryCleanupProgress::DeleteEntries(PrefixScanProgress { cursor, counters })
            }
        }
        SecondaryCleanupProgress::DeleteDeltas(progress) => {
            let cleanup = delete_delta_and_applied_rows(
                transaction,
                scope,
                operation.index_id(),
                operation.generation(),
                progress.counters,
                limits,
            )
            .await?;
            let (counters, exhausted) = match cleanup {
                CleanupBatch::Progress {
                    counters,
                    exhausted,
                    ..
                } => (counters, exhausted),
                CleanupBatch::Blocked(blocker) => {
                    return Ok(IndexOperationStepResult::Blocked(blocker));
                }
            };
            if !exhausted {
                SecondaryCleanupProgress::DeleteDeltas(PrefixScanProgress {
                    cursor: None,
                    counters,
                })
            } else {
                SecondaryCleanupProgress::FinishDrain(DrainProgress {
                    drain_epoch: None,
                    counters,
                })
            }
        }
        SecondaryCleanupProgress::FinishDrain(progress) => {
            return Ok(IndexOperationStepResult::Blocked(
                if progress.drain_epoch.is_some() {
                    IndexOperationBlocker::InvariantViolation
                } else {
                    IndexOperationBlocker::ReaderCoordinationUnavailable
                },
            ));
        }
        SecondaryCleanupProgress::Finalize(_) => {
            return Ok(IndexOperationStepResult::Completed(if aborting {
                IndexOperationOutcome::Build(BuildOperationOutcome::Aborted)
            } else {
                IndexOperationOutcome::DropSucceeded
            }));
        }
    };
    Ok(if aborting {
        IndexOperationStepResult::Progressed(IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Aborting(next),
        ))
    } else {
        IndexOperationStepResult::Progressed(IndexOperationProgress::SecondaryCleanup(next))
    })
}

async fn delete_generation_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    progress: &PrefixScanProgress,
    entity_kind: IndexElementKind,
    limits: SearchIndexBatchLimits,
) -> Result<CleanupBatch> {
    let prefix = generation_prefix(
        scope,
        IndexV2RecordKind::SecondaryEntry,
        index_id,
        generation,
    );
    let start =
        cursor_suffix(&prefix, progress.cursor.as_ref())?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    let mut cursor = progress.cursor.clone();
    let mut exhausted = true;
    while accounting.can_read_another() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        let mut plan = EntityWritePlan::default();
        plan.delete(row.key.clone());
        if !accounting.can_admit_input(input_bytes) || !accounting.can_admit_output(&plan) {
            if accounting.is_empty() {
                let Key::Data {
                    kind: DataKeyKind::IndexV2(IndexV2Key::SecondaryEntry(key)),
                    ..
                } = Key::parse_from_slice(scope, &row.key)?
                else {
                    return Err(corruption(
                        "secondary cleanup prefix yielded another key kind",
                    ));
                };
                let entity_id =
                    decode_secondary_entry_value(index_id, generation, key.lane, &row.value)?;
                return Ok(CleanupBatch::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind,
                        entity_id,
                        observed: input_bytes.max(plan.output_bytes),
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
        plan.stage(transaction)?;
        accounting.admit_scan(input_bytes, Some(&plan))?;
        cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
    }
    if !accounting.can_read_another() {
        exhausted = false;
    }
    Ok(CleanupBatch::Progress {
        cursor,
        counters: accounting.finish()?,
        exhausted,
    })
}

async fn delete_delta_and_applied_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<CleanupBatch> {
    let mut accounting = BatchAccounting::new(counters, limits);
    let mut exhausted = true;
    for kind in [
        IndexV2RecordKind::BuildDelta,
        IndexV2RecordKind::AppliedState,
    ] {
        let prefix = generation_prefix(scope, kind, index_id, generation);
        let mut rows = transaction.scan_prefix(&prefix, ..).await?;
        while accounting.can_read_another() {
            let Some(row) = rows.next().await? else {
                break;
            };
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            let mut plan = EntityWritePlan::default();
            plan.delete(row.key.clone());
            if !accounting.can_admit_input(input_bytes) || !accounting.can_admit_output(&plan) {
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
                    return Ok(CleanupBatch::Blocked(
                        IndexOperationBlocker::OversizedEntity {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                            observed: input_bytes.max(plan.output_bytes),
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
            plan.stage(transaction)?;
            accounting.admit_scan(input_bytes, Some(&plan))?;
        }
        if !accounting.can_read_another() {
            exhausted = false;
            break;
        }
        if !exhausted {
            break;
        }
    }
    Ok(CleanupBatch::Progress {
        cursor: None,
        counters: accounting.finish()?,
        exhausted,
    })
}

/// Closed result of one bounded cleanup transaction.
enum CleanupBatch {
    Progress {
        cursor: Option<IndexCursor>,
        counters: OperationCounters,
        exhausted: bool,
    },
    Blocked(IndexOperationBlocker),
}

async fn apply_active_change(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &SecondaryMutationTarget,
    entity_id: IndexEntityId,
    old_value: Option<CanonicalSecondaryValue>,
    new_value: Option<CanonicalSecondaryValue>,
) -> Result<()> {
    'delete_old: {
        let Some(old_value) = old_value else {
            break 'delete_old;
        };
        let old_key = secondary_entry_key(
            scope,
            target.index_id,
            target.generation,
            &target.definition,
            old_value,
            entity_id,
        )?;
        if target.definition.unique() {
            'verify_old: {
                let Some(value) = transaction.get(&old_key).await? else {
                    break 'verify_old;
                };
                let owner = decode_secondary_entry_value(
                    target.index_id,
                    target.generation,
                    definition_lane(&target.definition),
                    &value,
                )?;
                if owner != entity_id {
                    return Err(corruption(
                        "active unique secondary row belongs to another entity",
                    ));
                }
            }
        }
        transaction.delete(old_key)?;
    }
    'put_new: {
        let Some(new_value) = new_value else {
            break 'put_new;
        };
        let lane = definition_lane(&target.definition);
        let new_key = secondary_entry_key(
            scope,
            target.index_id,
            target.generation,
            &target.definition,
            new_value,
            entity_id,
        )?;
        if target.definition.unique() {
            'verify_new: {
                let Some(value) = transaction.get(&new_key).await? else {
                    break 'verify_new;
                };
                let owner =
                    decode_secondary_entry_value(target.index_id, target.generation, lane, &value)?;
                if owner != entity_id {
                    return Err(HelixDbError::UniqueConstraintViolation {
                        label: target.definition.label().as_str().to_string(),
                        property: target.definition.property().as_str().to_string(),
                        value: "<hashed secondary value>".to_string(),
                        existing_node_id: owner.get(),
                        attempted_node_id: entity_id.get(),
                    });
                }
            }
        }
        let value = IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
            index_id: target.index_id,
            generation: target.generation,
            lane,
            entity_id,
        });
        transaction.put(new_key, encode_work_value(&value))?;
    }
    Ok(())
}

async fn reconciliation_plan(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    next_value: Option<CanonicalSecondaryValue>,
) -> Result<ReconciliationPlan> {
    let entity = IndexEntity {
        kind: definition.element_kind(),
        id: entity_id,
    };
    let applied_key = scoped_index_key(
        scope,
        IndexV2Key::AppliedState(IndexEntityStateKey {
            index_id,
            generation,
            entity,
        }),
    );
    let previous = transaction
        .get(&applied_key)
        .await?
        .map(|bytes| decode_applied(scope, &applied_key, &bytes))
        .transpose()?
        .map(|(key_entity, applied)| {
            if key_entity != entity
                || applied.index_id != index_id
                || applied.generation != generation
                || applied.entity_kind != entity.kind
                || applied.entity_id != entity.id
            {
                return Err(corruption("secondary applied state key/value mismatch"));
            }
            let AppliedFamilyState::Secondary(value) = applied.state else {
                return Err(corruption(
                    "secondary generation contains another applied family",
                ));
            };
            Ok(value)
        })
        .transpose()?
        .flatten();
    let mut plan = EntityWritePlan::default();
    if previous != next_value {
        'delete_previous: {
            let Some(previous) = previous else {
                break 'delete_previous;
            };
            let key =
                secondary_entry_key(scope, index_id, generation, definition, previous, entity_id)?;
            if definition.unique() {
                'verify_previous: {
                    let Some(value) = transaction.get(&key).await? else {
                        break 'verify_previous;
                    };
                    let owner = decode_secondary_entry_value(
                        index_id,
                        generation,
                        definition_lane(definition),
                        &value,
                    )?;
                    if owner != entity_id {
                        return Ok(ReconciliationPlan::Blocked(
                            IndexOperationBlocker::UniquenessViolation {
                                first_entity_id: owner,
                                second_entity_id: entity_id,
                            },
                        ));
                    }
                }
            }
            plan.delete(key);
        }
        'put_next: {
            let Some(next) = next_value.as_ref() else {
                break 'put_next;
            };
            let lane = definition_lane(definition);
            let key = secondary_entry_key(
                scope,
                index_id,
                generation,
                definition,
                next.clone(),
                entity_id,
            )?;
            if definition.unique() {
                'verify_next: {
                    let Some(value) = transaction.get(&key).await? else {
                        break 'verify_next;
                    };
                    let owner = decode_secondary_entry_value(index_id, generation, lane, &value)?;
                    if owner != entity_id {
                        return Ok(ReconciliationPlan::Blocked(
                            IndexOperationBlocker::UniquenessViolation {
                                first_entity_id: owner,
                                second_entity_id: entity_id,
                            },
                        ));
                    }
                }
            }
            let value = IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
                index_id,
                generation,
                lane,
                entity_id,
            });
            plan.put(key, encode_work_value(&value));
        }
    }
    match next_value {
        Some(next) => {
            let value = IndexV2WorkValue::AppliedEntityState(AppliedEntityStateValue {
                index_id,
                generation,
                entity_kind: entity.kind,
                entity_id,
                state: AppliedFamilyState::Secondary(Some(next)),
            });
            plan.put(applied_key, encode_work_value(&value));
        }
        None => plan.delete(applied_key),
    }
    Ok(ReconciliationPlan::Writes(plan))
}

enum ReconciliationPlan {
    Writes(EntityWritePlan),
    Blocked(IndexOperationBlocker),
}

#[derive(Default)]
struct EntityWritePlan {
    writes: Vec<EntityWrite>,
    output_bytes: u64,
}

impl EntityWritePlan {
    fn put(&mut self, key: Bytes, value: Bytes) {
        self.output_bytes = self
            .output_bytes
            .saturating_add(key.len().saturating_add(value.len()) as u64);
        self.writes.push(EntityWrite::Put { key, value });
    }

    fn delete(&mut self, key: Bytes) {
        self.output_bytes = self.output_bytes.saturating_add(key.len() as u64);
        self.writes.push(EntityWrite::Delete(key));
    }

    fn stage(&self, transaction: &DbTransaction) -> Result<()> {
        for write in &self.writes {
            match write {
                EntityWrite::Put { key, value } => {
                    transaction.put(key, value)?;
                }
                EntityWrite::Delete(key) => transaction.delete(key)?,
            }
        }
        Ok(())
    }
}

enum EntityWrite {
    Put { key: Bytes, value: Bytes },
    Delete(Bytes),
}

struct BatchAccounting {
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
    entities: usize,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

impl BatchAccounting {
    fn new(counters: OperationCounters, limits: SearchIndexBatchLimits) -> Self {
        Self {
            counters,
            limits,
            entities: 0,
            input_bytes: 0,
            output_operations: 0,
            output_bytes: 0,
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

    fn can_admit_output(&self, plan: &EntityWritePlan) -> bool {
        self.output_operations
            .saturating_add(plan.writes.len() as u64)
            <= self.limits.max_output_operations().get()
            && self.output_bytes.saturating_add(plan.output_bytes)
                <= self.limits.max_output_bytes().get()
    }

    fn admit_scan(&mut self, input_bytes: u64, plan: Option<&EntityWritePlan>) -> Result<()> {
        self.entities += 1;
        self.input_bytes = checked_add(self.input_bytes, input_bytes, "batch input bytes")?;
        let Some(plan) = plan else {
            return Ok(());
        };
        self.output_operations = checked_add(
            self.output_operations,
            plan.writes.len() as u64,
            "batch output operations",
        )?;
        self.output_bytes =
            checked_add(self.output_bytes, plan.output_bytes, "batch output bytes")?;
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
                self.output_operations,
                "cumulative output operations",
            )?,
            output_bytes: checked_add(
                self.counters.output_bytes,
                self.output_bytes,
                "cumulative output bytes",
            )?,
        })
    }
}

fn canonical_value(
    definition: &ValidatedSecondaryIndexDefinition,
    properties: &[Property],
    _entity_id: IndexEntityId,
) -> std::result::Result<Option<CanonicalSecondaryValue>, SecondaryValueError> {
    let matches_label = properties.iter().any(|property| {
        property.name == "$label" && property.value.as_str() == Some(definition.label().as_str())
    });
    if !matches_label {
        return Ok(None);
    }
    let Some(property) = properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
    else {
        return Ok(None);
    };
    if !crate::search::property_value_is_secondary_indexable(&property.value) {
        return Err(SecondaryValueError::Unsupported(property_value_type_name(
            &property.value,
        )));
    }
    if definition.unique() {
        match crate::search::unique_node_equality_identity_key(&property.value) {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            Err(value_type) => return Err(SecondaryValueError::Unsupported(value_type)),
        }
    }
    let value = crate::search::property_value_to_index_string(&property.value);
    Ok(Some(match definition {
        ValidatedSecondaryIndexDefinition::NodeEquality { .. }
        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
            CanonicalSecondaryValue::equality(hash_property_value(&value))
        }
        ValidatedSecondaryIndexDefinition::NodeRange { direction, .. }
        | ValidatedSecondaryIndexDefinition::EdgeRange { direction, .. } => {
            let physical_direction = match direction {
                RangeIndexDirection::Asc => {
                    crate::encoding::indexes::range::RangeIndexDirection::Asc
                }
                RangeIndexDirection::Desc => {
                    crate::encoding::indexes::range::RangeIndexDirection::Desc
                }
            };
            CanonicalSecondaryValue::range(physical_direction, &value)
        }
    }))
}

#[derive(Debug, Clone, Copy)]
enum SecondaryValueError {
    Unsupported(&'static str),
}

fn mutation_value_error(
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    error: SecondaryValueError,
) -> HelixDbError {
    let SecondaryValueError::Unsupported(value_type) = error;
    if definition.unique() {
        return HelixDbError::UnsupportedUniqueIndexValueType {
            label: definition.label().as_str().to_string(),
            property: definition.property().as_str().to_string(),
            node_id: entity_id.get(),
            value_type: value_type.to_string(),
        };
    }
    HelixDbError::Query(format!(
        "secondary equality/range indexes do not support {value_type} for property '{}'",
        definition.property()
    ))
}

fn property_value_type_name(value: &PropertyValue) -> &'static str {
    match value {
        PropertyValue::Null => "Null",
        PropertyValue::Bool(_) => "Bool",
        PropertyValue::I64(_) => "I64",
        PropertyValue::DateTime(_) => "DateTime",
        PropertyValue::F64(_) => "F64",
        PropertyValue::F32(_) => "F32",
        PropertyValue::String(_) => "String",
        PropertyValue::Bytes(_) => "Bytes",
        PropertyValue::I64Array(_) => "I64Array",
        PropertyValue::F64Array(_) => "F64Array",
        PropertyValue::F32Array(_) => "F32Array",
        PropertyValue::StringArray(_) => "StringArray",
        PropertyValue::Array(_) => "Array",
        PropertyValue::Object(_) => "Object",
    }
}

/// Reads one exact Active equality generation from canonical kind-`0x05` rows.
///
/// The caller must run this function inside the request lease batch associated
/// with `handle`. Unique entries use one point read; non-unique entries scan
/// only the exact generation, lane, and equality-value hash prefix.
pub(crate) async fn lookup_active_equality_generation(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &str,
) -> Result<roaring::RoaringTreemap> {
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary equality serving received a non-secondary Active handle",
        ));
    };
    if !matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeEquality { .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }
    ) {
        return Err(corruption(
            "secondary equality serving received a range definition",
        ));
    }

    let lane = definition_lane(definition);
    let canonical = CanonicalSecondaryValue::equality(hash_property_value(value));
    if lane.is_unique() {
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            canonical,
            IndexEntityId::initial(),
        )?;
        let Some(bytes) = reader.get(key).await? else {
            return Ok(roaring::RoaringTreemap::new());
        };
        let owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &bytes)?;
        return Ok(roaring::RoaringTreemap::from_iter([owner.get()]));
    }

    let mut prefix = Key::data_prefix(
        handle.scope(),
        IndexV2Key::secondary_lane_prefix(handle.index_id(), handle.generation(), lane),
    )
    .to_vec();
    prefix.put_slice(canonical.as_bytes());
    let prefix = Bytes::from(prefix);
    let mut rows = reader.scan_prefix(&prefix, ..).await?;
    let mut owners = roaring::RoaringTreemap::new();
    while let Some(row) = rows.next().await? {
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::SecondaryEntry(key)),
            ..
        } = Key::parse_from_slice(handle.scope(), &row.key)?
        else {
            return Err(corruption(
                "secondary equality prefix yielded another key kind",
            ));
        };
        if key.index_id != handle.index_id()
            || key.generation != handle.generation()
            || key.lane != lane
            || key.value != canonical
        {
            return Err(corruption(
                "secondary equality entry escaped its exact serving prefix",
            ));
        }
        let Some(key_owner) = key.entity_id else {
            return Err(corruption(
                "non-unique secondary equality entry omitted its key owner",
            ));
        };
        let value_owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if key_owner != value_owner {
            return Err(corruption(
                "secondary equality entry key/value owners disagree",
            ));
        }
        owners.insert(value_owner.get());
    }
    Ok(owners)
}

/// Scans one exact Active range generation in its configured physical order.
///
/// Bounds are narrowed at the storage layer and checked again against decoded
/// canonical values. The second check is required for ascending values because
/// their historical encoding has no terminator before the entity-ID suffix, so
/// one value can be a byte prefix of another.
pub(crate) async fn scan_active_range_generation(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&crate::search::RangeQuery<'_>>,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary range serving received a non-secondary Active handle",
        ));
    };
    if !matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeRange { .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { .. }
    ) {
        return Err(corruption(
            "secondary range serving received an equality definition",
        ));
    }

    let direction = match definition.direction() {
        RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
        RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
    };
    let lane = definition_lane(definition);
    let bounds = match query {
        Some(query) => {
            let Some(bounds) = secondary_range_scan_bounds(direction, query) else {
                return Ok(Vec::new());
            };
            bounds
        }
        None => (Bound::Unbounded, Bound::Unbounded),
    };
    let prefix = Key::data_prefix(
        handle.scope(),
        IndexV2Key::secondary_lane_prefix(handle.index_id(), handle.generation(), lane),
    );
    let mut rows = reader.scan_prefix(&prefix, bounds).await?;
    let mut owners = Vec::new();
    while let Some(row) = rows.next().await? {
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::SecondaryEntry(key)),
            ..
        } = Key::parse_from_slice(handle.scope(), &row.key)?
        else {
            return Err(corruption(
                "secondary range prefix yielded another key kind",
            ));
        };
        if key.index_id != handle.index_id()
            || key.generation != handle.generation()
            || key.lane != lane
        {
            return Err(corruption(
                "secondary range entry escaped its exact serving prefix",
            ));
        }
        let Some(key_owner) = key.entity_id else {
            return Err(corruption("secondary range entry omitted its key owner"));
        };
        let decoded = key.value.decode_range(direction)?;
        if query.is_some_and(|query| !secondary_range_query_matches(query, &decoded)) {
            continue;
        }
        let value_owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if key_owner != value_owner {
            return Err(corruption(
                "secondary range entry key/value owners disagree",
            ));
        }
        owners.push(value_owner.get());
        if limit.is_some_and(|limit| owners.len() >= limit) {
            break;
        }
    }
    Ok(owners)
}

/// Produces suffix bounds for one generation/lane `scan_prefix` call.
fn secondary_range_scan_bounds(
    direction: StorageRangeIndexDirection,
    query: &crate::search::RangeQuery<'_>,
) -> Option<(Bound<Bytes>, Bound<Bytes>)> {
    let value_prefix = |value: &str| {
        Bytes::copy_from_slice(CanonicalSecondaryValue::range(direction, value).as_bytes())
    };
    let inclusive_end = |value: &str| {
        let mut end = value_prefix(value).to_vec();
        end.put_u64(u64::MAX);
        Bytes::from(end)
    };
    let start = |value: &str| Bound::Included(value_prefix(value));
    let end = |value: &str, inclusive: bool| {
        if inclusive {
            Bound::Included(inclusive_end(value))
        } else {
            Bound::Excluded(value_prefix(value))
        }
    };
    let between_is_empty = |min: &str, min_inclusive: bool, max: &str, max_inclusive: bool| {
        min > max || (min == max && (!min_inclusive || !max_inclusive))
    };

    Some(match query {
        crate::search::RangeQuery::Gt(value) => match direction {
            StorageRangeIndexDirection::Asc => (start(value), Bound::Unbounded),
            StorageRangeIndexDirection::Desc => (Bound::Unbounded, end(value, false)),
        },
        crate::search::RangeQuery::Gte(value) => match direction {
            StorageRangeIndexDirection::Asc => (start(value), Bound::Unbounded),
            StorageRangeIndexDirection::Desc => (Bound::Unbounded, end(value, true)),
        },
        crate::search::RangeQuery::Lt(value) => match direction {
            StorageRangeIndexDirection::Asc => (Bound::Unbounded, end(value, false)),
            StorageRangeIndexDirection::Desc => (start(value), Bound::Unbounded),
        },
        crate::search::RangeQuery::Lte(value) => match direction {
            StorageRangeIndexDirection::Asc => (Bound::Unbounded, end(value, true)),
            StorageRangeIndexDirection::Desc => (start(value), Bound::Unbounded),
        },
        crate::search::RangeQuery::Between(min, max) => {
            if between_is_empty(min, true, max, true) {
                return None;
            }
            match direction {
                StorageRangeIndexDirection::Asc => (start(min), end(max, true)),
                StorageRangeIndexDirection::Desc => (start(max), end(min, true)),
            }
        }
        crate::search::RangeQuery::BetweenBounds {
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => {
            if between_is_empty(min, *min_inclusive, max, *max_inclusive) {
                return None;
            }
            match direction {
                StorageRangeIndexDirection::Asc => (start(min), end(max, *max_inclusive)),
                StorageRangeIndexDirection::Desc => (start(max), end(min, *min_inclusive)),
            }
        }
    })
}

/// Applies exact logical bounds after the storage-level range narrowing.
fn secondary_range_query_matches(query: &crate::search::RangeQuery<'_>, value: &str) -> bool {
    match query {
        crate::search::RangeQuery::Gt(lower) => value > *lower,
        crate::search::RangeQuery::Gte(lower) => value >= *lower,
        crate::search::RangeQuery::Lt(upper) => value < *upper,
        crate::search::RangeQuery::Lte(upper) => value <= *upper,
        crate::search::RangeQuery::Between(lower, upper) => value >= *lower && value <= *upper,
        crate::search::RangeQuery::BetweenBounds {
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => {
            let lower_matches = if *min_inclusive {
                value >= *min
            } else {
                value > *min
            };
            let upper_matches = if *max_inclusive {
                value <= *max
            } else {
                value < *max
            };
            lower_matches && upper_matches
        }
    }
}

fn definition_lane(definition: &ValidatedSecondaryIndexDefinition) -> SecondaryEntryLane {
    match definition {
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: false, .. } => {
            SecondaryEntryLane::NodeEquality
        }
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. } => {
            SecondaryEntryLane::NodeUniqueEquality
        }
        ValidatedSecondaryIndexDefinition::NodeRange {
            direction: RangeIndexDirection::Asc,
            ..
        } => SecondaryEntryLane::NodeRangeAscending,
        ValidatedSecondaryIndexDefinition::NodeRange {
            direction: RangeIndexDirection::Desc,
            ..
        } => SecondaryEntryLane::NodeRangeDescending,
        ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => SecondaryEntryLane::EdgeEquality,
        ValidatedSecondaryIndexDefinition::EdgeRange {
            direction: RangeIndexDirection::Asc,
            ..
        } => SecondaryEntryLane::EdgeRangeAscending,
        ValidatedSecondaryIndexDefinition::EdgeRange {
            direction: RangeIndexDirection::Desc,
            ..
        } => SecondaryEntryLane::EdgeRangeDescending,
    }
}

fn secondary_entry_key(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    value: CanonicalSecondaryValue,
    entity_id: IndexEntityId,
) -> Result<Bytes> {
    let lane = definition_lane(definition);
    let key = SecondaryEntryKey::try_new(
        index_id,
        generation,
        lane,
        value,
        (!lane.is_unique()).then_some(entity_id),
    )?;
    Ok(scoped_index_key(scope, IndexV2Key::SecondaryEntry(key)))
}

fn decode_secondary_entry_value(
    index_id: IndexId,
    generation: IndexGenerationId,
    lane: SecondaryEntryLane,
    bytes: &[u8],
) -> Result<IndexEntityId> {
    let IndexV2WorkValue::SecondaryEntry(value) = decode_work_value(bytes)? else {
        return Err(corruption(
            "secondary entry key contains another value kind",
        ));
    };
    if value.index_id != index_id || value.generation != generation || value.lane != lane {
        return Err(corruption("secondary entry key/value ownership mismatch"));
    }
    Ok(value.entity_id)
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

async fn read_authoritative_properties(
    transaction: &DbTransaction,
    scope: DataScope,
    entity: IndexEntity,
) -> Result<Option<Vec<Property>>> {
    let key = match entity.kind {
        IndexElementKind::Node => Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes(),
        IndexElementKind::Edge => Key::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity.id.get())),
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
        return Err(corruption("secondary operation has no canonical index"));
    };
    let record = decode_index_record(&value)?;
    if record.index_id() != operation.index_id()
        || record.identity() != operation.identity()
        || record.revision() != operation.index_record_revision()
        || record.state().generation() != operation.generation()
    {
        return Err(corruption("secondary operation/canonical record mismatch"));
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
            return Err(corruption(
                "secondary source prefix yielded another key kind",
            ));
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
        return Err(corruption(
            "secondary cursor is outside its exact scan prefix",
        ));
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

fn progressed_build(stage: SecondaryBuildStage) -> IndexOperationStepResult {
    IndexOperationStepResult::Progressed(IndexOperationProgress::SecondaryBuild(
        SecondaryBuildProgress::Constructing(stage),
    ))
}

fn checked_add(left: u64, right: u64, name: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| corruption(&format!("secondary {name} overflowed")))
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

fn operation_error(error: crate::index_v2::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{SearchIndexBackfillLimits, SecondaryIndexDefinition};
    use crate::encoding::v1::property::encode_properties;
    use crate::encoding::v1::values::index_v2::encode_index_record;
    use crate::index_v2::lifecycle::{
        create_index_operation, drop_index_operation, InitialBuildProgress,
    };
    use crate::index_v2::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, read_operation,
        ClaimPermission, CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_v2::repository::bootstrap_writer;
    use crate::index_v2::{
        ClaimSequence, IndexDdlReceipt, IndexOperationExecutionState, IndexOperationId,
        IndexOperationKind, IndexRevision, IndexStateTransition, PhysicalGeneration, WriterEpoch,
    };

    const NOW_MILLIS: u64 = 1;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("secondary test database opens");
        bootstrap_writer(&db)
            .await
            .expect("secondary test database bootstraps V2 metadata");
        db
    }

    fn validated(definition: SecondaryIndexDefinition) -> ValidatedDynamicIndexDefinition {
        ValidatedDynamicIndexDefinition::try_from(definition)
            .expect("test secondary definition validates")
    }

    /// Persists an Active record and projects its exact serving handle.
    async fn active_read_handle(
        db: &Db,
        definition: SecondaryIndexDefinition,
    ) -> ActiveIndexHandle {
        let definition = validated(definition);
        let building = IndexRecordV2::building(
            IndexId::initial(),
            definition.clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .expect("secondary read fixture starts building");
        let active = building
            .transition(IndexStateTransition::Activate)
            .expect("secondary read fixture activates");
        db.put(
            scoped_index_key(
                DataScope::LegacyUnscoped,
                IndexV2Key::index_record(definition.identity()),
            ),
            encode_index_record(&active),
        )
        .await
        .expect("secondary read fixture Active record persists");
        ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("secondary read fixture projects an Active handle")
    }

    /// Persists one generation-qualified entry matching the fixture handle.
    async fn put_read_entry(db: &Db, handle: &ActiveIndexHandle, value: &str, entity_id: u64) {
        let definition = handle
            .secondary_definition()
            .expect("secondary read fixture uses a secondary handle");
        let canonical = match definition {
            ValidatedSecondaryIndexDefinition::NodeEquality { .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
                CanonicalSecondaryValue::equality(hash_property_value(value))
            }
            ValidatedSecondaryIndexDefinition::NodeRange { direction, .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { direction, .. } => {
                let direction = match direction {
                    RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
                    RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
                };
                CanonicalSecondaryValue::range(direction, value)
            }
        };
        let entity_id = IndexEntityId::new(entity_id);
        let lane = definition_lane(definition);
        db.put(
            secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                definition,
                canonical,
                entity_id,
            )
            .expect("secondary read fixture key validates"),
            encode_work_value(&IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane,
                entity_id,
            })),
        )
        .await
        .expect("secondary read fixture entry persists");
    }

    #[tokio::test]
    async fn active_equality_serving_covers_non_unique_unique_and_edge_lanes() {
        for (database, definition) in [
            (
                "secondary-read-node-equality",
                SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
            ),
            (
                "secondary-read-edge-equality",
                SecondaryIndexDefinition::edge_equality("FOLLOWS", "value").unwrap(),
            ),
        ] {
            let db = test_db(database).await;
            let handle = active_read_handle(&db, definition).await;
            put_read_entry(&db, &handle, "same", 9).await;
            put_read_entry(&db, &handle, "other", 4).await;
            put_read_entry(&db, &handle, "same", 2).await;

            assert_eq!(
                lookup_active_equality_generation(&db, &handle, "same")
                    .await
                    .expect("managed equality generation scans")
                    .into_iter()
                    .collect::<Vec<_>>(),
                vec![2, 9]
            );
            assert!(lookup_active_equality_generation(&db, &handle, "missing")
                .await
                .expect("missing equality value is empty")
                .is_empty());
            db.close().await.expect("equality read fixture closes");
        }

        let db = test_db("secondary-read-node-unique-equality").await;
        let handle = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_unique_equality("User", "value").unwrap(),
        )
        .await;
        put_read_entry(&db, &handle, "only", 7).await;
        assert_eq!(
            lookup_active_equality_generation(&db, &handle, "only")
                .await
                .expect("managed unique equality point-loads")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![7]
        );
        db.close()
            .await
            .expect("unique equality read fixture closes");
    }

    #[tokio::test]
    async fn active_range_serving_covers_node_edge_ascending_descending_and_prefix_values() {
        for (database, definition, expected_all, expected_gt) in [
            (
                "secondary-read-node-range-asc",
                SecondaryIndexDefinition::node_range("User", "value").unwrap(),
                vec![10, 20, 30],
                vec![20, 30],
            ),
            (
                "secondary-read-node-range-desc",
                SecondaryIndexDefinition::node_range_desc("User", "value").unwrap(),
                vec![30, 20, 10],
                vec![30, 20],
            ),
            (
                "secondary-read-edge-range-asc",
                SecondaryIndexDefinition::edge_range("FOLLOWS", "value").unwrap(),
                vec![10, 20, 30],
                vec![20, 30],
            ),
            (
                "secondary-read-edge-range-desc",
                SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "value").unwrap(),
                vec![30, 20, 10],
                vec![30, 20],
            ),
        ] {
            let db = test_db(database).await;
            let handle = active_read_handle(&db, definition).await;
            put_read_entry(&db, &handle, "a", 10).await;
            put_read_entry(&db, &handle, "aa", 20).await;
            put_read_entry(&db, &handle, "b", 30).await;

            assert_eq!(
                scan_active_range_generation(&db, &handle, None, None)
                    .await
                    .expect("managed all-range scan succeeds"),
                expected_all
            );
            assert_eq!(
                scan_active_range_generation(
                    &db,
                    &handle,
                    Some(&crate::search::RangeQuery::Gt("a")),
                    None,
                )
                .await
                .expect("exclusive prefix lower bound filters exact value"),
                expected_gt
            );
            assert_eq!(
                scan_active_range_generation(
                    &db,
                    &handle,
                    Some(&crate::search::RangeQuery::BetweenBounds {
                        min: "a",
                        min_inclusive: false,
                        max: "b",
                        max_inclusive: false,
                    }),
                    None,
                )
                .await
                .expect("exclusive between scan filters both endpoints"),
                vec![20]
            );
            assert_eq!(
                scan_active_range_generation(&db, &handle, None, Some(1))
                    .await
                    .expect("managed range limit is pushed into iteration"),
                expected_all.into_iter().take(1).collect::<Vec<_>>()
            );
            assert!(scan_active_range_generation(
                &db,
                &handle,
                Some(&crate::search::RangeQuery::BetweenBounds {
                    min: "b",
                    min_inclusive: true,
                    max: "a",
                    max_inclusive: true,
                }),
                None,
            )
            .await
            .expect("reversed between bounds are empty")
            .is_empty());
            db.close().await.expect("range read fixture closes");
        }
    }

    #[tokio::test]
    async fn active_secondary_serving_rejects_key_value_owner_disagreement() {
        let db = test_db("secondary-read-owner-mismatch").await;
        let handle = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        )
        .await;
        put_read_entry(&db, &handle, "same", 4).await;
        let definition = handle.secondary_definition().unwrap();
        let lane = definition_lane(definition);
        db.put(
            secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                definition,
                CanonicalSecondaryValue::equality(hash_property_value("same")),
                IndexEntityId::new(4),
            )
            .unwrap(),
            encode_work_value(&IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane,
                entity_id: IndexEntityId::new(5),
            })),
        )
        .await
        .unwrap();

        assert!(matches!(
            lookup_active_equality_generation(&db, &handle, "same").await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("key/value owners disagree")
        ));
        db.close().await.expect("owner mismatch fixture closes");
    }

    fn user_properties(value: &str) -> Vec<Property> {
        vec![
            Property::string("$label", "User"),
            Property::string("email", value),
        ]
    }

    fn source_key(scope: DataScope, kind: IndexElementKind, entity_id: u64) -> Bytes {
        match kind {
            IndexElementKind::Node => Key::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
            }
            .to_bytes(),
            IndexElementKind::Edge => Key::Data {
                scope,
                kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity_id)),
            }
            .to_bytes(),
        }
    }

    fn source_cursor(scope: DataScope, kind: IndexElementKind, entity_id: u64) -> IndexCursor {
        IndexCursor::try_new(source_key(scope, kind, entity_id))
            .expect("complete typed source key is a valid cursor")
    }

    async fn put_source(
        db: &Db,
        scope: DataScope,
        kind: IndexElementKind,
        entity_id: u64,
        properties: &[Property],
    ) {
        db.put(
            source_key(scope, kind, entity_id),
            encode_properties(properties),
        )
        .await
        .expect("authoritative source row is written");
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
            InitialBuildProgress::secondary(source_cursor(
                scope,
                definition.identity().element_kind(),
                upper_entity_id,
            )),
        )
        .await
        .expect("secondary build is enqueued");
        let IndexDdlReceipt::Accepted {
            operation_id,
            index_id,
            generation,
        } = receipt
        else {
            panic!("new secondary definition must enqueue a build");
        };
        (operation_id, index_id, generation)
    }

    async fn drive_one(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
    ) -> CommittedOperationStep {
        drive_one_with_limits(
            db,
            driver,
            operation_id,
            claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
        )
        .await
    }

    async fn drive_one_with_limits(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
    ) -> CommittedOperationStep {
        drive_one_at(db, driver, operation_id, claim_sequence, limits, NOW_MILLIS).await
    }

    async fn drive_one_at(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        now_unix_millis: u64,
    ) -> CommittedOperationStep {
        let writer_epoch = WriterEpoch::from_bytes([0x5A; 16]).expect("writer epoch is non-nil");
        let observation =
            observe_operation_pointer(db, operation_id, writer_epoch, now_unix_millis)
                .await
                .expect("operation pointer is readable");
        let OperationPointerObservation::Eligible(eligible) = observation else {
            panic!("queued secondary operation must be eligible: {observation:?}");
        };
        let sequence = ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero");
        *claim_sequence = claim_sequence
            .checked_add(1)
            .expect("test claim sequence remains bounded");
        let claimed = claim_operation(
            db,
            &eligible,
            writer_epoch,
            sequence,
            now_unix_millis,
            ClaimPermission::Normal,
        )
        .await
        .expect("secondary claim succeeds")
        .expect("exact queued revision is claimable");
        execute_claimed_step(db, &claimed, driver, limits, now_unix_millis)
            .await
            .expect("secondary step commits")
    }

    async fn drive_to_terminal(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
    ) -> CommittedOperationStep {
        for _ in 0..32 {
            let step = drive_one(db, driver, operation_id, claim_sequence).await;
            if !matches!(step, CommittedOperationStep::Progressed) {
                return step;
            }
        }
        panic!("secondary operation exceeded bounded test checkpoints")
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
            .expect("canonical secondary row is readable")
            .expect("canonical secondary row exists");
        decode_index_record(&value).expect("canonical secondary row decodes")
    }

    async fn generation_rows(
        db: &Db,
        scope: DataScope,
        kind: IndexV2RecordKind,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Vec<(Bytes, Bytes)> {
        let prefix = generation_prefix(scope, kind, index_id, generation);
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("secondary generation prefix is readable");
        let mut collected = Vec::new();
        while let Some(row) = rows.next().await.expect("secondary row is readable") {
            collected.push((row.key, row.value));
        }
        collected
    }

    async fn mutate_source(
        db: &Db,
        scope: DataScope,
        kind: IndexElementKind,
        entity_id: u64,
        before: &[Property],
        after: &[Property],
    ) -> Result<()> {
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mutations = load_mutation_set(&transaction, scope).await?;
        maintain_entity(
            &transaction,
            scope,
            &mutations,
            kind,
            entity_id,
            before,
            after,
        )
        .await?;
        let key = source_key(scope, kind, entity_id);
        if after.is_empty() {
            transaction.delete(key)?;
        } else {
            transaction.put(key, encode_properties(after))?;
        }
        transaction.commit().await?;
        Ok(())
    }

    #[tokio::test]
    async fn builder_and_active_mutations_cover_insert_update_delete_and_label_move() {
        let db = test_db("secondary-builder-active-mutations").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let alice = user_properties("alice@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &alice).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;

        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Active { .. }
        ));
        assert_eq!(
            generation_rows(
                &db,
                scope,
                IndexV2RecordKind::SecondaryEntry,
                index_id,
                generation,
            )
            .await
            .len(),
            1
        );

        let bob = user_properties("bob@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 1, &[], &bob)
            .await
            .expect("active insert maintains its entry");
        let charlie = user_properties("charlie@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 1, &bob, &charlie)
            .await
            .expect("active update moves its entry");
        let admin = vec![
            Property::string("$label", "Admin"),
            Property::string("email", "charlie@example.com"),
        ];
        mutate_source(&db, scope, IndexElementKind::Node, 1, &charlie, &admin)
            .await
            .expect("label move removes the old scoped entry");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &alice, &[])
            .await
            .expect("active delete removes its entry");

        assert!(generation_rows(
            &db,
            scope,
            IndexV2RecordKind::SecondaryEntry,
            index_id,
            generation,
        )
        .await
        .is_empty());
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn drop_persists_exact_fence_and_waits_for_the_registered_active_reader() {
        let db = test_db("secondary-reader-drain-race").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &user_properties("leased@example.com"),
        )
        .await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let coordinator = Arc::new(
            crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
            ),
        );
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = coordinator.clone();
        let driver = SecondaryIndexDriver::with_reader_leases(
            Arc::new(IndexScopeGates::default()),
            Some(reader_leases),
        );
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );

        let generation_key =
            crate::index_v2::reader_lease::LeaseGenerationKey::new(scope, index_id, generation);
        let lease = coordinator
            .acquire(
                generation_key,
                crate::index_v2::reader_lease::LeaseHolderId::new_v4(),
            )
            .await
            .expect("activation registered the generation before its DB commit");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("active secondary drop is enqueued")
        else {
            panic!("active secondary drop creates one cleanup operation");
        };

        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        let fenced = read_operation(&db, scope, drop_id)
            .await
            .expect("fenced cleanup operation is readable")
            .expect("fenced cleanup operation exists");
        assert!(matches!(
            fenced.progress(),
            IndexOperationProgress::SecondaryCleanup(
                SecondaryCleanupProgress::BeginDrain(DrainProgress {
                    drain_epoch: Some(epoch),
                    ..
                })
            ) if *epoch > 0
        ));
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::TransientFailure
        );
        assert!(!generation_rows(
            &db,
            scope,
            IndexV2RecordKind::SecondaryEntry,
            index_id,
            generation,
        )
        .await
        .is_empty());

        coordinator
            .release(&lease)
            .await
            .expect("exact active reader releases");
        assert_eq!(
            drive_one_at(
                &db,
                &driver,
                drop_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                NOW_MILLIS + 2_000,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            coordinator
                .acquire(
                    generation_key,
                    crate::index_v2::reader_lease::LeaseHolderId::new_v4(),
                )
                .await,
            Err(crate::index_v2::reader_lease::IndexLeaseError::GenerationClosed)
        ));
    }

    #[tokio::test]
    async fn activation_without_reader_coordination_blocks_before_active_commit() {
        let db = test_db("secondary-reader-coordination-unavailable").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &user_properties("blocked@example.com"),
        )
        .await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver =
            SecondaryIndexDriver::with_reader_leases(Arc::new(IndexScopeGates::default()), None);
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Blocked
        );
        let operation = read_operation(&db, scope, build_id)
            .await
            .expect("blocked operation is readable")
            .expect("blocked operation exists");
        assert!(matches!(
            operation.execution_state(),
            crate::index_v2::IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::ReaderCoordinationUnavailable
            )
        ));
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Building { .. }
        ));
    }

    #[tokio::test]
    async fn every_node_and_edge_equality_and_range_shape_builds_its_exact_lane() {
        let fixtures = [
            (
                SecondaryIndexDefinition::node_equality("User", "email")
                    .expect("node equality definition"),
                SecondaryEntryLane::NodeEquality,
            ),
            (
                SecondaryIndexDefinition::node_unique_equality("Account", "username")
                    .expect("node unique definition"),
                SecondaryEntryLane::NodeUniqueEquality,
            ),
            (
                SecondaryIndexDefinition::node_range("Person", "age")
                    .expect("node ascending range definition"),
                SecondaryEntryLane::NodeRangeAscending,
            ),
            (
                SecondaryIndexDefinition::node_range_desc("Score", "points")
                    .expect("node descending range definition"),
                SecondaryEntryLane::NodeRangeDescending,
            ),
            (
                SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind")
                    .expect("edge equality definition"),
                SecondaryEntryLane::EdgeEquality,
            ),
            (
                SecondaryIndexDefinition::edge_range("RATED", "weight")
                    .expect("edge ascending range definition"),
                SecondaryEntryLane::EdgeRangeAscending,
            ),
            (
                SecondaryIndexDefinition::edge_range_desc("RANKED", "rank")
                    .expect("edge descending range definition"),
                SecondaryEntryLane::EdgeRangeDescending,
            ),
        ];

        for (ordinal, (definition, expected_lane)) in fixtures.into_iter().enumerate() {
            let db = test_db(&format!("secondary-definition-shape-{ordinal}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(definition);
            let identity = definition.identity();
            let properties = vec![
                Property::string("$label", identity.label().as_str()),
                Property::string(identity.property().as_str(), "ordered-value"),
            ];
            put_source(&db, scope, identity.element_kind(), 0, &properties).await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 0).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            assert_eq!(
                drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                CommittedOperationStep::Completed
            );
            let rows = generation_rows(
                &db,
                scope,
                IndexV2RecordKind::SecondaryEntry,
                index_id,
                generation,
            )
            .await;
            assert_eq!(rows.len(), 1);
            let Key::Data {
                kind: DataKeyKind::IndexV2(IndexV2Key::SecondaryEntry(key)),
                ..
            } = Key::parse_from_slice(scope, &rows[0].0)
                .expect("generation-qualified secondary entry key decodes")
            else {
                panic!("secondary entry prefix contains only secondary entries");
            };
            assert_eq!(key.lane, expected_lane);
            db.close().await.expect("secondary shape database closes");
        }
    }

    #[tokio::test]
    async fn source_scan_commits_no_more_than_the_configured_entity_batch() {
        let db = test_db("secondary-bounded-source-scan").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        for entity_id in 0..3 {
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &user_properties(&format!("user-{entity_id}@example.com")),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let one_entity_limits = SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(32).expect("operation limit is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
        )
        .expect("single-entity test limits are internally consistent");

        assert_eq!(
            drive_one_with_limits(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                one_entity_limits,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id)
            .await
            .expect("bounded operation is readable")
            .expect("bounded operation exists");
        assert!(matches!(
            operation.progress(),
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Scan(SourceScanProgress {
                    cursor: Some(_),
                    counters: OperationCounters { entities: 1, .. },
                    ..
                })
            ))
        ));
        assert_eq!(
            generation_rows(
                &db,
                scope,
                IndexV2RecordKind::SecondaryEntry,
                index_id,
                generation,
            )
            .await
            .len(),
            1
        );

        let mut build_completed = false;
        for _ in 0..16 {
            let step = drive_one_with_limits(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                one_entity_limits,
            )
            .await;
            if step == CommittedOperationStep::Completed {
                build_completed = true;
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
        }
        assert!(
            build_completed,
            "bounded build completes within its stage bound"
        );
        assert_eq!(
            generation_rows(
                &db,
                scope,
                IndexV2RecordKind::SecondaryEntry,
                index_id,
                generation,
            )
            .await
            .len(),
            3
        );

        let receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("bounded cleanup is enqueued");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = receipt
        else {
            panic!("active secondary drop enqueues cleanup");
        };
        assert_eq!(
            drive_one_with_limits(
                &db,
                &driver,
                drop_id,
                &mut claim_sequence,
                one_entity_limits,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with_limits(
                &db,
                &driver,
                drop_id,
                &mut claim_sequence,
                one_entity_limits,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one_with_limits(
                &db,
                &driver,
                drop_id,
                &mut claim_sequence,
                one_entity_limits,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            generation_rows(
                &db,
                scope,
                IndexV2RecordKind::SecondaryEntry,
                index_id,
                generation,
            )
            .await
            .len(),
            2
        );
        let operation = read_operation(&db, scope, drop_id)
            .await
            .expect("bounded cleanup operation is readable")
            .expect("bounded cleanup operation exists");
        assert!(matches!(
            operation.progress(),
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                PrefixScanProgress {
                    cursor: Some(_),
                    ..
                }
            ))
        ));
        for _ in 0..16 {
            let step = drive_one_with_limits(
                &db,
                &driver,
                drop_id,
                &mut claim_sequence,
                one_entity_limits,
            )
            .await;
            if step == CommittedOperationStep::Completed {
                assert!(generation_rows(
                    &db,
                    scope,
                    IndexV2RecordKind::SecondaryEntry,
                    index_id,
                    generation,
                )
                .await
                .is_empty());
                db.close().await.expect("secondary test database closes");
                return;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
        }
        panic!("bounded secondary cleanup exceeded expected checkpoints");
    }

    #[tokio::test]
    async fn cleanup_blocks_instead_of_skipping_a_row_larger_than_one_transaction() {
        let db = test_db("secondary-oversized-cleanup-row").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &user_properties("oversized@example.com"),
        )
        .await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        let receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("oversized cleanup is enqueued");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = receipt
        else {
            panic!("active secondary drop enqueues cleanup");
        };
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            drive_one(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        let tiny_limits = SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
        )
        .expect("tiny limits are internally consistent");
        assert_eq!(
            drive_one_with_limits(&db, &driver, drop_id, &mut claim_sequence, tiny_limits,).await,
            CommittedOperationStep::Blocked
        );
        let operation = read_operation(&db, scope, drop_id)
            .await
            .expect("blocked cleanup is readable")
            .expect("blocked cleanup exists");
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::OversizedEntity {
                entity_kind: IndexElementKind::Node,
                entity_id,
                observed,
                limit: 1,
            }) if entity_id.get() == 0 && *observed > 1
        ));
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn source_upper_bound_uses_the_exclusive_allocator_watermark() {
        let db = test_db("secondary-source-upper-bound").await;
        let scope = DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(7));
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let ValidatedDynamicIndexDefinition::Secondary(definition) = &definition else {
            unreachable!("test definition is secondary");
        };

        assert_eq!(
            capture_source_upper_bound(&db, scope, definition)
                .await
                .expect("fresh-store source ceiling is valid"),
            source_cursor(scope, IndexElementKind::Node, 0)
        );
        db.put(
            Key::Global {
                kind: GlobalKeyKind::Metadata(MetadataKey::next_node_id_key()),
            }
            .to_bytes(),
            Bytes::copy_from_slice(&IdAllocationWatermarkValue::new(8).encode()),
        )
        .await
        .expect("exclusive node watermark is written");
        assert_eq!(
            capture_source_upper_bound(&db, scope, definition)
                .await
                .expect("leased source ceiling is valid"),
            source_cursor(scope, IndexElementKind::Node, 7)
        );

        let edge_definition = validated(
            SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind")
                .expect("edge equality definition"),
        );
        let ValidatedDynamicIndexDefinition::Secondary(edge_definition) = &edge_definition else {
            unreachable!("test definition is secondary");
        };
        db.put(
            Key::Global {
                kind: GlobalKeyKind::Metadata(MetadataKey::next_edge_id_key()),
            }
            .to_bytes(),
            Bytes::copy_from_slice(&IdAllocationWatermarkValue::new(5).encode()),
        )
        .await
        .expect("exclusive edge watermark is written");
        assert_eq!(
            capture_source_upper_bound(&db, scope, edge_definition)
                .await
                .expect("leased edge source ceiling is valid"),
            source_cursor(scope, IndexElementKind::Edge, 4)
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn building_mutation_coalesces_delta_and_catch_up_rereads_authoritative_state() {
        let db = test_db("secondary-build-delta-catch-up").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let before = user_properties("before@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &before).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;

        assert_eq!(
            drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        let after = user_properties("after@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &before, &after)
            .await
            .expect("building mutation stores its delta atomically");
        assert_eq!(
            generation_rows(
                &db,
                scope,
                IndexV2RecordKind::BuildDelta,
                index_id,
                generation,
            )
            .await
            .len(),
            1
        );

        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(generation_rows(
            &db,
            scope,
            IndexV2RecordKind::BuildDelta,
            index_id,
            generation,
        )
        .await
        .is_empty());
        assert!(generation_rows(
            &db,
            scope,
            IndexV2RecordKind::AppliedState,
            index_id,
            generation,
        )
        .await
        .is_empty());
        let ValidatedDynamicIndexDefinition::Secondary(secondary_definition) = &definition else {
            unreachable!("test definition is secondary");
        };
        let expected_key = secondary_entry_key(
            scope,
            index_id,
            generation,
            secondary_definition,
            canonical_value(secondary_definition, &after, IndexEntityId::initial())
                .expect("updated value is supported")
                .expect("updated value is indexed"),
            IndexEntityId::initial(),
        )
        .expect("expected active entry key is valid");
        assert!(db
            .get(expected_key)
            .await
            .expect("updated active entry is readable")
            .is_some());
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn unique_build_and_active_mutation_report_exact_conflicting_entity_ids() {
        let db = test_db("secondary-unique-build-conflict").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let duplicate = user_properties("duplicate@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &duplicate).await;
        put_source(&db, scope, IndexElementKind::Node, 1, &duplicate).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;

        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Blocked
        );
        let operation = read_operation(&db, scope, operation_id)
            .await
            .expect("blocked unique operation is readable")
            .expect("blocked unique operation exists");
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Blocked(
                IndexOperationBlocker::UniquenessViolation {
                    first_entity_id,
                    second_entity_id,
                }
            ) if first_entity_id.get() == 0 && second_entity_id.get() == 1
        ));
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Building { .. }
        ));
        db.close().await.expect("secondary test database closes");

        let db = test_db("secondary-unique-active-conflict").await;
        put_source(&db, scope, IndexElementKind::Node, 0, &duplicate).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            mutate_source(&db, scope, IndexElementKind::Node, 1, &[], &duplicate).await,
            Err(HelixDbError::UniqueConstraintViolation {
                existing_node_id: 0,
                attempted_node_id: 1,
                ..
            })
        ));
        assert!(db
            .get(source_key(scope, IndexElementKind::Node, 1))
            .await
            .expect("conflicting source row lookup succeeds")
            .is_none());
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn abort_and_drop_publish_non_visible_state_before_exact_generation_cleanup() {
        let db = test_db("secondary-abort-drop-cleanup").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let source = user_properties("cleanup@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &source).await;
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );

        let drop_receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("active secondary drop is accepted");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_receipt
        else {
            panic!("active secondary drop must enqueue cleanup");
        };
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropping { .. }
        ));
        assert!(!generation_rows(
            &db,
            scope,
            IndexV2RecordKind::SecondaryEntry,
            index_id,
            generation,
        )
        .await
        .is_empty());
        assert_eq!(
            drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Dropped { .. }
        ));
        assert!(generation_rows(
            &db,
            scope,
            IndexV2RecordKind::SecondaryEntry,
            index_id,
            generation,
        )
        .await
        .is_empty());

        let (build_id, _, next_generation) = create_build(&db, scope, &definition, 0).await;
        assert!(next_generation.get() > generation.get());
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        let abort_receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("building secondary drop begins abort");
        assert_eq!(
            abort_receipt,
            IndexDdlReceipt::ExistingOperation {
                operation_id: build_id,
            }
        );
        assert!(matches!(
            read_index(&db, scope, &definition).await.state(),
            IndexStateV2::Aborting { .. }
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
            IndexV2RecordKind::SecondaryEntry,
            IndexV2RecordKind::BuildDelta,
            IndexV2RecordKind::AppliedState,
        ] {
            assert!(generation_rows(&db, scope, kind, index_id, next_generation)
                .await
                .is_empty());
        }
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn tenant_move_keeps_generation_rows_in_their_exact_scopes() {
        let db = test_db("secondary-tenant-move").await;
        let tenant_a = DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(1));
        let tenant_b = DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(2));
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let source_a = user_properties("a@example.com");
        let source_b = user_properties("b@example.com");
        put_source(&db, tenant_a, IndexElementKind::Node, 0, &source_a).await;
        put_source(&db, tenant_b, IndexElementKind::Node, 0, &source_b).await;
        let (operation_a, index_a, generation_a) =
            create_build(&db, tenant_a, &definition, 0).await;
        let (operation_b, index_b, generation_b) =
            create_build(&db, tenant_b, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_a, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_b, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("tenant move transaction begins");
        let mutations_a = load_mutation_set(&transaction, tenant_a)
            .await
            .expect("tenant A mutation set loads");
        let mutations_b = load_mutation_set(&transaction, tenant_b)
            .await
            .expect("tenant B mutation set loads");
        maintain_entity(
            &transaction,
            tenant_a,
            &mutations_a,
            IndexElementKind::Node,
            0,
            &source_a,
            &[],
        )
        .await
        .expect("tenant A removal is staged");
        maintain_entity(
            &transaction,
            tenant_b,
            &mutations_b,
            IndexElementKind::Node,
            1,
            &[],
            &source_a,
        )
        .await
        .expect("tenant B insertion is staged");
        transaction
            .delete(source_key(tenant_a, IndexElementKind::Node, 0))
            .expect("tenant A source delete is staged");
        transaction
            .put(
                source_key(tenant_b, IndexElementKind::Node, 1),
                encode_properties(&source_a),
            )
            .expect("tenant B source put is staged");
        transaction.commit().await.expect("tenant move commits");

        assert!(generation_rows(
            &db,
            tenant_a,
            IndexV2RecordKind::SecondaryEntry,
            index_a,
            generation_a,
        )
        .await
        .is_empty());
        assert_eq!(
            generation_rows(
                &db,
                tenant_b,
                IndexV2RecordKind::SecondaryEntry,
                index_b,
                generation_b,
            )
            .await
            .len(),
            2
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn every_build_and_drop_stage_resumes_after_database_reopen() {
        let store = Arc::new(InMemory::new());
        let path = "secondary-reopen-every-stage";
        let mut db = Db::builder(path, store.clone())
            .build()
            .await
            .expect("reopen test database opens");
        bootstrap_writer(&db)
            .await
            .expect("reopen test database bootstraps");
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &user_properties("resume@example.com"),
        )
        .await;
        let (build_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = Arc::new(
            crate::index_v2::reader_lease::ProcessLocalIndexLeaseCoordinator::new(
                crate::index_v2::reader_lease::ReaderLeaseTiming::default(),
            ),
        );
        let mut claim_sequence = 1;
        let mut build_stages = BTreeSet::new();
        loop {
            let operation = read_operation(&db, scope, build_id)
                .await
                .expect("build operation is readable")
                .expect("build operation exists");
            let IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) =
                operation.progress()
            else {
                panic!("build operation retains constructing progress");
            };
            build_stages.insert(match stage {
                SecondaryBuildStage::Scan(_) => "scan",
                SecondaryBuildStage::CatchUp(_) => "catch_up",
                SecondaryBuildStage::Validate(_) => "validate",
                SecondaryBuildStage::Activate(_) => "activate",
            });
            let driver = SecondaryIndexDriver::with_reader_leases(
                Arc::new(IndexScopeGates::default()),
                Some(Arc::clone(&reader_leases)),
            );
            let step = drive_one(&db, &driver, build_id, &mut claim_sequence).await;
            if step == CommittedOperationStep::Completed {
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
            db.close().await.expect("checkpoint flushes before reopen");
            db = Db::builder(path, store.clone())
                .build()
                .await
                .expect("database reopens after build checkpoint");
        }
        assert_eq!(
            build_stages,
            BTreeSet::from(["activate", "catch_up", "scan", "validate"])
        );

        let drop_receipt = drop_index_operation(&db, scope, &definition)
            .await
            .expect("reopen test drop is accepted");
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_receipt
        else {
            panic!("active index drop enqueues cleanup");
        };
        let mut cleanup_stages = BTreeSet::new();
        loop {
            let operation = read_operation(&db, scope, drop_id)
                .await
                .expect("drop operation is readable")
                .expect("drop operation exists");
            assert_eq!(operation.kind(), IndexOperationKind::Drop);
            let IndexOperationProgress::SecondaryCleanup(stage) = operation.progress() else {
                panic!("drop operation retains secondary cleanup progress");
            };
            cleanup_stages.insert(match stage {
                SecondaryCleanupProgress::BeginDrain(_) => "begin_drain",
                SecondaryCleanupProgress::DeleteEntries(_) => "delete_entries",
                SecondaryCleanupProgress::DeleteDeltas(_) => "delete_deltas",
                SecondaryCleanupProgress::FinishDrain(_) => "finish_drain",
                SecondaryCleanupProgress::Finalize(_) => "finalize",
            });
            let driver = SecondaryIndexDriver::with_reader_leases(
                Arc::new(IndexScopeGates::default()),
                Some(Arc::clone(&reader_leases)),
            );
            let step = drive_one(&db, &driver, drop_id, &mut claim_sequence).await;
            if step == CommittedOperationStep::Completed {
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
            db.close()
                .await
                .expect("cleanup checkpoint flushes before reopen");
            db = Db::builder(path, store.clone())
                .build()
                .await
                .expect("database reopens after cleanup checkpoint");
        }
        assert_eq!(
            cleanup_stages,
            BTreeSet::from([
                "begin_drain",
                "delete_deltas",
                "delete_entries",
                "finalize",
                "finish_drain",
            ])
        );
        db.close().await.expect("reopen test database closes");
    }
}
