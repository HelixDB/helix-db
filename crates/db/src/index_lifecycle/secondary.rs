//! Generation-qualified secondary-index build, serving, mutation, and cleanup.
//!
//! [`SecondaryIndexDriver`] advances one bounded outbox checkpoint at a time.
//! Source scans read authoritative graph property rows; concurrent mutations
//! either maintain an `Active` generation or coalesce one entity delta for a
//! hidden `Building` generation in the same graph transaction. Catch-up always
//! re-reads authoritative state, so a delta is a reconciliation marker rather
//! than an optional copy of a property value.
//!
//! Serving reads only canonical generation-qualified rows selected by an exact
//! Active handle. The interpreter owns the surrounding request lease and
//! admits each call as one bounded physical batch before these functions touch
//! storage.

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::num::NonZeroU64;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{DbReadOps, DbTransaction};

use crate::config::{IndexLifecycleScanTuning, RangeIndexDirection, SearchIndexBatchLimits};
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
#[cfg(test)]
use crate::encoding::v2::keys::metadata::MetadataKey;
use crate::encoding::v2::keys::scope::DataScope;
#[cfg(test)]
use crate::encoding::v2::keys::GlobalKeyKind;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{
    CanonicalSecondaryValue, IndexEntity, IndexEntityStateKey, RecordKind, ScopedKey,
    SecondaryEntryKey, SecondaryEntryLane, SecondaryEqualityBitmapKey, STORAGE_KEY_MAX_LEN,
};
use crate::encoding::v2::keys::{
    DataKey, DataKeyKind, EdgePropertyByIdKey, KeyPrefix, NodePropertyKey,
};
#[cfg(test)]
use crate::encoding::v2::values::id_allocation::IdAllocationWatermarkValue;
use crate::encoding::v2::values::property::equality_index_value::{
    project_equality_value, EqualityValueProjection,
};
use crate::encoding::v2::values::property::range_index_value::{
    project_range_value, CanonicalRangeValue, RangeValueProjection,
};
use crate::encoding::v2::values::property::view;
use crate::encoding::v2::values::{
    decode_applied_state, decode_build_delta, decode_index_record, decode_secondary_entry,
    encode_applied_state, encode_build_delta, encode_secondary_entry, BitmapMembershipDelta,
    SecondaryEqualityBitmapValue,
};
use crate::error::{HelixDbError, Result, SecondaryIndexValueError};
use crate::index_lifecycle::outbox::{
    IndexOperationDriver, IndexOperationStepExecution, IndexOperationStepPermit,
    IndexOperationStepResult, PreparedIndexOperationStep,
};
use crate::index_lifecycle::repository::ReaderStorageCompatibility;
use crate::index_lifecycle::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaState,
    CoalescedBuildDeltaValue, SecondaryEntryValue,
};
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
use crate::index_lifecycle::IndexStateV2;
use crate::index_lifecycle::{
    ActiveIndexHandle, BuildOperationOutcome, IndexCursor, IndexElementKind, IndexEntityId,
    IndexGenerationId, IndexId, IndexOperationBlocker, IndexOperationFamily, IndexOperationOutcome,
    IndexOperationProgress, IndexOperationRecord, IndexRecordV2, NoCursorProgress,
    OperationCounters, PrefixScanProgress, SecondaryBuildProgress, SecondaryBuildStage,
    SecondaryCleanupProgress, SourceScanProgress, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition,
};

use super::IndexScopeGates;

mod exact;
pub(crate) use exact::lookup_active_unique_equality_batch_admitted;
#[cfg(all(feature = "production-coverage", not(test)))]
pub(crate) use exact::run_production_contracts as run_exact_production_contracts;
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) use exact::scan_active_range_generation_with_membership;
pub(crate) use exact::{
    count_active_range_generation_with_membership, lookup_active_equality_batch_admitted,
    lookup_active_equality_generations_admitted, lookup_active_equality_point_admitted,
    record_equality_graph_read, scan_active_range_generation_ordered, ExactRangeScanProgress,
    OrderedRangeCursor,
};
#[cfg(test)]
pub(crate) use exact::{
    lookup_active_equality_literal_batch, lookup_active_equality_point_literal,
};

#[cfg(any(test, feature = "production-coverage"))]
mod metrics;
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) use metrics::{
    equality_read_metrics, reset_equality_read_metrics, EqualityReadObserver,
    SecondaryEqualityReadMetrics,
};
#[cfg(any(test, feature = "production-coverage"))]
use metrics::{record, ReadKind};

/// Records one logical point read issued by the complete equality-serving path.
#[inline]
pub(crate) fn record_equality_point_read() {
    #[cfg(any(test, feature = "production-coverage"))]
    record(ReadKind::Point);
}

/// Family driver sharing the lifecycle scope gate.
pub(crate) struct SecondaryIndexDriver {
    scope_gates: Arc<IndexScopeGates>,
    scan_tuning: IndexLifecycleScanTuning,
}

impl core::fmt::Debug for SecondaryIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("SecondaryIndexDriver")
            .finish_non_exhaustive()
    }
}

impl SecondaryIndexDriver {
    /// Installs secondary lifecycle work against the mutation authority.
    pub(crate) fn new(scope_gates: Arc<IndexScopeGates>) -> Self {
        Self {
            scope_gates,
            scan_tuning: IndexLifecycleScanTuning::default(),
        }
    }

    /// Applies runtime source-scan prefetching without admitting blocks to cache.
    pub(crate) const fn with_scan_tuning(mut self, scan_tuning: IndexLifecycleScanTuning) -> Self {
        self.scan_tuning = scan_tuning;
        self
    }
}

/// Work prepared before one secondary step's repository transaction opens.
pub(crate) struct PreparedSecondaryOperationStep {
    step: PreparedSecondaryStep,
}

enum PreparedSecondaryStep {
    /// A catch-up backlog beyond [`GATED_BACKLOG_BATCHES`]: exact durable
    /// delta keys selected without adding an SSI range read, reconciled
    /// while writes continue.
    ExactCatchUp { keys: Vec<IndexEntityStateKey> },
    /// A catch-up, validation, or activation step holding the scope's
    /// exclusive permit. Bounded drains committed under that permit left at
    /// most one batch of deltas, so the step's own drain reaches the end of
    /// the prefix; `counters` include their work.
    Gated { counters: OperationCounters },
}

impl PreparedSecondaryOperationStep {
    /// Stages the prepared exact-key batch or gated step.
    pub(crate) async fn stage(
        &self,
        transaction: &impl crate::transaction::Mutation,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepExecution> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition() else {
            return Err(corruption(
                "prepared secondary operation loaded another family",
            ));
        };
        let IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) =
            operation.progress()
        else {
            return Err(corruption(
                "prepared secondary step received another operation stage",
            ));
        };
        let result = match (&self.step, stage) {
            (
                PreparedSecondaryStep::ExactCatchUp { keys },
                SecondaryBuildStage::CatchUp(progress),
            ) => {
                catch_up_exact(
                    transaction,
                    scope,
                    operation,
                    definition,
                    progress,
                    limits,
                    keys,
                )
                .await
            }
            (PreparedSecondaryStep::Gated { counters }, SecondaryBuildStage::CatchUp(progress)) => {
                let progress = PrefixScanProgress {
                    cursor: progress.cursor.clone(),
                    counters: *counters,
                };
                catch_up(transaction, scope, operation, definition, &progress, limits).await
            }
            (
                PreparedSecondaryStep::Gated { counters },
                SecondaryBuildStage::Validate(progress),
            ) => {
                let progress = PrefixScanProgress {
                    cursor: progress.cursor.clone(),
                    counters: *counters,
                };
                validate_and_release_applied(
                    transaction,
                    scope,
                    operation,
                    definition,
                    &progress,
                    limits,
                )
                .await
            }
            (PreparedSecondaryStep::Gated { counters }, SecondaryBuildStage::Activate(_)) => {
                activate(transaction, scope, operation, definition, *counters, limits).await
            }
            (
                PreparedSecondaryStep::ExactCatchUp { .. },
                SecondaryBuildStage::Scan(_)
                | SecondaryBuildStage::Validate(_)
                | SecondaryBuildStage::Activate(_),
            )
            | (PreparedSecondaryStep::Gated { .. }, SecondaryBuildStage::Scan(_)) => Err(
                corruption("prepared secondary step received another build stage"),
            ),
        }?;
        Ok(IndexOperationStepExecution::new(result))
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

/// Transaction-owned foreground secondary work with a terminal prepared state.
#[derive(Debug, Default)]
pub(crate) struct SecondaryMutationRuntime {
    state: SecondaryMutationRuntimeState,
}

#[derive(Debug, Default)]
enum SecondaryMutationRuntimeState {
    #[default]
    Collecting,
    Pending(Vec<PendingSecondaryMutation>),
    Prepared,
}

#[derive(Debug)]
struct PendingSecondaryMutation {
    scope: DataScope,
    target: usize,
    entity: IndexEntity,
    old_value: Option<CanonicalSecondaryValue>,
    new_value: Option<CanonicalSecondaryValue>,
}

impl SecondaryMutationRuntime {
    /// Collects exact routed projections without touching storage.
    pub(crate) fn collect(
        &mut self,
        scope: DataScope,
        mutations: &SecondaryMutationSet,
        routes: &super::mutation_catalog::RoutedMutationTargets<'_>,
        transition: &super::graph_mutation::GraphMutationTransition,
    ) -> Result<()> {
        if matches!(self.state, SecondaryMutationRuntimeState::Prepared) {
            return Err(HelixDbError::InvariantViolation(
                "prepared secondary mutation runtime cannot collect another mutation".to_string(),
            ));
        }
        let entity = transition.entity().index_entity();
        let before = transition.before().map_or(
            &[][..],
            super::graph_mutation::CanonicalPropertyRow::properties,
        );
        let after = transition.after().map_or(
            &[][..],
            super::graph_mutation::CanonicalPropertyRow::properties,
        );
        for ordinal in routes.iter().filter_map(|target| match target {
            super::mutation_catalog::MutationRouteTarget::Secondary(ordinal) => Some(ordinal),
            super::mutation_catalog::MutationRouteTarget::Vector(_)
            | super::mutation_catalog::MutationRouteTarget::TextBuilding(_)
            | super::mutation_catalog::MutationRouteTarget::TextActive(_) => None,
        }) {
            let target = mutations.targets.get(ordinal).ok_or_else(|| {
                corruption("secondary mutation route named a target outside its catalog")
            })?;
            let old_value = match canonical_value(&target.definition, before, entity.id) {
                Ok(value) => value,
                // While a build records deltas, an entity whose value the index
                // cannot hold blocks that build and never had an entry, so changing
                // or deleting it removes nothing. An active index holds no such value.
                Err(_) if matches!(target.mode, SecondaryMutationMode::RecordBuildDelta) => None,
                Err(error) => {
                    return Err(mutation_value_error(&target.definition, entity.id, error))
                }
            };
            let new_value = storable_value(
                scope,
                target.index_id,
                target.generation,
                &target.definition,
                after,
                entity.id,
            )
            .map_err(|error| mutation_value_error(&target.definition, entity.id, error))?;
            if old_value == new_value {
                continue;
            }
            let pending = PendingSecondaryMutation {
                scope,
                target: ordinal,
                entity,
                old_value,
                new_value,
            };
            match &mut self.state {
                SecondaryMutationRuntimeState::Collecting => {
                    self.state = SecondaryMutationRuntimeState::Pending(vec![pending]);
                }
                SecondaryMutationRuntimeState::Pending(pending_mutations) => {
                    pending_mutations.push(pending);
                }
                SecondaryMutationRuntimeState::Prepared => {
                    unreachable!("the prepared state was rejected before routing")
                }
            }
        }
        Ok(())
    }

    /// Observes exclusive keys once, applies changes in input order, and clears the epoch.
    pub(crate) async fn flush(
        &mut self,
        transaction: &impl crate::transaction::Mutation,
        mutations: &SecondaryMutationSet,
    ) -> Result<()> {
        let state = std::mem::take(&mut self.state);
        let pending = match state {
            SecondaryMutationRuntimeState::Collecting => return Ok(()),
            SecondaryMutationRuntimeState::Pending(pending) => pending,
            SecondaryMutationRuntimeState::Prepared => {
                self.state = SecondaryMutationRuntimeState::Prepared;
                return Err(HelixDbError::InvariantViolation(
                    "prepared secondary mutation runtime cannot flush another epoch".to_string(),
                ));
            }
        };

        let mut unique_keys = Vec::new();
        for change in &pending {
            let target = mutations
                .targets
                .get(change.target)
                .ok_or_else(|| corruption("pending secondary mutation lost its catalog target"))?;
            if !matches!(target.mode, SecondaryMutationMode::MaintainActive)
                || !target.definition.unique()
            {
                continue;
            }
            for value in [change.old_value.as_ref(), change.new_value.as_ref()]
                .into_iter()
                .flatten()
            {
                unique_keys.push(secondary_entry_key(
                    change.scope,
                    target.index_id,
                    target.generation,
                    &target.definition,
                    value.clone(),
                    change.entity.id,
                )?);
            }
        }
        unique_keys.sort_unstable();
        unique_keys.dedup();
        let unique_values = if unique_keys.is_empty() {
            Vec::new()
        } else {
            transaction.multi_get(&unique_keys).await?
        };
        let mut unique_overlay = unique_keys
            .into_iter()
            .zip(unique_values)
            .collect::<BTreeMap<_, _>>();
        let mut bitmap_changes = BTreeMap::<Bytes, BTreeMap<u64, bool>>::new();

        for change in pending {
            let target = mutations
                .targets
                .get(change.target)
                .ok_or_else(|| corruption("pending secondary mutation lost its catalog target"))?;
            match target.mode {
                SecondaryMutationMode::RecordBuildDelta => {
                    stage_secondary_build_delta(
                        transaction,
                        change.scope,
                        target,
                        change.entity,
                        change.old_value,
                    )
                    .await?;
                }
                SecondaryMutationMode::MaintainActive => {
                    if definition_uses_equality_bitmap(&target.definition) {
                        if let Some(old_value) = change.old_value {
                            let key = secondary_entry_key(
                                change.scope,
                                target.index_id,
                                target.generation,
                                &target.definition,
                                old_value,
                                change.entity.id,
                            )?;
                            bitmap_changes
                                .entry(key)
                                .or_default()
                                .insert(change.entity.id.get(), false);
                        }
                        if let Some(new_value) = change.new_value {
                            let key = secondary_entry_key(
                                change.scope,
                                target.index_id,
                                target.generation,
                                &target.definition,
                                new_value,
                                change.entity.id,
                            )?;
                            bitmap_changes
                                .entry(key)
                                .or_default()
                                .insert(change.entity.id.get(), true);
                        }
                    } else {
                        apply_active_change_from_overlay(
                            transaction,
                            change.scope,
                            target,
                            change.entity.id,
                            change.old_value,
                            change.new_value,
                            &mut unique_overlay,
                        )?;
                    }
                }
            }
        }
        stage_bitmap_changes(transaction, &bitmap_changes).await?;
        Ok(())
    }

    /// Flushes the final epoch and seals the runtime for commit.
    pub(crate) async fn prepare(
        &mut self,
        transaction: &impl crate::transaction::Mutation,
        mutations: &SecondaryMutationSet,
    ) -> Result<()> {
        self.flush(transaction, mutations).await?;
        self.state = SecondaryMutationRuntimeState::Prepared;
        Ok(())
    }

    /// Consumes only a runtime that completed final preparation.
    pub(crate) fn consume_prepared(self) -> Result<()> {
        match self.state {
            SecondaryMutationRuntimeState::Prepared => Ok(()),
            SecondaryMutationRuntimeState::Collecting
            | SecondaryMutationRuntimeState::Pending(_) => Err(HelixDbError::InvariantViolation(
                "secondary mutation runtime reached commit before prepare".to_string(),
            )),
        }
    }
}

impl SecondaryMutationSet {
    /// Returns an empty set for focused configured-index tests.
    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self {
            targets: Vec::new(),
        }
    }

    /// Counts classified records for the one-scan catalog contract.
    #[cfg(test)]
    pub(super) const fn catalog_entry_count(&self) -> usize {
        self.targets.len()
    }

    /// Classifies one same-snapshot canonical secondary record.
    pub(super) fn include_catalog_entry(
        &mut self,
        entry: super::mutation_catalog::MutationCatalogEntry<'_>,
    ) -> Result<usize> {
        let (record, mode) = match entry {
            super::mutation_catalog::MutationCatalogEntry::Building(record) => {
                (record, SecondaryMutationMode::RecordBuildDelta)
            }
            super::mutation_catalog::MutationCatalogEntry::Active { record, handle } => {
                if !matches!(handle, ActiveIndexHandle::Secondary { .. }) {
                    return Err(corruption(
                        "active secondary record carried another family handle",
                    ));
                }
                (record, SecondaryMutationMode::MaintainActive)
            }
        };
        let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition() else {
            return Err(corruption(
                "secondary mutation classifier received another family",
            ));
        };
        let ordinal = self.targets.len();
        self.targets.push(SecondaryMutationTarget {
            index_id: record.index_id(),
            generation: record.state().generation(),
            definition: definition.clone(),
            mode,
        });
        Ok(ordinal)
    }
}

/// Loads every secondary generation whose state requires mutation work.
///
/// The scan belongs to the caller's serializable graph transaction. Canonical
/// rows read here therefore conflict with a concurrent activate/drop revision,
/// and `Aborting`/`Dropping` generations cannot accidentally receive new work.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn load_mutation_set(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
) -> Result<SecondaryMutationSet> {
    let logical_prefix = ScopedKey::logical_prefix(RecordKind::IndexRecord);
    let physical_prefix = IndexKey::data_prefix(scope, logical_prefix);
    let mut rows = transaction.scan_prefix(&physical_prefix, ..).await?;
    let mut mutations = SecondaryMutationSet::default();
    while let Some(row) = rows.next().await? {
        let IndexKey::Data {
            kind: ScopedKey::IndexRecord(key),
            ..
        } = IndexKey::parse_from_slice(scope, &row.key)?
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
        match record.definition() {
            ValidatedDynamicIndexDefinition::Secondary(_) => {}
            ValidatedDynamicIndexDefinition::Vector(_)
            | ValidatedDynamicIndexDefinition::Text(_) => {
                continue;
            }
        }
        let active_handle = match record.state() {
            IndexStateV2::Building { .. } => None,
            IndexStateV2::Active { .. } => Some(
                ActiveIndexHandle::try_from_record(scope, &record).ok_or_else(|| {
                    corruption("active secondary record did not project a handle")
                })?,
            ),
            IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. }
            | IndexStateV2::Dropped { .. } => continue,
        };
        let entry = match active_handle.as_ref() {
            Some(handle) => super::mutation_catalog::MutationCatalogEntry::Active {
                record: &record,
                handle,
            },
            None => super::mutation_catalog::MutationCatalogEntry::Building(&record),
        };
        let _ = mutations.include_catalog_entry(entry)?;
    }
    Ok(mutations)
}

/// Maintains every V2 secondary generation affected by one graph entity.
///
/// `before` and `after` are complete authoritative property sets. Passing both
/// makes label moves, property deletion, and entity deletion the same closed
/// operation instead of separate optional flags.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn maintain_entity(
    transaction: &impl crate::transaction::Mutation,
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
        let old_value = match canonical_value(&target.definition, before, entity_id) {
            Ok(value) => value,
            // While a build records deltas, an entity whose value the index
            // cannot hold blocks that build and never had an entry, so changing
            // or deleting it removes nothing. An active index holds no such value.
            Err(_) if matches!(target.mode, SecondaryMutationMode::RecordBuildDelta) => None,
            Err(error) => return Err(mutation_value_error(&target.definition, entity_id, error)),
        };
        let new_value = storable_value(
            scope,
            target.index_id,
            target.generation,
            &target.definition,
            after,
            entity_id,
        )
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
                stage_secondary_build_delta(transaction, scope, target, entity, old_value).await?;
            }
        }
    }
    Ok(())
}

/// Preserves the original secondary value across repeated coalesced mutations.
async fn stage_secondary_build_delta(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    target: &SecondaryMutationTarget,
    entity: IndexEntity,
    initial_before: Option<CanonicalSecondaryValue>,
) -> Result<()> {
    let key = scoped_index_key(
        scope,
        ScopedKey::BuildDelta(IndexEntityStateKey {
            index_id: target.index_id,
            generation: target.generation,
            entity,
        }),
    );
    let state = match transaction.get(&key).await? {
        Some(existing) => decode_delta(scope, &key, &existing)?.1.state,
        None => CoalescedBuildDeltaState::SecondaryBefore(initial_before),
    };
    let value = CoalescedBuildDeltaValue {
        index_id: target.index_id,
        generation: target.generation,
        entity_kind: entity.kind,
        entity_id: entity.id,
        state,
    };
    transaction.put(key, encode_build_delta(&value))?;
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
                SecondaryBuildStage::Validate(_) | SecondaryBuildStage::Activate(_)
            )) | IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(_))
                | IndexOperationProgress::SecondaryCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.lifecycle_permit(scope).await));
        }
        Ok(Box::new(()))
    }

    /// Takes the scope's exclusive permit for every step that must end with
    /// the delta prefix empty, and drains the backlog the writes it waited
    /// for left behind before the step's own transaction opens.
    ///
    /// A catch-up backlog beyond [`GATED_BACKLOG_BATCHES`] is instead
    /// reconciled by exact keys while writes continue, so the gate is held
    /// only once the remainder is within that reach.
    async fn prepare_step(
        &self,
        db: &slatedb::Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        let IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) =
            operation.progress()
        else {
            let permit = self.acquire_step_permit(scope, operation).await?;
            return Ok(PreparedIndexOperationStep::driver_owned(
                self.family(),
                permit,
            ));
        };
        // Catch-up and activation only drain, so their own transaction may
        // finish one batch. Validation drains everything first, which leaves
        // its whole batch to the applied rows it releases.
        let (counters, disposition, remainder_batches) = match stage {
            SecondaryBuildStage::Scan(_) => {
                return Ok(PreparedIndexOperationStep::driver_owned(
                    self.family(),
                    Box::new(()),
                ));
            }
            SecondaryBuildStage::CatchUp(progress) => {
                match observe_backlog(db, scope, operation, limits, GATED_BACKLOG_BATCHES).await? {
                    Some(keys) => {
                        return Ok(PreparedIndexOperationStep::secondary(
                            Box::new(()),
                            PreparedSecondaryOperationStep {
                                step: PreparedSecondaryStep::ExactCatchUp { keys },
                            },
                        ));
                    }
                    None => (progress.counters, AppliedStateDisposition::Retain, 1),
                }
            }
            SecondaryBuildStage::Validate(progress) => {
                (progress.counters, AppliedStateDisposition::Release, 0)
            }
            SecondaryBuildStage::Activate(progress) => {
                (progress.counters, AppliedStateDisposition::Release, 1)
            }
        };
        let permit = self.scope_gates.lifecycle_permit(scope).await;
        let counters = drain_backlog_under_gate(
            db,
            scope,
            operation,
            counters,
            limits,
            disposition,
            remainder_batches,
        )
        .await?;
        Ok(PreparedIndexOperationStep::secondary(
            Box::new(permit),
            PreparedSecondaryOperationStep {
                step: PreparedSecondaryStep::Gated { counters },
            },
        ))
    }

    async fn step(
        &self,
        db: &slatedb::Db,
        transaction: &DbTransaction,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<IndexOperationStepExecution> {
        let record = load_operation_index(transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition() else {
            return Err(corruption("secondary operation loaded another family"));
        };
        let result = match operation.progress() {
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) => {
                step_build(
                    db,
                    transaction,
                    scope,
                    operation,
                    definition,
                    stage,
                    limits,
                    self.scan_tuning,
                )
                .await
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
        }?;
        Ok(IndexOperationStepExecution::new(result))
    }
}

/// Catch-up backlogs of up to this many batches drain under the exclusive
/// gate; larger ones are reconciled by exact keys while writes continue.
///
/// Draining a gated backlog stalls the scope's writers for at most this many
/// bounded transactions. An inflow faster than one exact batch per step, such
/// as several builds sharing one write stream, still converges once its
/// backlog falls within this reach.
const GATED_BACKLOG_BATCHES: usize = 8;

/// Observes the committed delta backlog outside any transaction.
///
/// Returns `None` when the whole prefix fits `batches` transactions of
/// `limits`, or else the exact keys of its first batch.
async fn observe_backlog(
    db: &slatedb::Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    limits: SearchIndexBatchLimits,
    batches: usize,
) -> Result<Option<Vec<IndexEntityStateKey>>> {
    let prefix = generation_prefix(
        scope,
        RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    );
    let mut rows = db.scan_prefix(&prefix, ..).await?;
    let entity_limit = limits.max_entities().get();
    let byte_limit = limits.max_input_bytes().get();
    let mut keys = Vec::with_capacity(entity_limit);
    let mut first_batch_full = false;
    let mut batch_input_bytes = 0_u64;
    let mut entities = 0_usize;
    let mut input_bytes = 0_u64;
    loop {
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let row_input_bytes =
            u64::try_from(row.key.len().saturating_add(row.value.len())).unwrap_or(u64::MAX);
        entities = entities.saturating_add(1);
        input_bytes = input_bytes.saturating_add(row_input_bytes);
        if entities > entity_limit.saturating_mul(batches)
            || input_bytes > byte_limit.saturating_mul(u64::try_from(batches).unwrap_or(u64::MAX))
        {
            return Ok(Some(keys));
        }
        if first_batch_full {
            continue;
        }
        if !keys.is_empty() && batch_input_bytes.saturating_add(row_input_bytes) > byte_limit {
            first_batch_full = true;
            continue;
        }
        let (entity, value) = decode_delta(scope, &row.key, &row.value)?;
        if value.index_id != operation.index_id() || value.generation != operation.generation() {
            return Err(corruption(
                "prepared secondary delta escaped its operation generation",
            ));
        }
        keys.push(IndexEntityStateKey {
            index_id: value.index_id,
            generation: value.generation,
            entity,
        });
        batch_input_bytes = batch_input_bytes.saturating_add(row_input_bytes);
        first_batch_full = keys.len() == entity_limit;
    }
}

/// Drains pending deltas in bounded transactions of their own until at most
/// `remainder_batches` batches remain, while the caller holds the scope's
/// exclusive lifecycle permit.
///
/// No writer records a delta while that permit is held, so the backlog only
/// shrinks: the step that follows drains the rest inside its own transaction
/// however many deltas the writes it waited for recorded. Each transaction
/// re-reads the claimed operation, so an abort or a newer claim makes it fail
/// instead of commit. A drain that blocks commits the reconciliations before
/// the blocked entity, which leaves that entity first for the step to block
/// on durably. Returns the counters the step continues from.
async fn drain_backlog_under_gate(
    db: &slatedb::Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    mut counters: OperationCounters,
    limits: SearchIndexBatchLimits,
    disposition: AppliedStateDisposition,
    remainder_batches: usize,
) -> Result<OperationCounters> {
    while observe_backlog(db, scope, operation, limits, remainder_batches)
        .await?
        .is_some()
    {
        let transaction = db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?;
        let claimed =
            super::outbox::load_exact_link(&transaction, scope, operation.operation_id()).await?;
        if !claimed.is_some_and(|(_, claimed, _)| {
            claimed.operation_revision() == operation.operation_revision()
                && claimed.execution_state() == operation.execution_state()
        }) {
            return Err(HelixDbError::TransactionConflict(
                "secondary operation claim changed before a gated drain".to_string(),
            ));
        }
        let record = load_operation_index(&transaction, scope, operation).await?;
        let ValidatedDynamicIndexDefinition::Secondary(definition) = record.definition() else {
            return Err(corruption("gated secondary drain loaded another family"));
        };
        let mut accounting = BatchAccounting::new(counters, limits);
        let drain = drain_build_deltas(
            &transaction,
            scope,
            operation,
            definition,
            &mut accounting,
            disposition,
        )
        .await?;
        counters = accounting.finish()?;
        transaction.commit().await?;
        if matches!(drain, DeltaDrain::Blocked(_)) {
            break;
        }
    }
    Ok(counters)
}

#[allow(
    clippy::too_many_arguments,
    reason = "one build step needs its storage, transaction, exact operation, definition, stage, batch limits, and scan tuning"
)]
async fn step_build(
    db: &slatedb::Db,
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    stage: &SecondaryBuildStage,
    limits: SearchIndexBatchLimits,
    scan_tuning: IndexLifecycleScanTuning,
) -> Result<IndexOperationStepResult> {
    match stage {
        SecondaryBuildStage::Scan(progress) => {
            scan_source(
                db,
                transaction,
                scope,
                operation,
                definition,
                progress,
                limits,
                scan_tuning,
            )
            .await
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
            activate(
                transaction,
                scope,
                operation,
                definition,
                progress.counters,
                limits,
            )
            .await
        }
    }
}

/// Drains the remaining deltas and activates the generation in the same
/// transaction.
///
/// Activation holds the exclusive gate, so a drain that reaches the end of
/// the prefix proves the generation equal to its source at commit. A drain
/// one batch cannot finish stays in activation for the next gated step.
async fn activate(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut accounting = BatchAccounting::new(counters, limits);
    match drain_build_deltas(
        transaction,
        scope,
        operation,
        definition,
        &mut accounting,
        AppliedStateDisposition::Release,
    )
    .await?
    {
        DeltaDrain::Drained => {}
        DeltaDrain::BudgetExhausted => {
            return Ok(progressed_build(SecondaryBuildStage::Activate(
                NoCursorProgress {
                    counters: accounting.finish()?,
                },
            )));
        }
        DeltaDrain::Blocked(blocker) => return Ok(IndexOperationStepResult::Blocked(blocker)),
    }
    // Validation released every applied row before activation, and a
    // release drain writes none.
    if generation_has_rows(
        transaction,
        scope,
        RecordKind::AppliedState,
        operation.index_id(),
        operation.generation(),
    )
    .await?
    {
        return Err(corruption(
            "secondary activation found applied state that validation released",
        ));
    }
    Ok(IndexOperationStepResult::Completed(
        IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
    ))
}

struct SourceScanCandidate {
    cursor: IndexCursor,
    input_bytes: u64,
    kind: SourceScanCandidateKind,
}

enum SourceScanCandidateKind {
    NonEntity,
    Entity {
        entity: IndexEntity,
        next_value: Option<CanonicalSecondaryValue>,
        applied_key: Bytes,
    },
}

struct ObservedSourceScanCandidate {
    cursor: IndexCursor,
    input_bytes: u64,
    kind: ObservedSourceScanCandidateKind,
}

enum ObservedSourceScanCandidateKind {
    NonEntity,
    Entity {
        entity: IndexEntity,
        held: HeldValue,
        next_value: Option<CanonicalSecondaryValue>,
    },
}

/// Reconciles one bounded batch of source rows at or below the build's
/// source upper bound.
///
/// The batch is read from a snapshot outside the step's serializable
/// transaction, so a write inside the scanned range does not abort the step.
/// Every write that commits after the build was created records a delta for
/// its entity (a write whose catalog read predates the creation conflicts
/// with it), and catch-up reconciles that delta from what this step leaves
/// the generation holding (see [`HeldValue`]), so the generation converges
/// whichever snapshot the step read. Rows the build owns (canonical record,
/// applied state, unique entries) stay in the transaction's read set. A
/// blocker is durable, so a step that blocks on a source row first reads
/// that row through the transaction: a write that repairs it before the step
/// commits makes the step retry. A unique entry an earlier batch gave an
/// entity that has since moved off the value is taken over rather than
/// reported as a duplicate (see [`decide_reconciliation`]).
#[allow(
    clippy::too_many_arguments,
    reason = "one source batch needs its storage, transaction, exact operation, definition, progress, limits, and scan tuning"
)]
async fn scan_source(
    db: &slatedb::Db,
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &SourceScanProgress,
    limits: SearchIndexBatchLimits,
    scan_tuning: IndexLifecycleScanTuning,
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
    let scan_options = scan_tuning.scan_options();
    let source = db.begin(slatedb::IsolationLevel::Snapshot).await?;
    let mut rows = source
        .scan_prefix_with_options(
            &source_prefix,
            (start, Bound::Included(upper)),
            &scan_options,
        )
        .await?;
    let mut candidates = Vec::with_capacity(limits.max_entities().get());
    let mut candidate_input_bytes = 0_u64;
    let mut exhausted = true;
    let mut scratch = view::Scratch::new();
    while candidates.len() < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if candidate_input_bytes.saturating_add(input_bytes) > limits.max_input_bytes().get() {
            if candidates.is_empty() {
                let entity_id = source_entity(scope, definition.element_kind(), &row.key)?
                    .unwrap_or(IndexEntityId::initial());
                transaction.get(&row.key).await?;
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
            candidates.push(SourceScanCandidate {
                cursor: complete_cursor,
                input_bytes,
                kind: SourceScanCandidateKind::NonEntity,
            });
            candidate_input_bytes = candidate_input_bytes.saturating_add(input_bytes);
            continue;
        };
        let properties = match decode_indexed_source(definition, &row.value, &mut scratch) {
            Ok(properties) => properties,
            Err(_) => {
                transaction.get(&row.key).await?;
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: definition.element_kind(),
                        entity_id,
                    },
                ));
            }
        };
        let value = match storable_value(
            scope,
            operation.index_id(),
            operation.generation(),
            definition,
            &properties,
            entity_id,
        ) {
            Ok(value) => value,
            Err(_) => {
                transaction.get(&row.key).await?;
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::InvalidSourceData {
                        entity_kind: definition.element_kind(),
                        entity_id,
                    },
                ));
            }
        };
        let entity = IndexEntity {
            kind: definition.element_kind(),
            id: entity_id,
        };
        let applied_key = scoped_index_key(
            scope,
            ScopedKey::AppliedState(IndexEntityStateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity,
            }),
        );
        candidates.push(SourceScanCandidate {
            cursor: complete_cursor,
            input_bytes,
            kind: SourceScanCandidateKind::Entity {
                entity,
                next_value: value,
                applied_key,
            },
        });
        candidate_input_bytes = candidate_input_bytes.saturating_add(input_bytes);
    }
    if candidates.len() == limits.max_entities().get() {
        exhausted = false;
    }

    let applied_keys = candidates
        .iter()
        .filter_map(|candidate| match &candidate.kind {
            SourceScanCandidateKind::NonEntity => None,
            SourceScanCandidateKind::Entity { applied_key, .. } => Some(applied_key.clone()),
        })
        .collect::<Vec<_>>();
    let applied_values = if applied_keys.is_empty() {
        Vec::new()
    } else {
        transaction.multi_get(&applied_keys).await?
    };
    let mut applied_values = applied_values.into_iter();
    let mut observed = Vec::with_capacity(candidates.len());
    let mut unique_keys = Vec::new();
    for candidate in candidates {
        let kind = match candidate.kind {
            SourceScanCandidateKind::NonEntity => ObservedSourceScanCandidateKind::NonEntity,
            SourceScanCandidateKind::Entity {
                entity,
                next_value,
                applied_key,
            } => {
                let applied_value = applied_values
                    .next()
                    .expect("each secondary source entity has one applied-state observation");
                // The scan reaches each entity once: without an applied row
                // the generation holds nothing for it yet.
                let held = match decode_previous_applied(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    entity,
                    &applied_key,
                    applied_value.as_deref(),
                )? {
                    Some(value) => HeldValue::Applied(value),
                    None => HeldValue::Unconfirmed(None),
                };
                if definition.unique() {
                    unique_keys.extend(unique_observation_keys(
                        scope,
                        operation.index_id(),
                        operation.generation(),
                        definition,
                        entity.id,
                        held.candidate(),
                        next_value.as_ref(),
                    )?);
                }
                ObservedSourceScanCandidateKind::Entity {
                    entity,
                    held,
                    next_value,
                }
            }
        };
        observed.push(ObservedSourceScanCandidate {
            cursor: candidate.cursor,
            input_bytes: candidate.input_bytes,
            kind,
        });
    }
    assert!(applied_values.next().is_none());

    unique_keys.sort_unstable();
    unique_keys.dedup();
    let unique_values = if unique_keys.is_empty() {
        Vec::new()
    } else {
        transaction.multi_get(&unique_keys).await?
    };
    let mut unique_entries = unique_keys
        .into_iter()
        .zip(unique_values)
        .collect::<BTreeMap<_, _>>();
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    let mut cursor = progress.cursor.clone();
    for candidate in observed {
        match candidate.kind {
            ObservedSourceScanCandidateKind::NonEntity => {
                accounting.admit_scan(candidate.input_bytes, None)?;
            }
            ObservedSourceScanCandidateKind::Entity {
                entity,
                held,
                next_value,
            } => {
                let plan = match decide_reconciliation(
                    transaction,
                    scope,
                    IndexEntityStateKey {
                        index_id: operation.index_id(),
                        generation: operation.generation(),
                        entity,
                    },
                    definition,
                    &held,
                    next_value.as_ref(),
                    &mut unique_entries,
                    AppliedStateDisposition::Retain,
                )
                .await?
                {
                    EntityReconciliation::Writes { plan, .. } => plan,
                    EntityReconciliation::Blocked(blocker) => {
                        transaction.get(candidate.cursor.as_bytes()).await?;
                        // The duplicate's owner joins the read set too, so a
                        // write that moves either entity before this step
                        // commits makes it retry.
                        let IndexOperationBlocker::UniquenessViolation {
                            first_entity_id, ..
                        } = blocker
                        else {
                            return Ok(IndexOperationStepResult::Blocked(blocker));
                        };
                        transaction
                            .get(authoritative_property_key(
                                scope,
                                IndexEntity {
                                    kind: entity.kind,
                                    id: first_entity_id,
                                },
                            ))
                            .await?;
                        return Ok(IndexOperationStepResult::Blocked(blocker));
                    }
                };
                if !accounting.can_admit_output(&plan) {
                    if accounting.is_empty() {
                        transaction.get(candidate.cursor.as_bytes()).await?;
                        return Ok(IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::OversizedEntity {
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                                observed: plan.output_bytes,
                                limit: limits.max_output_bytes().get(),
                            },
                        ));
                    }
                    exhausted = false;
                    break;
                }
                plan.stage(transaction).await?;
                for write in &plan.writes {
                    match write {
                        EntityWrite::Put { key, value } if unique_entries.contains_key(key) => {
                            unique_entries.insert(key.clone(), Some(value.clone()));
                        }
                        EntityWrite::Delete(key) if unique_entries.contains_key(key) => {
                            unique_entries.insert(key.clone(), None);
                        }
                        EntityWrite::Put { .. }
                        | EntityWrite::Delete(_)
                        | EntityWrite::Bitmap { .. } => {}
                    }
                }
                accounting.admit_scan(candidate.input_bytes, Some(&plan))?;
            }
        }
        cursor = Some(candidate.cursor);
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

/// Drains the delta prefix inside a gated step transaction.
///
/// The caller holds the scope's exclusive permit, so a drain that reaches the
/// end of the prefix proves it empty at commit and the build moves on to
/// validation, while a drain a budget stops stays in catch-up. Every applied
/// value is retained for validation to check.
async fn catch_up(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    let drain = drain_build_deltas(
        transaction,
        scope,
        operation,
        definition,
        &mut accounting,
        AppliedStateDisposition::Retain,
    )
    .await?;
    let progress = PrefixScanProgress {
        cursor: None,
        counters: accounting.finish()?,
    };
    Ok(match drain {
        DeltaDrain::Drained => progressed_build(SecondaryBuildStage::Validate(progress)),
        DeltaDrain::BudgetExhausted => progressed_build(SecondaryBuildStage::CatchUp(progress)),
        DeltaDrain::Blocked(blocker) => IndexOperationStepResult::Blocked(blocker),
    })
}

/// What one delta reconciliation leaves in an entity's applied-state row.
#[derive(Debug, Clone, Copy)]
enum AppliedStateDisposition {
    /// Record the reconciled value: it is the value the generation holds for
    /// the entity, which a later delta or validation compares against.
    Retain,
    /// Delete the row. Legal only while the same transaction deletes the
    /// entity's delta: the generation then holds exactly the source value,
    /// so the next delta a writer records carries that value as its
    /// before-image (see [`HeldValue`]). A writer racing the transaction
    /// conflicts on the delta or source key it reads.
    Release,
}

/// What an entity's lifecycle rows prove its generation holds.
///
/// Scan and catch-up write an applied-state row with every entry they give
/// an entity and delete it with the entity's last entry, so until validation
/// releases the entity its row mirrors the generation exactly. Validation
/// releases a row only together with the entity's pending delta, or while no
/// delta is pending, and the generation then holds the source value, which
/// the next delta records as its before-image. A missing row therefore means
/// the generation holds that before-image or nothing, and the rows alone
/// cannot tell which: a delta recorded before the scan reached the entity
/// carries a value the generation never held.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HeldValue {
    /// The applied-state row: the generation holds exactly this value.
    Applied(CanonicalSecondaryValue),
    /// No applied-state row: the generation holds this value or nothing.
    /// Reconciliation removes it only where an entry provably names the
    /// entity, and writes the next value even when it is equal. `None`
    /// proves the generation holds nothing.
    Unconfirmed(Option<CanonicalSecondaryValue>),
}

impl HeldValue {
    /// The value the generation may hold for the entity.
    fn candidate(&self) -> Option<&CanonicalSecondaryValue> {
        match self {
            Self::Applied(value) => Some(value),
            Self::Unconfirmed(value) => value.as_ref(),
        }
    }
}

/// Closed result of reconciling pending deltas inside one transaction.
#[derive(Debug)]
enum DeltaDrain {
    /// The in-transaction scan reached the end of the delta prefix.
    Drained,
    /// A batch budget stopped the scan before the end of the prefix.
    BudgetExhausted,
    /// One entity cannot be reconciled until an operator intervenes.
    Blocked(IndexOperationBlocker),
}

/// Reconciles and deletes pending deltas until the prefix ends or a budget
/// of `accounting` is reached.
///
/// Each delta reconciles the generation from what the entity's lifecycle
/// rows prove it holds (see [`HeldValue`]) to the current source value. A
/// transaction scan sees only the writes staged before it opened, so a scan
/// opened after this drain observes its deletes.
///
/// A `Blocked` result leaves the reconciliations staged before the blocked
/// entity for the caller to commit. Each of them left its entity reconciled
/// with no pending delta, so a retry from the unchanged checkpoint drains
/// only what remains.
async fn drain_build_deltas(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    accounting: &mut BatchAccounting,
    disposition: AppliedStateDisposition,
) -> Result<DeltaDrain> {
    let prefix = generation_prefix(
        scope,
        RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    );
    let mut rows = transaction.scan_prefix(&prefix, ..).await?;
    loop {
        let Some(row) = rows.next().await? else {
            return Ok(DeltaDrain::Drained);
        };
        if !accounting.can_read_another() {
            return Ok(DeltaDrain::BudgetExhausted);
        }
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
                return Ok(DeltaDrain::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        observed: input_bytes,
                        limit: accounting.limits.max_input_bytes().get(),
                    },
                ));
            }
            return Ok(DeltaDrain::BudgetExhausted);
        }
        let properties =
            read_authoritative_properties(transaction, scope, definition, entity).await?;
        let next_value = match properties {
            Some(properties) => match storable_value(
                scope,
                operation.index_id(),
                operation.generation(),
                definition,
                &properties,
                entity.id,
            ) {
                Ok(value) => value,
                Err(_) => {
                    return Ok(DeltaDrain::Blocked(
                        IndexOperationBlocker::InvalidSourceData {
                            entity_kind: entity.kind,
                            entity_id: entity.id,
                        },
                    ));
                }
            },
            None => None,
        };
        let mut plan = match reconciliation_plan(
            transaction,
            scope,
            IndexEntityStateKey {
                index_id: operation.index_id(),
                generation: operation.generation(),
                entity,
            },
            definition,
            &value.state,
            next_value.as_ref(),
            disposition,
        )
        .await?
        {
            EntityReconciliation::Writes { plan, .. } => plan,
            EntityReconciliation::Blocked(blocker) => return Ok(DeltaDrain::Blocked(blocker)),
        };
        plan.delete(row.key.clone());
        if !accounting.can_admit_output(&plan) {
            if accounting.is_empty() {
                return Ok(DeltaDrain::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: entity.kind,
                        entity_id: entity.id,
                        observed: plan.output_bytes,
                        limit: accounting.limits.max_output_bytes().get(),
                    },
                ));
            }
            return Ok(DeltaDrain::BudgetExhausted);
        }
        plan.stage(transaction).await?;
        accounting.admit_scan(input_bytes, Some(&plan))?;
    }
}

struct ExactCatchUpRow {
    delta_key: Bytes,
    entity: IndexEntity,
    input_bytes: u64,
    held: HeldValue,
    delta_state: CoalescedBuildDeltaState,
    next_value: Option<CanonicalSecondaryValue>,
    unique_entry_keys: Vec<Bytes>,
}

/// Reconciles one prepared batch of a backlog beyond the gated reach.
///
/// The step stays in catch-up: the next preparation reconciles the rest the
/// same way, or takes the exclusive gate once the remainder is within
/// [`GATED_BACKLOG_BATCHES`].
async fn catch_up_exact(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
    prepared_keys: &[IndexEntityStateKey],
) -> Result<IndexOperationStepResult> {
    assert!(
        !prepared_keys.is_empty(),
        "an exact catch-up batch contains at least one durable delta key"
    );
    let delta_keys = prepared_keys
        .iter()
        .map(|key| scoped_index_key(scope, ScopedKey::BuildDelta(*key)))
        .collect::<Vec<_>>();
    let delta_values = transaction.multi_get(&delta_keys).await?;
    let mut decoded = Vec::with_capacity(prepared_keys.len());
    for ((prepared_key, delta_key), delta_value) in
        prepared_keys.iter().zip(delta_keys).zip(delta_values)
    {
        let Some(delta_value) = delta_value else {
            continue;
        };
        let (entity, value) = decode_delta(scope, &delta_key, &delta_value)?;
        if *prepared_key
            != (IndexEntityStateKey {
                index_id: value.index_id,
                generation: value.generation,
                entity,
            })
            || value.index_id != operation.index_id()
            || value.generation != operation.generation()
            || entity.kind != definition.element_kind()
        {
            return Err(corruption("secondary exact delta ownership mismatch"));
        }
        decoded.push((
            delta_key,
            entity,
            u64::try_from(delta_value.len()).unwrap_or(u64::MAX),
            value.state,
        ));
    }

    let property_keys = decoded
        .iter()
        .map(|(_, entity, _, _)| authoritative_property_key(scope, *entity))
        .collect::<Vec<_>>();
    let property_values = transaction.multi_get(&property_keys).await?;
    let applied_keys = decoded
        .iter()
        .map(|(_, entity, _, _)| {
            scoped_index_key(
                scope,
                ScopedKey::AppliedState(IndexEntityStateKey {
                    index_id: operation.index_id(),
                    generation: operation.generation(),
                    entity: *entity,
                }),
            )
        })
        .collect::<Vec<_>>();
    let applied_values = transaction.multi_get(&applied_keys).await?;
    let mut rows = Vec::with_capacity(decoded.len());
    let mut unique_keys = Vec::new();
    let mut scratch = view::Scratch::new();
    for (
        (((delta_key, entity, delta_value_bytes, delta_state), property_key), property_value),
        applied_pair,
    ) in decoded
        .into_iter()
        .zip(property_keys)
        .zip(property_values)
        .zip(applied_keys.into_iter().zip(applied_values))
    {
        let next_value = match property_value.as_ref() {
            Some(properties) => {
                let properties = decode_indexed_source(definition, properties, &mut scratch)?;
                match storable_value(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    definition,
                    &properties,
                    entity.id,
                ) {
                    Ok(value) => value,
                    Err(_) => {
                        return Ok(IndexOperationStepResult::Blocked(
                            IndexOperationBlocker::InvalidSourceData {
                                entity_kind: entity.kind,
                                entity_id: entity.id,
                            },
                        ));
                    }
                }
            }
            None => None,
        };
        let (applied_key, applied_value) = applied_pair;
        let state_key = IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity,
        };
        let held = held_value(
            scope,
            state_key,
            &applied_key,
            applied_value.as_deref(),
            &delta_state,
        )?;
        let unique_entry_keys = if definition.unique() {
            // A claim released earlier in this batch leaves the entity
            // holding at most its before-image, so observe that entry too.
            let before = match &delta_state {
                CoalescedBuildDeltaState::SecondaryBefore(before) => before.as_ref(),
                CoalescedBuildDeltaState::Marker | CoalescedBuildDeltaState::VectorBefore(_) => {
                    None
                }
            };
            let mut keys = unique_observation_keys(
                scope,
                operation.index_id(),
                operation.generation(),
                definition,
                entity.id,
                held.candidate().into_iter().chain(before),
                next_value.as_ref(),
            )?;
            keys.sort_unstable();
            keys.dedup();
            unique_keys.extend(keys.iter().cloned());
            keys
        } else {
            Vec::new()
        };
        let input_bytes = u64::try_from(
            delta_key
                .len()
                .saturating_add(usize::try_from(delta_value_bytes).unwrap_or(usize::MAX))
                .saturating_add(property_key.len())
                .saturating_add(property_value.as_ref().map_or(0, Bytes::len))
                .saturating_add(applied_key.len())
                .saturating_add(applied_value.as_ref().map_or(0, Bytes::len)),
        )
        .unwrap_or(u64::MAX);
        rows.push(ExactCatchUpRow {
            delta_key,
            entity,
            input_bytes,
            held,
            delta_state,
            next_value,
            unique_entry_keys,
        });
    }

    unique_keys.sort_unstable();
    unique_keys.dedup();
    let unique_values = transaction.multi_get(&unique_keys).await?;
    let mut unique_entries = unique_keys
        .into_iter()
        .zip(unique_values)
        .collect::<BTreeMap<_, _>>();
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    // Owners whose stale unique claim an earlier entity of this batch took.
    let mut released = BTreeSet::new();
    for row in rows {
        let unique_input_bytes = row
            .unique_entry_keys
            .iter()
            .map(|key| {
                key.len().saturating_add(
                    unique_entries
                        .get(key)
                        .and_then(Option::as_ref)
                        .map_or(0, Bytes::len),
                )
            })
            .fold(0_usize, usize::saturating_add);
        let input_bytes = row
            .input_bytes
            .saturating_add(u64::try_from(unique_input_bytes).unwrap_or(u64::MAX));
        let state_key = IndexEntityStateKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            entity: row.entity,
        };
        let held = if released.contains(&row.entity.id) {
            held_value(scope, state_key, &[], None, &row.delta_state)?
        } else {
            row.held
        };
        let (mut plan, released_owner) = match decide_reconciliation(
            transaction,
            scope,
            state_key,
            definition,
            &held,
            row.next_value.as_ref(),
            &mut unique_entries,
            AppliedStateDisposition::Retain,
        )
        .await?
        {
            EntityReconciliation::Writes {
                plan,
                released_owner,
            } => (plan, released_owner),
            EntityReconciliation::Blocked(blocker) => {
                return Ok(IndexOperationStepResult::Blocked(blocker));
            }
        };
        plan.delete(row.delta_key);
        if !accounting.can_admit_input(input_bytes) || !accounting.can_admit_output(&plan) {
            if accounting.is_empty() {
                return Ok(IndexOperationStepResult::Blocked(
                    IndexOperationBlocker::OversizedEntity {
                        entity_kind: row.entity.kind,
                        entity_id: row.entity.id,
                        observed: input_bytes.max(plan.output_bytes),
                        limit: limits
                            .max_input_bytes()
                            .get()
                            .min(limits.max_output_bytes().get()),
                    },
                ));
            }
            break;
        }
        plan.stage(transaction).await?;
        for write in &plan.writes {
            match write {
                EntityWrite::Put { key, value } if unique_entries.contains_key(key) => {
                    unique_entries.insert(key.clone(), Some(value.clone()));
                }
                EntityWrite::Delete(key) if unique_entries.contains_key(key) => {
                    unique_entries.insert(key.clone(), None);
                }
                EntityWrite::Put { .. } | EntityWrite::Delete(_) | EntityWrite::Bitmap { .. } => {}
            }
        }
        released.extend(released_owner);
        accounting.admit_scan(input_bytes, Some(&plan))?;
    }
    Ok(progressed_build(SecondaryBuildStage::CatchUp(
        PrefixScanProgress {
            cursor: None,
            counters: accounting.finish()?,
        },
    )))
}

/// Releases one batch of applied-state rows under the exclusive gate.
///
/// A row may be released only while its entity has no pending delta: the
/// row is then the only proof of what the generation holds (see
/// [`HeldValue`]). The step therefore first drains every pending delta,
/// releasing each drained entity's row with it. The drain and the releases
/// share one batch's budgets. A drain the batch cannot finish stays in
/// validation at the same cursor: a release drain writes no applied row the
/// cursor could have passed.
async fn validate_and_release_applied(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    match drain_build_deltas(
        transaction,
        scope,
        operation,
        definition,
        &mut accounting,
        AppliedStateDisposition::Release,
    )
    .await?
    {
        DeltaDrain::Drained => {}
        DeltaDrain::BudgetExhausted => {
            return Ok(progressed_build(SecondaryBuildStage::Validate(
                PrefixScanProgress {
                    cursor: progress.cursor.clone(),
                    counters: accounting.finish()?,
                },
            )));
        }
        DeltaDrain::Blocked(blocker) => return Ok(IndexOperationStepResult::Blocked(blocker)),
    }
    let prefix = generation_prefix(
        scope,
        RecordKind::AppliedState,
        operation.index_id(),
        operation.generation(),
    );
    let start =
        cursor_suffix(&prefix, progress.cursor.as_ref())?.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = transaction
        .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
        .await?;
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
            let properties = read_authoritative_properties(transaction, scope, definition, entity)
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
        plan.stage(transaction).await?;
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
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &SecondaryCleanupProgress,
    aborting: bool,
    limits: SearchIndexBatchLimits,
) -> Result<IndexOperationStepResult> {
    let next = match progress {
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
                SecondaryCleanupProgress::Finalize(NoCursorProgress { counters })
            }
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
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    progress: &PrefixScanProgress,
    entity_kind: IndexElementKind,
    limits: SearchIndexBatchLimits,
) -> Result<CleanupBatch> {
    let mut accounting = BatchAccounting::new(progress.counters, limits);
    let mut cursor = progress.cursor.clone();
    let mut exhausted = true;
    let cursor_kind = match progress.cursor.as_ref() {
        None => None,
        Some(cursor) => {
            let IndexKey::Data { kind, .. } = IndexKey::parse_from_slice(scope, cursor.as_bytes())?
            else {
                return Err(corruption("secondary cleanup cursor is not a data key"));
            };
            Some(match kind {
                ScopedKey::SecondaryEntry(_) => RecordKind::SecondaryEntry,
                ScopedKey::SecondaryEqualityBitmap(_) => RecordKind::SecondaryEqualityBitmap,
                ScopedKey::IndexRecord(_)
                | ScopedKey::Operation(_)
                | ScopedKey::BuildDelta(_)
                | ScopedKey::AppliedState(_)
                | ScopedKey::TextManifestRoot(_)
                | ScopedKey::TextManifestPage(_)
                | ScopedKey::TextBuildArtifact(_)
                | ScopedKey::TextEntityState(_)
                | ScopedKey::VectorPartitionMapping(_)
                | ScopedKey::TextCorpusStatistics(_)
                | ScopedKey::TextTermStatistics(_)
                | ScopedKey::TextStatisticsEntity(_)
                | ScopedKey::IndexOperationQueue(_)
                | ScopedKey::IndexOperationRow(_) => {
                    return Err(corruption(
                        "secondary cleanup cursor is outside its entry lanes",
                    ));
                }
            })
        }
    };
    for kind in [
        RecordKind::SecondaryEntry,
        RecordKind::SecondaryEqualityBitmap,
    ] {
        if cursor_kind.is_some_and(|cursor_kind| cursor_kind != kind)
            && kind == RecordKind::SecondaryEntry
        {
            continue;
        }
        let prefix = if kind == RecordKind::SecondaryEqualityBitmap {
            IndexKey::data_prefix(
                scope,
                ScopedKey::secondary_equality_bitmap_prefix(index_id, generation, entity_kind),
            )
        } else {
            generation_prefix(scope, kind, index_id, generation)
        };
        let start = if cursor_kind == Some(kind) {
            cursor_suffix(&prefix, progress.cursor.as_ref())?
                .map_or(Bound::Unbounded, Bound::Excluded)
        } else {
            Bound::Unbounded
        };
        let mut rows = transaction
            .scan_prefix(&prefix, (start, Bound::<Bytes>::Unbounded))
            .await?;
        while accounting.can_read_another() {
            let Some(row) = rows.next().await? else {
                cursor = None;
                break;
            };
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            let mut plan = EntityWritePlan::default();
            plan.delete(row.key.clone());
            let parsed_key = IndexKey::parse_from_slice(scope, &row.key)?;
            let is_bitmap = matches!(
                &parsed_key,
                IndexKey::Data {
                    kind: ScopedKey::SecondaryEqualityBitmap(_),
                    ..
                }
            );
            let force_indivisible_bitmap_delete =
                is_bitmap && accounting.is_empty() && accounting.can_admit_output(&plan);
            if (!accounting.can_admit_input(input_bytes) && !force_indivisible_bitmap_delete)
                || !accounting.can_admit_output(&plan)
            {
                if accounting.is_empty() {
                    let entity_id = match parsed_key {
                        IndexKey::Data {
                            kind: ScopedKey::SecondaryEntry(key),
                            ..
                        } => decode_secondary_entry_value(
                            index_id,
                            generation,
                            key.lane(),
                            &row.value,
                        )?,
                        IndexKey::Data {
                            kind: ScopedKey::SecondaryEqualityBitmap(_),
                            ..
                        } => SecondaryEqualityBitmapValue::decode(&row.value)?
                            .ids()
                            .iter()
                            .next()
                            .map(IndexEntityId::new)
                            .unwrap_or_else(IndexEntityId::initial),
                        IndexKey::Global { .. } | IndexKey::Data { .. } => {
                            return Err(corruption(
                                "secondary cleanup prefix yielded another key kind",
                            ));
                        }
                    };
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
            plan.stage(transaction).await?;
            accounting.admit_scan(input_bytes, Some(&plan))?;
            cursor = Some(IndexCursor::try_new(row.key).map_err(operation_error)?);
        }
        if !accounting.can_read_another() {
            exhausted = false;
        }
        if !exhausted {
            break;
        }
    }
    Ok(CleanupBatch::Progress {
        cursor,
        counters: accounting.finish()?,
        exhausted,
    })
}

async fn delete_delta_and_applied_rows(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    counters: OperationCounters,
    limits: SearchIndexBatchLimits,
) -> Result<CleanupBatch> {
    let mut accounting = BatchAccounting::new(counters, limits);
    let mut exhausted = true;
    for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
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
                        RecordKind::BuildDelta => decode_delta(scope, &row.key, &row.value)?.0,
                        RecordKind::AppliedState => decode_applied(scope, &row.key, &row.value)?.0,
                        RecordKind::IndexRecord
                        | RecordKind::Operation
                        | RecordKind::SecondaryEntry
                        | RecordKind::TextManifestRoot
                        | RecordKind::TextManifestPage
                        | RecordKind::TextBuildArtifact
                        | RecordKind::TextEntityState
                        | RecordKind::VectorPartitionMapping
                        | RecordKind::TextCorpusStatistics
                        | RecordKind::TextTermStatistics
                        | RecordKind::TextStatisticsEntity
                        | RecordKind::SecondaryEqualityBitmap
                        | RecordKind::IndexOperationQueue
                        | RecordKind::IndexOperationRow => {
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
            plan.stage(transaction).await?;
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

#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
async fn apply_active_change(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    target: &SecondaryMutationTarget,
    entity_id: IndexEntityId,
    old_value: Option<CanonicalSecondaryValue>,
    new_value: Option<CanonicalSecondaryValue>,
) -> Result<()> {
    if definition_uses_equality_bitmap(&target.definition) {
        let mut changes = BTreeMap::<Bytes, BTreeMap<u64, bool>>::new();
        if let Some(old_value) = old_value {
            let key = secondary_entry_key(
                scope,
                target.index_id,
                target.generation,
                &target.definition,
                old_value,
                entity_id,
            )?;
            changes
                .entry(key)
                .or_default()
                .insert(entity_id.get(), false);
        }
        if let Some(new_value) = new_value {
            let key = secondary_entry_key(
                scope,
                target.index_id,
                target.generation,
                &target.definition,
                new_value,
                entity_id,
            )?;
            changes
                .entry(key)
                .or_default()
                .insert(entity_id.get(), true);
        }
        return stage_bitmap_changes(transaction, &changes).await;
    }

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
        let value = SecondaryEntryValue {
            index_id: target.index_id,
            generation: target.generation,
            lane,
            entity_id,
        };
        transaction.put(new_key, encode_secondary_entry(&value))?;
    }
    Ok(())
}

fn apply_active_change_from_overlay(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    target: &SecondaryMutationTarget,
    entity_id: IndexEntityId,
    old_value: Option<CanonicalSecondaryValue>,
    new_value: Option<CanonicalSecondaryValue>,
    unique_overlay: &mut BTreeMap<Bytes, Option<Bytes>>,
) -> Result<()> {
    if let Some(old_value) = old_value {
        let old_key = secondary_entry_key(
            scope,
            target.index_id,
            target.generation,
            &target.definition,
            old_value,
            entity_id,
        )?;
        if target.definition.unique() {
            let observed = unique_overlay.get_mut(&old_key).ok_or_else(|| {
                corruption("unique secondary release was not included in its observation batch")
            })?;
            if let Some(value) = observed.as_ref() {
                let owner = decode_secondary_entry_value(
                    target.index_id,
                    target.generation,
                    definition_lane(&target.definition),
                    value,
                )?;
                if owner != entity_id {
                    return Err(corruption(
                        "active unique secondary row belongs to another entity",
                    ));
                }
            }
            *observed = None;
        }
        transaction.delete(old_key)?;
    }

    if let Some(new_value) = new_value {
        let lane = definition_lane(&target.definition);
        let new_key = secondary_entry_key(
            scope,
            target.index_id,
            target.generation,
            &target.definition,
            new_value,
            entity_id,
        )?;
        let value = encode_secondary_entry(&SecondaryEntryValue {
            index_id: target.index_id,
            generation: target.generation,
            lane,
            entity_id,
        });
        if target.definition.unique() {
            let observed = unique_overlay.get_mut(&new_key).ok_or_else(|| {
                corruption("unique secondary claim was not included in its observation batch")
            })?;
            if let Some(existing) = observed.as_ref() {
                let owner = decode_secondary_entry_value(
                    target.index_id,
                    target.generation,
                    lane,
                    existing,
                )?;
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
            *observed = Some(value.clone());
        }
        transaction.put(new_key, value)?;
    }
    Ok(())
}

/// Plans one drained delta from the entity's lifecycle rows, read through
/// the transaction.
async fn reconciliation_plan(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    state_key: IndexEntityStateKey,
    definition: &ValidatedSecondaryIndexDefinition,
    delta_state: &CoalescedBuildDeltaState,
    next_value: Option<&CanonicalSecondaryValue>,
    disposition: AppliedStateDisposition,
) -> Result<EntityReconciliation> {
    let IndexEntityStateKey {
        index_id,
        generation,
        entity,
    } = state_key;
    if entity.kind != definition.element_kind() {
        return Err(corruption(
            "secondary reconciliation entity kind disagrees with definition",
        ));
    }
    let applied_key = scoped_index_key(scope, ScopedKey::AppliedState(state_key));
    let applied_value = transaction.get(&applied_key).await?;
    let held = held_value(
        scope,
        state_key,
        &applied_key,
        applied_value.as_deref(),
        delta_state,
    )?;
    let mut unique_entries = BTreeMap::new();
    if definition.unique() {
        for key in unique_observation_keys(
            scope,
            index_id,
            generation,
            definition,
            entity.id,
            held.candidate(),
            next_value,
        )? {
            let observed = transaction.get(&key).await?;
            unique_entries.insert(key, observed);
        }
    }
    decide_reconciliation(
        transaction,
        scope,
        state_key,
        definition,
        &held,
        next_value,
        &mut unique_entries,
        disposition,
    )
    .await
}

/// What an entity's applied-state row and pending delta prove its generation
/// holds (see [`HeldValue`]).
fn held_value(
    scope: DataScope,
    state_key: IndexEntityStateKey,
    applied_key: &[u8],
    applied_value: Option<&[u8]>,
    delta_state: &CoalescedBuildDeltaState,
) -> Result<HeldValue> {
    let Some(applied) = decode_previous_applied(
        scope,
        state_key.index_id,
        state_key.generation,
        state_key.entity,
        applied_key,
        applied_value,
    )?
    else {
        return match delta_state {
            CoalescedBuildDeltaState::SecondaryBefore(before) => {
                Ok(HeldValue::Unconfirmed(before.clone()))
            }
            CoalescedBuildDeltaState::Marker => Err(corruption(
                "secondary build delta has no original value after applied-state release",
            )),
            CoalescedBuildDeltaState::VectorBefore(_) => Err(corruption(
                "secondary build delta contains vector recovery state",
            )),
        };
    };
    Ok(HeldValue::Applied(applied))
}

fn decode_previous_applied(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    entity: IndexEntity,
    applied_key: &[u8],
    applied_value: Option<&[u8]>,
) -> Result<Option<CanonicalSecondaryValue>> {
    let Some(applied_value) = applied_value else {
        return Ok(None);
    };
    let (key_entity, applied) = decode_applied(scope, applied_key, applied_value)?;
    if key_entity != entity
        || applied.index_id != index_id
        || applied.generation != generation
        || applied.entity_kind != entity.kind
        || applied.entity_id != entity.id
    {
        return Err(corruption("secondary applied state key/value mismatch"));
    }
    match applied.state {
        AppliedFamilyState::Secondary(Some(value)) => Ok(Some(value)),
        AppliedFamilyState::Secondary(None) => {
            Err(corruption("secondary applied state names no value"))
        }
        AppliedFamilyState::Vector(_) | AppliedFamilyState::Text(_) => Err(corruption(
            "secondary generation contains another applied family",
        )),
    }
}

/// Unique entry keys one reconciliation must observe: each value in `held`
/// other than `next_value`, which it may release, and `next_value`, which it
/// claims. A key longer than storage admits names no entry and is skipped.
fn unique_observation_keys<'a>(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    held: impl IntoIterator<Item = &'a CanonicalSecondaryValue>,
    next_value: Option<&'a CanonicalSecondaryValue>,
) -> Result<Vec<Bytes>> {
    let mut keys = Vec::new();
    for value in held
        .into_iter()
        .filter(|value| Some(*value) != next_value)
        .chain(next_value)
    {
        let key = secondary_entry_key(
            scope,
            index_id,
            generation,
            definition,
            value.clone(),
            entity_id,
        )?;
        if key.len() <= STORAGE_KEY_MAX_LEN {
            keys.push(key);
        }
    }
    Ok(keys)
}

/// One entity's reconciliation with every unique claim decided.
enum EntityReconciliation {
    /// The writes that reconcile the entity, and the owner whose stale
    /// unique claim they take over, if any.
    Writes {
        plan: EntityWritePlan,
        released_owner: Option<IndexEntityId>,
    },
    Blocked(IndexOperationBlocker),
}

/// Plans one entity's reconciliation and decides a unique entry it must claim
/// from another entity.
///
/// While a build records deltas, writers leave its entries untouched, so an
/// entry may still name an entity whose authoritative row has moved off the
/// value since its last reconciliation: a stale claim that entity's pending
/// delta will release. Blocking on it would stop a build whose source holds
/// no duplicate, whichever order the claims drain in, so the claim moves to
/// `entity` here and the owner's applied row is deleted with it: the owner
/// then holds nothing until its pending delta reconciles it (see
/// [`HeldValue`]). An owner whose row still holds the value is a real
/// duplicate. Its row is read through the transaction, so a write that
/// changes it before commit makes the step retry.
#[allow(
    clippy::too_many_arguments,
    reason = "a decided reconciliation needs the transaction, exact entity, definition, held and next values, observed unique rows, and applied-state disposition"
)]
async fn decide_reconciliation(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    state_key: IndexEntityStateKey,
    definition: &ValidatedSecondaryIndexDefinition,
    held: &HeldValue,
    next_value: Option<&CanonicalSecondaryValue>,
    unique_entries: &mut BTreeMap<Bytes, Option<Bytes>>,
    disposition: AppliedStateDisposition,
) -> Result<EntityReconciliation> {
    let IndexEntityStateKey {
        index_id,
        generation,
        entity,
    } = state_key;
    let applied_key = scoped_index_key(scope, ScopedKey::AppliedState(state_key));
    let mut released_owner = None;
    loop {
        let (owner, key) = match reconciliation_plan_from_observations(
            scope,
            index_id,
            generation,
            definition,
            entity,
            applied_key.clone(),
            held,
            next_value,
            unique_entries,
            disposition,
        )? {
            ReconciliationPlan::Writes(mut plan) => {
                released_owner.into_iter().for_each(|owner| {
                    plan.delete(scoped_index_key(
                        scope,
                        ScopedKey::AppliedState(IndexEntityStateKey {
                            index_id,
                            generation,
                            entity: IndexEntity {
                                kind: entity.kind,
                                id: owner,
                            },
                        }),
                    ));
                });
                return Ok(EntityReconciliation::Writes {
                    plan,
                    released_owner,
                });
            }
            ReconciliationPlan::Blocked(blocker) => {
                return Ok(EntityReconciliation::Blocked(blocker));
            }
            ReconciliationPlan::Contended { owner, key } => (owner, key),
        };
        let Some(next) = next_value else {
            return Err(corruption(
                "secondary reconciliation contended a value it does not claim",
            ));
        };
        if released_owner.is_some() {
            return Err(corruption(
                "secondary reconciliation contended a released unique claim",
            ));
        }
        let owner_state = IndexEntityStateKey {
            index_id,
            generation,
            entity: IndexEntity {
                kind: entity.kind,
                id: owner,
            },
        };
        let owner_holds_value = transaction
            .get(authoritative_property_key(scope, owner_state.entity))
            .await?
            .is_some_and(|bytes| {
                decode_indexed_source(definition, &bytes, &mut view::Scratch::new())
                    .ok()
                    .and_then(|properties| {
                        canonical_value(definition, &properties, owner)
                            .ok()
                            .flatten()
                    })
                    .as_ref()
                    == Some(next)
            });
        if owner_holds_value {
            return Ok(EntityReconciliation::Blocked(
                IndexOperationBlocker::UniquenessViolation {
                    first_entity_id: owner,
                    second_entity_id: entity.id,
                },
            ));
        }
        // Every write that moves a reconciled row records a delta, so a
        // stale claim always has one pending.
        if transaction
            .get(scoped_index_key(scope, ScopedKey::BuildDelta(owner_state)))
            .await?
            .is_none()
        {
            return Err(corruption(
                "a unique secondary owner left its value without a pending delta",
            ));
        }
        unique_entries.insert(key, None);
        released_owner = Some(owner);
    }
}

/// Plans one entity's reconciliation from observed rows.
///
/// Every unique entry the plan reads must be in `unique_entries` (see
/// [`unique_observation_keys`]). A previous value is released where the
/// generation provably holds it for `entity`, and the next value is written
/// unless the applied row proves the generation already holds it. A unique
/// next value whose entry names another entity is returned as
/// [`ReconciliationPlan::Contended`] for [`decide_reconciliation`].
#[allow(
    clippy::too_many_arguments,
    reason = "reconciliation requires the exact generation, entity, held and next values, observed unique rows, and applied-state disposition"
)]
fn reconciliation_plan_from_observations(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    entity: IndexEntity,
    applied_key: Bytes,
    held: &HeldValue,
    next_value: Option<&CanonicalSecondaryValue>,
    unique_entries: &BTreeMap<Bytes, Option<Bytes>>,
    disposition: AppliedStateDisposition,
) -> Result<ReconciliationPlan> {
    let entity_id = entity.id;
    let lane = definition_lane(definition);
    let observed_owner = |key: &Bytes| -> Result<Option<IndexEntityId>> {
        unique_entries
            .get(key)
            .ok_or_else(|| {
                corruption("unique secondary entry was not observed before reconciliation")
            })?
            .as_deref()
            .map(|value| decode_secondary_entry_value(index_id, generation, lane, value))
            .transpose()
    };
    let applied = matches!(held, HeldValue::Applied(_));
    let previous = held.candidate();
    // Only the applied row proves the generation already holds a value.
    let holds_next = applied && previous == next_value;
    let mut plan = EntityWritePlan::default();
    'release_previous: {
        let Some(previous) = previous else {
            break 'release_previous;
        };
        if Some(previous) == next_value {
            break 'release_previous;
        }
        let key = secondary_entry_key(
            scope,
            index_id,
            generation,
            definition,
            previous.clone(),
            entity_id,
        )?;
        // A build delta keeps the value an entity had, including one
        // whose entry key storage could never write.
        if key.len() > STORAGE_KEY_MAX_LEN {
            break 'release_previous;
        }
        if definition.unique() {
            match observed_owner(&key)? {
                Some(owner) if owner == entity_id => {}
                // An unconfirmed value the entry does not credit to this
                // entity is not held by it.
                Some(_) | None if !applied => break 'release_previous,
                Some(owner) => {
                    return Ok(ReconciliationPlan::Blocked(
                        IndexOperationBlocker::UniquenessViolation {
                            first_entity_id: owner,
                            second_entity_id: entity_id,
                        },
                    ));
                }
                None => {}
            }
        }
        if definition_uses_equality_bitmap(definition) {
            plan.bitmap_remove(key, entity_id);
        } else {
            plan.delete(key);
        }
    }
    'claim_next: {
        let Some(next) = next_value else {
            break 'claim_next;
        };
        let key = secondary_entry_key(
            scope,
            index_id,
            generation,
            definition,
            next.clone(),
            entity_id,
        )?;
        if definition.unique() {
            match observed_owner(&key)? {
                Some(owner) if owner == entity_id => break 'claim_next,
                // Validation applies the same checks to a row it releases.
                Some(owner) if holds_next => {
                    return Ok(ReconciliationPlan::Blocked(
                        IndexOperationBlocker::UniquenessViolation {
                            first_entity_id: owner,
                            second_entity_id: entity_id,
                        },
                    ));
                }
                Some(owner) => return Ok(ReconciliationPlan::Contended { owner, key }),
                None if holds_next => {
                    return Err(corruption("unique secondary applied state has no entry"));
                }
                None => {}
            }
        } else if holds_next {
            break 'claim_next;
        }
        if definition_uses_equality_bitmap(definition) {
            plan.bitmap_add(key, entity_id);
        } else {
            let value = SecondaryEntryValue {
                index_id,
                generation,
                lane,
                entity_id,
            };
            plan.put(key, encode_secondary_entry(&value));
        }
    }
    match (disposition, next_value) {
        (AppliedStateDisposition::Retain, Some(next)) => {
            if !holds_next {
                let value = AppliedEntityStateValue {
                    index_id,
                    generation,
                    entity_kind: entity.kind,
                    entity_id,
                    state: AppliedFamilyState::Secondary(Some(next.clone())),
                };
                plan.put(applied_key, encode_applied_state(&value));
            }
        }
        (AppliedStateDisposition::Retain, None) | (AppliedStateDisposition::Release, _) => {
            if applied {
                plan.delete(applied_key);
            }
        }
    }
    Ok(ReconciliationPlan::Writes(plan))
}

enum ReconciliationPlan {
    Writes(EntityWritePlan),
    Blocked(IndexOperationBlocker),
    /// The next value's unique entry names another entity, which only that
    /// entity's authoritative row can tell from a stale claim.
    Contended {
        owner: IndexEntityId,
        key: Bytes,
    },
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

    fn bitmap_add(&mut self, key: Bytes, entity_id: IndexEntityId) {
        self.output_bytes = self
            .output_bytes
            .saturating_add(key.len().saturating_add(core::mem::size_of::<u64>()) as u64);
        self.writes.push(EntityWrite::Bitmap {
            key,
            entity_id,
            present: true,
        });
    }

    fn bitmap_remove(&mut self, key: Bytes, entity_id: IndexEntityId) {
        self.output_bytes = self
            .output_bytes
            .saturating_add(key.len().saturating_add(core::mem::size_of::<u64>()) as u64);
        self.writes.push(EntityWrite::Bitmap {
            key,
            entity_id,
            present: false,
        });
    }

    async fn stage(&self, transaction: &impl crate::transaction::Mutation) -> Result<()> {
        let mut bitmap_changes = BTreeMap::<Bytes, BTreeMap<u64, bool>>::new();
        for write in &self.writes {
            match write {
                EntityWrite::Put { key, value } => {
                    transaction.put(key, value)?;
                }
                EntityWrite::Delete(key) => transaction.delete(key)?,
                EntityWrite::Bitmap {
                    key,
                    entity_id,
                    present,
                } => {
                    bitmap_changes
                        .entry(key.clone())
                        .or_default()
                        .insert(entity_id.get(), *present);
                }
            }
        }
        stage_bitmap_changes(transaction, &bitmap_changes).await?;
        Ok(())
    }
}

enum EntityWrite {
    Put {
        key: Bytes,
        value: Bytes,
    },
    Delete(Bytes),
    Bitmap {
        key: Bytes,
        entity_id: IndexEntityId,
        present: bool,
    },
}

async fn stage_bitmap_changes(
    transaction: &impl crate::transaction::Mutation,
    changes: &BTreeMap<Bytes, BTreeMap<u64, bool>>,
) -> Result<()> {
    let mut merges = transaction.merge_batch(changes.len())?;
    for (key, members) in changes {
        let mut delta = BitmapMembershipDelta::default();
        for (&entity_id, &present) in members {
            if present {
                delta.add(entity_id);
            } else {
                delta.remove(entity_id);
            }
        }
        merges.bitmap(crate::transaction::merges::Key::Encoded(key), &delta)?;
    }
    merges.stage().await?;
    Ok(())
}

/// One transaction's batch budgets, shared by every stage of a step.
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

pub(crate) fn canonical_value(
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
    Ok(match definition {
        ValidatedSecondaryIndexDefinition::NodeEquality { .. }
        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
            match project_equality_value(&property.value) {
                EqualityValueProjection::Indexed(value) => {
                    Some(CanonicalSecondaryValue::equality(value))
                }
                EqualityValueProjection::AuthoritativeNull
                | EqualityValueProjection::NonReflexive => None,
                EqualityValueProjection::Unsupported(value_type) => {
                    return Err(SecondaryValueError::UnsupportedEquality(value_type));
                }
                EqualityValueProjection::Oversized {
                    encoded_len,
                    maximum,
                } => {
                    return Err(SecondaryValueError::Oversized {
                        encoded_len,
                        maximum,
                    });
                }
            }
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
            match project_range_value(&property.value, physical_direction) {
                RangeValueProjection::Indexed(value) => Some(CanonicalSecondaryValue::range(value)),
                RangeValueProjection::Unsupported(value_type) => {
                    return Err(SecondaryValueError::UnsupportedRange(value_type));
                }
                RangeValueProjection::NaN => return Err(SecondaryValueError::NaNRange),
                RangeValueProjection::Oversized {
                    encoded_len,
                    maximum,
                } => {
                    return Err(SecondaryValueError::Oversized {
                        encoded_len,
                        maximum,
                    });
                }
            }
        }
    })
}

/// Project a value about to be written. Storage writes keys of at most
/// [`STORAGE_KEY_MAX_LEN`] bytes, so a value whose entry key in this scope is
/// longer is oversized: a statement writing it fails and a build over it
/// blocks. A value being replaced was written or never had a key, and reads
/// of a longer key find nothing, so only values being written check it.
pub(crate) fn storable_value(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    properties: &[Property],
    entity_id: IndexEntityId,
) -> std::result::Result<Option<CanonicalSecondaryValue>, SecondaryValueError> {
    let Some(value) = canonical_value(definition, properties, entity_id)? else {
        return Ok(None);
    };
    let encoded_len = prepare_secondary_entry_key(
        scope,
        index_id,
        generation,
        definition,
        value.clone(),
        entity_id,
    )
    .expect("a value within its format limit forms a valid entry key")
    .encoded_len();
    if encoded_len > STORAGE_KEY_MAX_LEN {
        return Err(SecondaryValueError::Oversized {
            encoded_len,
            maximum: STORAGE_KEY_MAX_LEN,
        });
    }
    Ok(Some(value))
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SecondaryValueError {
    UnsupportedEquality(&'static str),
    UnsupportedRange(&'static str),
    NaNRange,
    Oversized { encoded_len: usize, maximum: usize },
}

fn mutation_value_error(
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    error: SecondaryValueError,
) -> HelixDbError {
    match error {
        SecondaryValueError::UnsupportedEquality(value_type) if definition.unique() => {
            HelixDbError::UnsupportedUniqueIndexValueType {
                label: definition.label().as_str().to_string(),
                property: definition.property().as_str().to_string(),
                node_id: entity_id.get(),
                value_type: value_type.to_string(),
            }
        }
        SecondaryValueError::UnsupportedEquality(value_type) => {
            SecondaryIndexValueError::UnsupportedEqualityValue { value_type }.into()
        }
        SecondaryValueError::UnsupportedRange(value_type) => {
            SecondaryIndexValueError::UnsupportedRangeValue { value_type }.into()
        }
        SecondaryValueError::NaNRange => SecondaryIndexValueError::NaNRangeValue.into(),
        SecondaryValueError::Oversized {
            encoded_len,
            maximum,
        } => SecondaryIndexValueError::EncodedKeyTooLarge {
            encoded_len,
            maximum,
        }
        .into(),
    }
}

/// Reads one exact Active equality generation from its typed physical row.
///
/// The caller must run this function inside the request lease batch associated
/// with `handle`. Unique entries and V4 non-unique bitmaps each use one point
/// read. Null and values no lane can encode are answered by the verified
/// label rows outside the lane (see [`unindexed_label_rows`]).
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn lookup_active_equality_generation(
    reader: &(impl DbReadOps + Send + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
) -> Result<roaring::RoaringTreemap> {
    exact::lookup_active_equality_generation_admitted(
        reader,
        handle,
        value,
        ReaderStorageCompatibility::Current,
        None,
        &|| Ok(()),
    )
    .await
    .map(crate::query_resources::bitmap::Bitmap::into_unbudgeted)
}

/// Owners of `values` in one Active unique node equality generation, read
/// unbudgeted (see [`exact::lookup_active_unique_equality_batch_admitted`]).
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn lookup_active_unique_equality_batch(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
) -> Result<roaring::RoaringTreemap> {
    exact::lookup_active_unique_equality_batch_admitted(reader, handle, values, None)
        .await
        .map(crate::query_resources::bitmap::Bitmap::into_unbudgeted)
}

/// One label-scoped property whose rows an equality lane may not hold.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnindexedLabel<'a> {
    pub(crate) scope: DataScope,
    pub(crate) kind: IndexElementKind,
    pub(crate) label: &'a str,
    pub(crate) property: &'a str,
}

/// A `within` set at most this large is verified directly: reading its
/// records costs at most this many rows in four `multi_get`s, while the label
/// bitmap and the lane scan grow with the label.
pub(crate) const DIRECT_UNINDEXED_VERIFICATION_ROWS: u64 =
    4 * helix_planner::cost::RECORD_BATCH_ROWS;

/// Whether [`unindexed_label_rows`] may read the label bitmap and scan the
/// equality lane at the same time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelLaneReads {
    /// Both reads are in flight together.
    Concurrent,
    /// The label bitmap is read first, then the lane: a caller with no free
    /// read in its request's budget keeps one read in flight.
    Sequential,
}

/// Candidate rows of `label` that no entry of the Active equality generation
/// `lane` holds, narrowed to `within` when it is given.
///
/// Writes reject values a lane cannot encode, so the rows outside the lane
/// are exactly the label rows whose property is missing, null or NaN (and
/// label-bitmap IDs whose record no longer carries the label). With no Active
/// generation (`lane` is `None`) every label row is a candidate. No row
/// outside the label is ever read.
///
/// A `within` of at most [`DIRECT_UNINDEXED_VERIFICATION_ROWS`] IDs is
/// returned as it is, without reading the label bitmap or scanning the lane,
/// so a selective intersection never pays for the whole label. It may then
/// hold rows of other labels or rows the lane holds: callers verify every
/// candidate with [`verified_unindexed_rows`], which drops rows outside the
/// label, and whose `accept` rejects every value a lane holds (null, or a
/// value no lane can encode) unless the caller unites the result with the
/// lane's own rows for those values anyway.
///
/// The lane scan checks `deadline` before every row it reads, so an expired
/// request stops scanning a large lane.
pub(crate) async fn unindexed_label_rows(
    reader: &(impl DbReadOps + Send + Sync),
    label: UnindexedLabel<'_>,
    lane: Option<(&ActiveIndexHandle, ReaderStorageCompatibility)>,
    within: Option<&roaring::RoaringTreemap>,
    reads: LabelLaneReads,
    deadline: &(impl Fn() -> Result<()> + Sync),
) -> Result<roaring::RoaringTreemap> {
    if let Some(within) = within.filter(|within| within.len() <= DIRECT_UNINDEXED_VERIFICATION_ROWS)
    {
        return Ok(within.clone());
    }
    let rows = async {
        record_equality_point_read();
        match label.kind {
            IndexElementKind::Node => {
                crate::search::lookup_equality_index_set_scoped(
                    reader,
                    "$label",
                    label.label,
                    label.scope,
                )
                .await
            }
            IndexElementKind::Edge => {
                crate::search::lookup_global_edge_label_index_scoped(
                    reader,
                    label.label,
                    label.scope,
                )
                .await
            }
        }
    };
    let held = async {
        let Some((handle, compatibility)) = lane else {
            return Ok(roaring::RoaringTreemap::new());
        };
        equality_lane_rows(reader, handle, compatibility, deadline).await
    };
    let (mut candidates, held) = match reads {
        LabelLaneReads::Concurrent => futures::try_join!(rows, held)?,
        LabelLaneReads::Sequential => (rows.await?, held.await?),
    };
    candidates -= held;
    if let Some(within) = within {
        candidates &= within;
    }
    Ok(candidates)
}

/// Every entity one Active equality generation holds, from one scan of its
/// lane (two for a reader that still unions deployed V3 entries), checking
/// `deadline` before every row.
async fn equality_lane_rows(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    compatibility: ReaderStorageCompatibility,
    deadline: &(impl Fn() -> Result<()> + Sync),
) -> Result<roaring::RoaringTreemap> {
    let Some(
        definition @ (ValidatedSecondaryIndexDefinition::NodeEquality { .. }
        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }),
    ) = handle.secondary_definition()
    else {
        return Err(corruption(
            "unindexed label rows require an Active equality definition",
        ));
    };
    let scope = handle.scope();
    let mut held = roaring::RoaringTreemap::new();
    if definition_uses_equality_bitmap(definition) {
        #[cfg(any(test, feature = "production-coverage"))]
        record(ReadKind::Scan);
        let prefix = IndexKey::data_prefix(
            scope,
            ScopedKey::secondary_equality_bitmap_prefix(
                handle.index_id(),
                handle.generation(),
                definition.element_kind(),
            ),
        );
        let mut rows = reader.scan_prefix(&prefix, ..).await?;
        loop {
            deadline()?;
            let Some(row) = rows.next().await? else {
                break;
            };
            let IndexKey::Data {
                kind: ScopedKey::SecondaryEqualityBitmap(key),
                ..
            } = IndexKey::parse_from_slice(scope, &row.key)?
            else {
                return Err(corruption(
                    "equality bitmap lane prefix yielded another key kind",
                ));
            };
            if key.index_id != handle.index_id() || key.generation != handle.generation() {
                return Err(corruption(
                    "equality bitmap lane prefix yielded another generation",
                ));
            }
            held |= SecondaryEqualityBitmapValue::decode(&row.value)?.into_ids();
        }
        if compatibility != ReaderStorageCompatibility::LegacyEqualityUnion {
            return Ok(held);
        }
    }
    // Unique owners, and the deployed V3 per-entity entries a legacy reader
    // still unions, are one entry per entity in the definition's lane.
    #[cfg(any(test, feature = "production-coverage"))]
    record(ReadKind::Scan);
    let lane = definition_lane(definition);
    let prefix = IndexKey::data_prefix(
        scope,
        ScopedKey::secondary_lane_prefix(handle.index_id(), handle.generation(), lane),
    );
    let mut rows = reader.scan_prefix(&prefix, ..).await?;
    loop {
        deadline()?;
        let Some(row) = rows.next().await? else {
            break;
        };
        let IndexKey::Data {
            kind: ScopedKey::SecondaryEntry(entry),
            ..
        } = IndexKey::parse_from_slice(scope, &row.key)?
        else {
            return Err(corruption(
                "equality entry lane prefix yielded another key kind",
            ));
        };
        let owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if entry
            .entity_id()
            .is_some_and(|key_owner| key_owner != owner)
        {
            return Err(corruption("equality entry key and value owners disagree"));
        }
        held.insert(owner.get());
    }
    Ok(held)
}

/// The `candidates` whose record carries `label.label` and whose
/// `label.property` (`None` when missing) passes `accept`.
///
/// Records are read in multi-gets of
/// [`helix_planner::cost::RECORD_BATCH_ROWS`] keys, so memory stays bounded
/// by one batch of records plus the result. `deadline` is checked before
/// every batch, so an expired request stops reading records. With a
/// `budget`, each batch's raw records and the one record being decoded are
/// admitted before they are read, and the result as it grows.
pub(crate) async fn verified_unindexed_rows(
    reader: &(impl DbReadOps + Sync),
    label: UnindexedLabel<'_>,
    candidates: roaring::RoaringTreemap,
    accept: impl Fn(Option<&PropertyValue>) -> bool,
    deadline: &(impl Fn() -> Result<()> + Sync),
    budget: Option<&crate::query_resources::Budget>,
) -> Result<crate::query_resources::bitmap::Bitmap> {
    const BATCH: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;
    let mut ids = candidates.into_iter();
    let mut verified = crate::query_resources::bitmap::Builder::new(budget)?;
    let mut scratch = view::Scratch::new();
    loop {
        deadline()?;
        let batch = ids.by_ref().take(BATCH).collect::<Vec<_>>();
        if batch.is_empty() {
            return Ok(verified.finish());
        }
        #[cfg(any(test, feature = "production-coverage"))]
        batch.iter().for_each(|_| record(ReadKind::Graph));
        let keys = batch
            .iter()
            .map(|id| {
                authoritative_property_key(
                    label.scope,
                    IndexEntity {
                        kind: label.kind,
                        id: IndexEntityId::new(*id),
                    },
                )
            })
            .collect::<Vec<_>>();
        if let Some(budget) = budget {
            budget.record_reads(crate::query_resources::StorageReadUsage {
                multi_get_batches: 1,
                multi_get_keys: keys.len(),
                ..Default::default()
            });
        }
        let records = reader.multi_get(&keys).await?;
        // Raw records stay held while one at a time is decoded, at the common
        // hydration decoder bound of 32 bytes per raw byte.
        let _records_memory = budget
            .map(|budget| {
                let records = records.iter().flatten();
                budget.reserve(
                    records
                        .clone()
                        .fold(0_usize, |bytes, record| bytes.saturating_add(record.len()))
                        .saturating_add(
                            records
                                .map(Bytes::len)
                                .max()
                                .unwrap_or(0)
                                .saturating_mul(32),
                        ),
                )
            })
            .transpose()?;
        for (id, record) in batch.into_iter().zip(records) {
            let Some(record) = record else {
                continue;
            };
            let properties = view::decode_selected(&record, &mut scratch, |name| {
                name == "$label" || name == label.property
            })?;
            let carries_label = properties.iter().any(|property| {
                property.name == "$label" && property.value.as_str() == Some(label.label)
            });
            let value = properties
                .iter()
                .find(|property| property.name == label.property)
                .map(|property| &property.value);
            if carries_label && accept(value) {
                verified.insert(id)?;
            }
        }
    }
}

/// Verify one unique hit against its authoritative graph row in the same view.
/// Raw property bytes remain admitted while the shared owned decoder runs.
async fn authoritative_equality_matches(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    query: &PropertyValue,
    budget: Option<&crate::query_resources::Budget>,
) -> Result<bool> {
    let entity = IndexEntity {
        kind: definition.element_kind(),
        id: entity_id,
    };
    let _key_memory = budget
        .map(|budget| {
            budget.reserve(
                scope
                    .encoded_len()
                    .saturating_add(size_of::<u8>() + size_of::<u64>()),
            )
        })
        .transpose()?;
    let key = authoritative_property_key(scope, entity);
    if let Some(budget) = budget {
        budget.record_reads(crate::query_resources::StorageReadUsage {
            point_gets: 1,
            ..Default::default()
        });
    }
    let Some(bytes) = reader.get(key).await? else {
        return Ok(false);
    };
    // Match the common hydration decoder bound plus its retained raw bytes.
    let _properties_memory = budget
        .map(|budget| budget.reserve(bytes.len().saturating_mul(33)))
        .transpose()?;
    let properties = decode_indexed_source(definition, &bytes, &mut view::Scratch::new())?;
    Ok(properties_match_definition(definition, &properties)
        && properties
            .iter()
            .find(|property| property.name == definition.property().as_str())
            .is_some_and(|property| property.value.eq_value(query)))
}

/// Decodes the properties of a stored source row that [`canonical_value`]
/// and [`properties_match_definition`] read, the label and the indexed
/// property, in stored order with duplicates. Every other property is left
/// undecoded.
///
/// Validation and errors are those of the full row decoder, and both
/// readers answer on the result exactly as on the complete row. A change to
/// what they read must change this selection with it.
fn decode_indexed_source(
    definition: &ValidatedSecondaryIndexDefinition,
    bytes: &[u8],
    scratch: &mut view::Scratch,
) -> std::result::Result<Vec<Property>, crate::encoding::error::EncodingError> {
    view::decode_selected(bytes, scratch, |name| {
        name == "$label" || name == definition.property().as_str()
    })
}

fn properties_match_definition(
    definition: &ValidatedSecondaryIndexDefinition,
    properties: &[Property],
) -> bool {
    properties.iter().any(|property| {
        property.name == "$label" && property.value.as_str() == Some(definition.label().as_str())
    })
}

/// One exact typed range predicate evaluated by managed secondary serving.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SecondaryRangeQuery {
    Lower {
        value: PropertyValue,
        inclusive: bool,
    },
    Upper {
        value: PropertyValue,
        inclusive: bool,
    },
    Between {
        lower: PropertyValue,
        lower_inclusive: bool,
        upper: PropertyValue,
        upper_inclusive: bool,
    },
}

/// Scans one exact Active range generation in its configured physical order.
///
/// Storage bounds use typed, self-delimiting payloads. Every candidate is then
/// checked against authoritative graph state before it can consume `limit`.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn scan_active_range_generation(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
    limit: Option<usize>,
) -> Result<Vec<u64>> {
    scan_active_range_generation_ordered(
        reader,
        handle,
        query,
        helix_planner::ir::RangeScanIteration::Forward,
        limit,
        &[],
        &exact::UnobservedRangeScan,
    )
    .await
}

/// Produces suffix bounds for one generation/lane `scan_prefix` call.
///
/// `None` is an empty range: inverted bounds, and bounds a range lane cannot
/// encode (null, bool, NaN, or a value of no range domain) or cannot order
/// against each other (different domains). A row's value compares with such a
/// bound in no domain, so the per-row predicate is false for every row. An
/// oversized bound stays an error; it reads nothing.
fn secondary_range_scan_bounds(
    direction: StorageRangeIndexDirection,
    query: &SecondaryRangeQuery,
) -> Result<Option<(Bound<Bytes>, Bound<Bytes>)>> {
    let physical = |value: &PropertyValue| project_query_range_value(value, direction);
    Ok(Some(match query {
        SecondaryRangeQuery::Lower { value, inclusive } => {
            let Some(value) = physical(value)? else {
                return Ok(None);
            };
            let (domain_start, domain_end) = value.domain_key_bounds();
            match direction {
                StorageRangeIndexDirection::Asc => (
                    if *inclusive {
                        Bound::Included(value.entity_key_suffix(u64::MIN))
                    } else {
                        Bound::Excluded(value.entity_key_suffix(u64::MAX))
                    },
                    domain_end,
                ),
                StorageRangeIndexDirection::Desc => (
                    domain_start,
                    if *inclusive {
                        Bound::Included(value.entity_key_suffix(u64::MAX))
                    } else {
                        Bound::Excluded(value.entity_key_suffix(u64::MIN))
                    },
                ),
            }
        }
        SecondaryRangeQuery::Upper { value, inclusive } => {
            let Some(value) = physical(value)? else {
                return Ok(None);
            };
            let (domain_start, domain_end) = value.domain_key_bounds();
            match direction {
                StorageRangeIndexDirection::Asc => (
                    domain_start,
                    if *inclusive {
                        Bound::Included(value.entity_key_suffix(u64::MAX))
                    } else {
                        Bound::Excluded(value.entity_key_suffix(u64::MIN))
                    },
                ),
                StorageRangeIndexDirection::Desc => (
                    if *inclusive {
                        Bound::Included(value.entity_key_suffix(u64::MIN))
                    } else {
                        Bound::Excluded(value.entity_key_suffix(u64::MAX))
                    },
                    domain_end,
                ),
            }
        }
        SecondaryRangeQuery::Between {
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
        } => {
            let Some(ordering) = lower.compare(upper) else {
                return Ok(None);
            };
            if ordering.is_gt() || (ordering.is_eq() && (!*lower_inclusive || !*upper_inclusive)) {
                return Ok(None);
            }
            let (Some(lower), Some(upper)) = (physical(lower)?, physical(upper)?) else {
                return Ok(None);
            };
            match direction {
                StorageRangeIndexDirection::Asc => (
                    if *lower_inclusive {
                        Bound::Included(lower.entity_key_suffix(u64::MIN))
                    } else {
                        Bound::Excluded(lower.entity_key_suffix(u64::MAX))
                    },
                    if *upper_inclusive {
                        Bound::Included(upper.entity_key_suffix(u64::MAX))
                    } else {
                        Bound::Excluded(upper.entity_key_suffix(u64::MIN))
                    },
                ),
                StorageRangeIndexDirection::Desc => (
                    if *upper_inclusive {
                        Bound::Included(upper.entity_key_suffix(u64::MIN))
                    } else {
                        Bound::Excluded(upper.entity_key_suffix(u64::MAX))
                    },
                    if *lower_inclusive {
                        Bound::Included(lower.entity_key_suffix(u64::MAX))
                    } else {
                        Bound::Excluded(lower.entity_key_suffix(u64::MIN))
                    },
                ),
            }
        }
    }))
}

/// The range-lane encoding of a query bound, or `None` when no range lane
/// entry can compare with it.
fn project_query_range_value(
    value: &PropertyValue,
    direction: StorageRangeIndexDirection,
) -> Result<Option<CanonicalRangeValue>> {
    match project_range_value(value, direction) {
        RangeValueProjection::Indexed(value) => Ok(Some(value)),
        RangeValueProjection::Unsupported(_) | RangeValueProjection::NaN => Ok(None),
        RangeValueProjection::Oversized {
            encoded_len,
            maximum,
        } => Err(SecondaryIndexValueError::EncodedKeyTooLarge {
            encoded_len,
            maximum,
        }
        .into()),
    }
}

fn secondary_range_query_matches(query: &SecondaryRangeQuery, value: &PropertyValue) -> bool {
    match query {
        SecondaryRangeQuery::Lower {
            value: lower,
            inclusive,
        } => value
            .compare(lower)
            .is_some_and(|ordering| ordering.is_gt() || (*inclusive && ordering.is_eq())),
        SecondaryRangeQuery::Upper {
            value: upper,
            inclusive,
        } => value
            .compare(upper)
            .is_some_and(|ordering| ordering.is_lt() || (*inclusive && ordering.is_eq())),
        SecondaryRangeQuery::Between {
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
        } => {
            let lower_matches = value
                .compare(lower)
                .is_some_and(|ordering| ordering.is_gt() || (*lower_inclusive && ordering.is_eq()));
            let upper_matches = value
                .compare(upper)
                .is_some_and(|ordering| ordering.is_lt() || (*upper_inclusive && ordering.is_eq()));
            lower_matches && upper_matches
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn authoritative_range_matches(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    direction: StorageRangeIndexDirection,
    key_value: &CanonicalRangeValue,
    query: Option<&SecondaryRangeQuery>,
    progress: &dyn ExactRangeScanProgress,
) -> Result<bool> {
    let entity = IndexEntity {
        kind: definition.element_kind(),
        id: entity_id,
    };
    progress.checkpoint()?;
    progress.authoritative_read();
    let Some(bytes) = reader
        .get(authoritative_property_key(scope, entity))
        .await?
    else {
        return Ok(false);
    };
    progress.authoritative_decode();
    let properties = decode_indexed_source(definition, &bytes, &mut view::Scratch::new())?;
    if !properties_match_definition(definition, &properties) {
        return Ok(false);
    }
    let Some(property) = properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
    else {
        return Ok(false);
    };
    let RangeValueProjection::Indexed(authoritative) =
        project_range_value(&property.value, direction)
    else {
        return Ok(false);
    };
    if &authoritative != key_value {
        return Ok(false);
    }
    Ok(query.is_none_or(|query| secondary_range_query_matches(query, &property.value)))
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

pub(crate) fn definition_uses_equality_bitmap(
    definition: &ValidatedSecondaryIndexDefinition,
) -> bool {
    matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeEquality { unique: false, .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }
    )
}

fn secondary_entry_key(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    value: CanonicalSecondaryValue,
    entity_id: IndexEntityId,
) -> Result<Bytes> {
    Ok(
        prepare_secondary_entry_key(scope, index_id, generation, definition, value, entity_id)?
            .to_bytes(),
    )
}

/// Construct a validated typed key before allocating its serialized frame.
fn prepare_secondary_entry_key(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    value: CanonicalSecondaryValue,
    entity_id: IndexEntityId,
) -> Result<IndexKey> {
    if definition_uses_equality_bitmap(definition) {
        let CanonicalSecondaryValue::Equality(value) = value else {
            return Err(corruption(
                "non-unique equality definition received a range value",
            ));
        };
        let key = SecondaryEqualityBitmapKey::try_new(
            index_id,
            generation,
            definition.element_kind(),
            value,
        )?;
        return Ok(IndexKey::Data {
            scope,
            kind: ScopedKey::SecondaryEqualityBitmap(key),
        });
    }
    let lane = definition_lane(definition);
    let key = SecondaryEntryKey::try_new(
        index_id,
        generation,
        lane,
        value,
        (!lane.is_unique()).then_some(entity_id),
    )?;
    Ok(IndexKey::Data {
        scope,
        kind: ScopedKey::SecondaryEntry(key),
    })
}

#[cfg(test)]
pub(crate) fn equality_bitmap_key_for_test(handle: &ActiveIndexHandle, value: &str) -> Bytes {
    let definition = handle
        .secondary_definition()
        .expect("an equality bitmap fixture requires a secondary handle");
    assert!(
        definition_uses_equality_bitmap(definition),
        "an equality bitmap fixture requires a non-unique equality definition"
    );
    secondary_entry_key(
        handle.scope(),
        handle.index_id(),
        handle.generation(),
        definition,
        CanonicalSecondaryValue::equality_string(value),
        IndexEntityId::initial(),
    )
    .expect("an equality bitmap fixture key validates")
}

fn decode_secondary_entry_value(
    index_id: IndexId,
    generation: IndexGenerationId,
    lane: SecondaryEntryLane,
    bytes: &[u8],
) -> Result<IndexEntityId> {
    let value = decode_secondary_entry(lane, bytes)?;
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
    let IndexKey::Data {
        kind: ScopedKey::BuildDelta(key),
        ..
    } = IndexKey::parse_from_slice(scope, key)?
    else {
        return Err(corruption("build-delta prefix yielded another key kind"));
    };
    let value = decode_build_delta(value)?;
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
    let value = decode_applied_state(value)?;
    if key.index_id != value.index_id
        || key.generation != value.generation
        || key.entity.kind != value.entity_kind
        || key.entity.id != value.entity_id
    {
        return Err(corruption("applied-state key/value mismatch"));
    }
    Ok((key.entity, value))
}

/// The label and indexed property of `entity`'s authoritative row, in stored
/// order; every other property is left undecoded.
async fn read_authoritative_properties(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    entity: IndexEntity,
) -> Result<Option<Vec<Property>>> {
    reader
        .get(authoritative_property_key(scope, entity))
        .await?
        .map(|bytes| {
            decode_indexed_source(definition, &bytes, &mut view::Scratch::new())
                .map_err(HelixDbError::from)
        })
        .transpose()
}

pub(crate) fn authoritative_property_key(scope: DataScope, entity: IndexEntity) -> Bytes {
    match entity.kind {
        IndexElementKind::Node => DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity.id.get())),
        }
        .to_bytes(),
        IndexElementKind::Edge => DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(entity.id.get())),
        }
        .to_bytes(),
    }
}

async fn load_operation_index(
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    operation: &IndexOperationRecord,
) -> Result<IndexRecordV2> {
    let key = scoped_index_key(scope, ScopedKey::index_record(operation.identity().clone()));
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
    transaction: &impl crate::transaction::Mutation,
    scope: DataScope,
    kind: RecordKind,
    index_id: IndexId,
    generation: IndexGenerationId,
) -> Result<bool> {
    let prefix = generation_prefix(scope, kind, index_id, generation);
    let mut rows = transaction.scan_prefix(prefix, ..).await?;
    Ok(rows.next().await?.is_some())
}

pub(crate) fn source_prefix(scope: DataScope, kind: IndexElementKind) -> Bytes {
    let prefix = match kind {
        IndexElementKind::Node => KeyPrefix::NodeProperty,
        IndexElementKind::Edge => KeyPrefix::EdgePropertyById,
    };
    DataKey::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

pub(crate) fn source_entity(
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
            return Err(corruption(
                "secondary source prefix yielded another key kind",
            ));
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
        return Err(corruption(
            "secondary cursor is outside its exact scan prefix",
        ));
    };
    Ok(Some(Bytes::copy_from_slice(suffix)))
}

fn scoped_index_key(scope: DataScope, key: ScopedKey) -> Bytes {
    IndexKey::Data { scope, kind: key }.to_bytes()
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

fn operation_error(error: crate::index_lifecycle::IndexOperationModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{ByteRangeBounds, Db, DbIterator, DbReadOps, IsolationLevel, KeyValue};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SecondaryIndexDefinition, VectorIndexDefinition,
    };
    use crate::encoding::v2::values::encode_index_record;
    use crate::encoding::v2::values::property::{decode_properties, encode_properties};
    use crate::index_lifecycle::lifecycle::{
        create_index_operation, drop_index_operation, InitialBuildProgress,
    };
    use crate::index_lifecycle::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, read_operation,
        ClaimPermission, CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_lifecycle::{
        ClaimSequence, IndexDdlReceipt, IndexOperationExecutionState, IndexOperationId,
        IndexOperationKind, IndexRevision, IndexStateTransition, PhysicalGeneration,
        VectorGenerationDescriptor, VectorPhysicalIndexId, VectorPhysicalLayout, WriterEpoch,
    };
    use crate::migrations::startup::bootstrap_writer;

    const NOW_MILLIS: u64 = 1;

    pub(super) async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
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
    pub(super) async fn active_read_handle(
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
                ScopedKey::index_record(definition.identity()),
            ),
            encode_index_record(&active),
        )
        .await
        .expect("secondary read fixture Active record persists");
        ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("secondary read fixture projects an Active handle")
    }

    pub(super) async fn active_vector_read_handle(db: &Db) -> ActiveIndexHandle {
        let definition = ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node(
                "User",
                "embedding",
                2,
                crate::search::vector::VectorDistanceMetric::Cosine,
            )
            .expect("vector fixture validates"),
        )
        .expect("validated vector fixture");
        let ValidatedDynamicIndexDefinition::Vector(vector) = &definition else {
            panic!("fixture is a vector definition")
        };
        let building = IndexRecordV2::building(
            IndexId::initial(),
            definition.clone(),
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::initial(),
                layout: VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::initial(),
                },
                descriptor: VectorGenerationDescriptor::for_definition(vector),
            },
            IndexOperationId::new_v4(),
        )
        .expect("vector read fixture starts building");
        let active = building
            .transition(IndexStateTransition::Activate)
            .expect("vector read fixture activates");
        db.put(
            scoped_index_key(
                DataScope::LegacyUnscoped,
                ScopedKey::index_record(definition.identity()),
            ),
            encode_index_record(&active),
        )
        .await
        .expect("vector read fixture Active record persists");
        ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("vector read fixture projects an Active handle")
    }

    struct ExactKeyScan<'a> {
        db: &'a Db,
        key: Bytes,
    }

    #[derive(Clone, Copy)]
    enum ExactReadFailure {
        Get,
        MultiGet,
        ShortMultiGet,
        Scan,
        Next,
    }

    struct FailingExactRead<'a> {
        db: &'a Db,
        failure: ExactReadFailure,
    }

    #[async_trait::async_trait]
    impl DbReadOps for FailingExactRead<'_> {
        async fn get_with_options<K: AsRef<[u8]> + Send>(
            &self,
            key: K,
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
            if matches!(self.failure, ExactReadFailure::Get) {
                return Err(slatedb::Error::unavailable(
                    "injected exact get failure".to_string(),
                ));
            }
            self.db.get_with_options(key, options).await
        }

        async fn multi_get_with_options<K>(
            &self,
            keys: &[K],
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error>
        where
            K: AsRef<[u8]> + Send + Sync,
        {
            if matches!(self.failure, ExactReadFailure::ShortMultiGet) {
                return Ok(Vec::new());
            }
            if matches!(self.failure, ExactReadFailure::MultiGet) {
                return Err(slatedb::Error::unavailable(
                    "injected exact multi-get failure".to_string(),
                ));
            }
            self.db.multi_get_with_options(keys, options).await
        }

        async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
            &self,
            key: K,
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Option<KeyValue>, slatedb::Error> {
            self.db.get_key_value_with_options(key, options).await
        }

        async fn scan_with_options<T>(
            &self,
            range: T,
            options: &slatedb::config::ScanOptions,
        ) -> std::result::Result<DbIterator, slatedb::Error>
        where
            T: ByteRangeBounds + Send,
        {
            if matches!(self.failure, ExactReadFailure::Scan) {
                return Err(slatedb::Error::unavailable(
                    "injected exact scan failure".to_string(),
                ));
            }
            let rows = self.db.scan_with_options(range, options).await?;
            if matches!(self.failure, ExactReadFailure::Next) {
                self.db.close().await?;
            }
            Ok(rows)
        }
    }

    #[tokio::test]
    async fn unique_batch_rejects_failed_and_truncated_multi_gets() {
        let db = test_db("unique-batch-storage-failures").await;
        let handle = active_read_handle(
            &db,
            crate::config::SecondaryIndexDefinition::node_unique_equality("Fixture", "key")
                .unwrap(),
        )
        .await;
        let values = [
            PropertyValue::String("first".into()),
            PropertyValue::String("second".into()),
        ];
        for failure in [ExactReadFailure::MultiGet, ExactReadFailure::ShortMultiGet] {
            assert!(lookup_active_unique_equality_batch(
                &FailingExactRead { db: &db, failure },
                &handle,
                &values
            )
            .await
            .is_err());
        }
        db.close().await.unwrap();
    }

    #[async_trait::async_trait]
    impl DbReadOps for ExactKeyScan<'_> {
        async fn get_with_options<K: AsRef<[u8]> + Send>(
            &self,
            key: K,
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Option<Bytes>, slatedb::Error> {
            self.db.get_with_options(key, options).await
        }

        async fn multi_get_with_options<K>(
            &self,
            keys: &[K],
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Vec<Option<Bytes>>, slatedb::Error>
        where
            K: AsRef<[u8]> + Send + Sync,
        {
            self.db.multi_get_with_options(keys, options).await
        }

        async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
            &self,
            key: K,
            options: &slatedb::config::ReadOptions,
        ) -> std::result::Result<Option<KeyValue>, slatedb::Error> {
            self.db.get_key_value_with_options(key, options).await
        }

        async fn scan_with_options<T>(
            &self,
            _range: T,
            options: &slatedb::config::ScanOptions,
        ) -> std::result::Result<DbIterator, slatedb::Error>
        where
            T: ByteRangeBounds + Send,
        {
            self.db
                .scan_with_options(self.key.clone()..=self.key.clone(), options)
                .await
        }
    }

    /// Persists one generation-qualified entry matching the fixture handle.
    pub(super) async fn put_read_entry(
        db: &Db,
        handle: &ActiveIndexHandle,
        value: &str,
        entity_id: u64,
    ) {
        let definition = handle
            .secondary_definition()
            .expect("secondary read fixture uses a secondary handle");
        let canonical = match definition {
            ValidatedSecondaryIndexDefinition::NodeEquality { .. }
            | ValidatedSecondaryIndexDefinition::EdgeEquality { .. } => {
                CanonicalSecondaryValue::equality_string(value)
            }
            ValidatedSecondaryIndexDefinition::NodeRange { direction, .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { direction, .. } => {
                let direction = match direction {
                    RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
                    RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
                };
                CanonicalSecondaryValue::range_string(direction, value)
            }
        };
        let entity_id = IndexEntityId::new(entity_id);
        let lane = definition_lane(definition);
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            canonical,
            entity_id,
        )
        .expect("secondary read fixture key validates");
        let encoded = if definition_uses_equality_bitmap(definition) {
            let mut ids = db
                .get(&key)
                .await
                .expect("secondary read fixture bitmap is readable")
                .map(|bytes| {
                    SecondaryEqualityBitmapValue::decode(&bytes)
                        .expect("secondary read fixture bitmap decodes")
                        .into_ids()
                })
                .unwrap_or_default();
            ids.insert(entity_id.get());
            SecondaryEqualityBitmapValue::new(ids).encode()
        } else {
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane,
                entity_id,
            })
        };
        db.put(key, encoded)
            .await
            .expect("secondary read fixture entry persists");
        db.put(
            authoritative_property_key(
                handle.scope(),
                IndexEntity {
                    kind: definition.element_kind(),
                    id: entity_id,
                },
            ),
            encode_properties(&[
                Property::string("$label", definition.label().as_str()),
                Property::string(definition.property().as_str(), value),
            ]),
        )
        .await
        .expect("secondary read fixture authoritative row persists");
    }

    #[tokio::test]
    async fn foreground_unique_batch_preserves_release_then_claim_order() {
        let db = test_db("secondary-foreground-unique-overlay").await;
        let scope = DataScope::LegacyUnscoped;
        let ValidatedDynamicIndexDefinition::Secondary(definition) = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique fixture validates"),
        ) else {
            panic!("fixture is secondary")
        };
        let mutations = SecondaryMutationSet {
            targets: vec![SecondaryMutationTarget {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                definition,
                mode: SecondaryMutationMode::MaintainActive,
            }],
        };
        let routes = super::super::mutation_catalog::RoutedMutationTargets::Owned(vec![
            super::super::mutation_catalog::MutationRouteTarget::Secondary(0),
        ]);
        let properties = |email: &str| {
            super::super::graph_mutation::CanonicalPropertyRow::new(vec![
                Property::string("$label", "User"),
                Property::string("email", email),
            ])
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("foreground unique transaction opens");
        let mut runtime = SecondaryMutationRuntime::default();
        runtime
            .collect(
                scope,
                &mutations,
                &routes,
                &super::super::graph_mutation::GraphMutationTransition::create(
                    scope,
                    super::super::graph_mutation::GraphEntity::node(1),
                    properties("released@example.com"),
                ),
            )
            .expect("initial claim collects");
        runtime
            .flush(&transaction, &mutations)
            .await
            .expect("initial claim stages");

        let super::super::graph_mutation::PropertyEditOutcome::Changed(release) =
            super::super::graph_mutation::GraphMutationTransition::edit(
                scope,
                super::super::graph_mutation::GraphEntity::node(1),
                properties("released@example.com"),
                super::super::graph_mutation::PropertyEdit::set(Property::string(
                    "email",
                    "replacement@example.com",
                )),
            )
        else {
            panic!("release fixture changes")
        };
        runtime
            .collect(scope, &mutations, &routes, &release)
            .expect("release collects");
        runtime
            .collect(
                scope,
                &mutations,
                &routes,
                &super::super::graph_mutation::GraphMutationTransition::create(
                    scope,
                    super::super::graph_mutation::GraphEntity::node(2),
                    properties("released@example.com"),
                ),
            )
            .expect("later claim collects");
        runtime
            .flush(&transaction, &mutations)
            .await
            .expect("release then claim succeeds in input order");

        runtime
            .collect(
                scope,
                &mutations,
                &routes,
                &super::super::graph_mutation::GraphMutationTransition::create(
                    scope,
                    super::super::graph_mutation::GraphEntity::node(3),
                    properties("released@example.com"),
                ),
            )
            .expect("conflicting claim collects");
        assert!(matches!(
            runtime.flush(&transaction, &mutations).await,
            Err(HelixDbError::UniqueConstraintViolation {
                existing_node_id: 2,
                attempted_node_id: 3,
                ..
            })
        ));
        transaction.rollback();
        db.close().await.expect("unique overlay fixture closes");
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
                lookup_active_equality_generation(
                    &db,
                    &handle,
                    &PropertyValue::String("same".to_string()),
                )
                .await
                .expect("managed equality generation scans")
                .into_iter()
                .collect::<Vec<_>>(),
                vec![2, 9]
            );
            assert_eq!(
                lookup_active_equality_point_literal(
                    &db,
                    &handle,
                    &PropertyValue::String("same".to_string()),
                )
                .await
                .expect("exact managed equality point read succeeds")
                .into_iter()
                .collect::<Vec<_>>(),
                vec![2, 9]
            );
            assert_eq!(
                lookup_active_equality_literal_batch(
                    &db,
                    &handle,
                    &[
                        PropertyValue::String("same".to_string()),
                        PropertyValue::String("other".to_string()),
                        PropertyValue::String("same".to_string()),
                    ],
                )
                .await
                .expect("exact literal batch preserves duplicate physical reads")
                .into_iter()
                .collect::<Vec<_>>(),
                vec![2, 4, 9]
            );
            assert!(lookup_active_equality_generation(
                &db,
                &handle,
                &PropertyValue::String("missing".to_string()),
            )
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
            lookup_active_equality_generation(
                &db,
                &handle,
                &PropertyValue::String("only".to_string()),
            )
            .await
            .expect("managed unique equality point-loads")
            .into_iter()
            .collect::<Vec<_>>(),
            vec![7]
        );
        assert_eq!(
            lookup_active_equality_point_literal(
                &db,
                &handle,
                &PropertyValue::String("only".to_string()),
            )
            .await
            .expect("exact unique owner point read succeeds")
            .into_iter()
            .collect::<Vec<_>>(),
            vec![7]
        );
        assert!(lookup_active_equality_point_literal(
            &db,
            &handle,
            &PropertyValue::String("missing".to_string()),
        )
        .await
        .expect("missing exact unique owner is empty")
        .is_empty());
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
                scan_active_range_generation_with_membership(&db, &handle, None, None, &[])
                    .await
                    .expect("exact range without membership succeeds"),
                expected_all
            );
            assert_eq!(
                scan_active_range_generation_with_membership(
                    &db,
                    &handle,
                    Some(&SecondaryRangeQuery::Lower {
                        value: PropertyValue::String("a".to_string()),
                        inclusive: false,
                    }),
                    None,
                    &[],
                )
                .await
                .expect("exact bounded range succeeds"),
                expected_gt
            );
            let membership = roaring::RoaringTreemap::from_iter([20, 30]);
            let second_filter = roaring::RoaringTreemap::from_iter([10, 20]);
            assert_eq!(
                scan_active_range_generation_with_membership(
                    &db,
                    &handle,
                    None,
                    None,
                    &[&membership, &second_filter],
                )
                .await
                .expect("exact range applies membership in encoded order"),
                vec![20]
            );
            assert_eq!(
                count_active_range_generation_with_membership(&db, &handle, None, None, &[])
                    .await
                    .expect("exact range count succeeds without owner materialization"),
                expected_all.len()
            );
            assert_eq!(
                count_active_range_generation_with_membership(
                    &db,
                    &handle,
                    None,
                    None,
                    &[&membership, &second_filter],
                )
                .await
                .expect("exact range count applies membership in encoded order"),
                1
            );
            assert_eq!(
                count_active_range_generation_with_membership(&db, &handle, None, Some(0), &[],)
                    .await
                    .expect("zero accepted-match threshold performs an empty count"),
                0
            );
            assert_eq!(
                count_active_range_generation_with_membership(&db, &handle, None, Some(2), &[],)
                    .await
                    .expect("bounded exact range count stops at its threshold"),
                expected_all.len().min(2)
            );
            assert_eq!(
                scan_active_range_generation(
                    &db,
                    &handle,
                    Some(&SecondaryRangeQuery::Lower {
                        value: PropertyValue::String("a".to_string()),
                        inclusive: false,
                    }),
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
                    Some(&SecondaryRangeQuery::Between {
                        lower: PropertyValue::String("a".to_string()),
                        lower_inclusive: false,
                        upper: PropertyValue::String("b".to_string()),
                        upper_inclusive: false,
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
                expected_all.iter().copied().take(1).collect::<Vec<_>>()
            );
            assert_eq!(
                scan_active_range_generation_with_membership(&db, &handle, None, Some(1), &[])
                    .await
                    .expect("exact range stops at its accepted-match threshold"),
                expected_all.iter().copied().take(1).collect::<Vec<_>>()
            );
            assert!(scan_active_range_generation(
                &db,
                &handle,
                Some(&SecondaryRangeQuery::Between {
                    lower: PropertyValue::String("b".to_string()),
                    lower_inclusive: true,
                    upper: PropertyValue::String("a".to_string()),
                    upper_inclusive: true,
                }),
                None,
            )
            .await
            .expect("reversed between bounds are empty")
            .is_empty());
            assert!(scan_active_range_generation_with_membership(
                &db,
                &handle,
                Some(&SecondaryRangeQuery::Between {
                    lower: PropertyValue::String("b".to_string()),
                    lower_inclusive: true,
                    upper: PropertyValue::String("a".to_string()),
                    upper_inclusive: true,
                }),
                None,
                &[],
            )
            .await
            .expect("exact reversed between bounds are empty")
            .is_empty());
            db.close().await.expect("range read fixture closes");
        }
    }

    #[tokio::test]
    async fn active_secondary_serving_rejects_malformed_bitmap_values() {
        let db = test_db("secondary-read-owner-mismatch").await;
        let handle = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        )
        .await;
        put_read_entry(&db, &handle, "same", 4).await;
        let definition = handle.secondary_definition().unwrap();
        db.put(
            secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                definition,
                CanonicalSecondaryValue::equality_string("same"),
                IndexEntityId::new(4),
            )
            .unwrap(),
            Bytes::from_static(b"not a portable bitmap"),
        )
        .await
        .unwrap();

        assert!(matches!(
            lookup_active_equality_generation(
                &db,
                &handle,
                &PropertyValue::String("same".to_string()),
            )
            .await,
            Err(HelixDbError::Encoding(
                crate::encoding::error::EncodingError::Io(_)
            ))
        ));
        assert!(matches!(
            lookup_active_equality_point_literal(
                &db,
                &handle,
                &PropertyValue::String("same".to_string()),
            )
            .await,
            Err(HelixDbError::Encoding(
                crate::encoding::error::EncodingError::Io(_)
            ))
        ));
        assert!(matches!(
            lookup_active_equality_literal_batch(
                &db,
                &handle,
                &[
                    PropertyValue::String("same".to_string()),
                    PropertyValue::String("same".to_string()),
                ],
            )
            .await,
            Err(HelixDbError::Encoding(
                crate::encoding::error::EncodingError::Io(_)
            ))
        ));
        db.close().await.expect("owner mismatch fixture closes");
    }

    #[tokio::test]
    async fn exact_secondary_primitives_reject_cross_family_and_unindexed_inputs() {
        let db = test_db("secondary-exact-primitive-contract-errors").await;
        let equality = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        )
        .await;
        let unique = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap(),
        )
        .await;
        let range = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        )
        .await;
        let vector = active_vector_read_handle(&db).await;

        for value in [PropertyValue::Null, PropertyValue::F64(f64::NAN)] {
            assert!(matches!(
                lookup_active_equality_point_literal(&db, &equality, &value).await,
                Err(HelixDbError::IndexCatalogCorruption(message))
                    if message.contains("non-indexed value")
            ));
        }
        assert!(matches!(
            lookup_active_equality_point_literal(
                &db,
                &range,
                &PropertyValue::String("a".to_string()),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("range definition")
        ));
        assert!(matches!(
            lookup_active_equality_literal_batch(
                &db,
                &equality,
                &[PropertyValue::String("only".to_string())],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("fewer than two")
        ));
        assert!(matches!(
            lookup_active_equality_literal_batch(
                &db,
                &unique,
                &[
                    PropertyValue::String("a".to_string()),
                    PropertyValue::String("b".to_string()),
                ],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("non-bitmap definition")
        ));
        assert!(matches!(
            lookup_active_equality_literal_batch(
                &db,
                &equality,
                &[
                    PropertyValue::String("a".to_string()),
                    PropertyValue::Null,
                ],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("non-indexed value")
        ));
        assert!(matches!(
            scan_active_range_generation_with_membership(&db, &equality, None, None, &[]).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("equality definition")
        ));
        for result in [
            lookup_active_equality_point_literal(
                &db,
                &vector,
                &PropertyValue::String("value".to_string()),
            )
            .await,
            lookup_active_equality_literal_batch(
                &db,
                &vector,
                &[
                    PropertyValue::String("a".to_string()),
                    PropertyValue::String("b".to_string()),
                ],
            )
            .await,
        ] {
            assert!(matches!(
                result,
                Err(HelixDbError::IndexCatalogCorruption(message))
                    if message.contains("non-secondary Active handle")
            ));
        }
        assert!(matches!(
            scan_active_range_generation_with_membership(&db, &vector, None, None, &[]).await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("non-secondary Active handle")
        ));
        let oversized = PropertyValue::Bytes(vec![0; 1_100_000]);
        let oversized_error = lookup_active_equality_point_literal(&db, &equality, &oversized)
            .await
            .unwrap_err();
        assert!(matches!(
            oversized_error,
            HelixDbError::SecondaryIndexValue(SecondaryIndexValueError::EncodedKeyTooLarge { .. })
        ));
        // A bound no range lane can order against is an empty range.
        assert_eq!(
            scan_active_range_generation_with_membership(
                &db,
                &range,
                Some(&SecondaryRangeQuery::Lower {
                    value: PropertyValue::Array(vec![PropertyValue::I64(1)]),
                    inclusive: true,
                }),
                None,
                &[],
            )
            .await
            .unwrap(),
            Vec::<u64>::new()
        );

        db.close()
            .await
            .expect("exact primitive contract fixture closes");
    }

    #[tokio::test]
    async fn exact_secondary_primitives_propagate_every_storage_boundary_failure() {
        let db = test_db("secondary-exact-storage-errors").await;
        let equality = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
        )
        .await;
        let get_failure = FailingExactRead {
            db: &db,
            failure: ExactReadFailure::Get,
        };
        assert!(lookup_active_equality_point_literal(
            &get_failure,
            &equality,
            &PropertyValue::String("same".to_string()),
        )
        .await
        .is_err());
        let multi_get_failure = FailingExactRead {
            db: &db,
            failure: ExactReadFailure::MultiGet,
        };
        assert!(lookup_active_equality_literal_batch(
            &multi_get_failure,
            &equality,
            &[
                PropertyValue::String("same".to_string()),
                PropertyValue::String("other".to_string()),
            ],
        )
        .await
        .is_err());

        let unique = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap(),
        )
        .await;
        let unique_definition = unique.secondary_definition().unwrap();
        db.put(
            secondary_entry_key(
                unique.scope(),
                unique.index_id(),
                unique.generation(),
                unique_definition,
                CanonicalSecondaryValue::equality_string("broken@example.com"),
                IndexEntityId::initial(),
            )
            .unwrap(),
            Bytes::from_static(b"malformed unique owner"),
        )
        .await
        .unwrap();
        assert!(lookup_active_equality_point_literal(
            &db,
            &unique,
            &PropertyValue::String("broken@example.com".to_string()),
        )
        .await
        .is_err());

        let range = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        )
        .await;
        let scan_failure = FailingExactRead {
            db: &db,
            failure: ExactReadFailure::Scan,
        };
        assert!(scan_active_range_generation_with_membership(
            &scan_failure,
            &range,
            None,
            None,
            &[],
        )
        .await
        .is_err());
        let malformed_key = Bytes::from_static(b"not-an-index-key");
        db.put(&malformed_key, Bytes::from_static(b"ignored"))
            .await
            .unwrap();
        assert!(scan_active_range_generation_with_membership(
            &ExactKeyScan {
                db: &db,
                key: malformed_key,
            },
            &range,
            None,
            None,
            &[],
        )
        .await
        .is_err());
        db.close().await.unwrap();

        let next_db = test_db("secondary-exact-next-error").await;
        let next_range = active_read_handle(
            &next_db,
            SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        )
        .await;
        put_read_entry(&next_db, &next_range, "a", 1).await;
        let next_failure = FailingExactRead {
            db: &next_db,
            failure: ExactReadFailure::Next,
        };
        assert!(scan_active_range_generation_with_membership(
            &next_failure,
            &next_range,
            None,
            None,
            &[],
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn exact_range_scan_rejects_adversarial_rows_and_stale_authority() {
        let db = test_db("secondary-exact-adversarial-range-rows").await;
        let handle = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        )
        .await;
        let other_kind = scoped_index_key(
            handle.scope(),
            ScopedKey::index_record(handle.identity().clone()),
        );
        db.put(&other_kind, Bytes::from_static(b"ignored"))
            .await
            .unwrap();
        assert!(matches!(
            scan_active_range_generation_with_membership(
                &ExactKeyScan {
                    db: &db,
                    key: other_kind,
                },
                &handle,
                None,
                None,
                &[],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("another key kind")
        ));

        let definition = handle.secondary_definition().unwrap();
        let escaped_key = secondary_entry_key(
            handle.scope(),
            handle.index_id().checked_next().unwrap(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::range_string(StorageRangeIndexDirection::Asc, "a"),
            IndexEntityId::new(1),
        )
        .unwrap();
        db.put(
            &escaped_key,
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id().checked_next().unwrap(),
                generation: handle.generation(),
                lane: definition_lane(definition),
                entity_id: IndexEntityId::new(1),
            }),
        )
        .await
        .unwrap();
        assert!(matches!(
            scan_active_range_generation_with_membership(
                &ExactKeyScan {
                    db: &db,
                    key: escaped_key,
                },
                &handle,
                None,
                None,
                &[],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("escaped its exact serving prefix")
        ));
        let generation_key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation().checked_next().unwrap(),
            definition,
            CanonicalSecondaryValue::range_string(StorageRangeIndexDirection::Asc, "a"),
            IndexEntityId::new(2),
        )
        .unwrap();
        db.put(
            &generation_key,
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation().checked_next().unwrap(),
                lane: definition_lane(definition),
                entity_id: IndexEntityId::new(2),
            }),
        )
        .await
        .unwrap();
        let equality_key = scoped_index_key(
            handle.scope(),
            ScopedKey::SecondaryEntry(
                SecondaryEntryKey::try_new(
                    handle.index_id(),
                    handle.generation(),
                    SecondaryEntryLane::NodeEquality,
                    CanonicalSecondaryValue::equality_string("a"),
                    Some(IndexEntityId::new(3)),
                )
                .unwrap(),
            ),
        );
        db.put(
            &equality_key,
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane: SecondaryEntryLane::NodeEquality,
                entity_id: IndexEntityId::new(3),
            }),
        )
        .await
        .unwrap();
        for key in [generation_key, equality_key] {
            assert!(matches!(
                scan_active_range_generation_with_membership(
                    &ExactKeyScan { db: &db, key },
                    &handle,
                    None,
                    None,
                    &[],
                )
                .await,
                Err(HelixDbError::IndexCatalogCorruption(message))
                    if message.contains("escaped its exact serving prefix")
            ));
        }
        db.close().await.unwrap();

        let mismatch_db = test_db("secondary-exact-range-owner-mismatch").await;
        let mismatch = active_read_handle(
            &mismatch_db,
            SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
        )
        .await;
        let mismatch_definition = mismatch.secondary_definition().unwrap();
        let mismatch_key = secondary_entry_key(
            mismatch.scope(),
            mismatch.index_id(),
            mismatch.generation(),
            mismatch_definition,
            CanonicalSecondaryValue::range_string(StorageRangeIndexDirection::Asc, "a"),
            IndexEntityId::new(1),
        )
        .unwrap();
        mismatch_db
            .put(
                mismatch_key.clone(),
                encode_secondary_entry(&SecondaryEntryValue {
                    index_id: mismatch.index_id(),
                    generation: mismatch.generation(),
                    lane: definition_lane(mismatch_definition),
                    entity_id: IndexEntityId::new(2),
                }),
            )
            .await
            .unwrap();
        assert!(matches!(
            scan_active_range_generation_with_membership(
                &mismatch_db,
                &mismatch,
                None,
                None,
                &[],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("key/value owners disagree")
        ));
        mismatch_db
            .put(mismatch_key, Bytes::from_static(b"malformed range owner"))
            .await
            .unwrap();
        assert!(scan_active_range_generation_with_membership(
            &mismatch_db,
            &mismatch,
            None,
            None,
            &[],
        )
        .await
        .is_err());
        mismatch_db.close().await.unwrap();

        for (database, authoritative) in [
            (
                "secondary-exact-range-stale-authority",
                encode_properties(&[
                    Property::string("$label", "User"),
                    Property::string("rank", "different"),
                ]),
            ),
            (
                "secondary-exact-range-malformed-authority",
                Bytes::from_static(b"malformed authoritative properties"),
            ),
        ] {
            let authority_db = test_db(database).await;
            let authority = active_read_handle(
                &authority_db,
                SecondaryIndexDefinition::node_range("User", "rank").unwrap(),
            )
            .await;
            put_read_entry(&authority_db, &authority, "a", 7).await;
            authority_db
                .put(
                    authoritative_property_key(
                        authority.scope(),
                        IndexEntity {
                            kind: IndexElementKind::Node,
                            id: IndexEntityId::new(7),
                        },
                    ),
                    authoritative,
                )
                .await
                .unwrap();
            let result = scan_active_range_generation_with_membership(
                &authority_db,
                &authority,
                None,
                None,
                &[],
            )
            .await;
            if database.contains("stale") {
                assert!(result.unwrap().is_empty());
            } else {
                assert!(result.is_err());
            }
            authority_db.close().await.unwrap();
        }
    }

    /// Covers the pure range-bound and checked-arithmetic contracts that lifecycle integration tests otherwise exercise only
    /// through their successful branches.
    #[test]
    fn secondary_helper_boundaries_are_total_and_typed() {
        assert_eq!(
            format!(
                "{:?}",
                SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()))
            ),
            "SecondaryIndexDriver { .. }"
        );

        // Bounds no range lane can encode or order are empty ranges.
        for query in [
            SecondaryRangeQuery::Lower {
                value: PropertyValue::Null,
                inclusive: true,
            },
            SecondaryRangeQuery::Upper {
                value: PropertyValue::Bool(true),
                inclusive: false,
            },
            SecondaryRangeQuery::Lower {
                value: PropertyValue::F64(f64::NAN),
                inclusive: false,
            },
            SecondaryRangeQuery::Between {
                lower: PropertyValue::I64(1),
                lower_inclusive: true,
                upper: PropertyValue::String("a".to_string()),
                upper_inclusive: true,
            },
            SecondaryRangeQuery::Between {
                lower: PropertyValue::Null,
                lower_inclusive: true,
                upper: PropertyValue::I64(5),
                upper_inclusive: true,
            },
        ] {
            for direction in [
                StorageRangeIndexDirection::Asc,
                StorageRangeIndexDirection::Desc,
            ] {
                assert!(secondary_range_scan_bounds(direction, &query)
                    .unwrap()
                    .is_none());
            }
        }

        for direction in [
            StorageRangeIndexDirection::Asc,
            StorageRangeIndexDirection::Desc,
        ] {
            for query in [
                SecondaryRangeQuery::Lower {
                    value: PropertyValue::String("a".to_string()),
                    inclusive: false,
                },
                SecondaryRangeQuery::Lower {
                    value: PropertyValue::String("a".to_string()),
                    inclusive: true,
                },
                SecondaryRangeQuery::Upper {
                    value: PropertyValue::String("b".to_string()),
                    inclusive: false,
                },
                SecondaryRangeQuery::Upper {
                    value: PropertyValue::String("b".to_string()),
                    inclusive: true,
                },
                SecondaryRangeQuery::Between {
                    lower: PropertyValue::String("a".to_string()),
                    lower_inclusive: true,
                    upper: PropertyValue::String("b".to_string()),
                    upper_inclusive: true,
                },
                SecondaryRangeQuery::Between {
                    lower: PropertyValue::String("a".to_string()),
                    lower_inclusive: false,
                    upper: PropertyValue::String("b".to_string()),
                    upper_inclusive: false,
                },
            ] {
                assert!(secondary_range_scan_bounds(direction, &query)
                    .unwrap()
                    .is_some());
            }
        }
        assert!(secondary_range_scan_bounds(
            StorageRangeIndexDirection::Asc,
            &SecondaryRangeQuery::Between {
                lower: PropertyValue::String("b".to_string()),
                lower_inclusive: true,
                upper: PropertyValue::String("a".to_string()),
                upper_inclusive: true,
            },
        )
        .unwrap()
        .is_none());
        assert!(secondary_range_scan_bounds(
            StorageRangeIndexDirection::Desc,
            &SecondaryRangeQuery::Between {
                lower: PropertyValue::String("same".to_string()),
                lower_inclusive: false,
                upper: PropertyValue::String("same".to_string()),
                upper_inclusive: true,
            },
        )
        .unwrap()
        .is_none());

        assert_eq!(checked_add(2, 3, "fixture").unwrap(), 5);
        assert!(matches!(
            checked_add(u64::MAX, 1, "fixture"),
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message == "secondary fixture overflowed"
        ));
        assert!(matches!(
            operation_error(crate::index_lifecycle::IndexOperationModelError::ZeroClaimSequence),
            HelixDbError::InvariantViolation(message)
                if message.contains("claim sequence must be non-zero")
        ));

        let non_unique =
            validated(SecondaryIndexDefinition::node_equality("User", "value").unwrap());
        let ValidatedDynamicIndexDefinition::Secondary(non_unique) = non_unique else {
            unreachable!("secondary fixture is type-checked")
        };
        assert!(matches!(
            mutation_value_error(
                &non_unique,
                IndexEntityId::new(7),
                SecondaryValueError::UnsupportedEquality("Array"),
            ),
            HelixDbError::SecondaryIndexValue(SecondaryIndexValueError::UnsupportedEqualityValue {
                value_type: "Array"
            })
        ));

        let unique =
            validated(SecondaryIndexDefinition::node_unique_equality("User", "value").unwrap());
        let ValidatedDynamicIndexDefinition::Secondary(unique) = unique else {
            unreachable!("secondary fixture is type-checked")
        };
        assert!(matches!(
            mutation_value_error(
                &unique,
                IndexEntityId::new(8),
                SecondaryValueError::UnsupportedEquality("Object"),
            ),
            HelixDbError::UnsupportedUniqueIndexValueType {
                node_id: 8,
                value_type,
                ..
            } if value_type == "Object"
        ));
    }

    /// Proves both old-value and new-value validation failures stop before a
    /// graph transaction stages any secondary mutation.
    #[tokio::test]
    async fn mutation_rejects_unsupported_values_on_both_sides() {
        let db = test_db("secondary-unsupported-mutation-values").await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("secondary mutation transaction begins");
        let definition =
            validated(SecondaryIndexDefinition::node_equality("User", "value").unwrap());
        let ValidatedDynamicIndexDefinition::Secondary(definition) = definition else {
            unreachable!("secondary fixture is type-checked")
        };
        let mutations = SecondaryMutationSet {
            targets: vec![SecondaryMutationTarget {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                definition,
                mode: SecondaryMutationMode::MaintainActive,
            }],
        };
        let unsupported = vec![
            Property::string("$label", "User"),
            Property::new("value", PropertyValue::Array(vec![PropertyValue::I64(1)])),
        ];
        assert!(matches!(
            maintain_entity(
                &transaction,
                DataScope::LegacyUnscoped,
                &mutations,
                IndexElementKind::Node,
                9,
                &unsupported,
                &[],
            )
            .await,
            Err(HelixDbError::SecondaryIndexValue(
                SecondaryIndexValueError::UnsupportedEqualityValue {
                    value_type: "Array"
                }
            ))
        ));
        assert!(matches!(
            maintain_entity(
                &transaction,
                DataScope::LegacyUnscoped,
                &mutations,
                IndexElementKind::Node,
                9,
                &[],
                &unsupported,
            )
            .await,
            Err(HelixDbError::SecondaryIndexValue(
                SecondaryIndexValueError::UnsupportedEqualityValue {
                    value_type: "Array"
                }
            ))
        ));
        drop(transaction);
        db.close().await.expect("secondary test database closes");
    }

    /// Proves missing and unsupported authoritative rows fail closed while a
    /// unique generation releases its applied-state validation rows.
    #[tokio::test]
    async fn source_and_unique_validation_corruption_are_typed() {
        let db = test_db("secondary-source-validation-corruption").await;
        let scope = DataScope::LegacyUnscoped;
        let definition =
            validated(SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap());
        let ValidatedDynamicIndexDefinition::Secondary(secondary_definition) = &definition else {
            unreachable!("secondary fixture is type-checked")
        };
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &user_properties("owner@example.com"),
        )
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id)
            .await
            .unwrap()
            .expect("secondary build operation remains durable");
        let progress = PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        };

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction
            .delete(source_key(scope, IndexElementKind::Node, 0))
            .unwrap();
        transaction.commit().await.unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            validate_and_release_applied(
                &transaction,
                scope,
                &operation,
                secondary_definition,
                &progress,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("source row disappeared")
        ));
        drop(transaction);

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction
            .put(
                source_key(scope, IndexElementKind::Node, 0),
                encode_properties(&[
                    Property::string("$label", "User"),
                    Property::new("email", PropertyValue::Array(vec![PropertyValue::I64(1)])),
                ]),
            )
            .unwrap();
        transaction.commit().await.unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            validate_and_release_applied(
                &transaction,
                scope,
                &operation,
                secondary_definition,
                &progress,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(message))
                if message.contains("source is unsupported")
        ));
        drop(transaction);
        db.close().await.expect("secondary test database closes");
    }

    /// Storage writes keys of at most `u16::MAX` bytes and a tenant envelope
    /// takes 17 of them, so the longest indexable string depends on the scope.
    #[test]
    fn canonical_values_fit_the_storage_key_limit_of_their_scope() {
        let tenant = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
        for (definition, unscoped) in [
            (
                SecondaryIndexDefinition::node_equality("User", "email"),
                65_499,
            ),
            (
                SecondaryIndexDefinition::node_unique_equality("User", "email"),
                65_499,
            ),
            (
                SecondaryIndexDefinition::node_range("User", "email"),
                65_505,
            ),
        ] {
            let ValidatedDynamicIndexDefinition::Secondary(definition) =
                validated(definition.expect("test definition is valid"))
            else {
                unreachable!("test definition is secondary");
            };
            for (scope, longest) in [
                (DataScope::LegacyUnscoped, unscoped),
                (tenant, unscoped - 17),
            ] {
                let project = |length: usize| {
                    storable_value(
                        scope,
                        IndexId::initial(),
                        IndexGenerationId::initial(),
                        &definition,
                        &user_properties(&"x".repeat(length)),
                        IndexEntityId::initial(),
                    )
                };
                assert!(matches!(project(longest), Ok(Some(_))));
                assert!(matches!(
                    project(longest + 1),
                    Err(SecondaryValueError::Oversized {
                        encoded_len,
                        maximum: STORAGE_KEY_MAX_LEN,
                    }) if encoded_len == STORAGE_KEY_MAX_LEN + 1
                ));
            }
        }
    }

    fn user_properties(value: &str) -> Vec<Property> {
        vec![
            Property::string("$label", "User"),
            Property::string("email", value),
        ]
    }

    fn source_key(scope: DataScope, kind: IndexElementKind, entity_id: u64) -> Bytes {
        match kind {
            IndexElementKind::Node => DataKey::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
            }
            .to_bytes(),
            IndexElementKind::Edge => DataKey::Data {
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
        let now_unix_millis = NOW_MILLIS
            .checked_add(*claim_sequence)
            .expect("test lifecycle clock remains bounded");
        drive_one_at(
            db,
            driver,
            operation_id,
            claim_sequence,
            limits,
            now_unix_millis,
        )
        .await
    }

    async fn drive_one_at(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        limits: SearchIndexBatchLimits,
        mut now_unix_millis: u64,
    ) -> CommittedOperationStep {
        let writer_epoch = WriterEpoch::from_bytes([0x5A; 16]).expect("writer epoch is non-nil");
        let eligible = loop {
            let observation =
                observe_operation_pointer(db, operation_id, writer_epoch, now_unix_millis)
                    .await
                    .expect("operation pointer is readable");
            match observation {
                OperationPointerObservation::Eligible(eligible) => break eligible,
                OperationPointerObservation::Delayed { delay_millis } => {
                    now_unix_millis = now_unix_millis
                        .checked_add(delay_millis)
                        .expect("test lifecycle clock remains bounded");
                }
                observation @ OperationPointerObservation::ClaimedByCurrentWriter(_)
                | observation @ OperationPointerObservation::StalePointerRemoved => {
                    panic!("queued secondary operation must be eligible: {observation:?}");
                }
            }
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

    async fn drive_until_catch_up(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
    ) -> IndexOperationRecord {
        drive_until_stage(db, driver, operation_id, claim_sequence, |stage| {
            matches!(stage, SecondaryBuildStage::CatchUp(_))
        })
        .await;
        read_operation(db, DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("secondary operation is readable")
            .expect("secondary operation exists")
    }

    fn one_entity_limits() -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(32).expect("operation limit is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
        )
        .expect("single-entity test limits are internally consistent")
    }

    fn one_plan_output_limits() -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(3).expect("three is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(2).expect("two writes are positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
        )
        .expect("one-plan output limits are internally consistent")
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
            .expect("canonical secondary row is readable")
            .expect("canonical secondary row exists");
        decode_index_record(&value).expect("canonical secondary row decodes")
    }

    async fn generation_rows(
        db: &Db,
        scope: DataScope,
        kind: RecordKind,
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
                RecordKind::SecondaryEqualityBitmap,
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
            RecordKind::SecondaryEqualityBitmap,
            index_id,
            generation,
        )
        .await
        .is_empty());
        db.close().await.expect("secondary test database closes");
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
            let bitmap = matches!(
                definition,
                ValidatedDynamicIndexDefinition::Secondary(
                    ValidatedSecondaryIndexDefinition::NodeEquality { unique: false, .. }
                        | ValidatedSecondaryIndexDefinition::EdgeEquality { .. }
                )
            );
            let rows = generation_rows(
                &db,
                scope,
                if bitmap {
                    RecordKind::SecondaryEqualityBitmap
                } else {
                    RecordKind::SecondaryEntry
                },
                index_id,
                generation,
            )
            .await;
            assert_eq!(rows.len(), 1);
            match IndexKey::parse_from_slice(scope, &rows[0].0)
                .expect("generation-qualified secondary entry key decodes")
            {
                IndexKey::Data {
                    kind: ScopedKey::SecondaryEntry(key),
                    ..
                } => assert_eq!(key.lane(), expected_lane),
                IndexKey::Data {
                    kind: ScopedKey::SecondaryEqualityBitmap(key),
                    ..
                } if bitmap => assert_eq!(key.element_kind, identity.element_kind()),
                IndexKey::Global { .. } | IndexKey::Data { .. } => {
                    panic!("secondary entry prefix contains its exact typed family")
                }
            }
            db.close().await.expect("secondary shape database closes");
        }
    }

    #[tokio::test]
    async fn shared_edge_equality_builds_one_bitmap_and_serves_one_point_read() {
        crate::index_lifecycle::secondary::EqualityReadObserver::default()
            .scope(async {
                let db = test_db("secondary-shared-edge-bitmap-read").await;
                let scope =
                    DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(
                        0xFD00_0000_0000_0000_0000_0000_0000_0007,
                    ));
                let definition = validated(
                    SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind")
                        .expect("edge equality definition validates"),
                );
                for edge_id in 0..8 {
                    put_source(
                        &db,
                        scope,
                        IndexElementKind::Edge,
                        edge_id,
                        &[
                            Property::string("$label", "FOLLOWS"),
                            Property::string("kind", "shared"),
                        ],
                    )
                    .await;
                }
                let (operation_id, index_id, generation) =
                    create_build(&db, scope, &definition, 7).await;
                let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
                let mut claim_sequence = 1;
                assert_eq!(
                    drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                    CommittedOperationStep::Completed
                );
                let rows = generation_rows(
                    &db,
                    scope,
                    RecordKind::SecondaryEqualityBitmap,
                    index_id,
                    generation,
                )
                .await;
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    SecondaryEqualityBitmapValue::decode(&rows[0].1)
                        .expect("edge equality bitmap decodes")
                        .ids()
                        .iter()
                        .collect::<Vec<_>>(),
                    (0..8).collect::<Vec<_>>()
                );
                let active = read_index(&db, scope, &definition).await;
                let handle = ActiveIndexHandle::try_from_record(scope, &active)
                    .expect("active edge equality handle projects");
                reset_equality_read_metrics();
                assert_eq!(
                    lookup_active_equality_generation(
                        &db,
                        &handle,
                        &PropertyValue::String("shared".to_string()),
                    )
                    .await
                    .expect("edge equality bitmap lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                    (0..8).collect::<Vec<_>>()
                );
                assert_eq!(
                    equality_read_metrics(),
                    SecondaryEqualityReadMetrics {
                        point_reads: 1,
                        multi_get_calls: 0,
                        scans: 0,
                        graph_reads: 0,
                    }
                );
                db.close().await.expect("edge bitmap database closes");
            })
            .await
    }

    #[tokio::test]
    async fn bitmap_reconciliation_persists_physical_and_applied_state_together() {
        let db = test_db("secondary-bitmap-reconciliation-state").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "status")
                .expect("node equality definition validates"),
        );
        let ValidatedDynamicIndexDefinition::Secondary(definition) = definition else {
            unreachable!("secondary fixture is type-checked")
        };
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::initial(),
        };
        let applied_key = scoped_index_key(
            scope,
            ScopedKey::AppliedState(IndexEntityStateKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                entity,
            }),
        );
        let next = CanonicalSecondaryValue::equality_string("ready");
        let ReconciliationPlan::Writes(plan) = reconciliation_plan_from_observations(
            scope,
            IndexId::initial(),
            IndexGenerationId::initial(),
            &definition,
            entity,
            applied_key.clone(),
            &HeldValue::Unconfirmed(None),
            Some(&next),
            &BTreeMap::new(),
            AppliedStateDisposition::Retain,
        )
        .expect("bitmap reconciliation plan validates") else {
            panic!("fresh bitmap membership cannot be blocked")
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("bitmap reconciliation transaction begins");
        plan.stage(&transaction)
            .await
            .expect("bitmap reconciliation stages");
        transaction
            .commit()
            .await
            .expect("bitmap reconciliation commits");

        assert!(db
            .get(&applied_key)
            .await
            .expect("applied state is readable")
            .is_some());
        let bitmap_key = secondary_entry_key(
            scope,
            IndexId::initial(),
            IndexGenerationId::initial(),
            &definition,
            next,
            entity.id,
        )
        .expect("bitmap key validates");
        assert_eq!(
            SecondaryEqualityBitmapValue::decode(
                &db.get(bitmap_key)
                    .await
                    .expect("bitmap is readable")
                    .expect("bitmap exists"),
            )
            .expect("bitmap decodes")
            .ids()
            .iter()
            .collect::<Vec<_>>(),
            vec![entity.id.get()]
        );
        db.close()
            .await
            .expect("bitmap reconciliation database closes");
    }

    #[tokio::test]
    async fn bitmap_source_scan_retains_applied_state_for_catch_up() {
        let db = test_db("secondary-bitmap-source-scan-state").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "status")
                .expect("node equality definition validates"),
        );
        put_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &[
                Property::string("$label", "User"),
                Property::string("status", "ready"),
            ],
        )
        .await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            generation_rows(&db, scope, RecordKind::AppliedState, index_id, generation,)
                .await
                .len(),
            1
        );
        db.close()
            .await
            .expect("bitmap source scan database closes");
    }

    #[tokio::test]
    async fn removal_bearing_bitmap_changes_merge_or_tombstone() {
        let db = test_db("secondary-mixed-bitmap-changes").await;
        let handle = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_equality("User", "status")
                .expect("node equality definition validates"),
        )
        .await;
        let definition = handle
            .secondary_definition()
            .expect("secondary handle retains its definition");
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::equality_string("shared"),
            IndexEntityId::initial(),
        )
        .expect("bitmap key validates");
        db.put(
            &key,
            SecondaryEqualityBitmapValue::new(roaring::RoaringTreemap::from_iter([1, 2])).encode(),
        )
        .await
        .expect("initial bitmap persists");

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("mixed bitmap transaction begins");
        stage_bitmap_changes(
            &transaction,
            &BTreeMap::from([(key.clone(), BTreeMap::from([(1, false), (3, true)]))]),
        )
        .await
        .expect("mixed removal and addition stages");
        transaction
            .commit()
            .await
            .expect("mixed bitmap transaction commits");
        assert_eq!(
            SecondaryEqualityBitmapValue::decode(
                &db.get(&key)
                    .await
                    .expect("replacement bitmap is readable")
                    .expect("replacement bitmap remains present"),
            )
            .expect("replacement bitmap decodes")
            .ids()
            .iter()
            .collect::<Vec<_>>(),
            vec![2, 3]
        );

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("bitmap deletion transaction begins");
        stage_bitmap_changes(
            &transaction,
            &BTreeMap::from([(key.clone(), BTreeMap::from([(2, false), (3, false)]))]),
        )
        .await
        .expect("complete removal stages");
        transaction
            .commit()
            .await
            .expect("bitmap deletion transaction commits");
        assert_eq!(
            db.get(&key).await.expect("deleted bitmap key is readable"),
            None
        );

        db.close().await.expect("mixed bitmap database closes");
    }

    #[tokio::test]
    async fn corrupt_node_and_edge_equality_rows_fail_closed() {
        for (kind, definition) in [
            (
                "node",
                SecondaryIndexDefinition::node_equality("User", "status")
                    .expect("node equality definition validates"),
            ),
            (
                "edge",
                SecondaryIndexDefinition::edge_equality("FOLLOWS", "status")
                    .expect("edge equality definition validates"),
            ),
        ] {
            let db = test_db(&format!("secondary-corrupt-{kind}-equality")).await;
            let handle = active_read_handle(&db, definition).await;
            let key = secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                handle
                    .secondary_definition()
                    .expect("secondary handle retains its definition"),
                CanonicalSecondaryValue::equality_string("corrupt"),
                IndexEntityId::initial(),
            )
            .expect("equality bitmap key validates");
            let corrupt = Bytes::from_static(b"corrupt-equality-bitmap");
            db.put(&key, corrupt.clone()).await.unwrap();

            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            assert!(
                stage_bitmap_changes(
                    &transaction,
                    &BTreeMap::from([(key.clone(), BTreeMap::from([(1, false)]))]),
                )
                .await
                .is_err(),
                "{kind} equality corruption"
            );
            transaction.rollback();
            assert_eq!(
                db.get(&key).await.unwrap(),
                Some(corrupt),
                "{kind} equality corruption"
            );
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn pure_bitmap_additions_commit_as_disjoint_merges() {
        let db = test_db("secondary-commutative-bitmap-additions").await;
        let handle = active_read_handle(
            &db,
            SecondaryIndexDefinition::node_equality("User", "status")
                .expect("node equality definition validates"),
        )
        .await;
        let definition = handle
            .secondary_definition()
            .expect("secondary handle retains its definition");
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            CanonicalSecondaryValue::equality_string("shared"),
            IndexEntityId::initial(),
        )
        .expect("bitmap key validates");
        db.put(
            &key,
            SecondaryEqualityBitmapValue::new(roaring::RoaringTreemap::from_iter([1])).encode(),
        )
        .await
        .expect("initial bitmap persists");

        let left = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("left bitmap transaction begins");
        let right = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("right bitmap transaction begins");
        stage_bitmap_changes(
            &left,
            &BTreeMap::from([(key.clone(), BTreeMap::from([(2, true)]))]),
        )
        .await
        .expect("left addition stages");
        stage_bitmap_changes(
            &right,
            &BTreeMap::from([(key.clone(), BTreeMap::from([(3, true)]))]),
        )
        .await
        .expect("right addition stages");

        left.commit().await.expect("left addition commits");
        right.commit().await.expect("right addition commits");
        assert_eq!(
            SecondaryEqualityBitmapValue::decode(
                &db.get(&key)
                    .await
                    .expect("merged bitmap is readable")
                    .expect("merged bitmap remains present"),
            )
            .expect("merged bitmap decodes")
            .ids()
            .iter()
            .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        db.close()
            .await
            .expect("commutative bitmap database closes");
    }

    #[tokio::test]
    async fn node_and_edge_equality_bitmap_races_respect_logical_members() {
        let fixtures = [
            (
                "node",
                SecondaryIndexDefinition::node_equality("User", "status").unwrap(),
            ),
            (
                "edge",
                SecondaryIndexDefinition::edge_equality("FOLLOWS", "status").unwrap(),
            ),
        ];
        for (kind, fixture) in fixtures {
            let facade = crate::HelixDB::open(crate::HelixDbSource::InMemory {
                database: format!("default-secondary-{kind}-races"),
            })
            .await
            .unwrap();
            let db = facade.inner_db();
            let handle = active_read_handle(&db, fixture).await;
            let definition = handle.secondary_definition().unwrap();

            for (left_present, right_present) in [(true, true), (false, false), (true, false)] {
                for left_first in [false, true] {
                    for overlap in [false, true] {
                        let key = secondary_entry_key(
                            handle.scope(),
                            handle.index_id(),
                            handle.generation(),
                            definition,
                            CanonicalSecondaryValue::equality_string(&format!(
                                "{left_present}-{right_present}-{left_first}-{overlap}"
                            )),
                            IndexEntityId::initial(),
                        )
                        .unwrap();
                        let right_id = if overlap { 1 } else { 2 };
                        let mut expected = roaring::RoaringTreemap::new();
                        for (id, present) in [(1, left_present), (right_id, right_present)] {
                            if !present {
                                expected.insert(id);
                            }
                        }
                        db.put(
                            &key,
                            SecondaryEqualityBitmapValue::new(expected.clone()).encode(),
                        )
                        .await
                        .unwrap();
                        // Direct commits with overlapping snapshots: no automatic retry.
                        let left = db
                            .begin(IsolationLevel::SerializableSnapshot)
                            .await
                            .unwrap();
                        let right = db
                            .begin(IsolationLevel::SerializableSnapshot)
                            .await
                            .unwrap();
                        for (txn, id, present) in
                            [(&left, 1, left_present), (&right, right_id, right_present)]
                        {
                            stage_bitmap_changes(
                                txn,
                                &BTreeMap::from([(key.clone(), BTreeMap::from([(id, present)]))]),
                            )
                            .await
                            .unwrap();
                        }
                        let (first, second, first_change, second_change) = if left_first {
                            (left, right, (1, left_present), (right_id, right_present))
                        } else {
                            (right, left, (right_id, right_present), (1, left_present))
                        };
                        first.commit().await.unwrap();
                        let outcome = second.commit().await;
                        if overlap {
                            assert_eq!(
                                outcome.unwrap_err().kind(),
                                slatedb::ErrorKind::Transaction
                            );
                        } else {
                            outcome.expect("different equality members must not conflict");
                        }
                        for (id, present) in
                            [Some(first_change), (!overlap).then_some(second_change)]
                                .into_iter()
                                .flatten()
                        {
                            if present {
                                expected.insert(id);
                            } else {
                                expected.remove(id);
                            }
                        }
                        let actual = db
                            .get(&key)
                            .await
                            .unwrap()
                            .map(|bytes| {
                                SecondaryEqualityBitmapValue::decode(&bytes)
                                    .unwrap()
                                    .into_ids()
                            })
                            .unwrap_or_default();
                        assert_eq!(
                            actual, expected,
                            "{kind}: overlap={overlap}, left_first={left_first}"
                        );
                    }
                }
            }
            facade.close().await.unwrap();
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
        let one_entity_limits = one_entity_limits();

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
                RecordKind::SecondaryEqualityBitmap,
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
                RecordKind::SecondaryEqualityBitmap,
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
            generation_rows(
                &db,
                scope,
                RecordKind::SecondaryEqualityBitmap,
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
                    RecordKind::SecondaryEqualityBitmap,
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
    async fn source_scan_stages_only_the_output_fitting_candidate_prefix() {
        let db = test_db("secondary-output-prefix-source-scan").await;
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
                &user_properties(&format!("prefix-{entity_id}@example.com")),
            )
            .await;
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;

        assert_eq!(
            drive_one_with_limits(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                one_plan_output_limits(),
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id)
            .await
            .expect("prefix-bounded operation is readable")
            .expect("prefix-bounded operation exists");
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
                RecordKind::SecondaryEqualityBitmap,
                index_id,
                generation,
            )
            .await
            .len(),
            1
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn exact_catch_up_does_not_conflict_with_a_different_entity_delta() {
        let db = test_db("secondary-exact-catch-up-different-entity").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let first = user_properties("first@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("first entity records a build delta");

        let prepared = PreparedSecondaryOperationStep {
            step: PreparedSecondaryStep::ExactCatchUp {
                keys: vec![IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::initial(),
                    },
                }],
            },
        };
        let catch_up = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("catch-up transaction begins");
        prepared
            .stage(&catch_up, scope, &operation, one_entity_limits())
            .await
            .expect("exact catch-up stages");

        let concurrent = user_properties("concurrent@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 1, &[], &concurrent)
            .await
            .expect("different entity mutation commits during catch-up");
        catch_up
            .commit()
            .await
            .expect("exact reads do not conflict with a different delta key");
        assert_eq!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .len(),
            1
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn exact_catch_up_conflicts_with_a_concurrent_same_entity_change() {
        let db = test_db("secondary-exact-catch-up-same-entity").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let first = user_properties("first@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("first update records a build delta");
        let prepared = PreparedSecondaryOperationStep {
            step: PreparedSecondaryStep::ExactCatchUp {
                keys: vec![IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::initial(),
                    },
                }],
            },
        };
        let catch_up = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("catch-up transaction begins");
        prepared
            .stage(&catch_up, scope, &operation, one_entity_limits())
            .await
            .expect("exact catch-up stages");

        let raced = user_properties("raced@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &updated, &raced)
            .await
            .expect("same entity races after catch-up reads");
        assert_eq!(
            catch_up
                .commit()
                .await
                .expect_err("stale same-entity reconciliation must conflict")
                .kind(),
            slatedb::ErrorKind::Transaction
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn abandoned_exact_preparation_leaves_all_durable_work_recoverable() {
        let db = test_db("secondary-abandoned-exact-preparation").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let first = user_properties("first@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("update records one build delta");
        let prepared = PreparedSecondaryOperationStep {
            step: PreparedSecondaryStep::ExactCatchUp {
                keys: vec![IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::initial(),
                    },
                }],
            },
        };
        drop(prepared);
        assert_eq!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .len(),
            1
        );
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .is_empty()
        );
        db.close().await.expect("secondary test database closes");
    }

    /// Drives one build until its durable stage satisfies `reached`.
    async fn drive_until_stage(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        claim_sequence: &mut u64,
        reached: fn(&SecondaryBuildStage) -> bool,
    ) {
        for _ in 0..8 {
            if reached(&build_stage(db, operation_id).await) {
                return;
            }
            assert_eq!(
                drive_one(db, driver, operation_id, claim_sequence).await,
                CommittedOperationStep::Progressed
            );
        }
        panic!("secondary build did not reach the expected stage")
    }

    /// Returns the durable build stage of one operation.
    async fn build_stage(db: &Db, operation_id: IndexOperationId) -> SecondaryBuildStage {
        let operation = read_operation(db, DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("secondary operation is readable")
            .expect("secondary operation exists");
        let IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(stage)) =
            operation.progress()
        else {
            panic!("secondary build retains constructing progress");
        };
        stage.clone()
    }

    /// Asserts an Active generation holds exactly the indexed values of
    /// `sources`, with no build delta or applied-state row left behind.
    async fn assert_generation_equals_source(
        db: &Db,
        scope: DataScope,
        definition: &ValidatedDynamicIndexDefinition,
        index_id: IndexId,
        generation: IndexGenerationId,
        sources: &BTreeMap<u64, Vec<Property>>,
    ) {
        assert!(matches!(
            read_index(db, scope, definition).await.state(),
            IndexStateV2::Active { .. }
        ));
        for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
            assert!(
                generation_rows(db, scope, kind, index_id, generation)
                    .await
                    .is_empty(),
                "{kind:?} rows outlive activation"
            );
        }
        let ValidatedDynamicIndexDefinition::Secondary(secondary) = definition else {
            unreachable!("secondary fixture is type-checked")
        };
        let mut expected = BTreeMap::<Bytes, BTreeSet<u64>>::new();
        for (&entity_id, properties) in sources {
            let entity = IndexEntityId::new(entity_id);
            let Some(value) = canonical_value(secondary, properties, entity)
                .expect("fixture source values are indexable")
            else {
                continue;
            };
            let key = secondary_entry_key(scope, index_id, generation, secondary, value, entity)
                .expect("fixture entry key validates");
            expected.entry(key).or_default().insert(entity_id);
        }
        let mut actual = BTreeMap::<Bytes, BTreeSet<u64>>::new();
        if definition_uses_equality_bitmap(secondary) {
            for (key, value) in generation_rows(
                db,
                scope,
                RecordKind::SecondaryEqualityBitmap,
                index_id,
                generation,
            )
            .await
            {
                let ids = SecondaryEqualityBitmapValue::decode(&value)
                    .expect("generation bitmap decodes")
                    .into_ids();
                if !ids.is_empty() {
                    actual.insert(key, ids.iter().collect());
                }
            }
        } else {
            for (key, value) in
                generation_rows(db, scope, RecordKind::SecondaryEntry, index_id, generation).await
            {
                let owner = decode_secondary_entry_value(
                    index_id,
                    generation,
                    definition_lane(secondary),
                    &value,
                )
                .expect("generation entry decodes");
                actual.entry(key).or_default().insert(owner.get());
            }
        }
        assert_eq!(actual, expected, "the generation diverged from its source");
    }

    /// Writes before every step used to send each gated step back to
    /// catch-up. Each step now drains the deltas it observes, so the build
    /// activates within a fixed number of steps for every lane.
    #[tokio::test]
    async fn builds_activate_while_a_fresh_write_precedes_every_step() {
        for (ordinal, definition) in [
            SecondaryIndexDefinition::node_equality("User", "email"),
            SecondaryIndexDefinition::node_unique_equality("User", "email"),
            SecondaryIndexDefinition::node_range("User", "email"),
        ]
        .into_iter()
        .enumerate()
        {
            let db = test_db(&format!("secondary-sustained-writes-{ordinal}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(definition.expect("test definition is valid"));
            let mut sources = BTreeMap::new();
            for entity_id in 0..4 {
                let properties = user_properties(&format!("seed-{entity_id}@example.com"));
                put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
                sources.insert(entity_id, properties);
            }
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 3).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;

            let mut activated = false;
            for step in 0_u64..12 {
                let inserted = 4 + step;
                let properties = user_properties(&format!("inserted-{step}@example.com"));
                mutate_source(
                    &db,
                    scope,
                    IndexElementKind::Node,
                    inserted,
                    &[],
                    &properties,
                )
                .await
                .expect("insert records a build delta");
                sources.insert(inserted, properties);
                let updated = step % 4;
                let after = user_properties(&format!("updated-{step}@example.com"));
                mutate_source(
                    &db,
                    scope,
                    IndexElementKind::Node,
                    updated,
                    &sources[&updated],
                    &after,
                )
                .await
                .expect("update records a build delta");
                sources.insert(updated, after);
                match drive_one(&db, &driver, operation_id, &mut claim_sequence).await {
                    CommittedOperationStep::Progressed => {}
                    CommittedOperationStep::Completed => {
                        activated = true;
                        break;
                    }
                    step @ (CommittedOperationStep::TransientFailure
                    | CommittedOperationStep::Blocked) => {
                        panic!("sustained writes must not block the build: {step:?}")
                    }
                }
            }
            assert!(
                activated,
                "{definition:?} must activate while every step follows a write"
            );
            let after = user_properties("after-activation@example.com");
            mutate_source(&db, scope, IndexElementKind::Node, 0, &sources[&0], &after)
                .await
                .expect("an Active update maintains its entry");
            sources.insert(0, after);
            assert_generation_equals_source(
                &db,
                scope,
                &definition,
                index_id,
                generation,
                &sources,
            )
            .await;
            db.close().await.expect("secondary test database closes");
        }
    }

    /// Reproduces a writer committing while each gated step waits for the
    /// scope permit, so every step observes a fresh delta. The step drains
    /// it under the gate and advances instead of returning to catch-up.
    #[tokio::test]
    async fn gated_steps_drain_the_write_they_waited_for() {
        for (ordinal, definition) in [
            SecondaryIndexDefinition::node_equality("User", "email"),
            SecondaryIndexDefinition::node_unique_equality("User", "email"),
        ]
        .into_iter()
        .enumerate()
        {
            let db = test_db(&format!("secondary-gated-write-window-{ordinal}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(definition.expect("test definition is valid"));
            let mut sources = BTreeMap::new();
            for entity_id in 0..4 {
                let properties = user_properties(&format!("seed-{entity_id}@example.com"));
                put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
                sources.insert(entity_id, properties);
            }
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 3).await;
            let gates = Arc::new(IndexScopeGates::default());
            let driver = SecondaryIndexDriver::new(Arc::clone(&gates));
            let mut claim_sequence = 1;
            drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;

            let mut activated = false;
            for step in 0_u64..8 {
                let in_flight = gates.mutation_permit(scope).await;
                let pending = drive_one(&db, &driver, operation_id, &mut claim_sequence);
                tokio::pin!(pending);
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), &mut pending)
                        .await
                        .is_err(),
                    "every step from catch-up on waits for the in-flight write"
                );
                let inserted = 4 + step;
                let properties = user_properties(&format!("waited-{step}@example.com"));
                mutate_source(
                    &db,
                    scope,
                    IndexElementKind::Node,
                    inserted,
                    &[],
                    &properties,
                )
                .await
                .expect("the in-flight write commits while the step waits");
                sources.insert(inserted, properties);
                drop(in_flight);
                match tokio::time::timeout(Duration::from_secs(5), pending)
                    .await
                    .expect("the step acquires the gate once the write commits")
                {
                    CommittedOperationStep::Progressed => {}
                    CommittedOperationStep::Completed => {
                        activated = true;
                        break;
                    }
                    step @ (CommittedOperationStep::TransientFailure
                    | CommittedOperationStep::Blocked) => {
                        panic!("a drained write must not block the build: {step:?}")
                    }
                }
            }
            assert!(
                activated,
                "{definition:?} must activate while each step waits for a write"
            );
            assert_generation_equals_source(
                &db,
                scope,
                &definition,
                index_id,
                generation,
                &sources,
            )
            .await;
            db.close().await.expect("secondary test database closes");
        }
    }

    /// A crash inside a gated step loses its uncommitted transaction and
    /// nothing else. Before every step a write lands; the step then runs the
    /// production claim, preparation, and gate, and is dropped once it has
    /// staged, with the database dropped unclosed. After each reopen every
    /// delta, applied row, and entry is as the step's committed drains left
    /// them, and the next step, from another writer epoch, still advances
    /// the build until it activates equal to its source.
    #[tokio::test]
    async fn sustained_writes_converge_across_a_crash_inside_every_gated_step() {
        let store = Arc::new(InMemory::new());
        let path = "secondary-sustained-writes-crash";
        let open = || {
            Db::builder(path, store.clone())
                .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
                .build()
        };
        let mut db = open().await.expect("crash test database opens");
        bootstrap_writer(&db)
            .await
            .expect("crash test database bootstraps");
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let mut sources = BTreeMap::new();
        for entity_id in 0..4 {
            let properties = user_properties(&format!("seed-{entity_id}@example.com"));
            put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
            sources.insert(entity_id, properties);
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 3).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        // The crashed steps run as a process the later steps do not share.
        let crashed_epoch = WriterEpoch::from_bytes([0x5C; 16]).expect("writer epoch is non-nil");

        let mut activated = false;
        for step in 0_u64..12 {
            let inserted = 4 + step;
            let properties = user_properties(&format!("inserted-{step}@example.com"));
            mutate_source(
                &db,
                scope,
                IndexElementKind::Node,
                inserted,
                &[],
                &properties,
            )
            .await
            .expect("insert records a build delta");
            sources.insert(inserted, properties);
            let updated = step % 4;
            let after = user_properties(&format!("updated-{step}@example.com"));
            mutate_source(
                &db,
                scope,
                IndexElementKind::Node,
                updated,
                &sources[&updated],
                &after,
            )
            .await
            .expect("update records a build delta");
            sources.insert(updated, after);

            let pause =
                crate::index_lifecycle::outbox::step_pause::StepPause::arm(&db, |progress| {
                    matches!(
                        progress,
                        IndexOperationProgress::SecondaryBuild(
                            SecondaryBuildProgress::Constructing(
                                SecondaryBuildStage::CatchUp(_)
                                    | SecondaryBuildStage::Validate(_)
                                    | SecondaryBuildStage::Activate(_)
                            )
                        )
                    )
                });
            let OperationPointerObservation::Eligible(eligible) =
                observe_operation_pointer(&db, operation_id, crashed_epoch, NOW_MILLIS)
                    .await
                    .expect("operation pointer is readable")
            else {
                panic!("the next gated step is eligible");
            };
            let claimed = claim_operation(
                &db,
                &eligible,
                crashed_epoch,
                ClaimSequence::new(claim_sequence).expect("claim sequence is non-zero"),
                NOW_MILLIS,
                ClaimPermission::Normal,
            )
            .await
            .expect("the crashing claim succeeds")
            .expect("exact queued revision is claimable");
            claim_sequence += 1;
            let lifecycle_rows = {
                let crashing = execute_claimed_step(
                    &db,
                    &claimed,
                    &driver,
                    SearchIndexBackfillLimits::default().batch(),
                    NOW_MILLIS,
                );
                tokio::pin!(crashing);
                tokio::select! {
                    () = pause.reached() => {}
                    outcome = &mut crashing => {
                        panic!("the gated step committed before its crash: {outcome:?}")
                    }
                }
                // Drains the step committed under its gate before its own
                // transaction are durable; that transaction is not.
                let mut committed = Vec::new();
                for kind in [
                    RecordKind::BuildDelta,
                    RecordKind::AppliedState,
                    RecordKind::SecondaryEntry,
                ] {
                    committed.push(generation_rows(&db, scope, kind, index_id, generation).await);
                }
                committed
                // The held step and its staged transaction die here.
            };
            drop(pause);
            drop(db);
            db = open()
                .await
                .expect("crash test database reopens after the crash");
            let mut recovered = Vec::new();
            for kind in [
                RecordKind::BuildDelta,
                RecordKind::AppliedState,
                RecordKind::SecondaryEntry,
            ] {
                recovered.push(generation_rows(&db, scope, kind, index_id, generation).await);
            }
            assert_eq!(
                recovered, lifecycle_rows,
                "an uncommitted step transaction leaves no trace"
            );

            match drive_one(&db, &driver, operation_id, &mut claim_sequence).await {
                CommittedOperationStep::Progressed => {}
                CommittedOperationStep::Completed => {
                    activated = true;
                    break;
                }
                step @ (CommittedOperationStep::TransientFailure
                | CommittedOperationStep::Blocked) => {
                    panic!("a recovered build must not stop under writes: {step:?}")
                }
            }
        }
        assert!(activated, "the build must activate across crashes");
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
        db.close().await.expect("crash test database closes");
    }

    /// Writes inside the scanned range commit while a source batch is held
    /// between staging and commit. The batch was read outside the step's
    /// serializable transaction, so the step still commits on its first
    /// attempt, and each write's delta carries its change into the generation.
    #[tokio::test]
    async fn source_scan_commits_through_writes_to_its_range() {
        for (ordinal, definition) in [
            SecondaryIndexDefinition::node_equality("User", "email"),
            SecondaryIndexDefinition::node_unique_equality("User", "email"),
            SecondaryIndexDefinition::node_range("User", "email"),
        ]
        .into_iter()
        .enumerate()
        {
            let db = test_db(&format!("secondary-scan-window-writes-{ordinal}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(definition.expect("test definition is valid"));
            let mut sources = BTreeMap::new();
            for entity_id in 0..4 {
                let properties = user_properties(&format!("seed-{entity_id}@example.com"));
                put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
                sources.insert(entity_id, properties);
            }
            // Leased IDs 4..=7 lie inside the source bound without rows yet.
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 7).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            let pause =
                crate::index_lifecycle::outbox::step_pause::StepPause::arm(&db, |progress| {
                    matches!(
                        progress,
                        IndexOperationProgress::SecondaryBuild(
                            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Scan(
                                SourceScanProgress { cursor: None, .. }
                            ))
                        )
                    )
                });
            let committed = {
                let step = drive_one(&db, &driver, operation_id, &mut claim_sequence);
                tokio::pin!(step);
                tokio::select! {
                    () = pause.reached() => {}
                    outcome = &mut step => panic!("the scan committed before its pause: {outcome:?}"),
                }
                let updated = user_properties("updated-1@example.com");
                mutate_source(
                    &db,
                    scope,
                    IndexElementKind::Node,
                    1,
                    &sources[&1],
                    &updated,
                )
                .await
                .expect("a scanned entity updates while the scan is held");
                sources.insert(1, updated);
                mutate_source(&db, scope, IndexElementKind::Node, 2, &sources[&2], &[])
                    .await
                    .expect("a scanned entity is deleted while the scan is held");
                sources.remove(&2);
                let inserted = user_properties("inserted-5@example.com");
                mutate_source(&db, scope, IndexElementKind::Node, 5, &[], &inserted)
                    .await
                    .expect("an entity inside the source bound is inserted while the scan is held");
                sources.insert(5, inserted);
                pause.release();

                step.await
            };
            assert_eq!(committed, CommittedOperationStep::Progressed);
            assert_eq!(pause.stagings(), 1, "the scan commits on its first attempt");
            assert!(matches!(
                build_stage(&db, operation_id).await,
                SecondaryBuildStage::CatchUp(_)
            ));
            assert_eq!(
                drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                CommittedOperationStep::Completed
            );
            assert_generation_equals_source(
                &db,
                scope,
                &definition,
                index_id,
                generation,
                &sources,
            )
            .await;
            db.close().await.expect("secondary test database closes");
        }
    }

    /// Batch limits whose single-vector cap equals the transaction's.
    fn batch_limits(
        entities: usize,
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
    ) -> SearchIndexBatchLimits {
        SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(entities).expect("entity limit is positive"),
            NonZeroU64::new(input_bytes).expect("input limit is positive"),
            NonZeroU64::new(output_operations).expect("operation limit is positive"),
            NonZeroU64::new(output_bytes).expect("output limit is positive"),
            NonZeroU64::new(output_bytes).expect("output limit is positive"),
        )
        .expect("test limits are internally consistent")
    }

    /// A source row a scan blocks on is read through its serializable
    /// transaction, so a repair that commits inside the step's window makes
    /// the step lose its commit rather than record a stale durable blocker.
    /// Each case repairs a different guarded row: an oversized input row, a
    /// row that does not decode, a value the index cannot hold, a plan larger
    /// than the output budget, and either owner of a duplicate.
    #[tokio::test]
    async fn a_scan_blocker_repaired_inside_the_step_window_retries() {
        enum Broken {
            Properties(Vec<Property>),
            Bytes(Bytes),
        }
        let unique = || {
            validated(
                SecondaryIndexDefinition::node_unique_equality("User", "email")
                    .expect("unique definition"),
            )
        };
        let equality = || {
            validated(
                SecondaryIndexDefinition::node_equality("User", "email")
                    .expect("equality definition"),
            )
        };
        let long = user_properties(&format!("{}@example.com", "x".repeat(600)));
        let duplicate = user_properties("duplicate@example.com");
        let cases = [
            (
                "oversized input row",
                equality(),
                batch_limits(1024, 512, 4096, 1024 * 1024),
                vec![(0, Broken::Properties(long.clone()))],
                0,
            ),
            (
                "undecodable row",
                equality(),
                SearchIndexBackfillLimits::default().batch(),
                vec![(0, Broken::Bytes(Bytes::from_static(b"malformed")))],
                0,
            ),
            (
                "unsupported value",
                validated(
                    SecondaryIndexDefinition::node_range("User", "email")
                        .expect("range definition"),
                ),
                SearchIndexBackfillLimits::default().batch(),
                vec![(
                    0,
                    Broken::Properties(vec![
                        Property::string("$label", "User"),
                        Property::new("email", PropertyValue::Bool(true)),
                    ]),
                )],
                0,
            ),
            (
                "oversized output plan",
                equality(),
                batch_limits(1024, 1024 * 1024, 4096, 400),
                vec![(0, Broken::Properties(long))],
                0,
            ),
            (
                "first owner of a duplicate",
                unique(),
                SearchIndexBackfillLimits::default().batch(),
                vec![
                    (0, Broken::Properties(duplicate.clone())),
                    (1, Broken::Properties(duplicate.clone())),
                ],
                0,
            ),
            (
                "second owner of a duplicate",
                unique(),
                SearchIndexBackfillLimits::default().batch(),
                vec![
                    (0, Broken::Properties(duplicate.clone())),
                    (1, Broken::Properties(duplicate)),
                ],
                1,
            ),
        ];
        for (ordinal, (case, definition, limits, rows, repaired)) in cases.into_iter().enumerate() {
            let db = test_db(&format!("secondary-scan-blocker-window-{ordinal}")).await;
            let scope = DataScope::LegacyUnscoped;
            let mut sources = BTreeMap::new();
            let mut upper = 0;
            for (entity_id, row) in rows {
                match row {
                    Broken::Properties(properties) => {
                        put_source(&db, scope, IndexElementKind::Node, entity_id, &properties)
                            .await;
                        sources.insert(entity_id, properties);
                    }
                    Broken::Bytes(bytes) => {
                        db.put(source_key(scope, IndexElementKind::Node, entity_id), bytes)
                            .await
                            .expect("malformed source row is written");
                    }
                }
                upper = entity_id;
            }
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, upper).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            let pause =
                crate::index_lifecycle::outbox::step_pause::StepPause::arm(&db, |progress| {
                    matches!(
                        progress,
                        IndexOperationProgress::SecondaryBuild(
                            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Scan(_))
                        )
                    )
                });
            // A writer epoch the later steps do not share.
            let held_epoch = WriterEpoch::from_bytes([0x5B; 16]).expect("writer epoch is non-nil");
            let OperationPointerObservation::Eligible(eligible) =
                observe_operation_pointer(&db, operation_id, held_epoch, NOW_MILLIS)
                    .await
                    .expect("operation pointer is readable")
            else {
                panic!("{case}: the queued scan is eligible");
            };
            let claimed = claim_operation(
                &db,
                &eligible,
                held_epoch,
                ClaimSequence::new(claim_sequence).expect("claim sequence is non-zero"),
                NOW_MILLIS,
                ClaimPermission::Normal,
            )
            .await
            .expect("scan claim succeeds")
            .expect("exact queued revision is claimable");
            claim_sequence += 1;
            let committed = {
                let step = execute_claimed_step(&db, &claimed, &driver, limits, NOW_MILLIS);
                tokio::pin!(step);
                tokio::select! {
                    () = pause.reached() => {}
                    outcome = &mut step => {
                        panic!("{case}: the scan committed before its pause: {outcome:?}")
                    }
                }
                let fixed = user_properties(&format!("repaired-{repaired}@example.com"));
                let key = source_key(scope, IndexElementKind::Node, repaired);
                db.put(&key, encode_properties(&fixed))
                    .await
                    .expect("the blocking row is repaired while the scan is held");
                sources.insert(repaired, fixed);
                pause.release();
                step.await
            };
            assert_eq!(
                committed.expect("a lost conflict releases the claim"),
                CommittedOperationStep::TransientFailure,
                "{case}: a repaired blocker must not commit"
            );
            assert_eq!(
                drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                CommittedOperationStep::Completed,
                "{case}: the retried scan converges"
            );
            assert_generation_equals_source(
                &db,
                scope,
                &definition,
                index_id,
                generation,
                &sources,
            )
            .await;
            db.close().await.expect("secondary test database closes");
        }
    }

    /// Returns the build stage a step result progressed to.
    fn progressed_stage(result: &IndexOperationStepResult) -> &SecondaryBuildStage {
        let IndexOperationStepResult::Progressed(IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Constructing(stage),
        )) = result
        else {
            panic!("the step progressed a build: {result:?}");
        };
        stage
    }

    /// Returns one build stage's cumulative counters.
    fn stage_counters(stage: &SecondaryBuildStage) -> OperationCounters {
        match stage {
            SecondaryBuildStage::Scan(progress) => progress.counters,
            SecondaryBuildStage::CatchUp(progress) | SecondaryBuildStage::Validate(progress) => {
                progress.counters
            }
            SecondaryBuildStage::Activate(progress) => progress.counters,
        }
    }

    /// Runs one step outside the outbox, as a gated step's own transaction
    /// does after its gate, and returns its result. The step's physical
    /// writes commit only when `commit` is set; the checkpoint never does.
    async fn direct_step(
        db: &Db,
        driver: &SecondaryIndexDriver,
        operation_id: IndexOperationId,
        limits: SearchIndexBatchLimits,
        commit: bool,
    ) -> IndexOperationStepResult {
        let operation = read_operation(db, DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("secondary operation is readable")
            .expect("secondary operation exists");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("direct step transaction begins");
        let execution = driver
            .step(
                db,
                &transaction,
                DataScope::LegacyUnscoped,
                &operation,
                limits,
            )
            .await
            .expect("the direct step stages");
        if commit {
            transaction.commit().await.expect("the direct step commits");
        }
        execution.result().clone()
    }

    /// A validation or activation drain that its transaction's budget stops
    /// keeps its stage, validation keeps its cursor, and the delta past the
    /// budget stays pending. Each budget is set to admit exactly the first
    /// of two pending reconciliations, measured by a step that rolls back.
    #[tokio::test]
    async fn gated_drains_a_budget_stops_keep_their_stage() {
        for (ordinal, activate) in [false, true].into_iter().enumerate() {
            for budget in 0..4_usize {
                let db = test_db(&format!("secondary-drain-budget-{ordinal}-{budget}")).await;
                let scope = DataScope::LegacyUnscoped;
                let definition = validated(
                    SecondaryIndexDefinition::node_equality("User", "email")
                        .expect("node equality definition"),
                );
                let mut sources = BTreeMap::new();
                for entity_id in 0..2 {
                    let properties = user_properties(&format!("seed-{entity_id}@example.com"));
                    put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
                    sources.insert(entity_id, properties);
                }
                let (operation_id, index_id, generation) =
                    create_build(&db, scope, &definition, 1).await;
                let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
                let mut claim_sequence = 1;
                let reached: fn(&SecondaryBuildStage) -> bool = if activate {
                    |stage| matches!(stage, SecondaryBuildStage::Activate(_))
                } else {
                    |stage| matches!(stage, SecondaryBuildStage::Validate(_))
                };
                drive_until_stage(&db, &driver, operation_id, &mut claim_sequence, reached).await;
                for entity_id in 0..2 {
                    let after = user_properties(&format!("moved-{entity_id}@example.com"));
                    mutate_source(
                        &db,
                        scope,
                        IndexElementKind::Node,
                        entity_id,
                        &sources[&entity_id],
                        &after,
                    )
                    .await
                    .expect("update records a build delta");
                    sources.insert(entity_id, after);
                }
                let start = build_stage(&db, operation_id).await;
                let before = stage_counters(&start);
                let generous = 8 * 1024 * 1024;
                let first = stage_counters(progressed_stage(
                    &direct_step(
                        &db,
                        &driver,
                        operation_id,
                        batch_limits(1, generous, 4096, generous),
                        false,
                    )
                    .await,
                ));
                let limits = match budget {
                    0 => batch_limits(1, generous, 4096, generous),
                    1 => batch_limits(1024, first.input_bytes - before.input_bytes, 4096, generous),
                    2 => batch_limits(
                        1024,
                        generous,
                        first.output_operations - before.output_operations,
                        generous,
                    ),
                    _ => batch_limits(
                        1024,
                        generous,
                        4096,
                        first.output_bytes - before.output_bytes,
                    ),
                };
                let result = direct_step(&db, &driver, operation_id, limits, true).await;
                let after = progressed_stage(&result);
                match &start {
                    SecondaryBuildStage::Validate(start) => {
                        let SecondaryBuildStage::Validate(after) = after else {
                            panic!("budget {budget} left validation: {after:?}");
                        };
                        assert_eq!(after.cursor, start.cursor, "budget {budget}");
                    }
                    SecondaryBuildStage::Activate(_) => assert!(
                        matches!(after, SecondaryBuildStage::Activate(_)),
                        "budget {budget} left activation: {after:?}"
                    ),
                    SecondaryBuildStage::Scan(_) | SecondaryBuildStage::CatchUp(_) => {
                        unreachable!("the build was driven to validation or activation")
                    }
                }
                assert_eq!(
                    generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation)
                        .await
                        .len(),
                    1,
                    "budget {budget}: the reconciliation past the budget stays pending"
                );
                assert_eq!(
                    drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                    CommittedOperationStep::Completed
                );
                assert_generation_equals_source(
                    &db,
                    scope,
                    &definition,
                    index_id,
                    generation,
                    &sources,
                )
                .await;
                db.close().await.expect("secondary test database closes");
            }
        }
    }

    /// Each write window brings more deltas than one transaction admits. The
    /// gated step holds the scope permit across bounded drains until the
    /// remainder fits its own transaction, so every step advances and the
    /// build activates within a fixed number of steps, for one build and
    /// for two builds sharing the scope.
    #[tokio::test]
    async fn gated_steps_converge_when_each_write_window_exceeds_a_batch() {
        for builds in [1_usize, 2] {
            let db = test_db(&format!("secondary-window-above-batch-{builds}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definitions = [
                SecondaryIndexDefinition::node_equality("User", "email"),
                SecondaryIndexDefinition::node_range("User", "email"),
            ]
            .into_iter()
            .take(builds)
            .map(|definition| validated(definition.expect("test definition is valid")))
            .collect::<Vec<_>>();
            let mut sources = BTreeMap::new();
            for entity_id in 0..2 {
                let properties = user_properties(&format!("seed-{entity_id}@example.com"));
                put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
                sources.insert(entity_id, properties);
            }
            let mut operations = Vec::new();
            for definition in &definitions {
                operations.push(create_build(&db, scope, definition, 1).await);
            }
            let gates = Arc::new(IndexScopeGates::default());
            let driver = SecondaryIndexDriver::new(Arc::clone(&gates));
            let limits = one_entity_limits();
            let mut claim_sequence = 1;
            for &(operation_id, _, _) in &operations {
                while !matches!(
                    build_stage(&db, operation_id).await,
                    SecondaryBuildStage::CatchUp(_)
                ) {
                    assert_eq!(
                        drive_one_with_limits(
                            &db,
                            &driver,
                            operation_id,
                            &mut claim_sequence,
                            limits
                        )
                        .await,
                        CommittedOperationStep::Progressed
                    );
                }
            }

            let mut running = operations.iter().map(|&(id, _, _)| id).collect::<Vec<_>>();
            for _ in 0..16 {
                let mut still_running = Vec::new();
                for operation_id in running {
                    // Three indexed inserts commit while the step waits for
                    // the scope permit they hold.
                    let in_flight = gates.mutation_permit(scope).await;
                    let pending = drive_one_with_limits(
                        &db,
                        &driver,
                        operation_id,
                        &mut claim_sequence,
                        limits,
                    );
                    tokio::pin!(pending);
                    assert!(
                        tokio::time::timeout(Duration::from_millis(50), &mut pending)
                            .await
                            .is_err(),
                        "every step from catch-up on waits for the in-flight writes"
                    );
                    let first = sources.keys().next_back().map_or(0, |last| last + 1);
                    for entity_id in first..first + 3 {
                        let properties =
                            user_properties(&format!("window-{entity_id}@example.com"));
                        mutate_source(
                            &db,
                            scope,
                            IndexElementKind::Node,
                            entity_id,
                            &[],
                            &properties,
                        )
                        .await
                        .expect("an in-flight write commits while the step waits");
                        sources.insert(entity_id, properties);
                    }
                    drop(in_flight);
                    match tokio::time::timeout(Duration::from_secs(5), pending)
                        .await
                        .expect("the step acquires the gate once the writes commit")
                    {
                        CommittedOperationStep::Progressed => still_running.push(operation_id),
                        CommittedOperationStep::Completed => {}
                        step @ (CommittedOperationStep::TransientFailure
                        | CommittedOperationStep::Blocked) => {
                            panic!("a window above one batch must not stop the build: {step:?}")
                        }
                    }
                }
                running = still_running;
                if running.is_empty() {
                    break;
                }
            }
            assert!(
                running.is_empty(),
                "{builds} builds must activate while each window exceeds a batch"
            );
            for (definition, &(_, index_id, generation)) in definitions.iter().zip(&operations) {
                assert_generation_equals_source(
                    &db, scope, definition, index_id, generation, &sources,
                )
                .await;
            }
            db.close().await.expect("secondary test database closes");
        }
    }

    /// A duplicate found by the validation drain blocks the build after the
    /// earlier entities' reconciliations commit; repairing the duplicate and
    /// retrying converges to the source.
    #[tokio::test]
    async fn a_blocked_validation_drain_commits_a_consistent_prefix() {
        let db = test_db("secondary-blocked-validation-drain").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let mut sources = BTreeMap::new();
        for entity_id in 0..3 {
            let properties = user_properties(&format!("seed-{entity_id}@example.com"));
            put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
            sources.insert(entity_id, properties);
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 2).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        drive_until_stage(&db, &driver, operation_id, &mut claim_sequence, |stage| {
            matches!(stage, SecondaryBuildStage::Validate(_))
        })
        .await;
        let moved = user_properties("moved-0@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &sources[&0], &moved)
            .await
            .expect("a distinct update records a build delta");
        sources.insert(0, moved);
        let duplicate = sources[&2].clone();
        mutate_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &sources[&1],
            &duplicate,
        )
        .await
        .expect("a Building unique index admits the duplicate as a delta");
        sources.insert(1, duplicate);

        assert_eq!(
            drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Blocked
        );
        let blocked = read_operation(&db, scope, operation_id)
            .await
            .expect("blocked operation is readable")
            .expect("blocked operation exists");
        assert!(matches!(
            blocked.execution_state(),
            IndexOperationExecutionState::Blocked(IndexOperationBlocker::UniquenessViolation {
                first_entity_id,
                second_entity_id,
            }) if first_entity_id.get() == 2 && second_entity_id.get() == 1
        ));
        let entity_key = |kind: fn(IndexEntityStateKey) -> ScopedKey, entity_id: u64| {
            scoped_index_key(
                scope,
                kind(IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(entity_id),
                    },
                }),
            )
        };
        for (key, present) in [
            (entity_key(ScopedKey::BuildDelta, 0), false),
            (entity_key(ScopedKey::AppliedState, 0), false),
            (entity_key(ScopedKey::BuildDelta, 1), true),
            (entity_key(ScopedKey::AppliedState, 1), true),
        ] {
            assert_eq!(
                db.get(key)
                    .await
                    .expect("lifecycle row is readable")
                    .is_some(),
                present
            );
        }

        let repaired = user_properties("repaired-1@example.com");
        mutate_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &sources[&1],
            &repaired,
        )
        .await
        .expect("the duplicate is repaired while the build is blocked");
        sources.insert(1, repaired);
        crate::index_lifecycle::outbox::retry_operation(&db, scope, operation_id)
            .await
            .expect("the blocked build retries at its checkpoint");
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
        db.close().await.expect("secondary test database closes");
    }

    /// An entity that had its value when the build was created, lost it
    /// before the scan reached it, and regained it after keeps its entry in
    /// every lane. Its delta still names the original value, which the
    /// generation never held: without an applied row that value is only a
    /// candidate, so the drain writes the restored value although the delta
    /// calls it unchanged.
    #[tokio::test]
    async fn a_value_lost_before_the_scan_and_restored_after_keeps_its_entry() {
        let lanes = [
            (
                SecondaryIndexDefinition::node_equality("User", "email"),
                IndexElementKind::Node,
            ),
            (
                SecondaryIndexDefinition::node_unique_equality("User", "email"),
                IndexElementKind::Node,
            ),
            (
                SecondaryIndexDefinition::node_range("User", "email"),
                IndexElementKind::Node,
            ),
            (
                SecondaryIndexDefinition::edge_equality("User", "email"),
                IndexElementKind::Edge,
            ),
            (
                SecondaryIndexDefinition::edge_range("User", "email"),
                IndexElementKind::Edge,
            ),
        ];
        let original = user_properties("original@example.com");
        let absences = [
            // The indexed property is removed.
            vec![Property::string("$label", "User")],
            // The entity moves to another label.
            vec![
                Property::string("$label", "Other"),
                Property::string("email", "original@example.com"),
            ],
        ];
        for (lane, (definition, kind)) in lanes.into_iter().enumerate() {
            let definition = validated(definition.expect("test definition is valid"));
            for (variant, absent) in absences.iter().enumerate() {
                let db = test_db(&format!("secondary-restored-after-scan-{lane}-{variant}")).await;
                let scope = DataScope::LegacyUnscoped;
                put_source(&db, scope, kind, 0, &original).await;
                let (operation_id, index_id, generation) =
                    create_build(&db, scope, &definition, 0).await;
                mutate_source(&db, scope, kind, 0, &original, absent)
                    .await
                    .expect("the value is lost before the scan");
                let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
                let mut claim_sequence = 1;
                drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
                mutate_source(&db, scope, kind, 0, absent, &original)
                    .await
                    .expect("the value is restored after the scan");
                assert_eq!(
                    drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                    CommittedOperationStep::Completed,
                    "lane {lane}, variant {variant}"
                );
                assert_generation_equals_source(
                    &db,
                    scope,
                    &definition,
                    index_id,
                    generation,
                    &BTreeMap::from([(0, original.clone())]),
                )
                .await;
                db.close().await.expect("secondary test database closes");
            }
        }
    }

    /// An entity deleted before the scan reached it leaves a delta naming the
    /// value it had when the build was created. Another entity takes that
    /// value before the scan, and the deleted entity never held it in the
    /// generation, so its drain leaves the new owner's entry alone instead
    /// of reporting a duplicate.
    #[tokio::test]
    async fn a_unique_value_freed_before_the_scan_does_not_block_its_new_owner() {
        let db = test_db("secondary-unique-freed-before-scan").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let shared = user_properties("shared@example.com");
        let other = user_properties("other@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &shared).await;
        put_source(&db, scope, IndexElementKind::Node, 1, &other).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        mutate_source(&db, scope, IndexElementKind::Node, 0, &shared, &[])
            .await
            .expect("the first owner is deleted before the scan");
        mutate_source(&db, scope, IndexElementKind::Node, 1, &other, &shared)
            .await
            .expect("another entity takes the freed value before the scan");
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert_generation_equals_source(
            &db,
            scope,
            &definition,
            index_id,
            generation,
            &BTreeMap::from([(1, shared)]),
        )
        .await;
        db.close().await.expect("secondary test database closes");
    }

    /// A drain that releases a unique entity whose value it finds unchanged
    /// checks the entity's entry exactly as validation would have: a missing
    /// entry is corruption and another owner blocks the build, where the
    /// applied row used to be released unchecked. Each broken entry is
    /// injected directly.
    #[tokio::test]
    async fn a_release_drain_checks_an_unchanged_unique_entry() {
        for (ordinal, foreign_owner) in [false, true].into_iter().enumerate() {
            let db = test_db(&format!("secondary-release-unchanged-unique-{ordinal}")).await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(
                SecondaryIndexDefinition::node_unique_equality("User", "email")
                    .expect("unique definition"),
            );
            let kept = user_properties("kept@example.com");
            put_source(&db, scope, IndexElementKind::Node, 0, &kept).await;
            put_source(
                &db,
                scope,
                IndexElementKind::Node,
                1,
                &user_properties("other@example.com"),
            )
            .await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 1).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            drive_until_stage(&db, &driver, operation_id, &mut claim_sequence, |stage| {
                matches!(stage, SecondaryBuildStage::Validate(_))
            })
            .await;
            let ValidatedDynamicIndexDefinition::Secondary(secondary) = &definition else {
                unreachable!("secondary fixture is type-checked")
            };
            let entity = IndexEntityId::initial();
            let entry_key = secondary_entry_key(
                scope,
                index_id,
                generation,
                secondary,
                canonical_value(secondary, &kept, entity)
                    .expect("fixture value is supported")
                    .expect("fixture value is indexed"),
                entity,
            )
            .expect("fixture entry key validates");
            if foreign_owner {
                db.put(
                    &entry_key,
                    encode_secondary_entry(&SecondaryEntryValue {
                        index_id,
                        generation,
                        lane: definition_lane(secondary),
                        entity_id: IndexEntityId::new(1),
                    }),
                )
                .await
                .expect("the entry is pointed at another entity");
            } else {
                db.delete(&entry_key).await.expect("the entry is removed");
            }
            // The entity leaves its value and returns inside the window.
            let away = user_properties("away@example.com");
            mutate_source(&db, scope, IndexElementKind::Node, 0, &kept, &away)
                .await
                .expect("the entity leaves its value");
            mutate_source(&db, scope, IndexElementKind::Node, 0, &away, &kept)
                .await
                .expect("the entity returns to its value");

            assert_eq!(
                drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
                CommittedOperationStep::Blocked
            );
            let blocked = read_operation(&db, scope, operation_id)
                .await
                .expect("blocked operation is readable")
                .expect("blocked operation exists");
            let expected = if foreign_owner {
                IndexOperationBlocker::UniquenessViolation {
                    first_entity_id: IndexEntityId::new(1),
                    second_entity_id: entity,
                }
            } else {
                IndexOperationBlocker::InvariantViolation
            };
            assert_eq!(
                blocked.execution_state(),
                &IndexOperationExecutionState::Blocked(expected)
            );
            db.close().await.expect("secondary test database closes");
        }
    }

    /// A unique entry the scan gave an earlier entity still names it after
    /// that entity moved off the value. A later entity that took the value
    /// before the scan reached it takes the stale claim over instead of
    /// blocking the build on a duplicate the source never held.
    #[tokio::test]
    async fn a_scan_takes_over_a_unique_claim_its_owner_moved_off() {
        let db = test_db("secondary-scan-stale-unique-owner").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let mut sources = BTreeMap::new();
        for (entity_id, email) in [(0, "taken@example.com"), (1, "spare@example.com")] {
            let properties = user_properties(email);
            put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
            sources.insert(entity_id, properties);
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        // The first batch scans only entity 0, which claims its value.
        assert_eq!(
            drive_one_with_limits(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                one_entity_limits()
            )
            .await,
            CommittedOperationStep::Progressed
        );
        assert!(matches!(
            build_stage(&db, operation_id).await,
            SecondaryBuildStage::Scan(_)
        ));
        for (entity_id, email) in [(0, "moved@example.com"), (1, "taken@example.com")] {
            let after = user_properties(email);
            mutate_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &sources[&entity_id],
                &after,
            )
            .await
            .expect("a building unique index records the move as a delta");
            sources.insert(entity_id, after);
        }
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
        db.close().await.expect("secondary test database closes");
    }

    /// Unique values that move between entities while the build records
    /// deltas leave entries naming entities that moved off them. A drain
    /// takes such a stale claim over instead of blocking on it, whichever
    /// entity it reconciles first: for a value moved to an entity that sorts
    /// first, and for two entities swapping values, in catch-up and inside a
    /// validation window.
    #[tokio::test]
    async fn unique_drains_take_over_claims_their_owners_moved_off() {
        for (ordinal, validate) in [false, true].into_iter().enumerate() {
            for swap in [false, true] {
                let db = test_db(&format!("secondary-unique-stale-owner-{ordinal}-{swap}")).await;
                let scope = DataScope::LegacyUnscoped;
                let definition = validated(
                    SecondaryIndexDefinition::node_unique_equality("User", "email")
                        .expect("unique definition"),
                );
                let mut sources = BTreeMap::new();
                for (entity_id, email) in [(0, "low@example.com"), (1, "high@example.com")] {
                    let properties = user_properties(email);
                    put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
                    sources.insert(entity_id, properties);
                }
                let (operation_id, index_id, generation) =
                    create_build(&db, scope, &definition, 1).await;
                let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
                let mut claim_sequence = 1;
                let reached: fn(&SecondaryBuildStage) -> bool = if validate {
                    |stage| matches!(stage, SecondaryBuildStage::Validate(_))
                } else {
                    |stage| matches!(stage, SecondaryBuildStage::CatchUp(_))
                };
                drive_until_stage(&db, &driver, operation_id, &mut claim_sequence, reached).await;
                // Entity 0 sorts first and takes entity 1's value; entity 1
                // takes entity 0's value or moves away.
                let moves = if swap {
                    [(0, "high@example.com"), (1, "low@example.com")]
                } else {
                    [(1, "away@example.com"), (0, "high@example.com")]
                };
                for (entity_id, email) in moves {
                    let after = user_properties(email);
                    mutate_source(
                        &db,
                        scope,
                        IndexElementKind::Node,
                        entity_id,
                        &sources[&entity_id],
                        &after,
                    )
                    .await
                    .expect("a building unique index records the move as a delta");
                    sources.insert(entity_id, after);
                }
                assert_eq!(
                    drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                    CommittedOperationStep::Completed,
                    "validate {validate}, swap {swap}"
                );
                assert_generation_equals_source(
                    &db,
                    scope,
                    &definition,
                    index_id,
                    generation,
                    &sources,
                )
                .await;
                db.close().await.expect("secondary test database closes");
            }
        }
    }

    /// An exact catch-up batch that takes a stale claim over from an entity
    /// later in the same batch reconciles that entity from its delta, since
    /// the applied row the batch read for it is gone.
    #[tokio::test]
    async fn exact_catch_up_takes_over_a_claim_from_an_entity_later_in_its_batch() {
        let db = test_db("secondary-exact-stale-unique-owner").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let mut sources = BTreeMap::new();
        for (entity_id, email) in [(0, "low@example.com"), (1, "high@example.com")] {
            let properties = user_properties(email);
            put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
            sources.insert(entity_id, properties);
        }
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        for (entity_id, email) in [(1, "away@example.com"), (0, "high@example.com")] {
            let after = user_properties(email);
            mutate_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &sources[&entity_id],
                &after,
            )
            .await
            .expect("a building unique index records the move as a delta");
            sources.insert(entity_id, after);
        }
        let prepared = PreparedSecondaryOperationStep {
            step: PreparedSecondaryStep::ExactCatchUp {
                keys: (0..2)
                    .map(|entity_id| IndexEntityStateKey {
                        index_id,
                        generation,
                        entity: IndexEntity {
                            kind: IndexElementKind::Node,
                            id: IndexEntityId::new(entity_id),
                        },
                    })
                    .collect(),
            },
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("exact catch-up transaction begins");
        let execution = prepared
            .stage(
                &transaction,
                scope,
                &operation,
                SearchIndexBackfillLimits::default().batch(),
            )
            .await
            .expect("exact catch-up stages");
        assert!(
            matches!(
                execution.result(),
                IndexOperationStepResult::Progressed(IndexOperationProgress::SecondaryBuild(
                    SecondaryBuildProgress::Constructing(SecondaryBuildStage::CatchUp(_))
                ))
            ),
            "the stale claim does not block the batch"
        );
        transaction.commit().await.expect("exact catch-up commits");
        assert!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation)
                .await
                .is_empty()
        );
        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
        db.close().await.expect("secondary test database closes");
    }

    /// Only a catch-up backlog within [`GATED_BACKLOG_BATCHES`] takes the
    /// exclusive gate; a larger one is reconciled by exact keys while writes
    /// continue, without waiting for the scope's in-flight writes.
    #[tokio::test]
    async fn only_a_catch_up_backlog_within_its_gated_reach_waits_for_scope_mutations() {
        let db = test_db("secondary-catch-up-final-barrier").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let entities = 10_u64;
        for entity_id in 0..entities {
            let properties = user_properties(&format!("seed-{entity_id}@example.com"));
            put_source(&db, scope, IndexElementKind::Node, entity_id, &properties).await;
        }
        let (operation_id, _, _) = create_build(&db, scope, &definition, entities - 1).await;
        let gates = Arc::new(IndexScopeGates::default());
        let driver = SecondaryIndexDriver::new(Arc::clone(&gates));
        let mut claim_sequence = 1;
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        for entity_id in 0..entities {
            mutate_source(
                &db,
                scope,
                IndexElementKind::Node,
                entity_id,
                &user_properties(&format!("seed-{entity_id}@example.com")),
                &user_properties(&format!("updated-{entity_id}@example.com")),
            )
            .await
            .expect("update records one build delta");
        }

        for (limits, waits) in [
            (SearchIndexBackfillLimits::default().batch(), true),
            (one_entity_limits(), false),
        ] {
            let mutation = gates.mutation_permit(scope).await;
            let prepared = driver.prepare_step(&db, scope, &operation, limits);
            tokio::pin!(prepared);
            if waits {
                assert!(
                    tokio::time::timeout(Duration::from_millis(50), &mut prepared)
                        .await
                        .is_err(),
                    "a backlog within the gated reach must wait for exclusive scope authority"
                );
                drop(mutation);
                let prepared = tokio::time::timeout(Duration::from_secs(2), prepared)
                    .await
                    .expect("the barrier acquires after the mutation finishes")
                    .expect("the barrier prepares");
                assert_eq!(prepared.family(), IndexOperationFamily::Secondary);
            } else {
                let prepared = tokio::time::timeout(Duration::from_secs(2), prepared)
                    .await
                    .expect("exact-key preparation does not wait for the shared scope permit")
                    .expect("exact-key catch-up prepares");
                assert_eq!(prepared.family(), IndexOperationFamily::Secondary);
                drop(prepared);
                drop(mutation);
            }
        }
        let batches = usize::try_from(entities).expect("entity count fits usize");
        for (limits, reach, gated) in [
            (
                SearchIndexBackfillLimits::default().batch(),
                GATED_BACKLOG_BATCHES,
                true,
            ),
            (one_entity_limits(), GATED_BACKLOG_BATCHES, false),
            (one_entity_limits(), batches, true),
            (one_entity_limits(), batches - 1, false),
        ] {
            let observed = observe_backlog(&db, scope, &operation, limits, reach)
                .await
                .expect("catch-up selection is observed");
            assert_eq!(observed.is_none(), gated, "{reach} batches of {limits:?}");
            assert!(observed.is_none_or(|keys| keys.len() == 1));
        }
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn cleanup_deletes_an_indivisible_bitmap_larger_than_the_input_limit() {
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
        let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
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
        let tiny_limits = SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1024).expect("one kibibyte is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
        )
        .expect("tiny limits are internally consistent");
        assert_eq!(
            drive_one_with_limits(&db, &driver, drop_id, &mut claim_sequence, tiny_limits,).await,
            CommittedOperationStep::Progressed
        );
        assert!(generation_rows(
            &db,
            scope,
            RecordKind::SecondaryEqualityBitmap,
            index_id,
            generation,
        )
        .await
        .is_empty());
        let operation = read_operation(&db, scope, drop_id)
            .await
            .expect("progressed cleanup is readable")
            .expect("progressed cleanup exists");
        assert!(matches!(
            operation.progress(),
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                PrefixScanProgress {
                    cursor: Some(_),
                    ..
                }
            ))
        ));
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn oversized_v4_bitmap_cleanup_survives_reopen_at_default_limit() {
        let default_limits = SearchIndexBackfillLimits::default().batch();
        let input_limit = usize::try_from(default_limits.max_input_bytes().get())
            .expect("default input limit fits usize");
        let mut ids = roaring::RoaringTreemap::new();
        let mut ordinal = 0_u64;
        while ids.serialized_size() <= input_limit {
            for _ in 0..100_000 {
                ids.insert(ordinal << 16);
                ordinal += 1;
            }
            assert!(
                ordinal <= 2_000_000,
                "sparse bitmap exceeds its generation bound"
            );
        }
        let oversized = SecondaryEqualityBitmapValue::new(ids).encode();
        assert!(oversized.len() > input_limit);

        let scopes = [
            DataScope::LegacyUnscoped,
            DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(
                u128::from_be_bytes([0xFD; 16]),
            )),
        ];
        for (case, scope) in scopes.into_iter().enumerate() {
            let store = Arc::new(InMemory::new());
            let database = format!("secondary-default-oversized-cleanup-{case}");
            let db = Db::builder(database.as_str(), store.clone())
                .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
                .build()
                .await
                .expect("oversized cleanup database opens");
            bootstrap_writer(&db)
                .await
                .expect("oversized cleanup database bootstraps");
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
            let (build_id, index_id, generation) = create_build(&db, scope, &definition, 0).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            assert_eq!(
                drive_to_terminal(&db, &driver, build_id, &mut claim_sequence).await,
                CommittedOperationStep::Completed
            );
            let rows = generation_rows(
                &db,
                scope,
                RecordKind::SecondaryEqualityBitmap,
                index_id,
                generation,
            )
            .await;
            assert_eq!(rows.len(), 1);
            db.put(&rows[0].0, oversized.clone())
                .await
                .expect("oversized bitmap replaces the built row");
            let IndexDdlReceipt::Accepted {
                operation_id: drop_id,
                ..
            } = drop_index_operation(&db, scope, &definition)
                .await
                .expect("oversized cleanup is enqueued")
            else {
                panic!("active secondary drop enqueues cleanup")
            };
            assert_eq!(
                drive_one_with_limits(&db, &driver, drop_id, &mut claim_sequence, default_limits,)
                    .await,
                CommittedOperationStep::Progressed
            );
            db.close()
                .await
                .expect("oversized cleanup closes after its first checkpoint");

            let db = Db::builder(database.as_str(), store)
                .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
                .build()
                .await
                .expect("oversized cleanup database reopens");
            bootstrap_writer(&db)
                .await
                .expect("reopened oversized cleanup database bootstraps");
            assert_eq!(
                drive_to_terminal(&db, &driver, drop_id, &mut claim_sequence).await,
                CommittedOperationStep::Completed
            );
            for kind in [
                RecordKind::SecondaryEqualityBitmap,
                RecordKind::BuildDelta,
                RecordKind::AppliedState,
            ] {
                assert!(
                    generation_rows(&db, scope, kind, index_id, generation)
                        .await
                        .is_empty(),
                    "terminal cleanup removes {kind:?} rows"
                );
            }
            assert!(matches!(
                read_index(&db, scope, &definition).await.state(),
                IndexStateV2::Dropped { .. }
            ));
            db.close().await.expect("oversized cleanup database closes");
        }
    }

    #[tokio::test]
    async fn cleanup_still_blocks_an_oversized_non_bitmap_row() {
        let db = test_db("secondary-oversized-non-bitmap-cleanup-row").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique node equality definition"),
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
        let IndexDdlReceipt::Accepted {
            operation_id: drop_id,
            ..
        } = drop_index_operation(&db, scope, &definition)
            .await
            .expect("oversized cleanup is enqueued")
        else {
            panic!("active secondary drop enqueues cleanup");
        };
        let tiny_limits = SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
            std::num::NonZeroU64::new(1024).expect("one kibibyte is positive"),
            std::num::NonZeroU64::new(1).expect("one is positive"),
        )
        .expect("tiny limits are internally consistent");

        assert_eq!(
            drive_one_with_limits(&db, &driver, drop_id, &mut claim_sequence, tiny_limits).await,
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
        let scope = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
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
            DataKey::Global {
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
            DataKey::Global {
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
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .len(),
            1
        );

        assert_eq!(
            drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Completed
        );
        assert!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .is_empty()
        );
        assert!(
            generation_rows(&db, scope, RecordKind::AppliedState, index_id, generation,)
                .await
                .is_empty()
        );
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

    /// An entity whose value is too long to index can still be deleted while
    /// a build runs, and catch-up never removes the key that value could not
    /// have had.
    #[tokio::test]
    async fn catch_up_skips_previous_values_storage_could_not_index() {
        for definition in [
            SecondaryIndexDefinition::node_equality("User", "email"),
            SecondaryIndexDefinition::node_unique_equality("User", "email"),
            SecondaryIndexDefinition::node_range("User", "email"),
        ] {
            let db = test_db("secondary-build-delta-unindexable").await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(definition.expect("test definition is valid"));
            let before = user_properties(&"x".repeat(70_000));
            put_source(&db, scope, IndexElementKind::Node, 0, &before).await;
            let (operation_id, index_id, generation) =
                create_build(&db, scope, &definition, 0).await;
            mutate_source(&db, scope, IndexElementKind::Node, 0, &before, &[])
                .await
                .expect("an unindexable entity can be deleted during a build");
            assert_eq!(
                generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation)
                    .await
                    .len(),
                1
            );

            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            assert_eq!(
                drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                CommittedOperationStep::Completed
            );
            for kind in [RecordKind::BuildDelta, RecordKind::AppliedState] {
                assert!(generation_rows(&db, scope, kind, index_id, generation)
                    .await
                    .is_empty());
            }
            db.close().await.expect("secondary test database closes");
        }
    }

    /// An entity whose value an index cannot hold at all, too large for the
    /// format or of an unsupported type, blocks a build over it, yet can
    /// still be fixed or deleted while that build exists.
    #[tokio::test]
    async fn values_an_index_cannot_hold_can_be_fixed_while_its_build_is_blocked() {
        for (definition, value) in [
            (
                SecondaryIndexDefinition::node_equality("User", "email"),
                PropertyValue::String("x".repeat(1_100_000)),
            ),
            (
                SecondaryIndexDefinition::node_range("User", "email"),
                PropertyValue::Bool(true),
            ),
        ] {
            let db = test_db("secondary-blocked-source-fix").await;
            let scope = DataScope::LegacyUnscoped;
            let definition = validated(definition.expect("test definition is valid"));
            let before = vec![
                Property::string("$label", "User"),
                Property::new("email", value),
            ];
            put_source(&db, scope, IndexElementKind::Node, 0, &before).await;
            let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let mut claim_sequence = 1;
            assert!(matches!(
                drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
                CommittedOperationStep::Blocked
            ));
            let fixed = user_properties("fixed@example.com");
            mutate_source(&db, scope, IndexElementKind::Node, 0, &before, &fixed)
                .await
                .expect("the blocking value can be replaced");
            mutate_source(&db, scope, IndexElementKind::Node, 0, &fixed, &[])
                .await
                .expect("the entity can then be deleted");
            db.close().await.expect("secondary test database closes");
        }
    }

    #[tokio::test]
    async fn driver_owned_catch_up_executes_the_legacy_exact_delta_contract() {
        let db = test_db("secondary-driver-owned-catch-up").await;
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
            .expect("building mutation stores one durable delta");
        let operation = read_operation(&db, scope, operation_id)
            .await
            .expect("catch-up operation is readable")
            .expect("catch-up operation exists");
        assert!(matches!(
            operation.progress(),
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::CatchUp(_)
            ))
        ));

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("driver-owned catch-up transaction begins");
        driver
            .step(&db, &transaction, scope, &operation, one_entity_limits())
            .await
            .expect("driver-owned catch-up executes");
        transaction
            .commit()
            .await
            .expect("driver-owned catch-up commits");

        assert!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .is_empty()
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn driver_owned_catch_up_stops_before_the_next_indivisible_write_plan() {
        let db = test_db("secondary-driver-owned-catch-up-output-boundary").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let first_before = user_properties("first-before@example.com");
        let second_before = user_properties("second-before@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first_before).await;
        put_source(&db, scope, IndexElementKind::Node, 1, &second_before).await;
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        assert_eq!(
            drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );

        mutate_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &first_before,
            &user_properties("first-after@example.com"),
        )
        .await
        .expect("first mutation stores a delta");
        mutate_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &second_before,
            &user_properties("second-after@example.com"),
        )
        .await
        .expect("second mutation stores a delta");
        let operation = read_operation(&db, scope, operation_id)
            .await
            .expect("catch-up operation is readable")
            .expect("catch-up operation exists");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("bounded catch-up transaction begins");
        let one_update_limits = SearchIndexBatchLimits::try_new(
            std::num::NonZeroUsize::new(2).expect("two entities are positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(4).expect("four writes are positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
            std::num::NonZeroU64::new(1024 * 1024).expect("one MiB is positive"),
        )
        .expect("one-update limits are internally consistent");
        let execution = driver
            .step(&db, &transaction, scope, &operation, one_update_limits)
            .await
            .expect("bounded driver-owned catch-up executes");
        assert!(
            matches!(
                execution.result(),
                IndexOperationStepResult::Progressed(IndexOperationProgress::SecondaryBuild(
                    SecondaryBuildProgress::Constructing(SecondaryBuildStage::CatchUp(_))
                ))
            ),
            "a drain the output budget stops stays in catch-up"
        );
        transaction
            .commit()
            .await
            .expect("bounded driver-owned catch-up commits");

        assert_eq!(
            generation_rows(&db, scope, RecordKind::BuildDelta, index_id, generation,)
                .await
                .len(),
            1,
            "the second indivisible reconciliation remains durable"
        );
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn unique_catch_up_observes_earlier_writes_in_the_same_batch() {
        let db = test_db("secondary-unique-catch-up-batch-conflict").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("unique definition"),
        );
        let first_before = user_properties("first@example.com");
        let second_before = user_properties("second@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first_before).await;
        put_source(&db, scope, IndexElementKind::Node, 1, &second_before).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 1).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;

        assert_eq!(
            drive_one(&db, &driver, operation_id, &mut claim_sequence).await,
            CommittedOperationStep::Progressed
        );
        let duplicate = user_properties("duplicate@example.com");
        mutate_source(
            &db,
            scope,
            IndexElementKind::Node,
            0,
            &first_before,
            &duplicate,
        )
        .await
        .expect("first building mutation stores its delta atomically");
        mutate_source(
            &db,
            scope,
            IndexElementKind::Node,
            1,
            &second_before,
            &duplicate,
        )
        .await
        .expect("second building mutation stores its delta atomically");

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
            RecordKind::SecondaryEqualityBitmap,
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
            RecordKind::SecondaryEqualityBitmap,
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
            RecordKind::SecondaryEntry,
            RecordKind::SecondaryEqualityBitmap,
            RecordKind::BuildDelta,
            RecordKind::AppliedState,
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
        let tenant_a = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(1));
        let tenant_b = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(2));
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
            RecordKind::SecondaryEqualityBitmap,
            index_a,
            generation_a,
        )
        .await
        .is_empty());
        assert_eq!(
            generation_rows(
                &db,
                tenant_b,
                RecordKind::SecondaryEqualityBitmap,
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
    async fn every_tenant_build_and_drop_stage_resumes_after_database_reopen() {
        let store = Arc::new(InMemory::new());
        let path = "secondary-reopen-every-stage";
        let mut db = Db::builder(path, store.clone())
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("reopen test database opens");
        bootstrap_writer(&db)
            .await
            .expect("reopen test database bootstraps");
        let scope = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(
            0xABCD,
        ));
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
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let step = drive_one(&db, &driver, build_id, &mut claim_sequence).await;
            if step == CommittedOperationStep::Completed {
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
            db.close().await.expect("checkpoint flushes before reopen");
            db = Db::builder(path, store.clone())
                .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
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
                SecondaryCleanupProgress::DeleteEntries(_) => "delete_entries",
                SecondaryCleanupProgress::DeleteDeltas(_) => "delete_deltas",
                SecondaryCleanupProgress::Finalize(_) => "finalize",
            });
            let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
            let step = drive_one(&db, &driver, drop_id, &mut claim_sequence).await;
            if step == CommittedOperationStep::Completed {
                break;
            }
            assert_eq!(step, CommittedOperationStep::Progressed);
            db.close()
                .await
                .expect("cleanup checkpoint flushes before reopen");
            db = Db::builder(path, store.clone())
                .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
                .build()
                .await
                .expect("database reopens after cleanup checkpoint");
        }
        assert_eq!(
            cleanup_stages,
            BTreeSet::from(["delete_deltas", "delete_entries", "finalize"])
        );
        db.close().await.expect("reopen test database closes");
    }

    /// Authoritative verification reads only the label and indexed property,
    /// with any-label and first-property semantics over duplicate names, and
    /// fails on a corrupt row exactly when it reads one.
    #[tokio::test]
    async fn authoritative_verification_reads_label_and_property_like_the_full_decoder() {
        let db = test_db("secondary-authoritative-verification-golden").await;
        let scope = DataScope::LegacyUnscoped;
        let key = |id: u64| {
            authoritative_property_key(
                scope,
                IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(id),
                },
            )
        };
        let a = PropertyValue::String("a".into());
        let rows = [
            vec![
                Property::string("$label", "User"),
                Property::string("email", "a"),
            ],
            vec![
                Property::string("$label", "Other"),
                Property::string("$label", "User"),
                Property::string("email", "a"),
            ],
            vec![
                Property::string("$label", "User"),
                Property::string("email", "b"),
                Property::string("email", "a"),
            ],
            vec![Property::string("$label", "User")],
            vec![Property::string("email", "a")],
            Vec::new(),
            vec![
                Property::string("$label", "User"),
                Property::i64("email", 5),
            ],
            vec![
                Property::f32_array("embedding", vec![0.25; 1536]),
                Property::string("email", "a"),
                Property::bytes("blob", vec![9; 10_000]),
                Property::string("$label", "User"),
            ],
        ];
        for (id, properties) in (1_u64..).zip(&rows) {
            db.put(key(id), encode_properties(properties))
                .await
                .unwrap();
        }
        db.put(key(9), Bytes::from_static(b"corrupt authority"))
            .await
            .unwrap();
        let label = UnindexedLabel {
            scope,
            kind: IndexElementKind::Node,
            label: "User",
            property: "email",
        };
        let deadline = || Ok(());
        let verified = |accept: fn(Option<&PropertyValue>) -> bool, ids: Vec<u64>| {
            verified_unindexed_rows(
                &db,
                UnindexedLabel { ..label },
                roaring::RoaringTreemap::from_iter(ids),
                accept,
                &deadline,
                None,
            )
        };
        let rows_of =
            |bitmap: crate::query_resources::bitmap::Bitmap| bitmap.iter().collect::<Vec<_>>();
        assert_eq!(
            rows_of(
                verified(
                    |value| value == Some(&PropertyValue::String("a".into())),
                    (1..=8).chain([10]).collect()
                )
                .await
                .unwrap()
            ),
            vec![1, 2, 8]
        );
        assert_eq!(
            rows_of(
                verified(|value| value.is_none(), (1..=8).collect())
                    .await
                    .unwrap()
            ),
            vec![4]
        );
        assert!(matches!(
            verified(|_| true, (1..=9).collect()).await,
            Err(HelixDbError::Encoding(_))
        ));

        let ValidatedDynamicIndexDefinition::Secondary(equality) =
            validated(SecondaryIndexDefinition::node_equality("User", "email").unwrap())
        else {
            unreachable!("equality definition is secondary");
        };
        let ValidatedDynamicIndexDefinition::Secondary(range) =
            validated(SecondaryIndexDefinition::node_range("User", "email").unwrap())
        else {
            unreachable!("range definition is secondary");
        };
        let RangeValueProjection::Indexed(range_a) =
            project_range_value(&a, StorageRangeIndexDirection::Asc)
        else {
            unreachable!("strings are range indexed");
        };
        let mut equal = Vec::new();
        let mut in_range = Vec::new();
        for id in (1..=8).chain([10]) {
            let entity = IndexEntityId::new(id);
            if authoritative_equality_matches(&db, scope, &equality, entity, &a, None)
                .await
                .unwrap()
            {
                equal.push(id);
            }
            if authoritative_range_matches(
                &db,
                scope,
                &range,
                entity,
                StorageRangeIndexDirection::Asc,
                &range_a,
                None,
                &exact::UnobservedRangeScan,
            )
            .await
            .unwrap()
            {
                in_range.push(id);
            }
        }
        assert_eq!(equal, vec![1, 2, 8]);
        assert_eq!(in_range, vec![1, 2, 8]);
        let corrupt = IndexEntityId::new(9);
        assert!(matches!(
            authoritative_equality_matches(&db, scope, &equality, corrupt, &a, None).await,
            Err(HelixDbError::Encoding(_))
        ));
        assert!(matches!(
            authoritative_range_matches(
                &db,
                scope,
                &range,
                corrupt,
                StorageRangeIndexDirection::Asc,
                &range_a,
                None,
                &exact::UnobservedRangeScan,
            )
            .await,
            Err(HelixDbError::Encoding(_))
        ));
        let entity = |id: u64| IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(id),
        };
        for id in 1..=8 {
            let stored = db.get(key(id)).await.unwrap().unwrap();
            let decoded = decode_properties(&stored).unwrap();
            let read = read_authoritative_properties(&db, scope, &equality, entity(id))
                .await
                .unwrap()
                .unwrap();
            for definition in [&equality, &range] {
                assert_eq!(
                    format!(
                        "{:?}",
                        canonical_value(definition, &read, IndexEntityId::new(id))
                    ),
                    format!(
                        "{:?}",
                        canonical_value(definition, &decoded, IndexEntityId::new(id))
                    ),
                    "{id}"
                );
            }
        }
        assert!(
            read_authoritative_properties(&db, scope, &equality, entity(10))
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            read_authoritative_properties(&db, scope, &equality, entity(9)).await,
            Err(HelixDbError::Encoding(_))
        ));
        db.close().await.unwrap();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/index_lifecycle_secondary_contracts.rs"]
mod external_contracts;

#[cfg(test)]
pub(crate) use exact::RangeScanCounters;

#[cfg(test)]
mod ordered_tests;

#[cfg(test)]
mod test_read_counters;

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../tests/production_support/secondary_unique_batch.rs"]
mod unique_batch_contracts;
#[cfg(all(feature = "production-coverage", not(test)))]
pub(crate) use unique_batch_contracts::run as run_unique_batch_production_contracts;
