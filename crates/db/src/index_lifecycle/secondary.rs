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

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::ops::Bound;
#[cfg(any(test, feature = "production-coverage"))]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{DbReadOps, DbTransaction};

use crate::config::{
    IndexLifecycleScanTuning, RangeIndexDirection, SearchIndexBatchLimits,
    SecondaryIndexLifecycleCatchUpTailDelayMillis,
};
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, Property};
#[cfg(test)]
use crate::encoding::v2::keys::metadata::MetadataKey;
use crate::encoding::v2::keys::scope::DataScope;
#[cfg(test)]
use crate::encoding::v2::keys::GlobalKeyKind;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{
    CanonicalSecondaryValue, IndexEntity, IndexEntityStateKey, RecordKind, ScopedKey,
    SecondaryEntryKey, SecondaryEntryLane, SecondaryEqualityBitmapKey,
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
use crate::encoding::v2::values::{
    decode_applied_state, decode_build_delta, decode_index_record, decode_secondary_entry,
    encode_applied_state, encode_build_delta, encode_secondary_entry, SecondaryEqualityBitmapValue,
};
use crate::error::{HelixDbError, Result, SecondaryIndexValueError};
use crate::index_lifecycle::outbox::{
    IndexOperationDriver, IndexOperationStepExecution, IndexOperationStepPermit,
    IndexOperationStepResult, PreparedIndexOperationStep,
};
use crate::index_lifecycle::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaValue, SecondaryEntryValue,
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
#[cfg(all(feature = "production-coverage", not(test)))]
pub(crate) use exact::run_production_contracts as run_exact_production_contracts;
#[cfg(test)]
pub(crate) use exact::scan_active_range_generation_with_membership;
pub(crate) use exact::{
    count_active_range_generation_with_membership, lookup_active_equality_literal_batch,
    lookup_active_equality_point_literal, record_equality_graph_read,
};

#[cfg(any(test, feature = "production-coverage"))]
static BENCHMARK_POINT_READS: AtomicU64 = AtomicU64::new(0);
#[cfg(any(test, feature = "production-coverage"))]
static BENCHMARK_MULTI_GETS: AtomicU64 = AtomicU64::new(0);
#[cfg(any(test, feature = "production-coverage"))]
static BENCHMARK_SCANS: AtomicU64 = AtomicU64::new(0);
#[cfg(any(test, feature = "production-coverage"))]
static BENCHMARK_GRAPH_READS: AtomicU64 = AtomicU64::new(0);

/// Exact storage operations issued by managed equality serving while the
/// production-coverage benchmark is measuring it.
#[cfg(any(test, feature = "production-coverage"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SecondaryEqualityReadMetrics {
    pub(crate) point_reads: u64,
    pub(crate) multi_get_calls: u64,
    pub(crate) scans: u64,
    pub(crate) graph_reads: u64,
}

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn reset_equality_read_metrics() {
    BENCHMARK_POINT_READS.store(0, AtomicOrdering::Relaxed);
    BENCHMARK_MULTI_GETS.store(0, AtomicOrdering::Relaxed);
    BENCHMARK_SCANS.store(0, AtomicOrdering::Relaxed);
    BENCHMARK_GRAPH_READS.store(0, AtomicOrdering::Relaxed);
}

#[cfg(any(test, feature = "production-coverage"))]
pub(crate) fn equality_read_metrics() -> SecondaryEqualityReadMetrics {
    SecondaryEqualityReadMetrics {
        point_reads: BENCHMARK_POINT_READS.load(AtomicOrdering::Relaxed),
        multi_get_calls: BENCHMARK_MULTI_GETS.load(AtomicOrdering::Relaxed),
        scans: BENCHMARK_SCANS.load(AtomicOrdering::Relaxed),
        graph_reads: BENCHMARK_GRAPH_READS.load(AtomicOrdering::Relaxed),
    }
}

/// Records one logical point read issued by the complete equality-serving path.
#[inline]
pub(crate) fn record_equality_point_read() {
    #[cfg(any(test, feature = "production-coverage"))]
    BENCHMARK_POINT_READS.fetch_add(1, AtomicOrdering::Relaxed);
}

/// Family driver sharing the lifecycle scope gate.
pub(crate) struct SecondaryIndexDriver {
    scope_gates: Arc<IndexScopeGates>,
    catch_up_tail_delay_millis: NonZeroU64,
    scan_tuning: IndexLifecycleScanTuning,
}

impl core::fmt::Debug for SecondaryIndexDriver {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("SecondaryIndexDriver")
            .field(
                "catch_up_tail_delay_millis",
                &self.catch_up_tail_delay_millis,
            )
            .finish()
    }
}

impl SecondaryIndexDriver {
    /// Installs secondary lifecycle work against the mutation authority.
    pub(crate) fn with_catch_up_delay(
        scope_gates: Arc<IndexScopeGates>,
        catch_up_tail_delay_millis: SecondaryIndexLifecycleCatchUpTailDelayMillis,
    ) -> Self {
        Self {
            scope_gates,
            catch_up_tail_delay_millis: NonZeroU64::new(catch_up_tail_delay_millis.get())
                .expect("validated catch-up tail delay is positive"),
            scan_tuning: IndexLifecycleScanTuning::default(),
        }
    }

    /// Applies runtime source-scan prefetching without admitting blocks to cache.
    pub(crate) const fn with_scan_tuning(mut self, scan_tuning: IndexLifecycleScanTuning) -> Self {
        self.scan_tuning = scan_tuning;
        self
    }

    /// Creates an isolated family driver for unit tests.
    #[cfg(test)]
    fn new(scope_gates: Arc<IndexScopeGates>) -> Self {
        Self::with_catch_up_delay(
            scope_gates,
            SecondaryIndexLifecycleCatchUpTailDelayMillis::new(1)
                .expect("test catch-up delay is positive"),
        )
    }
}

/// Bounded advisory delta selection consumed by one repository transaction.
pub(crate) struct PreparedSecondaryOperationStep {
    catch_up: PreparedSecondaryCatchUp,
}

enum PreparedSecondaryCatchUp {
    /// Exact durable delta keys selected without adding an SSI range read.
    Exact {
        keys: Vec<IndexEntityStateKey>,
        observed_prefix_exhausted: bool,
        tail_delay_millis: NonZeroU64,
    },
    /// Empty advisory scan; the retained exclusive permit authorizes a final scan.
    FinalBarrier,
}

impl PreparedSecondaryOperationStep {
    /// Stages the prepared exact-key batch or exclusive empty-prefix barrier.
    pub(crate) async fn stage(
        &self,
        transaction: &DbTransaction,
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
        let IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
            SecondaryBuildStage::CatchUp(progress),
        )) = operation.progress()
        else {
            return Err(corruption(
                "prepared secondary catch-up received another operation stage",
            ));
        };
        let result = match &self.catch_up {
            PreparedSecondaryCatchUp::Exact {
                keys,
                observed_prefix_exhausted,
                tail_delay_millis,
            } => {
                catch_up_exact(
                    transaction,
                    scope,
                    operation,
                    definition,
                    progress,
                    limits,
                    keys,
                    *observed_prefix_exhausted,
                    *tail_delay_millis,
                )
                .await
            }
            PreparedSecondaryCatchUp::FinalBarrier => {
                catch_up(transaction, scope, operation, definition, progress, limits).await
            }
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
        #[cfg(feature = "production-coverage")]
        let terminal_deletion = matches!(
            transition,
            super::graph_mutation::GraphMutationTransition::Delete { .. }
        );
        #[cfg(feature = "production-coverage")]
        let mut observed_terminal_deletions = 0usize;
        for ordinal in routes.iter().filter_map(|target| match target {
            super::mutation_catalog::MutationRouteTarget::Secondary(ordinal) => Some(ordinal),
            super::mutation_catalog::MutationRouteTarget::Vector(_)
            | super::mutation_catalog::MutationRouteTarget::TextBuilding(_)
            | super::mutation_catalog::MutationRouteTarget::TextActive(_) => None,
        }) {
            let target = mutations.targets.get(ordinal).ok_or_else(|| {
                corruption("secondary mutation route named a target outside its catalog")
            })?;
            let old_value = canonical_value(&target.definition, before, entity.id)
                .map_err(|error| mutation_value_error(&target.definition, entity.id, error))?;
            let new_value = canonical_value(&target.definition, after, entity.id)
                .map_err(|error| mutation_value_error(&target.definition, entity.id, error))?;
            if old_value == new_value {
                continue;
            }
            #[cfg(feature = "production-coverage")]
            if terminal_deletion && old_value.is_some() {
                observed_terminal_deletions = observed_terminal_deletions.saturating_add(1);
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
        #[cfg(feature = "production-coverage")]
        crate::execution::interpreter::mutation::benchmark_telemetry::record_secondary_deletions(
            observed_terminal_deletions,
        );
        Ok(())
    }

    /// Observes exclusive keys once, applies changes in input order, and clears the epoch.
    pub(crate) async fn flush(
        &mut self,
        transaction: &DbTransaction,
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
                    let key = scoped_index_key(
                        change.scope,
                        ScopedKey::BuildDelta(IndexEntityStateKey {
                            index_id: target.index_id,
                            generation: target.generation,
                            entity: change.entity,
                        }),
                    );
                    let value = CoalescedBuildDeltaValue {
                        index_id: target.index_id,
                        generation: target.generation,
                        entity_kind: change.entity.kind,
                        entity_id: change.entity.id,
                    };
                    transaction.put(key, encode_build_delta(&value))?;
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
        transaction: &DbTransaction,
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
    transaction: &DbTransaction,
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
                    ScopedKey::BuildDelta(IndexEntityStateKey {
                        index_id: target.index_id,
                        generation: target.generation,
                        entity,
                    }),
                );
                let value = CoalescedBuildDeltaValue {
                    index_id: target.index_id,
                    generation: target.generation,
                    entity_kind,
                    entity_id,
                };
                transaction.put(key, encode_build_delta(&value))?;
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
                SecondaryBuildStage::Validate(_) | SecondaryBuildStage::Activate(_)
            )) | IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(_))
                | IndexOperationProgress::SecondaryCleanup(_)
        );
        if needs_exclusive {
            return Ok(Box::new(self.scope_gates.lifecycle_permit(scope).await));
        }
        Ok(Box::new(()))
    }

    async fn prepare_step(
        &self,
        db: &slatedb::Db,
        scope: DataScope,
        operation: &IndexOperationRecord,
        limits: SearchIndexBatchLimits,
    ) -> Result<PreparedIndexOperationStep> {
        if matches!(
            operation.progress(),
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::CatchUp(_)
            ))
        ) {
            let prepared = prepare_secondary_catch_up(
                db,
                scope,
                operation,
                limits,
                self.catch_up_tail_delay_millis,
            )
            .await?;
            let permit: Box<dyn IndexOperationStepPermit> = match &prepared.catch_up {
                PreparedSecondaryCatchUp::Exact { .. } => Box::new(()),
                PreparedSecondaryCatchUp::FinalBarrier => {
                    Box::new(self.scope_gates.lifecycle_permit(scope).await)
                }
            };
            return Ok(PreparedIndexOperationStep::secondary(permit, prepared));
        }
        let permit = self.acquire_step_permit(scope, operation).await?;
        Ok(PreparedIndexOperationStep::driver_owned(
            self.family(),
            permit,
        ))
    }

    async fn step(
        &self,
        _db: &slatedb::Db,
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

async fn prepare_secondary_catch_up(
    db: &slatedb::Db,
    scope: DataScope,
    operation: &IndexOperationRecord,
    limits: SearchIndexBatchLimits,
    tail_delay_millis: NonZeroU64,
) -> Result<PreparedSecondaryOperationStep> {
    let prefix = generation_prefix(
        scope,
        RecordKind::BuildDelta,
        operation.index_id(),
        operation.generation(),
    );
    let mut rows = db.scan_prefix(&prefix, ..).await?;
    let mut keys = Vec::with_capacity(limits.max_entities().get());
    let mut input_bytes = 0_u64;
    let mut observed_prefix_exhausted = true;
    while keys.len() < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let row_input_bytes =
            u64::try_from(row.key.len().saturating_add(row.value.len())).unwrap_or(u64::MAX);
        if !keys.is_empty()
            && input_bytes.saturating_add(row_input_bytes) > limits.max_input_bytes().get()
        {
            observed_prefix_exhausted = false;
            break;
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
        input_bytes = input_bytes.saturating_add(row_input_bytes);
    }
    if keys.len() == limits.max_entities().get() {
        observed_prefix_exhausted = false;
    }
    let catch_up = if keys.is_empty() {
        PreparedSecondaryCatchUp::FinalBarrier
    } else {
        PreparedSecondaryCatchUp::Exact {
            keys,
            observed_prefix_exhausted,
            tail_delay_millis,
        }
    };
    Ok(PreparedSecondaryOperationStep { catch_up })
}

async fn step_build(
    transaction: &DbTransaction,
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
            if generation_has_rows(
                transaction,
                scope,
                RecordKind::BuildDelta,
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
                RecordKind::AppliedState,
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
        previous_value: Option<CanonicalSecondaryValue>,
        next_value: Option<CanonicalSecondaryValue>,
        applied_key: Bytes,
    },
}

async fn scan_source(
    transaction: &DbTransaction,
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
    let mut rows = transaction
        .scan_prefix_with_options(
            &source_prefix,
            (start, Bound::Included(upper)),
            &scan_options,
        )
        .await?;
    let mut candidates = Vec::with_capacity(limits.max_entities().get());
    let mut candidate_input_bytes = 0_u64;
    let mut exhausted = true;
    while candidates.len() < limits.max_entities().get() {
        let Some(row) = rows.next().await? else {
            break;
        };
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if candidate_input_bytes.saturating_add(input_bytes) > limits.max_input_bytes().get() {
            if candidates.is_empty() {
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
            candidates.push(SourceScanCandidate {
                cursor: complete_cursor,
                input_bytes,
                kind: SourceScanCandidateKind::NonEntity,
            });
            candidate_input_bytes = candidate_input_bytes.saturating_add(input_bytes);
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
                let previous_value = decode_previous_applied(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    entity,
                    &applied_key,
                    applied_value.as_deref(),
                )?;
                let mut unique_entry_keys = Vec::new();
                if definition.unique() && previous_value != next_value {
                    for value in [previous_value.as_ref(), next_value.as_ref()]
                        .into_iter()
                        .flatten()
                    {
                        unique_entry_keys.push(secondary_entry_key(
                            scope,
                            operation.index_id(),
                            operation.generation(),
                            definition,
                            value.clone(),
                            entity.id,
                        )?);
                    }
                    unique_keys.extend(unique_entry_keys.iter().cloned());
                }
                ObservedSourceScanCandidateKind::Entity {
                    entity,
                    previous_value,
                    next_value,
                    applied_key,
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
                previous_value,
                next_value,
                applied_key,
            } => {
                let plan = match reconciliation_plan_from_observations(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    definition,
                    entity,
                    applied_key,
                    previous_value,
                    next_value,
                    &unique_entries,
                )? {
                    ReconciliationPlan::Writes(plan) => plan,
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
        RecordKind::BuildDelta,
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
        plan.stage(transaction).await?;
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

struct ExactCatchUpRow {
    delta_key: Bytes,
    entity: IndexEntity,
    input_bytes: u64,
    applied_key: Bytes,
    previous_value: Option<CanonicalSecondaryValue>,
    next_value: Option<CanonicalSecondaryValue>,
    unique_entry_keys: Vec<Bytes>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "the prepared catch-up boundary requires exact operation, definition, limits, keys, and scheduling policy"
)]
async fn catch_up_exact(
    transaction: &DbTransaction,
    scope: DataScope,
    operation: &IndexOperationRecord,
    definition: &ValidatedSecondaryIndexDefinition,
    progress: &PrefixScanProgress,
    limits: SearchIndexBatchLimits,
    prepared_keys: &[IndexEntityStateKey],
    observed_prefix_exhausted: bool,
    tail_delay_millis: NonZeroU64,
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
        ));
    }

    let property_keys = decoded
        .iter()
        .map(|(_, entity, _)| authoritative_property_key(scope, *entity))
        .collect::<Vec<_>>();
    let property_values = transaction.multi_get(&property_keys).await?;
    let applied_keys = decoded
        .iter()
        .map(|(_, entity, _)| {
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
    for ((((delta_key, entity, delta_value_bytes), property_key), property_value), applied_pair) in
        decoded
            .into_iter()
            .zip(property_keys)
            .zip(property_values)
            .zip(applied_keys.into_iter().zip(applied_values))
    {
        let next_value = match property_value.as_ref() {
            Some(properties) => {
                let properties = decode_properties(properties)?;
                match canonical_value(definition, &properties, entity.id) {
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
        let previous_value = decode_previous_applied(
            scope,
            operation.index_id(),
            operation.generation(),
            entity,
            &applied_key,
            applied_value.as_deref(),
        )?;
        let mut unique_entry_keys = Vec::new();
        if definition.unique() && previous_value != next_value {
            if let Some(previous) = previous_value.as_ref() {
                unique_entry_keys.push(secondary_entry_key(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    definition,
                    previous.clone(),
                    entity.id,
                )?);
            }
            if let Some(next) = next_value.as_ref() {
                unique_entry_keys.push(secondary_entry_key(
                    scope,
                    operation.index_id(),
                    operation.generation(),
                    definition,
                    next.clone(),
                    entity.id,
                )?);
            }
            unique_keys.extend(unique_entry_keys.iter().cloned());
        }
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
            applied_key,
            previous_value,
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
    let mut processed = 0_usize;
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
        let plan = match reconciliation_plan_from_observations(
            scope,
            operation.index_id(),
            operation.generation(),
            definition,
            row.entity,
            row.applied_key,
            row.previous_value,
            row.next_value,
            &unique_entries,
        )? {
            ReconciliationPlan::Writes(mut plan) => {
                plan.delete(row.delta_key);
                plan
            }
            ReconciliationPlan::Blocked(blocker) => {
                return Ok(IndexOperationStepResult::Blocked(blocker));
            }
        };
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
        accounting.admit_scan(input_bytes, Some(&plan))?;
        processed = processed.saturating_add(1);
    }
    let counters = accounting.finish()?;
    let progress = IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
        SecondaryBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters,
        }),
    ));
    if processed < prepared_keys.len() || !observed_prefix_exhausted {
        return Ok(IndexOperationStepResult::Progressed(progress));
    }
    Ok(IndexOperationStepResult::ProgressedAfter {
        progress,
        delay_millis: tail_delay_millis,
    })
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
        RecordKind::BuildDelta,
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
        RecordKind::AppliedState,
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
    transaction: &DbTransaction,
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
    transaction: &DbTransaction,
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
                | ScopedKey::TextStatisticsEntity(_) => {
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
    transaction: &DbTransaction,
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
                        | RecordKind::SecondaryEqualityBitmap => {
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
    transaction: &DbTransaction,
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
    transaction: &DbTransaction,
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
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id,
            generation,
            entity,
        }),
    );
    let applied_value = transaction.get(&applied_key).await?;
    let previous = decode_previous_applied(
        scope,
        index_id,
        generation,
        entity,
        &applied_key,
        applied_value.as_deref(),
    )?;
    let mut unique_entries = BTreeMap::new();
    if definition.unique() && previous != next_value {
        for value in [previous.as_ref(), next_value.as_ref()]
            .into_iter()
            .flatten()
        {
            let key = secondary_entry_key(
                scope,
                index_id,
                generation,
                definition,
                value.clone(),
                entity_id,
            )?;
            let observed = transaction.get(&key).await?;
            unique_entries.insert(key, observed);
        }
    }
    reconciliation_plan_from_observations(
        scope,
        index_id,
        generation,
        definition,
        entity,
        applied_key,
        previous,
        next_value,
        &unique_entries,
    )
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
    let AppliedFamilyState::Secondary(value) = applied.state else {
        return Err(corruption(
            "secondary generation contains another applied family",
        ));
    };
    Ok(value)
}

#[allow(
    clippy::too_many_arguments,
    reason = "reconciliation requires the exact generation, entity, before/after state, and observed unique rows"
)]
fn reconciliation_plan_from_observations(
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: &ValidatedSecondaryIndexDefinition,
    entity: IndexEntity,
    applied_key: Bytes,
    previous: Option<CanonicalSecondaryValue>,
    next_value: Option<CanonicalSecondaryValue>,
    unique_entries: &BTreeMap<Bytes, Option<Bytes>>,
) -> Result<ReconciliationPlan> {
    let entity_id = entity.id;
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
                    let Some(value) = unique_entries.get(&key).and_then(Option::as_ref) else {
                        break 'verify_previous;
                    };
                    let owner = decode_secondary_entry_value(
                        index_id,
                        generation,
                        definition_lane(definition),
                        value,
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
            if definition_uses_equality_bitmap(definition) {
                plan.bitmap_remove(key, entity_id);
            } else {
                plan.delete(key);
            }
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
                    let Some(value) = unique_entries.get(&key).and_then(Option::as_ref) else {
                        break 'verify_next;
                    };
                    let owner = decode_secondary_entry_value(index_id, generation, lane, value)?;
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
    }
    match next_value {
        Some(next) => {
            let value = AppliedEntityStateValue {
                index_id,
                generation,
                entity_kind: entity.kind,
                entity_id,
                state: AppliedFamilyState::Secondary(Some(next)),
            };
            plan.put(applied_key, encode_applied_state(&value));
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

    async fn stage(&self, transaction: &DbTransaction) -> Result<()> {
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
    transaction: &DbTransaction,
    changes: &BTreeMap<Bytes, BTreeMap<u64, bool>>,
) -> Result<()> {
    let exclusive_keys = changes
        .iter()
        .filter(|(_, changes)| changes.values().any(|present| !present))
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    let exclusive_values = if exclusive_keys.is_empty() {
        Vec::new()
    } else {
        transaction.multi_get(&exclusive_keys).await?
    };
    let mut exclusive_values = exclusive_keys
        .into_iter()
        .zip(exclusive_values)
        .collect::<BTreeMap<_, _>>();

    for (key, changes) in changes {
        if changes.values().all(|present| *present) {
            let additions = roaring::RoaringTreemap::from_iter(changes.keys().copied());
            transaction
                .merge_commutative(key, SecondaryEqualityBitmapValue::new(additions).encode())?;
            continue;
        }

        let existing = exclusive_values
            .remove(key)
            .expect("each removal-bearing bitmap key was read exactly once");
        let mut bitmap = existing
            .as_deref()
            .map(SecondaryEqualityBitmapValue::decode)
            .transpose()?
            .map(SecondaryEqualityBitmapValue::into_ids)
            .unwrap_or_default();
        for (entity_id, present) in changes {
            if *present {
                bitmap.insert(*entity_id);
            } else {
                bitmap.remove(*entity_id);
            }
        }
        if bitmap.is_empty() {
            transaction.delete(key)?;
        } else {
            transaction.put(key, SecondaryEqualityBitmapValue::new(bitmap).encode())?;
        }
    }
    assert!(exclusive_values.is_empty());
    Ok(())
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

pub(super) fn canonical_value(
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

#[derive(Debug, Clone, Copy)]
pub(super) enum SecondaryValueError {
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

/// Reads one exact Active equality generation from its typed physical row.
///
/// The caller must run this function inside the request lease batch associated
/// with `handle`. Unique entries and V4 non-unique bitmaps each use one point
/// read. Authoritative-null lookup remains a graph scan because nulls are not
/// physically indexed.
pub(crate) async fn lookup_active_equality_generation(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    value: &PropertyValue,
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

    let canonical = match project_equality_value(value) {
        EqualityValueProjection::Indexed(value) => CanonicalSecondaryValue::equality(value),
        EqualityValueProjection::AuthoritativeNull => {
            return scan_authoritative_null_equality(reader, handle, definition).await;
        }
        EqualityValueProjection::NonReflexive => return Ok(roaring::RoaringTreemap::new()),
        EqualityValueProjection::Unsupported(value_type) => {
            return Err(SecondaryIndexValueError::UnsupportedEqualityValue { value_type }.into());
        }
        EqualityValueProjection::Oversized {
            encoded_len,
            maximum,
        } => {
            return Err(SecondaryIndexValueError::EncodedKeyTooLarge {
                encoded_len,
                maximum,
            }
            .into());
        }
    };
    let lane = definition_lane(definition);
    if lane.is_unique() {
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            canonical,
            IndexEntityId::initial(),
        )?;
        record_equality_point_read();
        let Some(bytes) = reader.get(key).await? else {
            return Ok(roaring::RoaringTreemap::new());
        };
        let owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &bytes)?;
        #[cfg(any(test, feature = "production-coverage"))]
        BENCHMARK_GRAPH_READS.fetch_add(1, AtomicOrdering::Relaxed);
        if !authoritative_equality_matches(reader, handle.scope(), definition, owner, value).await?
        {
            return Err(corruption(
                "unique secondary equality owner differs from authoritative graph state",
            ));
        }
        return Ok(roaring::RoaringTreemap::from_iter([owner.get()]));
    }

    let key = secondary_entry_key(
        handle.scope(),
        handle.index_id(),
        handle.generation(),
        definition,
        canonical,
        IndexEntityId::initial(),
    )?;
    record_equality_point_read();
    reader
        .get(key)
        .await?
        .map(|bytes| {
            SecondaryEqualityBitmapValue::decode(&bytes)
                .map(SecondaryEqualityBitmapValue::into_ids)
                .map_err(HelixDbError::from)
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

/// Reads and unions equality values from one exact Active generation.
///
/// Non-unique indexed values use one `multi_get` over their V4 bitmap rows.
/// Unique, null, non-reflexive, and error projections retain the authoritative
/// single-value path so their verification contracts remain unchanged.
pub(crate) async fn lookup_active_equality_generations(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    values: &[PropertyValue],
) -> Result<roaring::RoaringTreemap> {
    if values.is_empty() {
        return Ok(roaring::RoaringTreemap::new());
    }
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary equality batch serving received a non-secondary Active handle",
        ));
    };
    if !definition_uses_equality_bitmap(definition) {
        let mut owners = roaring::RoaringTreemap::new();
        for value in values {
            owners |= lookup_active_equality_generation(reader, handle, value).await?;
        }
        return Ok(owners);
    }

    let mut canonical = Vec::with_capacity(values.len());
    for value in values {
        let EqualityValueProjection::Indexed(value) = project_equality_value(value) else {
            let mut owners = roaring::RoaringTreemap::new();
            for value in values {
                owners |= lookup_active_equality_generation(reader, handle, value).await?;
            }
            return Ok(owners);
        };
        canonical.push(CanonicalSecondaryValue::equality(value));
    }
    let mut keys = canonical
        .into_iter()
        .map(|value| {
            secondary_entry_key(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                definition,
                value,
                IndexEntityId::initial(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    keys.sort_unstable();
    keys.dedup();
    keys.iter().for_each(|_| record_equality_point_read());
    if keys.len() == 1 {
        return reader
            .get(
                keys.pop()
                    .expect("one-key equality batch remains non-empty"),
            )
            .await?
            .map(|bytes| {
                SecondaryEqualityBitmapValue::decode(&bytes)
                    .map(SecondaryEqualityBitmapValue::into_ids)
                    .map_err(HelixDbError::from)
            })
            .transpose()
            .map(Option::unwrap_or_default);
    }
    #[cfg(any(test, feature = "production-coverage"))]
    BENCHMARK_MULTI_GETS.fetch_add(1, AtomicOrdering::Relaxed);
    let mut owners = roaring::RoaringTreemap::new();
    for bytes in reader.multi_get(&keys).await?.into_iter().flatten() {
        owners |= SecondaryEqualityBitmapValue::decode(&bytes)?.into_ids();
    }
    Ok(owners)
}

async fn scan_authoritative_null_equality(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    definition: &ValidatedSecondaryIndexDefinition,
) -> Result<roaring::RoaringTreemap> {
    #[cfg(any(test, feature = "production-coverage"))]
    BENCHMARK_SCANS.fetch_add(1, AtomicOrdering::Relaxed);
    let prefix = source_prefix(handle.scope(), definition.element_kind());
    let mut rows = reader.scan_prefix(&prefix, ..).await?;
    let mut owners = roaring::RoaringTreemap::new();
    while let Some(row) = rows.next().await? {
        #[cfg(any(test, feature = "production-coverage"))]
        BENCHMARK_GRAPH_READS.fetch_add(1, AtomicOrdering::Relaxed);
        let Some(entity_id) = source_entity(handle.scope(), definition.element_kind(), &row.key)?
        else {
            continue;
        };
        let properties = decode_properties(&row.value)?;
        if properties_match_definition(definition, &properties)
            && properties
                .iter()
                .find(|property| property.name == definition.property().as_str())
                .is_none_or(|property| matches!(property.value, PropertyValue::Null))
        {
            owners.insert(entity_id.get());
        }
    }
    Ok(owners)
}

async fn authoritative_equality_matches(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    query: &PropertyValue,
) -> Result<bool> {
    let entity = IndexEntity {
        kind: definition.element_kind(),
        id: entity_id,
    };
    let Some(properties) = read_authoritative_properties(reader, scope, entity).await? else {
        return Ok(false);
    };
    if !properties_match_definition(definition, &properties) {
        return Ok(false);
    }
    Ok(properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
        .is_some_and(|property| property.value.eq_value(query)))
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
pub(crate) async fn scan_active_range_generation(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
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
        Some(query) => match secondary_range_scan_bounds(direction, query)? {
            Some(bounds) => bounds,
            None => return Ok(Vec::new()),
        },
        None => (Bound::Unbounded, Bound::Unbounded),
    };
    let prefix = IndexKey::data_prefix(
        handle.scope(),
        ScopedKey::secondary_lane_prefix(handle.index_id(), handle.generation(), lane),
    );
    let mut rows = reader.scan_prefix(&prefix, bounds).await?;
    let mut owners = Vec::new();
    while let Some(row) = rows.next().await? {
        let IndexKey::Data {
            kind: ScopedKey::SecondaryEntry(key),
            ..
        } = IndexKey::parse_from_slice(handle.scope(), &row.key)?
        else {
            return Err(corruption(
                "secondary range prefix yielded another key kind",
            ));
        };
        if key.index_id() != handle.index_id()
            || key.generation() != handle.generation()
            || key.lane() != lane
        {
            return Err(corruption(
                "secondary range entry escaped its exact serving prefix",
            ));
        }
        let Some(key_owner) = key.entity_id() else {
            return Err(corruption("secondary range entry omitted its key owner"));
        };
        let value_owner =
            decode_secondary_entry_value(handle.index_id(), handle.generation(), lane, &row.value)?;
        if key_owner != value_owner {
            return Err(corruption(
                "secondary range entry key/value owners disagree",
            ));
        }
        let Some(key_value) = key.range_value() else {
            return Err(corruption(
                "secondary range lane contains an equality value",
            ));
        };
        if !authoritative_range_matches(
            reader,
            handle.scope(),
            definition,
            value_owner,
            direction,
            key_value,
            query,
        )
        .await?
        {
            continue;
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
    query: &SecondaryRangeQuery,
) -> Result<Option<(Bound<Bytes>, Bound<Bytes>)>> {
    let physical = |value: &PropertyValue| project_query_range_value(value, direction);
    Ok(Some(match query {
        SecondaryRangeQuery::Lower { value, inclusive } => {
            let value = physical(value)?;
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
            let value = physical(value)?;
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
                return Err(SecondaryIndexValueError::NonComparableDynamicBounds {
                    lower_type: property_value_type_name(lower),
                    upper_type: property_value_type_name(upper),
                }
                .into());
            };
            if ordering.is_gt() || (ordering.is_eq() && (!*lower_inclusive || !*upper_inclusive)) {
                return Ok(None);
            }
            let lower = physical(lower)?;
            let upper = physical(upper)?;
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

fn project_query_range_value(
    value: &PropertyValue,
    direction: StorageRangeIndexDirection,
) -> Result<CanonicalRangeValue> {
    match project_range_value(value, direction) {
        RangeValueProjection::Indexed(value) => Ok(value),
        RangeValueProjection::Unsupported(value_type) => {
            Err(SecondaryIndexValueError::UnsupportedRangeValue { value_type }.into())
        }
        RangeValueProjection::NaN => Err(SecondaryIndexValueError::NaNRangeValue.into()),
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

async fn authoritative_range_matches(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    direction: StorageRangeIndexDirection,
    key_value: &CanonicalRangeValue,
    query: Option<&SecondaryRangeQuery>,
) -> Result<bool> {
    let entity = IndexEntity {
        kind: definition.element_kind(),
        id: entity_id,
    };
    let Some(properties) = read_authoritative_properties(reader, scope, entity).await? else {
        return Ok(false);
    };
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

pub(super) fn definition_uses_equality_bitmap(
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
        return Ok(scoped_index_key(
            scope,
            ScopedKey::SecondaryEqualityBitmap(key),
        ));
    }
    let lane = definition_lane(definition);
    let key = SecondaryEntryKey::try_new(
        index_id,
        generation,
        lane,
        value,
        (!lane.is_unique()).then_some(entity_id),
    )?;
    Ok(scoped_index_key(scope, ScopedKey::SecondaryEntry(key)))
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

async fn read_authoritative_properties(
    reader: &(impl DbReadOps + Sync),
    scope: DataScope,
    entity: IndexEntity,
) -> Result<Option<Vec<Property>>> {
    reader
        .get(authoritative_property_key(scope, entity))
        .await?
        .map(|bytes| decode_properties(&bytes).map_err(HelixDbError::from))
        .transpose()
}

pub(super) fn authoritative_property_key(scope: DataScope, entity: IndexEntity) -> Bytes {
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
    transaction: &DbTransaction,
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

pub(super) fn source_prefix(scope: DataScope, kind: IndexElementKind) -> Bytes {
    let prefix = match kind {
        IndexElementKind::Node => KeyPrefix::NodeProperty,
        IndexElementKind::Edge => KeyPrefix::EdgePropertyById,
    };
    DataKey::data_prefix(scope, Bytes::copy_from_slice(prefix.as_slice()))
}

pub(super) fn source_entity(
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
    use std::collections::BTreeSet;
    use std::time::Duration;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{ByteRangeBounds, Db, DbIterator, DbReadOps, IsolationLevel, KeyValue};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SecondaryIndexDefinition, VectorIndexDefinition,
    };
    use crate::encoding::v2::values::encode_index_record;
    use crate::encoding::v2::values::property::encode_properties;
    use crate::index_lifecycle::lifecycle::{
        create_index_operation, drop_index_operation, InitialBuildProgress,
    };
    use crate::index_lifecycle::outbox::{
        claim_operation, execute_claimed_step, observe_operation_pointer, read_operation,
        ClaimPermission, CommittedOperationStep, OperationPointerObservation,
    };
    use crate::index_lifecycle::repository::bootstrap_writer;
    use crate::index_lifecycle::{
        ClaimSequence, IndexDdlReceipt, IndexOperationExecutionState, IndexOperationId,
        IndexOperationKind, IndexRevision, IndexStateTransition, PhysicalGeneration,
        VectorGenerationDescriptor, VectorPhysicalIndexId, VectorPhysicalLayout, WriterEpoch,
    };

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

    async fn active_vector_read_handle(db: &Db) -> ActiveIndexHandle {
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
    async fn put_read_entry(db: &Db, handle: &ActiveIndexHandle, value: &str, entity_id: u64) {
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
                    &[membership.clone(), second_filter.clone()],
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
                    &[membership.clone(), second_filter.clone()],
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
        assert!(scan_active_range_generation_with_membership(
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
        .is_err());

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

    /// Covers the pure range-bound, diagnostic, and checked-arithmetic
    /// contracts that lifecycle integration tests otherwise exercise only
    /// through their successful branches.
    #[test]
    fn secondary_helper_boundaries_are_total_and_typed() {
        assert_eq!(
            format!(
                "{:?}",
                SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()))
            ),
            "SecondaryIndexDriver { catch_up_tail_delay_millis: 1 }"
        );

        let mut object = std::collections::BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::Bool(true));
        for (value, expected) in [
            (PropertyValue::Null, "Null"),
            (PropertyValue::Bool(true), "Bool"),
            (PropertyValue::I64(1), "I64"),
            (PropertyValue::DateTime(2), "DateTime"),
            (PropertyValue::F64(3.0), "F64"),
            (PropertyValue::F32(4.0), "F32"),
            (PropertyValue::String("value".to_string()), "String"),
            (PropertyValue::Bytes(vec![1]), "Bytes"),
            (PropertyValue::I64Array(vec![1]), "I64Array"),
            (PropertyValue::F64Array(vec![2.0]), "F64Array"),
            (PropertyValue::F32Array(vec![3.0]), "F32Array"),
            (
                PropertyValue::StringArray(vec!["value".to_string()]),
                "StringArray",
            ),
            (PropertyValue::Array(vec![PropertyValue::Null]), "Array"),
            (PropertyValue::Object(object), "Object"),
        ] {
            assert_eq!(property_value_type_name(&value), expected);
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
        for _ in 0..8 {
            let operation = read_operation(db, DataScope::LegacyUnscoped, operation_id)
                .await
                .expect("secondary operation is readable")
                .expect("secondary operation exists");
            if matches!(
                operation.progress(),
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::CatchUp(_)
                ))
            ) {
                return operation;
            }
            assert_eq!(
                drive_one(db, driver, operation_id, claim_sequence).await,
                CommittedOperationStep::Progressed
            );
        }
        panic!("secondary build did not reach catch-up")
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
        let db = test_db("secondary-shared-edge-bitmap-read").await;
        let scope = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(
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
        let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 7).await;
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
    }

    #[tokio::test]
    async fn removal_bearing_bitmap_changes_replace_or_delete_exclusively() {
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
    async fn pure_bitmap_additions_commit_without_conflicts() {
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

        let prepared = prepare_secondary_catch_up(
            &db,
            scope,
            &operation,
            one_entity_limits(),
            NonZeroU64::new(1).unwrap(),
        )
        .await
        .expect("one exact delta prepares");
        assert!(matches!(
            &prepared.catch_up,
            PreparedSecondaryCatchUp::Exact { keys, .. } if keys.len() == 1
        ));
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
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("first update records a build delta");
        let prepared = prepare_secondary_catch_up(
            &db,
            scope,
            &operation,
            one_entity_limits(),
            NonZeroU64::new(1).unwrap(),
        )
        .await
        .expect("same-entity delta prepares");
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
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("update records one build delta");
        let prepared = prepare_secondary_catch_up(
            &db,
            scope,
            &operation,
            SearchIndexBackfillLimits::default().batch(),
            NonZeroU64::new(1).unwrap(),
        )
        .await
        .expect("delta selection prepares");
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

    #[tokio::test]
    async fn only_the_empty_catch_up_barrier_waits_for_scope_mutations() {
        let db = test_db("secondary-catch-up-final-barrier").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let first = user_properties("first@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let gates = Arc::new(IndexScopeGates::default());
        let driver = SecondaryIndexDriver::new(Arc::clone(&gates));
        let mut claim_sequence = 1;
        let operation = drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;

        let mutation = gates.mutation_permit(scope).await;
        let final_barrier = driver.prepare_step(
            &db,
            scope,
            &operation,
            SearchIndexBackfillLimits::default().batch(),
        );
        tokio::pin!(final_barrier);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut final_barrier)
                .await
                .is_err(),
            "empty catch-up must wait for exclusive scope authority"
        );
        drop(mutation);
        tokio::time::timeout(Duration::from_secs(2), final_barrier)
            .await
            .expect("final barrier acquires after the mutation finishes")
            .expect("final barrier prepares");

        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("update records one exact delta");
        let mutation = gates.mutation_permit(scope).await;
        let exact = tokio::time::timeout(
            Duration::from_secs(2),
            driver.prepare_step(
                &db,
                scope,
                &operation,
                SearchIndexBackfillLimits::default().batch(),
            ),
        )
        .await
        .expect("exact-key preparation does not wait for the shared scope permit")
        .expect("exact-key catch-up prepares");
        assert_eq!(exact.family(), IndexOperationFamily::Secondary);
        drop(exact);
        drop(mutation);
        db.close().await.expect("secondary test database closes");
    }

    #[tokio::test]
    async fn exhausted_exact_tail_persists_the_configured_coalescing_deadline() {
        let db = test_db("secondary-catch-up-tail-delay").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = validated(
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("node equality definition"),
        );
        let first = user_properties("first@example.com");
        put_source(&db, scope, IndexElementKind::Node, 0, &first).await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, 0).await;
        let driver = SecondaryIndexDriver::with_catch_up_delay(
            Arc::new(IndexScopeGates::default()),
            SecondaryIndexLifecycleCatchUpTailDelayMillis::new(7).unwrap(),
        );
        let mut claim_sequence = 1;
        drive_until_catch_up(&db, &driver, operation_id, &mut claim_sequence).await;
        let updated = user_properties("updated@example.com");
        mutate_source(&db, scope, IndexElementKind::Node, 0, &first, &updated)
            .await
            .expect("update records one build delta");
        assert_eq!(
            drive_one_at(
                &db,
                &driver,
                operation_id,
                &mut claim_sequence,
                SearchIndexBackfillLimits::default().batch(),
                100,
            )
            .await,
            CommittedOperationStep::Progressed
        );
        let operation = read_operation(&db, scope, operation_id)
            .await
            .expect("delayed operation is readable")
            .expect("delayed operation exists");
        assert!(matches!(
            operation.execution_state(),
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: Some(107)
            }
        ));
        let writer_epoch = WriterEpoch::from_bytes([0x5A; 16]).unwrap();
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, 106)
                .await
                .unwrap(),
            OperationPointerObservation::Delayed { delay_millis: 1 }
        ));
        assert!(matches!(
            observe_operation_pointer(&db, operation_id, writer_epoch, 107)
                .await
                .unwrap(),
            OperationPointerObservation::Eligible(_)
        ));
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
        driver
            .step(&db, &transaction, scope, &operation, one_update_limits)
            .await
            .expect("bounded driver-owned catch-up executes");
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
            let driver = SecondaryIndexDriver::with_catch_up_delay(
                Arc::new(IndexScopeGates::default()),
                SecondaryIndexLifecycleCatchUpTailDelayMillis::new(1).unwrap(),
            );
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
            let driver = SecondaryIndexDriver::with_catch_up_delay(
                Arc::new(IndexScopeGates::default()),
                SecondaryIndexLifecycleCatchUpTailDelayMillis::new(1).unwrap(),
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
}

#[cfg(test)]
#[path = "../../tests/unit/index_lifecycle_secondary_contracts.rs"]
mod external_contracts;
