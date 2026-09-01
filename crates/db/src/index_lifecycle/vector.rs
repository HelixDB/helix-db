//! Generation-qualified vector mutation and lifecycle ownership.
//!
//! Ordinary graph mutations load one [`VectorMutationSet`] from canonical V2
//! records in their serializable transaction. A hidden `Building` generation
//! receives one coalesced entity delta; an `Active` generation mutates only the
//! physical namespace authorized by its canonical record and checked tenant
//! mapping. Missing tenant mappings are created only with the first mutation
//! work for that partition, never by a read.
//!
//! The same semantic document projection is used by active mutation and the
//! outbox builder. It validates labels, dimensions, finite f32 conversion,
//! cosine zero vectors, metric-specific component magnitude, and type-preserving
//! tenant identity before any HNSW or lifecycle row is staged.

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorKey, VectorStorageLane,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::keys::{IndexEntity, IndexEntityStateKey, ScopedKey};
use crate::encoding::v2::legacy::vector::transaction_guard::{
    decode_active_txn_guard, LegacyVectorTxnGuardKey,
};
#[cfg(any(test, feature = "index-lifecycle-testing"))]
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::property::encode_index_partition_value;
use crate::encoding::v2::values::{decode_build_delta, encode_build_delta};
use crate::error::{HelixDbError, Result};
use crate::search;
use crate::search::vector::{
    self, Distance, ValidatedMetricVector, VectorCacheWriteSet, VectorDimension,
    VectorDistanceMetric, VectorIndexConfig,
};

use super::repository;
use super::work::{CoalescedBuildDeltaState, CoalescedBuildDeltaValue, VectorTenantPartition};
#[cfg(any(test, feature = "index-lifecycle-testing"))]
use super::IndexStateV2;
use super::{
    ActiveIndexHandle, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, TextPartition,
    ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition, VectorPhysicalIndexId,
    VectorPhysicalLayout,
};

mod driver;
pub(crate) use driver::VectorIndexDriver;

/// Validated vector and its canonical physical-partition identity.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorIndexedDocument {
    partition: TextPartition,
    vector: Vec<f32>,
}

impl VectorIndexedDocument {
    /// Borrows the canonical partition used by mapping and applied-state rows.
    pub(crate) const fn partition(&self) -> &TextPartition {
        &self.partition
    }

    /// Borrows the exact validated f32 vector staged into HNSW.
    pub(crate) fn vector(&self) -> &[f32] {
        &self.vector
    }
}

/// One generation and its only legal ordinary-mutation behavior.
#[derive(Debug, Clone)]
struct VectorMutationTarget {
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: ValidatedVectorIndexDefinition,
    mode: VectorMutationMode,
}

/// Closed maintenance choice derived from canonical lifecycle state.
#[derive(Debug, Clone)]
enum VectorMutationMode {
    MaintainActive(ActiveIndexHandle),
    RecordBuildDelta,
}

/// Transaction-local vector generations loaded from canonical records.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorMutationSet {
    targets: Vec<VectorMutationTarget>,
}

/// Complete authoritative property transition for one graph entity.
#[derive(Debug, Clone, Copy)]
pub(crate) struct VectorEntityMutation<'a> {
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    before: &'a [Property],
    after: &'a [Property],
}

impl<'a> VectorEntityMutation<'a> {
    /// Binds one entity to its complete before/after property snapshots.
    #[cfg(any(
        test,
        feature = "production-coverage",
        feature = "index-lifecycle-testing"
    ))]
    pub(crate) const fn new(
        entity_kind: IndexElementKind,
        entity_id: u64,
        before: &'a [Property],
        after: &'a [Property],
    ) -> Self {
        Self {
            entity_kind,
            entity_id: IndexEntityId::new(entity_id),
            before,
            after,
        }
    }
}

impl VectorMutationSet {
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

    /// Classifies one same-snapshot canonical vector record.
    pub(super) fn include_catalog_entry(
        &mut self,
        entry: super::mutation_catalog::MutationCatalogEntry<'_>,
    ) -> Result<usize> {
        let (record, mode) = match entry {
            super::mutation_catalog::MutationCatalogEntry::Building(record) => {
                (record, VectorMutationMode::RecordBuildDelta)
            }
            super::mutation_catalog::MutationCatalogEntry::Active { record, handle } => {
                if !matches!(handle, ActiveIndexHandle::Vector { .. }) {
                    return Err(corruption(
                        "active vector record carried another family handle",
                    ));
                }
                (record, VectorMutationMode::MaintainActive(handle.clone()))
            }
        };
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            return Err(corruption(
                "vector mutation classifier received another family",
            ));
        };
        let ordinal = self.targets.len();
        self.targets.push(VectorMutationTarget {
            index_id: record.index_id(),
            generation: record.state().generation(),
            definition: definition.clone(),
            mode,
        });
        Ok(ordinal)
    }
}

/// Loads every vector generation whose state requires mutation work.
///
/// The canonical record scan is part of the caller's serializable graph
/// transaction. Activation/drop revisions therefore conflict with the graph
/// commit rather than allowing writes to cross a lifecycle boundary.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) async fn load_mutation_set(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<VectorMutationSet> {
    let logical_prefix =
        ScopedKey::logical_prefix(crate::encoding::v2::keys::RecordKind::IndexRecord);
    let physical_prefix = IndexKey::data_prefix(scope, logical_prefix);
    let mut rows = transaction.scan_prefix(&physical_prefix, ..).await?;
    let mut mutations = VectorMutationSet::default();
    while let Some(row) = rows.next().await? {
        let IndexKey::Data {
            kind: ScopedKey::IndexRecord(key),
            ..
        } = IndexKey::parse_from_slice(scope, &row.key)?
        else {
            return Err(corruption(
                "vector mutation catalog prefix yielded another key kind",
            ));
        };
        let record = decode_index_record(&row.value)?;
        if key.identity != *record.identity() {
            return Err(corruption(
                "vector mutation catalog key/value identity mismatch",
            ));
        }
        match record.definition() {
            ValidatedDynamicIndexDefinition::Vector(_) => {}
            ValidatedDynamicIndexDefinition::Secondary(_)
            | ValidatedDynamicIndexDefinition::Text(_) => continue,
        }
        let active_handle = match record.state() {
            IndexStateV2::Building { .. } => None,
            IndexStateV2::Active { .. } => Some(
                ActiveIndexHandle::try_from_record(scope, &record)
                    .ok_or_else(|| corruption("active vector record did not project a handle"))?,
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

/// Maintains every V2 vector generation affected by one graph entity.
///
/// `before` and `after` are complete authoritative property sets. Partition
/// moves therefore become a typed remove-plus-upsert, and hidden builds receive
/// one coalesced reconciliation marker for any semantic document change.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn maintain_entity_with_runtime(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &VectorMutationSet,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    entity: VectorEntityMutation<'_>,
) -> Result<()> {
    for target in mutations
        .targets
        .iter()
        .filter(|target| target.definition.element_kind() == entity.entity_kind)
    {
        maintain_target(transaction, scope, target, runtime, cache_writes, entity).await?;
    }
    Ok(())
}

async fn maintain_target(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    entity: VectorEntityMutation<'_>,
) -> Result<()> {
    let new_document = vector_document(&target.definition, entity.after)?;
    let (old_document, force_build_delta) = match vector_document(&target.definition, entity.before)
    {
        Ok(document) => (document, false),
        Err(HelixDbError::VectorComponentMagnitudeExceeded { .. }) => match &target.mode {
            VectorMutationMode::RecordBuildDelta => (None, true),
            VectorMutationMode::MaintainActive(_) => {
                // An already-invalid active physical row must remain
                // untouched until the index is dropped and rebuilt.
                return Ok(());
            }
        },
        Err(error) => return Err(error),
    };
    if !force_build_delta && old_document == new_document {
        return Ok(());
    }
    match &target.mode {
        VectorMutationMode::RecordBuildDelta => {
            let index_entity = IndexEntity {
                kind: entity.entity_kind,
                id: entity.entity_id,
            };
            stage_vector_build_delta(
                transaction,
                scope,
                target,
                index_entity,
                old_document.map(|document| document.partition),
            )
            .await?;
        }
        VectorMutationMode::MaintainActive(handle) => {
            maintain_active(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity.entity_id,
                old_document,
                new_document,
            )
            .await?;
        }
    }
    Ok(())
}

/// Preserves the original partition across repeated coalesced mutations.
async fn stage_vector_build_delta(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    entity: IndexEntity,
    initial_before: Option<TextPartition>,
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
        Some(existing) => {
            let value = crate::index_lifecycle::expect_typed_value(
                decode_build_delta(&existing),
                "vector build-delta key contains another value kind",
            )?;
            if value.index_id != target.index_id
                || value.generation != target.generation
                || value.entity_kind != entity.kind
                || value.entity_id != entity.id
            {
                return Err(corruption("vector build-delta key/value mismatch"));
            }
            match value.state {
                CoalescedBuildDeltaState::Marker | CoalescedBuildDeltaState::VectorBefore(_) => {
                    value.state
                }
                CoalescedBuildDeltaState::SecondaryBefore(_) => {
                    return Err(corruption(
                        "vector build delta contains secondary recovery state",
                    ));
                }
            }
        }
        None => CoalescedBuildDeltaState::VectorBefore(initial_before),
    };
    transaction.put(
        key,
        encode_build_delta(&CoalescedBuildDeltaValue {
            index_id: target.index_id,
            generation: target.generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
            state,
        }),
    )?;
    Ok(())
}

/// Maintains only vector targets selected by the transaction-owned router.
pub(crate) async fn maintain_routed_entity_with_runtime(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &VectorMutationSet,
    routes: &super::mutation_catalog::RoutedMutationTargets<'_>,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    transition: &super::graph_mutation::GraphMutationTransition,
) -> Result<()> {
    let entity = transition.entity().index_entity();
    let before = transition.before().map_or(
        &[][..],
        super::graph_mutation::CanonicalPropertyRow::properties,
    );
    let after = transition.after().map_or(
        &[][..],
        super::graph_mutation::CanonicalPropertyRow::properties,
    );
    let entity = VectorEntityMutation {
        entity_kind: entity.kind,
        entity_id: entity.id,
        before,
        after,
    };
    for ordinal in routes.iter().filter_map(|target| match target {
        super::mutation_catalog::MutationRouteTarget::Vector(ordinal) => Some(ordinal),
        super::mutation_catalog::MutationRouteTarget::Secondary(_)
        | super::mutation_catalog::MutationRouteTarget::TextBuilding(_)
        | super::mutation_catalog::MutationRouteTarget::TextActive(_) => None,
    }) {
        let target = mutations.targets.get(ordinal).ok_or_else(|| {
            corruption("vector mutation route named a target outside its catalog")
        })?;
        maintain_target(transaction, scope, target, runtime, cache_writes, entity).await?;
    }
    Ok(())
}

/// Preserves the isolated per-entity contract as a differential-test oracle.
#[cfg(any(
    test,
    feature = "production-coverage",
    feature = "index-lifecycle-testing"
))]
pub(crate) async fn maintain_entity(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &VectorMutationSet,
    cache_writes: &VectorCacheWriteSet,
    entity: VectorEntityMutation<'_>,
) -> Result<()> {
    let mut runtime = vector::ActiveVectorMutationRuntime::new(
        std::num::NonZeroU64::new(8 * 1024 * 1024)
            .expect("the differential vector-session limit is non-zero"),
    );
    maintain_entity_with_runtime(
        transaction,
        scope,
        mutations,
        &mut runtime,
        cache_writes,
        entity,
    )
    .await?;
    runtime.prepare(transaction).await
}

#[allow(
    clippy::too_many_arguments,
    reason = "active mutation requires the exact transaction, generation, cache, entity, and state transition"
)]
async fn maintain_active(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    handle: &ActiveIndexHandle,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    entity_id: IndexEntityId,
    old_document: Option<VectorIndexedDocument>,
    new_document: Option<VectorIndexedDocument>,
) -> Result<()> {
    match target.definition.metric() {
        VectorDistanceMetric::Cosine => {
            maintain_active_with_distance::<vector::distance::Cosine>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                old_document,
                new_document,
            )
            .await
        }
        VectorDistanceMetric::Euclidean => {
            maintain_active_with_distance::<vector::distance::Euclidean>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                old_document,
                new_document,
            )
            .await
        }
        VectorDistanceMetric::Manhattan => {
            maintain_active_with_distance::<vector::distance::Manhattan>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                old_document,
                new_document,
            )
            .await
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the distance-specialized mutation owns one complete graph state transition"
)]
async fn maintain_active_with_distance<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    handle: &ActiveIndexHandle,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    entity_id: IndexEntityId,
    old_document: Option<VectorIndexedDocument>,
    new_document: Option<VectorIndexedDocument>,
) -> Result<()> {
    match (old_document, new_document) {
        (None, None) => Ok(()),
        (Some(old), None) => {
            remove_active_document::<D>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                &old,
            )
            .await
        }
        (None, Some(new)) => {
            upsert_active_document::<D>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                &new,
            )
            .await
        }
        (Some(old), Some(new)) if old.partition() == new.partition() => {
            upsert_active_document::<D>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                &new,
            )
            .await
        }
        (Some(old), Some(new)) => {
            remove_active_document::<D>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                &old,
            )
            .await?;
            upsert_active_document::<D>(
                transaction,
                scope,
                target,
                handle,
                runtime,
                cache_writes,
                entity_id,
                &new,
            )
            .await
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the removal binds exact lifecycle and transaction identity before physical access"
)]
async fn remove_active_document<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    active: &ActiveIndexHandle,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    entity_id: IndexEntityId,
    document: &VectorIndexedDocument,
) -> Result<()> {
    let (physical_index_id, created) = resolve_active_physical(
        transaction,
        scope,
        target,
        active,
        document.partition(),
        false,
    )
    .await?;
    if created {
        return Err(corruption(
            "vector remove path unexpectedly allocated a tenant partition",
        ));
    }
    let generation =
        vector::ValidatedVectorGenerationHandle::try_from_active::<D>(active, physical_index_id)
            .map_err(|error| corruption(error.to_string()))?;
    let empty = runtime
        .delete(transaction, &generation, cache_writes, entity_id.get())
        .await?;
    if matches!(document.partition(), TextPartition::TenantValue(_)) && empty {
        runtime.drain_generation(transaction, &generation).await?;
    }
    let index = crate::search::vector::VectorIndex::<D>::from_generation(&generation);
    reclaim_empty_tenant_partition(
        transaction,
        scope,
        target,
        &generation,
        cache_writes,
        document.partition(),
        &index,
    )
    .await
}

/// Reclaims one physically empty tenant namespace in the graph transaction.
///
/// The V2 count is an exact fast-path signal for newly allocated generations;
/// every physical lane is still probed before ownership is removed. Mapping,
/// metadata, and the optional legacy transaction guard disappear atomically.
/// Shared cache retirement is recorded only as a post-commit effect.
async fn reclaim_empty_tenant_partition<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    generation: &vector::ValidatedVectorGenerationHandle,
    cache_writes: &VectorCacheWriteSet,
    partition: &TextPartition,
    index: &crate::search::vector::VectorIndex<D>,
) -> Result<()> {
    let TextPartition::TenantValue(_) = partition else {
        return Ok(());
    };
    let metadata = index
        .get_metadata(transaction)
        .await?
        .ok_or_else(|| corruption("tenant vector partition lost metadata during deletion"))?;
    if metadata.count != 0 {
        return Ok(());
    }
    if metadata.validated_state()? != vector::VectorIndexState::Empty {
        return Err(HelixDbError::InvariantViolation(
            "zero-count tenant vector partition retains populated metadata state".to_string(),
        ));
    }
    let expected =
        VectorIndexConfig::from_v2_definition(&target.definition, generation.physical_name());
    if !metadata.config.has_same_physical_contract(&expected) {
        return Err(corruption(
            "empty tenant vector metadata conflicts with its active generation",
        ));
    }

    let physical_index_id = generation.physical_index_id();
    let metadata_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    let guard_key = DataKey::Data {
        scope,
        kind: DataKeyKind::Vector(VectorKey::TxnGuard(LegacyVectorTxnGuardKey::new(
            physical_index_id,
        ))),
    }
    .to_bytes();
    for lane in VectorStorageLane::ALL {
        let prefix = DataKey::data_prefix(scope, lane.prefix_key(physical_index_id).to_bytes());
        let mut rows = transaction.scan_prefix(prefix, ..).await?;
        while let Some(row) = rows.next().await? {
            if lane == VectorStorageLane::Core && row.key == metadata_key {
                continue;
            }
            if lane == VectorStorageLane::Core && row.key == guard_key {
                decode_active_txn_guard(&row.value).map_err(|error| {
                    HelixDbError::InvariantViolation(format!(
                        "empty tenant vector partition has a malformed transaction guard: {error}"
                    ))
                })?;
                continue;
            }
            return Err(HelixDbError::InvariantViolation(format!(
                "zero-count tenant vector partition {} retains a {:?} row",
                physical_index_id, lane
            )));
        }
    }

    let tenant = VectorTenantPartition::try_from_partition(partition.clone())
        .map_err(|error| corruption(error.to_string()))?;
    repository::stage_delete_vector_partition_mapping(
        transaction,
        scope,
        target.index_id,
        target.generation,
        VectorPhysicalLayout::Partitioned,
        &tenant,
        VectorPhysicalIndexId::new(physical_index_id)?,
    )
    .await?;
    transaction.delete(metadata_key)?;
    transaction.delete(guard_key)?;
    cache_writes.retire_after_commit(generation);
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the upsert binds exact lifecycle and transaction identity before physical access"
)]
async fn upsert_active_document<D: Distance>(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    active: &ActiveIndexHandle,
    runtime: &mut vector::ActiveVectorMutationRuntime,
    cache_writes: &VectorCacheWriteSet,
    entity_id: IndexEntityId,
    document: &VectorIndexedDocument,
) -> Result<()> {
    let (physical_index_id, created) = resolve_active_physical(
        transaction,
        scope,
        target,
        active,
        document.partition(),
        true,
    )
    .await?;
    let generation =
        vector::ValidatedVectorGenerationHandle::try_from_active::<D>(active, physical_index_id)
            .map_err(|error| corruption(error.to_string()))?;
    runtime
        .upsert(
            transaction,
            &generation,
            cache_writes,
            entity_id.get(),
            document.vector(),
            created,
        )
        .await
}

async fn resolve_active_physical(
    transaction: &DbTransaction,
    scope: DataScope,
    target: &VectorMutationTarget,
    active: &ActiveIndexHandle,
    partition: &TextPartition,
    create_missing: bool,
) -> Result<(VectorPhysicalIndexId, bool)> {
    let ActiveIndexHandle::Vector { layout, .. } = active else {
        return Err(corruption(
            "vector mutation target retained another active family",
        ));
    };
    match (layout, partition) {
        (
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
            TextPartition::Unpartitioned,
        ) => Ok((*physical_index_id, false)),
        (VectorPhysicalLayout::Partitioned, TextPartition::TenantValue(_)) => {
            let tenant = VectorTenantPartition::try_from_partition(partition.clone())
                .map_err(|error| corruption(error.to_string()))?;
            let existing = repository::load_vector_partition_mapping(
                transaction,
                scope,
                target.index_id,
                target.generation,
                *layout,
                &tenant,
            )
            .await?;
            if let Some(physical_index_id) = existing {
                return Ok((physical_index_id, false));
            }
            if !create_missing {
                return Err(corruption(
                    "active vector document has no tenant partition mapping",
                ));
            }
            let physical_index_id = repository::stage_vector_partition_mapping(
                transaction,
                scope,
                target.index_id,
                target.generation,
                *layout,
                &tenant,
            )
            .await?;
            Ok((physical_index_id, true))
        }
        (VectorPhysicalLayout::Unpartitioned { .. }, TextPartition::TenantValue(_))
        | (VectorPhysicalLayout::Partitioned, TextPartition::Unpartitioned) => Err(corruption(
            "canonical vector document partition disagrees with physical layout",
        )),
    }
}

/// Projects complete graph properties into one canonical V2 vector document.
pub(crate) fn vector_document(
    definition: &ValidatedVectorIndexDefinition,
    properties: &[Property],
) -> Result<Option<VectorIndexedDocument>> {
    let Some(property) = properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
    else {
        return Ok(None);
    };
    let Some(partition) = vector_partition(definition, properties)? else {
        return Ok(None);
    };
    let vector = property_vector_to_f32(&property.value)?;
    let dimension = VectorDimension::try_new(definition.dimension() as usize)
        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    ValidatedMetricVector::try_from_slice(&vector, definition.metric(), dimension)
        .map_err(HelixDbError::from)?;
    Ok(Some(VectorIndexedDocument { partition, vector }))
}

fn vector_partition(
    definition: &ValidatedVectorIndexDefinition,
    properties: &[Property],
) -> Result<Option<TextPartition>> {
    let matches_label = properties.iter().any(|property| {
        property.name == "$label" && property.value.as_str() == Some(definition.label().as_str())
    });
    if !matches_label {
        return Ok(None);
    }
    let partition = match definition.tenant_property() {
        None => TextPartition::Unpartitioned,
        Some(tenant_property) => {
            let Some(value) = properties
                .iter()
                .find(|property| property.name == tenant_property.as_str())
                .map(|property| &property.value)
                .and_then(search::text::normalize_tenant_value)
            else {
                return Ok(None);
            };
            TextPartition::try_tenant_value(encode_index_partition_value(value))
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?
        }
    };
    Ok(Some(partition))
}

fn property_vector_to_f32(value: &PropertyValue) -> Result<Vec<f32>> {
    match value {
        PropertyValue::F32Array(values) => Ok(values.clone()),
        PropertyValue::F64Array(values) => Ok(values.iter().map(|value| *value as f32).collect()),
        PropertyValue::I64Array(values) => Ok(values.iter().map(|value| *value as f32).collect()),
        PropertyValue::Array(values) => values.iter().map(numeric_value_to_f32).collect(),
        other @ (PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::StringArray(_)
        | PropertyValue::Object(_)) => Err(HelixDbError::Query(format!(
            "vector index property must be a numeric array, got {other:?}"
        ))),
    }
}

fn numeric_value_to_f32(value: &PropertyValue) -> Result<f32> {
    match value {
        PropertyValue::I64(value) => Ok(*value as f32),
        PropertyValue::F64(value) => Ok(*value as f32),
        PropertyValue::F32(value) => Ok(*value as f32),
        other @ (PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::StringArray(_)
        | PropertyValue::Array(_)
        | PropertyValue::Object(_)) => Err(HelixDbError::Query(format!(
            "vector index array item must be numeric, got {other:?}"
        ))),
    }
}

fn scoped_index_key(scope: DataScope, logical: ScopedKey) -> Bytes {
    IndexKey::Data {
        scope,
        kind: logical,
    }
    .to_bytes()
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

/// Proves a real tenant-indexed vector still requires its physical mapping.
#[cfg(all(feature = "production-coverage", not(test)))]
pub(crate) async fn run_missing_partition_mapping_delete_contract() {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    let db = Db::builder(
        "vector-production-missing-tenant-mapping",
        Arc::new(InMemory::new()),
    )
    .build()
    .await
    .expect("production contract database opens");
    crate::migrations::startup::bootstrap_writer(&db)
        .await
        .expect("production contract database bootstraps");

    let runtime = crate::config::VectorIndexDefinition::new_node(
        "Document",
        "embedding",
        3,
        VectorDistanceMetric::Euclidean,
    )
    .expect("production contract vector definition")
    .with_tenant_property("account_id")
    .expect("production contract tenant definition");
    let definition = ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
        .expect("production contract definition validates");
    let record = super::IndexRecordV2::building(
        IndexId::new(31).expect("production contract index ID is nonzero"),
        ValidatedDynamicIndexDefinition::Vector(definition.clone()),
        super::IndexRevision::initial(),
        super::PhysicalGeneration::Vector {
            generation: IndexGenerationId::new(7)
                .expect("production contract generation ID is nonzero"),
            layout: VectorPhysicalLayout::Partitioned,
            descriptor: super::VectorGenerationDescriptor::for_definition(&definition),
        },
        super::IndexOperationId::new_v4(),
    )
    .expect("production contract building record validates")
    .transition(super::IndexStateTransition::Activate)
    .expect("production contract record activates");
    let handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
        .expect("production contract active handle validates");
    let mutations = VectorMutationSet {
        targets: vec![VectorMutationTarget {
            index_id: record.index_id(),
            generation: record.state().generation(),
            definition,
            mode: VectorMutationMode::MaintainActive(handle),
        }],
    };
    let properties = vec![
        Property::new("$label", PropertyValue::String("Document".to_string())),
        Property::new("account_id", PropertyValue::I64(7)),
        Property::new("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
    ];
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("production contract transaction opens");

    assert!(matches!(
        maintain_entity(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 9, &properties, &[]),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    drop(transaction);
    db.close()
        .await
        .expect("production contract database closes");
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::encoding::v2::keys::{GlobalKey, IndexRecordKey, VectorPartitionMappingKey};
    use crate::encoding::v2::values::{encode_index_record, encode_metadata_value};
    use crate::index_lifecycle::{
        IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition, IndexV2MetadataValue,
        PhysicalGeneration, VectorGenerationDescriptor, VectorPhysicalIdWatermark,
    };
    use crate::search::vector::VectorIndex;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("in-memory vector lifecycle database opens");
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("empty writer bootstraps V2 metadata");
        db
    }

    fn property(name: &str, value: PropertyValue) -> Property {
        Property::new(name, value)
    }

    fn validated_definition(
        tenant_property: Option<&str>,
        metric: VectorDistanceMetric,
    ) -> ValidatedVectorIndexDefinition {
        let runtime =
            crate::config::VectorIndexDefinition::new_node("Document", "embedding", 3, metric)
                .expect("vector definition");
        let runtime = match tenant_property {
            Some(tenant_property) => runtime
                .with_tenant_property(tenant_property)
                .expect("tenant vector definition"),
            None => runtime,
        };
        ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
            .expect("validated V2 vector definition")
    }

    fn active_target(
        definition: ValidatedVectorIndexDefinition,
        layout: VectorPhysicalLayout,
    ) -> (VectorMutationTarget, ActiveIndexHandle) {
        let operation_id = IndexOperationId::new_v4();
        let dynamic = ValidatedDynamicIndexDefinition::Vector(definition.clone());
        let record = IndexRecordV2::building(
            IndexId::new(31).unwrap(),
            dynamic,
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::new(7).unwrap(),
                layout,
                descriptor: VectorGenerationDescriptor::for_definition(&definition),
            },
            operation_id,
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let handle = ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
            .expect("active vector projects a handle");
        (
            VectorMutationTarget {
                index_id: record.index_id(),
                generation: record.state().generation(),
                definition,
                mode: VectorMutationMode::MaintainActive(handle.clone()),
            },
            handle,
        )
    }

    #[tokio::test]
    async fn repeated_build_deltas_preserve_the_first_vector_partition() {
        let db = test_db("vector-build-delta-first-partition").await;
        let scope = DataScope::LegacyUnscoped;
        let target = VectorMutationTarget {
            index_id: IndexId::new(31).unwrap(),
            generation: IndexGenerationId::new(7).unwrap(),
            definition: validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean),
            mode: VectorMutationMode::RecordBuildDelta,
        };
        let first_partition =
            TextPartition::try_tenant_value(Bytes::from_static(b"first")).unwrap();
        let second_partition =
            TextPartition::try_tenant_value(Bytes::from_static(b"second")).unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        for (entity_id, first, second) in [
            (
                IndexEntityId::new(7),
                Some(first_partition.clone()),
                Some(second_partition.clone()),
            ),
            (IndexEntityId::new(8), None, Some(second_partition.clone())),
        ] {
            let entity = IndexEntity {
                kind: IndexElementKind::Node,
                id: entity_id,
            };
            stage_vector_build_delta(&transaction, scope, &target, entity, first.clone())
                .await
                .unwrap();
            stage_vector_build_delta(&transaction, scope, &target, entity, second)
                .await
                .unwrap();

            let key = scoped_index_key(
                scope,
                ScopedKey::BuildDelta(IndexEntityStateKey {
                    index_id: target.index_id,
                    generation: target.generation,
                    entity,
                }),
            );
            let delta = decode_build_delta(&transaction.get(&key).await.unwrap().unwrap()).unwrap();
            assert_eq!(delta.state, CoalescedBuildDeltaState::VectorBefore(first));
        }

        transaction.rollback();
        db.close().await.unwrap();
    }

    /// Exercises one unpartitioned active generation through insert and removal.
    async fn exercise_active_unpartitioned_metric<D: Distance>(
        db_name: &str,
        metric: VectorDistanceMetric,
        physical_index_id: VectorPhysicalIndexId,
    ) {
        let db = test_db(db_name).await;
        let (target, active) = active_target(
            validated_definition(None, metric),
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
        );
        let generation = vector::ValidatedVectorGenerationHandle::try_from_active::<D>(
            &active,
            physical_index_id,
        )
        .unwrap();
        let index = VectorIndex::<D>::from_generation(&generation);
        let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(
                    &target.definition,
                    generation.physical_name(),
                ),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();

        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let cache_writes = VectorCacheWriteSet::default();
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &insert,
            DataScope::LegacyUnscoped,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 9, &[], &properties),
        )
        .await
        .unwrap();
        insert.commit().await.unwrap();
        assert!(index.get_item(&db, 9).await.unwrap().is_some());

        let delete = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &delete,
            DataScope::LegacyUnscoped,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 9, &properties, &[]),
        )
        .await
        .unwrap();
        delete.commit().await.unwrap();
        assert!(index.get_item(&db, 9).await.unwrap().is_none());
        db.close().await.unwrap();
    }

    #[test]
    fn semantic_document_validates_partition_dimension_components_and_cosine_zero() {
        let tenant = validated_definition(Some("account_id"), VectorDistanceMetric::Cosine);
        let document = vector_document(
            &tenant,
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property("account_id", PropertyValue::I64(7)),
                property("embedding", PropertyValue::F64Array(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap()
        .expect("matching document");
        assert!(matches!(
            document.partition(),
            TextPartition::TenantValue(_)
        ));
        assert_eq!(document.vector(), &[1.0, 2.0, 3.0]);

        let missing_tenant = vector_document(
            &tenant,
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        assert_eq!(missing_tenant, None);

        let zero = vector_document(
            &validated_definition(None, VectorDistanceMetric::Cosine),
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property("embedding", PropertyValue::F32Array(vec![0.0, -0.0, 0.0])),
            ],
        );
        assert!(matches!(zero, Err(HelixDbError::ZeroNormCosineVector)));

        let overflow = vector_document(
            &validated_definition(None, VectorDistanceMetric::Euclidean),
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property(
                    "embedding",
                    PropertyValue::F64Array(vec![f64::MAX, 2.0, 3.0]),
                ),
            ],
        );
        assert!(matches!(
            overflow,
            Err(HelixDbError::InvalidVectorComponent { index: 0 })
        ));

        let unpartitioned = validated_definition(None, VectorDistanceMetric::Euclidean);
        let i64_document = vector_document(
            &unpartitioned,
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property("embedding", PropertyValue::I64Array(vec![1, 2, 3])),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(i64_document.vector(), &[1.0, 2.0, 3.0]);

        let mixed_document = vector_document(
            &unpartitioned,
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property(
                    "embedding",
                    PropertyValue::Array(vec![
                        PropertyValue::I64(1),
                        PropertyValue::F64(2.0),
                        PropertyValue::F32(3.0),
                    ]),
                ),
            ],
        )
        .unwrap()
        .unwrap();
        assert_eq!(mixed_document.vector(), &[1.0, 2.0, 3.0]);

        for value in [
            PropertyValue::String("not a vector".to_string()),
            PropertyValue::Array(vec![PropertyValue::Bool(true)]),
        ] {
            assert!(matches!(
                vector_document(
                    &unpartitioned,
                    &[
                        property("$label", PropertyValue::String("Document".to_string())),
                        property("embedding", value),
                    ],
                ),
                Err(HelixDbError::Query(_))
            ));
        }

        let oversized_tenant = vector_document(
            &tenant,
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property(
                    "account_id",
                    PropertyValue::Bytes(vec![0x7a; 16 * 1024 * 1024 + 1]),
                ),
                property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
            ],
        );
        assert!(matches!(
            oversized_tenant,
            Err(HelixDbError::InvariantViolation(_))
        ));

        let missing_vector_with_oversized_tenant = vector_document(
            &tenant,
            &[
                property("$label", PropertyValue::String("Document".to_string())),
                property(
                    "account_id",
                    PropertyValue::Bytes(vec![0x7a; 16 * 1024 * 1024 + 1]),
                ),
            ],
        );
        assert_eq!(missing_vector_with_oversized_tenant.unwrap(), None);

        let wrong_label_with_invalid_vector = vector_document(
            &tenant,
            &[
                property("$label", PropertyValue::String("Other".to_string())),
                property("account_id", PropertyValue::String("acme".to_string())),
                property("embedding", PropertyValue::String("invalid".to_string())),
            ],
        );
        assert_eq!(wrong_label_with_invalid_vector.unwrap(), None);
    }

    /// Covers active insert/remove dispatch for every supported distance metric.
    #[tokio::test]
    async fn active_unpartitioned_mutations_cover_every_distance_metric() {
        exercise_active_unpartitioned_metric::<vector::distance::Cosine>(
            "vector-active-unpartitioned-cosine",
            VectorDistanceMetric::Cosine,
            VectorPhysicalIndexId::new(41).unwrap(),
        )
        .await;
        exercise_active_unpartitioned_metric::<vector::distance::Euclidean>(
            "vector-active-unpartitioned-euclidean",
            VectorDistanceMetric::Euclidean,
            VectorPhysicalIndexId::new(42).unwrap(),
        )
        .await;
        exercise_active_unpartitioned_metric::<vector::distance::Manhattan>(
            "vector-active-unpartitioned-manhattan",
            VectorDistanceMetric::Manhattan,
            VectorPhysicalIndexId::new(43).unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn active_runtime_batches_repeated_entities_in_one_transaction() {
        let db = test_db("vector-active-runtime-batched-repeated-entities").await;
        let physical_index_id = VectorPhysicalIndexId::new(44).unwrap();
        let (target, active) = active_target(
            validated_definition(None, VectorDistanceMetric::Euclidean),
            VectorPhysicalLayout::Unpartitioned { physical_index_id },
        );
        let generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, physical_index_id)
        .unwrap();
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(
                    &target.definition,
                    generation.physical_name(),
                ),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();

        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let cache_writes = VectorCacheWriteSet::default();
        let mut runtime = vector::ActiveVectorMutationRuntime::new(
            std::num::NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        );
        let first = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("embedding", PropertyValue::F32Array(vec![1.0, 0.0, 0.0])),
        ];
        let second = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("embedding", PropertyValue::F32Array(vec![0.0, 1.0, 0.0])),
        ];
        let replacement = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("embedding", PropertyValue::F32Array(vec![0.0, 0.0, 1.0])),
        ];
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity_with_runtime(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &mut runtime,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 1, &[], &first),
        )
        .await
        .unwrap();
        maintain_entity_with_runtime(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &mut runtime,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 2, &[], &second),
        )
        .await
        .unwrap();
        maintain_entity_with_runtime(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &mut runtime,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 1, &first, &replacement),
        )
        .await
        .unwrap();
        runtime.flush(&transaction).await.unwrap();
        assert_eq!(
            index
                .get_item(&transaction, 1)
                .await
                .unwrap()
                .unwrap()
                .vector
                .to_vec(),
            vec![0.0, 0.0, 1.0]
        );
        runtime.prepare(&transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(index.get_metadata(&db).await.unwrap().unwrap().count, 2);
        assert!(index.get_item(&db, 2).await.unwrap().is_some());
        db.close().await.unwrap();
    }

    /// Treats deletion of a label-matching tenant row without a vector as no work.
    #[tokio::test]
    async fn active_missing_property_delete_stages_no_partition_work() {
        let db = test_db("vector-active-missing-property-delete").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, _) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
        ];
        let partition = vector_partition(&target.definition, &properties)
            .unwrap()
            .expect("label and tenant project a partition");
        let partition = VectorTenantPartition::try_from_partition(partition).unwrap();
        let index_id = target.index_id;
        let generation = target.generation;
        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        maintain_entity(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 9, &properties, &[]),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        assert!(repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            index_id,
            generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .is_none());
        db.close().await.unwrap();
    }

    /// Avoids a build delta when both graph snapshots lack the indexed vector.
    #[tokio::test]
    async fn building_missing_property_delete_stages_no_delta() {
        let db = test_db("vector-building-missing-property-delete").await;
        let index_id = IndexId::new(32).unwrap();
        let generation = IndexGenerationId::new(8).unwrap();
        let target = VectorMutationTarget {
            index_id,
            generation,
            definition: validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean),
            mode: VectorMutationMode::RecordBuildDelta,
        };
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
        ];
        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let delta_key = scoped_index_key(
            DataScope::LegacyUnscoped,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity: IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(9),
                },
            }),
        );
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        maintain_entity(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 9, &properties, &[]),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        assert!(db.get(&delta_key).await.unwrap().is_none());
        db.close().await.unwrap();
    }

    /// Rejects removal from a tenant partition whose physical mapping is absent.
    #[tokio::test]
    async fn active_tenant_removal_requires_an_existing_partition_mapping() {
        let db = test_db("vector-active-missing-tenant-mapping").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, _) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let cache_writes = VectorCacheWriteSet::default();
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        assert!(matches!(
            maintain_entity(
                &transaction,
                DataScope::LegacyUnscoped,
                &mutations,
                &cache_writes,
                VectorEntityMutation::new(IndexElementKind::Node, 9, &properties, &[]),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.close().await.unwrap();
    }

    /// Short-circuits an unchanged semantic document before any physical access.
    #[tokio::test]
    async fn unchanged_active_document_stages_no_vector_work() {
        let db = test_db("vector-active-unchanged-document").await;
        let (target, _) = active_target(
            validated_definition(None, VectorDistanceMetric::Euclidean),
            VectorPhysicalLayout::Unpartitioned {
                physical_index_id: VectorPhysicalIndexId::new(51).unwrap(),
            },
        );
        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        maintain_entity(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 9, &properties, &properties),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
        db.close().await.unwrap();
    }

    /// Rejects a canonical vector row whose key repeats a different identity.
    #[tokio::test]
    async fn mutation_set_rejects_catalog_key_value_identity_disagreement() {
        let db = test_db("vector-mutation-catalog-identity-mismatch").await;
        let definition = validated_definition(None, VectorDistanceMetric::Euclidean);
        let record = IndexRecordV2::building(
            IndexId::new(61).unwrap(),
            ValidatedDynamicIndexDefinition::Vector(definition.clone()),
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::new(7).unwrap(),
                layout: VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::new(62).unwrap(),
                },
                descriptor: VectorGenerationDescriptor::for_definition(&definition),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap();
        let other = validated_definition(None, VectorDistanceMetric::Manhattan);
        let other = crate::config::VectorIndexDefinition::new_node(
            other.label().as_str(),
            "other_embedding",
            other.dimension() as usize,
            other.metric(),
        )
        .unwrap();
        let other = ValidatedVectorIndexDefinition::try_from_runtime(&other).unwrap();
        let key = IndexKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: ScopedKey::IndexRecord(IndexRecordKey {
                identity: other.identity(),
            }),
        }
        .to_bytes();
        db.put(key, encode_index_record(&record)).await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        assert!(matches!(
            load_mutation_set(&transaction, DataScope::LegacyUnscoped).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.close().await.unwrap();
    }

    /// Propagates exhausted partition allocation through active upsert resolution.
    #[tokio::test]
    async fn active_tenant_upsert_propagates_physical_id_exhaustion() {
        let db = test_db("vector-active-tenant-id-exhaustion").await;
        db.put(
            IndexKey::Global {
                kind: GlobalKey::VectorPhysicalIdWatermark,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::new(u64::MAX).unwrap(),
                },
            )),
        )
        .await
        .unwrap();
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, _) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        assert!(matches!(
            maintain_entity(
                &transaction,
                DataScope::LegacyUnscoped,
                &mutations,
                &VectorCacheWriteSet::default(),
                VectorEntityMutation::new(IndexElementKind::Node, 9, &[], &properties),
            )
            .await,
            Err(HelixDbError::IdentifierExhausted(
                "vector physical index ID"
            ))
        ));
        drop(transaction);
        db.close().await.unwrap();
    }

    /// Rejects a partition mapping row containing a different V2 value family.
    #[tokio::test]
    async fn active_tenant_upsert_rejects_mistyped_partition_mapping() {
        let db = test_db("vector-active-mistyped-tenant-mapping").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, _) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let document = vector_document(&target.definition, &properties)
            .unwrap()
            .unwrap();
        let partition =
            VectorTenantPartition::try_from_partition(document.partition().clone()).unwrap();
        db.put(
            scoped_index_key(
                DataScope::LegacyUnscoped,
                ScopedKey::VectorPartitionMapping(VectorPartitionMappingKey {
                    index_id: target.index_id,
                    generation: target.generation,
                    partition: partition.fingerprint(),
                }),
            ),
            encode_build_delta(&CoalescedBuildDeltaValue {
                index_id: target.index_id,
                generation: target.generation,
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(9),
                state: CoalescedBuildDeltaState::Marker,
            }),
        )
        .await
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mutations = VectorMutationSet {
            targets: vec![target],
        };

        assert!(matches!(
            maintain_entity(
                &transaction,
                DataScope::LegacyUnscoped,
                &mutations,
                &VectorCacheWriteSet::default(),
                VectorEntityMutation::new(IndexElementKind::Node, 9, &[], &properties),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.close().await.unwrap();
    }

    /// Propagates a physical-index collision after a new mapping is staged.
    #[tokio::test]
    async fn active_tenant_upsert_rejects_preexisting_allocated_physical_index() {
        let db = test_db("vector-active-tenant-physical-collision").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, active) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let physical_index_id = repository::peek_vector_physical_id(&db).await.unwrap();
        let generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, physical_index_id)
        .unwrap();
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(
                    &target.definition,
                    generation.physical_name(),
                ),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mutations = VectorMutationSet {
            targets: vec![target],
        };

        assert!(matches!(
            maintain_entity(
                &transaction,
                DataScope::LegacyUnscoped,
                &mutations,
                &VectorCacheWriteSet::default(),
                VectorEntityMutation::new(IndexElementKind::Node, 9, &[], &properties),
            )
            .await,
            Err(HelixDbError::IndexAlreadyExists(_))
        ));
        drop(transaction);
        db.close().await.unwrap();
    }

    /// Propagates a missing physical HNSW generation during a tenant move.
    #[tokio::test]
    async fn active_tenant_move_rejects_mapping_without_physical_index() {
        let db = test_db("vector-active-tenant-missing-physical-index").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, _) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let properties = |tenant: i64| {
            vec![
                property("$label", PropertyValue::String("Document".to_string())),
                property("account_id", PropertyValue::I64(tenant)),
                property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
            ]
        };
        let before = properties(7);
        let after = properties(8);
        let document = vector_document(&target.definition, &before)
            .unwrap()
            .unwrap();
        let partition =
            VectorTenantPartition::try_from_partition(document.partition().clone()).unwrap();
        let mapping = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        repository::stage_vector_partition_mapping(
            &mapping,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap();
        mapping.commit().await.unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mutations = VectorMutationSet {
            targets: vec![target],
        };

        assert!(maintain_entity(
            &transaction,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 9, &before, &after),
        )
        .await
        .is_err());
        drop(transaction);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn active_tenant_move_allocates_mapping_with_first_work_and_removes_old_row() {
        let db = test_db("vector-active-tenant-move").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, active) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let mutations = VectorMutationSet {
            targets: vec![target.clone()],
        };
        let cache_writes = VectorCacheWriteSet::default();
        let properties = |tenant: i64, vector: Vec<f32>| {
            vec![
                property("$label", PropertyValue::String("Document".to_string())),
                property("account_id", PropertyValue::I64(tenant)),
                property("embedding", PropertyValue::F32Array(vector)),
            ]
        };
        let first = properties(7, vec![1.0, 2.0, 3.0]);
        let second = properties(8, vec![3.0, 2.0, 1.0]);

        let insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &insert,
            DataScope::LegacyUnscoped,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 19, &[], &first),
        )
        .await
        .unwrap();
        insert.commit().await.unwrap();

        let first_document = vector_document(&target.definition, &first)
            .unwrap()
            .unwrap();
        let first_partition =
            VectorTenantPartition::try_from_partition(first_document.partition().clone()).unwrap();
        let first_physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &first_partition,
        )
        .await
        .unwrap()
        .expect("first mutation publishes mapping");
        let first_generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, first_physical)
        .unwrap();
        let first_index =
            VectorIndex::<vector::distance::Euclidean>::from_generation(&first_generation);
        assert!(first_index.get_item(&db, 19).await.unwrap().is_some());
        let retained_snapshot = db.snapshot().await.unwrap();

        let update = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &update,
            DataScope::LegacyUnscoped,
            &mutations,
            &cache_writes,
            VectorEntityMutation::new(IndexElementKind::Node, 19, &first, &second),
        )
        .await
        .unwrap();
        update.commit().await.unwrap();
        assert!(first_index.get_item(&db, 19).await.unwrap().is_none());
        assert!(
            first_index
                .get_item(retained_snapshot.as_ref(), 19)
                .await
                .unwrap()
                .is_some(),
            "a reader that predates reclamation retains its SlateDB snapshot"
        );
        assert!(
            repository::load_vector_partition_mapping(
                &db,
                DataScope::LegacyUnscoped,
                target.index_id,
                target.generation,
                VectorPhysicalLayout::Partitioned,
                &first_partition,
            )
            .await
            .unwrap()
            .is_none(),
            "the empty source tenant no longer owns a physical mapping"
        );
        assert!(first_index.get_metadata(&db).await.unwrap().is_none());

        let second_document = vector_document(&target.definition, &second)
            .unwrap()
            .unwrap();
        let second_partition =
            VectorTenantPartition::try_from_partition(second_document.partition().clone()).unwrap();
        let second_physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &second_partition,
        )
        .await
        .unwrap()
        .expect("tenant move publishes destination mapping");
        assert_ne!(first_physical, second_physical);
        let second_generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, second_physical)
        .unwrap();
        let second_index =
            VectorIndex::<vector::distance::Euclidean>::from_generation(&second_generation);
        assert!(second_index.get_item(&db, 19).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn tenant_partition_reclaims_only_after_last_delete_and_reinsert_uses_fresh_id() {
        let db = test_db("vector-active-tenant-last-delete").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, active) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let mutations = VectorMutationSet {
            targets: vec![target.clone()],
        };
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let document = vector_document(&target.definition, &properties)
            .unwrap()
            .unwrap();
        let partition =
            VectorTenantPartition::try_from_partition(document.partition().clone()).unwrap();

        let insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let insert_cache_writes = VectorCacheWriteSet::default();
        for entity_id in [41, 42] {
            maintain_entity(
                &insert,
                DataScope::LegacyUnscoped,
                &mutations,
                &insert_cache_writes,
                VectorEntityMutation::new(IndexElementKind::Node, entity_id, &[], &properties),
            )
            .await
            .unwrap();
        }
        insert.commit().await.unwrap();
        let first_physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .unwrap();
        let first_generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, first_physical)
        .unwrap();
        let first_index =
            VectorIndex::<vector::distance::Euclidean>::from_generation(&first_generation);

        let delete_non_last = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &delete_non_last,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 41, &properties, &[]),
        )
        .await
        .unwrap();
        delete_non_last.commit().await.unwrap();
        assert_eq!(
            first_index.get_metadata(&db).await.unwrap().unwrap().count,
            1
        );
        assert_eq!(
            repository::load_vector_partition_mapping(
                &db,
                DataScope::LegacyUnscoped,
                target.index_id,
                target.generation,
                VectorPhysicalLayout::Partitioned,
                &partition,
            )
            .await
            .unwrap(),
            Some(first_physical)
        );
        let retained_snapshot = db.snapshot().await.unwrap();

        let delete_last = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &delete_last,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 42, &properties, &[]),
        )
        .await
        .unwrap();
        delete_last.commit().await.unwrap();
        assert!(repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .is_none());
        assert!(first_index.get_metadata(&db).await.unwrap().is_none());
        assert!(first_index
            .get_item(retained_snapshot.as_ref(), 42)
            .await
            .unwrap()
            .is_some());
        for lane in VectorStorageLane::ALL {
            let prefix = DataKey::data_prefix(
                DataScope::LegacyUnscoped,
                lane.prefix_key(first_physical.get()).to_bytes(),
            );
            let mut rows = db.scan_prefix(prefix, ..).await.unwrap();
            assert!(rows.next().await.unwrap().is_none(), "residue in {lane:?}");
        }

        let reinsert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &reinsert,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 43, &[], &properties),
        )
        .await
        .unwrap();
        reinsert.commit().await.unwrap();
        let second_physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(first_physical, second_physical);
        let second_generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, second_physical)
        .unwrap();
        let second_index =
            VectorIndex::<vector::distance::Euclidean>::from_generation(&second_generation);
        assert!(second_index.get_item(&db, 43).await.unwrap().is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_partition_reclamation_fails_closed_on_physical_residue() {
        let db = test_db("vector-active-tenant-reclamation-residue").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, active) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let mutations = VectorMutationSet {
            targets: vec![target.clone()],
        };
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let document = vector_document(&target.definition, &properties)
            .unwrap()
            .unwrap();
        let partition =
            VectorTenantPartition::try_from_partition(document.partition().clone()).unwrap();
        let insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &insert,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 51, &[], &properties),
        )
        .await
        .unwrap();
        insert.commit().await.unwrap();
        let physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .unwrap();
        let generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, physical)
        .unwrap();
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        let residue_key = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Vector(VectorKey::SimHash(
                crate::encoding::v2::keys::indexes::vector::VectorSimHashKey::new(
                    physical.get(),
                    999,
                ),
            )),
        }
        .to_bytes();
        db.put(
            residue_key,
            crate::encoding::v2::values::indexes::vector::simhash::encode_simhash(17),
        )
        .await
        .unwrap();

        let delete = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let error = maintain_entity(
            &delete,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 51, &properties, &[]),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, HelixDbError::InvariantViolation(_)));
        drop(delete);
        assert_eq!(
            repository::load_vector_partition_mapping(
                &db,
                DataScope::LegacyUnscoped,
                target.index_id,
                target.generation,
                VectorPhysicalLayout::Partitioned,
                &partition,
            )
            .await
            .unwrap(),
            Some(physical)
        );
        assert!(index.get_item(&db, 51).await.unwrap().is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn last_delete_racing_insert_conflicts_then_retries_on_fresh_partition() {
        let db = test_db("vector-active-tenant-reclamation-race").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, active) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let mutations = VectorMutationSet {
            targets: vec![target.clone()],
        };
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let document = vector_document(&target.definition, &properties)
            .unwrap()
            .unwrap();
        let partition =
            VectorTenantPartition::try_from_partition(document.partition().clone()).unwrap();
        let seed = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &seed,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 61, &[], &properties),
        )
        .await
        .unwrap();
        seed.commit().await.unwrap();
        let old_physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .unwrap();

        let delete = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &delete,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 61, &properties, &[]),
        )
        .await
        .unwrap();
        let racing_insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &racing_insert,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 62, &[], &properties),
        )
        .await
        .unwrap();

        delete.commit().await.unwrap();
        let conflict = racing_insert.commit().await.unwrap_err();
        assert_eq!(conflict.kind(), slatedb::ErrorKind::Transaction);

        let retry = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entity(
            &retry,
            DataScope::LegacyUnscoped,
            &mutations,
            &VectorCacheWriteSet::default(),
            VectorEntityMutation::new(IndexElementKind::Node, 62, &[], &properties),
        )
        .await
        .unwrap();
        retry.commit().await.unwrap();
        let fresh_physical = repository::load_vector_partition_mapping(
            &db,
            DataScope::LegacyUnscoped,
            target.index_id,
            target.generation,
            VectorPhysicalLayout::Partitioned,
            &partition,
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(old_physical, fresh_physical);
        let generation = vector::ValidatedVectorGenerationHandle::try_from_active::<
            vector::distance::Euclidean,
        >(&active, fresh_physical)
        .unwrap();
        let index = VectorIndex::<vector::distance::Euclidean>::from_generation(&generation);
        assert!(index.get_item(&db, 62).await.unwrap().is_some());
        db.close().await.unwrap();
    }
}
