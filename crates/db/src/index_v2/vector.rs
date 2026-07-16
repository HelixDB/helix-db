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
//! cosine zero vectors, and type-preserving tenant identity before any HNSW or
//! lifecycle row is staged.

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::config::VectorIndexDefinition;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v1::keys::index_v2::{IndexEntity, IndexEntityStateKey, IndexV2Key};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::property::encode_index_partition_value;
use crate::encoding::v1::values::index_v2::{
    decode_index_record, encode_work_value, IndexV2WorkValue,
};
use crate::error::{HelixDbError, Result};
use crate::search;
use crate::search::vector::{
    self, managed_vector_write_index, Distance, VectorCacheWriteSet, VectorDistanceMetric,
    VectorIndexConfig,
};

use super::repository;
use super::work::{CoalescedBuildDeltaValue, VectorTenantPartition};
use super::{
    ActiveIndexHandle, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexStateV2,
    TextPartition, ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition,
    VectorPhysicalIndexId, VectorPhysicalLayout,
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

    /// Returns whether an exact runtime definition has canonical V2 ownership.
    ///
    /// Both `Building` and `Active` targets own ordinary mutation behavior:
    /// builders record deltas, while active generations update physical rows.
    pub(crate) fn owns_runtime_definition(&self, definition: &VectorIndexDefinition) -> bool {
        let Ok(candidate) = ValidatedVectorIndexDefinition::try_from_runtime(definition) else {
            return false;
        };
        self.targets
            .iter()
            .any(|target| target.definition == candidate)
    }
}

/// Loads every vector generation whose state requires mutation work.
///
/// The canonical record scan is part of the caller's serializable graph
/// transaction. Activation/drop revisions therefore conflict with the graph
/// commit rather than allowing writes to cross a lifecycle boundary.
pub(crate) async fn load_mutation_set(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<VectorMutationSet> {
    let logical_prefix = IndexV2Key::logical_prefix(
        crate::encoding::v1::keys::index_v2::IndexV2RecordKind::IndexRecord,
    );
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
                "vector mutation catalog prefix yielded another key kind",
            ));
        };
        let record = decode_index_record(&row.value)?;
        if key.identity != *record.identity() {
            return Err(corruption(
                "vector mutation catalog key/value identity mismatch",
            ));
        }
        let definition = match record.definition() {
            ValidatedDynamicIndexDefinition::Vector(definition) => definition,
            ValidatedDynamicIndexDefinition::Secondary(_)
            | ValidatedDynamicIndexDefinition::Text(_) => continue,
        };
        let mode = match record.state() {
            IndexStateV2::Building { .. } => VectorMutationMode::RecordBuildDelta,
            IndexStateV2::Active { .. } => VectorMutationMode::MaintainActive(
                ActiveIndexHandle::try_from_record(scope, &record)
                    .ok_or_else(|| corruption("active vector record did not project a handle"))?,
            ),
            IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. }
            | IndexStateV2::Dropped { .. } => continue,
        };
        targets.push(VectorMutationTarget {
            index_id: record.index_id(),
            generation: record.state().generation(),
            definition: definition.clone(),
            mode,
        });
    }
    Ok(VectorMutationSet { targets })
}

/// Maintains every V2 vector generation affected by one graph entity.
///
/// `before` and `after` are complete authoritative property sets. Partition
/// moves therefore become a typed remove-plus-upsert, and hidden builds receive
/// one coalesced reconciliation marker for any semantic document change.
pub(crate) async fn maintain_entity(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &VectorMutationSet,
    cache_writes: &VectorCacheWriteSet,
    entity: VectorEntityMutation<'_>,
) -> Result<()> {
    for target in mutations
        .targets
        .iter()
        .filter(|target| target.definition.element_kind() == entity.entity_kind)
    {
        let old_document = vector_document(&target.definition, entity.before)?;
        let new_document = vector_document(&target.definition, entity.after)?;
        if old_document == new_document {
            continue;
        }
        match &target.mode {
            VectorMutationMode::RecordBuildDelta => {
                let index_entity = IndexEntity {
                    kind: entity.entity_kind,
                    id: entity.entity_id,
                };
                let key = scoped_index_key(
                    scope,
                    IndexV2Key::BuildDelta(IndexEntityStateKey {
                        index_id: target.index_id,
                        generation: target.generation,
                        entity: index_entity,
                    }),
                );
                let value = IndexV2WorkValue::CoalescedBuildDelta(CoalescedBuildDeltaValue {
                    index_id: target.index_id,
                    generation: target.generation,
                    entity_kind: entity.entity_kind,
                    entity_id: entity.entity_id,
                });
                transaction.put(key, encode_work_value(&value))?;
            }
            VectorMutationMode::MaintainActive(handle) => {
                maintain_active(
                    transaction,
                    scope,
                    target,
                    handle,
                    cache_writes,
                    entity.entity_id,
                    old_document,
                    new_document,
                )
                .await?;
            }
        }
    }
    Ok(())
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
    assert!(!created, "remove paths never allocate vector partitions");
    let generation =
        vector::ValidatedVectorGenerationHandle::try_from_active::<D>(active, physical_index_id)
            .map_err(|error| corruption(error.to_string()))?;
    let index = managed_vector_write_index::<D>(
        &generation,
        cache_writes.dirty_rows_for(&generation),
        cache_writes.simhasher_registry(),
    )
    .map_err(|error| corruption(error.to_string()))?;
    index.delete(transaction, entity_id.get()).await
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
    let index = managed_vector_write_index::<D>(
        &generation,
        cache_writes.dirty_rows_for(&generation),
        cache_writes.simhasher_registry(),
    )
    .map_err(|error| corruption(error.to_string()))?;
    if created {
        index
            .create(
                transaction,
                VectorIndexConfig::from_v2_definition(
                    &target.definition,
                    generation.physical_name(),
                ),
            )
            .await?;
    }
    index
        .insert(transaction, entity_id.get(), document.vector())
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
    let vector = property_vector_to_f32(&property.value)?;
    if vector.len() != definition.dimension() as usize {
        return Err(HelixDbError::InvalidDimension {
            expected: definition.dimension() as usize,
            got: vector.len(),
        });
    }
    if let Some(index) = vector.iter().position(|component| !component.is_finite()) {
        return Err(HelixDbError::InvalidVectorComponent { index });
    }
    if definition.metric() == VectorDistanceMetric::Cosine
        && vector.iter().all(|component| *component == 0.0)
    {
        return Err(HelixDbError::ZeroNormCosineVector);
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
    Ok(Some(VectorIndexedDocument { partition, vector }))
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

fn scoped_index_key(scope: DataScope, logical: IndexV2Key) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(logical),
    }
    .to_bytes()
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::index_v2::{
        IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition, PhysicalGeneration,
        VectorGenerationDescriptor,
    };
    use crate::search::vector::VectorIndex;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .expect("in-memory vector lifecycle database opens");
        repository::bootstrap_writer(&db)
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
    }

    #[tokio::test]
    async fn active_unpartitioned_mutation_upserts_and_removes_exact_physical_generation() {
        let db = test_db("vector-active-unpartitioned-mutation").await;
        let physical_index_id = VectorPhysicalIndexId::new(41).unwrap();
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
}
