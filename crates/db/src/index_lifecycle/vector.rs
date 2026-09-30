//! Generation-qualified vector mutation and lifecycle ownership.
//!
//! Ordinary graph mutations load one [`VectorMutationSet`] from canonical V2
//! records in their serializable transaction and enqueue one complete
//! operation per changed entity for every Building or Active generation.
//! Publication ([`publication`]) later plans that work with the build planner
//! into only the physical namespaces an `Active` generation's canonical record
//! and checked tenant mappings authorize, and defers a hidden `Building`
//! generation until activation. Missing tenant mappings are created only with
//! the first admitted work for that partition, never by a read.
//!
//! The same semantic document projection is used by the queue producer and
//! the outbox builder. It validates labels, dimensions, finite f32 conversion,
//! cosine zero vectors, metric-specific component magnitude, and type-preserving
//! tenant identity before any HNSW or lifecycle row is staged.

use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::values::property::encode_index_partition_value;
use crate::error::{HelixDbError, Result};
use crate::search;
use crate::search::vector::{ValidatedMetricVector, VectorDimension};

use super::{
    ActiveIndexHandle, IndexEntityId, IndexGenerationId, IndexId, TextPartition,
    ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition,
};

mod driver;
#[cfg(all(feature = "production-coverage", not(test)))]
pub(crate) use driver::build_cache_production_contracts::run as run_build_cache_contracts;
pub(crate) mod publication;
pub(crate) use driver::{RetainedVectorBuild, VectorBuildCache, VectorIndexDriver};

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

/// Lifecycle role of one routed vector generation.
#[derive(Debug, Clone)]
enum VectorMutationMode {
    /// Planner-visible generation the queue worker publishes into.
    Active(ActiveIndexHandle),
    /// Hidden build that scans source rows; queued work waits for activation.
    Building,
}

/// One vector generation selected for queued maintenance.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueuedVectorTarget<'a> {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) definition: &'a ValidatedVectorIndexDefinition,
    /// Active generation handle; `None` for a hidden build.
    pub(crate) active: Option<&'a ActiveIndexHandle>,
}

/// Transaction-local vector generations loaded from canonical records.
#[derive(Debug, Clone, Default)]
pub(crate) struct VectorMutationSet {
    targets: Vec<VectorMutationTarget>,
}

impl VectorMutationSet {
    /// Resolves one routed target for queued maintenance.
    pub(crate) fn queued_target(&self, ordinal: usize) -> Result<QueuedVectorTarget<'_>> {
        let target = self.targets.get(ordinal).ok_or_else(|| {
            corruption("vector mutation route named a target outside its catalog")
        })?;
        Ok(QueuedVectorTarget {
            index_id: target.index_id,
            generation: target.generation,
            definition: &target.definition,
            active: match &target.mode {
                VectorMutationMode::Active(handle) => Some(handle),
                VectorMutationMode::Building => None,
            },
        })
    }

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
                (record, VectorMutationMode::Building)
            }
            super::mutation_catalog::MutationCatalogEntry::Active { record, handle } => {
                if !matches!(handle, ActiveIndexHandle::Vector { .. }) {
                    return Err(corruption(
                        "active vector record carried another family handle",
                    ));
                }
                (record, VectorMutationMode::Active(handle.clone()))
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

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, DbTransaction, IsolationLevel};

    use bytes::Bytes;

    use super::*;
    use crate::config::SearchIndexBackfillLimits;
    use crate::encoding::v2::keys::indexes::vector::{VectorKey, VectorStorageLane};
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
    use crate::encoding::v2::keys::{
        DataKey, DataKeyKind, GlobalKey, ScopedKey, VectorPartitionMappingKey,
    };
    use crate::encoding::v2::values::encode_build_delta;
    use crate::index_lifecycle::queue::publication::VectorPublicationResources;
    use crate::index_lifecycle::queue::storage::AcknowledgementOutput;
    use crate::index_lifecycle::repository;
    use crate::index_lifecycle::work::{
        CoalescedBuildDeltaState, CoalescedBuildDeltaValue, VectorTenantPartition,
    };
    use crate::index_lifecycle::{IndexElementKind, VectorPhysicalIndexId, VectorPhysicalLayout};
    use crate::search::vector::{self, Distance, VectorCacheWriteSet, VectorDistanceMetric};

    fn scoped_index_key(scope: DataScope, logical: ScopedKey) -> Bytes {
        IndexKey::Data {
            scope,
            kind: logical,
        }
        .to_bytes()
    }
    use crate::encoding::v2::values::encode_metadata_value;
    use crate::index_lifecycle::{
        IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition, IndexV2MetadataValue,
        PhysicalGeneration, VectorGenerationDescriptor, VectorPhysicalIdWatermark,
    };
    use crate::search::vector::{VectorIndex, VectorIndexConfig};

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

    /// One complete entity transition, as the queue producer derives it.
    #[derive(Clone, Copy)]
    struct VectorEntityMutation<'a> {
        entity_id: u64,
        before: &'a [Property],
        after: &'a [Property],
    }

    impl<'a> VectorEntityMutation<'a> {
        const fn new(
            _kind: IndexElementKind,
            entity_id: u64,
            before: &'a [Property],
            after: &'a [Property],
        ) -> Self {
            Self {
                entity_id,
                before,
                after,
            }
        }
    }

    /// Publishes one transaction's entity transitions into every Active
    /// target through queued publication staging, the only production writer
    /// of Active vector rows, with no acknowledgement beside them.
    async fn maintain_entities(
        db: &Db,
        transaction: &DbTransaction,
        mutations: &VectorMutationSet,
        cache_writes: &VectorCacheWriteSet,
        entities: &[VectorEntityMutation<'_>],
    ) -> Result<()> {
        let resources = VectorPublicationResources {
            cache_registry: Arc::new(vector::VectorCacheRegistry::default()),
            simhasher_registry: Arc::new(vector::SimHasherRegistry::default()),
            batch_reads: crate::batch_reads::BatchReads::Single,
            planning_cache: Arc::new(VectorBuildCache::new(
                SearchIndexBackfillLimits::default().vector_build_cache_bytes(),
            )),
        };
        for target in &mutations.targets {
            let VectorMutationMode::Active(handle) = &target.mode else {
                continue;
            };
            let mut effects = Vec::new();
            for entity in entities {
                let before = vector_document(&target.definition, entity.before)?;
                let after = vector_document(&target.definition, entity.after)?;
                if before == after {
                    // The producer queues nothing for an unchanged document.
                    continue;
                }
                effects.push(publication::QueuedVectorEffect {
                    entity_id: IndexEntityId::new(entity.entity_id),
                    stale: before
                        .map(|document| document.partition)
                        .into_iter()
                        .collect(),
                    replacement: after
                        .map(|document| {
                            crate::encoding::v2::values::indexes::operation_queue::QueuedVectorReplacement::try_new(
                                document.partition,
                                document.vector.into(),
                            )
                        })
                        .transpose()
                        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
                });
            }
            let Some(expected) = NonZeroUsize::new(effects.len()) else {
                continue;
            };
            let staged = publication::stage_active_effects(
                db,
                transaction,
                handle,
                &effects,
                SearchIndexBackfillLimits::default().batch(),
                AcknowledgementOutput {
                    operations: 0,
                    bytes: 0,
                },
                &resources,
                cache_writes,
            )
            .await?;
            assert_eq!(staged, publication::StagedEffects::Prefix(expected));
        }
        Ok(())
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
                mode: VectorMutationMode::Active(handle.clone()),
            },
            handle,
        )
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
        maintain_entities(
            &db,
            &insert,
            &mutations,
            &cache_writes,
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                9,
                &[],
                &properties,
            )],
        )
        .await
        .unwrap();
        insert.commit().await.unwrap();
        assert!(index.get_item(&db, 9).await.unwrap().is_some());

        let delete = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entities(
            &db,
            &delete,
            &mutations,
            &cache_writes,
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                9,
                &properties,
                &[],
            )],
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

        for (vector, invalid_index) in [
            (vec![f32::NAN, 2.0, 3.0], 0),
            (vec![1.0, f32::INFINITY, 3.0], 1),
            (vec![1.0, 2.0, f32::NEG_INFINITY], 2),
        ] {
            assert!(matches!(
                vector_document(
                    &validated_definition(None, VectorDistanceMetric::Euclidean),
                    &[
                        property("$label", PropertyValue::String("Document".to_string())),
                        property("embedding", PropertyValue::F32Array(vector)),
                    ],
                ),
                Err(HelixDbError::InvalidVectorComponent { index }) if index == invalid_index
            ));
        }

        for vector in [vec![1.0, 2.0], vec![1.0, 2.0, 3.0, 4.0]] {
            let actual = vector.len();
            assert!(matches!(
                vector_document(
                    &validated_definition(None, VectorDistanceMetric::Euclidean),
                    &[
                        property("$label", PropertyValue::String("Document".to_string())),
                        property("embedding", PropertyValue::F32Array(vector)),
                    ],
                ),
                Err(HelixDbError::InvalidDimension { expected: 3, got }) if got == actual
            ));
        }

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

    /// Plans every effect of one batch in one publication transaction: later
    /// effects see earlier ones, and the transaction reads its own writes.
    #[tokio::test]
    async fn one_publication_stages_a_batch_of_inserts_updates_and_deletes() {
        let db = test_db("vector-active-publication-batch").await;
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
        let document = |vector: [f32; 3]| {
            vec![
                property("$label", PropertyValue::String("Document".to_string())),
                property("embedding", PropertyValue::F32Array(vector.to_vec())),
            ]
        };
        let first = document([1.0, 0.0, 0.0]);
        let second = document([0.0, 1.0, 0.0]);
        let third = document([0.0, 0.0, 1.0]);
        let replacement = document([1.0, 1.0, 1.0]);
        let insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entities(
            &db,
            &insert,
            &mutations,
            &cache_writes,
            &[
                VectorEntityMutation::new(IndexElementKind::Node, 1, &[], &first),
                VectorEntityMutation::new(IndexElementKind::Node, 2, &[], &second),
                VectorEntityMutation::new(IndexElementKind::Node, 3, &[], &third),
            ],
        )
        .await
        .unwrap();
        insert.commit().await.unwrap();
        assert_eq!(index.get_metadata(&db).await.unwrap().unwrap().count, 3);

        let change = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entities(
            &db,
            &change,
            &mutations,
            &cache_writes,
            &[
                VectorEntityMutation::new(IndexElementKind::Node, 1, &first, &replacement),
                VectorEntityMutation::new(IndexElementKind::Node, 2, &second, &[]),
            ],
        )
        .await
        .unwrap();
        assert_eq!(
            index
                .get_item(&change, 1)
                .await
                .unwrap()
                .unwrap()
                .vector
                .to_vec(),
            vec![1.0, 1.0, 1.0],
            "the publication transaction reads its applied plan"
        );
        change.commit().await.unwrap();

        assert_eq!(index.get_metadata(&db).await.unwrap().unwrap().count, 2);
        assert!(index.get_item(&db, 2).await.unwrap().is_none());
        assert!(index.get_item(&db, 3).await.unwrap().is_some());
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

        maintain_entities(
            &db,
            &transaction,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                9,
                &properties,
                &[],
            )],
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

    /// Tolerates removal from a tenant partition that never materialized:
    /// queued routing can name a state a hidden build never applied.
    #[tokio::test]
    async fn active_tenant_removal_tolerates_a_missing_partition_mapping() {
        let db = test_db("vector-active-missing-tenant-mapping").await;
        let definition = validated_definition(Some("account_id"), VectorDistanceMetric::Euclidean);
        let (target, _) = active_target(definition, VectorPhysicalLayout::Partitioned);
        let index_id = target.index_id;
        let generation = target.generation;
        let mutations = VectorMutationSet {
            targets: vec![target],
        };
        let cache_writes = VectorCacheWriteSet::default();
        let properties = vec![
            property("$label", PropertyValue::String("Document".to_string())),
            property("account_id", PropertyValue::I64(7)),
            property("embedding", PropertyValue::F32Array(vec![1.0, 2.0, 3.0])),
        ];
        let partition = VectorTenantPartition::try_from_partition(
            vector_partition(&mutations.targets[0].definition, &properties)
                .unwrap()
                .expect("label and tenant project a partition"),
        )
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();

        maintain_entities(
            &db,
            &transaction,
            &mutations,
            &cache_writes,
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                9,
                &properties,
                &[],
            )],
        )
        .await
        .expect("removal from an unmaterialized partition stages nothing");
        assert!(cache_writes.entries().is_empty());
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

        maintain_entities(
            &db,
            &transaction,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                9,
                &properties,
                &properties,
            )],
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();
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
            maintain_entities(
                &db,
                &transaction,
                &mutations,
                &VectorCacheWriteSet::default(),
                &[VectorEntityMutation::new(
                    IndexElementKind::Node,
                    9,
                    &[],
                    &properties
                )],
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
            maintain_entities(
                &db,
                &transaction,
                &mutations,
                &VectorCacheWriteSet::default(),
                &[VectorEntityMutation::new(
                    IndexElementKind::Node,
                    9,
                    &[],
                    &properties
                )],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        drop(transaction);
        db.close().await.unwrap();
    }

    /// A namespace at the ID the watermark offers, visible to the publication
    /// transaction itself, is a stale watermark and fails closed.
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
            maintain_entities(
                &db,
                &transaction,
                &mutations,
                &VectorCacheWriteSet::default(),
                &[VectorEntityMutation::new(
                    IndexElementKind::Node,
                    9,
                    &[],
                    &properties
                )],
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason)) if reason.contains("watermark")
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

        assert!(maintain_entities(
            &db,
            &transaction,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                9,
                &before,
                &after
            )],
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
        maintain_entities(
            &db,
            &insert,
            &mutations,
            &cache_writes,
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                19,
                &[],
                &first,
            )],
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
        maintain_entities(
            &db,
            &update,
            &mutations,
            &cache_writes,
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                19,
                &first,
                &second,
            )],
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
        maintain_entities(
            &db,
            &insert,
            &mutations,
            &insert_cache_writes,
            &[41, 42].map(|entity_id| {
                VectorEntityMutation::new(IndexElementKind::Node, entity_id, &[], &properties)
            }),
        )
        .await
        .unwrap();
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
        maintain_entities(
            &db,
            &delete_non_last,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                41,
                &properties,
                &[],
            )],
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
        maintain_entities(
            &db,
            &delete_last,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                42,
                &properties,
                &[],
            )],
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
        maintain_entities(
            &db,
            &reinsert,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                43,
                &[],
                &properties,
            )],
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
        maintain_entities(
            &db,
            &insert,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                51,
                &[],
                &properties,
            )],
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
        let error = maintain_entities(
            &db,
            &delete,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                51,
                &properties,
                &[],
            )],
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
        maintain_entities(
            &db,
            &seed,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                61,
                &[],
                &properties,
            )],
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
        maintain_entities(
            &db,
            &delete,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                61,
                &properties,
                &[],
            )],
        )
        .await
        .unwrap();
        let racing_insert = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        maintain_entities(
            &db,
            &racing_insert,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                62,
                &[],
                &properties,
            )],
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
        maintain_entities(
            &db,
            &retry,
            &mutations,
            &VectorCacheWriteSet::default(),
            &[VectorEntityMutation::new(
                IndexElementKind::Node,
                62,
                &[],
                &properties,
            )],
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
