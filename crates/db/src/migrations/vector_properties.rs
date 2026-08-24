//! Authoritative legacy vector-property materialization.
//!
//! The pinned pre-V2 writer stored indexed embeddings only in HNSW and removed
//! them from graph property rows. This blocking migration restores those
//! embeddings with the existing graph property codec before V2 can adopt,
//! rebuild, or retire the legacy physical source. Runtime lifecycle code can
//! therefore depend on one authoritative graph representation and needs no
//! legacy HNSW fallback.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use slatedb::{DbReadOps, DbTransaction};

use crate::config;
use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::{self, Property};
use crate::encoding::v2::keys::{DataKeyKind, EdgePropertyByIdKey, DataKey as Key, KeyPrefix};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexElementKind, ValidatedDynamicIndexDefinition};
use crate::search;
use crate::search::vector::{self, VectorIndex};

use super::{
    scan_bounds_for_prefix, LegacyDynamicIndexCatalogEntry, LegacyDynamicIndexDefinition,
    MigrationBatch, MigrationJob, MigrationJobState, MigrationResumeKey, MigrationStage,
};

/// Immutable definitions needed to hydrate one exclusive migration run.
///
/// Grouping by element kind and label prevents each graph row from scanning
/// unrelated definitions. The catalog is rebuilt after a crash and remains
/// immutable until materialization completes.
pub(super) struct LegacyVectorPropertyCatalog {
    by_scope: BTreeMap<
        (IndexElementKind, String),
        Vec<crate::index_lifecycle::ValidatedVectorIndexDefinition>,
    >,
}

impl LegacyVectorPropertyCatalog {
    /// Loads and validates every persisted legacy vector definition once.
    pub(super) async fn load(
        read: &(impl DbReadOps + Send + Sync),
        scope: DataScope,
    ) -> Result<Self> {
        let mut by_scope = BTreeMap::<
            (IndexElementKind, String),
            Vec<crate::index_lifecycle::ValidatedVectorIndexDefinition>,
        >::new();
        let mut identities = BTreeSet::new();
        for row in super::load_legacy_definition_rows(read, scope).await? {
            let LegacyDynamicIndexCatalogEntry::Definition(legacy) = row.entry else {
                continue;
            };
            if legacy.key() != row.identity {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "legacy vector materialization found a key/value identity mismatch".to_string(),
                ));
            }
            let LegacyDynamicIndexDefinition::Vector(_) = &legacy else {
                continue;
            };
            let ValidatedDynamicIndexDefinition::Vector(definition) = legacy.into_validated()?
            else {
                unreachable!("legacy vector definition validates as vector")
            };
            if !identities.insert(definition.identity()) {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "duplicate legacy vector identity during property materialization".to_string(),
                ));
            }
            by_scope
                .entry((
                    definition.element_kind(),
                    definition.label().as_str().to_string(),
                ))
                .or_default()
                .push(definition);
        }
        Ok(Self { by_scope })
    }

    fn definitions(
        &self,
        element_kind: IndexElementKind,
        properties: &[Property],
    ) -> &[crate::index_lifecycle::ValidatedVectorIndexDefinition] {
        let Some(label) = super::label_of(properties) else {
            return &[];
        };
        self.by_scope
            .get(&(element_kind, label.to_string()))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

/// Materializes one bounded node-property batch and its durable resume key.
pub(super) async fn materialize_node_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    catalog: &LegacyVectorPropertyCatalog,
) -> Result<MigrationBatch> {
    materialize_batch(
        transaction,
        scope,
        tuning,
        job,
        catalog,
        IndexElementKind::Node,
    )
    .await
}

/// Materializes one bounded edge-property batch and its durable resume key.
pub(super) async fn materialize_edge_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    catalog: &LegacyVectorPropertyCatalog,
) -> Result<MigrationBatch> {
    materialize_batch(
        transaction,
        scope,
        tuning,
        job,
        catalog,
        IndexElementKind::Edge,
    )
    .await
}

async fn materialize_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    catalog: &LegacyVectorPropertyCatalog,
    element_kind: IndexElementKind,
) -> Result<MigrationBatch> {
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationBatch::StageComplete);
    };
    let stage = match element_kind {
        IndexElementKind::Node => MigrationStage::NodeProperties,
        IndexElementKind::Edge => MigrationStage::EdgeEndpoints,
    };
    let prefix = match element_kind {
        IndexElementKind::Node => Key::data_prefix(
            scope,
            Bytes::copy_from_slice(KeyPrefix::NodeProperty.as_slice()),
        ),
        IndexElementKind::Edge => Key::data_prefix(
            scope,
            Bytes::copy_from_slice(KeyPrefix::EdgeEndpoints.as_slice()),
        ),
    };
    debug_assert_eq!(stage, job.state.running_stage().unwrap_or(stage));
    let bounds = scan_bounds_for_prefix(&prefix, resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadBefore)?;
    let mut rows = transaction.scan(bounds).await?;
    let mut processed_rows = 0_u64;
    let mut admitted_bytes = 0_usize;
    let mut committed_cursor = None;

    while processed_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX) {
        let Some(row) = rows.next().await? else {
            break;
        };
        let (entity_id, property_key, property_value, hydrated_property_bytes) = match element_kind
        {
            IndexElementKind::Node => {
                let Key::Data {
                    kind: DataKeyKind::NodeProperty(key),
                    ..
                } = Key::parse_from_slice(scope, &row.key)?
                else {
                    return Err(HelixDbError::InvariantViolation(
                        "vector materialization node scan returned another key kind".to_string(),
                    ));
                };
                (key.node_id(), row.key.clone(), Some(row.value.clone()), 0)
            }
            IndexElementKind::Edge => {
                let Key::Data {
                    kind: DataKeyKind::EdgeEndpoints(key),
                    ..
                } = Key::parse_from_slice(scope, &row.key)?
                else {
                    return Err(HelixDbError::InvariantViolation(
                        "vector materialization edge scan returned another key kind".to_string(),
                    ));
                };
                let property_key = Key::Data {
                    scope,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(key.edge_id())),
                }
                .to_bytes();
                let property_value = transaction.get(&property_key).await?;
                let hydrated_property_bytes = property_key
                    .len()
                    .checked_add(property_value.as_ref().map_or(0, Bytes::len))
                    .ok_or_else(|| {
                        HelixDbError::InvariantViolation(
                            "vector materialization edge-property input bytes overflowed usize"
                                .to_string(),
                        )
                    })?;
                (
                    key.edge_id(),
                    property_key,
                    property_value,
                    hydrated_property_bytes,
                )
            }
        };
        let mut properties = match property_value.as_deref() {
            Some(value) => property::decode_properties(value)?,
            None => Vec::new(),
        };
        let before = properties.clone();
        let vector_input_bytes = materialize_properties(
            transaction,
            scope,
            catalog,
            element_kind,
            entity_id,
            &mut properties,
        )
        .await?;
        let encoded = property::encode_properties(&properties);
        let changed = properties != before;
        let row_bytes = [
            row.key.len(),
            row.value.len(),
            hydrated_property_bytes,
            usize::try_from(vector_input_bytes).map_err(|_| {
                HelixDbError::InvariantViolation(
                    "vector materialization input bytes do not fit usize".to_string(),
                )
            })?,
            if changed { property_key.len() } else { 0 },
            if changed { encoded.len() } else { 0 },
        ]
        .into_iter()
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "vector materialization byte accounting overflowed usize".to_string(),
            )
        })?;
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(row_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "vector materialization batch bytes overflowed usize".to_string(),
            ));
        };
        if next_admitted_bytes > tuning.batch_bytes().get() {
            if processed_rows == 0 {
                return Err(HelixDbError::Config(format!(
                    "vector materialization entity {entity_id} requires {row_bytes} bytes, exceeding the {} byte batch limit",
                    tuning.batch_bytes().get()
                )));
            }
            break;
        }
        if changed {
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteBefore)?;
            transaction.put(property_key, encoded)?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteAfter)?;
        }
        admitted_bytes = next_admitted_bytes;
        processed_rows = processed_rows.saturating_add(1);
        committed_cursor = MigrationResumeKey::new(row.key.to_vec());
    }

    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadAfter)?;
    let Some(resume_after_key) = committed_cursor else {
        return Ok(MigrationBatch::StageComplete);
    };
    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: processed_rows,
        source_bytes: u64::try_from(admitted_bytes).map_err(|_| {
            HelixDbError::InvariantViolation(
                "vector materialization batch bytes do not fit u64".to_string(),
            )
        })?,
    })
}

async fn materialize_properties(
    transaction: &DbTransaction,
    scope: DataScope,
    catalog: &LegacyVectorPropertyCatalog,
    element_kind: IndexElementKind,
    entity_id: u64,
    properties: &mut Vec<Property>,
) -> Result<u64> {
    let definitions = catalog.definitions(element_kind, properties);
    let mut input_bytes = 0_u64;
    for definition in definitions {
        properties.retain(|property| property.name != definition.property().as_str());
        let runtime = definition.to_runtime();
        let physical_name = match runtime.tenant_property() {
            None => search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            ),
            Some(tenant_property) => {
                let Some(tenant_value) = properties
                    .iter()
                    .find(|property| property.name == tenant_property)
                    .map(|property| &property.value)
                    .and_then(search::text::normalize_tenant_value)
                else {
                    continue;
                };
                search::vector_tenant_index_name(
                    runtime.element_type(),
                    runtime.label(),
                    runtime.property(),
                    tenant_property,
                    tenant_value,
                )
            }
        };
        let read = match definition.metric() {
            vector::VectorDistanceMetric::Cosine => {
                VectorIndex::<vector::distance::Cosine>::for_legacy_migration(physical_name, scope)
                    .legacy_vector_for_migration(transaction, entity_id, definition)
                    .await
            }
            vector::VectorDistanceMetric::Euclidean => {
                VectorIndex::<vector::distance::Euclidean>::for_legacy_migration(
                    physical_name,
                    scope,
                )
                .legacy_vector_for_migration(transaction, entity_id, definition)
                .await
            }
            vector::VectorDistanceMetric::Manhattan => {
                VectorIndex::<vector::distance::Manhattan>::for_legacy_migration(
                    physical_name,
                    scope,
                )
                .legacy_vector_for_migration(transaction, entity_id, definition)
                .await
            }
        };
        let read = match read {
            Err(HelixDbError::InvalidVectorItem(
                vector::VectorItemDecodeError::ZeroNormCosineVector,
            )) => {
                return Err(HelixDbError::LegacyZeroNormCosineVector {
                    element_kind,
                    label: definition.label().as_str().to_string(),
                    property: definition.property().as_str().to_string(),
                    entity_id,
                });
            }
            result => result?,
        };
        input_bytes = input_bytes.checked_add(read.input_bytes()).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "vector materialization HNSW input bytes overflowed u64".to_string(),
            )
        })?;
        let vector = read.into_vector();
        let Some(vector) = vector else {
            continue;
        };
        if definition.metric() == vector::VectorDistanceMetric::Cosine
            && vector.iter().all(|component| *component == 0.0)
        {
            return Err(HelixDbError::LegacyZeroNormCosineVector {
                element_kind,
                label: definition.label().as_str().to_string(),
                property: definition.property().as_str().to_string(),
                entity_id,
            });
        }
        properties.push(Property::f32_array(definition.property().as_str(), vector));
    }
    Ok(input_bytes)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::VectorIndexDefinition;
    use crate::encoding::keys::{EdgeEndpointsKey, EdgePropertyByIdKey, NodePropertyKey};
    use crate::encoding::v2::keys::indexes::vector::{
        VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorSimHashKey,
    };
    use crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue;
    use crate::encoding::v2::values::indexes::vector::simhash;
    use crate::encoding::v2::legacy::vector::metadata as legacy_metadata;
    use crate::index_lifecycle::ValidatedVectorIndexDefinition;
    use crate::search::vector::{self, Item, VectorDistanceMetric, VectorIndexConfig};

    async fn database(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("materialization test database opens")
    }

    fn definition(metric: VectorDistanceMetric) -> ValidatedVectorIndexDefinition {
        let definition: ValidatedDynamicIndexDefinition =
            VectorIndexDefinition::new_node("Document", "embedding", 3, metric)
                .expect("test definition is valid")
                .try_into()
                .expect("test definition validates for V2");
        let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
            unreachable!("vector definition validates as vector")
        };
        definition
    }

    fn catalog(
        definitions: impl IntoIterator<Item = ValidatedVectorIndexDefinition>,
    ) -> LegacyVectorPropertyCatalog {
        let mut by_scope = BTreeMap::new();
        for definition in definitions {
            by_scope
                .entry((
                    definition.element_kind(),
                    definition.label().as_str().to_string(),
                ))
                .or_insert_with(Vec::new)
                .push(definition);
        }
        LegacyVectorPropertyCatalog { by_scope }
    }

    async fn populate_legacy_vector<D: vector::Distance>(
        db: &Db,
        definition: &ValidatedVectorIndexDefinition,
        entity_id: u64,
        value: &[f32],
    ) {
        let runtime = definition.to_runtime();
        let physical_name =
            search::vector_index_name(runtime.element_type(), runtime.label(), runtime.property());
        populate_legacy_vector_named::<D>(db, definition, physical_name, entity_id, value).await;
    }

    async fn populate_legacy_vector_named<D: vector::Distance>(
        db: &Db,
        definition: &ValidatedVectorIndexDefinition,
        physical_name: String,
        entity_id: u64,
        value: &[f32],
    ) {
        let physical_id = vector::index_id_from_name(&physical_name);
        let metadata_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                physical_id,
            ))),
        }
        .to_bytes();
        let index =
            VectorIndex::<D>::for_legacy_migration(physical_name, DataScope::LegacyUnscoped);
        let create = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index
            .create(
                &create,
                VectorIndexConfig::from_v2_definition(definition, index.name()),
            )
            .await
            .unwrap();
        create.commit().await.unwrap();
        let insert = db.begin(IsolationLevel::Snapshot).await.unwrap();
        index.insert(&insert, entity_id, value).await.unwrap();
        insert.commit().await.unwrap();
        let current = index.get_metadata(db).await.unwrap().unwrap();
        db.put(
            metadata_key,
            Bytes::copy_from_slice(&legacy_metadata::encode_legacy_metadata_for_contract(
                &current,
            )),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn node_batch_restores_the_hnsw_vector_as_a_graph_property() {
        let db = database("materialize-node-vector").await;
        let definition = definition(VectorDistanceMetric::Cosine);
        let entity_id = 41;
        populate_legacy_vector::<vector::distance::Cosine>(
            &db,
            &definition,
            entity_id,
            &[1.0, 2.0, 3.0],
        )
        .await;
        let property_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
        }
        .to_bytes();
        db.put(
            &property_key,
            property::encode_properties(&[
                Property::string("$label", "Document"),
                Property::string("title", "retained"),
            ]),
        )
        .await
        .unwrap();
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let batch = materialize_node_batch(
            &transaction,
            DataScope::LegacyUnscoped,
            config::MigrationTuning::default(),
            &MigrationJob::new(
                super::super::MigrationId::LegacyVectorPropertyMaterialization,
                super::super::MigrationMode::BlockingStartup,
            ),
            &catalog([definition]),
        )
        .await
        .unwrap();
        assert!(matches!(batch, MigrationBatch::Advanced { rows: 1, .. }));
        transaction.commit().await.unwrap();

        let properties =
            property::decode_properties(&db.get(property_key).await.unwrap().unwrap()).unwrap();
        assert!(properties.contains(&Property::string("title", "retained")));
        assert!(properties.contains(&Property::f32_array("embedding", vec![1.0, 2.0, 3.0])));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn node_batch_bounds_combined_graph_hnsw_and_staged_bytes() {
        let db = database("materialize-node-vector-byte-bound").await;
        let definition = definition(VectorDistanceMetric::Cosine);
        let entity_id = 42;
        populate_legacy_vector::<vector::distance::Cosine>(
            &db,
            &definition,
            entity_id,
            &[1.0, 2.0, 3.0],
        )
        .await;
        let property_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(entity_id)),
        }
        .to_bytes();
        let property_value = property::encode_properties(&[
            Property::string("$label", "Document"),
            Property::string("title", "retained"),
        ]);
        db.put(&property_key, property_value.clone()).await.unwrap();
        let job = MigrationJob::new(
            super::super::MigrationId::LegacyVectorPropertyMaterialization,
            super::super::MigrationMode::BlockingStartup,
        );
        let definitions = catalog([definition]);
        let measurement = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let MigrationBatch::Advanced {
            rows: 1,
            source_bytes,
            ..
        } = materialize_node_batch(
            &measurement,
            DataScope::LegacyUnscoped,
            config::MigrationTuning::default()
                .with_batch_rows(config::MigrationBatchRows::new(1).expect("one row is positive")),
            &job,
            &definitions,
        )
        .await
        .unwrap()
        else {
            panic!("one materialized row returns its measured batch")
        };
        measurement.rollback();
        assert!(
            source_bytes > u64::try_from(property_key.len() + property_value.len()).unwrap(),
            "the budget includes HNSW reads and the staged graph value"
        );

        let below_limit = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let error = materialize_node_batch(
            &below_limit,
            DataScope::LegacyUnscoped,
            config::MigrationTuning::default()
                .with_batch_rows(config::MigrationBatchRows::new(1).expect("one row is positive"))
                .with_batch_bytes(
                    config::MigrationBatchBytes::new(
                        usize::try_from(source_bytes - 1).expect("test bytes fit usize"),
                    )
                    .expect("measured bytes exceed one"),
                ),
            &job,
            &definitions,
        )
        .await
        .expect_err("one byte below the complete item must fail closed");
        below_limit.rollback();
        assert!(error.to_string().contains("exceeding the"));

        let exact_limit = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let batch = materialize_node_batch(
            &exact_limit,
            DataScope::LegacyUnscoped,
            config::MigrationTuning::default()
                .with_batch_rows(config::MigrationBatchRows::new(1).expect("one row is positive"))
                .with_batch_bytes(
                    config::MigrationBatchBytes::new(
                        usize::try_from(source_bytes).expect("test bytes fit usize"),
                    )
                    .expect("measured bytes are positive"),
                ),
            &job,
            &definitions,
        )
        .await
        .unwrap();
        assert!(matches!(
            batch,
            MigrationBatch::Advanced {
                rows: 1,
                source_bytes: exact,
                ..
            } if exact == source_bytes
        ));
        exact_limit.rollback();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn materialization_covers_nodes_edges_tenants_metrics_and_missing_items() {
        let db = database("materialize-vector-contract-matrix").await;
        let validated = |definition: VectorIndexDefinition| {
            let definition: ValidatedDynamicIndexDefinition = definition
                .try_into()
                .expect("test definition validates for V2");
            let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
                unreachable!("vector definition validates as vector")
            };
            definition
        };
        let cosine = validated(
            VectorIndexDefinition::new_node(
                "Document",
                "cosine_embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .expect("cosine definition is valid"),
        );
        let euclidean = validated(
            VectorIndexDefinition::new_node(
                "Document",
                "euclidean_embedding",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("euclidean definition is valid")
            .with_tenant_property("tenant")
            .expect("tenant property is valid"),
        );
        let manhattan = validated(
            VectorIndexDefinition::new_edge(
                "LINKS",
                "manhattan_embedding",
                3,
                VectorDistanceMetric::Manhattan,
            )
            .expect("manhattan definition is valid"),
        );
        let node_id = 11;
        let missing_node_id = 12;
        let edge_id = 21;
        populate_legacy_vector::<vector::distance::Cosine>(&db, &cosine, node_id, &[1.0, 0.0, 0.0])
            .await;
        let tenant = Property::string("tenant", "alpha");
        let tenant_runtime = euclidean.to_runtime();
        let tenant_name = search::vector_tenant_index_name(
            tenant_runtime.element_type(),
            tenant_runtime.label(),
            tenant_runtime.property(),
            tenant_runtime
                .tenant_property()
                .expect("fixture is tenant partitioned"),
            &tenant.value,
        );
        populate_legacy_vector_named::<vector::distance::Euclidean>(
            &db,
            &euclidean,
            tenant_name,
            node_id,
            &[2.0, 3.0, 4.0],
        )
        .await;
        populate_legacy_vector::<vector::distance::Manhattan>(
            &db,
            &manhattan,
            edge_id,
            &[5.0, 6.0, 7.0],
        )
        .await;

        let present_node_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
        }
        .to_bytes();
        db.put(
            &present_node_key,
            property::encode_properties(&[
                Property::string("$label", "Document"),
                tenant.clone(),
                Property::string("title", "retained"),
                Property::f32_array("cosine_embedding", vec![9.0, 9.0, 9.0]),
                Property::f32_array("euclidean_embedding", vec![9.0, 9.0, 9.0]),
            ]),
        )
        .await
        .unwrap();
        let missing_node_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(missing_node_id)),
        }
        .to_bytes();
        db.put(
            &missing_node_key,
            property::encode_properties(&[
                Property::string("$label", "Document"),
                tenant,
                Property::f32_array("cosine_embedding", vec![8.0, 8.0, 8.0]),
                Property::f32_array("euclidean_embedding", vec![8.0, 8.0, 8.0]),
            ]),
        )
        .await
        .unwrap();
        db.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
            }
            .to_bytes(),
            EdgeEndpointsValue::new(node_id, missing_node_id).encode(),
        )
        .await
        .unwrap();
        let edge_property_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
        }
        .to_bytes();
        db.put(
            &edge_property_key,
            property::encode_properties(&[
                Property::string("$label", "LINKS"),
                Property::string("title", "retained"),
                Property::f32_array("manhattan_embedding", vec![7.0, 7.0, 7.0]),
            ]),
        )
        .await
        .unwrap();

        let definitions = catalog([cosine, euclidean, manhattan]);
        let mut job = MigrationJob::new(
            super::super::MigrationId::LegacyVectorPropertyMaterialization,
            super::super::MigrationMode::BlockingStartup,
        );
        let node_transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let node_batch = materialize_node_batch(
            &node_transaction,
            DataScope::LegacyUnscoped,
            config::MigrationTuning::default(),
            &job,
            &definitions,
        )
        .await
        .unwrap();
        assert!(matches!(
            node_batch,
            MigrationBatch::Advanced { rows: 2, .. }
        ));
        node_transaction.commit().await.unwrap();
        super::super::advance_or_complete(&mut job);
        let edge_transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let edge_batch = materialize_edge_batch(
            &edge_transaction,
            DataScope::LegacyUnscoped,
            config::MigrationTuning::default(),
            &job,
            &definitions,
        )
        .await
        .unwrap();
        assert!(matches!(
            edge_batch,
            MigrationBatch::Advanced { rows: 1, .. }
        ));
        edge_transaction.commit().await.unwrap();

        let present =
            property::decode_properties(&db.get(present_node_key).await.unwrap().unwrap()).unwrap();
        assert!(present.contains(&Property::string("title", "retained")));
        assert!(present.contains(&Property::f32_array(
            "cosine_embedding",
            vec![1.0, 0.0, 0.0],
        )));
        assert!(present.contains(&Property::f32_array(
            "euclidean_embedding",
            vec![2.0, 3.0, 4.0],
        )));
        let missing =
            property::decode_properties(&db.get(missing_node_key).await.unwrap().unwrap()).unwrap();
        assert!(!missing.iter().any(|property| {
            matches!(
                property.name.as_str(),
                "cosine_embedding" | "euclidean_embedding"
            )
        }));
        let edge = property::decode_properties(&db.get(edge_property_key).await.unwrap().unwrap())
            .unwrap();
        assert!(edge.contains(&Property::string("title", "retained")));
        assert!(edge.contains(&Property::f32_array(
            "manhattan_embedding",
            vec![5.0, 6.0, 7.0],
        )));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn zero_norm_cosine_reports_the_exact_legacy_entity() {
        let db = database("materialize-zero-cosine").await;
        let definition = definition(VectorDistanceMetric::Cosine);
        let runtime = definition.to_runtime();
        let physical_name =
            search::vector_index_name(runtime.element_type(), runtime.label(), runtime.property());
        let physical_id = vector::index_id_from_name(&physical_name);
        let entity_id = 73;
        let config = VectorIndexConfig::from_v2_definition(&definition, &physical_name);
        let bits = 0;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::Vector(VectorKey::IndexMetadata(
                        VectorIndexMetadataKey::new(physical_id),
                    )),
                }
                .to_bytes(),
                Bytes::copy_from_slice(&legacy_metadata::encode_legacy_metadata_for_contract(
                    &vector::VectorIndexMetadata::new(config),
                )),
            )
            .unwrap();
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::Vector(VectorKey::SimHash(VectorSimHashKey::new(
                        physical_id,
                        entity_id,
                    ))),
                }
                .to_bytes(),
                Bytes::copy_from_slice(&simhash::encode_simhash(bits)),
            )
            .unwrap();
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::Vector(VectorKey::Vector(VectorItemKey::new(
                        physical_id,
                        vector::simhash::order_code_from_simhash_bits(bits),
                        entity_id,
                    ))),
                }
                .to_bytes(),
                vector::encode_item(&Item::<vector::distance::Cosine>::new(vec![0.0, 0.0, 0.0])),
            )
            .unwrap();
        transaction.commit().await.unwrap();
        let mut properties = vec![Property::string("$label", "Document")];
        let materialization = db.begin(IsolationLevel::Snapshot).await.unwrap();

        let error = materialize_properties(
            &materialization,
            DataScope::LegacyUnscoped,
            &catalog([definition]),
            IndexElementKind::Node,
            entity_id,
            &mut properties,
        )
        .await
        .expect_err("zero-norm cosine must block migration");
        assert!(matches!(
            error,
            HelixDbError::LegacyZeroNormCosineVector {
                element_kind: IndexElementKind::Node,
                ref label,
                ref property,
                entity_id: 73,
            } if label == "Document" && property == "embedding"
        ));
        assert_eq!(properties, vec![Property::string("$label", "Document")]);
        materialization.rollback();
        db.close().await.unwrap();
    }
}
