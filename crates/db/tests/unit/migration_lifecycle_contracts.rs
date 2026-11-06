//! Migration lifecycle contracts: legacy-definition convergence, populated
//! vector adoption, malformed and ineligible vector lanes, ownership
//! conflicts, invalid V2 bootstrap tuples, failure-preserving resume, and
//! Active/conflicting definition handling.
//!
//! Each runner is driven by `migration_acceptance.rs`; runners that arm
//! process-global migration failpoints run in their own process there.

use std::sync::Arc;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::graph::NodeRef;
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::{PropertyInput, PropertyValue as AstPropertyValue};
use sha2::{Digest, Sha256};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::config::{
    MigrationBatchRows, MigrationTuning, MigrationWorkerMode, SecondaryIndexDefinition,
    SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning, TextIndexDefinition,
    VectorIndexDefinition,
};
use crate::encoding::property;
use crate::encoding::v1::keys::vectors::{
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorSimHashKey,
};
use crate::encoding::v2::keys::{GlobalKey, ScopedKey};
use crate::encoding::v2::values::{
    decode_metadata_value, encode_index_record, encode_metadata_value,
};
use crate::index_lifecycle::{
    IndexId, IndexStateV2, IndexStorageVersion, IndexV2MetadataValue, LogicalIndexIdWatermark,
    PhysicalGeneration, ValidatedDynamicIndexDefinition, VectorPhysicalIdWatermark,
    VectorPhysicalIndexId, VectorPhysicalLayout,
};
use crate::search::vector::VectorDistanceMetric;
use crate::{DbConfig, HelixDB};

/// Proves valid incomplete schemas are promotable only through writer open:
/// covers the reader boundary for every resumable schema stage.
#[tokio::test]
async fn writer_migration_requirements_are_typed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database("writer-migration-requirements");
    let fixture = raw(&database, store).await;
    crate::migrations::startup::bootstrap_writer(&fixture)
        .await
        .expect("current bootstrap tuple commits");

    for marker in [
        GRAPH_FORMAT_V1_READY,
        INDEX_V2_MIGRATION_READY,
        STORAGE_SCHEMA_COMPLETE,
    ] {
        let error =
            crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&fixture)
                .await
                .expect_err("every incomplete current schema requires a writer");
        let HelixDbError::WriterMigrationRequired { requirement } = error else {
            panic!("incomplete current schema must remain typed: {error}")
        };
        assert_eq!(
            requirement,
            crate::error::WriterMigrationRequirement::IncompleteStorageSchema
        );
        assert_eq!(
            requirement.to_string(),
            "storage schema migration is incomplete"
        );
        fixture
            .put(
                scoped_metadata_key(DataScope::LegacyUnscoped, marker),
                Bytes::from_static(b"1"),
            )
            .await
            .expect("ordered readiness marker writes");
    }
    crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&fixture)
        .await
        .expect("complete current schema is reader-ready");

    fixture
        .delete(scoped_metadata_key(
            DataScope::LegacyUnscoped,
            INDEX_STORAGE_V4_CLEANUP_READY,
        ))
        .await
        .expect("legacy fixture clears the V4 cleanup marker");
    for legacy_version in [2, 3] {
        fixture
            .put(
                global(GlobalKey::StorageVersion),
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::new(legacy_version)
                        .expect("legacy reader version is nonzero"),
                )),
            )
            .await
            .expect("legacy storage marker writes");
        let before = all_rows(&fixture).await;
        assert_eq!(
            crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&fixture)
                .await
                .expect("complete versions two and three are explicitly reader-compatible"),
            crate::index_lifecycle::repository::ReaderStorageCompatibility::LegacyEqualityUnion
        );
        assert_eq!(all_rows(&fixture).await, before);
    }
    fixture.close().await.expect("fixture closes");
}

fn database(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4())
}

async fn raw(database: &str, store: Arc<dyn ObjectStore>) -> Db {
    Db::builder(database, store)
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("migration contract raw database opens")
}

async fn all_rows(read: &(impl DbReadOps + Send + Sync)) -> Vec<(Bytes, Bytes)> {
    let mut rows = read.scan(..).await.expect("migration contract scans");
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await.expect("migration contract row reads") {
        collected.push((row.key, row.value));
    }
    collected
}

fn global(key: GlobalKey) -> Bytes {
    IndexKey::Global { kind: key }.to_bytes()
}

async fn assert_current_storage_version(reader: &(impl DbReadOps + Sync)) {
    let marker = reader
        .get(global(GlobalKey::StorageVersion))
        .await
        .expect("storage marker reads")
        .expect("storage marker exists");
    assert_eq!(
        decode_metadata_value(&marker).expect("storage marker decodes"),
        IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
    );
}

/// Commits one row per migration and lifecycle batch; flushes every
/// millisecond so those commits do not each wait out SlateDB's 100 ms
/// default.
fn one_row_config() -> DbConfig {
    DbConfig::new()
        .with_slate_settings(slatedb::Settings {
            flush_interval: Some(std::time::Duration::from_millis(1)),
            ..slatedb::Settings::default()
        })
        .with_migration_tuning(
            MigrationTuning::default().with_batch_rows(
                MigrationBatchRows::new(1).expect("one migration row is positive"),
            ),
        )
        .with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_batch_rows(
                SecondaryIndexLifecycleBatchRows::new(1).expect("one lifecycle row is positive"),
            ),
        )
}

/// Persists one exact migration state without changing any row codec.
async fn put_migration_job(raw: &Db, mut job: MigrationJob) {
    if job.id == MigrationId::GraphFormatV1Rewrite {
        job.complete();
    }
    raw.put(
        MigrationJobKey::new(DataScope::LegacyUnscoped, job.id).into_bytes(),
        encode_json(&job).expect("migration fixture job encodes"),
    )
    .await
    .expect("migration fixture job writes");
}

/// Skips the graph rewrite so generic boundaries target vector work.
async fn mark_graph_rewrite_complete(raw: &Db) {
    put_migration_job(
        raw,
        MigrationJob::new(
            MigrationId::GraphFormatV1Rewrite,
            MigrationMode::BlockingStartup,
        ),
    )
    .await;
    raw.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, GRAPH_FORMAT_V1_READY),
        Bytes::from_static(b"1"),
    )
    .await
    .expect("graph-format readiness writes");
}

/// Marks vector materialization complete for a graph-authoritative fixture.
async fn mark_vector_materialization_complete(raw: &Db) {
    let mut job = MigrationJob::new(
        MigrationId::LegacyVectorPropertyMaterialization,
        MigrationMode::BlockingStartup,
    );
    job.complete();
    put_migration_job(raw, job).await;
}

/// Resets physical cleanup to its first durable stage after adding a source.
async fn reset_vector_cleanup_job(raw: &Db) {
    put_migration_job(
        raw,
        MigrationJob::new(
            MigrationId::LegacyVectorPhysicalCleanup,
            MigrationMode::BlockingStartup,
        ),
    )
    .await;
}

fn definitions() -> Vec<ValidatedDynamicIndexDefinition> {
    vec![
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        SecondaryIndexDefinition::node_unique_equality("User", "slug")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        SecondaryIndexDefinition::node_range("User", "rank")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        SecondaryIndexDefinition::node_range_desc("User", "age")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        SecondaryIndexDefinition::edge_range("FOLLOWS", "rank")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "created_at")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        VectorIndexDefinition::new_edge("RELATED", "embedding", 3, VectorDistanceMetric::Euclidean)
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        VectorIndexDefinition::new_node(
            "Location",
            "coordinates",
            5,
            VectorDistanceMetric::Manhattan,
        )
        .expect("valid definition")
        .with_m(8)
        .expect("valid connection count")
        .with_m0(16)
        .expect("valid layer-zero connection count")
        .with_ef_construction(64)
        .expect("valid construction beam")
        .with_ml(0.5)
        .expect("valid layer multiplier")
        .with_simhash_threshold(9)
        .expect("valid SimHash threshold")
        .with_sampling_ratio(0.5)
        .expect("valid sampling ratio")
        .with_adaptive_enabled(false)
        .with_adaptive_failure_prob(0.2)
        .expect("valid adaptive failure probability")
        .try_into()
        .expect("valid V2 definition"),
        TextIndexDefinition::new_node("Document", "body")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
        TextIndexDefinition::new_edge("RELATED", "notes")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
    ]
}

async fn seed_legacy_rows(
    database: &str,
    store: Arc<dyn ObjectStore>,
    definitions: &[ValidatedDynamicIndexDefinition],
    tombstone: Option<&ValidatedDynamicIndexDefinition>,
) {
    let raw = raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("legacy seed transaction opens");
    transaction
        .put(
            scoped_metadata_key(DataScope::LegacyUnscoped, b"parity-passthrough"),
            Bytes::from_static(b"preserve-me"),
        )
        .expect("passthrough row stages");
    for definition in definitions {
        let (key, value) = migration_parity_legacy_catalog_row(definition, false)
            .expect("legacy definition encodes");
        transaction.put(key, value).expect("legacy row stages");
        let ValidatedDynamicIndexDefinition::Vector(vector) = definition else {
            continue;
        };
        if vector.tenant_property().is_some() {
            continue;
        }
        let runtime = vector.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_id = crate::search::vector::index_id_from_name(&physical_name);
        let metadata = crate::search::vector::VectorIndexMetadata::new(
            crate::search::vector::VectorIndexConfig::from_v2_definition(vector, &physical_name),
        );
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(
                        VectorKey::IndexMetadata(VectorIndexMetadataKey::new(physical_id)),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("legacy vector metadata stages");
    }
    if let Some(definition) = tombstone {
        let (key, value) = migration_parity_legacy_catalog_row(definition, true)
            .expect("legacy tombstone encodes");
        transaction
            .put(key, value)
            .expect("legacy tombstone stages");
    }
    transaction.commit().await.expect("legacy rows commit");
    raw.close().await.expect("legacy seed database closes");
}

async fn assert_legacy_catalog_empty(db: &HelixDB) {
    assert!(
        load_legacy_definition_rows(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
            .await
            .expect("legacy catalog scans")
            .is_empty(),
        "legacy catalog must be retired only after V2 convergence"
    );
    assert!(
        index_v2_migration_ready(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
            .await
            .expect("readiness marker reads")
    );
    assert!(
        storage_schema_complete(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
            .await
            .expect("storage schema completion marker reads")
    );
}

async fn assert_reader_migration_gate(
    database: &str,
    store: Arc<dyn ObjectStore>,
    expected_ready: bool,
) {
    let reader = slatedb::DbReader::builder(database.to_string(), store)
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("migration-gate reader storage opens");
    let result =
        crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&reader).await;
    assert_eq!(
        result.is_ok(),
        expected_ready,
        "reader readiness must match durable schema completion: {result:?}"
    );
    reader.close().await.expect("migration-gate reader closes");
}

async fn assert_failpoint_blocks_reader_until_recovery(failpoint: MigrationFailpoint) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database(&format!("reader-gate-{}", failpoint.as_str()));
    inject_migration_failpoint_once(failpoint).expect("reader-gate failpoint injects");
    assert!(
        HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        )
        .await
        .is_err(),
        "{} must interrupt writer migration",
        failpoint.as_str()
    );
    assert!(
        migration_failpoint_was_triggered(),
        "{} must trigger",
        failpoint.as_str()
    );
    let inspection = raw(&database, Arc::clone(&store)).await;
    let graph_ready = graph_format_v1_ready(&inspection, DataScope::LegacyUnscoped)
        .await
        .expect("graph readiness reads");
    let index_ready = index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
        .await
        .expect("index readiness reads");
    assert!(
        !storage_schema_complete(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("schema completion reads"),
        "failed writer open must not publish full schema completion"
    );
    #[allow(clippy::wildcard_enum_match_arm)]
    match failpoint {
        MigrationFailpoint::BatchCommitAfter => {
            assert!(!graph_ready, "partial graph rewrite must remain unready");
            assert!(
                !index_ready,
                "index migration cannot precede graph readiness"
            );
        }
        MigrationFailpoint::MigrationReadyPublicationAfter
        | MigrationFailpoint::StorageSchemaCompletionBefore => {
            assert!(graph_ready, "graph rewrite must already be ready");
            assert!(index_ready, "index readiness must already be durable");
        }
        _ => panic!("unsupported reader-gate failpoint"),
    }
    inspection
        .close()
        .await
        .expect("reader-gate inspection closes");
    assert_reader_migration_gate(&database, Arc::clone(&store), false).await;

    let recovered = HelixDB::open_with_object_store_for_migration_parity(
        database.clone(),
        Arc::clone(&store),
        one_row_config(),
    )
    .await
    .expect("writer recovery completes every schema stage");
    recovered.close().await.expect("recovered writer closes");
    assert_reader_migration_gate(&database, store, true).await;
}

async fn populate_legacy_vector<D: crate::search::vector::Distance>(
    raw: &Db,
    definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    entity_id: u64,
    vector: &[f32],
) {
    let runtime = definition.to_runtime();
    let physical_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    populate_named_legacy_vector::<D>(raw, definition, physical_name, entity_id, vector).await;
}

async fn populate_named_legacy_vector<D: crate::search::vector::Distance>(
    raw: &Db,
    definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    physical_name: String,
    entity_id: u64,
    vector: &[f32],
) {
    let physical_id = crate::search::vector::index_id_from_name(&physical_name);
    let metadata_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::IndexMetadata(
            VectorIndexMetadataKey::new(physical_id),
        )),
    }
    .to_bytes();
    let index = crate::search::vector::VectorIndex::<D>::for_legacy_migration(
        physical_name,
        DataScope::LegacyUnscoped,
    );
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("current vector metadata transaction opens");
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(
                &crate::encoding::v1::values::vectors::metadata::encode_metadata(
                    &crate::search::vector::VectorIndexMetadata::new(
                        crate::search::vector::VectorIndexConfig::from_v2_definition(
                            definition,
                            index.name(),
                        ),
                    ),
                ),
            ),
        )
        .expect("current vector metadata stages");
    transaction
        .commit()
        .await
        .expect("current vector metadata commits");
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("legacy vector population transaction opens");
    index
        .insert(&transaction, entity_id, vector)
        .await
        .expect("legacy vector inserts");
    transaction
        .commit()
        .await
        .expect("legacy vector population commits");
    let metadata = index
        .get_metadata(raw)
        .await
        .expect("populated metadata reads")
        .expect("populated metadata exists");
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("legacy metadata transcode transaction opens");
    transaction
        .put(
            metadata_key,
            Bytes::copy_from_slice(
                &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )
        .expect("legacy metadata stages");
    transaction.commit().await.expect("legacy metadata commits");
}

/// Writes the graph-authoritative portion retained by the pinned legacy
/// writer after it moved the indexed embedding into HNSW.
async fn seed_legacy_node_without_vector(raw: &Db, entity_id: u64, label: &str) {
    raw.put(
        Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                crate::encoding::v1::keys::NodePropertyKey::new(entity_id),
            ),
        }
        .to_bytes(),
        property::encode_properties(&[
            Property::string("$label", label),
            Property::string("title", "retained graph property"),
        ]),
    )
    .await
    .expect("legacy graph property row writes");
}

/// Writes the current edge rows retained after the legacy writer moved the
/// indexed embedding into HNSW.
async fn seed_legacy_edge_without_vector(raw: &Db, edge_id: u64, from: u64, to: u64, label: &str) {
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("legacy edge transaction opens");
    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
            }
            .to_bytes(),
            crate::encoding::v1::values::edge_endpoints::EdgeEndpointsValue::new(from, to).encode(),
        )
        .expect("legacy edge endpoints stage");
    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
            }
            .to_bytes(),
            property::encode_properties(&[
                Property::string("$label", label),
                Property::string("title", "retained edge property"),
            ]),
        )
        .expect("legacy edge properties stage");
    transaction.commit().await.expect("legacy edge rows commit");
}

/// Proves migration restored the omitted embedding without disturbing
/// other graph properties.
async fn assert_materialized_node_vector(raw: &Db, entity_id: u64, expected: &[f32]) {
    let key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
            crate::encoding::v1::keys::NodePropertyKey::new(entity_id),
        ),
    }
    .to_bytes();
    let properties = property::decode_properties(
        &raw.get(key)
            .await
            .expect("materialized graph property reads")
            .expect("materialized graph property exists"),
    )
    .expect("materialized graph properties decode");
    assert!(properties.contains(&Property::string("title", "retained graph property")));
    assert!(properties.contains(&Property::f32_array("embedding", expected.to_vec())));
}

/// Proves edge materialization restored the omitted embedding without
/// disturbing the current edge-property row.
async fn assert_materialized_edge_vector(raw: &Db, edge_id: u64, expected: &[f32]) {
    let key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
    }
    .to_bytes();
    let properties = property::decode_properties(
        &raw.get(key)
            .await
            .expect("materialized edge property reads")
            .expect("materialized edge property exists"),
    )
    .expect("materialized edge properties decode");
    assert!(properties.contains(&Property::string("title", "retained edge property")));
    assert!(properties.contains(&Property::f32_array("embedding", expected.to_vec())));
}

async fn non_metadata_vector_digest(
    read: &(impl slatedb::DbReadOps + Send + Sync),
    physical_id: u64,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    for lane in crate::encoding::v1::keys::vectors::VectorStorageLane::ALL {
        let mut rows = read
            .scan_prefix(
                Key::data_prefix(
                    DataScope::LegacyUnscoped,
                    lane.prefix_key(physical_id).to_bytes(),
                ),
                ..,
            )
            .await
            .expect("vector lane scans");
        while let Some(row) = rows.next().await.expect("vector lane row reads") {
            let logical = DataScope::LegacyUnscoped
                .strip_key(&row.key)
                .expect("vector row has the expected scope");
            if matches!(
                VectorKey::parse_from_slice(logical).expect("vector row key parses"),
                VectorKey::IndexMetadata(_) | VectorKey::SimHashDirectory(_)
            ) {
                continue;
            }
            digest.update(u64::try_from(row.key.len()).unwrap().to_be_bytes());
            digest.update(&row.key);
            digest.update(u64::try_from(row.value.len()).unwrap().to_be_bytes());
            digest.update(&row.value);
        }
    }
    digest.finalize().into()
}

async fn vector_namespace_rows(
    read: &(impl slatedb::DbReadOps + Send + Sync),
    physical_id: u64,
) -> Vec<(Bytes, Bytes)> {
    let mut result = Vec::new();
    for lane in crate::encoding::v1::keys::vectors::VectorStorageLane::ALL {
        let mut rows = read
            .scan_prefix(
                Key::data_prefix(
                    DataScope::LegacyUnscoped,
                    lane.prefix_key(physical_id).to_bytes(),
                ),
                ..,
            )
            .await
            .expect("vector namespace scans");
        while let Some(row) = rows.next().await.expect("vector namespace row reads") {
            result.push((row.key, row.value));
        }
    }
    result
}

async fn assert_vector_search<D: crate::search::vector::Distance>(
    db: &HelixDB,
    definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    query: &[f32],
    expected_entity_id: u64,
) {
    let active = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| {
            matches!(
                handle,
                crate::index_lifecycle::ActiveIndexHandle::Vector {
                    definition: active,
                    ..
                } if active.as_ref() == definition
            )
        })
        .expect("adopted vector is runtime-active");
    let crate::index_lifecycle::ActiveIndexHandle::Vector { layout, .. } = &active else {
        panic!("adopted definition projected another family")
    };
    let physical_id = layout
        .physical_index_id()
        .expect("adopted vector is unpartitioned");
    let generation = crate::search::vector::ValidatedVectorGenerationHandle::try_from_active::<D>(
        &active,
        physical_id,
    )
    .expect("active vector generation validates");
    let hits = crate::search::vector::VectorIndex::<D>::from_generation(&generation)
        .search(
            db.inner_db().as_ref(),
            query,
            &crate::search::vector::SearchParams::new(1).expect("one result is valid"),
        )
        .await
        .expect("adopted vector searches");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].entity_id(), expected_entity_id);
}

fn returned_ids(response: &serde_json::Value, name: &str) -> Vec<u64> {
    let values = response
        .get(name)
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("query result `{name}` is an array"));
    let mut ids = values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .unwrap_or_else(|| panic!("query result `{name}` contains a u64"))
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

async fn planner_vector_search_ids(db: &HelixDB, query: Vec<f32>) -> Vec<u64> {
    let request = QueryRequest::read(
        read_batch()
            .var_as(
                "ids",
                g().vector_search_nodes("Document", "embedding", query, 8, None)
                    .id(),
            )
            .returning(["ids"]),
    );
    returned_ids(
        &db.query(request)
            .await
            .expect("planner vector search succeeds"),
        "ids",
    )
}

async fn planner_tenant_vector_search_ids(db: &HelixDB, tenant: &str, query: Vec<f32>) -> Vec<u64> {
    let request = QueryRequest::read(
        read_batch()
            .var_as(
                "ids",
                g().vector_search_nodes(
                    "Document",
                    "embedding",
                    query,
                    8,
                    Some(AstPropertyValue::String(tenant.to_string())),
                )
                .id(),
            )
            .returning(["ids"]),
    );
    returned_ids(
        &db.query(request)
            .await
            .expect("planner tenant vector search succeeds"),
        "ids",
    )
}

/// Waits until this handle's queued index operations are physically
/// published.
///
/// Vector updates reach their generation through the asynchronous queue;
/// strong searches see them through the queue overlay before then. Checks
/// that read the physical generation or its cache wait here first instead
/// of assuming the publisher keeps pace with back-to-back requests.
async fn wait_for_index_queue_published(db: &HelixDB) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let stats = db.index_operation_queue_stats();
            if stats.pending_operations == 0 && stats.uncertain_operations == 0 {
                break;
            }
            db.wake_index_worker().await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("queued index operations publish");
}

async fn exercise_adopted_vector_runtime(
    db: &HelixDB,
    definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
    physical_id: VectorPhysicalIndexId,
) {
    let active = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| {
            matches!(
                handle,
                crate::index_lifecycle::ActiveIndexHandle::Vector {
                    definition: active,
                    ..
                } if active.as_ref() == definition
            )
        })
        .expect("adopted vector is runtime-active");
    let generation = crate::search::vector::ValidatedVectorGenerationHandle::try_from_active::<
        crate::search::vector::distance::Cosine,
    >(&active, physical_id)
    .expect("adopted generation validates for cache hydration");
    wait_for_index_queue_published(db).await;
    // A background warm pass may own this generation's initial hydration,
    // which an explicit refresh skips, and a commit to the generation during
    // that load discards it. Admission is eventual, so refresh until the
    // generation is resident rather than assuming one call admits it.
    let cache_guard = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            db.refresh_vector_memory_cache()
                .await
                .expect("adopted vector cache refreshes");
            let Ok(guard) = db.vector_cache_registry().resident_guard_for(&generation) else {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };
            break guard;
        }
    })
    .await
    .expect("adopted generation is admitted to the vector cache");
    assert!(cache_guard.store().estimated_bytes() > 0);
    drop(cache_guard);

    let create = QueryRequest::write(
        write_batch()
            .var_as(
                "created",
                g().add_n(
                    "Document",
                    vec![(
                        "embedding",
                        PropertyInput::from(AstPropertyValue::F32Array(vec![0.0, 1.0, 0.0])),
                    )],
                ),
            )
            .var_as("created_id", g().n(NodeRef::var("created")).id())
            .returning(["created_id"]),
    );
    let created = db
        .query(create)
        .await
        .expect("planner vector create succeeds");
    let created_ids = returned_ids(&created, "created_id");
    let [created_id] = created_ids.as_slice() else {
        panic!("planner vector create returns one node ID")
    };
    assert!(planner_vector_search_ids(db, vec![0.0, 1.0, 0.0])
        .await
        .contains(created_id));

    db.query(QueryRequest::write(write_batch().var_as(
        "updated",
        g().n(NodeRef::id(*created_id)).set_property(
            "embedding",
            PropertyInput::from(AstPropertyValue::F32Array(vec![0.0, 0.0, 1.0])),
        ),
    )))
    .await
    .expect("planner vector update succeeds");
    assert!(planner_vector_search_ids(db, vec![0.0, 0.0, 1.0])
        .await
        .contains(created_id));

    db.query(QueryRequest::write(
        write_batch().var_as("deleted", g().n(NodeRef::id(*created_id)).drop()),
    ))
    .await
    .expect("planner vector delete succeeds");
    assert!(!planner_vector_search_ids(db, vec![0.0, 0.0, 1.0])
        .await
        .contains(created_id));
}

/// Proves a populated physical namespace is adopted byte-for-byte, remains
/// searchable after cold reopen, and is later owned by ordinary DROP and
/// recreate lifecycle behavior.
pub(crate) async fn run_vector_adoption_contract() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database("migration-vector-adoption");
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid vector definition")
            .try_into()
            .expect("valid V2 vector definition");
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("vector runtime definition validates as vector")
    };
    seed_legacy_rows(
        &database,
        Arc::clone(&store),
        std::slice::from_ref(&definition),
        None,
    )
    .await;
    let legacy_db = raw(&database, Arc::clone(&store)).await;
    seed_legacy_node_without_vector(&legacy_db, 77, "Document").await;
    populate_legacy_vector::<crate::search::vector::distance::Cosine>(
        &legacy_db,
        vector_definition,
        77,
        &[1.0, 0.0, 0.0],
    )
    .await;
    let colliding_property_key = crate::encoding::v1::indexes::range::EdgeRangeIndexKey::new(
        crate::encoding::v1::indexes::EdgeDirection::Out,
        crate::encoding::v1::indexes::range::EdgeRangeIndexDirection::Asc,
        1,
        [1, 2, 3, 4],
        std::borrow::Cow::Borrowed("twenty-byte-value---"),
        9,
    );
    let colliding_property_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::PropertyIndex(
            crate::encoding::v1::indexes::IndexKey::EdgeRange(colliding_property_key),
        ),
    }
    .to_bytes();
    assert_eq!(colliding_property_key.len(), 43);
    let collision_transaction = legacy_db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("property collision transaction opens");
    collision_transaction
        .put(colliding_property_key, Bytes::new())
        .expect("property collision row stages");
    collision_transaction
        .commit()
        .await
        .expect("property collision row commits");
    let runtime = vector_definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    let physical_id = crate::search::vector::index_id_from_name(&legacy_name);
    let guard_transaction = legacy_db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("legacy transaction guard transaction opens");
    guard_transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::TxnGuard(
                    crate::encoding::v1::keys::vectors::VectorTxnGuardKey::new(physical_id),
                )),
            }
            .to_bytes(),
            crate::encoding::v1::values::vectors::markers::encode_active_txn_guard(),
        )
        .expect("legacy transaction guard stages");
    guard_transaction
        .commit()
        .await
        .expect("legacy transaction guard commits");
    let before_digest = non_metadata_vector_digest(&legacy_db, physical_id).await;
    let empty_digest: [u8; 32] = Sha256::digest([]).into();
    assert_ne!(before_digest, empty_digest);
    legacy_db.close().await.expect("populated source closes");

    let migrated = Box::pin(HelixDB::open_with_object_store_for_migration_parity(
        database.clone(),
        Arc::clone(&store),
        one_row_config(),
    ))
    .await
    .expect("populated vector adopts");
    assert_legacy_catalog_empty(&migrated).await;
    assert_materialized_node_vector(migrated.inner_db().as_ref(), 77, &[1.0, 0.0, 0.0]).await;
    let full = migrated
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "full",
                    g().n(NodeRef::id(77)).value_map(None::<Vec<String>>),
                )
                .returning(["full"]),
        ))
        .await
        .expect("full materialized property read succeeds");
    let full = full
        .get("full")
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(serde_json::Value::as_object)
        .expect("full materialized read returns one object");
    assert_eq!(
        full.get("title").and_then(serde_json::Value::as_str),
        Some("retained graph property")
    );
    assert_eq!(
        full.get("embedding")
            .and_then(serde_json::Value::as_array)
            .expect("full read includes the embedding")
            .iter()
            .map(|value| value.as_f64().expect("embedding component is numeric"))
            .collect::<Vec<_>>(),
        vec![1.0, 0.0, 0.0]
    );
    let projected = migrated
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "projected",
                    g().n(NodeRef::id(77)).values(vec!["embedding"]),
                )
                .returning(["projected"]),
        ))
        .await
        .expect("projected materialized property read succeeds");
    let projected = projected
        .get("projected")
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(serde_json::Value::as_object)
        .and_then(|row| row.get("embedding"))
        .and_then(serde_json::Value::as_array)
        .expect("projected read includes only the requested embedding");
    assert_eq!(
        projected
            .iter()
            .map(|value| value.as_f64().expect("embedding component is numeric"))
            .collect::<Vec<_>>(),
        vec![1.0, 0.0, 0.0]
    );
    let record = crate::index_lifecycle::repository::load_index_record(
        migrated.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("adopted record reads")
    .expect("adopted record exists");
    let crate::index_lifecycle::IndexStateV2::Active {
        physical:
            crate::index_lifecycle::PhysicalGeneration::Vector {
                generation,
                layout:
                    crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = record.state()
    else {
        panic!("adopted record is not one active unpartitioned vector")
    };
    assert_eq!(physical_index_id.get(), physical_id);
    assert_eq!(
        crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
            migrated.inner_db().as_ref(),
            *physical_index_id,
        )
        .await
        .expect("active reservation reads"),
        Some(
            crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                index_id: record.index_id(),
                generation: *generation,
            }
        )
    );
    assert_eq!(
        non_metadata_vector_digest(migrated.inner_db().as_ref(), physical_id).await,
        before_digest,
        "adoption must not rewrite any HNSW row"
    );
    let metadata_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::IndexMetadata(
            VectorIndexMetadataKey::new(physical_id),
        )),
    }
    .to_bytes();
    let metadata = crate::encoding::v1::values::vectors::metadata::decode_metadata(
        &migrated
            .inner_db()
            .get(metadata_key)
            .await
            .expect("metadata reads")
            .expect("metadata exists"),
    )
    .expect("adopted metadata uses the current codec");
    assert_eq!(
        metadata.config.index_name,
        format!(
            "v2-vector-{}-{}-{}",
            record.index_id().get(),
            generation.get(),
            physical_id
        )
    );
    assert_vector_search::<crate::search::vector::distance::Cosine>(
        &migrated,
        vector_definition,
        &[1.0, 0.0, 0.0],
        77,
    )
    .await;
    migrated
        .query(QueryRequest::write(write_batch().var_as(
            "moved",
            g().n(NodeRef::id(77)).set_property(
                "$label",
                PropertyInput::from(AstPropertyValue::String("Archived".to_string())),
            ),
        )))
        .await
        .expect("label move without an embedding commits");
    assert!(!planner_vector_search_ids(&migrated, vec![1.0, 0.0, 0.0])
        .await
        .contains(&77));
    migrated
        .query(QueryRequest::write(write_batch().var_as(
            "restored",
            g().n(NodeRef::id(77)).set_property(
                "$label",
                PropertyInput::from(AstPropertyValue::String("Document".to_string())),
            ),
        )))
        .await
        .expect("label restoration without an embedding commits");
    assert!(planner_vector_search_ids(&migrated, vec![1.0, 0.0, 0.0])
        .await
        .contains(&77));
    exercise_adopted_vector_runtime(&migrated, vector_definition, *physical_index_id).await;
    migrated.close().await.expect("adopted database closes");

    let reopened = Box::pin(HelixDB::open_with_object_store_for_migration_parity(
        database,
        store,
        one_row_config(),
    ))
    .await
    .expect("adopted database cold-reopens");
    // Operations still queued at close publish after the reopen.
    wait_for_index_queue_published(&reopened).await;
    assert_vector_search::<crate::search::vector::distance::Cosine>(
        &reopened,
        vector_definition,
        &[1.0, 0.0, 0.0],
        77,
    )
    .await;
    let receipt = crate::index_lifecycle::lifecycle::drop_index_operation(
        reopened.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &definition,
    )
    .await
    .expect("adopted vector drop enqueues");
    let operation_id = receipt.operation_id().expect("drop has an operation");
    reopened.wake_index_worker().await;
    wait_for_index_operation(&reopened, DataScope::LegacyUnscoped, operation_id, false)
        .await
        .expect("adopted vector drop completes");
    assert!(
        crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
            reopened.inner_db().as_ref(),
            *physical_index_id,
        )
        .await
        .expect("dropped reservation reads")
        .is_none()
    );
    assert_eq!(
        non_metadata_vector_digest(reopened.inner_db().as_ref(), physical_id).await,
        empty_digest,
        "ordinary DROP must delete the adopted physical namespace"
    );
    reopened
        .install_index_for_tests(definition.clone())
        .await
        .expect("dropped vector recreates");
    let recreated = crate::index_lifecycle::repository::load_index_record(
        reopened.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("recreated record reads")
    .expect("recreated record exists");
    let Some(crate::index_lifecycle::PhysicalGeneration::Vector {
        layout:
            crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                physical_index_id: recreated_physical_id,
            },
        ..
    }) = recreated.state().physical()
    else {
        panic!("recreated vector has one physical namespace")
    };
    assert_ne!(recreated_physical_id.get(), physical_id);
    assert_vector_search::<crate::search::vector::distance::Cosine>(
        &reopened,
        vector_definition,
        &[1.0, 0.0, 0.0],
        77,
    )
    .await;
    reopened.close().await.expect("recreated database closes");
}

/// Requires every adoption-checkpoint failure to return from writer-open,
/// preserve the adopted HNSW source, and cold-recover exact ownership.
pub(crate) async fn run_vector_adoption_failpoint_recovery_contracts() -> Vec<&'static str> {
    // SlateDB's default 100 ms flush, not `one_row_config`'s 1 ms: a fast
    // flush lets the recovery open race a resumed index operation into a
    // transaction conflict (HEL-996).
    let adoption_config = || one_row_config().with_slate_settings(slatedb::Settings::default());
    const BOUNDARIES: [MigrationFailpoint; 8] = [
        MigrationFailpoint::LegacyVectorValidationCheckpointBefore,
        MigrationFailpoint::LegacyVectorValidationCheckpointAfter,
        MigrationFailpoint::LegacyVectorMetadataPublicationBefore,
        MigrationFailpoint::LegacyVectorMetadataPublicationAfter,
        MigrationFailpoint::LegacyVectorReservationTransitionBefore,
        MigrationFailpoint::LegacyVectorReservationTransitionAfter,
        MigrationFailpoint::LegacyDefinitionRetirementBefore,
        MigrationFailpoint::LegacyDefinitionRetirementAfter,
    ];

    let mut recovered_boundaries = Vec::with_capacity(BOUNDARIES.len());
    for failpoint in BOUNDARIES {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(&format!("migration-vector-adoption-{}", failpoint.as_str()));
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("adoption recovery vector definition validates")
        .try_into()
        .expect("adoption recovery vector definition converts");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("adoption recovery definition remains vector")
        };
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let source = raw(&database, Arc::clone(&store)).await;
        seed_legacy_node_without_vector(&source, 77, "Document").await;
        populate_legacy_vector::<crate::search::vector::distance::Cosine>(
            &source,
            vector_definition,
            77,
            &[1.0, 0.0, 0.0],
        )
        .await;
        let runtime = vector_definition.to_runtime();
        let legacy_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_id = crate::search::vector::index_id_from_name(&legacy_name);
        let source_digest = non_metadata_vector_digest(&source, physical_id).await;
        source
            .close()
            .await
            .expect("adoption recovery source closes");

        inject_migration_failpoint_once(failpoint).expect("adoption recovery failpoint injects");
        let interrupted = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HelixDB::open_with_object_store_for_migration_parity(
                database.clone(),
                Arc::clone(&store),
                adoption_config(),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} adoption recovery writer-open exceeded five seconds",
                failpoint.as_str()
            )
        });
        let Err(error) = interrupted else {
            panic!("{} must interrupt adoption", failpoint.as_str());
        };
        assert!(
            matches!(error, HelixDbError::MigrationRequired { .. }),
            "{} returned the wrong typed failure: {error}",
            failpoint.as_str()
        );
        assert!(
            migration_failpoint_was_triggered(),
            "{} must trigger during adoption",
            failpoint.as_str()
        );
        let inspection = raw(&database, Arc::clone(&store)).await;
        assert_eq!(
            non_metadata_vector_digest(&inspection, physical_id).await,
            source_digest,
            "{} must preserve every non-metadata HNSW row",
            failpoint.as_str()
        );
        assert_eq!(
            load_legacy_definition_rows(&inspection, DataScope::LegacyUnscoped)
                .await
                .expect("adoption recovery legacy catalog scans")
                .len(),
            1,
            "{} must retain its legacy definition until activation",
            failpoint.as_str()
        );
        assert!(
            !index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
                .await
                .expect("adoption recovery readiness reads"),
            "{} must not publish readiness",
            failpoint.as_str()
        );
        let current_before = crate::index_lifecycle::repository::load_index_record(
            &inspection,
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("adoption recovery current record reads")
        .expect("adoption recovery current record exists");
        let IndexStateV2::Building {
            build_operation_id, ..
        } = current_before.state()
        else {
            panic!("failed adoption remains Building");
        };
        let failed_operation = crate::index_lifecycle::outbox::read_operation(
            &inspection,
            DataScope::LegacyUnscoped,
            *build_operation_id,
        )
        .await
        .expect("adoption recovery failed operation reads")
        .expect("adoption recovery failed operation exists");
        assert!(
            matches!(
                failed_operation.queue_schedule(),
                Some(
                    crate::index_lifecycle::IndexOperationQueueSchedule::DelayedAfterTransientFailure {
                        ..
                    }
                )
            ),
            "{} did not retain its typed transient retry",
            failpoint.as_str()
        );
        let ownership_before = (
            current_before.index_id(),
            current_before.state().generation(),
        );
        inspection
            .close()
            .await
            .expect("adoption recovery inspection closes");

        let recovered = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HelixDB::open_with_object_store_for_migration_parity(
                database,
                store,
                adoption_config(),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{} adoption recovery cold open exceeded five seconds",
                failpoint.as_str()
            )
        })
        .expect("adoption recovery cold open converges");
        let record = crate::index_lifecycle::repository::load_index_record(
            recovered.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("adoption recovery final record reads")
        .expect("adoption recovery final record exists");
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    generation,
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = record.state()
        else {
            panic!("adoption recovery final record is active and unpartitioned")
        };
        assert_eq!(physical_index_id.get(), physical_id);
        assert_eq!(
            ownership_before,
            (record.index_id(), *generation),
            "{} changed allocated vector ownership",
            failpoint.as_str(),
        );
        assert_eq!(
            non_metadata_vector_digest(recovered.inner_db().as_ref(), physical_id).await,
            source_digest,
            "{} rebuilt non-metadata HNSW rows",
            failpoint.as_str()
        );
        assert_legacy_catalog_empty(&recovered).await;
        assert_materialized_node_vector(recovered.inner_db().as_ref(), 77, &[1.0, 0.0, 0.0]).await;
        assert_vector_search::<crate::search::vector::distance::Cosine>(
            &recovered,
            vector_definition,
            &[1.0, 0.0, 0.0],
            77,
        )
        .await;
        recovered
            .close()
            .await
            .expect("adoption recovery writer closes");
        recovered_boundaries.push(failpoint.as_str());
    }
    recovered_boundaries
}

/// Proves malformed rows in each physical lane block ownership transfer
/// while preserving the exact source catalog and vector namespace.
pub(crate) async fn run_vector_corruption_contracts() {
    enum CorruptionLane {
        Core,
        Hot,
        Layer0,
    }

    for (name, lane) in [
        ("core", CorruptionLane::Core),
        ("hot", CorruptionLane::Hot),
        ("layer-zero", CorruptionLane::Layer0),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(&format!("migration-vector-corrupt-{name}"));
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("valid vector definition")
        .try_into()
        .expect("valid V2 vector definition");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector runtime definition validates as vector")
        };
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let source_db = raw(&database, Arc::clone(&store)).await;
        populate_legacy_vector::<crate::search::vector::distance::Cosine>(
            &source_db,
            vector_definition,
            77,
            &[1.0, 0.0, 0.0],
        )
        .await;
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_id = crate::search::vector::index_id_from_name(&physical_name);
        let corrupt_key = match lane {
            CorruptionLane::Core => Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::IndexMetadata(
                    VectorIndexMetadataKey::new(physical_id),
                )),
            }
            .to_bytes(),
            CorruptionLane::Hot => Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::SimHash(
                    crate::encoding::v1::keys::vectors::VectorSimHashKey::new(physical_id, 77),
                )),
            }
            .to_bytes(),
            CorruptionLane::Layer0 => {
                let simhash_key = Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::SimHash(
                        crate::encoding::v1::keys::vectors::VectorSimHashKey::new(physical_id, 77),
                    )),
                }
                .to_bytes();
                let simhash = crate::encoding::v1::values::vectors::simhash::decode_simhash(
                    &source_db
                        .get(simhash_key)
                        .await
                        .expect("source SimHash reads")
                        .expect("source SimHash exists"),
                )
                .expect("source SimHash decodes");
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::Vector(
                        crate::encoding::v1::keys::vectors::VectorItemKey::new(
                            physical_id,
                            crate::search::vector::simhash::order_code_from_simhash_bits(simhash),
                            77,
                        ),
                    )),
                }
                .to_bytes()
            }
        };
        let transaction = source_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("corruption transaction opens");
        transaction
            .put(corrupt_key, Bytes::from_static(b"malformed"))
            .expect("corrupt row stages");
        transaction.commit().await.expect("corrupt row commits");
        let source_rows = vector_namespace_rows(&source_db, physical_id).await;
        source_db.close().await.expect("corrupt source closes");

        assert!(
            HelixDB::open_with_object_store_for_migration_parity(
                database.clone(),
                Arc::clone(&store),
                one_row_config(),
            )
            .await
            .is_err(),
            "malformed {name} lane must fail writer migration"
        );
        let inspection = raw(&database, Arc::clone(&store)).await;
        assert_eq!(
            load_legacy_definition_rows(&inspection, DataScope::LegacyUnscoped)
                .await
                .expect("legacy catalog scans")
                .len(),
            1,
            "malformed {name} lane must retain its legacy catalog row"
        );
        assert_eq!(
            vector_namespace_rows(&inspection, physical_id).await,
            source_rows,
            "malformed {name} lane must retain every physical byte"
        );
        inspection.close().await.expect("corrupt inspection closes");
    }
}

/// Proves previously consumed and tenant-partitioned legacy definitions use
/// the existing reconstruction path instead of in-place adoption.
pub(crate) async fn run_vector_ineligible_contracts() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database_name = database("migration-vector-consumed-id");
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid vector definition")
            .try_into()
            .expect("valid V2 vector definition");
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("vector runtime definition validates as vector")
    };
    seed_legacy_rows(
        &database_name,
        Arc::clone(&store),
        std::slice::from_ref(&definition),
        None,
    )
    .await;
    let raw = raw(&database_name, Arc::clone(&store)).await;
    seed_legacy_node_without_vector(&raw, 77, "Document").await;
    populate_legacy_vector::<crate::search::vector::distance::Cosine>(
        &raw,
        vector_definition,
        77,
        &[1.0, 0.0, 0.0],
    )
    .await;
    let runtime = vector_definition.to_runtime();
    let legacy_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    let legacy_physical_id = crate::search::vector::index_id_from_name(&legacy_name);
    let consumed_next = VectorPhysicalIndexId::new(
        legacy_physical_id
            .checked_add(1)
            .expect("fixture hash leaves room for a later physical ID"),
    )
    .expect("later fixture physical ID is nonzero");
    seed_bootstrap_tuple(
        &raw,
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        )),
        true,
        true,
    )
    .await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("consumed watermark transaction opens");
    transaction
        .put(
            global(GlobalKey::VectorPhysicalIdWatermark),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: consumed_next,
                },
            )),
        )
        .expect("consumed watermark stages");
    transaction
        .commit()
        .await
        .expect("consumed watermark commits");
    assert!(!vector_namespace_rows(&raw, legacy_physical_id)
        .await
        .is_empty());
    raw.close().await.expect("consumed source closes");

    let rebuilt = HelixDB::open_with_object_store_for_migration_parity(
        database_name,
        store,
        one_row_config(),
    )
    .await
    .expect("consumed legacy namespace rebuilds");
    assert_legacy_catalog_empty(&rebuilt).await;
    let record = crate::index_lifecycle::repository::load_index_record(
        rebuilt.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("rebuilt record reads")
    .expect("rebuilt record exists");
    let Some(crate::index_lifecycle::PhysicalGeneration::Vector {
        layout:
            crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                physical_index_id: rebuilt_physical_id,
            },
        ..
    }) = record.state().physical()
    else {
        panic!("rebuilt vector remains unpartitioned")
    };
    assert_ne!(rebuilt_physical_id.get(), legacy_physical_id);
    assert_materialized_node_vector(rebuilt.inner_db().as_ref(), 77, &[1.0, 0.0, 0.0]).await;
    assert_vector_search::<crate::search::vector::distance::Cosine>(
        &rebuilt,
        vector_definition,
        &[1.0, 0.0, 0.0],
        77,
    )
    .await;
    assert!(
        vector_namespace_rows(rebuilt.inner_db().as_ref(), legacy_physical_id)
            .await
            .is_empty(),
        "normal rebuild retires its legacy physical namespace"
    );
    assert!(
        crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
            rebuilt.inner_db().as_ref(),
            VectorPhysicalIndexId::new(legacy_physical_id)
                .expect("legacy fixture physical ID is nonzero"),
        )
        .await
        .expect("source reservation reads")
        .is_none(),
        "normal rebuild releases its source reservation"
    );
    rebuilt.close().await.expect("rebuilt database closes");

    let tenant_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tenant_database = database("migration-vector-tenant-partitioned");
    let tenant_definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid vector definition")
            .with_tenant_property("tenant")
            .expect("valid tenant property")
            .try_into()
            .expect("valid V2 vector definition");
    seed_legacy_rows(
        &tenant_database,
        Arc::clone(&tenant_store),
        std::slice::from_ref(&tenant_definition),
        None,
    )
    .await;
    let ValidatedDynamicIndexDefinition::Vector(tenant_vector_definition) = &tenant_definition
    else {
        unreachable!("tenant vector definition validates as vector")
    };
    let tenant_source = self::raw(&tenant_database, Arc::clone(&tenant_store)).await;
    let tenant_entity_id = 91;
    let source_tenant = Property::string("tenant", "alpha");
    tenant_source
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                    crate::encoding::v1::keys::NodePropertyKey::new(tenant_entity_id),
                ),
            }
            .to_bytes(),
            property::encode_properties(&[
                Property::string("$label", "Document"),
                Property::string("title", "retained graph property"),
                source_tenant.clone(),
            ]),
        )
        .await
        .expect("tenant legacy graph row writes");
    let tenant_runtime = tenant_vector_definition.to_runtime();
    let tenant_physical_name = crate::search::vector_tenant_index_name(
        tenant_runtime.element_type(),
        tenant_runtime.label(),
        tenant_runtime.property(),
        tenant_runtime
            .tenant_property()
            .expect("fixture is tenant partitioned"),
        &source_tenant.value,
    );
    populate_named_legacy_vector::<crate::search::vector::distance::Cosine>(
        &tenant_source,
        tenant_vector_definition,
        tenant_physical_name,
        tenant_entity_id,
        &[0.0, 1.0, 0.0],
    )
    .await;
    tenant_source
        .close()
        .await
        .expect("tenant legacy source closes");
    let tenant = HelixDB::open_with_object_store_for_migration_parity(
        tenant_database,
        tenant_store,
        one_row_config(),
    )
    .await
    .expect("tenant-partitioned legacy definition rebuilds");
    let tenant_record = crate::index_lifecycle::repository::load_index_record(
        tenant.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &tenant_definition.identity(),
    )
    .await
    .expect("tenant record reads")
    .expect("tenant record exists");
    assert!(matches!(
        tenant_record.state().physical(),
        Some(crate::index_lifecycle::PhysicalGeneration::Vector {
            layout: crate::index_lifecycle::VectorPhysicalLayout::Partitioned,
            ..
        })
    ));
    assert_legacy_catalog_empty(&tenant).await;
    assert_materialized_node_vector(
        tenant.inner_db().as_ref(),
        tenant_entity_id,
        &[0.0, 1.0, 0.0],
    )
    .await;
    assert!(
        planner_tenant_vector_search_ids(&tenant, "alpha", vec![0.0, 1.0, 0.0])
            .await
            .contains(&tenant_entity_id)
    );
    tenant
        .query(QueryRequest::write(write_batch().var_as(
            "moved",
            g().n(NodeRef::id(tenant_entity_id)).set_property(
                "tenant",
                PropertyInput::from(AstPropertyValue::String("beta".to_string())),
            ),
        )))
        .await
        .expect("tenant move without an embedding commits");
    assert!(
        !planner_tenant_vector_search_ids(&tenant, "alpha", vec![0.0, 1.0, 0.0])
            .await
            .contains(&tenant_entity_id)
    );
    assert!(
        planner_tenant_vector_search_ids(&tenant, "beta", vec![0.0, 1.0, 0.0])
            .await
            .contains(&tenant_entity_id)
    );
    tenant.close().await.expect("tenant database closes");
}

/// Proves physical-name and reservation ownership inconsistencies fail
/// before changing the persisted legacy catalog or vector namespace.
pub(crate) async fn run_vector_ownership_conflict_contracts() {
    enum Conflict {
        PhysicalName,
        MalformedReservation,
        ReservationWithoutOwner,
        ExistingV2Owner,
    }

    for (name, conflict) in [
        ("physical-name", Conflict::PhysicalName),
        ("malformed-reservation", Conflict::MalformedReservation),
        (
            "reservation-without-owner",
            Conflict::ReservationWithoutOwner,
        ),
        ("existing-v2-owner", Conflict::ExistingV2Owner),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(&format!("migration-vector-conflict-{name}"));
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("valid vector definition")
        .try_into()
        .expect("valid V2 vector definition");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector runtime definition validates as vector")
        };
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let source_db = raw(&database, Arc::clone(&store)).await;
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let raw_physical_id = crate::search::vector::index_id_from_name(&physical_name);
        let physical_id = VectorPhysicalIndexId::new(raw_physical_id)
            .expect("legacy fixture physical ID is nonzero");
        if !matches!(conflict, Conflict::PhysicalName) {
            seed_bootstrap_tuple(
                &source_db,
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::CURRENT,
                )),
                true,
                true,
            )
            .await;
        }
        let transaction = source_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("ownership conflict transaction opens");
        match conflict {
            Conflict::PhysicalName => {
                let metadata = crate::search::vector::VectorIndexMetadata::new(
                    crate::search::vector::VectorIndexConfig::from_v2_definition(
                        vector_definition,
                        "another-physical-name",
                    ),
                );
                transaction
                    .put(
                        Key::Data {
                            scope: DataScope::LegacyUnscoped,
                            kind: crate::encoding::v1::keys::DataKeyKind::Vector(
                                VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                                    raw_physical_id,
                                )),
                            ),
                        }
                        .to_bytes(),
                        Bytes::copy_from_slice(
                            &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                                &metadata,
                            ),
                        ),
                    )
                    .expect("mismatched metadata stages");
            }
            Conflict::MalformedReservation => {
                transaction
                    .put(
                        global(GlobalKey::LegacyVectorPhysicalReservation(physical_id)),
                        Bytes::from_static(b"malformed"),
                    )
                    .expect("malformed reservation stages");
            }
            Conflict::ReservationWithoutOwner => {
                transaction
                    .put(
                        global(GlobalKey::LegacyVectorPhysicalReservation(
                            physical_id,
                        )),
                        encode_metadata_value(
                            &IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                                crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                                    index_id: IndexId::initial(),
                                    generation: crate::index_lifecycle::IndexGenerationId::initial(),
                                },
                            ),
                        ),
                    )
                    .expect("inconsistent reservation stages");
            }
            Conflict::ExistingV2Owner => {
                let owner_definition: ValidatedDynamicIndexDefinition =
                    VectorIndexDefinition::new_node(
                        "ExistingOwner",
                        "embedding",
                        3,
                        VectorDistanceMetric::Cosine,
                    )
                    .expect("valid existing owner definition")
                    .try_into()
                    .expect("valid existing V2 owner definition");
                let ValidatedDynamicIndexDefinition::Vector(owner_vector) = &owner_definition
                else {
                    unreachable!("existing V2 owner validates as vector")
                };
                let owner_descriptor =
                    crate::index_lifecycle::VectorGenerationDescriptor::for_definition(
                        owner_vector,
                    );
                let record = crate::index_lifecycle::IndexRecordV2::building(
                    IndexId::initial(),
                    owner_definition,
                    crate::index_lifecycle::IndexRevision::initial(),
                    crate::index_lifecycle::PhysicalGeneration::Vector {
                        generation: crate::index_lifecycle::IndexGenerationId::initial(),
                        layout: crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                            physical_index_id: physical_id,
                        },
                        descriptor: owner_descriptor,
                    },
                    crate::index_lifecycle::IndexOperationId::new_v4(),
                )
                .expect("existing owner record builds")
                .transition(crate::index_lifecycle::IndexStateTransition::Activate)
                .expect("existing owner record activates");
                transaction
                    .put(
                        IndexKey::Data {
                            scope: DataScope::LegacyUnscoped,
                            kind: ScopedKey::index_record(record.identity().clone()),
                        }
                        .to_bytes(),
                        encode_index_record(&record),
                    )
                    .expect("existing V2 owner stages");
                transaction
                    .put(
                        global(GlobalKey::LogicalIndexIdWatermark),
                        encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                            LogicalIndexIdWatermark {
                                next_id: IndexId::new(2).expect("second logical ID is valid"),
                            },
                        )),
                    )
                    .expect("advanced logical watermark stages");
            }
        }
        transaction
            .commit()
            .await
            .expect("ownership conflict commits");
        let source_rows = vector_namespace_rows(&source_db, raw_physical_id).await;
        let (catalog_key, catalog_value) = migration_parity_legacy_catalog_row(&definition, false)
            .expect("legacy catalog row encodes");
        source_db.close().await.expect("conflict source closes");

        assert!(
            HelixDB::open_with_object_store_for_migration_parity(
                database.clone(),
                Arc::clone(&store),
                one_row_config(),
            )
            .await
            .is_err(),
            "{name} conflict must fail writer migration"
        );
        let inspection = raw(&database, store).await;
        assert_eq!(
            inspection
                .get(catalog_key)
                .await
                .expect("legacy catalog reads"),
            Some(catalog_value),
            "{name} conflict must retain its exact legacy catalog row"
        );
        assert_eq!(
            vector_namespace_rows(&inspection, raw_physical_id).await,
            source_rows,
            "{name} conflict must retain every physical byte"
        );
        inspection
            .close()
            .await
            .expect("conflict inspection closes");
    }
}

/// Covers empty/non-empty bootstrap, reader gating, every legacy definition
/// shape, one-row checkpoints, durable reopen, and idempotent convergence.
pub(crate) async fn run_migration_contracts() {
    let pristine_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let pristine_database = database("reader-gate-pristine");
    let pristine = raw(&pristine_database, Arc::clone(&pristine_store)).await;
    pristine
        .close()
        .await
        .expect("pristine legacy database closes");
    assert_reader_migration_gate(&pristine_database, pristine_store, true).await;

    let tuple_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let tuple_database = database("reader-gate-tuple-only");
    let tuple = raw(&tuple_database, Arc::clone(&tuple_store)).await;
    crate::migrations::startup::bootstrap_writer(&tuple)
        .await
        .expect("tuple-only writer bootstrap commits");
    assert_current_storage_version(&tuple).await;
    tuple.close().await.expect("tuple-only database closes");
    assert_reader_migration_gate(&tuple_database, Arc::clone(&tuple_store), false).await;
    let tuple_recovered = HelixDB::open_with_object_store_for_migration_parity(
        tuple_database.clone(),
        Arc::clone(&tuple_store),
        one_row_config(),
    )
    .await
    .expect("tuple-only store completes migration");
    tuple_recovered
        .close()
        .await
        .expect("tuple-only recovered writer closes");
    assert_reader_migration_gate(&tuple_database, tuple_store, true).await;

    for (name, graph, index, schema) in [
        (
            "malformed-graph",
            b"bad".as_slice(),
            b"1".as_slice(),
            b"1".as_slice(),
        ),
        (
            "malformed-index",
            b"1".as_slice(),
            b"bad".as_slice(),
            b"1".as_slice(),
        ),
        (
            "malformed-schema",
            b"1".as_slice(),
            b"1".as_slice(),
            b"bad".as_slice(),
        ),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(&format!("reader-gate-{name}"));
        let fixture = raw(&database, Arc::clone(&store)).await;
        crate::migrations::startup::bootstrap_writer(&fixture)
            .await
            .expect("malformed-readiness bootstrap commits");
        fixture
            .put(
                scoped_metadata_key(DataScope::LegacyUnscoped, GRAPH_FORMAT_V1_READY),
                Bytes::copy_from_slice(graph),
            )
            .await
            .expect("graph readiness fixture writes");
        fixture
            .put(
                scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_V2_MIGRATION_READY),
                Bytes::copy_from_slice(index),
            )
            .await
            .expect("index readiness fixture writes");
        fixture
            .put(
                scoped_metadata_key(DataScope::LegacyUnscoped, STORAGE_SCHEMA_COMPLETE),
                Bytes::copy_from_slice(schema),
            )
            .await
            .expect("schema readiness fixture writes");
        fixture
            .close()
            .await
            .expect("malformed-readiness fixture closes");
        assert_reader_migration_gate(&database, store, false).await;
    }

    assert_failpoint_blocks_reader_until_recovery(MigrationFailpoint::BatchCommitAfter).await;
    assert_failpoint_blocks_reader_until_recovery(
        MigrationFailpoint::MigrationReadyPublicationAfter,
    )
    .await;
    assert_failpoint_blocks_reader_until_recovery(
        MigrationFailpoint::StorageSchemaCompletionBefore,
    )
    .await;

    let empty_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let empty_database = database("migration-empty");
    let empty = HelixDB::open_with_object_store_for_migration_parity(
        empty_database,
        Arc::clone(&empty_store),
        one_row_config(),
    )
    .await
    .expect("empty legacy store bootstraps");
    assert_legacy_catalog_empty(&empty).await;
    empty.close().await.expect("empty database closes");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database("migration-definitions");
    let definitions = definitions();
    let tombstone: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("Deleted", "value")
            .expect("valid tombstone definition")
            .try_into()
            .expect("valid V2 tombstone definition");
    seed_legacy_rows(
        &database,
        Arc::clone(&store),
        &definitions,
        Some(&tombstone),
    )
    .await;

    let reader = slatedb::DbReader::builder(database.clone(), Arc::clone(&store))
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("pre-migration reader storage opens");
    crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&reader)
        .await
        .expect("pristine legacy rows remain readable before writer bootstrap");
    reader.close().await.expect("pre-migration reader closes");

    let migrated = HelixDB::open_with_object_store_for_migration_parity(
        database.clone(),
        Arc::clone(&store),
        one_row_config(),
    )
    .await
    .expect("all persisted legacy definitions migrate");
    assert_current_storage_version(migrated.inner_db().as_ref()).await;
    assert_legacy_catalog_empty(&migrated).await;
    crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(
        migrated.inner_db().as_ref(),
    )
    .await
    .expect("completed storage schema opens for readers");
    assert_eq!(
        migrated
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .len(),
        definitions.len(),
        "every non-tombstoned legacy definition must be Active"
    );
    for definition in &definitions {
        let ValidatedDynamicIndexDefinition::Vector(vector) = definition else {
            continue;
        };
        let runtime = vector.to_runtime();
        let legacy_physical_id =
            crate::search::vector::index_id_from_name(&crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            ));
        let record = crate::index_lifecycle::repository::load_index_record(
            migrated.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("migrated vector record reads")
        .expect("migrated vector record exists");
        let crate::index_lifecycle::IndexStateV2::Active {
            physical:
                crate::index_lifecycle::PhysicalGeneration::Vector {
                    generation,
                    layout:
                        crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                            physical_index_id,
                        },
                    ..
                },
            ..
        } = record.state()
        else {
            panic!("eligible empty legacy vector adopts one active namespace")
        };
        assert_eq!(physical_index_id.get(), legacy_physical_id);
        assert_eq!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                migrated.inner_db().as_ref(),
                *physical_index_id,
            )
            .await
            .expect("empty adopted vector reservation reads"),
            Some(
                crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                    index_id: record.index_id(),
                    generation: *generation,
                }
            )
        );
    }
    migrated.close().await.expect("migrated database closes");

    let reopened =
        HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config())
            .await
            .expect("converged migration cold-reopens");
    assert_legacy_catalog_empty(&reopened).await;
    assert_eq!(
        reopened
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .len(),
        definitions.len(),
        "repeated writer open must retain the exact Active projection"
    );
    reopened.close().await.expect("reopened database closes");
}

/// A disabled secondary worker must never leave writer-open waiting on work
/// that no production surface can advance.
pub(crate) async fn run_disabled_secondary_worker_open_contract() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database("migration-disabled-secondary-worker");
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("valid secondary definition")
            .try_into()
            .expect("valid V2 definition");
    seed_legacy_rows(
        &database,
        Arc::clone(&store),
        std::slice::from_ref(&definition),
        None,
    )
    .await;
    let prerequisite = raw(&database, Arc::clone(&store)).await;
    mark_graph_rewrite_complete(&prerequisite).await;
    mark_vector_materialization_complete(&prerequisite).await;
    prerequisite
        .close()
        .await
        .expect("migration prerequisite fixture closes");
    let config = one_row_config().with_secondary_index_lifecycle_tuning(
        SecondaryIndexLifecycleTuning::default()
            .with_worker_mode(crate::config::SecondaryIndexLifecycleWorkerMode::Disabled),
    );

    let opened = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        HelixDB::open_with_object_store_for_migration_parity(database, store, config),
    )
    .await;

    assert!(
        opened.is_ok(),
        "writer open must return success or a typed error when legacy secondary definitions exist and the lifecycle worker is disabled"
    );
    if let Ok(Ok(db)) = opened {
        db.close().await.expect("completed writer closes");
    }
}

/// Proves explicit migration stepping has exclusive controller ownership.
pub(crate) async fn run_migration_worker_mode_stepping_contract() {
    let scope = DataScope::LegacyUnscoped;

    let background = HelixDB::open_with_object_store_for_migration_parity(
        database("migration-background-step-rejection"),
        Arc::new(InMemory::new()),
        one_row_config(),
    )
    .await
    .expect("Background-mode writer opens");
    let worker = background
        .inner
        .migration_worker
        .lock()
        .await
        .take()
        .expect("Background mode owns an automatic migration worker");
    worker.stop().await;
    let job = MigrationJob::new(MigrationId::GraphFormatV1Cleanup, MigrationMode::Background);
    put_migration_job(background.inner_db().as_ref(), job).await;
    let key = MigrationJobKey::new(scope, MigrationId::GraphFormatV1Cleanup);
    let before = background
        .inner_db()
        .get(key.as_ref())
        .await
        .expect("Background-mode job reads")
        .expect("Background-mode job exists");

    assert!(matches!(
        background.process_migration_once().await,
        Err(HelixDbError::MigrationSteppingRequiresDisabledMode)
    ));
    assert_eq!(
        background
            .inner_db()
            .get(key.as_ref())
            .await
            .expect("rejected Background-mode job reads")
            .expect("rejected Background-mode job remains"),
        before,
        "rejection must happen before the migration controller mutates its job"
    );
    background
        .close()
        .await
        .expect("Background-mode writer closes");

    let disabled_config = one_row_config().with_migration_tuning(
        MigrationTuning::default()
            .with_batch_rows(MigrationBatchRows::new(1).expect("one row is positive"))
            .with_worker_mode(MigrationWorkerMode::Disabled),
    );
    let disabled = HelixDB::open_with_object_store_for_migration_parity(
        database("migration-disabled-manual-step"),
        Arc::new(InMemory::new()),
        disabled_config,
    )
    .await
    .expect("Disabled-mode writer opens");
    assert!(!migration_completed(
        disabled.inner_db().as_ref(),
        scope,
        MigrationId::GraphFormatV1Cleanup,
    )
    .await
    .expect("pending Disabled-mode job reads"));
    assert!(disabled
        .process_migration_once()
        .await
        .expect("Disabled mode permits one deterministic step"));
    assert!(migration_completed(
        disabled.inner_db().as_ref(),
        scope,
        MigrationId::GraphFormatV1Cleanup,
    )
    .await
    .expect("completed Disabled-mode job reads"));
    assert!(!disabled
        .process_migration_once()
        .await
        .expect("drained Disabled-mode scan succeeds"));
    disabled.close().await.expect("Disabled-mode writer closes");
}

async fn seed_bootstrap_tuple(
    raw: &Db,
    marker: Bytes,
    include_logical: bool,
    include_vector: bool,
) {
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("bootstrap seed transaction opens");
    transaction
        .put(global(GlobalKey::StorageVersion), marker)
        .expect("marker stages");
    if include_logical {
        transaction
            .put(
                global(GlobalKey::LogicalIndexIdWatermark),
                encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                    LogicalIndexIdWatermark {
                        next_id: IndexId::initial(),
                    },
                )),
            )
            .expect("logical watermark stages");
    }
    if include_vector {
        transaction
            .put(
                global(GlobalKey::VectorPhysicalIdWatermark),
                encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                    VectorPhysicalIdWatermark {
                        next_id: VectorPhysicalIndexId::initial(),
                    },
                )),
            )
            .expect("vector watermark stages");
    }
    transaction.commit().await.expect("bootstrap seed commits");
}

/// Covers missing, malformed, partial, older, and future bootstrap metadata.
pub(crate) async fn run_bootstrap_rejection_contracts() {
    for (name, marker, include_logical, include_vector) in [
        ("malformed", Bytes::from_static(b"bad"), true, true),
        (
            "partial",
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::CURRENT,
            )),
            false,
            true,
        ),
        (
            "older",
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(1).expect("legacy marker 1 is nonzero"),
            )),
            true,
            true,
        ),
        (
            "future",
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(IndexStorageVersion::MAX_SUPPORTED.get() + 1)
                    .expect("future version is nonzero"),
            )),
            true,
            true,
        ),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(name);
        let raw = raw(&database, Arc::clone(&store)).await;
        seed_bootstrap_tuple(&raw, marker, include_logical, include_vector).await;
        let error = crate::migrations::startup::bootstrap_writer(&raw)
            .await
            .expect_err("invalid bootstrap tuple must fail closed");
        assert!(
            matches!(
                error,
                HelixDbError::MigrationRequired { .. }
                    | HelixDbError::UnsupportedIndexStorageVersion { .. }
            ),
            "unexpected bootstrap rejection: {error}"
        );
        raw.close()
            .await
            .expect("invalid bootstrap database closes");
    }
}

/// Proves an injected migration error preserves the legacy source row and
/// that a later clean reopen converges it through the normal V2 worker.
pub(crate) async fn run_failure_preservation_contract() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database("migration-failure-preservation");
    let definition: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition");
    seed_legacy_rows(
        &database,
        Arc::clone(&store),
        std::slice::from_ref(&definition),
        None,
    )
    .await;
    inject_migration_failpoint_once(MigrationFailpoint::LegacyDefinitionEnqueueBefore)
        .expect("failpoint injects");
    assert!(
        HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        )
        .await
        .is_err(),
        "injected enqueue failure must fail the writer open"
    );
    let raw = raw(&database, Arc::clone(&store)).await;
    assert_eq!(
        load_legacy_definition_rows(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("legacy rows scan")
            .len(),
        1,
        "failed migration must preserve its source definition"
    );
    raw.close().await.expect("inspection database closes");
    let recovered =
        HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config())
            .await
            .expect("clean reopen resumes failed migration");
    assert_legacy_catalog_empty(&recovered).await;
    recovered.close().await.expect("recovered database closes");
}

/// Closed set of vector migration stages that own generic batch work.
///
/// Encoding the two materialization stages and six retirement stages as
/// variants prevents the recovery harness from constructing an unrelated
/// migration/stage pair.
#[derive(Debug, Clone, Copy)]
enum VectorRecoveryStage {
    MaterializeNodes,
    MaterializeEdges,
    FenceLegacySources,
    RetireHotRows,
    RetireLayerZeroRows,
    RetireCoreRows,
    RetireDefinitions,
    ReleaseReservations,
}

impl VectorRecoveryStage {
    const ALL: [Self; 8] = [
        Self::MaterializeNodes,
        Self::MaterializeEdges,
        Self::FenceLegacySources,
        Self::RetireHotRows,
        Self::RetireLayerZeroRows,
        Self::RetireCoreRows,
        Self::RetireDefinitions,
        Self::ReleaseReservations,
    ];

    const fn migration_id(self) -> MigrationId {
        match self {
            Self::MaterializeNodes | Self::MaterializeEdges => {
                MigrationId::LegacyVectorPropertyMaterialization
            }
            Self::FenceLegacySources
            | Self::RetireHotRows
            | Self::RetireLayerZeroRows
            | Self::RetireCoreRows
            | Self::RetireDefinitions
            | Self::ReleaseReservations => MigrationId::LegacyVectorPhysicalCleanup,
        }
    }

    const fn migration_stage(self) -> MigrationStage {
        match self {
            Self::MaterializeNodes => MigrationStage::NodeProperties,
            Self::MaterializeEdges => MigrationStage::EdgeEndpoints,
            Self::FenceLegacySources => MigrationStage::FenceLegacyVectorSources,
            Self::RetireHotRows => MigrationStage::LegacyVectorHotRows,
            Self::RetireLayerZeroRows => MigrationStage::LegacyVectorLayer0Rows,
            Self::RetireCoreRows => MigrationStage::LegacyVectorCoreRows,
            Self::RetireDefinitions => MigrationStage::LegacyVectorDefinitions,
            Self::ReleaseReservations => MigrationStage::ReleaseLegacyVectorReservations,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::MaterializeNodes => "materialize-nodes",
            Self::MaterializeEdges => "materialize-edges",
            Self::FenceLegacySources => "fence-legacy-sources",
            Self::RetireHotRows => "retire-hot-rows",
            Self::RetireLayerZeroRows => "retire-layer-zero-rows",
            Self::RetireCoreRows => "retire-core-rows",
            Self::RetireDefinitions => "retire-definitions",
            Self::ReleaseReservations => "release-reservations",
        }
    }

    fn definition(self) -> ValidatedDynamicIndexDefinition {
        let definition = match self {
            Self::MaterializeEdges => VectorIndexDefinition::new_edge(
                "RELATED",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            ),
            Self::MaterializeNodes
            | Self::FenceLegacySources
            | Self::RetireHotRows
            | Self::RetireLayerZeroRows
            | Self::RetireCoreRows
            | Self::RetireDefinitions
            | Self::ReleaseReservations => VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            ),
        }
        .expect("recovery vector definition validates");
        definition
            .try_into()
            .expect("recovery V2 vector definition validates")
    }

    fn stage_rank(self, stage: MigrationStage) -> Option<u8> {
        match self.migration_id() {
            MigrationId::LegacyVectorPropertyMaterialization => match stage {
                MigrationStage::NodeProperties => Some(0),
                MigrationStage::EdgeEndpoints => Some(1),
                MigrationStage::PropertyIndexes
                | MigrationStage::LegacyEdgePairs
                | MigrationStage::FenceLegacyVectorSources
                | MigrationStage::LegacyVectorHotRows
                | MigrationStage::LegacyVectorLayer0Rows
                | MigrationStage::LegacyVectorCoreRows
                | MigrationStage::LegacyVectorDefinitions
                | MigrationStage::ReleaseLegacyVectorReservations => None,
            },
            MigrationId::LegacyVectorPhysicalCleanup => match stage {
                MigrationStage::FenceLegacyVectorSources => Some(0),
                MigrationStage::LegacyVectorHotRows => Some(1),
                MigrationStage::LegacyVectorLayer0Rows => Some(2),
                MigrationStage::LegacyVectorCoreRows => Some(3),
                MigrationStage::LegacyVectorDefinitions => Some(4),
                MigrationStage::ReleaseLegacyVectorReservations => Some(5),
                MigrationStage::PropertyIndexes
                | MigrationStage::NodeProperties
                | MigrationStage::LegacyEdgePairs
                | MigrationStage::EdgeEndpoints => None,
            },
            MigrationId::GraphFormatV1Rewrite | MigrationId::GraphFormatV1Cleanup => None,
        }
    }
}

/// Observable state that must survive retirement of a legacy namespace.
#[derive(Debug)]
struct RetainedVectorGeneration {
    physical_id: u64,
    digest: [u8; 32],
    retired_physical_id: u64,
}

/// Case-specific result checked after a clean recovery open.
#[derive(Debug)]
enum VectorRecoveryExpectation {
    MaterializedNode,
    MaterializedEdge,
    Retired(RetainedVectorGeneration),
}

/// Fully persisted fixture positioned immediately before its target stage.
struct PreparedVectorRecovery {
    database: String,
    store: Arc<dyn ObjectStore>,
    definition: ValidatedDynamicIndexDefinition,
    checkpoint: MigrationJob,
    expectation: VectorRecoveryExpectation,
}

/// Reads one typed durable migration job from its canonical metadata key.
async fn read_migration_job(raw: &Db, id: MigrationId) -> MigrationJob {
    let value = raw
        .get(MigrationJobKey::new(DataScope::LegacyUnscoped, id).as_ref())
        .await
        .expect("recovery migration job reads")
        .expect("recovery migration job exists");
    decode_json(&value).expect("recovery migration job decodes")
}

/// Uses real controller turns to stop a persisted job at the requested
/// stage without manufacturing a job-state encoding.
async fn advance_vector_recovery_job(
    database: &str,
    store: Arc<dyn ObjectStore>,
    recovery_stage: VectorRecoveryStage,
) -> MigrationJob {
    let raw = Arc::new(raw(database, store).await);
    let config = one_row_config();
    let writer = crate::HelixWriter::new(
        Arc::clone(&raw),
        config.id_lease_size(),
        config.id_lease_refill_threshold(),
    );
    let target = recovery_stage.migration_stage();
    let target_rank = recovery_stage
        .stage_rank(target)
        .expect("recovery target belongs to its migration");
    let checkpoint = loop {
        let job = read_migration_job(raw.as_ref(), recovery_stage.migration_id()).await;
        let MigrationJobState::Running { stage, .. } = &job.state else {
            panic!("recovery fixture completed or failed before {target:?}: {job:?}")
        };
        let rank = recovery_stage
            .stage_rank(*stage)
            .expect("recovery job remains in its closed stage set");
        assert!(rank <= target_rank, "recovery fixture advanced past target");
        if *stage == target {
            break job;
        }
        assert!(
            process_migration_once_by_id(
                &writer,
                DataScope::LegacyUnscoped,
                config.migrations(),
                recovery_stage.migration_id(),
            )
            .await
            .expect("recovery fixture controller turn succeeds"),
            "recovery fixture controller must advance"
        );
    };
    drop(writer);
    raw.close()
        .await
        .expect("positioned recovery fixture closes");
    checkpoint
}

/// Confirms the injected failure left either the old durable checkpoint or
/// a complete controller turn, never an unrelated or partially advanced
/// stage.
fn assert_recovery_checkpoint(
    recovery_stage: VectorRecoveryStage,
    failpoint: MigrationFailpoint,
    before: &MigrationJob,
    after: &MigrationJob,
) {
    assert_eq!(before.id, recovery_stage.migration_id());
    assert_eq!(after.id, recovery_stage.migration_id());
    let MigrationJobState::Running {
        stage: before_stage,
        resume_after_key: before_key,
        processed_rows: before_rows,
    } = &before.state
    else {
        panic!("prepared recovery checkpoint must be running")
    };
    assert_eq!(*before_stage, recovery_stage.migration_stage());
    let target_rank = recovery_stage
        .stage_rank(*before_stage)
        .expect("prepared stage belongs to the recovery migration");
    match &after.state {
        MigrationJobState::Running {
            stage,
            resume_after_key,
            processed_rows,
        }
        | MigrationJobState::Failed {
            stage,
            resume_after_key,
            processed_rows,
            ..
        } => {
            let after_rank = recovery_stage
                .stage_rank(*stage)
                .expect("failed checkpoint belongs to the recovery migration");
            if failpoint == MigrationFailpoint::StageTransitionAfter {
                assert_eq!(after_rank, target_rank.saturating_add(1));
            } else {
                assert_eq!(after_rank, target_rank);
            }
            assert!(*processed_rows >= *before_rows);
            if processed_rows == before_rows {
                assert_eq!(resume_after_key, before_key);
            }
        }
        MigrationJobState::Completed { processed_rows } => {
            assert_eq!(failpoint, MigrationFailpoint::StageTransitionAfter);
            assert!(*processed_rows >= *before_rows);
        }
    }
}

/// Builds one materialization or retirement fixture and positions it at the
/// exact stage under test through the real migration controller.
async fn prepare_vector_recovery(recovery_stage: VectorRecoveryStage) -> PreparedVectorRecovery {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database(&format!("migration-vector-{}", recovery_stage.name()));
    let definition = recovery_stage.definition();
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("recovery definition is always vector")
    };

    let expectation = match recovery_stage {
        VectorRecoveryStage::MaterializeNodes | VectorRecoveryStage::MaterializeEdges => {
            seed_legacy_rows(
                &database,
                Arc::clone(&store),
                std::slice::from_ref(&definition),
                None,
            )
            .await;
            let source = raw(&database, Arc::clone(&store)).await;
            mark_graph_rewrite_complete(&source).await;
            put_migration_job(
                &source,
                MigrationJob::new(
                    MigrationId::LegacyVectorPropertyMaterialization,
                    MigrationMode::BlockingStartup,
                ),
            )
            .await;
            let expectation = match recovery_stage {
                VectorRecoveryStage::MaterializeNodes => {
                    seed_legacy_node_without_vector(&source, 77, "Document").await;
                    populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                        &source,
                        vector_definition,
                        77,
                        &[1.0, 0.0, 0.0],
                    )
                    .await;
                    VectorRecoveryExpectation::MaterializedNode
                }
                VectorRecoveryStage::MaterializeEdges => {
                    seed_legacy_edge_without_vector(&source, 77, 11, 12, "RELATED").await;
                    populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                        &source,
                        vector_definition,
                        77,
                        &[1.0, 0.0, 0.0],
                    )
                    .await;
                    VectorRecoveryExpectation::MaterializedEdge
                }
                VectorRecoveryStage::FenceLegacySources
                | VectorRecoveryStage::RetireHotRows
                | VectorRecoveryStage::RetireLayerZeroRows
                | VectorRecoveryStage::RetireCoreRows
                | VectorRecoveryStage::RetireDefinitions
                | VectorRecoveryStage::ReleaseReservations => {
                    unreachable!("materialization branch excludes retirement recovery stages")
                }
            };
            source.close().await.expect("materialization source closes");
            expectation
        }
        VectorRecoveryStage::FenceLegacySources
        | VectorRecoveryStage::RetireHotRows
        | VectorRecoveryStage::RetireLayerZeroRows
        | VectorRecoveryStage::RetireCoreRows
        | VectorRecoveryStage::RetireDefinitions
        | VectorRecoveryStage::ReleaseReservations => {
            let active = HelixDB::open_with_object_store_for_migration_parity(
                database.clone(),
                Arc::clone(&store),
                one_row_config(),
            )
            .await
            .expect("retirement Active fixture opens");
            active
                .inner_db()
                .put(
                    Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: DataKeyKind::NodeProperty(
                            crate::encoding::v1::keys::NodePropertyKey::new(77),
                        ),
                    }
                    .to_bytes(),
                    property::encode_properties(&[
                        Property::string("$label", "Document"),
                        Property::string("title", "retained graph property"),
                        Property::f32_array("embedding", vec![1.0, 0.0, 0.0]),
                    ]),
                )
                .await
                .expect("current graph vector row writes");
            active
                .install_index_for_tests(definition.clone())
                .await
                .expect("retirement Active fixture builds");
            let current_handle = active
                .active_index_handles_loaded(DataScope::LegacyUnscoped)
                .into_iter()
                .find(|handle| {
                    matches!(
                        handle,
                        crate::index_lifecycle::ActiveIndexHandle::Vector {
                            definition: active_definition,
                            ..
                        } if active_definition.as_ref() == vector_definition
                    )
                })
                .expect("retained current vector handle is active");
            let crate::index_lifecycle::ActiveIndexHandle::Vector { layout, .. } = &current_handle
            else {
                unreachable!("matched current handle remains vector")
            };
            let current_handle_physical_id = layout
                .physical_index_id()
                .expect("retained current vector is unpartitioned");
            let current_generation =
                crate::search::vector::ValidatedVectorGenerationHandle::try_from_active::<
                    crate::search::vector::distance::Cosine,
                >(&current_handle, current_handle_physical_id)
                .expect("retained current vector handle validates");
            let transaction = active
                .inner_db()
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("retained current vector transaction opens");
            crate::search::vector::VectorIndex::<
                crate::search::vector::distance::Cosine,
            >::from_generation(&current_generation)
            .insert(&transaction, 77, &[1.0, 0.0, 0.0])
            .await
            .expect("retained current vector inserts");
            transaction
                .commit()
                .await
                .expect("retained current vector commits");
            let active_record = crate::index_lifecycle::repository::load_index_record(
                active.inner_db().as_ref(),
                DataScope::LegacyUnscoped,
                &definition.identity(),
            )
            .await
            .expect("current vector record reads")
            .expect("current vector record exists");
            let crate::index_lifecycle::IndexStateV2::Active {
                physical:
                    PhysicalGeneration::Vector {
                        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                        ..
                    },
                ..
            } = active_record.state()
            else {
                panic!("current vector record is active and unpartitioned")
            };
            let current_physical_id = physical_index_id.get();
            let current_digest =
                non_metadata_vector_digest(active.inner_db().as_ref(), current_physical_id).await;
            let empty_digest: [u8; 32] = Sha256::digest([]).into();
            assert_ne!(current_digest, empty_digest);
            active
                .close()
                .await
                .expect("retirement Active fixture closes");

            let source = raw(&database, Arc::clone(&store)).await;
            source
                .delete(scoped_metadata_key(
                    DataScope::LegacyUnscoped,
                    INDEX_V2_MIGRATION_READY,
                ))
                .await
                .expect("readiness resets before legacy source insertion");
            let (definition_key, definition_value) =
                migration_parity_legacy_catalog_row(&definition, false)
                    .expect("legacy vector definition encodes");
            source
                .put(definition_key, definition_value)
                .await
                .expect("legacy vector definition writes");
            populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                &source,
                vector_definition,
                77,
                &[1.0, 0.0, 0.0],
            )
            .await;
            let runtime = vector_definition.to_runtime();
            let legacy_name = crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            );
            let retired_physical_id = crate::search::vector::index_id_from_name(&legacy_name);
            assert_ne!(retired_physical_id, current_physical_id);
            let retired_physical_id_typed = VectorPhysicalIndexId::new(retired_physical_id)
                .expect("legacy physical id is positive");
            source
                .put(
                    IndexKey::Global {
                        kind: GlobalKey::LegacyVectorPhysicalReservation(retired_physical_id_typed),
                    }
                    .to_bytes(),
                    encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                        crate::index_lifecycle::LegacyVectorPhysicalReservation::LegacySource,
                    )),
                )
                .await
                .expect("legacy Active reservation writes");
            mark_graph_rewrite_complete(&source).await;
            mark_vector_materialization_complete(&source).await;
            reset_vector_cleanup_job(&source).await;
            source.close().await.expect("retirement source closes");
            VectorRecoveryExpectation::Retired(RetainedVectorGeneration {
                physical_id: current_physical_id,
                digest: current_digest,
                retired_physical_id,
            })
        }
    };
    let checkpoint =
        advance_vector_recovery_job(&database, Arc::clone(&store), recovery_stage).await;
    PreparedVectorRecovery {
        database,
        store,
        definition,
        checkpoint,
        expectation,
    }
}

/// Proves every generic boundary is recoverable at both materialization
/// stages and every physical-retirement stage (8 × 8 = 64 cases).
pub(crate) async fn run_vector_failpoint_recovery_contracts() {
    const BOUNDARIES: [MigrationFailpoint; 8] = [
        MigrationFailpoint::BatchReadBefore,
        MigrationFailpoint::BatchReadAfter,
        MigrationFailpoint::BatchWriteBefore,
        MigrationFailpoint::BatchWriteAfter,
        MigrationFailpoint::BatchCommitBefore,
        MigrationFailpoint::BatchCommitAfter,
        MigrationFailpoint::StageTransitionBefore,
        MigrationFailpoint::StageTransitionAfter,
    ];

    for recovery_stage in VectorRecoveryStage::ALL {
        for failpoint in BOUNDARIES {
            let prepared = prepare_vector_recovery(recovery_stage).await;
            inject_migration_failpoint_once(failpoint)
                .expect("stage-targeted vector failpoint injects");
            assert!(
                HelixDB::open_with_object_store_for_migration_parity(
                    prepared.database.clone(),
                    Arc::clone(&prepared.store),
                    one_row_config(),
                )
                .await
                .is_err(),
                "{} must interrupt {}",
                failpoint.as_str(),
                recovery_stage.name()
            );
            assert!(
                migration_failpoint_was_triggered(),
                "{} must fire in {}",
                failpoint.as_str(),
                recovery_stage.name()
            );
            let inspection = raw(&prepared.database, Arc::clone(&prepared.store)).await;
            assert!(
                !index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
                    .await
                    .expect("failed recovery readiness reads"),
                "readiness must remain unpublished after an injected failure"
            );
            let failed_job = read_migration_job(&inspection, recovery_stage.migration_id()).await;
            assert_recovery_checkpoint(
                recovery_stage,
                failpoint,
                &prepared.checkpoint,
                &failed_job,
            );
            inspection
                .close()
                .await
                .expect("failed recovery inspection closes");

            let recovered = HelixDB::open_with_object_store_for_migration_parity(
                prepared.database,
                prepared.store,
                one_row_config(),
            )
            .await
            .expect("clean recovery open converges");
            assert!(migration_completed(
                recovered.inner_db().as_ref(),
                DataScope::LegacyUnscoped,
                recovery_stage.migration_id(),
            )
            .await
            .expect("recovered migration state reads"));
            assert_legacy_catalog_empty(&recovered).await;
            let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &prepared.definition
            else {
                unreachable!("recovery definition remains vector")
            };
            match prepared.expectation {
                VectorRecoveryExpectation::MaterializedNode => {
                    assert_materialized_node_vector(
                        recovered.inner_db().as_ref(),
                        77,
                        &[1.0, 0.0, 0.0],
                    )
                    .await;
                }
                VectorRecoveryExpectation::MaterializedEdge => {
                    assert_materialized_edge_vector(
                        recovered.inner_db().as_ref(),
                        77,
                        &[1.0, 0.0, 0.0],
                    )
                    .await;
                }
                VectorRecoveryExpectation::Retired(retained) => {
                    assert_eq!(
                        non_metadata_vector_digest(
                            recovered.inner_db().as_ref(),
                            retained.physical_id,
                        )
                        .await,
                        retained.digest,
                        "current V2 physical rows must survive legacy retirement"
                    );
                    assert!(
                        vector_namespace_rows(
                            recovered.inner_db().as_ref(),
                            retained.retired_physical_id,
                        )
                        .await
                        .is_empty(),
                        "legacy vector namespace must be fully retired"
                    );
                }
            }
            assert_vector_search::<crate::search::vector::distance::Cosine>(
                &recovered,
                vector_definition,
                &[1.0, 0.0, 0.0],
                77,
            )
            .await;
            recovered.close().await.expect("recovery database closes");
        }
    }
}

/// Proves zero-cosine failure retains the exact durable cursor and retries
/// the same entity after its legacy payload is repaired.
pub(crate) async fn run_vector_zero_cosine_recovery_contract() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database("migration-vector-zero-cosine-recovery");
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("valid vector definition")
            .try_into()
            .expect("valid V2 vector definition");
    let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
        unreachable!("vector definition validates as vector")
    };
    seed_legacy_rows(
        &database,
        Arc::clone(&store),
        std::slice::from_ref(&definition),
        None,
    )
    .await;
    let source = raw(&database, Arc::clone(&store)).await;
    mark_graph_rewrite_complete(&source).await;
    let preceding_entity_id = 76;
    let zero_entity_id = 77;
    seed_legacy_node_without_vector(&source, preceding_entity_id, "Document").await;
    seed_legacy_node_without_vector(&source, zero_entity_id, "Document").await;
    let runtime = vector_definition.to_runtime();
    let physical_name = crate::search::vector_index_name(
        runtime.element_type(),
        runtime.label(),
        runtime.property(),
    );
    let physical_id = crate::search::vector::index_id_from_name(&physical_name);
    let simhash_bits = 0_u64;
    let mut metadata = crate::search::vector::VectorIndexMetadata::new(
        crate::search::vector::VectorIndexConfig::from_v2_definition(
            vector_definition,
            &physical_name,
        ),
    );
    metadata.entry_point = Some(zero_entity_id);
    metadata.count = 1;
    let transaction = source
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("zero-cosine fixture transaction opens");
    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::Vector(
                    VectorKey::IndexMetadata(VectorIndexMetadataKey::new(physical_id)),
                ),
            }
            .to_bytes(),
            Bytes::copy_from_slice(
                &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )
        .expect("zero-cosine metadata stages");
    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::SimHash(
                    VectorSimHashKey::new(physical_id, zero_entity_id),
                )),
            }
            .to_bytes(),
            Bytes::copy_from_slice(
                &crate::encoding::v1::values::vectors::simhash::encode_simhash(simhash_bits),
            ),
        )
        .expect("zero-cosine SimHash stages");
    let item_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::Vector(
            VectorItemKey::new(
                physical_id,
                crate::search::vector::simhash::order_code_from_simhash_bits(simhash_bits),
                zero_entity_id,
            ),
        )),
    }
    .to_bytes();
    transaction
        .put(
            item_key.clone(),
            crate::search::vector::encode_item(&crate::search::vector::Item::<
                crate::search::vector::distance::Cosine,
            >::new(vec![0.0, 0.0, 0.0])),
        )
        .expect("zero-cosine item stages");
    transaction
        .commit()
        .await
        .expect("zero-cosine fixture commits");
    source.close().await.expect("zero-cosine source closes");

    let error = HelixDB::open_with_object_store_for_migration_parity(
        database.clone(),
        Arc::clone(&store),
        one_row_config(),
    )
    .await
    .err()
    .expect("zero-cosine materialization must block writer open");
    assert!(matches!(
        error,
        HelixDbError::LegacyZeroNormCosineVector {
            element_kind: crate::index_lifecycle::IndexElementKind::Node,
            entity_id: 77,
            ..
        }
    ));
    let inspection = raw(&database, Arc::clone(&store)).await;
    let persisted = inspection
        .get(
            MigrationJobKey::new(
                DataScope::LegacyUnscoped,
                MigrationId::LegacyVectorPropertyMaterialization,
            )
            .as_ref(),
        )
        .await
        .expect("failed materialization job reads")
        .expect("failed materialization job exists");
    let failed =
        decode_json::<MigrationJob>(&persisted).expect("failed materialization job decodes");
    let MigrationJobState::Failed {
        stage: MigrationStage::NodeProperties,
        resume_after_key: Some(resume_after_key),
        processed_rows: 1,
        ..
    } = failed.state
    else {
        panic!("zero-cosine failure must retain the prior committed node cursor")
    };
    let preceding_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
            crate::encoding::v1::keys::NodePropertyKey::new(preceding_entity_id),
        ),
    }
    .to_bytes();
    assert_eq!(resume_after_key.as_bytes(), preceding_key.as_ref());
    inspection
        .put(
            item_key,
            crate::search::vector::encode_item(&crate::search::vector::Item::<
                crate::search::vector::distance::Cosine,
            >::new(vec![1.0, 0.0, 0.0])),
        )
        .await
        .expect("corrected cosine payload writes");
    inspection
        .close()
        .await
        .expect("zero-cosine inspection closes");

    let recovered =
        HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config())
            .await
            .expect("corrected zero-cosine migration resumes");
    assert_materialized_node_vector(
        recovered.inner_db().as_ref(),
        zero_entity_id,
        &[1.0, 0.0, 0.0],
    )
    .await;
    assert_legacy_catalog_empty(&recovered).await;
    recovered
        .close()
        .await
        .expect("zero-cosine recovery closes");
}

async fn seed_legacy_row_after_v2(
    database: &str,
    store: Arc<dyn ObjectStore>,
    definition: &ValidatedDynamicIndexDefinition,
) {
    let raw = raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("post-V2 legacy transaction opens");
    transaction
        .delete(scoped_metadata_key(
            DataScope::LegacyUnscoped,
            INDEX_V2_MIGRATION_READY,
        ))
        .expect("readiness marker deletion stages");
    let (key, value) =
        migration_parity_legacy_catalog_row(definition, false).expect("legacy definition encodes");
    transaction.put(key, value).expect("legacy row stages");
    transaction
        .commit()
        .await
        .expect("post-V2 legacy row commits");
    raw.close().await.expect("post-V2 seed database closes");
}

/// Proves an exact already-Active definition retires its legacy row while a
/// same-identity settings conflict fails closed and preserves both sources.
pub(crate) async fn run_existing_active_and_conflict_contracts() {
    let exact_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let exact_database = database("migration-already-active");
    let exact: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition");
    let exact_db = HelixDB::open_with_object_store_for_migration_parity(
        exact_database.clone(),
        Arc::clone(&exact_store),
        one_row_config(),
    )
    .await
    .expect("exact Active fixture opens");
    exact_db
        .install_index_for_tests(exact.clone())
        .await
        .expect("exact Active fixture builds");
    exact_db.close().await.expect("exact fixture closes");
    seed_legacy_row_after_v2(&exact_database, Arc::clone(&exact_store), &exact).await;
    let exact_reopened = HelixDB::open_with_object_store_for_migration_parity(
        exact_database,
        exact_store,
        one_row_config(),
    )
    .await
    .expect("exact Active legacy row retires");
    assert_legacy_catalog_empty(&exact_reopened).await;
    exact_reopened
        .close()
        .await
        .expect("exact reopened database closes");

    let conflict_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let conflict_database = database("migration-conflict");
    let active: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_equality("User", "email")
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition");
    let conflict: ValidatedDynamicIndexDefinition =
        SecondaryIndexDefinition::node_unique_equality("User", "email")
            .expect("valid conflicting definition")
            .try_into()
            .expect("valid conflicting V2 definition");
    let conflict_db = HelixDB::open_with_object_store_for_migration_parity(
        conflict_database.clone(),
        Arc::clone(&conflict_store),
        one_row_config(),
    )
    .await
    .expect("conflict fixture opens");
    conflict_db
        .install_index_for_tests(active)
        .await
        .expect("conflict Active fixture builds");
    conflict_db.close().await.expect("conflict fixture closes");
    seed_legacy_row_after_v2(&conflict_database, Arc::clone(&conflict_store), &conflict).await;
    assert!(
        HelixDB::open_with_object_store_for_migration_parity(
            conflict_database.clone(),
            Arc::clone(&conflict_store),
            one_row_config(),
        )
        .await
        .is_err(),
        "same-identity definition conflict must fail closed"
    );
    let raw = raw(&conflict_database, conflict_store).await;
    assert_eq!(
        load_legacy_definition_rows(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("conflicting legacy row scans")
            .len(),
        1,
        "conflict must not delete legacy source data"
    );
    assert_eq!(
        crate::index_lifecycle::repository::load_scope_catalog(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("canonical Active catalog remains valid")
            .active_handles()
            .count(),
        1,
        "conflict must not delete canonical V2 ownership"
    );
    raw.close().await.expect("conflict inspection closes");
}
