#![allow(deprecated)]

//! Populated V1-to-current-index migration acceptance fixtures.
//!
//! These fixtures persist only deployed V1 keys and values, then enter through
//! normal writer bootstrap. They intentionally do not call migration stages or
//! index drivers directly.

mod collision;
mod failure;
mod prefix;
mod recovery;
mod retirement;
mod semantics;

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};

use crate::config::{
    MigrationBatchRows, MigrationTuning, SecondaryIndexDefinition,
    SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning, TextIndexDefinition,
    VectorIndexDefinition,
};
use crate::encoding::property::{decode_properties, encode_properties, Property};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::vectors::{VectorIndexMetadataKey, VectorKey};
use crate::encoding::v1::keys::{
    DataKeyKind, EdgeEndpointsKey, EdgePropertyByIdKey, EdgePropertyPairKey, Key, NodePropertyKey,
};
use crate::encoding::v1::values::edge_endpoints::EdgeEndpointsValue;
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexIdentityFamily, IndexStateV2, ValidatedDynamicIndexDefinition,
};
use crate::search::vector::VectorDistanceMetric;
use crate::{DbConfig, HelixDB};

pub use collision::{
    v1_property_hash_collision_migration_contract, V1CollisionIndexObservation,
    V1CollisionMigrationObservation,
};
pub use failure::{
    v1_malformed_catalog_failure_preservation_contract, v1_range_failure_preservation_contract,
    v1_unique_failure_preservation_contract, V1MalformedCatalogObservation,
    V1RangeFailureCaseObservation, V1RangeFailureMigrationObservation,
    V1UniqueMigrationObservation,
};
pub use prefix::{v1_prefix_successor_contract, V1PrefixSuccessorObservation};
pub use recovery::{v1_graph_crash_recovery_contract, V1CrashRecoveryObservation};
pub use retirement::{
    v1_secondary_retirement_failpoint_contract, V1RetirementFailpointObservation,
};
pub use semantics::{
    v1_equality_semantics_migration_contract, v1_range_semantics_migration_contract, V1ElementKind,
    V1EqualityMigrationObservation, V1EqualityQueryObservation, V1OracleValue, V1RangeAccess,
    V1RangeBound, V1RangeCaseObservation, V1RangeDirection, V1RangeMigrationObservation,
    V1SemanticRow,
};

const NODE_ZERO_PREFIX: u64 = 0x0000_0000_0000_0001;
const NODE_ASCII_PREFIX: u64 = 0x4100_0000_0000_0001;
const NODE_FF_PREFIX: u64 = 0xFF00_0000_0000_0001;
const LEGACY_VECTOR_EDGE_ID: u64 = 42;
const POPULATED_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Stable Active-index evidence retained across a cold reopen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1ActiveIndexObservation {
    /// Canonical family lane.
    pub family: &'static str,
    /// Canonical element kind.
    pub element_kind: &'static str,
    /// Exact label component.
    pub label: String,
    /// Exact property component.
    pub property: String,
    /// Stable logical index ID.
    pub index_id: u64,
    /// Stable physical generation.
    pub generation: u64,
}

/// Result of the populated V1 migration acceptance fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1PopulatedMigrationObservation {
    /// Authoritative node IDs retained by migration.
    pub node_ids: Vec<u64>,
    /// Current edge IDs allocated or retained by migration.
    pub edge_ids: Vec<u64>,
    /// Exact Active catalog projection.
    pub active_indexes: Vec<V1ActiveIndexObservation>,
    /// Whether graph, index, and storage readiness were all published.
    pub readiness_published: bool,
    /// Whether every legacy dynamic-definition row was retired.
    pub legacy_catalog_empty: bool,
    /// Whether a cold reopen retained identical graph and catalog evidence.
    pub cold_reopen_identical: bool,
}

fn database() -> String {
    format!("populated-v1-current-index-{}", uuid::Uuid::new_v4())
}

fn one_row_config() -> DbConfig {
    DbConfig::new()
        .with_migration_tuning(
            MigrationTuning::default().with_batch_rows(
                MigrationBatchRows::new(1).expect("one migration row is positive"),
            ),
        )
        .with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_batch_rows(
                SecondaryIndexLifecycleBatchRows::new(1)
                    .expect("one secondary lifecycle row is positive"),
            ),
        )
}

async fn raw(database: &str, store: Arc<dyn ObjectStore>) -> Db {
    Db::builder(database, store)
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("populated V1 raw database opens")
}

fn definitions() -> Vec<ValidatedDynamicIndexDefinition> {
    vec![
        SecondaryIndexDefinition::node_equality("User", "eq")
            .expect("node equality definition validates")
            .try_into()
            .expect("node equality converts"),
        SecondaryIndexDefinition::node_unique_equality("User", "unique")
            .expect("node unique definition validates")
            .try_into()
            .expect("node unique converts"),
        SecondaryIndexDefinition::node_range("User", "range_asc")
            .expect("node ascending range definition validates")
            .try_into()
            .expect("node ascending range converts"),
        SecondaryIndexDefinition::node_range_desc("User", "range_desc")
            .expect("node descending range definition validates")
            .try_into()
            .expect("node descending range converts"),
        SecondaryIndexDefinition::edge_equality("FOLLOWS", "eq")
            .expect("edge equality definition validates")
            .try_into()
            .expect("edge equality converts"),
        SecondaryIndexDefinition::edge_range("FOLLOWS", "range_asc")
            .expect("edge ascending range definition validates")
            .try_into()
            .expect("edge ascending range converts"),
        SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "range_desc")
            .expect("edge descending range definition validates")
            .try_into()
            .expect("edge descending range converts"),
        TextIndexDefinition::new_node("User", "body")
            .expect("node text definition validates")
            .try_into()
            .expect("node text converts"),
        TextIndexDefinition::new_edge("FOLLOWS", "notes")
            .expect("edge text definition validates")
            .try_into()
            .expect("edge text converts"),
        VectorIndexDefinition::new_node("User", "embedding", 3, VectorDistanceMetric::Cosine)
            .expect("node vector definition validates")
            .try_into()
            .expect("node vector converts"),
        VectorIndexDefinition::new_edge("FOLLOWS", "embedding", 3, VectorDistanceMetric::Euclidean)
            .expect("edge vector definition validates")
            .try_into()
            .expect("edge vector converts"),
    ]
}

fn node_properties(unique: &str, rank: i64) -> Vec<Property> {
    vec![
        Property::string("$label", "User"),
        Property::string("eq", "shared"),
        Property::string("unique", unique),
        Property::i64("range_asc", rank),
        Property::i64("range_desc", rank),
        Property::string("body", format!("migration body {unique}")),
    ]
}

fn edge_properties(kind: &str, rank: i64) -> Vec<Property> {
    vec![
        Property::string("$label", "FOLLOWS"),
        Property::string("eq", kind),
        Property::i64("range_asc", rank),
        Property::i64("range_desc", rank),
        Property::string("notes", format!("migration notes {kind}")),
    ]
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
    let physical_id = crate::search::vector::index_id_from_name(&physical_name);
    let metadata_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            physical_id,
        ))),
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
        .expect("populated legacy metadata reads")
        .expect("populated legacy metadata exists");
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
        .expect("legacy vector metadata stages");
    transaction
        .commit()
        .await
        .expect("legacy vector metadata commits");
}

async fn seed_populated_v1(
    database: &str,
    store: Arc<dyn ObjectStore>,
    definitions: &[ValidatedDynamicIndexDefinition],
) {
    let raw = raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("populated V1 transaction opens");

    for (node_id, properties) in [
        (NODE_ZERO_PREFIX, node_properties("zero", -1)),
        (NODE_ASCII_PREFIX, node_properties("ascii", 0)),
        (NODE_FF_PREFIX, node_properties("ff", 1)),
    ] {
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
                encode_properties(&properties),
            )
            .expect("V1 node row stages");
    }

    for (from, to, properties) in [
        (
            NODE_ZERO_PREFIX,
            NODE_ASCII_PREFIX,
            edge_properties("first", -1),
        ),
        (
            NODE_ASCII_PREFIX,
            NODE_FF_PREFIX,
            edge_properties("second", 1),
        ),
    ] {
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(from, to)),
                }
                .to_bytes(),
                encode_properties(&properties),
            )
            .expect("V1 edge-pair row stages");
    }

    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(LEGACY_VECTOR_EDGE_ID)),
            }
            .to_bytes(),
            EdgeEndpointsValue::new(NODE_FF_PREFIX, NODE_ZERO_PREFIX).encode(),
        )
        .expect("legacy vector edge endpoints stage");
    transaction
        .put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(
                    LEGACY_VECTOR_EDGE_ID,
                )),
            }
            .to_bytes(),
            encode_properties(&edge_properties("vector", 2)),
        )
        .expect("legacy vector edge properties stage");

    for definition in definitions {
        let (key, value) =
            crate::migrations::migration_parity_legacy_catalog_row(definition, false)
                .expect("V1 catalog definition encodes");
        transaction
            .put(key, value)
            .expect("V1 catalog definition stages");
    }

    transaction
        .commit()
        .await
        .expect("populated V1 fixture commits");

    for definition in definitions {
        let ValidatedDynamicIndexDefinition::Vector(vector) = definition else {
            continue;
        };
        match vector.element_kind() {
            crate::index_lifecycle::IndexElementKind::Node => {
                populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                    &raw,
                    vector,
                    NODE_ZERO_PREFIX,
                    &[1.0, 0.0, 0.0],
                )
                .await;
            }
            crate::index_lifecycle::IndexElementKind::Edge => {
                populate_legacy_vector::<crate::search::vector::distance::Euclidean>(
                    &raw,
                    vector,
                    LEGACY_VECTOR_EDGE_ID,
                    &[0.0, 1.0, 1.0],
                )
                .await;
            }
        }
    }
    raw.close().await.expect("populated V1 raw database closes");
}

fn family_name(family: IndexIdentityFamily) -> &'static str {
    match family {
        IndexIdentityFamily::SecondaryEquality => "secondary_equality",
        IndexIdentityFamily::SecondaryRange => "secondary_range",
        IndexIdentityFamily::Vector => "vector",
        IndexIdentityFamily::Text => "text",
    }
}

fn active_observations(db: &HelixDB) -> Vec<V1ActiveIndexObservation> {
    let mut observations = db
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .map(|handle| {
            let identity = handle.identity();
            V1ActiveIndexObservation {
                family: family_name(identity.family()),
                element_kind: match identity.element_kind() {
                    crate::index_lifecycle::IndexElementKind::Node => "node",
                    crate::index_lifecycle::IndexElementKind::Edge => "edge",
                },
                label: identity.label().as_str().to_string(),
                property: identity.property().as_str().to_string(),
                index_id: handle.index_id().get(),
                generation: handle.generation().get(),
            }
        })
        .collect::<Vec<_>>();
    observations.sort_by(|left, right| {
        (
            left.family,
            left.element_kind,
            left.label.as_str(),
            left.property.as_str(),
        )
            .cmp(&(
                right.family,
                right.element_kind,
                right.label.as_str(),
                right.property.as_str(),
            ))
    });
    observations
}

pub(super) async fn legacy_catalog_empty_raw(db: &Db) -> bool {
    let prefix = Key::data_prefix(
        DataScope::LegacyUnscoped,
        crate::encoding::v2::legacy::index_catalog::catalog_scan_prefix(DataScope::LegacyUnscoped),
    );
    let mut rows = db
        .scan_prefix(prefix, ..)
        .await
        .expect("legacy catalog scans");
    rows.next()
        .await
        .expect("legacy catalog row reads")
        .is_none()
}

pub(super) async fn legacy_catalog_empty(db: &HelixDB) -> bool {
    legacy_catalog_empty_raw(db.inner_db().as_ref()).await
}

async fn assert_graph_and_collect_edges(db: &HelixDB) -> Vec<u64> {
    for (node_id, unique, rank) in [
        (NODE_ZERO_PREFIX, "zero", -1),
        (NODE_ASCII_PREFIX, "ascii", 0),
        (NODE_FF_PREFIX, "ff", 1),
    ] {
        let bytes = db
            .inner_db()
            .get(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
            )
            .await
            .expect("migrated node row reads")
            .expect("migrated node row exists");
        let properties = decode_properties(&bytes).expect("migrated node properties decode");
        assert!(properties.contains(&Property::string("$label", "User")));
        assert!(properties.contains(&Property::string("unique", unique)));
        assert!(properties.contains(&Property::i64("range_asc", rank)));
        assert!(properties.contains(&Property::i64("range_desc", rank)));
        if node_id == NODE_ZERO_PREFIX {
            assert!(properties.contains(&Property::f32_array("embedding", vec![1.0, 0.0, 0.0],)));
        } else {
            assert!(properties
                .iter()
                .all(|property| property.name != "embedding"));
        }
    }

    let prefix = Key::data_prefix(
        DataScope::LegacyUnscoped,
        Bytes::copy_from_slice(EdgeEndpointsKey::key_prefix().as_slice()),
    );
    let mut rows = db
        .inner_db()
        .scan_prefix(prefix, ..)
        .await
        .expect("current edge endpoint rows scan");
    let mut edge_ids = Vec::new();
    while let Some(row) = rows.next().await.expect("current edge endpoint row reads") {
        let Key::Data {
            kind: DataKeyKind::EdgeEndpoints(key),
            ..
        } = Key::parse_from_slice(DataScope::LegacyUnscoped, &row.key)
            .expect("current edge endpoint key parses")
        else {
            panic!("edge endpoint prefix yielded another key kind");
        };
        let endpoints =
            EdgeEndpointsValue::decode(&row.value).expect("current edge endpoint value decodes");
        assert!(matches!(
            (endpoints.source(), endpoints.target()),
            (NODE_ZERO_PREFIX, NODE_ASCII_PREFIX)
                | (NODE_ASCII_PREFIX, NODE_FF_PREFIX)
                | (NODE_FF_PREFIX, NODE_ZERO_PREFIX)
        ));
        let properties = db
            .inner_db()
            .get(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(key.edge_id())),
                }
                .to_bytes(),
            )
            .await
            .expect("current edge property row reads")
            .expect("current edge property row exists");
        let properties =
            decode_properties(&properties).expect("current edge properties decode exactly");
        assert!(properties.contains(&Property::string("$label", "FOLLOWS")));
        assert!(properties.iter().any(|property| property.name == "eq"));
        assert!(properties
            .iter()
            .any(|property| property.name == "range_asc"));
        assert!(properties
            .iter()
            .any(|property| property.name == "range_desc"));
        assert!(properties.iter().any(|property| property.name == "notes"));
        if key.edge_id() == LEGACY_VECTOR_EDGE_ID {
            assert!(properties.contains(&Property::f32_array("embedding", vec![0.0, 1.0, 1.0],)));
        } else {
            assert!(properties
                .iter()
                .all(|property| property.name != "embedding"));
        }
        edge_ids.push(key.edge_id());
    }
    edge_ids.sort_unstable();
    edge_ids
}

async fn assert_active_records(db: &HelixDB, definitions: &[ValidatedDynamicIndexDefinition]) {
    for definition in definitions {
        let record = crate::index_lifecycle::repository::load_index_record(
            db.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("migrated canonical record reads")
        .expect("migrated canonical record exists");
        assert_eq!(record.definition(), definition);
        assert!(
            matches!(record.state(), IndexStateV2::Active { .. }),
            "every populated V1 definition must be Active"
        );
        assert!(
            ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record).is_some(),
            "every Active record must project a runtime capability"
        );
    }
}

async fn observe(
    db: &HelixDB,
    definitions: &[ValidatedDynamicIndexDefinition],
) -> V1PopulatedMigrationObservation {
    assert_active_records(db, definitions).await;
    let edge_ids = assert_graph_and_collect_edges(db).await;
    let readiness_published =
        crate::migrations::graph_format_v1_ready(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
            .await
            .expect("graph readiness reads")
            && crate::migrations::index_v2_migration_ready(
                db.inner_db().as_ref(),
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("index readiness reads")
            && crate::migrations::storage_schema_complete(
                db.inner_db().as_ref(),
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("storage readiness reads");
    V1PopulatedMigrationObservation {
        node_ids: vec![NODE_ZERO_PREFIX, NODE_ASCII_PREFIX, NODE_FF_PREFIX],
        edge_ids,
        active_indexes: active_observations(db),
        readiness_published,
        legacy_catalog_empty: legacy_catalog_empty(db).await,
        cold_reopen_identical: false,
    }
}

/// Migrates populated V1 graph rows and every dynamic-index family together.
pub async fn populated_v1_current_index_migration_contract() -> V1PopulatedMigrationObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = database();
    let definitions = definitions();
    seed_populated_v1(&database, Arc::clone(&store), &definitions).await;

    let migrated = tokio::time::timeout(
        POPULATED_OPEN_TIMEOUT,
        HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        ),
    )
    .await
    .expect("populated V1 writer open must not hang")
    .expect("populated V1 writer migration succeeds");
    let first = observe(&migrated, &definitions).await;
    migrated.close().await.expect("migrated writer closes");

    let reopened = tokio::time::timeout(
        POPULATED_OPEN_TIMEOUT,
        HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config()),
    )
    .await
    .expect("populated V1 cold reopen must not hang")
    .expect("populated V1 cold reopen succeeds");
    let second = observe(&reopened, &definitions).await;
    reopened.close().await.expect("reopened writer closes");

    assert_eq!(first.node_ids, second.node_ids);
    assert_eq!(first.edge_ids, second.edge_ids);
    assert_eq!(first.active_indexes, second.active_indexes);
    assert!(first.readiness_published && second.readiness_published);
    assert!(first.legacy_catalog_empty && second.legacy_catalog_empty);

    V1PopulatedMigrationObservation {
        cold_reopen_identical: true,
        ..second
    }
}

#[test]
fn v1_facade_forwards_to_identical_v2_bytes() {
    let v1_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(41)),
    };
    let v2_key = crate::encoding::v2::keys::DataKey::Data {
        scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::v2::keys::DataKeyKind::NodeProperty(
            crate::encoding::v2::keys::NodePropertyKey::new(41),
        ),
    };
    assert_eq!(v1_key.to_bytes(), v2_key.to_bytes());

    let properties = [Property::string("name", "migration-parity")];
    assert_eq!(
        crate::encoding::v1::property::encode_properties(&properties),
        crate::encoding::v2::values::property::encode_properties(&properties),
    );
}
