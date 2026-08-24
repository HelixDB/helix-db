//! V1 secondary property-hash collision and legacy-row independence fixtures.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::SecondaryIndexDefinition;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, encode_properties, Property};
use crate::encoding::v2::keys::indexes::range::RangeIndexDirection;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    DataKeyKind, EdgeEndpointsKey, EdgePropertyByIdKey, DataKey as Key, NodePropertyKey,
};
use crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue;
use crate::index_lifecycle::secondary::{
    lookup_active_equality_generation, scan_active_range_generation,
};
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;

const FIRST_PROPERTY: &str = "property_16755";
const SECOND_PROPERTY: &str = "property_36911";
const NODE_LABEL: &str = "User";
const EDGE_LABEL: &str = "User";
const FIRST_NODE: u64 = 50_000;
const SECOND_NODE: u64 = 50_001;
const FIRST_EDGE: u64 = 60_000;
const SECOND_EDGE: u64 = 60_001;
const STALE_NODE: u64 = 99_000;
const STALE_EDGE: u64 = 99_001;
const COLLISION_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// Exact result for one migrated colliding-property definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1CollisionIndexObservation {
    /// `node` or `edge`.
    pub element_kind: &'static str,
    /// `equality` or `range`.
    pub family: &'static str,
    /// Full un-hashed property identity.
    pub property: &'static str,
    /// Authoritative IDs served by the current generation.
    pub ids: Vec<u64>,
}

/// End-to-end collision migration evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1CollisionMigrationObservation {
    /// Both deployed scoped names reproduce the known 32-bit collision.
    pub exact_legacy_hash_collision: bool,
    /// Every full-string current identity and its isolated result.
    pub indexes: Vec<V1CollisionIndexObservation>,
    /// Authoritative graph rows and edge ownership remained byte-equivalent.
    pub graph_rows_exact: bool,
    /// Remaining legacy node equality memberships.
    pub legacy_node_equality_ids: Vec<u64>,
    /// Remaining legacy node range memberships.
    pub legacy_node_range_ids: Vec<u64>,
    /// Remaining legacy global edge equality memberships.
    pub legacy_global_edge_equality_ids: Vec<u64>,
    /// Remaining legacy global edge range memberships.
    pub legacy_global_edge_range_ids: Vec<u64>,
    /// All exact legacy catalog definitions were retired.
    pub legacy_catalog_empty: bool,
    /// Graph, index, and storage readiness were published.
    pub readiness_published: bool,
    /// A cold reopen preserved current identities and results.
    pub cold_reopen_identical: bool,
}

fn scoped(property: &str) -> String {
    crate::config::scoped_secondary_index_property(NODE_LABEL, property)
}

fn definitions() -> Vec<ValidatedDynamicIndexDefinition> {
    [
        SecondaryIndexDefinition::node_equality(NODE_LABEL, FIRST_PROPERTY),
        SecondaryIndexDefinition::node_equality(NODE_LABEL, SECOND_PROPERTY),
        SecondaryIndexDefinition::node_range(NODE_LABEL, FIRST_PROPERTY),
        SecondaryIndexDefinition::node_range(NODE_LABEL, SECOND_PROPERTY),
        SecondaryIndexDefinition::edge_equality(EDGE_LABEL, FIRST_PROPERTY),
        SecondaryIndexDefinition::edge_equality(EDGE_LABEL, SECOND_PROPERTY),
        SecondaryIndexDefinition::edge_range(EDGE_LABEL, FIRST_PROPERTY),
        SecondaryIndexDefinition::edge_range(EDGE_LABEL, SECOND_PROPERTY),
    ]
    .into_iter()
    .map(|definition| {
        definition
            .expect("collision definition validates")
            .try_into()
            .expect("collision definition converts")
    })
    .collect()
}

async fn seed(
    database: &str,
    store: Arc<dyn ObjectStore>,
    definitions: &[ValidatedDynamicIndexDefinition],
) {
    let raw = super::raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("collision seed transaction opens");
    for (node_id, property, value) in [
        (FIRST_NODE, FIRST_PROPERTY, 11_i64),
        (SECOND_NODE, SECOND_PROPERTY, 22_i64),
    ] {
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
                encode_properties(&[
                    Property::string("$label", NODE_LABEL),
                    Property::i64(property, value),
                ]),
            )
            .expect("collision node row stages");
        crate::search::add_to_equality_index_scoped(
            &transaction,
            "$label",
            NODE_LABEL,
            node_id,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("collision node label row stages");
    }
    for (edge_id, property, value) in [
        (FIRST_EDGE, FIRST_PROPERTY, 11_i64),
        (SECOND_EDGE, SECOND_PROPERTY, 22_i64),
    ] {
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
                }
                .to_bytes(),
                EdgeEndpointsValue::new(FIRST_NODE, SECOND_NODE).encode(),
            )
            .expect("collision edge endpoints stage");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
                }
                .to_bytes(),
                encode_properties(&[
                    Property::string("$label", EDGE_LABEL),
                    Property::i64(property, value),
                ]),
            )
            .expect("collision edge properties stage");
        crate::search::add_to_global_edge_label_index_scoped(
            &transaction,
            EDGE_LABEL,
            edge_id,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("collision edge label row stages");
    }

    let first = scoped(FIRST_PROPERTY);
    let second = scoped(SECOND_PROPERTY);
    assert_eq!(
        crate::encoding::indexes::hash_property_name(&first),
        crate::encoding::indexes::hash_property_name(&second),
        "fixture must reproduce the deployed property-hash collision"
    );

    for (property, value, node_id) in [
        (first.as_str(), "wrong-first", SECOND_NODE),
        (second.as_str(), "wrong-second", FIRST_NODE),
        (first.as_str(), "stale", STALE_NODE),
    ] {
        crate::search::add_to_equality_index_scoped(
            &transaction,
            property,
            value,
            node_id,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("misleading legacy node equality row stages");
    }
    for (property, value, node_id) in [
        (first.as_str(), "999", SECOND_NODE),
        (second.as_str(), "000", FIRST_NODE),
        (first.as_str(), "500", STALE_NODE),
    ] {
        crate::search::add_to_range_index_with_direction_scoped(
            &transaction,
            property,
            value,
            node_id,
            RangeIndexDirection::Asc,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("misleading legacy node range row stages");
    }
    for (property, value, edge_id) in [
        (first.as_str(), "wrong-first", SECOND_EDGE),
        (second.as_str(), "wrong-second", FIRST_EDGE),
        (first.as_str(), "stale", STALE_EDGE),
    ] {
        crate::search::add_to_edge_equality_index_scoped(
            &transaction,
            FIRST_NODE,
            SECOND_NODE,
            edge_id,
            property,
            value,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("misleading legacy edge equality row stages");
    }
    for (property, value, edge_id) in [
        (first.as_str(), "999", SECOND_EDGE),
        (second.as_str(), "000", FIRST_EDGE),
        (first.as_str(), "500", STALE_EDGE),
    ] {
        crate::search::add_to_edge_range_index_with_direction_scoped(
            &transaction,
            FIRST_NODE,
            SECOND_NODE,
            edge_id,
            property,
            value,
            RangeIndexDirection::Asc,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("misleading legacy edge range row stages");
    }

    for definition in definitions {
        let (key, value) =
            crate::migrations::migration_parity_legacy_catalog_row(definition, false)
                .expect("collision legacy definition encodes");
        transaction
            .put(key, value)
            .expect("collision legacy definition stages");
    }
    transaction
        .commit()
        .await
        .expect("collision seed transaction commits");
    raw.close().await.expect("collision raw database closes");
}

async fn open(database: String, store: Arc<dyn ObjectStore>) -> crate::HelixDB {
    tokio::time::timeout(
        COLLISION_OPEN_TIMEOUT,
        crate::HelixDB::open_with_object_store_for_migration_parity(
            database,
            store,
            super::one_row_config(),
        ),
    )
    .await
    .expect("collision V1 writer open must terminate")
    .expect("collision V1 migration succeeds")
}

async fn observe(db: &crate::HelixDB) -> Vec<V1CollisionIndexObservation> {
    let mut observations = Vec::new();
    for (property, value) in [(FIRST_PROPERTY, 11_i64), (SECOND_PROPERTY, 22_i64)] {
        for element_kind in [
            super::semantics::V1ElementKind::Node,
            super::semantics::V1ElementKind::Edge,
        ] {
            let equality = super::semantics::secondary_handle(db, element_kind, property, None);
            let equality_ids = lookup_active_equality_generation(
                db.inner_db().as_ref(),
                &equality,
                &PropertyValue::I64(value),
            )
            .await
            .expect("collision current equality lookup succeeds")
            .iter()
            .collect();
            observations.push(V1CollisionIndexObservation {
                element_kind: match element_kind {
                    super::semantics::V1ElementKind::Node => "node",
                    super::semantics::V1ElementKind::Edge => "edge",
                },
                family: "equality",
                property,
                ids: equality_ids,
            });

            let range = super::semantics::secondary_handle(db, element_kind, property, Some(false));
            let range_ids =
                scan_active_range_generation(db.inner_db().as_ref(), &range, None, None)
                    .await
                    .expect("collision current range scan succeeds");
            observations.push(V1CollisionIndexObservation {
                element_kind: match element_kind {
                    super::semantics::V1ElementKind::Node => "node",
                    super::semantics::V1ElementKind::Edge => "edge",
                },
                family: "range",
                property,
                ids: range_ids,
            });
        }
    }
    observations
}

async fn remaining_legacy_physical_rows(
    db: &crate::HelixDB,
) -> (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>) {
    let first = scoped(FIRST_PROPERTY);
    let second = scoped(SECOND_PROPERTY);
    let mut node_equality = Vec::new();
    let mut edge_equality = Vec::new();
    for property in [&first, &second] {
        for value in ["wrong-first", "wrong-second", "stale"] {
            node_equality.extend(
                crate::search::lookup_equality_index_scoped(
                    db.inner_db().as_ref(),
                    property,
                    value,
                    DataScope::LegacyUnscoped,
                )
                .await
                .expect("legacy node equality cleanup reads"),
            );
            edge_equality.extend(
                crate::search::lookup_global_edge_equality_index_scoped(
                    db.inner_db().as_ref(),
                    property,
                    value,
                    DataScope::LegacyUnscoped,
                )
                .await
                .expect("legacy edge equality cleanup reads")
                .iter(),
            );
        }
    }
    node_equality.sort_unstable();
    node_equality.dedup();
    edge_equality.sort_unstable();
    edge_equality.dedup();

    let mut node_range = crate::search::scan_range_index_scoped(
        db.inner_db().as_ref(),
        RangeIndexDirection::Asc,
        &first,
        DataScope::LegacyUnscoped,
    )
    .await
    .expect("legacy node range cleanup reads");
    node_range.extend(
        crate::search::scan_range_index_scoped(
            db.inner_db().as_ref(),
            RangeIndexDirection::Asc,
            &second,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("second legacy node range cleanup reads"),
    );
    node_range.sort_unstable();
    node_range.dedup();

    let mut edge_range =
        crate::search::scan_global_edge_range_index_all_with_direction_limited_scoped(
            db.inner_db().as_ref(),
            &first,
            RangeIndexDirection::Asc,
            None,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("legacy edge range cleanup reads");
    edge_range.extend(
        crate::search::scan_global_edge_range_index_all_with_direction_limited_scoped(
            db.inner_db().as_ref(),
            &second,
            RangeIndexDirection::Asc,
            None,
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("second legacy edge range cleanup reads"),
    );
    edge_range.sort_unstable();
    edge_range.dedup();
    (node_equality, node_range, edge_equality, edge_range)
}

async fn graph_rows_exact(db: &crate::HelixDB) -> bool {
    for (node_id, property, value) in [
        (FIRST_NODE, FIRST_PROPERTY, 11_i64),
        (SECOND_NODE, SECOND_PROPERTY, 22_i64),
    ] {
        let Some(bytes) = db
            .inner_db()
            .get(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
                }
                .to_bytes(),
            )
            .await
            .expect("collision node graph row reads")
        else {
            return false;
        };
        if decode_properties(&bytes).expect("collision node graph row decodes")
            != vec![
                Property::string("$label", NODE_LABEL),
                Property::i64(property, value),
            ]
        {
            return false;
        }
    }
    for (edge_id, property, value) in [
        (FIRST_EDGE, FIRST_PROPERTY, 11_i64),
        (SECOND_EDGE, SECOND_PROPERTY, 22_i64),
    ] {
        let Some(endpoints) = db
            .inner_db()
            .get(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
                }
                .to_bytes(),
            )
            .await
            .expect("collision edge endpoints read")
        else {
            return false;
        };
        let endpoints =
            EdgeEndpointsValue::decode(&endpoints).expect("collision edge endpoints decode");
        if endpoints.source() != FIRST_NODE || endpoints.target() != SECOND_NODE {
            return false;
        }
        let Some(properties) = db
            .inner_db()
            .get(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
                }
                .to_bytes(),
            )
            .await
            .expect("collision edge graph row reads")
        else {
            return false;
        };
        if decode_properties(&properties).expect("collision edge graph row decodes")
            != vec![
                Property::string("$label", EDGE_LABEL),
                Property::i64(property, value),
            ]
        {
            return false;
        }
    }
    true
}

/// Migrates the exact deployed 32-bit property-hash collision while rebuilding
/// every current node/edge equality/range generation from graph truth.
pub async fn v1_property_hash_collision_migration_contract() -> V1CollisionMigrationObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!("v1-property-hash-collision-{}", uuid::Uuid::new_v4());
    let definitions = definitions();
    seed(&database, Arc::clone(&store), &definitions).await;

    let migrated = open(database.clone(), Arc::clone(&store)).await;
    let first = observe(&migrated).await;
    let first_legacy = remaining_legacy_physical_rows(&migrated).await;
    migrated.close().await.expect("collision writer closes");

    let reopened = open(database, store).await;
    let second = observe(&reopened).await;
    let second_legacy = remaining_legacy_physical_rows(&reopened).await;
    let graph_rows_exact = graph_rows_exact(&reopened).await;
    let legacy_catalog_empty = super::legacy_catalog_empty(&reopened).await;
    let readiness_published = crate::migrations::graph_format_v1_ready(
        reopened.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
    )
    .await
    .expect("collision graph readiness reads")
        && crate::migrations::index_v2_migration_ready(
            reopened.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("collision index readiness reads")
        && crate::migrations::storage_schema_complete(
            reopened.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("collision storage readiness reads");
    reopened.close().await.expect("collision reopen closes");

    assert_eq!(first, second, "collision identities change across reopen");
    assert_eq!(
        first_legacy, second_legacy,
        "legacy collision debris changes across cold reopen"
    );
    V1CollisionMigrationObservation {
        exact_legacy_hash_collision: true,
        indexes: second,
        graph_rows_exact,
        legacy_node_equality_ids: second_legacy.0,
        legacy_node_range_ids: second_legacy.1,
        legacy_global_edge_equality_ids: second_legacy.2,
        legacy_global_edge_range_ids: second_legacy.3,
        legacy_catalog_empty,
        readiness_published,
        cold_reopen_identical: true,
    }
}
