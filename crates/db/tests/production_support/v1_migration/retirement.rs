//! Crash-boundary contracts for atomic legacy secondary retirement.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::SecondaryIndexDefinition;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v1::indexes::range::RangeIndexDirection;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, EdgeEndpointsKey, EdgePropertyByIdKey, Key};
use crate::encoding::v1::values::edge_endpoints::EdgeEndpointsValue;
use crate::index_lifecycle::secondary::{
    lookup_active_equality_generation, scan_active_range_generation,
};
use crate::index_lifecycle::{IndexStateV2, ValidatedDynamicIndexDefinition};
use crate::migrations::MigrationFailpoint;

const LABEL: &str = "User";
const PROPERTY: &str = "retirement";
const SOURCE: u64 = 0x0000_0000_0000_0001;
const TARGET: u64 = 0x4100_0000_0000_0001;
const EDGE: u64 = 0xFF00_0000_0000_0001;
const LEGACY_VALUE: &str = "legacy-stale";
const VALUE: i64 = 7;
const FAILPOINT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
enum Family {
    Equality,
    Range,
}

impl Family {
    const ALL: [Self; 2] = [Self::Equality, Self::Range];

    const fn name(self) -> &'static str {
        match self {
            Self::Equality => "equality",
            Self::Range => "range",
        }
    }

    fn definition(self) -> ValidatedDynamicIndexDefinition {
        match self {
            Self::Equality => SecondaryIndexDefinition::edge_equality(LABEL, PROPERTY),
            Self::Range => SecondaryIndexDefinition::edge_range(LABEL, PROPERTY),
        }
        .expect("retirement definition validates")
        .try_into()
        .expect("retirement definition converts")
    }
}

/// Durable evidence observed around one retirement commit boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1RetirementFailpointObservation {
    /// `equality` or `range`.
    pub family: &'static str,
    /// `before` or `after`.
    pub boundary: &'static str,
    /// Whether the exact legacy catalog row remained after interruption.
    pub legacy_catalog_present: bool,
    /// Directional outgoing physical memberships after interruption.
    pub directional_out_ids: Vec<u64>,
    /// Directional incoming physical memberships after interruption.
    pub directional_in_ids: Vec<u64>,
    /// Global physical memberships after interruption.
    pub global_ids: Vec<u64>,
    /// Whether the current record was Active after interruption.
    pub current_active: bool,
    /// Stable logical ID before recovery.
    pub index_id: u64,
    /// Stable physical generation before recovery.
    pub generation: u64,
    /// Whether the exact source graph bytes remained.
    pub graph_preserved: bool,
    /// Whether index readiness was incorrectly published before recovery.
    pub readiness_published: bool,
    /// Whether clean reopen retained the exact current ownership.
    pub ownership_stable_after_reopen: bool,
    /// Authoritative current-index result after clean reopen.
    pub current_ids_after_reopen: Vec<u64>,
    /// Whether clean reopen completed retirement and readiness.
    pub converged_after_reopen: bool,
}

async fn seed(
    database: &str,
    store: Arc<dyn ObjectStore>,
    family: Family,
    definition: &ValidatedDynamicIndexDefinition,
) -> (bytes::Bytes, bytes::Bytes) {
    let raw = super::raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("retirement seed transaction opens");
    let endpoints_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(EDGE)),
    }
    .to_bytes();
    let endpoints_value = EdgeEndpointsValue::new(SOURCE, TARGET).encode();
    transaction
        .put(endpoints_key, endpoints_value)
        .expect("retirement endpoints stage");
    let graph_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(EDGE)),
    }
    .to_bytes();
    let graph_value = encode_properties(&[
        Property::string("$label", LABEL),
        Property::i64(PROPERTY, VALUE),
    ]);
    transaction
        .put(graph_key.clone(), graph_value.clone())
        .expect("retirement graph row stages");
    crate::search::add_to_global_edge_label_index_scoped(
        &transaction,
        LABEL,
        EDGE,
        DataScope::LegacyUnscoped,
    )
    .await
    .expect("retirement edge-label row stages");

    let scoped_property = crate::config::scoped_secondary_index_property(LABEL, PROPERTY);
    match family {
        Family::Equality => {
            crate::search::add_to_edge_equality_index_scoped(
                &transaction,
                SOURCE,
                TARGET,
                EDGE,
                &scoped_property,
                LEGACY_VALUE,
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("retirement equality rows stage");
        }
        Family::Range => {
            crate::search::add_to_edge_range_index_with_direction_scoped(
                &transaction,
                SOURCE,
                TARGET,
                EDGE,
                &scoped_property,
                LEGACY_VALUE,
                RangeIndexDirection::Asc,
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("retirement range rows stage");
        }
    }
    let (catalog_key, catalog_value) =
        crate::migrations::migration_parity_legacy_catalog_row(definition, false)
            .expect("retirement catalog row encodes");
    transaction
        .put(catalog_key.clone(), catalog_value)
        .expect("retirement catalog row stages");
    transaction
        .commit()
        .await
        .expect("retirement seed transaction commits");
    raw.close().await.expect("retirement seed database closes");
    (catalog_key, graph_value)
}

async fn physical_ids(raw: &slatedb::Db, family: Family) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    let scoped_property = crate::config::scoped_secondary_index_property(LABEL, PROPERTY);
    match family {
        Family::Equality => {
            let out = crate::search::lookup_edges_out_by_equality_scoped(
                raw,
                SOURCE,
                &scoped_property,
                LEGACY_VALUE,
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("retirement outgoing equality reads")
            .iter()
            .collect();
            let incoming = crate::search::lookup_edges_in_by_equality_scoped(
                raw,
                TARGET,
                &scoped_property,
                LEGACY_VALUE,
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("retirement incoming equality reads")
            .iter()
            .collect();
            let global = crate::search::lookup_global_edge_equality_index_scoped(
                raw,
                &scoped_property,
                LEGACY_VALUE,
                DataScope::LegacyUnscoped,
            )
            .await
            .expect("retirement global equality reads")
            .iter()
            .collect();
            (out, incoming, global)
        }
        Family::Range => {
            let out = crate::search::scan_edge_range_index_out_prefix_with_direction(
                raw,
                SOURCE,
                &scoped_property,
                RangeIndexDirection::Asc,
            )
            .await
            .expect("retirement outgoing range reads");
            let incoming = crate::search::scan_edge_range_index_in_with_direction(
                raw,
                TARGET,
                &scoped_property,
                crate::search::RangeQuery::Between("", "\u{10ffff}"),
                RangeIndexDirection::Asc,
            )
            .await
            .expect("retirement incoming range reads");
            let global =
                crate::search::scan_global_edge_range_index_all_with_direction_limited_scoped(
                    raw,
                    &scoped_property,
                    RangeIndexDirection::Asc,
                    None,
                    DataScope::LegacyUnscoped,
                )
                .await
                .expect("retirement global range reads");
            (out, incoming, global)
        }
    }
}

async fn active_ownership(
    raw: &slatedb::Db,
    definition: &ValidatedDynamicIndexDefinition,
) -> (bool, u64, u64) {
    let record = crate::index_lifecycle::repository::load_index_record(
        raw,
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("retirement current record reads")
    .expect("retirement current record exists");
    (
        matches!(record.state(), IndexStateV2::Active { .. }),
        record.index_id().get(),
        record.state().generation().get(),
    )
}

async fn run_case(
    family: Family,
    failpoint: MigrationFailpoint,
    boundary: &'static str,
) -> V1RetirementFailpointObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!(
        "v1-retirement-{}-{}-{}",
        family.name(),
        boundary,
        uuid::Uuid::new_v4()
    );
    let definition = family.definition();
    let (catalog_key, graph_value) = seed(&database, Arc::clone(&store), family, &definition).await;
    crate::migrations::inject_migration_failpoint_once(failpoint)
        .expect("retirement failpoint injects");
    let interrupted = tokio::time::timeout(
        FAILPOINT_TIMEOUT,
        crate::HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            super::one_row_config(),
        ),
    )
    .await
    .expect("retirement failpoint writer-open must terminate");
    assert!(
        interrupted.is_err(),
        "{} {} failpoint must interrupt writer-open",
        family.name(),
        boundary
    );
    assert!(
        crate::migrations::migration_failpoint_was_triggered(),
        "{} {} failpoint must trigger",
        family.name(),
        boundary
    );

    let raw = super::raw(&database, Arc::clone(&store)).await;
    let legacy_catalog_present = raw
        .get(&catalog_key)
        .await
        .expect("retirement catalog reads")
        .is_some();
    let (directional_out_ids, directional_in_ids, global_ids) = physical_ids(&raw, family).await;
    let (current_active, index_id, generation) = active_ownership(&raw, &definition).await;
    let graph_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(EDGE)),
    }
    .to_bytes();
    let graph_preserved = raw
        .get(graph_key)
        .await
        .expect("retirement graph row reads")
        .as_ref()
        == Some(&graph_value);
    let readiness_published =
        crate::migrations::index_v2_migration_ready(&raw, DataScope::LegacyUnscoped)
            .await
            .expect("retirement readiness reads");
    raw.close()
        .await
        .expect("retirement interrupted inspection closes");

    let reopened = tokio::time::timeout(
        FAILPOINT_TIMEOUT,
        crate::HelixDB::open_with_object_store_for_migration_parity(
            database,
            store,
            super::one_row_config(),
        ),
    )
    .await
    .expect("retirement recovery writer-open must terminate")
    .expect("retirement recovery converges");
    let (reopened_active, reopened_index_id, reopened_generation) =
        active_ownership(reopened.inner_db().as_ref(), &definition).await;
    let handle = reopened
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| handle.identity() == &definition.identity())
        .expect("retirement current handle is loaded");
    let current_ids_after_reopen = match family {
        Family::Equality => lookup_active_equality_generation(
            reopened.inner_db().as_ref(),
            &handle,
            &PropertyValue::I64(VALUE),
        )
        .await
        .expect("retirement current equality reads")
        .iter()
        .collect(),
        Family::Range => {
            scan_active_range_generation(reopened.inner_db().as_ref(), &handle, None, None)
                .await
                .expect("retirement current range reads")
        }
    };
    let reopened_physical = physical_ids(reopened.inner_db().as_ref(), family).await;
    let converged_after_reopen = reopened_active
        && super::legacy_catalog_empty(&reopened).await
        && reopened_physical.0.is_empty()
        && reopened_physical.1.is_empty()
        && reopened_physical.2.is_empty()
        && crate::migrations::index_v2_migration_ready(
            reopened.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("retirement recovered readiness reads");
    reopened
        .close()
        .await
        .expect("retirement recovered writer closes");

    V1RetirementFailpointObservation {
        family: family.name(),
        boundary,
        legacy_catalog_present,
        directional_out_ids,
        directional_in_ids,
        global_ids,
        current_active,
        index_id,
        generation,
        graph_preserved,
        readiness_published,
        ownership_stable_after_reopen: reopened_index_id == index_id
            && reopened_generation == generation,
        current_ids_after_reopen,
        converged_after_reopen,
    }
}

/// Interrupts every edge-secondary retirement lane immediately before and
/// after its atomic catalog/physical-row commit, then cold-reopens it.
pub async fn v1_secondary_retirement_failpoint_contract() -> Vec<V1RetirementFailpointObservation> {
    let _failpoint_guard =
        crate::migrations::production_contracts::failpoint_contract_guard().await;
    let mut observations = Vec::with_capacity(4);
    for family in Family::ALL {
        observations.push(
            run_case(
                family,
                MigrationFailpoint::LegacyDefinitionRetirementBefore,
                "before",
            )
            .await,
        );
        observations.push(
            run_case(
                family,
                MigrationFailpoint::LegacyDefinitionRetirementAfter,
                "after",
            )
            .await,
        );
    }
    observations
}
