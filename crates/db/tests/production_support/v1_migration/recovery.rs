//! V1 graph/bootstrap crash and cold-reopen recovery contracts.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use crate::config::{
    MigrationBatchRows, MigrationTuning, MigrationWorkerMode, SecondaryIndexDefinition,
    SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning,
};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey as Key, DataKeyKind, NodePropertyKey};
use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey as EdgePropertyPairKey;
use crate::index_lifecycle::secondary::lookup_active_equality_generation;
use crate::index_lifecycle::{IndexStateV2, ValidatedDynamicIndexDefinition};
use crate::migrations::MigrationFailpoint;
use crate::{DbConfig, HelixDB};

const NODE_ID: u64 = 0x4100_0000_0000_0001;
const FROM: u64 = 0x0000_0000_0000_0001;
const TO: u64 = 0xFF00_0000_0000_0001;
const LABEL: &str = "RecoveryUser";
const PROPERTY: &str = "email";
const VALUE: &str = "recover@example.com";
const OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// BatchWriteBefore/After belong only to vector materialization and retirement;
// the existing 64-case vector recovery matrix owns those boundaries.
const BLOCKING_BOUNDARIES: [MigrationFailpoint; 20] = [
    MigrationFailpoint::JobCreationBeforeCommit,
    MigrationFailpoint::JobCreationAfterCommit,
    MigrationFailpoint::AllocatorReservationBefore,
    MigrationFailpoint::AllocatorReservationAfter,
    MigrationFailpoint::BatchReadBefore,
    MigrationFailpoint::BatchReadAfter,
    MigrationFailpoint::BatchCommitBefore,
    MigrationFailpoint::BatchCommitAfter,
    MigrationFailpoint::StageTransitionBefore,
    MigrationFailpoint::StageTransitionAfter,
    MigrationFailpoint::RewriteCompletionBefore,
    MigrationFailpoint::RewriteCompletionAfter,
    MigrationFailpoint::CleanupEnqueueBefore,
    MigrationFailpoint::CleanupEnqueueAfter,
    MigrationFailpoint::LegacyDefinitionEnqueueBefore,
    MigrationFailpoint::LegacyDefinitionEnqueueAfter,
    MigrationFailpoint::MigrationReadyPublicationBefore,
    MigrationFailpoint::MigrationReadyPublicationAfter,
    MigrationFailpoint::StorageSchemaCompletionBefore,
    MigrationFailpoint::StorageSchemaCompletionAfter,
];

const CLEANUP_BOUNDARIES: [MigrationFailpoint; 2] = [
    MigrationFailpoint::CleanupDeleteBefore,
    MigrationFailpoint::CleanupDeleteAfter,
];

/// Durable state observed after one injected crash and a clean cold reopen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V1CrashRecoveryObservation {
    /// Stable typed failpoint name.
    pub failpoint: &'static str,
    /// Whether the requested boundary fired.
    pub triggered: bool,
    /// Whether writer-open or explicit cleanup stepping respected its timeout.
    pub operation_terminated: bool,
    /// Exact authoritative node bytes survived the interruption.
    pub node_source_preserved: bool,
    /// The obsolete legacy edge-pair source remained until clean recovery.
    pub legacy_edge_pair_preserved: bool,
    /// Graph readiness after interruption.
    pub graph_ready_after_failure: bool,
    /// Index readiness after interruption.
    pub index_ready_after_failure: bool,
    /// Storage readiness after interruption.
    pub schema_ready_after_failure: bool,
    /// Whether any pre-existing current ownership survived recovery unchanged.
    pub existing_ownership_stable: bool,
    /// Whether clean recovery activated the definition, retired sources, and
    /// published every readiness marker.
    pub converged_after_reopen: bool,
}

fn definition() -> ValidatedDynamicIndexDefinition {
    SecondaryIndexDefinition::node_equality(LABEL, PROPERTY)
        .expect("crash-recovery definition validates")
        .try_into()
        .expect("crash-recovery definition converts")
}

fn config() -> DbConfig {
    DbConfig::new()
        .with_migration_tuning(
            MigrationTuning::default()
                .with_batch_rows(MigrationBatchRows::new(1).expect("one migration row is positive"))
                .with_worker_mode(MigrationWorkerMode::Disabled),
        )
        .with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_batch_rows(
                SecondaryIndexLifecycleBatchRows::new(1).expect("one secondary row is positive"),
            ),
        )
}

async fn seed(
    database: &str,
    store: Arc<dyn ObjectStore>,
    definition: &ValidatedDynamicIndexDefinition,
) -> (bytes::Bytes, bytes::Bytes, bytes::Bytes) {
    let raw = super::raw(database, store).await;
    let transaction = raw
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("crash-recovery seed transaction opens");
    let node_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(NODE_ID)),
    }
    .to_bytes();
    let node_value = encode_properties(&[
        Property::string("$label", LABEL),
        Property::string(PROPERTY, VALUE),
    ]);
    transaction
        .put(node_key.clone(), node_value.clone())
        .expect("crash-recovery node row stages");
    crate::search::add_to_equality_index_scoped(
        &transaction,
        "$label",
        LABEL,
        NODE_ID,
        DataScope::LegacyUnscoped,
    )
    .await
    .expect("crash-recovery label row stages");

    let legacy_edge_pair_key = Key::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(FROM, TO)),
    }
    .to_bytes();
    transaction
        .put(
            legacy_edge_pair_key.clone(),
            encode_properties(&[
                Property::string("$label", "RECOVERY_EDGE"),
                Property::string("kind", "retained"),
            ]),
        )
        .expect("crash-recovery legacy edge pair stages");
    let (catalog_key, catalog_value) =
        crate::migrations::migration_parity_legacy_catalog_row(definition, false)
            .expect("crash-recovery catalog row encodes");
    transaction
        .put(catalog_key, catalog_value)
        .expect("crash-recovery catalog row stages");
    transaction
        .commit()
        .await
        .expect("crash-recovery seed transaction commits");
    raw.close().await.expect("crash-recovery seed closes");
    (node_key, node_value, legacy_edge_pair_key)
}

async fn ownership(
    raw: &slatedb::Db,
    definition: &ValidatedDynamicIndexDefinition,
) -> Option<(u64, u64)> {
    let record = crate::index_lifecycle::repository::load_index_record(
        raw,
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("crash-recovery current record reads")?;
    Some((record.index_id().get(), record.state().generation().get()))
}

async fn drive_cleanup(db: &HelixDB) {
    for _ in 0..64 {
        if !db
            .process_migration_once()
            .await
            .expect("clean migration step succeeds")
        {
            return;
        }
    }
    panic!("clean migration recovery exceeded the bounded step count");
}

async fn run_case(
    failpoint: MigrationFailpoint,
    cleanup_boundary: bool,
) -> V1CrashRecoveryObservation {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = format!(
        "v1-crash-recovery-{}-{}",
        failpoint.as_str(),
        uuid::Uuid::new_v4()
    );
    let definition = definition();
    let (node_key, node_value, legacy_edge_pair_key) =
        seed(&database, Arc::clone(&store), &definition).await;
    crate::migrations::inject_migration_failpoint_once(failpoint)
        .expect("crash-recovery failpoint injects");
    let opened = tokio::time::timeout(
        OPEN_TIMEOUT,
        HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            config(),
        ),
    )
    .await;
    let mut operation_terminated = opened.is_ok();
    let mut opened_db = match opened {
        Ok(Ok(db)) => Some(db),
        Ok(Err(_)) | Err(_) => None,
    };
    if cleanup_boundary {
        let db = opened_db
            .as_ref()
            .expect("cleanup failpoint is reached after blocking writer bootstrap");
        let stepped = tokio::time::timeout(OPEN_TIMEOUT, async {
            loop {
                match db.process_migration_once().await {
                    Ok(true) => {}
                    Ok(false) => panic!("cleanup failpoint was not reached"),
                    Err(error) => return error,
                }
            }
        })
        .await;
        operation_terminated &= stepped.is_ok();
    }
    let triggered = crate::migrations::migration_failpoint_was_triggered();
    if let Some(db) = opened_db.take() {
        db.close()
            .await
            .expect("interrupted crash-recovery writer closes");
    }

    let inspection = super::raw(&database, Arc::clone(&store)).await;
    let node_source_preserved = inspection
        .get(&node_key)
        .await
        .expect("crash-recovery node source reads")
        .as_ref()
        == Some(&node_value);
    let legacy_edge_pair_preserved = inspection
        .get(&legacy_edge_pair_key)
        .await
        .expect("crash-recovery legacy edge source reads")
        .is_some();
    let graph_ready_after_failure =
        crate::migrations::graph_format_v1_ready(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("crash-recovery graph readiness reads");
    let index_ready_after_failure =
        crate::migrations::index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("crash-recovery index readiness reads");
    let schema_ready_after_failure =
        crate::migrations::storage_schema_complete(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("crash-recovery schema readiness reads");
    let ownership_after_failure = ownership(&inspection, &definition).await;
    inspection
        .close()
        .await
        .expect("crash-recovery inspection closes");

    let recovered = tokio::time::timeout(
        OPEN_TIMEOUT,
        HelixDB::open_with_object_store_for_migration_parity(database, store, config()),
    )
    .await
    .expect("crash-recovery cold open must terminate")
    .expect("crash-recovery cold open converges");
    drive_cleanup(&recovered).await;
    let final_ownership = ownership(recovered.inner_db().as_ref(), &definition).await;
    let record = crate::index_lifecycle::repository::load_index_record(
        recovered.inner_db().as_ref(),
        DataScope::LegacyUnscoped,
        &definition.identity(),
    )
    .await
    .expect("crash-recovery final record reads")
    .expect("crash-recovery final record exists");
    let handle = recovered
        .active_index_handles_loaded(DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| handle.identity() == &definition.identity())
        .expect("crash-recovery final handle loads");
    let served = lookup_active_equality_generation(
        recovered.inner_db().as_ref(),
        &handle,
        &PropertyValue::String(VALUE.to_string()),
    )
    .await
    .expect("crash-recovery final equality reads")
    .iter()
    .collect::<Vec<_>>();
    let converged_after_reopen = matches!(record.state(), IndexStateV2::Active { .. })
        && served == vec![NODE_ID]
        && super::legacy_catalog_empty(&recovered).await
        && recovered
            .inner_db()
            .get(&legacy_edge_pair_key)
            .await
            .expect("crash-recovery final legacy source reads")
            .is_none()
        && crate::migrations::graph_format_v1_ready(
            recovered.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("crash-recovery final graph readiness reads")
        && crate::migrations::index_v2_migration_ready(
            recovered.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("crash-recovery final index readiness reads")
        && crate::migrations::storage_schema_complete(
            recovered.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("crash-recovery final schema readiness reads");
    recovered
        .close()
        .await
        .expect("crash-recovery final writer closes");

    V1CrashRecoveryObservation {
        failpoint: failpoint.as_str(),
        triggered,
        operation_terminated,
        node_source_preserved,
        legacy_edge_pair_preserved,
        graph_ready_after_failure,
        index_ready_after_failure,
        schema_ready_after_failure,
        existing_ownership_stable: ownership_after_failure
            .is_none_or(|ownership| Some(ownership) == final_ownership),
        converged_after_reopen,
    }
}

/// Runs every graph/bootstrap boundary plus both physical-cleanup
/// deletion boundaries through interruption, durable inspection, and reopen.
pub async fn v1_graph_crash_recovery_contract() -> Vec<V1CrashRecoveryObservation> {
    let _failpoint_guard =
        crate::migrations::production_contracts::failpoint_contract_guard().await;
    let mut observations = Vec::with_capacity(BLOCKING_BOUNDARIES.len() + CLEANUP_BOUNDARIES.len());
    for failpoint in BLOCKING_BOUNDARIES {
        observations.push(run_case(failpoint, false).await);
    }
    for failpoint in CLEANUP_BOUNDARIES {
        observations.push(run_case(failpoint, true).await);
    }
    observations
}
