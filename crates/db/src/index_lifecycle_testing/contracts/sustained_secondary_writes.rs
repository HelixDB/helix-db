//! Secondary builds that must activate while public write batches never stop.
//!
//! Each batch inserts fresh rows and moves one seeded row found by a label and
//! key lookup, the shape of an upsert workload. A build used to re-enter
//! catch-up whenever a gated step observed the batch it had waited for; these
//! contracts bound how long activation may take and then compare every key
//! ever written with the Active generation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use helix_ast::expr::Predicate;
use helix_ast::query::QueryRequest;
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};
use serde_json::Value;

use crate::config::{
    SecondaryIndexDefinition, SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexOperationStatus, ValidatedDynamicIndexDefinition,
};
use crate::index_lifecycle_testing::{
    LifecycleTestController, LifecycleTestScheduling, LifecycleWorkTarget,
};
use crate::{DbConfig, HelixDB, HelixDbSource};

use super::{
    assert_build_deltas_empty, assert_equality, assert_identity_active, assert_monotonic_step,
};

const LABEL: &str = "SustainedFlight";
const PROPERTY: &str = "entity_key";
const INSERTS_PER_BATCH: u64 = 4;
/// Explicit steps the build may take while a batch lands before each one.
const MAXIMUM_EXPLICIT_ADVANCES: u64 = 16;
/// Wall-clock bound for activation under the automatic worker, the suite's
/// timeout for automatic work. A build that cannot activate under sustained
/// writes never does, so any finite bound detects it, and this one leaves
/// room for slow and loaded CI hosts.
const AUTOMATIC_ACTIVATION_BOUND: Duration = Duration::from_secs(120);

/// Every key ever written, mapped to the nodes that hold it now.
type ExpectedKeys = BTreeMap<String, BTreeSet<u64>>;

/// Runs explicit and automatic sustained-write convergence.
pub(super) async fn run() {
    run_explicit_batches_between_advances().await;
    run_automatic_worker_under_a_writer_loop().await;
}

/// Commits one public batch before every explicit advance.
async fn run_explicit_batches_between_advances() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-sustained-explicit".to_string(),
        },
        DbConfig::new(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("sustained explicit writer opens");
    let controller = LifecycleTestController::new();
    let scope = DataScope::LegacyUnscoped;
    let (seeds, mut keys) = seed_rows(&db, 32).await;
    let definition = definition();
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("sustained explicit build is accepted")
    else {
        panic!("fresh sustained explicit build must enqueue");
    };
    let target = LifecycleWorkTarget::Operation {
        scope,
        operation_id,
    };

    let mut activated = false;
    for batch in 0..MAXIMUM_EXPLICIT_ADVANCES {
        let ordinal = usize::try_from(batch).expect("explicit batch ordinal fits usize");
        commit_batch(&db, batch, Some((batch, seeds[ordinal])), &mut keys).await;
        let evidence = controller
            .advance(&db, target)
            .await
            .expect("a build step follows every public batch");
        assert_monotonic_step(&evidence);
        let status = db
            .get_index_operation(scope, operation_id)
            .await
            .expect("sustained explicit operation remains readable");
        assert!(
            !matches!(
                status,
                IndexOperationStatus::Blocked { .. } | IndexOperationStatus::Aborted { .. }
            ),
            "sustained writes must not stop the build: {status:?}"
        );
        if matches!(status, IndexOperationStatus::Succeeded { .. }) {
            activated = true;
            break;
        }
    }
    assert!(
        activated,
        "the build must activate within {MAXIMUM_EXPLICIT_ADVANCES} advances while a batch lands before each"
    );

    // The Active generation maintains the next batch itself.
    commit_batch(&db, MAXIMUM_EXPLICIT_ADVANCES, None, &mut keys).await;
    assert_index_matches(&db, scope, &definition, &keys).await;
    db.close().await.expect("sustained explicit writer closes");
}

/// Keeps one writer committing batches while the automatic worker builds.
async fn run_automatic_worker_under_a_writer_loop() {
    let db = HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory {
            database: "index-lifecycle-sustained-automatic".to_string(),
        },
        // Small batches spread the scan and validation over several steps,
        // each racing the writer.
        DbConfig::new().with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default().with_batch_rows(
                SecondaryIndexLifecycleBatchRows::new(64).expect("sustained batch is positive"),
            ),
        ),
        LifecycleTestScheduling::Automatic,
    )
    .await
    .expect("sustained automatic writer opens");
    let controller = LifecycleTestController::new();
    let scope = DataScope::LegacyUnscoped;
    let (seeds, keys) = seed_rows(&db, 256).await;
    let definition = definition();
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            scope,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("sustained automatic build is accepted")
    else {
        panic!("fresh sustained automatic build must enqueue");
    };

    let stop = AtomicBool::new(false);
    let committed = AtomicU64::new(0);
    let writer = async {
        let mut keys = keys;
        let mut batch = 0;
        while !stop.load(Ordering::Acquire) {
            let moved = usize::try_from(batch)
                .ok()
                .and_then(|ordinal| seeds.get(ordinal))
                .map(|&entity_id| (batch, entity_id));
            commit_batch(&db, batch, moved, &mut keys).await;
            batch += 1;
            committed.store(batch, Ordering::Release);
            tokio::task::yield_now().await;
        }
        keys
    };
    let watcher = async {
        let started = Instant::now();
        loop {
            let status = db
                .get_index_operation(scope, operation_id)
                .await
                .expect("sustained automatic operation remains readable");
            match status {
                IndexOperationStatus::Succeeded { .. } => break,
                IndexOperationStatus::Blocked { .. } | IndexOperationStatus::Aborted { .. } => {
                    panic!("sustained writes must not stop the build: {status:?}")
                }
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {}
            }
            assert!(
                started.elapsed() < AUTOMATIC_ACTIVATION_BOUND,
                "the build did not activate within {AUTOMATIC_ACTIVATION_BOUND:?} of sustained writes: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let during_build = committed.load(Ordering::Acquire);
        // Keep writing against the Active generation before stopping.
        let after_activation = during_build + 4;
        while committed.load(Ordering::Acquire) < after_activation {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        stop.store(true, Ordering::Release);
        during_build
    };
    let (keys, during_build) = tokio::join!(writer, watcher);
    assert!(
        during_build >= 2,
        "the writer must keep committing while the build runs, committed {during_build}"
    );
    assert_index_matches(&db, scope, &definition, &keys).await;
    db.close().await.expect("sustained automatic writer closes");
}

fn definition() -> ValidatedDynamicIndexDefinition {
    SecondaryIndexDefinition::node_equality(LABEL, PROPERTY)
        .expect("sustained-write definition validates")
        .try_into()
        .expect("sustained-write definition converts")
}

/// Seeds `count` rows through public writes, which also maintain the label
/// membership the lookups read, before the index exists. Returns each seed's
/// node ID by ordinal and the expected keys.
async fn seed_rows(db: &HelixDB, count: u64) -> (Vec<u64>, ExpectedKeys) {
    let ordinals = (0..count).collect::<Vec<_>>();
    let mut seeds = Vec::with_capacity(ordinals.len());
    for chunk in ordinals.chunks(64) {
        let names = chunk
            .iter()
            .map(|ordinal| format!("seed_{ordinal}"))
            .collect::<Vec<_>>();
        let request =
            chunk
                .iter()
                .zip(&names)
                .fold(batch::write_batch(), |request, (ordinal, name)| {
                    request.var_as(
                        name,
                        traversal::g()
                            .add_n(
                                LABEL,
                                vec![(PROPERTY, PropertyInput::from(format!("seed-{ordinal}")))],
                            )
                            .id(),
                    )
                });
        let response = db
            .query(QueryRequest::write(request.returning(names.clone())))
            .await
            .expect("sustained seed batch commits");
        seeds.extend(names.iter().map(|name| {
            let [entity_id] = ids(&response, name)[..] else {
                panic!("{name} inserts exactly one node");
            };
            entity_id
        }));
    }
    let keys = seeds
        .iter()
        .enumerate()
        .map(|(ordinal, &entity_id)| (format!("seed-{ordinal}"), BTreeSet::from([entity_id])))
        .collect();
    (seeds, keys)
}

/// Commits one public batch: fresh inserts and, when given, a move of the
/// seed with this ordinal and node ID found by a label and key lookup.
async fn commit_batch(
    db: &HelixDB,
    batch: u64,
    moved: Option<(u64, u64)>,
    keys: &mut ExpectedKeys,
) {
    let inserted = (0..INSERTS_PER_BATCH)
        .map(|ordinal| {
            (
                format!("added_{ordinal}"),
                format!("inserted-{batch}-{ordinal}"),
            )
        })
        .collect::<Vec<_>>();
    let request = inserted
        .iter()
        .fold(batch::write_batch(), |request, (name, key)| {
            request.var_as(
                name,
                traversal::g()
                    .add_n(LABEL, vec![(PROPERTY, PropertyInput::from(key.as_str()))])
                    .id(),
            )
        });
    let mut returned = inserted
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let request = match moved {
        Some((ordinal, _)) => {
            returned.push("moved".to_string());
            request.var_as(
                "moved",
                traversal::g()
                    .n_with_label_where(LABEL, Predicate::eq(PROPERTY, format!("seed-{ordinal}")))
                    .set_property(PROPERTY, format!("moved-{batch}"))
                    .id(),
            )
        }
        None => request,
    };
    let request = QueryRequest::write(request.returning(returned));
    let response = loop {
        match db.query(request.clone()).await {
            Ok(response) => break response,
            Err(error) if error.is_transaction_conflict() => tokio::task::yield_now().await,
            Err(error) => panic!("sustained public batch {batch} commits: {error}"),
        }
    };
    for (name, key) in inserted {
        let [entity_id] = ids(&response, &name)[..] else {
            panic!("{name} inserts exactly one node");
        };
        keys.entry(key).or_default().insert(entity_id);
    }
    let Some((ordinal, moved)) = moved else {
        return;
    };
    assert_eq!(
        ids(&response, "moved"),
        vec![moved],
        "the lookup finds exactly the seeded row"
    );
    keys.entry(format!("seed-{ordinal}"))
        .or_default()
        .remove(&moved);
    keys.entry(format!("moved-{batch}"))
        .or_default()
        .insert(moved);
}

/// Reads one projected ID binding, an array or one scalar.
fn ids(response: &Value, variable: &str) -> Vec<u64> {
    match &response[variable] {
        Value::Array(values) => values
            .iter()
            .map(|value| value.as_u64().expect("sustained ID is unsigned"))
            .collect(),
        Value::Number(value) => vec![value.as_u64().expect("sustained ID is unsigned")],
        other @ (Value::Null | Value::Bool(_) | Value::String(_) | Value::Object(_)) => {
            panic!("{variable} is not an ID binding: {other}")
        }
    }
}

/// Proves the Active generation holds every written key exactly.
async fn assert_index_matches(
    db: &HelixDB,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    keys: &ExpectedKeys,
) {
    assert_identity_active(db, scope, definition).await;
    assert_build_deltas_empty(db, scope, definition).await;
    for (key, entity_ids) in keys {
        assert_equality(db, scope, definition, key, entity_ids.iter().copied()).await;
    }
}
