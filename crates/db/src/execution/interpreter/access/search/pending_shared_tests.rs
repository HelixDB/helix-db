//! Read-request searches select pending work from the writer's shared views
//! exactly as a storage read at their own snapshots would, through every race
//! with enqueues, acknowledgements, drains, reopens, and cancellation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use helix_ast::{batch, traversal, value::PropertyInput};
use helix_planner::context::ParamBindings;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::*;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, QueueLayout, TextIndexDefinition, VectorIndexDefinition,
};
use crate::encoding::v2::values::indexes::operation_queue::QueuedOperation;
use crate::index_lifecycle::queue::publication::PublicationOutcome;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

const FAMILIES: [QueueFamily; 2] = [QueueFamily::Vector, QueueFamily::Text];
const CONSISTENCIES: [SearchConsistency; 2] =
    [SearchConsistency::Strong, SearchConsistency::Eventual];

fn definition(family: QueueFamily) -> ValidatedDynamicIndexDefinition {
    match family {
        QueueFamily::Vector => ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
                .unwrap(),
        )
        .unwrap(),
        QueueFamily::Text => ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Doc", "body").unwrap(),
        )
        .unwrap(),
    }
}

/// Opens a writer over `store` with publication paused, installing both
/// indexes when `fresh`.
async fn open(
    name: &str,
    store: Arc<dyn ObjectStore>,
    layout: QueueLayout,
    fresh: bool,
) -> HelixDB {
    let db = HelixDB::open_with_object_store_and_config(
        name,
        store,
        DbConfig::new().with_index_operation_queue_tuning(
            IndexOperationQueueTuning::default()
                .with_layout(layout)
                .with_publication_paused_for_tests(),
        ),
    )
    .await
    .unwrap();
    if fresh {
        for family in FAMILIES {
            db.install_index_for_tests(definition(family))
                .await
                .unwrap();
        }
    }
    db
}

/// A read request's context, its view pinned now.
async fn read_context(db: &HelixDB) -> ExecutionContext<'_> {
    let mut context = ExecutionContext::new(db, ParamBindings::default());
    context.enable_request_read_view().await.unwrap();
    context
}

/// Commits `batch` as one request, retrying conflicts.
async fn commit(db: &HelixDB, batch: impl Fn() -> batch::WriteBatch) {
    for _ in 0..100 {
        let plan = helix_planner::planning::plan_write_batch(
            &batch(),
            &db.planner_context(ParamBindings::default()),
        )
        .unwrap();
        let mut context = ExecutionContext::new(db, ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let executed = context
            .execute_steps(
                plan.steps(),
                plan.execution_order(),
                plan.root(),
                plan.execution_program(),
            )
            .await;
        let committed = match executed {
            Ok(_) => context.commit_request_write_scope().await,
            Err(error) => Err(error),
        };
        match committed {
            Ok(_) => return,
            Err(error) if error.is_transaction_conflict() => {}
            Err(error) => panic!("write failed: {error}"),
        }
    }
    panic!("write kept conflicting")
}

/// Inserts a document indexed by both families at `position`.
async fn insert_at(db: &HelixDB, position: f32) {
    commit(db, || {
        batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![
                    ("embedding", PropertyInput::from(vec![position, 0.0])),
                    ("body", PropertyInput::from(format!("shared {position}"))),
                ],
            ),
        )
    })
    .await;
}

/// Rewrites every document's indexed properties, or drops them all.
async fn rewrite_all(db: &HelixDB, position: f32, drop: bool) {
    commit(db, || {
        let all = traversal::g().n(helix_ast::graph::NodeRef::all());
        batch::write_batch().var_as(
            "changed",
            if drop {
                all.drop()
            } else {
                all.set_property("embedding", vec![position, 1.0])
                    .set_property("body", format!("shared rewritten {position}"))
            },
        )
    })
    .await;
}

/// Publishes one batch of `family`'s generation.
async fn publish(db: &HelixDB, family: QueueFamily) -> PublicationOutcome {
    let target = crate::index_lifecycle::queue::tests::target(db, family).await;
    db.index_queue_publisher()
        .unwrap()
        .publish_once(target)
        .await
        .unwrap()
}

/// Selected pending entities as comparable values: ID and latest state.
type Selected = Vec<(u64, String)>;

fn budget(db: &HelixDB, family: QueueFamily, consistency: SearchConsistency) -> u64 {
    let tuning = db.config().db().index_operation_queue();
    match (consistency, family) {
        (SearchConsistency::Eventual, _) => tuning.eventual_search_budget(),
        (SearchConsistency::Strong, QueueFamily::Vector) => {
            tuning.strong_vector_search_max_pending_bytes().get()
        }
        (SearchConsistency::Strong, QueueFamily::Text) => u64::MAX,
    }
}

/// What `context`'s searches overlay for `family` under `consistency`, and
/// what a storage read at the same snapshot selects.
async fn selections(
    context: &mut ExecutionContext<'_>,
    family: QueueFamily,
    consistency: SearchConsistency,
) -> (Selected, Selected) {
    context.search_consistency = consistency;
    let db = context.db;
    let actual = context
        .pending_selection(&definition(family).identity(), family)
        .await
        .unwrap()
        .map(|selection| {
            selection
                .entities()
                .iter()
                .map(|pending| (pending.entity.id.get(), format!("{:?}", pending.latest)))
                .collect()
        })
        .unwrap_or_default();
    let target = crate::index_lifecycle::queue::tests::target(db, family).await;
    let view = context.request_read_view().unwrap();
    // Read directly, so the reference counts as no search's storage read.
    let reference = db
        .index_queue_store()
        .read_latest(view, target)
        .await
        .unwrap()
        .map(|bytes| {
            bytes
                .decode_latest(budget(db, family, consistency))
                .unwrap()
        })
        .map(|latest| {
            latest
                .into_operations()
                .iter()
                .map(|operation| {
                    (
                        operation.entity().id.get(),
                        format!("{:?}", latest_value(operation.payload())),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    (actual, reference)
}

/// Checks every family and consistency of `context`.
async fn check(context: &mut ExecutionContext<'_>, label: &str) {
    for family in FAMILIES {
        for consistency in CONSISTENCIES {
            let (actual, reference) = selections(context, family, consistency).await;
            assert_eq!(actual, reference, "{label} {family:?} {consistency:?}");
        }
    }
}

/// Searches that read storage since the database opened.
fn fallbacks(db: &HelixDB) -> u64 {
    db.index_operation_queue_stats().search_queue_reads
}

#[tokio::test]
async fn read_views_at_different_snapshots_select_exactly_their_own_pending_work() {
    for layout in [QueueLayout::Map, QueueLayout::Rows] {
        let name = format!("shared-snapshots-{layout:?}");
        let db = open(&name, Arc::new(InMemory::new()), layout, true).await;
        insert_at(&db, 0.0).await;
        let mut older = read_context(&db).await;
        insert_at(&db, 1.0).await;
        // The first search builds each view at its own snapshot: nothing on
        // its targets was recorded above it.
        let mut first = read_context(&db).await;
        check(&mut first, "first").await;
        assert_eq!(fallbacks(&db), 0, "{layout:?}");
        // Enqueues, acknowledgements, and a drain after the build.
        insert_at(&db, 2.0).await;
        let mut second = read_context(&db).await;
        for family in FAMILIES {
            assert!(matches!(
                publish(&db, family).await,
                PublicationOutcome::Published { .. }
            ));
        }
        let mut third = read_context(&db).await;
        rewrite_all(&db, 3.0, false).await;
        let mut fourth = read_context(&db).await;
        for family in FAMILIES {
            while matches!(
                publish(&db, family).await,
                PublicationOutcome::Published { .. }
            ) {}
        }
        let mut drained = read_context(&db).await;
        rewrite_all(&db, 4.0, true).await;
        let mut dropped = read_context(&db).await;
        // Every later snapshot is served by the views, in any order and at
        // once.
        futures::future::join_all(
            [
                (&mut dropped, "dropped"),
                (&mut second, "second"),
                (&mut fourth, "fourth"),
                (&mut drained, "drained"),
                (&mut third, "third"),
            ]
            .into_iter()
            .map(|(context, label)| async move { check(context, label).await }),
        )
        .await;
        assert_eq!(
            fallbacks(&db),
            0,
            "{layout:?}: no later snapshot read storage"
        );
        // A snapshot below the views' bases reads storage, exactly as before:
        // once per queue and consistency.
        check(&mut older, "older").await;
        assert_eq!(fallbacks(&db), 4, "{layout:?}");
        for context in [older, first, second, third, fourth, drained, dropped] {
            let mut context = context;
            context.close_request_read_view().unwrap();
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn views_hold_each_operation_once_however_many_searches_read_them() {
    let db = open(
        "shared-charged-once",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    for position in 0..4 {
        insert_at(&db, position as f32).await;
    }
    let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
    let ledger = db
        .index_operation_backlog()
        .usage(vector.scope, vector.index_id)
        .retained_bytes;
    let mut contexts = Vec::new();
    for _ in 0..8 {
        contexts.push(read_context(&db).await);
    }
    for context in &mut contexts {
        let (actual, reference) =
            selections(context, QueueFamily::Vector, SearchConsistency::Strong).await;
        assert_eq!(actual, reference);
        assert_eq!(actual.len(), 4);
    }
    // Only the vector view exists, and it holds what the ledger charges its
    // index, once.
    assert_eq!(db.index_operation_queue_stats().shared_view_bytes, ledger);
    for context in contexts {
        let mut context = context;
        context.close_request_read_view().unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn write_transactions_read_the_committed_queue_from_storage() {
    let db = open(
        "shared-write-search",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    insert_at(&db, 0.0).await;
    let before = db.index_operation_queue_stats().search_queue_reads;
    commit(&db, || {
        batch::write_batch()
            .var_as(
                "created",
                traversal::g().add_n(
                    "Doc",
                    vec![("embedding", PropertyInput::from(vec![1.0_f32, 0.0]))],
                ),
            )
            .var_as(
                "nearest",
                traversal::g().vector_search_nodes("Doc", "embedding", vec![1.0, 0.0], 2, None),
            )
    })
    .await;
    assert_eq!(
        db.index_operation_queue_stats().search_queue_reads,
        before + 1
    );
    assert_eq!(db.index_operation_queue_stats().shared_view_bytes, 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_reopened_writer_rebuilds_its_views_from_storage() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("shared-reopen", Arc::clone(&store), QueueLayout::Map, true).await;
    insert_at(&db, 0.0).await;
    insert_at(&db, 1.0).await;
    let mut context = read_context(&db).await;
    check(&mut context, "before reopen").await;
    context.close_request_read_view().unwrap();
    assert!(matches!(
        publish(&db, QueueFamily::Vector).await,
        PublicationOutcome::Published { .. }
    ));
    insert_at(&db, 2.0).await;
    db.close().await.unwrap();

    let db = open("shared-reopen", store, QueueLayout::Map, false).await;
    assert_eq!(db.index_operation_queue_stats().shared_view_bytes, 0);
    let mut context = read_context(&db).await;
    check(&mut context, "after reopen").await;
    context.close_request_read_view().unwrap();
    insert_at(&db, 3.0).await;
    let mut context = read_context(&db).await;
    check(&mut context, "after a write since").await;
    assert_eq!(fallbacks(&db), 0, "the reopened views served every search");
    context.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_cancelled_search_never_strands_or_corrupts_the_view() {
    let db = open(
        "shared-cancel",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    for position in 0..64 {
        insert_at(&db, position as f32).await;
    }
    let identity = definition(QueueFamily::Vector).identity();
    let mut cancelled = 0;
    // Searches dropped after `step % 5` polls, so at each point of building
    // or selecting in turn (a build waits at least once, for its decode on
    // the blocking pool); a write after each moves the queue on. Counting
    // polls, not time, keeps the drop points independent of the timer's
    // resolution and the host's load.
    for step in 0..40_u64 {
        let context = read_context(&db).await;
        let mut search = Box::pin(context.pending_selection(&identity, QueueFamily::Vector));
        let mut finished = false;
        for _ in 0..step % 5 {
            if futures::FutureExt::now_or_never(search.as_mut()).is_some() {
                finished = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        if step % 5 > 0 && !finished {
            cancelled += 1;
        }
        drop(search);
        drop(context);
        insert_at(&db, 100.0 + step as f32).await;
        let mut context = read_context(&db).await;
        let (actual, reference) = tokio::time::timeout(
            Duration::from_secs(10),
            selections(&mut context, QueueFamily::Vector, SearchConsistency::Strong),
        )
        .await
        .expect("a cancelled search leaves no build behind");
        assert_eq!(actual, reference, "after step {step}");
        context.close_request_read_view().unwrap();
        // Free the view now and then, so cancellation also lands in builds.
        if step % 3 == 0 {
            let target =
                crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
            db.index_queue_store().shared().free(target);
        }
    }
    assert!(cancelled > 0, "some searches were cancelled");
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn searches_racing_enqueues_acknowledgements_and_drains_see_their_own_snapshots() {
    let db = Arc::new(
        open(
            "shared-race",
            Arc::new(InMemory::new()),
            QueueLayout::Map,
            true,
        )
        .await,
    );
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn({
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        async move {
            let mut position = 0.0_f32;
            while !stop.load(Ordering::Relaxed) {
                position += 1.0;
                match position as u64 % 7 {
                    0 => rewrite_all(&db, position, false).await,
                    6 => rewrite_all(&db, position, true).await,
                    _ => insert_at(&db, position).await,
                }
            }
        }
    });
    let publisher = tokio::spawn({
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        async move {
            while !stop.load(Ordering::Relaxed) {
                for family in FAMILIES {
                    publish(&db, family).await;
                }
                tokio::task::yield_now().await;
            }
        }
    });
    // Searches run until the writer and the publisher both made progress
    // past a few drains.
    let busy = |db: &HelixDB| {
        let stats = db.index_operation_queue_stats();
        stats.published_operations < 200 || stats.committed_operations < 200
    };
    let searchers = (0..4)
        .map(|searcher| {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                let mut round = 0;
                while round < 25 || busy(&db) {
                    round += 1;
                    let mut context = read_context(&db).await;
                    if round % 2 == 0 {
                        tokio::task::yield_now().await;
                    }
                    check(&mut context, &format!("searcher {searcher} round {round}")).await;
                    context.close_request_read_view().unwrap();
                    tokio::task::yield_now().await;
                }
            })
        })
        .collect::<Vec<_>>();
    for searcher in searchers {
        searcher.await.unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
    publisher.await.unwrap();
    assert!(!busy(&db));
    Arc::try_unwrap(db)
        .unwrap_or_else(|_| panic!("the test owns the database"))
        .close()
        .await
        .unwrap();
}

/// Searches every family and consistency once from a fresh request, so each
/// target has a view.
async fn search_all(db: &HelixDB, label: &str) {
    let mut context = read_context(db).await;
    check(&mut context, label).await;
    context.close_request_read_view().unwrap();
}

#[tokio::test]
async fn a_drained_queue_releases_its_views_once_request_snapshots_close() {
    let db = open(
        "shared-drain-idle",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    for position in 0..8 {
        insert_at(&db, position as f32).await;
    }
    let mut pinned = read_context(&db).await;
    check(&mut pinned, "before the drain").await;
    let held = db.index_operation_queue_stats().shared_view_bytes;
    assert!(held > 0);
    for family in FAMILIES {
        while matches!(
            publish(&db, family).await,
            PublicationOutcome::Published { .. }
        ) {}
    }
    // The open snapshot can still see every acknowledged operation.
    assert_eq!(db.index_operation_queue_stats().shared_view_bytes, held);
    check(&mut pinned, "pinned through the drain").await;
    // Nothing commits after the snapshot closes; closing it frees the views.
    pinned.close_request_read_view().unwrap();
    assert_eq!(db.index_operation_queue_stats().shared_view_bytes, 0);
    search_all(&db, "after the drain").await;
    assert_eq!(fallbacks(&db), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_publisher_settles_a_view_an_uncertain_commit_unsettled() {
    let db = open(
        "shared-settle",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    insert_at(&db, 0.0).await;
    insert_at(&db, 1.0).await;
    search_all(&db, "built").await;
    let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
    // A commit whose outcome is unknown, here one that never reached
    // storage, so the ledger holds no uncertain charge of it.
    let mut changes = crate::index_lifecycle::queue::shared::QueueChanges::default();
    changes.acknowledge(vector, &[QueuedOperationId::generate()]);
    db.index_queue_store()
        .shared()
        .begin_commit(changes)
        .uncertain();
    let before = fallbacks(&db);
    search_all(&db, "unsettled").await;
    assert_eq!(
        fallbacks(&db),
        before + 2,
        "the vector searches read storage"
    );
    // Any attempt of the target settles it, and the next search rebuilds.
    assert!(matches!(
        publish(&db, QueueFamily::Vector).await,
        PublicationOutcome::Published { .. }
    ));
    let before = fallbacks(&db);
    search_all(&db, "settled").await;
    assert_eq!(fallbacks(&db), before, "the rebuilt view served them");
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_producer_commit_that_fails_otherwise_than_by_a_conflict_unsettles_its_views() {
    let db = open(
        "shared-producer-failure",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    insert_at(&db, 0.0).await;
    search_all(&db, "built").await;
    let mut targets = Vec::new();
    for family in FAMILIES {
        targets.push(crate::index_lifecycle::queue::tests::target(&db, family).await);
    }
    // A write whose graph changes and queue operands are staged when its
    // storage closes underneath it.
    let plan = helix_planner::planning::plan_write_batch(
        &batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![
                    ("embedding", PropertyInput::from(vec![5.0_f32, 0.0])),
                    ("body", PropertyInput::from("shared 5".to_string())),
                ],
            ),
        ),
        &db.planner_context(ParamBindings::default()),
    )
    .unwrap();
    let mut context = ExecutionContext::new(&db, ParamBindings::default());
    context.enable_request_write_scope().await.unwrap();
    context
        .execute_steps(
            plan.steps(),
            plan.execution_order(),
            plan.root(),
            plan.execution_program(),
        )
        .await
        .unwrap();
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        unreachable!("a writer");
    };
    writer.db().close().await.unwrap();
    let error = context.commit_request_write_scope().await.unwrap_err();
    assert!(!error.is_transaction_conflict(), "{error}");
    // It may have applied, so both views read storage until reconciled.
    let shared = db.index_queue_store().shared();
    for target in targets {
        assert!(matches!(
            futures::FutureExt::now_or_never(shared.plan(target, u64::MAX)),
            Some(crate::index_lifecycle::queue::shared::Plan::Fallback)
        ));
    }
    // The closed storage cannot close again, so the database is dropped.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_whose_search_is_cancelled_still_installs_its_view_for_the_next_search() {
    let db = open(
        "shared-cancelled-build",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    for position in 0..4 {
        insert_at(&db, position as f32).await;
    }
    let shared = Arc::clone(db.index_queue_store().shared());
    let (reached, reached_rx) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    shared.gate_next_decode(reached, release_rx);
    let builds = shared.builds();
    let identity = definition(QueueFamily::Vector).identity();
    let context = read_context(&db).await;
    let mut search = Box::pin(context.pending_selection(&identity, QueueFamily::Vector));
    tokio::select! {
        _ = &mut search => panic!("the build's decode is held"),
        reached = tokio::task::spawn_blocking(move || reached_rx.recv()) => reached.unwrap().unwrap(),
    }
    // The search is dropped while its build decodes.
    drop(search);
    drop(context);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while db.index_operation_queue_stats().shared_view_bytes == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the abandoned search's build installs its view");
    let mut context = read_context(&db).await;
    let (actual, reference) =
        selections(&mut context, QueueFamily::Vector, SearchConsistency::Strong).await;
    assert_eq!(actual, reference);
    assert_eq!(actual.len(), 4);
    context.close_request_read_view().unwrap();
    assert_eq!(shared.builds(), builds + 1, "no second build");
    assert_eq!(fallbacks(&db), 0);
    db.close().await.unwrap();
}

/// Publishes `family` until a publication publishes nothing.
async fn drain(db: &HelixDB, family: QueueFamily) {
    while matches!(
        publish(db, family).await,
        PublicationOutcome::Published { .. }
    ) {}
}

/// Queue keys publications deleted because they drained them ("Delete on
/// drain" on `QueueStore::stage_acknowledge`).
fn drained_deletes(db: &HelixDB) -> u64 {
    db.index_operation_queue_stats().drained_queue_deletes
}

/// Holds `db`'s next publication after it staged its writes, until the
/// returned sender releases it; the receiver reports it reached the hold.
fn hold_next_publication(
    db: &HelixDB,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *db.index_queue_publisher()
        .unwrap()
        .hooks()
        .before_commit
        .lock() = Some((reached_tx, release_rx));
    (reached_rx, release_tx)
}

/// The kind of the newest stored entry of `family`'s queue key.
async fn newest(
    db: &HelixDB,
    family: QueueFamily,
) -> Option<crate::index_lifecycle::queue::drain_delete_tests::Stored> {
    let target = crate::index_lifecycle::queue::tests::target(db, family).await;
    crate::index_lifecycle::queue::drain_delete_tests::newest(&db.inner_db(), &target.key()).await
}

#[tokio::test]
async fn a_draining_delete_leaves_older_snapshots_their_operations_and_later_ones_none() {
    use crate::index_lifecycle::queue::drain_delete_tests::Stored;
    for layout in [QueueLayout::Map, QueueLayout::Rows] {
        let deletes = layout == QueueLayout::Map;
        let name = format!("shared-drain-delete-{layout:?}");
        let db = Arc::new(open(&name, Arc::new(InMemory::new()), layout, true).await);
        for position in 0..3 {
            insert_at(&db, position as f32).await;
        }
        let mut before = read_context(&db).await;
        check(&mut before, "before the drain").await;
        // The draining publication waits to commit, its writes staged, while
        // a request takes its snapshot below the delete.
        let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
        let (reached, release) = hold_next_publication(&db);
        let publishing = tokio::spawn({
            let db = Arc::clone(&db);
            async move {
                db.index_queue_publisher()
                    .unwrap()
                    .publish_once(vector)
                    .await
                    .unwrap()
            }
        });
        reached.await.unwrap();
        let mut staged = read_context(&db).await;
        release.send(()).unwrap();
        assert!(matches!(
            publishing.await.unwrap(),
            PublicationOutcome::Published { operations: 3, .. }
        ));
        assert_eq!(drained_deletes(&db), u64::from(deletes), "{layout:?}");
        drain(&db, QueueFamily::Text).await;
        assert_eq!(drained_deletes(&db), 2 * u64::from(deletes), "{layout:?}");
        if deletes {
            for family in FAMILIES {
                assert_eq!(newest(&db, family).await, Some(Stored::Tombstone));
            }
        }
        let mut drained = read_context(&db).await;
        // A later enqueue lands above the tombstones.
        insert_at(&db, 9.0).await;
        let mut later = read_context(&db).await;
        let (selected, _) =
            selections(&mut staged, QueueFamily::Vector, SearchConsistency::Strong).await;
        assert_eq!(selected.len(), 3, "the snapshot below the delete sees all");
        let (selected, _) =
            selections(&mut drained, QueueFamily::Vector, SearchConsistency::Strong).await;
        assert!(
            selected.is_empty(),
            "the snapshot above the delete sees none"
        );
        let (selected, _) =
            selections(&mut later, QueueFamily::Vector, SearchConsistency::Strong).await;
        assert_eq!(selected.len(), 1, "the later enqueue is never hidden");
        // Every snapshot, in any order and at once, from the views.
        futures::future::join_all(
            [
                (&mut later, "later"),
                (&mut before, "before"),
                (&mut drained, "drained"),
                (&mut staged, "staged"),
            ]
            .into_iter()
            .map(|(context, label)| async move { check(context, label).await }),
        )
        .await;
        assert_eq!(
            fallbacks(&db),
            0,
            "{layout:?}: the views served every search"
        );
        for context in [before, staged, drained, later] {
            let mut context = context;
            context.close_request_read_view().unwrap();
        }
        // With no snapshot left below the deletes, the views hold exactly
        // what the ledger charges: the later enqueue.
        let mut charged = 0;
        for family in FAMILIES {
            let target = crate::index_lifecycle::queue::tests::target(&db, family).await;
            charged += db
                .index_operation_backlog()
                .usage(target.scope, target.index_id)
                .retained_bytes;
        }
        assert!(charged > 0);
        assert_eq!(
            db.index_operation_queue_stats().shared_view_bytes,
            charged,
            "{layout:?}"
        );
        Arc::into_inner(db).unwrap().close().await.unwrap();
    }
}

#[tokio::test]
async fn an_enqueue_racing_a_draining_delete_is_never_hidden_and_nothing_removed_returns() {
    let db = Arc::new(
        open(
            "shared-drain-race",
            Arc::new(InMemory::new()),
            QueueLayout::Map,
            true,
        )
        .await,
    );
    insert_at(&db, 0.0).await;
    search_all(&db, "built").await;
    let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;

    // An enqueue that commits while the draining delete waits to commit
    // aborts it: nothing of the attempt is recorded.
    let (reached, release) = hold_next_publication(&db);
    let publishing = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            db.index_queue_publisher()
                .unwrap()
                .publish_once(vector)
                .await
                .unwrap()
        }
    });
    reached.await.unwrap();
    let mut waiting = read_context(&db).await;
    insert_at(&db, 1.0).await;
    let mut enqueued = read_context(&db).await;
    release.send(()).unwrap();
    assert_eq!(publishing.await.unwrap(), PublicationOutcome::Retry);
    assert_eq!(drained_deletes(&db), 0);
    let mut aborted = read_context(&db).await;
    for (context, label, count) in [
        (&mut waiting, "below the enqueue", 1),
        (&mut enqueued, "above the enqueue", 2),
        (&mut aborted, "after the aborted delete", 2),
    ] {
        let (selected, _) =
            selections(context, QueueFamily::Vector, SearchConsistency::Strong).await;
        assert_eq!(selected.len(), count, "{label}");
        check(context, label).await;
    }
    // The retry removes what it read before the enqueue, which the ledger
    // still charges; the attempt after it drains the queue and deletes it.
    drain(&db, QueueFamily::Vector).await;
    assert_eq!(drained_deletes(&db), 1);
    let mut deleted = read_context(&db).await;
    check(&mut deleted, "after the delete").await;

    // A producer whose transaction began before the next draining delete
    // committed commits after it, above the tombstone, recorded at its own
    // sequence.
    insert_at(&db, 2.0).await;
    let entity = db
        .index_queue_store()
        .read_latest(&*db.inner_db(), vector)
        .await
        .unwrap()
        .unwrap()
        .decode_all()
        .unwrap()
        .1[0]
        .entity()
        .id
        .get();
    search_all(&db, "before the producer").await;
    let producer = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let late = crate::index_lifecycle::queue::recovery_tests::vector(entity, &[7.0, 7.0]);
    let mut changes = crate::index_lifecycle::queue::shared::QueueChanges::default();
    db.index_queue_store()
        .stage_enqueue(
            &producer,
            &mut changes,
            vector,
            crate::encoding::v2::values::indexes::operation_queue::QueueOperand::enqueue(
                std::slice::from_ref(&late),
            )
            .unwrap(),
            std::slice::from_ref(&late),
        )
        .unwrap();
    assert!(matches!(
        publish(&db, QueueFamily::Vector).await,
        PublicationOutcome::Published { operations: 1, .. }
    ));
    assert_eq!(drained_deletes(&db), 2);
    let mut between = read_context(&db).await;
    crate::index_lifecycle::queue::shared::commit_recorded(
        producer,
        db.index_queue_store().shared().begin_commit(changes),
    )
    .await
    .expect("a draining delete never aborts a producer");
    let mut above = read_context(&db).await;
    for (context, label, count) in [
        (&mut between, "between the delete and the producer", 0),
        (&mut above, "above the producer", 1),
    ] {
        let (selected, _) =
            selections(context, QueueFamily::Vector, SearchConsistency::Strong).await;
        assert_eq!(selected.len(), count, "{label}");
        check(context, label).await;
    }
    for context in [&mut waiting, &mut enqueued, &mut aborted, &mut deleted] {
        check(context, "an older snapshot, read again").await;
    }
    assert_eq!(fallbacks(&db), 0, "the views served every search");
    for context in [waiting, enqueued, aborted, deleted, between, above] {
        let mut context = context;
        context.close_request_read_view().unwrap();
    }
    Arc::into_inner(db).unwrap().close().await.unwrap();
}

#[tokio::test]
async fn an_uncertain_draining_delete_unsettles_its_view_until_reconciled() {
    let db = open(
        "shared-uncertain-delete",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    insert_at(&db, 0.0).await;
    search_all(&db, "built").await;
    let hooks = db.index_queue_publisher().unwrap().hooks();
    // A draining text delete whose commit never reached storage, reported
    // uncertain: its view reads storage until the publisher reconciles it.
    hooks.uncertain_without_commit.store(true, Ordering::SeqCst);
    assert_eq!(
        publish(&db, QueueFamily::Text).await,
        PublicationOutcome::Retry
    );
    let before = fallbacks(&db);
    search_all(&db, "unsettled").await;
    assert_eq!(fallbacks(&db), before + 2, "the text searches read storage");
    // Reconciliation finds the operation still queued; the next attempt
    // deletes the queue and settles the view, which the next search rebuilds.
    drain(&db, QueueFamily::Text).await;
    assert_eq!(drained_deletes(&db), 1);
    let before = fallbacks(&db);
    search_all(&db, "settled").await;
    assert_eq!(fallbacks(&db), before, "the rebuilt view served them");
    // A draining delete that committed but was reported uncertain: the view
    // recorded the commit, so it keeps serving, and reconciliation counts the
    // delete once.
    insert_at(&db, 1.0).await;
    search_all(&db, "enqueued").await;
    hooks.uncertain_after_commit.store(true, Ordering::SeqCst);
    assert_eq!(
        publish(&db, QueueFamily::Text).await,
        PublicationOutcome::Retry
    );
    search_all(&db, "committed, reported uncertain").await;
    publish(&db, QueueFamily::Text).await;
    assert_eq!(drained_deletes(&db), 2);
    search_all(&db, "reconciled").await;
    assert_eq!(fallbacks(&db), before, "the view served every later search");
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_first_search_after_an_open_that_tombstones_drained_queues_builds_from_absence() {
    use crate::index_lifecycle::queue::drain_delete_tests::Stored;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "shared-open-tombstone",
        Arc::clone(&store),
        QueueLayout::Map,
        true,
    )
    .await;
    insert_at(&db, 0.0).await;
    insert_at(&db, 1.0).await;
    // What an earlier version left: queues drained by removal operands.
    for family in FAMILIES {
        let target = crate::index_lifecycle::queue::tests::target(&db, family).await;
        let (_, operations) = db
            .index_queue_store()
            .read_latest(&*db.inner_db(), target)
            .await
            .unwrap()
            .unwrap()
            .decode_all()
            .unwrap();
        let (bytes, tokens) =
            crate::encoding::v2::values::indexes::operation_queue::QueueOperand::acknowledge(
                family,
                operations.iter().map(QueuedOperation::id),
            )
            .unwrap()
            .into_parts();
        let transaction = db
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        transaction
            .merge_disjoint_tokens(target.key(), tokens, bytes)
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(newest(&db, family).await, Some(Stored::Merge));
    }
    db.close().await.unwrap();

    // The open tombstones them before any search, so no view records them.
    let db = open(
        "shared-open-tombstone",
        Arc::clone(&store),
        QueueLayout::Map,
        false,
    )
    .await;
    assert_eq!(
        db.index_operation_queue_stats()
            .open_drained_queue_tombstones,
        2
    );
    let shared = Arc::clone(db.index_queue_store().shared());
    assert!(!shared.searched());
    for family in FAMILIES {
        assert_eq!(newest(&db, family).await, Some(Stored::Tombstone));
    }
    // The first search builds each view from the absent key.
    search_all(&db, "first search").await;
    assert!(shared.searched());
    let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
    assert!(shared.holds_view(vector));
    insert_at(&db, 2.0).await;
    let mut enqueued = read_context(&db).await;
    check(&mut enqueued, "after an enqueue").await;
    for family in FAMILIES {
        drain(&db, family).await;
    }
    assert_eq!(drained_deletes(&db), 2);
    search_all(&db, "after the draining deletes").await;
    check(&mut enqueued, "after an enqueue, read again").await;
    enqueued.close_request_read_view().unwrap();
    assert_eq!(fallbacks(&db), 0);
    insert_at(&db, 3.0).await;
    db.close().await.unwrap();

    // A reopen finds nothing drained to tombstone; its views rebuild.
    let db = open("shared-open-tombstone", store, QueueLayout::Map, false).await;
    assert_eq!(
        db.index_operation_queue_stats()
            .open_drained_queue_tombstones,
        0
    );
    search_all(&db, "after the reopen").await;
    for family in FAMILIES {
        drain(&db, family).await;
    }
    assert_eq!(drained_deletes(&db), 2);
    search_all(&db, "drained after the reopen").await;
    assert_eq!(fallbacks(&db), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_retired_generation_frees_its_view_on_its_next_attempt() {
    let db = open(
        "shared-retired",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    insert_at(&db, 0.0).await;
    search_all(&db, "built").await;
    let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
    let shared = db.index_queue_store().shared();
    assert!(shared.holds_view(vector));
    let dropped = Box::pin(
        db.query(helix_ast::query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "dropped",
                    traversal::g().drop_index(helix_ast::index::IndexSpec::node_vector(
                        "Doc",
                        "embedding",
                        std::num::NonZeroUsize::new(2).unwrap(),
                        helix_ast::index::VectorDistanceMetric::Euclidean,
                        None::<&str>,
                    )),
                )
                .returning(["dropped"]),
        )),
    )
    .await;
    dropped.unwrap();
    // An open snapshot keeps a drained view's history, so only retirement
    // frees it here.
    let mut pinned = read_context(&db).await;
    db.index_queue_publisher()
        .unwrap()
        .publish_once(vector)
        .await
        .unwrap();
    assert!(
        !shared.holds_view(vector),
        "a retired generation's view is freed"
    );
    pinned.close_request_read_view().unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_view_a_request_selected_from_is_kept_and_shared_until_the_request_ends() {
    let db = open(
        "shared-leased",
        Arc::new(InMemory::new()),
        QueueLayout::Map,
        true,
    )
    .await;
    for position in 0..4 {
        insert_at(&db, position as f32).await;
    }
    let vector = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Vector).await;
    let shared = Arc::clone(db.index_queue_store().shared());
    let mut holding = read_context(&db).await;
    let (actual, reference) =
        selections(&mut holding, QueueFamily::Vector, SearchConsistency::Strong).await;
    assert_eq!((actual.len(), &actual), (4, &reference));
    // The request keeps the payloads it selected, and with them the view,
    // after its search returned: eviction cannot free it.
    assert_eq!(shared.leases(vector), (1, 0));
    shared.evict_idle();
    assert!(shared.holds_view(vector));
    // Another request shares that view instead of decoding a second copy.
    let builds = shared.builds();
    let mut next = read_context(&db).await;
    let (actual, reference) =
        selections(&mut next, QueueFamily::Vector, SearchConsistency::Strong).await;
    assert_eq!((actual.len(), &actual), (4, &reference));
    assert_eq!(shared.builds(), builds);
    assert_eq!(shared.leases(vector), (2, 0));
    // An empty selection keeps no lease.
    let text = crate::index_lifecycle::queue::tests::target(&db, QueueFamily::Text).await;
    while matches!(
        publish(&db, QueueFamily::Text).await,
        PublicationOutcome::Published { .. }
    ) {}
    let mut drained = read_context(&db).await;
    let (actual, _) = selections(&mut drained, QueueFamily::Text, SearchConsistency::Strong).await;
    assert!(actual.is_empty());
    assert_eq!(shared.leases(text), (0, 0));
    // Each request lets go when it ends; then the view can be evicted.
    for context in [next, drained] {
        let mut context = context;
        context.close_request_read_view().unwrap();
    }
    assert_eq!(shared.leases(vector), (1, 0));
    holding.close_request_read_view().unwrap();
    drop(holding);
    assert_eq!(shared.leases(vector), (0, 0));
    shared.evict_idle();
    assert!(!shared.holds_view(vector));
    db.close().await.unwrap();
}
