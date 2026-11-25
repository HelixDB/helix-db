//! Delete on drain: an acknowledgement that drains its queue deletes the key.
//!
//! Each test checks the stored entries themselves, newest first, besides the
//! queue a read resolves: the point of the delete is that nothing below the
//! tombstone is merged again.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IsolationLevel;

use super::publication::PublicationOutcome;
use super::publication_tests::{install_vector, publisher};
use super::recovery_tests::{enqueue, ledger, queue_target, raw_db, text, vector};
use super::storage::{QueueStore, StagedAcknowledgement};
use super::tests::{add_doc, add_text, install_text, open, publisher_with_limits, queue, queued};
use super::QueueTarget;
use crate::config::{DbConfig, IndexOperationQueueTuning, QueueLayout, SearchIndexBatchLimits};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueueOperand, QueuedOperation,
};
use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
use crate::HelixDB;

/// The kind of one stored entry of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Stored {
    Value,
    Merge,
    Tombstone,
}

/// Returns the kind of `key`'s newest stored entry across every source, or
/// `None` when no source holds the key.
pub(super) async fn newest(db: &slatedb::Db, key: &[u8]) -> Option<Stored> {
    history(db, key).await.first().copied()
}

/// Every stored entry of `key` across every source, newest first.
pub(super) async fn history(db: &slatedb::Db, key: &[u8]) -> Vec<Stored> {
    let mut found = Vec::new();
    let mut entries = db.scan_prefix_by_recency(key).await.unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.key.as_ref() != key {
            continue;
        }
        found.push(match entry.value {
            slatedb::ValueDeletable::Value(_) => Stored::Value,
            slatedb::ValueDeletable::Merge(_) => Stored::Merge,
            slatedb::ValueDeletable::Tombstone => Stored::Tombstone,
        });
    }
    found
}

/// Entity IDs of `family`'s queued operations, in queue order.
async fn queued_ids(db: &HelixDB, family: QueueFamily) -> Vec<u64> {
    queue(db, family).await.map_or_else(Vec::new, |queue| {
        queue
            .into_operations()
            .iter()
            .map(|operation| operation.entity().id.get())
            .collect()
    })
}

fn deletes(db: &HelixDB) -> u64 {
    db.index_operation_queue_stats().drained_queue_deletes
}

fn published(operations: u64) -> PublicationOutcome {
    PublicationOutcome::Published {
        operations,
        entities: operations,
    }
}

/// A drained acknowledgement tombstones the key; work queued later merges
/// above the tombstone, resolves without anything below it, and reopens
/// exactly as live admission charged it.
#[tokio::test]
async fn a_drained_vector_acknowledgement_deletes_the_key_and_later_work_resolves_above_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = || queued(IndexOperationQueueTuning::default());
    let db = open("drain-vector", Arc::clone(&store), config()).await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Vector).await;
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(2)
    );
    assert_eq!(deletes(&db), 1);
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    // Later work lands above the tombstone and resolves alone.
    let later = add_doc(&db, vec![5.0, 5.0], "c").await.unwrap();
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [later]);
    db.close().await.unwrap();

    let db = open("drain-vector", Arc::clone(&store), config()).await;
    let usage = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, target.index_id);
    assert_eq!(
        usage.operations, 1,
        "only the operation above the tombstone"
    );
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [later]);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(1)
    );
    assert_eq!(deletes(&db), 1);
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    db.close().await.unwrap();

    let db = open("drain-vector", store, config()).await;
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        Default::default()
    );
    db.close().await.unwrap();
}

/// Text publication deletes a drained queue the same way, and a partial
/// acknowledgement still merges a removal.
#[tokio::test]
async fn text_publication_deletes_only_a_drained_queue() {
    let db = open(
        "drain-text",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_text(&db).await;
    for body in ["alpha", "beta", "gamma"] {
        add_text(&db, body).await.unwrap();
    }
    let target = super::tests::target(&db, QueueFamily::Text).await;
    let one_entity = publisher_with_limits(
        &db,
        one_entity_limits(),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    assert_eq!(one_entity.publish_once(target).await.unwrap(), published(1));
    assert_eq!(
        one_entity
            .metrics()
            .drained_queue_deletes
            .load(Ordering::Relaxed),
        0,
        "two operations remain"
    );
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(2)
    );
    assert_eq!(deletes(&db), 1);
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    assert!(queue(&db, QueueFamily::Text).await.is_none());
    db.close().await.unwrap();
}

/// An acknowledgement that does not drain its queue merges a removal: when
/// its commit lands but is reported uncertain, reconciliation counts no
/// delete.
#[tokio::test]
async fn an_uncertain_removal_that_committed_is_not_counted_as_a_delete() {
    let db = open(
        "drain-uncertain-removal",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_text(&db).await;
    for body in ["alpha", "beta"] {
        add_text(&db, body).await.unwrap();
    }
    let target = super::tests::target(&db, QueueFamily::Text).await;
    let one_entity = publisher_with_limits(
        &db,
        one_entity_limits(),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    one_entity
        .hooks()
        .uncertain_after_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        one_entity.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(one_entity.publish_once(target).await.unwrap(), published(1));
    assert_eq!(
        one_entity
            .metrics()
            .drained_queue_deletes
            .load(Ordering::Relaxed),
        1,
        "only the second acknowledgement deleted the key"
    );
    db.close().await.unwrap();
}

/// Limits that select one entity per publication.
fn one_entity_limits() -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::new(1 << 30).unwrap(),
        NonZeroU64::new(1 << 20).unwrap(),
        NonZeroU64::new(8 << 20).unwrap(),
        NonZeroU64::new(8 << 20).unwrap(),
    )
    .unwrap()
}

/// An enqueue that commits after the acknowledging transaction began (here
/// while it waits to commit) conflicts with the delete, so nothing of the
/// attempt commits and the enqueue is kept.
#[tokio::test]
async fn an_enqueue_committed_while_the_acknowledgement_waits_aborts_the_delete() {
    let db = Arc::new(
        open(
            "drain-race",
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()),
        )
        .await,
    );
    install_vector(&db, None).await;
    let first = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Vector).await;
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *publisher(&db).hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let publishing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { publisher(&db).publish_once(target).await })
    };
    reached_rx.await.unwrap();
    let second = add_doc(&db, vec![5.0, 5.0], "b").await.unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(
        publishing.await.unwrap().unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(db.index_operation_queue_stats().commit_conflicts, 1);
    assert_eq!(deletes(&db), 0);
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [first, second]);
    // The retry continues from the queue read before the second enqueue, so
    // the ledger still charges the second and nothing is deleted.
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(1)
    );
    assert_eq!(deletes(&db), 0);
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [second]);
    // The next attempt reads the second, drains the queue, and deletes it.
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(1)
    );
    assert_eq!(deletes(&db), 1);
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        Default::default()
    );
    Arc::into_inner(db).unwrap().close().await.unwrap();
}

/// A producer whose transaction began before a draining acknowledgement
/// committed still commits after it: the delete aborts only its own
/// transaction, and the enqueue lands above the tombstone.
#[tokio::test]
async fn an_enqueue_begun_before_the_delete_committed_still_commits_above_it() {
    let db = open(
        "drain-producer",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let first = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Vector).await;
    let producer = db
        .inner_db()
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let late = vector(first + 1_000, &[3.0, 3.0]);
    QueueStore::new(QueueLayout::Map, u64::MAX, 0)
        .stage_enqueue(
            &producer,
            target,
            QueueOperand::enqueue(std::slice::from_ref(&late)).unwrap(),
            std::slice::from_ref(&late),
        )
        .unwrap();
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(1)
    );
    assert_eq!(deletes(&db), 1);
    producer
        .commit()
        .await
        .expect("a draining delete never aborts a producer");
    assert_eq!(
        history(&db.inner_db(), &target.key()).await[..2],
        [Stored::Merge, Stored::Tombstone]
    );
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [first + 1_000]);
    db.close().await.unwrap();
}

/// A publication that continues from a queue read before a later enqueue
/// committed (its retained queue) names every operation of that queue but
/// not the later one, which the ledger still charges: it never deletes.
#[tokio::test]
async fn a_stale_retained_read_never_deletes_an_operation_it_did_not_see() {
    let db = open(
        "drain-stale",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Vector).await;
    let one_entity = publisher_with_limits(
        &db,
        one_entity_limits(),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let deletes = || {
        one_entity
            .metrics()
            .drained_queue_deletes
            .load(Ordering::Relaxed)
    };
    // Reads both, publishes the first, and retains the second.
    assert_eq!(one_entity.publish_once(target).await.unwrap(), published(1));
    let third = add_doc(&db, vec![2.0, 2.0], "c").await.unwrap();
    // Publishes the retained second: every operation of its queue, but the
    // third is charged and unnamed.
    assert_eq!(one_entity.publish_once(target).await.unwrap(), published(1));
    assert_eq!(deletes(), 0, "the ledger still charged the third");
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [third]);
    assert_eq!(one_entity.publish_once(target).await.unwrap(), published(1));
    assert_eq!(deletes(), 1);
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    db.close().await.unwrap();
}

/// A discard that drains a retired generation's queue deletes its key too.
#[tokio::test]
async fn a_discard_that_drains_a_retired_queue_deletes_its_key() {
    let db = open(
        "drain-discard",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_text(&db).await;
    add_text(&db, "alpha").await.unwrap();
    add_text(&db, "beta").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Text).await;
    let dropped = super::lifecycle_tests::drop_index(
        &db,
        helix_ast::index::IndexSpec::node_text("Doc", "body", None::<&str>),
    )
    .await
    .expect("dropping an active index runs an operation");
    assert_eq!(
        super::lifecycle_tests::wait_terminal(&db, &dropped).await,
        "succeeded"
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Discarded { operations: 2 }
    );
    assert_eq!(deletes(&db), 1);
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

/// A draining discard whose commit landed but was reported uncertain is
/// counted as a delete once reconciliation finds the key absent.
#[tokio::test]
async fn an_uncertain_discard_delete_counts_once_reconciled() {
    let db = open(
        "drain-discard-uncertain",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_text(&db).await;
    add_text(&db, "alpha").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Text).await;
    let dropped = super::lifecycle_tests::drop_index(
        &db,
        helix_ast::index::IndexSpec::node_text("Doc", "body", None::<&str>),
    )
    .await
    .expect("dropping an active index runs an operation");
    assert_eq!(
        super::lifecycle_tests::wait_terminal(&db, &dropped).await,
        "succeeded"
    );
    publisher(&db)
        .hooks()
        .uncertain_after_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(deletes(&db), 0, "an uncertain delete is not counted yet");
    publisher(&db).publish_once(target).await.unwrap();
    assert_eq!(deletes(&db), 1, "reconciliation found the delete committed");
    assert!(db
        .index_operation_backlog()
        .outstanding_targets()
        .is_empty());
    db.close().await.unwrap();
}

/// A draining acknowledgement whose commit succeeded but was reported
/// uncertain reconciles as acknowledged: the flushed read finds the key
/// absent, which is how it reads a removal that committed.
#[tokio::test]
async fn an_uncertain_delete_that_committed_reconciles_as_acknowledged() {
    let db = open(
        "drain-uncertain-committed",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_text(&db).await;
    add_text(&db, "alpha").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Text).await;
    publisher(&db)
        .hooks()
        .uncertain_after_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    assert_eq!(deletes(&db), 0, "an uncertain delete is not counted yet");
    let usage = db
        .index_operation_backlog()
        .usage(DataScope::LegacyUnscoped, target.index_id);
    assert_eq!((usage.operations, usage.uncertain_operations), (1, 1));
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert_eq!(deletes(&db), 1, "reconciliation found the delete committed");
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id),
        Default::default()
    );
    let stats = db.index_operation_queue_stats();
    assert_eq!(
        (stats.acknowledged_operations, stats.uncertain_operations),
        (1, 0)
    );
    db.close().await.unwrap();
}

/// A draining acknowledgement whose uncertain commit never landed
/// reconciles as not acknowledged and is not counted as a delete; the next
/// attempt drains the queue again, and that delete is counted once.
#[tokio::test]
async fn an_uncertain_delete_that_never_committed_is_not_counted() {
    let db = open(
        "drain-uncertain-lost",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_text(&db).await;
    add_text(&db, "alpha").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Text).await;
    publisher(&db)
        .hooks()
        .uncertain_without_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Merge)
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(1)
    );
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    assert_eq!(deletes(&db), 1, "only the delete that committed counts");
    db.close().await.unwrap();
}

/// A draining acknowledgement whose WAL upload fails never becomes durable:
/// the process that restarts finds every operation still queued, charges it,
/// and drains it again.
#[tokio::test]
async fn a_delete_lost_with_its_wal_upload_leaves_every_operation_queued() {
    let gate = Arc::new(GatedWalStore::new());
    let config = || queued(IndexOperationQueueTuning::default());
    let db = open(
        "drain-wal-lost",
        Arc::clone(&gate) as Arc<dyn ObjectStore>,
        config(),
    )
    .await;
    install_vector(&db, None).await;
    let first = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let second = add_doc(&db, vec![1.0, 1.0], "b").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Vector).await;
    gate.uploads.send_replace(WalUploads::Failing);
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(db.index_operation_queue_stats().uncertain_commits, 1);
    assert_eq!(deletes(&db), 0, "an uncertain delete is not counted");
    // The failed WAL upload closed the writer, so it is dropped unclosed.
    drop(db);

    let db = open(
        "drain-wal-lost",
        Arc::new(gate.restarted()) as Arc<dyn ObjectStore>,
        config(),
    )
    .await;
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [first, second]);
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id)
            .operations,
        2
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(2)
    );
    assert_eq!(deletes(&db), 1);
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    db.close().await.unwrap();
}

/// A crash after the delete applied to the memtable but before its WAL
/// upload landed loses it with the rest of the tail: the restarted writer
/// reloads the whole queue.
#[tokio::test]
async fn a_crash_before_the_delete_is_durable_reloads_the_whole_queue() {
    let gate = Arc::new(GatedWalStore::new());
    let config = || queued(IndexOperationQueueTuning::default());
    let db = Arc::new(
        open(
            "drain-crash",
            Arc::clone(&gate) as Arc<dyn ObjectStore>,
            config(),
        )
        .await,
    );
    install_vector(&db, None).await;
    let only = add_doc(&db, vec![0.0, 0.0], "a").await.unwrap();
    let target = super::tests::target(&db, QueueFamily::Vector).await;
    gate.uploads.send_replace(WalUploads::Held);
    let publishing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { publisher(&db).publish_once(target).await })
    };
    // The delete is applied (reads find no queue) while its upload is held.
    tokio::time::timeout(Duration::from_secs(10), async {
        while queue(&db, QueueFamily::Vector).await.is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the delete applies while its WAL upload is held");
    assert_eq!(
        newest(&db.inner_db(), &target.key()).await,
        Some(Stored::Tombstone)
    );
    publishing.abort();
    // The held upload never lands: this is the crash.
    drop(db);

    let db = open(
        "drain-crash",
        Arc::new(gate.restarted()) as Arc<dyn ObjectStore>,
        config(),
    )
    .await;
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [only]);
    assert_eq!(
        db.index_operation_backlog()
            .usage(DataScope::LegacyUnscoped, target.index_id)
            .operations,
        1
    );
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        published(1)
    );
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    db.close().await.unwrap();
}

/// Storage stages a delete only when the acknowledgement names every
/// operation it read and the ledger charges the target nothing else; the
/// row layout always deletes rows.
#[tokio::test]
async fn storage_deletes_only_when_the_read_and_the_ledger_both_drain() {
    let db = raw_db(Arc::new(InMemory::new())).await;
    let map = QueueStore::new(QueueLayout::Map, u64::MAX, 0);
    let target = queue_target(DataScope::LegacyUnscoped, 1, 1);
    let operations = [text(1, "a"), text(2, "b")];
    enqueue(&db, &map, target, &operations).await;
    let stored = map.read(&db, target).await.unwrap().unwrap();
    let ids = operations
        .iter()
        .map(QueuedOperation::id)
        .collect::<Vec<_>>();
    let charge = |backlog: &super::backlog::IndexOperationBacklog,
                  target: QueueTarget,
                  operations: &[QueuedOperation]| {
        backlog.load_durable(
            target,
            operations.iter().map(|operation| {
                (
                    operation.id(),
                    operation.entity(),
                    operation.retained_bytes(),
                )
            }),
        );
    };
    let stage = |backlog: Arc<super::backlog::IndexOperationBacklog>, ids: Vec<_>| {
        let db = &db;
        let map = &map;
        let stored = &stored;
        async move {
            let transaction = db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .unwrap();
            map.stage_acknowledge(&transaction, &backlog, target, stored, &ids)
                .unwrap()
        }
    };
    // Both read operations named, both charged: drained.
    let both = ledger();
    charge(&both, target, &operations);
    assert_eq!(
        stage(Arc::clone(&both), ids.clone()).await,
        StagedAcknowledgement::QueueDelete
    );
    // The ledger alone would drain, but the read holds an unnamed operation.
    let first = ledger();
    charge(&first, target, &operations[..1]);
    assert_eq!(
        stage(first, ids[..1].to_vec()).await,
        StagedAcknowledgement::Removal
    );
    // Every read operation named, but the ledger charges another.
    let more = ledger();
    charge(&more, target, &operations);
    charge(&more, target, &[text(3, "c")]);
    assert_eq!(
        stage(more, ids.clone()).await,
        StagedAcknowledgement::Removal
    );
    // An uncertain charge keeps it.
    let uncertain = ledger();
    charge(&uncertain, target, &operations);
    uncertain.mark_acknowledgement_uncertain([ids[0]]);
    assert_eq!(
        stage(uncertain, ids.clone()).await,
        StagedAcknowledgement::Removal
    );
    // A delete is what commits: the key's newest entry is a tombstone.
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert_eq!(
        map.stage_acknowledge(&transaction, &both, target, &stored, &ids)
            .unwrap(),
        StagedAcknowledgement::QueueDelete
    );
    transaction.commit().await.unwrap();
    assert_eq!(newest(&db, &target.key()).await, Some(Stored::Tombstone));
    assert!(map.read(&db, target).await.unwrap().is_none());

    // The row layout deletes rows whatever the ledger says.
    let rows = QueueStore::new(QueueLayout::Rows, u64::MAX, 0);
    let row_target = queue_target(DataScope::LegacyUnscoped, 2, 1);
    let row_operations = [text(4, "d")];
    enqueue(&db, &rows, row_target, &row_operations).await;
    let stored = rows.read(&db, row_target).await.unwrap().unwrap();
    let drained = ledger();
    charge(&drained, row_target, &row_operations);
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    assert_eq!(
        rows.stage_acknowledge(
            &transaction,
            &drained,
            row_target,
            &stored,
            &[row_operations[0].id()]
        )
        .unwrap(),
        StagedAcknowledgement::RowDeletes
    );
    drop(transaction);
    db.close().await.unwrap();
}
