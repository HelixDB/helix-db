//! Durable-commit, exact-acknowledgement, lag, and merge-cost counters
//! observed through real queued writes, publication, restart, and SlateDB
//! merge resolution.

use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use helix_ast::query::SearchConsistency;
use slatedb::config::{MergeOptions, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::overlay_tests::{add, add_many, drain, update, vector_search};
use super::publication::PublicationOutcome;
use super::publication_tests::batch_limits;
use super::tests::{
    conflict_next_commit, install_vector_and_text, open, publisher_with_limits, queue, queued,
    target,
};
use super::QueueTarget;
use crate::config::{DbConfig, IndexOperationQueueTuning, VectorIndexDefinition};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QueueOperand, QueuedOperation, QueuedOperationId, QueuedPayload,
    QueuedTextPayload,
};
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, ValidatedDynamicIndexDefinition,
};
use crate::merge_operator::{HelixMergeOperator, QueueMerges};
use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
use crate::search::vector::VectorDistanceMetric;

#[tokio::test]
async fn every_acknowledgement_is_timed_or_censored_across_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = || queued(IndexOperationQueueTuning::default());
    let db = open("queue-stats", Arc::clone(&store), config()).await;
    install_vector_and_text(&db).await;
    let first = add(&db, [1.0, 0.0], "first", None).await;
    add(&db, [0.0, 1.0], "second", None).await;
    update(&db, first, [2.0, 2.0], "first again").await;

    // Three writes enqueue one vector and one text operation each.
    let pending = db.index_operation_queue_stats();
    assert_eq!(pending.committed_operations, 6);
    assert_eq!(pending.pending_operations, 6);
    assert_eq!(pending.acknowledged_operations, 0);
    assert!(pending.oldest_pending_micros > 0);

    let vector = target(&db, QueueFamily::Vector).await;
    let text = target(&db, QueueFamily::Text).await;
    drain(&db, vector).await;
    drain(&db, text).await;
    let published = db.index_operation_queue_stats();
    assert_eq!(published.acknowledged_operations, 6);
    assert_eq!(published.censored_acknowledgements, 0);
    assert_eq!(published.published_operations, 6);
    assert_eq!(published.pending_operations, 0);
    assert_eq!(published.oldest_pending_micros, 0);
    assert!(published.publication_attempts >= 2);
    assert!(published.publication_attempt_micros > 0);
    assert_eq!(published.publication_retries, 0);
    assert_eq!(db.index_operation_publication_lag().count(), 6);

    // Work retained across a restart has no observed commit instant.
    add(&db, [3.0, 3.0], "before restart", None).await;
    db.close().await.unwrap();
    let db = open("queue-stats", Arc::clone(&store), config()).await;
    let reopened = db.index_operation_queue_stats();
    assert_eq!(
        (
            reopened.committed_operations,
            reopened.discovered_operations
        ),
        (0, 2)
    );
    assert_eq!(reopened.oldest_pending_micros, 0);
    drain(&db, vector).await;
    drain(&db, text).await;
    let settled = db.index_operation_queue_stats();
    assert_eq!(
        (
            settled.acknowledged_operations,
            settled.censored_acknowledgements
        ),
        (2, 2)
    );
    assert_eq!(db.index_operation_publication_lag().count(), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn queue_merge_costs_count_slatedb_batches_not_chain_length() {
    // Only this database counts here, so parallel tests cannot move the costs.
    static MERGES: QueueMerges = QueueMerges::new();
    let db = slatedb::Db::builder("queue-merge-costs", Arc::new(InMemory::new()))
        .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
        .build()
        .await
        .unwrap();
    let key = QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    )
    .key();
    // 250 single-operation enqueues form one 250-operand merge chain.
    for id in 1..=250_u64 {
        let operation = QueuedOperation::new(
            QueuedOperationId::try_from_u128(u128::from(id)).unwrap(),
            IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(id),
            },
            QueuedPayload::Text(QueuedTextPayload { replacement: None }),
        );
        let (bytes, _) = QueueOperand::enqueue(&[operation]).unwrap().into_parts();
        db.merge_with_options(
            &key,
            bytes,
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    }
    let written = MERGES.stats();
    let value = db.get(&key).await.unwrap().unwrap();
    assert_eq!(
        OperationQueue::decode(&value).unwrap().operations().len(),
        250
    );
    let read = MERGES.stats();

    // The read folds raw operands in SlateDB batches of at most 100 ...
    assert_eq!(read.partial.merges - written.partial.merges, 3);
    assert_eq!(read.partial.operands - written.partial.operands, 250);
    assert_eq!(read.partial.max_operands, 100, "a batch, not the chain");
    // ... then resolves only the three batch results against the base.
    assert_eq!(read.resolved.merges - written.resolved.merges, 1);
    assert_eq!(read.resolved.operands - written.resolved.operands, 3);
    assert_eq!(read.resolved.max_operands, 3);
    assert_eq!(
        read.resolved.input_bytes - written.resolved.input_bytes,
        read.partial.output_bytes - written.partial.output_bytes,
        "a resolution reads the partial outputs again"
    );
    assert_eq!(
        read.resolved.output_bytes - written.resolved.output_bytes,
        value.len() as u64
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn publication_queue_reads_are_linear_in_backlog() {
    // Each attempt publishes at most 512 entities, so a drain that re-reads
    // the remaining queue per attempt reads about DOCS / 1024 backlogs.
    const DOCS: usize = 10_000;
    let db = open(
        "queue-read-linear",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
                .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    add_many(
        &db,
        (0..DOCS).map(|index| [index as f32, (index % 101) as f32]),
    )
    .await;
    let target = target(&db, QueueFamily::Vector).await;
    let queued = queue(&db, QueueFamily::Vector).await.unwrap();
    assert_eq!(queued.operations().len(), DOCS);
    let backlog = queued
        .operations()
        .iter()
        .map(QueuedOperation::retained_bytes)
        .sum::<u64>();

    let before = db.index_operation_queue_stats();
    drain(&db, target).await;
    let after = db.index_operation_queue_stats();
    assert_eq!(
        after.published_operations - before.published_operations,
        DOCS as u64
    );
    let reads = after.queue_reads - before.queue_reads;
    let read_bytes = after.queue_read_bytes - before.queue_read_bytes;
    assert!(reads > 1, "the drain took several attempts");
    assert!(
        read_bytes <= 4 * backlog,
        "draining a {backlog}-byte backlog read {read_bytes} queue bytes over {reads} reads"
    );
    db.close().await.unwrap();
}

/// Draining a vector and a text index of one label together, as the worker
/// does, reads each queue once: every target keeps its own remainder, even
/// when their remainders together exceed one index's retained-byte ceiling.
#[tokio::test]
async fn interleaved_drains_read_each_targets_queue_once() {
    const DOCS: usize = 4_000;
    const CEILING: u64 = 150_000;
    let db = open(
        "queue-read-two-targets",
        Arc::new(InMemory::new()),
        queued(
            IndexOperationQueueTuning::default()
                .with_max_retained_bytes(NonZeroU64::new(CEILING).unwrap())
                .unwrap(),
        ),
    )
    .await;
    install_vector_and_text(&db).await;
    add_many(&db, (0..DOCS).map(|index| [index as f32, 0.0])).await;
    let mut targets = Vec::new();
    let mut backlogs = Vec::new();
    for family in [QueueFamily::Vector, QueueFamily::Text] {
        targets.push(target(&db, family).await);
        backlogs.push(
            queue(&db, family)
                .await
                .unwrap()
                .operations()
                .iter()
                .map(QueuedOperation::retained_bytes)
                .sum::<u64>(),
        );
    }
    // Each batch publishes at most 512 entities, so after one batch of each
    // the two remainders still hold more than the ceiling together.
    assert!(
        backlogs.iter().all(|backlog| *backlog <= CEILING)
            && backlogs.iter().sum::<u64>() >= 3 * CEILING / 2,
        "each backlog fits the ceiling but their remainders do not: {backlogs:?}"
    );

    let publisher = db.index_queue_publisher().unwrap();
    let before = db.index_operation_queue_stats();
    // Alternate between the targets until both read empty.
    let mut attempts = [0_u32; 2];
    let mut drained = [false; 2];
    while drained.contains(&false) {
        for ((target, drained), attempts) in targets.iter().zip(&mut drained).zip(&mut attempts) {
            if *drained {
                continue;
            }
            *attempts += 1;
            match publisher.publish_once(*target).await.unwrap() {
                PublicationOutcome::Published { .. } => {}
                PublicationOutcome::Empty => *drained = true,
                outcome @ (PublicationOutcome::Discarded { .. }
                | PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Trimmed
                | PublicationOutcome::Blocked) => panic!("publication stalled: {outcome:?}"),
            }
        }
    }
    let after = db.index_operation_queue_stats();
    assert_eq!(
        after.published_operations - before.published_operations,
        2 * DOCS as u64
    );
    assert!(
        attempts.iter().all(|attempts| *attempts >= 3),
        "each target took several batches: {attempts:?}"
    );
    assert_eq!(
        after.queue_reads - before.queue_reads,
        4,
        "each target's first read and the read that finds it empty"
    );
    assert_eq!(db.index_queue_store().retained().retained_bytes(), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn publication_continues_from_its_last_commit_and_rereads_after_any_other_outcome() {
    let db = open(
        "queue-retained",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
                .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let ids = add_many(&db, (0..40).map(|index| [index as f32, 0.0])).await;
    let target = target(&db, QueueFamily::Vector).await;
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    // An input budget of exactly ten operations per batch.
    let narrow = publisher_with_limits(
        &db,
        batch_limits(
            operations[..10]
                .iter()
                .map(QueuedOperation::retained_bytes)
                .sum(),
            32_768,
        ),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let reads = || narrow.metrics().queue_reads.load(Ordering::Relaxed);
    let retained = || db.index_queue_store().retained().retained_bytes();
    let stored_bytes = || async {
        queue(&db, QueueFamily::Vector).await.map_or(0, |queue| {
            queue
                .operations()
                .iter()
                .map(QueuedOperation::retained_bytes)
                .sum::<u64>()
        })
    };
    let published = |outcome| match outcome {
        PublicationOutcome::Published { operations, .. } => operations,
        outcome @ (PublicationOutcome::Discarded { .. }
        | PublicationOutcome::Empty
        | PublicationOutcome::Deferred
        | PublicationOutcome::Retry
        | PublicationOutcome::Trimmed
        | PublicationOutcome::Blocked) => panic!("publication did not commit: {outcome:?}"),
    };

    assert_eq!(published(narrow.publish_once(target).await.unwrap()), 10);
    assert_eq!(reads(), 1);
    assert_eq!(retained(), stored_bytes().await, "the rest is retained");

    // Work committed after the read waits for the retained operations: the
    // next batch continues from them without reading storage.
    update(&db, ids[15], [100.0, 0.0], "moved").await;
    let late = add(&db, [200.0, 0.0], "late", None).await;
    let second = published(narrow.publish_once(target).await.unwrap());
    assert!(second > 0);
    assert_eq!(reads(), 1);
    let nearest = |query| {
        let db = &db;
        async move {
            vector_search(db, query, 1, None, SearchConsistency::Strong)
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        nearest([100.0, 0.0]).await,
        [ids[15]],
        "strong search sees the move"
    );

    // An attempt that does not commit takes the retained queue and drops it:
    // the next one reads storage again, newer work included.
    narrow
        .hooks()
        .fail_before_commit
        .store(true, Ordering::SeqCst);
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(retained(), 0);
    let mut drained = 0;
    loop {
        let outcome = narrow.publish_once(target).await.unwrap();
        if outcome == PublicationOutcome::Empty {
            break;
        }
        drained += published(outcome);
    }
    assert_eq!(
        drained,
        42 - 10 - second,
        "the rest, the move, and the late add"
    );
    assert_eq!(retained(), 0);
    assert!(queue(&db, QueueFamily::Vector).await.is_none());
    assert_eq!(
        reads(),
        3,
        "the read after the failure and the read that finds the queue empty"
    );
    // The move was published after the retained insert it follows.
    assert_eq!(nearest([100.0, 0.0]).await, [ids[15]]);
    assert_eq!(nearest([200.0, 0.0]).await, [late]);
    assert_ne!(nearest([15.0, 0.0]).await, [ids[15]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn publication_rereads_its_queue_after_a_conflict_or_an_uncertain_commit() {
    let gate = Arc::new(GatedWalStore::new());
    let db = open(
        "queue-retained-uncommitted",
        Arc::clone(&gate) as Arc<dyn ObjectStore>,
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node("Doc", "embedding", 2, VectorDistanceMetric::Euclidean)
                .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // One entity's add and two moves, published one operation per batch.
    let entity = add(&db, [0.0, 0.0], "add", None).await;
    update(&db, entity, [5.0, 5.0], "first move").await;
    update(&db, entity, [9.0, 9.0], "second move").await;
    let target = target(&db, QueueFamily::Vector).await;
    let queued_operations = || async {
        queue(&db, QueueFamily::Vector)
            .await
            .map_or(0, |queue| queue.operations().len())
    };
    assert_eq!(queued_operations().await, 3);
    let narrow = publisher_with_limits(
        &db,
        batch_limits(1, 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let reads = || narrow.metrics().queue_reads.load(Ordering::Relaxed);
    let retained = || db.index_queue_store().retained().retained_bytes();

    // A conflicting commit acknowledged nothing, so its attempt keeps no
    // queue: retaining the rest would publish the moves ahead of the add.
    let (outcome, ()) = tokio::join!(
        narrow.publish_once(target),
        conflict_next_commit(&db, &narrow)
    );
    assert_eq!(outcome.unwrap(), PublicationOutcome::Retry);
    assert_eq!(narrow.metrics().commit_conflicts.load(Ordering::Relaxed), 1);
    assert_eq!((reads(), retained()), (1, 0));
    assert_eq!(queued_operations().await, 3, "nothing was acknowledged");
    let mut published = 0;
    loop {
        match narrow.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            PublicationOutcome::Published { operations, .. } => published += operations,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked) => panic!("publication stalled: {outcome:?}"),
        }
    }
    assert_eq!(published, 3);
    assert_eq!(
        reads(),
        3,
        "the read after the conflict and the read that finds the queue empty"
    );
    let hits = vector_search(&db, [9.0, 9.0], 1, None, SearchConsistency::Strong).await;
    let [(hit, distance)] = hits[..] else {
        panic!("one entity is indexed, got {hits:?}");
    };
    assert_eq!(
        (hit, f64::from_bits(distance)),
        (entity, 0.0),
        "the last move is published last"
    );

    // An uncertain commit may have acknowledged its batch, so its attempt
    // keeps no queue either.
    update(&db, entity, [1.0, 1.0], "third move").await;
    update(&db, entity, [2.0, 2.0], "fourth move").await;
    gate.uploads.send_replace(WalUploads::Failing);
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_eq!(
        narrow.metrics().uncertain_commits.load(Ordering::Relaxed),
        1
    );
    assert_eq!(retained(), 0);
    // The failed WAL upload closed the writer, so it is dropped unclosed.
}
