//! Draining a generation queue: its cost against the backlog size, and its
//! correctness across retirement, recreation, restart, concurrent writes,
//! held entities, and many targets.
//!
//! The measurement is ignored by default and meant for release builds:
//!
//! ```text
//! HELIX_DRAIN_SIZES=10000,100000,250000 cargo test --release -p db --lib \
//!     drain_cost_against_backlog_size -- --ignored --nocapture
//! ```

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use helix_ast::{
    batch, index::IndexSpec, index::VectorDistanceMetric, query::QueryRequest,
    query::SearchConsistency, traversal, value::PropertyInput,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::lifecycle_tests::{create, drop_index, wait_terminal};
use super::overlay_tests::{delete, update, vector_search, write};
use super::publication::test_hooks::InjectedPlanningFailure;
use super::publication::{PublicationOutcome, QueuePublisher};
use super::publication_tests::{install_vector, publisher};
use super::tests::{add_doc, open, publisher_with_limits, queue, queued, target};
use super::QueueTarget;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, SearchIndexBatchLimits, VectorIndexDefinition,
};
use crate::encoding::v2::values::indexes::operation_queue::{QueueFamily, QueuedOperation};
use crate::error::{HelixDbError, IndexBackpressureResource};
use crate::index_lifecycle::vector::MAX_RETAINED_PUBLICATIONS;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::HelixDB;

/// Operations one benchmark write transaction enqueues.
const WRITE_BATCH: usize = 500;

/// Backlog sizes measured: `HELIX_DRAIN_SIZES`, comma-separated.
fn sizes() -> Vec<usize> {
    std::env::var("HELIX_DRAIN_SIZES").map_or_else(
        |_| vec![10_000, 100_000, 250_000],
        |sizes| {
            sizes
                .split(',')
                .map(|size| size.trim().parse().expect("a backlog size"))
                .collect()
        },
    )
}

/// Paused publication and member limits above every measured backlog.
async fn open_measured(name: &str) -> HelixDB {
    let db = open(
        name,
        Arc::new(InMemory::new()),
        queued(
            IndexOperationQueueTuning::default()
                .with_max_members(NonZeroU64::new(16_000_000).unwrap()),
        ),
    )
    .await;
    install_vector(&db, None).await;
    db
}

/// Enqueues `count` vector inserts in transactions of [`WRITE_BATCH`].
async fn enqueue(db: &HelixDB, count: usize) {
    for start in (0..count).step_by(WRITE_BATCH) {
        let end = (start + WRITE_BATCH).min(count);
        write(db, || {
            QueryRequest::write((start..end).fold(batch::write_batch(), |batch, index| {
                #[allow(clippy::cast_precision_loss, reason = "benchmark coordinates")]
                let embedding = vec![index as f32, (index % 997) as f32];
                batch.var_as(
                    &format!("created{index}"),
                    traversal::g()
                        .add_n("Doc", vec![("embedding", PropertyInput::from(embedding))]),
                )
            }))
        })
        .await;
    }
}

/// Every outcome of driving `target` until it reads empty, the wall time,
/// and the publisher's queue reads and their time.
struct Drain {
    outcomes: Vec<PublicationOutcome>,
    seconds: f64,
    reads: u64,
    read_millis: u64,
}

impl Drain {
    fn report(&self, scenario: &str, backlog: usize) {
        let operations = self
            .outcomes
            .iter()
            .map(|outcome| match outcome {
                PublicationOutcome::Published { operations, .. }
                | PublicationOutcome::Discarded { operations } => *operations,
                PublicationOutcome::Empty
                | PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Trimmed
                | PublicationOutcome::Blocked
                | PublicationOutcome::Stalled => 0,
            })
            .sum::<u64>();
        #[allow(clippy::cast_precision_loss, reason = "reported rates")]
        let rate = operations as f64 / self.seconds;
        println!(
            "DRAIN scenario={scenario} backlog={backlog} operations={operations} \
             attempts={} seconds={:.3} ops_per_sec={rate:.0} queue_reads={} \
             queue_read_ms={}",
            self.outcomes.len(),
            self.seconds,
            self.reads,
            self.read_millis,
        );
    }
}

/// Drives `target` through `publisher` until it reads empty.
async fn drain(publisher: &QueuePublisher, target: QueueTarget) -> Drain {
    let metrics = publisher.metrics();
    let (reads, read_micros) = (
        metrics.queue_reads.load(Ordering::Relaxed),
        metrics.queue_read_micros.load(Ordering::Relaxed),
    );
    let started = Instant::now();
    let mut outcomes = Vec::new();
    loop {
        let outcome = publisher.publish_once(target).await.unwrap();
        outcomes.push(outcome);
        match outcome {
            PublicationOutcome::Empty => break,
            PublicationOutcome::Stalled
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry => panic!("the drain stopped: {outcome:?}"),
            PublicationOutcome::Published { .. }
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked => {}
        }
    }
    Drain {
        outcomes,
        seconds: started.elapsed().as_secs_f64(),
        reads: metrics.queue_reads.load(Ordering::Relaxed) - reads,
        read_millis: (metrics.queue_read_micros.load(Ordering::Relaxed) - read_micros) / 1_000,
    }
}

/// Publishes a backlog of `backlog` inserts.
async fn publish(backlog: usize) {
    let db = open_measured("drain-publish").await;
    enqueue(&db, backlog).await;
    let target = target(&db, QueueFamily::Vector).await;
    drain(publisher(&db), target)
        .await
        .report("publish", backlog);
    db.close().await.unwrap();
}

/// Discards a backlog of `backlog` inserts after its index drops, in
/// acknowledgements of at most `acknowledgement_bytes`, or of the writer's
/// default output budget.
async fn discard(backlog: usize, acknowledgement_bytes: Option<u64>) {
    let db = open_measured("drain-discard").await;
    enqueue(&db, backlog).await;
    let target = target(&db, QueueFamily::Vector).await;
    let dropped = drop_index(
        &db,
        IndexSpec::node_vector(
            "Doc",
            "embedding",
            std::num::NonZeroUsize::new(2).unwrap(),
            VectorDistanceMetric::Euclidean,
            None::<&str>,
        ),
    )
    .await
    .expect("the drop is an operation");
    assert_eq!(wait_terminal(&db, &dropped).await, "succeeded");
    match acknowledgement_bytes {
        None => drain(publisher(&db), target)
            .await
            .report("discard", backlog),
        Some(bytes) => {
            let defaults = DbConfig::new().search_index_backfill();
            let batch = defaults.batch();
            let narrow = publisher_with_limits(
                &db,
                SearchIndexBatchLimits::try_new(
                    batch.max_entities(),
                    batch.max_input_bytes(),
                    batch.max_output_operations(),
                    NonZeroU64::new(bytes).unwrap(),
                    NonZeroU64::new(bytes).unwrap(),
                )
                .unwrap(),
                defaults.active_text_mutation(),
            );
            drain(&narrow, target)
                .await
                .report(&format!("discard-{}k", bytes / 1024), backlog);
        }
    }
    db.close().await.unwrap();
}

/// Publishes a backlog of `backlog` inserts, `failing` of which fail to plan
/// deterministically and are held back one attempt each.
async fn publish_with_failures(backlog: usize, failing: usize) {
    let db = open_measured("drain-blocked").await;
    enqueue(&db, backlog).await;
    let target = target(&db, QueueFamily::Vector).await;
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    {
        let mut failures = publisher(&db).hooks().planning_failures.lock();
        for operation in operations.iter().step_by(backlog / failing) {
            failures.insert(operation.id(), InjectedPlanningFailure::Corrupt);
        }
    }
    let mut outcomes = Vec::new();
    let publisher = publisher(&db);
    let metrics = publisher.metrics();
    let (reads, read_micros) = (
        metrics.queue_reads.load(Ordering::Relaxed),
        metrics.queue_read_micros.load(Ordering::Relaxed),
    );
    let started = Instant::now();
    loop {
        let outcome = publisher.publish_once(target).await.unwrap();
        outcomes.push(outcome);
        match outcome {
            PublicationOutcome::Stalled => break,
            PublicationOutcome::Published { .. } | PublicationOutcome::Blocked => {}
            PublicationOutcome::Empty
            | PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed => panic!("the drain stopped: {outcome:?}"),
        }
    }
    Drain {
        outcomes,
        seconds: started.elapsed().as_secs_f64(),
        reads: metrics.queue_reads.load(Ordering::Relaxed) - reads,
        read_millis: (metrics.queue_read_micros.load(Ordering::Relaxed) - read_micros) / 1_000,
    }
    .report("blocked", backlog);
    db.close().await.unwrap();
}

/// Prints drain throughput, attempts, and queue reads per backlog size for
/// publication, retired-queue discards, and publication past entities that
/// fail to plan.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "release-mode measurement; run explicitly"]
async fn drain_cost_against_backlog_size() {
    for backlog in sizes() {
        publish(backlog).await;
        discard(backlog, None).await;
        discard(backlog, Some(64 * 1024)).await;
        publish_with_failures(backlog, 64).await;
    }
}

/// Paused publication; eventual searches overlay no queued work, so they see
/// exactly what is published.
fn config() -> DbConfig {
    queued(IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(0))
}

fn spec() -> IndexSpec {
    IndexSpec::node_vector(
        "Doc",
        "embedding",
        NonZeroUsize::new(2).unwrap(),
        VectorDistanceMetric::Euclidean,
        None::<&str>,
    )
}

/// Inserts `count` documents at `[index, 0]` in one transaction, returning
/// their IDs in order.
async fn insert(db: &HelixDB, count: usize) -> Vec<u64> {
    let created = write(db, || {
        QueryRequest::write(
            (0..count)
                .fold(batch::write_batch(), |batch, index| {
                    #[allow(clippy::cast_precision_loss, reason = "test coordinates")]
                    let embedding = vec![index as f32, 0.0];
                    batch.var_as(
                        &format!("created{index}"),
                        traversal::g()
                            .add_n("Doc", vec![("embedding", PropertyInput::from(embedding))]),
                    )
                })
                .returning((0..count).map(|index| format!("created{index}"))),
        )
    })
    .await;
    (0..count)
        .map(|index| {
            created[format!("created{index}")][0]["$id"]
                .as_u64()
                .unwrap()
        })
        .collect()
}

/// A publisher over `db` whose batches take at most the input bytes of
/// `operations` of `target`'s queued operations, and whose acknowledgements
/// and effects stay within `output_bytes`.
async fn narrow(
    db: &HelixDB,
    target: QueueTarget,
    operations: usize,
    output_bytes: u64,
) -> Arc<QueuePublisher> {
    let input = db
        .index_queue_store()
        .read(db.inner_db().as_ref(), target)
        .await
        .unwrap()
        .expect("the target has queued work")
        .operations()
        .take(operations)
        .map(QueuedOperation::retained_bytes)
        .sum();
    let defaults = DbConfig::new().search_index_backfill();
    publisher_with_limits(
        db,
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::new(512).unwrap(),
            NonZeroU64::new(input).unwrap(),
            NonZeroU64::new(32_768).unwrap(),
            NonZeroU64::new(output_bytes).unwrap(),
            NonZeroU64::new(output_bytes).unwrap(),
        )
        .unwrap(),
        defaults.active_text_mutation(),
    )
}

/// Every outcome of `publisher`'s attempts on `target` until one reads it
/// empty.
async fn drain_with(publisher: &QueuePublisher, target: QueueTarget) -> Vec<PublicationOutcome> {
    let mut outcomes = Vec::new();
    loop {
        let outcome = publisher.publish_once(target).await.unwrap();
        outcomes.push(outcome);
        if outcome == PublicationOutcome::Empty {
            return outcomes;
        }
        assert!(outcomes.len() < 10_000, "the drain does not converge");
    }
}

/// Operations `outcomes` published, and those they discarded.
fn totals(outcomes: &[PublicationOutcome]) -> (u64, u64) {
    outcomes
        .iter()
        .fold((0, 0), |(published, discarded), outcome| match outcome {
            PublicationOutcome::Published { operations, .. } => (published + operations, discarded),
            PublicationOutcome::Discarded { operations } => (published, discarded + operations),
            PublicationOutcome::Empty
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled => (published, discarded),
        })
}

fn reads(publisher: &QueuePublisher) -> u64 {
    publisher.metrics().queue_reads.load(Ordering::Relaxed)
}

/// The published entity nearest `point`, and whether it lies exactly there.
async fn published_at(db: &HelixDB, point: [f32; 2]) -> Option<(u64, bool)> {
    vector_search(db, point, 1, None, SearchConsistency::Eventual)
        .await
        .first()
        .map(|(id, distance)| (*id, f64::from_bits(*distance) == 0.0))
}

/// Asserts `target` holds nothing: no stored queue, charge, retained queue,
/// or schedule of `publisher`'s.
async fn assert_released(db: &HelixDB, publisher: &QueuePublisher, target: QueueTarget) {
    assert!(db.inner_db().get(target.key()).await.unwrap().is_none());
    assert!(!db.index_operation_backlog().has_charges(target));
    assert!(db.index_queue_store().retained().take(target).is_none());
    assert_eq!(publisher.scheduled_targets().0, 0);
}

/// A generation retired while its queue drains is discarded from the queue
/// that drain retained: its retained operations are never published, each
/// batch releases its charges as it commits, and storage is read again only
/// to find the queue empty.
#[tokio::test]
async fn a_generation_retired_mid_drain_discards_from_its_retained_queue() {
    let db = open("drain-retire", Arc::new(InMemory::new()), config()).await;
    install_vector(&db, None).await;
    enqueue(&db, 300).await;
    let target = target(&db, QueueFamily::Vector).await;
    let publishing = narrow(&db, target, 10, 8 * 1024 * 1024).await;
    let mut published = 0;
    for _ in 0..2 {
        let outcome = publishing.publish_once(target).await.unwrap();
        assert!(matches!(outcome, PublicationOutcome::Published { .. }));
        published += totals(&[outcome]).0;
    }
    assert_eq!(reads(&publishing), 1);
    assert!(db.index_queue_store().retained().retained_bytes() > 0);
    let dropped = drop_index(&db, spec()).await.unwrap();
    assert_eq!(wait_terminal(&db, &dropped).await, "succeeded");

    // About sixty IDs fit one kilobyte acknowledgement.
    let discarding = narrow(&db, target, 1, 1024).await;
    let usage = || {
        db.index_operation_backlog()
            .usage(target.scope, target.index_id)
            .operations
    };
    let mut remaining = vec![usage()];
    let mut outcomes = Vec::new();
    loop {
        match discarding.publish_once(target).await.unwrap() {
            outcome @ PublicationOutcome::Discarded { .. } => outcomes.push(outcome),
            PublicationOutcome::Empty => break,
            outcome @ (PublicationOutcome::Published { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
                panic!("the retired queue was not discarded: {outcome:?}")
            }
        }
        remaining.push(usage());
    }
    assert_eq!(totals(&outcomes), (0, 300 - published));
    assert!(outcomes.len() >= 4, "{outcomes:?}");
    assert_eq!(
        remaining
            .windows(2)
            .map(|pair| pair[0] - pair[1])
            .collect::<Vec<_>>(),
        outcomes
            .iter()
            .map(|outcome| totals(std::slice::from_ref(outcome)).1)
            .collect::<Vec<_>>(),
        "each batch releases exactly its charges"
    );
    assert_eq!(remaining.last(), Some(&0));
    assert_eq!(
        reads(&discarding),
        1,
        "only the attempt that finds the queue empty reads it"
    );
    assert_eq!(
        publishing
            .metrics()
            .published_operations
            .load(Ordering::Relaxed),
        published
    );
    assert_released(&db, &discarding, target).await;
    db.close().await.unwrap();
}

/// A recreated index reuses its logical index, so the dropped generation's
/// queued operations count toward its limits until they are discarded. The
/// discard reads that queue once and releases each batch's charges as it
/// commits, so the recreated index admits writes after the first batch.
#[tokio::test]
async fn a_recreated_index_admits_writes_once_the_first_discard_commits() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("drain-recreate", Arc::clone(&store), config()).await;
    install_vector(&db, None).await;
    enqueue(&db, 200).await;
    let retired = target(&db, QueueFamily::Vector).await;
    let backlog = db
        .index_operation_backlog()
        .usage(retired.scope, retired.index_id)
        .retained_bytes;
    db.close().await.unwrap();
    // The dropped generation's backlog sits exactly at the limit.
    let db = open(
        "drain-recreate",
        store,
        queued(
            IndexOperationQueueTuning::default()
                .with_max_retained_bytes(NonZeroU64::new(backlog).unwrap())
                .unwrap()
                .with_eventual_search_budget_for_tests(0),
        ),
    )
    .await;
    let dropped = drop_index(&db, spec()).await.unwrap();
    assert_eq!(wait_terminal(&db, &dropped).await, "succeeded");
    let created = create(&db, spec()).await;
    assert_eq!(wait_terminal(&db, &created).await, "succeeded");
    let recreated = target(&db, QueueFamily::Vector).await;
    assert_eq!(recreated.index_id, retired.index_id);
    assert!(recreated.generation > retired.generation);
    assert!(matches!(
        add_doc(&db, vec![1_000.0, 0.0], "late").await,
        Err(HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::RetainedBytes,
            ..
        })
    ));

    let discarding = narrow(&db, retired, 1, 1024).await;
    let PublicationOutcome::Discarded { operations: first } =
        discarding.publish_once(retired).await.unwrap()
    else {
        panic!("the retired queue discards");
    };
    assert!(first < 200);
    let late = add_doc(&db, vec![1_000.0, 0.0], "late")
        .await
        .expect("the first discard released room");
    let (published, discarded) = totals(&drain_with(&discarding, retired).await);
    assert_eq!((published, discarded + first), (0, 200));
    assert_eq!(reads(&discarding), 2, "the first read and the empty one");
    assert_released(&db, &discarding, retired).await;
    assert_eq!(
        db.index_operation_backlog()
            .usage(retired.scope, retired.index_id)
            .operations,
        1,
        "only the recreated index's write is charged"
    );
    drain_with(publisher(&db), recreated).await;
    assert_eq!(published_at(&db, [1_000.0, 0.0]).await, Some((late, true)));
    db.close().await.unwrap();
}

/// A target's schedule lives only while it has work, and only the most
/// recently drained targets keep their latest vector commit, so draining
/// many targets leaves both bounded.
#[tokio::test]
async fn draining_many_targets_leaves_bounded_schedules() {
    const TARGETS: usize = MAX_RETAINED_PUBLICATIONS + 8;
    let db = open("drain-schedules", Arc::new(InMemory::new()), config()).await;
    for index in 0..TARGETS {
        db.install_index_for_tests(
            ValidatedDynamicIndexDefinition::try_from(
                VectorIndexDefinition::new_node(
                    format!("Doc{index}"),
                    "embedding",
                    2,
                    crate::search::vector::VectorDistanceMetric::Euclidean,
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    let publisher = publisher(&db);
    for index in 0..TARGETS {
        for round in 0..2_u8 {
            write(&db, || {
                QueryRequest::write(batch::write_batch().var_as(
                    "created",
                    traversal::g().add_n(
                        format!("Doc{index}"),
                        vec![(
                            "embedding",
                            PropertyInput::from(vec![f32::from(round), 1.0]),
                        )],
                    ),
                ))
            })
            .await;
            let [target] = db.index_operation_backlog().outstanding_targets()[..] else {
                panic!("one target has work");
            };
            assert!(matches!(
                publisher.publish_once(target).await.unwrap(),
                PublicationOutcome::Published { .. }
            ));
            // The commit drained the target, so its schedule is gone; a
            // later write resumes from its remembered commit.
            assert_eq!(
                publisher.scheduled_targets(),
                (0, (index + 1).min(MAX_RETAINED_PUBLICATIONS))
            );
        }
    }
    db.close().await.unwrap();
}

/// A publisher restarted mid-drain keeps no retained queue or schedule: it
/// reads the rest of the queue once and publishes each remaining operation
/// exactly once, every entity at its newest state.
#[tokio::test]
async fn a_drain_restarted_midway_publishes_the_rest_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("drain-restart", Arc::clone(&store), config()).await;
    install_vector(&db, None).await;
    let ids = insert(&db, 120).await;
    for (offset, id) in ids[..30].iter().enumerate() {
        let x = 1_000.0 + f32::from(u8::try_from(offset).unwrap());
        update(&db, *id, [x, 0.0], "moved").await;
    }
    for id in &ids[30..40] {
        delete(&db, *id).await;
    }
    let target = target(&db, QueueFamily::Vector).await;
    let early = narrow(&db, target, 10, 8 * 1024 * 1024).await;
    let mut published = 0;
    for _ in 0..3 {
        published += totals(&[early.publish_once(target).await.unwrap()]).0;
    }
    assert!((20..=30).contains(&published), "{published}");
    assert!(db.index_queue_store().retained().retained_bytes() > 0);
    db.close().await.unwrap();

    let db = open("drain-restart", store, config()).await;
    let publisher = publisher(&db);
    let (rest, discarded) = totals(&drain_with(publisher, target).await);
    assert_eq!((published + rest, discarded), (160, 0));
    assert_eq!(
        reads(publisher),
        2,
        "one read, and the one that finds it empty"
    );
    for (offset, id) in ids.iter().enumerate() {
        let x = f32::from(u16::try_from(offset).unwrap());
        let at = match offset {
            0..30 => published_at(&db, [1_000.0 + x, 0.0]).await,
            30..40 => {
                assert_ne!(
                    published_at(&db, [x, 0.0]).await.map(|(hit, _)| hit),
                    Some(*id)
                );
                continue;
            }
            _ => published_at(&db, [x, 0.0]).await,
        };
        assert_eq!(at, Some((*id, true)), "document {offset}");
    }
    assert_released(&db, publisher, target).await;
    db.close().await.unwrap();
}

/// Writes, rewrites, and deletes committed while a backlog drains each
/// publish exactly once, in order: the drain continues from its retained
/// queue and reads storage again only once that is exhausted.
#[tokio::test]
async fn writes_committed_during_a_drain_publish_once_in_order() {
    let db = open("drain-concurrent", Arc::new(InMemory::new()), config()).await;
    install_vector(&db, None).await;
    let ids = insert(&db, 200).await;
    let target = target(&db, QueueFamily::Vector).await;
    let publishing = narrow(&db, target, 10, 8 * 1024 * 1024).await;
    let done = AtomicBool::new(false);
    let writes = async {
        let mut added = Vec::new();
        for round in 0..60_u16 {
            let id = ids[usize::from(round * 7 % 200)];
            update(&db, id, [5_000.0 + f32::from(round), 0.0], "moved").await;
            if round % 6 == 0 {
                update(&db, id, [6_000.0 + f32::from(round), 0.0], "moved again").await;
            }
            if round % 9 == 0 {
                added.push((
                    add_doc(&db, vec![7_000.0 + f32::from(round), 0.0], "added")
                        .await
                        .unwrap(),
                    round,
                ));
            }
            tokio::task::yield_now().await;
        }
        delete(&db, ids[199]).await;
        done.store(true, Ordering::SeqCst);
        added
    };
    let drain = async {
        let mut outcomes = Vec::new();
        loop {
            let outcome = publishing.publish_once(target).await.unwrap();
            outcomes.push(outcome);
            match outcome {
                PublicationOutcome::Published { .. } => {}
                PublicationOutcome::Empty if done.load(Ordering::SeqCst) => return outcomes,
                PublicationOutcome::Empty => tokio::task::yield_now().await,
                outcome @ (PublicationOutcome::Discarded { .. }
                | PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Trimmed
                | PublicationOutcome::Blocked
                | PublicationOutcome::Stalled) => panic!("the drain stopped: {outcome:?}"),
            }
        }
    };
    let (added, mut outcomes) = tokio::join!(writes, drain);
    outcomes.extend(drain_with(&publishing, target).await);
    assert_eq!(
        totals(&outcomes).0,
        db.index_operation_queue_stats().committed_operations
    );
    // Every document is published at its newest state.
    let mut newest = ids
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, [f32::from(u16::try_from(index).unwrap()), 0.0]))
        .collect::<HashMap<_, _>>();
    for round in 0..60_u16 {
        let id = ids[usize::from(round * 7 % 200)];
        let x = if round % 6 == 0 { 6_000.0 } else { 5_000.0 };
        newest.insert(id, [x + f32::from(round), 0.0]);
    }
    newest.remove(&ids[199]);
    for (id, round) in added {
        newest.insert(id, [7_000.0 + f32::from(round), 0.0]);
    }
    for (id, point) in &newest {
        assert_eq!(
            published_at(&db, *point).await,
            Some((*id, true)),
            "{point:?}"
        );
    }
    assert_ne!(
        published_at(&db, [199.0, 0.0]).await.map(|(hit, _)| hit),
        Some(ids[199])
    );
    assert_released(&db, &publishing, target).await;
    db.close().await.unwrap();
}

/// Holding an entity back commits nothing, so its attempt keeps the queue:
/// the generation's other entities publish from it, and a held entity whose
/// retry falls due is planned again from it, without another read. Once
/// only held entities are left in it, a write admitted since its read makes
/// the next attempt read storage again, which publishes the write's repair.
#[tokio::test]
async fn holds_keep_their_queue_until_only_held_entities_and_new_work_remain() {
    let db = open("drain-holds", Arc::new(InMemory::new()), config()).await;
    install_vector(&db, None).await;
    let ids = insert(&db, 3).await;
    let target = target(&db, QueueFamily::Vector).await;
    let publisher = publisher(&db);
    let operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .into_operations();
    publisher
        .hooks()
        .planning_failures
        .lock()
        .insert(operations[2].id(), InjectedPlanningFailure::Corrupt);
    let mut outcomes = Vec::new();
    for retry_due in [false, false, true] {
        if retry_due {
            publisher.make_failed_retries_due();
        }
        outcomes.push(publisher.publish_once(target).await.unwrap());
    }
    assert_eq!(
        outcomes,
        [
            PublicationOutcome::Blocked,
            PublicationOutcome::Published {
                operations: 2,
                entities: 2
            },
            PublicationOutcome::Blocked,
        ]
    );
    assert_eq!(reads(publisher), 1, "every attempt continued from one read");
    assert_eq!(
        db.blocked_index_entities()
            .iter()
            .map(|blocked| blocked.id.get())
            .collect::<Vec<_>>(),
        [ids[2]]
    );

    update(&db, ids[2], [9.0, 9.0], "repaired").await;
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert_eq!(
        reads(publisher),
        2,
        "the held entity's queue was read again"
    );
    assert!(db.blocked_index_entities().is_empty());
    assert_eq!(published_at(&db, [9.0, 9.0]).await, Some((ids[2], true)));
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert_released(&db, publisher, target).await;
    db.close().await.unwrap();
}
