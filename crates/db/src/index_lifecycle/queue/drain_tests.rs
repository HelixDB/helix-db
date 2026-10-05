//! Cost of draining a generation queue against its backlog size.
//!
//! The measurement is ignored by default and meant for release builds:
//!
//! ```text
//! HELIX_DRAIN_SIZES=10000,100000,250000 cargo test --release -p db --lib \
//!     drain_cost_against_backlog_size -- --ignored --nocapture
//! ```

use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use helix_ast::{
    batch, index::IndexSpec, index::VectorDistanceMetric, query::QueryRequest, traversal,
    value::PropertyInput,
};
use slatedb::object_store::memory::InMemory;

use super::lifecycle_tests::{drop_index, wait_terminal};
use super::overlay_tests::write;
use super::publication::test_hooks::InjectedPlanningFailure;
use super::publication::PublicationOutcome;
use super::publication_tests::{install_vector, publisher};
use super::tests::{open, queue, queued, target};
use super::QueueTarget;
use crate::config::IndexOperationQueueTuning;
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
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
                    traversal::g().add_n("Doc", vec![("embedding", PropertyInput::from(embedding))]),
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

/// Drives `target` through the writer's publisher until it reads empty.
async fn drain(db: &HelixDB, target: QueueTarget) -> Drain {
    let publisher = publisher(db);
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
    drain(&db, target).await.report("publish", backlog);
    db.close().await.unwrap();
}

/// Discards a backlog of `backlog` inserts after its index drops.
async fn discard(backlog: usize) {
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
    drain(&db, target).await.report("discard", backlog);
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
        discard(backlog).await;
        publish_with_failures(backlog, 64).await;
    }
}
