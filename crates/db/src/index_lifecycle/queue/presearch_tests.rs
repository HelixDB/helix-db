//! Workers scoring distances never change what publication stores.
//!
//! Each seed applies one random script of inserts, near and far re-embeddings,
//! and deletes to several databases, publishing it under random budgets that
//! split the queue into attempts: one plans alone, the others beside workers.
//! Every database must then hold byte-identical vector rows. Layers are
//! deterministic, so any wrong distance surfaces as a different neighbor row.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;

use super::overlay_tests::{add, delete, update};
use super::publication::{PublicationOutcome, QueuePublisher};
use super::publication_tests::{batch_limits, install_vector, unpartitioned_vector_rows};
use super::soak_tests::Rng;
use super::tests::{open, publisher_with_presearch, queued, target};
use crate::config::{DbConfig, IndexOperationQueueTuning, VectorIndexDefinition};
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::{VectorDistanceMetric, AWAIT_WORKERS, MEMOIZED_LOOKUPS};
use crate::HelixDB;

const SEEDS: u64 = 4;
const ROUNDS: usize = 6;
const WRITES_PER_ROUND: usize = 60;
/// Output budgets that split a round into attempts of different sizes.
const OUTPUT_OPERATIONS: [u64; 4] = [1024, 2048, 4096, 8192];

/// One write of a script, addressing documents by creation order.
#[derive(Debug, Clone, Copy)]
enum Write {
    Insert([f32; 2]),
    Embed(usize, [f32; 2]),
    Delete(usize),
}

/// How a run plans.
#[derive(Debug, Clone, Copy)]
enum Planning {
    Alone,
    /// Beside workers, as production does.
    Racing(usize),
    /// Replaying a complete memo for every insert.
    Replaying(usize),
}

impl Planning {
    fn workers(self) -> Option<NonZeroUsize> {
        match self {
            Self::Alone => None,
            Self::Racing(workers) | Self::Replaying(workers) => NonZeroUsize::new(workers),
        }
    }
}

/// One round of writes and the output budget that publishes it.
#[derive(Debug, Clone)]
struct Round {
    writes: Vec<Write>,
    output_operations: u64,
}

fn point(rng: &mut Rng) -> [f32; 2] {
    let cluster = rng.below(8) as f32;
    [
        cluster * 10.0 + rng.below(1_000) as f32 / 500.0,
        cluster * -7.0 + rng.below(1_000) as f32 / 500.0,
    ]
}

/// Draws a script whose ids are valid however earlier writes ran.
fn script(seed: u64) -> Vec<Round> {
    let mut rng = Rng(seed);
    let mut live = (0..40).collect::<Vec<usize>>();
    let mut created = 40;
    (0..ROUNDS)
        .map(|round| {
            let writes = (0..WRITES_PER_ROUND)
                .map(|_| match rng.below(10) {
                    _ if round == 0 => Write::Insert(point(&mut rng)),
                    0..=3 => {
                        live.push(created);
                        created += 1;
                        Write::Insert(point(&mut rng))
                    }
                    4..=8 => {
                        let document = live[rng.below(live.len())];
                        Write::Embed(document, point(&mut rng))
                    }
                    _ => {
                        let position = rng.below(live.len());
                        Write::Delete(live.swap_remove(position))
                    }
                })
                .collect::<Vec<_>>();
            if round == 0 {
                created = 60;
                live = (0..60).collect();
            }
            Round {
                writes,
                output_operations: OUTPUT_OPERATIONS[rng.below(OUTPUT_OPERATIONS.len())],
            }
        })
        .collect()
}

async fn drain(publisher: &QueuePublisher, db: &HelixDB) {
    let target = target(db, QueueFamily::Vector).await;
    for _ in 0..10_000 {
        match publisher.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => return,
            PublicationOutcome::Published { .. }
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Retry => {}
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Blocked
            | PublicationOutcome::Stalled) => {
                panic!("publication did not progress: {outcome:?}")
            }
        }
    }
    panic!("publication never drained");
}

/// Runs `rounds`, returning the vector rows after each.
async fn run(
    name: &str,
    rounds: &[Round],
    metric: VectorDistanceMetric,
    planning: Planning,
) -> Vec<Vec<(bytes::Bytes, bytes::Bytes)>> {
    let db = open(
        name,
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()).with_wal_flush_interval_ms(NonZeroU64::MIN),
    )
    .await;
    match metric {
        VectorDistanceMetric::Euclidean => install_vector(&db, None).await,
        metric => {
            let definition =
                VectorIndexDefinition::new_node("Doc", "embedding", 2, metric).unwrap();
            db.install_index_for_tests(
                ValidatedDynamicIndexDefinition::try_from(definition).unwrap(),
            )
            .await
            .unwrap();
        }
    }
    let mut ids = Vec::new();
    let mut rows = Vec::with_capacity(rounds.len());
    for round in rounds {
        for write in &round.writes {
            match *write {
                Write::Insert(embedding) => ids.push(add(&db, embedding, "doc", None).await),
                Write::Embed(document, embedding) => {
                    update(&db, ids[document], embedding, "doc").await;
                }
                Write::Delete(document) => delete(&db, ids[document]).await,
            }
        }
        let publisher = publisher_with_presearch(
            &db,
            batch_limits(8 * 1024 * 1024, round.output_operations),
            DbConfig::new()
                .search_index_backfill()
                .active_text_mutation(),
            planning.workers(),
        );
        match planning {
            Planning::Alone | Planning::Racing(_) => drain(&publisher, &db).await,
            Planning::Replaying(_) => AWAIT_WORKERS.scope((), drain(&publisher, &db)).await,
        }
        rows.push(unpartitioned_vector_rows(&db).await);
    }
    db.close().await.unwrap();
    rows
}

async fn assert_identical(metric: VectorDistanceMetric, label: &str) {
    for seed in 0..SEEDS {
        let rounds = script(seed.wrapping_mul(0x9E37) ^ 0x5EED);
        let alone = run(
            &format!("{label}-alone-{seed}"),
            &rounds,
            metric,
            Planning::Alone,
        )
        .await;
        assert!(alone.iter().any(|rows| !rows.is_empty()));
        for planning in [
            Planning::Racing(2),
            Planning::Replaying(1),
            Planning::Replaying(2),
        ] {
            let name = format!("{label}-{planning:?}-{seed}");
            let beside = run(&name, &rounds, metric, planning).await;
            for (round, (expected, actual)) in alone.iter().zip(&beside).enumerate() {
                assert!(
                    expected == actual,
                    "seed {seed} round {round}: rows differ under {planning:?}"
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn euclidean_graphs_are_identical_with_and_without_workers() {
    let replayed = MEMOIZED_LOOKUPS.load(Ordering::Relaxed);
    assert_identical(VectorDistanceMetric::Euclidean, "euclidean").await;
    assert!(
        MEMOIZED_LOOKUPS.load(Ordering::Relaxed) > replayed,
        "no planner replayed a distance"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cosine_graphs_are_identical_with_and_without_workers() {
    assert_identical(VectorDistanceMetric::Cosine, "cosine").await;
}

#[tokio::test]
async fn a_single_threaded_runtime_runs_workers_too() {
    let rounds = script(7);
    let alone = run(
        "current-thread-alone",
        &rounds,
        VectorDistanceMetric::Euclidean,
        Planning::Alone,
    )
    .await;
    let beside = run(
        "current-thread-beside",
        &rounds,
        VectorDistanceMetric::Euclidean,
        Planning::Replaying(2),
    )
    .await;
    assert!(alone == beside, "rows differ on a current-thread runtime");
}
