//! Vector planning sessions retained between queue publication attempts.
//!
//! A session retained after one attempt's commit plans the target's next
//! attempt. It may change only how many rows planning reads from storage:
//! warm publication commits the rows cold publication commits, both agree
//! with a build of the final state, and every outcome other than a commit
//! forgets the session.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use helix_ast::{batch, graph::NodeRef, query::QueryRequest, query::SearchConsistency, traversal};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use tokio::sync::oneshot;

use super::lifecycle_tests::{config, create, drop_index, set_tenant, vector_spec, wait_terminal};
use super::overlay_tests::{add, delete, drain, update, vector_search, write};
use super::publication::{PublicationOutcome, QueuePublisher};
use super::publication_tests::{
    batch_limits, install_vector, mapped_partitions, physical_rows, publisher,
    unpartitioned_vector_rows,
};
use super::soak_tests::{assert_vector_graph, History};
use super::tests::{open, publisher_with_limits, queue, queued, target};
use super::QueueTarget;
use crate::config::{
    DbConfig, IndexOperationQueueTuning, SearchIndexBackfillLimits, VectorIndexDefinition,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::search::vector::distance::Euclidean;
use crate::search::vector::VectorDistanceMetric;
use crate::HelixDB;

/// Live vector documents: tenant and embedding by node ID.
type VectorState = BTreeMap<u64, (&'static str, [f32; 2])>;

/// A publisher over `db` whose output budget of `max_output_operations`
/// takes several attempts to publish a round.
fn narrow_publisher(db: &HelixDB, max_output_operations: u64) -> Arc<QueuePublisher> {
    publisher_with_limits(
        db,
        batch_limits(8 * 1024 * 1024, max_output_operations),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    )
}

/// Publishes `target` until its queue is empty, forgetting the retained
/// session after every attempt unless `retain`; returns the commits.
async fn publish_all(publisher: &QueuePublisher, target: QueueTarget, retain: bool) -> usize {
    let mut commits = 0;
    for _ in 0..1_000 {
        let outcome = publisher.publish_once(target).await.unwrap();
        if !retain {
            publisher.planning_cache().forget_publication(target).await;
        }
        match outcome {
            PublicationOutcome::Trimmed => {}
            PublicationOutcome::Published { .. } => {
                commits += 1;
                assert_eq!(
                    publisher
                        .planning_cache()
                        .retained_publication(target)
                        .is_some(),
                    retain,
                    "a commit retains its session unless the test forgets it"
                );
            }
            PublicationOutcome::Empty => return commits,
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => panic!("publication did not progress: {outcome:?}"),
        }
    }
    panic!("publication did not drain")
}

async fn remove_embedding(db: &HelixDB, id: u64) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "removed",
                traversal::g()
                    .n(NodeRef::from(id))
                    .remove_property("embedding"),
            ),
        )
    })
    .await;
}

/// Adds one document to `tenant` and tracks it in `state`.
async fn insert(
    db: &HelixDB,
    ids: &mut Vec<u64>,
    state: &mut VectorState,
    tenant: &'static str,
    embedding: [f32; 2],
) {
    let id = add(db, embedding, "doc", Some(tenant)).await;
    ids.push(id);
    state.insert(id, (tenant, embedding));
}

/// Applies one round of inserts, vector updates, tenant moves, deletes, and
/// removed or restored embeddings, tracking every live document in `state`.
///
/// Round 2 empties tenant `d`, whose partition publication reclaims, and
/// round 3 creates `d` again in a fresh namespace.
async fn churn(db: &HelixDB, round: u8, ids: &mut Vec<u64>, state: &mut VectorState) {
    match round {
        0 => {
            for index in 0..12_u8 {
                let tenant = ["a", "b", "c"][usize::from(index % 3)];
                insert(
                    db,
                    ids,
                    state,
                    tenant,
                    [f32::from(index % 5), f32::from(index / 5)],
                )
                .await;
            }
            insert(db, ids, state, "d", [7.0, 7.0]).await;
            insert(db, ids, state, "d", [8.0, 7.0]).await;
        }
        1 => {
            update(db, ids[0], [6.0, 1.0], "doc").await;
            state.get_mut(&ids[0]).unwrap().1 = [6.0, 1.0];
            set_tenant(db, ids[3], "b").await;
            state.get_mut(&ids[3]).unwrap().0 = "b";
            delete(db, ids[2]).await;
            state.remove(&ids[2]);
            remove_embedding(db, ids[4]).await;
            state.remove(&ids[4]);
        }
        2 => {
            delete(db, ids[12]).await;
            state.remove(&ids[12]);
            set_tenant(db, ids[13], "a").await;
            state.get_mut(&ids[13]).unwrap().0 = "a";
            update(db, ids[1], [0.0, 6.0], "doc").await;
            state.get_mut(&ids[1]).unwrap().1 = [0.0, 6.0];
        }
        _ => {
            insert(db, ids, state, "d", [7.0, 6.0]).await;
            insert(db, ids, state, "d", [6.0, 7.0]).await;
            // Document 4 lives in tenant `b`; its restored embedding is
            // inserted again.
            update(db, ids[4], [3.0, 3.0], "doc").await;
            state.insert(ids[4], ("b", [3.0, 3.0]));
            set_tenant(db, ids[5], "a").await;
            state.get_mut(&ids[5]).unwrap().0 = "a";
        }
    }
    for (offset, tenant) in (0_u8..).zip(["a", "b", "c"]) {
        insert(
            db,
            ids,
            state,
            tenant,
            [f32::from(round) + 9.0, f32::from(offset) * 2.0],
        )
        .await;
    }
}

/// Every hit of a whole-partition search, ordered by distance then ID.
async fn partition_hits(db: &HelixDB, tenant: &str, query: [f32; 2]) -> Vec<(u64, u64)> {
    let mut hits = vector_search(db, query, 64, Some(tenant), SearchConsistency::Strong).await;
    hits.sort_by_key(|(id, distance)| (*distance, *id));
    hits
}

/// Returns every physical row of every mapped tenant partition, by partition.
async fn partition_rows(db: &HelixDB) -> Vec<(u64, Vec<(bytes::Bytes, bytes::Bytes)>)> {
    let mut rows = Vec::new();
    for physical_index_id in mapped_partitions(db).await {
        rows.push((
            physical_index_id,
            physical_rows(db, physical_index_id).await,
        ));
    }
    rows
}

#[tokio::test]
async fn retained_sessions_publish_what_cold_sessions_and_a_build_publish() {
    let warm = open(
        "session-parity-warm",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    let cold = open(
        "session-parity-cold",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    let built = open(
        "session-parity-built",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&warm, Some("tenant")).await;
    install_vector(&cold, Some("tenant")).await;
    let (mut warm_ids, mut cold_ids, mut built_ids) = (Vec::new(), Vec::new(), Vec::new());
    let (mut state, mut cold_state, mut built_state) =
        (VectorState::new(), VectorState::new(), VectorState::new());
    let (warm_publisher, cold_publisher) =
        (narrow_publisher(&warm, 128), narrow_publisher(&cold, 128));
    let mut commits = 0;
    for round in 0..4 {
        churn(&warm, round, &mut warm_ids, &mut state).await;
        churn(&cold, round, &mut cold_ids, &mut cold_state).await;
        churn(&built, round, &mut built_ids, &mut built_state).await;
        let target = target(&warm, QueueFamily::Vector).await;
        commits += publish_all(&warm_publisher, target, true).await;
        publish_all(&cold_publisher, target, false).await;
        // Round 2 reclaimed tenant d and round 3 recreated it.
        assert_eq!(
            mapped_partitions(&warm).await.len(),
            if round == 2 { 3 } else { 4 }
        );
    }
    assert_eq!((&warm_ids, &state), (&cold_ids, &cold_state));
    assert_eq!((&warm_ids, &state), (&built_ids, &built_state));
    assert!(
        commits > 4,
        "rounds took several attempts, so sessions crossed commits: {commits}"
    );

    // Retention changes no row planning writes.
    let rows = partition_rows(&warm).await;
    assert!(rows.iter().all(|(_, rows)| !rows.is_empty()));
    assert!(
        rows == partition_rows(&cold).await,
        "warm and cold graphs differ"
    );
    let (warm_reads, cold_reads) = (
        warm_publisher.planning_cache().publication_reads(),
        cold_publisher.planning_cache().publication_reads(),
    );
    assert!(
        warm_reads < cold_reads,
        "retained sessions saved storage reads: {warm_reads} against {cold_reads}"
    );

    // A build of the final state indexes exactly the same documents.
    install_vector(&built, Some("tenant")).await;
    for tenant in ["a", "b", "c", "d"] {
        let expected = state
            .iter()
            .filter(|(_, (owner, _))| *owner == tenant)
            .map(|(id, _)| *id)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(!expected.is_empty());
        for query in [[3.31_f32, 2.77], [9.4, 0.6]] {
            let hits = partition_hits(&warm, tenant, query).await;
            assert_eq!(
                hits.iter()
                    .map(|(id, _)| *id)
                    .collect::<std::collections::BTreeSet<_>>(),
                expected,
                "tenant {tenant} holds exactly its live documents"
            );
            assert_eq!(hits, partition_hits(&built, tenant, query).await);
        }
    }
    for db in [warm, cold, built] {
        db.close().await.unwrap();
    }
}

/// Warm sessions under a budget of a few namespaces, trimmed by a build
/// checkout held every other attempt and evicting during planning, publish
/// the rows cold sessions under the default budget publish.
#[tokio::test]
async fn evicting_sessions_publish_what_cold_sessions_publish() {
    let warm = open(
        "session-evict-warm",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()).with_search_index_backfill_limits(
            SearchIndexBackfillLimits::default()
                .with_vector_build_cache_bytes(std::num::NonZeroU64::new(24 * 1024).unwrap()),
        ),
    )
    .await;
    let cold = open(
        "session-evict-cold",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&warm, Some("tenant")).await;
    install_vector(&cold, Some("tenant")).await;
    let (mut warm_ids, mut cold_ids) = (Vec::new(), Vec::new());
    let (mut state, mut cold_state) = (VectorState::new(), VectorState::new());
    let (warm_publisher, cold_publisher) =
        (narrow_publisher(&warm, 128), narrow_publisher(&cold, 128));
    let cache = warm_publisher.planning_cache();
    let mut commits = 0;
    for round in 0..4 {
        churn(&warm, round, &mut warm_ids, &mut state).await;
        churn(&cold, round, &mut cold_ids, &mut cold_state).await;
        let target = target(&warm, QueueFamily::Vector).await;
        let mut build = None;
        loop {
            // Holding the checkout halves the retained session's share, and
            // releasing it lets the next attempt grow again.
            build = match build {
                None => Some(cache.checkout_fresh::<Euclidean>().await),
                Some(_) => None,
            };
            match warm_publisher.publish_once(target).await.unwrap() {
                PublicationOutcome::Published { .. } => {
                    commits += 1;
                    // Only a drained session gives way to the build.
                    assert!(
                        cache.retained_publication(target).is_some()
                            || (build.is_some() && !warm_publisher.has_outstanding_work())
                    );
                }
                PublicationOutcome::Empty => break,
                outcome @ (PublicationOutcome::Discarded { .. }
                | PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Trimmed
                | PublicationOutcome::Blocked) => {
                    panic!("publication did not progress: {outcome:?}")
                }
            }
        }
        drop(build);
        publish_all(&cold_publisher, target, false).await;
    }
    assert_eq!((&warm_ids, &state), (&cold_ids, &cold_state));
    assert!(commits > 4, "sessions crossed commits: {commits}");
    assert!(
        cache.publication_evictions() > 0,
        "the budget evicted during planning"
    );
    assert_eq!(cold_publisher.planning_cache().publication_evictions(), 0);
    let rows = partition_rows(&warm).await;
    assert!(rows.iter().all(|(_, rows)| !rows.is_empty()));
    assert!(
        rows == partition_rows(&cold).await,
        "evicting warm and cold graphs differ"
    );
    for tenant in ["a", "b", "c", "d"] {
        for query in [[3.31_f32, 2.77], [9.4, 0.6]] {
            assert_eq!(
                partition_hits(&warm, tenant, query).await,
                partition_hits(&cold, tenant, query).await
            );
        }
    }
    for db in [warm, cold] {
        db.close().await.unwrap();
    }
}

/// An update a full batch discards is planned again, first, by the target's
/// next attempt, through the session the discarding attempt retained. The
/// discarded plan staged the node's new vector into that session, so were
/// the discard to keep it, the retry would read the new vector back, take
/// the update for a replay of the indexed state, and skip it.
///
/// Every document moves at its unchanged layer, so each update a batch
/// discards replaces an indexed vector. Retained sessions publish exactly
/// what fresh sessions that replace every upsert publish, and both leave a
/// valid graph: one reverse locator per link and no link to a removed node.
#[tokio::test]
async fn updates_discarded_from_full_batches_publish_through_the_retained_session() {
    let moved = |index: u8| [f32::from(index % 5) + 0.5, f32::from(index / 5) + 10.0];
    let mut runs = Vec::new();
    for retain in [true, false] {
        let name = if retain {
            "discarded-updates-warm"
        } else {
            "discarded-updates-cold"
        };
        let db = open(
            name,
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()),
        )
        .await;
        install_vector(&db, None).await;
        let mut ids = Vec::new();
        for index in 0..24_u8 {
            let embedding = [f32::from(index % 5), f32::from(index / 5)];
            ids.push(add(&db, embedding, "doc", None).await);
        }
        let target = target(&db, QueueFamily::Vector).await;
        drain(&db, target).await;
        for (index, id) in (0_u8..).zip(&ids) {
            update(&db, *id, moved(index), "doc").await;
        }

        // Every attempt selects each queued update, so each commit that
        // leaves one queued planned it and discarded it as `BatchFull`.
        let publisher = narrow_publisher(&db, 64);
        let reads = publisher.planning_cache().publication_reads();
        let commits = if retain {
            publish_all(&publisher, target, true).await
        } else {
            crate::search::vector::REPLACE_REPLAYS
                .scope((), publish_all(&publisher, target, false))
                .await
        };
        let reads = publisher.planning_cache().publication_reads() - reads;
        assert!(queue(&db, QueueFamily::Vector).await.is_none());
        assert!(db
            .index_operation_backlog()
            .outstanding_targets()
            .is_empty());
        for (index, id) in (0_u8..).zip(&ids) {
            let hits = vector_search(&db, moved(index), 1, None, SearchConsistency::Strong).await;
            assert_eq!(
                (hits[0].0, f64::from_bits(hits[0].1)),
                (*id, 0.0),
                "document {index} holds its new vector (retained sessions: {retain})"
            );
        }
        assert_vector_graph(&db, &History::new(), name).await;
        runs.push((commits, reads, unpartitioned_vector_rows(&db).await));
        db.close().await.unwrap();
    }
    let [(warm_commits, warm_reads, warm_rows), (cold_commits, cold_reads, cold_rows)] =
        <[_; 2]>::try_from(runs).unwrap();
    assert_eq!(warm_commits, cold_commits);
    assert!(
        warm_commits > 2,
        "full batches discarded several updates: {warm_commits} commits"
    );
    assert!(
        warm_reads < cold_reads,
        "retries planned through retained sessions: {warm_reads} against {cold_reads}"
    );
    assert!(warm_rows == cold_rows, "warm and cold graphs differ");
}

/// SHA-256 over every physical row [`publish_golden_workload`] leaves.
///
/// Planning caches decide only which rows publication reads again, so no
/// cache policy may change a byte of this graph.
const PUBLISHED_GOLDEN_DIGEST: &str =
    "9fcd3917bbbfbce187715f431e471b548ff48366d95d7798ca5f5277b22b8abb";
/// Physical row count [`publish_golden_workload`] leaves.
const PUBLISHED_GOLDEN_ROWS: usize = 7_954;

/// Integral embedding of `seed`. Every squared distance is exact in `f32`,
/// and embeddings repeat, so neighbor selection breaks many distance ties.
fn golden_embedding(seed: u16) -> [f32; 2] {
    [
        f32::from(seed.wrapping_mul(7) % 23),
        f32::from(seed.wrapping_mul(11) % 17),
    ]
}

/// Publishes 192 inserts, then two rounds of updates, same-value updates,
/// update chains, deletes, and inserts, each drained by `publisher`.
///
/// Returns the row count and SHA-256 of the graph it leaves.
async fn publish_golden_workload(
    db: &HelixDB,
    publisher: &QueuePublisher,
    retain: bool,
) -> (usize, String) {
    use sha2::{Digest, Sha256};

    let mut live = Vec::new();
    for seed in 0..192 {
        let embedding = golden_embedding(seed);
        live.push((add(db, embedding, "doc", None).await, embedding));
    }
    let target = target(db, QueueFamily::Vector).await;
    publish_all(publisher, target, retain).await;
    for round in 1..=2_u16 {
        let mut kept = Vec::new();
        for (index, (id, embedding)) in (0_u16..).zip(std::mem::take(&mut live)) {
            let moved = golden_embedding(index.wrapping_mul(3).wrapping_add(211 * round));
            let embedding = match (index + round) % 8 {
                0 | 3 => moved,
                // The queue replays the embedding the graph already holds.
                5 => embedding,
                // The chain collapses to its last value.
                6 => {
                    update(db, id, golden_embedding(index), "doc").await;
                    moved
                }
                7 => {
                    delete(db, id).await;
                    continue;
                }
                _ => {
                    kept.push((id, embedding));
                    continue;
                }
            };
            update(db, id, embedding, "doc").await;
            kept.push((id, embedding));
        }
        for seed in 0..32 {
            let embedding = golden_embedding(1_000 * round + seed);
            kept.push((add(db, embedding, "doc", None).await, embedding));
        }
        live = kept;
        publish_all(publisher, target, retain).await;
    }
    let rows = unpartitioned_vector_rows(db).await;
    let mut digest = Sha256::new();
    for (key, value) in &rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    let bytes: [u8; 32] = digest.finalize().into();
    (
        rows.len(),
        bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
    )
}

/// Warm, cold, and evicting planning sessions publish the pinned graph.
#[tokio::test]
async fn every_session_policy_publishes_the_pinned_golden_graph() {
    let default = SearchIndexBackfillLimits::default();
    let evicting =
        default.with_vector_build_cache_bytes(std::num::NonZeroU64::new(24 * 1024).unwrap());
    let mut published = Vec::new();
    for (name, limits, retain) in [
        ("published-golden-warm", default, true),
        ("published-golden-cold", default, false),
        ("published-golden-evicting", evicting, true),
    ] {
        let db = open(
            name,
            Arc::new(InMemory::new()),
            queued(IndexOperationQueueTuning::default()).with_search_index_backfill_limits(limits),
        )
        .await;
        install_vector(&db, None).await;
        let publisher = narrow_publisher(&db, 2_048);
        let graph = publish_golden_workload(&db, &publisher, retain).await;
        let evictions = publisher.planning_cache().publication_evictions();
        published.push((name, graph, evictions));
        db.close().await.unwrap();
    }
    for (name, (rows, digest), evictions) in &published {
        assert_eq!(
            (*rows, digest.as_str()),
            (PUBLISHED_GOLDEN_ROWS, PUBLISHED_GOLDEN_DIGEST),
            "{name} ({evictions} evictions): {published:?}"
        );
    }
    assert!(
        published[2].2 > 0,
        "the small budget evicted while planning: {published:?}"
    );
}

/// More vector targets with queued work than the cache keeps sessions for,
/// published round-robin: the retained targets stay warm rather than each
/// commit evicting the next target's session. Sessions of drained targets
/// then give way to a build.
#[tokio::test]
async fn round_robin_targets_past_the_session_bound_stay_partly_warm() {
    const INDEXES: usize = 20;
    const RETAINED: usize = 16;
    const DOCUMENTS: u8 = 3;
    let db = open(
        "session-round-robin",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    let properties = (0..INDEXES)
        .map(|index| format!("embedding_{index}"))
        .collect::<Vec<_>>();
    for property in &properties {
        db.install_index_for_tests(
            ValidatedDynamicIndexDefinition::try_from(
                VectorIndexDefinition::new_node(
                    "Doc",
                    property.as_str(),
                    2,
                    VectorDistanceMetric::Euclidean,
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    }
    for document in 0..DOCUMENTS {
        write(&db, || {
            let embedding = vec![f32::from(document), 1.0];
            QueryRequest::write(
                batch::write_batch().var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        properties
                            .iter()
                            .map(|property| {
                                (
                                    property.clone(),
                                    helix_ast::value::PropertyInput::from(embedding.clone()),
                                )
                            })
                            .collect::<Vec<_>>(),
                    ),
                ),
            )
        })
        .await;
    }
    let targets = db.index_operation_backlog().outstanding_targets();
    assert_eq!(targets.len(), INDEXES);
    // One entity per attempt, so each target takes one attempt per document.
    let publisher = publisher_with_limits(
        &db,
        batch_limits(1, 4_096),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let cache = publisher.planning_cache();
    let mut warm = Vec::new();
    for round in 0..DOCUMENTS {
        if round == 1 {
            // Every retained target has work left, so a build shares the
            // budget with their sessions and drops none.
            let build = cache.checkout_fresh::<Euclidean>().await;
            assert!(targets
                .iter()
                .take(RETAINED)
                .all(|target| cache.retained_publication(*target).is_some()));
            drop(build);
        }
        let mut hits = 0;
        for target in &targets {
            hits += usize::from(cache.retained_publication(*target).is_some());
            assert!(matches!(
                publisher.publish_once(*target).await.unwrap(),
                PublicationOutcome::Published { entities: 1, .. }
            ));
        }
        warm.push(hits);
    }
    assert_eq!(warm, [0, RETAINED, RETAINED]);
    assert!(!publisher.has_outstanding_work());

    // The last round drained every target: a build takes the whole budget
    // and drops their sessions.
    assert_eq!(
        targets
            .iter()
            .filter(|target| cache.retained_publication(**target).is_some())
            .count(),
        RETAINED
    );
    let build = cache.checkout_fresh::<Euclidean>().await;
    assert_eq!(
        u64::try_from(build.max_payload_bytes()).unwrap(),
        DbConfig::new()
            .search_index_backfill()
            .vector_build_cache_bytes()
            .get()
    );
    assert!(targets
        .iter()
        .all(|target| cache.retained_publication(*target).is_none()));
    drop(build);
    db.close().await.unwrap();
}

#[tokio::test]
async fn only_a_commit_keeps_the_target_session() {
    let db = open(
        "session-outcomes",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    let publisher = publisher(&db);
    let retained = |target| publisher.planning_cache().retained_publication(target);
    add(&db, [0.0, 0.0], "doc", None).await;
    let target = target(&db, QueueFamily::Vector).await;

    // Each commit retains its session at the next commit number.
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    let first = retained(target).expect("a commit retains its session");
    assert_eq!(first.target, target);
    add(&db, [1.0, 0.0], "doc", None).await;
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    let second = retained(target).expect("the next commit retains its session");
    assert!(second.commit > first.commit);
    assert_eq!(second.index_record_revision, first.index_record_revision);

    // An empty queue forgets it.
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Empty
    );
    assert!(retained(target).is_none());

    // Failures before and after the commit forget it.
    for (index, fail) in [
        &publisher.hooks().fail_before_commit,
        &publisher.hooks().fail_after_commit,
    ]
    .into_iter()
    .enumerate()
    {
        let x = f32::from(u8::try_from(index).unwrap());
        add(&db, [x, 3.0], "doc", None).await;
        assert!(retained(target).is_none(), "the queue was drained to Empty");
        assert!(matches!(
            publisher.publish_once(target).await.unwrap(),
            PublicationOutcome::Published { .. }
        ));
        add(&db, [x, 4.0], "doc", None).await;
        assert!(retained(target).is_some());
        fail.store(true, Ordering::SeqCst);
        assert_eq!(
            publisher.publish_once(target).await.unwrap(),
            PublicationOutcome::Retry
        );
        assert!(retained(target).is_none());
        drain(&db, target).await;
    }

    // A conflicting commit forgets it: rewriting the index record with its
    // own bytes commits inside the range the publication read.
    add(&db, [5.0, 0.0], "doc", None).await;
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    add(&db, [6.0, 0.0], "doc", None).await;
    assert!(retained(target).is_some());
    let (reached_tx, reached_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *publisher.hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let conflicting = async {
        reached_rx.await.unwrap();
        let storage = db.inner_db();
        let prefix = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::IndexRecord),
        );
        let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
        let record = rows.next().await.unwrap().expect("the index record exists");
        storage.put(&record.key, &record.value).await.unwrap();
        release_tx.send(()).unwrap();
    };
    let (outcome, ()) = tokio::join!(publisher.publish_once(target), conflicting);
    assert_eq!(outcome.unwrap(), PublicationOutcome::Retry);
    assert!(retained(target).is_none());

    // Trimming and blocking forget it too: a one-write budget cannot fit an
    // effect beside its acknowledgement.
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    let narrow = publisher_with_limits(
        &db,
        batch_limits(8 * 1024 * 1024, 1),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    add(&db, [7.0, 0.0], "doc", None).await;
    add(&db, [8.0, 0.0], "doc", None).await;
    assert!(retained(target).is_some());
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Trimmed
    );
    assert!(retained(target).is_none());
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    add(&db, [9.0, 0.0], "doc", None).await;
    assert!(retained(target).is_some());
    assert_eq!(
        narrow.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert!(retained(target).is_none());

    drain(&db, target).await;
    let hits = vector_search(&db, [9.0, 0.0], 1, None, SearchConsistency::Strong).await;
    assert_eq!(f64::from_bits(hits[0].1), 0.0, "every write was published");
    db.close().await.unwrap();
}

#[tokio::test]
async fn discarding_a_retired_queue_forgets_the_session() {
    let db = open("session-discard", Arc::new(InMemory::new()), config()).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    add(&db, [1.0, 1.0], "doc", Some("a")).await;
    let target = target(&db, QueueFamily::Vector).await;
    drain(&db, target).await;
    add(&db, [2.0, 2.0], "doc", Some("a")).await;
    let publisher = publisher(&db);
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    add(&db, [3.0, 3.0], "doc", Some("a")).await;
    assert!(publisher
        .planning_cache()
        .retained_publication(target)
        .is_some());

    // Holding the retired generation's ownership keeps its cleanup from
    // stepping, so only the discard can forget the session.
    let ownership = db.inner.index_scope_gates.publication_permit(target).await;
    drop_index(&db, vector_spec())
        .await
        .expect("dropping an Active index starts its cleanup");
    assert!(publisher
        .planning_cache()
        .retained_publication(target)
        .is_some());
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Discarded { operations: 1 }
    );
    assert!(publisher
        .planning_cache()
        .retained_publication(target)
        .is_none());
    drop(ownership);
    db.close().await.unwrap();
}

#[tokio::test]
async fn retirement_cleanup_forgets_the_session() {
    let db = open("session-retire", Arc::new(InMemory::new()), config()).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    add(&db, [1.0, 1.0], "doc", Some("a")).await;
    let target = target(&db, QueueFamily::Vector).await;
    drain(&db, target).await;
    add(&db, [2.0, 2.0], "doc", Some("a")).await;
    let publisher = publisher(&db);
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    assert!(publisher
        .planning_cache()
        .retained_publication(target)
        .is_some());

    // Automatic publication is paused and the queue is empty, so only the
    // cleanup of the retired generation runs.
    let cleanup = drop_index(&db, vector_spec())
        .await
        .expect("dropping an Active index starts its cleanup");
    assert_eq!(wait_terminal(&db, &cleanup).await, "succeeded");
    assert!(publisher
        .planning_cache()
        .retained_publication(target)
        .is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_reopened_writer_starts_cold_and_publishes_the_cold_graph() {
    // Both writers run the same writes and reopen at the same point, so they
    // allocate the same node IDs; only the first keeps sessions.
    let mut graphs = Vec::new();
    for (name, retain) in [
        ("session-restart-warm", true),
        ("session-restart-cold", false),
    ] {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open(
            name,
            Arc::clone(&store),
            queued(IndexOperationQueueTuning::default()),
        )
        .await;
        install_vector(&db, None).await;
        let mut ids = Vec::new();
        for index in 0..40_u8 {
            ids.push(
                add(
                    &db,
                    [f32::from(index % 7), f32::from(index / 7)],
                    "doc",
                    None,
                )
                .await,
            );
        }
        let target = target(&db, QueueFamily::Vector).await;
        publish_all(&narrow_publisher(&db, 512), target, retain).await;
        db.close().await.unwrap();

        let db = open(name, store, queued(IndexOperationQueueTuning::default())).await;
        let publisher = narrow_publisher(&db, 512);
        assert!(
            publisher
                .planning_cache()
                .retained_publication(target)
                .is_none(),
            "a reopened writer retains no session"
        );
        for index in 40..80_u8 {
            add(
                &db,
                [f32::from(index % 7), f32::from(index / 7)],
                "doc",
                None,
            )
            .await;
        }
        update(&db, ids[3], [0.5, 9.0], "doc").await;
        delete(&db, ids[4]).await;
        assert!(publish_all(&publisher, target, retain).await > 1);
        let hits = vector_search(&db, [0.5, 9.0], 1, None, SearchConsistency::Strong).await;
        assert_eq!(hits[0].0, ids[3]);
        graphs.push(unpartitioned_vector_rows(&db).await);
        db.close().await.unwrap();
    }
    assert!(
        graphs[0] == graphs[1],
        "restarted warm and cold graphs differ"
    );
}

/// Most keys one insert into a warm 300-node graph may read from storage:
/// twice the 8 it reads today, against about 200 with a cold session.
/// Planning reads the index metadata and the new node's absent rows; every
/// existing row it visits comes from the retained session.
const WARM_READS_PER_INSERT: u64 = 16;

#[tokio::test]
async fn a_warm_graph_plans_each_insert_with_few_storage_reads() {
    const INSERTS: u64 = 20;
    let db = open(
        "session-reads",
        Arc::new(InMemory::new()),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector(&db, None).await;
    for index in 0..300_u16 {
        add(
            &db,
            [f32::from(index % 17), f32::from(index / 17)],
            "doc",
            None,
        )
        .await;
    }
    let target = target(&db, QueueFamily::Vector).await;
    drain(&db, target).await;
    let publisher = publisher(&db);
    let cache = publisher.planning_cache();

    // The drain ended with Empty; one more commit leaves a warm session.
    add(&db, [0.5, 0.5], "doc", None).await;
    assert!(matches!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Published { .. }
    ));
    let mut per_insert = Vec::new();
    for retain in [true, false] {
        let before = cache.publication_reads();
        for index in 0..INSERTS {
            let offset = f32::from(u8::try_from(index).unwrap()) + 0.25;
            add(&db, [offset, 20.0 - offset], "doc", None).await;
            if !retain {
                cache.forget_publication(target).await;
            }
            assert!(matches!(
                publisher.publish_once(target).await.unwrap(),
                PublicationOutcome::Published { entities: 1, .. }
            ));
        }
        per_insert.push((cache.publication_reads() - before) / INSERTS);
    }
    let [warm, cold] = per_insert[..] else {
        unreachable!("one measurement per mode")
    };
    eprintln!("storage keys read per insert: warm {warm}, cold {cold}");
    assert!(
        warm <= WARM_READS_PER_INSERT,
        "a warm insert read {warm} keys from storage"
    );
    assert!(
        cold >= 4 * warm,
        "a cold session reads the visited rows again: {cold} against {warm}"
    );
    db.close().await.unwrap();
}
