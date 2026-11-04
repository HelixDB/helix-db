//! Release-only regression gate for traversal-scoped search on a synthetic
//! `Group <- Item -> Attribute` graph shaped like a production product graph.
//!
//! Every scoped vector shape must stay inside its traversal scope and keep
//! tie-aware recall@k against a client-side exact scan, at a floor set by the
//! restricted strategy it ran. The gate must exercise both strategies: the
//! kind-B scopes exceed the exact-scan admission limit and take the bounded
//! filtered graph walk, while the group scopes are scanned exactly. A
//! stored-property filter after expansion must count exactly the rows a
//! client-side filter keeps.
//!
//! ```text
//! cargo test --release -p db --features production-scale --test scoped_search_gate -- --ignored --nocapture
//! ```
//!
//! `HELIX_SCOPED_GATE_SCALE` (default 0.15, about 47k attributes and a 10k
//! kind-B scope) and `HELIX_SCOPED_GATE_DIM` (default 768) size the fixture.
//! A scale small enough that every scope is scanned exactly fails the gate,
//! because the walk would go unchecked. The vector index is backfilled after
//! the graph load, because publishing queued inserts while the load runs is
//! far slower at this scale. The load returns once every queued index
//! operation is published, and the gate checks that before measuring recall,
//! so no search overlays unpublished work and each scope keeps one strategy.

#![recursion_limit = "256"]

#[path = "../examples/scoped_search_bench/fixture.rs"]
mod fixture;

use std::collections::HashSet;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use db::production_scale::{self, RestrictedSearchStrategy};
use db::{DbConfig, HelixDB};
use helix_ast::prelude::*;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

const QUERIES: usize = 12;
/// An exact scan ranks every candidate, so it must return every neighbour.
/// Recall is tie-aware (see `fixture::recall_at_k`), so a float near-tie the
/// server orders the other way at rank k is not a miss, even at k = 5.
const EXACT_MIN_RECALL: f64 = 1.0;
/// The bounded walk's floor on the default fixture, pinned below its measured
/// baseline: kind-B recall@50 was 0.942 on each of three runs.
const WALK_MIN_RECALL: f64 = 0.90;
/// Bounds each index wait; the default fixture's vector backfill takes about
/// four minutes in a release build.
const INDEX_DEADLINE: Duration = Duration::from_secs(30 * 60);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "release-only scoped search recall and filter-semantics gate"]
async fn scoped_search_keeps_recall_and_exact_filter_semantics() {
    let scale = fixture::env_or("HELIX_SCOPED_GATE_SCALE", 0.15);
    let dimension = fixture::env_or("HELIX_SCOPED_GATE_DIM", 768);
    let store = Arc::new(fixture::CountingStore::new(
        Arc::new(InMemory::new()),
        Duration::ZERO,
    ));
    let db = HelixDB::open_with_object_store_and_config(
        "scoped-search-gate",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        DbConfig::new(),
    )
    .await
    .unwrap();
    let backend = fixture::Backend::Embedded { db, store };
    fixture::load(
        &backend,
        fixture::LoadOptions {
            scale,
            dimension,
            items_per_batch: 4,
            vector: fixture::VectorBuild::After,
            index_deadline: INDEX_DEADLINE,
        },
    )
    .await;
    let fixture::Backend::Embedded { db, .. } = &backend else {
        unreachable!("embedded backend was constructed above")
    };
    let queue = db.index_operation_queue_stats();
    assert_eq!(
        queue.pending_operations, 0,
        "recall must run against published indexes: {queue:?}"
    );

    let vectors = fixture::query_vectors(QUERIES, dimension);
    let shape_count = fixture::shapes(&vectors[0]).len();
    let mut strategies = Vec::new();
    for shape_index in 0..shape_count {
        let (name, scope, _) = fixture::shapes(&vectors[0]).swap_remove(shape_index);
        let Some((scope, k)) = scope else {
            continue;
        };
        let candidates = fixture::candidate_embeddings(&backend, scope).await;
        let candidate_ids = candidates.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
        let observed = futures::future::join_all(vectors.iter().map(|vector| {
            let (_, _, request) = fixture::shapes(vector).swap_remove(shape_index);
            let backend = &backend;
            let candidates = &candidates;
            let candidate_ids = &candidate_ids;
            async move {
                let (response, strategy) =
                    production_scale::observe_restricted_vector_strategy(backend.query(request))
                        .await;
                let returned = response.unwrap()["r"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["id"].as_u64().unwrap())
                    .collect::<HashSet<_>>();
                assert!(
                    returned.is_subset(candidate_ids),
                    "{name} returned ids outside its traversal scope"
                );
                (
                    strategy,
                    fixture::recall_at_k(candidates, vector, k, &returned),
                )
            }
        }))
        .await;
        let Some(strategy) = observed[0].0 else {
            panic!("{name} ran no restricted vector search");
        };
        assert!(
            observed
                .iter()
                .all(|(observed, _)| *observed == Some(strategy)),
            "{name} changed restricted strategy between queries"
        );
        let floor = match strategy {
            RestrictedSearchStrategy::Exact => EXACT_MIN_RECALL,
            RestrictedSearchStrategy::FilteredGraph => WALK_MIN_RECALL,
        };
        let mean = observed.iter().map(|(_, recall)| recall).sum::<f64>() / observed.len() as f64;
        println!(
            "{name}: strategy={strategy:?} recall={mean:.3} candidates={}",
            candidates.len()
        );
        assert!(
            mean >= floor,
            "{name} {strategy:?} recall {mean:.3} < {floor}"
        );
        strategies.push(strategy);
    }
    assert!(
        strategies.contains(&RestrictedSearchStrategy::Exact),
        "no scoped shape was scanned exactly"
    );
    assert!(
        strategies.contains(&RestrictedSearchStrategy::FilteredGraph),
        "no scoped shape reached the filtered graph walk; raise HELIX_SCOPED_GATE_SCALE"
    );

    // The post-expansion filter may be answered from index bitmaps; it must
    // agree with filtering the expanded rows client-side.
    let filtered = backend
        .query(fixture::read(
            fixture::group_scope()
                .where_(Predicate::eq("kind", "B"))
                .count(),
        ))
        .await
        .unwrap();
    let expanded = backend
        .query(fixture::read(
            fixture::group_scope().project(vec![PropertyProjection::new("kind")]),
        ))
        .await
        .unwrap();
    let expected = expanded["r"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["kind"] == "B")
        .count() as u64;
    assert!(expected > 0, "fixture has kind-B attributes in scope");
    assert_eq!(filtered["r"].as_u64(), Some(expected));
}

/// The largest item the fixture generates, `TOTAL_ATTRIBUTES / TOTAL_ITEMS`
/// scaled by at most 1.6 and rounded. Each attribute is one pending member of
/// the vector and of the text index, so this is the smallest member limit that
/// admits every one-item batch on its own.
const MAX_ITEM_ATTRIBUTES: u64 = 122;

/// An embedded load that saturates the async index queue is throttled by
/// retryable backpressure rather than failing, and returns only once the index
/// worker has published every queued operation.
///
/// With one item per batch and a member limit of one item, any two items in a
/// row of this 12-item load exceed the limit, so each batch after the first is
/// admitted only once its predecessor is at least partly published.
#[tokio::test(flavor = "multi_thread")]
async fn embedded_load_retries_backpressure_and_drains_the_index_queue() {
    let store = Arc::new(fixture::CountingStore::new(
        Arc::new(InMemory::new()),
        Duration::ZERO,
    ));
    let tuning = db::config::IndexOperationQueueTuning::default()
        .with_max_members(NonZeroU64::new(MAX_ITEM_ATTRIBUTES).unwrap());
    let db = HelixDB::open_with_object_store_and_config(
        "scoped-search-backpressure",
        Arc::clone(&store) as Arc<dyn ObjectStore>,
        DbConfig::new().with_index_operation_queue_tuning(tuning),
    )
    .await
    .unwrap();
    let backend = fixture::Backend::Embedded { db, store };
    let backpressured = fixture::load(
        &backend,
        fixture::LoadOptions {
            scale: 12.0 / fixture::TOTAL_ITEMS,
            dimension: 8,
            items_per_batch: 1,
            vector: fixture::VectorBuild::Before,
            index_deadline: Duration::from_secs(300),
        },
    )
    .await;
    assert!(backpressured > 0, "no load batch was backpressured");
    let fixture::Backend::Embedded { db, .. } = &backend else {
        unreachable!("embedded backend was constructed above")
    };
    let queue = db.index_operation_queue_stats();
    assert_eq!(
        queue.pending_operations, 0,
        "load returned with unpublished index work: {queue:?}"
    );
}
