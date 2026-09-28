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
//! the graph load, because incremental inserts during the load are far slower
//! at this scale.

#[path = "../examples/scoped_search_bench/fixture.rs"]
mod fixture;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use db::production_coverage::{self, RestrictedSearchStrategy};
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
                    production_coverage::observe_restricted_vector_strategy(backend.query(request))
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

/// Recall counts a returned id that ties the k-th neighbour within float
/// rounding as a hit, and a clearly worse neighbour as a miss.
#[test]
fn recall_counts_a_near_tie_at_rank_k_but_not_a_worse_neighbour() {
    let query = [1.0, 0.0, 0.0, 0.0];
    let candidates = vec![
        (1, vec![1.0, 0.0, 0.0, 0.0]),
        // Cosine 0.6, the 2nd neighbour.
        (2, vec![3.0, 4.0, 0.0, 0.0]),
        // Cosine 0.6 - 1.2e-8: ranked 3rd, within 4 * f32::EPSILON of the 2nd.
        (3, vec![3.0, 4.0, 0.0, 1e-3]),
        // Cosine 0.59988: 1.2e-4 below the 2nd.
        (4, vec![3.0, 4.0, 0.0, 0.1]),
    ];
    let recall = |returned: &[u64]| {
        fixture::recall_at_k(&candidates, &query, 2, &returned.iter().copied().collect())
    };
    assert_eq!(recall(&[1, 2]), 1.0);
    assert_eq!(recall(&[1, 3]), 1.0);
    assert_eq!(recall(&[1, 4]), 0.5);
    assert_eq!(recall(&[3, 4]), 0.5);
    assert_eq!(fixture::recall_at_k(&[], &query, 2, &HashSet::new()), 0.0);
}

const OPERATION_ID: &str = "00000000-0000-4000-8000-000000000000";

/// Serves index-operation status polls with `replies` in order, repeating the
/// last one; a `None` reply accepts its poll and never answers it.
fn status_stub(replies: &'static [Option<&'static str>]) -> fixture::Backend {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        let mut unanswered = Vec::new();
        for (poll, mut stream) in listener.incoming().map_while(Result::ok).enumerate() {
            let _ = stream.read(&mut [0; 4_096]);
            let Some(body) = replies[poll.min(replies.len() - 1)] else {
                unanswered.push(stream);
                continue;
            };
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.shutdown(std::net::Shutdown::Write);
            let _ = std::io::copy(&mut stream, &mut std::io::sink());
        }
    });
    fixture::Backend::Http {
        client: reqwest::Client::builder().no_proxy().build().unwrap(),
        url,
    }
}

/// A build that never leaves the queue fails the wait at its deadline with
/// the last status, instead of hanging the gate.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(
    expected = r#"index vector did not succeed within 1s; last status: {"op":{"status":"queued"}}"#
)]
async fn index_wait_fails_at_its_deadline_with_the_last_status() {
    let backend = status_stub(&[Some(r#"{"op":{"status":"queued"}}"#)]);
    fixture::wait_for_operations(
        &backend,
        &serde_json::json!({ "vector": { "operation_id": OPERATION_ID } }),
        &["vector"],
        Duration::from_secs(1),
    )
    .await;
}

/// A status poll that never answers fails at the deadline naming the
/// operation it was polling, not an earlier one that already succeeded.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "index vector did not succeed within 1s; last status: null")]
async fn index_wait_deadline_names_the_operation_whose_poll_hangs() {
    let backend = status_stub(&[
        Some(r#"{"op":{"status":"queued"}}"#),
        Some(r#"{"op":{"status":"succeeded"}}"#),
        None,
    ]);
    fixture::wait_for_operations(
        &backend,
        &serde_json::json!({
            "text": { "operation_id": OPERATION_ID },
            "vector": { "operation_id": OPERATION_ID },
        }),
        &["text", "vector"],
        Duration::from_secs(1),
    )
    .await;
}

/// A status outside the public queued/running/succeeded/blocked/aborted set,
/// or none at all, fails the wait at once instead of spinning to the deadline.
#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = r#"index vector reported unexpected state None: {"op":null}"#)]
async fn index_wait_fails_fast_on_an_unexpected_status() {
    let backend = status_stub(&[Some(r#"{"op":null}"#)]);
    fixture::wait_for_operations(
        &backend,
        &serde_json::json!({ "vector": { "operation_id": OPERATION_ID } }),
        &["vector"],
        Duration::from_secs(30),
    )
    .await;
}
