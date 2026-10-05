//! The strong vector search bound and request-scoped pending reuse through
//! public queries.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use helix_ast::{
    batch,
    graph::NodeRef,
    query::{QueryRequest, SearchConsistency},
    traversal,
    value::PropertyInput,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::overlay_tests::{add, delete, drain, hits, text_search, update, vector_search, write};
use super::tests::{install_vector_and_text, open, queue, queued, target};
use crate::config::IndexOperationQueueTuning;
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::error::{HelixDbError, IndexBackpressureResource};
use crate::HelixDB;

/// Retained bytes of each pending vector entity's latest operation: what a
/// strong vector search decodes and the bound charges.
async fn latest_vector_bytes(db: &HelixDB) -> u64 {
    let Some(queue) = queue(db, QueueFamily::Vector).await else {
        return 0;
    };
    queue
        .operations()
        .iter()
        .map(|operation| (operation.entity(), operation.retained_bytes()))
        .collect::<HashMap<_, _>>()
        .values()
        .sum()
}

fn bounded(bytes: u64) -> crate::config::DbConfig {
    queued(
        IndexOperationQueueTuning::default()
            .with_strong_vector_search_max_pending_bytes(NonZeroU64::new(bytes).unwrap()),
    )
}

fn strong_vector_request(k: usize) -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], k, None),
            )
            .returning(["hits"]),
    )
}

fn assert_past_bound(error: &HelixDbError, reached: u64, limit: u64) {
    assert!(
        matches!(
            error,
            HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::PendingVectorBytes,
                requested,
                limit: reported,
                ..
            } if *requested == reached && *reported == limit
        ),
        "{error}"
    );
    assert!(error.is_index_backpressure(), "retryable: {error}");
    assert!(
        error.to_string().contains(&format!(
            "pending_vector_bytes would reach {reached}, limit {limit}"
        )),
        "{error}"
    );
}

/// Committed pending work exactly at the bound searches strongly; one byte
/// less fails strong vector searches, whole-index and prefiltered, with
/// retryable backpressure, while eventual vector and strong text searches
/// keep answering. Only each entity's latest operation counts, so
/// superseded updates do not, and a deletion counts its own record.
#[tokio::test]
async fn strong_vector_searches_fail_only_past_the_bound() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = "strong-vector-bound";
    let db = open(
        name,
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector_and_text(&db).await;
    let mut ids = Vec::new();
    for index in 0..4 {
        ids.push(add(&db, [index as f32, 0.0], "alpha", None).await);
    }
    // Superseded operations and a deletion.
    update(&db, ids[0], [0.5, 0.0], "alpha").await;
    update(&db, ids[0], [0.25, 0.0], "alpha").await;
    delete(&db, ids[3]).await;
    let bytes = latest_vector_bytes(&db).await;
    let all_operations = queue(&db, QueueFamily::Vector)
        .await
        .unwrap()
        .operations()
        .iter()
        .map(|operation| operation.retained_bytes())
        .sum::<u64>();
    assert!(
        all_operations > bytes,
        "superseded operations are not charged"
    );
    db.close().await.unwrap();

    let expected = [ids[0], ids[1], ids[2]];
    for (bound, within) in [(bytes, true), (bytes - 1, false)] {
        let db = open(name, Arc::clone(&store), bounded(bound)).await;
        let strong = Box::pin(db.query(strong_vector_request(10))).await;
        let prefiltered = Box::pin(
            db.query(QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "hits",
                        traversal::g()
                            .n(NodeRef::from(ids[..3].to_vec()))
                            .vector_search("Doc", "embedding", vec![0.0, 0.0], 10, None),
                    )
                    .returning(["hits"]),
            )),
        )
        .await;
        if within {
            for result in [strong, prefiltered] {
                let found = hits(&result.unwrap(), "hits")
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>();
                assert_eq!(found, expected, "exact at the bound");
            }
        } else {
            for result in [strong, prefiltered] {
                assert_past_bound(&result.expect_err("past the bound"), bytes, bound);
            }
        }
        let eventual = vector_search(&db, [0.0, 0.0], 10, None, SearchConsistency::Eventual).await;
        assert_eq!(
            eventual.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            expected,
            "eventual searches keep their own budget"
        );
        assert_eq!(
            text_search(&db, "alpha", 10, None, SearchConsistency::Strong)
                .await
                .len(),
            3,
            "the bound covers vector searches only"
        );
        db.close().await.unwrap();
    }

    // Once published, nothing is pending and the strong search answers.
    let db = open(name, Arc::clone(&store), bounded(bytes - 1)).await;
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    assert_eq!(latest_vector_bytes(&db).await, 0);
    assert_eq!(
        hits(
            &Box::pin(db.query(strong_vector_request(10))).await.unwrap(),
            "hits"
        )
        .into_iter()
        .map(|(id, _)| id)
        .collect::<Vec<_>>(),
        expected
    );
    db.close().await.unwrap();
}

/// "Insert one document per position, searching after each insert; then move
/// the first committed document and search again."
fn insert_and_search_each(positions: &[f32], moved: u64) -> QueryRequest {
    let batch = positions
        .iter()
        .enumerate()
        .fold(batch::write_batch(), |batch, (index, position)| {
            batch
                .var_as(
                    &format!("created{index}"),
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyInput::from(vec![*position, 0.0])),
                            ("body", PropertyInput::from("alpha".to_string())),
                        ],
                    ),
                )
                .var_as(
                    &format!("search{index}"),
                    traversal::g().vector_search_nodes(
                        "Doc",
                        "embedding",
                        vec![0.0, 0.0],
                        10,
                        None,
                    ),
                )
        })
        .var_as(
            "moved",
            traversal::g()
                .n(NodeRef::from(moved))
                .set_property("embedding", vec![50.0, 0.0]),
        )
        .var_as(
            "after_move",
            traversal::g().vector_search_nodes("Doc", "embedding", vec![0.0, 0.0], 10, None),
        );
    let returns = (0..positions.len())
        .flat_map(|index| [format!("created{index}"), format!("search{index}")])
        .chain(["after_move".to_string()]);
    QueryRequest::write(batch.returning(returns))
}

/// A write batch's searches reuse one read of the committed queue, and each
/// still sees every change the batch staged before it. Its own changes never
/// count toward the bound, however large; committed work past it fails the
/// whole batch retryably and commits nothing.
#[tokio::test]
async fn write_batch_searches_see_their_own_changes_and_exempt_them_from_the_bound() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = "strong-vector-bound-writes";
    let db = open(
        name,
        Arc::clone(&store),
        queued(IndexOperationQueueTuning::default()),
    )
    .await;
    install_vector_and_text(&db).await;
    let committed = add(&db, [1.0, 0.0], "alpha", None).await;
    let bytes = latest_vector_bytes(&db).await;
    db.close().await.unwrap();

    // One byte below the committed work: the batch fails before commit.
    let db = open(name, Arc::clone(&store), bounded(bytes - 1)).await;
    let error = Box::pin(db.query(insert_and_search_each(&[7.0], committed)))
        .await
        .expect_err("committed work past the bound fails the batch");
    assert_past_bound(&error, bytes, bytes - 1);
    assert_eq!(
        latest_vector_bytes(&db).await,
        bytes,
        "nothing from the failed batch committed"
    );
    db.close().await.unwrap();

    // At the bound: three of the batch's own inserts, far more than the
    // bound's room, search beside the committed document.
    let db = open(name, Arc::clone(&store), bounded(bytes)).await;
    let result = write(&db, || insert_and_search_each(&[3.0, 2.0, 0.5], committed)).await;
    let created = (0..3)
        .map(|index| {
            result[format!("created{index}")][0]["$id"]
                .as_u64()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let ids = |name: &str| {
        hits(&result, name)
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids("search0"), [committed, created[0]]);
    assert_eq!(ids("search1"), [committed, created[1], created[0]]);
    assert_eq!(
        ids("search2"),
        [created[2], committed, created[1], created[0]]
    );
    assert_eq!(
        ids("after_move"),
        [created[2], created[1], created[0], committed],
        "the batch's own move of a committed document is overlaid"
    );

    // With the committed work published, the batch's own changes alone
    // never fail it.
    drain(&db, target(&db, QueueFamily::Vector).await).await;
    let result = write(&db, || insert_and_search_each(&[7.0, 8.0], committed)).await;
    assert_eq!(hits(&result, "search1").len(), 6);
    db.close().await.unwrap();
}
