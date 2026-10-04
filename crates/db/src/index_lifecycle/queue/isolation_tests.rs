//! Deterministic planning failures hold back only their own entity, while
//! transient failures still retry the whole batch.
//!
//! Failures are injected through [`PublicationHooks::planning_failures`],
//! keyed by the operation an entity's selection ends at, so a newer write to
//! the entity supersedes one. A corrupt injection fails the real planner: a
//! vector replacement of the wrong dimension, or a text effect naming the
//! other element kind.
//!
//! [`PublicationHooks::planning_failures`]: super::publication::test_hooks::PublicationHooks

use std::collections::HashSet;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use helix_ast::{
    batch, graph::NodeRef, query::QueryRequest, query::SearchConsistency, traversal,
    value::PropertyInput,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::overlay_tests::{text_search, vector_search, write};
use super::publication::test_hooks::InjectedPlanningFailure;
use super::publication::{FailureKind, NextTarget, PublicationOutcome, QueuePublisher};
use super::publication_tests::{install_vector, publisher};
use super::tests::{
    add_doc, install_vector_and_text, open, publisher_with_limits, queue, queued, target,
};
use super::QueueTarget;
use crate::config::{DbConfig, IndexOperationQueueTuning, TextIndexDefinition};
use crate::encoding::v2::values::indexes::operation_queue::{QueueFamily, QueuedOperationId};
use crate::error::HelixDbError;
use crate::index_lifecycle::ValidatedDynamicIndexDefinition;
use crate::HelixDB;

/// Paused publication, and eventual searches that overlay no queued work, so
/// they see exactly what is published.
fn tuning() -> IndexOperationQueueTuning {
    IndexOperationQueueTuning::default().with_eventual_search_budget_for_tests(0)
}

async fn install_text(db: &HelixDB) {
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Doc", "body").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
}

async fn set(db: &HelixDB, id: u64, property: &'static str, value: PropertyInput) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property(property, value.clone()),
            ),
        )
    })
    .await;
}

/// Entity IDs of `family`'s queued operations, in queue order.
async fn queued_ids(db: &HelixDB, family: QueueFamily) -> Vec<u64> {
    queue(db, family).await.map_or_else(Vec::new, |queue| {
        queue
            .operations()
            .iter()
            .map(|operation| operation.entity().id.get())
            .collect()
    })
}

/// The newest queued operation of entity `id` in `family`'s queue.
async fn newest(db: &HelixDB, family: QueueFamily, id: u64) -> QueuedOperationId {
    queue(db, family)
        .await
        .unwrap()
        .operations()
        .iter()
        .rev()
        .find(|operation| operation.entity().id.get() == id)
        .unwrap()
        .id()
}

fn inject(db: &HelixDB, operation: QueuedOperationId, failure: Option<InjectedPlanningFailure>) {
    inject_into(publisher(db), operation, failure);
}

fn inject_into(
    publisher: &QueuePublisher,
    operation: QueuedOperationId,
    failure: Option<InjectedPlanningFailure>,
) {
    let mut failures = publisher.hooks().planning_failures.lock();
    match failure {
        Some(failure) => failures.insert(operation, failure),
        None => failures.remove(&operation),
    };
}

/// Publishes `target` until only held-back work is left, returning every
/// outcome.
async fn settle(db: &HelixDB, target: QueueTarget) -> Vec<PublicationOutcome> {
    let mut outcomes = Vec::new();
    for _ in 0..32 {
        let outcome = publisher(db).publish_once(target).await.unwrap();
        outcomes.push(outcome);
        match outcome {
            PublicationOutcome::Stalled | PublicationOutcome::Empty => return outcomes,
            PublicationOutcome::Published { .. }
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked => {}
            PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry => panic!("publication did not progress: {outcomes:?}"),
        }
    }
    panic!("publication did not settle: {outcomes:?}")
}

/// The published entity nearest `point`.
async fn published_nearest(db: &HelixDB, point: [f32; 2]) -> Option<u64> {
    vector_search(db, point, 1, None, SearchConsistency::Eventual)
        .await
        .first()
        .map(|(id, _)| *id)
}

async fn published_text(db: &HelixDB, term: &str) -> Vec<u64> {
    text_search(db, term, 10, None, SearchConsistency::Eventual)
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

fn held(db: &HelixDB) -> Vec<u64> {
    db.blocked_index_entities()
        .into_iter()
        .map(|entity| entity.id.get())
        .collect()
}

#[test]
fn failures_are_classified_by_what_retrying_can_change() {
    for (error, kind) in [
        (HelixDbError::DatabaseClosed, FailureKind::Fatal),
        (
            HelixDbError::WriterFencedCommitOutcomeUnknown,
            FailureKind::Fatal,
        ),
        (
            HelixDbError::ObjectStore(slatedb::object_store::Error::Generic {
                store: "test",
                source: "outage".into(),
            }),
            FailureKind::Transient,
        ),
        (
            HelixDbError::TransactionConflict("conflict".to_string()),
            FailureKind::Transient,
        ),
        (
            HelixDbError::QueryCancelledByReaderRetirement,
            FailureKind::Transient,
        ),
        (
            HelixDbError::InvariantViolation("neighbor set contains its owner 7".to_string()),
            FailureKind::Deterministic,
        ),
        (
            HelixDbError::IndexCatalogCorruption("no metadata".to_string()),
            FailureKind::Deterministic,
        ),
        (
            HelixDbError::InvalidDimension {
                expected: 2,
                got: 3,
            },
            FailureKind::Deterministic,
        ),
    ] {
        assert_eq!(FailureKind::of(&error), kind, "{error}");
    }
}

/// A queued vector update that fails to plan on every attempt is held back
/// alone: every other entity keeps publishing, so writes to them are never
/// refused even with the member cap at two, until a newer write to it
/// publishes.
#[tokio::test]
async fn a_vector_entity_that_fails_to_plan_is_held_back_alone() {
    let db = open(
        "isolate-vector",
        Arc::new(InMemory::new()),
        queued(tuning().with_max_members(NonZeroU64::new(2).unwrap())),
    )
    .await;
    install_vector(&db, None).await;
    let mut published = Vec::new();
    for x in 0..4_u8 {
        published.push(add_doc(&db, vec![f32::from(x), 0.0], "doc").await.unwrap());
        let target = target(&db, QueueFamily::Vector).await;
        assert_eq!(
            settle(&db, target).await,
            [
                PublicationOutcome::Published {
                    operations: 1,
                    entities: 1
                },
                PublicationOutcome::Empty
            ]
        );
    }
    let target = target(&db, QueueFamily::Vector).await;
    let failing = published[1];
    set(&db, failing, "embedding", vec![5.0_f32, 5.0].into()).await;
    let corrupt = newest(&db, QueueFamily::Vector, failing).await;
    inject(&db, corrupt, Some(InjectedPlanningFailure::Corrupt));

    for (round, x) in (10..14_u8).enumerate() {
        // The held update and this insert fill the member cap, so the next
        // insert is admitted only once this one publishes.
        let inserted = add_doc(&db, vec![f32::from(x), 0.0], "doc")
            .await
            .unwrap_or_else(|error| panic!("round {round}: a write was refused: {error}"));
        let outcomes = settle(&db, target).await;
        let expected = [
            PublicationOutcome::Published {
                operations: 1,
                entities: 1,
            },
            PublicationOutcome::Stalled,
        ];
        if round == 0 {
            // The first attempt plans the update first and holds it back.
            assert_eq!(outcomes[0], PublicationOutcome::Blocked);
            assert_eq!(outcomes[1..], expected);
        } else {
            assert_eq!(outcomes, expected, "round {round}: the update stays held");
        }
        assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [failing]);
        assert_eq!(held(&db), [failing]);
        assert_eq!(db.blocked_index_entity_count(), 1);
        assert_eq!(
            published_nearest(&db, [f32::from(x), 0.0]).await,
            Some(inserted)
        );
    }
    let stats = db.index_operation_queue_stats();
    assert_eq!(stats.blocked_entities, 1);
    assert_eq!(stats.blocked_attempts, 1, "the update was planned once");
    assert_eq!(stats.publication_error_retries, 0);
    // The held update is queued, never acknowledged or dropped.
    assert_eq!(published_nearest(&db, [1.0, 0.0]).await, Some(failing));
    assert_eq!(
        vector_search(&db, [5.0, 5.0], 1, None, SearchConsistency::Strong).await[0].0,
        failing,
        "strong search serves the held update"
    );

    // A newer write supersedes the corrupt one, and its repair publishes both.
    set(&db, failing, "embedding", vec![6.0_f32, 6.0].into()).await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert!(held(&db).is_empty());
    assert_eq!(db.index_operation_queue_stats().blocked_entities, 0);
    assert!(queued_ids(&db, QueueFamily::Vector).await.is_empty());
    assert_eq!(published_nearest(&db, [6.0, 6.0]).await, Some(failing));
    db.close().await.unwrap();
}

/// A failure after other entities of the batch planned holds back the
/// failing entity, not the batch's first: the attempt commits nothing, and
/// the entities planned before it publish from the next attempt.
#[tokio::test]
async fn a_vector_entity_failing_after_planned_ones_is_the_one_held_back() {
    let db = open(
        "isolate-vector-position",
        Arc::new(InMemory::new()),
        queued(tuning()),
    )
    .await;
    install_vector(&db, None).await;
    let failing = add_doc(&db, vec![0.0, 0.0], "doc").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    settle(&db, target).await;
    let first = add_doc(&db, vec![1.0, 0.0], "doc").await.unwrap();
    let second = add_doc(&db, vec![2.0, 0.0], "doc").await.unwrap();
    set(&db, failing, "embedding", vec![5.0_f32, 5.0].into()).await;
    assert_eq!(
        queued_ids(&db, QueueFamily::Vector).await,
        [first, second, failing],
        "one batch plans the update after both inserts"
    );
    let corrupt = newest(&db, QueueFamily::Vector, failing).await;
    inject(&db, corrupt, Some(InjectedPlanningFailure::Corrupt));
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(held(&db), [failing]);
    assert_eq!(
        queued_ids(&db, QueueFamily::Vector).await,
        [first, second, failing],
        "the failed attempt commits nothing"
    );
    assert_eq!(
        settle(&db, target).await,
        [
            PublicationOutcome::Published {
                operations: 2,
                entities: 2
            },
            PublicationOutcome::Stalled,
        ]
    );
    assert_eq!(held(&db), [failing]);
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [failing]);
    assert_eq!(published_nearest(&db, [1.0, 0.0]).await, Some(first));
    assert_eq!(published_nearest(&db, [2.0, 0.0]).await, Some(second));
    assert_eq!(
        published_nearest(&db, [0.0, 0.0]).await,
        Some(failing),
        "the held update is not published"
    );
    db.close().await.unwrap();
}

/// An entity whose planning failed is planned again once its retry is due,
/// without a write: a stalled generation waits only until that retry, an
/// entity that still fails is held back again, and one whose failure was
/// repaired publishes.
#[tokio::test]
async fn a_failed_entity_is_planned_again_once_its_retry_is_due() {
    let db = open("isolate-retry", Arc::new(InMemory::new()), queued(tuning())).await;
    install_vector(&db, None).await;
    let failing = add_doc(&db, vec![1.0, 0.0], "doc").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    settle(&db, target).await;
    // The writer's own publisher never schedules in tests; one with the same
    // limits does.
    let defaults = DbConfig::new().search_index_backfill();
    let scheduler = publisher_with_limits(&db, defaults.batch(), defaults.active_text_mutation());
    set(&db, failing, "embedding", vec![5.0_f32, 5.0].into()).await;
    let corrupt = newest(&db, QueueFamily::Vector, failing).await;
    inject_into(&scheduler, corrupt, Some(InjectedPlanningFailure::Corrupt));
    let blocked =
        |scheduler: &QueuePublisher| scheduler.metrics().blocked_attempts.load(Ordering::Relaxed);
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    let stalled = Instant::now();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Stalled,
        "the retry is not due yet"
    );
    let none = HashSet::new();
    let NextTarget::Delayed(deadline) = scheduler.next_target(&none, stalled) else {
        panic!("a held entity waits for its retry");
    };
    assert!(
        deadline < stalled + Duration::from_secs(60),
        "the stall ends when the retry is due, not a full wait after the stall"
    );
    assert_eq!(
        scheduler.next_target(&none, deadline),
        NextTarget::Ready(target)
    );

    // Still failing when due: planned once more and held back again.
    scheduler.make_failed_retries_due();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(blocked(&scheduler), 2);
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Stalled
    );
    assert_eq!(blocked(&scheduler), 2, "a failed retry waits for the next");

    // Once what failed is repaired, the due retry publishes it.
    inject_into(&scheduler, corrupt, None);
    scheduler.make_failed_retries_due();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert!(scheduler.blocked_entities().is_empty());
    assert!(queued_ids(&db, QueueFamily::Vector).await.is_empty());
    assert_eq!(published_nearest(&db, [5.0, 5.0]).await, Some(failing));
    db.close().await.unwrap();
}

/// A second entity failing to plan with no publication between suggests a
/// failure that is not the entities' own, such as a partition's missing
/// metadata, so the next attempt backs off instead of holding back the queue
/// one immediate attempt at a time; a publication ends the backoff.
#[tokio::test]
async fn consecutive_planning_failures_back_off() {
    let db = open(
        "isolate-backoff",
        Arc::new(InMemory::new()),
        queued(tuning()),
    )
    .await;
    install_vector(&db, None).await;
    let mut ids = Vec::new();
    for x in 0..3_u8 {
        ids.push(add_doc(&db, vec![f32::from(x), 0.0], "doc").await.unwrap());
    }
    let target = target(&db, QueueFamily::Vector).await;
    let defaults = DbConfig::new().search_index_backfill();
    let scheduler = publisher_with_limits(&db, defaults.batch(), defaults.active_text_mutation());
    for id in &ids[..2] {
        let corrupt = newest(&db, QueueFamily::Vector, *id).await;
        inject_into(&scheduler, corrupt, Some(InjectedPlanningFailure::Corrupt));
    }
    let none = HashSet::new();
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    assert_eq!(
        scheduler.next_target(&none, Instant::now()),
        NextTarget::Ready(target),
        "one failure holds back only its entity"
    );
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Blocked
    );
    let NextTarget::Delayed(retry) = scheduler.next_target(&none, Instant::now()) else {
        panic!("a second failure in a row backs off");
    };
    assert_eq!(
        scheduler.next_target(&none, retry),
        NextTarget::Ready(target)
    );
    assert_eq!(
        scheduler.publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    assert_eq!(
        scheduler.next_target(&none, Instant::now()),
        NextTarget::Ready(target),
        "a publication ends the backoff"
    );
    let mut blocked = scheduler
        .blocked_entities()
        .into_iter()
        .map(|(_, entity)| entity.id.get())
        .collect::<Vec<_>>();
    blocked.sort_unstable();
    assert_eq!(blocked, ids[..2]);
    db.close().await.unwrap();
}

/// A text epoch fails as a whole, so its entity ceiling halves until the
/// failing entity publishes alone and is held back; every other entity
/// publishes, and a newer write to it publishes it.
#[tokio::test]
async fn a_text_entity_that_fails_to_plan_is_held_back_alone() {
    let db = open("isolate-text", Arc::new(InMemory::new()), queued(tuning())).await;
    install_text(&db).await;
    let mut ids = Vec::new();
    for word in ["apple", "banana", "cherry", "damson"] {
        ids.push(add_doc(&db, vec![0.0, 0.0], word).await.unwrap());
    }
    let target = target(&db, QueueFamily::Text).await;
    let corrupt = newest(&db, QueueFamily::Text, ids[2]).await;
    inject(&db, corrupt, Some(InjectedPlanningFailure::Corrupt));
    let published = |entities| PublicationOutcome::Published {
        operations: entities,
        entities,
    };
    assert_eq!(
        settle(&db, target).await,
        [
            PublicationOutcome::Trimmed,
            published(2),
            PublicationOutcome::Trimmed,
            PublicationOutcome::Blocked,
            published(1),
            PublicationOutcome::Stalled,
        ]
    );
    assert_eq!(queued_ids(&db, QueueFamily::Text).await, [ids[2]]);
    assert_eq!(held(&db), [ids[2]]);
    for (id, word) in ids.iter().zip(["apple", "banana", "cherry", "damson"]) {
        let expected = if *id == ids[2] { vec![] } else { vec![*id] };
        assert_eq!(published_text(&db, word).await, expected, "{word}");
    }
    assert_eq!(db.index_operation_queue_stats().blocked_attempts, 1);

    set(&db, ids[2], "body", "cherry repaired".to_string().into()).await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert!(held(&db).is_empty());
    assert!(queued_ids(&db, QueueFamily::Text).await.is_empty());
    assert_eq!(published_text(&db, "repaired").await, [ids[2]]);
    db.close().await.unwrap();
}

/// A transient planning failure retries the whole batch after backoff and
/// holds nothing back, for both families; the batch publishes once the
/// failure clears.
#[tokio::test]
async fn a_transient_planning_failure_retries_the_whole_batch() {
    let db = open(
        "isolate-transient",
        Arc::new(InMemory::new()),
        queued(tuning()),
    )
    .await;
    install_vector_and_text(&db).await;
    let mut ids = Vec::new();
    for x in 0..3_u8 {
        ids.push(add_doc(&db, vec![f32::from(x), 0.0], "word").await.unwrap());
    }
    for family in [QueueFamily::Vector, QueueFamily::Text] {
        let target = target(&db, family).await;
        let unavailable = newest(&db, family, ids[1]).await;
        inject(&db, unavailable, Some(InjectedPlanningFailure::Unavailable));
        for attempt in 1..=2 {
            assert_eq!(
                publisher(&db).publish_once(target).await.unwrap(),
                PublicationOutcome::Retry,
                "{family:?} attempt {attempt}"
            );
            assert_eq!(queued_ids(&db, family).await, ids, "{family:?}");
            assert!(held(&db).is_empty(), "{family:?}");
        }
        inject(&db, unavailable, None);
        assert_eq!(
            publisher(&db).publish_once(target).await.unwrap(),
            PublicationOutcome::Published {
                operations: 3,
                entities: 3
            },
            "{family:?}"
        );
    }
    let stats = db.index_operation_queue_stats();
    assert_eq!(stats.publication_error_retries, 4);
    assert_eq!(stats.blocked_attempts, 0);
    assert_eq!(stats.blocked_entities, 0);
    db.close().await.unwrap();
}

/// Holding an entity back is process memory: after a restart the failing
/// update is planned again, held back again in one attempt, and the
/// generation keeps publishing around it.
#[tokio::test]
async fn an_entity_held_back_before_a_restart_is_held_back_again_after_it() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("isolate-restart", Arc::clone(&store), queued(tuning())).await;
    install_vector(&db, None).await;
    let kept = add_doc(&db, vec![0.0, 0.0], "doc").await.unwrap();
    let failing = add_doc(&db, vec![1.0, 0.0], "doc").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    settle(&db, target).await;
    set(&db, failing, "embedding", vec![5.0_f32, 5.0].into()).await;
    let corrupt = newest(&db, QueueFamily::Vector, failing).await;
    inject(&db, corrupt, Some(InjectedPlanningFailure::Corrupt));
    assert_eq!(
        settle(&db, target).await,
        [PublicationOutcome::Blocked, PublicationOutcome::Stalled]
    );
    assert_eq!(held(&db), [failing]);
    db.close().await.unwrap();

    let db = open("isolate-restart", store, queued(tuning())).await;
    assert!(held(&db).is_empty(), "nothing durable records the hold");
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [failing]);
    inject(&db, corrupt, Some(InjectedPlanningFailure::Corrupt));
    let inserted = add_doc(&db, vec![2.0, 0.0], "doc").await.unwrap();
    assert_eq!(
        settle(&db, target).await,
        [
            PublicationOutcome::Blocked,
            PublicationOutcome::Published {
                operations: 1,
                entities: 1
            },
            PublicationOutcome::Stalled,
        ]
    );
    assert_eq!(held(&db), [failing]);
    assert_eq!(queued_ids(&db, QueueFamily::Vector).await, [failing]);
    assert_eq!(published_nearest(&db, [2.0, 0.0]).await, Some(inserted));
    assert_eq!(published_nearest(&db, [0.0, 0.0]).await, Some(kept));

    set(&db, failing, "embedding", vec![6.0_f32, 6.0].into()).await;
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 2,
            entities: 1
        }
    );
    assert!(held(&db).is_empty());
    assert_eq!(published_nearest(&db, [6.0, 6.0]).await, Some(failing));
    db.close().await.unwrap();
}

/// The publisher's error path classifies a failure outside one entity's
/// planning too: a corrupt queue value has no entity to hold back, so the
/// generation retries.
#[tokio::test]
async fn a_deterministic_failure_outside_planning_holds_nothing_back() {
    let db = open(
        "isolate-outside-planning",
        Arc::new(InMemory::new()),
        queued(tuning()),
    )
    .await;
    install_vector(&db, None).await;
    add_doc(&db, vec![0.0, 0.0], "doc").await.unwrap();
    let target = target(&db, QueueFamily::Vector).await;
    let storage = db.inner_db();
    let stored = storage.get(target.key()).await.unwrap().unwrap();
    storage
        .put(target.key(), b"not an operation queue")
        .await
        .unwrap();
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert!(held(&db).is_empty());
    assert_eq!(
        publisher(&db)
            .metrics()
            .error_retries
            .load(Ordering::Relaxed),
        1
    );
    storage.put(target.key(), &stored).await.unwrap();
    assert_eq!(
        publisher(&db).publish_once(target).await.unwrap(),
        PublicationOutcome::Published {
            operations: 1,
            entities: 1
        }
    );
    db.close().await.unwrap();
}
