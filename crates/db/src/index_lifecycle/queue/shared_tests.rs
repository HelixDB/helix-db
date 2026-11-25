//! Shared views select exactly what storage resolves at each snapshot.
//!
//! The reference for a snapshot is the real merge operator over every
//! operand committed at or below it, decoded by [`LatestOperations::decode`]:
//! what a search reading storage at that snapshot selects.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use bytes::Bytes;
use proptest::prelude::*;

use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values::indexes::operation_queue::{
    merge_with_base, QueueMergeResult, QueueOperand, QueuedPayload, QueuedVectorPayload,
    QueuedVectorReplacement,
};
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};

fn target() -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    )
}

fn other() -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(2).unwrap(),
    )
}

fn entity(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    }
}

/// A vector operation on `of` whose retained bytes grow with `dims`; zero
/// dims is a delete.
fn operation(raw: u128, of: u64, dims: usize) -> QueuedOperation {
    QueuedOperation::new(
        QueuedOperationId::try_from_u128(raw).unwrap(),
        entity(of),
        QueuedPayload::Vector(QueuedVectorPayload {
            previous: None,
            replacement: (dims > 0).then(|| {
                QueuedVectorReplacement::try_new(
                    TextPartition::Unpartitioned,
                    Arc::from(vec![raw as f32; dims]),
                )
                .unwrap()
            }),
        }),
    )
}

fn enqueue(target: QueueTarget, operations: &[QueuedOperation]) -> QueueChanges {
    let mut changes = QueueChanges::default();
    changes.enqueue(target, operations);
    changes
}

fn acknowledge(target: QueueTarget, ids: &[QueuedOperationId]) -> QueueChanges {
    let mut changes = QueueChanges::default();
    changes.acknowledge(target, ids);
    changes
}

/// The view a build read at `base` over `operations`, the queue there.
fn prepared(base: u64, operations: &[QueuedOperation]) -> PreparedView {
    PreparedView::new(
        base,
        (!operations.is_empty()).then(|| (QueueFamily::Vector, operations.to_vec())),
    )
}

/// Plans a search at `snapshot` that must not wait.
fn plan_now(shared: &Arc<SharedQueues>, target: QueueTarget, snapshot: u64) -> Plan {
    futures::FutureExt::now_or_never(shared.plan(target, snapshot))
        .expect("nothing the search waits for is in flight")
}

/// Builds `target`'s view at `base` over `operations`, which must be the
/// queue at `base`.
fn build(
    shared: &Arc<SharedQueues>,
    target: QueueTarget,
    base: u64,
    operations: &[QueuedOperation],
) {
    let Plan::Build(builder) = plan_now(shared, target, base) else {
        panic!("an absent view is built");
    };
    shared.base_chosen(&builder, base);
    shared.install(builder, prepared(base, operations));
}

fn shared_read(shared: &Arc<SharedQueues>, target: QueueTarget, snapshot: u64) -> SharedRead {
    match plan_now(shared, target, snapshot) {
        Plan::Shared(read) => read,
        other @ (Plan::Fallback | Plan::Build(_)) => {
            panic!("snapshot {snapshot} is served by the view, got {other:?}")
        }
    }
}

fn ids(latest: &Option<LatestOperations>) -> Vec<u128> {
    latest
        .clone()
        .map(|latest| {
            latest
                .into_operations()
                .iter()
                .map(|operation| operation.id().get())
                .collect()
        })
        .unwrap_or_default()
}

/// What a storage read at a snapshot selects, given every operand at or
/// below it in sequence order.
fn reference(operands: &[Bytes], budget: u64) -> Option<LatestOperations> {
    if operands.is_empty() {
        return None;
    }
    match merge_with_base(None, operands).unwrap() {
        QueueMergeResult::Value(value) => Some(LatestOperations::decode(&value, budget).unwrap()),
        QueueMergeResult::Empty => None,
    }
}

#[test]
fn selects_each_entity_at_its_latest_operation_visible_at_the_snapshot() {
    let shared = Arc::new(SharedQueues::default());
    // A request pinned before every commit may read at any snapshot.
    let _reader = shared.pin();
    let (a1, b1, a2) = (operation(1, 1, 4), operation(2, 2, 4), operation(3, 1, 4));
    build(&shared, target(), 10, &[a1.clone(), b1.clone()]);
    shared
        .begin_commit(enqueue(target(), std::slice::from_ref(&a2)))
        .applied(12);
    shared
        .begin_commit(acknowledge(target(), &[a1.id()]))
        .applied(14);
    let at = |snapshot| ids(&shared_read(&shared, target(), snapshot).select(u64::MAX));
    assert_eq!(at(10), vec![1, 2]);
    assert_eq!(at(11), vec![1, 2]);
    // a's latest replaces its first state in a's place, ahead of b.
    assert_eq!(at(12), vec![3, 2]);
    assert_eq!(at(13), vec![3, 2]);
    // Once a1 is acknowledged, a's oldest visible operation follows b's.
    assert_eq!(at(14), vec![2, 3]);
    // Below the base the view cannot know what was acknowledged since.
    assert!(matches!(plan_now(&shared, target(), 9), Plan::Fallback));
    // The view is per target.
    assert!(matches!(plan_now(&shared, other(), 14), Plan::Build(_)));
}

#[test]
fn budget_ends_the_selection_at_the_first_entity_that_does_not_fit() {
    let shared = Arc::new(SharedQueues::default());
    let operations = [operation(1, 1, 4), operation(2, 2, 8), operation(3, 3, 4)];
    build(&shared, target(), 1, &operations);
    let read = shared_read(&shared, target(), 1);
    let first = operations[0].retained_bytes();
    let second = operations[1].retained_bytes();
    let selected = read.select(first + second - 1).unwrap();
    assert_eq!(selected.refused(), Some(first + second));
    assert_eq!(ids(&Some(selected)), vec![1]);
    let selected = read.select(first - 1).unwrap();
    assert_eq!(selected.refused(), Some(first));
    assert!(selected.into_operations().is_empty());
    let all = read.select(u64::MAX).unwrap();
    assert_eq!(all.refused(), None);
    assert_eq!(all.family(), QueueFamily::Vector);
    // An empty view selects nothing.
    build(&shared, other(), 1, &[]);
    assert!(shared_read(&shared, other(), 1).select(u64::MAX).is_none());
}

#[test]
fn selections_share_payloads_and_the_view_is_charged_once() {
    let shared = Arc::new(SharedQueues::default());
    let operations = [operation(1, 1, 64), operation(2, 2, 64)];
    build(&shared, target(), 1, &operations);
    let charged = operations
        .iter()
        .map(|operation| charged_bytes(operation.retained_bytes()))
        .sum::<u64>();
    let selections = (0..8)
        .map(|_| shared_read(&shared, target(), 1).select(u64::MAX).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(shared.stats().0, charged, "however many searches read it");
    let payload = |operation: &QueuedOperation| match operation.payload() {
        QueuedPayload::Vector(vector) => vector.replacement.as_ref().unwrap().shared_vector(),
        QueuedPayload::Text(_) => unreachable!("vector operations"),
    };
    for selection in selections {
        for (selected, original) in selection.into_operations().iter().zip(&operations) {
            assert!(Arc::ptr_eq(&payload(selected), &payload(original)));
        }
    }
    // Acknowledged operations stay charged while snapshots may see them, and
    // are released once compaction drops them.
    shared
        .begin_commit(acknowledge(target(), &[operations[0].id()]))
        .applied(2);
    assert_eq!(
        shared.stats().0,
        charged_bytes(operations[1].retained_bytes())
    );
}

#[tokio::test]
async fn a_search_waits_for_queue_commits_in_flight_when_it_looked() {
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[]);
    let committing = shared.begin_commit(enqueue(target(), &[operation(1, 1, 4)]));
    // Another target's commit is not awaited.
    let unrelated = shared.begin_commit(enqueue(other(), &[operation(9, 9, 4)]));
    let search = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move {
            let Plan::Shared(read) = shared.plan(target(), 2).await else {
                panic!("the view serves the search");
            };
            ids(&read.select(u64::MAX))
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!search.is_finished(), "its snapshot may hold the commit");
    // A commit registered after the search looked applies above its snapshot.
    let later = shared.begin_commit(enqueue(target(), &[operation(2, 2, 4)]));
    committing.applied(2);
    assert_eq!(search.await.unwrap(), vec![1]);
    later.applied(3);
    unrelated.aborted();
    assert_eq!(
        ids(&shared_read(&shared, target(), 3).select(u64::MAX)),
        vec![1, 2]
    );
}

#[tokio::test]
async fn an_aborted_commit_releases_waiting_searches_and_changes_nothing() {
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[operation(1, 1, 4)]);
    let committing = shared.begin_commit(enqueue(target(), &[operation(2, 2, 4)]));
    let search = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move {
            let Plan::Shared(read) = shared.plan(target(), 5).await else {
                panic!("the view serves the search");
            };
            ids(&read.select(u64::MAX))
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!search.is_finished());
    committing.aborted();
    assert_eq!(search.await.unwrap(), vec![1]);
}

#[test]
fn an_uncertain_commit_unsettles_the_view_until_the_publisher_settles_it() {
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[operation(1, 1, 4)]);
    let before = shared.settlement();
    shared
        .begin_commit(enqueue(target(), &[operation(2, 2, 4)]))
        .uncertain();
    assert!(matches!(plan_now(&shared, target(), 5), Plan::Fallback));
    assert_eq!(shared.stats().0, 0, "an unsettled view holds nothing");
    // A settlement read before the uncertainty never ends it.
    shared.settle(target(), before);
    assert!(matches!(plan_now(&shared, target(), 5), Plan::Fallback));
    let after = shared.settlement();
    // Another uncertainty after the settlement was read keeps it unsettled.
    shared
        .begin_commit(acknowledge(target(), &[operation(1, 1, 4).id()]))
        .uncertain();
    shared.settle(target(), after);
    assert!(matches!(plan_now(&shared, target(), 5), Plan::Fallback));
    shared.settle(target(), shared.settlement());
    // Settled, the next search rebuilds from a fresh snapshot: the
    // uncertain commit's sequence is unknown.
    let Plan::Build(builder) = plan_now(&shared, target(), 5) else {
        panic!("a settled view is rebuilt");
    };
    assert!(!builder.own_snapshot());
}

#[test]
fn dropping_an_unresolved_commit_unsettles_unless_it_changes_no_queue() {
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[]);
    drop(shared.begin_commit(QueueChanges::default()));
    assert!(matches!(plan_now(&shared, target(), 1), Plan::Shared(_)));
    drop(shared.begin_commit(enqueue(target(), &[operation(1, 1, 4)])));
    assert!(matches!(plan_now(&shared, target(), 1), Plan::Fallback));
}

#[test]
fn a_builder_reads_its_own_snapshot_only_when_nothing_was_recorded_above_it() {
    let shared = Arc::new(SharedQueues::default());
    // Nothing recorded since open: any snapshot is a base.
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("absent");
    };
    assert!(builder.own_snapshot());
    drop(builder);
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .applied(7);
    let Plan::Build(builder) = plan_now(&shared, target(), 6) else {
        panic!("a dropped builder leaves the view absent");
    };
    assert!(
        !builder.own_snapshot(),
        "a commit at 7 is not in a base at 6"
    );
    drop(builder);
    let Plan::Build(builder) = plan_now(&shared, target(), 7) else {
        panic!("absent");
    };
    assert!(builder.own_snapshot());
    drop(builder);
    // The target held nothing after its commit, so it was untracked; a
    // target tracked anew assumes the commits of every untracked one.
    assert!(shared.state.lock().targets.is_empty());
    let Plan::Build(unrelated) = plan_now(&shared, other(), 6) else {
        panic!("absent");
    };
    assert!(!unrelated.own_snapshot());
    drop(unrelated);
    let Plan::Build(unrelated) = plan_now(&shared, other(), 7) else {
        panic!("absent");
    };
    assert!(unrelated.own_snapshot());
    // A tracked target's commits do not matter to another target.
    let Plan::Build(builder) = plan_now(&shared, target(), 7) else {
        panic!("absent");
    };
    shared
        .begin_commit(enqueue(target(), &[operation(2, 2, 4)]))
        .applied(9);
    drop(unrelated);
    let Plan::Build(unrelated) = plan_now(&shared, other(), 8) else {
        panic!("absent");
    };
    assert!(unrelated.own_snapshot());
    drop((builder, unrelated));
}

#[tokio::test]
async fn searches_wait_for_a_build_at_or_below_their_snapshot_and_read_storage_below_it() {
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 3) else {
        panic!("absent");
    };
    // Before the base is chosen, every search waits.
    let waiting = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move { shared.plan(target(), 4).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    shared.base_chosen(&builder, 3);
    assert!(matches!(plan_now(&shared, target(), 2), Plan::Fallback));
    // A commit recorded while building is logged and replayed above the
    // base; one at or below it is already in the base.
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .applied(3);
    shared
        .begin_commit(enqueue(target(), &[operation(2, 2, 4)]))
        .applied(4);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    shared.install(builder, prepared(3, &[operation(1, 1, 4)]));
    let Plan::Shared(read) = waiting.await.unwrap() else {
        panic!("the installed view serves the waiting search");
    };
    assert_eq!(ids(&read.select(u64::MAX)), vec![1, 2]);
    assert_eq!(
        ids(&shared_read(&shared, target(), 3).select(u64::MAX)),
        vec![1]
    );
}

#[tokio::test]
async fn a_cancelled_builder_hands_the_build_to_a_waiting_search() {
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 3) else {
        panic!("absent");
    };
    let waiting = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move {
            let Plan::Build(builder) = shared.plan(target(), 3).await else {
                panic!("the waiting search takes over the build");
            };
            shared.base_chosen(&builder, 3);
            shared.install(builder, prepared(3, &[operation(1, 1, 4)]));
            let Plan::Shared(read) = shared.plan(target(), 3).await else {
                panic!("installed");
            };
            ids(&read.select(u64::MAX))
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    drop(builder);
    assert_eq!(waiting.await.unwrap(), vec![1]);
}

#[test]
fn a_rebuild_never_takes_a_base_below_a_commit_its_previous_view_logged() {
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 2) else {
        panic!("absent");
    };
    shared.base_chosen(&builder, 2);
    // Logged while building, above the base.
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .applied(5);
    shared.install(builder, prepared(2, &[]));
    shared.free(target());
    // A base at 4 would miss the commit at 5, which no log holds any more.
    let Plan::Build(builder) = plan_now(&shared, target(), 4) else {
        panic!("a forgotten view is rebuilt");
    };
    assert!(!builder.own_snapshot());
}

#[test]
fn a_build_whose_base_does_not_decode_unsettles_the_target() {
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("absent");
    };
    shared.abandon(builder);
    assert!(matches!(plan_now(&shared, target(), 1), Plan::Fallback));
    // An abandoned build overtaken by a reset leaves the reset alone.
    shared.settle(target(), shared.settlement());
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("settled");
    };
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .uncertain();
    let before = shared.settlement();
    shared.abandon(builder);
    assert_eq!(shared.settlement(), before);
}

#[test]
fn a_build_overtaken_by_an_uncertain_commit_installs_nothing() {
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 3) else {
        panic!("absent");
    };
    shared.base_chosen(&builder, 3);
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .uncertain();
    shared.install(builder, prepared(3, &[]));
    assert!(matches!(plan_now(&shared, target(), 3), Plan::Fallback));
}

#[test]
fn a_change_the_view_contradicts_unsettles_it() {
    for change in [
        // An acknowledgement of an operation the view never held.
        acknowledge(target(), &[operation(9, 9, 4).id()]),
        // An operation enqueued twice.
        enqueue(target(), &[operation(1, 1, 4)]),
        // Another family.
        {
            let mut changes = QueueChanges::default();
            changes.enqueue(
                target(),
                &[QueuedOperation::new(
                    QueuedOperationId::try_from_u128(5).unwrap(),
                    entity(5),
                    QueuedPayload::Text(
                        crate::encoding::v2::values::indexes::operation_queue::QueuedTextPayload {
                            replacement: None,
                        },
                    ),
                )],
            );
            changes
        },
    ] {
        let shared = Arc::new(SharedQueues::default());
        build(&shared, target(), 1, &[operation(1, 1, 4)]);
        shared.begin_commit(change).applied(2);
        assert!(matches!(plan_now(&shared, target(), 2), Plan::Fallback));
    }
    // An acknowledgement applied twice contradicts it too.
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[operation(1, 1, 4)]);
    let id = operation(1, 1, 4).id();
    let _pin = shared.pin();
    shared.begin_commit(acknowledge(target(), &[id])).applied(2);
    shared.begin_commit(acknowledge(target(), &[id])).applied(3);
    assert!(matches!(plan_now(&shared, target(), 3), Plan::Fallback));
    // So does a logged change that contradicts the base.
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("absent");
    };
    shared.base_chosen(&builder, 1);
    shared
        .begin_commit(acknowledge(target(), &[operation(9, 9, 4).id()]))
        .applied(2);
    shared.install(builder, prepared(1, &[]));
    assert!(matches!(plan_now(&shared, target(), 2), Plan::Fallback));
}

#[tokio::test]
async fn an_acknowledgement_recorded_before_its_enqueue_waits_for_it() {
    let shared = Arc::new(SharedQueues::default());
    let _reader = shared.pin();
    build(&shared, target(), 1, &[]);
    let (a1, b1) = (operation(1, 1, 4), operation(2, 2, 4));
    // The enqueue applies at 2; the publisher reads it, acknowledges a1 at 3,
    // and records that before the producer records its commit.
    let enqueued = shared.begin_commit(enqueue(target(), &[a1.clone(), b1.clone()]));
    shared
        .begin_commit(acknowledge(target(), &[a1.id()]))
        .applied(3);
    let search = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move {
            let Plan::Shared(read) = shared.plan(target(), 3).await else {
                panic!("the view serves the search");
            };
            ids(&read.select(u64::MAX))
        }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!search.is_finished(), "the enqueue is still in flight");
    enqueued.applied(2);
    assert_eq!(search.await.unwrap(), vec![2]);
    assert_eq!(
        ids(&shared_read(&shared, target(), 2).select(u64::MAX)),
        vec![1, 2]
    );
    // An early acknowledgement whose enqueue never comes is inconsistent once
    // nothing is in flight.
    shared
        .begin_commit(acknowledge(target(), &[operation(9, 9, 4).id()]))
        .applied(4);
    assert!(matches!(plan_now(&shared, target(), 4), Plan::Fallback));
    // So is one at or below its enqueue's sequence.
    for acknowledged in [3, 4] {
        let shared = Arc::new(SharedQueues::default());
        build(&shared, target(), 1, &[]);
        let enqueued = shared.begin_commit(enqueue(target(), std::slice::from_ref(&a1)));
        shared
            .begin_commit(acknowledge(target(), &[a1.id()]))
            .applied(acknowledged);
        enqueued.applied(4);
        assert!(
            matches!(plan_now(&shared, target(), 4), Plan::Fallback),
            "acknowledged at {acknowledged}"
        );
    }
}

#[test]
fn compaction_keeps_what_pinned_snapshots_and_commits_in_flight_can_see() {
    let shared = Arc::new(SharedQueues::default());
    let operations = (1..=8)
        .map(|raw| operation(raw, raw as u64, 4))
        .collect::<Vec<_>>();
    build(&shared, target(), 1, &operations);
    shared
        .begin_commit(enqueue(other(), &[operation(100, 100, 4)]))
        .applied(2);
    // A snapshot pinned at 2 can still see what is acknowledged above it.
    let pin = shared.pin();
    let acknowledge_at = |sequence: u64, operation: &QueuedOperation| {
        shared
            .begin_commit(acknowledge(target(), &[operation.id()]))
            .applied(sequence);
    };
    for (sequence, operation) in (3..).zip(&operations[..3]) {
        acknowledge_at(sequence, operation);
    }
    assert_eq!(
        ids(&shared_read(&shared, target(), 2).select(u64::MAX)),
        (1..=8).collect::<Vec<_>>()
    );
    // A commit that begins now, at recorded sequence 5, applies above it,
    // so a snapshot at 5 may still miss it.
    let in_flight = shared.begin_commit(enqueue(target(), &[operation(9, 9, 4)]));
    for (sequence, operation) in (6..).zip(&operations[3..6]) {
        acknowledge_at(sequence, operation);
    }
    let charged = |count: usize| count as u64 * charged_bytes(operations[0].retained_bytes());
    assert_eq!(shared.stats().0, charged(8));
    // Without the pin the floor is the commit in flight's, below the newer
    // pin: only what was acknowledged through 5 is dropped.
    drop(pin);
    let _reader = shared.pin();
    acknowledge_at(9, &operations[6]);
    assert_eq!(
        shared.stats().0,
        charged(5),
        "dropped through the floor at 5"
    );
    let served_from = |shared: &SharedQueues| {
        let state = shared.state.lock();
        let ViewState::Ready(view) = &state.targets[&target()].view else {
            panic!("ready");
        };
        view.served_from
    };
    assert_eq!(served_from(&shared), 5);
    // Once the commit resolved, the floor is the newer pin's, at 8.
    in_flight.applied(10);
    assert_eq!(shared.stats().0, charged(3));
    assert_eq!(served_from(&shared), 8);
    assert!(matches!(plan_now(&shared, target(), 7), Plan::Fallback));
    assert_eq!(
        ids(&shared_read(&shared, target(), 8).select(u64::MAX)),
        vec![7, 8]
    );
    assert_eq!(
        ids(&shared_read(&shared, target(), 10).select(u64::MAX)),
        vec![8, 9]
    );
}

#[test]
fn freeing_a_view_rebuilds_it_on_the_next_search() {
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[]);
    shared.free(target());
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("a freed view is rebuilt");
    };
    // Freeing an untracked or building target changes nothing.
    shared.free(other());
    shared.free(target());
    assert!(futures::FutureExt::now_or_never(shared.plan(target(), 1)).is_none());
    drop(builder);
}

#[tokio::test]
async fn a_retired_generation_is_never_built_again_and_a_racing_build_installs_nothing() {
    let shared = Arc::new(SharedQueues::default());
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("absent");
    };
    shared.base_chosen(&builder, 2);
    // A search above the base waits for the build.
    let waiting = tokio::spawn({
        let shared = Arc::clone(&shared);
        async move { shared.plan(target(), 3).await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    shared.retire(target());
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(10), waiting)
                .await
                .expect("woken")
                .unwrap(),
            Plan::Fallback
        ),
        "a search waiting for the build reads storage"
    );
    // The build finishing after the retirement installs nothing.
    assert!(shared
        .install(builder, prepared(2, &[operation(1, 1, 4)]))
        .is_none());
    assert!(!ready(&shared, target()));
    assert_eq!(shared.stats().0, 0);
    // Later searches read storage instead of building, and retiring again,
    // or a commit on the generation, changes nothing.
    shared.retire(target());
    shared
        .begin_commit(enqueue(target(), &[operation(2, 2, 4)]))
        .applied(4);
    assert!(matches!(plan_now(&shared, target(), 5), Plan::Fallback));
    assert_eq!(shared.state.lock().held, 0);
    // Other generations are untouched.
    build(&shared, other(), 5, &[]);
    assert!(ready(&shared, other()));
}

/// Whether `target` is tracked at all.
fn tracked(shared: &SharedQueues, target: QueueTarget) -> bool {
    shared.state.lock().targets.contains_key(&target)
}

/// Whether `target`'s view is ready.
fn ready(shared: &SharedQueues, target: QueueTarget) -> bool {
    shared
        .state
        .lock()
        .targets
        .get(&target)
        .is_some_and(|tracked| matches!(tracked.view, ViewState::Ready(_)))
}

#[test]
fn compaction_drops_a_few_large_acknowledged_operations_by_their_bytes() {
    let shared = Arc::new(SharedQueues::default());
    // Ten small live operations and one large one.
    let small = (1..=10)
        .map(|raw| operation(raw, raw as u64, 1))
        .collect::<Vec<_>>();
    let large = operation(11, 11, 4_096);
    build(
        &shared,
        target(),
        1,
        &[small.clone(), vec![large.clone()]].concat(),
    );
    // One acknowledged slot is below half the ten live ones by count, but
    // its bytes are well above half theirs.
    shared
        .begin_commit(acknowledge(target(), &[large.id()]))
        .applied(2);
    let live = small
        .iter()
        .map(|operation| charged_bytes(operation.retained_bytes()))
        .sum::<u64>();
    assert_eq!(shared.stats().0, live);
    // A small acknowledgement reaching neither half waits.
    shared
        .begin_commit(acknowledge(target(), &[small[0].id()]))
        .applied(3);
    assert_eq!(shared.stats().0, live);
    // By count, half the live slots are enough even when their bytes are
    // not: deletes against large live operations.
    let deletes = (21..=26)
        .map(|raw| operation(raw, raw as u64, 0))
        .collect::<Vec<_>>();
    let large_live = (31..=36)
        .map(|raw| operation(raw, raw as u64, 1_024))
        .collect::<Vec<_>>();
    let by_count = Arc::new(SharedQueues::default());
    build(
        &by_count,
        target(),
        1,
        &[deletes.clone(), large_live.clone()].concat(),
    );
    let four = deletes[..4]
        .iter()
        .map(QueuedOperation::id)
        .collect::<Vec<_>>();
    by_count
        .begin_commit(acknowledge(target(), &four))
        .applied(2);
    let remaining = [&deletes[4..], &large_live[..]]
        .concat()
        .iter()
        .map(|operation| charged_bytes(operation.retained_bytes()))
        .sum::<u64>();
    assert_eq!(by_count.stats().0, remaining, "four of eight live by count");
    // Half the live slots by count.
    let half = small[1..5]
        .iter()
        .map(QueuedOperation::id)
        .collect::<Vec<_>>();
    shared.begin_commit(acknowledge(target(), &half)).applied(4);
    let kept = small[5..]
        .iter()
        .map(|operation| charged_bytes(operation.retained_bytes()))
        .sum::<u64>();
    assert_eq!(shared.stats().0, kept);
}

#[test]
fn a_rising_floor_compacts_a_queue_that_receives_no_more_commits() {
    let shared = Arc::new(SharedQueues::default());
    let operations = (1..=4)
        .map(|raw| operation(raw, raw as u64, 4))
        .collect::<Vec<_>>();
    build(&shared, target(), 1, &operations);
    let older = shared.pin();
    shared
        .begin_commit(enqueue(other(), &[operation(9, 9, 4)]))
        .applied(1);
    let newer = shared.pin();
    let ids_of = |operations: &[QueuedOperation]| {
        operations
            .iter()
            .map(QueuedOperation::id)
            .collect::<Vec<_>>()
    };
    shared
        .begin_commit(acknowledge(target(), &ids_of(&operations[..2])))
        .applied(2);
    let charged = |count: usize| count as u64 * charged_bytes(operations[0].retained_bytes());
    assert_eq!(shared.stats().0, charged(4), "both pins can see them");
    // A pin that is not the oldest raises nothing.
    drop(newer);
    assert_eq!(shared.stats().0, charged(4));
    assert!(shared.state.lock().blocked.contains(&target()));
    drop(older);
    assert_eq!(shared.stats().0, charged(2), "no commit followed the pin");
    assert!(shared.state.lock().blocked.is_empty());
    // A queue that drains under a pin is freed once the pin closes.
    let pin = shared.pin();
    shared
        .begin_commit(acknowledge(target(), &ids_of(&operations[2..])))
        .applied(3);
    assert_eq!(shared.stats().0, charged(2));
    assert!(ready(&shared, target()));
    drop(pin);
    assert_eq!(shared.stats().0, 0);
    assert!(!tracked(&shared, target()), "a drained view is freed");
    let Plan::Build(builder) = plan_now(&shared, target(), 3) else {
        panic!("a freed view is rebuilt");
    };
    assert!(builder.own_snapshot());
}

#[test]
fn a_drained_view_waits_for_in_flight_commits() {
    let shared = Arc::new(SharedQueues::default());
    let a1 = operation(1, 1, 4);
    build(&shared, target(), 1, std::slice::from_ref(&a1));
    // An enqueue in flight keeps the floor below its own sequence.
    let enqueued = shared.begin_commit(enqueue(target(), &[operation(2, 2, 4)]));
    shared
        .begin_commit(acknowledge(target(), &[operation(2, 2, 4).id()]))
        .applied(3);
    shared
        .begin_commit(acknowledge(target(), &[a1.id()]))
        .applied(4);
    assert!(
        ready(&shared, target()),
        "the floor is below a1's acknowledgement"
    );
    enqueued.applied(2);
    assert!(!ready(&shared, target()), "everything is acknowledged");
    assert_eq!(shared.stats().0, 0);
}

#[test]
fn a_drained_view_with_an_early_acknowledgement_left_is_freed_and_rebuilt_correctly() {
    let shared = Arc::new(SharedQueues::default());
    let a1 = operation(1, 1, 4);
    build(&shared, target(), 1, std::slice::from_ref(&a1));
    shared
        .begin_commit(acknowledge(target(), &[a1.id()]))
        .applied(2);
    assert!(!ready(&shared, target()));
    build(&shared, target(), 2, &[]);
    // An enqueue that begins at 2 and whose operation is acknowledged at 4
    // before the enqueue, at 3, is recorded.
    let b1 = operation(2, 2, 4);
    let enqueued = shared.begin_commit(enqueue(target(), std::slice::from_ref(&b1)));
    shared
        .begin_commit(acknowledge(target(), &[b1.id()]))
        .applied(4);
    enqueued.applied(3);
    // Both drained and nothing pins them, so the view is freed.
    assert!(!ready(&shared, target()));
    assert_eq!(shared.stats().0, 0);
}

#[test]
fn an_aborted_commit_raises_the_floor_it_held() {
    let shared = Arc::new(SharedQueues::default());
    let operations = (1..=4)
        .map(|raw| operation(raw, raw as u64, 4))
        .collect::<Vec<_>>();
    build(&shared, target(), 1, &operations);
    let held = shared.begin_commit(enqueue(target(), &[operation(9, 9, 4)]));
    shared
        .begin_commit(acknowledge(
            target(),
            &[operations[0].id(), operations[1].id()],
        ))
        .applied(2);
    let charged = |count: usize| count as u64 * charged_bytes(operations[0].retained_bytes());
    assert_eq!(
        shared.stats().0,
        charged(4),
        "the commit in flight holds the floor"
    );
    held.aborted();
    assert_eq!(shared.stats().0, charged(2));
}

#[test]
fn a_build_compacts_what_its_log_acknowledged_below_the_floor() {
    let shared = Arc::new(SharedQueues::default());
    let operations = (1..=4)
        .map(|raw| operation(raw, raw as u64, 4))
        .collect::<Vec<_>>();
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("absent");
    };
    shared.base_chosen(&builder, 1);
    shared
        .begin_commit(acknowledge(
            target(),
            &[operations[0].id(), operations[1].id()],
        ))
        .applied(2);
    shared.install(builder, prepared(1, &operations));
    assert_eq!(
        shared.stats().0,
        2 * charged_bytes(operations[0].retained_bytes())
    );
}

#[test]
fn an_empty_view_is_kept_for_the_searches_of_an_empty_queue() {
    let shared = Arc::new(SharedQueues::default());
    build(&shared, target(), 1, &[]);
    shared
        .begin_commit(enqueue(other(), &[operation(1, 1, 4)]))
        .applied(2);
    assert!(matches!(plan_now(&shared, target(), 2), Plan::Shared(_)));
}

#[test]
fn views_beyond_the_budget_evict_the_least_recently_searched_but_never_the_one_in_use() {
    let third = QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(2).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    );
    let one = operation(1, 1, 64);
    let weight = charged_bytes(one.retained_bytes()) + VIEW_OVERHEAD_BYTES;
    // Two views fit, not three.
    let shared = Arc::new(SharedQueues::new(2 * weight));
    build(&shared, target(), 1, std::slice::from_ref(&one));
    build(&shared, other(), 1, &[operation(2, 2, 64)]);
    // The first view is searched after the second was built.
    shared_read(&shared, target(), 1);
    build(&shared, third, 1, &[operation(3, 3, 64)]);
    assert!(ready(&shared, target()));
    assert!(
        !tracked(&shared, other()),
        "the least recently searched view"
    );
    assert!(ready(&shared, third), "the view just built");
    assert_eq!(shared.state.lock().held, 2 * weight);
    // A commit that grows a view evicts the others, but never that view.
    shared
        .begin_commit(enqueue(third, &[operation(4, 4, 4_096)]))
        .applied(2);
    assert!(!tracked(&shared, target()));
    assert!(ready(&shared, third));
    assert!(shared.state.lock().held > 2 * weight);
    // Empty views are charged their bookkeeping, so they are bounded too.
    let shared = Arc::new(SharedQueues::new(VIEW_OVERHEAD_BYTES));
    build(&shared, target(), 1, &[]);
    build(&shared, other(), 1, &[]);
    assert!(!tracked(&shared, target()));
    assert!(ready(&shared, other()));
}

#[test]
fn targets_holding_nothing_are_untracked_without_losing_what_bounds_a_base() {
    let shared = Arc::new(SharedQueues::default());
    // Commits on targets nobody searches leave nothing behind.
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .applied(5);
    shared
        .begin_commit(enqueue(other(), &[operation(2, 2, 4)]))
        .aborted();
    assert!(shared.state.lock().targets.is_empty());
    // A later build below a commit recorded before untracking takes a fresh
    // snapshot.
    let Plan::Build(builder) = plan_now(&shared, target(), 4) else {
        panic!("absent");
    };
    assert!(
        !builder.own_snapshot(),
        "a commit at 5 is not in a base at 4"
    );
    drop(builder);
    assert!(
        !tracked(&shared, target()),
        "an abandoned build leaves nothing"
    );
    // An unknown commit sequence is kept until a build replaces it.
    shared
        .begin_commit(enqueue(target(), &[operation(3, 3, 4)]))
        .uncertain();
    shared.settle(target(), shared.settlement());
    assert!(tracked(&shared, target()));
    let Plan::Build(builder) = plan_now(&shared, target(), 100) else {
        panic!("settled");
    };
    assert!(!builder.own_snapshot());
    shared.base_chosen(&builder, 100);
    shared.install(builder, prepared(100, &[]));
    shared.free(target());
    assert!(shared.state.lock().targets.is_empty());
}

#[test]
fn a_settlement_read_before_a_target_was_untracked_never_settles_it() {
    let shared = Arc::new(SharedQueues::default());
    shared
        .begin_commit(enqueue(target(), &[operation(1, 1, 4)]))
        .uncertain();
    shared.settle(target(), shared.settlement());
    let before = shared.settlement();
    build(&shared, target(), 10, &[]);
    shared.free(target());
    assert!(!tracked(&shared, target()));
    // Tracked anew and unsettled after the settlement was read.
    shared
        .begin_commit(enqueue(target(), &[operation(2, 2, 4)]))
        .uncertain();
    shared.settle(target(), before);
    assert!(matches!(plan_now(&shared, target(), 20), Plan::Fallback));
}

#[test]
fn a_retired_generation_view_is_freed() {
    let shared = Arc::new(SharedQueues::default());
    let _reader = shared.pin();
    let (a1, b1) = (operation(1, 1, 4), operation(2, 2, 4));
    build(&shared, target(), 1, &[a1.clone(), b1]);
    // Compaction waits on the pin for the acknowledgement.
    shared
        .begin_commit(acknowledge(target(), &[a1.id()]))
        .applied(2);
    assert!(shared.state.lock().blocked.contains(&target()));
    assert!(shared.stats().0 > 0);
    shared.retire(target());
    assert_eq!(shared.stats().0, 0);
    let state = shared.state.lock();
    assert_eq!(state.held, 0);
    assert!(state.blocked.is_empty(), "a freed view waits on nothing");
}

/// A store over an in-memory writer whose queue of `target()` holds
/// `operations`, committed and recorded.
async fn store_holding(
    name: &str,
    operations: &[QueuedOperation],
) -> (Arc<slatedb::Db>, Arc<super::super::storage::QueueStore>) {
    let db = slatedb::Db::builder(
        name,
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
    .build()
    .await
    .unwrap();
    let store = super::super::storage::QueueStore::new(
        crate::config::QueueLayout::Map,
        1 << 20,
        u64::MAX,
        super::super::resolutions::ResolutionBudget::default(),
    );
    enqueue_recorded(&db, &store, operations).await;
    (Arc::new(db), Arc::new(store))
}

/// Commits `operations` to `target()`'s queue, recorded, and returns where.
async fn enqueue_recorded(
    db: &slatedb::Db,
    store: &super::super::storage::QueueStore,
    operations: &[QueuedOperation],
) -> u64 {
    let transaction = db
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut changes = QueueChanges::default();
    store
        .stage_enqueue(
            &transaction,
            &mut changes,
            target(),
            QueueOperand::enqueue(operations).unwrap(),
            operations,
        )
        .unwrap();
    commit_recorded(transaction, store.shared().begin_commit(changes))
        .await
        .unwrap()
        .unwrap()
        .seqnum()
}

/// Starts a search of `target()` at a fresh snapshot of `db` that builds
/// its view, and waits until the build decodes, gated there.
async fn gated_build(
    db: &Arc<slatedb::Db>,
    store: &Arc<super::super::storage::QueueStore>,
) -> (
    tokio::task::JoinHandle<super::super::storage::PendingRead>,
    std::sync::mpsc::Sender<()>,
) {
    let (reached, waits) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    store.shared().gate_next_decode(reached, released);
    let snapshot = db.snapshot().await.unwrap();
    let search = tokio::spawn({
        let (db, store) = (Arc::clone(db), Arc::clone(store));
        async move {
            store
                .read_pending(&db, snapshot.as_ref(), snapshot.seq(), target())
                .await
                .unwrap()
        }
    });
    tokio::task::spawn_blocking(move || waits.recv().unwrap())
        .await
        .unwrap();
    (search, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_is_charged_for_its_base_read_until_it_installs_and_serves_its_search() {
    let (db, store) = store_holding(
        "shared-build-resolution",
        &[operation(1, 1, 256), operation(2, 2, 256)],
    )
    .await;
    let resolved = db.get(target().key()).await.unwrap().unwrap().len() as u64;
    let (search, release) = gated_build(&db, &store).await;
    // While it decodes, the build holds the charge of what it read: twice
    // its bytes, in whole kibibytes.
    assert_eq!(
        store.resolutions().charged(),
        (u32::try_from((2 * resolved).div_ceil(1 << 10)).unwrap(), 0)
    );
    release.send(()).unwrap();
    let super::super::storage::PendingRead::Shared(read) = search.await.unwrap() else {
        panic!("the build serves its own search");
    };
    assert_eq!(ids(&read.select(u64::MAX)), vec![1, 2]);
    assert_eq!(store.resolutions().charged(), (0, 0));
    assert_eq!(store.shared().builds(), 1);
    assert_eq!(store.shared().stats().1, 0, "nothing read storage again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_build_that_installs_nothing_reads_storage_once_instead_of_building_again() {
    let (db, store) = store_holding(
        "shared-build-overtaken",
        &[operation(1, 1, 4), operation(2, 2, 4)],
    )
    .await;
    let (search, release) = gated_build(&db, &store).await;
    // Retired while it decodes: the build installs nothing.
    store.shared().retire(target());
    release.send(()).unwrap();
    let read = search.await.unwrap();
    let super::super::storage::PendingRead::Stored { .. } = &read else {
        panic!("the search reads storage");
    };
    assert_eq!(ids(&read.select(u64::MAX).unwrap()), vec![1, 2]);
    assert_eq!(store.shared().builds(), 1, "it never builds again");
    assert_eq!(store.shared().stats(), (0, 1));
    // The storage read holds twice what it resolved until it is dropped.
    let resolved = db.get(target().key()).await.unwrap().unwrap().len() as u64;
    assert_eq!(
        store.resolutions().charged(),
        (u32::try_from((2 * resolved).div_ceil(1 << 10)).unwrap(), 0)
    );
    drop(read);
    assert_eq!(store.resolutions().charged(), (0, 0));
}

#[tokio::test]
async fn a_first_storage_read_is_charged_the_worst_case_then_what_it_read() {
    let (db, store) = store_holding("shared-stored-charge", &[operation(1, 1, 4)]).await;
    let snapshot = db.snapshot().await.unwrap();
    let read = store
        .read_pending_stored(snapshot.as_ref(), snapshot.seq(), target())
        .await
        .unwrap();
    // Charged a mebibyte (the test budget's worst case) until it read a
    // few hundred bytes: one kibibyte now.
    let resolved = db.get(target().key()).await.unwrap().unwrap().len() as u64;
    assert!(resolved < 512);
    assert_eq!(store.resolutions().charged(), (1, 0));
    assert_eq!(ids(&read.select(u64::MAX).unwrap()), vec![1]);
    assert_eq!(store.shared().stats(), (0, 1));
}

#[tokio::test]
async fn a_build_from_a_fresh_snapshot_is_charged_as_read_there() {
    let (db, store) = store_holding("shared-build-fresh", &[operation(1, 1, 4)]).await;
    let early = db.snapshot().await.unwrap();
    let later = enqueue_recorded(&db, &store, &[operation(2, 2, 4)]).await;
    // The build reads a fresh snapshot, which cannot serve the search's own:
    // the search reads storage at its snapshot.
    let read = store
        .read_pending(&db, early.as_ref(), early.seq(), target())
        .await
        .unwrap();
    assert_eq!(ids(&read.select(u64::MAX).unwrap()), vec![1]);
    // What the build read is known at the snapshot it read, not the
    // search's.
    let newest = store.resolutions().newest(target()).unwrap();
    assert!(newest >= later && newest > early.seq());
}

#[test]
fn a_storage_read_resolves_its_value_or_every_row() {
    use super::super::storage::PendingQueueBytes;
    assert_eq!(
        PendingQueueBytes::Map(Bytes::from(vec![0; 5])).resolved_bytes(),
        5
    );
    assert_eq!(
        PendingQueueBytes::Rows(vec![Bytes::from(vec![0; 3]), Bytes::from(vec![0; 4])])
            .resolved_bytes(),
        7
    );
}

#[test]
fn an_installed_view_serves_its_builder_even_when_evicted_before_it_selects() {
    let one = operation(1, 1, 64);
    let weight = charged_bytes(one.retained_bytes()) + VIEW_OVERHEAD_BYTES;
    // One view fits.
    let shared = Arc::new(SharedQueues::new(weight));
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("absent");
    };
    shared.base_chosen(&builder, 1);
    let own = shared
        .install(builder, prepared(1, std::slice::from_ref(&one)))
        .expect("the view serves its builder's snapshot");
    // Another target's install evicts it before the builder selects.
    build(&shared, other(), 1, &[operation(2, 2, 64)]);
    assert!(!tracked(&shared, target()));
    assert_eq!(ids(&own.select(u64::MAX)), vec![1]);
    // A view whose fresh base is above its builder's snapshot cannot serve
    // it; the builder reads storage.
    let Plan::Build(builder) = plan_now(&shared, target(), 1) else {
        panic!("evicted");
    };
    shared.base_chosen(&builder, 3);
    assert!(shared.install(builder, prepared(3, &[])).is_none());
    assert!(ready(&shared, target()));
}

#[tokio::test]
async fn large_frees_run_on_the_blocking_pool_and_small_ones_inline() {
    let shared = Arc::new(SharedQueues::default());
    // A small view drains and is freed on this thread, right away.
    let small = operation(1, 1, 4);
    build(&shared, target(), 1, std::slice::from_ref(&small));
    shared
        .begin_commit(acknowledge(target(), &[small.id()]))
        .applied(2);
    assert!(!tracked(&shared, target()));
    let here = std::thread::current().id();
    let [(thread, weight)] = shared.ran_after()[..] else {
        panic!("one free ran");
    };
    assert_eq!(thread, here);
    assert!(weight <= INLINE_AFTER_BYTES);
    // A view of more than a mebibyte is freed on the blocking pool.
    let large = (2..=200)
        .map(|raw| operation(raw, raw as u64, 2_048))
        .collect::<Vec<_>>();
    build(&shared, other(), 2, &large);
    assert!(shared.stats().0 > INLINE_AFTER_BYTES);
    shared
        .begin_commit(acknowledge(
            other(),
            &large.iter().map(QueuedOperation::id).collect::<Vec<_>>(),
        ))
        .applied(3);
    assert_eq!(shared.stats().0, 0, "accounted as freed at once");
    tokio::time::timeout(Duration::from_secs(10), async {
        while shared.ran_after().len() < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the free runs");
    let (thread, weight) = shared.ran_after()[1];
    assert_ne!(thread, here, "not on the thread that committed");
    assert!(weight > INLINE_AFTER_BYTES);
    // So is a compaction that drops more than a mebibyte.
    let kept = operation(300, 300, 4);
    let mut operations = large.clone();
    operations.push(kept.clone());
    build(&shared, target(), 4, &operations);
    shared
        .begin_commit(acknowledge(
            target(),
            &large.iter().map(QueuedOperation::id).collect::<Vec<_>>(),
        ))
        .applied(5);
    tokio::time::timeout(Duration::from_secs(10), async {
        while shared.stats().0 != charged_bytes(kept.retained_bytes()) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the compaction finishes");
    let ran = shared.ran_after();
    assert_eq!(ran.len(), 3);
    assert_ne!(ran[2].0, here);
    assert_eq!(
        ids(&shared_read(&shared, target(), 5).select(u64::MAX)),
        vec![300]
    );
}

#[test]
fn a_capture_keeps_its_selection_through_compactions_and_frees() {
    let shared = Arc::new(SharedQueues::default());
    let operations = (1..=6)
        .map(|raw| operation(raw, raw as u64, 4))
        .collect::<Vec<_>>();
    build(&shared, target(), 1, &operations);
    let pin = shared.pin();
    let captured = shared_read(&shared, target(), 1);
    for (sequence, operation) in (2..).zip(&operations) {
        shared
            .begin_commit(acknowledge(target(), &[operation.id()]))
            .applied(sequence);
    }
    drop(pin);
    assert!(!tracked(&shared, target()), "drained and freed");
    assert_eq!(ids(&captured.select(u64::MAX)), (1..=6).collect::<Vec<_>>());
}

#[test]
fn a_compaction_overtaken_by_a_reset_or_rebuild_changes_nothing() {
    let shared = Arc::new(SharedQueues::default());
    let operations = (1..=4)
        .map(|raw| operation(raw, raw as u64, 4))
        .collect::<Vec<_>>();
    build(&shared, target(), 1, &operations);
    let pin = shared.pin();
    shared
        .begin_commit(acknowledge(
            target(),
            &[operations[0].id(), operations[1].id()],
        ))
        .applied(2);
    // Start the compaction the closing pin makes due, without finishing it.
    let mut after = After::default();
    {
        let mut state = shared.state.lock();
        state.pins.clear();
        state.maintain(target(), &mut after);
    }
    std::mem::forget(pin);
    let [compaction] = &after.compactions[..] else {
        panic!("one compaction starts");
    };
    assert!(matches!(
        &shared.state.lock().targets[&target()].view,
        ViewState::Ready(view) if view.compacting
    ));
    // Another compaction of the same view waits for it.
    assert_eq!(
        {
            let state = shared.state.lock();
            let ViewState::Ready(view) = &state.targets[&target()].view else {
                unreachable!()
            };
            view.due(u64::MAX)
        },
        Due::Nothing
    );
    let build_of = compaction.build;
    // The view is rebuilt meanwhile; the stale compaction leaves the new one.
    shared.free(target());
    build(&shared, target(), 3, &operations[2..]);
    assert_ne!(
        {
            let state = shared.state.lock();
            let ViewState::Ready(view) = &state.targets[&target()].view else {
                unreachable!()
            };
            view.build
        },
        build_of
    );
    shared.run(after);
    assert_eq!(
        ids(&shared_read(&shared, target(), 3).select(u64::MAX)),
        vec![3, 4]
    );
    assert_eq!(
        shared.stats().0,
        2 * charged_bytes(operations[0].retained_bytes())
    );
}

#[tokio::test]
async fn real_commits_resolve_their_queue_changes_by_outcome() {
    let db = slatedb::Db::open(
        "shared-commit-outcomes",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let shared = Arc::new(SharedQueues::default());
    let _reader = shared.pin();
    build(&shared, target(), 0, &[]);
    let begin = || async {
        db.begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap()
    };
    // Applied: recorded at its sequence.
    let applied = begin().await;
    applied.put(b"a", b"value").unwrap();
    let handle = commit_recorded(
        applied,
        shared.begin_commit(enqueue(target(), &[operation(1, 1, 4)])),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        ids(&shared_read(&shared, target(), handle.seqnum()).select(u64::MAX)),
        vec![1]
    );
    // A conflict applied nothing.
    let loser = begin().await;
    loser.get(b"b").await.unwrap();
    loser.put(b"b", b"loser").unwrap();
    db.put(b"b", b"winner").await.unwrap();
    let error = commit_recorded(
        loser,
        shared.begin_commit(enqueue(target(), &[operation(2, 2, 4)])),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
    assert!(matches!(
        plan_now(&shared, target(), u64::MAX),
        Plan::Shared(_)
    ));
    // Any other failure may have applied, so the view is unsettled.
    let pending = begin().await;
    pending.put(b"c", b"value").unwrap();
    db.close().await.unwrap();
    let error = commit_recorded(
        pending,
        shared.begin_commit(enqueue(target(), &[operation(3, 3, 4)])),
    )
    .await
    .unwrap_err();
    assert_ne!(error.kind(), slatedb::ErrorKind::Transaction);
    assert!(matches!(
        plan_now(&shared, target(), u64::MAX),
        Plan::Fallback
    ));
}

#[test]
fn every_error_but_a_conflict_leaves_the_outcome_unknown() {
    for (error, unknown) in [
        (slatedb::Error::transaction("conflict".to_string()), false),
        (slatedb::Error::internal("lost".to_string()), true),
        (slatedb::Error::unavailable("lost".to_string()), true),
    ] {
        let shared = Arc::new(SharedQueues::default());
        build(&shared, target(), 1, &[]);
        record_applied(
            shared.begin_commit(enqueue(target(), &[operation(1, 1, 4)])),
            &Err(error),
        );
        assert_eq!(
            matches!(plan_now(&shared, target(), 1), Plan::Fallback),
            unknown
        );
    }
}

/// Storage of the concurrency model: every committed operand in sequence
/// order, so a commit's sequence is its position plus one.
#[derive(Default)]
struct Log {
    operands: Vec<Bytes>,
}

impl Log {
    /// The operations storage resolves at `snapshot`, in storage order.
    fn at(&self, snapshot: u64) -> Option<(QueueFamily, Vec<QueuedOperation>)> {
        let operands = &self.operands[..snapshot as usize];
        if operands.is_empty() {
            return None;
        }
        match merge_with_base(None, operands).unwrap() {
            QueueMergeResult::Value(value) => Some(
                super::super::storage::PendingQueueBytes::Map(value)
                    .decode_all()
                    .unwrap(),
            ),
            QueueMergeResult::Empty => None,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_commits_pins_and_searches_see_exactly_their_snapshots() {
    let shared = Arc::new(SharedQueues::default());
    let log = Arc::new(Mutex::new(Log::default()));
    let next_id = Arc::new(std::sync::atomic::AtomicU64::new(1));
    // Registered before the operand is appended, recorded after it, with a
    // yield in between so recordings complete out of order.
    let committing = |shared: Arc<SharedQueues>, log: Arc<Mutex<Log>>| {
        move |changes: QueueChanges, operand: Bytes| {
            let (shared, log) = (Arc::clone(&shared), Arc::clone(&log));
            async move {
                let queue = shared.begin_commit(changes);
                let sequence = {
                    let mut log = log.lock();
                    log.operands.push(operand);
                    log.operands.len() as u64
                };
                tokio::task::yield_now().await;
                queue.applied(sequence);
            }
        }
    };
    let producers = (0..2_u64)
        .map(|producer| {
            let commit = committing(Arc::clone(&shared), Arc::clone(&log));
            let next_id = Arc::clone(&next_id);
            tokio::spawn(async move {
                for round in 0..150_u64 {
                    let operations = (0..1 + round % 2)
                        .map(|offset| {
                            let raw = next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            operation(u128::from(raw), (producer * 3 + round + offset) % 8, 2)
                        })
                        .collect::<Vec<_>>();
                    let operand = QueueOperand::enqueue(&operations).unwrap().into_parts().0;
                    commit(enqueue(target(), &operations), operand).await;
                }
            })
        })
        .collect::<Vec<_>>();
    let publisher = {
        let commit = committing(Arc::clone(&shared), Arc::clone(&log));
        let log = Arc::clone(&log);
        tokio::spawn(async move {
            for _ in 0..200 {
                let read = {
                    let log = log.lock();
                    log.at(log.operands.len() as u64)
                };
                let Some((_, outstanding)) = read else {
                    tokio::task::yield_now().await;
                    continue;
                };
                let ids = outstanding
                    .iter()
                    .take(3)
                    .map(QueuedOperation::id)
                    .collect::<Vec<_>>();
                let operand = QueueOperand::acknowledge(QueueFamily::Vector, ids.iter().copied())
                    .unwrap()
                    .into_parts()
                    .0;
                commit(acknowledge(target(), &ids), operand).await;
            }
        })
    };
    let searchers = (0..3)
        .map(|_| {
            let (shared, log) = (Arc::clone(&shared), Arc::clone(&log));
            tokio::spawn(async move {
                let mut served = 0;
                for round in 0..150 {
                    let pin = shared.pin();
                    let (snapshot, expected) = {
                        let log = log.lock();
                        let snapshot = log.operands.len() as u64;
                        (snapshot, reference(&log.operands, u64::MAX))
                    };
                    match shared.plan(target(), snapshot).await {
                        Plan::Shared(read) => {
                            served += 1;
                            assert_eq!(
                                ids(&read.select(u64::MAX)),
                                ids(&expected),
                                "snapshot {snapshot}"
                            );
                        }
                        Plan::Fallback => {}
                        Plan::Build(builder) => {
                            let base = if builder.own_snapshot() {
                                snapshot
                            } else {
                                log.lock().operands.len() as u64
                            };
                            shared.base_chosen(&builder, base);
                            let queue = log.lock().at(base);
                            // The view serves its builder exactly when its
                            // base is the builder's snapshot.
                            let own = shared.install(builder, PreparedView::new(base, queue));
                            if let Some(read) = &own {
                                served += 1;
                                assert_eq!(
                                    ids(&read.select(u64::MAX)),
                                    ids(&expected),
                                    "builder at {snapshot}"
                                );
                            }
                            assert!(own.is_none() || base == snapshot);
                        }
                    }
                    if round % 3 == 0 {
                        tokio::task::yield_now().await;
                    }
                    drop(pin);
                }
                served
            })
        })
        .collect::<Vec<_>>();
    for producer in producers {
        producer.await.unwrap();
    }
    publisher.await.unwrap();
    let mut served = 0;
    for searcher in searchers {
        served += searcher.await.unwrap();
    }
    assert!(served > 0, "the view served searches");
    // Quiet, the view matches storage at its end and holds what it should.
    let last = log.lock().operands.len() as u64;
    let state = shared.state.lock();
    let held = state
        .targets
        .values()
        .map(|tracked| tracked.view.weight())
        .sum::<u64>();
    assert_eq!(state.held, held);
    assert!(state.pins.is_empty());
    drop(state);
    if let Plan::Shared(read) = plan_now(&shared, target(), last) {
        let expected = reference(&log.lock().operands, u64::MAX);
        assert_eq!(ids(&read.select(u64::MAX)), ids(&expected));
    }
}

/// One commit of the model: the operands it stages, applied at its index + 1.
#[derive(Debug, Clone)]
enum Step {
    /// Enqueue to these entities, with these payload sizes.
    Enqueue(Vec<(u64, usize)>),
    /// Acknowledge the outstanding operations at these positions.
    Acknowledge(Vec<usize>),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        proptest::collection::vec((0_u64..6, 0_usize..6), 1..4).prop_map(Step::Enqueue),
        proptest::collection::vec(0_usize..16, 1..4).prop_map(Step::Acknowledge),
    ]
}

/// Where the model's build stands.
enum Building {
    /// Started; the base is chosen once the next commit applied.
    Fresh(Builder),
    /// Base chosen at `.1` over the queue `.2`.
    Chosen(Builder, u64, Vec<QueuedOperation>),
}

/// A search started mid-history: its snapshot and its plan, polled as
/// commits are recorded.
struct Search {
    snapshot: u64,
    plan: std::pin::Pin<Box<dyn std::future::Future<Output = Plan> + Send>>,
}

/// Polls `plan` once without waiting.
fn poll_plan(
    plan: &mut std::pin::Pin<Box<dyn std::future::Future<Output = Plan> + Send>>,
) -> Option<Plan> {
    let waker = futures::task::noop_waker();
    match plan
        .as_mut()
        .poll(&mut std::task::Context::from_waker(&waker))
    {
        std::task::Poll::Ready(plan) => Some(plan),
        std::task::Poll::Pending => None,
    }
}

/// Checks `read`'s selections at its snapshot against storage's there.
fn check_selection(
    read: &SharedRead,
    snapshot: u64,
    operands_at: &BTreeMap<u64, Vec<Bytes>>,
    budgets: &[u64],
) -> Result<(), TestCaseError> {
    let reference_operands = operands_at
        .range(..=snapshot)
        .next_back()
        .map(|(_, operands)| operands.clone())
        .unwrap_or_default();
    for budget in budgets.iter().copied().chain([u64::MAX]) {
        let expected = reference(&reference_operands, budget);
        let actual = read.select(budget);
        prop_assert_eq!(
            ids(&actual),
            ids(&expected),
            "snapshot {} budget {}",
            snapshot,
            budget
        );
        let refused =
            |latest: &Option<LatestOperations>| latest.as_ref().and_then(LatestOperations::refused);
        prop_assert_eq!(refused(&actual).is_some(), refused(&expected).is_some());
        if let (Some(actual), Some(expected)) = (refused(&actual), refused(&expected)) {
            prop_assert!(actual >= expected && actual > budget);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Commits applied in sequence order but recorded in any order, around a
    /// build at any point from its own snapshot or a fresh one, with pins
    /// opened and closed anywhere and searches started mid-history whose
    /// plans wait across later recordings, leave a view whose selection at
    /// every snapshot it serves is exactly the storage read's at that
    /// snapshot, for every budget, both when each search is served and once
    /// the history ends; every snapshot at or above an open pin and the base
    /// is served; and the view holds exactly the outstanding operations live.
    #[test]
    fn view_selections_match_storage_reads_at_every_snapshot(
        steps in proptest::collection::vec(step(), 1..40),
        delays in proptest::collection::vec(0_usize..4, 40),
        build_at in 0_usize..44,
        fresh in any::<bool>(),
        install_delay in 0_usize..3,
        pin_at in proptest::collection::vec(0_usize..40, 0..4),
        unpin_after in proptest::collection::vec(1_usize..10, 4),
        budgets in proptest::collection::vec(0_u64..400, 3),
        searches_at in proptest::collection::vec((0_usize..40, 0_u64..3), 0..8),
    ) {
        let shared = Arc::new(SharedQueues::default());
        let mut operands = Vec::<Bytes>::new();
        let mut operands_at = BTreeMap::<u64, Vec<Bytes>>::new();
        let mut outstanding = Vec::<QueuedOperation>::new();
        let mut next_id = 1_u128;
        // Commits applied but not yet recorded: (record at step, commit, sequence).
        let mut unrecorded = Vec::<(usize, CommittingQueue, u64)>::new();
        let mut building = None::<Building>;
        let mut installed = None::<u64>;
        // Open pins: (closes after step, pin, the snapshot taken after it).
        let mut pins = Vec::<(usize, SnapshotPin, u64)>::new();
        let mut searches = Vec::<Search>::new();
        // Searches served: their snapshot and capture, checked again at the end.
        let mut served = Vec::<(u64, SharedRead)>::new();
        for (index, step) in steps.iter().enumerate() {
            let sequence = index as u64 + 1;
            if index == build_at {
                // The building search first waits for commits in flight.
                for (_, commit, sequence) in unrecorded.drain(..) {
                    commit.applied(sequence);
                }
                let Plan::Build(started) = plan_now(&shared, target(), sequence - 1) else {
                    panic!("the first search builds");
                };
                building = Some(if !fresh && started.own_snapshot() {
                    shared.base_chosen(&started, sequence - 1);
                    Building::Chosen(started, sequence - 1, outstanding.clone())
                } else {
                    Building::Fresh(started)
                });
            }
            if pin_at.contains(&index) {
                pins.push((index + unpin_after[pins.len() % 4], shared.pin(), sequence - 1));
            }
            // A search starting now has a snapshot at or below every commit
            // applied so far, and above every commit registered later.
            if installed.is_some() {
                for (_, lag) in searches_at.iter().filter(|(at, _)| *at == index) {
                    let snapshot = (sequence - 1).saturating_sub(*lag);
                    let plan = Box::pin({
                        let shared = Arc::clone(&shared);
                        async move { shared.plan(target(), snapshot).await }
                    });
                    searches.push(Search { snapshot, plan });
                }
            }
            let changes = match step {
                Step::Enqueue(entities) => {
                    // A transaction enqueues one final operation per entity.
                    let mut seen = std::collections::HashSet::new();
                    let operations = entities
                        .iter()
                        .filter(|(of, _)| seen.insert(*of))
                        .map(|(of, dims)| {
                            next_id += 1;
                            operation(next_id, *of, *dims)
                        })
                        .collect::<Vec<_>>();
                    operands.push(QueueOperand::enqueue(&operations).unwrap().into_parts().0);
                    outstanding.extend(operations.iter().cloned());
                    Some(enqueue(target(), &operations))
                }
                Step::Acknowledge(positions) => {
                    let mut chosen = positions
                        .iter()
                        .filter(|position| **position < outstanding.len())
                        .copied()
                        .collect::<Vec<_>>();
                    chosen.sort_unstable();
                    chosen.dedup();
                    let acknowledged = chosen
                        .iter()
                        .rev()
                        .map(|position| outstanding.remove(*position).id())
                        .collect::<Vec<_>>();
                    (!acknowledged.is_empty()).then(|| {
                        operands.push(
                            QueueOperand::acknowledge(QueueFamily::Vector, acknowledged.iter().copied())
                                .unwrap()
                                .into_parts()
                                .0,
                        );
                        acknowledge(target(), &acknowledged)
                    })
                }
            };
            operands_at.insert(sequence, operands.clone());
            if let Some(changes) = changes {
                unrecorded.push((index + delays[index], shared.begin_commit(changes), sequence));
            }
            // A fresh snapshot taken now sees this step's commit.
            if let Some(Building::Fresh(started)) = building.take() {
                shared.base_chosen(&started, sequence);
                building = Some(Building::Chosen(started, sequence, outstanding.clone()));
            }
            // Record what is due, in any order the delays give.
            let (due, waiting): (Vec<_>, Vec<_>) =
                unrecorded.into_iter().partition(|(at, _, _)| *at <= index);
            unrecorded = waiting;
            for (_, commit, sequence) in due.into_iter().rev() {
                commit.applied(sequence);
            }
            match building.take() {
                Some(Building::Chosen(started, chosen, queue)) if index >= build_at + install_delay => {
                    shared.install(started, prepared(chosen, &queue));
                    installed = Some(chosen);
                }
                unfinished => building = unfinished,
            }
            pins.retain(|(closes, _, _)| *closes > index);
            // Searches whose commits were all recorded are served now.
            let mut waiting = Vec::new();
            for mut search in searches.drain(..) {
                match poll_plan(&mut search.plan) {
                    None => waiting.push(search),
                    Some(Plan::Shared(read)) => {
                        check_selection(&read, search.snapshot, &operands_at, &budgets)?;
                        served.push((search.snapshot, read));
                    }
                    Some(Plan::Fallback) => {}
                    // The view drained and was freed; this search abandons
                    // its build.
                    Some(Plan::Build(builder)) => drop(builder),
                }
            }
            searches = waiting;
        }
        for (_, commit, sequence) in unrecorded {
            commit.applied(sequence);
        }
        for mut search in searches {
            let plan = poll_plan(&mut search.plan);
            prop_assert!(plan.is_some(), "nothing is in flight");
            if let Some(Plan::Shared(read)) = plan {
                check_selection(&read, search.snapshot, &operands_at, &budgets)?;
                served.push((search.snapshot, read));
            }
        }
        // Captures still select what they did, whatever happened since.
        for (snapshot, read) in &served {
            check_selection(read, *snapshot, &operands_at, &budgets)?;
        }
        let last = steps.len() as u64;
        let base = match (building, installed) {
            (Some(Building::Chosen(started, chosen, queue)), None) => {
                shared.install(started, prepared(chosen, &queue));
                chosen
            }
            (None, Some(chosen)) => chosen,
            (None, None) => {
                build(&shared, target(), last, &outstanding);
                last
            }
            (Some(_), Some(_)) | (Some(Building::Fresh(_)), None) => {
                unreachable!("one build, whose fresh base is chosen in its own step")
            }
        };
        // A view that drained and was freed is built again at the end.
        let base = if ready(&shared, target()) {
            base
        } else {
            build(&shared, target(), last, &outstanding);
            last
        };
        for snapshot in 0..=last {
            match plan_now(&shared, target(), snapshot) {
                Plan::Shared(read) => {
                    prop_assert!(snapshot >= base);
                    check_selection(&read, snapshot, &operands_at, &budgets)?;
                }
                Plan::Fallback => {
                    for (_, _, pinned) in &pins {
                        prop_assert!(
                            snapshot < (*pinned).max(base),
                            "snapshot {} at or above pin {} and base {} is served", snapshot, pinned, base
                        );
                    }
                }
                Plan::Build(_) => prop_assert!(false, "the view was built"),
            }
        }
        let state = shared.state.lock();
        let ViewState::Ready(view) = &state.targets[&target()].view else {
            panic!("the view is ready");
        };
        let mut live = view
            .chunks
            .values()
            .flat_map(|chunk| chunk.slots.iter())
            .filter(|slot| slot.acknowledged.load(std::sync::atomic::Ordering::Relaxed) == OUTSTANDING)
            .map(|slot| slot.operation.id().get())
            .collect::<Vec<_>>();
        live.sort_unstable();
        let mut expected = outstanding.iter().map(|operation| operation.id().get()).collect::<Vec<_>>();
        expected.sort_unstable();
        prop_assert_eq!(live, expected);
        let held = view
            .chunks
            .values()
            .flat_map(|chunk| chunk.slots.iter())
            .map(|slot| charged_bytes(slot.operation.retained_bytes()))
            .sum::<u64>();
        prop_assert_eq!(view.charged, held);
        prop_assert_eq!(view.slots.len(), view.chunks.values().map(|chunk| chunk.slots.len()).sum::<usize>());
        prop_assert_eq!(view.acknowledged_slots, view.acknowledged.values().map(|acknowledged| acknowledged.ids.len()).sum::<usize>());
        prop_assert_eq!(
            view.acknowledged_bytes,
            view.acknowledged.values().map(|acknowledged| acknowledged.bytes).sum::<u64>()
        );
        prop_assert!(!view.compacting);
        prop_assert_eq!(
            state.held,
            state.targets.values().map(|tracked| tracked.view.weight()).sum::<u64>()
        );
    }
}
