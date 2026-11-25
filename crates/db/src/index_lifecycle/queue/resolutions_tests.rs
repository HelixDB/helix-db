//! The resolution budget charges reads what they resolve, never lets small
//! reads wait behind large ones, and bounds what large ones hold.

use std::time::Duration;

use futures::FutureExt;

use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::{IndexGenerationId, IndexId};

const KIB: u64 = 1 << 10;

fn target(generation: u64) -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(generation).unwrap(),
    )
}

/// A budget of `units` kibibytes (an eighth of them the small lane), whose
/// largest read is charged `worst` kibibytes.
fn lanes(units: u64, worst: u64) -> Arc<ResolutionBudget> {
    Arc::new(ResolutionBudget::new(units * KIB, worst * KIB))
}

/// Charges a read that must not wait.
fn charge_now(budget: &Arc<ResolutionBudget>, target: QueueTarget, snapshot: u64) -> Charge {
    budget
        .charge(target, snapshot)
        .now_or_never()
        .expect("the read is charged at once")
}

/// Awaits what a test expects to finish once it is released.
async fn soon<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("released")
}

/// Re-charges a read that must not wait.
fn resolve_now(charge: &mut Charge, bytes: u64) {
    charge
        .resolved(bytes)
        .now_or_never()
        .expect("the read is re-charged at once");
}

#[test]
fn the_budget_is_an_eighth_of_memory_split_into_a_small_and_a_large_lane() {
    const GIB: u64 = 1 << 30;
    let host = ResolutionBudget::for_host(Some(64 * GIB), GIB);
    // 8 GiB: a small lane of 1 GiB, the rest large; the worst read is twice
    // the retained bytes.
    assert_eq!(host.small.capacity, (GIB / KIB) as u32);
    assert_eq!(host.large.capacity, (7 * GIB / KIB) as u32);
    assert_eq!(host.worst, (2 * GIB / KIB) as u32);
    // An unknown ceiling allows one worst read; a worst read beyond the
    // large lane is charged all of it.
    let unknown = ResolutionBudget::for_host(None, GIB);
    assert_eq!(
        unknown.small.capacity + unknown.large.capacity,
        (2 * GIB / KIB) as u32
    );
    assert_eq!(unknown.worst, unknown.large.capacity);
    // Each lane holds at least one unit, whatever the budget.
    let tiny = ResolutionBudget::new(0, 0);
    assert_eq!(
        (tiny.small.capacity, tiny.large.capacity, tiny.worst),
        (1, 1, 1)
    );
    // A charge is small up to a 32nd of the small lane.
    let budget = lanes(8 * 64, 64);
    assert_eq!(budget.small.capacity, 64);
    assert_eq!(budget.lane(2), LaneKind::Small);
    assert_eq!(budget.lane(3), LaneKind::Large);
}

#[test]
fn a_read_is_charged_twice_what_it_resolved_in_whole_kibibytes() {
    // Small charges are at most 4,096 units.
    let budget = lanes(1 << 20, 1 << 13);
    let mut read = charge_now(&budget, target(1), 5);
    // Never read: the worst case, in the large lane.
    assert_eq!(read.held(), Some((false, 1 << 13)));
    resolve_now(&mut read, 3 * KIB / 2);
    assert_eq!(read.held(), Some((false, 3)));
    assert_eq!(budget.charged(), (0, 3));
    drop(read);
    assert_eq!(budget.charged(), (0, 0));
    // An empty queue still takes one unit.
    let mut empty = charge_now(&budget, target(2), 5);
    resolve_now(&mut empty, 0);
    assert_eq!(empty.held(), Some((false, 1)));
}

#[test]
fn reads_at_or_below_the_newest_read_are_charged_its_size_at_once() {
    let budget = lanes(1 << 20, 1 << 13);
    let mut probe = charge_now(&budget, target(1), 5);
    resolve_now(&mut probe, 2 * KIB);
    drop(probe);
    // At the same snapshot and an older one, without probing.
    let same = charge_now(&budget, target(1), 5);
    let older = charge_now(&budget, target(1), 4);
    assert_eq!(same.held(), Some((true, 4)));
    assert_eq!(older.held(), Some((true, 4)));
    // Nor while a newer read probes.
    let probe = charge_now(&budget, target(1), 7);
    assert_eq!(charge_now(&budget, target(1), 5).held(), Some((true, 4)));
    drop(probe);
    // An older read never replaces the newest size.
    let mut older = older;
    resolve_now(&mut older, 100 * KIB);
    assert_eq!(charge_now(&budget, target(1), 5).held(), Some((true, 4)));
    // Another target knows nothing of it.
    assert_eq!(
        charge_now(&budget, target(2), 5).held(),
        Some((false, 1 << 13))
    );
}

#[tokio::test]
async fn one_read_per_target_probes_a_newer_snapshot_and_the_rest_wait_for_it() {
    let budget = lanes(1 << 20, 1 << 13);
    let mut probe = charge_now(&budget, target(1), 5);
    // Newer reads of the same target wait for the probe; another target's
    // and older reads do not.
    let newer = tokio::spawn({
        let budget = Arc::clone(&budget);
        async move { budget.charge(target(1), 5).await.held() }
    });
    let newest = tokio::spawn({
        let budget = Arc::clone(&budget);
        async move {
            let mut charge = budget.charge(target(1), 6).await;
            let held = charge.held();
            charge.resolved(KIB).await;
            held
        }
    });
    let other = charge_now(&budget, target(2), 9);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!newer.is_finished() && !newest.is_finished());
    resolve_now(&mut probe, 8 * KIB);
    // The read at the probe's snapshot is charged what it read; the newer
    // one probes next, expecting the same.
    assert_eq!(soon(newer).await.unwrap(), Some((true, 16)));
    assert_eq!(soon(newest).await.unwrap(), Some((true, 16)));
    // Its size is now the newest.
    assert_eq!(charge_now(&budget, target(1), 6).held(), Some((true, 2)));
    drop((probe, other));
}

#[tokio::test]
async fn a_probe_dropped_before_it_resolves_hands_probing_to_a_waiting_read() {
    let budget = lanes(1 << 20, 1 << 13);
    let probe = charge_now(&budget, target(1), 5);
    let waiting = tokio::spawn({
        let budget = Arc::clone(&budget);
        async move { budget.charge(target(1), 5).await.held() }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    drop(probe);
    // Nothing was read, so it probes for the worst case.
    assert_eq!(soon(waiting).await.unwrap(), Some((false, 1 << 13)));
    // A probe cancelled while it waits for its lane hands it over too.
    let budget = lanes(8 * 64, 1 << 10);
    let full = charge_now(&budget, target(3), 1);
    let mut cancelled = Box::pin(budget.charge(target(1), 1));
    assert!((&mut cancelled).now_or_never().is_none());
    drop(cancelled);
    drop(full);
    assert!(budget.charge(target(1), 1).now_or_never().is_some());
}

#[tokio::test]
async fn small_reads_never_wait_behind_large_ones() {
    // Small lane 64 units (charges of at most 2), large lane 448.
    let budget = lanes(8 * 64, 448);
    // 32 empty queues were read before.
    for generation in 2..34 {
        resolve_now(&mut charge_now(&budget, target(generation), 1), 0);
    }
    let mut large = charge_now(&budget, target(1), 1);
    resolve_now(&mut large, 200 * KIB);
    // Another large read waits for the lane, at its head.
    let waiting = tokio::spawn({
        let budget = Arc::clone(&budget);
        async move { budget.charge(target(1), 1).await.held() }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiting.is_finished());
    // Empty and small queues' reads go ahead, 32 at once.
    let small = (2..34)
        .map(|generation| charge_now(&budget, target(generation), 1))
        .collect::<Vec<_>>();
    // The waiting read gathers whatever the large lane frees meanwhile.
    assert_eq!(budget.charged(), (32, 448));
    drop(large);
    assert_eq!(soon(waiting).await.unwrap(), Some((false, 400)));
    drop(small);
}

#[tokio::test]
async fn a_read_that_outgrew_its_charge_takes_the_difference_or_waits_holding_nothing() {
    let budget = lanes(8 * 64, 448);
    let mut first = charge_now(&budget, target(1), 1);
    resolve_now(&mut first, 50 * KIB);
    let mut second = charge_now(&budget, target(1), 1);
    // Both expected 100 units; each found 300. The first gets it while the
    // lane has it.
    resolve_now(&mut first, 150 * KIB);
    assert_eq!(first.held(), Some((false, 300)));
    // The second cannot, so it gives back its 100 and waits for 300,
    // holding nothing: the first never waits on it.
    let mut growing = Box::pin(async move {
        second.resolved(150 * KIB).await;
        second
    });
    assert!((&mut growing).now_or_never().is_none());
    assert_eq!(first.held(), Some((false, 300)));
    drop(first);
    let second = soon(growing).await;
    assert_eq!(second.held(), Some((false, 300)));
    drop(second);
    // A small charge that outgrows the small lane moves to the large one.
    let mut probe = charge_now(&budget, target(2), 1);
    resolve_now(&mut probe, 0);
    let mut moved = charge_now(&budget, target(2), 1);
    assert_eq!(moved.held(), Some((true, 1)));
    drop(probe);
    resolve_now(&mut moved, 50 * KIB);
    assert_eq!(moved.held(), Some((false, 100)));
    assert_eq!(budget.charged(), (0, 100));
    // So does one that fills a one-unit small lane.
    let budget = lanes(8, 1);
    resolve_now(&mut charge_now(&budget, target(1), 1), 0);
    let mut moved = charge_now(&budget, target(1), 1);
    assert_eq!(moved.held(), Some((true, 1)));
    resolve_now(&mut moved, 50 * KIB);
    assert_eq!(moved.held(), Some((false, 7)));
}

#[test]
fn a_read_larger_than_its_lane_is_charged_all_of_it_and_runs_alone() {
    let budget = lanes(8 * 64, 1 << 20);
    let mut huge = charge_now(&budget, target(1), 1);
    assert_eq!(huge.held(), Some((false, 448)), "the worst case, capped");
    resolve_now(&mut huge, 1 << 30);
    assert_eq!(huge.held(), Some((false, 448)));
    // The next read of it waits for the whole lane.
    assert!(budget.charge(target(1), 1).now_or_never().is_none());
    drop(huge);
    assert!(budget.charge(target(1), 1).now_or_never().is_some());
}
