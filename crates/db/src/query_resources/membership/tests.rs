use super::*;
use crate::HelixDbError;

#[test]
fn rejected_and_abandoned_proposals_do_not_allocate_or_change_membership() {
    let empty_budget = Budget::new(0);
    let (result, allocation) =
        crate::allocation_testing::observe(|| Delta::new(Some(&empty_budget)));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocation.allocations, 0);
    let budget = Budget::new(1024 * 1024);
    let mut delta = Delta::new(Some(&budget)).unwrap();
    let before = budget.available();
    let proposal = delta.prepare(7, Change::Present).unwrap();
    assert!(budget.available() < before);
    drop(proposal);
    assert_eq!(budget.available(), before);
    assert!(!delta.value.contains_member(7));
    let occupied = budget.reserve(budget.available()).unwrap();
    let (result, allocation) =
        crate::allocation_testing::observe(|| delta.prepare(7, Change::Absent));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocation.allocations, 0);
    drop(result);
    assert!(!delta.value.contains_member(7));
    drop(occupied);
    drop(delta);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn membership_history_preserves_canonical_bytes_and_bounds_unique_growth() {
    let budget = Budget::new(16 * 1024 * 1024);
    for admission in [None, Some(&budget)] {
        let mut delta = Delta::new(admission).unwrap();
        let before = budget.available();
        let ((), allocation) = crate::allocation_testing::observe(|| {
            for id in (0..8193)
                .chain((1..65).map(|id| id << 32))
                .chain((1..65).map(|id| id << 16))
                .chain([u64::MAX])
            {
                delta.prepare(id, Change::Present).unwrap().apply();
            }
        });
        if admission.is_some() {
            assert!(before - budget.available() >= allocation.bytes);
        }
        let mut reference = BitmapMembershipDelta::default();
        let mut expected = std::collections::BTreeSet::new();
        for id in (0..8193)
            .chain((1..65).map(|id| id << 32))
            .chain((1..65).map(|id| id << 16))
            .chain([u64::MAX])
        {
            reference.add(id);
            expected.insert(id);
        }
        let before_flips = budget.available();
        for pass in 0..3 {
            for id in (0..8193)
                .chain((1..65).map(|id| id << 32))
                .chain((1..65).map(|id| id << 16))
                .chain([u64::MAX])
            {
                let change = if pass % 2 == 0 {
                    Change::Absent
                } else {
                    Change::Present
                };
                delta.prepare(id, change).unwrap().apply();
                match change {
                    Change::Present => {
                        reference.add(id);
                        expected.insert(id);
                    }
                    Change::Absent => {
                        reference.remove(id);
                        expected.remove(&id);
                    }
                }
            }
        }
        assert_eq!(
            budget.available(),
            before_flips,
            "repeated flips coalesce admission"
        );
        let (raw, memory) = delta.into_parts();
        assert_eq!(raw.encode(), reference.encode());
        let mut actual = roaring::RoaringTreemap::new();
        raw.apply_to(&mut actual);
        assert_eq!(
            actual.iter().collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            budget.available(),
            before_flips,
            "transferred raw delta retains admission"
        );
        drop(raw);
        drop(memory);
        assert_eq!(budget.available(), 16 * 1024 * 1024);
    }
}

#[test]
fn transitions_are_admitted_before_growth_and_flips_use_existing_allowance() {
    let budget = Budget::new(1024 * 1024);
    let mut delta = Delta::new(Some(&budget)).unwrap();
    for id in 0..4096 {
        delta.prepare(id, Change::Present).unwrap().apply();
    }
    let occupied = budget.reserve(budget.available()).unwrap();
    assert!(matches!(
        delta.prepare(4096, Change::Present),
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert!(!delta.value.contains_member(4096));
    for id in 0..4096 {
        delta.prepare(id, Change::Absent).unwrap().apply();
    }
    drop(occupied);
    delta.prepare(4096, Change::Absent).unwrap().apply();
    let before = budget.available();
    let occupied = budget.reserve(before).unwrap();
    for id in (0..4097).rev() {
        delta.prepare(id, Change::Present).unwrap().apply();
    }
    drop(occupied);
    assert_eq!(budget.available(), before);
    drop(delta);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn two_direction_preflight_can_fail_without_applying_the_first_direction() {
    let budget = Budget::new(1024 * 1024);
    let mut outgoing = Delta::new(Some(&budget)).unwrap();
    let mut incoming = Delta::new(Some(&budget)).unwrap();
    let before = budget.available();
    let proposal = outgoing.prepare(1, Change::Present).unwrap();
    let one = before - budget.available();
    drop(proposal);
    let occupied = budget.reserve(before - one).unwrap();
    let first = outgoing.prepare(1, Change::Present).unwrap();
    assert!(matches!(
        incoming.prepare(1, Change::Present),
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    drop(first);
    assert!(!outgoing.value.contains_member(1));
    assert!(!incoming.value.contains_member(1));
    drop(occupied);
    let first = outgoing.prepare(1, Change::Present).unwrap();
    let second = incoming.prepare(1, Change::Present).unwrap();
    first.apply();
    second.apply();
    assert!(outgoing.value.contains_member(1));
    assert!(incoming.value.contains_member(1));
    drop(outgoing);
    drop(incoming);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn a_single_container_needs_no_heap_allocations_for_its_counters() {
    let budget = Budget::new(1024 * 1024);
    let mut native = Delta::new(None).unwrap();
    let mut admitted = Delta::new(Some(&budget)).unwrap();
    let (_, native_allocations) =
        crate::allocation_testing::observe(|| native.prepare(1, Change::Present).unwrap().apply());
    let (_, admitted_allocations) = crate::allocation_testing::observe(|| {
        admitted.prepare(1, Change::Present).unwrap().apply()
    });
    assert_eq!(
        admitted_allocations.allocations, native_allocations.allocations,
        "single-container tracking must stay inline"
    );
}
