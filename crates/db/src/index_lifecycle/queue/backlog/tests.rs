//! Exact admission, reference-count, and outcome contracts for the ledger.

use super::*;
use crate::encoding::v2::keys::scope::TenantId;
use crate::index_lifecycle::IndexElementKind;

fn target(index: u64, generation: u64) -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(index).unwrap(),
        IndexGenerationId::new(generation).unwrap(),
    )
}

fn entity(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: crate::index_lifecycle::IndexEntityId::new(id),
    }
}

fn id(value: u128) -> QueuedOperationId {
    QueuedOperationId::try_from_u128(value).unwrap()
}

fn charge(target: QueueTarget, entity_id: u64, operation: u128, bytes: u64) -> OperationCharge {
    OperationCharge {
        target,
        entity: entity(entity_id),
        id: id(operation),
        bytes,
    }
}

fn ledger(max_retained_bytes: u64, max_members: u64) -> Arc<IndexOperationBacklog> {
    IndexOperationBacklog::new(BacklogLimits {
        max_retained_bytes,
        max_members,
    })
}

fn usage(backlog: &IndexOperationBacklog, index: u64) -> BacklogUsage {
    backlog.usage(DataScope::LegacyUnscoped, IndexId::new(index).unwrap())
}

#[test]
fn byte_limit_accepts_exactly_the_limit_and_rejects_one_more_byte() {
    let backlog = ledger(100, 1_000);
    backlog
        .reserve(&[charge(target(1, 1), 1, 1, 60)], &[])
        .unwrap()
        .committed();
    backlog
        .reserve(&[charge(target(1, 1), 2, 2, 40)], &[])
        .expect("reaching the limit exactly is admitted")
        .committed();
    let error = backlog
        .reserve(&[charge(target(1, 1), 3, 3, 1)], &[])
        .expect_err("one byte above the limit is rejected");
    assert!(matches!(
        error,
        HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::RetainedBytes,
            requested: 101,
            limit: 100,
            ..
        }
    ));
    assert!(error.is_index_backpressure());
    assert_eq!(usage(&backlog, 1).retained_bytes, 100);
    // Acknowledging releases exactly the acknowledged bytes.
    backlog.acknowledge([id(1)]);
    assert_eq!(usage(&backlog, 1).retained_bytes, 40);
    backlog
        .reserve(&[charge(target(1, 1), 3, 3, 60)], &[])
        .unwrap()
        .committed();
}

#[test]
fn members_count_once_per_generation_entity_until_every_operation_is_acknowledged() {
    let backlog = ledger(u64::MAX, 2);
    for operation in 1..=3 {
        // Repeated operations for one entity never add a member.
        backlog
            .reserve(&[charge(target(1, 1), 7, operation, 10)], &[])
            .unwrap()
            .committed();
    }
    backlog
        .reserve(&[charge(target(1, 1), 8, 4, 10)], &[])
        .expect("the second member reaches the limit exactly")
        .committed();
    assert_eq!(usage(&backlog, 1).members, 2);
    let error = backlog
        .reserve(&[charge(target(1, 1), 9, 5, 10)], &[])
        .expect_err("a third member is above the limit");
    assert!(matches!(
        error,
        HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::PendingMembers,
            requested: 3,
            limit: 2,
            ..
        }
    ));
    // Acknowledging older operations keeps membership while newer remain.
    backlog.acknowledge([id(1), id(2)]);
    assert_eq!(usage(&backlog, 1).members, 2);
    assert!(backlog
        .reserve(&[charge(target(1, 1), 9, 5, 10)], &[])
        .is_err());
    backlog.acknowledge([id(3)]);
    assert_eq!(usage(&backlog, 1).members, 1);
    backlog
        .reserve(&[charge(target(1, 1), 9, 5, 10)], &[])
        .unwrap()
        .committed();
}

#[test]
fn generations_are_distinct_members_aggregated_per_logical_index() {
    let backlog = ledger(u64::MAX, 2);
    backlog
        .reserve(&[charge(target(1, 1), 7, 1, 10)], &[])
        .unwrap()
        .committed();
    // The same entity in another generation of the same index is a new member.
    backlog
        .reserve(&[charge(target(1, 2), 7, 2, 10)], &[])
        .unwrap()
        .committed();
    assert!(backlog
        .reserve(&[charge(target(1, 3), 7, 3, 10)], &[])
        .is_err());
    // Another logical index has its own limits.
    backlog
        .reserve(&[charge(target(2, 1), 7, 3, 10)], &[])
        .unwrap()
        .committed();
    assert_eq!(
        backlog.outstanding_targets(),
        vec![target(1, 1), target(1, 2), target(2, 1)]
    );
    // Tenant scopes are distinct logical indexes.
    let tenant = QueueTarget::new(
        DataScope::Tenant(TenantId::from_u128(9)),
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    );
    backlog
        .reserve(&[charge(tenant, 7, 4, 10)], &[])
        .unwrap()
        .committed();
}

#[test]
fn multi_index_reservations_are_all_or_nothing() {
    let backlog = ledger(100, 10);
    backlog
        .reserve(&[charge(target(2, 1), 1, 1, 95)], &[])
        .unwrap()
        .committed();
    let error = backlog
        .reserve(
            &[
                charge(target(1, 1), 1, 2, 50),
                charge(target(2, 1), 2, 3, 10),
            ],
            &[],
        )
        .expect_err("the second index rejects the transaction");
    assert!(error.is_index_backpressure());
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
    assert_eq!(usage(&backlog, 2).retained_bytes, 95);
    // A duplicate operation ID is an invariant violation, not backpressure,
    // whether it is already retained or repeated within the transaction.
    assert!(matches!(
        backlog.reserve(&[charge(target(1, 1), 1, 1, 1)], &[]),
        Err(HelixDbError::InvariantViolation(_))
    ));
    assert!(matches!(
        backlog.reserve(
            &[charge(target(1, 1), 1, 7, 1), charge(target(1, 1), 2, 7, 1),],
            &[]
        ),
        Err(HelixDbError::InvariantViolation(_))
    ));
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
}

/// A blocked build's generation refuses every charge that takes a limit past
/// it with the non-retryable [`HelixDbError::IndexBuildBlocked`], except a
/// lone repair of its blocker's entity: the entity's first operation, or any
/// operation removing it from the index, which either limit admits. Writes to
/// pending entities add no member, so above the member limit they stay
/// admitted, blocked or not; only the byte limit refuses them. A removal
/// leaves the entity a member, so writing it back stays refused by the byte
/// limit and a block adds at most one member and two operations beyond the
/// limits.
#[test]
fn a_blocked_build_admits_only_its_blocker_repairs_beyond_the_limits() {
    let operation_id = crate::index_lifecycle::IndexOperationId::new_v4();
    let blocked = |repair| {
        [BlockedBuild {
            target: target(1, 1),
            operation_id,
            repair: Some(repair),
        }]
    };
    let replace = blocked(BlockerRepair::Replace(entity(9)));
    let remove = blocked(BlockerRepair::Remove(entity(9)));
    let refused = |result: Result<BacklogReservation>, expected: IndexBackpressureResource| {
        let error = result.expect_err("a blocked build's saturated generation refuses");
        assert!(!error.is_index_backpressure(), "{error}");
        assert!(
            error
                .to_string()
                .contains(&operation_id.as_uuid().to_string()),
            "{error}"
        );
        assert!(
            matches!(
                error,
                HelixDbError::IndexBuildBlocked { resource, .. } if resource == expected
            ),
            "{error:?}"
        );
    };

    let backlog = ledger(u64::MAX, 2);
    // Below the limits a blocked build admits as usual.
    for entity_id in 1..=2 {
        backlog
            .reserve(
                &[charge(target(1, 1), entity_id, u128::from(entity_id), 10)],
                &replace,
            )
            .unwrap()
            .committed();
    }
    backlog
        .reserve(&[charge(target(1, 1), 1, 3, 10)], &replace)
        .expect("an existing member adds none")
        .committed();
    refused(
        backlog.reserve(&[charge(target(1, 1), 3, 4, 10)], &replace),
        IndexBackpressureResource::PendingMembers,
    );
    for repair in [replace, remove] {
        refused(
            backlog.reserve(
                &[
                    charge(target(1, 1), 9, 5, 10),
                    charge(target(1, 1), 3, 6, 10),
                ],
                &repair,
            ),
            IndexBackpressureResource::PendingMembers,
        );
    }
    // A repair of another entity is not this blocker's.
    for repair in [
        Some(BlockerRepair::Replace(entity(8))),
        Some(BlockerRepair::Remove(entity(8))),
        None,
    ] {
        refused(
            backlog.reserve(
                &[charge(target(1, 1), 9, 5, 10)],
                &[BlockedBuild {
                    repair,
                    ..replace[0]
                }],
            ),
            IndexBackpressureResource::PendingMembers,
        );
    }
    // Each generation answers only to its own build's blocker: another
    // build naming the same entity exempts nothing here, wherever it is
    // listed.
    let other = BlockedBuild {
        target: target(2, 1),
        operation_id: crate::index_lifecycle::IndexOperationId::new_v4(),
        repair: Some(BlockerRepair::Replace(entity(9))),
    };
    assert!(backlog
        .reserve(&[charge(target(1, 1), 9, 5, 10)], &[other])
        .expect_err("saturated")
        .is_index_backpressure());
    refused(
        backlog.reserve(
            &[charge(target(1, 1), 9, 5, 10)],
            &[
                other,
                BlockedBuild {
                    repair: Some(BlockerRepair::Replace(entity(8))),
                    ..replace[0]
                },
            ],
        ),
        IndexBackpressureResource::PendingMembers,
    );
    // Without the blocked build the same charge is ordinary backpressure.
    assert!(backlog
        .reserve(&[charge(target(1, 1), 9, 5, 10)], &[])
        .expect_err("saturated")
        .is_index_backpressure());
    backlog
        .reserve(&[charge(target(1, 1), 9, 5, 10)], &replace)
        .expect("the repair is admitted above the member limit")
        .committed();
    assert_eq!(usage(&backlog, 1).members, 3);
    // Above the member limit a pending entity still adds none: a resident's
    // update, the repaired entity's next writes, and several together are
    // admitted while the build is blocked and once a retry leaves it
    // runnable (no longer blocked).
    let runnable: &[BlockedBuild] = &[];
    let writes: [(&[u64], u128, &[BlockedBuild]); 7] = [
        (&[1], 6, &replace),
        (&[9], 7, &replace),
        (&[9], 8, &remove),
        (&[9], 9, &replace),
        (&[1, 9], 10, &replace),
        (&[2], 12, runnable),
        (&[9, 1], 13, runnable),
    ];
    for (entity_ids, first_operation, blocked) in writes {
        let charges: Vec<_> = (first_operation..)
            .zip(entity_ids)
            .map(|(operation, entity_id)| charge(target(1, 1), *entity_id, operation, 10))
            .collect();
        backlog
            .reserve(&charges, blocked)
            .expect("an existing member adds none")
            .committed();
    }
    assert_eq!(
        (usage(&backlog, 1).members, usage(&backlog, 1).operations),
        (3, 13)
    );
    // A new member is still refused, alone or beside pending ones, and
    // counts against the members already above the limit.
    for charges in [
        &[charge(target(1, 1), 3, 15, 10)][..],
        &[
            charge(target(1, 1), 1, 15, 10),
            charge(target(1, 1), 3, 16, 10),
        ][..],
    ] {
        refused(
            backlog.reserve(charges, &replace),
            IndexBackpressureResource::PendingMembers,
        );
        assert!(matches!(
            backlog.reserve(charges, runnable),
            Err(HelixDbError::IndexBackpressure {
                resource: IndexBackpressureResource::PendingMembers,
                requested: 4,
                limit: 2,
                ..
            })
        ));
    }
    assert_eq!(usage(&backlog, 1).operations, 13);

    // Removing a blocker's entity that had no pending work takes the members
    // above the limit the same way, and pending entities stay writable,
    // including the removed entity written back.
    let backlog = ledger(u64::MAX, 2);
    for entity_id in 1..=2 {
        backlog
            .reserve(
                &[charge(target(1, 1), entity_id, u128::from(entity_id), 10)],
                &remove,
            )
            .unwrap()
            .committed();
    }
    backlog
        .reserve(&[charge(target(1, 1), 9, 3, 10)], &remove)
        .expect("the removal is admitted above the member limit")
        .committed();
    for (entity_id, operation, blocked) in [(1, 4, &remove), (9, 5, &replace)] {
        backlog
            .reserve(&[charge(target(1, 1), entity_id, operation, 10)], blocked)
            .expect("an existing member adds none")
            .committed();
    }
    refused(
        backlog.reserve(&[charge(target(1, 1), 3, 6, 10)], &remove),
        IndexBackpressureResource::PendingMembers,
    );
    assert_eq!(
        (usage(&backlog, 1).members, usage(&backlog, 1).operations),
        (3, 5)
    );

    let backlog = ledger(100, u64::MAX);
    backlog
        .reserve(&[charge(target(1, 1), 1, 1, 100)], &replace)
        .unwrap()
        .committed();
    refused(
        backlog.reserve(&[charge(target(1, 1), 1, 2, 1)], &replace),
        IndexBackpressureResource::RetainedBytes,
    );
    // A repair that fits no transaction on its own is still too large.
    for repair in [replace, remove] {
        assert!(matches!(
            backlog.reserve(&[charge(target(1, 1), 9, 2, 101)], &repair),
            Err(HelixDbError::IndexOperationBatchTooLarge { .. })
        ));
    }
    // A repair whose commit definitely aborts leaves the next one first.
    drop(
        backlog
            .reserve(&[charge(target(1, 1), 9, 2, 100)], &replace)
            .expect("the repair is admitted above the byte limit"),
    );
    backlog
        .reserve(&[charge(target(1, 1), 9, 3, 100)], &replace)
        .expect("an aborted repair leaves the next one first")
        .committed();
    assert_eq!(usage(&backlog, 1).retained_bytes, 200);
    refused(
        backlog.reserve(&[charge(target(1, 1), 9, 4, 1)], &replace),
        IndexBackpressureResource::RetainedBytes,
    );
    // A first write that left the blocker in place still lets its entity be
    // removed, which writing it back cannot follow.
    backlog
        .reserve(&[charge(target(1, 1), 9, 4, 1)], &remove)
        .expect("a removal is admitted above the byte limit")
        .committed();
    assert_eq!(usage(&backlog, 1).retained_bytes, 201);
    refused(
        backlog.reserve(&[charge(target(1, 1), 9, 5, 1)], &replace),
        IndexBackpressureResource::RetainedBytes,
    );
    assert_eq!(
        (usage(&backlog, 1).members, usage(&backlog, 1).operations),
        (2, 3)
    );
}

/// Only a limit a transaction grows can refuse it. Members above the limit,
/// as a reopen with a lower limit loads, still admit writes to pending
/// entities and refuse only new members, counted against every member
/// present; the byte limit refuses any write while it is exceeded.
#[test]
fn above_a_limit_only_writes_that_grow_it_are_refused() {
    let backlog = ledger(u64::MAX, 2);
    backlog.load_durable(
        target(1, 1),
        (1..=3).map(|entity_id| (id(u128::from(entity_id)), entity(entity_id), 10)),
    );
    backlog
        .reserve(
            &[
                charge(target(1, 1), 1, 4, 10),
                charge(target(1, 1), 3, 5, 10),
            ],
            &[],
        )
        .expect("pending members add none")
        .committed();
    assert!(matches!(
        backlog.reserve(&[charge(target(1, 1), 4, 6, 10)], &[]),
        Err(HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::PendingMembers,
            requested: 4,
            limit: 2,
            ..
        })
    ));
    // Releasing members below the limit admits new ones again.
    backlog.acknowledge([id(1), id(3), id(4), id(5)]);
    backlog
        .reserve(&[charge(target(1, 1), 4, 6, 10)], &[])
        .expect("a member freed below the limit admits a new one")
        .committed();
    assert_eq!(usage(&backlog, 1).members, 2);

    let backlog = ledger(15, u64::MAX);
    backlog.load_durable(
        target(1, 1),
        [(id(1), entity(1), 10), (id(2), entity(2), 10)],
    );
    assert!(matches!(
        backlog.reserve(&[charge(target(1, 1), 1, 3, 1)], &[]),
        Err(HelixDbError::IndexBackpressure {
            resource: IndexBackpressureResource::RetainedBytes,
            requested: 21,
            limit: 15,
            ..
        })
    ));
}

#[test]
fn reservation_outcomes_release_only_on_definite_abort() {
    let backlog = ledger(1_000, 10);
    // Dropping before commit submission is a definite abort.
    drop(
        backlog
            .reserve(&[charge(target(1, 1), 1, 1, 10)], &[])
            .unwrap(),
    );
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());

    backlog
        .reserve(&[charge(target(1, 1), 1, 2, 10)], &[])
        .unwrap()
        .aborted();
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());

    // Dropping after submission (cancelled response) stays uncertain.
    let mut cancelled = backlog
        .reserve(&[charge(target(1, 1), 1, 3, 10)], &[])
        .unwrap();
    cancelled.begin_commit();
    drop(cancelled);
    let mut explicit = backlog
        .reserve(&[charge(target(1, 1), 2, 4, 10)], &[])
        .unwrap();
    explicit.begin_commit();
    explicit.uncertain();
    assert_eq!(
        usage(&backlog, 1),
        BacklogUsage {
            retained_bytes: 20,
            members: 2,
            operations: 2,
            uncertain_operations: 2,
        }
    );
    assert_eq!(
        backlog.uncertain_targets().into_iter().collect::<Vec<_>>(),
        vec![target(1, 1)]
    );

    // A reconciliation begun before the uncertainty cannot release it.
    let stale_ticket = ReconciliationTicket { clock: 0 };
    assert_eq!(
        backlog.finish_reconciliation(stale_ticket, target(1, 1), []),
        0
    );
    // A later flushed read proves operation 3 committed and 4 did not.
    let ticket = backlog.begin_reconciliation();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target(1, 1), [(id(3), entity(1), 10)]),
        1
    );
    assert_eq!(
        usage(&backlog, 1),
        BacklogUsage {
            retained_bytes: 10,
            members: 1,
            operations: 1,
            uncertain_operations: 0,
        }
    );
    // Uncertainty marked after a ticket was issued survives that ticket.
    let ticket = backlog.begin_reconciliation();
    let mut late = backlog
        .reserve(&[charge(target(1, 1), 3, 5, 10)], &[])
        .unwrap();
    late.begin_commit();
    late.uncertain();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target(1, 1), [(id(3), entity(1), 10)]),
        0
    );
    assert_eq!(usage(&backlog, 1).uncertain_operations, 1);
}

#[test]
fn startup_observation_charges_durable_operations_once() {
    let backlog = ledger(1_000, 10);
    let durable = [
        (id(1), entity(1), 25),
        (id(2), entity(1), 5),
        (id(3), entity(2), 7),
    ];
    backlog.load_durable(target(1, 1), durable);
    backlog.load_durable(target(1, 1), durable);
    assert_eq!(
        usage(&backlog, 1),
        BacklogUsage {
            retained_bytes: 37,
            members: 2,
            operations: 3,
            uncertain_operations: 0,
        }
    );
    backlog.acknowledge([id(1), id(2), id(3), id(99)]);
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
    assert!(backlog.outstanding_targets().is_empty());
}

#[test]
fn acknowledgements_time_only_operations_whose_commit_was_observed() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    // Committed here: timed.
    backlog
        .reserve(&[charge(target, 1, 1, 10), charge(target, 2, 2, 10)], &[])
        .unwrap()
        .committed();
    // Uncertain, then proven durable by a flushed read: censored.
    let mut uncertain = backlog.reserve(&[charge(target, 4, 4, 10)], &[]).unwrap();
    uncertain.begin_commit();
    uncertain.uncertain();
    // The read also discovers operation 3 in storage: censored.
    let ticket = backlog.begin_reconciliation();
    let read = [1_u64, 2, 3, 4].map(|operation| (id(u128::from(operation)), entity(operation), 10));
    assert_eq!(backlog.finish_reconciliation(ticket, target, read), 0);
    // Acknowledged before the producer saw its commit return: censored.
    let mut racing = backlog.reserve(&[charge(target, 5, 5, 10)], &[]).unwrap();
    racing.begin_commit();

    let pending = backlog.totals();
    assert_eq!(
        pending.outcomes,
        LedgerOutcomes {
            committed: 2,
            discovered: 2,
            acknowledged: 0,
            acknowledged_censored: 0,
        }
    );
    assert_eq!(pending.usage.operations, 5);

    // Unknown IDs and repeated acknowledgements change nothing.
    backlog.acknowledge([id(1), id(2), id(3), id(4), id(5), id(1), id(99)]);
    racing.committed();
    let settled = backlog.totals();
    assert_eq!(
        settled.outcomes,
        LedgerOutcomes {
            committed: 3,
            discovered: 2,
            acknowledged: 5,
            acknowledged_censored: 3,
        }
    );
    assert_eq!(settled.usage, BacklogUsage::default());
    assert_eq!(settled.oldest_committed_pending_micros, 0);
    let lag = backlog.lag();
    assert_eq!(lag.count(), 2, "exactly the two observed commits are timed");
    assert_eq!(
        lag.count(),
        settled.outcomes.acknowledged - settled.outcomes.acknowledged_censored
    );
}

#[test]
fn aborts_and_reconciled_absences_are_not_acknowledgements() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    backlog
        .reserve(&[charge(target, 1, 1, 10)], &[])
        .unwrap()
        .aborted();
    let mut lost = backlog.reserve(&[charge(target, 2, 2, 10)], &[]).unwrap();
    lost.begin_commit();
    lost.uncertain();
    let ticket = backlog.begin_reconciliation();
    assert_eq!(backlog.finish_reconciliation(ticket, target, []), 1);
    let totals = backlog.totals();
    assert_eq!(totals.outcomes, LedgerOutcomes::default());
    assert_eq!(totals.usage, BacklogUsage::default());
    assert_eq!(backlog.lag().count(), 0);
}

#[test]
fn oldest_pending_age_tracks_only_observed_commits() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    backlog.load_durable(target, [(id(1), entity(1), 10)]);
    assert_eq!(
        backlog.totals().oldest_committed_pending_micros,
        0,
        "discovered work has no known commit instant"
    );
    backlog
        .reserve(&[charge(target, 2, 2, 10)], &[])
        .unwrap()
        .committed();
    std::thread::sleep(std::time::Duration::from_millis(5));
    backlog
        .reserve(&[charge(target, 3, 3, 10)], &[])
        .unwrap()
        .committed();
    let oldest = backlog.totals().oldest_committed_pending_micros;
    assert!(oldest >= 5_000, "the older commit sets the age: {oldest}");
    // An unknown acknowledgement outcome keeps the operation pending: with
    // operation 3 acknowledged, only operation 2's commit sets the age.
    backlog.mark_acknowledgement_uncertain([id(1), id(2)]);
    backlog.acknowledge([id(3)]);
    assert!(backlog.totals().oldest_committed_pending_micros >= oldest);
    // Discovered operation 1 remains pending but has no commit instant.
    backlog.acknowledge([id(2)]);
    assert_eq!(backlog.totals().oldest_committed_pending_micros, 0);
    assert_eq!(backlog.totals().usage.operations, 1);
}

#[test]
fn reconciliation_visits_only_the_target_uncertain_charges() {
    // Uncertainty checks and reconciliation run under the ledger lock that
    // foreground commits share, so neither may scale with other targets'
    // retained work. Reconciliation reads the target's queue once and then
    // settles only the target's uncertain charges, never its durable ones.
    let backlog = ledger(u64::MAX, u64::MAX);
    backlog.load_durable(
        target(2, 1),
        (0..1_000_u64).map(|operation| (id(u128::from(operation) + 1_000), entity(operation), 10)),
    );
    backlog.load_durable(target(1, 1), [(id(10), entity(10), 10)]);
    for operation in 1..=2 {
        let mut uncertain = backlog
            .reserve(
                &[charge(target(1, 1), operation, u128::from(operation), 10)],
                &[],
            )
            .unwrap();
        uncertain.begin_commit();
        uncertain.uncertain();
    }
    let visits = || backlog.state.lock().reconciliation_visits;
    assert!(backlog.has_uncertain(target(1, 1)));
    assert!(!backlog.has_uncertain(target(2, 1)));
    assert_eq!(
        backlog.uncertain_targets().into_iter().collect::<Vec<_>>(),
        vec![target(1, 1)]
    );
    let ticket = backlog.begin_reconciliation();
    assert_eq!(backlog.finish_reconciliation(ticket, target(3, 1), []), 0);
    assert_eq!(
        visits(),
        0,
        "a target without uncertain charges visits none"
    );
    let ticket = backlog.begin_reconciliation();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target(1, 1), [(id(10), entity(10), 10)]),
        2
    );
    assert_eq!(visits(), 2, "only the target's two uncertain charges");
    assert_eq!(usage(&backlog, 1).operations, 1);
    assert_eq!(usage(&backlog, 2).operations, 1_000);
}

#[test]
fn presence_proves_an_enqueue_durable_whenever_it_was_marked() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    backlog
        .reserve(&[charge(target, 2, 2, 10)], &[])
        .unwrap()
        .committed();
    let ticket = backlog.begin_reconciliation();
    // Both outcomes became unknown after the reconciliation began.
    let mut late = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
    late.begin_commit();
    late.uncertain();
    backlog.mark_acknowledgement_uncertain([id(2)]);
    let read = [(id(1), entity(1), 10), (id(2), entity(2), 10)];
    assert_eq!(backlog.finish_reconciliation(ticket, target, read), 0);
    // The read holds the enqueue, so it committed; the acknowledgement may
    // still commit after the read.
    assert_eq!(usage(&backlog, 1).uncertain_operations, 1);
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 1,
            discovered: 1,
            ..LedgerOutcomes::default()
        }
    );
    // Absent from a later flushed read, the acknowledgement committed.
    let ticket = backlog.begin_reconciliation();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target, [(id(1), entity(1), 10)]),
        1
    );
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 1,
            discovered: 1,
            acknowledged: 1,
            acknowledged_censored: 0,
        }
    );
    assert_eq!(backlog.lag().count(), 1, "the observed commit is timed");
}

#[test]
fn a_transaction_over_a_limit_on_its_own_is_a_hard_batch_error() {
    // Three distinct members can never fit a limit of two, even when empty.
    let backlog = ledger(1_000, 2);
    let error = backlog
        .reserve(
            &[
                charge(target(1, 1), 1, 1, 10),
                charge(target(1, 1), 2, 2, 10),
                charge(target(1, 1), 3, 3, 10),
            ],
            &[],
        )
        .expect_err("the transaction alone exceeds the member limit");
    assert!(matches!(
        error,
        HelixDbError::IndexOperationBatchTooLarge {
            index_id: 1,
            resource: IndexOperationBatchResource::PendingMembers,
            observed: 3,
            limit: 2,
        }
    ));
    assert!(!error.is_index_backpressure(), "{error}");
    assert!(error.is_invalid_input(), "{error}");
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
    // Repeated operations for one member count once.
    backlog
        .reserve(
            &[
                charge(target(1, 1), 1, 1, 10),
                charge(target(1, 1), 1, 2, 10),
                charge(target(1, 1), 2, 3, 10),
            ],
            &[],
        )
        .expect("two members reach the limit exactly")
        .committed();
    // Bytes alone above the limit are just as permanent.
    assert!(matches!(
        ledger(15, 10).reserve(
            &[charge(target(1, 1), 1, 1, 8), charge(target(1, 1), 2, 2, 8),],
            &[]
        ),
        Err(HelixDbError::IndexOperationBatchTooLarge {
            resource: IndexOperationBatchResource::RetainedBytes,
            observed: 16,
            limit: 15,
            ..
        })
    ));
}

#[test]
fn a_transaction_staging_two_generations_of_one_index_is_an_invariant_violation() {
    // A transaction routes through one catalog snapshot, which holds one
    // record, and so one generation, per logical index.
    let backlog = ledger(u64::MAX, u64::MAX);
    let error = backlog
        .reserve(
            &[
                charge(target(1, 1), 1, 1, 10),
                charge(target(2, 1), 1, 2, 10),
                charge(target(1, 2), 1, 3, 10),
            ],
            &[],
        )
        .expect_err("index 1 is staged in two generations");
    assert!(
        matches!(error, HelixDbError::InvariantViolation(_)),
        "{error}"
    );
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
    assert_eq!(usage(&backlog, 2), BacklogUsage::default());
    // The violation is reported before any limit is considered.
    assert!(matches!(
        ledger(1, 1).reserve(
            &[
                charge(target(1, 1), 1, 1, 10),
                charge(target(1, 2), 2, 2, 10),
            ],
            &[]
        ),
        Err(HelixDbError::InvariantViolation(_))
    ));
}

#[test]
fn a_hard_batch_error_outranks_backpressure_on_another_index() {
    let backlog = ledger(100, 2);
    backlog
        .reserve(&[charge(target(1, 1), 1, 1, 95)], &[])
        .unwrap()
        .committed();
    // Index 1 is saturated (retryable) but index 2 can never admit three
    // members, so retrying the transaction unchanged cannot succeed.
    let error = backlog
        .reserve(
            &[
                charge(target(1, 1), 2, 2, 10),
                charge(target(2, 1), 1, 3, 1),
                charge(target(2, 1), 2, 4, 1),
                charge(target(2, 1), 3, 5, 1),
            ],
            &[],
        )
        .expect_err("rejected");
    assert!(matches!(
        error,
        HelixDbError::IndexOperationBatchTooLarge { index_id: 2, .. }
    ));
    // Within its own limits, a transaction over the combined limit is
    // backpressure: retained work plus its own exceeds the ceiling.
    assert!(backlog
        .reserve(&[charge(target(1, 1), 2, 2, 10)], &[])
        .unwrap_err()
        .is_index_backpressure());
    assert_eq!(usage(&backlog, 1).retained_bytes, 95);
    assert_eq!(usage(&backlog, 2), BacklogUsage::default());
}

#[test]
fn uncertain_acknowledgements_keep_outcomes_exact() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    backlog
        .reserve(&[charge(target, 1, 1, 10), charge(target, 2, 2, 10)], &[])
        .unwrap()
        .committed();
    backlog.load_durable(target, [(id(3), entity(3), 10)]);
    // One publication's acknowledgement outcome is unknown, then a retry's
    // too; an ID the ledger no longer retains is ignored.
    backlog.mark_acknowledgement_uncertain([id(1), id(2), id(3)]);
    backlog.mark_acknowledgement_uncertain([id(1), id(99)]);
    assert!(backlog.has_uncertain(target));
    assert_eq!(backlog.totals().usage.uncertain_operations, 3);
    // A flushed read proves the acknowledgements of 1 and 3 committed and
    // that of 2 did not.
    let ticket = backlog.begin_reconciliation();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target, [(id(2), entity(2), 10)]),
        2
    );
    assert!(!backlog.has_uncertain(target));
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 2,
            discovered: 1,
            acknowledged: 2,
            acknowledged_censored: 1,
        },
        "a still-pending operation is not rediscovered and a committed \
         acknowledgement is an acknowledgement"
    );
    backlog.acknowledge([id(2)]);
    let settled = backlog.totals();
    assert_eq!(
        settled.outcomes,
        LedgerOutcomes {
            committed: 2,
            discovered: 1,
            acknowledged: 3,
            acknowledged_censored: 1,
        }
    );
    assert_eq!(settled.usage, BacklogUsage::default());
    assert_eq!(
        backlog.lag().count(),
        2,
        "both commits observed here keep their measured lag"
    );
}

#[test]
fn an_uncertain_acknowledgement_racing_the_producer_commit_stays_uncertain() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    let mut racing = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
    racing.begin_commit();
    // Publication read the committed operation before its producer saw the
    // commit return, and its acknowledgement outcome is unknown.
    backlog.mark_acknowledgement_uncertain([id(1)]);
    racing.committed();
    assert!(
        backlog.has_uncertain(target),
        "the unknown acknowledgement still needs reconciliation"
    );
    let ticket = backlog.begin_reconciliation();
    assert_eq!(backlog.finish_reconciliation(ticket, target, []), 1);
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 1,
            discovered: 0,
            acknowledged: 1,
            acknowledged_censored: 1,
        }
    );
}

#[test]
fn acknowledging_an_uncertain_enqueue_proves_it_durable() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    let mut uncertain = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
    uncertain.begin_commit();
    uncertain.uncertain();
    // Publication read and acknowledged it before any reconciliation.
    backlog.acknowledge([id(1)]);
    assert!(!backlog.has_uncertain(target));
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 0,
            discovered: 1,
            acknowledged: 1,
            acknowledged_censored: 1,
        }
    );
}

#[test]
fn an_acknowledgement_before_an_uncertain_producer_outcome_counts_one_discovery() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    let mut racing = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
    racing.begin_commit();
    // Publication acknowledged the operation before its producer's commit
    // returned without an outcome.
    backlog.acknowledge([id(1)]);
    racing.uncertain();
    let totals = backlog.totals();
    assert_eq!(
        totals.outcomes,
        LedgerOutcomes {
            committed: 0,
            discovered: 1,
            acknowledged: 1,
            acknowledged_censored: 1,
        }
    );
    assert_eq!(totals.usage, BacklogUsage::default());
    assert!(!backlog.has_uncertain(target));
}

#[test]
fn an_uncertain_producer_after_an_uncertain_acknowledgement_counts_one_discovery() {
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    let mut racing = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
    racing.begin_commit();
    // Publication read the operation and left its acknowledgement uncertain
    // before the producer's commit returned without an outcome.
    backlog.mark_acknowledgement_uncertain([id(1)]);
    racing.uncertain();
    let discovered = LedgerOutcomes {
        discovered: 1,
        ..LedgerOutcomes::default()
    };
    assert_eq!(backlog.totals().outcomes, discovered);
    // A flushed read still holds it: that acknowledgement did not commit.
    let ticket = backlog.begin_reconciliation();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target, vec![(id(1), entity(1), 10)]),
        0
    );
    assert_eq!(
        usage(&backlog, 1),
        BacklogUsage {
            retained_bytes: 10,
            members: 1,
            operations: 1,
            uncertain_operations: 0,
        }
    );
    assert_eq!(
        backlog.totals().outcomes,
        discovered,
        "a pending operation is not rediscovered"
    );
    // The next acknowledgement is uncertain too; a flushed read proves it
    // committed.
    backlog.mark_acknowledgement_uncertain([id(1)]);
    let ticket = backlog.begin_reconciliation();
    assert_eq!(backlog.finish_reconciliation(ticket, target, Vec::new()), 1);
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 0,
            discovered: 1,
            acknowledged: 1,
            acknowledged_censored: 1,
        }
    );
    assert_eq!(usage(&backlog, 1), BacklogUsage::default());
}

#[test]
fn a_producer_outcome_after_a_reconciled_acknowledgement_counts_once() {
    for committed in [true, false] {
        let backlog = ledger(1_000, 1_000);
        let target = target(1, 1);
        let mut racing = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
        racing.begin_commit();
        backlog.mark_acknowledgement_uncertain([id(1)]);
        let ticket = backlog.begin_reconciliation();
        assert_eq!(
            backlog.finish_reconciliation(ticket, target, vec![(id(1), entity(1), 10)]),
            0
        );
        assert_eq!(backlog.totals().outcomes, LedgerOutcomes::default());
        if committed {
            racing.committed();
        } else {
            racing.uncertain();
        }
        assert!(!backlog.has_uncertain(target));
        assert_eq!(usage(&backlog, 1).operations, 1);
        backlog.acknowledge([id(1)]);
        assert_eq!(
            backlog.totals().outcomes,
            LedgerOutcomes {
                committed: u64::from(committed),
                discovered: u64::from(!committed),
                acknowledged: 1,
                acknowledged_censored: 1,
            },
            "publication attempted its acknowledgement before the producer saw the commit return"
        );
        assert_eq!(backlog.lag().count(), 0);
        assert_eq!(usage(&backlog, 1), BacklogUsage::default());
    }
}

#[test]
fn a_read_before_the_producer_returns_leaves_the_lag_timed() {
    // Only an acknowledgement, committed or attempted, before the producer's
    // commit returns censors the lag; reading the operation first does not.
    let backlog = ledger(1_000, 1_000);
    let target = target(1, 1);
    let mut racing = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
    racing.begin_commit();
    let ticket = backlog.begin_reconciliation();
    assert_eq!(
        backlog.finish_reconciliation(ticket, target, [(id(1), entity(1), 10)]),
        0
    );
    racing.committed();
    backlog.acknowledge([id(1)]);
    assert_eq!(
        backlog.totals().outcomes,
        LedgerOutcomes {
            committed: 1,
            discovered: 0,
            acknowledged: 1,
            acknowledged_censored: 0,
        }
    );
    assert_eq!(backlog.lag().count(), 1);
}

#[test]
fn an_uncertain_acknowledgement_of_an_uncertain_enqueue_counts_one_discovery() {
    for present in [false, true] {
        let backlog = ledger(1_000, 1_000);
        let target = target(1, 1);
        let mut uncertain = backlog.reserve(&[charge(target, 1, 1, 10)], &[]).unwrap();
        uncertain.begin_commit();
        uncertain.uncertain();
        // Publication read it, proving the enqueue durable, and left its
        // acknowledgement uncertain.
        backlog.mark_acknowledgement_uncertain([id(1)]);
        assert_eq!(
            backlog.totals().outcomes,
            LedgerOutcomes {
                discovered: 1,
                ..LedgerOutcomes::default()
            }
        );
        let ticket = backlog.begin_reconciliation();
        let read = if present {
            vec![(id(1), entity(1), 10)]
        } else {
            Vec::new()
        };
        assert_eq!(
            backlog.finish_reconciliation(ticket, target, read),
            u64::from(!present)
        );
        assert_eq!(
            backlog.totals().outcomes,
            LedgerOutcomes {
                committed: 0,
                discovered: 1,
                acknowledged: u64::from(!present),
                acknowledged_censored: u64::from(!present),
            }
        );
        assert_eq!(
            usage(&backlog, 1),
            BacklogUsage {
                retained_bytes: 10 * u64::from(present),
                members: u64::from(present),
                operations: u64::from(present),
                uncertain_operations: 0,
            }
        );
    }
}
