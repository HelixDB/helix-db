//! Contracts of the registry: which entries a compaction job may discard,
//! and what a watched reclaim proves.

use super::*;
use crate::encoding::v2::keys::scope::TenantId;

const SCOPE: DataScope = DataScope::Tenant(TenantId::from_u128(7));

fn entry(index_id: u64) -> DroppedGeneration {
    DroppedGeneration {
        index_id: IndexId::new(index_id).unwrap(),
        generation: IndexGenerationId::initial(),
    }
}

fn owner(index_id: u64) -> OwnerKey {
    OwnerKey::Generation {
        scope: SCOPE,
        index_id,
        generation: IndexGenerationId::initial().get(),
    }
}

/// A registry that armed each `(index, gate)` as a secondary generation.
fn registry(armed: &[(u64, u64)]) -> Registry {
    Registry {
        state: parking_lot::Mutex::new(State::Filtering {
            armed: armed
                .iter()
                .map(|(index_id, gate)| {
                    (
                        entry(*index_id),
                        Armed {
                            gate: *gate,
                            value: DroppedGenerationValue {
                                scope: SCOPE,
                                rows: DroppedGenerationRows::Secondary,
                            },
                            owners: vec![owner(*index_id)],
                        },
                    )
                })
                .collect(),
            watch: None,
        }),
    }
}

fn job(destination: u32, retention_min_seq: Option<u64>) -> CompactionJobContext {
    CompactionJobContext {
        destination,
        is_dest_last_run: true,
        compaction_clock_tick: 0,
        retention_min_seq,
    }
}

#[test]
fn a_job_discards_an_entry_only_once_its_retention_reaches_the_gate() {
    let registry = registry(&[(1, 10), (2, 20)]);
    for (retention_min_seq, discarded) in [
        // A job that reports no retention sequence proves nothing.
        (None, vec![]),
        (Some(0), vec![]),
        (Some(9), vec![]),
        (Some(10), vec![owner(1)]),
        (Some(19), vec![owner(1)]),
        (Some(20), vec![owner(1), owner(2)]),
        (Some(u64::MAX), vec![owner(1), owner(2)]),
    ] {
        assert_eq!(
            registry.discardable(&job(0, retention_min_seq)),
            HashSet::from_iter(discarded),
            "retention {retention_min_seq:?}"
        );
    }
}

#[test]
fn an_entry_gives_up_every_owner_it_was_armed_with() {
    let physical = OwnerKey::PhysicalIndex {
        scope: SCOPE,
        physical_index_id: 4,
    };
    let registry = Registry {
        state: parking_lot::Mutex::new(State::Filtering {
            armed: HashMap::from([(
                entry(1),
                Armed {
                    gate: 5,
                    value: DroppedGenerationValue {
                        scope: SCOPE,
                        rows: DroppedGenerationRows::UnpartitionedVector {
                            physical_index_id: VectorPhysicalIndexId::new(4).unwrap(),
                        },
                    },
                    owners: vec![owner(1), physical],
                },
            )]),
            watch: None,
        }),
    };
    assert_eq!(
        registry.discardable(&job(0, Some(5))),
        HashSet::from([owner(1), physical])
    );
}

#[test]
fn a_watch_proves_only_what_every_filter_of_its_destination_discarded() {
    let registry = registry(&[(1, 10), (2, 20)]);
    // Nothing is watched, so nothing is proven.
    registry.discardable(&job(3, Some(30)));
    assert_eq!(registry.unwatch(), HashSet::new());

    // A watch no filter was created for proves nothing.
    registry.watch(3);
    assert_eq!(registry.unwatch(), HashSet::new());

    // Filters of other destinations are not the watched compaction's.
    registry.watch(3);
    registry.discardable(&job(2, Some(30)));
    assert_eq!(registry.unwatch(), HashSet::new());

    registry.watch(3);
    registry.discardable(&job(3, Some(30)));
    registry.discardable(&job(3, Some(30)));
    assert_eq!(registry.unwatch(), HashSet::from([entry(1), entry(2)]));
    // The watch ended with the first unwatch.
    registry.discardable(&job(3, Some(30)));
    assert_eq!(registry.unwatch(), HashSet::new());

    // One filter that could not discard an entry withdraws its proof.
    registry.watch(3);
    registry.discardable(&job(3, Some(30)));
    registry.discardable(&job(3, Some(15)));
    assert_eq!(registry.unwatch(), HashSet::from([entry(1)]));
    registry.watch(3);
    registry.discardable(&job(3, Some(30)));
    registry.discardable(&job(3, None));
    assert_eq!(registry.unwatch(), HashSet::new());

    // A new watch forgets the filters of the one it replaces.
    registry.watch(3);
    registry.discardable(&job(3, Some(30)));
    registry.watch(4);
    registry.discardable(&job(4, Some(15)));
    assert_eq!(registry.unwatch(), HashSet::from([entry(1)]));
}

#[test]
fn disarming_forgets_only_the_named_entries() {
    let registry = registry(&[(1, 10), (2, 20)]);
    registry.disarm(&HashSet::from([entry(1), entry(9)]));
    assert_eq!(
        registry
            .armed()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry(2)]
    );
    assert_eq!(
        registry.discardable(&job(0, Some(u64::MAX))),
        HashSet::from([owner(2)])
    );
}

#[test]
fn a_halted_registry_discards_and_proves_nothing() {
    let registry = registry(&[(1, 10)]);
    *registry.state.lock() = State::Halted;
    assert_eq!(registry.armed(), Vec::new());
    registry.watch(0);
    assert_eq!(
        registry.discardable(&job(0, Some(u64::MAX))),
        HashSet::new()
    );
    assert_eq!(registry.unwatch(), HashSet::new());
    registry.disarm(&HashSet::from([entry(1)]));
    assert!(matches!(&*registry.state.lock(), State::Halted));
}
