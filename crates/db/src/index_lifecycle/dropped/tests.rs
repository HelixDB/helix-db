//! Contracts of the registry and of the cross-check's verdict: which entries
//! a compaction job may discard, what restarts a proof, when a proof step
//! applies, and what contradicts an entry.

use super::*;
use crate::encoding::v2::keys::scope::TenantId;

const SCOPE: DataScope = DataScope::Tenant(TenantId::from_u128(7));

fn entry(index_id: u64) -> DroppedGeneration {
    generation(index_id, 1)
}

fn generation(index_id: u64, generation: u64) -> DroppedGeneration {
    DroppedGeneration {
        index_id: IndexId::new(index_id).unwrap(),
        generation: IndexGenerationId::new(generation).unwrap(),
    }
}

fn owner(index_id: u64) -> OwnerKey {
    OwnerKey::Generation {
        scope: SCOPE,
        index_id,
        generation: IndexGenerationId::initial().get(),
    }
}

fn physical(id: u64) -> VectorPhysicalIndexId {
    VectorPhysicalIndexId::new(id).unwrap()
}

const SECONDARY: DroppedGenerationValue = DroppedGenerationValue {
    scope: SCOPE,
    rows: DroppedGenerationRows::Secondary,
};

/// When every fixture entry was armed or quarantined.
static SINCE: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

fn armed(index_id: u64, gate: u64) -> Armed {
    Armed {
        gate,
        value: SECONDARY,
        owners: vec![owner(index_id)],
        kept: 0,
        proof: Proof::Unproven,
        since: *SINCE,
        progressed: *SINCE,
    }
}

fn quarantined(contradiction: Contradiction) -> Entry {
    Entry::Quarantined(Quarantined {
        value: SECONDARY,
        contradiction,
        since: *SINCE,
    })
}

fn registry_of(entries: HashMap<DroppedGeneration, Entry>) -> Registry {
    Registry {
        state: parking_lot::Mutex::new(State {
            tracking: entries.values().filter(|entry| entry.tracks()).count(),
            entries,
            compactions: CompactionActivity::default(),
            last_pass: None,
            compactor: DroppedStorageCompactor::default(),
        }),
        changed: tokio::sync::Notify::new(),
    }
}

/// A registry that armed each `(index, gate)` as a secondary generation.
fn registry(entries: &[(u64, u64)]) -> Registry {
    registry_of(
        entries
            .iter()
            .map(|(index_id, gate)| (entry(*index_id), Entry::Armed(armed(*index_id, *gate))))
            .collect(),
    )
}

/// Whether the registry has told its retirer of a change since last asked.
fn told(registry: &Registry) -> bool {
    futures::FutureExt::now_or_never(registry.changed()).is_some()
}

fn job(retention_min_seq: Option<u64>) -> CompactionJobContext {
    CompactionJobContext {
        destination: 0,
        is_dest_last_run: true,
        compaction_clock_tick: 0,
        retention_min_seq,
    }
}

/// The armed entry of `index_id`, with the time its proof last moved put
/// back to the fixture's, which
/// [`a_proof_that_moves_restarts_the_time_it_has_stood_still`] tests.
fn state(registry: &Registry, index_id: u64) -> Armed {
    Armed {
        progressed: *SINCE,
        ..progress(registry, index_id)
    }
}

/// Lets the clock move on, so that two instants read either side of it
/// differ.
fn tick() {
    std::thread::sleep(std::time::Duration::from_millis(2));
}

/// The armed entry of `index_id` as the registry holds it.
fn progress(registry: &Registry, index_id: u64) -> Armed {
    registry
        .armed()
        .into_iter()
        .find(|(armed, _)| *armed == entry(index_id))
        .expect("the entry is armed")
        .1
}

/// A proof that tracks `tables`, with a hundred bytes of the entry's rows
/// in table 1, two hundred in table 2, and so on.
fn tracking(tables: &[u128]) -> Proof {
    Proof::Tracking {
        tables: Tracked::new(
            tables
                .iter()
                .map(|table| (TableId(*table), *table as u64 * 100))
                .collect(),
        )
        .expect("a fixture proof tracks a table"),
    }
}

fn draining(compactions: &[u128]) -> Proof {
    Proof::Draining {
        compactions: Waiting::new(compactions.iter().copied().map(CompactionId).collect())
            .expect("a fixture proof waits for a compaction"),
    }
}

#[test]
fn a_proof_never_waits_for_nothing() {
    assert_eq!(Waiting::<TableId>::new(HashSet::new()), None);
    let one = Waiting::new(HashSet::from([TableId(7)])).expect("one member is enough");
    assert_eq!(one.members(), &HashSet::from([TableId(7)]));
    assert_eq!(Tracked::new(HashMap::new()), None);
    let one = Tracked::new(HashMap::from([(TableId(7), 0)])).expect("one table is enough");
    assert_eq!(one.tables(), &HashMap::from([(TableId(7), 0)]));
}

#[test]
fn a_proof_goes_on_tracking_the_tables_that_are_still_stored() {
    let Proof::Tracking { tables } = tracking(&[1, 2, 3]) else {
        unreachable!("the fixture tracks tables");
    };
    let stored = |ids: &[u128]| ids.iter().copied().map(TableId).collect::<HashSet<_>>();
    // Each table that stays keeps its own estimate; a table that was never
    // tracked is not picked up.
    assert_eq!(
        tables
            .still(&stored(&[2, 3, 4]))
            .map(|left| left.tables().clone()),
        Some(HashMap::from([(TableId(2), 200), (TableId(3), 300)]))
    );
    assert_eq!(tables.still(&stored(&[1, 2, 3])), Some(tables.clone()));
    assert_eq!(tables.still(&stored(&[4])), None);
    assert_eq!(tables.still(&stored(&[])), None);
}

#[test]
fn the_rows_a_compaction_would_discard_are_those_of_tracked_entries_past_their_gate() {
    let registry = registry(&[(1, 10), (2, 10), (3, 20), (4, 10)]);
    assert!(registry.advance(entry(1), 0, tracking(&[7, 8])));
    assert!(registry.advance(entry(2), 0, tracking(&[8])));
    assert!(registry.advance(entry(3), 0, tracking(&[8, 9])));
    assert!(registry.advance(entry(4), 0, draining(&[5])));
    // Below every gate nothing is discardable.
    assert_eq!(registry.discardable_tracked(9), HashMap::new());
    // At a gate its entries count, and a table two of them track counts
    // the rows of both. An entry that tracks no table yet adds nothing.
    assert_eq!(
        registry.discardable_tracked(10),
        HashMap::from([(TableId(7), 700), (TableId(8), 1600)])
    );
    assert_eq!(
        registry.discardable_tracked(19),
        HashMap::from([(TableId(7), 700), (TableId(8), 1600)])
    );
    assert_eq!(
        registry.discardable_tracked(20),
        HashMap::from([(TableId(7), 700), (TableId(8), 2400), (TableId(9), 900)])
    );
}

#[test]
fn a_job_discards_an_entry_only_once_its_retention_reaches_the_gate() {
    for (retention_min_seq, discarded) in [
        // A job that reports no retention sequence discards nothing.
        (None, vec![]),
        (Some(0), vec![]),
        (Some(9), vec![]),
        (Some(10), vec![owner(1)]),
        (Some(19), vec![owner(1)]),
        (Some(20), vec![owner(1), owner(2)]),
        (Some(u64::MAX), vec![owner(1), owner(2)]),
    ] {
        assert_eq!(
            registry(&[(1, 10), (2, 20)]).discardable(&job(retention_min_seq)),
            HashSet::from_iter(discarded),
            "retention {retention_min_seq:?}"
        );
    }
}

#[test]
fn an_entry_gives_up_every_owner_it_was_armed_with() {
    let namespace = OwnerKey::PhysicalIndex {
        scope: SCOPE,
        physical_index_id: 4,
    };
    let registry = registry_of(HashMap::from([(
        entry(1),
        Entry::Armed(Armed {
            value: DroppedGenerationValue {
                scope: SCOPE,
                rows: DroppedGenerationRows::UnpartitionedVector {
                    physical_index_id: physical(4),
                },
            },
            owners: vec![owner(1), namespace],
            ..armed(1, 5)
        }),
    )]));
    assert_eq!(
        registry.discardable(&job(Some(5))),
        HashSet::from([owner(1), namespace])
    );
}

#[test]
fn a_filter_that_keeps_an_entry_restarts_only_that_entrys_proof() {
    let registry = registry(&[(1, 10), (2, 20)]);
    assert!(registry.advance(entry(1), 0, tracking(&[7])));
    assert!(registry.advance(entry(2), 0, draining(&[8])));

    // A filter that discards both leaves both proofs alone.
    registry.discardable(&job(Some(20)));
    assert_eq!(
        state(&registry, 1),
        Armed {
            proof: tracking(&[7]),
            ..armed(1, 10)
        }
    );
    assert_eq!(
        state(&registry, 2),
        Armed {
            proof: draining(&[8]),
            ..armed(2, 20)
        }
    );

    // One that keeps the second counts against it alone.
    registry.discardable(&job(Some(19)));
    assert_eq!(
        state(&registry, 1),
        Armed {
            proof: tracking(&[7]),
            ..armed(1, 10)
        }
    );
    assert_eq!(
        state(&registry, 2),
        Armed {
            kept: 1,
            ..armed(2, 20)
        }
    );

    // One that reports no retention keeps both, and counts again.
    registry.discardable(&job(None));
    assert_eq!(
        state(&registry, 1),
        Armed {
            kept: 1,
            ..armed(1, 10)
        }
    );
    assert_eq!(
        state(&registry, 2),
        Armed {
            kept: 2,
            ..armed(2, 20)
        }
    );
}

#[test]
fn a_proof_step_applies_only_while_no_filter_has_kept_the_entry_since() {
    let registry = registry(&[(1, 10)]);
    assert!(registry.advance(entry(1), 0, draining(&[8])));
    registry.discardable(&job(Some(9)));
    // The step was decided before the filter kept the rows.
    assert!(!registry.advance(entry(1), 0, tracking(&[7])));
    assert_eq!(
        state(&registry, 1),
        Armed {
            kept: 1,
            ..armed(1, 10)
        }
    );
    assert!(!registry.retire(entry(1), 0));
    assert_eq!(registry.armed().len(), 1);
    // A step decided after it applies.
    assert!(registry.advance(entry(1), 1, tracking(&[7])));
    assert_eq!(state(&registry, 1).proof, tracking(&[7]));
    // A count ahead of the entry's is as stale as one behind it.
    assert!(!registry.advance(entry(1), 2, Proof::Unproven));
    assert!(!registry.retire(entry(1), 2));
    assert!(registry.retire(entry(1), 1));
    assert_eq!(registry.armed(), Vec::new());
    assert_eq!(registry.discardable(&job(Some(u64::MAX))), HashSet::new());
}

/// What a writer's compactor does for dropped generations follows from how
/// many compactions it runs at a time in its process, and the registry
/// reports what the open told it.
#[test]
fn a_compactor_reclaims_only_with_a_slot_to_spare_and_arms_only_with_a_worker() {
    for (slots, compactor) in [
        (None, DroppedStorageCompactor::None),
        (Some(0), DroppedStorageCompactor::SingleSlot),
        (Some(1), DroppedStorageCompactor::SingleSlot),
        (Some(2), DroppedStorageCompactor::Reclaims),
        (Some(4), DroppedStorageCompactor::Reclaims),
    ] {
        assert_eq!(DroppedStorageCompactor::of(slots), compactor, "{slots:?}");
        let registry = registry(&[(1, 10)]);
        assert_eq!(
            registry.storage().compactor,
            DroppedStorageCompactor::Reclaims
        );
        registry.compacts(compactor);
        assert_eq!(registry.storage().compactor, compactor, "{slots:?}");
    }
}

/// The registry answers whether any proof tracks tables from a count it
/// keeps, which every change of a proof moves: a step to tracking and away
/// from it, a filter that keeps the rows, and a retirement.
#[test]
fn the_count_of_tracking_proofs_follows_every_change_of_a_proof() {
    let registry = registry(&[(1, 10), (2, 10), (3, 99)]);
    let tracks = |expected: usize| {
        assert_eq!(registry.tracks_tables(), expected > 0);
        // The report counts by scanning, and asserts the two agree.
        assert_eq!(registry.storage().tracking, expected);
    };
    tracks(0);
    assert!(registry.advance(entry(1), 0, tracking(&[7])));
    assert!(registry.advance(entry(2), 0, tracking(&[7, 8])));
    assert!(registry.advance(entry(3), 0, tracking(&[9])));
    tracks(3);
    // The same proof again, and another set of tables: still three.
    assert!(registry.advance(entry(1), 0, tracking(&[7])));
    assert!(registry.advance(entry(2), 0, tracking(&[8])));
    tracks(3);
    // Back to waiting for compactions.
    assert!(registry.advance(entry(1), 0, Proof::Unproven));
    tracks(2);
    // A filter whose retention is behind entry 3's gate keeps its rows.
    registry.discardable(&job(Some(50)));
    tracks(1);
    assert!(registry.retire(entry(2), 0));
    tracks(0);
    // Retiring an entry that tracks nothing changes nothing.
    assert!(registry.retire(entry(1), 0));
    tracks(0);
}

#[test]
fn a_step_for_an_entry_that_is_not_armed_does_nothing() {
    let registry = registry(&[(1, 10)]);
    assert!(!registry.advance(entry(9), 0, tracking(&[7])));
    assert!(!registry.retire(entry(9), 0));
    registry
        .state
        .lock()
        .entries
        .insert(entry(2), quarantined(Contradiction::AnotherFamily));
    assert!(!registry.advance(entry(2), 0, tracking(&[7])));
    assert!(!registry.retire(entry(2), 0));
    assert_eq!(registry.state.lock().entries.len(), 2);
}

#[test]
fn retiring_forgets_only_the_named_entry() {
    let registry = registry(&[(1, 10), (2, 20)]);
    assert!(registry.retire(entry(1), 0));
    assert_eq!(
        registry
            .armed()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry(2)]
    );
    assert_eq!(
        registry.discardable(&job(Some(u64::MAX))),
        HashSet::from([owner(2)])
    );
}

#[test]
fn a_quarantined_entry_is_never_discarded_and_is_reported() {
    let registry = registry(&[(1, 10)]);
    registry.state.lock().entries.extend([
        (entry(3), quarantined(Contradiction::AnotherFamily)),
        (
            entry(2),
            Entry::Quarantined(Quarantined {
                value: DroppedGenerationValue {
                    scope: DataScope::LegacyUnscoped,
                    rows: DroppedGenerationRows::UnpartitionedVector {
                        physical_index_id: physical(4),
                    },
                },
                contradiction: Contradiction::IndexWithoutRecord,
                since: *SINCE,
            }),
        ),
    ]);
    assert_eq!(
        registry.discardable(&job(Some(u64::MAX))),
        HashSet::from([owner(1)])
    );
    assert_eq!(
        registry
            .armed()
            .into_iter()
            .map(|(entry, _)| entry)
            .collect::<Vec<_>>(),
        vec![entry(1)]
    );
    let storage = registry.storage();
    assert_eq!(
        storage,
        DroppedIndexStorage {
            unproven: 1,
            waiting_since: Some(*SINCE),
            stalled_since: Some(*SINCE),
            quarantined: vec![
                QuarantinedDroppedGeneration {
                    index_id: 2,
                    generation: 1,
                    scope: DataScope::LegacyUnscoped,
                    rows: DroppedEntryRows::UnpartitionedVector {
                        physical_index_id: 4,
                    },
                    contradiction: Contradiction::IndexWithoutRecord,
                    since: *SINCE,
                },
                QuarantinedDroppedGeneration {
                    index_id: 3,
                    generation: 1,
                    scope: SCOPE,
                    rows: DroppedEntryRows::Secondary,
                    contradiction: Contradiction::AnotherFamily,
                    since: *SINCE,
                },
            ],
            ..DroppedIndexStorage::default()
        }
    );
    assert_eq!(storage.entries(), 3);
    // Quarantined entries alone are waited on by nothing.
    assert_eq!(
        registry_of(HashMap::from([(
            entry(3),
            quarantined(Contradiction::AnotherFamily)
        )]))
        .storage()
        .waiting_since,
        None
    );
}

#[test]
fn only_a_quarantined_entry_is_removed_on_request() {
    let registry = registry(&[(1, 10)]);
    registry
        .state
        .lock()
        .entries
        .insert(entry(2), quarantined(Contradiction::GenerationNotReached));
    assert_eq!(registry.remove_quarantined(entry(9)), Unquarantined::Absent);
    // An armed entry stays: its rows are being discarded.
    assert_eq!(registry.remove_quarantined(entry(1)), Unquarantined::Armed);
    assert_eq!(registry.state.lock().entries.len(), 2);
    assert_eq!(
        registry.remove_quarantined(entry(2)),
        Unquarantined::Removed
    );
    assert_eq!(registry.remove_quarantined(entry(2)), Unquarantined::Absent);
    assert_eq!(registry.storage().quarantined, Vec::new());
    assert_eq!(state(&registry, 1), armed(1, 10));
}

#[test]
fn the_registry_counts_the_filters_of_this_process_alive() {
    let registry = registry(&[]);
    assert_eq!(registry.compactions(), CompactionActivity::default());
    registry.discardable(&job(Some(1)));
    registry.discardable(&job(None));
    assert_eq!(
        registry.compactions(),
        CompactionActivity {
            events: 2,
            running: 2
        }
    );
    registry.filter_dropped();
    assert_eq!(
        registry.compactions(),
        CompactionActivity {
            events: 3,
            running: 1
        }
    );
    registry.filter_dropped();
    assert_eq!(
        registry.compactions(),
        CompactionActivity {
            events: 4,
            running: 0
        }
    );
    // With no entry stored, no compaction is worth a look at the store.
    assert!(!told(&registry));
}

#[test]
fn the_retirer_is_told_only_of_what_a_pass_depends_on() {
    let registry = registry(&[(1, 10), (2, 20)]);
    assert!(!told(&registry));

    // A filter that discards every armed entry changes no proof.
    registry.discardable(&job(Some(20)));
    assert!(!told(&registry));
    // One that keeps an entry restarts its proof.
    registry.discardable(&job(Some(19)));
    assert!(told(&registry));
    assert!(!told(&registry));

    // The end of a compaction matters while a proof is not tracking tables.
    registry.filter_dropped();
    assert!(told(&registry));
    assert!(registry.advance(entry(1), 0, draining(&[8])));
    assert!(registry.advance(entry(2), 1, tracking(&[7])));
    registry.filter_dropped();
    assert!(told(&registry));
    // A proof that tracks tables is told by the manifest instead.
    assert!(registry.advance(entry(1), 0, tracking(&[7])));
    registry.discardable(&job(Some(20)));
    registry.filter_dropped();
    assert!(!told(&registry));

    // The last commit of an index operation may have stored an entry.
    registry.operation_committed();
    registry.operation_committed();
    assert!(told(&registry));
    assert!(!told(&registry));

    // A quarantined entry waits for no compaction.
    let quarantined_only = registry_of(HashMap::from([(
        entry(3),
        quarantined(Contradiction::AnotherFamily),
    )]));
    quarantined_only.discardable(&job(None));
    quarantined_only.filter_dropped();
    assert!(!told(&quarantined_only));
}

#[test]
fn a_tracked_table_has_left_once_any_proof_misses_one_from_the_manifest() {
    let registry = registry(&[(1, 10), (2, 10), (3, 10)]);
    let stored = |tables: &[u128]| tables.iter().copied().map(TableId).collect::<HashSet<_>>();
    // Proofs that track nothing yet are not waiting for the manifest.
    assert!(registry.advance(entry(3), 0, draining(&[5])));
    assert!(!registry.tracks_tables());
    assert!(!registry.tracked_table_left(&stored(&[])));

    assert!(registry.advance(entry(1), 0, tracking(&[7, 8])));
    assert!(registry.tracks_tables());
    assert!(registry.advance(entry(2), 0, tracking(&[8, 9])));
    assert!(registry.tracks_tables());
    assert!(!registry.tracked_table_left(&stored(&[7, 8, 9])));
    assert!(!registry.tracked_table_left(&stored(&[6, 7, 8, 9, 10])));
    assert!(registry.tracked_table_left(&stored(&[7, 8])));
    assert!(registry.tracked_table_left(&stored(&[8, 9])));
    assert!(registry.tracked_table_left(&stored(&[])));

    let quarantined_only = registry_of(HashMap::from([(
        entry(3),
        quarantined(Contradiction::AnotherFamily),
    )]));
    assert!(!quarantined_only.tracks_tables());
    assert!(!quarantined_only.tracked_table_left(&stored(&[])));
}

#[test]
fn every_contradiction_and_row_layout_is_reported_in_words() {
    for (contradiction, words) in [
        (
            Contradiction::IndexAboveWatermark,
            "names an index id above the watermark",
        ),
        (
            Contradiction::PhysicalIndexAboveWatermark,
            "names a vector physical index id above the watermark",
        ),
        (
            Contradiction::PhysicalIndexMappedByAnotherGeneration,
            "names a vector physical index another generation's partition maps to",
        ),
        (
            Contradiction::IndexWithoutRecord,
            "names an index its scope has no record of",
        ),
        (
            Contradiction::AnotherFamily,
            "names an index of another family",
        ),
        (
            Contradiction::GenerationStillOwned,
            "names a generation its index record still owns",
        ),
        (
            Contradiction::GenerationNotReached,
            "names a generation its index has not reached",
        ),
        (
            Contradiction::PhysicalIndexStillOwned,
            "names a vector physical index an index record still owns",
        ),
        (
            Contradiction::PhysicalIndexDescribed,
            "names a vector physical index that has a metadata row",
        ),
        (
            Contradiction::MappingOfAnotherGeneration,
            "has a partition mapping of another generation",
        ),
        (
            Contradiction::PhysicalIndexMappedTwice,
            "has two partitions mapped to one vector physical index",
        ),
    ] {
        assert_eq!(contradiction.to_string(), words);
    }
    for (rows, reported, words) in [
        (
            DroppedGenerationRows::Secondary,
            DroppedEntryRows::Secondary,
            "secondary",
        ),
        (DroppedGenerationRows::Text, DroppedEntryRows::Text, "text"),
        (
            DroppedGenerationRows::UnpartitionedVector {
                physical_index_id: physical(4),
            },
            DroppedEntryRows::UnpartitionedVector {
                physical_index_id: 4,
            },
            "vector, physical index 4",
        ),
        (
            DroppedGenerationRows::PartitionedVector,
            DroppedEntryRows::PartitionedVector,
            "partitioned vector",
        ),
    ] {
        assert_eq!(DroppedEntryRows::from(rows), reported);
        assert_eq!(reported.to_string(), words);
    }
}

#[test]
fn storage_counts_each_entry_by_what_its_proof_waits_for() {
    let registry = registry(&[(1, 10), (2, 10), (3, 10), (4, 10), (5, 10), (6, 30)]);
    assert_eq!(DroppedIndexStorage::default().entries(), 0);
    tick();
    assert!(registry.advance(entry(2), 0, draining(&[8, 9])));
    assert!(registry.advance(entry(3), 0, tracking(&[7, 8])));
    assert!(registry.advance(entry(4), 0, tracking(&[8, 9])));
    assert!(registry.advance(entry(5), 0, tracking(&[7])));
    // Keeps the rows of the sixth alone.
    registry.discardable(&job(Some(20)));
    let storage = registry.storage();
    // The proofs that track tables moved when they were advanced above.
    let tracking_since = storage.tracking_since;
    assert!(tracking_since.is_some_and(|moved| moved > *SINCE));
    assert_eq!(
        storage,
        DroppedIndexStorage {
            compactor: DroppedStorageCompactor::Reclaims,
            unproven: 2,
            retained: 1,
            draining: 1,
            tracking: 3,
            tracked_tables: 3,
            // Tables 7 and 8, 8 and 9, and 7: a table two generations track
            // counts the rows of each.
            tracked_bytes: 700 + 800 + 800 + 900 + 700,
            left_in_place: None,
            waiting_since: Some(*SINCE),
            // The unproven entries have not moved since they were armed.
            stalled_since: Some(*SINCE),
            tracking_since,
            last_pass: None,
            quarantined: Vec::new(),
        }
    );
    assert_eq!(storage.entries(), 6);
    // The wait reported is the oldest armed entry's.
    let earlier = SINCE
        .checked_sub(std::time::Duration::from_secs(60))
        .expect("the clock is a minute past its origin");
    let Some(Entry::Armed(armed)) = registry.state.lock().entries.get_mut(&entry(4)).cloned()
    else {
        panic!("the fourth entry is armed");
    };
    registry.state.lock().entries.insert(
        entry(4),
        Entry::Armed(Armed {
            since: earlier,
            ..armed
        }),
    );
    assert_eq!(registry.storage().waiting_since, Some(earlier));
}

/// Facts in which index 1 is a secondary index whose state names generation
/// 3, live or not.
fn facts(live: Option<u64>) -> ScopeFacts {
    ScopeFacts {
        records: HashMap::from([(
            IndexId::new(1).unwrap(),
            RecordFacts {
                family: IndexDefinitionFamily::Secondary,
                generation: IndexGenerationId::new(3).unwrap(),
                live: live.map(|live| IndexGenerationId::new(live).unwrap()),
            },
        )]),
        ..ScopeFacts::default()
    }
}

#[test]
fn an_entry_is_consistent_only_with_a_record_that_has_left_its_generation() {
    for (state, live, dropped, verdict) in [
        // The record moved on: dropped at 3, or building, active at 3.
        (3, None, 1, Ok(())),
        (3, None, 2, Ok(())),
        (3, None, 3, Ok(())),
        (3, Some(3), 1, Ok(())),
        (3, Some(3), 2, Ok(())),
        (3, Some(3), 3, Err(Contradiction::GenerationStillOwned)),
        // A live generation below the dropped one owns it as well: a
        // recreated index builds a strictly later generation.
        (3, Some(2), 2, Err(Contradiction::GenerationStillOwned)),
        (3, Some(2), 3, Err(Contradiction::GenerationStillOwned)),
        (3, None, 4, Err(Contradiction::GenerationNotReached)),
        (3, Some(3), 4, Err(Contradiction::GenerationStillOwned)),
    ] {
        let mut facts = facts(live);
        facts
            .records
            .get_mut(&IndexId::new(1).unwrap())
            .unwrap()
            .generation = IndexGenerationId::new(state).unwrap();
        assert_eq!(
            facts.check(
                generation(1, dropped),
                IndexDefinitionFamily::Secondary,
                &[],
                None
            ),
            verdict,
            "state {state}, live {live:?}, dropped {dropped}"
        );
    }
}

#[test]
fn an_entry_needs_a_record_of_its_own_family() {
    let facts = facts(None);
    assert_eq!(
        facts.check(entry(2), IndexDefinitionFamily::Secondary, &[], None),
        Err(Contradiction::IndexWithoutRecord)
    );
    for family in [IndexDefinitionFamily::Vector, IndexDefinitionFamily::Text] {
        assert_eq!(
            facts.check(entry(1), family, &[], None),
            Err(Contradiction::AnotherFamily),
            "{family:?}"
        );
    }
    assert_eq!(
        facts.check(entry(1), IndexDefinitionFamily::Secondary, &[], None),
        Ok(())
    );
}

#[test]
fn a_vector_entry_names_only_physical_ids_no_live_generation_can_hold() {
    let mut facts = facts(None);
    facts
        .records
        .get_mut(&IndexId::new(1).unwrap())
        .unwrap()
        .family = IndexDefinitionFamily::Vector;
    let check = |facts: &ScopeFacts, ids: &[u64], next: Option<u64>| {
        facts.check(
            entry(1),
            IndexDefinitionFamily::Vector,
            &ids.iter().copied().map(physical).collect::<Vec<_>>(),
            next.map(physical),
        )
    };
    assert_eq!(check(&facts, &[4, 5], Some(6)), Ok(()));
    // The watermark is the next id to allocate, so it is itself unallocated.
    for (ids, next) in [(&[4, 5][..], Some(5)), (&[4, 5], Some(4)), (&[5], None)] {
        assert_eq!(
            check(&facts, ids, next),
            Err(Contradiction::PhysicalIndexAboveWatermark),
            "{ids:?} below {next:?}"
        );
    }
    // No physical id, no watermark to read.
    assert_eq!(check(&facts, &[], None), Ok(()));

    facts.owned.insert(physical(5));
    assert_eq!(
        check(&facts, &[4, 5], Some(6)),
        Err(Contradiction::PhysicalIndexStillOwned)
    );
    assert_eq!(check(&facts, &[4], Some(6)), Ok(()));
    facts.owned.clear();

    // Its own generation's mappings are the entry's; anyone else's are not.
    facts.mapped.insert(physical(5), MappedBy::One(entry(1)));
    assert_eq!(check(&facts, &[4, 5], Some(6)), Ok(()));
    for mapped in [
        MappedBy::One(generation(1, 2)),
        MappedBy::One(entry(2)),
        MappedBy::Several,
    ] {
        facts.mapped.insert(physical(5), mapped);
        assert_eq!(
            check(&facts, &[4, 5], Some(6)),
            Err(Contradiction::PhysicalIndexMappedByAnotherGeneration),
            "{mapped:?}"
        );
        assert_eq!(check(&facts, &[4], Some(6)), Ok(()), "{mapped:?}");
    }
}

#[test]
fn only_a_writer_with_its_own_compaction_worker_compacts_in_process() {
    let settings = |compactor_options| slatedb::Settings {
        compactor_options,
        ..slatedb::Settings::default()
    };
    assert_eq!(compaction_slots(&slatedb::Settings::default()), Some(4));
    assert_eq!(compaction_slots(&settings(None)), None);
    assert_eq!(
        compaction_slots(&settings(Some(slatedb::config::CompactorOptions {
            worker: None,
            ..slatedb::config::CompactorOptions::default()
        }))),
        None
    );
    // The slots are what both the coordinator and the worker allow.
    for (coordinator, worker, slots) in [(6, 2, 2), (2, 6, 2), (3, 3, 3)] {
        assert_eq!(
            compaction_slots(&settings(Some(slatedb::config::CompactorOptions {
                max_concurrent_compactions: coordinator,
                worker: Some(slatedb::config::CompactionWorkerOptions {
                    max_concurrent_compactions: worker,
                    ..slatedb::config::CompactionWorkerOptions::default()
                }),
                ..slatedb::config::CompactorOptions::default()
            }))),
            Some(slots),
            "{coordinator} {worker}"
        );
    }
}

#[test]
fn a_proof_that_moves_restarts_the_time_it_has_stood_still() {
    let registry = registry(&[(1, 10), (2, 10)]);
    assert_eq!(progress(&registry, 1).progressed, *SINCE);
    // A step forward moves it.
    tick();
    assert!(registry.advance(entry(1), 0, draining(&[8])));
    let drained = progress(&registry, 1).progressed;
    assert!(drained > *SINCE);
    // The same proof again is no step: a pass that finds nothing new.
    tick();
    assert!(registry.advance(entry(1), 0, draining(&[8])));
    assert_eq!(progress(&registry, 1).progressed, drained);
    assert!(registry.advance(entry(1), 0, tracking(&[7, 8])));
    let tracked = progress(&registry, 1).progressed;
    assert!(tracked > drained);
    // A tracked table compacted away is progress.
    tick();
    assert!(registry.advance(entry(1), 0, tracking(&[7])));
    let shrunk = progress(&registry, 1).progressed;
    assert!(shrunk > tracked);
    // A step that does not apply moves nothing, and neither does a filter
    // that keeps the rows: that is the opposite of progress.
    tick();
    assert!(!registry.advance(entry(1), 5, tracking(&[9])));
    registry.discardable(&job(Some(9)));
    assert_eq!(progress(&registry, 1).proof, Proof::Unproven);
    assert_eq!(progress(&registry, 1).progressed, shrunk);

    // The report gives the longest standstill among the proofs that have
    // yet to track tables, and among those that do, apart.
    let storage = registry.storage();
    assert_eq!(storage.stalled_since, Some(*SINCE));
    assert_eq!(storage.tracking_since, None);
    assert!(registry.advance(entry(1), 1, tracking(&[7])));
    tick();
    assert!(registry.advance(entry(2), 1, tracking(&[7])));
    let storage = registry.storage();
    assert_eq!(storage.stalled_since, None);
    assert_eq!(
        storage.tracking_since,
        Some(progress(&registry, 1).progressed)
    );
    assert!(progress(&registry, 1).progressed < progress(&registry, 2).progressed);
}

#[test]
fn the_last_pass_is_reported_with_how_long_passes_have_failed() {
    let registry = registry(&[]);
    assert_eq!(registry.storage().last_pass, None);
    registry.pass_ended(None);
    let Some(crate::DroppedStoragePass::Completed { at: first }) = registry.storage().last_pass
    else {
        panic!("the pass completed");
    };
    registry.pass_ended(Some("object store unavailable".to_string()));
    let Some(crate::DroppedStoragePass::Failed { at, since, error }) = registry.storage().last_pass
    else {
        panic!("the pass failed");
    };
    assert!(at >= first);
    assert_eq!(since, at);
    assert_eq!(error, "object store unavailable");
    // A second failure in a row keeps the time of the first and the error
    // of the last.
    registry.pass_ended(Some("timed out".to_string()));
    let Some(crate::DroppedStoragePass::Failed {
        at: again,
        since: still,
        error,
    }) = registry.storage().last_pass
    else {
        panic!("the pass failed again");
    };
    assert!(again >= at);
    assert_eq!(still, since);
    assert_eq!(error, "timed out");
    // A pass that reads the store ends the run of failures; the next
    // failure starts a new one.
    tick();
    registry.pass_ended(None);
    assert!(matches!(
        registry.storage().last_pass,
        Some(crate::DroppedStoragePass::Completed { .. })
    ));
    registry.pass_ended(Some("timed out".to_string()));
    let Some(crate::DroppedStoragePass::Failed { at, since, .. }) = registry.storage().last_pass
    else {
        panic!("the pass failed");
    };
    assert_eq!(since, at);
    assert!(since > still);
}

#[test]
fn the_embedded_compactor_never_moves_tables_without_a_filter() {
    use slatedb::config::CompactorOptions;

    // Whatever the settings ask for: a trivial move creates no filter, so a
    // reclaim made of one would give no byte back, and SlateDB runs one in
    // its coordinator, outside the check that refuses a submitted compaction
    // which overlaps one in flight.
    for enable_trivial_move in [false, true] {
        let settings = slatedb::Settings {
            compactor_options: Some(CompactorOptions {
                enable_trivial_move,
                ..CompactorOptions::default()
            }),
            ..slatedb::Settings::default()
        };
        assert!(
            !compactor_options(&settings)
                .expect("the settings run a compactor")
                .enable_trivial_move
        );
    }
    assert_eq!(
        compactor_options(&slatedb::Settings {
            compactor_options: None,
            ..slatedb::Settings::default()
        })
        .map(|options| options.enable_trivial_move),
        None
    );
}
