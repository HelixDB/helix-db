//! Contracts of the retirement proof's pure steps: what one look at the
//! store makes of a proof, and when a table can hold a row under a prefix.

use super::*;

fn compactions(ids: &[u128]) -> HashSet<CompactionId> {
    ids.iter().copied().map(CompactionId).collect()
}

fn tables(ids: &[u128]) -> HashSet<TableId> {
    ids.iter().copied().map(TableId).collect()
}

fn waiting<T: Eq + std::hash::Hash>(members: HashSet<T>) -> Waiting<T> {
    Waiting::new(members).expect("the fixture waits for something")
}

/// Tracks `ids`, with ten bytes of rows in table 1, twenty in table 2, and
/// so on.
fn tracked(ids: &[u128]) -> Tracked {
    Tracked::new(
        ids.iter()
            .map(|id| (TableId(*id), *id as u64 * 10))
            .collect(),
    )
    .expect("the fixture tracks something")
}

/// What a pass left: this many entries unproven, draining and tracking.
fn left(unproven: usize, draining: usize, tracking: usize) -> DroppedIndexStorage {
    DroppedIndexStorage {
        unproven,
        draining,
        tracking,
        ..DroppedIndexStorage::default()
    }
}

#[test]
fn the_timer_after_a_pass_follows_what_the_entries_wait_for() {
    let now = Instant::now();
    let look = Duration::from_secs(5);
    let idle = Duration::from_secs(300);
    let compactions = CompactionActivity {
        events: 6,
        running: 0,
    };
    let after = |storage: &DroppedIndexStorage, reclaimed: bool| {
        Timer::after_pass(storage, compactions, reclaimed, now, look, idle)
    };
    // Nothing stored, or only quarantined entries: nothing to wait for.
    for reclaimed in [false, true] {
        assert_eq!(after(&left(0, 0, 0), reclaimed), Timer::Off);
        assert_eq!(
            after(
                &DroppedIndexStorage {
                    quarantined: vec![crate::QuarantinedDroppedGeneration {
                        index_id: 1,
                        generation: 1,
                        scope: DataScope::LegacyUnscoped,
                        rows: crate::DroppedEntryRows::Text,
                        contradiction: crate::DroppedEntryContradiction::AnotherFamily,
                        since: std::time::Instant::now(),
                    }],
                    ..DroppedIndexStorage::default()
                },
                reclaimed
            ),
            Timer::Off
        );
        // A proof that waits for compactions is looked at again, whatever
        // else tracks tables and whether or not a reclaim has run.
        for storage in [left(1, 0, 0), left(0, 1, 0), left(1, 1, 3), left(0, 1, 1)] {
            assert_eq!(
                after(&storage, reclaimed),
                Timer::Look { at: now + look },
                "{storage:?}"
            );
        }
    }
    // Every proof tracks tables: a reclaim is due once the store has been
    // idle, measured from the compactions seen before the pass, and at no
    // other time however long the rows have waited.
    assert_eq!(
        after(&left(0, 0, 2), false),
        Timer::Reclaim {
            at: now + idle,
            compactions,
        }
    );
    // One reclaim per idle period.
    assert_eq!(after(&left(0, 0, 2), true), Timer::Off);
}

#[test]
fn a_store_is_idle_only_while_no_compaction_started_ended_or_runs() {
    let seen = CompactionActivity {
        events: 6,
        running: 0,
    };
    // Nothing started or ended, and nothing runs.
    assert!(idle_since(seen, seen));
    for (activity, why) in [
        (
            CompactionActivity {
                events: 7,
                running: 1,
            },
            "a job runs",
        ),
        (
            CompactionActivity {
                events: 8,
                running: 0,
            },
            "a job ran and ended",
        ),
        (
            CompactionActivity {
                events: 5,
                running: 0,
            },
            "the count moved",
        ),
    ] {
        assert!(!idle_since(seen, activity), "{why}");
    }
    // A job that never ended keeps the store busy though no event followed.
    let running = CompactionActivity {
        events: 6,
        running: 1,
    };
    assert!(!idle_since(running, running));
}

#[test]
fn the_shape_of_a_store_moves_whenever_its_tables_do() {
    fn shape(l0: &[u128], runs: &[&[u128]]) -> Shape {
        Shape::of_tables(
            l0.iter().copied().map(TableId),
            runs.iter().map(|run| run.iter().copied().map(TableId)),
        )
    }
    let before = shape(&[3, 2, 1], &[&[20, 21], &[10, 11, 12]]);
    // Read twice with nothing between, or with writes that reach no table.
    assert_eq!(before, shape(&[3, 2, 1], &[&[20, 21], &[10, 11, 12]]));
    for (what, after) in [
        (
            "a flush puts a table at the head of L0",
            shape(&[4, 3, 2, 1], &[&[20, 21], &[10, 11, 12]]),
        ),
        (
            "a compaction takes the oldest L0 tables into a new run",
            shape(&[3], &[&[30], &[20, 21], &[10, 11, 12]]),
        ),
        (
            "a compaction takes them and writes no row",
            shape(&[3], &[&[20, 21], &[10, 11, 12]]),
        ),
        (
            "that, and as many flushes as it took tables",
            shape(&[5, 4, 3], &[&[20, 21], &[10, 11, 12]]),
        ),
        (
            "a run is rewritten into as many tables",
            shape(&[3, 2, 1], &[&[20, 21], &[40, 41, 42]]),
        ),
        (
            "two runs are merged into the older",
            shape(&[3, 2, 1], &[&[50, 51, 52, 53]]),
        ),
        (
            "a run is rewritten and keeps no row",
            shape(&[3, 2, 1], &[&[20, 21], &[]]),
        ),
        (
            "a run is rewritten, keeps no row and is gone",
            shape(&[3, 2, 1], &[&[20, 21]]),
        ),
        (
            "every L0 table is compacted away",
            shape(&[], &[&[60], &[20, 21], &[10, 11, 12]]),
        ),
    ] {
        assert_ne!(before, after, "{what}");
    }
    // A store with no table at all has a shape too.
    assert_eq!(shape(&[], &[]), shape(&[], &[]));
    assert_ne!(shape(&[], &[]), shape(&[1], &[]));
}

#[test]
fn a_timer_fires_when_it_says_unless_it_is_off() {
    let at = Instant::now() + Duration::from_secs(7);
    assert_eq!(Timer::Off.at(), None);
    assert_eq!(Timer::Look { at }.at(), Some(at));
    assert_eq!(Timer::Pass { at }.at(), Some(at));
    assert_eq!(
        Timer::Reclaim {
            at,
            compactions: CompactionActivity::default(),
        }
        .at(),
        Some(at)
    );
}

#[test]
fn an_unproven_entry_waits_for_every_compaction_in_flight() {
    // Whatever tables are stored: none is trusted before the drain.
    for stored in [tables(&[]), tables(&[1, 2])] {
        assert_eq!(
            Proof::Unproven.step(&compactions(&[7, 8]), &stored),
            Step::Drain(waiting(compactions(&[7, 8])))
        );
        assert_eq!(Proof::Unproven.step(&compactions(&[]), &stored), Step::Find);
    }
}

#[test]
fn a_draining_entry_waits_only_for_the_compactions_it_started_with() {
    let proof = Proof::Draining {
        compactions: waiting(compactions(&[7, 8])),
    };
    for (active, step) in [
        (
            compactions(&[7, 8, 9]),
            Step::Drain(waiting(compactions(&[7, 8]))),
        ),
        (
            compactions(&[8, 9]),
            Step::Drain(waiting(compactions(&[8]))),
        ),
        (compactions(&[7]), Step::Drain(waiting(compactions(&[7])))),
        // Compactions created since the proof started are not waited for:
        // the registry saw every filter of theirs.
        (compactions(&[9, 10]), Step::Find),
        (compactions(&[]), Step::Find),
    ] {
        assert_eq!(proof.step(&active, &tables(&[1])), step, "{active:?}");
    }
}

#[test]
fn a_tracked_entry_retires_once_no_suspect_is_stored() {
    let proof = Proof::Tracking {
        tables: tracked(&[1, 2]),
    };
    // Compactions in flight do not hold a tracked entry back: every one
    // created since the drain discards its rows.
    for active in [compactions(&[]), compactions(&[7])] {
        for (stored, step) in [
            (tables(&[1, 2, 3]), Step::Track(tracked(&[1, 2]))),
            (tables(&[2, 3]), Step::Track(tracked(&[2]))),
            (tables(&[1]), Step::Track(tracked(&[1]))),
            (tables(&[3, 4]), Step::Retire),
            (tables(&[]), Step::Retire),
        ] {
            assert_eq!(proof.step(&active, &stored), step, "{stored:?}");
        }
    }
}

fn entry(index_id: u64) -> DroppedGeneration {
    DroppedGeneration {
        index_id: crate::index_lifecycle::IndexId::new(index_id).unwrap(),
        generation: crate::index_lifecycle::IndexGenerationId::initial(),
    }
}

#[test]
fn a_tables_key_range_decides_only_what_it_can() {
    // Entry 1 owns `b` and `x`, entry 2 owns `m`, entry 3 owns `p`.
    let prefixes = BTreeMap::from([
        (Bytes::from_static(b"b"), entry(1)),
        (Bytes::from_static(b"m"), entry(2)),
        (Bytes::from_static(b"p"), entry(3)),
        (Bytes::from_static(b"x"), entry(1)),
    ]);
    for (first, last, held, inside) in [
        // The table starts under a prefix: its first key is a row of it.
        (Some(&b"m"[..]), Some(&b"n"[..]), vec![2], vec![]),
        (Some(b"ma"), Some(b"mz"), vec![2], vec![]),
        (Some(b"ma"), Some(b"o"), vec![2], vec![]),
        (Some(b"ma"), None, vec![2], vec![&b"p"[..], b"x"]),
        // The table ends under a prefix: its last key is a row of it.
        (Some(b"c"), Some(b"m"), vec![2], vec![]),
        (Some(b"c"), Some(b"mz"), vec![2], vec![]),
        (None, Some(b"mz"), vec![2], vec![b"b"]),
        // Both, of two entries, with a third strictly inside.
        (Some(b"bz"), Some(b"xa"), vec![1], vec![b"m", b"p"]),
        // A prefix strictly inside is left to the block index, unless the
        // range already shows a row of its entry.
        (Some(b"a"), Some(b"z"), vec![], vec![b"b", b"m", b"p", b"x"]),
        (Some(b"c"), Some(b"n"), vec![], vec![b"m"]),
        (Some(b"lz"), Some(b"n"), vec![], vec![b"m"]),
        (Some(b"a"), Some(b"xa"), vec![1], vec![b"m", b"p"]),
        (None, None, vec![], vec![b"b", b"m", b"p", b"x"]),
        // The table sorts outside every prefix: the one before its first key
        // does not cover it, and the ones after its last key are past it.
        (Some(b"c"), Some(b"l"), vec![], vec![]),
        (Some(b"n"), Some(b"o"), vec![], vec![]),
        (Some(b"a"), Some(b"az"), vec![], vec![]),
        (Some(b"y"), Some(b"z"), vec![], vec![]),
        (Some(b"y"), None, vec![], vec![]),
        (None, Some(b"a"), vec![], vec![]),
    ] {
        let (decided, undecided) = held_by_range(&prefixes, first, last);
        assert_eq!(
            decided,
            held.into_iter().map(entry).collect::<HashSet<_>>(),
            "{first:?}..={last:?}"
        );
        assert_eq!(
            undecided
                .iter()
                .map(|(prefix, _)| &prefix[..])
                .collect::<Vec<_>>(),
            inside,
            "{first:?}..={last:?}"
        );
        assert!(
            undecided
                .iter()
                .all(|(prefix, owner)| prefixes[*prefix] == *owner),
            "{first:?}..={last:?}"
        );
    }
}

#[test]
fn the_prefixes_a_table_can_hold_rows_under_are_the_one_it_starts_under_and_those_inside() {
    let prefixes = BTreeMap::from([
        (Bytes::from_static(b"b"), entry(1)),
        (Bytes::from_static(b"m"), entry(2)),
        (Bytes::from_static(b"p"), entry(3)),
    ]);
    for (first, last, expected) in [
        // Starts under `m`: `b` is behind it, `p` is in reach.
        (Some(&b"ma"[..]), Some(&b"q"[..]), vec![&b"m"[..], b"p"]),
        (Some(b"ma"), Some(b"mz"), vec![b"m"]),
        // A prefix equal to the first key covers it and is listed once.
        (Some(b"m"), Some(b"n"), vec![b"m"]),
        // Starts between prefixes: the one before does not cover it.
        (Some(b"c"), Some(b"pz"), vec![b"m", b"p"]),
        (Some(b"c"), Some(b"l"), vec![]),
        // The last key bounds the reach, inclusively.
        (Some(b"a"), Some(b"m"), vec![b"b", b"m"]),
        (Some(b"a"), Some(b"lz"), vec![b"b"]),
        // No first key: from below every prefix. No last key: past every one.
        (None, Some(b"c"), vec![b"b"]),
        (Some(b"n"), None, vec![b"p"]),
        (None, None, vec![b"b", b"m", b"p"]),
    ] {
        assert_eq!(
            overlapping(&prefixes, first, last)
                .map(|(prefix, owner)| {
                    assert_eq!(prefixes[prefix], owner);
                    &prefix[..]
                })
                .collect::<Vec<_>>(),
            expected,
            "{first:?}..={last:?}"
        );
    }
}

#[test]
fn the_bytes_under_a_prefix_are_those_of_the_blocks_indexed_under_it() {
    for (from, to, blocks) in [
        // Blocks 2, 3 and 4 are indexed under the prefix.
        (2, 5, 2..5),
        (0, 1, 0..1),
        (0, 3, 0..3),
        // None is: the rows are in the block before, when there is one.
        (3, 3, 2..3),
        (1, 1, 0..1),
        // The table starts under the prefix but its first block is indexed
        // below it: that block.
        (0, 0, 0..1),
    ] {
        assert_eq!(blocks_under(from, to), blocks, "{from}..{to}");
    }
}

#[test]
fn blocks_two_prefixes_share_count_once() {
    let ranges = |bounds: &[(usize, usize)]| {
        bounds
            .iter()
            .map(|(start, end)| *start..*end)
            .collect::<Vec<_>>()
    };
    for (given, joined) in [
        (vec![], vec![]),
        (vec![(2, 5)], vec![(2, 5)]),
        // Apart: both stay.
        (vec![(0, 1), (3, 4)], vec![(0, 1), (3, 4)]),
        // The same block twice, as when two prefixes fall inside one block.
        (vec![(2, 3), (2, 3)], vec![(2, 3)]),
        // Overlapping, touching, and one inside another.
        (vec![(0, 3), (2, 5)], vec![(0, 5)]),
        (vec![(0, 2), (2, 4)], vec![(0, 4)]),
        (vec![(0, 9), (3, 4), (8, 9)], vec![(0, 9)]),
        (vec![(0, 2), (1, 3), (5, 6), (6, 7)], vec![(0, 3), (5, 7)]),
    ] {
        assert_eq!(disjoint(ranges(&given)), ranges(&joined), "{given:?}");
    }
}

#[test]
fn a_block_index_points_at_the_one_block_that_can_hide_a_row() {
    for (before, next, verdict) in [
        // The first block indexed at or after the prefix starts under it.
        (0, Some(&b"m"[..]), Held::Yes),
        (0, Some(&b"ma"[..]), Held::Yes),
        (3, Some(b"mz"), Held::Yes),
        // Every block is indexed above the prefix's keys.
        (0, Some(b"n"), Held::No),
        (0, None, Held::No),
        // Rows under the prefix can only be in the last block indexed below
        // it.
        (1, Some(b"n"), Held::InBlock(0)),
        (3, Some(b"n"), Held::InBlock(2)),
        (3, None, Held::InBlock(2)),
    ] {
        assert_eq!(
            held_by_index(b"m", before, next),
            verdict,
            "{before} {next:?}"
        );
    }
}

#[test]
fn the_background_paces_are_ordered() {
    // A proof waiting for compactions is looked at more often than an idle
    // store is reclaimed, and no pass follows another sooner than the gap.
    assert!(LOOK_GAP <= DRAIN_LOOK_MAX);
    assert!(DRAIN_LOOK_MAX < IDLE_RECLAIM_AFTER);
    assert!(RETRY <= MAX_BACKOFF);
    // The task runs no pass while it waits for a reclaim's compaction: it
    // waits longer than it does between two looks, so that a small rewrite
    // ends within the wait, and no longer than a store must rest before it
    // is reclaimed, which is minutes and not the hours a large run takes.
    assert!(DRAIN_LOOK_MAX < RECLAIM_WAIT);
    assert!(RECLAIM_WAIT <= IDLE_RECLAIM_AFTER);
    assert_eq!(
        Pace::WRITER,
        Pace {
            look_gap: LOOK_GAP,
            idle_after: IDLE_RECLAIM_AFTER,
        }
    );
}
