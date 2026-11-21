//! Contracts of the retirement proof's pure steps: what one look at the
//! store makes of a proof, and when a table can hold a row under a prefix.

use super::*;

fn compactions(ids: &[u128]) -> HashSet<CompactionId> {
    ids.iter().copied().map(CompactionId).collect()
}

fn tables(ids: &[u128]) -> HashSet<TableId> {
    ids.iter().copied().map(TableId).collect()
}

#[test]
fn an_unproven_entry_waits_for_every_compaction_in_flight() {
    // Whatever tables are stored: none is trusted before the drain.
    for stored in [tables(&[]), tables(&[1, 2])] {
        assert_eq!(
            Proof::Unproven.step(&compactions(&[7, 8]), &stored),
            Step::Drain(compactions(&[7, 8]))
        );
        assert_eq!(Proof::Unproven.step(&compactions(&[]), &stored), Step::Find);
    }
}

#[test]
fn a_draining_entry_waits_only_for_the_compactions_it_started_with() {
    let proof = Proof::Draining {
        compactions: compactions(&[7, 8]),
    };
    for (active, step) in [
        (compactions(&[7, 8, 9]), Step::Drain(compactions(&[7, 8]))),
        (compactions(&[8, 9]), Step::Drain(compactions(&[8]))),
        (compactions(&[7]), Step::Drain(compactions(&[7]))),
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
        tables: tables(&[1, 2]),
    };
    // Compactions in flight do not hold a tracked entry back: every one
    // created since the drain discards its rows.
    for active in [compactions(&[]), compactions(&[7])] {
        for (stored, step) in [
            (tables(&[1, 2, 3]), Step::Track(tables(&[1, 2]))),
            (tables(&[2, 3]), Step::Track(tables(&[2]))),
            (tables(&[1]), Step::Track(tables(&[1]))),
            (tables(&[3, 4]), Step::Retire),
            (tables(&[]), Step::Retire),
        ] {
            assert_eq!(proof.step(&active, &stored), step, "{stored:?}");
        }
    }
}

#[test]
fn a_tables_key_range_decides_only_what_it_can() {
    let held = |first: Option<&[u8]>, last: Option<&[u8]>| held_by_range(b"m", first, last);
    for (first, last, verdict) in [
        // An end of the table is itself a row under the prefix.
        (Some(&b"m"[..]), Some(&b"z"[..]), Some(true)),
        (Some(b"ma"), Some(b"z"), Some(true)),
        (Some(b"a"), Some(b"m"), Some(true)),
        (Some(b"a"), Some(b"mz"), Some(true)),
        (Some(b"ma"), Some(b"mz"), Some(true)),
        (None, Some(b"mz"), Some(true)),
        (Some(b"ma"), None, Some(true)),
        // The table ends before the prefix, or starts after its keys.
        (Some(b"a"), Some(b"l"), Some(false)),
        (Some(b"a"), Some(b"lz"), Some(false)),
        (Some(b"n"), Some(b"z"), Some(false)),
        (None, Some(b"l"), Some(false)),
        (Some(b"n"), None, Some(false)),
        // The prefix sorts strictly inside the table.
        (Some(b"a"), Some(b"z"), None),
        (Some(b"l"), Some(b"n"), None),
        (Some(b"lz"), Some(b"n"), None),
        (None, Some(b"z"), None),
        (Some(b"a"), None, None),
        (None, None, None),
    ] {
        assert_eq!(held(first, last), verdict, "{first:?}..={last:?}");
    }
}

#[test]
fn a_block_index_points_at_the_one_block_that_can_hide_a_row() {
    for (before, next, verdict) in [
        // The first block at or after the prefix starts under it.
        (0, Some(&b"m"[..]), Held::Yes),
        (0, Some(&b"ma"[..]), Held::Yes),
        (3, Some(b"mz"), Held::Yes),
        // Every block starts above the prefix's keys.
        (0, Some(b"n"), Held::No),
        (0, None, Held::No),
        // Rows under the prefix can only follow the first key of the last
        // block that starts below it.
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
