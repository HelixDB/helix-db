//! Contracts of the reclaim's pure rules: which compactions a plan is made
//! of, which sorted runs are worth rewriting, and beside what a reclaim
//! starts a compaction.

use super::*;

const GIB: u64 = 1 << 30;
const MIB: u64 = 1 << 20;

fn laid(table: u128, bytes: u64) -> LaidTable {
    LaidTable {
        table: TableId(table),
        bytes,
    }
}

/// An L0 table whose view is named by its table id.
fn l0(table: u128, bytes: u64) -> (SourceId, LaidTable) {
    (view(table), laid(table, bytes))
}

fn view(table: u128) -> SourceId {
    SourceId::SstView(table.into())
}

fn tracked(tables: &[(u128, u64)]) -> HashMap<TableId, u64> {
    tables
        .iter()
        .map(|(table, bytes)| (TableId(*table), *bytes))
        .collect()
}

/// A rewrite of `ids`, newest first, into the lowest of them.
fn runs(ids: &[u32], bytes: u64, dropped_bytes: u64) -> Rewrite {
    Rewrite::Runs {
        ids: ids.to_vec(),
        into: *ids.iter().min().expect("a rewrite reads a run"),
        bytes,
        dropped_bytes,
    }
}

fn run(id: u32, bytes: u64, dropped_bytes: u64) -> Rewrite {
    runs(&[id], bytes, dropped_bytes)
}

const IDLE: Reclaimer = Reclaimer::IdleStore(Worth::WRITER);

fn worth_rewriting(run_bytes: u64, dropped_bytes: u64) -> bool {
    Worth::WRITER.rewriting(run_bytes, dropped_bytes)
}

#[test]
fn a_small_run_is_always_worth_rewriting() {
    for dropped in [0, 1, MIB, GIB] {
        assert!(worth_rewriting(0, dropped), "{dropped}");
        assert!(worth_rewriting(MIB, dropped), "{dropped}");
        assert!(worth_rewriting(GIB, dropped), "{dropped}");
    }
}

#[test]
fn a_rule_is_made_of_its_two_bounds() {
    let rule = Worth {
        small_run_bytes: 100,
        rewritten_per_dropped_byte: 3,
    };
    for (run_bytes, dropped_bytes, worth) in [
        (100, 0, true),
        (101, 0, false),
        (300, 100, true),
        (301, 100, false),
        (300, 99, false),
    ] {
        assert_eq!(
            rule.rewriting(run_bytes, dropped_bytes),
            worth,
            "{run_bytes} {dropped_bytes}"
        );
    }
}

#[test]
fn a_large_run_is_worth_rewriting_when_an_eighth_of_it_is_dropped() {
    assert_eq!(
        Worth::WRITER,
        Worth {
            small_run_bytes: GIB,
            rewritten_per_dropped_byte: 8,
        }
    );
    for (run_bytes, dropped_bytes, worth) in [
        // One byte past small, with nothing to speak of dropped.
        (GIB + 1, 0, false),
        (GIB + 1, MIB, false),
        // Exactly an eighth, and a byte either side of it.
        (8 * GIB, GIB, true),
        (8 * GIB, GIB - 1, false),
        (8 * GIB + 1, GIB, false),
        (8 * GIB - 1, GIB, true),
        // A large index in a larger run, and a small one.
        (40 * GIB, 10 * GIB, true),
        (40 * GIB, 5 * GIB, true),
        (40 * GIB, 5 * GIB - 1, false),
        (1024 * GIB, 10 * GIB, false),
        // All of it dropped.
        (40 * GIB, 40 * GIB, true),
        // The product does not wrap.
        (u64::MAX, u64::MAX / 4, true),
        (u64::MAX, GIB, false),
    ] {
        assert_eq!(
            worth_rewriting(run_bytes, dropped_bytes),
            worth,
            "{run_bytes} {dropped_bytes}"
        );
    }
}

#[test]
fn a_caller_is_never_refused_and_the_idle_store_follows_the_rule() {
    for (run_bytes, dropped_bytes) in [(MIB, 0), (40 * GIB, 0), (40 * GIB, MIB), (8 * GIB, GIB)] {
        assert!(Reclaimer::Caller.rewrites(run_bytes, dropped_bytes));
        assert_eq!(
            IDLE.rewrites(run_bytes, dropped_bytes),
            worth_rewriting(run_bytes, dropped_bytes),
            "{run_bytes} {dropped_bytes}"
        );
    }
}

#[test]
fn a_reclaim_never_takes_the_last_free_slot() {
    for (others, slots, caller, idle) in [
        // A compactor with nothing to do and room to spare: either starts.
        (0, 4, true, true),
        (0, 2, true, true),
        // Others run and a slot would stay free after its own: a caller's
        // starts beside them, an idle store's waits to be idle.
        (1, 4, true, false),
        (2, 4, true, false),
        (1, 3, true, false),
        // Its compaction would take the last free slot.
        (3, 4, false, false),
        (1, 2, false, false),
        // No slot is free at all.
        (4, 4, false, false),
        (5, 4, false, false),
        // With one slot it would take the only one, idle or not.
        (0, 1, false, false),
        (1, 1, false, false),
        // No slot, no compaction.
        (0, 0, false, false),
        (usize::MAX, 4, false, false),
    ] {
        assert_eq!(
            Reclaimer::Caller.starts_beside(others, slots),
            caller,
            "{others} {slots}"
        );
        assert_eq!(IDLE.starts_beside(others, slots), idle, "{others} {slots}");
    }
}

#[test]
fn a_plan_names_nothing_where_no_table_is_tracked() {
    let layout = Layout {
        l0: vec![l0(1, MIB)],
        runs: vec![(3, vec![laid(2, GIB)]), (0, vec![laid(3, 40 * GIB)])],
    };
    for reclaimer in [Reclaimer::Caller, IDLE] {
        assert_eq!(Plan::of(&layout, &tracked(&[]), reclaimer), Plan::default());
        // A tracked table that is no longer stored names nothing either.
        assert_eq!(
            Plan::of(&layout, &tracked(&[(9, GIB)]), reclaimer),
            Plan::default()
        );
        assert_eq!(
            Plan::of(&Layout::default(), &tracked(&[(1, GIB)]), reclaimer),
            Plan::default()
        );
    }
}

#[test]
fn a_plan_rewrites_only_the_runs_that_hold_tracked_tables() {
    // Runs 5, 4 and 0. Only run 4 holds tracked tables: two of its three.
    let layout = Layout {
        l0: vec![],
        runs: vec![
            (5, vec![laid(50, 100 * MIB)]),
            (
                4,
                vec![
                    laid(40, 200 * MIB),
                    laid(41, 300 * MIB),
                    laid(42, 100 * MIB),
                ],
            ),
            (0, vec![laid(1, 30 * GIB), laid(2, 10 * GIB)]),
        ],
    };
    let tracked = tracked(&[(40, 150 * MIB), (42, 20 * MIB)]);
    for reclaimer in [Reclaimer::Caller, IDLE] {
        assert_eq!(
            Plan::of(&layout, &tracked, reclaimer),
            Plan {
                // The whole run is what a rewrite costs; the rows of the
                // tracked tables are what it gives back.
                rewrites: vec![run(4, 600 * MIB, 170 * MIB)],
                left: None,
            }
        );
    }
}

#[test]
fn a_plan_leaves_a_run_that_is_not_worth_rewriting_unless_a_caller_asks() {
    let layout = Layout {
        l0: vec![],
        runs: vec![
            (7, vec![laid(70, 2 * GIB)]),
            (3, vec![laid(30, 4 * GIB), laid(31, 4 * GIB)]),
            (0, vec![laid(1, 30 * GIB), laid(2, 10 * GIB)]),
        ],
    };
    // A sliver in run 7, exactly an eighth of run 3, and next to nothing of
    // run 0.
    let tracked = tracked(&[(70, MIB), (30, GIB), (2, 3 * MIB)]);
    assert_eq!(
        Plan::of(&layout, &tracked, IDLE),
        Plan {
            rewrites: vec![run(3, 8 * GIB, GIB)],
            left: Some(LeftInPlace {
                dropped_bytes: 4 * MIB,
                run_bytes: 42 * GIB,
                runs: 2,
            }),
        }
    );
    assert_eq!(
        Plan::of(&layout, &tracked, Reclaimer::Caller),
        Plan {
            // A caller is refused none of them, and they follow one
            // another: one compaction.
            rewrites: vec![runs(&[7, 3, 0], 50 * GIB, GIB + 4 * MIB)],
            left: None,
        }
    );
}

/// An estimate can exceed its table: a block counts in full for each entry
/// that has rows in it. A plan counts a table for no more than its stored
/// bytes, so a run is not rewritten for dropped rows it cannot hold.
#[test]
fn a_plan_counts_a_table_for_no_more_than_its_size() {
    let layout = Layout {
        l0: vec![l0(90, 10 * MIB)],
        runs: vec![
            (5, vec![laid(50, 100 * MIB), laid(51, 1948 * MIB)]),
            (0, vec![laid(1, 200 * MIB), laid(2, 2 * GIB)]),
        ],
    };
    // Table 50 is estimated at three times its size, which would be more
    // than an eighth of run 5; table 1 at just its size, an eighth of run 0
    // short by nothing a cap changes; the L0 table at twice its size.
    let tracked = tracked(&[(90, 20 * MIB), (50, 300 * MIB), (1, 200 * MIB)]);
    assert!(worth_rewriting(2048 * MIB, 300 * MIB));
    assert!(!worth_rewriting(2048 * MIB, 100 * MIB));
    assert_eq!(
        Plan::of(&layout, &tracked, IDLE),
        Plan {
            rewrites: vec![Rewrite::L0 {
                sources: vec![view(90)],
                bytes: 10 * MIB,
                dropped_bytes: 10 * MIB,
            }],
            left: Some(LeftInPlace {
                dropped_bytes: 100 * MIB + 200 * MIB,
                run_bytes: 2048 * MIB + 200 * MIB + 2 * GIB,
                runs: 2,
            }),
        }
    );
    // A caller rewrites them whatever they hold, and is told the same
    // capped bytes.
    assert_eq!(
        Plan::of(&layout, &tracked, Reclaimer::Caller).rewrites[1..],
        [runs(
            &[5, 0],
            2048 * MIB + 200 * MIB + 2 * GIB,
            100 * MIB + 200 * MIB
        )]
    );
}

#[test]
fn a_plan_rewrites_runs_that_follow_one_another_in_one_compaction() {
    // Newest first. Runs 9 and 8 hold dropped rows and follow one another;
    // run 7 holds none; runs 5, 4 and 2 hold some, but 4 is not worth its
    // rewrite; run 0 holds none.
    let layout = Layout {
        l0: vec![],
        runs: vec![
            (9, vec![laid(90, 100 * MIB)]),
            (8, vec![laid(80, 300 * MIB), laid(81, 300 * MIB)]),
            (7, vec![laid(70, GIB)]),
            (5, vec![laid(50, 2 * GIB)]),
            (4, vec![laid(40, 20 * GIB)]),
            (2, vec![laid(20, 4 * GIB)]),
            (0, vec![laid(1, 30 * GIB)]),
        ],
    };
    let tracked = tracked(&[
        (90, 10 * MIB),
        (81, 20 * MIB),
        (50, GIB),
        (40, GIB),
        (20, 3 * GIB),
    ]);
    assert_eq!(
        Plan::of(&layout, &tracked, IDLE),
        Plan {
            rewrites: vec![
                // A run left in place stands between 5 and 2 as a run with
                // no dropped row would: each is rewritten into itself.
                run(2, 4 * GIB, 3 * GIB),
                run(5, 2 * GIB, GIB),
                // Into the lowest id of the two.
                runs(&[9, 8], 700 * MIB, 30 * MIB),
            ],
            left: Some(LeftInPlace {
                dropped_bytes: GIB,
                run_bytes: 20 * GIB,
                runs: 1,
            }),
        }
    );
    // A caller rewrites run 4 too, which joins its neighbours.
    assert_eq!(
        Plan::of(&layout, &tracked, Reclaimer::Caller),
        Plan {
            rewrites: vec![
                runs(&[5, 4, 2], 26 * GIB, 5 * GIB),
                runs(&[9, 8], 700 * MIB, 30 * MIB),
            ],
            left: None,
        }
    );
}

#[test]
fn a_plan_orders_its_compactions_of_runs_by_the_bytes_they_give_back() {
    // Four runs with dropped rows, kept apart by runs with none.
    let layout = Layout {
        l0: vec![],
        runs: vec![
            (9, vec![laid(90, GIB)]),
            (8, vec![laid(80, GIB)]),
            (6, vec![laid(60, GIB)]),
            (5, vec![laid(50, GIB)]),
            (4, vec![laid(40, GIB)]),
            (3, vec![laid(30, GIB)]),
            (2, vec![laid(20, GIB)]),
        ],
    };
    assert_eq!(
        Plan::of(
            &layout,
            &tracked(&[(90, 5), (60, 9), (40, 9), (20, 7)]),
            IDLE
        )
        .rewrites,
        // The two that give back the same stay in the order of the runs.
        vec![
            run(6, GIB, 9),
            run(4, GIB, 9),
            run(2, GIB, 7),
            run(9, GIB, 5)
        ]
    );
}

#[test]
fn a_plan_compacts_the_newest_tracked_l0_table_and_every_older_one() {
    // Newest first: tables 14, 13, 12, 11.
    let layout = Layout {
        l0: vec![
            l0(14, 4 * MIB),
            l0(13, 3 * MIB),
            l0(12, 2 * MIB),
            l0(11, MIB),
        ],
        runs: vec![(0, vec![laid(1, GIB)])],
    };
    for (tracked_tables, sources, bytes, dropped_bytes) in [
        // The oldest alone.
        (vec![(11, 7)], vec![11], MIB, 7),
        // One in the middle takes the older ones with it, tracked or not.
        (vec![(13, 5)], vec![13, 12, 11], 6 * MIB, 5),
        (vec![(13, 5), (11, 7)], vec![13, 12, 11], 6 * MIB, 12),
        // The newest takes them all.
        (vec![(14, 1)], vec![14, 13, 12, 11], 10 * MIB, 1),
    ] {
        for reclaimer in [Reclaimer::Caller, IDLE] {
            assert_eq!(
                Plan::of(&layout, &tracked(&tracked_tables), reclaimer),
                Plan {
                    rewrites: vec![Rewrite::L0 {
                        sources: sources.iter().copied().map(view).collect(),
                        bytes,
                        dropped_bytes,
                    }],
                    left: None,
                },
                "{tracked_tables:?}"
            );
        }
    }
}

#[test]
fn a_plan_compacts_l0_before_any_run_and_never_leaves_l0_in_place() {
    let layout = Layout {
        l0: vec![l0(12, 2 * MIB), l0(11, MIB)],
        runs: vec![(1, vec![laid(5, GIB)]), (0, vec![laid(1, 40 * GIB)])],
    };
    let tracked = tracked(&[(11, 1), (5, 9), (1, 2)]);
    assert_eq!(
        Plan::of(&layout, &tracked, IDLE),
        Plan {
            rewrites: vec![
                Rewrite::L0 {
                    sources: vec![view(11)],
                    bytes: MIB,
                    dropped_bytes: 1,
                },
                run(1, GIB, 9),
            ],
            left: Some(LeftInPlace {
                dropped_bytes: 2,
                run_bytes: 40 * GIB,
                runs: 1,
            }),
        }
    );
}

#[test]
fn a_rewrite_reads_what_it_names() {
    let l0 = Rewrite::L0 {
        sources: vec![view(12), view(11)],
        bytes: 30,
        dropped_bytes: 4,
    };
    assert_eq!(l0.sources(), vec![view(12), view(11)]);
    assert_eq!(l0.bytes(), (30, 4));
    assert_eq!(run(7, 50, 6).sources(), vec![SourceId::SortedRun(7)]);
    assert_eq!(run(7, 50, 6).bytes(), (50, 6));
    assert_eq!(
        runs(&[7, 5, 4], 90, 8).sources(),
        vec![
            SourceId::SortedRun(7),
            SourceId::SortedRun(5),
            SourceId::SortedRun(4)
        ]
    );
    assert_eq!(runs(&[7, 5, 4], 90, 8).bytes(), (90, 8));
}

#[test]
fn a_layout_knows_the_tables_a_rewrite_reads_and_whether_any_is_stored() {
    let layout = Layout {
        l0: vec![l0(13, MIB), l0(12, MIB), l0(11, MIB)],
        runs: vec![
            (4, vec![laid(40, GIB), laid(41, GIB)]),
            (0, vec![laid(1, GIB)]),
        ],
    };
    let tables = |ids: &[u128]| ids.iter().copied().map(TableId).collect::<HashSet<_>>();
    assert_eq!(layout.tables_of(&run(4, 2 * GIB, 0)), tables(&[40, 41]));
    assert_eq!(layout.tables_of(&run(0, GIB, 0)), tables(&[1]));
    assert_eq!(
        layout.tables_of(&runs(&[4, 0], 3 * GIB, 0)),
        tables(&[40, 41, 1])
    );
    // A run the layout does not have reads nothing.
    assert_eq!(layout.tables_of(&run(9, GIB, 0)), tables(&[]));
    assert_eq!(
        layout.tables_of(&Rewrite::L0 {
            sources: vec![view(12), view(11)],
            bytes: 2 * MIB,
            dropped_bytes: 0,
        }),
        tables(&[12, 11])
    );
    for (ids, stored) in [
        (vec![40], true),
        (vec![11], true),
        (vec![1, 99], true),
        (vec![99], false),
        (vec![], false),
    ] {
        assert_eq!(layout.holds_any(&tables(&ids)), stored, "{ids:?}");
    }
    assert!(!Layout::default().holds_any(&tables(&[1])));
}

#[test]
fn an_end_that_was_not_observed_is_settled_by_whether_its_sources_are_stored() {
    let obstacles = [
        ReclaimObstacle::Busy,
        ReclaimObstacle::Rejected,
        ReclaimObstacle::TimedOut,
        ReclaimObstacle::Closed,
    ];
    for sources_stored in [false, true] {
        // A completion that was read needs no second opinion.
        assert_eq!(Ended::Completed.obstacle(sources_stored), None);
        // Nor does an end that fell short: it keeps its own reason.
        for obstacle in obstacles {
            assert_eq!(
                Ended::Short(obstacle).obstacle(sources_stored),
                Some(obstacle)
            );
        }
    }
    // The record is gone. With the sources gone too the compaction replaced
    // them; with a source still stored it did not.
    assert_eq!(Ended::Unobserved.obstacle(false), None);
    assert_eq!(
        Ended::Unobserved.obstacle(true),
        Some(ReclaimObstacle::Rejected)
    );
}

#[test]
fn a_reclaim_that_ends_its_plan_says_what_it_did() {
    let left = LeftInPlace {
        dropped_bytes: MIB,
        run_bytes: 40 * GIB,
        runs: 1,
    };
    let compacted =
        |retired, entries, rewritten_bytes, dropped_bytes, left| DroppedStorageReclaim::Compacted {
            retired,
            entries,
            rewritten_bytes,
            dropped_bytes,
            left,
        };
    // It rewrote nothing because nothing was worth it: declined, however
    // many entries a pass retired on the way.
    for retired in [0, 2] {
        assert_eq!(
            DroppedStorageReclaim::planned_out(false, retired, 3, 0, 0, Some(left)),
            DroppedStorageReclaim::Declined { entries: 3, left }
        );
    }
    // It rewrote nothing because no proof tracked a table yet, and retired
    // nothing: the proofs wait for compactions in flight.
    assert_eq!(
        DroppedStorageReclaim::planned_out(false, 0, 2, 0, 0, None),
        DroppedStorageReclaim::Incomplete {
            entries: 2,
            obstacle: ReclaimObstacle::Busy,
        }
    );
    // Its first pass retired every entry it could, with nothing to rewrite.
    assert_eq!(
        DroppedStorageReclaim::planned_out(false, 1, 0, 0, 0, None),
        compacted(1, 0, 0, 0, None)
    );
    assert_eq!(
        DroppedStorageReclaim::planned_out(false, 1, 2, 0, 0, None),
        compacted(1, 2, 0, 0, None)
    );
    // No entry is left and none retired: one retired between the first look
    // and the plan, through a compaction the scheduler ran.
    assert_eq!(
        DroppedStorageReclaim::planned_out(false, 0, 0, 0, 0, None),
        compacted(0, 0, 0, 0, None)
    );
    // It rewrote: compacted, with what it left, whatever it retired.
    for (retired, entries, kept) in [(1, 0, None), (0, 1, None), (1, 1, Some(left))] {
        assert_eq!(
            DroppedStorageReclaim::planned_out(true, retired, entries, 3 * GIB, GIB, kept),
            compacted(retired, entries, 3 * GIB, GIB, kept)
        );
    }
}
