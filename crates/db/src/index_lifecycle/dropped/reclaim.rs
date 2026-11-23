//! Reclaims the storage of dropped generations.
//!
//! Compactions the scheduler picks discard the rows of an armed entry as they
//! pass, and [`super::retire`] deletes the entry once they have. That gives
//! the bytes back whenever the scheduler gets to the tables that hold them,
//! which for the oldest sorted run can be a long time, and on a store that
//! takes few writes is never. A *reclaim* compacts for the entries itself.
//!
//! # What a reclaim rewrites
//!
//! Only tables a proof tracks hold rows of a dropped generation, and the
//! proof knows them, with an estimate of the bytes of those rows in each
//! ([`Tracked`](super::Tracked)). A table cannot be rewritten by itself:
//! SlateDB compacts whole sources, a source is an L0 table or a sorted run,
//! the sources of one compaction are consecutive, and it writes the run with
//! the lowest id among them. So the smallest rewrite that covers a tracked
//! table is:
//!
//! - for a table of a sorted run, that run, into itself;
//! - for an L0 table, that table and every older L0 table, into a new run
//!   above every other. The older tables go with it because rows in L0 must
//!   stay newer than rows in any run.
//!
//! A reclaim plans exactly those ([`Plan`]) and submits them one at a time,
//! planning again after each from what is then stored. Runs to rewrite that
//! follow one another go in one compaction, into the oldest of them: the
//! same bytes as rewriting each alone, and one run instead of several that
//! the scheduler would merge right after. It never reads or rewrites a run
//! that holds no tracked table.
//!
//! # When a run is worth rewriting
//!
//! Rewriting a run costs its whole size in reads and writes and holds that
//! many more bytes in object storage until the old tables are collected,
//! whatever part of it is dropped rows. Leaving the rows costs their bytes
//! of storage for as long as the scheduler leaves the run alone, and nothing
//! else: an armed entry costs one hash probe per compacted row. So a reclaim
//! the writer starts by itself rewrites a run only when [`Worth::WRITER`]
//! says so:
//!
//! - the run is at most a gibibyte, which one compaction slot rewrites in
//!   about a quarter of a minute; or
//! - the dropped rows are at least an eighth of it.
//!
//! Dropped rows in any other run are *left in place*
//! ([`LeftInPlace`]): they wait for the scheduler to rewrite the run, or for
//! more drops to make it worth it, or for a caller, who is never refused,
//! to ask.
//!
//! # What a reclaim holds up
//!
//! A compaction of sorted runs takes one of the compactor's slots and those
//! runs. While it runs the scheduler cannot merge them with others; a merge
//! it has to wait for rewrites one of the same runs. Every other compaction
//! goes on in the slots that are left.
//!
//! Writes depend on one compaction only: the one that empties L0, since a
//! writer stalls when L0 is full. So a reclaim never takes the compactor's
//! last free slot: it submits a compaction only while that leaves the
//! scheduler one, and otherwise ends [`ReclaimObstacle::Busy`] and waits to
//! be started again. It holds one slot of at least two for as long as its
//! compaction runs, and never more; a compactor with a single slot is never
//! reclaimed. Nothing cancels a reclaim's compaction once it is submitted.
//!
//! Looking for a free slot and submitting are two steps. The scheduler can
//! fill the slots that were free in between, and the compactor then has one
//! compaction more than it runs at a time, until one of the scheduler's own
//! ends: the wait that costs a compaction of L0 tables is for one of the
//! scheduler's compactions, never for the reclaim's.
//!
//! A reclaim plans from one read of `.compactions` and the manifest
//! together, so a merge it sees finished is in the runs it weighs, and one
//! it sees in flight makes it leave those runs alone. A merge that starts
//! and ends between that read and the coordinator's next poll can still
//! grow a run after it was weighed: by what one poll interval merges.
//!
//! A compaction a reclaim submitted goes on if the process dies, and the
//! compactor of the next process resumes it like any other. Nobody needs to
//! watch it: it holds the one slot it was given until it ends. If its
//! filters are created before that process has armed the entry, which the
//! first background pass does, it keeps the rows it was submitted to
//! discard; the proof waits for it, finds the rows again, and the run is
//! rewritten once more when the store is next idle.
//!
//! A reclaim deletes no entry itself. Each of its compactions is followed by
//! a retirement pass, which deletes the entries left without rows.
//!
//! An entry is discardable once the manifest's retention sequence has reached
//! its gate. SlateDB advances that sequence only when it flushes a memtable,
//! so a reclaim that finds an entry waiting flushes once; a database that
//! takes no write after the last reader older than a drop has closed waits
//! for its next write.
//!
//! # What does not scale
//!
//! The unit is a sorted run. A small index whose rows sit in a very large
//! run is left in place by the writer, and costs a caller who insists the
//! whole run. A rewritten run needs up to its own size again in object
//! storage until the old tables are collected. A store that is never idle
//! is never reclaimed by its writer: its dropped rows stay until the
//! scheduler's own compactions rewrite the runs that hold them. A database
//! whose manifest has segments is not reclaimed: its entries still retire,
//! and every reclaim fails until this learns to compact segments.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use slatedb::compactor::{CompactionSpec, CompactionStatus, CompactorStateView, SourceId};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::VersionedManifest;
use tokio::time::Instant;

use crate::error::{HelixDbError, Result};

use super::retire::{Logged, Retirer};
use super::{DroppedIndexStorage, TableId};

/// Interval between reads of an awaited compaction's status.
const STATUS_POLL: Duration = Duration::from_secs(1);

/// When a sorted run is worth rewriting for the dropped rows in it; see the
/// module documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Worth {
    /// The largest sorted run rewritten whatever part of it is dropped rows.
    pub(crate) small_run_bytes: u64,
    /// The most bytes of a larger sorted run rewritten for each byte of
    /// dropped rows in it.
    pub(crate) rewritten_per_dropped_byte: u64,
}

impl Worth {
    /// The rule a writer follows by itself. Contracts make smaller ones.
    ///
    /// One compaction slot rewrites a gibibyte in about a quarter of a
    /// minute against object storage, so for a run that small retiring the
    /// entry is worth it by itself.
    ///
    /// For a larger run: a rewritten byte costs its read and its write, and
    /// a second copy in object storage until the old tables are collected,
    /// a day at the default. A dropped byte left in place costs its storage
    /// until the scheduler rewrites the run, which for an old run is weeks.
    /// At the prices of object storage the two meet near one part in sixteen
    /// for rows that would otherwise wait a month; one in eight leaves room
    /// for the compactor's time, and keeps what a reclaim rewrites for a
    /// byte below what size-tiered compaction already rewrites to bring a
    /// byte to its oldest run.
    pub(crate) const WRITER: Self = Self {
        small_run_bytes: 1 << 30,
        rewritten_per_dropped_byte: 8,
    };

    /// Whether rewriting a sorted run of `run_bytes` is worth the
    /// `dropped_bytes` it gives back.
    pub(crate) fn rewriting(self, run_bytes: u64, dropped_bytes: u64) -> bool {
        run_bytes <= self.small_run_bytes
            || run_bytes <= dropped_bytes.saturating_mul(self.rewritten_per_dropped_byte)
    }
}

/// Why a requested reclaim did not see a compaction of its plan complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimObstacle {
    /// A compaction of the same tables was already in flight; or the proof
    /// of an entry waits for compactions in flight before it can say which
    /// tables hold its rows; or the compaction would have taken the
    /// compactor's last free slot; or a reclaim the writer started found the
    /// compactor no longer idle. Nothing more was submitted.
    Busy,
    /// SlateDB refused the compaction, or one the scheduler started first
    /// replaced its sources.
    Rejected,
    /// The caller's wait ran out, before the reclaim began or while a
    /// compaction it submitted was running. That compaction goes on.
    TimedOut,
    /// The database closed.
    Closed,
}

/// Dropped rows a writer leaves where they are: they sit in sorted runs
/// larger than a gibibyte of which they are less than an eighth, which are
/// not worth rewriting for them.
///
/// ```
/// use db::LeftInPlace;
///
/// let left = LeftInPlace { dropped_bytes: 3 << 20, run_bytes: 40 << 30, runs: 1 };
/// // Getting 3 MiB back would mean rewriting 40 GiB.
/// assert!(left.run_bytes / left.dropped_bytes > 8);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeftInPlace {
    /// Estimate of the stored bytes of the dropped rows.
    pub dropped_bytes: u64,
    /// Stored bytes of the sorted runs that hold them: what getting them
    /// back would rewrite.
    pub run_bytes: u64,
    /// How many sorted runs those are.
    pub runs: usize,
}

/// What a requested reclaim of dropped index storage did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DroppedStorageReclaim {
    /// No dropped generation awaits compaction.
    Idle,
    /// A snapshot or transaction older than their drops is still open, or no
    /// memtable has been flushed since the last one closed: the rows of all
    /// `entries` dropped generations are retained and nothing was compacted.
    Retained {
        /// Dropped generations awaiting compaction.
        entries: usize,
    },
    /// A compaction of the plan was not seen to complete. The `entries`
    /// dropped generations retire when the scheduler's compactions reach
    /// their rows, or with a later reclaim.
    Incomplete {
        /// Dropped generations awaiting compaction.
        entries: usize,
        /// What stood in the way.
        obstacle: ReclaimObstacle,
    },
    /// Nothing was compacted: every table that holds rows of the `entries`
    /// dropped generations is in a sorted run not worth rewriting for them.
    /// Only a reclaim the writer starts by itself ends so.
    Declined {
        /// Dropped generations awaiting compaction.
        entries: usize,
        /// The rows and the runs that hold them.
        left: LeftInPlace,
    },
    /// Every table the plan named was rewritten without the rows of the
    /// discardable dropped generations.
    Compacted {
        /// Dropped generations whose entries the passes that followed
        /// deleted.
        retired: usize,
        /// Dropped generations still awaiting compaction: dropped after the
        /// reclaim began, still read by an older reader, left in place, or
        /// with rows in a table a compaction in flight had yet to replace.
        entries: usize,
        /// Stored bytes of the L0 tables and sorted runs that were
        /// rewritten.
        rewritten_bytes: u64,
        /// Estimate of the stored bytes the dropped rows occupied in them.
        dropped_bytes: u64,
        /// Dropped rows in sorted runs that were not worth rewriting.
        left: Option<LeftInPlace>,
    },
}

impl DroppedStorageReclaim {
    /// What a reclaim did that came to the end of its plan: whether it
    /// `rewrote` any table, the entries the passes after its compactions
    /// `retired`, the `entries` still armed, the bytes its compactions
    /// rewrote and the dropped rows among them, and the rows it `left`.
    fn planned_out(
        rewrote: bool,
        retired: usize,
        entries: usize,
        rewritten_bytes: u64,
        dropped_bytes: u64,
        left: Option<LeftInPlace>,
    ) -> Self {
        match (rewrote, left) {
            (false, Some(left)) => Self::Declined { entries, left },
            // Nothing tracked was there to rewrite, and entries are left:
            // their proofs wait for compactions in flight.
            (false, None) if entries > 0 && retired == 0 => Self::Incomplete {
                entries,
                obstacle: ReclaimObstacle::Busy,
            },
            (true | false, left) => Self::Compacted {
                retired,
                entries,
                rewritten_bytes,
                dropped_bytes,
                left,
            },
        }
    }
}

/// Who runs a reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reclaimer {
    /// A caller who asked for the bytes now, whatever they cost: every run
    /// that holds a tracked table is rewritten, beside whatever else the
    /// compactor is doing, while that leaves the scheduler a free slot.
    Caller,
    /// The background task, because the compactor has been idle. It
    /// rewrites a run only when that is worth it by this rule, and starts a
    /// compaction only while no other is in flight: once the scheduler is
    /// compacting again the reclaim ends [`ReclaimObstacle::Busy`].
    IdleStore(Worth),
}

impl Reclaimer {
    /// Whether this reclaimer rewrites a sorted run of `run_bytes` that
    /// holds `dropped_bytes` of dropped rows.
    fn rewrites(self, run_bytes: u64, dropped_bytes: u64) -> bool {
        match self {
            Self::Caller => true,
            Self::IdleStore(worth) => worth.rewriting(run_bytes, dropped_bytes),
        }
    }

    /// Whether this reclaimer starts a compaction beside `others` in flight
    /// on a compactor with `slots`: never when that would take the last
    /// free slot, and by itself only on a compactor with nothing to do.
    fn starts_beside(self, others: usize, slots: usize) -> bool {
        let leaves_a_slot = others.saturating_add(1) < slots;
        match self {
            Self::Caller => leaves_a_slot,
            Self::IdleStore(_) => leaves_a_slot && others == 0,
        }
    }
}

/// One table of a [`Layout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LaidTable {
    /// The id of its file.
    pub(crate) table: TableId,
    /// Its stored bytes.
    pub(crate) bytes: u64,
}

/// The tables of a store as a reclaim plans over them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Layout {
    /// The L0 tables, newest first, each with the source a compaction names
    /// it by.
    pub(crate) l0: Vec<(SourceId, LaidTable)>,
    /// The sorted runs by id, each with its tables.
    pub(crate) runs: Vec<(u32, Vec<LaidTable>)>,
}

impl Layout {
    /// The tables `rewrite` reads.
    fn tables_of(&self, rewrite: &Rewrite) -> HashSet<TableId> {
        match rewrite {
            Rewrite::L0 { sources, .. } => self
                .l0
                .iter()
                .filter(|(source, _)| sources.contains(source))
                .map(|(_, laid)| laid.table)
                .collect(),
            Rewrite::Runs { ids, .. } => self
                .runs
                .iter()
                .filter(|(run, _)| ids.contains(run))
                .flat_map(|(_, tables)| tables.iter().map(|laid| laid.table))
                .collect(),
        }
    }

    /// Whether any of `tables` is stored.
    fn holds_any(&self, tables: &HashSet<TableId>) -> bool {
        self.l0
            .iter()
            .map(|(_, laid)| laid)
            .chain(self.runs.iter().flat_map(|(_, tables)| tables))
            .any(|laid| tables.contains(&laid.table))
    }

    /// The tables of `manifest`, which has no segments.
    pub(crate) fn of(manifest: &VersionedManifest) -> Self {
        let laid = |view: &slatedb::manifest::SsTableView| LaidTable {
            table: TableId(view.sst.id.value().0),
            bytes: view.estimate_size(),
        };
        Self {
            l0: manifest
                .l0()
                .iter()
                .map(|view| (SourceId::SstView(view.id), laid(view)))
                .collect(),
            runs: manifest
                .compacted()
                .iter()
                .map(|run| (run.id, run.sst_views().iter().map(laid).collect()))
                .collect(),
        }
    }
}

/// One compaction of a [`Plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rewrite {
    /// These L0 tables, newest first, into a new sorted run.
    L0 {
        sources: Vec<SourceId>,
        bytes: u64,
        dropped_bytes: u64,
    },
    /// These sorted runs, which follow one another, newest first, into the
    /// one of them with the lowest id. One run is rewritten into itself.
    Runs {
        ids: Vec<u32>,
        into: u32,
        bytes: u64,
        dropped_bytes: u64,
    },
}

impl Rewrite {
    /// Stored bytes of the sources, and the estimate of the dropped rows in
    /// them.
    fn bytes(&self) -> (u64, u64) {
        match self {
            Self::L0 {
                bytes,
                dropped_bytes,
                ..
            }
            | Self::Runs {
                bytes,
                dropped_bytes,
                ..
            } => (*bytes, *dropped_bytes),
        }
    }

    /// What a compaction of this rewrite reads.
    fn sources(&self) -> Vec<SourceId> {
        match self {
            Self::L0 { sources, .. } => sources.clone(),
            Self::Runs { ids, .. } => ids.iter().copied().map(SourceId::SortedRun).collect(),
        }
    }
}

/// What a reclaimer does with one sorted run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// It holds no tracked table.
    Clean,
    /// It holds dropped rows and is rewritten.
    Rewritten,
    /// It holds dropped rows and is not worth rewriting for them.
    Left,
}

/// What a reclaim would compact, and what it would leave.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Plan {
    /// The compactions, in the order they are submitted: L0 first, then the
    /// sorted runs, those with the most dropped bytes first.
    pub(crate) rewrites: Vec<Rewrite>,
    /// Dropped rows in sorted runs the reclaimer does not rewrite.
    pub(crate) left: Option<LeftInPlace>,
}

impl Plan {
    /// The compactions that rewrite every table of `layout` among `tracked`
    /// and no sorted run without one, less the sorted runs `reclaimer` does
    /// not rewrite. `tracked` gives the estimated bytes of dropped rows in
    /// each table.
    ///
    /// Runs to rewrite that follow one another go in one compaction. That
    /// reads and writes the same bytes as rewriting each by itself, and
    /// leaves the scheduler nothing to merge among them: a run rewritten
    /// alone shrinks into the size of its neighbours, and the scheduler
    /// would rewrite them all once more.
    pub(crate) fn of(
        layout: &Layout,
        tracked: &HashMap<TableId, u64>,
        reclaimer: Reclaimer,
    ) -> Self {
        let dropped = |tables: &mut dyn Iterator<Item = &LaidTable>| {
            tables
                .filter_map(|laid| tracked.get(&laid.table))
                .sum::<u64>()
        };
        // The newest tracked L0 table and every older one.
        let l0 = layout
            .l0
            .iter()
            .position(|(_, laid)| tracked.contains_key(&laid.table))
            .map(|newest| &layout.l0[newest..])
            .map(|tables| Rewrite::L0 {
                sources: tables.iter().map(|(source, _)| *source).collect(),
                bytes: tables.iter().map(|(_, laid)| laid.bytes).sum(),
                dropped_bytes: dropped(&mut tables.iter().map(|(_, laid)| laid)),
            });
        let runs = layout
            .runs
            .iter()
            .map(|(id, tables)| {
                let bytes = tables.iter().map(|laid| laid.bytes).sum::<u64>();
                let dropped_bytes = dropped(&mut tables.iter());
                let verdict = match tables.iter().any(|laid| tracked.contains_key(&laid.table)) {
                    false => Verdict::Clean,
                    true if reclaimer.rewrites(bytes, dropped_bytes) => Verdict::Rewritten,
                    true => Verdict::Left,
                };
                (*id, bytes, dropped_bytes, verdict)
            })
            .collect::<Vec<_>>();
        let mut together = runs
            .chunk_by(|(.., newer), (.., older)| {
                *newer == Verdict::Rewritten && *older == Verdict::Rewritten
            })
            .filter(|runs| {
                runs.iter()
                    .all(|(.., verdict)| *verdict == Verdict::Rewritten)
            })
            .filter_map(|runs| {
                Some(Rewrite::Runs {
                    ids: runs.iter().map(|(id, ..)| *id).collect(),
                    into: runs.iter().map(|(id, ..)| *id).min()?,
                    bytes: runs.iter().map(|(_, bytes, ..)| bytes).sum(),
                    dropped_bytes: runs
                        .iter()
                        .map(|(_, _, dropped_bytes, _)| dropped_bytes)
                        .sum(),
                })
            })
            .collect::<Vec<_>>();
        together.sort_by_key(|rewrite| std::cmp::Reverse(rewrite.bytes().1));
        let left = runs
            .iter()
            .filter(|(.., verdict)| *verdict == Verdict::Left)
            .collect::<Vec<_>>();
        Self {
            rewrites: l0.into_iter().chain(together).collect(),
            left: (!left.is_empty()).then(|| LeftInPlace {
                dropped_bytes: left
                    .iter()
                    .map(|(_, _, dropped_bytes, _)| dropped_bytes)
                    .sum(),
                run_bytes: left.iter().map(|(_, bytes, ..)| bytes).sum(),
                runs: left.len(),
            }),
        }
    }
}

/// The dropped generations `writer` still stores, as of its last background
/// look, with the rows a writer that follows `worth` leaves in place, going
/// by the manifest it holds in memory. Reads no object storage.
pub(crate) fn storage(writer: &crate::HelixWriter, worth: Worth) -> DroppedIndexStorage {
    let registry = writer.dropped();
    let left_in_place = registry
        .tracks_tables()
        .then(|| {
            let manifest = writer.db().manifest();
            let tracked = registry.discardable_tracked(manifest.recent_snapshot_min_seq());
            Plan::of(
                &Layout::of(&manifest),
                &tracked,
                Reclaimer::IdleStore(worth),
            )
            .left
        })
        .flatten();
    DroppedIndexStorage {
        left_in_place,
        ..registry.storage()
    }
}

/// How an awaited compaction ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    Completed,
    /// Its record was replaced by a later one before its result was read:
    /// `.compactions` keeps the last finished compaction only. The manifest
    /// tells which it was.
    Unobserved,
    Short(ReclaimObstacle),
}

impl Ended {
    /// What kept the compaction from rewriting its sources, if anything.
    /// `sources_stored` says whether a table it read is still stored, which
    /// decides an end that was not observed: SlateDB writes the manifest
    /// before it records a compaction as completed, and a compaction that
    /// did not complete leaves its sources stored, so with them gone they
    /// were rewritten.
    fn obstacle(self, sources_stored: bool) -> Option<ReclaimObstacle> {
        match (self, sources_stored) {
            (Self::Completed, _) | (Self::Unobserved, false) => None,
            (Self::Unobserved, true) => Some(ReclaimObstacle::Rejected),
            (Self::Short(obstacle), _) => Some(obstacle),
        }
    }
}

impl Retirer {
    /// Compacts the store so that the rows of every discardable dropped
    /// generation are gone from the tables its [`Plan`] names, waiting no
    /// later than `deadline` for anything: a pass or another reclaim under
    /// way, or a compaction of its own. Logs what it did.
    pub(crate) async fn reclaim(
        &self,
        deadline: Instant,
        reclaimer: Reclaimer,
    ) -> Result<DroppedStorageReclaim> {
        let Ok(mut logged) = tokio::time::timeout_at(deadline, self.pass.lock()).await else {
            // What the registry holds as it stands: nothing was looked at.
            return Ok(DroppedStorageReclaim::Incomplete {
                entries: self.writer().dropped().armed().len(),
                obstacle: ReclaimObstacle::TimedOut,
            });
        };
        let outcome = self.compact(deadline, reclaimer, &mut logged).await?;
        // A store that takes a few writes is reclaimed again each time it
        // falls idle, and a reclaim that declines the same runs each time
        // says so once.
        if logged.reclaim != Some(outcome) {
            tracing::info!(
                ?outcome,
                ?reclaimer,
                "reclaim of dropped index storage ended"
            );
            logged.reclaim = Some(outcome);
        }
        Ok(outcome)
    }

    async fn compact(
        &self,
        deadline: Instant,
        reclaimer: Reclaimer,
        logged: &mut Logged,
    ) -> Result<DroppedStorageReclaim> {
        let db = self.writer().db();
        let registry = self.writer().dropped();
        registry.refresh(db).await?;
        let armed = registry.armed();
        if armed.is_empty() {
            return Ok(DroppedStorageReclaim::Idle);
        }
        let mut retention = self.view().await?.manifest().recent_snapshot_min_seq();
        if armed.iter().any(|(_, armed)| armed.gate > retention) {
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;
            retention = self.view().await?.manifest().recent_snapshot_min_seq();
        }
        if armed.iter().all(|(_, armed)| armed.gate > retention) {
            return Ok(DroppedStorageReclaim::Retained {
                entries: armed.len(),
            });
        }
        // A pass first: the plan is made of the tables the proofs track.
        let mut retired = self.look(logged).await?;
        let mut rewritten_bytes = 0u64;
        let mut dropped_bytes = 0u64;
        // A source is rewritten once: one whose rows a compaction kept is
        // left to a later reclaim.
        let mut rewritten = HashSet::new();
        let incomplete = |obstacle| DroppedStorageReclaim::Incomplete {
            entries: registry.armed().len(),
            obstacle,
        };
        // Imperative: each turn plans from what is stored now and awaits one
        // compaction.
        let left = loop {
            // One read of `.compactions` and then the manifest: a merge it
            // shows finished is in the runs the plan weighs, and one it
            // shows in flight keeps those runs out of this reclaim below.
            let view = self.view().await?;
            let manifest = view.manifest();
            let active = view
                .compactions()
                .into_iter()
                .flat_map(|compactions| compactions.recent_compactions())
                .filter(|compaction| compaction.active())
                .collect::<Vec<_>>();
            let tracked = registry.discardable_tracked(manifest.recent_snapshot_min_seq());
            let layout = Layout::of(manifest);
            let plan = Plan::of(&layout, &tracked, reclaimer);
            let Some(rewrite) = plan.rewrites.into_iter().find(|rewrite| {
                !rewrite
                    .sources()
                    .iter()
                    .any(|source| rewritten.contains(source))
            }) else {
                break plan.left;
            };
            if !reclaimer.starts_beside(active.len(), self.slots) {
                return Ok(incomplete(ReclaimObstacle::Busy));
            }
            let sources = rewrite.sources();
            let in_use = |source: &SourceId| {
                active.iter().any(|compaction| {
                    let spec = compaction.spec();
                    spec.sources().contains(source)
                        || spec.destination().map(SourceId::SortedRun) == Some(*source)
                })
            };
            let destination = match &rewrite {
                // One compaction of L0 tables runs at a time, and the one
                // in flight discards these rows itself or restarts the
                // proof.
                Rewrite::L0 { .. } if active.iter().any(|c| c.spec().has_l0_sources()) => {
                    return Ok(incomplete(ReclaimObstacle::Busy));
                }
                // Above every run and every destination in flight, as
                // SlateDB requires of a run made of L0 tables alone.
                Rewrite::L0 { .. } => manifest
                    .compacted()
                    .iter()
                    .map(|run| run.id)
                    .chain(active.iter().filter_map(|c| c.spec().destination()))
                    .max()
                    .map_or(0, |id| id + 1),
                Rewrite::Runs { .. } if sources.iter().any(in_use) => {
                    return Ok(incomplete(ReclaimObstacle::Busy));
                }
                Rewrite::Runs { into, .. } => *into,
            };
            let (bytes, dropped) = rewrite.bytes();
            tracing::info!(
                ?rewrite,
                destination,
                "reclaiming dropped generations: compacting"
            );
            let ended = self
                .compacted(CompactionSpec::new(sources.clone(), destination), deadline)
                .await?;
            // Only an end that was not observed is settled by the manifest.
            let sources_stored = match ended {
                Ended::Unobserved => {
                    Layout::of(self.view().await?.manifest()).holds_any(&layout.tables_of(&rewrite))
                }
                Ended::Completed | Ended::Short(_) => false,
            };
            match ended.obstacle(sources_stored) {
                Some(obstacle) => return Ok(incomplete(obstacle)),
                None => {
                    rewritten.extend(sources);
                    rewritten_bytes += bytes;
                    dropped_bytes += dropped;
                    retired += self.look(logged).await?;
                }
            }
        };
        Ok(DroppedStorageReclaim::planned_out(
            !rewritten.is_empty(),
            retired,
            registry.armed().len(),
            rewritten_bytes,
            dropped_bytes,
            left,
        ))
    }

    /// `.compactions` and the manifest of the store, read in that order.
    async fn view(&self) -> Result<CompactorStateView> {
        let view = self.admin.read_compactor_state_view().await?;
        if !view.manifest().segments().is_empty() {
            return Err(HelixDbError::InvariantViolation(
                "dropped-generation reclaim does not support segmented manifests".to_string(),
            ));
        }
        Ok(view)
    }

    /// Submits `spec` and waits for it to finish, no later than `deadline`.
    /// Nothing cancels it: one that outlives the wait, or this process, goes
    /// on.
    ///
    /// SlateDB refuses a compaction by marking its record failed, so a
    /// refusal is read here like any other end.
    async fn compacted(&self, spec: CompactionSpec, deadline: Instant) -> Result<Ended> {
        let mut closed = self.closed.subscribe();
        let id = self.admin.submit_compaction(spec).await?.id();
        // Imperative: each turn awaits a status read, then the poll interval.
        loop {
            if *closed.borrow() {
                return Ok(Ended::Short(ReclaimObstacle::Closed));
            }
            match self
                .admin
                .read_compaction(id, None)
                .await?
                .map(|compaction| compaction.status())
            {
                Some(CompactionStatus::Completed) => return Ok(Ended::Completed),
                Some(CompactionStatus::Failed) => {
                    return Ok(Ended::Short(ReclaimObstacle::Rejected));
                }
                None => return Ok(Ended::Unobserved),
                Some(
                    CompactionStatus::Submitted
                    | CompactionStatus::Scheduled
                    | CompactionStatus::Running
                    // Its output is complete: the coordinator commits it.
                    | CompactionStatus::Compacted,
                ) => {}
            }
            if Instant::now() >= deadline {
                return Ok(Ended::Short(ReclaimObstacle::TimedOut));
            }
            tokio::select! {
                () = tokio::time::sleep_until((Instant::now() + STATUS_POLL).min(deadline)) => {}
                _ = closed.changed() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests;
