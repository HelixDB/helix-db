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
//!   seconds; or
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
//! it has to wait for rewrites one of the same runs. Every other compaction goes on,
//! those of L0 tables included, as long as a slot is free.
//!
//! Writes depend on one compaction only: the one that empties L0, since a
//! writer stalls when L0 is full. A reclaim *gives way* to it: while its
//! compaction is in flight it watches `.compactions`, and when every slot is
//! taken, no compaction of L0 tables is in flight and L0 holds a table, it
//! cancels its own. The worker stops it within one heartbeat and the
//! coordinator frees the slot at its next poll; see
//! [`ReclaimObstacle::GaveWay`]. What the cancelled compaction had written
//! is lost, and the run is planned again by a later reclaim.
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
//! storage until the old tables are collected. A reclaim is lost when the
//! process restarts while its record is still unclaimed, and resumed by the
//! compactor of the next process otherwise. A database whose manifest has
//! segments is not reclaimed: its entries still retire, and every reclaim
//! fails until this learns to compact segments.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use slatedb::compactor::{Compaction, CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::VersionedManifest;
use tokio::time::Instant;

use crate::error::{HelixDbError, Result};

use super::retire::{Logged, Retirer};
use super::TableId;

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
    /// One compaction slot rewrites a gibibyte in a few seconds, so for a
    /// run that small retiring the entry is worth it by itself.
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

/// Whether a compaction that holds a slot keeps the compaction of L0 tables
/// from starting: the compactor has `slots`, `taken` of them are in use,
/// `l0_in_flight` says whether a compaction of L0 tables is among them, and
/// L0 holds `l0_tables`.
///
/// The scheduler starts no L0 compaction beside another, nor before L0 has
/// its minimum of tables; the second is not looked at here, so a reclaim
/// gives way a few flushes early on a compactor that is full.
pub(crate) fn starves_l0(slots: usize, taken: usize, l0_in_flight: bool, l0_tables: usize) -> bool {
    taken >= slots && !l0_in_flight && l0_tables > 0
}

/// Why a requested reclaim did not see a compaction of its plan complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimObstacle {
    /// A compaction of the same tables was already in flight; or the proof
    /// of an entry waits for compactions in flight before it can say which
    /// tables hold its rows; or a reclaim the writer started found the
    /// compactor no longer idle. Nothing more was submitted.
    Busy,
    /// SlateDB refused the compaction, or one the scheduler started first
    /// replaced its sources.
    Rejected,
    /// The reclaim cancelled its compaction because it held the last free
    /// slot while L0 tables waited to be compacted.
    GaveWay,
    /// The caller's wait ran out. A compaction that was submitted goes on.
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

/// Who runs a reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reclaimer {
    /// A caller who asked for the bytes now, whatever they cost: every run
    /// that holds a tracked table is rewritten, beside whatever else the
    /// compactor is doing.
    Caller,
    /// The background task, because the compactor has been idle. It
    /// rewrites a run only when that is worth it by this rule, and starts a
    /// compaction only while no other is in flight: once the scheduler is
    /// compacting again the reclaim ends [`ReclaimObstacle::Busy`].
    IdleStore(Worth),
    /// The background task, because proofs have tracked tables for long on
    /// a store that is never idle. It rewrites a run only when that is worth
    /// it by this rule, and starts a compaction only while that leaves the
    /// scheduler a free slot.
    BusyStore(Worth),
}

impl Reclaimer {
    /// Whether this reclaimer rewrites a sorted run of `run_bytes` that
    /// holds `dropped_bytes` of dropped rows.
    fn rewrites(self, run_bytes: u64, dropped_bytes: u64) -> bool {
        match self {
            Self::Caller => true,
            Self::IdleStore(worth) | Self::BusyStore(worth) => {
                worth.rewriting(run_bytes, dropped_bytes)
            }
        }
    }

    /// Whether this reclaimer starts a compaction beside `others` in flight
    /// on a compactor with `slots`.
    fn starts_beside(self, others: usize, slots: usize) -> bool {
        match self {
            Self::Caller => true,
            Self::IdleStore(_) => others == 0,
            Self::BusyStore(_) => others + 1 < slots,
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

/// What `writer` leaves in place of the dropped rows its proofs track, going
/// by the manifest it holds in memory. Reads no object storage.
pub(crate) fn left_in_place(writer: &crate::HelixWriter) -> Option<LeftInPlace> {
    if !writer.dropped().tracks_tables() {
        return None;
    }
    let manifest = writer.db().manifest();
    let tracked = writer
        .dropped()
        .discardable_tracked(manifest.recent_snapshot_min_seq());
    Plan::of(
        &Layout::of(&manifest),
        &tracked,
        Reclaimer::IdleStore(Worth::WRITER),
    )
    .left
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

impl Retirer {
    /// Compacts the store so that the rows of every discardable dropped
    /// generation are gone from the tables its [`Plan`] names, waiting no
    /// later than `deadline`. Logs what it did.
    pub(crate) async fn reclaim(
        &self,
        deadline: Instant,
        reclaimer: Reclaimer,
    ) -> Result<DroppedStorageReclaim> {
        let mut logged = self.pass.lock().await;
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
        let mut manifest = self.manifest().await?;
        if armed
            .iter()
            .any(|(_, armed)| armed.gate > manifest.recent_snapshot_min_seq())
        {
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;
            manifest = self.manifest().await?;
        }
        if armed
            .iter()
            .all(|(_, armed)| armed.gate > manifest.recent_snapshot_min_seq())
        {
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
            let manifest = self.manifest().await?;
            let tracked = registry.discardable_tracked(manifest.recent_snapshot_min_seq());
            let layout = Layout::of(&manifest);
            let plan = Plan::of(&layout, &tracked, reclaimer);
            let Some(rewrite) = plan.rewrites.into_iter().find(|rewrite| {
                !rewrite
                    .sources()
                    .iter()
                    .any(|source| rewritten.contains(source))
            }) else {
                break plan.left;
            };
            let active = self.active().await?;
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
            let ended = match self
                .compacted(CompactionSpec::new(sources.clone(), destination), deadline)
                .await?
            {
                Ended::Completed => Ok(()),
                // SlateDB writes the manifest before it records a compaction
                // as completed, and a compaction that did not complete
                // leaves its sources stored: gone, they were rewritten.
                Ended::Unobserved => {
                    match Layout::of(&self.manifest().await?).holds_any(&layout.tables_of(&rewrite))
                    {
                        true => Err(ReclaimObstacle::Rejected),
                        false => Ok(()),
                    }
                }
                Ended::Short(obstacle) => Err(obstacle),
            };
            match ended {
                Ok(()) => {
                    rewritten.extend(sources);
                    rewritten_bytes += bytes;
                    dropped_bytes += dropped;
                    retired += self.look(logged).await?;
                }
                Err(obstacle) => return Ok(incomplete(obstacle)),
            }
        };
        let entries = registry.armed().len();
        Ok(match (rewritten.is_empty(), left) {
            (true, Some(left)) => DroppedStorageReclaim::Declined { entries, left },
            // Nothing tracked was there to rewrite, and entries are left:
            // their proofs wait for compactions in flight.
            (true, None) if entries > 0 && retired == 0 => incomplete(ReclaimObstacle::Busy),
            (_, left) => DroppedStorageReclaim::Compacted {
                retired,
                entries,
                rewritten_bytes,
                dropped_bytes,
                left,
            },
        })
    }

    async fn manifest(&self) -> Result<VersionedManifest> {
        let manifest = self.admin.read_manifest(None).await?.ok_or_else(|| {
            HelixDbError::InvariantViolation("an open database has no manifest".to_string())
        })?;
        if !manifest.segments().is_empty() {
            return Err(HelixDbError::InvariantViolation(
                "dropped-generation reclaim does not support segmented manifests".to_string(),
            ));
        }
        Ok(manifest)
    }

    /// Every compaction `.compactions` shows in flight.
    async fn active(&self) -> Result<Vec<Compaction>> {
        Ok(self
            .admin
            .read_compactions(None)
            .await?
            .iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .filter(|compaction| compaction.active())
            .cloned()
            .collect())
    }

    /// Submits `spec` and waits for it to finish, no later than `deadline`,
    /// cancelling it if it comes to [starve](starves_l0) the compaction of
    /// L0 tables.
    ///
    /// SlateDB refuses a compaction by marking its record failed, so a
    /// refusal is read here like any other end.
    async fn compacted(&self, spec: CompactionSpec, deadline: Instant) -> Result<Ended> {
        let mut closed = self.closed.subscribe();
        let id = self.admin.submit_compaction(spec).await?.id();
        let mut gave_way = false;
        // Imperative: each turn awaits a status read, then the poll interval.
        loop {
            if *closed.borrow() {
                return Ok(Ended::Short(ReclaimObstacle::Closed));
            }
            let compactions = self.admin.read_compactions(None).await?;
            let recent = || {
                compactions
                    .iter()
                    .flat_map(|compactions| compactions.recent_compactions())
            };
            match recent()
                .find(|compaction| compaction.id() == id)
                .map(Compaction::status)
            {
                Some(CompactionStatus::Completed) => return Ok(Ended::Completed),
                Some(CompactionStatus::Failed) => {
                    return Ok(Ended::Short(match gave_way {
                        true => ReclaimObstacle::GaveWay,
                        false => ReclaimObstacle::Rejected,
                    }));
                }
                None => return Ok(Ended::Unobserved),
                // Its output is complete: the coordinator commits it.
                Some(CompactionStatus::Compacted) => {}
                Some(
                    CompactionStatus::Submitted
                    | CompactionStatus::Scheduled
                    | CompactionStatus::Running,
                ) => {
                    let taken = recent()
                        .filter(|compaction| match compaction.status() {
                            CompactionStatus::Submitted
                            | CompactionStatus::Scheduled
                            | CompactionStatus::Running => true,
                            CompactionStatus::Compacted
                            | CompactionStatus::Completed
                            | CompactionStatus::Failed => false,
                        })
                        .count();
                    let l0_in_flight = recent().any(|compaction| {
                        compaction.active() && compaction.spec().has_l0_sources()
                    });
                    let l0_tables = self.writer().db().manifest().l0().len();
                    if starves_l0(self.slots, taken, l0_in_flight, l0_tables) {
                        tracing::info!(
                            taken,
                            l0_tables,
                            "reclaiming dropped generations: giving way to the compaction of L0 \
                             tables"
                        );
                        // Whether or not this call is the one that cancels
                        // it, the next read tells how it ended.
                        gave_way = self.admin.cancel_compaction(id).await? || gave_way;
                    }
                }
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
