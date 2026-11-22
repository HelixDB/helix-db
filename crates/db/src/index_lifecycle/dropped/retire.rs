//! Retires dropped-generation entries: deletes each one once no table of the
//! store can hold a row of it.
//!
//! The compactions the scheduler picks discard an armed entry's rows as they
//! pass, and a [`Retirer`] pass looks at the store's tables until it can
//! prove none holds such a row. A pass compacts nothing. The background task
//! that runs the passes asks for a [reclaim](super::reclaim) of the sorted
//! runs that still hold an entry's rows, once the store is idle or the rows
//! have waited long; see [`RetireSupervisor`].
//!
//! # The proof
//!
//! Take an entry armed at time *a*. Every row of it is in a table of the
//! manifest by then: a dropped generation takes no write after its entry, the
//! entry's gate is at or after the entry's commit, and
//! [`Registry::refresh`](super::Registry::refresh) flushes the memtable after
//! it reads the gate and before it arms. From then on a row of the entry
//! reaches a new table only through a compaction.
//!
//! A filter *discards* the entry when it is created with the entry armed and
//! its job's retention sequence at the gate, and *keeps* it otherwise. The
//! registry sees every filter created in this process. It counts the ones
//! that keep the entry in [`Armed::kept`](super::Armed::kept), and each one
//! puts the proof back to [`Proof::Unproven`].
//!
//! 1. **Drain.** Read `.compactions` after *a*. A compaction that read does
//!    not show active has either finished, or will be claimed later. If it
//!    finished, its output is in every manifest read afterwards: SlateDB
//!    writes the manifest before it records a compaction as completed. If it
//!    is claimed later, every filter of it is created later still, so the
//!    registry sees each: a worker creates filters only for a compaction it
//!    has claimed, and a claim is a write to `.compactions`. The compactions
//!    the read does show active may hold output a filter kept before *a*, in
//!    this process or, for a compaction resumed after a restart, in an
//!    earlier one. The proof waits until none of them is active.
//! 2. **Find.** In the manifest read after the `.compactions` read that shows
//!    them finished, find the tables that hold a row under one of the entry's
//!    owner prefixes: its *suspects*. Each table is looked into, not judged
//!    by its key range, so an entry whose rows are already gone has no
//!    suspect and retires at once. Each suspect is kept with an estimate of
//!    the stored bytes of the entry's rows in it, from its block index: a
//!    reclaim weighs them against the size of the run, and the storage
//!    report adds them up.
//! 3. **Track.** A table that enters the manifest after that read is a
//!    flushed memtable, which holds no row of the entry, or the output of a
//!    compaction every filter of which discarded the entry: had one kept it,
//!    `kept` would have moved and the proof restarted. Trivial moves and
//!    segment drains create no table. So once no suspect is left in the
//!    manifest, no table holds a row of the entry.
//! 4. **Retire.** The entry leaves the registry, provided `kept` has not
//!    moved since the pass read it, and then storage: a partitioned vector
//!    generation's mappings first, then the entry row.
//!
//! A step is applied only if `kept` is what it was before the pass read the
//! store, so a filter that keeps the entry while a pass is reading voids that
//! pass's steps.
//!
//! The proof is kept in memory. A writer that opens starts every entry at
//! [`Proof::Unproven`] and looks into every table the store then has.
//!
//! # What the proof rests on
//!
//! - Every compaction of the store runs in this process, behind this
//!   writer's filter supplier; see
//!   [`compacts_in_process`](super::compacts_in_process). A compaction run by
//!   a worker in another process would replace suspects with tables this
//!   registry never filtered, and the entry would retire with rows still
//!   stored: garbage nothing reads and nothing discards any more.
//! - The order of SlateDB's coordinator writes, manifest before
//!   `.compactions`, and that an active compaction's record stays in
//!   `.compactions` until it finishes.
//!
//! # What does not scale
//!
//! A pass with an armed entry reads `.compactions` and the manifest from
//! object storage. Finding suspects reads the block index of every table
//! whose key range reaches an owner prefix without lying wholly under it,
//! and one data block for each run of prefixes that falls inside a block; a writer that opens onto many
//! entries pays that once for all of them, in the background. What a pass
//! reads of a table it keeps in a cache of [`TABLE_CACHE_BYTES`], so a
//! proof that starts again reads a table again only once the block indexes
//! in play outgrow that cache; a block index is a few megabytes for a full
//! table.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use slatedb::admin::Admin;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::manifest::{SsTableHandle, VersionedManifest};
use slatedb::object_store::ObjectStore;
use slatedb::{SstReader, WriteBatch};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::encoding::v2::keys::scope::DataScope;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::failpoints::{self, IndexOutboxFailpoint};
use crate::index_lifecycle::DroppedGenerationRows;
use crate::HelixWriter;

use super::reclaim::{Reclaimer, Worth};
use super::{
    CompactionActivity, CompactionId, DroppedGeneration, DroppedIndexStorage, Proof, TableId,
    Tracked, Unquarantined, Waiting,
};

/// Shortest time between two background passes, so that a burst of changes
/// costs one look at the store, not one each. Also the first wait before a
/// background pass looks again at compactions a proof waits for, whose end
/// this process is not told of; each such look in a row doubles that wait,
/// up to [`DRAIN_LOOK_MAX`].
const LOOK_GAP: Duration = Duration::from_secs(5);
/// Longest wait between two looks at compactions a proof waits for.
const DRAIN_LOOK_MAX: Duration = Duration::from_secs(30);
/// First wait after a background pass that fails; each failure in a row
/// doubles it, up to [`MAX_BACKOFF`].
const RETRY: Duration = Duration::from_secs(30);
/// Longest wait after passes that fail.
const MAX_BACKOFF: Duration = Duration::from_secs(600);
/// How long the store must have been idle before the background task
/// reclaims; see [`RetireSupervisor`].
const IDLE_RECLAIM_AFTER: Duration = Duration::from_secs(300);
/// How long proofs must have tracked tables before the background task
/// reclaims a store that is not idle, and how long it then waits before it
/// tries again; see [`RetireSupervisor`].
const BUSY_RECLAIM_AFTER: Duration = Duration::from_secs(30 * 60);
/// Longest a background reclaim waits for its compactions. One that outlives
/// it goes on, and the pass after it retires what it discarded.
const RECLAIM_WAIT: Duration = Duration::from_secs(24 * 60 * 60);
/// Bytes of block indexes and data blocks a retirer keeps of the tables it
/// has looked into.
const TABLE_CACHE_BYTES: u64 = 64 * 1024 * 1024;
/// Partition mappings deleted per write.
const MAPPING_DELETE_BATCH: usize = 1024;
/// Tables one pass looks into at a time.
const TABLE_READS: usize = 16;

/// What one look at the store makes of a proof.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// These compactions, in flight when the proof started, still are.
    Drain(Waiting<CompactionId>),
    /// Every compaction the proof waited for has finished: the tables of the
    /// manifest this look read that hold a row of the entry are its
    /// suspects.
    Find,
    /// These suspects are still in the manifest.
    Track(Tracked),
    /// No table holds a row of the entry.
    Retire,
}

impl Proof {
    /// The next step of this proof, given the compactions `.compactions`
    /// shows `active` and the tables `stored` in a manifest read after it.
    fn step(&self, active: &HashSet<CompactionId>, stored: &HashSet<TableId>) -> Step {
        let waiting = match self {
            Self::Unproven => active.clone(),
            Self::Draining { compactions } => compactions
                .members()
                .intersection(active)
                .copied()
                .collect(),
            Self::Tracking { tables } => {
                return tables.still(stored).map_or(Step::Retire, Step::Track);
            }
        };
        Waiting::new(waiting).map_or(Step::Find, Step::Drain)
    }
}

/// Whether a table holds a row under one prefix, as far as its block index
/// tells.
///
/// SlateDB does not index a block by its first key but by a shortened one:
/// a leading part of the first key, just long enough to sort above every key
/// of the block before. So an index key under the prefix means the block's
/// first row is under it, while an index key below the prefix says nothing
/// of the block's rows, its first included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Held {
    /// A row under the prefix starts a block.
    Yes,
    /// The index keys leave no room for one.
    No,
    /// Only the block at this index can hold one, as any of its rows.
    InBlock(usize),
}

/// The `prefixes` a table from `first` to `last` can hold a row under, in
/// key order: the one `first` is under, and each above `first` and at or
/// below `last`. Every other prefix sorts outside the table.
///
/// A table without a first key is treated as starting below every prefix,
/// and one without a last key as ending above every prefix.
fn overlapping<'a>(
    prefixes: &'a BTreeMap<Bytes, DroppedGeneration>,
    first: Option<&[u8]>,
    last: Option<&[u8]>,
) -> impl Iterator<Item = (&'a Bytes, DroppedGeneration)> {
    // No owner prefix is a prefix of another, so the only prefix at or below
    // `first` that can cover it is its predecessor.
    let covering = first.and_then(|first| {
        prefixes
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(first)))
            .next_back()
            .filter(|(prefix, _)| first.starts_with(prefix))
    });
    covering
        .into_iter()
        .chain(prefixes.range::<[u8], _>((
            first.map_or(Bound::Unbounded, Bound::Excluded),
            last.map_or(Bound::Unbounded, Bound::Included),
        )))
        .map(|(prefix, entry)| (prefix, *entry))
}

/// Sorts out, from the first and last keys of a table alone, the `prefixes`
/// it can hold a row under. Returns the entries that own a prefix the table
/// starts or ends under, which it therefore holds a row of, and the prefixes
/// of other entries that sort strictly inside the table, which only its block
/// index can decide. Every other prefix sorts outside the table.
fn held_by_range<'a>(
    prefixes: &'a BTreeMap<Bytes, DroppedGeneration>,
    first: Option<&[u8]>,
    last: Option<&[u8]>,
) -> (
    HashSet<DroppedGeneration>,
    Vec<(&'a Bytes, DroppedGeneration)>,
) {
    // A prefix above `first` and at or below `last` covers `last`, or every
    // key under it sorts strictly between the two.
    let (ends, inside): (Vec<_>, Vec<_>) =
        overlapping(prefixes, first, last).partition(|(prefix, _)| {
            first.is_some_and(|first| first.starts_with(prefix))
                || last.is_some_and(|last| last.starts_with(prefix))
        });
    let held = ends
        .into_iter()
        .map(|(_, entry)| entry)
        .collect::<HashSet<_>>();
    let inside = inside
        .into_iter()
        .filter(|(_, entry)| !held.contains(entry))
        .collect();
    (held, inside)
}

/// The blocks whose stored bytes estimate the rows under one prefix: blocks
/// `from..to` have an index key under the prefix. When none has, the rows
/// are in the one block before them, if there is one.
///
/// The estimate is exact to one block at each end: the block before `from`
/// may end with rows under the prefix, and the last block may go on past
/// them.
fn blocks_under(from: usize, to: usize) -> std::ops::Range<usize> {
    if from < to {
        from..to
    } else {
        from.saturating_sub(1)..from.max(1)
    }
}

/// `ranges`, which are in the order of their starts, with those that
/// overlap joined: every block they cover, once.
fn disjoint(ranges: Vec<std::ops::Range<usize>>) -> Vec<std::ops::Range<usize>> {
    let mut joined = Vec::<std::ops::Range<usize>>::with_capacity(ranges.len());
    for range in ranges {
        match joined.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            Some(_) | None => joined.push(range),
        }
    }
    joined
}

/// Decides [`Held`] from a table's block index: `before` blocks have an
/// index key below `prefix`, and the next block, if any, has `next`.
fn held_by_index(prefix: &[u8], before: usize, next: Option<&[u8]>) -> Held {
    if next.is_some_and(|next| next.starts_with(prefix)) {
        return Held::Yes;
    }
    // Every later block's index key, which no row of the block sorts below,
    // is above the prefix's keys. Every earlier block ends below the index
    // key of the last block before them, which is below the prefix. So a row
    // under the prefix can only be in that last block.
    match before.checked_sub(1) {
        Some(block) => Held::InBlock(block),
        None => Held::No,
    }
}

/// What removing a quarantined dropped-generation entry on request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantinedEntryRemoval {
    /// The entry was quarantined and is deleted. Its rows stay stored.
    Removed,
    /// The entry is armed: the catalog agrees with it, compactions discard
    /// its rows, and it retires by itself. Nothing was deleted.
    Armed,
    /// No such entry is stored.
    Absent,
}

/// What a retirer last logged.
#[derive(Debug, Default)]
pub(super) struct Logged {
    /// What the last pass that logged left.
    pub(super) storage: DroppedIndexStorage,
    /// How the last reclaim ended.
    pub(super) reclaim: Option<super::reclaim::DroppedStorageReclaim>,
}

/// Watches one writer's tables and retires its dropped-generation entries,
/// one pass at a time.
pub(crate) struct Retirer {
    writer: Arc<HelixWriter>,
    pub(super) admin: Admin,
    pub(super) tables: SstReader,
    /// Serializes passes, reclaims and removals. Holds what the last pass
    /// and the last reclaim logged, so that one that changes nothing logs
    /// nothing.
    pub(super) pass: tokio::sync::Mutex<Logged>,
    pub(super) closed: watch::Sender<bool>,
    /// How many compactions the writer's compactor runs at a time; see
    /// [`compaction_slots`](super::compaction_slots).
    pub(super) slots: usize,
    /// The pace of the background task, once it may run. Contracts step a
    /// retirer by hand, so under test it waits to be told.
    #[cfg(test)]
    background: watch::Sender<Option<Pace>>,
}

impl Retirer {
    /// A retirer for `writer`, whose database lives at `path` in
    /// `object_store` and runs every compaction in this process, `slots` at
    /// a time, with the writer's [`super::Registry`] behind its filter
    /// supplier.
    pub(crate) fn new(
        writer: Arc<HelixWriter>,
        path: &str,
        object_store: Arc<dyn ObjectStore>,
        slots: usize,
    ) -> Self {
        Self {
            writer,
            slots,
            admin: Admin::builder(path, Arc::clone(&object_store)).build(),
            // A table never changes, so what is read of one stays true.
            tables: SstReader::new(
                path,
                object_store,
                Some(Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                    max_capacity: TABLE_CACHE_BYTES,
                    ..FoyerCacheOptions::default()
                }))),
                None,
            ),
            pass: tokio::sync::Mutex::new(Logged::default()),
            closed: watch::channel(false).0,
            #[cfg(test)]
            background: watch::channel(None).0,
        }
    }

    /// Lets the background task of this retirer run at `pace`.
    #[cfg(test)]
    pub(crate) fn run_in_background(&self, pace: Pace) {
        self.background.send_replace(Some(pace));
    }

    /// Deletes the stored entry of `entry` if the catalog contradicts it,
    /// as it stands now: the entry is cross-checked again first, and one
    /// that has become consistent is armed and stays.
    ///
    /// Nothing else is deleted. The rows the entry named stay stored, no
    /// compaction discards them, and no later entry can name them: see
    /// [`crate::HelixDB::remove_quarantined_dropped_index_entry`].
    pub(crate) async fn remove_quarantined(
        &self,
        entry: DroppedGeneration,
    ) -> Result<QuarantinedEntryRemoval> {
        let _pass = self.pass.lock().await;
        let db = self.writer.db();
        let registry = self.writer.dropped();
        registry.refresh(db).await?;
        // Out of the registry first, as a retirement is: a delete that fails
        // leaves the entry stored, and the next refresh quarantines it again.
        Ok(match registry.remove_quarantined(entry) {
            Unquarantined::Removed => {
                db.delete(entry.key())
                    .await
                    .map_err(HelixDbError::from_storage_commit)?;
                tracing::warn!(
                    index_id = entry.index_id.get(),
                    generation = entry.generation.get(),
                    "removed a quarantined dropped-generation entry on request: its rows stay \
                     stored"
                );
                QuarantinedEntryRemoval::Removed
            }
            Unquarantined::Armed => QuarantinedEntryRemoval::Armed,
            Unquarantined::Absent => QuarantinedEntryRemoval::Absent,
        })
    }

    /// The writer whose entries this retires.
    pub(super) fn writer(&self) -> &HelixWriter {
        &self.writer
    }

    /// Runs one pass: arms new entries, moves every armed entry's proof on,
    /// and deletes the entries whose proof is complete. Returns what is left.
    pub(crate) async fn pass(&self) -> Result<DroppedIndexStorage> {
        let mut logged = self.pass.lock().await;
        self.look(&mut logged).await?;
        Ok(logged.storage.clone())
    }

    /// One pass, by a caller that holds the pass lock, whose value is
    /// `logged`. Returns how many entries it retired.
    pub(super) async fn look(&self, logged: &mut Logged) -> Result<usize> {
        let retired = self.advance().await?;
        tracing::debug!(retired, "dropped-generation retirement pass");
        let storage = self.writer.dropped().storage();
        if retired > 0 || storage != logged.storage {
            tracing::info!(
                retired,
                unproven = storage.unproven,
                kept_by_a_compaction = storage.retained,
                draining = storage.draining,
                tracking = storage.tracking,
                tracked_tables = storage.tracked_tables,
                tracked_bytes = storage.tracked_bytes,
                quarantined = storage.quarantined.len(),
                "dropped index generations awaiting compaction"
            );
            logged.storage = storage;
        }
        Ok(retired)
    }

    async fn advance(&self) -> Result<usize> {
        let db = self.writer.db();
        let registry = self.writer.dropped();
        registry.refresh(db).await?;
        // Read before the store is: a filter that keeps an entry's rows from
        // here on moves `kept` and voids the steps below for that entry.
        let armed = registry.armed();
        if armed.is_empty() {
            return Ok(0);
        }
        // Compactions first, then the manifest; see the proof.
        let view = self.admin.read_compactor_state_view().await?;
        let active = view
            .compactions()
            .into_iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .filter(|compaction| compaction.active())
            .map(|compaction| CompactionId(compaction.id().0))
            .collect::<HashSet<_>>();
        // By file: a table several views show is looked into once.
        let tables = view
            .manifest()
            .all_sst_views()
            .map(|view| (TableId(view.sst.id.value().0), view.sst.clone()))
            .collect::<HashMap<_, _>>();
        let stored = tables.keys().copied().collect::<HashSet<_>>();
        let mut find = Vec::new();
        let mut retire = Vec::new();
        for (entry, armed) in armed {
            match armed.proof.step(&active, &stored) {
                Step::Drain(compactions) => {
                    registry.advance(entry, armed.kept, Proof::Draining { compactions });
                }
                Step::Find => find.push((entry, armed)),
                Step::Track(tables) => {
                    registry.advance(entry, armed.kept, Proof::Tracking { tables });
                }
                Step::Retire => retire.push((entry, armed)),
            }
        }
        let prefixes = find
            .iter()
            .flat_map(|(entry, armed)| {
                armed
                    .owners
                    .iter()
                    .flat_map(|owner| owner.prefixes())
                    .map(|prefix| (prefix, *entry))
            })
            .collect::<BTreeMap<_, _>>();
        let mut holding = self.holding(tables, prefixes).await?;
        for (entry, armed) in find {
            match holding.remove(&entry).and_then(Tracked::new) {
                Some(tables) => {
                    registry.advance(entry, armed.kept, Proof::Tracking { tables });
                }
                None => retire.push((entry, armed)),
            }
        }
        if retire.is_empty() {
            return Ok(0);
        }
        failpoints::trip(IndexOutboxFailpoint::DroppedEntryDeleteBefore)?;
        // Out of the registry first: an entry a filter kept since this pass
        // read `kept` stays, and one whose deletes fail below is still stored
        // and is armed again, unproven, by the next refresh.
        let retired = retire
            .into_iter()
            .filter(|(entry, armed)| registry.retire(*entry, armed.kept))
            .collect::<Vec<_>>();
        // The mappings list a partitioned generation's physical ids, so they
        // go before the entry: a crash in between leaves an entry that names
        // fewer owners, never mappings without an entry.
        for (entry, armed) in &retired {
            match armed.value.rows {
                DroppedGenerationRows::PartitionedVector => {
                    self.delete_partition_mappings(*entry, armed.value.scope)
                        .await?;
                }
                DroppedGenerationRows::UnpartitionedVector { .. }
                | DroppedGenerationRows::Secondary
                | DroppedGenerationRows::Text => {}
            }
        }
        if retired.is_empty() {
            return Ok(0);
        }
        failpoints::trip(IndexOutboxFailpoint::DroppedEntryDeleteAfterMappings)?;
        let mut entries = WriteBatch::new();
        for (entry, _) in &retired {
            entries.delete(entry.key());
        }
        db.write(entries)
            .await
            .map_err(HelixDbError::from_storage_commit)?;
        Ok(retired.len())
    }

    /// Those of `tables` that hold a row under one of `prefixes`, by the
    /// entry that owns the prefix, each with an estimate of the stored bytes
    /// of that entry's rows in it. An entry no table holds a row of is
    /// absent.
    async fn holding(
        &self,
        tables: HashMap<TableId, SsTableHandle>,
        prefixes: BTreeMap<Bytes, DroppedGeneration>,
    ) -> Result<HashMap<DroppedGeneration, HashMap<TableId, u64>>> {
        if prefixes.is_empty() {
            return Ok(HashMap::new());
        }
        let prefixes = Arc::new(prefixes);
        let held = futures::stream::iter(tables)
            .map(|(table, sst): (TableId, SsTableHandle)| {
                let prefixes = Arc::clone(&prefixes);
                async move { Ok::<_, HelixDbError>((table, self.held(&sst, &prefixes).await?)) }
            })
            .buffer_unordered(TABLE_READS)
            .try_collect::<Vec<_>>()
            .await?;
        let mut holding = HashMap::<_, HashMap<_, _>>::new();
        for (table, entries) in held {
            for (entry, bytes) in entries {
                holding.entry(entry).or_default().insert(table, bytes);
            }
        }
        Ok(holding)
    }

    /// The entries that own a prefix under which the table `sst` holds a row,
    /// each with an estimate of the stored bytes of its rows in the table.
    ///
    /// Reads nothing of a table that sorts outside every prefix, or wholly
    /// under one. Of any other it reads the block index, and a data block
    /// only when a prefix sorts strictly inside that block.
    async fn held(
        &self,
        sst: &SsTableHandle,
        prefixes: &BTreeMap<Bytes, DroppedGeneration>,
    ) -> Result<HashMap<DroppedGeneration, u64>> {
        let first = sst.info.first_entry.as_deref();
        let last = sst.info.last_entry.as_deref();
        let (mut held, inside) = held_by_range(prefixes, first, last);
        if held.is_empty() && inside.is_empty() {
            return Ok(HashMap::new());
        }
        let whole = overlapping(prefixes, first, last)
            .next()
            .filter(|(prefix, _)| {
                first.is_some_and(|first| first.starts_with(prefix))
                    && last.is_some_and(|last| last.starts_with(prefix))
            });
        if let Some((_, entry)) = whole {
            return Ok(HashMap::from([(entry, sst.estimate_size())]));
        }
        let file = self.tables.open_with_handle(sst.clone())?;
        let index = file.index().await?;
        let mut in_blocks = BTreeMap::<usize, Vec<(&Bytes, DroppedGeneration)>>::new();
        for (prefix, entry) in inside {
            let before = index.partition_point(|start| start < &prefix[..]);
            match held_by_index(prefix, before, index.get(before).map(|(_, start)| start)) {
                Held::Yes => {
                    held.insert(entry);
                }
                Held::No => {}
                Held::InBlock(block) => in_blocks.entry(block).or_default().push((prefix, entry)),
            }
        }
        // Imperative: each block is read only while one of its prefixes
        // belongs to an entry the table is not yet known to hold.
        for (block, prefixes) in in_blocks {
            if prefixes.iter().all(|(_, entry)| held.contains(entry)) {
                continue;
            }
            let rows = file.read_block(block).await?;
            held.extend(
                prefixes
                    .into_iter()
                    .filter(|(prefix, _)| {
                        let at = rows.partition_point(|row| row.key[..] < prefix[..]);
                        rows.get(at).is_some_and(|row| row.key.starts_with(prefix))
                    })
                    .map(|(_, entry)| entry),
            );
        }
        // The data blocks end where the filter or the index starts.
        let info = &sst.info;
        let data_end = match info.filter_len {
            0 => info.index_offset,
            _ => info.index_offset.min(info.filter_offset),
        };
        let offset = |block| index.get(block).map_or(data_end, |(offset, _)| offset);
        // The blocks of each held entry, in key order. Two prefixes of one
        // entry can share a block, which counts once.
        let mut blocks = HashMap::<_, Vec<std::ops::Range<usize>>>::new();
        for (prefix, entry) in overlapping(prefixes, first, last) {
            if !held.contains(&entry) {
                continue;
            }
            let from = index.partition_point(|start| start < &prefix[..]);
            let to = index
                .partition_point(|start| start < &prefix[..] || start.starts_with(&prefix[..]));
            blocks
                .entry(entry)
                .or_default()
                .push(blocks_under(from, to));
        }
        Ok(blocks
            .into_iter()
            .map(|(entry, blocks)| {
                let bytes = disjoint(blocks)
                    .into_iter()
                    .map(|blocks| offset(blocks.end).saturating_sub(offset(blocks.start)))
                    .sum();
                (entry, bytes)
            })
            .collect())
    }

    /// Deletes the partition mappings a retired partitioned generation's
    /// drop left in place.
    async fn delete_partition_mappings(
        &self,
        entry: DroppedGeneration,
        scope: DataScope,
    ) -> Result<()> {
        let db = self.writer.db();
        let prefix = super::partition_mappings_prefix(scope, entry);
        loop {
            let mut rows = db.scan_prefix(prefix.clone(), ..).await?;
            let mut mappings = WriteBatch::new();
            let mut staged = 0;
            while staged < MAPPING_DELETE_BATCH {
                let Some(row) = rows.next().await? else {
                    break;
                };
                mappings.delete(row.key);
                staged += 1;
            }
            drop(rows);
            if staged == 0 {
                return Ok(());
            }
            db.write(mappings)
                .await
                .map_err(HelixDbError::from_storage_commit)?;
        }
    }
}

/// The id of every table in `manifest`.
fn table_ids(manifest: &VersionedManifest) -> HashSet<TableId> {
    manifest
        .all_sst_views()
        .map(|view| TableId(view.sst.id.value().0))
        .collect()
}

/// How often the background task acts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pace {
    /// Shortest time between two passes; see [`LOOK_GAP`].
    pub(crate) look_gap: Duration,
    /// How long the store must be idle before it is reclaimed; see
    /// [`IDLE_RECLAIM_AFTER`].
    pub(crate) idle_after: Duration,
    /// How long proofs must track tables before a store that is not idle is
    /// reclaimed, and between two such reclaims; see [`BUSY_RECLAIM_AFTER`].
    pub(crate) busy_after: Duration,
}

impl Pace {
    /// The pace of a writer.
    const WRITER: Self = Self {
        look_gap: LOOK_GAP,
        idle_after: IDLE_RECLAIM_AFTER,
        busy_after: BUSY_RECLAIM_AFTER,
    };
}

/// What the background task waits for besides being told of a change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Timer {
    /// Nothing: this process is told of whatever each entry waits for.
    Off,
    /// A pass at `at`: a proof waits for compactions that `.compactions`
    /// showed in flight, whose end this process is not told of.
    Look { at: Instant },
    /// A reclaim at `at`: of an idle store if the store is idle until then,
    /// which is that no compaction job of this process has started or ended
    /// since `compactions` was read and none is running; of a busy store if
    /// it is not and `at` has reached `overdue`.
    Reclaim {
        at: Instant,
        compactions: CompactionActivity,
        overdue: Instant,
    },
    /// A pass at `at`, after one that failed.
    Retry { at: Instant },
}

impl Timer {
    /// The timer after a pass that left `storage`.
    ///
    /// A proof that is not tracking tables waits for compactions, so the
    /// store is looked at again after `drain_look`. Once every proof tracks
    /// tables, only a compaction can move one on: the scheduler's, which
    /// this process is told of, or a reclaim, which is due after
    /// `idle_after` of idleness, or at `overdue` without it, unless one has
    /// `reclaimed` since the store last changed.
    fn after_pass(
        storage: &DroppedIndexStorage,
        compactions: CompactionActivity,
        reclaimed: bool,
        now: Instant,
        drain_look: Duration,
        idle_after: Duration,
        overdue: Instant,
    ) -> Self {
        if storage.unproven + storage.draining > 0 {
            Self::Look {
                at: now + drain_look,
            }
        } else if storage.tracking > 0 && !reclaimed {
            Self::Reclaim {
                at: (now + idle_after).min(overdue.max(now)),
                compactions,
                overdue,
            }
        } else {
            Self::Off
        }
    }

    /// When the timer fires, if it is on.
    fn at(self) -> Option<Instant> {
        match self {
            Self::Off => None,
            Self::Look { at } | Self::Reclaim { at, .. } | Self::Retry { at } => Some(at),
        }
    }

    /// What a reclaim timer that fires at `now` does, given what the
    /// compactor has done by then; `None` of any other timer.
    fn due(self, now: Instant, activity: CompactionActivity, worth: Worth) -> Option<Due> {
        let Self::Reclaim {
            compactions,
            overdue,
            ..
        } = self
        else {
            return None;
        };
        Some(if activity == compactions && activity.running == 0 {
            Due::Reclaim(Reclaimer::IdleStore(worth))
        } else if now >= overdue {
            Due::Reclaim(Reclaimer::BusyStore(worth))
        } else {
            Due::Later
        })
    }
}

/// What a reclaim timer that fires does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Due {
    /// This reclaim.
    Reclaim(Reclaimer),
    /// Nothing yet: a compaction ran meanwhile and the proofs have not
    /// tracked tables for long enough, so idle starts again.
    Later,
}

/// What the background task does when it wakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wake {
    /// A retirement pass.
    Pass,
    /// A reclaim, which ends with a pass.
    Reclaim(Reclaimer),
}

impl Wake {
    /// When a store that is not idle is next reclaimed, after this wake:
    /// `restarted` if the wait of a busy store starts again, `overdue` as it
    /// was otherwise. The wait starts when proofs come to track tables, so
    /// with a pass before which none `tracked` any, and again with each
    /// reclaim, whatever it achieved.
    fn overdue(self, tracked: bool, overdue: Instant, restarted: Instant) -> Instant {
        match (self, tracked) {
            (Self::Reclaim(_), _) | (Self::Pass, false) => restarted,
            (Self::Pass, true) => overdue,
        }
    }
}

/// The background retirement task of one writer, retained by the database
/// handle and stopped before storage closes.
///
/// # When it looks at the store
///
/// The task reads object storage only when something a pass depends on has
/// changed, and it is told of each such change in this process:
///
/// - the registry: an index operation ended, which may have stored an entry
///   or moved the catalog a quarantined entry is checked against; a filter
///   kept an armed entry's rows; or a compaction of this process ended while
///   a proof was not yet tracking tables
///   ([`Registry::changed`](super::Registry::changed));
/// - the manifest: the writer's own copy, which SlateDB keeps current, has
///   lost a table some proof tracks. While a proof tracks tables the task
///   compares them with that copy each time the writer's status moves, which
///   is every write: memory only, linear in the tables of the store.
///
/// One thing it is not told of is the end of a compaction that was in flight
/// when a proof started, so while a proof waits for one the task looks again
/// by a timer, [`LOOK_GAP`] doubling to [`DRAIN_LOOK_MAX`]. That wait ends
/// with the compaction. A pass that fails is tried again by a timer too.
/// With every proof tracking tables, or only quarantined entries stored, the
/// task sleeps until it is told of a change and reads nothing.
///
/// # When it reclaims
///
/// An entry whose proof tracks tables waits for compactions to rewrite them.
/// The scheduler compacts only as writes arrive, and rewrites its oldest and
/// largest run rarely however many arrive, so the entry, and the bytes of
/// its rows, could stay for long. The task therefore runs the
/// [reclaim](super::reclaim) a caller could ask for, limited to the sorted
/// runs worth rewriting ([`Worth::WRITER`]), in two cases.
///
/// **The store is idle**, for [`IDLE_RECLAIM_AFTER`]. Idle means that the
/// compactor has had nothing to do for that long: no compaction job of this
/// process started or ended, and none is running. Every job creates its
/// filters through this writer's registry, so that is known without reading
/// object storage. It is the compactor a reclaim competes with, not writes.
/// The scheduler starts a compaction within seconds of the flush that makes
/// one due, so a compactor that has rested for minutes belongs to a store
/// whose writes fill a memtable more slowly than that, or not at all: such a
/// store is reclaimed, writes or no. This reclaim starts a compaction only
/// while no other is in flight; see
/// [`Reclaimer::IdleStore`](super::reclaim::Reclaimer::IdleStore).
///
/// **The store is never idle**, and proofs have tracked tables for
/// [`BUSY_RECLAIM_AFTER`]. A store under load compacts every few seconds,
/// and its ordinary compactions retire most entries within minutes: the
/// rows that outlast that wait are in a run the scheduler is not about to
/// rewrite. This reclaim starts a compaction only while that leaves the
/// scheduler a free slot; see
/// [`Reclaimer::BusyStore`](super::reclaim::Reclaimer::BusyStore). It is
/// tried again after the same wait for as long as proofs track tables.
///
/// Either reclaim gives its slot back when the compaction of L0 tables needs
/// it; see [`ReclaimObstacle::GaveWay`](super::reclaim::ReclaimObstacle).
///
/// One reclaim runs per idle period. Whatever it achieved, the next waits
/// until the store has changed and been idle again, so a reclaim that
/// cannot make progress, for instance while a reader older than the drop
/// is open or because no run is worth rewriting, is not repeated on a store
/// that stays still.
///
/// # What does not scale
///
/// A reclaim rewrites whole sorted runs. What a drop costs to reclaim grows
/// with the runs its rows are in, not with the rows: a large index in the
/// oldest run costs a rewrite of that run, and a small index in it is left
/// in place, for as long as the scheduler leaves the run alone, unless a
/// caller asks; see [`LeftInPlace`](super::reclaim::LeftInPlace).
///
/// While a run is rewritten the scheduler cannot merge it with others, so
/// the runs newer than it wait to be merged into it until the reclaim has
/// ended, and reads look through that many more runs until then.
///
/// A reclaim that gives way loses what its compaction had written, and
/// starts the run again from its first byte when it next runs.
pub(crate) struct RetireSupervisor {
    retirer: Arc<Retirer>,
    handle: JoinHandle<()>,
}

/// A supervisor dropped without [`RetireSupervisor::stop`] still ends its
/// task, which would otherwise keep the writer alive.
impl Drop for RetireSupervisor {
    fn drop(&mut self) {
        self.retirer.closed.send_replace(true);
        self.handle.abort();
    }
}

impl RetireSupervisor {
    /// Starts the background task of `retirer`.
    pub(crate) fn start(retirer: Retirer) -> Self {
        let retirer = Arc::new(retirer);
        let handle = tokio::spawn(Self::run(Arc::clone(&retirer)));
        Self { retirer, handle }
    }

    async fn run(retirer: Arc<Retirer>) {
        let mut closed = retirer.closed.subscribe();
        #[cfg(not(test))]
        let pace = Pace::WRITER;
        #[cfg(test)]
        let pace = {
            let mut background = retirer.background.subscribe();
            let pace = tokio::select! {
                pace = background.wait_for(Option::is_some) => pace.ok().and_then(|pace| *pace),
                _ = closed.wait_for(|closed| *closed) => None,
            };
            let Some(pace) = pace else {
                return;
            };
            pace
        };
        let Pace {
            look_gap,
            idle_after,
            busy_after,
        } = pace;
        let registry = retirer.writer.dropped();
        let mut status = retirer.writer.db().subscribe();
        // The tables of the manifest the writer held when last compared
        // with the tables the proofs track.
        let mut stored = None::<HashSet<TableId>>;
        let mut looked = None::<Instant>;
        let mut drain_look = look_gap;
        let mut retry = RETRY;
        // Whether a reclaim has run since the store last changed.
        let mut reclaimed = false;
        // When a store that is not idle is next reclaimed: `busy_after`
        // after its proofs came to track tables, or after its last reclaim.
        let mut overdue = Instant::now() + busy_after;
        // The open cross-checked every stored entry, so the first pass is
        // due at once if one was armed, and not at all otherwise.
        let mut timer = Timer::after_pass(
            &registry.storage(),
            registry.compactions(),
            reclaimed,
            Instant::now(),
            Duration::ZERO,
            idle_after,
            overdue,
        );
        while !*closed.borrow() {
            // Imperative: waits until this process is told of a change a
            // pass depends on, or the timer fires.
            let wake = loop {
                tokio::select! {
                    biased;
                    _ = closed.changed() => return,
                    () = registry.changed() => {
                        reclaimed = false;
                        drain_look = look_gap;
                        break Wake::Pass;
                    }
                    changed = status.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        // The writer's status moves with every write. Its
                        // manifest matters only while a proof tracks tables,
                        // and its id is no guide to a change: the writer
                        // changes its copy before it stores it under a new
                        // one.
                        if !registry.tracks_tables() {
                            continue;
                        }
                        let now_stored = table_ids(&status.borrow_and_update().current_manifest);
                        if stored.as_ref() == Some(&now_stored) {
                            continue;
                        }
                        let left = registry.tracked_table_left(&now_stored);
                        stored = Some(now_stored);
                        reclaimed = false;
                        if left {
                            drain_look = look_gap;
                            break Wake::Pass;
                        }
                        // The store changed, so a reclaim may make progress
                        // again. A reclaim already due stays due: a table
                        // that only enters the manifest gives the compactor
                        // no work by itself.
                        if timer == Timer::Off {
                            timer = Timer::after_pass(
                                &registry.storage(),
                                registry.compactions(),
                                reclaimed,
                                Instant::now(),
                                drain_look,
                                idle_after,
                                overdue,
                            );
                        }
                    }
                    () = async {
                        match timer.at() {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    } => match timer {
                        Timer::Off => {}
                        Timer::Look { .. } => {
                            drain_look = (drain_look * 2).min(DRAIN_LOOK_MAX);
                            break Wake::Pass;
                        }
                        Timer::Retry { .. } => break Wake::Pass,
                        Timer::Reclaim { overdue, .. } => {
                            let now = Instant::now();
                            let activity = registry.compactions();
                            match timer.due(now, activity, Worth::WRITER) {
                                Some(Due::Reclaim(reclaimer)) => break Wake::Reclaim(reclaimer),
                                Some(Due::Later) | None => {
                                    timer = Timer::Reclaim {
                                        at: (now + idle_after).min(overdue),
                                        compactions: activity,
                                        overdue,
                                    };
                                }
                            }
                        }
                    },
                }
            };
            if let Some(looked) = looked {
                tokio::select! {
                    () = tokio::time::sleep_until(looked + look_gap) => {}
                    _ = closed.changed() => return,
                }
            }
            let compactions = registry.compactions();
            let tracked = registry.tracks_tables();
            let outcome = match wake {
                Wake::Pass => retirer.pass().await,
                Wake::Reclaim(reclaimer) => {
                    reclaimed = true;
                    retirer
                        .reclaim(Instant::now() + RECLAIM_WAIT, reclaimer)
                        .await
                        .map(|_| registry.storage())
                }
            };
            let now = Instant::now();
            looked = Some(now);
            overdue = wake.overdue(tracked, overdue, now + busy_after);
            timer = match outcome {
                Ok(storage) => {
                    retry = RETRY;
                    Timer::after_pass(
                        &storage,
                        compactions,
                        reclaimed,
                        now,
                        drain_look,
                        idle_after,
                        overdue,
                    )
                }
                Err(error) => {
                    if !*closed.borrow() {
                        tracing::warn!(%error, "dropped-generation retirement pass failed");
                    }
                    let at = now + retry;
                    retry = (retry * 2).min(MAX_BACKOFF);
                    Timer::Retry { at }
                }
            };
        }
    }

    /// The retirer, for a pass or a reclaim on request.
    pub(crate) fn retirer(&self) -> Arc<Retirer> {
        Arc::clone(&self.retirer)
    }

    /// Ends the background task and any reclaim in flight, before storage
    /// closes.
    pub(crate) async fn stop(mut self) {
        self.retirer.closed.send_replace(true);
        match (&mut self.handle).await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                tracing::warn!(%error, "dropped-generation retirement task failed during shutdown");
            }
        }
        // A requested reclaim ends at its next status read.
        drop(self.retirer.pass.lock().await);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "../../../tests/unit/dropped_storage_reclaim.rs"]
mod contracts;
