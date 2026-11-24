//! Retires dropped-generation entries: deletes each one once no table of the
//! store can hold a row of it.
//!
//! The compactions the scheduler picks discard an armed entry's rows as they
//! pass, and a [`Retirer`] pass looks at the store's tables until it can
//! prove none holds such a row. A pass compacts nothing. The background task
//! that runs the passes asks for a [reclaim](super::reclaim) of the sorted
//! runs that still hold an entry's rows once the store is idle; see
//! [`RetireSupervisor`].
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
//!   [`compaction_slots`](super::compaction_slots). A compaction run by
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
//! and one data block for each run of prefixes that falls inside a block; a
//! writer that opens onto many entries pays that once for all of them, in
//! the background. What a pass reads of a table it keeps in a cache of
//! [`TABLE_CACHE_BYTES`], so a proof that starts again reads a table again
//! only once the block indexes in play outgrow that cache; a block index is
//! a few megabytes for a full table.

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
use slatedb::{IsolationLevel, SstReader, WriteBatch};
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
    CompactionActivity, CompactionId, DroppedGeneration, DroppedIndexStorage, Proof, StoredEntry,
    TableId, Tracked, Unquarantined, Waiting,
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
/// Longest a background reclaim waits for its compactions. The task runs no
/// pass while it waits, so a drop committed meanwhile is armed, counted and
/// reported once the wait is over: minutes, not the hours a large run takes
/// to rewrite. A compaction that outlives the wait goes on, and the pass its
/// end starts retires what it discarded.
pub(super) const RECLAIM_WAIT: Duration = Duration::from_secs(120);
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
/// The estimate is off by up to one block at each end. It is short when the
/// block before `from` ends with rows under the prefix, and long when the
/// last block goes on past them, or when the one block before holds other
/// rows too.
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
    ranges
        .into_iter()
        .fold(Vec::<std::ops::Range<usize>>::new(), |mut joined, range| {
            match joined.last_mut() {
                Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
                Some(_) | None => joined.push(range),
            }
            joined
        })
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
    /// A background pass, or a reclaim looking at the store, was under way
    /// for the whole wait, or commits to the index catalog kept overtaking
    /// the removal until the wait was over. Nothing was deleted.
    Busy,
}

/// Test pause of a removal between staging its delete and committing it.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct RemovalPause {
    armed: std::sync::atomic::AtomicBool,
    staged: tokio::sync::Notify,
    released: tokio::sync::Notify,
}

#[cfg(test)]
impl RemovalPause {
    /// Holds the next removal once it has staged its delete.
    pub(crate) fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Waits until the armed removal has staged its delete and is held.
    pub(crate) async fn staged(&self) {
        self.staged.notified().await;
    }

    /// Lets the held removal go on to commit.
    pub(crate) fn release(&self) {
        self.released.notify_one();
    }

    async fn hold(&self) {
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.staged.notify_one();
            self.released.notified().await;
        }
    }
}

/// What a retirer's passes last logged.
#[derive(Debug, Default)]
pub(super) struct Logged {
    /// What the last pass that logged left.
    pub(super) storage: DroppedIndexStorage,
}

/// Watches one writer's tables and retires its dropped-generation entries,
/// one pass at a time.
pub(crate) struct Retirer {
    writer: Arc<HelixWriter>,
    pub(super) admin: Admin,
    pub(super) tables: SstReader,
    /// Serializes passes, removals, and the looking, planning and
    /// submitting of reclaims. Nothing holds it while a compaction runs, so
    /// it is held for a pass's worth of work at most. Holds what the last
    /// pass logged, so that one that changes nothing logs nothing.
    pub(super) pass: tokio::sync::Mutex<Logged>,
    /// How the last reclaim ended, so that one that ends the same way logs
    /// nothing.
    pub(super) reclaimed: parking_lot::Mutex<Option<super::reclaim::DroppedStorageReclaim>>,
    pub(super) closed: watch::Sender<bool>,
    /// How many compactions the writer's compactor runs at a time; see
    /// [`compaction_slots`](super::compaction_slots).
    pub(super) slots: usize,
    /// The pace of the background task, once it may run. Contracts step a
    /// retirer by hand, so under test it waits to be told.
    #[cfg(test)]
    background: watch::Sender<Option<Pace>>,
    /// Holds a removal before its commit, for the contract that commits a
    /// drop in between.
    #[cfg(test)]
    pub(crate) removal: RemovalPause,
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
            reclaimed: parking_lot::Mutex::new(None),
            closed: watch::channel(false).0,
            #[cfg(test)]
            background: watch::channel(None).0,
            #[cfg(test)]
            removal: RemovalPause::default(),
        }
    }

    /// Lets the background task of this retirer run at `pace`.
    #[cfg(test)]
    pub(crate) fn run_in_background(&self, pace: Pace) {
        self.background.send_replace(Some(pace));
    }

    /// Deletes the stored entry of `entry` if the catalog contradicts it,
    /// as it stands now: the entry is cross-checked again first, and one
    /// that has become consistent is armed and stays. Waits no later than
    /// `deadline` for a pass under way, and does nothing if it must.
    ///
    /// The entry is read, cross-checked and deleted in one serializable
    /// transaction ([`super::stored_entry`]). The step that completes a drop
    /// or an abort stores the generation's real entry under the same key
    /// without the pass lock: one that commits first is either read here as
    /// consistent, or makes this transaction conflict, and the removal looks
    /// again until `deadline`. The delete therefore never lands on an entry
    /// the removal did not find contradicted.
    ///
    /// Nothing else is deleted. The rows the entry named stay stored, no
    /// compaction discards them, and no later entry can name them: see
    /// [`crate::HelixDB::remove_quarantined_dropped_index_entry`].
    pub(crate) async fn remove_quarantined(
        &self,
        entry: DroppedGeneration,
        deadline: Instant,
    ) -> Result<QuarantinedEntryRemoval> {
        let Ok(_pass) = tokio::time::timeout_at(deadline, self.pass.lock()).await else {
            return Ok(QuarantinedEntryRemoval::Busy);
        };
        let db = self.writer.db();
        let registry = self.writer.dropped();
        registry.refresh(db).await?;
        // Out of the registry first, as a retirement is: a delete that fails
        // leaves the entry stored, and the next refresh quarantines it again.
        match registry.remove_quarantined(entry) {
            Unquarantined::Removed => {}
            Unquarantined::Armed => return Ok(QuarantinedEntryRemoval::Armed),
            Unquarantined::Absent => return Ok(QuarantinedEntryRemoval::Absent),
        }
        // Imperative: each turn is one transaction, and a conflict starts
        // the next.
        loop {
            let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
            match super::stored_entry(&transaction, entry).await? {
                StoredEntry::Contradicted => {}
                // A drop or an abort stored the generation's entry since the
                // refresh: it is armed, and stays.
                StoredEntry::Consistent => {
                    drop(transaction);
                    registry.refresh(db).await?;
                    return Ok(QuarantinedEntryRemoval::Armed);
                }
                StoredEntry::Absent => return Ok(QuarantinedEntryRemoval::Absent),
            }
            transaction.delete(entry.key())?;
            #[cfg(test)]
            self.removal.hold().await;
            match transaction
                .commit()
                .await
                .map_err(HelixDbError::from_storage_commit)
            {
                Ok(_) => break,
                Err(error) if error.is_transaction_conflict() => {
                    if Instant::now() >= deadline {
                        // Still stored: quarantined again, or armed.
                        registry.refresh(db).await?;
                        return Ok(QuarantinedEntryRemoval::Busy);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        tracing::warn!(
            index_id = entry.index_id.get(),
            generation = entry.generation.get(),
            "removed a quarantined dropped-generation entry on request: its rows stay stored"
        );
        Ok(QuarantinedEntryRemoval::Removed)
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
        let (ends, inside) = held_by_range(prefixes, first, last);
        if ends.is_empty() && inside.is_empty() {
            return Ok(HashMap::new());
        }
        let whole = overlapping(prefixes, first, last)
            .next()
            .filter(|(prefix, _)| {
                first.is_some_and(|first| first.starts_with(prefix))
                    && last.is_some_and(|last| last.starts_with(prefix))
            });
        match whole {
            // Every row of the table is the entry's.
            Some((_, entry)) => Ok(HashMap::from([(entry, sst.estimate_size())])),
            None => self.held_in_part(sst, prefixes, ends).await,
        }
    }

    /// [`Self::held`] for a table that reaches a prefix without lying wholly
    /// under one: `ends` own a prefix the table starts or ends under.
    ///
    /// The estimate counts, for each prefix found to have rows in the table,
    /// the blocks those rows are in: exact to one block at each end. Every
    /// prefix the table can hold a row under is weighed, those of an entry
    /// in `ends` included. A prefix with no row in the table adds nothing.
    /// Neither does one whose rows all lie inside a block that begins with
    /// another owner's row, when its entry is known to be held without it:
    /// that block is not read, so the estimate is short by less than a block
    /// for each such prefix.
    ///
    /// It can also be long, by less than a block for each prefix: the last
    /// block of a prefix counts in full though it goes on with other rows,
    /// and so does a block a prefix is found inside. A block two entries
    /// have rows in counts in full for each, so the sum over a table's
    /// entries can exceed the table: a reclaim's plan counts a table for no
    /// more than its size.
    async fn held_in_part(
        &self,
        sst: &SsTableHandle,
        prefixes: &BTreeMap<Bytes, DroppedGeneration>,
        ends: HashSet<DroppedGeneration>,
    ) -> Result<HashMap<DroppedGeneration, u64>> {
        let first = sst.info.first_entry.as_deref();
        let last = sst.info.last_entry.as_deref();
        let file = self.tables.open_with_handle(sst.clone())?;
        let index = file.index().await?;
        // The blocks each entry is known to have rows in, one range for
        // each of its prefixes found there.
        let mut blocks = HashMap::<DroppedGeneration, Vec<std::ops::Range<usize>>>::new();
        // The blocks only a read can decide, with the prefixes it decides.
        let mut in_blocks = BTreeMap::<usize, Vec<(&Bytes, DroppedGeneration)>>::new();
        for (prefix, entry) in overlapping(prefixes, first, last) {
            // Blocks `from..to` have an index key under the prefix.
            let from = index.partition_point(|start| start < &prefix[..]);
            let to = index
                .partition_point(|start| start < &prefix[..] || start.starts_with(&prefix[..]));
            // A prefix the table starts or ends under has rows in it; of any
            // other the block index says.
            let starts_under = first.is_some_and(|first| first.starts_with(prefix));
            let ends_under = last.is_some_and(|last| last.starts_with(prefix));
            let rows = match starts_under || ends_under {
                true => Held::Yes,
                false => held_by_index(prefix, from, index.get(from).map(|(_, start)| start)),
            };
            match rows {
                // The table's first row is in its first block, whatever
                // shortened key indexes that block.
                Held::Yes if starts_under => blocks.entry(entry).or_default().push(0..to.max(1)),
                Held::Yes => blocks
                    .entry(entry)
                    .or_default()
                    .push(blocks_under(from, to)),
                Held::No => {}
                // Held whatever the block has: it is not read for this.
                Held::InBlock(_) if ends.contains(&entry) => {}
                Held::InBlock(block) => in_blocks.entry(block).or_default().push((prefix, entry)),
            }
        }
        // Imperative: each block is read only while one of its prefixes
        // belongs to an entry the table is not yet known to hold.
        for (block, prefixes) in in_blocks {
            if prefixes.iter().all(|(_, entry)| blocks.contains_key(entry)) {
                continue;
            }
            let rows = file.read_block(block).await?;
            for (prefix, entry) in prefixes {
                let at = rows.partition_point(|row| row.key[..] < prefix[..]);
                if rows.get(at).is_some_and(|row| row.key.starts_with(prefix)) {
                    blocks.entry(entry).or_default().push(block..block + 1);
                }
            }
        }
        // The data blocks end where the filter or the index starts.
        let info = &sst.info;
        let data_end = match info.filter_len {
            0 => info.index_offset,
            _ => info.index_offset.min(info.filter_offset),
        };
        let offset = |block| index.get(block).map_or(data_end, |(offset, _)| offset);
        Ok(blocks
            .into_iter()
            .map(|(entry, mut blocks)| {
                // Two prefixes of one entry can share a block, which counts
                // once.
                blocks.sort_unstable_by_key(|blocks| blocks.start);
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

/// What differs between two manifests whenever their tables do, read in time
/// linear in the sorted runs, not in the tables: how many L0 tables there
/// are, which is newest, and which table each sorted run begins with.
///
/// The L0 tables change by a flush, which puts a new table at their head,
/// or by a compaction, which takes tables away. The tables of a sorted run
/// change only when a compaction writes the run, and then every one of them
/// is new, the first included; a run a compaction leaves without a row is
/// gone or empty. Nothing else changes the tables: trivial moves are off;
/// see [`compactor_options`](super::compactor_options).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Shape {
    l0: (usize, Option<TableId>),
    runs: Vec<Option<TableId>>,
}

impl Shape {
    pub(super) fn of(manifest: &VersionedManifest) -> Self {
        let table = |view: &slatedb::manifest::SsTableView| TableId(view.sst.id.value().0);
        Self::of_tables(
            manifest.l0().iter().map(table),
            manifest
                .compacted()
                .iter()
                .map(|run| run.sst_views().iter().map(table)),
        )
    }

    /// The shape of a store whose L0 tables are `l0`, newest first, and
    /// whose sorted runs hold `runs`, each in the order of its tables.
    fn of_tables<L, R, T>(mut l0: L, runs: R) -> Self
    where
        L: ExactSizeIterator<Item = TableId>,
        R: Iterator<Item = T>,
        T: Iterator<Item = TableId>,
    {
        Self {
            l0: (l0.len(), l0.next()),
            runs: runs.map(|mut tables| tables.next()).collect(),
        }
    }
}

/// How often the background task acts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pace {
    /// Shortest time between two passes; see [`LOOK_GAP`].
    pub(crate) look_gap: Duration,
    /// How long the store must be idle before it is reclaimed; see
    /// [`IDLE_RECLAIM_AFTER`].
    pub(crate) idle_after: Duration,
}

impl Pace {
    /// The pace of a writer.
    const WRITER: Self = Self {
        look_gap: LOOK_GAP,
        idle_after: IDLE_RECLAIM_AFTER,
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
    /// A reclaim at `at`, if the store is idle until then: no compaction job
    /// of this process has started or ended since `compactions` was read,
    /// and none is running. Otherwise idle starts again.
    Reclaim {
        at: Instant,
        compactions: CompactionActivity,
    },
    /// A pass at `at`: the first of a writer, which arms the entries it
    /// opened onto, or one after a pass that failed.
    Pass { at: Instant },
}

impl Timer {
    /// The timer after a pass that left `storage`.
    ///
    /// A proof that is not tracking tables waits for compactions, so the
    /// store is looked at again after `drain_look`. Once every proof tracks
    /// tables, only a compaction can move one on: the scheduler's, which
    /// this process is told of, or a reclaim, which is due after
    /// `idle_after` of idleness unless one has `reclaimed` since the store
    /// last changed.
    fn after_pass(
        storage: &DroppedIndexStorage,
        compactions: CompactionActivity,
        reclaimed: bool,
        now: Instant,
        drain_look: Duration,
        idle_after: Duration,
    ) -> Self {
        if storage.unproven + storage.draining > 0 {
            Self::Look {
                at: now + drain_look,
            }
        } else if storage.tracking > 0 && !reclaimed {
            Self::Reclaim {
                at: now + idle_after,
                compactions,
            }
        } else {
            Self::Off
        }
    }

    /// When the timer fires, if it is on.
    fn at(self) -> Option<Instant> {
        match self {
            Self::Off => None,
            Self::Look { at } | Self::Reclaim { at, .. } | Self::Pass { at } => Some(at),
        }
    }
}

/// Whether the compactor has been idle since it had done `seen`, having
/// done `activity` by now: nothing started or ended in between, and nothing
/// runs.
fn idle_since(seen: CompactionActivity, activity: CompactionActivity) -> bool {
    activity == seen && activity.running == 0
}

/// What the background task does when it wakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wake {
    /// A retirement pass.
    Pass,
    /// A reclaim of the idle store, which ends with a pass.
    Reclaim,
}

/// The background retirement task of one writer, retained by the database
/// handle and stopped before storage closes.
///
/// # When it looks at the store
///
/// The task reads object storage only when something a pass depends on has
/// changed, and it is told of each such change in this process:
///
/// - the registry: an index operation committed its last step, which may
///   have stored an entry or moved the catalog a quarantined entry is
///   checked against; a filter kept an armed entry's rows; or a compaction
///   of this process ended while a proof was not yet tracking tables
///   ([`Registry::changed`](super::Registry::changed));
/// - the manifest: the writer's own copy, which SlateDB keeps current, has
///   lost a table some proof tracks. While a proof tracks tables the task
///   reads the [`Shape`] of that copy each time the writer's status moves,
///   which is every write, and goes through its tables only when the shape
///   has moved, which is every flush and every compaction: memory only,
///   linear in the sorted runs for a write and in the tables for a flush.
///
/// A writer opens with an empty registry, so the task's first pass is due at
/// once: it arms the entries the store holds. One thing the task is not told
/// of is the end of a compaction that was in flight when a proof started, so
/// while a proof waits for one the task looks again by a timer,
/// [`LOOK_GAP`] doubling to [`DRAIN_LOOK_MAX`]. That wait ends with the
/// compaction. A pass that fails is tried again by a timer too, and what it
/// failed with is kept for [`DroppedIndexStorage::last_pass`]. With every
/// proof tracking tables, or only quarantined entries stored, the task
/// sleeps until it is told of a change and reads nothing.
///
/// # When it reclaims
///
/// An entry whose proof tracks tables waits for compactions to rewrite them.
/// The scheduler compacts only as writes arrive, and rewrites its oldest and
/// largest run rarely however many arrive, so the entry, and the bytes of
/// its rows, could stay for long. The task therefore runs the
/// [reclaim](super::reclaim) a caller could ask for, limited to the sorted
/// runs worth rewriting ([`Worth::WRITER`]), in one case: **the store is
/// idle**, for [`IDLE_RECLAIM_AFTER`].
///
/// Idle means that the compactor has had nothing to do for that long: no
/// compaction job of this process started or ended, and none is running.
/// Every job creates its filters through this writer's registry, so that is
/// known without reading object storage. It is the compactor a reclaim
/// competes with, not writes. The scheduler starts a compaction within
/// seconds of the flush that makes one due, so a compactor that has rested
/// for minutes belongs to a store whose writes fill a memtable more slowly
/// than that, or not at all: such a store is reclaimed, writes or no. The
/// reclaim starts a compaction only while no other is in flight, and never
/// in the compactor's last free slot; see
/// [`Reclaimer::IdleStore`](super::reclaim::Reclaimer::IdleStore).
///
/// **A store that is never idle is never reclaimed by this task.** Its
/// ordinary compactions discard the rows of the runs they rewrite, which
/// retires most entries within minutes of a drop; rows in a run the
/// scheduler is not about to rewrite stay, with their entry, until it does.
///
/// One reclaim runs per idle period. Whatever it achieved, the next waits
/// until the store has changed and been idle again, so a reclaim that
/// cannot make progress, for instance while a reader older than the drop
/// is open or because no run is worth rewriting, is not repeated on a store
/// that stays still.
///
/// The task waits for a reclaim's compaction for [`RECLAIM_WAIT`] at most,
/// and runs no pass meanwhile. A rewrite that takes longer goes on by
/// itself: the task is back to its passes, arms and counts what is dropped
/// meanwhile, and is told of the rewrite's end by the writer's manifest,
/// which loses the tables the proofs track.
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
/// ended, and reads look through that many more runs until then. Nothing
/// stops a reclaim's compaction once it has started.
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
        } = pace;
        let registry = retirer.writer.dropped();
        let mut status = retirer.writer.db().subscribe();
        // The shape of the manifest the writer held when its tables were
        // last compared with the tables the proofs track.
        let mut shape = None::<Shape>;
        let mut looked = None::<Instant>;
        let mut drain_look = look_gap;
        let mut retry = RETRY;
        // Whether a reclaim has run since the store last changed.
        let mut reclaimed = false;
        // A writer opens with an empty registry, which discards nothing:
        // its first pass arms the entries the store holds, so it is due at
        // once.
        let mut timer = Timer::Pass { at: Instant::now() };
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
                        // one. The shape is: it costs the sorted runs to
                        // read, and the tables are gone through only when
                        // it has moved, which a write alone does not do.
                        if !registry.tracks_tables() {
                            continue;
                        }
                        let left = {
                            let status = status.borrow_and_update();
                            let now = Shape::of(&status.current_manifest);
                            if shape.as_ref() == Some(&now) {
                                continue;
                            }
                            shape = Some(now);
                            registry.tracked_table_left(&table_ids(&status.current_manifest))
                        };
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
                        Timer::Pass { .. } => break Wake::Pass,
                        Timer::Reclaim { compactions, .. } => {
                            let activity = registry.compactions();
                            if idle_since(compactions, activity) {
                                break Wake::Reclaim;
                            }
                            // A compaction ran meanwhile: idle starts again.
                            timer = Timer::Reclaim {
                                at: Instant::now() + idle_after,
                                compactions: activity,
                            };
                        }
                    },
                }
            };
            // A burst of changes costs one look at the store.
            let earliest = looked.map_or_else(Instant::now, |looked| looked + look_gap);
            tokio::select! {
                () = tokio::time::sleep_until(earliest) => {}
                _ = closed.changed() => return,
            }
            let compactions = registry.compactions();
            let outcome = match wake {
                Wake::Pass => retirer.pass().await,
                Wake::Reclaim => {
                    reclaimed = true;
                    retirer
                        .reclaim(
                            Instant::now() + RECLAIM_WAIT,
                            Reclaimer::IdleStore(Worth::WRITER),
                        )
                        .await
                        .map(|_| registry.storage())
                }
            };
            let now = Instant::now();
            looked = Some(now);
            timer = match outcome {
                Ok(storage) => {
                    retry = RETRY;
                    registry.pass_ended(None);
                    Timer::after_pass(
                        &storage,
                        compactions,
                        reclaimed,
                        now,
                        drain_look,
                        idle_after,
                    )
                }
                Err(error) => {
                    if !*closed.borrow() {
                        tracing::warn!(%error, "dropped-generation retirement pass failed");
                        registry.pass_ended(Some(error.to_string()));
                    }
                    let at = now + retry;
                    retry = (retry * 2).min(MAX_BACKOFF);
                    Timer::Pass { at }
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
