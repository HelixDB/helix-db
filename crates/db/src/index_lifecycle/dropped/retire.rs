//! Retires dropped-generation entries: deletes each one once no table of the
//! store can hold a row of it.
//!
//! Nothing here compacts. The compactions the scheduler picks discard an
//! armed entry's rows as they pass, and a [`Retirer`] pass watches the
//! store's tables until it can prove none holds such a row.
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
//!    suspect and retires at once.
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
//! whose key range cuts through an owner prefix, and one data block for each
//! run of prefixes that falls inside a block; a writer that opens onto many
//! entries pays that once for all of them, in the background.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use slatedb::admin::Admin;
use slatedb::manifest::SsTableHandle;
use slatedb::object_store::ObjectStore;
use slatedb::{SstReader, WriteBatch};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::encoding::v2::keys::scope::DataScope;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::failpoints::{self, IndexOutboxFailpoint};
use crate::index_lifecycle::DroppedGenerationRows;
use crate::HelixWriter;

use super::{CompactionId, DroppedGeneration, DroppedIndexStorage, Proof, TableId};

/// Interval between background passes.
const TICK: Duration = Duration::from_secs(30);
/// Longest wait after passes that fail; each doubles it.
const MAX_BACKOFF: Duration = Duration::from_secs(600);
/// Partition mappings deleted per write.
const MAPPING_DELETE_BATCH: usize = 1024;
/// Tables one pass looks into at a time.
const TABLE_READS: usize = 16;

/// What one look at the store makes of a proof.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// These compactions, in flight when the proof started, still are.
    /// Never empty.
    Drain(HashSet<CompactionId>),
    /// Every compaction the proof waited for has finished: the tables of the
    /// manifest this look read that hold a row of the entry are its
    /// suspects.
    Find,
    /// These suspects are still in the manifest. Never empty.
    Track(HashSet<TableId>),
    /// No table holds a row of the entry.
    Retire,
}

impl Proof {
    /// The next step of this proof, given the compactions `.compactions`
    /// shows `active` and the tables `stored` in a manifest read after it.
    fn step(&self, active: &HashSet<CompactionId>, stored: &HashSet<TableId>) -> Step {
        let waiting = match self {
            Self::Unproven => active.clone(),
            Self::Draining { compactions } => compactions.intersection(active).copied().collect(),
            Self::Tracking { tables } => {
                let left = tables.intersection(stored).copied().collect::<HashSet<_>>();
                return if left.is_empty() {
                    Step::Retire
                } else {
                    Step::Track(left)
                };
            }
        };
        if waiting.is_empty() {
            Step::Find
        } else {
            Step::Drain(waiting)
        }
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

/// Sorts out, from the first and last keys of a table alone, the `prefixes`
/// it can hold a row under. Returns the entries that own a prefix the table
/// starts or ends under, which it therefore holds a row of, and the prefixes
/// of other entries that sort strictly inside the table, which only its block
/// index can decide. Every other prefix sorts outside the table.
///
/// A table without a first key is treated as starting below every prefix,
/// and one without a last key as ending above every prefix.
fn held_by_range<'a>(
    prefixes: &'a BTreeMap<Bytes, DroppedGeneration>,
    first: Option<&[u8]>,
    last: Option<&[u8]>,
) -> (
    HashSet<DroppedGeneration>,
    Vec<(&'a Bytes, DroppedGeneration)>,
) {
    // No owner prefix is a prefix of another, so the only prefix at or below
    // `first` that can cover it is its predecessor.
    let covering = first.and_then(|first| {
        prefixes
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(first)))
            .next_back()
            .filter(|(prefix, _)| first.starts_with(prefix))
    });
    // A prefix above `first` and at or below `last` covers `last`, or every
    // key under it sorts strictly between the two.
    let (ending, inside): (Vec<_>, Vec<_>) = prefixes
        .range::<[u8], _>((
            first.map_or(Bound::Unbounded, Bound::Excluded),
            last.map_or(Bound::Unbounded, Bound::Included),
        ))
        .partition(|(prefix, _)| last.is_some_and(|last| last.starts_with(prefix)));
    let held = covering
        .into_iter()
        .chain(ending)
        .map(|(_, entry)| *entry)
        .collect::<HashSet<_>>();
    let inside = inside
        .into_iter()
        .filter(|(_, entry)| !held.contains(entry))
        .map(|(prefix, entry)| (prefix, *entry))
        .collect();
    (held, inside)
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

/// Watches one writer's tables and retires its dropped-generation entries,
/// one pass at a time.
pub(crate) struct Retirer {
    writer: Arc<HelixWriter>,
    pub(super) admin: Admin,
    pub(super) tables: SstReader,
    /// Serializes passes and requested reclaims. Holds what the last pass
    /// logged, so a pass that changes nothing logs nothing.
    pub(super) pass: tokio::sync::Mutex<DroppedIndexStorage>,
    pub(super) closed: watch::Sender<bool>,
}

impl Retirer {
    /// A retirer for `writer`, whose database lives at `path` in
    /// `object_store` and runs every compaction in this process with the
    /// writer's [`super::Registry`] behind its filter supplier.
    pub(crate) fn new(
        writer: Arc<HelixWriter>,
        path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            writer,
            admin: Admin::builder(path, Arc::clone(&object_store)).build(),
            tables: SstReader::new(path, object_store, None, None),
            pass: tokio::sync::Mutex::new(DroppedIndexStorage::default()),
            closed: watch::channel(false).0,
        }
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
        Ok(logged.clone())
    }

    /// One pass, by a caller that holds the pass lock, whose value is
    /// `logged`. Returns how many entries it retired.
    pub(super) async fn look(&self, logged: &mut DroppedIndexStorage) -> Result<usize> {
        let retired = self.advance().await?;
        let storage = self.writer.dropped().storage();
        if retired > 0 || storage != *logged {
            tracing::info!(
                retired,
                unproven = storage.unproven,
                retained_for_older_readers = storage.retained,
                draining = storage.draining,
                tracking = storage.tracking,
                tracked_tables = storage.tracked_tables,
                quarantined = storage.quarantined.len(),
                "dropped index generations awaiting compaction"
            );
            *logged = storage;
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
            match holding.remove(&entry) {
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
    /// entry that owns the prefix. An entry no table holds a row of is
    /// absent.
    async fn holding(
        &self,
        tables: HashMap<TableId, SsTableHandle>,
        prefixes: BTreeMap<Bytes, DroppedGeneration>,
    ) -> Result<HashMap<DroppedGeneration, HashSet<TableId>>> {
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
        let mut holding = HashMap::<_, HashSet<_>>::new();
        for (table, entries) in held {
            for entry in entries {
                holding.entry(entry).or_default().insert(table);
            }
        }
        Ok(holding)
    }

    /// The entries that own a prefix under which the table `sst` holds a row.
    ///
    /// Reads the table's block index only when a prefix sorts strictly inside
    /// its key range, and a data block only when a prefix sorts strictly
    /// inside that block.
    async fn held(
        &self,
        sst: &SsTableHandle,
        prefixes: &BTreeMap<Bytes, DroppedGeneration>,
    ) -> Result<HashSet<DroppedGeneration>> {
        let (mut held, inside) = held_by_range(
            prefixes,
            sst.info.first_entry.as_deref(),
            sst.info.last_entry.as_deref(),
        );
        if inside.is_empty() {
            return Ok(held);
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
        Ok(held)
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

/// The background retirement task of one writer, retained by the database
/// handle and stopped before storage closes.
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
    /// Starts the background passes of `retirer`.
    pub(crate) fn start(retirer: Retirer) -> Self {
        let retirer = Arc::new(retirer);
        let handle = tokio::spawn({
            let retirer = Arc::clone(&retirer);
            async move {
                let mut closed = retirer.closed.subscribe();
                let mut delay = TICK;
                while !*closed.borrow() {
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = closed.changed() => continue,
                    }
                    delay = match retirer.pass().await {
                        Ok(_) => TICK,
                        Err(error) => {
                            if !*closed.borrow() {
                                tracing::warn!(%error, "dropped-generation retirement pass failed");
                            }
                            (delay * 2).min(MAX_BACKOFF)
                        }
                    };
                }
            }
        });
        Self { retirer, handle }
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
