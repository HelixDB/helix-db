//! Reclaims the storage of dropped generations and deletes their entries.
//!
//! Compactions the scheduler picks discard the rows of an armed entry as they
//! pass, at no extra cost, but they never prove that every table was
//! rewritten. A *reclaim* does: it flushes the memtable, then submits one
//! compaction of every L0 table and every sorted run into the oldest run.
//! While that job is awaited the [`super::Registry`] records what each filter
//! created for the job's destination could discard. Once the job has
//! completed, the entries every one of those filters could discard have no
//! row left in any table, and their entry rows are deleted.
//!
//! # When a reclaim runs
//!
//! - On request, whenever an armed entry is discardable.
//! - On the background tick, once the rows of discardable entries are
//!   estimated at [`DROPPED_PERCENT_OF_LIVE`] percent of the live bytes or
//!   more. The estimate is read from the manifest and the block indexes of
//!   the tables an entry's key ranges cut through.
//!
//! An entry is discardable once the manifest's retention sequence has reached
//! its gate. SlateDB advances that sequence only when it flushes a memtable,
//! so a pass that finds an entry waiting flushes once; a database that takes
//! no write after the last reader older than a drop has closed waits for its
//! next write.
//!
//! # What does not scale
//!
//! A reclaim rewrites the whole database on one compaction slot, needs up to
//! twice the stored bytes until the old tables are collected, and holds off
//! the scheduler's L0 compactions while it runs. Its work is lost when a
//! scheduler compaction of the same runs commits first, or when the process
//! restarts. An entry whose rows passing compactions have already discarded
//! stays, costing a filter lookup and a refresh row, until the next reclaim:
//! entries below the share accumulate until their sum reaches it or a caller
//! asks. Every tick reads the entry rows, and every tick with an armed entry
//! reads the manifest from object storage. A database whose manifest has
//! segments is not reclaimed: its drops still skip their deletes, and every
//! pass fails until the reclaim learns to compact segments.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use slatedb::admin::Admin;
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::ObjectStore;
use slatedb::{SstReader, VersionedManifest, WriteBatch};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::failpoints::{self, IndexOutboxFailpoint};
use crate::index_lifecycle::DroppedGenerationRows;
use crate::HelixWriter;

use super::DroppedGeneration;

/// The background reclaim runs once the estimated bytes of discardable
/// dropped rows reach this percentage of the remaining, live bytes. A reclaim
/// rewrites every live byte once, so at 20 percent each reclaimed byte costs
/// at most five rewritten.
pub(crate) const DROPPED_PERCENT_OF_LIVE: u64 = 20;

/// Interval between background passes.
const TICK: Duration = Duration::from_secs(30);
/// Longest wait after passes that fail or prove nothing; each doubles it.
const MAX_BACKOFF: Duration = Duration::from_secs(600);
/// Interval between reads of an awaited compaction's status.
const STATUS_POLL: Duration = Duration::from_secs(1);
/// Partition mappings deleted per write.
const MAPPING_DELETE_BATCH: usize = 1024;

/// Why a reclaim pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Trigger {
    /// The background tick: compacts only at [`DROPPED_PERCENT_OF_LIVE`].
    Share,
    /// A caller asked: compacts whenever an entry is discardable.
    Request,
}

/// What one pass over the dropped index generations did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DroppedStorageReclaim {
    /// No dropped generation awaits reclaim.
    Idle,
    /// A snapshot or transaction older than their drops is still open, or no
    /// memtable has been flushed since the last one closed: the rows of all
    /// `entries` dropped generations are retained and nothing was compacted.
    Retained {
        /// Dropped generations awaiting reclaim.
        entries: usize,
    },
    /// The background pass left `dropped_bytes` of discardable rows in place
    /// because they are a small share of `live_bytes`.
    BelowShare {
        /// Dropped generations whose rows may be discarded.
        entries: usize,
        /// Estimate of the stored bytes their rows still occupy.
        dropped_bytes: u64,
        /// Stored bytes of everything else.
        live_bytes: u64,
    },
    /// A compaction of every table was submitted but did not prove the rows
    /// gone: it failed, a concurrent compaction committed first, or the
    /// database closed. The `entries` stay for the next pass.
    Unproven {
        /// Dropped generations whose rows may be discarded.
        entries: usize,
    },
    /// A compaction of every table completed and the entries of `entries`
    /// dropped generations were deleted.
    Reclaimed {
        /// Dropped generations whose entries were deleted.
        entries: usize,
        /// Estimate of the stored bytes their rows occupied before.
        dropped_bytes: u64,
    },
}

/// Runs reclaim passes for one writer, one at a time.
pub(crate) struct Reclaimer {
    writer: Arc<HelixWriter>,
    admin: Admin,
    tables: SstReader,
    /// Serializes passes. Holds the bytes of dropped rows in tables their key
    /// ranges only partly cover, by table view id and range prefix: a table
    /// never changes, so its block index is read once per range.
    pass: tokio::sync::Mutex<HashMap<(u128, Bytes), u64>>,
    closed: watch::Sender<bool>,
}

impl Reclaimer {
    /// A reclaimer for `writer`, whose database lives at `path` in
    /// `object_store` and runs its compactor in this process with the
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
            pass: tokio::sync::Mutex::new(HashMap::new()),
            closed: watch::channel(false).0,
        }
    }

    /// Runs one pass: arms new entries, and reclaims the discardable ones
    /// when `trigger` calls for it.
    ///
    /// An entry is deleted only when the pass's own compaction completed and
    /// every filter created for it could discard the entry's rows. A
    /// cross-check failure halts the registry and is returned.
    pub(crate) async fn pass(&self, trigger: Trigger) -> Result<DroppedStorageReclaim> {
        let mut partial = self.pass.lock().await;
        let db = self.writer.db();
        let registry = self.writer.dropped();
        registry.refresh(db).await?;
        let armed = registry.armed();
        if armed.is_empty() {
            partial.clear();
            return Ok(DroppedStorageReclaim::Idle);
        }
        let flush = || {
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
        };
        let mut manifest = self.manifest().await?;
        if armed
            .iter()
            .any(|(_, armed)| armed.gate > manifest.recent_snapshot_min_seq())
        {
            flush().await?;
            manifest = self.manifest().await?;
        }
        let candidates = armed
            .iter()
            .filter(|(_, armed)| armed.gate <= manifest.recent_snapshot_min_seq())
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(DroppedStorageReclaim::Retained {
                entries: armed.len(),
            });
        }
        let prefixes = candidates
            .iter()
            .flat_map(|(_, armed)| armed.owners.iter())
            .flat_map(|owner| owner.prefixes())
            .collect::<Vec<_>>();
        let dropped_bytes = self
            .dropped_bytes(&manifest, &prefixes, &mut partial)
            .await?;
        let stored_bytes = manifest
            .l0()
            .iter()
            .map(|view| view.estimate_size())
            .chain(manifest.compacted().iter().map(|run| run.estimate_size()))
            .sum::<u64>();
        let live_bytes = stored_bytes.saturating_sub(dropped_bytes);
        match trigger {
            Trigger::Share
                if dropped_bytes.saturating_mul(100)
                    < live_bytes.saturating_mul(DROPPED_PERCENT_OF_LIVE) =>
            {
                return Ok(DroppedStorageReclaim::BelowShare {
                    entries: candidates.len(),
                    dropped_bytes,
                    live_bytes,
                });
            }
            Trigger::Share | Trigger::Request => {}
        }

        // Everything written so far goes into the tables the job reads; a
        // dropped generation takes no write after its entry.
        flush().await?;
        let manifest = self.manifest().await?;
        let sources = manifest
            .l0()
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .chain(
                manifest
                    .compacted()
                    .iter()
                    .map(|run| SourceId::SortedRun(run.id)),
            )
            .collect::<Vec<_>>();
        assert!(
            !sources.is_empty(),
            "a flushed database that stores a dropped-generation entry has a table"
        );
        let destination = manifest
            .compacted()
            .iter()
            .map(|run| run.id)
            .min()
            .unwrap_or(0);
        tracing::info!(
            entries = candidates.len(),
            dropped_bytes,
            live_bytes,
            sources = sources.len(),
            destination,
            ?trigger,
            "reclaiming dropped generations: compacting every table"
        );
        let watching = registry.watch(destination);
        // An error or a cancelled pass drops `watching`, which ends the watch.
        let status = self
            .compacted(CompactionSpec::new(sources, destination))
            .await?;
        let proven = watching.proven(status);
        let reclaimed = candidates
            .iter()
            .copied()
            .filter(|(entry, _)| proven.contains(entry))
            .collect::<Vec<_>>();
        if reclaimed.is_empty() {
            tracing::info!(
                ?status,
                entries = candidates.len(),
                "reclaim compaction proved no dropped generation gone"
            );
            return Ok(DroppedStorageReclaim::Unproven {
                entries: candidates.len(),
            });
        }
        failpoints::trip(IndexOutboxFailpoint::DroppedEntryDeleteBefore)?;
        // The mappings list a partitioned generation's physical ids, so they
        // go before the entry: a crash in between leaves an entry that names
        // fewer owners, never mappings without an entry.
        for (entry, armed) in &reclaimed {
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
        let mut entries = WriteBatch::new();
        for (entry, _) in &reclaimed {
            entries.delete(entry.key());
        }
        db.write(entries)
            .await
            .map_err(HelixDbError::from_storage_commit)?;
        registry.disarm(
            &reclaimed
                .iter()
                .map(|(entry, _)| *entry)
                .collect::<HashSet<_>>(),
        );
        tracing::info!(
            entries = reclaimed.len(),
            dropped_bytes,
            "reclaimed dropped generations"
        );
        Ok(DroppedStorageReclaim::Reclaimed {
            entries: reclaimed.len(),
            dropped_bytes,
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

    /// An estimate of the stored bytes under `prefixes` in the tables of
    /// `manifest`, exact to one block at each end of each prefix's rows.
    ///
    /// A table wholly under one prefix counts in full, from the manifest
    /// alone. In a table a prefix cuts through, the blocks whose first key is
    /// under the prefix count, from the table's block index, which is read
    /// once and remembered in `partial`.
    async fn dropped_bytes(
        &self,
        manifest: &VersionedManifest,
        prefixes: &[Bytes],
        partial: &mut HashMap<(u128, Bytes), u64>,
    ) -> Result<u64> {
        let mut prefixes = prefixes.iter().collect::<Vec<_>>();
        prefixes.sort_unstable();
        let views = manifest
            .l0()
            .iter()
            .chain(manifest.compacted().iter().flat_map(|run| run.sst_views()))
            .collect::<Vec<_>>();
        let stored = views.iter().map(|view| view.id.0).collect::<HashSet<_>>();
        partial.retain(|(view, _), _| stored.contains(view));
        let mut bytes = 0u64;
        // Imperative: each table awaits at most one index read, lazily.
        for view in views {
            let (Some(first), Some(last)) = (&view.sst.info.first_entry, &view.sst.info.last_entry)
            else {
                continue;
            };
            // No owner prefix is a prefix of another, so the only prefix at
            // or before `first` that can cover it is its predecessor.
            let after_first = prefixes.partition_point(|prefix| prefix[..] <= first[..]);
            let cut = prefixes[..after_first]
                .last()
                .filter(|prefix| first.starts_with(prefix))
                .into_iter()
                .chain(
                    prefixes[after_first..]
                        .iter()
                        .take_while(|prefix| prefix[..] <= last[..]),
                );
            let mut index = None;
            for prefix in cut {
                if first.starts_with(prefix) && last.starts_with(prefix) {
                    bytes += view.estimate_size();
                    continue;
                }
                let key = (view.id.0, (*prefix).clone());
                bytes += match partial.get(&key) {
                    Some(known) => *known,
                    None => {
                        let index = match index.as_ref() {
                            Some(index) => index,
                            None => index.insert(
                                self.tables
                                    .open_with_handle(view.sst.clone())?
                                    .index()
                                    .await?,
                            ),
                        };
                        let from = index.partition_point(|block| block < &prefix[..]);
                        let to = index.partition_point(|block| {
                            block < &prefix[..] || block.starts_with(prefix)
                        });
                        let info = &view.sst.info;
                        let end = match index.get(to) {
                            Some((offset, _)) => offset,
                            // The data blocks end where the filter or the index
                            // starts.
                            None if info.filter_len > 0 => {
                                info.index_offset.min(info.filter_offset)
                            }
                            None => info.index_offset,
                        };
                        let under = index
                            .get(from)
                            .map_or(0, |(offset, _)| end.saturating_sub(offset));
                        partial.insert(key, under);
                        under
                    }
                };
            }
        }
        Ok(bytes)
    }

    /// Submits `spec` and waits for it to finish. Returns its last status, or
    /// `None` when the database closed first or a later compaction has
    /// replaced its record.
    async fn compacted(&self, spec: CompactionSpec) -> Result<Option<CompactionStatus>> {
        let mut closed = self.closed.subscribe();
        let id = self.admin.submit_compaction(spec).await?.id();
        loop {
            if *closed.borrow() {
                return Ok(None);
            }
            let status = self
                .admin
                .read_compaction(id, None)
                .await?
                .map(|compaction| compaction.status());
            match status {
                Some(CompactionStatus::Completed | CompactionStatus::Failed) | None => {
                    return Ok(status);
                }
                Some(
                    CompactionStatus::Submitted
                    | CompactionStatus::Scheduled
                    | CompactionStatus::Running
                    | CompactionStatus::Compacted,
                ) => {}
            }
            tokio::select! {
                () = tokio::time::sleep(STATUS_POLL) => {}
                _ = closed.changed() => {}
            }
        }
    }

    /// Deletes the partition mappings a reclaimed partitioned generation's
    /// drop left in place.
    async fn delete_partition_mappings(
        &self,
        entry: DroppedGeneration,
        scope: crate::encoding::v2::keys::scope::DataScope,
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

/// The background reclaim task of one writer, retained by the database handle
/// and stopped before storage closes.
pub(crate) struct ReclaimSupervisor {
    reclaimer: Arc<Reclaimer>,
    handle: JoinHandle<()>,
}

/// A supervisor dropped without [`ReclaimSupervisor::stop`] still ends its
/// task, which would otherwise keep the writer alive.
impl Drop for ReclaimSupervisor {
    fn drop(&mut self) {
        self.reclaimer.closed.send_replace(true);
        self.handle.abort();
    }
}

impl ReclaimSupervisor {
    /// Starts the background passes of `reclaimer`.
    pub(crate) fn start(reclaimer: Reclaimer) -> Self {
        let reclaimer = Arc::new(reclaimer);
        let handle = tokio::spawn({
            let reclaimer = Arc::clone(&reclaimer);
            async move {
                let mut closed = reclaimer.closed.subscribe();
                let mut delay = TICK;
                while !*closed.borrow() {
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        _ = closed.changed() => continue,
                    }
                    delay = match reclaimer.pass(Trigger::Share).await {
                        Ok(
                            DroppedStorageReclaim::Idle
                            | DroppedStorageReclaim::Retained { .. }
                            | DroppedStorageReclaim::BelowShare { .. }
                            | DroppedStorageReclaim::Reclaimed { .. },
                        ) => TICK,
                        Ok(DroppedStorageReclaim::Unproven { .. }) => (delay * 2).min(MAX_BACKOFF),
                        Err(error) => {
                            if !*closed.borrow() {
                                tracing::warn!(%error, "dropped-generation reclaim pass failed");
                            }
                            (delay * 2).min(MAX_BACKOFF)
                        }
                    };
                }
            }
        });
        Self { reclaimer, handle }
    }

    /// The reclaimer, for a pass on request.
    pub(crate) fn reclaimer(&self) -> Arc<Reclaimer> {
        Arc::clone(&self.reclaimer)
    }

    /// Ends the background task and any pass in flight, before storage
    /// closes.
    pub(crate) async fn stop(mut self) {
        self.reclaimer.closed.send_replace(true);
        match (&mut self.handle).await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                tracing::warn!(%error, "dropped-generation reclaim task failed during shutdown");
            }
        }
        // A requested pass ends at its next status read.
        drop(self.reclaimer.pass.lock().await);
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/dropped_storage_reclaim.rs"]
mod contracts;
