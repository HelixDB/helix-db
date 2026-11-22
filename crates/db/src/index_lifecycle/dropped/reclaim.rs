//! Reclaims the storage of dropped generations.
//!
//! Compactions the scheduler picks discard the rows of an armed entry as they
//! pass, and [`super::retire`] deletes the entry once they have. That gives
//! the bytes back whenever the scheduler gets to the tables that hold them,
//! which for the oldest sorted run can be a long time, and on a store that
//! takes few writes is never. A *reclaim* submits up to two compactions and
//! waits for them, within the time its [`Reclaimer`] allows:
//!
//! 1. every L0 table into a new sorted run, unless a compaction of L0 tables
//!    is already in flight;
//! 2. every sorted run into the oldest.
//!
//! Neither holds off the scheduler's L0 compactions for long: SlateDB runs
//! one L0 compaction at a time, the first is one, and the second reads no L0
//! table. A reclaim deletes no entry itself. It ends with a retirement pass,
//! which deletes the entries its compactions left without rows.
//!
//! An entry is discardable once the manifest's retention sequence has reached
//! its gate. SlateDB advances that sequence only when it flushes a memtable,
//! so a reclaim that finds an entry waiting flushes once; a database that
//! takes no write after the last reader older than a drop has closed waits
//! for its next write.
//!
//! # What does not scale
//!
//! The second compaction rewrites every sorted run on one compaction slot and
//! needs up to twice their bytes until the old tables are collected. While it
//! runs the scheduler compacts no sorted run, so the runs L0 compactions add
//! meanwhile wait for it. It is lost when a compaction of sorted runs the
//! scheduler started first commits before it, and when the process restarts.
//! A database whose manifest has segments is not reclaimed: its entries
//! still retire, and every reclaim fails until this learns to compact
//! segments.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use slatedb::compactor::{CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::VersionedManifest;
use tokio::time::Instant;

use crate::error::{HelixDbError, Result};

use super::retire::Retirer;

/// Interval between reads of an awaited compaction's status.
const STATUS_POLL: Duration = Duration::from_secs(1);

/// Why a requested reclaim did not see its compaction of the sorted runs
/// complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimObstacle {
    /// A compaction of sorted runs was already in flight, or the store has
    /// no sorted run while a compaction of its L0 tables is in flight, or a
    /// reclaim of an idle store found the store no longer idle. No
    /// compaction of sorted runs was submitted.
    Busy,
    /// SlateDB refused the compaction, or one the scheduler started first
    /// replaced its sources.
    Rejected,
    /// The compaction's record was replaced by a later one before its result
    /// was read.
    Unobserved,
    /// The caller's wait ran out. A compaction that was submitted goes on.
    TimedOut,
    /// The database closed.
    Closed,
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
    /// The compaction of the sorted runs was not seen to complete. The
    /// `entries` dropped generations retire when the scheduler's compactions
    /// reach their rows, or with a later reclaim.
    Incomplete {
        /// Dropped generations awaiting compaction.
        entries: usize,
        /// What stood in the way.
        obstacle: ReclaimObstacle,
    },
    /// Every sorted run was rewritten without the rows of the discardable
    /// dropped generations.
    Compacted {
        /// Dropped generations whose entries the pass that followed deleted.
        retired: usize,
        /// Dropped generations still awaiting compaction: dropped after the
        /// reclaim began, still read by an older reader, or with rows in a
        /// table a compaction in flight had yet to replace.
        entries: usize,
        /// Estimate of the stored bytes the discardable rows occupied
        /// before.
        dropped_bytes: u64,
    },
}

/// Who runs a reclaim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reclaimer {
    /// A caller who asked for the bytes now, whatever else the store is
    /// doing.
    Caller,
    /// The background task, because the store has been idle. It gives way:
    /// when L0 tables are left once the first compaction is done with,
    /// writes have resumed or the scheduler is compacting them, and the
    /// reclaim ends [`ReclaimObstacle::Busy`] before it rewrites a sorted
    /// run.
    IdleStore,
}

/// How an awaited compaction ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ended {
    Completed,
    Short(ReclaimObstacle),
}

impl Retirer {
    /// Compacts the store so that the rows of every discardable dropped
    /// generation are gone, waiting no later than `deadline`, then runs a
    /// retirement pass. Logs what it did.
    pub(crate) async fn reclaim(
        &self,
        deadline: Instant,
        reclaimer: Reclaimer,
    ) -> Result<DroppedStorageReclaim> {
        let mut logged = self.pass.lock().await;
        let outcome = self.compact(deadline, reclaimer).await?;
        let outcome = match outcome {
            DroppedStorageReclaim::Compacted { dropped_bytes, .. } => {
                let retired = self.look(&mut logged).await?;
                DroppedStorageReclaim::Compacted {
                    retired,
                    entries: self.writer().dropped().armed().len(),
                    dropped_bytes,
                }
            }
            outcome @ (DroppedStorageReclaim::Idle
            | DroppedStorageReclaim::Retained { .. }
            | DroppedStorageReclaim::Incomplete { .. }) => outcome,
        };
        tracing::info!(
            ?outcome,
            ?reclaimer,
            "reclaim of dropped index storage ended"
        );
        Ok(outcome)
    }

    async fn compact(
        &self,
        deadline: Instant,
        reclaimer: Reclaimer,
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
        let prefixes = armed
            .iter()
            .filter(|(_, armed)| armed.gate <= manifest.recent_snapshot_min_seq())
            .flat_map(|(_, armed)| armed.owners.iter())
            .flat_map(|owner| owner.prefixes())
            .collect::<Vec<_>>();
        if prefixes.is_empty() {
            return Ok(DroppedStorageReclaim::Retained {
                entries: armed.len(),
            });
        }
        let dropped_bytes = self.dropped_bytes(&manifest, &prefixes).await?;
        let incomplete = |obstacle| DroppedStorageReclaim::Incomplete {
            entries: armed.len(),
            obstacle,
        };

        let active = self.active_specs().await?;
        let mut rewritten = None;
        if !manifest.l0().is_empty() && !active.iter().any(CompactionSpec::has_l0_sources) {
            // Above every run and every destination in flight, as SlateDB
            // requires of a run made of L0 tables alone.
            let destination = manifest
                .compacted()
                .iter()
                .map(|run| run.id)
                .chain(active.iter().filter_map(CompactionSpec::destination))
                .max()
                .map_or(0, |id| id + 1);
            let sources = manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect::<Vec<_>>();
            tracing::info!(
                sources = sources.len(),
                destination,
                "reclaiming dropped generations: compacting the L0 tables"
            );
            match self
                .compacted(CompactionSpec::new(sources, destination), deadline)
                .await?
            {
                Ended::Completed => rewritten = Some(destination),
                // The scheduler's own L0 compaction took them, or will.
                Ended::Short(
                    ReclaimObstacle::Busy | ReclaimObstacle::Rejected | ReclaimObstacle::Unobserved,
                ) => {}
                Ended::Short(obstacle @ (ReclaimObstacle::TimedOut | ReclaimObstacle::Closed)) => {
                    return Ok(incomplete(obstacle));
                }
            }
        }

        let manifest = self.manifest().await?;
        match reclaimer {
            Reclaimer::Caller => {}
            Reclaimer::IdleStore => {
                if !manifest.l0().is_empty() {
                    return Ok(incomplete(ReclaimObstacle::Busy));
                }
            }
        }
        let runs = manifest
            .compacted()
            .iter()
            .map(|run| run.id)
            .collect::<Vec<_>>();
        // The only run is the one the first compaction just wrote.
        if runs.len() == 1 && runs.first() == rewritten.as_ref() {
            return Ok(DroppedStorageReclaim::Compacted {
                retired: 0,
                entries: armed.len(),
                dropped_bytes,
            });
        }
        let Some(destination) = runs.iter().copied().min() else {
            return Ok(incomplete(ReclaimObstacle::Busy));
        };
        if self
            .active_specs()
            .await?
            .iter()
            .any(CompactionSpec::has_sr_sources)
        {
            return Ok(incomplete(ReclaimObstacle::Busy));
        }
        tracing::info!(
            sources = runs.len(),
            destination,
            dropped_bytes,
            "reclaiming dropped generations: compacting every sorted run"
        );
        let sources = runs.into_iter().map(SourceId::SortedRun).collect();
        Ok(
            match self
                .compacted(CompactionSpec::new(sources, destination), deadline)
                .await?
            {
                Ended::Completed => DroppedStorageReclaim::Compacted {
                    retired: 0,
                    entries: armed.len(),
                    dropped_bytes,
                },
                Ended::Short(obstacle) => incomplete(obstacle),
            },
        )
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

    /// The spec of every compaction `.compactions` shows in flight.
    async fn active_specs(&self) -> Result<Vec<CompactionSpec>> {
        Ok(self
            .admin
            .read_compactions(None)
            .await?
            .iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .filter(|compaction| compaction.active())
            .map(|compaction| compaction.spec().clone())
            .collect())
    }

    /// An estimate of the stored bytes under `prefixes` in the tables of
    /// `manifest`, exact to one block at each end of each prefix's rows.
    ///
    /// A table wholly under one prefix counts in full, from the manifest
    /// alone. In a table a prefix cuts through, the blocks whose first key is
    /// under the prefix count, from the table's block index.
    async fn dropped_bytes(&self, manifest: &VersionedManifest, prefixes: &[Bytes]) -> Result<u64> {
        let mut prefixes = prefixes.iter().collect::<Vec<_>>();
        prefixes.sort_unstable();
        // By file: a table several views show counts once.
        let tables = manifest
            .l0()
            .iter()
            .chain(manifest.compacted().iter().flat_map(|run| run.sst_views()))
            .map(|view| (view.sst.id, &view.sst))
            .collect::<HashMap<_, _>>();
        let mut bytes = 0u64;
        // Imperative: each table awaits at most one index read, lazily.
        for sst in tables.into_values() {
            let (Some(first), Some(last)) = (&sst.info.first_entry, &sst.info.last_entry) else {
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
                    bytes += sst.estimate_size();
                    continue;
                }
                let index = match index.as_ref() {
                    Some(index) => index,
                    None => index.insert(self.tables.open_with_handle(sst.clone())?.index().await?),
                };
                let from = index.partition_point(|block| block < &prefix[..]);
                let to =
                    index.partition_point(|block| block < &prefix[..] || block.starts_with(prefix));
                let info = &sst.info;
                let end = match index.get(to) {
                    Some((offset, _)) => offset,
                    // The data blocks end where the filter or the index
                    // starts.
                    None if info.filter_len > 0 => info.index_offset.min(info.filter_offset),
                    None => info.index_offset,
                };
                bytes += index
                    .get(from)
                    .map_or(0, |(offset, _)| end.saturating_sub(offset));
            }
        }
        Ok(bytes)
    }

    /// Submits `spec` and waits for it to finish, no later than `deadline`.
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
                None => return Ok(Ended::Short(ReclaimObstacle::Unobserved)),
                Some(
                    CompactionStatus::Submitted
                    | CompactionStatus::Scheduled
                    | CompactionStatus::Running
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
