//! Ordered range serving over one request read view.

use std::collections::VecDeque;

use helix_planner::ir::RangeScanIteration;

use super::*;

/// Request-local cancellation and observation boundary. Never retains a DB.
pub(crate) trait ExactRangeScanProgress: Sync {
    fn checkpoint(&self) -> Result<()>;
    fn entry_visited(&self) {}
    fn authoritative_read(&self) {}
    fn authoritative_decode(&self) {}
    fn stale_candidate(&self) {}
    fn fallback_scan(&self) {}
    fn retained_ids(&self, _count: usize) {}
}

pub(crate) struct UnobservedRangeScan;

impl ExactRangeScanProgress for UnobservedRangeScan {
    fn checkpoint(&self) -> Result<()> {
        Ok(())
    }
}

/// A checked entry is owned so the iterator can advance without retaining a row.
struct CheckedEntry {
    owner: IndexEntityId,
    value: CanonicalRangeValue,
}

/// All phases, including stale-entry recovery, share this exact read view.
struct RangeScan<'a, R> {
    reader: &'a R,
    handle: &'a ActiveIndexHandle,
    definition: &'a ValidatedSecondaryIndexDefinition,
    direction: StorageRangeIndexDirection,
    lane: SecondaryEntryLane,
    query: Option<&'a SecondaryRangeQuery>,
    membership: &'a [roaring::RoaringTreemap],
    progress: &'a dyn ExactRangeScanProgress,
}

impl<R: DbReadOps + Sync> RangeScan<'_, R> {
    fn checked_entry(&self, row: slatedb::KeyValue) -> Result<CheckedEntry> {
        self.progress.entry_visited();
        let IndexKey::Data {
            kind: ScopedKey::SecondaryEntry(key),
            ..
        } = IndexKey::parse_from_slice(self.handle.scope(), &row.key)?
        else {
            return Err(corruption(
                "secondary range prefix yielded another key kind",
            ));
        };
        if key.index_id() != self.handle.index_id()
            || key.generation() != self.handle.generation()
            || key.lane() != self.lane
        {
            return Err(corruption(
                "secondary range entry escaped its exact serving prefix",
            ));
        }
        let Some(owner) = key.entity_id() else {
            return Err(corruption("secondary range entry omitted its owner"));
        };
        let value_owner = decode_secondary_entry_value(
            self.handle.index_id(),
            self.handle.generation(),
            self.lane,
            &row.value,
        )?;
        if owner != value_owner {
            return Err(corruption(
                "secondary range entry key/value owners disagree",
            ));
        }
        let Some(value) = key.range_value() else {
            return Err(corruption("secondary range entry omitted its range value"));
        };
        Ok(CheckedEntry {
            owner,
            value: value.clone(),
        })
    }

    fn contains(&self, owner: IndexEntityId) -> bool {
        self.membership
            .iter()
            .all(|bitmap| bitmap.contains(owner.get()))
    }

    async fn verify(&self, owner: IndexEntityId, value: &CanonicalRangeValue) -> Result<bool> {
        self.progress.checkpoint()?;
        let matches = authoritative_range_matches(
            self.reader,
            self.handle.scope(),
            self.definition,
            owner,
            self.direction,
            value,
            self.query,
            self.progress,
        )
        .await?;
        if !matches {
            self.progress.stale_candidate();
        }
        Ok(matches)
    }

    /// Resolve a whole tie group atomically. Never publish the provisional prefix
    /// before deciding whether discarded candidates require a forward rescan.
    async fn resolve_group(
        &self,
        value: &CanonicalRangeValue,
        retained: &VecDeque<IndexEntityId>,
        discarded: bool,
        needed: usize,
    ) -> Result<Vec<u64>> {
        let mut provisional = Vec::new();
        for owner in retained.iter().rev().copied() {
            if self.verify(owner, value).await? {
                provisional.push(owner.get());
            } else if discarded {
                // Drop all provisional state before recovery; this avoids duplicate
                // results and keeps temporary retained state proportional to needed.
                provisional.clear();
                self.progress.checkpoint()?;
                self.progress.fallback_scan();
                let prefix = IndexKey::data_prefix(
                    self.handle.scope(),
                    ScopedKey::secondary_lane_prefix(
                        self.handle.index_id(),
                        self.handle.generation(),
                        self.lane,
                    ),
                );
                let bounds = (
                    Bound::Included(value.entity_key_suffix(u64::MIN)),
                    Bound::Included(value.entity_key_suffix(u64::MAX)),
                );
                let mut rows = self.reader.scan_prefix(prefix, bounds).await?;
                loop {
                    self.progress.checkpoint()?;
                    let Some(row) = rows.next().await? else {
                        break;
                    };
                    let entry = self.checked_entry(row)?;
                    if &entry.value != value {
                        return Err(corruption("secondary tie recovery escaped its exact value"));
                    }
                    if self.contains(entry.owner) && self.verify(entry.owner, &entry.value).await? {
                        provisional.push(entry.owner.get());
                        if provisional.len() == needed {
                            break;
                        }
                    }
                }
                return Ok(provisional);
            }
        }
        Ok(provisional)
    }
}

/// Serve an ordered range without changing its physical lane or persisted data.
/// Reverse scans retain at most the remaining result count in each tie group.
/// The count bounds accepted rows, never the number of scanned physical entries.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn scan_active_range_generation_ordered(
    reader: &(impl DbReadOps + Sync),
    handle: &ActiveIndexHandle,
    query: Option<&SecondaryRangeQuery>,
    iteration: RangeScanIteration,
    limit: Option<usize>,
    membership: &[roaring::RoaringTreemap],
    progress: &dyn ExactRangeScanProgress,
) -> Result<Vec<u64>> {
    progress.checkpoint()?;
    let Some(definition) = handle.secondary_definition() else {
        return Err(corruption(
            "secondary range serving received a non-secondary Active handle",
        ));
    };
    if !matches!(
        definition,
        ValidatedSecondaryIndexDefinition::NodeRange { .. }
            | ValidatedSecondaryIndexDefinition::EdgeRange { .. }
    ) {
        return Err(corruption(
            "secondary range serving received an equality definition",
        ));
    }
    let direction = match definition.direction() {
        RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
        RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
    };
    let bounds = match query {
        Some(query) => {
            let Some(bounds) = secondary_range_scan_bounds(direction, query)? else {
                return Ok(Vec::new());
            };
            bounds
        }
        None => (Bound::Unbounded, Bound::Unbounded),
    };
    if limit == Some(0) || membership.iter().any(roaring::RoaringTreemap::is_empty) {
        return Ok(Vec::new());
    }
    let scan = RangeScan {
        reader,
        handle,
        definition,
        direction,
        lane: definition_lane(definition),
        query,
        membership,
        progress,
    };
    let prefix = IndexKey::data_prefix(
        handle.scope(),
        ScopedKey::secondary_lane_prefix(handle.index_id(), handle.generation(), scan.lane),
    );
    let options = slatedb::config::ScanOptions::default().with_order(match iteration {
        RangeScanIteration::Forward => slatedb::IterationOrder::Ascending,
        RangeScanIteration::Reverse => slatedb::IterationOrder::Descending,
    });
    progress.checkpoint()?;
    let mut rows = reader
        .scan_prefix_with_options(prefix, bounds, &options)
        .await?;
    let mut accepted = Vec::new();
    let mut group_value = None;
    let mut retained = VecDeque::new();
    let mut discarded = false;
    let maximum = limit.unwrap_or(usize::MAX);
    loop {
        // Check before each storage read, including long runs of non-members.
        progress.checkpoint()?;
        let Some(row) = rows.next().await? else {
            break;
        };
        let entry = scan.checked_entry(row)?;
        if iteration == RangeScanIteration::Forward {
            if scan.contains(entry.owner) && scan.verify(entry.owner, &entry.value).await? {
                accepted.push(entry.owner.get());
                if accepted.len() == maximum {
                    break;
                }
            }
            continue;
        }
        match group_value.as_ref() {
            Some(value) if value != &entry.value => {
                accepted.extend(
                    scan.resolve_group(value, &retained, discarded, maximum - accepted.len())
                        .await?,
                );
                retained.clear();
                discarded = false;
                if accepted.len() == maximum {
                    return Ok(accepted);
                }
            }
            _ => {}
        }
        group_value = Some(entry.value);
        if scan.contains(entry.owner) {
            let needed = maximum - accepted.len();
            // Evict before pushing to keep the retained count bounded by needed.
            if retained.len() == needed {
                retained.pop_front();
                discarded = true;
            }
            retained.push_back(entry.owner);
            progress.retained_ids(retained.len());
        }
    }
    if let Some(value) = group_value {
        accepted.extend(
            scan.resolve_group(&value, &retained, discarded, maximum - accepted.len())
                .await?,
        );
    }
    Ok(accepted)
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct RangeScanCounters {
    pub entries: std::sync::atomic::AtomicUsize,
    pub reads: std::sync::atomic::AtomicUsize,
    pub decodes: std::sync::atomic::AtomicUsize,
    pub stale: std::sync::atomic::AtomicUsize,
    pub fallbacks: std::sync::atomic::AtomicUsize,
    pub peak: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl ExactRangeScanProgress for RangeScanCounters {
    fn checkpoint(&self) -> Result<()> {
        Ok(())
    }
    fn entry_visited(&self) {
        self.entries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn authoritative_read(&self) {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn authoritative_decode(&self) {
        self.decodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn stale_candidate(&self) {
        self.stale
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn fallback_scan(&self) {
        self.fallbacks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn retained_ids(&self, count: usize) {
        self.peak
            .fetch_max(count, std::sync::atomic::Ordering::Relaxed);
    }
}
