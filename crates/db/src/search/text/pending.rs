//! Analyses of unpublished text that text searches reuse.
//!
//! A text search overlays every unpublished document of its partition: it
//! analyzes each one for exact BM25 statistics and scores it from that
//! analysis. Under sustained ingest nearly all of them were already pending
//! for the previous search, so [`PendingTextAnalyses`] keeps their analyses
//! keyed by the queued operation that carries them.
//!
//! # Exactness
//!
//! A queued operation's payload is immutable and its random 121-bit ID is
//! never reused, and one generation, which the queue target names, has one
//! analyzer, so a cached analysis is exactly what analyzing the operation's
//! text again would produce. Nothing invalidates an entry: publication,
//! supersession, a rebuilt generation, or a dropped index only make it
//! unreachable, since no later search selects its operation. Searches still
//! compare each reused analysis's text with the queued text and fail closed
//! on a mismatch.
//!
//! # Memory
//!
//! Per generation queue and partition the cache holds the analyses of the
//! latest selection a strong search analyzed there: each such search
//! replaces the partition's entries, dropping operations publication
//! acknowledged or a newer write superseded since. Least recently used
//! partitions give up analyses, only as many as a replacement needs, to keep
//! the bytes their analyses were charged within the budget, one strong
//! search's bound. The index worker also drops a generation's entries once
//! nothing of it stays queued, and a strong search that finds its queue
//! empty does too, so a published or dropped index keeps none. A search
//! pinned to a view from before such a drop may cache its selection again;
//! its next strong search or eviction drops it. Nothing here is persisted,
//! so a restart or an eviction only costs the next search its analysis
//! again.
//!
//! Strong searches analyze what the cache lacks one at a time
//! ([`PendingTextAnalyses::analyzing`]) and score documents from these
//! analyses without copying them, so however many run at once their
//! committed text takes about two bounds of analyses: one cached, one being
//! analyzed. A search also keeps what it read until it finishes, even
//! analyses evicted meanwhile, and its write transaction's own documents,
//! which are never cached.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::encoding::v2::values::indexes::operation_queue::QueuedOperationId;
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::work::TextPartition;

use super::IndexedTextAnalysis;

/// Analyses of one partition's queued text, by queued operation.
pub(crate) type PartitionAnalyses = HashMap<QueuedOperationId, Arc<IndexedTextAnalysis>>;

/// Shared analyses of queued text, bounded by the bytes their analysis
/// charged ([`IndexedTextAnalysis::retained_bytes`]).
#[derive(Debug)]
pub(crate) struct PendingTextAnalyses {
    budget: NonZeroU64,
    state: Mutex<CacheState>,
    /// Held by the one strong search analyzing text the cache lacks.
    analyzing: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Default)]
struct CacheState {
    /// Charged bytes of every cached analysis.
    held: u64,
    /// Last recency stamp handed out.
    clock: u64,
    targets: HashMap<QueueTarget, HashMap<TextPartition, CachedPartition>>,
    /// Every cached partition by its recency stamp, least recent first.
    recency: BTreeMap<u64, (QueueTarget, TextPartition)>,
}

#[derive(Debug)]
struct CachedPartition {
    /// Recency stamp, also this partition's key in `recency`.
    used: u64,
    /// Charged bytes of `analyses`.
    bytes: u64,
    /// Shared with searches reading it, so a lookup holds the lock only to
    /// clone it.
    analyses: Arc<PartitionAnalyses>,
}

impl CacheState {
    fn stamp(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Removes `target`'s `partition`, releasing its bytes, and returns it so
    /// the caller frees it after unlocking.
    fn remove(
        &mut self,
        target: QueueTarget,
        partition: &TextPartition,
    ) -> Option<CachedPartition> {
        let partitions = self.targets.get_mut(&target)?;
        let removed = partitions.remove(partition)?;
        if partitions.is_empty() {
            self.targets.remove(&target);
        }
        self.release(&removed);
        Some(removed)
    }

    fn release(&mut self, removed: &CachedPartition) {
        self.held -= removed.bytes;
        self.recency
            .remove(&removed.used)
            .expect("every cached partition has a recency stamp");
    }

    /// Releases at least `excess` bytes of `target`'s `partition`, dropping
    /// its analyses in no particular order and keeping the rest, or the whole
    /// partition when nothing would be left. Returns what it replaced so the
    /// caller frees it after unlocking.
    fn shed(
        &mut self,
        target: QueueTarget,
        partition: &TextPartition,
        excess: u64,
    ) -> Option<Arc<PartitionAnalyses>> {
        let cached = self.targets.get_mut(&target)?.get_mut(partition)?;
        let mut dropped = 0;
        let kept = cached
            .analyses
            .iter()
            .filter(|(_, analysis)| {
                let keep = dropped >= excess;
                if !keep {
                    dropped += analysis.retained_bytes();
                }
                keep
            })
            .map(|(operation, analysis)| (*operation, Arc::clone(analysis)))
            .collect::<PartitionAnalyses>();
        if kept.is_empty() {
            return self
                .remove(target, partition)
                .map(|removed| removed.analyses);
        }
        cached.bytes -= dropped;
        let shed = std::mem::replace(&mut cached.analyses, Arc::new(kept));
        self.held -= dropped;
        Some(shed)
    }
}

impl PendingTextAnalyses {
    /// Holds analyses charged at most `budget` bytes in total.
    pub(crate) fn new(budget: NonZeroU64) -> Self {
        Self {
            budget,
            state: Mutex::new(CacheState::default()),
            analyzing: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Waits for the turn to analyze text the cache lacks, held until the
    /// returned guard drops.
    ///
    /// Strong searches take turns, in arrival order, so analysis of unpublished
    /// text in progress stays within one strong search's bound however many
    /// search at once, and a search that waited finds what the one before it
    /// analyzed already cached. The guard is owned so the analysis it admits
    /// can hold it on the blocking pool until that analysis is cached or
    /// stops, even after its request stopped awaiting it. Reading and
    /// replacing entries never waits.
    pub(crate) async fn analyzing(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.analyzing).lock_owned().await
    }

    /// Returns the analyses cached for `target`'s `partition`, if any, and
    /// marks the partition used.
    pub(crate) fn get(
        &self,
        target: QueueTarget,
        partition: &TextPartition,
    ) -> Option<Arc<PartitionAnalyses>> {
        let mut state = self.state.lock();
        let stamp = state.stamp();
        let state = &mut *state;
        let cached = state.targets.get_mut(&target)?.get_mut(partition)?;
        let key = state
            .recency
            .remove(&cached.used)
            .expect("every cached partition has a recency stamp");
        state.recency.insert(stamp, key);
        cached.used = stamp;
        Some(Arc::clone(&cached.analyses))
    }

    /// Replaces the analyses cached for `target`'s `partition` with exactly
    /// `analyses`, the partition's committed selection as one strong search
    /// analyzed it.
    ///
    /// Other partitions give up analyses, least recently used first and
    /// each only as many as the new entries still need, until those fit the
    /// budget: partitions whose backlogs together exceed it then reanalyze
    /// only the excess. Entries that alone exceed the budget, or an empty
    /// selection, leave the partition uncached and evict nothing else.
    pub(crate) fn replace(
        &self,
        target: QueueTarget,
        partition: &TextPartition,
        analyses: PartitionAnalyses,
    ) {
        let bytes = analyses
            .values()
            .map(|analysis| analysis.retained_bytes())
            .fold(0, u64::saturating_add);
        let mut state = self.state.lock();
        let mut freed = state
            .remove(target, partition)
            .map(|removed| removed.analyses)
            .into_iter()
            .collect::<Vec<_>>();
        if !analyses.is_empty() && bytes <= self.budget.get() {
            while state.held + bytes > self.budget.get() {
                let excess = state.held + bytes - self.budget.get();
                let (oldest_target, oldest_partition) = state
                    .recency
                    .first_key_value()
                    .map(|(_, key)| key.clone())
                    .expect("held bytes belong to a cached partition");
                freed.extend(state.shed(oldest_target, &oldest_partition, excess));
            }
            let used = state.stamp();
            state.held += bytes;
            state.recency.insert(used, (target, partition.clone()));
            state.targets.entry(target).or_default().insert(
                partition.clone(),
                CachedPartition {
                    used,
                    bytes,
                    analyses: Arc::new(analyses),
                },
            );
        }
        drop(state);
        drop(freed);
    }

    /// Drops every analysis cached for `target`, nothing of whose queue is
    /// left to search.
    pub(crate) fn forget(&self, target: QueueTarget) {
        let mut state = self.state.lock();
        let Some(partitions) = state.targets.remove(&target) else {
            return;
        };
        partitions
            .values()
            .for_each(|removed| state.release(removed));
        drop(state);
        drop(partitions);
    }

    /// Charged bytes of every cached analysis.
    #[cfg(test)]
    pub(crate) fn held_bytes(&self) -> u64 {
        self.state.lock().held
    }

    /// Number of analyses cached for `target`'s `partition`, without marking
    /// it used.
    #[cfg(test)]
    pub(crate) fn cached(&self, target: QueueTarget, partition: &TextPartition) -> usize {
        self.state
            .lock()
            .targets
            .get(&target)
            .and_then(|partitions| partitions.get(partition))
            .map_or(0, |cached| cached.analyses.len())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::config::TextAnalyzerKind;
    use crate::encoding::v2::keys::scope::DataScope;
    use crate::index_lifecycle::{IndexGenerationId, IndexId};
    use crate::search::text::{analyze_text_for_indexing, TextAnalysisMemoryBudget};

    fn target(generation: u64) -> QueueTarget {
        QueueTarget::new(
            DataScope::LegacyUnscoped,
            IndexId::new(3).unwrap(),
            IndexGenerationId::new(generation).unwrap(),
        )
    }

    fn operation(id: u128) -> QueuedOperationId {
        QueuedOperationId::try_from_u128(id).unwrap()
    }

    fn analysis(text: &str) -> Arc<IndexedTextAnalysis> {
        Arc::new(
            analyze_text_for_indexing(
                TextAnalyzerKind::Standard,
                text.to_string(),
                &mut TextAnalysisMemoryBudget::new(NonZeroU64::MAX),
            )
            .unwrap(),
        )
    }

    fn analyses(entries: &[(u128, &Arc<IndexedTextAnalysis>)]) -> PartitionAnalyses {
        entries
            .iter()
            .map(|(id, analysis)| (operation(*id), Arc::clone(analysis)))
            .collect()
    }

    fn found(
        cache: &PendingTextAnalyses,
        target: QueueTarget,
        partition: &TextPartition,
        ids: &[u128],
    ) -> Vec<bool> {
        let cached = cache.get(target, partition);
        ids.iter()
            .map(|id| {
                cached
                    .as_ref()
                    .is_some_and(|cached| cached.contains_key(&operation(*id)))
            })
            .collect()
    }

    fn tenant(name: &'static str) -> TextPartition {
        TextPartition::TenantValue(Bytes::from_static(name.as_bytes()))
    }

    #[test]
    fn a_replacement_keeps_exactly_the_latest_selection() {
        let (one, two) = (analysis("alpha beta"), analysis("gamma"));
        let charge = one.retained_bytes() + two.retained_bytes();
        let cache = PendingTextAnalyses::new(NonZeroU64::new(charge).unwrap());
        let all = TextPartition::Unpartitioned;
        assert!(cache.get(target(1), &all).is_none());

        cache.replace(target(1), &all, analyses(&[(1, &one), (2, &two)]));
        assert_eq!(cache.held_bytes(), charge, "exactly the budget fits");
        let hits = cache.get(target(1), &all).unwrap();
        assert!(Arc::ptr_eq(&hits[&operation(2)], &two));
        assert!(Arc::ptr_eq(&hits[&operation(1)], &one));
        assert!(!hits.contains_key(&operation(9)));

        // A later selection without operation 1 (published) drops it, while
        // a search still holding the earlier snapshot keeps reading it.
        cache.replace(target(1), &all, analyses(&[(2, &two)]));
        assert_eq!(found(&cache, target(1), &all, &[1, 2]), [false, true]);
        assert_eq!(cache.held_bytes(), two.retained_bytes());
        assert!(Arc::ptr_eq(&hits[&operation(1)], &one));
        // Another generation of the same index is a different queue.
        assert_eq!(found(&cache, target(2), &all, &[2]), [false]);

        // An empty selection leaves nothing cached.
        cache.replace(target(1), &all, HashMap::new());
        assert_eq!(cache.held_bytes(), 0);
        assert_eq!(cache.cached(target(1), &all), 0);
    }

    #[test]
    fn a_selection_one_byte_past_the_budget_is_not_cached() {
        let (one, two) = (analysis("alpha beta"), analysis("gamma"));
        let charge = one.retained_bytes() + two.retained_bytes();
        let cache = PendingTextAnalyses::new(NonZeroU64::new(charge - 1).unwrap());
        let all = TextPartition::Unpartitioned;
        cache.replace(target(1), &all, analyses(&[(1, &one)]));
        assert_eq!(cache.cached(target(1), &all), 1);
        // Replacing it with a selection one byte past the budget drops the
        // partition rather than keep a stale or partial selection.
        cache.replace(target(1), &all, analyses(&[(1, &one), (2, &two)]));
        assert_eq!(cache.cached(target(1), &all), 0);
        assert_eq!(cache.held_bytes(), 0);
        assert!(cache.get(target(1), &all).is_none());
    }

    #[test]
    fn partitions_are_cached_and_evicted_least_recently_used_first() {
        let text = analysis("alpha");
        let charge = text.retained_bytes();
        let cache = PendingTextAnalyses::new(NonZeroU64::new(2 * charge).unwrap());
        let (a, b, c) = (tenant("a"), tenant("b"), tenant("c"));
        cache.replace(target(1), &a, analyses(&[(1, &text)]));
        cache.replace(target(1), &b, analyses(&[(2, &text)]));
        // Tenant `a` is used after `b`, so `b` is the least recently used.
        assert_eq!(found(&cache, target(1), &a, &[1]), [true]);
        assert_eq!(found(&cache, target(1), &a, &[2]), [false], "per partition");
        cache.replace(target(1), &c, analyses(&[(3, &text)]));
        assert_eq!(
            [&a, &b, &c].map(|partition| cache.cached(target(1), partition)),
            [1, 0, 1]
        );
        assert_eq!(cache.held_bytes(), 2 * charge);

        // A selection larger than the whole budget is never cached and
        // evicts nothing else.
        cache.replace(
            target(1),
            &b,
            analyses(&[(4, &text), (5, &text), (6, &text)]),
        );
        assert_eq!(
            [&a, &b, &c].map(|partition| cache.cached(target(1), partition)),
            [1, 0, 1]
        );
        // Replacing a cached partition releases its own bytes first, so a
        // selection of the whole budget evicts only the other partition.
        cache.replace(target(1), &a, analyses(&[(1, &text), (7, &text)]));
        assert_eq!(
            [&a, &b, &c].map(|partition| cache.cached(target(1), partition)),
            [2, 0, 0]
        );
        assert_eq!(cache.held_bytes(), 2 * charge);
    }

    /// Partitions whose backlogs together exceed the budget keep what fits:
    /// a replacement takes only the bytes it needs from the least recently
    /// used partition, so alternating searches reanalyze only the excess,
    /// and a partition left with nothing is dropped whole.
    #[test]
    fn a_replacement_sheds_only_the_excess_of_older_partitions() {
        let text = analysis("alpha");
        let charge = text.retained_bytes();
        let cache = PendingTextAnalyses::new(NonZeroU64::new(4 * charge).unwrap());
        let (a, b, c) = (tenant("a"), tenant("b"), tenant("c"));
        let three =
            |first: u128| analyses(&[(first, &text), (first + 1, &text), (first + 2, &text)]);
        cache.replace(target(1), &a, three(1));
        cache.replace(target(1), &b, three(4));
        assert_eq!(
            [&a, &b].map(|partition| cache.cached(target(1), partition)),
            [1, 3]
        );
        assert_eq!(cache.held_bytes(), 4 * charge);
        // The survivor of `a` is a subset of its selection, still reusable.
        let survivor = cache.get(target(1), &a).unwrap();
        assert!(survivor.keys().all(|id| (1..=3).contains(&id.get())));
        cache.replace(target(1), &a, three(1));
        assert_eq!(
            [&a, &b].map(|partition| cache.cached(target(1), partition)),
            [3, 1]
        );
        assert_eq!(cache.held_bytes(), 4 * charge);

        // A partition that one analysis past the excess would empty is
        // dropped whole, though that frees more than needed.
        let long = analysis("alpha beta gamma delta");
        assert!(long.retained_bytes() > charge);
        let cache =
            PendingTextAnalyses::new(NonZeroU64::new(long.retained_bytes() + 2 * charge).unwrap());
        cache.replace(target(1), &b, analyses(&[(3, &long)]));
        cache.replace(target(1), &a, analyses(&[(1, &text), (2, &text)]));
        cache.replace(target(1), &c, analyses(&[(4, &text)]));
        assert_eq!(
            [&a, &b, &c].map(|partition| cache.cached(target(1), partition)),
            [2, 0, 1]
        );
        assert_eq!(cache.held_bytes(), 3 * charge);
    }

    #[test]
    fn forgetting_a_target_drops_every_partition_of_it_only() {
        let text = analysis("alpha");
        let charge = text.retained_bytes();
        let cache = PendingTextAnalyses::new(NonZeroU64::new(3 * charge).unwrap());
        let tenant = tenant("a");
        cache.replace(
            target(1),
            &TextPartition::Unpartitioned,
            analyses(&[(1, &text)]),
        );
        cache.replace(target(1), &tenant, analyses(&[(2, &text)]));
        cache.replace(target(2), &tenant, analyses(&[(3, &text)]));
        cache.forget(target(1));
        cache.forget(target(3));
        assert_eq!(cache.held_bytes(), charge);
        assert_eq!(cache.cached(target(1), &tenant), 0);
        assert_eq!(cache.cached(target(2), &tenant), 1);
        // The released bytes are reusable without evicting the survivor, and
        // the released recency stamps are never consulted again.
        cache.replace(target(1), &tenant, analyses(&[(4, &text), (5, &text)]));
        assert_eq!(cache.held_bytes(), 3 * charge);
        assert_eq!(cache.cached(target(2), &tenant), 1);
        assert_eq!(found(&cache, target(1), &tenant, &[4, 5]), [true, true]);
        cache.replace(target(3), &tenant, analyses(&[(6, &text)]));
        assert_eq!(cache.cached(target(2), &tenant), 0, "least recently used");
        assert_eq!(cache.held_bytes(), 3 * charge);
    }

    /// A search pinned to a view from before its generation's queue drained
    /// may cache that view's selection after the drop: the entries are
    /// unreachable, since no later selection names their operations, and the
    /// next strong search of the partition replaces them.
    #[test]
    fn a_selection_cached_after_a_forget_is_replaced_by_the_next_search() {
        let text = analysis("alpha");
        let cache = PendingTextAnalyses::new(NonZeroU64::MAX);
        let all = TextPartition::Unpartitioned;
        cache.replace(target(1), &all, analyses(&[(1, &text)]));
        cache.forget(target(1));
        cache.replace(target(1), &all, analyses(&[(1, &text)]));
        assert_eq!(cache.held_bytes(), text.retained_bytes());
        cache.replace(target(1), &all, analyses(&[(2, &text)]));
        assert_eq!(found(&cache, target(1), &all, &[1, 2]), [false, true]);
        assert_eq!(cache.held_bytes(), text.retained_bytes());
    }
}
