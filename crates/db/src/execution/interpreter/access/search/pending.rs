//! Pending-data selection for searches over committed but unpublished work.
//!
//! One pinned request view supplies the physical index, the outstanding
//! operation queue, and (for text) indexed-entity statistics, so an overlay
//! never mixes snapshots. A search only ever sees a pending entity at its
//! latest state in that view, never at an earlier state of its chain: a build
//! or publication may already have written the latest state physically, and
//! an earlier one would hide it. Strong searches select every pending entity,
//! and a strong vector search fails with retryable index backpressure rather
//! than decode and score more than its configured bound; eventual searches
//! select the oldest pending entities whose latest operations fit their
//! budget and leave the rest to their physical representation. Only decoding
//! and searching follow those bounds; reading the queue still follows the
//! backlog (see
//! [`crate::index_lifecycle::queue::storage::QueueStore::read_latest`]).
//! One request reads and decodes each searched queue once ([`PendingSets`]),
//! on the blocking pool.
//! Write transactions read the queue through their serializable transaction,
//! additionally overlay their own uncommitted changes from the write context,
//! and always search strongly. No publication clears the physical results
//! their own changes supersede, so those never count toward the suppression
//! limit.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use helix_ast::query::SearchConsistency;
use roaring::RoaringTreemap;

use super::*;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueuedOperationId, QueuedPayload,
};
use crate::index_lifecycle::queue::producer::PendingEntityState;
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::IndexIdentity;
use crate::search::text::pending::PendingTextAnalyses;
use crate::search::text::IndexedTextAnalysis;

/// Latest searchable value of one pending entity.
#[derive(Debug, Clone)]
pub(super) enum PendingValue {
    /// Exact queued vector components.
    Vector(Arc<[f32]>),
    /// Exact queued text.
    Text(Arc<str>),
}

/// One selected pending entity and its latest state in the pinned view.
#[derive(Debug, Clone)]
pub(super) struct PendingEntity {
    pub(super) entity: IndexEntity,
    /// `None` means the latest state deletes the entity from the index.
    pub(super) latest: Option<(TextPartition, PendingValue)>,
}

impl PendingEntity {
    /// The latest text a text search of `partition` analyzes for the entity:
    /// `None` when it is deleted, moved to another partition, or a vector.
    pub(super) fn text_in(&self, partition: &TextPartition) -> Option<&str> {
        match &self.latest {
            Some((latest, PendingValue::Text(text))) if latest == partition => Some(text),
            Some(_) | None => None,
        }
    }
}

/// Where an overlaid entity's latest state comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingSource {
    /// The committed queued operation whose payload it is. A queued
    /// operation's payload never changes and its ID is never reused, so the
    /// ID names that state for good ([`PendingTextAnalyses`]).
    Committed(QueuedOperationId),
    /// The searching write transaction's own uncommitted change.
    Local,
}

/// One committed pending entity and the queued operation carrying its latest
/// state.
#[derive(Debug, Clone)]
struct CommittedEntity {
    operation: QueuedOperationId,
    pending: PendingEntity,
}

/// Committed pending entities of one generation queue as one request view
/// sees them: decoded once and shared by every search of the request.
#[derive(Debug, Clone)]
struct CommittedPending {
    /// Entities in selection order, each at its latest state in the view.
    entities: Arc<[CommittedEntity]>,
    /// Exactly the IDs of `entities`; a generation indexes one element kind,
    /// so the ID alone identifies an entity.
    superseded: Arc<RoaringTreemap>,
}

impl CommittedPending {
    fn new(entities: Vec<CommittedEntity>) -> Self {
        let superseded = entities
            .iter()
            .map(|committed| committed.pending.entity.id.get())
            .collect::<RoaringTreemap>();
        debug_assert_eq!(
            superseded.len(),
            entities.len() as u64,
            "a selection names each entity once"
        );
        Self {
            entities: Arc::from(entities),
            superseded: Arc::new(superseded),
        }
    }
}

/// What one request view's read of a generation queue gives its searches.
#[derive(Debug, Clone)]
enum CommittedRead {
    /// The committed pending entities a search under the read's consistency
    /// overlays.
    Selected(CommittedPending),
    /// Committed pending vector work a strong search may not decode: its
    /// selection reached `reached` retained bytes, past `limit`, when it
    /// stopped.
    PastStrongVectorBound { reached: u64, limit: u64 },
}

/// One committed pending set, read once by whichever search needs it first.
type PendingSet = Arc<tokio::sync::OnceCell<CommittedRead>>;

/// Committed pending sets read in one request view: one per searched
/// generation queue and search consistency, shared by every search of the
/// request.
///
/// # Contract
///
/// A set is read and decoded at most once while its view stands, however
/// many searches, steps, `ForEach` frames, or parallel step contexts of the
/// request search its queue; concurrent first searches wait for one read.
/// A failed or abandoned read leaves the set unread, so the next search reads
/// it again and fails again if the queue is corrupt.
///
/// A read request's view is one immutable snapshot, which every parallel
/// step context shares, so they share one instance. A write transaction reads
/// the committed queue through itself and stages its own queue operands only
/// at commit, so the committed queue it reads never changes while it stands:
/// a set stays exact across its writes, and each search overlays the
/// transaction's current changes on it afresh. The owning context forgets
/// every set whenever the transaction opens, commits, or aborts, when an
/// isolated mutation scope commits, and on index DDL, exactly where it
/// forgets prepared memberships.
///
/// Sets live until the request ends or forgets them, so a request holds the
/// decoded pending work of every queue it searched at once; each is bounded
/// as one search of it is.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct PendingSets {
    sets: parking_lot::Mutex<HashMap<(QueueTarget, SearchConsistency), PendingSet>>,
    /// Queue reads made, for tests that prove reuse.
    #[cfg(test)]
    reads: std::sync::atomic::AtomicUsize,
}

impl PendingSets {
    /// The set of `target` under `consistency`, produced by `read` only when
    /// no search of this view has produced it yet.
    async fn get_or_read(
        &self,
        target: QueueTarget,
        consistency: SearchConsistency,
        read: impl std::future::Future<Output = Result<CommittedRead>>,
    ) -> Result<CommittedRead> {
        // The guard is released before awaiting the set.
        let set = Arc::clone(self.sets.lock().entry((target, consistency)).or_default());
        set.get_or_try_init(|| {
            #[cfg(test)]
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            read
        })
        .await
        .cloned()
    }

    /// Returns how many queue reads the sets made.
    #[cfg(test)]
    pub(in crate::execution::interpreter) fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The searching write transaction's own changes to one generation, at their
/// latest states.
#[derive(Debug, Default)]
struct LocalChanges {
    /// Exactly the IDs of `entities`.
    ids: RoaringTreemap,
    entities: Vec<PendingEntity>,
}

/// The entities one search overlays: a selected prefix of the request view's
/// committed pending entities, with the searching write transaction's own
/// latest states in place of theirs.
///
/// Cloning shares the entities instead of copying them, so work on the
/// blocking pool can own a clone.
#[derive(Debug, Clone)]
pub(super) struct PendingEntities {
    committed: Arc<[CommittedEntity]>,
    /// Length of the selected prefix of `committed`.
    selected: usize,
    /// Empty for read requests.
    local: Arc<LocalChanges>,
}

impl PendingEntities {
    /// Every overlaid entity at its latest state: selected committed
    /// entities in selection order, then the write transaction's own.
    pub(super) fn iter(&self) -> impl Iterator<Item = &PendingEntity> {
        self.sourced().map(|(_, pending)| pending)
    }

    /// [`Self::iter`], with where each latest state comes from.
    fn sourced(&self) -> impl Iterator<Item = (PendingSource, &PendingEntity)> {
        self.selected_committed()
            .filter(|committed| !self.local.ids.contains(committed.pending.entity.id.get()))
            .map(|committed| {
                (
                    PendingSource::Committed(committed.operation),
                    &committed.pending,
                )
            })
            .chain(
                self.local
                    .entities
                    .iter()
                    .map(|pending| (PendingSource::Local, pending)),
            )
    }

    /// Selected committed entities, including those the write transaction
    /// changed since.
    fn selected_committed(&self) -> std::slice::Iter<'_, CommittedEntity> {
        self.committed[..self.selected].iter()
    }

    /// Number of overlaid entities; walks the selection.
    pub(super) fn len(&self) -> usize {
        self.iter().count()
    }

    fn is_empty(&self) -> bool {
        self.selected == 0 && self.local.entities.is_empty()
    }
}

/// Pending entities selected for one search.
#[derive(Debug)]
pub(super) struct PendingSelection {
    /// Generation queue the entities were selected from.
    target: QueueTarget,
    /// Consistency the entities were selected under. Eventual selections
    /// belong to read requests and hold no local changes.
    consistency: SearchConsistency,
    entities: PendingEntities,
    /// Entities whose physical representation is superseded: exactly the
    /// IDs of [`Self::entities`].
    pub(super) superseded: Arc<RoaringTreemap>,
}

impl PendingSelection {
    fn new(
        target: QueueTarget,
        consistency: SearchConsistency,
        committed: CommittedPending,
    ) -> Self {
        Self {
            target,
            consistency,
            entities: PendingEntities {
                selected: committed.entities.len(),
                committed: committed.entities,
                local: Arc::default(),
            },
            superseded: committed.superseded,
        }
    }

    /// Strongly selects `committed` entities with a write transaction's own
    /// `local` changes overlaid on their latest states.
    ///
    /// Costs one pass over the local changes plus, when there are any, one
    /// union of the committed and local IDs, not a scan of the committed
    /// backlog per change.
    fn overlaid<'a>(
        target: QueueTarget,
        committed: CommittedPending,
        local: impl Iterator<Item = (IndexEntity, &'a PendingEntityState)>,
    ) -> Self {
        let mut selection = Self::new(target, SearchConsistency::Strong, committed);
        let entities = local
            .map(|(entity, state)| PendingEntity {
                entity,
                latest: match state {
                    PendingEntityState::Vector { current, .. } => {
                        current.as_ref().map(|document| {
                            (
                                document.partition().clone(),
                                PendingValue::Vector(Arc::from(document.vector())),
                            )
                        })
                    }
                    PendingEntityState::Text { current, .. } => current.as_ref().map(|document| {
                        (
                            document.partition.clone(),
                            PendingValue::Text(Arc::clone(&document.text)),
                        )
                    }),
                },
            })
            .collect::<Vec<_>>();
        if entities.is_empty() {
            return selection;
        }
        let ids = entities
            .iter()
            .map(|pending| pending.entity.id.get())
            .collect::<RoaringTreemap>();
        assert_eq!(
            ids.len(),
            entities.len() as u64,
            "a transaction changes each entity of a generation once"
        );
        selection.superseded = Arc::new(&*selection.superseded | &ids);
        selection.entities.local = Arc::new(LocalChanges { ids, entities });
        selection
    }

    /// A strong read selection of `entities`, for tests of overlay consumers.
    /// Entity `n` of `entities` is the payload of queued operation `n + 1`.
    #[cfg(test)]
    pub(super) fn strong_for_tests(target: QueueTarget, entities: Vec<PendingEntity>) -> Self {
        Self::new(
            target,
            SearchConsistency::Strong,
            CommittedPending::new(
                (1..)
                    .zip(entities)
                    .map(|(operation, pending)| CommittedEntity {
                        operation: QueuedOperationId::try_from_u128(operation)
                            .expect("a positive operation ID is valid"),
                        pending,
                    })
                    .collect(),
            ),
        )
    }

    /// Whether the selection holds every pending entity, as strong search
    /// requires.
    pub(super) const fn is_strong(&self) -> bool {
        matches!(self.consistency, SearchConsistency::Strong)
    }

    /// The entities this search overlays.
    pub(super) const fn entities(&self) -> &PendingEntities {
        &self.entities
    }

    /// IDs of the superseded entities the searching write transaction changed
    /// itself, or `None` for an eventual selection, which has none.
    pub(super) fn local(&self) -> Option<&RoaringTreemap> {
        match self.consistency {
            SearchConsistency::Strong => Some(&self.entities.local.ids),
            SearchConsistency::Eventual => None,
        }
    }

    /// Keeps only the first `end` committed entities of an eventual
    /// selection, leaving the rest to their published representation.
    fn truncate_eventual(&mut self, end: usize) {
        debug_assert!(
            self.consistency == SearchConsistency::Eventual && self.entities.local.ids.is_empty(),
            "only an eventual read selection shrinks"
        );
        self.entities.selected = end.min(self.entities.selected);
        self.superseded = Arc::new(
            self.entities
                .selected_committed()
                .map(|committed| committed.pending.entity.id.get())
                .collect(),
        );
    }

    /// Resolves a search whose answer lies behind more than
    /// [`MAX_SUPPRESSED_SEARCH_RESULTS`] physical results superseded by
    /// committed work; its last attempt skipped `skipped` of those. Results
    /// superseded by the searching write transaction's own changes never
    /// count, since no publication clears them.
    ///
    /// Strong search fails with retryable index backpressure rather than
    /// widen with the backlog or answer without a committed change. Eventual
    /// search keeps its first [`MAX_SUPPRESSED_SEARCH_RESULTS`] entities in
    /// selection order and leaves the rest to their published representation
    /// until publication, so a rerun supersedes at most that many results and
    /// always settles.
    ///
    /// [`MAX_SUPPRESSED_SEARCH_RESULTS`]: super::limits::MAX_SUPPRESSED_SEARCH_RESULTS
    pub(super) fn yield_to_suppression_limit(&mut self, skipped: usize) -> Result<()> {
        let limit = super::limits::MAX_SUPPRESSED_SEARCH_RESULTS;
        match self.consistency {
            SearchConsistency::Strong => Err(HelixDbError::IndexBackpressure {
                scope: self.target.scope,
                index_id: self.target.index_id.get(),
                resource: crate::error::IndexBackpressureResource::SuppressedSearchResults,
                requested: u64::try_from(skipped).unwrap_or(u64::MAX),
                limit: limit as u64,
            }),
            SearchConsistency::Eventual if self.entities.selected > limit => {
                self.truncate_eventual(limit);
                Ok(())
            }
            SearchConsistency::Eventual => Err(HelixDbError::InvariantViolation(format!(
                "a search superseding {} entities skipped {skipped} results, past a suppression \
                 limit it cannot reach",
                self.entities.selected
            ))),
        }
    }

    /// Analyzes the unpublished text one text search overlays in
    /// `partition`, bounded to `limit` analysis bytes, and returns each
    /// overlaid entity's analyzed latest document there, in
    /// [`PendingEntities::iter`] order: `None` where
    /// [`PendingEntity::text_in`] has none.
    ///
    /// A text overlay needs the latest document of every selected entity in
    /// the searched partition, for corpus statistics and to score it,
    /// whether or not a traversal restricts the search. That work
    /// grows with the committed backlog rather than with `k`, so it is
    /// charged exactly as one publication charges the documents it analyzes
    /// ([`crate::search::text::TextAnalysisMemoryBudget`]: text bytes plus a
    /// fixed overhead per retained token, so dense short-token text costs
    /// far more than its length) and bounded by `limit`. Charging stops at
    /// the first token past the bound, so a search never analyzes more than
    /// the bound either. It runs on the blocking pool
    /// ([`super::blocking::run_blocking`]) and stops before the next document
    /// it would analyze once the request stops awaiting it.
    ///
    /// Committed documents reuse the analyses `cache` holds for their queued
    /// operations and are charged exactly what analyzing them again would,
    /// up to the same refusal; the rest are analyzed here. A strong selection
    /// holds every committed operation of the generation, so it replaces the
    /// partition's cached analyses with the ones it analyzed or reused, even
    /// when it stops at the bound: searches repeated past the bound then
    /// reuse the analyses instead of redoing them. Eventual selections only
    /// read the cache.
    ///
    /// A strong selection analyzes afresh only while it holds the turn
    /// ([`PendingTextAnalyses::analyzing`]): reaching a document it would
    /// analyze afresh without it, it stops, waits for the turn, and starts
    /// over reading the cache again. The turn passes on only once the holder
    /// has cached what it analyzed or stopped, so however many strong
    /// searches run at once they analyze at most one bound together, and
    /// those that waited reuse what the one before them cached rather than
    /// repeat it. Eventual selections never wait; one publication's budget
    /// bounds each.
    ///
    /// The searching write transaction's own documents are analyzed and
    /// charged first, and never cached: no publication clears them, so a
    /// write whose own documents exceed the bound fails with
    /// [`HelixDbError::IndexOperationBatchTooLarge`]. Past the bound with
    /// committed documents, strong search fails with retryable index
    /// backpressure rather than analyze more; within it, it stays exact.
    /// Eventual search keeps the longest prefix of its selection within the
    /// bound and leaves the rest to their published representation until
    /// publication. Either error reports the charge where analysis stopped,
    /// whether or not the cache held the documents before it.
    ///
    /// Text of entities the index worker holds back
    /// ([`crate::BlockedIndexEntity`]) counts too, although no publication
    /// drains it: only a writer knows which entities those are, and exempting
    /// them would let analysis grow with the retained backlog again. While
    /// held-back text alone exceeds the bound, which takes publication limits
    /// lowered below documents already admitted and a bound no larger than
    /// them, strong text searches of the partition keep failing until a
    /// later write to each of those entities publishes or the bound is
    /// raised.
    pub(super) async fn analyze_text(
        &mut self,
        control: &crate::execution_control::ExecutionControl,
        partition: &TextPartition,
        analyzer: crate::config::TextAnalyzerKind,
        limit: NonZeroU64,
        cache: &Arc<PendingTextAnalyses>,
    ) -> Result<Vec<Option<Arc<IndexedTextAnalysis>>>> {
        let strong = self.is_strong();
        // One pass over the selection, or `None` when a strong pass without
        // the turn reached a document it would analyze afresh.
        let pass = |turn: Option<tokio::sync::OwnedMutexGuard<()>>| {
            let entities = self.entities.clone();
            let (target, partition, cache) = (self.target, partition.clone(), Arc::clone(cache));
            super::blocking::run_blocking(control, move |probe| {
                let may_analyze = !strong || turn.is_some();
                let cached = cache.get(target, &partition);
                let mut budget = crate::search::text::TextAnalysisMemoryBudget::new(limit);
                // The analysis, or the charge the bound refused.
                let analyze = |text: &str,
                               budget: &mut crate::search::text::TextAnalysisMemoryBudget|
                 -> Result<std::result::Result<Arc<IndexedTextAnalysis>, u64>> {
                    probe.check()?;
                    match crate::search::text::analyze_text_for_indexing(
                        analyzer,
                        text.to_owned(),
                        budget,
                    ) {
                        Ok(analysis) => Ok(Ok(Arc::new(analysis))),
                        Err(HelixDbError::ActiveTextMutationLimitExceeded { observed, .. }) => {
                            Ok(Err(observed))
                        }
                        Err(error) => Err(error),
                    }
                };
                let mut local = Vec::with_capacity(entities.local.entities.len());
                for pending in &entities.local.entities {
                    let Some(text) = pending.text_in(&partition) else {
                        local.push(None);
                        continue;
                    };
                    if !may_analyze {
                        return Ok(None);
                    }
                    local.push(Some(analyze(text, &mut budget)?.map_err(|observed| {
                        HelixDbError::IndexOperationBatchTooLarge {
                            index_id: target.index_id.get(),
                            resource:
                                crate::error::IndexOperationBatchResource::PendingTextAnalysisBytes,
                            observed,
                            limit: limit.get(),
                        }
                    })?));
                }
                // A strong selection names every committed operation, so the
                // analyses it reaches replace the partition's cached ones.
                let mut reached = strong.then(HashMap::new);
                let mut analyses = Vec::with_capacity(entities.selected + local.len());
                let mut refused = None;
                for (position, committed) in entities.selected_committed().enumerate() {
                    let pending = &committed.pending;
                    if entities.local.ids.contains(pending.entity.id.get()) {
                        continue;
                    }
                    let Some(text) = pending.text_in(&partition) else {
                        analyses.push(None);
                        continue;
                    };
                    let charged = match cached
                        .as_ref()
                        .and_then(|cached| cached.get(&committed.operation))
                    {
                        Some(cached) if cached.text() != text => {
                            return Err(HelixDbError::InvariantViolation(
                                "a cached pending text analysis differs from its queued operation"
                                    .to_string(),
                            ));
                        }
                        Some(cached) => match cached.recharge(&mut budget) {
                            Ok(()) => Ok(Arc::clone(cached)),
                            Err(HelixDbError::ActiveTextMutationLimitExceeded {
                                observed, ..
                            }) => Err(observed),
                            Err(error) => return Err(error),
                        },
                        None if !may_analyze => return Ok(None),
                        None => analyze(text, &mut budget)?,
                    };
                    let analysis = match charged {
                        Ok(analysis) => analysis,
                        Err(observed) => {
                            refused = Some((position, observed));
                            break;
                        }
                    };
                    if let Some(reached) = &mut reached {
                        reached.insert(committed.operation, Arc::clone(&analysis));
                    }
                    analyses.push(Some(analysis));
                }
                if let Some(reached) = reached {
                    cache.replace(target, &partition, reached);
                }
                // The next strong search takes the turn only once the cache
                // holds what this one analyzed.
                drop(turn);
                analyses.extend(local);
                Ok(Some((analyses, refused)))
            })
        };
        let (analyses, refused) = match pass(None).await? {
            Some(analyzed) => analyzed,
            None => pass(Some(cache.analyzing().await)).await?.ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "a text analysis holding the turn stopped to wait for it".to_string(),
                )
            })?,
        };
        let Some((end, requested)) = refused else {
            return Ok(analyses);
        };
        match self.consistency {
            SearchConsistency::Strong => Err(HelixDbError::IndexBackpressure {
                scope: self.target.scope,
                index_id: self.target.index_id.get(),
                resource: crate::error::IndexBackpressureResource::PendingTextAnalysisBytes,
                requested,
                limit: limit.get(),
            }),
            SearchConsistency::Eventual => {
                debug_assert_eq!(
                    analyses.len(),
                    end,
                    "an eventual selection has no local text"
                );
                self.truncate_eventual(end);
                Ok(analyses)
            }
        }
    }
}

impl<'db> ExecutionContext<'db> {
    /// Selects pending entities for one index search, or `None` when none
    /// is pending or no queued publication is configured.
    ///
    /// Every selected entity is searched at its latest state in the view.
    /// Strong search selects every pending entity; a strong vector search
    /// whose committed pending entities' latest operations retain more than
    /// [`IndexOperationQueueTuning::strong_vector_search_max_pending_bytes`]
    /// fails with retryable index backpressure instead of decoding and
    /// scoring them. Eventual search selects
    /// entities in the order of their oldest pending operation until the
    /// next one's latest operation would exceed the per-search source-input
    /// budget; unselected entities keep their physical representation, stale
    /// or not, until published, and an entity is never selected at an
    /// earlier state, which could be older than that representation. The
    /// bounds cover decoding and searching, not the queue read: the map
    /// layout fetches its whole value, and while merge operands are pending
    /// above its base SlateDB resolves them against all of it, validating
    /// every record, so that read costs the backlog and fails on a corrupt
    /// record the budget never selects (see
    /// [`crate::index_lifecycle::queue::storage::QueueStore::read_latest`]).
    /// One request reads and decodes each queue once per consistency
    /// ([`PendingSets`]), decoding on the blocking pool. An eventual search
    /// may shrink its selection further to stay within the suppression limit
    /// (see [`PendingSelection::yield_to_suppression_limit`]). Write
    /// transactions are always strong and add their own uncommitted changes,
    /// which never count toward the strong vector bound: the transaction
    /// bounds them itself.
    ///
    /// A write transaction reads the queue through its serializable
    /// transaction, so the searched generation's queue becomes a read
    /// dependency: a concurrent commit that enqueues to or acknowledges that
    /// generation aborts this transaction with a retryable conflict instead
    /// of letting it commit a decision made without that work. Writes that do
    /// not search read no queue and keep committing concurrently; reusing a
    /// set keeps the dependency its first read recorded.
    ///
    /// [`IndexOperationQueueTuning::strong_vector_search_max_pending_bytes`]: crate::config::IndexOperationQueueTuning::strong_vector_search_max_pending_bytes
    pub(super) async fn pending_selection(
        &self,
        identity: &IndexIdentity,
        family: QueueFamily,
    ) -> Result<Option<PendingSelection>> {
        let (target, consistency, read) = if let Some(active) = self.active_write_tx() {
            let Some(handle) = crate::index_lifecycle::repository::load_active_handle(
                &active.txn,
                self.tenant_scope,
                identity,
            )
            .await?
            else {
                return Ok(None);
            };
            let target =
                QueueTarget::new(self.tenant_scope, handle.index_id(), handle.generation());
            let read = self
                .committed_pending(&active.txn, target, family, SearchConsistency::Strong)
                .await?;
            (target, SearchConsistency::Strong, read)
        } else if let Some(view) = self.request_read_view() {
            let Some(handle) = crate::index_lifecycle::repository::load_active_handle(
                view,
                self.tenant_scope,
                identity,
            )
            .await?
            else {
                return Ok(None);
            };
            let target =
                QueueTarget::new(self.tenant_scope, handle.index_id(), handle.generation());
            let read = self
                .committed_pending(view, target, family, self.search_consistency)
                .await?;
            (target, self.search_consistency, read)
        } else {
            return Ok(None);
        };
        let committed = match read {
            CommittedRead::Selected(committed) => committed,
            CommittedRead::PastStrongVectorBound { reached, limit } => {
                return Err(HelixDbError::IndexBackpressure {
                    scope: target.scope,
                    index_id: target.index_id.get(),
                    resource: crate::error::IndexBackpressureResource::PendingVectorBytes,
                    requested: reached,
                    limit,
                });
            }
        };
        let selection = match self.active_write_tx() {
            Some(active) => PendingSelection::overlaid(
                target,
                committed,
                active
                    .index_context
                    .queued_mutations()
                    .pending_entities(target),
            ),
            None => PendingSelection::new(target, consistency, committed),
        };
        Ok((!selection.entities.is_empty()).then_some(selection))
    }

    /// The committed pending entities of `target` a search under
    /// `consistency` overlays, read through `read` once per request view
    /// ([`PendingSets`]) and decoded on the blocking pool.
    ///
    /// A strong vector search decodes within
    /// `strong_vector_search_max_pending_bytes` and reports a selection the
    /// bound ends instead of decoding past it; a strong text search decodes
    /// everything; an eventual search decodes within its source-input budget.
    async fn committed_pending(
        &self,
        read: &(impl slatedb::DbReadOps + Sync),
        target: QueueTarget,
        family: QueueFamily,
        consistency: SearchConsistency,
    ) -> Result<CommittedRead> {
        let tuning = self.db.config().db().index_operation_queue();
        let budget = match (consistency, family) {
            (SearchConsistency::Eventual, _) => tuning.eventual_search_budget(),
            (SearchConsistency::Strong, QueueFamily::Vector) => {
                tuning.strong_vector_search_max_pending_bytes().get()
            }
            (SearchConsistency::Strong, QueueFamily::Text) => u64::MAX,
        };
        let load = async move {
            let Some(bytes) = self
                .db
                .index_queue_store()
                .read_latest(read, target)
                .await?
            else {
                return Ok(CommittedRead::Selected(CommittedPending::new(Vec::new())));
            };
            super::blocking::run_blocking(&self.execution_control, move |probe| {
                probe.check()?;
                let latest = bytes.decode_latest(budget)?;
                if latest.family() != family {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "search overlay read another family's operation queue".to_string(),
                    ));
                }
                match (consistency, family, latest.refused()) {
                    (SearchConsistency::Strong, QueueFamily::Vector, Some(reached)) => {
                        return Ok(CommittedRead::PastStrongVectorBound {
                            reached,
                            limit: budget,
                        });
                    }
                    (SearchConsistency::Strong, QueueFamily::Text, Some(_)) => {
                        return Err(HelixDbError::InvariantViolation(
                            "an unbounded strong text overlay left pending work out".to_string(),
                        ));
                    }
                    (SearchConsistency::Strong, _, None) | (SearchConsistency::Eventual, _, _) => {}
                }
                probe.check()?;
                Ok(CommittedRead::Selected(CommittedPending::new(
                    latest
                        .into_operations()
                        .into_iter()
                        .map(|operation| CommittedEntity {
                            operation: operation.id(),
                            pending: PendingEntity {
                                entity: operation.entity(),
                                latest: latest_value(operation.payload()),
                            },
                        })
                        .collect(),
                )))
            })
            .await
        };
        self.pending_sets
            .get_or_read(target, consistency, load)
            .await
    }
}

fn latest_value(payload: &QueuedPayload) -> Option<(TextPartition, PendingValue)> {
    match payload {
        QueuedPayload::Vector(payload) => payload.replacement.as_ref().map(|replacement| {
            (
                replacement.partition().clone(),
                PendingValue::Vector(replacement.shared_vector()),
            )
        }),
        QueuedPayload::Text(payload) => payload.replacement.as_ref().map(|replacement| {
            (
                replacement.partition().clone(),
                PendingValue::Text(replacement.shared_text()),
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::error_code::QueryErrorCode;
    use helix_ast::{batch, traversal, value::PropertyInput};
    use helix_planner::context::ParamBindings;
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::config::{
        DbConfig, IndexOperationQueueTuning, QueueLayout, TextIndexDefinition,
        VectorIndexDefinition,
    };
    use crate::encoding::v2::keys::DataScope;
    use crate::error::{IndexBackpressureResource, IndexOperationBatchResource};
    use crate::index_lifecycle::queue::publication::PublicationOutcome;
    use crate::index_lifecycle::{
        IndexElementKind, IndexEntityId, IndexGenerationId, IndexId,
        ValidatedDynamicIndexDefinition,
    };
    use crate::search::vector::VectorDistanceMetric;
    use crate::HelixDB;

    /// Opens a queued database, publication paused, with `Doc` vector and
    /// text indexes.
    async fn open(name: &str, layout: QueueLayout) -> HelixDB {
        let db = HelixDB::open_with_object_store_and_config(
            name,
            Arc::new(InMemory::new()),
            DbConfig::new().with_index_operation_queue_tuning(
                IndexOperationQueueTuning::default()
                    .with_layout(layout)
                    .with_publication_paused_for_tests(),
            ),
        )
        .await
        .unwrap();
        for definition in [
            ValidatedDynamicIndexDefinition::try_from(
                VectorIndexDefinition::new_node(
                    "Doc",
                    "embedding",
                    2,
                    VectorDistanceMetric::Euclidean,
                )
                .unwrap(),
            )
            .unwrap(),
            ValidatedDynamicIndexDefinition::try_from(
                TextIndexDefinition::new_node("Doc", "body").unwrap(),
            )
            .unwrap(),
        ] {
            db.install_index_for_tests(definition).await.unwrap();
        }
        db
    }

    /// Executes one write batch in a request transaction left uncommitted.
    async fn staged(db: &HelixDB, batch: batch::WriteBatch) -> ExecutionContext<'_> {
        let plan = helix_planner::planning::plan_write_batch(
            &batch,
            &db.planner_context(ParamBindings::default()),
        )
        .unwrap();
        let mut context = ExecutionContext::new(db, ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        context
            .execute_steps(
                plan.steps(),
                plan.execution_order(),
                plan.root(),
                plan.execution_program(),
            )
            .await
            .unwrap();
        context
    }

    fn insert(properties: Vec<(&'static str, PropertyInput)>) -> batch::WriteBatch {
        batch::write_batch().var_as("created", traversal::g().add_n("Doc", properties))
    }

    fn target() -> QueueTarget {
        QueueTarget::new(
            DataScope::LegacyUnscoped,
            IndexId::new(7).unwrap(),
            IndexGenerationId::new(9).unwrap(),
        )
    }

    fn node(id: u64) -> IndexEntity {
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(id),
        }
    }

    /// The queued operation of fixture entity `id`.
    fn operation(id: u64) -> QueuedOperationId {
        QueuedOperationId::try_from_u128(u128::from(id) + 1).unwrap()
    }

    /// Committed node `id`, carried by [`operation`] `id`, at `latest`.
    fn committed(id: u64, latest: Option<(TextPartition, PendingValue)>) -> CommittedEntity {
        CommittedEntity {
            operation: operation(id),
            pending: PendingEntity {
                entity: node(id),
                latest,
            },
        }
    }

    /// `count` committed deletions of nodes, oldest first by ID.
    fn deleted(count: u64) -> CommittedPending {
        CommittedPending::new((0..count).map(|id| committed(id, None)).collect())
    }

    /// A selection of `count` deleted nodes, oldest first by ID.
    fn deletions(consistency: SearchConsistency, count: u64) -> PendingSelection {
        PendingSelection::new(target(), consistency, deleted(count))
    }

    fn selected(selection: &PendingSelection) -> Vec<u64> {
        selection
            .entities()
            .iter()
            .map(|pending| pending.entity.id.get())
            .collect()
    }

    #[test]
    fn strong_selections_reject_searches_past_the_suppression_limit() {
        let limit = super::super::limits::MAX_SUPPRESSED_SEARCH_RESULTS;
        let mut strong = deletions(SearchConsistency::Strong, limit as u64 + 1);
        let error = strong
            .yield_to_suppression_limit(limit + 1)
            .expect_err("strong search never answers without a committed change");
        assert!(
            matches!(
                error,
                HelixDbError::IndexBackpressure {
                    scope: DataScope::LegacyUnscoped,
                    index_id: 7,
                    resource: IndexBackpressureResource::SuppressedSearchResults,
                    requested,
                    limit: reported,
                } if requested == limit as u64 + 1 && reported == limit as u64
            ),
            "{error}"
        );
        assert!(error.is_index_backpressure());
        assert_eq!(selected(&strong).len(), limit + 1, "the selection is kept");
    }

    #[test]
    fn eventual_selections_keep_their_oldest_entities_within_the_suppression_limit() {
        let limit = super::super::limits::MAX_SUPPRESSED_SEARCH_RESULTS;
        let mut eventual = deletions(SearchConsistency::Eventual, limit as u64 + 5);
        eventual.yield_to_suppression_limit(limit + 1).unwrap();
        let oldest = (0..limit as u64).collect::<Vec<_>>();
        assert_eq!(selected(&eventual), oldest);
        assert_eq!(
            *eventual.superseded,
            oldest.into_iter().collect::<RoaringTreemap>()
        );
        // A selection within the limit can never be past it.
        let error = eventual
            .yield_to_suppression_limit(limit + 1)
            .expect_err("a selection within the limit cannot exceed it");
        assert!(
            matches!(error, HelixDbError::InvariantViolation(_)),
            "{error}"
        );
    }

    #[test]
    fn overlaid_selections_supersede_and_mark_every_local_change() {
        let removed = PendingEntityState::Vector {
            first: None,
            current: None,
        };
        let committed = CommittedPending::new(
            (0..3)
                .map(|id| {
                    committed(
                        id,
                        Some((
                            TextPartition::Unpartitioned,
                            PendingValue::Vector(Arc::from([id as f32, 0.0])),
                        )),
                    )
                })
                .collect(),
        );
        // Local changes to a committed pending entity and to one without
        // committed work.
        let selection = PendingSelection::overlaid(
            target(),
            committed.clone(),
            [(node(2), &removed), (node(5), &removed)].into_iter(),
        );
        assert_eq!(selected(&selection), [0, 1, 2, 5]);
        assert!(
            selection
                .entities()
                .iter()
                .filter(|pending| [2, 5].contains(&pending.entity.id.get()))
                .all(|pending| pending.latest.is_none()),
            "the transaction's own latest state replaces the committed one"
        );
        assert_eq!(
            *selection.superseded,
            [0, 1, 2, 5].into_iter().collect::<RoaringTreemap>()
        );
        assert_eq!(
            selection.local(),
            Some(&[2, 5].into_iter().collect::<RoaringTreemap>()),
            "both local changes are exempt from the suppression limit"
        );
        // The shared committed set is untouched for the request's other
        // searches.
        assert_eq!(
            *committed.superseded,
            [0, 1, 2].into_iter().collect::<RoaringTreemap>()
        );
        assert!(committed
            .entities
            .iter()
            .all(|committed| committed.pending.latest.is_some()));
        assert_eq!(
            selection
                .entities()
                .sourced()
                .map(|(source, _)| source)
                .collect::<Vec<_>>(),
            [
                PendingSource::Committed(operation(0)),
                PendingSource::Committed(operation(1)),
                PendingSource::Local,
                PendingSource::Local,
            ],
            "a local change is never served from its committed operation"
        );
        assert_eq!(selection.entities().len(), 4);

        let selection = PendingSelection::overlaid(
            target(),
            deleted(3),
            std::iter::empty::<(IndexEntity, &PendingEntityState)>(),
        );
        assert_eq!(selection.local(), Some(&RoaringTreemap::new()));
        assert_eq!(deletions(SearchConsistency::Eventual, 1).local(), None);
    }

    #[test]
    #[should_panic(expected = "a transaction changes each entity of a generation once")]
    fn an_overlay_rejects_a_local_entity_changed_twice() {
        let removed = PendingEntityState::Vector {
            first: None,
            current: None,
        };
        PendingSelection::overlaid(
            target(),
            deleted(1),
            [(node(4), &removed), (node(4), &removed)].into_iter(),
        );
    }

    /// A selection whose entity `id` has latest state `documents[id]`: text in
    /// a partition, or a deletion.
    fn texts(
        consistency: SearchConsistency,
        documents: &[Option<(TextPartition, &str)>],
    ) -> PendingSelection {
        PendingSelection::new(target(), consistency, committed_texts(documents))
    }

    fn committed_texts(documents: &[Option<(TextPartition, &str)>]) -> CommittedPending {
        CommittedPending::new(
            documents
                .iter()
                .enumerate()
                .map(|(id, document)| {
                    committed(
                        id as u64,
                        document.clone().map(|(partition, text)| {
                            (partition, PendingValue::Text(Arc::from(text)))
                        }),
                    )
                })
                .collect(),
        )
    }

    const ANALYZER: crate::config::TextAnalyzerKind = crate::config::TextAnalyzerKind::Standard;

    /// What one publication's analysis charges for `texts`.
    fn charge(texts: &[&str]) -> u64 {
        texts
            .iter()
            .map(|text| {
                crate::search::text::analyze_text_within_budget(
                    ANALYZER,
                    text,
                    &mut crate::search::text::TextAnalysisMemoryBudget::new(NonZeroU64::MAX),
                )
                .unwrap()
                .1
                .analysis_bytes
            })
            .sum()
    }

    fn bound(bytes: u64) -> NonZeroU64 {
        NonZeroU64::new(bytes).unwrap()
    }

    /// `committed` with one write transaction's own `text` overlaid as entity
    /// `id` in `partition`.
    fn with_local(
        committed: CommittedPending,
        id: u64,
        partition: &TextPartition,
        text: &str,
    ) -> PendingSelection {
        let local = PendingEntityState::Text {
            first: None,
            current: Some(
                crate::index_lifecycle::queue::producer::QueuedTextDocument {
                    partition: partition.clone(),
                    text: Arc::from(text),
                },
            ),
        };
        PendingSelection::overlaid(target(), committed, std::iter::once((node(id), &local)))
    }

    /// A cache no fixture selection fills.
    fn unbounded_cache() -> Arc<PendingTextAnalyses> {
        Arc::new(PendingTextAnalyses::new(NonZeroU64::MAX))
    }

    /// Analyzes `selection`'s text in `partition` within `limit` with
    /// `cache`, for a request without a deadline.
    async fn analyze(
        selection: &mut PendingSelection,
        partition: &TextPartition,
        limit: NonZeroU64,
        cache: &Arc<PendingTextAnalyses>,
    ) -> Result<Vec<Option<Arc<IndexedTextAnalysis>>>> {
        selection
            .analyze_text(
                &crate::execution_control::ExecutionControl::unlimited(),
                partition,
                ANALYZER,
                limit,
                cache,
            )
            .await
    }

    #[tokio::test]
    async fn text_analysis_charges_only_text_in_the_searched_partition() {
        let searched = TextPartition::Unpartitioned;
        let other = TextPartition::try_tenant_value(bytes::Bytes::from_static(b"t")).unwrap();
        let documents = [
            Some((searched.clone(), "abcd")),
            None,
            Some((other, "a much longer document elsewhere")),
            Some((searched.clone(), "efgh")),
        ];
        // Exactly at the bound: nothing changes and strong stays exact.
        let limit = bound(charge(&["abcd", "efgh"]));
        for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
            let mut selection = texts(consistency, &documents);
            analyze(&mut selection, &searched, limit, &unbounded_cache())
                .await
                .unwrap();
            assert_eq!(selected(&selection), [0, 1, 2, 3], "{consistency:?}");
            assert_eq!(
                *selection.superseded,
                [0, 1, 2, 3].into_iter().collect::<RoaringTreemap>()
            );
        }
    }

    /// The charge is per retained token, not per byte: short dense tokens
    /// cost far more than their length, so a bound in text bytes would let
    /// them through.
    #[tokio::test]
    async fn dense_short_tokens_are_charged_per_token() {
        let searched = TextPartition::Unpartitioned;
        let dense = "a a a a a a a a";
        let wide = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
        assert!(dense.len() < wide.len() && charge(&[dense]) > 4 * charge(&[wide]));
        let limit = bound(charge(&[wide, wide]));
        let mut strong = texts(
            SearchConsistency::Strong,
            &[
                Some((searched.clone(), wide)),
                Some((searched.clone(), wide)),
            ],
        );
        analyze(&mut strong, &searched, limit, &unbounded_cache())
            .await
            .unwrap();
        let mut strong = texts(
            SearchConsistency::Strong,
            &[Some((searched.clone(), dense))],
        );
        let error = analyze(&mut strong, &searched, limit, &unbounded_cache())
            .await
            .expect_err("dense text shorter than the bound exceeds it");
        assert!(
            matches!(
                error,
                HelixDbError::IndexBackpressure {
                    resource: IndexBackpressureResource::PendingTextAnalysisBytes,
                    ..
                }
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn strong_text_analysis_past_the_bound_fails_with_backpressure() {
        let searched = TextPartition::Unpartitioned;
        let mut strong = texts(
            SearchConsistency::Strong,
            &[
                Some((searched.clone(), "abcd")),
                Some((searched.clone(), "efgh")),
                None,
                Some((searched.clone(), "ij")),
            ],
        );
        // "ij" crosses the bound at its only token, so the charge reached is
        // the whole selection's.
        let limit = charge(&["abcd", "efgh"]) + 2;
        let error = analyze(&mut strong, &searched, bound(limit), &unbounded_cache())
            .await
            .expect_err("strong search never analyzes past the bound");
        let requested = charge(&["abcd", "efgh", "ij"]);
        assert!(
            matches!(
                error,
                HelixDbError::IndexBackpressure {
                    scope: DataScope::LegacyUnscoped,
                    index_id: 7,
                    resource: IndexBackpressureResource::PendingTextAnalysisBytes,
                    requested: reached,
                    limit: reported,
                } if reached == requested && reported == limit
            ),
            "{error}"
        );
        assert!(error.is_index_backpressure());
        assert_eq!(selected(&strong), [0, 1, 2, 3], "the selection is kept");
    }

    #[tokio::test]
    async fn eventual_text_analysis_keeps_the_longest_prefix_within_the_bound() {
        let searched = TextPartition::Unpartitioned;
        let documents = [
            Some((searched.clone(), "abcd")),
            None,
            Some((searched.clone(), "efgh")),
            None,
            Some((searched.clone(), "ij")),
        ];
        let mut eventual = texts(SearchConsistency::Eventual, &documents);
        let analyses = analyze(
            &mut eventual,
            &searched,
            bound(charge(&["abcd", "efgh"]) - 1),
            &unbounded_cache(),
        )
        .await
        .unwrap();
        // The deletion after the first document costs nothing, but the
        // prefix ends at the first document past the bound.
        assert_eq!(selected(&eventual), [0, 1]);
        assert_eq!(analyzed_entities(&analyses), [true, false]);
        assert_eq!(
            *eventual.superseded,
            [0, 1].into_iter().collect::<RoaringTreemap>()
        );
        // A first document past the bound leaves nothing to overlay.
        let mut eventual = texts(SearchConsistency::Eventual, &documents);
        let analyses = analyze(
            &mut eventual,
            &searched,
            bound(charge(&["abcd"]) - 1),
            &unbounded_cache(),
        )
        .await
        .unwrap();
        assert!(analyses.is_empty());
        assert!(eventual.entities().is_empty());
        assert!(eventual.superseded.is_empty());
    }

    /// A write transaction's own documents are charged first. No publication
    /// clears them, so past the bound alone they fail the write for good;
    /// committed documents past the bound beside them fail it retryably.
    #[tokio::test]
    async fn a_write_transactions_own_text_is_charged_first_and_fails_it_for_good() {
        let searched = TextPartition::Unpartitioned;
        let other = TextPartition::try_tenant_value(bytes::Bytes::from_static(b"t")).unwrap();
        let committed = || {
            committed_texts(&[
                Some((searched.clone(), "abcd")),
                Some((searched.clone(), "efgh")),
            ])
        };
        let own = "one two three";
        let limit = charge(&["abcd", "efgh"]);
        assert!(charge(&[own]) > limit);
        // Its own document past the bound alone, even overlaying a committed
        // entity, can never be searched.
        for id in [1, 2] {
            let mut selection = with_local(committed(), id, &searched, own);
            let error = analyze(&mut selection, &searched, bound(limit), &unbounded_cache())
                .await
                .expect_err("a write's own text past the bound fails it");
            assert!(
                matches!(
                    error,
                    HelixDbError::IndexOperationBatchTooLarge {
                        index_id: 7,
                        resource: IndexOperationBatchResource::PendingTextAnalysisBytes,
                        observed,
                        limit: reported,
                    } if observed == charge(&[own]) && reported == limit
                ),
                "{error}"
            );
            assert!(!error.is_index_backpressure());
        }
        // Within the bound, committed documents past it beside it fail the
        // search retryably, charging its own document first.
        let mut selection = with_local(committed(), 2, &searched, "ij");
        let error = analyze(&mut selection, &searched, bound(limit), &unbounded_cache())
            .await
            .expect_err("committed text past the bound beside the write's own fails it");
        assert!(
            matches!(
                error,
                HelixDbError::IndexBackpressure {
                    resource: IndexBackpressureResource::PendingTextAnalysisBytes,
                    requested,
                    ..
                } if requested == charge(&["ij", "abcd", "efgh"])
            ),
            "{error}"
        );
        // Its own text in another partition costs nothing.
        let mut selection = with_local(committed(), 2, &other, own);
        analyze(&mut selection, &searched, bound(limit), &unbounded_cache())
            .await
            .unwrap();
        assert_eq!(selected(&selection), [0, 1, 2]);
    }

    /// Analyses `selection` returned, as whether each entity has one.
    fn analyzed_entities(analyses: &[Option<Arc<IndexedTextAnalysis>>]) -> Vec<bool> {
        analyses.iter().map(Option::is_some).collect()
    }

    /// A strong selection analyzes each document once: it caches the
    /// analyses of its committed operations, and a later selection of the
    /// same operations reuses them and is charged exactly the same, while a
    /// selection without one (published) drops it from the cache.
    #[tokio::test]
    async fn strong_selections_cache_and_reuse_committed_analyses() {
        let searched = TextPartition::Unpartitioned;
        let other = TextPartition::try_tenant_value(bytes::Bytes::from_static(b"t")).unwrap();
        let documents = [
            Some((searched.clone(), "abcd")),
            None,
            Some((other.clone(), "elsewhere")),
            Some((searched.clone(), "efgh ij")),
        ];
        let cache = unbounded_cache();
        let limit = bound(charge(&["abcd", "efgh ij"]));
        let first = analyze(
            &mut texts(SearchConsistency::Strong, &documents),
            &searched,
            limit,
            &cache,
        )
        .await
        .unwrap();
        assert_eq!(analyzed_entities(&first), [true, false, false, true]);
        assert_eq!(first[0].as_ref().unwrap().text(), "abcd");
        assert_eq!(cache.cached(target(), &searched), 2);
        assert_eq!(
            cache.cached(target(), &other),
            0,
            "only the searched partition"
        );
        assert_eq!(cache.held_bytes(), limit.get());

        let second = analyze(
            &mut texts(SearchConsistency::Strong, &documents),
            &searched,
            limit,
            &cache,
        )
        .await
        .unwrap();
        for (reused, analyzed) in second.iter().zip(&first) {
            assert_eq!(reused.is_some(), analyzed.is_some());
            if let (Some(reused), Some(analyzed)) = (reused, analyzed) {
                assert!(Arc::ptr_eq(reused, analyzed), "the analysis is reused");
            }
        }
        // Reused analyses are charged what analyzing them charged: one byte
        // less fails exactly where analysis would.
        let error = analyze(
            &mut texts(SearchConsistency::Strong, &documents),
            &searched,
            bound(limit.get() - 1),
            &cache,
        )
        .await
        .expect_err("reused analyses still count toward the bound");
        assert!(
            matches!(
                error,
                HelixDbError::IndexBackpressure {
                    resource: IndexBackpressureResource::PendingTextAnalysisBytes,
                    requested,
                    ..
                } if requested == limit.get()
            ),
            "{error}"
        );

        // Publication acknowledged the first operation: a selection without
        // it replaces the partition's entries.
        let published = CommittedPending::new(
            committed_texts(&documents)
                .entities
                .iter()
                .skip(1)
                .cloned()
                .collect(),
        );
        analyze(
            &mut PendingSelection::new(target(), SearchConsistency::Strong, published),
            &searched,
            limit,
            &cache,
        )
        .await
        .unwrap();
        assert_eq!(cache.cached(target(), &searched), 1);
        assert_eq!(cache.held_bytes(), charge(&["efgh ij"]));
    }

    /// A strong search past the bound caches the analyses it made before
    /// stopping, so a retry before publication reuses them; eventual
    /// searches only read the cache.
    #[tokio::test]
    async fn refused_strong_and_eventual_selections_keep_the_cache_useful() {
        let searched = TextPartition::Unpartitioned;
        let documents = [
            Some((searched.clone(), "abcd")),
            Some((searched.clone(), "efgh")),
            Some((searched.clone(), "ij")),
        ];
        let cache = unbounded_cache();
        let limit = bound(charge(&["abcd", "efgh"]));
        let eventual = analyze(
            &mut texts(SearchConsistency::Eventual, &documents),
            &searched,
            limit,
            &cache,
        )
        .await
        .unwrap();
        assert_eq!(analyzed_entities(&eventual), [true, true]);
        assert_eq!(
            cache.held_bytes(),
            0,
            "eventual selections never fill the cache"
        );

        for attempt in 0..2 {
            let error = analyze(
                &mut texts(SearchConsistency::Strong, &documents),
                &searched,
                limit,
                &cache,
            )
            .await
            .expect_err("the backlog exceeds the bound");
            assert!(
                matches!(
                    error,
                    HelixDbError::IndexBackpressure {
                        resource: IndexBackpressureResource::PendingTextAnalysisBytes,
                        requested,
                        ..
                    } if requested == charge(&["abcd", "efgh"]) + 2
                ),
                "attempt {attempt}: {error}"
            );
            assert_eq!(cache.cached(target(), &searched), 2, "attempt {attempt}");
        }
        // An eventual selection reuses them and keeps its prefix.
        let mut eventual = texts(SearchConsistency::Eventual, &documents);
        let reused = analyze(&mut eventual, &searched, limit, &cache)
            .await
            .unwrap();
        assert_eq!(selected(&eventual), [0, 1]);
        let cached = cache.get(target(), &searched).unwrap();
        for (id, analysis) in reused.iter().enumerate() {
            assert!(Arc::ptr_eq(
                analysis.as_ref().unwrap(),
                &cached[&operation(id as u64)]
            ));
        }
        // A cached document past the bound is refused where analyzing it
        // would be: reserving its text, or at the token where analysis
        // stops, whether every document is cached or none is.
        let warm = || async {
            let cache = unbounded_cache();
            analyze(
                &mut texts(SearchConsistency::Strong, &documents),
                &searched,
                bound(u64::MAX),
                &cache,
            )
            .await
            .unwrap();
            assert_eq!(cache.cached(target(), &searched), 3);
            cache
        };
        for (limit, requested) in [
            (
                charge(&["abcd"]) + 1,
                charge(&["abcd"]) + "efgh".len() as u64,
            ),
            (charge(&["abcd", "efgh"]) - 1, charge(&["abcd", "efgh"])),
        ] {
            for cache in [warm().await, unbounded_cache()] {
                let error = analyze(
                    &mut texts(SearchConsistency::Strong, &documents),
                    &searched,
                    bound(limit),
                    &cache,
                )
                .await
                .expect_err("the second document does not fit");
                assert!(
                    matches!(
                        error,
                        HelixDbError::IndexBackpressure { requested: reached, .. }
                            if reached == requested
                    ),
                    "{limit}: {error}"
                );
            }
        }
    }

    /// A write transaction's own documents are analyzed for each search and
    /// never cached, including one that overlays a committed entity.
    #[tokio::test]
    async fn a_write_transactions_own_text_is_never_cached() {
        let searched = TextPartition::Unpartitioned;
        let cache = unbounded_cache();
        let mut selection = with_local(
            committed_texts(&[
                Some((searched.clone(), "abcd")),
                Some((searched.clone(), "efgh")),
            ]),
            1,
            &searched,
            "own",
        );
        assert_eq!(
            selection
                .entities()
                .sourced()
                .map(|(source, pending)| (source, pending.entity.id.get()))
                .collect::<Vec<_>>(),
            [
                (PendingSource::Committed(operation(0)), 0),
                (PendingSource::Local, 1)
            ]
        );
        let analyses = analyze(&mut selection, &searched, bound(u64::MAX), &cache)
            .await
            .unwrap();
        assert_eq!(analyzed_entities(&analyses), [true, true]);
        assert_eq!(analyses[1].as_ref().unwrap().text(), "own");
        assert_eq!(cache.cached(target(), &searched), 1);
        assert_eq!(cache.held_bytes(), charge(&["abcd"]));
    }

    /// An operation ID names one immutable payload, so a cached analysis of
    /// other text is an invariant violation rather than a stale hit.
    #[tokio::test]
    async fn a_cached_analysis_of_other_text_fails_closed() {
        let searched = TextPartition::Unpartitioned;
        let cache = unbounded_cache();
        cache.replace(
            target(),
            &searched,
            HashMap::from([(operation(0), fresh_analysis("other"))]),
        );
        let error = analyze(
            &mut texts(
                SearchConsistency::Strong,
                &[Some((searched.clone(), "abcd"))],
            ),
            &searched,
            bound(u64::MAX),
            &cache,
        )
        .await
        .expect_err("a mismatched cached analysis is never used");
        assert!(
            matches!(error, HelixDbError::InvariantViolation(_)),
            "{error}"
        );
    }

    /// `text` analyzed outside any search.
    fn fresh_analysis(text: &str) -> Arc<IndexedTextAnalysis> {
        Arc::new(
            crate::search::text::analyze_text_for_indexing(
                ANALYZER,
                text.to_string(),
                &mut crate::search::text::TextAnalysisMemoryBudget::new(NonZeroU64::MAX),
            )
            .unwrap(),
        )
    }

    /// Whether `analysis` finishes while the test holds the analysis turn.
    /// One that waits for the turn never does; one that does not wait
    /// finishes within the generous timeout.
    async fn finishes_without_the_turn<T>(analysis: impl std::future::Future<Output = T>) -> bool {
        tokio::time::timeout(std::time::Duration::from_secs(30), analysis)
            .await
            .is_ok()
    }

    /// Strong selections that would analyze text afresh take turns: one
    /// waiting for the turn then reuses what the holder cached instead of
    /// analyzing it again, while selections analyzing nothing afresh and
    /// eventual ones never wait. A write transaction's own text always does.
    #[tokio::test]
    async fn strong_selections_take_turns_to_analyze_uncached_text() {
        let searched = TextPartition::Unpartitioned;
        let other = TextPartition::try_tenant_value(bytes::Bytes::from_static(b"t")).unwrap();
        let documents = [
            Some((searched.clone(), "abcd")),
            None,
            Some((searched.clone(), "efgh")),
        ];
        let cache = unbounded_cache();
        let limit = bound(u64::MAX);
        let turn = cache.analyzing().await;
        for (consistency, documents) in [
            (SearchConsistency::Eventual, &documents[..]),
            (
                SearchConsistency::Strong,
                &[None, Some((other.clone(), "abcd"))],
            ),
        ] {
            let mut selection = texts(consistency, documents);
            assert!(
                finishes_without_the_turn(analyze(&mut selection, &searched, limit, &cache)).await,
                "{consistency:?} {documents:?}"
            );
        }
        assert_eq!(cache.held_bytes(), 0);

        let mut cold = texts(SearchConsistency::Strong, &documents);
        let mut cold = Box::pin(analyze(&mut cold, &searched, limit, &cache));
        // It starts, finds nothing cached, and waits for the turn.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut cold)
                .await
                .is_err()
        );
        // The holder caches the selection while the cold search waits.
        let held = [(0, "abcd"), (2, "efgh")]
            .into_iter()
            .map(|(id, text)| (operation(id), fresh_analysis(text)))
            .collect::<HashMap<_, _>>();
        cache.replace(target(), &searched, held.clone());
        let mut warm = texts(SearchConsistency::Strong, &documents);
        assert!(
            finishes_without_the_turn(analyze(&mut warm, &searched, limit, &cache)).await,
            "a cached selection never waits"
        );
        let mut local = with_local(committed_texts(&documents), 1, &searched, "own");
        let mut local = Box::pin(analyze(&mut local, &searched, limit, &cache));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut local)
                .await
                .is_err(),
            "its own text is never cached"
        );

        drop(turn);
        let reused = cold.await.unwrap();
        assert_eq!(analyzed_entities(&reused), [true, false, true]);
        for (id, analysis) in [(0, &reused[0]), (2, &reused[2])] {
            assert!(Arc::ptr_eq(
                analysis.as_ref().unwrap(),
                &held[&operation(id)]
            ));
        }
        let own = local.await.unwrap();
        assert_eq!(own[2].as_ref().unwrap().text(), "own");
    }

    /// An expired request analyzes nothing, and a request dropped while it
    /// waits for the turn leaves the turn to the next one.
    #[tokio::test]
    async fn abandoned_analyses_leave_the_cache_and_the_turn_to_others() {
        let searched = TextPartition::Unpartitioned;
        let cache = unbounded_cache();
        let documents = (0..64)
            .map(|_| Some((searched.clone(), "abcd")))
            .collect::<Vec<_>>();
        let error = texts(SearchConsistency::Strong, &documents)
            .analyze_text(
                &crate::execution_control::ExecutionControl::from_timeout(
                    std::time::Duration::ZERO,
                ),
                &searched,
                ANALYZER,
                bound(u64::MAX),
                &cache,
            )
            .await
            .expect_err("an expired request analyzes nothing");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));
        assert_eq!(cache.held_bytes(), 0);

        // A request dropped while waiting for the turn leaves it to the
        // next one.
        let turn = cache.analyzing().await;
        let mut abandoned = texts(SearchConsistency::Strong, &documents);
        let mut abandoned = Box::pin(analyze(&mut abandoned, &searched, bound(u64::MAX), &cache));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut abandoned)
                .await
                .is_err()
        );
        drop(abandoned);
        drop(turn);
        let analyses = analyze(
            &mut texts(SearchConsistency::Strong, &documents),
            &searched,
            bound(u64::MAX),
            &cache,
        )
        .await
        .unwrap();
        assert_eq!(analyses.len(), documents.len());
        assert_eq!(cache.cached(target(), &searched), documents.len());
    }

    /// The property `family` indexes, set for a document at `position`.
    fn indexed(family: QueueFamily, position: f32) -> (&'static str, PropertyInput) {
        match family {
            QueueFamily::Vector => ("embedding", PropertyInput::from(vec![position, 0.0])),
            QueueFamily::Text => ("body", PropertyInput::from(format!("shared {position}"))),
        }
    }

    /// "Insert a document, then rank documents through the `family` index."
    fn insert_and_search(family: QueueFamily, position: f32) -> batch::WriteBatch {
        let ranked = match family {
            QueueFamily::Vector => {
                traversal::g().vector_search_nodes("Doc", "embedding", vec![position, 0.0], 2, None)
            }
            QueueFamily::Text => traversal::g().text_search_nodes("Doc", "body", "shared", 2, None),
        };
        insert(vec![indexed(family, position)]).var_as("nearest", ranked)
    }

    #[tokio::test]
    async fn write_searches_conflict_with_concurrent_commits_to_the_searched_generation() {
        for layout in [QueueLayout::Map, QueueLayout::Rows] {
            for (family, other) in [
                (QueueFamily::Vector, QueueFamily::Text),
                (QueueFamily::Text, QueueFamily::Vector),
            ] {
                let label = format!("{layout:?}-{family:?}");
                let db = open(&format!("pending-write-skew-{label}"), layout).await;

                // Each transaction sees only its own insert. In either serial
                // order the later one would see both, so both may not commit.
                let mut first = staged(&db, insert_and_search(family, 0.0)).await;
                let mut second = staged(&db, insert_and_search(family, 0.1)).await;
                first.commit_request_write_scope().await.unwrap();
                let error = second
                    .commit_request_write_scope()
                    .await
                    .expect_err("a search that missed a concurrent enqueue must not commit");
                assert_eq!(
                    error.error_code(),
                    QueryErrorCode::TransactionConflict,
                    "{label}"
                );

                // Writes that search nothing gain no queue dependency.
                let mut first = staged(&db, insert(vec![indexed(family, 1.0)])).await;
                let mut second = staged(&db, insert(vec![indexed(family, 2.0)])).await;
                first.commit_request_write_scope().await.unwrap();
                second.commit_request_write_scope().await.unwrap();

                // The dependency covers only the searched generation's queue.
                let mut searching = staged(&db, insert_and_search(family, 3.0)).await;
                let mut unsearched = staged(&db, insert(vec![indexed(other, 3.0)])).await;
                unsearched.commit_request_write_scope().await.unwrap();
                searching.commit_request_write_scope().await.unwrap();

                // The index worker acknowledging the searched generation's work
                // also changes what the search read.
                let mut searching = staged(&db, insert_and_search(family, 4.0)).await;
                let published = db
                    .index_queue_publisher()
                    .unwrap()
                    .publish_once(crate::index_lifecycle::queue::tests::target(&db, family).await)
                    .await
                    .unwrap();
                assert!(
                    matches!(published, PublicationOutcome::Published { .. }),
                    "{label}: {published:?}"
                );
                let error = searching.commit_request_write_scope().await.expect_err(
                    "a search that missed a concurrent acknowledgement must not commit",
                );
                assert_eq!(
                    error.error_code(),
                    QueryErrorCode::TransactionConflict,
                    "{label}"
                );
                db.close().await.unwrap();
            }
        }
    }

    fn selected_read(count: u64) -> CommittedRead {
        CommittedRead::Selected(deleted(count))
    }

    fn selected_count(read: &CommittedRead) -> usize {
        match read {
            CommittedRead::Selected(committed) => committed.entities.len(),
            CommittedRead::PastStrongVectorBound { .. } => panic!("{read:?} selected nothing"),
        }
    }

    #[tokio::test]
    async fn pending_sets_read_each_queue_and_consistency_once() {
        let sets = PendingSets::default();
        let other = QueueTarget::new(
            DataScope::LegacyUnscoped,
            IndexId::new(7).unwrap(),
            IndexGenerationId::new(10).unwrap(),
        );
        // A failed read leaves the set unread, and the next search reads it.
        let error = sets
            .get_or_read(target(), SearchConsistency::Strong, async {
                Err(HelixDbError::IndexCatalogCorruption("damaged".to_string()))
            })
            .await
            .expect_err("a failed read fails its search");
        assert!(matches!(error, HelixDbError::IndexCatalogCorruption(_)));
        let read = sets
            .get_or_read(target(), SearchConsistency::Strong, async {
                Ok(selected_read(2))
            })
            .await
            .unwrap();
        assert_eq!(selected_count(&read), 2);
        // Later searches reuse it without reading.
        let reused = sets
            .get_or_read(target(), SearchConsistency::Strong, async {
                Err(HelixDbError::InvariantViolation("read twice".to_string()))
            })
            .await
            .unwrap();
        assert_eq!(selected_count(&reused), 2);
        assert_eq!(sets.reads(), 2);
        // Another consistency or generation is another set.
        for (target, consistency) in [
            (target(), SearchConsistency::Eventual),
            (other, SearchConsistency::Strong),
        ] {
            let read = sets
                .get_or_read(target, consistency, async { Ok(selected_read(1)) })
                .await
                .unwrap();
            assert_eq!(selected_count(&read), 1);
        }
        assert_eq!(sets.reads(), 4);

        // Concurrent first searches wait for one read.
        let sets = PendingSets::default();
        let (first, second) = tokio::join!(
            sets.get_or_read(target(), SearchConsistency::Strong, async {
                tokio::task::yield_now().await;
                Ok(selected_read(3))
            }),
            sets.get_or_read(target(), SearchConsistency::Strong, async {
                Ok(selected_read(1))
            }),
        );
        assert_eq!(selected_count(&first.unwrap()), 3);
        assert_eq!(selected_count(&second.unwrap()), 3);
        assert_eq!(sets.reads(), 1);

        // A refused strong vector read is reused too.
        let sets = PendingSets::default();
        for _ in 0..2 {
            let read = sets
                .get_or_read(target(), SearchConsistency::Strong, async {
                    Ok(CommittedRead::PastStrongVectorBound {
                        reached: 9,
                        limit: 8,
                    })
                })
                .await
                .unwrap();
            assert!(matches!(
                read,
                CommittedRead::PastStrongVectorBound {
                    reached: 9,
                    limit: 8
                }
            ));
        }
        assert_eq!(sets.reads(), 1);
    }

    /// Commits one document indexed by both families at `position`.
    async fn commit_document(db: &HelixDB, position: f32) {
        let mut context = staged(
            db,
            insert(vec![
                indexed(QueueFamily::Vector, position),
                indexed(QueueFamily::Text, position),
            ]),
        )
        .await;
        context.commit_request_write_scope().await.unwrap();
    }

    fn rank_both(
        batch: batch::ReadBatch,
        suffix: &str,
        position: f32,
        k: usize,
    ) -> batch::ReadBatch {
        batch
            .var_as(
                &format!("vector{suffix}"),
                traversal::g().vector_search_nodes(
                    "Doc",
                    "embedding",
                    vec![position, 0.0],
                    k,
                    None,
                ),
            )
            .var_as(
                &format!("text{suffix}"),
                traversal::g().text_search_nodes("Doc", "body", "shared", k, None),
            )
    }

    /// Every search of one request, read or write, shares one read of each
    /// queue it searches.
    #[tokio::test]
    async fn one_request_reads_each_searched_queue_once() {
        for layout in [QueueLayout::Map, QueueLayout::Rows] {
            let db = open(&format!("pending-reuse-{layout:?}"), layout).await;
            for position in [0.0, 1.0, 2.0] {
                commit_document(&db, position).await;
            }
            let batch = (0..3).fold(batch::read_batch(), |batch, index| {
                rank_both(batch, &index.to_string(), index as f32, index + 1)
            });
            for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
                let plan = helix_planner::planning::plan_read_batch(
                    &batch,
                    &db.planner_context(ParamBindings::default()),
                )
                .unwrap();
                let mut context = ExecutionContext::new(&db, ParamBindings::default());
                context.search_consistency = consistency;
                context.enable_request_read_view().await.unwrap();
                context
                    .execute_steps(
                        plan.steps(),
                        plan.execution_order(),
                        plan.root(),
                        plan.execution_program(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    context.pending_sets.reads(),
                    2,
                    "{layout:?} {consistency:?}: one read per queue"
                );
                context.close_request_read_view().unwrap();
            }

            // A write batch that changes and searches both indexes in turn.
            let batch = (0..3).fold(batch::write_batch(), |batch, index| {
                let position = 10.0 + index as f32;
                batch
                    .var_as(
                        &format!("created{index}"),
                        traversal::g().add_n(
                            "Doc",
                            vec![
                                indexed(QueueFamily::Vector, position),
                                indexed(QueueFamily::Text, position),
                            ],
                        ),
                    )
                    .var_as(
                        &format!("vector{index}"),
                        traversal::g().vector_search_nodes(
                            "Doc",
                            "embedding",
                            vec![position, 0.0],
                            1,
                            None,
                        ),
                    )
                    .var_as(
                        &format!("text{index}"),
                        traversal::g().text_search_nodes("Doc", "body", "shared", 1, None),
                    )
            });
            let mut context = staged(&db, batch).await;
            assert_eq!(context.pending_sets.reads(), 2, "{layout:?}: writes reuse");

            // Committing, opening, and aborting the transaction forget every
            // set; the next transaction reads its own view.
            let forgotten = |context: &ExecutionContext<'_>, before: &Arc<PendingSets>| {
                !Arc::ptr_eq(before, &context.pending_sets) && context.pending_sets.reads() == 0
            };
            let before = Arc::clone(&context.pending_sets);
            context.commit_request_write_scope().await.unwrap();
            assert!(forgotten(&context, &before), "{layout:?}: commit");
            let before = Arc::clone(&context.pending_sets);
            context.enable_request_write_scope().await.unwrap();
            assert!(forgotten(&context, &before), "{layout:?}: open");
            let before = Arc::clone(&context.pending_sets);
            context.abort_request_write_scope();
            assert!(forgotten(&context, &before), "{layout:?}: abort");
            db.close().await.unwrap();
        }
    }
}
