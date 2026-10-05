//! Pending-data selection for searches over committed but unpublished work.
//!
//! One pinned request view supplies the physical index, the outstanding
//! operation queue, and (for text) indexed-entity statistics, so an overlay
//! never mixes snapshots. A search only ever sees a pending entity at its
//! latest state in that view, never at an earlier state of its chain: a build
//! or publication may already have written the latest state physically, and
//! an earlier one would hide it. Strong searches select every pending entity;
//! eventual searches select the oldest pending entities whose latest
//! operations fit their budget and leave the rest to their physical
//! representation. Only decoding and searching follow that budget; reading
//! the queue still follows the backlog (see
//! [`crate::index_lifecycle::queue::storage::QueueStore::read_latest`]).
//! Write transactions read the queue through their serializable transaction,
//! additionally overlay their own uncommitted changes from the write context,
//! and always search strongly. No publication clears the physical results
//! their own changes supersede, so those never count toward the suppression
//! limit.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use helix_ast::query::SearchConsistency;
use roaring::RoaringTreemap;

use super::*;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    LatestOperations, QueueFamily, QueuedPayload,
};
use crate::index_lifecycle::queue::producer::PendingEntityState;
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::IndexIdentity;

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

/// Consistency a selection was made under.
#[derive(Debug)]
enum SelectionConsistency {
    /// Every pending entity. `local` holds the IDs of those the searching
    /// write transaction changed itself, which no publication clears; it is
    /// empty for read requests.
    Strong { local: RoaringTreemap },
    /// The oldest committed pending entities whose latest operations fit
    /// the eventual search budget, each at that latest state. Only read
    /// requests search eventually.
    Eventual,
}

/// Pending entities selected for one search.
///
/// `superseded` holds exactly the IDs of `entities`; a generation indexes one
/// element kind, so the ID alone identifies an entity.
#[derive(Debug)]
pub(super) struct PendingSelection {
    /// Generation queue the entities were selected from.
    target: QueueTarget,
    /// Consistency the entities were selected under.
    consistency: SelectionConsistency,
    /// Entities whose physical representation is superseded.
    pub(super) superseded: RoaringTreemap,
    /// Selected entities in deterministic selection order.
    pub(super) entities: Vec<PendingEntity>,
}

impl PendingSelection {
    fn new(
        target: QueueTarget,
        consistency: SearchConsistency,
        entities: Vec<PendingEntity>,
    ) -> Self {
        Self {
            target,
            consistency: match consistency {
                SearchConsistency::Strong => SelectionConsistency::Strong {
                    local: RoaringTreemap::new(),
                },
                SearchConsistency::Eventual => SelectionConsistency::Eventual,
            },
            superseded: entities
                .iter()
                .map(|pending| pending.entity.id.get())
                .collect(),
            entities,
        }
    }

    /// Strongly selects committed `entities` with a write transaction's own
    /// `local` changes overlaid on their latest states.
    ///
    /// Costs one pass over the selection plus one lookup per local change,
    /// not a scan of the committed backlog per change.
    fn overlaid<'a>(
        target: QueueTarget,
        entities: Vec<PendingEntity>,
        local: impl Iterator<Item = (IndexEntity, &'a PendingEntityState)>,
    ) -> Self {
        let mut selection = Self::new(target, SearchConsistency::Strong, entities);
        let mut local = local.peekable();
        if local.peek().is_none() {
            return selection;
        }
        let mut positions = selection
            .entities
            .iter()
            .enumerate()
            .map(|(position, pending)| (pending.entity, position))
            .collect::<HashMap<_, _>>();
        let mut local_ids = RoaringTreemap::new();
        for (entity, state) in local {
            let latest = match state {
                PendingEntityState::Vector { current, .. } => current.as_ref().map(|document| {
                    (
                        document.partition().clone(),
                        PendingValue::Vector(Arc::from(document.vector())),
                    )
                }),
                PendingEntityState::Text { current, .. } => current.as_ref().map(|document| {
                    (
                        document.partition.clone(),
                        PendingValue::Text(Arc::clone(&document.text)),
                    )
                }),
            };
            local_ids.insert(entity.id.get());
            match positions.entry(entity) {
                Entry::Occupied(position) => selection.entities[*position.get()].latest = latest,
                Entry::Vacant(position) => {
                    position.insert(selection.entities.len());
                    selection.superseded.insert(entity.id.get());
                    selection.entities.push(PendingEntity { entity, latest });
                }
            }
        }
        selection.consistency = SelectionConsistency::Strong { local: local_ids };
        selection
    }

    /// IDs of the superseded entities the searching write transaction changed
    /// itself, or `None` for an eventual selection, which has none.
    pub(super) fn local(&self) -> Option<&RoaringTreemap> {
        match &self.consistency {
            SelectionConsistency::Strong { local } => Some(local),
            SelectionConsistency::Eventual => None,
        }
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
            SelectionConsistency::Strong { .. } => Err(HelixDbError::IndexBackpressure {
                scope: self.target.scope,
                index_id: self.target.index_id.get(),
                resource: crate::error::IndexBackpressureResource::SuppressedSearchResults,
                requested: u64::try_from(skipped).unwrap_or(u64::MAX),
                limit: limit as u64,
            }),
            SelectionConsistency::Eventual if self.entities.len() > limit => {
                self.entities.truncate(limit);
                self.superseded = self
                    .entities
                    .iter()
                    .map(|pending| pending.entity.id.get())
                    .collect();
                Ok(())
            }
            SelectionConsistency::Eventual => Err(HelixDbError::InvariantViolation(format!(
                "a search superseding {} entities skipped {skipped} results, past a suppression \
                 limit it cannot reach",
                self.entities.len()
            ))),
        }
    }

    /// Bounds the unpublished text one text search analyzes in `partition`
    /// to `limit` analysis bytes, the analysis budget of one text
    /// publication.
    ///
    /// A text overlay analyzes the latest document of every selected entity
    /// in the searched partition, for corpus statistics and again for its
    /// in-memory index, whether or not a traversal restricts the search. That
    /// work grows with the committed backlog rather than with `k`, so it is
    /// charged exactly as one publication charges the documents it analyzes
    /// ([`crate::search::text::TextAnalysisMemoryBudget`]: text bytes plus a
    /// fixed overhead per retained token, so dense short-token text costs
    /// far more than its length) and bounded by that publication budget.
    /// Charging stops at the first token past the bound, so sizing never
    /// analyzes more than the bound either.
    ///
    /// The bound caps one search's analysis memory at one publication's; it
    /// is not what one publication drains. A publication also selects at most
    /// its batch input bytes and entities, across every partition of its
    /// generation, so the index worker may need several publications to bring
    /// a partition's backlog back within the bound.
    ///
    /// The searching write transaction's own documents are charged first: no
    /// publication clears them, so a write whose own documents exceed the
    /// bound fails with [`HelixDbError::IndexOperationBatchTooLarge`]. Past
    /// the bound with committed documents, strong search fails with retryable
    /// index backpressure rather than analyze more; within it, it stays
    /// exact. Eventual search keeps the longest prefix of its selection
    /// within the bound and leaves the rest to their published
    /// representation until publication. Either error reports the charge
    /// reached when analysis stopped.
    ///
    /// Text of entities the index worker holds back
    /// ([`crate::BlockedIndexEntity`]) counts too, although no publication
    /// drains it: only a writer knows which entities those are, and exempting
    /// them would let analysis grow with the retained backlog again. While
    /// held-back text alone exceeds the bound, which takes limits lowered
    /// below documents already admitted, strong text searches of the
    /// partition keep failing until a later write to each of those entities
    /// publishes or the limits are raised.
    pub(super) fn yield_to_text_analysis_limit(
        &mut self,
        partition: &TextPartition,
        analyzer: crate::config::TextAnalyzerKind,
        limit: NonZeroU64,
    ) -> Result<()> {
        let mut budget = crate::search::text::TextAnalysisMemoryBudget::new(limit);
        // The charge the bound refused, or `None` once the document fits.
        let mut refused = |text: &str| match crate::search::text::analyze_text_within_budget(
            analyzer,
            text,
            &mut budget,
        ) {
            Ok(_) => Ok(None),
            Err(HelixDbError::ActiveTextMutationLimitExceeded { observed, .. }) => {
                Ok(Some(observed))
            }
            Err(error) => Err(error),
        };
        let local = self.local();
        let is_local = |pending: &PendingEntity| {
            local.is_some_and(|local| local.contains(pending.entity.id.get()))
        };
        self.entities
            .iter()
            .filter(|pending| is_local(pending))
            .filter_map(|pending| pending.text_in(partition))
            .map(&mut refused)
            .find_map(Result::transpose)
            .transpose()?
            .map_or(Ok(()), |observed| {
                Err(HelixDbError::IndexOperationBatchTooLarge {
                    index_id: self.target.index_id.get(),
                    resource: crate::error::IndexOperationBatchResource::PendingTextAnalysisBytes,
                    observed,
                    limit: limit.get(),
                })
            })?;
        let committed = self
            .entities
            .iter()
            .enumerate()
            .filter(|(_, pending)| !is_local(pending))
            .filter_map(|(position, pending)| {
                pending.text_in(partition).map(|text| (position, text))
            })
            .map(|(position, text)| {
                refused(text).map(|refused| refused.map(|observed| (position, observed)))
            })
            .find_map(Result::transpose)
            .transpose()?;
        let Some((end, requested)) = committed else {
            return Ok(());
        };
        match self.consistency {
            SelectionConsistency::Strong { .. } => Err(HelixDbError::IndexBackpressure {
                scope: self.target.scope,
                index_id: self.target.index_id.get(),
                resource: crate::error::IndexBackpressureResource::PendingTextAnalysisBytes,
                requested,
                limit: limit.get(),
            }),
            SelectionConsistency::Eventual => {
                self.entities.truncate(end);
                self.superseded = self
                    .entities
                    .iter()
                    .map(|pending| pending.entity.id.get())
                    .collect();
                Ok(())
            }
        }
    }
}

impl<'db> ExecutionContext<'db> {
    /// Selects pending entities for one index search, or `None` when none
    /// is pending or no queued publication is configured.
    ///
    /// Every selected entity is searched at its latest state in the view.
    /// Strong search selects every pending entity. Eventual search selects
    /// entities in the order of their oldest pending operation until the
    /// next one's latest operation would exceed the per-search source-input
    /// budget; unselected entities keep their physical representation, stale
    /// or not, until published, and an entity is never selected at an
    /// earlier state, which could be older than that representation. The
    /// budget bounds decoding and searching, not the queue read: the map
    /// layout fetches its whole value, and while merge operands are pending
    /// above its base SlateDB resolves them against all of it, validating
    /// every record, so that read costs the backlog and fails on a corrupt
    /// record the budget never selects (see
    /// [`crate::index_lifecycle::queue::storage::QueueStore::read_latest`]).
    /// An eventual search may shrink its selection further to stay within
    /// the suppression limit (see
    /// [`PendingSelection::yield_to_suppression_limit`]). Write transactions
    /// are always strong and add their own uncommitted changes.
    ///
    /// A write transaction reads the queue through its serializable
    /// transaction, so the searched generation's queue becomes a read
    /// dependency: a concurrent commit that enqueues to or acknowledges that
    /// generation aborts this transaction with a retryable conflict instead
    /// of letting it commit a decision made without that work. Writes that do
    /// not search read no queue and keep committing concurrently.
    pub(super) async fn pending_selection(
        &self,
        identity: &IndexIdentity,
        family: QueueFamily,
    ) -> Result<Option<PendingSelection>> {
        let (target, latest, consistency) = if let Some(active) = self.active_write_tx() {
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
            let latest = self
                .db
                .index_queue_store()
                .read_latest(&active.txn, target, u64::MAX)
                .await?;
            (target, latest, SearchConsistency::Strong)
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
            let budget = match self.search_consistency {
                SearchConsistency::Strong => u64::MAX,
                SearchConsistency::Eventual => self
                    .db
                    .config()
                    .db()
                    .index_operation_queue()
                    .eventual_search_budget(),
            };
            let latest = self
                .db
                .index_queue_store()
                .read_latest(view, target, budget)
                .await?;
            (target, latest, self.search_consistency)
        } else {
            return Ok(None);
        };
        if let Some(latest) = &latest
            && latest.family() != family
        {
            return Err(HelixDbError::IndexCatalogCorruption(
                "search overlay read another family's operation queue".to_string(),
            ));
        }
        let entities = latest
            .map(LatestOperations::into_operations)
            .unwrap_or_default()
            .into_iter()
            .map(|operation| PendingEntity {
                entity: operation.entity(),
                latest: latest_value(operation.payload()),
            })
            .collect();
        let selection = match self.active_write_tx() {
            Some(active) => PendingSelection::overlaid(
                target,
                entities,
                active
                    .index_context
                    .queued_mutations()
                    .pending_entities(target),
            ),
            None => PendingSelection::new(target, consistency, entities),
        };
        Ok((!selection.entities.is_empty()).then_some(selection))
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

    /// A selection of `count` deleted nodes, oldest first by ID.
    fn deletions(consistency: SearchConsistency, count: u64) -> PendingSelection {
        PendingSelection::new(
            QueueTarget::new(
                DataScope::LegacyUnscoped,
                IndexId::new(7).unwrap(),
                IndexGenerationId::new(9).unwrap(),
            ),
            consistency,
            (0..count)
                .map(|id| PendingEntity {
                    entity: IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(id),
                    },
                    latest: None,
                })
                .collect(),
        )
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
        assert_eq!(strong.entities.len(), limit + 1, "the selection is kept");
    }

    #[test]
    fn eventual_selections_keep_their_oldest_entities_within_the_suppression_limit() {
        let limit = super::super::limits::MAX_SUPPRESSED_SEARCH_RESULTS;
        let mut eventual = deletions(SearchConsistency::Eventual, limit as u64 + 5);
        eventual.yield_to_suppression_limit(limit + 1).unwrap();
        let oldest = (0..limit as u64).collect::<Vec<_>>();
        assert_eq!(
            eventual
                .entities
                .iter()
                .map(|pending| pending.entity.id.get())
                .collect::<Vec<_>>(),
            oldest
        );
        assert_eq!(
            eventual.superseded,
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
        let entity = |id| IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(id),
        };
        let deleted = PendingEntityState::Vector {
            first: None,
            current: None,
        };
        let committed = deletions(SearchConsistency::Strong, 3);
        // Local changes to a committed pending entity and to one without
        // committed work.
        let selection = PendingSelection::overlaid(
            committed.target,
            committed.entities,
            [(entity(2), &deleted), (entity(5), &deleted)].into_iter(),
        );
        assert_eq!(
            selection
                .entities
                .iter()
                .map(|pending| pending.entity.id.get())
                .collect::<Vec<_>>(),
            [0, 1, 2, 5]
        );
        assert_eq!(
            selection.superseded,
            [0, 1, 2, 5].into_iter().collect::<RoaringTreemap>()
        );
        assert_eq!(
            selection.local(),
            Some(&[2, 5].into_iter().collect::<RoaringTreemap>()),
            "both local changes are exempt from the suppression limit"
        );

        let committed = deletions(SearchConsistency::Strong, 3);
        let selection = PendingSelection::overlaid(
            committed.target,
            committed.entities,
            std::iter::empty::<(IndexEntity, &PendingEntityState)>(),
        );
        assert_eq!(selection.local(), Some(&RoaringTreemap::new()));
        assert_eq!(deletions(SearchConsistency::Eventual, 1).local(), None);
    }

    /// A selection whose entity `id` has latest state `documents[id]`: text in
    /// a partition, or a deletion.
    fn texts(
        consistency: SearchConsistency,
        documents: &[Option<(TextPartition, &str)>],
    ) -> PendingSelection {
        let mut selection = deletions(consistency, documents.len() as u64);
        for (pending, document) in selection.entities.iter_mut().zip(documents) {
            pending.latest = document
                .clone()
                .map(|(partition, text)| (partition, PendingValue::Text(Arc::from(text))));
        }
        selection
    }

    fn selected(selection: &PendingSelection) -> Vec<u64> {
        selection
            .entities
            .iter()
            .map(|pending| pending.entity.id.get())
            .collect()
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
        committed: PendingSelection,
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
        PendingSelection::overlaid(
            committed.target,
            committed.entities,
            std::iter::once((
                IndexEntity {
                    kind: IndexElementKind::Node,
                    id: IndexEntityId::new(id),
                },
                &local,
            )),
        )
    }

    #[test]
    fn text_analysis_charges_only_text_in_the_searched_partition() {
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
            selection
                .yield_to_text_analysis_limit(&searched, ANALYZER, limit)
                .unwrap();
            assert_eq!(selected(&selection), [0, 1, 2, 3], "{consistency:?}");
            assert_eq!(
                selection.superseded,
                [0, 1, 2, 3].into_iter().collect::<RoaringTreemap>()
            );
        }
    }

    /// The charge is per retained token, not per byte: short dense tokens
    /// cost far more than their length, so a bound in text bytes would let
    /// them through.
    #[test]
    fn dense_short_tokens_are_charged_per_token() {
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
        strong
            .yield_to_text_analysis_limit(&searched, ANALYZER, limit)
            .unwrap();
        let mut strong = texts(
            SearchConsistency::Strong,
            &[Some((searched.clone(), dense))],
        );
        let error = strong
            .yield_to_text_analysis_limit(&searched, ANALYZER, limit)
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

    #[test]
    fn strong_text_analysis_past_the_bound_fails_with_backpressure() {
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
        let error = strong
            .yield_to_text_analysis_limit(&searched, ANALYZER, bound(limit))
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

    #[test]
    fn eventual_text_analysis_keeps_the_longest_prefix_within_the_bound() {
        let searched = TextPartition::Unpartitioned;
        let documents = [
            Some((searched.clone(), "abcd")),
            None,
            Some((searched.clone(), "efgh")),
            None,
            Some((searched.clone(), "ij")),
        ];
        let mut eventual = texts(SearchConsistency::Eventual, &documents);
        eventual
            .yield_to_text_analysis_limit(&searched, ANALYZER, bound(charge(&["abcd", "efgh"]) - 1))
            .unwrap();
        // The deletion after the first document costs nothing, but the
        // prefix ends at the first document past the bound.
        assert_eq!(selected(&eventual), [0, 1]);
        assert_eq!(
            eventual.superseded,
            [0, 1].into_iter().collect::<RoaringTreemap>()
        );
        // A first document past the bound leaves nothing to overlay.
        let mut eventual = texts(SearchConsistency::Eventual, &documents);
        eventual
            .yield_to_text_analysis_limit(&searched, ANALYZER, bound(charge(&["abcd"]) - 1))
            .unwrap();
        assert!(eventual.entities.is_empty());
        assert!(eventual.superseded.is_empty());
    }

    /// A write transaction's own documents are charged first. No publication
    /// clears them, so past the bound alone they fail the write for good;
    /// committed documents past the bound beside them fail it retryably.
    #[test]
    fn a_write_transactions_own_text_is_charged_first_and_fails_it_for_good() {
        let searched = TextPartition::Unpartitioned;
        let other = TextPartition::try_tenant_value(bytes::Bytes::from_static(b"t")).unwrap();
        let committed = || {
            texts(
                SearchConsistency::Strong,
                &[
                    Some((searched.clone(), "abcd")),
                    Some((searched.clone(), "efgh")),
                ],
            )
        };
        let own = "one two three";
        let limit = charge(&["abcd", "efgh"]);
        assert!(charge(&[own]) > limit);
        // Its own document past the bound alone, even overlaying a committed
        // entity, can never be searched.
        for id in [1, 2] {
            let error = with_local(committed(), id, &searched, own)
                .yield_to_text_analysis_limit(&searched, ANALYZER, bound(limit))
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
        let error = with_local(committed(), 2, &searched, "ij")
            .yield_to_text_analysis_limit(&searched, ANALYZER, bound(limit))
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
        selection
            .yield_to_text_analysis_limit(&searched, ANALYZER, bound(limit))
            .unwrap();
        assert_eq!(selected(&selection), [0, 1, 2]);
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
}
