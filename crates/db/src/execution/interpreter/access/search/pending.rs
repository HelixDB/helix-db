//! Pending-data selection for searches over committed but unpublished work.
//!
//! One pinned request view supplies the physical index, the outstanding
//! operation queue, and (for text) indexed-entity statistics, so an overlay
//! never mixes snapshots. Outstanding operations deduplicate to each entity's
//! latest state in that view; historical payloads are never searched. Write
//! transactions read the queue through their serializable transaction,
//! additionally overlay their own uncommitted changes from the write context,
//! and always search strongly. No publication clears the physical results
//! their own changes supersede, so those never count toward the suppression
//! limit.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use helix_ast::query::SearchConsistency;
use roaring::RoaringTreemap;

use super::*;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QueuedOperation, QueuedPayload,
};
use crate::index_lifecycle::queue::producer::PendingEntityState;
use crate::index_lifecycle::queue::storage::StoredQueue;
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

/// Consistency a selection was made under.
#[derive(Debug)]
enum SelectionConsistency {
    /// Every pending entity. `local` holds the IDs of those the searching
    /// write transaction changed itself, which no publication clears; it is
    /// empty for read requests.
    Strong { local: RoaringTreemap },
    /// Complete committed entities within the eventual search budget. Only
    /// read requests search eventually.
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
}

impl<'db> ExecutionContext<'db> {
    /// Selects pending entities for one index search, or `None` when none
    /// is pending or no queued publication is configured.
    ///
    /// Strong search selects every pending entity. Eventual search selects
    /// complete entities in queue order until the next would exceed the
    /// per-search source-input budget; unselected entities keep their stale
    /// physical representation until published. An eventual search may
    /// shrink its selection further to stay within the suppression limit
    /// (see [`PendingSelection::yield_to_suppression_limit`]). Write
    /// transactions are always strong and add their own uncommitted changes.
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
        let (target, stored, consistency) = if let Some(active) = self.active_write_tx() {
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
            let stored = self
                .db
                .index_queue_store()
                .read(&active.txn, target)
                .await?;
            (target, stored, SearchConsistency::Strong)
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
            let stored = self.db.index_queue_store().read(view, target).await?;
            (target, stored, self.search_consistency)
        } else {
            return Ok(None);
        };
        let queue = stored.map(StoredQueue::into_queue);
        if let Some(queue) = &queue
            && queue.family() != family
        {
            return Err(HelixDbError::IndexCatalogCorruption(
                "search overlay read another family's operation queue".to_string(),
            ));
        }
        let budget = match consistency {
            SearchConsistency::Strong => u64::MAX,
            SearchConsistency::Eventual => self
                .db
                .config()
                .db()
                .index_operation_queue()
                .eventual_search_budget(),
        };
        let entities = select_latest(
            queue.as_ref().map_or(&[][..], OperationQueue::operations),
            budget,
        );
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

/// Deduplicates queue operations to each entity's latest state and selects
/// complete entities in queue order within `budget` source-input bytes.
fn select_latest(operations: &[QueuedOperation], budget: u64) -> Vec<PendingEntity> {
    let mut order = Vec::new();
    let mut latest: HashMap<IndexEntity, &QueuedOperation> = HashMap::new();
    for operation in operations {
        if latest.insert(operation.entity(), operation).is_none() {
            order.push(operation.entity());
        }
    }
    let mut selected = Vec::new();
    let mut remaining = budget;
    for entity in order {
        let operation = latest[&entity];
        let cost = operation.retained_bytes();
        if cost > remaining {
            break;
        }
        remaining -= cost;
        selected.push(PendingEntity {
            entity,
            latest: latest_value(operation.payload()),
        });
    }
    selected
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
    use crate::error::IndexBackpressureResource;
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
