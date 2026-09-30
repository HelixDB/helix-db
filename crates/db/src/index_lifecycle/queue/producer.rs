//! Transaction-local construction of immutable queued index operations.
//!
//! A graph transaction records every routed vector/text transition here
//! instead of mutating physical index rows. Repeated changes to one entity
//! inside the uncommitted transaction collapse to one final operation whose
//! `previous` routing comes from the entity's state before the transaction's
//! first change; the collector keeps the latest state for transaction-local
//! searches. Nothing collapses across committed transactions: every commit
//! contributes fresh immutable operations.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueOperand, QueuedOperation, QueuedOperationId, QueuedPayload, QueuedTextPayload,
    QueuedTextReplacement, QueuedVectorPayload, QueuedVectorReplacement,
};
use crate::error::{HelixDbError, IndexOperationBatchResource, Result};
use crate::index_lifecycle::graph_mutation::{CanonicalPropertyRow, GraphMutationTransition};
use crate::index_lifecycle::mutation_catalog::{MutationRouteTarget, RoutedMutationTargets};
use crate::index_lifecycle::vector::VectorIndexedDocument;
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::IndexRecordV2;

use super::backlog::OperationCharge;
use super::QueueTarget;

/// Validated text source state carried by a queued text operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuedTextDocument {
    pub(crate) partition: TextPartition,
    pub(crate) text: Arc<str>,
}

/// Transaction-local state of one entity in one generation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PendingEntityState {
    /// Vector state before the transaction's first change and now.
    Vector {
        first: Option<VectorIndexedDocument>,
        current: Option<VectorIndexedDocument>,
    },
    /// Text state before the transaction's first change and now.
    Text {
        first: Option<QueuedTextDocument>,
        current: Option<QueuedTextDocument>,
    },
}

#[derive(Debug, Default)]
struct PendingGeneration {
    order: Vec<IndexEntity>,
    entities: HashMap<IndexEntity, PendingEntityState>,
}

/// Every queued vector/text transition staged by one graph transaction.
#[derive(Debug)]
pub(crate) struct QueuedMutationCollector {
    scope: DataScope,
    generations: BTreeMap<QueueTarget, PendingGeneration>,
    /// Canonical record of every routed text generation, for admission.
    text_records: BTreeMap<QueueTarget, IndexRecordV2>,
}

/// One generation's fresh operations and their map-layout operand.
#[derive(Debug)]
pub(crate) struct StagedQueueOperand {
    pub(crate) target: QueueTarget,
    pub(crate) operand: QueueOperand,
    pub(crate) operations: Vec<QueuedOperation>,
}

/// Operands and admission charges ready to stage in the graph transaction.
#[derive(Debug, Default)]
pub(crate) struct StagedQueueWrites {
    pub(crate) operands: Vec<StagedQueueOperand>,
    pub(crate) charges: Vec<OperationCharge>,
}

impl StagedQueueWrites {
    /// Returns whether the transaction queued no index work.
    pub(crate) fn is_empty(&self) -> bool {
        self.operands.is_empty()
    }
}

impl QueuedMutationCollector {
    /// Creates an empty collector for one scoped graph transaction.
    pub(crate) fn new(scope: DataScope) -> Self {
        Self {
            scope,
            generations: BTreeMap::new(),
            text_records: BTreeMap::new(),
        }
    }

    /// Records one routed graph transition for every vector/text target.
    ///
    /// Indexed payloads are validated here with the same rules initial builds
    /// apply to source rows, so the worker never receives an operation that
    /// its index definition would reject. A previous text row, or a hidden
    /// build's vector row, that fails those rules was never indexed, so a
    /// write repairing or deleting it proceeds. An Active vector row whose
    /// magnitude is out of range is left untouched until a rebuild: writes
    /// to it queue nothing for that index.
    pub(crate) fn collect(
        &mut self,
        vector: &crate::index_lifecycle::vector::VectorMutationSet,
        text: &crate::index_lifecycle::text::mutation::TextMutationSet,
        routes: &RoutedMutationTargets<'_>,
        transition: &GraphMutationTransition,
    ) -> Result<()> {
        if transition.scope() != self.scope {
            return Err(HelixDbError::InvariantViolation(
                "queued index transition crossed its transaction scope".to_string(),
            ));
        }
        let entity = transition.entity().index_entity();
        let before = transition
            .before()
            .map_or(&[][..], CanonicalPropertyRow::properties);
        let after = transition
            .after()
            .map_or(&[][..], CanonicalPropertyRow::properties);
        for route in routes.iter() {
            match route {
                MutationRouteTarget::Vector(ordinal) => {
                    let target = vector.queued_target(ordinal)?;
                    if target.definition.element_kind() != entity.kind {
                        continue;
                    }
                    let after =
                        crate::index_lifecycle::vector::vector_document(target.definition, after)?;
                    let before = match crate::index_lifecycle::vector::vector_document(
                        target.definition,
                        before,
                    ) {
                        Ok(document) => document,
                        // A hidden build blocks on every invalid row without
                        // indexing it, so its repair has no previous routing.
                        Err(_) if target.active.is_none() => None,
                        // An already-invalid Active row stays untouched until a
                        // rebuild.
                        Err(HelixDbError::VectorComponentMagnitudeExceeded { .. }) => continue,
                        Err(error) => return Err(error),
                    };
                    self.record(
                        QueueTarget::new(self.scope, target.index_id, target.generation),
                        entity,
                        PendingEntityState::Vector {
                            first: before,
                            current: after,
                        },
                    )?;
                }
                MutationRouteTarget::TextBuilding(_) | MutationRouteTarget::TextActive(_) => {
                    let target = text.queued_target(route)?;
                    let definition = target.definition;
                    if definition.element_kind() != entity.kind {
                        continue;
                    }
                    // A before row that is not valid indexed text was never
                    // indexed: builds block on it and Active writes reject it.
                    // Queued text carries only the final document, so the row
                    // has no previous document and its repair proceeds.
                    let before = match crate::index_lifecycle::text::mutation::queued_document(
                        definition, before,
                    ) {
                        Ok(document) => document,
                        Err(HelixDbError::InvalidIndexSourceData { .. }) => None,
                        Err(error) => return Err(error),
                    };
                    let after =
                        crate::index_lifecycle::text::mutation::queued_document(definition, after)?;
                    let queue_target =
                        QueueTarget::new(self.scope, target.index_id, target.generation);
                    self.text_records
                        .entry(queue_target)
                        .or_insert_with(|| target.record.clone());
                    self.record(
                        queue_target,
                        entity,
                        PendingEntityState::Text {
                            first: before,
                            current: after,
                        },
                    )?;
                }
                MutationRouteTarget::Secondary(_) => {}
            }
        }
        Ok(())
    }

    /// Folds one `(first, current)` transition into the entity's state.
    fn record(
        &mut self,
        target: QueueTarget,
        entity: IndexEntity,
        transition: PendingEntityState,
    ) -> Result<()> {
        let generation = self.generations.entry(target).or_default();
        let Some(existing) = generation.entities.get_mut(&entity) else {
            let unchanged = match &transition {
                PendingEntityState::Vector { first, current } => first == current,
                PendingEntityState::Text { first, current } => first == current,
            };
            if !unchanged {
                generation.order.push(entity);
                generation.entities.insert(entity, transition);
            }
            return Ok(());
        };
        match (existing, transition) {
            (
                PendingEntityState::Vector { current, .. },
                PendingEntityState::Vector {
                    first: before,
                    current: after,
                },
            ) => {
                if *current != before {
                    return Err(discontinuous());
                }
                *current = after;
            }
            (
                PendingEntityState::Text { current, .. },
                PendingEntityState::Text {
                    first: before,
                    current: after,
                },
            ) => {
                if *current != before {
                    return Err(discontinuous());
                }
                *current = after;
            }
            (PendingEntityState::Vector { .. }, PendingEntityState::Text { .. })
            | (PendingEntityState::Text { .. }, PendingEntityState::Vector { .. }) => {
                return Err(HelixDbError::InvariantViolation(
                    "one queue generation received two index families".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Returns every entity this transaction changed in one generation.
    pub(crate) fn pending_entities(
        &self,
        target: QueueTarget,
    ) -> impl Iterator<Item = (IndexEntity, &PendingEntityState)> {
        self.generations
            .get(&target)
            .into_iter()
            .flat_map(|generation| {
                generation.order.iter().filter_map(|entity| {
                    generation
                        .entities
                        .get(entity)
                        .map(|state| (*entity, state))
                })
            })
    }

    /// Builds fresh immutable operations, their operands, and admission charges.
    ///
    /// An entity whose final state equals its state before the transaction
    /// produces no operation. Each final text document must fit one
    /// publication beside any other admitted document of its entity under
    /// `text_limits` (see
    /// [`crate::index_lifecycle::text::active_batch::TextDocumentFootprint`]),
    /// so the publisher never meets an entity it cannot publish. Each operand
    /// must fit `max_operand_bytes`, the durable WAL entry bound for one queue
    /// key in one transaction.
    pub(crate) fn finalize(
        &self,
        max_operand_bytes: u64,
        text_limits: ActiveTextMutationLimits,
    ) -> Result<StagedQueueWrites> {
        let mut staged = StagedQueueWrites::default();
        for (target, generation) in &self.generations {
            let mut operations = Vec::with_capacity(generation.order.len());
            for entity in &generation.order {
                let Some(state) = generation.entities.get(entity) else {
                    continue;
                };
                let payload = match state {
                    PendingEntityState::Vector { first, current } if first != current => {
                        QueuedPayload::Vector(QueuedVectorPayload {
                            previous: first.as_ref().map(|document| document.partition().clone()),
                            replacement: current.as_ref().map(|document| {
                                QueuedVectorReplacement::try_new(
                                    document.partition().clone(),
                                    Arc::from(document.vector()),
                                )
                                .expect("validated vector documents are non-empty and finite")
                            }),
                        })
                    }
                    PendingEntityState::Text { first, current } if first != current => {
                        current
                            .as_ref()
                            .map(|document| {
                                let record = self.text_records.get(target).ok_or_else(|| {
                                    HelixDbError::InvariantViolation(
                                        "queued text generation has no canonical record"
                                            .to_string(),
                                    )
                                })?;
                                crate::index_lifecycle::text::active_batch::admit_queued_document(
                                    target.scope,
                                    record,
                                    *entity,
                                    &document.partition,
                                    &document.text,
                                    text_limits,
                                )
                            })
                            .transpose()?;
                        QueuedPayload::Text(QueuedTextPayload {
                            replacement: current.as_ref().map(|document| {
                                QueuedTextReplacement::new(
                                    document.partition.clone(),
                                    Arc::clone(&document.text),
                                )
                            }),
                        })
                    }
                    PendingEntityState::Vector { .. } | PendingEntityState::Text { .. } => {
                        continue;
                    }
                };
                operations.push(QueuedOperation::new(
                    QueuedOperationId::generate(),
                    *entity,
                    payload,
                ));
            }
            if operations.is_empty() {
                continue;
            }
            let operand = QueueOperand::enqueue(&operations)
                .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
            let observed = operand.bytes().len() as u64;
            if observed > max_operand_bytes {
                return Err(HelixDbError::IndexOperationBatchTooLarge {
                    index_id: target.index_id.get(),
                    resource: IndexOperationBatchResource::OperandBytes,
                    observed,
                    limit: max_operand_bytes,
                });
            }
            staged
                .charges
                .extend(operations.iter().map(|operation| OperationCharge {
                    target: *target,
                    entity: operation.entity(),
                    id: operation.id(),
                    bytes: operation.retained_bytes(),
                }));
            staged.operands.push(StagedQueueOperand {
                target: *target,
                operand,
                operations,
            });
        }
        Ok(staged)
    }
}

fn discontinuous() -> HelixDbError {
    HelixDbError::InvariantViolation(
        "queued index transition does not continue the transaction's previous state".to_string(),
    )
}
