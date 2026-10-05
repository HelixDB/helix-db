//! Grouped queues select and acknowledge exactly as regrouping every attempt
//! did.
//!
//! [`regrouping_selection`] is the selection publication made before queues
//! were grouped once per read: it regroups the whole queue on every call and
//! finds each held entity's last known operation by position. A model drives
//! both through random enqueues, rereads, publications with and without
//! commits, holds of every kind, releases, discards, and cursor moves, and
//! requires identical selections and identical remaining queues throughout.

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use proptest::collection::vec;
use proptest::prelude::*;

use super::publication::{select_batch, HeldEntity, SelectedEntity};
use super::storage::{RetainedQueues, StoredQueue};
use super::QueueTarget;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueuedOperation, QueuedOperationId, QueuedPayload, QueuedTextPayload,
    QueuedTextReplacement,
};
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};

fn entity(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    }
}

fn id(raw: u128) -> QueuedOperationId {
    QueuedOperationId::try_from_u128(raw).unwrap()
}

/// A text operation whose retained bytes grow with `text`.
fn operation(raw: u128, of: IndexEntity, text: usize) -> QueuedOperation {
    QueuedOperation::new(
        id(raw),
        of,
        QueuedPayload::Text(QueuedTextPayload {
            replacement: (text > 0).then(|| {
                QueuedTextReplacement::new(
                    TextPartition::Unpartitioned,
                    Arc::from("x".repeat(text)),
                )
            }),
        }),
    )
}

fn target() -> QueueTarget {
    QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    )
}

/// Each selected entity with the IDs it acknowledges and supersedes.
type Shape = Vec<(IndexEntity, Vec<QueuedOperationId>, Vec<QueuedOperationId>)>;

fn shape(selection: &[SelectedEntity<'_>]) -> Shape {
    selection
        .iter()
        .map(|selected| {
            (
                selected.entity,
                selected.operations.iter().map(|o| o.id()).collect(),
                selected.superseding.iter().map(|o| o.id()).collect(),
            )
        })
        .collect()
}

/// Entities of `operations` in first-appearance order, each with its
/// operations in order.
fn regroup(operations: &[QueuedOperation]) -> Vec<(IndexEntity, Vec<&QueuedOperation>)> {
    let mut order = Vec::new();
    let mut grouped = HashMap::<IndexEntity, Vec<&QueuedOperation>>::new();
    for operation in operations {
        grouped
            .entry(operation.entity())
            .or_insert_with(|| {
                order.push(operation.entity());
                Vec::new()
            })
            .push(operation);
    }
    order
        .into_iter()
        .map(|entity| {
            let operations = grouped.remove(&entity).unwrap();
            (entity, operations)
        })
        .collect()
}

/// Entities of `operations` in rotation order after `after`.
fn regrouped_rotation(
    operations: &[QueuedOperation],
    after: Option<IndexEntity>,
) -> Vec<IndexEntity> {
    let order = regroup(operations)
        .into_iter()
        .map(|(entity, _)| entity)
        .collect::<Vec<_>>();
    let start = after
        .and_then(|entity| order.iter().position(|candidate| *candidate == entity))
        .map_or(0, |position| position + 1);
    (0..order.len())
        .map(|offset| order[(start + offset) % order.len()])
        .collect()
}

/// The selection before grouping, verbatim but for its inlined repair width.
fn regrouping_selection<'a>(
    operations: &'a [QueuedOperation],
    after: Option<IndexEntity>,
    held: &HashMap<IndexEntity, HeldEntity>,
    max_entities: usize,
    max_operations: NonZeroUsize,
    max_acknowledged: NonZeroUsize,
    max_input_bytes: u64,
) -> Vec<SelectedEntity<'a>> {
    let mut grouped = regroup(operations).into_iter().collect::<HashMap<_, _>>();
    let rotation = regrouped_rotation(operations, after);
    let max_operations = max_operations.min(max_acknowledged).get();
    let now = Instant::now();
    let repair_width = |hold: HeldEntity, queued: &[&QueuedOperation]| {
        let (through, due) = match hold {
            HeldEntity::Repairing { width, .. } => return Some(width.get().min(queued.len())),
            HeldEntity::Waiting { through } | HeldEntity::Draining { through } => (through, false),
            HeldEntity::Failed { through, retry } => (through, retry <= now),
        };
        let known = queued
            .iter()
            .position(|operation| operation.id() == through)
            .map_or(0, |position| position + 1);
        (due || queued.len() > known).then_some(queued.len())
    };
    let mut selected = Vec::new();
    let mut input_bytes = 0_u64;
    let mut selected_operations = 0_usize;
    for entity in rotation {
        if selected.len() >= max_entities.max(1) {
            break;
        }
        let mut queued = grouped.remove(&entity).unwrap_or_default();
        match held.get(&entity).map(|hold| repair_width(*hold, &queued)) {
            Some(None) => continue,
            Some(Some(_)) if !selected.is_empty() => break,
            Some(Some(width)) => {
                queued.truncate(width);
                let superseding = queued.split_off(width.min(max_acknowledged.get()));
                return vec![SelectedEntity {
                    entity,
                    operations: queued,
                    superseding,
                }];
            }
            None => {}
        }
        let mut prefix = Vec::new();
        for operation in queued {
            let bytes = operation.retained_bytes();
            let first_of_batch = selected.is_empty() && prefix.is_empty();
            if selected_operations == max_operations
                || (!first_of_batch && input_bytes.saturating_add(bytes) > max_input_bytes)
            {
                break;
            }
            input_bytes = input_bytes.saturating_add(bytes);
            selected_operations += 1;
            prefix.push(operation);
        }
        let exhausted = prefix.is_empty();
        if !exhausted {
            selected.push(SelectedEntity {
                entity,
                operations: prefix,
                superseding: Vec::new(),
            });
        }
        if exhausted || input_bytes >= max_input_bytes || selected_operations == max_operations {
            break;
        }
    }
    selected
}

#[derive(Debug, Clone)]
enum Step {
    /// Commits an operation to storage; a retained queue does not see it.
    Enqueue {
        entity: u64,
        text: usize,
    },
    /// Reads storage again.
    Reread,
    /// Selects a batch and, when `commit`, acknowledges it as a committed
    /// publication would, holding a repair that superseded operations
    /// draining.
    Publish {
        max_entities: usize,
        max_operations: usize,
        max_acknowledged: usize,
        max_input_bytes: u64,
        commit: bool,
    },
    /// Holds an entity back; `kind` picks the hold, and `through` one of its
    /// queued operations, or an unknown one past them.
    Hold {
        entity: u64,
        kind: u8,
        through: usize,
        width: usize,
    },
    Release {
        entity: u64,
    },
    /// Discards a retired queue's oldest `count` operations.
    Discard {
        count: usize,
    },
    Cursor {
        entity: Option<u64>,
    },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        5 => (0..8_u64, 0..48_usize).prop_map(|(entity, text)| Step::Enqueue { entity, text }),
        1 => Just(Step::Reread),
        5 => (1..6_usize, 1..12_usize, 1..12_usize, 1..400_u64, any::<bool>()).prop_map(
            |(max_entities, max_operations, max_acknowledged, max_input_bytes, commit)| {
                Step::Publish {
                    max_entities,
                    max_operations,
                    max_acknowledged,
                    max_input_bytes,
                    commit,
                }
            }
        ),
        2 => (0..8_u64, 0..5_u8, 0..6_usize, 1..6_usize).prop_map(
            |(entity, kind, through, width)| Step::Hold {
                entity,
                kind,
                through,
                width,
            }
        ),
        1 => (0..8_u64).prop_map(|entity| Step::Release { entity }),
        1 => (1..12_usize).prop_map(|count| Step::Discard { count }),
        1 => proptest::option::of(0..8_u64).prop_map(|entity| Step::Cursor { entity }),
    ]
}

/// Storage, and the retained queue both selections run over.
struct Model {
    /// Every operation storage holds, in storage order.
    durable: Vec<QueuedOperation>,
    /// The retained queue, in storage order, as the oracle sees it.
    view: Vec<QueuedOperation>,
    retained: Option<StoredQueue>,
    held: HashMap<IndexEntity, HeldEntity>,
    cursor: Option<IndexEntity>,
    next_id: u128,
    started: Instant,
}

impl Model {
    fn new() -> Self {
        Self {
            durable: Vec::new(),
            view: Vec::new(),
            retained: None,
            held: HashMap::new(),
            cursor: None,
            next_id: 1,
            started: Instant::now(),
        }
    }

    /// Acknowledges `acknowledged` through the retained-queue store.
    fn acknowledge(&mut self, acknowledged: &[QueuedOperationId]) {
        let ids = acknowledged.iter().copied().collect::<HashSet<_>>();
        self.durable
            .retain(|operation| !ids.contains(&operation.id()));
        self.view.retain(|operation| !ids.contains(&operation.id()));
        let store = RetainedQueues::new(u64::MAX);
        store.retain(target(), self.retained.take().unwrap(), acknowledged);
        self.retained = store.take(target());
    }

    fn apply(&mut self, step: Step) -> Result<(), TestCaseError> {
        match step {
            Step::Enqueue { entity: of, text } => {
                self.durable.push(operation(self.next_id, entity(of), text));
                self.next_id += 1;
            }
            Step::Reread => {
                self.view.clone_from(&self.durable);
                self.retained =
                    StoredQueue::new(QueueFamily::Text, self.durable.clone(), HashMap::new(), 0);
            }
            Step::Publish {
                max_entities,
                max_operations,
                max_acknowledged,
                max_input_bytes,
                commit,
            } => {
                let Some(stored) = &self.retained else {
                    return Ok(());
                };
                let max_operations = NonZeroUsize::new(max_operations).unwrap();
                let max_acknowledged = NonZeroUsize::new(max_acknowledged).unwrap();
                let grouped = select_batch(
                    stored,
                    self.cursor,
                    &self.held,
                    max_entities,
                    max_operations,
                    max_acknowledged,
                    max_input_bytes,
                );
                let regrouped = regrouping_selection(
                    &self.view,
                    self.cursor,
                    &self.held,
                    max_entities,
                    max_operations,
                    max_acknowledged,
                    max_input_bytes,
                );
                let selected = shape(&grouped);
                prop_assert_eq!(&selected, &shape(&regrouped));
                if selected.is_empty() {
                    prop_assert!(regroup(&self.view)
                        .iter()
                        .all(|(entity, _)| self.held.contains_key(entity)));
                }
                if !commit || selected.is_empty() {
                    return Ok(());
                }
                // As `QueuePublisher::advance_past` does.
                for (entity, acknowledged, superseding) in &selected {
                    match (superseding.is_empty(), acknowledged.last()) {
                        (false, Some(last)) => {
                            self.held
                                .insert(*entity, HeldEntity::Draining { through: *last });
                        }
                        (true, _) | (false, None) => {
                            self.held.remove(entity);
                        }
                    }
                }
                self.cursor = selected.last().map(|(entity, _, _)| *entity);
                let acknowledged = selected
                    .iter()
                    .flat_map(|(_, acknowledged, _)| acknowledged.iter().copied())
                    .collect::<Vec<_>>();
                self.acknowledge(&acknowledged);
            }
            Step::Hold {
                entity: of,
                kind,
                through,
                width,
            } => {
                let of = entity(of);
                let queued = self
                    .durable
                    .iter()
                    .filter(|operation| operation.entity() == of)
                    .map(QueuedOperation::id)
                    .collect::<Vec<_>>();
                let through = queued.get(through).copied().unwrap_or(id(1 << 100));
                let tried = queued.last().copied().unwrap_or(through);
                let hold = match kind {
                    0 => HeldEntity::Waiting { through },
                    1 => HeldEntity::Failed {
                        through,
                        retry: self.started,
                    },
                    2 => HeldEntity::Failed {
                        through,
                        retry: self.started + Duration::from_secs(3_600),
                    },
                    3 => HeldEntity::Draining { through },
                    _ => HeldEntity::Repairing {
                        through,
                        tried,
                        width: NonZeroUsize::new(width).unwrap(),
                    },
                };
                self.held.insert(of, hold);
            }
            Step::Release { entity: of } => {
                self.held.remove(&entity(of));
            }
            Step::Discard { count } => {
                let Some(stored) = &self.retained else {
                    return Ok(());
                };
                let discarded = stored
                    .rotation(None)
                    .flat_map(|queued| queued.iter())
                    .take(count)
                    .map(QueuedOperation::id)
                    .collect::<Vec<_>>();
                let expected = regroup(&self.view)
                    .into_iter()
                    .flat_map(|(_, operations)| operations)
                    .take(count)
                    .map(QueuedOperation::id)
                    .collect::<Vec<_>>();
                prop_assert_eq!(&discarded, &expected);
                self.acknowledge(&discarded);
            }
            Step::Cursor { entity: of } => self.cursor = of.map(entity),
        }
        self.check()
    }

    /// The retained queue holds exactly the oracle's operations, grouped as
    /// regrouping them would.
    fn check(&self) -> Result<(), TestCaseError> {
        let Some(stored) = &self.retained else {
            prop_assert!(self.view.is_empty());
            return Ok(());
        };
        prop_assert_eq!(
            stored.operations().cloned().collect::<Vec<_>>(),
            self.view.clone()
        );
        prop_assert_eq!(stored.len().get(), self.view.len());
        prop_assert_eq!(
            stored.retained_bytes(),
            self.view
                .iter()
                .map(QueuedOperation::retained_bytes)
                .sum::<u64>()
        );
        let regrouped = regroup(&self.view);
        prop_assert_eq!(stored.entities(), regrouped.len());
        for (entity, operations) in &regrouped {
            prop_assert!(stored.contains(*entity));
            let queued = stored
                .rotation(Some(*entity))
                .last()
                .expect("the rotation ends at its start");
            prop_assert_eq!(queued.entity(), *entity);
            prop_assert_eq!(queued.len().get(), operations.len());
            prop_assert_eq!(queued.newest(), *operations.last().unwrap());
            prop_assert_eq!(queued.iter().collect::<Vec<_>>(), operations.clone());
        }
        prop_assert!(!stored.contains(entity(99)));
        for after in [None, self.cursor, Some(entity(99))] {
            prop_assert_eq!(
                stored
                    .rotation(after)
                    .map(|queued| queued.entity())
                    .collect::<Vec<_>>(),
                regrouped_rotation(&self.view, after)
            );
        }
        Ok(())
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_024))]

    /// Grouped selection and acknowledgement match regrouping on every step.
    #[test]
    fn grouped_queues_drain_as_regrouping_did(steps in vec(step(), 1..96)) {
        let mut model = Model::new();
        for step in steps {
            model.apply(step)?;
        }
    }
}

/// The grouping the type documents: chains through the read order, and
/// entities by their oldest outstanding operation.
#[test]
fn grouping_follows_each_entity_s_oldest_operation() {
    let (a, b, c) = (entity(1), entity(2), entity(3));
    let read = vec![
        operation(1, a, 1),
        operation(2, b, 1),
        operation(3, a, 1),
        operation(4, c, 1),
        operation(5, b, 1),
    ];
    let order = |stored: &StoredQueue, after| {
        stored
            .rotation(after)
            .map(|queued| {
                (
                    queued.entity().id.get(),
                    queued.iter().map(|o| o.id().get()).collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };
    let stored = StoredQueue::new(QueueFamily::Text, read, HashMap::new(), 7).unwrap();
    assert_eq!(stored.encoded_bytes(), 7);
    assert_eq!(
        order(&stored, None),
        [(1, vec![1, 3]), (2, vec![2, 5]), (3, vec![4])]
    );
    assert_eq!(
        order(&stored, Some(b)),
        [(3, vec![4]), (1, vec![1, 3]), (2, vec![2, 5])]
    );
    // Acknowledging a's oldest moves it behind b.
    let store = RetainedQueues::new(u64::MAX);
    store.retain(target(), stored, &[id(1)]);
    let stored = store.take(target()).unwrap();
    assert_eq!(stored.encoded_bytes(), 0, "a remainder reads no storage");
    assert_eq!(
        order(&stored, None),
        [(2, vec![2, 5]), (1, vec![3]), (3, vec![4])]
    );
    // An entity whose every operation is acknowledged leaves the rotation,
    // which then starts from the first entity.
    store.retain(target(), stored, &[id(4)]);
    let stored = store.take(target()).unwrap();
    assert!(!stored.contains(c));
    assert_eq!(order(&stored, Some(c)), [(2, vec![2, 5]), (1, vec![3])]);
    // Acknowledging every operation leaves nothing to retain.
    store.retain(target(), stored, &[id(2), id(5), id(3)]);
    assert!(store.take(target()).is_none());
    assert!(StoredQueue::new(QueueFamily::Text, Vec::new(), HashMap::new(), 0).is_none());
}

#[test]
#[should_panic(expected = "an acknowledgement names each entity's oldest outstanding operations")]
fn acknowledging_past_an_entity_s_oldest_operation_is_an_invariant_violation() {
    let a = entity(1);
    let stored = StoredQueue::new(
        QueueFamily::Text,
        vec![operation(1, a, 1), operation(2, a, 1)],
        HashMap::new(),
        0,
    )
    .unwrap();
    RetainedQueues::new(u64::MAX).retain(target(), stored, &[id(2)]);
}

#[test]
#[should_panic(expected = "an acknowledgement names each entity's oldest outstanding operations")]
fn acknowledging_one_operation_twice_is_an_invariant_violation() {
    let (a, b) = (entity(1), entity(2));
    let stored = StoredQueue::new(
        QueueFamily::Text,
        vec![operation(1, a, 1), operation(2, b, 1)],
        HashMap::new(),
        0,
    )
    .unwrap();
    RetainedQueues::new(u64::MAX).retain(target(), stored, &[id(1), id(1)]);
}

/// Prints the bookkeeping cost of draining a backlog in batches of 512
/// entities: selecting each batch and removing what it acknowledged, by
/// regrouping every attempt and by grouping once. Sizes come from
/// `HELIX_DRAIN_SIZES`; regrouping is measured only up to 250,000.
#[test]
#[ignore = "release-mode measurement; run explicitly"]
fn drain_bookkeeping_cost_against_backlog_size() {
    let sizes = std::env::var("HELIX_DRAIN_SIZES").map_or_else(
        |_| vec![10_000, 100_000, 250_000, 1_000_000],
        |sizes| {
            sizes
                .split(',')
                .map(|size| size.trim().parse().expect("a backlog size"))
                .collect::<Vec<usize>>()
        },
    );
    let all = NonZeroUsize::MAX;
    let held = HashMap::new();
    for backlog in sizes {
        // Half the entities have a second operation queued behind the rest.
        let operations = (0..backlog)
            .map(|index| {
                let of = u64::try_from(index % (backlog / 2 + backlog % 2)).unwrap();
                operation(u128::try_from(index).unwrap() + 1, entity(of), 16)
            })
            .collect::<Vec<_>>();
        if backlog <= 250_000 {
            let started = Instant::now();
            let mut view = operations.clone();
            let (mut cursor, mut batches) = (None, 0);
            while !view.is_empty() {
                let acknowledged =
                    regrouping_selection(&view, cursor, &held, 512, all, all, u64::MAX)
                        .iter()
                        .inspect(|selected| cursor = Some(selected.entity))
                        .flat_map(|selected| selected.operations.iter().map(|o| o.id()))
                        .collect::<HashSet<_>>();
                view.retain(|operation| !acknowledged.contains(&operation.id()));
                batches += 1;
            }
            println!(
                "BOOKKEEPING algorithm=regrouping backlog={backlog} batches={batches} seconds={:.3}",
                started.elapsed().as_secs_f64()
            );
        }
        let started = Instant::now();
        let store = RetainedQueues::new(u64::MAX);
        let mut stored = StoredQueue::new(QueueFamily::Text, operations, HashMap::new(), 0);
        let (mut cursor, mut batches) = (None, 0);
        while let Some(queue) = stored {
            let acknowledged = select_batch(&queue, cursor, &held, 512, all, all, u64::MAX)
                .iter()
                .inspect(|selected| cursor = Some(selected.entity))
                .flat_map(|selected| selected.operations.iter().map(|o| o.id()))
                .collect::<Vec<_>>();
            store.retain(target(), queue, &acknowledged);
            stored = store.take(target());
            batches += 1;
        }
        println!(
            "BOOKKEEPING algorithm=grouped backlog={backlog} batches={batches} seconds={:.3}",
            started.elapsed().as_secs_f64()
        );
    }
}
