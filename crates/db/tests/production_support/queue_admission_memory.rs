//! Measured process memory of retained index-queue operations.
//!
//! The admission ledger bounds each logical index's backlog in charged bytes.
//! These samples measure, through the calling test binary's allocation
//! probe, the heap a backlog of one operation shape actually holds per
//! operation:
//!
//! - `ledger`: the admission ledger after every operation is committed, each
//!   on its own entity (one member per operation, the worst case);
//! - `decoded`: a fully decoded queue grouped by entity, as publication
//!   reads and retains it between attempts;
//! - `resolution`: the peak working memory of resolving the queue's merge
//!   operands, once against a resolved base holding the backlog plus one new
//!   operand (a read after compaction), and once with every operation in its
//!   own operand (a read before any compaction);
//!
//! each the worst per-operation value over backlog lengths at the growth
//! boundaries of the vectors and hash tables involved.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue as codec;
use crate::index_lifecycle::queue::backlog::{
    BacklogLimits, IndexOperationBacklog, OperationCharge,
};
use crate::index_lifecycle::queue::storage::StoredQueue;
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, TextPartition,
};

/// Heap accounting of the calling thread, supplied by a test binary's global
/// allocator.
pub trait AllocationProbe {
    /// Heap bytes this thread holds now, as the allocator charges them.
    fn allocated(&self) -> isize;
    /// Restarts peak tracking at the current allocation.
    fn reset_peak(&self);
    /// Highest [`Self::allocated`] since the last [`Self::reset_peak`].
    fn peak(&self) -> isize;
}

/// Worst measured heap bytes per retained operation of one shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueMemorySample {
    /// Operation shape.
    pub shape: &'static str,
    /// Encoded record bytes of one operation.
    pub encoded: u64,
    /// Admission ledger bytes per operation.
    pub ledger: u64,
    /// Decoded queue bytes per operation.
    pub decoded: u64,
    /// Peak resolution bytes per operation against a resolved base.
    pub resolution_over_base: u64,
    /// Peak resolution bytes per operation of single-operation operands.
    pub resolution_of_operands: u64,
}

/// Backlog lengths just past the growth boundaries of `Vec` doubling and of
/// hash tables at their 7/8 load factor, where per-operation memory peaks.
const LENGTHS: [usize; 6] = [8_193, 14_337, 16_385, 28_673, 32_769, 57_345];

/// Samples every operation shape a foreground write can queue.
pub fn index_operation_queue_memory_samples(probe: &dyn AllocationProbe) -> Vec<QueueMemorySample> {
    let tenant = || TextPartition::TenantValue(Bytes::from_static(b"tenant-a"));
    let vector = |dimensions: usize| -> Arc<[f32]> {
        (0..dimensions)
            .map(|component| component as f32 * 0.25 + 1.0)
            .collect()
    };
    let text_insert = |text: &str| {
        let text: Arc<str> = Arc::from(text);
        move || {
            codec::QueuedPayload::Text(codec::QueuedTextPayload {
                replacement: Some(codec::QueuedTextReplacement::new(
                    TextPartition::Unpartitioned,
                    Arc::clone(&text),
                )),
            })
        }
    };
    let vector_insert = |dimensions: usize, partition: fn() -> TextPartition, previous: bool| {
        let components = vector(dimensions);
        move || {
            codec::QueuedPayload::Vector(codec::QueuedVectorPayload {
                previous: previous.then(partition),
                replacement: Some(
                    codec::QueuedVectorReplacement::try_new(partition(), Arc::clone(&components))
                        .expect("a finite non-empty vector"),
                ),
            })
        }
    };
    let short_text = text_insert("doc 1234567 g7 alpha shared");
    let long_text = text_insert(&"lorem ipsum ".repeat(342));
    let shapes: Vec<(&'static str, Box<dyn Fn() -> codec::QueuedPayload>)> = vec![
        (
            "text delete",
            Box::new(|| codec::QueuedPayload::Text(codec::QueuedTextPayload { replacement: None })),
        ),
        (
            "vector delete",
            Box::new(|| {
                codec::QueuedPayload::Vector(codec::QueuedVectorPayload {
                    previous: Some(TextPartition::Unpartitioned),
                    replacement: None,
                })
            }),
        ),
        ("text 28 B", Box::new(short_text)),
        (
            "vector 4d",
            Box::new(vector_insert(4, || TextPartition::Unpartitioned, false)),
        ),
        (
            "vector 4d tenant update",
            Box::new(vector_insert(4, tenant, true)),
        ),
        (
            "vector 128d",
            Box::new(vector_insert(128, || TextPartition::Unpartitioned, false)),
        ),
        (
            "vector 1536d",
            Box::new(vector_insert(1_536, || TextPartition::Unpartitioned, false)),
        ),
        ("text 4 KiB", Box::new(long_text)),
    ];
    shapes
        .iter()
        .map(|(shape, payload)| sample(probe, shape, payload.as_ref()))
        .collect()
}

fn sample(
    probe: &dyn AllocationProbe,
    shape: &'static str,
    payload: &dyn Fn() -> codec::QueuedPayload,
) -> QueueMemorySample {
    let target = QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).expect("index ID is nonzero"),
        IndexGenerationId::new(1).expect("generation is nonzero"),
    );
    let mut measured = QueueMemorySample {
        shape,
        encoded: 0,
        ledger: 0,
        decoded: 0,
        resolution_over_base: 0,
        resolution_of_operands: 0,
    };
    for length in LENGTHS {
        // Realistic graph IDs encode as three-byte varints.
        let operations = (0..length)
            .map(|offset| {
                codec::QueuedOperation::new(
                    codec::QueuedOperationId::generate(),
                    IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(1_000_000 + offset as u64),
                    },
                    payload(),
                )
            })
            .collect::<Vec<_>>();
        let encoded = operations[0].retained_bytes();
        assert!(
            operations
                .iter()
                .all(|operation| operation.retained_bytes() == encoded),
            "{shape}: every sampled operation encodes alike"
        );
        measured.encoded = encoded;
        let per_operation = |bytes: isize| {
            u64::try_from(bytes).expect("a retained structure holds memory") / length as u64
        };

        let before = probe.allocated();
        let backlog = IndexOperationBacklog::new(
            BacklogLimits {
                max_retained_bytes: u64::MAX,
                max_members: u64::MAX,
            },
            crate::index_lifecycle::worker::IndexWorkerWakeHandle::default(),
        );
        for batch in operations.chunks(2_000) {
            let charges = batch
                .iter()
                .map(|operation| OperationCharge {
                    target,
                    entity: operation.entity(),
                    id: operation.id(),
                    bytes: operation.retained_bytes(),
                })
                .collect::<Vec<_>>();
            let mut reservation = backlog.reserve(&charges, &[]).expect("unbounded ledger");
            reservation.begin_commit();
            reservation.committed();
        }
        measured.ledger = measured
            .ledger
            .max(per_operation(probe.allocated() - before));
        drop(backlog);

        let operands = operations
            .iter()
            .map(|operation| {
                codec::QueueOperand::enqueue(std::slice::from_ref(operation))
                    .expect("a valid operand")
                    .into_parts()
                    .0
            })
            .collect::<Vec<_>>();
        let (base_operations, newest) = operations.split_at(length - 1);
        let base = resolved(
            codec::QueueOperand::enqueue(base_operations)
                .expect("a valid operand")
                .into_parts()
                .0,
        );
        let newest = codec::QueueOperand::enqueue(newest)
            .expect("a valid operand")
            .into_parts()
            .0;

        probe.reset_peak();
        let before = probe.allocated();
        let over_base = codec::merge_with_base(Some(&base), std::slice::from_ref(&newest))
            .expect("a valid merge");
        measured.resolution_over_base = measured
            .resolution_over_base
            .max(per_operation(probe.peak() - before));
        drop(over_base);

        probe.reset_peak();
        let before = probe.allocated();
        let of_operands = resolved_merge(&operands);
        measured.resolution_of_operands = measured
            .resolution_of_operands
            .max(per_operation(probe.peak() - before));

        let before = probe.allocated();
        let queue = codec::OperationQueue::decode(&of_operands).expect("a resolved queue");
        let stored = StoredQueue::new(
            queue.family(),
            queue.into_operations(),
            HashMap::new(),
            of_operands.len() as u64,
        )
        .expect("a non-empty queue");
        measured.decoded = measured
            .decoded
            .max(per_operation(probe.allocated() - before));
        assert_eq!(
            stored.operations().count(),
            length,
            "{shape}: every operation decodes"
        );
        drop(stored);
    }
    measured
}

fn resolved_merge(operands: &[Bytes]) -> Bytes {
    match codec::merge_with_base(None, operands).expect("a valid merge") {
        codec::QueueMergeResult::Value(value) => value,
        codec::QueueMergeResult::Empty => panic!("an enqueue-only merge retains its operations"),
    }
}

fn resolved(operand: Bytes) -> Bytes {
    resolved_merge(std::slice::from_ref(&operand))
}
