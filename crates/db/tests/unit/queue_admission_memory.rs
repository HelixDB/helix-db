//! Measured process memory of retained index-queue operations.
//!
//! The admission ledger bounds each logical index's backlog in charged bytes:
//! each operation's encoded size plus a fixed overhead. These samples
//! measure, through the test binary's thread-local allocation observer at
//! glibc's chunk size (the server image's allocator), the heap a backlog of
//! one operation shape actually holds per operation:
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

use crate::allocation_testing;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue as codec;
use crate::index_lifecycle::queue::backlog::{
    charged_bytes, BacklogLimits, IndexOperationBacklog, OperationCharge,
};
use crate::index_lifecycle::queue::storage::StoredQueue;
use crate::index_lifecycle::queue::QueueTarget;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, TextPartition,
};

/// Worst measured heap bytes per retained operation of one shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueueMemorySample {
    /// Operation shape.
    shape: &'static str,
    /// Encoded record bytes of one operation.
    encoded: u64,
    /// Bytes one operation counts toward its index's retained-byte limit.
    charged: u64,
    /// Admission ledger bytes per operation.
    ledger: u64,
    /// Decoded queue bytes per operation.
    decoded: u64,
    /// Peak resolution bytes per operation against a resolved base.
    resolution_over_base: u64,
    /// Peak resolution bytes per operation of single-operation operands.
    resolution_of_operands: u64,
}

/// Builds one sampled operation's payload.
type PayloadFn = dyn Fn() -> codec::QueuedPayload;

/// Backlog lengths just past the growth boundaries of `Vec` doubling and of
/// hash tables at their 7/8 load factor, where per-operation memory peaks.
const LENGTHS: [usize; 6] = [8_193, 14_337, 16_385, 28_673, 32_769, 57_345];

/// Samples every operation shape a foreground write can queue.
fn index_operation_queue_memory_samples() -> Vec<QueueMemorySample> {
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
    let shapes: Vec<(&'static str, Box<PayloadFn>)> = vec![
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
        .map(|(shape, payload)| sample(shape, payload.as_ref()))
        .collect()
}

fn sample(shape: &'static str, payload: &PayloadFn) -> QueueMemorySample {
    let target = QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).expect("index ID is nonzero"),
        IndexGenerationId::new(1).expect("generation is nonzero"),
    );
    let mut measured = QueueMemorySample {
        shape,
        encoded: 0,
        charged: 0,
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
        measured.charged = charged_bytes(encoded);
        let per_operation = |bytes: i128| {
            u64::try_from(bytes).expect("a retained structure holds memory") / length as u64
        };

        let (backlog, ledger) = allocation_testing::observe(|| {
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
                        encoded_bytes: operation.retained_bytes(),
                    })
                    .collect::<Vec<_>>();
                let mut reservation = backlog.reserve(&charges, &[]).expect("unbounded ledger");
                reservation.begin_commit();
                reservation.committed();
            }
            backlog
        });
        measured.ledger = measured.ledger.max(per_operation(ledger.chunk_live_bytes));
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

        let (over_base, resolution) = allocation_testing::observe(|| {
            codec::merge_with_base(Some(&base), std::slice::from_ref(&newest))
                .expect("a valid merge")
        });
        measured.resolution_over_base = measured
            .resolution_over_base
            .max(per_operation(resolution.chunk_peak_bytes));
        drop(over_base);

        let (of_operands, resolution) = allocation_testing::observe(|| resolved_merge(&operands));
        measured.resolution_of_operands = measured
            .resolution_of_operands
            .max(per_operation(resolution.chunk_peak_bytes));

        let (stored, decoded) = allocation_testing::observe(|| {
            let queue = codec::OperationQueue::decode(&of_operands).expect("a resolved queue");
            StoredQueue::new(
                queue.family(),
                queue.into_operations(),
                HashMap::new(),
                of_operands.len() as u64,
            )
            .expect("a non-empty queue")
        });
        measured.decoded = measured
            .decoded
            .max(per_operation(decoded.chunk_live_bytes));
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

/// The heap one retained operation holds in the admission ledger, in one
/// decoded copy of its queue, and in one resolution of its queue's merge
/// operands stays within twice what it is charged, for every operation shape;
/// the fixed overhead never charges more than the smallest operation costs.
#[test]
fn retained_operation_memory_stays_within_twice_its_charge() {
    let overhead = crate::config::IndexOperationQueueTuning::OPERATION_OVERHEAD_BYTES;
    let samples = index_operation_queue_memory_samples();
    println!(
        "{:<24} {:>7} {:>7} {:>7} {:>7} {:>8} {:>7} {:>9} {:>10}",
        "shape",
        "encoded",
        "charged",
        "ledger",
        "decoded",
        "res/base",
        "res/ops",
        "heap/chg",
        "heap/enc"
    );
    let heap = |sample: &QueueMemorySample| {
        sample.ledger
            + sample.decoded
            + sample
                .resolution_over_base
                .max(sample.resolution_of_operands)
    };
    // Every shape is reported before any is checked, so one failure still
    // shows the whole calibration.
    for sample in &samples {
        let QueueMemorySample {
            shape,
            encoded,
            charged,
            ledger,
            decoded,
            resolution_over_base,
            resolution_of_operands,
        } = *sample;
        println!(
            "{shape:<24} {encoded:>7} {charged:>7} {ledger:>7} {decoded:>7} \
             {resolution_over_base:>8} {resolution_of_operands:>7} {:>9.2} {:>10.2}",
            heap(sample) as f64 / charged as f64,
            heap(sample) as f64 / encoded as f64,
        );
    }
    for sample in &samples {
        let shape = sample.shape;
        assert_eq!(sample.charged, sample.encoded + overhead, "{shape}");
        assert!(
            heap(sample) <= 2 * sample.charged,
            "{shape}: {} heap bytes for {sample:?}",
            heap(sample)
        );
        assert!(
            heap(sample) >= overhead,
            "{shape}: the overhead exceeds what it covers"
        );
    }
}
