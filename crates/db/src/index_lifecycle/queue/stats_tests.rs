//! Durable-commit, exact-acknowledgement, lag, and merge-cost counters
//! observed through real queued writes, publication, restart, and SlateDB
//! merge resolution.

use std::sync::Arc;

use slatedb::config::{MergeOptions, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::overlay_tests::{add, drain, update};
use super::tests::{install_vector_and_text, open, queued, target};
use super::QueueTarget;
use crate::config::IndexOperationQueueTuning;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::IndexEntity;
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QueueOperand, QueuedOperation, QueuedOperationId, QueuedPayload,
    QueuedTextPayload,
};
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};
use crate::merge_operator::{HelixMergeOperator, QueueMerges};

#[tokio::test]
async fn every_acknowledgement_is_timed_or_censored_across_restart() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = || queued(IndexOperationQueueTuning::default());
    let db = open("queue-stats", Arc::clone(&store), config()).await;
    install_vector_and_text(&db).await;
    let first = add(&db, [1.0, 0.0], "first", None).await;
    add(&db, [0.0, 1.0], "second", None).await;
    update(&db, first, [2.0, 2.0], "first again").await;

    // Three writes enqueue one vector and one text operation each.
    let pending = db.index_operation_queue_stats();
    assert_eq!(pending.committed_operations, 6);
    assert_eq!(pending.pending_operations, 6);
    assert_eq!(pending.acknowledged_operations, 0);
    assert!(pending.oldest_pending_micros > 0);

    let vector = target(&db, QueueFamily::Vector).await;
    let text = target(&db, QueueFamily::Text).await;
    drain(&db, vector).await;
    drain(&db, text).await;
    let published = db.index_operation_queue_stats();
    assert_eq!(published.acknowledged_operations, 6);
    assert_eq!(published.censored_acknowledgements, 0);
    assert_eq!(published.published_operations, 6);
    assert_eq!(published.pending_operations, 0);
    assert_eq!(published.oldest_pending_micros, 0);
    assert!(published.publication_attempts >= 2);
    assert!(published.publication_attempt_micros > 0);
    assert_eq!(published.publication_retries, 0);
    assert_eq!(db.index_operation_publication_lag().count(), 6);

    // Work retained across a restart has no observed commit instant.
    add(&db, [3.0, 3.0], "before restart", None).await;
    db.close().await.unwrap();
    let db = open("queue-stats", Arc::clone(&store), config()).await;
    let reopened = db.index_operation_queue_stats();
    assert_eq!(
        (
            reopened.committed_operations,
            reopened.discovered_operations
        ),
        (0, 2)
    );
    assert_eq!(reopened.oldest_pending_micros, 0);
    drain(&db, vector).await;
    drain(&db, text).await;
    let settled = db.index_operation_queue_stats();
    assert_eq!(
        (
            settled.acknowledged_operations,
            settled.censored_acknowledgements
        ),
        (2, 2)
    );
    assert_eq!(db.index_operation_publication_lag().count(), 0);
    db.close().await.unwrap();
}

#[tokio::test]
async fn queue_merge_costs_count_slatedb_batches_not_chain_length() {
    // Only this database counts here, so parallel tests cannot move the costs.
    static MERGES: QueueMerges = QueueMerges::new();
    let db = slatedb::Db::builder("queue-merge-costs", Arc::new(InMemory::new()))
        .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
        .build()
        .await
        .unwrap();
    let key = QueueTarget::new(
        DataScope::LegacyUnscoped,
        IndexId::new(1).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    )
    .key();
    // 250 single-operation enqueues form one 250-operand merge chain.
    for id in 1..=250_u64 {
        let operation = QueuedOperation::new(
            QueuedOperationId::try_from_u128(u128::from(id)).unwrap(),
            IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::new(id),
            },
            QueuedPayload::Text(QueuedTextPayload { replacement: None }),
        );
        let (bytes, _) = QueueOperand::enqueue(&[operation]).unwrap().into_parts();
        db.merge_with_options(
            &key,
            bytes,
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
                ..WriteOptions::default()
            },
        )
        .await
        .unwrap();
    }
    let written = MERGES.stats();
    let value = db.get(&key).await.unwrap().unwrap();
    assert_eq!(
        OperationQueue::decode(&value).unwrap().operations().len(),
        250
    );
    let read = MERGES.stats();

    // The read folds raw operands in SlateDB batches of at most 100 ...
    assert_eq!(read.partial.merges - written.partial.merges, 3);
    assert_eq!(read.partial.operands - written.partial.operands, 250);
    assert_eq!(read.partial.max_operands, 100, "a batch, not the chain");
    // ... then resolves only the three batch results against the base.
    assert_eq!(read.resolved.merges - written.resolved.merges, 1);
    assert_eq!(read.resolved.operands - written.resolved.operands, 3);
    assert_eq!(read.resolved.max_operands, 3);
    assert_eq!(
        read.resolved.input_bytes - written.resolved.input_bytes,
        read.partial.output_bytes - written.partial.output_bytes,
        "a resolution reads the partial outputs again"
    );
    assert_eq!(
        read.resolved.output_bytes - written.resolved.output_bytes,
        value.len() as u64
    );
    db.close().await.unwrap();
}
