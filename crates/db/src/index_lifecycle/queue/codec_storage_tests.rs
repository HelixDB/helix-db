//! Queue contracts against real SlateDB transactions, flushes, and compaction.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use slatedb::object_store::memory::InMemory;
use slatedb::{
    compactor, config, Db, IsolationLevel, MergeOperator, MergeOperatorError, MergeResult,
};

use bytes::Bytes;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{IndexEntity, IndexOperationQueueKey, ManagedIndexKey, ScopedKey};
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QueueOperand, QueuedOperation, QueuedOperationId, QueuedPayload,
    QueuedTextPayload, QueuedTextReplacement,
};
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};
use crate::merge_operator::HelixMergeOperator;

fn id(value: u128) -> QueuedOperationId {
    QueuedOperationId::try_from_u128(value).expect("test operation IDs keep bit 127 clear")
}

fn text_operation(operation: u128, entity: u64, text: Option<&str>) -> QueuedOperation {
    QueuedOperation::new(
        id(operation),
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(entity),
        },
        QueuedPayload::Text(QueuedTextPayload {
            replacement: text.map(|text| {
                QueuedTextReplacement::new(TextPartition::Unpartitioned, Arc::from(text))
            }),
        }),
    )
}

fn ids_of(queue: Option<&OperationQueue>) -> Vec<u128> {
    queue.map_or_else(Vec::new, |queue| {
        queue
            .operations()
            .iter()
            .map(|operation| operation.id().get())
            .collect()
    })
}

const PATH: &str = "operation-queue-storage";

fn queue_key() -> Bytes {
    ManagedIndexKey::Data {
        scope: DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(0x51)),
        kind: ScopedKey::IndexOperationQueue(IndexOperationQueueKey {
            index_id: IndexId::new(3).unwrap(),
            generation: IndexGenerationId::new(1).unwrap(),
        }),
    }
    .to_bytes()
}

/// Counts every merge-operator path so tests can prove who resolved the row.
#[derive(Default)]
struct CountingMergeOperator {
    inner: HelixMergeOperator,
    partial: AtomicUsize,
    resolved: AtomicUsize,
}

impl CountingMergeOperator {
    fn resolved(&self) -> usize {
        self.resolved.load(Ordering::SeqCst)
    }

    fn partial(&self) -> usize {
        self.partial.load(Ordering::SeqCst)
    }
}

impl MergeOperator for CountingMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.partial.fetch_add(1, Ordering::SeqCst);
        self.inner.merge(key, existing_value, value)
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        self.partial.fetch_add(1, Ordering::SeqCst);
        self.inner.merge_batch(key, existing_value, operands)
    }

    fn merge_batch_with_base(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<MergeResult, MergeOperatorError> {
        self.resolved.fetch_add(1, Ordering::SeqCst);
        self.inner
            .merge_batch_with_base(key, existing_value, operands)
    }
}

async fn open(store: Arc<InMemory>, merge: Arc<dyn MergeOperator + Send + Sync>) -> Db {
    Db::builder(PATH, store)
        .with_merge_operator(merge)
        .build()
        .await
        .expect("queue database opens")
}

async fn stage(transaction: &slatedb::DbTransaction, operand: QueueOperand) {
    let (bytes, tokens) = operand.into_parts();
    transaction
        .merge_disjoint_tokens(queue_key(), tokens, bytes)
        .expect("blind queue merge stages");
}

fn enqueue(operations: &[QueuedOperation]) -> QueueOperand {
    QueueOperand::enqueue(operations).expect("valid enqueue")
}

fn acknowledge(ids: &[u128]) -> QueueOperand {
    QueueOperand::acknowledge(QueueFamily::Text, ids.iter().copied().map(id)).expect("valid ack")
}

async fn queued_ids(reader: &(impl slatedb::DbReadOps + Sync)) -> Vec<u128> {
    let value = reader.get(queue_key()).await.expect("queue reads");
    ids_of(
        value
            .map(|value| OperationQueue::decode(&value).expect("resolved queue decodes"))
            .as_ref(),
    )
}

async fn commit(db: &Db, operands: Vec<QueueOperand>) {
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    for operand in operands {
        stage(&transaction, operand).await;
    }
    transaction
        .commit()
        .await
        .expect("queue transaction commits");
}

#[tokio::test]
async fn competing_same_entity_enqueues_conflict_but_distinct_entities_commit() {
    let db = open(
        Arc::new(InMemory::new()),
        Arc::new(HelixMergeOperator::new()),
    )
    .await;

    let first = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let second = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    stage(&first, enqueue(&[text_operation(1, 10, Some("a"))])).await;
    stage(&second, enqueue(&[text_operation(2, 10, Some("b"))])).await;
    first
        .commit()
        .await
        .expect("first same-entity enqueue commits");
    let conflict = second
        .commit()
        .await
        .expect_err("same entity must serialize");
    assert_eq!(conflict.kind(), slatedb::ErrorKind::Transaction);

    let left = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let right = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    stage(&left, enqueue(&[text_operation(3, 11, Some("c"))])).await;
    stage(&right, enqueue(&[text_operation(4, 12, Some("d"))])).await;
    right.commit().await.expect("distinct entity commits");
    left.commit()
        .await
        .expect("disjoint entity commits concurrently");

    // Two acknowledgements of one exact operation ID conflict.
    let ack_one = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let ack_two = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    stage(&ack_one, acknowledge(&[1])).await;
    stage(&ack_two, acknowledge(&[1, 3])).await;
    ack_one.commit().await.unwrap();
    assert_eq!(
        ack_two
            .commit()
            .await
            .expect_err("same ID conflicts")
            .kind(),
        slatedb::ErrorKind::Transaction
    );
    assert_eq!(queued_ids(&db).await, vec![4, 3]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn older_acknowledgement_and_newer_enqueue_commit_in_either_order() {
    for ack_first in [true, false] {
        let db = open(
            Arc::new(InMemory::new()),
            Arc::new(HelixMergeOperator::new()),
        )
        .await;
        commit(&db, vec![enqueue(&[text_operation(1, 7, Some("old"))])]).await;

        let ack = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let newer = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage(&ack, acknowledge(&[1])).await;
        stage(&newer, enqueue(&[text_operation(2, 7, Some("new"))])).await;
        if ack_first {
            ack.commit().await.expect("older ACK commits");
            newer
                .commit()
                .await
                .expect("newer enqueue is disjoint from the ACK");
        } else {
            newer.commit().await.expect("newer enqueue commits");
            ack.commit()
                .await
                .expect("older ACK is disjoint from the enqueue");
        }
        assert_eq!(queued_ids(&db).await, vec![2], "ack_first={ack_first}");
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn producer_staging_registers_no_queue_read_and_resolves_nothing() {
    let merge = Arc::new(CountingMergeOperator::default());
    let db = open(Arc::new(InMemory::new()), merge.clone()).await;
    commit(&db, vec![enqueue(&[text_operation(1, 1, Some("base"))])]).await;
    db.flush().await.unwrap();

    let resolved_before = merge.resolved();
    let producer = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    stage(&producer, enqueue(&[text_operation(2, 2, Some("x"))])).await;
    stage(&producer, enqueue(&[text_operation(3, 3, Some("y"))])).await;
    // A concurrent disjoint writer and a worker ACK commit first; a read
    // dependency on the row would make the producer abort.
    commit(&db, vec![enqueue(&[text_operation(4, 4, Some("z"))])]).await;
    commit(&db, vec![acknowledge(&[1])]).await;
    producer
        .commit()
        .await
        .expect("blind producer has no row dependency");
    assert_eq!(
        merge.resolved(),
        resolved_before,
        "staging and commit never resolve the existing queue"
    );
    assert_eq!(queued_ids(&db).await, vec![4, 2, 3]);
    assert!(
        merge.resolved() > resolved_before,
        "the explicit read resolves"
    );
    assert!(merge.partial() > 0, "two operands in one batch compose");
    db.close().await.unwrap();
}

#[tokio::test]
async fn flush_reopen_snapshots_and_reader_replay_preserve_order() {
    let store = Arc::new(InMemory::new());
    let db = open(store.clone(), Arc::new(HelixMergeOperator::new())).await;
    commit(
        &db,
        vec![enqueue(&[
            text_operation(1, 1, Some("a1")),
            text_operation(2, 2, Some("b1")),
        ])],
    )
    .await;
    let early = db.snapshot().await.unwrap();
    db.flush_with_options(config::FlushOptions {
        flush_type: config::FlushType::MemTable,
    })
    .await
    .unwrap();
    commit(&db, vec![enqueue(&[text_operation(3, 1, Some("a2"))])]).await;
    commit(&db, vec![acknowledge(&[1])]).await;
    commit(&db, vec![enqueue(&[text_operation(4, 1, Some("a3"))])]).await;
    assert_eq!(queued_ids(&db).await, vec![2, 3, 4]);
    assert_eq!(queued_ids(early.as_ref()).await, vec![1, 2]);

    // A reader replaying the WAL observes the same committed order.
    db.flush().await.unwrap();
    let reader = slatedb::DbReader::builder(PATH, store.clone())
        .with_merge_operator(Arc::new(HelixMergeOperator::new()))
        .build()
        .await
        .unwrap();
    assert_eq!(queued_ids(&reader).await, vec![2, 3, 4]);
    reader.close().await.unwrap();
    drop(early);
    db.close().await.unwrap();

    let reopened = open(store, Arc::new(HelixMergeOperator::new())).await;
    assert_eq!(queued_ids(&reopened).await, vec![2, 3, 4]);
    commit(&reopened, vec![acknowledge(&[2, 3, 4])]).await;
    assert_eq!(
        reopened.get(queue_key()).await.unwrap(),
        None,
        "an empty queue is absent"
    );
    reopened.close().await.unwrap();
}

async fn submit_compaction(
    admin: &slatedb::admin::Admin,
    sources: Vec<compactor::SourceId>,
    destination: u32,
) {
    assert!(!sources.is_empty(), "the test must perform real compaction");
    let submitted = admin
        .submit_compaction(compactor::CompactionSpec::new(sources, destination))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let current = admin
                .read_compaction(submitted.id(), None)
                .await
                .unwrap()
                .unwrap();
            match current.status() {
                compactor::CompactionStatus::Completed => break,
                compactor::CompactionStatus::Failed => panic!("compaction failed: {current:?}"),
                compactor::CompactionStatus::Submitted
                | compactor::CompactionStatus::Scheduled
                | compactor::CompactionStatus::Running
                | compactor::CompactionStatus::Compacted => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await
    .expect("compaction commits its manifest");
}

async fn compact_l0(admin: &slatedb::admin::Admin, destination: u32) {
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    submit_compaction(
        admin,
        manifest
            .l0()
            .iter()
            .map(|sst| compactor::SourceId::SstView(sst.id))
            .collect(),
        destination,
    )
    .await;
}

async fn flushed_commit(db: &Db, operands: Vec<QueueOperand>) {
    commit(db, operands).await;
    db.flush_with_options(config::FlushOptions {
        flush_type: config::FlushType::MemTable,
    })
    .await
    .unwrap();
}

/// Reads only compacted SSTs, with no WAL replay or writer memtable.
async fn compacted_ids(store: &Arc<InMemory>) -> Vec<u128> {
    let reader = slatedb::DbReader::builder(PATH, store.clone())
        .with_db_cache_disabled()
        .with_options(config::DbReaderOptions {
            skip_wal_replay: true,
            ..Default::default()
        })
        .with_merge_operator(Arc::new(HelixMergeOperator::new()))
        .build()
        .await
        .unwrap();
    let ids = queued_ids(&reader).await;
    reader.close().await.unwrap();
    ids
}

#[tokio::test]
async fn multi_level_compaction_preserves_order_resets_and_acknowledgements() {
    let store = Arc::new(InMemory::new());
    let settings = config::Settings {
        flush_interval: Some(Duration::from_millis(1)),
        manifest_poll_interval: Duration::from_millis(10),
        compactor_options: Some(config::CompactorOptions {
            poll_interval: Duration::from_millis(10),
            commit_compacted_interval: Duration::from_millis(10),
            // Manual submissions decide exactly which base stays hidden.
            scheduler_options: config::SizeTieredCompactionSchedulerOptions {
                min_compaction_sources: 1024,
                max_compaction_sources: 1024,
                ..Default::default()
            }
            .into(),
            worker: Some(config::CompactionWorkerOptions {
                compactions_poll_interval: Duration::from_millis(10),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let db = Db::builder(PATH, store.clone())
        .with_settings(settings.clone())
        .with_merge_operator(Arc::new(HelixMergeOperator::new()))
        .build()
        .await
        .unwrap();
    let admin = slatedb::admin::Admin::builder(PATH, store.clone()).build();

    flushed_commit(
        &db,
        vec![enqueue(&[
            text_operation(1, 1, Some("e1-first")),
            text_operation(2, 2, Some("e2-first")),
        ])],
    )
    .await;
    compact_l0(&admin, 0).await;
    let oldest = db.snapshot().await.unwrap();

    // Upper runs: ACK(1), a newer same-entity operation, and a reset of ID 2
    // composed without the older base.
    flushed_commit(&db, vec![acknowledge(&[1])]).await;
    flushed_commit(
        &db,
        vec![
            enqueue(&[text_operation(3, 1, Some("e1-second"))]),
            acknowledge(&[2]),
        ],
    )
    .await;
    flushed_commit(
        &db,
        vec![enqueue(&[text_operation(2, 2, Some("e2-reset"))])],
    )
    .await;
    compact_l0(&admin, 1).await;
    assert_eq!(compacted_ids(&store).await, vec![3, 2]);
    let intermediate = db.snapshot().await.unwrap();

    flushed_commit(
        &db,
        vec![enqueue(&[text_operation(4, 1, Some("e1-third"))])],
    )
    .await;
    compact_l0(&admin, 2).await;
    submit_compaction(
        &admin,
        vec![
            compactor::SourceId::SortedRun(2),
            compactor::SourceId::SortedRun(1),
        ],
        1,
    )
    .await;
    assert_eq!(compacted_ids(&store).await, vec![3, 2, 4]);
    submit_compaction(
        &admin,
        vec![
            compactor::SourceId::SortedRun(1),
            compactor::SourceId::SortedRun(0),
        ],
        0,
    )
    .await;
    let bottom = compacted_ids(&store).await;
    assert_eq!(bottom, vec![3, 2, 4]);
    let resolved = db.get(queue_key()).await.unwrap().unwrap();
    let queue = OperationQueue::decode(&resolved).unwrap();
    let bodies = queue
        .operations()
        .iter()
        .map(|operation| match operation.payload() {
            QueuedPayload::Text(payload) => {
                payload.replacement.as_ref().unwrap().text().to_string()
            }
            QueuedPayload::Vector(_) => unreachable!("text queue"),
        })
        .collect::<Vec<_>>();
    assert_eq!(bodies, ["e1-second", "e2-reset", "e1-third"]);
    assert_eq!(queued_ids(oldest.as_ref()).await, vec![1, 2]);
    assert_eq!(queued_ids(intermediate.as_ref()).await, vec![3, 2]);
    drop(oldest);
    drop(intermediate);

    // Draining the queue leaves no row after bottom-level compaction.
    flushed_commit(&db, vec![acknowledge(&[2, 3, 4])]).await;
    compact_l0(&admin, 1).await;
    submit_compaction(
        &admin,
        vec![
            compactor::SourceId::SortedRun(1),
            compactor::SourceId::SortedRun(0),
        ],
        0,
    )
    .await;
    assert!(compacted_ids(&store).await.is_empty());
    db.close().await.unwrap();
    let reopened = Db::builder(PATH, store)
        .with_settings(settings)
        .with_merge_operator(Arc::new(HelixMergeOperator::new()))
        .build()
        .await
        .unwrap();
    assert_eq!(reopened.get(queue_key()).await.unwrap(), None);
    reopened.close().await.unwrap();
}
