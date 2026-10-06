//! Queue contracts against real SlateDB transactions, flushes, and compaction.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use slatedb::{
    compactor, config, Db, IsolationLevel, MergeOperator, MergeOperatorError, MergeResult,
};

use bytes::Bytes;

use super::storage::QueueStore;
use super::QueueTarget;
use crate::config::QueueLayout;
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{
    IndexEntity, IndexOperationQueueKey, IndexOperationRowKey, ManagedIndexKey, ScopedKey,
};
use crate::encoding::v2::values::indexes::operation_queue::{
    merge_with_base, OperationQueue, QueueFamily, QueueMergeResult, QueueOperand, QueueRow,
    QueuedOperation, QueuedOperationId, QueuedPayload, QueuedTextPayload, QueuedTextReplacement,
};
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};
use crate::merge_operator::{HelixMergeOperator, QueueMerges};

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
        scope: DataScope::Tenant(TenantId::from_u128(0x51)),
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

/// Settings under which only submitted compactions run, so a test decides
/// exactly which base stays hidden below upper sorted runs.
fn manual_compaction_settings() -> config::Settings {
    config::Settings {
        flush_interval: Some(Duration::from_millis(1)),
        manifest_poll_interval: Duration::from_millis(10),
        compactor_options: Some(config::CompactorOptions {
            poll_interval: Duration::from_millis(10),
            commit_compacted_interval: Duration::from_millis(10),
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
    }
}

#[tokio::test]
async fn multi_level_compaction_preserves_order_resets_and_acknowledgements() {
    let store = Arc::new(InMemory::new());
    let settings = manual_compaction_settings();
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

/// IDs of the compacted sorted runs, in ascending order.
async fn sorted_runs(admin: &slatedb::admin::Admin) -> Vec<u32> {
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    let mut runs = manifest
        .compacted()
        .iter()
        .map(|run| run.id)
        .collect::<Vec<_>>();
    runs.sort_unstable();
    runs
}

#[tokio::test]
async fn acknowledged_removals_stay_bounded_above_an_uncompacted_bottom_run() {
    // Only this database counts here, so parallel tests cannot move the costs.
    static MERGES: QueueMerges = QueueMerges::new();
    const LIVE: u128 = 1;
    const ROUNDS: u128 = 20;
    const PER_ROUND: u128 = 500;
    // Enqueued just before the midway snapshot and acknowledged after it, so
    // that snapshot provably retains a version the upper runs superseded.
    const SPLIT: u128 = LIVE + ROUNDS * PER_ROUND + 1;
    let store = Arc::new(InMemory::new());
    let db = Db::builder(PATH, store.clone())
        .with_settings(manual_compaction_settings())
        .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
        .build()
        .await
        .unwrap();
    let admin = slatedb::admin::Admin::builder(PATH, store.clone()).build();

    flushed_commit(&db, vec![enqueue(&[text_operation(LIVE, 1, Some("live"))])]).await;
    compact_l0(&admin, 0).await;
    let mut snapshots = vec![db.snapshot().await.unwrap()];
    for round in 0..ROUNDS {
        let ids = (0..PER_ROUND)
            .map(|offset| LIVE + 1 + round * PER_ROUND + offset)
            .collect::<Vec<_>>();
        let operations = ids
            .iter()
            .map(|&operation| {
                text_operation(
                    operation,
                    u64::try_from(operation).unwrap(),
                    Some("acknowledged"),
                )
            })
            .collect::<Vec<_>>();
        commit(&db, vec![enqueue(&operations)]).await;
        let mut acknowledged = ids;
        if round == ROUNDS / 2 + 1 {
            acknowledged.push(SPLIT);
        }
        flushed_commit(&db, vec![acknowledge(&acknowledged)]).await;
        // Every round composes into SR1; SR0 keeps the only base.
        if round == 0 {
            compact_l0(&admin, 1).await;
        } else {
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
        }
        if round == ROUNDS / 2 {
            commit(
                &db,
                vec![enqueue(&[text_operation(SPLIT, 0, Some("split"))])],
            )
            .await;
            snapshots.push(db.snapshot().await.unwrap());
        }
    }
    assert_eq!(sorted_runs(&admin).await, vec![0, 1]);
    assert_eq!(
        queued_ids(snapshots[1].as_ref()).await,
        vec![LIVE, SPLIT],
        "the midway snapshot still pins its version across the compactions"
    );

    let before = MERGES.stats();
    assert_eq!(queued_ids(&db).await, vec![LIVE]);
    let read = MERGES.stats();
    assert_eq!(
        read.resolved.merges - before.resolved.merges,
        1,
        "the read resolves the upper run against SR0's base"
    );
    let read_cost = read.resolved.input_bytes - before.resolved.input_bytes;
    assert!(
        read_cost < 4 * 1024,
        "reading a one-operation queue resolved {read_cost} bytes after {} acknowledgements",
        ROUNDS * PER_ROUND + 1
    );

    // Released snapshots stop pinning anything: once a flush publishes that,
    // compacting everything into the bottom run leaves only the live value.
    drop(snapshots);
    let ids = (0..PER_ROUND)
        .map(|offset| SPLIT + 1 + offset)
        .collect::<Vec<_>>();
    let operations = ids
        .iter()
        .map(|&operation| {
            text_operation(
                operation,
                u64::try_from(operation).unwrap(),
                Some("acknowledged"),
            )
        })
        .collect::<Vec<_>>();
    commit(&db, vec![enqueue(&operations)]).await;
    flushed_commit(&db, vec![acknowledge(&ids)]).await;
    let manifest = admin.read_manifest(None).await.unwrap().unwrap();
    submit_compaction(
        &admin,
        manifest
            .l0()
            .iter()
            .map(|sst| compactor::SourceId::SstView(sst.id))
            .chain([
                compactor::SourceId::SortedRun(1),
                compactor::SourceId::SortedRun(0),
            ])
            .collect(),
        0,
    )
    .await;
    assert_eq!(sorted_runs(&admin).await, vec![0]);
    let live = match merge_with_base(
        None,
        std::slice::from_ref(enqueue(&[text_operation(LIVE, 1, Some("live"))]).bytes()),
    )
    .unwrap()
    {
        QueueMergeResult::Value(value) => value,
        QueueMergeResult::Empty => unreachable!("one operation is live"),
    };
    // A reader built now starts from the committed manifest; the writer
    // would see it only after its next manifest poll. Every commit is
    // flushed, so the compacted runs hold the whole history.
    let reader = slatedb::DbReader::builder(PATH, store.clone())
        .with_options(config::DbReaderOptions {
            skip_wal_replay: true,
            ..Default::default()
        })
        .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
        .build()
        .await
        .unwrap();
    let before = MERGES.stats();
    let stored = reader.get(queue_key()).await.unwrap().unwrap();
    let read = MERGES.stats();
    reader.close().await.unwrap();
    assert_eq!(stored, live, "storage keeps exactly the live operation");
    let read_cost = (read.resolved.input_bytes - before.resolved.input_bytes)
        + (read.partial.input_bytes - before.partial.input_bytes);
    assert!(
        read_cost <= live.len() as u64,
        "a fully compacted read costs {read_cost} bytes for {} live bytes",
        live.len()
    );
    db.close().await.unwrap();
}

/// WAL replay re-applies only commits above the flushed L0 frontier, so an
/// acknowledgement replayed above its flushed enqueue meets the only copy of
/// that insert: an upper compaction cancels the pair instead of carrying the
/// removal until the bottom run, and nothing resurrects across the restart.
#[tokio::test]
async fn wal_replay_and_upper_compaction_cancel_acknowledged_enqueues() {
    static MERGES: QueueMerges = QueueMerges::new();
    let store = Arc::new(InMemory::new());
    let open = || {
        Db::builder(PATH, store.clone())
            .with_settings(manual_compaction_settings())
            .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
            .build()
    };
    let base = enqueue(&[text_operation(1, 1, Some("base"))]);
    let live = enqueue(&[text_operation(4, 4, Some("live"))]);

    let db = open().await.unwrap();
    let admin = slatedb::admin::Admin::builder(PATH, store.clone()).build();
    flushed_commit(&db, vec![base.clone()]).await;
    compact_l0(&admin, 0).await;
    flushed_commit(&db, vec![enqueue(&[text_operation(2, 2, Some("flushed"))])]).await;
    compact_l0(&admin, 1).await;
    // Only the WAL holds the acknowledgement of 2 and a pair that cancels.
    commit(&db, vec![acknowledge(&[2])]).await;
    commit(
        &db,
        vec![enqueue(&[text_operation(3, 3, Some("cancelled"))])],
    )
    .await;
    commit(&db, vec![acknowledge(&[3])]).await;
    commit(&db, vec![live.clone()]).await;
    db.close_with_options(
        config::CloseOptions::default().with_flush_type(Some(config::FlushType::Wal)),
    )
    .await
    .unwrap();
    assert_eq!(compacted_ids(&store).await, vec![1, 2]);

    let db = open().await.unwrap();
    assert_eq!(queued_ids(&db).await, vec![1, 4]);
    db.flush_with_options(config::FlushOptions {
        flush_type: config::FlushType::MemTable,
    })
    .await
    .unwrap();
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
    assert_eq!(sorted_runs(&admin).await, vec![0, 1]);
    assert_eq!(compacted_ids(&store).await, vec![1, 4]);
    // The upper run keeps exactly the live insert: a read of the compacted
    // runs folds that one operand and resolves SR0's base against it. A
    // reader built now starts from the committed manifest, whereas the
    // writer may still read the uncompacted runs, whose read folds SR1's
    // enqueue and the removal too before resolving the same input.
    let reader = slatedb::DbReader::builder(PATH, store.clone())
        .with_db_cache_disabled()
        .with_options(config::DbReaderOptions {
            skip_wal_replay: true,
            ..Default::default()
        })
        .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
        .build()
        .await
        .unwrap();
    let before = MERGES.stats();
    assert_eq!(queued_ids(&reader).await, vec![1, 4]);
    let read = MERGES.stats();
    reader.close().await.unwrap();
    assert_eq!(
        (
            read.partial.input_bytes - before.partial.input_bytes,
            read.resolved.input_bytes - before.resolved.input_bytes,
        ),
        (
            live.bytes().len() as u64,
            (base.bytes().len() + live.bytes().len()) as u64
        ),
        "the compacted upper run holds one insert and no removal"
    );
    assert_eq!(queued_ids(&db).await, vec![1, 4]);
    db.close().await.unwrap();
}

/// Object store over a shared in-memory store that can hide every WAL object
/// from its user, so a reader stops replaying the writer's WAL.
#[derive(Debug)]
struct WalBlindStore {
    inner: Arc<InMemory>,
    blind: AtomicBool,
}

impl std::fmt::Display for WalBlindStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("wal-blind-memory")
    }
}

#[async_trait::async_trait]
impl ObjectStore for WalBlindStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        if self.blind.load(Ordering::SeqCst) && location.as_ref().contains("/wal/") {
            return Err(slatedb::object_store::Error::NotFound {
                path: location.to_string(),
                source: "the WAL is hidden from this store".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A reader that replayed an enqueue from the WAL drops that copy once the
/// writer's flush and compaction cancel the enqueue against its
/// acknowledgement.
///
/// Cancellation is exact only while no view holds two copies of one
/// committed operand (see the algebra's cancellation contract). Here the
/// reader holds the enqueue in a replayed memtable and never sees the
/// acknowledgement's WAL, so the acknowledgement reaches it only inside the
/// compacted run where the pair already cancelled. Installing that manifest
/// must drop the replayed copy, which SlateDB does by filtering replayed
/// memtables at the manifest's last L0 sequence; a copy kept beside the run
/// would resurrect the acknowledged operation.
#[tokio::test]
async fn a_replaying_reader_drops_an_enqueue_its_new_manifest_cancelled() {
    let store = Arc::new(InMemory::new());
    let db = Db::builder(PATH, store.clone())
        .with_settings(manual_compaction_settings())
        .with_merge_operator(Arc::new(HelixMergeOperator::new()))
        .build()
        .await
        .unwrap();
    let admin = slatedb::admin::Admin::builder(PATH, store.clone()).build();
    flushed_commit(&db, vec![enqueue(&[text_operation(1, 1, Some("base"))])]).await;
    compact_l0(&admin, 0).await;
    // Only the WAL and the writer's memtable hold the enqueue of 2.
    commit(
        &db,
        vec![enqueue(&[text_operation(2, 2, Some("replayed"))])],
    )
    .await;

    let reader_store = Arc::new(WalBlindStore {
        inner: Arc::clone(&store),
        blind: AtomicBool::new(false),
    });
    // A hidden WAL looks truncated once the manifest's replay boundary
    // passes the last WAL the reader finds: a reader following the latest
    // manifest logs that and keeps polling, where a checkpointing one would
    // stop.
    let reader =
        slatedb::DbReader::builder(PATH, Arc::clone(&reader_store) as Arc<dyn ObjectStore>)
            .with_reader_mode(slatedb::DbReaderMode::FollowLatest)
            .with_db_cache_disabled()
            .with_options(config::DbReaderOptions {
                manifest_poll_interval: Duration::from_millis(10),
                ..Default::default()
            })
            .with_merge_operator(Arc::new(HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
    assert!(reader.manifest().l0().is_empty(), "2 was never flushed");
    assert_eq!(queued_ids(&reader).await, vec![1, 2], "2 is replayed");

    // From here on the reader sees no WAL, so it never replays the
    // acknowledgement.
    reader_store.blind.store(true, Ordering::SeqCst);
    commit(&db, vec![acknowledge(&[2])]).await;
    assert_eq!(queued_ids(&db).await, vec![1]);
    assert_eq!(queued_ids(&reader).await, vec![1, 2]);
    db.flush_with_options(config::FlushOptions {
        flush_type: config::FlushType::MemTable,
    })
    .await
    .unwrap();
    compact_l0(&admin, 1).await;
    assert_eq!(sorted_runs(&admin).await, vec![0, 1]);
    assert_eq!(compacted_ids(&store).await, vec![1], "the pair cancelled");

    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let installed = reader.manifest();
            if installed.l0().is_empty() && installed.compacted().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the reader installs the compacted manifest");
    assert_eq!(
        queued_ids(&reader).await,
        vec![1],
        "the replayed enqueue of 2 is not resurrected"
    );
    reader.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn retained_queues_share_one_budget_and_keep_only_unacknowledged_operations() {
    let db = open(
        Arc::new(InMemory::new()),
        Arc::new(HelixMergeOperator::new()),
    )
    .await;
    commit(
        &db,
        vec![enqueue(&[
            text_operation(1, 1, Some("a")),
            text_operation(2, 2, Some("b")),
        ])],
    )
    .await;
    let target = QueueTarget::new(
        DataScope::Tenant(TenantId::from_u128(0x51)),
        IndexId::new(3).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    );
    assert_eq!(target.key(), queue_key());
    let targets = (2..=5)
        .map(|generation| {
            QueueTarget::new(
                target.scope,
                target.index_id,
                IndexGenerationId::new(generation).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let one = text_operation(2, 2, Some("b")).retained_bytes();
    let both = one + text_operation(1, 1, Some("a")).retained_bytes();
    assert_eq!(both, 2 * one);
    // Room for two whole queues across every target.
    let store = QueueStore::new(QueueLayout::Map, 1 << 20, 2 * both);
    let retained = store.retained();
    let stored = || async { store.read(&db, target).await.unwrap().unwrap() };

    // Nothing remains once every operation read is acknowledged.
    retained.retain(target, stored().await, &[id(1), id(2)]);
    assert!(retained.take(target).is_none());
    assert_eq!(retained.retained_bytes(), 0);
    // An attempt that committed nothing retains the whole queue; one that
    // acknowledged an operation retains the rest.
    retained.retain(target, stored().await, &[]);
    retained.retain(targets[0], stored().await, &[id(1)]);
    assert_eq!(retained.retained_bytes(), both + one);
    // A queue that does not fit beside those held is dropped, and evicts
    // none of them; one that fits exactly is held.
    retained.retain(targets[1], stored().await, &[]);
    assert!(retained.take(targets[1]).is_none());
    retained.retain(targets[2], stored().await, &[id(1)]);
    assert_eq!(retained.retained_bytes(), 2 * both);
    retained.retain(targets[3], stored().await, &[id(2)]);
    assert!(retained.take(targets[3]).is_none());
    assert_eq!(retained.retained_bytes(), 2 * both);

    // A take releases its bytes for the next queue.
    let taken = retained.take(targets[0]).expect("the remainder was held");
    assert_eq!(ids_of(Some(taken.queue())), vec![2]);
    assert_eq!(
        taken.encoded_bytes(),
        0,
        "a retained queue reads no storage"
    );
    assert!(
        retained.take(targets[0]).is_none(),
        "a take removes the queue"
    );
    let whole = retained.take(target).expect("the whole queue was held");
    assert_eq!(ids_of(Some(whole.queue())), vec![1, 2]);
    assert_eq!(retained.retained_bytes(), one);
    retained.retain(targets[1], stored().await, &[]);
    assert_eq!(retained.retained_bytes(), both + one);
    for (target, ids) in [(targets[1], vec![1, 2]), (targets[2], vec![2])] {
        let taken = retained.take(target).expect("the queue was held");
        assert_eq!(ids_of(Some(taken.queue())), ids);
    }
    assert_eq!(retained.retained_bytes(), 0);
    db.close().await.unwrap();
}

#[tokio::test]
#[should_panic(expected = "an attempt retains only the queue it took")]
async fn retaining_a_queue_that_was_not_taken_is_an_invariant_violation() {
    let db = open(
        Arc::new(InMemory::new()),
        Arc::new(HelixMergeOperator::new()),
    )
    .await;
    commit(&db, vec![enqueue(&[text_operation(1, 1, Some("a"))])]).await;
    let target = QueueTarget::new(
        DataScope::Tenant(TenantId::from_u128(0x51)),
        IndexId::new(3).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    );
    let store = QueueStore::new(QueueLayout::Map, 1 << 20, u64::MAX);
    for _ in 0..2 {
        let stored = store.read(&db, target).await.unwrap().unwrap();
        store.retained().retain(target, stored, &[]);
    }
}

#[tokio::test]
async fn latest_reads_of_rows_decode_only_the_operations_they_select() {
    let db = open(
        Arc::new(InMemory::new()),
        Arc::new(HelixMergeOperator::new()),
    )
    .await;
    let target = QueueTarget::new(
        DataScope::Tenant(TenantId::from_u128(0x51)),
        IndexId::new(3).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    );
    let operations = [
        text_operation(1, 1, Some("a")),
        text_operation(2, 2, Some("b")),
        text_operation(3, 1, Some("c")),
    ];
    let one = operations[0].retained_bytes();
    let store = QueueStore::new(QueueLayout::Rows, 1 << 20, 0);
    let row = |sequence| {
        ManagedIndexKey::Data {
            scope: target.scope,
            kind: ScopedKey::IndexOperationRow(IndexOperationRowKey {
                index_id: target.index_id,
                generation: target.generation,
                sequence,
            }),
        }
        .to_bytes()
    };
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    for (sequence, operation) in operations.iter().enumerate() {
        transaction
            .put(
                row(u64::try_from(sequence).unwrap()),
                QueueRow::encode(QueueFamily::Text, operation),
            )
            .unwrap();
    }
    // A fourth row whose entity is intact but whose payload is truncated.
    let corrupt = QueueRow::encode(QueueFamily::Text, &text_operation(4, 4, Some("d")));
    transaction
        .put(row(3), corrupt.slice(..corrupt.len() - 1))
        .unwrap();
    transaction.commit().await.unwrap();

    let latest = |budget| {
        let store = &store;
        let db = &db;
        async move {
            let Some(bytes) = store.read_latest(db, target).await? else {
                return Ok(Vec::new());
            };
            bytes.decode_latest(budget).map(|latest| {
                latest
                    .into_operations()
                    .iter()
                    .map(|operation| operation.id().get())
                    .collect::<Vec<_>>()
            })
        }
    };
    // Entity 1 is selected at its latest operation, 3, or not at all.
    assert_eq!(latest(one - 1).await.unwrap(), Vec::<u128>::new());
    assert_eq!(latest(one).await.unwrap(), vec![3]);
    assert_eq!(latest(2 * one).await.unwrap(), vec![3, 2]);
    assert!(
        latest(u64::MAX).await.is_err(),
        "a read that selects the corrupt row decodes it"
    );
    assert!(store.read(&db, target).await.is_err());
    db.close().await.unwrap();
}

/// Known limitation, pinned so that lifting it shows up here; not a contract.
///
/// A latest read's budget bounds what it decodes, not what SlateDB merges to
/// produce the value. Once the queue is resolved in storage, a read merges
/// nothing; while an operand is pending above it, which is the normal state
/// of an index taking writes, every read resolves the whole queue whatever
/// its budget. Bounding the merge needs a queue layout whose reads can stop
/// at the budget; once one exists, assert that bound here instead.
#[tokio::test]
async fn known_limitation_a_latest_read_merges_the_whole_queue_below_a_pending_operand() {
    // Only this database counts here, so parallel tests cannot move the costs.
    static MERGES: QueueMerges = QueueMerges::new();
    const BACKLOG: u64 = 1_000;
    // Only submitted compactions run, so nothing resolves the pending operand.
    let db = Db::builder(PATH, Arc::new(InMemory::new()))
        .with_settings(manual_compaction_settings())
        .with_merge_operator(Arc::new(HelixMergeOperator::with_queue_merges(&MERGES)))
        .build()
        .await
        .unwrap();
    let target = QueueTarget::new(
        DataScope::Tenant(TenantId::from_u128(0x51)),
        IndexId::new(3).unwrap(),
        IndexGenerationId::new(1).unwrap(),
    );
    let store = QueueStore::new(QueueLayout::Map, 1 << 20, 0);
    let operations = (1..=BACKLOG)
        .map(|entity| text_operation(u128::from(entity), entity, Some("backlog")))
        .collect::<Vec<_>>();
    let QueueMergeResult::Value(resolved) =
        merge_with_base(None, std::slice::from_ref(enqueue(&operations).bytes())).unwrap()
    else {
        unreachable!("the backlog is outstanding");
    };
    // A resolved value in storage: the read merges nothing. The flush makes
    // it a base that no flush folds the later operand into.
    db.put(queue_key(), resolved.clone()).await.unwrap();
    db.flush_with_options(config::FlushOptions {
        flush_type: config::FlushType::MemTable,
    })
    .await
    .unwrap();
    let budget = operations[0].retained_bytes();
    let merged = || {
        let stats = MERGES.stats();
        stats.partial.input_bytes + stats.resolved.input_bytes
    };
    let before = merged();
    let latest = store
        .read_latest(&db, target)
        .await
        .unwrap()
        .unwrap()
        .decode_latest(budget)
        .unwrap();
    assert_eq!(latest.into_operations(), operations[..1]);
    assert_eq!(merged() - before, 0, "a resolved value is read as stored");

    // One pending operand: every read resolves the whole backlog.
    commit(
        &db,
        vec![enqueue(&[text_operation(
            u128::from(BACKLOG) + 1,
            BACKLOG + 1,
            Some("pending"),
        )])],
    )
    .await;
    for _ in 0..2 {
        let before = merged();
        let latest = store
            .read_latest(&db, target)
            .await
            .unwrap()
            .unwrap()
            .decode_latest(budget)
            .unwrap();
        assert_eq!(latest.into_operations(), operations[..1]);
        let read = merged() - before;
        assert!(
            read >= resolved.len() as u64,
            "a {budget}-byte latest read merged {read} bytes of a {}-byte queue",
            resolved.len()
        );
    }
    db.close().await.unwrap();
}
