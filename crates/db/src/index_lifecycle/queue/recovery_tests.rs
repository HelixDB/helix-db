//! Startup backlog loading: one forward pass, one queue in memory at a time.
//!
//! Every test compares the ledger a load builds with the one the previous
//! load built from the same storage, which fully decoded each scope's queues
//! into one list before charging them ([`reference`]): streaming must change
//! memory and seeks only, never charges, members, admissions, or outcomes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use helix_ast::index::IndexSpec;
use helix_ast::query::QueryRequest;
use helix_ast::{batch, traversal};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use slatedb::{Db, DbReadOps, IsolationLevel};

use super::backlog::{BacklogLimits, IndexOperationBacklog};
use super::codec_storage_tests::{compact_l0, manual_compaction_settings};
use super::lifecycle_tests::{config, create, drop_index, vector_spec, wait_terminal};
use super::overlay_tests::{add, delete, update};
use super::publication::PublicationOutcome;
use super::publication_tests::batch_limits;
use super::recovery::{discover_scopes, load_backlog, LoadedQueueSummary};
use super::storage::{discovery_range, QueueStore};
use super::tests::{open, publisher_with_limits, queue, target};
use super::QueueTarget;
use crate::config::{DbConfig, QueueLayout};
use crate::encoding::v2::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
use crate::encoding::v2::keys::{IndexEntity, ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::indexes::operation_queue::{
    OperationQueue, QueueFamily, QueueOperand, QueueRow, QueuedOperation, QueuedOperationId,
    QueuedPayload, QueuedTextPayload, QueuedTextReplacement, QueuedVectorPayload,
    QueuedVectorReplacement,
};
use crate::error::HelixDbError;
use crate::index_lifecycle::work::TextPartition;
use crate::index_lifecycle::worker::IndexWorkerWakeHandle;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, ValidatedDynamicIndexDefinition,
};
use crate::merge_operator::HelixMergeOperator;
use crate::HelixDB;

const PATH: &str = "queue-recovery";

fn ledger() -> Arc<IndexOperationBacklog> {
    IndexOperationBacklog::new(
        BacklogLimits {
            max_retained_bytes: u64::MAX,
            max_members: u64::MAX,
        },
        IndexWorkerWakeHandle::default(),
    )
}

fn tenant(id: u128) -> DataScope {
    DataScope::Tenant(TenantId::from_u128(id))
}

fn queue_target(scope: DataScope, index: u64, generation: u64) -> QueueTarget {
    QueueTarget::new(
        scope,
        IndexId::new(index).unwrap(),
        IndexGenerationId::new(generation).unwrap(),
    )
}

fn node(id: u64) -> IndexEntity {
    IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(id),
    }
}

fn text(entity: u64, body: &str) -> QueuedOperation {
    QueuedOperation::new(
        QueuedOperationId::generate(),
        node(entity),
        QueuedPayload::Text(QueuedTextPayload {
            replacement: Some(QueuedTextReplacement::new(
                TextPartition::Unpartitioned,
                Arc::from(body),
            )),
        }),
    )
}

fn vector(entity: u64, components: &[f32]) -> QueuedOperation {
    QueuedOperation::new(
        QueuedOperationId::generate(),
        node(entity),
        QueuedPayload::Vector(QueuedVectorPayload {
            previous: None,
            replacement: Some(
                QueuedVectorReplacement::try_new(
                    TextPartition::Unpartitioned,
                    Arc::from(components),
                )
                .unwrap(),
            ),
        }),
    )
}

/// The ledger the previous load built from `reader`: each scope's queues
/// fully decoded into one list, then charged in key order.
async fn reference(
    reader: &(impl DbReadOps + Sync),
    layout: QueueLayout,
) -> (Arc<IndexOperationBacklog>, LoadedQueueSummary) {
    let backlog = ledger();
    let mut summary = LoadedQueueSummary::default();
    let own = match layout {
        QueueLayout::Map => RecordKind::IndexOperationQueue,
        QueueLayout::Rows => RecordKind::IndexOperationRow,
    };
    for scope in discover_scopes(reader).await.unwrap() {
        let prefix = ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(own));
        let mut rows = reader.scan_prefix(&prefix, ..).await.unwrap();
        let mut queues: Vec<(QueueTarget, Vec<QueuedOperation>)> = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            let (target, operations) =
                match (layout, ManagedIndexKey::parse_data_from_slice(&row.key)) {
                    (
                        QueueLayout::Map,
                        Ok(ManagedIndexKey::Data {
                            kind: ScopedKey::IndexOperationQueue(key),
                            ..
                        }),
                    ) => (
                        QueueTarget::new(scope, key.index_id, key.generation),
                        OperationQueue::decode(&row.value)
                            .unwrap()
                            .into_operations(),
                    ),
                    (
                        QueueLayout::Rows,
                        Ok(ManagedIndexKey::Data {
                            kind: ScopedKey::IndexOperationRow(key),
                            ..
                        }),
                    ) => (
                        QueueTarget::new(scope, key.index_id, key.generation),
                        vec![QueueRow::decode(&row.value).unwrap().1],
                    ),
                    (_, _) => panic!("the reference reads only its layout's queue keys"),
                };
            match queues.last_mut() {
                Some((last, retained)) if *last == target => retained.extend(operations),
                Some(_) | None => queues.push((target, operations)),
            }
        }
        for (target, operations) in queues {
            summary.queues += 1;
            summary.operations += operations.len() as u64;
            backlog.load_durable(
                target,
                operations.iter().map(|operation| {
                    (
                        operation.id(),
                        operation.entity(),
                        operation.retained_bytes(),
                    )
                }),
            );
        }
    }
    (backlog, summary)
}

/// Asserts that two ledgers hold the same charges, members, admission
/// order, and outcomes.
fn assert_same_ledger(actual: &IndexOperationBacklog, expected: &IndexOperationBacklog) {
    assert_eq!(actual.charges(), expected.charges(), "charges");
    assert_eq!(
        actual.outstanding_admissions(),
        expected.outstanding_admissions(),
        "targets and their admission order"
    );
    assert_eq!(actual.totals(), expected.totals(), "usage and outcomes");
}

/// Loads `reader` into a fresh ledger and asserts it equals the reference.
async fn assert_loads_like_the_reference(
    reader: &(impl DbReadOps + Sync),
    layout: QueueLayout,
) -> (Arc<IndexOperationBacklog>, LoadedQueueSummary) {
    let (expected, expected_summary) = reference(reader, layout).await;
    let backlog = ledger();
    let summary = load_backlog(reader, &QueueStore::new(layout, u64::MAX, 0), &backlog)
        .await
        .unwrap();
    assert_eq!(summary, expected_summary);
    assert_same_ledger(&backlog, &expected);
    (backlog, summary)
}

async fn raw_db(store: Arc<dyn ObjectStore>) -> Db {
    Db::builder(PATH, store)
        .with_settings(manual_compaction_settings())
        .with_merge_operator(Arc::new(HelixMergeOperator::new()))
        .with_db_cache_disabled()
        .build()
        .await
        .unwrap()
}

async fn flush(db: &Db) {
    db.flush_with_options(slatedb::config::FlushOptions {
        flush_type: slatedb::config::FlushType::MemTable,
    })
    .await
    .unwrap();
}

async fn enqueue(
    db: &Db,
    queues: &QueueStore,
    target: QueueTarget,
    operations: &[QueuedOperation],
) {
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    queues
        .stage_enqueue(
            &transaction,
            target,
            QueueOperand::enqueue(operations).unwrap(),
            operations,
        )
        .unwrap();
    transaction.commit().await.unwrap();
}

/// Acknowledges the first `count` operations of `target`'s queue.
async fn acknowledge(db: &Db, queues: &QueueStore, target: QueueTarget, count: usize) {
    let stored = queues.read(db, target).await.unwrap().unwrap();
    let ids = stored
        .operations()
        .take(count)
        .map(QueuedOperation::id)
        .collect::<Vec<_>>();
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    queues
        .stage_acknowledge(&transaction, target, &stored, &ids)
        .unwrap();
    transaction.commit().await.unwrap();
}

/// Writes keys of `scope` that sort just below and just above its queue
/// range, plus graph-like keys before and after its index records, none of
/// which a load may read as a queue.
async fn surround(db: &Db, scope: DataScope) {
    let mut prefix = Vec::new();
    scope.encode_key_prefix(&mut prefix);
    let range = discovery_range(scope);
    let mut below = range.start.to_vec();
    *below.last_mut().unwrap() -= 1;
    below.extend_from_slice(&[0xFF; 4]);
    for key in [
        [prefix.as_slice(), &[0x01, 0x02]].concat(),
        below,
        range.end.to_vec(),
        [range.end.as_ref(), &[0x00]].concat(),
        [prefix.as_slice(), &[0x07, 0x01]].concat(),
    ] {
        db.put(key, b"not a queue").await.unwrap();
    }
}

/// Every key and value in `db`.
async fn snapshot(db: &Db) -> BTreeMap<Bytes, Bytes> {
    let mut rows = db.scan(..).await.unwrap();
    let mut all = BTreeMap::new();
    while let Some(row) = rows.next().await.unwrap() {
        all.insert(row.key, row.value);
    }
    all
}

/// Scopes in key order with a queue: tenants 7 and 8 are adjacent, and the
/// last tenant ID has no successor to seek to.
fn queued_scopes() -> [DataScope; 5] {
    [
        DataScope::LegacyUnscoped,
        tenant(0),
        tenant(7),
        tenant(8),
        tenant(u128::MAX),
    ]
}

/// Map-layout queues across scopes, generations, families, and storage
/// tiers (a compacted run, L0, and the memtable), with partial and full
/// acknowledgements, between keys a load must skip.
async fn map_fixture(db: &Db, admin: &slatedb::admin::Admin) -> BTreeSet<QueueTarget> {
    let queues = QueueStore::new(QueueLayout::Map, u64::MAX, 0);
    let mut expected = BTreeSet::new();
    for scope in queued_scopes().into_iter().chain((100..140).map(tenant)) {
        surround(db, scope).await;
    }
    for (ordinal, scope) in queued_scopes().into_iter().enumerate() {
        let base = ordinal as u64 * 100;
        // Compacted: a text queue and two generations of one vector index.
        enqueue(
            db,
            &queues,
            queue_target(scope, 1, 1),
            &[text(base, "a"), text(base + 1, "b")],
        )
        .await;
        enqueue(
            db,
            &queues,
            queue_target(scope, 2, 1),
            &[vector(base, &[1.0, 2.0])],
        )
        .await;
        enqueue(
            db,
            &queues,
            queue_target(scope, 2, 2),
            &[vector(base + 1, &[3.0, 4.0])],
        )
        .await;
    }
    flush(db).await;
    compact_l0(admin, 0).await;
    for (ordinal, scope) in queued_scopes().into_iter().enumerate() {
        let base = ordinal as u64 * 100;
        // L0: more work above the compacted base, and a queue that drains.
        enqueue(
            db,
            &queues,
            queue_target(scope, 1, 1),
            &[text(base, "a2"), text(base + 2, "c")],
        )
        .await;
        enqueue(
            db,
            &queues,
            queue_target(scope, 3, 1),
            &[text(base, "drained")],
        )
        .await;
    }
    flush(db).await;
    for (ordinal, scope) in queued_scopes().into_iter().enumerate() {
        let base = ordinal as u64 * 100;
        // Memtable: partial and full acknowledgements, and fresh work.
        acknowledge(db, &queues, queue_target(scope, 1, 1), 3).await;
        acknowledge(db, &queues, queue_target(scope, 3, 1), 1).await;
        enqueue(
            db,
            &queues,
            queue_target(scope, 2, 2),
            &[vector(base + 1, &[5.0, 6.0])],
        )
        .await;
        expected.extend([
            queue_target(scope, 1, 1),
            queue_target(scope, 2, 1),
            queue_target(scope, 2, 2),
        ]);
    }
    expected
}

#[test]
fn a_discovery_range_holds_exactly_one_scopes_queue_keys_of_both_layouts() {
    let queue_key = |scope, index, generation| queue_target(scope, index, generation).key();
    let row_key = |scope, index, generation, sequence| {
        ManagedIndexKey::Data {
            scope,
            kind: ScopedKey::IndexOperationRow(crate::encoding::v2::keys::IndexOperationRowKey {
                index_id: IndexId::new(index).unwrap(),
                generation: IndexGenerationId::new(generation).unwrap(),
                sequence,
            }),
        }
        .to_bytes()
    };
    let record_key = |scope: DataScope, kind: RecordKind| {
        ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(kind))
    };
    let scopes = [
        DataScope::LegacyUnscoped,
        tenant(0),
        tenant(1),
        tenant(u128::MAX - 1),
        tenant(u128::MAX),
    ];
    for scope in scopes {
        let range = discovery_range(scope);
        for key in [
            queue_key(scope, 1, 1),
            queue_key(scope, u64::MAX, u64::MAX),
            row_key(scope, 1, 1, 0),
            row_key(scope, u64::MAX, u64::MAX, u64::MAX),
        ] {
            assert!(range.contains(&key), "{scope:?} holds {key:?}");
        }
        for kind in [RecordKind::IndexRecord, RecordKind::SecondaryEqualityBitmap] {
            assert!(
                !range.contains(&record_key(scope, kind)),
                "{scope:?} {kind:?}"
            );
        }
        for other in scopes.into_iter().filter(|other| *other != scope) {
            assert!(
                !range.contains(&queue_key(other, 1, 1)),
                "{scope:?} {other:?}"
            );
            assert!(
                !range.contains(&row_key(other, u64::MAX, u64::MAX, u64::MAX)),
                "{scope:?} {other:?}"
            );
        }
    }
}

#[test]
fn discovery_completes_map_queues_per_row_and_row_queues_per_generation() {
    let first = queue_target(tenant(3), 1, 1);
    let second = queue_target(tenant(3), 2, 1);
    let operations = [text(1, "a"), text(2, "b")];
    let frames = |operations: &[QueuedOperation]| {
        operations
            .iter()
            .map(|operation| {
                crate::encoding::v2::values::indexes::operation_queue::OperationFrame {
                    id: operation.id(),
                    entity: operation.entity(),
                    retained_bytes: operation.retained_bytes(),
                }
            })
            .collect::<Vec<_>>()
    };

    // Map layout: every row is a whole queue.
    let map = QueueStore::new(QueueLayout::Map, u64::MAX, 0);
    let mut discovery = map.discovery();
    for target in [first, second] {
        let value = QueueOperand::enqueue(&operations).unwrap();
        let queue = discovery
            .push(&target.key(), value.bytes())
            .unwrap()
            .expect("a map row is a whole queue");
        assert_eq!(
            (queue.target, queue.family, queue.frames),
            (target, QueueFamily::Text, frames(&operations[..]))
        );
    }
    assert_eq!(discovery.finish(), None);

    // Row layout: a generation completes when the next one's first row arrives.
    let rows = QueueStore::new(QueueLayout::Rows, u64::MAX, 0);
    let mut discovery = rows.discovery();
    let row = |target: QueueTarget, sequence| {
        ManagedIndexKey::Data {
            scope: target.scope,
            kind: ScopedKey::IndexOperationRow(crate::encoding::v2::keys::IndexOperationRowKey {
                index_id: target.index_id,
                generation: target.generation,
                sequence,
            }),
        }
        .to_bytes()
    };
    let encoded = |operation| QueueRow::encode(QueueFamily::Text, operation);
    assert_eq!(
        discovery
            .push(&row(first, 0), &encoded(&operations[0]))
            .unwrap(),
        None
    );
    assert_eq!(
        discovery
            .push(&row(first, 1), &encoded(&operations[1]))
            .unwrap(),
        None
    );
    let completed = discovery
        .push(&row(second, 2), &encoded(&operations[0]))
        .unwrap()
        .expect("the next generation completes the previous one");
    assert_eq!(
        (completed.target, completed.frames),
        (first, frames(&operations[..]))
    );
    let last = discovery
        .finish()
        .expect("finish completes the last generation");
    assert_eq!(
        (last.target, last.frames),
        (second, frames(&operations[..1]))
    );

    // One generation's rows must share a family.
    let mut discovery = rows.discovery();
    discovery
        .push(&row(first, 0), &encoded(&operations[0]))
        .unwrap();
    let vector_row = QueueRow::encode(QueueFamily::Vector, &vector(1, &[1.0]));
    assert!(matches!(
        discovery.push(&row(first, 1), &vector_row),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}

#[tokio::test]
async fn one_pass_charges_exactly_what_full_decodes_charged_across_scopes_and_tiers() {
    let store = Arc::new(InMemory::new());
    let db = raw_db(store.clone()).await;
    let admin = slatedb::admin::Admin::builder(PATH, store.clone()).build();
    let expected = map_fixture(&db, &admin).await;

    let (backlog, summary) = assert_loads_like_the_reference(&db, QueueLayout::Map).await;
    assert_eq!(
        backlog
            .outstanding_targets()
            .into_iter()
            .collect::<BTreeSet<_>>(),
        expected,
        "every queue with work and no drained one"
    );
    // Per scope: text queue 1 keeps 1 of 4, vector generations 1 and 2 keep
    // 1 and 2.
    assert_eq!(
        summary,
        LoadedQueueSummary {
            queues: 3 * 5,
            operations: 4 * 5,
        }
    );
    assert_eq!(backlog.totals().outcomes.discovered, 4 * 5);
    db.close().await.unwrap();

    // A writer reopened over the same storage, whatever replayed, loads the
    // same ledger again.
    let db = raw_db(store).await;
    let (reopened, _) = assert_loads_like_the_reference(&db, QueueLayout::Map).await;
    assert_same_ledger(&reopened, &backlog);
    db.close().await.unwrap();
}

#[tokio::test]
async fn the_row_layout_groups_each_generation_and_resumes_its_sequence() {
    let store = Arc::new(InMemory::new());
    let db = raw_db(store.clone()).await;
    let writer = QueueStore::new(QueueLayout::Rows, u64::MAX, 0);
    let scopes = [DataScope::LegacyUnscoped, tenant(5), tenant(6)];
    for scope in scopes.into_iter().chain([tenant(4), tenant(9)]) {
        surround(&db, scope).await;
    }
    // Interleaved commits: one generation's rows are contiguous by key only.
    for round in 0..3_u64 {
        for scope in scopes {
            enqueue(&db, &writer, queue_target(scope, 1, 1), &[text(round, "t")]).await;
            enqueue(&db, &writer, queue_target(scope, 1, 2), &[text(round, "u")]).await;
            enqueue(
                &db,
                &writer,
                queue_target(scope, 2, 1),
                &[vector(round, &[1.0])],
            )
            .await;
        }
        flush(&db).await;
    }
    acknowledge(&db, &writer, queue_target(tenant(5), 1, 1), 2).await;
    acknowledge(&db, &writer, queue_target(tenant(6), 2, 1), 3).await;

    let (expected, expected_summary) = reference(&db, QueueLayout::Rows).await;
    let loaded = QueueStore::new(QueueLayout::Rows, u64::MAX, 0);
    let backlog = ledger();
    let summary = load_backlog(&db, &loaded, &backlog).await.unwrap();
    assert_eq!(summary, expected_summary);
    assert_eq!(
        summary,
        LoadedQueueSummary {
            queues: 3 * 3 - 1,
            operations: 3 * 3 * 3 - 2 - 3,
        }
    );
    assert_same_ledger(&backlog, &expected);

    // The loaded store allocates past every retained row's sequence.
    let sequences = |all: BTreeMap<Bytes, Bytes>| {
        all.into_keys()
            .filter_map(|key| match ManagedIndexKey::parse_data_from_slice(&key) {
                Ok(ManagedIndexKey::Data {
                    kind: ScopedKey::IndexOperationRow(row),
                    ..
                }) => Some(row.sequence),
                Ok(_) | Err(_) => None,
            })
            .collect::<BTreeSet<_>>()
    };
    let before = sequences(snapshot(&db).await);
    enqueue(
        &db,
        &loaded,
        queue_target(tenant(5), 1, 1),
        &[text(9, "after")],
    )
    .await;
    let after = sequences(snapshot(&db).await);
    let added = after.difference(&before).copied().collect::<Vec<_>>();
    assert_eq!(added.len(), 1);
    assert!(added[0] > *before.last().unwrap());
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_queue_of_the_other_layout_fails_closed_in_every_scope() {
    for (writer, reader) in [
        (QueueLayout::Rows, QueueLayout::Map),
        (QueueLayout::Map, QueueLayout::Rows),
    ] {
        for scope in [DataScope::LegacyUnscoped, tenant(3), tenant(u128::MAX)] {
            let db = raw_db(Arc::new(InMemory::new())).await;
            let own = QueueStore::new(reader, u64::MAX, 0);
            // This layout's queues load in every other scope first.
            for other in [DataScope::LegacyUnscoped, tenant(2), tenant(4)] {
                if other != scope {
                    enqueue(&db, &own, queue_target(other, 1, 1), &[text(1, "own")]).await;
                }
            }
            let foreign = QueueStore::new(writer, u64::MAX, 0);
            enqueue(
                &db,
                &foreign,
                queue_target(scope, 1, 1),
                &[text(1, "foreign")],
            )
            .await;
            let Err(HelixDbError::Config(message)) =
                load_backlog(&db, &QueueStore::new(reader, u64::MAX, 0), &ledger()).await
            else {
                panic!("{reader:?} must refuse {writer:?} queues in {scope:?}");
            };
            assert!(
                message.contains(&format!("layout other than {reader:?}")),
                "{message}"
            );
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn corrupt_keys_values_and_envelopes_fail_closed() {
    let queues = QueueStore::new(QueueLayout::Map, u64::MAX, 0);
    let scope = tenant(0x51);
    let target = queue_target(scope, 1, 1);
    // A corrupt payload behind valid framing: a stored value no merge
    // validated, as after bit rot in a resolved value.
    let mut nan = vec![0x01, 0x01, 0x00, 0x01, 0x01, 0x01];
    nan.extend_from_slice(&f32::NAN.to_bits().to_be_bytes());
    let mut value = vec![0x01, 0x14, 0x01, 0x00, 0x01, 0x01];
    value.extend_from_slice(&1_u128.to_be_bytes());
    value.push(u8::try_from(nan.len()).unwrap());
    value.extend_from_slice(&nan);
    // The same operation ID twice in one value.
    let operation = text(1, "twice");
    let once = QueueOperand::enqueue(std::slice::from_ref(&operation)).unwrap();
    let record = &once.bytes()[5..];
    let duplicate = [&[0x01_u8, 0x14, 0x02, 0x00, 0x02][..], record, record].concat();
    let mut truncated = target.key().to_vec();
    truncated.pop();
    for (name, key, value, expected) in [
        ("payload", target.key().to_vec(), value, "not finite"),
        ("duplicate", target.key().to_vec(), duplicate, "twice"),
        (
            "foreign key",
            truncated,
            once.bytes().to_vec(),
            "operation queue prefix holds another key",
        ),
        (
            "envelope",
            vec![TENANT_KEY_PREFIX, 0x01],
            b"x".to_vec(),
            "tenant discovery encountered an invalid envelope",
        ),
        // Malformed envelopes sorting before tenant zero's queue range: the
        // lone marker, and one byte short of tenant zero's envelope.
        (
            "lone envelope marker",
            vec![TENANT_KEY_PREFIX],
            b"x".to_vec(),
            "tenant discovery encountered an invalid envelope",
        ),
        (
            "short tenant zero envelope",
            [vec![TENANT_KEY_PREFIX], vec![0x00; 15]].concat(),
            b"x".to_vec(),
            "tenant discovery encountered an invalid envelope",
        ),
    ] {
        let db = raw_db(Arc::new(InMemory::new())).await;
        enqueue(
            &db,
            &queues,
            queue_target(DataScope::LegacyUnscoped, 1, 1),
            &[text(1, "ok")],
        )
        .await;
        db.put(&key, &value).await.unwrap();
        let error = load_backlog(&db, &queues, &ledger()).await.expect_err(name);
        assert!(error.to_string().contains(expected), "{name}: {error}");
        db.close().await.unwrap();
    }
}

/// Object store that fails the `remaining`-th SST read once armed.
#[derive(Debug)]
struct FailingSstReads {
    inner: Arc<InMemory>,
    armed: AtomicBool,
    remaining: AtomicUsize,
}

impl std::fmt::Display for FailingSstReads {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("failing-sst-reads")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingSstReads {
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
        let sst = location.as_ref().ends_with(".sst") && !location.as_ref().contains("/wal/");
        if sst
            && self.armed.load(Ordering::SeqCst)
            && self.remaining.fetch_sub(1, Ordering::SeqCst) == 0
        {
            self.armed.store(false, Ordering::SeqCst);
            return Err(slatedb::object_store::Error::NotImplemented {
                operation: "an injected crash".to_string(),
                implementer: self.to_string(),
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

/// A load is read-only, so a writer that dies at any storage read of it
/// reopens to the same ledger and storage.
#[tokio::test]
async fn a_load_interrupted_at_any_read_reloads_the_same_ledger() {
    let memory = Arc::new(InMemory::new());
    let db = raw_db(memory.clone()).await;
    let admin = slatedb::admin::Admin::builder(PATH, memory.clone()).build();
    map_fixture(&db, &admin).await;
    flush(&db).await;
    let (expected, _) = reference(&db, QueueLayout::Map).await;
    let stored = snapshot(&db).await;
    db.close().await.unwrap();

    let store = Arc::new(FailingSstReads {
        inner: memory,
        armed: AtomicBool::new(false),
        remaining: AtomicUsize::new(0),
    });
    let mut interrupted = 0;
    for read in 0.. {
        let db = raw_db(store.clone()).await;
        store.remaining.store(read, Ordering::SeqCst);
        store.armed.store(true, Ordering::SeqCst);
        let partial = ledger();
        let result = load_backlog(
            &db,
            &QueueStore::new(QueueLayout::Map, u64::MAX, 0),
            &partial,
        )
        .await;
        let crashed = !store.armed.swap(false, Ordering::SeqCst);
        // The writer dies with whatever it charged, however its storage
        // handle closes; the next one starts over.
        drop(partial);
        drop(db.close().await);
        let db = raw_db(store.clone()).await;
        let backlog = ledger();
        load_backlog(
            &db,
            &QueueStore::new(QueueLayout::Map, u64::MAX, 0),
            &backlog,
        )
        .await
        .unwrap();
        assert_same_ledger(&backlog, &expected);
        assert_eq!(snapshot(&db).await, stored, "a load writes nothing");
        db.close().await.unwrap();
        if !crashed {
            result.expect("an uninterrupted load succeeds");
            break;
        }
        assert!(result.is_err(), "read {read} was injected");
        interrupted += 1;
    }
    assert!(interrupted > 5, "the load read storage {interrupted} times");
}

/// Opens a scoped index through the query boundary and waits for its build.
async fn create_scoped(db: &HelixDB, scope: DataScope, spec: IndexSpec) {
    let receipt = db
        .query_scoped(
            QueryRequest::write(
                batch::write_batch()
                    .var_as("created", traversal::g().create_index_if_not_exists(spec))
                    .returning(["created"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let operation = receipt["created"]["operation_id"]
        .as_str()
        .or_else(|| receipt["created"][0]["operation_id"].as_str())
        .unwrap_or_else(|| panic!("create accepted a build: {receipt}"))
        .to_string();
    for _ in 0..12_000 {
        let status = db
            .query_scoped(
                QueryRequest::read(
                    batch::read_batch()
                        .var_as(
                            "status",
                            traversal::g().get_index_operation(operation.as_str()),
                        )
                        .returning(["status"]),
                ),
                scope,
            )
            .await
            .unwrap();
        match status["status"]["status"].as_str() {
            Some("succeeded") => return,
            Some("queued" | "running") => {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await
            }
            other => panic!("scoped build did not succeed: {other:?} {status}"),
        }
    }
    panic!("scoped build stalled");
}

async fn scoped_add(db: &HelixDB, scope: DataScope, embedding: [f32; 2]) {
    db.query_scoped(
        QueryRequest::write(batch::write_batch().var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![(
                    "embedding",
                    helix_ast::value::PropertyInput::from(embedding.to_vec()),
                )],
            ),
        )),
        scope,
    )
    .await
    .unwrap();
}

/// A reopened writer charges exactly what the previous load charged from the
/// same storage, and exactly what the closed writer held: partially
/// published queues, a dropped index's queue, and tenant queues with and
/// without a canonical record.
#[tokio::test]
async fn a_reopened_writer_reloads_its_ledger_exactly() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open("recovery-writer", Arc::clone(&store), config()).await;
    let text_spec = IndexSpec::node_text("Doc", "body", None::<&str>);
    for spec in [vector_spec(), text_spec.clone()] {
        let operation = create(&db, spec).await;
        assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    }
    let vector_target = target(&db, QueueFamily::Vector).await;
    let text_target = target(&db, QueueFamily::Text).await;
    let mut ids = Vec::new();
    for index in 0..6_u8 {
        let tenant_value = if index % 2 == 0 { "a" } else { "b" };
        ids.push(
            add(
                &db,
                [f32::from(index), 1.0],
                &format!("doc {index}"),
                Some(tenant_value),
            )
            .await,
        );
    }
    update(&db, ids[1], [9.0, 9.0], "rewritten").await;
    delete(&db, ids[2]).await;
    // Publish one vector operation, so its queue reloads partially acknowledged.
    let first = queue(&db, QueueFamily::Vector).await.unwrap().operations()[0].retained_bytes();
    let narrow = publisher_with_limits(
        &db,
        batch_limits(first, 32_768),
        DbConfig::new()
            .search_index_backfill()
            .active_text_mutation(),
    );
    let outcome = narrow.publish_once(vector_target).await.unwrap();
    assert!(
        matches!(outcome, PublicationOutcome::Published { operations: 1, .. }),
        "{outcome:?}"
    );
    // Dropping the text index leaves its queued work to discard.
    let dropped = drop_index(&db, text_spec).await.unwrap();
    assert_eq!(wait_terminal(&db, &dropped).await, "succeeded");
    // A tenant index with a canonical record, and a tenant queue without one.
    let scoped = tenant(0x51);
    create_scoped(
        &db,
        scoped,
        IndexSpec::node_vector(
            "Doc",
            "embedding",
            std::num::NonZeroUsize::new(2).unwrap(),
            helix_ast::index::VectorDistanceMetric::Euclidean,
            None::<&str>,
        ),
    )
    .await;
    for index in 0..3_u8 {
        scoped_add(&db, scoped, [f32::from(index), 2.0]).await;
    }
    let orphan = queue_target(tenant(0x52), 999, 1);
    enqueue(
        &db.inner_db(),
        &QueueStore::new(QueueLayout::Map, u64::MAX, 0),
        orphan,
        &[vector(1, &[1.0, 1.0]), vector(2, &[2.0, 2.0])],
    )
    .await;

    let backlog = db.index_operation_backlog();
    let targets = backlog.outstanding_targets();
    assert!(targets.contains(&vector_target) && targets.contains(&text_target));
    assert!(targets.iter().any(|target| target.scope == scoped));
    let usage = targets
        .iter()
        .filter(|target| **target != orphan)
        .map(|target| (*target, backlog.usage(target.scope, target.index_id)))
        .collect::<Vec<_>>();
    let pending = db.index_operation_queue_stats().pending_operations;
    db.close().await.unwrap();

    let db = open("recovery-writer", Arc::clone(&store), config()).await;
    let (expected, _) = reference(db.inner_db().as_ref(), QueueLayout::Map).await;
    let reloaded = db.index_operation_backlog();
    assert_same_ledger(reloaded, &expected);
    let mut with_orphan = targets.clone();
    with_orphan.push(orphan);
    with_orphan.sort_unstable();
    with_orphan.dedup();
    assert_eq!(reloaded.outstanding_targets(), with_orphan);
    for (target, before) in usage {
        assert_eq!(
            reloaded.usage(target.scope, target.index_id),
            before,
            "{target:?} reloads the usage it held"
        );
    }
    let stats = db.index_operation_queue_stats();
    assert_eq!(stats.pending_operations, pending + 2);
    assert_eq!(stats.discovered_operations, stats.pending_operations);
    // The load only charges: publication starts with no queue read,
    // retained, scheduled, or held back.
    let publisher = db.index_queue_publisher().unwrap();
    assert_eq!(stats.queue_reads, 0);
    assert_eq!(db.index_queue_store().retained().retained_bytes(), 0);
    assert_eq!(publisher.scheduled_targets(), (0, 0));
    assert_eq!(publisher.blocked_entity_count(), 0);

    // Everything reloaded drains: live queues publish, the dropped index's
    // and the orphan's are discarded. Each queue is read once and drains
    // from what its commits retained, then is read again only to find it
    // empty, and no drained target keeps a schedule.
    let drained = reloaded.outstanding_targets();
    for target in drained.iter().copied() {
        for _ in 0..100 {
            match publisher.publish_once(target).await.unwrap() {
                PublicationOutcome::Empty => break,
                PublicationOutcome::Published { .. }
                | PublicationOutcome::Discarded { .. }
                | PublicationOutcome::Trimmed => {}
                outcome @ (PublicationOutcome::Deferred
                | PublicationOutcome::Retry
                | PublicationOutcome::Blocked
                | PublicationOutcome::Stalled) => panic!("{target:?} stalled: {outcome:?}"),
            }
        }
    }
    assert!(reloaded.outstanding_targets().is_empty());
    let stats = db.index_operation_queue_stats();
    assert_eq!(stats.pending_operations, 0);
    assert_eq!(stats.queue_reads, 2 * drained.len() as u64);
    assert_eq!(db.index_queue_store().retained().retained_bytes(), 0);
    assert_eq!(publisher.scheduled_targets().0, 0);
    db.close().await.unwrap();
}

const OWNERS: &str = "recovery-owners";

/// The key of the canonical record in `scope` whose definition `owner`
/// accepts, and the queue target it names.
async fn scoped_owner(
    db: &HelixDB,
    scope: DataScope,
    owner: fn(&ValidatedDynamicIndexDefinition) -> bool,
) -> (Bytes, QueueTarget) {
    let prefix =
        ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
    let storage = db.inner_db();
    let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let record = decode_index_record(&row.value).unwrap();
        if owner(record.definition()) {
            return (
                row.key,
                QueueTarget::new(scope, record.index_id(), record.state().generation()),
            );
        }
    }
    panic!("{scope:?} holds the owner");
}

/// A closed writer whose load reads three scopes' index records in order:
/// the legacy scope (an owned vector queue), tenant 0x41 (an orphan queue
/// after its own indexes' keys), and tenant 0x42 (no queue). Both tenants
/// hold a vector and a secondary index with no queue of their own.
async fn owner_fixture() -> InMemory {
    let fixture = InMemory::new();
    let db = open(OWNERS, Arc::new(fixture.clone()), config()).await;
    let operation = create(&db, vector_spec()).await;
    assert_eq!(wait_terminal(&db, &operation).await, "succeeded");
    add(&db, [1.0, 1.0], "legacy", Some("a")).await;
    for scope in [tenant(0x41), tenant(0x42)] {
        create_scoped(
            &db,
            scope,
            IndexSpec::node_vector(
                "Doc",
                "embedding",
                std::num::NonZeroUsize::new(2).unwrap(),
                helix_ast::index::VectorDistanceMetric::Euclidean,
                None::<&str>,
            ),
        )
        .await;
        create_scoped(&db, scope, IndexSpec::node_equality("Doc", "status")).await;
    }
    enqueue(
        &db.inner_db(),
        &QueueStore::new(QueueLayout::Map, u64::MAX, 0),
        queue_target(tenant(0x41), 999, 1),
        &[vector(1, &[1.0, 1.0])],
    )
    .await;
    db.close().await.unwrap();
    fixture
}

fn is_vector(definition: &ValidatedDynamicIndexDefinition) -> bool {
    matches!(definition, ValidatedDynamicIndexDefinition::Vector(_))
}

fn is_secondary(definition: &ValidatedDynamicIndexDefinition) -> bool {
    matches!(definition, ValidatedDynamicIndexDefinition::Secondary(_))
}

/// Each queue is validated against its own scope's records, whichever scope
/// yielded the queue before it. Index IDs are global, so checking a tenant
/// queue against the legacy scope's records, or another tenant's, misses
/// its index and skips validation: a tenant queue owned by a secondary
/// index, or holding another family or element kind than its owner, fails
/// the open closed after either.
#[tokio::test]
async fn a_misowned_tenant_queue_fails_the_open_after_other_scopes_queues() {
    let fixture = owner_fixture().await;
    let control = open(OWNERS, Arc::new(fixture.fork()), config()).await;
    let loaded = control.index_operation_backlog().outstanding_targets();
    assert!(loaded.contains(&queue_target(tenant(0x41), 999, 1)));
    assert!(loaded
        .iter()
        .any(|target| target.scope == DataScope::LegacyUnscoped));
    control.close().await.unwrap();

    let edge = QueuedOperation::new(
        QueuedOperationId::generate(),
        IndexEntity {
            kind: IndexElementKind::Edge,
            id: IndexEntityId::new(1),
        },
        vector(1, &[1.0, 1.0]).payload().clone(),
    );
    // Tenant 0x41's queue sorts before its orphan, so it follows the legacy
    // scope's records; tenant 0x42's follows tenant 0x41's.
    for scope in [tenant(0x41), tenant(0x42)] {
        for (owner, operation, expected) in [
            (
                is_secondary as fn(&ValidatedDynamicIndexDefinition) -> bool,
                vector(1, &[1.0, 1.0]),
                "owns an operation queue",
            ),
            (
                is_vector,
                text(1, "another family"),
                "does not match its canonical definition",
            ),
            (
                is_vector,
                edge.clone(),
                "does not match its canonical definition",
            ),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(fixture.fork());
            let db = open(OWNERS, Arc::clone(&store), config()).await;
            let (_, target) = scoped_owner(&db, scope, owner).await;
            assert!(
                target.index_id.get() < 999,
                "{target:?} sorts before the orphan"
            );
            enqueue(
                &db.inner_db(),
                &QueueStore::new(QueueLayout::Map, u64::MAX, 0),
                target,
                &[operation],
            )
            .await;
            db.close().await.unwrap();
            let Err(error) =
                HelixDB::open_with_object_store_and_config(OWNERS, store, config()).await
            else {
                panic!("{scope:?}: the writer opened over a misowned queue ({expected})");
            };
            assert!(
                matches!(&error, HelixDbError::IndexCatalogCorruption(message) if message.contains(expected)),
                "{scope:?}: {error}"
            );
        }
    }
}

/// A scope's index records are read at open only once it yields a queue:
/// a corrupt record fails the open in a tenant with a queue, while in a
/// tenant without one it fails only that tenant's own requests, never the
/// open or other scopes.
#[tokio::test]
async fn a_corrupt_record_fails_the_open_only_in_a_scope_with_a_queue() {
    let fixture = owner_fixture().await;
    for (scope, has_queue) in [(tenant(0x41), true), (tenant(0x42), false)] {
        let store: Arc<dyn ObjectStore> = Arc::new(fixture.fork());
        let db = open(OWNERS, Arc::clone(&store), config()).await;
        let (key, _) = scoped_owner(&db, scope, is_secondary).await;
        db.inner_db().put(key, b"not a record").await.unwrap();
        db.close().await.unwrap();
        match (
            HelixDB::open_with_object_store_and_config(OWNERS, store, config()).await,
            has_queue,
        ) {
            (Err(error), true) => {
                assert!(matches!(error, HelixDbError::Encoding(_)), "{error}");
            }
            (Ok(db), false) => {
                let write = |scope| {
                    db.query_scoped(
                        QueryRequest::write(batch::write_batch().var_as(
                            "created",
                            traversal::g().add_n(
                                "Doc",
                                vec![(
                                    "status",
                                    helix_ast::value::PropertyInput::from("new".to_string()),
                                )],
                            ),
                        )),
                        scope,
                    )
                };
                let error = write(scope).await.unwrap_err();
                assert!(matches!(error, HelixDbError::Encoding(_)), "{error}");
                write(tenant(0x41)).await.unwrap();
                write(DataScope::LegacyUnscoped).await.unwrap();
                db.close().await.unwrap();
            }
            (Ok(_), true) => panic!("{scope:?}: the writer opened over a corrupt owner"),
            (Err(error), false) => panic!("{scope:?}: a queueless scope failed the open: {error}"),
        }
    }
}
