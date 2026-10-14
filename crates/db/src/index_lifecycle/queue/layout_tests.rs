//! The row-per-operation baseline layout against the merge-backed map layout.
//!
//! The layout selector must change storage (rows versus one merge value per
//! generation) while producers, the publisher, recovery, and search overlays
//! behave identically.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use helix_ast::query::SearchConsistency;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::backlog::{BacklogLimits, IndexOperationBacklog};
use super::overlay_tests::{add, delete, drain, text_search, update, vector_search};
use super::recovery::load_backlog;
use super::storage::QueueStore;
use super::tests::{install_vector_and_text, open, queued, target};
use super::QueueTarget;
use crate::config::{DbConfig, IndexOperationQueueTuning, QueueLayout};
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{
    IndexEntity, IndexOperationRowKey, ManagedIndexKey, RecordKind, ScopedKey,
};
use crate::encoding::v2::values::indexes::operation_queue::{
    QueueFamily, QueueOperand, QueueRow, QueuedOperation, QueuedOperationId, QueuedPayload,
    QueuedTextPayload, QueuedVectorPayload,
};
use crate::error::HelixDbError;
use crate::index_lifecycle::worker::IndexWorkerWakeHandle;
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};
use crate::HelixDB;

fn tuning(layout: QueueLayout) -> IndexOperationQueueTuning {
    IndexOperationQueueTuning::default().with_layout(layout)
}

/// Row keys stored for one generation.
async fn row_keys(db: &HelixDB, target: QueueTarget) -> Vec<Bytes> {
    let prefix = ManagedIndexKey::data_prefix(
        target.scope,
        ScopedKey::generation_prefix(
            RecordKind::IndexOperationRow,
            target.index_id,
            target.generation,
        ),
    );
    let storage = db.inner_db();
    let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
    let mut keys = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        keys.push(row.key);
    }
    keys
}

/// One workload of inserts, updates, a delete, and repeated hot updates.
async fn workload(db: &HelixDB) -> Vec<u64> {
    let mut ids = Vec::new();
    for (index, body) in ["rust storage", "graph engine", "rust graph", "search"]
        .into_iter()
        .enumerate()
    {
        let offset = index as f32;
        ids.push(add(db, [offset, 1.0 - offset], body, None).await);
    }
    update(db, ids[1], [5.0, 5.0], "rewritten graph").await;
    delete(db, ids[3]).await;
    for revision in 0..3_u8 {
        update(
            db,
            ids[0],
            [f32::from(revision), 2.0],
            &format!("hot rust {revision}"),
        )
        .await;
    }
    ids
}

/// Every hit of the fixed queries, keyed by workload ordinal.
async fn results(
    db: &HelixDB,
    ids: &[u64],
    consistency: SearchConsistency,
) -> BTreeSet<(String, usize, u64)> {
    let ordinal = |id: u64| ids.iter().position(|candidate| *candidate == id).unwrap();
    let mut found = BTreeSet::new();
    for query in [[0.0_f32, 0.0], [5.0, 5.0], [2.0, 2.0]] {
        for (id, score) in vector_search(db, query, 10, None, consistency).await {
            found.insert((format!("vector {query:?}"), ordinal(id), score));
        }
    }
    for term in ["rust", "graph", "hot", "search"] {
        for (id, score) in text_search(db, term, 10, None, consistency).await {
            found.insert((format!("text {term}"), ordinal(id), score));
        }
    }
    found
}

#[tokio::test]
async fn layouts_store_differently_and_search_and_publish_identically() {
    let map = open(
        "layout-map",
        Arc::new(InMemory::new()),
        queued(tuning(QueueLayout::Map)),
    )
    .await;
    let rows = open(
        "layout-rows",
        Arc::new(InMemory::new()),
        queued(tuning(QueueLayout::Rows)),
    )
    .await;
    let mut reference = None;
    for db in [&map, &rows] {
        install_vector_and_text(db).await;
        let ids = workload(db).await;
        let vector = target(db, QueueFamily::Vector).await;
        let text = target(db, QueueFamily::Text).await;
        let stored_rows = row_keys(db, vector).await.len() + row_keys(db, text).await.len();
        let map_value = db.inner_db().get(vector.key()).await.unwrap();
        match db.index_queue_store().layout() {
            QueueLayout::Map => {
                assert_eq!(stored_rows, 0, "the map layout writes no operation rows");
                assert!(map_value.is_some(), "the map layout keeps one merge value");
            }
            QueueLayout::Rows => {
                // 4 inserts, 1 update, 1 delete, 3 hot updates, per index.
                assert_eq!(stored_rows, 18, "one row per queued operation");
                assert!(map_value.is_none(), "the row layout writes no merge value");
            }
        }
        let before = (
            results(db, &ids, SearchConsistency::Strong).await,
            results(db, &ids, SearchConsistency::Eventual).await,
        );
        drain(db, vector).await;
        drain(db, text).await;
        assert!(row_keys(db, vector).await.is_empty() && row_keys(db, text).await.is_empty());
        assert!(db.inner_db().get(vector.key()).await.unwrap().is_none());
        assert!(db
            .index_operation_backlog()
            .outstanding_targets()
            .is_empty());
        let after = results(db, &ids, SearchConsistency::Eventual).await;
        assert_eq!(
            before.0, after,
            "strong search before publication equals published state"
        );
        match &reference {
            None => reference = Some((before, after)),
            Some(expected) => assert_eq!(&(before, after), expected, "layouts agree"),
        }
    }
    map.close().await.unwrap();
    rows.close().await.unwrap();
}

#[tokio::test]
async fn row_layout_recovers_on_restart_and_orders_new_rows_after_old_ones() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = || queued(tuning(QueueLayout::Rows));
    let db = open("layout-rows-restart", Arc::clone(&store), config()).await;
    install_vector_and_text(&db).await;
    let first = add(&db, [1.0, 1.0], "before restart", None).await;
    let vector = target(&db, QueueFamily::Vector).await;
    let before = row_keys(&db, vector).await;
    db.close().await.unwrap();

    let db = open("layout-rows-restart", Arc::clone(&store), config()).await;
    assert_eq!(
        db.index_operation_backlog()
            .usage(vector.scope, vector.index_id)
            .operations,
        1,
        "restart reloads retained rows into the ledger"
    );
    update(&db, first, [2.0, 2.0], "after restart").await;
    let after = row_keys(&db, vector).await;
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], before[0], "the retained row keeps its position");
    assert!(
        after[1] > after[0],
        "new rows sort after rows written before restart"
    );
    drain(&db, vector).await;
    assert_eq!(
        vector_search(&db, [2.0, 2.0], 1, None, SearchConsistency::Eventual)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        [first]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn reopening_with_the_other_layout_fails_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(
        "layout-mismatch",
        Arc::clone(&store),
        queued(tuning(QueueLayout::Rows)),
    )
    .await;
    install_vector_and_text(&db).await;
    add(&db, [1.0, 1.0], "queued as rows", None).await;
    db.close().await.unwrap();

    let Err(error) = HelixDB::open_with_object_store_and_config(
        "layout-mismatch",
        store,
        queued(tuning(QueueLayout::Map)),
    )
    .await
    else {
        panic!("map layout refuses row-layout queues");
    };
    assert!(error.to_string().contains("layout"), "{error}");
}

#[tokio::test]
async fn readers_with_the_other_layout_fail_closed() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open(
        "layout-reader-mismatch",
        Arc::clone(&store),
        queued(tuning(QueueLayout::Rows)),
    )
    .await;
    install_vector_and_text(&writer).await;
    let queued_id = add(&writer, [1.0, 1.0], "queued as rows", None).await;
    let reader_config = |layout| DbConfig::new().with_index_operation_queue_tuning(tuning(layout));

    // A map reader would overlay nothing from row queues, so it refuses to open.
    let Err(error) = HelixDB::open_reader_with_object_store_and_config(
        "layout-reader-mismatch",
        Arc::clone(&store),
        reader_config(QueueLayout::Map),
    )
    .await
    else {
        panic!("a map reader refuses row-layout queues");
    };
    assert!(
        error.to_string().contains("layout other than Map"),
        "{error}"
    );
    let Err(error) = HelixDB::open_reader_with_object_store_for_tests(
        "layout-reader-mismatch",
        Arc::clone(&store),
    )
    .await
    else {
        panic!("the default-layout test reader refuses row-layout queues");
    };
    assert!(
        error.to_string().contains("layout other than Map"),
        "{error}"
    );

    // A reader with the writer's layout overlays the queued row.
    let reader = HelixDB::open_reader_with_object_store_and_config(
        "layout-reader-mismatch",
        Arc::clone(&store),
        reader_config(QueueLayout::Rows),
    )
    .await
    .unwrap();
    assert_eq!(
        vector_search(&reader, [1.0, 1.0], 10, None, SearchConsistency::Strong)
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        [queued_id]
    );
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn readers_check_the_queue_layout_of_every_tenant_scope() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = open(
        "layout-reader-tenants",
        Arc::clone(&store),
        queued(tuning(QueueLayout::Map)),
    )
    .await;
    let storage = writer.inner_db();
    let transaction = storage
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    // Only tenant scopes hold queues: the first as a map, the second as rows.
    for (tenant, layout) in [(0x51, QueueLayout::Map), (0x52, QueueLayout::Rows)] {
        let operations = [deletion(
            1,
            QueuedPayload::Text(QueuedTextPayload { replacement: None }),
        )];
        QueueStore::new(layout, writer.index_operand_limit(), 0)
            .stage_enqueue(
                &transaction,
                QueueTarget::new(
                    DataScope::Tenant(TenantId::from_u128(tenant)),
                    IndexId::new(3).unwrap(),
                    IndexGenerationId::new(1).unwrap(),
                ),
                QueueOperand::enqueue(&operations).unwrap(),
                &operations,
            )
            .unwrap();
    }
    transaction.commit().await.unwrap();
    writer.close().await.unwrap();

    // The map reader must reach the second tenant; the row reader fails on
    // the first.
    for layout in [QueueLayout::Map, QueueLayout::Rows] {
        let Err(error) = HelixDB::open_reader_with_object_store_and_config(
            "layout-reader-tenants",
            Arc::clone(&store),
            DbConfig::new().with_index_operation_queue_tuning(tuning(layout)),
        )
        .await
        else {
            panic!("a {layout:?} reader refuses the other layout's tenant queues");
        };
        assert!(
            error
                .to_string()
                .contains(&format!("layout other than {layout:?}")),
            "{error}"
        );
    }
}

/// Row key of `sequence` in `target`'s generation.
fn row_key(target: QueueTarget, sequence: u64) -> Bytes {
    ManagedIndexKey::Data {
        scope: target.scope,
        kind: ScopedKey::IndexOperationRow(IndexOperationRowKey {
            index_id: target.index_id,
            generation: target.generation,
            sequence,
        }),
    }
    .to_bytes()
}

fn deletion(id: u128, payload: QueuedPayload) -> QueuedOperation {
    QueuedOperation::new(
        QueuedOperationId::try_from_u128(id).unwrap(),
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        },
        payload,
    )
}

#[tokio::test]
async fn corrupt_row_queues_fail_closed() {
    let db = open(
        "layout-rows-corrupt",
        Arc::new(InMemory::new()),
        queued(tuning(QueueLayout::Rows)),
    )
    .await;
    install_vector_and_text(&db).await;
    let text = target(&db, QueueFamily::Text).await;
    let storage = db.inner_db();
    let store = QueueStore::new(QueueLayout::Rows, db.index_operand_limit(), 0);
    let text_row = deletion(
        1,
        QueuedPayload::Text(QueuedTextPayload { replacement: None }),
    );
    storage
        .put(
            row_key(text, 0),
            QueueRow::encode(QueueFamily::Text, &text_row),
        )
        .await
        .unwrap();

    // Acknowledging an operation the read did not return never deletes blindly.
    let stored = store.read(storage.as_ref(), text).await.unwrap().unwrap();
    let transaction = storage
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let unread = QueuedOperationId::try_from_u128(2).unwrap();
    assert!(matches!(
        store.stage_acknowledge(&transaction, text, &stored, &[unread]),
        Err(HelixDbError::InvariantViolation(_))
    ));
    drop(transaction);

    // One generation's rows must share a family.
    let vector_row = deletion(
        2,
        QueuedPayload::Vector(QueuedVectorPayload {
            previous: None,
            replacement: None,
        }),
    );
    storage
        .put(
            row_key(text, 1),
            QueueRow::encode(QueueFamily::Vector, &vector_row),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.read(storage.as_ref(), text).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let backlog = IndexOperationBacklog::new(
        BacklogLimits {
            max_retained_bytes: u64::MAX,
            max_members: u64::MAX,
        },
        IndexWorkerWakeHandle::default(),
    );
    assert!(matches!(
        load_backlog(storage.as_ref(), &store, &backlog).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));

    // A key inside the row prefix that does not parse is corruption too.
    storage.delete(row_key(text, 1)).await.unwrap();
    let truncated = row_key(text, 7);
    storage
        .put(
            &truncated[..truncated.len() - 1],
            QueueRow::encode(QueueFamily::Text, &text_row),
        )
        .await
        .unwrap();
    assert!(matches!(
        load_backlog(storage.as_ref(), &store, &backlog).await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    db.close().await.unwrap();
}
