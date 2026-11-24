//! What a writer does once a newer writer opens its store and fences it.
//!
//! SlateDB closes fenced storage for good: the handle must say so through
//! [`HelixDB::storage_state`] without waiting for traffic, fail every request
//! with the typed fenced code, and stop the background work it runs over the
//! store. A reader of the same store never notices.

use std::sync::Arc;
use std::time::Duration;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::error_code::QueryErrorCode;
use helix_ast::graph::NodeRef;
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::PropertyInput;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::*;

/// Longer than SlateDB's default one-second manifest poll, by which an idle
/// fenced writer learns of the fence.
const DETECTION_BOUND: Duration = Duration::from_secs(10);

fn add_document(name: &str) -> QueryRequest {
    QueryRequest::write(
        write_batch()
            .var_as(
                "created",
                g().add_n("Document", vec![("name", PropertyInput::from(name))]),
            )
            .returning(["created"]),
    )
}

fn count_documents() -> QueryRequest {
    QueryRequest::read(
        read_batch()
            .var_as("count", g().n(NodeRef::all()).count())
            .returning(["count"]),
    )
}

/// Polls `condition` until it holds or [`DETECTION_BOUND`] passes.
async fn eventually(what: &str, mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(DETECTION_BOUND, async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} within {DETECTION_BOUND:?}"));
}

/// Whether any background task of `db` still runs.
async fn background_tasks_running(db: &HelixDB) -> [bool; 7] {
    let inner = &db.inner;
    [
        inner.migration_worker.lock().await.is_some(),
        inner.dropped_retire.lock().await.is_some(),
        inner.index_worker.lock().await.is_some(),
        inner.caches.startup_tasks.slate.lock().await.is_some(),
        inner.caches.startup_tasks.fts.lock().await.is_some(),
        inner
            .caches
            .startup_tasks
            .vector_parts
            .lock()
            .await
            .is_some(),
        inner
            .caches
            .vector_memory
            .refresh_task
            .lock()
            .await
            .is_some(),
    ]
}

#[test]
fn every_close_reason_has_one_state() {
    assert_eq!(StorageState::of(None), StorageState::Open);
    assert_eq!(
        StorageState::of(Some(slatedb::CloseReason::Fenced)),
        StorageState::Fenced
    );
    assert_eq!(
        StorageState::of(Some(slatedb::CloseReason::Clean)),
        StorageState::Closed
    );
    assert_eq!(
        StorageState::of(Some(slatedb::CloseReason::Panic)),
        StorageState::Failed
    );
    assert_eq!(
        [
            StorageState::Open,
            StorageState::Fenced,
            StorageState::Failed,
            StorageState::Closed,
        ]
        .map(StorageState::as_str),
        ["open", "fenced", "failed", "closed"]
    );
}

/// A writer that receives nothing still learns of the fence, stops its
/// background work, and then fails reads and writes alike with the typed
/// fenced code, while the newer writer serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_fenced_writer_reports_it_and_stops_its_background_work() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = HelixDB::open_with_object_store("fenced-idle-writer", Arc::clone(&object_store))
        .await
        .expect("first writer opens");
    old.query(add_document("before"))
        .await
        .expect("first writer writes");
    assert_eq!(old.storage_state(), StorageState::Open);
    let running = background_tasks_running(&old).await;
    assert!(
        running[2] && running[6],
        "the index worker and vector refresh run on an open writer: {running:?}"
    );

    let new = HelixDB::open_with_object_store("fenced-idle-writer", Arc::clone(&object_store))
        .await
        .expect("second writer opens and fences the first");

    eventually("the idle writer sees its fence", async || {
        old.storage_state() == StorageState::Fenced
    })
    .await;
    eventually("the fenced writer stops its background work", async || {
        background_tasks_running(&old).await == [false; 7]
    })
    .await;

    for request in [count_documents(), add_document("after")] {
        let error = old
            .query(request)
            .await
            .expect_err("a fenced writer serves nothing");
        assert_eq!(
            error.error_code(),
            QueryErrorCode::WriterFencedCommitOutcomeUnknown,
            "{error}"
        );
    }

    assert_eq!(new.storage_state(), StorageState::Open);
    new.query(add_document("after"))
        .await
        .expect("newer writer writes");
    let count = new
        .query(count_documents())
        .await
        .expect("newer writer reads");
    assert_eq!(count["count"], 2);

    old.close()
        .await
        .expect("a fenced writer closes without flushing");
    assert_eq!(
        old.storage_state(),
        StorageState::Fenced,
        "a close keeps the reason storage closed"
    );
    new.close().await.expect("newer writer closes");
    assert_eq!(new.storage_state(), StorageState::Closed);
}

/// A write in flight on the old writer, or the first request after the
/// fence, is what notices it here: the outcome is the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_busy_fenced_writer_fails_its_next_write_with_the_typed_code() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = HelixDB::open_with_object_store("fenced-busy-writer", Arc::clone(&object_store))
        .await
        .expect("first writer opens");
    old.query(add_document("before"))
        .await
        .expect("first writer writes");
    let new = HelixDB::open_with_object_store("fenced-busy-writer", Arc::clone(&object_store))
        .await
        .expect("second writer opens and fences the first");

    let error = old
        .query(add_document("raced"))
        .await
        .expect_err("the old writer cannot commit after the fence");
    assert_eq!(
        error.error_code(),
        QueryErrorCode::WriterFencedCommitOutcomeUnknown,
        "{error}"
    );
    eventually("the writer reports the fence it hit", async || {
        old.storage_state() == StorageState::Fenced
    })
    .await;
    eventually("the fenced writer stops its background work", async || {
        background_tasks_running(&old).await == [false; 7]
    })
    .await;

    old.close().await.expect("fenced writer closes");
    new.close().await.expect("newer writer closes");
}

/// A reader polls the store the writers share and sees each writer's
/// commits; a fence between them changes nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_is_unaffected_when_its_writer_is_fenced() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = HelixDB::open_with_object_store("fenced-writer-reader", Arc::clone(&object_store))
        .await
        .expect("first writer opens");
    old.query(add_document("before"))
        .await
        .expect("first writer writes");
    old.flush_writer().await.expect("first writer flushes");
    let reader =
        HelixDB::open_reader_with_object_store("fenced-writer-reader", Arc::clone(&object_store))
            .await
            .expect("reader opens");

    let new = HelixDB::open_with_object_store("fenced-writer-reader", Arc::clone(&object_store))
        .await
        .expect("second writer opens and fences the first");
    eventually("the old writer sees its fence", async || {
        old.storage_state() == StorageState::Fenced
    })
    .await;
    new.query(add_document("after"))
        .await
        .expect("newer writer writes");
    new.flush_writer().await.expect("newer writer flushes");

    eventually("the reader sees the newer writer's commit", async || {
        reader
            .query(count_documents())
            .await
            .expect("the reader serves throughout")["count"]
            == 2
    })
    .await;
    assert_eq!(reader.storage_state(), StorageState::Open);
    assert!(
        background_tasks_running(&reader).await[6],
        "the reader's vector refresh keeps running"
    );

    reader.close().await.expect("reader closes");
    old.close().await.expect("fenced writer closes");
    new.close().await.expect("newer writer closes");
}
