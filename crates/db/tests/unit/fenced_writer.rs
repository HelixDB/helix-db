//! What a writer does once a newer writer opens its store and fences it.
//!
//! SlateDB closes fenced storage for good: the handle must say so through
//! [`HelixDB::storage_state`] without waiting for traffic, fail every request
//! with the typed fenced code, and stop the background work it runs over the
//! store. A reader of the same store never notices.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::error_code::QueryErrorCode;
use helix_ast::graph::NodeRef;
use helix_ast::index::{IndexSpec, VectorDistanceMetric};
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

/// Whether `error` is the closed-storage error SlateDB gives every request
/// once a newer writer fenced the handle.
fn is_fenced(error: &HelixDbError) -> bool {
    matches!(
        error,
        HelixDbError::Storage(storage)
            if storage.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
    ) && error.error_code() == QueryErrorCode::WriterFencedCommitOutcomeUnknown
}

/// A close that runs while the storage watcher still joins a task waits
/// for that task before it closes storage, as every close joins every task
/// first. Here the index worker is the task: its step is held between
/// staging and commit, and stopping the worker waits for the step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_waits_for_the_task_the_fenced_writer_is_still_stopping() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let old = HelixDB::open_with_object_store("fenced-close-joins", Arc::clone(&object_store))
        .await
        .expect("first writer opens");
    let pause =
        index_lifecycle::outbox::step_pause::StepPause::arm(old.inner_db().as_ref(), |_| true);
    old.query(QueryRequest::write(
        write_batch()
            .var_as(
                "operation",
                g().create_index_if_not_exists(IndexSpec::node_vector(
                    "Doc",
                    "embedding",
                    NonZeroUsize::new(4).expect("the dimension is nonzero"),
                    VectorDistanceMetric::Cosine,
                    None::<&str>,
                )),
            )
            .returning(["operation"]),
    ))
    .await
    .expect("the vector index is created");
    tokio::time::timeout(DETECTION_BOUND, pause.reached())
        .await
        .expect("the worker's first step is held");

    let new = HelixDB::open_with_object_store("fenced-close-joins", Arc::clone(&object_store))
        .await
        .expect("second writer opens and fences the first");
    // The watcher holds the worker's slot while it joins the worker.
    eventually("the watcher stops the index worker", async || {
        old.storage_state() == StorageState::Fenced && old.inner.index_worker.try_lock().is_err()
    })
    .await;
    let closing = tokio::spawn({
        let old = HelixDB {
            inner: Arc::clone(&old.inner),
        };
        async move { old.close().await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !closing.is_finished(),
        "close waits for the worker the watcher still joins"
    );

    pause.release();
    tokio::time::timeout(DETECTION_BOUND, closing)
        .await
        .expect("close ends once the worker is joined")
        .expect("close runs to the end")
        .expect("a fenced writer closes");
    new.close().await.expect("newer writer closes");
}

/// How a test ends a writer's background work.
#[derive(Debug, Clone, Copy)]
enum StopBy {
    /// A newer writer fences it.
    Fence,
    /// [`HelixDB::close`].
    Close,
}

/// A bulk vector build in pass 2 when its writer is fenced or closed is
/// cancelled: its construction thread ends and its memory reservation is
/// released, though a fenced writer's process lives on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bulk_build_in_pass_two_ends_and_releases_its_memory_when_its_writer_stops() {
    for stop in [StopBy::Fence, StopBy::Close] {
        let database = format!("fenced-bulk-build-{stop:?}");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = HelixDB::open_with_object_store_and_config(
            database.clone(),
            Arc::clone(&object_store),
            DbConfig::new().with_index_lifecycle_throughput_tuning(
                config::IndexLifecycleThroughputTuning::default().with_vector_bulk_build(
                    config::VectorBulkBuildTuning::try_new(
                        1,
                        config::VectorBulkBuildMemory::Bytes(
                            // Accounted, not allocated: room for the
                            // whole write buffer.
                            NonZeroU64::new(64 << 30).expect("the budget is positive"),
                        ),
                        16,
                    )
                    .expect("the bulk tuning validates"),
                ),
            ),
        )
        .await
        .expect("first writer opens");
        for seed in 0..64_u8 {
            let vector: Vec<f32> = vec![f32::from(seed), 1.0, 2.0, f32::from(seed % 7)];
            old.query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "node",
                        g().add_n("Doc", vec![("embedding", PropertyInput::from(vector))]),
                    )
                    .returning(["node"]),
            ))
            .await
            .expect("first writer writes");
        }
        // Pass 2 links nothing until the build is cancelled.
        old.inner.vector_driver.hold_bulk_linking_until_cancelled();
        let receipt = old
            .query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "operation",
                        g().create_index_if_not_exists(IndexSpec::node_vector(
                            "Doc",
                            "embedding",
                            NonZeroUsize::new(4).expect("the dimension is nonzero"),
                            VectorDistanceMetric::Cosine,
                            None::<&str>,
                        )),
                    )
                    .returning(["operation"]),
            ))
            .await
            .expect("the vector index is created");
        let operation_id = index_lifecycle::IndexOperationId::new(
            uuid::Uuid::parse_str(
                receipt["operation"]["operation_id"]
                    .as_str()
                    .unwrap_or_else(|| panic!("the create names its operation: {receipt}")),
            )
            .expect("the operation id is a UUID"),
        )
        .expect("the operation id is not nil");
        eventually("the build reaches pass 2", async || {
            matches!(
                old.inner.vector_build_reports.phase(operation_id),
                Some(index_lifecycle::VectorBulkBuildPhase::Linking { .. })
            )
        })
        .await;
        assert!(old.inner.vector_driver.bulk_reserved_bytes() > 0);

        let new = match stop {
            StopBy::Fence => Some(
                HelixDB::open_with_object_store(database.clone(), Arc::clone(&object_store))
                    .await
                    .expect("second writer opens and fences the first"),
            ),
            StopBy::Close => {
                old.close().await.expect("the writer closes");
                None
            }
        };
        // The construction thread holds the reservation and drops it as it
        // ends, which, held as it is, only a cancellation lets it do.
        eventually("the construction thread ends", async || {
            old.inner.vector_driver.bulk_reserved_bytes() == 0
        })
        .await;
        assert!(
            old.inner.vector_build_reports.phase(operation_id).is_none(),
            "{stop:?}: the build reports nothing"
        );
        match stop {
            StopBy::Fence => assert_eq!(old.storage_state(), StorageState::Fenced),
            StopBy::Close => assert_eq!(old.storage_state(), StorageState::Closed),
        }
        old.close().await.expect("the old writer closes");
        futures::future::OptionFuture::from(new.as_ref().map(HelixDB::close))
            .await
            .transpose()
            .expect("newer writer closes");
    }
}

/// Every call that needs a background task a fenced writer stopped fails
/// with the fence, as its requests do, before and after its close; after a
/// clean close they report the close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn calls_that_need_a_stopped_task_report_why_storage_closed() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let stepped = || {
        DbConfig::new().with_secondary_index_lifecycle_tuning(
            config::SecondaryIndexLifecycleTuning::default()
                .with_worker_mode(config::SecondaryIndexLifecycleWorkerMode::Disabled),
        )
    };
    let old = HelixDB::open_with_object_store_and_config(
        "fenced-stopped-calls",
        Arc::clone(&object_store),
        stepped(),
    )
    .await
    .expect("first writer opens");
    assert!(!old
        .process_secondary_index_lifecycle_once()
        .await
        .expect("an open writer steps"));
    old.index_worker_epoch()
        .await
        .expect("an open writer has a worker epoch");
    old.reclaim_dropped_index_storage(Duration::from_secs(5))
        .await
        .expect("an open writer reclaims");

    let new = HelixDB::open_with_object_store_and_config(
        "fenced-stopped-calls",
        Arc::clone(&object_store),
        stepped(),
    )
    .await
    .expect("second writer opens and fences the first");
    eventually("the fenced writer stops its background work", async || {
        background_tasks_running(&old).await == [false; 7]
    })
    .await;
    for closed in [false, true] {
        if closed {
            old.close().await.expect("a fenced writer closes");
        }
        let errors = [
            old.process_secondary_index_lifecycle_once()
                .await
                .expect_err("a fenced writer steps nothing"),
            old.index_worker_epoch()
                .await
                .expect_err("a fenced writer runs no worker"),
            old.reclaim_dropped_index_storage(Duration::from_secs(5))
                .await
                .expect_err("a fenced writer reclaims nothing"),
            old.remove_quarantined_dropped_index_entry(1, 1, Duration::from_secs(5))
                .await
                .expect_err("a fenced writer removes nothing"),
        ];
        for error in errors {
            assert!(is_fenced(&error), "closed: {closed}: {error:?}");
        }
    }

    new.close().await.expect("newer writer closes");
    let errors = [
        new.process_secondary_index_lifecycle_once()
            .await
            .expect_err("a closed writer steps nothing"),
        new.index_worker_epoch()
            .await
            .expect_err("a closed writer runs no worker"),
        new.reclaim_dropped_index_storage(Duration::from_secs(5))
            .await
            .expect_err("a closed writer reclaims nothing"),
        new.remove_quarantined_dropped_index_entry(1, 1, Duration::from_secs(5))
            .await
            .expect_err("a closed writer removes nothing"),
    ];
    for error in errors {
        assert!(
            matches!(error, HelixDbError::DatabaseClosed),
            "a clean close is reported as one: {error:?}"
        );
    }
}
