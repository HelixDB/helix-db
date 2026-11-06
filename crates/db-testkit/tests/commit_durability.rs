//! Contracts for when a write request returns relative to its WAL upload, and
//! what a crash may lose afterwards. A crash here is every unuploaded byte
//! failing to land, which is the only state a writer leaves behind.

#![recursion_limit = "256"]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use db::encoding::v2::keys::scope::DataScope;
use db::execution_control::ExecutionControl;
use db::query_service::{HelixQueryService, QueryMode, QueryResponse, QueryServiceError};
use db::HelixDB;
use helix_ast::batch;
use helix_ast::index::IndexSpec;
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use helix_ast::value::PropertyInput;
use helix_db_testkit::gated_wal::GatedWalStore;

const PATH: &str = "commit-durability";
/// Longer than SlateDB's 100 ms flush interval, so a request still pending
/// after this long is waiting on the held upload and not on the timer.
const PENDING: Duration = Duration::from_millis(400);
const RETURNS: Duration = Duration::from_secs(30);

async fn open(store: &Arc<GatedWalStore>) -> Arc<HelixDB> {
    Arc::new(
        HelixDB::open_with_object_store(PATH, Arc::clone(store) as _)
            .await
            .expect("writer opens"),
    )
}

/// Opens a writer that has leased node IDs. A writer's first insert persists
/// its lease durably, so it would wait on a held upload in any mode.
async fn open_leased(store: &Arc<GatedWalStore>) -> Arc<HelixDB> {
    let db = open(store).await;
    let lease = QueryRequest::write(batch::write_batch().var_as(
        "lease",
        traversal::g().add_n("Lease", vec![("held", PropertyInput::from("no"))]),
    ));
    execute(&db, lease, QueryMode::Execute).await;
    db
}

fn add(name: &str) -> QueryRequest {
    QueryRequest::write(batch::write_batch().var_as(
        "created",
        traversal::g().add_n("User", vec![("name", PropertyInput::from(name))]),
    ))
}

fn count() -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as("count", traversal::g().n_with_label("User").count())
            .returning(["count"]),
    )
}

fn create_name_index() -> QueryRequest {
    QueryRequest::write(batch::write_batch().var_as(
        "index",
        traversal::g().create_index_if_not_exists(IndexSpec::node_equality("User", "name")),
    ))
}

/// A write request that stages nothing and reports what it observed.
fn observe() -> QueryRequest {
    QueryRequest::write(
        batch::write_batch()
            .var_as("count", traversal::g().n_with_label("User").count())
            .returning(["count"]),
    )
}

async fn node_count(db: &Arc<HelixDB>) -> u64 {
    HelixQueryService::new(Arc::clone(db))
        .execute_query(count())
        .await
        .expect("count reads")
        .returns()["count"]
        .as_u64()
        .expect("count is a number")
}

async fn execute(db: &Arc<HelixDB>, request: QueryRequest, mode: QueryMode) {
    tokio::time::timeout(
        RETURNS,
        HelixQueryService::new(Arc::clone(db)).execute_query_with_mode(request, mode),
    )
    .await
    .expect("request returns")
    .expect("request succeeds");
}

/// Runs `future` until it is pending past [`PENDING`] and hands it back.
async fn assert_pending<F: Future + Unpin>(mut future: F) -> F {
    assert!(
        tokio::time::timeout(PENDING, &mut future).await.is_err(),
        "the request must wait for its WAL upload"
    );
    future
}

/// Loses everything not yet uploaded and reopens from what storage holds. A
/// writer with an upload to fail closes; the next writer fences any other.
async fn crash_and_reopen(store: &Arc<GatedWalStore>, crashed: Arc<HelixDB>) -> Arc<HelixDB> {
    store.fail();
    tokio::time::sleep(PENDING).await;
    store.recover();
    let reopened = open(store).await;
    drop(crashed);
    reopened
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_write_waits_for_its_wal_upload() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    let write = assert_pending(Box::pin(service.execute_query(add("a")))).await;
    assert_eq!(node_count(&db).await, 1, "applied before it is durable");
    store.release();
    write.await.expect("write commits");
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn applied_write_returns_before_its_wal_upload() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;

    store.hold();
    execute(&db, add("a"), QueryMode::ExecuteWithoutDurableWait).await;
    assert_eq!(node_count(&db).await, 1, "visible on the writer");
    store.release();
    db.flush_writer().await.unwrap();
    drop(db);
    store.fail();

    tokio::time::sleep(PENDING).await;
    store.recover();
    let reopened = open(&store).await;
    assert_eq!(node_count(&reopened).await, 1, "the flush uploaded it");
    reopened.close().await.unwrap();
}

/// The commit's own wait is what publishes it: a reader needs no extra flush.
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_response_is_readable_from_a_new_reader() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let new_reader = || async {
        Arc::new(
            HelixDB::open_reader_with_object_store(PATH, Arc::clone(&store) as _)
                .await
                .expect("reader opens"),
        )
    };

    execute(&db, add("durable"), QueryMode::Execute).await;
    assert_eq!(node_count(&new_reader().await).await, 1);

    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    assert_eq!(node_count(&new_reader().await).await, 1, "not uploaded yet");
    store.release();
    db.flush_writer().await.unwrap();
    assert_eq!(node_count(&new_reader().await).await, 2);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_before_upload_loses_only_the_applied_suffix() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;

    execute(&db, add("durable"), QueryMode::Execute).await;
    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    assert_eq!(node_count(&db).await, 2);

    let reopened = crash_and_reopen(&store, db).await;
    assert_eq!(node_count(&reopened).await, 1, "only the durable write");
    reopened.close().await.unwrap();
}

/// WAL order is what lets internal durable commits build on applied ones.
#[tokio::test(flavor = "multi_thread")]
async fn durable_write_carries_every_earlier_applied_write() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    let durable = assert_pending(Box::pin(service.execute_query(add("durable")))).await;
    store.release();
    durable.await.expect("durable write commits");

    let reopened = crash_and_reopen(&store, db).await;
    assert_eq!(node_count(&reopened).await, 2);
    reopened.close().await.unwrap();
}

/// A write request that commits nothing has no WAL entry of its own, but its
/// response may rest on an applied write it read.
#[tokio::test(flavor = "multi_thread")]
async fn empty_durable_write_waits_for_the_writes_it_observed() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    let observed = assert_pending(Box::pin(service.execute_query(observe()))).await;
    store.release();
    let response = observed.await.expect("empty write succeeds");
    assert_eq!(response.returns()["count"].as_u64(), Some(1));

    let reopened = crash_and_reopen(&store, db).await;
    assert_eq!(node_count(&reopened).await, 1);
    reopened.close().await.unwrap();
}

/// The durable sequence must reach the snapshot of a writer that has written
/// nothing yet, or an empty write would wait forever.
#[tokio::test(flavor = "multi_thread")]
async fn empty_durable_write_returns_on_a_fresh_or_recovered_writer() {
    let store = Arc::new(GatedWalStore::default());
    let fresh = open(&store).await;
    execute(&fresh, observe(), QueryMode::Execute).await;

    execute(&fresh, add("closed"), QueryMode::Execute).await;
    fresh.close().await.unwrap();
    let reopened = open(&store).await;
    execute(&reopened, observe(), QueryMode::Execute).await;

    execute(&reopened, add("crashed"), QueryMode::Execute).await;
    store.hold();
    execute(&reopened, add("lost"), QueryMode::ExecuteWithoutDurableWait).await;
    let recovered = crash_and_reopen(&store, reopened).await;
    assert_eq!(node_count(&recovered).await, 2);
    execute(&recovered, observe(), QueryMode::Execute).await;
    recovered.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_durable_write_forces_no_upload() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    execute(&db, add("a"), QueryMode::Execute).await;

    let uploads = store.uploads();
    for _ in 0..16 {
        execute(&db, observe(), QueryMode::Execute).await;
    }
    assert_eq!(store.uploads(), uploads);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_applied_write_does_not_wait() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;

    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    execute(&db, observe(), QueryMode::ExecuteWithoutDurableWait).await;
    store.release();
    db.close().await.unwrap();
}

/// The wait cannot outlive the writer: a closing writer surfaces as a failed
/// request, and a request that never saw durability must not report success.
#[tokio::test(flavor = "multi_thread")]
async fn empty_durable_write_fails_when_the_writer_closes() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    let observed = assert_pending(Box::pin(service.execute_query(observe()))).await;
    store.fail();
    tokio::time::timeout(RETURNS, observed)
        .await
        .expect("a closing writer ends the wait")
        .expect_err("the observed write never became durable");
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_close_uploads_applied_writes() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;

    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    db.close().await.unwrap();

    let reopened = open(&store).await;
    assert_eq!(node_count(&reopened).await, 1);
    reopened.close().await.unwrap();
}

/// Index DDL owns its lifecycle commits, so a request's mode does not weaken it.
#[tokio::test(flavor = "multi_thread")]
async fn index_ddl_stays_durable_without_a_durable_wait() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    let ddl = assert_pending(Box::pin(service.execute_query_with_mode(
        create_name_index(),
        QueryMode::ExecuteWithoutDurableWait,
    )))
    .await;
    store.release();
    ddl.await.expect("index DDL commits");
    db.close().await.unwrap();
}

/// An `IF NOT EXISTS` that loses a race commits nothing, but its receipt
/// promises the index the winner has only applied so far.
#[tokio::test(flavor = "multi_thread")]
async fn no_op_index_ddl_waits_for_the_record_it_observed() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));
    store.hold();
    let winner = assert_pending(Box::pin(service.execute_query(create_name_index()))).await;
    let loser = assert_pending(Box::pin(service.execute_query(create_name_index()))).await;
    store.release();
    winner.await.expect("the first DDL commits");
    loser.await.expect("the second DDL observes it");
    db.close().await.unwrap();
}

/// A request stuck behind a held upload still stops at its deadline, and the
/// write it observed is untouched.
async fn bounded(
    db: &Arc<HelixDB>,
    request: QueryRequest,
) -> Result<QueryResponse, QueryServiceError> {
    tokio::time::timeout(
        RETURNS,
        HelixQueryService::new(Arc::clone(db)).execute_query_with_mode_scoped_controlled(
            request,
            QueryMode::Execute,
            DataScope::LegacyUnscoped,
            ExecutionControl::from_timeout(Duration::from_millis(200)),
        ),
    )
    .await
    .expect("the deadline ends the wait")
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_durable_write_stops_at_its_deadline() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;

    store.hold();
    execute(&db, add("applied"), QueryMode::ExecuteWithoutDurableWait).await;
    let error = bounded(&db, observe())
        .await
        .expect_err("nothing is durable yet");
    assert!(error.is_deadline_exceeded(), "{error}");
    store.release();
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn no_op_index_ddl_stops_at_its_deadline() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    let winner = assert_pending(Box::pin(service.execute_query(create_name_index()))).await;
    let error = bounded(&db, create_name_index())
        .await
        .expect_err("nothing is durable yet");
    assert!(error.is_deadline_exceeded(), "{error}");
    store.release();
    winner.await.expect("the first DDL commits");
    db.close().await.unwrap();
}

/// Reading an operation's status is planned like DDL, but stays a read: it never
/// waits on a held upload and keeps working on a replica.
#[tokio::test(flavor = "multi_thread")]
async fn operation_status_reads_stay_reads() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));
    let create = QueryRequest::write(
        batch::write_batch()
            .var_as(
                "index",
                traversal::g().create_index_if_not_exists(IndexSpec::node_equality("User", "name")),
            )
            .returning(["index"]),
    );
    let response = service.execute_query(create).await.expect("DDL commits");
    let operation_id = response.returns()["index"]["operation_id"]
        .as_str()
        .expect("the receipt names its operation")
        .to_string();
    let status = || {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "status",
                    traversal::g().get_index_operation(operation_id.clone()),
                )
                .returning(["status"]),
        )
    };
    let reader = Arc::new(
        HelixDB::open_reader_with_object_store(PATH, Arc::clone(&store) as _)
            .await
            .expect("reader opens"),
    );

    store.hold();
    execute(&db, status(), QueryMode::Execute).await;
    execute(&reader, status(), QueryMode::Execute).await;
    store.release();
    db.close().await.unwrap();
}

/// IDs must never repeat after a crash, so the lease behind them stays durable
/// even when the insert that needs it asked not to wait.
#[tokio::test(flavor = "multi_thread")]
async fn first_insert_waits_for_its_durable_id_lease() {
    let store = Arc::new(GatedWalStore::default());
    let db = open(&store).await;
    let service = HelixQueryService::new(Arc::clone(&db));

    store.hold();
    let insert = assert_pending(Box::pin(
        service.execute_query_with_mode(add("first"), QueryMode::ExecuteWithoutDurableWait),
    ))
    .await;
    store.release();
    insert.await.expect("first insert applies");
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_ignore_the_durable_wait() {
    let store = Arc::new(GatedWalStore::default());
    let db = open_leased(&store).await;

    execute(&db, count(), QueryMode::ExecuteWithoutDurableWait).await;
    store.hold();
    execute(&db, count(), QueryMode::Execute).await;
    store.release();
    db.close().await.unwrap();
}
