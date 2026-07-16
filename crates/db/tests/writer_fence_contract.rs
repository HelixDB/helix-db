//! Executable proof of SlateDB's single-writer epoch contract.
//!
//! Index V2 workers rely on a newer writer fencing every transaction opened by
//! an older writer before the newer handle can issue database transactions.
//! This test exercises that storage boundary directly so lifecycle code does
//! not need to invent a second writer-election protocol.

use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::{CloseReason, Db, ErrorKind, IsolationLevel};

#[tokio::test]
async fn newer_writer_open_fences_an_already_open_old_transaction() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let database = "writer-fence-contract";

    let old_writer = Db::open(database, Arc::clone(&object_store))
        .await
        .expect("old writer opens");
    let old_transaction = old_writer
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("old transaction begins before the fence");
    old_transaction
        .put(b"old-writer", b"must-not-commit")
        .expect("old transaction stages a write");

    let new_writer = Db::open(database, Arc::clone(&object_store))
        .await
        .expect("new writer claims its epoch before open returns");
    let new_transaction = new_writer
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("new writer can transact after claiming the epoch");
    new_transaction
        .put(b"new-writer", b"committed")
        .expect("new transaction stages a write");
    new_transaction
        .commit()
        .await
        .expect("new writer commits after its open fence");

    let error = old_transaction
        .commit()
        .await
        .expect_err("the old transaction must not commit after the newer writer opens");
    assert_eq!(error.kind(), ErrorKind::Closed(CloseReason::Fenced));

    new_writer.close().await.expect("new writer closes cleanly");
}
