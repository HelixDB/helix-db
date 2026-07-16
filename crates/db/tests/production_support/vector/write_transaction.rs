//! Production contracts for measured vector write transactions.
//!
//! This feature-gated child module verifies the last-write-wins recorder,
//! checkpoint identity, SlateDB read delegation, and deterministic pre-write
//! failures used by bounded lifecycle builders. All writes remain uncommitted
//! in isolated in-memory databases, so no persisted key or value format changes.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{DbReadOps, IsolationLevel};

use super::*;

/// Verifies final measurement, checkpoints, read delegation, and fault seams.
async fn run_measured_transaction_contract() {
    let db = slatedb::Db::open(
        "production-vector-write-transaction",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    assert_eq!(measured.measurement().unwrap().operations(), 0);
    assert_eq!(measured.measurement().unwrap().encoded_bytes(), 0);

    measured.put(b"old", b"stable").unwrap();
    let checkpoint = measured.checkpoint();
    measured.put(b"first", b"superseded").unwrap();
    measured.put(b"first", b"final").unwrap();
    measured
        .put_bytes(Bytes::from_static(b"second"), Bytes::from_static(b"value"))
        .unwrap();
    measured.delete(b"second").unwrap();

    let complete = measured.measurement().unwrap();
    assert_eq!(complete.operations(), 3);
    assert_eq!(
        complete.encoded_bytes(),
        u64::try_from(
            b"old".len() + b"stable".len() + b"first".len() + b"final".len() + b"second".len()
        )
        .unwrap()
    );
    let since = measured.measurement_since(checkpoint).unwrap();
    assert_eq!(since.operations(), 2);
    assert_eq!(
        since.encoded_bytes(),
        u64::try_from(b"first".len() + b"final".len() + b"second".len()).unwrap()
    );

    let foreign = MeasuredVectorTransaction::new(&txn).checkpoint();
    assert_eq!(
        measured.measurement_since(foreign),
        Err(VectorWriteMeasurementError::ForeignCheckpoint)
    );
    let future = VectorWriteCheckpoint {
        recorder_identity: Arc::clone(&measured.recorder.identity),
        revision: u64::MAX,
    };
    assert_eq!(
        measured.measurement_since(future),
        Err(VectorWriteMeasurementError::ForeignCheckpoint)
    );

    assert_eq!(measured.get(b"first").await.unwrap().unwrap(), b"final"[..]);
    assert!(measured.get(b"second").await.unwrap().is_none());
    assert!(measured.get_key_value(b"first").await.unwrap().is_some());
    assert_eq!(
        measured
            .multi_get(&[&b"old"[..], &b"first"[..]])
            .await
            .unwrap()
            .len(),
        2
    );
    let mut scan = measured.scan(..).await.unwrap();
    assert!(scan.next().await.unwrap().is_some());
    let mut prefix = measured.scan_prefix(b"f", ..).await.unwrap();
    assert!(prefix.next().await.unwrap().is_some());

    measured.fail_read_after(0);
    assert!(measured.get(b"first").await.is_err());
    measured.fail_read_after(0);
    assert!(measured.get_key_value(b"first").await.is_err());
    measured.fail_read_after(0);
    assert!(measured
        .multi_get(&[&b"old"[..], &b"first"[..]])
        .await
        .is_err());
    measured.fail_read_after(0);
    assert!(measured.scan(..).await.is_err());
    measured.fail_read_after(0);
    assert!(measured.scan_prefix(b"f", ..).await.is_err());

    measured.fail_next_write();
    assert!(measured.put(b"failed", b"put").is_err());
    assert!(measured.get(b"failed").await.unwrap().is_none());
    measured.put(b"failed", b"put").unwrap();
    measured.fail_next_write();
    assert!(measured.delete(b"failed").is_err());
    assert_eq!(measured.get(b"failed").await.unwrap().unwrap(), b"put"[..]);
    txn.rollback();
}

/// Verifies one recorder retains cumulative identity across short transaction borrows.
async fn run_shared_recorder_contract() {
    let db = slatedb::Db::open(
        "production-vector-write-recorder",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let recorder = VectorWriteRecorder::new();

    let first = recorder.bind(&txn);
    first.put(b"shared", b"first").unwrap();
    let checkpoint = first.checkpoint();
    drop(first);

    let second = recorder.bind(&txn);
    second.put(b"shared", b"replacement").unwrap();
    second.put(b"new", b"value").unwrap();
    assert_eq!(second.measurement().unwrap().operations(), 2);
    assert_eq!(
        second.measurement_since(checkpoint).unwrap(),
        second.measurement().unwrap()
    );
    txn.rollback();
}

/// Exercises measured-write replacement, delegation, and failure boundaries.
pub(crate) async fn run() {
    run_measured_transaction_contract().await;
    run_shared_recorder_contract().await;
}
