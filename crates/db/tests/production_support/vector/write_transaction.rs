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
    let direct_since = measured
        .recorder
        .writes
        .lock()
        .measurement_after(Some(&checkpoint))
        .unwrap();
    assert_eq!(direct_since.operations(), 2);
    assert_eq!(
        direct_since.encoded_bytes(),
        u64::try_from(b"first".len() + b"final".len() + b"second".len()).unwrap()
    );
    let since = measured.plan_since(checkpoint).unwrap().measurement();
    assert_eq!(since.operations(), 2);
    assert_eq!(
        since.encoded_bytes(),
        u64::try_from(b"first".len() + b"final".len() + b"second".len()).unwrap()
    );

    let foreign = MeasuredVectorTransaction::new(&txn).checkpoint();
    assert!(matches!(
        measured.plan_since(foreign),
        Err(VectorWriteMeasurementError::ForeignCheckpoint)
    ));
    let future = VectorWriteCheckpoint {
        recorder_identity: Arc::clone(&measured.recorder.identity),
        revision: u64::MAX,
    };
    assert!(matches!(
        measured.plan_since(future),
        Err(VectorWriteMeasurementError::ForeignCheckpoint)
    ));

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
    let target = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let recorder = VectorWriteRecorder::new();

    let first = recorder.bind(&txn);
    first.put(b"shared", b"first").unwrap();
    target.put(b"shared", b"first").unwrap();
    let checkpoint = first.checkpoint();
    drop(first);

    let second = recorder.bind(&txn);
    second.put(b"shared", b"replacement").unwrap();
    second.put(b"new", b"value").unwrap();
    second.put(b"deleted", b"temporary").unwrap();
    second.delete(b"deleted").unwrap();
    assert_eq!(second.measurement().unwrap().operations(), 3);
    let plan = second.plan_since(checkpoint).unwrap();
    assert_eq!(plan.measurement(), second.measurement().unwrap());
    plan.apply_to(&target).unwrap();
    assert_eq!(
        target.get(b"shared").await.unwrap().unwrap(),
        b"replacement"[..]
    );
    assert_eq!(target.get(b"new").await.unwrap().unwrap(), b"value"[..]);
    assert!(target.get(b"deleted").await.unwrap().is_none());

    let foreign = MeasuredVectorTransaction::new(&txn).checkpoint();
    assert!(matches!(
        second.plan_since(foreign),
        Err(VectorWriteMeasurementError::ForeignCheckpoint)
    ));
    target.rollback();
    txn.rollback();
}

/// Verifies a plan dirties only its namespace's resident rows and fails closed
/// on a write outside the handle's data scope or with a non-vector key.
async fn run_planned_cache_contract() {
    use crate::encoding::keys::scope::DataScope;
    use crate::encoding::v2::keys::indexes::vector::{
        VectorIndexMetadataKey, VectorKey, VectorLayer0NeighborsKey, VectorSimHashKey,
        VectorUpperNeighborsKey, VectorUpperVectorKey,
    };
    use crate::encoding::v2::keys::scope::TenantId;
    use crate::encoding::v2::keys::{DataKey, DataKeyKind};
    use crate::error::HelixDbError;
    use crate::search::vector::distance::Cosine;
    use crate::search::vector::{
        ValidatedVectorGenerationHandle, VectorCacheWriteSet, VectorDimension,
        VectorGenerationIdentity,
    };

    let handle = |scope| {
        ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                scope,
                8,
                "production-planned-cache-rows".to_string(),
                80,
                std::num::NonZeroU64::MIN,
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let key = |key| {
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Vector(key),
        }
        .to_bytes()
    };
    let db = slatedb::Db::open(
        "production-vector-planned-cache-rows",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let recorder = VectorWriteRecorder::new();
    let write = recorder.bind(&txn);
    let checkpoint = write.checkpoint();
    write
        .put(key(VectorKey::SimHash(VectorSimHashKey::new(80, 1))), b"s")
        .unwrap();
    write
        .delete(key(VectorKey::UpperVector(VectorUpperVectorKey::new(
            80, 2,
        ))))
        .unwrap();
    write
        .put(
            key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                80, 3, 4,
            ))),
            b"n",
        )
        .unwrap();
    write
        .put(
            key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                80, 5,
            ))),
            b"l",
        )
        .unwrap();
    write
        .put(
            key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(80))),
            b"m",
        )
        .unwrap();
    write
        .put(key(VectorKey::SimHash(VectorSimHashKey::new(81, 6))), b"o")
        .unwrap();
    let plan = write.plan_since(checkpoint).unwrap();

    let writes = VectorCacheWriteSet::default();
    writes
        .record_planned(&handle(DataScope::LegacyUnscoped), &plan)
        .unwrap();
    let entries = writes.entries();
    assert_eq!(entries.len(), 1, "one namespace was recorded");
    let rows = entries[0].dirty_rows().expect("recorded rows are evicted");
    let mut nodes = rows.dirty_nodes();
    nodes.sort_unstable();
    assert_eq!(
        nodes,
        [1, 2],
        "only SimHash and upper-vector rows are resident"
    );
    assert_eq!(rows.dirty_upper_neighbors(), [(3, 4)]);

    let tenant = DataScope::Tenant(TenantId::from_u128(1));
    assert!(
        matches!(
            VectorCacheWriteSet::default().record_planned(&handle(tenant), &plan),
            Err(HelixDbError::InvariantViolation(_))
        ),
        "a write outside the handle's data scope fails closed"
    );
    let checkpoint = write.checkpoint();
    write.put(b"not a vector key", b"x").unwrap();
    assert!(
        matches!(
            VectorCacheWriteSet::default().record_planned(
                &handle(DataScope::LegacyUnscoped),
                &write.plan_since(checkpoint).unwrap()
            ),
            Err(HelixDbError::InvariantViolation(_))
        ),
        "a non-vector key fails closed"
    );
    txn.rollback();
}

/// Exercises measured-write replacement, delegation, and failure boundaries.
pub(crate) async fn run() {
    run_measured_transaction_contract().await;
    run_shared_recorder_contract().await;
    run_planned_cache_contract().await;
}
