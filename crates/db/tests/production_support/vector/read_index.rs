//! Production contracts for request-scoped vector reads.
//!
//! This feature-gated child module exercises every `DbReadOps` delegation arm
//! for transaction and snapshot views plus descriptor-bound reader
//! construction. Cache access is proven by exact generation and snapshot
//! sequence; every mismatch falls back to storage without changing persisted
//! rows or their codecs. No descriptorless reader constructor exists.

use std::num::NonZeroU64;
use std::sync::Arc;

use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReadOps, IsolationLevel};

use super::*;
use crate::encoding::keys::tenant::DataScope;
use crate::search::vector::distance::{Cosine, Euclidean};
use crate::search::vector::{
    VectorDimension, VectorGenerationIdentity, VectorMemoryStore, VectorReadView,
};

/// Exercises transaction/snapshot delegation and exact-generation cache leases.
pub(crate) async fn run() {
    let db = Db::open("production-vector-read-boundary", Arc::new(InMemory::new()))
        .await
        .unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    txn.put(b"alpha", b"one").unwrap();
    txn.put(b"beta", b"two").unwrap();
    {
        let read = VectorReadView::<slatedb::DbSnapshot>::transaction(&txn);
        assert_eq!(read.get(b"alpha").await.unwrap().unwrap(), b"one"[..]);
        assert!(read.get_key_value(b"beta").await.unwrap().is_some());
        let values = read
            .multi_get(&[&b"alpha"[..], &b"missing"[..]])
            .await
            .unwrap();
        assert_eq!(values[0].as_deref(), Some(&b"one"[..]));
        assert!(values[1].is_none());
        let mut scan = read.scan(..).await.unwrap();
        assert!(scan.next().await.unwrap().is_some());
        let mut prefix = read.scan_prefix(b"a", ..).await.unwrap();
        assert!(prefix.next().await.unwrap().is_some());
    }
    txn.commit().await.unwrap();

    let snapshot = db.snapshot().await.unwrap();
    let read = VectorReadView::snapshot(snapshot.as_ref());
    assert_eq!(read.get(b"alpha").await.unwrap().unwrap(), b"one"[..]);
    assert!(read.get_key_value(b"beta").await.unwrap().is_some());
    let values = read
        .multi_get(&[&b"alpha"[..], &b"missing"[..]])
        .await
        .unwrap();
    assert_eq!(values[0].as_deref(), Some(&b"one"[..]));
    assert!(values[1].is_none());
    let mut scan = read.scan(..).await.unwrap();
    assert!(scan.next().await.unwrap().is_some());
    let mut prefix = read.scan_prefix(b"a", ..).await.unwrap();
    assert!(prefix.next().await.unwrap().is_some());

    let handle = ValidatedVectorGenerationHandle::create_current::<Cosine>(
        VectorGenerationIdentity::try_new(
            DataScope::LegacyUnscoped,
            4,
            "production-managed-read".to_string(),
            40,
            NonZeroU64::MIN,
            1,
            crate::index_v2::IndexElementKind::Node,
            VectorDimension::try_new(3).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let registry = VectorCacheRegistry::default();
    let (entry, owns_hydration) = registry.entry_for(&handle);
    assert!(owns_hydration);
    assert!(entry.finish_hydration(Arc::new(VectorMemoryStore::new(
        DataScope::LegacyUnscoped,
        handle.physical_index_id(),
        9,
    ))));

    let simhashers = Arc::new(super::super::SimHasherRegistry::default());
    let exact = ValidatedVectorReadIndex::<Cosine>::managed(
        &handle,
        &registry,
        Arc::clone(&simhashers),
        VectorReadVisibility::Comparable(9),
    )
    .unwrap();
    assert!(exact._cache_lease.is_some());
    assert!(exact.get_metadata(&read).await.unwrap().is_none());
    assert!(matches!(
        exact
            .search(&read, &[1.0, 0.0, 0.0], &SearchParams::new(1).unwrap())
            .await,
        Err(HelixDbError::IndexNotFound(_))
    ));

    let stale = ValidatedVectorReadIndex::<Cosine>::managed(
        &handle,
        &registry,
        Arc::clone(&simhashers),
        VectorReadVisibility::Comparable(10),
    )
    .unwrap();
    assert!(stale._cache_lease.is_none());
    let unavailable = ValidatedVectorReadIndex::<Cosine>::managed(
        &handle,
        &registry,
        Arc::clone(&simhashers),
        VectorReadVisibility::Unavailable,
    )
    .unwrap();
    assert!(unavailable._cache_lease.is_none());
    assert!(matches!(
        ValidatedVectorReadIndex::<Euclidean>::managed(
            &handle,
            &registry,
            Arc::clone(&simhashers),
            VectorReadVisibility::Comparable(9),
        ),
        Err(super::super::VectorGenerationValidationError::MetricMismatch)
    ));
}
