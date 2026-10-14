use super::*;
use crate::{
    encoding::v2::keys::scope::DataScope, index_lifecycle::graph_mutation::GraphEntity,
    query_resources,
};
use std::sync::Arc;

#[tokio::test]
async fn vector_dispatch_does_not_add_an_unadmitted_frame() {
    let db = slatedb::Db::open(
        "vector-dispatch-admission",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = crate::transaction::Owned::begin(&db, Some(&budget))
        .await
        .unwrap();
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    for view in [
        VectorReadView::<crate::transaction::Owned>::transaction(&transaction),
        VectorReadView::snapshot(&transaction),
    ] {
        fn check<F>(budget: &query_resources::Budget, create: impl FnOnce() -> F) {
            let before = budget.available();
            let (future, allocation) = crate::allocation_testing::observe(create);
            assert_eq!(allocation.allocations, 1);
            assert_eq!(
                before - budget.available(),
                allocation.bytes,
                "dispatch must forward the admitted frame"
            );
            drop(future);
            assert_eq!(budget.available(), before);
        }
        let keys = [&key];
        let read = slatedb::config::ReadOptions::default();
        let scan = slatedb::config::ScanOptions::default();
        check(&budget, || view.get(&key));
        check(&budget, || view.get_with_options(&key, &read));
        check(&budget, || view.get_key_value(&key));
        check(&budget, || view.get_key_value_with_options(&key, &read));
        check(&budget, || view.multi_get(&keys));
        check(&budget, || view.multi_get_with_options(&keys, &read));
        check(&budget, || view.scan(..));
        check(&budget, || view.scan_with_options(.., &scan));
        check(&budget, || view.scan_prefix(&key, ..));
        check(&budget, || view.scan_prefix_with_options(&key, .., &scan));
    }
    drop(transaction);
    db.close().await.unwrap();
}

#[tokio::test]
async fn vector_dispatch_rejects_without_allocation() {
    let db = slatedb::Db::open(
        "vector-dispatch-refusal",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = crate::transaction::Owned::begin(&db, Some(&budget))
        .await
        .unwrap();
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let occupied = budget.reserve(budget.available()).unwrap();
    for view in [
        VectorReadView::<crate::transaction::Owned>::transaction(&transaction),
        VectorReadView::snapshot(&transaction),
    ] {
        async fn check<T, F: std::future::Future<Output = Result<T, slatedb::Error>>>(
            create: impl FnOnce() -> F,
        ) {
            let (future, allocation) = crate::allocation_testing::observe(create);
            assert_eq!(
                allocation.allocations, 0,
                "dispatch must not allocate before refusal"
            );
            let Err(error) = future.await else {
                panic!("exhausted read must fail")
            };
            assert!(crate::transaction::is_admission_failure(&error));
        }
        let keys = [&key];
        let read = slatedb::config::ReadOptions::default();
        let scan = slatedb::config::ScanOptions::default();
        check(|| view.get(&key)).await;
        check(|| view.get_with_options(&key, &read)).await;
        check(|| view.get_key_value(&key)).await;
        check(|| view.get_key_value_with_options(&key, &read)).await;
        check(|| view.multi_get(&keys)).await;
        check(|| view.multi_get_with_options(&keys, &read)).await;
        check(|| view.scan(..)).await;
        check(|| view.scan_with_options(.., &scan)).await;
        check(|| view.scan_prefix(&key, ..)).await;
        check(|| view.scan_prefix_with_options(&key, .., &scan)).await;
    }
    drop(occupied);
    drop(transaction);
    db.close().await.unwrap();
}

#[tokio::test]
async fn vector_dispatch_preserves_snapshot_and_transaction_overlays() {
    use crate::encoding::v2::values::property::Property;
    use crate::index_lifecycle::graph_mutation::CanonicalPropertyRow;
    let db = slatedb::Db::open(
        "vector-dispatch-isolation",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
    )
    .await
    .unwrap();
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let missing = GraphEntity::node(2).property_key(DataScope::LegacyUnscoped);
    let original = CanonicalPropertyRow::new(vec![Property::i64("value", 7)]);
    let updated = CanonicalPropertyRow::new(vec![Property::i64("value", 9)]);
    db.put(&key, original.encoded()).await.unwrap();
    let snapshot = crate::transaction::Owned::begin(&db, None).await.unwrap();
    let transaction = crate::transaction::Owned::begin(&db, None).await.unwrap();
    crate::transaction::Mutation::put_bytes(&transaction, key.clone(), updated.encoded().clone())
        .unwrap();
    for (view, expected) in [
        (VectorReadView::transaction(&transaction), updated.encoded()),
        (VectorReadView::snapshot(&snapshot), original.encoded()),
    ] {
        assert_eq!(view.get(&key).await.unwrap().as_ref(), Some(expected));
        assert_eq!(
            view.get_key_value(&key).await.unwrap().unwrap().value,
            *expected
        );
        assert_eq!(
            view.multi_get(&[&key, &missing, &key]).await.unwrap(),
            vec![Some(expected.clone()), None, Some(expected.clone())]
        );
        let mut cursor = view.scan_prefix(&key, ..).await.unwrap();
        assert_eq!(cursor.next().await.unwrap().unwrap().value, *expected);
        assert!(cursor.next().await.unwrap().is_none());
        assert!(view
            .scan(missing.clone()..)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .is_none());
    }
    drop(transaction);
    drop(snapshot);
    db.close().await.unwrap();
}
