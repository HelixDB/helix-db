use super::*;
use crate::{
    encoding::v2::{keys::scope::DataScope, values::property::Property},
    index_lifecycle::graph_mutation::{CanonicalPropertyRow, GraphEntity},
    query_resources,
};
use slatedb::{config, object_store::memory::InMemory, DbReadOps};

#[tokio::test]
async fn native_read_frames_reserve_before_first_poll() {
    let db = slatedb::Db::open("native-unpolled-read-admission", Arc::new(InMemory::new()))
        .await
        .unwrap();
    for admitted in [false, true] {
        let budget = query_resources::Budget::new(1024 * 1024);
        let transaction = crate::transaction::Owned::begin(&db, admitted.then_some(&budget))
            .await
            .unwrap();
        let measured = MeasuredVectorTransaction::new(&transaction);
        measured.reads_until_failure.store(0, Ordering::Release);
        let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
        fn check<F>(budget: &query_resources::Budget, admitted: bool, create: impl FnOnce() -> F) {
            let before = budget.available();
            let (future, allocation) = crate::allocation_testing::observe(create);
            assert_eq!(allocation.allocations, 1, "one frame per native read");
            assert!(allocation.bytes > 0);
            assert_eq!(
                before - budget.available(),
                if admitted { allocation.bytes } else { 0 }
            );
            drop(future);
            assert_eq!(budget.available(), before);
        }
        let keys = [&key];
        let read = config::ReadOptions::default();
        let scan = config::ScanOptions::default();
        check(&budget, admitted, || measured.get(&key));
        check(&budget, admitted, || measured.get_with_options(&key, &read));
        check(&budget, admitted, || measured.get_key_value(&key));
        check(&budget, admitted, || {
            measured.get_key_value_with_options(&key, &read)
        });
        check(&budget, admitted, || measured.multi_get(&keys));
        check(&budget, admitted, || {
            measured.multi_get_with_options(&keys, &read)
        });
        check(&budget, admitted, || measured.scan(..));
        check(&budget, admitted, || measured.scan_with_options(.., &scan));
        check(&budget, admitted, || measured.scan_prefix(&key, ..));
        check(&budget, admitted, || {
            measured.scan_prefix_with_options(&key, .., &scan)
        });
        assert_eq!(measured.reads_until_failure.load(Ordering::Acquire), 0);
        drop(measured);
        drop(transaction);
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn native_read_frames_reject_without_allocation() {
    let db = slatedb::Db::open("native-rejected-read-admission", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = crate::transaction::Owned::begin(&db, Some(&budget))
        .await
        .unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    measured.reads_until_failure.store(0, Ordering::Release);
    let occupied = budget.reserve(budget.available()).unwrap();
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    async fn check<T, F: std::future::Future<Output = Result<T, slatedb::Error>>>(
        create: impl FnOnce() -> F,
    ) {
        let (future, allocation) = crate::allocation_testing::observe(create);
        assert_eq!(
            allocation.allocations, 0,
            "rejected native read frame allocated"
        );
        let Err(error) = future.await else {
            panic!("exhausted read must fail")
        };
        assert!(crate::transaction::is_admission_failure(&error));
    }
    let keys = [&key];
    let read = config::ReadOptions::default();
    let scan = config::ScanOptions::default();
    check(|| measured.get(&key)).await;
    check(|| measured.get_with_options(&key, &read)).await;
    check(|| measured.get_key_value(&key)).await;
    check(|| measured.get_key_value_with_options(&key, &read)).await;
    check(|| measured.multi_get(&keys)).await;
    check(|| measured.multi_get_with_options(&keys, &read)).await;
    check(|| measured.scan(..)).await;
    check(|| measured.scan_with_options(.., &scan)).await;
    check(|| measured.scan_prefix(&key, ..)).await;
    check(|| measured.scan_prefix_with_options(&key, .., &scan)).await;
    assert_eq!(measured.reads_until_failure.load(Ordering::Acquire), 0);
    drop(occupied);
    let before = budget.available();
    assert!(measured
        .get(&key)
        .await
        .unwrap_err()
        .to_string()
        .contains("injected measured vector read failure"));
    assert_eq!(
        budget.available(),
        before,
        "failed hook must not register a dependency"
    );
    assert_eq!(
        measured.reads_until_failure.load(Ordering::Acquire),
        NO_INJECTED_FAILURE
    );
    assert!(measured.get(&key).await.unwrap().is_none());
    assert!(
        budget.available() < before,
        "successful read retains its dependency"
    );
    drop(measured);
    drop(transaction);
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}

#[tokio::test]
async fn native_read_entries_preserve_values_and_invoke_one_hook_each() {
    let db = slatedb::Db::open("native-read-value-contract", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let next = GraphEntity::node(2).property_key(DataScope::LegacyUnscoped);
    let row = CanonicalPropertyRow::new(vec![Property::i64("value", 7)]);
    for admitted in [false, true] {
        let budget = query_resources::Budget::new(1024 * 1024);
        let transaction = crate::transaction::Owned::begin(&db, admitted.then_some(&budget))
            .await
            .unwrap();
        let measured = VectorWriteRecorder::new().bind(&transaction);
        measured
            .put_bytes(key.clone(), row.encoded().clone())
            .unwrap();
        let measurement = measured.measurement().unwrap();
        measured.reads_until_failure.store(10, Ordering::Release);
        let read = config::ReadOptions::default();
        let scan = config::ScanOptions::default();
        assert_eq!(
            measured.get(&key).await.unwrap().as_ref(),
            Some(row.encoded())
        );
        assert_eq!(
            measured
                .get_with_options(&key, &read)
                .await
                .unwrap()
                .as_ref(),
            Some(row.encoded())
        );
        let pair = measured.get_key_value(&key).await.unwrap().unwrap();
        assert_eq!((&pair.key, &pair.value), (&key, row.encoded()));
        let pair = measured
            .get_key_value_with_options(&key, &read)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((&pair.key, &pair.value), (&key, row.encoded()));
        let keys = [&key, &next, &key];
        let expected = vec![
            Some(row.encoded().clone()),
            None,
            Some(row.encoded().clone()),
        ];
        assert_eq!(measured.multi_get(&keys).await.unwrap(), expected);
        assert_eq!(
            measured.multi_get_with_options(&keys, &read).await.unwrap(),
            expected
        );
        for mut cursor in [
            measured.scan(key.clone()..next.clone()).await.unwrap(),
            measured
                .scan_with_options(key.clone()..next.clone(), &scan)
                .await
                .unwrap(),
            measured.scan_prefix(&key, ..).await.unwrap(),
            measured
                .scan_prefix_with_options(&key, .., &scan)
                .await
                .unwrap(),
        ] {
            let pair = cursor.next().await.unwrap().unwrap();
            assert_eq!((&pair.key, &pair.value), (&key, row.encoded()));
            assert!(cursor.next().await.unwrap().is_none());
        }
        assert_eq!(measured.reads_until_failure.load(Ordering::Acquire), 0);
        assert!(measured
            .get(&key)
            .await
            .unwrap_err()
            .to_string()
            .contains("injected measured vector read failure"));
        assert_eq!(
            measured.reads_until_failure.load(Ordering::Acquire),
            NO_INJECTED_FAILURE
        );
        assert_eq!(
            measured.measurement().unwrap(),
            measurement,
            "reads cannot alter write measurement"
        );
        drop(measured);
        drop(transaction);
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

/// Process-global benchmark counters require this contract to run alone.
#[cfg(feature = "production-coverage")]
#[tokio::test]
#[ignore = "run alone: native_read_telemetry_counts_each_entry_once --ignored"]
async fn native_read_telemetry_counts_each_entry_once() {
    let db = slatedb::Db::open("native-read-telemetry", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = crate::transaction::Owned::begin(&db, Some(&budget))
        .await
        .unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let keys = [&key, &key];
    let read = config::ReadOptions::default();
    let scan = config::ScanOptions::default();
    crate::search::vector::reset_benchmark_telemetry();
    macro_rules! check {
        ($call:expr) => {{
            measured.fail_read_after(0);
            assert!($call.await.is_err());
            drop($call.await.unwrap());
        }};
    }
    check!(measured.get(&key));
    check!(measured.get_with_options(&key, &read));
    check!(measured.get_key_value(&key));
    check!(measured.get_key_value_with_options(&key, &read));
    check!(measured.multi_get(&keys));
    check!(measured.multi_get_with_options(&keys, &read));
    check!(measured.scan(..));
    check!(measured.scan_with_options(.., &scan));
    check!(measured.scan_prefix(&key, ..));
    check!(measured.scan_prefix_with_options(&key, .., &scan));
    let occupied = budget.reserve(budget.available()).unwrap();
    assert!(crate::transaction::is_admission_failure(
        &measured.get(&key).await.unwrap_err()
    ));
    let counts = crate::search::vector::benchmark_telemetry_snapshot();
    assert_eq!(counts.point_get_calls, 4);
    assert_eq!(counts.multi_get_calls, 2);
    assert_eq!(counts.multi_get_keys, 4);
    assert_eq!(counts.scan_calls, 4);
    assert_eq!(counts.put_calls, 0);
    assert_eq!(counts.delete_calls, 0);
    assert_eq!(counts.staged_write_bytes, 0);
    drop(occupied);
    drop(measured);
    drop(transaction);
    db.close().await.unwrap();
}
