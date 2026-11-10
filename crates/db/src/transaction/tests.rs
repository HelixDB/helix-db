use super::*;
use crate::{encoding::v2::keys::scope::DataScope, index_lifecycle::graph_mutation};
use slatedb::config;

#[tokio::test]
async fn unpolled_read_future_allocations_are_admitted_and_released_with_the_future() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "unpolled-read-admission".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let key = graph_mutation::GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    fn contract(reader: &(impl DbReadOps + Sync), budget: &query_resources::Budget, key: &Bytes) {
        fn check<F>(budget: &query_resources::Budget, create: impl FnOnce() -> F) {
            let before = budget.available();
            let (future, allocation) = crate::allocation_testing::observe(create);
            assert!(allocation.bytes > 0);
            assert_eq!(
                before - budget.available(),
                allocation.bytes,
                "unpolled read future allocation must be admitted exactly"
            );
            drop(future);
            assert_eq!(budget.available(), before);
        }
        let keys = [key];
        let read = config::ReadOptions::default();
        let scan = config::ScanOptions::default();
        check(budget, || reader.get(key));
        check(budget, || reader.get_with_options(key, &read));
        check(budget, || reader.get_key_value(key));
        check(budget, || reader.get_key_value_with_options(key, &read));
        check(budget, || reader.multi_get(&keys));
        check(budget, || reader.multi_get_with_options(&keys, &read));
        check(budget, || reader.scan(..));
        check(budget, || reader.scan_with_options(.., &scan));
        check(budget, || reader.scan_prefix(key, ..));
        check(budget, || reader.scan_prefix_with_options(key, .., &scan));
    }
    contract(&transaction, &budget, &key);
    contract(&transaction.mutation_view(), &budget, &key);
    drop(transaction);
    db.close().await.unwrap();
}

#[tokio::test]
async fn rejected_read_futures_do_not_allocate_before_polling() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "rejected-read-future-admission".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let occupied = budget.reserve(budget.available()).unwrap();
    let key = graph_mutation::GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    async fn contract(reader: &(impl DbReadOps + Sync), key: &Bytes) {
        async fn check<
            T,
            F: std::future::Future<Output = std::result::Result<T, slatedb::Error>>,
        >(
            create: impl FnOnce() -> F,
        ) {
            let (future, allocation) = crate::allocation_testing::observe(create);
            assert_eq!(allocation.allocations, 0, "rejected read future allocated");
            let Err(error) = future.await else {
                panic!("exhausted read must fail")
            };
            assert!(is_admission_failure(&error));
        }
        let keys = [key];
        let read = config::ReadOptions::default();
        let scan = config::ScanOptions::default();
        check(|| reader.get(key)).await;
        check(|| reader.get_with_options(key, &read)).await;
        check(|| reader.get_key_value(key)).await;
        check(|| reader.get_key_value_with_options(key, &read)).await;
        check(|| reader.multi_get(&keys)).await;
        check(|| reader.multi_get_with_options(&keys, &read)).await;
        check(|| reader.scan(..)).await;
        check(|| reader.scan_with_options(.., &scan)).await;
        check(|| reader.scan_prefix(key, ..)).await;
        check(|| reader.scan_prefix_with_options(key, .., &scan)).await;
    }
    contract(&transaction, &key).await;
    contract(&transaction.mutation_view(), &key).await;
    drop(occupied);
    drop(transaction);
    db.close().await.unwrap();
}

async fn read_contract(
    reader: &(impl DbReadOps + Send + Sync),
    key: &Bytes,
    next: &Bytes,
    value: &Bytes,
) {
    assert_eq!(reader.get(key).await.unwrap().as_ref(), Some(value));
    assert_eq!(
        reader
            .get_with_options(key, &config::ReadOptions::default())
            .await
            .unwrap()
            .as_ref(),
        Some(value)
    );
    let pair = reader.get_key_value(key).await.unwrap().unwrap();
    assert_eq!((&pair.key, &pair.value), (key, value));
    let pair = reader
        .get_key_value_with_options(key, &config::ReadOptions::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((&pair.key, &pair.value), (key, value));
    let keys = [key, next, key];
    let expected = vec![Some(value.clone()), None, Some(value.clone())];
    assert_eq!(reader.multi_get(&keys).await.unwrap(), expected);
    assert_eq!(
        reader
            .multi_get_with_options(&keys, &config::ReadOptions::default())
            .await
            .unwrap(),
        expected
    );
    let mut scan = reader.scan(key.clone()..next.clone()).await.unwrap();
    assert_eq!(scan.next().await.unwrap().unwrap().value, *value);
    assert!(scan.next().await.unwrap().is_none());
    let mut scan = reader
        .scan_with_options(key.clone()..next.clone(), &config::ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(scan.next().await.unwrap().unwrap().value, *value);
    let mut scan = reader.scan_prefix(key, ..).await.unwrap();
    assert_eq!(scan.next().await.unwrap().unwrap().value, *value);
    let mut scan = reader
        .scan_prefix_with_options(key, .., &config::ScanOptions::default())
        .await
        .unwrap();
    assert_eq!(scan.next().await.unwrap().unwrap().value, *value);
}

#[tokio::test]
async fn owned_and_borrowed_read_entries_preserve_values_and_admission() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "transaction-read-contract".into(),
    })
    .await
    .unwrap();
    let key = graph_mutation::GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let next = graph_mutation::GraphEntity::node(2).property_key(DataScope::LegacyUnscoped);
    let row = graph_mutation::CanonicalPropertyRow::new(vec![
        crate::encoding::v2::values::property::Property::i64("value", 7),
    ]);
    db.inner_db().put(&key, row.encoded()).await.unwrap();
    for admitted in [false, true] {
        let budget = query_resources::Budget::new(1024 * 1024);
        let raw = db
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        read_contract(&raw.mutation_view(), &key, &next, row.encoded()).await;
        drop(raw);
        let transaction = Owned::begin(&db.inner_db(), admitted.then_some(&budget))
            .await
            .unwrap();
        read_contract(&transaction, &key, &next, row.encoded()).await;
        read_contract(&transaction.mutation_view(), &key, &next, row.encoded()).await;
        transaction.mark_read([&key]).unwrap();
        assert_eq!(budget.available() < 1024 * 1024, admitted);
        // Dropping all returned values does not release retained dependencies.
        drop(transaction);
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn rejected_read_does_not_reach_backend_or_hide_its_memory_error() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "transaction-read-rejected".into(),
    })
    .await
    .unwrap();
    let key = graph_mutation::GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let next = graph_mutation::GraphEntity::node(2).property_key(DataScope::LegacyUnscoped);
    let row = graph_mutation::CanonicalPropertyRow::new(Vec::new());
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let occupied = budget.reserve(budget.available()).unwrap();
    let error = transaction.get(&key).await.unwrap_err();
    assert!(is_admission_failure(&error));
    assert!(matches!(
        HelixDbError::from(error),
        HelixDbError::QueryMemoryLimitExceeded
    ));
    let error = transaction.get_key_value(&key).await.unwrap_err();
    let error = HelixDbError::Storage(error);
    assert_eq!(
        error.error_code(),
        helix_ast::error_code::QueryErrorCode::QueryMemoryLimitExceeded
    );
    let crate::cypher::Error::Query(error) = crate::cypher::Error::from(error) else {
        panic!("memory error must retain the Cypher resource category")
    };
    assert_eq!(error.detail, "memory_limit");
    assert!(is_admission_failure(
        &transaction.multi_get(&[&key, &next]).await.unwrap_err()
    ));
    assert!(transaction.scan(key.clone()..next.clone()).await.is_err());
    assert!(transaction.scan_prefix(&key, ..).await.is_err());
    assert!(is_admission_failure(
        &transaction.mark_read([&key]).unwrap_err()
    ));
    drop(occupied);
    // A concurrent write to the rejected read key must not create a phantom
    // dependency. Our unrelated write can still commit under serializable mode.
    db.inner_db().put(&key, row.encoded()).await.unwrap();
    transaction.put(&next, row.encoded()).unwrap();
    transaction.commit(CommitDurability::Durable).await.unwrap();
    assert_eq!(budget.available(), 1024 * 1024);
    assert_eq!(
        db.inner_db().get(next).await.unwrap().as_ref(),
        Some(row.encoded())
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn admitted_point_and_empty_range_reads_keep_serializable_conflicts() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "transaction-read-conflicts".into(),
    })
    .await
    .unwrap();
    let row = graph_mutation::CanonicalPropertyRow::new(Vec::new());
    for mode in 0..3 {
        let budget = query_resources::Budget::new(1024 * 1024);
        let key =
            graph_mutation::GraphEntity::node(10 * mode).property_key(DataScope::LegacyUnscoped);
        let next = graph_mutation::GraphEntity::node(10 * mode + 1)
            .property_key(DataScope::LegacyUnscoped);
        let own = graph_mutation::GraphEntity::node(10 * mode + 2)
            .property_key(DataScope::LegacyUnscoped);
        let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        match mode {
            0 => assert!(transaction.get(&key).await.unwrap().is_none()),
            1 => {
                drop(transaction.scan(key.clone()..next).await.unwrap());
            }
            2 => transaction.mark_read([&key, &key]).unwrap(),
            _ => unreachable!(),
        }
        db.inner_db().put(&key, row.encoded()).await.unwrap();
        transaction.put(&own, row.encoded()).unwrap();
        assert_eq!(
            transaction
                .commit(CommitDurability::Durable)
                .await
                .unwrap_err()
                .kind(),
            slatedb::ErrorKind::Transaction
        );
        assert!(db.inner_db().get(own).await.unwrap().is_none());
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn backend_read_failure_retains_the_conservative_ledger_until_drop() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "transaction-read-closed".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let before = budget.available();
    db.close().await.unwrap();
    let key = graph_mutation::GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let error = transaction.get(key).await.unwrap_err();
    assert!(!is_admission_failure(&error));
    assert!(matches!(
        HelixDbError::from(error),
        HelixDbError::Storage(_)
    ));
    assert!(budget.available() < before);
    drop(transaction);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn storage_admission_classification_uses_the_private_source_type() {
    let fake = slatedb::Error::unavailable("transaction read admission failed".into());
    assert!(!is_admission_failure(&fake));
    assert!(matches!(HelixDbError::from(fake), HelixDbError::Storage(_)));
    let nested = slatedb::Error::invalid("outer".into())
        .with_source(Box::new(storage_error(AdmissionFailure)));
    assert!(is_admission_failure(&nested));
    assert!(matches!(
        HelixDbError::from(nested),
        HelixDbError::QueryMemoryLimitExceeded
    ));
}

#[tokio::test]
async fn fresh_transaction_admission_releases_on_open_failure() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "transaction-open-admission".into(),
    })
    .await
    .unwrap();
    assert!(matches!(
        Owned::begin(&db.inner_db(), Some(&query_resources::Budget::new(0))).await,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    db.close().await.unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    assert!(Owned::begin(&db.inner_db(), Some(&budget)).await.is_err());
    assert_eq!(budget.available(), 1024 * 1024);
}

#[tokio::test]
async fn detached_completion_retains_read_admission_and_abort_releases_it() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "transaction-read-owner".into(),
    })
    .await
    .unwrap();
    let keys: Vec<_> = (0..128)
        .map(|id| graph_mutation::GraphEntity::node(id).property_key(DataScope::LegacyUnscoped))
        .collect();
    let row = graph_mutation::CanonicalPropertyRow::new(Vec::new());
    for commit in [false, true] {
        let budget = query_resources::Budget::new(1024 * 1024);
        let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        transaction.multi_get(&keys).await.unwrap();
        let written = graph_mutation::GraphEntity::node(if commit { 1001 } else { 1000 })
            .property_key(DataScope::LegacyUnscoped);
        transaction
            .put_bytes(written.clone(), row.encoded().clone())
            .unwrap();
        let retained = budget.available();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        if commit {
            let task = db
                .inner
                .commit_completions
                .run(async move {
                    wait.await.unwrap();
                    transaction.commit(CommitDurability::Durable).await.unwrap();
                })
                .unwrap();
            drop(task);
            assert_eq!(budget.available(), retained);
            release.send(()).unwrap();
            db.inner.commit_completions.seal_and_wait().await;
            assert!(db.inner_db().get(&written).await.unwrap().is_some());
        } else {
            let task = tokio::spawn(async move {
                let _transaction = transaction;
                let _ = wait.await;
            });
            assert_eq!(budget.available(), retained);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(release);
            assert!(db.inner_db().get(&written).await.unwrap().is_none());
        }
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

/// The put guard classifies the key first: any row deltas never merge into
/// returns before the value is read, admitted or allocated for, so property
/// puts pay only the key classification.
#[tokio::test]
async fn puts_to_rows_without_merges_skip_validation_and_admission() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "put-guard-early-return".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(1024 * 1024);
    let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let property = graph_mutation::GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    // Undecodable as any membership row, and no admission is left to take.
    let value = Bytes::from(vec![0xAB; 64 * 1024]);
    let occupied = budget.reserve(budget.available()).unwrap();
    let view = transaction.mutation_view();
    let (result, allocations) =
        crate::allocation_testing::observe(|| validate_put(view.tracking, &property, &value));
    result.unwrap();
    assert_eq!(allocations.bytes, 0);
    Mutation::put(&transaction, &property, &value).unwrap();
    Mutation::put_bytes(&transaction, property.clone(), value.clone()).unwrap();
    drop(occupied);
    assert_eq!(
        transaction.raw.get(&property).await.unwrap(),
        Some(value.clone())
    );
    drop(transaction);
    db.close().await.unwrap();
}

/// A membership put admits its decode's bound before decoding, releases it
/// afterwards, and reports a refused admission as a memory-limit error rather
/// than an invalid value.
#[tokio::test]
async fn budgeted_membership_puts_admit_the_decode_before_running_it() {
    use crate::encoding::v2::keys::{AdjacencyKey, DataKey, DataKeyKind, EdgePairIndexKey};
    use crate::encoding::v2::values::adjacency;

    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "put-guard-admission".into(),
    })
    .await
    .unwrap();
    let bitmap = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(1, 2)),
    }
    .to_bytes();
    let adjacency_key = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(1)),
    }
    .to_bytes();
    let ids = roaring::RoaringTreemap::from_iter((0..4096_u64).map(|id| id << 32));
    let mut edges = adjacency::Edges::new();
    ids.iter().for_each(|id| edges.add_out(id));
    for (key, value) in [
        (
            bitmap,
            crate::encoding::v2::values::indexes::SecondaryEqualityValue::encode_ids(&ids),
        ),
        (adjacency_key, adjacency::encode_edges(&edges)),
    ] {
        let row = crate::merge_operator::MembershipRow::of(&key).unwrap();
        let bound = row.base_allocation_bound(&value).unwrap();
        let budget = query_resources::Budget::new(64 * 1024 * 1024);
        let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        let view = transaction.mutation_view();
        let initial = budget.available();
        let (result, allocations) =
            crate::allocation_testing::observe(|| validate_put(view.tracking, &key, &value));
        result.unwrap();
        assert!(allocations.peak_bytes > 0 && allocations.peak_bytes <= bound);
        assert_eq!(
            budget.available(),
            initial,
            "the decode admission is transient"
        );

        let occupied = budget.reserve(initial - bound + 1).unwrap();
        let (result, allocations) =
            crate::allocation_testing::observe(|| validate_put(view.tracking, &key, &value));
        let error = result.unwrap_err();
        assert!(is_admission_failure(&error), "{error:?}");
        assert!(matches!(
            HelixDbError::from(error),
            HelixDbError::QueryMemoryLimitExceeded
        ));
        // Only the admission error's message: the decode would allocate
        // per container, far beyond this.
        assert!(
            allocations.peak_bytes <= 256,
            "decoded before admission: {allocations:?}"
        );
        assert!(Mutation::put_bytes(&transaction, key.clone(), value.clone()).is_err());
        drop(occupied);
        Mutation::put_bytes(&transaction, key.clone(), value.clone()).unwrap();
        assert_eq!(transaction.raw.get(&key).await.unwrap(), Some(value));

        let error = Mutation::put(&transaction, &key, b"corrupt").unwrap_err();
        assert!(!is_admission_failure(&error));
        assert_eq!(error.kind(), slatedb::ErrorKind::Invalid);
        assert_eq!(
            HelixDbError::from(error).error_code(),
            helix_ast::error_code::QueryErrorCode::StorageError
        );
        drop(transaction);
        assert_eq!(budget.available(), 64 * 1024 * 1024);
    }
    db.close().await.unwrap();
}
