use super::*;
use crate::{
    allocation_testing,
    encoding::v2::keys::{scope::DataScope, AdjacencyKey, DataKeyKind, EdgePairIndexKey},
    transaction::{Mutation, Owned},
};

fn bitmap_key(id: u64) -> DataKey<'static> {
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(id, id)),
    }
}

#[test]
fn encoded_key_aliases_keep_admission_until_the_last_owner_drops() {
    let encoded = bitmap_key(7).to_bytes();
    for key in [Key::Typed(bitmap_key(7)), Key::Encoded(&encoded)] {
        let budget = Budget::new(4096);
        let (bytes, allocations) =
            allocation_testing::observe(|| key.encode(Some(&budget)).unwrap());
        assert_eq!(bytes, encoded);
        assert!(4096 - budget.available() >= allocations.bytes);
        let slice = bytes.slice(1..);
        let clone = bytes.clone();
        drop(bytes);
        drop(clone);
        assert!(budget.available() < 4096);
        drop(slice);
        assert_eq!(budget.available(), 4096);
    }
    for key in [Key::Typed(bitmap_key(7)), Key::Encoded(&encoded)] {
        let budget = Budget::new(0);
        let (result, allocations) = allocation_testing::observe(|| key.encode(Some(&budget)));
        assert!(matches!(
            result,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(allocations.allocations, 0);
    }
    assert_eq!(Key::Typed(bitmap_key(7)).encode(None).unwrap(), encoded);
    assert_eq!(Key::Encoded(&encoded).encode(None).unwrap(), encoded);
}

#[test]
fn history_admission_is_monotone_atomic_and_released_with_the_owner() {
    let budget = Budget::new(1024 * 1024);
    let tracker = Tracker::new(&budget).unwrap();
    let empty = budget.available();
    tracker.retain(Totals::default()).unwrap();
    assert_eq!(budget.available(), empty);
    let entry = Totals {
        entries: 1,
        keys: 17,
        operands: 32,
        tokens: 8,
    };
    for epoch in 1..=128 {
        tracker.retain(entry).unwrap();
        let state = tracker.state.lock();
        assert_eq!(state.totals.entries, epoch);
        assert_eq!(state.totals.tokens, epoch * 8);
        assert_eq!(
            1024 * 1024 - budget.available(),
            state.totals.history_bytes()
        );
    }
    let held = budget.reserve(budget.available()).unwrap();
    let (result, allocations) = allocation_testing::observe(|| tracker.retain(entry));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocations.allocations, 0);
    assert_eq!(tracker.state.lock().totals.entries, 128);
    drop(held);
    drop(tracker);
    assert_eq!(budget.available(), 1024 * 1024);
    assert!(Tracker::new(&Budget::new(0)).is_err());
}

#[test]
fn arithmetic_saturates_bounds_and_rejects_total_overflow() {
    for total in [
        Totals {
            entries: usize::MAX,
            ..Totals::default()
        },
        Totals {
            keys: usize::MAX,
            entries: 1,
            ..Totals::default()
        },
        Totals {
            operands: usize::MAX,
            entries: 1,
            ..Totals::default()
        },
        Totals {
            tokens: usize::MAX,
            entries: 1,
            ..Totals::default()
        },
    ] {
        assert_eq!(total.history_bytes(), usize::MAX);
        assert!(total.plus(total).is_err());
    }
    assert_eq!(
        Batch::construction_bytes(usize::MAX, Totals::default()),
        usize::MAX
    );
    assert_eq!(
        Batch::construction_bytes(
            1,
            Totals {
                operands: usize::MAX,
                ..Totals::default()
            }
        ),
        usize::MAX
    );
    assert_eq!(
        Batch::construction_bytes(
            1,
            Totals {
                tokens: usize::MAX,
                ..Totals::default()
            }
        ),
        usize::MAX
    );
}

#[tokio::test]
async fn retained_bound_covers_actual_backend_token_unions_and_operation_growth() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-backend-allocation-bound".into(),
    })
    .await
    .unwrap();
    for distinct_keys in [false, true] {
        for overlapping_tokens in [false, true] {
            for (epochs, members) in [(1, 1), (2, 4096), (9, 64), (128, 32), (1024, 1)] {
                let raw = db
                    .inner_db()
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .unwrap();
                let keys: Vec<_> = (0..epochs)
                    .map(|epoch| bitmap_key(if distinct_keys { epoch } else { 0 }).to_bytes())
                    .collect();
                let mut delta = equality::BitmapMembershipDelta::default();
                delta.add(1);
                let operand = delta.encode();
                // The unchecked native method reaches exactly the same pinned
                // backend staging implementation, without hidden validation I/O.
                // All staged heap owners start inside this observation; only
                // borrowed input slices and the empty transaction predate it.
                let (_, allocation) = allocation_testing::observe(|| {
                    for (epoch, key) in keys.iter().enumerate() {
                        let start = if overlapping_tokens {
                            0
                        } else {
                            epoch as u128 * members
                        };
                        raw.merge_disjoint_tokens(
                            key.as_ref(),
                            start..start + members,
                            operand.as_ref(),
                        )
                        .unwrap();
                    }
                });
                let totals = Totals {
                    entries: epochs as usize,
                    keys: keys.iter().map(Bytes::len).sum(),
                    operands: epochs as usize * operand.len(),
                    tokens: epochs as usize * members as usize,
                };
                assert!(totals.history_bytes() >= allocation.peak_bytes,
                    "epochs={epochs}, members={members}, distinct={distinct_keys}, overlap={overlapping_tokens}, peak={}, bound={}",
                    allocation.peak_bytes, totals.history_bytes());
            }
        }
    }
    // The public batch uses the same operation B-tree/SmallVec layout. Its
    // clone independently exercises commit metadata copies; payloads are shared.
    for distinct in [false, true] {
        let keys: Vec<_> = (0..128)
            .map(|id| bitmap_key(if distinct { id } else { 0 }).to_bytes())
            .collect();
        let payload = [7_u8; 1024];
        let (_, allocation) = allocation_testing::observe(|| {
            let mut batch = slatedb::WriteBatch::new();
            for key in &keys {
                batch.merge(key.as_ref(), payload);
            }
            let cloned = batch.clone();
            std::hint::black_box((&batch, &cloned));
        });
        let totals = Totals {
            entries: keys.len(),
            keys: keys.iter().map(Bytes::len).sum(),
            operands: keys.len() * payload.len(),
            tokens: 0,
        };
        assert!(totals.history_bytes() >= allocation.peak_bytes);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn batch_and_entry_rejection_precede_encoding_and_backend_writes() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-rejection".into(),
    })
    .await
    .unwrap();
    let budget = Budget::new(1024 * 1024);
    let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let initial = budget.available();
    let held = budget.reserve(initial).unwrap();
    let (result, allocations) = allocation_testing::observe(|| transaction.merge_batch(1));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocations.allocations, 0);
    drop(held);
    let mut batch = transaction.merge_batch(1).unwrap();
    let held = budget.reserve(budget.available()).unwrap();
    let (result, allocations) = allocation_testing::observe(|| {
        batch.push(Key::Typed(bitmap_key(1)), 1, std::iter::once(1), || {
            panic!("encoder ran before admission")
        })
    });
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocations.allocations, 0);
    assert!(batch.entries.is_empty());
    drop(held);
    let mut delta = equality::BitmapMembershipDelta::default();
    delta.add(7);
    drop(batch.bitmap(Key::Typed(bitmap_key(1)), &delta).unwrap());
    let held = budget.reserve(budget.available()).unwrap();
    assert!(matches!(
        batch.stage().await,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(
        transaction
            .merges
            .as_ref()
            .unwrap()
            .state
            .lock()
            .totals
            .entries,
        0
    );
    assert!(transaction
        .raw
        .get(bitmap_key(1).to_bytes())
        .await
        .unwrap()
        .is_none());
    drop(held);
    assert_eq!(budget.available(), initial);
    drop(transaction);
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}

#[tokio::test]
async fn canonical_bitmap_and_adjacency_batches_preserve_values_and_epoch_history() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-epochs".into(),
    })
    .await
    .unwrap();
    for admitted in [false, true] {
        let budget = Budget::new(16 * 1024 * 1024);
        let transaction = Owned::begin(&db.inner_db(), admitted.then_some(&budget))
            .await
            .unwrap();
        let mut expected = roaring::RoaringTreemap::new();
        let mut previous = budget.available();
        let key = bitmap_key(u64::from(admitted)).to_bytes();
        for epoch in 0..16 {
            let mut delta = equality::BitmapMembershipDelta::default();
            delta.add(epoch);
            expected.insert(epoch);
            if epoch > 0 {
                delta.remove(epoch - 1);
                expected.remove(epoch - 1);
            }
            let mut edges = adjacency::AdjacencyMembershipDelta::default();
            edges.add_out(epoch);
            edges.add_in(epoch);
            let adjacency_key = DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::Adjacency(AdjacencyKey::new(u64::from(admitted))),
            };
            let mut batch = transaction.merge_batch(2).unwrap();
            drop(batch.bitmap(Key::Encoded(&key), &delta).unwrap());
            let edge_key = batch.adjacency(Key::Typed(adjacency_key), &edges).unwrap();
            batch.stage().await.unwrap();
            let value = transaction.raw.get(&key).await.unwrap().unwrap();
            assert_eq!(
                equality::SecondaryEqualityBitmapValue::decode(&value)
                    .unwrap()
                    .ids(),
                &expected
            );
            let edges =
                adjacency::decode_edges(&transaction.raw.get(&edge_key).await.unwrap().unwrap())
                    .unwrap();
            assert_eq!(
                edges.iter_out().collect::<Vec<_>>(),
                (0..=epoch).collect::<Vec<_>>()
            );
            assert_eq!(
                edges.iter_in().collect::<Vec<_>>(),
                (0..=epoch).collect::<Vec<_>>()
            );
            drop(edge_key);
            if admitted {
                assert!(budget.available() < previous);
            }
            previous = budget.available();
        }
        transaction.commit().await.unwrap();
        assert_eq!(budget.available(), 16 * 1024 * 1024);
        assert!(db.inner_db().get(&key).await.unwrap().is_some());
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn checked_backend_errors_leave_no_partial_writes_and_retain_admission() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-atomic-validation".into(),
    })
    .await
    .unwrap();
    let budget = Budget::new(1024 * 1024);
    for duplicate in [false, true] {
        if !duplicate {
            db.inner_db()
                .put(bitmap_key(2).to_bytes(), b"corrupt")
                .await
                .unwrap();
        }
        let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        let mut delta = equality::BitmapMembershipDelta::default();
        delta.add(1);
        let mut batch = transaction.merge_batch(2).unwrap();
        drop(batch.bitmap(Key::Typed(bitmap_key(1)), &delta).unwrap());
        drop(
            batch
                .bitmap(
                    Key::Typed(bitmap_key(if duplicate { 1 } else { 2 })),
                    &delta,
                )
                .unwrap(),
        );
        let error = batch.stage().await.unwrap_err();
        assert!(!matches!(error, HelixDbError::QueryMemoryLimitExceeded));
        assert_eq!(
            transaction
                .merges
                .as_ref()
                .unwrap()
                .state
                .lock()
                .totals
                .entries,
            2
        );
        assert!(transaction
            .raw
            .get(bitmap_key(1).to_bytes())
            .await
            .unwrap()
            .is_none());
        drop(transaction);
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn abandoned_batches_and_cancelled_transactions_release_all_merge_owners() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-lifetime".into(),
    })
    .await
    .unwrap();
    for commit in [false, true] {
        let budget = Budget::new(1024 * 1024);
        let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        let initial = budget.available();
        let mut delta = equality::BitmapMembershipDelta::default();
        delta.add(1);
        let mut abandoned = transaction.merge_batch(1).unwrap();
        let alias = abandoned
            .bitmap(Key::Typed(bitmap_key(100)), &delta)
            .unwrap();
        drop(abandoned.stage()); // An unpolled future never stages backend state.
        assert!(budget.available() < initial);
        drop(alias);
        assert_eq!(budget.available(), initial);
        let mut batch = transaction.merge_batch(1).unwrap();
        let key = batch
            .bitmap(Key::Typed(bitmap_key(u64::from(commit))), &delta)
            .unwrap();
        batch.stage().await.unwrap();
        let plain_key = Bytes::copy_from_slice(&key);
        drop(key);
        let retained = budget.available();
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        if commit {
            let task = db
                .inner
                .commit_completions
                .spawn(async move {
                    wait.await.unwrap();
                    transaction.commit().await.unwrap();
                })
                .unwrap();
            drop(task);
            assert_eq!(budget.available(), retained);
            release.send(()).unwrap();
            db.inner.commit_completions.seal_and_wait().await;
        } else {
            let task = tokio::spawn(async move {
                let _transaction = transaction;
                let _ = wait.await;
            });
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            drop(release);
        }
        assert_eq!(
            db.inner_db().get(&plain_key).await.unwrap().is_some(),
            commit
        );
        assert_eq!(budget.available(), 1024 * 1024);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn typed_entry_construction_fits_admission_for_dense_sparse_and_large_deltas() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-construction-bound".into(),
    })
    .await
    .unwrap();
    for count in [1, 8, 32, 4096, 8192] {
        for sparse in [false, true] {
            let mut delta = equality::BitmapMembershipDelta::default();
            for id in 0..count {
                delta.add(if sparse { id << 32 } else { id });
            }
            let budget = Budget::new(64 * 1024 * 1024);
            let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
            let initial = budget.available();
            let (batch, allocations) = allocation_testing::observe(|| {
                let mut batch = transaction.merge_batch(1).unwrap();
                drop(batch.bitmap(Key::Typed(bitmap_key(1)), &delta).unwrap());
                batch
            });
            assert_eq!(batch.totals.tokens, count as usize);
            assert!(
                initial - budget.available() >= allocations.bytes,
                "count={count}, sparse={sparse}, allocated={}",
                allocations.bytes
            );
            drop(batch);
            assert_eq!(budget.available(), initial);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn admitted_batches_preserve_disjoint_conflicts_and_concurrent_view_accounting() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-concurrent-admission".into(),
    })
    .await
    .unwrap();
    for overlap in [false, true] {
        let budget = Budget::new(1024 * 1024);
        let left = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        let right = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        let mut first = equality::BitmapMembershipDelta::default();
        first.add(1);
        let mut second = equality::BitmapMembershipDelta::default();
        second.add(if overlap { 1 } else { 2 });
        let mut left_batch = left.merge_batch(1).unwrap();
        let mut right_batch = right.merge_batch(1).unwrap();
        drop(
            left_batch
                .bitmap(Key::Typed(bitmap_key(u64::from(overlap))), &first)
                .unwrap(),
        );
        drop(
            right_batch
                .bitmap(Key::Typed(bitmap_key(u64::from(overlap))), &second)
                .unwrap(),
        );
        let (left_result, right_result) = tokio::join!(left_batch.stage(), right_batch.stage());
        left_result.unwrap();
        right_result.unwrap();
        left.commit().await.unwrap();
        assert_eq!(right.commit().await.is_err(), overlap);
        assert_eq!(budget.available(), 1024 * 1024);
    }
    let budget = Budget::new(1024 * 1024);
    let owner = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let view = owner.mutation_view();
    let mut first = equality::BitmapMembershipDelta::default();
    first.add(7);
    let mut left = owner.merge_batch(1).unwrap();
    let mut right = view.merge_batch(1).unwrap();
    drop(left.bitmap(Key::Typed(bitmap_key(10)), &first).unwrap());
    drop(right.bitmap(Key::Typed(bitmap_key(11)), &first).unwrap());
    let (left, right) = tokio::join!(left.stage(), right.stage());
    left.unwrap();
    right.unwrap();
    assert_eq!(
        owner.merges.as_ref().unwrap().state.lock().totals.entries,
        2
    );
    owner.commit().await.unwrap();
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}

#[tokio::test]
async fn missing_cardinality_and_key_admission_failure_never_invoke_the_encoder() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-preflight-failures".into(),
    })
    .await
    .unwrap();
    let budget = Budget::new(1024 * 1024);
    let owner = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let mut batch = owner.merge_batch(1).unwrap();
    let result = batch.push(
        Key::Typed(bitmap_key(1)),
        1,
        std::iter::from_fn(|| Some(1)),
        || panic!("unknown cardinality encoded"),
    );
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    let entry = Totals {
        entries: 1,
        keys: bitmap_key(1).encoded_len(),
        operands: 1,
        tokens: 1,
    };
    let additional =
        Batch::construction_bytes(1, entry) - Batch::construction_bytes(1, Totals::default());
    let held = budget.reserve(budget.available() - additional).unwrap();
    let result = batch.push(Key::Typed(bitmap_key(1)), 1, std::iter::once(1), || {
        panic!("rejected key encoded operand")
    });
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert!(batch.entries.is_empty());
    drop(held);
    drop(batch);
    owner.merge_batch(0).unwrap().stage().await.unwrap();
    drop(owner);
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}
