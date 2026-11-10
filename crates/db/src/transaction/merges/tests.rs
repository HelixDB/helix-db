use super::*;
use crate::{
    allocation_testing,
    encoding::v2::keys::{scope::DataScope, AdjacencyKey, DataKeyKind, EdgePairIndexKey},
    transaction::{CommitDurability, Mutation, Owned},
};

fn bitmap_key(id: u64) -> DataKey<'static> {
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(id, id)),
    }
}

fn adjacency_key(id: u64) -> DataKey<'static> {
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(id)),
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
        batch.push(
            Family::Bitmap,
            Key::Typed(bitmap_key(1)),
            1,
            std::iter::once(1),
            || panic!("encoder ran before admission"),
        )
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
        batch.stage(),
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
            batch.stage().unwrap();
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
        transaction.commit(CommitDurability::Durable).await.unwrap();
        assert_eq!(budget.available(), 16 * 1024 * 1024);
        assert!(db.inner_db().get(&key).await.unwrap().is_some());
    }
    db.close().await.unwrap();
}

/// One invalid entry rejects its whole batch before any operand is staged.
#[derive(Debug, Clone, Copy)]
enum Rejection {
    DuplicateKey,
    BitmapDeltaOnAdjacencyRow,
    AdjacencyDeltaOnBitmapRow,
    BitmapDeltaOnRangeRow,
    BitmapDeltaOnPropertyRow,
    AdjacencyDeltaOnPropertyRow,
    OverlappingDelta,
    TruncatedDelta,
}

/// A range presence row: it shares the property-index prefix with membership
/// bitmaps but never takes a merge.
fn range_key(id: u64) -> DataKey<'static> {
    use crate::encoding::indexes::range::{RangeIndexDirection, RangeIndexKey};
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::PropertyIndex(
            crate::encoding::v2::keys::indexes::PropertyIndexKey::Range(RangeIndexKey::new(
                RangeIndexDirection::Asc,
                crate::encoding::indexes::hash_property_name("age"),
                std::borrow::Cow::Borrowed("42"),
                id,
            )),
        ),
    }
}

fn property_key(id: u64) -> DataKey<'static> {
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(crate::encoding::v2::keys::NodePropertyKey::new(id)),
    }
}

#[tokio::test]
async fn rejected_batches_stage_nothing_and_release_admission() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-rejection-atomicity".into(),
    })
    .await
    .unwrap();
    let mut delta = equality::BitmapMembershipDelta::default();
    delta.add(1);
    let mut edges = adjacency::AdjacencyMembershipDelta::default();
    edges.add_out(1);
    // Additions and removals both hold member 1: the frame is well formed, so
    // only the merge operator's own decoder rejects it.
    let overlapping = {
        let mut bytes = b"HLXRBM2\0".to_vec();
        let mut member = Vec::new();
        roaring::RoaringTreemap::from_iter([1_u64])
            .serialize_into(&mut member)
            .unwrap();
        for _ in 0..2 {
            bytes.extend_from_slice(&u32::try_from(member.len()).unwrap().to_be_bytes());
            bytes.extend_from_slice(&member);
        }
        Bytes::from(bytes)
    };
    let truncated = delta.encode().slice(..12);
    for rejection in [
        Rejection::DuplicateKey,
        Rejection::BitmapDeltaOnAdjacencyRow,
        Rejection::AdjacencyDeltaOnBitmapRow,
        Rejection::BitmapDeltaOnRangeRow,
        Rejection::BitmapDeltaOnPropertyRow,
        Rejection::AdjacencyDeltaOnPropertyRow,
        Rejection::OverlappingDelta,
        Rejection::TruncatedDelta,
    ] {
        for budgeted in [false, true] {
            let budget = Budget::new(1024 * 1024);
            let transaction = Owned::begin(&db.inner_db(), budgeted.then_some(&budget))
                .await
                .unwrap();
            let mut batch = transaction.merge_batch(2).unwrap();
            drop(batch.bitmap(Key::Typed(bitmap_key(1)), &delta).unwrap());
            let second = match rejection {
                Rejection::DuplicateKey => batch.bitmap(Key::Typed(bitmap_key(1)), &delta),
                Rejection::BitmapDeltaOnAdjacencyRow => {
                    batch.bitmap(Key::Typed(adjacency_key(2)), &delta)
                }
                Rejection::AdjacencyDeltaOnBitmapRow => {
                    batch.adjacency(Key::Typed(bitmap_key(2)), &edges)
                }
                Rejection::BitmapDeltaOnRangeRow => batch.bitmap(Key::Typed(range_key(2)), &delta),
                Rejection::BitmapDeltaOnPropertyRow => {
                    batch.bitmap(Key::Typed(property_key(2)), &delta)
                }
                Rejection::AdjacencyDeltaOnPropertyRow => {
                    batch.adjacency(Key::Typed(property_key(2)), &edges)
                }
                Rejection::OverlappingDelta => batch.push(
                    Family::Bitmap,
                    Key::Typed(bitmap_key(2)),
                    overlapping.len(),
                    std::iter::once(1),
                    || overlapping.clone(),
                ),
                Rejection::TruncatedDelta => batch.push(
                    Family::Bitmap,
                    Key::Typed(bitmap_key(2)),
                    truncated.len(),
                    std::iter::once(1),
                    || truncated.clone(),
                ),
            };
            drop(second.unwrap());
            // Budgeted or not, and whichever check refuses it, a rejected
            // batch is one invalid-argument storage error.
            let error = batch.stage().unwrap_err();
            assert!(
                matches!(
                    &error,
                    HelixDbError::Storage(error) if error.kind() == slatedb::ErrorKind::Invalid
                ),
                "{rejection:?} budgeted={budgeted}: {error:?}"
            );
            assert_eq!(
                error.error_code(),
                helix_ast::error_code::QueryErrorCode::StorageError
            );
            assert!(
                transaction
                    .raw
                    .get(bitmap_key(1).to_bytes())
                    .await
                    .unwrap()
                    .is_none(),
                "{rejection:?} staged its valid entry"
            );
            if budgeted {
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
            }
            drop(transaction);
            assert_eq!(budget.available(), 1024 * 1024);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn staging_never_reads_the_stored_row() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-no-stored-row-read".into(),
    })
    .await
    .unwrap();
    let mut delta = equality::BitmapMembershipDelta::default();
    delta.add(u64::MAX - 1);
    delta.remove(3);
    let mut edges = adjacency::AdjacencyMembershipDelta::default();
    edges.add_out(u64::MAX - 1);
    edges.remove_in(3);
    let large = roaring::RoaringTreemap::from_iter((0..1 << 20).map(|id| id * 3));
    let mut large_edges = adjacency::Edges::new();
    large.iter().for_each(|id| large_edges.add_out(id));
    let mut stage_allocations = Vec::new();
    for (stored, row) in [
        ("absent", None),
        (
            "small",
            Some((
                equality::SecondaryEqualityBitmapValue::new([3].into_iter().collect()).encode(),
                adjacency::encode_edges(&adjacency::Edges::new()),
            )),
        ),
        (
            "large",
            Some((
                equality::SecondaryEqualityBitmapValue::new(large.clone()).encode(),
                adjacency::encode_edges(&large_edges),
            )),
        ),
        (
            "corrupt",
            Some((
                Bytes::from_static(b"corrupt-bitmap"),
                Bytes::from_static(b"corrupt-adjacency"),
            )),
        ),
    ] {
        let id = stage_allocations.len() as u64;
        if let Some((bitmap, adjacency)) = row {
            db.inner_db()
                .put(bitmap_key(id).to_bytes(), bitmap)
                .await
                .unwrap();
            db.inner_db()
                .put(adjacency_key(id).to_bytes(), adjacency)
                .await
                .unwrap();
        }
        let transaction = Owned::begin(&db.inner_db(), None).await.unwrap();
        let mut batch = transaction.merge_batch(2).unwrap();
        drop(batch.bitmap(Key::Typed(bitmap_key(id)), &delta).unwrap());
        drop(
            batch
                .adjacency(Key::Typed(adjacency_key(id)), &edges)
                .unwrap(),
        );
        let (result, allocations) = allocation_testing::observe(|| batch.stage());
        result.unwrap_or_else(|error| panic!("{stored} row: {error}"));
        stage_allocations.push((stored, allocations.bytes));
        drop(transaction);
    }
    // Staging allocates only for the delta, so every stored row costs the same.
    assert!(
        stage_allocations
            .iter()
            .all(|(_, bytes)| *bytes == stage_allocations[0].1),
        "{stage_allocations:?}"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn staged_entries_apply_in_push_order_across_epochs() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-push-order".into(),
    })
    .await
    .unwrap();
    for (row, add_first) in [(0, true), (1, false)] {
        let transaction = Owned::begin(&db.inner_db(), None).await.unwrap();
        for add in [add_first, !add_first] {
            let mut delta = equality::BitmapMembershipDelta::default();
            let mut edges = adjacency::AdjacencyMembershipDelta::default();
            if add {
                delta.add(7);
                edges.add_out(7);
            } else {
                delta.remove(7);
                edges.remove_out(7);
            }
            let mut batch = transaction.merge_batch(2).unwrap();
            drop(batch.bitmap(Key::Typed(bitmap_key(row)), &delta).unwrap());
            drop(
                batch
                    .adjacency(Key::Typed(adjacency_key(row)), &edges)
                    .unwrap(),
            );
            batch.stage().unwrap();
        }
        transaction.commit(CommitDurability::Durable).await.unwrap();
        let bitmap = db.inner_db().get(bitmap_key(row).to_bytes()).await.unwrap();
        let adjacency = db
            .inner_db()
            .get(adjacency_key(row).to_bytes())
            .await
            .unwrap();
        if add_first {
            assert_eq!(bitmap, None);
            assert_eq!(adjacency, None);
        } else {
            assert_eq!(
                equality::SecondaryEqualityBitmapValue::decode(&bitmap.unwrap())
                    .unwrap()
                    .ids(),
                &roaring::RoaringTreemap::from_iter([7])
            );
            assert_eq!(
                adjacency::decode_edges(&adjacency.unwrap())
                    .unwrap()
                    .iter_out()
                    .collect::<Vec<_>>(),
                vec![7]
            );
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn budgeted_staging_admits_validation_before_decoding() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-budgeted-validation".into(),
    })
    .await
    .unwrap();
    let mut delta = equality::BitmapMembershipDelta::default();
    let mut edges = adjacency::AdjacencyMembershipDelta::default();
    for id in 0..4096 {
        delta.add(id << 32);
        edges.add_out(id << 32);
        edges.remove_in(id);
    }
    for decode_admitted in [false, true] {
        let budget = Budget::new(64 * 1024 * 1024);
        let transaction = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
        let initial = budget.available();
        let mut batch = transaction.merge_batch(2).unwrap();
        drop(batch.bitmap(Key::Typed(bitmap_key(1)), &delta).unwrap());
        drop(
            batch
                .adjacency(Key::Typed(adjacency_key(1)), &edges)
                .unwrap(),
        );
        let decode = equality::SecondaryEqualityBitmapValue::prepare(&batch.entries[0].operand)
            .unwrap()
            .allocation_bound()
            .max(
                adjacency::prepare_edges(&batch.entries[1].operand)
                    .unwrap()
                    .allocation_bound(),
            );
        let construction = Batch::construction_bytes(batch.capacity, batch.totals);
        let history = batch.totals.history_bytes();
        // Leave room for everything but the decode, so admission fails first.
        let held = (!decode_admitted).then(|| budget.reserve(budget.available() - 1).unwrap());
        let (result, allocations) = allocation_testing::observe(|| batch.stage());
        if decode_admitted {
            result.unwrap();
            assert!(
                allocations.peak_bytes <= construction + decode + history,
                "peak={}, admitted={}",
                allocations.peak_bytes,
                construction + decode + history
            );
            // Construction and decode admission end with staging; history stays.
            assert_eq!(initial - budget.available(), history - size_of::<Tracker>());
        } else {
            assert!(matches!(
                result,
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
            // Only the duplicate-key set: the refused decode never ran.
            assert!(allocations.peak_bytes <= 1024, "{allocations:?}");
            assert!(transaction
                .raw
                .get(bitmap_key(1).to_bytes())
                .await
                .unwrap()
                .is_none());
        }
        drop(held);
        drop(transaction);
        assert_eq!(budget.available(), 64 * 1024 * 1024);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn adjacency_conflicts_follow_member_and_direction() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "merge-adjacency-conflicts".into(),
    })
    .await
    .unwrap();
    for (row, right_incoming, right_member, conflicts) in [
        (0, false, 5, true),
        (1, true, 5, false),
        (2, false, 6, false),
    ] {
        let left = Owned::begin(&db.inner_db(), None).await.unwrap();
        let right = Owned::begin(&db.inner_db(), None).await.unwrap();
        let mut left_edges = adjacency::AdjacencyMembershipDelta::default();
        left_edges.add_out(5);
        let mut right_edges = adjacency::AdjacencyMembershipDelta::default();
        if right_incoming {
            right_edges.add_in(right_member);
        } else {
            right_edges.remove_out(right_member);
        }
        for (transaction, edges) in [(&left, &left_edges), (&right, &right_edges)] {
            let mut batch = transaction.merge_batch(1).unwrap();
            drop(
                batch
                    .adjacency(Key::Typed(adjacency_key(row)), edges)
                    .unwrap(),
            );
            batch.stage().unwrap();
        }
        left.commit(CommitDurability::Durable).await.unwrap();
        assert_eq!(
            right.commit(CommitDurability::Durable).await.is_err(),
            conflicts,
            "row {row}"
        );
        let stored = adjacency::decode_edges(
            &db.inner_db()
                .get(adjacency_key(row).to_bytes())
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(stored.iter_out().collect::<Vec<_>>(), vec![5], "row {row}");
        assert_eq!(
            stored.iter_in().collect::<Vec<_>>(),
            if right_incoming { vec![5] } else { vec![] },
            "row {row}"
        );
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
        drop(abandoned); // An unstaged batch leaves no backend state.
        assert!(budget.available() < initial);
        drop(alias);
        assert_eq!(budget.available(), initial);
        let mut batch = transaction.merge_batch(1).unwrap();
        let key = batch
            .bitmap(Key::Typed(bitmap_key(u64::from(commit))), &delta)
            .unwrap();
        batch.stage().unwrap();
        let plain_key = Bytes::copy_from_slice(&key);
        drop(key);
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

/// Staging is synchronous, so concurrency comes from threads: each batch
/// stages on its own thread, released together by a barrier, against one
/// shared budget and, for views of one transaction, one shared history ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for (view, member) in [
                (left.mutation_view(), 1),
                (right.mutation_view(), if overlap { 1 } else { 2 }),
            ] {
                let barrier = &barrier;
                threads.spawn(move || {
                    let mut delta = equality::BitmapMembershipDelta::default();
                    delta.add(member);
                    let mut batch = view.merge_batch(1).unwrap();
                    drop(
                        batch
                            .bitmap(Key::Typed(bitmap_key(u64::from(overlap))), &delta)
                            .unwrap(),
                    );
                    barrier.wait();
                    batch.stage().unwrap();
                });
            }
        });
        left.commit(CommitDurability::Durable).await.unwrap();
        assert_eq!(
            right.commit(CommitDurability::Durable).await.is_err(),
            overlap
        );
        assert_eq!(budget.available(), 1024 * 1024);
    }
    const THREADS: u64 = 8;
    const BATCHES: u64 = 16;
    let budget = Budget::new(16 * 1024 * 1024);
    let owner = Owned::begin(&db.inner_db(), Some(&budget)).await.unwrap();
    let initial = budget.available();
    let barrier = std::sync::Barrier::new(THREADS as usize);
    std::thread::scope(|threads| {
        for thread in 0..THREADS {
            let (barrier, view) = (&barrier, owner.mutation_view());
            threads.spawn(move || {
                let mut delta = equality::BitmapMembershipDelta::default();
                delta.add(7);
                barrier.wait();
                for batch in 0..BATCHES {
                    let mut staged = view.merge_batch(1).unwrap();
                    drop(
                        staged
                            .bitmap(Key::Typed(bitmap_key(thread * BATCHES + batch)), &delta)
                            .unwrap(),
                    );
                    staged.stage().unwrap();
                }
            });
        }
    });
    let history = owner.merges.as_ref().unwrap().state.lock().totals;
    assert_eq!(history.entries as u64, THREADS * BATCHES);
    // Construction and decode admission ended with each batch; history stays.
    assert_eq!(
        initial - budget.available(),
        history.history_bytes() - size_of::<Tracker>()
    );
    owner.commit(CommitDurability::Durable).await.unwrap();
    assert_eq!(budget.available(), 16 * 1024 * 1024);
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
        Family::Bitmap,
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
    let result = batch.push(
        Family::Bitmap,
        Key::Typed(bitmap_key(1)),
        1,
        std::iter::once(1),
        || panic!("rejected key encoded operand"),
    );
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert!(batch.entries.is_empty());
    drop(held);
    drop(batch);
    owner.merge_batch(0).unwrap().stage().unwrap();
    drop(owner);
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}

/// Both paths end in the backend's `merge_disjoint_with_discriminators` with
/// the same keys, operands and tokens, so conflicts and stored rows match.
#[tokio::test]
async fn staging_matches_the_previous_checked_backend_staging() {
    let mut runs = Vec::new();
    for checked in [false, true] {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: format!("merge-equivalence-{checked}"),
        })
        .await
        .unwrap();
        let mut outcomes = Vec::new();
        for (row, left_member, right_member, right_removes) in [
            (0, 1, 1, false),
            (1, 1, 2, false),
            (2, 1, 1, true),
            (3, 1, 2, true),
        ] {
            let left = Owned::begin(&db.inner_db(), None).await.unwrap();
            let right = Owned::begin(&db.inner_db(), None).await.unwrap();
            for (transaction, member, removes) in [
                (&left, left_member, false),
                (&right, right_member, right_removes),
            ] {
                let mut delta = equality::BitmapMembershipDelta::default();
                let mut edges = adjacency::AdjacencyMembershipDelta::default();
                if removes {
                    delta.remove(member);
                    edges.remove_in(member);
                } else {
                    delta.add(member);
                    edges.add_in(member);
                }
                if checked {
                    let incoming = edges
                        .incoming_members()
                        .map(|id| (1_u128 << u64::BITS) | u128::from(id));
                    transaction
                        .raw
                        .merge_disjoint_checked_batch([
                            slatedb::DisjointMergeBatchEntry::from_tokens(
                                bitmap_key(row).to_bytes(),
                                delta.members().map(u128::from),
                                delta.encode(),
                            ),
                            slatedb::DisjointMergeBatchEntry::from_tokens(
                                adjacency_key(row).to_bytes(),
                                incoming,
                                edges.encode(),
                            ),
                        ])
                        .await
                        .unwrap();
                } else {
                    let mut batch = transaction.merge_batch(2).unwrap();
                    drop(batch.bitmap(Key::Typed(bitmap_key(row)), &delta).unwrap());
                    drop(
                        batch
                            .adjacency(Key::Typed(adjacency_key(row)), &edges)
                            .unwrap(),
                    );
                    batch.stage().unwrap();
                }
            }
            left.commit(CommitDurability::Durable).await.unwrap();
            outcomes.push(right.commit(CommitDurability::Durable).await.is_ok());
        }
        db.inner_db()
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
            .unwrap();
        let mut rows = Vec::new();
        for row in 0..4 {
            for key in [bitmap_key(row).to_bytes(), adjacency_key(row).to_bytes()] {
                rows.push(db.inner_db().get(&key).await.unwrap());
            }
        }
        runs.push((outcomes, rows));
        db.close().await.unwrap();
    }
    assert_eq!(runs[0].0, vec![false, true, false, true]);
    assert_eq!(runs[0], runs[1]);
}
