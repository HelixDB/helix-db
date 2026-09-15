use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::graph_mutation::GraphEntity;

#[test]
fn retained_keys_deduplicate_while_covering_backend_capacity_growth() {
    let budget = Budget::new(16 * 1024 * 1024);
    let tracker = Tracker::new(&budget).unwrap();
    let mut backend = HashSet::<Bytes>::new();
    let keys: Vec<_> = (0..1000)
        .map(|id| GraphEntity::node(id).property_key(DataScope::LegacyUnscoped))
        .collect();
    for count in [0, 1, 3, 7, 14, 28, 56, 112, 224, 1000] {
        for _ in 0..3 {
            let batch = &keys[..count];
            let admitted = tracker.keys(batch).unwrap();
            backend.extend(batch.iter().cloned().collect::<HashSet<_>>());
            let state = tracker.state.lock();
            assert_eq!(state.keys.len(), backend.len());
            assert_eq!(
                state.key_payload,
                batch.iter().map(Bytes::len).sum::<usize>()
            );
            assert!(
                state.backend_table
                    >= allocation::hash_table_retained_bytes::<Bytes, ()>(backend.capacity())
            );
            drop(state);
            drop(admitted);
        }
    }
    let settled = budget.available();
    for _ in 0..100 {
        drop(tracker.keys(&keys).unwrap());
        assert_eq!(budget.available(), settled);
    }
    let duplicate_batch = vec![keys[0].clone(); 4096];
    drop(tracker.keys(&duplicate_batch).unwrap());
    assert_eq!(
        budget.available(),
        settled,
        "batch duplicates must not inflate retained payload or table allowance"
    );
    drop(tracker);
    assert_eq!(budget.available(), 16 * 1024 * 1024);
}

#[test]
fn admission_failure_precedes_key_allocation_and_preserves_existing_state() {
    assert!(matches!(
        Tracker::new(&Budget::new(0)),
        Err(AdmissionFailure)
    ));
    let budget = Budget::new(65536);
    let tracker = Tracker::new(&budget).unwrap();
    let first = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    drop(tracker.keys(std::slice::from_ref(&first)).unwrap());
    let occupied = budget.reserve(budget.available()).unwrap();
    let second = GraphEntity::node(2).property_key(DataScope::LegacyUnscoped);
    for keys in [
        std::slice::from_ref(&second),
        &[first.clone(), second.clone()],
    ] {
        let (result, allocations) = crate::allocation_testing::observe(|| tracker.keys(keys));
        assert!(matches!(result, Err(AdmissionFailure)));
        assert_eq!(allocations.allocations, 0);
        assert_eq!(tracker.state.lock().keys.len(), 1);
    }
    assert!(matches!(
        tracker.range(&(first.clone()..second.clone()), None),
        Err(AdmissionFailure)
    ));
    assert_eq!(tracker.state.lock().ranges, 0);
    drop(occupied);
    drop(tracker.keys(std::slice::from_ref(&second)).unwrap());
    assert_eq!(tracker.state.lock().keys.len(), 2);
    drop(tracker);
    assert_eq!(budget.available(), 65536);
}

#[test]
fn empty_and_prefix_ranges_retain_every_request_and_release_with_the_owner() {
    let budget = Budget::new(1024 * 1024);
    let tracker = Tracker::new(&budget).unwrap();
    let key = GraphEntity::node(1).property_key(DataScope::LegacyUnscoped);
    let mut before = budget.available();
    for _ in 0..10 {
        drop(tracker.range(&.., None).unwrap());
        assert!(budget.available() < before);
        before = budget.available();
    }
    drop(
        tracker
            .range(
                &(Bound::Included(key.clone()), Bound::Excluded(key.clone())),
                None,
            )
            .unwrap(),
    );
    drop(tracker.range(&(..=key.clone()), Some(key.len())).unwrap());
    drop(tracker.range(&(key.clone()..), Some(0)).unwrap());
    drop(tracker.range(&.., Some(key.len())).unwrap());
    let state = tracker.state.lock();
    assert_eq!(state.ranges, 14);
    assert_eq!(state.range_payload, 8 * key.len());
    drop(state);
    drop(tracker);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn retained_bound_arithmetic_rejects_overflow() {
    assert_eq!(State::bytes(0, 0, 0, 0, 0).unwrap(), size_of::<Tracker>());
    for args in [
        (usize::MAX, 0, 0, 0, 0),
        (0, usize::MAX, 0, 0, 0),
        (0, 0, usize::MAX, 0, 0),
        (0, 0, 0, usize::MAX, 0),
        (0, 0, 0, 0, usize::MAX),
        (1, usize::MAX / 2, 0, 0, 0),
    ] {
        assert!(matches!(
            State::bytes(args.0, args.1, args.2, args.3, args.4),
            Err(AdmissionFailure)
        ));
    }
    let tracker = Tracker::new(&Budget::new(1024)).unwrap();
    assert!(matches!(
        tracker.range(&.., Some(usize::MAX)),
        Err(AdmissionFailure)
    ));
}

#[tokio::test]
async fn explicit_read_iterator_stops_before_unadmitted_keys_and_keeps_prior_dependencies() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "explicit-read-admission".into(),
    })
    .await
    .unwrap();
    let budget = Budget::new(8192);
    let tracker = Tracker::new(&budget).unwrap();
    let transaction = db
        .inner_db()
        .begin(slatedb::IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let keys: Vec<_> = (0..1024)
        .map(|id| GraphEntity::node(id).property_key(DataScope::LegacyUnscoped))
        .collect();
    let error = tracker.mark_read(&transaction, &keys).unwrap_err();
    assert!(super::super::is_admission_failure(&error));
    let state = tracker.state.lock();
    assert!(!state.keys.is_empty() && state.keys.len() < keys.len());
    assert!(state.keys.contains(keys[0].as_ref()));
    assert!(!state.keys.contains(keys.last().unwrap().as_ref()));
    drop(state);
    let row = crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::new(Vec::new());
    db.inner_db().put(&keys[0], row.encoded()).await.unwrap();
    assert_eq!(
        transaction.commit().await.unwrap_err().kind(),
        slatedb::ErrorKind::Transaction
    );
    assert!(budget.available() < 8192);
    drop(tracker);
    assert_eq!(budget.available(), 8192);
    db.close().await.unwrap();
}
