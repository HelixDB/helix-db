//! Production contracts for the single vector-search session.
//!
//! This feature-gated child module verifies deterministic helper policies,
//! request validation, empty and populated sessions, layer reads, recovery,
//! and optional observation through the real production implementation. It
//! uses only current vector rows in isolated in-memory databases.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::v1::keys::vectors::{
    VectorEntryCandidateNodeKey, VectorKey, VectorLayer0NeighborsKey, VectorSimHashKey,
};
use crate::encoding::v1::values::vectors::entry::encode_entry_candidate_layer;
use crate::search::vector::distance::{Cosine, Distance};
use crate::search::vector::mutation::VectorInsertContract;
use crate::search::vector::read_fault_production_support::{FaultingRead, ReadFault};
use crate::search::vector::storage::{VectorRows, VectorWriteRows};
use crate::search::vector::{
    encode_item, Item, MeasuredVectorTransaction, SimHashMode, VectorIndexConfig,
};

/// Distance type without an active persisted semantic binding.
#[derive(Debug, Clone)]
enum UnboundSearchDistance {}

impl Distance for UnboundSearchDistance {
    type Header = ();
    type VectorCodec = f32;

    fn name() -> &'static str {
        "production-unbound-search-distance"
    }

    fn new_header(_vector: &UnalignedVector<Self::VectorCodec>) -> Self::Header {}

    fn distance(_left: &Item<Self>, _right: &Item<Self>) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

impl crate::search::vector::distance::sealed::Sealed for UnboundSearchDistance {}

/// Verifies deterministic prefetch ranking and deferred visited-state ownership.
fn run_helper_contracts() {
    assert!(select_layer0_neighbor_prefetch_targets(
        &[(1, 1.0)],
        &HashMap::new(),
        &HashMap::new(),
        8,
    )
    .is_empty());
    assert!(select_layer0_neighbor_prefetch_targets(
        &[(1, 1.0), (2, 2.0)],
        &HashMap::new(),
        &HashMap::new(),
        0,
    )
    .is_empty());
    let current = HashMap::from([(2, vec![9])]);
    let prefetched = HashMap::from([(3, vec![9])]);
    assert_eq!(
        select_layer0_neighbor_prefetch_targets(
            &[(2, 0.5), (1, 0.25), (1, 0.25), (3, 0.75), (5, 1.0)],
            &current,
            &prefetched,
            2,
        ),
        vec![1, 5]
    );

    let mut visited = HashSet::from([1]);
    assert_eq!(
        mark_sampled_neighbors_visited(&mut visited, vec![(1, 7), (2, 8), (2, 9), (3, 10)]),
        vec![(2, 8), (3, 10)]
    );

    let mut destination = SearchStats::default();
    let completed = SearchStats {
        expansion_steps: 7,
        ..SearchStats::default()
    };
    SearchObserver::disabled().publish(completed.clone());
    SearchObserver::collecting(&mut destination).publish(completed);
    assert_eq!(destination.expansion_steps, 7);
}

/// Verifies validation, empty state, typed layer reads, and populated search.
async fn run_session_contracts() {
    let db = Db::open(
        "production-vector-search-contract",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let missing = VectorIndex::<Cosine>::new("production-vector-search-missing");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let mut session = SearchSession::new(&missing, &txn, SearchObserver::disabled());
    assert!(matches!(
        session
            .run(&[1.0, 0.0, 0.0], &SearchParams::new(1).unwrap())
            .await,
        Err(HelixDbError::IndexNotFound(_))
    ));
    txn.rollback();

    let index = VectorIndex::<Cosine>::new("production-vector-search-contract-index");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &txn,
            VectorIndexConfig::new(index.name(), "embedding", 3)
                .with_m(4)
                .with_m0(16)
                .with_ef_construction(16),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let mut session = SearchSession::new(&index, &txn, SearchObserver::disabled());
    assert!(matches!(
        session
            .run(&[1.0, 0.0], &SearchParams::new(1).unwrap())
            .await,
        Err(HelixDbError::InvalidDimension { .. })
    ));
    assert!(matches!(
        session
            .run(&[1.0, f32::NAN, 0.0], &SearchParams::new(1).unwrap(),)
            .await,
        Err(HelixDbError::InvalidVectorComponent { index: 1 })
    ));
    assert!(SearchParams::new(0).is_err());
    assert!(session
        .run(&[1.0, 0.0, 0.0], &SearchParams::new(1).unwrap())
        .await
        .unwrap()
        .is_empty());
    txn.rollback();

    for (node_id, vector, layer) in [
        (1, [1.0, 0.0, 0.0], 2),
        (2, [0.95, 0.05, 0.0], 1),
        (3, [0.9, 0.1, 0.0], 0),
        (4, [0.8, 0.2, 0.0], 0),
        (5, [0.7, 0.3, 0.0], 0),
        (6, [0.6, 0.4, 0.0], 0),
        (7, [0.5, 0.5, 0.0], 0),
        (8, [0.4, 0.6, 0.0], 0),
        (9, [0.2, 0.8, 0.0], 0),
        (10, [0.0, 1.0, 0.0], 0),
        (11, [0.9, 0.0, 0.1], 0),
        (12, [0.8, 0.0, 0.2], 0),
        (13, [0.7, 0.0, 0.3], 0),
        (14, [0.6, 0.0, 0.4], 0),
        (15, [0.5, 0.0, 0.5], 0),
        (16, [0.4, 0.0, 0.6], 0),
        (17, [0.3, 0.0, 0.7], 0),
        (18, [0.2, 0.0, 0.8], 0),
        (19, [0.1, 0.0, 0.9], 0),
        (20, [0.0, 0.0, 1.0], 0),
    ] {
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);
        index
            .insert_with_measured_transaction(
                &measured,
                node_id,
                &vector,
                VectorInsertContract::Upsert,
                Some(layer),
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }

    // Pin every greedy-descent branch to an explicit upper-layer graph. The
    // insertion topology is intentionally irrelevant here: node 99 exercises
    // stale-neighbor recovery, node 101 improves the entry, and its backlink
    // forces a second pass that terminates through the visited set.
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_upper_vector(100, encode_item(&Item::<Cosine>::new(vec![0.0, 1.0, 0.0])))
        .unwrap();
    rows.put_upper_vector(101, encode_item(&Item::<Cosine>::new(vec![1.0, 0.0, 0.0])))
        .unwrap();
    rows.put_upper_neighbors(1, 100, &[99, 101]).unwrap();
    rows.put_upper_neighbors(1, 101, &[100]).unwrap();
    txn.commit().await.unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let (_, reads) = index.load_neighbors_layer0_counted(&txn, 1).await.unwrap();
    assert_eq!(reads, 1);
    let mut prefetched = HashMap::new();
    assert_eq!(
        index
            .prefetch_layer0_neighbors_counted(&txn, &[], &mut prefetched)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        index
            .prefetch_layer0_neighbors_counted(&txn, &[1, 99], &mut prefetched)
            .await
            .unwrap(),
        2
    );
    let entry = index
        .find_live_entry_candidate_readonly(&txn)
        .await
        .unwrap()
        .unwrap();
    let query = Item::<Cosine>::new(vec![1.0, 0.0, 0.0]);
    assert_eq!(
        index
            .search_layer_greedy(&txn, &query, u64::MAX, 0)
            .await
            .unwrap(),
        u64::MAX
    );
    assert!(index
        .search_layer_greedy(&txn, &query, entry, 0)
        .await
        .is_ok());
    assert!(index
        .search_layer_greedy(&txn, &query, entry, 2)
        .await
        .is_ok());
    assert_eq!(
        index
            .search_layer_greedy(&txn, &query, 100, 1)
            .await
            .unwrap(),
        101
    );
    let metadata = index.get_metadata(&txn).await.unwrap().unwrap();
    let query_simhash = Layer0QuerySimHash::Computed(
        index
            .simhash_cache(3)
            .unwrap()
            .simhasher()
            .hash_from_slice(&[1.0, 0.0, 0.0])
            .unwrap(),
    );
    let expected_dimension = VectorDimension::try_new(3).unwrap();
    let unbound = VectorIndex::<UnboundSearchDistance>::new(index.name());
    assert!(matches!(
        unbound
            .search_layer0_with_simhash::<true, false>(
                &txn,
                &Item::<UnboundSearchDistance>::new(vec![1.0, 0.0, 0.0]),
                &query_simhash,
                expected_dimension,
                entry,
                &SearchParams::new(1).unwrap(),
                metadata.config.simhash_threshold,
                metadata.config.sampling_ratio,
                metadata.config.adaptive_enabled,
                metadata.config.adaptive_failure_prob,
            )
            .await,
        Err(HelixDbError::Config(_))
    ));
    assert!(matches!(
        index
            .search_layer0_with_simhash::<true, false>(
                &txn,
                &query,
                &query_simhash,
                expected_dimension,
                entry,
                &SearchParams::new(1).unwrap(),
                crate::search::vector::SIMHASH_BITS + 1,
                metadata.config.sampling_ratio,
                metadata.config.adaptive_enabled,
                metadata.config.adaptive_failure_prob,
            )
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));
    assert!(matches!(
        index
            .search_layer0_with_simhash::<true, true>(
                &txn,
                &query,
                &Layer0QuerySimHash::UnusedExhaustive,
                expected_dimension,
                entry,
                &SearchParams::new(1)
                    .unwrap()
                    .with_simhash_mode(SimHashMode::Always)
                    .with_pre_simhash_sampling_ratio(1.0)
                    .unwrap(),
                metadata.config.simhash_threshold,
                metadata.config.sampling_ratio,
                metadata.config.adaptive_enabled,
                metadata.config.adaptive_failure_prob,
            )
            .await,
        Err(HelixDbError::InvariantViolation(message))
            if message.contains("requires a query fingerprint")
    ));
    assert!(SearchParams::new(1)
        .unwrap()
        .with_pre_simhash_sampling_ratio(f32::NAN)
        .is_err());
    assert!(matches!(
        index
            .search_layer0_with_simhash::<true, false>(
                &txn,
                &query,
                &query_simhash,
                expected_dimension,
                entry,
                &SearchParams::new(1).unwrap(),
                metadata.config.simhash_threshold,
                f32::NAN,
                metadata.config.adaptive_enabled,
                metadata.config.adaptive_failure_prob,
            )
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));
    assert!(matches!(
        index
            .search_layer0_with_simhash::<true, false>(
                &txn,
                &query,
                &query_simhash,
                expected_dimension,
                entry,
                &SearchParams::new(1).unwrap(),
                metadata.config.simhash_threshold,
                metadata.config.sampling_ratio,
                metadata.config.adaptive_enabled,
                f32::NAN,
            )
            .await,
        Err(HelixDbError::InvalidVectorConfig(_))
    ));

    let point = FaultingRead::new(&txn, ReadFault::Point);
    assert!(index.load_neighbors_layer0(&point, entry).await.is_err());
    assert!(index
        .search_layer_greedy(&point, &query, entry, 0)
        .await
        .is_err());
    let mut point_session = SearchSession::new(&index, &point, SearchObserver::disabled());
    assert!(point_session
        .run(&[1.0, 0.0, 0.0], &SearchParams::new(1).unwrap())
        .await
        .is_err());

    let multi_get = FaultingRead::new(&txn, ReadFault::MultiGet);
    assert!(index
        .prefetch_layer0_neighbors_counted(&multi_get, &[entry], &mut HashMap::new())
        .await
        .is_err());
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &multi_get,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &SearchParams::new(1).unwrap(),
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());

    let scan = FaultingRead::new(&txn, ReadFault::Scan);
    assert!(index
        .find_live_entry_candidate_readonly(&scan)
        .await
        .is_err());

    let mut stats = SearchStats::default();
    let mut observed = SearchSession::new(&index, &txn, SearchObserver::collecting(&mut stats));
    let results = observed
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(4)
                .unwrap()
                .with_ef(8)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always),
        )
        .await
        .unwrap();
    assert!(!results.is_empty());
    assert!(stats.txn_get_total > 0);

    let mut off_stats = SearchStats::default();
    let mut off = SearchSession::new(&index, &txn, SearchObserver::collecting(&mut off_stats));
    assert!(!off
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Off)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(off_stats.txn_get_simhash_filter, 0);

    let mut pre_sample_stats = SearchStats::default();
    let mut pre_sample = SearchSession::new(
        &index,
        &txn,
        SearchObserver::collecting(&mut pre_sample_stats),
    );
    assert!(!pre_sample
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always)
                .with_pre_simhash_sampling_ratio(0.0)
                .unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
    assert!(pre_sample_stats.pre_simhash_sample_kept > 0);
    assert!(pre_sample_stats.pre_simhash_sample_dropped > 0);

    let mut deferred_stats = SearchStats::default();
    let mut deferred = SearchSession::new(
        &index,
        &txn,
        SearchObserver::collecting(&mut deferred_stats),
    );
    assert!(!deferred
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap()
                .with_simhash_sampling_ratio(0.000_001)
                .unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
    assert!(deferred_stats.simhash_passed_before_sampling > 0);

    let mut zero_sample_stats = SearchStats::default();
    let mut zero_sample = SearchSession::new(
        &index,
        &txn,
        SearchObserver::collecting(&mut zero_sample_stats),
    );
    assert!(!zero_sample
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap()
                .with_simhash_sampling_ratio(0.0)
                .unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
    assert!(zero_sample_stats.simhash_passed_before_sampling > 0);
    assert_eq!(zero_sample_stats.simhash_passed_after_sampling, 0);

    let mut adaptive_stats = SearchStats::default();
    let mut adaptive = SearchSession::new(
        &index,
        &txn,
        SearchObserver::collecting(&mut adaptive_stats),
    );
    assert!(!adaptive
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Adaptive)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap()
                .with_simhash_bypass_tuning(1, 3, 0.0, 1)
                .unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
    assert!(adaptive_stats.simhash_bypass_trigger_budget > 0);

    let mut low_yield_stats = SearchStats::default();
    let mut low_yield = SearchSession::new(
        &index,
        &txn,
        SearchObserver::collecting(&mut low_yield_stats),
    );
    assert!(!low_yield
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Adaptive)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap()
                .with_simhash_bypass_tuning(1, 1, 1.0, usize::MAX)
                .unwrap(),
        )
        .await
        .unwrap()
        .is_empty());
    assert!(low_yield_stats.simhash_bypass_trigger_low_yield > 0);
    txn.rollback();

    run_recovery_contracts(&db, &index).await;
    run_corruption_contracts(&db, &index).await;
}

/// Verifies read-only candidate skipping, stale-root fallback, and fail-closed rows.
async fn run_recovery_contracts(db: &Db, index: &VectorIndex<Cosine>) {
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    rows.put_entry_candidate(96, 10).unwrap();
    measured
        .put(
            index.row_keyspace().key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), 96),
            )),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    rows.put_entry_candidate(95, 9).unwrap();
    rows.delete_entry_candidate_node(95).unwrap();
    rows.put_entry_candidate(94, 8).unwrap();
    measured
        .put(
            index.row_keyspace().key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(index.id(), 94),
            )),
            encode_entry_candidate_layer(7),
        )
        .unwrap();
    rows.put_entry_candidate(93, 6).unwrap();
    assert!(index
        .find_live_entry_candidate_readonly(&measured)
        .await
        .unwrap()
        .is_some());

    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let stale_entry = metadata.entry_point.unwrap();
    let (canonical, _) = index
        .resolve_required_canonical_vector_key_counted(
            &measured,
            stale_entry,
            "seeding stale search entry",
        )
        .await
        .unwrap();
    rows.delete_canonical_vector(&canonical).unwrap();
    let query = Item::<Cosine>::new(vec![1.0, 0.0, 0.0]);
    let query_simhash = Layer0QuerySimHash::Computed(
        index
            .simhash_cache(3)
            .unwrap()
            .simhasher()
            .hash_from_slice(&[1.0, 0.0, 0.0])
            .unwrap(),
    );
    let expected_dimension = VectorDimension::try_new(3).unwrap();
    let (fallback, fallback_stats) = index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            stale_entry,
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Off)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap(),
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .unwrap();
    assert!(!fallback.is_empty());
    assert!(fallback_stats.txn_get_vectors > 1);
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let neighbors = index
        .load_neighbors_layer0(&measured, stale_entry)
        .await
        .unwrap();
    let Some(&missing_hash_node) = neighbors.first() else {
        panic!("deterministic populated entry has a layer-zero neighbor")
    };
    VectorWriteRows::new(&measured, index.row_keyspace())
        .delete_simhash(missing_hash_node)
        .unwrap();
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            stale_entry,
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap(),
            0,
            1.0,
            true,
            0.1,
        )
        .await
        .is_err());
    let mut session = SearchSession::new(index, &measured, SearchObserver::disabled());
    assert!(session
        .run(
            &[1.0, 0.0, 0.0],
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap(),
        )
        .await
        .is_err());
    txn.rollback();

    let empty = VectorIndex::<Cosine>::new("production-vector-search-unrecoverable-entry");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    empty
        .create(&txn, VectorIndexConfig::new(empty.name(), "embedding", 3))
        .await
        .unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let mut stale_metadata = empty.get_metadata(&measured).await.unwrap().unwrap();
    stale_metadata.entry_point = Some(999);
    VectorWriteRows::new(&measured, empty.row_keyspace())
        .put_metadata(&stale_metadata)
        .unwrap();
    let (results, stats) = empty
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            999,
            &SearchParams::new(1).unwrap(),
            stale_metadata.config.simhash_threshold,
            stale_metadata.config.sampling_ratio,
            stale_metadata.config.adaptive_enabled,
            stale_metadata.config.adaptive_failure_prob,
        )
        .await
        .unwrap();
    assert!(results.is_empty());
    assert!(stats.txn_get_total > 0);
    assert!(VectorRows::new(&measured, empty.row_keyspace())
        .metadata()
        .await
        .unwrap()
        .is_some());
    txn.rollback();
}

/// Verifies corrupt and missing current rows fail or recover at their owner.
async fn run_corruption_contracts(db: &Db, index: &VectorIndex<Cosine>) {
    let query = Item::<Cosine>::new(vec![1.0, 0.0, 0.0]);
    let query_simhash = Layer0QuerySimHash::Computed(
        index
            .simhash_cache(3)
            .unwrap()
            .simhasher()
            .hash_from_slice(&[1.0, 0.0, 0.0])
            .unwrap(),
    );
    let expected_dimension = VectorDimension::try_new(3).unwrap();
    let params = SearchParams::new(10)
        .unwrap()
        .with_ef(10)
        .unwrap()
        .with_simhash_mode(SimHashMode::Off)
        .with_pre_simhash_sampling_ratio(1.0)
        .unwrap();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let (entry_key, _) = index
        .resolve_required_canonical_vector_key_counted(
            &measured,
            entry,
            "corrupting the search entry payload",
        )
        .await
        .unwrap();
    VectorWriteRows::new(&measured, index.row_keyspace())
        .put_canonical_vector(&entry_key, Bytes::from_static(b"corrupt"))
        .unwrap();
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &params,
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    measured
        .put(
            index
                .row_keyspace()
                .key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                    index.id(),
                    entry,
                ))),
            Bytes::from_static(b"corrupt-neighbors"),
        )
        .unwrap();
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &params,
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let neighbors = index.load_neighbors_layer0(&measured, entry).await.unwrap();
    let Some(&neighbor) = neighbors.first() else {
        panic!("populated search entry has a layer-zero neighbor")
    };
    measured
        .put(
            index
                .row_keyspace()
                .key(VectorKey::SimHash(VectorSimHashKey::new(
                    index.id(),
                    neighbor,
                ))),
            Bytes::from_static(b"corrupt-simhash"),
        )
        .unwrap();
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &SearchParams::new(10)
                .unwrap()
                .with_ef(10)
                .unwrap()
                .with_simhash_mode(SimHashMode::Always)
                .with_pre_simhash_sampling_ratio(1.0)
                .unwrap(),
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let neighbors = index.load_neighbors_layer0(&measured, entry).await.unwrap();
    let Some(_) = neighbors.first() else {
        panic!("populated search entry has a layer-zero neighbor")
    };
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    for neighbor in neighbors {
        rows.delete_simhash(neighbor).unwrap();
    }
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &params,
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let neighbors = index.load_neighbors_layer0(&measured, entry).await.unwrap();
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    for neighbor in &neighbors {
        let (key, _) = index
            .resolve_required_canonical_vector_key_counted(
                &measured,
                *neighbor,
                "corrupting a candidate payload",
            )
            .await
            .unwrap();
        rows.put_canonical_vector(&key, Bytes::from_static(b"corrupt-candidate"))
            .unwrap();
    }
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &params,
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let neighbors = index.load_neighbors_layer0(&measured, entry).await.unwrap();
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    for neighbor in neighbors {
        let (key, _) = index
            .resolve_required_canonical_vector_key_counted(
                &measured,
                neighbor,
                "removing a candidate payload",
            )
            .await
            .unwrap();
        rows.delete_canonical_vector(&key).unwrap();
    }
    let (results, stats) = index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &params,
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(stats.txn_get_vectors > 1);
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    let missing_hash_node = 9_999;
    let rows = VectorWriteRows::new(&measured, index.row_keyspace());
    let missing_hash_key = index.canonical_vector_key_from_simhash(
        missing_hash_node,
        crate::search::vector::SimHash::from_bits(0xDEAD),
    );
    rows.put_canonical_vector(
        &missing_hash_key,
        encode_item(&Item::<Cosine>::new(vec![0.9, 0.1, 0.0])),
    )
    .unwrap();
    rows.put_layer0_neighbors(entry, &[missing_hash_node])
        .unwrap();
    assert!(index
        .search_layer0_with_simhash::<true, false>(
            &measured,
            &query,
            &query_simhash,
            expected_dimension,
            entry,
            &params,
            metadata.config.simhash_threshold,
            metadata.config.sampling_ratio,
            metadata.config.adaptive_enabled,
            metadata.config.adaptive_failure_prob,
        )
        .await
        .is_err());
    txn.rollback();

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let metadata = index.get_metadata(&measured).await.unwrap().unwrap();
    let entry = metadata.entry_point.unwrap();
    VectorWriteRows::new(&measured, index.row_keyspace())
        .delete_upper_vector(entry)
        .unwrap();
    assert_eq!(
        index
            .search_layer_greedy(&measured, &query, entry, metadata.max_layer)
            .await
            .unwrap(),
        entry
    );
    txn.rollback();
}

/// Exercises helper policy, validation, traversal, recovery, and observation.
pub(crate) async fn run() {
    run_helper_contracts();
    run_session_contracts().await;
}
