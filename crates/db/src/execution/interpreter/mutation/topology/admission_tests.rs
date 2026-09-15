use super::*;
use crate::{execution::interpreter::ExecutionContext, query_resources};

#[tokio::test]
async fn rejected_first_member_leaves_no_empty_row_for_flush() {
    use helix_planner::relational::allocation;
    let probe = query_resources::Budget::new(1024 * 1024);
    let mut delta = membership::Delta::new(Some(&probe)).unwrap();
    let ledger_bytes = 1024 * 1024 - probe.available();
    let proposal = delta.prepare(1, membership::Change::Present).unwrap();
    let first_member_bytes = 1024 * 1024 - probe.available() - ledger_bytes;
    drop(proposal);
    drop(delta);
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "topology-first-member-refusal".into(),
    })
    .await
    .unwrap();
    // Cover rejection after constructing the ledger, including the second
    // direction of a new adjacency row after its first proposal was admitted.
    for (adjacency, member_allowance) in [(false, 0), (true, 0), (true, first_member_bytes)] {
        let row_bytes = if adjacency {
            allocation::btree_bytes::<(DataScope, u64), AdjacencyDelta>(1) + 2 * ledger_bytes
        } else {
            allocation::btree_bytes::<BitmapRow, membership::Delta>(1) + ledger_bytes
        };
        let limit = row_bytes + member_allowance;
        let budget = query_resources::Budget::new(limit);
        let mut runtime = TopologyMutationRuntime::new(Some(&budget));
        let result = if adjacency {
            runtime.add_adjacency(
                DataScope::LegacyUnscoped,
                9,
                1,
                helix_planner::ir::ExpandDirection::Both,
            )
        } else {
            runtime.add_node_label(DataScope::LegacyUnscoped, "Person", 1)
        };
        assert!(matches!(
            result,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        let batch = runtime.collecting_batch().unwrap();
        assert!(
            batch.bitmaps.is_empty(),
            "rejected bitmap row must stay absent"
        );
        assert!(
            batch.adjacency.is_empty(),
            "rejected adjacency row must stay absent"
        );
        let transaction = db
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        runtime.flush(&transaction).await.unwrap();
        assert!(runtime.staged_keys.is_empty());
        assert_eq!(budget.available(), limit);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn request_topology_collection_admits_allocations_before_growth() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "topology-collection-admission".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(16 * 1024 * 1024);
    let mut context = ExecutionContext::new(&db, Default::default());
    context.row_memory = Some(budget.clone());
    let (transaction, mut indexes) = context.begin_write_tx().await.unwrap();
    let before = budget.available();
    let ((), allocation) = crate::allocation_testing::observe(|| {
        for id in (0..512).chain([1 << 32, u64::MAX]) {
            indexes
                .topology_mutations()
                .add_node_label(DataScope::LegacyUnscoped, "Person", id)
                .unwrap();
        }
    });
    let admitted = before - budget.available();
    drop(indexes);
    drop(transaction);
    drop(context);
    db.close().await.unwrap();
    assert!(allocation.bytes > 0);
    assert!(
        admitted >= allocation.bytes,
        "topology allocated {} bytes with {admitted} admitted",
        allocation.bytes
    );
    assert_eq!(budget.available(), 16 * 1024 * 1024);
}

#[tokio::test]
async fn request_topology_collection_rejects_before_allocation() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "topology-collection-refusal".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(16 * 1024 * 1024);
    let mut context = ExecutionContext::new(&db, Default::default());
    context.row_memory = Some(budget.clone());
    let (transaction, mut indexes) = context.begin_write_tx().await.unwrap();
    let occupied = budget.reserve(budget.available()).unwrap();
    let (result, allocation) = crate::allocation_testing::observe(|| {
        indexes
            .topology_mutations()
            .add_node_label(DataScope::LegacyUnscoped, "Person", 1)
    });
    drop(occupied);
    drop(indexes);
    drop(transaction);
    drop(context);
    db.close().await.unwrap();
    assert!(
        matches!(result, Err(HelixDbError::QueryMemoryLimitExceeded)),
        "exhausted topology collection must reject before growth"
    );
    assert_eq!(
        allocation.allocations, 0,
        "refused topology collection allocated"
    );
    assert_eq!(budget.available(), 16 * 1024 * 1024);
}

#[tokio::test]
async fn collection_epochs_preserve_overlays_and_release_consumed_delta_state() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "topology-collection-epochs".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(16 * 1024 * 1024);
    let mut context = ExecutionContext::new(&db, Default::default());
    context.row_memory = Some(budget.clone());
    let (transaction, mut indexes) = context.begin_write_tx().await.unwrap();
    let scope = DataScope::LegacyUnscoped;
    let label = node_label_key(scope, "Person");
    let adjacency = DataKey::Data {
        scope,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(9)),
    }
    .to_bytes();
    for id in 1..=3 {
        let before = budget.available();
        indexes
            .topology_mutations()
            .add_node_label(scope, "Person", id)
            .unwrap();
        indexes
            .topology_mutations()
            .add_adjacency(scope, 9, id, helix_planner::ir::ExpandDirection::Both)
            .unwrap();
        if id > 1 {
            indexes
                .topology_mutations()
                .remove_node_label(scope, "Person", id - 1)
                .unwrap();
            indexes
                .topology_mutations()
                .remove_adjacency(scope, 9, id - 1, helix_planner::ir::ExpandDirection::Both)
                .unwrap();
        }
        assert!(budget.available() < before);
        indexes.flush_topology(&transaction).await.unwrap();
        // Collection ownership ends at flush, while canonical key aliases and
        // the backend's growing operand history remain admitted.
        assert!(budget.available() < before);
        let values = indexes
            .observe_topology(&transaction, &[label.clone(), adjacency.clone()])
            .await
            .unwrap();
        let labels = secondary::SecondaryEqualityValue::decode(values[0].as_ref().unwrap())
            .unwrap()
            .into_ids();
        let edges = edges::decode_edges(values[1].as_ref().unwrap()).unwrap();
        assert_eq!(labels.iter().collect::<Vec<_>>(), vec![id]);
        assert_eq!(edges.nxts_out.iter().collect::<Vec<_>>(), vec![id]);
        assert_eq!(edges.nxts_in.iter().collect::<Vec<_>>(), vec![id]);
    }
    drop(indexes);
    drop(transaction);
    drop(context);
    assert_eq!(budget.available(), 16 * 1024 * 1024);
    assert!(db.inner_db().get(&label).await.unwrap().is_none());
    assert!(db.inner_db().get(&adjacency).await.unwrap().is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn rejected_both_direction_update_preserves_the_first_direction() {
    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "topology-both-direction-refusal".into(),
    })
    .await
    .unwrap();
    let budget = query_resources::Budget::new(16 * 1024 * 1024);
    let mut context = ExecutionContext::new(&db, Default::default());
    context.row_memory = Some(budget.clone());
    let (transaction, mut indexes) = context.begin_write_tx().await.unwrap();
    let scope = DataScope::LegacyUnscoped;
    let key = DataKey::Data {
        scope,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(9)),
    }
    .to_bytes();
    indexes
        .topology_mutations()
        .add_adjacency(scope, 9, 1, helix_planner::ir::ExpandDirection::Out)
        .unwrap();
    let occupied = budget.reserve(budget.available()).unwrap();
    // The outgoing container has spare admitted capacity; the new incoming
    // container requires allocation. Neither change may apply unless both fit.
    let result = indexes.topology_mutations().add_adjacency(
        scope,
        9,
        2,
        helix_planner::ir::ExpandDirection::Both,
    );
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    drop(occupied);
    indexes.flush_topology(&transaction).await.unwrap();
    let values = indexes
        .observe_topology(&transaction, std::slice::from_ref(&key))
        .await
        .unwrap();
    let edges = edges::decode_edges(values[0].as_ref().unwrap()).unwrap();
    assert_eq!(edges.nxts_out.iter().collect::<Vec<_>>(), vec![1]);
    assert!(edges.nxts_in.is_empty());
    indexes
        .topology_mutations()
        .add_adjacency(scope, 9, 2, helix_planner::ir::ExpandDirection::Both)
        .unwrap();
    indexes.flush_topology(&transaction).await.unwrap();
    let values = indexes
        .observe_topology(&transaction, std::slice::from_ref(&key))
        .await
        .unwrap();
    let edges = edges::decode_edges(values[0].as_ref().unwrap()).unwrap();
    assert_eq!(edges.nxts_out.iter().collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(edges.nxts_in.iter().collect::<Vec<_>>(), vec![2]);
    drop(indexes);
    drop(transaction);
    drop(context);
    assert_eq!(budget.available(), 16 * 1024 * 1024);
    db.close().await.unwrap();
}
