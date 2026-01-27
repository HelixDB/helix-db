use crate::helix_engine::{
    storage_core::{
        HelixGraphStorage, StorageConfig, storage_methods::DBMethods, version_info::VersionInfo,
    },
    traversal_core::config::Config,
    types::SecondaryIndex,
};
use tempfile::TempDir;

// Helper function to create a test storage instance
fn setup_test_storage() -> (HelixGraphStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::default();
    let version_info = VersionInfo::default();

    let storage =
        HelixGraphStorage::new(temp_dir.path().to_str().unwrap(), config, version_info).unwrap();

    (storage, temp_dir)
}

// ============================================================================
// Key Packing/Unpacking Tests
// ============================================================================

#[test]
fn test_node_key() {
    let id = 12345u128;
    let key = HelixGraphStorage::node_key(&id);
    assert_eq!(*key, id);
}

#[test]
fn test_edge_key() {
    let id = 67890u128;
    let key = HelixGraphStorage::edge_key(&id);
    assert_eq!(*key, id);
}

#[test]
fn test_out_edge_key() {
    let from_node_id = 100u128;
    let label = [1, 2, 3, 4];

    let key = HelixGraphStorage::out_edge_key(&from_node_id, &label);

    // Verify key structure
    assert_eq!(key.len(), 20);

    // Verify node ID is in first 16 bytes
    let node_id_bytes = &key[0..16];
    assert_eq!(
        u128::from_be_bytes(node_id_bytes.try_into().unwrap()),
        from_node_id
    );

    // Verify label is in last 4 bytes
    let label_bytes = &key[16..20];
    assert_eq!(label_bytes, &label);
}

#[test]
fn test_in_edge_key() {
    let to_node_id = 200u128;
    let label = [5, 6, 7, 8];

    let key = HelixGraphStorage::in_edge_key(&to_node_id, &label);

    // Verify key structure
    assert_eq!(key.len(), 20);

    // Verify node ID is in first 16 bytes
    let node_id_bytes = &key[0..16];
    assert_eq!(
        u128::from_be_bytes(node_id_bytes.try_into().unwrap()),
        to_node_id
    );

    // Verify label is in last 4 bytes
    let label_bytes = &key[16..20];
    assert_eq!(label_bytes, &label);
}

#[test]
fn test_out_edge_key_deterministic() {
    let from_node_id = 42u128;
    let label = [9, 8, 7, 6];

    let key1 = HelixGraphStorage::out_edge_key(&from_node_id, &label);
    let key2 = HelixGraphStorage::out_edge_key(&from_node_id, &label);

    assert_eq!(key1, key2);
}

#[test]
fn test_in_edge_key_deterministic() {
    let to_node_id = 84u128;
    let label = [1, 1, 1, 1];

    let key1 = HelixGraphStorage::in_edge_key(&to_node_id, &label);
    let key2 = HelixGraphStorage::in_edge_key(&to_node_id, &label);

    assert_eq!(key1, key2);
}

#[test]
fn test_pack_edge_data() {
    let edge_id = 123u128;
    let node_id = 456u128;

    let packed = HelixGraphStorage::pack_edge_data(&edge_id, &node_id);

    // Verify packed data structure
    assert_eq!(packed.len(), 32);

    // Verify edge ID is in first 16 bytes
    let edge_id_bytes = &packed[0..16];
    assert_eq!(
        u128::from_be_bytes(edge_id_bytes.try_into().unwrap()),
        edge_id
    );

    // Verify node ID is in last 16 bytes
    let node_id_bytes = &packed[16..32];
    assert_eq!(
        u128::from_be_bytes(node_id_bytes.try_into().unwrap()),
        node_id
    );
}

#[test]
fn test_unpack_adj_edge_data() {
    let edge_id = 789u128;
    let node_id = 1011u128;

    let packed = HelixGraphStorage::pack_edge_data(&edge_id, &node_id);
    let (unpacked_edge_id, unpacked_node_id) =
        HelixGraphStorage::unpack_adj_edge_data(&packed).unwrap();

    assert_eq!(unpacked_edge_id, edge_id);
    assert_eq!(unpacked_node_id, node_id);
}

#[test]
fn test_pack_unpack_edge_data_roundtrip() {
    let test_cases = vec![
        (0u128, 0u128),
        (1u128, 1u128),
        (u128::MAX, u128::MAX),
        (12345u128, 67890u128),
        (u128::MAX / 2, u128::MAX / 3),
    ];

    for (edge_id, node_id) in test_cases {
        let packed = HelixGraphStorage::pack_edge_data(&edge_id, &node_id);
        let (unpacked_edge, unpacked_node) =
            HelixGraphStorage::unpack_adj_edge_data(&packed).unwrap();

        assert_eq!(
            unpacked_edge, edge_id,
            "Edge ID mismatch for ({}, {})",
            edge_id, node_id
        );
        assert_eq!(
            unpacked_node, node_id,
            "Node ID mismatch for ({}, {})",
            edge_id, node_id
        );
    }
}

#[test]
#[should_panic]
fn test_unpack_adj_edge_data_invalid_length() {
    let invalid_data = vec![1u8, 2, 3, 4, 5]; // Too short

    // This will panic when trying to slice the data
    let _ = HelixGraphStorage::unpack_adj_edge_data(&invalid_data);
}

// ============================================================================
// Secondary Index Tests
// ============================================================================

#[test]
fn test_create_secondary_index() {
    let (mut storage, _temp_dir) = setup_test_storage();

    let result = storage.create_secondary_index(SecondaryIndex::Index("test_index".to_string()));
    assert!(result.is_ok());

    // Verify index was added to secondary_indices map
    assert!(storage.secondary_indices.contains_key("test_index"));
}

#[test]
fn test_drop_secondary_index() {
    let (mut storage, _temp_dir) = setup_test_storage();

    // Create an index first
    storage
        .create_secondary_index(SecondaryIndex::Index("test_index".to_string()))
        .unwrap();
    assert!(storage.secondary_indices.contains_key("test_index"));

    // Drop the index
    let result = storage.drop_secondary_index("test_index");
    assert!(result.is_ok());

    // Verify index was removed
    assert!(!storage.secondary_indices.contains_key("test_index"));
}

#[test]
fn test_drop_nonexistent_secondary_index() {
    let (mut storage, _temp_dir) = setup_test_storage();

    let result = storage.drop_secondary_index("nonexistent_index");
    assert!(result.is_err());
}

#[test]
fn test_multiple_secondary_indices() {
    let (mut storage, _temp_dir) = setup_test_storage();

    storage
        .create_secondary_index(SecondaryIndex::Index("index1".to_string()))
        .unwrap();
    storage
        .create_secondary_index(SecondaryIndex::Index("index2".to_string()))
        .unwrap();
    storage
        .create_secondary_index(SecondaryIndex::Index("index3".to_string()))
        .unwrap();

    assert_eq!(storage.secondary_indices.len(), 3);
    assert!(storage.secondary_indices.contains_key("index1"));
    assert!(storage.secondary_indices.contains_key("index2"));
    assert!(storage.secondary_indices.contains_key("index3"));
}

// ============================================================================
// Storage Creation and Configuration Tests
// ============================================================================

#[test]
fn test_storage_creation() {
    let temp_dir = TempDir::new().unwrap();
    let config = Config::default();
    let version_info = VersionInfo::default();

    let result = HelixGraphStorage::new(temp_dir.path().to_str().unwrap(), config, version_info);

    assert!(result.is_ok());
    let _ = result.unwrap();

    // Verify databases were created
    assert!(temp_dir.path().join("data.mdb").exists());
}

#[test]
fn test_storage_config() {
    let schema = Some("test_schema".to_string());
    let graphvis = Some("name".to_string());
    let embedding = Some("openai".to_string());

    let config = StorageConfig::new(schema.clone(), graphvis.clone(), embedding.clone());

    assert_eq!(config.schema, schema);
    assert_eq!(config.graphvis_node_label, graphvis);
    assert_eq!(config.embedding_model, embedding);
}

#[test]
fn test_storage_with_large_db_size() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = Config::default();
    config.db_max_size_gb = Some(10000); // Should cap at 9998

    let version_info = VersionInfo::default();

    let result = HelixGraphStorage::new(temp_dir.path().to_str().unwrap(), config, version_info);

    assert!(result.is_ok());
}

// ============================================================================
// Edge Cases and Boundary Tests
// ============================================================================

#[test]
fn test_edge_key_with_zero_id() {
    let id = 0u128;
    let key = HelixGraphStorage::edge_key(&id);
    assert_eq!(*key, 0);
}

#[test]
fn test_edge_key_with_max_id() {
    let id = u128::MAX;
    let key = HelixGraphStorage::edge_key(&id);
    assert_eq!(*key, u128::MAX);
}

#[test]
fn test_out_edge_key_with_zero_values() {
    let from_node_id = 0u128;
    let label = [0, 0, 0, 0];

    let key = HelixGraphStorage::out_edge_key(&from_node_id, &label);
    assert_eq!(key, [0u8; 20]);
}

#[test]
fn test_out_edge_key_with_max_values() {
    let from_node_id = u128::MAX;
    let label = [255, 255, 255, 255];

    let key = HelixGraphStorage::out_edge_key(&from_node_id, &label);

    // All bytes should be 255
    assert!(key.iter().all(|&b| b == 255));
}

#[test]
fn test_pack_edge_data_with_zero_values() {
    let edge_id = 0u128;
    let node_id = 0u128;

    let packed = HelixGraphStorage::pack_edge_data(&edge_id, &node_id);
    assert_eq!(packed, [0u8; 32]);
}

#[test]
fn test_pack_edge_data_with_max_values() {
    let edge_id = u128::MAX;
    let node_id = u128::MAX;

    let packed = HelixGraphStorage::pack_edge_data(&edge_id, &node_id);
    assert!(packed.iter().all(|&b| b == 255));
}

// ============================================================================
// Additional Key Packing/Unpacking Edge Case Tests
// ============================================================================

#[test]
fn test_out_edge_key_different_labels_produce_different_keys() {
    let node_id = 100u128;
    let label1 = [1, 2, 3, 4];
    let label2 = [5, 6, 7, 8];

    let key1 = HelixGraphStorage::out_edge_key(&node_id, &label1);
    let key2 = HelixGraphStorage::out_edge_key(&node_id, &label2);

    assert_ne!(key1, key2);
}

#[test]
fn test_out_edge_key_different_nodes_produce_different_keys() {
    let node1 = 100u128;
    let node2 = 200u128;
    let label = [1, 2, 3, 4];

    let key1 = HelixGraphStorage::out_edge_key(&node1, &label);
    let key2 = HelixGraphStorage::out_edge_key(&node2, &label);

    assert_ne!(key1, key2);
}

#[test]
fn test_in_edge_key_different_labels_produce_different_keys() {
    let node_id = 100u128;
    let label1 = [1, 2, 3, 4];
    let label2 = [5, 6, 7, 8];

    let key1 = HelixGraphStorage::in_edge_key(&node_id, &label1);
    let key2 = HelixGraphStorage::in_edge_key(&node_id, &label2);

    assert_ne!(key1, key2);
}

#[test]
fn test_out_and_in_edge_keys_same_for_same_node_and_label() {
    // This verifies that out_edge_key and in_edge_key produce the same key format
    let node_id = 12345u128;
    let label = [9, 8, 7, 6];

    let out_key = HelixGraphStorage::out_edge_key(&node_id, &label);
    let in_key = HelixGraphStorage::in_edge_key(&node_id, &label);

    // They should be equal since they use the same structure
    assert_eq!(out_key, in_key);
}

#[test]
fn test_pack_edge_data_different_edge_ids_produce_different_data() {
    let edge1 = 100u128;
    let edge2 = 200u128;
    let node_id = 500u128;

    let packed1 = HelixGraphStorage::pack_edge_data(&edge1, &node_id);
    let packed2 = HelixGraphStorage::pack_edge_data(&edge2, &node_id);

    assert_ne!(packed1, packed2);
}

#[test]
fn test_pack_edge_data_different_node_ids_produce_different_data() {
    let edge_id = 100u128;
    let node1 = 500u128;
    let node2 = 600u128;

    let packed1 = HelixGraphStorage::pack_edge_data(&edge_id, &node1);
    let packed2 = HelixGraphStorage::pack_edge_data(&edge_id, &node2);

    assert_ne!(packed1, packed2);
}

#[test]
#[should_panic(expected = "range end index")]
fn test_unpack_adj_edge_data_short_slice_panics() {
    // 31 bytes - just one byte short
    // Note: Current implementation panics on short slices during slice indexing
    let short_data = vec![0u8; 31];
    let _ = HelixGraphStorage::unpack_adj_edge_data(&short_data);
}

#[test]
#[should_panic(expected = "range end index")]
fn test_unpack_adj_edge_data_empty_slice_panics() {
    // Note: Current implementation panics on empty slices during slice indexing
    let empty_data: Vec<u8> = vec![];
    let _ = HelixGraphStorage::unpack_adj_edge_data(&empty_data);
}

#[test]
#[should_panic(expected = "range end index")]
fn test_unpack_adj_edge_data_16_bytes_panics() {
    // Only edge_id portion, missing node_id
    // Note: Current implementation panics on partial slices during slice indexing
    let partial_data = vec![0u8; 16];
    let _ = HelixGraphStorage::unpack_adj_edge_data(&partial_data);
}

#[test]
fn test_pack_unpack_preserves_byte_order() {
    // Test with a value that has different high and low bytes
    let edge_id = 0x0102030405060708090A0B0C0D0E0F10u128;
    let node_id = 0x1112131415161718191A1B1C1D1E1F20u128;

    let packed = HelixGraphStorage::pack_edge_data(&edge_id, &node_id);
    let (unpacked_edge, unpacked_node) = HelixGraphStorage::unpack_adj_edge_data(&packed).unwrap();

    assert_eq!(unpacked_edge, edge_id);
    assert_eq!(unpacked_node, node_id);
}

#[test]
fn test_out_edge_key_preserves_node_id_byte_order() {
    let node_id = 0x0102030405060708090A0B0C0D0E0F10u128;
    let label = [0xAA, 0xBB, 0xCC, 0xDD];

    let key = HelixGraphStorage::out_edge_key(&node_id, &label);

    // Extract node_id from key and verify
    let extracted_node_id = u128::from_be_bytes(key[0..16].try_into().unwrap());
    assert_eq!(extracted_node_id, node_id);

    // Verify label
    assert_eq!(&key[16..20], &label);
}

// ============================================================================
// Drop Operation Tests (Direct StorageMethods)
// ============================================================================

use crate::helix_engine::storage_core::storage_methods::StorageMethods;
use crate::utils::{
    items::{Edge, Node},
    label_hash::hash_label,
};
use bumpalo::Bump;

fn create_test_node<'a>(arena: &'a Bump, id: u128, label: &str) -> Node<'a> {
    Node {
        id,
        label: arena.alloc_str(label),
        version: 1,
        properties: None,
    }
}

fn create_test_edge<'a>(
    arena: &'a Bump,
    id: u128,
    label: &str,
    from_node: u128,
    to_node: u128,
) -> Edge<'a> {
    Edge {
        id,
        label: arena.alloc_str(label),
        version: 1,
        from_node,
        to_node,
        properties: None,
    }
}

fn insert_node(storage: &HelixGraphStorage, node: &Node) {
    let mut txn = storage.graph_env.write_txn().unwrap();
    let bytes = node.to_bincode_bytes().unwrap();
    storage
        .nodes_db
        .put(&mut txn, HelixGraphStorage::node_key(&node.id), &bytes)
        .unwrap();
    txn.commit().unwrap();
}

fn insert_edge(storage: &HelixGraphStorage, edge: &Edge) {
    let mut txn = storage.graph_env.write_txn().unwrap();
    let bytes = edge.to_bincode_bytes().unwrap();
    storage
        .edges_db
        .put(&mut txn, HelixGraphStorage::edge_key(&edge.id), &bytes)
        .unwrap();

    // Insert into out_edges_db
    let label_hash = hash_label(edge.label, None);
    let out_key = HelixGraphStorage::out_edge_key(&edge.from_node, &label_hash);
    let edge_data = HelixGraphStorage::pack_edge_data(&edge.id, &edge.to_node);
    storage
        .out_edges_db
        .put(&mut txn, &out_key, &edge_data)
        .unwrap();

    // Insert into in_edges_db
    let in_key = HelixGraphStorage::in_edge_key(&edge.to_node, &label_hash);
    let in_edge_data = HelixGraphStorage::pack_edge_data(&edge.id, &edge.from_node);
    storage
        .in_edges_db
        .put(&mut txn, &in_key, &in_edge_data)
        .unwrap();

    txn.commit().unwrap();
}

#[test]
fn test_drop_node_with_no_edges() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node = create_test_node(&arena, 1001, "TestNode");
    insert_node(&storage, &node);

    // Verify node exists
    let txn = storage.graph_env.read_txn().unwrap();
    let result = storage.get_node(&txn, &node.id, &arena);
    assert!(result.is_ok());
    drop(txn);

    // Drop the node
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_node(&mut txn, &node.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify node is gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let result = storage.get_node(&txn, &node.id, &arena);
    assert!(result.is_err());
}

#[test]
fn test_drop_node_with_outgoing_edges_only() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 2001, "Node1");
    let node2 = create_test_node(&arena, 2002, "Node2");
    let edge = create_test_edge(&arena, 3001, "CONNECTS", node1.id, node2.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_edge(&storage, &edge);

    // Verify edge exists
    let txn = storage.graph_env.read_txn().unwrap();
    let result = storage.get_edge(&txn, &edge.id, &arena);
    assert!(result.is_ok());
    drop(txn);

    // Drop node1 (has outgoing edge)
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_node(&mut txn, &node1.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify node1 is gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_node(&txn, &node1.id, &arena).is_err());

    // Verify node2 still exists
    assert!(storage.get_node(&txn, &node2.id, &arena).is_ok());

    // Verify edge is gone (cascading delete)
    assert!(storage.get_edge(&txn, &edge.id, &arena).is_err());
}

#[test]
fn test_drop_node_with_incoming_edges_only() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 4001, "Node1");
    let node2 = create_test_node(&arena, 4002, "Node2");
    let edge = create_test_edge(&arena, 5001, "CONNECTS", node1.id, node2.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_edge(&storage, &edge);

    // Drop node2 (has incoming edge)
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_node(&mut txn, &node2.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify node2 is gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_node(&txn, &node2.id, &arena).is_err());

    // Verify node1 still exists
    assert!(storage.get_node(&txn, &node1.id, &arena).is_ok());

    // Verify edge is gone (cascading delete)
    assert!(storage.get_edge(&txn, &edge.id, &arena).is_err());
}

#[test]
fn test_drop_node_with_both_incoming_and_outgoing_edges() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 6001, "Node1");
    let node2 = create_test_node(&arena, 6002, "Node2");
    let node3 = create_test_node(&arena, 6003, "Node3");
    let edge1 = create_test_edge(&arena, 7001, "OUTGOING", node2.id, node3.id); // node2 -> node3
    let edge2 = create_test_edge(&arena, 7002, "INCOMING", node1.id, node2.id); // node1 -> node2

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_node(&storage, &node3);
    insert_edge(&storage, &edge1);
    insert_edge(&storage, &edge2);

    // Drop node2 (has both incoming and outgoing edges)
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_node(&mut txn, &node2.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify node2 is gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_node(&txn, &node2.id, &arena).is_err());

    // Verify node1 and node3 still exist
    assert!(storage.get_node(&txn, &node1.id, &arena).is_ok());
    assert!(storage.get_node(&txn, &node3.id, &arena).is_ok());

    // Verify both edges are gone
    assert!(storage.get_edge(&txn, &edge1.id, &arena).is_err());
    assert!(storage.get_edge(&txn, &edge2.id, &arena).is_err());
}

#[test]
fn test_drop_node_with_multiple_edges_same_label() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let center = create_test_node(&arena, 8001, "Center");
    let neighbor1 = create_test_node(&arena, 8002, "Neighbor1");
    let neighbor2 = create_test_node(&arena, 8003, "Neighbor2");
    let neighbor3 = create_test_node(&arena, 8004, "Neighbor3");

    let edge1 = create_test_edge(&arena, 9001, "KNOWS", center.id, neighbor1.id);
    let edge2 = create_test_edge(&arena, 9002, "KNOWS", center.id, neighbor2.id);
    let edge3 = create_test_edge(&arena, 9003, "KNOWS", center.id, neighbor3.id);

    insert_node(&storage, &center);
    insert_node(&storage, &neighbor1);
    insert_node(&storage, &neighbor2);
    insert_node(&storage, &neighbor3);
    insert_edge(&storage, &edge1);
    insert_edge(&storage, &edge2);
    insert_edge(&storage, &edge3);

    // Drop center node
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_node(&mut txn, &center.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify center is gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_node(&txn, &center.id, &arena).is_err());

    // Verify all neighbors still exist
    assert!(storage.get_node(&txn, &neighbor1.id, &arena).is_ok());
    assert!(storage.get_node(&txn, &neighbor2.id, &arena).is_ok());
    assert!(storage.get_node(&txn, &neighbor3.id, &arena).is_ok());

    // Verify all edges are gone
    assert!(storage.get_edge(&txn, &edge1.id, &arena).is_err());
    assert!(storage.get_edge(&txn, &edge2.id, &arena).is_err());
    assert!(storage.get_edge(&txn, &edge3.id, &arena).is_err());
}

#[test]
fn test_drop_node_with_multiple_edge_labels() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 10001, "Node1");
    let node2 = create_test_node(&arena, 10002, "Node2");

    let edge1 = create_test_edge(&arena, 11001, "KNOWS", node1.id, node2.id);
    let edge2 = create_test_edge(&arena, 11002, "LIKES", node1.id, node2.id);
    let edge3 = create_test_edge(&arena, 11003, "FOLLOWS", node1.id, node2.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_edge(&storage, &edge1);
    insert_edge(&storage, &edge2);
    insert_edge(&storage, &edge3);

    // Drop node1
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_node(&mut txn, &node1.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify all edges with different labels are gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_edge(&txn, &edge1.id, &arena).is_err());
    assert!(storage.get_edge(&txn, &edge2.id, &arena).is_err());
    assert!(storage.get_edge(&txn, &edge3.id, &arena).is_err());
}

#[test]
fn test_drop_edge_exists() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 12001, "Node1");
    let node2 = create_test_node(&arena, 12002, "Node2");
    let edge = create_test_edge(&arena, 13001, "CONNECTS", node1.id, node2.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_edge(&storage, &edge);

    // Verify edge exists
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_edge(&txn, &edge.id, &arena).is_ok());
    drop(txn);

    // Drop the edge
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_edge(&mut txn, &edge.id);
    assert!(result.is_ok());
    txn.commit().unwrap();

    // Verify edge is gone
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_edge(&txn, &edge.id, &arena).is_err());

    // Verify nodes still exist
    assert!(storage.get_node(&txn, &node1.id, &arena).is_ok());
    assert!(storage.get_node(&txn, &node2.id, &arena).is_ok());
}

#[test]
fn test_drop_edge_nonexistent() {
    let (storage, _temp_dir) = setup_test_storage();

    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = storage.drop_edge(&mut txn, &99999);

    // Should return EdgeNotFound error
    assert!(result.is_err());
}

#[test]
fn test_drop_edge_verifies_out_edges_db_cleanup() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 14001, "Node1");
    let node2 = create_test_node(&arena, 14002, "Node2");
    let edge = create_test_edge(&arena, 15001, "LINK", node1.id, node2.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_edge(&storage, &edge);

    // Verify out_edges_db has the entry
    let txn = storage.graph_env.read_txn().unwrap();
    let label_hash = hash_label("LINK", None);
    let out_key = HelixGraphStorage::out_edge_key(&node1.id, &label_hash);
    let out_entry = storage.out_edges_db.get(&txn, &out_key).unwrap();
    assert!(out_entry.is_some());
    drop(txn);

    // Drop the edge
    let mut txn = storage.graph_env.write_txn().unwrap();
    storage.drop_edge(&mut txn, &edge.id).unwrap();
    txn.commit().unwrap();

    // Verify out_edges_db entry is gone
    let txn = storage.graph_env.read_txn().unwrap();
    let out_entry = storage.out_edges_db.get(&txn, &out_key).unwrap();
    assert!(out_entry.is_none());
}

#[test]
fn test_drop_edge_verifies_in_edges_db_cleanup() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 16001, "Node1");
    let node2 = create_test_node(&arena, 16002, "Node2");
    let edge = create_test_edge(&arena, 17001, "LINK", node1.id, node2.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_edge(&storage, &edge);

    // Verify in_edges_db has the entry
    let txn = storage.graph_env.read_txn().unwrap();
    let label_hash = hash_label("LINK", None);
    let in_key = HelixGraphStorage::in_edge_key(&node2.id, &label_hash);
    let in_entry = storage.in_edges_db.get(&txn, &in_key).unwrap();
    assert!(in_entry.is_some());
    drop(txn);

    // Drop the edge
    let mut txn = storage.graph_env.write_txn().unwrap();
    storage.drop_edge(&mut txn, &edge.id).unwrap();
    txn.commit().unwrap();

    // Verify in_edges_db entry is gone
    let txn = storage.graph_env.read_txn().unwrap();
    let in_entry = storage.in_edges_db.get(&txn, &in_key).unwrap();
    assert!(in_entry.is_none());
}

#[test]
fn test_drop_one_edge_preserves_other_edges() {
    let (storage, _temp_dir) = setup_test_storage();
    let arena = Bump::new();

    let node1 = create_test_node(&arena, 18001, "Node1");
    let node2 = create_test_node(&arena, 18002, "Node2");
    let node3 = create_test_node(&arena, 18003, "Node3");

    let edge1 = create_test_edge(&arena, 19001, "LINK", node1.id, node2.id);
    let edge2 = create_test_edge(&arena, 19002, "LINK", node1.id, node3.id);

    insert_node(&storage, &node1);
    insert_node(&storage, &node2);
    insert_node(&storage, &node3);
    insert_edge(&storage, &edge1);
    insert_edge(&storage, &edge2);

    // Drop only edge1
    let mut txn = storage.graph_env.write_txn().unwrap();
    storage.drop_edge(&mut txn, &edge1.id).unwrap();
    txn.commit().unwrap();

    // Verify edge1 is gone but edge2 remains
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    assert!(storage.get_edge(&txn, &edge1.id, &arena).is_err());
    assert!(storage.get_edge(&txn, &edge2.id, &arena).is_ok());
}
