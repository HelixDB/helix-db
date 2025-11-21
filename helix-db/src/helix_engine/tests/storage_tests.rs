use crate::helix_engine::{
    storage_core::{
        HelixGraphStorage, StorageConfig, storage_methods::DBMethods, version_info::VersionInfo,
    },
    traversal_core::config::Config,
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
    let key = HelixGraphStorage::node_key(id);
    assert_eq!(key, id);
}

#[test]
fn test_edge_key() {
    let id = 67890u128;
    let key = HelixGraphStorage::edge_key(id);
    assert_eq!(key, id);
}

#[test]
fn test_out_edge_key() {
    let from_node_id = 100u128;
    let label = [1, 2, 3, 4];

    let key = HelixGraphStorage::out_edge_key(from_node_id, &label);

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

    let key = HelixGraphStorage::in_edge_key(to_node_id, &label);

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

    let key1 = HelixGraphStorage::out_edge_key(from_node_id, &label);
    let key2 = HelixGraphStorage::out_edge_key(from_node_id, &label);

    assert_eq!(key1, key2);
}

#[test]
fn test_in_edge_key_deterministic() {
    let to_node_id = 84u128;
    let label = [1, 1, 1, 1];

    let key1 = HelixGraphStorage::in_edge_key(to_node_id, &label);
    let key2 = HelixGraphStorage::in_edge_key(to_node_id, &label);

    assert_eq!(key1, key2);
}

#[test]
fn test_pack_edge_data() {
    let edge_id = 123u128;
    let node_id = 456u128;

    let packed = HelixGraphStorage::pack_edge_data(edge_id, node_id);

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

    let packed = HelixGraphStorage::pack_edge_data(edge_id, node_id);
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
        let packed = HelixGraphStorage::pack_edge_data(edge_id, node_id);
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

    let result = storage.create_secondary_index("test_index");
    assert!(result.is_ok());

    // Verify index was added to secondary_indices map
    assert!(storage.secondary_indices.contains_key("test_index"));
}

#[test]
fn test_drop_secondary_index() {
    let (mut storage, _temp_dir) = setup_test_storage();

    // Create an index first
    storage.create_secondary_index("test_index").unwrap();
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

    storage.create_secondary_index("index1").unwrap();
    storage.create_secondary_index("index2").unwrap();
    storage.create_secondary_index("index3").unwrap();

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
    let key = HelixGraphStorage::edge_key(id);
    assert_eq!(key, 0);
}

#[test]
fn test_edge_key_with_max_id() {
    let id = u128::MAX;
    let key = HelixGraphStorage::edge_key(id);
    assert_eq!(key, u128::MAX);
}

#[test]
fn test_out_edge_key_with_zero_values() {
    let from_node_id = 0u128;
    let label = [0, 0, 0, 0];

    let key = HelixGraphStorage::out_edge_key(from_node_id, &label);
    assert_eq!(key, [0u8; 20]);
}

#[test]
fn test_out_edge_key_with_max_values() {
    let from_node_id = u128::MAX;
    let label = [255, 255, 255, 255];

    let key = HelixGraphStorage::out_edge_key(from_node_id, &label);

    // All bytes should be 255
    assert!(key.iter().all(|&b| b == 255));
}

#[test]
fn test_pack_edge_data_with_zero_values() {
    let edge_id = 0u128;
    let node_id = 0u128;

    let packed = HelixGraphStorage::pack_edge_data(edge_id, node_id);
    assert_eq!(packed, [0u8; 32]);
}

#[test]
fn test_pack_edge_data_with_max_values() {
    let edge_id = u128::MAX;
    let node_id = u128::MAX;

    let packed = HelixGraphStorage::pack_edge_data(edge_id, node_id);
    assert!(packed.iter().all(|&b| b == 255));
}
