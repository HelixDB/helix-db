use std::sync::Arc;

use bumpalo::Bump;
use heed3::RoTxn;
use tempfile::TempDir;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::ops::{
            g::G,
            in_::to_v::ToVAdapter,
            out::{out::OutAdapter, out_e::OutEdgesAdapter},
            source::{
                add_e::AddEAdapter, add_n::AddNAdapter, e_from_type::EFromTypeAdapter,
                n_from_id::NFromIdAdapter, v_from_id::VFromIdAdapter, v_from_type::VFromTypeAdapter,
            },
            util::drop::Drop,
            vectors::{
                brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
                search::SearchVAdapter,
            },
        },
        types::GraphError,
        vector_core::vector::HVector,
    },
    utils::properties::ImmutablePropertiesMap,
};

type Filter = fn(&HVector, &RoTxn) -> bool;

fn setup_test_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(
        db_path,
        crate::helix_engine::traversal_core::config::Config::default(),
        Default::default(),
    )
    .unwrap();
    (temp_dir, Arc::new(storage))
}

#[test]
fn test_insert_and_fetch_vector() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[0.1, 0.2, 0.3], "embedding", None)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let fetched = G::new(&storage, &txn, &arena)
        .e_from_type("embedding")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(fetched.is_empty());

    let results = G::new(&storage, &txn, &arena)
        .search_v::<Filter, _>(&[0.1, 0.2, 0.3], 10, "embedding", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), vector.id());
}

#[test]
fn test_vector_edges_from_and_to_node() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let vector_id = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 0.0, 0.0], "embedding", None)
        .collect_to_obj()
        .unwrap()
        .id();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("has_vector", None, node_id, vector_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let neighbors = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_e("has_vector")
        .to_v(true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].id(), vector_id);
}

#[test]
fn test_brute_force_vector_search_orders_by_distance() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj()
        .unwrap();

    let vectors = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];
    let mut vector_ids = Vec::new();
    for vector in vectors {
        let vec_id = G::new_mut(&storage, &arena, &mut txn)
            .insert_v::<Filter>(&vector, "vector", None)
            .collect_to_obj()
            .unwrap()
            .id();
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("embedding", None, node.id(), vec_id, false)
            .collect_to_obj()
            .unwrap();
        vector_ids.push(vec_id);
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&node.id())
        .out_e("embedding")
        .to_v(true)
        .brute_force_search_v(&[1.0, 2.0, 3.0], 10)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), vector_ids[0]);
}

#[test]
fn test_drop_vector_removes_edges() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let vector_id = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[0.5, 0.5, 0.5], "vector", None)
        .collect_to_obj()
        .unwrap()
        .id();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("has_vector", None, node_id, vector_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let vectors = G::new(&storage, &txn, &arena)
        .search_v::<Filter, _>(&[0.5, 0.5, 0.5], 10, "vector", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(
        vectors
            .into_iter()
            .map(Ok::<_, crate::helix_engine::types::GraphError>),
        storage.as_ref(),
        &mut txn,
    )
    .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let remaining = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_vec("has_vector", false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(remaining.is_empty());
}

// ============================================================================
// v_from_type Tests
// ============================================================================

#[test]
fn test_v_from_type_basic_with_vector_data() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert a vector with label "test_label"
    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "test_label", None)
        .collect_to_obj()
        .unwrap();
    let vector_id = vector.id();
    txn.commit().unwrap();

    // Retrieve vectors with the label, including vector data
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("test_label", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), vector_id);

    // Verify it's a full HVector with data
    if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::Vector(v) = &results[0] {
        assert_eq!(v.data.len(), 3);
        assert_eq!(v.data[0], 1.0);
    } else {
        panic!("Expected TraversalValue::Vector");
    }
}

#[test]
fn test_v_from_type_without_vector_data() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert a vector with label "no_data_label"
    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[4.0, 5.0, 6.0], "no_data_label", None)
        .collect_to_obj()
        .unwrap();
    let vector_id = vector.id();
    txn.commit().unwrap();

    // Retrieve vectors without vector data
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("no_data_label", false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), vector_id);

    // Verify it's a VectorWithoutData
    match &results[0] {
        crate::helix_engine::traversal_core::traversal_value::TraversalValue::VectorNodeWithoutVectorData(v) => {
            assert_eq!(*v.id(), vector_id);
            assert_eq!(v.label(), "no_data_label");
        }
        _ => panic!("Expected TraversalValue::VectorNodeWithoutVectorData"),
    }
}

#[test]
fn test_v_from_type_multiple_same_label() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert multiple vectors with the same label
    let v1 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "shared_label", None)
        .collect_to_obj()
        .unwrap();
    let v2 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[4.0, 5.0, 6.0], "shared_label", None)
        .collect_to_obj()
        .unwrap();
    let v3 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[7.0, 8.0, 9.0], "shared_label", None)
        .collect_to_obj()
        .unwrap();

    let vector_ids = vec![v1.id(), v2.id(), v3.id()];
    txn.commit().unwrap();

    // Retrieve all vectors with the shared label
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("shared_label", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 3);

    // Verify all vector IDs are present
    let retrieved_ids: Vec<_> = results.iter().map(|v| v.id()).collect();
    for id in &vector_ids {
        assert!(retrieved_ids.contains(id));
    }
}

#[test]
fn test_v_from_type_multiple_different_labels() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert vectors with different labels
    let v1 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "label_a", None)
        .collect_to_obj()
        .unwrap();
    let _v2 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[4.0, 5.0, 6.0], "label_b", None)
        .collect_to_obj()
        .unwrap();
    let _v3 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[7.0, 8.0, 9.0], "label_c", None)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Retrieve vectors with only label_a
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("label_a", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), v1.id());
}

#[test]
fn test_v_from_type_nonexistent_label() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert a vector with a different label
    let _vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "existing_label", None)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Try to retrieve vectors with a non-existent label
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("nonexistent_label", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_v_from_type_empty_database() {
    let (_temp_dir, storage) = setup_test_db();

    // Query empty database
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("any_label", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_v_from_type_with_properties() {
    use std::collections::HashMap;
    use crate::protocol::value::Value;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create properties with various Value types
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), Value::String("test_vector".to_string()));
    properties.insert("count".to_string(), Value::I64(42));
    properties.insert("score".to_string(), Value::F64(3.14));
    properties.insert("active".to_string(), Value::Boolean(true));
    properties.insert("tags".to_string(), Value::Array(vec![
        Value::String("tag1".to_string()),
        Value::String("tag2".to_string()),
    ]));

    // Convert to ImmutablePropertiesMap
    let props_map = ImmutablePropertiesMap::new(
        properties.len(),
        properties.iter().map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
        &arena,
    );

    // Insert vector with properties
    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "prop_label", Some(props_map))
        .collect_to_obj()
        .unwrap();
    let vector_id = vector.id();
    txn.commit().unwrap();

    // Retrieve without data to check properties
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("prop_label", false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), vector_id);

    // Verify properties are preserved
    if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::VectorNodeWithoutVectorData(v) = &results[0] {
        let props = v.properties.as_ref().unwrap();
        assert_eq!(props.get("name"), Some(&Value::String("test_vector".to_string())));
        assert_eq!(props.get("count"), Some(&Value::I64(42)));
        assert_eq!(props.get("score"), Some(&Value::F64(3.14)));
        assert_eq!(props.get("active"), Some(&Value::Boolean(true)));
    } else {
        panic!("Expected VectorNodeWithoutVectorData");
    }
}

#[test]
fn test_v_from_type_deleted_vectors_filtered() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert two vectors with the same label
    let v1 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "delete_test", None)
        .collect_to_obj()
        .unwrap();
    let v2 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[4.0, 5.0, 6.0], "delete_test", None)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Delete the first vector by re-querying it
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let vectors_to_delete = G::new(&storage, &txn, &arena)
        .v_from_type("delete_test", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|v| v.id() == v1.id())
        .collect::<Vec<_>>();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    use crate::helix_engine::types::GraphError;
    Drop::drop_traversal(
        vectors_to_delete.into_iter().map(Ok::<_, GraphError>),
        storage.as_ref(),
        &mut txn,
    )
    .unwrap();
    txn.commit().unwrap();

    // Retrieve vectors - should only get the non-deleted one
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .v_from_type("delete_test", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), v2.id());
}

#[test]
fn test_v_from_type_with_edges_and_nodes() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a node
    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("document", None, None)
        .collect_to_obj()
        .unwrap();

    // Create vectors and connect them to the node
    let v1 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 0.0, 0.0], "embedding", None)
        .collect_to_obj()
        .unwrap();
    let v2 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[0.0, 1.0, 0.0], "embedding", None)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("has_embedding", None, node.id(), v1.id(), false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("has_embedding", None, node.id(), v2.id(), false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Use v_from_type to retrieve all embeddings
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let all_embeddings = G::new(&storage, &txn, &arena)
        .v_from_type("embedding", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(all_embeddings.len(), 2);

    let embedding_ids: Vec<_> = all_embeddings.iter().map(|v| v.id()).collect();
    assert!(embedding_ids.contains(&v1.id()));
    assert!(embedding_ids.contains(&v2.id()));

    // Verify we can also traverse from the node to vectors
    let from_node = G::new(&storage, &txn, &arena)
        .n_from_id(&node.id())
        .out_e("has_embedding")
        .to_v(true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(from_node.len(), 2);
}

#[test]
fn test_v_from_type_after_migration() {
    use std::collections::HashMap;
    use crate::protocol::value::Value;
    use crate::helix_engine::storage_core::storage_migration::migrate;

    // Helper to create old-format vector properties (HashMap-based)
    fn create_old_properties(
        label: &str,
        is_deleted: bool,
        extra_props: HashMap<String, Value>,
    ) -> Vec<u8> {
        let mut props = HashMap::new();
        props.insert("label".to_string(), Value::String(label.to_string()));
        props.insert("is_deleted".to_string(), Value::Boolean(is_deleted));

        for (k, v) in extra_props {
            props.insert(k, v);
        }

        bincode::serialize(&props).unwrap()
    }

    // Helper to clear metadata (simulates PreMetadata state)
    fn clear_metadata(storage: &mut crate::helix_engine::storage_core::HelixGraphStorage) -> Result<(), crate::helix_engine::types::GraphError> {
        let mut txn = storage.graph_env.write_txn()?;
        storage.metadata_db.clear(&mut txn)?;
        txn.commit()?;
        Ok(())
    }

    let (_temp_dir, storage) = setup_test_db();
    let mut storage_mut = match Arc::try_unwrap(storage) {
        Ok(s) => s,
        Err(_) => panic!("Failed to unwrap Arc - there are multiple references"),
    };

    // Clear metadata to simulate PreMetadata state (before migration)
    clear_metadata(&mut storage_mut).unwrap();

    // Create old-format vectors with various properties
    {
        let mut txn = storage_mut.graph_env.write_txn().unwrap();

        // Vector 1: Simple vector with test label
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("vector1".to_string()));
        props1.insert("count".to_string(), Value::I64(100));
        let old_bytes1 = create_old_properties("test_migration", false, props1);
        storage_mut
            .vectors
            .vector_properties_db
            .put(&mut txn, &1u128, &old_bytes1)
            .unwrap();

        // Add actual vector data with proper key format
        let vector_data1: Vec<f64> = vec![1.0, 2.0, 3.0];
        let bytes1: Vec<u8> = vector_data1.iter().flat_map(|f| f.to_be_bytes()).collect();
        let key1 = [b"v:".as_slice(), &1u128.to_be_bytes(), &0usize.to_be_bytes()].concat();
        storage_mut
            .vectors
            .vectors_db
            .put(&mut txn, &key1, &bytes1)
            .unwrap();

        // Vector 2: Another vector with same label
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("vector2".to_string()));
        props2.insert("score".to_string(), Value::F64(0.95));
        let old_bytes2 = create_old_properties("test_migration", false, props2);
        storage_mut
            .vectors
            .vector_properties_db
            .put(&mut txn, &2u128, &old_bytes2)
            .unwrap();

        // Add actual vector data with proper key format
        let vector_data2: Vec<f64> = vec![4.0, 5.0, 6.0];
        let bytes2: Vec<u8> = vector_data2.iter().flat_map(|f| f.to_be_bytes()).collect();
        let key2 = [b"v:".as_slice(), &2u128.to_be_bytes(), &0usize.to_be_bytes()].concat();
        storage_mut
            .vectors
            .vectors_db
            .put(&mut txn, &key2, &bytes2)
            .unwrap();

        // Vector 3: Different label
        let mut props3 = HashMap::new();
        props3.insert("name".to_string(), Value::String("vector3".to_string()));
        let old_bytes3 = create_old_properties("other_label", false, props3);
        storage_mut
            .vectors
            .vector_properties_db
            .put(&mut txn, &3u128, &old_bytes3)
            .unwrap();

        // Add actual vector data with proper key format
        let vector_data3: Vec<f64> = vec![7.0, 8.0, 9.0];
        let bytes3: Vec<u8> = vector_data3.iter().flat_map(|f| f.to_be_bytes()).collect();
        let key3 = [b"v:".as_slice(), &3u128.to_be_bytes(), &0usize.to_be_bytes()].concat();
        storage_mut
            .vectors
            .vectors_db
            .put(&mut txn, &key3, &bytes3)
            .unwrap();

        txn.commit().unwrap();
    }

    // Run migration
    let result = migrate(&mut storage_mut);
    assert!(result.is_ok(), "Migration should succeed");

    // Now query using v_from_type on the migrated data
    let storage = Arc::new(storage_mut);
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query for "test_migration" label - should find 2 vectors
    let results_with_data = G::new(&storage, &txn, &arena)
        .v_from_type("test_migration", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results_with_data.len(), 2, "Should find 2 vectors with test_migration label");

    // Verify we got the right vectors
    let ids: Vec<u128> = results_with_data.iter().map(|v| v.id()).collect();
    assert!(ids.contains(&1u128), "Should contain vector 1");
    assert!(ids.contains(&2u128), "Should contain vector 2");

    // Verify vector data is accessible
    if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::Vector(v) = &results_with_data[0] {
        assert_eq!(v.data.len(), 3, "Vector should have 3 dimensions");
    } else {
        panic!("Expected TraversalValue::Vector");
    }

    // Query without vector data to check properties
    let arena2 = Bump::new();
    let results_without_data = G::new(&storage, &txn, &arena2)
        .v_from_type("test_migration", false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results_without_data.len(), 2, "Should still find 2 vectors");

    // Verify properties are preserved after migration
    for result in &results_without_data {
        if let crate::helix_engine::traversal_core::traversal_value::TraversalValue::VectorNodeWithoutVectorData(v) = result {
            assert_eq!(v.label(), "test_migration");

            // Check that properties are accessible
            let props = v.properties.as_ref().unwrap();
            let name = props.get("name");
            assert!(name.is_some(), "name property should exist");

            // Verify it's a string
            match name.unwrap() {
                Value::String(s) => assert!(s == "vector1" || s == "vector2"),
                _ => panic!("Expected name to be a string"),
            }
        }
    }

    // Query for "other_label" - should find 1 vector
    let arena3 = Bump::new();
    let other_results = G::new(&storage, &txn, &arena3)
        .v_from_type("other_label", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(other_results.len(), 1, "Should find 1 vector with other_label");
    assert_eq!(other_results[0].id(), 3u128);

    // Query for non-existent label after migration
    let arena4 = Bump::new();
    let empty_results = G::new(&storage, &txn, &arena4)
        .v_from_type("nonexistent", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(empty_results.is_empty(), "Should find no vectors with nonexistent label");
}

// ============================================================================
// Error Tests for v_from_id
// ============================================================================

#[test]
fn test_v_from_id_with_nonexistent_id_with_data() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Generate a random ID that doesn't exist
    let fake_id = uuid::Uuid::new_v4().as_u128();

    // Attempt to query with include_vector_data = true
    let result = G::new(&storage, &txn, &arena)
        .v_from_id(&fake_id, true)
        .collect_to_obj();

    // Assert it returns VectorError (VectorNotFound)
    assert!(
        matches!(result, Err(GraphError::VectorError(_))),
        "Expected VectorError but got: {:?}",
        result
    );
}

#[test]
fn test_v_from_id_with_nonexistent_id_without_data() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Generate a random ID that doesn't exist
    let fake_id = uuid::Uuid::new_v4().as_u128();

    // Attempt to query with include_vector_data = false
    let result = G::new(&storage, &txn, &arena)
        .v_from_id(&fake_id, false)
        .collect_to_obj();

    // Assert it returns VectorError (VectorNotFound)
    assert!(
        matches!(result, Err(GraphError::VectorError(_))),
        "Expected VectorError but got: {:?}",
        result
    );
}

#[test]
fn test_v_from_id_with_deleted_vector() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a vector
    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[1.0, 2.0, 3.0], "test_vector", None)
        .collect_to_obj()
        .unwrap();
    let vector_id = vector.id();

    txn.commit().unwrap();

    // Delete the vector
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let vector_to_delete = G::new(&storage, &txn, &arena)
        .v_from_id(&vector_id, true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(
        vector_to_delete.into_iter().map(Ok::<_, GraphError>),
        storage.as_ref(),
        &mut txn,
    )
    .unwrap();
    txn.commit().unwrap();

    // Try to query the deleted vector
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let result = G::new(&storage, &txn, &arena)
        .v_from_id(&vector_id, true)
        .collect_to_obj();

    // Assert it returns VectorError (VectorDeleted or VectorNotFound)
    assert!(
        matches!(result, Err(GraphError::VectorError(_))),
        "Expected VectorError but got: {:?}",
        result
    );
}

#[test]
fn test_v_from_id_with_zero_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query with ID = 0
    let result = G::new(&storage, &txn, &arena)
        .v_from_id(&0, true)
        .collect_to_obj();

    // Assert it returns VectorError
    assert!(
        matches!(result, Err(GraphError::VectorError(_))),
        "Expected VectorError but got: {:?}",
        result
    );
}

#[test]
fn test_v_from_id_with_max_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query with ID = u128::MAX
    let result = G::new(&storage, &txn, &arena)
        .v_from_id(&u128::MAX, true)
        .collect_to_obj();

    // Assert it returns VectorError
    assert!(
        matches!(result, Err(GraphError::VectorError(_))),
        "Expected VectorError but got: {:?}",
        result
    );
}
