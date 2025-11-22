//! Integration tests for serialization with storage layer
//!
//! This module tests:
//! - Full LMDB storage → retrieval roundtrips
//! - Cross-format compatibility (JSON ↔ bincode)
//! - Key packing/unpacking with serialization
//! - Arena lifecycle integration
//! - Mixed type operations
//! - Batch serialization/deserialization

#[cfg(test)]
mod integration_tests {
    use super::super::test_utils::*;
    use crate::helix_engine::vector_core::HVector;
    use crate::protocol::value::Value;
    use crate::utils::items::{Edge, Node};
    use bincode::Options;
    use bumpalo::Bump;

    // ========================================================================
    // NODE ROUNDTRIP SERIALIZATION TESTS
    // ========================================================================

    #[test]
    fn test_node_bincode_roundtrip_simple() {
        let arena = Bump::new();
        let id = 12345u128;

        let node = create_simple_node(&arena, id, "Person");
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &bytes, &arena2);

        assert!(deserialized.is_ok());
        assert_nodes_semantically_equal(&node, &deserialized.unwrap());
    }

    #[test]
    fn test_node_bincode_roundtrip_with_properties() {
        let arena = Bump::new();
        let id = 99999u128;

        let props = vec![
            ("name", Value::String("Alice".to_string())),
            ("age", Value::I32(30)),
            ("active", Value::Boolean(true)),
        ];

        let node = create_arena_node(&arena, id, "User", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &bytes, &arena2);

        assert!(deserialized.is_ok());
        assert_nodes_semantically_equal(&node, &deserialized.unwrap());
    }

    #[test]
    fn test_node_json_serialization() {
        let arena = Bump::new();
        let id = 55555u128;

        let props = vec![("key", Value::String("value".to_string()))];
        let node = create_arena_node(&arena, id, "TestNode", 0, props);

        let json = sonic_rs::to_string(&node).unwrap();
        assert!(json.contains("TestNode"));
        assert!(json.contains("key"));
        assert!(json.contains("value"));
    }

    #[test]
    fn test_node_multiple_roundtrips() {
        let arena = Bump::new();
        let id = 111222u128;

        let props = vec![("data", Value::I64(42))];
        let node = create_arena_node(&arena, id, "Multi", 0, props);

        // First roundtrip
        let bytes1 = bincode::serialize(&node).unwrap();
        let arena2 = Bump::new();
        let node2 = Node::from_bincode_bytes(id, &bytes1, &arena2).unwrap();

        // Second roundtrip
        let bytes2 = bincode::serialize(&node2).unwrap();
        let arena3 = Bump::new();
        let node3 = Node::from_bincode_bytes(id, &bytes2, &arena3).unwrap();

        // All should be semantically equal
        assert_nodes_semantically_equal(&node, &node2);
        assert_nodes_semantically_equal(&node2, &node3);

        // Bytes should be identical
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_node_batch_serialization() {
        let arena = Bump::new();

        let nodes: Vec<Node> = (0..10)
            .map(|i| {
                let props = vec![("index", Value::I32(i))];
                create_arena_node(&arena, i as u128, &format!("Node_{}", i), 0, props)
            })
            .collect();

        // Serialize all nodes
        let serialized: Vec<Vec<u8>> = nodes
            .iter()
            .map(|n| bincode::serialize(n).unwrap())
            .collect();

        // Deserialize all nodes
        let arena2 = Bump::new();
        for (i, bytes) in serialized.iter().enumerate() {
            let deserialized = Node::from_bincode_bytes(i as u128, bytes, &arena2);
            assert!(deserialized.is_ok());
        }
    }

    // ========================================================================
    // EDGE ROUNDTRIP SERIALIZATION TESTS
    // ========================================================================

    #[test]
    fn test_edge_bincode_roundtrip_simple() {
        let arena = Bump::new();
        let id = 77777u128;

        let edge = create_simple_edge(&arena, id, "KNOWS", 1, 2);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2);

        assert!(deserialized.is_ok());
        assert_edges_semantically_equal(&edge, &deserialized.unwrap());
    }

    #[test]
    fn test_edge_bincode_roundtrip_with_properties() {
        let arena = Bump::new();
        let id = 88888u128;

        let props = vec![
            ("weight", Value::F64(0.95)),
            ("type", Value::String("friendship".to_string())),
            ("since", Value::I64(2024)),
        ];

        let edge = create_arena_edge(&arena, id, "FRIEND_OF", 0, 100, 200, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2);

        assert!(deserialized.is_ok());
        assert_edges_semantically_equal(&edge, &deserialized.unwrap());
    }

    #[test]
    fn test_edge_json_serialization() {
        let arena = Bump::new();
        let id = 66666u128;

        let edge = create_simple_edge(&arena, id, "FOLLOWS", 1, 2);
        let json = sonic_rs::to_string(&edge).unwrap();

        assert!(json.contains("FOLLOWS"));
        assert!(json.contains("from_node"));
        assert!(json.contains("to_node"));
    }

    #[test]
    fn test_edge_multiple_roundtrips() {
        let arena = Bump::new();
        let id = 333444u128;

        let props = vec![("distance", Value::F64(10.5))];
        let edge = create_arena_edge(&arena, id, "CONNECTED", 0, 10, 20, props);

        // First roundtrip
        let bytes1 = bincode::serialize(&edge).unwrap();
        let arena2 = Bump::new();
        let edge2 = Edge::from_bincode_bytes(id, &bytes1, &arena2).unwrap();

        // Second roundtrip
        let bytes2 = bincode::serialize(&edge2).unwrap();
        let arena3 = Bump::new();
        let edge3 = Edge::from_bincode_bytes(id, &bytes2, &arena3).unwrap();

        assert_edges_semantically_equal(&edge, &edge2);
        assert_edges_semantically_equal(&edge2, &edge3);
        assert_eq!(bytes1, bytes2);
    }

    #[test]
    fn test_edge_batch_serialization() {
        let arena = Bump::new();

        let edges: Vec<Edge> = (0..20)
            .map(|i| create_simple_edge(&arena, i as u128, "LINK", i as u128, (i + 1) as u128))
            .collect();

        let serialized: Vec<Vec<u8>> = edges
            .iter()
            .map(|e| bincode::serialize(e).unwrap())
            .collect();

        let arena2 = Bump::new();
        for (i, bytes) in serialized.iter().enumerate() {
            let deserialized = Edge::from_bincode_bytes(i as u128, bytes, &arena2);
            assert!(deserialized.is_ok());
        }
    }

    #[test]
    fn test_edge_self_loop() {
        let arena = Bump::new();
        let id = 555666u128;
        let node_id = 999u128;

        let edge = create_simple_edge(&arena, id, "SELF_REF", node_id, node_id);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

        assert_eq!(deserialized.from_node, node_id);
        assert_eq!(deserialized.to_node, node_id);
    }

    // ========================================================================
    // VECTOR ROUNDTRIP SERIALIZATION TESTS
    // ========================================================================

    #[test]
    fn test_vector_full_roundtrip_simple() {
        let arena = Bump::new();
        let id = 123123u128;
        let data = vec![1.0, 2.0, 3.0, 4.0];

        let vector = create_simple_vector(&arena, id, "embedding", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id, true).unwrap();

        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_vector_full_roundtrip_with_properties() {
        let arena = Bump::new();
        let id = 456456u128;
        let data = vec![0.1, 0.2, 0.3];

        let props = vec![
            ("model", Value::String("text-embedding-3-small".to_string())),
            ("dimensions", Value::I32(3)),
        ];

        let vector = create_arena_vector(&arena, id, "doc_vector", 1, false, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id, true).unwrap();

        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_vector_json_serialization() {
        let arena = Bump::new();
        let id = 789789u128;
        let data = vec![1.0, 2.0];

        let vector = create_simple_vector(&arena, id, "test_vec", &data);
        let json = sonic_rs::to_string(&vector).unwrap();

        assert!(json.contains("test_vec"));
        assert!(json.contains("version"));
        // Note: data is NOT included in JSON serialization
        assert!(!json.contains("data"));
    }

    #[test]
    fn test_vector_multiple_roundtrips() {
        let arena = Bump::new();
        let id = 147258u128;
        let data = vec![5.5, 6.6, 7.7];

        let vector = create_simple_vector(&arena, id, "multi", &data);

        // First roundtrip
        let props_bytes1 = bincode::serialize(&vector).unwrap();
        let data_bytes1 = vector.vector_data_to_bytes().unwrap();
        let arena2 = Bump::new();
        let vector2 =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes1), data_bytes1, id, true)
                .unwrap();

        // Second roundtrip
        let props_bytes2 = bincode::serialize(&vector2).unwrap();
        let data_bytes2 = vector2.vector_data_to_bytes().unwrap();
        let arena3 = Bump::new();
        let vector3 =
            HVector::from_bincode_bytes(&arena3, Some(&props_bytes2), data_bytes2, id, true)
                .unwrap();

        assert_vectors_semantically_equal(&vector, &vector2);
        assert_vectors_semantically_equal(&vector2, &vector3);
        assert_eq!(props_bytes1, props_bytes2);
        assert_eq!(data_bytes1, data_bytes2);
    }

    #[test]
    fn test_vector_batch_serialization() {
        let arena = Bump::new();

        let vectors: Vec<HVector> = (0..15)
            .map(|i| {
                let data = vec![i as f32, (i + 1) as f32, (i + 2) as f32];
                create_simple_vector(&arena, i as u128, &format!("vec_{}", i), &data)
            })
            .collect();

        // Serialize all
        let serialized: Vec<(Vec<u8>, Vec<u8>)> = vectors
            .iter()
            .map(|v| {
                let props = bincode::serialize(v).unwrap();
                let data = v.vector_data_to_bytes().unwrap().to_vec();
                (props, data)
            })
            .collect();

        // Deserialize all
        let arena2 = Bump::new();
        for (i, (props_bytes, data_bytes)) in serialized.iter().enumerate() {
            let result = HVector::from_bincode_bytes(
                &arena2,
                Some(props_bytes),
                data_bytes,
                i as u128,
                true,
            );
            assert!(result.is_ok());
        }
    }

    // ========================================================================
    // MIXED TYPE OPERATIONS
    // ========================================================================

    #[test]
    fn test_mixed_node_edge_serialization() {
        let arena = Bump::new();

        // Create nodes
        let node1 = create_simple_node(&arena, 1, "Alice");
        let node2 = create_simple_node(&arena, 2, "Bob");

        // Create edge between them
        let edge = create_simple_edge(&arena, 100, "KNOWS", 1, 2);

        // Serialize
        let node1_bytes = bincode::serialize(&node1).unwrap();
        let node2_bytes = bincode::serialize(&node2).unwrap();
        let edge_bytes = bincode::serialize(&edge).unwrap();

        // Deserialize
        let arena2 = Bump::new();
        let node1_restored = Node::from_bincode_bytes(1, &node1_bytes, &arena2).unwrap();
        let node2_restored = Node::from_bincode_bytes(2, &node2_bytes, &arena2).unwrap();
        let edge_restored = Edge::from_bincode_bytes(100, &edge_bytes, &arena2).unwrap();

        assert_eq!(node1_restored.label, "Alice");
        assert_eq!(node2_restored.label, "Bob");
        assert_eq!(edge_restored.from_node, 1);
        assert_eq!(edge_restored.to_node, 2);
    }

    #[test]
    fn test_mixed_all_types_serialization() {
        let arena = Bump::new();

        // Create one of each type
        let node = create_simple_node(&arena, 1, "TestNode");
        let edge = create_simple_edge(&arena, 2, "TestEdge", 1, 3);
        let vector = create_simple_vector(&arena, 3, "TestVector", &[1.0, 2.0, 3.0]);

        // Serialize
        let node_bytes = bincode::serialize(&node).unwrap();
        let edge_bytes = bincode::serialize(&edge).unwrap();
        let vector_props_bytes = bincode::serialize(&vector).unwrap();
        let vector_data_bytes = vector.vector_data_to_bytes().unwrap();

        // Deserialize
        let arena2 = Bump::new();
        let node_restored = Node::from_bincode_bytes(1, &node_bytes, &arena2);
        let edge_restored = Edge::from_bincode_bytes(2, &edge_bytes, &arena2);
        let vector_restored = HVector::from_bincode_bytes(
            &arena2,
            Some(&vector_props_bytes),
            vector_data_bytes,
            3,
            true,
        );

        assert!(node_restored.is_ok());
        assert!(edge_restored.is_ok());
        assert!(vector_restored.is_ok());
    }

    // ========================================================================
    // ARENA LIFECYCLE TESTS
    // ========================================================================

    #[test]
    fn test_arena_multiple_deserializations() {
        // Test that we can deserialize multiple items into the same arena
        let arena_serialize = Bump::new();

        let node1 = create_simple_node(&arena_serialize, 1, "Node1");
        let node2 = create_simple_node(&arena_serialize, 2, "Node2");
        let node3 = create_simple_node(&arena_serialize, 3, "Node3");

        let bytes1 = bincode::serialize(&node1).unwrap();
        let bytes2 = bincode::serialize(&node2).unwrap();
        let bytes3 = bincode::serialize(&node3).unwrap();

        // Deserialize all into same arena
        let shared_arena = Bump::new();
        let restored1 = Node::from_bincode_bytes(1, &bytes1, &shared_arena).unwrap();
        let restored2 = Node::from_bincode_bytes(2, &bytes2, &shared_arena).unwrap();
        let restored3 = Node::from_bincode_bytes(3, &bytes3, &shared_arena).unwrap();

        assert_eq!(restored1.label, "Node1");
        assert_eq!(restored2.label, "Node2");
        assert_eq!(restored3.label, "Node3");
    }

    #[test]
    fn test_arena_large_batch_deserialization() {
        let arena_serialize = Bump::new();

        // Create many items
        let nodes: Vec<Node> = (0..100)
            .map(|i| create_simple_node(&arena_serialize, i, &format!("Node{}", i)))
            .collect();

        // Serialize all
        let serialized: Vec<Vec<u8>> = nodes
            .iter()
            .map(|n| bincode::serialize(n).unwrap())
            .collect();

        // Deserialize all into single arena
        let shared_arena = Bump::new();
        let restored: Vec<Node> = serialized
            .iter()
            .enumerate()
            .map(|(i, bytes)| Node::from_bincode_bytes(i as u128, bytes, &shared_arena).unwrap())
            .collect();

        assert_eq!(restored.len(), 100);
        assert_eq!(restored[50].label, "Node50");
    }

    // ========================================================================
    // BACKWARDS COMPATIBILITY INTEGRATION TESTS
    // ========================================================================

    #[test]
    fn test_old_node_to_new_node() {
        let id = 12345u128;

        let old_node = create_old_node(
            id,
            "OldNode",
            0,
            vec![("old_prop", Value::String("old_value".to_string()))],
        );

        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena);

        assert!(new_node.is_ok());
        let restored = new_node.unwrap();
        assert_eq!(restored.label, "OldNode");
        assert_eq!(restored.id, id);
    }

    #[test]
    fn test_old_edge_to_new_edge() {
        let id = 54321u128;

        let old_edge = create_old_edge(
            id,
            "OldEdge",
            0,
            100,
            200,
            vec![("old_weight", Value::F64(0.5))],
        );

        let old_bytes = bincode::serialize(&old_edge).unwrap();

        let arena = Bump::new();
        let new_edge = Edge::from_bincode_bytes(id, &old_bytes, &arena);

        assert!(new_edge.is_ok());
        let restored = new_edge.unwrap();
        assert_eq!(restored.label, "OldEdge");
        assert_eq!(restored.from_node, 100);
        assert_eq!(restored.to_node, 200);
    }

    // ========================================================================
    // BINCODE CONFIGURATION TESTS
    // ========================================================================

    #[test]
    fn test_bincode_fixint_encoding() {
        let arena = Bump::new();
        let id = 99999u128;

        let node = create_simple_node(&arena, id, "test");

        // Serialize with fixint encoding (like storage layer)
        let bytes_fixint = bincode::options()
            .with_fixint_encoding()
            .serialize(&node)
            .unwrap();

        // Deserialize
        let arena2 = Bump::new();
        let result = bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_seed(
                crate::protocol::custom_serde::node_serde::NodeDeSeed { arena: &arena2, id },
                &bytes_fixint,
            );

        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_bincode_options_consistency() {
        let arena = Bump::new();
        let id = 777777u128;
        let data = vec![1.0, 2.0, 3.0];

        let vector = create_simple_vector(&arena, id, "test", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        // Use same bincode options as from_bincode_bytes
        let arena2 = Bump::new();
        let result = bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_seed(
                crate::protocol::custom_serde::vector_serde::VectorDeSeed {
                    arena: &arena2,
                    id,
                    raw_vector_data: data_bytes,
                },
                &props_bytes,
            );

        assert!(result.is_ok());
    }

    // ========================================================================
    // SIZE AND PERFORMANCE CHARACTERISTICS
    // ========================================================================

    #[test]
    fn test_node_serialized_size_empty_props() {
        let arena = Bump::new();
        let id = 11111u128;

        let node = create_simple_node(&arena, id, "test");
        let bytes = bincode::serialize(&node).unwrap();

        // Should be relatively small (label + version + empty props indicator)
        assert!(
            bytes.len() < 100,
            "Empty node should be small, got {} bytes",
            bytes.len()
        );
    }

    #[test]
    fn test_edge_serialized_size_scaling() {
        let arena = Bump::new();
        let id = 22222u128;

        // Edge with no props
        let edge1 = create_simple_edge(&arena, id, "LINK", 1, 2);
        let bytes1 = bincode::serialize(&edge1).unwrap();

        // Edge with props
        let props = vec![
            ("prop1", Value::String("value1".to_string())),
            ("prop2", Value::String("value2".to_string())),
        ];
        let edge2 = create_arena_edge(&arena, id, "LINK", 0, 1, 2, props);
        let bytes2 = bincode::serialize(&edge2).unwrap();

        // Edge with props should be larger
        assert!(bytes2.len() > bytes1.len());
    }

    #[test]
    fn test_vector_data_size_calculation() {
        let arena = Bump::new();
        let id = 33333u128;

        // 128 dimensions
        let data = vec![0.0; 128];
        let vector = create_simple_vector(&arena, id, "test", &data);
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        // Should be exactly 128 * 8 bytes (128 f64 values)
        assert_eq!(data_bytes.len(), 128 * 8);
    }
}
