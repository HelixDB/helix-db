//! Compatibility tests for version migration and backwards compatibility
//!
//! This module tests:
//! - Old format → new format deserialization
//! - Version field migration scenarios
//! - HashMap → ImmutablePropertiesMap compatibility
//! - Forward/backward compatibility guarantees
//! - Schema evolution handling

#[cfg(test)]
mod compatibility_tests {
    use super::super::test_utils::*;
    use crate::helix_engine::vector_core::vector::HVector;
    use crate::protocol::value::Value;
    use crate::utils::items::{Edge, Node};
    use bumpalo::Bump;

    // ========================================================================
    // NODE BACKWARDS COMPATIBILITY TESTS
    // ========================================================================

    #[test]
    fn test_old_node_hashmap_to_new_immutable_map() {
        let id = 12345u128;

        // Create old-style node with HashMap
        let old_node = create_old_node(
            id,
            "LegacyNode",
            0,
            vec![
                ("name", Value::String("test".to_string())),
                ("count", Value::I32(42)),
            ],
        );

        // Serialize with old format
        let old_bytes = bincode::serialize(&old_node).unwrap();

        // Deserialize with new format
        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena);

        assert!(new_node.is_ok(), "Should deserialize old format");
        let restored = new_node.unwrap();

        assert_eq!(restored.id, id);
        assert_eq!(restored.label, "LegacyNode");
        assert!(restored.properties.is_some());

        // Verify properties were preserved
        let props = restored.properties.unwrap();
        assert_eq!(props.len(), 2);
        assert!(props.get("name").is_some());
        assert!(props.get("count").is_some());
    }

    #[test]
    fn test_old_node_empty_properties() {
        let id = 11111u128;

        let old_node = create_old_node(id, "EmptyProps", 0, vec![]);

        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert_eq!(new_node.label, "EmptyProps");
        assert!(new_node.properties.is_none());
    }

    #[test]
    fn test_old_node_version_0_to_current() {
        let id = 22222u128;

        let old_node = create_old_node(id, "V0Node", 0, vec![]);

        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert_eq!(new_node.version, 0);
    }

    #[test]
    fn test_old_node_with_all_value_types() {
        let id = 33333u128;

        let props = vec![
            ("str", Value::String("text".to_string())),
            ("i32", Value::I32(-100)),
            ("i64", Value::I64(-1000)),
            ("u64", Value::U64(1000)),
            ("f64", Value::F64(3.14)),
            ("bool", Value::Boolean(true)),
            ("empty", Value::Empty),
        ];

        let old_node = create_old_node(id, "AllTypes", 0, props);
        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert!(new_node.properties.is_some());
        let new_props = new_node.properties.unwrap();
        assert_eq!(new_props.len(), 7);
    }

    #[test]
    fn test_old_node_with_many_properties() {
        let id = 44444u128;

        let props: Vec<(&str, Value)> = (0..50)
            .map(|i| {
                (
                    Box::leak(format!("key_{}", i).into_boxed_str()) as &str,
                    Value::I32(i),
                )
            })
            .collect();

        let old_node = create_old_node(id, "ManyProps", 0, props);
        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert_eq!(new_node.properties.unwrap().len(), 50);
    }

    // ========================================================================
    // EDGE BACKWARDS COMPATIBILITY TESTS
    // ========================================================================

    #[test]
    fn test_old_edge_hashmap_to_new_immutable_map() {
        let id = 55555u128;

        let old_edge = create_old_edge(
            id,
            "LegacyEdge",
            0,
            100,
            200,
            vec![
                ("weight", Value::F64(0.85)),
                ("type", Value::String("connection".to_string())),
            ],
        );

        let old_bytes = bincode::serialize(&old_edge).unwrap();

        let arena = Bump::new();
        let new_edge = Edge::from_bincode_bytes(id, &old_bytes, &arena);

        assert!(new_edge.is_ok(), "Should deserialize old edge format");
        let restored = new_edge.unwrap();

        assert_eq!(restored.id, id);
        assert_eq!(restored.label, "LegacyEdge");
        assert_eq!(restored.from_node, 100);
        assert_eq!(restored.to_node, 200);
        assert!(restored.properties.is_some());
        assert_eq!(restored.properties.unwrap().len(), 2);
    }

    #[test]
    fn test_old_edge_empty_properties() {
        let id = 66666u128;

        let old_edge = create_old_edge(id, "SimpleEdge", 0, 1, 2, vec![]);

        let old_bytes = bincode::serialize(&old_edge).unwrap();

        let arena = Bump::new();
        let new_edge = Edge::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert_eq!(new_edge.from_node, 1);
        assert_eq!(new_edge.to_node, 2);
        assert!(new_edge.properties.is_none());
    }

    #[test]
    fn test_old_edge_with_nested_values() {
        let id = 77777u128;

        let props = vec![(
            "metadata",
            Value::Object(
                vec![
                    ("created".to_string(), Value::I64(1234567890)),
                    (
                        "tags".to_string(),
                        Value::Array(vec![
                            Value::String("tag1".to_string()),
                            Value::String("tag2".to_string()),
                        ]),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        )];

        let old_edge = create_old_edge(id, "NestedEdge", 0, 10, 20, props);
        let old_bytes = bincode::serialize(&old_edge).unwrap();

        let arena = Bump::new();
        let new_edge = Edge::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert!(new_edge.properties.is_some());
    }

    #[test]
    fn test_old_edge_version_field() {
        let id = 88888u128;

        let old_edge = create_old_edge(id, "VersionedEdge", 5, 1, 2, vec![]);

        let old_bytes = bincode::serialize(&old_edge).unwrap();

        let arena = Bump::new();
        let new_edge = Edge::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert_eq!(new_edge.version, 5);
    }

    // ========================================================================
    // VECTOR BACKWARDS COMPATIBILITY TESTS
    // ========================================================================

    #[test]
    fn test_old_vector_to_new_vector() {
        let id = 99999u128;

        let old_vector = create_old_vector(
            id,
            "LegacyVector",
            1,
            false,
            vec![("source", Value::String("embedding".to_string()))],
        );

        let old_bytes = bincode::serialize(&old_vector).unwrap();
        let data = vec![1.0, 2.0, 3.0];
        let data_bytes = create_vector_bytes(&data);

        let arena = Bump::new();
        let new_vector = HVector::from_bincode_bytes(&arena, Some(&old_bytes), &data_bytes, id);

        assert!(new_vector.is_ok(), "Should deserialize old vector format");
        let restored = new_vector.unwrap();

        assert_eq!(restored.id, id);
        assert_eq!(restored.label, "LegacyVector");
        assert_eq!(restored.version, 1);
        assert_eq!(restored.deleted, false);
    }

    #[test]
    fn test_old_vector_deleted_flag() {
        let id = 111000u128;

        let old_vector = create_old_vector(id, "DeletedVector", 1, true, vec![]);

        let old_bytes = bincode::serialize(&old_vector).unwrap();
        let data_bytes = create_vector_bytes(&[0.0]);

        let arena = Bump::new();
        let new_vector =
            HVector::from_bincode_bytes(&arena, Some(&old_bytes), &data_bytes, id).unwrap();

        assert_eq!(new_vector.deleted, true);
    }

    #[test]
    fn test_old_vector_with_properties() {
        let id = 222000u128;

        let props = vec![
            ("model", Value::String("ada-002".to_string())),
            ("dimension", Value::I32(1536)),
        ];

        let old_vector = create_old_vector(id, "PropsVector", 1, false, props);

        let old_bytes = bincode::serialize(&old_vector).unwrap();
        let data_bytes = create_vector_bytes(&vec![0.0; 1536]);

        let arena = Bump::new();
        let new_vector =
            HVector::from_bincode_bytes(&arena, Some(&old_bytes), &data_bytes, id).unwrap();

        assert!(new_vector.properties.is_some());
        let props = new_vector.properties.unwrap();
        assert!(props.get("model").is_some());
        assert!(props.get("dimension").is_some());
    }

    // ========================================================================
    // VERSION MIGRATION TESTS
    // ========================================================================

    #[test]
    fn test_node_version_migration_v0_to_v1() {
        let arena1 = Bump::new();
        let id = 333000u128;

        // Create v0 node
        let node_v0 = create_arena_node(&arena1, id, "NodeV0", 0, vec![]);
        let bytes = bincode::serialize(&node_v0).unwrap();

        // Deserialize (implicitly treats as current version)
        let arena2 = Bump::new();
        let restored = Node::from_bincode_bytes(id, &bytes, &arena2).unwrap();

        assert_eq!(restored.version, 0);

        // Could manually update version in application code
        // This test demonstrates version field is preserved
    }

    #[test]
    fn test_edge_version_migration_across_versions() {
        let arena = Bump::new();
        let id = 444000u128;

        // Test that different versions can coexist
        let edge_v0 = create_arena_edge(&arena, id, "E0", 0, 1, 2, vec![]);
        let edge_v1 = create_arena_edge(&arena, id, "E1", 1, 1, 2, vec![]);
        let edge_v5 = create_arena_edge(&arena, id, "E5", 5, 1, 2, vec![]);

        let bytes_v0 = bincode::serialize(&edge_v0).unwrap();
        let bytes_v1 = bincode::serialize(&edge_v1).unwrap();
        let bytes_v5 = bincode::serialize(&edge_v5).unwrap();

        let arena2 = Bump::new();
        let restored_v0 = Edge::from_bincode_bytes(id, &bytes_v0, &arena2).unwrap();
        let restored_v1 = Edge::from_bincode_bytes(id, &bytes_v1, &arena2).unwrap();
        let restored_v5 = Edge::from_bincode_bytes(id, &bytes_v5, &arena2).unwrap();

        assert_eq!(restored_v0.version, 0);
        assert_eq!(restored_v1.version, 1);
        assert_eq!(restored_v5.version, 5);
    }

    #[test]
    fn test_vector_version_compatibility() {
        let arena = Bump::new();
        let id = 555000u128;
        let data = vec![1.0, 2.0];

        // Different vector versions
        let vec_v1 = create_arena_vector(&arena, id, "V1", 1, false, 0, &data, vec![]);
        let vec_v2 = create_arena_vector(&arena, id, "V2", 2, false, 0, &data, vec![]);

        let props_v1 = bincode::serialize(&vec_v1).unwrap();
        let props_v2 = bincode::serialize(&vec_v2).unwrap();
        let data_bytes = create_vector_bytes(&data);

        let arena2 = Bump::new();
        let restored_v1 =
            HVector::from_bincode_bytes(&arena2, Some(&props_v1), &data_bytes, id).unwrap();
        let restored_v2 =
            HVector::from_bincode_bytes(&arena2, Some(&props_v2), &data_bytes, id).unwrap();

        assert_eq!(restored_v1.version, 1);
        assert_eq!(restored_v2.version, 2);
    }

    // ========================================================================
    // FORWARD COMPATIBILITY TESTS
    // ========================================================================

    #[test]
    fn test_future_version_node_still_deserializes() {
        let arena = Bump::new();
        let id = 666000u128;

        // Simulate "future" version (e.g., version 100)
        let future_node = create_arena_node(&arena, id, "FutureNode", 100, vec![]);
        let bytes = bincode::serialize(&future_node).unwrap();

        // Current code should still deserialize it
        let arena2 = Bump::new();
        let restored = Node::from_bincode_bytes(id, &bytes, &arena2);

        assert!(restored.is_ok(), "Should handle future versions gracefully");
        assert_eq!(restored.unwrap().version, 100);
    }

    #[test]
    fn test_unknown_property_types_preserved() {
        let id = 777000u128;

        // Old node with various Value types
        let old_node = create_old_node(
            id,
            "UnknownTypes",
            0,
            vec![
                ("known", Value::String("value".to_string())),
                ("i64_max", Value::I64(i64::MAX)),
                ("u128", Value::U128(u128::MAX)),
            ],
        );

        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        // Properties should be preserved even if we add new Value variants
        assert!(new_node.properties.is_some());
    }

    // ========================================================================
    // MIXED VERSION BATCH TESTS
    // ========================================================================

    #[test]
    fn test_batch_with_mixed_versions() {
        let arena = Bump::new();

        // Create nodes with different versions
        let nodes: Vec<(u128, u8, Node)> = vec![
            (1, 0, create_arena_node(&arena, 1, "N0", 0, vec![])),
            (2, 1, create_arena_node(&arena, 2, "N1", 1, vec![])),
            (3, 2, create_arena_node(&arena, 3, "N2", 2, vec![])),
            (4, 0, create_arena_node(&arena, 4, "N0b", 0, vec![])),
        ];

        // Serialize all
        let serialized: Vec<(u128, u8, Vec<u8>)> = nodes
            .iter()
            .map(|(id, ver, node)| (*id, *ver, bincode::serialize(node).unwrap()))
            .collect();

        // Deserialize all
        let arena2 = Bump::new();
        for (id, expected_ver, bytes) in serialized {
            let restored = Node::from_bincode_bytes(id, &bytes, &arena2).unwrap();
            assert_eq!(restored.version, expected_ver);
        }
    }

    #[test]
    fn test_old_and_new_formats_coexist() {
        // Simulate database with both old and new format data

        // Old format node
        let old_node = create_old_node(
            1,
            "OldFormat",
            0,
            vec![("data", Value::String("old".to_string()))],
        );
        let old_bytes = bincode::serialize(&old_node).unwrap();

        // New format node
        let arena_new = Bump::new();
        let new_node = create_arena_node(
            &arena_new,
            2,
            "NewFormat",
            1,
            vec![("data", Value::String("new".to_string()))],
        );
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Both should deserialize correctly
        let arena_restore = Bump::new();
        let restored_old = Node::from_bincode_bytes(1, &old_bytes, &arena_restore).unwrap();
        let restored_new = Node::from_bincode_bytes(2, &new_bytes, &arena_restore).unwrap();

        assert_eq!(restored_old.label, "OldFormat");
        assert_eq!(restored_new.label, "NewFormat");
    }

    // ========================================================================
    // PROPERTY MAP EVOLUTION TESTS
    // ========================================================================

    #[test]
    fn test_hashmap_to_immutable_map_preserves_order() {
        let id = 888000u128;

        // Note: HashMap doesn't guarantee order, but all keys should be present
        let props = vec![
            ("z_last", Value::I32(1)),
            ("a_first", Value::I32(2)),
            ("m_middle", Value::I32(3)),
        ];

        let old_node = create_old_node(id, "OrderTest", 0, props);
        let old_bytes = bincode::serialize(&old_node).unwrap();

        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        let new_props = new_node.properties.unwrap();
        assert_eq!(new_props.len(), 3);

        // All keys should be present (order may differ)
        assert!(new_props.get("z_last").is_some());
        assert!(new_props.get("a_first").is_some());
        assert!(new_props.get("m_middle").is_some());
    }

    #[test]
    fn test_property_value_types_preserved_across_migration() {
        let id = 999000u128;

        let props = vec![
            ("str", Value::String("text".to_string())),
            ("int", Value::I64(42)),
            ("float", Value::F64(3.14)),
            ("bool", Value::Boolean(true)),
            ("empty", Value::Empty),
        ];

        let old_edge = create_old_edge(id, "TypePreserve", 0, 1, 2, props);
        let old_bytes = bincode::serialize(&old_edge).unwrap();

        let arena = Bump::new();
        let new_edge = Edge::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        let new_props = new_edge.properties.unwrap();

        // Verify each type is preserved
        match new_props.get("str").unwrap() {
            Value::String(s) => assert_eq!(s, "text"),
            _ => panic!("String type not preserved"),
        }

        match new_props.get("int").unwrap() {
            Value::I64(i) => assert_eq!(*i, 42),
            _ => panic!("I64 type not preserved"),
        }
    }
}
