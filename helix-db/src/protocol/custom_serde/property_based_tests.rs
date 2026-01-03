//! Property-based tests for serialization using proptest
//!
//! This module uses generative testing to verify serialization properties:
//! - Roundtrip property: deserialize(serialize(x)) == x
//! - Semantic equivalence across serialization
//! - Invariant checking (IDs, relationships preserved)
//! - Automatic shrinking on failure

#[cfg(test)]
mod property_based_tests {
    use super::super::test_utils::*;
    use crate::helix_engine::vector_core::HVector;
    use crate::protocol::value::Value;
    use crate::utils::items::{Edge, Node};
    use bumpalo::Bump;
    use proptest::prelude::*;

    // ========================================================================
    // PROPTEST STRATEGIES
    // ========================================================================

    // Strategy for generating valid UTF-8 strings
    fn arb_label() -> impl Strategy<Value = String> {
        prop::string::string_regex("[a-zA-Z0-9_]{1,50}").unwrap()
    }

    // Strategy for generating longer strings
    fn arb_long_string() -> impl Strategy<Value = String> {
        prop::string::string_regex("[a-zA-Z0-9 ]{0,200}").unwrap()
    }

    // Strategy for generating Value types
    fn arb_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<i32>().prop_map(Value::I32),
            any::<i64>().prop_map(Value::I64),
            any::<u32>().prop_map(Value::U32),
            any::<u64>().prop_map(Value::U64),
            any::<f64>()
                .prop_filter("Not NaN", |f| !f.is_nan())
                .prop_map(Value::F64),
            any::<bool>().prop_map(Value::Boolean),
            arb_long_string().prop_map(Value::String),
            Just(Value::Empty),
        ]
    }

    // Strategy for generating property maps
    fn arb_properties() -> impl Strategy<Value = Vec<(String, Value)>> {
        prop::collection::vec(
            (arb_label(), arb_value()),
            0..10, // 0 to 10 properties
        )
    }

    // Strategy for generating vector data
    fn arb_vector_data() -> impl Strategy<Value = Vec<f32>> {
        prop::collection::vec(
            any::<f32>().prop_filter("Not NaN", |f| !f.is_nan()),
            1..128, // 1 to 128 dimensions
        )
    }

    // ========================================================================
    // NODE PROPERTY TESTS
    // ========================================================================

    proptest! {
        #[test]
        fn prop_node_roundtrip_preserves_label(
            label in arb_label(),
            id in any::<u128>(),
        ) {
            let arena = Bump::new();
            let node = create_simple_node(&arena, id, &label);

            let bytes = bincode::serialize(&node).unwrap();

            let arena2 = Bump::new();
            let deserialized = Node::from_bincode_bytes(id, &bytes, &arena2).unwrap();

            prop_assert_eq!(deserialized.label, label.as_str());
            prop_assert_eq!(deserialized.id, id);
        }

        #[test]
        fn prop_node_roundtrip_preserves_version(
            label in arb_label(),
            id in any::<u128>(),
            version in any::<u8>(),
        ) {
            let arena = Bump::new();
            let node = create_arena_node(&arena, id, &label, version, vec![]);

            let bytes = bincode::serialize(&node).unwrap();

            let arena2 = Bump::new();
            let deserialized = Node::from_bincode_bytes(id, &bytes, &arena2).unwrap();

            prop_assert_eq!(deserialized.version, version);
        }

        #[test]
        fn prop_node_roundtrip_with_properties(
            label in arb_label(),
            id in any::<u128>(),
            props in arb_properties(),
        ) {
            let arena = Bump::new();

            // Convert String keys to &str
            let props_refs: Vec<(&str, Value)> = props.iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();

            let node = create_arena_node(&arena, id, &label, 0, props_refs);

            let bytes = bincode::serialize(&node).unwrap();

            let arena2 = Bump::new();
            let deserialized = Node::from_bincode_bytes(id, &bytes, &arena2).unwrap();

            prop_assert_eq!(deserialized.label, label.as_str());
            prop_assert_eq!(deserialized.id, id);

            // Check property count
            match (&node.properties, &deserialized.properties) {
                (None, None) => {},
                (Some(p1), Some(p2)) => {
                    prop_assert_eq!(p1.len(), p2.len());
                }
                _ => prop_assert!(false, "Property presence mismatch"),
            }
        }

        #[test]
        fn prop_node_serialization_idempotent(
            label in arb_label(),
            id in any::<u128>(),
        ) {
            let arena = Bump::new();
            let node = create_simple_node(&arena, id, &label);

            // Serialize twice
            let bytes1 = bincode::serialize(&node).unwrap();
            let bytes2 = bincode::serialize(&node).unwrap();

            // Should produce identical bytes
            prop_assert_eq!(bytes1, bytes2);
        }

        #[test]
        fn prop_node_double_roundtrip_stable(
            label in arb_label(),
            id in any::<u128>(),
        ) {
            let arena = Bump::new();
            let node = create_simple_node(&arena, id, &label);

            // First roundtrip
            let bytes1 = bincode::serialize(&node).unwrap();
            let arena2 = Bump::new();
            let node2 = Node::from_bincode_bytes(id, &bytes1, &arena2).unwrap();

            // Second roundtrip
            let bytes2 = bincode::serialize(&node2).unwrap();
            let arena3 = Bump::new();
            let node3 = Node::from_bincode_bytes(id, &bytes2, &arena3).unwrap();

            // Bytes should be identical
            prop_assert_eq!(bytes1, bytes2);

            // Semantics should be preserved
            prop_assert_eq!(node2.label, node3.label);
            prop_assert_eq!(node2.id, node3.id);
        }
    }

    // ========================================================================
    // EDGE PROPERTY TESTS
    // ========================================================================

    proptest! {
        #[test]
        fn prop_edge_roundtrip_preserves_all_fields(
            label in arb_label(),
            id in any::<u128>(),
            from_node in any::<u128>(),
            to_node in any::<u128>(),
            version in any::<u8>(),
        ) {
            let arena = Bump::new();
            let edge = create_arena_edge(&arena, id, &label, version, from_node, to_node, vec![]);

            let bytes = bincode::serialize(&edge).unwrap();

            let arena2 = Bump::new();
            let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

            prop_assert_eq!(deserialized.label, label.as_str());
            prop_assert_eq!(deserialized.id, id);
            prop_assert_eq!(deserialized.from_node, from_node);
            prop_assert_eq!(deserialized.to_node, to_node);
            prop_assert_eq!(deserialized.version, version);
        }

        #[test]
        fn prop_edge_roundtrip_with_properties(
            label in arb_label(),
            id in any::<u128>(),
            from_node in any::<u128>(),
            to_node in any::<u128>(),
            props in arb_properties(),
        ) {
            let arena = Bump::new();

            let props_refs: Vec<(&str, Value)> = props.iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();

            let edge = create_arena_edge(&arena, id, &label, 0, from_node, to_node, props_refs);

            let bytes = bincode::serialize(&edge).unwrap();

            let arena2 = Bump::new();
            let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

            prop_assert_eq!(deserialized.from_node, from_node);
            prop_assert_eq!(deserialized.to_node, to_node);
        }

        #[test]
        fn prop_edge_serialization_idempotent(
            label in arb_label(),
            id in any::<u128>(),
            from_node in any::<u128>(),
            to_node in any::<u128>(),
        ) {
            let arena = Bump::new();
            let edge = create_simple_edge(&arena, id, &label, from_node, to_node);

            let bytes1 = bincode::serialize(&edge).unwrap();
            let bytes2 = bincode::serialize(&edge).unwrap();

            prop_assert_eq!(bytes1, bytes2);
        }

        #[test]
        fn prop_edge_relationship_preserved(
            label in arb_label(),
            id in any::<u128>(),
            from_node in any::<u128>(),
            to_node in any::<u128>(),
        ) {
            // Property: After serialization, edge relationship must be preserved
            let arena = Bump::new();
            let edge = create_simple_edge(&arena, id, &label, from_node, to_node);

            let bytes = bincode::serialize(&edge).unwrap();

            let arena2 = Bump::new();
            let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

            // Relationship invariant
            prop_assert_eq!(deserialized.from_node, from_node);
            prop_assert_eq!(deserialized.to_node, to_node);
        }
    }

    // ========================================================================
    // VECTOR PROPERTY TESTS
    // ========================================================================

    proptest! {
        #[test]
        fn prop_vector_roundtrip_preserves_data(
            label in arb_label(),
            id in any::<u128>(),
            data in arb_vector_data(),
        ) {
            let arena = Bump::new();
            let vector = create_simple_vector(&arena, id, &label, &data);

            let props_bytes = bincode::serialize(&vector).unwrap();
            let data_bytes = vector.vector_data_to_bytes().unwrap();

            let arena2 = Bump::new();
            let deserialized = HVector::from_bincode_bytes(
                &arena2,
                Some(&props_bytes),
                data_bytes,
                id,
                true,
            ).unwrap();

            prop_assert_eq!(deserialized.label, label.as_str());
            prop_assert_eq!(deserialized.id, id);
            prop_assert_eq!(deserialized.len(), data.len());

            // Check each data point (with floating point tolerance)
            for (i, (&orig, &deser)) in data.iter().zip(deserialized.data_borrowed().iter()).enumerate() {
                let diff = (orig as f64 - deser as f64).abs();
                prop_assert!(diff < 1e-10, "Data mismatch at index {}: {} vs {}", i, orig, deser);
            }
        }

        #[test]
        fn prop_vector_roundtrip_with_properties(
            label in arb_label(),
            id in any::<u128>(),
            data in arb_vector_data(),
            props in arb_properties(),
        ) {
            let arena = Bump::new();

            let props_refs: Vec<(&str, Value)> = props.iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();

            let vector = create_arena_vector(&arena, id, &label, 1, &data, props_refs);

            let props_bytes = bincode::serialize(&vector).unwrap();
            let data_bytes = vector.vector_data_to_bytes().unwrap();

            let arena2 = Bump::new();
            let deserialized = HVector::from_bincode_bytes(
                &arena2,
                Some(&props_bytes),
                data_bytes,
                id,
                true,
            ).unwrap();

            prop_assert_eq!(deserialized.len(), data.len());
        }

        #[test]
        fn prop_vector_data_bytes_roundtrip(
            data in arb_vector_data(),
        ) {
            let arena = Bump::new();

            // Convert to bytes and back
            let bytes = create_vector_bytes(&data);
            let restored = HVector::raw_vector_data_to_vec( &bytes,&arena).unwrap();

            prop_assert_eq!(restored.len(), data.len());

            for (i, (&orig, &rest)) in data.iter().zip(restored.iter()).enumerate() {
                let diff = (orig as f64 - rest as f64).abs();
                prop_assert!(diff < 1e-10, "Data mismatch at index {}: {} vs {}", i, orig, rest);
            }
        }

        #[test]
        fn prop_vector_serialization_idempotent(
            label in arb_label(),
            id in any::<u128>(),
            data in arb_vector_data(),
        ) {
            let arena = Bump::new();
            let vector = create_simple_vector(&arena, id, &label, &data);

            let props_bytes1 = bincode::serialize(&vector).unwrap();
            let data_bytes1 = vector.vector_data_to_bytes().unwrap();

            let props_bytes2 = bincode::serialize(&vector).unwrap();
            let data_bytes2 = vector.vector_data_to_bytes().unwrap();

            prop_assert_eq!(props_bytes1, props_bytes2);
            prop_assert_eq!(data_bytes1, data_bytes2);
        }

        #[test]
        fn prop_vector_double_roundtrip_stable(
            label in arb_label(),
            id in any::<u128>(),
            data in arb_vector_data(),
        ) {
            let arena = Bump::new();
            let vector = create_simple_vector(&arena, id, &label, &data);

            // First roundtrip
            let props_bytes1 = bincode::serialize(&vector).unwrap();
            let data_bytes1 = vector.vector_data_to_bytes().unwrap();
            let arena2 = Bump::new();
            let vector2 = HVector::from_bincode_bytes(
                &arena2,
                Some(&props_bytes1),
                data_bytes1,
                id,
                true,
            ).unwrap();

            // Second roundtrip
            let props_bytes2 = bincode::serialize(&vector2).unwrap();
            let data_bytes2 = vector2.vector_data_to_bytes().unwrap();

            // Bytes should be identical
            prop_assert_eq!(props_bytes1, props_bytes2);
            prop_assert_eq!(data_bytes1, data_bytes2);
        }
    }

    // ========================================================================
    // CROSS-TYPE INVARIANT TESTS
    // ========================================================================

    proptest! {
        #[test]
        fn prop_id_preserved_across_all_types(
            id in any::<u128>(),
            label in arb_label(),
        ) {
            // Node
            let arena_node = Bump::new();
            let node = create_simple_node(&arena_node, id, &label);
            let node_bytes = bincode::serialize(&node).unwrap();
            let arena_node2 = Bump::new();
            let node_restored = Node::from_bincode_bytes(id, &node_bytes, &arena_node2).unwrap();
            prop_assert_eq!(node_restored.id, id);

            // Edge
            let arena_edge = Bump::new();
            let edge = create_simple_edge(&arena_edge, id, &label, 1, 2);
            let edge_bytes = bincode::serialize(&edge).unwrap();
            let arena_edge2 = Bump::new();
            let edge_restored = Edge::from_bincode_bytes(id, &edge_bytes, &arena_edge2).unwrap();
            prop_assert_eq!(edge_restored.id, id);

            // Vector
            let arena_vec = Bump::new();
            let vector = create_simple_vector(&arena_vec, id, &label, &[1.0, 2.0]);
            let props_bytes = bincode::serialize(&vector).unwrap();
            let data_bytes = vector.vector_data_to_bytes().unwrap();
            let arena_vec2 = Bump::new();
            let vector_restored = HVector::from_bincode_bytes(
                &arena_vec2,
                Some(&props_bytes),
                data_bytes,
                id,
                true,
            ).unwrap();
            prop_assert_eq!(vector_restored.id, id);
        }

        #[test]
        fn prop_label_preserved_across_all_types(
            id in any::<u128>(),
            label in arb_label(),
        ) {
            // Node
            let arena_node = Bump::new();
            let node = create_simple_node(&arena_node, id, &label);
            let node_bytes = bincode::serialize(&node).unwrap();
            let arena_node2 = Bump::new();
            let node_restored = Node::from_bincode_bytes(id, &node_bytes, &arena_node2).unwrap();
            prop_assert_eq!(node_restored.label, label.as_str());

            // Edge
            let arena_edge = Bump::new();
            let edge = create_simple_edge(&arena_edge, id, &label, 1, 2);
            let edge_bytes = bincode::serialize(&edge).unwrap();
            let arena_edge2 = Bump::new();
            let edge_restored = Edge::from_bincode_bytes(id, &edge_bytes, &arena_edge2).unwrap();
            prop_assert_eq!(edge_restored.label, label.as_str());

            // Vector
            let arena_vec = Bump::new();
            let vector = create_simple_vector(&arena_vec, id, &label, &[1.0]);
            let props_bytes = bincode::serialize(&vector).unwrap();
            let data_bytes = vector.vector_data_to_bytes().unwrap();
            let arena_vec2 = Bump::new();
            let vector_restored = HVector::from_bincode_bytes(
                &arena_vec2,
                Some(&props_bytes),
                data_bytes,
                id,
                true,
            ).unwrap();
            prop_assert_eq!(vector_restored.label, label.as_str());
        }
    }
}
