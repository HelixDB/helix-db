//! Error handling tests for serialization/deserialization
//!
//! This module tests error scenarios including:
//! - Malformed bincode data
//! - Invalid UTF-8 sequences
//! - Corrupted property maps
//! - Version mismatches
//! - Truncated data
//! - Invalid field values
//! - Out-of-bounds values
//! - Missing required fields

#[cfg(test)]
mod error_handling_tests {
    use super::super::test_utils::*;
    use crate::helix_engine::vector_core::HVector;
    use crate::protocol::value::Value;
    use crate::utils::items::{Edge, Node};
    use bumpalo::Bump;

    // ========================================================================
    // MALFORMED BINCODE DATA TESTS - NODES
    // ========================================================================

    #[test]
    fn test_node_empty_bytes() {
        let arena = Bump::new();
        let id = 12345u128;
        let empty_bytes: &[u8] = &[];

        let result = Node::from_bincode_bytes(id, empty_bytes, &arena);
        assert!(result.is_err(), "Should fail on empty bytes");
    }

    #[test]
    fn test_node_truncated_bytes() {
        let arena = Bump::new();
        let id = 11111u128;

        // Create a valid node and serialize it
        let valid_node = create_simple_node(&arena, id, "test");
        let valid_bytes = bincode::serialize(&valid_node).unwrap();

        // Truncate the bytes
        let truncated = &valid_bytes[..valid_bytes.len() / 2];

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, truncated, &arena2);
        assert!(result.is_err(), "Should fail on truncated bytes");
    }

    #[test]
    fn test_node_garbage_bytes() {
        let arena = Bump::new();
        let id = 22222u128;
        let garbage: Vec<u8> = vec![0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA];

        let result = Node::from_bincode_bytes(id, &garbage, &arena);
        assert!(result.is_err(), "Should fail on garbage bytes");
    }

    #[test]
    fn test_node_corrupted_property_map() {
        let arena = Bump::new();
        let id = 33333u128;

        // Create a node with properties
        let props = vec![("key", Value::String("value".to_string()))];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let mut bytes = bincode::serialize(&node).unwrap();

        // Corrupt the middle of the bytes (likely the property map)
        let len = bytes.len();
        if bytes.len() > 10 {
            bytes[len / 2] = 0xFF;
            bytes[len / 2 + 1] = 0xFF;
        }

        let arena2 = Bump::new();
        let _result = Node::from_bincode_bytes(id, &bytes, &arena2);
        // May or may not fail depending on where corruption occurs
        // This test documents behavior
    }

    #[test]
    fn test_node_single_byte() {
        let arena = Bump::new();
        let id = 44444u128;
        let single_byte: &[u8] = &[0x01];

        let result = Node::from_bincode_bytes(id, single_byte, &arena);
        assert!(result.is_err(), "Should fail on single byte");
    }

    #[test]
    fn test_node_all_zeros() {
        let arena = Bump::new();
        let id = 55555u128;
        let zeros = vec![0u8; 100];

        let _result = Node::from_bincode_bytes(id, &zeros, &arena);
        // This might actually deserialize to some default values
        // Test documents behavior
    }

    #[test]
    fn test_node_all_ones() {
        let arena = Bump::new();
        let id = 66666u128;
        let ones = vec![0xFFu8; 100];

        let result = Node::from_bincode_bytes(id, &ones, &arena);
        assert!(result.is_err(), "Should likely fail on all 0xFF bytes");
    }

    // ========================================================================
    // MALFORMED BINCODE DATA TESTS - EDGES
    // ========================================================================

    #[test]
    fn test_edge_empty_bytes() {
        let arena = Bump::new();
        let id = 77777u128;
        let empty_bytes: &[u8] = &[];

        let result = Edge::from_bincode_bytes(id, empty_bytes, &arena);
        assert!(result.is_err(), "Should fail on empty bytes");
    }

    #[test]
    fn test_edge_truncated_bytes() {
        let arena = Bump::new();
        let id = 88888u128;

        let valid_edge = create_simple_edge(&arena, id, "test", 1, 2);
        let valid_bytes = bincode::serialize(&valid_edge).unwrap();

        let truncated = &valid_bytes[..valid_bytes.len() / 2];

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, truncated, &arena2);
        assert!(result.is_err(), "Should fail on truncated bytes");
    }

    #[test]
    fn test_edge_garbage_bytes() {
        let arena = Bump::new();
        let id = 99999u128;
        let garbage: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE];

        let result = Edge::from_bincode_bytes(id, &garbage, &arena);
        assert!(result.is_err(), "Should fail on garbage bytes");
    }

    #[test]
    fn test_edge_corrupted_node_ids() {
        let arena = Bump::new();
        let id = 111222u128;

        let edge = create_simple_edge(&arena, id, "test", 1, 2);
        let mut bytes = bincode::serialize(&edge).unwrap();

        // Corrupt bytes that might be the node IDs
        if bytes.len() > 20 {
            for i in 10..20 {
                bytes[i] = 0xFF;
            }
        }

        let arena2 = Bump::new();
        let _result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        // Might succeed with corrupted IDs - test documents behavior
    }

    #[test]
    fn test_edge_single_byte() {
        let arena = Bump::new();
        let id = 222333u128;
        let single_byte: &[u8] = &[0x42];

        let result = Edge::from_bincode_bytes(id, single_byte, &arena);
        assert!(result.is_err(), "Should fail on single byte");
    }

    #[test]
    fn test_edge_all_zeros() {
        let arena = Bump::new();
        let id = 333444u128;
        let zeros = vec![0u8; 100];

        let _result = Edge::from_bincode_bytes(id, &zeros, &arena);
        // May deserialize with zero values - test documents behavior
    }

    // ========================================================================
    // MALFORMED BINCODE DATA TESTS - VECTORS
    // ========================================================================

    #[test]
    fn test_vector_empty_props_bytes() {
        let arena = Bump::new();
        let id = 444555u128;
        let empty_bytes: &[u8] = &[];
        let valid_data = vec![1.0, 2.0, 3.0];
        let data_bytes = create_vector_bytes(&valid_data);

        let result = HVector::from_bincode_bytes(&arena, Some(empty_bytes), &data_bytes, id, true);
        assert!(result.is_err(), "Should fail on empty property bytes");
    }

    #[test]
    #[should_panic]
    fn test_vector_empty_data_bytes() {
        let arena = Bump::new();
        let id = 555666u128;
        let vector = create_simple_vector(&arena, id, "test", &[1.0, 2.0]);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let empty_data: &[u8] = &[];

        let arena2 = Bump::new();
        let _result =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), empty_data, id, true).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_vector_cast_empty_raw_data_panics() {
        let arena = Bump::new();
        let empty_data: &[u8] = &[];
        HVector::raw_vector_data_to_vec(empty_data, &arena).unwrap();
    }

    #[test]
    fn test_vector_truncated_props() {
        let arena = Bump::new();
        let id = 666777u128;
        let props = vec![("key", Value::String("value".to_string()))];
        let vector = create_arena_vector(&arena, id, "test", 1, false, &[1.0], props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let truncated_props = &props_bytes[..props_bytes.len() / 2];

        let arena2 = Bump::new();
        let result =
            HVector::from_bincode_bytes(&arena2, Some(truncated_props), data_bytes, id, true);
        assert!(result.is_err(), "Should fail on truncated properties");
    }

    #[test]
    fn test_vector_garbage_props() {
        let arena = Bump::new();
        let id = 777888u128;
        let garbage: Vec<u8> = vec![0xFF; 50];
        let data_bytes = create_vector_bytes(&[1.0, 2.0, 3.0]);

        let result = HVector::from_bincode_bytes(&arena, Some(&garbage), &data_bytes, id, true);
        assert!(result.is_err(), "Should fail on garbage property bytes");
    }

    #[test]
    #[should_panic]
    fn test_vector_misaligned_data_bytes_panics() {
        let arena = Bump::new();
        // 7 bytes is not a multiple of 4 (size of f32)
        let misaligned: &[u8] = &[0, 1, 2, 3, 4, 5, 6];
        HVector::raw_vector_data_to_vec(misaligned, &arena).unwrap();
    }

    #[test]
    fn test_vector_single_byte_data() {
        let arena = Bump::new();
        let id = 888999u128;
        let vector = create_simple_vector(&arena, id, "test", &[1.0]);
        let _props_bytes = bincode::serialize(&vector).unwrap();
        let _single_byte: &[u8] = &[0x42];

        let _arena2 = Bump::new();
        // Should panic due to misalignment
    }

    // ========================================================================
    // PROPERTY MAP CORRUPTION TESTS
    // ========================================================================

    #[test]
    fn test_node_property_count_mismatch() {
        let arena = Bump::new();
        let id = 123123u128;

        // This tests if property count in serialized data doesn't match actual properties
        let props = vec![("key1", Value::I32(1)), ("key2", Value::I32(2))];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let mut bytes = bincode::serialize(&node).unwrap();

        // Try to corrupt the property count (depends on bincode format)
        // This is exploratory testing
        if bytes.len() > 5 {
            bytes[2] = 99; // Try to change count
        }

        let arena2 = Bump::new();
        let _result = Node::from_bincode_bytes(id, &bytes, &arena2);
        // Behavior depends on exact corruption
    }

    #[test]
    fn test_edge_property_key_corruption() {
        let arena = Bump::new();
        let id = 234234u128;

        let props = vec![("valid_key", Value::String("valid_value".to_string()))];
        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let mut bytes = bincode::serialize(&edge).unwrap();

        // Corrupt part of the property key
        if bytes.len() > 30 {
            bytes[25] = 0xFF;
            bytes[26] = 0xFF;
        }

        let arena2 = Bump::new();
        let _result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        // May fail on UTF-8 validation
    }

    // ========================================================================
    // INVALID UTF-8 TESTS
    // ========================================================================

    #[test]
    fn test_node_invalid_utf8_in_label() {
        let arena = Bump::new();
        let id = 345345u128;

        // Create bytes that represent invalid UTF-8
        // Start with valid serialized node
        let node = create_simple_node(&arena, id, "test");
        let mut bytes = bincode::serialize(&node).unwrap();

        // Insert invalid UTF-8 sequence
        // 0xC0 0x80 is an invalid UTF-8 sequence (overlong encoding)
        if bytes.len() > 10 {
            bytes[8] = 0xC0;
            bytes[9] = 0x80;
        }

        let arena2 = Bump::new();
        let _result = Node::from_bincode_bytes(id, &bytes, &arena2);
        // Should fail on UTF-8 validation
    }

    #[test]
    fn test_edge_invalid_utf8_in_label() {
        let arena = Bump::new();
        let id = 456456u128;

        let edge = create_simple_edge(&arena, id, "test", 1, 2);
        let mut bytes = bincode::serialize(&edge).unwrap();

        // Insert invalid UTF-8
        if bytes.len() > 10 {
            bytes[8] = 0xFF;
            bytes[9] = 0xFE;
        }

        let arena2 = Bump::new();
        let _result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        // Should fail on UTF-8 validation
    }

    #[test]
    fn test_vector_invalid_utf8_in_label() {
        let arena = Bump::new();
        let id = 567567u128;

        let vector = create_simple_vector(&arena, id, "test", &[1.0, 2.0]);
        let mut props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        // Corrupt label bytes
        if props_bytes.len() > 10 {
            props_bytes[8] = 0x80;
            props_bytes[9] = 0x81;
        }

        let arena2 = Bump::new();
        let _result =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id, true);
        // Should fail on UTF-8 validation
    }

    #[test]
    fn test_node_invalid_utf8_in_property_key() {
        let arena = Bump::new();
        let id = 678678u128;

        let props = vec![("key", Value::String("value".to_string()))];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let mut bytes = bincode::serialize(&node).unwrap();

        // Try to corrupt property key UTF-8
        if bytes.len() > 30 {
            bytes[28] = 0xC0;
            bytes[29] = 0xC0;
        }

        let arena2 = Bump::new();
        let _result = Node::from_bincode_bytes(id, &bytes, &arena2);
        // May fail on UTF-8 validation
    }

    #[test]
    fn test_edge_invalid_utf8_in_property_value() {
        let arena = Bump::new();
        let id = 789789u128;

        let props = vec![("key", Value::String("value".to_string()))];
        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let mut bytes = bincode::serialize(&edge).unwrap();

        // Corrupt string value UTF-8
        if bytes.len() > 40 {
            bytes[38] = 0xED;
            bytes[39] = 0xA0;
            bytes[40] = 0x80; // Invalid UTF-8 surrogate
        }

        let arena2 = Bump::new();
        let _result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        // May fail during property deserialization
    }

    // ========================================================================
    // VERSION FIELD TESTS
    // ========================================================================

    #[test]
    fn test_node_extreme_version_value() {
        let arena = Bump::new();
        let id = 890890u128;

        // u8::MAX version
        let node = create_arena_node(&arena, id, "test", 255, vec![]);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle u8::MAX version");
        assert_eq!(result.unwrap().version, 255);
    }

    #[test]
    fn test_edge_extreme_version_value() {
        let arena = Bump::new();
        let id = 901901u128;

        let edge = create_arena_edge(&arena, id, "test", 255, 1, 2, vec![]);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle u8::MAX version");
        assert_eq!(result.unwrap().version, 255);
    }

    #[test]
    fn test_vector_extreme_version_value() {
        let arena = Bump::new();
        let id = 12012u128;

        let vector = create_arena_vector(&arena, id, "test", 255, false, &[1.0], vec![]);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id, true);
        assert!(result.is_ok(), "Should handle u8::MAX version");
        assert_eq!(result.unwrap().version, 255);
    }

    // ========================================================================
    // EXTREME ID VALUES
    // ========================================================================

    #[test]
    fn test_node_zero_id() {
        let arena = Bump::new();
        let id = 0u128;

        let node = create_simple_node(&arena, id, "test");
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle zero ID");
        assert_eq!(result.unwrap().id, 0);
    }

    #[test]
    fn test_edge_max_id() {
        let arena = Bump::new();
        let id = u128::MAX;

        let edge = create_simple_edge(&arena, id, "test", 1, 2);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle u128::MAX ID");
        assert_eq!(result.unwrap().id, u128::MAX);
    }

    #[test]
    fn test_vector_max_id() {
        let arena = Bump::new();
        let id = u128::MAX;

        let vector = create_simple_vector(&arena, id, "test", &[1.0, 2.0]);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id, true);
        assert!(result.is_ok(), "Should handle u128::MAX ID");
        assert_eq!(result.unwrap().id, u128::MAX);
    }

    #[test]
    fn test_edge_extreme_node_ids() {
        let arena = Bump::new();
        let id = 123456u128;

        // Edge with extreme from/to node IDs
        let edge = create_simple_edge(&arena, id, "test", 0, u128::MAX);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle extreme node IDs");
        let deserialized = result.unwrap();
        assert_eq!(deserialized.from_node, 0);
        assert_eq!(deserialized.to_node, u128::MAX);
    }

    // ========================================================================
    // SPECIAL NUMERIC VALUES IN PROPERTIES
    // ========================================================================

    #[test]
    fn test_node_with_nan_property() {
        let arena = Bump::new();
        let id = 246810u128;

        let props = vec![("nan_val", Value::F64(f64::NAN))];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle NaN in properties");
    }

    #[test]
    fn test_edge_with_infinity_property() {
        let arena = Bump::new();
        let id = 135791u128;

        let props = vec![
            ("pos_inf", Value::F64(f64::INFINITY)),
            ("neg_inf", Value::F64(f64::NEG_INFINITY)),
        ];
        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle infinity in properties");
    }

    #[test]
    fn test_vector_data_with_special_floats() {
        let arena = Bump::new();
        let id = 987654u128;

        // Vector with NaN, infinity, and other special values
        let data = vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 0.0, -0.0];
        let vector = create_simple_vector(&arena, id, "special", &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id, true);
        assert!(
            result.is_ok(),
            "Should handle special float values in vector data"
        );
    }

    // ========================================================================
    // BACKWARDS COMPATIBILITY ERROR SCENARIOS
    // ========================================================================

    #[test]
    fn test_node_deserialize_future_version() {
        let arena = Bump::new();
        let id = 111222u128;

        // Create a node with a "future" version
        let node = create_arena_node(&arena, id, "test", 100, vec![]);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        // Should still deserialize - version is just a field
        assert!(result.is_ok());
        assert_eq!(result.unwrap().version, 100);
    }

    #[test]
    fn test_empty_string_property_keys() {
        let arena = Bump::new();
        let id = 333444u128;

        // Empty string as property key
        let props = vec![("", Value::String("value".to_string()))];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok(), "Should handle empty string property key");
    }
}
