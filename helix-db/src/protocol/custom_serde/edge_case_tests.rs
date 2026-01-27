//! Edge case tests for serialization/deserialization
//!
//! This module tests edge cases including:
//! - Maximum size property maps (1000+ properties)
//! - Extreme ID and numeric values
//! - Empty strings vs null handling
//! - Very long labels and strings
//! - Deeply nested Value structures
//! - Special characters and Unicode edge cases
//! - Numeric precision boundaries
//! - Self-referential structures

#[cfg(test)]
mod edge_case_tests {
    use super::super::test_utils::*;
    use crate::helix_engine::vector_core::vector::HVector;
    use crate::protocol::value::Value;
    use crate::utils::items::{Edge, Node};
    use bumpalo::Bump;
    use std::collections::HashMap;

    // ========================================================================
    // MAXIMUM SIZE PROPERTY MAPS
    // ========================================================================

    #[test]
    fn test_node_with_100_properties() {
        let arena = Bump::new();
        let id = 11111u128;

        let props: Vec<(&str, Value)> = (0..100)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("property_{}", i));
                (key, Value::I32(i))
            })
            .collect();

        let node = create_arena_node(&arena, id, "many_props", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.properties.unwrap().len(), 100);
    }

    #[test]
    fn test_edge_with_200_properties() {
        let arena = Bump::new();
        let id = 22222u128;

        let props: Vec<(&str, Value)> = (0..200)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("edge_prop_{}", i));
                (key, Value::String(format!("value_{}", i)))
            })
            .collect();

        let edge = create_arena_edge(&arena, id, "many_props", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().properties.unwrap().len(), 200);
    }

    #[test]
    fn test_vector_with_500_properties() {
        let arena = Bump::new();
        let id = 33333u128;
        let data = vec![1.0, 2.0, 3.0];

        let props: Vec<(&str, Value)> = (0..500)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("vec_prop_{}", i));
                (key, Value::U64(i as u64))
            })
            .collect();

        let vector = create_arena_vector(&arena, id, "many_props", 1, false, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().properties.unwrap().len(), 500);
    }

    #[test]
    fn test_node_with_1000_properties() {
        let arena = Bump::new();
        let id = 44444u128;

        let props: Vec<(&str, Value)> = (0..1000)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("k{}", i));
                (key, Value::I64(i as i64))
            })
            .collect();

        let node = create_arena_node(&arena, id, "stress", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().properties.unwrap().len(), 1000);
    }

    // ========================================================================
    // VERY LONG STRINGS AND LABELS
    // ========================================================================

    #[test]
    fn test_node_with_1kb_label() {
        let arena = Bump::new();
        let id = 55555u128;
        let long_label = "x".repeat(1024);

        let node = create_simple_node(&arena, id, &long_label);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().label.len(), 1024);
    }

    #[test]
    fn test_edge_with_10kb_label() {
        let arena = Bump::new();
        let id = 66666u128;
        let very_long_label = "EdgeLabel".repeat(1142); // ~10KB

        let edge = create_simple_edge(&arena, id, &very_long_label, 1, 2);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert!(result.unwrap().label.len() > 10000);
    }

    #[test]
    fn test_vector_with_long_label() {
        let arena = Bump::new();
        let id = 77777u128;
        let long_label = "Vector".repeat(500);
        let data = vec![1.0, 2.0];

        let vector = create_simple_vector(&arena, id, &long_label, &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
        assert!(result.unwrap().label.len() > 2000);
    }

    #[test]
    fn test_node_with_very_long_property_value() {
        let arena = Bump::new();
        let id = 88888u128;
        let long_value = "PropertyValue".repeat(1000); // ~13KB

        let props = vec![("data", Value::String(long_value.clone()))];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_with_multiple_long_strings() {
        let arena = Bump::new();
        let id = 99999u128;

        let props = vec![
            ("str1", Value::String("A".repeat(5000))),
            ("str2", Value::String("B".repeat(5000))),
            ("str3", Value::String("C".repeat(5000))),
        ];

        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    // ========================================================================
    // UNICODE AND SPECIAL CHARACTERS
    // ========================================================================

    #[test]
    fn test_node_with_emoji_label() {
        let arena = Bump::new();
        let id = 111000u128;

        let node = create_simple_node(&arena, id, "🚀🔥💯🎉⭐");
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().label, "🚀🔥💯🎉⭐");
    }

    #[test]
    fn test_edge_with_mixed_unicode() {
        let arena = Bump::new();
        let id = 222000u128;

        let mixed = "Hello世界Привет🌍مرحبا";
        let edge = create_simple_edge(&arena, id, mixed, 1, 2);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().label, mixed);
    }

    #[test]
    fn test_vector_with_unicode_properties() {
        let arena = Bump::new();
        let id = 333000u128;
        let data = vec![1.0];

        let props = vec![
            ("名前", Value::String("値".to_string())),
            ("emoji_key🔑", Value::String("emoji_value🎯".to_string())),
            ("Ключ", Value::String("Значение".to_string())),
        ];

        let vector = create_arena_vector(&arena, id, "unicode", 1, false, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_node_with_all_whitespace_label() {
        let arena = Bump::new();
        let id = 444000u128;

        let whitespace = "   \t\n\r   ";
        let node = create_simple_node(&arena, id, whitespace);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().label, whitespace);
    }

    #[test]
    fn test_edge_with_special_characters() {
        let arena = Bump::new();
        let id = 555000u128;

        let special = r#"!@#$%^&*()[]{}|\\;:'",.<>?/~`"#;
        let edge = create_simple_edge(&arena, id, special, 1, 2);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().label, special);
    }

    #[test]
    fn test_node_with_null_char_in_string() {
        let arena = Bump::new();
        let id = 666000u128;

        let with_null = "before\0after";
        let node = create_simple_node(&arena, id, with_null);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().label, with_null);
    }

    // ========================================================================
    // DEEPLY NESTED VALUES
    // ========================================================================

    #[test]
    fn test_node_with_deeply_nested_arrays() {
        let arena = Bump::new();
        let id = 777000u128;

        // Create 10-level nested array
        let mut nested = Value::I32(42);
        for _ in 0..10 {
            nested = Value::Array(vec![nested]);
        }

        let props = vec![("deep", nested)];
        let node = create_arena_node(&arena, id, "nested", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_with_deeply_nested_objects() {
        let arena = Bump::new();
        let id = 888000u128;

        // Create nested objects
        let mut nested = Value::String("deep_value".to_string());
        for i in 0..10 {
            let mut map = HashMap::new();
            map.insert(format!("level_{}", i), nested);
            nested = Value::Object(map);
        }

        let props = vec![("nested_obj", nested)];
        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_with_mixed_nested_structures() {
        let arena = Bump::new();
        let id = 999000u128;
        let data = vec![1.0];

        // Mix of arrays and objects
        let mut map = HashMap::new();
        map.insert(
            "array_in_object".to_string(),
            Value::Array(vec![
                Value::I32(1),
                Value::Object({
                    let mut inner = HashMap::new();
                    inner.insert(
                        "inner_key".to_string(),
                        Value::String("inner_value".to_string()),
                    );
                    inner
                }),
            ]),
        );

        let props = vec![("complex", Value::Object(map))];
        let vector = create_arena_vector(&arena, id, "complex", 1, false, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    // ========================================================================
    // EMPTY AND NULL EDGE CASES
    // ========================================================================

    #[test]
    fn test_node_empty_label_empty_properties() {
        let arena = Bump::new();
        let id = 100100u128;

        let node = create_simple_node(&arena, id, "");
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.label, "");
        assert!(deserialized.properties.is_none());
    }

    #[test]
    fn test_edge_zero_node_ids() {
        let arena = Bump::new();
        let id = 200200u128;

        let edge = create_simple_edge(&arena, id, "test", 0, 0);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.from_node, 0);
        assert_eq!(deserialized.to_node, 0);
    }

    #[test]
    fn test_node_with_empty_value() {
        let arena = Bump::new();
        let id = 300300u128;

        let props = vec![
            ("empty1", Value::Empty),
            ("empty2", Value::Empty),
            ("normal", Value::I32(42)),
        ];

        let node = create_arena_node(&arena, id, "test", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_with_empty_array_property() {
        let arena = Bump::new();
        let id = 400400u128;

        let props = vec![("empty_array", Value::Array(vec![]))];
        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_with_empty_object_property() {
        let arena = Bump::new();
        let id = 500500u128;
        let data = vec![1.0];

        let props = vec![("empty_obj", Value::Object(HashMap::new()))];
        let vector = create_arena_vector(&arena, id, "test", 1, false, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    // ========================================================================
    // NUMERIC PRECISION AND BOUNDARY TESTS
    // ========================================================================

    #[test]
    fn test_node_with_all_numeric_extremes() {
        let arena = Bump::new();
        let id = 600600u128;

        let props = vec![
            ("i8_min", Value::I8(i8::MIN)),
            ("i8_max", Value::I8(i8::MAX)),
            ("i16_min", Value::I16(i16::MIN)),
            ("i16_max", Value::I16(i16::MAX)),
            ("i32_min", Value::I32(i32::MIN)),
            ("i32_max", Value::I32(i32::MAX)),
            ("i64_min", Value::I64(i64::MIN)),
            ("i64_max", Value::I64(i64::MAX)),
            ("u8_max", Value::U8(u8::MAX)),
            ("u16_max", Value::U16(u16::MAX)),
            ("u32_max", Value::U32(u32::MAX)),
            ("u64_max", Value::U64(u64::MAX)),
            ("u128_max", Value::U128(u128::MAX)),
        ];

        let node = create_arena_node(&arena, id, "numeric_extremes", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_with_floating_point_extremes() {
        let arena = Bump::new();
        let id = 700700u128;

        let props = vec![
            ("f32_min", Value::F32(f32::MIN)),
            ("f32_max", Value::F32(f32::MAX)),
            ("f32_epsilon", Value::F32(f32::EPSILON)),
            ("f64_min", Value::F64(f64::MIN)),
            ("f64_max", Value::F64(f64::MAX)),
            ("f64_epsilon", Value::F64(f64::EPSILON)),
        ];

        let edge = create_arena_edge(&arena, id, "float_extremes", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_with_subnormal_numbers() {
        let arena = Bump::new();
        let id = 800800u128;

        // Subnormal (denormalized) numbers
        let data = vec![f64::MIN_POSITIVE, f64::MIN_POSITIVE / 2.0, 1e-308, 1e-320];

        let vector = create_simple_vector(&arena, id, "subnormal", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_with_zero_positive_and_negative() {
        let arena = Bump::new();
        let id = 900900u128;

        let data = vec![0.0, -0.0, 0.0, -0.0];
        let vector = create_simple_vector(&arena, id, "zeros", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    // ========================================================================
    // PROPERTY KEY EDGE CASES
    // ========================================================================

    #[test]
    fn test_node_with_single_char_property_keys() {
        let arena = Bump::new();
        let id = 101101u128;

        let props = vec![
            ("a", Value::I32(1)),
            ("b", Value::I32(2)),
            ("c", Value::I32(3)),
            ("d", Value::I32(4)),
        ];

        let node = create_arena_node(&arena, id, "test", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_with_very_long_property_keys() {
        let arena = Bump::new();
        let id = 202202u128;

        let long_key = "property_key_".repeat(100); // ~1.3KB key
        let key_ref: &str = arena.alloc_str(&long_key);
        let props = vec![(key_ref, Value::String("value".to_string()))];

        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_with_numeric_string_keys() {
        let arena = Bump::new();
        let id = 303303u128;
        let data = vec![1.0];

        let props = vec![
            ("0", Value::String("zero".to_string())),
            ("1", Value::String("one".to_string())),
            ("123", Value::String("one-two-three".to_string())),
        ];

        let vector = create_arena_vector(&arena, id, "numeric_keys", 1, false, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    // ========================================================================
    // LARGE ARRAY PROPERTY VALUES
    // ========================================================================

    #[test]
    fn test_node_with_large_array_property() {
        let arena = Bump::new();
        let id = 404404u128;

        let large_array = Value::Array((0..1000).map(Value::I32).collect());

        let props = vec![("big_array", large_array)];
        let node = create_arena_node(&arena, id, "test", 0, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_with_array_of_strings() {
        let arena = Bump::new();
        let id = 505505u128;

        let string_array = Value::Array(
            (0..100)
                .map(|i| Value::String(format!("string_{}", i)))
                .collect(),
        );

        let props = vec![("strings", string_array)];
        let edge = create_arena_edge(&arena, id, "test", 0, 1, 2, props);
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_with_mixed_type_array() {
        let arena = Bump::new();
        let id = 606606u128;
        let data = vec![1.0];

        let mixed_array = Value::Array(vec![
            Value::String("text".to_string()),
            Value::I32(42),
            Value::F64(3.14),
            Value::Boolean(true),
            Value::Empty,
            Value::Array(vec![Value::I32(1), Value::I32(2)]),
        ]);

        let props = vec![("mixed", mixed_array)];
        let vector = create_arena_vector(&arena, id, "test", 1, false, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }

    // ========================================================================
    // VECTOR DIMENSION EDGE CASES
    // ========================================================================

    #[test]
    fn test_vector_with_8192_dimensions() {
        let arena = Bump::new();
        let id = 707707u128;
        let data: Vec<f64> = (0..8192).map(|i| (i as f64) * 0.0001).collect();

        let vector = create_simple_vector(&arena, id, "8k_dims", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().data.len(), 8192);
    }

    #[test]
    fn test_vector_with_all_zero_data() {
        let arena = Bump::new();
        let id = 808808u128;
        let data = vec![0.0; 1536];

        let vector = create_simple_vector(&arena, id, "zeros", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert!(deserialized.data.iter().all(|&v| v == 0.0));
    }

    #[test]
    fn test_vector_with_all_same_value() {
        let arena = Bump::new();
        let id = 909909u128;
        let data = vec![42.42; 512];

        let vector = create_simple_vector(&arena, id, "constant", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert!(deserialized.data.iter().all(|&v| (v - 42.42).abs() < 1e-10));
    }

    // ========================================================================
    // COMBINATION EDGE CASES
    // ========================================================================

    #[test]
    fn test_node_max_everything() {
        let arena = Bump::new();
        let id = u128::MAX;

        let props: Vec<(&str, Value)> = (0..500)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("property_{}", i));
                (key, Value::String(format!("value_{}", i).repeat(10)))
            })
            .collect();

        let node = create_arena_node(&arena, id, &"Label".repeat(100), 255, props);
        let bytes = bincode::serialize(&node).unwrap();

        let arena2 = Bump::new();
        let result = Node::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_edge_max_everything() {
        let arena = Bump::new();
        let id = u128::MAX;

        let props: Vec<(&str, Value)> = (0..300)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("k{}", i));
                (key, Value::I64(i64::MAX - i as i64))
            })
            .collect();

        let edge = create_arena_edge(
            &arena,
            id,
            &"E".repeat(500),
            255,
            u128::MAX - 1,
            u128::MAX - 2,
            props,
        );
        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let result = Edge::from_bincode_bytes(id, &bytes, &arena2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vector_max_complexity() {
        let arena = Bump::new();
        let id = u128::MAX;
        let data: Vec<f64> = (0..2048).map(|i| (i as f64).sin()).collect();

        let props: Vec<(&str, Value)> = (0..200)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("prop_{}", i));
                (key, Value::String(format!("🚀{}", i).repeat(20)))
            })
            .collect();

        let vector =
            create_arena_vector(&arena, id, &"Vec".repeat(200), 255, true, 0, &data, props);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);
        assert!(result.is_ok());
    }
}
