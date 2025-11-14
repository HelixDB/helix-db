//! Comprehensive tests for HVector serialization and deserialization
//!
//! This module tests:
//! - Basic vector roundtrip serialization
//! - Raw vector data casting and byte conversion
//! - Combined properties + raw data deserialization
//! - VectorWithoutData serialization
//! - Version compatibility
//! - Edge cases (empty vectors, large dimensions, deleted flags)
//! - UTF-8 labels
//! - Property handling with vectors

#[cfg(test)]
mod vector_serialization_tests {
    use super::super::test_utils::*;
    use crate::helix_engine::vector_core::vector::HVector;
    use crate::helix_engine::vector_core::vector_without_data::VectorWithoutData;
    use crate::protocol::value::Value;

    use bumpalo::Bump;

    // ========================================================================
    // BASIC ROUNDTRIP TESTS
    // ========================================================================

    #[test]
    fn test_vector_empty_properties_roundtrip() {
        let arena = Bump::new();
        let id = 12345u128;
        let data = vec![1.0, 2.0, 3.0, 4.0];

        let vector = create_simple_vector(&arena, id, "test_vector", &data);

        // Serialize properties (should be minimal since no properties)
        let props_bytes = bincode::serialize(&vector).unwrap();

        // Serialize vector data
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        // Deserialize
        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_vector_with_single_property_roundtrip() {
        let arena = Bump::new();
        let id = 99999u128;
        let data = vec![0.5, -0.5, 1.5, -1.5];
        let props = vec![("name", Value::String("test".to_string()))];

        let vector = create_arena_vector(&arena, id, "labeled_vector", 1, false, 0, &data, props);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_vector_with_multiple_properties_roundtrip() {
        let arena = Bump::new();
        let id = 555555u128;
        let data = vec![1.1, 2.2, 3.3, 4.4, 5.5];
        let props = vec![
            ("name", Value::String("multi_prop_vector".to_string())),
            ("version", Value::I32(2)),
            ("score", Value::F64(0.95)),
        ];

        let vector = create_arena_vector(&arena, id, "vector_label", 1, false, 0, &data, props);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_vector_with_all_value_types() {
        let arena = Bump::new();
        let id = 777777u128;
        let data = vec![0.0; 128]; // Standard embedding dimension
        let props = all_value_types_props();

        let vector = create_arena_vector(&arena, id, "all_types", 1, false, 0, &data, props);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_vector_with_nested_values() {
        let arena = Bump::new();
        let id = 888888u128;
        let data = vec![1.0, 2.0, 3.0];
        let props = nested_value_props();

        let vector = create_arena_vector(&arena, id, "nested", 1, false, 0, &data, props);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        // Just verify basic structure instead of deep equality due to HashMap ordering
        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, "nested");
        assert_eq!(deserialized.data.len(), 3);
        assert!(deserialized.properties.is_some());
        assert_eq!(deserialized.properties.unwrap().len(), 3);
    }

    // ========================================================================
    // RAW VECTOR DATA CASTING TESTS
    // ========================================================================

    #[test]
    fn test_vector_data_to_bytes_128d() {
        let arena = Bump::new();
        let id = 111111u128;
        let data: Vec<f64> = (0..128).map(|i| i as f64 * 0.1).collect();

        let vector = create_simple_vector(&arena, id, "vector_128", &data);
        let bytes = vector.vector_data_to_bytes().unwrap();

        assert_eq!(bytes.len(), 128 * 8); // 128 dimensions * 8 bytes per f64
    }

    #[test]
    fn test_vector_data_to_bytes_384d() {
        let arena = Bump::new();
        let id = 222222u128;
        let data: Vec<f64> = (0..384).map(|i| i as f64 * 0.01).collect();

        let vector = create_simple_vector(&arena, id, "vector_384", &data);
        let bytes = vector.vector_data_to_bytes().unwrap();

        assert_eq!(bytes.len(), 384 * 8);
    }

    #[test]
    fn test_vector_data_to_bytes_1536d() {
        let arena = Bump::new();
        let id = 333333u128;
        let data: Vec<f64> = (0..1536).map(|i| (i as f64).sin()).collect();

        let vector = create_simple_vector(&arena, id, "vector_1536", &data);
        let bytes = vector.vector_data_to_bytes().unwrap();

        assert_eq!(bytes.len(), 1536 * 8);
    }

    #[test]
    fn test_cast_raw_vector_data_128d() {
        let arena = Bump::new();
        let original_data: Vec<f64> = (0..128).map(|i| i as f64).collect();
        let raw_bytes = create_vector_bytes(&original_data);

        let casted_data = HVector::cast_raw_vector_data(&arena, &raw_bytes);

        assert_eq!(casted_data.len(), 128);
        for (i, &val) in casted_data.iter().enumerate() {
            assert!((val - original_data[i]).abs() < 1e-10);
        }
    }

    #[test]
    fn test_cast_raw_vector_data_roundtrip() {
        let arena = Bump::new();
        let original_data = vec![3.14159, 2.71828, 1.41421, 1.73205];
        let raw_bytes = create_vector_bytes(&original_data);

        let casted_data = HVector::cast_raw_vector_data(&arena, &raw_bytes);

        assert_eq!(casted_data.len(), original_data.len());
        for (orig, casted) in original_data.iter().zip(casted_data.iter()) {
            assert!((orig - casted).abs() < 1e-10);
        }
    }

    #[test]
    fn test_from_raw_vector_data() {
        let arena = Bump::new();
        let id = 444444u128;
        let label = arena.alloc_str("raw_vector");
        let data = vec![1.0, 2.0, 3.0, 4.0];
        let raw_bytes = create_vector_bytes(&data);

        let vector = HVector::from_raw_vector_data(&arena, &raw_bytes, label, id).unwrap();

        assert_eq!(vector.id, id);
        assert_eq!(vector.label, label);
        assert_eq!(vector.data.len(), 4);
        assert_eq!(vector.version, 1);
        assert_eq!(vector.deleted, false);
        assert_eq!(vector.level, 0);
        assert!(vector.properties.is_none());
    }

    // ========================================================================
    // COMBINED PROPERTIES + RAW DATA DESERIALIZATION
    // ========================================================================

    #[test]
    fn test_combined_empty_props_with_data() {
        let arena = Bump::new();
        let id = 555666u128;
        let data = vec![0.1, 0.2, 0.3];

        let vector = create_simple_vector(&arena, id, "combined_test", &data);

        // Serialize properties (empty)
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        // Deserialize combining both
        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);

        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_combined_with_props_and_data() {
        let arena = Bump::new();
        let id = 666777u128;
        let data = vec![1.5, 2.5, 3.5, 4.5];
        let props = vec![
            ("model", Value::String("text-embedding-3".to_string())),
            ("dimension", Value::I32(4)),
        ];

        let vector = create_arena_vector(&arena, id, "embedding", 1, false, 0, &data, props);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);

        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_vectors_semantically_equal(&vector, &deserialized);
    }

    #[test]
    fn test_combined_none_props_with_data() {
        let arena = Bump::new();
        let id = 777888u128;
        let data = vec![9.9, 8.8, 7.7];

        let vector = create_simple_vector(&arena, id, "no_props", &data);
        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        // Deserialize with serialized empty properties
        let arena2 = Bump::new();
        let result = HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id);

        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, "no_props");
        assert_eq!(deserialized.data.len(), 3);
        assert!(deserialized.properties.is_none());
    }

    // ========================================================================
    // VECTOR WITHOUT DATA TESTS
    // ========================================================================

    #[test]
    fn test_vector_without_data_serialization() {
        let arena = Bump::new();
        let id = 999000u128;
        let label = arena.alloc_str("metadata_only");
        let props = vec![("type", Value::String("embedding".to_string()))];
        let len = props.len();
        let props_iter = props.into_iter().map(|(k, v)| {
            let key: &str = arena.alloc_str(k);
            (key, v)
        });
        let props_map =
            crate::utils::properties::ImmutablePropertiesMap::new(len, props_iter, &arena);

        let vector_without_data = VectorWithoutData {
            id,
            label,
            version: 1,
            deleted: false,
            level: 0,
            properties: Some(props_map),
        };

        // Serialize and deserialize
        let bytes = bincode::serialize(&vector_without_data).unwrap();
        let arena2 = Bump::new();
        let result = VectorWithoutData::from_bincode_bytes(&arena2, &bytes, id);
        println!("{:?}", result);
        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, label);
        assert_eq!(deserialized.version, 1);
        assert_eq!(deserialized.deleted, false);
    }

    #[test]
    fn test_vector_without_data_empty_properties() {
        let arena = Bump::new();
        let id = 111000u128;
        let label = arena.alloc_str("empty_meta");

        let vector_without_data = VectorWithoutData {
            id,
            label,
            version: 1,
            deleted: false,
            level: 0,
            properties: None,
        };

        let bytes = bincode::serialize(&vector_without_data).unwrap();
        let arena2 = Bump::new();
        let result = VectorWithoutData::from_bincode_bytes(&arena2, &bytes, id);

        assert!(result.is_ok());
        let deserialized = result.unwrap();
        assert_eq!(deserialized.id, id);
        assert!(deserialized.properties.is_none());
    }

    // ========================================================================
    // VERSION AND FLAGS TESTS
    // ========================================================================

    #[test]
    fn test_vector_with_version_field() {
        let arena = Bump::new();
        let id = 123456u128;
        let data = vec![1.0, 2.0];

        let vector = create_arena_vector(&arena, id, "versioned", 5, false, 0, &data, vec![]);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.version, 5);
    }

    #[test]
    fn test_vector_deleted_flag_true() {
        let arena = Bump::new();
        let id = 654321u128;
        let data = vec![0.0, 1.0];

        let vector = create_arena_vector(&arena, id, "deleted", 1, true, 0, &data, vec![]);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.deleted, true);
    }

    #[test]
    fn test_vector_deleted_flag_false() {
        let arena = Bump::new();
        let id = 987654u128;
        let data = vec![1.0, 0.0];

        let vector = create_arena_vector(&arena, id, "active", 1, false, 0, &data, vec![]);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.deleted, false);
    }

    // ========================================================================
    // UTF-8 AND LABEL TESTS
    // ========================================================================

    #[test]
    fn test_vector_utf8_label() {
        let arena = Bump::new();
        let id = 135790u128;
        let data = vec![1.0, 2.0, 3.0];

        let vector = create_simple_vector(&arena, id, "向量测试", &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.label, "向量测试");
    }

    #[test]
    fn test_vector_emoji_label() {
        let arena = Bump::new();
        let id = 246801u128;
        let data = vec![0.5];

        let vector = create_simple_vector(&arena, id, "🚀🔥💯", &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.label, "🚀🔥💯");
    }

    #[test]
    fn test_vector_empty_label() {
        let arena = Bump::new();
        let id = 369258u128;
        let data = vec![1.0];

        let vector = create_simple_vector(&arena, id, "", &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.label, "");
    }

    #[test]
    fn test_vector_very_long_label() {
        let arena = Bump::new();
        let id = 147258u128;
        let data = vec![1.0, 2.0];
        let long_label = "a".repeat(1000);

        let vector = create_simple_vector(&arena, id, &long_label, &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.label.len(), 1000);
        assert_eq!(deserialized.label, long_label);
    }

    // ========================================================================
    // PROPERTY MAP SIZE TESTS
    // ========================================================================

    #[test]
    fn test_vector_with_many_properties() {
        let arena = Bump::new();
        let id = 753951u128;
        let data = vec![1.0, 2.0, 3.0];
        let props: Vec<(&str, Value)> = (0..50)
            .map(|i| {
                let key: &str = arena.alloc_str(&format!("key_{}", i));
                (key, Value::I32(i))
            })
            .collect();

        let vector = create_arena_vector(&arena, id, "many_props", 1, false, 0, &data, props);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.properties.unwrap().len(), 50);
    }

    // ========================================================================
    // DIMENSION EDGE CASES
    // ========================================================================

    #[test]
    fn test_vector_single_dimension() {
        let arena = Bump::new();
        let id = 159357u128;
        let data = vec![42.0];

        let vector = create_simple_vector(&arena, id, "1d", &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.data.len(), 1);
        assert!((deserialized.data[0] - 42.0).abs() < 1e-10);
    }

    #[test]
    fn test_vector_large_dimension_4096() {
        let arena = Bump::new();
        let id = 951753u128;
        let data: Vec<f64> = (0..4096).map(|i| i as f64 * 0.001).collect();

        let vector = create_simple_vector(&arena, id, "4096d", &data);

        let props_bytes = bincode::serialize(&vector).unwrap();
        let data_bytes = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes), data_bytes, id).unwrap();

        assert_eq!(deserialized.data.len(), 4096);
    }

    // ========================================================================
    // BYTE-LEVEL VERIFICATION TESTS
    // ========================================================================

    #[test]
    fn test_vector_byte_level_roundtrip() {
        let arena = Bump::new();
        let id = 112233u128;
        let data = vec![1.1, 2.2, 3.3];
        let props = vec![("test", Value::String("value".to_string()))];

        let vector = create_arena_vector(&arena, id, "byte_test", 1, false, 0, &data, props);

        // First roundtrip
        let props_bytes1 = bincode::serialize(&vector).unwrap();
        let data_bytes1 = vector.vector_data_to_bytes().unwrap();

        let arena2 = Bump::new();
        let deserialized1 =
            HVector::from_bincode_bytes(&arena2, Some(&props_bytes1), data_bytes1, id).unwrap();

        // Second roundtrip
        let props_bytes2 = bincode::serialize(&deserialized1).unwrap();
        let data_bytes2 = deserialized1.vector_data_to_bytes().unwrap();

        // Bytes should be identical across roundtrips
        assert_eq!(props_bytes1, props_bytes2);
        assert_eq!(data_bytes1, data_bytes2);
    }

    #[test]
    fn test_vector_data_bytes_consistency() {
        let arena = Bump::new();
        let id = 445566u128;
        let data = vec![3.14159, 2.71828, 1.41421];

        let vector = create_simple_vector(&arena, id, "consistency", &data);

        // Call vector_data_to_bytes multiple times
        let bytes1 = vector.vector_data_to_bytes().unwrap();
        let bytes2 = vector.vector_data_to_bytes().unwrap();
        let bytes3 = vector.vector_data_to_bytes().unwrap();

        // All calls should produce identical bytes
        assert_eq!(bytes1, bytes2);
        assert_eq!(bytes2, bytes3);
    }
}
