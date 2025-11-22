//! Tests for custom serialization/deserialization compatibility
//!
//! These tests verify that the new arena-based Node implementation can:
//! 1. Deserialize data serialized by the old HashMap-based implementation (backwards compatibility)
//! 2. Round-trip serialize/deserialize correctly
//! 3. Preserve all data semantically (property order may differ due to HashMap randomization)

#[cfg(test)]
mod node_serialization_tests {
    use crate::protocol::value::Value;
    use crate::utils::items::Node;
    use crate::utils::properties::ImmutablePropertiesMap;
    use bumpalo::Bump;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    /// Old Node implementation for comparison testing
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct OldNode {
        #[serde(skip)]
        pub id: u128,
        pub label: String,
        #[serde(default)]
        pub version: u8,
        #[serde(default)]
        pub properties: Option<HashMap<String, Value>>,
    }

    /// Helper to create a test arena node with properties
    fn create_arena_node_with_props<'arena>(
        arena: &'arena Bump,
        id: u128,
        label: &str,
        props: Vec<(&str, Value)>,
    ) -> Node<'arena> {
        let label_ref = arena.alloc_str(label);

        if props.is_empty() {
            Node {
                id,
                label: label_ref,
                version: 0,
                properties: None,
            }
        } else {
            let len = props.len();
            let props_iter = props.into_iter().map(|(k, v)| {
                let key: &'arena str = arena.alloc_str(k);
                (key, v)
            });
            let props_map = ImmutablePropertiesMap::new(len, props_iter, arena);

            Node {
                id,
                label: label_ref,
                version: 0,
                properties: Some(props_map),
            }
        }
    }

    /// Helper to create an old node with properties
    fn create_old_node_with_props(
        id: u128,
        label: &str,
        props: Vec<(&str, Value)>,
    ) -> OldNode {
        if props.is_empty() {
            OldNode {
                id,
                label: label.to_string(),
                version: 0,
                properties: None,
            }
        } else {
            let mut props_map = HashMap::new();
            for (k, v) in props {
                props_map.insert(k.to_string(), v);
            }

            OldNode {
                id,
                label: label.to_string(),
                version: 0,
                properties: Some(props_map),
            }
        }
    }

    #[test]
    fn test_detailed_byte_analysis_empty_properties() {
        let arena = Bump::new();
        let id = 12345u128;

        // Create both old and new nodes
        let old_node = OldNode {
            id,
            label: "Person".to_string(),
            version: 0,
            properties: None,
        };

        let new_node = Node {
            id,
            label: arena.alloc_str("Person"),
            version: 0,
            properties: None,
        };

        // Serialize both
        let old_bytes = bincode::serialize(&old_node).unwrap();
        let new_bytes = bincode::serialize(&new_node).unwrap();

        println!("\n=== EMPTY PROPERTIES COMPARISON ===");
        println!("Old bytes ({} total): {:02x?}", old_bytes.len(), old_bytes);
        println!("New bytes ({} total): {:02x?}", new_bytes.len(), new_bytes);

        // Detailed analysis
        println!("\nByte-by-byte comparison:");
        for (i, (old_byte, new_byte)) in old_bytes.iter().zip(new_bytes.iter()).enumerate() {
            if old_byte != new_byte {
                println!("  Index {}: old={:02x} ({}), new={:02x} ({})",
                    i, old_byte, old_byte, new_byte, new_byte);
            }
        }

        // Bytes should be IDENTICAL
        assert_eq!(
            old_bytes, new_bytes,
            "Serialized bytes differ for empty properties!"
        );

        // Test that new format can deserialize its own output
        println!("\nAttempting to deserialize new_bytes...");
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2);
        if let Err(e) = &deserialized {
            println!("Deserialization error: {:?}", e);
        }
        assert!(deserialized.is_ok(), "Failed to deserialize new format: {:?}", deserialized.err());

        // Test that new format can deserialize old format
        println!("Attempting to deserialize old_bytes...");
        let arena3 = Bump::new();
        let old_deserialized = Node::from_bincode_bytes(id, &old_bytes, &arena3);
        if let Err(e) = &old_deserialized {
            println!("Deserialization error from old format: {:?}", e);
        }
        assert!(old_deserialized.is_ok(), "Failed to deserialize old format: {:?}", old_deserialized.err());
    }

    #[test]
    fn test_detailed_byte_analysis_with_properties() {
        let arena = Bump::new();
        let id = 99999u128;

        // Create old node with properties
        let mut old_props_map = HashMap::new();
        old_props_map.insert("name".to_string(), Value::String("Alice".to_string()));
        old_props_map.insert("age".to_string(), Value::I32(30));

        let old_node = OldNode {
            id,
            label: "User".to_string(),
            version: 0,
            properties: Some(old_props_map),
        };

        // Create new node with same properties
        let new_props = vec![
            ("name", Value::String("Alice".to_string())),
            ("age", Value::I32(30)),
        ];
        let new_node = create_arena_node_with_props(&arena, id, "User", new_props);

        // Serialize both
        let old_bytes = bincode::serialize(&old_node).unwrap();
        let new_bytes = bincode::serialize(&new_node).unwrap();

        println!("\n=== WITH PROPERTIES COMPARISON ===");
        println!("Old bytes ({} total): {:02x?}", old_bytes.len(), old_bytes);
        println!("New bytes ({} total): {:02x?}", new_bytes.len(), new_bytes);

        println!("\nOld as string interpretation:");
        for (i, byte) in old_bytes.iter().enumerate() {
            if *byte >= 32 && *byte < 127 {
                print!("{}", *byte as char);
            } else {
                print!("[{:02x}]", byte);
            }
            if (i + 1) % 40 == 0 {
                println!();
            }
        }
        println!();

        println!("\nNew as string interpretation:");
        for (i, byte) in new_bytes.iter().enumerate() {
            if *byte >= 32 && *byte < 127 {
                print!("{}", *byte as char);
            } else {
                print!("[{:02x}]", byte);
            }
            if (i + 1) % 40 == 0 {
                println!();
            }
        }
        println!();

        // Test deserialization of new format
        println!("\nAttempting to deserialize new_bytes...");
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2);
        if let Err(e) = &deserialized {
            println!("Deserialization error: {:?}", e);
        } else {
            println!("Successfully deserialized!");
            let node = deserialized.unwrap();
            println!("  Label: {}", node.label);
            println!("  Version: {}", node.version);
            if let Some(props) = &node.properties {
                println!("  Properties: {} entries", props.len());
                for (k, v) in props.iter() {
                    println!("    {}: {:?}", k, v);
                }
            }
        }

        // Test deserialization of old format
        println!("\nAttempting to deserialize old_bytes...");
        let arena3 = Bump::new();
        let old_deserialized = Node::from_bincode_bytes(id, &old_bytes, &arena3);
        if let Err(e) = &old_deserialized {
            println!("Deserialization error from old format: {:?}", e);
        } else {
            println!("Successfully deserialized old format!");
            let node = old_deserialized.unwrap();
            println!("  Label: {}", node.label);
            println!("  Version: {}", node.version);
            if let Some(props) = &node.properties {
                println!("  Properties: {} entries", props.len());
                for (k, v) in props.iter() {
                    println!("    {}: {:?}", k, v);
                }
            }
        }
    }

    #[test]
    fn test_node_serialization_single_property() {
        let arena = Bump::new();
        let id = 67890u128;

        let props = vec![("name", Value::String("Alice".to_string()))];

        let old_node = create_old_node_with_props(id, "User", props.clone());
        let new_node = create_arena_node_with_props(&arena, id, "User", props);

        let old_bytes = bincode::serialize(&old_node).unwrap();
        let new_bytes = bincode::serialize(&new_node).unwrap();

        assert_eq!(
            old_bytes, new_bytes,
            "Serialized bytes differ for single property!\nOld: {:?}\nNew: {:?}",
            old_bytes, new_bytes
        );
    }

    #[test]
    fn test_node_serialization_multiple_properties_semantic_equality() {
        let arena = Bump::new();
        let id = 99999u128;

        let props = vec![
            ("name", Value::String("Bob".to_string())),
            ("age", Value::I64(30)),
            ("score", Value::F64(95.5)),
        ];

        let new_node = create_arena_node_with_props(&arena, id, "Player", props);

        // Serialize the new node
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Deserialize it back
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        // Verify all fields match
        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, "Player");
        assert_eq!(deserialized.version, 0);

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 3);
        assert_eq!(props.get("name"), Some(&Value::String("Bob".to_string())));
        assert_eq!(props.get("age"), Some(&Value::I64(30)));
        assert_eq!(props.get("score"), Some(&Value::F64(95.5)));
    }

    #[test]
    fn test_node_serialization_various_value_types_roundtrip() {
        let arena = Bump::new();
        let id = 11111u128;

        let props = vec![
            ("string_val", Value::String("test".to_string())),
            ("i8_val", Value::I8(-42)),
            ("i16_val", Value::I16(1000)),
            ("i32_val", Value::I32(100000)),
            ("i64_val", Value::I64(9999999)),
            ("u8_val", Value::U8(255)),
            ("u16_val", Value::U16(65535)),
            ("u32_val", Value::U32(4294967295)),
            ("u64_val", Value::U64(18446744073709551615)),
            ("u128_val", Value::U128(340282366920938463463374607431768211455)),
            ("f32_val", Value::F32(3.14159)),
            ("f64_val", Value::F64(2.71828)),
            ("bool_val", Value::Boolean(true)),
        ];

        let new_node = create_arena_node_with_props(&arena, id, "AllTypes", props);
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Deserialize and verify all values
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 13);
        assert_eq!(props.get("string_val"), Some(&Value::String("test".to_string())));
        assert_eq!(props.get("i8_val"), Some(&Value::I8(-42)));
        assert_eq!(props.get("i16_val"), Some(&Value::I16(1000)));
        assert_eq!(props.get("i32_val"), Some(&Value::I32(100000)));
        assert_eq!(props.get("i64_val"), Some(&Value::I64(9999999)));
        assert_eq!(props.get("u8_val"), Some(&Value::U8(255)));
        assert_eq!(props.get("u16_val"), Some(&Value::U16(65535)));
        assert_eq!(props.get("u32_val"), Some(&Value::U32(4294967295)));
        assert_eq!(props.get("u64_val"), Some(&Value::U64(18446744073709551615)));
        assert_eq!(props.get("u128_val"), Some(&Value::U128(340282366920938463463374607431768211455)));
        assert_eq!(props.get("f32_val"), Some(&Value::F32(3.14159)));
        assert_eq!(props.get("f64_val"), Some(&Value::F64(2.71828)));
        assert_eq!(props.get("bool_val"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_node_serialization_nested_values() {
        let arena = Bump::new();
        let id = 22222u128;

        let props = vec![
            ("array", Value::Array(vec![
                Value::I32(1),
                Value::I32(2),
                Value::I32(3),
            ])),
            ("nested_obj", {
                let mut map = HashMap::new();
                map.insert("inner_key".to_string(), Value::String("inner_value".to_string()));
                Value::Object(map)
            }),
        ];

        let old_node = create_old_node_with_props(id, "Complex", props.clone());
        let new_node = create_arena_node_with_props(&arena, id, "Complex", props);

        let old_bytes = bincode::serialize(&old_node).unwrap();
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Note: Property order may differ between HashMap (old) and ImmutablePropertiesMap (new)
        // So we check semantic equality by deserializing both and comparing the values
        let arena2 = Bump::new();
        let arena3 = Bump::new();

        let deserialized_old = Node::from_bincode_bytes(id, &old_bytes, &arena2).unwrap();
        let deserialized_new = Node::from_bincode_bytes(id, &new_bytes, &arena3).unwrap();

        assert_eq!(deserialized_old.id, id);
        assert_eq!(deserialized_old.label, "Complex");
        assert_eq!(deserialized_new.id, id);
        assert_eq!(deserialized_new.label, "Complex");

        let old_props = deserialized_old.properties.unwrap();
        let new_props = deserialized_new.properties.unwrap();
        assert_eq!(old_props.len(), new_props.len());

        // Check that both have the same keys and values (regardless of order)
        for (key, old_value) in old_props.iter() {
            let new_value = new_props.get(key).unwrap_or_else(|| panic!("Missing key: {}", key));
            // For nested objects, we need to compare recursively since HashMap order may differ
            assert!(values_equal(old_value, new_value), "Value mismatch for key {}: {:?} != {:?}", key, old_value, new_value);
        }
    }

    // Helper function to compare Values recursively, ignoring HashMap order
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Empty, Value::Empty) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::I8(a), Value::I8(b)) => a == b,
            (Value::I16(a), Value::I16(b)) => a == b,
            (Value::I32(a), Value::I32(b)) => a == b,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::U8(a), Value::U8(b)) => a == b,
            (Value::U16(a), Value::U16(b)) => a == b,
            (Value::U32(a), Value::U32(b)) => a == b,
            (Value::U64(a), Value::U64(b)) => a == b,
            (Value::U128(a), Value::U128(b)) => a == b,
            (Value::F32(a), Value::F32(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Value::F64(a), Value::F64(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_equal(x, y))
            }
            (Value::Object(a), Value::Object(b)) => {
                a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|bv| values_equal(v, bv)))
            }
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Id(a), Value::Id(b)) => a == b,
            _ => false,
        }
    }

    #[test]
    fn test_node_serialization_with_version() {
        let arena = Bump::new();
        let id = 33333u128;
        let label = "VersionedNode";

        // Create nodes with explicit version
        let old_node = OldNode {
            id,
            label: label.to_string(),
            version: 5,
            properties: None,
        };

        let new_node = Node {
            id,
            label: arena.alloc_str(label),
            version: 5,
            properties: None,
        };

        let old_bytes = bincode::serialize(&old_node).unwrap();
        let new_bytes = bincode::serialize(&new_node).unwrap();

        assert_eq!(
            old_bytes, new_bytes,
            "Serialized bytes differ for versioned nodes!"
        );
    }

    #[test]
    fn test_node_deserialization_from_old_format() {
        let arena = Bump::new();
        let id = 44444u128;

        let props = vec![
            ("name", Value::String("Charlie".to_string())),
            ("count", Value::U64(42)),
        ];

        // Create and serialize old node
        let old_node = create_old_node_with_props(id, "OldFormat", props);
        let old_bytes = bincode::serialize(&old_node).unwrap();

        // Deserialize into new node format
        let deserialized_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        // Verify fields
        assert_eq!(deserialized_node.id, id);
        assert_eq!(deserialized_node.label, "OldFormat");
        assert_eq!(deserialized_node.version, 0);
        assert!(deserialized_node.properties.is_some());

        let props = deserialized_node.properties.unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(props.get("name"), Some(&Value::String("Charlie".to_string())));
        assert_eq!(props.get("count"), Some(&Value::U64(42)));
    }

    #[test]
    fn test_node_roundtrip_serialization() {
        let arena = Bump::new();
        let id = 55555u128;

        let props = vec![
            ("key1", Value::String("value1".to_string())),
            ("key2", Value::I32(100)),
            ("key3", Value::Boolean(false)),
        ];

        // Create new node
        let original_node = create_arena_node_with_props(&arena, id, "Roundtrip", props);

        // Serialize
        let serialized = bincode::serialize(&original_node).unwrap();

        // Deserialize
        let arena2 = Bump::new();
        let deserialized_node = Node::from_bincode_bytes(id, &serialized, &arena2).unwrap();

        // Verify all fields match
        assert_eq!(deserialized_node.id, original_node.id);
        assert_eq!(deserialized_node.label, original_node.label);
        assert_eq!(deserialized_node.version, original_node.version);

        // Verify properties
        match (original_node.properties, deserialized_node.properties) {
            (Some(orig_props), Some(deser_props)) => {
                assert_eq!(orig_props.len(), deser_props.len());
                for (k, v) in orig_props.iter() {
                    assert_eq!(deser_props.get(k), Some(v));
                }
            }
            (None, None) => {}
            _ => panic!("Properties mismatch: one is Some, other is None"),
        }
    }

    #[test]
    fn test_node_byte_level_comparison_empty() {
        let arena = Bump::new();
        let id = 66666u128;

        let old_node = create_old_node_with_props(id, "Empty", vec![]);
        let new_node = create_arena_node_with_props(&arena, id, "Empty", vec![]);

        let old_bytes = bincode::serialize(&old_node).unwrap();
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Detailed byte comparison
        assert_eq!(old_bytes.len(), new_bytes.len(), "Byte lengths differ");

        for (i, (old_byte, new_byte)) in old_bytes.iter().zip(new_bytes.iter()).enumerate() {
            assert_eq!(
                old_byte, new_byte,
                "Byte mismatch at index {}: old={:02x}, new={:02x}",
                i, old_byte, new_byte
            );
        }
    }

    #[test]
    fn test_node_deserialization_semantic_equivalence() {
        let arena = Bump::new();
        let id = 77777u128;

        let props = vec![
            ("alpha", Value::String("beta".to_string())),
            ("gamma", Value::I64(999)),
        ];

        let new_node = create_arena_node_with_props(&arena, id, "ByteTest", props);
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Deserialize and verify
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, "ByteTest");

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(props.get("alpha"), Some(&Value::String("beta".to_string())));
        assert_eq!(props.get("gamma"), Some(&Value::I64(999)));
    }

    #[test]
    fn test_node_serialization_edge_cases() {
        let arena = Bump::new();

        // Empty label
        let node1_old = create_old_node_with_props(1, "", vec![]);
        let node1_new = create_arena_node_with_props(&arena, 1, "", vec![]);
        assert_eq!(
            bincode::serialize(&node1_old).unwrap(),
            bincode::serialize(&node1_new).unwrap()
        );

        // Very long label
        let long_label = "a".repeat(1000);
        let node2_old = create_old_node_with_props(2, &long_label, vec![]);
        let node2_new = create_arena_node_with_props(&arena, 2, &long_label, vec![]);
        assert_eq!(
            bincode::serialize(&node2_old).unwrap(),
            bincode::serialize(&node2_new).unwrap()
        );

        // Max u128 ID
        let node3_old = create_old_node_with_props(u128::MAX, "MaxID", vec![]);
        let node3_new = create_arena_node_with_props(&arena, u128::MAX, "MaxID", vec![]);
        assert_eq!(
            bincode::serialize(&node3_old).unwrap(),
            bincode::serialize(&node3_new).unwrap()
        );
    }

    #[test]
    fn test_node_serialization_utf8_labels() {
        let arena = Bump::new();

        let utf8_labels = ["Hello",
            "世界",
            "🚀🌟",
            "Привет",
            "مرحبا",
            "Ñoño"];

        for (idx, label) in utf8_labels.iter().enumerate() {
            let id = idx as u128;
            let old_node = create_old_node_with_props(id, label, vec![]);
            let new_node = create_arena_node_with_props(&arena, id, label, vec![]);

            let old_bytes = bincode::serialize(&old_node).unwrap();
            let new_bytes = bincode::serialize(&new_node).unwrap();

            assert_eq!(
                old_bytes, new_bytes,
                "UTF-8 label '{}' serialization differs", label
            );
        }
    }

    #[test]
    fn test_node_serialization_utf8_property_keys_and_values_roundtrip() {
        let arena = Bump::new();
        let id = 88888u128;

        let props = vec![
            ("名前", Value::String("太郎".to_string())),
            ("возраст", Value::I32(25)),
            ("emoji_key_🎉", Value::String("party_🎊".to_string())),
        ];

        let new_node = create_arena_node_with_props(&arena, id, "UTF8Props", props);
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Deserialize and verify UTF-8 handling
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 3);
        assert_eq!(props.get("名前"), Some(&Value::String("太郎".to_string())));
        assert_eq!(props.get("возраст"), Some(&Value::I32(25)));
        assert_eq!(props.get("emoji_key_🎉"), Some(&Value::String("party_🎊".to_string())));
    }

    #[test]
    fn test_node_serialization_many_properties_roundtrip() {
        let arena = Bump::new();
        let id = 99999u128;

        // Create 50 properties
        let props: Vec<(&str, Value)> = (0..50)
            .map(|i| {
                let key = Box::leak(format!("key_{}", i).into_boxed_str());
                (key as &str, Value::I32(i))
            })
            .collect();

        let new_node = create_arena_node_with_props(&arena, id, "ManyProps", props);
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Deserialize and verify all 50 properties
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 50);

        // Verify all properties are present with correct values
        for i in 0..50 {
            let key = format!("key_{}", i);
            assert_eq!(props.get(&key), Some(&Value::I32(i)), "Missing or incorrect value for {}", key);
        }
    }

    #[test]
    fn test_node_deserialization_from_old_preserves_all_properties() {
        let id = 12121u128;

        // Create old node and serialize it
        let props = vec![
            ("aaa", Value::I32(1)),
            ("bbb", Value::I32(2)),
            ("ccc", Value::I32(3)),
        ];

        let old_node = create_old_node_with_props(id, "Ordered", props);
        let old_bytes = bincode::serialize(&old_node).unwrap();

        // Deserialize using new format
        let arena = Bump::new();
        let new_node = Node::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        // Verify all properties are present (order may differ)
        let new_props = new_node.properties.unwrap();
        assert_eq!(new_props.len(), 3);
        assert_eq!(new_props.get("aaa"), Some(&Value::I32(1)));
        assert_eq!(new_props.get("bbb"), Some(&Value::I32(2)));
        assert_eq!(new_props.get("ccc"), Some(&Value::I32(3)));
    }

    #[test]
    fn test_node_empty_value_serialization_roundtrip() {
        let arena = Bump::new();
        let id = 13131u128;

        let props = vec![
            ("empty_val", Value::Empty),
            ("normal_val", Value::I32(42)),
        ];

        let new_node = create_arena_node_with_props(&arena, id, "EmptyValue", props);
        let new_bytes = bincode::serialize(&new_node).unwrap();

        // Deserialize and verify Empty value is preserved
        let arena2 = Bump::new();
        let deserialized = Node::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(props.get("empty_val"), Some(&Value::Empty));
        assert_eq!(props.get("normal_val"), Some(&Value::I32(42)));
    }
}

#[cfg(test)]
mod edge_serialization_tests {
    use crate::protocol::value::Value;
    use crate::utils::items::Edge;
    use crate::utils::properties::ImmutablePropertiesMap;
    use bumpalo::Bump;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    /// Old Edge implementation for comparison testing
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct OldEdge {
        #[serde(skip)]
        pub id: u128,
        pub label: String,
        #[serde(default)]
        pub version: u8,
        pub from_node: u128,
        pub to_node: u128,
        #[serde(default)]
        pub properties: Option<HashMap<String, Value>>,
    }

    /// Helper to create a test arena edge with properties
    fn create_arena_edge_with_props<'arena>(
        arena: &'arena Bump,
        id: u128,
        label: &str,
        from_node: u128,
        to_node: u128,
        props: Vec<(&str, Value)>,
    ) -> Edge<'arena> {
        let label_ref = arena.alloc_str(label);

        if props.is_empty() {
            Edge {
                id,
                label: label_ref,
                version: 0,
                from_node,
                to_node,
                properties: None,
            }
        } else {
            let len = props.len();
            let props_iter = props.into_iter().map(|(k, v)| {
                let key: &'arena str = arena.alloc_str(k);
                (key, v)
            });

            let props_map = ImmutablePropertiesMap::new(len, props_iter, arena);

            Edge {
                id,
                label: label_ref,
                version: 0,
                from_node,
                to_node,
                properties: Some(props_map),
            }
        }
    }

    /// Helper to create old edge with properties
    fn create_old_edge_with_props(
        id: u128,
        label: &str,
        from_node: u128,
        to_node: u128,
        props: Vec<(&str, Value)>,
    ) -> OldEdge {
        let properties = if props.is_empty() {
            None
        } else {
            let mut map = HashMap::new();
            for (k, v) in props {
                map.insert(k.to_string(), v);
            }
            Some(map)
        };

        OldEdge {
            id,
            label: label.to_string(),
            version: 0,
            from_node,
            to_node,
            properties,
        }
    }

    // Helper function to compare Values recursively, ignoring HashMap order
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Empty, Value::Empty) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::I8(a), Value::I8(b)) => a == b,
            (Value::I16(a), Value::I16(b)) => a == b,
            (Value::I32(a), Value::I32(b)) => a == b,
            (Value::I64(a), Value::I64(b)) => a == b,
            (Value::U8(a), Value::U8(b)) => a == b,
            (Value::U16(a), Value::U16(b)) => a == b,
            (Value::U32(a), Value::U32(b)) => a == b,
            (Value::U64(a), Value::U64(b)) => a == b,
            (Value::U128(a), Value::U128(b)) => a == b,
            (Value::F32(a), Value::F32(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Value::F64(a), Value::F64(b)) => (a.is_nan() && b.is_nan()) || a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| values_equal(x, y))
            }
            (Value::Object(a), Value::Object(b)) => {
                a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|bv| values_equal(v, bv)))
            }
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Id(a), Value::Id(b)) => a == b,
            _ => false,
        }
    }

    #[test]
    fn test_edge_serialization_empty_properties() {
        let arena = Bump::new();
        let id = 1000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let old_edge = OldEdge {
            id,
            label: "KNOWS".to_string(),
            version: 0,
            from_node,
            to_node,
            properties: None,
        };

        let new_edge = Edge {
            id,
            label: arena.alloc_str("KNOWS"),
            version: 0,
            from_node,
            to_node,
            properties: None,
        };

        let old_bytes = bincode::serialize(&old_edge).unwrap();
        let new_bytes = bincode::serialize(&new_edge).unwrap();

        // Bytes should be IDENTICAL
        assert_eq!(
            old_bytes, new_bytes,
            "Serialized bytes differ for empty properties!"
        );

        // Test that new format can deserialize its own output
        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &new_bytes, &arena2).unwrap();

        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, "KNOWS");
        assert_eq!(deserialized.version, 0);
        assert_eq!(deserialized.from_node, from_node);
        assert_eq!(deserialized.to_node, to_node);
        assert!(deserialized.properties.is_none());
    }

    #[test]
    fn test_edge_serialization_with_properties() {
        let arena = Bump::new();
        let id = 2000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let props = vec![
            ("weight", Value::F64(0.8)),
            ("type", Value::String("friend".to_string())),
            ("since", Value::I32(2020)),
        ];

        let old_edge = create_old_edge_with_props(id, "KNOWS", from_node, to_node, props.clone());
        let new_edge = create_arena_edge_with_props(&arena, id, "KNOWS", from_node, to_node, props);

        let old_bytes = bincode::serialize(&old_edge).unwrap();
        let new_bytes = bincode::serialize(&new_edge).unwrap();

        // Deserialize both and compare semantically
        let arena2 = Bump::new();
        let arena3 = Bump::new();

        let deserialized_old = Edge::from_bincode_bytes(id, &old_bytes, &arena2).unwrap();
        let deserialized_new = Edge::from_bincode_bytes(id, &new_bytes, &arena3).unwrap();

        assert_eq!(deserialized_old.id, id);
        assert_eq!(deserialized_new.id, id);
        assert_eq!(deserialized_old.label, "KNOWS");
        assert_eq!(deserialized_new.label, "KNOWS");
        assert_eq!(deserialized_old.from_node, from_node);
        assert_eq!(deserialized_new.from_node, from_node);
        assert_eq!(deserialized_old.to_node, to_node);
        assert_eq!(deserialized_new.to_node, to_node);

        let old_props = deserialized_old.properties.unwrap();
        let new_props = deserialized_new.properties.unwrap();
        assert_eq!(old_props.len(), new_props.len());

        // Check semantic equality (order may differ)
        for (key, old_value) in old_props.iter() {
            let new_value = new_props.get(key).unwrap_or_else(|| panic!("Missing key: {}", key));
            assert!(values_equal(old_value, new_value), "Value mismatch for key {}", key);
        }
    }

    #[test]
    fn test_edge_roundtrip_serialization() {
        let arena = Bump::new();
        let id = 3000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let props = vec![
            ("confidence", Value::F64(0.95)),
            ("verified", Value::Boolean(true)),
        ];

        let original = create_arena_edge_with_props(&arena, id, "RELATED_TO", from_node, to_node, props);
        let bytes = bincode::serialize(&original).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

        assert_eq!(deserialized.id, original.id);
        assert_eq!(deserialized.label, original.label);
        assert_eq!(deserialized.version, original.version);
        assert_eq!(deserialized.from_node, original.from_node);
        assert_eq!(deserialized.to_node, original.to_node);

        let original_props = original.properties.unwrap();
        let deserialized_props = deserialized.properties.unwrap();

        assert_eq!(original_props.len(), deserialized_props.len());
        for (key, value) in original_props.iter() {
            assert_eq!(deserialized_props.get(key), Some(value));
        }
    }

    #[test]
    fn test_edge_deserialization_from_old_format() {
        let id = 4000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let props = vec![
            ("strength", Value::I32(5)),
            ("label_text", Value::String("connection".to_string())),
        ];

        let old_edge = create_old_edge_with_props(id, "CONNECTS", from_node, to_node, props);
        let old_bytes = bincode::serialize(&old_edge).unwrap();

        // New format should deserialize old format
        let arena = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &old_bytes, &arena).unwrap();

        assert_eq!(deserialized.id, id);
        assert_eq!(deserialized.label, "CONNECTS");
        assert_eq!(deserialized.version, 0);
        assert_eq!(deserialized.from_node, from_node);
        assert_eq!(deserialized.to_node, to_node);

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 2);
        assert_eq!(props.get("strength"), Some(&Value::I32(5)));
        assert_eq!(props.get("label_text"), Some(&Value::String("connection".to_string())));
    }

    #[test]
    fn test_edge_with_nested_values() {
        let arena = Bump::new();
        let id = 5000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let props = vec![
            ("metadata", {
                let mut map = HashMap::new();
                map.insert("created_by".to_string(), Value::String("system".to_string()));
                map.insert("timestamp".to_string(), Value::I64(1234567890));
                Value::Object(map)
            }),
            ("tags", Value::Array(vec![
                Value::String("important".to_string()),
                Value::String("verified".to_string()),
            ])),
        ];

        let old_edge = create_old_edge_with_props(id, "HAS_TAG", from_node, to_node, props.clone());
        let new_edge = create_arena_edge_with_props(&arena, id, "HAS_TAG", from_node, to_node, props);

        let old_bytes = bincode::serialize(&old_edge).unwrap();
        let new_bytes = bincode::serialize(&new_edge).unwrap();

        // Deserialize and compare semantically
        let arena2 = Bump::new();
        let arena3 = Bump::new();

        let deserialized_old = Edge::from_bincode_bytes(id, &old_bytes, &arena2).unwrap();
        let deserialized_new = Edge::from_bincode_bytes(id, &new_bytes, &arena3).unwrap();

        let old_props = deserialized_old.properties.unwrap();
        let new_props = deserialized_new.properties.unwrap();
        assert_eq!(old_props.len(), new_props.len());

        // Compare nested values
        for (key, old_value) in old_props.iter() {
            let new_value = new_props.get(key).unwrap_or_else(|| panic!("Missing key: {}", key));
            assert!(values_equal(old_value, new_value), "Value mismatch for key {}: {:?} != {:?}", key, old_value, new_value);
        }
    }

    #[test]
    fn test_edge_with_many_properties() {
        let arena = Bump::new();
        let id = 6000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        // Create edge with 20 properties
        let props: Vec<(&str, Value)> = (0..20)
            .map(|i| {
                (
                    Box::leak(format!("prop_{}", i).into_boxed_str()) as &str,
                    Value::I32(i),
                )
            })
            .collect();

        let new_edge = create_arena_edge_with_props(&arena, id, "BULK", from_node, to_node, props);
        let bytes = bincode::serialize(&new_edge).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

        let props = deserialized.properties.unwrap();
        assert_eq!(props.len(), 20);

        // Verify all properties are present
        for i in 0..20 {
            let key = format!("prop_{}", i);
            assert_eq!(props.get(&key), Some(&Value::I32(i)), "Property {} mismatch", key);
        }
    }

    #[test]
    fn test_edge_byte_level_comparison_empty() {
        let arena = Bump::new();
        let id = 7000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let old_edge = OldEdge {
            id,
            label: "LINKS".to_string(),
            version: 0,
            from_node,
            to_node,
            properties: None,
        };

        let new_edge = Edge {
            id,
            label: arena.alloc_str("LINKS"),
            version: 0,
            from_node,
            to_node,
            properties: None,
        };

        let old_bytes = bincode::serialize(&old_edge).unwrap();
        let new_bytes = bincode::serialize(&new_edge).unwrap();

        println!("\n=== EDGE EMPTY PROPERTIES COMPARISON ===");
        println!("Old bytes ({} total): {:02x?}", old_bytes.len(), old_bytes);
        println!("New bytes ({} total): {:02x?}", new_bytes.len(), new_bytes);

        // Detailed analysis
        println!("\nByte-by-byte comparison:");
        for (i, (old_byte, new_byte)) in old_bytes.iter().zip(new_bytes.iter()).enumerate() {
            if old_byte != new_byte {
                println!("  Index {}: old={:02x} ({}), new={:02x} ({})",
                    i, old_byte, old_byte, new_byte, new_byte);
            }
        }

        // Bytes should be IDENTICAL
        assert_eq!(
            old_bytes, new_bytes,
            "Serialized bytes differ for empty properties!"
        );
    }

    #[test]
    fn test_edge_with_utf8_labels_and_properties() {
        let arena = Bump::new();
        let id = 8000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let props = vec![
            ("名前", Value::String("太郎".to_string())),
            ("emoji", Value::String("🔗".to_string())),
        ];

        let new_edge = create_arena_edge_with_props(&arena, id, "繋がり", from_node, to_node, props);
        let bytes = bincode::serialize(&new_edge).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

        assert_eq!(deserialized.label, "繋がり");
        let props = deserialized.properties.unwrap();
        assert_eq!(props.get("名前"), Some(&Value::String("太郎".to_string())));
        assert_eq!(props.get("emoji"), Some(&Value::String("🔗".to_string())));
    }

    #[test]
    fn test_edge_with_version() {
        let arena = Bump::new();
        let id = 9000u128;
        let from_node = 100u128;
        let to_node = 200u128;

        let edge = Edge {
            id,
            label: arena.alloc_str("VERSIONED"),
            version: 42,
            from_node,
            to_node,
            properties: None,
        };

        let bytes = bincode::serialize(&edge).unwrap();

        let arena2 = Bump::new();
        let deserialized = Edge::from_bincode_bytes(id, &bytes, &arena2).unwrap();

        assert_eq!(deserialized.version, 42);
    }
}
