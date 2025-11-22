//! Test utilities for serialization/deserialization testing
//!
//! This module provides factory functions, assertion helpers, and common test utilities
//! for comprehensive serialization testing of nodes, edges, and vectors.

#![cfg(test)]

use crate::helix_engine::vector_core::HVector;
use crate::helix_engine::vector_core::distance::Cosine;
use crate::helix_engine::vector_core::node::Item;
use crate::protocol::value::Value;
use crate::utils::items::{Edge, Node};
use crate::utils::properties::ImmutablePropertiesMap;
use bumpalo::Bump;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// OLD TYPES FOR BACKWARDS COMPATIBILITY TESTING
// ============================================================================

/// Old Node implementation for compatibility testing
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct OldNode {
    #[serde(skip)]
    pub id: u128,
    pub label: String,
    #[serde(default)]
    pub version: u8,
    #[serde(default)]
    pub properties: Option<HashMap<String, Value>>,
}

/// Old Edge implementation for compatibility testing
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct OldEdge {
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

/// Old Vector implementation for compatibility testing
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub struct OldVector {
    #[serde(skip)]
    pub id: u128,
    pub label: String,
    #[serde(default)]
    pub version: u8,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub properties: Option<HashMap<String, Value>>,
}

// ============================================================================
// NODE FACTORY FUNCTIONS
// ============================================================================

/// Creates an arena-allocated Node with properties
pub fn create_arena_node<'arena>(
    arena: &'arena Bump,
    id: u128,
    label: &str,
    version: u8,
    props: Vec<(&str, Value)>,
) -> Node<'arena> {
    let label_ref = arena.alloc_str(label);

    if props.is_empty() {
        Node {
            id,
            label: label_ref,
            version,
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
            version,
            properties: Some(props_map),
        }
    }
}

/// Creates a simple arena-allocated Node without properties
pub fn create_simple_node<'arena>(arena: &'arena Bump, id: u128, label: &str) -> Node<'arena> {
    create_arena_node(arena, id, label, 0, vec![])
}

/// Creates an old-style Node for compatibility testing
pub fn create_old_node(id: u128, label: &str, version: u8, props: Vec<(&str, Value)>) -> OldNode {
    if props.is_empty() {
        OldNode {
            id,
            label: label.to_string(),
            version,
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
            version,
            properties: Some(props_map),
        }
    }
}

// ============================================================================
// EDGE FACTORY FUNCTIONS
// ============================================================================

/// Creates an arena-allocated Edge with properties
pub fn create_arena_edge<'arena>(
    arena: &'arena Bump,
    id: u128,
    label: &str,
    version: u8,
    from_node: u128,
    to_node: u128,
    props: Vec<(&str, Value)>,
) -> Edge<'arena> {
    let label_ref = arena.alloc_str(label);

    if props.is_empty() {
        Edge {
            id,
            label: label_ref,
            version,
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
            version,
            from_node,
            to_node,
            properties: Some(props_map),
        }
    }
}

/// Creates a simple arena-allocated Edge without properties
pub fn create_simple_edge<'arena>(
    arena: &'arena Bump,
    id: u128,
    label: &str,
    from_node: u128,
    to_node: u128,
) -> Edge<'arena> {
    create_arena_edge(arena, id, label, 0, from_node, to_node, vec![])
}

/// Creates an old-style Edge for compatibility testing
pub fn create_old_edge(
    id: u128,
    label: &str,
    version: u8,
    from_node: u128,
    to_node: u128,
    props: Vec<(&str, Value)>,
) -> OldEdge {
    if props.is_empty() {
        OldEdge {
            id,
            label: label.to_string(),
            version,
            from_node,
            to_node,
            properties: None,
        }
    } else {
        let mut props_map = HashMap::new();
        for (k, v) in props {
            props_map.insert(k.to_string(), v);
        }

        OldEdge {
            id,
            label: label.to_string(),
            version,
            from_node,
            to_node,
            properties: Some(props_map),
        }
    }
}

// ============================================================================
// VECTOR FACTORY FUNCTIONS
// ============================================================================

/// Creates an arena-allocated HVector with properties
pub fn create_arena_vector<'arena>(
    arena: &'arena Bump,
    id: u128,
    label: &str,
    version: u8,
    deleted: bool,
    data: &[f32],
    props: Vec<(&str, Value)>,
) -> HVector<'arena> {
    let label_ref = arena.alloc_str(label);

    let mut bump_vec = bumpalo::collections::Vec::new_in(arena);
    bump_vec.extend_from_slice(data);

    if props.is_empty() {
        HVector {
            id,
            label: label_ref,
            version,
            deleted,
            distance: None,
            data: Some(Item::<Cosine>::new(bump_vec)),
            properties: None,
            level: None,
        }
    } else {
        let len = props.len();
        let props_iter = props.into_iter().map(|(k, v)| {
            let key: &'arena str = arena.alloc_str(k);
            (key, v)
        });
        let props_map = ImmutablePropertiesMap::new(len, props_iter, arena);

        HVector {
            id,
            label: label_ref,
            version,
            deleted,
            distance: None,
            data: Some(Item::<Cosine>::new(bump_vec)),
            properties: Some(props_map),
            level: None,
        }
    }
}

/// Creates a simple arena-allocated HVector without properties
pub fn create_simple_vector<'arena>(
    arena: &'arena Bump,
    id: u128,
    label: &str,
    data: &[f32],
) -> HVector<'arena> {
    create_arena_vector(arena, id, label, 1, false, data, vec![])
}

/// Creates vector data as raw bytes
pub fn create_vector_bytes(data: &[f32]) -> Vec<u8> {
    bytemuck::cast_slice(data).to_vec()
}

/// Creates an old-style Vector for compatibility testing
pub fn create_old_vector(
    id: u128,
    label: &str,
    version: u8,
    deleted: bool,
    props: Vec<(&str, Value)>,
) -> OldVector {
    if props.is_empty() {
        OldVector {
            id,
            label: label.to_string(),
            version,
            deleted,
            properties: None,
        }
    } else {
        let mut props_map = HashMap::new();
        for (k, v) in props {
            props_map.insert(k.to_string(), v);
        }

        OldVector {
            id,
            label: label.to_string(),
            version,
            deleted,
            properties: Some(props_map),
        }
    }
}

// ============================================================================
// PROPERTY BUILDERS
// ============================================================================

/// Creates a vector of properties with all Value types for comprehensive testing
pub fn all_value_types_props() -> Vec<(&'static str, Value)> {
    vec![
        ("string_val", Value::String("test".to_string())),
        ("f32_val", Value::F32(3.14)),
        ("f64_val", Value::F64(2.718)),
        ("i8_val", Value::I8(-127)),
        ("i16_val", Value::I16(-32000)),
        ("i32_val", Value::I32(-2000000)),
        ("i64_val", Value::I64(-9000000000)),
        ("u8_val", Value::U8(255)),
        ("u16_val", Value::U16(65000)),
        ("u32_val", Value::U32(4000000)),
        ("u64_val", Value::U64(18000000000)),
        (
            "u128_val",
            Value::U128(340282366920938463463374607431768211455),
        ),
        ("bool_val", Value::Boolean(true)),
        ("empty_val", Value::Empty),
    ]
}

/// Creates nested Value structures for edge case testing
pub fn nested_value_props() -> Vec<(&'static str, Value)> {
    vec![
        (
            "array_val",
            Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]),
        ),
        (
            "object_val",
            Value::Object(
                vec![
                    (
                        "nested_key".to_string(),
                        Value::String("nested_value".to_string()),
                    ),
                    ("nested_num".to_string(), Value::I32(42)),
                ]
                .into_iter()
                .collect(),
            ),
        ),
        (
            "deeply_nested",
            Value::Array(vec![Value::Object(
                vec![(
                    "inner".to_string(),
                    Value::Array(vec![Value::I32(1), Value::I32(2)]),
                )]
                .into_iter()
                .collect(),
            )]),
        ),
    ]
}

/// Creates a large number of properties for stress testing
#[allow(dead_code)]
pub fn many_props(count: usize) -> Vec<(String, Value)> {
    (0..count)
        .map(|i| (format!("key_{}", i), Value::I32(i as i32)))
        .collect()
}

// ============================================================================
// ASSERTION HELPERS
// ============================================================================

/// Asserts that two nodes are semantically equal (properties may be in different order)
pub fn assert_nodes_semantically_equal(node1: &Node, node2: &Node) {
    assert_eq!(node1.id, node2.id, "Node IDs differ");
    assert_eq!(node1.label, node2.label, "Node labels differ");
    assert_eq!(node1.version, node2.version, "Node versions differ");

    match (&node1.properties, &node2.properties) {
        (None, None) => {}
        (Some(props1), Some(props2)) => {
            assert_eq!(props1.len(), props2.len(), "Node property counts differ");
            // Check each property exists and has the same value
            for (key1, val1) in props1.iter() {
                if let Some(val2) = props2.get(key1) {
                    assert_eq!(val1, val2, "Property value differs for key: {}", key1);
                } else {
                    panic!("Property key '{}' not found in second node", key1);
                }
            }
        }
        _ => panic!("One node has properties and the other doesn't"),
    }
}

/// Asserts that two edges are semantically equal (properties may be in different order)
pub fn assert_edges_semantically_equal(edge1: &Edge, edge2: &Edge) {
    assert_eq!(edge1.id, edge2.id, "Edge IDs differ");
    assert_eq!(edge1.label, edge2.label, "Edge labels differ");
    assert_eq!(edge1.version, edge2.version, "Edge versions differ");
    assert_eq!(edge1.from_node, edge2.from_node, "Edge from_node differs");
    assert_eq!(edge1.to_node, edge2.to_node, "Edge to_node differs");

    match (&edge1.properties, &edge2.properties) {
        (None, None) => {}
        (Some(props1), Some(props2)) => {
            assert_eq!(props1.len(), props2.len(), "Edge property counts differ");
            for (key1, val1) in props1.iter() {
                if let Some(val2) = props2.get(key1) {
                    assert_eq!(val1, val2, "Property value differs for key: {}", key1);
                } else {
                    panic!("Property key '{}' not found in second edge", key1);
                }
            }
        }
        _ => panic!("One edge has properties and the other doesn't"),
    }
}

/// Asserts that two vectors are semantically equal (excluding distance and level which are runtime)
pub fn assert_vectors_semantically_equal(vec1: &HVector, vec2: &HVector) {
    assert_eq!(vec1.id, vec2.id, "Vector IDs differ");
    assert_eq!(vec1.label, vec2.label, "Vector labels differ");
    assert_eq!(vec1.version, vec2.version, "Vector versions differ");
    assert_eq!(vec1.deleted, vec2.deleted, "Vector deleted flags differ");
    assert_eq!(vec1.len(), vec2.len(), "Vector dimensions differ");

    // Compare vector data with floating point tolerance
    for (i, (v1, v2)) in vec1
        .data_borrowed()
        .iter()
        .zip(vec2.data_borrowed().iter())
        .enumerate()
    {
        assert!(
            (v1 - v2).abs() < 1e-10,
            "Vector data differs at index {}: {} vs {}",
            i,
            v1,
            v2
        );
    }

    match (&vec1.properties, &vec2.properties) {
        (None, None) => {}
        (Some(props1), Some(props2)) => {
            assert_eq!(props1.len(), props2.len(), "Vector property counts differ");
            for (key1, val1) in props1.iter() {
                if let Some(val2) = props2.get(key1) {
                    assert_eq!(val1, val2, "Property value differs for key: {}", key1);
                } else {
                    panic!("Property key '{}' not found in second vector", key1);
                }
            }
        }
        _ => panic!("One vector has properties and the other doesn't"),
    }
}

// ============================================================================
// DIAGNOSTIC HELPERS
// ============================================================================

/// Prints byte-level comparison of two byte arrays
#[allow(dead_code)]
pub fn print_byte_comparison(label: &str, bytes1: &[u8], bytes2: &[u8]) {
    println!("\n=== {} ===", label);
    println!("Bytes1 ({} total): {:02x?}", bytes1.len(), bytes1);
    println!("Bytes2 ({} total): {:02x?}", bytes2.len(), bytes2);

    if bytes1.len() != bytes2.len() {
        println!("WARNING: Byte arrays have different lengths!");
    }

    println!("\nByte-by-byte comparison:");
    let min_len = bytes1.len().min(bytes2.len());
    for (i, (b1, b2)) in bytes1.iter().zip(bytes2.iter()).take(min_len).enumerate() {
        if b1 != b2 {
            println!(
                "  Index {}: bytes1={:02x} ({}), bytes2={:02x} ({})",
                i, b1, b1, b2, b2
            );
        }
    }

    if bytes1.len() > min_len {
        println!("  Bytes1 has {} extra bytes", bytes1.len() - min_len);
    }
    if bytes2.len() > min_len {
        println!("  Bytes2 has {} extra bytes", bytes2.len() - min_len);
    }
}

/// Prints human-readable interpretation of bytes
#[allow(dead_code)]
pub fn print_byte_interpretation(label: &str, bytes: &[u8]) {
    println!("\n{} as string interpretation:", label);
    for (i, byte) in bytes.iter().enumerate() {
        if *byte >= 32 && *byte < 127 {
            print!("{}", *byte as char);
        } else {
            print!("[{:02x}]", byte);
        }
        if (i + 1) % 60 == 0 {
            println!();
        }
    }
    println!();
}

// ============================================================================
// RANDOM DATA GENERATORS (for property-based testing)
// ============================================================================

/// Generates a random string of given length
#[allow(dead_code)]
pub fn random_string(len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..len)
        .map(|_| rng.random_range(b'a'..=b'z') as char)
        .collect()
}

/// Generates random valid UTF-8 strings including Unicode
#[allow(dead_code)]
pub fn random_utf8_string(len: usize) -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let chars = vec!['a', 'b', 'ñ', '中', '文', '🔥', '🚀', 'é', 'ü'];
    (0..len)
        .map(|_| chars[rng.random_range(0..chars.len())])
        .collect()
}

/// Generates a random f64 vector of given dimensions
#[allow(dead_code)]
pub fn random_f64_vector(dimensions: usize) -> Vec<f64> {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..dimensions)
        .map(|_| rng.random_range(-1.0..1.0))
        .collect()
}

/// Generates a random Value for property testing
#[allow(dead_code)]
pub fn random_value() -> Value {
    use rand::Rng;
    let mut rng = rand::rng();
    match rng.random_range(0..10) {
        0 => Value::String(random_string(10)),
        1 => Value::I32(rng.random()),
        2 => Value::I64(rng.random()),
        3 => Value::F64(rng.random()),
        4 => Value::Boolean(rng.random()),
        5 => Value::U64(rng.random()),
        6 => Value::Empty,
        7 => Value::Array(vec![Value::I32(rng.random()), Value::I32(rng.random())]),
        8 => {
            let mut map = HashMap::new();
            map.insert("key".to_string(), Value::I32(rng.random()));
            Value::Object(map)
        }
        _ => Value::String("default".to_string()),
    }
}
