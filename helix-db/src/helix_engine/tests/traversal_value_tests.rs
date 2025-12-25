//! Unit tests for TraversalValue enum and its methods.

use crate::helix_engine::traversal_core::traversal_value::TraversalValue;
use crate::helix_engine::vector_core::vector::HVector;
use crate::helix_engine::vector_core::vector_without_data::VectorWithoutData;
use crate::protocol::value::Value;
use crate::utils::items::{Edge, Node};
use crate::utils::properties::ImmutablePropertiesMap;
use bumpalo::Bump;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// ============================================================================
// Test Helpers
// ============================================================================

fn create_test_node<'a>(arena: &'a Bump, id: u128, label: &str) -> Node<'a> {
    Node {
        id,
        label: arena.alloc_str(label),
        version: 1,
        properties: None,
    }
}

fn create_test_node_with_props<'a>(
    arena: &'a Bump,
    id: u128,
    label: &str,
    props: Vec<(&str, Value)>,
) -> Node<'a> {
    let properties = ImmutablePropertiesMap::new(
        props.len(),
        props
            .into_iter()
            .map(|(k, v)| (arena.alloc_str(k) as &str, v)),
        arena,
    );
    Node {
        id,
        label: arena.alloc_str(label),
        version: 1,
        properties: Some(properties),
    }
}

fn create_test_edge<'a>(
    arena: &'a Bump,
    id: u128,
    label: &str,
    from_node: u128,
    to_node: u128,
) -> Edge<'a> {
    Edge {
        id,
        label: arena.alloc_str(label),
        version: 1,
        from_node,
        to_node,
        properties: None,
    }
}

fn create_test_edge_with_props<'a>(
    arena: &'a Bump,
    id: u128,
    label: &str,
    from_node: u128,
    to_node: u128,
    props: Vec<(&str, Value)>,
) -> Edge<'a> {
    let properties = ImmutablePropertiesMap::new(
        props.len(),
        props
            .into_iter()
            .map(|(k, v)| (arena.alloc_str(k) as &str, v)),
        arena,
    );
    Edge {
        id,
        label: arena.alloc_str(label),
        version: 1,
        from_node,
        to_node,
        properties: Some(properties),
    }
}

fn create_test_vector<'a>(arena: &'a Bump, id: u128, label: &str, data: &[f64]) -> HVector<'a> {
    HVector {
        id,
        label: arena.alloc_str(label),
        version: 1,
        deleted: false,
        level: 0,
        distance: Some(0.5),
        data: arena.alloc_slice_copy(data),
        properties: None,
    }
}

fn create_test_vector_with_props<'a>(
    arena: &'a Bump,
    id: u128,
    label: &str,
    data: &[f64],
    props: Vec<(&str, Value)>,
) -> HVector<'a> {
    let properties = ImmutablePropertiesMap::new(
        props.len(),
        props
            .into_iter()
            .map(|(k, v)| (arena.alloc_str(k) as &str, v)),
        arena,
    );
    HVector {
        id,
        label: arena.alloc_str(label),
        version: 1,
        deleted: false,
        level: 0,
        distance: Some(0.5),
        data: arena.alloc_slice_copy(data),
        properties: Some(properties),
    }
}

fn create_test_vector_without_data<'a>(arena: &'a Bump, id: u128, label: &str) -> VectorWithoutData<'a> {
    VectorWithoutData {
        id,
        label: arena.alloc_str(label),
        version: 1,
        deleted: false,
        level: 0,
        properties: None,
    }
}

fn create_test_vector_without_data_with_props<'a>(
    arena: &'a Bump,
    id: u128,
    label: &str,
    props: Vec<(&str, Value)>,
) -> VectorWithoutData<'a> {
    let properties = ImmutablePropertiesMap::new(
        props.len(),
        props
            .into_iter()
            .map(|(k, v)| (arena.alloc_str(k) as &str, v)),
        arena,
    );
    VectorWithoutData {
        id,
        label: arena.alloc_str(label),
        version: 1,
        deleted: false,
        level: 0,
        properties: Some(properties),
    }
}

fn hash_value<T: Hash>(val: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    val.hash(&mut hasher);
    hasher.finish()
}

// ============================================================================
// id() Method Tests
// ============================================================================

#[test]
fn test_id_node_returns_correct_id() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 12345, "TestNode");
    let tv = TraversalValue::Node(node);
    assert_eq!(tv.id(), 12345);
}

#[test]
fn test_id_edge_returns_correct_id() {
    let arena = Bump::new();
    let edge = create_test_edge(&arena, 67890, "TestEdge", 1, 2);
    let tv = TraversalValue::Edge(edge);
    assert_eq!(tv.id(), 67890);
}

#[test]
fn test_id_vector_returns_correct_id() {
    let arena = Bump::new();
    let vector = create_test_vector(&arena, 11111, "TestVector", &[1.0, 2.0, 3.0]);
    let tv = TraversalValue::Vector(vector);
    assert_eq!(tv.id(), 11111);
}

#[test]
fn test_id_vector_without_data_returns_correct_id() {
    let arena = Bump::new();
    let vector = create_test_vector_without_data(&arena, 22222, "TestVector");
    let tv = TraversalValue::VectorNodeWithoutVectorData(vector);
    assert_eq!(tv.id(), 22222);
}

#[test]
fn test_id_node_with_score_returns_correct_id() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 33333, "TestNode");
    let tv = TraversalValue::NodeWithScore { node, score: 0.95 };
    assert_eq!(tv.id(), 33333);
}

#[test]
fn test_id_empty_returns_zero() {
    let tv: TraversalValue = TraversalValue::Empty;
    assert_eq!(tv.id(), 0);
}

#[test]
fn test_id_path_returns_zero() {
    let tv: TraversalValue = TraversalValue::Path((vec![], vec![]));
    assert_eq!(tv.id(), 0);
}

#[test]
fn test_id_value_returns_zero() {
    let tv = TraversalValue::Value(Value::String("test".to_string()));
    assert_eq!(tv.id(), 0);
}

// ============================================================================
// label() Method Tests
// ============================================================================

#[test]
fn test_label_node_returns_correct_label() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Person");
    let tv = TraversalValue::Node(node);
    assert_eq!(tv.label(), "Person");
}

#[test]
fn test_label_edge_returns_correct_label() {
    let arena = Bump::new();
    let edge = create_test_edge(&arena, 1, "KNOWS", 1, 2);
    let tv = TraversalValue::Edge(edge);
    assert_eq!(tv.label(), "KNOWS");
}

#[test]
fn test_label_vector_returns_correct_label() {
    let arena = Bump::new();
    let vector = create_test_vector(&arena, 1, "Embedding", &[1.0]);
    let tv = TraversalValue::Vector(vector);
    assert_eq!(tv.label(), "Embedding");
}

#[test]
fn test_label_vector_without_data_returns_correct_label() {
    let arena = Bump::new();
    let vector = create_test_vector_without_data(&arena, 1, "Embedding");
    let tv = TraversalValue::VectorNodeWithoutVectorData(vector);
    assert_eq!(tv.label(), "Embedding");
}

#[test]
fn test_label_node_with_score_returns_correct_label() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Document");
    let tv = TraversalValue::NodeWithScore { node, score: 0.9 };
    assert_eq!(tv.label(), "Document");
}

#[test]
fn test_label_empty_returns_empty_string() {
    let tv: TraversalValue = TraversalValue::Empty;
    assert_eq!(tv.label(), "");
}

#[test]
fn test_label_path_returns_empty_string() {
    let tv: TraversalValue = TraversalValue::Path((vec![], vec![]));
    assert_eq!(tv.label(), "");
}

#[test]
fn test_label_value_returns_empty_string() {
    let tv = TraversalValue::Value(Value::I32(42));
    assert_eq!(tv.label(), "");
}

// ============================================================================
// from_node() / to_node() Method Tests
// ============================================================================

#[test]
fn test_from_node_edge_returns_correct_value() {
    let arena = Bump::new();
    let edge = create_test_edge(&arena, 1, "LIKES", 100, 200);
    let tv = TraversalValue::Edge(edge);
    assert_eq!(tv.from_node(), 100);
}

#[test]
fn test_to_node_edge_returns_correct_value() {
    let arena = Bump::new();
    let edge = create_test_edge(&arena, 1, "LIKES", 100, 200);
    let tv = TraversalValue::Edge(edge);
    assert_eq!(tv.to_node(), 200);
}

#[test]
#[should_panic(expected = "not implemented")]
fn test_from_node_node_panics() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Test");
    let tv = TraversalValue::Node(node);
    let _ = tv.from_node();
}

#[test]
#[should_panic(expected = "not implemented")]
fn test_to_node_node_panics() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Test");
    let tv = TraversalValue::Node(node);
    let _ = tv.to_node();
}

// ============================================================================
// data() Method Tests
// ============================================================================

#[test]
fn test_data_vector_returns_correct_slice() {
    let arena = Bump::new();
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
    let vector = create_test_vector(&arena, 1, "Vec", &data);
    let tv = TraversalValue::Vector(vector);
    assert_eq!(tv.data(), &[1.0, 2.0, 3.0, 4.0, 5.0]);
}

#[test]
fn test_data_vector_without_data_returns_empty_slice() {
    let arena = Bump::new();
    let vector = create_test_vector_without_data(&arena, 1, "Vec");
    let tv = TraversalValue::VectorNodeWithoutVectorData(vector);
    assert_eq!(tv.data(), &[] as &[f64]);
}

#[test]
#[should_panic(expected = "not implemented")]
fn test_data_node_panics() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Test");
    let tv = TraversalValue::Node(node);
    let _ = tv.data();
}

// ============================================================================
// score() Method Tests
// ============================================================================

#[test]
fn test_score_vector_returns_distance() {
    let arena = Bump::new();
    let mut vector = create_test_vector(&arena, 1, "Vec", &[1.0]);
    vector.distance = Some(0.75);
    let tv = TraversalValue::Vector(vector);
    assert!((tv.score() - 0.75).abs() < f64::EPSILON);
}

#[test]
fn test_score_vector_without_data_returns_two() {
    let arena = Bump::new();
    let vector = create_test_vector_without_data(&arena, 1, "Vec");
    let tv = TraversalValue::VectorNodeWithoutVectorData(vector);
    assert!((tv.score() - 2.0).abs() < f64::EPSILON);
}

#[test]
fn test_score_node_with_score_returns_stored_score() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Doc");
    let tv = TraversalValue::NodeWithScore {
        node,
        score: 0.123456,
    };
    assert!((tv.score() - 0.123456).abs() < f64::EPSILON);
}

#[test]
#[should_panic(expected = "not implemented")]
fn test_score_node_panics() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Test");
    let tv = TraversalValue::Node(node);
    let _ = tv.score();
}

// ============================================================================
// get_property() Method Tests
// ============================================================================

#[test]
fn test_get_property_node_existing_property() {
    let arena = Bump::new();
    let node = create_test_node_with_props(&arena, 1, "Person", vec![("name", Value::String("Alice".to_string()))]);
    let tv = TraversalValue::Node(node);
    let prop = tv.get_property("name");
    assert!(prop.is_some());
    assert_eq!(prop.unwrap(), &Value::String("Alice".to_string()));
}

#[test]
fn test_get_property_node_nonexistent_property() {
    let arena = Bump::new();
    let node = create_test_node_with_props(&arena, 1, "Person", vec![("name", Value::String("Alice".to_string()))]);
    let tv = TraversalValue::Node(node);
    let prop = tv.get_property("age");
    assert!(prop.is_none());
}

#[test]
fn test_get_property_edge_existing_property() {
    let arena = Bump::new();
    let edge = create_test_edge_with_props(&arena, 1, "KNOWS", 1, 2, vec![("since", Value::I32(2020))]);
    let tv = TraversalValue::Edge(edge);
    let prop = tv.get_property("since");
    assert!(prop.is_some());
    assert_eq!(prop.unwrap(), &Value::I32(2020));
}

#[test]
fn test_get_property_vector_existing_property() {
    let arena = Bump::new();
    let vector = create_test_vector_with_props(&arena, 1, "Embedding", &[1.0], vec![("model", Value::String("gpt-4".to_string()))]);
    let tv = TraversalValue::Vector(vector);
    let prop = tv.get_property("model");
    assert!(prop.is_some());
    assert_eq!(prop.unwrap(), &Value::String("gpt-4".to_string()));
}

#[test]
fn test_get_property_vector_without_data_existing_property() {
    let arena = Bump::new();
    let vector = create_test_vector_without_data_with_props(&arena, 1, "Embedding", vec![("dim", Value::I32(768))]);
    let tv = TraversalValue::VectorNodeWithoutVectorData(vector);
    let prop = tv.get_property("dim");
    assert!(prop.is_some());
    assert_eq!(prop.unwrap(), &Value::I32(768));
}

#[test]
fn test_get_property_node_with_score_existing_property() {
    let arena = Bump::new();
    let node = create_test_node_with_props(&arena, 1, "Doc", vec![("title", Value::String("Report".to_string()))]);
    let tv = TraversalValue::NodeWithScore { node, score: 0.9 };
    let prop = tv.get_property("title");
    assert!(prop.is_some());
    assert_eq!(prop.unwrap(), &Value::String("Report".to_string()));
}

#[test]
fn test_get_property_empty_returns_none() {
    let tv: TraversalValue = TraversalValue::Empty;
    assert!(tv.get_property("anything").is_none());
}

#[test]
fn test_get_property_path_returns_none() {
    let tv: TraversalValue = TraversalValue::Path((vec![], vec![]));
    assert!(tv.get_property("anything").is_none());
}

#[test]
fn test_get_property_value_returns_none() {
    let tv = TraversalValue::Value(Value::String("test".to_string()));
    assert!(tv.get_property("anything").is_none());
}

// ============================================================================
// Hash Implementation Tests
// ============================================================================

#[test]
fn test_hash_same_node_ids_hash_equally() {
    let arena = Bump::new();
    let node1 = create_test_node(&arena, 12345, "Person");
    let node2 = create_test_node(&arena, 12345, "Different");
    let tv1 = TraversalValue::Node(node1);
    let tv2 = TraversalValue::Node(node2);
    assert_eq!(hash_value(&tv1), hash_value(&tv2));
}

#[test]
fn test_hash_different_node_ids_hash_differently() {
    let arena = Bump::new();
    let node1 = create_test_node(&arena, 12345, "Person");
    let node2 = create_test_node(&arena, 54321, "Person");
    let tv1 = TraversalValue::Node(node1);
    let tv2 = TraversalValue::Node(node2);
    assert_ne!(hash_value(&tv1), hash_value(&tv2));
}

#[test]
fn test_hash_edge_uses_id() {
    let arena = Bump::new();
    let edge1 = create_test_edge(&arena, 100, "KNOWS", 1, 2);
    let edge2 = create_test_edge(&arena, 100, "LIKES", 3, 4);
    let tv1 = TraversalValue::Edge(edge1);
    let tv2 = TraversalValue::Edge(edge2);
    assert_eq!(hash_value(&tv1), hash_value(&tv2));
}

#[test]
fn test_hash_vector_uses_id() {
    let arena = Bump::new();
    let vec1 = create_test_vector(&arena, 555, "Vec", &[1.0, 2.0]);
    let vec2 = create_test_vector(&arena, 555, "Vec", &[3.0, 4.0]);
    let tv1 = TraversalValue::Vector(vec1);
    let tv2 = TraversalValue::Vector(vec2);
    assert_eq!(hash_value(&tv1), hash_value(&tv2));
}

#[test]
fn test_hash_empty_is_consistent() {
    let tv1: TraversalValue = TraversalValue::Empty;
    let tv2: TraversalValue = TraversalValue::Empty;
    assert_eq!(hash_value(&tv1), hash_value(&tv2));
}

// ============================================================================
// PartialEq Implementation Tests
// ============================================================================

#[test]
fn test_eq_node_same_id() {
    let arena = Bump::new();
    let node1 = create_test_node(&arena, 100, "Person");
    let node2 = create_test_node(&arena, 100, "Different");
    let tv1 = TraversalValue::Node(node1);
    let tv2 = TraversalValue::Node(node2);
    assert_eq!(tv1, tv2);
}

#[test]
fn test_neq_node_different_id() {
    let arena = Bump::new();
    let node1 = create_test_node(&arena, 100, "Person");
    let node2 = create_test_node(&arena, 200, "Person");
    let tv1 = TraversalValue::Node(node1);
    let tv2 = TraversalValue::Node(node2);
    assert_ne!(tv1, tv2);
}

#[test]
fn test_eq_edge_same_id() {
    let arena = Bump::new();
    let edge1 = create_test_edge(&arena, 50, "KNOWS", 1, 2);
    let edge2 = create_test_edge(&arena, 50, "LIKES", 3, 4);
    let tv1 = TraversalValue::Edge(edge1);
    let tv2 = TraversalValue::Edge(edge2);
    assert_eq!(tv1, tv2);
}

#[test]
fn test_eq_vector_same_id() {
    let arena = Bump::new();
    let vec1 = create_test_vector(&arena, 300, "Vec", &[1.0]);
    let vec2 = create_test_vector(&arena, 300, "Vec", &[2.0]);
    let tv1 = TraversalValue::Vector(vec1);
    let tv2 = TraversalValue::Vector(vec2);
    assert_eq!(tv1, tv2);
}

#[test]
fn test_eq_vector_and_vector_without_data_same_id() {
    let arena = Bump::new();
    let vec1 = create_test_vector(&arena, 400, "Vec", &[1.0, 2.0]);
    let vec2 = create_test_vector_without_data(&arena, 400, "Vec");
    let tv1 = TraversalValue::Vector(vec1);
    let tv2 = TraversalValue::VectorNodeWithoutVectorData(vec2);
    assert_eq!(tv1, tv2);
}

#[test]
fn test_eq_node_with_score_same_id() {
    let arena = Bump::new();
    let node1 = create_test_node(&arena, 500, "Doc");
    let node2 = create_test_node(&arena, 500, "Doc");
    let tv1 = TraversalValue::NodeWithScore {
        node: node1,
        score: 0.5,
    };
    let tv2 = TraversalValue::NodeWithScore {
        node: node2,
        score: 0.9,
    };
    assert_eq!(tv1, tv2);
}

#[test]
fn test_eq_empty() {
    let tv1: TraversalValue = TraversalValue::Empty;
    let tv2: TraversalValue = TraversalValue::Empty;
    assert_eq!(tv1, tv2);
}

#[test]
fn test_neq_different_variants() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 100, "Test");
    let edge = create_test_edge(&arena, 100, "Test", 1, 2);
    let tv1 = TraversalValue::Node(node);
    let tv2 = TraversalValue::Edge(edge);
    assert_ne!(tv1, tv2);
}

#[test]
fn test_neq_node_and_empty() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 100, "Test");
    let tv1 = TraversalValue::Node(node);
    let tv2: TraversalValue = TraversalValue::Empty;
    assert_ne!(tv1, tv2);
}

// ============================================================================
// label_arena() Method Tests
// ============================================================================

#[test]
fn test_label_arena_node() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "MyLabel");
    let tv = TraversalValue::Node(node);
    assert_eq!(tv.label_arena(), "MyLabel");
}

#[test]
fn test_label_arena_empty() {
    let tv: TraversalValue = TraversalValue::Empty;
    assert_eq!(tv.label_arena(), "");
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn test_node_with_max_u128_id() {
    let arena = Bump::new();
    let node = create_test_node(&arena, u128::MAX, "MaxNode");
    let tv = TraversalValue::Node(node);
    assert_eq!(tv.id(), u128::MAX);
}

#[test]
fn test_node_with_zero_id() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 0, "ZeroNode");
    let tv = TraversalValue::Node(node);
    assert_eq!(tv.id(), 0);
}

#[test]
fn test_vector_with_empty_data() {
    let arena = Bump::new();
    let vector = create_test_vector(&arena, 1, "EmptyVec", &[]);
    let tv = TraversalValue::Vector(vector);
    assert!(tv.data().is_empty());
}

#[test]
fn test_vector_with_large_data() {
    let arena = Bump::new();
    let large_data: Vec<f64> = (0..1024).map(|i| i as f64).collect();
    let vector = create_test_vector(&arena, 1, "LargeVec", &large_data);
    let tv = TraversalValue::Vector(vector);
    assert_eq!(tv.data().len(), 1024);
}

#[test]
fn test_node_with_score_zero_score() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Test");
    let tv = TraversalValue::NodeWithScore { node, score: 0.0 };
    assert!((tv.score() - 0.0).abs() < f64::EPSILON);
}

#[test]
fn test_node_with_score_negative_score() {
    let arena = Bump::new();
    let node = create_test_node(&arena, 1, "Test");
    let tv = TraversalValue::NodeWithScore { node, score: -1.5 };
    assert!((tv.score() - (-1.5)).abs() < f64::EPSILON);
}

#[test]
fn test_path_with_nodes_and_edges() {
    let arena = Bump::new();
    let node1 = create_test_node(&arena, 1, "A");
    let node2 = create_test_node(&arena, 2, "B");
    let edge = create_test_edge(&arena, 1, "LINK", 1, 2);
    let tv = TraversalValue::Path((vec![node1, node2], vec![edge]));
    // Path returns 0 for id()
    assert_eq!(tv.id(), 0);
    assert_eq!(tv.label(), "");
}
