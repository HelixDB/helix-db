//! Node and Edge types for the graph.
//!
//! Nodes are the main entities in the graph and edges are the connections between them.
//!
//! Nodes and edges are serialised without enum variant names in JSON format.

use crate::helix_engine::storage_core::Arena;
use crate::protocol::custom_serde::edge_serde::EdgeDeSeed;
use crate::protocol::custom_serde::node_serde::NodeDeSeed;
use crate::protocol::value::Value;
use crate::utils::id::uuid_str_from_buf;
use crate::utils::properties::ImmutablePropertiesMap;
use bincode::Options;
use serde::ser::SerializeMap;
use std::cmp::Ordering;

/// A node in the graph containing an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Clone, Copy)]
pub struct Node<'arena> {
    /// The ID of the node.
    ///
    /// This is not serialized when stored as it is the key.
    pub id: u128,
    /// The label of the node.
    pub label: &'arena str,
    /// The version of the node.
    pub version: u8,
    /// The properties of the node.
    ///
    /// Properties are optional and can be None.
    /// Properties are serialised without enum variant names in JSON format.
    pub properties: Option<ImmutablePropertiesMap<'arena>>,
}

// Custom Serialize implementation to match old #[derive(Serialize)] behavior
// Bincode serializes #[derive(Serialize)] structs using serialize_struct internally
// which produces a compact format without length prefixes
// For JSON serialization, the id field is included, but for bincode it is skipped
impl<'arena> serde::Serialize for Node<'arena> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        // Check if this is a human-readable format (like JSON)
        if serializer.is_human_readable() {
            // Include id for JSON serialization
            let mut buffer = [0u8; 36];
            let mut state = serializer.serialize_map(Some(
                3 + self.properties.as_ref().map(|p| p.len()).unwrap_or(0),
            ))?;
            state.serialize_entry("id", uuid_str_from_buf(self.id, &mut buffer))?;
            state.serialize_entry("label", self.label)?;
            state.serialize_entry("version", &self.version)?;
            if let Some(properties) = &self.properties {
                for (key, value) in properties.iter() {
                    state.serialize_entry(key, value)?;
                }
            }
            state.end()
        } else {
            // Skip id for bincode serialization
            let mut state = serializer.serialize_struct("Node", 3)?;
            state.serialize_field("label", self.label)?;
            state.serialize_field("version", &self.version)?;
            state.serialize_field("properties", &self.properties)?;
            state.end()
        }
    }
}

impl<'arena> Node<'arena> {
    /// Gets property from node
    ///
    /// NOTE: the `'arena` lifetime which comes from the fact the node's ImmutablePropertiesMap
    #[inline(always)]
    pub fn get_property(&self, prop: &str) -> Option<&'arena Value> {
        self.properties.and_then(|value| value.get(prop))
    }

    /// Deserializes bytes into a node using a custom deserializer that allocates into the provided arena
    ///
    /// NOTE: in this method, fixint encoding is used
    #[inline(always)]
    pub fn from_bincode_bytes<'txn>(
        id: u128,
        bytes: &'txn [u8],
        arena: Arena<'arena>,
    ) -> bincode::Result<Self> {
        // Use fixint encoding to match bincode::serialize() behavior (8-byte lengths)
        // Allow trailing bytes since we manually control Option reading
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_seed(NodeDeSeed { arena, id }, bytes)
    }

    #[inline(always)]
    pub fn to_bincode_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
}

// Core trait implementations for Node
impl std::fmt::Display for Node<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {} }}",
            uuid::Uuid::from_u128(self.id),
            self.label,
        )
    }
}
impl std::fmt::Debug for Node<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \nid:{},\nlabel:{} }}",
            uuid::Uuid::from_u128(self.id),
            self.label,
        )
    }
}

impl PartialEq for Node<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Node<'_> {}
impl Ord for Node<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for Node<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An edge in the graph connecting two nodes with an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Clone, Copy)]
pub struct Edge<'arena> {
    /// The ID of the edge.
    ///
    /// This is not serialized when stored as it is the key.
    pub id: u128,
    /// The label of the edge.
    pub label: &'arena str,
    /// The version of the edge.
    pub version: u8,
    /// The ID of the from node.
    pub from_node: u128,
    /// The ID of the to node.
    pub to_node: u128,
    /// The properties of the edge.
    ///
    /// Properties are optional and can be None.
    /// Properties are serialised without enum variant names in JSON format.
    pub properties: Option<ImmutablePropertiesMap<'arena>>,
}

// Custom Serialize implementation to match old #[derive(Serialize)] behavior
// Bincode serializes #[derive(Serialize)] structs using serialize_struct internally
// which produces a compact format without length prefixes
// For JSON serialization, the id field is included, but for bincode it is skipped
impl<'arena> serde::Serialize for Edge<'arena> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        // Check if this is a human-readable format (like JSON)
        if serializer.is_human_readable() {
            // Include id for JSON serialization
            let mut buffer = [0u8; 36];
            let mut state = serializer.serialize_map(Some(
                5 + self.properties.as_ref().map(|p| p.len()).unwrap_or(0),
            ))?;
            state.serialize_entry("id", uuid_str_from_buf(self.id, &mut buffer))?;
            state.serialize_entry("label", self.label)?;
            state.serialize_entry("version", &self.version)?;
            state.serialize_entry("from_node", &self.from_node)?;
            state.serialize_entry("to_node", &self.to_node)?;
            if let Some(properties) = &self.properties {
                for (key, value) in properties.iter() {
                    state.serialize_entry(key, value)?;
                }
            }
            state.end()
        } else {
            // Skip id for bincode serialization
            let mut state = serializer.serialize_struct("Edge", 5)?;
            state.serialize_field("label", self.label)?;
            state.serialize_field("version", &self.version)?;
            state.serialize_field("from_node", &self.from_node)?;
            state.serialize_field("to_node", &self.to_node)?;
            state.serialize_field("properties", &self.properties)?;
            state.end()
        }
    }
}

impl<'arena> Edge<'arena> {
    /// Gets property from node
    ///
    /// NOTE: the `'arena` lifetime which comes from the fact the node's ImmutablePropertiesMap
    #[inline(always)]
    pub fn get_property(&self, prop: &str) -> Option<&'arena Value> {
        self.properties.as_ref().and_then(|value| value.get(prop))
    }

    /// Deserializes bytes into an edge using a custom deserializer that allocates into the provided arena
    ///
    /// NOTE: in this method, fixint encoding is used
    #[inline(always)]
    pub fn from_bincode_bytes<'txn>(
        id: u128,
        bytes: &'txn [u8],
        arena: Arena<'arena>,
    ) -> bincode::Result<Self> {
        // Use fixint encoding to match bincode::serialize() behavior (8-byte lengths)
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_seed(EdgeDeSeed { arena, id }, bytes)
    }

    #[inline(always)]
    pub fn to_bincode_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
}

// Core trait implementations for Edge
impl std::fmt::Display for Edge<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, from_node: {}, to_node: {}}}",
            uuid::Uuid::from_u128(self.id),
            self.label,
            uuid::Uuid::from_u128(self.from_node),
            uuid::Uuid::from_u128(self.to_node),
        )
    }
}
impl std::fmt::Debug for Edge<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \nid: {},\nlabel: {},\nfrom_node: {},\nto_node: {}}}",
            uuid::Uuid::from_u128(self.id),
            self.label,
            uuid::Uuid::from_u128(self.from_node),
            uuid::Uuid::from_u128(self.to_node),
        )
    }
}
impl Eq for Edge<'_> {}
impl PartialEq for Edge<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Ord for Edge<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for Edge<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bumpalo::Bump;

    use super::*;
    use crate::protocol::value::Value as PropsValue;

    // Helper function to create a test node
    fn create_test_node<'arena>(
        id: u128,
        label: &'arena str,
        props: Option<ImmutablePropertiesMap<'arena>>,
    ) -> Node<'arena> {
        Node {
            id,
            label,
            version: 0,
            properties: props,
        }
    }

    // Helper function to create a test edge
    fn create_test_edge<'arena>(
        id: u128,
        label: &'arena str,
        from: u128,
        to: u128,
        props: Option<ImmutablePropertiesMap<'arena>>,
    ) -> Edge<'arena> {
        Edge {
            id,
            label,
            version: 0,
            from_node: from,
            to_node: to,
            properties: props,
        }
    }

    // Helper function to create ImmutablePropertiesMap from a HashMap
    fn create_props_map<'arena>(
        props: HashMap<&'arena str, PropsValue>,
        arena: Arena<'arena>,
    ) -> ImmutablePropertiesMap<'arena> {
        let len = props.len();
        ImmutablePropertiesMap::new(len, props.into_iter(), arena)
    }

    // Basic Node tests

    #[test]
    fn test_node_creation_basic() {
        let arena = Bump::new();
        let node = create_test_node(12345, arena.alloc_str("person"), None);

        assert_eq!(node.id, 12345);
        assert_eq!(node.label, "person");
        assert_eq!(node.version, 0);
        assert!(node.properties.is_none());
    }

    #[test]
    fn test_node_with_properties() {
        let arena = Bump::new();
        let mut props = HashMap::new();
        let name_key: &str = arena.alloc_str("name");
        let age_key: &str = arena.alloc_str("age");
        props.insert(name_key, PropsValue::String("John".to_string()));
        props.insert(age_key, PropsValue::I64(20));

        let props_map = create_props_map(props, &arena);
        let label: &str = arena.alloc_str("person");
        let node = create_test_node(456, label, Some(props_map));

        assert_eq!(node.id, 456);
        assert_eq!(node.label, "person");
        assert!(node.properties.is_some());

        let properties = node.properties.as_ref().unwrap();
        assert_eq!(properties.len(), 2);
        assert!(properties.get("name").is_some());
        assert!(properties.get("age").is_some());
    }

    #[test]
    fn test_node_get_property() {
        let arena = Bump::new();
        let mut props = HashMap::new();
        let name_key: &str = arena.alloc_str("name");
        props.insert(name_key, PropsValue::String("John".to_string()));

        let props_map = create_props_map(props, &arena);
        let label: &str = arena.alloc_str("person");
        let node = create_test_node(789, label, Some(props_map));

        // Note: get_property returns Option<&Value> where Value is from protocol, not properties
        // This is a type mismatch in the current implementation
        assert!(node.get_property("name").is_some());
        assert_eq!(node.get_property("nonexistent"), None);
    }

    // Basic Edge tests

    #[test]
    fn test_edge_creation_basic() {
        let arena = Bump::new();
        let edge = create_test_edge(1, arena.alloc_str("knows"), 100, 200, None);

        assert_eq!(edge.id, 1);
        assert_eq!(edge.label, "knows");
        assert_eq!(edge.from_node, 100);
        assert_eq!(edge.to_node, 200);
        assert_eq!(edge.version, 0);
        assert!(edge.properties.is_none());
    }

    #[test]
    fn test_edge_with_properties() {
        let arena = Bump::new();
        let mut props = HashMap::new();
        let weight_key: &str = arena.alloc_str("weight");
        let since_key: &str = arena.alloc_str("since");
        props.insert(weight_key, PropsValue::F64(1.0));
        props.insert(since_key, PropsValue::I64(2020));

        let props_map = create_props_map(props, &arena);
        let label: &str = arena.alloc_str("knows");
        let edge = create_test_edge(2, label, 300, 400, Some(props_map));

        assert_eq!(edge.id, 2);
        assert_eq!(edge.from_node, 300);
        assert_eq!(edge.to_node, 400);

        let properties = edge.properties.as_ref().unwrap();
        assert_eq!(properties.len(), 2);
        assert!(properties.get("weight").is_some());
    }

    #[test]
    fn test_edge_get_property() {
        let arena = Bump::new();
        let mut props = HashMap::new();
        let type_key: &str = arena.alloc_str("type");
        props.insert(type_key, PropsValue::String("friend".to_string()));

        let props_map = create_props_map(props, &arena);
        let label: &str = arena.alloc_str("knows");
        let edge = create_test_edge(3, label, 500, 600, Some(props_map));

        assert!(edge.get_property("type").is_some());
        assert_eq!(edge.get_property("nonexistent"), None);
    }

    #[test]
    fn test_edge_self_loop() {
        let arena = Bump::new();
        let edge = create_test_edge(4, arena.alloc_str("self_reference"), 700, 700, None);

        assert_eq!(edge.from_node, edge.to_node);
        assert_eq!(edge.from_node, 700);
    }

    #[test]
    fn test_edge_large_node_ids() {
        let arena = Bump::new();
        let max_id = u128::MAX;
        let edge = create_test_edge(6, arena.alloc_str("test"), max_id - 1, max_id, None);

        assert_eq!(edge.from_node, max_id - 1);
        assert_eq!(edge.to_node, max_id);
    }

    // Test Display and Debug implementations

    #[test]
    fn test_node_display() {
        let arena = Bump::new();
        let mut props = HashMap::new();
        let key: &str = arena.alloc_str("key");
        props.insert(key, PropsValue::String("value".to_string()));
        let props_map = create_props_map(props, &arena);

        let label: &str = arena.alloc_str("test");
        let node = create_test_node(123456789, label, Some(props_map));

        let display = format!("{}", node);
        assert!(display.contains("test"));
        assert!(display.contains("id"));
    }

    #[test]
    fn test_edge_display() {
        let arena = Bump::new();
        let edge = create_test_edge(123, arena.alloc_str("knows"), 100, 200, None);

        let display = format!("{}", edge);
        assert!(display.contains("knows"));
        assert!(display.contains("from_node"));
        assert!(display.contains("to_node"));
    }

    // Test ordering implementations

    #[test]
    fn test_node_ordering() {
        let arena = Bump::new();
        let node1 = create_test_node(100, arena.alloc_str("a"), None);
        let node2 = create_test_node(200, arena.alloc_str("b"), None);
        let node3 = create_test_node(100, arena.alloc_str("a"), None); // Same ID and label

        assert!(node1 < node2);
        assert!(node2 > node1);
        // Nodes with same ID are equal (PartialEq only compares ID)
        assert_eq!(node1, node3);
        // Nodes with same ID but different label are still equal (by ID)
        let node4 = create_test_node(100, arena.alloc_str("different"), None);
        assert_eq!(node1, node4);
        // Ordering only considers ID
        assert_eq!(node1.cmp(&node4), Ordering::Equal);
    }

    #[test]
    fn test_edge_ordering() {
        let arena = Bump::new();
        let edge1 = create_test_edge(100, arena.alloc_str("a"), 1, 2, None);
        let edge2 = create_test_edge(200, arena.alloc_str("b"), 3, 4, None);
        let edge3 = create_test_edge(100, arena.alloc_str("a"), 1, 2, None); // Same ID

        assert!(edge1 < edge2);
        assert!(edge2 > edge1);
        // Edges with same ID are equal (PartialEq only compares ID)
        assert_eq!(edge1, edge3);
        // Edges with same ID but different data are still equal (by ID)
        let edge4 = create_test_edge(100, arena.alloc_str("different"), 5, 6, None);
        assert_eq!(edge1, edge4);
        // Ordering only considers ID
        assert_eq!(edge1.cmp(&edge4), Ordering::Equal);
    }
}
