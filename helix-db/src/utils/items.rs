//! Node and Edge types for the graph.
//!
//! Nodes are the main entities in the graph and edges are the connections between them.
//!
//! Nodes and edges are serialised without enum variant names in JSON format.

use crate::helix_engine::types::GraphError;
use crate::protocol::value::Value;
use sonic_rs::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap};

/// A node in the graph containing an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    /// The ID of the node.
    ///
    /// This is not serialized when stored as it is the key.
    #[serde(skip)]
    pub id: u128,
    /// The label of the node.
    pub label: String,
    /// The version of the node.
    pub version: u8,
    /// The properties of the node.
    ///
    /// Properties are optional and can be None.
    /// Properties are serialised without enum variant names in JSON format.
    #[serde(default)]
    pub properties: Option<HashMap<String, Value>>,
}

impl Node {
    /// The number of properties in a node.
    ///
    /// This is used as a constant in the return value mixin methods.
    pub const NUM_PROPERTIES: usize = 2;

    /// Decodes a node from a byte slice.
    ///
    /// Takes ID as the ID is not serialized when stored as it is the key.
    /// Uses the known ID (either from the query or the key in an LMDB iterator) to construct a new node.
    pub fn decode_node(bytes: &[u8], id: u128) -> Result<Node, GraphError> {
        match bincode::deserialize::<Node>(bytes) {
            Ok(node) => Ok(Node { id, ..node }),
            Err(e) => Err(GraphError::ConversionError(format!(
                "Error deserializing node: {e}"
            ))),
        }
    }

    /// Encodes a node into a byte slice
    ///
    /// This skips the ID and if the properties are None, it skips the properties.
    pub fn encode_node(&self) -> Result<Vec<u8>, GraphError> {
        bincode::serialize(&self)
            .map_err(|e| GraphError::ConversionError(format!("Error serializing node: {e}")))
    }
}

// Core trait implementations for Node
impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, properties: {:?} }}",
            uuid::Uuid::from_u128(self.id),
            self.label,
            self.properties
        )
    }
}
impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \nid:{},\nlabel:{},\nproperties:{:#?} }}",
            uuid::Uuid::from_u128(self.id),
            self.label,
            self.properties
        )
    }
}
impl Eq for Node {}
impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An edge in the graph connecting two nodes with an ID, label, and property map.
/// Properties are serialised without enum variant names in JSON format.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Edge {
    /// The ID of the edge.
    ///
    /// This is not serialized when stored as it is the key.
    #[serde(skip)]
    pub id: u128,
    /// The label of the edge.
    pub label: String,
    pub version: u8,
    /// The ID of the from node.
    pub from_node: u128,
    /// The ID of the to node.
    pub to_node: u128,
    /// The properties of the edge.
    ///
    /// Properties are optional and can be None.
    /// Properties are serialised without enum variant names in JSON format.
    #[serde(default)]
    pub properties: Option<HashMap<String, Value>>,
}

impl Edge {
    /// The number of properties in an edge.
    ///
    /// This is used as a constant in the return value mixin methods.
    pub const NUM_PROPERTIES: usize = 4;

    /// Decodes an edge from a byte slice.
    ///
    /// Takes ID as the ID is not serialized when stored as it is the key.
    /// Uses the known ID (either from the query or the key in an LMDB iterator) to construct a new edge.
    pub fn decode_edge(bytes: &[u8], id: u128) -> Result<Edge, GraphError> {
        match bincode::deserialize::<Edge>(bytes) {
            Ok(edge) => Ok(Edge { id, ..edge }),
            Err(e) => Err(GraphError::ConversionError(format!(
                "Error deserializing edge: {e}"
            ))),
        }
    }

    /// Encodes an edge into a byte slice
    ///
    /// This skips the ID and if the properties are None, it skips the properties.
    pub fn encode_edge(&self) -> Result<Vec<u8>, GraphError> {
        bincode::serialize(self)
            .map_err(|e| GraphError::ConversionError(format!("Error serializing edge: {e}")))
    }
}

// Core trait implementations for Edge
impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ id: {}, label: {}, from_node: {}, to_node: {}, properties: {:?} }}",
            uuid::Uuid::from_u128(self.id),
            self.label,
            uuid::Uuid::from_u128(self.from_node),
            uuid::Uuid::from_u128(self.to_node),
            self.properties
        )
    }
}
impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \nid: {},\nlabel: {},\nfrom_node: {},\nto_node: {},\nproperties: {:#?} }}",
            uuid::Uuid::from_u128(self.id),
            self.label,
            uuid::Uuid::from_u128(self.from_node),
            uuid::Uuid::from_u128(self.to_node),
            self.properties
        )
    }
}
impl Eq for Edge {}
impl Ord for Edge {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for Edge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
