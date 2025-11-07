use serde::Serialize;

use crate::{
    helix_engine::vector_core::{vector::HVector, vector_without_data::VectorWithoutData},
    protocol::value::Value,
    utils::items::{Edge, Node},
};
use std::{borrow::Cow, hash::Hash};

pub type Variable<'arena> = Cow<'arena, TraversalValue<'arena>>;

#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum TraversalValue<'arena> {
    /// A node in the graph
    Node(Node<'arena>),
    /// An edge in the graph
    Edge(Edge<'arena>),
    /// A vector in the graph
    Vector(HVector<'arena>),
    /// Vector node without vector data
    VectorNodeWithoutVectorData(VectorWithoutData<'arena>),
    /// A count of the number of items
    /// A path between two nodes in the graph
    Path((Vec<Node<'arena>>, Vec<Edge<'arena>>)),
    /// A value in the graph
    Value(Value),

    /// Item With Score
    NodeWithScore { node: Node<'arena>, score: f64 },
    /// An empty traversal value
    Empty,
}

impl<'arena> TraversalValue<'arena> {
    pub fn id(&self) -> u128 {
        match self {
            TraversalValue::Node(node) => node.id,
            TraversalValue::Edge(edge) => edge.id,
            TraversalValue::Vector(vector) => vector.id,
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.id,
            TraversalValue::Empty => 0,
            _ => 0,
        }
    }

    pub fn label(&self) -> &'arena str {
        match self {
            TraversalValue::Node(node) => node.label,
            TraversalValue::Edge(edge) => edge.label,
            TraversalValue::Vector(vector) => vector.label,
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.label,
            TraversalValue::Empty => "",
            _ => "",
        }
    }

    pub fn from_node(&self) -> u128 {
        match self {
            TraversalValue::Edge(edge) => edge.from_node,
            _ => unimplemented!(),
        }
    }

    pub fn to_node(&self) -> u128 {
        match self {
            TraversalValue::Edge(edge) => edge.to_node,
            _ => unimplemented!(),
        }
    }

    pub fn data(&self) -> &'arena [f64] {
        match self {
            TraversalValue::Vector(vector) => vector.data,
            _ => unimplemented!(),
        }
    }

    pub fn score(&self) -> f64 {
        match self {
            TraversalValue::Vector(vector) => vector.score(),
            _ => unimplemented!(),
        }
    }

    pub fn label_arena(&self) -> &'arena str {
        match self {
            TraversalValue::Node(node) => node.label,
            TraversalValue::Edge(edge) => edge.label,
            TraversalValue::Vector(vector) => vector.label,
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.label,
            TraversalValue::Empty => "",
            _ => "",
        }
    }

    pub fn get_property(&self, property: &str) -> Option<&'arena Value> {
        match self {
            TraversalValue::Node(node) => node.get_property(property),
            TraversalValue::Edge(edge) => edge.get_property(property),
            TraversalValue::Vector(vector) => vector.get_property(property),
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.get_property(property),
            TraversalValue::Empty => None,
            _ => None,
        }
    }
}

impl Hash for TraversalValue<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            TraversalValue::Node(node) => node.id.hash(state),
            TraversalValue::Edge(edge) => edge.id.hash(state),
            TraversalValue::Vector(vector) => vector.id.hash(state),
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.id.hash(state),
            TraversalValue::Empty => state.write_u8(0),
            _ => state.write_u8(0),
        }
    }
}

impl Eq for TraversalValue<'_> {}
impl PartialEq for TraversalValue<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TraversalValue::Node(node1), TraversalValue::Node(node2)) => node1.id == node2.id,
            (TraversalValue::Edge(edge1), TraversalValue::Edge(edge2)) => edge1.id == edge2.id,
            (TraversalValue::Vector(vector1), TraversalValue::Vector(vector2)) => {
                vector1.id() == vector2.id()
            }
            (
                TraversalValue::VectorNodeWithoutVectorData(vector1),
                TraversalValue::VectorNodeWithoutVectorData(vector2),
            ) => vector1.id() == vector2.id(),
            (
                TraversalValue::Vector(vector1),
                TraversalValue::VectorNodeWithoutVectorData(vector2),
            ) => vector1.id() == vector2.id(),
            (
                TraversalValue::VectorNodeWithoutVectorData(vector1),
                TraversalValue::Vector(vector2),
            ) => vector1.id() == vector2.id(),
            (TraversalValue::Empty, TraversalValue::Empty) => true,
            _ => false,
        }
    }
}
