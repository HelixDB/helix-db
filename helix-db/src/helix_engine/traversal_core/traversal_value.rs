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
            Self::Node(node) => node.id,
            Self::Edge(edge) => edge.id,
            Self::Vector(vector) => vector.id,
            Self::VectorNodeWithoutVectorData(vector) => vector.id,
            Self::Empty => 0,
            _ => 0,
        }
    }

    pub fn label(&self) -> &'arena str {
        match self {
            Self::Node(node) => node.label,
            Self::Edge(edge) => edge.label,
            Self::Vector(vector) => vector.label,
            Self::VectorNodeWithoutVectorData(vector) => vector.label,
            Self::Empty => "",
            _ => "",
        }
    }

    pub fn from_node(&self) -> u128 {
        match self {
            Self::Edge(edge) => edge.from_node,
            _ => unimplemented!(),
        }
    }

    pub fn to_node(&self) -> u128 {
        match self {
            Self::Edge(edge) => edge.to_node,
            _ => unimplemented!(),
        }
    }

    pub fn data(&self) -> &'arena [f64] {
        match self {
            Self::Vector(vector) => vector.data,
            Self::VectorNodeWithoutVectorData(_) => &[],
            _ => unimplemented!(),
        }
    }

    pub fn score(&self) -> f64 {
        match self {
            Self::Vector(vector) => vector.score(),
            Self::VectorNodeWithoutVectorData(_) => 2f64,
            _ => unimplemented!(),
        }
    }

    pub fn label_arena(&self) -> &'arena str {
        match self {
            Self::Node(node) => node.label,
            Self::Edge(edge) => edge.label,
            Self::Vector(vector) => vector.label,
            Self::VectorNodeWithoutVectorData(vector) => vector.label,
            Self::Empty => "",
            _ => "",
        }
    }

    pub fn get_property(&self, property: &str) -> Option<&'arena Value> {
        match self {
            Self::Node(node) => node.get_property(property),
            Self::Edge(edge) => edge.get_property(property),
            Self::Vector(vector) => vector.get_property(property),
            Self::VectorNodeWithoutVectorData(vector) => vector.get_property(property),
            Self::Empty => None,
            _ => None,
        }
    }
}

impl Hash for TraversalValue<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Node(node) => node.id.hash(state),
            Self::Edge(edge) => edge.id.hash(state),
            Self::Vector(vector) => vector.id.hash(state),
            Self::VectorNodeWithoutVectorData(vector) => vector.id.hash(state),
            Self::Empty => state.write_u8(0),
            _ => state.write_u8(0),
        }
    }
}

impl Eq for TraversalValue<'_> {}
impl PartialEq for TraversalValue<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Node(node1), Self::Node(node2)) => node1.id == node2.id,
            (Self::Edge(edge1), Self::Edge(edge2)) => edge1.id == edge2.id,
            (Self::Vector(vector1), Self::Vector(vector2)) => {
                vector1.id() == vector2.id()
            }
            (
                Self::VectorNodeWithoutVectorData(vector1),
                Self::VectorNodeWithoutVectorData(vector2),
            ) => vector1.id() == vector2.id(),
            (
                Self::Vector(vector1),
                Self::VectorNodeWithoutVectorData(vector2),
            ) => vector1.id() == vector2.id(),
            (
                Self::VectorNodeWithoutVectorData(vector1),
                Self::Vector(vector2),
            ) => vector1.id() == vector2.id(),
            (Self::Empty, Self::Empty) => true,
            _ => false,
        }
    }
}
