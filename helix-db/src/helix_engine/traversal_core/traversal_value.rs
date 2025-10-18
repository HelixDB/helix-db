use crate::{
    helix_engine::{
        types::GraphError,
        vector_core::{vector::HVector, vector_without_data::VectorWithoutData},
    },
    protocol::value::Value,
    utils::{
        count::Count,
        filterable::Filterable,
        items::{Edge, Node},
    },
};
use std::{borrow::Cow, collections::HashMap, hash::Hash};

pub type Variable<'arena> = Cow<'arena, TraversalValue<'arena>>;

#[derive(Clone, Debug)]
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
    Count(Count),
    /// A path between two nodes in the graph
    Path((Vec<Node<'arena>>, Vec<Edge<'arena>>)),
    /// A value in the graph
    Value(Value),
    /// An empty traversal value
    Empty,
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

impl<'a> IntoIterator for TraversalValue<'a> {
    type Item = TraversalValue<'a>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        vec![self].into_iter()
    }
}

pub enum TraversableType {
    Value,
    Vec,
}

/// A trait for all traversable values in the graph
///
/// This trait is used to define the common methods for all traversable values in the graph so we don't need to write match statements to access id's and properties every time.
pub trait Traversable {
    fn id(&self) -> u128;
    fn label(&self) -> String;
    fn check_property(&self, prop: &str) -> Result<Cow<'_, Value>, GraphError>;
    fn uuid(&self) -> String;
    fn traversal_type(&self) -> TraversableType;
    fn get_properties(&self) -> &Option<HashMap<String, Value>>;
}

impl Traversable for TraversalValue<'_> {
    fn id(&self) -> u128 {
        match self {
            TraversalValue::Node(node) => node.id,
            TraversalValue::Edge(edge) => edge.id,
            TraversalValue::Vector(vector) => vector.id,
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.id,
            TraversalValue::Value(_) => unreachable!(),
            TraversalValue::Empty => 0,
            t => {
                println!("invalid traversal value {t:?}");
                panic!("Invalid traversal value")
            }
        }
    }

    fn traversal_type(&self) -> TraversableType {
        TraversableType::Value
    }

    fn uuid(&self) -> String {
        match self {
            TraversalValue::Node(node) => uuid::Uuid::from_u128(node.id).to_string(),
            TraversalValue::Edge(edge) => uuid::Uuid::from_u128(edge.id).to_string(),
            TraversalValue::Vector(vector) => uuid::Uuid::from_u128(vector.id).to_string(),
            TraversalValue::VectorNodeWithoutVectorData(vector) => {
                uuid::Uuid::from_u128(vector.id).to_string()
            }
            _ => panic!("Invalid traversal value"),
        }
    }

    fn label(&self) -> String {
        match self {
            TraversalValue::Node(node) => node.label.to_string(),
            TraversalValue::Edge(edge) => edge.label.to_string(),
            _ => panic!("Invalid traversal value"),
        }
    }

    fn check_property(&self, prop: &str) -> Result<Cow<'_, Value>, GraphError> {
        match self {
            TraversalValue::Node(node) => node.check_property(prop),
            TraversalValue::Edge(edge) => edge.check_property(prop),
            TraversalValue::Vector(vector) => vector.check_property(prop),
            TraversalValue::VectorNodeWithoutVectorData(vector) => vector.check_property(prop),
            _ => Err(GraphError::ConversionError(
                "Invalid traversal value".to_string(),
            )),
        }
    }

    fn get_properties(&self) -> &Option<HashMap<String, Value>> {
        match self {
            TraversalValue::Node(node) => &node.properties,
            TraversalValue::Edge(edge) => &edge.properties,
            TraversalValue::Vector(vector) => &vector.properties,
            TraversalValue::VectorNodeWithoutVectorData(vector) => &Some(vector.properties),
            _ => &None,
        }
    }
}

impl Traversable for Vec<TraversalValue<'_>> {
    fn id(&self) -> u128 {
        if self.is_empty() {
            return 0;
        }
        self[0].id()
    }

    fn label(&self) -> String {
        if self.is_empty() {
            return "".to_string();
        }
        self[0].label()
    }

    fn check_property(&self, prop: &str) -> Result<Cow<'_, Value>, GraphError> {
        if self.is_empty() {
            return Err(GraphError::ConversionError(
                "Invalid traversal value".to_string(),
            ));
        }
        self[0].check_property(prop)
    }

    fn get_properties(&self) -> &Option<HashMap<String, Value>> {
        if self.is_empty() {
            return &None;
        }
        self[0].get_properties()
    }

    fn uuid(&self) -> String {
        if self.is_empty() {
            return "".to_string();
        }
        self[0].uuid()
    }

    fn traversal_type(&self) -> TraversableType {
        TraversableType::Vec
    }
}

pub trait IntoTraversalValues<'a> {
    fn into(self) -> Vec<TraversalValue<'a>>;
}

impl<'a> IntoTraversalValues<'a> for Vec<TraversalValue<'a>> {
    fn into(self) -> Vec<TraversalValue<'a>> {
        self
    }
}

impl<'a> IntoTraversalValues<'a> for TraversalValue<'a> {
    fn into(self) -> Vec<TraversalValue<'a>> {
        vec![self]
    }
}
