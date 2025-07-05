use crate::{
    helix_engine::{types::GraphError, vector_core::vector::HVector},
    protocol::{
        count::Count,
        filterable::Filterable,
        items::{Edge, Node},
        value::Value,
    },
};
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum TraversalVal {
    /// A node in the graph
    Node(Node),
    /// An edge in the graph
    Edge(Edge),
    /// A vector in the graph
    Vector(HVector),
    /// A count of the number of items
    Count(Count),
    /// A path between two nodes in the graph
    Path((Vec<Node>, Vec<Edge>)),
    /// A value in the graph
    Value(Value),
    /// An empty traversal value
    Empty,
}

impl Hash for TraversalVal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            TraversalVal::Node(node) => node.id.hash(state),
            TraversalVal::Edge(edge) => edge.id.hash(state),
            TraversalVal::Vector(vector) => vector.id.hash(state),
            TraversalVal::Empty => state.write_u8(0),
            _ => state.write_u8(0),
        }
    }
}

impl Eq for TraversalVal {}
impl PartialEq for TraversalVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TraversalVal::Node(node1), TraversalVal::Node(node2)) => node1.id == node2.id,
            (TraversalVal::Edge(edge1), TraversalVal::Edge(edge2)) => edge1.id == edge2.id,
            (TraversalVal::Vector(vector1), TraversalVal::Vector(vector2)) => {
                vector1.id() == vector2.id()
            }
            (TraversalVal::Empty, TraversalVal::Empty) => true,
            _ => false,
        }
    }
}

/// A trait for all traversable values in the graph
///
/// This trait is used to define the common methods for all traversable values in the graph so we don't need to write match statements to access id's and properties every time.
pub trait Traversable {
    fn id(&self) -> u128;
    fn label(&self) -> String;
    fn check_property(&self, prop: &str) -> Result<&Value, GraphError>;
    fn uuid(&self) -> String;
}

impl Traversable for TraversalVal {
    fn id(&self) -> u128 {
        match self {
            TraversalVal::Node(node) => node.id,
            TraversalVal::Edge(edge) => edge.id,

            TraversalVal::Vector(vector) => vector.id,
            TraversalVal::Value(_) => unreachable!(),
            TraversalVal::Empty => 0,
            t => {
                println!("invalid traversal value {:?}", t);
                panic!("Invalid traversal value")
            }
        }
    }

    fn uuid(&self) -> String {
        match self {
            TraversalVal::Node(node) => uuid::Uuid::from_u128(node.id).to_string(),
            TraversalVal::Edge(edge) => uuid::Uuid::from_u128(edge.id).to_string(),
            TraversalVal::Vector(vector) => uuid::Uuid::from_u128(vector.id).to_string(),
            _ => panic!("Invalid traversal value"),
        }
    }

    fn label(&self) -> String {
        match self {
            TraversalVal::Node(node) => node.label.clone(),
            TraversalVal::Edge(edge) => edge.label.clone(),
            _ => panic!("Invalid traversal value"),
        }
    }

    fn check_property(&self, prop: &str) -> Result<&Value, GraphError> {
        match self {
            TraversalVal::Node(node) => node.check_property(prop),
            TraversalVal::Edge(edge) => edge.check_property(prop),
            TraversalVal::Vector(vector) => vector.check_property(prop),
            _ => Err(GraphError::ConversionError(format!(
                "Invalid traversal value"
            ))),
        }
    }
}

impl Traversable for Vec<TraversalVal> {
    fn id(&self) -> u128 {
        if self.is_empty() {
            println!("retting zero line 118!");
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

    fn check_property(&self, prop: &str) -> Result<&Value, GraphError> {
        if self.is_empty() {
            return Err(GraphError::ConversionError(format!(
                "Invalid traversal value"
            )));
        }
        self[0].check_property(prop)
    }

    fn uuid(&self) -> String {
        if self.is_empty() {
            return "".to_string();
        }
        self[0].uuid()
    }
}

pub trait IntoTraversalValues {
    fn into(self) -> Vec<TraversalVal>;
}

impl IntoTraversalValues for Vec<TraversalVal> {
    fn into(self) -> Vec<TraversalVal> {
        self
    }
}

impl IntoTraversalValues for TraversalVal {
    fn into(self) -> Vec<TraversalVal> {
        vec![self]
    }
}
