use crate::helix_engine::vector_core::vector::HVector;

use super::{count::Count, items::Edge, items::Node, value::Value};
use serde::Serializer;
use sonic_rs::{Deserialize, Serialize};

#[derive(Deserialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum TraversalValue {
    Empty,
    Count(Count),
    NodeArray(Vec<Node>),
    EdgeArray(Vec<Edge>),
    ValueArray(Vec<(String, Value)>),
    Paths(Vec<(Vec<Node>, Vec<Edge>)>),
    VectorArray(Vec<HVector>),
}

impl FromIterator<TraversalValue> for TraversalValue {
    fn from_iter<T: IntoIterator<Item = TraversalValue>>(iter: T) -> Self {
        let mut nodes = Vec::with_capacity(10);
        let mut edges = Vec::with_capacity(10);
        let mut values = Vec::with_capacity(10);
        let mut paths = Vec::with_capacity(10);
        let mut vectors = Vec::with_capacity(10);
        for value in iter {
            match value {
                TraversalValue::Count(count) => return TraversalValue::Count(count),
                TraversalValue::NodeArray(mut node_vec) => nodes.append(&mut node_vec),
                TraversalValue::EdgeArray(mut edge_vec) => edges.append(&mut edge_vec),
                TraversalValue::ValueArray(mut value_vec) => values.append(&mut value_vec),
                TraversalValue::Paths(mut path_vecs) => paths.append(&mut path_vecs),
                TraversalValue::VectorArray(mut vector_vec) => vectors.append(&mut vector_vec),
                TraversalValue::Empty => (),
            }
        }

        if !nodes.is_empty() {
            TraversalValue::NodeArray(nodes)
        } else if !edges.is_empty() {
            TraversalValue::EdgeArray(edges)
        } else if !values.is_empty() {
            TraversalValue::ValueArray(values)
        } else {
            TraversalValue::Empty
        }
    }
}

// Implementation for owned Edge
impl From<Edge> for TraversalValue {
    fn from(edge: Edge) -> Self {
        TraversalValue::EdgeArray(vec![edge])
    }
}

// Implementation for Edge reference
impl From<& Edge> for TraversalValue {
    fn from(edge: & Edge) -> Self {
        TraversalValue::EdgeArray(vec![edge.clone()])
    }
}

// Implementation for Node (unchanged as Node doesn't have lifetime parameter)
impl From<Node> for TraversalValue {
    fn from(node: Node) -> Self {
        TraversalValue::NodeArray(vec![node])
    }
}

impl From<&Node> for TraversalValue {
    fn from(node: &Node) -> Self {
        TraversalValue::NodeArray(vec![node.clone()])
    }
}

impl From<(String, Value)> for TraversalValue {
    fn from(value: (String, Value)) -> Self {
        TraversalValue::ValueArray(vec![value])
    }
}

impl std::fmt::Debug for TraversalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraversalValue::Count(count) => write!(f, "Count: {:?}", count.value()),
            TraversalValue::Empty => write!(f, "[]"),
            TraversalValue::NodeArray(nodes) => nodes.fmt(f),
            TraversalValue::EdgeArray(edges) => edges.fmt(f),
            TraversalValue::ValueArray(values) => values.fmt(f),
            TraversalValue::Paths(paths) => paths.fmt(f),
            TraversalValue::VectorArray(vectors) => vectors.fmt(f),
        }
    }
}

impl Serialize for TraversalValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TraversalValue::Empty => serializer.serialize_none(),
            TraversalValue::Count(count) => count.serialize(serializer),
            TraversalValue::NodeArray(nodes) => nodes.serialize(serializer),
            TraversalValue::EdgeArray(edges) => edges.serialize(serializer),
            TraversalValue::ValueArray(values) => values.serialize(serializer),
            TraversalValue::Paths(paths) => paths.serialize(serializer),
            TraversalValue::VectorArray(vectors) => vectors.serialize(serializer),
        }
    }
}

impl TraversalValue {
    pub fn is_empty(&self) -> bool {
        match self {
            TraversalValue::Empty => true,
            _ => false,
        }
    }

    pub fn get_id(&self) -> Result<u128, TraversalValueError> {
        match self {
            TraversalValue::NodeArray(nodes) => {
                if nodes.is_empty() {
                    return Err(TraversalValueError::Empty);
                }
                if let Some(node) = nodes.first() {
                    return Ok(node.id);
                }
                Err(TraversalValueError::NoId)
            },
            TraversalValue::EdgeArray(edges) => {
                if edges.is_empty() {
                    return Err(TraversalValueError::Empty);
                }
                if let Some(edge) = edges.first() {
                    return Ok(edge.id);
                }
                Err(TraversalValueError::NoId)
            },
            // error because no id for other variants
            _ => Err(TraversalValueError::NoId),
        }
    }

    pub fn as_count(&self) -> Result<Count, TraversalValueError> {
        match self {
            TraversalValue::Count(count) => Ok(count.clone()),
            _ => Err(TraversalValueError::NoCount),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TraversalValueError {
    Empty,
    NoId,
    NoCount,
}

impl std::fmt::Display for TraversalValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraversalValueError::Empty => write!(f, "TraversalValue is empty"),
            TraversalValueError::NoId => write!(f, "TraversalValue has no id"),
            TraversalValueError::NoCount => write!(f, "TraversalValue is not a count"),
        }
    }
}