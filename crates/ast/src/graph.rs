use serde::{Deserialize, Serialize};

/// Type alias for node IDs.
pub type NodeId = u64;

/// Type alias for edge IDs.
pub type EdgeId = u64;
/// A reference to nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRef {
    /// All nodes.
    All,
    /// Concrete node IDs.
    Ids(Vec<NodeId>),
    /// Named variable.
    Var(String),
    /// Runtime parameter containing IDs.
    Param(String),
}

impl NodeRef {
    /// Reference all nodes.
    pub fn all() -> Self {
        Self::All
    }

    /// Reference one node.
    pub fn id(id: NodeId) -> Self {
        Self::Ids(vec![id])
    }

    /// Reference multiple nodes.
    pub fn ids(ids: impl IntoIterator<Item = NodeId>) -> Self {
        Self::Ids(ids.into_iter().collect())
    }

    /// Reference a variable.
    pub fn var(name: impl Into<String>) -> Self {
        Self::Var(name.into())
    }

    /// Reference a runtime parameter.
    pub fn param(name: impl Into<String>) -> Self {
        Self::Param(name.into())
    }
}

impl From<NodeId> for NodeRef {
    fn from(value: NodeId) -> Self {
        Self::id(value)
    }
}

impl From<Vec<NodeId>> for NodeRef {
    fn from(value: Vec<NodeId>) -> Self {
        Self::Ids(value)
    }
}

impl<const N: usize> From<[NodeId; N]> for NodeRef {
    fn from(value: [NodeId; N]) -> Self {
        Self::Ids(value.to_vec())
    }
}

impl From<&str> for NodeRef {
    fn from(value: &str) -> Self {
        Self::Var(value.to_string())
    }
}

/// A reference to edges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeRef {
    /// All edges.
    All,
    /// Concrete edge IDs.
    Ids(Vec<EdgeId>),
    /// Named variable.
    Var(String),
    /// Runtime parameter containing IDs.
    Param(String),
}

impl EdgeRef {
    /// Reference all edges.
    pub fn all() -> Self {
        Self::All
    }

    /// Reference one edge.
    pub fn id(id: EdgeId) -> Self {
        Self::Ids(vec![id])
    }

    /// Reference multiple edges.
    pub fn ids(ids: impl IntoIterator<Item = EdgeId>) -> Self {
        Self::Ids(ids.into_iter().collect())
    }

    /// Reference a variable.
    pub fn var(name: impl Into<String>) -> Self {
        Self::Var(name.into())
    }

    /// Reference a runtime parameter.
    pub fn param(name: impl Into<String>) -> Self {
        Self::Param(name.into())
    }
}

impl From<EdgeId> for EdgeRef {
    fn from(value: EdgeId) -> Self {
        Self::id(value)
    }
}

impl From<Vec<EdgeId>> for EdgeRef {
    fn from(value: Vec<EdgeId>) -> Self {
        Self::Ids(value)
    }
}

impl<const N: usize> From<[EdgeId; N]> for EdgeRef {
    fn from(value: [EdgeId; N]) -> Self {
        Self::Ids(value.to_vec())
    }
}
