use std::borrow::Cow;

use crate::helix_engine::types::GraphError;
use heed3::{
    types::{Bytes, Unit},
    Database, RoTxn, RwTxn,
};
use crate::protocol::{
    items::{Edge, Node},
    value::Value,
};
use uuid::Uuid;

use super::vectors::{HVector};

pub trait DBMethods {
    /// Creates a new database with a given name for a secondary index
    fn create_secondary_index(&mut self, name: &str) -> Result<(), GraphError>;

    /// Opens a database with a given name for a secondary index
    fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError>;
}

pub trait BasicStorageMethods {
    /// Gets a node object for a given node id without copying its underlying data.
    ///
    /// This should only used when fetched data is only needed temporarily
    /// as underlying data is pinned.
    fn get_temp_node<'a>(&self, txn: &'a RoTxn, id: &str) -> Result<&'a [u8], GraphError>;

    /// Gets a edge object for a given edge id without copying its underlying data.
    ///
    /// This should only used when fetched data is only needed temporarily
    /// as underlying data is pinned.
    fn get_temp_edge<'a>(&self, txn: &'a RoTxn, id: &str) -> Result<&'a [u8], GraphError>;
}
pub trait StorageMethods {
    /// Checks whether an entry with a given id exists.
    /// Works for nodes or edges.
    fn check_exists(&self, txn: &RoTxn, id: &str) -> Result<bool, GraphError>;

    /// Gets a node object for a given node id
    fn get_node(&self, txn: &RoTxn, id: &str) -> Result<Node, GraphError>;
    /// Gets a edge object for a given edge id
    fn get_edge(&self, txn: &RoTxn, id: &str) -> Result<Edge, GraphError>;

    fn get_node_by_secondary_index(
        &self,
        txn: &RoTxn,
        index: &str,
        value: &Value,
    ) -> Result<Node, GraphError>;

    /// Returns a list of edge objects of the outgoing edges from a given node
    fn get_out_edges(
        &self,
        txn: &RoTxn,
        node_id: &str,
        edge_label: &str,
    ) -> Result<Vec<Edge>, GraphError>;
    /// Returns a list of edge objects of the incoming edges from a given node
    fn get_in_edges(
        &self,
        txn: &RoTxn,
        node_id: &str,
        edge_label: &str,
    ) -> Result<Vec<Edge>, GraphError>;

    /// Returns a list of node objects of the outgoing nodes from a given node
    fn get_out_nodes(
        &self,
        txn: &RoTxn,
        node_id: &str,
        edge_label: &str,
    ) -> Result<Vec<Node>, GraphError>;
    /// Returns a list of node objects of the incoming nodes from a given node
    fn get_in_nodes(
        &self,
        txn: &RoTxn,
        node_id: &str,
        edge_label: &str,
    ) -> Result<Vec<Node>, GraphError>;

    /// Returns all nodes in the graph
    fn get_all_nodes(&self, txn: &RoTxn) -> Result<Vec<Node>, GraphError>;
    /// Returns all edges in the graph
    fn get_all_edges(&self, txn: &RoTxn) -> Result<Vec<Edge>, GraphError>;

    fn get_nodes_by_types(&self, txn: &RoTxn, labels: &[&str]) -> Result<Vec<Node>, GraphError>;

    /// Creates a node entry
    fn create_node(
        &self,
        txn: &mut RwTxn,
        label: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
        secondary_indices: Option<&[String]>,
    ) -> Result<Node, GraphError>;

    /// Creates an edge entry between two nodes
    fn create_edge(
        &self,
        txn: &mut RwTxn,
        label: &str,
        from_node: &str,
        to_node: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Edge, GraphError>;

    /// Deletes a node entry along with all of its connected edges
    fn drop_node(&self, txn: &mut RwTxn, id: &str) -> Result<(), GraphError>;

    /// Deletes an edge entry
    fn drop_edge(&self, txn: &mut RwTxn, id: &str) -> Result<(), GraphError>;

    /// Updates a node entry
    /// If a property does not exist, it will be created
    /// If a property exists, it will be updated
    fn update_node(
        &self,
        txn: &mut RwTxn,
        id: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Node, GraphError>;

    /// Updates an edge entry
    /// If a property does not exist, it will be created
    /// If a property exists, it will be updated
    fn update_edge(
        &self,
        txn: &mut RwTxn,
        id: &str,
        properties: impl IntoIterator<Item = (String, Value)>,
    ) -> Result<Edge, GraphError>;
}

pub trait SearchMethods {
    /// Find shortest path between two nodes
    fn shortest_path(
        &self,
        txn: &RoTxn<'_>,
        from_id: &str,
        to_id: &str,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError>;

    fn shortest_mutual_path(
        &self,
        txn: &RoTxn<'_>,
        from_id: &str,
        to_id: &str,
    ) -> Result<(Vec<Node>, Vec<Edge>), GraphError>;
}
