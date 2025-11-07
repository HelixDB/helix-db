use crate::helix_engine::types::GraphError;
use crate::utils::items::{Edge, Node};
use heed3::{RoTxn, RwTxn};

pub trait DBMethods {
    /// Creates a new database with a given name for a secondary index
    fn create_secondary_index(&mut self, name: &str) -> Result<(), GraphError>;

    /// Opens a database with a given name for a secondary index
    fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError>;
}

pub trait StorageMethods {
    /// Gets a node object for a given node id
    fn get_node<'arena>(
        &self,
        txn: &RoTxn,
        id: &u128,
        arena: &'arena bumpalo::Bump,
    ) -> Result<Node<'arena>, GraphError>;

    /// Gets a edge object for a given edge id
    fn get_edge<'arena>(
        &self,
        txn: &RoTxn,
        id: &u128,
        arena: &'arena bumpalo::Bump,
    ) -> Result<Edge<'arena>, GraphError>;

    /// Removes the following from the storage engine:
    /// - The given node
    /// - All connected incoming AND outgoing edge mappings and the actual edges
    /// - All secondary indexes for the given node
    fn drop_node(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError>;

    /// Removes the following from the storage engine:
    /// - The given edge 
    /// - All incoming and outgoing mappings for that edge
    fn drop_edge(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError>;

    /// Sets the `deleted` field of a vector to true
    /// 
    /// NOTE: The vector is not ACTUALLY deleted and is still present in the db. 
    fn drop_vector(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError>;
}
