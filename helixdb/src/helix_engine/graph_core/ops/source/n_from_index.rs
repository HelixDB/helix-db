use std::{iter::Once, sync::Arc};

use heed3::{RoTxn, RwTxn};
use serde::Serialize;

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::{items::Node, value::Value},
};

use super::super::tr_val::TraversalVal;

pub struct NFromIndex<'a, T, K: Into<Value> + Serialize> {
    iter: Once<Result<TraversalVal, GraphError>>, // Use Once instead of Empty so we get exactly one item
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    index: &'a str,
    key: &'a K,
}

impl<'a, K> Iterator for NFromIndex<'a, RoTxn<'a>, K>
where
    K: Into<Value> + Serialize,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let db = self
                .storage
                .secondary_indices
                .get(self.index)
                .ok_or(GraphError::New(format!(
                    "Secondary Index {} not found",
                    self.index
                )))?;
            let node_id = db
                .get(self.txn, &bincode::serialize(self.key)?)?
                .ok_or(GraphError::NodeNotFound)?;
            let node_id =
                u128::from_be_bytes(node_id.try_into().expect("Invalid byte array length"));

            self.storage
                .get_node(self.txn, &node_id)
                .map(TraversalVal::Node)
        })
    }
}

impl<'a, K> Iterator for NFromIndex<'a, RwTxn<'a>, K>
where
    K: Into<Value> + Serialize,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let db = self
                .storage
                .secondary_indices
                .get(self.index)
                .ok_or(GraphError::New(format!(
                    "Secondary Index {} not found",
                    self.index
                )))?;
            let node_id = db
                .get(self.txn, &bincode::serialize(self.key)?)?
                .ok_or(GraphError::NodeNotFound)?;
            let node_id =
                u128::from_be_bytes(node_id.try_into().expect("Invalid byte array length"));

            self.storage
                .get_node(self.txn, &node_id)
                .map(TraversalVal::Node)
        })
    }
}

pub trait NFromIndexAdapter<'a, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    fn n_from_index(self, index: &'a str, key: &'a K) -> Self::OutputIter
    where
        K: Into<Value> + Serialize;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, K: Into<Value> + Serialize + 'a>
    NFromIndexAdapter<'a, K> for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, NFromIndex<'a, RoTxn<'a>, K>>;

    fn n_from_index(self, index: &'a str, key: &'a K) -> Self::OutputIter
    where
        K: Into<Value> + Serialize,
    {
        let n_from_index = NFromIndex {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            index,
            key,
        };

        RoTraversalIterator {
            inner: n_from_index,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
