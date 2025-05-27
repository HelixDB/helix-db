use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::value::Value,
};
use heed3::RoTxn;
use serde::Serialize;
use std::{iter::Once, sync::Arc};

pub struct NFromIndex<'a, T, K: Into<Value> + Serialize> {
    iter: Once<Result<TraversalVal, GraphError>>,
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

pub trait NFromIndexAdapter<'a, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalVal, GraphError>>
{
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns a new iterator that will return the node from the secondary index.
    ///
    /// # Arguments
    ///
    /// * `index` - The name of the secondary index.
    /// * `key` - The key to search for in the secondary index.
    ///
    /// Note that both the `index` and `key` must be provided.
    /// The index must be a valid and existing secondary index and the key should match the type of the index.
    fn n_from_index(self, index: &'a str, key: &'a K) -> Self::OutputIter
    where
        K: Into<Value> + Serialize;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>, K: Into<Value> + Serialize + 'a>
    NFromIndexAdapter<'a, K> for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, NFromIndex<'a, RoTxn<'a>, K>>;

    #[inline]
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
