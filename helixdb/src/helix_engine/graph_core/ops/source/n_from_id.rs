use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::items::Node,
};
use heed3::RoTxn;
use std::{iter::Once, sync::Arc};

pub struct NFromId<'a, T> {
    iter: Once<Result<TraversalVal, GraphError>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    id: u128,
}

impl<'a> Iterator for NFromId<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let node: Node = match self.storage.get_node(self.txn, &self.id) {
                Ok(node) => node,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Node(node))
        })
    }
}

pub trait NFromIdAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns an iterator containing the node with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing node id.
    fn n_from_id(self, id: &u128) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> NFromIdAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, NFromId<'a, RoTxn<'a>>>;

    #[inline]
    fn n_from_id(self, id: &u128) -> Self::OutputIter {
        let n_from_id = NFromId {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            id: *id,
        };

        RoTraversalIterator {
            inner: n_from_id,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
