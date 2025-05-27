use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::items::Edge,
};
use heed3::RoTxn;
use std::{iter::Once, sync::Arc};

pub struct EFromId<'a, T> {
    iter: Once<Result<TraversalVal, GraphError>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    id: &'a u128,
}

impl<'a> Iterator for EFromId<'a, RoTxn<'a>> {

    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let edge: Edge = match self.storage.get_edge(self.txn, self.id) {
                Ok(edge) => edge,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Edge(edge))
        })
    }
}
pub trait EFromIdAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns an iterator containing the edge with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing edge id.
    fn e_from_id(self, id: &'a u128) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> EFromIdAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, EFromId<'a, RoTxn<'a>>>;

    #[inline]
    fn e_from_id(self, id: &'a u128) -> Self::OutputIter {
        let e_from_id = EFromId {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            id,
        };

        RoTraversalIterator {
            inner: e_from_id,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
