use crate::{
    helix_engine::vector_core::vector::HVector,
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        storage_core::{storage_core::HelixGraphStorage},
        types::GraphError,
    },
};
use heed3::RoTxn;
use helix_macros::debug_trace;
use std::{iter::Once, sync::Arc};

pub struct VFromId<'a, T> {
    iter: Once<Result<TraversalVal, GraphError>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    id: u128,
}

impl<'a> Iterator for VFromId<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    #[debug_trace("V_FROM_ID")]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let vec: HVector = match self.storage.get_vector(self.txn, &self.id) {
                Ok(vec) => vec,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Vector(vec))
        })
    }
}

pub trait VFromIdAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    /// Returns an iterator containing the vector with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing vector id.
    fn v_from_id(self, id: &u128) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> VFromIdAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, VFromId<'a, RoTxn<'a>>>;

    #[inline]
    fn v_from_id(self, id: &u128) -> Self::OutputIter {
        let v_from_id = VFromId {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            id: *id,
        };

        RoTraversalIterator {
            inner: v_from_id,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
