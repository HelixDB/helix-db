use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::items::Edge,
};
use heed3::RoTxn;
use std::iter::Once;

pub struct EFromId<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RoTxn<'db>,
    pub iter: Once<Result<TraversalValue<'arena>, GraphError>>,
    pub id: u128,
}

impl<'db, 'arena, 'txn> Iterator for EFromId<'db, 'arena, 'txn> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let edge: Edge = match self.storage.get_edge(self.txn, &self.id, self.arena) {
                Ok(edge) => edge,
                Err(e) => return Err(e),
            };
            Ok(TraversalValue::Edge(edge))
        })
    }
}
pub trait EFromIdAdapter<'arena>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    type OutputIter: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>;

    /// Returns an iterator containing the edge with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing edge id.
    fn e_from_id(self, id: &u128) -> Self::OutputIter;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    EFromIdAdapter<'arena> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    type OutputIter = RoTraversalIterator<'db, 'arena, 'txn, EFromId<'db, 'arena, 'txn>>;

    #[inline]
    fn e_from_id(self, id: &u128) -> Self::OutputIter {
        let e_from_id = EFromId {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            iter: std::iter::once(Ok(TraversalValue::Empty)),
            id: *id,
        };

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: e_from_id,
        }
    }
}
