use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};

pub trait EFromIdAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
where
    'db: 'arena,
    'arena: 'txn,
{
    /// Returns an iterator containing the edge with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing edge id.
    fn e_from_id(
        self,
        id: &u128,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    EFromIdAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn e_from_id(
        self,
        id: &u128,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once({
                match self.storage.get_edge(self.txn, id, self.arena) {
                    Ok(edge) => Ok(TraversalValue::Edge(edge)),
                    Err(e) => Err(e),
                }
            }),
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    EFromIdAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn e_from_id(
        self,
        id: &u128,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once({
                match self.storage.get_edge(self.txn, *id, self.arena) {
                    Ok(edge) => Ok(TraversalValue::Edge(edge)),
                    Err(e) => Err(e),
                }
            }),
        }
    }
}
