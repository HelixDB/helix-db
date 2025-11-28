use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};

pub trait NFromIdAdapter<
    'db: 'arena,
    'arena: 'txn,
    'txn,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
>: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the node with the given id.
    ///
    /// Note that the `id` cannot be empty and must be a valid, existing node id.
    fn n_from_id(
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
    NFromIdAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_id(
        self,
        id: &u128,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let n_from_id = std::iter::once({
            match self.storage.get_node(self.txn, *id, self.arena) {
                Ok(node) => Ok(TraversalValue::Node(node)),
                Err(e) => Err(e),
            }
        });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: n_from_id,
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    NFromIdAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_id(
        self,
        id: &u128,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let n_from_id = std::iter::once({
            match self.storage.get_node(self.txn, *id, self.arena) {
                Ok(node) => Ok(TraversalValue::Node(node)),
                Err(e) => Err(e),
            }
        });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: n_from_id,
        }
    }
}

#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
#[cfg(feature = "slate")]
use futures::Stream;

#[cfg(feature = "slate")]
pub trait AsyncNFromIDAdapter<'db, 'arena, 'txn>: Sized {
    fn n_from_id(
        self,
        id: &u128,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "slate")]
impl<'db, 'arena, 'txn, I: Stream<Item = Result<TraversalValue<'arena>, GraphError>>>
    AsyncNFromIDAdapter<'db, 'arena, 'txn> for AsyncRoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_id(
        self,
        id: &u128,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        AsyncRoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: futures::stream::once(async {
                match self.storage.get_node(self.txn, *id, self.arena).await {
                    Ok(node) => Ok(TraversalValue::Node(node)),
                    Err(e) => Err(e),
                }
            }),
        }
    }
}
