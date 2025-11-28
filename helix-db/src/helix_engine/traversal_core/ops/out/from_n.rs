#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
use crate::helix_engine::{
    storage_core::storage_methods::StorageMethods,
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};

pub trait FromNAdapter<'db, 'arena, 'txn, I>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the nodes that the edges in `self.inner` originate from.
    fn from_n(
        self,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    FromNAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline(always)]
    fn from_n(
        self,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self.inner.filter_map(move |item| {
            if let Ok(TraversalValue::Edge(item)) = item {
                match self.storage.get_node(self.txn, item.from_node, self.arena) {
                    Ok(node) => Some(Ok(TraversalValue::Node(node))),
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        });
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    FromNAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline(always)]
    fn from_n(
        self,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self.inner.filter_map(move |item| {
            if let Ok(TraversalValue::Edge(item)) = item {
                match self.storage.get_node(self.txn, item.from_node, self.arena) {
                    Ok(node) => Some(Ok(TraversalValue::Node(node))),
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        });
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}

#[cfg(feature = "slate")]
use futures::Stream;

#[cfg(feature = "slate")]
pub trait AsyncFromNAdapter<'db, 'arena, 'txn>: Sized {
    fn from_n(
        self,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "slate")]
impl<'db, 'arena, 'txn, S> AsyncFromNAdapter<'db, 'arena, 'txn>
    for AsyncRoTraversalIterator<'db, 'arena, 'txn, S>
where
    S: Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    #[inline(always)]
    fn from_n(
        self,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        use futures::StreamExt;

        let stream = async_stream::try_stream! {
            let mut inner = Box::pin(self.inner);

            while let Some(item) = inner.next().await {
                let item = item?;
                if let TraversalValue::Edge(edge) = item {
                    let node = self.storage.get_node(self.txn, edge.from_node, self.arena).await?;
                    yield TraversalValue::Node(node);
                }
            }
        };

        AsyncRoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: stream,
        }
    }
}
