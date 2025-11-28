#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};

pub trait ToVAdapter<'db, 'arena, 'txn, I>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn to_v(
        self,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    ToVAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline(always)]
    fn to_v(
        self,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self.inner.filter_map(move |item| {
            if let Ok(TraversalValue::Edge(item)) = item {
                if get_vector_data {
                    match self
                        .storage
                        .vectors
                        .get_full_vector(self.txn, item.to_node, self.arena)
                    {
                        Ok(vector) => Some(Ok(TraversalValue::Vector(vector))),
                        Err(e) => Some(Err(GraphError::from(e))),
                    }
                } else {
                    match self.storage.vectors.get_vector_properties(
                        self.txn,
                        item.to_node,
                        self.arena,
                    ) {
                        Ok(Some(vector)) => {
                            Some(Ok(TraversalValue::VectorNodeWithoutVectorData(vector)))
                        }
                        Ok(None) => None,
                        Err(e) => Some(Err(GraphError::from(e))),
                    }
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
    ToVAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline(always)]
    fn to_v(
        self,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self.inner.filter_map(move |item| {
            if let Ok(TraversalValue::Edge(item)) = item {
                if get_vector_data {
                    match self
                        .storage
                        .vectors
                        .get_full_vector(self.txn, item.to_node, self.arena)
                    {
                        Ok(vector) => Some(Ok(TraversalValue::Vector(vector))),
                        Err(e) => Some(Err(GraphError::from(e))),
                    }
                } else {
                    match self.storage.vectors.get_vector_properties(
                        self.txn,
                        item.to_node,
                        self.arena,
                    ) {
                        Ok(Some(vector)) => {
                            Some(Ok(TraversalValue::VectorNodeWithoutVectorData(vector)))
                        }
                        Ok(None) => None,
                        Err(e) => Some(Err(GraphError::from(e))),
                    }
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
pub trait AsyncToVAdapter<'db, 'arena, 'txn>: Sized {
    fn to_v(
        self,
        get_vector_data: bool,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "slate")]
impl<'db, 'arena, 'txn, S> AsyncToVAdapter<'db, 'arena, 'txn>
    for AsyncRoTraversalIterator<'db, 'arena, 'txn, S>
where
    S: Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    #[inline(always)]
    fn to_v(
        self,
        get_vector_data: bool,
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
                    if get_vector_data {
                        // TODO: implement get_full_vector for slate
                        todo!("get_full_vector not yet implemented for slate");
                    } else {
                        // TODO: implement get_vector_properties for slate
                        todo!("get_vector_properties not yet implemented for slate");
                    }
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
