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
