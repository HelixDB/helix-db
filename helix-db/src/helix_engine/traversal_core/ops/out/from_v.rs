use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::{GraphError, VectorError},
};

pub trait FromVAdapter<'db, 'arena, 'txn, I>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
where
    'db: 'arena,
    'arena: 'txn,
{
    fn from_v(
        self,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    FromVAdapter<'db, 'arena, 'txn, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
where
    'db: 'arena,
    'arena: 'txn,
{
    #[inline(always)]
    fn from_v(
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
                let vector = if get_vector_data {
                    match self
                        .storage
                        .vectors
                        .get_full_vector(self.txn, item.from_node, self.arena)
                    {
                        Ok(vector) => TraversalValue::Vector(vector),
                        Err(e) => return Some(Err(GraphError::from(e))),
                    }
                } else {
                    match self.storage.vectors.get_vector_properties(
                        self.txn,
                        item.from_node,
                        self.arena,
                    ) {
                        Ok(Some(vector)) => TraversalValue::Vector(vector),
                        Ok(None) => {
                            return Some(Err(GraphError::from(VectorError::VectorNotFound(
                                item.from_node.to_string(),
                            ))));
                        }
                        Err(e) => return Some(Err(GraphError::from(e))),
                    }
                };

                Some(Ok(vector))
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
