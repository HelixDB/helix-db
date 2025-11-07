use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};

use heed3::RoTxn;

pub struct Map<'db, 'txn, I, F> {
    iter: I,
    txn: &'txn RoTxn<'db>,
    f: F,
}

// implementing iterator for filter ref
impl<'db, 'arena, 'txn, I, F> Iterator for Map<'db, 'txn, I, F>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    F: FnMut(TraversalValue<'arena>, &RoTxn<'db>) -> Result<TraversalValue<'arena>, GraphError>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.iter.by_ref().next() {
            return match item {
                Ok(item) => Some((self.f)(item, self.txn)),
                Err(e) => return Some(Err(e)),
            };
        }
        None
    }
}

pub trait MapAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// MapTraversal maps the iterator by taking a reference
    /// to each item and a transaction.
    ///
    /// # Arguments
    ///
    /// * `f` - A function to map the iterator
    ///
    /// # Example
    ///
    /// ```rust
    /// let traversal = G::new(storage, &txn).map_traversal(|item, txn| {
    ///     Ok(item)
    /// });
    /// ```
    fn map_traversal<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: FnMut(TraversalValue<'arena>, &RoTxn<'db>) -> Result<TraversalValue<'arena>, GraphError>;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    MapAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn map_traversal<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: FnMut(TraversalValue<'arena>, &RoTxn<'db>) -> Result<TraversalValue<'arena>, GraphError>,
    {
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: Map {
                iter: self.inner,
                txn: self.txn,
                f,
            },
        }
    }
}
