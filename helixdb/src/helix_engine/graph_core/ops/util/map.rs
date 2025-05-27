use crate::helix_engine::{
    graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
    types::GraphError,
};

use super::super::tr_val::TraversalVal;
use heed3::RoTxn;

pub struct Map<'a, I, F> {
    iter: I,
    txn: &'a RoTxn<'a>,
    f: F,
}

// implementing iterator for filter ref
impl<'a, I, F> Iterator for Map<'a, I, F>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(TraversalVal, &RoTxn<'a>) -> Result<TraversalVal, GraphError>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.iter.next() {
            return match item {
                Ok(item) => Some((self.f)(item, &self.txn)),
                Err(e) => return Some(Err(e)),
            };
        }
        None
    }
}

pub trait MapAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
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
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: FnMut(TraversalVal, &RoTxn<'a>) -> Result<TraversalVal, GraphError>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> MapAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    #[inline]
    fn map_traversal<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: FnMut(TraversalVal, &RoTxn<'a>) -> Result<TraversalVal, GraphError>,
    {
        RoTraversalIterator {
            inner: Map {
                iter: self.inner,
                txn: self.txn,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}

pub struct MapMut<I, F> {
    iter: I,
    f: F,
}
impl<I, F> Iterator for MapMut<I, F>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: Fn(I::Item) -> Result<TraversalVal, GraphError>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(item) = self.iter.next() {
            if let Ok(item) = (self.f)(item) {
                return Some(Ok(item));
            }
        }
        None
    }
}
pub trait MapAdapterMut<'scope, 'env>: Iterator<Item = Result<TraversalVal, GraphError>> {
    /// MapTraversalMut maps the iterator by taking a mutable
    /// reference to each item and a transaction.
    ///
    /// # Arguments
    ///
    /// * `f` - A function to map the iterator
    fn map_traversal_mut<F>(
        self,
        f: F,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(Result<TraversalVal, GraphError>) -> Result<TraversalVal, GraphError>;
}

impl<'scope, 'env, I: Iterator<Item = Result<TraversalVal, GraphError>>> MapAdapterMut<'scope, 'env>
    for RwTraversalIterator<'scope, 'env, I>
{
    #[inline]
    fn map_traversal_mut<F>(
        self,
        f: F,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(I::Item) -> Result<TraversalVal, GraphError>,
    {
        RwTraversalIterator {
            inner: MapMut {
                iter: self.inner,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
