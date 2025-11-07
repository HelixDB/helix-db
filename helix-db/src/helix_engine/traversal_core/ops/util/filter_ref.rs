use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};
use heed3::RoTxn;

pub struct FilterRef<'db, 'txn, I, F> {
    iter: I,
    txn: &'txn RoTxn<'db>,
    f: F,
}

impl<'db, 'arena, 'txn, I, F> Iterator for FilterRef<'db, 'txn, I, F>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    F: Fn(&I::Item, &RoTxn) -> Result<bool, GraphError>,
{
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            match (self.f)(&item, self.txn) {
                Ok(result) => {
                    if result {
                        return Some(item);
                    }
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
        None
    }
}

pub trait FilterRefAdapter<'db, 'arena, 'txn>: Iterator {
    /// FilterRef filters the iterator by taking a reference
    /// to each item and a transaction.
    fn filter_ref<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&Result<TraversalValue<'arena>, GraphError>, &RoTxn) -> Result<bool, GraphError>;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    FilterRefAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn filter_ref<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&Result<TraversalValue<'arena>, GraphError>, &RoTxn) -> Result<bool, GraphError>,
    {
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: FilterRef {
                iter: self.inner,
                txn: self.txn,
                f,
            },
        }
    }
}
