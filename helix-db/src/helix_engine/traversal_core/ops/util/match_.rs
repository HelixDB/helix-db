use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};
use heed3::RoTxn;
use helix_macros::debug_trace;

pub struct Match<'a, I, F> {
    iter: I,
    txn: &'a RoTxn<'a>,
    f: F,
}

impl<'a, I, F> Iterator for Match<'a, I, F>
where
    I: Iterator<Item = Result<TraversalValue, GraphError>>,
    F: Fn(TraversalValue, &RoTxn) -> Option<TraversalValue>,
{
    type Item = I::Item;
    #[debug_trace("MATCH")]
    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            if let Ok(item) = item {
                match (self.f)(item, self.txn) {
                    Some(result) => {
                        return Some(Ok(result));
                    }
                    None => {
                        return None;
                    }
                }
            } else {
                return None;
            }
        }
        None
    }
}

pub trait MatchAdapter<'a>: Iterator {
    /// Match filters the iterator by taking a reference
    /// to each item and a transaction.
    fn match_<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(TraversalValue, &RoTxn) -> Option<TraversalValue>;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> MatchAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    #[inline]
    fn match_<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(TraversalValue, &RoTxn) -> Option<TraversalValue>,
    {
        RoTraversalIterator {
            inner: Match {
                iter: self.inner,
                txn: self.txn,
                f,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
