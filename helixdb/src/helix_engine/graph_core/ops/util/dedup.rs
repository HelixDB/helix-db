use std::{collections::HashSet, sync::Arc};

use crate::helix_engine::{
    graph_core::{
        ops::tr_val::{Traversable, TraversalVal},
        traversal_iter::RoTraversalIterator,
    },
    types::GraphError,
};

pub struct Dedup<I> {
    iter: I,
    seen: HashSet<String>,
}

impl<I> Iterator for Dedup<I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                Ok(item) => {
                    if self.seen.insert(item.id().to_string()) {
                        Some(Ok(item))
                    } else {
                        self.next()
                    }
                }
                _ => Some(item),
            },
            None => None,
        }
    }
}

pub trait DedupAdapter<'a>: Iterator {
    /// Dedup returns an iterator that will return unique items when collected
    fn dedup(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> DedupAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn dedup(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        {
            let upper_bound = match self.inner.size_hint() {
                (_, Some(upper_bound)) => upper_bound,
                (lower, None) => lower,
            };
            RoTraversalIterator {
                inner: Dedup {
                    iter: self.inner,
                    seen: HashSet::with_capacity(upper_bound),
                },
                storage: Arc::clone(&self.storage),
                txn: self.txn,
            }
        }
    }
}
