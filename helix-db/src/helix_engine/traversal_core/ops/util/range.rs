use std::sync::Arc;

use crate::helix_engine::{
    traversal_core::{traversal_value::TraversalValue, traversal_iter::RoTraversalIterator},
    types::GraphError,
};

pub struct Range<I> {
    iter: I,
    curr_idx: usize,
    start: usize,
    end: usize,
}

// implementing iterator for Range
impl<I> Iterator for Range<I>
where
    I: Iterator<Item = Result<TraversalValue, GraphError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // skips to start
        while self.curr_idx < self.start {
            match self.iter.next() {
                Some(_) => self.curr_idx += 1,
                None => return None, // out of items
            }
        }

        // return between start and end
        if self.curr_idx < self.end {
            match self.iter.next() {
                Some(item) => {
                    self.curr_idx += 1;
                    Some(item)
                }
                None => None,
            }
        } else {
            // all consumed
            None
        }
    }
}

pub trait RangeAdapter<'a>: Iterator {
    /// Range returns a slice of the current step between two points
    ///
    /// # Arguments
    ///
    /// * `start` - The starting index
    /// * `end` - The ending index
    ///
    /// # Example
    ///
    /// ```rust
    /// let traversal = G::new(storage, &txn).range(0, 10);
    /// ```
    fn range<N, K>(
        self,
        start: N,
        end: K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
        N: TryInto<usize>,
        K: TryInto<usize>,
        N::Error: std::fmt::Debug,
        K::Error: std::fmt::Debug;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>> + 'a> RangeAdapter<'a>
    for RoTraversalIterator<'a, I>
{   
    #[inline(always)]
    fn range<N, K>(
        self,
        start: N,
        end: K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
        N: TryInto<usize>,
        K: TryInto<usize>,
        N::Error: std::fmt::Debug,
        K::Error: std::fmt::Debug,
    {
        {
            let start_usize = start.try_into().expect("Start index must be non-negative and fit in usize");
            let end_usize = end.try_into().expect("End index must be non-negative and fit in usize");
            
            RoTraversalIterator {
                inner: Range {
                    iter: self.inner,
                    curr_idx: 0,
                    start: start_usize,
                    end: end_usize,
                },
                storage: Arc::clone(&self.storage),
                txn: self.txn,
            }
        }
    }
}
