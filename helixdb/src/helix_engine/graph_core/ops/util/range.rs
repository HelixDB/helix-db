use std::sync::Arc;

use crate::helix_engine::{
    graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
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
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
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
    fn range(
        self,
        start: usize,
        end: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized + Iterator,
        Self::Item: Send;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> RangeAdapter<'a>
    for RoTraversalIterator<'a, I>
{   
    #[inline(always)]
    fn range(
        self,
        start: usize,
        end: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
    {
        {
            RoTraversalIterator {
                inner: Range {
                    iter: self.inner,
                    curr_idx: 0,
                    start,
                    end,
                },
                storage: Arc::clone(&self.storage),
                txn: self.txn,
            }
        }
    }
}
