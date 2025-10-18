use crate::helix_engine::{
    traversal_core::{
        traversal_iter::RoTraversalIterator,
        traversal_value::{Traversable, TraversalValue},
    },
    types::GraphError,
};

pub struct Dedup<'arena, I> {
    arena: &'arena bumpalo::Bump,
    seen: bumpalo::collections::Vec<'arena, u128>,
    iter: I,
}

impl<'arena, I> Iterator for Dedup<'arena, I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                Ok(item) => {
                    if !self.seen.contains(&item.id()) {
                        Some(Ok(item))
                    } else {
                        self.seen.push(item.id());
                        self.next()
                    }
                }
                _ => Some(item),
            },
            None => None,
        }
    }
}

pub trait DedupAdapter<'db, 'arena, 'txn>: Iterator {
    /// Dedup returns an iterator that will return unique items when collected
    fn dedup(
        self,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    DedupAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn dedup(
        self,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        {
            RoTraversalIterator {
                arena: self.arena,
                storage: self.storage,
                txn: self.txn,
                inner: Dedup {
                    arena: self.arena,
                    iter: self.inner,
                    seen: bumpalo::collections::Vec::new_in(self.arena),
                },
            }
        }
    }
}
