use std::collections::HashSet;

use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};

pub struct Dedup<'arena, I> {
    seen: bumpalo::collections::Vec<'arena, u128>,
    iter: I,
}

pub trait DedupAdapter<'db, 'arena, 'txn> {
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

#[cfg(not(feature = "slate"))]
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
        let capacity = match self.inner.size_hint().0 {
            usize::MAX => usize::MAX,
            0 => 100,
            size => size,
        };
        let mut seen = HashSet::with_capacity(capacity);
        RoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: self.inner.filter_map(move |item| match item {
                Ok(item) => {
                    if !seen.contains(&item.id()) {
                        seen.insert(item.id());
                        Some(Ok(item))
                    } else {
                        None
                    }
                }
                _ => Some(item),
            }),
        }
    }
}

#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
#[cfg(feature = "slate")]
use futures::Stream;

pub trait AsyncDedupAdapter<'db, 'arena, 'txn> {
    /// Dedup returns an iterator that will return unique items when collected
    fn dedup(
        self,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, S: Stream<Item = Result<TraversalValue<'arena>, GraphError>>>
    AsyncDedupAdapter<'db, 'arena, 'txn> for AsyncRoTraversalIterator<'db, 'arena, 'txn, S>
{
    fn dedup(
        self,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        use futures::StreamExt;
        let stream = async_stream::try_stream! {
            let mut inner = Box::pin(self.inner);
            let capacity = match inner.size_hint().0 {
                usize::MAX => usize::MAX,
                0 => 100,
                size => size,
            };
            let mut seen = HashSet::with_capacity(capacity);
            while let Some(item) = inner.next().await {
                let item = item?;
                if !seen.contains(&item.id()) {
                    seen.insert(item.id());
                    yield item;
                }
            }
        };
        AsyncRoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: stream,
        }
    }
}
