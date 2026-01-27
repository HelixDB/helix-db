use std::cmp::Ordering;

use itertools::Itertools;

use crate::{
    helix_engine::{
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    protocol::value::Value,
};

pub struct OrderByAsc<I> {
    iter: I,
}

impl<'arena, I> Iterator for OrderByAsc<I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct OrderByDesc<I> {
    iter: I,
}

impl<'arena, I> Iterator for OrderByDesc<I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait OrderByAdapter<'db, 'arena, 'txn>: Iterator {
    fn order_by_asc<F>(
        self,
        property: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&TraversalValue<'arena>) -> Value;

    fn order_by_desc<F>(
        self,
        property: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&TraversalValue<'arena>) -> Value;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    OrderByAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn order_by_asc<F>(
        self,
        property: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&TraversalValue<'arena>) -> Value,
    {
        let iter = self.inner.sorted_by(|a, b| match (a, b) {
            (Ok(a), Ok(b)) => property(a).cmp(&property(b)),
            (Err(_), _) => Ordering::Equal,
            (_, Err(_)) => Ordering::Equal,
        });

        RoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: OrderByAsc { iter },
        }
    }

    fn order_by_desc<F>(
        self,
        property: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&TraversalValue<'arena>) -> Value,
    {
        RoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: OrderByDesc {
                iter: self.inner.sorted_by(|a, b| match (a, b) {
                    (Ok(a), Ok(b)) => property(b).cmp(&property(a)),
                    (Err(_), _) => Ordering::Equal,
                    (_, Err(_)) => Ordering::Equal,
                }),
            },
        }
    }
}
