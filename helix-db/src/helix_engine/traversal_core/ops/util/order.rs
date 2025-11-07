use std::cmp::Ordering;

use itertools::Itertools;

use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
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
    fn order_by_asc(
        self,
        property: &str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn order_by_desc(
        self,
        property: &str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    OrderByAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn order_by_asc(
        self,
        property: &str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        RoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: OrderByAsc {
                iter: self.inner.sorted_by(|a, b| match (a, b) {
                    (Ok(a), Ok(b)) => match (a, b) {
                        (TraversalValue::Node(a), TraversalValue::Node(b)) => {
                            match (a.get_property(property), b.get_property(property)) {
                                (Some(val_a), Some(val_b)) => val_a.cmp(val_b),
                                (Some(_), None) => Ordering::Less,
                                (None, Some(_)) => Ordering::Greater,
                                (None, None) => Ordering::Equal,
                            }
                        }
                        (TraversalValue::Edge(a), TraversalValue::Edge(b)) => {
                            match (a.get_property(property), b.get_property(property)) {
                                (Some(val_a), Some(val_b)) => val_a.cmp(val_b),
                                (Some(_), None) => Ordering::Less,
                                (None, Some(_)) => Ordering::Greater,
                                (None, None) => Ordering::Equal,
                            }
                        }
                        (TraversalValue::Vector(a), TraversalValue::Vector(b)) => {
                            match (a.get_property(property), b.get_property(property)) {
                                (Some(val_a), Some(val_b)) => val_a.cmp(val_b),
                                (Some(_), None) => Ordering::Less,
                                (None, Some(_)) => Ordering::Greater,
                                (None, None) => Ordering::Equal,
                            }
                        }
                        (TraversalValue::Value(val_a), TraversalValue::Value(val_b)) => {
                            val_a.cmp(val_b)
                        }
                        _ => Ordering::Equal,
                    },
                    (Err(_), _) => Ordering::Equal,
                    (_, Err(_)) => Ordering::Equal,
                }),
            },
        }
    }

    fn order_by_desc(
        self,
        property: &str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        RoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: OrderByAsc {
                iter: self.inner.sorted_by(|a, b| match (a, b) {
                    (Ok(a), Ok(b)) => match (a, b) {
                        (TraversalValue::Node(a), TraversalValue::Node(b)) => {
                            match (a.get_property(property), b.get_property(property)) {
                                (Some(val_a), Some(val_b)) => val_b.cmp(val_a),
                                (Some(_), None) => Ordering::Less,
                                (None, Some(_)) => Ordering::Greater,
                                (None, None) => Ordering::Equal,
                            }
                        }
                        (TraversalValue::Edge(a), TraversalValue::Edge(b)) => {
                            match (a.get_property(property), b.get_property(property)) {
                                (Some(val_a), Some(val_b)) => val_b.cmp(val_a),
                                (Some(_), None) => Ordering::Less,
                                (None, Some(_)) => Ordering::Greater,
                                (None, None) => Ordering::Equal,
                            }
                        }
                        (TraversalValue::Vector(a), TraversalValue::Vector(b)) => {
                            match (a.get_property(property), b.get_property(property)) {
                                (Some(val_a), Some(val_b)) => val_b.cmp(val_a),
                                (Some(_), None) => Ordering::Less,
                                (None, Some(_)) => Ordering::Greater,
                                (None, None) => Ordering::Equal,
                            }
                        }
                        (TraversalValue::Value(val_a), TraversalValue::Value(val_b)) => {
                            val_b.cmp(val_a)
                        }
                        _ => Ordering::Equal,
                    },
                    (Err(_), _) => Ordering::Equal,
                    (_, Err(_)) => Ordering::Equal,
                }),
            },
        }
    }
}
