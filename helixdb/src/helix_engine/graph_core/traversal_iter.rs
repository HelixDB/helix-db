use std::{cmp::Ordering, sync::Arc};

use heed3::{RoTxn, RwTxn};

use super::ops::tr_val::TraversalVal;
use crate::{
    helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError},
    protocol::{filterable::Filterable, value::Value},
};
use itertools::Itertools;

pub struct RoTraversalIterator<'a, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a RoTxn<'a>,
}

// implementing iterator for TraversalIterator
impl<'a, I> Iterator for RoTraversalIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> RoTraversalIterator<'a, I> {
    pub fn take_and_collect_to<B: FromIterator<TraversalVal>>(self, n: usize) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .take(n)
            .collect::<B>()
    }

    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner.filter_map(|item| {
            item.ok()
        }).collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }

    pub fn collect_to_obj(self) -> TraversalVal {
        match self.inner.filter_map(|item| item.ok()).next() {
            Some(val) => val,
            None => TraversalVal::Empty,
        }
    }

    pub fn count_to_val(self) -> Value {
        Value::from(self.inner.count())
    }

    pub fn order_by_asc(self, property: &str) -> Result<Vec<TraversalVal>, GraphError> {
        let result = self
            .inner
            .filter_map(|item| item.ok())
            .sorted_by(|a, b| match (a, b) {
                (TraversalVal::Node(a), TraversalVal::Node(b)) => {
                    match (a.check_property(property), b.check_property(property)) {
                        (Ok(val_a), Ok(val_b)) => val_a.cmp(val_b),
                        (Ok(_), Err(_)) => Ordering::Less,
                        (Err(_), Ok(_)) => Ordering::Greater,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                }
                (TraversalVal::Edge(a), TraversalVal::Edge(b)) => {
                    match (a.check_property(property), b.check_property(property)) {
                        (Ok(val_a), Ok(val_b)) => val_a.cmp(val_b),
                        (Ok(_), Err(_)) => Ordering::Less,
                        (Err(_), Ok(_)) => Ordering::Greater,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                }
                (TraversalVal::Vector(a), TraversalVal::Vector(b)) => {
                    match (a.check_property(property), b.check_property(property)) {
                        (Ok(val_a), Ok(val_b)) => val_a.cmp(val_b),
                        (Ok(_), Err(_)) => Ordering::Less,
                        (Err(_), Ok(_)) => Ordering::Greater,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                }
                (TraversalVal::Count(val_a), TraversalVal::Count(val_b)) => val_a.cmp(val_b),
                (TraversalVal::Value(val_a), TraversalVal::Value(val_b)) => val_a.cmp(val_b),
                _ => Ordering::Equal,
            })
            .collect::<Vec<_>>();

        Ok(result)
    }

    pub fn order_by_desc(self, property: &str) -> Result<Vec<TraversalVal>, GraphError> {
        let result = self
            .inner
            .filter_map(|item| item.ok())
            .sorted_by(|a, b| match (a, b) {
                (TraversalVal::Node(a), TraversalVal::Node(b)) => {
                    match (a.check_property(property), b.check_property(property)) {
                        (Ok(val_a), Ok(val_b)) => val_b.cmp(val_a),
                        (Ok(_), Err(_)) => Ordering::Greater,
                        (Err(_), Ok(_)) => Ordering::Less,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                }
                (TraversalVal::Edge(a), TraversalVal::Edge(b)) => {
                    match (a.check_property(property), b.check_property(property)) {
                        (Ok(val_a), Ok(val_b)) => val_b.cmp(val_a),
                        (Ok(_), Err(_)) => Ordering::Greater,
                        (Err(_), Ok(_)) => Ordering::Less,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                }
                (TraversalVal::Vector(a), TraversalVal::Vector(b)) => {
                    match (a.check_property(property), b.check_property(property)) {
                        (Ok(val_a), Ok(val_b)) => val_b.cmp(val_a),
                        (Ok(_), Err(_)) => Ordering::Greater,
                        (Err(_), Ok(_)) => Ordering::Less,
                        (Err(_), Err(_)) => Ordering::Equal,
                    }
                }
                (TraversalVal::Count(val_a), TraversalVal::Count(val_b)) => val_b.cmp(val_a),
                (TraversalVal::Value(val_a), TraversalVal::Value(val_b)) => val_b.cmp(val_a),
                _ => Ordering::Equal,
            })
            .collect::<Vec<_>>();

        Ok(result)
    }

    pub fn map_value_or(
        mut self,
        default: bool,
        f: impl Fn(&Value) -> bool,
    ) -> Result<bool, GraphError> {
        let val = match &self.inner.next() {
            Some(Ok(TraversalVal::Value(val))) => {
                println!("value : {:?}", val);
                Ok(f(val))
            }
            Some(Ok(_)) => Err(GraphError::ConversionError(
                "Expected value, got something else".to_string(),
            )),
            Some(Err(err)) => Err(GraphError::from(err.to_string())),
            None => Ok(default),
        };
        println!("result: {:?}", val);
        val
    }
}

pub struct RwTraversalIterator<'scope, 'env, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'scope mut RwTxn<'env>,
}

// implementing iterator for TraversalIterator
impl<'scope, 'env, I> Iterator for RwTraversalIterator<'scope, 'env, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'scope, 'env, I: Iterator> RwTraversalIterator<'scope, 'env, I> {
    pub fn new(storage: Arc<HelixGraphStorage>, txn: &'scope mut RwTxn<'env>, inner: I) -> Self {
        Self {
            inner,
            storage,
            txn,
        }
    }

    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B
    where
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
    {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_to_val(self) -> TraversalVal
    where
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
    {
        match self
            .inner
            .filter_map(|item| item.ok())
            .collect::<Vec<_>>()
            .first()
        {
            Some(val) => val.clone(), // TODO: Remove clone
            None => TraversalVal::Empty,
        }
    }
}
// pub trait TraversalIteratorMut<'a> {
//     type Inner: Iterator<Item = Result<TraversalVal, GraphError>>;

//     fn next<'b>(
//         &mut self,
//         storage: Arc<HelixGraphStorage>,
//         txn: &'b mut RwTxn<'a>,
//     ) -> Option<Result<TraversalVal, GraphError>>;

// }
