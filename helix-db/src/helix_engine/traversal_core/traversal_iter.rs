use std::sync::Arc;

use heed3::{AnyTls, RoTxn, RwTxn};

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage, traversal_core::traversal_value::TraversalValue,
        types::GraphError,
    },
    protocol::value::Value,
};
use itertools::Itertools;

pub struct RoTraversalIterator<'a, I, Tls = AnyTls> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a RoTxn<'a, Tls>,
}

// implementing iterator for TraversalIterator
impl<'a, I> Iterator for RoTraversalIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalValue, GraphError>>,
{
    type Item = Result<TraversalValue, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> RoTraversalIterator<'a, I> {
    pub fn take_and_collect_to<B: FromIterator<TraversalValue>>(self, n: usize) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .take(n)
            .collect::<B>()
    }

    pub fn collect_to<B: FromIterator<TraversalValue>>(self) -> B {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalValue>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }

    pub fn collect_to_obj(self) -> TraversalValue {
        match self.inner.filter_map(|item| item.ok()).next() {
            Some(val) => val,
            None => TraversalValue::Empty,
        }
    }

    pub fn count_to_val(self) -> Value {
        Value::from(self.inner.count())
    }

    pub fn map_value_or(
        mut self,
        default: bool,
        f: impl Fn(&Value) -> bool,
    ) -> Result<bool, GraphError> {
        let val = match &self.inner.next() {
            Some(Ok(TraversalValue::Value(val))) => {
                println!("value : {val:?}");
                Ok(f(val))
            }
            Some(Ok(_)) => Err(GraphError::ConversionError(
                "Expected value, got something else".to_string(),
            )),
            Some(Err(err)) => Err(GraphError::from(err.to_string())),
            None => Ok(default),
        };
        println!("result: {val:?}");
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
    I: Iterator<Item = Result<TraversalValue, GraphError>>,
{
    type Item = Result<TraversalValue, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'scope, 'env, I: Iterator<Item = Result<TraversalValue, GraphError>>>
    RwTraversalIterator<'scope, 'env, I>
{
    pub fn new(storage: Arc<HelixGraphStorage>, txn: &'scope mut RwTxn<'env>, inner: I) -> Self {
        Self {
            inner,
            storage,
            txn,
        }
    }

    pub fn collect_to<B: FromIterator<TraversalValue>>(self) -> B
    where
        I: Iterator<Item = Result<TraversalValue, GraphError>>,
    {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_to_val(self) -> TraversalValue
    where
        I: Iterator<Item = Result<TraversalValue, GraphError>>,
    {
        match self
            .inner
            .filter_map(|item| item.ok())
            .collect::<Vec<_>>()
            .first()
        {
            Some(val) => val.clone(), // TODO: Remove clone
            None => TraversalValue::Empty,
        }
    }
    pub fn collect_to_obj(self) -> TraversalValue {
        match self.inner.filter_map(|item| item.ok()).next() {
            Some(val) => val,
            None => TraversalValue::Empty,
        }
    }
}
// pub trait TraversalIteratorMut<'a> {
//     type Inner: Iterator<Item = Result<TraversalValue, GraphError>>;

//     fn next<'b>(
//         &mut self,
//         storage: Arc<HelixGraphStorage>,
//         txn: &'b mut RwTxn<'a>,
//     ) -> Option<Result<TraversalValue, GraphError>>;

// }
