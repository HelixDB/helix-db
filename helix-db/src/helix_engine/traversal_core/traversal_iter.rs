use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage, traversal_core::traversal_value::TraversalValue,
        types::GraphError,
    },
    protocol::value::Value,
};
use heed3::{RoTxn, RwTxn};
use itertools::Itertools;

pub struct RoTraversalIterator<'db, 'arena, 'txn, I>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RoTxn<'db>,
    pub inner: I,
}

// implementing iterator for TraversalIterator
impl<'db, 'arena, 'txn, I> Iterator for RoTraversalIterator<'db, 'arena, 'txn, I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    RoTraversalIterator<'db, 'arena, 'txn, I>
{
    pub fn take_and_collect_to<B: FromIterator<TraversalValue<'arena>>>(self, n: usize) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .take(n)
            .collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalValue<'arena>>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }

    pub fn collect_to_obj(mut self) -> Result<TraversalValue<'arena>, GraphError> {
        self.inner.next().unwrap_or(Err(GraphError::New("No value found".to_string())))
    }

    pub fn collect_to_value(self) -> Value {
        match self.inner.filter_map(|item| item.ok()).next() {
            Some(TraversalValue::Value(val)) => val,
            _ => Value::Empty,
        }
    }

    pub fn map_value_or(
        mut self,
        default: bool,
        f: impl Fn(&Value) -> bool,
    ) -> Result<bool, GraphError> {
        
        match &self.inner.next() {
            Some(Ok(TraversalValue::Value(val))) => Ok(f(val)),
            Some(Ok(_)) => Err(GraphError::ConversionError(
                "Expected value, got something else".to_string(),
            )),
            Some(Err(err)) => Err(GraphError::from(err.to_string())),
            None => Ok(default),
        }
    }
}

pub struct RwTraversalIterator<'db, 'arena, 'txn, I>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn mut RwTxn<'db>,
    pub inner: I,
}

// implementing iterator for TraversalIterator
impl<'db, 'arena, 'txn, I> Iterator for RwTraversalIterator<'db, 'arena, 'txn, I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    RwTraversalIterator<'db, 'arena, 'txn, I>
{
    pub fn new(
        storage: &'db HelixGraphStorage,
        txn: &'txn mut RwTxn<'db>,
        arena: &'arena bumpalo::Bump,
        inner: I,
    ) -> Self {
        Self {
            storage,
            txn,
            arena,
            inner,
        }
    }

    pub fn take_and_collect_to<B: FromIterator<TraversalValue<'arena>>>(self, n: usize) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .take(n)
            .collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalValue<'arena>>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }

    pub fn collect_to_obj(mut self) -> Result<TraversalValue<'arena>, GraphError> {
        self.inner.next().unwrap_or(Err(GraphError::New("No value found".to_string())))
    }

    pub fn map_value_or(
        mut self,
        default: bool,
        f: impl Fn(&Value) -> bool,
    ) -> Result<bool, GraphError> {
        
        match &self.inner.next() {
            Some(Ok(TraversalValue::Value(val))) => Ok(f(val)),
            Some(Ok(_)) => Err(GraphError::ConversionError(
                "Expected value, got something else".to_string(),
            )),
            Some(Err(err)) => Err(GraphError::from(err.to_string())),
            None => Ok(default),
        }
    }
}
