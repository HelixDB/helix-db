use std::{rc::Rc, sync::Arc};

use heed3::RoTxn;

use crate::{
    helix_engine::{
        traversal_core::{
            traversal_iter::{RwTraversalIterator, RwVecTraversalIterator},
            traversal_value::TraversalValue,
        },
        types::GraphError,
        vector_core::{hnsw::HNSW, vector::HVector},
    },
    protocol::value::Value,
};

pub struct InsertVIterator {
    inner: std::iter::Once<Result<TraversalValue, GraphError>>,
}

impl Iterator for InsertVIterator {
    type Item = Result<TraversalValue, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait InsertMultiVAdapter<'a, 'b>: Iterator<Item = Result<TraversalValue, GraphError>> {
    fn insert_v<F>(
        self,
        query: &[f64],
        label: &str,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwVecTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool + Sync;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalValue, GraphError>>> InsertMultiVAdapter<'a, 'b>
    for RwVecTraversalIterator<'a, 'b, I>
{
    fn insert_v<F>(
        self,
        query: &[f64],
        label: &str,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwVecTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool + Sync,
    {
        let fields = match fields {
            Some(mut fields) => {
                fields.push((String::from("label"), Value::String(label.to_string())));
                fields.push((String::from("is_deleted"), Value::Boolean(false)));
                Some(fields)
            }
            None => Some(vec![
                (String::from("label"), Value::String(label.to_string())),
                (String::from("is_deleted"), Value::Boolean(false)),
            ]),
        };
        let vector = self
            .storage
            .vectors
            .insert_with_vec_txn::<F>(self.txn, query, fields);

        let result = match vector {
            Ok(vector) => Ok(TraversalValue::Vector(Arc::unwrap_or_clone(vector))),
            Err(e) => Err(GraphError::from(e)),
        };

        RwVecTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}

pub trait InsertVAdapter<'a, 'b>: Iterator<Item = Result<TraversalValue, GraphError>> {
    fn insert_v<F>(
        self,
        query: &[f64],
        label: &str,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool + Sync;
}
impl<'a, 'b, I: Iterator<Item = Result<TraversalValue, GraphError>>> InsertVAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn insert_v<F>(
        self,
        query: &[f64],
        label: &str,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool + Sync,
    {
        let fields = match fields {
            Some(mut fields) => {
                fields.push((String::from("label"), Value::String(label.to_string())));
                fields.push((String::from("is_deleted"), Value::Boolean(false)));
                Some(fields)
            }
            None => Some(vec![
                (String::from("label"), Value::String(label.to_string())),
                (String::from("is_deleted"), Value::Boolean(false)),
            ]),
        };
        let vector = self
            .storage
            .vectors
            .insert_with_lmdb_txn::<F>(self.txn, query, fields);

        let result = match vector {
            Ok(vector) => Ok(TraversalValue::Vector(vector)),
            Err(e) => Err(GraphError::from(e)),
        };

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
