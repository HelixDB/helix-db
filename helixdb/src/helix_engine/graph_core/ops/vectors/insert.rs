use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        types::GraphError,
        vector_core::{hnsw::HNSW, vector::HVector},
    },
    protocol::value::Value,
};
use std::{collections::HashMap, sync::Arc};

pub struct InsertVIterator {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for InsertVIterator {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait InsertVAdapter<'a, 'b>:
    Iterator<Item = Result<TraversalVal, GraphError>> + Sized
{
    fn insert_v<F>(
        self,
        vec: &Vec<f64>,
        label: &str,
        fields: Option<HashMap<String, Value>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&HVector) -> bool;

    fn insert_vs<F>(
        self,
        vecs: &Vec<Vec<f64>>,
        fields: Option<HashMap<String, Value>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&HVector) -> bool;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> InsertVAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn insert_v<F>(
        self,
        query: &Vec<f64>,
        label: &str,
        fields: Option<HashMap<String, Value>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&HVector) -> bool,
    {
        let vector = self
            .storage
            .vectors
            .insert::<F>(self.txn, &query, fields);

        let result = match vector {
            Ok(vector) => Ok(TraversalVal::Vector(vector)),
            Err(e) => Err(GraphError::from(e)),
        };

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }

    fn insert_vs<F>(
        self,
        vecs: &Vec<Vec<f64>>,
        fields: Option<HashMap<String, Value>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&HVector) -> bool,
    {
        let txn = self.txn;
        let storage = Arc::clone(&self.storage);
        let iter = vecs
            .iter()
            .map(|vec| {
                let vector = storage.vectors.insert::<F>(txn, &vec, fields.clone()); // TODO: remove clone
                match vector {
                    Ok(vector) => Ok(TraversalVal::Vector(vector)),
                    Err(e) => Err(GraphError::from(e)),
                }
            })
            .collect::<Vec<_>>();

        RwTraversalIterator {
            inner: iter.into_iter(),
            storage: self.storage,
            txn,
        }
    }
}
