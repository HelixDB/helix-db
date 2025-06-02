use heed3::RoTxn;

use super::super::tr_val::TraversalVal;
use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    types::{GraphError, VectorError},
    vector_core::{hnsw::HNSW, vector::HVector},
};
use std::iter::once;

pub struct SearchV<I: Iterator<Item = Result<TraversalVal, GraphError>>> {
    iter: I,
}

// implementing iterator for OutIterator
impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> Iterator for SearchV<I> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait SearchVAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>>  {
    fn search_v<F>(
        self,
        query: &Vec<f64>,
        k: usize,
        filter: Option<&[F]>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> SearchVAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn search_v<F>(
        self,
        query: &Vec<f64>,
        k: usize,
        filter: Option<&[F]>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let iter = match self
            .storage
            .vectors
            .search(self.txn, &query, k, filter, false) {
                Ok(vectors) => vectors
                    .into_iter()
                    .map(|vector| Ok::<TraversalVal, GraphError>(TraversalVal::Vector(vector)))
                    .collect::<Vec<_>>()
                    .into_iter(),
                Err(e) => once(Err(GraphError::from(e))).collect::<Vec<_>>().into_iter()
            };

        let iter = SearchV { iter };

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}

