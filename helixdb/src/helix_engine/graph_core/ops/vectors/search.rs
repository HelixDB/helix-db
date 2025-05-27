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
        let vectors = self
            .storage
            .vectors
            .search(self.txn, &query, k, filter, false);

        let iter = match vectors {
            Ok(vectors) => vectors
                .into_iter()
                .map(|vector| Ok::<TraversalVal, GraphError>(TraversalVal::Vector(vector)))
                .collect::<Vec<_>>()
                .into_iter(),
            Err(VectorError::VectorNotFound(id)) => {
                let error = GraphError::VectorError(format!("vector not found for id {}", id));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            },
            Err(VectorError::InvalidVectorData) => {
                let error = GraphError::VectorError("invalid vector data".to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            },
            Err(VectorError::EntryPointNotFound) => {
                let error = GraphError::VectorError("no entry point found for hnsw index".to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            },
            Err(VectorError::ConversionError(e)) => {
                let error = GraphError::VectorError(format!("conversion error: {}", e));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            },
            Err(VectorError::VectorCoreError(e)) => {
                let error = GraphError::VectorError(format!("vector core error: {}", e));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            },
            Err(VectorError::InvalidVectorLength) => {
                let error = GraphError::VectorError("invalid vector dimensions!".to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(_) => once(Err(GraphError::VectorError(
                "a vector error has occured!".to_string(),
            )))
            .collect::<Vec<_>>()
            .into_iter(),
        };

        let iter = SearchV { iter };

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
