use heed3::RoTxn;

use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::{GraphError, VectorError},
    vector_core::{hnsw::HNSW, vector::HVector, vector_distance::SimilarityMethod},
};
use helix_macros::debug_trace;
use std::iter::once;

pub struct SearchV<I: Iterator<Item = Result<TraversalValue, GraphError>>> {
    iter: I,
}

// implementing iterator for OutIterator
impl<I: Iterator<Item = Result<TraversalValue, GraphError>>> Iterator for SearchV<I> {
    type Item = Result<TraversalValue, GraphError>;

    #[debug_trace("SEARCH_V")]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait SearchVAdapter<'a>: Iterator<Item = Result<TraversalValue, GraphError>> {
    fn search_v<F, K>(
        self,
        query: &[f64],
        k: K,
        label: &str,
        filter: Option<&[F]>,
        method: &SimilarityMethod,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
        K: TryInto<usize>,
        K::Error: std::fmt::Debug;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>> + 'a> SearchVAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn search_v<F, K>(
        self,
        query: &[f64],
        k: K,
        label: &str,
        filter: Option<&[F]>,
        method: &SimilarityMethod,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
        K: TryInto<usize>,
        K::Error: std::fmt::Debug,
    {
        let vectors = self.storage.vectors.search(
            self.txn,
            query,
            k.try_into().unwrap(),
            label,
            filter,
            false,
            method,
        );

        let iter = match vectors {
            Ok(vectors) => vectors
                .into_iter()
                .map(|vector| Ok::<TraversalValue, GraphError>(TraversalValue::Vector(vector)))
                .collect::<Vec<_>>()
                .into_iter(),
            Err(VectorError::VectorNotFound(id)) => {
                let error = GraphError::VectorError(format!("vector not found for id {id}"));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(VectorError::InvalidVectorData) => {
                let error = GraphError::VectorError("invalid vector data".to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(VectorError::EntryPointNotFound) => {
                let error =
                    GraphError::VectorError("no entry point found for hnsw index".to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(VectorError::ConversionError(e)) => {
                let error = GraphError::VectorError(format!("conversion error: {e}"));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(VectorError::VectorCoreError(e)) => {
                let error = GraphError::VectorError(format!("vector core error: {e}"));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(VectorError::InvalidVectorLength) => {
                let error = GraphError::VectorError("invalid vector dimensions!".to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
            Err(id) => {
                let error = GraphError::VectorError(format!("vector already deleted for id {id}"));
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
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
