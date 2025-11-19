use heed3::RoTxn;

use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::{GraphError, VectorError},
    vector_core::HVector,
};
use std::iter::once;

pub trait SearchVAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn search_v<F, K>(
        self,
        query: &'arena [f64],
        k: K,
        label: &'arena str,
        filter: Option<&'arena [F]>,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&HVector, &RoTxn) -> bool,
        K: TryInto<usize>,
        K::Error: std::fmt::Debug;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    SearchVAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn search_v<F, K>(
        self,
        query: &'arena [f64],
        k: K,
        label: &'arena str,
        filter: Option<&'arena [F]>,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
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
            false,
            self.arena,
        );

        let iter = match vectors {
            Ok(vectors) => vectors
                .into_iter()
                // copying here!
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

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}
