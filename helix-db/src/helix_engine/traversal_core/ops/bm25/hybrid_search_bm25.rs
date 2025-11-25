/*
use heed3::RoTxn;

use super::super::tr_val::TraversalValue;
use crate::helix_engine::{
    bm25::bm25::{BM25, HybridSearch},
    graph_core::traversal_iter::RoTraversalIterator,
    storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use std::sync::Arc;

pub struct HybridSearchBM25<'scope, 'inner> {
    txn: &'scope RoTxn<'scope>,
    iter: std::vec::IntoIter<(u128, f32)>,
    storage: Arc<HelixGraphStorage>,
    label: &'inner str,
}

// implementing iterator for HybridSearchBM25
impl<'scope, 'inner> Iterator for HybridSearchBM25<'scope, 'inner> {
    type Item = Result<TraversalValue, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next()?;
        match self.storage.get_node(self.txn, &next.0) {
            Ok(node) => {
                if node.label == self.label {
                    Some(Ok(TraversalValue::Node(node)))
                } else {
                    return None;
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait HybridSearchBM25Adapter<'a>: Iterator<Item = Result<TraversalValue, GraphError>> {
    fn hybrid_search_bm25(
        self,
        label: &str,
        query: &str,
        query_vector: Vec<f64>,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> HybridSearchBM25Adapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn hybrid_search_bm25(
        self,
        label: &str,
        query: &str,
        query_vector: &Vec<f64>,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>> {
        // check bm25 enabled

        let results = self.storage.hybrid_search(
            query,
            query_vector,
            0.5,
            k,
        );

        let iter = HybridSearchBM25 {
            txn: self.txn,
            iter: results,
            storage: Arc::clone(&self.storage),
            label,
        };
        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
*/
