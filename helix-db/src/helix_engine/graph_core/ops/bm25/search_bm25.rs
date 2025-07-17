use heed3::RoTxn;

use super::super::tr_val::TraversalVal;
use crate::helix_engine::{
    bm25::bm25::BM25,
    graph_core::traversal_iter::RoTraversalIterator,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use std::sync::Arc;

pub struct SearchBM25<'scope, 'inner> {
    txn: &'scope RoTxn<'scope>,
    iter: std::vec::IntoIter<(u128, f32)>,
    storage: Arc<HelixGraphStorage>,
    label: &'inner str,
}

// implementing iterator for SearchBM25
impl<'scope, 'inner> Iterator for SearchBM25<'scope, 'inner> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next()?;
        match self.storage.get_node(self.txn, &next.0) {
            Ok(node) => {
                if node.label == self.label {
                    Some(Ok(TraversalVal::Node(node)))
                } else {
                    return None;
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait SearchBM25Adapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn search_bm25(
        self,
        label: &str,
        query: &str,
        k: usize,
    ) -> Result<
        RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>,
        GraphError,
    >;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> SearchBM25Adapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn search_bm25(
        self,
        label: &str,
        query: &str,
        k: usize,
    ) -> Result<
        RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>,
        GraphError,
    > {
        let results = match self.storage.bm25.as_ref() {
            Some(s) => match s.search(self.txn, query, k) {
                Ok(results) => results,
                Err(e) => return Err(e),
            },
            None => return Err(GraphError::from("BM25 not enabled!")),
        };

        let iter = SearchBM25 {
            txn: self.txn,
            iter: results.into_iter(),
            storage: Arc::clone(&self.storage),
            label,
        };
        Ok(RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        })
    }
}

