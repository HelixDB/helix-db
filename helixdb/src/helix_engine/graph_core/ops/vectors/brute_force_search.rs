use crate::helix_storage::heed3::RoTxn;

use super::super::tr_val::TraversalVal;
use crate::helix_engine::{
    graph_core::traversal_iter::RoTraversalIterator,
    types::{GraphError, VectorError},
    vector_core::{
        hnsw::HNSW,
        vector::{cosine_similarity, HVector},
    },
};
use std::{collections::BinaryHeap, iter::once};

pub struct BruteForceSearchV<I: Iterator<Item = Result<TraversalVal, GraphError>>> {
    iter: I,
}

// implementing iterator for OutIterator
impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> Iterator for BruteForceSearchV<I> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait BruteForceSearchVAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn brute_force_search_v(
        self,
        query: &Vec<f64>,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> BruteForceSearchVAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn brute_force_search_v(
        self,
        query: &Vec<f64>,
        k: usize,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let mut iter = self.inner.collect::<Vec<_>>();
        iter.sort_by(|v1, v2| match (v1, v2) {
            (Ok(TraversalVal::Vector(v1)), Ok(TraversalVal::Vector(v2))) => {
                let d1 = cosine_similarity(&v1.get_data(), query).unwrap();
                let d2 = cosine_similarity(&v2.get_data(), query).unwrap();
                d1.partial_cmp(&d2).unwrap()
            }
            _ => panic!("expected vector traversal values"),
        });

        let iter = iter.into_iter().take(k);

        RoTraversalIterator {
            inner: iter.into_iter(),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
