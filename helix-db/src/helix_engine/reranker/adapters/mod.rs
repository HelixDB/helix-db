// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Traversal iterator adapter for reranking operations.
//!
//! This adapter allows reranking to be chained into traversal pipelines:
//!
//! ```ignore
//! storage.search_v(query_vec, 100, "doc", None)
//!     .rerank(&mmr_reranker, None)
//!     .take(20)
//!     .collect_to::<Vec<_>>()
//! ```


use crate::helix_engine::{
    reranker::reranker::Reranker,
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};
use helix_macros::debug_trace;
use std::iter::once;

/// Iterator wrapper that performs reranking.
pub struct RerankIterator<I: Iterator<Item = Result<TraversalValue, GraphError>>> {
    iter: I,
}

impl<I: Iterator<Item = Result<TraversalValue, GraphError>>> Iterator for RerankIterator<I> {
    type Item = Result<TraversalValue, GraphError>;

    #[debug_trace("RERANK")]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Trait that adds reranking capability to traversal iterators.
pub trait RerankAdapter<'a>: Iterator<Item = Result<TraversalValue, GraphError>> {
    /// Apply a reranker to the current traversal results.
    ///
    /// # Arguments
    /// * `reranker` - The reranker implementation to use
    /// * `query` - Optional query text for relevance-based reranking
    ///
    /// # Returns
    /// A new traversal iterator with reranked results
    ///
    /// # Example
    /// ```ignore
    /// use helix_db::helix_engine::reranker::fusion::MMRReranker;
    ///
    /// let mmr = MMRReranker::new(0.7).unwrap();
    /// let results = storage.search_v(query, 100, "doc", None)
    ///     .rerank(&mmr, Some("search query"))
    ///     .take(20)
    ///     .collect_to::<Vec<_>>();
    /// ```
    fn rerank<R: Reranker>(
        self,
        reranker: &R,
        query: Option<&str>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>> + 'a> RerankAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn rerank<R: Reranker>(
        self,
        reranker: &R,
        query: Option<&str>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>> {
        // Collect all items from the iterator
        let items = self.inner.filter_map(|item| item.ok());

        // Apply reranking
        let reranked = match reranker.rerank(items, query) {
            Ok(results) => results
                .into_iter()
                .map(Ok::<TraversalValue, GraphError>)
                .collect::<Vec<_>>()
                .into_iter(),
            Err(e) => {
                let error = GraphError::RerankerError(e.to_string());
                once(Err(error)).collect::<Vec<_>>().into_iter()
            }
        };

        let iter = RerankIterator { iter: reranked };

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helix_engine::{reranker::fusion::RRFReranker, vector_core::vector::HVector};

    #[test]
    fn test_rerank_adapter_trait() {
        // This test verifies that the trait compiles correctly
        // Actual integration tests would need a full storage setup
        let reranker = RRFReranker::new();
        assert_eq!(reranker.name(), "RRF");
    }

    #[test]
    fn test_rerank_iterator() {
        let items = vec![
            Ok(TraversalValue::Vector(HVector::new(vec![1.0]))),
            Ok(TraversalValue::Vector(HVector::new(vec![2.0]))),
        ];

        let mut iter = RerankIterator {
            iter: items.into_iter(),
        };

        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }
}
