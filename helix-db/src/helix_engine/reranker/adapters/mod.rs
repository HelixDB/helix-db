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
use std::iter::once;

/// Iterator wrapper that performs reranking.
pub struct RerankIterator<'arena, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>> {
    iter: I,
}

impl<'arena, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>> Iterator
    for RerankIterator<'arena, I>
{
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Trait that adds reranking capability to traversal iterators.
pub trait RerankAdapter<'arena, 'db, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
where
    'db: 'arena,
    'arena: 'txn,
{
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
    /// let results = storage.search_v(query, 100, "doc", None)
    ///     .rerank(MMRReranker::new(0.7).unwrap(), Some("search query"))
    ///     .take(20)
    ///     .collect_to::<Vec<_>>();
    /// ```
    fn rerank<R: Reranker>(
        self,
        reranker: R,
        query: Option<&str>,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I> RerankAdapter<'arena, 'db, 'txn>
    for RoTraversalIterator<'db, 'arena, 'txn, I>
where
    'db: 'arena,
    'arena: 'txn,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>> + 'arena,
{
    fn rerank<R: Reranker>(
        self,
        reranker: R,
        query: Option<&str>,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        // Collect all items from the iterator
        let items = self.inner.filter_map(|item| item.ok());

        // Apply reranking
        let reranked = match reranker.rerank(items, query) {
            Ok(results) => results
                .into_iter()
                .map(Ok::<TraversalValue<'arena>, GraphError>)
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
            arena: self.arena,
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
        let arena = bumpalo::Bump::new();
        let data1 = arena.alloc_slice_copy(&[1.0]);
        let data2 = arena.alloc_slice_copy(&[2.0]);
        let items = vec![
            Ok(TraversalValue::Vector(HVector::from_slice(
                "test", 0, data1,
            ))),
            Ok(TraversalValue::Vector(HVector::from_slice(
                "test", 0, data2,
            ))),
        ];

        let mut iter = RerankIterator {
            iter: items.into_iter(),
        };

        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        assert!(iter.next().is_none());
    }
}
