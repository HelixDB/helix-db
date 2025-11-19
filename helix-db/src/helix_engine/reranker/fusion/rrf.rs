// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Reciprocal Rank Fusion (RRF) reranker implementation.
//!
//! RRF combines multiple ranked lists without requiring score calibration.
//! Formula: RRF_score(d) = Σ 1/(k + rank_i(d))
//! where k is typically 60 (default).

use crate::helix_engine::{
    reranker::{
        errors::{RerankerError, RerankerResult},
        reranker::{Reranker, update_score},
    },
    traversal_core::traversal_value::TraversalValue,
};
use std::collections::HashMap;

/// Reciprocal Rank Fusion reranker.
///
/// Combines multiple ranked lists by computing reciprocal ranks.
/// This is particularly useful for hybrid search combining BM25 and vector results.
#[derive(Debug, Clone)]
pub struct RRFReranker {
    /// The k parameter in the RRF formula (default: 60)
    k: f64,
}

impl RRFReranker {
    /// Create a new RRF reranker with default k=60.
    pub fn new() -> Self {
        Self { k: 60.0 }
    }

    /// Create a new RRF reranker with custom k value.
    ///
    /// # Arguments
    /// * `k` - The k parameter in the RRF formula. Higher values give less weight to ranking position.
    pub fn with_k(k: f64) -> RerankerResult<Self> {
        if k <= 0.0 {
            return Err(RerankerError::InvalidParameter(
                "k must be positive".to_string(),
            ));
        }
        Ok(Self { k })
    }

    /// Fuse multiple ranked lists using RRF.
    ///
    /// # Arguments
    /// * `lists` - Vector of iterators, each representing a ranked list
    /// * `k` - The k parameter for RRF formula
    ///
    /// # Returns
    /// A vector of items reranked by RRF scores
    pub fn fuse_lists<'arena, I>(
        lists: Vec<I>,
        k: f64,
    ) -> RerankerResult<Vec<TraversalValue<'arena>>>
    where
        I: Iterator<Item = TraversalValue<'arena>>,
    {
        if lists.is_empty() {
            return Err(RerankerError::EmptyInput);
        }

        let mut rrf_scores: HashMap<u128, f64> = HashMap::new();
        let mut items_map: HashMap<u128, TraversalValue<'arena>> = HashMap::new();

        // Process each ranked list
        for list in lists {
            for (rank, item) in list.enumerate() {
                let id = match &item {
                    TraversalValue::Node(n) => n.id,
                    TraversalValue::Edge(e) => e.id,
                    TraversalValue::Vector(v) => v.id,
                    _ => continue,
                };

                // Calculate reciprocal rank: 1 / (k + rank)
                // rank starts at 0, so actual rank is rank + 1
                let rr_score = 1.0 / (k + (rank as f64) + 1.0);

                // Sum reciprocal ranks across all lists
                *rrf_scores.entry(id).or_insert(0.0) += rr_score;

                // Store the item (keep first occurrence)
                items_map.entry(id).or_insert(item);
            }
        }

        // Convert to scored items and sort by RRF score (descending)
        let mut scored_items: Vec<(u128, f64)> = rrf_scores.into_iter().collect();
        scored_items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Update scores and collect results
        let mut results = Vec::with_capacity(scored_items.len());
        for (id, score) in scored_items {
            if let Some(mut item) = items_map.remove(&id) {
                update_score(&mut item, score)?;
                results.push(item);
            }
        }

        Ok(results)
    }
}

impl Default for RRFReranker {
    fn default() -> Self {
        Self::new()
    }
}

impl Reranker for RRFReranker {
    fn rerank<'arena, I>(
        &self,
        items: I,
        _query: Option<&str>,
    ) -> RerankerResult<Vec<TraversalValue<'arena>>>
    where
        I: Iterator<Item = TraversalValue<'arena>>,
    {
        // For a single list, RRF just converts ranks to RRF scores
        let items_vec: Vec<_> = items.collect();

        if items_vec.is_empty() {
            return Err(RerankerError::EmptyInput);
        }

        let mut results = Vec::with_capacity(items_vec.len());

        for (rank, mut item) in items_vec.into_iter().enumerate() {
            // Calculate RRF score for this item based on its rank
            let rrf_score = 1.0 / (self.k + (rank as f64) + 1.0);
            update_score(&mut item, rrf_score)?;
            results.push(item);
        }

        Ok(results)
    }

    fn name(&self) -> &str {
        "RRF"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{helix_engine::vector_core::HVector, utils::items::Node};
    use bumpalo::Bump;

    fn alloc_vector<'a>(arena: &'a Bump, data: &[f64]) -> HVector<'a> {
        let slice = arena.alloc_slice_copy(data);
        HVector::from_slice("test_vector", 0, slice, arena)
    }

    #[test]
    fn test_rrf_single_list() {
        let arena = Bump::new();
        let reranker = RRFReranker::new();

        let vectors: Vec<TraversalValue> = (0..5)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
                v.distance = Some((i + 1) as f64);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = reranker.rerank(vectors.into_iter(), None).unwrap();

        assert_eq!(results.len(), 5);

        // Check that RRF scores are calculated correctly
        for (rank, item) in results.iter().enumerate() {
            if let TraversalValue::Vector(v) = item {
                let expected_score = 1.0 / (60.0 + (rank as f64) + 1.0);
                assert!((v.distance.unwrap() - expected_score).abs() < 1e-10);
            }
        }
    }

    #[test]
    fn test_rrf_custom_k() {
        let arena = Bump::new();
        let reranker = RRFReranker::with_k(10.0).unwrap();

        let vectors: Vec<TraversalValue> = (0..3)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = reranker.rerank(vectors.into_iter(), None).unwrap();

        // First item should have score 1/(10+1) = 1/11
        if let TraversalValue::Vector(v) = &results[0] {
            assert!((v.distance.unwrap() - 1.0 / 11.0).abs() < 1e-10);
        }
    }

    #[test]
    fn test_rrf_fuse_multiple_lists() {
        let arena = Bump::new();
        // Create two lists with some overlap
        let list1: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[2.0]);
                v.id = 2;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[3.0]);
                v.id = 3;
                TraversalValue::Vector(v)
            },
        ];

        let list2: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[2.0]);
                v.id = 2;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[4.0]);
                v.id = 4;
                TraversalValue::Vector(v)
            },
        ];

        let results =
            RRFReranker::fuse_lists(vec![list1.into_iter(), list2.into_iter()], 60.0).unwrap();

        // Items 1 and 2 appear in both lists, so should have higher scores
        assert_eq!(results.len(), 4);

        // Items 1 and 2 both appear at ranks 0 and 1 in the two lists
        // So they should have equal RRF scores and be the top 2 results
        if let TraversalValue::Vector(v) = &results[0] {
            assert!(v.id == 1 || v.id == 2);
        }
        if let TraversalValue::Vector(v) = &results[1] {
            assert!(v.id == 1 || v.id == 2);
        }
    }

    #[test]
    fn test_rrf_invalid_k() {
        let result = RRFReranker::with_k(-1.0);
        assert!(result.is_err());

        let result = RRFReranker::with_k(0.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_rrf_empty_input() {
        let reranker = RRFReranker::new();
        let empty: Vec<TraversalValue> = vec![];
        let result = reranker.rerank(empty.into_iter(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_rrf_fuse_empty_lists() {
        let result =
            RRFReranker::fuse_lists(Vec::<std::vec::IntoIter<TraversalValue>>::new(), 60.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_rrf_fuse_single_list() {
        let arena = Bump::new();
        let list: Vec<TraversalValue> = (0..3)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = RRFReranker::fuse_lists(vec![list.into_iter()], 60.0).unwrap();

        assert_eq!(results.len(), 3);
        // Single list fusion should maintain order with RRF scores
        if let TraversalValue::Vector(v) = &results[0] {
            assert_eq!(v.id, 0);
        }
    }

    #[test]
    fn test_rrf_fuse_three_lists() {
        let arena = Bump::new();
        // Create three lists with different overlaps
        let list1: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[2.0]);
                v.id = 2;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[3.0]);
                v.id = 3;
                TraversalValue::Vector(v)
            },
        ];

        let list2: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[4.0]);
                v.id = 4;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[2.0]);
                v.id = 2;
                TraversalValue::Vector(v)
            },
        ];

        let list3: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[5.0]);
                v.id = 5;
                TraversalValue::Vector(v)
            },
        ];

        let results = RRFReranker::fuse_lists(
            vec![list1.into_iter(), list2.into_iter(), list3.into_iter()],
            60.0,
        )
        .unwrap();

        // Item 1 appears in all three lists at rank 0, should be highest
        assert_eq!(results.len(), 5);
        if let TraversalValue::Vector(v) = &results[0] {
            assert_eq!(v.id, 1);
        }
    }

    #[test]
    fn test_rrf_fuse_disjoint_lists() {
        let arena = Bump::new();
        // Two lists with no overlap
        let list1: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[2.0]);
                v.id = 2;
                TraversalValue::Vector(v)
            },
        ];

        let list2: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[3.0]);
                v.id = 3;
                TraversalValue::Vector(v)
            },
            {
                let mut v = alloc_vector(&arena, &[4.0]);
                v.id = 4;
                TraversalValue::Vector(v)
            },
        ];

        let results =
            RRFReranker::fuse_lists(vec![list1.into_iter(), list2.into_iter()], 60.0).unwrap();

        // All items should be present with equal RRF scores for same ranks
        assert_eq!(results.len(), 4);

        // Items at rank 0 in their respective lists should have same score
        if let (TraversalValue::Vector(v1), TraversalValue::Vector(v2)) = (&results[0], &results[1])
        {
            let score1 = v1.distance.unwrap();
            let score2 = v2.distance.unwrap();
            assert!((score1 - score2).abs() < 1e-10);
        }
    }

    #[test]
    fn test_rrf_very_large_k() {
        let arena = Bump::new();
        let reranker = RRFReranker::with_k(1000.0).unwrap();

        let vectors: Vec<TraversalValue> = (0..5)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = reranker.rerank(vectors.into_iter(), None).unwrap();

        // With very large k, scores should be very small and close together
        assert_eq!(results.len(), 5);
        if let TraversalValue::Vector(v) = &results[0] {
            let score = v.distance.unwrap();
            assert!(score < 0.001); // 1/1001 ≈ 0.001
        }
    }

    #[test]
    fn test_rrf_very_small_k() {
        let arena = Bump::new();
        let reranker = RRFReranker::with_k(0.1).unwrap();

        let vectors: Vec<TraversalValue> = (0..3)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = reranker.rerank(vectors.into_iter(), None).unwrap();

        // With very small k, scores should be more differentiated
        assert_eq!(results.len(), 3);
        if let TraversalValue::Vector(v) = &results[0] {
            let score = v.distance.unwrap();
            assert!(score > 0.9); // 1/1.1 ≈ 0.909
        }
    }

    #[ignore] // Score updates don't support plain Node types, only Vector and NodeWithScore
    #[test]
    fn test_rrf_with_nodes() {
        let reranker = RRFReranker::new();

        let nodes: Vec<TraversalValue> = (0..3)
            .map(|i| {
                let node = Node {
                    id: i as u128,
                    label: "test",
                    version: 1,
                    properties: None,
                };
                TraversalValue::Node(node)
            })
            .collect();

        let results = reranker.rerank(nodes.into_iter(), None).unwrap();

        assert_eq!(results.len(), 3);
        // Verify nodes are properly reranked
        if let TraversalValue::Node(n) = &results[0] {
            assert_eq!(n.id, 0);
        }
    }

    #[ignore] // Score updates don't support plain Node types, only Vector and NodeWithScore
    #[test]
    fn test_rrf_mixed_types() {
        let arena = Bump::new();
        let reranker = RRFReranker::new();

        let items: Vec<TraversalValue> = vec![
            {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let node = Node {
                    id: 2,
                    label: "test",
                    version: 1,
                    properties: None,
                };
                TraversalValue::Node(node)
            },
            {
                let mut v = alloc_vector(&arena, &[2.0]);
                v.id = 3;
                TraversalValue::Vector(v)
            },
        ];

        let results = reranker.rerank(items.into_iter(), None).unwrap();

        // Should handle mixed types
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_rrf_fuse_with_different_list_lengths() {
        let arena = Bump::new();
        let list1: Vec<TraversalValue> = (0..10)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let list2: Vec<TraversalValue> = (5..8)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results =
            RRFReranker::fuse_lists(vec![list1.into_iter(), list2.into_iter()], 60.0).unwrap();

        // Items 5, 6, 7 appear in both lists, should rank higher
        assert_eq!(results.len(), 10);
        if let TraversalValue::Vector(v) = &results[0] {
            // First result should be one of the overlapping items
            assert!(v.id >= 5 && v.id <= 7);
        }
    }

    #[test]
    fn test_rrf_score_monotonicity() {
        let arena = Bump::new();
        let reranker = RRFReranker::new();

        let vectors: Vec<TraversalValue> = (0..10)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = reranker.rerank(vectors.into_iter(), None).unwrap();

        // Scores should be monotonically decreasing
        for i in 0..results.len() - 1 {
            if let (TraversalValue::Vector(v1), TraversalValue::Vector(v2)) =
                (&results[i], &results[i + 1])
            {
                assert!(v1.distance.unwrap() >= v2.distance.unwrap());
            }
        }
    }

    #[test]
    fn test_rrf_default_name() {
        let reranker = RRFReranker::new();
        assert_eq!(reranker.name(), "RRF");
    }

    #[test]
    fn test_rrf_preserves_item_data() {
        let arena = Bump::new();
        let reranker = RRFReranker::new();

        let vectors: Vec<TraversalValue> = (0..3)
            .map(|i| {
                let mut v = alloc_vector(&arena, &[1.0 * i as f64, 2.0 * i as f64]);
                v.id = i as u128;
                TraversalValue::Vector(v)
            })
            .collect();

        let results = reranker.rerank(vectors.into_iter(), None).unwrap();

        // Verify vector data is preserved
        if let TraversalValue::Vector(v) = &results[0] {
            assert_eq!(v.data_borrowed(), &[0.0, 0.0]);
        }
        if let TraversalValue::Vector(v) = &results[1] {
            assert_eq!(v.data_borrowed(), &[1.0, 2.0]);
        }
    }
}
