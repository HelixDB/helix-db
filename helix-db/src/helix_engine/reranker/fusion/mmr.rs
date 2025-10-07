// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Maximal Marginal Relevance (MMR) reranker implementation.
//!
//! MMR balances relevance with diversity to reduce redundancy in results.
//! Formula: MMR = λ * Sim1(d, q) - (1-λ) * max(Sim2(d, d_i))
//! where:
//! - Sim1: similarity to query (relevance)
//! - Sim2: similarity to already selected documents (diversity)
//! - λ: trade-off parameter (typically 0.5-0.8)

use crate::{
    helix_engine::{
        reranker::{
            errors::{RerankerError, RerankerResult},
            reranker::{extract_score, update_score, Reranker},
        },
        traversal_core::traversal_value::TraversalValue,
    },
    utils::filterable::Filterable,
};
use std::collections::HashMap;

/// Distance calculation method for MMR.
#[derive(Debug, Clone, Copy)]
pub enum DistanceMethod {
    Cosine,
    Euclidean,
    DotProduct,
}

/// Maximal Marginal Relevance reranker.
///
/// Selects items that maximize the trade-off between relevance and diversity.
#[derive(Debug, Clone)]
pub struct MMRReranker {
    /// Lambda parameter: controls relevance vs diversity trade-off
    /// Higher values (closer to 1.0) favor relevance
    /// Lower values (closer to 0.0) favor diversity
    lambda: f64,

    /// Distance metric for similarity calculation
    distance_method: DistanceMethod,

    /// Optional query vector for relevance calculation
    query_vector: Option<Vec<f64>>,
}

impl MMRReranker {
    /// Create a new MMR reranker with default lambda=0.7 (favoring relevance).
    pub fn new(lambda: f64) -> RerankerResult<Self> {
        if !(0.0..=1.0).contains(&lambda) {
            return Err(RerankerError::InvalidParameter(
                "lambda must be between 0.0 and 1.0".to_string(),
            ));
        }

        Ok(Self {
            lambda,
            distance_method: DistanceMethod::Cosine,
            query_vector: None,
        })
    }

    /// Create an MMR reranker with a custom distance metric.
    pub fn with_distance(lambda: f64, distance_method: DistanceMethod) -> RerankerResult<Self> {
        if !(0.0..=1.0).contains(&lambda) {
            return Err(RerankerError::InvalidParameter(
                "lambda must be between 0.0 and 1.0".to_string(),
            ));
        }

        Ok(Self {
            lambda,
            distance_method,
            query_vector: None,
        })
    }

    /// Set the query vector for relevance calculation.
    pub fn with_query_vector(mut self, query: Vec<f64>) -> Self {
        self.query_vector = Some(query);
        self
    }

    /// Extract vector data from a TraversalValue.
    fn extract_vector_data<'a>(&self, item: &'a TraversalValue) -> RerankerResult<&'a [f64]> {
        match item {
            TraversalValue::Vector(v) => Ok(&v.data),
            TraversalValue::Node(n) => Ok(n.vector_data()),
            TraversalValue::Edge(e) => Ok(e.vector_data()),
            _ => Err(RerankerError::TextExtractionError(
                "Cannot extract vector from this item type".to_string(),
            )),
        }
    }

    /// Calculate similarity between two items.
    fn calculate_similarity(&self, item1: &[f64], item2: &[f64]) -> RerankerResult<f64> {
        if item1.len() != item2.len() {
            return Err(RerankerError::InvalidParameter(
                "Vector dimensions must match".to_string(),
            ));
        }

        let distance = match self.distance_method {
            DistanceMethod::Cosine => {
                // Calculate cosine similarity (1 - cosine distance)
                let dot_product: f64 = item1.iter().zip(item2.iter()).map(|(a, b)| a * b).sum();
                let norm1: f64 = item1.iter().map(|x| x * x).sum::<f64>().sqrt();
                let norm2: f64 = item2.iter().map(|x| x * x).sum::<f64>().sqrt();

                if norm1 == 0.0 || norm2 == 0.0 {
                    0.0
                } else {
                    dot_product / (norm1 * norm2)
                }
            }
            DistanceMethod::Euclidean => {
                // Convert Euclidean distance to similarity (using negative exponential)
                let dist_sq: f64 = item1
                    .iter()
                    .zip(item2.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                (-dist_sq.sqrt()).exp()
            }
            DistanceMethod::DotProduct => {
                // Dot product as similarity
                item1.iter().zip(item2.iter()).map(|(a, b)| a * b).sum()
            }
        };

        Ok(distance)
    }

    /// Perform MMR selection on the given items.
    fn mmr_select(&self, items: Vec<TraversalValue>) -> RerankerResult<Vec<TraversalValue>> {
        if items.is_empty() {
            return Err(RerankerError::EmptyInput);
        }

        let n = items.len();
        let mut selected: Vec<TraversalValue> = Vec::with_capacity(n);
        let mut remaining: Vec<(TraversalValue, f64)> = Vec::with_capacity(n);

        // Extract original scores and prepare remaining items
        for item in items {
            let score = extract_score(&item)?;
            remaining.push((item, score));
        }

        // Cache for similarity calculations
        let mut similarity_cache: HashMap<(usize, usize), f64> = HashMap::new();

        // Select first item (highest original score)
        remaining.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let first = remaining.remove(0);
        selected.push(first.0);

        // Iteratively select remaining items
        while !remaining.is_empty() {
            let mut best_idx = 0;
            let mut best_mmr_score = f64::NEG_INFINITY;

            for (idx, (item, relevance_score)) in remaining.iter().enumerate() {
                let item_vec = self.extract_vector_data(item)?;

                // Calculate relevance term
                let relevance = if let Some(query) = &self.query_vector {
                    self.calculate_similarity(item_vec, query)?
                } else {
                    *relevance_score // Use original score as relevance
                };

                // Calculate diversity term (max similarity to selected items)
                let mut max_similarity: f64 = 0.0;
                for (sel_idx, selected_item) in selected.iter().enumerate() {
                    // Check cache first
                    let cache_key = (idx, sel_idx);
                    let similarity = if let Some(&cached) = similarity_cache.get(&cache_key) {
                        cached
                    } else {
                        let sel_vec = self.extract_vector_data(selected_item)?;
                        let sim = self.calculate_similarity(item_vec, sel_vec)?;
                        similarity_cache.insert(cache_key, sim);
                        sim
                    };

                    max_similarity = max_similarity.max(similarity);
                }

                // Calculate MMR score
                let mmr_score = self.lambda * relevance - (1.0 - self.lambda) * max_similarity;

                if mmr_score > best_mmr_score {
                    best_mmr_score = mmr_score;
                    best_idx = idx;
                }
            }

            // Add the best item to selected
            let (mut best_item, _) = remaining.remove(best_idx);
            update_score(&mut best_item, best_mmr_score)?;
            selected.push(best_item);
        }

        Ok(selected)
    }
}

impl Reranker for MMRReranker {
    fn rerank<I>(&self, items: I, _query: Option<&str>) -> RerankerResult<Vec<TraversalValue>>
    where
        I: Iterator<Item = TraversalValue>,
    {
        let items_vec: Vec<_> = items.collect();
        self.mmr_select(items_vec)
    }

    fn name(&self) -> &str {
        "MMR"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helix_engine::vector_core::vector::HVector;

    #[test]
    fn test_mmr_creation() {
        let mmr = MMRReranker::new(0.7).unwrap();
        assert_eq!(mmr.lambda, 0.7);

        let mmr_invalid = MMRReranker::new(1.5);
        assert!(mmr_invalid.is_err());

        let mmr_invalid = MMRReranker::new(-0.1);
        assert!(mmr_invalid.is_err());
    }

    #[test]
    fn test_mmr_diversity() {
        let mmr = MMRReranker::new(0.5).unwrap(); // Equal weight to relevance and diversity

        // Create vectors: two very similar, one different
        let vectors: Vec<TraversalValue> = vec![
            {
                let mut v = HVector::new(vec![1.0, 0.0, 0.0]);
                v.distance = Some(0.9);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = HVector::new(vec![0.99, 0.01, 0.0]); // Very similar to first
                v.distance = Some(0.85);
                v.id = 2;
                TraversalValue::Vector(v)
            },
            {
                let mut v = HVector::new(vec![0.0, 1.0, 0.0]); // Different
                v.distance = Some(0.7);
                v.id = 3;
                TraversalValue::Vector(v)
            },
        ];

        let results = mmr.rerank(vectors.into_iter(), None).unwrap();

        assert_eq!(results.len(), 3);

        // First should be the highest scored (id=1)
        if let TraversalValue::Vector(v) = &results[0] {
            assert_eq!(v.id, 1);
        }

        // Second should be the diverse one (id=3), not the similar one (id=2)
        // because MMR should prefer diversity
        if let TraversalValue::Vector(v) = &results[1] {
            assert_eq!(v.id, 3);
        }
    }

    #[test]
    fn test_mmr_high_lambda_favors_relevance() {
        let mmr = MMRReranker::new(0.99).unwrap(); // Strongly favor relevance

        let vectors: Vec<TraversalValue> = vec![
            {
                let mut v = HVector::new(vec![1.0, 0.0]);
                v.distance = Some(1.0);
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = HVector::new(vec![0.99, 0.01]); // Similar but lower score
                v.distance = Some(0.9);
                v.id = 2;
                TraversalValue::Vector(v)
            },
            {
                let mut v = HVector::new(vec![0.0, 1.0]); // Different but much lower score
                v.distance = Some(0.5);
                v.id = 3;
                TraversalValue::Vector(v)
            },
        ];

        let results = mmr.rerank(vectors.into_iter(), None).unwrap();

        // With high lambda, should maintain roughly original order by relevance
        if let TraversalValue::Vector(v) = &results[0] {
            assert_eq!(v.id, 1);
        }
        if let TraversalValue::Vector(v) = &results[1] {
            assert_eq!(v.id, 2); // Similar item selected second despite similarity
        }
    }

    #[test]
    fn test_mmr_empty_input() {
        let mmr = MMRReranker::new(0.7).unwrap();
        let empty: Vec<TraversalValue> = vec![];
        let result = mmr.rerank(empty.into_iter(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_mmr_with_query_vector() {
        let query = vec![1.0, 0.0, 0.0];
        let mmr = MMRReranker::new(0.7)
            .unwrap()
            .with_query_vector(query);

        let vectors: Vec<TraversalValue> = vec![
            {
                let mut v = HVector::new(vec![0.9, 0.1, 0.0]);
                v.distance = Some(0.9); // Higher original score
                v.id = 1;
                TraversalValue::Vector(v)
            },
            {
                let mut v = HVector::new(vec![0.1, 0.9, 0.0]);
                v.distance = Some(0.5); // Lower original score
                v.id = 2;
                TraversalValue::Vector(v)
            },
        ];

        let results = mmr.rerank(vectors.into_iter(), None).unwrap();

        // MMR first selects highest scored item (id=1)
        // With query vector [1,0,0], id=1 with vector [0.9,0.1,0] is also more similar
        if let TraversalValue::Vector(v) = &results[0] {
            assert_eq!(v.id, 1);
        }

        // Verify we got all items
        assert_eq!(results.len(), 2);
    }
}
