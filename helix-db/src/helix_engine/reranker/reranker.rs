// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Core Reranker trait and related types.

use crate::helix_engine::{
    reranker::errors::{RerankerError, RerankerResult},
    traversal_core::traversal_value::TraversalValue,
};

/// Represents a scored item for reranking.
#[derive(Debug, Clone)]
pub struct ScoredItem<T> {
    pub item: T,
    pub score: f64,
    pub original_rank: usize,
}

impl<T> ScoredItem<T> {
    pub fn new(item: T, score: f64, rank: usize) -> Self {
        Self {
            item,
            score,
            original_rank: rank,
        }
    }
}

/// Core trait for reranking operations.
///
/// This trait defines the interface for different reranking strategies
/// (RRF, MMR, Cross-Encoder, etc.) to operate on traversal values.
pub trait Reranker: Send + Sync {
    /// Rerank a list of items with their original scores.
    ///
    /// # Arguments
    /// * `items` - Iterator of items to rerank
    /// * `query` - Optional query context for relevance-based reranking
    ///
    /// # Returns
    /// A vector of reranked items with updated scores
    fn rerank<'arena, I>(
        &self,
        items: I,
        query: Option<&str>,
    ) -> RerankerResult<Vec<TraversalValue<'arena>>>
    where
        I: Iterator<Item = TraversalValue<'arena>>;

    /// Get the name of this reranker for debugging/logging
    fn name(&self) -> &str;
}

/// Extract score from a TraversalValue.
///
/// This handles the different types (Node, Edge, Vector) and extracts
/// their associated score/distance value.
pub fn extract_score(item: &TraversalValue) -> RerankerResult<f64> {
    match item {
        TraversalValue::Vector(v) => Ok(v.score()),
        TraversalValue::NodeWithScore { score, .. } => Ok(*score),
        _ => {
            // For nodes and edges without explicit scores, try to extract from properties
            // or return a default score of 0.0
            Ok(0.0)
        }
    }
}

/// Update the score of a TraversalValue.
///
/// This modifies the distance/score field of the item to reflect
/// the new reranked score.
pub fn update_score(item: &mut TraversalValue, new_score: f64) -> RerankerResult<()> {
    match item {
        TraversalValue::Vector(v) => {
            v.distance = Some(new_score);
            Ok(())
        }
        TraversalValue::NodeWithScore { score, .. } => {
            *score = new_score;
            Ok(())
        }
        // Note: Node and Edge have ImmutablePropertiesMap which cannot be modified
        // For now, we cannot update scores on plain Node/Edge items
        // They would need to be wrapped in NodeWithScore variant
        _ => Err(RerankerError::ScoreExtractionError(
            "Cannot update score for this traversal value type (only Vector and NodeWithScore supported)".to_string(),
        )),
    }
}
