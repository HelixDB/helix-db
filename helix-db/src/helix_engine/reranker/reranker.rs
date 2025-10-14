// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Core Reranker trait and related types.

use crate::{
    helix_engine::{
        reranker::errors::{RerankerError, RerankerResult},
        traversal_core::traversal_value::TraversalValue,
    },
    utils::filterable::Filterable,
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
    fn rerank<I>(&self, items: I, query: Option<&str>) -> RerankerResult<Vec<TraversalValue>>
    where
        I: Iterator<Item = TraversalValue>;

    /// Get the name of this reranker for debugging/logging
    fn name(&self) -> &str;
}

/// Extract score from a TraversalValue.
///
/// This handles the different types (Node, Edge, Vector) and extracts
/// their associated score/distance value.
pub fn extract_score(item: &TraversalValue) -> RerankerResult<f64> {
    match item {
        TraversalValue::Vector(v) => Ok(v.distance.unwrap_or(0.0)),
        TraversalValue::Node(n) => Ok(n.score()),
        TraversalValue::Edge(e) => Ok(e.score()),
        _ => Err(RerankerError::ScoreExtractionError(
            "Cannot extract score from this traversal value type".to_string(),
        )),
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
        TraversalValue::Node(n) => {
            // Store in properties
            if n.properties.is_none() {
                n.properties = Some(std::collections::HashMap::new());
            }
            if let Some(props) = &mut n.properties {
                props.insert(
                    "rerank_score".to_string(),
                    crate::protocol::value::Value::F64(new_score),
                );
            }
            Ok(())
        }
        TraversalValue::Edge(e) => {
            // Store in properties
            if e.properties.is_none() {
                e.properties = Some(std::collections::HashMap::new());
            }
            if let Some(props) = &mut e.properties {
                props.insert(
                    "rerank_score".to_string(),
                    crate::protocol::value::Value::F64(new_score),
                );
            }
            Ok(())
        }
        _ => Err(RerankerError::ScoreExtractionError(
            "Cannot update score for this traversal value type".to_string(),
        )),
    }
}
