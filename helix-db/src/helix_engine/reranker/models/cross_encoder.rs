// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Cross-encoder reranker base implementation.
//!
//! Cross-encoders jointly encode query-document pairs for more accurate
//! relevance scoring compared to bi-encoders (separate embeddings).
//!
//! This module provides the foundation for cross-encoder reranking.
//! Actual model implementations (local ONNX/Candle, external APIs) can be
//! added as features are expanded.

use crate::helix_engine::reranker::{
    errors::{RerankerError, RerankerResult},
    reranker::{Reranker, update_score},
};
use crate::helix_engine::traversal_core::traversal_value::TraversalValue;

/// Configuration for cross-encoder reranking.
#[derive(Debug, Clone)]
pub struct CrossEncoderConfig {
    /// Model identifier (e.g., "bge-reranker-base")
    pub model_name: String,

    /// Batch size for processing
    pub batch_size: usize,

    /// Maximum sequence length
    pub max_length: usize,

    /// API endpoint for external models (optional)
    pub api_endpoint: Option<String>,

    /// API key for external models (optional)
    pub api_key: Option<String>,
}

impl CrossEncoderConfig {
    pub fn new(model_name: impl Into<String>) -> Self {
        Self {
            model_name: model_name.into(),
            batch_size: 32,
            max_length: 512,
            api_endpoint: None,
            api_key: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_max_length(mut self, max_length: usize) -> Self {
        self.max_length = max_length;
        self
    }

    pub fn with_api(mut self, endpoint: String, api_key: Option<String>) -> Self {
        self.api_endpoint = Some(endpoint);
        self.api_key = api_key;
        self
    }
}

/// Cross-encoder reranker (base implementation).
///
/// This struct provides the framework for cross-encoder reranking.
/// Concrete implementations will be added for:
/// - Local models (ONNX, Candle)
/// - External APIs (Cohere, Voyage, etc.)
#[derive(Debug)]
pub struct CrossEncoderReranker {
    pub config: CrossEncoderConfig,
}

impl CrossEncoderReranker {
    pub fn new(_config: CrossEncoderConfig) -> Self {
        todo!();
    }

    /// Extract text from a TraversalValue for reranking.
    ///
    /// This tries to extract meaningful text from the item's properties.
    /// Common property names like "text", "content", "description" are checked.
    fn extract_text(&self, item: &TraversalValue) -> RerankerResult<String> {
        let properties = match item {
            TraversalValue::Node(n) => n.properties,
            TraversalValue::Edge(e) => e.properties,
            TraversalValue::Vector(v) => v.properties,
            TraversalValue::NodeWithScore { node, .. } => node.properties,
            _ => None,
        };

        if let Some(props) = properties {
            // Try common text field names
            for field in &["text", "content", "description", "body", "title"] {
                if let Some(value) = props.get(field)
                    && let crate::protocol::value::Value::String(text) = value
                {
                    return Ok(text.to_string());
                }
            }

            // If no standard field found, try to concatenate all string values
            let mut texts = Vec::new();
            for (_, value) in props.iter() {
                if let crate::protocol::value::Value::String(text) = value {
                    texts.push(text.as_str());
                }
            }

            if !texts.is_empty() {
                return Ok(texts.join(" "));
            }
        }

        Err(RerankerError::TextExtractionError(
            "No text fields found in item properties".to_string(),
        ))
    }

    /// Score a query-document pair using the cross-encoder model.
    ///
    /// This is a placeholder for actual model inference.
    /// TODO: Implement actual model loading and inference.
    fn score_pair(&self, _query: &str, _document: &str) -> RerankerResult<f32> {
        todo!();
    }
}

impl Reranker for CrossEncoderReranker {
    fn rerank<'arena, I>(
        &self,
        items: I,
        query: Option<&str>,
    ) -> RerankerResult<Vec<TraversalValue<'arena>>>
    where
        I: Iterator<Item = TraversalValue<'arena>>,
    {
        let query_text = query.ok_or_else(|| {
            RerankerError::InvalidParameter("Cross-encoder reranking requires a query".to_string())
        })?;

        let items_vec: Vec<_> = items.collect();

        if items_vec.is_empty() {
            return Err(RerankerError::EmptyInput);
        }

        let mut scored_items = Vec::with_capacity(items_vec.len());

        // Extract texts and score in batches
        for mut item in items_vec {
            let text = self.extract_text(&item)?;
            let score = self.score_pair(query_text, &text)?;
            update_score(&mut item, score)?;
            scored_items.push((score, item));
        }

        // Sort by score (descending)
        scored_items.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(scored_items.into_iter().map(|(_, item)| item).collect())
    }

    fn name(&self) -> &str {
        "CrossEncoder"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helix_engine::vector_core::HVector;
    use bumpalo::Bump;

    fn alloc_vector<'a>(arena: &'a Bump, data: &[f32]) -> HVector<'a> {
        let mut bump_vec = bumpalo::collections::Vec::new_in(arena);
        bump_vec.extend_from_slice(data);
        HVector::from_vec("test_vector", bump_vec)
    }

    #[ignore]
    #[test]
    fn test_cross_encoder_config() {
        let config = CrossEncoderConfig::new("test-model")
            .with_batch_size(16)
            .with_max_length(256);

        assert_eq!(config.model_name, "test-model");
        assert_eq!(config.batch_size, 16);
        assert_eq!(config.max_length, 256);
    }

    #[ignore]
    #[test]
    fn test_text_extraction() {
        let _arena = Bump::new();
        let _reranker = CrossEncoderReranker::new(CrossEncoderConfig::new("test"));

        // Note: This test is ignored because CrossEncoderReranker::new uses todo!()
        // and creating ImmutablePropertiesMap requires arena-allocated data structures.
        // When the actual implementation is ready, this test should be updated to properly
        // create a vector with properties using the arena.
    }

    #[ignore]
    #[test]
    fn test_text_extraction_no_text() {
        let arena = Bump::new();
        let reranker = CrossEncoderReranker::new(CrossEncoderConfig::new("test"));

        let v = alloc_vector(&arena, &[1.0, 2.0]);
        let item = TraversalValue::Vector(v);

        let result = reranker.extract_text(&item);
        assert!(result.is_err());
    }

    #[ignore]
    #[test]
    fn test_rerank_without_query() {
        let arena = Bump::new();
        let config = CrossEncoderConfig::new("test-model");
        let reranker = CrossEncoderReranker::new(config);

        let vectors: Vec<TraversalValue> =
            vec![TraversalValue::Vector(alloc_vector(&arena, &[1.0]))];

        let result = reranker.rerank(vectors.into_iter(), None);
        assert!(result.is_err());
    }
}
