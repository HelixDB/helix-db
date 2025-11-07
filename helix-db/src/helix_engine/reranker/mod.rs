// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Reranking module for HelixDB.
//!
//! This module provides reranking capabilities for search results, including:
//! - RRF (Reciprocal Rank Fusion): Combine multiple ranked lists
//! - MMR (Maximal Marginal Relevance): Balance relevance with diversity
//! - Cross-Encoder: More accurate relevance scoring (base structure for future implementation)
//!
//! # Usage
//!
//! Rerankers can be chained into traversal pipelines:
//!
//! ```ignore
//! use helix_db::helix_engine::reranker::fusion::{RRFReranker, MMRReranker};
//!
//! // RRF: Combine multiple search results
//! let rrf = RRFReranker::new();
//! let vec_results = storage.search_v(query_vec, 100, "doc", None);
//! let bm25_results = storage.search_bm25("doc", query_text, 100)?;
//! let fused = RRFReranker::fuse_lists(vec![vec_results, bm25_results], 60.0)?;
//!
//! // MMR: Diversify results
//! let diverse_results = storage.search_v(query_vec, 100, "doc", None)
//!     .rerank(MMRReranker::new(0.7)?, None) // 70% relevance, 30% diversity
//!     .take(20)
//!     .collect_to::<Vec<_>>();
//! ```

pub mod adapters;
pub mod errors;
pub mod fusion;
pub mod models;
pub mod reranker;

pub use adapters::RerankAdapter;
pub use errors::{RerankerError, RerankerResult};
pub use fusion::{MMRReranker, RRFReranker};
pub use models::CrossEncoderConfig;
pub use reranker::Reranker;
