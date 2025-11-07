// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Error types for reranker operations.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RerankerError {
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Empty input provided to reranker")]
    EmptyInput,

    #[error("Score extraction failed: {0}")]
    ScoreExtractionError(String),

    #[error("Model error: {0}")]
    ModelError(String),

    #[error("External API error: {0}")]
    ExternalApiError(String),

    #[error("Text extraction failed: {0}")]
    TextExtractionError(String),

    #[error("Batch processing error: {0}")]
    BatchProcessingError(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}

pub type RerankerResult<T> = Result<T, RerankerError>;
