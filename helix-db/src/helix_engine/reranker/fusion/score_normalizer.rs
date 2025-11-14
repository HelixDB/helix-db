// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Score normalization utilities for reranking.

use crate::helix_engine::reranker::errors::{RerankerError, RerankerResult};

/// Normalization strategies for score ranges.
#[derive(Debug, Clone, Copy)]
pub enum NormalizationMethod {
    /// Min-Max normalization: (x - min) / (max - min)
    MinMax,
    /// Z-score normalization: (x - mean) / stddev
    ZScore,
    /// No normalization
    None,
}

/// Normalize a list of scores using the specified method.
///
/// # Arguments
/// * `scores` - Slice of scores to normalize
/// * `method` - Normalization method to use
///
/// # Returns
/// A vector of normalized scores in the range [0, 1] for MinMax,
/// or z-scores for ZScore method.
pub fn normalize_scores(scores: &[f64], method: NormalizationMethod) -> RerankerResult<Vec<f64>> {
    if scores.is_empty() {
        return Err(RerankerError::EmptyInput);
    }

    match method {
        NormalizationMethod::MinMax => normalize_minmax(scores),
        NormalizationMethod::ZScore => normalize_zscore(scores),
        NormalizationMethod::None => Ok(scores.to_vec()),
    }
}

/// Min-Max normalization: scales scores to [0, 1] range.
fn normalize_minmax(scores: &[f64]) -> RerankerResult<Vec<f64>> {
    let min = scores.iter().fold(f64::INFINITY, |a, &b| a.min(b));
    let max = scores.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

    let range = max - min;

    if range == 0.0 {
        // All scores are the same, return all 0.5
        return Ok(vec![0.5; scores.len()]);
    }

    Ok(scores.iter().map(|&score| (score - min) / range).collect())
}

/// Z-score normalization: centers scores around mean with unit variance.
fn normalize_zscore(scores: &[f64]) -> RerankerResult<Vec<f64>> {
    let n = scores.len() as f64;
    let mean = scores.iter().sum::<f64>() / n;

    let variance = scores
        .iter()
        .map(|&score| (score - mean).powi(2))
        .sum::<f64>()
        / n;

    let stddev = variance.sqrt();

    if stddev == 0.0 {
        // All scores are the same, return all zeros
        return Ok(vec![0.0; scores.len()]);
    }

    Ok(scores
        .iter()
        .map(|&score| (score - mean) / stddev)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minmax_normalization() {
        let scores = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let normalized = normalize_scores(&scores, NormalizationMethod::MinMax).unwrap();

        assert_eq!(normalized[0], 0.0);
        assert_eq!(normalized[4], 1.0);
        assert!((normalized[2] - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_minmax_same_scores() {
        let scores = vec![5.0, 5.0, 5.0];
        let normalized = normalize_scores(&scores, NormalizationMethod::MinMax).unwrap();

        assert!(normalized.iter().all(|&x| x == 0.5));
    }

    #[test]
    fn test_zscore_normalization() {
        let scores = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let normalized = normalize_scores(&scores, NormalizationMethod::ZScore).unwrap();

        // Mean should be close to 0
        let mean: f64 = normalized.iter().sum::<f64>() / normalized.len() as f64;
        assert!(mean.abs() < 1e-10);
    }

    #[test]
    fn test_empty_scores() {
        let scores: Vec<f64> = vec![];
        let result = normalize_scores(&scores, NormalizationMethod::MinMax);

        assert!(result.is_err());
    }
}
