use crate::helix_engine::{
    traversal_core::config::SimilarityMethod, types::VectorError, vector_core::vector::HVector,
};

pub const MAX_DISTANCE: f64 = 2.0;
pub const ORTHOGONAL: f64 = 1.0;
pub const MIN_DISTANCE: f64 = 0.0;

pub trait DistanceCalc {
    fn distance(from: &[f64], to: &[f64], method: &SimilarityMethod) -> Result<f64, VectorError>;
}
impl DistanceCalc for HVector {
    /// Calculates the distance between two vectors.
    ///
    /// It normalizes the distance to be between 0 and 2.
    ///
    /// - 1.0 (most similar) → Distance 0.0 (closest)
    /// - 0.0 (orthogonal) → Distance 1.0
    /// - -1.0 (most dissimilar) → Distance 2.0 (furthest)
    #[inline(always)]
    fn distance(from: &[f64], to: &[f64], method: &SimilarityMethod) -> Result<f64, VectorError> {
        match method {
            SimilarityMethod::CosineDistance => cosine_similarity(&from, &to).map(|sim| 1.0 - sim),
            SimilarityMethod::CosineSimilarity => cosine_similarity(&from, &to),
            SimilarityMethod::EuclideanDistance => euclidean_distance(&from, &to),
        }
    }
}

#[inline]
pub fn cosine_similarity(from: &[f64], to: &[f64]) -> Result<f64, VectorError> {
    let len = from.len();
    let other_len = to.len();

    if len != other_len {
        println!("mis-match in vector dimensions!\n{len} != {other_len}");
        return Err(VectorError::InvalidVectorLength);
    }
    //debug_assert_eq!(len, other.data.len(), "Vectors must have the same length");

    #[cfg(target_feature = "avx2")]
    {
        return cosine_similarity_avx2(from, to);
    }

    let mut dot_product = 0.0;
    let mut magnitude_a = 0.0;
    let mut magnitude_b = 0.0;

    const CHUNK_SIZE: usize = 8;
    let chunks = len / CHUNK_SIZE;
    let remainder = len % CHUNK_SIZE;

    for i in 0..chunks {
        let offset = i * CHUNK_SIZE;
        let a_chunk = &from[offset..offset + CHUNK_SIZE];
        let b_chunk = &to[offset..offset + CHUNK_SIZE];

        let mut local_dot = 0.0;
        let mut local_mag_a = 0.0;
        let mut local_mag_b = 0.0;

        for j in 0..CHUNK_SIZE {
            let a_val = a_chunk[j];
            let b_val = b_chunk[j];
            local_dot += a_val * b_val;
            local_mag_a += a_val * a_val;
            local_mag_b += b_val * b_val;
        }

        dot_product += local_dot;
        magnitude_a += local_mag_a;
        magnitude_b += local_mag_b;
    }

    let remainder_offset = chunks * CHUNK_SIZE;
    for i in 0..remainder {
        let a_val = from[remainder_offset + i];
        let b_val = to[remainder_offset + i];
        dot_product += a_val * b_val;
        magnitude_a += a_val * a_val;
        magnitude_b += b_val * b_val;
    }

    if magnitude_a.abs() == 0.0 || magnitude_b.abs() == 0.0 {
        return Ok(-1.0);
    }

    Ok(dot_product / (magnitude_a.sqrt() * magnitude_b.sqrt()))
}

// SIMD implementation using AVX2 (256-bit vectors)
#[cfg(target_feature = "avx2")]
#[inline(always)]
pub fn cosine_similarity_avx2(a: &[f64], b: &[f64]) -> f64 {
    use std::arch::x86_64::*;

    let len = a.len();
    let chunks = len / 4; // AVX2 processes 4 f64 values at once

    unsafe {
        let mut dot_product = _mm256_setzero_pd();
        let mut magnitude_a = _mm256_setzero_pd();
        let mut magnitude_b = _mm256_setzero_pd();

        for i in 0..chunks {
            let offset = i * 4;

            // Load data - handle unaligned data
            let a_chunk = _mm256_loadu_pd(&a[offset]);
            let b_chunk = _mm256_loadu_pd(&b[offset]);

            // Calculate dot product and magnitudes in parallel
            dot_product = _mm256_add_pd(dot_product, _mm256_mul_pd(a_chunk, b_chunk));
            magnitude_a = _mm256_add_pd(magnitude_a, _mm256_mul_pd(a_chunk, a_chunk));
            magnitude_b = _mm256_add_pd(magnitude_b, _mm256_mul_pd(b_chunk, b_chunk));
        }

        // Horizontal sum of 4 doubles in each vector
        let dot_sum = horizontal_sum_pd(dot_product);
        let mag_a_sum = horizontal_sum_pd(magnitude_a);
        let mag_b_sum = horizontal_sum_pd(magnitude_b);

        // Handle remainder elements
        let mut dot_remainder = 0.0;
        let mut mag_a_remainder = 0.0;
        let mut mag_b_remainder = 0.0;

        let remainder_offset = chunks * 4;
        for i in remainder_offset..len {
            let a_val = a[i];
            let b_val = b[i];
            dot_remainder += a_val * b_val;
            mag_a_remainder += a_val * a_val;
            mag_b_remainder += b_val * b_val;
        }

        // Combine SIMD and scalar results
        let dot_product_total = dot_sum + dot_remainder;
        let magnitude_a_total = (mag_a_sum + mag_a_remainder).sqrt();
        let magnitude_b_total = (mag_b_sum + mag_b_remainder).sqrt();

        dot_product_total / (magnitude_a_total * magnitude_b_total)
    }
}

// Helper function to sum the 4 doubles in an AVX2 vector
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn horizontal_sum_pd(__v: __m256d) -> f64 {
    use std::arch::x86_64::*;

    // Extract the high 128 bits and add to the low 128 bits
    let sum_hi_lo = _mm_add_pd(_mm256_castpd256_pd128(__v), _mm256_extractf128_pd(__v, 1));

    // Add the high 64 bits to the low 64 bits
    let sum = _mm_add_sd(sum_hi_lo, _mm_unpackhi_pd(sum_hi_lo, sum_hi_lo));

    // Extract the low 64 bits as a scalar
    _mm_cvtsd_f64(sum)
}

pub fn euclidean_distance(from: &[f64], to: &[f64]) -> Result<f64, VectorError> {
    if from.len() != to.len() {
        return Err(VectorError::InvalidVectorLength);
    }
    Ok(from
        .iter()
        .zip(to.iter())
        .map(|(x, y)| {
            let c = x - y;
            c * c
        })
        .sum())
}
