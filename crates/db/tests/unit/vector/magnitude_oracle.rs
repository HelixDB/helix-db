//! Independent test-only numeric oracle for active f32 vector metrics.

use crate::search::vector::VectorDistanceMetric;

/// Returns the inclusive component limit proposed for one active metric.
pub(crate) fn inclusive_limit(metric: VectorDistanceMetric, dimension: usize) -> Option<f32> {
    assert!(dimension > 0, "oracle dimensions are non-zero");
    let factor = match metric {
        VectorDistanceMetric::Cosine => return None,
        VectorDistanceMetric::Euclidean => 8_u64,
        VectorDistanceMetric::Manhattan => 4_u64,
    };
    let dimension = u64::try_from(dimension).expect("oracle dimension fits u64");
    let divisor = dimension
        .checked_mul(factor)
        .expect("oracle divisor arithmetic remains bounded");
    let exact = match metric {
        VectorDistanceMetric::Euclidean => (f64::from(f32::MAX) / divisor as f64).sqrt(),
        VectorDistanceMetric::Manhattan => f64::from(f32::MAX) / divisor as f64,
        VectorDistanceMetric::Cosine => unreachable!("cosine returned above"),
    };
    let rounded = exact as f32;
    let downward = if f64::from(rounded) > exact {
        f32::from_bits(
            rounded
                .to_bits()
                .checked_sub(1)
                .expect("positive finite f32 has a predecessor"),
        )
    } else {
        rounded
    };
    assert!(downward.is_finite() && downward > 0.0);
    assert!(f64::from(downward) <= exact);
    Some(downward)
}

/// Returns the exact unrounded f64 limit for rounding assertions.
pub(crate) fn exact_limit(metric: VectorDistanceMetric, dimension: usize) -> Option<f64> {
    assert!(dimension > 0, "oracle dimensions are non-zero");
    let factor = match metric {
        VectorDistanceMetric::Cosine => return None,
        VectorDistanceMetric::Euclidean => 8_u64,
        VectorDistanceMetric::Manhattan => 4_u64,
    };
    let dimension = u64::try_from(dimension).expect("oracle dimension fits u64");
    let divisor = dimension
        .checked_mul(factor)
        .expect("oracle divisor arithmetic remains bounded");
    Some(match metric {
        VectorDistanceMetric::Euclidean => (f64::from(f32::MAX) / divisor as f64).sqrt(),
        VectorDistanceMetric::Manhattan => f64::from(f32::MAX) / divisor as f64,
        VectorDistanceMetric::Cosine => unreachable!("cosine returned above"),
    })
}

/// Returns the next representable f32 greater than one positive finite value.
pub(crate) fn next_up(value: f32) -> f32 {
    assert!(value.is_finite() && value > 0.0);
    f32::from_bits(
        value
            .to_bits()
            .checked_add(1)
            .expect("positive finite f32 has a successor"),
    )
}

/// Reports whether every component is inside the metric-specific oracle domain.
pub(crate) fn accepts(metric: VectorDistanceMetric, dimension: usize, vector: &[f32]) -> bool {
    if vector.len() != dimension || vector.iter().any(|value| !value.is_finite()) {
        return false;
    }
    let Some(limit) = inclusive_limit(metric, dimension) else {
        return true;
    };
    vector.iter().all(|value| value.abs() <= limit)
}

/// Computes the descriptor-defined squared-Euclidean score in f64.
pub(crate) fn squared_euclidean(left: &[f32], right: &[f32]) -> f64 {
    assert_eq!(left.len(), right.len());
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let difference = f64::from(*left) - f64::from(*right);
            difference * difference
        })
        .sum()
}

/// Computes Manhattan distance in f64.
pub(crate) fn manhattan(left: &[f32], right: &[f32]) -> f64 {
    assert_eq!(left.len(), right.len());
    left.iter()
        .zip(right)
        .map(|(left, right)| (f64::from(*left) - f64::from(*right)).abs())
        .sum()
}
