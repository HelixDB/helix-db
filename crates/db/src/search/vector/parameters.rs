//! Validated numeric contracts used by vector configuration and search.
//!
//! ```
//! use db::search::vector::{
//!     CollisionThreshold, Connections, ConstructionBeamWidth, DistanceScore,
//!     FailureProbability, LayerMultiplier, ResultCount, SearchBeamWidth, UnitInterval,
//! };
//! use std::num::NonZeroUsize;
//!
//! let connections = Connections::try_new(16)?;
//! assert_eq!(connections.checked_double()?.get(), 32);
//! assert_eq!(ConstructionBeamWidth::try_new(200, connections)?.get(), 200);
//! let result_count = ResultCount::try_new(10)?;
//! assert_eq!(SearchBeamWidth::try_new(100, result_count)?.get(), 100);
//! assert_eq!(LayerMultiplier::try_new(0.5)?.get(), 0.5);
//! assert_eq!(UnitInterval::try_new(0.8)?.get(), 0.8);
//! assert_eq!(FailureProbability::try_new(0.1)?.get(), 0.1);
//! assert_eq!(CollisionThreshold::try_new(43, NonZeroUsize::new(64).unwrap())?.get(), 43);
//! assert_eq!(DistanceScore::try_new(0.25)?.get(), 0.25);
//! # Ok::<(), db::search::vector::VectorParameterError>(())
//! ```

use core::cmp::Ordering;
use std::num::NonZeroUsize;

/// A non-zero HNSW connection limit for upper layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Connections(NonZeroUsize);

impl Connections {
    /// Validate an upper-layer connection limit.
    pub fn try_new(value: usize) -> Result<Self, VectorParameterError> {
        non_zero(value, "connections").map(Self)
    }

    /// Return the validated connection count.
    pub const fn get(self) -> usize {
        self.0.get()
    }

    /// Derive the conventional layer-0 limit of twice the upper-layer limit.
    pub fn checked_double(self) -> Result<Layer0Connections, VectorParameterError> {
        let Some(value) = self.get().checked_mul(2) else {
            return Err(VectorParameterError::ArithmeticOverflow {
                parameter: "layer-0 connections",
            });
        };
        Layer0Connections::try_new(value, self)
    }
}

/// A non-zero layer-0 connection limit that is at least the upper-layer limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Layer0Connections(NonZeroUsize);

impl Layer0Connections {
    /// Validate an explicit layer-0 connection limit.
    pub fn try_new(value: usize, connections: Connections) -> Result<Self, VectorParameterError> {
        let value = non_zero(value, "layer-0 connections")?;
        if value.get() < connections.get() {
            return Err(VectorParameterError::BelowMinimum {
                parameter: "layer-0 connections",
                minimum: connections.get(),
                actual: value.get(),
            });
        }
        Ok(Self(value))
    }

    /// Return the validated layer-0 connection count.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// A non-zero HNSW construction beam width that is at least `m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstructionBeamWidth(NonZeroUsize);

impl ConstructionBeamWidth {
    /// Validate a construction beam width against the connection limit.
    pub fn try_new(value: usize, connections: Connections) -> Result<Self, VectorParameterError> {
        let value = non_zero(value, "construction beam width")?;
        if value.get() < connections.get() {
            return Err(VectorParameterError::BelowMinimum {
                parameter: "construction beam width",
                minimum: connections.get(),
                actual: value.get(),
            });
        }
        Ok(Self(value))
    }

    /// Return the validated construction beam width.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// A non-zero requested result count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResultCount(NonZeroUsize);

impl ResultCount {
    /// Validate a requested result count.
    pub fn try_new(value: usize) -> Result<Self, VectorParameterError> {
        non_zero(value, "result count").map(Self)
    }

    /// Return the validated result count.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// A non-zero search beam width that is at least the requested result count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SearchBeamWidth(NonZeroUsize);

impl SearchBeamWidth {
    /// Validate a search beam width against the requested result count.
    pub fn try_new(value: usize, result_count: ResultCount) -> Result<Self, VectorParameterError> {
        let value = non_zero(value, "search beam width")?;
        if value.get() < result_count.get() {
            return Err(VectorParameterError::BelowMinimum {
                parameter: "search beam width",
                minimum: result_count.get(),
                actual: value.get(),
            });
        }
        Ok(Self(value))
    }

    /// Return the validated search beam width.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// A finite, positive HNSW layer multiplier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LayerMultiplier(f32);

impl LayerMultiplier {
    /// Validate a layer multiplier.
    pub fn try_new(value: f32) -> Result<Self, VectorParameterError> {
        if !value.is_finite() {
            return Err(VectorParameterError::NonFinite {
                parameter: "layer multiplier",
            });
        }
        if value <= 0.0 {
            return Err(VectorParameterError::NotPositive {
                parameter: "layer multiplier",
            });
        }
        Ok(Self(value))
    }

    /// Return the validated multiplier.
    pub const fn get(self) -> f32 {
        self.0
    }
}

/// A finite probability or ratio in the closed interval `[0, 1]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UnitInterval(f32);

impl UnitInterval {
    /// Validate a closed-unit-interval value.
    pub fn try_new(value: f32) -> Result<Self, VectorParameterError> {
        if !value.is_finite() {
            return Err(VectorParameterError::NonFinite {
                parameter: "unit interval",
            });
        }
        if !(0.0..=1.0).contains(&value) {
            return Err(VectorParameterError::OutsideClosedUnitInterval {
                parameter: "unit interval",
            });
        }
        Ok(Self(normalize_zero(value)))
    }

    /// Return the validated value.
    pub const fn get(self) -> f32 {
        self.0
    }
}

/// A finite probability in the open interval `(0, 1)`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FailureProbability(f32);

impl FailureProbability {
    /// Validate an open-unit-interval failure probability.
    pub fn try_new(value: f32) -> Result<Self, VectorParameterError> {
        if !value.is_finite() {
            return Err(VectorParameterError::NonFinite {
                parameter: "failure probability",
            });
        }
        if value <= 0.0 || value >= 1.0 {
            return Err(VectorParameterError::OutsideOpenUnitInterval {
                parameter: "failure probability",
            });
        }
        Ok(Self(value))
    }

    /// Return the validated probability.
    pub const fn get(self) -> f32 {
        self.0
    }
}

/// A SimHash collision threshold bounded by the algorithm bit width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollisionThreshold(usize);

impl CollisionThreshold {
    /// Validate a collision threshold.
    pub fn try_new(value: usize, bit_width: NonZeroUsize) -> Result<Self, VectorParameterError> {
        if value > bit_width.get() {
            return Err(VectorParameterError::AboveMaximum {
                parameter: "collision threshold",
                maximum: bit_width.get(),
                actual: value,
            });
        }
        Ok(Self(value))
    }

    /// Return the validated threshold.
    pub const fn get(self) -> usize {
        self.0
    }
}

/// A finite, nonnegative score safe to use in total ordering.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DistanceScore(f32);

impl DistanceScore {
    /// Validate a distance score without changing its positive finite value.
    pub fn try_new(value: f32) -> Result<Self, VectorParameterError> {
        if !value.is_finite() {
            return Err(VectorParameterError::NonFinite {
                parameter: "distance score",
            });
        }
        if value < 0.0 {
            return Err(VectorParameterError::NegativeDistanceScore);
        }
        Ok(Self(normalize_zero(value)))
    }

    /// Return the validated score.
    pub const fn get(self) -> f32 {
        self.0
    }
}

impl Eq for DistanceScore {}

impl Ord for DistanceScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .expect("validated distance scores are finite")
    }
}

impl PartialOrd for DistanceScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// An invalid numeric vector parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum VectorParameterError {
    /// A required count was zero.
    #[error("{parameter} must be non-zero")]
    Zero {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },
    /// A value was below a dependent minimum.
    #[error("{parameter} must be at least {minimum}, got {actual}")]
    BelowMinimum {
        /// Name of the invalid parameter.
        parameter: &'static str,
        /// Inclusive minimum.
        minimum: usize,
        /// Actual value.
        actual: usize,
    },
    /// A value exceeded an inclusive maximum.
    #[error("{parameter} must be at most {maximum}, got {actual}")]
    AboveMaximum {
        /// Name of the invalid parameter.
        parameter: &'static str,
        /// Inclusive maximum.
        maximum: usize,
        /// Actual value.
        actual: usize,
    },
    /// Checked derivation overflowed `usize`.
    #[error("{parameter} arithmetic overflow")]
    ArithmeticOverflow {
        /// Name of the derived parameter.
        parameter: &'static str,
    },
    /// A floating-point parameter was NaN or infinite.
    #[error("{parameter} must be finite")]
    NonFinite {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },
    /// A floating-point parameter was not strictly positive.
    #[error("{parameter} must be positive")]
    NotPositive {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },
    /// A probability was outside `(0, 1)`.
    #[error("{parameter} must be greater than zero and less than one")]
    OutsideOpenUnitInterval {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },
    /// A probability or ratio was outside `[0, 1]`.
    #[error("{parameter} must be between zero and one inclusive")]
    OutsideClosedUnitInterval {
        /// Name of the invalid parameter.
        parameter: &'static str,
    },
    /// A distance score was negative.
    #[error("distance score must be nonnegative")]
    NegativeDistanceScore,
}

fn non_zero(value: usize, parameter: &'static str) -> Result<NonZeroUsize, VectorParameterError> {
    let Some(value) = NonZeroUsize::new(value) else {
        return Err(VectorParameterError::Zero { parameter });
    };
    Ok(value)
}

const fn normalize_zero(value: f32) -> f32 {
    if value == 0.0 {
        0.0
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_types_reject_zero_and_dependent_underflow() {
        assert_eq!(
            Connections::try_new(0).unwrap_err(),
            VectorParameterError::Zero {
                parameter: "connections"
            }
        );
        let connections = Connections::try_new(16).unwrap();
        assert_eq!(connections.checked_double().unwrap().get(), 32);
        assert!(Layer0Connections::try_new(15, connections).is_err());
        assert!(ConstructionBeamWidth::try_new(15, connections).is_err());

        let result_count = ResultCount::try_new(10).unwrap();
        assert!(SearchBeamWidth::try_new(9, result_count).is_err());
        assert_eq!(
            SearchBeamWidth::try_new(10, result_count).unwrap().get(),
            10
        );
    }

    #[test]
    fn checked_double_rejects_overflow() {
        let connections = Connections::try_new(usize::MAX).unwrap();
        assert_eq!(
            connections.checked_double().unwrap_err(),
            VectorParameterError::ArithmeticOverflow {
                parameter: "layer-0 connections"
            }
        );
    }

    #[test]
    fn floating_parameters_reject_non_finite_and_out_of_range_values() {
        assert!(LayerMultiplier::try_new(0.0).is_err());
        assert!(LayerMultiplier::try_new(f32::NAN).is_err());
        assert_eq!(LayerMultiplier::try_new(0.5).unwrap().get(), 0.5);

        for invalid in [f32::NAN, f32::INFINITY, -0.1, 1.1] {
            assert!(UnitInterval::try_new(invalid).is_err());
        }
        assert_eq!(
            UnitInterval::try_new(-0.0).unwrap().get().to_bits(),
            0.0_f32.to_bits()
        );

        for invalid in [f32::NAN, f32::INFINITY, 0.0, 1.0, -0.1, 1.1] {
            assert!(FailureProbability::try_new(invalid).is_err());
        }
        assert_eq!(FailureProbability::try_new(0.1).unwrap().get(), 0.1);
    }

    #[test]
    fn collision_threshold_accepts_full_closed_range() {
        let bit_width = NonZeroUsize::new(64).unwrap();
        assert_eq!(CollisionThreshold::try_new(0, bit_width).unwrap().get(), 0);
        assert_eq!(
            CollisionThreshold::try_new(64, bit_width).unwrap().get(),
            64
        );
        assert!(CollisionThreshold::try_new(65, bit_width).is_err());
    }

    #[test]
    fn distance_score_is_finite_nonnegative_and_totally_ordered() {
        for invalid in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0] {
            assert!(DistanceScore::try_new(invalid).is_err());
        }

        let negative_zero = DistanceScore::try_new(-0.0).unwrap();
        let positive_zero = DistanceScore::try_new(0.0).unwrap();
        assert_eq!(negative_zero, positive_zero);
        assert_eq!(negative_zero.cmp(&positive_zero), Ordering::Equal);

        let one = DistanceScore::try_new(1.0).unwrap();
        assert!(positive_zero < one);
        assert_eq!(one.get(), 1.0);
    }
}
