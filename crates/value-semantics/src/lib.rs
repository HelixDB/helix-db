//! Shared exact value semantics independent of AST and storage representations.
//!
//! The crate intentionally accepts primitive numeric values only. Callers in
//! the planner and database adapt their own property-value enums at their
//! respective contract boundaries.

#![deny(unsafe_code)]

use core::cmp::Ordering;

/// Exact finite magnitude represented as `odd_significand * 2^exponent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CanonicalFinite {
    exponent: i16,
    odd_significand: u64,
    floor_log2: i16,
    normalized_significand: u64,
}

impl CanonicalFinite {
    /// Binary exponent paired with [`Self::odd_significand`].
    pub const fn exponent(self) -> i16 {
        self.exponent
    }

    /// Non-zero odd significand carrying the exact finite magnitude.
    pub const fn odd_significand(self) -> u64 {
        self.odd_significand
    }

    /// Floor of the base-two logarithm of the finite magnitude.
    pub const fn floor_log2(self) -> i16 {
        self.floor_log2
    }

    /// Significand normalized so its highest bit is set.
    pub const fn normalized_significand(self) -> u64 {
        self.normalized_significand
    }

    fn magnitude_cmp(self, other: Self) -> Ordering {
        self.floor_log2.cmp(&other.floor_log2).then_with(|| {
            self.normalized_significand
                .cmp(&other.normalized_significand)
        })
    }
}

/// Exact non-NaN numeric value shared by planner proofs and storage codecs.
///
/// ```
/// use helix_value_semantics::CanonicalNumber;
///
/// assert_eq!(
///     CanonicalNumber::from_i64(42),
///     CanonicalNumber::from_f64(42.0).unwrap()
/// );
/// assert_eq!(
///     CanonicalNumber::from_f32(-0.0),
///     CanonicalNumber::from_f64(0.0)
/// );
/// assert!(CanonicalNumber::from_f64(f64::NAN).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CanonicalNumber {
    /// Negative infinity.
    NegativeInfinity,
    /// Negative finite number.
    NegativeFinite(CanonicalFinite),
    /// Sign-normalized zero.
    Zero,
    /// Positive finite number.
    PositiveFinite(CanonicalFinite),
    /// Positive infinity.
    PositiveInfinity,
}

impl CanonicalNumber {
    /// Normalize an `i64` without converting through floating point.
    pub fn from_i64(value: i64) -> Self {
        if value == 0 {
            return Self::Zero;
        }
        Self::finite(value.is_negative(), value.unsigned_abs(), 0)
    }

    /// Normalize an `f64`, returning `None` for NaN.
    pub fn from_f64(value: f64) -> Option<Self> {
        let bits = value.to_bits();
        let negative = bits >> 63 != 0;
        let exponent_bits = ((bits >> 52) & 0x7FF) as i16;
        let fraction = bits & ((1_u64 << 52) - 1);
        if exponent_bits == 0x7FF {
            return (fraction == 0).then_some(if negative {
                Self::NegativeInfinity
            } else {
                Self::PositiveInfinity
            });
        }
        if exponent_bits == 0 && fraction == 0 {
            return Some(Self::Zero);
        }
        let (significand, exponent) = if exponent_bits == 0 {
            (fraction, -1074)
        } else {
            ((1_u64 << 52) | fraction, exponent_bits - 1023 - 52)
        };
        Some(Self::finite(negative, significand, exponent))
    }

    /// Normalize an `f32`, returning `None` for NaN.
    pub fn from_f32(value: f32) -> Option<Self> {
        let bits = value.to_bits();
        let negative = bits >> 31 != 0;
        let exponent_bits = ((bits >> 23) & 0xFF) as i16;
        let fraction = u64::from(bits & ((1_u32 << 23) - 1));
        if exponent_bits == 0xFF {
            return (fraction == 0).then_some(if negative {
                Self::NegativeInfinity
            } else {
                Self::PositiveInfinity
            });
        }
        if exponent_bits == 0 && fraction == 0 {
            return Some(Self::Zero);
        }
        let (significand, exponent) = if exponent_bits == 0 {
            (fraction, -149)
        } else {
            ((1_u64 << 23) | fraction, exponent_bits - 127 - 23)
        };
        Some(Self::finite(negative, significand, exponent))
    }

    fn finite(negative: bool, significand: u64, exponent: i16) -> Self {
        debug_assert_ne!(significand, 0);
        let trailing = significand.trailing_zeros() as i16;
        let odd_significand = significand >> trailing;
        let exponent = exponent + trailing;
        let floor_log2 = exponent + (u64::BITS - 1 - odd_significand.leading_zeros()) as i16;
        let finite = CanonicalFinite {
            exponent,
            odd_significand,
            floor_log2,
            normalized_significand: odd_significand << odd_significand.leading_zeros(),
        };
        if negative {
            Self::NegativeFinite(finite)
        } else {
            Self::PositiveFinite(finite)
        }
    }
}

impl Ord for CanonicalNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        use CanonicalNumber::{
            NegativeFinite, NegativeInfinity, PositiveFinite, PositiveInfinity, Zero,
        };
        match (*self, *other) {
            (NegativeInfinity, NegativeInfinity)
            | (Zero, Zero)
            | (PositiveInfinity, PositiveInfinity) => Ordering::Equal,
            (NegativeInfinity, _) | (_, PositiveInfinity) => Ordering::Less,
            (_, NegativeInfinity) | (PositiveInfinity, _) => Ordering::Greater,
            (NegativeFinite(left), NegativeFinite(right)) => right.magnitude_cmp(left),
            (PositiveFinite(left), PositiveFinite(right)) => left.magnitude_cmp(right),
            (NegativeFinite(_), _) | (Zero, PositiveFinite(_)) => Ordering::Less,
            (_, NegativeFinite(_)) | (PositiveFinite(_), Zero) => Ordering::Greater,
        }
    }
}

impl PartialOrd for CanonicalNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_cross_numeric_boundary_does_not_round_through_f64() {
        let exact = CanonicalNumber::from_i64(9_007_199_254_740_992);
        let next = CanonicalNumber::from_i64(9_007_199_254_740_993);
        let float = CanonicalNumber::from_f64(9_007_199_254_740_992.0).unwrap();

        assert_eq!(exact, float);
        assert_ne!(next, float);
        assert!(next > float);
    }

    #[test]
    fn finite_and_infinite_order_covers_every_numeric_class() {
        let ordered = [
            CanonicalNumber::from_f64(f64::NEG_INFINITY).unwrap(),
            CanonicalNumber::from_i64(i64::MIN),
            CanonicalNumber::from_f64(-f64::MIN_POSITIVE).unwrap(),
            CanonicalNumber::from_f64(-f64::from_bits(1)).unwrap(),
            CanonicalNumber::from_f64(-0.0).unwrap(),
            CanonicalNumber::from_f64(f64::from_bits(1)).unwrap(),
            CanonicalNumber::from_f64(f64::MIN_POSITIVE).unwrap(),
            CanonicalNumber::from_i64(i64::MAX),
            CanonicalNumber::from_f64(f64::INFINITY).unwrap(),
        ];
        assert!(ordered.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
