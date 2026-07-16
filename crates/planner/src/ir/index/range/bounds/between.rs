//! Paired range-index bound contracts.

use std::cmp::Ordering;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::super::RangeIndexValue;
use super::bound::IndexBound;

/// Pair of index bounds whose static literal values are not inverted.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IndexBetweenRange {
    lower: IndexBound,
    upper: IndexBound,
}

impl IndexBetweenRange {
    /// Build paired bounds, returning `None` when static literal bounds are
    /// inverted or not mutually comparable.
    pub fn new(lower: IndexBound, upper: IndexBound) -> Option<Self> {
        match (&lower, &upper) {
            (
                IndexBound::Inclusive(RangeIndexValue::Literal(lower_value))
                | IndexBound::Exclusive(RangeIndexValue::Literal(lower_value)),
                IndexBound::Inclusive(RangeIndexValue::Literal(upper_value))
                | IndexBound::Exclusive(RangeIndexValue::Literal(upper_value)),
            ) => lower_value
                .partial_cmp_same_type(upper_value)
                .filter(|ordering| *ordering != Ordering::Greater)
                .map(|_| Self { lower, upper }),
            _ => Some(Self { lower, upper }),
        }
    }

    /// Lower bound.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{IndexBetweenRange, IndexBound, RangeIndexValue};
    ///
    /// let lower = IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(1)).unwrap());
    /// let upper = IndexBound::Inclusive(RangeIndexValue::literal(PropertyValue::from(2)).unwrap());
    /// let range = IndexBetweenRange::new(lower.clone(), upper).unwrap();
    /// assert_eq!(range.lower(), &lower);
    /// ```
    pub const fn lower(&self) -> &IndexBound {
        &self.lower
    }

    /// Upper bound.
    pub const fn upper(&self) -> &IndexBound {
        &self.upper
    }
}

impl<'de> Deserialize<'de> for IndexBetweenRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Bounds {
            lower: IndexBound,
            upper: IndexBound,
        }

        let bounds = Bounds::deserialize(deserializer)?;
        Self::new(bounds.lower, bounds.upper)
            .ok_or_else(|| D::Error::custom("expected index range lower <= upper"))
    }
}
