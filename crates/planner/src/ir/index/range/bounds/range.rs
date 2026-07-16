//! Static range-index proof contracts.

use serde::{Deserialize, Serialize};

use super::{between, bound};
use crate::ir::{RangeIndexLiteral, SecondaryIndexLiteral};

/// Index range bounds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexRange {
    /// All values present in the scoped range index.
    All,
    /// Lower-bounded range.
    Lower {
        /// Lower bound.
        lower: bound::IndexBound,
    },
    /// Upper-bounded range.
    Upper {
        /// Upper bound.
        upper: bound::IndexBound,
    },
    /// Inclusive or exclusive bounded range.
    Between(between::IndexBetweenRange),
}

impl IndexRange {
    /// Return the static intersection of two ranges when the tighter bounds can be proven.
    ///
    /// Complementary lower and upper ranges become a bounded range. Exact
    /// structural equality is preserved, including dynamic bounds. Different
    /// dynamic bounds and incomparable literal types return `None`, because the
    /// tighter bound cannot be selected at planning time.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{
    ///     IndexBound, IndexBetweenRange, IndexRange, RangeIndexValue,
    /// };
    ///
    /// let lower = IndexRange::Lower {
    ///     lower: IndexBound::Inclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
    ///     ),
    /// };
    /// let upper = IndexRange::Upper {
    ///     upper: IndexBound::Exclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(65)).unwrap(),
    ///     ),
    /// };
    ///
    /// assert_eq!(
    ///     lower.intersect(&upper),
    ///     IndexBetweenRange::new(
    ///         IndexBound::Inclusive(
    ///             RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
    ///         ),
    ///         IndexBound::Exclusive(
    ///             RangeIndexValue::literal(PropertyValue::from(65)).unwrap(),
    ///         ),
    ///     ).map(IndexRange::Between)
    /// );
    /// ```
    pub fn intersect(&self, other: &Self) -> Option<Self> {
        if self == other {
            return Some(self.clone());
        }
        match (self, other) {
            (Self::All, range) | (range, Self::All) => return Some(range.clone()),
            _ => {}
        }
        let lower = match (self.lower_bound(), other.lower_bound()) {
            (Some(left), Some(right)) => Some(left.tighter_lower_bound(right)?),
            (Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
            (None, None) => None,
        };
        let upper = match (self.upper_bound(), other.upper_bound()) {
            (Some(left), Some(right)) => Some(left.tighter_upper_bound(right)?),
            (Some(bound), None) | (None, Some(bound)) => Some(bound.clone()),
            (None, None) => None,
        };
        match (lower, upper) {
            (Some(lower), Some(upper)) => {
                between::IndexBetweenRange::new(lower, upper).map(Self::Between)
            }
            (Some(lower), None) => Some(Self::Lower { lower }),
            (None, Some(upper)) => Some(Self::Upper { upper }),
            (None, None) => None,
        }
    }

    /// Return whether this range is proven to contain every value from another range.
    ///
    /// Exact structural equality returns `true`, including dynamic bounds.
    /// Otherwise dynamic bounds and values with incomparable literal types return
    /// `false`, because containment cannot be proven at planning time.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{
    ///     IndexBound, IndexBetweenRange, IndexRange, RangeIndexValue,
    /// };
    ///
    /// let wider = IndexRange::Lower {
    ///     lower: IndexBound::Inclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
    ///     ),
    /// };
    /// let narrower = IndexRange::Between(IndexBetweenRange::new(
    ///     IndexBound::Inclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(21)).unwrap(),
    ///     ),
    ///     IndexBound::Exclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(65)).unwrap(),
    ///     ),
    /// ).unwrap());
    ///
    /// assert!(wider.contains_range(&narrower));
    /// assert!(!narrower.contains_range(&wider));
    /// ```
    pub fn contains_range(&self, other: &Self) -> bool {
        if self == other {
            return true;
        }
        self.lower_bound().is_none_or(|lower| {
            other
                .lower_bound()
                .is_some_and(|other_lower| lower.proven_allows_lower_bound(other_lower))
        }) && self.upper_bound().is_none_or(|upper| {
            other
                .upper_bound()
                .is_some_and(|other_upper| upper.proven_allows_upper_bound(other_upper))
        })
    }

    /// Return whether this static range is proven to contain an equality lookup literal.
    ///
    /// Dynamic range bounds and values whose equality-index literal is not
    /// range-orderable return `false`, because containment cannot be proven at
    /// planning time.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{
    ///     IndexBound, IndexRange, RangeIndexValue, SecondaryIndexLiteral,
    /// };
    ///
    /// let range = IndexRange::Lower {
    ///     lower: IndexBound::Inclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
    ///     ),
    /// };
    /// let value = SecondaryIndexLiteral::new(PropertyValue::from(21)).unwrap();
    ///
    /// assert!(range.contains_secondary_literal(&value));
    /// ```
    pub fn contains_secondary_literal(&self, value: &SecondaryIndexLiteral) -> bool {
        let Some(value) =
            RangeIndexLiteral::try_from_property_value(value.as_property_value().clone())
        else {
            return false;
        };
        match self {
            Self::All => true,
            Self::Lower { lower } => lower.proven_allows_lower_value(&value),
            Self::Upper { upper } => upper.proven_allows_upper_value(&value),
            Self::Between(bounds) => {
                bounds.lower().proven_allows_lower_value(&value)
                    && bounds.upper().proven_allows_upper_value(&value)
            }
        }
    }

    /// Return whether this static range is proven to exclude an equality lookup literal.
    ///
    /// Dynamic range bounds and values whose equality-index literal is not
    /// range-orderable return `false`, because exclusion cannot be proven at
    /// planning time.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{
    ///     IndexBound, IndexRange, RangeIndexValue, SecondaryIndexLiteral,
    /// };
    ///
    /// let range = IndexRange::Lower {
    ///     lower: IndexBound::Inclusive(
    ///         RangeIndexValue::literal(PropertyValue::from(18)).unwrap(),
    ///     ),
    /// };
    ///
    /// assert!(range.excludes_secondary_literal(
    ///     &SecondaryIndexLiteral::new(PropertyValue::from(17)).unwrap()
    /// ));
    /// assert!(!range.excludes_secondary_literal(
    ///     &SecondaryIndexLiteral::new(PropertyValue::from(18)).unwrap()
    /// ));
    /// ```
    pub fn excludes_secondary_literal(&self, value: &SecondaryIndexLiteral) -> bool {
        let Some(value) =
            RangeIndexLiteral::try_from_property_value(value.as_property_value().clone())
        else {
            return false;
        };
        match self {
            Self::All => false,
            Self::Lower { lower } => lower.proven_excludes_lower_value(&value),
            Self::Upper { upper } => upper.proven_excludes_upper_value(&value),
            Self::Between(bounds) => {
                bounds.lower().proven_excludes_lower_value(&value)
                    || bounds.upper().proven_excludes_upper_value(&value)
            }
        }
    }

    fn lower_bound(&self) -> Option<&bound::IndexBound> {
        match self {
            Self::Lower { lower } => Some(lower),
            Self::Between(bounds) => Some(bounds.lower()),
            Self::All | Self::Upper { .. } => None,
        }
    }

    fn upper_bound(&self) -> Option<&bound::IndexBound> {
        match self {
            Self::Upper { upper } => Some(upper),
            Self::Between(bounds) => Some(bounds.upper()),
            Self::All | Self::Lower { .. } => None,
        }
    }
}
