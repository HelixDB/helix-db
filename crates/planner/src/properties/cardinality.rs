use std::ops::Range;

use serde::{Deserialize, Serialize};

/// Cardinality bounds with an optional finite upper bound.
///
/// ```
/// use helix_planner::properties::CardinalityBounds;
///
/// assert!(CardinalityBounds::new(3, Some(2)).is_none());
/// assert_eq!(CardinalityBounds::exact(4).upper(), Some(4));
/// assert_eq!(CardinalityBounds::zero_to(Some(4)).lower(), 0);
/// assert_eq!(CardinalityBounds::unknown().lower(), 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CardinalityBounds {
    lower: usize,
    upper: Option<usize>,
}

impl CardinalityBounds {
    /// Build bounds, rejecting `lower > upper` when the upper bound is known.
    pub fn new(lower: usize, upper: Option<usize>) -> Option<Self> {
        upper
            .is_none_or(|upper| lower <= upper)
            .then_some(Self { lower, upper })
    }

    /// Build bounds from zero to an optional finite upper bound.
    ///
    /// ```
    /// use helix_planner::properties::CardinalityBounds;
    ///
    /// assert_eq!(CardinalityBounds::zero_to(Some(3)).upper(), Some(3));
    /// assert_eq!(CardinalityBounds::zero_to(None), CardinalityBounds::unknown());
    /// ```
    pub const fn zero_to(upper: Option<usize>) -> Self {
        Self { lower: 0, upper }
    }

    /// Unknown finite upper bound and zero lower bound.
    pub const fn unknown() -> Self {
        Self {
            lower: 0,
            upper: None,
        }
    }

    /// Exact cardinality.
    pub const fn exact(value: usize) -> Self {
        Self {
            lower: value,
            upper: Some(value),
        }
    }

    /// Cardinality after applying a literal `limit`.
    ///
    /// ```
    /// use helix_planner::properties::CardinalityBounds;
    ///
    /// let bounds = CardinalityBounds::new(3, Some(10)).unwrap();
    ///
    /// assert_eq!(bounds.after_limit(4), CardinalityBounds::new(3, Some(4)).unwrap());
    /// assert_eq!(bounds.after_limit(2), CardinalityBounds::exact(2));
    /// ```
    pub fn after_limit(self, limit: usize) -> Self {
        let lower = self.lower.min(limit);
        let upper = Some(self.upper.map_or(limit, |upper| upper.min(limit)));
        Self { lower, upper }
    }

    /// Cardinality after applying a literal `skip`.
    ///
    /// ```
    /// use helix_planner::properties::CardinalityBounds;
    ///
    /// let bounds = CardinalityBounds::new(3, Some(10)).unwrap();
    ///
    /// assert_eq!(bounds.after_skip(2), CardinalityBounds::new(1, Some(8)).unwrap());
    /// assert_eq!(bounds.after_skip(12), CardinalityBounds::exact(0));
    /// ```
    pub fn after_skip(self, skip: usize) -> Self {
        Self {
            lower: self.lower.saturating_sub(skip),
            upper: self.upper.map(|upper| upper.saturating_sub(skip)),
        }
    }

    /// Cardinality after applying a literal stream range.
    ///
    /// ```
    /// use helix_planner::properties::CardinalityBounds;
    ///
    /// let bounds = CardinalityBounds::new(3, Some(10)).unwrap();
    ///
    /// assert_eq!(bounds.after_range(2..5), CardinalityBounds::new(1, Some(3)).unwrap());
    /// assert_eq!(bounds.after_range(10..12), CardinalityBounds::exact(0));
    /// ```
    pub fn after_range(self, range: Range<usize>) -> Self {
        let width = range.end.saturating_sub(range.start);
        let after_start = |count: usize| count.saturating_sub(range.start).min(width);
        Self {
            lower: after_start(self.lower),
            upper: Some(self.upper.map_or(width, after_start)),
        }
    }

    /// Lower bound.
    pub const fn lower(self) -> usize {
        self.lower
    }

    /// Optional upper bound.
    pub const fn upper(self) -> Option<usize> {
        self.upper
    }
}

impl Default for CardinalityBounds {
    fn default() -> Self {
        Self::unknown()
    }
}
