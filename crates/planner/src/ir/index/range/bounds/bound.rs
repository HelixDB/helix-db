//! Single range-index bound contracts and proof helpers.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use super::super::{RangeIndexLiteral, RangeIndexValue};

/// One index range bound.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexBound {
    /// Inclusive bound.
    Inclusive(RangeIndexValue),
    /// Exclusive bound.
    Exclusive(RangeIndexValue),
}

impl IndexBound {
    pub(super) fn literal_endpoint(&self) -> Option<(&RangeIndexLiteral, bool)> {
        match self {
            Self::Inclusive(RangeIndexValue::Literal(bound)) => Some((bound, true)),
            Self::Exclusive(RangeIndexValue::Literal(bound)) => Some((bound, false)),
            Self::Inclusive(RangeIndexValue::Param(_))
            | Self::Exclusive(RangeIndexValue::Param(_)) => None,
        }
    }

    pub(super) fn proven_allows_lower_value(&self, value: &RangeIndexLiteral) -> bool {
        let Some((bound, inclusive)) = self.literal_endpoint() else {
            return false;
        };
        match value.partial_cmp_same_type(bound) {
            Some(Ordering::Less) | None => false,
            Some(Ordering::Equal) => inclusive,
            Some(Ordering::Greater) => true,
        }
    }

    pub(super) fn proven_allows_upper_value(&self, value: &RangeIndexLiteral) -> bool {
        let Some((bound, inclusive)) = self.literal_endpoint() else {
            return false;
        };
        match value.partial_cmp_same_type(bound) {
            Some(Ordering::Greater) | None => false,
            Some(Ordering::Equal) => inclusive,
            Some(Ordering::Less) => true,
        }
    }

    pub(super) fn proven_excludes_lower_value(&self, value: &RangeIndexLiteral) -> bool {
        let Some((bound, inclusive)) = self.literal_endpoint() else {
            return false;
        };
        match value.partial_cmp_same_type(bound) {
            Some(Ordering::Less) => true,
            Some(Ordering::Equal) => !inclusive,
            Some(Ordering::Greater) | None => false,
        }
    }

    pub(super) fn proven_excludes_upper_value(&self, value: &RangeIndexLiteral) -> bool {
        let Some((bound, inclusive)) = self.literal_endpoint() else {
            return false;
        };
        match value.partial_cmp_same_type(bound) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => !inclusive,
            Some(Ordering::Less) | None => false,
        }
    }

    pub(super) fn proven_allows_lower_bound(&self, other: &Self) -> bool {
        let Some((bound, inclusive)) = self.literal_endpoint() else {
            return false;
        };
        let Some((other_bound, other_inclusive)) = other.literal_endpoint() else {
            return false;
        };
        match other_bound.partial_cmp_same_type(bound) {
            Some(Ordering::Less) | None => false,
            Some(Ordering::Equal) => inclusive || !other_inclusive,
            Some(Ordering::Greater) => true,
        }
    }

    pub(super) fn proven_allows_upper_bound(&self, other: &Self) -> bool {
        let Some((bound, inclusive)) = self.literal_endpoint() else {
            return false;
        };
        let Some((other_bound, other_inclusive)) = other.literal_endpoint() else {
            return false;
        };
        match other_bound.partial_cmp_same_type(bound) {
            Some(Ordering::Greater) | None => false,
            Some(Ordering::Equal) => inclusive || !other_inclusive,
            Some(Ordering::Less) => true,
        }
    }

    pub(super) fn tighter_lower_bound(&self, other: &Self) -> Option<Self> {
        if self == other {
            return Some(self.clone());
        }
        let (bound, inclusive) = self.literal_endpoint()?;
        let (other_bound, other_inclusive) = other.literal_endpoint()?;
        match bound.partial_cmp_same_type(other_bound)? {
            Ordering::Less => Some(other.clone()),
            Ordering::Greater => Some(self.clone()),
            Ordering::Equal => Some(if inclusive && !other_inclusive {
                other.clone()
            } else {
                self.clone()
            }),
        }
    }

    pub(super) fn tighter_upper_bound(&self, other: &Self) -> Option<Self> {
        if self == other {
            return Some(self.clone());
        }
        let (bound, inclusive) = self.literal_endpoint()?;
        let (other_bound, other_inclusive) = other.literal_endpoint()?;
        match bound.partial_cmp_same_type(other_bound)? {
            Ordering::Less => Some(self.clone()),
            Ordering::Greater => Some(other.clone()),
            Ordering::Equal => Some(if inclusive && !other_inclusive {
                other.clone()
            } else {
                self.clone()
            }),
        }
    }
}

/// Whether a range bound includes its endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundInclusivity {
    /// Endpoint is included.
    Inclusive,
    /// Endpoint is excluded.
    Exclusive,
}
