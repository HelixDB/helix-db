use serde::{de, Deserialize, Deserializer, Serialize};

/// Integer microsecond latency estimate.
///
/// ```
/// use helix_planner::cost::LatencyEstimate;
///
/// assert_eq!(LatencyEstimate::micros(7).as_micros(), 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LatencyEstimate(u64);

impl LatencyEstimate {
    /// Zero latency.
    pub const ZERO: Self = Self(0);

    /// Build from microseconds.
    pub const fn micros(value: u64) -> Self {
        Self(value)
    }

    /// Return microseconds.
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// Saturating addition.
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating multiplication by a count.
    pub const fn saturating_mul(self, rhs: u64) -> Self {
        Self(self.0.saturating_mul(rhs))
    }
}

/// Estimated byte count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ByteEstimate(u64);

impl ByteEstimate {
    /// Zero bytes.
    pub const ZERO: Self = Self(0);

    /// Build from bytes.
    pub const fn bytes(value: u64) -> Self {
        Self(value)
    }

    /// Return bytes.
    pub const fn as_bytes(self) -> u64 {
        self.0
    }

    /// Saturating addition.
    pub const fn saturating_add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }

    /// Saturating multiplication by a count.
    pub const fn saturating_mul(self, rhs: u64) -> Self {
        Self(self.0.saturating_mul(rhs))
    }
}

/// Estimated row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EstimatedRows(u64);

impl EstimatedRows {
    /// Zero rows.
    pub const ZERO: Self = Self(0);

    /// Build from rows.
    pub const fn rows(value: u64) -> Self {
        Self(value)
    }

    /// Return rows.
    pub const fn as_rows(self) -> u64 {
        self.0
    }
}

/// Estimated row count statically bounded by a const maximum.
///
/// Use this for profile knobs whose legal range is part of an optimizer
/// contract. The bound is validated during construction and deserialization, so
/// a profile cannot represent impossible row assumptions.
///
/// ```
/// use helix_planner::cost::{EstimatedRows, EstimatedRowsAtMost};
///
/// type SingletonRows = EstimatedRowsAtMost<1>;
///
/// assert!(SingletonRows::new(EstimatedRows::rows(1)).is_some());
/// assert!(SingletonRows::new(EstimatedRows::rows(2)).is_none());
/// assert_eq!(SingletonRows::at_most(2).as_rows(), 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct EstimatedRowsAtMost<const MAX: u64>(EstimatedRows);

impl<const MAX: u64> EstimatedRowsAtMost<MAX> {
    /// Zero rows.
    pub const ZERO: Self = Self(EstimatedRows::ZERO);

    /// Build a bounded estimate from an existing row estimate.
    pub const fn new(rows: EstimatedRows) -> Option<Self> {
        if rows.as_rows() <= MAX {
            Some(Self(rows))
        } else {
            None
        }
    }

    /// Build a bounded estimate from a raw row count.
    pub const fn rows(value: u64) -> Option<Self> {
        Self::new(EstimatedRows::rows(value))
    }

    /// Build a bounded estimate by clamping a raw row count to the maximum.
    pub const fn at_most(value: u64) -> Self {
        Self::clamp(EstimatedRows::rows(value))
    }

    /// Clamp an estimate into this bounded row domain.
    pub const fn clamp(rows: EstimatedRows) -> Self {
        if rows.as_rows() <= MAX {
            Self(rows)
        } else {
            Self(EstimatedRows::rows(MAX))
        }
    }

    /// Return the bounded row count.
    pub const fn as_rows(self) -> u64 {
        self.0.as_rows()
    }

    /// Return the underlying row estimate.
    pub const fn estimated_rows(self) -> EstimatedRows {
        self.0
    }
}

impl<'de, const MAX: u64> Deserialize<'de> for EstimatedRowsAtMost<MAX> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let rows = EstimatedRows::deserialize(deserializer)?;
        Self::new(rows).ok_or_else(|| {
            de::Error::custom(format!(
                "estimated rows must be at most {MAX}, got {}",
                rows.as_rows()
            ))
        })
    }
}

/// Estimated row count for unique equality lookups.
pub type UniqueEqualityRows = EstimatedRowsAtMost<1>;

/// Selectivity represented as parts per million.
///
/// ```
/// use helix_planner::cost::Selectivity;
///
/// assert!(Selectivity::from_parts_per_million(1_000_001).is_none());
/// assert_eq!(Selectivity::from_ratio(1, 4).unwrap().parts_per_million(), 250_000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Selectivity(u32);

impl Selectivity {
    /// One million parts per million is 100%.
    pub const ONE: Self = Self(1_000_000);
    /// Zero selectivity.
    pub const ZERO: Self = Self(0);

    /// Build from parts per million.
    pub const fn from_parts_per_million(value: u32) -> Option<Self> {
        if value <= 1_000_000 {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Build from a numerator/denominator ratio.
    pub fn from_ratio(numerator: u64, denominator: u64) -> Option<Self> {
        if denominator == 0 || numerator > denominator {
            return None;
        }
        let value = (u128::from(numerator) * 1_000_000_u128) / u128::from(denominator);
        u32::try_from(value)
            .ok()
            .and_then(Self::from_parts_per_million)
    }

    /// Return parts per million.
    pub const fn parts_per_million(self) -> u32 {
        self.0
    }

    /// Apply selectivity to a row estimate, rounding up.
    pub fn apply_to(self, rows: EstimatedRows) -> EstimatedRows {
        let numerator = rows
            .as_rows()
            .saturating_mul(u64::from(self.parts_per_million()));
        EstimatedRows::rows(numerator.saturating_add(999_999) / 1_000_000)
    }
}
