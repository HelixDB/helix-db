//! Statically valid access-window range composition.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::ir;

/// Static stream window applied to an access path.
///
/// `end == None` means the window is open-ended. Bounded windows encode
/// `start <= end`, so access-window rules never handle inverted slices.
///
/// ```
/// use helix_planner::logical::AccessWindowRange;
///
/// assert!(AccessWindowRange::new(3, Some(2)).is_none());
/// let window = AccessWindowRange::new(3, Some(8)).unwrap();
/// assert_eq!(window.start(), 3);
/// assert_eq!(window.end(), Some(8));
/// assert!(!window.is_empty());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct AccessWindowRange {
    start: usize,
    end: Option<usize>,
}

impl AccessWindowRange {
    /// Build a window, rejecting bounded windows whose end is before start.
    pub fn new(start: usize, end: Option<usize>) -> Option<Self> {
        end.is_none_or(|end| start <= end)
            .then_some(Self { start, end })
    }

    /// First row retained by the window.
    pub const fn start(self) -> usize {
        self.start
    }

    /// Exclusive row end, or `None` for an open-ended window.
    pub const fn end(self) -> Option<usize> {
        self.end
    }

    /// Return the checked literal stream range for bounded windows.
    ///
    /// ```
    /// use helix_planner::logical::AccessWindowRange;
    ///
    /// let bounded = AccessWindowRange::new(3, Some(8)).unwrap();
    /// let range = bounded.bounded_stream_range().unwrap();
    /// assert_eq!((range.start(), range.end()), (3, 8));
    ///
    /// let open = AccessWindowRange::new(3, None).unwrap();
    /// assert!(open.bounded_stream_range().is_none());
    /// ```
    pub fn bounded_stream_range(self) -> Option<ir::StreamLiteralRange> {
        let end = self.end?;
        ir::StreamLiteralRange::new(self.start, end)
    }

    /// Whether this bounded window is statically empty.
    pub const fn is_empty(self) -> bool {
        matches!(self.end, Some(end) if end == self.start)
    }

    /// Whether this window keeps every row from a source with the given finite
    /// upper bound.
    pub(crate) const fn fully_contains_bounded_prefix(self, upper: usize) -> bool {
        self.start == 0 && matches!(self.end, Some(end) if end >= upper)
    }

    pub(crate) fn identity() -> Self {
        Self {
            start: 0,
            end: None,
        }
    }

    pub(crate) fn then_limit(self, count: usize) -> Self {
        let end = self.end.map_or_else(
            || self.start.saturating_add(count),
            |end| end.min(self.start.saturating_add(count)),
        );
        Self {
            end: Some(end),
            ..self
        }
    }

    pub(crate) fn then_skip(self, count: usize) -> Self {
        match self.end {
            Some(end) => {
                let len = end.saturating_sub(self.start);
                Self {
                    start: self.start.saturating_add(count.min(len)),
                    end: Some(end),
                }
            }
            None => Self {
                start: self.start.saturating_add(count),
                end: None,
            },
        }
    }

    pub(crate) fn then_range(self, range: &ir::StreamLiteralRange) -> Self {
        match self.end {
            Some(end) => {
                let len = end.saturating_sub(self.start);
                Self {
                    start: self.start.saturating_add(range.start().min(len)),
                    end: Some(self.start.saturating_add(range.end().min(len))),
                }
            }
            None => Self {
                start: self.start.saturating_add(range.start()),
                end: Some(self.start.saturating_add(range.end())),
            },
        }
    }

    pub(crate) fn is_identity(self) -> bool {
        self.start == 0 && self.end.is_none()
    }
}

impl<'de> Deserialize<'de> for AccessWindowRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Bounds {
            start: usize,
            end: Option<usize>,
        }

        let bounds = Bounds::deserialize(deserializer)?;
        Self::new(bounds.start, bounds.end)
            .ok_or_else(|| D::Error::custom("expected access window start <= end"))
    }
}
