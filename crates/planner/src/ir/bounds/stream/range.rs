//! Statically ordered literal and dynamic stream-range contracts.

use helix_ast::expr::StreamBound;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use super::bound::{StreamBoundExprPlan, StreamBoundPlan, StreamBoundPlanError};

/// Invalid stream range payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRangePlanError {
    /// Literal range starts after it ends.
    InvertedLiteralRange {
        /// Literal start bound.
        start: usize,
        /// Literal end bound.
        end: usize,
    },
    /// Runtime bound expression failed validation.
    Bound(StreamBoundPlanError),
}

/// Literal stream range with statically ordered bounds.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::StreamLiteralRange;
///
/// assert!(StreamLiteralRange::new(2, 8).is_some());
/// assert!(StreamLiteralRange::new(8, 2).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StreamLiteralRange {
    start: usize,
    end: usize,
}

impl StreamLiteralRange {
    /// Build a literal range, returning `None` when `start > end`.
    pub fn new(start: usize, end: usize) -> Option<Self> {
        (start <= end).then_some(Self { start, end })
    }

    /// Start bound of the checked literal stream range.
    ///
    /// ```
    /// use helix_planner::ir::StreamLiteralRange;
    ///
    /// let range = StreamLiteralRange::new(2, 8).unwrap();
    /// assert_eq!(range.start(), 2);
    /// ```
    pub fn start(&self) -> usize {
        self.start
    }

    /// End bound of the checked literal stream range.
    ///
    /// ```
    /// use helix_planner::ir::StreamLiteralRange;
    ///
    /// let range = StreamLiteralRange::new(2, 8).unwrap();
    /// assert_eq!(range.end(), 8);
    /// ```
    pub fn end(&self) -> usize {
        self.end
    }
}

impl<'de> Deserialize<'de> for StreamLiteralRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Bounds {
            start: usize,
            end: usize,
        }

        let bounds = Bounds::deserialize(deserializer)?;
        Self::new(bounds.start, bounds.end)
            .ok_or_else(|| D::Error::custom("expected stream range start <= end"))
    }
}

/// Stream range with at least one runtime expression bound.
///
/// # Examples
///
/// ```
/// use helix_ast::expr::{Expr, StreamBound};
/// use helix_planner::ir::{StreamBoundPlan, StreamDynamicRange};
///
/// assert!(StreamDynamicRange::new(
///     StreamBoundPlan::new(StreamBound::expr(Expr::param("start"))).unwrap(),
///     StreamBoundPlan::Literal(8),
/// )
/// .is_some());
/// assert!(StreamDynamicRange::new(StreamBoundPlan::Literal(2), StreamBoundPlan::Literal(8))
///     .is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StreamDynamicRange {
    start: StreamBoundPlan,
    end: StreamBoundPlan,
}

impl StreamDynamicRange {
    /// Build a dynamic range, returning `None` when both bounds are literals.
    pub fn new(start: StreamBoundPlan, end: StreamBoundPlan) -> Option<Self> {
        (!matches!(
            (&start, &end),
            (StreamBoundPlan::Literal(_), StreamBoundPlan::Literal(_))
        ))
        .then_some(Self { start, end })
    }

    /// Build a dynamic range from a required runtime start expression.
    ///
    /// ```
    /// use helix_ast::expr::Expr;
    /// use helix_planner::ir::{StreamBoundExprPlan, StreamBoundPlan, StreamDynamicRange};
    ///
    /// let start = StreamBoundExprPlan::new(Expr::param("start")).unwrap();
    /// let range = StreamDynamicRange::from_dynamic_start(start, StreamBoundPlan::Literal(8));
    ///
    /// assert!(matches!(range.start(), StreamBoundPlan::Expr(_)));
    /// ```
    pub fn from_dynamic_start(start: StreamBoundExprPlan, end: StreamBoundPlan) -> Self {
        Self {
            start: StreamBoundPlan::Expr(start),
            end,
        }
    }

    /// Build a dynamic range from a required runtime end expression.
    ///
    /// ```
    /// use helix_ast::expr::Expr;
    /// use helix_planner::ir::{StreamBoundExprPlan, StreamBoundPlan, StreamDynamicRange};
    ///
    /// let end = StreamBoundExprPlan::new(Expr::param("end")).unwrap();
    /// let range = StreamDynamicRange::from_dynamic_end(StreamBoundPlan::Literal(2), end);
    ///
    /// assert!(matches!(range.end(), StreamBoundPlan::Expr(_)));
    /// ```
    pub fn from_dynamic_end(start: StreamBoundPlan, end: StreamBoundExprPlan) -> Self {
        Self {
            start,
            end: StreamBoundPlan::Expr(end),
        }
    }

    /// Start bound of the dynamic stream range.
    ///
    /// ```
    /// use helix_ast::expr::{Expr, StreamBound};
    /// use helix_planner::ir::{StreamBoundPlan, StreamDynamicRange};
    ///
    /// let range = StreamDynamicRange::new(
    ///     StreamBoundPlan::new(StreamBound::expr(Expr::param("start"))).unwrap(),
    ///     StreamBoundPlan::Literal(8),
    /// )
    /// .unwrap();
    /// assert!(matches!(range.start(), StreamBoundPlan::Expr(_)));
    /// ```
    pub fn start(&self) -> &StreamBoundPlan {
        &self.start
    }

    /// End bound of the dynamic stream range.
    ///
    /// ```
    /// use helix_ast::expr::{Expr, StreamBound};
    /// use helix_planner::ir::{StreamBoundPlan, StreamDynamicRange};
    ///
    /// let range = StreamDynamicRange::new(
    ///     StreamBoundPlan::Literal(2),
    ///     StreamBoundPlan::new(StreamBound::expr(Expr::param("end"))).unwrap(),
    /// )
    /// .unwrap();
    /// assert!(matches!(range.end(), StreamBoundPlan::Expr(_)));
    /// ```
    pub fn end(&self) -> &StreamBoundPlan {
        &self.end
    }
}

impl<'de> Deserialize<'de> for StreamDynamicRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Bounds {
            start: StreamBoundPlan,
            end: StreamBoundPlan,
        }

        let bounds = Bounds::deserialize(deserializer)?;
        Self::new(bounds.start, bounds.end)
            .ok_or_else(|| D::Error::custom("expected at least one dynamic stream range bound"))
    }
}

/// Stream range with validated bound relationships.
///
/// Literal bounds are checked statically; dynamic ranges are reserved for
/// ranges where at least one bound is evaluated at runtime.
///
/// ```
/// use helix_ast::expr::{Expr, StreamBound};
/// use helix_planner::ir::{StreamRangePlan, StreamRangePlanError};
///
/// let literal = StreamRangePlan::new(StreamBound::Literal(2), StreamBound::Literal(8)).unwrap();
/// assert!(matches!(literal, StreamRangePlan::Literal(_)));
/// assert!(matches!(
///     StreamRangePlan::new(StreamBound::Literal(8), StreamBound::Literal(2)),
///     Err(StreamRangePlanError::InvertedLiteralRange { start: 8, end: 2 })
/// ));
/// assert!(matches!(
///     StreamRangePlan::new(StreamBound::expr(Expr::param("start")), StreamBound::Literal(8))
///         .unwrap(),
///     StreamRangePlan::Dynamic(_)
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamRangePlan {
    /// Statically checked literal range.
    Literal(StreamLiteralRange),
    /// Range with at least one runtime expression bound.
    Dynamic(StreamDynamicRange),
}

impl StreamRangePlan {
    /// Build a stream range plan after validating bound expressions and static ordering.
    pub fn new(start: StreamBound, end: StreamBound) -> Result<Self, StreamRangePlanError> {
        let start = StreamBoundPlan::new(start).map_err(StreamRangePlanError::Bound)?;
        let end = StreamBoundPlan::new(end).map_err(StreamRangePlanError::Bound)?;

        match (start, end) {
            (StreamBoundPlan::Literal(start), StreamBoundPlan::Literal(end)) => {
                StreamLiteralRange::new(start, end)
                    .map(Self::Literal)
                    .ok_or(StreamRangePlanError::InvertedLiteralRange { start, end })
            }
            (StreamBoundPlan::Expr(start), end) => Ok(Self::Dynamic(
                StreamDynamicRange::from_dynamic_start(start, end),
            )),
            (start, StreamBoundPlan::Expr(end)) => Ok(Self::Dynamic(
                StreamDynamicRange::from_dynamic_end(start, end),
            )),
        }
    }
}
