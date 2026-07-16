use crate::{cost, ir, properties};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::rules) enum StreamRowUpperBound {
    Known(u64),
    Unknown,
}

impl StreamRowUpperBound {
    pub(in crate::rules) const fn known(rows: u64) -> Self {
        Self::Known(rows)
    }

    pub(in crate::rules) const fn from_usize(rows: usize) -> Self {
        Self::Known(rows as u64)
    }

    pub(in crate::rules) fn to_cardinality_upper(self) -> Option<usize> {
        match self {
            Self::Known(rows) => usize::try_from(rows).ok(),
            Self::Unknown => None,
        }
    }

    pub(in crate::rules) fn clamp_rows(self, rows: cost::EstimatedRows) -> cost::EstimatedRows {
        match self {
            Self::Known(upper) => cost::EstimatedRows::rows(rows.as_rows().min(upper)),
            Self::Unknown => rows,
        }
    }
}

pub(in crate::rules) fn estimated_pipeline_rows(
    delivered: &properties::DeliveredProperties,
    fallback: cost::EstimatedRows,
) -> cost::EstimatedRows {
    delivered
        .cardinality
        .upper()
        .map(|upper| cost::EstimatedRows::rows(upper as u64))
        .unwrap_or(fallback)
}

pub(in crate::rules) fn with_cardinality(
    delivered: properties::DeliveredProperties,
    upper: Option<usize>,
) -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        cardinality: properties::CardinalityBounds::zero_to(upper),
        ..delivered
    }
}

pub(in crate::rules) fn stream_bound_upper(count: &ir::StreamBoundPlan) -> StreamRowUpperBound {
    match count {
        ir::StreamBoundPlan::Literal(count) => StreamRowUpperBound::from_usize(*count),
        ir::StreamBoundPlan::Expr(_) => StreamRowUpperBound::Unknown,
    }
}

pub(in crate::rules) fn estimated_rows_bounded_by(
    rows: cost::EstimatedRows,
    upper: StreamRowUpperBound,
) -> cost::EstimatedRows {
    upper.clamp_rows(rows)
}

pub(in crate::rules) fn stream_range_upper(range: &ir::StreamRangePlan) -> StreamRowUpperBound {
    match range {
        ir::StreamRangePlan::Literal(range) => {
            StreamRowUpperBound::from_usize(range.end().saturating_sub(range.start()))
        }
        ir::StreamRangePlan::Dynamic(_) => StreamRowUpperBound::Unknown,
    }
}
