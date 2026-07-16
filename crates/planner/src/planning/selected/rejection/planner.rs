//! Planner-error conversion for selected-root reconstruction failures.

use super::reason::Reason;
use crate::error;

/// Build a stable planner error for selected-root reconstruction failures.
pub(in crate::planning::selected) fn unsupported(reason: Reason) -> error::PlannerError {
    error::PlannerError::UnsupportedCascadesPlan {
        reason: reason.as_str().to_owned(),
    }
}
