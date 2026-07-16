//! Planner-error conversion for unsupported native selected shapes.

use super::reason::NativeUnsupportedReason;
use crate::error;

/// Build a stable planner error for an unsupported native selected shape.
pub(in crate::planning::selected::native) fn unsupported(
    reason: NativeUnsupportedReason,
) -> error::PlannerError {
    error::PlannerError::UnsupportedCascadesPlan {
        reason: reason.as_str().to_owned(),
    }
}
