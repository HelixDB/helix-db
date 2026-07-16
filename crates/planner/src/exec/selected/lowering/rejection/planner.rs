use super::Reason;
use crate::{exec::ExecPlanError, ir};

/// Build a stable executable-planner error for an unsupported selected shape.
pub(in crate::exec::selected::lowering) fn unsupported(reason: Reason) -> ExecPlanError {
    ExecPlanError::UnsupportedSelectedExecutableAlternative {
        reason: ir::NonEmptyString::from_static(reason.as_str()),
    }
}
