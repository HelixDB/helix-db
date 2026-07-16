//! Selected executable DAG finalization.
//!
//! Generic DAG allocation owns step IDs and draft storage. Selected lowering
//! owns the rejection reasons for empty or unrooted selected executable DAGs.

use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec) fn finish_with_root(
        self,
        root: ExecStepId,
        empty_reason: rejection::Reason,
    ) -> Result<ExecutableSubplan, ExecPlanError> {
        let steps = ir::AtLeast::<_, 1>::try_from_vec(self.steps)
            .ok_or_else(|| rejection::unsupported(empty_reason))?;
        ExecutableSubplan::new(steps, root)
    }

    pub(in crate::exec) fn finish_with_previous(
        self,
        missing_reason: rejection::Reason,
        empty_reason: rejection::Reason,
    ) -> Result<ExecutableSubplan, ExecPlanError> {
        let root = self
            .previous
            .ok_or_else(|| rejection::unsupported(missing_reason))?;
        self.finish_with_root(root, empty_reason)
    }
}
