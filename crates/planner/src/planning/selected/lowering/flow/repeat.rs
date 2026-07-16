//! Selected repeat reconstruction from memo-child plans.

use super::super::super::{rejection, SelectedCascadesPlanner};
use super::super::memo_children;
use crate::{error, exec, logical};

impl SelectedCascadesPlanner<'_> {
    pub(in crate::planning::selected::lowering) fn selected_repeat_input_and_plan(
        &mut self,
        repeat: &logical::RootRepeat,
        child_plans: memo_children::MemoChildPlanContext<'_, '_>,
        metrics: &mut exec::PlannerMetrics,
    ) -> Result<(exec::SelectedExecutableRunRoot, exec::SelectedRepeatPlan), error::PlannerError>
    {
        let selected = self.selected_flow_children(2, child_plans, metrics)?;
        let selected = SelectedRepeatRoots::new(selected)?;
        Ok((
            selected.input,
            exec::SelectedRepeatPlan {
                body: Box::new(selected.body),
                stop: repeat.plan().stop.clone(),
                emit: repeat.plan().emit.clone(),
                max_depth: repeat.plan().max_depth,
            },
        ))
    }
}

#[derive(Debug)]
struct SelectedRepeatRoots {
    input: exec::SelectedExecutableRunRoot,
    body: exec::SelectedExecutableRunRoot,
}

impl SelectedRepeatRoots {
    fn new(selected: Vec<exec::SelectedExecutableRunRoot>) -> Result<Self, error::PlannerError> {
        let mut selected = selected.into_iter();
        let input = selected
            .next()
            .ok_or_else(|| rejection::unsupported(rejection::Reason::RepeatRootArityMismatch))?;
        let body = selected
            .next()
            .ok_or_else(|| rejection::unsupported(rejection::Reason::RepeatRootArityMismatch))?;
        if selected.next().is_some() {
            return Err(rejection::unsupported(
                rejection::Reason::RepeatRootArityMismatch,
            ));
        }
        Ok(Self { input, body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cost, logical, physical, properties};

    fn selected_root() -> exec::SelectedExecutableRunRoot {
        exec::SelectedExecutableRunRoot::alternative(
            logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::NoOp,
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
        )
    }

    #[test]
    fn repeat_root_split_requires_exact_input_and_body() {
        assert!(SelectedRepeatRoots::new(vec![selected_root(), selected_root()]).is_ok());
        assert_eq!(
            SelectedRepeatRoots::new(vec![selected_root()]).unwrap_err(),
            rejection::unsupported(rejection::Reason::RepeatRootArityMismatch)
        );
        assert_eq!(
            SelectedRepeatRoots::new(vec![selected_root(), selected_root(), selected_root()])
                .unwrap_err(),
            rejection::unsupported(rejection::Reason::RepeatRootArityMismatch)
        );
    }
}
