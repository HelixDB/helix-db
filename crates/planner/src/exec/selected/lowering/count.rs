//! One-to-one lowering for selected exact count programs.

use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_count_root(
        &mut self,
        count: SelectedRootCount,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let (alternative, _provenance, input, plan) = count.into_parts();
        let dependencies = match input {
            SelectedCountInput::Direct => dependencies,
            SelectedCountInput::Rows(root) | SelectedCountInput::Scalars(root) => {
                let input = self.push_selected_run_root(
                    *root,
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition.clone(),
                )?;
                vec![input]
            }
        };
        let (delivered, cost) = alternative.clone_contract();
        self.push_step(StepDraft {
            dependencies,
            output,
            condition,
            op: ExecOp::Count {
                plan: Box::new(plan),
            },
            schedule: ExecSchedule::Barrier,
            delivered,
            cost,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependent_count_propagates_child_lowering_failure() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::with_next_id(&profile, None);
        let child = SelectedExecutableRunRoot::alternative(
            logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::NoOp,
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
        );
        let plan = crate::exec::ExecCountPlan::InputRows {
            window: crate::exec::ExecCountWindowPlan::identity(),
        };
        let alternative = SelectedPhysicalPlan::new(
            physical::PhysicalExpr::Cardinality(Box::new(physical::PhysicalCountPlan::new(plan))),
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        );
        let count = SelectedRootCount::new(
            alternative,
            crate::exec::selected::provenance::test_selected_root_provenance(),
            SelectedCountInput::Rows(Box::new(child)),
        )
        .unwrap();

        assert_eq!(
            lowering.push_selected_count_root(
                count,
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            ),
            Err(ExecPlanError::StepIdSpaceExhausted)
        );
    }
}
