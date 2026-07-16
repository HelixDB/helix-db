use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec) fn override_step_contract(
        &mut self,
        id: ExecStepId,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Result<(), ExecPlanError> {
        let step = self
            .steps
            .iter_mut()
            .find(|step| step.id == id)
            .ok_or_else(|| {
                unsupported_selected_alternative(rejection::Reason::UnknownLoweredStep)
            })?;
        step.delivered = delivered;
        step.cost = cost;
        Ok(())
    }
}
