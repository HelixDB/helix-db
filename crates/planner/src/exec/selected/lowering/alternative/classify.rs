use super::super::super::{
    SelectedExecutableAlternativeClassification, SelectedExecutableAlternativeFamily,
};
use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec) fn push_selected_executable_alternative(
        &mut self,
        source_expr: &logical::LogicalExpr,
        alternative: &SelectedPhysicalPlan,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let family =
            match SelectedExecutableAlternativeFamily::classify(source_expr, alternative.expr()) {
                SelectedExecutableAlternativeClassification::Classified(family) => family,
                SelectedExecutableAlternativeClassification::Unsupported => {
                    return Err(unsupported_selected_alternative(
                        rejection::Reason::LogicalPhysicalAlternativeMismatch,
                    ));
                }
            };
        self.push_classified_selected_executable_alternative(
            family,
            source_expr,
            alternative,
            dependencies,
            output,
            condition,
        )
    }
}
