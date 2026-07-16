use super::super::super::{
    SelectedExecutableAlternativeClassification, SelectedExecutableAlternativeFamily,
};
use super::super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_classified_selected_executable_alternative(
        &mut self,
        family: SelectedExecutableAlternativeFamily,
        source_expr: &logical::LogicalExpr,
        alternative: &SelectedPhysicalPlan,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        debug_assert_eq!(
            SelectedExecutableAlternativeFamily::classify(source_expr, alternative.expr()),
            SelectedExecutableAlternativeClassification::Classified(family)
        );
        match (family, source_expr, alternative.expr()) {
            (
                SelectedExecutableAlternativeFamily::NodeAccessPath,
                logical::LogicalExpr::AccessPath(logical::AccessPath::Node(path)),
                physical::PhysicalExpr::Access { access, .. },
            ) if selected_node_access_matches(path.source().as_ref(), access) => self
                .push_selected_node_access(
                    path.source().as_ref(),
                    access,
                    dependencies,
                    output,
                    condition,
                ),
            (
                SelectedExecutableAlternativeFamily::EdgeAccessPath,
                logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(path)),
                physical::PhysicalExpr::Access { access, .. },
            ) if selected_edge_access_matches(path.source().as_ref(), access) => self
                .push_selected_edge_access(
                    path.source().as_ref(),
                    access,
                    dependencies,
                    output,
                    condition,
                ),
            (
                SelectedExecutableAlternativeFamily::KvSource,
                logical::LogicalExpr::Pure(logical::PureLogicalOp::Source { element }),
                physical::PhysicalExpr::Access {
                    element: physical_element,
                    access: physical::PhysicalAccess::Kv(read),
                },
            ) if element == physical_element => self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::KvRead(read.clone()),
                schedule: ExecSchedule::Pipeline,
                delivered: alternative.delivered().clone(),
                cost: alternative.cost(),
            }),
            (
                SelectedExecutableAlternativeFamily::NoOp,
                logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
                physical::PhysicalExpr::NoOp,
            ) => self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::Noop,
                schedule: ExecSchedule::Pipeline,
                delivered: alternative.delivered().clone(),
                cost: alternative.cost(),
            }),
            (
                SelectedExecutableAlternativeFamily::VariableSource,
                logical::LogicalExpr::VariableSource(source),
                physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
            ) => self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::Variable {
                    op: ExecVariableOp::SourceInject {
                        variable: source.variable().clone(),
                    },
                },
                schedule: ExecSchedule::Pipeline,
                delivered: alternative.delivered().clone(),
                cost: alternative.cost(),
            }),
            (
                SelectedExecutableAlternativeFamily::AccessFilterPipeline,
                logical::LogicalExpr::AccessFilter(filter),
                physical::PhysicalExpr::Pipeline(pipeline),
            ) => {
                self.push_selected_access_filter(filter, pipeline, dependencies, output, condition)
            }
            (
                SelectedExecutableAlternativeFamily::AccessWindowPipeline,
                logical::LogicalExpr::AccessWindow(window),
                physical::PhysicalExpr::Pipeline(pipeline),
            ) => {
                self.push_selected_access_window(window, pipeline, dependencies, output, condition)
            }
            (
                SelectedExecutableAlternativeFamily::AccessOrderPipeline,
                logical::LogicalExpr::AccessOrder(order),
                physical::PhysicalExpr::Pipeline(pipeline),
            ) => self.push_selected_access_order(order, pipeline, dependencies, output, condition),
            (
                SelectedExecutableAlternativeFamily::AccessDistinctPipeline,
                logical::LogicalExpr::AccessDistinct(distinct),
                physical::PhysicalExpr::Pipeline(pipeline),
            ) => self.push_selected_access_distinct(
                distinct,
                pipeline,
                dependencies,
                output,
                condition,
            ),
            (
                SelectedExecutableAlternativeFamily::AccessPipeline,
                logical::LogicalExpr::AccessPipeline(access_pipeline),
                physical::PhysicalExpr::Pipeline(pipeline),
            ) => self.push_selected_access_pipeline(
                access_pipeline,
                pipeline,
                dependencies,
                output,
                condition,
            ),
            _ => Err(unsupported_selected_alternative(
                rejection::Reason::LogicalPhysicalAlternativeMismatch,
            )),
        }
    }
}
