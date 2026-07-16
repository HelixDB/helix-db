//! Edge access-plan executable allocation.

use super::super::*;
use crate::exec;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_edge_access_plan(
        &mut self,
        plan: &ir::EdgeAccessPlan,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        self.push_selected_edge_access_plan_with_read_limit(
            plan,
            exec::ExecAccessReadLimit::Unbounded,
            dependencies,
            output,
            condition,
        )
    }

    pub(in crate::exec::selected::lowering) fn push_selected_edge_access_plan_with_read_limit(
        &mut self,
        plan: &ir::EdgeAccessPlan,
        read_limit: exec::ExecAccessReadLimit,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let read_limit =
            read_limit.elide_if_covered_by_hard_upper(exec::edge_access_hard_upper_bound(plan));
        match plan {
            ir::EdgeAccessPlan::PointIds { ids } => self.push_selected_point_ids(
                exec::ElementKeyspace::EdgeEndpoints,
                ids,
                read_limit,
                dependencies,
                output,
                condition,
            ),
            ir::EdgeAccessPlan::Empty
            | ir::EdgeAccessPlan::FromParam { .. }
            | ir::EdgeAccessPlan::FromVar { .. }
            | ir::EdgeAccessPlan::AllScan
            | ir::EdgeAccessPlan::LabelScan { .. }
            | ir::EdgeAccessPlan::EqualityIndex { .. }
            | ir::EdgeAccessPlan::RangeIndex { .. }
            | ir::EdgeAccessPlan::VectorSearch { .. }
            | ir::EdgeAccessPlan::TextSearch { .. } => {
                let exec_access = read_limit.apply_to(exec::ExecAccessPlan::Edge(
                    exec::edge_exec_access(exec::SimpleEdgeAccessLeaf::try_from(plan)?),
                ));
                self.push_step(StepDraft {
                    dependencies,
                    output,
                    condition,
                    op: ExecOp::Access {
                        plan: Box::new(exec_access),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: edge_access_delivered_properties(plan),
                    cost: edge_access_cost(plan, self.profile),
                })
            }
            ir::EdgeAccessPlan::Union(plans) => {
                let delivered = edge_access_delivered_properties(plan);
                let root = self.push_selected_edge_access_merge(
                    plans,
                    exec::ExecMergeMode::Union,
                    dependencies,
                    super::compound_access_output(read_limit, &output),
                    condition.clone(),
                    delivered.clone(),
                )?;
                super::push_compound_access_read_limit(
                    self, root, read_limit, delivered, output, condition,
                )
            }
            ir::EdgeAccessPlan::Intersect(plans) => {
                let delivered = edge_access_delivered_properties(plan);
                let root = self.push_selected_edge_access_merge(
                    plans,
                    exec::ExecMergeMode::Intersect,
                    dependencies,
                    super::compound_access_output(read_limit, &output),
                    condition.clone(),
                    delivered.clone(),
                )?;
                super::push_compound_access_read_limit(
                    self, root, read_limit, delivered, output, condition,
                )
            }
            ir::EdgeAccessPlan::ScanThenFilter { source, residual } => {
                let source_id = self.push_selected_edge_access_plan(
                    source.as_ref(),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition.clone(),
                )?;
                let delivered = filtered_delivered_properties(edge_access_delivered_properties(
                    source.as_ref(),
                ));
                self.push_step(StepDraft {
                    dependencies: vec![source_id],
                    output: super::compound_access_output(read_limit, &output),
                    condition: condition.clone(),
                    op: ExecOp::Filter {
                        predicate: residual.clone(),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: delivered.clone(),
                    cost: predicate_cost_for_rows(
                        self.profile,
                        exec::edge_access_hard_upper_bound(source.as_ref()).map(|rows| rows as u64),
                    ),
                })
                .and_then(|root| {
                    super::push_compound_access_read_limit(
                        self, root, read_limit, delivered, output, condition,
                    )
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element_ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    fn edge_source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
        ir::EdgeAccessSourcePlan::new(plan).unwrap()
    }

    #[test]
    fn edge_intersection_allocates_child_reads_before_set_merge() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let plan = ir::EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            edge_source(ir::EdgeAccessPlan::PointIds {
                ids: element_ids(vec![10]),
            }),
            edge_source(ir::EdgeAccessPlan::PointIds {
                ids: element_ids(vec![11]),
            }),
        ));

        let root = lowering
            .push_selected_edge_access_plan(
                &plan,
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 3);
        assert_eq!(lowering.steps.len(), 3);
        assert!(matches!(
            lowering.steps[2].op,
            ExecOp::Merge {
                mode: exec::ExecMergeMode::Intersect
            }
        ));
        assert_eq!(
            lowering.steps[2].dependencies,
            vec![lowering.steps[0].id, lowering.steps[1].id]
        );
    }

    #[test]
    fn edge_intersection_read_limit_is_elided_when_hard_bound_covers_it() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let plan = ir::EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
            edge_source(ir::EdgeAccessPlan::PointIds {
                ids: element_ids(vec![10]),
            }),
            edge_source(ir::EdgeAccessPlan::PointIds {
                ids: element_ids(vec![11]),
            }),
        ));

        let root = lowering
            .push_selected_edge_access_plan_with_read_limit(
                &plan,
                exec::ExecAccessReadLimit::bounded(properties::PositiveUsize::at_least_one(1)),
                Vec::new(),
                ir::BatchOutputPlan::Bind(ir::NonEmptyString::new("out").unwrap()),
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 3);
        assert_eq!(lowering.steps.len(), 3);
        assert!(matches!(
            lowering.steps[2].op,
            ExecOp::Merge {
                mode: exec::ExecMergeMode::Intersect
            }
        ));
        assert_eq!(
            lowering.steps[2].output,
            ir::BatchOutputPlan::Bind(ir::NonEmptyString::new("out").unwrap())
        );
    }
}
