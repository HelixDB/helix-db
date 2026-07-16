//! Node access-plan executable allocation.

use super::super::*;
use crate::exec;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_node_access_plan(
        &mut self,
        plan: &ir::NodeAccessPlan,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        self.push_selected_node_access_plan_with_read_limit(
            plan,
            exec::ExecAccessReadLimit::Unbounded,
            dependencies,
            output,
            condition,
        )
    }

    pub(in crate::exec::selected::lowering) fn push_selected_node_access_plan_with_read_limit(
        &mut self,
        plan: &ir::NodeAccessPlan,
        read_limit: exec::ExecAccessReadLimit,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let read_limit =
            read_limit.elide_if_covered_by_hard_upper(exec::node_access_hard_upper_bound(plan));
        match plan {
            ir::NodeAccessPlan::PointIds { ids } => self.push_selected_point_ids(
                exec::ElementKeyspace::NodeProperty,
                ids,
                read_limit,
                dependencies,
                output,
                condition,
            ),
            ir::NodeAccessPlan::Empty
            | ir::NodeAccessPlan::FromParam { .. }
            | ir::NodeAccessPlan::FromVar { .. }
            | ir::NodeAccessPlan::AllScan
            | ir::NodeAccessPlan::LabelScan { .. }
            | ir::NodeAccessPlan::EqualityIndex { .. }
            | ir::NodeAccessPlan::RangeIndex { .. }
            | ir::NodeAccessPlan::VectorSearch { .. }
            | ir::NodeAccessPlan::TextSearch { .. } => {
                let exec_access = read_limit.apply_to(exec::ExecAccessPlan::Node(
                    exec::node_exec_access(exec::SimpleNodeAccessLeaf::try_from(plan)?),
                ));
                self.push_step(StepDraft {
                    dependencies,
                    output,
                    condition,
                    op: ExecOp::Access {
                        plan: Box::new(exec_access),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: node_access_delivered_properties(plan),
                    cost: node_access_cost(plan, self.profile),
                })
            }
            ir::NodeAccessPlan::Union(plans) => {
                let delivered = node_access_delivered_properties(plan);
                let root = self.push_selected_node_access_merge(
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
            ir::NodeAccessPlan::Intersect(plans) => {
                let delivered = node_access_delivered_properties(plan);
                let root = self.push_selected_node_access_merge(
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
            ir::NodeAccessPlan::ScanThenFilter { source, residual } => {
                let source_id = self.push_selected_node_access_plan(
                    source.as_ref(),
                    dependencies,
                    ir::BatchOutputPlan::Discard,
                    condition.clone(),
                )?;
                let delivered = filtered_delivered_properties(node_access_delivered_properties(
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
                        exec::node_access_hard_upper_bound(source.as_ref()).map(|rows| rows as u64),
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
    use helix_ast::expr::Predicate;

    fn element_ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    fn node_source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
        ir::NodeAccessSourcePlan::new(plan).unwrap()
    }

    #[test]
    fn node_union_allocates_child_reads_before_set_merge() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let plan = ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_source(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![7]),
            }),
            node_source(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![3]),
            }),
        ));

        let root = lowering
            .push_selected_node_access_plan(
                &plan,
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 3);
        assert_eq!(lowering.steps.len(), 3);
        assert!(matches!(
            lowering.steps[0].op,
            ExecOp::KvRead(exec::KvReadPlan::Get { .. })
        ));
        assert!(matches!(
            lowering.steps[1].op,
            ExecOp::KvRead(exec::KvReadPlan::Get { .. })
        ));
        assert!(matches!(
            lowering.steps[2].op,
            ExecOp::Merge {
                mode: exec::ExecMergeMode::Union
            }
        ));
        assert_eq!(
            lowering.steps[2].dependencies,
            vec![lowering.steps[0].id, lowering.steps[1].id]
        );
    }

    #[test]
    fn node_union_read_limit_is_preserved_after_set_merge() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let plan = ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_source(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![7]),
            }),
            node_source(ir::NodeAccessPlan::PointIds {
                ids: element_ids(vec![3]),
            }),
        ));

        let root = lowering
            .push_selected_node_access_plan_with_read_limit(
                &plan,
                exec::ExecAccessReadLimit::bounded(properties::PositiveUsize::at_least_one(1)),
                Vec::new(),
                ir::BatchOutputPlan::Bind(ir::NonEmptyString::new("out").unwrap()),
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 4);
        assert_eq!(lowering.steps.len(), 4);
        assert!(matches!(
            lowering.steps[2].op,
            ExecOp::Merge {
                mode: exec::ExecMergeMode::Union
            }
        ));
        assert_eq!(lowering.steps[2].output, ir::BatchOutputPlan::Discard);
        assert!(matches!(
            lowering.steps[3].op,
            ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(1)
            }
        ));
        assert_eq!(lowering.steps[3].dependencies, vec![lowering.steps[2].id]);
        assert_eq!(
            lowering.steps[3].output,
            ir::BatchOutputPlan::Bind(ir::NonEmptyString::new("out").unwrap())
        );
    }

    #[test]
    fn node_scan_then_filter_allocates_filter_after_source_access() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let predicate = ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap();
        let plan = ir::NodeAccessPlan::ScanThenFilter {
            source: node_source(ir::NodeAccessPlan::AllScan),
            residual: predicate.clone(),
        };

        let root = lowering
            .push_selected_node_access_plan(
                &plan,
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 2);
        assert_eq!(lowering.steps.len(), 2);
        assert!(matches!(
            &lowering.steps[0].op,
            ExecOp::Access { plan }
                if matches!(plan.as_ref(), exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan))
        ));
        assert!(matches!(
            &lowering.steps[1].op,
            ExecOp::Filter { predicate: actual } if actual == &predicate
        ));
        assert_eq!(lowering.steps[1].dependencies, vec![lowering.steps[0].id]);
    }

    #[test]
    fn node_scan_then_filter_read_limit_is_preserved_after_filter() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let predicate = ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap();
        let plan = ir::NodeAccessPlan::ScanThenFilter {
            source: node_source(ir::NodeAccessPlan::AllScan),
            residual: predicate,
        };

        let root = lowering
            .push_selected_node_access_plan_with_read_limit(
                &plan,
                exec::ExecAccessReadLimit::bounded(properties::PositiveUsize::at_least_one(1)),
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 3);
        assert_eq!(lowering.steps.len(), 3);
        assert!(matches!(&lowering.steps[1].op, ExecOp::Filter { .. }));
        assert_eq!(lowering.steps[1].output, ir::BatchOutputPlan::Discard);
        assert!(matches!(
            lowering.steps[2].op,
            ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(1)
            }
        ));
        assert_eq!(lowering.steps[2].dependencies, vec![lowering.steps[1].id]);
    }
}
