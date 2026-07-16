use super::support;
use crate::{context, cost, exec, ir, logical, physical, properties};

#[test]
fn selected_run_root_from_plan_wraps_supported_selected_root_families() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();

    let mutation = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::RootMutation(logical::RootMutation::new(
                support::source_mutation(),
            )),
            support::barrier_alternative(),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        mutation,
        exec::SelectedExecutableRunRoot::Mutation(root)
            if matches!(root.plan(), exec::SelectedMutationPlan::AddNode {
                input: exec::SelectedMutationInput::Source,
                ..
            })
            && root.provenance().optimizer_rule_id().as_ref() == "test_impl"
    ));

    let ddl_plan = ir::IndexDdlPlan::Drop {
        spec: ir::IndexDdlDropSpec::NodeEquality {
            key: crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            uniqueness: crate::catalog::IndexUniqueness::NonUnique,
        },
    };
    let ddl = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::RootIndexDdl(logical::RootIndexDdl::new(ddl_plan)),
            support::barrier_alternative(),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        ddl,
        exec::SelectedExecutableRunRoot::IndexDdl(root)
            if matches!(root.plan(), ir::IndexDdlPlan::Drop { .. })
                && root.provenance().optimizer_rule_id().as_ref() == "test_impl"
    ));

    let pipeline = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::RootPipeline(support::root_pipeline()),
            support::pipeline_alternative(),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        pipeline,
        exec::SelectedExecutableRunRoot::Pipeline(root)
            if matches!(root.input(), exec::SelectedRootStreamInput::VariableSource(_))
                && root.ops().len() == 1
                && root.provenance().optimizer_rule_id().as_ref() == "test_impl"
    ));

    let project = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                support::variable_stream(),
                ir::ProjectionPlan::Count,
            )),
            terminal_pipeline_alternative(physical::PhysicalStreamOp::Project),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        project,
        exec::SelectedExecutableRunRoot::Terminal(root)
            if matches!(root.plan(), exec::SelectedRootTerminal::Project { .. })
    ));

    let aggregate = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
                support::variable_stream(),
                ir::AggregatePlan::Group(support::name("kind")),
            )),
            terminal_pipeline_alternative(physical::PhysicalStreamOp::Aggregate),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        aggregate,
        exec::SelectedExecutableRunRoot::Terminal(root)
            if matches!(root.plan(), exec::SelectedRootTerminal::Aggregate { .. })
    ));

    let reserved = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
                support::variable_stream(),
                ir::ReservedOp::Fold,
            )),
            terminal_pipeline_alternative(physical::PhysicalStreamOp::Reserved),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        reserved,
        exec::SelectedExecutableRunRoot::Terminal(root)
            if matches!(root.plan(), exec::SelectedRootTerminal::Reserved { .. })
    ));

    let write = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
                support::variable_stream(),
                logical::StreamVariableWriteOp::Store(support::name("saved")),
            )),
            terminal_pipeline_alternative(physical::PhysicalStreamOp::Variable),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        write,
        exec::SelectedExecutableRunRoot::Terminal(root)
            if matches!(root.plan(), exec::SelectedRootTerminal::VariableWrite { .. })
    ));

    let ordinary = planner
        .selected_run_root_from_plan(
            logical::LogicalExpr::AccessPath(support::access_path()),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::Access {
                    element: properties::ElementKind::Node,
                    access: physical::PhysicalAccess::LabelScan,
                },
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
            support::optimizer_provenance(),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(
        ordinary,
        exec::SelectedExecutableRunRoot::Alternative(root)
            if root.provenance().optimizer_rule_id().as_ref() == "test_impl"
                && root.family() == exec::SelectedExecutableAlternativeFamily::NODE_ACCESS_PATH
    ));
}

#[test]
fn selected_run_root_from_plan_rejects_selected_root_physical_mismatches() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();
    let ddl_plan = ir::IndexDdlPlan::Drop {
        spec: ir::IndexDdlDropSpec::NodeEquality {
            key: crate::catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
            uniqueness: crate::catalog::IndexUniqueness::NonUnique,
        },
    };
    let cases = vec![
        (
            logical::LogicalExpr::RootMutation(logical::RootMutation::new(
                support::source_mutation(),
            )),
            noop_alternative(),
        ),
        (
            logical::LogicalExpr::RootIndexDdl(logical::RootIndexDdl::new(ddl_plan)),
            support::pipeline_alternative(),
        ),
        (
            logical::LogicalExpr::RootBranch(logical::RootBranch::new(
                support::node_root(),
                support::branch_plan(),
            )),
            support::control_alternative(physical::PhysicalControlOp::Repeat),
        ),
        (
            logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
                support::node_root(),
                support::repeat_plan(),
            )),
            support::control_alternative(physical::PhysicalControlOp::Branch),
        ),
        (
            logical::LogicalExpr::RootPipeline(support::root_pipeline()),
            support::barrier_alternative(),
        ),
        (
            logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                support::variable_stream(),
                ir::ProjectionPlan::Count,
            )),
            support::barrier_alternative(),
        ),
        (
            logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
                support::variable_stream(),
                ir::AggregatePlan::Group(support::name("kind")),
            )),
            support::barrier_alternative(),
        ),
        (
            logical::LogicalExpr::StreamReserved(logical::StreamReserved::new(
                support::variable_stream(),
                ir::ReservedOp::Fold,
            )),
            support::barrier_alternative(),
        ),
        (
            logical::LogicalExpr::StreamVariableWrite(logical::StreamVariableWrite::new(
                support::variable_stream(),
                logical::StreamVariableWriteOp::Store(support::name("saved")),
            )),
            support::barrier_alternative(),
        ),
    ];

    for (source_expr, alternative) in cases {
        assert_eq!(
            planner
                .selected_run_root_from_plan(
                    source_expr,
                    alternative,
                    support::optimizer_provenance(),
                    &mut metrics,
                )
                .unwrap_err(),
            super::super::super::rejection::unsupported(
                super::super::super::rejection::Reason::SelectedRootPhysicalMismatch
            )
        );
    }
}

#[test]
fn selected_run_root_from_plan_rejects_selected_root_suffix_mismatches() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();

    let long_pipeline = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            support::variable_stream(),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Distinct,
                vec![logical::StreamPipelineOp::Distinct],
            ),
        )
        .unwrap(),
    );
    assert_eq!(
        planner
            .selected_run_root_from_plan(
                long_pipeline,
                support::pipeline_alternative(),
                support::optimizer_provenance(),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::SelectedRootPipelineLogicalSuffixTooLong
        )
    );

    let project_pipeline = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Project,
            )),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );
    assert_eq!(
        planner
            .selected_run_root_from_plan(
                logical::LogicalExpr::RootPipeline(support::root_pipeline()),
                project_pipeline,
                support::optimizer_provenance(),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::SelectedRootPipelinePhysicalSuffixMismatch
        )
    );

    let aggregate_pipeline = physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Aggregate,
            )),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );
    assert_eq!(
        planner
            .selected_run_root_from_plan(
                logical::LogicalExpr::StreamProject(logical::StreamProject::new(
                    support::variable_stream(),
                    ir::ProjectionPlan::Count,
                )),
                aggregate_pipeline,
                support::optimizer_provenance(),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::SelectedRootTerminalPhysicalSuffixMismatch
        )
    );
}

#[test]
fn selected_run_root_from_plan_rejects_non_executable_generic_alternatives() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();
    let cases = vec![
        (
            logical::LogicalExpr::AccessPath(support::access_path()),
            noop_alternative(),
        ),
        (
            logical::LogicalExpr::Pure(logical::PureLogicalOp::Empty),
            empty_alternative(),
        ),
        (
            logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter {
                predicate: support::predicate(),
            }),
            residual_filter_alternative(),
        ),
        (
            logical::LogicalExpr::PurePipeline(logical::PurePipeline::new(
                ir::AtLeast::<_, 1>::from_one(logical::PureLogicalOp::Distinct),
            )),
            support::pipeline_alternative(),
        ),
        (
            logical::LogicalExpr::Barrier(logical::BarrierLogicalOp::Mutation),
            support::barrier_alternative(),
        ),
    ];

    for (source_expr, alternative) in cases {
        assert_eq!(
            planner
                .selected_run_root_from_plan(
                    source_expr,
                    alternative,
                    support::optimizer_provenance(),
                    &mut metrics,
                )
                .unwrap_err(),
            super::super::super::rejection::unsupported(
                super::super::super::rejection::Reason::SelectedAlternativeUnsupported
            )
        );
    }
}

fn noop_alternative() -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::NoOp,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}

fn terminal_pipeline_alternative(op: physical::PhysicalStreamOp) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(op)),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}

fn empty_alternative() -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Empty,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}

fn residual_filter_alternative() -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::ResidualFilter,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}
