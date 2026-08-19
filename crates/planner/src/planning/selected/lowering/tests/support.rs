use std::num::NonZeroUsize;

use super::super::super::{root::SelectableRunRoot, SelectedCascadesPlanner};
use super::super::memo_children;
use crate::{context, cost, exec, ir, logical, memo, optimizer, physical, properties, rules};

pub(super) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("test names are non-empty")
}

pub(super) fn node_root() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    )))
}

pub(super) fn edge_root() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(logical::EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::AllScan).unwrap(),
    )))
}

pub(super) fn ids(id: u64) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(id)).unwrap()
}

pub(super) fn predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap()
}

pub(super) fn source_mutation() -> ir::MutationPlan<logical::LogicalExpr> {
    ir::MutationPlan::AddNode {
        input: ir::MutationInput::Source,
        label: name("User"),
        properties: ir::PropertyAssignments::default(),
    }
}

pub(super) fn input_mutation() -> ir::MutationPlan<logical::LogicalExpr> {
    ir::MutationPlan::SetProperty {
        input: Box::new(node_root()),
        name: name("active"),
        value: ir::PropertyInputPlan::Value(helix_ast::value::PropertyValue::Bool(true)),
    }
}

pub(super) fn branch_plan() -> ir::BranchPlan<logical::LogicalExpr> {
    ir::BranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(node_root(), edge_root()))
}

pub(super) fn repeat_plan() -> ir::RepeatPlan<logical::LogicalExpr> {
    ir::RepeatPlan {
        body: Box::new(node_root()),
        stop: ir::RepeatStopPlan::MaxDepthOnly,
        emit: ir::RepeatEmitPlan::None,
        max_depth: NonZeroUsize::new(2).unwrap(),
    }
}

pub(super) fn variable_source() -> logical::VariableSource {
    logical::VariableSource::new(name("seed"))
}

pub(super) fn variable_stream() -> logical::RootStream {
    logical::RootStream::VariableSource(variable_source())
}

pub(super) fn access_path() -> logical::AccessPath {
    logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    ))
}

pub(super) fn root_pipeline() -> logical::RootPipeline {
    logical::RootPipeline::new(
        variable_stream(),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
    )
    .unwrap()
}

pub(super) fn nested_root_pipeline() -> logical::LogicalExpr {
    logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Pipeline(Box::new(root_pipeline())),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap(),
    )
}

pub(super) fn selected_optimizer_group(
    root: &exec::SelectedExecutableRunRoot,
) -> memo::MemoGroupId {
    match root {
        exec::SelectedExecutableRunRoot::Alternative(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Mutation(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::IndexDdl(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Branch(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Repeat(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::ShortestPath(root) => {
            root.provenance().optimizer().group()
        }
        exec::SelectedExecutableRunRoot::Pipeline(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Terminal(root) => root.provenance().optimizer().group(),
        exec::SelectedExecutableRunRoot::Count(root) => root.provenance().optimizer().group(),
    }
}

pub(super) fn barrier_alternative() -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Barrier,
        properties::DeliveredProperties::default(),
        cost::CostVector {
            cpu_units: 1,
            ..cost::CostVector::ZERO
        },
    )
}

pub(super) fn control_alternative(
    op: physical::PhysicalControlOp,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Control(op),
        properties::DeliveredProperties::default(),
        cost::CostVector {
            cpu_units: 1,
            ..cost::CostVector::ZERO
        },
    )
}

pub(super) fn pipeline_alternative() -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(
            ir::AtLeast::<_, 1>::from_one(physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Distinct,
            )),
        )),
        properties::DeliveredProperties::default(),
        cost::CostVector {
            cpu_units: 1,
            ..cost::CostVector::ZERO
        },
    )
}

pub(super) fn optimizer_provenance() -> exec::SelectedRootProvenance {
    exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
        rules::RuleId::new("test_impl").unwrap(),
        memo::MemoGroupId::new(1).unwrap(),
        memo::MemoExprId::new(1).unwrap(),
        memo::PhysicalAlternativeId::new(1).unwrap(),
        memo::MemoChildGroups::empty(),
    ))
}

pub(super) fn optimizer_result(
    ctx: &context::PlannerContext,
    root: logical::LogicalExpr,
) -> optimizer::OptimizationResult {
    let config = optimizer::OptimizerConfig::from_context(ctx);
    rules::SeedRuleSet::default()
        .optimizer()
        .optimize(root, &config)
        .expect("test optimizer memo allocation should fit")
}

pub(super) fn provenance_from_selected(
    selected: optimizer::SelectedPhysicalAlternative<'_>,
) -> exec::SelectedRootProvenance {
    exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
        selected.entry.provenance.rule_id().clone(),
        selected.group,
        selected.source_expr.id,
        selected.entry.id,
        selected.source_expr.children.clone(),
    ))
}

pub(super) fn root_child_context<'result, 'selection>(
    result: &'result optimizer::OptimizationResult,
    selection: &'selection mut optimizer::SelectionSession<'result>,
) -> memo_children::MemoChildPlanContext<'result, 'selection> {
    let selected = selection
        .best_plan(result.root())
        .expect("test root has a selected physical alternative");
    let provenance = provenance_from_selected(selected);
    memo_children::MemoChildPlanContext::from_selection_and_provenance(selection, &provenance)
}

pub(super) fn root_child_availability<'result, 'selection>(
    result: &'result optimizer::OptimizationResult,
    selection: &'selection mut optimizer::SelectionSession<'result>,
) -> memo_children::MemoChildPlanAvailability<'result, 'selection> {
    memo_children::MemoChildPlanAvailability::Available(root_child_context(result, selection))
}

pub(super) fn selected_root_stream_with_parent_context(
    planner: &mut SelectedCascadesPlanner<'_>,
    ctx: &context::PlannerContext,
    input: &logical::RootStream,
    metrics: &mut exec::PlannerMetrics,
) -> Result<exec::SelectedRootStreamInput, crate::error::PlannerError> {
    let parent = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            input.clone(),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
        )
        .expect("test parent pipeline is canonical"),
    );
    let result = optimizer_result(ctx, parent);
    let mut selection = result.selection_session();
    let child_plans = root_child_availability(&result, &mut selection);
    planner.selected_root_stream_input_with_memo_children(input, child_plans, metrics)
}

pub(super) fn selectable_root(expr: logical::LogicalExpr) -> SelectableRunRoot {
    SelectableRunRoot::new(expr)
}

pub(super) fn selected_planner(
    ctx: &crate::context::PlannerContext,
) -> SelectedCascadesPlanner<'_> {
    SelectedCascadesPlanner::new(ctx)
}
