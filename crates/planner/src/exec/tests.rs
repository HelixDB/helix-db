use super::*;
use std::num::NonZeroUsize;

use helix_ast::expr::Predicate;
use helix_ast::index::RangeIndexDirection;
use helix_ast::traversal::Order;
use helix_ast::value::{PropertyInput, PropertyValue};

use crate::{memo, rules};

fn id(value: usize) -> ExecStepId {
    ExecStepId::new(value).unwrap()
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

fn element_ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
}

fn predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap()
}

fn selected_root_provenance() -> SelectedRootProvenance {
    SelectedRootProvenance::from_optimizer(SelectedOptimizerProvenance::new(
        rules::RuleId::new("test_exec_selected").unwrap(),
        memo::MemoGroupId::new(1).unwrap(),
        memo::MemoExprId::new(1).unwrap(),
        memo::PhysicalAlternativeId::new(1).unwrap(),
        memo::MemoChildGroups::empty(),
    ))
}

fn node_source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::new(plan).unwrap()
}

fn edge_source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
    ir::EdgeAccessSourcePlan::new(plan).unwrap()
}

fn index_value(value: impl Into<PropertyValue>) -> ir::IndexValue {
    ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value.into()).unwrap())
}

fn lower_range(value: i64) -> ir::IndexRange {
    ir::IndexRange::Lower {
        lower: ir::IndexBound::Inclusive(
            ir::RangeIndexValue::literal(PropertyValue::from(value)).unwrap(),
        ),
    }
}

fn search_index_plan() -> ir::SearchIndexPlan {
    ir::SearchIndexPlan {
        index_id: name("search_idx"),
        tenant: ir::SearchTenantPlan::Unscoped,
    }
}

fn literal_search_limit(value: usize) -> ir::SearchLimitPlan {
    ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).unwrap())
}

fn step(value: usize, dependencies: Vec<ExecStepId>, schedule: ExecSchedule) -> ExecStep {
    ExecStep {
        id: id(value),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        condition: ExecCondition::Always,
        op: ExecOp::Noop,
        schedule,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

fn run_step(value: usize, dependencies: Vec<ExecStepId>, condition: ExecCondition) -> ExecStep {
    ExecStep {
        id: id(value),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        condition,
        op: ExecOp::Noop,
        schedule: ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

fn key(space: ElementKeyspace, id: u64) -> KvKey {
    space.point_key(id)
}

fn executable(
    steps: ir::AtLeast<ExecStep, 1>,
    root: ExecStepId,
) -> Result<ExecutablePlan, ExecPlanError> {
    ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        steps,
        root,
        trace::PlanningTrace::default(),
        PlannerMetrics::default(),
    )
}

fn node_access_expr(plan: ir::NodeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(plan).unwrap(),
    )))
}

fn edge_access_expr(plan: ir::EdgeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(logical::EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::new(plan).unwrap(),
    )))
}

fn logical_optional_branch(
    input: logical::LogicalExpr,
    body: logical::LogicalExpr,
) -> logical::LogicalExpr {
    logical::LogicalExpr::RootBranch(logical::RootBranch::new(
        input,
        ir::BranchPlan::Optional(Box::new(body)),
    ))
}

fn logical_repeat(
    input: logical::LogicalExpr,
    body: logical::LogicalExpr,
    max_depth: usize,
) -> logical::LogicalExpr {
    logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        input,
        ir::RepeatPlan {
            body: Box::new(body),
            stop: ir::RepeatStopPlan::MaxDepthOnly,
            emit: ir::RepeatEmitPlan::None,
            max_depth: NonZeroUsize::new(max_depth).unwrap(),
        },
    ))
}

fn node_access_path(plan: ir::NodeAccessPlan) -> logical::AccessPath {
    logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(plan).unwrap(),
    ))
}

fn node_access_filter_expr(
    plan: ir::NodeAccessPlan,
    predicate: ir::PredicatePlan,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessFilter(logical::AccessFilter::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )),
        predicate,
    ))
}

fn node_access_window_expr(
    plan: ir::NodeAccessPlan,
    window: logical::AccessWindowRange,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessWindow(logical::AccessWindow::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )),
        window,
    ))
}

fn node_access_order_expr(
    plan: ir::NodeAccessPlan,
    ordering: ir::OrderKeys,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessOrder(logical::AccessOrder::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )),
        ordering,
    ))
}

fn node_access_distinct_expr(plan: ir::NodeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessDistinct(logical::AccessDistinct::new(logical::AccessPath::Node(
        logical::NodeAccessPath::new(ir::NodeAccessSourcePlan::new(plan).unwrap()),
    )))
}

fn selected_kv_node_access() -> physical::PhysicalPipelineOp {
    physical::PhysicalPipelineOp::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::NodeProperty,
            start: KvKeyBound::Unbounded,
            end: KvKeyBound::Unbounded,
            limit: None,
        }),
    }
}

fn selected_kv_node_scan() -> physical::PhysicalAlternative {
    let profile = cost::StorageCostProfile::default();
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Access {
            element: properties::ElementKind::Node,
            access: physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
                keyspace: ElementKeyspace::NodeProperty,
                start: KvKeyBound::Unbounded,
                end: KvKeyBound::Unbounded,
                limit: None,
            }),
        },
        properties::DeliveredProperties {
            element: Some(properties::ElementKind::Node),
            ..properties::DeliveredProperties::default()
        },
        profile.range_scan(profile.default_unknown_scan_rows),
    )
}

fn selected_kv_node_scan_root() -> SelectedExecutableRunRoot {
    SelectedExecutableRunRoot::alternative(
        node_access_expr(ir::NodeAccessPlan::AllScan),
        selected_kv_node_scan(),
    )
}

fn selected_run_root_plan(
    root: SelectedExecutableRunRoot,
    output: ir::BatchOutputPlan,
    profile: &cost::StorageCostProfile,
) -> ExecutablePlan {
    ExecutablePlan::from_selected_executable_batch(SelectedExecutableBatchPlanRequest {
        kind: ir::PlanKind::Read,
        returns: ir::ReturnPlan::None,
        trace: trace::PlanningTrace::default(),
        metrics: PlannerMetrics::default(),
        entries: SelectedExecutableBatchEntries::Single(SelectedInitialExecutableBatchEntry::Run(
            Box::new(SelectedExecutableRunEntry {
                root,
                output,
                condition: ir::RunConditionPlan::Always,
            }),
        )),
        profile,
    })
    .expect("selected test root must lower to an executable plan")
}

fn selected_terminal_plan(
    alternative: physical::PhysicalAlternative,
    plan: SelectedRootTerminal,
    output: ir::BatchOutputPlan,
    profile: &cost::StorageCostProfile,
) -> ExecutablePlan {
    selected_run_root_plan(
        SelectedExecutableRunRoot::Terminal(Box::new(selected_root_terminal_plan(
            alternative,
            plan,
        ))),
        output,
        profile,
    )
}

fn selected_root_pipeline(
    alternative: physical::PhysicalAlternative,
    input: SelectedRootStreamInput,
    ops: ir::AtLeast<logical::StreamPipelineOp, 1>,
) -> SelectedRootPipeline {
    SelectedRootPipeline::new(alternative.into(), selected_root_provenance(), input, ops)
        .expect("selected root pipeline test fixture must have a valid physical suffix")
}

fn selected_root_terminal_plan(
    alternative: physical::PhysicalAlternative,
    plan: SelectedRootTerminal,
) -> SelectedRootTerminalPlan {
    SelectedRootTerminalPlan::new(alternative.into(), selected_root_provenance(), plan)
        .expect("selected root terminal test fixture must have a valid physical suffix")
}

fn selected_root_mutation(
    alternative: physical::PhysicalAlternative,
    plan: SelectedMutationPlan,
) -> SelectedRootMutation {
    SelectedRootMutation::new(alternative.into(), selected_root_provenance(), plan)
        .expect("selected mutation test fixture must use a barrier physical plan")
}

fn selected_root_index_ddl(
    alternative: physical::PhysicalAlternative,
    plan: ir::IndexDdlPlan,
) -> SelectedRootIndexDdl {
    SelectedRootIndexDdl::new(alternative.into(), selected_root_provenance(), plan)
        .expect("selected index-DDL test fixture must use a barrier physical plan")
}

fn selected_root_branch(
    alternative: physical::PhysicalAlternative,
    input: SelectedExecutableRunRoot,
    plan: SelectedBranchPlan,
) -> SelectedRootBranch {
    SelectedRootBranch::new(
        alternative.into(),
        selected_root_provenance(),
        Box::new(input),
        plan,
    )
    .expect("selected branch test fixture must use a branch physical plan")
}

fn selected_root_repeat(
    alternative: physical::PhysicalAlternative,
    input: SelectedExecutableRunRoot,
    plan: SelectedRepeatPlan,
) -> SelectedRootRepeat {
    SelectedRootRepeat::new(
        alternative.into(),
        selected_root_provenance(),
        Box::new(input),
        plan,
    )
    .expect("selected repeat test fixture must use a repeat physical plan")
}

fn selected_access_stream_input(plan: ir::NodeAccessPlan) -> SelectedRootStreamInput {
    SelectedRootStreamInput::Access(logical::AccessStream::Path(node_access_path(plan)))
}

fn selected_barrier_delivered_properties() -> properties::DeliveredProperties {
    properties::DeliveredProperties {
        materialization: properties::Materialization::Materialized,
        effect: properties::EffectKind::Barrier,
        ..properties::DeliveredProperties::default()
    }
}

fn selected_branch_alternative(
    profile: &cost::StorageCostProfile,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch),
        selected_barrier_delivered_properties(),
        profile.barrier(),
    )
}

fn selected_repeat_alternative(
    profile: &cost::StorageCostProfile,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat),
        selected_barrier_delivered_properties(),
        profile.stream_operator(profile.default_unknown_scan_rows),
    )
}

fn selected_mutation_alternative(
    profile: &cost::StorageCostProfile,
) -> physical::PhysicalAlternative {
    physical::PhysicalAlternative::new(
        physical::PhysicalExpr::Barrier,
        selected_barrier_delivered_properties(),
        profile.barrier(),
    )
}

mod costs;
mod kv;
mod plan;
mod selected_lowering;
