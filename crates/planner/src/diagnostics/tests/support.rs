use crate::{catalog, context, cost, diagnostics, exec, ir, planning, properties, trace};
use helix_ast::batch::{read_batch, ReadBatch};
use helix_ast::traversal::{ReadOnly, Traversal, TraversalState};

pub(super) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

pub(super) fn plan<S: TraversalState>(
    traversal: Traversal<S, ReadOnly>,
    ctx: &context::PlannerContext,
) -> planning::PlanningOutput {
    let batch = read_batch()
        .var_as("result", traversal)
        .returning(["result"]);
    planning::plan_read_batch_with_diagnostics(&batch, ctx).unwrap()
}

pub(super) fn plan_batch(
    batch: &ReadBatch,
    ctx: &context::PlannerContext,
) -> planning::PlanningOutput {
    planning::plan_read_batch_with_diagnostics(batch, ctx).unwrap()
}

pub(super) fn missing_indexes(
    output: &planning::PlanningOutput,
) -> Vec<&diagnostics::MissingIndexInsight> {
    missing_index_insights(output.diagnostics())
}

pub(super) fn missing_index_insights(
    diagnostics: &diagnostics::PlannerDiagnostics,
) -> Vec<&diagnostics::MissingIndexInsight> {
    diagnostics
        .insights
        .iter()
        .filter_map(|insight| match insight {
            diagnostics::PlannerInsight::MissingIndex(insight) => Some(insight),
            diagnostics::PlannerInsight::UnboundedScan(_)
            | diagnostics::PlannerInsight::DeepTraversal(_) => None,
        })
        .collect()
}

pub(super) fn unbounded_scans(
    diagnostics: &diagnostics::PlannerDiagnostics,
) -> Vec<&diagnostics::UnboundedScanInsight> {
    diagnostics
        .insights
        .iter()
        .filter_map(|insight| match insight {
            diagnostics::PlannerInsight::UnboundedScan(insight) => Some(insight),
            diagnostics::PlannerInsight::MissingIndex(_)
            | diagnostics::PlannerInsight::DeepTraversal(_) => None,
        })
        .collect()
}

pub(super) fn deep_traversals(
    diagnostics: &diagnostics::PlannerDiagnostics,
) -> Vec<&diagnostics::DeepTraversalInsight> {
    diagnostics
        .insights
        .iter()
        .filter_map(|insight| match insight {
            diagnostics::PlannerInsight::DeepTraversal(insight) => Some(insight),
            diagnostics::PlannerInsight::MissingIndex(_)
            | diagnostics::PlannerInsight::UnboundedScan(_) => None,
        })
        .collect()
}

pub(super) fn step(
    id: usize,
    dependencies: Vec<exec::ExecStepId>,
    op: exec::ExecOp,
) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).unwrap(),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

pub(super) fn expand_op(label: &str) -> exec::ExecOp {
    exec::ExecOp::Expand {
        plan: ir::ExpandPlan {
            direction: ir::ExpandDirection::Out,
            output: ir::ExpandOutput::Nodes,
            label: ir::ExpandLabelPlan::Label(name(label)),
        },
    }
}

pub(super) fn linear_steps(ops: impl IntoIterator<Item = exec::ExecOp>) -> Vec<exec::ExecStep> {
    ops.into_iter()
        .enumerate()
        .map(|(index, op)| {
            let id = index + 1;
            let dependencies = (id > 1)
                .then(|| exec::ExecStepId::new(id - 1).unwrap())
                .into_iter()
                .collect();
            step(id, dependencies, op)
        })
        .collect()
}

pub(super) fn subplan(ops: impl IntoIterator<Item = exec::ExecOp>) -> exec::ExecutableSubplan {
    let steps = linear_steps(ops);
    let root = steps.last().unwrap().id;
    exec::ExecutableSubplan::new(ir::AtLeast::<_, 1>::try_from_vec(steps).unwrap(), root).unwrap()
}

pub(super) fn executable_plan(
    steps: Vec<exec::ExecStep>,
    metrics: exec::PlannerMetrics,
) -> exec::ExecutablePlan {
    let root = steps.last().unwrap().id;
    exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).unwrap(),
        root,
        trace::PlanningTrace::default(),
        metrics,
    )
    .unwrap()
}

pub(super) fn diagnostics_for_ops(
    ops: impl IntoIterator<Item = exec::ExecOp>,
) -> diagnostics::PlannerDiagnostics {
    diagnostics_for_ops_with(
        ops,
        exec::PlannerMetrics::default(),
        &context::PlannerContext::default(),
    )
}

pub(super) fn diagnostics_for_ops_with(
    ops: impl IntoIterator<Item = exec::ExecOp>,
    metrics: exec::PlannerMetrics,
    ctx: &context::PlannerContext,
) -> diagnostics::PlannerDiagnostics {
    let plan = executable_plan(linear_steps(ops), metrics);
    diagnostics::analyze(&plan, ctx)
}

pub(super) fn search_context() -> context::PlannerContext {
    use catalog::{ElementKind, SearchIndexKey, SearchIndexScope};

    context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default()
            .with_vector(
                SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
                SearchIndexScope::Unscoped,
            )
            .with_text(
                SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
                SearchIndexScope::Unscoped,
            )
            .with_vector(
                SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
                SearchIndexScope::Unscoped,
            )
            .with_text(
                SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
                SearchIndexScope::Unscoped,
            ),
        ..context::PlannerContext::default()
    }
}

pub(super) fn assert_same_plan_shape(
    ordinary: &exec::ExecutablePlan,
    diagnostic: &exec::ExecutablePlan,
) {
    assert_eq!(ordinary.kind(), diagnostic.kind());
    assert_eq!(ordinary.returns(), diagnostic.returns());
    assert_eq!(ordinary.steps(), diagnostic.steps());
    assert_eq!(ordinary.root(), diagnostic.root());
    assert_eq!(ordinary.trace(), diagnostic.trace());
}
