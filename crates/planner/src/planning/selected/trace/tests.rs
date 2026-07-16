use crate::{cost, exec, ir, logical, memo, physical, properties, rules, trace};

use super::append_selected_trace;

fn alternative_root() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::alternative(
        logical::LogicalExpr::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::from_static("seed"),
        )),
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        ),
    )
}

fn optimizer_alternative_root() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::try_alternative_with_provenance(
        logical::LogicalExpr::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::from_static("seed"),
        )),
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Stream(physical::PhysicalStreamOp::Variable),
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        ),
        exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
            rules::RuleId::new("variable_source_impl").unwrap(),
            memo::MemoGroupId::new(1).unwrap(),
            memo::MemoExprId::new(1).unwrap(),
            memo::PhysicalAlternativeId::new(1).unwrap(),
            memo::MemoChildGroups::empty(),
        )),
    )
    .unwrap()
}

fn optimizer_root_with_children() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::Pipeline(Box::new(
        exec::SelectedRootPipeline::new(
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::Pipeline(physical::PhysicalPipeline::new(ir::AtLeast::<
                    _,
                    1,
                >::from_one(
                    physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
                ))),
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            )
            .into(),
            exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
                rules::RuleId::known(rules::KnownRuleId::SeedRootPipeline),
                memo::MemoGroupId::new(1).unwrap(),
                memo::MemoExprId::new(2).unwrap(),
                memo::PhysicalAlternativeId::new(3).unwrap(),
                memo::MemoChildGroups::new(vec![
                    memo::MemoGroupId::new(4).unwrap(),
                    memo::MemoGroupId::new(5).unwrap(),
                ]),
            )),
            exec::SelectedRootStreamInput::VariableSource(logical::VariableSource::new(
                ir::NonEmptyString::from_static("seed"),
            )),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap(),
    ))
}

fn initial_run() -> exec::SelectedInitialExecutableBatchEntry {
    exec::SelectedInitialExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: alternative_root(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

fn followup_run() -> exec::SelectedFollowupExecutableBatchEntry {
    exec::SelectedFollowupExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: alternative_root(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

fn optimizer_initial_run() -> exec::SelectedInitialExecutableBatchEntry {
    exec::SelectedInitialExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: optimizer_alternative_root(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

fn optimizer_initial_run_with_children() -> exec::SelectedInitialExecutableBatchEntry {
    exec::SelectedInitialExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: optimizer_root_with_children(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

fn optimizer_mutation_with_input() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::Mutation(Box::new(
        exec::SelectedRootMutation::new(
            exec::SelectedPhysicalPlan::new(
                physical::PhysicalExpr::Barrier,
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
            exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
                rules::RuleId::known(rules::KnownRuleId::SeedRootMutation),
                memo::MemoGroupId::new(2).unwrap(),
                memo::MemoExprId::new(3).unwrap(),
                memo::PhysicalAlternativeId::new(4).unwrap(),
                memo::MemoChildGroups::new(vec![memo::MemoGroupId::new(1).unwrap()]),
            )),
            exec::SelectedMutationPlan::SetProperty {
                input: Box::new(optimizer_alternative_root()),
                name: ir::NonEmptyString::from_static("active"),
                value: ir::PropertyInputPlan::Value(helix_ast::value::PropertyValue::from(true)),
            },
        )
        .unwrap(),
    ))
}

fn optimizer_mutation_input_run() -> exec::SelectedInitialExecutableBatchEntry {
    exec::SelectedInitialExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: optimizer_mutation_with_input(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

fn optimizer_branch_with_children() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::Branch(Box::new(
        exec::SelectedRootBranch::new(
            exec::SelectedPhysicalPlan::new(
                physical::PhysicalExpr::Control(physical::PhysicalControlOp::Branch),
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
            exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
                rules::RuleId::known(rules::KnownRuleId::SeedRootBranch),
                memo::MemoGroupId::new(2).unwrap(),
                memo::MemoExprId::new(3).unwrap(),
                memo::PhysicalAlternativeId::new(4).unwrap(),
                memo::MemoChildGroups::new(vec![
                    memo::MemoGroupId::new(1).unwrap(),
                    memo::MemoGroupId::new(5).unwrap(),
                ]),
            )),
            Box::new(optimizer_alternative_root()),
            exec::SelectedBranchPlan::Optional(Box::new(optimizer_alternative_root())),
        )
        .unwrap(),
    ))
}

fn optimizer_branch_run() -> exec::SelectedInitialExecutableBatchEntry {
    exec::SelectedInitialExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: optimizer_branch_with_children(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

fn optimizer_repeat_with_children() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::Repeat(Box::new(
        exec::SelectedRootRepeat::new(
            exec::SelectedPhysicalPlan::new(
                physical::PhysicalExpr::Control(physical::PhysicalControlOp::Repeat),
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
            exec::SelectedRootProvenance::from_optimizer(exec::SelectedOptimizerProvenance::new(
                rules::RuleId::known(rules::KnownRuleId::SeedRootRepeat),
                memo::MemoGroupId::new(2).unwrap(),
                memo::MemoExprId::new(3).unwrap(),
                memo::PhysicalAlternativeId::new(4).unwrap(),
                memo::MemoChildGroups::new(vec![
                    memo::MemoGroupId::new(1).unwrap(),
                    memo::MemoGroupId::new(5).unwrap(),
                ]),
            )),
            Box::new(optimizer_alternative_root()),
            exec::SelectedRepeatPlan {
                body: Box::new(optimizer_alternative_root()),
                stop: ir::RepeatStopPlan::MaxDepthOnly,
                emit: ir::RepeatEmitPlan::None,
                max_depth: std::num::NonZeroUsize::new(100).unwrap(),
            },
        )
        .unwrap(),
    ))
}

fn optimizer_repeat_run() -> exec::SelectedInitialExecutableBatchEntry {
    exec::SelectedInitialExecutableBatchEntry::Run(Box::new(exec::SelectedExecutableRunEntry {
        root: optimizer_repeat_with_children(),
        output: ir::BatchOutputPlan::Discard,
        condition: ir::RunConditionPlan::Always,
    }))
}

#[test]
fn selected_trace_records_single_run_root_family() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(initial_run()),
    );

    assert_eq!(trace.events.len(), 3);
    assert_eq!(trace.events[0].pass, trace::TracePass::SelectedHandoff);
    assert_eq!(trace.events[0].path.as_ref(), "selected.entry[0].root");
    assert_eq!(
        trace.events[0].decision,
        trace::TraceDecision::SelectedRunRoot
    );
    assert_eq!(
        trace.events[0].reason,
        trace::TraceReason::SelectedRootFamily(ir::NonEmptyString::from_static("alternative"))
    );
    assert_eq!(trace.events[1].path.as_ref(), "selected.entry[0].root.rule");
    assert_eq!(trace.events[2].path.as_ref(), "selected.entry[0].root.memo");
}

#[test]
fn selected_trace_records_optimizer_rule_provenance() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(optimizer_initial_run()),
    );

    assert_eq!(trace.events.len(), 3);
    assert_eq!(trace.events[0].path.as_ref(), "selected.entry[0].root");
    assert_eq!(trace.events[1].path.as_ref(), "selected.entry[0].root.rule");
    assert_eq!(
        trace.events[1].decision,
        trace::TraceDecision::SelectedOptimizerRule
    );
    assert_eq!(
        trace.events[1].reason,
        trace::TraceReason::SelectedOptimizerRule(ir::NonEmptyString::from_static(
            "variable_source_impl"
        ))
    );
    assert_eq!(trace.events[2].path.as_ref(), "selected.entry[0].root.memo");
    assert_eq!(
        trace.events[2].decision,
        trace::TraceDecision::SelectedMemoExpression
    );
    assert_eq!(
        trace.events[2].reason,
        trace::TraceReason::SelectedMemoExpression(ir::NonEmptyString::from_static(
            "group=1 expr=1 alternative=1 children=[]"
        ))
    );
}

#[test]
fn selected_trace_records_memo_child_lineage() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(optimizer_initial_run_with_children()),
    );

    assert_eq!(trace.events.len(), 5);
    assert_eq!(trace.events[2].path.as_ref(), "selected.entry[0].root.memo");
    assert_eq!(
        trace.events[2].reason,
        trace::TraceReason::SelectedMemoExpression(ir::NonEmptyString::from_static(
            "group=1 expr=2 alternative=3 children=[4,5]"
        ))
    );
    assert_eq!(
        trace.events[3].path.as_ref(),
        "selected.entry[0].root.memo.child[0]"
    );
    assert_eq!(
        trace.events[3].decision,
        trace::TraceDecision::SelectedMemoChild
    );
    assert_eq!(
        trace.events[3].reason,
        trace::TraceReason::SelectedMemoChild(ir::NonEmptyString::from_static("index=0 group=4"))
    );
    assert_eq!(
        trace.events[4].path.as_ref(),
        "selected.entry[0].root.memo.child[1]"
    );
    assert_eq!(
        trace.events[4].reason,
        trace::TraceReason::SelectedMemoChild(ir::NonEmptyString::from_static("index=1 group=5"))
    );
}

#[test]
fn selected_trace_records_input_mutation_child_roots() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(optimizer_mutation_input_run()),
    );

    assert_eq!(trace.events.len(), 7);
    assert_eq!(trace.events[0].path.as_ref(), "selected.entry[0].root");
    assert_eq!(trace.events[1].path.as_ref(), "selected.entry[0].root.rule");
    assert_eq!(trace.events[2].path.as_ref(), "selected.entry[0].root.memo");
    assert_eq!(
        trace.events[3].path.as_ref(),
        "selected.entry[0].root.memo.child[0]"
    );
    assert_eq!(
        trace.events[4].path.as_ref(),
        "selected.entry[0].root.input[0].root"
    );
    assert_eq!(
        trace.events[5].path.as_ref(),
        "selected.entry[0].root.input[0].root.rule"
    );
    assert_eq!(
        trace.events[5].reason,
        trace::TraceReason::SelectedOptimizerRule(ir::NonEmptyString::from_static(
            "variable_source_impl"
        ))
    );
    assert_eq!(
        trace.events[6].path.as_ref(),
        "selected.entry[0].root.input[0].root.memo"
    );
}

#[test]
fn selected_trace_records_branch_input_and_body_roots() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(optimizer_branch_run()),
    );

    assert_trace_rule(&trace, "selected.entry[0].root.rule", "seed_root_branch");
    assert_trace_rule(
        &trace,
        "selected.entry[0].root.input[0].root.rule",
        "variable_source_impl",
    );
    assert_trace_rule(
        &trace,
        "selected.entry[0].root.optional.root.rule",
        "variable_source_impl",
    );
}

#[test]
fn selected_trace_records_repeat_input_and_body_roots() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(optimizer_repeat_run()),
    );

    assert_trace_rule(&trace, "selected.entry[0].root.rule", "seed_root_repeat");
    assert_trace_rule(
        &trace,
        "selected.entry[0].root.input[0].root.rule",
        "variable_source_impl",
    );
    assert_trace_rule(
        &trace,
        "selected.entry[0].root.body.root.rule",
        "variable_source_impl",
    );
}

#[test]
fn selected_trace_records_nested_foreach_bodies() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::ForEach(exec::SelectedForEachBatch::new(
                ir::NonEmptyString::from_static("item"),
                exec::SelectedExecutableBatchEntries::Single(initial_run()),
            )),
        ),
    );

    assert_eq!(trace.events.len(), 4);
    assert_eq!(trace.events[0].path.as_ref(), "selected.entry[0]");
    assert_eq!(
        trace.events[0].decision,
        trace::TraceDecision::SelectedForEach
    );
    assert_eq!(
        trace.events[0].reason,
        trace::TraceReason::SelectedForEachBody
    );
    assert_eq!(
        trace.events[1].path.as_ref(),
        "selected.entry[0].body[0].root"
    );
    assert_eq!(
        trace.events[2].path.as_ref(),
        "selected.entry[0].body[0].root.rule"
    );
    assert_eq!(
        trace.events[3].path.as_ref(),
        "selected.entry[0].body[0].root.memo"
    );
}

fn assert_trace_rule(trace: &trace::PlanningTrace, path: &str, expected: &str) {
    assert!(
        trace.events.iter().any(|event| {
            event.path.as_ref() == path
                && matches!(
                    &event.reason,
                    trace::TraceReason::SelectedOptimizerRule(actual) if actual.as_ref() == expected
                )
        }),
        "missing selected rule {expected:?} at {path:?} in trace: {:?}",
        trace.events
    );
}

#[test]
fn selected_trace_records_followup_runs_and_foreach_bodies() {
    let mut trace = trace::PlanningTrace::default();
    append_selected_trace(
        &mut trace,
        &exec::SelectedExecutableBatchEntries::WithFollowups {
            first: initial_run(),
            rest: ir::AtLeast::<_, 1>::try_from_vec(vec![
                followup_run(),
                exec::SelectedFollowupExecutableBatchEntry::ForEach(
                    exec::SelectedForEachBatch::new(
                        ir::NonEmptyString::from_static("item"),
                        exec::SelectedExecutableBatchEntries::Single(initial_run()),
                    ),
                ),
            ])
            .unwrap(),
        },
    );

    assert_eq!(trace.events.len(), 10);
    assert_eq!(trace.events[0].path.as_ref(), "selected.entry[0].root");
    assert_eq!(trace.events[1].path.as_ref(), "selected.entry[0].root.rule");
    assert_eq!(trace.events[2].path.as_ref(), "selected.entry[0].root.memo");
    assert_eq!(trace.events[3].path.as_ref(), "selected.entry[1].root");
    assert_eq!(trace.events[4].path.as_ref(), "selected.entry[1].root.rule");
    assert_eq!(trace.events[5].path.as_ref(), "selected.entry[1].root.memo");
    assert_eq!(trace.events[6].path.as_ref(), "selected.entry[2]");
    assert_eq!(
        trace.events[7].path.as_ref(),
        "selected.entry[2].body[0].root"
    );
    assert_eq!(
        trace.events[8].path.as_ref(),
        "selected.entry[2].body[0].root.rule"
    );
    assert_eq!(
        trace.events[9].path.as_ref(),
        "selected.entry[2].body[0].root.memo"
    );
}
