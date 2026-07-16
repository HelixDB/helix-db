use super::super::super::rejection;
use super::exact::ExactMemoChildPlanContext;
use super::*;
use crate::{context, error, ir, logical, memo, optimizer, rules};

fn optimization_result() -> optimizer::OptimizationResult {
    let ctx = context::PlannerContext::default();
    let config = optimizer::OptimizerConfig::from_context(&ctx);
    let rules = rules::SeedRuleSet::default();
    rules
        .optimizer()
        .optimize(node_access_expr(), &config)
        .expect("test optimizer memo allocation should fit")
}

fn node_access_expr() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    )))
}

fn context<'result, 'selection>(
    selection: &'selection mut optimizer::SelectionSession<'result>,
    children: Vec<memo::MemoGroupId>,
) -> MemoChildPlanContext<'result, 'selection> {
    MemoChildPlanContext::for_test(selection, children)
}

fn exactly<'result, 'selection>(
    context: MemoChildPlanContext<'result, 'selection>,
    expected: usize,
) -> Result<ExactMemoChildPlanContext<'result, 'selection>, error::PlannerError> {
    context.exactly(expected, rejection::Reason::MemoChildArityMismatch)
}

#[test]
fn exact_child_context_encodes_parent_arity() {
    let result = optimization_result();
    let mut selection = result.selection_session();
    let child = memo::MemoGroupId::new(1).unwrap();

    assert!(exactly(context(&mut selection, vec![child]), 1).is_ok());
    assert!(exactly(context(&mut selection, Vec::new()), 0).is_ok());
    assert_eq!(
        exactly(context(&mut selection, Vec::new()), 1)
            .err()
            .unwrap(),
        rejection::unsupported(rejection::Reason::MemoChildArityMismatch)
    );
    assert_eq!(
        exactly(context(&mut selection, vec![child, child]), 1)
            .err()
            .unwrap(),
        rejection::unsupported(rejection::Reason::MemoChildArityMismatch)
    );
}

#[test]
fn exact_child_context_preserves_ordered_selection() {
    let result = optimization_result();
    let mut selection = result.selection_session();
    let first = memo::MemoGroupId::new(1).unwrap();
    let second = memo::MemoGroupId::new(1).unwrap();
    let mut exact = exactly(context(&mut selection, vec![first, second]), 2)
        .expect("expected arity matches children");

    assert_eq!(exact.selected(0).unwrap().selected.group, first);
    assert_eq!(exact.selected(1).unwrap().selected.group, second);
    assert_eq!(
        exact.selected(2).unwrap_err(),
        rejection::unsupported(rejection::Reason::MemoChildPlanMissing)
    );
}

#[test]
fn exact_single_child_context_is_only_constructible_for_one_child() {
    let result = optimization_result();
    let mut selection = result.selection_session();
    let child = memo::MemoGroupId::new(1).unwrap();

    assert!(exactly(context(&mut selection, vec![child]), 1)
        .unwrap()
        .single()
        .is_ok());
    assert_eq!(
        exactly(context(&mut selection, Vec::new()), 0)
            .unwrap()
            .single()
            .unwrap_err(),
        rejection::unsupported(rejection::Reason::MemoChildPlanMissing)
    );
}
