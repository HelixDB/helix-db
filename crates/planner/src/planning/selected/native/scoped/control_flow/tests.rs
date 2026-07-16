use super::*;
use crate::{context, error, ir, logical};
use helix_ast::graph::NodeRef;
use helix_ast::traversal;

fn nodes() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

#[test]
fn scoped_roots_keep_control_flow_inside_cascades() {
    let optional = control_flow_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Optional {
            input: nodes(),
            traversal: traversal::sub().out(Some("FOLLOWS")),
        },
        NativeAstScope::QueryRoot,
    )
    .unwrap();
    let ControlFlowRoot::Branch(optional) = optional else {
        panic!("optional is selectable");
    };

    assert!(matches!(
        &optional,
        branch
            if matches!(branch.input(), logical::LogicalExpr::AccessPath(_))
                && matches!(
                    branch.plan(),
                    ir::BranchPlan::Optional(body)
                        if matches!(
                            body.as_ref(),
                            logical::LogicalExpr::RootPipeline(pipeline)
                                if matches!(
                                    pipeline.input(),
                                    logical::RootStream::VariableSource(source)
                                        if source.variable().as_ref() == "$context"
                                )
                        )
                )
    ));

    let repeat = control_flow_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Repeat {
            input: nodes(),
            config: traversal::RepeatConfig::new(traversal::sub().out(Some("FOLLOWS"))).times(2),
        },
        NativeAstScope::QueryRoot,
    )
    .unwrap();
    let ControlFlowRoot::Repeat(repeat) = repeat else {
        panic!("repeat is selectable");
    };

    assert!(matches!(
        &repeat,
        repeat
            if matches!(repeat.input(), logical::LogicalExpr::AccessPath(_))
                && matches!(
                    repeat.plan().body.as_ref(),
                    logical::LogicalExpr::RootPipeline(pipeline)
                        if matches!(
                            pipeline.input(),
                            logical::RootStream::VariableSource(source)
                                if source.variable().as_ref() == "$context"
                        )
                )
                && matches!(repeat.plan().stop, ir::RepeatStopPlan::Times { .. })
    ));
}

#[test]
fn scoped_control_flow_reports_non_family_and_invalid_arity() {
    assert!(matches!(
        control_flow_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Context,
            NativeAstScope::QueryRoot,
        )
        .unwrap(),
        ControlFlowRoot::NotControlFlow
    ));

    assert!(matches!(
        control_flow_from_ast(
            &context::PlannerContext::default(),
            &AstNode::Union {
                input: nodes(),
                traversals: vec![traversal::sub().out(Some("FOLLOWS"))],
            },
            NativeAstScope::QueryRoot,
        ),
        Err(error::PlannerError::InvalidBranchArity {
            op: error::BranchOp::Union,
            min: 2,
            actual: 1,
        })
    ));
}
