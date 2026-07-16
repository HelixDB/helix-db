use super::*;
use crate::ir;
use crate::{context, error, logical};
use helix_ast::traversal::AstNode;
use std::num::NonZeroUsize;

fn source_add_node() -> AstNode {
    AstNode::AddN {
        input: None,
        label: "User".to_owned(),
        properties: Vec::new(),
    }
}

fn node_root() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    )))
}

fn source_mutation() -> logical::RootMutation {
    logical::RootMutation::new(ir::MutationPlan::AddNode {
        input: ir::MutationInput::Source,
        label: ir::NonEmptyString::new("User").unwrap(),
        properties: ir::PropertyAssignments::default(),
    })
}

#[test]
fn root_stream_admits_source_mutation_roots() {
    let stream = root_stream_from_ast(&context::PlannerContext::default(), &source_add_node())
        .unwrap()
        .expect_stream("source mutation is a root stream");

    assert!(matches!(
        stream,
        logical::RootStream::Mutation(mutation)
            if matches!(
                mutation.plan(),
                ir::MutationPlan::AddNode {
                    input: ir::MutationInput::Source,
                    ..
                }
        )
    ));
}

#[test]
fn root_stream_admits_control_flow_roots() {
    let optional = root_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Optional {
            input: Box::new(AstNode::Nodes {
                reference: helix_ast::graph::NodeRef::All,
            }),
            traversal: helix_ast::traversal::sub().out(Some("FOLLOWS")),
        },
    )
    .unwrap()
    .expect_stream("optional branch is a root stream");
    assert!(matches!(optional, logical::RootStream::Branch(_)));

    let repeat = root_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Repeat {
            input: Box::new(AstNode::Nodes {
                reference: helix_ast::graph::NodeRef::All,
            }),
            config: helix_ast::traversal::RepeatConfig::new(
                helix_ast::traversal::sub().out(Some("FOLLOWS")),
            )
            .times(2),
        },
    )
    .unwrap()
    .expect_stream("repeat is a root stream");
    assert!(matches!(repeat, logical::RootStream::Repeat(_)));
}

#[test]
fn root_stream_expr_admits_root_barrier_contracts() {
    let mutation =
        root_stream_from_expr(logical::LogicalExpr::RootMutation(source_mutation())).unwrap();
    assert!(matches!(mutation, logical::RootStream::Mutation(_)));

    let branch = root_stream_from_expr(logical::LogicalExpr::RootBranch(logical::RootBranch::new(
        node_root(),
        ir::BranchPlan::Optional(Box::new(node_root())),
    )))
    .unwrap();
    assert!(matches!(branch, logical::RootStream::Branch(_)));

    let repeat = root_stream_from_expr(logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        node_root(),
        ir::RepeatPlan {
            body: Box::new(node_root()),
            stop: ir::RepeatStopPlan::MaxDepthOnly,
            emit: ir::RepeatEmitPlan::None,
            max_depth: NonZeroUsize::new(2).unwrap(),
        },
    )))
    .unwrap();
    assert!(matches!(repeat, logical::RootStream::Repeat(_)));
}

#[test]
fn root_stream_expr_rejects_non_stream_logical_roots() {
    assert!(matches!(
        root_stream_from_expr(logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp)),
        Err(error::PlannerError::UnsupportedCascadesPlan { .. })
    ));
}

#[test]
fn root_stream_keeps_source_mutation_pipelines_in_cascades_contracts() {
    let stream = root_stream_from_ast(
        &context::PlannerContext::default(),
        &AstNode::Store {
            input: Box::new(source_add_node()),
            name: "created".to_owned(),
        },
    )
    .unwrap()
    .expect_stream("source mutation pipeline is a root stream");

    assert!(matches!(
        stream,
        logical::RootStream::Pipeline(pipeline)
            if matches!(pipeline.input(), logical::RootStream::Mutation(_))
                && matches!(
                    pipeline.ops(),
                    [logical::StreamPipelineOp::VariableWrite { op }]
                        if matches!(
                            op,
                            logical::StreamVariableWriteOp::Store(name)
                                if name.as_ref() == "created"
                        )
                )
    ));
}

#[test]
fn root_stream_reports_non_stream_roots_explicitly() {
    assert!(matches!(
        root_stream_from_ast(&context::PlannerContext::default(), &AstNode::Context).unwrap(),
        NativeRootStream::NotRootStream
    ));
}
