use crate::{ir, logical};
use helix_ast::traversal::AstNode;

pub(super) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

pub(super) fn access_path() -> logical::AccessPath {
    logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    ))
}

pub(super) fn access_expr() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(access_path())
}

pub(super) fn node_ast() -> AstNode {
    AstNode::Nodes {
        reference: helix_ast::graph::NodeRef::All,
    }
}

pub(super) fn node_box() -> Box<AstNode> {
    Box::new(node_ast())
}

pub(super) fn variable_stream() -> logical::RootStream {
    logical::RootStream::VariableSource(logical::VariableSource::new(name("seed")))
}

pub(super) fn root_pipeline() -> logical::RootPipeline {
    logical::RootPipeline::new(
        variable_stream(),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        }),
    )
    .unwrap()
}
