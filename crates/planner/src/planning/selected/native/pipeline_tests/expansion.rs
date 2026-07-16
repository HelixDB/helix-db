use helix_ast::traversal::AstNode;

use super::support;
use crate::{ir, logical};

#[test]
fn native_pipeline_lowers_expansion_contracts() {
    [
        (
            AstNode::Out {
                input: support::node_source(),
                label: Some("LIKES".to_owned()),
            },
            ir::ExpandDirection::Out,
            ir::ExpandOutput::Nodes,
        ),
        (
            AstNode::In {
                input: support::node_source(),
                label: None,
            },
            ir::ExpandDirection::In,
            ir::ExpandOutput::Nodes,
        ),
        (
            AstNode::Both {
                input: support::node_source(),
                label: Some("KNOWS".to_owned()),
            },
            ir::ExpandDirection::Both,
            ir::ExpandOutput::Nodes,
        ),
        (
            AstNode::OutE {
                input: support::node_source(),
                label: None,
            },
            ir::ExpandDirection::Out,
            ir::ExpandOutput::Edges,
        ),
        (
            AstNode::InE {
                input: support::node_source(),
                label: Some("LIKES".to_owned()),
            },
            ir::ExpandDirection::In,
            ir::ExpandOutput::Edges,
        ),
        (
            AstNode::BothE {
                input: support::node_source(),
                label: Some("KNOWS".to_owned()),
            },
            ir::ExpandDirection::Both,
            ir::ExpandOutput::Edges,
        ),
        (
            AstNode::OutN {
                input: support::edge_source(),
            },
            ir::ExpandDirection::Out,
            ir::ExpandOutput::Nodes,
        ),
        (
            AstNode::InN {
                input: support::edge_source(),
            },
            ir::ExpandDirection::In,
            ir::ExpandOutput::Nodes,
        ),
        (
            AstNode::OtherN {
                input: Box::new(AstNode::OutE {
                    input: support::node_source(),
                    label: None,
                }),
            },
            ir::ExpandDirection::Both,
            ir::ExpandOutput::Nodes,
        ),
    ]
    .into_iter()
    .for_each(|(root, expected_direction, expected_output)| {
        let expr = support::lower(root)
            .unwrap()
            .expect_native("expansion is native");
        assert!(matches!(
            expr,
            logical::LogicalExpr::AccessPipeline(pipeline)
                if matches!(
                    pipeline.ops().last(),
                    Some(logical::StreamPipelineOp::Expand { plan })
                        if plan.direction == expected_direction && plan.output == expected_output
                )
        ));
    });
}
