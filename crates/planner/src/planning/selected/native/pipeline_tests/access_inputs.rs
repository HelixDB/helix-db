use helix_ast::expr::StreamBound;
use helix_ast::graph::NodeRef;
use helix_ast::traversal::{AstNode, Order};

use super::support;
use crate::logical;

#[test]
fn native_pipeline_flattens_access_inputs() {
    let filtered_expand = support::lower(AstNode::Out {
        input: Box::new(AstNode::Has {
            input: support::node_source(),
            property: "active".to_owned(),
            value: true.into(),
        }),
        label: None,
    })
    .unwrap()
    .expect_native("filtered expansion is native");
    assert!(matches!(
        filtered_expand,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    logical::StreamPipelineOp::Filter { .. },
                    logical::StreamPipelineOp::Expand { .. }
                ]
            )
    ));

    let distinct_expand = support::lower(AstNode::In {
        input: Box::new(AstNode::Dedup {
            input: support::node_source(),
        }),
        label: None,
    })
    .unwrap()
    .expect_native("distinct expansion is native");
    assert!(matches!(
        distinct_expand,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    logical::StreamPipelineOp::Distinct,
                    logical::StreamPipelineOp::Expand { .. }
                ]
            )
    ));

    let ordered_expand = support::lower(AstNode::Both {
        input: Box::new(AstNode::OrderBy {
            input: support::node_source(),
            property: "age".to_owned(),
            order: Order::Asc,
        }),
        label: None,
    })
    .unwrap()
    .expect_native("ordered expansion is native");
    assert!(matches!(
        ordered_expand,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    logical::StreamPipelineOp::Order { .. },
                    logical::StreamPipelineOp::Expand { .. }
                ]
            )
    ));

    let windowed_expand = support::lower(AstNode::Out {
        input: Box::new(AstNode::Limit {
            input: support::node_source(),
            count: StreamBound::Literal(3),
        }),
        label: None,
    })
    .unwrap()
    .expect_native("windowed expansion is native");
    assert!(matches!(
        windowed_expand,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    logical::StreamPipelineOp::Window { .. },
                    logical::StreamPipelineOp::Expand { .. }
                ]
            )
    ));

    let stored_expand = support::lower(AstNode::Out {
        input: Box::new(AstNode::Store {
            input: support::node_source(),
            name: "seen".to_owned(),
        }),
        label: None,
    })
    .unwrap()
    .expect_native("access-pipeline-rooted expansion is native");
    assert!(matches!(
        stored_expand,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [
                    logical::StreamPipelineOp::VariableWrite { .. },
                    logical::StreamPipelineOp::Expand { .. }
                ]
            )
    ));

    let all_scan_expand = support::lower(AstNode::Out {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        }),
        label: None,
    })
    .unwrap()
    .expect_native("source-rooted expansion is native");
    assert!(matches!(
        all_scan_expand,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(pipeline.ops(), [logical::StreamPipelineOp::Expand { .. }])
    ));
}
