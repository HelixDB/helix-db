use helix_ast::expr::StreamBound;
use helix_ast::traversal::AstNode;

use super::support;
use crate::logical;

#[test]
fn native_pipeline_lowers_stream_wrappers_above_variable_sources() {
    let expr = support::lower(AstNode::Limit {
        input: Box::new(AstNode::Inject {
            input: None,
            variable: "seed".to_owned(),
        }),
        count: StreamBound::Literal(10),
    })
    .unwrap()
    .expect_native("variable-source-rooted limit is native");
    assert!(matches!(
        expr,
        logical::LogicalExpr::RootPipeline(pipeline)
            if matches!(
                pipeline.input(),
                logical::RootStream::VariableSource(source)
                    if source.variable().as_ref() == "seed"
            )
                && matches!(pipeline.ops(), [logical::StreamPipelineOp::Limit { .. }])
    ));
}
