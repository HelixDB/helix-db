use helix_ast::expr::StreamBound;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index::IndexSpec;
use helix_ast::traversal::{self, AstNode};
use helix_ast::value::{PropertyInput, PropertyValue};

use super::support;
use crate::{ir, logical};

#[test]
fn native_root_lowers_access_sources_and_rejects_unsupported_wrappers() {
    let node = support::lower(AstNode::Nodes {
        reference: NodeRef::Ids(vec![7, 9]),
    })
    .unwrap()
    .expect_native("node source is native");
    assert!(matches!(
        node,
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(_))
    ));

    let edge = support::lower(AstNode::Edges {
        reference: EdgeRef::All,
    })
    .unwrap()
    .expect_native("edge source is native");
    assert!(matches!(
        edge,
        logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(_))
    ));

    let search_ctx = support::search_ctx();
    let node_search = support::lower_with(
        &search_ctx,
        AstNode::VectorSearchNodes {
            label: "Doc".to_owned(),
            property: "embedding".to_owned(),
            tenant_value: None,
            query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1])),
            k: StreamBound::Literal(10),
        },
    )
    .unwrap()
    .expect_native("node vector search is native");
    assert!(matches!(
        node_search,
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(path))
            if matches!(path.source().as_ref(), ir::NodeAccessPlan::VectorSearch { .. })
    ));

    let edge_search = support::lower_with(
        &search_ctx,
        AstNode::TextSearchEdges {
            label: "MENTIONS".to_owned(),
            property: "body".to_owned(),
            tenant_value: Some(PropertyInput::from("tenant-a")),
            query_text: PropertyInput::from("needle"),
            k: StreamBound::Literal(3),
        },
    )
    .unwrap()
    .expect_native("edge text search is native");
    assert!(matches!(
        edge_search,
        logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(path))
            if matches!(path.source().as_ref(), ir::EdgeAccessPlan::TextSearch { .. })
    ));

    let source = support::lower(AstNode::Inject {
        input: None,
        variable: "seed".to_owned(),
    })
    .unwrap()
    .expect_native("source inject is native");
    assert!(matches!(
        source,
        logical::LogicalExpr::VariableSource(source)
            if source.variable().as_ref() == "seed"
    ));

    let ddl = support::lower(AstNode::CreateIndex {
        spec: IndexSpec::node_unique_equality("Person", "email"),
        if_not_exists: true,
    })
    .unwrap()
    .expect_native("index DDL is native");
    assert!(matches!(
        ddl,
        logical::LogicalExpr::RootIndexDdl(ddl)
            if matches!(ddl.plan(), ir::IndexDdlPlan::Create { .. })
    ));

    let mutation = support::lower(AstNode::AddN {
        input: None,
        label: "Person".to_owned(),
        properties: Vec::new(),
    })
    .unwrap()
    .expect_native("source mutation is native");
    assert!(matches!(
        mutation,
        logical::LogicalExpr::RootMutation(mutation)
            if matches!(mutation.plan(), ir::MutationPlan::AddNode { .. })
    ));

    let optional = support::lower(AstNode::Optional {
        input: support::node_source(),
        traversal: traversal::sub().out(Some("FOLLOWS")),
    })
    .unwrap()
    .expect_native("optional branch is native");
    assert!(matches!(
        optional,
        logical::LogicalExpr::RootBranch(branch)
            if matches!(branch.plan(), ir::BranchPlan::Optional(_))
    ));

    let repeat = support::lower(AstNode::Repeat {
        input: support::node_source(),
        config: traversal::RepeatConfig::new(traversal::sub().out(Some("FOLLOWS"))).times(2),
    })
    .unwrap()
    .expect_native("repeat is native");
    assert!(matches!(
        repeat,
        logical::LogicalExpr::RootRepeat(repeat)
            if matches!(repeat.plan().stop, ir::RepeatStopPlan::Times { .. })
    ));

    let terminal = support::lower(AstNode::Count {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        }),
    })
    .unwrap()
    .expect_native("count terminal is native");
    assert!(matches!(
        terminal,
        logical::LogicalExpr::StreamProject(project)
            if matches!(project.projection(), ir::ProjectionPlan::Count)
    ));

    let reserved = support::lower(AstNode::Fold {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        }),
    })
    .unwrap()
    .expect_native("fold terminal is native");
    assert!(matches!(
        reserved,
        logical::LogicalExpr::StreamReserved(reserved)
            if matches!(reserved.op(), ir::ReservedOp::Fold)
    ));

    let expansion = support::lower(AstNode::Out {
        input: Box::new(AstNode::Nodes {
            reference: NodeRef::All,
        }),
        label: Some("LIKES".to_owned()),
    })
    .unwrap()
    .expect_native("expansion pipeline is native");
    assert!(matches!(
        expansion,
        logical::LogicalExpr::AccessPipeline(pipeline)
            if matches!(
                pipeline.ops(),
                [logical::StreamPipelineOp::Expand { plan }]
                    if matches!(plan.direction, ir::ExpandDirection::Out)
                        && matches!(plan.output, ir::ExpandOutput::Nodes)
            )
    ));

    assert!(matches!(
        super::super::native_selectable_root_from_ast(&support::ctx(), &AstNode::Context).unwrap(),
        super::super::NativeSelectableRoot::NotSelectable
    ));
}
