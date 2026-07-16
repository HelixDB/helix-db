//! Top-level AST-family classification for native selected planning.
//!
//! This is intentionally a cheap, exhaustive `AstNode` dispatch. Native root,
//! root-stream, and scoped entry modules consume this shared ADT so a new AST
//! variant cannot drift across multiple broad match ladders.

use helix_ast::traversal::AstNode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::planning::selected::native) enum NativeAstFamily {
    Terminal,
    VariableSource,
    IndexDdl,
    ShortestPath,
    SourceMutation,
    ControlFlow,
    AccessOrPipeline,
    Context,
}

impl NativeAstFamily {
    pub(in crate::planning::selected::native) fn from_ast(root: &AstNode) -> Self {
        match root {
            AstNode::Count { .. }
            | AstNode::Exists { .. }
            | AstNode::Id { .. }
            | AstNode::Label { .. }
            | AstNode::Values { .. }
            | AstNode::ValueMap { .. }
            | AstNode::Project { .. }
            | AstNode::ProjectBindings { .. }
            | AstNode::EdgeProperties { .. }
            | AstNode::Group { .. }
            | AstNode::GroupCount { .. }
            | AstNode::AggregateBy { .. }
            | AstNode::Fold { .. }
            | AstNode::Unfold { .. }
            | AstNode::Path { .. }
            | AstNode::SimplePath { .. }
            | AstNode::WithSack { .. }
            | AstNode::SackSet { .. }
            | AstNode::SackAdd { .. }
            | AstNode::SackGet { .. } => Self::Terminal,
            AstNode::Inject { input: None, .. } => Self::VariableSource,
            AstNode::CreateIndex { .. }
            | AstNode::DropIndex { .. }
            | AstNode::GetIndexOperation { .. }
            | AstNode::RetryIndexOperation { .. }
            | AstNode::AbortIndexOperation { .. } => Self::IndexDdl,
            AstNode::ShortestPath { .. } => Self::ShortestPath,
            AstNode::AddN { .. }
            | AstNode::AddE { .. }
            | AstNode::SetProperty { .. }
            | AstNode::RemoveProperty { .. }
            | AstNode::Drop { .. }
            | AstNode::DropEdge { .. }
            | AstNode::DropEdgeLabeled { .. }
            | AstNode::DropEdgeById { .. } => Self::SourceMutation,
            AstNode::Repeat { .. }
            | AstNode::Union { .. }
            | AstNode::Choose { .. }
            | AstNode::Coalesce { .. }
            | AstNode::Optional { .. } => Self::ControlFlow,
            AstNode::Nodes { .. }
            | AstNode::Edges { .. }
            | AstNode::NodesWhere { .. }
            | AstNode::EdgesWhere { .. }
            | AstNode::VectorSearchNodes { .. }
            | AstNode::TextSearchNodes { .. }
            | AstNode::VectorSearchEdges { .. }
            | AstNode::TextSearchEdges { .. }
            | AstNode::Has { .. }
            | AstNode::EdgeHas { .. }
            | AstNode::HasLabel { .. }
            | AstNode::EdgeHasLabel { .. }
            | AstNode::HasKey { .. }
            | AstNode::Where { .. }
            | AstNode::Dedup { .. }
            | AstNode::Limit { .. }
            | AstNode::Skip { .. }
            | AstNode::Range { .. }
            | AstNode::OrderBy { .. }
            | AstNode::OrderByMultiple { .. }
            | AstNode::Within { .. }
            | AstNode::Without { .. }
            | AstNode::Select { .. }
            | AstNode::Bind { .. }
            | AstNode::Inject { input: Some(_), .. }
            | AstNode::As { .. }
            | AstNode::Store { .. }
            | AstNode::Out { .. }
            | AstNode::In { .. }
            | AstNode::Both { .. }
            | AstNode::OutE { .. }
            | AstNode::InE { .. }
            | AstNode::BothE { .. }
            | AstNode::OutN { .. }
            | AstNode::InN { .. }
            | AstNode::OtherN { .. } => Self::AccessOrPipeline,
            AstNode::Context => Self::Context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::graph::NodeRef;
    use helix_ast::index::IndexSpec;
    use helix_ast::traversal::RepeatConfig;

    #[test]
    fn classifies_roots_by_top_level_ast_variant() {
        let cases = [
            (
                AstNode::Count {
                    input: Box::new(AstNode::Context),
                },
                NativeAstFamily::Terminal,
            ),
            (
                AstNode::Inject {
                    input: None,
                    variable: "seed".to_owned(),
                },
                NativeAstFamily::VariableSource,
            ),
            (
                AstNode::CreateIndex {
                    spec: IndexSpec::node_unique_equality("User", "email"),
                    if_not_exists: false,
                },
                NativeAstFamily::IndexDdl,
            ),
            (
                AstNode::ShortestPath {
                    source: NodeRef::id(1),
                    target: NodeRef::id(2),
                    label: None,
                    direction: helix_ast::traversal::ShortestPathDirection::Out,
                    max_depth: 2,
                },
                NativeAstFamily::ShortestPath,
            ),
            (
                AstNode::AddN {
                    input: None,
                    label: "User".to_owned(),
                    properties: Vec::new(),
                },
                NativeAstFamily::SourceMutation,
            ),
            (
                AstNode::Repeat {
                    input: Box::new(AstNode::Context),
                    config: RepeatConfig::new(Default::default()),
                },
                NativeAstFamily::ControlFlow,
            ),
            (
                AstNode::Nodes {
                    reference: NodeRef::All,
                },
                NativeAstFamily::AccessOrPipeline,
            ),
            (AstNode::Context, NativeAstFamily::Context),
        ];

        for (root, expected) in cases {
            assert_eq!(NativeAstFamily::from_ast(&root), expected);
        }
    }
}
