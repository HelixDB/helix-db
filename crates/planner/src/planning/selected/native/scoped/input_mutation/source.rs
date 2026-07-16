//! Source-only mutation recognition.

use helix_ast::traversal::AstNode;

pub(super) fn is_source_only_mutation(root: &AstNode) -> bool {
    matches!(
        root,
        AstNode::AddN { input: None, .. } | AstNode::DropEdgeById { input: None, .. }
    )
}
