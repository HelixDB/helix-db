//! Native source AST-shape contracts.

use helix_ast::expr::{Predicate, StreamBound};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::traversal::AstNode;
use helix_ast::value::PropertyInput;

/// AST source shape that can start a native access stream.
#[derive(Debug, Clone, Copy)]
pub(in crate::planning::selected::native) enum NativeSourceAst<'a> {
    /// Node source by reference.
    Nodes(&'a NodeRef),
    /// Edge source by reference.
    Edges(&'a EdgeRef),
    /// Node source constrained by a predicate.
    NodesWhere(&'a Predicate),
    /// Edge source constrained by a predicate.
    EdgesWhere(&'a Predicate),
    /// Node vector-search source.
    NodeVectorSearch {
        label: &'a str,
        property: &'a str,
        tenant_value: Option<&'a PropertyInput>,
        query_vector: &'a PropertyInput,
        k: &'a StreamBound,
    },
    /// Node text-search source.
    NodeTextSearch {
        label: &'a str,
        property: &'a str,
        tenant_value: Option<&'a PropertyInput>,
        query_text: &'a PropertyInput,
        k: &'a StreamBound,
    },
    /// Edge vector-search source.
    EdgeVectorSearch {
        label: &'a str,
        property: &'a str,
        tenant_value: Option<&'a PropertyInput>,
        query_vector: &'a PropertyInput,
        k: &'a StreamBound,
    },
    /// Edge text-search source.
    EdgeTextSearch {
        label: &'a str,
        property: &'a str,
        tenant_value: Option<&'a PropertyInput>,
        query_text: &'a PropertyInput,
        k: &'a StreamBound,
    },
}

/// Native source AST recognition result.
#[derive(Debug, Clone, Copy)]
pub(in crate::planning::selected::native) enum NativeSourceAstMatch<'a> {
    /// The AST root is a supported native source.
    Source(NativeSourceAst<'a>),
    /// The AST root is not a native source.
    NotSource,
}

impl<'a> NativeSourceAst<'a> {
    /// Recognize a supported source AST node.
    pub(in crate::planning::selected::native) fn from_ast(
        root: &'a AstNode,
    ) -> NativeSourceAstMatch<'a> {
        match root {
            AstNode::Nodes { reference } => NativeSourceAstMatch::Source(Self::Nodes(reference)),
            AstNode::Edges { reference } => NativeSourceAstMatch::Source(Self::Edges(reference)),
            AstNode::NodesWhere { predicate } => {
                NativeSourceAstMatch::Source(Self::NodesWhere(predicate))
            }
            AstNode::EdgesWhere { predicate } => {
                NativeSourceAstMatch::Source(Self::EdgesWhere(predicate))
            }
            AstNode::VectorSearchNodes {
                label,
                property,
                tenant_value,
                query_vector,
                k,
            } => NativeSourceAstMatch::Source(Self::NodeVectorSearch {
                label,
                property,
                tenant_value: tenant_value.as_ref(),
                query_vector,
                k,
            }),
            AstNode::TextSearchNodes {
                label,
                property,
                tenant_value,
                query_text,
                k,
            } => NativeSourceAstMatch::Source(Self::NodeTextSearch {
                label,
                property,
                tenant_value: tenant_value.as_ref(),
                query_text,
                k,
            }),
            AstNode::VectorSearchEdges {
                label,
                property,
                tenant_value,
                query_vector,
                k,
            } => NativeSourceAstMatch::Source(Self::EdgeVectorSearch {
                label,
                property,
                tenant_value: tenant_value.as_ref(),
                query_vector,
                k,
            }),
            AstNode::TextSearchEdges {
                label,
                property,
                tenant_value,
                query_text,
                k,
            } => NativeSourceAstMatch::Source(Self::EdgeTextSearch {
                label,
                property,
                tenant_value: tenant_value.as_ref(),
                query_text,
                k,
            }),
            _ => NativeSourceAstMatch::NotSource,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::StreamBound;
    use helix_ast::graph::{EdgeRef, NodeRef};
    use helix_ast::value::{PropertyInput, PropertyValue};

    #[test]
    fn native_source_ast_recognizes_only_source_roots() {
        assert!(matches!(
            NativeSourceAst::from_ast(&AstNode::Nodes {
                reference: NodeRef::All
            }),
            NativeSourceAstMatch::Source(NativeSourceAst::Nodes(_))
        ));
        assert!(matches!(
            NativeSourceAst::from_ast(&AstNode::Edges {
                reference: EdgeRef::All
            }),
            NativeSourceAstMatch::Source(NativeSourceAst::Edges(_))
        ));
        assert!(matches!(
            NativeSourceAst::from_ast(&AstNode::Context),
            NativeSourceAstMatch::NotSource
        ));
    }

    #[test]
    fn native_source_ast_preserves_search_payloads() {
        let query_vector = PropertyInput::from(PropertyValue::F32Array(vec![0.1]));
        let root = AstNode::VectorSearchNodes {
            label: "Doc".to_owned(),
            property: "embedding".to_owned(),
            tenant_value: None,
            query_vector,
            k: StreamBound::Literal(10),
        };

        assert!(matches!(
            NativeSourceAst::from_ast(&root),
            NativeSourceAstMatch::Source(NativeSourceAst::NodeVectorSearch {
                label: "Doc",
                property: "embedding",
                tenant_value: None,
                k: StreamBound::Literal(10),
                ..
            })
        ));
    }
}
