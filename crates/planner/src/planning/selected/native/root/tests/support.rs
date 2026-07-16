use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::traversal::AstNode;

use crate::{catalog, context, error, logical};

#[derive(Debug)]
pub(super) enum LoweredRoot {
    Native(Box<logical::LogicalExpr>),
    NotNative,
}

impl LoweredRoot {
    pub(super) fn expect_native(self, message: &str) -> logical::LogicalExpr {
        match self {
            Self::Native(expr) => *expr,
            Self::NotNative => panic!("{message}"),
        }
    }

    pub(super) const fn is_native(&self) -> bool {
        matches!(self, Self::Native(_))
    }
}

pub(super) fn node_source() -> Box<AstNode> {
    Box::new(AstNode::Nodes {
        reference: NodeRef::All,
    })
}

pub(super) fn edge_source() -> Box<AstNode> {
    Box::new(AstNode::Edges {
        reference: EdgeRef::All,
    })
}

pub(super) fn ctx() -> context::PlannerContext {
    context::PlannerContext::default()
}

pub(super) fn search_ctx() -> context::PlannerContext {
    context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default()
            .with_vector(
                catalog::SearchIndexKey::try_new(catalog::ElementKind::Node, "Doc", "embedding")
                    .unwrap(),
                catalog::SearchIndexScope::Unscoped,
            )
            .with_text(
                catalog::SearchIndexKey::try_new(catalog::ElementKind::Edge, "MENTIONS", "body")
                    .unwrap(),
                catalog::SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
            ),
        ..context::PlannerContext::default()
    }
}

pub(super) fn lower(root: AstNode) -> Result<LoweredRoot, error::PlannerError> {
    lower_with(&ctx(), root)
}

pub(super) fn lower_with(
    ctx: &context::PlannerContext,
    root: AstNode,
) -> Result<LoweredRoot, error::PlannerError> {
    super::super::native_selectable_root_from_ast(ctx, &root).map(|root| match root {
        super::super::NativeSelectableRoot::Root(root) => {
            LoweredRoot::Native(Box::new(root.expr().clone()))
        }
        super::super::NativeSelectableRoot::NotSelectable => LoweredRoot::NotNative,
    })
}

pub(super) fn assert_native(root: AstNode) {
    assert!(
        lower(root).unwrap().is_native(),
        "root should lower natively"
    );
}
