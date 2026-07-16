//! Scope-aware `$context` validation for native AST query roots.
//!
//! Public query roots do not bind implicit context. Branch and repeat
//! sub-traversal bodies do, so the validator deliberately switches scope only at
//! those AST fields instead of treating every nested `Context` as invalid.

use helix_ast::traversal::{AstNode, SubTraversal};

use super::scope::NativeAstScope;
use crate::error;

pub(super) fn validate_query_root_context(root: &AstNode) -> Result<(), error::PlannerError> {
    validate_ast_context(root, NativeAstScope::QueryRoot)?;
    validate_after_bind(root).map(|_| ())
}

fn validate_sub_traversal_context(traversal: &SubTraversal) -> Result<(), error::PlannerError> {
    validate_ast_context(&traversal.root, NativeAstScope::SubTraversal)
}

fn validate_ast_context(root: &AstNode, scope: NativeAstScope) -> Result<(), error::PlannerError> {
    match root {
        AstNode::Context if scope.binds_context() => Ok(()),
        AstNode::Context => Err(error::PlannerError::UnboundContext),
        AstNode::Nodes { .. }
        | AstNode::NodesWhere { .. }
        | AstNode::Edges { .. }
        | AstNode::EdgesWhere { .. }
        | AstNode::VectorSearchNodes { .. }
        | AstNode::TextSearchNodes { .. }
        | AstNode::VectorSearchEdges { .. }
        | AstNode::TextSearchEdges { .. }
        | AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. }
        | AstNode::ShortestPath { .. }
        | AstNode::AddN { input: None, .. }
        | AstNode::DropEdgeById { input: None, .. }
        | AstNode::Inject { input: None, .. } => {
            if scope == NativeAstScope::SubTraversal {
                Err(error::PlannerError::InvalidSubTraversalOperation {
                    op: error::SubTraversalOp::Source,
                })
            } else {
                Ok(())
            }
        }
        AstNode::Out { input, .. }
        | AstNode::In { input, .. }
        | AstNode::Both { input, .. }
        | AstNode::OutE { input, .. }
        | AstNode::InE { input, .. }
        | AstNode::BothE { input, .. }
        | AstNode::OutN { input }
        | AstNode::InN { input }
        | AstNode::OtherN { input }
        | AstNode::Has { input, .. }
        | AstNode::HasLabel { input, .. }
        | AstNode::HasKey { input, .. }
        | AstNode::Where { input, .. }
        | AstNode::Dedup { input }
        | AstNode::Within { input, .. }
        | AstNode::Without { input, .. }
        | AstNode::EdgeHas { input, .. }
        | AstNode::EdgeHasLabel { input, .. }
        | AstNode::Limit { input, .. }
        | AstNode::Skip { input, .. }
        | AstNode::Range { input, .. }
        | AstNode::As { input, .. }
        | AstNode::Store { input, .. }
        | AstNode::Select { input, .. }
        | AstNode::Bind { input, .. }
        | AstNode::Inject {
            input: Some(input), ..
        }
        | AstNode::Count { input }
        | AstNode::Exists { input }
        | AstNode::Id { input }
        | AstNode::Label { input }
        | AstNode::Values { input, .. }
        | AstNode::ValueMap { input, .. }
        | AstNode::Project { input, .. }
        | AstNode::EdgeProperties { input }
        | AstNode::AddN {
            input: Some(input), ..
        }
        | AstNode::AddE { input, .. }
        | AstNode::SetProperty { input, .. }
        | AstNode::RemoveProperty { input, .. }
        | AstNode::Drop { input }
        | AstNode::DropEdge { input, .. }
        | AstNode::DropEdgeLabeled { input, .. }
        | AstNode::DropEdgeById {
            input: Some(input), ..
        }
        | AstNode::OrderBy { input, .. }
        | AstNode::OrderByMultiple { input, .. }
        | AstNode::Group { input, .. }
        | AstNode::GroupCount { input, .. }
        | AstNode::AggregateBy { input, .. }
        | AstNode::Fold { input }
        | AstNode::Unfold { input }
        | AstNode::Path { input }
        | AstNode::SimplePath { input }
        | AstNode::WithSack { input, .. }
        | AstNode::SackSet { input, .. }
        | AstNode::SackAdd { input, .. }
        | AstNode::SackGet { input } => validate_ast_context(input, scope),
        AstNode::ProjectBindings { input, .. } => {
            if scope == NativeAstScope::SubTraversal {
                Err(error::PlannerError::InvalidSubTraversalOperation {
                    op: error::SubTraversalOp::ProjectBindings,
                })
            } else {
                validate_ast_context(input, scope)
            }
        }
        AstNode::Repeat { input, config } => {
            validate_ast_context(input, scope)?;
            validate_sub_traversal_context(&config.traversal)
        }
        AstNode::Union { input, traversals } | AstNode::Coalesce { input, traversals } => {
            validate_ast_context(input, scope)?;
            traversals
                .iter()
                .try_for_each(validate_sub_traversal_context)
        }
        AstNode::Choose {
            input,
            then_traversal,
            else_traversal,
            ..
        } => {
            validate_ast_context(input, scope)?;
            validate_sub_traversal_context(then_traversal)?;
            else_traversal
                .as_ref()
                .map(validate_sub_traversal_context)
                .unwrap_or(Ok(()))
        }
        AstNode::Optional { input, traversal } => {
            validate_ast_context(input, scope)?;
            validate_sub_traversal_context(traversal)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundCurrentKind {
    Node,
    Edge,
    Unknown,
}

impl BoundCurrentKind {
    const fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Node, Self::Node) => Self::Node,
            (Self::Edge, Self::Edge) => Self::Edge,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BindState {
    has_bindings: bool,
    current: BoundCurrentKind,
}

impl BindState {
    const fn unbound(current: BoundCurrentKind) -> Self {
        Self {
            has_bindings: false,
            current,
        }
    }

    const fn with_bindings(self) -> Self {
        Self {
            has_bindings: true,
            current: self.current,
        }
    }

    const fn with_current(self, current: BoundCurrentKind) -> Self {
        Self {
            has_bindings: self.has_bindings,
            current,
        }
    }

    const fn merge(self, other: Self) -> Self {
        Self {
            has_bindings: self.has_bindings || other.has_bindings,
            current: self.current.merge(other.current),
        }
    }
}

fn validate_after_bind(root: &AstNode) -> Result<BindState, error::PlannerError> {
    validate_after_bind_with_context(root, BindState::unbound(BoundCurrentKind::Unknown))
}

fn validate_after_bind_with_context(
    root: &AstNode,
    context: BindState,
) -> Result<BindState, error::PlannerError> {
    match root {
        AstNode::Context => Ok(context),
        AstNode::Nodes { .. }
        | AstNode::NodesWhere { .. }
        | AstNode::VectorSearchNodes { .. }
        | AstNode::TextSearchNodes { .. } => Ok(BindState::unbound(BoundCurrentKind::Node)),
        AstNode::Edges { .. }
        | AstNode::EdgesWhere { .. }
        | AstNode::VectorSearchEdges { .. }
        | AstNode::TextSearchEdges { .. } => Ok(BindState::unbound(BoundCurrentKind::Edge)),
        AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. }
        | AstNode::ShortestPath { .. }
        | AstNode::AddN { input: None, .. }
        | AstNode::DropEdgeById { input: None, .. }
        | AstNode::Inject { input: None, .. } => Ok(BindState::unbound(BoundCurrentKind::Unknown)),
        AstNode::Bind { input, .. } => {
            validate_after_bind_with_context(input, context).map(BindState::with_bindings)
        }
        AstNode::Choose {
            input,
            then_traversal,
            else_traversal,
            ..
        } => {
            let input_state = validate_after_bind_with_context(input, context)?;
            if input_state.has_bindings {
                return Err(error::PlannerError::InvalidAfterBindOperation {
                    op: error::AfterBindOp::Choose,
                });
            }
            let then_state = validate_after_bind_with_context(&then_traversal.root, input_state)?;
            let else_state = else_traversal
                .as_ref()
                .map(|traversal| validate_after_bind_with_context(&traversal.root, input_state))
                .unwrap_or(Ok(input_state))?;
            Ok(then_state.merge(else_state))
        }
        AstNode::OrderBy { input, .. } | AstNode::OrderByMultiple { input, .. } => {
            let input_state = validate_after_bind_with_context(input, context)?;
            if input_state.has_bindings {
                Err(error::PlannerError::InvalidAfterBindOperation {
                    op: error::AfterBindOp::OrderBy,
                })
            } else {
                Ok(input_state)
            }
        }
        AstNode::Repeat { input, config } => {
            let input_state = validate_after_bind_with_context(input, context)?;
            if input_state.has_bindings {
                return Err(error::PlannerError::InvalidAfterBindOperation {
                    op: error::AfterBindOp::Repeat,
                });
            }
            validate_after_bind_with_context(&config.traversal.root, input_state)
        }
        AstNode::Id { input } => {
            validate_after_bind_terminal(input, context, error::AfterBindOp::Id)
        }
        AstNode::Label { input } => {
            validate_after_bind_terminal(input, context, error::AfterBindOp::Label)
        }
        AstNode::Values { input, .. } => {
            validate_after_bind_terminal(input, context, error::AfterBindOp::Values)
        }
        AstNode::ValueMap { input, .. } => {
            validate_after_bind_terminal(input, context, error::AfterBindOp::ValueMap)
        }
        AstNode::Project { input, .. } => {
            validate_after_bind_terminal(input, context, error::AfterBindOp::Project)
        }
        AstNode::EdgeProperties { input } => {
            validate_after_bind_terminal(input, context, error::AfterBindOp::EdgeProperties)
        }
        AstNode::Union { input, traversals } | AstNode::Coalesce { input, traversals } => {
            let input_state = validate_after_bind_with_context(input, context)?;
            traversals.iter().try_fold(input_state, |state, traversal| {
                validate_after_bind_with_context(&traversal.root, input_state)
                    .map(|branch_state| state.merge(branch_state))
            })
        }
        AstNode::Optional { input, traversal } => {
            let input_state = validate_after_bind_with_context(input, context)?;
            validate_after_bind_with_context(&traversal.root, input_state)
                .map(|branch_state| input_state.merge(branch_state))
        }
        AstNode::Out { input, .. } | AstNode::In { input, .. } | AstNode::Both { input, .. } => {
            validate_after_bind_with_context(input, context)
                .map(|state| state.with_current(BoundCurrentKind::Node))
        }
        AstNode::OutE { input, .. } | AstNode::InE { input, .. } | AstNode::BothE { input, .. } => {
            validate_after_bind_with_context(input, context)
                .map(|state| state.with_current(BoundCurrentKind::Edge))
        }
        AstNode::OutN { input } | AstNode::InN { input } | AstNode::OtherN { input } => {
            validate_after_bind_with_context(input, context)
                .map(|state| state.with_current(BoundCurrentKind::Node))
        }
        AstNode::Has { input, .. }
        | AstNode::HasLabel { input, .. }
        | AstNode::HasKey { input, .. }
        | AstNode::Where { input, .. }
        | AstNode::Dedup { input }
        | AstNode::Within { input, .. }
        | AstNode::Without { input, .. }
        | AstNode::Limit { input, .. }
        | AstNode::Skip { input, .. }
        | AstNode::Range { input, .. }
        | AstNode::As { input, .. }
        | AstNode::Store { input, .. }
        | AstNode::Select { input, .. }
        | AstNode::Inject {
            input: Some(input), ..
        }
        | AstNode::Count { input }
        | AstNode::Exists { input }
        | AstNode::ProjectBindings { input, .. }
        | AstNode::AddN {
            input: Some(input), ..
        }
        | AstNode::AddE { input, .. }
        | AstNode::SetProperty { input, .. }
        | AstNode::RemoveProperty { input, .. }
        | AstNode::Drop { input }
        | AstNode::DropEdge { input, .. }
        | AstNode::DropEdgeLabeled { input, .. }
        | AstNode::DropEdgeById {
            input: Some(input), ..
        }
        | AstNode::Group { input, .. }
        | AstNode::GroupCount { input, .. }
        | AstNode::AggregateBy { input, .. }
        | AstNode::Fold { input }
        | AstNode::Unfold { input }
        | AstNode::Path { input }
        | AstNode::SimplePath { input }
        | AstNode::WithSack { input, .. }
        | AstNode::SackSet { input, .. }
        | AstNode::SackAdd { input, .. }
        | AstNode::SackGet { input } => validate_after_bind_with_context(input, context),
        AstNode::EdgeHas { input, .. } => {
            validate_after_bind_edge_filter(input, context, error::AfterBindOp::EdgeHas)
        }
        AstNode::EdgeHasLabel { input, .. } => {
            validate_after_bind_edge_filter(input, context, error::AfterBindOp::EdgeHasLabel)
        }
    }
}

fn validate_after_bind_terminal(
    input: &AstNode,
    context: BindState,
    op: error::AfterBindOp,
) -> Result<BindState, error::PlannerError> {
    let state = validate_after_bind_with_context(input, context)?;
    if state.has_bindings {
        Err(error::PlannerError::InvalidAfterBindOperation { op })
    } else {
        Ok(state)
    }
}

fn validate_after_bind_edge_filter(
    input: &AstNode,
    context: BindState,
    op: error::AfterBindOp,
) -> Result<BindState, error::PlannerError> {
    let state = validate_after_bind_with_context(input, context)?;
    if state.has_bindings && state.current != BoundCurrentKind::Edge {
        Err(error::PlannerError::InvalidAfterBindOperation { op })
    } else {
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::{Predicate, StreamBound};
    use helix_ast::graph::NodeRef;
    use helix_ast::traversal;

    #[test]
    fn query_root_context_validation_rejects_unbound_context_under_wrappers() {
        assert_eq!(
            validate_query_root_context(&AstNode::Context),
            Err(error::PlannerError::UnboundContext)
        );
        assert_eq!(
            validate_query_root_context(&AstNode::Count {
                input: Box::new(AstNode::Context),
            }),
            Err(error::PlannerError::UnboundContext)
        );
        assert_eq!(
            validate_query_root_context(&AstNode::Limit {
                input: Box::new(AstNode::Context),
                count: StreamBound::Literal(1),
            }),
            Err(error::PlannerError::UnboundContext)
        );
    }

    #[test]
    fn query_root_context_validation_allows_context_inside_sub_traversals_only() {
        validate_query_root_context(&AstNode::Optional {
            input: Box::new(AstNode::Nodes {
                reference: NodeRef::All,
            }),
            traversal: traversal::sub().out(Some("FOLLOWS")),
        })
        .expect("sub-traversal context is bound");

        assert_eq!(
            validate_query_root_context(&AstNode::Optional {
                input: Box::new(AstNode::Context),
                traversal: traversal::sub().out(Some("FOLLOWS")),
            }),
            Err(error::PlannerError::UnboundContext)
        );
    }

    #[test]
    fn sub_traversal_validation_rejects_source_roots_and_nested_binding_projection() {
        assert_eq!(
            validate_query_root_context(&AstNode::Optional {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                traversal: traversal::SubTraversal {
                    root: Box::new(AstNode::Nodes {
                        reference: NodeRef::All,
                    }),
                },
            }),
            Err(error::PlannerError::InvalidSubTraversalOperation {
                op: error::SubTraversalOp::Source,
            })
        );

        assert_eq!(
            validate_query_root_context(&AstNode::Optional {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                traversal: traversal::SubTraversal {
                    root: Box::new(AstNode::ProjectBindings {
                        input: Box::new(AstNode::Context),
                        projections: vec![helix_ast::projection::BindingProjection::current(
                            "$id", "id",
                        )],
                        distinct: false,
                    }),
                },
            }),
            Err(error::PlannerError::InvalidSubTraversalOperation {
                op: error::SubTraversalOp::ProjectBindings,
            })
        );
    }

    #[test]
    fn row_binding_validation_rejects_choose_and_order_after_bind() {
        assert_eq!(
            validate_query_root_context(&AstNode::Choose {
                input: Box::new(AstNode::Bind {
                    input: Box::new(AstNode::Nodes {
                        reference: NodeRef::All,
                    }),
                    name: "service".to_string(),
                }),
                condition: Predicate::eq("tenant", "acme"),
                then_traversal: traversal::sub().out(Some("ROUTES_TO")),
                else_traversal: None,
            }),
            Err(error::PlannerError::InvalidAfterBindOperation {
                op: error::AfterBindOp::Choose,
            })
        );

        assert_eq!(
            validate_query_root_context(&AstNode::OrderBy {
                input: Box::new(AstNode::Union {
                    input: Box::new(AstNode::Nodes {
                        reference: NodeRef::All,
                    }),
                    traversals: vec![
                        traversal::sub().bind("service"),
                        traversal::sub().out(Some("ROUTES_TO")),
                    ],
                }),
                property: "name".to_string(),
                order: traversal::Order::Asc,
            }),
            Err(error::PlannerError::InvalidAfterBindOperation {
                op: error::AfterBindOp::OrderBy,
            })
        );
    }

    #[test]
    fn row_binding_validation_rejects_non_row_terminals_after_bind() {
        let bound = || {
            Box::new(AstNode::Bind {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                name: "service".to_string(),
            })
        };
        let cases = [
            (AstNode::Id { input: bound() }, error::AfterBindOp::Id),
            (AstNode::Label { input: bound() }, error::AfterBindOp::Label),
            (
                AstNode::Values {
                    input: bound(),
                    properties: vec!["name".to_string()],
                },
                error::AfterBindOp::Values,
            ),
            (
                AstNode::ValueMap {
                    input: bound(),
                    properties: Some(vec!["name".to_string()]),
                },
                error::AfterBindOp::ValueMap,
            ),
            (
                AstNode::Project {
                    input: bound(),
                    projections: vec![helix_ast::projection::Projection::property("name", "name")],
                },
                error::AfterBindOp::Project,
            ),
            (
                AstNode::EdgeProperties { input: bound() },
                error::AfterBindOp::EdgeProperties,
            ),
            (
                AstNode::Repeat {
                    input: bound(),
                    config: traversal::RepeatConfig::new(traversal::sub().out(Some("OWNS"))),
                },
                error::AfterBindOp::Repeat,
            ),
        ];

        for (root, op) in cases {
            assert_eq!(
                validate_query_root_context(&root),
                Err(error::PlannerError::InvalidAfterBindOperation { op })
            );
        }

        assert_eq!(
            validate_query_root_context(&AstNode::Optional {
                input: bound(),
                traversal: traversal::SubTraversal {
                    root: Box::new(AstNode::Id {
                        input: Box::new(AstNode::Context),
                    }),
                },
            }),
            Err(error::PlannerError::InvalidAfterBindOperation {
                op: error::AfterBindOp::Id,
            })
        );
    }

    #[test]
    fn row_binding_validation_allows_row_preserving_terminals_after_bind() {
        let bound = || {
            Box::new(AstNode::Bind {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                name: "service".to_string(),
            })
        };

        validate_query_root_context(&AstNode::Count { input: bound() })
            .expect("count after bind is allowed");
        validate_query_root_context(&AstNode::Exists { input: bound() })
            .expect("exists after bind is allowed");
        validate_query_root_context(&AstNode::ProjectBindings {
            input: bound(),
            projections: vec![helix_ast::projection::BindingProjection::binding(
                "service",
                "$id",
                "service_id",
            )],
            distinct: false,
        })
        .expect("project_bindings after bind is allowed");
    }

    #[test]
    fn row_binding_validation_requires_edge_current_for_edge_filters_after_bind() {
        let node_bound = || {
            Box::new(AstNode::Bind {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                name: "service".to_string(),
            })
        };
        assert_eq!(
            validate_query_root_context(&AstNode::EdgeHasLabel {
                input: node_bound(),
                label: "OWNS".to_string(),
            }),
            Err(error::PlannerError::InvalidAfterBindOperation {
                op: error::AfterBindOp::EdgeHasLabel,
            })
        );
        assert_eq!(
            validate_query_root_context(&AstNode::EdgeHas {
                input: node_bound(),
                property: "weight".to_string(),
                value: helix_ast::value::PropertyInput::from(1_i64),
            }),
            Err(error::PlannerError::InvalidAfterBindOperation {
                op: error::AfterBindOp::EdgeHas,
            })
        );

        validate_query_root_context(&AstNode::EdgeHasLabel {
            input: Box::new(AstNode::Bind {
                input: Box::new(AstNode::OutE {
                    input: Box::new(AstNode::Nodes {
                        reference: NodeRef::All,
                    }),
                    label: Some("OWNS".to_string()),
                }),
                name: "route".to_string(),
            }),
            label: "OWNS".to_string(),
        })
        .expect("edge-current edge filters after bind are allowed");

        validate_query_root_context(&AstNode::Optional {
            input: Box::new(AstNode::OutE {
                input: Box::new(AstNode::Nodes {
                    reference: NodeRef::All,
                }),
                label: Some("OWNS".to_string()),
            }),
            traversal: traversal::sub().bind("route").edge_has_label("OWNS"),
        })
        .expect("edge-current branch edge filters after bind are allowed");
    }
}
