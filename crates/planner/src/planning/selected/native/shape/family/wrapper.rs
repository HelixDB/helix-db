//! Native access-stream wrapper-shape recognition.
//!
//! This module only captures the recursive input and the pending append
//! operation. Payload validation still happens after the input has proved to be
//! an access stream, preserving fail-closed unsupported-input behavior.

use helix_ast::expr::Predicate;
use helix_ast::traversal::AstNode;

use super::super::operation;

/// Access-stream wrapper over a recursive input AST.
pub(in crate::planning::selected::native::shape) struct NativeAccessStreamWrapper<'a> {
    input: &'a AstNode,
    op: operation::NativeAccessStreamOp<'a>,
}

impl<'a> NativeAccessStreamWrapper<'a> {
    const fn new(input: &'a AstNode, op: operation::NativeAccessStreamOp<'a>) -> Self {
        Self { input, op }
    }

    pub(in crate::planning::selected::native::shape) const fn input(&self) -> &'a AstNode {
        self.input
    }

    pub(in crate::planning::selected::native::shape) fn into_op(
        self,
    ) -> operation::NativeAccessStreamOp<'a> {
        self.op
    }
}

pub(super) enum NativeAccessStreamWrapperMatch<'a> {
    Wrapper(NativeAccessStreamWrapper<'a>),
    NotWrapper,
}

pub(super) fn wrapper_from_ast(root: &AstNode) -> NativeAccessStreamWrapperMatch<'_> {
    NativeAccessStreamWrapperMatch::Wrapper(match root {
        AstNode::Has {
            input,
            property,
            value,
        } => wrapper(
            input,
            operation::NativeAccessStreamOp::Filter(Predicate::eq(property.clone(), value.clone())),
        ),
        AstNode::EdgeHas {
            input,
            property,
            value,
        } => wrapper(
            input,
            operation::NativeAccessStreamOp::Filter(Predicate::eq(property.clone(), value.clone())),
        ),
        AstNode::HasLabel { input, label } | AstNode::EdgeHasLabel { input, label } => wrapper(
            input,
            operation::NativeAccessStreamOp::Filter(Predicate::eq("$label", label.clone())),
        ),
        AstNode::HasKey { input, property } => wrapper(
            input,
            operation::NativeAccessStreamOp::Filter(Predicate::has_key(property)),
        ),
        AstNode::Where { input, predicate } => wrapper(
            input,
            operation::NativeAccessStreamOp::Filter(predicate.clone()),
        ),
        AstNode::Dedup { input } => wrapper(input, operation::NativeAccessStreamOp::Distinct),
        AstNode::Limit { input, count } => {
            wrapper(input, operation::NativeAccessStreamOp::Limit(count))
        }
        AstNode::Skip { input, count } => {
            wrapper(input, operation::NativeAccessStreamOp::Skip(count))
        }
        AstNode::Range { input, start, end } => {
            wrapper(input, operation::NativeAccessStreamOp::Range { start, end })
        }
        AstNode::OrderBy {
            input,
            property,
            order,
        } => wrapper(
            input,
            operation::NativeAccessStreamOp::OrderBy {
                property,
                order: *order,
            },
        ),
        AstNode::OrderByMultiple { input, orderings } => wrapper(
            input,
            operation::NativeAccessStreamOp::OrderByMultiple(orderings),
        ),
        AstNode::Within { input, variable } => {
            wrapper(input, operation::NativeAccessStreamOp::Within(variable))
        }
        AstNode::Without { input, variable } => {
            wrapper(input, operation::NativeAccessStreamOp::Without(variable))
        }
        AstNode::Select { input, name } => {
            wrapper(input, operation::NativeAccessStreamOp::Select(name))
        }
        AstNode::Bind { input, name } => {
            wrapper(input, operation::NativeAccessStreamOp::Bind(name))
        }
        AstNode::Inject {
            input: Some(input),
            variable,
        } => wrapper(input, operation::NativeAccessStreamOp::Inject(variable)),
        AstNode::As { input, name } => wrapper(input, operation::NativeAccessStreamOp::As(name)),
        AstNode::Store { input, name } => {
            wrapper(input, operation::NativeAccessStreamOp::Store(name))
        }
        _ => return NativeAccessStreamWrapperMatch::NotWrapper,
    })
}

fn wrapper<'a>(
    input: &'a AstNode,
    op: operation::NativeAccessStreamOp<'a>,
) -> NativeAccessStreamWrapper<'a> {
    NativeAccessStreamWrapper::new(input, op)
}
