//! Iterative nesting bound for native requests.
//!
//! Planning, telemetry, execution and drop glue walk a request recursively, so
//! a request built in memory must be bounded before any of them run. JSON
//! requests are already bounded by their text; this pass bounds every other
//! request with one explicit stack, whatever its shape.

use crate::batch::BatchEntry;
use crate::expr::{Expr, Predicate, StreamBound};
use crate::projection::Projection;
use crate::query::QueryValue;
use crate::traversal::{AstNode, RepeatConfig};
use crate::value::{PropertyInput, PropertyValue};

/// One nested part of a request.
pub(crate) enum Node<'a> {
    Entry(&'a BatchEntry),
    Ast(&'a AstNode),
    Predicate(&'a Predicate),
    Expr(&'a Expr),
    Input(&'a PropertyInput),
    Value(&'a PropertyValue),
    Query(&'a QueryValue),
}

/// Whether no part below `roots` nests more than `maximum` levels deep. Each
/// batch entry, step, predicate, expression and list or map value is one level.
pub(crate) fn within<'a>(roots: impl IntoIterator<Item = Node<'a>>, maximum: usize) -> bool {
    let mut pending = roots.into_iter().map(|node| (node, 1)).collect::<Vec<_>>();
    while let Some((node, depth)) = pending.pop() {
        if depth > maximum {
            return false;
        }
        let mut push = |child| pending.push((child, depth + 1));
        match node {
            Node::Entry(BatchEntry::Query(query)) => push(Node::Ast(&query.root)),
            Node::Entry(BatchEntry::ForEach { body, .. }) => {
                body.iter().for_each(|entry| push(Node::Entry(entry)))
            }
            Node::Ast(node) => ast_children(node, &mut push),
            Node::Predicate(predicate) => match predicate {
                Predicate::Eq { left, right }
                | Predicate::Neq { left, right }
                | Predicate::Gt { left, right }
                | Predicate::Gte { left, right }
                | Predicate::Lt { left, right }
                | Predicate::Lte { left, right }
                | Predicate::Compare { left, right, .. }
                | Predicate::StartsWith {
                    value: left,
                    prefix: right,
                }
                | Predicate::EndsWith {
                    value: left,
                    suffix: right,
                }
                | Predicate::Contains {
                    value: left,
                    substring: right,
                }
                | Predicate::IsIn {
                    value: left,
                    values: right,
                } => {
                    push(Node::Expr(left));
                    push(Node::Expr(right));
                }
                Predicate::Between { value, min, max } => {
                    [value, min, max]
                        .into_iter()
                        .for_each(|expr| push(Node::Expr(expr)));
                }
                Predicate::And { predicates } | Predicate::Or { predicates } => {
                    predicates
                        .iter()
                        .for_each(|predicate| push(Node::Predicate(predicate)));
                }
                Predicate::Not { predicate } => push(Node::Predicate(predicate)),
                Predicate::HasKey { .. }
                | Predicate::IsNull { .. }
                | Predicate::IsNotNull { .. } => {}
            },
            Node::Expr(expr) => match expr {
                Expr::Add { left, right }
                | Expr::Sub { left, right }
                | Expr::Mul { left, right }
                | Expr::Div { left, right }
                | Expr::Mod { left, right } => {
                    push(Node::Expr(left));
                    push(Node::Expr(right));
                }
                Expr::Neg { expr } => push(Node::Expr(expr)),
                Expr::Case {
                    when_then,
                    else_expr,
                } => {
                    for branch in when_then {
                        push(Node::Predicate(&branch.when));
                        push(Node::Expr(&branch.then));
                    }
                    else_expr.iter().for_each(|expr| push(Node::Expr(expr)));
                }
                Expr::Constant(value) => push(Node::Value(value)),
                Expr::Property(_)
                | Expr::Id
                | Expr::Timestamp
                | Expr::DateTimeNow
                | Expr::Param(_) => {}
            },
            Node::Input(PropertyInput::Value(value)) => push(Node::Value(value)),
            Node::Input(PropertyInput::Expr(expr)) => push(Node::Expr(expr)),
            Node::Value(PropertyValue::Array(values)) => {
                values.iter().for_each(|value| push(Node::Value(value)))
            }
            Node::Value(PropertyValue::Object(values)) => {
                values.values().for_each(|value| push(Node::Value(value)));
            }
            Node::Value(
                PropertyValue::Null
                | PropertyValue::Bool(_)
                | PropertyValue::I64(_)
                | PropertyValue::DateTime(_)
                | PropertyValue::F64(_)
                | PropertyValue::F32(_)
                | PropertyValue::String(_)
                | PropertyValue::Bytes(_)
                | PropertyValue::I64Array(_)
                | PropertyValue::F64Array(_)
                | PropertyValue::F32Array(_)
                | PropertyValue::StringArray(_),
            ) => {}
            Node::Query(QueryValue::Array(values)) => {
                values.iter().for_each(|value| push(Node::Query(value)))
            }
            Node::Query(QueryValue::Object(values)) => {
                values.values().for_each(|value| push(Node::Query(value)));
            }
            Node::Query(
                QueryValue::Null
                | QueryValue::Bool(_)
                | QueryValue::I64(_)
                | QueryValue::F64(_)
                | QueryValue::F32(_)
                | QueryValue::String(_),
            ) => {}
        }
    }
    true
}

/// The nested parts of one traversal step: its input and anything it embeds.
fn ast_children<'a>(node: &'a AstNode, push: &mut impl FnMut(Node<'a>)) {
    let bound = |push: &mut dyn FnMut(Node<'a>), bound: &'a StreamBound| match bound {
        StreamBound::Expr(expr) => push(Node::Expr(expr)),
        StreamBound::Literal(_) => {}
    };
    match node {
        AstNode::Context
        | AstNode::Nodes { .. }
        | AstNode::Edges { .. }
        | AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. }
        | AstNode::ShortestPath { .. } => {}
        AstNode::NodesWhere { predicate } | AstNode::EdgesWhere { predicate } => {
            push(Node::Predicate(predicate));
        }
        AstNode::VectorSearchNodes {
            tenant_value,
            query_vector: query,
            k,
            ..
        }
        | AstNode::TextSearchNodes {
            tenant_value,
            query_text: query,
            k,
            ..
        }
        | AstNode::VectorSearchEdges {
            tenant_value,
            query_vector: query,
            k,
            ..
        }
        | AstNode::TextSearchEdges {
            tenant_value,
            query_text: query,
            k,
            ..
        } => {
            tenant_value
                .iter()
                .for_each(|value| push(Node::Input(value)));
            push(Node::Input(query));
            bound(push, k);
        }
        AstNode::TextSearchNodesWithin {
            input,
            tenant_value,
            query_text: query,
            k,
            ..
        }
        | AstNode::TextSearchEdgesWithin {
            input,
            tenant_value,
            query_text: query,
            k,
            ..
        }
        | AstNode::VectorSearchNodesWithin {
            input,
            tenant_value,
            query_vector: query,
            k,
            ..
        }
        | AstNode::VectorSearchEdgesWithin {
            input,
            tenant_value,
            query_vector: query,
            k,
            ..
        } => {
            push(Node::Ast(input));
            tenant_value
                .iter()
                .for_each(|value| push(Node::Input(value)));
            push(Node::Input(query));
            bound(push, k);
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
        | AstNode::HasLabel { input, .. }
        | AstNode::HasKey { input, .. }
        | AstNode::Dedup { input }
        | AstNode::Within { input, .. }
        | AstNode::Without { input, .. }
        | AstNode::EdgeHasLabel { input, .. }
        | AstNode::As { input, .. }
        | AstNode::Store { input, .. }
        | AstNode::Select { input, .. }
        | AstNode::Bind { input, .. }
        | AstNode::Count { input }
        | AstNode::Exists { input }
        | AstNode::Id { input }
        | AstNode::Label { input }
        | AstNode::Values { input, .. }
        | AstNode::ValueMap { input, .. }
        | AstNode::ProjectBindings { input, .. }
        | AstNode::EdgeProperties { input }
        | AstNode::RemoveProperty { input, .. }
        | AstNode::Drop { input }
        | AstNode::DropEdge { input, .. }
        | AstNode::DropEdgeLabeled { input, .. }
        | AstNode::OrderBy { input, .. }
        | AstNode::OrderByMultiple { input, .. }
        | AstNode::Group { input, .. }
        | AstNode::GroupCount { input, .. }
        | AstNode::AggregateBy { input, .. }
        | AstNode::Fold { input }
        | AstNode::Unfold { input }
        | AstNode::Path { input }
        | AstNode::SimplePath { input }
        | AstNode::SackSet { input, .. }
        | AstNode::SackAdd { input, .. }
        | AstNode::SackGet { input } => push(Node::Ast(input)),
        AstNode::Inject { input, .. } | AstNode::DropEdgeById { input, .. } => {
            input.iter().for_each(|input| push(Node::Ast(input)));
        }
        AstNode::Has { input, value, .. }
        | AstNode::WithSack {
            input,
            initial: value,
        } => {
            push(Node::Ast(input));
            push(Node::Value(value));
        }
        AstNode::Where { input, predicate } => {
            push(Node::Ast(input));
            push(Node::Predicate(predicate));
        }
        AstNode::EdgeHas { input, value, .. } | AstNode::SetProperty { input, value, .. } => {
            push(Node::Ast(input));
            push(Node::Input(value));
        }
        AstNode::Limit { input, count } | AstNode::Skip { input, count } => {
            push(Node::Ast(input));
            bound(push, count);
        }
        AstNode::Range { input, start, end } => {
            push(Node::Ast(input));
            bound(push, start);
            bound(push, end);
        }
        AstNode::Project { input, projections } => {
            push(Node::Ast(input));
            for projection in projections {
                match projection {
                    Projection::Expr(projection) => push(Node::Expr(&projection.expr)),
                    Projection::Property(_) => {}
                }
            }
        }
        AstNode::AddN {
            input, properties, ..
        } => {
            input.iter().for_each(|input| push(Node::Ast(input)));
            properties
                .iter()
                .for_each(|(_, value)| push(Node::Input(value)));
        }
        AstNode::AddE {
            input, properties, ..
        } => {
            push(Node::Ast(input));
            properties
                .iter()
                .for_each(|(_, value)| push(Node::Input(value)));
        }
        AstNode::Repeat {
            input,
            config:
                RepeatConfig {
                    traversal,
                    until,
                    emit_predicate,
                    ..
                },
        } => {
            push(Node::Ast(input));
            push(Node::Ast(&traversal.root));
            until
                .iter()
                .chain(emit_predicate)
                .for_each(|predicate| push(Node::Predicate(predicate)));
        }
        AstNode::Union { input, traversals } | AstNode::Coalesce { input, traversals } => {
            push(Node::Ast(input));
            traversals
                .iter()
                .for_each(|traversal| push(Node::Ast(&traversal.root)));
        }
        AstNode::Choose {
            input,
            condition,
            then_traversal,
            else_traversal,
        } => {
            push(Node::Ast(input));
            push(Node::Predicate(condition));
            push(Node::Ast(&then_traversal.root));
            else_traversal
                .iter()
                .for_each(|traversal| push(Node::Ast(&traversal.root)));
        }
        AstNode::Optional { input, traversal } => {
            push(Node::Ast(input));
            push(Node::Ast(&traversal.root));
        }
    }
}
