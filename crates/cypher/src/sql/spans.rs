//! Source spans of sqlparser syntax, and deep recursion, without overflowing
//! the stack.
//!
//! sqlparser's `Spanned` recurses into every child expression. Its parser
//! builds operator chains in a loop rather than by recursion, so a chain can
//! nest as deep as the operator bound allows. These functions compute the
//! same spans, recursing on a stack that grows on demand, as translation
//! itself does.
use sqlparser::ast::{self, Spanned};
use sqlparser::tokenizer::Span;

/// Room left on the stack before a level of span computation moves to a new
/// segment, and the size of each new segment.
const RED_ZONE: usize = 128 * 1024;
const SEGMENT: usize = 2 * 1024 * 1024;
/// Stack for sqlparser computing a span on its own, deep enough for the
/// deepest statement the guards accept even in an unoptimized build.
const DEEP_STACK: usize = 64 * 1024 * 1024;

/// Run one level of a recursion that may nest deeply, on a new stack
/// segment when the current one runs low.
pub(super) fn grown<T>(level: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(RED_ZONE, SEGMENT, level)
}

/// sqlparser's span of `node`, computed on a stack of its own. For forms
/// whose span is rarely needed, such as errors and unsupported syntax.
pub(super) fn deep(node: &impl Spanned) -> Span {
    stacker::grow(DEEP_STACK, || node.span())
}

/// sqlparser's span of an expression, one level at a time.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "sqlparser's expression set is open-ended; other forms use `deep`"
)]
pub(super) fn expr(expression: &ast::Expr) -> Span {
    use ast::Expr as E;
    grown(|| match expression {
        E::Identifier(_)
        | E::CompoundIdentifier(_)
        | E::Value(_)
        | E::TypedString(_)
        | E::Wildcard(_)
        | E::QualifiedWildcard(..) => expression.span(),
        E::IsFalse(value)
        | E::IsNotFalse(value)
        | E::IsTrue(value)
        | E::IsNotTrue(value)
        | E::IsNull(value)
        | E::IsNotNull(value)
        | E::IsUnknown(value)
        | E::IsNotUnknown(value)
        | E::Nested(value)
        | E::IsJson { expr: value, .. }
        | E::IsNormalized { expr: value, .. }
        | E::Ceil { expr: value, .. }
        | E::Floor { expr: value, .. }
        | E::UnaryOp { expr: value, .. }
        | E::Cast { expr: value, .. }
        | E::Extract { expr: value, .. } => expr(value),
        E::IsDistinctFrom(left, right)
        | E::IsNotDistinctFrom(left, right)
        | E::BinaryOp { left, right, .. }
        | E::AnyOp { left, right, .. }
        | E::AllOp { left, right, .. }
        | E::Like {
            expr: left,
            pattern: right,
            ..
        }
        | E::ILike {
            expr: left,
            pattern: right,
            ..
        }
        | E::SimilarTo {
            expr: left,
            pattern: right,
            ..
        }
        | E::Position {
            expr: left,
            r#in: right,
        }
        | E::AtTimeZone {
            timestamp: left,
            time_zone: right,
        } => expr(left).union(&expr(right)),
        E::InList {
            expr: value, list, ..
        } => union(std::iter::once(value.as_ref()).chain(list)),
        E::Between {
            expr: value,
            low,
            high,
            ..
        } => union([value.as_ref(), low.as_ref(), high.as_ref()]),
        E::Tuple(items) => union(items),
        E::Array(array) => union(&array.elem),
        E::Substring {
            expr: value,
            substring_from,
            substring_for,
            ..
        } => union(
            std::iter::once(value.as_ref())
                .chain(substring_from.as_deref())
                .chain(substring_for.as_deref()),
        ),
        E::Trim {
            expr: value,
            trim_what,
            trim_characters,
            ..
        } => union(
            std::iter::once(value.as_ref())
                .chain(trim_what.as_deref())
                .chain(trim_characters.iter().flatten()),
        ),
        E::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => conditions
            .iter()
            .flat_map(|when| [&when.condition, &when.result])
            .chain(operand.as_deref())
            .chain(else_result.as_deref())
            .fold(case_token.0.span.union(&end_token.0.span), |span, part| {
                span.union(&expr(part))
            }),
        E::Function(function) => self::function(function),
        E::CompoundFieldAccess { root, access_chain } => {
            access_chain
                .iter()
                .fold(expr(root), |span, access| match access {
                    ast::AccessExpr::Dot(field) => span.union(&expr(field)),
                    ast::AccessExpr::Subscript(subscript) => span.union(&deep(subscript)),
                })
        }
        other => deep(other),
    })
}

/// The union of the spans of `expressions`, as sqlparser joins them.
fn union<'a>(expressions: impl IntoIterator<Item = &'a ast::Expr>) -> Span {
    expressions
        .into_iter()
        .fold(Span::empty(), |span, expression| {
            span.union(&expr(expression))
        })
}

/// sqlparser's span of a function call: its name, arguments, `FILTER` and
/// `WITHIN GROUP`.
fn function(function: &ast::Function) -> Span {
    let arguments = |arguments: &ast::FunctionArguments| match arguments {
        ast::FunctionArguments::None => Span::empty(),
        ast::FunctionArguments::Subquery(query) => deep(query.as_ref()),
        ast::FunctionArguments::List(list) => list
            .args
            .iter()
            .map(argument)
            .chain(list.clauses.iter().map(deep))
            .fold(Span::empty(), |span, part| span.union(&part)),
    };
    function
        .name
        .0
        .iter()
        .map(Spanned::span)
        .chain([arguments(&function.args), arguments(&function.parameters)])
        .chain(function.filter.as_deref().map(expr))
        .chain(
            function
                .within_group
                .iter()
                .map(|order| expr(&order.expr).union_opt(&order.with_fill.as_ref().map(deep))),
        )
        .fold(Span::empty(), |span, part| span.union(&part))
}

/// sqlparser's span of one function argument.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only an expression argument holds a nested expression"
)]
pub(super) fn argument(argument: &ast::FunctionArg) -> Span {
    let value = |value: &ast::FunctionArgExpr| match value {
        ast::FunctionArgExpr::Expr(value) => expr(value),
        other => other.span(),
    };
    match argument {
        ast::FunctionArg::Unnamed(value_expression) => value(value_expression),
        ast::FunctionArg::Named { name, arg, .. } => name.span.union(&value(arg)),
        ast::FunctionArg::ExprNamed { name, arg, .. } => expr(name).union(&value(arg)),
    }
}

/// sqlparser's span of a select-list entry.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "wildcard entries hold no expressions of their own depth"
)]
pub(super) fn select_item(item: &ast::SelectItem) -> Span {
    match item {
        ast::SelectItem::UnnamedExpr(value) => expr(value),
        ast::SelectItem::ExprWithAlias { expr: value, alias } => expr(value).union(&alias.span),
        ast::SelectItem::ExprWithAliases {
            expr: value,
            aliases,
        } => aliases
            .iter()
            .fold(expr(value), |span, alias| span.union(&alias.span)),
        other => deep(other),
    }
}

/// The span of a `FROM` item: a table's name and alias. Other items, which
/// translation rejects, use sqlparser's own span.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "only plain tables translate; other FROM items are reported whole"
)]
pub(super) fn table(factor: &ast::TableFactor) -> Span {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => name
            .0
            .iter()
            .map(Spanned::span)
            .chain(alias.iter().map(|alias| alias.name.span))
            .fold(Span::empty(), |span, part| span.union(&part)),
        other => deep(other),
    }
}
