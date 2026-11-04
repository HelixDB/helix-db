//! Expressions: SQL operators, predicates and functions as Cypher expressions.
//!
//! Each SQL construct maps to the Cypher expression with the same result.
//! Where neo4j-jdbc's translation differs from SQL, this follows SQL:
//! `SUBSTRING` starts at 1, `(a, b) <> (c, d)` holds when any pair differs,
//! and two-argument `REPLACE` removes the search string.
use super::tables::Scope;
use super::{binary, call, invalid, literal, node, unsupported, Translator};
use crate::syntax::{Expr, ExprKind};
use helix_planner::{
    ir::AtLeast,
    relational::{self as r, category, detail, QueryError, Result, Span},
};
use sqlparser::ast;

/// Nesting allowed while translating, above the Cypher limit so that the
/// limit itself reports the depth of the translated tree.
const MAX_TRANSLATION_DEPTH: usize = 2 * r::MAX_EXPRESSION_DEPTH;

impl Translator<'_> {
    /// Translate one expression, resolving columns in `scope`.
    pub(super) fn expr(&self, expression: &ast::Expr, scope: &Scope) -> Result<Expr> {
        let depth = self.depth.get() + 1;
        if depth > MAX_TRANSLATION_DEPTH {
            return Err(QueryError::compile(
                category::RESOURCE_LIMIT,
                detail::EXPRESSION_DEPTH,
                "SQL expression nesting exceeds the translation limit",
            )
            .at(self.at(expression)));
        }
        self.depth.set(depth);
        let result = self.expr_at_depth(expression, scope);
        self.depth.set(depth - 1);
        result
    }

    /// An operand of a comparison, `NOT`, `AND` or a predicate. sqlparser binds
    /// `XOR` tighter than all of these, unlike the dialects with a logical
    /// `XOR`, so an unparenthesized one here would be grouped wrongly.
    fn logical(&self, expression: &ast::Expr, scope: &Scope) -> Result<Expr> {
        if matches!(
            expression,
            ast::Expr::BinaryOp {
                op: ast::BinaryOperator::Xor,
                ..
            }
        ) {
            return Err(unsupported(
                "XOR inside a comparison, NOT or AND without parentheses; parenthesize it",
                self.at(expression),
            ));
        }
        self.expr(expression, scope)
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "sqlparser's expression set is open-ended; every other form is unsupported"
    )]
    fn expr_at_depth(&self, expression: &ast::Expr, scope: &Scope) -> Result<Expr> {
        use ast::Expr as E;
        // Spans of connective chains come from their operands, so no
        // recursive span computation walks a long chain.
        let span = || self.at(expression);
        let unary = |operator, value: &ast::Expr| {
            node(
                ExprKind::Unary(operator, Box::new(self.logical(value, scope)?)),
                span(),
            )
        };
        match expression {
            E::Identifier(ident) => scope.column(std::slice::from_ref(ident), span()),
            E::CompoundIdentifier(parts) => scope.column(parts, span()),
            E::Value(value) => self.value(value, false, span()),
            E::Nested(inner) => self.expr(inner, scope),
            E::BinaryOp { left, op, right } => self.binary_op(expression, left, op, right, scope),
            E::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr,
            } => unary(r::Unary::Not, expr),
            E::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => match expr.as_ref() {
                E::Value(value) => self.value(value, true, span()),
                other => unary(r::Unary::Negate, other),
            },
            E::UnaryOp {
                op: ast::UnaryOperator::Plus,
                expr,
            } => self.expr(expr, scope),
            E::IsNull(value) => self.null_test(value, r::Unary::IsNull, scope, span()),
            E::IsNotNull(value) => self.null_test(value, r::Unary::IsNotNull, scope, span()),
            E::IsTrue(value) => self.truth(value, true, false, scope, span()),
            E::IsNotTrue(value) => self.truth(value, true, true, scope, span()),
            E::IsFalse(value) => self.truth(value, false, false, scope, span()),
            E::IsNotFalse(value) => self.truth(value, false, true, scope, span()),
            E::IsDistinctFrom(left, right) => self.distinct_from(left, right, true, scope, span()),
            E::IsNotDistinctFrom(left, right) => {
                self.distinct_from(left, right, false, scope, span())
            }
            E::InList {
                expr,
                list,
                negated,
            } => {
                let span = span();
                let value = self.logical(expr, scope)?;
                let list = match list.as_slice() {
                    // `IN (?)` binds a list parameter, as in neo4j-jdbc.
                    [single @ E::Value(ast::ValueWithSpan {
                        value: ast::Value::Placeholder(_),
                        ..
                    })] => self.expr(single, scope)?,
                    items => node(
                        ExprKind::List(
                            items
                                .iter()
                                .map(|item| self.expr(item, scope))
                                .collect::<Result<_>>()?,
                        ),
                        span,
                    )?,
                };
                negate(binary(r::Binary::In, value, list, span)?, *negated, span)
            }
            E::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let span = span();
                let value = self.logical(expr, scope)?;
                let low = self.expr(low, scope)?;
                let high = self.expr(high, scope)?;
                let range = connective(
                    r::Connective::And,
                    vec![
                        binary(r::Binary::LessEqual, low, value.clone(), span)?,
                        binary(r::Binary::LessEqual, value, high, span)?,
                    ],
                    span,
                )?;
                negate(range, *negated, span)
            }
            E::Like {
                negated,
                any: false,
                expr,
                pattern,
                escape_char: None,
            } => self.like(expr, pattern, false, *negated, scope, span()),
            E::ILike {
                negated,
                any: false,
                expr,
                pattern,
                escape_char: None,
            } => self.like(expr, pattern, true, *negated, scope, span()),
            E::Cast {
                kind: _,
                expr,
                data_type,
                format: None,
            } => {
                let span = span();
                let function = cast_function(data_type)
                    .ok_or_else(|| unsupported(format!("CAST to `{data_type}`"), span))?;
                call(function, vec![self.expr(expr, scope)?], span)
            }
            E::Function(function) => self.function(function, scope, span()),
            E::Case {
                operand,
                conditions,
                else_result,
                ..
            } => node(
                ExprKind::Case {
                    operand: operand
                        .as_deref()
                        .map(|operand| self.expr(operand, scope).map(Box::new))
                        .transpose()?,
                    branches: conditions
                        .iter()
                        .map(|when| {
                            Ok((
                                self.expr(&when.condition, scope)?,
                                self.expr(&when.result, scope)?,
                            ))
                        })
                        .collect::<Result<_>>()?,
                    otherwise: else_result
                        .as_deref()
                        .map(|otherwise| self.expr(otherwise, scope).map(Box::new))
                        .transpose()?,
                },
                span(),
            ),
            E::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let span = span();
                if substring_from.is_none() && substring_for.is_none() {
                    return Err(invalid(
                        detail::INVALID_NUMBER_OF_ARGUMENTS,
                        "SUBSTRING needs a start or a length",
                        span,
                    ));
                }
                let value = self.expr(expr, scope)?;
                let start = substring_from
                    .as_deref()
                    .map(|start| self.expr(start, scope))
                    .transpose()?;
                let length = substring_for
                    .as_deref()
                    .map(|length| self.expr(length, scope))
                    .transpose()?;
                substring(value, start, length, span)
            }
            // Cypher trims whitespace, so the only characters to trim are spaces.
            E::Trim {
                trim_where,
                trim_what,
                expr,
                trim_characters: None,
            } if trim_what.as_deref().is_none_or(|what| {
                matches!(what, E::Value(ast::ValueWithSpan {
                    value: ast::Value::SingleQuotedString(text),
                    ..
                }) if text == " ")
            }) =>
            {
                let function = match trim_where {
                    None | Some(ast::TrimWhereField::Both) => "trim",
                    Some(ast::TrimWhereField::Leading) => "ltrim",
                    Some(ast::TrimWhereField::Trailing) => "rtrim",
                };
                call(function, vec![self.expr(expr, scope)?], span())
            }
            E::Ceil { expr, field } | E::Floor { expr, field } => {
                let ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime) = field
                else {
                    return Err(unsupported("CEIL and FLOOR with a scale or unit", span()));
                };
                let name = if matches!(expression, E::Ceil { .. }) {
                    "ceil"
                } else {
                    "floor"
                };
                call(name, vec![self.expr(expr, scope)?], span())
            }
            E::Array(array) => node(
                ExprKind::List(
                    array
                        .elem
                        .iter()
                        .map(|item| self.expr(item, scope))
                        .collect::<Result<_>>()?,
                ),
                span(),
            ),
            E::Exists { .. } | E::Subquery(_) | E::InSubquery { .. } => Err(QueryError::compile(
                category::UNSUPPORTED_FEATURE,
                detail::PROCEDURES_AND_SUBQUERIES,
                "SQL translation does not support subqueries",
            )
            .at(span())),
            E::Tuple(_) => Err(unsupported(
                "row values outside comparisons and null tests",
                span(),
            )),
            other => Err(unsupported(
                format!("the expression `{}`", self.excerpt(span())),
                self.at(other),
            )),
        }
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "sqlparser's operator set is open-ended; every other operator is unsupported"
    )]
    fn binary_op(
        &self,
        whole: &ast::Expr,
        left: &ast::Expr,
        op: &ast::BinaryOperator,
        right: &ast::Expr,
        scope: &Scope,
    ) -> Result<Expr> {
        use ast::BinaryOperator as O;
        use r::Binary as B;
        let operator = match op {
            O::And => return self.connective_chain(whole, op, r::Connective::And, scope),
            O::Or => return self.connective_chain(whole, op, r::Connective::Or, scope),
            O::Xor => return self.connective_chain(whole, op, r::Connective::Xor, scope),
            O::Plus | O::StringConcat => B::Add,
            O::Minus => B::Subtract,
            O::Multiply => B::Multiply,
            O::Divide => B::Divide,
            O::Modulo => B::Modulo,
            O::Eq => B::Equal,
            O::NotEq => B::NotEqual,
            O::Lt => B::Less,
            O::LtEq => B::LessEqual,
            O::Gt => B::Greater,
            O::GtEq => B::GreaterEqual,
            other => {
                return Err(unsupported(
                    format!("the operator `{other}`"),
                    self.at(whole),
                ));
            }
        };
        let span = self.at(whole);
        match (left, right) {
            (ast::Expr::Tuple(left), ast::Expr::Tuple(right)) => {
                self.row_comparison(operator, left, right, scope, span)
            }
            _ => binary(
                operator,
                self.logical(left, scope)?,
                self.logical(right, scope)?,
                span,
            ),
        }
    }

    /// A run of one connective as one flat node, like Cypher's parser builds,
    /// collected without recursing down the chain.
    fn connective_chain(
        &self,
        whole: &ast::Expr,
        op: &ast::BinaryOperator,
        kind: r::Connective,
        scope: &Scope,
    ) -> Result<Expr> {
        let mut rights = Vec::new();
        let mut current = whole;
        while let ast::Expr::BinaryOp {
            left,
            op: next,
            right,
        } = current
            && next == op
        {
            rights.push(right.as_ref());
            current = left;
        }
        let operands = std::iter::once(current)
            .chain(rights.into_iter().rev())
            .map(|operand| match kind {
                r::Connective::And => self.logical(operand, scope),
                r::Connective::Or | r::Connective::Xor => self.expr(operand, scope),
            })
            .collect::<Result<Vec<_>>>()?;
        let span = Span {
            start: operands.first().map_or(0, |o| o.span.start),
            end: operands.last().map_or(0, |o| o.span.end),
        };
        connective(kind, operands, span)
    }

    /// `(a1, a2) op (b1, b2)` compared lexicographically, as SQL does.
    fn row_comparison(
        &self,
        operator: r::Binary,
        left: &[ast::Expr],
        right: &[ast::Expr],
        scope: &Scope,
        span: Span,
    ) -> Result<Expr> {
        use r::Binary as B;
        if left.len() != right.len() || left.is_empty() {
            return Err(invalid(
                detail::INVALID_STATEMENT,
                "compared row values need the same number of fields",
                span,
            ));
        }
        let pairs = left
            .iter()
            .zip(right)
            .map(|(a, b)| Ok((self.expr(a, scope)?, self.expr(b, scope)?)))
            .collect::<Result<Vec<_>>>()?;
        let each = |operator| {
            pairs
                .iter()
                .map(|(a, b)| binary(operator, a.clone(), b.clone(), span))
                .collect::<Result<Vec<_>>>()
        };
        match operator {
            B::Equal => connective(r::Connective::And, each(B::Equal)?, span),
            B::NotEqual => connective(r::Connective::Or, each(B::NotEqual)?, span),
            B::Less | B::LessEqual | B::Greater | B::GreaterEqual => {
                let strict = if matches!(operator, B::Less | B::LessEqual) {
                    B::Less
                } else {
                    B::Greater
                };
                // Built from the last field outward: a1 < b1 OR (a1 = b1 AND rest).
                let mut fields = pairs.iter().rev();
                let (a, b) = fields.next().expect("rows are nonempty");
                fields.try_fold(
                    binary(operator, a.clone(), b.clone(), span)?,
                    |rest, (a, b)| {
                        let equal = binary(B::Equal, a.clone(), b.clone(), span)?;
                        connective(
                            r::Connective::Or,
                            vec![
                                binary(strict, a.clone(), b.clone(), span)?,
                                connective(r::Connective::And, vec![equal, rest], span)?,
                            ],
                            span,
                        )
                    },
                )
            }
            B::Add
            | B::Subtract
            | B::Multiply
            | B::Divide
            | B::Modulo
            | B::Power
            | B::In
            | B::StartsWith
            | B::EndsWith
            | B::Contains => Err(unsupported("arithmetic on row values", span)),
        }
    }

    fn null_test(
        &self,
        value: &ast::Expr,
        test: r::Unary,
        scope: &Scope,
        span: Span,
    ) -> Result<Expr> {
        let ast::Expr::Tuple(fields) = value else {
            return node(
                ExprKind::Unary(test, Box::new(self.logical(value, scope)?)),
                span,
            );
        };
        // A row is null when every field is, and not null when every field is not.
        let tests = fields
            .iter()
            .map(|field| {
                node(
                    ExprKind::Unary(test, Box::new(self.expr(field, scope)?)),
                    span,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        connective(r::Connective::And, tests, span)
    }

    /// `IS [NOT] TRUE` and `IS [NOT] FALSE` are never null.
    fn truth(
        &self,
        value: &ast::Expr,
        truth: bool,
        negated: bool,
        scope: &Scope,
        span: Span,
    ) -> Result<Expr> {
        let test = binary(
            r::Binary::Equal,
            self.logical(value, scope)?,
            literal(r::Value::Boolean(truth), span)?,
            span,
        )?;
        let test = call(
            "coalesce",
            vec![test, literal(r::Value::Boolean(false), span)?],
            span,
        )?;
        negate(test, negated, span)
    }

    /// `a IS [NOT] DISTINCT FROM b` treats two nulls as equal.
    fn distinct_from(
        &self,
        left: &ast::Expr,
        right: &ast::Expr,
        distinct: bool,
        scope: &Scope,
        span: Span,
    ) -> Result<Expr> {
        let left = self.expr(left, scope)?;
        let right = self.expr(right, scope)?;
        let null = |value: &Expr| {
            node(
                ExprKind::Unary(r::Unary::IsNull, Box::new(value.clone())),
                span,
            )
        };
        let same = call(
            "coalesce",
            vec![
                binary(r::Binary::Equal, left.clone(), right.clone(), span)?,
                connective(r::Connective::And, vec![null(&left)?, null(&right)?], span)?,
            ],
            span,
        )?;
        negate(same, distinct, span)
    }

    /// `LIKE` with a literal pattern of at most a leading and a trailing `%`.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "only plain string literals are LIKE patterns"
    )]
    fn like(
        &self,
        value: &ast::Expr,
        pattern: &ast::Expr,
        case_insensitive: bool,
        negated: bool,
        scope: &Scope,
        span: Span,
    ) -> Result<Expr> {
        let text = match pattern {
            ast::Expr::Value(ast::ValueWithSpan {
                value:
                    ast::Value::SingleQuotedString(text)
                    | ast::Value::EscapedStringLiteral(text)
                    | ast::Value::NationalStringLiteral(text),
                ..
            }) => text,
            _ => {
                return Err(unsupported(
                    "LIKE with a pattern that is not a string literal",
                    span,
                ))
            }
        };
        let mut value = self.logical(value, scope)?;
        let text = if case_insensitive {
            value = call("toLower", vec![value], span)?;
            text.to_lowercase()
        } else {
            text.clone()
        };
        let leading = text.starts_with('%');
        let inner = text.strip_prefix('%').unwrap_or(&text);
        let trailing = inner.ends_with('%');
        let inner = inner.strip_suffix('%').unwrap_or(inner);
        if inner.contains(['%', '_', '\\']) {
            return Err(unsupported(
                "LIKE patterns with `_`, escapes, or `%` other than at the start and end",
                span,
            ));
        }
        let operator = match (leading, trailing) {
            (true, true) => r::Binary::Contains,
            (true, false) => r::Binary::EndsWith,
            (false, true) => r::Binary::StartsWith,
            (false, false) => r::Binary::Equal,
        };
        let pattern = literal(r::Value::String(inner.to_owned()), span)?;
        negate(binary(operator, value, pattern, span)?, negated, span)
    }

    /// A SQL function call as its Cypher equivalent. Functions without a SQL
    /// spelling of their own keep their name, so the binder reports any that
    /// HelixDB's Cypher profile lacks.
    fn function(&self, function: &ast::Function, scope: &Scope, span: Span) -> Result<Expr> {
        if !matches!(function.parameters, ast::FunctionArguments::None)
            || function.filter.is_some()
            || function.null_treatment.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
        {
            return Err(unsupported(
                "window functions, FILTER, WITHIN GROUP and parameterized functions",
                span,
            ));
        }
        let [ast::ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
            return Err(QueryError::compile(
                category::UNSUPPORTED_FEATURE,
                detail::NAMESPACED_FUNCTION,
                "SQL translation does not support qualified function names",
            )
            .at(span));
        };
        let (arguments, distinct, star) = match &function.args {
            ast::FunctionArguments::None => (Vec::new(), false, false),
            ast::FunctionArguments::Subquery(_) => {
                return Err(QueryError::compile(
                    category::UNSUPPORTED_FEATURE,
                    detail::PROCEDURES_AND_SUBQUERIES,
                    "SQL translation does not support subqueries",
                )
                .at(span));
            }
            ast::FunctionArguments::List(list) => {
                if !list.clauses.is_empty() {
                    return Err(unsupported("clauses inside function arguments", span));
                }
                let distinct = matches!(
                    list.duplicate_treatment,
                    Some(ast::DuplicateTreatment::Distinct)
                );
                match list.args.as_slice() {
                    [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)] => {
                        (Vec::new(), distinct, true)
                    }
                    args => (
                        args.iter()
                            .map(|argument| match argument {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(value)) => {
                                    self.expr(value, scope)
                                }
                                ast::FunctionArg::Unnamed(
                                    ast::FunctionArgExpr::Wildcard
                                    | ast::FunctionArgExpr::WildcardWithOptions(_)
                                    | ast::FunctionArgExpr::QualifiedWildcard(_),
                                )
                                | ast::FunctionArg::Named { .. }
                                | ast::FunctionArg::ExprNamed { .. } => Err(unsupported(
                                    "named or wildcard function arguments",
                                    self.at(argument),
                                )),
                            })
                            .collect::<Result<Vec<_>>>()?,
                        distinct,
                        false,
                    ),
                }
            }
        };
        let lower = name.value.to_lowercase();
        let arity = |count: usize| {
            if arguments.len() == count {
                Ok(())
            } else {
                Err(invalid(
                    detail::INVALID_NUMBER_OF_ARGUMENTS,
                    format!("{} takes {count} arguments", name.value),
                    span,
                ))
            }
        };
        let renamed = match lower.as_str() {
            "lower" | "lcase" => "toLower",
            "upper" | "ucase" => "toUpper",
            "length" | "char_length" | "character_length" | "len" => "size",
            "nvl" | "ifnull" => "coalesce",
            "ln" => "log",
            "ceiling" => "ceil",
            "random" => "rand",
            "stddev" | "stddev_samp" => "stDev",
            "stddev_pop" => "stDevP",
            "array_agg" => "collect",
            "elementid" => "id",
            "current_timestamp" | "now" => "localdatetime",
            "current_date" => "date",
            "current_time" => "localtime",
            "concat" => {
                let mut parts = arguments.into_iter().rev();
                let last = parts.next().ok_or_else(|| {
                    invalid(
                        detail::INVALID_NUMBER_OF_ARGUMENTS,
                        "concat takes at least one argument",
                        span,
                    )
                })?;
                return parts.try_fold(last, |rest, part| binary(r::Binary::Add, part, rest, span));
            }
            "substring" | "substr" => {
                let mut parts = arguments.into_iter();
                let (Some(value), start, length, None) =
                    (parts.next(), parts.next(), parts.next(), parts.next())
                else {
                    return Err(invalid(
                        detail::INVALID_NUMBER_OF_ARGUMENTS,
                        "substring takes two or three arguments",
                        span,
                    ));
                };
                return substring(value, start, length, span);
            }
            "replace" if arguments.len() == 2 => {
                let mut arguments = arguments;
                arguments.push(literal(r::Value::String(String::new()), span)?);
                return call("replace", arguments, span);
            }
            "nullif" => {
                arity(2)?;
                let [value, other]: [Expr; 2] = arguments.try_into().expect("two arguments");
                return node(
                    ExprKind::Case {
                        operand: None,
                        branches: vec![(
                            binary(r::Binary::Equal, value.clone(), other, span)?,
                            literal(r::Value::Null, span)?,
                        )],
                        otherwise: Some(Box::new(value)),
                    },
                    span,
                );
            }
            "nvl2" => {
                arity(3)?;
                let [value, present, absent]: [Expr; 3] =
                    arguments.try_into().expect("three arguments");
                return node(
                    ExprKind::Case {
                        operand: None,
                        branches: vec![(
                            node(ExprKind::Unary(r::Unary::IsNotNull, Box::new(value)), span)?,
                            present,
                        )],
                        otherwise: Some(Box::new(absent)),
                    },
                    span,
                );
            }
            "square" => {
                arity(1)?;
                let value = arguments.into_iter().next().expect("one argument");
                return binary(r::Binary::Multiply, value.clone(), value, span);
            }
            "power" | "pow" => {
                arity(2)?;
                let [base, exponent]: [Expr; 2] = arguments.try_into().expect("two arguments");
                return binary(r::Binary::Power, base, exponent, span);
            }
            "mod" => {
                arity(2)?;
                let [value, divisor]: [Expr; 2] = arguments.try_into().expect("two arguments");
                return binary(r::Binary::Modulo, value, divisor, span);
            }
            "log" if arguments.len() == 2 => {
                let [base, value]: [Expr; 2] = arguments.try_into().expect("two arguments");
                return binary(
                    r::Binary::Divide,
                    call("log", vec![value], span)?,
                    call("log", vec![base], span)?,
                    span,
                );
            }
            _ => name.value.as_str(),
        };
        let renamed = CYPHER_FUNCTIONS
            .iter()
            .find(|known| known.eq_ignore_ascii_case(renamed))
            .copied()
            .unwrap_or(renamed);
        node(
            ExprKind::Call {
                name: renamed.to_owned(),
                arguments,
                distinct,
                star,
            },
            span,
        )
    }

    /// A literal or placeholder. `negative` folds a leading minus into a
    /// number, so the smallest integer stays representable.
    fn value(&self, value: &ast::ValueWithSpan, negative: bool, span: Span) -> Result<Expr> {
        use ast::Value as V;
        let constant = match &value.value {
            V::Number(text, _) => number(text, negative, span)?,
            _ if negative => {
                let positive = self.value(value, false, span)?;
                return node(ExprKind::Unary(r::Unary::Negate, Box::new(positive)), span);
            }
            V::SingleQuotedString(text)
            | V::DoubleQuotedString(text)
            | V::EscapedStringLiteral(text)
            | V::UnicodeStringLiteral(text)
            | V::NationalStringLiteral(text)
            | V::TripleSingleQuotedString(text)
            | V::TripleDoubleQuotedString(text) => r::Value::String(text.clone()),
            V::DollarQuotedString(text) => r::Value::String(text.value.clone()),
            V::Boolean(value) => r::Value::Boolean(*value),
            V::Null => r::Value::Null,
            V::Placeholder(placeholder) => {
                return node(
                    ExprKind::Parameter(self.parameter(placeholder, value.span, span)?),
                    span,
                );
            }
            V::SingleQuotedByteStringLiteral(_)
            | V::DoubleQuotedByteStringLiteral(_)
            | V::TripleSingleQuotedByteStringLiteral(_)
            | V::TripleDoubleQuotedByteStringLiteral(_)
            | V::SingleQuotedRawStringLiteral(_)
            | V::DoubleQuotedRawStringLiteral(_)
            | V::TripleSingleQuotedRawStringLiteral(_)
            | V::TripleDoubleQuotedRawStringLiteral(_)
            | V::QuoteDelimitedStringLiteral(_)
            | V::NationalQuoteDelimitedStringLiteral(_)
            | V::HexStringLiteral(_) => {
                return Err(unsupported(
                    "byte, raw and hexadecimal string literals",
                    span,
                ));
            }
        };
        literal(constant, span)
    }

    /// `?` is numbered by its position in the text; `$n`, `:name`, `@name`
    /// and `?n` keep their name.
    fn parameter(
        &self,
        placeholder: &str,
        location: sqlparser::tokenizer::Span,
        span: Span,
    ) -> Result<String> {
        if placeholder == "?" {
            let offset = self.lines.offset(location.start);
            return offset
                .and_then(|offset| self.positional.binary_search(&offset).ok())
                .map(|index| (index + 1).to_string())
                .ok_or_else(|| {
                    invalid(
                        detail::INVALID_PARAMETER,
                        "could not number this `?` placeholder",
                        span,
                    )
                });
        }
        placeholder
            .strip_prefix(['$', ':', '@', '?'])
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                invalid(
                    detail::INVALID_PARAMETER,
                    format!("`{placeholder}` is not a parameter name"),
                    span,
                )
            })
    }

    /// Source text for an error message, shortened.
    pub(super) fn excerpt(&self, span: Span) -> String {
        let text = self
            .lines
            .text
            .get(span.start..span.end)
            .unwrap_or_default();
        let mut excerpt: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if excerpt.chars().count() > 60 {
            excerpt = excerpt.chars().take(57).chain("...".chars()).collect();
        }
        excerpt
    }
}

/// Cypher functions as Cypher spells them, so `COUNT(*)` becomes `count(*)`.
/// Other names pass through as written.
const CYPHER_FUNCTIONS: &[&str] = &[
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "avg",
    "ceil",
    "coalesce",
    "collect",
    "cos",
    "cot",
    "count",
    "date",
    "datetime",
    "degrees",
    "exists",
    "exp",
    "floor",
    "head",
    "id",
    "keys",
    "labels",
    "last",
    "left",
    "length",
    "localdatetime",
    "localtime",
    "log",
    "log10",
    "max",
    "min",
    "nodes",
    "percentileCont",
    "percentileDisc",
    "pi",
    "properties",
    "radians",
    "rand",
    "range",
    "relationships",
    "replace",
    "reverse",
    "right",
    "round",
    "rtrim",
    "ltrim",
    "sign",
    "sin",
    "size",
    "split",
    "sqrt",
    "stDev",
    "stDevP",
    "substring",
    "sum",
    "tan",
    "time",
    "timestamp",
    "toBoolean",
    "toFloat",
    "toInteger",
    "toLower",
    "toString",
    "toUpper",
    "trim",
    "type",
];

/// SQL's 1-based `SUBSTRING(value FROM start FOR length)` on Cypher's 0-based
/// `substring`.
fn substring(value: Expr, start: Option<Expr>, length: Option<Expr>, span: Span) -> Result<Expr> {
    let start = match start {
        None => literal(r::Value::Integer(0), span)?,
        Some(start) => {
            if let ExprKind::Literal(r::Value::Integer(position)) = start.kind()
                && let Some(previous) = position.checked_sub(1)
            {
                literal(r::Value::Integer(previous), span)?
            } else {
                binary(
                    r::Binary::Subtract,
                    start,
                    literal(r::Value::Integer(1), span)?,
                    span,
                )?
            }
        }
    };
    call(
        "substring",
        std::iter::once(value)
            .chain(std::iter::once(start))
            .chain(length)
            .collect(),
        span,
    )
}

/// The Cypher conversion a SQL type casts with.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "sqlparser's type set is open-ended; every other type has no Cypher conversion"
)]
fn cast_function(data_type: &ast::DataType) -> Option<&'static str> {
    use ast::DataType as T;
    Some(match data_type {
        T::Character(_)
        | T::Char(_)
        | T::CharacterVarying(_)
        | T::CharVarying(_)
        | T::Varchar(_)
        | T::Nvarchar(_)
        | T::Text
        | T::String(_)
        | T::Clob(_)
        | T::CharacterLargeObject(_)
        | T::CharLargeObject(_)
        | T::TinyText
        | T::MediumText
        | T::LongText => "toString",
        T::TinyInt(_)
        | T::SmallInt(_)
        | T::MediumInt(_)
        | T::Int(_)
        | T::Integer(_)
        | T::BigInt(_)
        | T::Int2(_)
        | T::Int4(_)
        | T::Int8(_)
        | T::Int16
        | T::Int32
        | T::Int64
        | T::Signed
        | T::SignedInteger => "toInteger",
        T::Float(_)
        | T::Real
        | T::Double(_)
        | T::DoublePrecision
        | T::Float4
        | T::Float8
        | T::Float32
        | T::Float64
        | T::Decimal(_)
        | T::Numeric(_)
        | T::Dec(_) => "toFloat",
        T::Bool | T::Boolean => "toBoolean",
        _ => return None,
    })
}

fn number(text: &str, negative: bool, span: Span) -> Result<r::Value> {
    let error = |detail: &str| {
        QueryError::compile(
            category::SYNTAX_ERROR,
            detail,
            "invalid or out-of-range numeric literal",
        )
        .at(span)
    };
    if text.contains(['.', 'e', 'E']) {
        let value = text
            .parse::<f64>()
            .map_err(|_| error(detail::INVALID_NUMBER_LITERAL))?;
        if !value.is_finite() {
            return Err(error(detail::FLOATING_POINT_OVERFLOW));
        }
        return Ok(r::Value::Float(if negative { -value } else { value }));
    }
    let magnitude = text
        .parse::<u64>()
        .map_err(|_| error(detail::INTEGER_OVERFLOW))?;
    let value = if negative {
        0_i64.checked_sub_unsigned(magnitude)
    } else {
        i64::try_from(magnitude).ok()
    };
    value
        .map(r::Value::Integer)
        .ok_or_else(|| error(detail::INTEGER_OVERFLOW))
}

fn negate(value: Expr, negated: bool, span: Span) -> Result<Expr> {
    if negated {
        node(ExprKind::Unary(r::Unary::Not, Box::new(value)), span)
    } else {
        Ok(value)
    }
}

/// One connective over `operands`, or the operand itself when there is one.
pub(super) fn connective(kind: r::Connective, operands: Vec<Expr>, span: Span) -> Result<Expr> {
    let mut operands = operands.into_iter();
    match (operands.next(), operands.next()) {
        (Some(single), None) => Ok(single),
        (first, second) => node(
            ExprKind::Connective(
                kind,
                AtLeast::try_from_vec(first.into_iter().chain(second).chain(operands).collect())
                    .expect("a connective has two operands"),
            ),
            span,
        ),
    }
}

/// Visit `expression` and every subexpression, parents first.
pub(super) fn visit(expression: &Expr, f: &mut dyn FnMut(&Expr)) {
    f(expression);
    children(expression)
        .into_iter()
        .for_each(|child| visit(child, f));
}

fn children(expression: &Expr) -> Vec<&Expr> {
    match expression.kind() {
        ExprKind::Literal(_)
        | ExprKind::Variable(_)
        | ExprKind::Parameter(_)
        | ExprKind::PatternPredicate(_) => Vec::new(),
        ExprKind::Property(value, _) | ExprKind::Unary(_, value) | ExprKind::HasLabel(value, _) => {
            vec![value]
        }
        ExprKind::Index(value, index) => vec![value, index],
        ExprKind::Binary(_, left, right) => vec![left, right],
        ExprKind::Slice { value, start, end } => std::iter::once(value.as_ref())
            .chain(start.as_deref())
            .chain(end.as_deref())
            .collect(),
        ExprKind::Connective(_, operands) => operands.iter().collect(),
        ExprKind::Call { arguments, .. } | ExprKind::List(arguments) => arguments.iter().collect(),
        ExprKind::Map(entries) => entries.iter().map(|(_, value)| value).collect(),
        ExprKind::Case {
            operand,
            branches,
            otherwise,
        } => operand
            .as_deref()
            .into_iter()
            .chain(branches.iter().flat_map(|(when, then)| [when, then]))
            .chain(otherwise.as_deref())
            .collect(),
    }
}

/// Rebuild `expression`, replacing every subtree for which `replace` returns
/// a value; the others keep their shape with rebuilt children.
pub(super) fn rewrite(
    expression: &Expr,
    replace: &mut dyn FnMut(&Expr) -> Result<Option<Expr>>,
) -> Result<Expr> {
    if let Some(replacement) = replace(expression)? {
        return Ok(replacement);
    }
    let mut again = |value: &Expr| rewrite(value, replace);
    let kind = match expression.kind() {
        kind @ (ExprKind::Literal(_)
        | ExprKind::Variable(_)
        | ExprKind::Parameter(_)
        | ExprKind::PatternPredicate(_)) => kind.clone(),
        ExprKind::Property(value, key) => ExprKind::Property(Box::new(again(value)?), key.clone()),
        ExprKind::HasLabel(value, label) => {
            ExprKind::HasLabel(Box::new(again(value)?), label.clone())
        }
        ExprKind::Unary(operator, value) => ExprKind::Unary(*operator, Box::new(again(value)?)),
        ExprKind::Index(value, index) => {
            ExprKind::Index(Box::new(again(value)?), Box::new(again(index)?))
        }
        ExprKind::Binary(operator, left, right) => {
            ExprKind::Binary(*operator, Box::new(again(left)?), Box::new(again(right)?))
        }
        ExprKind::Slice { value, start, end } => ExprKind::Slice {
            value: Box::new(again(value)?),
            start: start.as_deref().map(&mut again).transpose()?.map(Box::new),
            end: end.as_deref().map(&mut again).transpose()?.map(Box::new),
        },
        ExprKind::Connective(connective, operands) => ExprKind::Connective(
            *connective,
            AtLeast::try_from_vec(operands.iter().map(&mut again).collect::<Result<_>>()?)
                .expect("a rebuilt connective keeps its operands"),
        ),
        ExprKind::Call {
            name,
            arguments,
            distinct,
            star,
        } => ExprKind::Call {
            name: name.clone(),
            arguments: arguments.iter().map(&mut again).collect::<Result<_>>()?,
            distinct: *distinct,
            star: *star,
        },
        ExprKind::List(values) => {
            ExprKind::List(values.iter().map(&mut again).collect::<Result<_>>()?)
        }
        ExprKind::Map(entries) => ExprKind::Map(
            entries
                .iter()
                .map(|(key, value)| Ok((key.clone(), again(value)?)))
                .collect::<Result<_>>()?,
        ),
        ExprKind::Case {
            operand,
            branches,
            otherwise,
        } => ExprKind::Case {
            operand: operand
                .as_deref()
                .map(&mut again)
                .transpose()?
                .map(Box::new),
            branches: branches
                .iter()
                .map(|(when, then)| Ok((again(when)?, again(then)?)))
                .collect::<Result<_>>()?,
            otherwise: otherwise
                .as_deref()
                .map(&mut again)
                .transpose()?
                .map(Box::new),
        },
    };
    node(kind, expression.span)
}
