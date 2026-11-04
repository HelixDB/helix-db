//! `SELECT`: FROM and WHERE become `MATCH`, the select list becomes `RETURN`,
//! and grouping goes through `WITH` when Cypher's implicit grouping would
//! group differently from SQL.
use super::expr::{rewrite, visit};
use super::tables::{is_table_variable, reads_table, Scope};
use super::{invalid, item, unsupported, variable, Translator};
use crate::syntax::{self, Clause, Expr, ExprKind};
use helix_planner::relational::{self as r, category, detail, QueryError, Result, Span};
use sqlparser::ast;
use std::collections::{HashMap, HashSet};

/// Aggregate functions, lowercased, including those the binder rejects, so
/// that grouping is decided before the binder reports them.
const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "collect",
    "stdev",
    "stdevp",
    "percentilecont",
    "percentiledisc",
];

/// One select-list entry after translation.
struct Projected {
    /// `None` for `*`.
    expression: Option<Expr>,
    /// The result column, made unique unless the SQL wrote it.
    name: String,
    alias: Alias,
}

/// Where a select-list entry's column name comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alias {
    /// Written as `AS name`; printed, and never renamed.
    Explicit,
    /// An unqualified column's name or the table of `t.*`; printed.
    Derived,
    /// The entry's SQL text, not printed: the item's text names the column.
    Text,
}

/// The entries of a grouping `WITH`, findable by expression.
#[derive(Default)]
struct With {
    items: Vec<(Expr, String)>,
    names: HashMap<String, String>,
}

impl With {
    fn push(&mut self, expression: Expr, name: String) {
        self.names
            .entry(expression.to_string())
            .or_insert_with(|| name.clone());
        self.items.push((expression, name));
    }

    /// The name of the first entry that computes `expression`.
    fn name(&self, expression: &Expr) -> Option<&str> {
        self.names.get(&expression.to_string()).map(String::as_str)
    }
}

/// The result window of a query: `OFFSET`/`SKIP` and `LIMIT`/`FETCH`/`TOP`.
#[derive(Default)]
struct Window {
    skip: Option<Expr>,
    limit: Option<Expr>,
}

impl Translator<'_> {
    pub(super) fn query(&self, query: &ast::Query) -> Result<syntax::Statement> {
        let span = self.at(query);
        let ast::Query {
            with,
            body,
            order_by,
            limit_clause,
            fetch,
            locks,
            for_clause,
            settings,
            format_clause,
            pipe_operators,
        } = query;
        if with.is_some() {
            return Err(unsupported("common table expressions (WITH)", span));
        }
        if !locks.is_empty()
            || for_clause.is_some()
            || settings.is_some()
            || format_clause.is_some()
            || !pipe_operators.is_empty()
        {
            return Err(unsupported(
                "locking, FOR, SETTINGS, FORMAT and pipe clauses",
                span,
            ));
        }
        let select = match body.as_ref() {
            ast::SetExpr::Select(select) => select,
            ast::SetExpr::SetOperation { .. } => {
                return Err(QueryError::compile(
                    category::UNSUPPORTED_FEATURE,
                    detail::UNION,
                    "SQL translation does not support UNION, INTERSECT or EXCEPT",
                )
                .at(span));
            }
            ast::SetExpr::Query(_)
            | ast::SetExpr::Values(_)
            | ast::SetExpr::Insert(_)
            | ast::SetExpr::Update(_)
            | ast::SetExpr::Delete(_)
            | ast::SetExpr::Merge(_)
            | ast::SetExpr::Table(_) => {
                return Err(unsupported("this query form; write a single SELECT", span));
            }
        };
        let window = self.window(
            limit_clause.as_ref(),
            fetch.as_ref(),
            select.top.as_ref(),
            span,
        )?;
        self.select(select, order_by.as_ref(), window)
    }

    fn window(
        &self,
        limit: Option<&ast::LimitClause>,
        fetch: Option<&ast::Fetch>,
        top: Option<&ast::Top>,
        span: Span,
    ) -> Result<Window> {
        let empty = Scope::default();
        let constant = |value: &ast::Expr| self.expr(value, &empty);
        let mut window = match limit {
            None => Window::default(),
            Some(ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(unsupported("LIMIT BY", span));
                }
                Window {
                    skip: offset.as_ref().map(|o| constant(&o.value)).transpose()?,
                    limit: limit.as_ref().map(constant).transpose()?,
                }
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => Window {
                skip: Some(constant(offset)?),
                limit: Some(constant(limit)?),
            },
        };
        let extra = match (fetch, top) {
            (None, None) => None,
            (Some(_), Some(_)) => return Err(unsupported("both FETCH and TOP", span)),
            (
                Some(ast::Fetch {
                    with_ties: false,
                    percent: false,
                    quantity: Some(quantity),
                }),
                None,
            ) => Some(constant(quantity)?),
            (
                None,
                Some(ast::Top {
                    with_ties: false,
                    percent: false,
                    quantity: Some(quantity),
                }),
            ) => Some(match quantity {
                ast::TopQuantity::Expr(quantity) => constant(quantity)?,
                ast::TopQuantity::Constant(quantity) => super::literal(
                    r::Value::Integer(i64::try_from(*quantity).map_err(|_| {
                        invalid(
                            detail::INTEGER_OVERFLOW,
                            "TOP exceeds the integer range",
                            span,
                        )
                    })?),
                    span,
                )?,
            }),
            (Some(_), None) | (None, Some(_)) => {
                return Err(unsupported(
                    "FETCH or TOP with ties, percent or no count",
                    span,
                ));
            }
        };
        match (extra, &window.limit) {
            (Some(_), Some(_)) => {
                return Err(unsupported("LIMIT together with FETCH or TOP", span));
            }
            (Some(extra), None) => window.limit = Some(extra),
            (None, _) => {}
        }
        Ok(window)
    }

    fn select(
        &self,
        select: &ast::Select,
        order_by: Option<&ast::OrderBy>,
        window: Window,
    ) -> Result<syntax::Statement> {
        let span = self.at(select);
        if !select.optimizer_hints.is_empty()
            || select.select_modifiers.is_some()
            || select.exclude.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || !select.named_window.is_empty()
            || select.qualify.is_some()
            || select.value_table_mode.is_some()
            || !matches!(select.flavor, ast::SelectFlavor::Standard)
        {
            return Err(unsupported(
                "dialect-specific SELECT clauses such as INTO, QUALIFY, WINDOW and CLUSTER BY",
                span,
            ));
        }
        if select.projection.is_empty() {
            return Err(invalid(
                detail::INVALID_STATEMENT,
                "SELECT needs at least one column",
                span,
            ));
        }
        let distinct = match &select.distinct {
            None | Some(ast::Distinct::All) => false,
            Some(ast::Distinct::Distinct) => true,
            Some(ast::Distinct::On(_)) => return Err(unsupported("DISTINCT ON", span)),
        };
        let (patterns, mut scope) = self.from(&select.from)?;
        let predicate = select
            .selection
            .as_ref()
            .map(|selection| self.expr(selection, &scope))
            .transpose()?;
        let projected = unique(
            select
                .projection
                .iter()
                .map(|entry| self.projected(entry, &scope))
                .collect::<Result<Vec<_>>>()?,
        );
        let explicit: Vec<(String, Expr)> = projected
            .iter()
            .filter(|p| p.alias == Alias::Explicit)
            .filter_map(|p| Some((p.name.clone(), p.expression.clone()?)))
            .collect();
        // GROUP BY reads an alias as its expression; HAVING and ORDER BY read
        // it as the projected column.
        scope.aliases = super::lowercased(explicit.clone());
        let keys = self.group_keys(&select.group_by, &projected, &scope, span)?;
        scope.aliases = super::lowercased(
            explicit
                .iter()
                .map(|(name, _)| Ok((name.clone(), variable(name, span)?)))
                .collect::<Result<Vec<_>>>()?,
        );
        let having = select
            .having
            .as_ref()
            .map(|having| self.expr(having, &scope))
            .transpose()?;
        let ordering = order_by
            .map(|order_by| self.ordering(order_by, &projected, &scope))
            .transpose()?
            .unwrap_or_default();
        let mut clauses = Vec::new();
        let patterns_empty = patterns.is_empty();
        if !patterns.is_empty() {
            clauses.push(Clause::Match {
                patterns,
                optional: false,
                predicate,
            });
        } else if predicate.is_some() {
            return Err(unsupported("WHERE without FROM", span));
        }
        let aggregating = !keys.is_empty()
            || having.is_some()
            || projected
                .iter()
                .filter_map(|p| p.expression.as_ref())
                .any(aggregates);
        if !aggregating {
            // Reading a column can fail, so Cypher only stops a scan at a
            // LIMIT whose projection passes variables along. Windowing the
            // table variables in a WITH, then reading columns in RETURN,
            // returns the same rows and lets the scan stop early.
            let windowed = (window.skip.is_some() || window.limit.is_some())
                && !distinct
                && !patterns_empty
                // An alias that shadows a table variable would change what
                // ORDER BY means outside RETURN.
                && projected
                    .iter()
                    .filter(|p| p.alias != Alias::Text)
                    .all(|p| !scope.has_variable(&p.name));
            if windowed {
                let aliased: HashMap<&str, &Expr> = projected
                    .iter()
                    .filter(|p| p.alias != Alias::Text)
                    .filter_map(|p| Some((p.name.as_str(), p.expression.as_ref()?)))
                    .collect();
                let ordering = ordering
                    .into_iter()
                    .map(|(order, descending)| {
                        // Before RETURN, an alias is its select-list expression.
                        let order = rewrite(&order, &mut |e| {
                            let ExprKind::Variable(name) = e.kind() else {
                                return Ok(None);
                            };
                            aliased
                                .get(name.as_str())
                                .map(|expression| self.copy(expression))
                                .transpose()
                        })?;
                        Ok((order, descending))
                    })
                    .collect::<Result<Vec<_>>>()?;
                clauses.push(Clause::Project {
                    returning: false,
                    items: scope
                        .variables()
                        .map(|name| Ok(item(variable(name, span)?, None)))
                        .collect::<Result<_>>()?,
                    distinct: false,
                    ordering,
                    skip: window.skip,
                    limit: window.limit,
                    predicate: None,
                });
                clauses.push(Clause::Project {
                    returning: true,
                    items: projected.into_iter().map(Projected::item).collect(),
                    distinct: false,
                    ordering: Vec::new(),
                    skip: None,
                    limit: None,
                    predicate: None,
                });
                return Ok(syntax::Statement { clauses });
            }
            clauses.push(Clause::Project {
                returning: true,
                items: projected.into_iter().map(Projected::item).collect(),
                distinct,
                ordering,
                skip: window.skip,
                limit: window.limit,
                predicate: None,
            });
            return Ok(syntax::Statement { clauses });
        }
        let expressions = projected
            .iter()
            .map(|p| {
                p.expression.as_ref().ok_or_else(|| {
                    invalid(
                        detail::INVALID_AGGREGATION,
                        "`*` cannot be selected together with grouping or aggregates",
                        span,
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Expressions compare by their printed text, which is canonical and
        // injective, through hash sets so that wide statements stay linear.
        let keys_printed: HashSet<String> = keys.iter().map(Expr::to_string).collect();
        let items_printed: Vec<String> = expressions.iter().map(|e| e.to_string()).collect();
        // Every column outside an aggregate must be grouped, as SQL requires.
        let grouped = |expression: &Expr| -> Result<bool> {
            let rest = rewrite(expression, &mut |e| {
                (aggregate_call(e) || keys_printed.contains(&e.to_string()))
                    .then(|| Expr::new(ExprKind::Literal(r::Value::Null), e.span))
                    .transpose()
            })?;
            Ok(!reads_table(&scope, &rest))
        };
        for (expression, p) in expressions.iter().zip(&projected) {
            if !grouped(expression)? {
                return Err(invalid(
                    detail::INVALID_AGGREGATION,
                    format!(
                        "`{}` must appear in GROUP BY or be used in an aggregate",
                        p.name
                    ),
                    expression.span,
                ));
            }
        }
        // Cypher groups only when a projection aggregates, so GROUP BY
        // without an aggregate deduplicates instead.
        let aggregated = expressions.iter().any(|e| aggregates(e))
            || having.as_ref().is_some_and(aggregates)
            || ordering.iter().any(|(order, _)| aggregates(order));
        // Without GROUP BY, SQL aggregates the whole input into one row even
        // when it is empty; Cypher would group by any constant it projects.
        let global = keys.is_empty();
        if global && !aggregated {
            return Err(invalid(
                detail::INVALID_AGGREGATION,
                "HAVING needs GROUP BY or an aggregate",
                span,
            ));
        }
        let plain: HashSet<&str> = expressions
            .iter()
            .zip(&items_printed)
            .filter(|(e, _)| !aggregates(e))
            .map(|(_, text)| text.as_str())
            .collect();
        let selected: HashSet<&str> = items_printed.iter().map(String::as_str).collect();
        let simple = having.is_none()
            && !(global && !plain.is_empty())
            && keys_printed.iter().all(|key| plain.contains(key.as_str()))
            && expressions.iter().zip(&items_printed).all(|(e, text)| {
                aggregates(e) || keys_printed.contains(text) || !reads_table(&scope, e)
            })
            && ordering.iter().all(|(order, _)| {
                matches!(order.kind(), ExprKind::Variable(_))
                    || selected.contains(order.to_string().as_str())
            });
        if simple {
            clauses.push(Clause::Project {
                returning: true,
                items: projected.into_iter().map(Projected::item).collect(),
                distinct: distinct || !aggregated,
                ordering,
                skip: window.skip,
                limit: window.limit,
                predicate: None,
            });
            return Ok(syntax::Statement { clauses });
        }
        // Explicit grouping: WITH projects every select entry under its name,
        // grouping keys and HAVING or ORDER BY aggregates that are not
        // selected, then RETURN reads the names. A global aggregate leaves
        // its constants to RETURN, so that they group nothing.
        let mut with = With::default();
        expressions
            .iter()
            .zip(&projected)
            .filter(|(e, _)| !global || aggregates(e))
            .for_each(|(e, p)| with.push((*e).clone(), p.name.clone()));
        let mut counter = 0;
        let mut hidden = |prefix: &str, expression: &Expr, with: &mut With| {
            let name = format!("__{prefix}_col_{counter}");
            counter += 1;
            with.push(expression.clone(), name.clone());
            name
        };
        for key in &keys {
            if with.name(key).is_none() {
                hidden("group", key, &mut with);
            }
        }
        let mut lift = |expression: &Expr, prefix: &str, with: &mut With| {
            let lifted = rewrite(expression, &mut |e| match (
                with.name(e).map(str::to_owned),
                aggregate_call(e),
            ) {
                (Some(name), _) => Ok(Some(variable(&name, e.span)?)),
                (None, true) => Ok(Some(variable(&hidden(prefix, e, with), e.span)?)),
                (None, false) => Ok(None),
            })?;
            if reads_table(&scope, &lifted) {
                return Err(invalid(
                    detail::INVALID_AGGREGATION,
                    "HAVING and ORDER BY may only use grouped columns and aggregates",
                    expression.span,
                ));
            }
            Ok(lifted)
        };
        let having = having
            .map(|having| lift(&having, "having", &mut with))
            .transpose()?;
        let ordering = ordering
            .into_iter()
            .map(|(order, descending)| Ok((lift(&order, "having", &mut with)?, descending)))
            .collect::<Result<Vec<_>>>()?;
        let projected_names: HashSet<String> =
            with.items.iter().map(|(_, name)| name.clone()).collect();
        clauses.push(Clause::Project {
            returning: false,
            items: with
                .items
                .into_iter()
                .map(|(expression, name)| item(expression, Some(name)))
                .collect(),
            distinct: !aggregated,
            ordering: Vec::new(),
            skip: None,
            limit: None,
            predicate: having,
        });
        clauses.push(Clause::Project {
            returning: true,
            items: projected
                .into_iter()
                .map(|p| {
                    if !projected_names.contains(&p.name) {
                        return Ok(p.item());
                    }
                    Ok(syntax::Item::Expression {
                        expression: variable(&p.name, span)?,
                        alias: None,
                        text: p.name,
                    })
                })
                .collect::<Result<_>>()?,
            distinct,
            ordering,
            skip: window.skip,
            limit: window.limit,
            predicate: None,
        });
        Ok(syntax::Statement { clauses })
    }

    /// A select list, as for `RETURNING`.
    pub(super) fn items(
        &self,
        items: &[ast::SelectItem],
        scope: &Scope,
    ) -> Result<Vec<syntax::Item>> {
        Ok(unique(
            items
                .iter()
                .map(|entry| self.projected(entry, scope))
                .collect::<Result<Vec<_>>>()?,
        )
        .into_iter()
        .map(Projected::item)
        .collect())
    }

    /// One select-list entry.
    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "only an unqualified column keeps its name; every other expression is named by its text"
    )]
    fn projected(&self, entry: &ast::SelectItem, scope: &Scope) -> Result<Projected> {
        let span = self.at(entry);
        match entry {
            ast::SelectItem::UnnamedExpr(value) => {
                let expression = self.expr(value, scope)?;
                // An unqualified column keeps its SQL name, as neo4j-jdbc does.
                let (name, alias) = match value {
                    ast::Expr::Identifier(column)
                        if matches!(expression.kind(), ExprKind::Property(..)) =>
                    {
                        (column.value.clone(), Alias::Derived)
                    }
                    _ if is_table_variable(scope, &expression) => {
                        (expression.to_string(), Alias::Text)
                    }
                    _ => (value.to_string(), Alias::Text),
                };
                Ok(Projected {
                    expression: Some(expression),
                    name,
                    alias,
                })
            }
            ast::SelectItem::ExprWithAlias { expr, alias } => Ok(Projected {
                expression: Some(self.expr(expr, scope)?),
                name: alias.value.clone(),
                alias: Alias::Explicit,
            }),
            ast::SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                options,
            ) if plain(options) => {
                let table = scope.table(name, span)?;
                let expression =
                    super::call("properties", vec![variable(&table.variable, span)?], span)?;
                Ok(Projected {
                    expression: Some(expression),
                    name: table.variable.clone(),
                    alias: Alias::Derived,
                })
            }
            ast::SelectItem::Wildcard(options) if plain(options) => {
                if scope.written().next().is_none() {
                    return Err(invalid(
                        detail::NO_VARIABLES_IN_SCOPE,
                        "`*` needs a table in FROM",
                        span,
                    ));
                }
                Ok(Projected {
                    expression: None,
                    name: "*".to_owned(),
                    alias: Alias::Text,
                })
            }
            ast::SelectItem::QualifiedWildcard(..)
            | ast::SelectItem::Wildcard(_)
            | ast::SelectItem::ExprWithAliases { .. } => Err(unsupported(
                "this select-list form; list columns or use `*` or `t.*`",
                span,
            )),
        }
    }

    fn group_keys(
        &self,
        group_by: &ast::GroupByExpr,
        projected: &[Projected],
        scope: &Scope,
        span: Span,
    ) -> Result<Vec<Expr>> {
        let ast::GroupByExpr::Expressions(keys, modifiers) = group_by else {
            return Err(unsupported("GROUP BY ALL", span));
        };
        if !modifiers.is_empty() {
            return Err(unsupported(
                "GROUP BY modifiers such as ROLLUP and CUBE",
                span,
            ));
        }
        let keys = keys
            .iter()
            .map(|key| match self.position(key, projected)? {
                Some(index) => projected[index]
                    .expression
                    .as_ref()
                    .map(|expression| self.copy(expression))
                    .transpose()?
                    .ok_or_else(|| {
                        invalid(
                            detail::INVALID_AGGREGATION,
                            "cannot group by `*`",
                            self.at(key),
                        )
                    }),
                None => self.expr(key, scope),
            })
            .collect::<Result<Vec<_>>>()?;
        match keys.iter().find(|key| aggregates(key)) {
            Some(key) => Err(invalid(
                detail::INVALID_AGGREGATION,
                "GROUP BY cannot use an aggregate",
                key.span,
            )),
            None => Ok(keys),
        }
    }

    fn ordering(
        &self,
        order_by: &ast::OrderBy,
        projected: &[Projected],
        scope: &Scope,
    ) -> Result<Vec<(Expr, bool)>> {
        let span = self.at(order_by);
        let ast::OrderBy {
            kind: ast::OrderByKind::Expressions(expressions),
            interpolate: None,
        } = order_by
        else {
            return Err(unsupported("ORDER BY ALL and INTERPOLATE", span));
        };
        expressions
            .iter()
            .map(|order| {
                let span = self.at(&order.expr);
                let descending = match order.options.sort {
                    None | Some(ast::OrderBySort::Asc) => false,
                    Some(ast::OrderBySort::Desc) => true,
                    Some(ast::OrderBySort::Using(_)) => {
                        return Err(unsupported("ORDER BY USING", span));
                    }
                };
                // Cypher sorts nulls last ascending and first descending.
                if order
                    .options
                    .nulls_first
                    .is_some_and(|first| first != descending)
                    || order.with_fill.is_some()
                {
                    return Err(unsupported(
                        "NULLS FIRST ascending, NULLS LAST descending and WITH FILL",
                        span,
                    ));
                }
                let expression = match self.position(&order.expr, projected)? {
                    Some(index) => {
                        let p = &projected[index];
                        match (p.alias, &p.expression) {
                            (Alias::Explicit | Alias::Derived, _) => variable(&p.name, span)?,
                            (Alias::Text, Some(expression)) => self.copy(expression)?,
                            (Alias::Text, None) => {
                                return Err(invalid(
                                    detail::INVALID_STATEMENT,
                                    "cannot order by `*`",
                                    span,
                                ));
                            }
                        }
                    }
                    None => self.expr(&order.expr, scope)?,
                };
                Ok((expression, descending))
            })
            .collect()
    }

    /// A 1-based select-list position, as in `ORDER BY 1`.
    fn position(&self, expression: &ast::Expr, projected: &[Projected]) -> Result<Option<usize>> {
        let ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(text, _),
            ..
        }) = expression
        else {
            return Ok(None);
        };
        text.parse::<usize>()
            .ok()
            .filter(|position| (1..=projected.len()).contains(position))
            .map(|position| Some(position - 1))
            .ok_or_else(|| {
                invalid(
                    detail::INVALID_STATEMENT,
                    format!("{text} is not a select-list position"),
                    self.at(expression),
                )
            })
    }
}

impl Projected {
    /// The entry as a projection item. A column without an alias is named by
    /// its SQL text, as written, rather than by the translated Cypher.
    fn item(self) -> syntax::Item {
        match self.expression {
            None => syntax::Item::Wildcard,
            Some(expression) => syntax::Item::Expression {
                // Cypher names an unaliased variable after itself, so a
                // renumbered table variable needs an alias.
                alias: match self.alias {
                    Alias::Explicit | Alias::Derived => Some(self.name.clone()),
                    Alias::Text => matches!(
                        expression.kind(),
                        ExprKind::Variable(variable) if *variable != self.name
                    )
                    .then(|| self.name.clone()),
                },
                expression,
                text: self.name,
            },
        }
    }
}

/// Number repeated names of entries without an explicit alias, as `name`,
/// `name_2`, …, so that `SELECT 1, 1` and `SELECT name, name` have two
/// columns. Explicit aliases stay as written.
fn unique(mut projected: Vec<Projected>) -> Vec<Projected> {
    let mut seen: HashSet<String> = HashSet::new();
    // The next suffix to try per repeated name, so numbering stays linear.
    let mut next: HashMap<String, usize> = HashMap::new();
    for entry in &mut projected {
        if entry.alias != Alias::Explicit
            && entry.expression.is_some()
            && seen.contains(&entry.name)
        {
            let suffix = next.entry(entry.name.clone()).or_insert(2);
            let name = std::iter::repeat_with(|| {
                let candidate = format!("{}_{suffix}", entry.name);
                *suffix += 1;
                candidate
            })
            .find(|candidate| !seen.contains(candidate))
            .expect("a free name exists");
            entry.name = name;
        }
        seen.insert(entry.name.clone());
    }
    projected
}

fn plain(options: &ast::WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_none()
        && options.opt_exclude.is_none()
        && options.opt_except.is_none()
        && options.opt_replace.is_none()
        && options.opt_rename.is_none()
        && options.opt_alias.is_none()
}

fn aggregate_call(expression: &Expr) -> bool {
    matches!(expression.kind(), ExprKind::Call { name, .. }
        if AGGREGATES.iter().any(|a| name.eq_ignore_ascii_case(a)))
}

fn aggregates(expression: &Expr) -> bool {
    let mut found = false;
    visit(expression, &mut |e| found |= aggregate_call(e));
    found
}
